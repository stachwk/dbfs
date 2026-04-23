use std::panic;
use std::slice;

use crate::{
    assemble_read_slice, block_count_for_length, block_transfer_plan, copy_segments, crc32_bytes,
    dirty_block_size, logical_resize_plan, pack_changed_ranges, pad_block_bytes, parallel_worker_count,
    parallel_worker_plan, persist_block_plan, persist_layout_plan, pg::DbRepo, read_ahead_blocks, read_fetch_bounds,
    read_missing_range_worker_count, read_slice_plan, sorted_contiguous_ranges, write_copy_plan,
    write_copy_worker_count,
};

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsCopySegment {
    pub src: u64,
    pub dst: u64,
    pub len: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsRange {
    pub start: u64,
    pub end: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsReadBlock {
    pub index: u64,
    pub ptr: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsReadSequenceStepResult {
    pub sequential: u8,
    pub streak: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsReadBounds {
    pub fetch_first: u64,
    pub fetch_last: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsReadSlicePlan {
    pub total_blocks: u64,
    pub fetch_first: u64,
    pub fetch_last: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsTextQueryResult {
    pub found: u8,
}

#[repr(C)]
pub struct DbfsPgRepo {
    pub repo: DbRepo,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPgBootstrapSnapshot {
    pub block_size: u32,
    pub block_size_found: u8,
    pub is_in_recovery: u8,
    pub schema_version: u32,
    pub schema_version_found: u8,
    pub schema_is_initialized: u8,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPgResolvedPath {
    pub parent_id: u64,
    pub parent_found: u8,
    pub kind: u8,
    pub entry_id: u64,
    pub entry_found: u8,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsBlockTransferPlan {
    pub total_blocks: u64,
    pub parallel: u8,
    pub workers: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsParallelWorkerPlan {
    pub parallel: u8,
    pub workers: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsWriteCopyPlan {
    pub total_blocks: u64,
    pub dedupe_enabled: u8,
    pub parallel: u8,
    pub workers: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsLogicalResizePlan {
    pub old_size: u64,
    pub new_size: u64,
    pub block_size: u64,
    pub old_total_blocks: u64,
    pub new_total_blocks: u64,
    pub shrinking: u8,
    pub has_valid_blocks: u8,
    pub delete_from_block: u64,
    pub max_valid_block: u64,
    pub has_partial_tail: u8,
    pub tail_block_index: u64,
    pub tail_valid_len: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistLayoutPlan {
    pub total_blocks: u64,
    pub truncate_only: u8,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistBlockPlanEntry {
    pub block_index: u64,
    pub used_len: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistBlockInput {
    pub block_index: u64,
    pub ptr: *const u8,
    pub len: usize,
    pub used_len: u64,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistCrcPlanEntry {
    pub block_index: u64,
    pub has_crc: u8,
    pub crc32: u32,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistBlockPlan {
    pub total_blocks: u64,
    pub truncate_only: u8,
    pub blocks_ptr: *mut DbfsPersistBlockPlanEntry,
    pub blocks_len: usize,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct DbfsPersistCrcPlan {
    pub rows_ptr: *mut DbfsPersistCrcPlanEntry,
    pub rows_len: usize,
}

unsafe fn slice_from_raw<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    Some(slice::from_raw_parts(ptr, len))
}

unsafe fn write_boxed_output<T>(
    values: Vec<T>,
    out_ptr: *mut *mut T,
    out_len: *mut usize,
) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return 1;
    }

    let mut boxed = values.into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    *out_ptr = ptr;
    *out_len = len;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_copy_plan(
    off_in: u64,
    off_out: u64,
    length: u64,
    block_size: u64,
    workers: u64,
    out_ptr: *mut *mut DbfsCopySegment,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        let segments = copy_segments(off_in, off_out, length, block_size, workers)
            .into_iter()
            .map(|(src, dst, len)| DbfsCopySegment { src, dst, len })
            .collect::<Vec<_>>();

        unsafe { write_boxed_output(segments, out_ptr, out_len) }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_copy_pack(
    off_out: u64,
    total_len: u64,
    block_size: u64,
    changed_mask_ptr: *const u8,
    changed_mask_len: usize,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        let changed_mask = match slice_from_raw(changed_mask_ptr, changed_mask_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let changed_mask = changed_mask
            .iter()
            .copied()
            .map(|byte| byte != 0)
            .collect::<Vec<_>>();
        let ranges = pack_changed_ranges(off_out, total_len, block_size, &changed_mask)
            .into_iter()
            .map(|(start, end)| DbfsRange { start, end })
            .collect::<Vec<_>>();

        write_boxed_output(ranges, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_copy_dedupe(
    dst_offset: u64,
    payload_ptr: *const u8,
    payload_len: usize,
    current_ptr: *const u8,
    current_len: usize,
    block_size: usize,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        let payload = match slice_from_raw(payload_ptr, payload_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let current = match slice_from_raw(current_ptr, current_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let block_size = block_size.max(1);
        let mut changed_mask = Vec::new();

        for rel_offset in (0..payload.len()).step_by(block_size) {
            let rel_end = (rel_offset + block_size).min(payload.len());
            let payload_chunk = &payload[rel_offset..rel_end];
            let current_chunk = if rel_offset >= current.len() {
                &[]
            } else {
                let current_end = (rel_offset + block_size).min(current.len());
                &current[rel_offset..current_end]
            };
            changed_mask.push(payload_chunk != current_chunk);
        }

        let ranges = pack_changed_ranges(dst_offset, payload.len() as u64, block_size as u64, &changed_mask)
            .into_iter()
            .map(|(start, end)| DbfsRange { start, end })
            .collect::<Vec<_>>();

        write_boxed_output(ranges, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_persist_pad(
    input_ptr: *const u8,
    input_len: usize,
    used_len: usize,
    block_size: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        let input = match slice_from_raw(input_ptr, input_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let output = pad_block_bytes(input, used_len as u64, block_size as u64);
        write_boxed_output(output, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_assemble(
    blocks_ptr: *const DbfsReadBlock,
    blocks_len: usize,
    fetch_first: u64,
    fetch_last: u64,
    offset: u64,
    end_offset: u64,
    block_size: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        let blocks = if blocks_len == 0 {
            &[][..]
        } else if blocks_ptr.is_null() {
            return 1;
        } else {
            slice::from_raw_parts(blocks_ptr, blocks_len)
        };

        let mut parsed_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            let data = match slice_from_raw(block.ptr, block.len) {
                Some(slice) => slice,
                None => return 1,
            };
            parsed_blocks.push((block.index, data.to_vec()));
        }

        let output = assemble_read_slice(
            fetch_first,
            fetch_last,
            offset,
            end_offset,
            block_size as u64,
            &parsed_blocks,
        );
        write_boxed_output(output, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_new(
    conninfo_ptr: *const u8,
    conninfo_len: usize,
    out_repo: *mut *mut DbfsPgRepo,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if out_repo.is_null() {
            return 1;
        }
        let conninfo = match slice_from_raw(conninfo_ptr, conninfo_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let conninfo = match std::str::from_utf8(conninfo) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let repo = match DbRepo::new(conninfo) {
            Ok(repo) => repo,
            Err(_) => return 3,
        };
        let boxed = Box::new(DbfsPgRepo { repo });
        *out_repo = Box::into_raw(boxed);
        0
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_free(repo_ptr: *mut DbfsPgRepo) {
    if repo_ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(repo_ptr);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_query_scalar_text(
    repo_ptr: *mut DbfsPgRepo,
    sql_ptr: *const u8,
    sql_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() {
            return 1;
        }
        let sql = match slice_from_raw(sql_ptr, sql_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let sql = match std::str::from_utf8(sql) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let value = match (*repo_ptr).repo.query_scalar_text(sql) {
            Ok(value) => value.into_bytes(),
            Err(_) => return 3,
        };
        write_boxed_output(value, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_config_value(
    repo_ptr: *mut DbfsPgRepo,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let key = match slice_from_raw(key_ptr, key_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let key = match std::str::from_utf8(key) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.query_config_value(key) {
            Ok(Some(value)) => {
                *out_found = 1;
                write_boxed_output(value.into_bytes(), out_ptr, out_len)
            }
            Ok(None) => {
                *out_found = 0;
                if !out_ptr.is_null() {
                    *out_ptr = std::ptr::null_mut();
                }
                if !out_len.is_null() {
                    *out_len = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_is_in_recovery(
    repo_ptr: *mut DbfsPgRepo,
    out_value: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_value.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.is_in_recovery() {
            Ok(value) => {
                *out_value = if value { 1 } else { 0 };
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_schema_version(
    repo_ptr: *mut DbfsPgRepo,
    out_value: *mut u32,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_value.is_null() || out_found.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.schema_version() {
            Ok(Some(value)) => {
                *out_value = value;
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_schema_is_initialized(
    repo_ptr: *mut DbfsPgRepo,
    out_value: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_value.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.schema_is_initialized() {
            Ok(value) => {
                *out_value = if value { 1 } else { 0 };
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_bootstrap_snapshot(
    repo_ptr: *mut DbfsPgRepo,
    out_block_size: *mut u32,
    out_block_size_found: *mut u8,
    out_is_in_recovery: *mut u8,
    out_schema_version: *mut u32,
    out_schema_version_found: *mut u8,
    out_schema_is_initialized: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null()
            || out_block_size.is_null()
            || out_block_size_found.is_null()
            || out_is_in_recovery.is_null()
            || out_schema_version.is_null()
            || out_schema_version_found.is_null()
            || out_schema_is_initialized.is_null()
        {
            return 1;
        }
        match (*repo_ptr).repo.startup_snapshot() {
            Ok(snapshot) => {
                *out_block_size = snapshot.block_size.unwrap_or(0);
                *out_block_size_found = if snapshot.block_size.is_some() { 1 } else { 0 };
                *out_is_in_recovery = if snapshot.is_in_recovery { 1 } else { 0 };
                *out_schema_version = snapshot.schema_version.unwrap_or(0);
                *out_schema_version_found = if snapshot.schema_version.is_some() { 1 } else { 0 };
                *out_schema_is_initialized = if snapshot.schema_is_initialized { 1 } else { 0 };
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_dir_id(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.get_dir_id(path) {
            Ok(Some(value)) => {
                if !out_value.is_null() {
                    *out_value = value;
                }
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                if !out_value.is_null() {
                    *out_value = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_file_id(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.get_file_id(path) {
            Ok(Some(value)) => {
                if !out_value.is_null() {
                    *out_value = value;
                }
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                if !out_value.is_null() {
                    *out_value = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_count_file_links(
    repo_ptr: *mut DbfsPgRepo,
    file_id: u64,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() || out_value.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.count_file_links(file_id) {
            Ok(value) => {
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_path_has_children(
    repo_ptr: *mut DbfsPgRepo,
    directory_id: u64,
    out_value: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_value.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.path_has_children(directory_id) {
            Ok(value) => {
                *out_value = if value { 1 } else { 0 };
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_file_mode_value(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.get_file_mode_value(path) {
            Ok(Some(value)) => {
                *out_found = 1;
                write_boxed_output(value.into_bytes(), out_ptr, out_len)
            }
            Ok(None) => {
                *out_found = 0;
                if !out_ptr.is_null() {
                    *out_ptr = std::ptr::null_mut();
                }
                if !out_len.is_null() {
                    *out_len = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_hardlink_id(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.get_hardlink_id(path) {
            Ok(Some(value)) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_symlink_id(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.get_symlink_id(path) {
            Ok(Some(value)) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_get_hardlink_file_id(
    repo_ptr: *mut DbfsPgRepo,
    hardlink_id: u64,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.get_hardlink_file_id(hardlink_id) {
            Ok(Some(value)) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Ok(None) => {
                *out_found = 0;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_create_hardlink(
    repo_ptr: *mut DbfsPgRepo,
    source_file_id: u64,
    target_parent_id: u64,
    target_parent_found: u8,
    target_name_ptr: *const u8,
    target_name_len: usize,
    uid: u32,
    gid: u32,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let target_name = match slice_from_raw(target_name_ptr, target_name_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target_name = match std::str::from_utf8(target_name) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let parent_id = if target_parent_found != 0 {
            Some(target_parent_id)
        } else {
            None
        };
        match (*repo_ptr).repo.create_hardlink(source_file_id, parent_id, target_name, uid, gid) {
            Ok(value) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_create_directory(
    repo_ptr: *mut DbfsPgRepo,
    target_parent_id: u64,
    target_parent_found: u8,
    target_name_ptr: *const u8,
    target_name_len: usize,
    mode: u32,
    uid: u32,
    gid: u32,
    inode_seed_ptr: *const u8,
    inode_seed_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let target_name = match slice_from_raw(target_name_ptr, target_name_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target_name = match std::str::from_utf8(target_name) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let inode_seed = match slice_from_raw(inode_seed_ptr, inode_seed_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let inode_seed = match std::str::from_utf8(inode_seed) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let parent_id = if target_parent_found != 0 {
            Some(target_parent_id)
        } else {
            None
        };
        match (*repo_ptr).repo.create_directory(parent_id, target_name, mode, uid, gid, inode_seed) {
            Ok(value) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_create_file(
    repo_ptr: *mut DbfsPgRepo,
    target_parent_id: u64,
    target_parent_found: u8,
    target_name_ptr: *const u8,
    target_name_len: usize,
    mode: u32,
    uid: u32,
    gid: u32,
    inode_seed_ptr: *const u8,
    inode_seed_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let target_name = match slice_from_raw(target_name_ptr, target_name_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target_name = match std::str::from_utf8(target_name) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let inode_seed = match slice_from_raw(inode_seed_ptr, inode_seed_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let inode_seed = match std::str::from_utf8(inode_seed) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let parent_id = if target_parent_found != 0 {
            Some(target_parent_id)
        } else {
            None
        };
        match (*repo_ptr).repo.create_file(parent_id, target_name, mode, uid, gid, inode_seed) {
            Ok(value) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_create_special_file(
    repo_ptr: *mut DbfsPgRepo,
    target_parent_id: u64,
    target_parent_found: u8,
    target_name_ptr: *const u8,
    target_name_len: usize,
    mode: u32,
    uid: u32,
    gid: u32,
    inode_seed_ptr: *const u8,
    inode_seed_len: usize,
    file_kind_ptr: *const u8,
    file_kind_len: usize,
    rdev_major: u32,
    rdev_minor: u32,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let target_name = match slice_from_raw(target_name_ptr, target_name_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target_name = match std::str::from_utf8(target_name) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let inode_seed = match slice_from_raw(inode_seed_ptr, inode_seed_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let inode_seed = match std::str::from_utf8(inode_seed) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let file_kind = match slice_from_raw(file_kind_ptr, file_kind_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let file_kind = match std::str::from_utf8(file_kind) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let parent_id = if target_parent_found != 0 {
            Some(target_parent_id)
        } else {
            None
        };
        match (*repo_ptr)
            .repo
            .create_special_file(parent_id, target_name, mode, uid, gid, inode_seed, file_kind, rdev_major, rdev_minor)
        {
            Ok(value) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_create_symlink(
    repo_ptr: *mut DbfsPgRepo,
    target_parent_id: u64,
    target_parent_found: u8,
    target_name_ptr: *const u8,
    target_name_len: usize,
    target_ptr: *const u8,
    target_len: usize,
    uid: u32,
    gid: u32,
    inode_seed_ptr: *const u8,
    inode_seed_len: usize,
    out_value: *mut u64,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() {
            return 1;
        }
        let target_name = match slice_from_raw(target_name_ptr, target_name_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target_name = match std::str::from_utf8(target_name) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let target = match slice_from_raw(target_ptr, target_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let target = match std::str::from_utf8(target) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let inode_seed = match slice_from_raw(inode_seed_ptr, inode_seed_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let inode_seed = match std::str::from_utf8(inode_seed) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let parent_id = if target_parent_found != 0 {
            Some(target_parent_id)
        } else {
            None
        };
        match (*repo_ptr).repo.create_symlink(parent_id, target_name, target, uid, gid, inode_seed) {
            Ok(value) => {
                if out_value.is_null() {
                    return 1;
                }
                *out_value = value;
                *out_found = 1;
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_choose_primary_hardlink(
    repo_ptr: *mut DbfsPgRepo,
    file_id: u64,
    out_hardlink_id: *mut u64,
    out_parent_id: *mut u64,
    out_parent_found: *mut u8,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_found.is_null() || out_parent_found.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.choose_primary_hardlink(file_id) {
            Ok(Some((hardlink_id, parent_id, name))) => {
                if out_hardlink_id.is_null() {
                    return 1;
                }
                *out_hardlink_id = hardlink_id;
                match parent_id {
                    Some(value) => {
                        if out_parent_id.is_null() {
                            return 1;
                        }
                        *out_parent_id = value;
                        *out_parent_found = 1;
                    }
                    None => {
                        *out_parent_found = 0;
                        if !out_parent_id.is_null() {
                            *out_parent_id = 0;
                        }
                    }
                }
                *out_found = 1;
                write_boxed_output(name.into_bytes(), out_ptr, out_len)
            }
            Ok(None) => {
                *out_found = 0;
                *out_parent_found = 0;
                if !out_hardlink_id.is_null() {
                    *out_hardlink_id = 0;
                }
                if !out_parent_id.is_null() {
                    *out_parent_id = 0;
                }
                if !out_ptr.is_null() {
                    *out_ptr = std::ptr::null_mut();
                }
                if !out_len.is_null() {
                    *out_len = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                *out_parent_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_promote_hardlink_to_primary(
    repo_ptr: *mut DbfsPgRepo,
    file_id: u64,
    out_promoted: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null() || out_promoted.is_null() {
            return 1;
        }
        match (*repo_ptr).repo.promote_hardlink_to_primary(file_id) {
            Ok(value) => {
                *out_promoted = if value { 1 } else { 0 };
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_rust_pg_repo_resolve_path(
    repo_ptr: *mut DbfsPgRepo,
    path_ptr: *const u8,
    path_len: usize,
    out_parent_id: *mut u64,
    out_parent_found: *mut u8,
    out_kind: *mut u8,
    out_entry_id: *mut u64,
    out_entry_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if repo_ptr.is_null()
            || out_parent_found.is_null()
            || out_kind.is_null()
            || out_entry_found.is_null()
        {
            return 1;
        }
        let path = match slice_from_raw(path_ptr, path_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let path = match std::str::from_utf8(path) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        match (*repo_ptr).repo.resolve_path(path) {
            Ok(resolved) => {
                if let Some(parent_id) = resolved.parent_id {
                    if !out_parent_id.is_null() {
                        *out_parent_id = parent_id;
                    }
                    *out_parent_found = 1;
                } else {
                    if !out_parent_id.is_null() {
                        *out_parent_id = 0;
                    }
                    *out_parent_found = 0;
                }
                *out_kind = match resolved.kind.as_deref() {
                    Some("hardlink") => 1,
                    Some("symlink") => 2,
                    Some("file") => 3,
                    Some("dir") => 4,
                    _ => 0,
                };
                if let Some(entry_id) = resolved.entry_id {
                    if !out_entry_id.is_null() {
                        *out_entry_id = entry_id;
                    }
                    *out_entry_found = 1;
                } else {
                    if !out_entry_id.is_null() {
                        *out_entry_id = 0;
                    }
                    *out_entry_found = 0;
                }
                0
            }
            Err(_) => 3,
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_pg_query_scalar_text(
    conninfo_ptr: *const u8,
    conninfo_len: usize,
    sql_ptr: *const u8,
    sql_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        let conninfo = match slice_from_raw(conninfo_ptr, conninfo_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let sql = match slice_from_raw(sql_ptr, sql_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let conninfo = match std::str::from_utf8(conninfo) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let sql = match std::str::from_utf8(sql) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let repo = match DbRepo::new(conninfo) {
            Ok(repo) => repo,
            Err(_) => return 3,
        };
        let value = match repo.query_scalar_text(sql) {
            Ok(value) => value.into_bytes(),
            Err(_) => return 3,
        };
        write_boxed_output(value, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_pg_get_config_value(
    conninfo_ptr: *const u8,
    conninfo_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    out_found: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if out_found.is_null() {
            return 1;
        }
        let conninfo = match slice_from_raw(conninfo_ptr, conninfo_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let key = match slice_from_raw(key_ptr, key_len) {
            Some(slice) => slice,
            None => return 1,
        };
        let conninfo = match std::str::from_utf8(conninfo) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let key = match std::str::from_utf8(key) {
            Ok(value) => value,
            Err(_) => return 1,
        };
        let repo = match DbRepo::new(conninfo) {
            Ok(repo) => repo,
            Err(_) => return 3,
        };
        match repo.query_config_value(key) {
            Ok(Some(value)) => {
                *out_found = 1;
                write_boxed_output(value.into_bytes(), out_ptr, out_len)
            }
            Ok(None) => {
                *out_found = 0;
                if !out_ptr.is_null() {
                    *out_ptr = std::ptr::null_mut();
                }
                if !out_len.is_null() {
                    *out_len = 0;
                }
                0
            }
            Err(_) => {
                *out_found = 0;
                3
            }
        }
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_crc32(input_ptr: *const u8, input_len: usize) -> u32 {
    unsafe {
        match slice_from_raw(input_ptr, input_len) {
            Some(slice) => crc32_bytes(slice),
            None => 0,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_sequence_step(
    has_previous: u8,
    previous_last_end: u64,
    offset: u64,
    previous_streak: u64,
) -> DbfsReadSequenceStepResult {
    let sequential = has_previous != 0 && previous_last_end == offset;
    let streak = if sequential {
        previous_streak.saturating_add(1)
    } else {
        0
    };

    DbfsReadSequenceStepResult {
        sequential: sequential as u8,
        streak,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_ahead_blocks(
    read_ahead_blocks_value: u64,
    sequential_read_ahead_blocks_value: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: u8,
) -> u64 {
    read_ahead_blocks(
        read_ahead_blocks_value,
        sequential_read_ahead_blocks_value,
        streak,
        read_cache_limit_blocks,
        sequential != 0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_fetch_bounds(
    total_blocks: u64,
    requested_first: u64,
    requested_last: u64,
    read_ahead_blocks_value: u64,
    sequential_read_ahead_blocks_value: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: u8,
    small_file_threshold_blocks: u64,
    out_ptr: *mut DbfsReadBounds,
) -> i32 {
    let result = panic::catch_unwind(|| {
        let bounds = match read_fetch_bounds(
            total_blocks,
            requested_first,
            requested_last,
            read_ahead_blocks_value,
            sequential_read_ahead_blocks_value,
            streak,
            read_cache_limit_blocks,
            sequential != 0,
            small_file_threshold_blocks,
        ) {
            Some((fetch_first, fetch_last)) => DbfsReadBounds {
                fetch_first,
                fetch_last,
            },
            None => return 1,
        };

        if out_ptr.is_null() {
            return 1;
        }
        unsafe {
            *out_ptr = bounds;
        }
        0
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_slice_plan(
    file_size: u64,
    offset: u64,
    size: u64,
    block_size: u64,
    read_ahead_blocks_value: u64,
    sequential_read_ahead_blocks_value: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: u8,
    small_file_threshold_blocks: u64,
    out_ptr: *mut DbfsReadSlicePlan,
) -> i32 {
    let result = panic::catch_unwind(|| {
        let plan = match read_slice_plan(
            file_size,
            offset,
            size,
            block_size,
            read_ahead_blocks_value,
            sequential_read_ahead_blocks_value,
            streak,
            read_cache_limit_blocks,
            sequential != 0,
            small_file_threshold_blocks,
        ) {
            Some((total_blocks, fetch_first, fetch_last)) => DbfsReadSlicePlan {
                total_blocks,
                fetch_first,
                fetch_last,
            },
            None => return 1,
        };

        if out_ptr.is_null() {
            return 1;
        }
        unsafe {
            *out_ptr = plan;
        }
        0
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_block_transfer_plan(
    length: u64,
    block_size: u64,
    requested_workers: u64,
    workers_min_blocks: u64,
    minimum_one: u8,
) -> DbfsBlockTransferPlan {
    let (total_blocks, parallel, workers) = block_transfer_plan(
        length,
        block_size,
        requested_workers,
        workers_min_blocks,
        minimum_one != 0,
    );
    DbfsBlockTransferPlan {
        total_blocks,
        parallel: parallel as u8,
        workers,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_read_missing_range_worker_count(
    workers_read: u64,
    workers_read_min_blocks: u64,
    missing_len: u64,
    contiguous_ranges_len: u64,
) -> u64 {
    read_missing_range_worker_count(
        workers_read,
        workers_read_min_blocks,
        missing_len,
        contiguous_ranges_len,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_block_count_for_length(length: u64, block_size: u64, minimum_one: u8) -> u64 {
    block_count_for_length(length, block_size, minimum_one != 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_dirty_block_size(file_size: u64, block_index: u64, block_size: u64) -> u64 {
    dirty_block_size(file_size, block_index, block_size)
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_logical_resize_plan(
    old_size: u64,
    new_size: u64,
    block_size: u64,
) -> DbfsLogicalResizePlan {
    let plan = logical_resize_plan(old_size, new_size, block_size);
    DbfsLogicalResizePlan {
        old_size: plan.old_size,
        new_size: plan.new_size,
        block_size: plan.block_size,
        old_total_blocks: plan.old_total_blocks,
        new_total_blocks: plan.new_total_blocks,
        shrinking: plan.shrinking as u8,
        has_valid_blocks: plan.has_valid_blocks as u8,
        delete_from_block: plan.delete_from_block,
        max_valid_block: plan.max_valid_block,
        has_partial_tail: plan.has_partial_tail as u8,
        tail_block_index: plan.tail_block_index,
        tail_valid_len: plan.tail_valid_len,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_write_copy_worker_count(
    total_blocks: u64,
    workers_write: u64,
    workers_write_min_blocks: u64,
) -> u64 {
    write_copy_worker_count(total_blocks, workers_write, workers_write_min_blocks)
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_parallel_worker_count(
    requested_workers: u64,
    minimum_items_for_parallel: u64,
    total_items: u64,
    parallel_groups: u64,
) -> u64 {
    parallel_worker_count(
        requested_workers,
        minimum_items_for_parallel,
        total_items,
        parallel_groups,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_parallel_worker_plan(
    requested_workers: u64,
    minimum_items_for_parallel: u64,
    total_items: u64,
    parallel_groups: u64,
) -> DbfsParallelWorkerPlan {
    let (parallel, workers) = parallel_worker_plan(
        requested_workers,
        minimum_items_for_parallel,
        total_items,
        parallel_groups,
    );
    DbfsParallelWorkerPlan {
        parallel: parallel as u8,
        workers,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_write_copy_plan(
    length: u64,
    block_size: u64,
    workers_write: u64,
    workers_write_min_blocks: u64,
    copy_dedupe_enabled: u8,
    copy_dedupe_min_blocks: u64,
    copy_dedupe_max_blocks: u64,
) -> DbfsWriteCopyPlan {
    let (total_blocks, dedupe_enabled, parallel, workers) = write_copy_plan(
        length,
        block_size,
        workers_write,
        workers_write_min_blocks,
        copy_dedupe_enabled != 0,
        copy_dedupe_min_blocks,
        copy_dedupe_max_blocks,
    );
    DbfsWriteCopyPlan {
        total_blocks,
        dedupe_enabled: dedupe_enabled as u8,
        parallel: parallel as u8,
        workers,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_sorted_contiguous_ranges(
    values_ptr: *const u64,
    values_len: usize,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if values_len == 0 {
            return write_boxed_output(Vec::<DbfsRange>::new(), out_ptr, out_len);
        }
        if values_ptr.is_null() {
            return 1;
        }
        let values = slice::from_raw_parts(values_ptr, values_len);
        let ranges = sorted_contiguous_ranges(values)
            .into_iter()
            .map(|(start, end)| DbfsRange { start, end })
            .collect::<Vec<_>>();
        write_boxed_output(ranges, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_dirty_block_ranges_plan(
    file_size: u64,
    block_size: u64,
    dirty_ptr: *const u64,
    dirty_len: usize,
    out_total_blocks: *mut u64,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if out_total_blocks.is_null() {
            return 1;
        }
        let total_blocks = block_count_for_length(file_size, block_size, false);
        if dirty_len == 0 {
            *out_total_blocks = total_blocks;
            return write_boxed_output(Vec::<DbfsRange>::new(), out_ptr, out_len);
        }
        if dirty_ptr.is_null() {
            return 1;
        }
        let dirty = slice::from_raw_parts(dirty_ptr, dirty_len);
        let ranges = sorted_contiguous_ranges(dirty)
            .into_iter()
            .map(|(start, end)| DbfsRange { start, end })
            .collect::<Vec<_>>();
        *out_total_blocks = total_blocks;
        write_boxed_output(ranges, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_persist_layout_plan(
    file_size: u64,
    block_size: u64,
    truncate_pending: u8,
    dirty_ptr: *const u64,
    dirty_len: usize,
    out_total_blocks: *mut u64,
    out_truncate_only: *mut u8,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if out_total_blocks.is_null() || out_truncate_only.is_null() {
            return 1;
        }
        let dirty = if dirty_len == 0 {
            &[][..]
        } else {
            if dirty_ptr.is_null() {
                return 1;
            }
            slice::from_raw_parts(dirty_ptr, dirty_len)
        };
        let plan = persist_layout_plan(file_size, block_size, truncate_pending != 0, dirty);
        *out_total_blocks = plan.total_blocks;
        *out_truncate_only = if plan.truncate_only { 1 } else { 0 };
        let ranges = plan
            .ordered_dirty_ranges
            .into_iter()
            .map(|(start, end)| DbfsRange { start, end })
            .collect::<Vec<_>>();
        write_boxed_output(ranges, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_persist_block_plan(
    file_size: u64,
    block_size: u64,
    truncate_pending: u8,
    dirty_ptr: *const u64,
    dirty_len: usize,
    out_total_blocks: *mut u64,
    out_truncate_only: *mut u8,
    out_ptr: *mut *mut DbfsPersistBlockPlanEntry,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if out_total_blocks.is_null() || out_truncate_only.is_null() {
            return 1;
        }
        let dirty = if dirty_len == 0 {
            &[][..]
        } else {
            if dirty_ptr.is_null() {
                return 1;
            }
            slice::from_raw_parts(dirty_ptr, dirty_len)
        };
        let plan = persist_block_plan(file_size, block_size, truncate_pending != 0, dirty);
        *out_total_blocks = plan.total_blocks;
        *out_truncate_only = if plan.truncate_only { 1 } else { 0 };
        let blocks = plan
            .blocks
            .into_iter()
            .map(|entry| DbfsPersistBlockPlanEntry {
                block_index: entry.block_index,
                used_len: entry.used_len,
            })
            .collect::<Vec<_>>();
        write_boxed_output(blocks, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_persist_block_crc_plan(
    block_size: u64,
    blocks_ptr: *const DbfsPersistBlockInput,
    blocks_len: usize,
    out_ptr: *mut *mut DbfsPersistCrcPlanEntry,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if blocks_len == 0 {
            return write_boxed_output(Vec::<DbfsPersistCrcPlanEntry>::new(), out_ptr, out_len);
        }
        if blocks_ptr.is_null() {
            return 1;
        }
        let blocks = slice::from_raw_parts(blocks_ptr, blocks_len);
        let mut rows = Vec::with_capacity(blocks.len());
        let block_size = block_size.max(1);
        for block in blocks {
            let data = match slice_from_raw(block.ptr, block.len) {
                Some(slice) => slice,
                None => return 1,
            };
            let used_len = block.used_len.min(block_size);
            if used_len >= block_size {
                rows.push(DbfsPersistCrcPlanEntry {
                    block_index: block.block_index,
                    has_crc: 1,
                    crc32: crc32_bytes(data),
                });
            } else {
                rows.push(DbfsPersistCrcPlanEntry {
                    block_index: block.block_index,
                    has_crc: 0,
                    crc32: 0,
                });
            }
        }
        write_boxed_output(rows, out_ptr, out_len)
    });

    match result {
        Ok(status) => status,
        Err(_) => 2,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_free_copy_segments(ptr: *mut DbfsCopySegment, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_free_ranges(ptr: *mut DbfsRange, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_free_persist_blocks(ptr: *mut DbfsPersistBlockPlanEntry, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_free_persist_crc_rows(ptr: *mut DbfsPersistCrcPlanEntry, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dbfs_free_bytes(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        dbfs_block_count_for_length, dbfs_copy_dedupe, dbfs_copy_pack, dbfs_copy_plan,
        dbfs_crc32, dbfs_free_bytes, dbfs_free_copy_segments, dbfs_free_persist_blocks,
        dbfs_free_persist_crc_rows,
        dbfs_free_ranges, dbfs_persist_block_plan, dbfs_persist_pad, dbfs_read_assemble,
        dbfs_read_ahead_blocks, dbfs_read_fetch_bounds, dbfs_read_missing_range_worker_count,
        dbfs_read_sequence_step, dbfs_read_slice_plan, dbfs_sorted_contiguous_ranges,
        dbfs_block_transfer_plan, dbfs_dirty_block_ranges_plan, dbfs_logical_resize_plan,
        dbfs_parallel_worker_count, dbfs_parallel_worker_plan, dbfs_persist_block_crc_plan,
        dbfs_persist_layout_plan,
        dbfs_write_copy_plan, dbfs_write_copy_worker_count,
        dbfs_rust_pg_repo_promote_hardlink_to_primary, DbfsBlockTransferPlan, DbfsCopySegment,
        DbfsLogicalResizePlan, DbfsParallelWorkerPlan, DbfsPersistBlockInput,
        DbfsPersistBlockPlanEntry, DbfsPersistCrcPlanEntry, DbfsRange, DbfsReadBlock,
        DbfsReadBounds, DbfsReadSlicePlan, DbfsWriteCopyPlan, crc32_bytes,
    };

    #[test]
    fn exports_copy_plan_segments() {
        let mut out_ptr: *mut DbfsCopySegment = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_copy_plan(10, 20, 8193, 4096, 4, &mut out_ptr, &mut out_len);
        assert_eq!(status, 0);
        assert!(!out_ptr.is_null());
        assert_eq!(out_len, 3);

        let segments = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(segments[0].src, 10);
        assert_eq!(segments[0].dst, 20);
        assert_eq!(segments[0].len, 4096);
        assert_eq!(segments[2].len, 1);

        dbfs_free_copy_segments(out_ptr, out_len);
    }

    #[test]
    fn exports_copy_pack_ranges() {
        let mask = [1u8, 1, 0, 1];
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_copy_pack(
            100,
            4 * 4096,
            4096,
            mask.as_ptr(),
            mask.len(),
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let ranges = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(ranges, &[
            DbfsRange { start: 100, end: 100 + 2 * 4096 },
            DbfsRange { start: 100 + 3 * 4096, end: 100 + 4 * 4096 },
        ]);
        dbfs_free_ranges(out_ptr, out_len);
    }

    #[test]
    fn exports_persist_pad_bytes() {
        let payload = b"abc";
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_persist_pad(
            payload.as_ptr(),
            payload.len(),
            2,
            5,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let bytes = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(bytes, &[b'a', b'b', 0, 0, 0]);
        dbfs_free_bytes(out_ptr, out_len);
    }

    #[test]
    fn exports_read_assemble_bytes() {
        let block0 = b"abcd";
        let block1 = b"efgh";
        let blocks = [
            DbfsReadBlock {
                index: 0,
                ptr: block0.as_ptr(),
                len: block0.len(),
            },
            DbfsReadBlock {
                index: 1,
                ptr: block1.as_ptr(),
                len: block1.len(),
            },
        ];
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_read_assemble(
            blocks.as_ptr(),
            blocks.len(),
            0,
            1,
            1,
            7,
            4,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let bytes = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(bytes, b"bcdefg");
        dbfs_free_bytes(out_ptr, out_len);
    }

    #[test]
    fn exports_copy_dedupe_ranges() {
        let payload = b"AAAA" as &[u8];
        let current = b"AXAA" as &[u8];
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_copy_dedupe(
            0,
            payload.as_ptr(),
            payload.len(),
            current.as_ptr(),
            current.len(),
            4,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let ranges = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 4);
        dbfs_free_ranges(out_ptr, out_len);
    }

    #[test]
    fn exports_promote_hardlink_to_primary() {
        let mut out_promoted = 0u8;
        let status = dbfs_rust_pg_repo_promote_hardlink_to_primary(
            std::ptr::null_mut(),
            1,
            &mut out_promoted,
        );
        assert_eq!(status, 1);
        assert_eq!(out_promoted, 0);
    }

    #[test]
    fn exports_crc32() {
        assert_eq!(dbfs_crc32(b"123456789".as_ptr(), 9), 0xCBF4_3926);
        assert_eq!(dbfs_crc32(std::ptr::null(), 0), 0);
    }

    #[test]
    fn exports_read_sequence_step() {
        let next = dbfs_read_sequence_step(1, 128, 128, 3);
        assert_eq!(next.sequential, 1);
        assert_eq!(next.streak, 4);

        let reset = dbfs_read_sequence_step(1, 128, 64, 3);
        assert_eq!(reset.sequential, 0);
        assert_eq!(reset.streak, 0);

        let empty = dbfs_read_sequence_step(0, 0, 0, 99);
        assert_eq!(empty.sequential, 0);
        assert_eq!(empty.streak, 0);
    }

    #[test]
    fn exports_read_ahead_blocks() {
        assert_eq!(dbfs_read_ahead_blocks(2, 8, 0, 256, 0), 2);
        assert_eq!(dbfs_read_ahead_blocks(2, 8, 1, 256, 1), 8);
        assert_eq!(dbfs_read_ahead_blocks(2, 8, 3, 10, 1), 9);
        assert_eq!(dbfs_read_ahead_blocks(16, 8, 4, 4, 1), 3);
    }

    #[test]
    fn exports_sorted_contiguous_ranges() {
        let missing = [7u64, 3, 4, 10, 11, 11, 8];
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_sorted_contiguous_ranges(
            missing.as_ptr(),
            missing.len(),
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let ranges = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            ranges,
            &[
                DbfsRange { start: 3, end: 4 },
                DbfsRange { start: 7, end: 8 },
                DbfsRange { start: 10, end: 11 },
            ]
        );
        dbfs_free_ranges(out_ptr, out_len);
    }

    #[test]
    fn exports_dirty_block_ranges_plan() {
        let dirty = [7u64, 3, 4, 10, 11, 11, 8];
        let mut total_blocks = 0u64;
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_dirty_block_ranges_plan(
            65536,
            4096,
            dirty.as_ptr(),
            dirty.len(),
            &mut total_blocks,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        assert_eq!(total_blocks, 16);
        let ranges = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            ranges,
            &[
                DbfsRange { start: 3, end: 4 },
                DbfsRange { start: 7, end: 8 },
                DbfsRange { start: 10, end: 11 },
            ]
        );
        dbfs_free_ranges(out_ptr, out_len);
    }

    #[test]
    fn exports_persist_layout_plan() {
        let dirty = [7u64, 3, 4, 10, 11, 11, 8];
        let mut total_blocks = 0u64;
        let mut truncate_only = 0u8;
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_persist_layout_plan(
            65536,
            4096,
            1,
            dirty.as_ptr(),
            dirty.len(),
            &mut total_blocks,
            &mut truncate_only,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        assert_eq!(total_blocks, 16);
        assert_eq!(truncate_only, 0);
        let ranges = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            ranges,
            &[
                DbfsRange { start: 3, end: 4 },
                DbfsRange { start: 7, end: 8 },
                DbfsRange { start: 10, end: 11 },
            ]
        );
        dbfs_free_ranges(out_ptr, out_len);
    }

    #[test]
    fn exports_persist_block_plan() {
        let dirty = [7u64, 3, 4, 10, 11, 11, 8];
        let mut total_blocks = 0u64;
        let mut truncate_only = 0u8;
        let mut out_ptr: *mut DbfsPersistBlockPlanEntry = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_persist_block_plan(
            65536,
            4096,
            1,
            dirty.as_ptr(),
            dirty.len(),
            &mut total_blocks,
            &mut truncate_only,
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        assert_eq!(total_blocks, 16);
        assert_eq!(truncate_only, 0);
        let blocks = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            blocks,
            &[
                DbfsPersistBlockPlanEntry { block_index: 3, used_len: 4096 },
                DbfsPersistBlockPlanEntry { block_index: 4, used_len: 4096 },
                DbfsPersistBlockPlanEntry { block_index: 7, used_len: 4096 },
                DbfsPersistBlockPlanEntry { block_index: 8, used_len: 4096 },
                DbfsPersistBlockPlanEntry { block_index: 10, used_len: 4096 },
                DbfsPersistBlockPlanEntry { block_index: 11, used_len: 4096 },
            ]
        );
        dbfs_free_persist_blocks(out_ptr, out_len);
    }

    #[test]
    fn exports_persist_block_crc_plan() {
        let full = vec![0xABu8; 4096];
        let partial = vec![0xCDu8; 3];
        let inputs = [
            DbfsPersistBlockInput {
                block_index: 3,
                ptr: full.as_ptr(),
                len: full.len(),
                used_len: 4096,
            },
            DbfsPersistBlockInput {
                block_index: 4,
                ptr: partial.as_ptr(),
                len: partial.len(),
                used_len: 3,
            },
            DbfsPersistBlockInput {
                block_index: 7,
                ptr: full.as_ptr(),
                len: full.len(),
                used_len: 4096,
            },
        ];
        let mut out_ptr: *mut DbfsPersistCrcPlanEntry = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_persist_block_crc_plan(
            4096,
            inputs.as_ptr(),
            inputs.len(),
            &mut out_ptr,
            &mut out_len,
        );
        assert_eq!(status, 0);
        let rows = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            rows,
            &[
                DbfsPersistCrcPlanEntry {
                    block_index: 3,
                    has_crc: 1,
                    crc32: crc32_bytes(&full),
                },
                DbfsPersistCrcPlanEntry {
                    block_index: 4,
                    has_crc: 0,
                    crc32: 0,
                },
                DbfsPersistCrcPlanEntry {
                    block_index: 7,
                    has_crc: 1,
                    crc32: crc32_bytes(&full),
                },
            ]
        );
        dbfs_free_persist_crc_rows(out_ptr, out_len);
    }

    #[test]
    fn exports_logical_resize_plan() {
        assert_eq!(
            dbfs_logical_resize_plan(10, 0, 4),
            DbfsLogicalResizePlan {
                old_size: 10,
                new_size: 0,
                block_size: 4,
                old_total_blocks: 3,
                new_total_blocks: 0,
                shrinking: 1,
                has_valid_blocks: 0,
                delete_from_block: 0,
                max_valid_block: 0,
                has_partial_tail: 0,
                tail_block_index: 0,
                tail_valid_len: 0,
            }
        );
        assert_eq!(
            dbfs_logical_resize_plan(10, 6, 4),
            DbfsLogicalResizePlan {
                old_size: 10,
                new_size: 6,
                block_size: 4,
                old_total_blocks: 3,
                new_total_blocks: 2,
                shrinking: 1,
                has_valid_blocks: 1,
                delete_from_block: 2,
                max_valid_block: 1,
                has_partial_tail: 1,
                tail_block_index: 1,
                tail_valid_len: 2,
            }
        );
        assert_eq!(
            dbfs_logical_resize_plan(10, 16, 4),
            DbfsLogicalResizePlan {
                old_size: 10,
                new_size: 16,
                block_size: 4,
                old_total_blocks: 3,
                new_total_blocks: 4,
                shrinking: 0,
                has_valid_blocks: 1,
                delete_from_block: 3,
                max_valid_block: 3,
                has_partial_tail: 0,
                tail_block_index: 0,
                tail_valid_len: 0,
            }
        );
    }

    #[test]
    fn exports_read_fetch_bounds() {
        let mut out = DbfsReadBounds {
            fetch_first: 0,
            fetch_last: 0,
        };

        assert_eq!(
            dbfs_read_fetch_bounds(0, 0, 0, 2, 8, 0, 256, 0, 8, &mut out),
            1
        );
        assert_eq!(
            dbfs_read_fetch_bounds(4, 0, 0, 2, 8, 0, 256, 0, 8, &mut out),
            0
        );
        assert_eq!(out.fetch_first, 0);
        assert_eq!(out.fetch_last, 3);
        assert_eq!(
            dbfs_read_fetch_bounds(32, 2, 3, 2, 8, 1, 256, 1, 8, &mut out),
            0
        );
        assert_eq!(out.fetch_first, 2);
        assert_eq!(out.fetch_last, 11);
    }

    #[test]
    fn exports_read_slice_plan() {
        let mut out = DbfsReadSlicePlan {
            total_blocks: 0,
            fetch_first: 0,
            fetch_last: 0,
        };

        assert_eq!(
            dbfs_read_slice_plan(16, 0, 4, 4, 2, 8, 0, 256, 0, 8, &mut out),
            0
        );
        assert_eq!(
            out,
            DbfsReadSlicePlan {
                total_blocks: 4,
                fetch_first: 0,
                fetch_last: 3,
            }
        );
    }

    #[test]
    fn exports_block_transfer_plan() {
        assert_eq!(
            dbfs_block_transfer_plan(0, 4096, 4, 8, 0),
            DbfsBlockTransferPlan {
                total_blocks: 0,
                parallel: 0,
                workers: 1,
            }
        );
        assert_eq!(
            dbfs_block_transfer_plan(65536, 4096, 4, 8, 1),
            DbfsBlockTransferPlan {
                total_blocks: 16,
                parallel: 1,
                workers: 4,
            }
        );
    }

    #[test]
    fn exports_read_missing_range_worker_count() {
        assert_eq!(dbfs_read_missing_range_worker_count(1, 8, 10, 3), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 7, 3), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 8, 1), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 9, 3), 3);
        assert_eq!(dbfs_read_missing_range_worker_count(8, 8, 9, 12), 8);
    }

    #[test]
    fn exports_block_count_for_length() {
        assert_eq!(dbfs_block_count_for_length(0, 4096, 0), 0);
        assert_eq!(dbfs_block_count_for_length(0, 4096, 1), 1);
        assert_eq!(dbfs_block_count_for_length(1, 4096, 0), 1);
        assert_eq!(dbfs_block_count_for_length(4096, 4096, 0), 1);
        assert_eq!(dbfs_block_count_for_length(4097, 4096, 0), 2);
    }

    #[test]
    fn exports_write_copy_worker_count() {
        assert_eq!(dbfs_write_copy_worker_count(0, 4, 8), 1);
        assert_eq!(dbfs_write_copy_worker_count(7, 4, 8), 1);
        assert_eq!(dbfs_write_copy_worker_count(8, 1, 8), 1);
        assert_eq!(dbfs_write_copy_worker_count(8, 4, 8), 4);
        assert_eq!(dbfs_write_copy_worker_count(3, 8, 1), 3);
    }

    #[test]
    fn exports_parallel_worker_count() {
        assert_eq!(dbfs_parallel_worker_count(1, 8, 10, 3), 1);
        assert_eq!(dbfs_parallel_worker_count(4, 8, 7, 3), 1);
        assert_eq!(dbfs_parallel_worker_count(4, 8, 8, 1), 1);
        assert_eq!(dbfs_parallel_worker_count(4, 8, 9, 3), 3);
        assert_eq!(dbfs_parallel_worker_count(8, 8, 9, 12), 8);
    }

    #[test]
    fn exports_parallel_worker_plan() {
        assert_eq!(
            dbfs_parallel_worker_plan(1, 8, 10, 3),
            DbfsParallelWorkerPlan {
                parallel: 0,
                workers: 1,
            }
        );
        assert_eq!(
            dbfs_parallel_worker_plan(4, 8, 9, 3),
            DbfsParallelWorkerPlan {
                parallel: 1,
                workers: 3,
            }
        );
    }

    #[test]
    fn exports_write_copy_plan() {
        assert_eq!(
            dbfs_write_copy_plan(0, 4096, 4, 8, 1, 16, 0),
            DbfsWriteCopyPlan {
                total_blocks: 1,
                dedupe_enabled: 0,
                parallel: 0,
                workers: 1,
            }
        );
        assert_eq!(
            dbfs_write_copy_plan(65536, 4096, 4, 8, 1, 16, 0),
            DbfsWriteCopyPlan {
                total_blocks: 16,
                dedupe_enabled: 1,
                parallel: 1,
                workers: 4,
            }
        );
    }

}
