use std::panic;
use std::slice;

use crate::{
    assemble_read_slice, contiguous_ranges, copy_segments, crc32_bytes, pack_changed_ranges,
    pad_block_bytes, read_ahead_blocks, read_fetch_bounds, read_missing_range_worker_count,
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
pub extern "C" fn dbfs_contiguous_missing_ranges(
    missing_ptr: *const u64,
    missing_len: usize,
    out_ptr: *mut *mut DbfsRange,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| unsafe {
        if missing_len == 0 {
            return write_boxed_output(Vec::<DbfsRange>::new(), out_ptr, out_len);
        }
        if missing_ptr.is_null() {
            return 1;
        }
        let missing = slice::from_raw_parts(missing_ptr, missing_len);
        let ranges = contiguous_ranges(missing)
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
        dbfs_contiguous_missing_ranges, dbfs_copy_dedupe, dbfs_copy_pack, dbfs_copy_plan,
        dbfs_crc32, dbfs_free_bytes, dbfs_free_copy_segments, dbfs_free_ranges,
        dbfs_persist_pad, dbfs_read_assemble, dbfs_read_ahead_blocks, dbfs_read_fetch_bounds,
        dbfs_read_missing_range_worker_count, dbfs_read_sequence_step, DbfsCopySegment,
        DbfsRange, DbfsReadBlock, DbfsReadBounds,
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
    fn exports_contiguous_missing_ranges() {
        let missing = [2u64, 3, 4, 7, 8, 10];
        let mut out_ptr: *mut DbfsRange = std::ptr::null_mut();
        let mut out_len: usize = 0;

        let status = dbfs_contiguous_missing_ranges(
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
                DbfsRange { start: 2, end: 4 },
                DbfsRange { start: 7, end: 8 },
                DbfsRange { start: 10, end: 10 },
            ]
        );
        dbfs_free_ranges(out_ptr, out_len);
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
    fn exports_read_missing_range_worker_count() {
        assert_eq!(dbfs_read_missing_range_worker_count(1, 8, 10, 3), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 7, 3), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 8, 1), 1);
        assert_eq!(dbfs_read_missing_range_worker_count(4, 8, 9, 3), 3);
        assert_eq!(dbfs_read_missing_range_worker_count(8, 8, 9, 12), 8);
    }
}
