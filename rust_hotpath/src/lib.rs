use std::sync::OnceLock;

pub mod ffi;

fn crc32_table() -> &'static [u32; 256] {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        let poly = 0xEDB8_8320u32;
        let mut i = 0u32;
        while i < 256 {
            let mut crc = i;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 != 0 { (crc >> 1) ^ poly } else { crc >> 1 };
                bit += 1;
            }
            table[i as usize] = crc;
            i += 1;
        }
        table
    })
}

pub fn crc32_bytes(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    let table = crc32_table();
    for &byte in data {
        let idx = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[idx];
    }
    !crc
}

pub fn copy_segments(
    off_in: u64,
    off_out: u64,
    length: u64,
    block_size: u64,
    workers: u64,
) -> Vec<(u64, u64, u64)> {
    if length == 0 {
        return Vec::new();
    }

    let block_size = block_size.max(1);
    let workers = workers.max(1);
    let total_blocks = 1 + (length - 1) / block_size;
    let worker_count = workers.min(total_blocks).max(1);
    let blocks_per_worker = ((total_blocks + worker_count - 1) / worker_count).max(1);
    let bytes_per_worker = blocks_per_worker.saturating_mul(block_size);

    let mut segments = Vec::new();
    let mut src_cursor = off_in;
    let mut dst_cursor = off_out;
    let mut remaining = length;

    while remaining > 0 {
        let chunk_len = remaining.min(bytes_per_worker);
        segments.push((src_cursor, dst_cursor, chunk_len));
        src_cursor = src_cursor.saturating_add(chunk_len);
        dst_cursor = dst_cursor.saturating_add(chunk_len);
        remaining -= chunk_len;
    }

    segments
}

pub fn pack_changed_ranges(
    off_out: u64,
    total_len: u64,
    block_size: u64,
    changed_mask: &[bool],
) -> Vec<(u64, u64)> {
    let block_size = block_size.max(1);
    let mut ranges = Vec::new();
    let mut run_start: Option<u64> = None;
    let copy_end = off_out.saturating_add(total_len);

    for (block_index, changed) in changed_mask.iter().copied().enumerate() {
        let block_start = off_out.saturating_add((block_index as u64).saturating_mul(block_size));

        if changed {
            if run_start.is_none() {
                run_start = Some(block_start);
            }
            continue;
        }

        if let Some(start) = run_start.take() {
            ranges.push((start, block_start));
        }
    }

    if let Some(start) = run_start {
        let end = copy_end.max(off_out);
        ranges.push((start, end));
    }

    ranges
}

pub fn pack_changed_copy_pairs(
    off_out: u64,
    total_len: u64,
    block_size: u64,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> Vec<(u64, u64)> {
    let changed_mask: Vec<bool> = pairs.iter().map(|(payload, current)| payload != current).collect();
    pack_changed_ranges(off_out, total_len, block_size, &changed_mask)
}

pub fn contiguous_ranges(values: &[u64]) -> Vec<(u64, u64)> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut start = values[0];
    let mut end = values[0];
    for &value in &values[1..] {
        if value == end.saturating_add(1) {
            end = value;
            continue;
        }
        ranges.push((start, end));
        start = value;
        end = value;
    }
    ranges.push((start, end));
    ranges
}

pub fn read_ahead_blocks(
    read_ahead_blocks: u64,
    sequential_read_ahead_blocks: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: bool,
) -> u64 {
    let mut effective = read_ahead_blocks;
    if sequential {
        let dynamic_ahead = sequential_read_ahead_blocks.saturating_mul(streak.max(1));
        effective = effective.max(dynamic_ahead);
    }

    let max_allowed = read_cache_limit_blocks.saturating_sub(1);
    effective.min(max_allowed)
}

pub fn read_fetch_bounds(
    total_blocks: u64,
    requested_first: u64,
    requested_last: u64,
    read_ahead_blocks_value: u64,
    sequential_read_ahead_blocks_value: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: bool,
    small_file_threshold_blocks: u64,
) -> Option<(u64, u64)> {
    if total_blocks == 0 {
        return None;
    }

    if total_blocks <= small_file_threshold_blocks {
        return Some((0, total_blocks.saturating_sub(1)));
    }

    let read_ahead = read_ahead_blocks(
        read_ahead_blocks_value,
        sequential_read_ahead_blocks_value,
        streak,
        read_cache_limit_blocks,
        sequential,
    );
    let fetch_first = requested_first;
    let fetch_last = requested_last
        .saturating_add(read_ahead)
        .min(total_blocks.saturating_sub(1));
    Some((fetch_first, fetch_last))
}

pub fn read_slice_plan(
    file_size: u64,
    offset: u64,
    size: u64,
    block_size: u64,
    read_ahead_blocks_value: u64,
    sequential_read_ahead_blocks_value: u64,
    streak: u64,
    read_cache_limit_blocks: u64,
    sequential: bool,
    small_file_threshold_blocks: u64,
) -> Option<(u64, u64, u64)> {
    if size == 0 || offset >= file_size {
        return None;
    }

    let block_size = block_size.max(1);
    let total_blocks = block_count_for_length(file_size, block_size, false);
    if total_blocks == 0 {
        return None;
    }

    let end_offset = offset.saturating_add(size).min(file_size);
    let requested_first = offset / block_size;
    let requested_last = end_offset
        .saturating_sub(1)
        .checked_div(block_size)
        .unwrap_or(0)
        .max(requested_first);

    let (fetch_first, fetch_last) = read_fetch_bounds(
        total_blocks,
        requested_first,
        requested_last,
        read_ahead_blocks_value,
        sequential_read_ahead_blocks_value,
        streak,
        read_cache_limit_blocks,
        sequential,
        small_file_threshold_blocks,
    )?;

    Some((total_blocks, fetch_first, fetch_last))
}

pub fn read_missing_range_worker_count(
    workers_read: u64,
    workers_read_min_blocks: u64,
    missing_len: u64,
    contiguous_ranges_len: u64,
) -> u64 {
    parallel_worker_count(
        workers_read,
        workers_read_min_blocks,
        missing_len,
        contiguous_ranges_len,
    )
}

pub fn block_count_for_length(length: u64, block_size: u64, minimum_one: bool) -> u64 {
    if length == 0 {
        return if minimum_one { 1 } else { 0 };
    }
    let block_size = block_size.max(1);
    let count = 1 + (length - 1) / block_size;
    if minimum_one { count.max(1) } else { count }
}

pub fn write_copy_worker_count(total_blocks: u64, workers_write: u64, workers_write_min_blocks: u64) -> u64 {
    parallel_worker_count(workers_write, workers_write_min_blocks, total_blocks, total_blocks)
}

pub fn write_copy_plan(
    length: u64,
    block_size: u64,
    workers_write: u64,
    workers_write_min_blocks: u64,
    copy_dedupe_enabled: bool,
    copy_dedupe_min_blocks: u64,
    copy_dedupe_max_blocks: u64,
    ) -> (u64, bool, bool, u64) {
    let block_size = block_size.max(1);
    let total_blocks = block_count_for_length(length, block_size, true);
    let dedupe_enabled = copy_dedupe_enabled
        && total_blocks >= copy_dedupe_min_blocks.max(1)
        && (copy_dedupe_max_blocks == 0 || total_blocks <= copy_dedupe_max_blocks);
    let (parallel, workers) = parallel_worker_plan(
        workers_write,
        workers_write_min_blocks,
        total_blocks,
        total_blocks,
    );
    (total_blocks, dedupe_enabled, parallel, workers)
}

pub fn write_copy_dedupe_plan(
    length: u64,
    block_size: u64,
    copy_dedupe_enabled: bool,
    copy_dedupe_min_blocks: u64,
    copy_dedupe_max_blocks: u64,
) -> (u64, bool) {
    let block_size = block_size.max(1);
    let total_blocks = block_count_for_length(length, block_size, true);
    let dedupe_enabled = copy_dedupe_enabled
        && total_blocks >= copy_dedupe_min_blocks.max(1)
        && (copy_dedupe_max_blocks == 0 || total_blocks <= copy_dedupe_max_blocks);
    (total_blocks, dedupe_enabled)
}

pub fn parallel_worker_count(
    requested_workers: u64,
    minimum_items_for_parallel: u64,
    total_items: u64,
    parallel_groups: u64,
) -> u64 {
    if requested_workers <= 1 || total_items < minimum_items_for_parallel || parallel_groups <= 1 {
        return 1;
    }

    requested_workers.min(parallel_groups).max(1)
}

pub fn parallel_worker_plan(
    requested_workers: u64,
    minimum_items_for_parallel: u64,
    total_items: u64,
    parallel_groups: u64,
) -> (bool, u64) {
    let workers = parallel_worker_count(
        requested_workers,
        minimum_items_for_parallel,
        total_items,
        parallel_groups,
    );
    (workers > 1, workers)
}

pub fn pad_block_bytes(payload: &[u8], used_len: u64, block_size: u64) -> Vec<u8> {
    let block_size = block_size.max(1) as usize;
    let used_len = used_len.min(block_size as u64) as usize;
    let copy_len = payload.len().min(used_len);

    let mut out = Vec::with_capacity(block_size);
    out.extend_from_slice(&payload[..copy_len]);
    if out.len() < block_size {
        out.resize(block_size, 0);
    }
    out
}

pub fn assemble_read_slice(
    fetch_first: u64,
    fetch_last: u64,
    offset: u64,
    end_offset: u64,
    block_size: u64,
    blocks: &[(u64, Vec<u8>)],
) -> Vec<u8> {
    if fetch_first > fetch_last {
        return Vec::new();
    }

    let block_size = block_size.max(1) as usize;
    let start_offset = offset.saturating_sub(fetch_first.saturating_mul(block_size as u64)) as usize;
    let requested_len = end_offset.saturating_sub(offset) as usize;
    let total_blocks = fetch_last.saturating_sub(fetch_first).saturating_add(1) as usize;
    let mut joined = Vec::with_capacity(total_blocks.saturating_mul(block_size));

    let mut by_index = std::collections::BTreeMap::new();
    for (index, data) in blocks {
        by_index.insert(*index, data);
    }

    for block_index in fetch_first..=fetch_last {
        if let Some(data) = by_index.get(&block_index) {
            joined.extend_from_slice(data);
        } else {
            joined.resize(joined.len() + block_size, 0);
        }
    }

    let end_offset_in_raw = start_offset.saturating_add(requested_len).min(joined.len());
    joined[start_offset.min(joined.len())..end_offset_in_raw].to_vec()
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_read_slice, block_count_for_length, copy_segments, pack_changed_copy_pairs,
        pack_changed_ranges, pad_block_bytes, parallel_worker_count, parallel_worker_plan,
        read_ahead_blocks, read_fetch_bounds, read_missing_range_worker_count, read_slice_plan,
        write_copy_dedupe_plan, write_copy_plan, write_copy_worker_count,
    };

    #[test]
    fn returns_empty_for_zero_length() {
        assert!(copy_segments(0, 0, 0, 4096, 4).is_empty());
    }

    #[test]
    fn matches_small_single_chunk() {
        assert_eq!(copy_segments(3, 5, 1, 4096, 4), vec![(3, 5, 1)]);
    }

    #[test]
    fn splits_large_transfers_into_worker_chunks() {
        assert_eq!(
            copy_segments(10, 20, 8193, 4096, 4),
            vec![(10, 20, 4096), (4106, 4116, 4096), (8202, 8212, 1)]
        );
    }

    #[test]
    fn handles_extreme_lengths_without_overflow() {
        assert_eq!(
            copy_segments(10, 20, u64::MAX, u64::MAX, 1),
            vec![(10, 20, u64::MAX)]
        );
    }

    #[test]
    fn packs_changed_ranges_into_contiguous_segments() {
        assert_eq!(
            pack_changed_ranges(
                100,
                7 * 4096,
                4096,
                &[true, true, false, true, false, false, true]
            ),
            vec![
                (100, 100 + 2 * 4096),
                (100 + 3 * 4096, 100 + 4 * 4096),
                (100 + 6 * 4096, 100 + 7 * 4096)
            ]
        );
    }

    #[test]
    fn packs_changed_copy_pairs_into_contiguous_segments() {
        let pairs = vec![
            (b"same".to_vec(), b"same".to_vec()),
            (b"diff".to_vec(), b"DIFF".to_vec()),
            (b"diff2".to_vec(), b"DIFF2".to_vec()),
            (b"same2".to_vec(), b"same2".to_vec()),
        ];
        assert_eq!(
            pack_changed_copy_pairs(100, 4 * 4096, 4096, &pairs),
            vec![(100 + 1 * 4096, 100 + 3 * 4096)]
        );
    }

    #[test]
    fn pads_block_bytes_with_zeros() {
        assert_eq!(
            pad_block_bytes(b"abc", 2, 5),
            vec![b'a', b'b', 0, 0, 0]
        );
    }

    #[test]
    fn assembles_requested_read_slice() {
        let blocks = vec![
            (2, b"block2".to_vec()),
            (3, b"block3".to_vec()),
            (5, b"block5".to_vec()),
        ];
        assert_eq!(
            assemble_read_slice(2, 5, 2 * 6 + 1, 5 * 6 - 2, 6, &blocks),
            b"lock2block3\x00\x00\x00\x00".to_vec()
        );
        let aligned = vec![(1, b"abcdefgh".to_vec())];
        assert_eq!(
            assemble_read_slice(1, 1, 9, 12, 8, &aligned),
            b"bcd".to_vec()
        );
    }

    #[test]
    fn plans_read_fetch_bounds() {
        assert_eq!(read_fetch_bounds(0, 0, 0, 2, 8, 0, 256, false, 8), None);
        assert_eq!(read_fetch_bounds(4, 0, 0, 2, 8, 0, 256, false, 8), Some((0, 3)));
        assert_eq!(read_fetch_bounds(32, 2, 3, 2, 8, 1, 256, true, 8), Some((2, 11)));
        assert_eq!(read_fetch_bounds(32, 2, 3, 16, 8, 4, 4, true, 8), Some((2, 6)));
        assert_eq!(read_ahead_blocks(2, 8, 3, 10, true), 9);
    }

    #[test]
    fn plans_read_slice_plan() {
        assert_eq!(read_slice_plan(0, 0, 1, 4, 2, 8, 0, 256, false, 8), None);
        assert_eq!(read_slice_plan(16, 0, 4, 4, 2, 8, 0, 256, false, 8), Some((4, 0, 3)));
        assert_eq!(read_slice_plan(64, 8, 8, 4, 2, 8, 1, 256, true, 8), Some((16, 2, 11)));
    }

    #[test]
    fn plans_missing_range_worker_count() {
        assert_eq!(read_missing_range_worker_count(1, 8, 10, 3), 1);
        assert_eq!(read_missing_range_worker_count(4, 8, 7, 3), 1);
        assert_eq!(read_missing_range_worker_count(4, 8, 8, 1), 1);
        assert_eq!(read_missing_range_worker_count(4, 8, 9, 3), 3);
        assert_eq!(read_missing_range_worker_count(8, 8, 9, 12), 8);
    }

    #[test]
    fn counts_blocks_for_length() {
        assert_eq!(block_count_for_length(0, 4096, false), 0);
        assert_eq!(block_count_for_length(0, 4096, true), 1);
        assert_eq!(block_count_for_length(1, 4096, false), 1);
        assert_eq!(block_count_for_length(4096, 4096, false), 1);
        assert_eq!(block_count_for_length(4097, 4096, false), 2);
    }

    #[test]
    fn plans_write_copy_worker_count() {
        assert_eq!(write_copy_worker_count(0, 4, 8), 1);
        assert_eq!(write_copy_worker_count(7, 4, 8), 1);
        assert_eq!(write_copy_worker_count(8, 1, 8), 1);
        assert_eq!(write_copy_worker_count(8, 4, 8), 4);
        assert_eq!(write_copy_worker_count(3, 8, 1), 3);
    }

    #[test]
    fn plans_write_copy_plan() {
        assert_eq!(write_copy_plan(0, 4096, 4, 8, true, 16, 0), (1, false, false, 1));
        assert_eq!(write_copy_plan(4096, 4096, 4, 8, true, 16, 0), (1, false, false, 1));
        assert_eq!(write_copy_plan(65536, 4096, 4, 8, true, 16, 0), (16, true, true, 4));
        assert_eq!(write_copy_plan(65536, 4096, 1, 8, true, 16, 0), (16, true, false, 1));
    }

    #[test]
    fn plans_write_copy_dedupe_plan() {
        assert_eq!(write_copy_dedupe_plan(0, 4096, true, 16, 0), (1, false));
        assert_eq!(write_copy_dedupe_plan(4096, 4096, true, 16, 0), (1, false));
        assert_eq!(write_copy_dedupe_plan(65536, 4096, true, 16, 0), (16, true));
        assert_eq!(write_copy_dedupe_plan(65536, 4096, false, 16, 0), (16, false));
    }

    #[test]
    fn plans_parallel_worker_count() {
        assert_eq!(parallel_worker_count(1, 8, 10, 3), 1);
        assert_eq!(parallel_worker_count(4, 8, 7, 3), 1);
        assert_eq!(parallel_worker_count(4, 8, 8, 1), 1);
        assert_eq!(parallel_worker_count(4, 8, 9, 3), 3);
        assert_eq!(parallel_worker_count(8, 8, 9, 12), 8);
    }

    #[test]
    fn plans_parallel_worker_plan() {
        assert_eq!(parallel_worker_plan(1, 8, 10, 3), (false, 1));
        assert_eq!(parallel_worker_plan(4, 8, 7, 3), (false, 1));
        assert_eq!(parallel_worker_plan(4, 8, 8, 1), (false, 1));
        assert_eq!(parallel_worker_plan(4, 8, 9, 3), (true, 3));
        assert_eq!(parallel_worker_plan(8, 8, 9, 12), (true, 8));
    }

    #[test]
    fn returns_empty_for_reversed_fetch_range() {
        let blocks = vec![(3, b"block3".to_vec())];
        assert!(assemble_read_slice(5, 3, 0, 12, 4, &blocks).is_empty());
    }
}
