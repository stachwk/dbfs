pub mod ffi;

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
        assemble_read_slice, copy_segments, pack_changed_copy_pairs, pack_changed_ranges,
        pad_block_bytes,
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
    fn returns_empty_for_reversed_fetch_range() {
        let blocks = vec![(3, b"block3".to_vec())];
        assert!(assemble_read_slice(5, 3, 0, 12, 4, &blocks).is_empty());
    }
}
