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
    let total_blocks = ((length + block_size - 1) / block_size).max(1);
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

#[cfg(test)]
mod tests {
    use super::copy_segments;

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
}
