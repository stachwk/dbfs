use dbfs_rust_hotpath::{
    assemble_read_slice, block_count_for_length, block_transfer_plan, copy_segments, crc32_bytes,
    dirty_block_size, logical_resize_plan, pack_changed_copy_pairs, pack_changed_ranges,
    parallel_worker_count, parallel_worker_plan, pad_block_bytes, persist_block_plan,
    persist_layout_plan, read_ahead_blocks, read_fetch_bounds, read_missing_range_worker_count,
    read_slice_plan, sorted_contiguous_ranges, write_copy_dedupe_plan, write_copy_plan,
    write_copy_worker_count, LogicalResizePlan, PersistBlockPlanEntry,
};

#[test]
fn crc32_and_chunking_helpers_match_expected_values() {
    assert_eq!(crc32_bytes(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32_bytes(b""), 0);
    assert_eq!(copy_segments(0, 0, 0, 4096, 4), Vec::<(u64, u64, u64)>::new());
    assert_eq!(copy_segments(3, 5, 1, 4096, 4), vec![(3, 5, 1)]);
    assert_eq!(
        copy_segments(10, 20, 8193, 4096, 4),
        vec![(10, 20, 4096), (4106, 4116, 4096), (8202, 8212, 1)]
    );
}

#[test]
fn range_packing_helpers_match_expected_values() {
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
            (100 + 6 * 4096, 100 + 7 * 4096),
        ]
    );

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
    assert_eq!(
        sorted_contiguous_ranges(&[7, 3, 4, 10, 11, 11, 8]),
        vec![(3, 4), (7, 8), (10, 11)]
    );
}

#[test]
fn read_helpers_match_expected_values() {
    assert_eq!(read_ahead_blocks(2, 8, 3, 10, true), 9);
    assert_eq!(read_fetch_bounds(0, 0, 0, 2, 8, 0, 256, false, 8), None);
    assert_eq!(read_fetch_bounds(4, 0, 0, 2, 8, 0, 256, false, 8), Some((0, 3)));
    assert_eq!(read_fetch_bounds(32, 2, 3, 2, 8, 1, 256, true, 8), Some((2, 11)));
    assert_eq!(read_fetch_bounds(32, 2, 3, 16, 8, 4, 4, true, 8), Some((2, 6)));
    assert_eq!(read_slice_plan(0, 0, 1, 4, 2, 8, 0, 256, false, 8), None);
    assert_eq!(read_slice_plan(16, 0, 4, 4, 2, 8, 0, 256, false, 8), Some((4, 0, 3)));
    assert_eq!(read_slice_plan(64, 8, 8, 4, 2, 8, 1, 256, true, 8), Some((16, 2, 11)));
    assert_eq!(read_missing_range_worker_count(1, 8, 10, 3), 1);
    assert_eq!(read_missing_range_worker_count(4, 8, 7, 3), 1);
    assert_eq!(read_missing_range_worker_count(4, 8, 8, 1), 1);
    assert_eq!(read_missing_range_worker_count(4, 8, 9, 3), 3);
    assert_eq!(read_missing_range_worker_count(8, 8, 9, 12), 8);
}

#[test]
fn read_workers_parallel_plan_matches_expected_values() {
    assert_eq!(read_missing_range_worker_count(4, 2, 8, 2), 2);
    assert_eq!(read_missing_range_worker_count(4, 2, 8, 1), 1);
    assert_eq!(read_missing_range_worker_count(1, 2, 8, 2), 1);
    assert_eq!(read_missing_range_worker_count(4, 8, 8, 2), 2);
}

#[test]
fn read_ahead_sequence_plan_matches_expected_values() {
    assert_eq!(read_ahead_blocks(0, 2, 1, 8, true), 2);
    assert_eq!(read_ahead_blocks(0, 2, 2, 8, true), 4);
    assert_eq!(read_ahead_blocks(0, 2, 4, 8, true), 7);
    assert_eq!(read_ahead_blocks(2, 8, 3, 10, false), 2);
    assert_eq!(read_fetch_bounds(16, 0, 0, 0, 2, 1, 8, true, 0), Some((0, 2)));
    assert_eq!(read_fetch_bounds(16, 4, 7, 0, 2, 2, 8, true, 0), Some((4, 11)));
}

#[test]
fn read_cache_benchmark_plan_matches_expected_values() {
    let iterations = std::env::var("READ_CACHE_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(12);
    let blocks = std::env::var("READ_CACHE_BENCH_BLOCKS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(384);
    assert!(iterations > 0);
    assert!(blocks > 0);
    assert_eq!(read_ahead_blocks(0, 2, 1, blocks, true), 2.min(blocks.saturating_sub(1)));
    assert_eq!(read_fetch_bounds(blocks, 0, 0, 0, 2, 1, blocks, true, 0), Some((0, 2.min(blocks.saturating_sub(1)))));
    assert_eq!(read_fetch_bounds(blocks, 1, 1, 0, 2, 2, blocks, true, 0), Some((1, (1 + 4).min(blocks.saturating_sub(1)))));
}

#[test]
fn size_and_resize_helpers_match_expected_values() {
    assert_eq!(block_count_for_length(0, 4096, false), 0);
    assert_eq!(block_count_for_length(0, 4096, true), 1);
    assert_eq!(block_count_for_length(1, 4096, false), 1);
    assert_eq!(block_count_for_length(4096, 4096, false), 1);
    assert_eq!(block_count_for_length(4097, 4096, false), 2);
    assert_eq!(dirty_block_size(0, 0, 4096), 0);
    assert_eq!(dirty_block_size(1, 0, 4096), 1);
    assert_eq!(dirty_block_size(4096, 0, 4096), 4096);
    assert_eq!(dirty_block_size(4100, 1, 4096), 4);
    assert_eq!(dirty_block_size(8192, 3, 4096), 0);
    assert_eq!(
        logical_resize_plan(10, 0, 4),
        LogicalResizePlan {
            old_size: 10,
            new_size: 0,
            block_size: 4,
            old_total_blocks: 3,
            new_total_blocks: 0,
            shrinking: true,
            has_valid_blocks: false,
            delete_from_block: 0,
            max_valid_block: 0,
            has_partial_tail: false,
            tail_block_index: 0,
            tail_valid_len: 0,
        }
    );
    assert_eq!(
        logical_resize_plan(10, 6, 4),
        LogicalResizePlan {
            old_size: 10,
            new_size: 6,
            block_size: 4,
            old_total_blocks: 3,
            new_total_blocks: 2,
            shrinking: true,
            has_valid_blocks: true,
            delete_from_block: 2,
            max_valid_block: 1,
            has_partial_tail: true,
            tail_block_index: 1,
            tail_valid_len: 2,
        }
    );
    assert_eq!(
        logical_resize_plan(10, 16, 4),
        LogicalResizePlan {
            old_size: 10,
            new_size: 16,
            block_size: 4,
            old_total_blocks: 3,
            new_total_blocks: 4,
            shrinking: false,
            has_valid_blocks: true,
            delete_from_block: 3,
            max_valid_block: 3,
            has_partial_tail: false,
            tail_block_index: 0,
            tail_valid_len: 0,
        }
    );
}

#[test]
fn persist_and_copy_plans_match_expected_values() {
    let layout = persist_layout_plan(65536, 4096, true, &[7, 3, 4, 10, 11, 11, 8]);
    assert_eq!(layout.total_blocks, 16);
    assert!(!layout.truncate_only);
    assert_eq!(layout.ordered_dirty_ranges, vec![(3, 4), (7, 8), (10, 11)]);

    let truncate_only = persist_layout_plan(4096, 4096, true, &[]);
    assert_eq!(truncate_only.total_blocks, 1);
    assert!(truncate_only.truncate_only);
    assert!(truncate_only.ordered_dirty_ranges.is_empty());

    let blocks = persist_block_plan(65536, 4096, true, &[7, 3, 4, 10, 11, 11, 8]);
    assert_eq!(blocks.total_blocks, 16);
    assert!(!blocks.truncate_only);
    assert_eq!(
        blocks.blocks,
        vec![
            PersistBlockPlanEntry { block_index: 3, used_len: 4096 },
            PersistBlockPlanEntry { block_index: 4, used_len: 4096 },
            PersistBlockPlanEntry { block_index: 7, used_len: 4096 },
            PersistBlockPlanEntry { block_index: 8, used_len: 4096 },
            PersistBlockPlanEntry { block_index: 10, used_len: 4096 },
            PersistBlockPlanEntry { block_index: 11, used_len: 4096 },
        ]
    );
    assert_eq!(block_transfer_plan(0, 4096, 4, 8, false), (0, false, 1));
    assert_eq!(block_transfer_plan(4096, 4096, 4, 8, false), (1, false, 1));
    assert_eq!(block_transfer_plan(65536, 4096, 4, 8, true), (16, true, 4));
    assert_eq!(write_copy_worker_count(0, 4, 8), 1);
    assert_eq!(write_copy_worker_count(7, 4, 8), 1);
    assert_eq!(write_copy_worker_count(8, 1, 8), 1);
    assert_eq!(write_copy_worker_count(8, 4, 8), 4);
    assert_eq!(write_copy_worker_count(3, 8, 1), 3);
    assert_eq!(write_copy_plan(0, 4096, 4, 8, true, 16, 0), (1, false, false, 1));
    assert_eq!(write_copy_plan(4096, 4096, 4, 8, true, 16, 0), (1, false, false, 1));
    assert_eq!(write_copy_plan(65536, 4096, 4, 8, true, 16, 0), (16, true, true, 4));
    assert_eq!(write_copy_plan(65536, 4096, 1, 8, true, 16, 0), (16, true, false, 1));
    assert_eq!(write_copy_dedupe_plan(0, 4096, true, 16, 0), (1, false));
    assert_eq!(write_copy_dedupe_plan(4096, 4096, true, 16, 0), (1, false));
    assert_eq!(write_copy_dedupe_plan(65536, 4096, true, 16, 0), (16, true));
    assert_eq!(write_copy_dedupe_plan(65536, 4096, false, 16, 0), (16, false));
    assert_eq!(parallel_worker_count(1, 8, 10, 3), 1);
    assert_eq!(parallel_worker_count(4, 8, 7, 3), 1);
    assert_eq!(parallel_worker_count(4, 8, 8, 1), 1);
    assert_eq!(parallel_worker_count(4, 8, 9, 3), 3);
    assert_eq!(parallel_worker_count(8, 8, 9, 12), 8);
    assert_eq!(parallel_worker_plan(1, 8, 10, 3), (false, 1));
    assert_eq!(parallel_worker_plan(4, 8, 7, 3), (false, 1));
    assert_eq!(parallel_worker_plan(4, 8, 8, 1), (false, 1));
    assert_eq!(parallel_worker_plan(4, 8, 9, 3), (true, 3));
    assert_eq!(parallel_worker_plan(8, 8, 9, 12), (true, 8));
}

#[test]
fn write_workers_parallel_copy_plan_matches_expected_values() {
    assert_eq!(write_copy_worker_count(1, 4, 2), 1);
    assert_eq!(write_copy_worker_count(7, 4, 8), 1);
    assert_eq!(write_copy_worker_count(8, 4, 2), 4);
    assert_eq!(write_copy_worker_count(16, 4, 2), 4);

    assert_eq!(write_copy_plan(1 * 4096, 4096, 4, 2, true, 1, 16), (1, true, false, 1));
    assert_eq!(write_copy_plan(8 * 4096, 4096, 4, 2, true, 1, 16), (8, true, true, 4));
    assert_eq!(write_copy_plan(16 * 4096, 4096, 4, 2, true, 1, 16), (16, true, true, 4));
    assert_eq!(write_copy_plan(16 * 4096, 4096, 4, 2, false, 1, 16), (16, false, true, 4));
    assert_eq!(write_copy_dedupe_plan(1 * 4096, 4096, true, 1, 16), (1, true));
    assert_eq!(write_copy_dedupe_plan(16 * 4096, 4096, true, 1, 16), (16, true));
    assert_eq!(write_copy_dedupe_plan(16 * 4096, 4096, false, 1, 16), (16, false));
}

#[test]
fn write_worker_thresholds_block_size_plan_matches_expected_values() {
    assert_eq!(write_copy_plan(1 * 4096, 4096, 2, 2, false, 0, 0), (1, false, false, 1));
    assert_eq!(write_copy_plan(2 * 4096, 4096, 2, 2, false, 0, 0), (2, false, true, 2));
    assert_eq!(write_copy_plan(4 * 4096, 4096, 2, 2, false, 0, 0), (4, false, true, 2));
    assert_eq!(write_copy_plan(4 * 4096, 4096, 1, 2, false, 0, 0), (4, false, false, 1));
}

#[test]
fn assemble_and_pad_helpers_match_expected_values() {
    assert_eq!(pad_block_bytes(b"abc", 2, 5), vec![b'a', b'b', 0, 0, 0]);
    let blocks = vec![(2, b"block2".to_vec()), (3, b"block3".to_vec()), (5, b"block5".to_vec())];
    assert_eq!(
        assemble_read_slice(2, 5, 2 * 6 + 1, 5 * 6 - 2, 6, &blocks),
        b"lock2block3\x00\x00\x00\x00".to_vec()
    );
    let aligned = vec![(1, b"abcdefgh".to_vec())];
    assert_eq!(assemble_read_slice(1, 1, 9, 12, 8, &aligned), b"bcd".to_vec());
    let reversed = vec![(3, b"block3".to_vec())];
    assert!(assemble_read_slice(5, 3, 0, 12, 4, &reversed).is_empty());
}
