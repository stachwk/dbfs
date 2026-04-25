from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import DbfsBlockTransferPlan, DbfsWriteCopyPlan, StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        ((0, 4, 8), 1),
        ((7, 4, 8), 1),
        ((8, 1, 8), 1),
        ((8, 4, 8), 4),
        ((3, 8, 1), 3),
    ]

    for args, expected in cases:
        result = lib.dbfs_write_copy_worker_count(*args)
        assert int(result) == expected, (args, int(result), expected)

    plan_cases = [
        ((0, 4096, 4, 8), (1, False, False, 1)),
        ((4096, 4096, 4, 8), (1, False, False, 1)),
        ((65536, 4096, 4, 8), (16, True, True, 4)),
    ]

    for args, expected in plan_cases:
        length, block_size, workers_write, workers_write_min_blocks = args
        result = lib.dbfs_write_copy_plan(length, block_size, workers_write, workers_write_min_blocks, 1, 16, 0)
        assert int(result.total_blocks) == expected[0], (args, int(result.total_blocks), expected)
        assert int(result.dedupe_enabled) == int(expected[1]), (args, int(result.dedupe_enabled), expected)
        assert int(result.parallel) == int(expected[2]), (args, int(result.parallel), expected)
        assert int(result.workers) == expected[3], (args, int(result.workers), expected)

    dedupe_cases = [
        ((0, 4096, 1, 1), (1, False, False, 1)),
        ((4096, 4096, 1, 1), (1, False, False, 1)),
        ((65536, 4096, 1, 1), (16, True, False, 1)),
        ((65536, 4096, 4, 8), (16, True, True, 4)),
    ]

    for args, expected in dedupe_cases:
        length, block_size, workers_write, workers_write_min_blocks = args
        result = lib.dbfs_write_copy_plan(length, block_size, workers_write, workers_write_min_blocks, 1, 16, 0)
        assert int(result.total_blocks) == expected[0], (args, int(result.total_blocks), expected)
        assert int(result.dedupe_enabled) == int(expected[1]), (args, int(result.dedupe_enabled), expected)
        assert int(result.parallel) == int(expected[2]), (args, int(result.parallel), expected)
        assert int(result.workers) == expected[3], (args, int(result.workers), expected)

    transfer_cases = [
        ((0, 4096, 4, 8, True), (1, False, 1)),
        ((4096, 4096, 4, 8, True), (1, False, 1)),
        ((65536, 4096, 4, 8, True), (16, True, 4)),
    ]

    for args, expected in transfer_cases:
        result = lib.dbfs_block_transfer_plan(*args)
        assert int(result.total_blocks) == expected[0], (args, int(result.total_blocks), expected)
        assert int(result.parallel) == int(expected[1]), (args, int(result.parallel), expected)
        assert int(result.workers) == expected[2], (args, int(result.workers), expected)

    print("OK rust-hotpath-write-copy-worker-count")


if __name__ == "__main__":
    main()
