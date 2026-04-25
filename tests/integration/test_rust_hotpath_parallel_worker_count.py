from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        ((1, 8, 10, 3), 1),
        ((4, 8, 7, 3), 1),
        ((4, 8, 8, 1), 1),
        ((4, 8, 9, 3), 3),
        ((8, 8, 9, 12), 8),
    ]

    for args, expected in cases:
        requested_workers, minimum_items_for_parallel, total_items, parallel_groups = args
        result = lib.dbfs_parallel_worker_count(
            requested_workers,
            minimum_items_for_parallel,
            total_items,
            parallel_groups,
        )
        assert int(result) == expected, (args, int(result), expected)
        plan = lib.dbfs_parallel_worker_plan(
            requested_workers,
            minimum_items_for_parallel,
            total_items,
            parallel_groups,
        )
        expected_parallel = 1 if expected > 1 else 0
        assert int(plan.parallel) == expected_parallel, (args, int(plan.parallel), expected_parallel)
        assert int(plan.workers) == expected, (args, int(plan.workers), expected)

    print("OK rust-hotpath-parallel-worker-count")


if __name__ == "__main__":
    main()
