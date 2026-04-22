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
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    cases = [
        ((1, 8, 10, 3), 1),
        ((4, 8, 7, 3), 1),
        ((4, 8, 8, 1), 1),
        ((4, 8, 9, 3), 3),
        ((8, 8, 9, 12), 8),
    ]

    for args, expected in cases:
        result = storage._parallel_worker_count_rust_ffi(*args)
        assert result is not None, args
        assert result == expected, (args, result, expected)
        plan = storage._parallel_worker_plan_rust_ffi(*args)
        assert plan is not None, args
        expected_plan = (expected > 1, expected)
        assert plan == expected_plan, (args, plan, expected_plan)

    print("OK rust-hotpath-parallel-worker-count")


if __name__ == "__main__":
    main()
