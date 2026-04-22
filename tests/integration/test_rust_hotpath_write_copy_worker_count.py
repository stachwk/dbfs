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
        ((0, 4, 8), 1),
        ((7, 4, 8), 1),
        ((8, 1, 8), 1),
        ((8, 4, 8), 4),
        ((3, 8, 1), 3),
    ]

    for args, expected in cases:
        result = storage._write_copy_worker_count_rust_ffi(*args)
        assert result is not None, args
        assert result == expected, (args, result, expected)

    plan_cases = [
        ((0, 4096, 4, 8), (1, False, False, 1)),
        ((4096, 4096, 4, 8), (1, False, False, 1)),
        ((65536, 4096, 4, 8), (16, False, True, 4)),
    ]

    for args, expected in plan_cases:
        result = storage._write_copy_plan_rust_ffi(*args)
        assert result is not None, args
        assert result == expected, (args, result, expected)

    dedupe_cases = [
        ((0, 4096), (1, False)),
        ((4096, 4096), (1, False)),
        ((65536, 4096), (16, False)),
    ]

    for args, expected in dedupe_cases:
        result = storage._write_copy_dedupe_plan_rust_ffi(*args)
        assert result is not None, args
        assert result == expected, (args, result, expected)

    transfer_cases = [
        ((0, 4096, 4, 8, True), (1, False, 1)),
        ((4096, 4096, 4, 8, True), (1, False, 1)),
        ((65536, 4096, 4, 8, True), (16, True, 4)),
    ]

    for args, expected in transfer_cases:
        result = storage._block_transfer_plan_rust_ffi(*args)
        assert result is not None, args
        assert result == expected, (args, result, expected)

    print("OK rust-hotpath-write-copy-worker-count")


if __name__ == "__main__":
    main()
