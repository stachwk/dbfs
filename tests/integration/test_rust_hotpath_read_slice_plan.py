from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import DbfsReadSlicePlan, StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        ((16, 0, 4, 4, 2, 8, 0, 256, False, 8), (4, 0, 3)),
    ]

    for (
        file_size,
        offset,
        size,
        block_size,
        read_ahead_blocks,
        sequential_read_ahead_blocks,
        streak,
        read_cache_limit_blocks,
        sequential,
        small_file_threshold_blocks,
    ), expected in cases:
        out = DbfsReadSlicePlan()
        rc = lib.dbfs_read_slice_plan(
            file_size,
            offset,
            size,
            block_size,
            read_ahead_blocks,
            sequential_read_ahead_blocks,
            streak,
            read_cache_limit_blocks,
            1 if sequential else 0,
            small_file_threshold_blocks,
            ctypes.byref(out),
        )
        assert rc == 0, (file_size, offset, size, block_size, rc)
        assert (out.total_blocks, out.fetch_first, out.fetch_last) == expected, (
            (file_size, offset, size, block_size, read_ahead_blocks, sequential_read_ahead_blocks, streak, read_cache_limit_blocks, sequential, small_file_threshold_blocks),
            out,
            expected,
        )

    print("OK rust-hotpath-read-slice-plan")


if __name__ == "__main__":
    main()
