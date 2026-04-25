from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import DbfsReadBounds, StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        ((0, 0, 0, 2, 8, 0, 256, False, 8), 1, None),
        ((4, 0, 0, 2, 8, 0, 256, False, 8), 0, (0, 3)),
        ((32, 2, 3, 2, 8, 1, 256, True, 1), 0, (2, 11)),
        ((32, 2, 3, 2, 8, 4, 256, True, 1), 0, (2, 31)),
    ]

    for args, expected_rc, expected in cases:
        total_blocks, requested_first, requested_last, read_ahead_blocks, sequential_read_ahead_blocks, streak, read_cache_limit_blocks, sequential, small_file_threshold_blocks = args
        out = DbfsReadBounds()
        rc = lib.dbfs_read_fetch_bounds(
            total_blocks,
            requested_first,
            requested_last,
            read_ahead_blocks,
            sequential_read_ahead_blocks,
            streak,
            read_cache_limit_blocks,
            1 if sequential else 0,
            small_file_threshold_blocks,
            ctypes.byref(out),
        )
        assert rc == expected_rc, (args, rc, expected_rc)
        if expected is not None:
            assert (out.fetch_first, out.fetch_last) == expected, (args, out, expected)

    print("OK rust-hotpath-read-fetch-bounds")


if __name__ == "__main__":
    main()
