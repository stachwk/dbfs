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
        ((0, 0, 1, 4, False, 0), None),
        ((16, 0, 4, 4, False, 0), (0, 3)),
        ((64, 8, 8, 4, True, 1), (2, 11)),
    ]

    for (file_size, offset, size, block_size, sequential, streak), expected in cases:
        result = storage.python_to_rust_hotpath_read_slice_plan(file_size, offset, size, block_size, sequential, streak)
        if expected is None:
            assert result is None, (file_size, offset, size, block_size, sequential, streak, result)
            continue

        assert result is not None, (file_size, offset, size, block_size, sequential, streak)
        total_blocks, fetch_first, fetch_last = result
        assert total_blocks > 0, result
        assert (fetch_first, fetch_last) == expected, (
            (file_size, offset, size, block_size, sequential, streak),
            result,
            expected,
        )

    print("OK rust-hotpath-read-slice-plan")


if __name__ == "__main__":
    main()
