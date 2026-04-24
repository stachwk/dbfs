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
        ([], []),
        ([1], [(1, 1)]),
        ([2, 3, 4, 7, 8, 10], [(2, 4), (7, 8), (10, 10)]),
        ([7, 3, 4, 10, 11, 11, 8], [(3, 4), (7, 8), (10, 11)]),
        ([5, 7, 8, 9, 12, 13], [(5, 5), (7, 9), (12, 13)]),
    ]

    for missing, expected in cases:
        result = storage.python_to_rust_hotpath_sorted_contiguous_ranges(missing)
        assert result is not None, missing
        assert result == expected, (missing, result, expected)
        assert storage._missing_block_ranges(missing) == expected

    print("OK rust-hotpath-missing-ranges")


if __name__ == "__main__":
    main()
