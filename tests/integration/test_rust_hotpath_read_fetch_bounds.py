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
        ((0, 0, 0, False, 0), None),
        ((4, 0, 0, False, 0), (0, 3)),
        ((32, 2, 3, True, 1), (2, 11)),
        ((32, 2, 3, True, 4), (2, 31)),
    ]

    for args, expected in cases:
        result = storage._read_fetch_bounds_rust_ffi(*args)
        if expected is None:
            assert result is None, (args, result)
        else:
            assert result == expected, (args, result, expected)

    print("OK rust-hotpath-read-fetch-bounds")


if __name__ == "__main__":
    main()
