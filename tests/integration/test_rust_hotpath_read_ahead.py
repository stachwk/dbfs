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
        ((2, 8, 0, 256, False), 2),
        ((2, 8, 1, 256, True), 8),
        ((2, 8, 3, 10, True), 9),
        ((16, 8, 4, 4, True), 3),
    ]

    for args, expected in cases:
        result = lib.dbfs_read_ahead_blocks(*args)
        assert int(result) == expected, (args, int(result), expected)

    print("OK rust-hotpath-read-ahead")


if __name__ == "__main__":
    main()
