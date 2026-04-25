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
        ((0, 4096, False), 0),
        ((0, 4096, True), 1),
        ((1, 4096, False), 1),
        ((4096, 4096, False), 1),
        ((4097, 4096, False), 2),
    ]

    for args, expected in cases:
        length, block_size, minimum_one = args
        result = lib.dbfs_block_count_for_length(length, block_size, 1 if minimum_one else 0)
        assert int(result) == expected, (args, int(result), expected)

    print("OK rust-hotpath-block-count")


if __name__ == "__main__":
    main()
