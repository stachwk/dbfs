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
        ((0, 0, 4096), 0),
        ((1, 0, 4096), 1),
        ((4096, 0, 4096), 4096),
        ((4100, 1, 4096), 4),
        ((8192, 3, 4096), 0),
    ]

    for args, expected in cases:
        file_size, block_index, block_size = args
        result = lib.dbfs_dirty_block_size(file_size, block_index, block_size)
        assert int(result) == expected, (args, int(result), expected)

    print("OK rust-hotpath-dirty-block-size")


if __name__ == "__main__":
    main()
