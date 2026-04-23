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

    full = bytes(range(256)) * 16
    partial = b"abc"
    plan = storage.python_to_rust_hotpath_persist_block_crc_plan(
        4096,
        [
            (3, full, 4096),
            (4, partial, 3),
            (7, full, 4096),
        ],
    )
    assert plan is not None
    assert plan == [
        (3, True, storage.python_to_rust_hotpath_crc32(full)),
        (4, False, 0),
        (7, True, storage.python_to_rust_hotpath_crc32(full)),
    ], plan

    print("OK rust-hotpath-persist-block-crc-plan")


if __name__ == "__main__":
    main()
