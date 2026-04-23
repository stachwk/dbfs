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

    plan = storage.python_to_rust_hotpath_persist_block_plan(
        65536,
        4096,
        True,
        [7, 3, 4, 10, 11, 11, 8],
    )
    assert plan is not None
    total_blocks, truncate_only, blocks = plan
    assert total_blocks == 16, plan
    assert truncate_only is False, plan
    assert blocks == [
        (3, 4096),
        (4, 4096),
        (7, 4096),
        (8, 4096),
        (10, 4096),
        (11, 4096),
    ], plan

    truncate_only_plan = storage.python_to_rust_hotpath_persist_block_plan(4096, 4096, True, [])
    assert truncate_only_plan is not None
    total_blocks, truncate_only, blocks = truncate_only_plan
    assert total_blocks == 1, truncate_only_plan
    assert truncate_only is True, truncate_only_plan
    assert blocks == [], truncate_only_plan

    print("OK rust-hotpath-persist-block-plan")


if __name__ == "__main__":
    main()
