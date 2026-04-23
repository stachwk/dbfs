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
        ((10, 0, 4), {"shrinking": True, "has_valid_blocks": False, "old_total_blocks": 3, "new_total_blocks": 0, "delete_from_block": 0, "max_valid_block": 0, "has_partial_tail": False, "tail_block_index": 0, "tail_valid_len": 0}),
        ((10, 6, 4), {"shrinking": True, "has_valid_blocks": True, "old_total_blocks": 3, "new_total_blocks": 2, "delete_from_block": 2, "max_valid_block": 1, "has_partial_tail": True, "tail_block_index": 1, "tail_valid_len": 2}),
        ((10, 16, 4), {"shrinking": False, "has_valid_blocks": True, "old_total_blocks": 3, "new_total_blocks": 4, "delete_from_block": 3, "max_valid_block": 3, "has_partial_tail": False, "tail_block_index": 0, "tail_valid_len": 0}),
    ]

    for (old_size, new_size, block_size), expected in cases:
        plan = storage.python_to_rust_hotpath_logical_resize_plan(old_size, new_size, block_size)
        assert int(plan.old_size) == old_size
        assert int(plan.new_size) == new_size
        assert int(plan.block_size) == block_size
        assert bool(plan.shrinking) is expected["shrinking"], (old_size, new_size, block_size, plan)
        assert bool(plan.has_valid_blocks) is expected["has_valid_blocks"], (old_size, new_size, block_size, plan)
        assert int(plan.old_total_blocks) == expected["old_total_blocks"], (old_size, new_size, block_size, plan)
        assert int(plan.new_total_blocks) == expected["new_total_blocks"], (old_size, new_size, block_size, plan)
        assert int(plan.delete_from_block) == expected["delete_from_block"], (old_size, new_size, block_size, plan)
        assert int(plan.max_valid_block) == expected["max_valid_block"], (old_size, new_size, block_size, plan)
        assert bool(plan.has_partial_tail) is expected["has_partial_tail"], (old_size, new_size, block_size, plan)
        assert int(plan.tail_block_index) == expected["tail_block_index"], (old_size, new_size, block_size, plan)
        assert int(plan.tail_valid_len) == expected["tail_valid_len"], (old_size, new_size, block_size, plan)

    print("OK rust-hotpath-logical-resize-plan")


if __name__ == "__main__":
    main()
