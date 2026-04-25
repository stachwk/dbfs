from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import DbfsRange, StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        ((65536, 4096, True, [7, 3, 4, 10, 11, 11, 8]), (16, False, [(3, 4), (7, 8), (10, 11)])),
        ((4096, 4096, True, []), (1, True, [])),
    ]

    for (file_size, block_size, truncate_pending, dirty_blocks), expected in cases:
        dirty = [int(block_index) for block_index in dirty_blocks]
        dirty_array = (ctypes.c_uint64 * len(dirty))(*dirty) if dirty else None
        out_total_blocks = ctypes.c_uint64()
        out_truncate_only = ctypes.c_ubyte()
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()
        rc = lib.dbfs_persist_layout_plan(
            ctypes.c_uint64(int(file_size)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_ubyte(1 if truncate_pending else 0),
            dirty_array,
            ctypes.c_size_t(len(dirty)),
            ctypes.byref(out_total_blocks),
            ctypes.byref(out_truncate_only),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        assert rc == 0, rc
        try:
            ranges = [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)
        assert int(out_total_blocks.value) == expected[0], (file_size, block_size, truncate_pending, dirty_blocks, expected)
        assert bool(out_truncate_only.value) is expected[1], (file_size, block_size, truncate_pending, dirty_blocks, expected)
        assert ranges == expected[2], (file_size, block_size, truncate_pending, dirty_blocks, ranges, expected)

    print("OK rust-hotpath-persist-layout-plan")


if __name__ == "__main__":
    main()
