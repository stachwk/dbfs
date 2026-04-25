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
        ([], []),
        ([1], [(1, 1)]),
        ([2, 3, 4, 7, 8, 10], [(2, 4), (7, 8), (10, 10)]),
        ([7, 3, 4, 10, 11, 11, 8], [(3, 4), (7, 8), (10, 11)]),
        ([5, 7, 8, 9, 12, 13], [(5, 5), (7, 9), (12, 13)]),
    ]

    for missing, expected in cases:
        values = [int(block_index) for block_index in missing]
        if not values:
            result = []
        else:
            values_array = (ctypes.c_uint64 * len(values))(*values)
            out_ptr = ctypes.POINTER(DbfsRange)()
            out_len = ctypes.c_size_t()
            rc = lib.dbfs_sorted_contiguous_ranges(
                values_array,
                ctypes.c_size_t(len(values)),
                ctypes.byref(out_ptr),
                ctypes.byref(out_len),
            )
            assert rc == 0, (missing, rc)
            try:
                result = [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
            finally:
                lib.dbfs_free_ranges(out_ptr, out_len)
        assert result == expected, (missing, result, expected)
        assert storage._missing_block_ranges(missing) == expected

    print("OK rust-hotpath-missing-ranges")


if __name__ == "__main__":
    main()
