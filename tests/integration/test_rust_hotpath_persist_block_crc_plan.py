from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import DbfsPersistBlockInput, DbfsPersistCrcPlanEntry, StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    full = bytes(range(256)) * 16
    partial = b"abc"
    payloads = [
        (3, full, 4096),
        (4, partial, 3),
        (7, full, 4096),
    ]
    buffers = []
    inputs = []
    for block_index, data, used_len in payloads:
        payload = bytes(data)
        buffer = ctypes.create_string_buffer(payload, len(payload))
        buffers.append(buffer)
        inputs.append(
            DbfsPersistBlockInput(
                block_index=ctypes.c_uint64(int(block_index)),
                ptr=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)),
                len=ctypes.c_size_t(len(payload)),
                used_len=ctypes.c_uint64(int(used_len)),
            )
        )

    inputs_array = (DbfsPersistBlockInput * len(inputs))(*inputs)
    out_ptr = ctypes.POINTER(DbfsPersistCrcPlanEntry)()
    out_len = ctypes.c_size_t()
    rc = lib.dbfs_persist_block_crc_plan(
        ctypes.c_uint64(4096),
        inputs_array,
        ctypes.c_size_t(len(inputs)),
        ctypes.byref(out_ptr),
        ctypes.byref(out_len),
    )
    assert rc == 0, rc
    try:
        plan = [
            (int(out_ptr[i].block_index), bool(out_ptr[i].has_crc), int(out_ptr[i].crc32))
            for i in range(out_len.value)
        ]
    finally:
        lib.dbfs_free_persist_crc_rows(out_ptr, out_len)

    full_buffer = ctypes.create_string_buffer(full, len(full))
    full_crc = lib.dbfs_crc32(ctypes.cast(full_buffer, ctypes.POINTER(ctypes.c_ubyte)), len(full))
    assert plan == [
        (3, True, full_crc),
        (4, False, 0),
        (7, True, full_crc),
    ], plan

    print("OK rust-hotpath-persist-block-crc-plan")


if __name__ == "__main__":
    main()
