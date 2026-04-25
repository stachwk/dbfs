from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
import ctypes

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import StorageSupport


def main() -> None:
    storage = StorageSupport(SimpleNamespace())
    lib = storage._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        (b"", 0x00000000),
        (b"123456789", 0xCBF43926),
        (b"dbfs-hotpath-crc32", 0x94AA2FE0),
        (bytes(range(256)), 0x29058C73),
    ]

    for payload, expected in cases:
        buf = ctypes.create_string_buffer(payload, len(payload))
        rust_value = lib.dbfs_crc32(ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)), len(payload))
        assert int(rust_value) == expected, (payload, int(rust_value), expected)

    print("OK rust-hotpath-crc32")


if __name__ == "__main__":
    main()
