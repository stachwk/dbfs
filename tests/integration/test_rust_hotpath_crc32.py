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
        (b"", 0x00000000),
        (b"123456789", 0xCBF43926),
        (b"dbfs-hotpath-crc32", 0x94AA2FE0),
        (bytes(range(256)), 0x29058C73),
    ]

    for payload, expected in cases:
        rust_value = storage.python_to_rust_hotpath_crc32(payload)
        assert rust_value is not None, payload
        assert rust_value == expected, (payload, rust_value, expected)

    print("OK rust-hotpath-crc32")


if __name__ == "__main__":
    main()
