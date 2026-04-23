#!/usr/bin/env python3

from __future__ import annotations

from types import SimpleNamespace

from dbfs_storage import StorageSupport


def python_persist_pad(payload: bytes, used_len: int, block_size: int) -> bytes:
    block_size = max(1, int(block_size))
    used_len = min(int(used_len), block_size)
    out = bytes(payload[:used_len])
    if len(out) < block_size:
        out += b"\x00" * (block_size - len(out))
    return out


def main() -> None:
    cases = [
        (b"abc", 2, 5),
        (b"abcdef", 6, 4),
        (b"", 0, 1),
        (b"payload", 3, 8),
    ]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_persist_pad=True))
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    for payload, used_len, block_size in cases:
        python_result = python_persist_pad(payload, used_len, block_size)
        runtime_result = storage._persist_block_payload(payload, used_len, block_size)
        assert runtime_result == python_result, (
            f"pad mismatch for {(payload, used_len, block_size)!r}: "
            f"python={python_result!r} runtime={runtime_result!r}"
        )

    print("OK rust-hotpath-persist-pad")


if __name__ == "__main__":
    main()
