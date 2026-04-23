#!/usr/bin/env python3

from __future__ import annotations

from types import SimpleNamespace

from dbfs_storage import StorageSupport


def python_assemble(fetch_first, fetch_last, offset, end_offset, block_size, blocks):
    block_size = max(1, int(block_size))
    block_map = {index: data for index, data in blocks}
    chunks = []
    for block_index in range(fetch_first, fetch_last + 1):
        block = block_map.get(block_index, b"\x00" * block_size)
        chunks.append(block)
    raw = b"".join(chunks)
    start_offset = offset - (fetch_first * block_size)
    end_offset_in_raw = start_offset + (end_offset - offset)
    return raw[start_offset:end_offset_in_raw]


def main() -> None:
    cases = [
        (0, 2, 0, 12, 4, [(0, b"abcd"), (1, b"efgh"), (2, b"ijkl")]),
        (2, 4, 8, 20, 4, [(2, b"2345"), (4, b"6789")]),
        (1, 1, 8, 16, 8, [(1, b"abcdefgh")]),
    ]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_read_assemble=True))
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    for case in cases:
        fetch_first, fetch_last, offset, end_offset, block_size, blocks = case
        block_map = {index: data for index, data in blocks}
        storage.owner.block_size = block_size
        storage._fetch_block_range = lambda file_id, first_block, last_block, block_map=block_map: block_map
        python_result = python_assemble(*case)
        runtime_result = storage._assemble_blocks(99, fetch_first, fetch_last)
        assert runtime_result == python_result, (
            f"read-assemble mismatch for {case!r}: "
            f"python={python_result!r} runtime={runtime_result!r}"
        )

    print("OK rust-hotpath-read-assemble")


if __name__ == "__main__":
    main()
