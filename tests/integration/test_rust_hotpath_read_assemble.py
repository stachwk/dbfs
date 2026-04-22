#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import StorageSupport

RUST_MANIFEST = ROOT / "rust_hotpath" / "Cargo.toml"


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


def run_rust_assemble(fetch_first, fetch_last, offset, end_offset, block_size, blocks):
    input_data = "\n".join(f"{block_index}|{block.hex()}" for block_index, block in blocks)
    completed = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(RUST_MANIFEST),
            "--bin",
            "read-assemble",
            "--",
            str(int(fetch_first)),
            str(int(fetch_last)),
            str(int(offset)),
            str(int(end_offset)),
            str(int(block_size)),
        ],
        input=input_data.encode(),
        check=True,
        capture_output=True,
    )
    return completed.stdout


def main() -> None:
    cases = [
        (0, 2, 1, 11, 4, [(0, b"abcd"), (1, b"efgh"), (2, b"ijkl")]),
        (2, 4, 9, 19, 4, [(2, b"2345"), (4, b"6789")]),
        (1, 1, 9, 12, 8, [(1, b"abcdefgh")]),
    ]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_read_assemble=True))
    assert storage.rust_hotpath_read_assemble_bin_path() is not None, "expected built Rust helper binary"

    for case in cases:
        python_result = python_assemble(*case)
        rust_result = run_rust_assemble(*case)
        assert rust_result == python_result, (
            f"read-assemble mismatch for {case!r}: "
            f"python={python_result!r} rust={rust_result!r}"
        )

    print("OK rust-hotpath-read-assemble")


if __name__ == "__main__":
    main()
