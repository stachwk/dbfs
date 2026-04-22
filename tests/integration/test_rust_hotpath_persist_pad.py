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


def python_persist_pad(payload: bytes, used_len: int, block_size: int) -> bytes:
    block_size = max(1, int(block_size))
    used_len = min(int(used_len), block_size)
    out = bytes(payload[:used_len])
    if len(out) < block_size:
        out += b"\x00" * (block_size - len(out))
    return out


def run_rust_pad(payload: bytes, used_len: int, block_size: int) -> bytes:
    completed = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(RUST_MANIFEST),
            "--bin",
            "dbfs-persist-pad",
            "--",
            str(int(used_len)),
            str(int(block_size)),
        ],
        input=payload,
        check=True,
        capture_output=True,
    )
    return completed.stdout


def main() -> None:
    cases = [
        (b"abc", 2, 5),
        (b"abcdef", 6, 4),
        (b"", 0, 1),
        (b"payload", 3, 8),
    ]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_persist_pad=True))
    assert storage.rust_hotpath_persist_pad_bin_path() is not None, "expected built Rust helper binary"

    for payload, used_len, block_size in cases:
        python_result = python_persist_pad(payload, used_len, block_size)
        rust_result = run_rust_pad(payload, used_len, block_size)
        assert rust_result == python_result, (
            f"pad mismatch for {(payload, used_len, block_size)!r}: "
            f"python={python_result!r} rust={rust_result!r}"
        )

        runtime_result = storage._persist_block_payload(payload, used_len, block_size)
        assert runtime_result == python_result, (
            f"runtime pad mismatch for {(payload, used_len, block_size)!r}: "
            f"python={python_result!r} runtime={runtime_result!r}"
        )

    print("OK rust-hotpath-persist-pad")


if __name__ == "__main__":
    main()
