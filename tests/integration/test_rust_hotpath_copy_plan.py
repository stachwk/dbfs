from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

from dbfs_storage import StorageSupport


ROOT = Path(__file__).resolve().parents[2]
RUST_MANIFEST = ROOT / "rust_hotpath" / "Cargo.toml"
RUST_BIN = [
    "cargo",
    "run",
    "--quiet",
    "--manifest-path",
    str(RUST_MANIFEST),
    "--bin",
    "copy-plan",
    "--",
]


def run_rust_plan(off_in: int, off_out: int, length: int, block_size: int, workers: int):
    completed = subprocess.run(
        [*RUST_BIN, str(off_in), str(off_out), str(length), str(block_size), str(workers)],
        check=True,
        capture_output=True,
        text=True,
    )
    segments = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        src, dst, chunk_len = line.split(",")
        segments.append((int(src), int(dst), int(chunk_len)))
    return segments


def main() -> None:
    support = StorageSupport(SimpleNamespace())
    cases = [
        (0, 0, 0, 4096, 4),
        (3, 5, 1, 4096, 4),
        (10, 20, 8193, 4096, 4),
        (123, 456, 16384, 4096, 3),
        (7, 9, 32768, 8192, 2),
    ]

    for case in cases:
        python_segments = support._copy_segments(*case)
        rust_segments = run_rust_plan(*case)
        assert rust_segments == python_segments, (
            f"copy plan mismatch for {case}: "
            f"python={python_segments!r} rust={rust_segments!r}"
        )

    print("OK rust-hotpath-copy-plan")


if __name__ == "__main__":
    main()
