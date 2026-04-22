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


def python_pack_changed_ranges(
    off_out: int,
    total_len: int,
    block_size: int,
    changed_mask: list[bool],
):
    block_size = max(1, int(block_size))
    ranges = []
    run_start = None
    copy_end = off_out + total_len

    for block_index, changed in enumerate(changed_mask):
        block_start = off_out + block_index * block_size
        if changed:
            if run_start is None:
                run_start = block_start
            continue

        if run_start is not None:
            ranges.append((run_start, block_start))
            run_start = None

    if run_start is not None:
        ranges.append((run_start, copy_end))

    return ranges


def run_rust_pack(
    off_out: int,
    total_len: int,
    block_size: int,
    changed_mask: list[bool],
):
    mask_arg = ",".join("1" if bit else "0" for bit in changed_mask)
    completed = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(RUST_MANIFEST),
            "--bin",
            "dbfs-copy-pack",
            "--",
            str(off_out),
            str(total_len),
            str(block_size),
            mask_arg,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    ranges = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        start, end = line.split(",")
        ranges.append((int(start), int(end)))
    return ranges


def main() -> None:
    cases = [
        (100, 7 * 4096, 4096, [True, True, False, True, False, False, True]),
        (0, 0, 1, []),
        (7, 1024, 8192, [True]),
        (12, 5 * 512, 512, [False, True, True, True, False]),
    ]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_copy_pack=True))
    assert storage.rust_hotpath_copy_pack_bin_path() is not None, "expected built Rust helper binary"

    for case in cases:
        python_ranges = python_pack_changed_ranges(*case)
        rust_ranges = run_rust_pack(*case)
        assert rust_ranges == python_ranges, (
            f"pack mismatch for {case}: "
            f"python={python_ranges!r} rust={rust_ranges!r}"
        )
        runtime_ranges = storage._pack_changed_copy_ranges(*case)
        assert runtime_ranges == python_ranges, (
            f"runtime pack mismatch for {case}: "
            f"python={python_ranges!r} runtime={runtime_ranges!r}"
        )

    print("OK rust-hotpath-copy-pack")


if __name__ == "__main__":
    main()
