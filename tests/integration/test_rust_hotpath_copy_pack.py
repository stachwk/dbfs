from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUST_MANIFEST = ROOT / "rust_hotpath" / "Cargo.toml"


def python_pack_changed_ranges(off_out: int, block_size: int, changed_mask: list[bool]):
    block_size = max(1, int(block_size))
    ranges = []
    run_start = None

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
        ranges.append((run_start, off_out + len(changed_mask) * block_size))

    return ranges


def run_rust_pack(off_out: int, block_size: int, changed_mask: list[bool]):
    mask_arg = ",".join("1" if bit else "0" for bit in changed_mask)
    completed = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(RUST_MANIFEST),
            "--bin",
            "copy-pack",
            "--",
            str(off_out),
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
        (100, 4096, [True, True, False, True, False, False, True]),
        (0, 1, []),
        (7, 8192, [True]),
        (12, 512, [False, True, True, True, False]),
    ]

    for case in cases:
        python_ranges = python_pack_changed_ranges(*case)
        rust_ranges = run_rust_pack(*case)
        assert rust_ranges == python_ranges, (
            f"pack mismatch for {case}: "
            f"python={python_ranges!r} rust={rust_ranges!r}"
        )

    print("OK rust-hotpath-copy-pack")


if __name__ == "__main__":
    main()
