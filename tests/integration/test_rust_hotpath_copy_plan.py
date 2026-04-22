from __future__ import annotations

import os
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
    "dbfs-copy-plan",
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

    original_helper = os.environ.get("DBFS_RUST_HOTPATH_COPY_PLAN_BIN")
    original_lib = os.environ.get("DBFS_RUST_HOTPATH_LIB")
    os.environ["DBFS_RUST_HOTPATH_COPY_PLAN_BIN"] = str(ROOT / "rust_hotpath" / "target" / "debug" / "dbfs-copy-plan")
    os.environ["DBFS_RUST_HOTPATH_LIB"] = str(
        ROOT / "rust_hotpath" / "target" / "debug" / "libdbfs_rust_hotpath.so"
    )
    original_run = subprocess.run
    original_loader = StorageSupport._load_rust_hotpath_lib

    ffi_support = StorageSupport(SimpleNamespace(rust_hotpath_copy_plan=True))
    assert ffi_support._copy_segments(7, 9, 42, 4096, 3) == [(7, 9, 42)]

    class Completed:
        def __init__(self, stdout: str):
            self.stdout = stdout

    seen_args = {}

    def fake_run(cmd, check, capture_output, text):
        seen_args["cmd"] = cmd
        seen_args["check"] = check
        seen_args["capture_output"] = capture_output
        seen_args["text"] = text
        return Completed("111,222,333\n444,555,666\n")

    try:
        StorageSupport._load_rust_hotpath_lib = lambda self: None
        subprocess.run = fake_run
        support = StorageSupport(SimpleNamespace(rust_hotpath_copy_plan=True))
        segments = support._copy_segments(7, 9, 42, 4096, 3)
        assert segments == [(111, 222, 333), (444, 555, 666)], segments
        assert seen_args["cmd"][:1] == [str(ROOT / "rust_hotpath" / "target" / "debug" / "dbfs-copy-plan")], seen_args
        assert seen_args["cmd"][1:] == ["7", "9", "42", "4096", "3"], seen_args
        assert seen_args["check"] is True, seen_args
        assert seen_args["capture_output"] is True, seen_args
        assert seen_args["text"] is True, seen_args
    finally:
        StorageSupport._load_rust_hotpath_lib = original_loader
        subprocess.run = original_run
        if original_helper is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_PLAN_BIN", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_PLAN_BIN"] = original_helper
        if original_lib is None:
            os.environ.pop("DBFS_RUST_HOTPATH_LIB", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_LIB"] = original_lib

    print("OK rust-hotpath-copy-plan")


if __name__ == "__main__":
    main()
