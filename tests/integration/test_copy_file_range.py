#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def main():
    suffix = uuid.uuid4().hex[:8]
    src_payload = b"ABCDEFGHIJ"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-copy-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"copy_{suffix}"
            src_path = dir_path / "source.txt"
            dst_path = dir_path / "dest.txt"

            dir_path.mkdir()
            src_path.write_bytes(src_payload)
            dst_path.write_bytes(b"")

            with src_path.open("rb") as src_fh, dst_path.open("r+b") as dst_fh:
                copied = os.copy_file_range(src_fh.fileno(), dst_fh.fileno(), 5, offset_src=2, offset_dst=4)
            assert copied == 5, copied

            assert dst_path.stat().st_size == 9, dst_path.stat()
            data = dst_path.read_bytes()
            assert data == b"\x00\x00\x00\x00CDEFG", data
            assert src_path.read_bytes() == src_payload, src_path.read_bytes()
            print("OK copy_file_range")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
