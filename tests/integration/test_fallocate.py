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
    payload = b"dbfs"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-fallocate-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"fallocate_{suffix}"
            file_path = dir_path / "prealloc.txt"

            dir_path.mkdir()
            file_path.write_bytes(payload)
            with file_path.open("r+b") as fh:
                if not hasattr(os, "posix_fallocate"):
                    raise AssertionError("os.posix_fallocate is not available")
                os.posix_fallocate(fh.fileno(), 16, 32)

            stat = file_path.stat()
            assert stat.st_size == 48, stat
            data = file_path.read_bytes()
            assert data[:4] == payload, data
            assert data[4:] == b"\x00" * 44, data

            print("OK fallocate")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
