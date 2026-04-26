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
    payload = b"hello dbfs"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-itest-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"itest_{suffix}"
            file_path = dir_path / "hello.txt"

            dir_path.mkdir()
            file_path.write_bytes(payload)
            assert file_path.read_bytes() == payload, f"read returned {file_path.read_bytes()!r}, expected {payload!r}"
            print("OK mkdir/create/write/read")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
