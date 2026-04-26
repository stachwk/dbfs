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
    dir_path = f"destroy_{suffix}"
    file_name = "payload.txt"
    payload = b"destroy flush payload"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-destroy-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_mount = mountpoint / dir_path
            file_mount = dir_mount / file_name

            dir_mount.mkdir()
            file_mount.write_bytes(payload)

            launcher.stop()
            launcher.start(str(mountpoint))
            try:
                assert file_mount.stat().st_size == len(payload)
                assert file_mount.read_bytes() == payload
            finally:
                launcher.stop()
        finally:
            try:
                file_mount.unlink()
            except Exception:
                pass
            try:
                dir_mount.rmdir()
            except Exception:
                pass

    print("OK destroy/cleanup")


if __name__ == "__main__":
    main()
