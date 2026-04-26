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


def main() -> None:
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-metadata-cache-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"metadata-cache-{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"metadata-cache\n"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            attrs_first = file_path.stat()
            attrs_second = file_path.stat()
            assert attrs_first.st_size == attrs_second.st_size, (attrs_first, attrs_second)

            statfs_first = os.statvfs(mountpoint)
            statfs_second = os.statvfs(mountpoint)
            assert statfs_first.f_blocks == statfs_second.f_blocks, (statfs_first, statfs_second)
            assert statfs_first.f_bsize == statfs_second.f_bsize, (statfs_first, statfs_second)

            with file_path.open("ab") as fh:
                fh.write(b"cache-bust\n")

            attrs_third = file_path.stat()
            assert attrs_third.st_size == len(payload) + len(b"cache-bust\n"), attrs_third

            statfs_third = os.statvfs(mountpoint)
            assert statfs_third.f_blocks >= statfs_first.f_blocks, (statfs_first, statfs_third)

            print("OK metadata-cache")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
