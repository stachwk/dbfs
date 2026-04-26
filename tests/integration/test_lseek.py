#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def main() -> None:
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-lseek-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"lseek_{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"seekable payload"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            fd = os.open(file_path, os.O_RDONLY)
            try:
                end_offset = os.lseek(fd, 0, os.SEEK_END)
                assert end_offset == len(payload), end_offset

                current_offset = os.lseek(fd, -1, os.SEEK_CUR)
                assert current_offset == len(payload) - 1, current_offset

                reset_offset = os.lseek(fd, 0, os.SEEK_SET)
                assert reset_offset == 0, reset_offset
            finally:
                os.close(fd)

            print("OK lseek/mount")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
