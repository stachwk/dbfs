#!/usr/bin/env python3

from __future__ import annotations

import ctypes
import fcntl
import os
import sys
import tempfile
import termios
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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-ioctl-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"ioctl_{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"ioctl payload"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            fd = os.open(file_path, os.O_RDONLY)
            try:
                out = ctypes.c_int(-1)
                fcntl.ioctl(fd, termios.FIONREAD, out, True)
                assert out.value == len(payload), out.value
            finally:
                os.close(fd)

            print("OK ioctl/mount")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
