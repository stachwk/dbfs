#!/usr/bin/env python3

from __future__ import annotations

import errno
import os
import stat
import tempfile
import sys
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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-mknod-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"mknod_{suffix}"
            fifo_path = dir_path / "pipe"
            chr_path = dir_path / "tty-like"

            dir_path.mkdir()
            os.mkfifo(fifo_path, 0o644)
            chr_rdev = os.makedev(1, 7) if hasattr(os, "makedev") else 0
            os.mknod(chr_path, stat.S_IFCHR | 0o600, chr_rdev)

            attrs = fifo_path.stat()
            assert stat.S_ISFIFO(attrs.st_mode), attrs
            chr_attrs = chr_path.stat()
            assert stat.S_ISCHR(chr_attrs.st_mode), chr_attrs
            assert chr_attrs.st_rdev == chr_rdev, chr_attrs

            try:
                fifo_fd = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
            except OSError as exc:
                assert exc.errno in {errno.EOPNOTSUPP, errno.ENOTSUP}, exc
            else:
                os.close(fifo_fd)
                raise AssertionError("expected FIFO open to be unsupported in backend mode")

            try:
                chr_fd = os.open(chr_path, os.O_RDONLY | os.O_NONBLOCK)
            except OSError as exc:
                assert exc.errno in {errno.EOPNOTSUPP, errno.ENOTSUP}, exc
            else:
                os.close(chr_fd)
                raise AssertionError("expected char device open to be unsupported in backend mode")

            chr_path.unlink()
            fifo_path.unlink()
            dir_path.rmdir()
            print("OK mknod/special-nodes")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
