#!/usr/bin/env python3

from __future__ import annotations

import errno
import os
import stat
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/mknod_{suffix}"
    fifo_path = f"{dir_path}/pipe"
    chr_path = f"{dir_path}/tty-like"

    try:
        fs.mkdir(dir_path, 0o755)
        fs.mknod(fifo_path, stat.S_IFIFO | 0o644, 0)
        chr_rdev = os.makedev(1, 7) if hasattr(os, "makedev") else 0
        fs.mknod(chr_path, stat.S_IFCHR | 0o600, chr_rdev)

        attrs = fs.getattr(fifo_path)
        assert stat.S_ISFIFO(attrs["st_mode"]), attrs
        chr_attrs = fs.getattr(chr_path)
        assert stat.S_ISCHR(chr_attrs["st_mode"]), chr_attrs
        assert chr_attrs["st_rdev"] == chr_rdev, chr_attrs

        try:
            fs.open(fifo_path, os.O_RDONLY)
        except OSError as exc:
            assert exc.errno in {errno.EOPNOTSUPP, errno.ENOTSUP}, exc
        else:
            raise AssertionError("expected FIFO open to be unsupported in backend mode")

        try:
            fs.open(chr_path, os.O_RDONLY)
        except OSError as exc:
            assert exc.errno in {errno.EOPNOTSUPP, errno.ENOTSUP}, exc
        else:
            raise AssertionError("expected char device open to be unsupported in backend mode")

        fs.unlink(chr_path)
        fs.unlink(fifo_path)
        fs.rmdir(dir_path)
        print("OK mknod/special-nodes")
    finally:
        try:
            fs.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
