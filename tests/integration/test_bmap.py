#!/usr/bin/env python3

import ctypes
import errno
import os
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, FuseOSError, load_dsn_from_config
from tests.integration.dbfs_mount import DBFSMount


def main():
    suffix = uuid.uuid4().hex[:8]
    payload = b"x" * 6000
    blocksize = 4096
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-bmap-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = f"/bmap_{suffix}"
            file_path = f"{dir_path}/payload.bin"
            link_path = f"{dir_path}/payload-link.bin"
            mount_dir = mountpoint / dir_path.lstrip("/")
            mount_file = mountpoint / file_path.lstrip("/")
            mount_link = mountpoint / link_path.lstrip("/")

            fs.mkdir(dir_path, 0o755)
            fh = fs.create(file_path, 0o644)
            fs.write(file_path, payload, 0, fh)
            fs.flush(file_path, fh)
            fs.release(file_path, fh)
            os.link(mount_file, mount_link)

            block0 = ctypes.c_uint64(0)
            block1 = ctypes.c_uint64(1)
            assert fs.bmap(file_path, blocksize, block0) == 0
            assert block0.value == 0, block0.value
            assert fs.bmap(link_path, blocksize, block1) == 0
            assert block1.value == 1, block1.value

            try:
                fs.bmap(dir_path, blocksize, ctypes.c_uint64(0))
            except FuseOSError as exc:
                assert exc.errno == errno.EOPNOTSUPP, exc.errno
            else:
                raise AssertionError("bmap on directory did not fail")

            print("OK bmap/logical")
        finally:
            try:
                mount_link.unlink()
            except Exception:
                pass
            try:
                mount_file.unlink()
            except Exception:
                pass
            try:
                mount_dir.rmdir()
            except Exception:
                pass
            try:
                fs.connection_pool.closeall()
            except Exception:
                pass
            launcher.stop()


if __name__ == "__main__":
    main()
