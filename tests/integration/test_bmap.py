#!/usr/bin/env python3

import ctypes
import errno
import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, FuseOSError, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/bmap_{suffix}"
    file_path = f"{dir_path}/payload.bin"
    link_path = f"{dir_path}/payload-link.bin"
    payload = b"x" * 6000
    blocksize = 4096

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fs.link(link_path, file_path)

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
            fs.unlink(link_path)
        except Exception:
            pass
        try:
            fs.unlink(file_path)
        except Exception:
            pass
        try:
            fs.rmdir(dir_path)
        except Exception:
            pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
