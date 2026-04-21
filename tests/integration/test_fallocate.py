#!/usr/bin/env python3

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/fallocate_{suffix}"
    file_path = f"{dir_path}/prealloc.txt"
    payload = b"dbfs"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)

        fs.fallocate(file_path, 0, 16, 32, fh)
        stat = fs.getattr(file_path)
        assert stat["st_size"] == 48, stat
        data = fs.read(file_path, 48, 0, fh)
        assert data[:4] == payload, data
        assert data[4:] == b"\x00" * 44, data

        fs.release(file_path, fh)
        fh = None
        print("OK fallocate")
    finally:
        if fh is not None:
            try:
                fs.release(file_path, fh)
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
            fs.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
