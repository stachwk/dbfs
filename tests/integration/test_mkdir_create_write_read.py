#!/usr/bin/env python3

import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/itest_{suffix}"
    file_path = f"{dir_path}/hello.txt"
    payload = b"hello dbfs"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), f"write returned {written}, expected {len(payload)}"

        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fh = fs.open(file_path, 0)
        data = fs.read(file_path, len(payload), 0, fh)
        assert data == payload, f"read returned {data!r}, expected {payload!r}"

        print("OK mkdir/create/write/read")
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


if __name__ == "__main__":
    main()
