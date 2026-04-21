#!/usr/bin/env python3

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
    dir_path = f"/itest_{suffix}"
    file_path = f"{dir_path}/data.txt"
    renamed_path = f"{dir_path}/data-renamed.txt"
    payload = b"abcdef123456"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fs.rename(file_path, renamed_path)

        fh = fs.open(renamed_path, 0)
        data = fs.read(renamed_path, len(payload), 0, fh)
        assert data == payload, f"rename/read returned {data!r}, expected {payload!r}"
        fs.release(renamed_path, fh)
        fh = None

        fh = fs.open(renamed_path, 0)
        fs.truncate(renamed_path, 4, fh)
        fs.flush(renamed_path, fh)
        fs.release(renamed_path, fh)
        fh = None

        fh = fs.open(renamed_path, 0)
        truncated = fs.read(renamed_path, 64, 0, fh)
        assert truncated == payload[:4], f"truncate/read returned {truncated!r}, expected {payload[:4]!r}"

        try:
            fs.open(file_path, 0)
        except FuseOSError:
            pass
        else:
            raise AssertionError("old path still opens after rename")

        print("OK truncate/rename")
    finally:
        if fh is not None:
            try:
                fs.release(renamed_path, fh)
            except Exception:
                pass

        try:
            fs.unlink(renamed_path)
        except Exception:
            pass

        try:
            fs.rmdir(dir_path)
        except Exception:
            pass


if __name__ == "__main__":
    main()
