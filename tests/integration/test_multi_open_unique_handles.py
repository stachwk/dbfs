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
    dir_path = f"/multi_open_{suffix}"
    file_path = f"{dir_path}/payload.bin"

    fh_create = None
    fh_plain = None
    fh_append = None

    try:
        fs.mkdir(dir_path, 0o755)

        fh_create = fs.create(file_path, 0o644)
        fs.release(file_path, fh_create)
        fh_create = None

        fh_plain = fs.open(file_path, os.O_WRONLY)
        fh_append = fs.open(file_path, os.O_WRONLY | os.O_APPEND)

        assert fh_plain != fh_append, (fh_plain, fh_append)

        fs.write(file_path, b"AA", 0, fh_plain)
        fs.write(file_path, b"BB", 0, fh_append)
        fs.flush(file_path, fh_plain)
        fs.flush(file_path, fh_append)
        fs.release(file_path, fh_plain)
        fh_plain = None
        fs.release(file_path, fh_append)
        fh_append = None

        fh_read = fs.open(file_path, os.O_RDONLY)
        data = fs.read(file_path, 16, 0, fh_read)
        fs.release(file_path, fh_read)

        assert data == b"AABB", data
        print("OK multi-open unique handles")
    finally:
        if fh_create is not None:
            try:
                fs.release(file_path, fh_create)
            except Exception:
                pass
        if fh_plain is not None:
            try:
                fs.release(file_path, fh_plain)
            except Exception:
                pass
        if fh_append is not None:
            try:
                fs.release(file_path, fh_append)
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
