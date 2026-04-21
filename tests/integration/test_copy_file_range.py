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
    dir_path = f"/copy_{suffix}"
    src_path = f"{dir_path}/source.txt"
    dst_path = f"{dir_path}/dest.txt"
    src_payload = b"ABCDEFGHIJ"

    src_fh = None
    dst_fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        src_fh = fs.create(src_path, 0o644)
        dst_fh = fs.create(dst_path, 0o644)

        fs.write(src_path, src_payload, 0, src_fh)
        fs.flush(src_path, src_fh)

        copied = fs.copy_file_range(src_path, src_fh, 2, dst_path, dst_fh, 4, 5, 0)
        assert copied == 5, copied

        dst_stat = fs.getattr(dst_path)
        assert dst_stat["st_size"] == 9, dst_stat
        data = fs.read(dst_path, 9, 0, dst_fh)
        assert data == b"\x00\x00\x00\x00CDEFG", data

        src_data = fs.read(src_path, len(src_payload), 0, src_fh)
        assert src_data == src_payload, src_data
        print("OK copy_file_range")
    finally:
        if dst_fh is not None:
            try:
                fs.release(dst_path, dst_fh)
            except Exception:
                pass
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        try:
            fs.unlink(dst_path)
        except Exception:
            pass
        try:
            fs.unlink(src_path)
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
