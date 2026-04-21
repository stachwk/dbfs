#!/usr/bin/env python3

from __future__ import annotations

import os
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
    dir_path = f"/lseek_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"seekable payload"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)

        end_offset = fs.lseek(file_path, 0, os.SEEK_END, fh)
        assert end_offset == len(payload), end_offset

        current_offset = fs.lseek(file_path, -1, os.SEEK_CUR, fh)
        assert current_offset == len(payload) - 1, current_offset

        reset_offset = fs.lseek(file_path, 0, os.SEEK_SET, fh)
        assert reset_offset == 0, reset_offset

        fs.flush(file_path, fh)
        print("OK lseek/backend")
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
