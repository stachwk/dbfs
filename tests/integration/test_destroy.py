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
    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/destroy_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"destroy flush payload"

    fs = DBFS(dsn, db_config)
    fs.mkdir(dir_path, 0o755)
    fh = fs.create(file_path, 0o644)
    fs.write(file_path, payload, 0, fh)
    fs.destroy("/")

    fs2 = DBFS(dsn, db_config)
    try:
        assert fs2.getattr(file_path)["st_size"] == len(payload)
        assert fs2.read(file_path, len(payload), 0, fs2.open(file_path, 0)) == payload
    finally:
        try:
            fs2.unlink(file_path)
        except Exception:
            pass
        try:
            fs2.rmdir(dir_path)
        except Exception:
            pass
        fs2.cleanup_resources()

    print("OK destroy/cleanup")


if __name__ == "__main__":
    main()
