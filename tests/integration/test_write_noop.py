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
    path = f"/write-noop-{suffix}.txt"

    try:
        fh = fs.create(path, 0o644)
        fs.write(path, b"payload\n", 0, fh)
        fs.flush(path, fh)
        fs.release(path, fh)

        before = fs.getattr(path)
        before_size = before["st_size"]
        before_mtime = before["st_mtime"]
        before_ctime = before["st_ctime"]

        fh = fs.open(path, os.O_WRONLY)
        written = fs.write(path, b"", 0, fh)
        assert written == 0, written
        fs.flush(path, fh)
        fs.release(path, fh)

        after = fs.getattr(path)
        assert after["st_size"] == before_size, (before_size, after["st_size"])
        assert after["st_mtime"] == before_mtime, (before_mtime, after["st_mtime"])
        assert after["st_ctime"] == before_ctime, (before_ctime, after["st_ctime"])

        print("OK write/noop")
    finally:
        try:
            fs.unlink(path)
        except Exception:
            pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
