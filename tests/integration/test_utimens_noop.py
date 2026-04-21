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
    path = f"/utimens-{suffix}.txt"
    dir_path = f"/utimens-{suffix}"

    try:
        fh = fs.create(path, 0o644)
        fs.write(path, b"utimens\n", 0, fh)
        fs.flush(path, fh)
        fs.release(path, fh)

        before = fs.getattr(path)
        before_atime = before["st_atime"]
        before_mtime = before["st_mtime"]
        before_ctime = before["st_ctime"]

        fs.utimens(path, (before_atime, before_mtime))
        after_same = fs.getattr(path)
        assert after_same["st_atime"] == before_atime, (before_atime, after_same["st_atime"])
        assert after_same["st_mtime"] == before_mtime, (before_mtime, after_same["st_mtime"])
        assert after_same["st_ctime"] == before_ctime, (before_ctime, after_same["st_ctime"])

        newer_atime = before_atime + 10
        newer_mtime = before_mtime + 10
        fs.utimens(path, (newer_atime, newer_mtime))
        after_change = fs.getattr(path)
        assert after_change["st_atime"] == newer_atime, (newer_atime, after_change["st_atime"])
        assert after_change["st_mtime"] == newer_mtime, (newer_mtime, after_change["st_mtime"])
        assert after_change["st_ctime"] >= before_ctime, (before_ctime, after_change["st_ctime"])

        fs.mkdir(dir_path, 0o755)
        dir_before = fs.getattr(dir_path)
        dir_atime = dir_before["st_atime"]
        dir_mtime = dir_before["st_mtime"]
        dir_ctime = dir_before["st_ctime"]

        fs.utimens(dir_path, (dir_atime, dir_mtime))
        dir_after_same = fs.getattr(dir_path)
        assert dir_after_same["st_atime"] == dir_atime, (dir_atime, dir_after_same["st_atime"])
        assert dir_after_same["st_mtime"] == dir_mtime, (dir_mtime, dir_after_same["st_mtime"])
        assert dir_after_same["st_ctime"] == dir_ctime, (dir_ctime, dir_after_same["st_ctime"])

        fs.utimens(dir_path, (dir_atime + 10, dir_mtime + 10))
        dir_after_change = fs.getattr(dir_path)
        assert dir_after_change["st_atime"] == dir_atime + 10, (dir_atime + 10, dir_after_change["st_atime"])
        assert dir_after_change["st_mtime"] == dir_mtime + 10, (dir_mtime + 10, dir_after_change["st_mtime"])
        assert dir_after_change["st_ctime"] >= dir_ctime, (dir_ctime, dir_after_change["st_ctime"])

        print("OK utimens/noop")
    finally:
        try:
            fs.unlink(path)
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
