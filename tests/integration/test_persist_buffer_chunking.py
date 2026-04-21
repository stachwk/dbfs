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
    dir_path = f"/persist-chunk-{suffix}"
    file_path = f"{dir_path}/big.bin"
    payload = b"\0" * (5 * 1024 * 1024)

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), written
        fs.flush(file_path, fh)
        stat = fs.getattr(file_path)
        assert stat["st_size"] == len(payload), stat
        assert fs.read(file_path, 4, 0, fh) == payload[:4]
        assert fs.read(file_path, 4, len(payload) - 4, fh) == payload[-4:]
        fs.release(file_path, fh)
        fh = None
        print("OK persist_buffer/chunking")
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
