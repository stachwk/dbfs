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
    dir_path = f"/bufio_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"buffered io payload"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write_buf(file_path, memoryview(payload), 0, fh)
        assert written == len(payload), written

        tail = fs.write_buf(file_path, b"!", len(payload), fh)
        assert tail == 1, tail
        fs.flush(file_path, fh)

        data = fs.read_buf(file_path, len(payload) + 1, 0, fh)
        assert data == payload + b"!", data
        print("OK bufio/read_buf/write_buf")
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
