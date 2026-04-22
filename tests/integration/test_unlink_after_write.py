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
    dir_path = f"/unlink-after-write-{suffix}"
    file_path = f"{dir_path}/payload.bin"

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, b"payload", 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fs.unlink(file_path)

        try:
            fs.getattr(file_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            if getattr(exc, "errno", None) not in (2,):
                raise
        else:
            raise AssertionError("file still exists after unlink")

        print("OK unlink/after-write")
    finally:
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
