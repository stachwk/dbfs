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
    dir_path = f"/dirhooks_{suffix}"
    file_path = f"{dir_path}/payload.txt"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.release(file_path, fh)

        assert fs.opendir(dir_path) == 0
        assert fs.fsyncdir(dir_path, 0, 0) == 0
        assert fs.releasedir(dir_path, 0) == 0
    finally:
        try:
            if fh is not None:
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
        fs.cleanup_resources()

    print("OK dirhooks/opendir/releasedir/fsyncdir")


if __name__ == "__main__":
    main()
