#!/usr/bin/env python3

from __future__ import annotations

import os
import select
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
    dir_path = f"/poll_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"poll payload"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)

        mask = fs.poll(file_path, fh, select.POLLIN | select.POLLOUT)
        assert mask & select.POLLIN, mask
        assert mask & select.POLLOUT, mask

        replica_fs = DBFS(dsn, db_config, role="replica")
        replica_fh = replica_fs.open(file_path, os.O_RDONLY)
        replica_mask = replica_fs.poll(file_path, replica_fh, select.POLLIN | select.POLLOUT)
        assert replica_mask & select.POLLIN, replica_mask
        assert not (replica_mask & select.POLLOUT), replica_mask
        replica_fs.release(file_path, replica_fh)

        print("OK poll/backend")
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
