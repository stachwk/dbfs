#!/usr/bin/env python3

from __future__ import annotations

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
    fs.profile_io = True
    fs.write_flush_threshold_bytes = 1

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/flush-threshold-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    payload = b"ab"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), written
        assert not fs.is_write_buffer_dirty(fh), fs._dirty_write_buffers
        stat = fs.getattr(file_path)
        assert stat["st_size"] == len(payload), stat
        assert fs.read(file_path, len(payload), 0, fh) == payload
        persist_profile = fs._io_profile.get("persist_buffer", {})
        assert int(persist_profile.get("count", 0)) == 1, persist_profile
        fs.release(file_path, fh)
        fh = None
        print("OK write/flush-threshold")
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
