#!/usr/bin/env python3

from __future__ import annotations

import ctypes
import os
import sys
import termios
import uuid
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/ioctl_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"ioctl payload"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)

        out = ctypes.c_int(-1)
        fs.ioctl(file_path, termios.FIONREAD, 0, SimpleNamespace(fh=fh), 0, ctypes.byref(out))
        assert out.value == len(payload), out.value

        fs.release(file_path, fh)
        fh = None
        print("OK ioctl/FIONREAD")
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
