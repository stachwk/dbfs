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
    dir_path = f"/truncate_block_{suffix}"
    file_path = f"{dir_path}/payload.bin"
    block_size = 4096
    payload = (b"A" * block_size) + (b"B" * 1904)
    expected = (b"A" * block_size) + (b"\x00" * block_size)

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)

        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fh = fs.open(file_path, os.O_RDWR)
        fs.truncate(file_path, block_size, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fh = fs.open(file_path, os.O_RDWR)
        fs.truncate(file_path, block_size * 2, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fh = fs.open(file_path, os.O_RDONLY)
        data = fs.read(file_path, block_size * 2, 0, fh)
        assert len(data) == block_size * 2, len(data)
        assert data == expected, data[block_size:block_size + 64]
        fs.release(file_path, fh)
        fh = None

        print("OK truncate shrink block boundary")
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
