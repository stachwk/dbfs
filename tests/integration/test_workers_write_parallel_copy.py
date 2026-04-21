#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)

    fs.workers_write = 4
    fs.workers_write_min_blocks = 2

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/copy-workers-{suffix}"
    src_path = f"{dir_path}/src.bin"
    dst_path = f"{dir_path}/dst.bin"
    block_size = fs.block_size
    payload = (b"abcdefgh" * ((block_size * 16) // 8 + 8))[: block_size * 16]

    src_fh = None
    dst_fh = None
    original_reader = fs.storage._read_segment_for_copy
    calls = []
    thread_ids = set()
    calls_guard = threading.Lock()

    def wrapped_reader(src_file_id, src_offset, length):
        with calls_guard:
            calls.append((src_offset, length))
            thread_ids.add(threading.get_ident())
        time.sleep(0.05)
        return original_reader(src_file_id, src_offset, length)

    fs.storage._read_segment_for_copy = wrapped_reader
    try:
        fs.mkdir(dir_path, 0o755)

        src_fh = fs.create(src_path, 0o644)
        written = fs.write(src_path, payload, 0, src_fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(src_path, src_fh)
        fs.release(src_path, src_fh)
        src_fh = None

        dst_fh = fs.create(dst_path, 0o644)
        copied = fs.copy_file_range(src_path, None, 0, dst_path, dst_fh, 0, len(payload), 0)
        assert copied == len(payload), (copied, len(payload))
        fs.flush(dst_path, dst_fh)
        fs.release(dst_path, dst_fh)
        dst_fh = None

        read_fh = fs.open(dst_path, os.O_RDONLY)
        data = fs.read(dst_path, len(payload), 0, read_fh)
        fs.release(dst_path, read_fh)

        assert data == payload, (len(data), len(payload))
        assert len(calls) >= 2, calls
        assert len(thread_ids) >= 2, thread_ids

        print("OK workers-write parallel copy")
    finally:
        fs.storage._read_segment_for_copy = original_reader
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        if dst_fh is not None:
            try:
                fs.release(dst_path, dst_fh)
            except Exception:
                pass
        try:
            fs.unlink(src_path)
        except Exception:
            pass
        try:
            fs.unlink(dst_path)
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
