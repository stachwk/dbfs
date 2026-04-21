#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import threading
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)

    fs.workers_read = 4
    fs.workers_read_min_blocks = 2
    fs.read_ahead_blocks = 0
    fs.sequential_read_ahead_blocks = 0
    fs.small_file_read_threshold_blocks = 0

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/read-workers-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    block_size = fs.block_size
    payload = (b"abcdefgh" * ((block_size * 16) // 8 + 8))[: block_size * 16]

    fh = None
    original_fetch = fs.storage._fetch_block_range_chunk
    calls = []
    thread_ids = set()
    calls_guard = threading.Lock()

    def wrapped_fetch(file_id, first_block, last_block):
        with calls_guard:
            calls.append((first_block, last_block))
            thread_ids.add(threading.get_ident())
        time.sleep(0.05)
        return original_fetch(file_id, first_block, last_block)

    fs.storage._fetch_block_range_chunk = wrapped_fetch
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fh = fs.open(file_path, os.O_RDONLY)
        file_id = fs.get_file_id(file_path)
        assert file_id is not None
        fs.storage.clear_read_cache(file_id)

        data = fs.read(file_path, len(payload), 0, fh)
        assert data == payload, (len(data), len(payload))
        assert len(calls) >= 2, calls
        assert len(thread_ids) >= 2, thread_ids

        print("OK workers-read parallel fetch")
    finally:
        fs.storage._fetch_block_range_chunk = original_fetch
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
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
