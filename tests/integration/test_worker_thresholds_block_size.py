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

    fs.workers_write = 2
    fs.workers_write_min_blocks = 2
    fs.copy_skip_unchanged_blocks = False

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/threshold-workers-{suffix}"
    src_path = f"{dir_path}/src.bin"
    small_dst_path = f"{dir_path}/small-dst.bin"
    large_dst_path = f"{dir_path}/large-dst.bin"
    block_size = fs.block_size
    payload = (b"abcdefgh" * ((block_size * 4) // 8 + 8))[: block_size * 4]

    src_fh = None
    small_dst_fh = None
    large_dst_fh = None
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

        small_payload = payload[:block_size]
        small_dst_fh = fs.create(small_dst_path, 0o644)
        copied = fs.copy_file_range(src_path, None, 0, small_dst_path, small_dst_fh, 0, len(small_payload), 0)
        assert copied == len(small_payload), (copied, len(small_payload))
        fs.flush(small_dst_path, small_dst_fh)
        fs.release(small_dst_path, small_dst_fh)
        small_dst_fh = None

        small_read_fh = fs.open(small_dst_path, os.O_RDONLY)
        try:
            data = fs.read(small_dst_path, len(small_payload), 0, small_read_fh)
        finally:
            fs.release(small_dst_path, small_read_fh)
        assert data == small_payload, (len(data), len(small_payload))
        assert calls == [], calls
        assert thread_ids == set(), thread_ids

        calls.clear()
        thread_ids.clear()

        large_payload_len = block_size * 2
        large_dst_fh = fs.create(large_dst_path, 0o644)
        copied = fs.copy_file_range(src_path, None, 0, large_dst_path, large_dst_fh, 0, large_payload_len, 0)
        assert copied == large_payload_len, (copied, large_payload_len)
        fs.flush(large_dst_path, large_dst_fh)
        fs.release(large_dst_path, large_dst_fh)
        large_dst_fh = None

        large_read_fh = fs.open(large_dst_path, os.O_RDONLY)
        try:
            data = fs.read(large_dst_path, large_payload_len, 0, large_read_fh)
        finally:
            fs.release(large_dst_path, large_read_fh)
        assert data == payload[:large_payload_len], (len(data), large_payload_len)
        assert len(calls) >= 2, calls
        assert len(thread_ids) >= 2, thread_ids

        print("OK worker-thresholds/block-size")
    finally:
        fs.storage._read_segment_for_copy = original_reader
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        if small_dst_fh is not None:
            try:
                fs.release(small_dst_path, small_dst_fh)
            except Exception:
                pass
        if large_dst_fh is not None:
            try:
                fs.release(large_dst_path, large_dst_fh)
            except Exception:
                pass
        try:
            fs.unlink(src_path)
        except Exception:
            pass
        try:
            fs.unlink(small_dst_path)
        except Exception:
            pass
        try:
            fs.unlink(large_dst_path)
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
