#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def _cache_keys_for_file(fs, file_id):
    with fs._read_block_cache_guard:
        return {key for key in fs._read_block_cache if key[0] == file_id}


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)
    fs.read_ahead_blocks = 0
    fs.sequential_read_ahead_blocks = 2
    fs.small_file_read_threshold_blocks = 0

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/ra-seq-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    block_size = fs.block_size
    payload = (b"abcdefgh" * ((block_size * 3) // 8 + 4))[: block_size * 3]

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fh = fs.open(file_path, os.O_RDONLY)
        file_id = fs.file_id_for_handle(fh)
        assert file_id is not None, fh
        fs.storage.clear_read_cache(file_id)

        first = fs.read(file_path, 1, 0, fh)
        assert first == payload[:1], (first, payload[:1])
        first_cache = _cache_keys_for_file(fs, file_id)
        assert (file_id, 0) in first_cache, first_cache
        assert (file_id, 1) not in first_cache, first_cache

        second = fs.read(file_path, 1, 1, fh)
        assert second == payload[1:2], (second, payload[1:2])
        second_cache = _cache_keys_for_file(fs, file_id)
        assert (file_id, 1) in second_cache, second_cache
        assert (file_id, 2) in second_cache, second_cache

        print("OK read-ahead/sequence")
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
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
