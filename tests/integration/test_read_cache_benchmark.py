#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
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

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/read-cache-bench-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    block_size = fs.block_size
    iterations = int(os.environ.get("READ_CACHE_BENCH_ITERATIONS", "12"))
    blocks = int(os.environ.get("READ_CACHE_BENCH_BLOCKS", "384"))
    policy = fs.read_cache_max_blocks
    payload = (b"abcdefghijklmnopqrstuvwxyz012345" * ((block_size * blocks) // 32 + 4))[: block_size * blocks]

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fh = fs.open(file_path, os.O_RDONLY)
        fs.storage.clear_read_cache(fh)
        start = time.perf_counter()
        total = 0
        for _ in range(iterations):
            fs.clear_read_sequence_state(fh)
            for offset in range(0, len(payload), block_size):
                chunk = fs.read(file_path, block_size, offset, fh)
                total += len(chunk)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        print(
            f"OK read-cache-benchmark/cache={policy} elapsed_ms={elapsed_ms} "
            f"iterations={iterations} blocks={blocks} bytes={total}"
        )
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
