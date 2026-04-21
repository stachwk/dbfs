#!/usr/bin/env python3

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    previous_lock_backend = os.environ.get("DBFS_LOCK_BACKEND")
    os.environ["DBFS_LOCK_BACKEND"] = "memory"
    try:
        fs = DBFS(dsn, db_config, runtime_config=runtime_config)
    finally:
        if previous_lock_backend is None:
            os.environ.pop("DBFS_LOCK_BACKEND", None)
        else:
            os.environ["DBFS_LOCK_BACKEND"] = previous_lock_backend

    expected = {
        "pool_max_connections": 10,
        "write_flush_threshold_bytes": 64 * 1024 * 1024,
        "read_cache_blocks": 1024,
        "read_ahead_blocks": 4,
        "sequential_read_ahead_blocks": 8,
        "small_file_read_threshold_blocks": 8,
        "workers_read": 4,
        "workers_read_min_blocks": 8,
        "workers_write": 4,
        "workers_write_min_blocks": 8,
        "metadata_cache_ttl_seconds": 1,
        "statfs_cache_ttl_seconds": 2,
        "lock_backend": "postgres_lease",
    }

    assert fs.pool_max_connections == expected["pool_max_connections"], fs.pool_max_connections
    assert fs.write_flush_threshold_bytes == expected["write_flush_threshold_bytes"], fs.write_flush_threshold_bytes
    assert fs.read_cache_max_blocks == expected["read_cache_blocks"], fs.read_cache_max_blocks
    assert fs.read_ahead_blocks == expected["read_ahead_blocks"], fs.read_ahead_blocks
    assert fs.sequential_read_ahead_blocks == expected["sequential_read_ahead_blocks"], fs.sequential_read_ahead_blocks
    assert fs.small_file_read_threshold_blocks == expected["small_file_read_threshold_blocks"], fs.small_file_read_threshold_blocks
    assert fs.workers_read == expected["workers_read"], fs.workers_read
    assert fs.workers_read_min_blocks == expected["workers_read_min_blocks"], fs.workers_read_min_blocks
    assert fs.workers_write == expected["workers_write"], fs.workers_write
    assert fs.workers_write_min_blocks == expected["workers_write_min_blocks"], fs.workers_write_min_blocks
    assert fs.metadata_cache_ttl_seconds == expected["metadata_cache_ttl_seconds"], fs.metadata_cache_ttl_seconds
    assert fs.statfs_cache_ttl_seconds == expected["statfs_cache_ttl_seconds"], fs.statfs_cache_ttl_seconds
    assert fs.lock_backend == expected["lock_backend"], fs.lock_backend
    assert fs.locking._pg_lock_manager is not None, fs.locking._pg_lock_manager

    print("OK runtime-config")


if __name__ == "__main__":
    main()
