#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    config_path = ROOT / "dbfs_config.ini"
    original_profile = os.environ.get("DBFS_PROFILE")
    original_sync_commit = os.environ.get("DBFS_SYNCHRONOUS_COMMIT")
    os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
    fs = None
    try:
        os.environ["DBFS_PROFILE"] = "bulk_write"
        runtime_config = load_dbfs_runtime_config(config_path)
        fs = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs.runtime_config_get("profile") == "bulk_write", fs.runtime_config
        assert fs.synchronous_commit == "on", fs.synchronous_commit
        assert fs.write_flush_threshold_bytes == 256 * 1024 * 1024, fs.write_flush_threshold_bytes
        assert fs.read_cache_max_blocks == 512, fs.read_cache_max_blocks
        assert fs.read_ahead_blocks == 2, fs.read_ahead_blocks
        assert fs.sequential_read_ahead_blocks == 4, fs.sequential_read_ahead_blocks
        assert fs.small_file_read_threshold_blocks == 4, fs.small_file_read_threshold_blocks
        assert fs.workers_read == 4, fs.workers_read
        assert fs.workers_read_min_blocks == 8, fs.workers_read_min_blocks
        assert fs.workers_write == 8, fs.workers_write
        assert fs.workers_write_min_blocks == 8, fs.workers_write_min_blocks
        assert fs.persist_buffer_chunk_blocks == 512, fs.persist_buffer_chunk_blocks
        assert fs.metadata_cache_ttl_seconds == 2, fs.metadata_cache_ttl_seconds
        assert fs.statfs_cache_ttl_seconds == 2, fs.statfs_cache_ttl_seconds
        with fs.backend.connection() as conn, conn.cursor() as cur:
            cur.execute("SHOW synchronous_commit")
            assert cur.fetchone()[0] == "on", fs.synchronous_commit
        print("OK runtime-profile")
    finally:
        if fs is not None:
            fs.close()
        if original_profile is None:
            os.environ.pop("DBFS_PROFILE", None)
        else:
            os.environ["DBFS_PROFILE"] = original_profile
        if original_sync_commit is None:
            os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
        else:
            os.environ["DBFS_SYNCHRONOUS_COMMIT"] = original_sync_commit


if __name__ == "__main__":
    main()
