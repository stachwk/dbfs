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
    original_copy_dedupe = os.environ.get("DBFS_RUST_HOTPATH_COPY_DEDUPE")
    original_copy_plan = os.environ.get("DBFS_RUST_HOTPATH_COPY_PLAN")
    original_rust_hotpath = os.environ.get("DBFS_RUST_HOTPATH_COPY_PACK")
    os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_PLAN", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_PACK", None)
    fs = None
    try:
        profile_expectations = [
            (
                "bulk_write",
                {
                    "write_flush_threshold_bytes": 256 * 1024 * 1024,
                    "read_cache_max_blocks": 1024,
                    "read_ahead_blocks": 4,
                    "sequential_read_ahead_blocks": 8,
                    "small_file_read_threshold_blocks": 8,
                    "workers_read": 4,
                    "workers_read_min_blocks": 8,
                    "workers_write": 8,
                    "workers_write_min_blocks": 16,
                    "persist_buffer_chunk_blocks": 512,
                    "rust_hotpath_copy_dedupe": False,
                    "rust_hotpath_copy_plan": False,
                    "rust_hotpath_copy_pack": False,
                    "metadata_cache_ttl_seconds": 1,
                    "statfs_cache_ttl_seconds": 1,
                    "lock_poll_interval_seconds": 0.1,
                },
            ),
            (
                "metadata_heavy",
                {
                    "write_flush_threshold_bytes": 64 * 1024 * 1024,
                    "read_cache_max_blocks": 2048,
                    "read_ahead_blocks": 4,
                    "sequential_read_ahead_blocks": 8,
                    "small_file_read_threshold_blocks": 16,
                    "workers_read": 2,
                    "workers_read_min_blocks": 16,
                    "workers_write": 2,
                    "workers_write_min_blocks": 16,
                    "persist_buffer_chunk_blocks": 64,
                    "rust_hotpath_copy_dedupe": False,
                    "rust_hotpath_copy_plan": False,
                    "rust_hotpath_copy_pack": False,
                    "metadata_cache_ttl_seconds": 10,
                    "statfs_cache_ttl_seconds": 10,
                    "lock_poll_interval_seconds": 0.1,
                },
            ),
            (
                "pg_locking",
                {
                    "workers_read": 1,
                    "workers_read_min_blocks": 16,
                    "workers_write": 1,
                    "workers_write_min_blocks": 16,
                    "persist_buffer_chunk_blocks": 64,
                    "rust_hotpath_copy_dedupe": False,
                    "rust_hotpath_copy_plan": False,
                    "rust_hotpath_copy_pack": False,
                    "metadata_cache_ttl_seconds": 1,
                    "statfs_cache_ttl_seconds": 1,
                    "lock_poll_interval_seconds": 0.05,
                },
            ),
        ]

        for profile_name, expectations in profile_expectations:
            os.environ["DBFS_PROFILE"] = profile_name
            runtime_config = load_dbfs_runtime_config(config_path)
            fs = DBFS(dsn, db_config, runtime_config=runtime_config)
            assert fs.runtime_config_get("profile") == profile_name, fs.runtime_config
            assert fs.synchronous_commit == "on", fs.synchronous_commit
            for attr_name, expected_value in expectations.items():
                assert getattr(fs, attr_name) == expected_value, (profile_name, attr_name, getattr(fs, attr_name))
            with fs.backend.connection() as conn, conn.cursor() as cur:
                cur.execute("SHOW synchronous_commit")
                assert cur.fetchone()[0] == "on", fs.synchronous_commit
            fs.close()
            fs = None
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
        if original_copy_dedupe is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE"] = original_copy_dedupe
        if original_copy_plan is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_PLAN", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_PLAN"] = original_copy_plan
        if original_rust_hotpath is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_PACK", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_PACK"] = original_rust_hotpath


if __name__ == "__main__":
    main()
