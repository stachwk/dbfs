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
    previous_sync_commit = os.environ.get("DBFS_SYNCHRONOUS_COMMIT")
    previous_copy_dedupe_enabled = os.environ.get("DBFS_COPY_DEDUPE_ENABLED")
    previous_copy_max_blocks = os.environ.get("DBFS_COPY_DEDUPE_MAX_BLOCKS")
    previous_copy_crc_table = os.environ.get("DBFS_COPY_DEDUPE_CRC_TABLE")
    previous_copy_plan = os.environ.get("DBFS_RUST_HOTPATH_COPY_PLAN")
    previous_rust_copy_dedupe = os.environ.get("DBFS_RUST_HOTPATH_COPY_DEDUPE")
    previous_rust_hotpath = os.environ.get("DBFS_RUST_HOTPATH_COPY_PACK")
    previous_persist_pad = os.environ.get("DBFS_RUST_HOTPATH_PERSIST_PAD")
    previous_read_assemble = os.environ.get("DBFS_RUST_HOTPATH_READ_ASSEMBLE")
    os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_PLAN", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE", None)
    os.environ.pop("DBFS_RUST_HOTPATH_COPY_PACK", None)
    os.environ.pop("DBFS_RUST_HOTPATH_PERSIST_PAD", None)
    os.environ.pop("DBFS_RUST_HOTPATH_READ_ASSEMBLE", None)
    os.environ["DBFS_LOCK_BACKEND"] = "memory"
    fs = None
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
        "persist_buffer_chunk_blocks": 128,
        "copy_dedupe_enabled": False,
        "copy_dedupe_min_blocks": 16,
        "copy_dedupe_max_blocks": 0,
        "copy_dedupe_crc_table": False,
        "rust_hotpath_copy_plan": True,
        "rust_hotpath_copy_dedupe": True,
        "rust_hotpath_copy_pack": True,
        "rust_hotpath_persist_pad": True,
        "rust_hotpath_read_assemble": True,
        "metadata_cache_ttl_seconds": 1,
        "statfs_cache_ttl_seconds": 2,
        "synchronous_commit": "on",
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
    assert fs.persist_buffer_chunk_blocks == expected["persist_buffer_chunk_blocks"], fs.persist_buffer_chunk_blocks
    assert fs.copy_dedupe_enabled == expected["copy_dedupe_enabled"], fs.copy_dedupe_enabled
    assert fs.copy_dedupe_min_blocks == expected["copy_dedupe_min_blocks"], fs.copy_dedupe_min_blocks
    assert fs.copy_dedupe_max_blocks == expected["copy_dedupe_max_blocks"], fs.copy_dedupe_max_blocks
    assert fs.copy_dedupe_crc_table == expected["copy_dedupe_crc_table"], fs.copy_dedupe_crc_table
    assert fs.metadata_cache_ttl_seconds == expected["metadata_cache_ttl_seconds"], fs.metadata_cache_ttl_seconds
    assert fs.statfs_cache_ttl_seconds == expected["statfs_cache_ttl_seconds"], fs.statfs_cache_ttl_seconds
    assert fs.synchronous_commit == expected["synchronous_commit"], fs.synchronous_commit
    assert fs.lock_backend == expected["lock_backend"], fs.lock_backend
    assert fs.locking._pg_lock_manager is not None, fs.locking._pg_lock_manager

    with fs.backend.connection() as conn, conn.cursor() as cur:
        cur.execute("SHOW synchronous_commit")
        assert cur.fetchone()[0] == "on", fs.synchronous_commit

    os.environ["DBFS_SYNCHRONOUS_COMMIT"] = "off"
    fs_override = None
    try:
        fs_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_override.synchronous_commit == "off", fs_override.synchronous_commit
        with fs_override.backend.connection() as conn, conn.cursor() as cur:
            cur.execute("SHOW synchronous_commit")
            assert cur.fetchone()[0] == "off", fs_override.synchronous_commit
    finally:
        if fs_override is not None:
            fs_override.close()
        if previous_sync_commit is None:
            os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
        else:
            os.environ["DBFS_SYNCHRONOUS_COMMIT"] = previous_sync_commit

    os.environ["DBFS_COPY_DEDUPE_ENABLED"] = "1"
    fs_copy_override = None
    try:
        fs_copy_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_copy_override.copy_dedupe_enabled is True, fs_copy_override.copy_dedupe_enabled
    finally:
        if fs_copy_override is not None:
            fs_copy_override.close()
        if previous_copy_dedupe_enabled is None:
            os.environ.pop("DBFS_COPY_DEDUPE_ENABLED", None)
        else:
            os.environ["DBFS_COPY_DEDUPE_ENABLED"] = previous_copy_dedupe_enabled

    os.environ["DBFS_COPY_DEDUPE_MAX_BLOCKS"] = "32"
    fs_copy_max_override = None
    try:
        fs_copy_max_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_copy_max_override.copy_dedupe_max_blocks == 32, fs_copy_max_override.copy_dedupe_max_blocks
    finally:
        if fs_copy_max_override is not None:
            fs_copy_max_override.close()
        if previous_copy_max_blocks is None:
            os.environ.pop("DBFS_COPY_DEDUPE_MAX_BLOCKS", None)
        else:
            os.environ["DBFS_COPY_DEDUPE_MAX_BLOCKS"] = previous_copy_max_blocks

    os.environ["DBFS_COPY_DEDUPE_CRC_TABLE"] = "1"
    fs_copy_crc_override = None
    try:
        fs_copy_crc_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_copy_crc_override.copy_dedupe_crc_table is True, fs_copy_crc_override.copy_dedupe_crc_table
    finally:
        if fs_copy_crc_override is not None:
            fs_copy_crc_override.close()
        if previous_copy_crc_table is None:
            os.environ.pop("DBFS_COPY_DEDUPE_CRC_TABLE", None)
        else:
            os.environ["DBFS_COPY_DEDUPE_CRC_TABLE"] = previous_copy_crc_table

    os.environ["DBFS_RUST_HOTPATH_COPY_PLAN"] = "1"
    fs_plan_override = None
    try:
        fs_plan_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_plan_override.rust_hotpath_copy_plan is True, fs_plan_override.rust_hotpath_copy_plan
    finally:
        if fs_plan_override is not None:
            fs_plan_override.close()
        if previous_copy_plan is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_PLAN", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_PLAN"] = previous_copy_plan

    os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE"] = "1"
    fs_dedupe_override = None
    try:
        fs_dedupe_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_dedupe_override.rust_hotpath_copy_dedupe is True, fs_dedupe_override.rust_hotpath_copy_dedupe
    finally:
        if fs_dedupe_override is not None:
            fs_dedupe_override.close()
        if previous_rust_copy_dedupe is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE"] = previous_rust_copy_dedupe

    os.environ["DBFS_RUST_HOTPATH_COPY_PACK"] = "1"
    fs_rust_override = None
    try:
        fs_rust_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_rust_override.rust_hotpath_copy_pack is True, fs_rust_override.rust_hotpath_copy_pack
    finally:
        if fs_rust_override is not None:
            fs_rust_override.close()
        if previous_rust_hotpath is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_PACK", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_PACK"] = previous_rust_hotpath
        if previous_copy_dedupe is None:
            os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE"] = previous_copy_dedupe

    os.environ["DBFS_RUST_HOTPATH_PERSIST_PAD"] = "1"
    fs_pad_override = None
    try:
        fs_pad_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_pad_override.rust_hotpath_persist_pad is True, fs_pad_override.rust_hotpath_persist_pad
    finally:
        if fs_pad_override is not None:
            fs_pad_override.close()
        if previous_persist_pad is None:
            os.environ.pop("DBFS_RUST_HOTPATH_PERSIST_PAD", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_PERSIST_PAD"] = previous_persist_pad

    os.environ["DBFS_RUST_HOTPATH_READ_ASSEMBLE"] = "1"
    fs_read_assemble_override = None
    try:
        fs_read_assemble_override = DBFS(dsn, db_config, runtime_config=runtime_config)
        assert fs_read_assemble_override.rust_hotpath_read_assemble is True, fs_read_assemble_override.rust_hotpath_read_assemble
    finally:
        if fs_read_assemble_override is not None:
            fs_read_assemble_override.close()
        if previous_read_assemble is None:
            os.environ.pop("DBFS_RUST_HOTPATH_READ_ASSEMBLE", None)
        else:
            os.environ["DBFS_RUST_HOTPATH_READ_ASSEMBLE"] = previous_read_assemble

    fs.close()

    print("OK runtime-config")


if __name__ == "__main__":
    main()
