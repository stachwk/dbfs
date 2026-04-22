#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dsn_from_config
from dbfs_fuse import DBFS


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    previous_sync_commit = os.environ.get("DBFS_SYNCHRONOUS_COMMIT")
    os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)

    try:
        try:
            DBFS(dsn, db_config, runtime_config={"lock_poll_interval_seconds": "0"})
        except ValueError as exc:
            message = str(exc)
            if "lock_poll_interval_seconds" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid lock_poll_interval_seconds to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"read_cache_blocks": "0"})
        except ValueError as exc:
            message = str(exc)
            if "read_cache_blocks" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid read_cache_blocks to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"workers_read": "0"})
        except ValueError as exc:
            message = str(exc)
            if "workers_read" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid workers_read to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"workers_write": "0"})
        except ValueError as exc:
            message = str(exc)
            if "workers_write" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid workers_write to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"persist_buffer_chunk_blocks": "0"})
        except ValueError as exc:
            message = str(exc)
            if "persist_buffer_chunk_blocks" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid persist_buffer_chunk_blocks to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"copy_skip_unchanged_blocks": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "copy_skip_unchanged_blocks" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid copy_skip_unchanged_blocks to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"copy_skip_unchanged_blocks_crc_table": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "copy_skip_unchanged_blocks_crc_table" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid copy_skip_unchanged_blocks_crc_table to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"rust_hotpath_copy_pack": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "rust_hotpath_copy_pack" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid rust_hotpath_copy_pack to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"rust_hotpath_copy_plan": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "rust_hotpath_copy_plan" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid rust_hotpath_copy_plan to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"rust_hotpath_copy_dedupe": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "rust_hotpath_copy_dedupe" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid rust_hotpath_copy_dedupe to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"rust_hotpath_persist_pad": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "rust_hotpath_persist_pad" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid rust_hotpath_persist_pad to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"rust_hotpath_read_assemble": "maybe"})
        except ValueError as exc:
            message = str(exc)
            if "rust_hotpath_read_assemble" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid rust_hotpath_read_assemble to fail fast")

        try:
            DBFS(dsn, db_config, runtime_config={"synchronous_commit": "banana"})
        except ValueError as exc:
            message = str(exc)
            if "synchronous_commit" not in message:
                raise AssertionError(message)
        else:
            raise AssertionError("expected invalid synchronous_commit to fail fast")

        print("OK runtime-validation")
    finally:
        if previous_sync_commit is None:
            os.environ.pop("DBFS_SYNCHRONOUS_COMMIT", None)
        else:
            os.environ["DBFS_SYNCHRONOUS_COMMIT"] = previous_sync_commit


if __name__ == "__main__":
    main()
