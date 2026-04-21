#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
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
    fs.metadata_cache_ttl_seconds = 60.0
    fs.statfs_cache_ttl_seconds = 60.0

    conn_calls = 0
    original_db_connection = fs.db_connection

    def tracked_db_connection(*args, **kwargs):
        nonlocal conn_calls
        conn_calls += 1
        return original_db_connection(*args, **kwargs)

    fs.db_connection = tracked_db_connection

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/metadata-cache-{suffix}"
    file_path = f"{dir_path}/payload.txt"
    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        payload = b"metadata-cache\n"
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        before_getattr = conn_calls
        attrs_first = fs.getattr(file_path)
        after_getattr_first = conn_calls
        attrs_second = fs.getattr(file_path)
        after_getattr_second = conn_calls
        assert after_getattr_second == after_getattr_first, (
            before_getattr,
            after_getattr_first,
            after_getattr_second,
        )
        assert attrs_first["st_size"] == attrs_second["st_size"], (attrs_first, attrs_second)

        before_readdir = conn_calls
        entries_first = fs.readdir("/", 777)
        after_readdir_first = conn_calls
        entries_second = fs.readdir("/", 777)
        after_readdir_second = conn_calls
        assert after_readdir_second == after_readdir_first, (
            before_readdir,
            after_readdir_first,
            after_readdir_second,
        )
        assert "." in entries_first and ".." in entries_first, entries_first
        assert entries_first == entries_second, (entries_first, entries_second)

        before_statfs = conn_calls
        statfs_first = fs.statfs("/")
        after_statfs_first = conn_calls
        statfs_second = fs.statfs("/")
        after_statfs_second = conn_calls
        assert after_statfs_second == after_statfs_first, (
            before_statfs,
            after_statfs_first,
            after_statfs_second,
        )
        assert statfs_first["f_blocks"] == statfs_second["f_blocks"], (statfs_first, statfs_second)

        fh = fs.open(file_path, os.O_WRONLY)
        extra = b"cache-bust\n"
        before_mutation = conn_calls
        fs.write(file_path, extra, len(payload), fh)
        fs.flush(file_path, fh)
        after_mutation = conn_calls
        assert after_mutation > before_mutation, (before_mutation, after_mutation)

        before_invalidated_getattr = conn_calls
        attrs_third = fs.getattr(file_path)
        after_invalidated_getattr = conn_calls
        assert after_invalidated_getattr > before_invalidated_getattr, (
            before_invalidated_getattr,
            after_invalidated_getattr,
        )
        assert attrs_third["st_size"] == len(payload) + len(extra), attrs_third

        before_invalidated_statfs = conn_calls
        statfs_third = fs.statfs("/")
        after_invalidated_statfs = conn_calls
        assert after_invalidated_statfs > before_invalidated_statfs, (
            before_invalidated_statfs,
            after_invalidated_statfs,
        )
        assert statfs_third["f_blocks"] >= statfs_first["f_blocks"], (statfs_first, statfs_third)

        fs.release(file_path, fh)
        fh = None

        print("OK metadata-cache")
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
            fs.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
