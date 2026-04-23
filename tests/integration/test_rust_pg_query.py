#!/usr/bin/env python3

from __future__ import annotations

import os
import uuid
import sys

import psycopg2

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import PostgresBackend, load_dsn_from_config
from dbfs_fuse import DBFS
from dbfs_schema import SCHEMA_VERSION


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    backend = PostgresBackend(dsn, db_config, synchronous_commit="on")
    fs = DBFS(dsn, db_config)
    try:
        startup_snapshot = backend.python_to_rust_pg_startup_snapshot()
        assert startup_snapshot is not None, startup_snapshot
        assert startup_snapshot["block_size_found"] is True, startup_snapshot
        assert startup_snapshot["block_size"] == int(fs.block_size), startup_snapshot
        assert startup_snapshot["is_in_recovery"] == backend.python_to_rust_pg_is_in_recovery(), startup_snapshot
        assert startup_snapshot["schema_version"] == SCHEMA_VERSION, startup_snapshot
        assert startup_snapshot["schema_version_found"] is True, startup_snapshot
        assert startup_snapshot["schema_is_initialized"] is True, startup_snapshot

        rust_value = backend.python_to_rust_pg_query_scalar_text("SELECT 1")
        assert rust_value == "1", rust_value

        rust_block_size = backend.python_to_rust_pg_get_config_value("block_size")
        assert rust_block_size is not None, rust_block_size

        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            python_value = str(cur.fetchone()[0])
        assert rust_value == python_value, (rust_value, python_value)

        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = %s", ("block_size",))
            python_block_size = cur.fetchone()[0]
        assert rust_block_size == str(python_block_size), (rust_block_size, python_block_size)

        rust_recovery = backend.python_to_rust_pg_is_in_recovery()
        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            python_recovery = bool(cur.fetchone()[0])
        assert rust_recovery == python_recovery, (rust_recovery, python_recovery)

        rust_schema_version = backend.python_to_rust_pg_schema_version()
        assert rust_schema_version == SCHEMA_VERSION, (rust_schema_version, SCHEMA_VERSION)

        rust_schema_initialized = backend.python_to_rust_pg_schema_is_initialized()
        assert rust_schema_initialized is True, rust_schema_initialized

        namespace_name = f"/rust-pg-namespace-{uuid.uuid4().hex[:8]}"
        fs.mkdir(namespace_name, 0o755)
        try:
            rust_dir_id = backend.python_to_rust_namespace_get_dir_id(namespace_name)
            python_dir_id = fs.get_dir_id(namespace_name)
            assert rust_dir_id == python_dir_id, (rust_dir_id, python_dir_id)
            assert rust_dir_id is not None, rust_dir_id

            rust_resolved_dir = backend.python_to_rust_namespace_resolve_path(namespace_name)
            assert rust_resolved_dir == (None, "dir", rust_dir_id), rust_resolved_dir

            rust_created_dir_name = f"{namespace_name}/rust-created-dir"
            rust_created_dir_id = backend.python_to_rust_namespace_create_directory(
                rust_dir_id,
                "rust-created-dir",
                0o755,
                *fs.current_uid_gid(),
                fs.generate_inode_seed(),
            )
            assert rust_created_dir_id is not None, rust_created_dir_id
            assert fs.get_dir_id(rust_created_dir_name) == rust_created_dir_id
            fs.rmdir(rust_created_dir_name)

            file_name = f"{namespace_name}/payload.txt"
            file_fh = fs.create(file_name, 0o644)
            try:
                rust_resolved_file = backend.python_to_rust_namespace_resolve_path(file_name)
                rust_file_id = backend.python_to_rust_namespace_get_file_id(file_name)
                rust_file_mode = backend.python_to_rust_namespace_get_file_mode_value(file_name)
                rust_file_links = backend.python_to_rust_namespace_count_file_links(rust_file_id)
                python_file_id = fs.get_file_id(file_name)
                python_file_mode = fs.get_file_mode_value(file_name)
                assert rust_file_id == python_file_id, (rust_file_id, python_file_id)
                assert rust_file_id is not None, rust_file_id
                assert rust_resolved_file == (rust_dir_id, "file", rust_file_id), rust_resolved_file
                assert rust_file_mode == "644", rust_file_mode
                assert python_file_mode == "644", python_file_mode
                assert rust_file_links == 1, rust_file_links
                assert backend.python_to_rust_storage_path_has_children(rust_dir_id) is True

                hardlink_name = f"{namespace_name}/payload-link.txt"
                fs.link(hardlink_name, file_name)
                try:
                    rust_hardlink_id = backend.python_to_rust_namespace_get_hardlink_id(hardlink_name)
                    python_hardlink_id = fs.get_hardlink_id(hardlink_name)
                    rust_hardlink_mode = backend.python_to_rust_namespace_get_file_mode_value(hardlink_name)
                    python_hardlink_mode = fs.get_file_mode_value(hardlink_name)
                    rust_primary_hardlink = backend.python_to_rust_namespace_choose_primary_hardlink(rust_file_id)
                    rust_hardlink_file_id = backend.python_to_rust_namespace_get_hardlink_file_id(rust_hardlink_id)
                    python_hardlink_file_id = fs.get_hardlink_file_id(python_hardlink_id)
                    assert rust_hardlink_id == python_hardlink_id, (rust_hardlink_id, python_hardlink_id)
                    assert rust_hardlink_id is not None, rust_hardlink_id
                    assert rust_hardlink_mode == "644", rust_hardlink_mode
                    assert python_hardlink_mode == "644", python_hardlink_mode
                    assert rust_primary_hardlink == (rust_hardlink_id, rust_dir_id, "payload-link.txt"), rust_primary_hardlink
                    assert rust_hardlink_file_id == python_hardlink_file_id, (rust_hardlink_file_id, python_hardlink_file_id)
                    assert rust_hardlink_file_id == rust_file_id, (rust_hardlink_file_id, rust_file_id)
                    assert backend.python_to_rust_namespace_count_file_links(rust_file_id) == 2
                finally:
                    fs.unlink(hardlink_name)

                symlink_name = f"{namespace_name}/payload-link.symlink"
                fs.symlink(symlink_name, file_name)
                try:
                    rust_symlink_id = backend.python_to_rust_namespace_get_symlink_id(symlink_name)
                    python_symlink_id = fs.get_symlink_id(symlink_name)
                    assert rust_symlink_id == python_symlink_id, (rust_symlink_id, python_symlink_id)
                    assert rust_symlink_id is not None, rust_symlink_id

                    rust_direct_symlink_name = f"{namespace_name}/payload-link-rust.symlink"
                    direct_uid, direct_gid = fs.current_uid_gid()
                    direct_inode_seed = fs.generate_inode_seed()
                    rust_direct_symlink_id = backend.python_to_rust_namespace_create_symlink(
                        rust_dir_id,
                        "payload-link-rust.symlink",
                        file_name,
                        direct_uid,
                        direct_gid,
                        direct_inode_seed,
                    )
                    assert rust_direct_symlink_id is not None, rust_direct_symlink_id
                    assert fs.get_symlink_id(rust_direct_symlink_name) == rust_direct_symlink_id
                finally:
                    fs.unlink(symlink_name)
                    if fs.get_symlink_id(f"{namespace_name}/payload-link-rust.symlink") is not None:
                        fs.unlink(f"{namespace_name}/payload-link-rust.symlink")
            finally:
                fs.release(file_name, file_fh)
                fs.unlink(file_name)
        finally:
            fs.rmdir(namespace_name)
    finally:
        fs.close()
        backend.close()

    print("OK rust-pg-query")


if __name__ == "__main__":
    main()
