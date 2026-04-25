#!/usr/bin/env python3

from __future__ import annotations

import os
import uuid
import sys
import zlib
import ctypes

import psycopg2

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import PostgresBackend, load_dsn_from_config
from dbfs_fuse import DBFS
from dbfs_schema import SCHEMA_VERSION


def _rust_repo_and_lib(backend):
    repo = backend._load_rust_pg_repo()
    lib = backend._load_rust_hotpath_lib()
    assert repo is not None, repo
    assert lib is not None, lib
    return repo, lib


def _rust_startup_snapshot(backend):
    repo, lib = _rust_repo_and_lib(backend)
    out_block_size = ctypes.c_uint32()
    out_block_size_found = ctypes.c_ubyte()
    out_is_in_recovery = ctypes.c_ubyte()
    out_schema_version = ctypes.c_uint32()
    out_schema_version_found = ctypes.c_ubyte()
    out_schema_is_initialized = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_bootstrap_snapshot(
        repo,
        ctypes.byref(out_block_size),
        ctypes.byref(out_block_size_found),
        ctypes.byref(out_is_in_recovery),
        ctypes.byref(out_schema_version),
        ctypes.byref(out_schema_version_found),
        ctypes.byref(out_schema_is_initialized),
    )
    assert status == 0, status
    return {
        "block_size": int(out_block_size.value),
        "block_size_found": bool(out_block_size_found.value),
        "is_in_recovery": bool(out_is_in_recovery.value),
        "schema_version": int(out_schema_version.value),
        "schema_version_found": bool(out_schema_version_found.value),
        "schema_is_initialized": bool(out_schema_is_initialized.value),
    }


def _rust_query_scalar_text(backend, sql):
    repo, lib = _rust_repo_and_lib(backend)
    sql_bytes = str(sql).encode("utf-8")
    out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
    out_len = ctypes.c_size_t()
    status = lib.dbfs_rust_pg_repo_query_scalar_text(
        repo,
        sql_bytes,
        len(sql_bytes),
        ctypes.byref(out_ptr),
        ctypes.byref(out_len),
    )
    assert status == 0 and out_ptr, (status, out_ptr)
    try:
        return ctypes.string_at(out_ptr, out_len.value).decode("utf-8").strip()
    finally:
        lib.dbfs_free_bytes(out_ptr, out_len)


def _rust_config_value(backend, key):
    repo, lib = _rust_repo_and_lib(backend)
    key_bytes = str(key).encode("utf-8")
    out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
    out_len = ctypes.c_size_t()
    out_found = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_get_config_value(
        repo,
        key_bytes,
        len(key_bytes),
        ctypes.byref(out_ptr),
        ctypes.byref(out_len),
        ctypes.byref(out_found),
    )
    assert status == 0 and out_found.value, (status, out_found.value)
    try:
        return ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
    finally:
        lib.dbfs_free_bytes(out_ptr, out_len)


def _rust_is_in_recovery(backend):
    repo, lib = _rust_repo_and_lib(backend)
    out_value = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_is_in_recovery(repo, ctypes.byref(out_value))
    assert status == 0, status
    return bool(out_value.value)


def _rust_schema_version(backend):
    repo, lib = _rust_repo_and_lib(backend)
    out_value = ctypes.c_uint32()
    out_found = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_schema_version(repo, ctypes.byref(out_value), ctypes.byref(out_found))
    assert status == 0 and out_found.value, (status, out_found.value)
    return int(out_value.value)


def _rust_schema_is_initialized(backend):
    repo, lib = _rust_repo_and_lib(backend)
    out_value = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_schema_is_initialized(repo, ctypes.byref(out_value))
    assert status == 0, status
    return bool(out_value.value)


def _resolve_path(backend, path):
    repo, lib = _rust_repo_and_lib(backend)
    path_bytes = str(path).encode("utf-8")
    out_parent_id = ctypes.c_uint64()
    out_parent_found = ctypes.c_ubyte()
    out_kind = ctypes.c_ubyte()
    out_entry_id = ctypes.c_uint64()
    out_entry_found = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_resolve_path(
        repo,
        path_bytes,
        len(path_bytes),
        ctypes.byref(out_parent_id),
        ctypes.byref(out_parent_found),
        ctypes.byref(out_kind),
        ctypes.byref(out_entry_id),
        ctypes.byref(out_entry_found),
    )
    assert status == 0, status
    kind_map = {0: None, 1: "hardlink", 2: "symlink", 3: "file", 4: "dir"}
    return (
        int(out_parent_id.value) if out_parent_found.value else None,
        kind_map.get(int(out_kind.value)),
        int(out_entry_id.value) if out_entry_found.value else None,
    )


def _rust_u64(backend, fn, *args):
    repo, _ = _rust_repo_and_lib(backend)
    out_value = ctypes.c_uint64()
    out_found = ctypes.c_ubyte()
    status = fn(repo, *args, ctypes.byref(out_value), ctypes.byref(out_found))
    assert status == 0 and out_found.value, (status, out_found.value)
    return int(out_value.value)


def _rust_optional_u64(backend, fn, *args):
    repo, _ = _rust_repo_and_lib(backend)
    out_value = ctypes.c_uint64()
    out_found = ctypes.c_ubyte()
    status = fn(repo, *args, ctypes.byref(out_value), ctypes.byref(out_found))
    assert status == 0, (status,)
    if not out_found.value:
        return None
    return int(out_value.value)


def _rust_text(backend, fn, *args):
    repo, lib = _rust_repo_and_lib(backend)
    out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
    out_len = ctypes.c_size_t()
    out_found = ctypes.c_ubyte()
    status = fn(
        repo,
        *args,
        ctypes.byref(out_ptr),
        ctypes.byref(out_len),
        ctypes.byref(out_found),
    )
    assert status == 0 and out_found.value, (status, out_found.value)
    try:
        return ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
    finally:
        lib.dbfs_free_bytes(out_ptr, out_len)


def _rust_choose_primary_hardlink(backend, file_id):
    repo, lib = _rust_repo_and_lib(backend)
    out_hardlink_id = ctypes.c_uint64()
    out_parent_id = ctypes.c_uint64()
    out_parent_found = ctypes.c_ubyte()
    out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
    out_len = ctypes.c_size_t()
    out_found = ctypes.c_ubyte()
    status = lib.dbfs_rust_pg_repo_choose_primary_hardlink(
        repo,
        int(file_id),
        ctypes.byref(out_hardlink_id),
        ctypes.byref(out_parent_id),
        ctypes.byref(out_parent_found),
        ctypes.byref(out_ptr),
        ctypes.byref(out_len),
        ctypes.byref(out_found),
    )
    assert status == 0 and out_found.value, (status, out_found.value)
    try:
        name = ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
    finally:
        lib.dbfs_free_bytes(out_ptr, out_len)
    parent_id = int(out_parent_id.value) if out_parent_found.value else None
    return int(out_hardlink_id.value), parent_id, name


def _rust_create_directory(backend, parent_id, name, mode, uid, gid, inode_seed):
    repo, lib = _rust_repo_and_lib(backend)
    out_value = ctypes.c_uint64()
    out_found = ctypes.c_ubyte()
    name_bytes = str(name).encode("utf-8")
    inode_seed_bytes = str(inode_seed).encode("utf-8")
    status = lib.dbfs_rust_pg_repo_create_directory(
        repo,
        int(parent_id or 0),
        ctypes.c_ubyte(1 if parent_id is not None else 0),
        name_bytes,
        len(name_bytes),
        ctypes.c_uint32(int(mode)),
        ctypes.c_uint32(int(uid)),
        ctypes.c_uint32(int(gid)),
        inode_seed_bytes,
        len(inode_seed_bytes),
        ctypes.byref(out_value),
        ctypes.byref(out_found),
    )
    assert status == 0 and out_found.value, (status, out_found.value)
    return int(out_value.value)


def _rust_create_symlink(backend, parent_id, name, target, uid, gid, inode_seed):
    repo, lib = _rust_repo_and_lib(backend)
    out_value = ctypes.c_uint64()
    out_found = ctypes.c_ubyte()
    name_bytes = str(name).encode("utf-8")
    target_bytes = str(target).encode("utf-8")
    inode_seed_bytes = str(inode_seed).encode("utf-8")
    status = lib.dbfs_rust_pg_repo_create_symlink(
        repo,
        int(parent_id or 0),
        ctypes.c_ubyte(1 if parent_id is not None else 0),
        name_bytes,
        len(name_bytes),
        target_bytes,
        len(target_bytes),
        ctypes.c_uint32(int(uid)),
        ctypes.c_uint32(int(gid)),
        inode_seed_bytes,
        len(inode_seed_bytes),
        ctypes.byref(out_value),
        ctypes.byref(out_found),
    )
    assert status == 0 and out_found.value, (status, out_found.value)
    return int(out_value.value)


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    backend = PostgresBackend(dsn, db_config, synchronous_commit="on")
    fs = DBFS(dsn, db_config)
    try:
        startup_snapshot = _rust_startup_snapshot(backend)
        assert startup_snapshot is not None, startup_snapshot
        assert startup_snapshot["block_size_found"] is True, startup_snapshot
        assert startup_snapshot["block_size"] == int(fs.block_size), startup_snapshot
        assert startup_snapshot["is_in_recovery"] == _rust_is_in_recovery(backend), startup_snapshot
        assert startup_snapshot["schema_version"] == SCHEMA_VERSION, startup_snapshot
        assert startup_snapshot["schema_version_found"] is True, startup_snapshot
        assert startup_snapshot["schema_is_initialized"] is True, startup_snapshot

        rust_value = _rust_query_scalar_text(backend, "SELECT 1")
        assert rust_value == "1", rust_value

        rust_block_size = _rust_config_value(backend, "block_size")
        assert rust_block_size is not None, rust_block_size

        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            python_value = str(cur.fetchone()[0])
        assert rust_value == python_value, (rust_value, python_value)

        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = %s", ("block_size",))
            python_block_size = cur.fetchone()[0]
        assert rust_block_size == str(python_block_size), (rust_block_size, python_block_size)

        rust_recovery = _rust_is_in_recovery(backend)
        with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            python_recovery = bool(cur.fetchone()[0])
        assert rust_recovery == python_recovery, (rust_recovery, python_recovery)

        rust_schema_version = _rust_schema_version(backend)
        assert rust_schema_version == SCHEMA_VERSION, (rust_schema_version, SCHEMA_VERSION)

        rust_schema_initialized = _rust_schema_is_initialized(backend)
        assert rust_schema_initialized is True, rust_schema_initialized

        namespace_name = f"/rust-pg-namespace-{uuid.uuid4().hex[:8]}"
        fs.mkdir(namespace_name, 0o755)
        try:
            repo, lib = _rust_repo_and_lib(backend)
            namespace_bytes = str(namespace_name).encode("utf-8")
            rust_dir_id = _rust_u64(backend, lib.dbfs_rust_pg_repo_get_dir_id, namespace_bytes, len(namespace_bytes))
            python_dir_id = fs.repository.get_dir_id(namespace_name)
            assert rust_dir_id == python_dir_id, (rust_dir_id, python_dir_id)
            assert rust_dir_id is not None, rust_dir_id

            rust_resolved_dir = _resolve_path(backend, namespace_name)
            assert rust_resolved_dir == (None, "dir", rust_dir_id), rust_resolved_dir

            rust_created_dir_name = f"{namespace_name}/rust-created-dir"
            rust_created_dir_id = _rust_create_directory(
                backend,
                rust_dir_id,
                "rust-created-dir",
                0o755,
                *fs.current_uid_gid(),
                fs.generate_inode_seed(),
            )
            assert rust_created_dir_id is not None, rust_created_dir_id
            assert fs.repository.get_dir_id(rust_created_dir_name) == rust_created_dir_id
            fs.rmdir(rust_created_dir_name)

            data_block_size = int(fs.block_size)
            data_payload = bytes(range(256)) * 16
            copy_src_name = f"{namespace_name}/copy-src.txt"
            copy_dst_name = f"{namespace_name}/copy-dst.txt"
            copy_src_fh = fs.create(copy_src_name, 0o644)
            try:
                assert fs.write(copy_src_name, data_payload, 0, copy_src_fh) == len(data_payload)
                fs.flush(copy_src_name, copy_src_fh)
            finally:
                fs.release(copy_src_name, copy_src_fh)
            copy_src_id = fs.repository.get_file_id(copy_src_name)
            assert copy_src_id is not None, copy_src_id
            with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
                cur.execute("SELECT data_object_id FROM files WHERE id_file = %s", (copy_src_id,))
                copy_src_object_id = int(cur.fetchone()[0])
                cur.execute("SELECT _order, data FROM data_blocks WHERE data_object_id = %s ORDER BY _order", (copy_src_object_id,))
                src_rows = cur.fetchall()
            assert len(src_rows) == 1 or len(src_rows) == 4, src_rows
            assert bytes(src_rows[0][1]).startswith(data_payload[: min(len(data_payload), data_block_size)])

            copy_dst_fh = fs.create(copy_dst_name, 0o644)
            try:
                fs.copy_dedupe_enabled = True
                fs.copy_dedupe_min_blocks = 1
                copied = fs.copy_file_range(copy_src_name, None, 0, copy_dst_name, copy_dst_fh, 0, len(data_payload), 0)
                assert copied == len(data_payload), copied
                fs.flush(copy_dst_name, copy_dst_fh)
            finally:
                fs.release(copy_dst_name, copy_dst_fh)
            copy_dst_id = fs.repository.get_file_id(copy_dst_name)
            assert copy_dst_id is not None, copy_dst_id
            with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
                cur.execute("SELECT data_object_id FROM files WHERE id_file = %s", (copy_src_id,))
                copy_src_object_id_after = int(cur.fetchone()[0])
                cur.execute("SELECT data_object_id FROM files WHERE id_file = %s", (copy_dst_id,))
                copy_dst_object_id = int(cur.fetchone()[0])
            assert copy_src_object_id_after == copy_src_object_id, (copy_src_object_id_after, copy_src_object_id)
            assert copy_dst_object_id == copy_src_object_id, (copy_dst_object_id, copy_src_object_id)

            mutated_payload = b"\xFF" + data_payload[1:]
            copy_dst_fh = fs.open(copy_dst_name, os.O_WRONLY)
            try:
                assert fs.write(copy_dst_name, mutated_payload[:data_block_size], 0, copy_dst_fh) == data_block_size
                fs.flush(copy_dst_name, copy_dst_fh)
            finally:
                fs.release(copy_dst_name, copy_dst_fh)
            with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
                cur.execute("SELECT data_object_id FROM files WHERE id_file = %s", (copy_src_id,))
                copy_src_object_id_final = int(cur.fetchone()[0])
                cur.execute("SELECT data_object_id FROM files WHERE id_file = %s", (copy_dst_id,))
                copy_dst_object_id_final = int(cur.fetchone()[0])
                cur.execute("SELECT _order, data FROM data_blocks WHERE data_object_id = %s ORDER BY _order", (copy_dst_object_id_final,))
                dst_rows = cur.fetchall()
            assert copy_src_object_id_final == copy_src_object_id, (copy_src_object_id_final, copy_src_object_id)
            assert copy_dst_object_id_final != copy_src_object_id, (copy_dst_object_id_final, copy_src_object_id)
            assert bytes(dst_rows[0][1]).startswith(mutated_payload[: min(len(mutated_payload), data_block_size)]), dst_rows

            file_name = f"{namespace_name}/payload.txt"
            file_fh = fs.create(file_name, 0o644)
            file_cleanup_name = file_name
            try:
                rust_resolved_file = _resolve_path(backend, file_name)
                repo, lib = _rust_repo_and_lib(backend)
                file_bytes = str(file_name).encode("utf-8")
                rust_file_id = _rust_u64(backend, lib.dbfs_rust_pg_repo_get_file_id, file_bytes, len(file_bytes))
                rust_file_mode = _rust_text(backend, lib.dbfs_rust_pg_repo_get_file_mode_value, file_bytes, len(file_bytes))
                rust_file_links = _rust_u64(backend, lib.dbfs_rust_pg_repo_count_file_links, int(rust_file_id))
                python_file_id = fs.repository.get_file_id(file_name)
                python_file_mode = _rust_text(backend, lib.dbfs_rust_pg_repo_get_file_mode_value, file_bytes, len(file_bytes))
                assert rust_file_id == python_file_id, (rust_file_id, python_file_id)
                assert rust_file_id is not None, rust_file_id
                assert rust_resolved_file == (rust_dir_id, "file", rust_file_id), rust_resolved_file
                assert rust_file_mode == "644", rust_file_mode
                assert python_file_mode == "644", python_file_mode
                assert rust_file_links == 1, rust_file_links
                out_value = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_path_has_children(repo, int(rust_dir_id), ctypes.byref(out_value))
                assert status == 0 and bool(out_value.value) is True, (status, out_value.value)

                user_value = b"rust-pg-xattr"
                fs.setxattr(file_name, "user.comment", user_value, 0)
                assert fs.getxattr(file_name, "user.comment") == user_value
                assert fs.xattr_store.fetch_xattr_value(file_name, "user.comment") == user_value

                hardlink_name = f"{namespace_name}/payload-link.txt"
                fs.link(hardlink_name, file_name)
                hardlink_bytes = str(hardlink_name).encode("utf-8")
                rust_hardlink_id = _rust_u64(backend, lib.dbfs_rust_pg_repo_get_hardlink_id, hardlink_bytes, len(hardlink_bytes))
                rust_hardlink_mode = _rust_text(backend, lib.dbfs_rust_pg_repo_get_file_mode_value, hardlink_bytes, len(hardlink_bytes))
                rust_primary_hardlink = _rust_choose_primary_hardlink(backend, rust_file_id)
                rust_hardlink_file_id = _rust_u64(backend, lib.dbfs_rust_pg_repo_get_hardlink_file_id, int(rust_hardlink_id))
                assert rust_hardlink_id is not None, rust_hardlink_id
                assert rust_hardlink_mode == "644", rust_hardlink_mode
                assert rust_primary_hardlink == (rust_hardlink_id, rust_dir_id, "payload-link.txt"), rust_primary_hardlink
                assert rust_hardlink_file_id == rust_file_id, (rust_hardlink_file_id, rust_file_id)
                assert _rust_u64(backend, lib.dbfs_rust_pg_repo_count_file_links, int(rust_file_id)) == 2
                assert fs.xattr_store.fetch_xattr_value(hardlink_name, "user.comment") == user_value

                fs.release(file_name, file_fh)
                file_fh = None
                fs.unlink(file_name)
                assert fs.repository.get_file_id(file_name) is None
                assert fs.repository.get_file_id(hardlink_name) == rust_file_id
                assert _rust_optional_u64(backend, lib.dbfs_rust_pg_repo_get_hardlink_id, hardlink_bytes, len(hardlink_bytes)) is None
                assert _rust_text(backend, lib.dbfs_rust_pg_repo_get_file_mode_value, hardlink_bytes, len(hardlink_bytes)) == "644"
                assert _rust_u64(backend, lib.dbfs_rust_pg_repo_count_file_links, int(rust_file_id)) == 1
                file_cleanup_name = hardlink_name

                symlink_name = f"{namespace_name}/payload-link.symlink"
                fs.symlink(symlink_name, file_name)
                try:
                    symlink_bytes = str(symlink_name).encode("utf-8")
                    rust_symlink_id = _rust_u64(backend, lib.dbfs_rust_pg_repo_get_symlink_id, symlink_bytes, len(symlink_bytes))
                    assert rust_symlink_id is not None, rust_symlink_id

                    rust_direct_symlink_name = f"{namespace_name}/payload-link-rust.symlink"
                    direct_uid, direct_gid = fs.current_uid_gid()
                    direct_inode_seed = fs.generate_inode_seed()
                    rust_direct_symlink_id = _rust_create_symlink(
                        backend,
                        rust_dir_id,
                        "payload-link-rust.symlink",
                        file_name,
                        direct_uid,
                        direct_gid,
                        direct_inode_seed,
                    )
                    assert rust_direct_symlink_id is not None, rust_direct_symlink_id
                    rust_direct_symlink_bytes = str(rust_direct_symlink_name).encode("utf-8")
                    assert _rust_u64(backend, lib.dbfs_rust_pg_repo_get_symlink_id, rust_direct_symlink_bytes, len(rust_direct_symlink_bytes)) == rust_direct_symlink_id
                finally:
                    fs.unlink(symlink_name)
                    payload_link_rust_bytes = f"{namespace_name}/payload-link-rust.symlink".encode("utf-8")
                    if _rust_optional_u64(backend, lib.dbfs_rust_pg_repo_get_symlink_id, payload_link_rust_bytes, len(payload_link_rust_bytes)) is not None:
                        fs.unlink(f"{namespace_name}/payload-link-rust.symlink")
            finally:
                if file_fh is not None:
                    fs.release(file_name, file_fh)
                fs.unlink(file_cleanup_name)
                for cleanup_path in (copy_dst_name, copy_src_name):
                    try:
                        fs.unlink(cleanup_path)
                    except Exception:
                        pass
        finally:
            fs.rmdir(namespace_name)
    finally:
        fs.close()
        backend.close()

    print("OK rust-pg-query")


if __name__ == "__main__":
    main()
