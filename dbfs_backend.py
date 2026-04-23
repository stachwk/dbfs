from __future__ import annotations

import ctypes
import os
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.pool
from psycopg2.extensions import make_dsn

from dbfs_config import load_config_parser
from dbfs_pg_tls import resolve_pg_connection_params


def load_dsn_from_config(file_path):
    config, config_path = load_config_parser(file_path)
    db_config = config["database"]
    connection_params = resolve_pg_connection_params(db_config, config_dir=config_path.parent)
    return connection_params, dict(connection_params)


def load_dbfs_runtime_config(file_path):
    config, _ = load_config_parser(file_path)
    runtime = dict(config["dbfs"]) if config.has_section("dbfs") else {}
    profile_name = os.environ.get("DBFS_PROFILE") or runtime.get("profile")
    if profile_name:
        for section_name in (f"dbfs.profile.{profile_name}", f"dbfs.profile:{profile_name}"):
            if config.has_section(section_name):
                runtime.update(dict(config[section_name]))
                runtime["profile"] = profile_name
                break
    return runtime


class PostgresBackend:
    def __init__(self, dsn, db_config, pool_max_connections=10, synchronous_commit="on"):
        self.dsn = dsn
        self.db_config = db_config
        self.pool_max_connections = self.resolve_pool_max_connections(pool_max_connections)
        self.synchronous_commit = self.resolve_synchronous_commit(synchronous_commit)
        self._session_initialized_connection_ids = set()
        if isinstance(self.dsn, Mapping):
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(1, self.pool_max_connections, **self.dsn)
        else:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(1, self.pool_max_connections, self.dsn)
        self._rust_pg_repo_handle = None

    def rust_hotpath_lib_path(self):
        raw_value = os.environ.get("DBFS_RUST_HOTPATH_LIB")
        candidates = []
        if raw_value:
            candidates.append(Path(raw_value))
        candidates.extend(
            [
                Path("/usr/local/lib/libdbfs-2.so"),
                Path("/usr/local/lib/libdbfs_rust_hotpath.so"),
                Path(__file__).resolve().parent / "rust_hotpath" / "target" / "debug" / "libdbfs_rust_hotpath.so",
                Path(__file__).resolve().parent / "rust_hotpath" / "target" / "release" / "libdbfs_rust_hotpath.so",
            ]
        )
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
        return None

    def _load_rust_hotpath_lib(self):
        cached = getattr(self, "_rust_hotpath_lib_handle", None)
        if cached is not None:
            return cached

        lib_path = self.rust_hotpath_lib_path()
        if lib_path is None:
            return None

        try:
            lib = ctypes.CDLL(lib_path)
        except OSError:
            return None

        lib.dbfs_pg_query_scalar_text.argtypes = [
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_pg_query_scalar_text.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_new.argtypes = [
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.dbfs_rust_pg_repo_new.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_free.argtypes = [ctypes.c_void_p]
        lib.dbfs_rust_pg_repo_free.restype = None
        lib.dbfs_rust_pg_repo_query_scalar_text.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_query_scalar_text.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_config_value.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_config_value.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_is_in_recovery.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_is_in_recovery.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_schema_version.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_schema_version.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_schema_is_initialized.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_schema_is_initialized.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_bootstrap_snapshot.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_bootstrap_snapshot.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_dir_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_dir_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_file_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_file_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_file_mode_value.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_file_mode_value.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_hardlink_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_hardlink_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_symlink_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_symlink_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_hardlink_file_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_hardlink_file_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_hardlink.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_create_hardlink.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_directory.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_create_directory.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_file.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_create_file.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_special_file.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_create_special_file.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_symlink.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_create_symlink.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_choose_primary_hardlink.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_choose_primary_hardlink.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_promote_hardlink_to_primary.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_promote_hardlink_to_primary.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_file_links.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_count_file_links.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_path_has_children.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_path_has_children.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_resolve_path.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_resolve_path.restype = ctypes.c_int
        lib.dbfs_free_bytes.argtypes = [ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
        lib.dbfs_free_bytes.restype = None

        self._rust_hotpath_lib_handle = lib
        return lib

    def _load_rust_pg_repo(self):
        cached = getattr(self, "_rust_pg_repo_handle", None)
        if cached is not None:
            return cached

        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        conninfo = self._dsn_conninfo_string().encode("utf-8")
        repo = ctypes.c_void_p()
        status = lib.dbfs_rust_pg_repo_new(conninfo, len(conninfo), ctypes.byref(repo))
        if status != 0 or not repo.value:
            return None

        self._rust_pg_repo_handle = repo
        return repo

    def _dsn_conninfo_string(self):
        if isinstance(self.dsn, Mapping):
            return make_dsn("", **self.dsn)
        return str(self.dsn)

    def python_to_rust_pg_query_scalar_text(self, sql):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        sql_bytes = sql.encode("utf-8")
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        lib = self._load_rust_hotpath_lib()
        status = lib.dbfs_rust_pg_repo_query_scalar_text(
            repo,
            sql_bytes,
            len(sql_bytes),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if status != 0 or not out_ptr:
            return None

        try:
            return ctypes.string_at(out_ptr, out_len.value).decode("utf-8").strip()
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def python_to_rust_pg_get_config_value(self, key):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        key_bytes = str(key).encode("utf-8")
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        out_found = ctypes.c_ubyte()
        lib = self._load_rust_hotpath_lib()
        status = lib.dbfs_rust_pg_repo_get_config_value(
            repo,
            key_bytes,
            len(key_bytes),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None

        try:
            return ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def python_to_rust_pg_is_in_recovery(self):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_is_in_recovery(repo, ctypes.byref(out_value))
        if status != 0:
            return None
        return bool(out_value.value)

    def python_to_rust_pg_schema_version(self):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint32()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_schema_version(repo, ctypes.byref(out_value), ctypes.byref(out_found))
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_pg_schema_is_initialized(self):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_schema_is_initialized(repo, ctypes.byref(out_value))
        if status != 0:
            return None
        return bool(out_value.value)

    def python_to_rust_pg_startup_snapshot(self):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
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
        if status != 0:
            return None
        return {
            "block_size": int(out_block_size.value),
            "block_size_found": bool(out_block_size_found.value),
            "is_in_recovery": bool(out_is_in_recovery.value),
            "schema_version": int(out_schema_version.value),
            "schema_version_found": bool(out_schema_version_found.value),
            "schema_is_initialized": bool(out_schema_is_initialized.value),
        }

    def python_to_rust_namespace_get_dir_id(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_dir_id(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_get_file_id(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_file_id(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_get_file_mode_value(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_file_mode_value(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None

        try:
            return ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def python_to_rust_namespace_get_hardlink_id(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_hardlink_id(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_get_symlink_id(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_symlink_id(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_get_hardlink_file_id(self, hardlink_id):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_hardlink_file_id(
            repo,
            int(hardlink_id),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_create_hardlink(self, source_file_id, target_parent_id, target_name, uid, gid):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        target_name_bytes = str(target_name).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_create_hardlink(
            repo,
            int(source_file_id),
            int(target_parent_id or 0),
            ctypes.c_ubyte(1 if target_parent_id is not None else 0),
            target_name_bytes,
            len(target_name_bytes),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_create_directory(self, target_parent_id, target_name, mode, uid, gid, inode_seed):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        target_name_bytes = str(target_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_create_directory(
            repo,
            int(target_parent_id or 0),
            ctypes.c_ubyte(1 if target_parent_id is not None else 0),
            target_name_bytes,
            len(target_name_bytes),
            ctypes.c_uint32(int(mode)),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_create_file(self, target_parent_id, target_name, mode, uid, gid, inode_seed):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        target_name_bytes = str(target_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_create_file(
            repo,
            int(target_parent_id or 0),
            ctypes.c_ubyte(1 if target_parent_id is not None else 0),
            target_name_bytes,
            len(target_name_bytes),
            ctypes.c_uint32(int(mode)),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_create_special_file(self, target_parent_id, target_name, mode, uid, gid, inode_seed, file_kind, rdev_major, rdev_minor):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        target_name_bytes = str(target_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        file_kind_bytes = str(file_kind).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_create_special_file(
            repo,
            int(target_parent_id or 0),
            ctypes.c_ubyte(1 if target_parent_id is not None else 0),
            target_name_bytes,
            len(target_name_bytes),
            ctypes.c_uint32(int(mode)),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            file_kind_bytes,
            len(file_kind_bytes),
            ctypes.c_uint32(int(rdev_major)),
            ctypes.c_uint32(int(rdev_minor)),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_create_symlink(self, target_parent_id, target_name, target, uid, gid, inode_seed):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        target_name_bytes = str(target_name).encode("utf-8")
        target_bytes = str(target).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_create_symlink(
            repo,
            int(target_parent_id or 0),
            ctypes.c_ubyte(1 if target_parent_id is not None else 0),
            target_name_bytes,
            len(target_name_bytes),
            target_bytes,
            len(target_bytes),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_namespace_choose_primary_hardlink(self, file_id):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
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
        if status != 0 or not out_found.value:
            return None

        try:
            name = ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

        parent_id = int(out_parent_id.value) if out_parent_found.value else None
        return int(out_hardlink_id.value), parent_id, name

    def python_to_rust_namespace_promote_hardlink_to_primary(self, file_id):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_promoted = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_promote_hardlink_to_primary(
            repo,
            int(file_id),
            ctypes.byref(out_promoted),
        )
        if status != 0:
            return None
        return bool(out_promoted.value)

    def python_to_rust_namespace_count_file_links(self, file_id):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_count_file_links(
            repo,
            int(file_id),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def python_to_rust_storage_path_has_children(self, directory_id):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        lib = self._load_rust_hotpath_lib()
        out_value = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_path_has_children(
            repo,
            int(directory_id),
            ctypes.byref(out_value),
        )
        if status != 0:
            return None
        return bool(out_value.value)

    def python_to_rust_namespace_resolve_path(self, path):
        repo = self._load_rust_pg_repo()
        if repo is None:
            return None

        path_bytes = str(path).encode("utf-8")
        lib = self._load_rust_hotpath_lib()
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
        if status != 0:
            return None
        kind_map = {
            0: None,
            1: "hardlink",
            2: "symlink",
            3: "file",
            4: "dir",
        }
        return (
            int(out_parent_id.value) if out_parent_found.value else None,
            kind_map.get(int(out_kind.value)),
            int(out_entry_id.value) if out_entry_found.value else None,
        )

    def resolve_pool_max_connections(self, pool_max_connections):
        try:
            if hasattr(pool_max_connections, "getint"):
                pool_max_connections = pool_max_connections.getint("pool_max_connections", fallback=10)
        except Exception:
            pool_max_connections = 10

        pool_max_connections = int(pool_max_connections)
        if pool_max_connections < 1:
            return 1
        return pool_max_connections

    def resolve_synchronous_commit(self, synchronous_commit):
        value = "on" if synchronous_commit in {None, ""} else str(synchronous_commit).strip().lower()
        allowed = {"on", "off", "local", "remote_write", "remote_apply"}
        if value not in allowed:
            allowed_values = ", ".join(sorted(allowed))
            raise ValueError(f"synchronous_commit must be one of: {allowed_values}")
        return value

    def _physical_connection_id(self, conn):
        raw_conn = getattr(conn, "_dbfs_raw_connection", conn)
        return id(raw_conn), raw_conn

    def _initialize_session_settings(self, conn):
        conn_id, _ = self._physical_connection_id(conn)
        if conn_id in self._session_initialized_connection_ids:
            return
        original_autocommit = conn.autocommit
        try:
            if not original_autocommit:
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
                cur.execute(f"SET synchronous_commit TO '{self.synchronous_commit}'")
        finally:
            if conn.autocommit != original_autocommit:
                conn.autocommit = original_autocommit
        self._session_initialized_connection_ids.add(conn_id)

    @contextmanager
    def connection(self):
        conn = self.connection_pool.getconn()
        _, raw_conn = self._physical_connection_id(conn)
        discarded = False
        try:
            conn.autocommit = False
            self._initialize_session_settings(conn)
            yield conn
        except Exception as exc:
            if self.is_transient_connection_error(exc):
                discarded = True
                self.discard_connection(raw_conn)
            raise
        finally:
            try:
                conn.rollback()
            except Exception:
                pass
            if not discarded:
                self.connection_pool.putconn(raw_conn)

    def close(self):
        lib = getattr(self, "_rust_hotpath_lib_handle", None)
        repo = getattr(self, "_rust_pg_repo_handle", None)
        if lib is not None and repo is not None:
            try:
                lib.dbfs_rust_pg_repo_free(repo)
            except Exception:
                pass
        self._rust_pg_repo_handle = None
        self.connection_pool.closeall()
        self._session_initialized_connection_ids.clear()

    def is_transient_connection_error(self, exc):
        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def discard_connection(self, conn):
        if conn is None:
            return
        raw_conn = getattr(conn, "_dbfs_raw_connection", conn)
        self._session_initialized_connection_ids.discard(id(raw_conn))
        try:
            self.connection_pool.putconn(raw_conn, close=True)
        except Exception:
            pass

    def get_config_value(self, key, default=None):
        rust_value = self.python_to_rust_pg_get_config_value(key)
        if rust_value is not None:
            return rust_value

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = %s", (key,))
            result = cur.fetchone()
            return result[0] if result else default

    def is_in_recovery(self):
        rust_value = self.python_to_rust_pg_is_in_recovery()
        if rust_value is not None:
            return rust_value

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            result = cur.fetchone()
            return bool(result[0]) if result else False

    def schema_version(self):
        rust_value = self.python_to_rust_pg_schema_version()
        if rust_value is not None:
            return rust_value

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
            result = cur.fetchone()
            return int(result[0]) if result else None

    def schema_is_initialized(self):
        rust_value = self.python_to_rust_pg_schema_is_initialized()
        if rust_value is not None:
            return rust_value

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    to_regclass('public.directories') IS NOT NULL
                    AND to_regclass('public.files') IS NOT NULL
                    AND to_regclass('public.schema_version') IS NOT NULL
                """
            )
            result = cur.fetchone()
            return bool(result[0]) if result else False
