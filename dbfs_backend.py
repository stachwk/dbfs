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


class RustPersistBlockInput(ctypes.Structure):
    _fields_ = [
        ("block_index", ctypes.c_uint64),
        ("ptr", ctypes.POINTER(ctypes.c_ubyte)),
        ("len", ctypes.c_size_t),
        ("used_len", ctypes.c_uint64),
    ]


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
                Path(__file__).resolve().parent / "rust_hotpath" / "target" / "debug" / "libdbfs_rust_hotpath.so",
                Path(__file__).resolve().parent / "rust_hotpath" / "target" / "release" / "libdbfs_rust_hotpath.so",
                Path("/usr/local/lib/libdbfs_rust_hotpath.so"),
                Path("/usr/local/lib/libdbfs-2.so"),
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
        lib.dbfs_rust_pg_repo_count_file_blocks.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_file_blocks.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_file_data_object_id.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_file_data_object_id.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_file_size.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_file_size.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_load_block.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_load_block.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_fetch_block_range.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(DbfsReadBlock)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_fetch_block_range.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_assemble_file_slice.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_assemble_file_slice.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_create_data_object.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_create_data_object.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_touch_data_object.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_touch_data_object.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_persist_copy_block_crc_rows.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(RustPersistBlockInput),
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_persist_copy_block_crc_rows.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_persist_file_blocks.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.POINTER(RustPersistBlockInput),
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_persist_file_blocks.restype = ctypes.c_int
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
        lib.dbfs_rust_pg_repo_count_directory_children.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_directory_children.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_directory_subdirs.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_directory_subdirs.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_root_directory_children.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_root_directory_children.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_symlinks.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_symlinks.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_load_symlink_target.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_load_symlink_target.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_get_special_file_metadata.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_get_special_file_metadata.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_files.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_files.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_count_directories.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_count_directories.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_total_data_size.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_total_data_size.restype = ctypes.c_int
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
        lib.dbfs_rust_pg_repo_fetch_xattr_value.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_fetch_xattr_value.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_list_xattr_names_for_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_ubyte),
        ]
        lib.dbfs_rust_pg_repo_list_xattr_names_for_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_store_xattr_value_for_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_store_xattr_value_for_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_owner_xattrs.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_delete_owner_xattrs.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_remove_xattr_for_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.dbfs_rust_pg_repo_remove_xattr_for_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_file_mode.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_file_mode.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_directory_mode.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_directory_mode.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_file_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_file_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_directory_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_directory_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_symlink_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        lib.dbfs_rust_pg_repo_update_symlink_owner.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_touch_file_times.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_touch_file_times.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_touch_directory_times.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_touch_directory_times.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_file_access_date.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_file_access_date.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_update_directory_access_date.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_update_directory_access_date.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_append_journal_event.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_append_journal_event.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_ensure_lock_schema.argtypes = [ctypes.c_void_p]
        lib.dbfs_rust_pg_repo_ensure_lock_schema.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_prune_lock_leases.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_rust_pg_repo_prune_lock_leases.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_lock_lease.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_delete_lock_lease.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_prune_lock_range_leases.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_rust_pg_repo_prune_lock_range_leases.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_range_leases.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_rust_pg_repo_delete_range_leases.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_acquire_flock_lease.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int64,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_int32,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_acquire_flock_lease.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_release_flock_lease.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_release_flock_lease.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_try_advisory_xact_lock.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int64,
        ]
        lib.dbfs_rust_pg_repo_try_advisory_xact_lock.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_heartbeat_lock_lease.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_heartbeat_lock_lease.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_heartbeat_lock_range_lease.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_heartbeat_lock_range_lease.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_load_lock_range_state_blob.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_load_lock_range_state_blob.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_persist_lock_range_state_blob.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_persist_lock_range_state_blob.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_touch_directory_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_touch_directory_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_list_directory_entries.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_list_directory_entries.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_fetch_path_attrs.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_rust_pg_repo_fetch_path_attrs.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_rename_file_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_rename_file_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_rename_hardlink_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_rename_hardlink_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_rename_symlink_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_rename_symlink_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_rename_directory_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_rename_directory_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_hardlink_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_delete_hardlink_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_symlink_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_delete_symlink_entry.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_delete_directory_entry.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
        ]
        lib.dbfs_rust_pg_repo_delete_directory_entry.restype = ctypes.c_int
        lib.dbfs_free_bytes.argtypes = [ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
        lib.dbfs_free_bytes.restype = None
        lib.dbfs_free_read_blocks.argtypes = [ctypes.POINTER(DbfsReadBlock), ctypes.c_size_t]
        lib.dbfs_free_read_blocks.restype = None

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
