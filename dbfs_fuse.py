#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ctypes
from collections import OrderedDict
import fcntl
import logging
import os
import uuid
import select
import struct
import stat
import time
import errno
import termios
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from fuse import FuseOSError, Operations
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import threading

from dbfs_backend import PostgresBackend
from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_schema import SCHEMA_VERSION
from dbfs_identity import (
    compute_device_id as identity_compute_device_id,
    current_group_ids as identity_current_group_ids,
    current_uid_gid as identity_current_uid_gid,
    ctime_column as identity_ctime_column,
    generate_inode_seed as identity_generate_inode_seed,
    inherited_directory_mode as identity_inherited_directory_mode,
    logical_inode as identity_logical_inode,
    normalize_path as identity_normalize_path,
    stable_inode as identity_stable_inode,
)
from dbfs_namespace import NamespaceSupport
from dbfs_xattr_acl import XattrAclSupport
from dbfs_locking import LockingSupport
from dbfs_permissions import PermissionPolicy
from dbfs_repository import NamespaceRepository
from dbfs_time import db_timestamp_to_epoch, epoch_to_utc_datetime
from dbfs_storage import StorageSupport
from dbfs_metadata import MetadataSupport
from dbfs_journal import JournalSupport
from dbfs_xattr_store import XattrStore
from dbfs_runtime_validation import validate_runtime_config

def configure_logging(level_name=None):
    resolved_level = (level_name or os.environ.get("DBFS_LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, resolved_level, logging.INFO)
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    return level


class DBFS(Operations):
    DEFAULT_WRITE_FLUSH_THRESHOLD_BYTES = 64 * 1024 * 1024

    def __init__(self, dsn, db_config, runtime_config=None, selinux_mode="off", acl_mode="off", role="auto", pool_max_connections=10):
        self.dsn = dsn
        self.db_config = db_config
        self.runtime_config = validate_runtime_config(runtime_config or {})
        self.instance_id = uuid.uuid4().hex
        self.synchronous_commit = self.resolve_synchronous_commit()
        self.backend = PostgresBackend(
            self.dsn,
            self.db_config,
            pool_max_connections=pool_max_connections,
            synchronous_commit=self.synchronous_commit,
        )
        self.pool_max_connections = self.backend.pool_max_connections
        self.connection_pool = self.backend.connection_pool
        self.xattr_acl = XattrAclSupport(self)
        self.default_block_size = 4096
        self.block_size = self.load_block_size()
        self.default_max_fs_size_bytes = 10 * 1024**3
        self.write_flush_threshold_bytes = self.resolve_write_flush_threshold_bytes()
        self.read_cache_max_blocks = self.resolve_read_cache_max_blocks()
        self.read_ahead_blocks = self.resolve_read_ahead_blocks()
        self.sequential_read_ahead_blocks = self.resolve_sequential_read_ahead_blocks()
        self.small_file_read_threshold_blocks = self.resolve_small_file_read_threshold_blocks()
        self.workers_read = self.resolve_workers_read()
        self.workers_read_min_blocks = self.resolve_workers_read_min_blocks()
        self.workers_write = self.resolve_workers_write()
        self.workers_write_min_blocks = self.resolve_workers_write_min_blocks()
        self.persist_buffer_chunk_blocks = self.resolve_persist_buffer_chunk_blocks()
        self.copy_skip_unchanged_blocks = self.resolve_copy_skip_unchanged_blocks()
        self.copy_skip_unchanged_blocks_min_blocks = self.resolve_copy_skip_unchanged_blocks_min_blocks()
        self.metadata_cache_ttl_seconds = self.resolve_metadata_cache_ttl_seconds()
        self.statfs_cache_ttl_seconds = self.resolve_statfs_cache_ttl_seconds()
        self.lock_backend = self.resolve_lock_backend()
        self.lock_lease_ttl_seconds = self.resolve_lock_lease_ttl_seconds()
        self.lock_heartbeat_interval_seconds = self.resolve_lock_heartbeat_interval_seconds()
        self.lock_poll_interval_seconds = self.resolve_lock_poll_interval_seconds()
        self.device_id = self.compute_device_id()
        self.selinux_mode = (selinux_mode or "off").lower()
        self.selinux_enabled = self.resolve_selinux_mode(self.selinux_mode)
        self.acl_mode = (acl_mode or "off").lower()
        self.acl_enabled = self.resolve_acl_mode(self.acl_mode)
        self.atime_policy = "default"
        self.profile_io = self.env_flag("DBFS_PROFILE_IO", False)
        self._io_profile = {
            "write": {"count": 0, "seconds": 0.0, "bytes": 0, "blocks": 0, "max_seconds": 0.0},
            "persist_buffer": {"count": 0, "seconds": 0.0, "bytes": 0, "blocks": 0, "max_seconds": 0.0},
            "flush": {"count": 0, "seconds": 0.0, "bytes": 0, "max_seconds": 0.0},
            "release": {"count": 0, "seconds": 0.0, "bytes": 0, "max_seconds": 0.0},
            "fsync": {"count": 0, "seconds": 0.0, "bytes": 0, "max_seconds": 0.0},
        }
        self.requested_role = (role or "auto").lower()
        self.role = self.resolve_runtime_role(self.requested_role)
        self.read_only = self.role == "replica"
        self._path_locks_guard = threading.RLock()
        self._seek_positions = {}
        self._timestamp_touch_guard = threading.RLock()
        self._timestamp_touch_once = {}
        self._read_block_cache_guard = threading.RLock()
        self._read_block_cache = OrderedDict()
        self._read_sequence_guard = threading.RLock()
        self._read_sequence_state = {}
        self._attr_cache_guard = threading.RLock()
        self._attr_cache = {}
        self._dir_entries_cache_guard = threading.RLock()
        self._dir_entries_cache = {}
        self._statfs_cache_guard = threading.RLock()
        self._statfs_cache = None
        self._namespace_epoch_guard = threading.RLock()
        self._namespace_epoch = time.time()
        self.locking = LockingSupport(
            self,
            backend_mode=self.lock_backend,
            lease_ttl_seconds=self.lock_lease_ttl_seconds,
            heartbeat_interval_seconds=self.lock_heartbeat_interval_seconds,
            poll_interval_seconds=self.lock_poll_interval_seconds,
        ).bind_state(self._path_locks_guard, self._seek_positions)
        self._path_locks = self.locking._path_locks
        self._flock_locks = self.locking._flock_locks
        self.permissions = PermissionPolicy(self)
        self.namespace = NamespaceSupport(self)
        self.metadata = MetadataSupport(self)
        self.journal = JournalSupport(self)
        self.logging = logging
        self.FuseOSError = FuseOSError
        self.repository = NamespaceRepository(self)
        if not self.backend.schema_is_initialized():
            raise RuntimeError(
                "DBFS schema is not initialized. Run `make init` first, or restore the schema-admin secret and rerun init/upgrade."
            )
        schema_version = self.backend.schema_version()
        if schema_version != SCHEMA_VERSION:
            raise RuntimeError(
                f"DBFS schema version mismatch: database has {schema_version}, code expects {SCHEMA_VERSION}. "
                "Run `mkfs.dbfs.py upgrade` with the schema-admin secret before mounting."
            )
        self.storage = StorageSupport(self)
        self.xattr_store = XattrStore(self)
        self._open_flags = {}
        self._handle_guard = threading.RLock()
        self._fh_to_file_id = {}
        self._next_fh = 1
        self._destroyed = False

    def current_uid_gid(self):
        return identity_current_uid_gid()

    def current_group_ids(self):
        return identity_current_group_ids()

    def ctime_column(self, table_name):
        return identity_ctime_column(table_name)

    def creation_uid_gid(self, parent_path):
        uid, gid = self.current_uid_gid()
        parent_path = self.normalize_path(parent_path)
        if parent_path != "/":
            try:
                parent_attrs = self.getattr(parent_path)
            except Exception:
                return uid, gid
            if parent_attrs["st_mode"] & stat.S_ISGID:
                gid = parent_attrs["st_gid"]
        return uid, gid

    def inherited_directory_mode(self, parent_path, mode):
        return identity_inherited_directory_mode(parent_path, mode, self.getattr, self.normalize_path)

    def compute_device_id(self):
        return identity_compute_device_id(self.db_config)

    def close(self):
        self.backend.close()

    def generate_inode_seed(self):
        return identity_generate_inode_seed()

    def logical_inode(self, obj_type, entry_id):
        return identity_logical_inode(obj_type, entry_id)

    def stable_inode(self, obj_type, inode_seed, entry_id):
        return identity_stable_inode(obj_type, inode_seed, entry_id)

    def host_selinux_enabled(self):
        if os.path.exists("/sys/fs/selinux/enforce"):
            return True

        try:
            import selinux  # type: ignore

            return bool(selinux.is_selinux_enabled())
        except Exception:
            return False

    def resolve_selinux_mode(self, selinux_mode):
        if selinux_mode == "off":
            return False
        if selinux_mode == "on":
            return True
        return self.host_selinux_enabled()

    def resolve_acl_mode(self, acl_mode):
        if acl_mode == "on":
            return True
        return False

    def resolve_write_flush_threshold_bytes(self):
        raw_value = os.environ.get("DBFS_WRITE_FLUSH_THRESHOLD_BYTES")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("write_flush_threshold_bytes", self.DEFAULT_WRITE_FLUSH_THRESHOLD_BYTES)
            return max(1, int(value) if value is not None else self.DEFAULT_WRITE_FLUSH_THRESHOLD_BYTES)
        try:
            threshold = int(raw_value)
        except Exception:
            return self.DEFAULT_WRITE_FLUSH_THRESHOLD_BYTES
        return max(1, threshold)

    def resolve_read_cache_max_blocks(self):
        raw_value = os.environ.get("DBFS_READ_CACHE_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("read_cache_blocks", 1024)
            return max(1, int(value) if value is not None else 1024)
        try:
            value = int(raw_value)
        except Exception:
            return 1024
        if value <= 0:
            raise ValueError("DBFS_READ_CACHE_BLOCKS must be > 0")
        return value

    def resolve_read_ahead_blocks(self):
        raw_value = os.environ.get("DBFS_READ_AHEAD_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("read_ahead_blocks", 4)
            return max(0, int(value) if value is not None else 4)
        try:
            value = int(raw_value)
        except Exception:
            return 4
        if value < 0:
            raise ValueError("DBFS_READ_AHEAD_BLOCKS must be >= 0")
        return value

    def resolve_sequential_read_ahead_blocks(self):
        raw_value = os.environ.get("DBFS_SEQUENTIAL_READ_AHEAD_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("sequential_read_ahead_blocks", 8)
            return max(0, int(value) if value is not None else 8)
        try:
            value = int(raw_value)
        except Exception:
            return 8
        if value < 0:
            raise ValueError("DBFS_SEQUENTIAL_READ_AHEAD_BLOCKS must be >= 0")
        return value

    def resolve_small_file_read_threshold_blocks(self):
        raw_value = os.environ.get("DBFS_SMALL_FILE_READ_THRESHOLD_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("small_file_read_threshold_blocks", 8)
            return max(0, int(value) if value is not None else 8)
        try:
            value = int(raw_value)
        except Exception:
            return 8
        if value < 0:
            raise ValueError("DBFS_SMALL_FILE_READ_THRESHOLD_BLOCKS must be >= 0")
        return value

    def resolve_workers_read(self):
        raw_value = os.environ.get("DBFS_WORKERS_READ")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("workers_read", 4)
            return max(1, int(value) if value is not None else 4)
        try:
            value = int(raw_value)
        except Exception:
            return 4
        if value < 1:
            raise ValueError("DBFS_WORKERS_READ must be >= 1")
        return value

    def resolve_workers_read_min_blocks(self):
        raw_value = os.environ.get("DBFS_WORKERS_READ_MIN_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("workers_read_min_blocks", 8)
            return max(1, int(value) if value is not None else 8)
        try:
            value = int(raw_value)
        except Exception:
            return 8
        if value < 1:
            raise ValueError("DBFS_WORKERS_READ_MIN_BLOCKS must be >= 1")
        return value

    def resolve_workers_write(self):
        raw_value = os.environ.get("DBFS_WORKERS_WRITE")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("workers_write", 4)
            return max(1, int(value) if value is not None else 4)
        try:
            value = int(raw_value)
        except Exception:
            return 4
        if value < 1:
            raise ValueError("DBFS_WORKERS_WRITE must be >= 1")
        return value

    def resolve_workers_write_min_blocks(self):
        raw_value = os.environ.get("DBFS_WORKERS_WRITE_MIN_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("workers_write_min_blocks", 8)
            return max(1, int(value) if value is not None else 8)
        try:
            value = int(raw_value)
        except Exception:
            return 8
        if value < 1:
            raise ValueError("DBFS_WORKERS_WRITE_MIN_BLOCKS must be >= 1")
        return value

    def resolve_persist_buffer_chunk_blocks(self):
        raw_value = os.environ.get("DBFS_PERSIST_BUFFER_CHUNK_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("persist_buffer_chunk_blocks", 128)
            return max(1, int(value) if value is not None else 128)
        try:
            value = int(raw_value)
        except Exception:
            return 128
        if value < 1:
            raise ValueError("DBFS_PERSIST_BUFFER_CHUNK_BLOCKS must be >= 1")
        return value

    def resolve_copy_skip_unchanged_blocks(self):
        raw_value = os.environ.get("DBFS_COPY_SKIP_UNCHANGED_BLOCKS")
        if raw_value is None or raw_value == "":
            return bool(self.runtime_config_getbool("copy_skip_unchanged_blocks", False))
        return self.parse_bool_value(raw_value, "DBFS_COPY_SKIP_UNCHANGED_BLOCKS")

    def resolve_copy_skip_unchanged_blocks_min_blocks(self):
        raw_value = os.environ.get("DBFS_COPY_SKIP_UNCHANGED_BLOCKS_MIN_BLOCKS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("copy_skip_unchanged_blocks_min_blocks", 16)
            return max(1, int(value) if value is not None else 16)
        try:
            value = int(raw_value)
        except Exception:
            return 16
        if value < 1:
            raise ValueError("DBFS_COPY_SKIP_UNCHANGED_BLOCKS_MIN_BLOCKS must be >= 1")
        return value

    def resolve_metadata_cache_ttl_seconds(self):
        raw_value = os.environ.get("DBFS_METADATA_CACHE_TTL_SECONDS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("metadata_cache_ttl_seconds", 1)
            return max(0.0, float(value) if value is not None else 1.0)
        try:
            value = float(raw_value)
        except Exception:
            return 1.0
        if value < 0:
            raise ValueError("DBFS_METADATA_CACHE_TTL_SECONDS must be >= 0")
        return value

    def resolve_statfs_cache_ttl_seconds(self):
        raw_value = os.environ.get("DBFS_STATFS_CACHE_TTL_SECONDS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("statfs_cache_ttl_seconds", 2)
            return max(0.0, float(value) if value is not None else 2.0)
        try:
            value = float(raw_value)
        except Exception:
            return 2.0
        if value < 0:
            raise ValueError("DBFS_STATFS_CACHE_TTL_SECONDS must be >= 0")
        return value

    def resolve_fuse_cache_timeout_seconds(self, env_name, default_value=0.0):
        raw_value = os.environ.get(env_name)
        if raw_value is None or raw_value == "":
            return float(default_value)
        try:
            value = float(raw_value)
        except Exception:
            return float(default_value)
        if value < 0:
            raise ValueError(f"{env_name} must be >= 0")
        return value

    def resolve_lock_backend(self):
        raw_value = os.environ.get("DBFS_LOCK_BACKEND")
        configured_value = self.runtime_config_get("lock_backend", "postgres_lease")
        if raw_value and raw_value.strip().lower() != "postgres_lease":
            logging.warning("DBFS_LOCK_BACKEND=%s is ignored; postgres_lease is the only supported backend", raw_value)
        if configured_value and str(configured_value).strip().lower() != "postgres_lease":
            logging.warning("lock_backend=%s is ignored; postgres_lease is the only supported backend", configured_value)
        return "postgres_lease"

    def resolve_synchronous_commit(self):
        raw_value = os.environ.get("DBFS_SYNCHRONOUS_COMMIT")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_get("synchronous_commit", "on")
        else:
            value = raw_value
        value = "on" if value in {None, ""} else str(value).strip().lower()
        allowed = {"on", "off", "local", "remote_write", "remote_apply"}
        if value not in allowed:
            allowed_values = ", ".join(sorted(allowed))
            raise ValueError(f"synchronous_commit must be one of: {allowed_values}")
        return value

    def resolve_lock_lease_ttl_seconds(self):
        raw_value = os.environ.get("DBFS_LOCK_LEASE_TTL_SECONDS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("lock_lease_ttl_seconds", 30)
            return max(1, int(value) if value is not None else 30)
        try:
            value = int(raw_value)
        except Exception:
            return 30
        if value < 1:
            raise ValueError("DBFS_LOCK_LEASE_TTL_SECONDS must be >= 1")
        return value

    def resolve_lock_heartbeat_interval_seconds(self):
        raw_value = os.environ.get("DBFS_LOCK_HEARTBEAT_INTERVAL_SECONDS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_getint("lock_heartbeat_interval_seconds", 10)
            return max(1, int(value) if value is not None else 10)
        try:
            value = int(raw_value)
        except Exception:
            return 10
        if value < 1:
            raise ValueError("DBFS_LOCK_HEARTBEAT_INTERVAL_SECONDS must be >= 1")
        return value

    def resolve_lock_poll_interval_seconds(self):
        raw_value = os.environ.get("DBFS_LOCK_POLL_INTERVAL_SECONDS")
        if raw_value is None or raw_value == "":
            value = self.runtime_config_get("lock_poll_interval_seconds", 0.1)
        else:
            value = raw_value
        try:
            value = float(value)
        except Exception:
            return 0.1
        if value <= 0:
            raise ValueError("DBFS_LOCK_POLL_INTERVAL_SECONDS must be > 0")
        return value

    def normalize_xattr_name(self, name):
        return self.xattr_store.normalize_xattr_name(name)

    def normalize_xattr_value(self, value):
        return self.xattr_store.normalize_xattr_value(value)

    def is_selinux_xattr(self, name):
        return self.xattr_store.is_selinux_xattr(name)

    def is_posix_acl_xattr(self, name):
        return self.xattr_store.is_posix_acl_xattr(name)

    def parse_posix_acl_xattr(self, value):
        return self.xattr_store.parse_posix_acl_xattr(value)

    def build_posix_acl_xattr(self, entries):
        return self.xattr_store.build_posix_acl_xattr(entries)

    def fetch_xattr_value(self, path, name, cur=None):
        return self.xattr_store.fetch_xattr_value(path, name, cur=cur)

    def store_xattr_value(self, path, name, value, cur):
        return self.xattr_store.store_xattr_value(path, name, value, cur)

    def copy_default_acl_to_child(self, parent_path, child_path, child_is_dir, cur, owner_key=None):
        return self.xattr_store.copy_default_acl_to_child(parent_path, child_path, child_is_dir, cur, owner_key=owner_key)

    def acl_permission_from_entries(self, entries, attrs, required_mode):
        return self.xattr_acl.acl_permission_from_entries(entries, attrs, required_mode)

    def acl_allows(self, path, attrs, mode):
        return self.xattr_acl.acl_allows(path, attrs, mode)

    def estimate_blocks(self, obj_type, size, entry_id=None):
        size = max(0, int(size))
        if obj_type == "dir":
            child_count = self.count_directory_children(entry_id) if entry_id is not None else 0
            estimated_bytes = 256 + (child_count * 128)
            return max(1, (estimated_bytes + 511) // 512)
        if obj_type in {"file", "hardlink"}:
            if size <= 0:
                return 0
            return max(1, (size + 511) // 512)
        if obj_type == "symlink":
            if size <= 0:
                return 0
            return max(1, (size + 511) // 512)
        return max(0, (size + 511) // 512)

    def normalize_path(self, path):
        return identity_normalize_path(path)

    def _lock_owner_key(self, fh):
        return self.locking._lock_owner_key(fh)

    def _normalize_lock_range(self, path, fh, lock):
        return self.locking._normalize_lock_range(path, fh, lock)

    @staticmethod
    def _ranges_overlap(start_a, end_a, start_b, end_b):
        return LockingSupport._ranges_overlap(start_a, end_a, start_b, end_b)

    def _lock_records(self, resource_state):
        return self.locking._lock_records(resource_state)

    @staticmethod
    def _range_to_end(start, end):
        return LockingSupport._range_to_end(start, end)

    def _iter_lock_records(self, resource_state):
        return self.locking._iter_lock_records(resource_state)

    def _lock_record_conflicts(self, record, requested_type, requested_start, requested_end):
        return self.locking._lock_record_conflicts(record, requested_type, requested_start, requested_end)

    def _unlock_lock_records(self, records, owner_key, unlock_start, unlock_end):
        return self.locking._unlock_lock_records(records, owner_key, unlock_start, unlock_end)

    def _merge_lock_record(self, records, owner_key, new_record):
        return self.locking._merge_lock_record(records, owner_key, new_record)

    def _get_lock_state(self, resource_key):
        return self.locking._get_lock_state(resource_key)

    @staticmethod
    def _get_lock_field(lock, name, default=None):
        return LockingSupport._get_lock_field(lock, name, default)

    @staticmethod
    def _set_lock_field(lock, name, value):
        return LockingSupport._set_lock_field(lock, name, value)

    def _clear_path_lock_state(self, resource_key, owner_key=None):
        return self.locking._clear_path_lock_state(resource_key, owner_key)

    def _get_flock_state(self, resource_key):
        return self.locking._get_flock_state(resource_key)

    def _clear_flock_state(self, resource_key, owner_key=None):
        return self.locking._clear_flock_state(resource_key, owner_key)

    def cleanup_resources(self):
        if self._destroyed:
            return
        self._destroyed = True

        file_ids_to_flush = set()

        if hasattr(self, "_write_states"):
            for file_id, state in self._write_states.items():
                if state.get("dirty_blocks") or state.get("truncate_pending", False):
                    file_ids_to_flush.add(file_id)

        for file_id in list(file_ids_to_flush):
            try:
                self.persist_buffer(file_id)
            except Exception as exc:
                logging.debug("Failed to persist buffer during cleanup for %s: %s", file_id, exc)

        self.storage.cleanup()
        self.locking.cleanup()
        self.clear_timestamp_touch_state()
        self.clear_read_sequence_state()
        if self.profile_io:
            self.log_io_profile_summary()

        with self._handle_guard:
            self._fh_to_file_id.clear()

        try:
            self.backend.close()
        except Exception as exc:
            logging.debug("Failed to close DB connection pool during cleanup: %s", exc)
            
    def _lock_resource_key(self, path):
        return self.locking._lock_resource_key(path)

    def _lock_conflicts(self, state, request_type):
        return self.locking._lock_conflicts(state, request_type)

    def _describe_conflicting_lock(self, state, requested_type):
        return self.locking._describe_conflicting_lock(state, requested_type)

    def resolve_runtime_role(self, requested_role):
        if requested_role in {"primary", "replica"}:
            return requested_role

        try:
            return "replica" if self.backend.is_in_recovery() else "primary"
        except Exception as exc:
            logging.warning("Unable to detect DBFS role from PostgreSQL; defaulting to primary: %s", exc)
            return "primary"

    def require_writable(self):
        if self.read_only:
            raise FuseOSError(errno.EROFS)

    def env_flag(self, name, default=False):
        value = os.environ.get(name)
        if value is None:
            return default
        return value not in {"0", "false", "False", "no", "off"}

    def parse_bool_value(self, value, name):
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"{name} must be a boolean value")

    def runtime_config_get(self, key, default=None):
        config = self.runtime_config
        if isinstance(config, dict):
            value = config.get(key, default)
            return default if value is None else value
        if hasattr(config, "get"):
            try:
                value = config.get(key, fallback=default)
                return default if value is None else value
            except Exception:
                return default
        return default

    def runtime_config_getint(self, key, default=None):
        config = self.runtime_config
        if isinstance(config, dict):
            value = config.get(key, default)
            if value is None:
                return default
            try:
                return int(value)
            except Exception:
                return default
        if hasattr(config, "getint"):
            try:
                return config.getint(key, fallback=default)
            except Exception:
                return default
        value = self.runtime_config_get(key, default)
        if value is None:
            return default
        try:
            return int(value)
        except Exception:
            return default

    def runtime_config_getbool(self, key, default=None):
        config = self.runtime_config
        if isinstance(config, dict):
            value = config.get(key, default)
        elif hasattr(config, "get"):
            try:
                value = config.get(key, fallback=default)
            except Exception:
                return default
        else:
            value = default
        if value is None or value == "":
            return default
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return default

    def apply_mount_options(self, mount_kwargs, args):
        self.atime_policy = getattr(args, "atime_policy", "default") or "default"
        mount_kwargs["use_ino"] = True
        mount_kwargs["ro"] = self.read_only
        mount_kwargs["entry_timeout"] = self.resolve_fuse_cache_timeout_seconds("DBFS_ENTRY_TIMEOUT_SECONDS", 0.0)
        mount_kwargs["attr_timeout"] = self.resolve_fuse_cache_timeout_seconds("DBFS_ATTR_TIMEOUT_SECONDS", 0.0)
        mount_kwargs["negative_timeout"] = self.resolve_fuse_cache_timeout_seconds("DBFS_NEGATIVE_TIMEOUT_SECONDS", 0.0)
        if args.default_permissions:
            mount_kwargs["default_permissions"] = True
        if self.env_flag("DBFS_ALLOW_OTHER", False):
            mount_kwargs["allow_other"] = True

        if getattr(args, "lazytime", False):
            mount_kwargs["lazytime"] = True
        if getattr(args, "sync", False):
            mount_kwargs["sync"] = True
        if getattr(args, "dirsync", False):
            mount_kwargs["dirsync"] = True

        selinux_context = os.environ.get("DBFS_SELINUX_CONTEXT")
        selinux_fscontext = os.environ.get("DBFS_SELINUX_FSCONTEXT")
        selinux_defcontext = os.environ.get("DBFS_SELINUX_DEFCONTEXT")
        selinux_rootcontext = os.environ.get("DBFS_SELINUX_ROOTCONTEXT")
        if selinux_context:
            mount_kwargs["context"] = selinux_context
        if selinux_fscontext:
            mount_kwargs["fscontext"] = selinux_fscontext
        if selinux_defcontext:
            mount_kwargs["defcontext"] = selinux_defcontext
        if selinux_rootcontext:
            mount_kwargs["rootcontext"] = selinux_rootcontext

        return mount_kwargs

    def should_update_file_atime(self):
        return self.atime_policy not in {"noatime"}

    def should_update_dir_atime(self):
        return self.atime_policy not in {"noatime", "nodiratime"}

    def update_access_date(self, table_name, id_column, entry_id, timestamp):
        with self.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table_name} SET access_date = %s WHERE {id_column} = %s",
                (epoch_to_utc_datetime(timestamp).replace(tzinfo=None), entry_id),
            )
            conn.commit()

    def update_access_date_once(self, table_name, id_column, entry_id, timestamp, touch_key):
        with self._timestamp_touch_guard:
            touched = self._timestamp_touch_once.setdefault(touch_key, set())
            if "access_date" in touched:
                return False
        self.update_access_date(table_name, id_column, entry_id, timestamp)
        with self._timestamp_touch_guard:
            self._timestamp_touch_once.setdefault(touch_key, set()).add("access_date")
        return True

    def clear_timestamp_touch_state(self, touch_key=None):
        with self._timestamp_touch_guard:
            if touch_key is None:
                self._timestamp_touch_once.clear()
            else:
                self._timestamp_touch_once.pop(touch_key, None)

    def clear_read_sequence_state(self, file_id=None):
        with self._read_sequence_guard:
            if file_id is None:
                self._read_sequence_state.clear()
            else:
                self._read_sequence_state.pop(file_id, None)

    def clear_metadata_cache(self, path=None):
        return self.metadata.clear_metadata_cache(path)

    def clear_attr_cache(self, path=None):
        return self.metadata.clear_attr_cache(path)

    def clear_dir_cache(self, path=None):
        return self.metadata.clear_dir_cache(path)

    def clear_statfs_cache(self):
        return self.metadata.clear_statfs_cache()

    def invalidate_metadata_cache(self, path=None, include_statfs=False):
        return self.metadata.invalidate_metadata_cache(path, include_statfs=include_statfs)

    def _attr_cache_get(self, path):
        return self.metadata._attr_cache_get(path)

    def _attr_cache_set(self, path, value):
        return self.metadata._attr_cache_set(path, value)

    def _dir_cache_get(self, path):
        return self.metadata._dir_cache_get(path)

    def _dir_cache_set(self, path, value):
        return self.metadata._dir_cache_set(path, value)

    # Kompatybilnosc wsteczna dla miejsc, ktore nadal uzywaja starej nazwy.
    # Stary cache ogolny mapujemy na cache atrybutow, aby nie wywalac getattr().
    def _metadata_cache_get(self, path):
        return self._attr_cache_get(path)

    def _metadata_cache_set(self, path, value):
        return self._attr_cache_set(path, value)

    def _statfs_cache_get(self):
        return self.metadata._statfs_cache_get()

    def _statfs_cache_set(self, value):
        return self.metadata._statfs_cache_set(value)

    def touch_namespace_epoch(self):
        with self._namespace_epoch_guard:
            self._namespace_epoch = time.time()

    def namespace_epoch(self):
        with self._namespace_epoch_guard:
            return self._namespace_epoch

    def file_mode_bits(self, mode):
        try:
            return int(mode, 8)
        except (TypeError, ValueError):
            return 0o644

    def decode_file_mode(self, mode):
        if isinstance(mode, str) and ":" in mode:
            parts = mode.split(":")
            type_tag = parts[0]
            if type_tag == "fifo" and len(parts) >= 2:
                return stat.S_IFIFO, self.file_mode_bits(parts[1]), 0
            if type_tag in {"char", "block"} and len(parts) >= 4:
                major = int(parts[2])
                minor = int(parts[3])
                rdev = os.makedev(major, minor) if hasattr(os, "makedev") else 0
                file_type = stat.S_IFCHR if type_tag == "char" else stat.S_IFBLK
                return file_type, self.file_mode_bits(parts[1]), rdev
        return stat.S_IFREG, self.file_mode_bits(mode), 0

    def encode_file_mode(self, file_type, mode_bits, rdev=0):
        mode_bits = int(mode_bits) & 0o777
        if file_type == stat.S_IFIFO:
            return f"fifo:{oct(mode_bits)[-3:]}"
        if file_type in {stat.S_IFCHR, stat.S_IFBLK}:
            major = os.major(rdev) if hasattr(os, "major") else 0
            minor = os.minor(rdev) if hasattr(os, "minor") else 0
            type_tag = "char" if file_type == stat.S_IFCHR else "block"
            return f"{type_tag}:{oct(mode_bits)[-3:]}:{major}:{minor}"
        return oct(mode_bits)[-3:]

    def required_mode_mask(self, mode):
        return self.permissions.required_mode_mask(mode)

    def can_access(self, attrs, mode):
        return self.permissions.can_access(attrs, mode)

    def enforce_sticky_bit(self, parent_path, entry_attrs):
        return self.permissions.enforce_sticky_bit(parent_path, entry_attrs)

    def can_modify_metadata(self, attrs):
        return self.permissions.can_modify_metadata(attrs)

    def can_change_owner(self, attrs, uid, gid):
        return self.permissions.can_change_owner(attrs, uid, gid)

    @contextmanager
    def db_connection(self):
        with self.backend.connection() as conn:
            yield conn

    def append_journal_event(self, cur, action, path=None, file_id=None, directory_id=None):
        return self.journal.append_journal_event(cur, action, path=path, file_id=file_id, directory_id=directory_id)

    def get_config_value(self, key, default=None):
        return self.backend.get_config_value(key, default)

    def load_block_size(self):
        try:
            block_size = self.get_config_value("block_size", self.default_block_size)
            return max(1, int(block_size))
        except Exception:
            return self.default_block_size

    def get_file_id(self, path):
        return self.repository.get_file_id(path)

    def get_file_mode_value(self, path):
        return self.repository.get_file_mode_value(path)

    def get_special_file_metadata(self, file_id):
        return self.metadata.get_special_file_metadata(file_id)

    def get_hardlink_id(self, path):
        return self.repository.get_hardlink_id(path)

    def get_hardlink_file_id(self, hardlink_id):
        return self.repository.get_hardlink_file_id(hardlink_id)

    def get_symlink_id(self, path):
        return self.repository.get_symlink_id(path)

    def get_dir_id_by_path(self, path):
        return self.repository.get_dir_id_by_path(path)

    def get_entry_kind_and_id(self, path):
        return self.repository.get_entry_kind_and_id(path)

    def entry_exists(self, path, entry_kind):
        return self.repository.entry_exists(path, entry_kind)

    def entry_exists_any(self, path):
        return self.repository.entry_exists_any(path)

    def count_file_links(self, file_id):
        return self.repository.count_file_links(file_id)

    def promote_hardlink_to_primary(self, file_id, cur):
        return self.repository.promote_hardlink_to_primary(file_id, cur)

    def load_file_bytes(self, file_id):
        return self.storage.load_file_bytes(file_id)

    def get_file_size(self, file_id):
        return self.storage.get_file_size(file_id)

    def load_symlink_target(self, symlink_id):
        return self.metadata.load_symlink_target(symlink_id)

    def path_has_children(self, directory_id):
        return self.storage.path_has_children(directory_id)

    def count_directory_children(self, directory_id):
        return self.metadata.count_directory_children(directory_id)

    def count_directory_subdirs(self, directory_id):
        return self.metadata.count_directory_subdirs(directory_id)

    def count_root_directory_children(self):
        return self.metadata.count_root_directory_children()

    def count_file_blocks(self, file_id):
        return self.metadata.count_file_blocks(file_id)

    def count_symlinks(self):
        return self.metadata.count_symlinks()

    def delete_path_xattrs(self, path, recursive=False, cur=None):
        return self.xattr_store.delete_inode_xattrs(path, cur=cur)

    def move_path_xattrs(self, old_path, new_path, recursive=False, cur=None):
        return self.xattr_store.move_path_xattrs(old_path, new_path, recursive=recursive, cur=cur)

    def ensure_write_buffer(self, file_id):
        return self.storage.ensure_write_buffer(file_id)

    def mark_write_buffer_dirty(self, file_id):
        self.storage.mark_write_buffer_dirty(file_id)

    def mark_write_range_dirty(self, file_id, start_offset, end_offset):
        self.storage.mark_write_range_dirty(file_id, start_offset, end_offset)

    def dirty_write_buffer_bytes(self, file_id):
        return self.storage.dirty_write_buffer_bytes(file_id)

    def maybe_flush_dirty_write_buffer(self, file_id):
        return self.storage.maybe_flush_dirty_write_buffer(file_id)

    def clear_write_buffer_dirty(self, file_id):
        return self.storage.clear_write_buffer_dirty(file_id)

    def is_write_buffer_dirty(self, file_id):
        return self.storage.is_write_buffer_dirty(file_id)

    def persist_buffer(self, file_id):
        return self.storage.persist_buffer(file_id)

    def read_file_slice(self, file_id, offset, size):
        return self.storage.read_file_slice(file_id, offset, size)

    def clear_read_cache(self, file_id=None):
        return self.storage.clear_read_cache(file_id)

    def allocate_file_handle(self, file_id, flags=0, initial_offset=0):
        with self._handle_guard:
            fh = self._next_fh
            self._next_fh += 1
            self._fh_to_file_id[fh] = int(file_id)
        self._open_flags[fh] = int(flags or 0)
        self._seek_positions[fh] = int(initial_offset or 0)
        return fh

    def file_id_for_handle(self, fh):
        if fh is None:
            return None
        with self._handle_guard:
            return self._fh_to_file_id.get(fh)

    def close_file_handle(self, fh):
        if fh is None:
            return
        with self._handle_guard:
            self._fh_to_file_id.pop(fh, None)
        self._open_flags.pop(fh, None)
        self._seek_positions.pop(fh, None)

    def record_io_profile(self, op_name, elapsed, bytes_count=0, blocks=0):
        if not self.profile_io:
            return
        stats = self._io_profile.get(op_name)
        if stats is None:
            stats = {"count": 0, "seconds": 0.0, "bytes": 0, "blocks": 0, "max_seconds": 0.0}
            self._io_profile[op_name] = stats
        stats["count"] = int(stats.get("count", 0)) + 1
        stats["seconds"] = float(stats.get("seconds", 0.0)) + float(elapsed)
        stats["bytes"] = int(stats.get("bytes", 0)) + int(bytes_count or 0)
        stats["blocks"] = int(stats.get("blocks", 0)) + int(blocks or 0)
        stats["max_seconds"] = max(float(stats.get("max_seconds", 0.0)), float(elapsed))

    def log_io_profile_summary(self):
        parts = []
        for op_name in ("write", "persist_buffer", "flush", "release", "fsync"):
            stats = self._io_profile.get(op_name, {})
            count = stats.get("count", 0)
            if not count:
                continue
            total_seconds = stats.get("seconds", 0.0)
            avg_seconds = total_seconds / count if count else 0.0
            parts.append(
                f"{op_name}:count={count} total={total_seconds:.6f}s avg={avg_seconds:.6f}s max={stats.get('max_seconds', 0.0):.6f}s bytes={stats.get('bytes', 0)} blocks={stats.get('blocks', 0)}"
            )
        if parts:
            logging.info("DBFS I/O profile: %s", " | ".join(parts))

    def require_directory(self, path):
        path = self.normalize_path(path)
        if path == "/":
            return path
        kind, _ = self.get_entry_kind_and_id(path)
        if kind is None:
            raise FuseOSError(errno.ENOENT)
        if kind != "dir":
            raise FuseOSError(errno.ENOTDIR)
        return path

    def readdir(self, path, fh):
        path = self.normalize_path(path)
        if path.startswith('/.Trash') or path.startswith('/.hidden'):
            return ['.', '..']
        if path != '/':
            self.require_directory(path)

        cached_entries = self._dir_cache_get(path)
        cached_directory_id = None

        if isinstance(cached_entries, dict):
            cached_directory_id = cached_entries.get('directory_id')
            cached_entries = list(cached_entries.get('entries', []))
        elif cached_entries is not None:
            cached_entries = list(cached_entries)

        if cached_entries is not None:
            if path != "/" and self.should_update_dir_atime():
                directory_id = cached_directory_id if cached_directory_id is not None else self.get_dir_id(path)
                if directory_id is not None:
                    touch_key = ("dir", fh if fh is not None else directory_id)
                    if self.update_access_date_once("directories", "id_directory", directory_id, time.time(), touch_key):
                        self.clear_metadata_cache(path)
            return ['.', '..'] + cached_entries

        listing = self.repository.list_directory_entries(path)
        directory_id = listing.get("directory_id")
        entries = list(listing.get("entries", []))

        if path != "/" and self.should_update_dir_atime():
            if directory_id is None:
                directory_id = self.get_dir_id(path)
            if directory_id is not None:
                touch_key = ("dir", fh if fh is not None else directory_id)
                if self.update_access_date_once("directories", "id_directory", directory_id, time.time(), touch_key):
                    self.clear_metadata_cache(path)

        self._dir_cache_set(path, {"entries": entries, "directory_id": directory_id})
        return ['.', '..'] + entries

    def getattr(self, path, fh=None):
        path = self.normalize_path(path)
        cached_attrs = self._attr_cache_get(path)
        if cached_attrs is not None and "st_ino" in cached_attrs and "st_mode" in cached_attrs:
            return cached_attrs

        now = time.time()

        if path == '/':
            uid, gid = self.current_uid_gid()
            root_child_count = self.count_root_directory_children()
            root_epoch = max(now, self.namespace_epoch())
            attrs = {
                'st_ino': self.stable_inode("dir", "root", 1),
                'st_mode': stat.S_IFDIR | 0o755,
                'st_nlink': 2 + root_child_count,
                'st_size': 0,
                'st_blocks': self.estimate_blocks("dir", 0, None) + max(0, root_child_count // 8),
                'st_blksize': self.default_block_size,
                'st_ctime': root_epoch,
                'st_mtime': root_epoch,
                'st_atime': now,
                'st_uid': uid,
                'st_gid': gid
            }
            self._attr_cache_set(path, attrs)
            return attrs

        if path.startswith('/.Trash') or path.startswith('/.hidden'):
            uid, gid = self.current_uid_gid()
            pseudo_child_count = 0
            attrs = {
                'st_ino': self.stable_inode("dir", f"pseudo:{path}", 2 if path.startswith('/.Trash') else 3),
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': 2,
                'st_size': 0,
                'st_blocks': self.estimate_blocks("dir", 0, None) + max(0, pseudo_child_count // 8),
                'st_blksize': self.default_block_size,
                'st_ctime': now,
                'st_mtime': now,
                'st_atime': now,
                'st_uid': uid,
                'st_gid': gid
            }
            self._attr_cache_set(path, attrs)
            return attrs

        attrs = self.repository.fetch_path_attrs(path, now=now)
        self._attr_cache_set(path, attrs)
        return attrs

    def access(self, path, mode):
        path = self.normalize_path(path)
        attrs = self.getattr(path)
        if self.acl_allows(path, attrs, mode):
            return 0
        raise FuseOSError(errno.EACCES)

    def get_dir_id(self, path):
        return self.repository.get_dir_id(path)

    def get_symlink_attrs(self, path):
        return self.repository.get_symlink_attrs(path)

    def mkdir(self, path, mode):
        return self.repository.mkdir(path, mode)

    def symlink(self, target, source):
        return self.repository.symlink(target, source)

    def link(self, target, source):
        return self.repository.link(target, source)

    def readlink(self, path):
        return self.repository.readlink(path)

    def read(self, path, size, offset, fh):
        path = self.normalize_path(path)
        file_id = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        data = self.read_file_slice(file_id, offset, size)

        if self.should_update_file_atime():
            now = time.time()
            touch_key = ("file", fh if fh is not None else file_id)
            if self.update_access_date_once("files", "id_file", file_id, now, touch_key):
                self.clear_metadata_cache(path)

        if fh is not None:
            self._seek_positions[fh] = offset + len(data)
        return data

    def read_buf(self, path, size, offset, fh):
        path = self.normalize_path(path)
        return self.read(path, size, offset, fh)

    def poll(self, path, fh=None, events=0):
        path = self.normalize_path(path)
        kind, _ = self.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.EINVAL)

        file_id = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        size = self.storage.get_logical_file_size(file_id)

        mask = 0
        if events == 0 or events & select.POLLIN:
            mask |= select.POLLIN
        if not self.read_only and (events == 0 or events & select.POLLOUT):
            mask |= select.POLLOUT
        if size == 0 and events & select.POLLPRI:
            mask |= select.POLLPRI
        return mask

    def lseek(self, path, offset, whence, fh=None):
        path = self.normalize_path(path)
        kind, _ = self.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.EINVAL)

        file_id = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        size = self.storage.get_logical_file_size(file_id)
        current_offset = self._seek_positions.get(fh, 0) if fh is not None else 0

        if whence == os.SEEK_SET:
            new_offset = offset
        elif whence == os.SEEK_CUR:
            new_offset = current_offset + offset
        elif whence == os.SEEK_END:
            new_offset = size + offset
        else:
            raise FuseOSError(errno.EINVAL)

        if new_offset < 0:
            raise FuseOSError(errno.EINVAL)
        return new_offset

    def write(self, path, buf, offset, fh):
        path = self.normalize_path(path)
        started = time.perf_counter() if self.profile_io else None

        if len(buf) == 0:
            return 0

        file_id = self.file_id_for_handle(fh)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        state = self.storage.ensure_write_state(file_id)

        if self._open_flags.get(fh, 0) & os.O_APPEND:
            offset = int(state["file_size"])

        result = self.storage.write_into_state(file_id, buf, offset)
        self._seek_positions[fh] = result["end_offset"]
        self.storage.maybe_flush_dirty_write_buffer(file_id)

        if started is not None:
            self.record_io_profile(
                "write",
                time.perf_counter() - started,
                bytes_count=len(buf),
                blocks=result["touched_blocks"],
            )

        return len(buf)

    def write_buf(self, path, buf, offset, fh):
        path = self.normalize_path(path)
        return self.write(path, buf, offset, fh)


    def flush_to_db(self, fh):
        self.require_writable()
        file_id = self.file_id_for_handle(fh)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)
        self.persist_buffer(file_id)

    def getxattr(self, path, name, position=0):
        path = self.normalize_path(path)
        xattr_name = self.normalize_xattr_name(name)
        entry_kind, _ = self.get_entry_kind_and_id(path)
        if entry_kind is None:
            raise FuseOSError(errno.ENOENT)
        if self.is_posix_acl_xattr(xattr_name) and not self.acl_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)
        if self.is_selinux_xattr(xattr_name) and not self.selinux_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)
        value = self.fetch_xattr_value(path, xattr_name)
        if value is None:
            raise FuseOSError(errno.ENODATA)
        return value

    def setxattr(self, path, name, value, options, position=0):
        path = self.normalize_path(path)
        self.require_writable()
        xattr_name = self.normalize_xattr_name(name)
        entry_kind, _ = self.get_entry_kind_and_id(path)
        if entry_kind is None:
            raise FuseOSError(errno.ENOENT)
        if self.is_posix_acl_xattr(xattr_name) and not self.acl_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)
        if self.is_selinux_xattr(xattr_name) and not self.selinux_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)
        if self.is_posix_acl_xattr(xattr_name):
            self.parse_posix_acl_xattr(value)

        with self.db_connection() as conn, conn.cursor() as cur:
            self.store_xattr_value(path, xattr_name, value, cur)
            conn.commit()
        self.invalidate_metadata_cache(path, include_statfs=False)
        return 0

    def listxattr(self, path):
        path = self.normalize_path(path)
        entry_kind, _ = self.get_entry_kind_and_id(path)
        if entry_kind is None:
            raise FuseOSError(errno.ENOENT)
        names = self.xattr_store.list_xattr_names(path)
        filtered = []
        for xattr_name in names:
            if self.is_posix_acl_xattr(xattr_name) and not self.acl_enabled:
                continue
            if self.is_selinux_xattr(xattr_name) and not self.selinux_enabled:
                continue
            filtered.append(xattr_name)
        return filtered

    def removexattr(self, path, name):
        path = self.normalize_path(path)
        self.require_writable()
        xattr_name = self.normalize_xattr_name(name)
        entry_kind, _ = self.get_entry_kind_and_id(path)
        if entry_kind is None:
            raise FuseOSError(errno.ENOENT)
        if self.is_posix_acl_xattr(xattr_name) and not self.acl_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)
        if self.is_selinux_xattr(xattr_name) and not self.selinux_enabled:
            raise FuseOSError(errno.EOPNOTSUPP)

        deleted = self.xattr_store.remove_xattr(path, xattr_name)
        if deleted == 0:
            raise FuseOSError(errno.ENODATA)
        self.invalidate_metadata_cache(path, include_statfs=False)
        return 0

    def create(self, path, mode, fi=None):
        file_id = self.repository.create(path, mode, fi)
        flags = int(getattr(fi, "flags", 0)) if fi is not None and hasattr(fi, "flags") else 0
        return self.allocate_file_handle(file_id, flags=flags, initial_offset=0)

    def mknod(self, path, mode, dev=0):
        return self.repository.mknod(path, mode, dev)

    def unlink(self, path):
        return self.repository.unlink(path)

    def truncate(self, path, length, fh=None):
        path = self.normalize_path(path)
        self.require_writable()

        id_file = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if id_file is None:
            raise FuseOSError(errno.ENOENT)

        old_length = self.storage.get_logical_file_size(id_file)
        if length == old_length:
            return 0

        self.storage.truncate_to_size(id_file, length)
        self.storage.maybe_flush_dirty_write_buffer(id_file)

        with self.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE files
                SET size = %s, modification_date = NOW(), {file_ctime} = NOW()
                WHERE id_file = %s
                """.format(file_ctime=self.ctime_column("files")),
                (length, id_file),
            )
            self.append_journal_event(cur, "truncate", path, file_id=id_file)
            conn.commit()

        self.clear_read_cache(id_file)
        self.invalidate_metadata_cache(path, include_statfs=True)

    def fallocate(self, path, mode, offset, length, fh=None):
        path = self.normalize_path(path)
        self.require_writable()

        if offset < 0 or length < 0:
            raise FuseOSError(errno.EINVAL)
        if mode not in (0,):
            raise FuseOSError(errno.EOPNOTSUPP)

        id_file = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if id_file is None:
            raise FuseOSError(errno.ENOENT)

        old_size = self.storage.get_logical_file_size(id_file)
        end_offset = offset + length
        new_size = max(old_size, end_offset)

        # W modelu overlay nie ladujemy calego pliku do RAM.
        # Jesli trzeba rozszerzyc plik, podnosimy tylko logiczny rozmiar.
        if new_size == old_size:
            return 0

        self.storage.truncate_to_size(id_file, new_size)

        with self.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE files
                SET size = %s, modification_date = NOW(), {file_ctime} = NOW()
                WHERE id_file = %s
                """.format(file_ctime=self.ctime_column("files")),
                (new_size, id_file),
            )
            self.append_journal_event(cur, "fallocate", path, file_id=id_file)
            conn.commit()

        self.clear_read_cache(id_file)
        self.invalidate_metadata_cache(path, include_statfs=True)
        return 0
        
    def copy_file_range(self, path_in, fh_in, off_in, path_out, fh_out, off_out, length, flags):
        path_in = self.normalize_path(path_in)
        path_out = self.normalize_path(path_out)
        self.require_writable()

        if any(value < 0 for value in (off_in, off_out, length)):
            raise FuseOSError(errno.EINVAL)
        if flags not in (0, None):
            raise FuseOSError(errno.EOPNOTSUPP)

        src_file_id = self.file_id_for_handle(fh_in) if fh_in is not None else self.get_file_id(path_in)
        dst_file_id = self.file_id_for_handle(fh_out) if fh_out is not None else self.get_file_id(path_out)
        if src_file_id is None or dst_file_id is None:
            raise FuseOSError(errno.ENOENT)
        if src_file_id == dst_file_id and off_in == off_out:
            return 0

        copied = self.storage.copy_file_range_into_state(src_file_id, dst_file_id, off_in, off_out, length)
        if copied <= 0:
            return 0

        self.storage.maybe_flush_dirty_write_buffer(dst_file_id)

        new_size = self.storage.get_logical_file_size(dst_file_id)
        with self.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE files
                SET size = %s, modification_date = NOW(), {file_ctime} = NOW()
                WHERE id_file = %s
                """.format(file_ctime=self.ctime_column("files")),
                (new_size, dst_file_id),
            )
            self.append_journal_event(cur, "copy_file_range", path_out, file_id=dst_file_id)
            conn.commit()

        self.clear_read_cache(dst_file_id)
        self.invalidate_metadata_cache(path_out, include_statfs=True)
        return copied

    def ioctl(self, path, cmd, arg, fip, flags, data):
        path = self.normalize_path(path)
        kind, _ = self.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.ENOTTY)

        if cmd != termios.FIONREAD:
            raise FuseOSError(errno.ENOTTY)

        fh = getattr(fip, "fh", None)
        file_id = self.file_id_for_handle(fh) if fh is not None else self.get_file_id(path)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        size = self.storage.get_logical_file_size(file_id)

        if data:
            ctypes.cast(data, ctypes.POINTER(ctypes.c_int))[0] = int(size)
        return 0

    def _read_uint64_value(self, ref, default=0):
        if ref is None:
            return default
        if isinstance(ref, int):
            return int(ref)
        if hasattr(ref, "value"):
            try:
                return int(ref.value)
            except Exception:
                return default
        if hasattr(ref, "contents") and hasattr(ref.contents, "value"):
            try:
                return int(ref.contents.value)
            except Exception:
                return default
        if isinstance(ref, (list, tuple)) and ref:
            try:
                return int(ref[0])
            except Exception:
                return default
        return default

    def _write_uint64_value(self, ref, value):
        if ref is None:
            return
        try:
            if hasattr(ref, "value"):
                ref.value = int(value)
                return
            if hasattr(ref, "contents") and hasattr(ref.contents, "value"):
                ref.contents.value = int(value)
                return
            if isinstance(ref, list) and ref:
                ref[0] = int(value)
                return
            if isinstance(ref, tuple) and ref:
                return
            if hasattr(ref, "__setitem__"):
                ref[0] = int(value)
        except Exception:
            pass

    def bmap(self, path, blocksize, idx):
        path = self.normalize_path(path)
        kind, _ = self.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.EOPNOTSUPP)
        blocksize = int(blocksize)
        if blocksize <= 0:
            raise FuseOSError(errno.EINVAL)

        file_id = self.get_file_id(path)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)

        size = self.storage.get_logical_file_size(file_id)
        logical_blocks = max(1 if size > 0 else 0, (size + blocksize - 1) // blocksize)
        block_index = self._read_uint64_value(idx, 0)
        if block_index < 0 or block_index >= logical_blocks:
            raise FuseOSError(errno.EINVAL)

        self._write_uint64_value(idx, block_index)
        return 0

    def open(self, path, flags):
        path = self.normalize_path(path)
        attrs = self.getattr(path)
        file_type = stat.S_IFMT(attrs.get("st_mode", 0))
        if file_type != stat.S_IFREG:
            raise FuseOSError(errno.EOPNOTSUPP)
        access_mode = 0
        open_access = flags & os.O_ACCMODE
        if open_access == os.O_WRONLY:
            access_mode = os.W_OK
        elif open_access == os.O_RDWR:
            access_mode = os.R_OK | os.W_OK
        else:
            access_mode = os.R_OK
        if not self.acl_allows(path, attrs, access_mode):
            raise FuseOSError(errno.EACCES)
        id_file = self.get_file_id(path)
        if id_file is None:
            raise FuseOSError(errno.ENOENT)
        return self.allocate_file_handle(id_file, flags=int(flags), initial_offset=0)

    def flush(self, path, fh):
        path = self.normalize_path(path)
        self.require_writable()
        file_id = self.file_id_for_handle(fh)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)
        if self.is_write_buffer_dirty(file_id):
            started = time.perf_counter()
            self.persist_buffer(file_id)
            self.record_io_profile("flush", time.perf_counter() - started)
        return 0

    def release(self, path, fh):
        path = self.normalize_path(path)
        resource_key = self._lock_resource_key(path)
        owner_key = self._lock_owner_key(fh)
        self.locking.release(resource_key, owner_key)

        file_id = self.file_id_for_handle(fh)

        if self.read_only:
            self.close_file_handle(fh)
            self.clear_timestamp_touch_state(resource_key)
            self.clear_read_sequence_state(fh)
            return 0

        started = time.perf_counter() if (file_id is not None and self.is_write_buffer_dirty(file_id)) else None
        if started is not None:
            self.persist_buffer(file_id)
        self.close_file_handle(fh)
        self.clear_timestamp_touch_state(resource_key)
        self.clear_read_sequence_state(fh)

        if started is not None:
            self.record_io_profile("release", time.perf_counter() - started)

        return 0

    def opendir(self, path):
        path = self.normalize_path(path)
        logging.debug("opendir path=%s", path)
        return 0

    def releasedir(self, path, fh):
        path = self.normalize_path(path)
        logging.debug("releasedir path=%s fh=%s", path, fh)
        resource_key = ("dir", fh if fh is not None else path)
        self.clear_timestamp_touch_state(resource_key)
        return 0

    def fsyncdir(self, path, datasync, fh):
        path = self.normalize_path(path)
        logging.debug("fsyncdir path=%s datasync=%s fh=%s", path, datasync, fh)
        return 0

    def fsync(self, path, fdatasync, fh):
        path = self.normalize_path(path)
        self.require_writable()
        file_id = self.file_id_for_handle(fh)
        if file_id is None:
            raise FuseOSError(errno.ENOENT)
        if self.is_write_buffer_dirty(file_id):
            started = time.perf_counter()
            self.persist_buffer(file_id)
            self.record_io_profile("fsync", time.perf_counter() - started)
        return 0

    def destroy(self, path):
        logging.info("Destroying DBFS instance on unmount")
        self.cleanup_resources()
        return 0

    def lock(self, path, fh, cmd, lock):
        return self.locking.lock(path, fh, cmd, lock)

    def flock(self, path, fh, op):
        return self.locking.flock(path, fh, op)

    def rename(self, old, new):
        return self.repository.rename(old, new)

    def rmdir(self, path):
        return self.repository.rmdir(path)

    def chmod(self, path, mode):
        path = self.normalize_path(path)
        self.require_writable()
        kind, entry_id = self.get_entry_kind_and_id(path)
        file_id = self.get_file_id(path)
        parent_id = self.get_dir_id(os.path.dirname(path))

        with self.db_connection() as conn, conn.cursor() as cur:
            if kind == "symlink":
                raise FuseOSError(errno.EPERM)
            attrs = self.getattr(path)
            if not self.can_modify_metadata(attrs):
                raise FuseOSError(errno.EPERM)
            current_mode = attrs.get("st_mode", 0) & 0o7777
            new_mode = int(mode) & 0o7777
            if current_mode == new_mode:
                return 0
            if kind in {"file", "hardlink"} and file_id is not None:
                cur.execute(f"UPDATE files SET mode = %s, {self.ctime_column('files')} = NOW() WHERE id_file = %s", (oct(new_mode)[2:], file_id))
            elif kind == "dir":
                cur.execute(f"UPDATE directories SET mode = %s, {self.ctime_column('directories')} = NOW() WHERE id_directory = %s", (oct(new_mode)[2:], entry_id))
            self.append_journal_event(cur, "chmod", path, directory_id=parent_id)
            conn.commit()
        self.invalidate_metadata_cache(path, include_statfs=False)

        return 0

    def chown(self, path, uid, gid):
        path = self.normalize_path(path)
        self.require_writable()
        if uid == -1 and gid == -1:
            return 0
        kind, entry_id = self.get_entry_kind_and_id(path)
        file_id = self.get_file_id(path)
        parent_id = self.get_dir_id(os.path.dirname(path))

        with self.db_connection() as conn, conn.cursor() as cur:
            attrs = self.getattr(path)
            current_uid, _ = self.current_uid_gid()
            ownership_changed = (
                (uid != -1 and uid != attrs.get("st_uid"))
                or (gid != -1 and gid != attrs.get("st_gid"))
            )
            if kind == "symlink":
                if current_uid != 0:
                    raise FuseOSError(errno.EPERM)
            elif not self.can_change_owner(attrs, uid, gid):
                raise FuseOSError(errno.EPERM)

            new_uid = attrs.get("st_uid") if uid == -1 else uid
            new_gid = attrs.get("st_gid") if gid == -1 else gid
            if not ownership_changed:
                return 0
            if kind in {"file", "hardlink"} and file_id is not None:
                current_mode = self.getattr(path).get("st_mode", 0)
                if current_uid != 0 and ownership_changed:
                    current_mode &= ~stat.S_ISUID
                    current_mode &= ~stat.S_ISGID
                cur.execute(
                    f"UPDATE files SET uid = %s, gid = %s, mode = %s, {self.ctime_column('files')} = NOW() WHERE id_file = %s",
                    (new_uid, new_gid, oct(current_mode)[2:], file_id),
                )
            elif kind == "dir":
                current_mode = self.getattr(path).get("st_mode", 0)
                if current_uid != 0 and ownership_changed:
                    current_mode &= ~stat.S_ISUID
                cur.execute(
                    f"UPDATE directories SET uid = %s, gid = %s, mode = %s, {self.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                    (new_uid, new_gid, oct(current_mode)[2:], entry_id),
                )
            elif kind == "symlink":
                cur.execute(f"UPDATE symlinks SET uid = %s, gid = %s, {self.ctime_column('symlinks')} = NOW() WHERE id_symlink = %s", (new_uid, new_gid, entry_id))
            self.append_journal_event(cur, "chown", path, directory_id=parent_id)
            conn.commit()
        self.invalidate_metadata_cache(path, include_statfs=False)

        return 0

    def utimens(self, path, times=None):
        path = self.normalize_path(path)
        self.require_writable()
        now = time.time()
        atime, mtime = times if times else (now, now)

        kind, entry_id = self.get_entry_kind_and_id(path)
        file_id = self.get_file_id(path)
        parent_id = self.get_dir_id(os.path.dirname(path))
        attrs = self.getattr(path)
        current_atime = attrs.get("st_atime")
        current_mtime = attrs.get("st_mtime")
        if current_atime == atime and current_mtime == mtime:
            return 0

        with self.db_connection() as conn, conn.cursor() as cur:
            if kind in {"file", "hardlink"} and file_id is not None:
                cur.execute("""
                    UPDATE files SET access_date = %s, modification_date = %s, {file_ctime} = NOW()
                    WHERE id_file = %s
                """.format(file_ctime=self.ctime_column("files")), (epoch_to_utc_datetime(atime).replace(tzinfo=None), epoch_to_utc_datetime(mtime).replace(tzinfo=None), file_id))
            elif kind == "dir":
                cur.execute("""
                    UPDATE directories SET access_date = %s, modification_date = %s, {dir_ctime} = NOW()
                    WHERE id_directory = %s
                """.format(dir_ctime=self.ctime_column("directories")), (epoch_to_utc_datetime(atime).replace(tzinfo=None), epoch_to_utc_datetime(mtime).replace(tzinfo=None), entry_id))
            self.append_journal_event(cur, "utimens", path, directory_id=parent_id)
            conn.commit()
        self.invalidate_metadata_cache(path, include_statfs=False)

    def statfs(self, path):
        path = self.normalize_path(path)
        cached_statfs = self._statfs_cache_get()
        if cached_statfs is not None:
            return cached_statfs
        with self.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = 'block_size'")
            block_size_result = cur.fetchone()
            block_size = block_size_result[0] if block_size_result else self.default_block_size

            cur.execute("SELECT COUNT(*) FROM files")
            file_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM directories")
            dir_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM symlinks")
            symlink_count = cur.fetchone()[0]

            cur.execute("SELECT COALESCE(SUM(LENGTH(data)), 0) FROM data_blocks")
            total_data_size = cur.fetchone()[0]

            cur.execute("SELECT value FROM config WHERE key = 'max_fs_size_bytes'")
            total_size_result = cur.fetchone()
            max_fs_size_bytes = total_size_result[0] if total_size_result else self.default_max_fs_size_bytes

            max_blocks = max_fs_size_bytes // block_size
            used_blocks = (total_data_size + block_size - 1) // block_size
            free_blocks = max(0, max_blocks - used_blocks)

            inode_total = max(1000000, file_count + dir_count + symlink_count + 1024)
            inode_free = max(0, inode_total - (file_count + dir_count + symlink_count))
            result = {
                "f_bsize": block_size,
                "f_frsize": block_size,
                "f_blocks": max_blocks,
                "f_bfree": free_blocks,
                "f_bavail": free_blocks,
                "f_files": inode_total,
                "f_ffree": inode_free,
                "f_favail": inode_free,
                "f_flag": 0,
                "f_namemax": 255,
            }
            self._statfs_cache_set(result)
            return result

if __name__ == '__main__':
    from dbfs_bootstrap import main

    main()
