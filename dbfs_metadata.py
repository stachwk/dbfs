from __future__ import annotations

import ctypes
import errno
import os
import time


class MetadataSupport:
    def __init__(self, owner):
        self.owner = owner

    def _rust_repo(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise self.owner.FuseOSError(errno.EIO)
        return repo, lib

    def _clear_cache_bucket(self, guard, bucket, path=None):
        normalized_path = self.owner.normalize_path(path) if path is not None else None
        with guard:
            if normalized_path is None:
                bucket.clear()
            else:
                bucket.pop(normalized_path, None)

    def clear_attr_cache(self, path=None):
        self._clear_cache_bucket(self.owner._attr_cache_guard, self.owner._attr_cache, path)

    def clear_dir_cache(self, path=None):
        self._clear_cache_bucket(self.owner._dir_entries_cache_guard, self.owner._dir_entries_cache, path)

    def clear_metadata_cache(self, path=None):
        self.clear_attr_cache(path)
        self.clear_dir_cache(path)

    def clear_statfs_cache(self):
        with self.owner._statfs_cache_guard:
            self.owner._statfs_cache = None

    def invalidate_metadata_cache(self, path=None, include_statfs=False):
        self.clear_metadata_cache(path)
        if include_statfs:
            self.clear_statfs_cache()

    def _cache_get(self, guard, bucket, path):
        normalized_path = self.owner.normalize_path(path)
        with guard:
            cached = bucket.get(normalized_path)
            if not cached:
                return None
            expires_at, value = cached
            if expires_at <= time.time():
                bucket.pop(normalized_path, None)
                return None
            return value

    def _cache_set(self, guard, bucket, path, value):
        if self.owner.metadata_cache_ttl_seconds <= 0:
            return
        normalized_path = self.owner.normalize_path(path)
        with guard:
            cached_value = value.copy() if isinstance(value, dict) else value
            bucket[normalized_path] = (
                time.time() + self.owner.metadata_cache_ttl_seconds,
                cached_value,
            )

    def _attr_cache_get(self, path):
        return self._cache_get(self.owner._attr_cache_guard, self.owner._attr_cache, path)

    def _attr_cache_set(self, path, value):
        self._cache_set(self.owner._attr_cache_guard, self.owner._attr_cache, path, value)

    def _dir_cache_get(self, path):
        return self._cache_get(self.owner._dir_entries_cache_guard, self.owner._dir_entries_cache, path)

    def _dir_cache_set(self, path, value):
        self._cache_set(self.owner._dir_entries_cache_guard, self.owner._dir_entries_cache, path, value)

    def _statfs_cache_get(self):
        with self.owner._statfs_cache_guard:
            cached = self.owner._statfs_cache
            if not cached:
                return None
            expires_at, value = cached
            if expires_at <= time.time():
                self.owner._statfs_cache = None
                return None
            return value

    def _statfs_cache_set(self, value):
        if self.owner.statfs_cache_ttl_seconds <= 0:
            return
        with self.owner._statfs_cache_guard:
            cached_value = value.copy() if isinstance(value, dict) else value
            self.owner._statfs_cache = (time.time() + self.owner.statfs_cache_ttl_seconds, cached_value)

    def load_symlink_target(self, symlink_id):
        repo, lib = self._rust_repo()
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_load_symlink_target(
            repo,
            int(symlink_id),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
            ctypes.byref(out_found),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)
        if not out_found.value:
            return None
        try:
            return ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def get_symlink_attrs(self, path):
        path = self.owner.normalize_path(path)
        repo, lib = self._rust_repo()
        path_bytes = str(path).encode("utf-8")
        out_symlink_id = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_symlink_id(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_symlink_id),
            ctypes.byref(out_found),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)
        if not out_found.value:
            return None
        target = self.load_symlink_target(int(out_symlink_id.value))
        if target is None:
            return None
        return int(out_symlink_id.value), target

    def get_special_file_metadata(self, file_id):
        repo, lib = self._rust_repo()
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        out_rdev_major = ctypes.c_uint32()
        out_rdev_minor = ctypes.c_uint32()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_get_special_file_metadata(
            repo,
            int(file_id),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
            ctypes.byref(out_rdev_major),
            ctypes.byref(out_rdev_minor),
            ctypes.byref(out_found),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)
        if not out_found.value:
            return None
        try:
            file_type = ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)
        rdev = os.makedev(int(out_rdev_major.value), int(out_rdev_minor.value)) if hasattr(os, "makedev") else 0
        return file_type, rdev

    def count_directory_children(self, directory_id):
        return self.owner.storage.count_directory_children(directory_id)

    def count_directory_subdirs(self, directory_id):
        return self.owner.storage.count_directory_subdirs(directory_id)

    def count_root_directory_children(self):
        return self.owner.storage.count_root_directory_children()

    def count_file_blocks(self, file_id):
        return self.owner.storage.count_file_blocks(file_id)

    def count_symlinks(self):
        return self.owner.storage.count_symlinks()
