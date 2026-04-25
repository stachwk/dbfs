#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import ctypes
import errno
import os
import stat


class NamespaceRepositoryLookup:
    def _rust_repo(self):
        dbfs = self.dbfs
        repo = dbfs.backend._load_rust_pg_repo()
        lib = dbfs.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise dbfs.FuseOSError(errno.EIO)
        return repo, lib

    @staticmethod
    def _table_parent_clause(table_name):
        if table_name == "directories":
            return "id_parent"
        if table_name == "symlinks":
            return "id_parent"
        return "id_directory"

    def _refresh_lookup_cache(self):
        current_epoch = self.dbfs.namespace_epoch()
        if self._lookup_epoch != current_epoch:
            self._lookup_epoch = current_epoch
            self._dir_id_cache.clear()
            self._file_id_cache.clear()
            self._file_mode_cache.clear()
            self._hardlink_id_cache.clear()
            self._symlink_id_cache.clear()
            self._entry_cache.clear()
            self._symlink_attrs_cache.clear()

    def get_dir_id(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        if path == "/":
            return None
        self._refresh_lookup_cache()
        if path in self._dir_id_cache:
            return self._dir_id_cache[path]

        repo, lib = self._rust_repo()
        path_bytes = str(path).encode("utf-8")
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
            raise dbfs.FuseOSError(errno.EIO)
        rust_value = int(out_value.value)
        self._dir_id_cache[path] = rust_value
        return rust_value

    def _entry_lookup(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._entry_cache:
            return self._entry_cache[path]

        repo, lib = self._rust_repo()
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
        if status != 0:
            raise dbfs.FuseOSError(errno.EIO)
        value = (
            int(out_parent_id.value) if out_parent_found.value else None,
            {0: None, 1: "dir", 2: "file", 3: "hardlink", 4: "symlink"}.get(int(out_kind.value), None),
            int(out_entry_id.value) if out_entry_found.value else None,
        )
        self._entry_cache[path] = value
        return value

    def resolve_path(self, path):
        return self._entry_lookup(path)

    def get_file_id(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._file_id_cache:
            return self._file_id_cache[path]

        repo, lib = self._rust_repo()
        path_bytes = str(path).encode("utf-8")
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
            raise dbfs.FuseOSError(errno.EIO)
        rust_value = int(out_value.value)
        self._file_id_cache[path] = rust_value
        return rust_value

    def get_entry_kind_and_id(self, path):
        _, kind, entry_id = self.resolve_path(path)
        return kind, entry_id
