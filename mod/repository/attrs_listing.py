#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import ctypes
import errno
import os
import stat

from dbfs_time import db_timestamp_to_epoch


class NamespaceRepositoryAttrsListing:
    def _rust_repo(self):
        dbfs = self.dbfs
        repo = dbfs.backend._load_rust_pg_repo()
        lib = dbfs.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise dbfs.FuseOSError(errno.EIO)
        return repo, lib

    def _decode_nul_fields(self, blob):
        if not blob:
            return []
        if isinstance(blob, (bytes, bytearray)):
            raw = bytes(blob)
        else:
            raw = bytes(blob)
        return [part.decode("utf-8") for part in raw.split(b"\0")]

    def _fetch_blob(self, func_name, *args):
        repo, lib = self._rust_repo()
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        func = getattr(lib, func_name)
        status = func(repo, *args, ctypes.byref(out_ptr), ctypes.byref(out_len))
        if status != 0:
            raise self.dbfs.FuseOSError(errno.EIO)
        if not out_ptr or out_len.value == 0:
            return b""
        try:
            return ctypes.string_at(out_ptr, out_len.value)
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def get_symlink_attrs(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._symlink_attrs_cache:
            return self._symlink_attrs_cache[path]
        repo, lib = self._rust_repo()
        path_bytes = path.encode("utf-8")
        out_value = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        status = lib.dbfs_rust_pg_repo_load_symlink_target(
            repo,
            path_bytes,
            len(path_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_len),
        )
        if status != 0 or not out_value:
            raise dbfs.FuseOSError(errno.EIO)
        try:
            target = ctypes.string_at(out_value, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_value, out_len)
        self._symlink_attrs_cache[path] = target
        return target

    def readlink(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        target = self.get_symlink_attrs(path)
        if target is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        return target

    def list_directory_entries(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        directory_id = self.get_dir_id(path)
        blob = self._fetch_blob(
            "dbfs_rust_pg_repo_list_directory_entries",
            path.encode("utf-8"),
            len(path.encode("utf-8")),
        )
        entries = self._decode_nul_fields(blob)
        return {
            "directory_id": directory_id,
            "entries": entries,
        }

    def fetch_path_attrs(self, path, now=None):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        now = now if now is not None else 0.0
        blob = self._fetch_blob(
            "dbfs_rust_pg_repo_fetch_path_attrs",
            path.encode("utf-8"),
            len(path.encode("utf-8")),
        )
        if not blob:
            raise dbfs.FuseOSError(errno.ENOENT)

        fields = self._decode_nul_fields(blob)
        if len(fields) < 3:
            raise dbfs.FuseOSError(errno.EIO)

        obj_type = fields[0]
        raw_inode = int(fields[1])
        inode = raw_inode
        file_link_id = None

        if obj_type == "symlink":
            target = fields[2]
            mod_date = fields[3]
            acc_date = fields[4]
            chg_date = fields[5]
            uid = int(fields[6]) if fields[6] else dbfs.current_uid_gid()[0]
            gid = int(fields[7]) if fields[7] else dbfs.current_uid_gid()[1]
            inode_seed = fields[8]
            size = len(target.encode("utf-8"))
            mode_bits = 0o777
            file_type = stat.S_IFLNK
            rdev = 0
            inode = dbfs.stable_inode(obj_type, inode_seed, raw_inode)
            st_nlink = 1
        else:
            size = int(fields[2]) if fields[2] else 0
            mode = fields[3]
            mod_date = fields[4]
            acc_date = fields[5]
            chg_date = fields[6]
            uid = int(fields[7]) if fields[7] else dbfs.current_uid_gid()[0]
            gid = int(fields[8]) if fields[8] else dbfs.current_uid_gid()[1]
            inode_seed = fields[9]
            file_type, mode_bits, rdev = stat.S_IFREG, dbfs.file_mode_bits(mode), 0
            if obj_type == "hardlink":
                repo, lib = self._rust_repo()
                out_value = ctypes.c_uint64()
                out_found = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_get_hardlink_file_id(
                    repo,
                    int(raw_inode),
                    ctypes.byref(out_value),
                    ctypes.byref(out_found),
                )
                if status != 0 or not out_found.value:
                    raise dbfs.FuseOSError(errno.EIO)
                file_link_id = int(out_value.value)
                inode = dbfs.stable_inode("file", inode_seed, file_link_id)
            else:
                inode = dbfs.stable_inode(obj_type, inode_seed, raw_inode)
            special_file_id = file_link_id if obj_type == "hardlink" and file_link_id is not None else raw_inode
            special_metadata = dbfs.get_special_file_metadata(special_file_id)
            if special_metadata is not None:
                special_type, rdev = special_metadata
                if special_type == "fifo":
                    file_type = stat.S_IFIFO
                elif special_type == "char":
                    file_type = stat.S_IFCHR
                elif special_type == "block":
                    file_type = stat.S_IFBLK
            if mode_bits == 0o644 and obj_type == "dir":
                mode_bits = 0o755
            if obj_type == "hardlink":
                repo, lib = self._rust_repo()
                out_value = ctypes.c_uint64()
                out_found = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_count_file_links(
                    repo,
                    int(file_link_id if file_link_id is not None else raw_inode),
                    ctypes.byref(out_value),
                    ctypes.byref(out_found),
                )
                if status != 0 or not out_found.value:
                    raise dbfs.FuseOSError(errno.EIO)
                st_nlink = int(out_value.value)
            elif obj_type == "file":
                repo, lib = self._rust_repo()
                out_value = ctypes.c_uint64()
                out_found = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_count_file_links(
                    repo,
                    int(raw_inode),
                    ctypes.byref(out_value),
                    ctypes.byref(out_found),
                )
                if status != 0 or not out_found.value:
                    raise dbfs.FuseOSError(errno.EIO)
                st_nlink = int(out_value.value)
            else:
                st_nlink = 2 + dbfs.storage.count_directory_subdirs(raw_inode)

        st_mode = (
            file_type | mode_bits
            if obj_type in {"file", "hardlink"}
            else stat.S_IFDIR | mode_bits
            if obj_type == "dir"
            else stat.S_IFLNK | mode_bits
        )

        return {
            'st_ino': inode,
            'st_mode': st_mode,
            'st_size': size,
            'st_blocks': dbfs.estimate_blocks(obj_type, size, raw_inode),
            'st_blksize': dbfs.default_block_size,
            'st_dev': dbfs.device_id,
            'st_rdev': rdev if obj_type in {'file', 'hardlink'} else 0,
            'st_mtime': db_timestamp_to_epoch(mod_date),
            'st_atime': db_timestamp_to_epoch(acc_date),
            'st_ctime': db_timestamp_to_epoch(chg_date),
            'st_nlink': st_nlink,
            'st_uid': uid,
            'st_gid': gid,
        }
