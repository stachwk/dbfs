#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import ctypes
import errno
import os
import stat


def _rust_repo_and_lib(dbfs):
    repo = dbfs.backend._load_rust_pg_repo()
    lib = dbfs.backend._load_rust_hotpath_lib()
    if repo is None or lib is None:
        raise dbfs.FuseOSError(errno.EIO)
    return repo, lib


class NamespaceRepositoryCreateMutations:
    def mkdir(self, path, mode):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        dbfs.require_writable()
        parent_id, existing_kind, existing_id = self.resolve_path(path)
        if existing_id is not None:
            raise dbfs.FuseOSError(errno.EEXIST)
        parent_path = os.path.dirname(path)
        dir_name = os.path.basename(path)

        if parent_path != "/" and self.get_dir_id(parent_path) is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        uid, gid = dbfs.creation_uid_gid(parent_path)
        inherited_mode = dbfs.inherited_directory_mode(parent_path, mode)
        inode_seed = dbfs.generate_inode_seed()
        repo, lib = _rust_repo_and_lib(dbfs)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        dir_name_bytes = str(dir_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_create_directory(
            repo,
            int(parent_id or 0),
            ctypes.c_ubyte(1 if parent_id is not None else 0),
            dir_name_bytes,
            len(dir_name_bytes),
            ctypes.c_uint32(int(inherited_mode)),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            raise dbfs.FuseOSError(errno.EIO)
        rust_dir_id = int(out_value.value)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=True, cur=cur, owner_key=("dir", rust_dir_id))
            dbfs.append_journal_event(cur, "mkdir", path, directory_id=rust_dir_id)
            conn.commit()
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=True)

    def symlink(self, target, source):
        dbfs = self.dbfs
        target = dbfs.normalize_path(target)
        dbfs.require_writable()
        parent_path = os.path.dirname(target)
        link_name = os.path.basename(target)
        parent_id, existing_kind, existing_id = self.resolve_path(target)
        if existing_id is not None:
            raise dbfs.FuseOSError(errno.EEXIST)

        if parent_path != "/" and self.get_dir_id(parent_path) is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        uid, gid = dbfs.creation_uid_gid(parent_path)
        inode_seed = dbfs.generate_inode_seed()
        repo, lib = _rust_repo_and_lib(dbfs)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        link_name_bytes = str(link_name).encode("utf-8")
        source_bytes = str(source).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_create_symlink(
            repo,
            int(parent_id or 0),
            ctypes.c_ubyte(1 if parent_id is not None else 0),
            link_name_bytes,
            len(link_name_bytes),
            source_bytes,
            len(source_bytes),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            raise dbfs.FuseOSError(errno.EIO)
        rust_symlink_id = int(out_value.value)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.append_journal_event(cur, "symlink", target, directory_id=parent_id)
            conn.commit()
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=True)
        return 0

    def link(self, target, source):
        dbfs = self.dbfs
        target = dbfs.normalize_path(target)
        source = dbfs.normalize_path(source)
        dbfs.require_writable()
        dbfs.logging.debug("link request target=%s source=%s", target, source)

        source_file_id = dbfs.repository.get_file_id(source)
        if source_file_id is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        dbfs.persist_buffer(source_file_id)

        source_kind, _ = dbfs.repository.get_entry_kind_and_id(source)
        if source_kind == "dir":
            raise dbfs.FuseOSError(errno.EPERM)
        if stat.S_IFMT(dbfs.getattr(source).get("st_mode", stat.S_IFREG)) != stat.S_IFREG:
            raise dbfs.FuseOSError(errno.EOPNOTSUPP)

        target_parent_path = os.path.dirname(target)
        target_name = os.path.basename(target)
        target_parent_id, existing_kind, existing_id = self.resolve_path(target)
        if existing_id is not None:
            raise dbfs.FuseOSError(errno.EEXIST)
        if target_parent_path != "/" and self.get_dir_id(target_parent_path) is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        uid, gid = dbfs.current_uid_gid()
        repo, lib = _rust_repo_and_lib(dbfs)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        target_name_bytes = str(target_name).encode("utf-8")
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
            raise dbfs.FuseOSError(errno.EIO)
        rust_hardlink_id = int(out_value.value)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.append_journal_event(cur, "link", target, file_id=source_file_id, directory_id=target_parent_id)
            conn.commit()
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=True)
        dbfs.logging.debug("link created target=%s source=%s file_id=%s hardlink_id=%s", target, source, source_file_id, rust_hardlink_id)
        return 0

    def create(self, path, mode, fi=None):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        dbfs.require_writable()
        parent_id, existing_kind, existing_id = self.resolve_path(path)
        if existing_id is not None:
            raise dbfs.FuseOSError(errno.EEXIST)
        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)

        if parent_path != "/" and self.get_dir_id(parent_path) is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        uid, gid = dbfs.creation_uid_gid(parent_path)
        inode_seed = dbfs.generate_inode_seed()
        repo, lib = _rust_repo_and_lib(dbfs)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        file_name_bytes = str(file_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_create_file(
            repo,
            int(parent_id or 0),
            ctypes.c_ubyte(1 if parent_id is not None else 0),
            file_name_bytes,
            len(file_name_bytes),
            ctypes.c_uint32(int(mode)),
            ctypes.c_uint32(int(uid)),
            ctypes.c_uint32(int(gid)),
            inode_seed_bytes,
            len(inode_seed_bytes),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            raise dbfs.FuseOSError(errno.EIO)
        rust_file_id = int(out_value.value)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=False, cur=cur, owner_key=("file", rust_file_id))
            dbfs.append_journal_event(cur, "create", path, file_id=rust_file_id, directory_id=parent_id)
            conn.commit()
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=True)
        return rust_file_id

    def mknod(self, path, mode, dev=0):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        dbfs.require_writable()
        parent_id, existing_kind, existing_id = self.resolve_path(path)
        if existing_id is not None:
            raise dbfs.FuseOSError(errno.EEXIST)
        file_type = stat.S_IFMT(mode)
        if file_type not in {stat.S_IFIFO, stat.S_IFCHR, stat.S_IFBLK}:
            raise dbfs.FuseOSError(errno.EOPNOTSUPP)

        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        if parent_path != "/" and self.get_dir_id(parent_path) is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        uid, gid = dbfs.creation_uid_gid(parent_path)
        inode_seed = dbfs.generate_inode_seed()
        repo, lib = _rust_repo_and_lib(dbfs)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        file_name_bytes = str(file_name).encode("utf-8")
        inode_seed_bytes = str(inode_seed).encode("utf-8")
        if file_type in {stat.S_IFCHR, stat.S_IFBLK}:
            major = os.major(dev) if hasattr(os, "major") else 0
            minor = os.minor(dev) if hasattr(os, "minor") else 0
            file_kind_bytes = b"char" if file_type == stat.S_IFCHR else b"block"
            status = lib.dbfs_rust_pg_repo_create_special_file(
                repo,
                int(parent_id or 0),
                ctypes.c_ubyte(1 if parent_id is not None else 0),
                file_name_bytes,
                len(file_name_bytes),
                ctypes.c_uint32(int(mode)),
                ctypes.c_uint32(int(uid)),
                ctypes.c_uint32(int(gid)),
                inode_seed_bytes,
                len(inode_seed_bytes),
                file_kind_bytes,
                len(file_kind_bytes),
                ctypes.c_uint32(int(major)),
                ctypes.c_uint32(int(minor)),
                ctypes.byref(out_value),
                ctypes.byref(out_found),
            )
        elif file_type == stat.S_IFIFO:
            file_kind_bytes = b"fifo"
            status = lib.dbfs_rust_pg_repo_create_special_file(
                repo,
                int(parent_id or 0),
                ctypes.c_ubyte(1 if parent_id is not None else 0),
                file_name_bytes,
                len(file_name_bytes),
                ctypes.c_uint32(int(mode)),
                ctypes.c_uint32(int(uid)),
                ctypes.c_uint32(int(gid)),
                inode_seed_bytes,
                len(inode_seed_bytes),
                file_kind_bytes,
                len(file_kind_bytes),
                ctypes.c_uint32(0),
                ctypes.c_uint32(0),
                ctypes.byref(out_value),
                ctypes.byref(out_found),
            )
        else:
            status = lib.dbfs_rust_pg_repo_create_file(
                repo,
                int(parent_id or 0),
                ctypes.c_ubyte(1 if parent_id is not None else 0),
                file_name_bytes,
                len(file_name_bytes),
                ctypes.c_uint32(int(mode)),
                ctypes.c_uint32(int(uid)),
                ctypes.c_uint32(int(gid)),
                inode_seed_bytes,
                len(inode_seed_bytes),
                ctypes.byref(out_value),
                ctypes.byref(out_found),
            )
        if status != 0 or not out_found.value:
            raise dbfs.FuseOSError(errno.EIO)
        rust_file_id = int(out_value.value)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=False, cur=cur, owner_key=("file", rust_file_id))
            dbfs.append_journal_event(cur, "mknod", path, file_id=rust_file_id, directory_id=parent_id)
            conn.commit()
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=True)
        return 0
