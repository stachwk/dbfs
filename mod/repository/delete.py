#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import ctypes
import errno
import os

import psycopg2


class NamespaceRepositoryDeleteMutations:
    def _rust_repo(self):
        dbfs = self.dbfs
        repo = dbfs.backend._load_rust_pg_repo()
        lib = dbfs.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise dbfs.FuseOSError(errno.EIO)
        return repo, lib

    def _purge_primary_file(self, cur, file_id):
        dbfs = self.dbfs
        repo, lib = self._rust_repo()
        status = lib.dbfs_rust_pg_repo_purge_primary_file(repo, int(file_id))
        if status != 0:
            raise dbfs.FuseOSError(errno.EIO)
        dbfs.clear_write_buffer_dirty(file_id)
        dbfs._clear_path_lock_state(("file", file_id))
        dbfs.clear_read_cache(file_id)

    def _finish_namespace_mutation(self, include_statfs=True):
        dbfs = self.dbfs
        dbfs.touch_namespace_epoch()
        dbfs.invalidate_metadata_cache(include_statfs=include_statfs)

    def unlink(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        dbfs.require_writable()
        parent_id, kind, entry_id = self.resolve_path(path)
        if kind == "hardlink":
            directory_id = parent_id
            file_id = dbfs.repository.get_file_id(path)
            if file_id is None:
                raise dbfs.FuseOSError(errno.ENOENT)
            dbfs.enforce_sticky_bit(os.path.dirname(path), dbfs.getattr(path))
            repo, lib = self._rust_repo()
            status = lib.dbfs_rust_pg_repo_delete_hardlink_entry(repo, int(entry_id))
            if status != 0:
                raise dbfs.FuseOSError(errno.EIO)
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.append_journal_event(cur, "unlink", path, file_id=file_id, directory_id=directory_id)
                conn.commit()
            self._finish_namespace_mutation()
            return

        parent_path = os.path.dirname(path)
        if kind == "dir":
            raise dbfs.FuseOSError(errno.EISDIR)
        if kind == "file":
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
                repo, lib = self._rust_repo()
                out_promoted = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_promote_hardlink_to_primary(
                    repo,
                    int(entry_id),
                    ctypes.byref(out_promoted),
                )
                if status != 0:
                    raise dbfs.FuseOSError(errno.EIO)
                promoted = bool(out_promoted.value)
                if not promoted:
                    self._purge_primary_file(cur, entry_id)
                dbfs.xattr_store.delete_inode_xattrs(path, cur=cur)
                if parent_id is not None:
                    status = lib.dbfs_rust_pg_repo_touch_directory_entry(repo, int(parent_id))
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                dbfs.append_journal_event(cur, "unlink", path, file_id=entry_id, directory_id=parent_id)
                conn.commit()
                self._finish_namespace_mutation()
                return

        if kind != "symlink":
            raise dbfs.FuseOSError(errno.ENOENT)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
            repo, lib = self._rust_repo()
            status = lib.dbfs_rust_pg_repo_delete_symlink_entry(repo, int(entry_id))
            if status != 0:
                raise dbfs.FuseOSError(errno.EIO)
            dbfs.xattr_store.delete_inode_xattrs(path, cur=cur)
            dbfs.append_journal_event(cur, "unlink", path, directory_id=parent_id)
            if parent_id is not None:
                status = lib.dbfs_rust_pg_repo_touch_directory_entry(repo, int(parent_id))
                if status != 0:
                    raise dbfs.FuseOSError(errno.EIO)
            conn.commit()
            self._finish_namespace_mutation()

    def rename(self, old, new):
        dbfs = self.dbfs
        old = dbfs.normalize_path(old)
        new = dbfs.normalize_path(new)
        dbfs.require_writable()
        if old == "/" or new == "/":
            raise dbfs.FuseOSError(errno.EPERM)
        old_parent_id, old_kind, old_id = self.resolve_path(old)
        if old_kind is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        if old_kind == "dir" and new.startswith(old.rstrip("/") + "/"):
            raise dbfs.FuseOSError(errno.EINVAL)

        new_parent_path = os.path.dirname(new)
        new_parent_id, existing_kind, existing_id = self.resolve_path(new)
        if new_parent_path != "/" and new_parent_id is None:
            raise dbfs.FuseOSError(errno.ENOENT)

        new_name = os.path.basename(new)
        if existing_id is not None and existing_id == old_id and existing_kind == old_kind:
            return 0
        if existing_id is not None and existing_id != old_id:
            if old_kind == "dir":
                if existing_kind != "dir":
                    raise dbfs.FuseOSError(errno.ENOTDIR)
                repo, lib = self._rust_repo()
                out_value = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_path_has_children(repo, int(existing_id), ctypes.byref(out_value))
                if status != 0:
                    raise dbfs.FuseOSError(errno.EIO)
                if bool(out_value.value):
                    raise dbfs.FuseOSError(errno.ENOTEMPTY)
            elif existing_kind == "dir":
                raise dbfs.FuseOSError(errno.EISDIR)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                if existing_id is not None and existing_id != old_id:
                    if existing_kind == "hardlink":
                        repo, lib = self._rust_repo()
                        status = lib.dbfs_rust_pg_repo_delete_hardlink_entry(repo, int(existing_id))
                        if status != 0:
                            raise dbfs.FuseOSError(errno.EIO)
                        dbfs.xattr_store.delete_inode_xattrs(new, cur=cur)
                    elif existing_kind == "file":
                        target_file_id = existing_id
                        repo, lib = self._rust_repo()
                        out_promoted = ctypes.c_ubyte()
                        status = lib.dbfs_rust_pg_repo_promote_hardlink_to_primary(
                            repo,
                            int(target_file_id),
                            ctypes.byref(out_promoted),
                        )
                        if status != 0:
                            raise dbfs.FuseOSError(errno.EIO)
                        if not bool(out_promoted.value):
                            self._purge_primary_file(cur, target_file_id)
                        dbfs.xattr_store.delete_inode_xattrs(new, cur=cur)
                    elif existing_kind == "symlink":
                        repo, lib = self._rust_repo()
                        status = lib.dbfs_rust_pg_repo_delete_symlink_entry(repo, int(existing_id))
                        if status != 0:
                            raise dbfs.FuseOSError(errno.EIO)
                        dbfs.xattr_store.delete_inode_xattrs(new, cur=cur)
                    elif existing_kind == "dir":
                        repo, lib = self._rust_repo()
                        status = lib.dbfs_rust_pg_repo_delete_directory_entry(repo, int(existing_id))
                        if status != 0:
                            raise dbfs.FuseOSError(errno.EIO)
                        dbfs.xattr_store.delete_inode_xattrs(new, cur=cur)

                if old_kind == "hardlink":
                    repo, lib = self._rust_repo()
                    new_name_bytes = new_name.encode("utf-8")
                    status = lib.dbfs_rust_pg_repo_rename_hardlink_entry(
                        repo,
                        int(old_id),
                        int(new_parent_id or 0),
                        ctypes.c_ubyte(1 if new_parent_id is not None else 0),
                        new_name_bytes,
                        len(new_name_bytes),
                    )
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                elif old_kind == "file":
                    repo, lib = self._rust_repo()
                    new_name_bytes = new_name.encode("utf-8")
                    status = lib.dbfs_rust_pg_repo_rename_file_entry(
                        repo,
                        int(old_id),
                        int(new_parent_id or 0),
                        ctypes.c_ubyte(1 if new_parent_id is not None else 0),
                        new_name_bytes,
                        len(new_name_bytes),
                    )
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                elif old_kind == "symlink":
                    repo, lib = self._rust_repo()
                    new_name_bytes = new_name.encode("utf-8")
                    status = lib.dbfs_rust_pg_repo_rename_symlink_entry(
                        repo,
                        int(old_id),
                        int(new_parent_id or 0),
                        ctypes.c_ubyte(1 if new_parent_id is not None else 0),
                        new_name_bytes,
                        len(new_name_bytes),
                    )
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                else:
                    repo, lib = self._rust_repo()
                    new_name_bytes = new_name.encode("utf-8")
                    status = lib.dbfs_rust_pg_repo_rename_directory_entry(
                        repo,
                        int(old_id),
                        int(new_parent_id or 0),
                        ctypes.c_ubyte(1 if new_parent_id is not None else 0),
                        new_name_bytes,
                        len(new_name_bytes),
                    )
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)

                if cur.rowcount != 1:
                    raise dbfs.FuseOSError(errno.ENOENT)

                dbfs.xattr_store.move_path_xattrs(old, new, recursive=(old_kind == "dir"), cur=cur)
                if old_kind == "hardlink":
                    repo, lib = self._rust_repo()
                    out_value = ctypes.c_uint64()
                    out_found = ctypes.c_ubyte()
                    status = lib.dbfs_rust_pg_repo_get_hardlink_file_id(
                        repo,
                        int(old_id),
                        ctypes.byref(out_value),
                        ctypes.byref(out_found),
                    )
                    if status != 0 or not out_found.value:
                        raise dbfs.FuseOSError(errno.EIO)
                    journal_file_id = int(out_value.value)
                elif old_kind == "file":
                    journal_file_id = old_id
                else:
                    journal_file_id = None
                journal_dir_id = old_id if old_kind == "dir" else new_parent_id
                dbfs.append_journal_event(cur, "rename", f"{old}->{new}", file_id=journal_file_id, directory_id=journal_dir_id)
                if old_parent_id is not None:
                    status = lib.dbfs_rust_pg_repo_touch_directory_entry(repo, int(old_parent_id))
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                if new_parent_id is not None:
                    status = lib.dbfs_rust_pg_repo_touch_directory_entry(repo, int(new_parent_id))
                    if status != 0:
                        raise dbfs.FuseOSError(errno.EIO)
                conn.commit()
                self._finish_namespace_mutation()
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

    def rmdir(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        dbfs.require_writable()
        parent_id, kind, dir_id = self.resolve_path(path)
        if kind is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        if kind != "dir":
            raise dbfs.FuseOSError(errno.ENOTDIR)

        if dbfs.storage.path_has_children(dir_id):
            raise dbfs.FuseOSError(errno.ENOTEMPTY)

        parent_path = os.path.dirname(path)
        dir_name = os.path.basename(path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
            dbfs.append_journal_event(cur, "rmdir", path, directory_id=dir_id)
            repo, lib = self._rust_repo()
            status = lib.dbfs_rust_pg_repo_delete_directory_entry(repo, int(dir_id))
            if status != 0:
                raise dbfs.FuseOSError(errno.EIO)
            dbfs.xattr_store.delete_inode_xattrs(path, cur=cur)
            if parent_id is not None:
                status = lib.dbfs_rust_pg_repo_touch_directory_entry(repo, int(parent_id))
                if status != 0:
                    raise dbfs.FuseOSError(errno.EIO)
            conn.commit()
            self._finish_namespace_mutation()
