#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import errno
import os

import psycopg2


class NamespaceRepositoryDeleteMutations:
    def _purge_primary_file(self, cur, file_id):
        dbfs = self.dbfs
        rust_purged = dbfs.backend.python_to_rust_pg_repo_purge_primary_file(file_id)
        if not rust_purged:
            cur.execute("DELETE FROM data_blocks WHERE id_file = %s", (file_id,))
            cur.execute("DELETE FROM copy_block_crc WHERE id_file = %s", (file_id,))
            cur.execute("DELETE FROM files WHERE id_file = %s", (file_id,))
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
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT id_directory, id_file FROM hardlinks WHERE id_hardlink = %s", (entry_id,))
                result = cur.fetchone()
                if not result:
                    raise dbfs.FuseOSError(errno.ENOENT)
                directory_id, file_id = result
                dbfs.enforce_sticky_bit(os.path.dirname(path), dbfs.getattr(path))
                dbfs.append_journal_event(cur, "unlink", path, file_id=file_id, directory_id=directory_id)
                cur.execute("DELETE FROM hardlinks WHERE id_hardlink = %s", (entry_id,))
                conn.commit()
                self._finish_namespace_mutation()
                return

        parent_path = os.path.dirname(path)
        if kind == "dir":
            raise dbfs.FuseOSError(errno.EISDIR)
        if kind == "file":
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
                dbfs.append_journal_event(cur, "unlink", path, file_id=entry_id, directory_id=parent_id)
                promoted = dbfs.promote_hardlink_to_primary(entry_id, cur)
                if not promoted:
                    self._purge_primary_file(cur, entry_id)
                dbfs.delete_path_xattrs(path, cur=cur)
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                conn.commit()
                self._finish_namespace_mutation()
                return

        if kind != "symlink":
            raise dbfs.FuseOSError(errno.ENOENT)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
            cur.execute("DELETE FROM symlinks WHERE id_symlink = %s", (entry_id,))
            dbfs.delete_path_xattrs(path, cur=cur)
            if parent_id is not None:
                cur.execute(
                    f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                    (parent_id,),
                )
            dbfs.append_journal_event(cur, "unlink", path, directory_id=parent_id)
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
                if dbfs.path_has_children(existing_id):
                    raise dbfs.FuseOSError(errno.ENOTEMPTY)
            elif existing_kind == "dir":
                raise dbfs.FuseOSError(errno.EISDIR)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                if existing_id is not None and existing_id != old_id:
                    if existing_kind == "hardlink":
                        cur.execute("DELETE FROM hardlinks WHERE id_hardlink = %s", (existing_id,))
                        dbfs.delete_path_xattrs(new, cur=cur)
                    elif existing_kind == "file":
                        target_file_id = existing_id
                        promoted = dbfs.promote_hardlink_to_primary(target_file_id, cur)
                        if not promoted:
                            self._purge_primary_file(cur, target_file_id)
                        dbfs.delete_path_xattrs(new, cur=cur)
                    elif existing_kind == "symlink":
                        cur.execute("DELETE FROM symlinks WHERE id_symlink = %s", (existing_id,))
                        dbfs.delete_path_xattrs(new, cur=cur)
                    elif existing_kind == "dir":
                        cur.execute("DELETE FROM directories WHERE id_directory = %s", (existing_id,))
                        dbfs.delete_path_xattrs(new, recursive=True, cur=cur)

                if old_kind == "hardlink":
                    if old_parent_id is None:
                        cur.execute(
                            f"UPDATE hardlinks SET name = %s, id_directory = %s, modification_date = NOW(), {dbfs.ctime_column('hardlinks')} = NOW() WHERE id_hardlink = %s AND id_directory IS NULL",
                            (new_name, new_parent_id, old_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE hardlinks SET name = %s, id_directory = %s, modification_date = NOW(), {dbfs.ctime_column('hardlinks')} = NOW() WHERE id_hardlink = %s",
                            (new_name, new_parent_id, old_id),
                        )
                elif old_kind == "file":
                    if old_parent_id is None:
                        cur.execute(
                            f"UPDATE files SET name = %s, id_directory = %s, {dbfs.ctime_column('files')} = NOW() WHERE id_file = %s AND id_directory IS NULL",
                            (new_name, new_parent_id, old_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE files SET name = %s, id_directory = %s, {dbfs.ctime_column('files')} = NOW() WHERE id_file = %s",
                            (new_name, new_parent_id, old_id),
                        )
                elif old_kind == "symlink":
                    if old_parent_id is None:
                        cur.execute(
                            f"UPDATE symlinks SET name = %s, id_parent = %s, {dbfs.ctime_column('symlinks')} = NOW() WHERE id_symlink = %s AND id_parent IS NULL",
                            (new_name, new_parent_id, old_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE symlinks SET name = %s, id_parent = %s, {dbfs.ctime_column('symlinks')} = NOW() WHERE id_symlink = %s",
                            (new_name, new_parent_id, old_id),
                        )
                else:
                    if old_parent_id is None:
                        cur.execute(
                            f"UPDATE directories SET name = %s, id_parent = %s, {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s AND id_parent IS NULL",
                            (new_name, new_parent_id, old_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE directories SET name = %s, id_parent = %s, {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                            (new_name, new_parent_id, old_id),
                        )

                if cur.rowcount != 1:
                    raise dbfs.FuseOSError(errno.ENOENT)

                dbfs.move_path_xattrs(old, new, recursive=(old_kind == "dir"), cur=cur)
                if old_kind == "hardlink":
                    journal_file_id = dbfs.get_hardlink_file_id(old_id)
                elif old_kind == "file":
                    journal_file_id = old_id
                else:
                    journal_file_id = None
                journal_dir_id = old_id if old_kind == "dir" else new_parent_id
                dbfs.append_journal_event(cur, "rename", f"{old}->{new}", file_id=journal_file_id, directory_id=journal_dir_id)
                if old_parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (old_parent_id,),
                    )
                if new_parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (new_parent_id,),
                    )
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

        if dbfs.path_has_children(dir_id):
            raise dbfs.FuseOSError(errno.ENOTEMPTY)

        parent_path = os.path.dirname(path)
        dir_name = os.path.basename(path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
            dbfs.append_journal_event(cur, "rmdir", path, directory_id=dir_id)
            if parent_id is None:
                cur.execute(
                    "DELETE FROM directories WHERE name = %s AND id_parent IS NULL",
                    (dir_name,),
                )
            else:
                cur.execute(
                    "DELETE FROM directories WHERE name = %s AND id_parent = %s",
                    (dir_name, parent_id),
                )
            dbfs.delete_path_xattrs(path, recursive=True, cur=cur)
            if parent_id is not None:
                cur.execute(
                    f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                    (parent_id,),
                )
            conn.commit()
            self._finish_namespace_mutation()
