#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import errno
import os
import stat

import psycopg2


class NamespaceRepositoryMutations:
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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                uid, gid = dbfs.creation_uid_gid(parent_path)
                inherited_mode = dbfs.inherited_directory_mode(parent_path, mode)
                dir_ctime = dbfs.ctime_column("directories")
                cur.execute(
                    f"INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, {dir_ctime}, creation_date, modification_date, access_date) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW()) RETURNING id_directory",
                    (parent_id, dir_name, oct(inherited_mode)[2:], uid, gid, dbfs.generate_inode_seed()),
                )
                created_dir_id = cur.fetchone()[0]
                dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=True, cur=cur, owner_key=("dir", created_dir_id))
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                dbfs.append_journal_event(cur, "mkdir", path, directory_id=created_dir_id)
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                uid, gid = dbfs.creation_uid_gid(parent_path)
                symlink_ctime = dbfs.ctime_column("symlinks")
                cur.execute(
                    """
                    INSERT INTO symlinks (id_parent, name, target, uid, gid, inode_seed, {symlink_ctime}, creation_date, modification_date, access_date)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())
                    RETURNING id_symlink
                    """.format(symlink_ctime=symlink_ctime),
                    (parent_id, link_name, source, uid, gid, dbfs.generate_inode_seed()),
                )
                cur.fetchone()[0]
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                dbfs.append_journal_event(cur, "symlink", target, directory_id=parent_id)
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                return 0
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

    def link(self, target, source):
        dbfs = self.dbfs
        target = dbfs.normalize_path(target)
        source = dbfs.normalize_path(source)
        dbfs.require_writable()
        dbfs.logging.debug("link request target=%s source=%s", target, source)

        source_file_id = dbfs.get_file_id(source)
        if source_file_id is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        dbfs.persist_buffer(source_file_id)

        source_kind, _ = dbfs.get_entry_kind_and_id(source)
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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                uid, gid = dbfs.current_uid_gid()
                hardlink_ctime = dbfs.ctime_column("hardlinks")
                cur.execute(
                    """
                    INSERT INTO hardlinks (id_file, id_directory, name, uid, gid, {hardlink_ctime}, creation_date, modification_date, access_date)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())
                    RETURNING id_hardlink
                    """.format(hardlink_ctime=hardlink_ctime),
                    (source_file_id, target_parent_id, target_name, uid, gid),
                )
                dbfs.append_journal_event(cur, "link", target, file_id=source_file_id, directory_id=target_parent_id)
                if target_parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (target_parent_id,),
                    )
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                dbfs.logging.debug("link created target=%s source=%s file_id=%s", target, source, source_file_id)
                return 0
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                uid, gid = dbfs.creation_uid_gid(parent_path)
                file_ctime = dbfs.ctime_column("files")
                cur.execute(
                    """
                    INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, modification_date, access_date, {file_ctime}, creation_date)
                    VALUES (%s, %s, 0, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())
                    RETURNING id_file
                """.format(file_ctime=file_ctime),
                    (parent_id, file_name, oct(mode)[2:], uid, gid, dbfs.generate_inode_seed()),
                )

                id_file = cur.fetchone()[0]
                dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=False, cur=cur, owner_key=("file", id_file))
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                dbfs.append_journal_event(cur, "create", path, file_id=id_file, directory_id=parent_id)
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                return id_file
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                uid, gid = dbfs.creation_uid_gid(parent_path)
                file_ctime = dbfs.ctime_column("files")
                cur.execute(
                    """
                    INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, modification_date, access_date, {file_ctime}, creation_date)
                    VALUES (%s, %s, 0, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())
                    RETURNING id_file
                """.format(file_ctime=file_ctime),
                    (parent_id, file_name, oct(mode)[2:], uid, gid, dbfs.generate_inode_seed()),
                )

                id_file = cur.fetchone()[0]
                if file_type in {stat.S_IFCHR, stat.S_IFBLK}:
                    major = os.major(dev) if hasattr(os, "major") else 0
                    minor = os.minor(dev) if hasattr(os, "minor") else 0
                    cur.execute(
                        """
                        INSERT INTO special_files (id_file, file_type, rdev_major, rdev_minor)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            id_file,
                            "char" if file_type == stat.S_IFCHR else "block",
                            major,
                            minor,
                        ),
                    )
                elif file_type == stat.S_IFIFO:
                    cur.execute(
                        """
                        INSERT INTO special_files (id_file, file_type, rdev_major, rdev_minor)
                        VALUES (%s, %s, 0, 0)
                        """,
                        (id_file, "fifo"),
                    )
                dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=False, cur=cur, owner_key=("file", id_file))
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                dbfs.append_journal_event(cur, "mknod", path, file_id=id_file, directory_id=parent_id)
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                return 0
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                raise dbfs.FuseOSError(errno.EEXIST) from exc

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
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                return

        parent_path = os.path.dirname(path)
        if kind == "dir":
            raise dbfs.FuseOSError(errno.EISDIR)
        if kind == "file":
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.enforce_sticky_bit(parent_path, dbfs.getattr(path))
                dbfs.append_journal_event(cur, "unlink", path, file_id=entry_id, directory_id=parent_id)
                link_count = dbfs.count_file_links(entry_id)
                if link_count > 1:
                    promoted = dbfs.promote_hardlink_to_primary(entry_id, cur)
                    if promoted is None:
                        raise dbfs.FuseOSError(errno.ENOENT)
                else:
                    cur.execute("DELETE FROM data_blocks WHERE id_file = %s", (entry_id,))
                    cur.execute("DELETE FROM files WHERE id_file = %s", (entry_id,))
                    dbfs.clear_write_buffer_dirty(entry_id)
                    dbfs.write_cache.pop(entry_id, None)
                    dbfs._clear_path_lock_state(("file", entry_id))
                    dbfs.clear_read_cache(entry_id)
                dbfs.delete_path_xattrs(path, cur=cur)
                if parent_id is not None:
                    cur.execute(
                        f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                        (parent_id,),
                    )
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
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
            dbfs.touch_namespace_epoch()
            dbfs.invalidate_metadata_cache(include_statfs=True)

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
                        link_count = dbfs.count_file_links(target_file_id)
                        if link_count > 1:
                            promoted = dbfs.promote_hardlink_to_primary(target_file_id, cur)
                            if promoted is None:
                                raise dbfs.FuseOSError(errno.ENOENT)
                        else:
                            cur.execute("DELETE FROM data_blocks WHERE id_file = %s", (target_file_id,))
                            cur.execute("DELETE FROM files WHERE id_file = %s", (target_file_id,))
                            dbfs.clear_write_buffer_dirty(target_file_id)
                            dbfs.write_cache.pop(target_file_id, None)
                            dbfs._clear_path_lock_state(("file", target_file_id))
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
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
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
            dbfs.touch_namespace_epoch()
            dbfs.invalidate_metadata_cache(include_statfs=True)
