#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import errno
import os
import stat

import psycopg2


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
        rust_dir_id = dbfs.backend.python_to_rust_namespace_create_directory(
            parent_id,
            dir_name,
            inherited_mode,
            uid,
            gid,
            inode_seed,
        )
        if rust_dir_id is not None:
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.copy_default_acl_to_child(parent_path, path, child_is_dir=True, cur=cur, owner_key=("dir", rust_dir_id))
                dbfs.append_journal_event(cur, "mkdir", path, directory_id=rust_dir_id)
                conn.commit()
            dbfs.touch_namespace_epoch()
            dbfs.invalidate_metadata_cache(include_statfs=True)
            return

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
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

        uid, gid = dbfs.creation_uid_gid(parent_path)
        inode_seed = dbfs.generate_inode_seed()
        rust_symlink_id = dbfs.backend.python_to_rust_namespace_create_symlink(
            parent_id,
            link_name,
            source,
            uid,
            gid,
            inode_seed,
        )
        if rust_symlink_id is not None:
            with dbfs.db_connection() as conn, conn.cursor() as cur:
                dbfs.append_journal_event(cur, "symlink", target, directory_id=parent_id)
                conn.commit()
            dbfs.touch_namespace_epoch()
            dbfs.invalidate_metadata_cache(include_statfs=True)
            return 0

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
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

        uid, gid = dbfs.current_uid_gid()
        rust_hardlink_id = dbfs.backend.python_to_rust_namespace_create_hardlink(
            source_file_id,
            target_parent_id,
            target_name,
            uid,
            gid,
        )

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            try:
                hardlink_ctime = dbfs.ctime_column("hardlinks")
                if rust_hardlink_id is None:
                    cur.execute(
                        """
                        INSERT INTO hardlinks (id_file, id_directory, name, uid, gid, {hardlink_ctime}, creation_date, modification_date, access_date)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())
                        RETURNING id_hardlink
                        """.format(hardlink_ctime=hardlink_ctime),
                        (source_file_id, target_parent_id, target_name, uid, gid),
                    )
                    rust_hardlink_id = cur.fetchone()[0]
                    if target_parent_id is not None:
                        cur.execute(
                            f"UPDATE directories SET modification_date = NOW(), {dbfs.ctime_column('directories')} = NOW() WHERE id_directory = %s",
                            (target_parent_id,),
                        )
                dbfs.append_journal_event(cur, "link", target, file_id=source_file_id, directory_id=target_parent_id)
                conn.commit()
                dbfs.touch_namespace_epoch()
                dbfs.invalidate_metadata_cache(include_statfs=True)
                dbfs.logging.debug("link created target=%s source=%s file_id=%s hardlink_id=%s", target, source, source_file_id, rust_hardlink_id)
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
