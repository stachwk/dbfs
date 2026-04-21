#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import errno
import os
import stat

from dbfs_time import db_timestamp_to_epoch


class NamespaceRepositoryAttrsListing:
    def get_symlink_attrs(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._symlink_attrs_cache:
            return self._symlink_attrs_cache[path]
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            symlink_ctime = dbfs.ctime_column("symlinks")
            if parent_id is None:
                cur.execute(
                    """
                    SELECT id_symlink, target, modification_date, access_date, {symlink_ctime}, uid, gid
                    FROM symlinks
                    WHERE name = %s AND id_parent IS NULL
                """.format(symlink_ctime=symlink_ctime),
                    (name,),
                )
            else:
                cur.execute(
                    """
                    SELECT id_symlink, target, modification_date, access_date, {symlink_ctime}, uid, gid
                    FROM symlinks
                    WHERE name = %s AND id_parent = %s
                """.format(symlink_ctime=symlink_ctime),
                    (name, parent_id),
                )

            result = cur.fetchone()
            value = result if result else None
            self._symlink_attrs_cache[path] = value
            return value

    def readlink(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        entry = self.get_symlink_attrs(path)
        if entry is None:
            raise dbfs.FuseOSError(errno.ENOENT)
        return entry[1]

    def list_directory_entries(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if path == '/':
                cur.execute("""
                    SELECT name FROM directories WHERE id_parent IS NULL AND name != '/'
                    UNION ALL
                    SELECT name FROM files WHERE id_directory IS NULL
                    UNION ALL
                    SELECT name FROM hardlinks WHERE id_directory IS NULL
                    UNION ALL
                    SELECT name FROM symlinks WHERE id_parent IS NULL
                """)
                return {
                    "directory_id": None,
                    "entries": [row[0] for row in cur.fetchall()],
                }

            directory_id = self.get_dir_id(path)
            cur.execute("""
                SELECT name FROM files WHERE id_directory = %s
                UNION ALL
                SELECT name FROM hardlinks WHERE id_directory = %s
                UNION ALL
                SELECT name FROM directories WHERE id_parent = %s
                UNION ALL
                SELECT name FROM symlinks WHERE id_parent = %s
            """, (directory_id, directory_id, directory_id, directory_id))
            return {
                "directory_id": directory_id,
                "entries": [row[0] for row in cur.fetchall()],
            }

    def fetch_path_attrs(self, path, now=None):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        now = now if now is not None else 0.0

        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            symlink_ctime = dbfs.ctime_column("symlinks")
            file_ctime = dbfs.ctime_column("files")
            dir_ctime = dbfs.ctime_column("directories")
            if parent_id is None:
                cur.execute("""
                    SELECT id_symlink, 0, target, modification_date, access_date, {symlink_ctime}, uid, gid, inode_seed, 'symlink' FROM symlinks
                    WHERE name = %s AND id_parent IS NULL
                    UNION ALL
                    SELECT hardlinks.id_hardlink, files.size, files.mode, files.modification_date, files.access_date, files.{file_ctime}, files.uid, files.gid, files.inode_seed, 'hardlink'
                    FROM hardlinks
                    JOIN files ON hardlinks.id_file = files.id_file
                    WHERE hardlinks.name = %s AND hardlinks.id_directory IS NULL
                    UNION ALL
                    SELECT id_file, size, mode, modification_date, access_date, {file_ctime}, uid, gid, inode_seed, 'file' FROM files
                    WHERE name = %s AND id_directory IS NULL
                    UNION ALL
                    SELECT id_directory, 0, mode, modification_date, access_date, {dir_ctime}, uid, gid, inode_seed, 'dir' FROM directories
                    WHERE name = %s AND id_parent IS NULL
                """.format(symlink_ctime=symlink_ctime, file_ctime=file_ctime, dir_ctime=dir_ctime), (name, name, name, name))
            else:
                cur.execute("""
                    SELECT id_symlink, 0, target, modification_date, access_date, {symlink_ctime}, uid, gid, inode_seed, 'symlink' FROM symlinks
                    WHERE name = %s AND id_parent = %s
                    UNION ALL
                    SELECT hardlinks.id_hardlink, files.size, files.mode, files.modification_date, files.access_date, files.{file_ctime}, files.uid, files.gid, files.inode_seed, 'hardlink'
                    FROM hardlinks
                    JOIN files ON hardlinks.id_file = files.id_file
                    WHERE hardlinks.name = %s AND hardlinks.id_directory = %s
                    UNION ALL
                    SELECT id_file, size, mode, modification_date, access_date, {file_ctime}, uid, gid, inode_seed, 'file' FROM files
                    WHERE name = %s AND id_directory = %s
                    UNION ALL
                    SELECT id_directory, 0, mode, modification_date, access_date, {dir_ctime}, uid, gid, inode_seed, 'dir' FROM directories
                    WHERE name = %s AND id_parent = %s
                """.format(symlink_ctime=symlink_ctime, file_ctime=file_ctime, dir_ctime=dir_ctime), (name, parent_id, name, parent_id, name, parent_id, name, parent_id))

            result = cur.fetchone()
            if not result:
                raise dbfs.FuseOSError(errno.ENOENT)

            raw_inode, size, mode, mod_date, acc_date, chg_date, uid, gid, inode_seed, obj_type = result
            inode = raw_inode
            file_link_id = None

            if obj_type == 'hardlink':
                file_link_id = self.get_hardlink_file_id(raw_inode)
                if file_link_id is not None:
                    inode = dbfs.stable_inode("file", inode_seed, file_link_id)
            elif obj_type in {"file", "dir", "symlink"}:
                inode = dbfs.stable_inode(obj_type, inode_seed, raw_inode)

            if obj_type == 'symlink':
                mode_bits = 0o777
                if isinstance(mode, str):
                    size = len(mode.encode("utf-8"))
                elif isinstance(mode, (bytes, bytearray)):
                    size = len(mode)
                else:
                    size = len(str(mode).encode("utf-8"))
                file_type = stat.S_IFLNK
                rdev = 0
            else:
                file_type, mode_bits, rdev = stat.S_IFREG, dbfs.file_mode_bits(mode), 0
                if obj_type == 'hardlink':
                    special_file_id = file_link_id if file_link_id is not None else raw_inode
                else:
                    special_file_id = raw_inode
                special_metadata = dbfs.get_special_file_metadata(special_file_id)
                if special_metadata is not None:
                    special_type, rdev = special_metadata
                    if special_type == "fifo":
                        file_type = stat.S_IFIFO
                    elif special_type == "char":
                        file_type = stat.S_IFCHR
                    elif special_type == "block":
                        file_type = stat.S_IFBLK
                if mode_bits == 0o644 and obj_type == 'dir':
                    mode_bits = 0o755
                if obj_type in {'file', 'hardlink'} and file_type == stat.S_IFREG:
                    size = int(size)

            if obj_type == 'hardlink':
                st_nlink = dbfs.count_file_links(file_link_id if file_link_id is not None else raw_inode)
            elif obj_type == 'file':
                st_nlink = dbfs.count_file_links(raw_inode)
            elif obj_type == 'symlink':
                st_nlink = 1
            else:
                st_nlink = 2 + dbfs.count_directory_subdirs(raw_inode)

            st_mode = (
                file_type | mode_bits
                if obj_type == 'file'
                else file_type | mode_bits
                if obj_type == 'hardlink'
                else stat.S_IFDIR | mode_bits
                if obj_type == 'dir'
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
                'st_uid': uid if uid is not None else dbfs.current_uid_gid()[0],
                'st_gid': gid if gid is not None else dbfs.current_uid_gid()[1],
            }
