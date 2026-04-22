from __future__ import annotations

import os
import time


class MetadataSupport:
    def __init__(self, owner):
        self.owner = owner

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
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT target FROM symlinks WHERE id_symlink = %s",
                (symlink_id,),
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_symlink_attrs(self, path):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
            symlink_ctime = self.owner.ctime_column("symlinks")
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
            return result if result else None

    def get_special_file_metadata(self, file_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT file_type, rdev_major, rdev_minor FROM special_files WHERE id_file = %s",
                (file_id,),
            )
            result = cur.fetchone()
            if not result:
                return None
            file_type, rdev_major, rdev_minor = result
            rdev = os.makedev(int(rdev_major), int(rdev_minor)) if hasattr(os, "makedev") else 0
            return file_type, rdev

    def count_directory_children(self, directory_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM directories WHERE id_parent = %s)
                  + (SELECT COUNT(*) FROM files WHERE id_directory = %s)
                  + (SELECT COUNT(*) FROM hardlinks WHERE id_directory = %s)
                  + (SELECT COUNT(*) FROM symlinks WHERE id_parent = %s)
                """,
                (directory_id, directory_id, directory_id, directory_id),
            )
            return cur.fetchone()[0]

    def count_directory_subdirs(self, directory_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM directories WHERE id_parent = %s",
                (directory_id,),
            )
            return cur.fetchone()[0]

    def count_root_directory_children(self):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)
                FROM directories
                WHERE id_parent IS NULL AND name != '/'
                """,
            )
            return cur.fetchone()[0]

    def count_file_blocks(self, file_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM data_blocks WHERE id_file = %s",
                (file_id,),
            )
            return cur.fetchone()[0]

    def count_symlinks(self):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM symlinks")
            return cur.fetchone()[0]
