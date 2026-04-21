#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import errno
import os
import stat


class NamespaceRepositoryLookup:
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

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH RECURSIVE parts AS (
                    SELECT part, ord
                    FROM unnest(string_to_array(btrim(%s, '/'), '/')) WITH ORDINALITY AS t(part, ord)
                ),
                walk AS (
                    SELECT d.id_directory, p.ord
                    FROM directories d
                    JOIN parts p ON p.ord = 1
                    WHERE d.id_parent IS NULL AND d.name = p.part
                    UNION ALL
                    SELECT d.id_directory, p.ord
                    FROM walk w
                    JOIN parts p ON p.ord = w.ord + 1
                    JOIN directories d ON d.id_parent = w.id_directory AND d.name = p.part
                )
                SELECT id_directory
                FROM walk
                ORDER BY ord DESC
                LIMIT 1
                """,
                (path,),
            )
            result = cur.fetchone()
            value = result[0] if result else None
            self._dir_id_cache[path] = value
            return value

    def _entry_lookup(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._entry_cache:
            return self._entry_cache[path]

        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    """
                    SELECT kind, entry_id FROM (
                        SELECT 1 AS precedence, 'hardlink' AS kind, h.id_hardlink AS entry_id
                        FROM hardlinks h
                        WHERE h.name = %s AND h.id_directory IS NULL
                        UNION ALL
                        SELECT 2 AS precedence, 'symlink' AS kind, s.id_symlink AS entry_id
                        FROM symlinks s
                        WHERE s.name = %s AND s.id_parent IS NULL
                        UNION ALL
                        SELECT 3 AS precedence, 'file' AS kind, f.id_file AS entry_id
                        FROM files f
                        WHERE f.name = %s AND f.id_directory IS NULL
                        UNION ALL
                        SELECT 4 AS precedence, 'dir' AS kind, d.id_directory AS entry_id
                        FROM directories d
                        WHERE d.name = %s AND d.id_parent IS NULL
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (name, name, name, name),
                )
            else:
                cur.execute(
                    """
                    SELECT kind, entry_id FROM (
                        SELECT 1 AS precedence, 'hardlink' AS kind, h.id_hardlink AS entry_id
                        FROM hardlinks h
                        WHERE h.name = %s AND h.id_directory = %s
                        UNION ALL
                        SELECT 2 AS precedence, 'symlink' AS kind, s.id_symlink AS entry_id
                        FROM symlinks s
                        WHERE s.name = %s AND s.id_parent = %s
                        UNION ALL
                        SELECT 3 AS precedence, 'file' AS kind, f.id_file AS entry_id
                        FROM files f
                        WHERE f.name = %s AND f.id_directory = %s
                        UNION ALL
                        SELECT 4 AS precedence, 'dir' AS kind, d.id_directory AS entry_id
                        FROM directories d
                        WHERE d.name = %s AND d.id_parent = %s
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (name, parent_id, name, parent_id, name, parent_id, name, parent_id),
                )
            result = cur.fetchone()
            value = (parent_id, result[0], result[1]) if result else (parent_id, None, None)
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

        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    """
                    SELECT id_file FROM (
                        SELECT 1 AS precedence, id_file FROM hardlinks WHERE name = %s AND id_directory IS NULL
                        UNION ALL
                        SELECT 2 AS precedence, id_file FROM files WHERE name = %s AND id_directory IS NULL
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (file_name, file_name),
                )
            else:
                cur.execute(
                    """
                    SELECT id_file FROM (
                        SELECT 1 AS precedence, id_file FROM hardlinks WHERE name = %s AND id_directory = %s
                        UNION ALL
                        SELECT 2 AS precedence, id_file FROM files WHERE name = %s AND id_directory = %s
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (file_name, parent_id, file_name, parent_id),
                )

            result = cur.fetchone()
            value = result[0] if result else None
            self._file_id_cache[path] = value
            return value

    def get_file_mode_value(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._file_mode_cache:
            return self._file_mode_cache[path]

        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    """
                    SELECT mode FROM (
                        SELECT 1 AS precedence, mode FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file WHERE hardlinks.name = %s AND hardlinks.id_directory IS NULL
                        UNION ALL
                        SELECT 2 AS precedence, mode FROM files WHERE name = %s AND id_directory IS NULL
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (file_name, file_name),
                )
            else:
                cur.execute(
                    """
                    SELECT mode FROM (
                        SELECT 1 AS precedence, mode FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file WHERE hardlinks.name = %s AND hardlinks.id_directory = %s
                        UNION ALL
                        SELECT 2 AS precedence, mode FROM files WHERE name = %s AND id_directory = %s
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    """,
                    (file_name, parent_id, file_name, parent_id),
                )

            result = cur.fetchone()
            value = result[0] if result else None
            self._file_mode_cache[path] = value
            return value

    def get_hardlink_id(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._hardlink_id_cache:
            return self._hardlink_id_cache[path]

        parent_path = os.path.dirname(path)
        link_name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    "SELECT id_hardlink FROM hardlinks WHERE name = %s AND id_directory IS NULL",
                    (link_name,),
                )
            else:
                cur.execute(
                    "SELECT id_hardlink FROM hardlinks WHERE name = %s AND id_directory = %s",
                    (link_name, parent_id),
                )

            result = cur.fetchone()
            value = result[0] if result else None
            self._hardlink_id_cache[path] = value
            return value

    def get_hardlink_file_id(self, hardlink_id):
        dbfs = self.dbfs
        with dbfs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT id_file FROM hardlinks WHERE id_hardlink = %s", (hardlink_id,))
            result = cur.fetchone()
            return result[0] if result else None

    def get_symlink_id(self, path):
        dbfs = self.dbfs
        path = dbfs.normalize_path(path)
        self._refresh_lookup_cache()
        if path in self._symlink_id_cache:
            return self._symlink_id_cache[path]

        parent_path = os.path.dirname(path)
        link_name = os.path.basename(path)
        parent_id = self.get_dir_id(parent_path)

        with dbfs.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    "SELECT id_symlink FROM symlinks WHERE name = %s AND id_parent IS NULL",
                    (link_name,),
                )
            else:
                cur.execute(
                    "SELECT id_symlink FROM symlinks WHERE name = %s AND id_parent = %s",
                    (link_name, parent_id),
                )

            result = cur.fetchone()
            value = result[0] if result else None
            self._symlink_id_cache[path] = value
            return value

    def get_dir_id_by_path(self, path):
        return self.get_dir_id(self.dbfs.normalize_path(path))

    def get_entry_kind_and_id(self, path):
        _, kind, entry_id = self.resolve_path(path)
        return kind, entry_id

    def entry_exists(self, path, entry_kind):
        _, kind, entry_id = self.resolve_path(path)
        if kind != entry_kind:
            return None
        return entry_id

    def entry_exists_any(self, path):
        _, kind, entry_id = self.resolve_path(path)
        return kind is not None and entry_id is not None

    def count_file_links(self, file_id):
        dbfs = self.dbfs
        with dbfs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hardlinks WHERE id_file = %s", (file_id,))
            return 1 + cur.fetchone()[0]

    def promote_hardlink_to_primary(self, file_id, cur):
        cur.execute(
            """
            SELECT id_hardlink, id_directory, name
            FROM hardlinks
            WHERE id_file = %s
            ORDER BY id_hardlink ASC
            LIMIT 1
            """,
            (file_id,),
        )
        result = cur.fetchone()
        if not result:
            return None

        hardlink_id, hardlink_dir_id, hardlink_name = result
        cur.execute(
            """
            UPDATE files
            SET id_directory = %s, name = %s
            WHERE id_file = %s
            """,
            (hardlink_dir_id, hardlink_name, file_id),
        )
        cur.execute("DELETE FROM hardlinks WHERE id_hardlink = %s", (hardlink_id,))
        return hardlink_id, hardlink_dir_id, hardlink_name
