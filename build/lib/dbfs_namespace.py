from __future__ import annotations

import os


class NamespaceSupport:
    def __init__(self, owner):
        self.owner = owner

    def get_file_id(self, path):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    "SELECT id_file FROM hardlinks WHERE name = %s AND id_directory IS NULL",
                    (file_name,),
                )
            else:
                cur.execute(
                    "SELECT id_file FROM hardlinks WHERE name = %s AND id_directory = %s",
                    (file_name, parent_id),
                )

            result = cur.fetchone()
            if result:
                return result[0]

            if parent_id is None:
                cur.execute(
                    "SELECT id_file FROM files WHERE name = %s AND id_directory IS NULL",
                    (file_name,),
                )
            else:
                cur.execute(
                    "SELECT id_file FROM files WHERE name = %s AND id_directory = %s",
                    (file_name, parent_id),
                )

            result = cur.fetchone()
            return result[0] if result else None

    def get_file_mode_value(self, path):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
            if parent_id is None:
                cur.execute(
                    "SELECT mode FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file WHERE hardlinks.name = %s AND hardlinks.id_directory IS NULL",
                    (file_name,),
                )
            else:
                cur.execute(
                    "SELECT mode FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file WHERE hardlinks.name = %s AND hardlinks.id_directory = %s",
                    (file_name, parent_id),
                )

            result = cur.fetchone()
            if result:
                return result[0]

            if parent_id is None:
                cur.execute(
                    "SELECT mode FROM files WHERE name = %s AND id_directory IS NULL",
                    (file_name,),
                )
            else:
                cur.execute(
                    "SELECT mode FROM files WHERE name = %s AND id_directory = %s",
                    (file_name, parent_id),
                )

            result = cur.fetchone()
            return result[0] if result else None

    def get_hardlink_id(self, path):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        link_name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
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
            return result[0] if result else None

    def get_hardlink_file_id(self, hardlink_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT id_file FROM hardlinks WHERE id_hardlink = %s", (hardlink_id,))
            result = cur.fetchone()
            return result[0] if result else None

    def get_symlink_id(self, path):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        link_name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
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
            return result[0] if result else None

    def get_dir_id_by_path(self, path):
        return self.owner.get_dir_id(self.owner.normalize_path(path))

    def get_entry_kind_and_id(self, path):
        path = self.owner.normalize_path(path)
        hardlink_id = self.get_hardlink_id(path)
        if hardlink_id is not None:
            return "hardlink", hardlink_id

        symlink_id = self.get_symlink_id(path)
        if symlink_id is not None:
            return "symlink", symlink_id

        file_id = self.get_file_id(path)
        if file_id is not None:
            return "file", file_id

        dir_id = self.get_dir_id_by_path(path)
        if dir_id is not None:
            return "dir", dir_id

        return None, None

    def entry_exists(self, path, entry_kind):
        path = self.owner.normalize_path(path)
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_id = self.owner.get_dir_id(parent_path)

        with self.owner.db_connection() as conn, conn.cursor() as cur:
            if entry_kind == "file":
                if parent_id is None:
                    cur.execute(
                        "SELECT id_file FROM files WHERE name = %s AND id_directory IS NULL",
                        (name,),
                    )
                else:
                    cur.execute(
                        "SELECT id_file FROM files WHERE name = %s AND id_directory = %s",
                        (name, parent_id),
                    )
            else:
                if parent_id is None:
                    cur.execute(
                        "SELECT id_hardlink FROM hardlinks WHERE name = %s AND id_directory IS NULL",
                        (name,),
                    )
                else:
                    cur.execute(
                        "SELECT id_hardlink FROM hardlinks WHERE name = %s AND id_directory = %s",
                        (name, parent_id),
                    )

            result = cur.fetchone()
            return result[0] if result else None

    def entry_exists_any(self, path):
        kind, entry_id = self.get_entry_kind_and_id(path)
        return kind is not None and entry_id is not None

    def count_file_links(self, file_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
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
