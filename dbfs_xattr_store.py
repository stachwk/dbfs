from __future__ import annotations

from typing import Any


class XattrStore:
    def __init__(self, owner):
        self.owner = owner

    def _root_dir_id(self):
        return 0

    def resolve_xattr_owner(self, path):
        kind, entry_id = self.owner.get_entry_kind_and_id(path)
        if kind is None:
            return None
        if kind == "hardlink":
            file_id = self.owner.get_file_id(path)
            if file_id is None:
                return None
            return ("file", int(file_id))
        if kind == "file":
            return ("file", int(entry_id))
        if kind == "dir":
            return ("dir", self._root_dir_id() if entry_id is None else int(entry_id))
        if kind == "symlink":
            return ("symlink", int(entry_id))
        return None

    def normalize_xattr_name(self, name):
        return self.owner.xattr_acl.normalize_xattr_name(name)

    def normalize_xattr_value(self, value):
        return self.owner.xattr_acl.normalize_xattr_value(value)

    def is_selinux_xattr(self, name):
        return self.owner.xattr_acl.is_selinux_xattr(name)

    def is_posix_acl_xattr(self, name):
        return self.owner.xattr_acl.is_posix_acl_xattr(name)

    def parse_posix_acl_xattr(self, value):
        return self.owner.xattr_acl.parse_posix_acl_xattr(value)

    def build_posix_acl_xattr(self, entries):
        return self.owner.xattr_acl.format_posix_acl_xattr(entries)

    def _owner_clause(self, owner_key):
        owner_kind, owner_id = owner_key
        if owner_kind == "dir" and owner_id == 0:
            return owner_kind, owner_id
        return owner_kind, int(owner_id)

    def fetch_xattr_value(self, path, name, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return None
        xattr_name = self.normalize_xattr_name(name)
        owner_kind, owner_id = self._owner_clause(owner_key)
        if cur is None:
            with self.owner.db_connection() as conn, conn.cursor() as cur2:
                cur2.execute(
                    "SELECT value FROM xattrs WHERE owner_kind = %s AND owner_id = %s AND name = %s",
                    (owner_kind, owner_id, xattr_name),
                )
                result = cur2.fetchone()
                return bytes(result[0]) if result else None

        cur.execute(
            "SELECT value FROM xattrs WHERE owner_kind = %s AND owner_id = %s AND name = %s",
            (owner_kind, owner_id, xattr_name),
        )
        result = cur.fetchone()
        return bytes(result[0]) if result else None

    def list_xattr_names(self, path, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return []
        owner_kind, owner_id = self._owner_clause(owner_key)
        if cur is None:
            with self.owner.db_connection() as conn, conn.cursor() as cur2:
                cur2.execute(
                    "SELECT name FROM xattrs WHERE owner_kind = %s AND owner_id = %s ORDER BY name",
                    (owner_kind, owner_id),
                )
                return [row[0] for row in cur2.fetchall()]
        cur.execute(
            "SELECT name FROM xattrs WHERE owner_kind = %s AND owner_id = %s ORDER BY name",
            (owner_kind, owner_id),
        )
        return [row[0] for row in cur.fetchall()]

    def store_xattr_value(self, path, name, value, cur):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return
        self.store_owner_xattr_value(owner_key, name, value, cur)

    def store_owner_xattr_value(self, owner_key, name, value, cur):
        owner_kind, owner_id = self._owner_clause(owner_key)
        xattr_name = self.normalize_xattr_name(name)
        stored_value = self.normalize_xattr_value(value)
        cur.execute(
            """
            INSERT INTO xattrs (owner_kind, owner_id, name, value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (owner_kind, owner_id, name) DO UPDATE
            SET value = EXCLUDED.value
            """,
            (owner_kind, owner_id, xattr_name, stored_value),
        )

    def delete_inode_xattrs(self, path, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return
        owner_kind, owner_id = self._owner_clause(owner_key)
        if cur is not None:
            cur.execute(
                "DELETE FROM xattrs WHERE owner_kind = %s AND owner_id = %s",
                (owner_kind, owner_id),
            )
            return
        with self.owner.db_connection() as conn, conn.cursor() as cur2:
            self.delete_inode_xattrs(path, cur=cur2)
            conn.commit()

    def remove_xattr(self, path, name, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return 0
        owner_kind, owner_id = self._owner_clause(owner_key)
        xattr_name = self.normalize_xattr_name(name)
        if cur is not None:
            cur.execute(
                "DELETE FROM xattrs WHERE owner_kind = %s AND owner_id = %s AND name = %s",
                (owner_kind, owner_id, xattr_name),
            )
            return cur.rowcount
        with self.owner.db_connection() as conn, conn.cursor() as cur2:
            self.remove_xattr(path, name, cur=cur2)
            conn.commit()
            return cur2.rowcount

    def move_path_xattrs(self, old_path, new_path, recursive=False, cur=None):
        # Inode-centric xattrs do not move on rename.
        return 0

    def copy_default_acl_to_child(self, parent_path, child_path, child_is_dir, cur, owner_key=None):
        return self.owner.xattr_acl.copy_default_acl_to_child(parent_path, child_path, child_is_dir, cur, owner_key=owner_key)
