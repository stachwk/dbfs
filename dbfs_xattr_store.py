from __future__ import annotations

import ctypes
import errno
from typing import Any


class XattrStore:
    def __init__(self, owner):
        self.owner = owner

    def _rust_repo(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise self.owner.FuseOSError(errno.EIO)
        return repo, lib

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
        backend = getattr(self.owner, "backend", None)
        if backend is not None:
            repo = backend._load_rust_pg_repo()
            lib = backend._load_rust_hotpath_lib()
            if repo is not None and lib is not None:
                path_bytes = str(path).encode("utf-8")
                name_bytes = str(name).encode("utf-8")
                out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
                out_len = ctypes.c_size_t()
                out_found = ctypes.c_ubyte()
                status = lib.dbfs_rust_pg_repo_fetch_xattr_value(
                    repo,
                    path_bytes,
                    len(path_bytes),
                    name_bytes,
                    len(name_bytes),
                    ctypes.byref(out_ptr),
                    ctypes.byref(out_len),
                    ctypes.byref(out_found),
                )
                if status == 0 and out_found.value:
                    try:
                        return ctypes.string_at(out_ptr, out_len.value)
                    finally:
                        lib.dbfs_free_bytes(out_ptr, out_len)
            raise self.owner.FuseOSError(errno.EIO)
        raise self.owner.FuseOSError(errno.EIO)

    def list_xattr_names(self, path, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return []
        owner_kind, owner_id = self._owner_clause(owner_key)
        repo, lib = self._rust_repo()
        owner_kind_bytes = str(owner_kind).encode("utf-8")
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_list_xattr_names_for_owner(
            repo,
            owner_kind_bytes,
            len(owner_kind_bytes),
            int(owner_id),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
            ctypes.byref(out_found),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)
        if not out_found.value:
            return []
        try:
            raw = ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)
        return [line for line in raw.split("\n") if line]

    def store_xattr_value(self, path, name, value, cur):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return
        self.store_owner_xattr_value(owner_key, name, value, cur)

    def store_owner_xattr_value(self, owner_key, name, value, cur):
        owner_kind, owner_id = self._owner_clause(owner_key)
        repo, lib = self._rust_repo()
        owner_kind_bytes = str(owner_kind).encode("utf-8")
        name_bytes = str(self.normalize_xattr_name(name)).encode("utf-8")
        value_bytes = bytes(self.normalize_xattr_value(value))
        status = lib.dbfs_rust_pg_repo_store_xattr_value_for_owner(
            repo,
            owner_kind_bytes,
            len(owner_kind_bytes),
            int(owner_id),
            name_bytes,
            len(name_bytes),
            value_bytes,
            len(value_bytes),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)

    def delete_inode_xattrs(self, path, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return
        owner_kind, owner_id = self._owner_clause(owner_key)
        repo, lib = self._rust_repo()
        owner_kind_bytes = str(owner_kind).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_delete_owner_xattrs(
            repo,
            owner_kind_bytes,
            len(owner_kind_bytes),
            int(owner_id),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)

    def remove_xattr(self, path, name, cur=None):
        owner_key = self.resolve_xattr_owner(path)
        if owner_key is None:
            return 0
        owner_kind, owner_id = self._owner_clause(owner_key)
        repo, lib = self._rust_repo()
        owner_kind_bytes = str(owner_kind).encode("utf-8")
        name_bytes = str(self.normalize_xattr_name(name)).encode("utf-8")
        out_deleted = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_remove_xattr_for_owner(
            repo,
            owner_kind_bytes,
            len(owner_kind_bytes),
            int(owner_id),
            name_bytes,
            len(name_bytes),
            ctypes.byref(out_deleted),
        )
        if status != 0:
            raise self.owner.FuseOSError(errno.EIO)
        return int(out_deleted.value)

    def move_path_xattrs(self, old_path, new_path, recursive=False, cur=None):
        # Inode-centric xattrs do not move on rename.
        return 0

    def copy_default_acl_to_child(self, parent_path, child_path, child_is_dir, cur, owner_key=None):
        return self.owner.xattr_acl.copy_default_acl_to_child(parent_path, child_path, child_is_dir, cur, owner_key=owner_key)
