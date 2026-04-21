from __future__ import annotations

import errno
import os
from typing import Any, Mapping

from fuse import FuseOSError


class XattrAclSupport:
    POSIX_ACL_XATTR_VERSION = 0x0002
    POSIX_ACL_TAG_USER_OBJ = 0x0001
    POSIX_ACL_TAG_USER = 0x0002
    POSIX_ACL_TAG_GROUP_OBJ = 0x0004
    POSIX_ACL_TAG_GROUP = 0x0008
    POSIX_ACL_TAG_MASK = 0x0010
    POSIX_ACL_TAG_OTHER = 0x0020

    def __init__(self, owner):
        self.owner = owner

    def normalize_xattr_name(self, name):
        if isinstance(name, bytes):
            return name.decode("utf-8")
        return str(name)

    def normalize_xattr_value(self, value):
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, str):
            return value.encode("utf-8")
        if value is None:
            return b""
        return bytes(value)

    def is_selinux_xattr(self, name):
        return name == "security.selinux"

    def is_posix_acl_xattr(self, name):
        return name in {"system.posix_acl_access", "system.posix_acl_default"}

    def parse_posix_acl_xattr(self, value):
        if isinstance(value, str):
            value = value.encode("utf-8")
        if len(value) < 4:
            raise FuseOSError(errno.EINVAL)

        version = int.from_bytes(value[0:4], byteorder="little", signed=False)
        if version != self.POSIX_ACL_XATTR_VERSION:
            raise FuseOSError(errno.EINVAL)

        entries = []
        idx = 4
        entry_size = 8
        while idx + entry_size <= len(value):
            tag = int.from_bytes(value[idx : idx + 2], byteorder="little", signed=False)
            perm = int.from_bytes(value[idx + 2 : idx + 4], byteorder="little", signed=False)
            entry_id = int.from_bytes(value[idx + 4 : idx + 8], byteorder="little", signed=False)
            if tag == self.POSIX_ACL_TAG_USER_OBJ:
                entry_id = -1
            elif tag in {self.POSIX_ACL_TAG_OTHER, self.POSIX_ACL_TAG_MASK, self.POSIX_ACL_TAG_GROUP_OBJ}:
                entry_id = -1
            entries.append({"tag": tag, "perm": perm, "id": entry_id})
            idx += entry_size

        if idx != len(value):
            raise FuseOSError(errno.EINVAL)

        return entries

    def format_posix_acl_xattr(self, entries):
        payload = bytearray()
        payload += int(self.POSIX_ACL_XATTR_VERSION).to_bytes(4, byteorder="little", signed=False)
        for entry in entries:
            tag = int(entry["tag"]) & 0xFFFF
            perm = int(entry.get("perm", 0)) & 0xFFFF
            entry_id = int(entry.get("id", -1))
            payload += tag.to_bytes(2, byteorder="little", signed=False)
            payload += perm.to_bytes(2, byteorder="little", signed=False)
            payload += (0 if entry_id < 0 else entry_id).to_bytes(4, byteorder="little", signed=False)
        return bytes(payload)

    def acl_permission_from_entries(self, entries, attrs, required_mode):
        current_uid, current_gid = self.owner.current_uid_gid()
        if current_uid == 0:
            return True

        required = self.owner.required_mode_mask(required_mode)
        named_group_matches = []
        mask_perm = 0o7
        user_obj_perm = None
        group_obj_perm = None
        other_perm = None
        named_user_perm = None

        for entry in entries:
            tag = entry["tag"]
            perm = entry["perm"] & 0o7
            if tag == self.POSIX_ACL_TAG_USER_OBJ:
                user_obj_perm = perm
            elif tag == self.POSIX_ACL_TAG_USER and entry["id"] == current_uid:
                named_user_perm = perm
            elif tag == self.POSIX_ACL_TAG_GROUP_OBJ:
                group_obj_perm = perm
            elif tag == self.POSIX_ACL_TAG_GROUP:
                named_group_matches.append((int(entry["id"]), perm))
            elif tag == self.POSIX_ACL_TAG_MASK:
                mask_perm = perm
            elif tag == self.POSIX_ACL_TAG_OTHER:
                other_perm = perm

        if attrs.get("st_uid") == current_uid:
            allowed = user_obj_perm if user_obj_perm is not None else 0
            return (allowed & required) == required

        if named_user_perm is not None:
            allowed = named_user_perm & mask_perm
            return (allowed & required) == required

        group_ids = self.owner.current_group_ids()
        group_allowed = 0
        if attrs.get("st_gid") in group_ids and group_obj_perm is not None:
            group_allowed |= group_obj_perm
        for group_id, perm in named_group_matches:
            if group_id in group_ids:
                group_allowed |= perm

        if group_allowed:
            allowed = group_allowed & mask_perm
            return (allowed & required) == required

        allowed = other_perm if other_perm is not None else 0
        return (allowed & required) == required

    def acl_allows(self, path, attrs, mode):
        if not self.owner.acl_enabled:
            return self.owner.can_access(attrs, mode)
        acl_value = self.owner.fetch_xattr_value(path, "system.posix_acl_access")
        if acl_value is None:
            return self.owner.can_access(attrs, mode)
        entries = self.parse_posix_acl_xattr(acl_value)
        return self.acl_permission_from_entries(entries, attrs, mode)

    def copy_default_acl_to_child(self, parent_path, child_path, child_is_dir, cur, owner_key=None):
        if not self.owner.acl_enabled:
            return
        default_acl = self.owner.fetch_xattr_value(parent_path, "system.posix_acl_default", cur=cur)
        if default_acl is None:
            return
        if owner_key is not None:
            self.owner.xattr_store.store_owner_xattr_value(owner_key, "system.posix_acl_access", default_acl, cur)
        else:
            self.owner.store_xattr_value(child_path, "system.posix_acl_access", default_acl, cur)
        if child_is_dir:
            if owner_key is not None:
                self.owner.xattr_store.store_owner_xattr_value(owner_key, "system.posix_acl_default", default_acl, cur)
            else:
                self.owner.store_xattr_value(child_path, "system.posix_acl_default", default_acl, cur)
