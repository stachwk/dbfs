from __future__ import annotations

import errno
import os
import stat

from fuse import FuseOSError


class PermissionPolicy:
    def __init__(self, dbfs):
        self.dbfs = dbfs

    def required_mode_mask(self, mode):
        mask = 0
        if mode & os.R_OK:
            mask |= 0o4
        if mode & os.W_OK:
            mask |= 0o2
        if mode & os.X_OK:
            mask |= 0o1
        return mask

    def can_access(self, attrs, mode):
        current_uid, current_gid = self.dbfs.current_uid_gid()
        if current_uid == 0:
            return True

        file_mode = attrs.get("st_mode", 0)
        if current_uid == attrs.get("st_uid"):
            shift = 6
        else:
            group_ids = {current_gid}
            if hasattr(os, "getgroups"):
                group_ids.update(os.getgroups())

            if attrs.get("st_gid") in group_ids:
                shift = 3
            else:
                shift = 0

        allowed = (file_mode >> shift) & 0o7
        required = self.required_mode_mask(mode)
        return (allowed & required) == required

    def enforce_sticky_bit(self, parent_path, entry_attrs):
        parent_path = self.dbfs.normalize_path(parent_path)
        if parent_path == "/":
            return
        parent_attrs = self.dbfs.getattr(parent_path)
        if not (parent_attrs.get("st_mode", 0) & stat.S_ISVTX):
            return

        current_uid, _ = self.dbfs.current_uid_gid()
        if current_uid == 0:
            return
        if current_uid in {entry_attrs.get("st_uid"), parent_attrs.get("st_uid")}:
            return
        raise FuseOSError(errno.EPERM)

    def can_modify_metadata(self, attrs):
        current_uid, _ = self.dbfs.current_uid_gid()
        return current_uid == 0 or current_uid == attrs.get("st_uid")

    def can_change_owner(self, attrs, uid, gid):
        current_uid, _ = self.dbfs.current_uid_gid()
        if current_uid == 0:
            return True
        if attrs.get("st_uid") != current_uid:
            return False
        if uid not in (-1, attrs.get("st_uid"), current_uid):
            return False
        allowed_gids = {-1} | set(self.dbfs.current_group_ids())
        if gid not in allowed_gids:
            return False
        return True
