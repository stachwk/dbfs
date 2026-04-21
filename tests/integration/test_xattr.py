#!/usr/bin/env python3

import errno
import os
import struct
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from fuse import FuseOSError

from dbfs_fuse import DBFS, load_dsn_from_config


def build_posix_acl(user_perm: int, group_perm: int, other_perm: int) -> bytes:
    version = 0x0002
    acl = bytearray(struct.pack("<I", version))
    acl.extend(struct.pack("<HHI", 0x0001, user_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0004, group_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0010, group_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0020, other_perm & 0o7, 0xFFFFFFFF))
    return bytes(acl)


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config, selinux_mode="on", acl_mode="on")
    fs_off = DBFS(dsn, db_config, selinux_mode="off", acl_mode="off")

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/xattr_{suffix}"
    file_path = f"{dir_path}/meta.txt"
    renamed_path = f"{dir_path}/meta-renamed.txt"
    user_value = b"dbfs-note"
    trusted_value = b"dbfs-trusted"
    selinux_value = b"system_u:object_r:tmp_t:s0"
    access_acl = build_posix_acl(user_perm=0o6, group_perm=0o0, other_perm=0o0)
    fh = None
    off_path = f"/xattr_off_{suffix}.txt"
    off_fh = None
    acl_dir_path = f"/xattr_acl_{suffix}"
    acl_file_path = f"{acl_dir_path}/child.txt"
    acl_fh = None
    hardlink_path = f"/xattr_hardlink_{suffix}.txt"
    hardlink_linked_path = f"/xattr_hardlink_linked_{suffix}.txt"

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)

        fs.setxattr(file_path, "user.comment", user_value, 0)
        assert fs.getxattr(file_path, "user.comment") == user_value
        assert "user.comment" in fs.listxattr(file_path)

        fs.setxattr(file_path, "trusted.dbfs", trusted_value, 0)
        assert fs.getxattr(file_path, "trusted.dbfs") == trusted_value
        assert "trusted.dbfs" in fs.listxattr(file_path)

        fs.setxattr(file_path, "security.selinux", selinux_value, 0)
        assert fs.getxattr(file_path, "security.selinux") == selinux_value
        assert "security.selinux" in fs.listxattr(file_path)

        fs.setxattr(file_path, "system.posix_acl_access", access_acl, 0)
        assert fs.getxattr(file_path, "system.posix_acl_access") == access_acl
        assert "system.posix_acl_access" in fs.listxattr(file_path)

        fs.link(hardlink_path, file_path)
        assert fs.getxattr(hardlink_path, "user.comment") == user_value
        fs.setxattr(hardlink_path, "user.comment", b"dbfs-note-hardlink", 0)
        assert fs.getxattr(file_path, "user.comment") == b"dbfs-note-hardlink"
        fs.rename(hardlink_path, hardlink_linked_path)
        assert fs.getxattr(hardlink_linked_path, "user.comment") == b"dbfs-note-hardlink"

        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fs.rename(file_path, renamed_path)
        assert fs.getxattr(renamed_path, "user.comment") == b"dbfs-note-hardlink"
        assert fs.getxattr(renamed_path, "trusted.dbfs") == trusted_value
        assert fs.getxattr(renamed_path, "security.selinux") == selinux_value

        fs.removexattr(renamed_path, "user.comment")
        assert "user.comment" not in fs.listxattr(renamed_path)
        fs.removexattr(renamed_path, "trusted.dbfs")
        assert "trusted.dbfs" not in fs.listxattr(renamed_path)

        fs.mkdir(acl_dir_path, 0o755)
        fs.setxattr(acl_dir_path, "system.posix_acl_default", access_acl, 0)
        acl_fh = fs.create(acl_file_path, 0o000)
        assert fs.getxattr(acl_file_path, "system.posix_acl_access") == access_acl
        assert "system.posix_acl_access" in fs.listxattr(acl_file_path)
        assert fs.access(acl_file_path, os.R_OK) == 0
        assert fs.access(acl_file_path, os.W_OK) == 0
        acl_open_fh = fs.open(acl_file_path, os.O_RDONLY)
        fs.release(acl_file_path, acl_open_fh)
        try:
            fs.access(acl_file_path, os.X_OK)
            raise AssertionError("execute access unexpectedly granted by ACL")
        except FuseOSError as exc:
            assert exc.errno == errno.EACCES, f"unexpected errno: {exc.errno}"

        fs.release(acl_file_path, acl_fh)
        acl_fh = None

        fs.setxattr(renamed_path, "system.posix_acl_access", access_acl, 0)
        assert fs.getxattr(renamed_path, "system.posix_acl_access") == access_acl
        fs.removexattr(renamed_path, "system.posix_acl_access")
        assert "system.posix_acl_access" not in fs.listxattr(renamed_path)

        off_fh = fs_off.create(off_path, 0o644)
        try:
            fs_off.setxattr(off_path, "security.selinux", selinux_value, 0)
            raise AssertionError("security.selinux xattr unexpectedly enabled in selinux=off mode")
        except FuseOSError as exc:
            assert exc.errno == errno.EOPNOTSUPP, f"unexpected errno: {exc.errno}"
        try:
            fs_off.setxattr(off_path, "system.posix_acl_access", access_acl, 0)
            raise AssertionError("system.posix_acl_access xattr unexpectedly enabled in acl=off mode")
        except FuseOSError as exc:
            assert exc.errno == errno.EOPNOTSUPP, f"unexpected errno: {exc.errno}"
        finally:
            fs_off.release(off_path, off_fh)
            off_fh = None
            fs_off.unlink(off_path)

        print("OK xattr/selinux/acl")
    finally:
        if fh is not None:
            try:
                fs.release(file_path, fh)
            except Exception:
                pass

        for candidate in (renamed_path, file_path):
            try:
                fs.unlink(candidate)
            except Exception:
                pass

        for candidate in (hardlink_linked_path, hardlink_path):
            try:
                fs.unlink(candidate)
            except Exception:
                pass

        for candidate in (acl_file_path,):
            try:
                fs.unlink(candidate)
            except Exception:
                pass

        try:
            fs.rmdir(dir_path)
        except Exception:
            pass

        try:
            fs.rmdir(acl_dir_path)
        except Exception:
            pass

        if off_fh is not None:
            try:
                fs_off.release(off_path, off_fh)
            except Exception:
                pass
            try:
                fs_off.unlink(off_path)
            except Exception:
                pass

        if acl_fh is not None:
            try:
                fs.release(acl_file_path, acl_fh)
            except Exception:
                pass


if __name__ == "__main__":
    main()
