#!/usr/bin/env python3

from __future__ import annotations

import errno
import os
import struct
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def build_posix_acl(user_perm: int, group_perm: int, other_perm: int) -> bytes:
    version = 0x0002
    acl = bytearray(struct.pack("<I", version))
    acl.extend(struct.pack("<HHI", 0x0001, user_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0004, group_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0010, group_perm & 0o7, 0xFFFFFFFF))
    acl.extend(struct.pack("<HHI", 0x0020, other_perm & 0o7, 0xFFFFFFFF))
    return bytes(acl)


def main() -> None:
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-xattr-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"xattr_{suffix}"
            file_path = dir_path / "meta.txt"
            renamed_path = dir_path / "meta-renamed.txt"
            user_value = b"dbfs-note"
            trusted_value = b"dbfs-trusted"
            selinux_value = b"system_u:object_r:tmp_t:s0"
            access_acl = build_posix_acl(user_perm=0o6, group_perm=0o0, other_perm=0o0)
            off_path = mountpoint / f"xattr_off_{suffix}.txt"
            acl_dir_path = mountpoint / f"xattr_acl_{suffix}"
            acl_file_path = acl_dir_path / "child.txt"
            hardlink_path = mountpoint / f"xattr_hardlink_{suffix}.txt"
            hardlink_linked_path = mountpoint / f"xattr_hardlink_linked_{suffix}.txt"

            dir_path.mkdir()
            file_path.write_bytes(b"payload\n")

            os.setxattr(file_path, "user.comment", user_value)
            assert os.getxattr(file_path, "user.comment") == user_value
            assert "user.comment" in os.listxattr(file_path)

            os.setxattr(file_path, "trusted.dbfs", trusted_value)
            assert os.getxattr(file_path, "trusted.dbfs") == trusted_value
            assert "trusted.dbfs" in os.listxattr(file_path)

            os.setxattr(file_path, "security.selinux", selinux_value)
            assert os.getxattr(file_path, "security.selinux") == selinux_value
            assert "security.selinux" in os.listxattr(file_path)

            os.setxattr(file_path, "system.posix_acl_access", access_acl)
            assert os.getxattr(file_path, "system.posix_acl_access") == access_acl
            assert "system.posix_acl_access" in os.listxattr(file_path)

            os.link(file_path, hardlink_path)
            assert os.getxattr(hardlink_path, "user.comment") == user_value
            os.setxattr(hardlink_path, "user.comment", b"dbfs-note-hardlink")
            assert os.getxattr(file_path, "user.comment") == b"dbfs-note-hardlink"
            os.rename(hardlink_path, hardlink_linked_path)
            assert os.getxattr(hardlink_linked_path, "user.comment") == b"dbfs-note-hardlink"

            os.rename(file_path, renamed_path)
            assert os.getxattr(renamed_path, "user.comment") == b"dbfs-note-hardlink"
            assert os.getxattr(renamed_path, "trusted.dbfs") == trusted_value
            assert os.getxattr(renamed_path, "security.selinux") == selinux_value

            os.removexattr(renamed_path, "user.comment")
            assert "user.comment" not in os.listxattr(renamed_path)
            os.removexattr(renamed_path, "trusted.dbfs")
            assert "trusted.dbfs" not in os.listxattr(renamed_path)

            acl_dir_path.mkdir()
            os.setxattr(acl_dir_path, "system.posix_acl_default", access_acl)
            acl_file_path.write_bytes(b"acl-payload\n")
            assert os.getxattr(acl_file_path, "system.posix_acl_access") == access_acl
            assert "system.posix_acl_access" in os.listxattr(acl_file_path)
            assert os.access(acl_file_path, os.R_OK)
            assert os.access(acl_file_path, os.W_OK)
            assert not os.access(acl_file_path, os.X_OK)

            os.setxattr(renamed_path, "system.posix_acl_access", access_acl)
            assert os.getxattr(renamed_path, "system.posix_acl_access") == access_acl
            os.removexattr(renamed_path, "system.posix_acl_access")
            assert "system.posix_acl_access" not in os.listxattr(renamed_path)

            off_path.write_bytes(b"off\n")
            try:
                os.setxattr(off_path, "security.selinux", selinux_value)
                raise AssertionError("security.selinux xattr unexpectedly enabled in selinux=off mode")
            except OSError as exc:
                assert exc.errno == errno.EOPNOTSUPP, f"unexpected errno: {exc.errno}"
            try:
                os.setxattr(off_path, "system.posix_acl_access", access_acl)
                raise AssertionError("system.posix_acl_access xattr unexpectedly enabled in acl=off mode")
            except OSError as exc:
                assert exc.errno == errno.EOPNOTSUPP, f"unexpected errno: {exc.errno}"

            print("OK xattr/selinux/acl")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
