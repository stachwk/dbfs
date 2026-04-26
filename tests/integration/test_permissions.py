#!/usr/bin/env python3

from __future__ import annotations

import errno
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def _sudo(*args: str) -> None:
    subprocess.run(["sudo", "-n", *args], check=True)


def main() -> None:
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    current_uid = os.getuid()
    current_gid = os.getgid()
    groups = [gid for gid in os.getgroups() if gid != current_gid]
    if not groups:
        raise AssertionError("expected at least one supplementary group")
    alt_gid = groups[0]

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-permissions-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            sticky_dir = mountpoint / f"sticky-{suffix}"
            sticky_file = sticky_dir / "payload.txt"
            sticky_subdir = sticky_dir / "nested"

            suid_dir = mountpoint / f"suid-{suffix}"
            suid_file = suid_dir / "suid.txt"
            suid_link = suid_dir / "suid-link"
            group_file = suid_dir / "group.txt"
            group_dir = suid_dir / "groupdir"

            sticky_dir.mkdir()
            os.chmod(sticky_dir, 0o1777)
            sticky_subdir.mkdir()
            sticky_file.write_text("sticky\n", encoding="utf-8")

            try:
                _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_dir))
                _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_subdir))
                _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_file))
            except subprocess.CalledProcessError as exc:
                raise AssertionError(("sudo chown failed", exc.returncode)) from exc

            try:
                _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_file))
                _sudo("chmod", "0600", str(sticky_file))
                _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_file))
            except subprocess.CalledProcessError as exc:
                raise AssertionError(("root chmod/chown failed", exc.returncode)) from exc

            unlink_attempt = subprocess.run(["sudo", "-n", "-u", "nobody", "rm", "-f", str(sticky_file)], check=False)
            assert unlink_attempt.returncode != 0, "expected sticky-bit unlink by other user to fail"

            rmdir_attempt = subprocess.run(["sudo", "-n", "-u", "nobody", "rmdir", str(sticky_subdir)], check=False)
            assert rmdir_attempt.returncode != 0, "expected sticky-bit rmdir by other user to fail"

            _sudo("chown", f"{current_uid}:{current_gid}", str(sticky_file))
            sticky_file.unlink()
            sticky_subdir.rmdir()
            os.chmod(sticky_dir, 0o755)
            sticky_dir.rmdir()

            suid_dir.mkdir()
            os.chmod(suid_dir, 0o755)
            suid_file.write_text("suid\n", encoding="utf-8")
            os.chmod(suid_file, 0o6755)
            before = suid_file.stat().st_mode
            assert before & 0o6000 == 0o6000, oct(before)

            _sudo("chown", f"{current_uid}:{alt_gid}", str(suid_file))
            after = suid_file.stat().st_mode
            assert after & 0o6000 == 0, oct(after)

            group_file.write_text("group\n", encoding="utf-8")
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_file))
            group_stat = group_file.stat()
            assert group_stat.st_gid == alt_gid, group_stat
            group_ctime_before = group_stat.st_ctime
            group_mode_before = group_stat.st_mode
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_file))
            group_stat_same = group_file.stat()
            assert group_stat_same.st_gid == alt_gid, group_stat_same
            assert group_stat_same.st_ctime == group_ctime_before, (group_ctime_before, group_stat_same.st_ctime)
            assert group_stat_same.st_mode == group_mode_before, (group_mode_before, group_stat_same.st_mode)
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_file))
            group_stat_same_all = group_file.stat()
            assert group_stat_same_all.st_uid == current_uid, group_stat_same_all
            assert group_stat_same_all.st_gid == alt_gid, group_stat_same_all
            assert group_stat_same_all.st_ctime == group_ctime_before, (group_ctime_before, group_stat_same_all.st_ctime)
            assert group_stat_same_all.st_mode == group_mode_before, (group_mode_before, group_stat_same_all.st_mode)
            os.chmod(group_file, group_mode_before & 0o7777)
            group_stat_chmod_same = group_file.stat()
            assert group_stat_chmod_same.st_mode == group_mode_before, (group_mode_before, group_stat_chmod_same.st_mode)
            assert group_stat_chmod_same.st_ctime == group_ctime_before, (group_ctime_before, group_stat_chmod_same.st_ctime)
            group_ctime_after = group_file.stat().st_ctime
            assert group_ctime_after == group_ctime_before, (group_ctime_before, group_ctime_after)

            group_dir.mkdir()
            os.chmod(group_dir, 0o2755)
            group_dir_before = group_dir.stat()
            assert group_dir_before.st_mode & 0o2000, group_dir_before
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_dir))
            group_dir_after = group_dir.stat()
            assert group_dir_after.st_gid == alt_gid, group_dir_after
            assert group_dir_after.st_mode & 0o2000, group_dir_after
            group_dir_ctime_before = group_dir_after.st_ctime
            group_dir_mode_before = group_dir_after.st_mode
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_dir))
            group_dir_same_all = group_dir.stat()
            assert group_dir_same_all.st_uid == current_uid, group_dir_same_all
            assert group_dir_same_all.st_gid == alt_gid, group_dir_same_all
            assert group_dir_same_all.st_ctime == group_dir_ctime_before, (group_dir_ctime_before, group_dir_same_all.st_ctime)
            assert group_dir_same_all.st_mode == group_dir_mode_before, (group_dir_mode_before, group_dir_same_all.st_mode)
            _sudo("chown", f"{current_uid}:{alt_gid}", str(group_dir))
            group_dir_same = group_dir.stat()
            assert group_dir_same.st_gid == alt_gid, group_dir_same
            assert group_dir_same.st_ctime == group_dir_ctime_before, (group_dir_ctime_before, group_dir_same.st_ctime)
            assert group_dir_same.st_mode == group_dir_mode_before, (group_dir_mode_before, group_dir_same.st_mode)
            group_dir_noop = group_dir.stat()
            assert group_dir_noop.st_uid == current_uid, group_dir_noop
            assert group_dir_noop.st_gid == alt_gid, group_dir_noop
            assert group_dir_noop.st_ctime == group_dir_ctime_before, (group_dir_ctime_before, group_dir_noop.st_ctime)
            assert group_dir_noop.st_mode == group_dir_mode_before, (group_mode_before, group_dir_noop.st_mode)

            suid_subdir = suid_dir / "suiddir"
            suid_subdir.mkdir()
            os.chmod(suid_subdir, 0o6755)
            suid_dir_before = suid_subdir.stat()
            assert suid_dir_before.st_mode & 0o6000 == 0o6000, suid_dir_before
            _sudo("chown", f"{current_uid}:{alt_gid}", str(suid_subdir))
            suid_dir_after = suid_subdir.stat()
            assert suid_dir_after.st_gid == alt_gid, suid_dir_after
            assert suid_dir_after.st_mode & 0o4000 == 0, suid_dir_after
            assert suid_dir_after.st_mode & 0o2000 == 0o2000, suid_dir_after

            suid_link.symlink_to(suid_file)
            _sudo("chown", f"{current_uid}:{alt_gid}", str(suid_link))
            symlink_stat = suid_link.lstat()
            assert symlink_stat.st_uid == current_uid, symlink_stat
            assert symlink_stat.st_gid == alt_gid, symlink_stat

            try:
                os.chmod(suid_link, 0o777, follow_symlinks=False)
            except (NotImplementedError, PermissionError, OSError):
                pass

            suid_link.unlink()
            suid_file.unlink()
            group_file.unlink()
            group_dir.rmdir()
            suid_subdir.rmdir()
            suid_dir.rmdir()

            print("OK permissions/sticky/chown")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
