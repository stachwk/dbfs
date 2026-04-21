#!/usr/bin/env python3

from __future__ import annotations

import errno
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)
    current_uid = os.getuid()
    current_gid = os.getgid()
    test_uid = current_uid + 4242
    test_gid = current_gid + 4242
    other_uid = test_uid + 1
    other_gid = test_gid + 1
    third_uid = other_uid + 1
    third_gid = other_gid + 1
    fs.current_uid_gid = lambda: (test_uid, test_gid)

    suffix = uuid.uuid4().hex[:8]
    sticky_dir = f"/sticky-{suffix}"
    sticky_file = f"{sticky_dir}/payload.txt"
    sticky_subdir = f"{sticky_dir}/nested"

    suid_dir = f"/suid-{suffix}"
    suid_file = f"{suid_dir}/suid.txt"
    suid_link = f"{suid_dir}/suid-link"
    group_file = f"{suid_dir}/group.txt"
    group_dir = f"{suid_dir}/groupdir"
    alt_gid = test_gid + 1000

    try:
        fs.mkdir(sticky_dir, 0o1777)
        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_dir, other_uid, other_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        fs.mkdir(sticky_subdir, 0o755)
        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_subdir, other_uid, other_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        fh = fs.create(sticky_file, 0o644)
        fs.write(sticky_file, b"sticky\n", 0, fh)
        fs.flush(sticky_file, fh)
        fs.release(sticky_file, fh)
        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_file, other_uid, other_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        try:
            fs.chmod(sticky_file, 0o600)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected chmod to fail for non-owner")

        try:
            fs.chown(sticky_file, test_uid, test_gid)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected chown to fail for non-owner")

        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_file, test_uid, test_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        fs.current_uid_gid = lambda: (third_uid, third_gid)
        try:
            fs.unlink(sticky_file)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected sticky-bit unlink to fail for non-owner")
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_file, test_uid, test_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)
        fs.unlink(sticky_file)

        fs.current_uid_gid = lambda: (third_uid, third_gid)
        try:
            fs.rmdir(sticky_subdir)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected sticky-bit rmdir to fail for non-owner")
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)

        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_subdir, test_uid, test_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)
        fs.rmdir(sticky_subdir)
        fs.current_uid_gid = lambda: (0, 0)
        try:
            fs.chown(sticky_dir, test_uid, test_gid)
        finally:
            fs.current_uid_gid = lambda: (test_uid, test_gid)
        fs.rmdir(sticky_dir)

        fs.current_group_ids = lambda: {test_gid, alt_gid}
        fs.mkdir(suid_dir, 0o755)
        fh = fs.create(suid_file, 0o6755)
        fs.write(suid_file, b"suid\n", 0, fh)
        fs.flush(suid_file, fh)
        fs.release(suid_file, fh)
        before = fs.getattr(suid_file)["st_mode"]
        assert before & 0o6000 == 0o6000, oct(before)

        fs.chown(suid_file, test_uid, alt_gid)
        after = fs.getattr(suid_file)["st_mode"]
        assert after & 0o6000 == 0, oct(after)

        group_fh = fs.create(group_file, 0o640)
        fs.release(group_file, group_fh)
        fs.chown(group_file, -1, alt_gid)
        group_stat = fs.getattr(group_file)
        assert group_stat["st_gid"] == alt_gid, group_stat
        group_ctime_before = group_stat["st_ctime"]
        group_mode_before = group_stat["st_mode"]
        fs.chown(group_file, -1, alt_gid)
        group_stat_same = fs.getattr(group_file)
        assert group_stat_same["st_gid"] == alt_gid, group_stat_same
        assert group_stat_same["st_ctime"] == group_ctime_before, (group_ctime_before, group_stat_same["st_ctime"])
        assert group_stat_same["st_mode"] == group_mode_before, (group_mode_before, group_stat_same["st_mode"])
        fs.chown(group_file, test_uid, alt_gid)
        group_stat_same_all = fs.getattr(group_file)
        assert group_stat_same_all["st_uid"] == test_uid, group_stat_same_all
        assert group_stat_same_all["st_gid"] == alt_gid, group_stat_same_all
        assert group_stat_same_all["st_ctime"] == group_ctime_before, (group_ctime_before, group_stat_same_all["st_ctime"])
        assert group_stat_same_all["st_mode"] == group_mode_before, (group_mode_before, group_stat_same_all["st_mode"])
        fs.chmod(group_file, group_mode_before & 0o7777)
        group_stat_chmod_same = fs.getattr(group_file)
        assert group_stat_chmod_same["st_mode"] == group_mode_before, (group_mode_before, group_stat_chmod_same["st_mode"])
        assert group_stat_chmod_same["st_ctime"] == group_ctime_before, (group_ctime_before, group_stat_chmod_same["st_ctime"])
        fs.chown(group_file, -1, -1)
        group_ctime_after = fs.getattr(group_file)["st_ctime"]
        assert group_ctime_after == group_ctime_before, (group_ctime_before, group_ctime_after)
        try:
            fs.chown(group_file, -1, other_gid)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected chown to reject unrelated group")

        fs.mkdir(group_dir, 0o2755)
        group_dir_before = fs.getattr(group_dir)
        assert group_dir_before["st_mode"] & 0o2000, group_dir_before
        fs.chown(group_dir, -1, alt_gid)
        group_dir_after = fs.getattr(group_dir)
        assert group_dir_after["st_gid"] == alt_gid, group_dir_after
        assert group_dir_after["st_mode"] & 0o2000, group_dir_after
        group_dir_ctime_before = group_dir_after["st_ctime"]
        group_dir_mode_before = group_dir_after["st_mode"]
        fs.chown(group_dir, test_uid, alt_gid)
        group_dir_same_all = fs.getattr(group_dir)
        assert group_dir_same_all["st_uid"] == test_uid, group_dir_same_all
        assert group_dir_same_all["st_gid"] == alt_gid, group_dir_same_all
        assert group_dir_same_all["st_ctime"] == group_dir_ctime_before, (group_dir_ctime_before, group_dir_same_all["st_ctime"])
        assert group_dir_same_all["st_mode"] == group_dir_mode_before, (group_dir_mode_before, group_dir_same_all["st_mode"])
        fs.chown(group_dir, -1, alt_gid)
        group_dir_same = fs.getattr(group_dir)
        assert group_dir_same["st_gid"] == alt_gid, group_dir_same
        assert group_dir_same["st_ctime"] == group_dir_ctime_before, (group_dir_ctime_before, group_dir_same["st_ctime"])
        assert group_dir_same["st_mode"] == group_dir_mode_before, (group_dir_mode_before, group_dir_same["st_mode"])
        fs.chown(group_dir, -1, -1)
        group_dir_noop = fs.getattr(group_dir)
        assert group_dir_noop["st_uid"] == test_uid, group_dir_noop
        assert group_dir_noop["st_gid"] == alt_gid, group_dir_noop
        assert group_dir_noop["st_ctime"] == group_dir_ctime_before, (group_dir_ctime_before, group_dir_noop["st_ctime"])
        assert group_dir_noop["st_mode"] == group_dir_mode_before, (group_dir_mode_before, group_dir_noop["st_mode"])
        fs.chmod(group_dir, group_dir_mode_before & 0o7777)
        group_dir_chmod_same = fs.getattr(group_dir)
        assert group_dir_chmod_same["st_mode"] == group_dir_mode_before, (group_dir_mode_before, group_dir_chmod_same["st_mode"])
        assert group_dir_chmod_same["st_ctime"] == group_dir_ctime_before, (group_dir_ctime_before, group_dir_chmod_same["st_ctime"])

        suid_subdir = f"{suid_dir}/suiddir"
        fs.mkdir(suid_subdir, 0o6755)
        suid_dir_before = fs.getattr(suid_subdir)
        assert suid_dir_before["st_mode"] & 0o6000 == 0o6000, suid_dir_before
        fs.chown(suid_subdir, -1, alt_gid)
        suid_dir_after = fs.getattr(suid_subdir)
        assert suid_dir_after["st_gid"] == alt_gid, suid_dir_after
        assert suid_dir_after["st_mode"] & 0o4000 == 0, suid_dir_after
        assert suid_dir_after["st_mode"] & 0o2000 == 0o2000, suid_dir_after

        fs.symlink(suid_link, suid_file)
        fs.current_uid_gid = lambda: (0, 0)
        fs.chown(suid_link, other_uid, other_gid)
        symlink_stat = fs.getattr(suid_link)
        assert symlink_stat["st_uid"] == other_uid, symlink_stat
        assert symlink_stat["st_gid"] == other_gid, symlink_stat
        fs.current_uid_gid = lambda: (test_uid, test_gid)

        try:
            fs.chmod(suid_link, 0o777)
        except Exception as exc:
            assert getattr(exc, "errno", None) == errno.EPERM, exc
        else:
            raise AssertionError("expected chmod on symlink to fail")

        fs.unlink(suid_link)
        fs.unlink(suid_file)
        fs.unlink(group_file)
        fs.rmdir(group_dir)
        fs.rmdir(suid_subdir)
        fs.rmdir(suid_dir)

        print("OK permissions/sticky/chown")
    finally:
        for path in (group_dir, group_file, suid_subdir, suid_link, suid_file, suid_dir, sticky_file, sticky_subdir, sticky_dir):
            try:
                if path.endswith("/"):
                    continue
                if path.endswith(".txt"):
                    fs.unlink(path)
                else:
                    fs.rmdir(path)
            except Exception:
                pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
