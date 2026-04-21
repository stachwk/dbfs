#!/usr/bin/env python3

import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    parent_dir = f"/ownership_{suffix}"
    child_dir = f"{parent_dir}/child"
    nested_dir = f"{parent_dir}/nested/leaf"
    child_file = f"{parent_dir}/child.txt"
    moved_dir_src = f"/ownership_move_src_{suffix}"
    moved_dir_dst = f"{parent_dir}/moved"

    current_uid = os.getuid()
    current_gid = os.getgid()
    supplementary_groups = [gid for gid in os.getgroups() if gid != current_gid]
    if not supplementary_groups:
        raise AssertionError("expected at least one supplementary group for ownership inheritance test")
    inherited_gid = supplementary_groups[0]

    try:
        fs.mkdir(parent_dir, 0o755)
        fs.chown(parent_dir, current_uid, inherited_gid)
        fs.chmod(parent_dir, 0o2755)

        fs.mkdir(f"{parent_dir}/nested", 0o755)
        fs.mkdir(nested_dir, 0o755)
        nested_parent_stat = fs.getattr(f"{parent_dir}/nested")
        nested_leaf_stat = fs.getattr(nested_dir)
        assert nested_parent_stat["st_gid"] == inherited_gid, nested_parent_stat
        assert nested_parent_stat["st_mode"] & 0o2000, nested_parent_stat
        assert nested_leaf_stat["st_gid"] == inherited_gid, nested_leaf_stat
        assert nested_leaf_stat["st_mode"] & 0o2000, nested_leaf_stat

        file_fh = fs.create(child_file, 0o644)
        fs.release(child_file, file_fh)
        fs.mkdir(child_dir, 0o755)
        fs.mkdir(moved_dir_src, 0o755)
        fs.rename(moved_dir_src, moved_dir_dst)

        parent_stat = fs.getattr(parent_dir)
        file_stat = fs.getattr(child_file)
        dir_stat = fs.getattr(child_dir)
        moved_stat = fs.getattr(moved_dir_dst)

        assert parent_stat["st_gid"] == inherited_gid, parent_stat
        assert file_stat["st_gid"] == inherited_gid, file_stat
        assert dir_stat["st_gid"] == inherited_gid, dir_stat
        assert moved_stat["st_gid"] == current_gid, moved_stat
        assert not (moved_stat["st_mode"] & 0o2000), moved_stat
        print("OK ownership/inheritance")
    finally:
        try:
            fs.unlink(child_file)
        except Exception:
            pass
        try:
            fs.rmdir(moved_dir_dst)
        except Exception:
            pass
        try:
            fs.rmdir(moved_dir_src)
        except Exception:
            pass
        try:
            fs.rmdir(child_dir)
        except Exception:
            pass
        try:
            fs.rmdir(f"{parent_dir}/nested")
        except Exception:
            pass
        try:
            fs.rmdir(parent_dir)
        except Exception:
            pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
