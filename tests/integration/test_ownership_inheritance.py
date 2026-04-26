#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def main() -> None:
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    current_uid = os.getuid()
    current_gid = os.getgid()
    supplementary_groups = [gid for gid in os.getgroups() if gid != current_gid]
    if not supplementary_groups:
        raise AssertionError("expected at least one supplementary group for ownership inheritance test")
    inherited_gid = supplementary_groups[0]

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-ownership-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            parent_dir = mountpoint / f"ownership_{suffix}"
            child_dir = parent_dir / "child"
            nested_dir = parent_dir / "nested" / "leaf"
            child_file = parent_dir / "child.txt"
            moved_dir_src = mountpoint / f"ownership_move_src_{suffix}"
            moved_dir_dst = parent_dir / "moved"

            parent_dir.mkdir()
            os.chown(parent_dir, current_uid, inherited_gid)
            os.chmod(parent_dir, 0o2755)

            (parent_dir / "nested").mkdir()
            nested_dir.mkdir()
            nested_parent_stat = (parent_dir / "nested").stat()
            nested_leaf_stat = nested_dir.stat()
            assert nested_parent_stat.st_gid == inherited_gid, nested_parent_stat
            assert nested_parent_stat.st_mode & 0o2000, nested_parent_stat
            assert nested_leaf_stat.st_gid == inherited_gid, nested_leaf_stat
            assert nested_leaf_stat.st_mode & 0o2000, nested_leaf_stat

            child_file.write_bytes(b"ownership\n")
            child_dir.mkdir()
            moved_dir_src.mkdir()
            os.rename(moved_dir_src, moved_dir_dst)

            parent_stat = parent_dir.stat()
            file_stat = child_file.stat()
            dir_stat = child_dir.stat()
            moved_stat = moved_dir_dst.stat()

            assert parent_stat.st_gid == inherited_gid, parent_stat
            assert file_stat.st_gid == inherited_gid, file_stat
            assert dir_stat.st_gid == inherited_gid, dir_stat
            assert moved_stat.st_gid == current_gid, moved_stat
            assert not (moved_stat.st_mode & 0o2000), moved_stat
            print("OK ownership/inheritance")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
