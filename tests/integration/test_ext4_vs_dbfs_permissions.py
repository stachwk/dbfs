#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def _fs_type(path: Path) -> str:
    return subprocess.check_output(["stat", "-f", "-c", "%T", str(path)], text=True).strip()


def _run_permission_matrix(root: Path) -> dict[str, object]:
    suffix = uuid.uuid4().hex[:8]
    dir_path = root / f"perm-compare-{suffix}"
    file_path = dir_path / "data.txt"
    renamed_path = dir_path / "data-renamed.txt"
    payload = b"payload\n"

    dir_path.mkdir()
    file_path.write_bytes(payload)
    file_path.chmod(0o640)
    dir_path.chmod(0o750)

    file_stat = file_path.stat()
    dir_stat = dir_path.stat()

    observed = {
        "fs_type": _fs_type(root),
        "file_mode": oct(file_stat.st_mode & 0o777),
        "dir_mode": oct(dir_stat.st_mode & 0o777),
        "file_uid": file_stat.st_uid,
        "file_gid": file_stat.st_gid,
        "dir_uid": dir_stat.st_uid,
        "dir_gid": dir_stat.st_gid,
        "file_size": file_stat.st_size,
        "dir_size": dir_stat.st_size,
        "file_access_r": os.access(file_path, os.R_OK),
        "file_access_w": os.access(file_path, os.W_OK),
        "file_access_x": os.access(file_path, os.X_OK),
        "dir_access_r": os.access(dir_path, os.R_OK),
        "dir_access_w": os.access(dir_path, os.W_OK),
        "dir_access_x": os.access(dir_path, os.X_OK),
    }

    file_path.chmod(0o000)
    observed.update(
        {
            "file_access_r_after_chmod": os.access(file_path, os.R_OK),
            "file_access_w_after_chmod": os.access(file_path, os.W_OK),
            "file_access_x_after_chmod": os.access(file_path, os.X_OK),
        }
    )

    file_path.rename(renamed_path)
    renamed_stat = renamed_path.stat()
    observed.update(
        {
            "renamed_mode": oct(renamed_stat.st_mode & 0o777),
            "renamed_uid": renamed_stat.st_uid,
            "renamed_gid": renamed_stat.st_gid,
            "renamed_size": renamed_stat.st_size,
        }
    )

    renamed_path.unlink()
    dir_path.rmdir()
    return observed


def main() -> None:
    repo_root = ROOT
    baseline_base = Path("/home/wojtek")
    if not baseline_base.exists() or not os.access(baseline_base, os.W_OK | os.X_OK):
        baseline_base = repo_root
    local_root = Path(tempfile.mkdtemp(prefix=f"dbfs-ext4-compare-{uuid.uuid4().hex[:8]}.", dir=str(baseline_base)))
    mount_tmp = tempfile.TemporaryDirectory(prefix="/tmp/dbfs-ext4-compare.mount.")
    mountpoint = Path(mount_tmp.name)
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    try:
        launcher.start(str(mountpoint))
        local_result = _run_permission_matrix(local_root)
        dbfs_result = _run_permission_matrix(mountpoint)

        comparable_keys = [
            "file_mode",
            "dir_mode",
            "file_uid",
            "file_gid",
            "dir_uid",
            "dir_gid",
            "file_size",
            "file_access_r",
            "file_access_w",
            "file_access_x",
            "dir_access_r",
            "dir_access_w",
            "dir_access_x",
            "file_access_r_after_chmod",
            "file_access_w_after_chmod",
            "file_access_x_after_chmod",
            "renamed_mode",
            "renamed_uid",
            "renamed_gid",
            "renamed_size",
        ]

        for key in comparable_keys:
            assert local_result[key] == dbfs_result[key], (key, local_result[key], dbfs_result[key])

        print(
            "OK ext4-vs-dbfs-permissions",
            f"local_fs={local_result['fs_type']}",
            f"dbfs_fs={dbfs_result['fs_type']}",
        )
    finally:
        launcher.stop()
        shutil.rmtree(local_root, ignore_errors=True)
        mount_tmp.cleanup()


if __name__ == "__main__":
    main()
