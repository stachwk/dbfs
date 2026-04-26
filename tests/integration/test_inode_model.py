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


def main():
    suffix = uuid.uuid4().hex[:8]
    dir_name = f"inode_{suffix}"
    file_name = f"payload.txt"
    hardlink_name = f"payload-hard.txt"
    symlink_name = f"payload-link.txt"
    payload = b"inode-model\n"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-inode-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / dir_name
            file_path = dir_path / file_name
            hardlink_path = dir_path / hardlink_name
            symlink_path = dir_path / symlink_name

            dir_path.mkdir()
            file_path.write_bytes(payload)
            os.link(file_path, hardlink_path)
            os.symlink(file_path.name, symlink_path)

            snapshot = {
                "dir": dir_path.stat().st_ino,
                "file": file_path.stat().st_ino,
                "hardlink": hardlink_path.stat().st_ino,
                "symlink": symlink_path.stat().st_ino,
            }
            assert snapshot["file"] == snapshot["hardlink"], snapshot

            launcher.stop()
            launcher.start(str(mountpoint))
            try:
                assert dir_path.stat().st_ino == snapshot["dir"], snapshot
                assert file_path.stat().st_ino == snapshot["file"], snapshot
                assert hardlink_path.stat().st_ino == snapshot["hardlink"], snapshot
                assert symlink_path.stat().st_ino == snapshot["symlink"], snapshot
                print("OK inode/model")
            finally:
                launcher.stop()
        finally:
            try:
                hardlink_path.unlink()
            except Exception:
                pass
            try:
                symlink_path.unlink()
            except Exception:
                pass
            try:
                file_path.unlink()
            except Exception:
                pass
            try:
                dir_path.rmdir()
            except Exception:
                pass


if __name__ == "__main__":
    main()
