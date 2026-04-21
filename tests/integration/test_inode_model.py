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
    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/inode_{suffix}"
    file_path = f"{dir_path}/payload.txt"
    hardlink_path = f"{dir_path}/payload-hard.txt"
    symlink_path = f"{dir_path}/payload-link.txt"
    payload = b"inode-model\n"

    fs = DBFS(dsn, db_config)
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fs.link(hardlink_path, file_path)
        fs.symlink(symlink_path, file_path)

        snapshot = {
            "dir": fs.getattr(dir_path)["st_ino"],
            "file": fs.getattr(file_path)["st_ino"],
            "hardlink": fs.getattr(hardlink_path)["st_ino"],
            "symlink": fs.getattr(symlink_path)["st_ino"],
        }
        assert snapshot["file"] == snapshot["hardlink"], snapshot

        fs.connection_pool.closeall()

        fs2 = DBFS(dsn, db_config)
        try:
            assert fs2.getattr(dir_path)["st_ino"] == snapshot["dir"], snapshot
            assert fs2.getattr(file_path)["st_ino"] == snapshot["file"], snapshot
            assert fs2.getattr(hardlink_path)["st_ino"] == snapshot["hardlink"], snapshot
            assert fs2.getattr(symlink_path)["st_ino"] == snapshot["symlink"], snapshot
            print("OK inode/model")
        finally:
            try:
                fs2.unlink(hardlink_path)
            except Exception:
                pass
            try:
                fs2.unlink(symlink_path)
            except Exception:
                pass
            try:
                fs2.unlink(file_path)
            except Exception:
                pass
            try:
                fs2.rmdir(dir_path)
            except Exception:
                pass
            try:
                fs2.connection_pool.closeall()
            except Exception:
                pass
    finally:
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
