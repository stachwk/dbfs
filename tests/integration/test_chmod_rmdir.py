#!/usr/bin/env python3

import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, FuseOSError, load_dsn_from_config


def safe_unlink(fs, path):
    kind, _ = fs.get_entry_kind_and_id(path)
    if kind == "file":
        fs.unlink(path)


def safe_rmdir(fs, path):
    kind, _ = fs.get_entry_kind_and_id(path)
    if kind == "dir":
        fs.rmdir(path)


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/itest_{suffix}"
    file_path = f"{dir_path}/mode.txt"
    payload = b"mode test"

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fs.chmod(dir_path, 0o700)
        dir_attrs = fs.getattr(dir_path)
        assert dir_attrs["st_mode"] & 0o777 == 0o700, f"dir chmod failed: {oct(dir_attrs['st_mode'] & 0o777)}"

        fh = fs.create(file_path, 0o644)
        fs.write(file_path, payload, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fs.chmod(file_path, 0o600)
        file_attrs = fs.getattr(file_path)
        assert file_attrs["st_mode"] & 0o777 == 0o600, f"file chmod failed: {oct(file_attrs['st_mode'] & 0o777)}"

        fs.unlink(file_path)
        fs.rmdir(dir_path)

        kind, _ = fs.get_entry_kind_and_id(dir_path)
        if kind is not None:
            raise AssertionError("directory still exists after rmdir")

        print("OK chmod/rmdir")
    finally:
        if fh is not None:
            try:
                fs.release(file_path, fh)
            except Exception:
                pass

        try:
            safe_unlink(fs, file_path)
        except Exception:
            pass

        try:
            safe_rmdir(fs, dir_path)
        except Exception:
            pass


if __name__ == "__main__":
    main()
