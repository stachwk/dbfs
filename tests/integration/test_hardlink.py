#!/usr/bin/env python3

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/hardlink_{suffix}"
    source_path = f"{dir_path}/source.txt"
    linked_path = f"{dir_path}/linked.txt"
    renamed_path = f"{dir_path}/linked-renamed.txt"
    payload = b"hardlink payload\n"

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(source_path, 0o644)
        fs.write(source_path, payload, 0, fh)
        fs.flush(source_path, fh)
        fs.release(source_path, fh)

        source_stat = fs.getattr(source_path)
        fs.link(linked_path, source_path)
        linked_stat = fs.getattr(linked_path)
        assert linked_stat["st_ino"] == source_stat["st_ino"], (linked_stat, source_stat)
        assert linked_stat["st_nlink"] == 2, linked_stat
        assert fs.storage.path_has_children(fs.repository.get_dir_id(dir_path)), dir_path
        assert fs.read(linked_path, len(payload), 0, fs.open(linked_path, os.O_RDONLY)) == payload

        fs.rename(linked_path, renamed_path)
        renamed_stat = fs.getattr(renamed_path)
        assert renamed_stat["st_ino"] == source_stat["st_ino"], renamed_stat
        assert renamed_stat["st_nlink"] == 2, renamed_stat

        fs.unlink(source_path)
        remaining_stat = fs.getattr(renamed_path)
        assert remaining_stat["st_nlink"] == 1, remaining_stat
        assert fs.storage.path_has_children(fs.repository.get_dir_id(dir_path)), dir_path
        assert fs.read(renamed_path, len(payload), 0, fs.open(renamed_path, os.O_RDONLY)) == payload
        fs.unlink(renamed_path)
        fs.rmdir(dir_path)
    finally:
        try:
            fs.cleanup_resources()
        except Exception:
            pass

    print("OK hardlink/backend")


if __name__ == "__main__":
    main()
