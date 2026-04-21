#!/usr/bin/env python3

import os
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/timestamp-touch-{suffix}"
    file_path = f"{dir_path}/payload.txt"
    payload = b"touch-once\n"

    file_fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        file_fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, file_fh)
        assert written == len(payload), f"write returned {written}, expected {len(payload)}"
        fs.flush(file_path, file_fh)
        fs.release(file_path, file_fh)

        file_stat = fs.getattr(file_path)
        stale_atime = file_stat["st_atime"] - 86400
        fs.utimens(file_path, (stale_atime, file_stat["st_mtime"]))

        file_fh = fs.open(file_path, os.O_RDONLY)
        first_read = fs.read(file_path, 4, 0, file_fh)
        assert first_read == payload[:4], f"unexpected first read payload: {first_read!r}"
        after_first = fs.getattr(file_path)["st_atime"]
        time.sleep(1.1)
        second_read = fs.read(file_path, 4, 4, file_fh)
        assert second_read == payload[4:8], f"unexpected second read payload: {second_read!r}"
        after_second = fs.getattr(file_path)["st_atime"]
        assert after_second == after_first, (after_first, after_second)

        fs.release(file_path, file_fh)
        file_fh = None

        file_before = fs.getattr(file_path)
        before_mtime = file_before["st_mtime"]
        before_ctime = file_before["st_ctime"]

        file_fh = fs.open(file_path, os.O_WRONLY)
        first_chunk = b"chunk-one\n"
        second_chunk = b"chunk-two\n"
        first_written = fs.write(file_path, first_chunk, 0, file_fh)
        assert first_written == len(first_chunk), (first_written, len(first_chunk))
        time.sleep(1.1)
        second_written = fs.write(file_path, second_chunk, len(first_chunk), file_fh)
        assert second_written == len(second_chunk), (second_written, len(second_chunk))

        mid = fs.getattr(file_path)
        assert mid["st_mtime"] == before_mtime, (before_mtime, mid["st_mtime"])
        assert mid["st_ctime"] == before_ctime, (before_ctime, mid["st_ctime"])

        fs.flush(file_path, file_fh)
        after_flush = fs.getattr(file_path)
        assert after_flush["st_mtime"] >= before_mtime, (before_mtime, after_flush["st_mtime"])
        assert after_flush["st_ctime"] >= before_ctime, (before_ctime, after_flush["st_ctime"])

        fs.release(file_path, file_fh)
        file_fh = None

        dir_stat = fs.getattr(dir_path)
        stale_dir_atime = dir_stat["st_atime"] - 86400
        fs.utimens(dir_path, (stale_dir_atime, dir_stat["st_mtime"]))

        dir_fh = 424242
        first_entries = fs.readdir(dir_path, dir_fh)
        assert "." in first_entries and ".." in first_entries, first_entries
        dir_after_first = fs.getattr(dir_path)["st_atime"]
        time.sleep(1.1)
        second_entries = fs.readdir(dir_path, dir_fh)
        assert "." in second_entries and ".." in second_entries, second_entries
        dir_after_second = fs.getattr(dir_path)["st_atime"]
        assert dir_after_second == dir_after_first, (dir_after_first, dir_after_second)
        fs.releasedir(dir_path, dir_fh)

        print("OK timestamp-touch-once")
    finally:
        if file_fh is not None:
            try:
                fs.release(file_path, file_fh)
            except Exception:
                pass

        try:
            fs.unlink(file_path)
        except Exception:
            pass

        try:
            fs.rmdir(dir_path)
        except Exception:
            pass


if __name__ == "__main__":
    main()
