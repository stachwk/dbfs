#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


def main():
    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-touch-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"timestamp-touch-{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"touch-once\n"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            file_stat = file_path.stat()
            stale_atime = file_stat.st_atime - 86400
            os.utime(file_path, (stale_atime, file_stat.st_mtime))

            first_read = file_path.read_bytes()[:4]
            assert first_read == payload[:4], f"unexpected first read payload: {first_read!r}"
            after_first = file_path.stat().st_atime
            time.sleep(1.1)
            second_read = file_path.read_bytes()[4:8]
            assert second_read == payload[4:8], f"unexpected second read payload: {second_read!r}"
            after_second = file_path.stat().st_atime
            assert after_second == after_first, (after_first, after_second)

            dir_stat = dir_path.stat()
            stale_dir_atime = dir_stat.st_atime - 86400
            os.utime(dir_path, (stale_dir_atime, dir_stat.st_mtime))

            first_entries = sorted(os.listdir(dir_path))
            assert first_entries == ["payload.txt"], first_entries
            dir_after_first = dir_path.stat().st_atime
            time.sleep(1.1)
            second_entries = sorted(os.listdir(dir_path))
            assert second_entries == ["payload.txt"], second_entries
            dir_after_second = dir_path.stat().st_atime
            assert dir_after_second == dir_after_first, (dir_after_first, dir_after_second)

            print("OK timestamp-touch-once")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
