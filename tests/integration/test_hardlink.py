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

    suffix = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-hardlink-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"hardlink_{suffix}"
            source_path = dir_path / "source.txt"
            linked_path = dir_path / "linked.txt"
            renamed_path = dir_path / "linked-renamed.txt"
            payload = b"hardlink payload\n"

            dir_path.mkdir()
            source_path.write_bytes(payload)

            source_stat = source_path.stat()
            os.link(source_path, linked_path)
            linked_stat = linked_path.stat()
            assert linked_stat.st_ino == source_stat.st_ino, (linked_stat, source_stat)
            assert linked_stat.st_nlink == 2, linked_stat
            assert len(list(dir_path.iterdir())) > 0, dir_path
            assert linked_path.read_bytes() == payload

            os.rename(linked_path, renamed_path)
            renamed_stat = renamed_path.stat()
            assert renamed_stat.st_ino == source_stat.st_ino, renamed_stat
            assert renamed_stat.st_nlink == 2, renamed_stat

            source_path.unlink()
            remaining_stat = renamed_path.stat()
            assert remaining_stat.st_nlink == 1, remaining_stat
            assert len(list(dir_path.iterdir())) > 0, dir_path
            assert renamed_path.read_bytes() == payload
            renamed_path.unlink()
            dir_path.rmdir()
        finally:
            launcher.stop()

    print("OK hardlink/backend")


if __name__ == "__main__":
    main()
