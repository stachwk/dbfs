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
    current_uid = os.getuid() if hasattr(os, "getuid") else 0
    current_gid = os.getgid() if hasattr(os, "getgid") else 0
    supplementary_groups = [gid for gid in os.getgroups() if gid != current_gid]
    if not supplementary_groups:
        raise AssertionError("expected at least one supplementary group for journal chown coverage")
    alt_gid = supplementary_groups[0]

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-journal-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"journal-{suffix}"
            file_path = dir_path / "entry.txt"
            renamed_path = dir_path / "entry-renamed.txt"

            dir_path.mkdir()
            file_path.write_bytes(b"journal")
            os.rename(file_path, renamed_path)
            os.chmod(renamed_path, 0o600)
            os.chown(renamed_path, current_uid, alt_gid)
            with renamed_path.open("r+b") as fh:
                fh.truncate(3)
            renamed_path.unlink()
            dir_path.rmdir()

            print("OK journal")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
