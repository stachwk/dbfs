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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-access-groups-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"access_groups_{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"access-groups\n"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            file_path.chmod(0o640)
            dir_path.chmod(0o750)

            assert os.access(file_path, os.R_OK)
            assert os.access(file_path, os.W_OK)
            assert not os.access(file_path, os.X_OK)
            assert os.access(dir_path, os.R_OK)
            assert os.access(dir_path, os.W_OK)
            assert os.access(dir_path, os.X_OK)

            file_path.chmod(0o000)
            dir_path.chmod(0o000)

            assert not os.access(file_path, os.R_OK)
            assert not os.access(file_path, os.W_OK)
            assert not os.access(file_path, os.X_OK)
            assert not os.access(dir_path, os.R_OK)
            assert not os.access(dir_path, os.W_OK)
            assert not os.access(dir_path, os.X_OK)

            print("OK access/groups")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
