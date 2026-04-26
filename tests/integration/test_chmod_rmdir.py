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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-chmod-rmdir-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"itest_{suffix}"
            file_path = dir_path / "mode.txt"
            payload = b"mode test"

            dir_path.mkdir()
            dir_path.chmod(0o700)
            dir_attrs = dir_path.stat()
            assert dir_attrs.st_mode & 0o777 == 0o700, f"dir chmod failed: {oct(dir_attrs.st_mode & 0o777)}"

            file_path.write_bytes(payload)
            file_path.chmod(0o600)
            file_attrs = file_path.stat()
            assert file_attrs.st_mode & 0o777 == 0o600, f"file chmod failed: {oct(file_attrs.st_mode & 0o777)}"

            file_path.unlink()
            dir_path.rmdir()

            try:
                dir_path.stat()
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("directory still exists after rmdir")

            print("OK chmod/rmdir")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
