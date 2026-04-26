#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

from tests.integration.dbfs_mount import DBFSMount


def main() -> None:
    suffix = uuid.uuid4().hex[:8]
    payload = b"buffered io payload"

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-bufio-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"bufio_{suffix}"
            file_path = dir_path / "payload.txt"

            dir_path.mkdir()
            with file_path.open("wb") as fh:
                fh.write(payload)
                fh.write(b"!")

            data = file_path.read_bytes()
            assert data == payload + b"!", data
            print("OK bufio/read/write")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
