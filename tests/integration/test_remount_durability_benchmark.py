#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
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
    payload = (b"dbfs-remount-durability-" * 1024)[: 64 * 1024]
    file_name = f"durability-{suffix}.bin"

    mount1 = tempfile.TemporaryDirectory(prefix="/tmp/dbfs-remount-1.")
    mount2 = tempfile.TemporaryDirectory(prefix="/tmp/dbfs-remount-2.")
    try:
        launcher.start(mount1.name)
        file_path = Path(mount1.name) / file_name
        file_path.write_bytes(payload)
        launcher.stop()

        start = time.perf_counter()
        launcher.start(mount2.name)
        remount_path = Path(mount2.name) / file_name
        read_back = remount_path.read_bytes()
        elapsed = time.perf_counter() - start

        if read_back != payload:
            raise AssertionError("remount durability payload mismatch")

        print(
            f"OK remount-durability bytes={len(payload)} elapsed_s={elapsed:.6f} "
            f"mount1={mount1.name} mount2={mount2.name}"
        )
    finally:
        try:
            launcher.stop()
        except Exception:
            pass
        mount1.cleanup()
        mount2.cleanup()


if __name__ == "__main__":
    main()
