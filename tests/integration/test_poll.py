#!/usr/bin/env python3

from __future__ import annotations

import os
import select
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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-poll-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        replica_mountpoint = Path(tempfile.mkdtemp(prefix=f"/tmp/dbfs-poll-replica-{suffix}."))
        launcher.start(str(mountpoint))
        replica = DBFSMount(str(ROOT), role="replica")
        replica.start(str(replica_mountpoint))
        try:
            dir_path = mountpoint / f"poll_{suffix}"
            file_path = dir_path / "payload.txt"
            payload = b"poll payload"

            dir_path.mkdir()
            file_path.write_bytes(payload)

            fd = os.open(file_path, os.O_RDWR)
            try:
                poller = select.poll()
                poller.register(fd, select.POLLIN | select.POLLOUT)
                events = dict(poller.poll(0))
                mask = events.get(fd, 0)
                assert mask & select.POLLIN, mask
                assert mask & select.POLLOUT, mask
            finally:
                os.close(fd)

            replica_path = replica_mountpoint / f"poll_{suffix}" / "payload.txt"
            replica_fd = os.open(replica_path, os.O_RDONLY)
            try:
                poller = select.poll()
                poller.register(replica_fd, select.POLLIN | select.POLLOUT)
                events = dict(poller.poll(0))
                replica_mask = events.get(replica_fd, 0)
                assert replica_mask & select.POLLIN, replica_mask
                assert not (replica_mask & select.POLLOUT), replica_mask
            finally:
                os.close(replica_fd)

            print("OK poll/mount")
        finally:
            replica.stop()
            launcher.stop()


if __name__ == "__main__":
    main()
