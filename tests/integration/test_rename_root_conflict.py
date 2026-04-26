#!/usr/bin/env python3

from __future__ import annotations

import errno
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
    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-rename-root-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            source = mountpoint / f"rename_{suffix}_a.txt"
            occupied = mountpoint / f"rename_{suffix}_b.txt"
            target = mountpoint / f"rename_{suffix}_c.txt"
            payload = b"rename root"
            occupied_payload = b"occupied root"
            cross_parent_src_dir = mountpoint / f"rename_{suffix}_cross_src"
            cross_parent_dst_dir = mountpoint / f"rename_{suffix}_cross_dst"
            cross_parent_src = cross_parent_src_dir / "source.txt"
            cross_parent_dst = cross_parent_dst_dir / "target.txt"
            source_dir = mountpoint / f"rename_{suffix}_dir_a"
            occupied_dir = mountpoint / f"rename_{suffix}_dir_b"
            cycle_dir = mountpoint / f"rename_{suffix}_cycle"
            cycle_child = cycle_dir / "child"

            source.write_bytes(payload)
            occupied.write_bytes(occupied_payload)

            os.rename(source, target)
            assert target.read_bytes() == payload, "rename root read mismatch"

            os.rename(target, occupied)
            assert occupied.read_bytes() == payload, "rename replace mismatch"
            try:
                source.open("rb")
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("old path still exists after replace rename")

            cross_parent_src_dir.mkdir()
            cross_parent_dst_dir.mkdir()
            cross_parent_src.write_bytes(b"cross-parent")
            os.rename(cross_parent_src, cross_parent_dst)
            assert cross_parent_dst.read_bytes() == b"cross-parent"
            try:
                cross_parent_src.open("rb")
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("cross-parent old path still exists after rename")

            source_dir.mkdir()
            occupied_dir.mkdir()
            os.rename(source_dir, occupied_dir)
            assert occupied_dir.is_dir()
            try:
                source_dir.stat()
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("old directory path still exists after replace rename")

            cycle_dir.mkdir()
            cycle_child.mkdir()
            try:
                os.rename(cycle_dir, cycle_child / "inner")
            except OSError as exc:
                assert exc.errno in {errno.EINVAL, errno.EBUSY}, exc
            else:
                raise AssertionError("rename into descendant did not fail")

            try:
                os.rename(source, mountpoint)
            except OSError as exc:
                assert exc.errno == errno.EPERM, exc
            else:
                raise AssertionError("rename to root did not fail")

            try:
                os.rename(mountpoint, occupied)
            except OSError as exc:
                assert exc.errno == errno.EPERM, exc
            else:
                raise AssertionError("rename from root did not fail")

            print("OK rename/root-conflict")
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
