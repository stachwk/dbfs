#!/usr/bin/env python3

import errno
import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, FuseOSError, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    source = f"/rename_{suffix}_a.txt"
    occupied = f"/rename_{suffix}_b.txt"
    target = f"/rename_{suffix}_c.txt"
    payload = b"rename root"
    occupied_payload = b"occupied root"
    cross_parent_src_dir = f"/rename_{suffix}_cross_src"
    cross_parent_dst_dir = f"/rename_{suffix}_cross_dst"
    cross_parent_src = f"{cross_parent_src_dir}/source.txt"
    cross_parent_dst = f"{cross_parent_dst_dir}/target.txt"
    source_dir = f"/rename_{suffix}_dir_a"
    occupied_dir = f"/rename_{suffix}_dir_b"
    cycle_dir = f"/rename_{suffix}_cycle"
    cycle_child = f"{cycle_dir}/child"

    fh_source = None
    fh_occupied = None
    try:
        fh_source = fs.create(source, 0o644)
        fs.write(source, payload, 0, fh_source)
        fs.flush(source, fh_source)
        fs.release(source, fh_source)
        fh_source = None

        fh_occupied = fs.create(occupied, 0o644)
        fs.write(occupied, occupied_payload, 0, fh_occupied)
        fs.flush(occupied, fh_occupied)
        fs.release(occupied, fh_occupied)
        fh_occupied = None

        fs.rename(source, target)

        fh = fs.open(target, 0)
        data = fs.read(target, len(payload), 0, fh)
        assert data == payload, f"rename root read returned {data!r}, expected {payload!r}"
        fs.release(target, fh)

        fs.rename(target, occupied)
        fh = fs.open(occupied, 0)
        data = fs.read(occupied, len(payload), 0, fh)
        assert data == payload, f"rename replace returned {data!r}, expected {payload!r}"
        fs.release(occupied, fh)
        try:
            fs.open(target, 0)
        except FuseOSError as exc:
            assert exc.errno == errno.ENOENT, f"expected ENOENT for old path, got {exc.errno}"
        else:
            raise AssertionError("old path still exists after replace rename")

        fs.mkdir(cross_parent_src_dir, 0o755)
        fs.mkdir(cross_parent_dst_dir, 0o755)
        fh_cross = fs.create(cross_parent_src, 0o644)
        fs.write(cross_parent_src, b"cross-parent", 0, fh_cross)
        fs.flush(cross_parent_src, fh_cross)
        fs.release(cross_parent_src, fh_cross)
        fs.rename(cross_parent_src, cross_parent_dst)
        fh = fs.open(cross_parent_dst, 0)
        data = fs.read(cross_parent_dst, len(b"cross-parent"), 0, fh)
        assert data == b"cross-parent", data
        fs.release(cross_parent_dst, fh)
        try:
            fs.open(cross_parent_src, 0)
        except FuseOSError as exc:
            assert exc.errno == errno.ENOENT, f"expected ENOENT for old cross-parent path, got {exc.errno}"
        else:
            raise AssertionError("cross-parent old path still exists after rename")

        fs.mkdir(source_dir, 0o755)
        fs.mkdir(occupied_dir, 0o755)
        fs.rename(source_dir, occupied_dir)
        assert fs.getattr(occupied_dir)["st_mode"] & 0o170000 == 0o040000
        try:
            fs.getattr(source_dir)
        except FuseOSError as exc:
            assert exc.errno == errno.ENOENT, f"expected ENOENT for renamed dir source, got {exc.errno}"
        else:
            raise AssertionError("old directory path still exists after replace rename")

        fs.mkdir(cycle_dir, 0o755)
        fs.mkdir(cycle_child, 0o755)
        try:
            fs.rename(cycle_dir, f"{cycle_child}/inner")
        except FuseOSError as exc:
            assert exc.errno in {errno.EINVAL, errno.EBUSY}, f"expected EINVAL/EBUSY, got {exc.errno}"
        else:
            raise AssertionError("rename into descendant did not fail")

        try:
            fs.rename(source, "/")
        except FuseOSError as exc:
            assert exc.errno == errno.EPERM, f"expected EPERM for rename to root, got {exc.errno}"
        else:
            raise AssertionError("rename to root did not fail")

        try:
            fs.rename("/", occupied)
        except FuseOSError as exc:
            assert exc.errno == errno.EPERM, f"expected EPERM for rename from root, got {exc.errno}"
        else:
            raise AssertionError("rename from root did not fail")

        print("OK rename/root-conflict")
    finally:
        for path in (
            source,
            occupied,
            target,
            cross_parent_src,
            cross_parent_dst,
            cross_parent_src_dir,
            cross_parent_dst_dir,
            source_dir,
            occupied_dir,
            cycle_child,
            cycle_dir,
        ):
            try:
                fs.unlink(path)
            except Exception:
                pass
        for path in (
            cross_parent_dst_dir,
            cross_parent_src_dir,
            occupied_dir,
            source_dir,
            cycle_dir,
        ):
            try:
                fs.rmdir(path)
            except Exception:
                pass


if __name__ == "__main__":
    main()
