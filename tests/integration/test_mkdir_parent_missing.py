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
    missing_parent = f"/missing-parent-{suffix}"
    nested_dir = f"{missing_parent}/child"

    try:
        try:
            fs.mkdir(nested_dir, 0o755)
        except FuseOSError as exc:
            assert exc.errno == errno.ENOENT, f"expected ENOENT for missing parent, got {exc.errno}"
        else:
            raise AssertionError("mkdir unexpectedly created missing parents")

        kind, entry_id = fs.repository.get_entry_kind_and_id(missing_parent)
        assert kind is None and entry_id is None, "missing parent should not have been created"
        print("OK mkdir/parent-missing")
    finally:
        try:
            fs.rmdir(nested_dir)
        except Exception:
            pass
        try:
            fs.rmdir(missing_parent)
        except Exception:
            pass


if __name__ == "__main__":
    main()
