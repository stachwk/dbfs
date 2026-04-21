#!/usr/bin/env python3

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config, role="auto")

    try:
        assert fs.requested_role == "auto", f"requested role mismatch: {fs.requested_role}"
        assert fs.role == "primary", f"expected primary on local PostgreSQL, got {fs.role}"
        assert not fs.read_only, "local primary should not mount read-only"
        print("OK role/autodetect")
    finally:
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
