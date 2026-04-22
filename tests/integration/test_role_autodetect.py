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
    replica_fs = None

    try:
        assert fs.requested_role == "auto", f"requested role mismatch: {fs.requested_role}"
        assert fs.role == "primary", f"expected primary on local PostgreSQL, got {fs.role}"
        assert not fs.read_only, "local primary should not mount read-only"

        replica_fs = DBFS(dsn, db_config, role="replica")
        assert replica_fs.requested_role == "replica", f"requested role mismatch: {replica_fs.requested_role}"
        assert replica_fs.role == "replica", f"expected replica role, got {replica_fs.role}"
        assert replica_fs.read_only, "replica mount should be read-only"
        assert replica_fs.lock_backend == "memory", replica_fs.lock_backend
        print("OK role/autodetect")
    finally:
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass
        try:
            if replica_fs is not None:
                replica_fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
