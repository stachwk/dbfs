#!/usr/bin/env python3

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dbfs_runtime_config, load_dsn_from_config


def main():
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    expected = int(runtime_config.get("pool_max_connections", 10))
    fs = DBFS(dsn, db_config, pool_max_connections=expected)

    try:
        assert fs.pool_max_connections == expected, f"expected pool_max_connections={expected}, got {fs.pool_max_connections}"
        assert fs.connection_pool.maxconn == expected, f"connection pool maxconn mismatch: {fs.connection_pool.maxconn}"
        assert fs.connection_pool.minconn == 1, f"connection pool minconn mismatch: {fs.connection_pool.minconn}"
        print("OK pool/max_connections")
    finally:
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
