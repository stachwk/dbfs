#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dsn_from_config
from dbfs_fuse import DBFS


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)

    try:
        DBFS(dsn, db_config, runtime_config={"lock_poll_interval_seconds": "0"})
    except ValueError as exc:
        message = str(exc)
        if "lock_poll_interval_seconds" not in message:
            raise AssertionError(message)
    else:
        raise AssertionError("expected invalid lock_poll_interval_seconds to fail fast")

    try:
        DBFS(dsn, db_config, runtime_config={"read_cache_blocks": "0"})
    except ValueError as exc:
        message = str(exc)
        if "read_cache_blocks" not in message:
            raise AssertionError(message)
    else:
        raise AssertionError("expected invalid read_cache_blocks to fail fast")

    try:
        DBFS(dsn, db_config, runtime_config={"workers_read": "0"})
    except ValueError as exc:
        message = str(exc)
        if "workers_read" not in message:
            raise AssertionError(message)
    else:
        raise AssertionError("expected invalid workers_read to fail fast")

    try:
        DBFS(dsn, db_config, runtime_config={"workers_write": "0"})
    except ValueError as exc:
        message = str(exc)
        if "workers_write" not in message:
            raise AssertionError(message)
    else:
        raise AssertionError("expected invalid workers_write to fail fast")

    print("OK runtime-validation")


if __name__ == "__main__":
    main()
