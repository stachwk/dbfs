#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_time import db_timestamp_to_epoch, epoch_to_utc_datetime


def main() -> None:
    assert db_timestamp_to_epoch(datetime(2026, 4, 19, 12, 0, 0)) == datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert db_timestamp_to_epoch(epoch_to_utc_datetime(0)) == 0.0

    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    pool_max_connections = int(runtime_config.get("pool_max_connections", 10))

    with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
        assert conn.autocommit is False, "DBFS PostgreSQL connections must run with autocommit disabled"
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("SHOW TIME ZONE")
        time_zone = cur.fetchone()[0]
        cur.execute("SHOW server_version_num")
        server_version_num = int(cur.fetchone()[0])
        cur.execute("SHOW max_connections")
        max_connections = int(cur.fetchone()[0])

    required_min_version = 90500
    required_min_connections = pool_max_connections + 2

    assert server_version_num >= required_min_version, (
        f"PostgreSQL {required_min_version // 10000}.{(required_min_version // 100) % 100}+ is required, "
        f"got server_version_num={server_version_num}"
    )
    assert max_connections >= required_min_connections, (
        f"max_connections must be at least pool_max_connections + 2 "
        f"({required_min_connections}), got {max_connections}"
    )
    assert time_zone.upper() == "UTC", f"DBFS PostgreSQL sessions must run in UTC, got {time_zone!r}"

    print(
        "OK postgres-requirements "
        f"version={server_version_num} max_connections={max_connections} "
        f"pool_max_connections={pool_max_connections}"
    )


if __name__ == "__main__":
    main()
