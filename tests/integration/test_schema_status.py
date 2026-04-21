#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import secrets
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dsn_from_config
from dbfs_schema import SCHEMA_ADMIN_TABLE
from dbfs_schema import SCHEMA_VERSION_TABLE


def run_mkfs(action: str, env: dict[str, str], extra_args: list[str] | None = None) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "mkfs.dbfs.py", action]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def main() -> None:
    _, db_config = load_dsn_from_config(ROOT)
    env = os.environ.copy()
    env.setdefault("POSTGRES_DB", db_config["dbname"])
    env.setdefault("POSTGRES_USER", db_config["user"])
    env.setdefault("POSTGRES_PASSWORD", db_config["password"])
    schema_password = os.environ.get("DBFS_SCHEMA_ADMIN_PASSWORD") or secrets.token_urlsafe(24)
    schema_args = ["--schema-admin-password", schema_password]

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        conn.commit()

    run_mkfs("init", env, extra_args=schema_args)
    status_after_init = run_mkfs("status", env).stdout
    if "Schema version: 3" not in status_after_init:
        raise AssertionError(status_after_init)
    if "Latest migration version: 3" not in status_after_init:
        raise AssertionError(status_after_init)
    if "Schema admin secret: present" not in status_after_init:
        raise AssertionError(status_after_init)
    if "DBFS ready: yes" not in status_after_init:
        raise AssertionError(status_after_init)
    if "Pending migrations: none" not in status_after_init:
        raise AssertionError(status_after_init)
    if "0001: 0001_base.sql" not in status_after_init:
        raise AssertionError(status_after_init)
    if "0002: 0002_schema_admin.sql" not in status_after_init:
        raise AssertionError(status_after_init)
    if "0003: 0003_schema_version_sql.sql" not in status_after_init:
        raise AssertionError(status_after_init)

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA_VERSION_TABLE}")
        conn.commit()

    status_without_version = run_mkfs("status", env).stdout
    if "Schema version: none" not in status_without_version:
        raise AssertionError(status_without_version)
    if "Schema admin secret: present" not in status_without_version:
        raise AssertionError(status_without_version)
    if "DBFS ready: no" not in status_without_version:
        raise AssertionError(status_without_version)
    if "Pending migrations: 0001, 0002, 0003" not in status_without_version:
        raise AssertionError(status_without_version)

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT id, password_hash, password_salt, password_iterations, created_at, updated_at FROM {SCHEMA_ADMIN_TABLE} WHERE id = 1"
        )
        row = cur.fetchone()
        if not row:
            raise AssertionError("schema admin secret row missing before status secret test")
        cur.execute(f"DELETE FROM {SCHEMA_ADMIN_TABLE}")
        conn.commit()

        status_without_secret = run_mkfs("status", env).stdout
        if "Schema admin secret: missing" not in status_without_secret:
            raise AssertionError(status_without_secret)
        if "DBFS ready: no" not in status_without_secret:
            raise AssertionError(status_without_secret)

        cur.execute(
            f"""
            INSERT INTO {SCHEMA_ADMIN_TABLE} (id, password_hash, password_salt, password_iterations, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            row,
        )
        cur.execute(f"INSERT INTO {SCHEMA_VERSION_TABLE} (version, applied_at) VALUES (%s, NOW())", (3,))
        conn.commit()

    print("OK schema-status")


if __name__ == "__main__":
    main()
