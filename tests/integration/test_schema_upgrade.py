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
from dbfs_backend import load_dbfs_runtime_config
from dbfs_schema import SCHEMA_ADMIN_TABLE, SCHEMA_VERSION, SCHEMA_VERSION_TABLE
from dbfs_fuse import DBFS


def run_mkfs(action: str, env: dict[str, str], extra_args: list[str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "mkfs.dbfs.py", action]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )


def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table_name,),
        )
        return bool(cur.fetchone()[0])


def column_exists(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            )
            """,
            (table_name, column_name),
        )
        return bool(cur.fetchone()[0])


def assert_upgrade_message(output: str) -> None:
    if (
        f"Schema upgraded to version {SCHEMA_VERSION}." in output
        or f"Schema already at version {SCHEMA_VERSION}." in output
    ):
        return
    raise AssertionError(output)


def assert_password_source(output: str, source: str) -> None:
    expected = f"Schema admin password source: {source} (no prompt needed)"
    if expected not in output:
        raise AssertionError(output)


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    env = os.environ.copy()
    env.setdefault("POSTGRES_DB", db_config["dbname"])
    env.setdefault("POSTGRES_USER", db_config["user"])
    env.setdefault("POSTGRES_PASSWORD", db_config["password"])
    schema_password = os.environ.get("DBFS_SCHEMA_ADMIN_PASSWORD") or f"dbfs-{secrets.token_urlsafe(24)}"
    schema_args = ["--schema-admin-password", schema_password]

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        conn.commit()

    guard_table = "schema_upgrade_guard"
    guard_sql = f"CREATE TABLE IF NOT EXISTS {guard_table} (id INTEGER PRIMARY KEY, note TEXT NOT NULL)"

    try:
        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(guard_sql)
            cur.execute(
                f"INSERT INTO {guard_table} (id, note) VALUES (1, 'guard') "
                "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note"
            )
            conn.commit()

        init_without_secret = run_mkfs("init", env, check=False)
        if init_without_secret.returncode == 0:
            raise AssertionError("init without the schema secret unexpectedly succeeded")
        init_without_output = init_without_secret.stdout + init_without_secret.stderr
        if "Schema admin password is required for init; pass --schema-admin-password." not in init_without_output:
            raise AssertionError(init_without_output)

        init_with_secret = run_mkfs("init", env, extra_args=schema_args, check=True)
        assert_password_source(init_with_secret.stdout, "cli")
        if "Initialization completed successfully." not in init_with_secret.stdout:
            raise AssertionError(init_with_secret.stdout)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            if not table_exists(conn, guard_table):
                raise AssertionError("guard table was removed by init")
            cur.execute(f"SELECT note FROM {guard_table} WHERE id = 1")
            row = cur.fetchone()
            if not row or row[0] != "guard":
                raise AssertionError(row)
            cur.execute(f"SELECT version FROM {SCHEMA_VERSION_TABLE} ORDER BY applied_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row or int(row[0]) != SCHEMA_VERSION:
                raise AssertionError(row)
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_ADMIN_TABLE} WHERE id = 1")
            if int(cur.fetchone()[0]) != 1:
                raise AssertionError("schema admin secret row was not created")

        upgrade_wrong = run_mkfs("upgrade", env, extra_args=["--schema-admin-password", "wrong-password"], check=False)
        if upgrade_wrong.returncode == 0:
            raise AssertionError("upgrade with wrong secret unexpectedly succeeded")
        if "does not match the schema-admin secret currently stored in the DBFS database" not in (upgrade_wrong.stderr + upgrade_wrong.stdout):
            raise AssertionError(upgrade_wrong.stderr + upgrade_wrong.stdout)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHEMA_VERSION_TABLE}")
            conn.commit()

        upgrade_result = run_mkfs("upgrade", env, extra_args=schema_args, check=True)
        assert_password_source(upgrade_result.stdout, "cli")
        assert_upgrade_message(upgrade_result.stdout)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            if not table_exists(conn, guard_table):
                raise AssertionError("guard table was removed by upgrade")
            cur.execute(f"SELECT note FROM {guard_table} WHERE id = 1")
            row = cur.fetchone()
            if not row or row[0] != "guard":
                raise AssertionError(row)
            cur.execute(f"SELECT version FROM {SCHEMA_VERSION_TABLE} ORDER BY applied_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row or int(row[0]) != SCHEMA_VERSION:
                raise AssertionError(row)

        no_secret_clean = env.copy()
        clean_missing_secret = run_mkfs("clean", no_secret_clean, check=False)
        if clean_missing_secret.returncode == 0:
            raise AssertionError("clean without the schema secret unexpectedly succeeded")
        clean_missing_output = clean_missing_secret.stdout + clean_missing_secret.stderr
        if "Schema admin password is required for clean; pass --schema-admin-password." not in clean_missing_output:
            raise AssertionError(clean_missing_output)

        clean_result = run_mkfs("clean", env, extra_args=schema_args, check=True)
        assert_password_source(clean_result.stdout, "cli")
        if "Cleanup completed." not in clean_result.stdout:
            raise AssertionError(clean_result.stdout)

        clean_again = run_mkfs("clean", env, extra_args=schema_args, check=True)
        if "Cleanup completed." not in clean_again.stdout:
            raise AssertionError(clean_again.stdout)

        init_after_clean = run_mkfs("init", env, extra_args=schema_args, check=True)
        assert_password_source(init_after_clean.stdout, "cli")
        if "Initialization completed successfully." not in init_after_clean.stdout:
            raise AssertionError(init_after_clean.stdout)

        upgrade_after_clean = run_mkfs("upgrade", env, extra_args=schema_args, check=True)
        assert_password_source(upgrade_after_clean.stdout, "cli")
        assert_upgrade_message(upgrade_after_clean.stdout)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(f"UPDATE {SCHEMA_VERSION_TABLE} SET version = %s", (SCHEMA_VERSION - 1,))
            conn.commit()

        mismatch_error = None
        try:
            fs = DBFS(dsn=dsn, db_config=db_config, runtime_config=load_dbfs_runtime_config(ROOT))
        except RuntimeError as exc:
            mismatch_error = str(exc)
        else:
            fs.close()
        if mismatch_error is None:
            raise AssertionError("DBFS unexpectedly accepted a mismatched schema version")
        if "DBFS schema version mismatch" not in mismatch_error:
            raise AssertionError(mismatch_error)

        upgrade_result = run_mkfs("upgrade", env, extra_args=schema_args, check=True)
        assert_password_source(upgrade_result.stdout, "cli")
        assert_upgrade_message(upgrade_result.stdout)

        fs = DBFS(dsn=dsn, db_config=db_config, runtime_config=load_dbfs_runtime_config(ROOT))
        fs.close()

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHEMA_ADMIN_TABLE}")
            cur.execute(f"DELETE FROM lock_range_leases")
            cur.execute(f"UPDATE {SCHEMA_VERSION_TABLE} SET version = 1")
            conn.commit()

        upgrade_result = run_mkfs("upgrade", env, extra_args=schema_args, check=True)
        assert_password_source(upgrade_result.stdout, "cli")
        if f"Schema upgraded to version {SCHEMA_VERSION}." not in upgrade_result.stdout:
            raise AssertionError(upgrade_result.stdout)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            if not table_exists(conn, SCHEMA_ADMIN_TABLE):
                raise AssertionError("schema_admin table missing after migration from version 1")
            if not table_exists(conn, "lock_range_leases"):
                raise AssertionError("lock_range_leases table missing after migration from version 1")
            cur.execute(f"SELECT version FROM {SCHEMA_VERSION_TABLE} ORDER BY applied_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row or int(row[0]) != SCHEMA_VERSION:
                raise AssertionError(row)
    finally:
        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {guard_table}")
            conn.commit()

    print("OK schema-upgrade/non-destructive/password-protected")


if __name__ == "__main__":
    main()
