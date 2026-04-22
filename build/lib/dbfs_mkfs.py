#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import base64
import hashlib
import hmac
import os
import secrets
from pathlib import Path
import psycopg2
from psycopg2 import errors
from psycopg2 import sql

from dbfs_config import load_config_parser, resolve_config_path
from dbfs_migrations import latest_migration_version, migration_exists, migration_manifest, migration_sql
from dbfs_pg_tls import generate_client_tls_pair, resolve_pg_connection_params
from dbfs_schema import SCHEMA_ADMIN_TABLE, SCHEMA_VERSION, SCHEMA_VERSION_TABLE
from dbfs_version import DBFS_VERSION_LABEL


def load_db_config(file_path=None):
    config, config_path = load_config_parser(file_path, base_dir=os.path.dirname(os.path.abspath(__file__)))
    return resolve_pg_connection_params(config["database"], config_dir=config_path.parent)


def parse_truthy_arg(value):
    if value is None:
        return True
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError("expected 0/1, true/false, yes/no, or on/off")


def prepare_tls_material(args, config_path):
    if not args.generate_client_tls_pair:
        return None

    if args.action not in {"init", "upgrade"}:
        raise ValueError("--generate-client-tls-pair is only supported with init or upgrade")

    tls_material_dir = args.tls_material_dir or ".dbfs/tls"
    material_dir = Path(tls_material_dir).expanduser()
    if not material_dir.is_absolute():
        material_dir = Path(config_path).expanduser().resolve().parent / material_dir
    return generate_client_tls_pair(material_dir, common_name=args.tls_common_name, days=args.tls_cert_days)


def load_schema_admin_password(args, config_path):
    if args.schema_admin_password:
        return args.schema_admin_password, "cli"
    return None, None


def derive_schema_admin_secret(password, salt=None, iterations=200_000):
    salt_bytes = salt or secrets.token_bytes(16)
    if isinstance(salt_bytes, str):
        salt_bytes = base64.b64decode(salt_bytes.encode("ascii"))
    if isinstance(password, str):
        password_bytes = password.encode("utf-8")
    else:
        password_bytes = password
    digest = hashlib.pbkdf2_hmac("sha256", password_bytes, salt_bytes, iterations)
    return (
        base64.b64encode(salt_bytes).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
        int(iterations),
    )


def verify_schema_admin_secret(password, salt_b64, hash_b64, iterations):
    _, derived_hash, _ = derive_schema_admin_secret(password, salt_b64, iterations)
    return hmac.compare_digest(derived_hash, hash_b64)


def ensure_schema_admin_secret(db_config, password):
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_ADMIN_TABLE} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                password_iterations INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"SELECT password_hash, password_salt, password_iterations FROM {SCHEMA_ADMIN_TABLE} WHERE id = 1"
        )
        row = cur.fetchone()
        if row:
            if not password:
                raise RuntimeError(
                    "Schema admin password is required for this existing database; pass --schema-admin-password."
                )
            if not verify_schema_admin_secret(password, row[1], row[0], row[2]):
                raise PermissionError(
                    "Schema admin password does not match the schema-admin secret currently stored in the DBFS database. "
                    "This usually means you are using a secret from a different bootstrap; rerun init to generate a new secret or provide the current one."
                )
            return False

        if not password:
            raise RuntimeError(
                "Schema admin password is required for the first DBFS bootstrap; pass --schema-admin-password."
            )
        salt_b64, hash_b64, iterations = derive_schema_admin_secret(password)
        cur.execute(
            f"""
            INSERT INTO {SCHEMA_ADMIN_TABLE} (id, password_hash, password_salt, password_iterations, created_at, updated_at)
            VALUES (1, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                password_salt = EXCLUDED.password_salt,
                password_iterations = EXCLUDED.password_iterations,
                updated_at = NOW()
            """,
            (hash_b64, salt_b64, iterations),
        )
        conn.commit()
        return True


def format_schema_admin_source_message(source):
    return f"Schema admin password source: {source} (no prompt needed)"


def schema_admin_secret_required_message(action_name):
    return f"Schema admin password is required for {action_name}; pass --schema-admin-password."


DROP_SCHEMAS_SQL = """
DROP SCHEMA IF EXISTS public CASCADE;
"""

GRANT_PERMISSIONS_SQL = """
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {};
"""

REVOKE_PERMISSIONS_SQL = """
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM {};
"""


def run_sql_commands(db_config, sql_commands):
    try:
        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(sql_commands)
            conn.commit()
    except Exception as e:
        print(f"Error occurred: {type(e).__name__}: {e}")
        raise


def run_sql_commands_user(db_config, sql_commands):
    try:
        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(sql.SQL(sql_commands).format(sql.Identifier(db_config['user'])))
            conn.commit()
    except Exception as e:
        print(f"Error occurred: {type(e).__name__}: {e}")
        raise


def apply_migration(db_config, version):
    sql_text = migration_sql(version)
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute(sql_text)
        conn.commit()


def apply_migrations_up_to(db_config, target_version):
    for version in range(1, target_version + 1):
        if not migration_exists(version):
            raise RuntimeError(f"Missing migration file for version {version}")
        apply_migration(db_config, version)


def read_schema_version(db_config):
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        try:
            cur.execute(f"SELECT version FROM {SCHEMA_VERSION_TABLE} ORDER BY applied_at DESC LIMIT 1")
        except errors.UndefinedTable:
            conn.rollback()
            return None
        result = cur.fetchone()
        return int(result[0]) if result else None


def public_schema_exists(db_config):
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regnamespace('public') IS NOT NULL")
        result = cur.fetchone()
        return bool(result[0]) if result else False


def schema_admin_secret_exists(db_config):
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        try:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {SCHEMA_ADMIN_TABLE} WHERE id = 1)")
        except errors.UndefinedTable:
            conn.rollback()
            return False
        result = cur.fetchone()
        return bool(result[0]) if result else False


def write_schema_version(db_config, version):
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA_VERSION_TABLE}")
        cur.execute(
            f"INSERT INTO {SCHEMA_VERSION_TABLE} (version, applied_at) VALUES (%s, NOW())",
            (int(version),),
        )
        conn.commit()


def schema_state_report(db_config):
    current_version = read_schema_version(db_config)
    latest_version = latest_migration_version()
    pending_versions = list(range((current_version or 0) + 1, latest_version + 1))
    return {
        "current_version": current_version,
        "latest_version": latest_version,
        "pending_versions": pending_versions,
        "manifest": migration_manifest(),
    }


def print_schema_state(db_config):
    state = schema_state_report(db_config)
    current_version = state["current_version"]
    latest_version = state["latest_version"]
    pending_versions = state["pending_versions"]
    manifest = state["manifest"]
    secret_present = schema_admin_secret_exists(db_config)
    ready = current_version == latest_version and secret_present
    print(f"Schema version: {current_version if current_version is not None else 'none'}")
    print(f"Latest migration version: {latest_version}")
    print(f"Schema admin secret: {'present' if secret_present else 'missing'}")
    print(f"DBFS ready: {'yes' if ready else 'no'}")
    if pending_versions:
        print("Pending migrations: " + ", ".join(f"{version:04d}" for version in pending_versions))
    else:
        print("Pending migrations: none")
    print("Migration path:")
    for item in manifest:
        print(f"  - {item['version']:04d}: {item['filename']} :: {item['description']}")


def main(argv=None):
    parser = argparse.ArgumentParser(description='Manage the DBFS schema.')
    parser.add_argument('--version', action='version', version=DBFS_VERSION_LABEL)
    parser.add_argument('action', choices=['init', 'upgrade', 'clean', 'status'], help='Action to perform: init, upgrade, clean or status')
    parser.add_argument('--block-size', type=int, default=4096, help='Default block size (must be a multiple of 1024).')
    parser.add_argument(
        '--schema-admin-password',
        default=None,
        help='Password required for init/upgrade/clean when the schema admin secret is being created or checked.',
    )
    parser.add_argument(
        '--generate-client-tls-pair',
        nargs='?',
        const='1',
        default=False,
        type=parse_truthy_arg,
        help='Generate a local PostgreSQL TLS client cert/key pair before init or upgrade. Accepts 1/0 or true/false.',
    )
    parser.add_argument('--tls-material-dir', default='.dbfs/tls', help='Directory for generated PostgreSQL TLS material.')
    parser.add_argument('--tls-common-name', default='dbfs', help='Common name for generated PostgreSQL TLS material.')
    parser.add_argument('--tls-cert-days', type=int, default=365, help='Validity window for generated PostgreSQL TLS material.')
    args = parser.parse_args(argv)

    if args.block_size % 1024 != 0:
        raise ValueError("block_size must be a multiple of 1024")

    config_path = resolve_config_path(base_dir=os.path.dirname(os.path.abspath(__file__)))
    if args.action in {'init', 'upgrade'}:
        prepare_tls_material(args, config_path)
    db_config = load_db_config(config_path)
    uid = os.getuid() if hasattr(os, "getuid") else 0
    gid = os.getgid() if hasattr(os, "getgid") else 0
    schema_admin_password, schema_admin_source = load_schema_admin_password(args, config_path)

    if args.action == 'init':
        if not schema_admin_password:
            raise RuntimeError(schema_admin_secret_required_message("init"))
        print(format_schema_admin_source_message(schema_admin_source))
        apply_migrations_up_to(db_config, latest_migration_version())
        ensure_schema_admin_secret(db_config, schema_admin_password)

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO config (key, value)
                VALUES ('max_fs_size_bytes', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (10 * 1024 * 1024 * 1024,),
            )
            cur.execute(
                """
                INSERT INTO config (key, value)
                VALUES ('block_size', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (args.block_size,),
            )
            cur.execute(f"DELETE FROM {SCHEMA_VERSION_TABLE}")
            cur.execute(
                f"INSERT INTO {SCHEMA_VERSION_TABLE} (version, applied_at) VALUES (%s, NOW())",
                (SCHEMA_VERSION,),
            )
            cur.execute(
                "UPDATE directories SET uid = %s, gid = %s WHERE name IN ('/', '.Trash-1000') AND id_parent IS NULL",
                (uid, gid),
            )
            conn.commit()

        run_sql_commands_user(db_config, GRANT_PERMISSIONS_SQL.format(db_config['user']))
        print("Initialization completed successfully.")

    elif args.action == 'upgrade':
        if not schema_admin_password:
            raise RuntimeError(schema_admin_secret_required_message("upgrade"))
        print(format_schema_admin_source_message(schema_admin_source))
        current_version = read_schema_version(db_config)
        if current_version is not None and current_version > SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported schema version {current_version}; expected {SCHEMA_VERSION}.")
        start_version = current_version or 0
        for version in range(start_version + 1, SCHEMA_VERSION + 1):
            if not migration_exists(version):
                raise RuntimeError(f"Missing migration file for version {version}")
            apply_migration(db_config, version)
        ensure_schema_admin_secret(db_config, schema_admin_password)
        write_schema_version(db_config, SCHEMA_VERSION)
        if current_version == SCHEMA_VERSION:
            print(f"Schema already at version {SCHEMA_VERSION}.")
        else:
            print(f"Schema upgraded to version {SCHEMA_VERSION}.")

    elif args.action == 'clean':
        if not public_schema_exists(db_config):
            print("Cleanup completed.")
            return
        if not schema_admin_password:
            raise RuntimeError(schema_admin_secret_required_message("clean"))
        print(format_schema_admin_source_message(schema_admin_source))
        ensure_schema_admin_secret(db_config, schema_admin_password)
        run_sql_commands_user(db_config, REVOKE_PERMISSIONS_SQL.format(db_config['user']))
        run_sql_commands(db_config, DROP_SCHEMAS_SQL)
        print("Cleanup completed.")

    elif args.action == 'status':
        print_schema_state(db_config)


if __name__ == "__main__":
    main()
