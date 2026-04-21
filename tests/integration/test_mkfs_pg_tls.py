#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import secrets
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_backend import load_dsn_from_config

MKFS_PATH = Path(ROOT) / "mkfs.dbfs.py"
MKFS_SPEC = importlib.util.spec_from_file_location("mkfs_dbfs", MKFS_PATH)
if MKFS_SPEC is None or MKFS_SPEC.loader is None:
    raise RuntimeError(f"Unable to load mkfs helper from {MKFS_PATH}")
mkfs = importlib.util.module_from_spec(MKFS_SPEC)
MKFS_SPEC.loader.exec_module(mkfs)


def main():
    with tempfile.TemporaryDirectory(prefix="dbfs-mkfs-tls-") as tmpdir:
        tmp_path = Path(tmpdir)

        tls_config = tmp_path / "dbfs_tls_config.ini"
        tls_config.write_text(
            "\n".join(
                [
                    "[database]",
                    "host = 127.0.0.1",
                    "port = 5432",
                    "dbname = dbfsdbname",
                    "user = dbfsuser",
                    "password = cichosza",
                    "sslmode = require",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        connection_params, _ = load_dsn_from_config(tls_config)
        assert connection_params["sslmode"] == "require", connection_params

        schema_config = tmp_path / "dbfs_schema_config.ini"
        schema_config.write_text(
            "\n".join(
                [
                    "[database]",
                    "host = 127.0.0.1",
                    "port = 5432",
                    "dbname = dbfsdbname",
                    "user = dbfsuser",
                    "password = cichosza",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        os.environ["DBFS_CONFIG"] = str(schema_config)
        calls: list[tuple[str, str, int]] = []

        class DummyCursor:
            def __init__(self):
                self.statements: list[tuple[str, tuple | None]] = []

            def execute(self, statement, params=None):
                self.statements.append((statement, params))

            def fetchone(self):
                return (1,)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyConnection:
            def __init__(self):
                self.cursor_obj = DummyCursor()

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                return None

            def rollback(self):
                return None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_generate_client_tls_pair(material_dir, common_name="dbfs", days=365):
            material_path = Path(material_dir)
            material_path.mkdir(parents=True, exist_ok=True)
            cert_path = material_path / "client.crt"
            key_path = material_path / "client.key"
            cert_path.write_text("CERT", encoding="utf-8")
            key_path.write_text("KEY", encoding="utf-8")
            calls.append((str(material_path), common_name, days))
            return cert_path, key_path

        schema_password = os.environ.get("DBFS_SCHEMA_ADMIN_PASSWORD") or secrets.token_urlsafe(24)

        def fake_ensure_schema_admin_secret(db_config_arg, password):
            assert password == schema_password, password
            return True

        def fake_connect(**kwargs):
            return DummyConnection()

        original = mkfs.generate_client_tls_pair
        original_connect = mkfs.psycopg2.connect
        original_apply_migrations_up_to = mkfs.apply_migrations_up_to
        original_ensure_schema_admin_secret = mkfs.ensure_schema_admin_secret
        original_read_schema_version = mkfs.read_schema_version
        original_write_schema_version = mkfs.write_schema_version
        original_run_sql_commands_user = mkfs.run_sql_commands_user
        mkfs.generate_client_tls_pair = fake_generate_client_tls_pair
        mkfs.psycopg2.connect = fake_connect
        mkfs.apply_migrations_up_to = lambda *args, **kwargs: None
        mkfs.ensure_schema_admin_secret = fake_ensure_schema_admin_secret
        mkfs.read_schema_version = lambda *args, **kwargs: mkfs.SCHEMA_VERSION
        mkfs.write_schema_version = lambda *args, **kwargs: None
        mkfs.run_sql_commands_user = lambda *args, **kwargs: None
        try:
            mkfs.main([
                "init",
                "--schema-admin-password",
                schema_password,
                "--generate-client-tls-pair",
                "1",
                "--tls-material-dir",
                "tls-material",
                "--tls-common-name",
                "dbfs-test",
            ])
            mkfs.main([
                "upgrade",
                "--schema-admin-password",
                schema_password,
                "--generate-client-tls-pair",
                "1",
                "--tls-material-dir",
                "tls-material",
                "--tls-common-name",
                "dbfs-test",
            ])
        finally:
            mkfs.generate_client_tls_pair = original
            mkfs.psycopg2.connect = original_connect
            mkfs.apply_migrations_up_to = original_apply_migrations_up_to
            mkfs.ensure_schema_admin_secret = original_ensure_schema_admin_secret
            mkfs.read_schema_version = original_read_schema_version
            mkfs.write_schema_version = original_write_schema_version
            mkfs.run_sql_commands_user = original_run_sql_commands_user
            os.environ.pop("DBFS_CONFIG", None)

        expected_dir = str((tmp_path / "tls-material").resolve())
        assert calls and calls[0][0] == expected_dir, calls
        assert calls[0][1] == "dbfs-test", calls
        assert (tmp_path / "tls-material" / "client.crt").exists(), calls
        assert (tmp_path / "tls-material" / "client.key").exists(), calls
        print("OK mkfs-pg-tls")


if __name__ == "__main__":
    main()
