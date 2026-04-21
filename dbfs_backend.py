from __future__ import annotations

import os
from contextlib import contextmanager
from collections.abc import Mapping

import psycopg2
import psycopg2.pool

from dbfs_config import load_config_parser
from dbfs_pg_tls import resolve_pg_connection_params


def load_dsn_from_config(file_path):
    config, config_path = load_config_parser(file_path)
    db_config = config["database"]
    connection_params = resolve_pg_connection_params(db_config, config_dir=config_path.parent)
    return connection_params, dict(connection_params)


def load_dbfs_runtime_config(file_path):
    config, _ = load_config_parser(file_path)
    runtime = dict(config["dbfs"]) if config.has_section("dbfs") else {}
    profile_name = os.environ.get("DBFS_PROFILE") or runtime.get("profile")
    if profile_name:
        for section_name in (f"dbfs.profile.{profile_name}", f"dbfs.profile:{profile_name}"):
            if config.has_section(section_name):
                runtime.update(dict(config[section_name]))
                runtime["profile"] = profile_name
                break
    return runtime


class PostgresBackend:
    def __init__(self, dsn, db_config, pool_max_connections=10, synchronous_commit="on"):
        self.dsn = dsn
        self.db_config = db_config
        self.pool_max_connections = self.resolve_pool_max_connections(pool_max_connections)
        self.synchronous_commit = self.resolve_synchronous_commit(synchronous_commit)
        self._session_initialized_connection_ids = set()
        if isinstance(self.dsn, Mapping):
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(1, self.pool_max_connections, **self.dsn)
        else:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(1, self.pool_max_connections, self.dsn)

    def resolve_pool_max_connections(self, pool_max_connections):
        try:
            if hasattr(pool_max_connections, "getint"):
                pool_max_connections = pool_max_connections.getint("pool_max_connections", fallback=10)
        except Exception:
            pool_max_connections = 10

        pool_max_connections = int(pool_max_connections)
        if pool_max_connections < 1:
            return 1
        return pool_max_connections

    def resolve_synchronous_commit(self, synchronous_commit):
        value = "on" if synchronous_commit in {None, ""} else str(synchronous_commit).strip().lower()
        allowed = {"on", "off", "local", "remote_write", "remote_apply"}
        if value not in allowed:
            allowed_values = ", ".join(sorted(allowed))
            raise ValueError(f"synchronous_commit must be one of: {allowed_values}")
        return value

    def _physical_connection_id(self, conn):
        raw_conn = getattr(conn, "_dbfs_raw_connection", conn)
        return id(raw_conn), raw_conn

    def _initialize_session_settings(self, conn):
        conn_id, _ = self._physical_connection_id(conn)
        if conn_id in self._session_initialized_connection_ids:
            return
        original_autocommit = conn.autocommit
        try:
            if not original_autocommit:
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
                cur.execute(f"SET synchronous_commit TO '{self.synchronous_commit}'")
        finally:
            if conn.autocommit != original_autocommit:
                conn.autocommit = original_autocommit
        self._session_initialized_connection_ids.add(conn_id)

    @contextmanager
    def connection(self):
        conn = self.connection_pool.getconn()
        _, raw_conn = self._physical_connection_id(conn)
        discarded = False
        try:
            conn.autocommit = False
            self._initialize_session_settings(conn)
            yield conn
        except Exception as exc:
            if self.is_transient_connection_error(exc):
                discarded = True
                self.discard_connection(raw_conn)
            raise
        finally:
            try:
                conn.rollback()
            except Exception:
                pass
            if not discarded:
                self.connection_pool.putconn(raw_conn)

    def close(self):
        self.connection_pool.closeall()
        self._session_initialized_connection_ids.clear()

    def is_transient_connection_error(self, exc):
        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def discard_connection(self, conn):
        if conn is None:
            return
        raw_conn = getattr(conn, "_dbfs_raw_connection", conn)
        self._session_initialized_connection_ids.discard(id(raw_conn))
        try:
            self.connection_pool.putconn(raw_conn, close=True)
        except Exception:
            pass

    def get_config_value(self, key, default=None):
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = %s", (key,))
            result = cur.fetchone()
            return result[0] if result else default

    def is_in_recovery(self):
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            result = cur.fetchone()
            return bool(result[0]) if result else False

    def schema_is_initialized(self):
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    to_regclass('public.directories') IS NOT NULL
                    AND to_regclass('public.files') IS NOT NULL
                    AND to_regclass('public.schema_version') IS NOT NULL
                """
            )
            result = cur.fetchone()
            return bool(result[0]) if result else False

    def schema_version(self):
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
            result = cur.fetchone()
            return int(result[0]) if result else None
