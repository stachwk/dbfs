#!/usr/bin/env python3

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg2

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def ensure_schema_ready() -> None:
    schema_password = os.environ.get("DBFS_SCHEMA_ADMIN_PASSWORD") or f"dbfs-{secrets.token_urlsafe(24)}"
    result = subprocess.run(
        [
            sys.executable,
            str(Path(ROOT) / "mkfs.dbfs.py"),
            "init",
            "--schema-admin-password",
            schema_password,
        ],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return
    combined_output = "\n".join(filter(None, [result.stdout, result.stderr]))
    if "Schema admin password is required for this existing database" in combined_output:
        return
    if "Schema admin password does not match the schema-admin secret currently stored in the DBFS database" in combined_output:
        return
    raise RuntimeError(
        "Unable to bootstrap DBFS schema for connection-recovery test:\n"
        f"{combined_output}".strip()
    )


class FlakyCursor:
    def __init__(self, real_cursor, state):
        self._real_cursor = real_cursor
        self._state = state

    def execute(self, statement, params=None):
        if not self._state["failed"]:
            self._state["failed"] = True
            raise psycopg2.OperationalError("simulated connection loss")
        return self._real_cursor.execute(statement, params)

    def fetchone(self):
        return self._real_cursor.fetchone()

    def fetchall(self):
        return self._real_cursor.fetchall()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._real_cursor.close()
        return False


class FlakyConnection:
    def __init__(self, real_connection, state):
        self._dbfs_raw_connection = real_connection
        self._state = state

    def cursor(self):
        return FlakyCursor(self._dbfs_raw_connection.cursor(), self._state)

    def rollback(self):
        return self._dbfs_raw_connection.rollback()

    def commit(self):
        return self._dbfs_raw_connection.commit()

    def close(self):
        return self._dbfs_raw_connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def main() -> None:
    ensure_schema_ready()
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/recovery-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    payload = (b"resume-after-drop-" * 256) + b"EOF"
    fh = None
    original_getconn = fs.backend.connection_pool.getconn

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))

        flaky_state = {"failed": False}

        def flaky_getconn():
            real_conn = original_getconn()
            if not flaky_state["failed"]:
                return FlakyConnection(real_conn, flaky_state)
            return real_conn

        fs.backend.connection_pool.getconn = flaky_getconn
        fs.persist_buffer(fs.file_id_for_handle(fh))

        recovered_size = fs.getattr(file_path)["st_size"]
        assert recovered_size == len(payload), (recovered_size, len(payload))

        fs.backend.connection_pool.getconn = original_getconn
        fs.storage.clear_read_cache()

        flaky_state = {"failed": False}

        def flaky_getconn_read():
            real_conn = original_getconn()
            if not flaky_state["failed"]:
                return FlakyConnection(real_conn, flaky_state)
            return real_conn

        fs.backend.connection_pool.getconn = flaky_getconn_read
        read_back = fs.read(file_path, len(payload), 0, fh)
        assert read_back == payload, "read recovery mismatch"

        print("OK connection-recovery/write-read")
    finally:
        fs.backend.connection_pool.getconn = original_getconn
        if fh is not None:
            try:
                fs.release(file_path, fh)
            except Exception:
                pass
        try:
            fs.unlink(file_path)
        except Exception:
            pass
        try:
            fs.rmdir(dir_path)
        except Exception:
            pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
