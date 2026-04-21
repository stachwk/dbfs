#!/usr/bin/env python3

from __future__ import annotations

import errno
import fcntl
import os
import sys
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dsn_from_config
from dbfs_fuse import DBFS


def dummy_lock(lock_type: int) -> object:
    return type("Lock", (), {"l_type": lock_type, "l_whence": 0, "l_start": 0, "l_len": 0, "l_pid": 0})()


def range_lock(lock_type: int, start: int, length: int, whence: int = os.SEEK_SET) -> object:
    return type("Lock", (), {"l_type": lock_type, "l_whence": whence, "l_start": start, "l_len": length, "l_pid": 0})()


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM lock_range_leases")
        cur.execute("DELETE FROM lock_leases")
        conn.commit()
    runtime_config = {
        "lock_lease_ttl_seconds": "2",
        "lock_heartbeat_interval_seconds": "1",
        "lock_poll_interval_seconds": "0.05",
    }
    fs1 = DBFS(dsn, db_config, runtime_config=runtime_config)
    fs2 = DBFS(dsn, db_config, runtime_config=runtime_config)
    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/pg-lock-{suffix}"
    file_path = f"{dir_path}/file.txt"

    try:
        fs1.mkdir(dir_path, 0o755)
        fh_create = fs1.create(file_path, 0o644)
        fs1.write(file_path, b"pg-lock", 0, fh_create)
        fs1.flush(file_path, fh_create)
        fs1.release(file_path, fh_create)

        fh1 = fs1.open(file_path, os.O_RDWR)
        fh2 = fs2.open(file_path, os.O_RDWR)
        fh3 = None
        try:
            fs1.flock(file_path, fh1, fcntl.LOCK_EX)
            fs1.write(file_path, b"client-one", 0, fh1)
            fs1.flush(file_path, fh1)
            try:
                fs2.flock(file_path, fh2, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                assert exc.errno in {errno.EWOULDBLOCK, errno.EAGAIN}, exc
            else:
                raise AssertionError("expected exclusive conflict while first client holds the write lease")

            fs1.flock(file_path, fh1, fcntl.LOCK_UN)
            fs2.flock(file_path, fh2, fcntl.LOCK_EX)
            fs2.write(file_path, b"client-two", 0, fh2)
            fs2.flush(file_path, fh2)
            final_data = fs2.read(file_path, 64, 0, fh2)
            assert final_data == b"client-two", final_data
            fs2.flock(file_path, fh2, fcntl.LOCK_UN)
            fh3 = fs2.open(file_path, os.O_RDONLY)

            fs1.lock(file_path, fh1, fcntl.F_SETLK, range_lock(fcntl.F_RDLCK, 0, 8))
            fs2.lock(file_path, fh2, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 8, 8))
            try:
                fs2.lock(file_path, fh3, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 4, 4))
            except OSError as exc:
                assert exc.errno in {errno.EWOULDBLOCK, errno.EAGAIN}, exc
            else:
                raise AssertionError("expected overlapping range conflict in PostgreSQL backend")

            fs1.lock(file_path, fh1, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 2, 4))
            query = range_lock(fcntl.F_WRLCK, 1, 1)
            fs2.lock(file_path, fh3, fcntl.F_GETLK, query)
            assert query.l_type == fcntl.F_RDLCK, query.l_type
            query = range_lock(fcntl.F_WRLCK, 3, 1)
            fs2.lock(file_path, fh3, fcntl.F_GETLK, query)
            assert query.l_type == fcntl.F_UNLCK, query.l_type
            query = range_lock(fcntl.F_WRLCK, 6, 1)
            fs2.lock(file_path, fh3, fcntl.F_GETLK, query)
            assert query.l_type == fcntl.F_RDLCK, query.l_type

            fs1.lock(file_path, fh1, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 0, 0))
            fs2.lock(file_path, fh2, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 8, 8))

            with fs1.db_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lock_range_leases")
                remaining_range = int(cur.fetchone()[0])
                assert remaining_range == 0, remaining_range

            with fs1.db_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lock_leases")
                remaining = int(cur.fetchone()[0])
                assert remaining == 0, remaining
            print("OK pg-lock-manager/multi-client-write")
        finally:
            try:
                fs1.release(file_path, fh1)
            except Exception:
                pass
            try:
                fs2.release(file_path, fh2)
            except Exception:
                pass
            try:
                if fh3 is not None:
                    fs2.release(file_path, fh3)
            except Exception:
                pass
    finally:
        try:
            fs1.unlink(file_path)
        except Exception:
            pass
        try:
            fs1.rmdir(dir_path)
        except Exception:
            pass
        try:
            fs1.cleanup_resources()
        except Exception:
            pass
        try:
            fs2.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
