#!/usr/bin/env python3

from __future__ import annotations

import errno
import fcntl
import os
import sys
import uuid
import threading
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def dummy_lock(lock_type: int) -> SimpleNamespace:
    return SimpleNamespace(l_type=lock_type, l_whence=0, l_start=0, l_len=0, l_pid=0)


def range_lock(lock_type: int, start: int, length: int, whence: int = os.SEEK_SET) -> SimpleNamespace:
    return SimpleNamespace(l_type=lock_type, l_whence=whence, l_start=start, l_len=length, l_pid=0)


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)
    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/lock-{suffix}"
    file_path = f"{dir_path}/file.txt"

    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, b"lock-test", 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_RDLCK))
        try:
            fs.lock(file_path, fh + 1, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected exclusive lock conflict")

        query = dummy_lock(fcntl.F_WRLCK)
        fs.lock(file_path, fh + 1, fcntl.F_GETLK, query)
        assert query.l_type == fcntl.F_RDLCK, query.l_type
        assert query.l_pid == os.getpid(), query.l_pid
        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))
        query = dummy_lock(fcntl.F_WRLCK)
        fs.lock(file_path, fh + 1, fcntl.F_GETLK, query)
        assert query.l_type == fcntl.F_UNLCK, query.l_type

        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))

        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_RDLCK, 0, 8))
        fs.lock(file_path, fh + 500, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 8, 8))
        try:
            fs.lock(file_path, fh + 600, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 4, 4))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected overlapping range conflict")
        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 2, 4))
        query = range_lock(fcntl.F_WRLCK, 1, 1)
        fs.lock(file_path, fh + 600, fcntl.F_GETLK, query)
        assert query.l_type == fcntl.F_RDLCK, query.l_type
        query = range_lock(fcntl.F_WRLCK, 3, 1)
        fs.lock(file_path, fh + 600, fcntl.F_GETLK, query)
        assert query.l_type == fcntl.F_UNLCK, query.l_type
        query = range_lock(fcntl.F_WRLCK, 6, 1)
        fs.lock(file_path, fh + 600, fcntl.F_GETLK, query)
        assert query.l_type == fcntl.F_RDLCK, query.l_type
        fs.lock(file_path, fh + 600, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 2, 2))
        fs.lock(file_path, fh + 600, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 2, 2))
        try:
            fs.lock(file_path, fh + 600, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 1, 4))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected conflict across locked regions")
        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 0, 0))
        fs.lock(file_path, fh + 500, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 8, 8))

        fs._seek_positions[fh] = 4
        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_RDLCK, 2, 2, os.SEEK_CUR))
        try:
            fs.lock(file_path, fh + 700, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 6, 1))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected SEEK_CUR-derived overlap conflict")
        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, 0, 0, os.SEEK_CUR))

        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_RDLCK, -4, 2, os.SEEK_END))
        try:
            fs.lock(file_path, fh + 800, fcntl.F_SETLK, range_lock(fcntl.F_WRLCK, 6, 1))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected SEEK_END-derived overlap conflict")
        fs.lock(file_path, fh, fcntl.F_SETLK, range_lock(fcntl.F_UNLCK, -4, 2, os.SEEK_END))

        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_RDLCK))
        fs.lock(file_path, fh + 100, fcntl.F_SETLK, dummy_lock(fcntl.F_RDLCK))
        try:
            fs.lock(file_path, fh + 200, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected write lock conflict while read locks are held")

        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))
        try:
            fs.lock(file_path, fh + 200, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected write lock conflict while a second read lock is held")

        fs.lock(file_path, fh + 100, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))
        fs.lock(file_path, fh + 200, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        fs.lock(file_path, fh + 200, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))

        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        try:
            fs.lock(file_path, fh + 400, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        except OSError as exc:
            assert exc.errno in {errno.EWOULDBLOCK, errno.EACCES}, exc
        else:
            raise AssertionError("expected write lock conflict while the lock is held")
        fs.lock(file_path, fh, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))
        fs.lock(file_path, fh + 400, fcntl.F_SETLK, dummy_lock(fcntl.F_WRLCK))
        fs.lock(file_path, fh + 400, fcntl.F_SETLK, dummy_lock(fcntl.F_UNLCK))

        flock_fh = fh + 300
        second_fh = fh + 301
        try:
            fs.flock(file_path, flock_fh, fcntl.LOCK_SH)
            fs.flock(file_path, flock_fh, fcntl.LOCK_SH)
            fs.flock(file_path, flock_fh, fcntl.LOCK_EX)
            fs.flock(file_path, flock_fh, fcntl.LOCK_UN)
            fs.flock(file_path, flock_fh, fcntl.LOCK_SH)
            fs.flock(file_path, second_fh, fcntl.LOCK_SH)
            try:
                fs.flock(file_path, fh + 302, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                assert exc.errno in {errno.EWOULDBLOCK, errno.EAGAIN}, exc
            else:
                raise AssertionError("expected flock exclusive conflict while second shared lock is held")

            fs.flock(file_path, second_fh, fcntl.LOCK_UN)
            try:
                fs.flock(file_path, fh + 302, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                assert exc.errno in {errno.EWOULDBLOCK, errno.EAGAIN}, exc
            else:
                raise AssertionError("expected flock exclusive conflict while first shared lock is held")
            fs.flock(file_path, flock_fh, fcntl.LOCK_UN)
            fs.flock(file_path, fh + 302, fcntl.LOCK_EX)
            fs.flock(file_path, fh + 302, fcntl.LOCK_UN)
        finally:
            try:
                fs.flock(file_path, second_fh, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                fs.flock(file_path, flock_fh, fcntl.LOCK_UN)
            except Exception:
                pass

        print("OK locking/flock")
    finally:
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
