#!/usr/bin/env python3

from __future__ import annotations

import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def assert_missing(profile: dict, key: str) -> None:
    stats = profile.get(key)
    if stats is None:
        return
    if stats.get("count", 0) != 0:
        raise AssertionError(f"unexpected profile activity for {key}: {stats}")


def assert_count(profile: dict, key: str, expected: int) -> None:
    stats = profile.get(key)
    if stats is None:
        raise AssertionError(f"missing profile activity for {key}")
    actual = int(stats.get("count", 0))
    if actual != expected:
        raise AssertionError(f"unexpected profile count for {key}: expected={expected} actual={actual} stats={stats}")


def profile_seconds(profile: dict, key: str) -> float:
    stats = profile.get(key)
    if stats is None:
        return 0.0
    return float(stats.get("seconds", 0.0))


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)
    fs.profile_io = True

    suffix = uuid.uuid4().hex[:8]
    clean_dir = f"/flush-clean-{suffix}"
    clean_file = f"{clean_dir}/empty.txt"
    dirty_dir = f"/flush-dirty-{suffix}"
    dirty_file = f"{dirty_dir}/payload.txt"
    fh = None
    dirty_fh = None
    try:
        fs.mkdir(clean_dir, 0o755)
        fh = fs.create(clean_file, 0o644)
        fs.flush(clean_file, fh)
        fs.release(clean_file, fh)
        assert_missing(fs._io_profile, "persist_buffer")
        assert_missing(fs._io_profile, "flush")
        assert_missing(fs._io_profile, "release")

        fs.mkdir(dirty_dir, 0o755)
        dirty_fh = fs.create(dirty_file, 0o644)
        written = fs.write(dirty_file, b"flush-release-profile", 0, dirty_fh)
        if written != len(b"flush-release-profile"):
            raise AssertionError(f"unexpected write length: {written}")
        fs.flush(dirty_file, dirty_fh)
        fs.release(dirty_file, dirty_fh)

        assert_count(fs._io_profile, "write", 1)
        assert_count(fs._io_profile, "persist_buffer", 1)
        assert_count(fs._io_profile, "flush", 1)
        assert_missing(fs._io_profile, "release")

        write_seconds = profile_seconds(fs._io_profile, "write")
        persist_seconds = profile_seconds(fs._io_profile, "persist_buffer")
        flush_seconds = profile_seconds(fs._io_profile, "flush")
        release_seconds = profile_seconds(fs._io_profile, "release")
        finalization_seconds = persist_seconds + flush_seconds + release_seconds

        print(
            "OK flush-release/profile "
            f"write_seconds={write_seconds:.6f} "
            f"persist_seconds={persist_seconds:.6f} "
            f"flush_seconds={flush_seconds:.6f} "
            f"finalization_seconds={finalization_seconds:.6f}"
        )
    finally:
        if dirty_fh is not None:
            try:
                fs.release(dirty_file, dirty_fh)
            except Exception:
                pass
        if fh is not None:
            try:
                fs.release(clean_file, fh)
            except Exception:
                pass
        try:
            fs.unlink(dirty_file)
        except Exception:
            pass
        try:
            fs.rmdir(dirty_dir)
        except Exception:
            pass
        try:
            fs.unlink(clean_file)
        except Exception:
            pass
        try:
            fs.rmdir(clean_dir)
        except Exception:
            pass
        try:
            fs.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
