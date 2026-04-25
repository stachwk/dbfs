#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config


def profile_seconds(profile: dict, key: str) -> float:
    stats = profile.get(key)
    if stats is None:
        return 0.0
    return float(stats.get("seconds", 0.0))


def assert_count(profile: dict, key: str, expected: int) -> None:
    stats = profile.get(key)
    if stats is None:
        raise AssertionError(f"missing profile activity for {key}")
    actual = int(stats.get("count", 0))
    if actual != expected:
        raise AssertionError(f"unexpected profile count for {key}: expected={expected} actual={actual} stats={stats}")


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)
    fs.profile_io = True

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/truncate-release-{suffix}"
    file_path = f"{dir_path}/payload.txt"
    block_size = fs.block_size
    payload = (b"abcdefgh" * ((block_size * 16) // 8 + 8))[: block_size * 16]

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, payload, 0, fh)
        assert written == len(payload), (written, len(payload))
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        fs._io_profile.clear()

        fh = fs.open(file_path, os.O_RDWR)
        fs.truncate(file_path, 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None

        persist_seconds = profile_seconds(fs._io_profile, "persist_buffer")
        flush_seconds = profile_seconds(fs._io_profile, "flush")
        release_seconds = profile_seconds(fs._io_profile, "release")
        assert_count(fs._io_profile, "release", 1)
        finalization_seconds = persist_seconds + flush_seconds + release_seconds

        print(
            "OK truncate-release/profile "
            f"persist_seconds={persist_seconds:.6f} "
            f"flush_seconds={flush_seconds:.6f} "
            f"finalization_seconds={finalization_seconds:.6f}"
        )
    finally:
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
            fs.cleanup_resources()
        except Exception:
            pass


if __name__ == "__main__":
    main()
