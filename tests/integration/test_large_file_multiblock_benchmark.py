#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS


def _parse_bytes(value: str) -> int:
    value = value.strip()
    suffix = value[-1:].lower() if value else ""
    if suffix == "k":
        return int(value[:-1]) * 1024
    if suffix == "m":
        return int(value[:-1]) * 1024 * 1024
    if suffix == "g":
        return int(value[:-1]) * 1024 * 1024 * 1024
    return int(value)


def _profile_seconds(profile: dict, key: str) -> float:
    stats = profile.get(key)
    if stats is None:
        return 0.0
    return float(stats.get("seconds", 0.0))


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)
    fs.profile_io = True
    fs.write_flush_threshold_bytes = 1 << 60

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/large-file-{suffix}"
    file_path = f"{dir_path}/payload.bin"
    chunk_size = _parse_bytes(os.environ.get("LARGE_FILE_CHUNK_SIZE", "4M"))
    chunk_count = int(os.environ.get("LARGE_FILE_CHUNK_COUNT", "16"))
    payload = (b"dbfs-large-file-" * ((chunk_size * chunk_count) // 16 + 4))[: chunk_size * chunk_count]

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        start = time.perf_counter()
        offset = 0
        while offset < len(payload):
            chunk = payload[offset: offset + chunk_size]
            written = fs.write(file_path, chunk, offset, fh)
            if written != len(chunk):
                raise AssertionError((written, len(chunk)))
            offset += len(chunk)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fh = None
        elapsed = time.perf_counter() - start

        read_fh = fs.open(file_path, os.O_RDONLY)
        try:
            read_back = fs.read(file_path, len(payload), 0, read_fh)
        finally:
            fs.release(file_path, read_fh)
        if read_back != payload:
            raise AssertionError("large multi-block payload mismatch")

        write_seconds = _profile_seconds(fs._io_profile, "write")
        persist_seconds = _profile_seconds(fs._io_profile, "persist_buffer")
        flush_seconds = _profile_seconds(fs._io_profile, "flush")
        finalization_seconds = persist_seconds + flush_seconds
        throughput_mb_s = (len(payload) / 1024 / 1024) / elapsed if elapsed > 0 else 0.0
        print(
            "OK large-file-multiblock "
            f"bytes={len(payload)} elapsed_s={elapsed:.6f} throughput_mib_s={throughput_mb_s:.2f} "
            f"write_seconds={write_seconds:.6f} persist_seconds={persist_seconds:.6f} "
            f"flush_seconds={flush_seconds:.6f} finalization_seconds={finalization_seconds:.6f}"
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
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
