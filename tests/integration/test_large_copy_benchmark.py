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


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/large-copy-{suffix}"
    src_path = f"{dir_path}/src.bin"
    dst_path = f"{dir_path}/dst.bin"
    block_size = _parse_bytes(os.environ.get("LARGE_COPY_BLOCK_SIZE", "4M"))
    block_count = int(os.environ.get("LARGE_COPY_BLOCK_COUNT", "16"))
    payload = (b"dbfs-large-copy-" * ((block_size * block_count) // 16 + 4))[: block_size * block_count]

    src_fh = None
    dst_fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        src_fh = fs.create(src_path, 0o644)
        written = fs.write(src_path, payload, 0, src_fh)
        if written != len(payload):
            raise AssertionError((written, len(payload)))
        fs.flush(src_path, src_fh)
        fs.release(src_path, src_fh)
        src_fh = None

        dst_fh = fs.create(dst_path, 0o644)
        start = time.perf_counter()
        copied = fs.copy_file_range(src_path, None, 0, dst_path, dst_fh, 0, len(payload), 0)
        if copied != len(payload):
            raise AssertionError((copied, len(payload)))
        fs.flush(dst_path, dst_fh)
        fs.release(dst_path, dst_fh)
        dst_fh = None
        elapsed = time.perf_counter() - start

        read_fh = fs.open(dst_path, os.O_RDONLY)
        try:
            read_back = fs.read(dst_path, len(payload), 0, read_fh)
        finally:
            fs.release(dst_path, read_fh)
        if read_back != payload:
            raise AssertionError("large copy payload mismatch")

        throughput_mb_s = (len(payload) / 1024 / 1024) / elapsed if elapsed > 0 else 0.0
        print(
            f"OK large-copy-benchmark bytes={len(payload)} elapsed_s={elapsed:.6f} "
            f"throughput_mib_s={throughput_mb_s:.2f}"
        )
    finally:
        if dst_fh is not None:
            try:
                fs.release(dst_path, dst_fh)
            except Exception:
                pass
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        try:
            fs.unlink(dst_path)
        except Exception:
            pass
        try:
            fs.unlink(src_path)
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
