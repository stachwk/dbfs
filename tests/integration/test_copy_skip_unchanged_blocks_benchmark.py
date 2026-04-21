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


def _run_copy(fs, src_path, dst_path, src_len, skip_unchanged_blocks):
    fs.copy_skip_unchanged_blocks = skip_unchanged_blocks
    fs.copy_skip_unchanged_blocks_min_blocks = 1
    fs._io_profile.clear()

    dst_fh = fs.open(dst_path, os.O_WRONLY)
    try:
        start = time.perf_counter()
        copied = fs.copy_file_range(src_path, None, 0, dst_path, dst_fh, 0, src_len, 0)
        if copied != src_len:
            raise AssertionError((copied, src_len))
        fs.flush(dst_path, dst_fh)
        fs.release(dst_path, dst_fh)
        dst_fh = None
        elapsed = time.perf_counter() - start
    finally:
        if dst_fh is not None:
            fs.release(dst_path, dst_fh)

    read_fh = fs.open(dst_path, os.O_RDONLY)
    try:
        read_back = fs.read(dst_path, src_len, 0, read_fh)
    finally:
        fs.release(dst_path, read_fh)
    return {
        "elapsed": elapsed,
        "read_back": read_back,
        "write_seconds": _profile_seconds(fs._io_profile, "write"),
        "persist_seconds": _profile_seconds(fs._io_profile, "persist_buffer"),
        "flush_seconds": _profile_seconds(fs._io_profile, "flush"),
        "finalization_seconds": _profile_seconds(fs._io_profile, "persist_buffer")
        + _profile_seconds(fs._io_profile, "flush"),
    }


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)
    fs.profile_io = True

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/copy-dedupe-{suffix}"
    src_path = f"{dir_path}/src.bin"
    dst_off_path = f"{dir_path}/dst-off.bin"
    dst_on_path = f"{dir_path}/dst-on.bin"
    block_size = _parse_bytes(os.environ.get("COPY_DEDUPE_BLOCK_SIZE", "4M"))
    block_count = int(os.environ.get("COPY_DEDUPE_BLOCK_COUNT", "16"))
    payload = (b"dbfs-copy-dedupe-" * ((block_size * block_count) // 17 + 4))[: block_size * block_count]

    src_fh = None
    dst_off_fh = None
    dst_on_fh = None
    try:
        fs.mkdir(dir_path, 0o755)

        src_fh = fs.create(src_path, 0o644)
        written = fs.write(src_path, payload, 0, src_fh)
        if written != len(payload):
            raise AssertionError((written, len(payload)))
        fs.flush(src_path, src_fh)
        fs.release(src_path, src_fh)
        src_fh = None

        dst_off_fh = fs.create(dst_off_path, 0o644)
        written = fs.write(dst_off_path, payload, 0, dst_off_fh)
        if written != len(payload):
            raise AssertionError((written, len(payload)))
        fs.flush(dst_off_path, dst_off_fh)
        fs.release(dst_off_path, dst_off_fh)
        dst_off_fh = None

        dst_on_fh = fs.create(dst_on_path, 0o644)
        written = fs.write(dst_on_path, payload, 0, dst_on_fh)
        if written != len(payload):
            raise AssertionError((written, len(payload)))
        fs.flush(dst_on_path, dst_on_fh)
        fs.release(dst_on_path, dst_on_fh)
        dst_on_fh = None

        off_result = _run_copy(fs, src_path, dst_off_path, len(payload), False)
        on_result = _run_copy(fs, src_path, dst_on_path, len(payload), True)

        if off_result["read_back"] != payload:
            raise AssertionError("copy dedupe off payload mismatch")
        if on_result["read_back"] != payload:
            raise AssertionError("copy dedupe on payload mismatch")

        throughput_off = (len(payload) / 1024 / 1024) / off_result["elapsed"] if off_result["elapsed"] > 0 else 0.0
        throughput_on = (len(payload) / 1024 / 1024) / on_result["elapsed"] if on_result["elapsed"] > 0 else 0.0
        print(
            "OK copy-skip-unchanged-blocks/off "
            f"bytes={len(payload)} elapsed_s={off_result['elapsed']:.6f} throughput_mib_s={throughput_off:.2f} "
            f"write_seconds={off_result['write_seconds']:.6f} persist_seconds={off_result['persist_seconds']:.6f} "
            f"flush_seconds={off_result['flush_seconds']:.6f} finalization_seconds={off_result['finalization_seconds']:.6f}"
        )
        print(
            "OK copy-skip-unchanged-blocks/on "
            f"bytes={len(payload)} elapsed_s={on_result['elapsed']:.6f} throughput_mib_s={throughput_on:.2f} "
            f"write_seconds={on_result['write_seconds']:.6f} persist_seconds={on_result['persist_seconds']:.6f} "
            f"flush_seconds={on_result['flush_seconds']:.6f} finalization_seconds={on_result['finalization_seconds']:.6f}"
        )
    finally:
        if dst_off_fh is not None:
            try:
                fs.release(dst_off_path, dst_off_fh)
            except Exception:
                pass
        if dst_on_fh is not None:
            try:
                fs.release(dst_on_path, dst_on_fh)
            except Exception:
                pass
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        try:
            fs.unlink(dst_off_path)
        except Exception:
            pass
        try:
            fs.unlink(dst_on_path)
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
