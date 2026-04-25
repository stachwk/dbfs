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


def _make_payload(block_size: int, block_count: int) -> bytes:
    payload = os.urandom(block_size * block_count)
    return payload


def _make_mixed_destination(source_payload: bytes, block_size: int) -> bytes:
    buf = bytearray(source_payload)
    total_blocks = max(1, (len(source_payload) + block_size - 1) // block_size)
    for block_index in range(0, total_blocks, 2):
        start = block_index * block_size
        end = min(start + block_size, len(buf))
        fill_byte = (block_index * 17 + 73) % 251
        buf[start:end] = bytes([fill_byte]) * (end - start)
    return bytes(buf)


def _run_copy(fs, src_path, dst_path, src_len, rust_hotpath_copy_pack):
    fs.copy_dedupe_enabled = True
    fs.copy_dedupe_min_blocks = 1
    fs.rust_hotpath_copy_pack = rust_hotpath_copy_pack
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
    dir_path = f"/copy-pack-bench-{suffix}"
    src_path = f"{dir_path}/src.bin"
    dst_py_path = f"{dir_path}/dst-python.bin"
    dst_rs_path = f"{dir_path}/dst-rust.bin"
    block_size = _parse_bytes(os.environ.get("COPY_PACK_BLOCK_SIZE", "512K"))
    block_count = int(os.environ.get("COPY_PACK_BLOCK_COUNT", "8"))
    source_payload = _make_payload(block_size, block_count)
    mixed_payload = _make_mixed_destination(source_payload, block_size)

    src_fh = None
    dst_py_fh = None
    dst_rs_fh = None
    try:
        fs.mkdir(dir_path, 0o755)

        src_fh = fs.create(src_path, 0o644)
        written = fs.write(src_path, source_payload, 0, src_fh)
        if written != len(source_payload):
            raise AssertionError((written, len(source_payload)))
        fs.flush(src_path, src_fh)
        fs.release(src_path, src_fh)
        src_fh = None

        dst_py_fh = fs.create(dst_py_path, 0o644)
        written = fs.write(dst_py_path, mixed_payload, 0, dst_py_fh)
        if written != len(mixed_payload):
            raise AssertionError((written, len(mixed_payload)))
        fs.flush(dst_py_path, dst_py_fh)
        fs.release(dst_py_path, dst_py_fh)
        dst_py_fh = None

        dst_rs_fh = fs.create(dst_rs_path, 0o644)
        written = fs.write(dst_rs_path, mixed_payload, 0, dst_rs_fh)
        if written != len(mixed_payload):
            raise AssertionError((written, len(mixed_payload)))
        fs.flush(dst_rs_path, dst_rs_fh)
        fs.release(dst_rs_path, dst_rs_fh)
        dst_rs_fh = None

        py_result = _run_copy(fs, src_path, dst_py_path, len(source_payload), False)
        rs_result = _run_copy(fs, src_path, dst_rs_path, len(source_payload), True)

        if py_result["read_back"] != source_payload:
            raise AssertionError("copy-pack/python payload mismatch")
        if rs_result["read_back"] != source_payload:
            raise AssertionError("copy-pack/rust payload mismatch")

        throughput_py = (len(source_payload) / 1024 / 1024) / py_result["elapsed"] if py_result["elapsed"] > 0 else 0.0
        throughput_rs = (len(source_payload) / 1024 / 1024) / rs_result["elapsed"] if rs_result["elapsed"] > 0 else 0.0
        print(
            "OK copy-pack/python "
            f"bytes={len(source_payload)} elapsed_s={py_result['elapsed']:.6f} throughput_mib_s={throughput_py:.2f} "
            f"write_seconds={py_result['write_seconds']:.6f} persist_seconds={py_result['persist_seconds']:.6f} "
            f"flush_seconds={py_result['flush_seconds']:.6f} finalization_seconds={py_result['finalization_seconds']:.6f}"
        )
        print(
            "OK copy-pack/rust "
            f"bytes={len(source_payload)} elapsed_s={rs_result['elapsed']:.6f} throughput_mib_s={throughput_rs:.2f} "
            f"write_seconds={rs_result['write_seconds']:.6f} persist_seconds={rs_result['persist_seconds']:.6f} "
            f"flush_seconds={rs_result['flush_seconds']:.6f} finalization_seconds={rs_result['finalization_seconds']:.6f}"
        )
    finally:
        if dst_py_fh is not None:
            try:
                fs.release(dst_py_path, dst_py_fh)
            except Exception:
                pass
        if dst_rs_fh is not None:
            try:
                fs.release(dst_rs_path, dst_rs_fh)
            except Exception:
                pass
        if src_fh is not None:
            try:
                fs.release(src_path, src_fh)
            except Exception:
                pass
        for path in (dst_py_path, dst_rs_path, src_path):
            try:
                fs.unlink(path)
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
