#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.dbfs_mount import DBFSMount


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
    suffix = uuid.uuid4().hex[:8]
    block_size = _parse_bytes(os.environ.get("LARGE_COPY_BLOCK_SIZE", "4M"))
    block_count = int(os.environ.get("LARGE_COPY_BLOCK_COUNT", "16"))
    sync_mode = os.environ.get("LARGE_COPY_SYNC", "0").strip().lower() not in {"0", "false", "no", "off"}
    payload = (b"dbfs-large-copy-" * ((block_size * block_count) // 16 + 4))[: block_size * block_count]

    launcher = DBFSMount(str(ROOT))
    launcher.init_schema()

    with tempfile.TemporaryDirectory(prefix=f"/tmp/dbfs-large-copy-{suffix}.") as tmpdir:
        mountpoint = Path(tmpdir)
        launcher.start(str(mountpoint))
        try:
            dir_path = mountpoint / f"large-copy-{suffix}"
            src_path = dir_path / "src.bin"
            dst_path = dir_path / "dst.bin"

            dir_path.mkdir()
            src_path.write_bytes(payload)
            with src_path.open("rb") as src_fh, dst_path.open("wb") as dst_fh:
                start = time.perf_counter()
                offset = 0
                while offset < len(payload):
                    copied = os.copy_file_range(src_fh.fileno(), dst_fh.fileno(), len(payload) - offset)
                    if copied == 0:
                        break
                    offset += copied
                if offset != len(payload):
                    raise AssertionError((offset, len(payload)))
                if sync_mode:
                    os.fsync(dst_fh.fileno())
                elapsed = time.perf_counter() - start

            read_back = dst_path.read_bytes()
            if read_back != payload:
                raise AssertionError("large copy payload mismatch")

            throughput_mb_s = (len(payload) / 1024 / 1024) / elapsed if elapsed > 0 else 0.0
            print(
                f"OK large-copy-benchmark bytes={len(payload)} elapsed_s={elapsed:.6f} "
                f"throughput_mib_s={throughput_mb_s:.2f}"
            )
        finally:
            launcher.stop()


if __name__ == "__main__":
    main()
