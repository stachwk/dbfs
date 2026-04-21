#!/usr/bin/env python3

import os
import subprocess
import sys
import secrets
import uuid
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


def ensure_schema_ready() -> None:
    schema_password = os.environ.get("DBFS_SCHEMA_ADMIN_PASSWORD") or secrets.token_urlsafe(24)
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
        "Unable to bootstrap DBFS schema for block-read test:\n"
        f"{combined_output}".strip()
    )


def main():
    ensure_schema_ready()
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/blockread_{suffix}"
    file_path = f"{dir_path}/payload.bin"
    block_size = fs.block_size
    payload_size = (block_size * 3) + 321
    pattern = (b"0123456789abcdef" * ((payload_size // 16) + 2))[:payload_size]

    fh = None
    original_load = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        written = fs.write(file_path, pattern, 0, fh)
        assert written == len(pattern), f"write returned {written}, expected {len(pattern)}"
        fs.flush(file_path, fh)
        fs.release(file_path, fh)

        fh = fs.open(file_path, os.O_RDONLY)
        original_load = fs.load_file_bytes

        def fail_load(_file_id):
            raise AssertionError("read() unexpectedly fell back to load_file_bytes()")

        fs.load_file_bytes = fail_load

        offset = block_size - 7
        size = block_size + 33
        chunk = fs.read(file_path, size, offset, fh)
        assert chunk == pattern[offset : offset + size], "partial read mismatch"

        tail_offset = len(pattern) - 17
        tail = fs.read(file_path, 64, tail_offset, fh)
        assert tail == pattern[tail_offset:], "tail read mismatch"

        fs.load_file_bytes = original_load
        print("OK block-read/range")
    finally:
        if original_load is not None:
            fs.load_file_bytes = original_load
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


if __name__ == "__main__":
    main()
