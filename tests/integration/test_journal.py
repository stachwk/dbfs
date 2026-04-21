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


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    fs = DBFS(dsn, db_config)

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/journal-{suffix}"
    file_path = f"{dir_path}/entry.txt"
    renamed_path = f"{dir_path}/entry-renamed.txt"
    current_uid = os.getuid() if hasattr(os, "getuid") else 0
    current_gid = os.getgid() if hasattr(os, "getgid") else 0
    supplementary_groups = [gid for gid in os.getgroups() if gid != current_gid]
    if not supplementary_groups:
        raise AssertionError("expected at least one supplementary group for journal chown coverage")
    alt_gid = supplementary_groups[0]

    fh = None
    try:
        fs.mkdir(dir_path, 0o755)
        fh = fs.create(file_path, 0o644)
        fs.write(file_path, b"journal", 0, fh)
        fs.flush(file_path, fh)
        fs.release(file_path, fh)
        fs.rename(file_path, renamed_path)
        fs.chmod(renamed_path, 0o600)
        fs.chown(renamed_path, current_uid, alt_gid)
        fs.truncate(renamed_path, 3)
        fs.unlink(renamed_path)
        fs.rmdir(dir_path)

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT action, id_user FROM journal WHERE action LIKE %s ORDER BY id_entry",
                (f"%{suffix}%",),
            )
            rows = cur.fetchall()
            actions = [row[0] for row in rows]
            assert all(row[1] == current_uid for row in rows), rows

        expected = ["mkdir", "create", "rename", "chmod", "chown", "truncate", "unlink", "rmdir"]
        for marker in expected:
            assert any(action.startswith(f"{marker}:") for action in actions), f"missing journal action: {marker}"

        print("OK journal")
    finally:
        if fh is not None:
            try:
                fs.release(file_path, fh)
            except Exception:
                pass
        try:
            fs.connection_pool.closeall()
        except Exception:
            pass


if __name__ == "__main__":
    main()
