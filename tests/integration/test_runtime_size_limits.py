#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_config import parse_size_bytes
from dbfs_fuse import DBFS


def build_fs(runtime_config: dict[str, object]) -> DBFS:
    fs = DBFS.__new__(DBFS)
    fs.runtime_config = runtime_config
    fs.default_max_fs_size_bytes = parse_size_bytes("10GiB")
    fs.backend = SimpleNamespace(
        rust_pg_query_scalar_text=lambda sql: runtime_config.get("pg_visible_path"),
    )
    fs.runtime_config_get = lambda key, default=None: runtime_config.get(key, default)
    fs.get_config_value = lambda key, default=None: default
    return fs


def main() -> None:
    assert parse_size_bytes("50GiB") == 50 * 1024**3
    assert parse_size_bytes("1TiB") == 1024**4
    assert parse_size_bytes("10GiB") == 10 * 1024**3
    assert parse_size_bytes("4096") == 4096
    assert parse_size_bytes("2gib") == 2 * 1024**3

    with tempfile.TemporaryDirectory() as temp_dir:
        stats = os.statvfs(temp_dir)
        visible_total_bytes = int(stats.f_frsize) * int(stats.f_blocks)

        huge_fs = build_fs(
            {
                "max_fs_size_bytes": "50TiB",
                "pg_visible_path": temp_dir,
            }
        )
        assert huge_fs.resolve_max_fs_size_bytes() == visible_total_bytes, (
            huge_fs.resolve_max_fs_size_bytes(),
            visible_total_bytes,
        )

        smaller_limit = max(1, visible_total_bytes // 2)
        small_fs = build_fs(
            {
                "max_fs_size_bytes": str(smaller_limit),
                "pg_visible_path": temp_dir,
            }
        )
        assert small_fs.resolve_max_fs_size_bytes() == smaller_limit, (
            small_fs.resolve_max_fs_size_bytes(),
            smaller_limit,
        )

        explicit_path_fs = build_fs(
            {
                "max_fs_size_bytes": "50GiB",
                "pg_visible_path": temp_dir,
            }
        )
        assert explicit_path_fs.resolve_pg_visible_fs_total_bytes() == visible_total_bytes

    print("OK runtime-size-limits")


if __name__ == "__main__":
    main()
