#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_fuse import DBFS
from dbfs_schema import SCHEMA_VERSION


def main() -> None:
    dsn, db_config = load_dsn_from_config(ROOT)
    runtime_config = load_dbfs_runtime_config(ROOT)
    fs = DBFS(dsn, db_config, runtime_config=runtime_config)
    fs.profile_io = True
    crc_helper = fs.storage
    assert crc_helper._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    if fs.backend.schema_version() != SCHEMA_VERSION:
        raise AssertionError((fs.backend.schema_version(), SCHEMA_VERSION))

    suffix = uuid.uuid4().hex[:8]
    dir_path = f"/copy-crc-{suffix}"
    src_path = f"{dir_path}/src.bin"
    dst_path = f"{dir_path}/dst.bin"
    block_size = fs.block_size
    payload = (b"dbfs-copy-crc-" * ((block_size * 4) // 14 + 4))[: block_size * 4]

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
        written = fs.write(dst_path, payload, 0, dst_fh)
        if written != len(payload):
            raise AssertionError((written, len(payload)))
        fs.flush(dst_path, dst_fh)
        fs.release(dst_path, dst_fh)
        dst_fh = None

        if fs.get_file_size(fs.get_file_id(dst_path)) != len(payload):
            raise AssertionError((fs.get_file_size(fs.get_file_id(dst_path)), len(payload)))
        read_fh = fs.open(dst_path, os.O_RDONLY)
        try:
            if fs.read(dst_path, len(payload), 0, read_fh) != payload:
                raise AssertionError("destination payload mismatch before CRC copy")
        finally:
            fs.release(dst_path, read_fh)

        fs.copy_dedupe_enabled = True
        fs.copy_dedupe_min_blocks = 1
        fs.copy_dedupe_crc_table = True
        fs.rust_hotpath_copy_pack = False
        fs.rust_hotpath_copy_dedupe = False
        fs._io_profile.clear()

        dst_file_id = fs.get_file_id(dst_path)
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM copy_block_crc WHERE id_file = %s", (dst_file_id,))
            conn.commit()
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (dst_file_id,))
            initial_crc_rows = int(cur.fetchone()[0])
        if initial_crc_rows != 0:
            raise AssertionError(initial_crc_rows)

        dst_fh = fs.open(dst_path, os.O_WRONLY)
        try:
            copied = fs.copy_file_range(src_path, None, 0, dst_path, dst_fh, 0, len(payload), 0)
            if copied != len(payload):
                raise AssertionError((copied, len(payload)))
            fs.flush(dst_path, dst_fh)
            fs.release(dst_path, dst_fh)
            dst_fh = None
        finally:
            if dst_fh is not None:
                fs.release(dst_path, dst_fh)

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (dst_file_id,))
            after_first_copy = int(cur.fetchone()[0])
        expected_full_blocks = len(payload) // block_size
        if after_first_copy != expected_full_blocks:
            raise AssertionError((after_first_copy, expected_full_blocks))

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT _order, crc32 FROM copy_block_crc WHERE id_file = %s ORDER BY _order",
                (dst_file_id,),
            )
            crc_rows_after_first_copy = cur.fetchall()
        expected_source_crcs = [
            crc_helper._crc32_rust_ffi(payload[index * block_size : (index + 1) * block_size])
            for index in range(expected_full_blocks)
        ]
        if any(value is None for value in expected_source_crcs):
            raise AssertionError(expected_source_crcs)
        if [int(row[1]) for row in crc_rows_after_first_copy] != expected_source_crcs:
            raise AssertionError((crc_rows_after_first_copy, expected_source_crcs))

        mutated_block = bytearray(payload[:block_size])
        mutated_block[0] ^= 0xFF
        mutated_payload = bytes(mutated_block) + payload[block_size:]
        mutated_crc = crc_helper._crc32_rust_ffi(mutated_payload[:block_size])
        if mutated_crc is None:
            raise AssertionError("expected Rust CRC32 to be available")

        dst_fh = fs.open(dst_path, os.O_WRONLY)
        try:
            written = fs.write(dst_path, mutated_payload[:block_size], 0, dst_fh)
            if written != block_size:
                raise AssertionError((written, block_size))
            fs.flush(dst_path, dst_fh)
            fs.release(dst_path, dst_fh)
            dst_fh = None
        finally:
            if dst_fh is not None:
                fs.release(dst_path, dst_fh)

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT crc32 FROM copy_block_crc WHERE id_file = %s AND _order = 0",
                (dst_file_id,),
            )
            updated_crc = int(cur.fetchone()[0])
        if updated_crc != mutated_crc:
            raise AssertionError((updated_crc, mutated_crc))

        victim_path = f"{dir_path}/victim.bin"
        move_src_path = f"{dir_path}/move-src.bin"
        victim_seed_payload = b"seed-" * ((block_size * 2) // 5 + 4)
        victim_payload = b"victim-" * ((block_size * 2) // 7 + 4)
        victim_seed_fh = fs.create(victim_path, 0o644)
        try:
            written = fs.write(victim_path, victim_seed_payload[: block_size * 2], 0, victim_seed_fh)
            if written != block_size * 2:
                raise AssertionError((written, block_size * 2))
            fs.flush(victim_path, victim_seed_fh)
        finally:
            fs.release(victim_path, victim_seed_fh)
        victim_file_id = fs.get_file_id(victim_path)
        if victim_file_id is None:
            raise AssertionError("missing victim file id")
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (victim_file_id,))
            victim_crc_rows_before = int(cur.fetchone()[0])
        if victim_crc_rows_before != 2:
            raise AssertionError(victim_crc_rows_before)

        move_src_fh = fs.create(move_src_path, 0o644)
        try:
            written = fs.write(move_src_path, victim_payload[: block_size * 2], 0, move_src_fh)
            if written != block_size * 2:
                raise AssertionError((written, block_size * 2))
            fs.flush(move_src_path, move_src_fh)
        finally:
            fs.release(move_src_path, move_src_fh)
        move_src_file_id = fs.get_file_id(move_src_path)
        if move_src_file_id is None:
            raise AssertionError("missing move-src file id")
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (move_src_file_id,))
            move_src_crc_rows = int(cur.fetchone()[0])
        if move_src_crc_rows != 2:
            raise AssertionError(move_src_crc_rows)

        fs.rename(move_src_path, victim_path)
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (victim_file_id,))
            after_rename_crc_rows = int(cur.fetchone()[0])
        if after_rename_crc_rows != 0:
            raise AssertionError(after_rename_crc_rows)

        dst_fh = fs.open(dst_path, os.O_WRONLY)
        try:
            copied = fs.copy_file_range(src_path, None, 0, dst_path, dst_fh, 0, len(payload), 0)
            if copied != len(payload):
                raise AssertionError((copied, len(payload)))
            fs.flush(dst_path, dst_fh)
            fs.release(dst_path, dst_fh)
            dst_fh = None
        finally:
            if dst_fh is not None:
                fs.release(dst_path, dst_fh)

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (dst_file_id,))
            after_second_copy = int(cur.fetchone()[0])
        if after_second_copy != expected_full_blocks:
            raise AssertionError((after_second_copy, expected_full_blocks))

        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT _order, crc32 FROM copy_block_crc WHERE id_file = %s ORDER BY _order",
                (dst_file_id,),
            )
            crc_rows_after_second_copy = cur.fetchall()
        if [int(row[1]) for row in crc_rows_after_second_copy] != expected_source_crcs:
            raise AssertionError((crc_rows_after_second_copy, expected_source_crcs))

        read_fh = fs.open(dst_path, os.O_RDONLY)
        try:
            read_back = fs.read(dst_path, len(payload), 0, read_fh)
        finally:
            fs.release(dst_path, read_fh)
        if read_back != payload:
            raise AssertionError("copy block crc payload mismatch")

        fs.unlink(dst_path)
        with fs.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM copy_block_crc WHERE id_file = %s", (dst_file_id,))
            after_unlink_crc_rows = int(cur.fetchone()[0])
        if after_unlink_crc_rows != 0:
            raise AssertionError(after_unlink_crc_rows)

        print(
            "OK copy-block-crc-table "
            f"bytes={len(payload)} full_blocks={expected_full_blocks} crc_rows={after_second_copy}"
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
        for path in (dst_path, src_path):
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
