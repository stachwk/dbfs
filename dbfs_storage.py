from __future__ import annotations

import ctypes
import binascii
import os
import time
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from psycopg2.extras import execute_values


class DbfsCopySegment(ctypes.Structure):
    _fields_ = [
        ("src", ctypes.c_uint64),
        ("dst", ctypes.c_uint64),
        ("len", ctypes.c_uint64),
    ]


class DbfsRange(ctypes.Structure):
    _fields_ = [
        ("start", ctypes.c_uint64),
        ("end", ctypes.c_uint64),
    ]


class DbfsReadBlock(ctypes.Structure):
    _fields_ = [
        ("index", ctypes.c_uint64),
        ("ptr", ctypes.POINTER(ctypes.c_ubyte)),
        ("len", ctypes.c_size_t),
    ]


class DbfsReadSequenceStepResult(ctypes.Structure):
    _fields_ = [
        ("sequential", ctypes.c_ubyte),
        ("streak", ctypes.c_uint64),
    ]


class DbfsReadBounds(ctypes.Structure):
    _fields_ = [
        ("fetch_first", ctypes.c_uint64),
        ("fetch_last", ctypes.c_uint64),
    ]


class DbfsReadSlicePlan(ctypes.Structure):
    _fields_ = [
        ("total_blocks", ctypes.c_uint64),
        ("fetch_first", ctypes.c_uint64),
        ("fetch_last", ctypes.c_uint64),
    ]


class DbfsBlockTransferPlan(ctypes.Structure):
    _fields_ = [
        ("total_blocks", ctypes.c_uint64),
        ("parallel", ctypes.c_ubyte),
        ("workers", ctypes.c_uint64),
    ]


class DbfsParallelWorkerPlan(ctypes.Structure):
    _fields_ = [
        ("parallel", ctypes.c_ubyte),
        ("workers", ctypes.c_uint64),
    ]


class DbfsWriteCopyPlan(ctypes.Structure):
    _fields_ = [
        ("total_blocks", ctypes.c_uint64),
        ("dedupe_enabled", ctypes.c_ubyte),
        ("parallel", ctypes.c_ubyte),
        ("workers", ctypes.c_uint64),
    ]


class DbfsLogicalResizePlan(ctypes.Structure):
    _fields_ = [
        ("old_size", ctypes.c_uint64),
        ("new_size", ctypes.c_uint64),
        ("block_size", ctypes.c_uint64),
        ("old_total_blocks", ctypes.c_uint64),
        ("new_total_blocks", ctypes.c_uint64),
        ("shrinking", ctypes.c_ubyte),
        ("has_valid_blocks", ctypes.c_ubyte),
        ("delete_from_block", ctypes.c_uint64),
        ("max_valid_block", ctypes.c_uint64),
        ("has_partial_tail", ctypes.c_ubyte),
        ("tail_block_index", ctypes.c_uint64),
        ("tail_valid_len", ctypes.c_uint64),
    ]


class DbfsPersistLayoutPlan(ctypes.Structure):
    _fields_ = [
        ("total_blocks", ctypes.c_uint64),
        ("truncate_only", ctypes.c_ubyte),
    ]


class DbfsPersistBlockPlanEntry(ctypes.Structure):
    _fields_ = [
        ("block_index", ctypes.c_uint64),
        ("used_len", ctypes.c_uint64),
    ]


class DbfsPersistBlockInput(ctypes.Structure):
    _fields_ = [
        ("block_index", ctypes.c_uint64),
        ("ptr", ctypes.POINTER(ctypes.c_ubyte)),
        ("len", ctypes.c_size_t),
        ("used_len", ctypes.c_uint64),
    ]


class DbfsPersistCrcPlanEntry(ctypes.Structure):
    _fields_ = [
        ("block_index", ctypes.c_uint64),
        ("has_crc", ctypes.c_ubyte),
        ("crc32", ctypes.c_uint32),
    ]


class DbfsWriteTransferPlan:
    __slots__ = ("total_blocks", "dedupe_enabled", "parallel", "workers", "use_crc_table")

    def __init__(self, total_blocks, dedupe_enabled, parallel, workers, use_crc_table):
        self.total_blocks = int(total_blocks)
        self.dedupe_enabled = bool(dedupe_enabled)
        self.parallel = bool(parallel)
        self.workers = int(workers)
        self.use_crc_table = bool(use_crc_table)


class StorageSupport:
    PERSIST_BUFFER_CHUNK_BLOCKS = 128

    def __init__(self, owner):
        self.owner = owner

    def load_file_bytes(self, file_id):
        size = self.get_file_size(file_id)
        return self.read_file_slice(file_id, 0, size)

    def get_file_size(self, file_id):
        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT size FROM files WHERE id_file = %s",
                        (file_id,),
                    )
                    result = cur.fetchone()
                    return int(result[0]) if result else 0
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                continue

    def read_cache_limit_blocks(self):
        return max(1, int(getattr(self.owner, "read_cache_max_blocks", 256) or 256))

    def read_ahead_blocks(self):
        return max(0, int(getattr(self.owner, "read_ahead_blocks", 2) or 0))

    def small_file_threshold_blocks(self):
        return max(0, int(getattr(self.owner, "small_file_read_threshold_blocks", 8) or 0))

    def sequential_read_ahead_blocks(self):
        return max(0, int(getattr(self.owner, "sequential_read_ahead_blocks", 8) or 0))

    def clear_read_cache(self, file_id=None):
        with self.owner._read_block_cache_guard:
            if file_id is None:
                self.owner._read_block_cache.clear()
                return
            stale_keys = [key for key in self.owner._read_block_cache if key[0] == file_id]
            for key in stale_keys:
                self.owner._read_block_cache.pop(key, None)

    def _cached_block(self, file_id, block_index):
        cache_key = (file_id, block_index)
        with self.owner._read_block_cache_guard:
            block = self.owner._read_block_cache.get(cache_key)
            if block is not None:
                self.owner._read_block_cache.move_to_end(cache_key)
            return block

    def _store_cached_block(self, file_id, block_index, data):
        cache_key = (file_id, block_index)
        with self.owner._read_block_cache_guard:
            self.owner._read_block_cache[cache_key] = bytes(data)
            self.owner._read_block_cache.move_to_end(cache_key)
            while len(self.owner._read_block_cache) > self.read_cache_limit_blocks():
                self.owner._read_block_cache.popitem(last=False)

    def _missing_block_ranges(self, missing):
        if not missing:
            return []

        stepped = self.python_to_rust_hotpath_sorted_contiguous_ranges(missing)
        if stepped is not None:
            return stepped

        ranges = []
        range_start = missing[0]
        range_end = missing[0]
        for block_index in missing[1:]:
            if block_index == range_end + 1:
                range_end = block_index
                continue
            ranges.append((range_start, range_end))
            range_start = range_end = block_index
        ranges.append((range_start, range_end))
        return ranges

    def _fetch_block_range_chunk(self, file_id, first_block, last_block):
        result = {}
        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT _order, data
                        FROM data_blocks
                        WHERE id_file = %s AND _order BETWEEN %s AND %s
                        ORDER BY _order ASC
                        """,
                        (file_id, first_block, last_block),
                    )
                    for block_index, data in cur.fetchall():
                        result[block_index] = bytes(data)
                    return result
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                result = {}
                continue

    def _fetch_block_range(self, file_id, first_block, last_block):
        if last_block < first_block:
            return {}

        cached = {}
        missing = []
        for block_index in range(first_block, last_block + 1):
            block = self._cached_block(file_id, block_index)
            if block is None:
                missing.append(block_index)
            else:
                cached[block_index] = block

        if missing:
            workers_read = max(1, int(getattr(self.owner, "workers_read", 1) or 1))
            workers_read_min_blocks = max(1, int(getattr(self.owner, "workers_read_min_blocks", 8) or 8))
            contiguous_ranges = self._missing_block_ranges(missing)

            plan = self.python_to_rust_hotpath_read_missing_range_worker_plan(
                workers_read,
                workers_read_min_blocks,
                len(missing),
                len(contiguous_ranges),
            )
            if plan is None:
                if workers_read <= 1 or len(missing) < workers_read_min_blocks or len(contiguous_ranges) <= 1:
                    max_workers = 1
                else:
                    max_workers = max(1, min(workers_read, len(contiguous_ranges)))
            else:
                parallel, max_workers = plan
                if not parallel:
                    max_workers = 1

            if max_workers <= 1:
                fetched_maps = [self._fetch_block_range_chunk(file_id, missing[0], missing[-1])]
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(self._fetch_block_range_chunk, file_id, range_first, range_last)
                        for range_first, range_last in contiguous_ranges
                    ]
                    fetched_maps = [future.result() for future in futures]

            for fetched in fetched_maps:
                for block_index, data in fetched.items():
                    cached[block_index] = data
                    self._store_cached_block(file_id, block_index, data)

        return cached

    def _persist_block_chunks(self, cur, blocks):
        chunk_size = max(1, int(getattr(self.owner, "persist_buffer_chunk_blocks", self.PERSIST_BUFFER_CHUNK_BLOCKS) or self.PERSIST_BUFFER_CHUNK_BLOCKS))
        blocks = iter(blocks)
        first_block = next(blocks, None)
        if first_block is None:
            return
        second_block = next(blocks, None)
        if second_block is None:
            file_id, block_index, data = first_block
            cur.execute(
                """
                INSERT INTO data_blocks (id_file, _order, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_file, _order)
                DO UPDATE SET data = EXCLUDED.data
                """,
                (file_id, block_index, data),
            )
            return
        execute_values(
            cur,
            """
            INSERT INTO data_blocks (id_file, _order, data)
            VALUES %s
            ON CONFLICT (id_file, _order)
            DO UPDATE SET data = EXCLUDED.data
            """,
            chain((first_block, second_block), blocks),
            page_size=chunk_size,
        )

    def _persist_copy_block_crc_rows(self, cur, block_rows, block_size):
        if not block_rows:
            return

        file_id = int(block_rows[0][0])
        rust_persist_copy_block_crc_rows = getattr(
            self.owner.backend,
            "python_to_rust_pg_repo_persist_copy_block_crc_rows",
            None,
        )
        if rust_persist_copy_block_crc_rows is not None:
            if rust_persist_copy_block_crc_rows(file_id, block_size, block_rows):
                return

        crc_rows = []
        stale_rows = []
        for file_id, block_index, data, used_len in block_rows:
            if used_len >= block_size:
                crc_value = self.python_to_rust_hotpath_crc32(data)
                if crc_value is None:
                    crc_value = binascii.crc32(bytes(data)) & 0xFFFFFFFF
                crc_rows.append((file_id, block_index, crc_value))
            else:
                stale_rows.append((file_id, block_index))

        if crc_rows:
            chunk_size = max(1, int(getattr(self.owner, "persist_buffer_chunk_blocks", self.PERSIST_BUFFER_CHUNK_BLOCKS) or self.PERSIST_BUFFER_CHUNK_BLOCKS))
            execute_values(
                cur,
                """
                INSERT INTO copy_block_crc (id_file, _order, crc32)
                VALUES %s
                ON CONFLICT (id_file, _order)
                DO UPDATE SET crc32 = EXCLUDED.crc32, updated_at = NOW()
                """,
                crc_rows,
                page_size=chunk_size,
            )

        for file_id, block_index in stale_rows:
            cur.execute(
                """
                DELETE FROM copy_block_crc
                WHERE id_file = %s AND _order = %s
                """,
                (file_id, block_index),
            )

    def python_to_rust_hotpath_persist_pad_enabled(self):
        return bool(getattr(self.owner, "rust_hotpath_persist_pad", True))

    def python_to_rust_hotpath_read_assemble_enabled(self):
        return bool(getattr(self.owner, "rust_hotpath_read_assemble", True))

    def rust_hotpath_lib_path(self):
        raw_value = os.environ.get("DBFS_RUST_HOTPATH_LIB")
        candidates = []
        if raw_value:
            candidates.append(Path(raw_value))
        candidates.extend(
            [
                Path("/usr/local/lib/libdbfs-2.so"),
                Path("/usr/local/lib/libdbfs_rust_hotpath.so"),
                Path(__file__).resolve().parent
                / "rust_hotpath"
                / "target"
                / "debug"
                / "libdbfs_rust_hotpath.so",
                Path(__file__).resolve().parent
                / "rust_hotpath"
                / "target"
                / "release"
                / "libdbfs_rust_hotpath.so",
            ]
        )
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
        return None

    def _load_rust_hotpath_lib(self):
        cached = getattr(self, "_rust_hotpath_lib_handle", None)
        if cached is not None:
            return cached

        lib_path = self.rust_hotpath_lib_path()
        if lib_path is None:
            return None

        try:
            lib = ctypes.CDLL(lib_path)
        except OSError:
            return None

        lib.dbfs_copy_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.POINTER(DbfsCopySegment)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_copy_plan.restype = ctypes.c_int
        lib.dbfs_free_copy_segments.argtypes = [
            ctypes.POINTER(DbfsCopySegment),
            ctypes.c_size_t,
        ]
        lib.dbfs_free_copy_segments.restype = None
        lib.dbfs_copy_pack.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(DbfsRange)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_copy_pack.restype = ctypes.c_int
        lib.dbfs_copy_dedupe.argtypes = [
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(DbfsRange)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_copy_dedupe.restype = ctypes.c_int
        lib.dbfs_persist_pad.argtypes = [
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_persist_pad.restype = ctypes.c_int
        lib.dbfs_read_assemble.argtypes = [
            ctypes.POINTER(DbfsReadBlock),
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_read_assemble.restype = ctypes.c_int
        lib.dbfs_free_ranges.argtypes = [
            ctypes.POINTER(DbfsRange),
            ctypes.c_size_t,
        ]
        lib.dbfs_free_ranges.restype = None
        lib.dbfs_free_bytes.argtypes = [
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
        ]
        lib.dbfs_free_bytes.restype = None
        lib.dbfs_crc32.argtypes = [
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.c_size_t,
        ]
        lib.dbfs_crc32.restype = ctypes.c_uint32
        lib.dbfs_read_sequence_step.argtypes = [
            ctypes.c_ubyte,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_read_sequence_step.restype = DbfsReadSequenceStepResult
        lib.dbfs_read_ahead_blocks.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_read_ahead_blocks.restype = ctypes.c_uint64
        lib.dbfs_read_fetch_bounds.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_uint64,
            ctypes.POINTER(DbfsReadBounds),
        ]
        lib.dbfs_read_fetch_bounds.restype = ctypes.c_int
        lib.dbfs_read_slice_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_uint64,
            ctypes.POINTER(DbfsReadSlicePlan),
        ]
        lib.dbfs_read_slice_plan.restype = ctypes.c_int
        lib.dbfs_block_transfer_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_block_transfer_plan.restype = DbfsBlockTransferPlan
        lib.dbfs_read_missing_range_worker_count.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_read_missing_range_worker_count.restype = ctypes.c_uint64
        lib.dbfs_block_count_for_length.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
        ]
        lib.dbfs_block_count_for_length.restype = ctypes.c_uint64
        lib.dbfs_dirty_block_size.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_dirty_block_size.restype = ctypes.c_uint64
        lib.dbfs_logical_resize_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_logical_resize_plan.restype = DbfsLogicalResizePlan
        lib.dbfs_write_copy_worker_count.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_write_copy_worker_count.restype = ctypes.c_uint64
        lib.dbfs_parallel_worker_count.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_parallel_worker_count.restype = ctypes.c_uint64
        lib.dbfs_parallel_worker_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_parallel_worker_plan.restype = DbfsParallelWorkerPlan
        lib.dbfs_write_copy_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.dbfs_write_copy_plan.restype = DbfsWriteCopyPlan
        lib.dbfs_sorted_contiguous_ranges.argtypes = [
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(DbfsRange)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_sorted_contiguous_ranges.restype = ctypes.c_int
        lib.dbfs_dirty_block_ranges_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.POINTER(DbfsRange)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_dirty_block_ranges_plan.restype = ctypes.c_int
        lib.dbfs_persist_layout_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.POINTER(DbfsRange)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_persist_layout_plan.restype = ctypes.c_int
        lib.dbfs_persist_block_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_ubyte),
            ctypes.POINTER(ctypes.POINTER(DbfsPersistBlockPlanEntry)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_persist_block_plan.restype = ctypes.c_int
        lib.dbfs_persist_block_crc_plan.argtypes = [
            ctypes.c_uint64,
            ctypes.POINTER(DbfsPersistBlockInput),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(DbfsPersistCrcPlanEntry)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.dbfs_persist_block_crc_plan.restype = ctypes.c_int
        lib.dbfs_free_persist_blocks.argtypes = [
            ctypes.POINTER(DbfsPersistBlockPlanEntry),
            ctypes.c_size_t,
        ]
        lib.dbfs_free_persist_blocks.restype = None
        lib.dbfs_free_persist_crc_rows.argtypes = [
            ctypes.POINTER(DbfsPersistCrcPlanEntry),
            ctypes.c_size_t,
        ]
        lib.dbfs_free_persist_crc_rows.restype = None

        self._rust_hotpath_lib_handle = lib
        return lib

    def python_to_rust_hotpath_read_sequence_step(self, previous, offset):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        has_previous = 1 if previous else 0
        previous_last_end = int(previous.get("last_end", 0)) if previous else 0
        previous_streak = int(previous.get("streak", 0)) if previous else 0
        result = lib.dbfs_read_sequence_step(
            ctypes.c_ubyte(has_previous),
            ctypes.c_uint64(previous_last_end),
            ctypes.c_uint64(int(offset)),
            ctypes.c_uint64(previous_streak),
        )
        return bool(result.sequential), int(result.streak)

    def python_to_rust_hotpath_read_ahead_blocks(self, read_ahead_blocks, sequential_read_ahead_blocks, streak, read_cache_limit_blocks, sequential):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        return int(
            lib.dbfs_read_ahead_blocks(
                ctypes.c_uint64(int(read_ahead_blocks)),
                ctypes.c_uint64(int(sequential_read_ahead_blocks)),
                ctypes.c_uint64(int(streak)),
                ctypes.c_uint64(int(read_cache_limit_blocks)),
                ctypes.c_ubyte(1 if sequential else 0),
            )
        )

    def python_to_rust_hotpath_read_fetch_bounds(self, total_blocks, requested_first, requested_last, sequential, streak):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        out = DbfsReadBounds()
        rc = lib.dbfs_read_fetch_bounds(
            ctypes.c_uint64(int(total_blocks)),
            ctypes.c_uint64(int(requested_first)),
            ctypes.c_uint64(int(requested_last)),
            ctypes.c_uint64(int(self.read_ahead_blocks())),
            ctypes.c_uint64(int(self.sequential_read_ahead_blocks())),
            ctypes.c_uint64(int(streak)),
            ctypes.c_uint64(int(self.read_cache_limit_blocks())),
            ctypes.c_ubyte(1 if sequential else 0),
            ctypes.c_uint64(int(self.small_file_threshold_blocks())),
            ctypes.byref(out),
        )
        if rc != 0:
            return None
        return int(out.fetch_first), int(out.fetch_last)

    def python_to_rust_hotpath_read_slice_plan(self, file_size, offset, size, block_size, sequential, streak):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        out = DbfsReadSlicePlan()
        rc = lib.dbfs_read_slice_plan(
            ctypes.c_uint64(int(file_size)),
            ctypes.c_uint64(int(offset)),
            ctypes.c_uint64(int(size)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(self.read_ahead_blocks())),
            ctypes.c_uint64(int(self.sequential_read_ahead_blocks())),
            ctypes.c_uint64(int(streak)),
            ctypes.c_uint64(int(self.read_cache_limit_blocks())),
            ctypes.c_ubyte(1 if sequential else 0),
            ctypes.c_uint64(int(self.small_file_threshold_blocks())),
            ctypes.byref(out),
        )
        if rc != 0:
            return None
        return int(out.total_blocks), int(out.fetch_first), int(out.fetch_last)

    def python_to_rust_hotpath_block_transfer_plan(self, length, block_size, requested_workers, workers_min_blocks, minimum_one):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        result = lib.dbfs_block_transfer_plan(
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(requested_workers)),
            ctypes.c_uint64(int(workers_min_blocks)),
            ctypes.c_ubyte(1 if minimum_one else 0),
        )
        return int(result.total_blocks), bool(result.parallel), int(result.workers)

    def python_to_rust_hotpath_block_transfer_total_blocks(self, length, block_size, minimum_one):
        plan = self.python_to_rust_hotpath_block_transfer_plan(length, block_size, 1, 1, minimum_one)
        if plan is None:
            return None
        return int(plan[0])

    def python_to_rust_hotpath_read_missing_range_worker_count(self, workers_read, workers_read_min_blocks, missing_len, contiguous_ranges_len):
        plan = self.python_to_rust_hotpath_parallel_worker_plan(
            workers_read,
            workers_read_min_blocks,
            missing_len,
            contiguous_ranges_len,
        )
        if plan is None:
            return None
        return plan[1]

    def python_to_rust_hotpath_read_missing_range_worker_plan(self, workers_read, workers_read_min_blocks, missing_len, contiguous_ranges_len):
        return self.python_to_rust_hotpath_parallel_worker_plan(
            workers_read,
            workers_read_min_blocks,
            missing_len,
            contiguous_ranges_len,
        )

    def python_to_rust_hotpath_block_count_for_length(self, length, block_size, minimum_one):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        return int(
            lib.dbfs_block_count_for_length(
                ctypes.c_uint64(int(length)),
                ctypes.c_uint64(int(block_size)),
                ctypes.c_ubyte(1 if minimum_one else 0),
            )
        )

    def _block_count_for_length(self, length, block_size, minimum_one):
        count = self.python_to_rust_hotpath_block_count_for_length(length, block_size, minimum_one)
        if count is not None:
            return count
        if block_size <= 0 or length <= 0:
            return 1 if minimum_one else 0
        return max(1 if minimum_one else 0, (int(length) + int(block_size) - 1) // int(block_size))

    def python_to_rust_hotpath_dirty_block_size(self, file_size, block_index, block_size):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        return int(
            lib.dbfs_dirty_block_size(
                ctypes.c_uint64(int(file_size)),
                ctypes.c_uint64(int(block_index)),
                ctypes.c_uint64(int(block_size)),
            )
        )

    def python_to_rust_hotpath_write_copy_worker_count(self, total_blocks, workers_write, workers_write_min_blocks):
        plan = self.python_to_rust_hotpath_parallel_worker_plan(
            workers_write,
            workers_write_min_blocks,
            total_blocks,
            total_blocks,
        )
        if plan is None:
            return None
        return plan[1]

    def python_to_rust_hotpath_write_copy_worker_plan(self, total_blocks, workers_write, workers_write_min_blocks):
        return self.python_to_rust_hotpath_parallel_worker_plan(
            workers_write,
            workers_write_min_blocks,
            total_blocks,
            total_blocks,
        )

    def python_to_rust_hotpath_write_copy_plan(self, length, block_size, workers_write, workers_write_min_blocks):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        copy_dedupe_enabled = bool(getattr(self.owner, "copy_dedupe_enabled", False))
        copy_dedupe_min_blocks = max(1, int(getattr(self.owner, "copy_dedupe_min_blocks", 16) or 16))
        copy_dedupe_max_blocks = max(0, int(getattr(self.owner, "copy_dedupe_max_blocks", 0) or 0))

        result = lib.dbfs_write_copy_plan(
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(workers_write)),
            ctypes.c_uint64(int(workers_write_min_blocks)),
            ctypes.c_ubyte(1 if copy_dedupe_enabled else 0),
            ctypes.c_uint64(int(copy_dedupe_min_blocks)),
            ctypes.c_uint64(int(copy_dedupe_max_blocks)),
        )
        return (
            int(result.total_blocks),
            bool(result.dedupe_enabled),
            bool(result.parallel),
            int(result.workers),
        )

    def _block_transfer_plan(self, length, block_size, requested_workers, workers_min_blocks, minimum_one):
        plan = self.python_to_rust_hotpath_block_transfer_plan(
            length,
            block_size,
            requested_workers,
            workers_min_blocks,
            minimum_one,
        )
        if plan is not None:
            total_blocks, parallel, workers = plan
            return DbfsBlockTransferPlan(total_blocks, parallel, workers)

        total_blocks = self._block_count_for_length(length, block_size, minimum_one)
        if requested_workers <= 1 or total_blocks < workers_min_blocks:
            parallel = False
            workers = 1
        else:
            parallel = True
            workers = max(1, min(int(requested_workers), total_blocks))
        return DbfsBlockTransferPlan(total_blocks, parallel, workers)

    def _write_transfer_plan(self, length, block_size, workers_write, workers_write_min_blocks):
        base_plan = self._block_transfer_plan(length, block_size, workers_write, workers_write_min_blocks, True)
        plan = self.python_to_rust_hotpath_write_copy_plan(length, block_size, workers_write, workers_write_min_blocks)
        if plan is None:
            total_blocks = base_plan.total_blocks
            skip_unchanged_blocks = bool(getattr(self.owner, "copy_dedupe_enabled", False))
            skip_unchanged_blocks_min_blocks = max(1, int(getattr(self.owner, "copy_dedupe_min_blocks", 16) or 16))
            skip_unchanged_blocks_max_blocks = max(0, int(getattr(self.owner, "copy_dedupe_max_blocks", 0) or 0))
            dedupe_enabled = (
                skip_unchanged_blocks
                and total_blocks >= skip_unchanged_blocks_min_blocks
                and (skip_unchanged_blocks_max_blocks <= 0 or total_blocks <= skip_unchanged_blocks_max_blocks)
            )
            parallel = base_plan.parallel
            workers = base_plan.workers
        else:
            total_blocks, dedupe_enabled, parallel, workers = plan
            parallel = bool(parallel)
            workers = 1 if not parallel else int(workers)

        return DbfsWriteTransferPlan(
            total_blocks,
            dedupe_enabled,
            parallel,
            workers,
            self._copy_dedupe_crc_table_enabled(),
        )

    def python_to_rust_hotpath_parallel_worker_count(self, requested_workers, minimum_items_for_parallel, total_items, parallel_groups):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        return int(
            lib.dbfs_parallel_worker_count(
                ctypes.c_uint64(int(requested_workers)),
                ctypes.c_uint64(int(minimum_items_for_parallel)),
                ctypes.c_uint64(int(total_items)),
                ctypes.c_uint64(int(parallel_groups)),
            )
        )

    def python_to_rust_hotpath_parallel_worker_plan(self, requested_workers, minimum_items_for_parallel, total_items, parallel_groups):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        result = lib.dbfs_parallel_worker_plan(
            ctypes.c_uint64(int(requested_workers)),
            ctypes.c_uint64(int(minimum_items_for_parallel)),
            ctypes.c_uint64(int(total_items)),
            ctypes.c_uint64(int(parallel_groups)),
        )
        return bool(result.parallel), int(result.workers)

    def python_to_rust_hotpath_sorted_contiguous_ranges(self, values):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        values = [int(block_index) for block_index in values]
        if not values:
            return []

        values_array = (ctypes.c_uint64 * len(values))(*values)
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_sorted_contiguous_ranges(
            values_array,
            ctypes.c_size_t(len(values)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

    def python_to_rust_hotpath_dirty_block_ranges_plan(self, file_size, block_size, dirty_blocks):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        dirty_blocks = [int(block_index) for block_index in dirty_blocks]
        dirty_array = (ctypes.c_uint64 * len(dirty_blocks))(*dirty_blocks) if dirty_blocks else None
        out_total_blocks = ctypes.c_uint64()
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_dirty_block_ranges_plan(
            ctypes.c_uint64(int(file_size)),
            ctypes.c_uint64(int(block_size)),
            dirty_array,
            ctypes.c_size_t(len(dirty_blocks)),
            ctypes.byref(out_total_blocks),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            ranges = [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
            return int(out_total_blocks.value), ranges
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

    def python_to_rust_hotpath_persist_layout_plan(self, file_size, block_size, truncate_pending, dirty_blocks):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        dirty_blocks = [int(block_index) for block_index in dirty_blocks]
        dirty_array = (ctypes.c_uint64 * len(dirty_blocks))(*dirty_blocks) if dirty_blocks else None
        out_total_blocks = ctypes.c_uint64()
        out_truncate_only = ctypes.c_ubyte()
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_persist_layout_plan(
            ctypes.c_uint64(int(file_size)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_ubyte(1 if truncate_pending else 0),
            dirty_array,
            ctypes.c_size_t(len(dirty_blocks)),
            ctypes.byref(out_total_blocks),
            ctypes.byref(out_truncate_only),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            ranges = [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
            return int(out_total_blocks.value), bool(out_truncate_only.value), ranges
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

    def python_to_rust_hotpath_persist_block_plan(self, file_size, block_size, truncate_pending, dirty_blocks):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        dirty_blocks = [int(block_index) for block_index in dirty_blocks]
        dirty_array = (ctypes.c_uint64 * len(dirty_blocks))(*dirty_blocks) if dirty_blocks else None
        out_total_blocks = ctypes.c_uint64()
        out_truncate_only = ctypes.c_ubyte()
        out_ptr = ctypes.POINTER(DbfsPersistBlockPlanEntry)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_persist_block_plan(
            ctypes.c_uint64(int(file_size)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_ubyte(1 if truncate_pending else 0),
            dirty_array,
            ctypes.c_size_t(len(dirty_blocks)),
            ctypes.byref(out_total_blocks),
            ctypes.byref(out_truncate_only),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            blocks = [
                (int(out_ptr[i].block_index), int(out_ptr[i].used_len))
                for i in range(out_len.value)
            ]
            return int(out_total_blocks.value), bool(out_truncate_only.value), blocks
        finally:
            lib.dbfs_free_persist_blocks(out_ptr, out_len)

    def python_to_rust_hotpath_persist_block_crc_plan(self, block_size, block_rows):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        if not block_rows:
            return []

        payload_buffers = []
        inputs = []
        for block_index, data, used_len in block_rows:
            payload = bytes(data)
            buffer = ctypes.create_string_buffer(payload, len(payload))
            payload_buffers.append(buffer)
            inputs.append(
                DbfsPersistBlockInput(
                    block_index=ctypes.c_uint64(int(block_index)),
                    ptr=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)),
                    len=ctypes.c_size_t(len(payload)),
                    used_len=ctypes.c_uint64(int(used_len)),
                )
            )

        inputs_array = (DbfsPersistBlockInput * len(inputs))(*inputs)
        out_ptr = ctypes.POINTER(DbfsPersistCrcPlanEntry)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_persist_block_crc_plan(
            ctypes.c_uint64(int(block_size)),
            inputs_array,
            ctypes.c_size_t(len(inputs)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return [
                (
                    int(out_ptr[i].block_index),
                    bool(out_ptr[i].has_crc),
                    int(out_ptr[i].crc32),
                )
                for i in range(out_len.value)
            ]
        finally:
            lib.dbfs_free_persist_crc_rows(out_ptr, out_len)

    def python_to_rust_hotpath_copy_segments(self, off_in, off_out, length, block_size, workers):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        out_ptr = ctypes.POINTER(DbfsCopySegment)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_copy_plan(
            ctypes.c_uint64(int(off_in)),
            ctypes.c_uint64(int(off_out)),
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(workers)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return [
                (int(out_ptr[i].src), int(out_ptr[i].dst), int(out_ptr[i].len))
                for i in range(out_len.value)
            ]
        finally:
            lib.dbfs_free_copy_segments(out_ptr, out_len)

    def _ffi_ubyte_buffer(self, data):
        data = bytes(data)
        if not data:
            return None, None
        buffer = ctypes.create_string_buffer(data, len(data))
        return buffer, ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))

    def python_to_rust_hotpath_crc32(self, data):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        data = bytes(data)
        if not data:
            return 0

        buffer = ctypes.create_string_buffer(data, len(data))
        return int(lib.dbfs_crc32(ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)), len(data)))

    def python_to_rust_hotpath_persist_block_payload(self, payload, used_len, block_size):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        payload_buf, payload_ptr = self._ffi_ubyte_buffer(bytes(payload[:block_size]))
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_persist_pad(
            payload_ptr,
            ctypes.c_size_t(0 if payload_buf is None else len(payload_buf.raw)),
            ctypes.c_size_t(int(used_len)),
            ctypes.c_size_t(int(block_size)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return ctypes.string_at(out_ptr, out_len.value)
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def python_to_rust_hotpath_assemble_blocks(self, file_id, first_block, last_block):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        block_map = self._fetch_block_range(file_id, first_block, last_block)
        block_buffers = []
        blocks = []
        block_size = int(self.owner.block_size)
        for block_index in range(first_block, last_block + 1):
            block = bytes(block_map.get(block_index, b"\x00" * block_size))
            if len(block) < block_size:
                block = block + (b"\x00" * (block_size - len(block)))
            buffer, ptr = self._ffi_ubyte_buffer(block)
            block_buffers.append(buffer)
            blocks.append(DbfsReadBlock(index=block_index, ptr=ptr, len=len(block)))

        blocks_array = (DbfsReadBlock * len(blocks))(*blocks) if blocks else None
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_read_assemble(
            blocks_array,
            ctypes.c_size_t(len(blocks)),
            ctypes.c_uint64(int(first_block)),
            ctypes.c_uint64(int(last_block)),
            ctypes.c_uint64(0),
            ctypes.c_uint64(int((last_block - first_block + 1) * block_size)),
            ctypes.c_size_t(block_size),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return ctypes.string_at(out_ptr, out_len.value)
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)

    def python_to_rust_hotpath_pack_changed_copy_ranges(self, dst_offset, total_len, block_size, changed_mask):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        mask_bytes = bytes(1 if changed else 0 for changed in changed_mask)
        mask_buffer, mask_ptr = self._ffi_ubyte_buffer(mask_bytes)
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_copy_pack(
            ctypes.c_uint64(int(dst_offset)),
            ctypes.c_uint64(int(total_len)),
            ctypes.c_uint64(int(block_size)),
            mask_ptr,
            ctypes.c_size_t(0 if mask_buffer is None else len(mask_buffer.raw)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

    def python_to_rust_hotpath_copy_dedupe(self, dst_offset, payload, block_size, current_bytes):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return None

        payload_buffer, payload_ptr = self._ffi_ubyte_buffer(bytes(payload))
        current_buffer, current_ptr = self._ffi_ubyte_buffer(bytes(current_bytes))
        out_ptr = ctypes.POINTER(DbfsRange)()
        out_len = ctypes.c_size_t()

        rc = lib.dbfs_copy_dedupe(
            ctypes.c_uint64(int(dst_offset)),
            payload_ptr,
            ctypes.c_size_t(0 if payload_buffer is None else len(payload_buffer.raw)),
            current_ptr,
            ctypes.c_size_t(0 if current_buffer is None else len(current_buffer.raw)),
            ctypes.c_size_t(int(block_size)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            return None

        try:
            return [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

    def _persist_block_payload(self, payload, used_len, block_size):
        if self.python_to_rust_hotpath_persist_pad_enabled():
            ffi_result = self.python_to_rust_hotpath_persist_block_payload(payload, used_len, block_size)
            if ffi_result is not None and len(ffi_result) == max(1, int(block_size)):
                return ffi_result
        if used_len >= block_size:
            return memoryview(payload)[:block_size]
        return bytes(payload[:used_len]) + (b"\x00" * (block_size - used_len))

    def _copy_dedupe_crc_table_enabled(self):
        return bool(getattr(self.owner, "copy_dedupe_crc_table", False))

    def _copy_block_crc(self, file_id, block_index):
        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT crc32
                        FROM copy_block_crc
                        WHERE id_file = %s AND _order = %s
                        """,
                        (file_id, block_index),
                    )
                    result = cur.fetchone()
                    if result is not None:
                        return int(result[0])
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                continue

        current = self._read_copy_destination_chunk(file_id, block_index * self.owner.block_size, self.owner.block_size)
        crc_value = self.python_to_rust_hotpath_crc32(current)
        if crc_value is None:
            crc_value = binascii.crc32(bytes(current)) & 0xFFFFFFFF
        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO copy_block_crc (id_file, _order, crc32)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id_file, _order)
                        DO UPDATE SET crc32 = EXCLUDED.crc32, updated_at = NOW()
                        """,
                        (file_id, block_index, crc_value),
                    )
                    conn.commit()
                    return crc_value
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                continue
        return crc_value

    def _assemble_blocks(self, file_id, first_block, last_block):
        block_size = self.owner.block_size
        block_map = self._fetch_block_range(file_id, first_block, last_block)
        if self.python_to_rust_hotpath_read_assemble_enabled():
            ffi_result = self.python_to_rust_hotpath_assemble_blocks(file_id, first_block, last_block)
            if ffi_result is not None:
                return ffi_result

        chunks = []
        for block_index in range(first_block, last_block + 1):
            block = block_map.get(block_index)
            if block is None:
                block = b"\x00" * block_size
            chunks.append(block)
        return b"".join(chunks)

    def _record_read_sequence(self, file_id, offset, end_offset):
        with self.owner._read_sequence_guard:
            previous = self.owner._read_sequence_state.get(file_id)
            stepped = self.python_to_rust_hotpath_read_sequence_step(previous, offset)
            if stepped is None:
                sequential = bool(previous and previous.get("last_end") == offset)
                streak = (int(previous.get("streak", 0)) + 1) if sequential and previous else 0
            else:
                sequential, streak = stepped
            self.owner._read_sequence_state[file_id] = {"last_end": end_offset, "streak": streak}
        return sequential, streak

    def read_file_slice(self, file_id, offset, size):
        # Czyta dane z overlay lub z PostgreSQL, bez dodatkowego stanu write-path
        if size <= 0:
            return b""

        state = self.get_write_state(file_id)
        file_size = int(state["file_size"]) if state is not None else self.get_file_size(file_id)

        if offset >= file_size:
            return b""

        end_offset = min(file_size, offset + size)
        block_size = self.owner.block_size
        sequential, streak = self._record_read_sequence(file_id, offset, end_offset)

        plan = self.python_to_rust_hotpath_read_slice_plan(file_size, offset, size, block_size, sequential, streak)
        if plan is None:
            total_blocks = self._block_transfer_plan(file_size, block_size, 1, 1, False).total_blocks
            if total_blocks == 0:
                return b""

            requested_first = offset // block_size
            requested_last = max(requested_first, (end_offset - 1) // block_size)
            stepped = self.python_to_rust_hotpath_read_fetch_bounds(total_blocks, requested_first, requested_last, sequential, streak)
            if stepped is None:
                if total_blocks <= self.small_file_threshold_blocks():
                    fetch_first = 0
                    fetch_last = total_blocks - 1
                else:
                    fetch_first = requested_first
                    read_ahead_blocks = self.read_ahead_blocks()
                    stepped_read_ahead = self.python_to_rust_hotpath_read_ahead_blocks(
                        read_ahead_blocks,
                        self.sequential_read_ahead_blocks(),
                        streak,
                        self.read_cache_limit_blocks(),
                        sequential,
                    )
                    if stepped_read_ahead is not None:
                        read_ahead_blocks = stepped_read_ahead
                    else:
                        if sequential:
                            dynamic_ahead = self.sequential_read_ahead_blocks() * max(1, streak)
                            read_ahead_blocks = max(read_ahead_blocks, dynamic_ahead)
                        read_ahead_blocks = min(read_ahead_blocks, max(0, self.read_cache_limit_blocks() - 1))
                    fetch_last = min(total_blocks - 1, requested_last + read_ahead_blocks)
            else:
                fetch_first, fetch_last = stepped
        else:
            total_blocks, fetch_first, fetch_last = plan
            if total_blocks == 0:
                return b""

        if total_blocks == 0:
            return b""

        block_map = self._fetch_block_range(file_id, fetch_first, fetch_last)

        if self.python_to_rust_hotpath_read_assemble_enabled():
            ffi_result = self.python_to_rust_hotpath_assemble_blocks(file_id, fetch_first, fetch_last)
            if ffi_result is not None:
                return ffi_result

        chunks = []
        for block_index in range(fetch_first, fetch_last + 1):
            if state is not None and block_index in state["overlay_blocks"]:
                block = bytes(state["overlay_blocks"][block_index])
            else:
                block = block_map.get(block_index)
                if block is None:
                    block = b"\x00" * block_size
            chunks.append(block)

        raw = b"".join(chunks)
        start_offset = offset - (fetch_first * block_size)
        end_offset_in_raw = start_offset + (end_offset - offset)
        return raw[start_offset:end_offset_in_raw]

    def path_has_children(self, directory_id):
        rust_value = self.owner.backend.python_to_rust_storage_path_has_children(directory_id)
        if rust_value is not None:
            return rust_value

        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM files
                WHERE id_directory = %s
                UNION ALL
                SELECT 1
                FROM directories
                WHERE id_parent = %s
                UNION ALL
                SELECT 1
                FROM hardlinks
                WHERE id_directory = %s
                UNION ALL
                SELECT 1
                FROM symlinks
                WHERE id_parent = %s
                LIMIT 1
                """,
                (directory_id, directory_id, directory_id, directory_id),
            )
            return cur.fetchone() is not None

    def count_directory_children(self, directory_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM directories WHERE id_parent = %s)
                  + (SELECT COUNT(*) FROM files WHERE id_directory = %s)
                  + (SELECT COUNT(*) FROM hardlinks WHERE id_directory = %s)
                  + (SELECT COUNT(*) FROM symlinks WHERE id_parent = %s)
                """,
                (directory_id, directory_id, directory_id, directory_id),
            )
            return cur.fetchone()[0]

    def count_directory_subdirs(self, directory_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM directories WHERE id_parent = %s",
                (directory_id,),
            )
            return cur.fetchone()[0]

    def count_root_directory_children(self):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)
                FROM directories
                WHERE id_parent IS NULL AND name != '/'
                """,
            )
            return cur.fetchone()[0]

    def count_file_blocks(self, file_id):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM data_blocks WHERE id_file = %s",
                (file_id,),
            )
            return cur.fetchone()[0]

    def count_symlinks(self):
        with self.owner.db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM symlinks")
            return cur.fetchone()[0]

    def ensure_write_buffer(self, file_id):
        # Kompatybilnosc wsteczna - stara nazwa teraz zwraca stan zapisu
        return self.ensure_write_state(file_id)

    def mark_write_buffer_dirty(self, file_id):
        if file_id is None:
            return
        state = self.ensure_write_state(file_id)
        file_size = int(state["file_size"])
        block_size = self.owner.block_size
        total_blocks = self._block_transfer_plan(file_size, block_size, 1, 1, False).total_blocks
        for block_index in range(total_blocks):
            self._mark_dirty_block(state, block_index, file_size)

    def mark_write_range_dirty(self, file_id, start_offset, end_offset):
        if file_id is None or end_offset <= start_offset:
            return

        state = self.ensure_write_state(file_id)
        block_size = self.owner.block_size
        first_block = max(0, start_offset // block_size)
        last_block = max(0, (end_offset - 1) // block_size)
        file_size = int(state["file_size"])
        for block_index in range(first_block, last_block + 1):
            self._mark_dirty_block(state, block_index, file_size)

    def dirty_write_buffer_bytes(self, file_id):
        # Liczy tylko logiczne bajty dirty, bez pelnego bufora w RAM
        state = self.get_write_state(file_id)
        if state is None:
            return 0
        return int(state.get("dirty_bytes", 0))

    def maybe_flush_dirty_write_buffer(self, file_id):
        # Auto-flush przy przekroczeniu progu
        if file_id is None:
            return
        if self.owner.write_flush_threshold_bytes <= 0:
            return
        if self.dirty_write_buffer_bytes(file_id) < self.owner.write_flush_threshold_bytes:
            return
        self.persist_buffer(file_id)

    def clear_write_buffer_dirty(self, file_id):
        # Czysci liste dirty blokow
        state = self.get_write_state(file_id)
        if state is not None:
            state["dirty_blocks"].clear()
            state["dirty_block_bytes"].clear()
            state["dirty_bytes"] = 0
            state["truncate_pending"] = False

    def is_write_buffer_dirty(self, file_id):
        # Sprawdza czy sa dirty bloki
        state = self.get_write_state(file_id)
        return bool(state and (state["dirty_blocks"] or state.get("truncate_pending", False)))

    def persist_buffer(self, file_id):
        # Zapisuje tylko dirty bloki z overlay
        state = self.get_write_state(file_id)
        if state is None:
            return

        truncate_pending = bool(state.get("truncate_pending", False))
        dirty_blocks = state["dirty_blocks"]
        if not dirty_blocks and not truncate_pending:
            return

        file_size = int(state["file_size"])
        block_size = self.owner.block_size
        plan = self.python_to_rust_hotpath_persist_block_plan(file_size, block_size, truncate_pending, dirty_blocks)
        if plan is None:
            total_blocks = self._block_transfer_plan(file_size, block_size, 1, 1, False).total_blocks
            ordered_dirty_blocks = sorted({int(block_index) for block_index in dirty_blocks})
            ordered_dirty_plan = [
                (block_index, self._dirty_block_size(file_size, block_index, block_size))
                for block_index in ordered_dirty_blocks
                if block_index < total_blocks
            ]
            truncate_only = bool(truncate_pending and not dirty_blocks)
        else:
            total_blocks, truncate_only, ordered_dirty_plan = plan
        blocks_written = 0

        started = time.perf_counter()

        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    block_rows = []
                    if not truncate_only:
                        overlay_blocks = state["overlay_blocks"]
                        for block_index, used_len in ordered_dirty_plan:
                            if block_index >= total_blocks:
                                continue

                            payload = overlay_blocks.get(block_index)
                            if payload is None:
                                continue

                            data = self._persist_block_payload(payload, used_len, block_size)
                            block_rows.append((file_id, block_index, data, used_len))

                    rust_persist_file_blocks = getattr(
                        self.owner.backend,
                        "python_to_rust_pg_repo_persist_file_blocks",
                        None,
                    )
                    rust_persisted = False
                    if rust_persist_file_blocks is not None:
                        rust_persisted = rust_persist_file_blocks(
                            file_id,
                            file_size,
                            block_size,
                            total_blocks,
                            truncate_pending,
                            block_rows,
                        )
                    if rust_persisted:
                        blocks_written = len(block_rows)
                        conn.commit()
                        break

                    if truncate_pending:
                        if total_blocks == 0:
                            cur.execute(
                                """
                                DELETE FROM data_blocks
                                WHERE id_file = %s
                                """,
                                (file_id,),
                            )
                            cur.execute(
                                """
                                DELETE FROM copy_block_crc
                                WHERE id_file = %s
                                """,
                                (file_id,),
                            )
                        else:
                            cur.execute(
                                """
                                DELETE FROM data_blocks
                                WHERE id_file = %s AND _order >= %s
                                """,
                                (file_id, total_blocks),
                            )
                            cur.execute(
                                """
                                DELETE FROM copy_block_crc
                                WHERE id_file = %s AND _order >= %s
                                """,
                                (file_id, total_blocks),
                            )

                    if not truncate_only and block_rows:
                        blocks_written = len(block_rows)
                        self._persist_block_chunks(
                            cur,
                            ((file_id, block_index, data) for file_id, block_index, data, _ in block_rows),
                        )
                        self._persist_copy_block_crc_rows(cur, block_rows, block_size)

                    cur.execute(
                        """
                        UPDATE files
                        SET size = %s,
                            modification_date = NOW(),
                            {file_ctime} = NOW()
                        WHERE id_file = %s
                        """.format(file_ctime=self.owner.ctime_column("files")),
                        (file_size, file_id),
                    )

                    conn.commit()
                    break
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                continue

        elapsed = time.perf_counter() - started
        self.owner.record_io_profile(
            "persist_buffer",
            elapsed,
            bytes_count=file_size,
            blocks=blocks_written,
        )

        # Najbezpieczniejszy wariant: po flush usun stan z RAM
        self.drop_write_state(file_id)
        self.clear_read_cache(file_id)
        self.owner.invalidate_metadata_cache(include_statfs=True)

    def cleanup(self):
        # Czyci wszystkie stany tymczasowe
        if hasattr(self.owner, "_write_states"):
            self.owner._write_states.clear()
        self.clear_read_cache()
        self.owner.clear_read_sequence_state()

    def _write_states(self):
        # Lazily tworz kontener na stany zapisu
        if not hasattr(self.owner, "_write_states"):
            self.owner._write_states = {}
        return self.owner._write_states

    def ensure_write_state(self, file_id):
        # Tworzy pusty stan zapisu bez ladowania calego pliku
        states = self._write_states()
        state = states.get(file_id)
        if state is not None:
            return state

        state = {
            "file_size": self.get_file_size(file_id),
            "overlay_blocks": {},
            "dirty_blocks": set(),
            "dirty_block_bytes": {},
            "dirty_bytes": 0,
            "truncate_pending": False,
        }
        states[file_id] = state
        return state

    def get_write_state(self, file_id):
        # Zwraca stan zapisu lub None
        return self._write_states().get(file_id)

    def drop_write_state(self, file_id):
        # Usuwa stan zapisu po flush/release
        self._write_states().pop(file_id, None)

    def get_logical_file_size(self, file_id):
        # Zwraca logiczny rozmiar pliku widoczny dla aktywnego uchwytu
        state = self.get_write_state(file_id)
        if state is not None:
            return int(state["file_size"])
        return self.get_file_size(file_id)

    def load_block(self, file_id, block_index):
        # Laduje pojedynczy blok z cache lub z PostgreSQL
        cached = self._cached_block(file_id, block_index)
        if cached is not None:
            return cached

        conn = None
        row = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT data
                        FROM data_blocks
                        WHERE id_file = %s AND _order = %s
                        """,
                        (file_id, block_index),
                    )
                    row = cur.fetchone()
                    break
            except Exception as exc:
                if not self.owner.backend.is_transient_connection_error(exc) or attempt >= 1:
                    raise
                self.owner.backend.discard_connection(conn)
                continue

        if row is None:
            block = b"\x00" * self.owner.block_size
        else:
            block = bytes(row[0])
            if len(block) < self.owner.block_size:
                block = block + (b"\x00" * (self.owner.block_size - len(block)))

        self._store_cached_block(file_id, block_index, block)
        return block

    def _dirty_block_size(self, file_size, block_index):
        block_size = self.owner.block_size
        size = self.python_to_rust_hotpath_dirty_block_size(file_size, block_index, block_size)
        if size is not None:
            return size
        block_start = block_index * block_size
        block_end = min(int(file_size), block_start + block_size)
        return max(0, block_end - block_start)

    def _python_logical_resize_plan(self, old_size, new_size, block_size):
        plan = DbfsLogicalResizePlan()
        old_size = max(0, int(old_size))
        new_size = max(0, int(new_size))
        block_size = max(1, int(block_size))

        plan.old_size = old_size
        plan.new_size = new_size
        plan.block_size = block_size
        plan.old_total_blocks = self._block_count_for_length(old_size, block_size, False)
        plan.new_total_blocks = self._block_count_for_length(new_size, block_size, False)
        plan.shrinking = 1 if new_size < old_size else 0
        plan.has_valid_blocks = 1 if new_size > 0 else 0
        plan.max_valid_block = ((new_size - 1) // block_size) if new_size > 0 else 0
        plan.has_partial_tail = 1 if (new_size > 0 and (new_size % block_size) != 0) else 0
        plan.tail_block_index = (new_size // block_size) if plan.has_partial_tail else 0
        plan.tail_valid_len = (new_size % block_size) if plan.has_partial_tail else 0
        plan.delete_from_block = int(plan.new_total_blocks if plan.shrinking else plan.old_total_blocks)
        return plan

    def python_to_rust_hotpath_logical_resize_plan(self, old_size, new_size, block_size):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            return self._python_logical_resize_plan(old_size, new_size, block_size)

        return lib.dbfs_logical_resize_plan(
            ctypes.c_uint64(int(old_size)),
            ctypes.c_uint64(int(new_size)),
            ctypes.c_uint64(int(block_size)),
        )

    def _mark_dirty_block(self, state, block_index, file_size):
        dirty_blocks = state["dirty_blocks"]
        if block_index in dirty_blocks:
            return

        dirty_blocks.add(block_index)
        block_bytes = self._dirty_block_size(file_size, block_index)
        state["dirty_block_bytes"][block_index] = block_bytes
        state["dirty_bytes"] = int(state.get("dirty_bytes", 0)) + block_bytes

    def _refresh_dirty_block_bytes(self, state, block_index, file_size):
        dirty_block_bytes = state["dirty_block_bytes"]
        if block_index not in dirty_block_bytes:
            return

        old_bytes = dirty_block_bytes[block_index]
        new_bytes = self._dirty_block_size(file_size, block_index)
        if new_bytes == old_bytes:
            return

        dirty_block_bytes[block_index] = new_bytes
        state["dirty_bytes"] = int(state.get("dirty_bytes", 0)) + (new_bytes - old_bytes)

    def ensure_overlay_block(self, file_id, block_index):
        # Laduje tylko jeden blok do overlay
        state = self.ensure_write_state(file_id)
        overlay_blocks = state["overlay_blocks"]

        block = overlay_blocks.get(block_index)
        if block is not None:
            return block

        block = bytearray(self.load_block(file_id, block_index))
        overlay_blocks[block_index] = block
        return block

    def ensure_overlay_block_for_write(self, file_id, block_index, file_size_before_write=None):
        # Dla nowych zakresow poza EOF nie laduje bloku z PostgreSQL
        state = self.ensure_write_state(file_id)
        overlay_blocks = state["overlay_blocks"]

        block = overlay_blocks.get(block_index)
        if block is not None:
            return block

        block_start_abs = block_index * self.owner.block_size
        if file_size_before_write is not None and block_start_abs >= int(file_size_before_write):
            block = bytearray(self.owner.block_size)
        else:
            block = bytearray(self.load_block(file_id, block_index))

        overlay_blocks[block_index] = block
        return block

    def write_into_state(self, file_id, buf, offset):
        # Zapisuje dane do overlay blokow bez ladowania calego pliku
        state = self.ensure_write_state(file_id)
        file_size_before_write = int(state["file_size"])
        write_length = len(buf)
        end_offset = offset + write_length
        new_file_size = max(file_size_before_write, end_offset)
        block_size = self.owner.block_size

        first_block = offset // block_size
        last_block = max(first_block, (end_offset - 1) // block_size)

        src_pos = 0
        for block_index in range(first_block, last_block + 1):
            block_start_abs = block_index * block_size
            block_end_abs = block_start_abs + block_size
            block = self.ensure_overlay_block_for_write(file_id, block_index, file_size_before_write)

            write_start_abs = max(offset, block_start_abs)
            write_end_abs = min(end_offset, block_end_abs)

            block_start_rel = write_start_abs - block_start_abs
            block_end_rel = write_end_abs - block_start_abs

            chunk_len = write_end_abs - write_start_abs
            block[block_start_rel:block_end_rel] = buf[src_pos:src_pos + chunk_len]

            self._mark_dirty_block(state, block_index, new_file_size)
            src_pos += chunk_len

        if end_offset > state["file_size"]:
            state["file_size"] = end_offset
            if file_size_before_write > 0:
                self._refresh_dirty_block_bytes(
                    state,
                    max(0, (file_size_before_write - 1) // block_size),
                    state["file_size"],
                )

        return {
            "end_offset": end_offset,
            "first_block": first_block,
            "last_block": last_block,
            "touched_blocks": (last_block - first_block + 1),
        }

    def truncate_to_size(self, file_id, length):
        # Obsluguje truncate w modelu overlay
        if length < 0:
            raise ValueError("truncate length must be >= 0")

        state = self.ensure_write_state(file_id)
        old_size = int(state["file_size"])
        block_size = self.owner.block_size
        plan = self.python_to_rust_hotpath_logical_resize_plan(old_size, length, block_size)

        if length == old_size:
            return

        if plan.shrinking:
            state["truncate_pending"] = True

        state["file_size"] = int(plan.new_size)

        # Usun bloki calkowicie poza nowym EOF
        delete_from_block = int(plan.delete_from_block)
        if delete_from_block > 0:
            stale_blocks = [
                block_index
                for block_index in list(state["overlay_blocks"].keys())
                if block_index >= delete_from_block
            ]
        else:
            stale_blocks = list(state["overlay_blocks"].keys())
        for block_index in stale_blocks:
            state["overlay_blocks"].pop(block_index, None)
            if block_index in state["dirty_blocks"]:
                state["dirty_blocks"].discard(block_index)
                removed_bytes = state["dirty_block_bytes"].pop(block_index, 0)
                state["dirty_bytes"] = max(0, int(state.get("dirty_bytes", 0)) - int(removed_bytes))

        # Jesli skracamy do srodka bloku, wyzeruj ogon tego bloku
        if plan.has_partial_tail:
            last_block = int(plan.tail_block_index)
            block = self.ensure_overlay_block_for_write(file_id, last_block, state["file_size"])
            valid_len = int(plan.tail_valid_len)
            if valid_len < block_size:
                block[valid_len:] = b"\x00" * (block_size - valid_len)
            self._mark_dirty_block(state, last_block, state["file_size"])
            self._refresh_dirty_block_bytes(state, last_block, state["file_size"])

    def _copy_segments(self, off_in, off_out, length, block_size, workers):
        if length <= 0:
            return []

        if self.python_to_rust_hotpath_copy_plan_enabled():
            segments = self.python_to_rust_hotpath_copy_segments(
                off_in, off_out, length, block_size, workers
            )
            if segments is not None:
                return segments

        total_blocks = self._block_count_for_length(length, block_size, True)
        worker_count = max(1, min(int(workers), total_blocks))
        blocks_per_worker = max(1, (total_blocks + worker_count - 1) // worker_count)
        bytes_per_worker = blocks_per_worker * block_size

        segments = []
        src_cursor = int(off_in)
        dst_cursor = int(off_out)
        remaining = int(length)

        while remaining > 0:
            chunk_len = min(remaining, bytes_per_worker)
            segments.append((src_cursor, dst_cursor, chunk_len))
            src_cursor += chunk_len
            dst_cursor += chunk_len
            remaining -= chunk_len

        return segments

    def python_to_rust_hotpath_copy_plan_enabled(self):
        return bool(getattr(self.owner, "rust_hotpath_copy_plan", False))

    def _read_copy_destination_chunk(self, dst_file_id, dst_offset, length):
        current = self.read_file_slice(dst_file_id, dst_offset, length)
        if len(current) < length:
            current += b"\x00" * (length - len(current))
        return current

    def python_to_rust_hotpath_copy_pack_enabled(self):
        return bool(getattr(self.owner, "rust_hotpath_copy_pack", False))

    def python_to_rust_hotpath_copy_dedupe_enabled(self):
        return bool(getattr(self.owner, "rust_hotpath_copy_dedupe", False))

    def _pack_changed_copy_ranges(self, dst_offset, total_len, block_size, changed_mask):
        if self.python_to_rust_hotpath_copy_pack_enabled():
            ffi_result = self.python_to_rust_hotpath_pack_changed_copy_ranges(
                dst_offset, total_len, block_size, changed_mask
            )
            if ffi_result is not None:
                return ffi_result

        ranges = []
        run_start = None
        copy_end = int(dst_offset) + int(total_len)
        for block_index, changed in enumerate(changed_mask):
            block_start = int(dst_offset) + int(block_index * block_size)
            if changed:
                if run_start is None:
                    run_start = block_start
                continue
            if run_start is not None:
                ranges.append((run_start, block_start))
                run_start = None
        if run_start is not None:
            ranges.append((run_start, copy_end))
        return ranges

    def _write_copy_payload_if_changed(self, dst_file_id, dst_offset, payload):
        block_size = self.owner.block_size
        state = self.get_write_state(dst_file_id)
        current_size = int(state["file_size"]) if state is not None else self.get_file_size(dst_file_id)
        target_end = dst_offset + len(payload)

        if state is None and target_end > current_size:
            state = self.ensure_write_state(dst_file_id)
            current_size = int(state["file_size"])
        if state is not None and target_end > state["file_size"]:
            state["file_size"] = target_end

        if dst_offset >= current_size:
            self.write_into_state(dst_file_id, payload, dst_offset)
            return len(payload)

        plan = self._write_transfer_plan(len(payload), block_size, 1, 1)
        total_blocks = plan.total_blocks
        dedupe_enabled = plan.dedupe_enabled

        if dedupe_enabled and self.python_to_rust_hotpath_copy_dedupe_enabled():
            ffi_ranges = self.python_to_rust_hotpath_copy_dedupe(
                dst_offset,
                payload,
                block_size,
            b"".join(
                self._read_copy_destination_chunk(
                    dst_file_id,
                    dst_offset + rel_offset,
                    len(payload[rel_offset:rel_offset + block_size]),
                )
                for rel_offset in range(0, len(payload), block_size)
            ),
            )
            if ffi_ranges is not None:
                bytes_written = 0
                for run_start, run_end in ffi_ranges:
                    if run_end <= run_start:
                        continue
                    rel_start = run_start - dst_offset
                    rel_end = run_end - dst_offset
                    self.write_into_state(dst_file_id, payload[rel_start:rel_end], run_start)
                    bytes_written += rel_end - rel_start
                return bytes_written

        changed_mask = []
        use_crc_table = self._copy_dedupe_crc_table_enabled()
        dirty_blocks = set(state.get("dirty_blocks", [])) if state is not None else set()
        for rel_offset in range(0, len(payload), block_size):
            chunk = payload[rel_offset:rel_offset + block_size]
            dst_chunk_offset = dst_offset + rel_offset
            block_index = dst_chunk_offset // block_size
            if dedupe_enabled and use_crc_table and len(chunk) == block_size and block_index not in dirty_blocks:
                source_crc = self.python_to_rust_hotpath_crc32(chunk)
                if source_crc is None:
                    source_crc = binascii.crc32(bytes(chunk)) & 0xFFFFFFFF
                dest_crc = self._copy_block_crc(dst_file_id, block_index)
                changed_mask.append(source_crc != dest_crc)
            else:
                current = self._read_copy_destination_chunk(dst_file_id, dst_chunk_offset, len(chunk))
                changed_mask.append(current != chunk)

        ranges = self._pack_changed_copy_ranges(dst_offset, len(payload), block_size, changed_mask)

        bytes_written = 0
        for run_start, run_end in ranges:
            if run_end <= run_start:
                continue
            rel_start = run_start - dst_offset
            rel_end = run_end - dst_offset
            self.write_into_state(dst_file_id, payload[rel_start:rel_end], run_start)
            bytes_written += rel_end - rel_start

        return bytes_written

    def _read_segment_for_copy(self, src_file_id, src_offset, length):
        return self.read_file_slice(src_file_id, src_offset, length)

    def copy_file_range_into_state(self, src_file_id, dst_file_id, off_in, off_out, length):
        # Bezpieczny wariant workers_write:
        # czytanie segmentow moze byc rownolegle, ale zapis do overlay jest sekwencyjny
        if length <= 0:
            return 0

        block_size = self.owner.block_size
        workers_write = max(1, int(getattr(self.owner, "workers_write", 1) or 1))
        workers_write_min_blocks = max(1, int(getattr(self.owner, "workers_write_min_blocks", 8) or 8))
        plan = self._write_transfer_plan(length, block_size, workers_write, workers_write_min_blocks)
        total_blocks = plan.total_blocks
        dedupe_enabled = plan.dedupe_enabled
        parallel_workers = plan.workers

        if parallel_workers <= 1:
            chunk = self.read_file_slice(src_file_id, off_in, length)
            if not chunk:
                return 0
            if dedupe_enabled:
                self._write_copy_payload_if_changed(dst_file_id, off_out, chunk)
            else:
                self.write_into_state(dst_file_id, chunk, off_out)
            return len(chunk)

        segments = self._copy_segments(off_in, off_out, length, block_size, workers_write)
        max_workers = max(1, min(parallel_workers, len(segments)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._read_segment_for_copy, src_file_id, src_offset, chunk_len)
                for src_offset, _, chunk_len in segments
            ]
            payloads = [future.result() for future in futures]

        copied = 0
        for (_, dst_offset, _), payload in zip(segments, payloads):
            if not payload:
                continue
            if dedupe_enabled:
                self._write_copy_payload_if_changed(dst_file_id, dst_offset, payload)
            else:
                self.write_into_state(dst_file_id, payload, dst_offset)
            copied += len(payload)

        return copied
