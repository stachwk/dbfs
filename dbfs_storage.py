from __future__ import annotations

import ctypes
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


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
    __slots__ = ("total_blocks", "dedupe_enabled", "parallel", "workers")

    def __init__(self, total_blocks, dedupe_enabled, parallel, workers):
        self.total_blocks = int(total_blocks)
        self.dedupe_enabled = bool(dedupe_enabled)
        self.parallel = bool(parallel)
        self.workers = int(workers)


class StorageSupport:
    PERSIST_BUFFER_CHUNK_BLOCKS = 128

    def __init__(self, owner):
        self.owner = owner

    def load_file_bytes(self, file_id):
        size = self.get_file_size(file_id)
        return self.read_file_slice(file_id, 0, size)

    def get_file_size(self, file_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_file_size(
            repo,
            int(file_id),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

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

        lib = self._load_rust_hotpath_lib()
        if lib is not None:
            values = [int(block_index) for block_index in missing]
            if values:
                values_array = (ctypes.c_uint64 * len(values))(*values)
                out_ptr = ctypes.POINTER(DbfsRange)()
                out_len = ctypes.c_size_t()
                rc = lib.dbfs_sorted_contiguous_ranges(
                    values_array,
                    ctypes.c_size_t(len(values)),
                    ctypes.byref(out_ptr),
                    ctypes.byref(out_len),
                )
                if rc == 0:
                    try:
                        return [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
                    finally:
                        lib.dbfs_free_ranges(out_ptr, out_len)

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
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            return {}
        out_ptr = ctypes.POINTER(DbfsReadBlock)()
        out_len = ctypes.c_size_t()
        status = lib.dbfs_rust_pg_repo_fetch_block_range(
            repo,
            int(file_id),
            int(first_block),
            int(last_block),
            int(self.owner.block_size),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if status != 0:
            return {}
        try:
            result = {}
            for index in range(out_len.value):
                block = out_ptr[index]
                result[int(block.index)] = ctypes.string_at(block.ptr, block.len)
            return result
        finally:
            lib.dbfs_free_read_blocks(out_ptr, out_len)

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

            lib = self._load_rust_hotpath_lib()
            if lib is None:
                raise FuseOSError(errno.EIO)
            result = lib.dbfs_parallel_worker_plan(
                ctypes.c_uint64(int(workers_read)),
                ctypes.c_uint64(int(workers_read_min_blocks)),
                ctypes.c_uint64(int(len(missing))),
                ctypes.c_uint64(int(len(contiguous_ranges))),
            )
            plan = (bool(result.parallel), int(result.workers))
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
        lib.dbfs_rust_pg_repo_persist_copy_block_crc_rows.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(DbfsPersistBlockInput),
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_persist_copy_block_crc_rows.restype = ctypes.c_int
        lib.dbfs_rust_pg_repo_persist_file_blocks.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_ubyte,
            ctypes.POINTER(DbfsPersistBlockInput),
            ctypes.c_size_t,
        ]
        lib.dbfs_rust_pg_repo_persist_file_blocks.restype = ctypes.c_int
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

    def _block_count_for_length(self, length, block_size, minimum_one):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
        return int(
            lib.dbfs_block_count_for_length(
                ctypes.c_uint64(int(length)),
                ctypes.c_uint64(int(block_size)),
                ctypes.c_ubyte(1 if minimum_one else 0),
            )
        )

    def _block_transfer_plan(self, length, block_size, requested_workers, workers_min_blocks, minimum_one):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)

        result = lib.dbfs_block_transfer_plan(
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(requested_workers)),
            ctypes.c_uint64(int(workers_min_blocks)),
            ctypes.c_ubyte(1 if minimum_one else 0),
        )
        total_blocks = int(result.total_blocks)
        parallel = bool(result.parallel)
        workers = int(result.workers)
        return DbfsBlockTransferPlan(total_blocks, parallel, workers)

    def _write_transfer_plan(self, length, block_size, workers_write, workers_write_min_blocks):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)

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
        total_blocks = int(result.total_blocks)
        dedupe_enabled = bool(result.dedupe_enabled)
        parallel = bool(result.parallel)
        workers = int(result.workers)
        parallel = bool(parallel)
        workers = 1 if not parallel else int(workers)
        return DbfsWriteTransferPlan(
            total_blocks,
            dedupe_enabled,
            parallel,
            workers,
        )

    def _ffi_ubyte_buffer(self, data):
        data = bytes(data)
        if not data:
            return None, None
        buffer = ctypes.create_string_buffer(data, len(data))
        return buffer, ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))

    def _persist_block_payload(self, payload, used_len, block_size):
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)

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
            raise FuseOSError(errno.EIO)

        try:
            ffi_result = ctypes.string_at(out_ptr, out_len.value)
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)
        if ffi_result is None:
            raise FuseOSError(errno.EIO)
        return ffi_result

    def _record_read_sequence(self, file_id, offset, end_offset):
        with self.owner._read_sequence_guard:
            previous = self.owner._read_sequence_state.get(file_id)
            lib = self._load_rust_hotpath_lib()
            if lib is None:
                raise FuseOSError(errno.EIO)
            has_previous = 1 if previous else 0
            previous_last_end = int(previous.get("last_end", 0)) if previous else 0
            previous_streak = int(previous.get("streak", 0)) if previous else 0
            result = lib.dbfs_read_sequence_step(
                ctypes.c_ubyte(has_previous),
                ctypes.c_uint64(previous_last_end),
                ctypes.c_uint64(int(offset)),
                ctypes.c_uint64(previous_streak),
            )
            sequential = bool(result.sequential)
            streak = int(result.streak)
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

        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
        plan = DbfsReadSlicePlan()
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
            ctypes.byref(plan),
        )
        if rc != 0:
            raise FuseOSError(errno.EIO)
        total_blocks, fetch_first, fetch_last = plan

        if total_blocks == 0:
            return b""

        if state is None:
            repo = self.owner.backend._load_rust_pg_repo()
            lib = self.owner.backend._load_rust_hotpath_lib()
            if repo is None or lib is None:
                raise FuseOSError(errno.EIO)
            out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
            out_len = ctypes.c_size_t()
            rc = lib.dbfs_rust_pg_repo_assemble_file_slice(
                repo,
                int(file_id),
                int(fetch_first),
                int(fetch_last),
                int(offset),
                int(end_offset),
                int(block_size),
                ctypes.byref(out_ptr),
                ctypes.byref(out_len),
            )
            if rc != 0 or not out_ptr:
                raise FuseOSError(errno.EIO)
            try:
                ffi_result = ctypes.string_at(out_ptr, out_len.value)
            finally:
                lib.dbfs_free_bytes(out_ptr, out_len)
            start_offset = offset - (fetch_first * block_size)
            end_offset_in_raw = start_offset + (end_offset - offset)
            return ffi_result[start_offset:end_offset_in_raw]

        block_map = self._fetch_block_range(file_id, fetch_first, fetch_last)
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
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_path_has_children(
            repo,
            int(directory_id),
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return bool(out_value.value)

    def count_directory_children(self, directory_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_count_directory_children(
            repo,
            int(directory_id),
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

    def count_directory_subdirs(self, directory_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_count_directory_subdirs(
            repo,
            int(directory_id),
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

    def count_root_directory_children(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_count_root_directory_children(
            repo,
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

    def count_file_blocks(self, file_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_count_file_blocks(
            repo,
            int(file_id),
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

    def _file_data_object_id(self, file_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            return None
        out_value = ctypes.c_uint64()
        out_found = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_file_data_object_id(
            repo,
            int(file_id),
            ctypes.byref(out_value),
            ctypes.byref(out_found),
        )
        if status != 0 or not out_found.value:
            return None
        return int(out_value.value)

    def _create_data_object(self, cur, file_size=0, content_hash=None):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        if content_hash is None:
            content_hash_bytes = None
            content_hash_len = 0
        else:
            content_hash_bytes = str(content_hash).encode("utf-8")
            content_hash_len = len(content_hash_bytes)
        status = lib.dbfs_rust_pg_repo_create_data_object(
            repo,
            int(file_size),
            content_hash_bytes,
            content_hash_len,
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

    def _touch_data_object(self, cur, data_object_id, file_size=None):
        if data_object_id is None:
            return
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_touched = ctypes.c_ubyte()
        if file_size is None:
            file_size_value = 0
            has_file_size = 0
        else:
            file_size_value = int(file_size)
            has_file_size = 1
        status = lib.dbfs_rust_pg_repo_touch_data_object(
            repo,
            int(data_object_id),
            file_size_value,
            ctypes.c_ubyte(has_file_size),
            ctypes.byref(out_touched),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)

    def count_symlinks(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_value = ctypes.c_uint64()
        status = lib.dbfs_rust_pg_repo_count_symlinks(
            repo,
            ctypes.byref(out_value),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        return int(out_value.value)

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
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)

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
            raise FuseOSError(errno.EIO)
        try:
            total_blocks = int(out_total_blocks.value)
            truncate_only = bool(out_truncate_only.value)
            ordered_dirty_plan = [
                (int(out_ptr[i].block_index), int(out_ptr[i].used_len))
                for i in range(out_len.value)
            ]
        finally:
            lib.dbfs_free_persist_blocks(out_ptr, out_len)
        blocks_written = 0

        started = time.perf_counter()

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

        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)

        payload_buffers = []
        inputs = []
        for _, block_index, data, used_len in block_rows:
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

        inputs_array = (DbfsPersistBlockInput * len(inputs))(*inputs) if inputs else None
        status = lib.dbfs_rust_pg_repo_persist_file_blocks(
            repo,
            int(file_id),
            int(file_size),
            int(block_size),
            int(total_blocks),
            ctypes.c_ubyte(1 if truncate_pending else 0),
            inputs_array,
            len(inputs),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        blocks_written = len(block_rows)

        if block_rows:
            status = lib.dbfs_rust_pg_repo_persist_copy_block_crc_rows(
                repo,
                int(file_id),
                int(block_size),
                inputs_array,
                len(inputs),
            )
            if status != 0:
                raise FuseOSError(errno.EIO)

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

        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            block = b"\x00" * self.owner.block_size
        else:
            out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
            out_len = ctypes.c_size_t()
            out_found = ctypes.c_ubyte()
            status = lib.dbfs_rust_pg_repo_load_block(
                repo,
                int(file_id),
                int(block_index),
                int(self.owner.block_size),
                ctypes.byref(out_ptr),
                ctypes.byref(out_len),
                ctypes.byref(out_found),
            )
            if status != 0 or not out_found.value:
                block = b"\x00" * self.owner.block_size
            else:
                try:
                    block = ctypes.string_at(out_ptr, out_len.value)
                finally:
                    lib.dbfs_free_bytes(out_ptr, out_len)

        self._store_cached_block(file_id, block_index, block)
        return block

    def _dirty_block_size(self, file_size, block_index):
        block_size = self.owner.block_size
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
        return int(
            lib.dbfs_dirty_block_size(
                ctypes.c_uint64(int(file_size)),
                ctypes.c_uint64(int(block_index)),
                ctypes.c_uint64(int(block_size)),
            )
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
        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
        plan = lib.dbfs_logical_resize_plan(
            ctypes.c_uint64(int(old_size)),
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
        )

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

    def _adopt_source_data_object(self, src_file_id, dst_file_id):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        out_adopted = ctypes.c_ubyte()
        status = lib.dbfs_rust_pg_repo_adopt_source_data_object(
            repo,
            int(src_file_id),
            int(dst_file_id),
            ctypes.byref(out_adopted),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        adopted = bool(out_adopted.value)
        state = self.get_write_state(dst_file_id)
        if state is not None and adopted:
            state["file_size"] = self.get_file_size(src_file_id)
        self.clear_read_cache(dst_file_id)
        self.owner.invalidate_metadata_cache(path=None, include_statfs=True)
        return bool(adopted)

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

        current_bytes = self.read_file_slice(dst_file_id, dst_offset, len(payload))
        if len(current_bytes) < len(payload):
            current_bytes = current_bytes + (b"\x00" * (len(payload) - len(current_bytes)))

        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
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
            raise FuseOSError(errno.EIO)
        try:
            ranges = [(int(out_ptr[i].start), int(out_ptr[i].end)) for i in range(out_len.value)]
        finally:
            lib.dbfs_free_ranges(out_ptr, out_len)

        bytes_written = 0
        for run_start, run_end in ranges:
            if run_end <= run_start:
                continue
            rel_start = run_start - dst_offset
            rel_end = run_end - dst_offset
            self.write_into_state(dst_file_id, payload[rel_start:rel_end], run_start)
            bytes_written += rel_end - rel_start

        return bytes_written

    def copy_file_range_into_state(self, src_file_id, dst_file_id, off_in, off_out, length):
        # Bezpieczny wariant workers_write:
        # czytanie segmentow moze byc rownolegle, ale zapis do overlay jest sekwencyjny
        if length <= 0:
            return 0

        block_size = self.owner.block_size
        workers_write = max(1, int(getattr(self.owner, "workers_write", 1) or 1))
        workers_write_min_blocks = max(1, int(getattr(self.owner, "workers_write_min_blocks", 8) or 8))
        plan = self._write_transfer_plan(length, block_size, workers_write, workers_write_min_blocks)
        dedupe_enabled = plan.dedupe_enabled
        if (
            dedupe_enabled
            and off_in == 0
            and off_out == 0
            and length == self.get_logical_file_size(src_file_id)
            and self.get_logical_file_size(dst_file_id) == 0
            and not self.is_write_buffer_dirty(src_file_id)
            and not self.is_write_buffer_dirty(dst_file_id)
            and self._adopt_source_data_object(src_file_id, dst_file_id)
        ):
            return length
        total_blocks = plan.total_blocks
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

        lib = self._load_rust_hotpath_lib()
        if lib is None:
            raise FuseOSError(errno.EIO)
        out_ptr = ctypes.POINTER(DbfsCopySegment)()
        out_len = ctypes.c_size_t()
        rc = lib.dbfs_copy_plan(
            ctypes.c_uint64(int(off_in)),
            ctypes.c_uint64(int(off_out)),
            ctypes.c_uint64(int(length)),
            ctypes.c_uint64(int(block_size)),
            ctypes.c_uint64(int(workers_write)),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            raise FuseOSError(errno.EIO)
        try:
            segments = [
                (int(out_ptr[i].src), int(out_ptr[i].dst), int(out_ptr[i].len))
                for i in range(out_len.value)
            ]
        finally:
            lib.dbfs_free_copy_segments(out_ptr, out_len)
        max_workers = max(1, min(parallel_workers, len(segments)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.read_file_slice, src_file_id, src_offset, chunk_len)
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
