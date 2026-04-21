from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor


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

    def _missing_block_slices(self, missing, workers):
        if not missing:
            return []

        worker_count = max(1, min(int(workers), len(missing)))
        chunk_size = max(1, (len(missing) + worker_count - 1) // worker_count)

        slices = []
        for start in range(0, len(missing), chunk_size):
            chunk = missing[start:start + chunk_size]
            if chunk:
                slices.append((chunk[0], chunk[-1]))
        return slices

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

            if workers_read <= 1 or len(missing) < workers_read_min_blocks:
                fetched_maps = [self._fetch_block_range_chunk(file_id, missing[0], missing[-1])]
            else:
                ranges = self._missing_block_slices(missing, workers_read)
                max_workers = max(1, min(workers_read, len(ranges)))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(self._fetch_block_range_chunk, file_id, range_first, range_last)
                        for range_first, range_last in ranges
                    ]
                    fetched_maps = [future.result() for future in futures]

            for fetched in fetched_maps:
                for block_index, data in fetched.items():
                    cached[block_index] = data
                    self._store_cached_block(file_id, block_index, data)

        return cached

    def _persist_block_chunks(self, cur, blocks):
        if not blocks:
            return

        chunk_size = max(1, int(getattr(self.owner, "persist_buffer_chunk_blocks", self.PERSIST_BUFFER_CHUNK_BLOCKS) or self.PERSIST_BUFFER_CHUNK_BLOCKS))
        for start in range(0, len(blocks), chunk_size):
            chunk = blocks[start:start + chunk_size]
            cur.executemany(
                """
                INSERT INTO data_blocks (id_file, _order, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_file, _order)
                DO UPDATE SET data = EXCLUDED.data
                """,
                chunk,
            )

    def _assemble_blocks(self, file_id, first_block, last_block):
        block_size = self.owner.block_size
        block_map = self._fetch_block_range(file_id, first_block, last_block)
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
            sequential = bool(previous and previous.get("last_end") == offset)
            streak = (int(previous.get("streak", 0)) + 1) if sequential and previous else 0
            self.owner._read_sequence_state[file_id] = {"last_offset": offset, "last_end": end_offset, "streak": streak}
        return sequential, streak

    def read_file_slice(self, file_id, offset, size):
        # Czyta dane z overlay lub z PostgreSQL, bez pelnego write_cache
        if size <= 0:
            return b""

        state = self.get_write_state(file_id)
        file_size = int(state["file_size"]) if state is not None else self.get_file_size(file_id)

        if offset >= file_size:
            return b""

        end_offset = min(file_size, offset + size)
        block_size = self.owner.block_size
        total_blocks = (file_size + block_size - 1) // block_size
        if total_blocks == 0:
            return b""

        requested_first = offset // block_size
        requested_last = max(requested_first, (end_offset - 1) // block_size)
        sequential, streak = self._record_read_sequence(file_id, offset, end_offset)

        if total_blocks <= self.small_file_threshold_blocks():
            fetch_first = 0
            fetch_last = total_blocks - 1
        else:
            fetch_first = requested_first
            read_ahead_blocks = self.read_ahead_blocks()
            if sequential:
                dynamic_ahead = self.sequential_read_ahead_blocks() * max(1, streak)
                read_ahead_blocks = max(read_ahead_blocks, dynamic_ahead)
            read_ahead_blocks = min(read_ahead_blocks, max(0, self.read_cache_limit_blocks() - 1))
            fetch_last = min(total_blocks - 1, requested_last + read_ahead_blocks)

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
        total_blocks = (file_size + block_size - 1) // block_size if file_size > 0 else 0
        state["dirty_blocks"].update(range(total_blocks))
        self.owner._dirty_write_buffers[file_id] = None

    def mark_write_range_dirty(self, file_id, start_offset, end_offset):
        if file_id is None or end_offset <= start_offset:
            return

        state = self.ensure_write_state(file_id)
        block_size = self.owner.block_size
        first_block = max(0, start_offset // block_size)
        last_block = max(0, (end_offset - 1) // block_size)
        state["dirty_blocks"].update(range(first_block, last_block + 1))

        compat_dirty = self.owner._dirty_write_buffers.get(file_id)
        if compat_dirty is None and file_id in self.owner._dirty_write_buffers:
            return
        compat_dirty = self.owner._dirty_write_buffers.setdefault(file_id, set())
        compat_dirty.update(range(first_block, last_block + 1))

    def dirty_write_buffer_bytes(self, file_id):
        # Liczy tylko logiczne bajty dirty, bez pelnego bufora w RAM
        state = self.get_write_state(file_id)
        if state is None:
            return 0

        dirty_blocks = state["dirty_blocks"]
        if not dirty_blocks:
            return 0

        block_size = self.owner.block_size
        file_size = int(state["file_size"])
        total = 0

        for block_index in dirty_blocks:
            block_start = block_index * block_size
            block_end = min(file_size, block_start + block_size)
            if block_end > block_start:
                total += (block_end - block_start)

        return total

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
            state["truncate_pending"] = False
        self.owner._dirty_write_buffers.pop(file_id, None)

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
        dirty_blocks = set(state["dirty_blocks"])
        if not dirty_blocks and not truncate_pending:
            return

        file_size = int(state["file_size"])
        block_size = self.owner.block_size
        total_blocks = (file_size + block_size - 1) // block_size if file_size > 0 else 0

        overlay_blocks = state["overlay_blocks"]
        blocks = []

        for block_index in dirty_blocks:
            if block_index >= total_blocks:
                # Blok poza EOF nie powinien byc upsertowany
                continue

            payload = overlay_blocks.get(block_index)
            if payload is None:
                continue

            block_start = block_index * block_size
            block_end = min(file_size, block_start + block_size)
            used_len = max(0, block_end - block_start)

            data = bytes(payload[:used_len])
            if used_len < block_size:
                data = data + (b"\x00" * (block_size - used_len))

            blocks.append((file_id, block_index, data))

        started = time.perf_counter()

        conn = None
        for attempt in range(2):
            try:
                with self.owner.db_connection() as conn, conn.cursor() as cur:
                    if total_blocks == 0:
                        cur.execute(
                            """
                            DELETE FROM data_blocks
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

                    if blocks:
                        self._persist_block_chunks(cur, blocks)

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
            blocks=len(blocks),
        )

        # Najbezpieczniejszy wariant: po flush usun stan z RAM
        self.owner._dirty_write_buffers.pop(file_id, None)
        self.drop_write_state(file_id)
        self.clear_read_cache(file_id)
        self.owner.invalidate_metadata_cache(include_statfs=True)

    def cleanup(self):
        # Czyci wszystkie stany tymczasowe
        self.owner.write_cache.clear()
        self.owner._dirty_write_buffers.clear()
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

    def write_into_state(self, file_id, buf, offset):
        # Zapisuje dane do overlay blokow bez ladowania calego pliku
        state = self.ensure_write_state(file_id)
        write_length = len(buf)
        end_offset = offset + write_length
        block_size = self.owner.block_size

        first_block = offset // block_size
        last_block = max(first_block, (end_offset - 1) // block_size)

        src_pos = 0
        for block_index in range(first_block, last_block + 1):
            block = self.ensure_overlay_block(file_id, block_index)

            block_start_abs = block_index * block_size
            block_end_abs = block_start_abs + block_size

            write_start_abs = max(offset, block_start_abs)
            write_end_abs = min(end_offset, block_end_abs)

            block_start_rel = write_start_abs - block_start_abs
            block_end_rel = write_end_abs - block_start_abs

            chunk_len = write_end_abs - write_start_abs
            block[block_start_rel:block_end_rel] = buf[src_pos:src_pos + chunk_len]

            state["dirty_blocks"].add(block_index)
            src_pos += chunk_len

        if end_offset > state["file_size"]:
            state["file_size"] = end_offset

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

        if length == old_size:
            return

        if length < old_size:
            state["truncate_pending"] = True
            self.owner._dirty_write_buffers[file_id] = None

        state["file_size"] = int(length)

        # Usun bloki calkowicie poza nowym EOF
        max_valid_block = ((length - 1) // block_size) if length > 0 else -1
        stale_blocks = [
            block_index
            for block_index in list(state["overlay_blocks"].keys())
            if block_index > max_valid_block
        ]
        for block_index in stale_blocks:
            state["overlay_blocks"].pop(block_index, None)
            state["dirty_blocks"].discard(block_index)

        # Jesli skracamy do srodka bloku, wyzeruj ogon tego bloku
        if length > 0 and (length % block_size) != 0:
            last_block = length // block_size
            block = self.ensure_overlay_block(file_id, last_block)
            valid_len = length - (last_block * block_size)
            if valid_len < block_size:
                block[valid_len:] = b"\x00" * (block_size - valid_len)
            state["dirty_blocks"].add(last_block)

            compat_dirty = self.owner._dirty_write_buffers.get(file_id)
            if not (compat_dirty is None and file_id in self.owner._dirty_write_buffers):
                compat_dirty = self.owner._dirty_write_buffers.setdefault(file_id, set())
                compat_dirty.add(last_block)

    def _copy_segments(self, off_in, off_out, length, block_size, workers):
        if length <= 0:
            return []

        total_blocks = max(1, (length + block_size - 1) // block_size)
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
        total_blocks = max(1, (length + block_size - 1) // block_size)

        if workers_write <= 1 or total_blocks < workers_write_min_blocks:
            chunk = self.read_file_slice(src_file_id, off_in, length)
            if not chunk:
                return 0
            self.write_into_state(dst_file_id, chunk, off_out)
            return len(chunk)

        segments = self._copy_segments(off_in, off_out, length, block_size, workers_write)
        max_workers = max(1, min(workers_write, len(segments)))

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
            self.write_into_state(dst_file_id, payload, dst_offset)
            copied += len(payload)

        return copied
