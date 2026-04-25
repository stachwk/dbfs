from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import logging
import threading
import time

from fuse import FuseOSError


class PostgresLeaseLockManager:
    LOCK_KIND = "flock"
    RANGE_KIND = "fcntl"

    def __init__(self, owner, lease_ttl_seconds=30, heartbeat_interval_seconds=10, poll_interval_seconds=0.1):
        self.owner = owner
        self.lease_ttl_seconds = max(1, int(lease_ttl_seconds))
        self.heartbeat_interval_seconds = max(1, int(heartbeat_interval_seconds))
        self.poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        self._active_guard = threading.RLock()
        self._active_leases = set()
        self._stop_event = threading.Event()
        self._heartbeat_thread = None
        self._ensure_schema()
        self._start_heartbeat()

    def _ensure_schema(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        status = lib.dbfs_rust_pg_repo_ensure_lock_schema(repo)
        if status != 0:
            raise FuseOSError(errno.EIO)

    def _rust_repo(self):
        repo = self.owner.backend._load_rust_pg_repo()
        lib = self.owner.backend._load_rust_hotpath_lib()
        if repo is None or lib is None:
            raise FuseOSError(errno.EIO)
        return repo, lib

    def _resource_id(self, resource_key):
        payload = repr(resource_key).encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        value = int.from_bytes(digest[:8], "big", signed=False)
        if value >= 2**63:
            value -= 2**64
        return value

    def _resource_hash(self, resource_key):
        return self._resource_id(("lock", resource_key, self.LOCK_KIND))

    def _lease_key(self, resource_key, owner_key):
        resource_kind, resource_id = resource_key
        return (resource_kind, int(resource_id), int(owner_key), self.LOCK_KIND)

    def _range_lease_key(self, resource_key, owner_key, start, end):
        resource_kind, resource_id = resource_key
        return (resource_kind, int(resource_id), int(owner_key), self.RANGE_KIND, int(start), None if end is None else int(end))

    def _register_active(self, resource_key, owner_key):
        with self._active_guard:
            self._active_leases.add(self._lease_key(resource_key, owner_key))

    def _unregister_active(self, resource_key, owner_key):
        with self._active_guard:
            self._active_leases.discard(self._lease_key(resource_key, owner_key))

    def _register_range_active(self, resource_key, owner_key, start, end):
        with self._active_guard:
            self._active_leases.add(self._range_lease_key(resource_key, owner_key, start, end))

    def _clear_range_active(self, resource_key, owner_key=None):
        with self._active_guard:
            if owner_key is None:
                self._active_leases = {
                    lease for lease in self._active_leases if not (lease[3] == self.RANGE_KIND and lease[0] == resource_key[0] and lease[1] == resource_key[1])
                }
                return
            self._active_leases = {
                lease for lease in self._active_leases if not (lease[3] == self.RANGE_KIND and lease[0] == resource_key[0] and lease[1] == resource_key[1] and lease[2] == owner_key)
            }

    def _active_snapshot(self):
        with self._active_guard:
            return list(self._active_leases)

    def _load_range_state(self, cur, resource_key):
        repo, lib = self._rust_repo()
        resource_kind, resource_id = resource_key
        resource_kind_bytes = str(resource_kind).encode("utf-8")
        out_ptr = ctypes.POINTER(ctypes.c_ubyte)()
        out_len = ctypes.c_size_t()
        status = lib.dbfs_rust_pg_repo_load_lock_range_state_blob(
            repo,
            resource_kind_bytes,
            len(resource_kind_bytes),
            int(resource_id),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        if not out_ptr or out_len.value == 0:
            return {}
        try:
            payload = ctypes.string_at(out_ptr, out_len.value).decode("utf-8")
        finally:
            lib.dbfs_free_bytes(out_ptr, out_len)
        state = {}
        for line in payload.splitlines():
            if not line:
                continue
            owner_key_text, lock_type_text, start_text, end_text = line.split("\t")
            state.setdefault(int(owner_key_text), []).append(
                {
                    "type": int(lock_type_text),
                    "start": int(start_text),
                    "end": None if end_text == "" else int(end_text),
                }
            )
        return state

    def _persist_range_state(self, cur, resource_key, state):
        repo, lib = self._rust_repo()
        resource_kind, resource_id = resource_key
        resource_kind_bytes = str(resource_kind).encode("utf-8")
        lines = []
        for owner_key, records in state.items():
            for record in records:
                end_text = "" if record["end"] is None else str(int(record["end"]))
                lines.append(
                    "\t".join(
                        [
                            str(int(owner_key)),
                            str(int(record["type"])),
                            str(int(record["start"])),
                            end_text,
                        ]
                    )
                )
        payload = "\n".join(lines).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_persist_lock_range_state_blob(
            repo,
            resource_kind_bytes,
            len(resource_kind_bytes),
            int(resource_id),
            int(self.lease_ttl_seconds),
            payload,
            len(payload),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)

    def _range_active_snapshot(self):
        with self._active_guard:
            return [lease for lease in self._active_leases if len(lease) == 6 and lease[3] == self.RANGE_KIND]

    @staticmethod
    def _range_to_end(start, end):
        return float("inf") if end is None else end

    @staticmethod
    def _ranges_overlap(start_a, end_a, start_b, end_b):
        end_a = float("inf") if end_a is None else end_a
        end_b = float("inf") if end_b is None else end_b
        return start_a < end_b and start_b < end_a

    def _range_conflicts(self, requested_type, other_type):
        if requested_type == fcntl.F_RDLCK:
            return other_type == fcntl.F_WRLCK
        if requested_type == fcntl.F_WRLCK:
            return other_type in {fcntl.F_RDLCK, fcntl.F_WRLCK}
        return False

    def _find_range_conflict(self, state, owner_key, requested_type, requested_start, requested_end):
        for other_owner, owner_records in state.items():
            if other_owner == owner_key:
                continue
            for record in owner_records:
                if self._ranges_overlap(requested_start, requested_end, record["start"], record["end"]) and self._range_conflicts(requested_type, record["type"]):
                    return other_owner, record
        return None, None

    def _unlock_range_records(self, records, owner_key, unlock_start, unlock_end):
        owner_records = records.get(owner_key, [])
        updated = []
        for record in owner_records:
            if not self._ranges_overlap(unlock_start, unlock_end, record["start"], record["end"]):
                updated.append(record)
                continue
            record_end = self._range_to_end(record["start"], record["end"])
            unlock_end_value = self._range_to_end(unlock_start, unlock_end)
            if unlock_start > record["start"]:
                left_end = min(unlock_start, record_end)
                if left_end > record["start"]:
                    updated.append({"type": record["type"], "start": record["start"], "end": left_end})
            if unlock_end_value < record_end:
                right_start = max(unlock_end_value, record["start"])
                right_end = record["end"]
                if right_start < self._range_to_end(right_start, right_end):
                    updated.append({"type": record["type"], "start": right_start, "end": right_end})
        if updated:
            records[owner_key] = sorted(updated, key=lambda item: (item["start"], self._range_to_end(item["start"], item["end"])))
        else:
            records.pop(owner_key, None)

    def _merge_range_record(self, records, owner_key, new_record):
        owner_records = records.setdefault(owner_key, [])
        owner_records.append(new_record)
        owner_records.sort(key=lambda item: (item["start"], self._range_to_end(item["start"], item["end"])))
        merged = []
        for record in owner_records:
            if not merged:
                merged.append(record)
                continue
            prev = merged[-1]
            prev_end = self._range_to_end(prev["start"], prev["end"])
            if prev["type"] == record["type"] and record["start"] <= prev_end:
                merged[-1] = {
                    "type": prev["type"],
                    "start": prev["start"],
                    "end": None if prev["end"] is None or record["end"] is None else max(prev["end"], record["end"]),
                }
                continue
            if prev["type"] == record["type"] and record["start"] == prev_end:
                merged[-1] = {
                    "type": prev["type"],
                    "start": prev["start"],
                    "end": None if prev["end"] is None or record["end"] is None else max(prev["end"], record["end"]),
                }
                continue
            merged.append(record)
        records[owner_key] = merged

    def _set_range_state(self, cur, resource_key, state):
        self._persist_range_state(cur, resource_key, state)
        self._clear_range_active(resource_key)
        for owner_key, owner_records in state.items():
            for record in owner_records:
                self._register_range_active(resource_key, owner_key, record["start"], record["end"])

    def _heartbeat_active_leases(self):
        snapshot = self._active_snapshot()
        if not snapshot:
            return
        repo, lib = self._rust_repo()
        for lease in snapshot:
            if len(lease) == 4:
                resource_kind, resource_id, owner_key, lease_kind = lease
                if lease_kind != self.LOCK_KIND:
                    continue
                resource_kind_bytes = str(resource_kind).encode("utf-8")
                status = lib.dbfs_rust_pg_repo_heartbeat_lock_lease(
                    repo,
                    resource_kind_bytes,
                    len(resource_kind_bytes),
                    int(resource_id),
                    int(owner_key),
                    int(self.lease_ttl_seconds),
                )
                if status != 0:
                    logging.debug("Failed to heartbeat PostgreSQL lock lease", exc_info=True)
                continue
            resource_kind, resource_id, owner_key, lease_kind, range_start, range_end = lease
            if lease_kind != self.RANGE_KIND:
                continue
            resource_kind_bytes = str(resource_kind).encode("utf-8")
            status = lib.dbfs_rust_pg_repo_heartbeat_lock_range_lease(
                repo,
                resource_kind_bytes,
                len(resource_kind_bytes),
                int(resource_id),
                int(owner_key),
                int(range_start),
                0 if range_end is None else int(range_end),
                ctypes.c_ubyte(0 if range_end is None else 1),
                int(self.lease_ttl_seconds),
            )
            if status != 0:
                logging.debug("Failed to heartbeat PostgreSQL range lease", exc_info=True)

    def _heartbeat_loop(self):
        while not self._stop_event.wait(self.heartbeat_interval_seconds):
            self._heartbeat_active_leases()

    def _start_heartbeat(self):
        if self._heartbeat_thread is not None:
            return
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="dbfs-lock-heartbeat", daemon=True)
        self._heartbeat_thread.start()

    def acquire(self, resource_key, owner_key, requested_type, nonblocking=False):
        if requested_type not in {fcntl.LOCK_SH, fcntl.LOCK_EX}:
            raise FuseOSError(errno.EINVAL)

        resource_lock_id = self._resource_hash(resource_key)
        repo, lib = self._rust_repo()
        resource_kind, resource_id = resource_key
        resource_kind_bytes = str(resource_kind).encode("utf-8")
        while True:
            status = lib.dbfs_rust_pg_repo_acquire_flock_lease(
                repo,
                int(resource_lock_id),
                resource_kind_bytes,
                len(resource_kind_bytes),
                int(resource_id),
                int(owner_key),
                int(requested_type),
                int(self.lease_ttl_seconds),
            )
            if status == 0:
                self._register_active(resource_key, owner_key)
                return 0
            if status != 1:
                raise FuseOSError(errno.EIO)
            if nonblocking:
                raise FuseOSError(errno.EWOULDBLOCK)
            time.sleep(self.poll_interval_seconds)

    def release(self, resource_key, owner_key):
        repo, lib = self._rust_repo()
        resource_kind, resource_id = resource_key
        resource_kind_bytes = str(resource_kind).encode("utf-8")
        status = lib.dbfs_rust_pg_repo_release_flock_lease(
            repo,
            resource_kind_bytes,
            len(resource_kind_bytes),
            int(resource_id),
            int(owner_key),
        )
        if status != 0:
            raise FuseOSError(errno.EIO)
        self._unregister_active(resource_key, owner_key)
        self._clear_range_active(resource_key, owner_key)
        return 0

    def cleanup(self):
        self._stop_event.set()
        thread = self._heartbeat_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        snapshot = self._active_snapshot()
        for resource_kind, resource_id, owner_key, lease_kind in snapshot:
            if lease_kind != self.LOCK_KIND:
                continue
            try:
                self.release((resource_kind, resource_id), owner_key)
            except Exception:
                logging.debug("Failed to release PostgreSQL lease during cleanup", exc_info=True)
        for resource_kind, resource_id, owner_key, lease_kind, range_start, range_end in self._range_active_snapshot():
            try:
                self.release((resource_kind, resource_id), owner_key)
            except Exception:
                logging.debug("Failed to release PostgreSQL range lease during cleanup", exc_info=True)
        with self._active_guard:
            self._active_leases.clear()

    def get_range_conflict(self, resource_key, owner_key, requested_type, requested_start, requested_end):
        resource_lock_id = self._resource_hash(resource_key)
        while True:
            repo, lib = self._rust_repo()
            resource_kind, resource_id = resource_key
            resource_kind_bytes = str(resource_kind).encode("utf-8")
            lock_status = lib.dbfs_rust_pg_repo_try_advisory_xact_lock(repo, int(resource_lock_id))
            if lock_status == 0:
                self._prune_expired_range(None, resource_key)
                state = self._load_range_state(None, resource_key)
                _, conflict = self._find_range_conflict(state, owner_key, requested_type, requested_start, requested_end)
                if conflict is None:
                    return None
                return conflict
            if lock_status != 1:
                raise FuseOSError(errno.EIO)
            time.sleep(self.poll_interval_seconds)

    def _prune_expired_range(self, cur, resource_key=None):
        repo, lib = self._rust_repo()
        if resource_key is None:
            status = lib.dbfs_rust_pg_repo_prune_lock_range_leases(repo, None, 0, 0, ctypes.c_ubyte(0))
        else:
            resource_kind, resource_id = resource_key
            resource_kind_bytes = str(resource_kind).encode("utf-8")
            status = lib.dbfs_rust_pg_repo_prune_lock_range_leases(
                repo,
                resource_kind_bytes,
                len(resource_kind_bytes),
                int(resource_id),
                ctypes.c_ubyte(1),
            )
        if status != 0:
            raise FuseOSError(errno.EIO)

    def acquire_range(self, resource_key, owner_key, requested_type, requested_start, requested_end, nonblocking=False):
        if requested_type not in {fcntl.F_RDLCK, fcntl.F_WRLCK}:
            raise FuseOSError(errno.EINVAL)

        resource_lock_id = self._resource_hash(resource_key)
        while True:
            repo, lib = self._rust_repo()
            lock_status = lib.dbfs_rust_pg_repo_try_advisory_xact_lock(repo, int(resource_lock_id))
            if lock_status == 0:
                self._prune_expired_range(None, resource_key)
                state = self._load_range_state(None, resource_key)
                _, conflict = self._find_range_conflict(state, owner_key, requested_type, requested_start, requested_end)
                if conflict is not None:
                    if nonblocking:
                        raise FuseOSError(errno.EWOULDBLOCK)
                    time.sleep(self.poll_interval_seconds)
                    continue
                self._unlock_range_records(state, owner_key, requested_start, requested_end)
                self._merge_range_record(
                    state,
                    owner_key,
                    {"type": requested_type, "start": requested_start, "end": requested_end},
                )
                self._set_range_state(None, resource_key, state)
                return 0
            if lock_status != 1:
                raise FuseOSError(errno.EIO)
            if nonblocking:
                raise FuseOSError(errno.EWOULDBLOCK)
            time.sleep(self.poll_interval_seconds)

    def unlock_range(self, resource_key, owner_key, unlock_start, unlock_end):
        resource_lock_id = self._resource_hash(resource_key)
        while True:
            repo, lib = self._rust_repo()
            lock_status = lib.dbfs_rust_pg_repo_try_advisory_xact_lock(repo, int(resource_lock_id))
            if lock_status == 0:
                self._prune_expired_range(None, resource_key)
                state = self._load_range_state(None, resource_key)
                self._unlock_range_records(state, owner_key, unlock_start, unlock_end)
                self._set_range_state(None, resource_key, state)
                return 0
            if lock_status != 1:
                raise FuseOSError(errno.EIO)
            time.sleep(self.poll_interval_seconds)
