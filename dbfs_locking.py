from __future__ import annotations

import errno
import fcntl
import logging
import os
import hashlib
import time

from fuse import FuseOSError

from dbfs_pg_lock_manager import PostgresLeaseLockManager


class LockingSupport:
    def __init__(self, owner, backend_mode="memory", lease_ttl_seconds=30, heartbeat_interval_seconds=10, poll_interval_seconds=0.1):
        self.owner = owner
        self._path_locks = {}
        self._path_locks_guard = owner._path_locks_guard if hasattr(owner, "_path_locks_guard") else None
        self._flock_locks = {}
        self._seek_positions = owner._seek_positions if hasattr(owner, "_seek_positions") else {}
        self._backend_mode = (backend_mode or "memory").lower()
        self._pg_lock_manager = None
        self._poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        if self._backend_mode == "postgres_lease":
            self._pg_lock_manager = PostgresLeaseLockManager(
                owner,
                lease_ttl_seconds=lease_ttl_seconds,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
            )

    def bind_state(self, path_locks_guard, seek_positions):
        self._path_locks_guard = path_locks_guard
        self._seek_positions = seek_positions
        return self

    def _lock_owner_key(self, fh):
        instance_id = getattr(self.owner, "instance_id", None)
        if instance_id is None:
            try:
                return int(fh)
            except Exception:
                return fh
        payload = repr((instance_id, fh)).encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        value = int.from_bytes(digest[:8], "big", signed=False)
        if value >= 2**63:
            value -= 2**64
        return value

    def _normalize_lock_range(self, path, fh, lock):
        attrs = self.owner.getattr(path)
        file_size = int(attrs.get("st_size", 0))
        whence = self._get_lock_field(lock, "l_whence", os.SEEK_SET)
        start = int(self._get_lock_field(lock, "l_start", 0))
        length = int(self._get_lock_field(lock, "l_len", 0))

        if whence == os.SEEK_SET:
            base = 0
        elif whence == os.SEEK_CUR:
            base = int(self._seek_positions.get(fh, 0))
        elif whence == os.SEEK_END:
            base = file_size
        else:
            raise FuseOSError(errno.EINVAL)

        abs_start = base + start
        if abs_start < 0:
            raise FuseOSError(errno.EINVAL)

        if length == 0:
            abs_end = None
        elif length > 0:
            abs_end = abs_start + length
        else:
            abs_end = file_size + length
            if abs_end < abs_start:
                raise FuseOSError(errno.EINVAL)

        return abs_start, abs_end

    @staticmethod
    def _ranges_overlap(start_a, end_a, start_b, end_b):
        end_a = float("inf") if end_a is None else end_a
        end_b = float("inf") if end_b is None else end_b
        return start_a < end_b and start_b < end_a

    def _lock_records(self, resource_state):
        return resource_state.setdefault("records", {})

    @staticmethod
    def _range_to_end(start, end):
        return float("inf") if end is None else end

    def _iter_lock_records(self, resource_state):
        for owner_key, owner_records in self._lock_records(resource_state).items():
            for record in owner_records:
                yield owner_key, record

    def _lock_record_conflicts(self, record, requested_type, requested_start, requested_end):
        if record["type"] == fcntl.F_UNLCK:
            return False
        if not self._ranges_overlap(requested_start, requested_end, record["start"], record["end"]):
            return False
        return self._lock_conflicts(record, requested_type)

    def _unlock_lock_records(self, records, owner_key, unlock_start, unlock_end):
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

    def _merge_lock_record(self, records, owner_key, new_record):
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
            curr_end = self._range_to_end(record["start"], record["end"])
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

    def _get_lock_state(self, resource_key):
        with self._path_locks_guard:
            return self._path_locks.setdefault(resource_key, {"records": {}})

    @staticmethod
    def _get_lock_field(lock, name, default=None):
        if hasattr(lock, name):
            return getattr(lock, name)
        if hasattr(lock, "contents") and hasattr(lock.contents, name):
            return getattr(lock.contents, name)
        return default

    @staticmethod
    def _set_lock_field(lock, name, value):
        if hasattr(lock, name):
            setattr(lock, name, value)
            return
        if hasattr(lock, "contents") and hasattr(lock.contents, name):
            setattr(lock.contents, name, value)

    def _clear_path_lock_state(self, resource_key, owner_key=None):
        with self._path_locks_guard:
            if owner_key is None:
                self._path_locks.pop(resource_key, None)
                return
            resource_state = self._path_locks.get(resource_key)
            if not resource_state:
                return
            records = self._lock_records(resource_state)
            records.pop(owner_key, None)
            if not records:
                self._path_locks.pop(resource_key, None)

    def _get_flock_state(self, resource_key):
        with self._path_locks_guard:
            return self._flock_locks.setdefault(resource_key, {"records": {}})

    def _clear_flock_state(self, resource_key, owner_key=None):
        with self._path_locks_guard:
            if owner_key is None:
                self._flock_locks.pop(resource_key, None)
                return
            resource_state = self._flock_locks.get(resource_key)
            if not resource_state:
                return
            records = self._lock_records(resource_state)
            records.pop(owner_key, None)
            if not records:
                self._flock_locks.pop(resource_key, None)

    def cleanup(self):
        if self._pg_lock_manager is not None:
            self._pg_lock_manager.cleanup()
        self._path_locks.clear()
        self._flock_locks.clear()
        self._seek_positions.clear()

    def _lock_resource_key(self, path):
        kind, entry_id = self.owner.get_entry_kind_and_id(path)
        if kind in {"file", "hardlink"}:
            file_id = self.owner.get_file_id(path)
            if file_id is not None:
                return ("file", file_id)
        if kind == "dir":
            return ("dir", entry_id)
        return ("path", path)

    def _lock_conflicts(self, state, request_type):
        if request_type == fcntl.F_RDLCK:
            return state["type"] == fcntl.F_WRLCK
        if request_type == fcntl.F_WRLCK:
            return state["type"] in {fcntl.F_RDLCK, fcntl.F_WRLCK}
        return False

    def _describe_conflicting_lock(self, state, requested_type):
        if state["type"] == fcntl.F_WRLCK:
            return fcntl.F_WRLCK
        if state["type"] == fcntl.F_RDLCK:
            return fcntl.F_RDLCK
        return fcntl.F_UNLCK

    def lock(self, path, fh, cmd, lock):
        path = self.owner.normalize_path(path)
        kind, _ = self.owner.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.EBADF)

        requested_type = self._get_lock_field(lock, "l_type", fcntl.F_UNLCK)
        owner_key = self._lock_owner_key(fh)
        logging.debug("lock request path=%s fh=%s cmd=%s type=%s", path, fh, cmd, requested_type)
        if cmd not in (fcntl.F_GETLK, fcntl.F_SETLK, fcntl.F_SETLKW):
            raise FuseOSError(errno.EINVAL)

        resource_key = self._lock_resource_key(path)
        requested_start, requested_end = self._normalize_lock_range(path, fh, lock)
        if self._pg_lock_manager is not None:
            if cmd == fcntl.F_GETLK:
                if requested_type == fcntl.F_UNLCK:
                    self._set_lock_field(lock, "l_type", fcntl.F_UNLCK)
                    return 0
                conflict = self._pg_lock_manager.get_range_conflict(resource_key, owner_key, requested_type, requested_start, requested_end)
                if conflict is None:
                    self._set_lock_field(lock, "l_type", fcntl.F_UNLCK)
                else:
                    self._set_lock_field(lock, "l_type", self._describe_conflicting_lock(conflict, requested_type))
                    self._set_lock_field(lock, "l_whence", os.SEEK_SET)
                    self._set_lock_field(lock, "l_start", conflict["start"])
                    if conflict["end"] is None:
                        self._set_lock_field(lock, "l_len", 0)
                    else:
                        self._set_lock_field(lock, "l_len", max(0, conflict["end"] - conflict["start"]))
                    self._set_lock_field(lock, "l_pid", os.getpid())
                return 0

            if requested_type == fcntl.F_UNLCK:
                return self._pg_lock_manager.unlock_range(resource_key, owner_key, requested_start, requested_end)

            if self.owner.read_only and requested_type == fcntl.F_WRLCK:
                raise FuseOSError(errno.EROFS)

            nonblocking = cmd == fcntl.F_SETLK
            return self._pg_lock_manager.acquire_range(
                resource_key,
                owner_key,
                requested_type,
                requested_start,
                requested_end,
                nonblocking=nonblocking,
            )

        state = self._get_lock_state(resource_key)
        if cmd == fcntl.F_GETLK:
            conflicting_state = None
            for other_owner, owner_state in self._iter_lock_records(state):
                if other_owner == owner_key:
                    continue
                if self._ranges_overlap(
                    requested_start,
                    requested_end,
                    owner_state["start"],
                    owner_state["end"],
                ) and self._lock_conflicts(owner_state, requested_type):
                    conflicting_state = owner_state
                    break
            if conflicting_state is not None:
                logging.debug("lock conflict path=%s state=%s requested=%s", path, conflicting_state, requested_type)
                self._set_lock_field(lock, "l_type", self._describe_conflicting_lock(conflicting_state, requested_type))
                self._set_lock_field(lock, "l_whence", os.SEEK_SET)
                self._set_lock_field(lock, "l_start", conflicting_state["start"])
                if conflicting_state["end"] is None:
                    self._set_lock_field(lock, "l_len", 0)
                else:
                    self._set_lock_field(lock, "l_len", max(0, conflicting_state["end"] - conflicting_state["start"]))
                self._set_lock_field(lock, "l_pid", os.getpid())
            else:
                self._set_lock_field(lock, "l_type", fcntl.F_UNLCK)
            return 0

        if requested_type == fcntl.F_UNLCK:
            logging.debug("lock unlock path=%s", path)
            with self._path_locks_guard:
                self._unlock_lock_records(self._lock_records(state), owner_key, requested_start, requested_end)
                if not self._lock_records(state):
                    self._path_locks.pop(resource_key, None)
            return 0

        if self.owner.read_only and requested_type == fcntl.F_WRLCK:
            raise FuseOSError(errno.EROFS)

        while True:
            conflicting_state = None
            for other_owner, owner_state in self._iter_lock_records(state):
                if other_owner == owner_key:
                    continue
                if self._ranges_overlap(
                    requested_start,
                    requested_end,
                    owner_state["start"],
                    owner_state["end"],
                ) and self._lock_conflicts(owner_state, requested_type):
                    conflicting_state = owner_state
                    break
            if conflicting_state is None:
                with self._path_locks_guard:
                    records = self._lock_records(state)
                    self._unlock_lock_records(records, owner_key, requested_start, requested_end)
                    self._merge_lock_record(
                        records,
                        owner_key,
                        {
                            "type": requested_type,
                            "start": requested_start,
                            "end": requested_end,
                        },
                    )
                    logging.debug(
                        "lock granted path=%s owner=%s state=%s",
                        path,
                        owner_key,
                        records[owner_key],
                    )
                return 0

            if cmd == fcntl.F_SETLK:
                raise FuseOSError(errno.EWOULDBLOCK)
            time.sleep(0.1)
            state = self._get_lock_state(resource_key)

    def flock(self, path, fh, op):
        path = self.owner.normalize_path(path)
        kind, _ = self.owner.get_entry_kind_and_id(path)
        if kind not in {"file", "hardlink"}:
            raise FuseOSError(errno.EBADF)

        resource_key = self._lock_resource_key(path)
        owner_key = self._lock_owner_key(fh)
        lock_type = op & (fcntl.LOCK_SH | fcntl.LOCK_EX | fcntl.LOCK_UN)
        nonblocking = bool(op & fcntl.LOCK_NB)
        logging.debug("flock request path=%s fh=%s op=%s type=%s", path, fh, op, lock_type)

        if self._pg_lock_manager is not None:
            resource_key = self._lock_resource_key(path)
            if lock_type == fcntl.LOCK_UN:
                return self.release(resource_key, owner_key)
            return self._pg_lock_manager.acquire(resource_key, owner_key, lock_type, nonblocking=nonblocking)

        if lock_type == fcntl.LOCK_UN:
            self._clear_flock_state(resource_key, owner_key)
            return 0

        if lock_type not in {fcntl.LOCK_SH, fcntl.LOCK_EX}:
            raise FuseOSError(errno.EINVAL)

        while True:
            with self._path_locks_guard:
                state = self._flock_locks.setdefault(resource_key, {"records": {}})
                records = self._lock_records(state)
                owner_state = records.setdefault(owner_key, [])
                conflict = False
                for other_owner, other_state in self._iter_lock_records(state):
                    if other_owner == owner_key:
                        continue
                    if lock_type == fcntl.LOCK_SH and other_state.get("type") == fcntl.LOCK_EX:
                        conflict = True
                        break
                    if lock_type == fcntl.LOCK_EX and other_state.get("type") in {fcntl.LOCK_SH, fcntl.LOCK_EX}:
                        conflict = True
                        break
                if not conflict:
                    owner_state[:] = [{"type": lock_type, "count": 1}]
                    return 0

            if nonblocking:
                raise FuseOSError(errno.EWOULDBLOCK)
            time.sleep(0.1)

    def release(self, resource_key, owner_key):
        if self._pg_lock_manager is not None:
            self._pg_lock_manager.release(resource_key, owner_key)
        self._clear_flock_state(resource_key, owner_key)
        self._clear_path_lock_state(resource_key, owner_key)
        return 0
