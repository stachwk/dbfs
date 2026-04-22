from __future__ import annotations

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
        with self.owner.db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS lock_leases (
                            id_lock SERIAL PRIMARY KEY,
                            resource_kind VARCHAR(20) NOT NULL,
                            resource_id BIGINT NOT NULL,
                            owner_key BIGINT NOT NULL,
                            lease_kind VARCHAR(20) NOT NULL,
                            lock_type INTEGER NOT NULL,
                            lease_expires_at TIMESTAMP NOT NULL,
                            heartbeat_at TIMESTAMP NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            UNIQUE(resource_kind, resource_id, owner_key, lease_kind)
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_lock_leases_resource
                        ON lock_leases (resource_kind, resource_id, lease_kind)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_lock_leases_expires
                        ON lock_leases (lease_expires_at)
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS lock_range_leases (
                            id_lock SERIAL PRIMARY KEY,
                            resource_kind VARCHAR(20) NOT NULL,
                            resource_id BIGINT NOT NULL,
                            owner_key BIGINT NOT NULL,
                            lock_type INTEGER NOT NULL,
                            range_start BIGINT NOT NULL,
                            range_end BIGINT NULL,
                            lease_expires_at TIMESTAMP NOT NULL,
                            heartbeat_at TIMESTAMP NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_resource
                        ON lock_range_leases (resource_kind, resource_id)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_expires
                        ON lock_range_leases (lease_expires_at)
                        """
                    )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

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

    def _flock_conflicts(self, requested_type, other_type):
        if requested_type == fcntl.LOCK_SH:
            return other_type == fcntl.LOCK_EX
        if requested_type == fcntl.LOCK_EX:
            return other_type in {fcntl.LOCK_SH, fcntl.LOCK_EX}
        return False

    def _prune_expired(self, cur, resource_key=None):
        if resource_key is None:
            cur.execute("DELETE FROM lock_leases WHERE lease_expires_at <= NOW()")
            return
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            DELETE FROM lock_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND lease_kind = %s
              AND lease_expires_at <= NOW()
            """,
            (resource_kind, resource_id, self.LOCK_KIND),
        )

    def _find_conflict(self, cur, resource_key, owner_key, requested_type):
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            SELECT owner_key, lock_type
            FROM lock_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND lease_kind = %s
              AND lease_expires_at > NOW()
              AND owner_key <> %s
            ORDER BY owner_key
            """,
            (resource_kind, resource_id, self.LOCK_KIND, owner_key),
        )
        for _, other_type in cur.fetchall():
            if self._flock_conflicts(requested_type, other_type):
                return True
        return False

    def _upsert_lease(self, cur, resource_key, owner_key, lock_type):
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            INSERT INTO lock_leases (
                resource_kind,
                resource_id,
                owner_key,
                lease_kind,
                lock_type,
                lease_expires_at,
                heartbeat_at,
                created_at,
                updated_at
            ) VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                NOW() + (%s || ' seconds')::interval,
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT (resource_kind, resource_id, owner_key, lease_kind)
            DO UPDATE SET
                lock_type = EXCLUDED.lock_type,
                lease_expires_at = EXCLUDED.lease_expires_at,
                heartbeat_at = EXCLUDED.heartbeat_at,
                updated_at = NOW()
            """,
            (
                resource_kind,
                resource_id,
                owner_key,
                self.LOCK_KIND,
                int(lock_type),
                self.lease_ttl_seconds,
            ),
        )

    def _delete_lease(self, cur, resource_key, owner_key):
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            DELETE FROM lock_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND owner_key = %s
              AND lease_kind = %s
            """,
            (resource_kind, resource_id, owner_key, self.LOCK_KIND),
        )

    def _delete_range_leases(self, cur, resource_key, owner_key=None):
        resource_kind, resource_id = resource_key
        if owner_key is None:
            cur.execute(
                """
                DELETE FROM lock_range_leases
                WHERE resource_kind = %s
                  AND resource_id = %s
                """,
                (resource_kind, resource_id),
            )
            return
        cur.execute(
            """
            DELETE FROM lock_range_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND owner_key = %s
            """,
            (resource_kind, resource_id, owner_key),
        )

    def _load_range_state(self, cur, resource_key):
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            SELECT owner_key, lock_type, range_start, range_end
            FROM lock_range_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND lease_expires_at > NOW()
            ORDER BY owner_key, range_start, COALESCE(range_end, 9223372036854775807)
            """,
            (resource_kind, resource_id),
        )
        state = {}
        for owner_key, lock_type, start, end in cur.fetchall():
            state.setdefault(int(owner_key), []).append(
                {"type": int(lock_type), "start": int(start), "end": None if end is None else int(end)}
            )
        return state

    def _persist_range_state(self, cur, resource_key, state):
        self._delete_range_leases(cur, resource_key)
        resource_kind, resource_id = resource_key
        for owner_key, records in state.items():
            for record in records:
                cur.execute(
                    """
                    INSERT INTO lock_range_leases (
                        resource_kind,
                        resource_id,
                        owner_key,
                        lock_type,
                        range_start,
                        range_end,
                        lease_expires_at,
                        heartbeat_at,
                        created_at,
                        updated_at
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        NOW() + (%s || ' seconds')::interval,
                        NOW(),
                        NOW(),
                        NOW()
                    )
                    """,
                    (
                        resource_kind,
                        resource_id,
                        int(owner_key),
                        int(record["type"]),
                        int(record["start"]),
                        record["end"],
                        self.lease_ttl_seconds,
                    ),
                )

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

    def _heartbeat_range_leases(self):
        snapshot = self._range_active_snapshot()
        if not snapshot:
            return
        with self.owner.db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    for resource_kind, resource_id, owner_key, lease_kind, range_start, range_end in snapshot:
                        cur.execute(
                            """
                            UPDATE lock_range_leases
                            SET lease_expires_at = NOW() + (%s || ' seconds')::interval,
                                heartbeat_at = NOW(),
                                updated_at = NOW()
                            WHERE resource_kind = %s
                              AND resource_id = %s
                              AND owner_key = %s
                              AND range_start = %s
                              AND range_end IS NOT DISTINCT FROM %s
                            """,
                            (self.lease_ttl_seconds, resource_kind, resource_id, owner_key, range_start, range_end),
                        )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logging.debug("Failed to heartbeat PostgreSQL range leases", exc_info=True)

    def _heartbeat_active_leases(self):
        snapshot = self._active_snapshot()
        if not snapshot:
            return
        with self.owner.db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    for lease in snapshot:
                        if len(lease) == 4:
                            resource_kind, resource_id, owner_key, lease_kind = lease
                            if lease_kind != self.LOCK_KIND:
                                continue
                            cur.execute(
                                """
                                UPDATE lock_leases
                                SET lease_expires_at = NOW() + (%s || ' seconds')::interval,
                                    heartbeat_at = NOW(),
                                    updated_at = NOW()
                                WHERE resource_kind = %s
                                  AND resource_id = %s
                                  AND owner_key = %s
                                  AND lease_kind = %s
                                """,
                                (self.lease_ttl_seconds, resource_kind, resource_id, owner_key, lease_kind),
                            )
                            continue
                        resource_kind, resource_id, owner_key, lease_kind, range_start, range_end = lease
                        if lease_kind != self.RANGE_KIND:
                            continue
                        cur.execute(
                            """
                            UPDATE lock_range_leases
                            SET lease_expires_at = NOW() + (%s || ' seconds')::interval,
                                heartbeat_at = NOW(),
                                updated_at = NOW()
                            WHERE resource_kind = %s
                              AND resource_id = %s
                              AND owner_key = %s
                              AND range_start = %s
                              AND range_end IS NOT DISTINCT FROM %s
                            """,
                            (self.lease_ttl_seconds, resource_kind, resource_id, owner_key, range_start, range_end),
                        )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logging.debug("Failed to heartbeat PostgreSQL lock leases", exc_info=True)

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
        while True:
            with self.owner.db_connection() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (resource_lock_id,))
                        if not cur.fetchone()[0]:
                            conn.rollback()
                            if nonblocking:
                                raise FuseOSError(errno.EWOULDBLOCK)
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._prune_expired(cur, resource_key)
                        if self._find_conflict(cur, resource_key, owner_key, requested_type):
                            conn.rollback()
                            if nonblocking:
                                raise FuseOSError(errno.EWOULDBLOCK)
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._upsert_lease(cur, resource_key, owner_key, requested_type)
                    conn.commit()
                    self._register_active(resource_key, owner_key)
                    return 0
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise

            if nonblocking:
                raise FuseOSError(errno.EWOULDBLOCK)

    def release(self, resource_key, owner_key):
        resource_lock_id = self._resource_hash(resource_key)
        while True:
            with self.owner.db_connection() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (resource_lock_id,))
                        if not cur.fetchone()[0]:
                            conn.rollback()
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._delete_lease(cur, resource_key, owner_key)
                        self._delete_range_leases(cur, resource_key, owner_key)
                    conn.commit()
                    self._unregister_active(resource_key, owner_key)
                    self._clear_range_active(resource_key, owner_key)
                    return 0
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise

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
            with self.owner.db_connection() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (resource_lock_id,))
                        if not cur.fetchone()[0]:
                            conn.rollback()
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._prune_expired_range(cur, resource_key)
                        state = self._load_range_state(cur, resource_key)
                        _, conflict = self._find_range_conflict(state, owner_key, requested_type, requested_start, requested_end)
                        if conflict is None:
                            return None
                        return conflict
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise

    def _prune_expired_range(self, cur, resource_key=None):
        if resource_key is None:
            cur.execute("DELETE FROM lock_range_leases WHERE lease_expires_at <= NOW()")
            return
        resource_kind, resource_id = resource_key
        cur.execute(
            """
            DELETE FROM lock_range_leases
            WHERE resource_kind = %s
              AND resource_id = %s
              AND lease_expires_at <= NOW()
            """,
            (resource_kind, resource_id),
        )

    def acquire_range(self, resource_key, owner_key, requested_type, requested_start, requested_end, nonblocking=False):
        if requested_type not in {fcntl.F_RDLCK, fcntl.F_WRLCK}:
            raise FuseOSError(errno.EINVAL)

        resource_lock_id = self._resource_hash(resource_key)
        while True:
            with self.owner.db_connection() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (resource_lock_id,))
                        if not cur.fetchone()[0]:
                            conn.rollback()
                            if nonblocking:
                                raise FuseOSError(errno.EWOULDBLOCK)
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._prune_expired_range(cur, resource_key)
                        state = self._load_range_state(cur, resource_key)
                        _, conflict = self._find_range_conflict(state, owner_key, requested_type, requested_start, requested_end)
                        if conflict is not None:
                            conn.rollback()
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
                        self._set_range_state(cur, resource_key, state)
                    conn.commit()
                    return 0
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise

    def unlock_range(self, resource_key, owner_key, unlock_start, unlock_end):
        resource_lock_id = self._resource_hash(resource_key)
        while True:
            with self.owner.db_connection() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (resource_lock_id,))
                        if not cur.fetchone()[0]:
                            conn.rollback()
                            time.sleep(self.poll_interval_seconds)
                            continue
                        self._prune_expired_range(cur, resource_key)
                        state = self._load_range_state(cur, resource_key)
                        self._unlock_range_records(state, owner_key, unlock_start, unlock_end)
                        self._set_range_state(cur, resource_key, state)
                    conn.commit()
                    return 0
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
