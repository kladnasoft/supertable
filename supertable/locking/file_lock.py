# route: supertable.locking.file_lock
"""
POSIX fcntl-based local file locking for **single-host** development.

API is intentionally aligned with :class:`supertable.locking.redis_lock.RedisLocking`
so the two backends can be used interchangeably during local development.

Lock file structure (JSON list of lock records)::

    [
      {"res": "<key>", "exp": <unix_ts>, "tok": "<uuid_token>"}, ...
    ]
"""

from __future__ import annotations

import json
import math
import os
import time
import uuid
import fcntl
import threading
import atexit
from typing import Callable, Dict, List, Optional, Tuple

from supertable.config.defaults import logger


def _require_positive_ttl_ms(ttl_ms: object) -> int:
    """Return an exact positive integer TTL without coercing booleans/floats."""

    if type(ttl_ms) is not int or ttl_ms <= 0:
        raise ValueError("ttl_ms must be a positive integer")
    return ttl_ms


class FileLocking:
    """
    POSIX fcntl-based local file locking.

    Intended for single-host dev environments.  The public API mirrors
    :class:`~supertable.locking.redis_lock.RedisLocking` so the two
    backends are interchangeable::

        locker.acquire(key, ttl_s=30, timeout_s=10)  -> token | None
        locker.release(key, token)                    -> bool
        locker.extend(key, token, ttl_ms)             -> bool
    """

    def __init__(
        self,
        working_dir: str,
        lock_file_name: str = ".lock.json",
        retry_interval: float = 0.1,
    ):
        if not working_dir:
            raise ValueError("working_dir is required for FileLocking")
        self.retry_interval = max(0.01, float(retry_interval))
        self.lock_path = os.path.join(working_dir, lock_file_name)

        # key -> (token, ttl_seconds) for locks held by this instance.  The TTL
        # is per lease: one long-lived lock must never slow renewal of a shorter
        # lease held by the same process.
        self._held: Dict[str, Tuple[str, float]] = {}
        self._held_lock = threading.Lock()
        # Serialize expiry mutation per exact lease token.  A single process-
        # wide mutex lets an obsolete heartbeat stalled before file I/O block
        # renewal of every newer/disjoint lease.  Token-scoped locks preserve
        # explicit-extend ordering for the same lease while replacement
        # generations can skip a busy stale token and renew useful siblings.
        self._lease_op_locks: Dict[
            Tuple[str, str], Tuple[threading.Lock, int]
        ] = {}
        self._lease_op_locks_guard = threading.Lock()

        # Heartbeat state
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._owner_pid = os.getpid()

        os.makedirs(working_dir, exist_ok=True)
        # Initialize the shared document with a complete JSON value.  From this
        # point onward an empty/truncated file is evidence of an interrupted
        # writer and must fail closed rather than mean "there are no locks".
        try:
            fd = os.open(
                self.lock_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            pass
        else:
            try:
                os.write(fd, b"[]")
                os.fsync(fd)
            finally:
                os.close(fd)
        register_at_fork = getattr(os, "register_at_fork", None)
        if callable(register_at_fork):
            register_at_fork(after_in_child=self._reset_after_fork_in_child)
        atexit.register(self._on_exit)

    def _reset_after_fork_in_child(self) -> None:
        """Drop inherited process-local ownership without touching the file."""

        self._owner_pid = os.getpid()
        self._held = {}
        self._held_lock = threading.Lock()
        self._lease_op_locks = {}
        self._lease_op_locks_guard = threading.Lock()
        self._hb_stop = threading.Event()
        self._hb_thread = None

    # ------------------------------------------------------------------ internal helpers

    def _atomic_read_write(self, callback: Callable[[List[Dict]], List[Dict]]) -> List[Dict]:
        """
        Open (or create) the lock file under LOCK_EX, read current records,
        pass them through ``callback(records) -> new_records``, and write back.
        Returns the records that were written.

        The constructor creates a valid empty document with ``O_EXCL``.  A
        missing, empty, malformed, or structurally invalid file after that
        boundary is treated as unavailable lock state, never as an empty set.
        """
        with open(self.lock_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                data = f.read()
                if not data:
                    raise RuntimeError("file-lock state is empty or truncated")
                try:
                    records = json.loads(data)
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    raise RuntimeError("file-lock state is not valid JSON") from exc
                self._validate_records(records)
                new_records = callback(records)
                self._validate_records(new_records)
                f.seek(0)
                f.truncate(0)
                f.write(json.dumps(new_records, separators=(",", ":"), ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
                return new_records
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _read_file(self) -> List[Dict]:
        """Read records under a shared lock, failing closed on damaged state."""
        with open(self.lock_path, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                data = f.read()
                if not data:
                    raise RuntimeError("file-lock state is empty or truncated")
                try:
                    records = json.loads(data)
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    raise RuntimeError("file-lock state is not valid JSON") from exc
                self._validate_records(records)
                return records
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @staticmethod
    def _validate_records(records: object) -> None:
        if not isinstance(records, list):
            raise RuntimeError("file-lock state must be a JSON list")
        seen: set[str] = set()
        for record in records:
            if not isinstance(record, dict):
                raise RuntimeError("file-lock state contains a non-object record")
            resource = record.get("res")
            token = record.get("tok")
            expiry = record.get("exp")
            if (
                not isinstance(resource, str)
                or not resource
                or resource in seen
                or not isinstance(token, str)
                or not token
                or isinstance(expiry, bool)
                or not isinstance(expiry, (int, float))
                or not math.isfinite(float(expiry))
            ):
                raise RuntimeError("file-lock state contains an invalid record")
            seen.add(resource)

    @staticmethod
    def _purge_expired(records: List[Dict]) -> List[Dict]:
        now = time.time()
        return [r for r in records if float(r["exp"]) > now]

    # ------------------------------------------------------------------ public API

    def acquire(
        self,
        key: str,
        ttl_s: int = 30,
        timeout_s: int = 30,
        retry_interval: float | None = None,
    ) -> Optional[str]:
        """
        Acquire a lock on *key*.

        Returns a unique token (``str``) on success, or ``None`` on timeout.
        The token **must** be passed to :meth:`release` or :meth:`extend`.
        """
        token = uuid.uuid4().hex
        ttl_seconds = max(1.0, float(ttl_s))
        interval = retry_interval if retry_interval is not None else self.retry_interval
        deadline = time.time() + max(1, int(timeout_s))

        while time.time() < deadline:
            acquired = False

            def _try_acquire(records: List[Dict]) -> List[Dict]:
                nonlocal acquired
                records = self._purge_expired(records)

                # Conflict: key held by a different token
                for r in records:
                    if r.get("res") == key:
                        # Re-entrant: same token already holds it (shouldn't
                        # happen with UUIDs, but defensive)
                        if r.get("tok") == token:
                            acquired = True
                            return records
                        acquired = False
                        return records  # unchanged

                # Key is free — claim it
                records.append({
                    "res": key,
                    "exp": time.time() + ttl_seconds,
                    "tok": token,
                })
                acquired = True
                return records

            try:
                self._atomic_read_write(_try_acquire)
                if acquired:
                    restart_heartbeat = False
                    with self._held_lock:
                        previous_min_ttl = min(
                            (held_ttl for _, held_ttl in self._held.values()),
                            default=None,
                        )
                        self._held[key] = (token, ttl_seconds)
                        if self._hb_thread is None or not self._hb_thread.is_alive():
                            self._start_heartbeat_locked()
                        elif (
                            previous_min_ttl is not None
                            and ttl_seconds < previous_min_ttl
                        ):
                            # The existing generation may be asleep for half of
                            # a much longer lease. Restart it so the new shorter
                            # lease receives its first renewal in time.
                            restart_heartbeat = True
                    if restart_heartbeat:
                        self._stop_heartbeat(restart_if_held=True)
                    return token
            except Exception as e:
                logger.debug(f"[file-lock] acquire error on {key}: {e}")

            time.sleep(interval)

        return None

    def release(self, key: str, token: str) -> bool:
        """
        Release the lock on *key* only if it is still held by *token*.

        Uses ``_atomic_read_write`` to avoid TOCTOU races.
        Returns ``True`` if the lock was released, ``False`` otherwise.
        """
        released = False

        def _try_release(records: List[Dict]) -> List[Dict]:
            nonlocal released
            new_records: List[Dict] = []
            for r in records:
                if r.get("res") == key and r.get("tok") == token:
                    released = True
                    continue  # drop this record
                new_records.append(r)
            return new_records

        try:
            self._atomic_read_write(_try_release)
        except Exception as e:
            logger.debug(f"[file-lock] release error on {key}: {e}")
            return False
        finally:
            if released:
                should_stop_hb = False
                with self._held_lock:
                    held_entry = self._held.get(key)
                    if held_entry is not None and held_entry[0] == token:
                        self._held.pop(key, None)
                    if not self._held:
                        should_stop_hb = True
                if should_stop_hb:
                    self._stop_heartbeat(restart_if_held=True)

        return released

    def extend(self, key: str, token: str, ttl_ms: int) -> bool:
        """
        Extend the TTL of *key* only if it is still held by *token*.

        Returns ``True`` if the TTL was extended, ``False`` otherwise.
        """
        requested_ttl_ms = _require_positive_ttl_ms(ttl_ms)
        extended = False
        ttl_seconds = max(1.0, requested_ttl_ms / 1000.0)

        def _try_extend(records: List[Dict]) -> List[Dict]:
            nonlocal extended
            for r in records:
                if r.get("res") == key and r.get("tok") == token:
                    r["exp"] = time.time() + ttl_seconds
                    extended = True
                    break
            return records

        restart_heartbeat = False
        operation_lock = self._retain_lease_op_lock(
            key, token, blocking=True,
        )
        if operation_lock is None:  # pragma: no cover - blocking acquire
            raise RuntimeError("could not acquire file lease-operation lock")
        try:
            self._atomic_read_write(_try_extend)
            if extended:
                with self._held_lock:
                    held_entry = self._held.get(key)
                    if held_entry is not None and held_entry[0] == token:
                        previous_min_ttl = min(
                            held_ttl for _, held_ttl in self._held.values()
                        )
                        self._held[key] = (token, ttl_seconds)
                        if (
                            self._hb_thread is not None
                            and self._hb_thread.is_alive()
                            and ttl_seconds < previous_min_ttl
                        ):
                            restart_heartbeat = True
        except Exception as e:
            logger.debug(f"[file-lock] extend error on {key}: {e}")
            return False
        finally:
            self._drop_lease_op_lock_reference(
                key, token, operation_lock,
            )
        if restart_heartbeat:
            self._stop_heartbeat(restart_if_held=True)
        return extended

    def _retain_lease_op_lock(
        self,
        key: str,
        token: str,
        *,
        blocking: bool,
    ) -> Optional[threading.Lock]:
        """Retain and acquire the operation lock for one exact file lease."""

        operation_key = (key, token)
        with self._lease_op_locks_guard:
            entry = self._lease_op_locks.get(operation_key)
            if entry is None:
                lock = threading.Lock()
                references = 0
            else:
                lock, references = entry
            self._lease_op_locks[operation_key] = (lock, references + 1)
        acquired = lock.acquire(blocking=blocking)
        if acquired:
            return lock
        self._drop_lease_op_lock_reference(
            key, token, lock, release=False,
        )
        return None

    def _drop_lease_op_lock_reference(
        self,
        key: str,
        token: str,
        lock: threading.Lock,
        *,
        release: bool = True,
    ) -> None:
        """Release one operation-lock reference and remove its idle entry."""

        if release:
            lock.release()
        operation_key = (key, token)
        with self._lease_op_locks_guard:
            entry = self._lease_op_locks.get(operation_key)
            if entry is None or entry[0] is not lock:
                return
            references = entry[1] - 1
            if references <= 0:
                self._lease_op_locks.pop(operation_key, None)
            else:
                self._lease_op_locks[operation_key] = (lock, references)

    def who(self, key: str) -> Optional[str]:
        """
        Return the token currently holding *key*, or ``None``.

        Only returns non-expired holders.
        """
        records = self._purge_expired(self._read_file())
        for r in records:
            if r.get("res") == key:
                return r.get("tok")
        return None

    # ------------------------------------------------------------------ heartbeat

    def _start_heartbeat_locked(self) -> None:
        """Start one heartbeat generation while ``_held_lock`` is owned."""
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._hb_loop,
            args=(stop_event,),
            daemon=True,
        )
        self._hb_stop = stop_event
        self._hb_thread = thread
        thread.start()

    def _start_heartbeat(self) -> None:
        """Compatibility wrapper for tests and maintenance callers."""
        with self._held_lock:
            if self._hb_thread is None or not self._hb_thread.is_alive():
                self._start_heartbeat_locked()

    def _stop_heartbeat(self, *, restart_if_held: bool = False) -> None:
        """Stop one heartbeat generation without clobbering a newer one."""
        with self._held_lock:
            thread = self._hb_thread
            stop_event = self._hb_stop
        if thread is None:
            return
        stop_event.set()
        replacement_started = False
        with self._held_lock:
            if (
                restart_if_held
                and self._held
                and self._hb_thread is thread
            ):
                # Publish a new generation before joining the old one.  The
                # old heartbeat can be stalled before its shared-file update;
                # making a concurrently acquired one-second lease wait for the
                # bounded join would consume the lease before renewal starts.
                self._start_heartbeat_locked()
                replacement_started = True
        if (
            not replacement_started
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join(timeout=2.0)
        with self._held_lock:
            if self._hb_thread is thread:
                self._hb_thread = None
            if restart_if_held and self._held and (
                self._hb_thread is None or not self._hb_thread.is_alive()
            ):
                self._start_heartbeat_locked()

    def _hb_loop(self, stop_event: threading.Event) -> None:
        current = threading.current_thread()
        retry_delay_s: Optional[float] = None
        try:
            while not stop_event.is_set():
                with self._held_lock:
                    if not self._held:
                        break
                    min_ttl_s = min(ttl for _, ttl in self._held.values())
                interval = (
                    retry_delay_s
                    if retry_delay_s is not None
                    else max(0.05, min_ttl_s / 2.0)
                )
                stop_event.wait(timeout=interval)
                if stop_event.is_set():
                    break
                retry_delay_s = None

                with self._held_lock:
                    snapshot = dict(self._held)
                active: List[Tuple[str, str, float, threading.Lock]] = []
                for key, (token, ttl_s) in snapshot.items():
                    operation_lock = self._retain_lease_op_lock(
                        key, token, blocking=False,
                    )
                    if operation_lock is None:
                        retry_delay_s = min(
                            0.25, max(0.05, ttl_s / 10.0),
                        )
                        continue
                    with self._held_lock:
                        still_current = self._held.get(key) == (token, ttl_s)
                    if not still_current:
                        self._drop_lease_op_lock_reference(
                            key, token, operation_lock,
                        )
                        continue
                    active.append((key, token, ttl_s, operation_lock))

                if not active:
                    continue
                active_snapshot = {
                    key: (token, ttl_s)
                    for key, token, ttl_s, _operation_lock in active
                }
                refreshed: set[str] = set()
                try:
                    def _refresh(records: List[Dict]) -> List[Dict]:
                        records = self._purge_expired(records)
                        now = time.time()
                        for record in records:
                            resource = record.get("res")
                            if not isinstance(resource, str):
                                continue
                            held_entry = active_snapshot.get(resource)
                            if (
                                held_entry is not None
                                and record.get("tok") == held_entry[0]
                            ):
                                record["exp"] = now + held_entry[1]
                                refreshed.add(resource)
                        return records

                    self._atomic_read_write(_refresh)
                except Exception as e:
                    logger.debug(f"[file-lock] heartbeat error: {e}")
                    retry_delay_s = min(0.25, max(0.05, min_ttl_s / 10.0))
                    continue
                finally:
                    for key, token, _ttl_s, operation_lock in active:
                        self._drop_lease_op_lock_reference(
                            key, token, operation_lock,
                        )

                lost = set(active_snapshot) - refreshed
                if lost:
                    with self._held_lock:
                        for key in lost:
                            if self._held.get(key) == active_snapshot[key]:
                                self._held.pop(key, None)
        finally:
            with self._held_lock:
                if self._hb_thread is current:
                    self._hb_thread = None
                    if self._held and not stop_event.is_set():
                        self._start_heartbeat_locked()

    # ------------------------------------------------------------------ cleanup

    def _on_exit(self) -> None:
        if os.getpid() != self._owner_pid:
            # A fork child inherits atexit registrations and capability tokens,
            # but it must never release leases owned by the still-live parent.
            return
        try:
            with self._held_lock:
                snapshot = dict(self._held)
            for key, (token, _ttl_s) in snapshot.items():
                try:
                    self.release(key, token)
                except Exception:
                    pass
        except Exception:
            pass
        self._stop_heartbeat()
