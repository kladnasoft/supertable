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
import os
import time
import uuid
import fcntl
import threading
import atexit
from typing import Callable, Dict, List, Optional

from supertable.config.defaults import logger


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

        # key -> token for locks held by this instance
        self._held: Dict[str, str] = {}
        self._held_lock = threading.Lock()

        # Heartbeat state
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._hb_ttl_s: int = 30  # refreshed by acquire

        os.makedirs(working_dir, exist_ok=True)
        atexit.register(self._on_exit)

    # ------------------------------------------------------------------ internal helpers

    def _atomic_read_write(self, callback: Callable[[List[Dict]], List[Dict]]) -> List[Dict]:
        """
        Open (or create) the lock file under LOCK_EX, read current records,
        pass them through ``callback(records) -> new_records``, and write back.
        Returns the records that were written.

        Uses ``a+`` mode so the open itself creates the file atomically if
        missing — no TOCTOU between existence check and open.
        """
        with open(self.lock_path, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                data = f.read() or "[]"
                try:
                    records = json.loads(data)
                except Exception:
                    records = []
                new_records = callback(records)
                f.seek(0)
                f.truncate(0)
                f.write(json.dumps(new_records, separators=(",", ":"), ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
                return new_records
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _read_file(self) -> List[Dict]:
        """Read records under a shared lock.  Returns [] if file missing or corrupt."""
        if not os.path.exists(self.lock_path):
            return []
        with open(self.lock_path, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                data = f.read() or "[]"
                return json.loads(data)
            except Exception:
                return []
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @staticmethod
    def _purge_expired(records: List[Dict]) -> List[Dict]:
        now = int(time.time())
        return [r for r in records if int(r.get("exp", 0)) > now]

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
                now = int(time.time())
                records.append({"res": key, "exp": now + max(1, int(ttl_s)), "tok": token})
                acquired = True
                return records

            try:
                self._atomic_read_write(_try_acquire)
                if acquired:
                    with self._held_lock:
                        self._held[key] = token
                        self._hb_ttl_s = max(1, int(ttl_s))
                        if self._hb_thread is None or not self._hb_thread.is_alive():
                            self._start_heartbeat()
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
                    self._held.pop(key, None)
                    if not self._held:
                        should_stop_hb = True
                if should_stop_hb:
                    self._stop_heartbeat()

        return released

    def extend(self, key: str, token: str, ttl_ms: int) -> bool:
        """
        Extend the TTL of *key* only if it is still held by *token*.

        Returns ``True`` if the TTL was extended, ``False`` otherwise.
        """
        extended = False

        def _try_extend(records: List[Dict]) -> List[Dict]:
            nonlocal extended
            now = int(time.time())
            for r in records:
                if r.get("res") == key and r.get("tok") == token:
                    r["exp"] = now + max(1, int(ttl_ms) // 1000)
                    extended = True
                    break
            return records

        try:
            self._atomic_read_write(_try_extend)
        except Exception as e:
            logger.debug(f"[file-lock] extend error on {key}: {e}")
            return False

        return extended

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

    def _start_heartbeat(self) -> None:
        self._hb_stop.clear()
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
        self._hb_thread.start()

    def _stop_heartbeat(self) -> None:
        self._hb_stop.set()
        if self._hb_thread is not None and self._hb_thread.is_alive():
            self._hb_thread.join(timeout=1.0)
        self._hb_thread = None

    def _hb_loop(self) -> None:
        while not self._hb_stop.is_set():
            with self._held_lock:
                ttl_s = self._hb_ttl_s
            interval = max(1, ttl_s // 2)
            self._hb_stop.wait(timeout=interval)
            if self._hb_stop.is_set():
                break

            with self._held_lock:
                snapshot = dict(self._held)  # key -> token
            if not snapshot:
                continue

            try:
                ttl = ttl_s

                def _refresh(records: List[Dict]) -> List[Dict]:
                    now = int(time.time())
                    for r in records:
                        tok = r.get("tok")
                        if tok and tok in snapshot.values() and r.get("res") in snapshot:
                            r["exp"] = now + max(1, ttl)
                    return self._purge_expired(records)

                self._atomic_read_write(_refresh)
            except Exception as e:
                logger.debug(f"[file-lock] heartbeat error: {e}")

    # ------------------------------------------------------------------ cleanup

    def _on_exit(self) -> None:
        try:
            with self._held_lock:
                snapshot = dict(self._held)
            for key, token in snapshot.items():
                try:
                    self.release(key, token)
                except Exception:
                    pass
        except Exception:
            pass
        self._stop_heartbeat()
