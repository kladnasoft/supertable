# supertable/locking/file_lock.py

from __future__ import annotations

import json
import os
import time
import fcntl
import threading
import atexit
from typing import Iterable, List, Dict, Optional, Set

from supertable.config.defaults import logger


class FileLocking:
    """
    Simple, POSIX fcntl-based local file locking. Intended for **single-host**
    development environments. It remains available as a fallback, but Redis is
    the default for multi-host/process safe locks.

    Lock file structure (JSON list of lock records):
    [
      {"res": "<resource>", "exp": <unix_ts>, "pid": "<identity>", "who": "<human-id>"}, ...
    ]
    """

    def __init__(
        self,
        identity: str,
        working_dir: str | None,
        lock_file_name: str = ".lock.json",
        check_interval: float = 0.1,
    ):
        if not working_dir:
            raise ValueError("working_dir is required for FileLocking")
        self.identity = identity
        self.check_interval = max(0.01, float(check_interval))
        self.lock_path = os.path.join(working_dir, lock_file_name)
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._held: Set[str] = set()
        os.makedirs(working_dir, exist_ok=True)
        atexit.register(self._on_exit)

    # ---------------- internal helpers ----------------

    def _read_file(self) -> List[Dict]:
        if not os.path.exists(self.lock_path):
            return []
        with open(self.lock_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                try:
                    f.seek(0)
                    data = f.read() or "[]"
                    return json.loads(data)
                except Exception:
                    return []
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _write_file(self, records: List[Dict]) -> None:
        # Use "a+" to avoid truncation before the lock is acquired.
        # We truncate manually after getting LOCK_EX.
        with open(self.lock_path, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                f.truncate(0)
                f.write(json.dumps(records, separators=(",", ":"), ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _atomic_read_write(self, records_callback) -> List[Dict]:
        """
        Open the lock file under an exclusive lock, read current records,
        pass them through `records_callback(records) -> new_records`, and
        write back. Returns the records that were written. This prevents
        TOCTOU races between _read_file and _write_file.
        """
        # Ensure file exists
        if not os.path.exists(self.lock_path):
            with open(self.lock_path, "w") as f:
                f.write("[]")

        with open(self.lock_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                data = f.read() or "[]"
                try:
                    records = json.loads(data)
                except Exception:
                    records = []
                new_records = records_callback(records)
                f.seek(0)
                f.truncate(0)
                f.write(json.dumps(new_records, separators=(",", ":"), ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
                return new_records
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _purge_expired(self, records: List[Dict]) -> List[Dict]:
        now = int(time.time())
        return [r for r in records if int(r.get("exp", 0)) > now]

    # ---------------- public API ----------------

    def acquire(self, resources: Iterable[str], duration: int = 30, who: str = "") -> bool:
        """
        Acquire all resources atomically. Returns True if acquired.
        """
        resources = [str(r) for r in resources]
        deadline = time.time() + 30  # hard cap on busy-wait
        while time.time() < deadline:
            acquired = False

            def _try_acquire(records: List[Dict]) -> List[Dict]:
                nonlocal acquired
                records = self._purge_expired(records)

                # Conflict check: ignore resources held by self (re-entrant)
                held_by_others = {
                    r["res"] for r in records
                    if r.get("pid") != self.identity
                }
                if any(r in held_by_others for r in resources):
                    acquired = False
                    return records  # unchanged

                # Remove our own stale entries for these resources
                records = [
                    r for r in records
                    if not (r.get("res") in resources and r.get("pid") == self.identity)
                ]

                now = int(time.time())
                exp = now + max(1, int(duration))
                for r in resources:
                    records.append({"res": r, "exp": exp, "pid": self.identity, "who": who})
                acquired = True
                return records

            try:
                self._atomic_read_write(_try_acquire)
                if acquired:
                    self._held.update(resources)
                    if not self._hb_thread:
                        self._start_heartbeat(duration)
                    return True
            except Exception as e:
                logger.debug(f"[file-lock] write error: {e}")

            time.sleep(self.check_interval)

        return False

    def release(self, resources: Iterable[str]) -> None:
        resources = {str(r) for r in resources}
        if not resources:
            return
        records = self._read_file()
        remained = [r for r in records if r.get("res") not in resources or r.get("pid") != self.identity]
        try:
            self._write_file(remained)
        finally:
            self._held.difference_update(resources)
            if not self._held:
                self._stop_heartbeat()

    def who(self, resources: Iterable[str]) -> Dict[str, str]:
        """
        Returns a dict {resource: who_str} for held resources.
        """
        resources = {str(r) for r in resources}
        out: Dict[str, str] = {}
        records = self._read_file()
        for r in records:
            res = str(r.get("res", ""))
            if res in resources:
                out[res] = str(r.get("who", ""))
        return out

    # ---------------- heartbeat ----------------

    def _start_heartbeat(self, duration: int):
        self._hb_stop.clear()
        self._hb_thread = threading.Thread(target=self._hb_loop, args=(duration,), daemon=True)
        self._hb_thread.start()

    def _stop_heartbeat(self):
        self._hb_stop.set()
        if self._hb_thread and self._hb_thread.is_alive():
            self._hb_thread.join(timeout=1.0)
        self._hb_thread = None

    def _hb_loop(self, duration: int):
        # refresh half-duration
        interval = max(1, int(duration // 2))
        while not self._hb_stop.is_set():
            time.sleep(interval)
            try:
                if not self._held:
                    continue

                identity = self.identity
                held_snapshot = self._held.copy()

                def _refresh(records: List[Dict]) -> List[Dict]:
                    now = int(time.time())
                    for r in records:
                        if r.get("pid") == identity and r.get("res") in held_snapshot:
                            r["exp"] = now + max(1, int(duration))
                    return self._purge_expired(records)

                self._atomic_read_write(_refresh)
            except Exception as e:
                logger.debug(f"[file-lock] heartbeat error: {e}")

    # ---------------- cleanup ----------------

    def _on_exit(self):
        try:
            if self._held:
                self.release(self._held.copy())
        except Exception:
            pass
        self._stop_heartbeat()
