import json
import os
import secrets
import time
import fcntl
import threading
import atexit
import builtins
import random
from typing import Iterable, List, Dict, Optional, Set

from supertable.config.defaults import default, logger


class FileLocking:
    """
    File-based, multi-thread & multi-process safe lock manager.

    - JSON lock file guarded by OS fcntl exclusive lock
    - TTL-based entries with heartbeat extension
    - Subset/full release and atexit cleanup
    - DEBUG logs:
        * resources requested
        * current content snapshot
        * per-resource conflict lines:
          "[<identity>] lock blocked by <res> (held by <who>, TTL=<s>s), retrying…"
        * heartbeat lifecycle
    - Conflict semantics:
        * Acquire succeeds only if none of the requested resources is held by others.
    """

    def __init__(
        self,
        identity: str,
        working_dir: Optional[str],
        lock_file_name: str = ".lock.json",
        check_interval: float = 0.1,
    ):
        self.identity: str = identity
        self.lock_id: str = secrets.token_hex(8)  # unique to this instance

        base_dir = working_dir or os.getcwd()
        self.lock_file_dir: str = os.path.abspath(base_dir)
        self.lock_file_path: str = os.path.join(self.lock_file_dir, lock_file_name)
        self.check_interval: float = check_interval

        self._state_lock = threading.RLock()
        self._owned_resources: Set[str] = set()

        self._hb_thread: Optional[threading.Thread] = None
        self._hb_stop = threading.Event()
        self._hb_interval_sec: int = 0
        self._last_duration_sec: int = 0

        self._expiry_timer: Optional[threading.Timer] = None

        # rate-limit noisy OS-lock messages per instance/thread
        self._last_oslock_report: float = 0.0

        os.makedirs(self.lock_file_dir, exist_ok=True)
        if not os.path.exists(self.lock_file_path):
            with builtins.open(self.lock_file_path, "w") as f:
                json.dump([], f)

        atexit.register(self._atexit_cleanup)

    # ---------------- Internal helpers ----------------

    def _atexit_cleanup(self):
        try:
            self.release_lock()
        except Exception as e:
            try:
                logger.debug(f"{self.identity}: atexit release_lock error: {e}")
            except Exception:
                pass

        try:
            with getattr(self, "_state_lock", threading.RLock()):
                hb_thread = getattr(self, "_hb_thread", None)
                hb_stop = getattr(self, "_hb_stop", None)
                if hb_thread is not None and hb_stop is not None:
                    hb_stop.set()
                    self._hb_thread = None
        except Exception:
            pass

    def _read_lock_file(self, lock_file) -> List[Dict]:
        lock_file.seek(0)
        try:
            try:
                size = os.fstat(lock_file.fileno()).st_size
            except Exception:
                size = None
            if size == 0:
                logger.debug(f"{self.identity}: lock file empty (size=0), treating as [] at {self.lock_file_path}")
                return []
            data = json.load(lock_file)
            if isinstance(data, list):
                return data
            logger.debug(f"{self.identity}: unexpected lock file content type={type(data)}, treating as []")
        except json.JSONDecodeError as e:
            logger.debug(f"{self.identity}: JSON decode error reading lock file ({self.lock_file_path}): {e}; treating as []")
        except Exception as e:
            logger.debug(f"{self.identity}: error reading lock file ({self.lock_file_path}): {e}; treating as []")
        return []

    def _write_lock_file(self, lock_data: List[Dict], lock_file) -> None:
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(lock_data, lock_file, separators=(",", ":"))
        lock_file.flush()
        os.fsync(lock_file.fileno())

    def _remove_expired(self, lock_data: List[Dict]) -> List[Dict]:
        now = int(time.time())
        before = len(lock_data)
        filtered = [L for L in lock_data if int(L.get("exp", 0)) > now]
        expired = before - len(filtered)
        if expired and logger.isEnabledFor(10):
            logger.debug(f"{self.identity}: pruned {expired} expired lock entr{'y' if expired==1 else 'ies'}")
        return filtered

    def _ttl_of(self, exp: int) -> int:
        return max(0, int(exp) - int(time.time()))

    def _snapshot_str(self, data: List[Dict], max_chars: int = 600) -> str:
        try:
            s = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            if len(s) > max_chars:
                s = s[:max_chars] + f"...(+{len(s)-max_chars} chars)"
            return s
        except Exception:
            return "<unserializable>"

    def _sleep_backoff(self):
        time.sleep(min(0.25, self.check_interval * (0.75 + random.random() * 0.5)))

    def _log_os_locked_retry(self):
        now = time.time()
        if now - self._last_oslock_report > 0.75:
            logger.debug(f"{self.identity}: file is locked by another process; retrying…")
            self._last_oslock_report = now

    # ---------------- Public API ----------------

    def self_lock(
        self,
        timeout_seconds: int = 60,  # default longer wait (was default.DEFAULT_TIMEOUT_SEC)
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
    ) -> bool:
        return self.lock_resources(
            [self.identity],
            timeout_seconds=timeout_seconds,
            lock_duration_seconds=lock_duration_seconds,
        )

    def lock_resources(
        self,
        resources: List[str],
        timeout_seconds: int = 60,  # default longer wait (was default.DEFAULT_TIMEOUT_SEC)
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
    ) -> bool:
        """
        Acquire a lock on the given resources atomically (all-or-nothing).

        Conflict rule:
          - We block if any of the requested resources is currently held by another process.
        """
        start = time.time()

        if isinstance(resources, str):
            resources = [resources]
        elif not isinstance(resources, (list, tuple, set)):
            raise TypeError(f"resources must be a list/tuple/set of str, got {type(resources).__name__}")

        # ensure all are strings
        if not all(isinstance(r, str) for r in resources):
            raise TypeError("all resources must be str")

        # de-dup while preserving order
        resources = list(dict.fromkeys(resources))

        logger.debug(
            f"{self.identity}: attempting file-lock on resources={resources} "
            f"(timeout={timeout_seconds}s, duration={lock_duration_seconds}s, path={self.lock_file_path})"
        )

        while time.time() - start < timeout_seconds:
            # In-process overlap guard (avoid re-entrant conflict in the same process)
            with self._state_lock:
                owned = set(self._owned_resources)
                req = set(resources)
                inproc_conflict = owned & req

                if inproc_conflict:
                    for res in sorted(inproc_conflict):
                        logger.debug(f"[{self.identity}] lock blocked by {res} (held by {self.identity}, TTL=?s), retrying…")
                    self._sleep_backoff()
                    continue

            # Open or create lock file
            try:
                f = builtins.open(self.lock_file_path, "r+")
            except FileNotFoundError:
                try:
                    os.makedirs(self.lock_file_dir, exist_ok=True)
                    with builtins.open(self.lock_file_path, "w") as initf:
                        json.dump([], initf)
                    f = builtins.open(self.lock_file_path, "r+")
                    logger.debug(f"{self.identity}: created new lock file at {self.lock_file_path}")
                except Exception as e:
                    logger.error(f"{self.identity}: cannot create lock file: {e}")
                    self._sleep_backoff()
                    continue

            with f:
                # OS-level non-blocking lock
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self._log_os_locked_retry()
                    self._sleep_backoff()
                    continue

                try:
                    # Read + GC expired
                    data = self._read_lock_file(f)
                    data = self._remove_expired(data)
                    if logger.isEnabledFor(10):
                        logger.debug(f"{self.identity}: current lock content={self._snapshot_str(data)}")

                    # Check conflicts with others (direct overlap only)
                    owned_by_others = [L for L in data if L.get("pid") != self.lock_id]
                    conflict_entry: Optional[Dict] = None

                    req_set = set(resources)
                    for lock in owned_by_others:
                        lock_res = set(lock.get("res", []))
                        if any(r in lock_res for r in req_set):
                            conflict_entry = lock
                            break

                    if conflict_entry:
                        ttl = self._ttl_of(int(conflict_entry.get("exp", 0)))
                        holder = conflict_entry.get("who") or conflict_entry.get("pid")
                        overlap = sorted(set(resources) & set(conflict_entry.get("res", []))) or ["<unknown>"]
                        for res in overlap:
                            logger.debug(f"[{self.identity}] lock blocked by {res} (held by {holder}, TTL={ttl}s), retrying…")
                        logger.debug(
                            f"{self.identity}: conflict blocking acquisition → "
                            f"pid={conflict_entry.get('pid')} res={conflict_entry.get('res')} "
                            f"exp={conflict_entry.get('exp')} (ttl≈{ttl}s)"
                        )
                        self._sleep_backoff()
                        continue

                    # Append/merge our entry for these resources
                    now = int(time.time())
                    exp = now + int(lock_duration_seconds)

                    ours = [L for L in data if L.get("pid") == self.lock_id]
                    if ours:
                        before = set(ours[0].get("res", []))
                        after = sorted(list(before | set(resources)))
                        ours[0]["res"] = after
                        ours[0]["exp"] = max(int(ours[0].get("exp", 0)), exp)
                        ours[0]["who"] = self.identity  # keep 'who' updated
                        logger.debug(
                            f"{self.identity}: merging resources into existing entry "
                            f"(before={sorted(before)}, after={after}, exp={ours[0]['exp']})"
                        )
                    else:
                        data.append({"pid": self.lock_id, "who": self.identity, "exp": exp, "res": list(resources)})
                        logger.debug(f"{self.identity}: creating new lock entry pid={self.lock_id} res={resources} exp={exp}")

                    self._write_lock_file(data, f)

                    with self._state_lock:
                        self._owned_resources |= set(resources)
                        self._last_duration_sec = int(lock_duration_seconds)
                        self._start_heartbeat_if_needed()

                    logger.debug(f"{self.identity}: file lock acquired on {resources}, exp={exp}")
                    return True
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)

            self._sleep_backoff()

        logger.debug(f"{self.identity}: FAILED to acquire file lock on {resources} within {timeout_seconds}s")
        return False

    def release_lock(self, resources: Optional[Iterable[str]] = None) -> None:
        with self._state_lock:
            targets = set(resources) if resources is not None else set(self._owned_resources)

        if resources is not None and not targets:
            logger.debug(f"{self.identity}: release called with empty/unknown resources → nothing to do")
            return

        if not os.path.exists(self.lock_file_path):
            logger.warning(f"{self.identity}: lock file missing at release: {self.lock_file_path}")
            with self._state_lock:
                if resources is None:
                    self._owned_resources.clear()
                else:
                    self._owned_resources -= targets
                if not self._owned_resources:
                    self._stop_heartbeat_nolock()
                    if self._expiry_timer:
                        self._expiry_timer.cancel()
                        self._expiry_timer = None
            return

        try:
            with builtins.open(self.lock_file_path, "r+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    data = self._read_lock_file(f)
                    data = self._remove_expired(data)

                    idx = next((i for i, L in enumerate(data) if L.get("pid") == self.lock_id), None)
                    if idx is not None:
                        entry = data[idx]
                        current_res = set(entry.get("res", []))
                        if resources is None:
                            logger.debug(f"{self.identity}: releasing ALL resources {sorted(current_res)}")
                            del data[idx]
                            with self._state_lock:
                                self._owned_resources.clear()
                        else:
                            remainder = current_res - targets
                            logger.debug(
                                f"{self.identity}: releasing subset {sorted(targets)}; "
                                f"remaining after release={sorted(remainder)}"
                            )
                            entry["res"] = sorted(list(remainder))
                            entry["who"] = self.identity
                            with self._state_lock:
                                self._owned_resources -= targets
                            if not entry["res"]:
                                del data[idx]

                    self._write_lock_file(data, f)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except FileNotFoundError:
            logger.warning(f"{self.identity}: lock file disappeared during release: {self.lock_file_path}")
        except Exception as e:
            logger.error(f"{self.identity}: release failed: {e}")

        with self._state_lock:
            if not self._owned_resources:
                self._stop_heartbeat_nolock()
            if self._expiry_timer:
                self._expiry_timer.cancel()
                self._expiry_timer = None

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire file lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()

    def __del__(self):
        try:
            self.release_lock()
        except Exception:
            pass

    # ---------------- Heartbeat support ----------------

    def _start_heartbeat_if_needed(self):
        interval = max(1, int(self._last_duration_sec // 2) or 1)
        self._hb_interval_sec = interval
        if self._hb_thread is None or not self._hb_thread.is_alive():
            self._hb_stop.clear()
            t = threading.Thread(target=self._heartbeat_loop, name=f"FileLockHB-{self.identity}", daemon=True)
            t.start()
            self._hb_thread = t
            logger.debug(f"{self.identity}: file-lock heartbeat started (interval={interval}s)")

    def _stop_heartbeat_nolock(self):
        if self._hb_thread is not None:
            self._hb_stop.set()
            self._hb_thread = None
            logger.debug(f"{self.identity}: file-lock heartbeat stopped")

    def _heartbeat_loop(self):
        while not self._hb_stop.wait(max(1, int(self._hb_interval_sec or 1))):
            with self._state_lock:
                still_own = bool(self._owned_resources)
                duration = max(1, int(self._last_duration_sec or default.DEFAULT_LOCK_DURATION_SEC))
            if not still_own:
                break

            try:
                with builtins.open(self.lock_file_path, "r+") as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
                    try:
                        data = self._read_lock_file(f)
                        data = self._remove_expired(data)
                        for L in data:
                            if L.get("pid") == self.lock_id and L.get("res"):
                                L["exp"] = int(time.time()) + duration
                                L["who"] = self.identity
                                break
                        self._write_lock_file(data, f)
                    finally:
                        fcntl.flock(f, fcntl.LOCK_UN)
            except FileNotFoundError:
                logger.debug(f"{self.identity}: heartbeat skipped, lock file missing")
            except Exception as e:
                logger.debug(f"{self.identity}: heartbeat error: {e}")
