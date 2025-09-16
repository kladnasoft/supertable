# redis_lock.py

import time
import secrets
import threading
import atexit
import random
from typing import Iterable, Optional, Set, List

import redis
from supertable.config.defaults import default, logger


class RedisLocking:
    """
    Redis-based, multi-thread & multi-process safe lock manager.

    Semantics intentionally mirror FileLocking (but implemented on Redis):
      - All-or-nothing acquisition for a requested set of resources
      - TTL-based lock with background heartbeat extension
      - Subset and full release supported
      - In-process overlap guard (don't deadlock yourself)
      - Context manager support
      - atexit cleanup
      - DEBUG logs:
          * resources requested
          * per-resource conflict lines:
            "[<identity>] lock blocked by <res> (held by <who>, TTL=<s>s), retrying…"
          * heartbeat lifecycle
    """

    def __init__(
        self,
        identity: str,
        check_interval: float = 0.1,
        redis_client: Optional[redis.Redis] = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
    ):
        self.identity: str = identity
        self.lock_id: str = secrets.token_hex(8)  # unique to this instance
        self.check_interval: float = check_interval

        self.redis = redis_client or redis.Redis(host=host, port=port, db=db, password=password)

        # State
        self._state_lock = threading.RLock()
        self._owned_keys: Set[str] = set()  # "lock:<resource>"

        # Heartbeat state
        self._hb_thread: Optional[threading.Thread] = None
        self._hb_stop = threading.Event()
        self._hb_interval_sec: int = 0
        self._last_duration_sec: int = 0

        atexit.register(self._atexit_cleanup)

    # ---------------- Internals ----------------

    def _atexit_cleanup(self):
        try:
            self.release_lock()
        except Exception as e:
            try:
                logger.debug(f"{self.identity}: atexit release_lock error: {e}")
            except Exception:
                pass
        # Best-effort stop HB
        try:
            with getattr(self, "_state_lock", threading.RLock()):
                hb_thread = getattr(self, "_hb_thread", None)
                hb_stop = getattr(self, "_hb_stop", None)
                if hb_thread is not None and hb_stop is not None:
                    hb_stop.set()
                    self._hb_thread = None
        except Exception:
            pass

    @staticmethod
    def _lock_key(resource: str) -> str:
        return f"lock:{resource}"

    @staticmethod
    def _who_key(resource: str) -> str:
        return f"lock:{resource}:who"

    def _sleep_backoff(self):
        # Similar jitter/backoff as file backend
        time.sleep(min(0.25, self.check_interval * (0.75 + random.random() * 0.5)))

    def _start_heartbeat_if_needed(self, lock_duration_seconds: int):
        interval = max(1, int(lock_duration_seconds // 2) or 1)
        with self._state_lock:
            self._last_duration_sec = int(lock_duration_seconds)
            self._hb_interval_sec = interval
            if self._hb_thread is None or not self._hb_thread.is_alive():
                self._hb_stop.clear()
                t = threading.Thread(
                    target=self._heartbeat_loop, name=f"RedisLockHB-{self.identity}", daemon=True
                )
                t.start()
                self._hb_thread = t
                logger.debug(
                    f"{self.identity}: redis heartbeat started (interval={interval}s, duration={lock_duration_seconds}s)"
                )

    # ---------------- Public API ----------------

    def self_lock(
        self,
        timeout_seconds: int = 60,  # match FileLocking default
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
    ) -> bool:
        return self.lock_resources(
            [self.identity],
            timeout_seconds=timeout_seconds,
            lock_duration_seconds=lock_duration_seconds,
        )

    def lock_resources(
        self,
        resources: Iterable[str],
        timeout_seconds: int = 60,  # match FileLocking default
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
    ) -> bool:
        """
        Acquire a lock on the given resources atomically (all-or-nothing).

        Conflict rule:
          - We block if any of the requested resources is currently held by another process.
        """
        start_time = time.time()

        # Normalize/validate resources just like FileLocking
        if isinstance(resources, str):
            resources = [resources]
        elif not isinstance(resources, (list, tuple, set)):
            raise TypeError(f"resources must be a list/tuple/set of str, got {type(resources).__name__}")

        if not all(isinstance(r, str) for r in resources):
            raise TypeError("all resources must be str")

        # de-dup while preserving order
        resources = list(dict.fromkeys(resources))
        keys: List[str] = [self._lock_key(r) for r in resources]
        expiration = int(lock_duration_seconds)

        logger.debug(
            f"{self.identity}: attempting redis-lock on resources={resources} "
            f"(timeout={timeout_seconds}s, duration={lock_duration_seconds}s)"
        )

        while time.time() - start_time < timeout_seconds:
            # In-process overlap guard (avoid self-conflict)
            with self._state_lock:
                owned = set(self._owned_keys)
                req = set(keys)
                inproc_conflict = owned & req
                if inproc_conflict:
                    # Try to log per-resource, mirroring file backend
                    for key in sorted(inproc_conflict):
                        res = key.split("lock:", 1)[1]
                        logger.debug(f"[{self.identity}] lock blocked by {res} (held by {self.identity}, TTL=?s), retrying…")
                    self._sleep_backoff()
                    continue

            acquired = []
            try:
                all_ok = True
                for res in resources:
                    key = self._lock_key(res)
                    who = self._who_key(res)

                    # SET key=lock_id NX EX=expiration
                    ok = self.redis.set(key, self.lock_id, ex=expiration, nx=True)
                    if ok:
                        # Sidecar 'who' is best-effort metadata
                        try:
                            self.redis.set(who, self.identity, ex=expiration)
                        except Exception as e:
                            logger.debug(f"{self.identity}: unable to set who key for {res}: {e}")
                        acquired.append((key, who))
                        logger.debug(f"{self.identity}: redis set OK key={key} exp={expiration}")
                    else:
                        all_ok = False
                        # Conflict introspection (best-effort)
                        try:
                            cur = self.redis.get(key)
                            ttl = self.redis.ttl(key)
                            holder_id = cur.decode() if cur else None
                            holder_who = self.redis.get(who)
                            holder_who = holder_who.decode() if holder_who else holder_id
                            logger.debug(
                                f"[{self.identity}] lock blocked by {res} (held by {holder_who}, TTL={ttl}s), retrying…"
                            )
                        except Exception as ie:
                            logger.debug(f"{self.identity}: redis conflict introspection failed for res={res}: {ie}")
                        break

                if all_ok:
                    with self._state_lock:
                        self._owned_keys.update(k for k, _ in acquired)
                    self._start_heartbeat_if_needed(expiration)
                    logger.debug(f"{self.identity}: redis lock acquired on {resources}")
                    return True

                # Roll back any partial acquisitions
                for key, who in acquired:
                    try:
                        current_value = self.redis.get(key)
                        if current_value and current_value.decode() == self.lock_id:
                            self.redis.delete(key)
                            logger.debug(f"{self.identity}: rolled back key={key}")
                        try:
                            self.redis.delete(who)
                        except Exception:
                            pass
                    except Exception as re:
                        logger.debug(f"{self.identity}: rollback error for key={key}: {re}")

                self._sleep_backoff()

            except Exception as e:
                logger.debug(f"{self.identity}: redis lock acquisition error: {e}")
                self._sleep_backoff()

        logger.debug(f"{self.identity}: FAILED to acquire redis lock on {resources} within {timeout_seconds}s")
        return False

    def release_lock(self, resources: Optional[Iterable[str]] = None) -> None:
        """
        Release all or a subset of currently owned resources.
        Mirrors FileLocking.release_lock semantics.
        """
        if resources is None:
            # Release everything we think we own
            with self._state_lock:
                keys = list(self._owned_keys)

            for key in keys:
                res = key.split("lock:", 1)[1]
                who = self._who_key(res)
                try:
                    cur = self.redis.get(key)
                    if cur and cur.decode() == self.lock_id:
                        self.redis.delete(key)
                        logger.debug(f"{self.identity}: released key={key}")
                    try:
                        self.redis.delete(who)
                    except Exception:
                        pass
                except Exception as e:
                    logger.debug(f"{self.identity}: error releasing key={key}: {e}")

            with self._state_lock:
                self._owned_keys.clear()
                if self._hb_thread is not None:
                    self._hb_stop.set()
                    self._hb_thread = None
                    logger.debug(f"{self.identity}: redis heartbeat stopped")
            return

        # Subset release
        targets = set(self._lock_key(r) for r in resources)
        with self._state_lock:
            owned_snapshot = set(self._owned_keys)

        for key in (targets & owned_snapshot):
            res = key.split("lock:", 1)[1]
            who = self._who_key(res)
            try:
                cur = self.redis.get(key)
                if cur and cur.decode() == self.lock_id:
                    self.redis.delete(key)
                    logger.debug(f"{self.identity}: released key={key}")
                try:
                    self.redis.delete(who)
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"{self.identity}: error releasing key={key}: {e}")

        with self._state_lock:
            self._owned_keys.difference_update(targets)
            if not self._owned_keys and self._hb_thread is not None:
                self._hb_stop.set()
                self._hb_thread = None
                logger.debug(f"{self.identity}: redis heartbeat stopped")

    # ---------------- Context manager & finalizer ----------------

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire Redis lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()

    def __del__(self):
        try:
            self.release_lock()
        except Exception:
            pass

    # ---------------- Heartbeat -----------------

    def _heartbeat_loop(self):
        while not self._hb_stop.wait(max(1, int(self._hb_interval_sec or 1))):
            with self._state_lock:
                keys = list(self._owned_keys)
                duration = max(1, int(self._last_duration_sec or default.DEFAULT_LOCK_DURATION_SEC))
                still_own = bool(keys)
            if not still_own:
                break

            for key in keys:
                try:
                    val = self.redis.get(key)
                    if val and val.decode() == self.lock_id:
                        # extend the lock TTL
                        self.redis.expire(key, duration)
                        # keep sidecar who key in sync
                        res = key.split("lock:", 1)[1]
                        who = self._who_key(res)
                        try:
                            self.redis.set(who, self.identity, ex=duration)
                        except Exception:
                            pass
                except Exception as e:
                    logger.debug(f"{self.identity}: redis heartbeat error on {key}: {e}")
        # Stopped message is logged on release()
