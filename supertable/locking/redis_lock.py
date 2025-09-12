import time
import secrets
import threading
import atexit
import random
from typing import Iterable, Optional, Set

import redis
from supertable.config.defaults import default, logger


class RedisLocking:
    """
    Redis-based distributed lock manager.

    - Key per resource: lock:<resource> = <lock_id>, EX=TTL
    - Sidecar identity key: lock:<resource>:who = <identity>, EX matches
    - All-or-nothing acquisition; partials rolled back
    - Heartbeat extends TTL while held
    - DEBUG logs:
        * desired resources
        * per-resource conflict lines like:
          "[<identity>] lock blocked by <res> (held by <who>, TTL=<s>s), retrying…"
        * heartbeat lifecycle
    """

    def __init__(
        self,
        identity: str,
        check_interval: float = 0.1,
        redis_client: Optional[redis.Redis] = None,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None
    ):
        self.identity = identity
        self.lock_id = secrets.token_hex(8)
        self.check_interval = check_interval
        self.redis = redis_client or redis.Redis(host=host, port=port, db=db, password=password)

        self._owned_keys: Set[str] = set()
        self._state_lock = threading.Lock()

        self._hb_thread: Optional[threading.Thread] = None
        self._hb_stop   = threading.Event()
        self._hb_interval_sec: int = 0
        self._last_duration_sec: int = 0

        atexit.register(self._atexit_cleanup)

    # ---------------- internals ----------------

    def _atexit_cleanup(self):
        try:
            self.release_lock()
        except Exception as e:
            try:
                logger.debug(f"{self.identity}: atexit release_lock error: {e}")
            except Exception:
                pass

    def _lock_key(self, resource: str) -> str:
        return f"lock:{resource}"

    def _who_key(self, resource: str) -> str:
        return f"lock:{resource}:who"

    def _start_heartbeat_if_needed(self, lock_duration_seconds: int):
        refresh_interval = max(1, int(lock_duration_seconds // 2))
        with self._state_lock:
            self._last_duration_sec = int(lock_duration_seconds)
            self._hb_interval_sec = int(refresh_interval)
            if self._hb_thread is None or not self._hb_thread.is_alive():
                self._hb_stop.clear()
                t = threading.Thread(target=self._heartbeat_loop, name=f"RedisLockHB-{self.identity}", daemon=True)
                t.start()
                self._hb_thread = t
                logger.debug(f"{self.identity}: redis heartbeat started (interval={refresh_interval}s, duration={lock_duration_seconds}s)")

    def _sleep_backoff(self):
        time.sleep(min(0.25, self.check_interval * (0.75 + random.random() * 0.5)))

    # ---------------- public API ----------------

    def lock_resources(
        self,
        resources: Iterable[str],
        timeout_seconds: int = 60,  # default longer wait
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC
    ) -> bool:
        start_time = time.time()
        expiration = int(lock_duration_seconds)
        resources = list(resources)

        logger.debug(f"{self.identity}: attempting redis-lock on resources={resources} "
                     f"(timeout={timeout_seconds}s, duration={lock_duration_seconds}s)")

        while time.time() - start_time < timeout_seconds:
            acquired = []
            try:
                all_ok = True
                for res in resources:
                    key = self._lock_key(res)
                    who = self._who_key(res)
                    # Try SET NX EX on the main key
                    result = self.redis.set(key, self.lock_id, ex=expiration, nx=True)
                    if result:
                        # Set/refresh the sidecar who key
                        try:
                            self.redis.set(who, self.identity, ex=expiration)
                        except Exception as e:
                            logger.debug(f"{self.identity}: unable to set who key for {res}: {e}")
                        acquired.append((key, who))
                        logger.debug(f"{self.identity}: redis set OK key={key} exp={expiration}")
                    else:
                        all_ok = False
                        # Inspect conflict holder & TTL (read-only)
                        try:
                            cur = self.redis.get(key)
                            ttl = self.redis.ttl(key)
                            holder_id = cur.decode() if cur else None
                            holder_who = self.redis.get(who)
                            holder_who = holder_who.decode() if holder_who else holder_id
                            logger.debug(f"[{self.identity}] lock blocked by {res} (held by {holder_who}, TTL={ttl}s), retrying…")
                        except Exception as ie:
                            logger.debug(f"{self.identity}: redis conflict introspection failed for res={res}: {ie}")
                        break

                if all_ok:
                    with self._state_lock:
                        self._owned_keys.update(k for k, _ in acquired)
                    self._start_heartbeat_if_needed(expiration)
                    logger.debug(f"{self.identity}: redis lock acquired on {resources}")
                    return True
                else:
                    # Roll back any partial acquisitions we own
                    for key, who in acquired:
                        try:
                            current_value = self.redis.get(key)
                            if current_value and current_value.decode() == self.lock_id:
                                self.redis.delete(key)
                                logger.debug(f"{self.identity}: rolled back key={key}")
                            # best-effort cleanup who key
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

    def self_lock(self, timeout_seconds: int = 60, lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC):
        return self.lock_resources([self.identity], timeout_seconds, lock_duration_seconds)

    def release_lock(self, resources=None):
        if resources is None:
            with self._state_lock:
                keys = list(self._owned_keys)
            for key in keys:
                res = key.split("lock:", 1)[1]
                who = self._who_key(res)
                try:
                    current_value = self.redis.get(key)
                    if current_value and current_value.decode() == self.lock_id:
                        self.redis.delete(key)
                        logger.debug(f"{self.identity}: released key={key}")
                    # best-effort delete of who key
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

        targets = set(self._lock_key(r) for r in resources)
        with self._state_lock:
            owned_snapshot = set(self._owned_keys)

        for key in (targets & owned_snapshot):
            res = key.split("lock:", 1)[1]
            who = self._who_key(res)
            try:
                current_value = self.redis.get(key)
                if current_value and current_value.decode() == self.lock_id:
                    self.redis.delete(key)
                    logger.debug(f"{self.identity}: released key={key}")
                # best-effort delete of who key
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
        with self._state_lock:
            interval = max(1, int(self._hb_interval_sec or 1))
            duration = max(1, int(self._last_duration_sec or default.DEFAULT_LOCK_DURATION_SEC))

        while not self._hb_stop.wait(interval):
            with self._state_lock:
                keys = list(self._owned_keys)

            for key in keys:
                try:
                    val = self.redis.get(key)
                    if val and val.decode() == self.lock_id:
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
        # stopped message logged on release
