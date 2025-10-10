# supertable/locking/redis_lock.py

from __future__ import annotations

import time
import uuid
import threading
import atexit
from typing import Iterable, Optional, Set, Dict

import redis
from supertable.config.defaults import logger


class RedisLocking:
    """
    Redis-based, multi-process safe lock manager using tokenized keys:
      lock:{resource} -> "<token>"  (SET NX EX)

    * Heartbeat refreshes TTL while locks are held.
    * `who` sidecar is optional (best effort) for observability:
        lockwho:{resource} -> "<identity>" (EX same TTL)
    """

    def __init__(
        self,
        identity: str,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        check_interval: float = 0.05,
        socket_timeout: float | None = 3.0,
        socket_connect_timeout: float | None = 3.0,
    ):
        self.identity = identity
        self.check_interval = max(0.01, float(check_interval))
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            decode_responses=True,
        )
        self._held: Dict[str, str] = {}  # resource -> token
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        atexit.register(self._on_exit)

    # ---------------- internals ----------------

    @staticmethod
    def _key(resource: str) -> str:
        return f"lock:{resource}"

    @staticmethod
    def _who(resource: str) -> str:
        return f"lockwho:{resource}"

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
        interval = max(1, int(duration // 2))
        while not self._hb_stop.is_set():
            time.sleep(interval)
            try:
                # refresh TTL for all held
                for res, token in list(self._held.items()):
                    key = self._key(res)
                    cur = self.redis.get(key)
                    if cur == token:
                        # extend key + who
                        self.redis.expire(key, duration)
                        try:
                            self.redis.set(self._who(res), self.identity, ex=duration)
                        except Exception:
                            pass
            except Exception as e:
                logger.debug(f"[redis-lock] heartbeat error: {e}")

    # ---------------- public API ----------------

    def acquire(self, resources: Iterable[str], duration: int = 30, who: str = "") -> bool:
        """
        Acquire all resources atomically. If any fails, none are kept.
        """
        resources = [str(r) for r in resources]
        tokens: Dict[str, str] = {}
        deadline = time.time() + 30

        while time.time() < deadline:
            try:
                # try to set all with pipeline
                pipe = self.redis.pipeline()
                tokens.clear()
                for r in resources:
                    t = uuid.uuid4().hex
                    tokens[r] = t
                    pipe.set(self._key(r), t, nx=True, ex=max(1, int(duration)))
                results = pipe.execute()

                if all(bool(ok) for ok in results):
                    # set who (best effort)
                    try:
                        p2 = self.redis.pipeline()
                        for r in resources:
                            p2.set(self._who(r), who or self.identity, ex=max(1, int(duration)))
                        p2.execute()
                    except Exception:
                        pass

                    self._held.update(tokens)
                    if not self._hb_thread:
                        self._start_heartbeat(duration)
                    return True

                # rollback partial acquisitions
                try:
                    p3 = self.redis.pipeline()
                    for r, t in tokens.items():
                        # delete only if we hold it
                        if self.redis.get(self._key(r)) == t:
                            p3.delete(self._key(r))
                            p3.delete(self._who(r))
                    p3.execute()
                except Exception:
                    pass

            except Exception as e:
                logger.debug(f"[redis-lock] acquire error: {e}")

            time.sleep(self.check_interval)

        return False

    def release(self, resources: Iterable[str]) -> None:
        resources = [str(r) for r in resources]
        try:
            p = self.redis.pipeline()
            for r in resources:
                key = self._key(r)
                tok = self._held.get(r)
                cur = self.redis.get(key)
                if cur and tok and cur == tok:
                    p.delete(key)
                    p.delete(self._who(r))
                    self._held.pop(r, None)
            p.execute()
        finally:
            if not self._held:
                self._stop_heartbeat()

    def who(self, resources: Iterable[str]) -> Dict[str, str]:
        res = [str(r) for r in resources]
        out: Dict[str, str] = {}
        try:
            p = self.redis.pipeline()
            for r in res:
                p.get(self._who(r))
            vals = p.execute()
            for r, v in zip(res, vals):
                if v:
                    out[r] = str(v)
        except Exception:
            pass
        return out

    # ---------------- cleanup ----------------

    def _on_exit(self):
        try:
            if self._held:
                self.release(list(self._held.keys()))
        except Exception:
            pass
        self._stop_heartbeat()
