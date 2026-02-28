# supertable/locking/redis_lock.py

from __future__ import annotations

import time
import uuid
import threading
import atexit
from typing import Iterable, Optional, Set, Dict

import redis
from supertable.config.defaults import logger


# Lua script for atomic release: only delete if current value matches token
_RELEASE_LUA = """
local key = KEYS[1]
local who_key = KEYS[2]
local expected_token = ARGV[1]
local current = redis.call('GET', key)
if current == expected_token then
    redis.call('DEL', key)
    redis.call('DEL', who_key)
    return 1
end
return 0
"""

# Lua script for atomic conditional delete (rollback): delete only if value matches
_COND_DEL_LUA = """
local key = KEYS[1]
local who_key = KEYS[2]
local expected_token = ARGV[1]
local current = redis.call('GET', key)
if current == expected_token then
    redis.call('DEL', key)
    redis.call('DEL', who_key)
end
return 0
"""

# Lua script for atomic heartbeat refresh: only extend TTL if token still matches
_REFRESH_LUA = """
local key = KEYS[1]
local who_key = KEYS[2]
local expected_token = ARGV[1]
local duration = tonumber(ARGV[2])
local identity = ARGV[3]
local current = redis.call('GET', key)
if current == expected_token then
    redis.call('EXPIRE', key, duration)
    redis.call('SET', who_key, identity, 'EX', duration)
    return 1
end
return 0
"""


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
        self._release_script = self.redis.register_script(_RELEASE_LUA)
        self._cond_del_script = self.redis.register_script(_COND_DEL_LUA)
        self._refresh_script = self.redis.register_script(_REFRESH_LUA)
        atexit.register(self._on_exit)

    # ---------------- internals ----------------

    @staticmethod
    def _key(resource: str) -> str:
        return f"lock:{resource}"

    @staticmethod
    def _who_key(resource: str) -> str:
        return f"lockwho:{resource}"

    # Keep old name as alias for backward compatibility
    _who = _who_key

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
                # refresh TTL for all held — atomically via Lua
                for res, token in list(self._held.items()):
                    try:
                        self._refresh_script(
                            keys=[self._key(res), self._who_key(res)],
                            args=[token, duration, self.identity],
                        )
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
                            p2.set(self._who_key(r), who or self.identity, ex=max(1, int(duration)))
                        p2.execute()
                    except Exception:
                        pass

                    self._held.update(tokens)
                    if not self._hb_thread:
                        self._start_heartbeat(duration)
                    return True

                # rollback partial acquisitions atomically using Lua
                for r, t in tokens.items():
                    try:
                        self._cond_del_script(
                            keys=[self._key(r), self._who_key(r)],
                            args=[t],
                        )
                    except Exception:
                        pass

            except Exception as e:
                logger.debug(f"[redis-lock] acquire error: {e}")

            time.sleep(self.check_interval)

        return False

    def release(self, resources: Iterable[str]) -> None:
        resources = [str(r) for r in resources]
        try:
            for r in resources:
                tok = self._held.get(r)
                if not tok:
                    continue
                try:
                    self._release_script(
                        keys=[self._key(r), self._who_key(r)],
                        args=[tok],
                    )
                except Exception:
                    pass
                self._held.pop(r, None)
        finally:
            if not self._held:
                self._stop_heartbeat()

    def who(self, resources: Iterable[str]) -> Dict[str, str]:
        res = [str(r) for r in resources]
        out: Dict[str, str] = {}
        try:
            p = self.redis.pipeline()
            for r in res:
                p.get(self._who_key(r))
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
