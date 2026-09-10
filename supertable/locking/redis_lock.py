# route: supertable.locking.redis_lock
"""
Generic Redis-backed distributed lock.

Uses SET NX EX for acquisition with retry/backoff, and Lua scripts for
atomic compare-and-delete (release) and compare-and-extend (extend).

A background heartbeat thread automatically extends held locks at half-TTL,
so operations of any duration are safe. The TTL controls *crash recovery
time*, not operation timeout: if the holder dies, the heartbeat dies with
it, and the lock expires within one TTL cycle.

This module does NOT create its own Redis connection. It receives a
``redis.Redis`` client from the caller — ensuring it always participates
in whatever connection topology (Sentinel, SSL, etc.) the application uses.

Usage::

    from supertable.redis_connector import create_redis_client
    from supertable.locking.redis_lock import RedisLocking

    locker = RedisLocking(create_redis_client())
    token = locker.acquire("my:lock:key", ttl_s=30, timeout_s=10)
    if token:
        try:
            ...
        finally:
            locker.release("my:lock:key", token)
"""

from __future__ import annotations

import atexit
import time
import threading
import uuid
from collections import OrderedDict
from typing import Dict, Optional, Tuple

import redis
from supertable.config.defaults import logger


# -- Lua scripts (atomic server-side operations) ----------------------------- #

_LUA_RELEASE_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('DEL', key)
  return 1
end
return 0
"""

_LUA_EXTEND_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
return 0
"""


class RedisLocking:
    """
    Generic Redis distributed lock with automatic heartbeat renewal.

    Receives an already-configured ``redis.Redis`` client — never creates
    its own connection.  This guarantees the lock traffic follows the same
    Sentinel / SSL / password / DB path as every other Redis consumer.

    While locks are held, a background thread extends their TTL at half
    the original duration.  On crash, the thread dies and Redis expires
    the key after one TTL cycle — giving fast crash recovery without
    risking silent lock loss during long operations.
    """

    def __init__(self, r: redis.Redis) -> None:
        self.r = r
        self._release_if_token = self.r.register_script(_LUA_RELEASE_IF_TOKEN)
        self._extend_if_token = self.r.register_script(_LUA_EXTEND_IF_TOKEN)

        # key -> (token, ttl_ms) for locks held by this instance
        self._held: Dict[str, Tuple[str, int]] = {}
        self._held_lock = threading.Lock()

        # (key, token) pairs whose ownership this instance has provably lost
        # (a heartbeat extend returned 0 => expired or stolen).  Recorded so
        # the loss is observable by the holder instead of only being logged;
        # cleared when the same key is acquired again.  Bounded so a
        # long-running process cannot grow it without limit.
        self._lost: "OrderedDict[Tuple[str, str], None]" = OrderedDict()
        self._lost_max = 1024

        # Heartbeat state
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None

        atexit.register(self._on_exit)

    # ------------------------------------------------------------------ acquire

    def acquire(
        self,
        key: str,
        ttl_s: int = 30,
        timeout_s: int = 30,
        retry_interval: float = 0.05,
    ) -> Optional[str]:
        """
        Attempt to acquire a lock on *key* using SET NX EX.

        Retries with ``retry_interval`` sleep until ``timeout_s`` elapses.
        Returns a unique token (``str``) on success, or ``None`` on timeout.
        The token **must** be passed to :meth:`release` or :meth:`extend`.

        A background heartbeat automatically extends the lock at half-TTL
        while it is held.  The ``ttl_s`` controls crash recovery time only.
        """
        token = uuid.uuid4().hex
        ttl_ms = max(1000, int(ttl_s) * 1000)
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    with self._held_lock:
                        self._held[key] = (token, ttl_ms)
                        self._lost.pop((key, token), None)
                        if self._hb_thread is None or not self._hb_thread.is_alive():
                            self._start_heartbeat()
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-lock] acquire error on {key}: {e}")
            time.sleep(retry_interval)
        return None

    # ------------------------------------------------------------------ ownership

    def is_held(self, key: str, token: str) -> bool:
        """Return True only if *token* still owns *key*.

        Two independent signals, both required:

          * the heartbeat has not recorded a loss for this (key, token);
          * Redis still stores exactly *token* at *key*.

        A Redis error is reported as NOT held: the caller uses this to decide
        whether it is safe to publish a mutation, and "unknown" must not be
        treated as "safe".
        """
        with self._held_lock:
            if (key, token) in self._lost:
                return False
        try:
            cur = self.r.get(key)
        except redis.RedisError as e:
            logger.warning(f"[redis-lock] ownership check failed on {key}: {e}")
            return False
        if isinstance(cur, bytes):
            cur = cur.decode("utf-8", "replace")
        return cur == token

    # ------------------------------------------------------------------ release

    def release(self, key: str, token: str) -> bool:
        """
        Release the lock on *key* only if it is still held by *token*.

        Uses a Lua script for atomic compare-and-delete.
        Returns ``True`` if the lock was released, ``False`` otherwise.
        """
        try:
            res = self._release_if_token(keys=[key], args=[token])
            released = int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] release error on {key}: {e}")
            released = False

        # Always remove from held tracking (even if Lua returned 0, the key
        # may have expired — either way this instance no longer owns it).
        # Decide whether to stop heartbeat inside the lock, but execute
        # _stop_heartbeat outside to avoid deadlock (heartbeat thread also
        # acquires _held_lock).
        should_stop_hb = False
        with self._held_lock:
            held_entry = self._held.get(key)
            if held_entry is not None and held_entry[0] == token:
                del self._held[key]
            if not self._held:
                should_stop_hb = True

        if should_stop_hb:
            self._stop_heartbeat()

        return released

    # ------------------------------------------------------------------ extend

    def extend(self, key: str, token: str, ttl_ms: int) -> bool:
        """
        Extend the TTL of *key* only if it is still held by *token*.

        Uses a Lua script for atomic compare-and-extend.
        Returns ``True`` if the TTL was extended, ``False`` otherwise.
        """
        try:
            res = self._extend_if_token(keys=[key], args=[token, int(ttl_ms)])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] extend error on {key}: {e}")
            return False

    # ------------------------------------------------------------------ heartbeat

    def _start_heartbeat(self) -> None:
        self._hb_stop.clear()
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
        self._hb_thread.start()

    def _stop_heartbeat(self) -> None:
        self._hb_stop.set()
        if self._hb_thread is not None and self._hb_thread.is_alive():
            self._hb_thread.join(timeout=2.0)
        self._hb_thread = None

    def _hb_loop(self) -> None:
        """
        Refresh all held locks at half their TTL.

        The sleep interval adapts to the shortest TTL across all currently
        held locks.  Uses ``Event.wait`` so ``_stop_heartbeat`` can interrupt
        immediately without waiting for the full interval.
        """
        while not self._hb_stop.is_set():
            # Compute sleep interval from held locks
            with self._held_lock:
                if not self._held:
                    break
                min_ttl_ms = min(ttl_ms for _, ttl_ms in self._held.values())

            # Sleep for half the shortest TTL
            interval_s = max(1.0, (min_ttl_ms / 1000.0) / 2.0)
            self._hb_stop.wait(timeout=interval_s)
            if self._hb_stop.is_set():
                break

            # Snapshot held locks under the lock, then extend outside of it
            with self._held_lock:
                snapshot = dict(self._held)

            for key, (token, ttl_ms) in snapshot.items():
                try:
                    ok = self.extend(key, token, ttl_ms)
                    if not ok:
                        # Lock expired or was stolen.  Record the loss so the
                        # holder can find out (is_held) instead of continuing
                        # to mutate shared state believing it is protected —
                        # a debug line alone is invisible to the caller.
                        logger.warning(
                            f"[redis-lock] heartbeat: LOST lock on {key} "
                            f"(expired or stolen); holder is no longer protected"
                        )
                        with self._held_lock:
                            self._lost[(key, token)] = None
                            while len(self._lost) > self._lost_max:
                                self._lost.popitem(last=False)
                            held_entry = self._held.get(key)
                            if held_entry is not None and held_entry[0] == token:
                                del self._held[key]
                except Exception as e:
                    logger.debug(f"[redis-lock] heartbeat error on {key}: {e}")

    # ------------------------------------------------------------------ cleanup

    def _on_exit(self) -> None:
        """Best-effort release of all held locks on interpreter shutdown."""
        try:
            with self._held_lock:
                snapshot = dict(self._held)
            for key, (token, _ttl_ms) in snapshot.items():
                try:
                    self.release(key, token)
                except Exception:
                    pass
        except Exception:
            pass
        self._stop_heartbeat()
