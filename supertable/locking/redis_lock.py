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
import os
import time
import threading
import uuid
from typing import Dict, Optional, Tuple

import redis
from supertable.config.defaults import logger


def _require_positive_ttl_ms(ttl_ms: object) -> int:
    """Return an exact positive integer TTL without coercing booleans/floats."""

    if type(ttl_ms) is not int or ttl_ms <= 0:
        raise ValueError("ttl_ms must be a positive integer")
    return ttl_ms


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

_LUA_EXTEND_MANY_IF_TOKENS = """
-- redis-lock extend-many-if-tokens
local results = {}
for i = 1, #KEYS do
  local token = ARGV[(i - 1) * 2 + 1]
  local ttl_ms = tonumber(ARGV[(i - 1) * 2 + 2])
  -- A corrupt/non-string key is loss of this lease, not permission to abort
  -- renewal of every healthy sibling that follows it in the batch.
  local cur = redis.pcall('GET', KEYS[i])
  if type(cur) ~= 'table' and cur and cur == token then
    redis.call('PEXPIRE', KEYS[i], ttl_ms)
    results[i] = 1
  else
    results[i] = 0
  end
end
return results
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
        self._extend_many_if_tokens = self.r.register_script(
            _LUA_EXTEND_MANY_IF_TOKENS
        )

        # key -> (token, ttl_ms) for locks held by this instance
        self._held: Dict[str, Tuple[str, int]] = {}
        self._held_lock = threading.Lock()
        # Serialize TTL mutation and local metadata per key.  A process-wide
        # mutex would let a stalled Redis operation for one lease prevent the
        # heartbeat from renewing an unrelated short-lived lease.  Entries are
        # reference-counted so a workload with many transient keys does not
        # leave an unbounded lock registry behind.
        self._lease_op_locks: Dict[
            Tuple[str, str], Tuple[threading.Lock, int]
        ] = {}
        self._lease_op_locks_guard = threading.Lock()

        # Heartbeat state
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._owner_pid = os.getpid()

        register_at_fork = getattr(os, "register_at_fork", None)
        if callable(register_at_fork):
            register_at_fork(after_in_child=self._reset_after_fork_in_child)
        atexit.register(self._on_exit)

    def _reset_after_fork_in_child(self) -> None:
        """Drop inherited lease capabilities and dead thread primitives."""

        self._owner_pid = os.getpid()
        self._held = {}
        self._held_lock = threading.Lock()
        self._lease_op_locks = {}
        self._lease_op_locks_guard = threading.Lock()
        self._hb_stop = threading.Event()
        self._hb_thread = None

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
                    restart_heartbeat = False
                    with self._held_lock:
                        previous_entry = self._held.get(key)
                        previous_min_ttl = min(
                            (held_ttl for _, held_ttl in self._held.values()),
                            default=None,
                        )
                        self._held[key] = (token, ttl_ms)
                        if self._hb_thread is None or not self._hb_thread.is_alive():
                            self._start_heartbeat_locked()
                        elif (
                            previous_min_ttl is not None
                            and (
                                ttl_ms < previous_min_ttl
                                or previous_entry is not None
                            )
                        ):
                            # The current generation may still be sleeping for
                            # half of a much longer lease, or blocked renewing
                            # an expired prior token for this same key. Wake a
                            # new generation so the new token is renewed in
                            # time; token-scoped operation locks let it bypass
                            # the obsolete generation safely.
                            restart_heartbeat = True
                    if restart_heartbeat:
                        self._stop_heartbeat(restart_if_held=True)
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-lock] acquire error on {key}: {e}")
            time.sleep(retry_interval)
        return None

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
            # Another acquire can race between the empty-held decision above
            # and stopping the old thread.  Recheck/restart after joining so a
            # newly acquired long-running mutation never loses lease renewal.
            self._stop_heartbeat(restart_if_held=True)

        return released

    # ------------------------------------------------------------------ extend

    def _retain_lease_op_lock(
        self,
        key: str,
        token: str,
        *,
        blocking: bool,
    ) -> Optional[threading.Lock]:
        """Retain and acquire the operation lock for exactly one Redis key."""

        with self._lease_op_locks_guard:
            operation_key = (key, token)
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
        with self._lease_op_locks_guard:
            operation_key = (key, token)
            entry = self._lease_op_locks.get(operation_key)
            if entry is None or entry[0] is not lock:
                return
            references = entry[1] - 1
            if references <= 0:
                self._lease_op_locks.pop(operation_key, None)
            else:
                self._lease_op_locks[operation_key] = (lock, references)

    def extend(self, key: str, token: str, ttl_ms: int) -> bool:
        """
        Extend the TTL of *key* only if it is still held by *token*.

        Uses a Lua script for atomic compare-and-extend.
        Returns ``True`` if the TTL was extended, ``False`` otherwise.
        """
        # A Lua 0 is definitive: the key is absent or another token owns it.
        # A transport exception is ambiguous (Redis may even have applied the
        # PEXPIRE before the reply was lost), so propagate it. The heartbeat
        # retains tracking and retries; treating ambiguity as False would
        # abandon a valid long-running compaction lease after one timeout.
        restart_heartbeat = False
        requested_ttl_ms = _require_positive_ttl_ms(ttl_ms)
        operation_lock = self._retain_lease_op_lock(
            key, token, blocking=True,
        )
        if operation_lock is None:  # pragma: no cover - blocking acquire
            raise RuntimeError("could not acquire Redis lease-operation lock")
        try:
            res = self._extend_if_token(keys=[key], args=[token, requested_ttl_ms])
            extended = int(res or 0) == 1
            if extended:
                with self._held_lock:
                    held_entry = self._held.get(key)
                    if held_entry is not None and held_entry[0] == token:
                        previous_min_ttl = min(
                            held_ttl for _, held_ttl in self._held.values()
                        )
                        self._held[key] = (token, requested_ttl_ms)
                        if (
                            self._hb_thread is not None
                            and self._hb_thread.is_alive()
                            and requested_ttl_ms < previous_min_ttl
                        ):
                            # PEXPIRE has already shortened the lease, so interrupt
                            # an older long sleep before returning to the caller.
                            restart_heartbeat = True
        finally:
            self._drop_lease_op_lock_reference(key, token, operation_lock)
        if restart_heartbeat:
            self._stop_heartbeat(restart_if_held=True)
        return extended

    # ------------------------------------------------------------------ heartbeat

    def _start_heartbeat_locked(self) -> None:
        """Start one heartbeat generation while ``_held_lock`` is owned."""
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._hb_loop, args=(stop_event,), daemon=True,
        )
        self._hb_stop = stop_event
        self._hb_thread = thread
        thread.start()

    def _start_heartbeat(self) -> None:
        """Compatibility wrapper used by tests and maintenance callers."""
        with self._held_lock:
            if self._hb_thread is None or not self._hb_thread.is_alive():
                self._start_heartbeat_locked()

    def _stop_heartbeat(self, *, restart_if_held: bool = False) -> None:
        """Stop the captured heartbeat generation without clobbering a newer one.

        ``release`` passes ``restart_if_held=True`` to close the race where a
        second key is acquired after release observed an empty held set but
        before the old heartbeat was joined.  Direct callers may intentionally
        disable renewal for expiry tests, so the default remains no restart.
        """
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
                # Publish the replacement before waiting for the old
                # generation.  The old thread may be stuck in a Redis call;
                # making a newly acquired short lease wait for join(timeout)
                # can consume its entire TTL before renewal even begins.
                self._start_heartbeat_locked()
                replacement_started = True
        if (
            not replacement_started
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join(timeout=2.0)
        with self._held_lock:
            # An acquire may already have installed a new generation.  Never
            # clear its pointer or signal its distinct stop event.
            if self._hb_thread is thread:
                self._hb_thread = None
            if restart_if_held and self._held and (
                self._hb_thread is None or not self._hb_thread.is_alive()
            ):
                self._start_heartbeat_locked()

    def _hb_loop(self, stop_event: threading.Event) -> None:
        """
        Refresh all held locks at half their TTL.

        The sleep interval adapts to the shortest TTL across all currently
        held locks.  Uses ``Event.wait`` so ``_stop_heartbeat`` can interrupt
        immediately without waiting for the full interval.
        """
        current = threading.current_thread()
        retry_delay_s: Optional[float] = None
        try:
            while not stop_event.is_set():
                # Compute sleep interval from held locks
                with self._held_lock:
                    if not self._held:
                        break
                    min_ttl_ms = min(ttl_ms for _, ttl_ms in self._held.values())

                # Sleep for half the shortest TTL
                interval_s = (
                    retry_delay_s
                    if retry_delay_s is not None
                    else max(0.05, (min_ttl_ms / 1000.0) / 2.0)
                )
                stop_event.wait(timeout=interval_s)
                if stop_event.is_set():
                    break
                retry_delay_s = None

                # Snapshot held locks under the lock, then extend outside of it
                with self._held_lock:
                    snapshot = dict(self._held)

                active: list[Tuple[str, str, int, threading.Lock]] = []
                for key, (token, ttl_ms) in snapshot.items():
                    operation_lock = self._retain_lease_op_lock(
                        key,
                        token,
                        blocking=False,
                    )
                    if operation_lock is None:
                        # Another generation or an explicit extend owns only
                        # this key's mutation boundary. It must not hold up the
                        # remaining batch.
                        retry_delay_s = min(
                            1.0, max(0.1, (ttl_ms / 1000.0) / 10.0)
                        )
                        continue
                    with self._held_lock:
                        still_current = self._held.get(key) == (token, ttl_ms)
                    if not still_current:
                        self._drop_lease_op_lock_reference(
                            key, token, operation_lock,
                        )
                        continue
                    active.append((key, token, ttl_ms, operation_lock))

                if not active:
                    continue
                try:
                    # One server-side batch prevents a slow/lost key from
                    # becoming a separate network round trip in front of every
                    # sibling lease. Token checks and PEXPIRE remain atomic per
                    # key inside the Lua invocation.
                    results = self._extend_many_if_tokens(
                        keys=[key for key, _, _, _ in active],
                        args=[
                            value
                            for _, token, ttl_ms, _ in active
                            for value in (token, ttl_ms)
                        ],
                    )
                    if not isinstance(results, (list, tuple)) or (
                        len(results) != len(active)
                    ):
                        raise RuntimeError(
                            "Redis heartbeat returned an incomplete batch result"
                        )
                except Exception as e:
                    logger.debug(f"[redis-lock] heartbeat batch error: {e}")
                    retry_delay_s = min(
                        1.0,
                        max(
                            0.1,
                            min(ttl_ms for _, _, ttl_ms, _ in active)
                            / 1000.0
                            / 10.0,
                        ),
                    )
                    continue
                finally:
                    for key, token, _, operation_lock in active:
                        self._drop_lease_op_lock_reference(
                            key, token, operation_lock,
                        )

                for (key, token, _ttl_ms, _operation_lock), result in zip(
                    active, results,
                ):
                    if int(result or 0) == 1:
                        continue
                    logger.debug(f"[redis-lock] heartbeat: lost lock on {key}")
                    with self._held_lock:
                        held_entry = self._held.get(key)
                        if held_entry is not None and held_entry[0] == token:
                            del self._held[key]
        finally:
            # A natural exit has a narrow teardown window: the Thread is still
            # alive, so a concurrent acquire could add a new lock, observe this
            # generation as alive, and skip starting its replacement. Publish
            # termination under the same lock used by acquire; if this was not
            # an intentional stop, hand any newly-held locks to a new generation.
            with self._held_lock:
                if self._hb_thread is current:
                    self._hb_thread = None
                    if self._held and not stop_event.is_set():
                        self._start_heartbeat_locked()

    # ------------------------------------------------------------------ cleanup

    def _on_exit(self) -> None:
        """Best-effort release of all held locks on interpreter shutdown."""
        if os.getpid() != self._owner_pid:
            # Fork children inherit atexit handlers. The parent's token remains
            # live and must not be compare-deleted by child teardown.
            return
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
