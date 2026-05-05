# route: supertable.locking.tests.test_redis_lock
"""
Unit tests for :class:`supertable.locking.redis_lock.RedisLocking`.

Uses a lightweight in-memory fake of the subset of ``redis.Redis`` that
RedisLocking actually calls (``set`` with ``nx``/``ex``, ``register_script``,
plus invocation of the registered Lua scripts via the keys/args contract).
This keeps the tests hermetic — no real Redis required — while still
exercising the genuine acquire / release / extend / heartbeat code paths.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional, Tuple

import pytest

from supertable.locking.redis_lock import RedisLocking


# ---------------------------------------------------------------------------
# In-memory Redis fake
# ---------------------------------------------------------------------------

class _RegisteredScript:
    """Mimics ``redis.client.Script``: callable with ``keys=`` and ``args=``."""

    def __init__(self, fake, body: str):
        self._fake = fake
        self._body = body

    def __call__(self, keys=(), args=()):
        # Both Lua scripts in redis_lock.py follow the same shape:
        #   1) compare GET(KEYS[1]) to ARGV[1] (token)
        #   2a) for RELEASE: DEL on match
        #   2b) for EXTEND : PEXPIRE ARGV[2] on match
        key = keys[0]
        token = args[0]
        cur = self._fake.get(key)
        if cur is None or cur != token:
            return 0
        if "DEL" in self._body:
            self._fake.delete(key)
            return 1
        if "PEXPIRE" in self._body:
            ttl_ms = int(args[1])
            self._fake.pexpire(key, ttl_ms)
            return 1
        return 0


class FakeRedis:
    """Tiny in-memory Redis stand-in for the methods RedisLocking touches."""

    def __init__(self):
        self._store: Dict[str, Tuple[str, Optional[float]]] = {}
        self._lock = threading.Lock()

    # ---- expiry helpers ----

    def _expired(self, expires_at: Optional[float]) -> bool:
        return expires_at is not None and time.time() >= expires_at

    def _purge_if_expired(self, key: str) -> None:
        entry = self._store.get(key)
        if entry is not None and self._expired(entry[1]):
            del self._store[key]

    # ---- public-ish API used by RedisLocking ----

    def set(self, key: str, value: str, nx: bool = False, ex: Optional[int] = None) -> Optional[bool]:
        with self._lock:
            self._purge_if_expired(key)
            if nx and key in self._store:
                return None
            expires_at = time.time() + ex if ex else None
            self._store[key] = (value, expires_at)
            return True

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            self._purge_if_expired(key)
            entry = self._store.get(key)
            return entry[0] if entry is not None else None

    def delete(self, key: str) -> int:
        with self._lock:
            return 1 if self._store.pop(key, None) is not None else 0

    def pexpire(self, key: str, ttl_ms: int) -> int:
        with self._lock:
            self._purge_if_expired(key)
            if key not in self._store:
                return 0
            value, _ = self._store[key]
            self._store[key] = (value, time.time() + ttl_ms / 1000.0)
            return 1

    def register_script(self, body: str) -> _RegisteredScript:
        return _RegisteredScript(self, body)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_redis():
    return FakeRedis()


@pytest.fixture()
def locker(fake_redis):
    rl = RedisLocking(fake_redis)
    yield rl
    # Ensure heartbeat thread is stopped between tests.
    try:
        rl._on_exit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:

    def test_registers_both_scripts(self, fake_redis):
        rl = RedisLocking(fake_redis)
        assert isinstance(rl._release_if_token, _RegisteredScript)
        assert isinstance(rl._extend_if_token, _RegisteredScript)

    def test_initial_state_no_holds(self, locker):
        assert locker._held == {}
        assert locker._hb_thread is None


# ---------------------------------------------------------------------------
# acquire / release
# ---------------------------------------------------------------------------

class TestAcquireRelease:

    def test_acquire_returns_token(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert isinstance(token, str)
        assert len(token) > 0

    def test_acquire_writes_to_redis(self, locker, fake_redis):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert fake_redis.get("k") == token

    def test_acquire_tracks_held_lock(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert "k" in locker._held
        assert locker._held["k"][0] == token

    def test_release_with_correct_token(self, locker, fake_redis):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker.release("k", token) is True
        assert fake_redis.get("k") is None

    def test_release_with_wrong_token_returns_false(self, locker, fake_redis):
        locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker.release("k", "bogus") is False
        # Genuine holder still owns the key.
        assert fake_redis.get("k") is not None

    def test_release_clears_local_tracking_on_match(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        locker.release("k", token)
        assert "k" not in locker._held

    def test_acquire_after_release(self, locker):
        t1 = locker.acquire("k", ttl_s=5, timeout_s=2)
        locker.release("k", t1)
        t2 = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert t2 is not None
        assert t2 != t1


# ---------------------------------------------------------------------------
# Contention
# ---------------------------------------------------------------------------

class TestContention:

    def test_second_locker_blocked_until_timeout(self, fake_redis):
        a = RedisLocking(fake_redis)
        b = RedisLocking(fake_redis)
        try:
            t1 = a.acquire("c", ttl_s=30, timeout_s=2)
            assert t1 is not None

            start = time.time()
            t2 = b.acquire("c", ttl_s=30, timeout_s=1, retry_interval=0.01)
            elapsed = time.time() - start
            assert t2 is None
            assert elapsed >= 1.0
        finally:
            a._on_exit()
            b._on_exit()

    def test_second_locker_succeeds_after_release(self, fake_redis):
        a = RedisLocking(fake_redis)
        b = RedisLocking(fake_redis)
        try:
            t1 = a.acquire("c", ttl_s=30, timeout_s=2)
            assert b.acquire("c", ttl_s=5, timeout_s=1, retry_interval=0.01) is None
            a.release("c", t1)
            t2 = b.acquire("c", ttl_s=5, timeout_s=2, retry_interval=0.01)
            assert t2 is not None
        finally:
            a._on_exit()
            b._on_exit()

    def test_disjoint_keys_dont_block(self, locker):
        ta = locker.acquire("A", ttl_s=5, timeout_s=2)
        tb = locker.acquire("B", ttl_s=5, timeout_s=2)
        assert ta is not None and tb is not None
        assert ta != tb


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------

class TestExpiry:

    def test_expired_lock_can_be_reacquired(self, fake_redis):
        a = RedisLocking(fake_redis)
        b = RedisLocking(fake_redis)
        try:
            # Disable heartbeat so we can observe genuine expiry.
            a.acquire("k", ttl_s=1, timeout_s=2)
            a._stop_heartbeat()
            time.sleep(1.1)
            t2 = b.acquire("k", ttl_s=5, timeout_s=2, retry_interval=0.01)
            assert t2 is not None
        finally:
            a._on_exit()
            b._on_exit()


# ---------------------------------------------------------------------------
# extend
# ---------------------------------------------------------------------------

class TestExtend:

    def test_extend_with_correct_token(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker.extend("k", token, ttl_ms=10_000) is True

    def test_extend_with_wrong_token(self, locker):
        locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker.extend("k", "bogus", ttl_ms=10_000) is False

    def test_extend_missing_key(self, locker):
        assert locker.extend("missing", "tok", ttl_ms=5_000) is False


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeat:

    def test_heartbeat_thread_starts_on_first_acquire(self, locker):
        assert locker._hb_thread is None
        locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker._hb_thread is not None
        assert locker._hb_thread.is_alive()

    def test_heartbeat_thread_reused_for_second_acquire(self, locker):
        locker.acquire("a", ttl_s=5, timeout_s=2)
        first = locker._hb_thread
        locker.acquire("b", ttl_s=5, timeout_s=2)
        assert locker._hb_thread is first

    def test_heartbeat_stops_after_last_release(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker._hb_thread is not None
        locker.release("k", token)
        # _stop_heartbeat is invoked synchronously inside release()
        # when there are no remaining held locks.
        assert locker._hb_thread is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def test_release_swallows_redis_errors(self, fake_redis, monkeypatch):
        import redis as _redis

        rl = RedisLocking(fake_redis)
        try:
            rl.acquire("k", ttl_s=5, timeout_s=2)

            def boom(*_a, **_kw):
                raise _redis.RedisError("boom")

            monkeypatch.setattr(rl, "_release_if_token", boom)
            # Should return False rather than propagate.
            assert rl.release("k", "irrelevant") is False
        finally:
            rl._on_exit()

    def test_extend_swallows_redis_errors(self, fake_redis, monkeypatch):
        import redis as _redis

        rl = RedisLocking(fake_redis)
        try:
            rl.acquire("k", ttl_s=5, timeout_s=2)

            def boom(*_a, **_kw):
                raise _redis.RedisError("boom")

            monkeypatch.setattr(rl, "_extend_if_token", boom)
            assert rl.extend("k", "irrelevant", ttl_ms=5_000) is False
        finally:
            rl._on_exit()
