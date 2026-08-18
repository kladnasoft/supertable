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
from typing import Any, Dict, Optional, Tuple

import pytest
import redis

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
        if "redis-lock extend-many-if-tokens" in self._body:
            results = []
            for index, key in enumerate(keys):
                token = args[index * 2]
                ttl_ms = int(args[index * 2 + 1])
                try:
                    cur = self._fake.get(key)
                except redis.RedisError:
                    if "redis.pcall('GET'" not in self._body:
                        raise
                    results.append(0)
                    continue
                if cur is None or cur != token:
                    results.append(0)
                    continue
                self._fake.pexpire(key, ttl_ms)
                results.append(1)
            return results
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
        self._store: Dict[str, Tuple[Any, Optional[float]]] = {}
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
            if entry is None:
                return None
            if not isinstance(entry[0], str):
                raise redis.ResponseError("WRONGTYPE Operation against a key")
            return entry[0]

    def set_wrong_type(self, key: str) -> None:
        with self._lock:
            self._store[key] = ({"not": "a string"}, None)

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

    def pttl(self, key: str) -> int:
        with self._lock:
            self._purge_if_expired(key)
            entry = self._store.get(key)
            if entry is None:
                return -2
            if entry[1] is None:
                return -1
            return max(0, int((entry[1] - time.time()) * 1000))

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
        assert isinstance(rl._extend_many_if_tokens, _RegisteredScript)

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

    def test_inherited_child_cleanup_cannot_release_parent_token(
        self, locker, fake_redis, monkeypatch,
    ):
        token = locker.acquire("parent-owned", ttl_s=5, timeout_s=2)
        assert token is not None

        monkeypatch.setattr(
            "supertable.locking.redis_lock.os.getpid",
            lambda: locker._owner_pid + 1,
        )
        locker._on_exit()

        assert fake_redis.get("parent-owned") == token

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

    def test_heartbeat_blocks_contender_beyond_original_lease(self, fake_redis):
        """Long stage/prefix deletion remains exclusive after its first TTL."""
        owner = RedisLocking(fake_redis)
        contender = RedisLocking(fake_redis)
        try:
            token = owner.acquire("stage-delete", ttl_s=2, timeout_s=1)
            assert token is not None
            # Heartbeat runs at t=1s and t=2s; at t>original TTL the key must
            # still be owned, so a concurrent stage save cannot enter.
            time.sleep(2.2)
            assert contender.acquire(
                "stage-delete", ttl_s=2, timeout_s=1, retry_interval=0.01,
            ) is None
            assert fake_redis.get("stage-delete") == token
        finally:
            owner._on_exit()
            contender._on_exit()


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

    @pytest.mark.parametrize("invalid_ttl", [0, -1, True, False, 1.5, "5000", None])
    def test_invalid_ttl_is_rejected_without_mutating_live_lease(
        self, locker, fake_redis, invalid_ttl,
    ):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert token is not None
        locker._stop_heartbeat()
        before_ttl = fake_redis.pttl("k")

        with pytest.raises(ValueError, match="positive integer"):
            locker.extend("k", token, ttl_ms=invalid_ttl)

        assert fake_redis.get("k") == token
        assert fake_redis.pttl("k") <= before_ttl
        assert fake_redis.pttl("k") > before_ttl - 250
        assert locker._held["k"] == (token, 5_000)

    def test_extend_transport_error_is_not_reported_as_definitive_loss(
            self, locker, monkeypatch,
    ):
        monkeypatch.setattr(
            locker,
            "_extend_if_token",
            lambda **kwargs: (_ for _ in ()).throw(redis.TimeoutError("reply lost")),
        )
        with pytest.raises(redis.TimeoutError, match="reply lost"):
            locker.extend("k", "tok", ttl_ms=5_000)


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

    def test_shorter_acquire_interrupts_existing_long_ttl_sleep(
        self, locker, fake_redis,
    ):
        long_token = locker.acquire("long", ttl_s=4, timeout_s=2)
        first_generation = locker._hb_thread
        short_token = locker.acquire("short", ttl_s=1, timeout_s=2)

        assert long_token is not None and short_token is not None
        assert locker._hb_thread is not first_generation
        assert locker._hb_thread is not None and locker._hb_thread.is_alive()
        # The original generation sleeps for two seconds. Without an immediate
        # restart, this one-second lease is gone before that first wake-up.
        time.sleep(1.2)
        assert fake_redis.get("short") == short_token

    def test_shorter_extend_interrupts_existing_long_ttl_sleep(
        self, locker, fake_redis,
    ):
        token = locker.acquire("shortened", ttl_s=4, timeout_s=2)
        first_generation = locker._hb_thread

        assert token is not None
        assert locker.extend("shortened", token, ttl_ms=500)
        assert locker._hb_thread is not first_generation
        assert locker._held["shortened"] == (token, 500)
        # The renewed generation uses the shortened lease's half-TTL instead
        # of the original two-second sleep (including sub-second leases).
        time.sleep(0.7)
        assert fake_redis.get("shortened") == token

    def test_stale_heartbeat_cannot_undo_longer_explicit_extend(
        self, locker, fake_redis, monkeypatch,
    ):
        token = locker.acquire("lengthened", ttl_s=1, timeout_s=2)
        heartbeat = locker._hb_thread
        assert token is not None and heartbeat is not None

        entered_heartbeat = threading.Event()
        resume_heartbeat = threading.Event()
        original_extend = locker._extend_many_if_tokens
        blocked_once = False

        def pause_heartbeat(*, keys, args):
            nonlocal blocked_once
            if threading.current_thread() is heartbeat and not blocked_once:
                blocked_once = True
                entered_heartbeat.set()
                assert resume_heartbeat.wait(timeout=2)
            return original_extend(keys=keys, args=args)

        monkeypatch.setattr(locker, "_extend_many_if_tokens", pause_heartbeat)
        assert entered_heartbeat.wait(timeout=2)

        result = []
        explicit = threading.Thread(
            target=lambda: result.append(
                locker.extend("lengthened", token, ttl_ms=5_000)
            ),
        )
        explicit.start()
        # The explicit operation must serialize behind the in-flight heartbeat;
        # once it commits, no older snapshot can subsequently shorten the key.
        time.sleep(0.05)
        assert explicit.is_alive()
        resume_heartbeat.set()
        explicit.join(timeout=2)

        assert result == [True]
        assert locker._held["lengthened"] == (token, 5_000)
        assert fake_redis.pttl("lengthened") > 4_000

    def test_stalled_renewal_cannot_expire_disjoint_short_lease(
        self, locker, fake_redis, monkeypatch,
    ):
        """A stuck old generation must not block an unrelated key's renewal."""

        long_token = locker.acquire("long", ttl_s=2, timeout_s=2)
        first_generation = locker._hb_thread
        assert long_token is not None and first_generation is not None

        entered_long_renewal = threading.Event()
        resume_long_renewal = threading.Event()
        original_extend = locker._extend_many_if_tokens

        def stall_long_renewal(*, keys, args):
            if (
                threading.current_thread() is first_generation
                and keys == ["long"]
            ):
                entered_long_renewal.set()
                assert resume_long_renewal.wait(timeout=5)
            return original_extend(keys=keys, args=args)

        monkeypatch.setattr(locker, "_extend_many_if_tokens", stall_long_renewal)
        contender = RedisLocking(fake_redis)
        try:
            assert entered_long_renewal.wait(timeout=3)
            started = time.monotonic()
            short_token = locker.acquire("short", ttl_s=1, timeout_s=2)

            assert short_token is not None
            # Restarting renewal must not spend the complete short TTL waiting
            # for the unrelated generation's bounded join.
            assert time.monotonic() - started < 0.75
            assert locker._hb_thread is not first_generation

            time.sleep(1.2)
            assert fake_redis.get("short") == short_token
            assert contender.acquire(
                "short", ttl_s=1, timeout_s=1, retry_interval=0.01,
            ) is None
            assert locker._held["short"] == (short_token, 1_000)
        finally:
            resume_long_renewal.set()
            contender._on_exit()

        deadline = time.monotonic() + 2
        while locker._lease_op_locks and time.monotonic() < deadline:
            time.sleep(0.01)
        assert locker._lease_op_locks == {}

    def test_heartbeat_renews_equal_ttl_keys_in_one_server_batch(
        self, locker, fake_redis, monkeypatch,
    ):
        """No per-key heartbeat RPC can strand a same-cadence sibling."""

        monkeypatch.setattr(
            locker,
            "_extend_if_token",
            lambda **_kwargs: (_ for _ in ()).throw(
                redis.TimeoutError("a per-key renewal must not be used")
            ),
        )
        first = locker.acquire("equal-a", ttl_s=1, timeout_s=2)
        second = locker.acquire("equal-b", ttl_s=1, timeout_s=2)

        assert first is not None and second is not None
        time.sleep(1.2)
        assert fake_redis.get("equal-a") == first
        assert fake_redis.get("equal-b") == second

        # Each batch entry has a distinct token-scoped operation lock.  The
        # heartbeat must release the reference with that entry's token; using
        # the final loop token for every key leaks one registry entry per
        # multi-key renewal forever.
        deadline = time.monotonic() + 1
        while locker._lease_op_locks and time.monotonic() < deadline:
            time.sleep(0.01)
        assert locker._lease_op_locks == {}

    def test_wrong_type_lease_does_not_abort_healthy_sibling_renewal(
        self, locker, fake_redis,
    ):
        poisoned = locker.acquire("poisoned", ttl_s=1, timeout_s=2)
        healthy = locker.acquire("healthy", ttl_s=1, timeout_s=2)
        assert poisoned is not None and healthy is not None

        # Model a namespace collision/corruption after acquisition. The next
        # batch must mark only this exact lease lost and continue to PEXPIRE
        # the healthy sibling later in the same server-side loop.
        fake_redis.set_wrong_type("poisoned")
        time.sleep(1.2)

        assert fake_redis.get("healthy") == healthy
        assert "poisoned" not in locker._held
        assert locker._held["healthy"] == (healthy, 1_000)

    def test_stale_token_renewal_cannot_block_same_key_reincarnation(
        self, locker, fake_redis, monkeypatch,
    ):
        old_token = locker.acquire("reused", ttl_s=1, timeout_s=2)
        old_generation = locker._hb_thread
        assert old_token is not None and old_generation is not None

        entered_old_renewal = threading.Event()
        resume_old_renewal = threading.Event()
        original_batch = locker._extend_many_if_tokens

        def stall_old_token(*, keys, args):
            if threading.current_thread() is old_generation:
                entered_old_renewal.set()
                assert resume_old_renewal.wait(timeout=6)
            return original_batch(keys=keys, args=args)

        monkeypatch.setattr(
            locker, "_extend_many_if_tokens", stall_old_token,
        )
        contender = RedisLocking(fake_redis)
        new_token = None
        try:
            assert entered_old_renewal.wait(timeout=2)
            # The old server-side lease expires while its reply/RPC is stuck.
            time.sleep(0.6)
            started = time.monotonic()
            new_token = locker.acquire("reused", ttl_s=1, timeout_s=2)

            assert new_token is not None and new_token != old_token
            assert time.monotonic() - started < 0.75
            assert locker._hb_thread is not old_generation
            time.sleep(1.2)
            assert fake_redis.get("reused") == new_token
            assert contender.acquire(
                "reused", ttl_s=1, timeout_s=1, retry_interval=0.01,
            ) is None
            assert locker._held["reused"] == (new_token, 1_000)
        finally:
            resume_old_renewal.set()
            contender._on_exit()

        old_generation.join(timeout=2)
        assert not old_generation.is_alive()
        assert new_token is not None
        assert locker._held["reused"][0] == new_token

    def test_heartbeat_stops_after_last_release(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker._hb_thread is not None
        locker.release("k", token)
        # _stop_heartbeat is invoked synchronously inside release()
        # when there are no remaining held locks.
        assert locker._hb_thread is None

    def test_release_stop_race_restarts_for_newly_acquired_key(
        self, locker, monkeypatch,
    ):
        first = locker.acquire("first", ttl_s=5, timeout_s=2)
        entered_stop = threading.Event()
        resume_stop = threading.Event()
        original_stop = locker._stop_heartbeat

        def paused_stop(*, restart_if_held=False):
            entered_stop.set()
            assert resume_stop.wait(timeout=2)
            return original_stop(restart_if_held=restart_if_held)

        monkeypatch.setattr(locker, "_stop_heartbeat", paused_stop)
        release_thread = threading.Thread(
            target=lambda: locker.release("first", first), daemon=True,
        )
        release_thread.start()
        assert entered_stop.wait(timeout=2)

        second = locker.acquire("second", ttl_s=5, timeout_s=2)
        assert second is not None
        resume_stop.set()
        release_thread.join(timeout=2)

        assert not release_thread.is_alive()
        assert locker._hb_thread is not None
        assert locker._hb_thread.is_alive()
        assert locker._held["second"][0] == second

    def test_natural_heartbeat_exit_cannot_orphan_concurrent_acquire(
        self, locker, fake_redis, monkeypatch,
    ):
        """A dying-but-still-alive generation must not suppress its successor."""
        loop_returned = threading.Event()
        allow_thread_exit = threading.Event()
        original_loop = locker._hb_loop

        def pause_after_loop(stop_event):
            original_loop(stop_event)
            loop_returned.set()
            assert allow_thread_exit.wait(timeout=3)

        monkeypatch.setattr(locker, "_hb_loop", pause_after_loop)
        first = locker.acquire("first", ttl_s=1, timeout_s=2)
        first_thread = locker._hb_thread
        assert first is not None and first_thread is not None

        # Force extend() to report the first lease lost. The heartbeat removes
        # it, reaches a natural exit, then our wrapper keeps the Thread itself
        # alive to expose the teardown interleaving deterministically.
        fake_redis.delete("first")
        assert loop_returned.wait(timeout=3)

        second = locker.acquire("second", ttl_s=5, timeout_s=2)
        second_thread = locker._hb_thread
        assert second is not None
        assert second_thread is not None
        assert second_thread is not first_thread
        assert second_thread.is_alive()

        allow_thread_exit.set()
        first_thread.join(timeout=2)
        assert not first_thread.is_alive()
        assert locker._hb_thread is second_thread
        assert locker._held["second"][0] == second

    def test_transient_extend_error_keeps_tracking_and_retries(
            self, locker, monkeypatch,
    ):
        token = locker.acquire("slow-drain", ttl_s=2, timeout_s=2)
        original = locker._extend_many_if_tokens
        failed_once = threading.Event()
        succeeded_after = threading.Event()
        attempts = 0

        def flaky(*, keys, args):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                failed_once.set()
                raise redis.TimeoutError("transient")
            result = original(keys=keys, args=args)
            succeeded_after.set()
            return result

        monkeypatch.setattr(locker, "_extend_many_if_tokens", flaky)
        assert failed_once.wait(timeout=3)
        assert locker._held["slow-drain"][0] == token
        assert succeeded_after.wait(timeout=2)
        assert locker._held["slow-drain"][0] == token


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

    def test_extend_propagates_ambiguous_redis_errors(self, fake_redis, monkeypatch):
        import redis as _redis

        rl = RedisLocking(fake_redis)
        try:
            rl.acquire("k", ttl_s=5, timeout_s=2)

            def boom(*_a, **_kw):
                raise _redis.RedisError("boom")

            monkeypatch.setattr(rl, "_extend_if_token", boom)
            with pytest.raises(_redis.RedisError, match="boom"):
                rl.extend("k", "irrelevant", ttl_ms=5_000)
        finally:
            rl._on_exit()
