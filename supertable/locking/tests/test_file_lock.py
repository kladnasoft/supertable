# route: supertable.locking.tests.test_file_lock
"""
Unit tests for :class:`supertable.locking.file_lock.FileLocking`.

Covers the public API contract — acquire / release / extend / who — plus the
expiry / heartbeat / multi-instance behaviour. The fcntl-based implementation
runs against a real temporary directory so we exercise the actual file I/O
path rather than mocking it.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from supertable.locking.file_lock import FileLocking


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def lock_dir(tmp_path):
    """A clean, isolated working directory for each test."""
    return str(tmp_path)


@pytest.fixture()
def locker(lock_dir):
    """A FileLocking instance with a tight retry interval for fast tests."""
    fl = FileLocking(working_dir=lock_dir, retry_interval=0.01)
    yield fl
    # Best-effort cleanup of held locks/heartbeat thread.
    try:
        fl._on_exit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:

    def test_requires_working_dir(self):
        with pytest.raises(ValueError, match="working_dir is required"):
            FileLocking(working_dir="")

    def test_creates_working_dir(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        FileLocking(working_dir=str(nested))
        assert nested.is_dir()

    def test_lock_path_uses_custom_filename(self, tmp_path):
        fl = FileLocking(working_dir=str(tmp_path), lock_file_name="custom.json")
        assert fl.lock_path.endswith("custom.json")

    def test_retry_interval_floor(self, tmp_path):
        # Anything below 0.01 should be clamped.
        fl = FileLocking(working_dir=str(tmp_path), retry_interval=0.0)
        assert fl.retry_interval >= 0.01


# ---------------------------------------------------------------------------
# acquire / release
# ---------------------------------------------------------------------------

class TestAcquireRelease:

    def test_acquire_returns_token(self, locker):
        token = locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert isinstance(token, str)
        assert len(token) > 0

    def test_acquire_persists_to_file(self, locker, lock_dir):
        locker.acquire("key1", ttl_s=5, timeout_s=2)
        with open(os.path.join(lock_dir, ".lock.json")) as f:
            records = json.loads(f.read())
        assert any(r["res"] == "key1" for r in records)

    def test_release_with_correct_token(self, locker):
        token = locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert locker.release("key1", token) is True

    def test_release_with_wrong_token_fails(self, locker):
        locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert locker.release("key1", "not-the-token") is False

    def test_release_unknown_key_returns_false(self, locker):
        assert locker.release("nonexistent", "tok") is False

    def test_release_clears_record_from_file(self, locker, lock_dir):
        token = locker.acquire("key1", ttl_s=5, timeout_s=2)
        locker.release("key1", token)
        with open(os.path.join(lock_dir, ".lock.json")) as f:
            records = json.loads(f.read())
        assert all(r["res"] != "key1" for r in records)

    def test_acquire_again_after_release(self, locker):
        t1 = locker.acquire("key1", ttl_s=5, timeout_s=2)
        locker.release("key1", t1)
        t2 = locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert t2 is not None
        assert t2 != t1

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork required")
    def test_fork_child_cleanup_cannot_release_live_parent_lock(
        self, lock_dir,
    ):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        token = owner.acquire("parent-owned", ttl_s=5, timeout_s=1)
        assert token is not None

        pid = os.fork()
        if pid == 0:  # pragma: no cover - assertions execute in parent
            owner._on_exit()
            os._exit(0)
        try:
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0
            assert owner.who("parent-owned") == token
            assert contender.acquire(
                "parent-owned", ttl_s=2, timeout_s=1, retry_interval=0.01,
            ) is None
        finally:
            owner._on_exit()
            contender._on_exit()


# ---------------------------------------------------------------------------
# Conflict / contention
# ---------------------------------------------------------------------------

class TestContention:

    def test_second_acquire_blocks_until_timeout(self, locker, lock_dir):
        # First instance grabs the key.
        t1 = locker.acquire("contended", ttl_s=30, timeout_s=2)
        assert t1 is not None

        # A second locker (separate instance, same file) cannot acquire.
        other = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        try:
            t2 = other.acquire("contended", ttl_s=30, timeout_s=1)
            assert t2 is None
        finally:
            other._on_exit()

    def test_second_acquire_succeeds_after_release(self, locker, lock_dir):
        t1 = locker.acquire("contended", ttl_s=30, timeout_s=2)
        other = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        try:
            assert other.acquire("contended", ttl_s=5, timeout_s=1) is None
            locker.release("contended", t1)
            t2 = other.acquire("contended", ttl_s=5, timeout_s=2)
            assert t2 is not None
        finally:
            other._on_exit()

    def test_disjoint_keys_do_not_block_each_other(self, locker):
        t1 = locker.acquire("A", ttl_s=5, timeout_s=2)
        t2 = locker.acquire("B", ttl_s=5, timeout_s=2)
        assert t1 is not None
        assert t2 is not None
        assert t1 != t2

    def test_mixed_ttls_renew_at_half_the_shortest_lease(self, lock_dir):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        try:
            long = owner.acquire("long", ttl_s=8, timeout_s=1)
            # The long lease has already put the shared heartbeat to sleep when
            # the shorter lease is introduced.
            short = owner.acquire("short", ttl_s=2, timeout_s=1)
            assert short is not None and long is not None

            # The historical process-global TTL refreshed both leases for two
            # seconds and then slept four, letting ``short`` expire here.
            time.sleep(3.2)
            assert contender.acquire(
                "short", ttl_s=5, timeout_s=1, retry_interval=0.01,
            ) is None
            assert owner.who("short") == short
        finally:
            owner._on_exit()
            contender._on_exit()

    def test_release_stop_race_restarts_for_concurrent_acquire(
        self, lock_dir, monkeypatch,
    ):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        first = owner.acquire("first", ttl_s=2, timeout_s=1)
        entered_stop = threading.Event()
        resume_stop = threading.Event()
        original_stop = owner._stop_heartbeat

        def paused_stop(*, restart_if_held=False):
            entered_stop.set()
            assert resume_stop.wait(2)
            return original_stop(restart_if_held=restart_if_held)

        monkeypatch.setattr(owner, "_stop_heartbeat", paused_stop)
        released = []
        thread = threading.Thread(
            target=lambda: released.append(owner.release("first", first)),
        )
        try:
            thread.start()
            assert entered_stop.wait(2)
            second = owner.acquire("second", ttl_s=2, timeout_s=1)
            assert second is not None
            resume_stop.set()
            thread.join(3)

            assert released == [True]
            assert owner._held["second"][0] == second
            assert owner._hb_thread is not None
            assert owner._hb_thread.is_alive()
            time.sleep(2.2)
            assert contender.acquire(
                "second", ttl_s=5, timeout_s=1, retry_interval=0.01,
            ) is None
        finally:
            resume_stop.set()
            thread.join(3)
            monkeypatch.setattr(owner, "_stop_heartbeat", original_stop)
            owner._on_exit()
            contender._on_exit()

    def test_stalled_old_heartbeat_cannot_delay_replacement_generation(
        self, lock_dir, monkeypatch,
    ):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        first = owner.acquire("first", ttl_s=1, timeout_s=1)
        old_generation = owner._hb_thread
        assert first is not None and old_generation is not None

        entered_stop = threading.Event()
        resume_stop = threading.Event()
        original_stop = owner._stop_heartbeat

        def pause_stop(*, restart_if_held=False):
            entered_stop.set()
            assert resume_stop.wait(timeout=2)
            return original_stop(restart_if_held=restart_if_held)

        # Model an old generation whose teardown/join is slow even though its
        # shared-file operation is not blocking the replacement generation.
        # The replacement must be published before that join is attempted.
        join_called = threading.Event()
        original_join = old_generation.join

        def delayed_join(timeout=None):
            join_called.set()
            time.sleep(1.2)
            return original_join(timeout=0)

        monkeypatch.setattr(owner, "_stop_heartbeat", pause_stop)
        monkeypatch.setattr(old_generation, "join", delayed_join)
        release_result = []
        release_thread = threading.Thread(
            target=lambda: release_result.append(owner.release("first", first)),
        )
        try:
            release_thread.start()
            assert entered_stop.wait(timeout=2)
            second = owner.acquire("second", ttl_s=1, timeout_s=1)
            assert second is not None
            resume_stop.set()
            release_thread.join(timeout=1)

            assert owner._hb_thread is not old_generation
            assert not join_called.is_set()

            time.sleep(1.2)
            assert contender.acquire(
                "second", ttl_s=2, timeout_s=1, retry_interval=0.01,
            ) is None
            assert owner.who("second") == second
        finally:
            resume_stop.set()
            release_thread.join(timeout=3)
            owner._on_exit()
            contender._on_exit()

        assert release_result == [True]

    def test_stalled_stale_token_renewal_cannot_expire_new_short_lease(
        self, lock_dir, monkeypatch,
    ):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        first = owner.acquire("old", ttl_s=2, timeout_s=1)
        old_generation = owner._hb_thread
        assert first is not None and old_generation is not None

        entered_old_renewal = threading.Event()
        resume_old_renewal = threading.Event()
        original_read_write = owner._atomic_read_write
        blocked_once = False

        def stall_old_generation(callback):
            nonlocal blocked_once
            if threading.current_thread() is old_generation and not blocked_once:
                blocked_once = True
                entered_old_renewal.set()
                assert resume_old_renewal.wait(timeout=5)
            return original_read_write(callback)

        monkeypatch.setattr(owner, "_atomic_read_write", stall_old_generation)
        try:
            assert entered_old_renewal.wait(timeout=3)
            short = owner.acquire("new-short", ttl_s=1, timeout_s=1)
            assert short is not None
            assert owner._hb_thread is not old_generation

            # The obsolete generation is still alive and blocked, but its
            # exact-token mutation boundary must not strand this new lease.
            time.sleep(1.2)
            assert contender.acquire(
                "new-short", ttl_s=2, timeout_s=1, retry_interval=0.01,
            ) is None
            assert owner.who("new-short") == short
        finally:
            resume_old_renewal.set()
            owner._on_exit()
            contender._on_exit()

        old_generation.join(timeout=2)
        assert not old_generation.is_alive()
        deadline = time.monotonic() + 2
        while owner._lease_op_locks and time.monotonic() < deadline:
            time.sleep(0.01)
        assert owner._lease_op_locks == {}


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------

class TestExpiry:

    def test_expired_lock_is_purged_on_acquire(self, locker, lock_dir):
        # Manually write an already-expired record into the file.
        old = {"res": "stale", "exp": int(time.time()) - 60, "tok": "old-tok"}
        with open(os.path.join(lock_dir, ".lock.json"), "w") as f:
            f.write(json.dumps([old]))

        # A fresh acquire should succeed and purge the stale record.
        token = locker.acquire("stale", ttl_s=5, timeout_s=2)
        assert token is not None
        assert token != "old-tok"

    def test_who_returns_none_for_expired(self, locker, lock_dir):
        old = {"res": "x", "exp": int(time.time()) - 1, "tok": "t"}
        with open(os.path.join(lock_dir, ".lock.json"), "w") as f:
            f.write(json.dumps([old]))
        assert locker.who("x") is None

    def test_one_second_lease_is_renewed_before_exact_expiry(self, lock_dir):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        try:
            token = owner.acquire("short", ttl_s=1, timeout_s=1)
            assert token is not None
            time.sleep(1.2)
            assert contender.acquire(
                "short", ttl_s=5, timeout_s=1, retry_interval=0.01,
            ) is None
            assert owner.who("short") == token
        finally:
            owner._on_exit()
            contender._on_exit()

    @pytest.mark.parametrize("damaged", ["", "{not-json", "{}"])
    def test_corrupt_lock_document_fails_closed(self, lock_dir, damaged):
        owner = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        contender = FileLocking(working_dir=lock_dir, retry_interval=0.01)
        try:
            token = owner.acquire("held", ttl_s=30, timeout_s=1)
            assert token is not None
            owner._stop_heartbeat()
            with open(owner.lock_path, "w") as handle:
                handle.write(damaged)

            assert contender.acquire(
                "held", ttl_s=5, timeout_s=1, retry_interval=0.01,
            ) is None
            with pytest.raises(RuntimeError, match="file-lock state"):
                contender.who("held")
        finally:
            owner._stop_heartbeat()
            contender._stop_heartbeat()


# ---------------------------------------------------------------------------
# extend
# ---------------------------------------------------------------------------

class TestExtend:

    def test_extend_with_correct_token(self, locker):
        token = locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert locker.extend("key1", token, ttl_ms=10_000) is True

    def test_extend_with_wrong_token_fails(self, locker):
        locker.acquire("key1", ttl_s=5, timeout_s=2)
        assert locker.extend("key1", "wrong-tok", ttl_ms=10_000) is False

    def test_extend_unknown_key_returns_false(self, locker):
        assert locker.extend("missing", "tok", ttl_ms=5_000) is False

    @pytest.mark.parametrize("invalid_ttl", [0, -1, True, False, 1.5, "5000", None])
    def test_invalid_ttl_is_rejected_without_mutating_live_lease(
        self, locker, lock_dir, invalid_ttl,
    ):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert token is not None
        locker._stop_heartbeat()
        before = _read_record(lock_dir, "k")

        with pytest.raises(ValueError, match="positive integer"):
            locker.extend("k", token, ttl_ms=invalid_ttl)

        assert _read_record(lock_dir, "k") == before
        assert locker._held["k"] == (token, 5.0)

    def test_extend_pushes_expiry_into_future(self, locker, lock_dir):
        token = locker.acquire("k", ttl_s=2, timeout_s=2)
        # Read current expiry then extend.
        before = _read_record(lock_dir, "k")["exp"]
        ok = locker.extend("k", token, ttl_ms=60_000)
        after = _read_record(lock_dir, "k")["exp"]
        assert ok is True
        assert after >= before

    def test_stale_heartbeat_cannot_undo_longer_explicit_extend(
        self, locker, lock_dir, monkeypatch,
    ):
        token = locker.acquire("lengthened", ttl_s=1, timeout_s=2)
        heartbeat = locker._hb_thread
        assert token is not None and heartbeat is not None

        entered_heartbeat = threading.Event()
        resume_heartbeat = threading.Event()
        original_read_write = locker._atomic_read_write
        blocked_once = False

        def pause_heartbeat(callback):
            nonlocal blocked_once
            if threading.current_thread() is heartbeat and not blocked_once:
                blocked_once = True
                entered_heartbeat.set()
                assert resume_heartbeat.wait(timeout=2)
            return original_read_write(callback)

        monkeypatch.setattr(locker, "_atomic_read_write", pause_heartbeat)
        assert entered_heartbeat.wait(timeout=2)

        result = []
        explicit = threading.Thread(
            target=lambda: result.append(
                locker.extend("lengthened", token, ttl_ms=5_000)
            ),
        )
        explicit.start()
        time.sleep(0.05)
        assert explicit.is_alive()
        resume_heartbeat.set()
        explicit.join(timeout=2)

        assert result == [True]
        assert locker._held["lengthened"] == (token, 5.0)
        assert _read_record(lock_dir, "lengthened")["exp"] - time.time() > 4.0


# ---------------------------------------------------------------------------
# who
# ---------------------------------------------------------------------------

class TestWho:

    def test_who_returns_token_when_held(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        assert locker.who("k") == token

    def test_who_returns_none_when_unheld(self, locker):
        assert locker.who("nonexistent") is None

    def test_who_after_release_returns_none(self, locker):
        token = locker.acquire("k", ttl_s=5, timeout_s=2)
        locker.release("k", token)
        assert locker.who("k") is None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_record(lock_dir: str, key: str) -> dict:
    with open(os.path.join(lock_dir, ".lock.json")) as f:
        records = json.loads(f.read())
    for r in records:
        if r["res"] == key:
            return r
    raise AssertionError(f"no record for key={key!r}")
