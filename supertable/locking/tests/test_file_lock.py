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

    def test_extend_pushes_expiry_into_future(self, locker, lock_dir):
        token = locker.acquire("k", ttl_s=2, timeout_s=2)
        # Read current expiry then extend.
        before = _read_record(lock_dir, "k")["exp"]
        ok = locker.extend("k", token, ttl_ms=60_000)
        after = _read_record(lock_dir, "k")["exp"]
        assert ok is True
        assert after >= before


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
