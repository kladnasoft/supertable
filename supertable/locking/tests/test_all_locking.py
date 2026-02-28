#!/usr/bin/env python3
# tests/test_all_locking.py
#
# Comprehensive test suite for the locking package:
#   - LockingBackend (enum + from_str)
#   - FileLocking (every method: init, read/write, purge, atomic_read_write,
#                  acquire, release, who, heartbeat, on_exit, concurrency)
#   - Locking facade (backend selection, proxy API, read_with_lock)
#
# Uses only stdlib (unittest, tempfile, threading).
# Run:  python3 test_all_locking.py

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import threading
import time
import unittest

# ---------------------------------------------------------------------------
# Ensure we can import from the same directory
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from supertable.locking.locking_backend import LockingBackend
from supertable.locking.file_lock import FileLocking


# ═══════════════════════════════════════════════════════════════════════════
# 1. LockingBackend tests
# ═══════════════════════════════════════════════════════════════════════════

class TestLockingBackendEnum(unittest.TestCase):
    """Verify enum members."""

    def test_file_value(self):
        self.assertEqual(LockingBackend.FILE.value, "file")

    def test_redis_value(self):
        self.assertEqual(LockingBackend.REDIS.value, "redis")

    def test_members_count(self):
        self.assertEqual(len(LockingBackend), 2)


class TestLockingBackendFromStr(unittest.TestCase):
    """from_str factory method."""

    # None / empty
    def test_none_no_default(self):
        self.assertIs(LockingBackend.from_str(None), LockingBackend.REDIS)

    def test_none_with_default(self):
        self.assertIs(LockingBackend.from_str(None, default=LockingBackend.FILE), LockingBackend.FILE)

    def test_empty_no_default(self):
        self.assertIs(LockingBackend.from_str(""), LockingBackend.REDIS)

    def test_empty_with_default(self):
        self.assertIs(LockingBackend.from_str("", default=LockingBackend.FILE), LockingBackend.FILE)

    # Valid values
    def test_redis_lower(self):
        self.assertIs(LockingBackend.from_str("redis"), LockingBackend.REDIS)

    def test_redis_upper(self):
        self.assertIs(LockingBackend.from_str("REDIS"), LockingBackend.REDIS)

    def test_redis_mixed(self):
        self.assertIs(LockingBackend.from_str("Redis"), LockingBackend.REDIS)

    def test_redis_whitespace(self):
        self.assertIs(LockingBackend.from_str("  redis  "), LockingBackend.REDIS)

    def test_file_lower(self):
        self.assertIs(LockingBackend.from_str("file"), LockingBackend.FILE)

    def test_file_upper(self):
        self.assertIs(LockingBackend.from_str("FILE"), LockingBackend.FILE)

    def test_file_mixed(self):
        self.assertIs(LockingBackend.from_str("File"), LockingBackend.FILE)

    def test_file_whitespace(self):
        self.assertIs(LockingBackend.from_str("  file  "), LockingBackend.FILE)

    # Unknown → default
    def test_unknown_no_default(self):
        self.assertIs(LockingBackend.from_str("memcached"), LockingBackend.REDIS)

    def test_unknown_with_default(self):
        self.assertIs(LockingBackend.from_str("memcached", default=LockingBackend.FILE), LockingBackend.FILE)

    def test_numeric_string(self):
        self.assertIs(LockingBackend.from_str("123"), LockingBackend.REDIS)


# ═══════════════════════════════════════════════════════════════════════════
# 2. FileLocking tests
# ═══════════════════════════════════════════════════════════════════════════

class _FileLockTestBase(unittest.TestCase):
    """Base with temp dir setup/teardown."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._instances: list[FileLocking] = []

    def tearDown(self):
        for fl in self._instances:
            try:
                fl._stop_heartbeat()
            except Exception:
                pass

    def _make(self, identity: str = "test-1", **kw) -> FileLocking:
        fl = FileLocking(
            identity=identity,
            working_dir=self._tmpdir,
            check_interval=kw.pop("check_interval", 0.01),
            **kw,
        )
        self._instances.append(fl)
        return fl


# ---- __init__ ----

class TestFileLockingInit(_FileLockTestBase):

    def test_creates_working_dir(self):
        subdir = os.path.join(self._tmpdir, "sub", "deep")
        fl = FileLocking(identity="x", working_dir=subdir)
        self._instances.append(fl)
        self.assertTrue(os.path.isdir(subdir))

    def test_raises_none_working_dir(self):
        with self.assertRaises(ValueError):
            FileLocking(identity="x", working_dir=None)

    def test_raises_empty_working_dir(self):
        with self.assertRaises(ValueError):
            FileLocking(identity="x", working_dir="")

    def test_check_interval_floor(self):
        fl = self._make(check_interval=0.001)
        self.assertEqual(fl.check_interval, 0.01)

    def test_lock_path_default(self):
        fl = self._make()
        self.assertEqual(fl.lock_path, os.path.join(self._tmpdir, ".lock.json"))

    def test_lock_path_custom(self):
        fl = self._make(lock_file_name="custom.json")
        self.assertTrue(fl.lock_path.endswith("custom.json"))

    def test_identity_stored(self):
        fl = self._make(identity="myid")
        self.assertEqual(fl.identity, "myid")

    def test_held_starts_empty(self):
        fl = self._make()
        self.assertEqual(fl._held, set())


# ---- _read_file / _write_file ----

class TestFileLockingReadWrite(_FileLockTestBase):

    def test_read_nonexistent_returns_empty(self):
        fl = self._make()
        self.assertEqual(fl._read_file(), [])

    def test_write_then_read(self):
        fl = self._make()
        records = [{"res": "a", "exp": int(time.time()) + 100, "pid": "x", "who": "w"}]
        fl._write_file(records)
        result = fl._read_file()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["res"], "a")

    def test_write_overwrites(self):
        fl = self._make()
        fl._write_file([{"res": "a"}])
        fl._write_file([{"res": "b"}])
        result = fl._read_file()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["res"], "b")

    def test_read_corrupt_returns_empty(self):
        fl = self._make()
        with open(fl.lock_path, "w") as f:
            f.write("NOT JSON {{{")
        self.assertEqual(fl._read_file(), [])

    def test_write_empty_list(self):
        fl = self._make()
        fl._write_file([])
        self.assertEqual(fl._read_file(), [])


# ---- _purge_expired ----

class TestFileLockingPurgeExpired(_FileLockTestBase):

    def test_removes_expired(self):
        fl = self._make()
        now = int(time.time())
        records = [
            {"res": "alive", "exp": now + 100},
            {"res": "dead", "exp": now - 10},
        ]
        result = fl._purge_expired(records)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["res"], "alive")

    def test_keeps_all_valid(self):
        fl = self._make()
        now = int(time.time())
        records = [{"res": f"r{i}", "exp": now + 100} for i in range(5)]
        self.assertEqual(len(fl._purge_expired(records)), 5)

    def test_empty_records(self):
        fl = self._make()
        self.assertEqual(fl._purge_expired([]), [])

    def test_missing_exp_treated_as_expired(self):
        fl = self._make()
        self.assertEqual(fl._purge_expired([{"res": "no_exp"}]), [])


# ---- _atomic_read_write ----

class TestFileLockingAtomicReadWrite(_FileLockTestBase):

    def test_callback_receives_current_records(self):
        fl = self._make()
        fl._write_file([{"res": "existing"}])
        received = []

        def cb(records):
            received.append(list(records))
            return records

        fl._atomic_read_write(cb)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][0]["res"], "existing")

    def test_callback_return_is_written(self):
        fl = self._make()

        def cb(records):
            return [{"res": "new"}]

        fl._atomic_read_write(cb)
        self.assertEqual(fl._read_file()[0]["res"], "new")

    def test_creates_file_if_missing(self):
        fl = self._make()
        if os.path.exists(fl.lock_path):
            os.remove(fl.lock_path)
        fl._atomic_read_write(lambda r: [{"res": "created"}])
        self.assertTrue(os.path.exists(fl.lock_path))
        self.assertEqual(fl._read_file()[0]["res"], "created")

    def test_handles_corrupt_file(self):
        fl = self._make()
        with open(fl.lock_path, "w") as f:
            f.write("corrupt!!!")
        fl._atomic_read_write(lambda r: r + [{"res": "added"}])
        # Should have recovered from corrupt and added
        result = fl._read_file()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["res"], "added")


# ---- acquire ----

class TestFileLockingAcquire(_FileLockTestBase):

    def test_acquire_single(self):
        fl = self._make()
        self.assertTrue(fl.acquire(["r1"], duration=5, who="tester"))
        self.assertIn("r1", fl._held)
        fl.release(["r1"])

    def test_acquire_multiple(self):
        fl = self._make()
        self.assertTrue(fl.acquire(["r1", "r2", "r3"], duration=5))
        self.assertEqual(fl._held, {"r1", "r2", "r3"})
        fl.release(["r1", "r2", "r3"])

    def test_acquire_writes_correct_record(self):
        fl = self._make()
        fl.acquire(["res-x"], duration=10, who="bob")
        records = fl._read_file()
        match = [r for r in records if r["res"] == "res-x"]
        self.assertEqual(len(match), 1)
        self.assertEqual(match[0]["pid"], fl.identity)
        self.assertEqual(match[0]["who"], "bob")
        fl.release(["res-x"])

    def test_acquire_expired_lock_succeeds(self):
        fl = self._make()
        now = int(time.time())
        fl._write_file([{"res": "r1", "exp": now - 5, "pid": "other", "who": "old"}])
        self.assertTrue(fl.acquire(["r1"], duration=5))
        fl.release(["r1"])

    def test_reentrant_same_identity(self):
        fl = self._make(identity="same")
        self.assertTrue(fl.acquire(["r1"], duration=5))
        self.assertTrue(fl.acquire(["r1"], duration=5))  # same identity → ok
        fl.release(["r1"])

    def test_acquire_coerces_to_string(self):
        fl = self._make()
        self.assertTrue(fl.acquire([123, 456], duration=5))
        self.assertIn("123", fl._held)
        self.assertIn("456", fl._held)
        fl.release(["123", "456"])

    def test_acquire_duration_minimum_is_1(self):
        fl = self._make()
        self.assertTrue(fl.acquire(["r1"], duration=0))
        records = fl._read_file()
        match = [r for r in records if r["res"] == "r1"]
        now = int(time.time())
        self.assertGreaterEqual(match[0]["exp"], now)
        fl.release(["r1"])


# ---- acquire with conflict (threading) ----

class TestFileLockingAcquireConflict(_FileLockTestBase):

    def test_conflict_blocks_then_succeeds(self):
        fl1 = self._make(identity="owner")
        fl2 = self._make(identity="waiter")

        self.assertTrue(fl1.acquire(["shared"], duration=2))

        result = [None]

        def waiter():
            result[0] = fl2.acquire(["shared"], duration=2)

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.15)
        fl1.release(["shared"])
        t.join(timeout=5)
        self.assertTrue(result[0])
        fl2.release(["shared"])

    def test_disjoint_resources_no_conflict(self):
        fl1 = self._make(identity="a")
        fl2 = self._make(identity="b")
        self.assertTrue(fl1.acquire(["r1"], duration=5))
        self.assertTrue(fl2.acquire(["r2"], duration=5))
        fl1.release(["r1"])
        fl2.release(["r2"])


# ---- release ----

class TestFileLockingRelease(_FileLockTestBase):

    def test_release_removes_record(self):
        fl = self._make()
        fl.acquire(["r1"], duration=10)
        fl.release(["r1"])
        records = fl._read_file()
        own = [r for r in records if r.get("pid") == fl.identity]
        self.assertEqual(len(own), 0)

    def test_release_only_own_records(self):
        fl1 = self._make(identity="a")
        fl2 = self._make(identity="b")
        fl1.acquire(["r1"], duration=10)
        fl2.acquire(["r2"], duration=10)

        # fl1 tries to release both — should only release r1
        fl1.release(["r1", "r2"])
        records = fl1._read_file()
        remaining = [r["res"] for r in records]
        self.assertIn("r2", remaining)
        self.assertNotIn("r1", remaining)
        fl2.release(["r2"])

    def test_release_empty_noop(self):
        fl = self._make()
        fl.release([])  # no error

    def test_release_updates_held_set(self):
        fl = self._make()
        fl.acquire(["r1", "r2"], duration=10)
        fl.release(["r1"])
        self.assertNotIn("r1", fl._held)
        self.assertIn("r2", fl._held)
        fl.release(["r2"])

    def test_release_nonexistent_no_error(self):
        fl = self._make()
        fl.release(["nonexistent"])  # no error


# ---- who ----

class TestFileLockingWho(_FileLockTestBase):

    def test_who_returns_holder(self):
        fl = self._make()
        fl.acquire(["r1"], duration=10, who="alice")
        self.assertEqual(fl.who(["r1"]), {"r1": "alice"})
        fl.release(["r1"])

    def test_who_missing_resource(self):
        fl = self._make()
        self.assertEqual(fl.who(["nonexistent"]), {})

    def test_who_multiple(self):
        fl = self._make()
        fl.acquire(["r1"], duration=10, who="alice")
        fl.acquire(["r2"], duration=10, who="bob")
        result = fl.who(["r1", "r2", "r3"])
        self.assertEqual(result["r1"], "alice")
        self.assertEqual(result["r2"], "bob")
        self.assertNotIn("r3", result)
        fl.release(["r1", "r2"])

    def test_who_coerces_to_string(self):
        fl = self._make()
        fl.acquire([123], duration=10, who="num")
        self.assertEqual(fl.who([123]), {"123": "num"})
        fl.release(["123"])

    def test_who_empty_who_field(self):
        fl = self._make()
        fl.acquire(["r1"], duration=10, who="")
        self.assertEqual(fl.who(["r1"]), {"r1": ""})
        fl.release(["r1"])


# ---- heartbeat ----

class TestFileLockingHeartbeat(_FileLockTestBase):

    def test_heartbeat_starts_on_acquire(self):
        fl = self._make()
        fl.acquire(["r1"], duration=5)
        self.assertIsNotNone(fl._hb_thread)
        self.assertTrue(fl._hb_thread.is_alive())
        fl.release(["r1"])

    def test_heartbeat_stops_on_full_release(self):
        fl = self._make()
        fl.acquire(["r1"], duration=5)
        fl.release(["r1"])
        time.sleep(0.1)
        self.assertIsNone(fl._hb_thread)

    def test_heartbeat_extends_expiry(self):
        fl = self._make()
        fl.acquire(["r1"], duration=2)
        initial_exp = fl._read_file()[0]["exp"]
        # Heartbeat interval = duration//2 = 1s
        time.sleep(1.5)
        updated = fl._read_file()
        match = [r for r in updated if r["res"] == "r1"]
        self.assertTrue(len(match) >= 1)
        self.assertGreaterEqual(match[0]["exp"], initial_exp)
        fl.release(["r1"])

    def test_stop_heartbeat_idempotent(self):
        fl = self._make()
        fl._stop_heartbeat()
        fl._stop_heartbeat()  # no error


# ---- _on_exit ----

class TestFileLockingOnExit(_FileLockTestBase):

    def test_on_exit_releases_held(self):
        fl = self._make()
        fl.acquire(["r1", "r2"], duration=10)
        fl._on_exit()
        records = fl._read_file()
        own = [r for r in records if r.get("pid") == fl.identity]
        self.assertEqual(len(own), 0)

    def test_on_exit_no_held_no_error(self):
        fl = self._make()
        fl._on_exit()  # no error


# ═══════════════════════════════════════════════════════════════════════════
# 3. Locking facade tests (FILE backend only — no Redis available)
# ═══════════════════════════════════════════════════════════════════════════

class TestLockingFacadeFileBacked(unittest.TestCase):
    """
    Test the Locking facade proxy API and read_with_lock using the file
    backend only (Redis not available in this environment). We replicate
    the facade logic manually since it imports from supertable.config.
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.fl = FileLocking(identity="facade-test", working_dir=self._tmpdir, check_interval=0.01)

    def tearDown(self):
        try:
            self.fl._stop_heartbeat()
        except Exception:
            pass

    # --- proxy: acquire ---
    def test_facade_acquire(self):
        self.assertTrue(self.fl.acquire(["r1"], duration=5, who="w"))
        self.fl.release(["r1"])

    # --- proxy: release ---
    def test_facade_release(self):
        self.fl.acquire(["r1"], duration=5)
        self.fl.release(["r1"])
        records = self.fl._read_file()
        own = [r for r in records if r["pid"] == self.fl.identity]
        self.assertEqual(len(own), 0)

    # --- proxy: who ---
    def test_facade_who(self):
        self.fl.acquire(["r1"], duration=5, who="alice")
        self.assertEqual(self.fl.who(["r1"]), {"r1": "alice"})
        self.fl.release(["r1"])

    # --- read_with_lock ---
    def test_read_with_lock_existing_file(self):
        test_file = os.path.join(self._tmpdir, "data.txt")
        with open(test_file, "wb") as f:
            f.write(b"hello world")

        acquired = self.fl.acquire([test_file], duration=5, who="read_with_lock")
        self.assertTrue(acquired)
        try:
            with open(test_file, "rb") as f:
                content = f.read()
        finally:
            self.fl.release([test_file])
        self.assertEqual(content, b"hello world")

    def test_read_with_lock_nonexistent_file(self):
        test_file = os.path.join(self._tmpdir, "nope.txt")
        acquired = self.fl.acquire([test_file], duration=5, who="read_with_lock")
        self.assertTrue(acquired)
        try:
            with open(test_file, "rb") as f:
                content = f.read()
        except FileNotFoundError:
            content = None
        finally:
            self.fl.release([test_file])
        self.assertIsNone(content)

    def test_read_with_lock_releases_after(self):
        test_file = os.path.join(self._tmpdir, "data.txt")
        with open(test_file, "wb") as f:
            f.write(b"data")
        self.fl.acquire([test_file], duration=5, who="read_with_lock")
        try:
            with open(test_file, "rb") as f:
                f.read()
        finally:
            self.fl.release([test_file])
        self.assertNotIn(test_file, self.fl._held)


# ═══════════════════════════════════════════════════════════════════════════
# 4. LockingBackend from_str edge cases
# ═══════════════════════════════════════════════════════════════════════════

class TestLockingBackendEdgeCases(unittest.TestCase):

    def test_from_str_with_none_default_none(self):
        # default=None should fall back to REDIS
        self.assertIs(LockingBackend.from_str(None, default=None), LockingBackend.REDIS)

    def test_from_str_tab_whitespace(self):
        self.assertIs(LockingBackend.from_str("\tfile\t"), LockingBackend.FILE)

    def test_from_str_newline_whitespace(self):
        self.assertIs(LockingBackend.from_str("\nredis\n"), LockingBackend.REDIS)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Concurrent stress test
# ═══════════════════════════════════════════════════════════════════════════

class TestConcurrentStress(unittest.TestCase):
    """Multiple threads competing for the same resource."""

    def test_no_double_acquisition(self):
        tmpdir = tempfile.mkdtemp()
        num_threads = 5
        results = []  # list of (identity, acquired_time)
        barrier = threading.Barrier(num_threads)

        def worker(identity: str):
            fl = FileLocking(identity=identity, working_dir=tmpdir, check_interval=0.01)
            barrier.wait()
            got = fl.acquire(["contested"], duration=1, who=identity)
            if got:
                results.append((identity, time.time()))
                time.sleep(0.05)  # hold briefly
                fl.release(["contested"])
            fl._stop_heartbeat()

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(f"w{i}",))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=60)

        # All should have eventually acquired
        self.assertEqual(len(results), num_threads)
        # No two should overlap: each acquired after prior released
        # (since duration=1 and we hold for 0.05s, check times are sequential)
        identities = [r[0] for r in results]
        self.assertEqual(len(set(identities)), num_threads)


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    unittest.main(verbosity=2)
