# route: supertable.locking.tests.test_a_lock_object_is_kept_only_while_it_holds_a_lock
"""
A ``RedisLocking`` is kept by the process only while it holds a lock.

Every ``RedisCatalog`` builds one, and a read or a write builds a catalog
several times. Each used to hand ``atexit`` its own ``_on_exit``, and atexit
keeps what it is given until the process ends: no lock object was ever freed,
11 to 15 KB stayed behind for every read and every write, and the list of exit
callbacks grew by one for each.

What that registration promised is kept another way. One exit hook serves
every instance, and it knows an instance only while that instance holds a
lock. So:

* an instance that holds none is freed when its owner drops it;
* a lock still held at exit is released, whoever still refers to its holder,
  and never one that has passed to somebody else.
"""

from __future__ import annotations

import atexit
import gc
import os
import subprocess
import sys
import textwrap
import threading
import time
import weakref
from pathlib import Path
from types import SimpleNamespace

import pytest

from supertable.locking import redis_lock
from supertable.locking.redis_lock import RedisLocking
from supertable.locking.tests.test_redis_lock import FakeRedis


@pytest.fixture(autouse=True)
def _nothing_left_holding():
    """Release whatever a test left held, so no test hands a holder to the next."""
    before = set(redis_lock._holders)
    yield
    for locker in list(set(redis_lock._holders) - before):
        locker._on_exit()


def _live_lock_objects() -> int:
    gc.collect()
    return sum(1 for obj in gc.get_objects() if type(obj) is RedisLocking)


def _wait_until(condition, seconds: float = 5.0) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


# ---------------------------------------------------------------------------
# An instance that holds no lock
# ---------------------------------------------------------------------------

class TestOneThatHoldsNoLock:

    def test_building_it_adds_no_exit_callback(self):
        before = atexit._ncallbacks()
        lockers = [RedisLocking(FakeRedis()) for _ in range(50)]
        assert atexit._ncallbacks() == before
        assert len(lockers) == 50

    def test_it_is_freed_when_its_owner_drops_it(self):
        locker = RedisLocking(FakeRedis())
        ref = weakref.ref(locker)
        del locker
        gc.collect()
        assert ref() is None

    def test_it_is_freed_without_the_cycle_collector(self):
        """Nothing refers back to it, so dropping the last name is enough:
        a busy service does not wait for a collection to get the memory back."""
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            locker = RedisLocking(FakeRedis())
            ref = weakref.ref(locker)
            del locker
            assert ref() is None
        finally:
            if was_enabled:
                gc.enable()

    def test_a_thousand_of_them_leave_none_behind(self):
        fake = FakeRedis()
        before = _live_lock_objects()
        for _ in range(1000):
            RedisLocking(fake)
        assert _live_lock_objects() == before

    def test_it_is_not_known_to_the_exit_hook(self):
        locker = RedisLocking(FakeRedis())
        assert locker not in redis_lock._holders


# ---------------------------------------------------------------------------
# An instance that has locked
# ---------------------------------------------------------------------------

class TestOneThatHasLocked:

    def test_taking_a_lock_adds_no_exit_callback(self):
        before = atexit._ncallbacks()
        fake = FakeRedis()
        for i in range(20):
            locker = RedisLocking(fake)
            token = locker.acquire(f"k{i}", ttl_s=30, timeout_s=1)
            locker.release(f"k{i}", token)
        assert atexit._ncallbacks() == before

    def test_it_is_kept_while_it_holds_the_lock_even_if_its_owner_drops_it(self):
        """A lock nobody refers to any more is still a lock to release at exit."""
        fake = FakeRedis()
        locker = RedisLocking(fake)
        token = locker.acquire("k", ttl_s=30, timeout_s=1)
        ref = weakref.ref(locker)
        del locker
        gc.collect()

        assert ref() is not None
        assert ref() in redis_lock._holders
        assert fake.get("k") == token
        assert ref()._hb_thread.is_alive()

        ref().release("k", token)

    def test_it_is_kept_by_the_lock_alone_not_by_its_heartbeat(self):
        """The heartbeat thread refers to its instance too, while it runs. The
        instance must not depend on that: a lock whose heartbeat has stopped is
        still held in Redis until its TTL, and is still released at exit."""
        fake = FakeRedis()
        locker = RedisLocking(fake)
        token = locker.acquire("k", ttl_s=30, timeout_s=1)
        locker._stop_heartbeat()
        ref = weakref.ref(locker)
        del locker
        gc.collect()

        assert ref() is not None
        assert fake.get("k") == token

        redis_lock._release_held_locks()

        assert fake.get("k") is None
        gc.collect()
        assert ref() is None

    def test_it_is_freed_once_it_has_released(self):
        locker = RedisLocking(FakeRedis())
        token = locker.acquire("k", ttl_s=30, timeout_s=1)
        assert locker.release("k", token) is True
        assert locker not in redis_lock._holders

        ref = weakref.ref(locker)
        del locker
        gc.collect()
        assert ref() is None

    def test_holding_two_locks_and_releasing_one_keeps_it(self):
        locker = RedisLocking(FakeRedis())
        token_a = locker.acquire("a", ttl_s=30, timeout_s=1)
        token_b = locker.acquire("b", ttl_s=30, timeout_s=1)

        locker.release("a", token_a)
        assert locker in redis_lock._holders

        locker.release("b", token_b)
        assert locker not in redis_lock._holders

    def test_a_release_with_the_wrong_token_keeps_it(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        token = locker.acquire("k", ttl_s=30, timeout_s=1)

        assert locker.release("k", "not-the-token") is False
        assert locker in redis_lock._holders
        assert fake.get("k") == token

        locker.release("k", token)

    def test_releasing_a_key_it_never_held_changes_nothing(self):
        locker = RedisLocking(FakeRedis())
        token = locker.acquire("k", ttl_s=30, timeout_s=1)

        assert locker.release("another", "anything") is False
        assert locker in redis_lock._holders

        locker.release("k", token)
        assert locker not in redis_lock._holders

    def test_locking_again_after_a_release_is_noted_again(self):
        locker = RedisLocking(FakeRedis())
        first = locker.acquire("k", ttl_s=30, timeout_s=1)
        locker.release("k", first)
        assert locker not in redis_lock._holders

        second = locker.acquire("k", ttl_s=30, timeout_s=1)
        assert locker in redis_lock._holders
        locker.release("k", second)
        assert locker not in redis_lock._holders

    def test_a_lock_it_could_not_get_does_not_note_it(self):
        fake = FakeRedis()
        owner = RedisLocking(fake)
        token = owner.acquire("k", ttl_s=30, timeout_s=1)
        late = RedisLocking(fake)

        assert late.acquire("k", ttl_s=30, timeout_s=1, retry_interval=0.01) is None
        assert late not in redis_lock._holders

        owner.release("k", token)


# ---------------------------------------------------------------------------
# A lock that is lost: the heartbeat finds it gone
# ---------------------------------------------------------------------------

class TestOneWhoseLockIsLost:

    def test_it_is_forgotten_when_its_only_lock_is_lost(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        token = locker.acquire("k", ttl_s=1, timeout_s=1)   # heartbeat every second
        assert locker in redis_lock._holders

        fake.set("k", "somebody-elses-token")               # the lock changes hands

        held = locker._held
        assert _wait_until(lambda: not held)
        assert locker not in redis_lock._holders
        assert locker.is_held("k", token) is False
        assert fake.get("k") == "somebody-elses-token"

        ref = weakref.ref(locker)
        thread = locker._hb_thread
        del locker
        thread.join(timeout=5)                               # it ends itself: nothing left to renew
        del thread
        gc.collect()
        assert ref() is None

    def test_losing_one_of_two_locks_keeps_it(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        lost = locker.acquire("goes", ttl_s=1, timeout_s=1)
        kept = locker.acquire("stays", ttl_s=30, timeout_s=1)

        fake.set("goes", "somebody-elses-token")

        assert _wait_until(lambda: "goes" not in locker._held)
        assert "stays" in locker._held
        assert locker in redis_lock._holders
        assert locker.is_held("goes", lost) is False
        assert locker.is_held("stays", kept) is True

        locker.release("stays", kept)
        assert locker not in redis_lock._holders


# ---------------------------------------------------------------------------
# The exit hook
# ---------------------------------------------------------------------------

class TestAtExit:

    def test_a_lock_still_held_is_released(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("k", ttl_s=30, timeout_s=1)

        redis_lock._release_held_locks()

        assert fake.get("k") is None
        assert locker._held == {}
        assert locker._hb_thread is None
        assert locker not in redis_lock._holders

    def test_one_whose_owner_dropped_it_is_released_too_and_then_freed(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("k", ttl_s=30, timeout_s=1)
        ref = weakref.ref(locker)
        del locker

        redis_lock._release_held_locks()

        assert fake.get("k") is None
        gc.collect()
        assert ref() is None

    def test_every_lock_of_every_object_is_released(self):
        fake = FakeRedis()
        lockers = [RedisLocking(fake) for _ in range(3)]
        for i, locker in enumerate(lockers):
            locker.acquire(f"a{i}", ttl_s=30, timeout_s=1)
            locker.acquire(f"b{i}", ttl_s=30, timeout_s=1)

        redis_lock._release_held_locks()

        for i in range(3):
            assert fake.get(f"a{i}") is None
            assert fake.get(f"b{i}") is None
        assert not (set(lockers) & redis_lock._holders)

    def test_a_lock_that_passed_to_somebody_else_is_left_alone(self):
        """The release compares tokens, at exit as at any other time."""
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("k", ttl_s=30, timeout_s=1)
        fake.set("k", "the-next-owners-token")

        redis_lock._release_held_locks()

        assert fake.get("k") == "the-next-owners-token"
        assert locker not in redis_lock._holders

    def test_an_object_that_holds_nothing_is_not_touched(self, monkeypatch):
        idle = RedisLocking(FakeRedis())
        released = RedisLocking(FakeRedis())
        released.release("k", released.acquire("k", ttl_s=30, timeout_s=1))
        calls = []
        monkeypatch.setattr(idle, "_on_exit", lambda: calls.append("idle"))
        monkeypatch.setattr(released, "_on_exit", lambda: calls.append("released"))

        redis_lock._release_held_locks()

        assert calls == []

    def test_one_object_failing_does_not_keep_the_others_locks(self, monkeypatch):
        fake = FakeRedis()
        failing = RedisLocking(fake)
        failing_token = failing.acquire("failing", ttl_s=30, timeout_s=1)
        others = [RedisLocking(fake) for _ in range(4)]
        for i, locker in enumerate(others):
            locker.acquire(f"other{i}", ttl_s=30, timeout_s=1)
        real_on_exit = failing._on_exit

        def boom():
            raise RuntimeError("this one cannot clean up")

        monkeypatch.setattr(failing, "_on_exit", boom)

        redis_lock._release_held_locks()      # must not raise

        for i in range(4):
            assert fake.get(f"other{i}") is None
        assert fake.get("failing") == failing_token

        monkeypatch.setattr(failing, "_on_exit", real_on_exit)
        failing.release("failing", failing_token)

    def test_running_it_twice_is_harmless(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("k", ttl_s=30, timeout_s=1)

        redis_lock._release_held_locks()
        redis_lock._release_held_locks()

        assert fake.get("k") is None

    def test_a_lock_taken_after_it_ran_is_noted_again(self):
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("k", ttl_s=30, timeout_s=1)
        redis_lock._release_held_locks()

        token = locker.acquire("k", ttl_s=30, timeout_s=1)
        assert locker in redis_lock._holders
        locker.release("k", token)

    def test_the_instance_method_still_releases_its_own_locks(self):
        """``_on_exit`` is what the hook calls, and callers use it directly."""
        fake = FakeRedis()
        locker = RedisLocking(fake)
        locker.acquire("a", ttl_s=30, timeout_s=1)
        locker.acquire("b", ttl_s=30, timeout_s=1)

        locker._on_exit()

        assert fake.get("a") is None and fake.get("b") is None
        assert locker._held == {}
        assert locker._hb_thread is None
        assert locker not in redis_lock._holders


# ---------------------------------------------------------------------------
# A real interpreter exit
# ---------------------------------------------------------------------------

_AT_EXIT = textwrap.dedent(
    """
    import atexit
    import sys

    seen = {}


    def report():
        fake = seen["fake"]
        sys.stdout.write("AFTER kept=%r dropped=%r released=%r\\n" % (
            fake.get("kept"), fake.get("dropped"), fake.get("released")))
        sys.stdout.flush()


    # Registered before the lock module is imported, so it runs after that
    # module's exit hook: exit callbacks run last in, first out.
    atexit.register(report)

    from supertable.locking.redis_lock import RedisLocking
    from supertable.locking.tests.test_redis_lock import FakeRedis


    def registered_later():
        fake = seen["fake"]
        sys.stdout.write("MID kept_held=%s dropped_held=%s\\n" % (
            fake.get("kept") is not None, fake.get("dropped") is not None))
        sys.stdout.flush()


    # Registered after the lock module was imported, before any lock object
    # exists: it runs before that module's exit hook.
    atexit.register(registered_later)

    callbacks = atexit._ncallbacks()
    fake = seen["fake"] = FakeRedis()

    kept = RedisLocking(fake)
    kept_token = kept.acquire("kept", ttl_s=30, timeout_s=2)

    dropped = RedisLocking(fake)
    dropped_token = dropped.acquire("dropped", ttl_s=30, timeout_s=2)
    del dropped

    released = RedisLocking(fake)
    released.release("released", released.acquire("released", ttl_s=30, timeout_s=2))

    idle = [RedisLocking(fake) for _ in range(300)]

    sys.stdout.write("BEFORE kept=%s dropped=%s released=%r callbacks_added=%d\\n" % (
        fake.get("kept") == kept_token, fake.get("dropped") == dropped_token,
        fake.get("released"), atexit._ncallbacks() - callbacks))
    sys.stdout.flush()
    """
)


@pytest.fixture(scope="module")
def exit_report(tmp_path_factory):
    """Run a small program that exits while holding two locks, once."""
    workdir = tmp_path_factory.mktemp("exit")
    repo_root = Path(redis_lock.__file__).resolve().parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")
    env["SUPERTABLE_HOME"] = str(workdir / "home")
    done = subprocess.run(
        [sys.executable, "-c", _AT_EXIT], cwd=str(workdir), env=env,
        capture_output=True, text=True, timeout=180, check=False,
    )
    assert done.returncode == 0, done.stderr[-2000:]
    lines = {line.split(" ", 1)[0]: line for line in done.stdout.splitlines()
             if line.startswith(("BEFORE", "MID", "AFTER"))}
    assert set(lines) == {"BEFORE", "MID", "AFTER"}, done.stdout + done.stderr[-2000:]
    return SimpleNamespace(before=lines["BEFORE"], mid=lines["MID"], after=lines["AFTER"])


class TestARealInterpreterExit:

    def test_the_locks_were_held_until_the_exit(self, exit_report):
        assert "kept=True dropped=True released=None" in exit_report.before

    def test_three_hundred_idle_objects_registered_nothing(self, exit_report):
        assert exit_report.before.endswith("callbacks_added=0")

    def test_a_hook_registered_after_the_module_was_imported_runs_first(self, exit_report):
        """Exit hooks run last in, first out, and this module registers its one
        hook when it is imported. So whatever the process registers later (the
        streaming runner's ``shutdown`` is one) runs while the locks are still
        held: work is stopped first, and what it left locked is released after.

        With a hook per lock object, registered when the object was built, the
        order depended on when each object happened to be built: the locks of
        objects built after ``shutdown`` was registered were released before it
        ran, under writes still in flight."""
        assert "kept_held=True dropped_held=True" in exit_report.mid

    def test_the_lock_its_owner_still_referred_to_is_released(self, exit_report):
        assert "kept=None" in exit_report.after

    def test_the_lock_its_owner_had_dropped_is_released(self, exit_report):
        assert "dropped=None" in exit_report.after


# ---------------------------------------------------------------------------
# Many threads
# ---------------------------------------------------------------------------

class TestManyThreads:

    def test_each_with_its_own_object(self):
        fake = FakeRedis()
        errors, used = [], []
        used_lock = threading.Lock()

        def work(n):
            try:
                for i in range(25):
                    locker = RedisLocking(fake)
                    key = f"own-{n}-{i}"
                    token = locker.acquire(key, ttl_s=30, timeout_s=5)
                    assert token is not None
                    assert locker in redis_lock._holders
                    assert locker.release(key, token) is True
                    with used_lock:
                        used.append(locker)
            except Exception as error:  # noqa: BLE001 - reported below
                errors.append(repr(error))

        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        assert errors == []
        assert len(used) == 8 * 25
        assert not (set(used) & redis_lock._holders)
        assert all(fake.get(f"own-{n}-{i}") is None for n in range(8) for i in range(25))

    def test_all_sharing_one_object(self):
        fake = FakeRedis()
        shared = RedisLocking(fake)
        errors = []

        def work(n):
            try:
                for i in range(25):
                    key = f"shared-{n}-{i}"
                    token = shared.acquire(key, ttl_s=30, timeout_s=5)
                    assert token is not None
                    assert shared.release(key, token) is True
            except Exception as error:  # noqa: BLE001 - reported below
                errors.append(repr(error))

        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        assert errors == []
        assert shared._held == {}
        assert shared not in redis_lock._holders
        assert all(fake.get(f"shared-{n}-{i}") is None for n in range(8) for i in range(25))


# ---------------------------------------------------------------------------
# Through the catalog, which is how every read and write gets one
# ---------------------------------------------------------------------------

class TestThroughTheCatalog:

    @pytest.fixture
    def catalog_class(self, monkeypatch):
        fakeredis = pytest.importorskip("fakeredis")
        from supertable import redis_catalog

        client = fakeredis.FakeStrictRedis(decode_responses=True)
        monkeypatch.setattr(redis_catalog, "RedisConnector",
                            lambda options=None: SimpleNamespace(r=client))
        # One lock round trip before any test counts objects. The first call of
        # a script the server has not seen raises NoScriptError inside the
        # client, which loads the script and calls again. fakeredis keeps that
        # one exception in a frame of its connection, and through its traceback
        # the object that was releasing. Once per fake server, not per catalog.
        first = redis_catalog.RedisCatalog()
        first.release_simple_lock(
            "acme", "lake", "first",
            first.acquire_simple_lock("acme", "lake", "first", ttl_s=30, timeout_s=2))
        del first
        return redis_catalog.RedisCatalog

    def test_a_catalog_built_and_dropped_leaves_no_lock_object(self, catalog_class):
        catalog_class()                       # whatever a first build keeps
        before_objects, before_callbacks = _live_lock_objects(), atexit._ncallbacks()

        for _ in range(40):
            catalog_class()

        assert _live_lock_objects() == before_objects
        assert atexit._ncallbacks() == before_callbacks

    def test_nor_does_one_that_locked_and_released_a_table(self, catalog_class):
        catalog_class()
        before_objects, before_callbacks = _live_lock_objects(), atexit._ncallbacks()

        for i in range(10):
            catalog = catalog_class()
            token = catalog.acquire_simple_lock("acme", "lake", f"t{i}", ttl_s=30, timeout_s=2)
            assert token
            assert catalog.verify_simple_lock("acme", "lake", f"t{i}", token) is True
            assert catalog.release_simple_lock("acme", "lake", f"t{i}", token) is True
            del catalog

        assert _live_lock_objects() == before_objects
        assert atexit._ncallbacks() == before_callbacks

    def test_a_table_lock_held_at_exit_is_released(self, catalog_class):
        catalog = catalog_class()
        token = catalog.acquire_simple_lock("acme", "lake", "t", ttl_s=30, timeout_s=2)
        assert catalog.verify_simple_lock("acme", "lake", "t", token) is True
        probe = catalog_class()

        redis_lock._release_held_locks()

        assert probe.verify_simple_lock("acme", "lake", "t", token) is False
        retaken = probe.acquire_simple_lock("acme", "lake", "t", ttl_s=30, timeout_s=2)
        assert retaken and retaken != token
        probe.release_simple_lock("acme", "lake", "t", retaken)
