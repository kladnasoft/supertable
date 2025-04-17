import tempfile
import pytest
from supertable.locking.locking import Locking
from supertable.locking.locking_backend import LockingBackend

try:
    import redis
except ImportError:
    redis = None


# ---------------------------
# File-based locking tests
# ---------------------------

def test_file_locking_context_manager():
    with tempfile.TemporaryDirectory() as tmp_dir:
        resource = "test_resource_file"
        # Create the first lock using file-based locking.
        lock1 = Locking(identity=resource, backend=LockingBackend.FILE, working_dir=tmp_dir)
        with lock1:
            # While lock1 is active, try acquiring the same resource with a second lock.
            lock2 = Locking(identity=resource, backend=LockingBackend.FILE, working_dir=tmp_dir)
            acquired = lock2.self_lock(timeout_seconds=1, lock_duration_seconds=2)
            assert acquired is False, "Should not acquire the lock if resource is already locked"
        # After lock1 releases, lock2 should be able to acquire the lock.
        acquired = lock2.self_lock(timeout_seconds=1, lock_duration_seconds=2)
        assert acquired is True, "Lock should be acquired after the previous lock is released"
        lock2.release_lock()

def test_file_locking_explicit_lock_release():
    with tempfile.TemporaryDirectory() as tmp_dir:
        resource = "test_resource_file_release"
        lock = Locking(identity=resource, backend=LockingBackend.FILE, working_dir=tmp_dir)
        acquired = lock.self_lock(timeout_seconds=1, lock_duration_seconds=2)
        assert acquired is True, "Lock should be acquired initially"
        lock.release_lock()
        # Now try to acquire the lock again.
        acquired = lock.self_lock(timeout_seconds=1, lock_duration_seconds=2)
        assert acquired is True, "Lock should be acquired after explicit release"
        lock.release_lock()


# ---------------------------
# Redis-based locking tests
# ---------------------------

@pytest.fixture
def redis_available():
    if redis is None:
        pytest.skip("Redis module is not installed")
    try:
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.ping()
    except Exception:
        pytest.skip("Redis server is not available for testing")
    return True

def test_redis_locking_context_manager(redis_available):
    resource = "test_resource_redis"
    # Create the first Redis lock.
    lock1 = Locking(identity=resource, backend=LockingBackend.REDIS)
    with lock1:
        # Attempt to acquire the same resource with a second Redis lock.
        lock2 = Locking(identity=resource, backend=LockingBackend.REDIS)
        acquired = lock2.self_lock(timeout_seconds=1, lock_duration_seconds=2)
        assert acquired is False, "Should not acquire Redis lock if resource is already locked"
    # Now that lock1 is released, lock2 should be able to acquire the lock.
    acquired = lock2.self_lock(timeout_seconds=1, lock_duration_seconds=2)
    assert acquired is True, "Redis lock should be acquired after previous lock is released"
    lock2.release_lock()

def test_redis_locking_explicit_lock_release(redis_available):
    resource = "test_resource_redis_release"
    lock = Locking(identity=resource, backend=LockingBackend.REDIS)
    acquired = lock.self_lock(timeout_seconds=1, lock_duration_seconds=2)
    assert acquired is True, "Redis lock should be acquired initially"
    lock.release_lock()
    # Try acquiring again after release.
    acquired = lock.self_lock(timeout_seconds=1, lock_duration_seconds=2)
    assert acquired is True, "Redis lock should be acquired after explicit release"
    lock.release_lock()
