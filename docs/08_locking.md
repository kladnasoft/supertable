# Locking

## Business Context

SuperTable is a multi-writer system. Multiple API servers, ingestion workers, and background tasks (GC, compaction, data quality) can attempt to modify the same table concurrently. Without coordination, concurrent writes would corrupt snapshots, produce orphaned Parquet files, or silently lose data.

The locking subsystem provides mutual exclusion at the SimpleTable level: only one writer can modify a given table at a time. Two interchangeable backends are provided:

- **`RedisLocking`** -- distributed locks for production multi-host deployments. Uses Redis `SET NX EX` with Lua-script-based atomic operations and a background heartbeat thread.
- **`FileLocking`** -- POSIX `fcntl`-based file locks for single-host development environments. Same API surface, no external dependencies beyond the filesystem.

Both backends share the same public interface (`acquire`, `release`, `extend`) and the same token-based ownership model, so the rest of the codebase does not need to know which backend is active.

---

## Module Locations

| Module | Class | Purpose |
|---|---|---|
| `supertable/locking/redis_lock.py` | `RedisLocking` | Production distributed lock using Redis |
| `supertable/locking/file_lock.py` | `FileLocking` | Development single-host lock using POSIX fcntl |

---

## Redis Distributed Locks

### Overview

`RedisLocking` implements a distributed mutual exclusion lock using Redis as the coordination backend. It relies on three Redis primitives:

1. **`SET key token NX EX ttl`** -- atomic acquire (set-if-not-exists with expiry).
2. **Lua compare-and-delete** -- atomic release (only the holder can delete).
3. **Lua compare-and-extend** -- atomic TTL extension (only the holder can extend).

A background heartbeat thread automatically renews held locks at half their TTL, so long-running operations never lose their lock due to expiry. The TTL controls crash recovery time, not operation timeout: if the lock holder dies, the heartbeat thread dies with it, and Redis expires the key within one TTL cycle.

### Constructor

```python
class RedisLocking:
    def __init__(self, r: redis.Redis) -> None
```

Receives an already-configured `redis.Redis` client -- it never creates its own connection. This ensures lock traffic follows the same Sentinel / SSL / password / DB path as every other Redis consumer in the application.

On initialization:

- Registers the two Lua scripts with the Redis server via `r.register_script()`.
- Initializes `_held: Dict[str, Tuple[str, int]]` -- a dict mapping lock keys to `(token, ttl_ms)` for all locks held by this instance.
- Creates a `threading.Lock` (`_held_lock`) to protect concurrent access to the `_held` dict.
- Sets up heartbeat state (`_hb_stop` event, `_hb_thread`).
- Registers `_on_exit()` via `atexit` for best-effort cleanup on interpreter shutdown.

### Lua Scripts

#### Release (compare-and-delete)

```lua
local key = KEYS[1]
local token = ARGV[1]
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('DEL', key)
  return 1
end
return 0
```

This script atomically checks whether the current lock holder matches the provided token. If so, it deletes the key and returns 1. Otherwise, it returns 0 without modifying anything. This prevents a process from accidentally releasing a lock it no longer owns (e.g., after the lock expired and was re-acquired by another process).

#### Extend (compare-and-extend)

```lua
local key = KEYS[1]
local token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
return 0
```

Atomically extends the TTL of a lock only if the caller still owns it. Used by the heartbeat thread to keep locks alive during long operations.

### acquire()

```python
def acquire(
    self,
    key: str,
    ttl_s: int = 30,
    timeout_s: int = 30,
    retry_interval: float = 0.05,
) -> Optional[str]
```

**Parameters**:

| Parameter | Default | Description |
|---|---|---|
| `key` | (required) | The Redis key to lock |
| `ttl_s` | 30 | Lock TTL in seconds (controls crash recovery time) |
| `timeout_s` | 30 | Maximum time to wait for acquisition |
| `retry_interval` | 0.05 | Sleep between retry attempts (50ms) |

**Algorithm**:

1. Generate a unique token via `uuid.uuid4().hex`.
2. Compute `ttl_ms = max(1000, ttl_s * 1000)`.
3. Loop until `timeout_s` elapses:
   a. Attempt `SET key token NX EX ttl_s`.
   b. On success: register the lock in `_held`, start the heartbeat thread (if not already running), return the token.
   c. On failure or `RedisError`: sleep for `retry_interval` and retry.
4. On timeout: return `None`.

**Return value**: A unique token string on success, or `None` if the lock could not be acquired within the timeout.

### release()

```python
def release(self, key: str, token: str) -> bool
```

Releases the lock by executing the Lua compare-and-delete script. Regardless of whether the Lua script returns success (the lock may have already expired), the key is removed from the `_held` tracking dict. If no locks remain held, the heartbeat thread is stopped.

The heartbeat stop decision is made inside `_held_lock`, but `_stop_heartbeat()` is called outside to avoid deadlock (the heartbeat thread also acquires `_held_lock`).

### extend()

```python
def extend(self, key: str, token: str, ttl_ms: int) -> bool
```

Extends the lock TTL by executing the Lua compare-and-extend script. Returns `True` if the extension succeeded, `False` if the lock was lost.

### Heartbeat Thread

The heartbeat is a daemon thread (`_hb_loop`) that automatically extends all held locks at half their TTL interval.

```python
def _hb_loop(self) -> None:
    while not self._hb_stop.is_set():
        # Compute sleep = half of shortest TTL across held locks
        min_ttl_ms = min(ttl_ms for _, ttl_ms in self._held.values())
        interval_s = max(1.0, (min_ttl_ms / 1000.0) / 2.0)

        self._hb_stop.wait(timeout=interval_s)
        # ... extend all held locks ...
```

**Key behaviors**:

- **Adaptive interval**: Sleeps for half the shortest TTL across all currently held locks. This ensures every lock is renewed before it expires.
- **Interruptible**: Uses `Event.wait()` instead of `time.sleep()` so `_stop_heartbeat()` can wake it immediately.
- **Lost lock detection**: If `extend()` returns `False` for a lock (expired or stolen), the lock is removed from `_held` tracking and a debug message is logged.
- **Thread-safe**: The `_held` dict is always accessed under `_held_lock`. A snapshot is taken under the lock, then extensions are performed outside the lock.

### Crash Recovery

The TTL on the Redis key is the crash recovery mechanism:

1. Writer A acquires a lock with `ttl_s=30`.
2. The heartbeat thread renews the lock every 15 seconds.
3. Writer A crashes. The heartbeat thread dies with it.
4. After at most 30 seconds, Redis expires the key.
5. Writer B can now acquire the lock.

No manual intervention or separate crash-detection process is needed.

### Cleanup on Exit

```python
def _on_exit(self) -> None:
    # Best-effort release of all held locks on interpreter shutdown
    for key, (token, _) in snapshot.items():
        self.release(key, token)
    self._stop_heartbeat()
```

Registered via `atexit.register()`. Attempts to release all held locks during normal interpreter shutdown. Failures are silently ignored.

---

## File Locks (Development)

### Overview

`FileLocking` provides the same locking semantics using POSIX `fcntl` advisory locks on a shared JSON file. It is designed for single-host development environments where Redis is not available.

### Lock File Structure

The lock state is stored in a JSON file (default: `.lock.json`) as a list of lock records:

```json
[
  {"res": "my:lock:key", "exp": 1713192030, "tok": "a1b2c3d4..."},
  {"res": "other:key", "exp": 1713192045, "tok": "e5f6g7h8..."}
]
```

Each record contains:

| Field | Type | Description |
|---|---|---|
| `res` | string | The resource key being locked |
| `exp` | int | Unix timestamp when the lock expires |
| `tok` | string | UUID token identifying the lock holder |

### Constructor

```python
class FileLocking:
    def __init__(
        self,
        working_dir: str,
        lock_file_name: str = ".lock.json",
        retry_interval: float = 0.1,
    )
```

Creates the working directory if it does not exist. Registers `_on_exit()` via `atexit` for cleanup.

### Atomic Read-Write

The core primitive is `_atomic_read_write(callback)`:

```python
def _atomic_read_write(self, callback):
    with open(self.lock_path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)      # exclusive lock
        f.seek(0)
        records = json.loads(f.read() or "[]")
        new_records = callback(records)
        f.seek(0)
        f.truncate(0)
        f.write(json.dumps(new_records))
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f, fcntl.LOCK_UN)      # release
```

Uses `a+` mode to atomically create the file if missing (no TOCTOU race between existence check and open). The `LOCK_EX` ensures only one process can modify the file at a time. `os.fsync()` ensures data is durable before the lock is released.

### acquire()

```python
def acquire(
    self,
    key: str,
    ttl_s: int = 30,
    timeout_s: int = 30,
    retry_interval: float | None = None,
) -> Optional[str]
```

Same semantics as `RedisLocking.acquire()`:

1. Generate a UUID token.
2. Loop until timeout:
   a. Open the lock file under `LOCK_EX`.
   b. Purge expired records.
   c. Check if `key` is already held by a different token. If so, retry.
   d. If the key is free, append a new record with expiry `now + ttl_s`.
   e. Return the token.
3. On timeout: return `None`.

Starts the heartbeat thread on successful acquisition.

### release()

```python
def release(self, key: str, token: str) -> bool
```

Opens the lock file under `LOCK_EX` and removes the record matching both `key` and `token`. Returns `True` if a record was removed. Stops the heartbeat if no locks remain held.

### extend()

```python
def extend(self, key: str, token: str, ttl_ms: int) -> bool
```

Opens the lock file under `LOCK_EX` and updates the `exp` field of the matching record to `now + ttl_ms / 1000`. Returns `True` if the record was found and updated.

### who()

```python
def who(self, key: str) -> Optional[str]
```

Returns the token currently holding the specified key, or `None` if the key is not locked. Only checks non-expired records. This method is not present on `RedisLocking` and is specific to the file-lock backend for debugging purposes.

### Heartbeat

The file-lock heartbeat works similarly to the Redis version but uses a single fixed TTL for all held locks:

```python
def _hb_loop(self) -> None:
    while not self._hb_stop.is_set():
        interval = max(1, self._hb_ttl_s // 2)
        self._hb_stop.wait(timeout=interval)
        # Refresh all held locks via _atomic_read_write
```

The refresh callback updates the `exp` field of all records whose token appears in `self._held` and then purges expired records.

---

## Backend Comparison

| Feature | `RedisLocking` | `FileLocking` |
|---|---|---|
| **Deployment** | Multi-host (production) | Single-host (development) |
| **Coordination** | Redis server | POSIX fcntl on shared file |
| **Acquire** | `SET NX EX` with retry | `fcntl.LOCK_EX` + JSON read/write |
| **Release** | Lua compare-and-delete (atomic) | `fcntl.LOCK_EX` + JSON read/write |
| **Extend** | Lua compare-and-extend (atomic) | `fcntl.LOCK_EX` + JSON read/write |
| **Heartbeat** | Half of shortest held TTL | Half of TTL (fixed per instance) |
| **Crash recovery** | Redis key expires after TTL | Lock record expires after TTL |
| **Atomicity** | Server-side Lua scripts | Kernel-level file lock |
| **Dependencies** | `redis` Python package | POSIX `fcntl` (Linux/macOS) |
| **Throughput** | High (network + Redis) | Moderate (disk I/O + fsync) |
| **`who()` method** | Not available | Available for debugging |
| **Token storage** | Redis key value | JSON record in file |
| **Expiry purge** | Automatic (Redis TTL) | On next acquire/extend |
| **Cleanup on exit** | `atexit` best-effort release | `atexit` best-effort release |

---

## Deadlock Prevention

The locking subsystem uses several strategies to prevent deadlocks:

1. **Single-level locking**: The write path acquires exactly one lock per operation (the per-SimpleTable lock). There is no hierarchy of nested locks that could create circular dependencies.

2. **Timeout-based acquisition**: Both backends use a bounded timeout (`timeout_s`, default 30 seconds). If a lock cannot be acquired within the timeout, `None` is returned (or `TimeoutError` is raised by the caller). This prevents indefinite blocking.

3. **TTL-based expiry**: Every lock has a finite TTL. Even if a holder crashes without releasing, the lock expires automatically. Redis handles this natively; the file backend purges expired records on the next operation.

4. **Token-based ownership**: Releases and extensions are conditioned on the caller's token matching the current holder. This prevents a slow process from accidentally releasing a lock that was already expired and re-acquired by another process.

5. **Heartbeat separation**: The heartbeat thread takes a snapshot of held locks under `_held_lock` and then performs extensions outside the lock. This prevents the heartbeat from blocking on `_held_lock` for extended periods.

6. **Lock release outside held_lock**: In `RedisLocking.release()`, the decision to stop the heartbeat is made inside `_held_lock`, but `_stop_heartbeat()` is called outside the lock to avoid deadlock with the heartbeat thread (which also acquires `_held_lock`).

---

## Configuration

### Default Constants

The lock system does not define its own module-level duration constants -- instead, callers specify TTL and timeout at each call site. The `DataWriter` uses:

```python
token = self.catalog.acquire_simple_lock(
    org, super_name, simple_name,
    ttl_s=30,       # lock TTL (crash recovery window)
    timeout_s=60    # maximum wait for acquisition
)
```

The staging area uses:

```python
acquired = self.catalog.r.set(lock_key, token, nx=True, ex=30)  # 30s TTL
```

Pipe operations use a shorter TTL:

```python
acquired = self.catalog.r.set(lock_key, token, nx=True, ex=10)  # 10s TTL
```

### RedisLocking Defaults

| Parameter | Default | Description |
|---|---|---|
| `ttl_s` | 30 | Lock TTL in seconds |
| `timeout_s` | 30 | Maximum wait time for acquisition |
| `retry_interval` | 0.05 | Sleep between retries (50ms) |

### FileLocking Defaults

| Parameter | Default | Description |
|---|---|---|
| `lock_file_name` | `.lock.json` | Name of the lock state file |
| `retry_interval` | 0.1 | Sleep between retries (100ms) |
| `ttl_s` | 30 | Default lock TTL |
| `timeout_s` | 30 | Default maximum wait time |

### Heartbeat Timing

| Backend | Heartbeat Interval |
|---|---|
| `RedisLocking` | `max(1.0, (min_ttl_ms / 1000) / 2)` seconds -- adapts to the shortest held lock |
| `FileLocking` | `max(1, ttl_s // 2)` seconds -- fixed per instance |

With the default 30-second TTL, the heartbeat fires every 15 seconds, giving two renewal opportunities before expiry.

---

## Usage Patterns

### Direct Usage (RedisLocking)

```python
from supertable.redis_connector import create_redis_client
from supertable.locking.redis_lock import RedisLocking

locker = RedisLocking(create_redis_client())
token = locker.acquire("my:lock:key", ttl_s=30, timeout_s=10)
if token:
    try:
        # ... critical section ...
        pass
    finally:
        locker.release("my:lock:key", token)
```

### Via RedisCatalog (DataWriter)

The `RedisCatalog` class wraps `RedisLocking` and provides domain-specific lock methods:

```python
token = self.catalog.acquire_simple_lock(org, super_name, simple_name, ttl_s=30, timeout_s=60)
# ... write operations ...
self.catalog.release_simple_lock(org, super_name, simple_name, token)
```

### Via Staging Area (Inline Lock)

The `Staging` and `SuperPipe` classes use inline Redis `SET NX EX` for lighter-weight locking:

```python
from supertable import redis_keys as RK

lock_key = RK.lock_stage(org, sup, staging_name)
# → supertable:{org}:lakes:{sup}:lock:stage:doc:{staging_name}
token = uuid.uuid4().hex
acquired = self.catalog.r.set(lock_key, token, nx=True, ex=30)
# ... operation ...
# Release via Lua compare-and-delete
lua = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""
self.catalog.r.eval(lua, 1, lock_key, token)
```
