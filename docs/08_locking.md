# Locking

## Business Context

SuperTable is a multi-writer system. Multiple API servers, ingestion workers, and background tasks (GC, compaction, data quality) can attempt to modify the same table concurrently. Without coordination, concurrent writes would corrupt snapshots, produce orphaned Parquet files, or silently lose data.

The locking subsystem provides mutual exclusion at the SimpleTable level: only one writer can modify a given table at a time.

**`RedisLocking` is the only locking backend.** There is no backend selection, no factory, and no configuration switch: `RedisCatalog.__init__` constructs `RedisLocking(self.r)` directly, and every lock in the system flows through the `RedisCatalog` domain methods. A working Redis is therefore a hard requirement for any write path, not a production-only optimization.

> Earlier releases shipped a `FileLocking` class alongside it. It was never wired into `RedisCatalog` and it was never selectable, so it never held a real SuperTable lock. It also coordinated exclusively through POSIX `fcntl` on a local path and never touched `StorageInterface`, which means it could not have worked on object storage: two pods sharing an S3/MinIO bucket have no shared filesystem, so each would have taken an independent, mutually invisible lock while believing it was serialized. It has been deleted.

---

## Module Locations

| Module | Class | Purpose |
|---|---|---|
| `supertable/locking/redis_lock.py` | `RedisLocking` | The distributed lock — the only backend |

---

## Redis Distributed Locks

### Overview

`RedisLocking` implements a distributed mutual exclusion lock using Redis as the coordination backend. It relies on four Redis primitives:

1. **`SET key token NX EX ttl`** -- atomic acquire (set-if-not-exists with expiry).
2. **Lua compare-and-delete** -- atomic release (only the holder can delete).
3. **Lua compare-and-extend** -- atomic TTL extension (only the holder can extend).
4. **Lua batched compare-and-extend** -- one round trip that renews every held lease.

A background heartbeat thread automatically renews held locks at half their TTL, so long-running operations never lose their lock due to expiry. The TTL controls crash recovery time, not operation timeout: if the lock holder dies, the heartbeat thread dies with it, and Redis expires the key within one TTL cycle.

### Constructor

```python
class RedisLocking:
    def __init__(self, r: redis.Redis) -> None
```

Receives an already-configured `redis.Redis` client -- it never creates its own connection. This ensures lock traffic follows the same Sentinel / SSL / password / DB path as every other Redis consumer in the application.

On initialization:

- Registers the three Lua scripts with the Redis server via `r.register_script()`.
- Initializes `_held: Dict[str, Tuple[str, int]]` -- a dict mapping lock keys to `(token, ttl_ms)` for all locks held by this instance, guarded by `_held_lock`.
- Initializes `_lease_op_locks` -- a reference-counted `threading.Lock` per `(key, token)` pair, guarded by `_lease_op_locks_guard`. TTL mutation is serialized **per lease**, not process-wide, so a stalled Redis call for one lease cannot block renewal of an unrelated short-lived lease. Entries are dropped when their last reference goes, so heavy key churn leaves no registry behind.
- Initializes `_lost_leases` -- a bounded `OrderedDict` (`_LOST_LEASE_MEMORY = 256`) of leases the heartbeat proved lost, guarded by `_held_lock`.
- Sets up heartbeat state (`_hb_stop` event, `_hb_thread`) and records `_owner_pid`.
- Adds itself to the module-level `_LIVE_LOCKERS` registry (see [Process-Wide Registry](#process-wide-registry)).

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

Atomically extends the TTL of a lock only if the caller still owns it. Used by explicit `extend()` calls.

#### Extend-many (batched compare-and-extend)

```lua
local results = {}
for i = 1, #KEYS do
  local token = ARGV[(i - 1) * 2 + 1]
  local ttl_ms = tonumber(ARGV[(i - 1) * 2 + 2])
  local cur = redis.pcall('GET', KEYS[i])
  if type(cur) ~= 'table' and cur and cur == token then
    redis.call('PEXPIRE', KEYS[i], ttl_ms)
    results[i] = 1
  else
    results[i] = 0
  end
end
return results
```

The heartbeat renews every held lease in a single invocation, so one slow or lost key does not become a separate network round trip ahead of every sibling lease. The token check and `PEXPIRE` remain atomic per key. `redis.pcall` is deliberate: a corrupt or non-string value is loss of *that* lease, not permission to abort renewal of the healthy siblings behind it in the batch.

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
| `retry_interval` | 0.05 | Base sleep between retry attempts (50ms) |

**Algorithm**:

1. Generate a unique token via `uuid.uuid4().hex`.
2. Compute `ttl_ms = max(1000, ttl_s * 1000)` and `deadline = now + max(1, timeout_s)`.
3. Loop until the deadline elapses:
   a. Attempt `SET key token NX EX ttl_s`.
   b. On success: register the lock in `_held`, start or refresh the heartbeat generation, return the token.
   c. On failure or `RedisError`: sleep `_retry_delay(...)` and retry.
4. On timeout: return `None`.

**Return value**: A unique token string on success, or `None` if the lock could not be acquired within the timeout.

**Heartbeat generation on acquire.** If no heartbeat thread is alive, one is started. If one is already running, it is restarted when the new lease is *shorter* than the shortest currently held lease, or when this key was already tracked under a prior token — otherwise the running generation could still be sleeping for half of a much longer lease, or blocked renewing an expired prior token for this same key, and the new short lease would expire unrenewed. Per-`(key, token)` operation locks let the new generation bypass the obsolete one safely.

**Unreturned-lease safety.** If registration raises before the token reaches the caller (realistically `Thread.start` raising `RuntimeError` under thread exhaustion), the lease is live in Redis but nothing could ever release it — and the heartbeat would renew it forever, so the TTL could never reclaim it either. `_abandon_unreturned_lease()` compare-deletes it before the exception propagates.

### Retry pacing and jitter

```python
@staticmethod
def _retry_delay(attempt: int, retry_interval: float, deadline: float) -> float
```

The first `_ACQUIRE_FAST_RETRIES` (4) attempts sleep exactly `retry_interval`, so the uncontended path — four times the 50ms default covers a short critical section — never pays for jitter. After that, the delay becomes `random.uniform(retry_interval * 0.5, retry_interval * 1.5)`. The delay is then clamped so a waiter never sleeps past its own deadline.

This is **mean-preserving jitter, not backoff**. A fixed poll interval makes every waiter on a contended key wake in lockstep and race the same `SET NX`; spreading wake-ups over `[0.5x, 1.5x]` decorrelates that herd while leaving the mean poll rate identical, so the lock is never left idle longer than before. Exponential backoff was measured and rejected: this lock's bottleneck is the lock itself, not Redis, so backing off leaves the lock idle. With 16 waiters on 200ms sections at `timeout_s=5`, capped-exponential raised timeouts from 10 to 12 and p95 wait from 1931ms to 2665ms, while mean-preserving jitter measured 8-9 timeouts against a baseline of 10.

Jitter does **not** solve starvation and is not claimed to. `SET NX` contention has no queue, so a waiter's expected wait still scales with the number of contenders; the ceiling remains roughly `timeout_s / hold_time` concurrent writers per key. Bounding that properly needs a fair (FIFO) lock, which is a different design.

### release()

```python
def release(self, key: str, token: str) -> bool
```

Releases the lock by executing the Lua compare-and-delete script. Regardless of whether the Lua script returns success (the lock may have already expired), the key is removed from the `_held` tracking dict. If no locks remain held, the heartbeat thread is stopped.

The heartbeat stop decision is made inside `_held_lock`, but `_stop_heartbeat()` is called outside to avoid deadlock (the heartbeat thread also acquires `_held_lock`). It is called with `restart_if_held=True`, because another `acquire()` can race between the empty-`_held` decision and the actual stop — without the restart, a newly acquired long-running mutation would silently lose lease renewal.

### extend()

```python
def extend(self, key: str, token: str, ttl_ms: int) -> bool
```

Extends the lock TTL by executing the Lua compare-and-extend script. Returns `True` if the extension succeeded, `False` if the lock was definitively lost.

- `ttl_ms` must be an exact positive `int`. `bool` and `float` are rejected with `ValueError` (the check is `type(ttl_ms) is not int`, so `True` does not slip through as `1`), and the live lease is not mutated.
- The call is serialized under the `(key, token)` operation lock.
- A Lua `0` is definitive: the key is absent or another token owns it, so `False` is returned. A **transport exception is ambiguous** — Redis may even have applied the `PEXPIRE` before the reply was lost — so it propagates instead of being flattened to `False`. The heartbeat retains tracking and retries; treating ambiguity as loss would abandon a valid long-running compaction lease after one timeout.
- If the extension shortens the lease below the previous minimum, the heartbeat generation is restarted before returning, since `PEXPIRE` has already taken effect while an older generation may still be in a long sleep.

### lease_lost()

```python
def lease_lost(self, key: str, token: str) -> bool
```

Returns whether the heartbeat **proved** this exact lease was lost. A lost lease is not cosmetic: the holder is still doing storage I/O it can no longer publish (the commit fences on this exact token), and another writer may already be mutating the same table. `lease_lost()` lets a long operation ask before paying for work its commit cannot publish.

`False` is **not** a liveness guarantee — it only means no loss has been *observed* yet. This is a cheap early-abort hint, never a substitute for the token fence enforced inside the publication script. The backing `_lost_leases` record is bounded to the most recent 256 entries so a long-lived process with heavy key churn cannot accumulate them; a caller that cares checks within its own operation, long before eviction could matter.

### Heartbeat Thread

The heartbeat is a daemon thread (`_hb_loop`) that renews all held locks at half the shortest held TTL. It is **generation-based**: each start captures its own `threading.Event`, so stopping one generation can never signal or clobber a newer one.

Each cycle:

1. Compute `interval_s = max(0.05, (min_ttl_ms / 1000) / 2)` across held locks (or a shorter retry delay left over from the previous cycle) and `Event.wait()` on it.
2. Snapshot `_held` under `_held_lock`.
3. For each key, take the `(key, token)` operation lock **non-blocking**. A key whose lock is busy (an explicit `extend()` or another generation owns that lease's mutation boundary) is skipped and retried sooner rather than holding up the batch.
4. Skip keys whose `_held` entry is no longer current.
5. Renew everything remaining in **one** `EXTEND_MANY` invocation; an incomplete or non-list reply is treated as an error.
6. For every `0` result: log a warning, drop the key from `_held`, and record `(key, token)` in `_lost_leases`.

**Key behaviors**:

- **Adaptive interval**: half the shortest TTL across all currently held locks, so every lock is renewed before it expires.
- **Interruptible**: `Event.wait()` rather than `time.sleep()`, so `_stop_heartbeat()` wakes it immediately.
- **Batched**: one Lua round trip per cycle regardless of how many leases are held.
- **Fail-soft on transport errors**: a batch exception logs a redacted error type and retries at roughly a tenth of the shortest TTL (clamped to `[0.1s, 1.0s]`) without abandoning tracking.
- **Self-healing exit**: a natural loop exit has a narrow teardown window where a concurrent `acquire()` could observe this generation as still alive and skip starting a replacement. The `finally` block publishes termination under `_held_lock` and, if the stop was not intentional, hands any newly-held locks to a fresh generation.

`_stop_heartbeat(restart_if_held=...)` publishes the replacement generation **before** joining the old one. The old thread may be stuck in a Redis call, and making a newly acquired short lease wait for `join(timeout=2.0)` could consume its entire TTL before renewal even begins. When no replacement is needed, it joins with a 2-second timeout.

### Crash Recovery

The TTL on the Redis key is the crash recovery mechanism:

1. Writer A acquires a lock with `ttl_s=30`.
2. The heartbeat thread renews the lock every 15 seconds.
3. Writer A crashes. The heartbeat thread dies with it.
4. After at most 30 seconds, Redis expires the key.
5. Writer B can now acquire the lock.

No manual intervention or separate crash-detection process is needed.

### Process-Wide Registry

Shutdown and fork cleanup are driven by **one** module-level `atexit` handler and **one** module-level `os.register_at_fork(after_in_child=...)` handler, both registered at import time. Live instances join a `weakref.WeakSet`:

```python
_LIVE_LOCKERS: "weakref.WeakSet[RedisLocking]" = weakref.WeakSet()

_register_at_fork = getattr(os, "register_at_fork", None)
if callable(_register_at_fork):
    _register_at_fork(after_in_child=_reset_live_lockers_after_fork_in_child)
atexit.register(_release_live_lockers_at_exit)
```

This replaces per-instance registration, which leaked. Registering `self._on_exit` / `self._reset_after_fork_in_child` per instance stores a *strong* bound method — and therefore `self`, and its Redis client — in a registry that can never be unregistered. One `RedisLocking` is built per catalog and a catalog is built per write, so per-instance registration pinned every locker ever created for the life of the process. The weak registry preserves the exact same semantics while retaining nothing.

Both dispatchers swallow per-locker exceptions on purpose, to preserve the isolation the underlying mechanisms already provide: CPython runs the remaining at-fork handlers after one raises, and `atexit` isolates each registered callback. One damaged locker must not leave its siblings holding a parent lease, a dead thread primitive, or a stranded lease.

### Cleanup on Exit

```python
def _on_exit(self) -> None:
    if os.getpid() != self._owner_pid:
        return
    # Best-effort release of all held locks on interpreter shutdown
    for key, (token, _) in snapshot.items():
        self.release(key, token)
    self._stop_heartbeat()
```

Attempts to release all held locks during normal interpreter shutdown. Failures are silently ignored.

The PID guard is load-bearing: fork children inherit `atexit` handlers, and the parent's token is still live in Redis. Without the guard, a forked child exiting would compare-delete a lock its parent is actively holding.

### Cleanup After Fork

```python
def _reset_after_fork_in_child(self) -> None
```

Runs in the child after `fork()`. It re-reads `_owner_pid` and discards every piece of inherited state that the child has no right to and no working machinery for: `_held` (leases that belong to the parent), `_lost_leases`, and the thread primitives (`_held_lock`, `_lease_op_locks`, `_lease_op_locks_guard`, `_hb_stop`, `_hb_thread`) — a mutex inherited mid-hold from a thread that does not exist in the child can never be released.

### Diagnostics

Every logged failure passes through `_safe_error_type()` (`supertable.utils.diagnostic_redaction.safe_exception_type`), which yields bounded exception taxonomy and never renders backend text. Redis error strings can carry connection URIs, key material, and command payloads; lock diagnostics never reproduce them.

---

## Deadlock Prevention

The locking subsystem uses several strategies to prevent deadlocks:

1. **Fixed hierarchy**: ordinary writes acquire the per-SimpleTable lock. Structural create/delete operations acquire the namespace lock before a child table lock; whole-namespace deletion drains child locks in sorted order. Callers must preserve that order.

2. **Timeout-based acquisition**: acquisition is bounded by `timeout_s` (default 30 seconds). If a lock cannot be acquired within the timeout, `None` is returned (or `TimeoutError` is raised by the caller). This prevents indefinite blocking.

3. **TTL-based expiry**: every lock has a finite TTL. Even if a holder crashes without releasing, Redis expires the key automatically.

4. **Token-based ownership**: releases and extensions are conditioned on the caller's token matching the current holder. This prevents a slow process from accidentally releasing a lock that was already expired and re-acquired by another process.

5. **Heartbeat separation**: the heartbeat thread takes a snapshot of held locks under `_held_lock` and then performs extensions outside the lock. This prevents the heartbeat from blocking on `_held_lock` for extended periods.

6. **Per-lease operation locks**: TTL mutation is serialized per `(key, token)` rather than process-wide, and the heartbeat only ever takes those locks non-blocking. A stalled Redis call for one lease cannot stall renewal of the rest.

7. **Lock release outside `_held_lock`**: in `release()`, the decision to stop the heartbeat is made inside `_held_lock`, but `_stop_heartbeat()` is called outside the lock to avoid deadlock with the heartbeat thread (which also acquires `_held_lock`).

---

## Configuration

There is **no** locking backend setting. `RedisLocking` is selected unconditionally by `RedisCatalog`, and it is configured entirely by the Redis connection settings documented in [02 Configuration](02_configuration.md) (`SUPERTABLE_REDIS_*`).

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
token = self.catalog.acquire_stage_lock(
    org, super_name, staging_name, ttl_s=30, timeout_s=30
)
# RedisLocking renews the lease until release_stage_lock().
```

Pipe operations share the staging lock and its renewal policy:

```python
token = self.catalog.acquire_stage_lock(
    org, super_name, staging_name, ttl_s=30, timeout_s=30
)
```

### RedisLocking Defaults

| Parameter | Default | Description |
|---|---|---|
| `ttl_s` | 30 | Lock TTL in seconds |
| `timeout_s` | 30 | Maximum wait time for acquisition |
| `retry_interval` | 0.05 | Base sleep between retries (50ms) |

### Internal Tuning Constants

| Constant | Value | Description |
|---|---|---|
| `_ACQUIRE_FAST_RETRIES` | 4 | Attempts that use the exact `retry_interval` before jitter starts |
| `_LOST_LEASE_MEMORY` | 256 | Most recent proven-lost leases remembered for `lease_lost()` |

### Heartbeat Timing

The heartbeat sleeps `max(0.05, (min_ttl_ms / 1000) / 2)` seconds, adapting to the shortest held lock. With the default 30-second TTL it fires every 15 seconds, giving two renewal opportunities before expiry. A transport failure shortens the next interval to roughly a tenth of the shortest held TTL, clamped to `[0.1s, 1.0s]`.

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
        # Optional early abort for long operations:
        if locker.lease_lost("my:lock:key", token):
            raise RuntimeError("lease lost; this commit would be fenced out")
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

### Via Staging Area (RedisCatalog)

`Staging` and `SuperPipe` use the same auto-renewing `RedisLocking` owner through domain-specific `RedisCatalog` methods. This is required because a verified cloud prefix deletion can exceed the initial lease and pipe publication must share its deletion fence.

```python
token = self.catalog.acquire_stage_lock(
    org, sup, staging_name, ttl_s=30, timeout_s=30
)
try:
    # ... stage upload or verified deletion ...
    pass
finally:
    self.catalog.release_stage_lock(org, sup, staging_name, token)
```

## Durable Deletion Tombstones

Expiring locks alone cannot make fixed object-store prefixes safe. A process can lose its lease while an already-issued storage request is stalled, then resume after another process has deleted and recreated the same path. SuperTable therefore creates a no-TTL deletion intent atomically while it still owns the required locks.

Successful SimpleTable, SuperTable, and staging deletion changes that exact intent to `status=deleted` but does not remove it. Constructors and every supported publication boundary reject either an active or terminal tombstone. A different caller cannot take over an expired intent, and ordinary recreation remains blocked.

Clearing a tombstone is a deliberate recovery operation. The operator supplies the exact intent ID and `confirm_previous_owner_stopped=True`; recovery reacquires locks, repeats and verifies cleanup, atomically reaches the terminal state, and only then removes the tombstone. The confirmation means all former mutation owners are terminated and their in-flight object-store requests can no longer resume. Lease expiry by itself is not that proof.

```python
SimpleTable.recover_pending_delete(
    organization=org,
    super_name=super_name,
    simple_name=table_name,
    role_name=superadmin_role,
    intent_id=recorded_intent_id,
    confirm_previous_owner_stopped=True,
)

SuperTable.recover_pending_delete(
    organization=org,
    super_name=super_name,
    role_name=superadmin_role,
    intent_id=recorded_intent_id,
    confirm_previous_owner_stopped=True,
)
```

SimpleTable deletion covers the core `tables/{name}` prefix plus its `delta/{name}`, `iceberg/{name}`, and `parquet/{name}` projections. Whole-SuperTable cleanup preserves RBAC state and the deletion/lock keys while scanning, rechecks the owner token for every batch, and can be recovered through `SuperTable.recover_pending_delete(...)` even after the catalog root has already gone.
