# Locking Package

Resource locking for single-host (file-based) and distributed (Redis-based) environments.

## Architecture

```
locking/
├── __init__.py            # Exposes Locking
├── locking.py             # Facade — selects backend, proxies API
├── locking_backend.py     # LockingBackend enum (FILE, REDIS)
├── file_lock.py           # POSIX fcntl-based file locking
├── redis_lock.py          # Redis SET NX EX with Lua scripts
├── simulations/           # Benchmarks and stress tests
└── tests/
    └── test_all_locking.py
```

## Backends

| Backend | Scope | Mechanism | Best for |
|---------|-------|-----------|----------|
| **FILE** | Single host | `fcntl.flock` + JSON lock file | Local dev, single-process |
| **REDIS** | Multi-host | `SET NX EX` + Lua atomic scripts | Production, distributed |

### Backend Selection (highest → lowest priority)

1. Explicit `backend=LockingBackend.FILE` or `backend=LockingBackend.REDIS` constructor argument
2. `LOCKING_BACKEND` setting in `supertable.defaults` (`"file"` or `"redis"`)
3. Default: **REDIS**

## API Reference

### `Locking` (facade)

```python
from supertable.locking import Locking
from supertable.locking.locking_backend import LockingBackend
```

#### Constructor

```python
lock = Locking(
    identity="worker-1",              # Required: unique identity for this lock holder
    backend=LockingBackend.FILE,      # Optional: FILE or REDIS (default: auto-detect)
    working_dir="/path/to/locks",     # Required for FILE backend
    lock_file_name=".lock.json",      # Optional: lock file name (default: .lock.json)
    check_interval=0.05,              # Optional: poll interval in seconds (default: 0.05, min: 0.01)
)
```

For Redis backend, additional kwargs are forwarded to `redis.Redis()`:
```python
lock = Locking(
    identity="worker-1",
    backend=LockingBackend.REDIS,
    host="redis.example.com",         # default: from supertable.defaults.REDIS_HOST
    port=6380,                        # default: from supertable.defaults.REDIS_PORT
    password="secret",                # default: from supertable.defaults.REDIS_PASSWORD
)
```

#### `acquire(resources, duration=30, who="") -> bool`

Acquire exclusive locks on all listed resources atomically (all-or-nothing).
Blocks with polling until acquired or a 30-second hard timeout is reached.

```python
acquired = lock.acquire(
    ["file_A", "file_B"],   # list of resource names (strings)
    duration=10,            # lock TTL in seconds (auto-refreshed by heartbeat)
    who="ingest-job-42",    # optional human-readable holder description
)
if acquired:
    try:
        # critical section
        pass
    finally:
        lock.release(["file_A", "file_B"])
```

**Parameters:**
- `resources` — iterable of resource name strings to lock
- `duration` — TTL in seconds; a background heartbeat refreshes this automatically
- `who` — optional label for observability (visible via `who()`)

**Returns:** `True` if all resources were acquired, `False` on timeout.

#### `release(resources) -> None`

Release the specified resources. Only releases locks owned by this identity.

```python
lock.release(["file_A", "file_B"])
```

**You must always pass the explicit list of resources to release.** There is no
release-all shorthand; track your acquired resources and release them explicitly.

#### `who(resources) -> dict[str, str]`

Query who holds each resource. Returns `{resource: who_str}` for held resources.
Resources not currently locked are omitted from the result.

```python
holders = lock.who(["file_A", "file_B", "file_C"])
# {"file_A": "ingest-job-42", "file_B": "ingest-job-42"}
```

#### `read_with_lock(file_path, duration=5) -> bytes | None`

Convenience helper: acquire a lock on `file_path`, read its contents, release.
Returns `None` if the lock cannot be acquired or the file does not exist.

```python
data = lock.read_with_lock("/data/config.json", duration=5)
```

### `FileLocking` (direct use)

For local scripts that don't need the facade:

```python
from supertable.locking.file_lock import FileLocking

fl = FileLocking(
    identity="my-script",
    working_dir="/tmp/locks",
    lock_file_name=".lock.json",    # optional
    check_interval=0.05,            # optional
)

if fl.acquire(["resource_1"], duration=10, who="my-script"):
    try:
        # critical section
        pass
    finally:
        fl.release(["resource_1"])
```

### `RedisLocking` (direct use)

```python
from supertable.locking.redis_lock import RedisLocking

rl = RedisLocking(
    identity="my-script",
    host="localhost",
    port=6379,
)

if rl.acquire(["resource_1"], duration=10, who="my-script"):
    try:
        # critical section
        pass
    finally:
        rl.release(["resource_1"])
```

## How It Works

### Lock Lifecycle

1. **Acquire** — tries to claim all resources atomically; retries with polling until success or 30s timeout
2. **Heartbeat** — a background daemon thread auto-refreshes TTL at `duration / 2` intervals while locks are held
3. **Release** — explicitly removes lock records for the specified resources
4. **Cleanup** — `atexit` handler releases any held locks on interpreter shutdown

### File Backend Internals

Locks are stored as JSON records in a single lock file:
```json
[
  {"res": "file_A", "exp": 1706000100, "pid": "worker-1", "who": "ingest-job"},
  {"res": "file_B", "exp": 1706000100, "pid": "worker-1", "who": "ingest-job"}
]
```

- `res` — resource name
- `exp` — unix timestamp when this lock expires (unless heartbeat refreshes it)
- `pid` — identity of the holder
- `who` — human-readable description

All reads use `fcntl.LOCK_SH`, all writes use `fcntl.LOCK_EX`.
The `acquire()` method uses `_atomic_read_write()` which holds `LOCK_EX` across the
full read-check-write cycle to prevent TOCTOU races.

Expired records are purged on every acquire attempt.

The same identity re-acquiring a resource it already holds will succeed (re-entrant).

### Redis Backend Internals

Each resource maps to a Redis key:
```
lock:{resource}    -> token (uuid4 hex)     SET NX EX
lockwho:{resource} -> who string            SET EX (best-effort sidecar)
```

- Acquire uses `SET NX EX` via pipeline for all-or-nothing semantics
- Partial acquisitions are rolled back atomically using a Lua `compare-and-delete` script
- Release uses a Lua `compare-and-delete` script (only deletes if token matches)
- Heartbeat uses a Lua `compare-and-expire` script for atomic TTL refresh

## Configuration

When using the `Locking` facade, these `supertable.defaults` settings are checked:

| Setting | Purpose | Default |
|---------|---------|---------|
| `LOCKING_BACKEND` | `"file"` or `"redis"` | `"redis"` |
| `REDIS_HOST` | Redis hostname | `"localhost"` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_DB` | Redis database number | `0` |
| `REDIS_PASSWORD` | Redis password | `None` |

## Testing

```bash
pytest supertable/locking/tests/test_all_locking.py -v
```

The test suite covers every public and internal method of `LockingBackend`,
`FileLocking`, and the `Locking` facade (74 tests total), including concurrent
stress tests with multiple threads contending for the same resource.

## Simulations

Standalone scripts in `simulations/` for benchmarking and stress testing:

| Script | Purpose |
|--------|---------|
| `measure_lock_time.py` | Measures single-thread acquire/release latency |
| `measure_lock_speed.py` | Multi-threaded contention benchmark with wait analysis |
| `concurrency_lock_simulation.py` | Heavy stress test with partial releases and invariant checks |
| `escalate_lock.py` | Demonstrates table-wide lock escalation blocking individual workers |

Run any simulation standalone:
```bash
python3 -m supertable.locking.simulations.measure_lock_speed --threads 10 --seed 42
```

## License

MIT License
