# Feature / Component Reverse Engineering — `supertable.locking`

## 1. Executive Summary

The `supertable.locking` package implements a **dual-backend distributed locking subsystem** for the Supertable product. It provides two interchangeable lock backends behind a unified API:

- **`RedisLocking`** — production-grade distributed lock using Redis `SET NX EX`, Lua CAS scripts, and Sentinel-aware connection reuse. Designed for multi-host deployments.
- **`FileLocking`** — POSIX `fcntl`-based file lock for single-host development environments. Uses a JSON lock-file with atomic read-modify-write cycles.

Both backends expose the same three-method contract (`acquire`, `release`, `extend`) and include automatic **heartbeat-based TTL renewal** via background daemon threads, ensuring long-running operations never lose their locks while still providing fast crash recovery (lock expires within one TTL cycle if the holder process dies).

**Business purpose:** Mutual exclusion for concurrent access to shared resources (tables, catalog entries, compaction targets, etc.) within the Supertable data platform. Without this subsystem, concurrent writes or maintenance operations could corrupt data or produce inconsistent state.

**Technical role:** Infrastructure-layer locking primitive consumed by higher-level Supertable services (catalog operations, compaction, ingestion, schema mutations). Sits below domain logic and above the storage/connection layer.

---

## 2. Functional Overview

### Product Capability

The Supertable platform can safely coordinate concurrent access to shared resources — even across multiple hosts — without data corruption or lost updates. This includes scenarios such as:

- Concurrent table writes from multiple ingestion workers
- Catalog metadata mutations during schema evolution
- Compaction or optimization tasks that must run exclusively
- Any operation where exactly-once or at-most-once semantics are required

### Target Actors

| Actor | Relationship |
|---|---|
| Supertable application services | Primary consumers — acquire locks before mutating shared resources |
| Platform operators | Configure backend choice (Redis vs file), TTL, Redis topology |
| Developers | Use file backend locally with zero Redis dependency |

### Business Workflows Enabled

1. **Safe concurrent ingestion** — multiple writers can target the same table; locking serializes conflicting operations.
2. **Exclusive maintenance operations** — compaction, vacuum, or rewrite tasks acquire a lock to prevent overlapping execution.
3. **Catalog consistency** — schema changes or metadata updates are protected against concurrent modification.
4. **Dev/prod parity** — developers use the same locking API locally (file backend) as in production (Redis backend), reducing environment-specific bugs.

### Strategic Significance

- **Core infrastructure** — almost every write-path or mutation-path in a data platform requires coordination. This subsystem is foundational.
- **Operational safety** — prevents data corruption, the most damaging class of bug in a data product.
- **Backend flexibility** — the pluggable design reduces operational burden (no Redis required for local dev) and supports future backend additions (e.g., etcd, ZooKeeper) without API changes.

---

## 3. Technical Overview

### Architectural Style

- **Strategy pattern**: two backend implementations behind an identical public API, selected at construction time by the caller.
- **Token-based ownership**: every lock acquisition returns a UUID token; only the token holder can release or extend. Prevents accidental release by a different caller.
- **Optimistic retry with timeout**: acquisition loops with configurable sleep interval until a deadline, returning `None` on timeout.
- **Background heartbeat**: a single daemon thread per `Locking` instance extends all held locks at half-TTL intervals, decoupling operation duration from lock lifetime.
- **Graceful shutdown**: `atexit` handlers release all held locks on interpreter exit.

### Major Components

| Component | Role |
|---|---|
| `RedisLocking` | Production distributed lock via Redis SET NX / Lua CAS |
| `FileLocking` | Dev single-host lock via POSIX `fcntl` + JSON file |
| `__init__.py` | Package facade exporting both backends |
| Lua scripts (`_LUA_RELEASE_IF_TOKEN`, `_LUA_EXTEND_IF_TOKEN`) | Atomic server-side compare-and-delete / compare-and-extend |
| Heartbeat thread (`_hb_loop`) | Background TTL renewal for held locks |

### Key Control Flow

1. Caller constructs a backend (`RedisLocking(redis_client)` or `FileLocking(working_dir)`).
2. Caller invokes `acquire(key, ttl_s, timeout_s)` → receives a token or `None`.
3. While lock is held, heartbeat thread extends TTL at half-TTL intervals.
4. Caller invokes `release(key, token)` → lock record removed atomically.
5. On process exit, `atexit` handler releases any remaining locks.

### Key Data Flow

- **Redis backend**: lock state lives in Redis keys; values are UUID tokens with server-managed expiry.
- **File backend**: lock state lives in a JSON file (`.lock.json`); records contain `{res, exp, tok}` dicts.

### Design Patterns

- Strategy / pluggable backend
- Token-based fencing
- Compare-and-swap (CAS) via Lua scripts (Redis) and atomic file I/O (file)
- Half-TTL heartbeat renewal
- Graceful degradation (`atexit` cleanup)
- TOCTOU prevention (exclusive file locks, atomic Redis operations)

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes / Functions | Relevance |
|---|---|---|---|
| `redis_lock.py` | Production distributed locking via Redis | `RedisLocking`, Lua scripts `_LUA_RELEASE_IF_TOKEN`, `_LUA_EXTEND_IF_TOKEN` | Core production backend |
| `file_lock.py` | Single-host dev locking via POSIX fcntl | `FileLocking`, `_atomic_read_write`, `_read_file`, `_purge_expired` | Dev/test fallback backend |
| `__init__.py` | Package facade | Exports `RedisLocking`, `FileLocking` | API surface definition |
| `benchmark_results.md` | Performance characterization | Latency and contention benchmarks | Operational evidence and design validation |

---

## 5. Detailed Functional Capabilities

### 5.1 Lock Acquisition

- **Description:** Attempt to exclusively acquire a named lock with retry-until-timeout semantics.
- **Business purpose:** Prevent concurrent mutation of the same resource.
- **Trigger/input:** `acquire(key, ttl_s=30, timeout_s=30, retry_interval=...)` called by any service needing mutual exclusion.
- **Processing behavior:**
  - Generates a UUID4 hex token.
  - Loops until deadline: attempts atomic lock creation (Redis `SET NX EX` / file atomic append); on conflict, sleeps `retry_interval` and retries.
  - On success, registers the lock in `_held` dict and starts heartbeat thread if not running.
- **Output/result:** `str` token on success, `None` on timeout.
- **Dependencies:** Redis client (Redis backend), filesystem + `fcntl` (file backend).
- **Constraints/assumptions:** Key string uniqueness maps 1:1 to the protected resource. TTL must be ≥ 1 second.
- **Risks/edge cases:**
  - Clock skew between hosts (Redis backend mitigated by server-side expiry; file backend vulnerable if NFS-mounted — but explicitly designed for single-host only).
  - Starvation under heavy contention (no fairness guarantee — purely optimistic retry).
  - File backend JSON corruption on OS crash mid-write (mitigated by `fsync`, but not journaled).
- **Confidence:** Explicit.

### 5.2 Lock Release

- **Description:** Atomically release a lock only if the caller still holds it (token match).
- **Business purpose:** Free the resource for other consumers; prevent accidental release by a non-holder.
- **Trigger/input:** `release(key, token)`.
- **Processing behavior:**
  - Redis: executes Lua `_LUA_RELEASE_IF_TOKEN` (atomic GET + compare + DEL).
  - File: `_atomic_read_write` removes matching record under `LOCK_EX`.
  - Removes key from `_held` tracking. Stops heartbeat if no locks remain.
- **Output/result:** `bool` — `True` if released, `False` if token mismatch or error.
- **Risks/edge cases:** Redis backend always removes from `_held` even if Lua returns 0 (lock may have expired — correct defensive behavior).
- **Confidence:** Explicit.

### 5.3 Lock Extension (Manual)

- **Description:** Manually extend the TTL of a held lock.
- **Business purpose:** Allow callers to explicitly extend locks beyond heartbeat behavior if needed.
- **Trigger/input:** `extend(key, token, ttl_ms)`.
- **Processing behavior:**
  - Redis: Lua `_LUA_EXTEND_IF_TOKEN` (atomic GET + compare + PEXPIRE).
  - File: `_atomic_read_write` updates `exp` field for matching record.
- **Output/result:** `bool`.
- **Note:** `ttl_ms` is in milliseconds for both backends. File backend converts to seconds (`ttl_ms // 1000`).
- **Confidence:** Explicit.

### 5.4 Heartbeat-Based Auto-Renewal

- **Description:** Background daemon thread extends all held locks at half their TTL interval.
- **Business purpose:** Decouples operation duration from lock lifetime — operations of any length are safe. TTL becomes a crash-recovery parameter, not an operation timeout.
- **Trigger/input:** Started automatically on first `acquire`; stopped when all locks released or on exit.
- **Processing behavior:**
  - Sleeps for `max(1, min_ttl / 2)` seconds (Redis uses minimum TTL across all held locks; file uses the single `_hb_ttl_s` value).
  - On wake, snapshots `_held` under lock, then extends each lock outside the lock.
  - Redis backend detects lost locks (extend returns `False`) and removes them from tracking.
- **Risks/edge cases:**
  - File backend heartbeat does not detect lost locks (no explicit check on extend failure for removal from `_held`).
  - Redis backend's adaptive interval (shortest TTL) is more robust than file backend's single `_hb_ttl_s`.
- **Confidence:** Explicit.

### 5.5 Lock Ownership Query (`who`)

- **Description:** Returns the token currently holding a given key. File backend only.
- **Business purpose:** Diagnostic / introspection — allows checking who holds a lock without attempting acquisition.
- **Trigger/input:** `who(key)`.
- **Output/result:** `str` token or `None`.
- **Note:** Not present on `RedisLocking`. API asymmetry — minor deviation from stated interchangeability.
- **Confidence:** Explicit.

### 5.6 Graceful Shutdown

- **Description:** `atexit` handler releases all held locks and stops the heartbeat thread on interpreter exit.
- **Business purpose:** Prevents lock leakage when a process exits normally (non-crash). Locks are released immediately rather than waiting for TTL expiry.
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 `RedisLocking` (redis_lock.py)

| Member | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `__init__` | method | Initialize with Redis client; register Lua scripts | `r: redis.Redis` | — | Registers Lua scripts, registers `atexit` handler | Critical |
| `acquire` | method | Acquire lock with retry | `key, ttl_s=30, timeout_s=30, retry_interval=0.05` | `Optional[str]` | Writes Redis key; starts heartbeat thread; updates `_held` | Critical |
| `release` | method | Release lock atomically (Lua CAS) | `key, token` | `bool` | Deletes Redis key; updates `_held`; may stop heartbeat | Critical |
| `extend` | method | Extend TTL atomically (Lua CAS) | `key, token, ttl_ms` | `bool` | Updates Redis key expiry | Important |
| `_start_heartbeat` | method | Launch daemon heartbeat thread | — | — | Creates and starts thread | Important |
| `_stop_heartbeat` | method | Signal heartbeat to stop and join | — | — | Joins thread | Important |
| `_hb_loop` | method | Heartbeat main loop: extend all held locks at half-TTL | — | — | Extends Redis keys; removes lost locks from `_held` | Critical |
| `_on_exit` | method | atexit handler: release all, stop heartbeat | — | — | Best-effort cleanup | Supporting |

**Lua Scripts (module-level):**

| Name | Purpose | Atomicity Guarantee |
|---|---|---|
| `_LUA_RELEASE_IF_TOKEN` | Compare token, if match DELETE key | Server-side atomic (single Redis command) |
| `_LUA_EXTEND_IF_TOKEN` | Compare token, if match PEXPIRE key | Server-side atomic (single Redis command) |

### 6.2 `FileLocking` (file_lock.py)

| Member | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `__init__` | method | Initialize with working dir; create lock file path | `working_dir, lock_file_name=".lock.json", retry_interval=0.1` | — | Creates directory; registers `atexit` handler | Critical |
| `acquire` | method | Acquire lock with retry (file CAS) | `key, ttl_s=30, timeout_s=30, retry_interval=None` | `Optional[str]` | Writes to lock file; starts heartbeat; updates `_held` | Critical |
| `release` | method | Release lock atomically (file CAS) | `key, token` | `bool` | Removes record from lock file; updates `_held`; may stop heartbeat | Critical |
| `extend` | method | Extend TTL atomically (file CAS) | `key, token, ttl_ms` | `bool` | Updates `exp` in lock file | Important |
| `who` | method | Query current lock holder | `key` | `Optional[str]` | Read-only | Supporting |
| `_atomic_read_write` | method | Open file under `LOCK_EX`, apply callback, write back, `fsync` | `callback` | `List[Dict]` | File I/O with exclusive lock | Critical |
| `_read_file` | method | Read records under `LOCK_SH` | — | `List[Dict]` | Read-only file I/O | Supporting |
| `_purge_expired` | static method | Filter out expired records by comparing `exp` to `time.time()` | `records` | `List[Dict]` | None | Important |
| `_hb_loop` | method | Heartbeat main loop: refresh all held locks | — | — | Writes to lock file | Critical |
| `_on_exit` | method | atexit handler | — | — | Best-effort cleanup | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Lock Acquisition → Operation → Release (Primary Workflow)

1. **Caller invokes** `locker.acquire("my:resource:key", ttl_s=30, timeout_s=10)`.
2. **Token generated** — `uuid4().hex`.
3. **Atomic attempt** — Redis: `SET key token NX EX 30`; File: append record under `LOCK_EX`.
4. **On conflict** — sleep `retry_interval`, retry until deadline.
5. **On success** — token stored in `_held`; heartbeat thread started (if not running).
6. **Heartbeat runs** — every `ttl_s / 2` seconds, extends all held locks.
7. **Caller performs operation** — arbitrary duration; heartbeat keeps lock alive.
8. **Caller invokes** `locker.release("my:resource:key", token)`.
9. **Atomic release** — Redis: Lua compare-and-delete; File: remove record under `LOCK_EX`.
10. **Tracking updated** — key removed from `_held`; heartbeat stopped if no locks remain.

**Failure modes:**
- **Timeout on acquire** — returns `None`; caller must handle (retry, abort, or queue).
- **Process crash during hold** — heartbeat dies; lock expires after one TTL cycle (~30s default).
- **Redis/file I/O error** — logged at debug level; acquire retries; release returns `False`.
- **Lock stolen (Redis heartbeat detects)** — removed from `_held`; logged.

### 7.2 Heartbeat Renewal Loop

1. **Thread wakes** after `max(1, min_held_ttl / 2)` seconds.
2. **Snapshot** `_held` under `_held_lock`.
3. **For each held lock** — call `extend(key, token, ttl_ms)`.
4. **If extend fails (Redis)** — lock lost; remove from `_held`, log warning.
5. **If `_held` is empty** — thread exits.
6. **If `_hb_stop` signaled** — thread exits immediately.

### 7.3 Graceful Shutdown

1. **Interpreter exit triggers** `atexit` → `_on_exit()`.
2. **Snapshot** `_held`.
3. **For each held lock** — call `release(key, token)` (best-effort, exceptions swallowed).
4. **Stop heartbeat thread.**

---

## 8. Data Model and Information Flow

### Lock Record (File Backend)

```json
{"res": "<key>", "exp": <unix_timestamp_seconds>, "tok": "<uuid_hex>"}
```

- `res` — resource key (the protected entity identifier)
- `exp` — expiration time as Unix epoch seconds
- `tok` — UUID4 hex token identifying the holder

**Stored as:** JSON array in `.lock.json` file at `{working_dir}/.lock.json`.

### Lock Record (Redis Backend)

- **Key:** the resource key string directly (e.g., `"my:lock:key"`)
- **Value:** UUID4 hex token
- **Expiry:** managed by Redis `EX` (seconds) and `PEXPIRE` (milliseconds)

No separate data structure — Redis key-value with native TTL.

### In-Memory Tracking (`_held`)

| Backend | Structure | Content |
|---|---|---|
| Redis | `Dict[str, Tuple[str, int]]` | `key → (token, ttl_ms)` |
| File | `Dict[str, str]` | `key → token` |

**Note:** File backend does not track `ttl_ms` per lock in `_held`; it uses a single `_hb_ttl_s` for heartbeat interval. This is a simplification acceptable for single-host dev use.

### Information Flow

```
Caller → acquire(key) → [Redis SET NX / File atomic write] → token
Caller → release(key, token) → [Redis Lua DEL / File atomic remove] → bool
Heartbeat → extend(key, token, ttl_ms) → [Redis Lua PEXPIRE / File atomic update] → bool
```

State is authoritative in the storage backend (Redis or file). The `_held` dict is a local cache for the heartbeat thread and cleanup logic.

---

## 9. Dependencies and Integrations

### External Dependencies

| Dependency | Used By | Purpose |
|---|---|---|
| `redis` (Python package) | `RedisLocking` | Redis client for SET, Lua script execution |
| `fcntl` (stdlib, POSIX) | `FileLocking` | File-level exclusive/shared locking |
| `json` (stdlib) | `FileLocking` | Lock file serialization |
| `threading` (stdlib) | Both | Heartbeat thread, `_held_lock` mutex |
| `uuid` (stdlib) | Both | Token generation |
| `atexit` (stdlib) | Both | Graceful shutdown registration |
| `os` (stdlib) | `FileLocking` | Directory creation, `fsync` |
| `time` (stdlib) | Both | Deadlines, expiry calculation |

### Internal Dependencies

| Dependency | Purpose |
|---|---|
| `supertable.config.defaults.logger` | Debug-level logging for lock operations |
| `supertable.redis_connector.create_redis_client` | Creates the `redis.Redis` instance passed to `RedisLocking` (documented in benchmark, not imported directly) |
| `supertable.locking.__init__` | Package facade exporting both backends |

### Integration Points

| Integration | Direction | Description |
|---|---|---|
| Redis (Sentinel-aware) | Outbound | `RedisLocking` receives a pre-configured `redis.Redis` client, participating in the application's Sentinel/SSL/password/DB topology |
| Filesystem | Outbound | `FileLocking` reads/writes `.lock.json` in a configured working directory |
| Caller services | Inbound | Any Supertable service needing mutual exclusion imports and uses either backend |

### Configuration / Environment Variables (from benchmark doc)

| Variable | Default | Purpose |
|---|---|---|
| `SUPERTABLE_REDIS_URL` | — | Full Redis connection URL |
| `SUPERTABLE_REDIS_HOST` | `localhost` | Redis host |
| `SUPERTABLE_REDIS_PORT` | `6379` | Redis port |
| `SUPERTABLE_REDIS_DB` | `0` | Redis DB index |
| `SUPERTABLE_REDIS_PASSWORD` | — | Redis auth |
| `SUPERTABLE_REDIS_DECODE_RESPONSES` | `false` | String vs bytes |
| `SUPERTABLE_REDIS_SENTINEL` | `false` | Enable Sentinel |
| `SUPERTABLE_REDIS_SENTINELS` | — | Sentinel endpoints |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | `mymaster` | Master name |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | — | Sentinel auth |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | `false` | Fail if Sentinel unavailable |

**Note:** These variables are consumed by `RedisOptions` / `RedisConnector` (not in this package), which produces the `redis.Redis` client injected into `RedisLocking`.

---

## 10. Architecture Positioning

### System Layer

```
┌──────────────────────────────────────────────┐
│  Domain Services (Catalog, Ingestion, etc.)  │  ← consumers
├──────────────────────────────────────────────┤
│  supertable.locking  (this package)          │  ← coordination layer
├──────────────────────────────────────────────┤
│  Redis / Filesystem                          │  ← storage backends
└──────────────────────────────────────────────┘
```

- **Layer:** Infrastructure / coordination primitive.
- **Upstream callers:** Any Supertable service requiring mutual exclusion — likely `RedisCatalog`, compaction workers, ingestion pipelines, schema mutation handlers.
- **Downstream:** Redis (via injected client) or local filesystem.
- **Boundary:** Clean — no domain knowledge in the locking package. Pure resource-key-based locking.

### Coupling

- **Low coupling to Redis internals** — receives a `redis.Redis` client; does not construct connections.
- **No coupling to domain model** — keys are opaque strings.
- **Backend selection** is the caller's responsibility — the package does not contain a factory or config-driven selector (likely exists in a higher-level wiring module).

### Scalability Significance

- Redis backend supports **multi-host distributed locking** — essential for horizontal scaling of Supertable workers.
- File backend is explicitly **single-host only** — appropriate for development but not production.
- Heartbeat mechanism prevents lock starvation during long operations, enabling safe scale-out of worker processes.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Preventing data corruption and inconsistent state during concurrent access to shared resources in a data platform |
| **Why it matters** | Data corruption is catastrophic for a data product — it destroys user trust and can cause data loss |
| **Category** | Foundational infrastructure — enables every write-path feature to be safe |
| **Revenue impact** | Indirect but critical — without safe concurrency, the product cannot scale to multi-user or multi-worker scenarios |
| **What would be lost** | Concurrent writes would race, producing corrupted tables, inconsistent catalogs, or duplicate work |
| **Differentiating vs commodity** | The dual-backend design with heartbeat auto-renewal and token-based fencing is a well-engineered implementation of a standard pattern. The dev/prod parity via API-compatible backends is a quality-of-life differentiator for the development team. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate — standard distributed locking patterns, but well-implemented with attention to edge cases |
| **Architectural maturity** | High — clean separation of concerns, pluggable backends, no domain leakage |
| **Maintainability** | Good — small focused classes, clear docstrings, consistent API across backends |
| **Extensibility** | Good — new backends (etcd, ZooKeeper, DynamoDB) could be added by implementing the same 3-method API |
| **Operational sensitivity** | High — locking failures can cause data corruption or deadlocks; debug-level logging may be insufficient for production alerting |
| **Performance** | Redis: ~2.9ms avg acquire+release; File: ~7.8ms avg. Both acceptable. Redis ~2.7× faster (benchmark data). |
| **Reliability** | Good — TTL-based crash recovery, heartbeat renewal, atexit cleanup. File backend vulnerable to corruption on hard OS crash despite `fsync`. |
| **Security** | Token-based ownership prevents unauthorized release. No authentication at the locking layer — delegated to Redis auth and filesystem permissions. |
| **Testing clues** | Benchmark file implies a test harness exists. No test files provided. |

### Notable Implementation Details

- **Lua scripts** for Redis CAS eliminate round-trip race conditions — production-quality pattern.
- **`fcntl.LOCK_EX`** with `a+` mode avoids TOCTOU between file creation and locking — defensive and correct.
- **`os.fsync`** after write ensures durability on the file backend.
- **Heartbeat adaptive interval** (Redis) uses the minimum TTL across all held locks — prevents under-renewal when locks have different TTLs.
- **File backend heartbeat** uses a single `_hb_ttl_s` — simpler but less correct when locks have different TTLs. Acceptable trade-off for dev-only use.

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Both backends expose `acquire(key, ttl_s, timeout_s) → token | None`, `release(key, token) → bool`, `extend(key, token, ttl_ms) → bool`.
- Token-based ownership via UUID4 hex.
- Heartbeat auto-renewal at half-TTL via daemon thread.
- Redis backend uses Lua scripts for atomic CAS.
- File backend uses `fcntl.LOCK_EX` for atomic file operations.
- `atexit` cleanup registered in both backends.
- `RedisLocking` does not create its own Redis connection.
- `FileLocking` creates directory and lock file as needed.
- Expired records purged during file backend operations.
- Redis heartbeat detects and removes lost locks; file heartbeat does not.
- `who()` method exists only on `FileLocking`.

### Reasonable Inferences

- **Backend selection** is performed by a higher-level factory or configuration module (not present in this package). *Evidence: `__init__.py` exports both; no selector logic.*
- **`RedisCatalog`** and other Redis consumers share the same connection topology. *Evidence: benchmark doc states "same path as RedisCatalog and all other Redis consumers."*
- **Resource keys** likely follow a namespaced convention (e.g., `"table:<id>:lock"`, `"catalog:mutation"`). *Evidence: benchmark mentions "pool of 50" keys; Redis key format supports colons.*
- **The subsystem is consumed by catalog, ingestion, and compaction services.** *Evidence: locking is a foundational primitive; benchmark doc confirms Sentinel production topology.*
- **A `RedisConnector` / `create_redis_client()` module exists** that handles connection construction. *Evidence: benchmark doc and redis_lock.py docstring reference it.*

### Unknown / Not Visible in Provided Snippet

- Exact callers and lock key naming conventions.
- Backend selection mechanism (config key, environment variable, or factory).
- Whether a higher-level context manager wraps acquire/release.
- Error escalation policy — all errors are logged at `debug` level; unclear if higher-level code retries or alerts.
- Whether `RedisLocking` is used with Redlock (multi-master) or single-master semantics only.
- Test suite structure and coverage.
- Metrics/observability beyond debug logging (no counters, histograms, or tracing visible).
- Whether `who()` being absent from `RedisLocking` is intentional or an oversight.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `supertable.locking` — Distributed Locking Subsystem
- **Alternative names/aliases:** `RedisLocking`, `FileLocking`, `redis_lock`, `file_lock`, lock backend, distributed lock
- **Main responsibilities:** Mutual exclusion for shared resource access; token-based lock ownership; automatic heartbeat renewal; crash recovery via TTL expiry
- **Important entities:** Lock key (string), lock token (UUID4 hex), TTL (seconds/milliseconds), lock record (`{res, exp, tok}`)
- **Important workflows:** acquire → heartbeat → release; acquire → crash → TTL expiry; atexit cleanup
- **Integration points:** `redis.Redis` client (injected), `supertable.config.defaults.logger`, filesystem (`.lock.json`), `supertable.redis_connector.create_redis_client` (upstream)
- **Business purpose keywords:** concurrency control, mutual exclusion, data integrity, crash recovery, distributed coordination
- **Architecture keywords:** strategy pattern, token fencing, CAS, Lua scripting, heartbeat renewal, pluggable backend, POSIX fcntl, infrastructure primitive
- **Follow-up files for completeness:**
  - `supertable.redis_connector` — Redis client construction and Sentinel configuration
  - `supertable.config.defaults` — logger and default configuration values
  - Any module that imports `RedisLocking` or `FileLocking` — to map actual callers and key conventions
  - `supertable.locking` test files — to validate behavior assumptions
  - Factory or configuration module that selects the locking backend

---

## 15. Suggested Documentation Tags

`distributed-locking`, `concurrency-control`, `mutual-exclusion`, `redis`, `fcntl`, `posix`, `heartbeat-renewal`, `token-fencing`, `compare-and-swap`, `lua-scripting`, `infrastructure`, `coordination-primitive`, `crash-recovery`, `ttl-based-expiry`, `pluggable-backend`, `strategy-pattern`, `dev-prod-parity`, `sentinel-aware`, `data-integrity`, `supertable-platform`, `background-thread`, `atexit-cleanup`

---

## Appendix A — Benchmark Data

Extracted from `benchmark_results.md` (2025-03-27, Linux dev workstation, Redis Sentinel mode).

### Latency (No Contention, 100 iterations)

Single-threaded acquire + release round-trip.

| Metric | File | Redis | Redis Advantage |
|---|---|---|---|
| AVG | 7.8 ms | 2.9 ms | 2.7× |
| P50 | 5.0 ms | 2.7 ms | 1.9× |
| P99 | 20.5 ms | 6.6 ms | 3.1× |
| MIN | 3.0 ms | 1.5 ms | 2.0× |
| MAX | 20.5 ms | 6.6 ms | 3.1× |

**File cost profile:** `fcntl` flock → JSON parse → JSON serialize → `fsync` per cycle.
**Redis cost profile:** `SET NX EX` → Lua `EVALSHA` (compare-and-delete) per cycle.

### Contention (12 Threads, 0.5s Hold, 50-Key Pool)

| Metric | File | Redis | Redis Advantage |
|---|---|---|---|
| Successful acquisitions | 12/12 | 12/12 | — |
| Avg wait | 136.0 ms | 60.1 ms | 2.3× |
| Min wait | 25.4 ms | 6.3 ms | 4.0× |
| Max wait | 601.1 ms | 538.9 ms | 1.1× |

**Max wait (~500–600ms)** is expected: one thread waits for a 0.5s hold to expire. This validates correct contention behavior.

### Key Takeaway

Redis is ~2.7× faster per cycle and the correct choice for multi-host production. File backend is viable for single-host development at ~8ms avg latency with no external dependencies.

---

## Merge Readiness

- **Suggested canonical name:** `supertable.locking` — Distributed Locking Subsystem
- **Standalone or partial:** Standalone package — fully self-contained coordination primitive. However, understanding its role in the product requires analyzing its callers.
- **Related components for later merge:**
  - `supertable.redis_connector` / `RedisConnector` / `create_redis_client` — connection management
  - `supertable.config.defaults` — logger and configuration
  - `supertable.catalog` (likely `RedisCatalog`) — probable major consumer
  - Compaction / ingestion workers — probable consumers
  - Backend selection / factory module — how the product chooses Redis vs file
- **What would most improve certainty:**
  - Callers of `acquire()` — to map protected resources and key naming conventions
  - Backend selection logic — to understand production vs dev switching
  - Test files — to validate edge case handling
- **Recommended merge key phrases:** `distributed locking`, `RedisLocking`, `FileLocking`, `supertable.locking`, `lock acquire release extend`, `heartbeat renewal`, `token fencing`
