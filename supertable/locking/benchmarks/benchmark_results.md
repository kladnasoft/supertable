# Locking Benchmark Results

**Date:** 2025-03-27  
**Host:** Linux (single-host, dev workstation)  
**Redis:** Sentinel mode (3 sentinels, `mymaster`, DB 1, strict mode)  
**Python:** 3.12, `.venv`

---

## Latency (no contention, 100 iterations)

Single-threaded acquire + release round-trip, no competing threads.

| Metric | File     | Redis   | Redis advantage |
|--------|----------|---------|-----------------|
| AVG    | 7.8 ms   | 2.9 ms  | 2.7×            |
| P50    | 5.0 ms   | 2.7 ms  | 1.9×            |
| P99    | 20.5 ms  | 6.6 ms  | 3.1×            |
| MIN    | 3.0 ms   | 1.5 ms  | 2.0×            |
| MAX    | 20.5 ms  | 6.6 ms  | 3.1×            |

**File backend cost breakdown:** `fcntl` flock + JSON parse + JSON serialize + `fsync` per cycle.  
**Redis backend cost breakdown:** `SET NX EX` + Lua `EVALSHA` (compare-and-delete) per cycle.

---

## Contention (12 threads, 0.5s hold)

12 threads each pick a random key from a pool of 50. Threads targeting the same key contend; others proceed concurrently.

| Metric     | File      | Redis     | Redis advantage |
|------------|-----------|-----------|-----------------|
| Threads    | 12        | 12        | —               |
| Successful | 12        | 12        | —               |
| Avg wait   | 136.0 ms  | 60.1 ms   | 2.3×            |
| Min wait   | 25.4 ms   | 6.3 ms    | 4.0×            |
| Max wait   | 601.1 ms  | 538.9 ms  | 1.1×            |

**Max wait (~500-600ms):** expected — one thread waits for the 0.5s hold to expire. This is correct contention behavior, not a performance issue.

---

## Architecture

```
supertable/locking/
├── __init__.py        # exports RedisLocking, FileLocking
├── redis_lock.py      # production backend (Sentinel-aware, heartbeat, Lua CAS)
└── file_lock.py       # dev fallback (fcntl, same API)
```

Both backends share the same API:

```python
token = locker.acquire(key, ttl_s=30, timeout_s=10)  # → str | None
locker.release(key, token)                             # → bool
locker.extend(key, token, ttl_ms)                      # → bool
```

**Heartbeat:** both backends auto-extend held locks at half-TTL via a background thread. The TTL is a crash recovery timeout, not an operation timeout. If the holder dies, the lock expires within one TTL cycle (~30s default).

**Token safety:** only the UUID token holder can release or extend. Lua scripts (Redis) and atomic file operations (file) guarantee no TOCTOU races.

---

## Connection path

`RedisLocking` receives `redis.Redis` from `RedisConnector` → `create_redis_client()`. This ensures locks participate in Sentinel discovery, SSL, password resolution, and DB selection — same path as `RedisCatalog` and all other Redis consumers.

**Env vars consumed (via `RedisOptions`):**

| Variable | Default | Purpose |
|----------|---------|---------|
| `SUPERTABLE_REDIS_URL` | — | Full connection URL (overrides split vars) |
| `SUPERTABLE_REDIS_HOST` | `localhost` | Redis host |
| `SUPERTABLE_REDIS_PORT` | `6379` | Redis port |
| `SUPERTABLE_REDIS_DB` | `0` | Redis database index |
| `SUPERTABLE_REDIS_PASSWORD` | — | Redis auth |
| `SUPERTABLE_REDIS_DECODE_RESPONSES` | `false` | Return `str` instead of `bytes` |
| `SUPERTABLE_REDIS_SENTINEL` | `false` | Enable Sentinel mode |
| `SUPERTABLE_REDIS_SENTINELS` | — | Comma-separated `host:port` pairs |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | `mymaster` | Sentinel master name |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | — | Sentinel auth (falls back to `REDIS_PASSWORD`) |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | `false` | Fail hard if Sentinel unavailable |

---

## Conclusion

Redis is **~2.7× faster** per acquire/release cycle and the correct choice for multi-host production. File backend is a viable single-host dev fallback at ~8ms avg latency.
