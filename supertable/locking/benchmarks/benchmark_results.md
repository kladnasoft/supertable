# Locking Benchmark Results

**Date:** 2025-03-27  
**Host:** Linux (single-host, dev workstation)  
**Redis:** Sentinel mode (3 sentinels, `mymaster`, DB 1, strict mode)  
**Python:** 3.12, `.venv`

`RedisLocking` is the only locking backend. These numbers are a baseline for
the lock primitive itself, not a comparison between alternatives.

---

## Latency (no contention, 100 iterations)

Single-threaded acquire + release round-trip, no competing threads.
Reproduce with `python3 measure_lock_time.py --iterations 100`.

| Metric | Redis   |
|--------|---------|
| AVG    | 2.9 ms  |
| P50    | 2.7 ms  |
| P99    | 6.6 ms  |
| MIN    | 1.5 ms  |
| MAX    | 6.6 ms  |

**Cost breakdown:** `SET NX EX` + Lua `EVALSHA` (compare-and-delete) per cycle.

---

## Contention (12 threads, 0.5s hold)

12 threads each pick a random key from a pool of 50. Threads targeting the same
key contend; others proceed concurrently. Reproduce with
`python3 measure_lock_speed.py --threads 12 --hold 0.5`.

| Metric     | Redis     |
|------------|-----------|
| Threads    | 12        |
| Successful | 12        |
| Avg wait   | 60.1 ms   |
| Min wait   | 6.3 ms    |
| Max wait   | 538.9 ms  |

**Max wait (~500ms):** expected — one thread waits for the 0.5s hold to expire.
This is correct contention behavior, not a performance issue.

Waiters poll rather than queue, so `acquire()` applies mean-preserving jitter
after the first few attempts to decorrelate the retry herd. That keeps the mean
poll rate identical while spreading wake-ups; it does not make the lock fair.

---

## Architecture

```
supertable/locking/
├── __init__.py        # exports RedisLocking
└── redis_lock.py      # the only backend (Sentinel-aware, heartbeat, Lua CAS)
```

```python
token = locker.acquire(key, ttl_s=30, timeout_s=10)  # → str | None
locker.release(key, token)                             # → bool
locker.extend(key, token, ttl_ms)                      # → bool
locker.lease_lost(key, token)                          # → bool
```

**Heartbeat:** a background thread auto-extends held locks at half the shortest
held TTL, batching all renewals into one Lua round trip. The TTL is a crash
recovery timeout, not an operation timeout. If the holder dies, the lock expires
within one TTL cycle (~30s default).

**Token safety:** only the UUID token holder can release or extend; the Lua
compare-and-delete / compare-and-extend scripts make that check atomic.

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
