# Data Island Core — Locking

## Overview

The locking subsystem provides distributed mutual exclusion for concurrent write operations. Every `DataWriter.write()` acquires a per-table Redis lock before modifying data. This ensures that two concurrent writes to the same table never produce corrupted or interleaved snapshots — even across multiple server instances sharing the same Redis.

The lock implementation uses Redis `SET NX EX` for atomic acquisition, Lua scripts for atomic release, and a background heartbeat thread for automatic TTL renewal. This combination provides safety (only one holder at a time), liveness (locks expire on crash), and no artificial time limits (the heartbeat extends the lock for as long as the operation runs).

---

## How it works

### Acquire

```python
token = locker.acquire("supertable:acme:example:lock:orders", ttl_s=30, timeout_s=60)
```

1. Generate a random UUID token
2. Attempt `SET key token NX EX 30` (set only if key doesn't exist, expire in 30 seconds)
3. If the key already exists (another writer holds the lock), sleep for 50ms and retry
4. Repeat until success or timeout (60 seconds)
5. On success, register the lock in the held-locks dict and start the heartbeat thread (if not already running)
6. Return the token (or `None` on timeout)

### Release

```python
locker.release("supertable:acme:example:lock:orders", token)
```

Executes a Lua script on Redis that atomically checks if the key's value matches the token and deletes it:

```lua
local cur = redis.call('GET', KEYS[1])
if cur and cur == token then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
```

This ensures that only the current holder can release the lock. If the lock expired and was acquired by another writer, the release is a safe no-op.

### Heartbeat (automatic TTL renewal)

While any lock is held, a background daemon thread runs the heartbeat loop:

1. Compute the sleep interval: half the shortest TTL across all held locks
2. Sleep for that interval (interruptible via `threading.Event`)
3. For each held lock, execute the extend Lua script:
   ```lua
   if redis.call('GET', key) == token then
       redis.call('PEXPIRE', key, ttl_ms)
       return 1
   end
   return 0
   ```
4. If extend fails (lock expired or stolen), remove from tracking

This means the TTL controls **crash recovery time**, not operation duration. A write that takes 5 minutes will continuously extend its 30-second lock via heartbeat. If the process crashes, the heartbeat dies with it, and the lock expires in at most 30 seconds.

### Cleanup

On interpreter shutdown (`atexit`), the locking class makes a best-effort attempt to release all held locks. This accelerates lock recovery on graceful shutdown.

---

## Lock granularity

Locks are per-SimpleTable:

```
Key pattern: supertable:{org}:{sup}:lock:{simple_name}
```

Two concurrent writes to different tables (e.g., `orders` and `customers`) proceed in parallel — they acquire different lock keys. Writes to the same table are serialized.

Staging areas have their own locks:

```
Key pattern: supertable:{org}:{sup}:lock:stage:{staging_name}
```

These protect staging and pipe metadata from concurrent modification.

---

## File-based fallback

`locking/file_lock.py` provides a file-based locking mechanism as a fallback for environments without Redis. It uses `fcntl.flock()` on Unix systems. This is used only in edge cases — the Redis lock is the primary mechanism.

File locks are local to a single machine and do not provide distributed mutual exclusion. They are suitable for single-process, single-machine deployments only.

---

## Deadlock prevention

Deadlocks are prevented by design:

1. **Single lock per operation**: each `DataWriter.write()` acquires exactly one lock (the target table's lock). There is no lock ordering problem because no operation holds two locks simultaneously.
2. **Timeout on acquisition**: `acquire()` has a configurable timeout (default: 60 seconds). If a lock cannot be acquired, `TimeoutError` is raised rather than waiting indefinitely.
3. **TTL expiry**: even if a holder hangs (but doesn't crash), the heartbeat continues extending. If Redis itself restarts, all locks are lost and writers can proceed.
4. **No lock promotion**: the system never upgrades a read lock to a write lock. There are no read locks — reads are lock-free.

---

## Configuration

| Setting | Default | Description |
|---|---|---|
| `DEFAULT_LOCK_DURATION_SEC` | `30` | Lock TTL (seconds). Controls crash recovery time. |
| `DEFAULT_TIMEOUT_SEC` | `60` | Lock acquisition timeout (seconds). |
| `LOCKING_BACKEND` | `redis` | Lock backend: `redis` or `file`. |

---

## Module structure

```
supertable/locking/
  __init__.py        Package marker
  redis_lock.py      RedisLocking — SET NX, Lua release/extend, heartbeat (280 lines)
  file_lock.py       FileLocking — fcntl.flock fallback (130 lines)
```

---

## Frequently asked questions

**What happens if Redis goes down while a lock is held?**
The lock is effectively released — the key no longer exists. When Redis recovers, the writer's heartbeat extend will fail (key missing), and the writer logs a warning. The next writer can acquire the lock normally.

**Can two server instances deadlock each other?**
No. Each write acquires exactly one lock. There is no scenario where instance A holds lock X and waits for lock Y while instance B holds lock Y and waits for lock X — because no operation requires two locks.

**What if a write takes longer than the TTL?**
The heartbeat thread extends the lock automatically at half the TTL interval. A 30-second TTL with a write that takes 10 minutes results in ~40 heartbeat extensions — the lock is never lost while the holder is alive.

**Can I increase the TTL for slow writes?**
Yes. Set `DEFAULT_LOCK_DURATION_SEC` to a higher value. However, this increases crash recovery time — a higher TTL means other writers wait longer when a holder crashes. The heartbeat makes this trade-off less important than it seems: the TTL only matters for crash recovery.

**How do I debug lock contention?**
Check Redis directly: `redis-cli GET "supertable:acme:example:lock:orders"`. A non-empty value means the lock is held. The value is the holder's UUID token. Check server logs for `[redis-lock]` entries showing acquire/release/extend events.
