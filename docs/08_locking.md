# Locking

Normal table writes coordinate through Redis even when the data backend is local. The catalog owns a `RedisLocking` instance and exposes locks for simple tables and staging areas. `FileLocking` is also exported as a standalone implementation, but the table writer does not select it based on storage type.

Sources: [Redis locks](../supertable/locking/redis_lock.py), [file locks](../supertable/locking/file_lock.py), [catalog wrappers](../supertable/redis_catalog.py), [writer](../supertable/data_writer.py).

## Acquire, verify, and release a table lock

```python
from supertable.redis_catalog import RedisCatalog

catalog = RedisCatalog()
token = catalog.acquire_simple_lock(
    "acme", "sales", "orders", ttl_s=30, timeout_s=60
)
if token is None:
    raise TimeoutError("The table lock could not be acquired")

try:
    if not catalog.verify_simple_lock("acme", "sales", "orders", token):
        raise RuntimeError("The table lock was lost")
    # Perform the operation protected by this lock.
finally:
    catalog.release_simple_lock("acme", "sales", "orders", token)
```

Each acquisition creates a fresh UUID-hex token. Redis `SET key token NX EX ttl` succeeds only while the key is absent. A held lock is identified by both its key and token; a second acquire call is not reentrant.

| Method | Result and defaults |
| --- | --- |
| `acquire(key, ttl_s=30, timeout_s=30, retry_interval=0.05)` | Token on success; `None` after the retry deadline |
| `is_held(key, token)` | True only if Redis holds that token and it has not been recorded as lost |
| `release(key, token)` | True only when a Lua comparison deletes the matching token |
| `extend(key, token, ttl_ms)` | True only when a Lua comparison renews the matching token with `PEXPIRE` |

TTL and acquire timeout are converted to whole seconds and clamped to at least one second. Acquisition retries Redis errors until its deadline. The retry loop uses wall-clock time. `is_held()` returns false on a Redis error. Release and extension return false on Redis errors.

Releasing an expired token cannot delete a new owner's token: comparison and deletion run inside one Lua invocation. Extension applies the same token comparison before changing expiry.

## Automatic renewal and lost ownership

After the first successful acquisition, the lock manager starts a daemon heartbeat thread. It tracks each held key with its token and TTL in milliseconds. It renews all held entries every half of the smallest TTL, with a minimum interval of one second.

When renewal returns false, the manager removes that token from its held set and records it as lost. Up to 1,024 lost `(key, token)` pairs are retained. Subsequent `is_held()` checks for those pairs immediately return false. The heartbeat does not reacquire a lost lock or interrupt application code already running.

The heartbeat stops when normal releases empty the held set. An `atexit` handler attempts to release all remaining tokens. A process exit that bypasses cleanup leaves expiry as the recovery mechanism.

An explicit `extend()` renews Redis for that call; it does not update the TTL stored for future heartbeat renewals. Very short TTLs should be evaluated against the one-second minimum heartbeat interval.

## Writer publication boundary

`DataWriter.write()` and `DataWriter.compact()` acquire a simple-table lock using a 30-second TTL and a 60-second acquisition timeout. They release it in `finally`. The lock covers reading the current snapshot, generating data and metadata, publishing the leaf, and bumping the root version.

Immediately before publishing a new leaf, the writer calls `_assert_lock_still_held()`. A literal false result raises `LockLostError`. The normal catalog's ownership checker returns false when Redis cannot be read. The compatibility helper skips verification if the catalog lacks the method or if that method itself raises.

The ownership check and leaf publication are separate Redis operations. The leaf update Lua script does not receive the lock token and does not compare an expected version. There is consequently a gap between successful verification and publication in which the lease can expire. See [catalog consistency](05_redis_catalog.md) for the exact atomic operations.

## Lock key scope

| Resource | Key |
| --- | --- |
| Simple table | `supertable:<org>:lakes:<sup>:lock:leaf:doc:<simple>` |
| Stage and its pipes | `supertable:<org>:lakes:<sup>:lock:stage:doc:<stage>` |

Different simple tables use different keys. `RoleManager` also uses a leaf lock named `roles_init` while initializing the superadmin role. Builders and name constraints are described in [Redis layout](16_redis_layout.md).

## Staging and pipe locks

[Staging](../supertable/staging_area.py) and [SuperPipe](../supertable/super_pipe.py) implement their own `_with_lock()` operations using the same stage key. This makes staging mutations and pipe-definition mutations contend with one another.

| Caller | Lease | Acquisition behavior |
| --- | --- | --- |
| `Staging._with_lock()` | 30 seconds | One `SET NX` attempt; contention raises `RuntimeError` |
| `SuperPipe._with_lock()` | 10 seconds | One `SET NX` attempt; contention raises `RuntimeError` |

Both use token-checked Lua deletion in `finally`. Neither starts a heartbeat, retries acquisition, or checks ownership before completing the callback. A callback that outlives its TTL continues executing after the key can be acquired elsewhere. The catalog's `acquire_stage_lock()` wrapper uses the renewable lock manager, but these two callers do not use that wrapper.

## FileLocking

```python
from supertable.locking import FileLocking

locks = FileLocking("/tmp/supertable-locks")
token = locks.acquire("import-orders", ttl_s=30, timeout_s=10)
if token is None:
    raise TimeoutError("Import is already locked")
try:
    owner = locks.who("import-orders")
finally:
    locks.release("import-orders", token)
```

`FileLocking(working_dir, lock_file_name=".lock.json", retry_interval=0.1)` creates the working directory and maintains a JSON array in its lock file:

```json
[{"res": "import-orders", "exp": 1770000030, "tok": "example-token"}]
```

`res` is the resource name, `exp` is an integer epoch-second expiry, and `tok` is the owner token. Mutations acquire an exclusive `fcntl.flock`, read and rewrite the array, flush, and call `fsync`. Reads take a shared file lock. Invalid JSON is treated as an empty array.

Acquisition purges expired records, then inserts a record only if that resource has no live owner. It retries until timeout and returns a token or `None`. The constructor clamps its retry interval to at least 10 ms. `who(key)` returns a current nonexpired token or `None`; this implementation has no `is_held()` method. Release and extension require matching resource and token.

A heartbeat renews held records at half its configured TTL, with a minimum one-second interval. Unlike the Redis implementation, one TTL is shared across the instance: the latest successful acquisition sets the heartbeat TTL for every held resource. Extension converts milliseconds to integer seconds with a minimum of one second. It can renew an expired matching record that has not yet been purged.

File locking depends on Unix `fcntl` and on every participant sharing the same lock file with working filesystem lock semantics. It does not coordinate with Redis locks. It also has no lost-token cache or automatic exception in the protected operation when renewal fails.
