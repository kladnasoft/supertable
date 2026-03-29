# Data Island Core — Redis Catalog

## Overview

Redis is the metadata backbone of Data Island Core. Every piece of platform state that isn't a data file lives in Redis: table snapshots, version pointers, RBAC roles and users, auth tokens, distributed locks, staging area metadata, pipe configurations, engine settings, monitoring metrics, and audit event streams.

The catalog is implemented in two files. `redis_connector.py` handles connection management (standalone, Sentinel, TLS, connection pooling). `redis_catalog.py` provides all catalog operations as methods on the `RedisCatalog` class — the single point of access for metadata across all servers.

---

## How it works

### Connection lifecycle

At import time, `RedisOptions` reads connection settings from the `Settings` dataclass and constructs a frozen configuration. `RedisConnector` uses this to create a `redis.Redis` client (or a Sentinel-managed master connection). The client is shared across all catalog instances via the `server_common.py` singleton.

Connection modes:

1. **Standalone** (default): direct connection to `SUPERTABLE_REDIS_HOST:SUPERTABLE_REDIS_PORT`.
2. **Sentinel**: connects to Sentinel nodes, discovers the master, and obtains a master connection. Automatic failover on master change.
3. **URL**: if `SUPERTABLE_REDIS_URL` is set, it overrides host/port/db entirely.

All connections use `decode_responses=True` — every value returned from Redis is a Python string. JSON data is serialized/deserialized by the catalog methods.

### Key naming conventions

Every Redis key follows the pattern `supertable:{org}:{sup}:...` where `org` is the organization and `sup` is the SuperTable name. This scopes all data to a tenant and makes it possible to host multiple organizations in a single Redis instance.

Key families:

| Prefix pattern | Redis type | Purpose |
|---|---|---|
| `supertable:{org}:{sup}:meta:root` | Hash | Root metadata (version counter, timestamps) |
| `supertable:{org}:{sup}:meta:leaf:{simple}` | Hash | Per-table snapshot pointer + metadata |
| `supertable:{org}:{sup}:lock:{simple}` | String (SET NX) | Per-table distributed write lock |
| `supertable:{org}:{sup}:meta:mirrors` | Set | Enabled mirror formats |
| `supertable:{org}:{sup}:rbac:role:meta` | Hash | Role metadata (version, count) |
| `supertable:{org}:{sup}:rbac:role:index` | Set | All role IDs |
| `supertable:{org}:{sup}:rbac:role:doc:{role_id}` | Hash | Individual role definition |
| `supertable:{org}:{sup}:rbac:role:type:{type}` | Set | Role IDs grouped by type |
| `supertable:{org}:{sup}:rbac:role:name_to_id` | Hash | Role name → role ID lookup |
| `supertable:{org}:{sup}:rbac:user:meta` | Hash | User metadata (version, count) |
| `supertable:{org}:{sup}:rbac:user:index` | Set | All user IDs |
| `supertable:{org}:{sup}:rbac:user:doc:{user_id}` | Hash | Individual user definition |
| `supertable:{org}:{sup}:rbac:user:username_to_id` | Hash | Username → user ID lookup |
| `supertable:{org}:auth:tokens` | Hash | Auth tokens (token_id → JSON) |
| `supertable:{org}:{sup}:meta:staging:{name}` | Hash | Staging area metadata |
| `supertable:{org}:{sup}:meta:staging:meta` | Set | Staging name index |
| `supertable:{org}:{sup}:meta:staging:{stg}:pipe:{name}` | Hash | Pipe configuration |
| `supertable:{org}:{sup}:meta:staging:{stg}:pipe:meta` | Set | Pipe name index per staging |
| `supertable:{org}:{sup}:meta:table_config:{simple}` | Hash | Per-table config (dedup, primary keys) |
| `supertable:{org}:{sup}:config:engine` | Hash | DuckDB/Spark engine runtime config |
| `supertable:{org}:{sup}:lock:stage:{name}` | String (SET NX) | Per-staging lock |
| `spark:{org}:thrifts` | Hash | Spark Thrift cluster registry |
| `spark:{org}:plugs` | Hash | Spark plug (worker) registry |
| `supertable:{org}:audit:stream` | Stream | Audit event hot tier |
| `supertable:{org}:audit:chain_head:{instance}` | Hash | Audit chain state per instance |

---

## RedisCatalog API reference

### Snapshot management

| Method | Description |
|---|---|
| `ensure_root(org, sup)` | Create the root metadata hash if it doesn't exist. |
| `root_exists(org, sup)` | Check if a SuperTable root exists. |
| `get_root(org, sup)` | Get root metadata (version counter, timestamps). |
| `bump_root(org, sup)` | Increment root version and update timestamp. |
| `leaf_exists(org, sup, simple)` | Check if a table (leaf) exists. |
| `get_leaf(org, sup, simple)` | Get the current snapshot pointer for a table. |
| `set_leaf_path_cas(org, sup, simple, path)` | Set the snapshot path with compare-and-swap. |
| `set_leaf_payload_cas(org, sup, simple, ...)` | Update the full leaf payload atomically. |
| `scan_leaf_keys(org, sup)` | Iterate all leaf keys for a SuperTable. |
| `scan_leaf_items(org, sup)` | Iterate all leaf metadata dicts. |

### Lock management

| Method | Description |
|---|---|
| `acquire_simple_lock(org, sup, simple, ttl_s, timeout_s)` | Acquire a per-table write lock. Returns a token or None. |
| `release_simple_lock(org, sup, simple, token)` | Release the lock (only if the token matches). |
| `acquire_stage_lock(org, sup, stage_name, ...)` | Acquire a per-staging lock. |

Locks use Redis SET with NX (create-if-not-exists) and EX (TTL). The token is a random UUID — only the holder can release. See the Locking documentation for full details.

### RBAC — Roles

| Method | Description |
|---|---|
| `rbac_init_role_meta(org, sup)` | Initialize the role metadata structure. |
| `rbac_create_role(org, sup, role_id, role_data)` | Create a new role. |
| `rbac_update_role(org, sup, role_id, fields)` | Update role fields. |
| `rbac_delete_role(org, sup, role_id)` | Delete a role and all its index entries. |
| `rbac_role_exists(org, sup, role_id)` | Check if a role exists. |
| `rbac_get_role_ids_by_type(org, sup, role_type)` | List role IDs of a given type. |
| `rbac_get_superadmin_role_id(org, sup)` | Get the superadmin role ID. |
| `rbac_get_role_id_by_name(org, sup, role_name)` | Look up role ID by name. |
| `get_role_details(org, sup, role_id)` | Get full role definition. |
| `get_roles(org, sup)` | List all roles with summaries. |

### RBAC — Users

| Method | Description |
|---|---|
| `rbac_init_user_meta(org, sup)` | Initialize the user metadata structure. |
| `rbac_create_user(org, sup, user_id, user_data)` | Create a new user. |
| `rbac_update_user(org, sup, user_id, fields)` | Update user fields. |
| `rbac_rename_user(org, sup, user_id, old, new)` | Rename a user (updates the username→ID index). |
| `rbac_delete_user(org, sup, user_id)` | Delete a user and all index entries. |
| `rbac_get_user_id_by_username(org, sup, username)` | Look up user ID by username. |
| `rbac_list_user_ids(org, sup)` | List all user IDs. |
| `rbac_add_role_to_user(org, sup, user_id, role_id)` | Assign a role to a user. |
| `rbac_remove_role_from_user(org, sup, user_id, role_id)` | Remove a role from a user. |
| `get_user_details(org, sup, user_id)` | Get full user definition. |
| `get_users(org, sup)` | List all users with summaries. |

### Auth tokens

| Method | Description |
|---|---|
| `list_auth_tokens(org)` | List all tokens (metadata only, not the token values). |
| `create_auth_token(org, created_by, label)` | Create a new token. Returns `{token_id, token, label, ...}`. |
| `delete_auth_token(org, token_id)` | Revoke a token by ID. |
| `validate_auth_token(org, token)` | Validate a raw token against stored SHA-256 hashes. |

Tokens are stored as SHA-256 hashes. The plaintext token is returned only once at creation time — it is never persisted or retrievable.

### Mirroring

| Method | Description |
|---|---|
| `get_mirrors(org, sup)` | List enabled mirror formats. |
| `set_mirrors(org, sup, formats)` | Set the mirror formats list. |
| `enable_mirror(org, sup, fmt)` | Enable a mirror format (e.g., `"delta"`, `"iceberg"`). |
| `disable_mirror(org, sup, fmt)` | Disable a mirror format. |

### Staging & Pipes

| Method | Description |
|---|---|
| `upsert_staging_meta(org, sup, staging_name, meta)` | Create or update staging area metadata. |
| `get_staging_meta(org, sup, staging_name)` | Get staging area metadata. |
| `list_stagings(org, sup)` | List all staging area names. |
| `delete_staging_meta(org, sup, staging_name)` | Delete staging area and all its pipes. |
| `upsert_pipe_meta(org, sup, staging_name, pipe_name, meta)` | Create or update a pipe. |
| `get_pipe_meta(org, sup, staging_name, pipe_name)` | Get pipe configuration. |
| `list_pipe_metas(org, sup, staging_name)` | List all pipe configurations for a staging. |
| `list_pipes(org, sup, staging_name)` | List pipe names for a staging. |
| `delete_pipe_meta(org, sup, staging_name, pipe_name)` | Delete a single pipe. |

### Table & Engine configuration

| Method | Description |
|---|---|
| `set_table_config(org, sup, simple, config)` | Set per-table config (dedup, primary keys, chunk sizes). |
| `get_table_config(org, sup, simple)` | Get per-table config. |
| `set_engine_config(org, sup, config)` | Set DuckDB/Spark engine runtime config. |
| `get_engine_config(org, sup)` | Get engine runtime config. |

### Spark cluster management

| Method | Description |
|---|---|
| `register_spark_cluster(org, cluster_id, config)` | Register a Spark Thrift server. |
| `list_spark_clusters(org)` | List all registered Spark clusters. |
| `select_spark_cluster(org, job_bytes, force)` | Select the best cluster for a given data size. |
| `register_spark_plug(org, plug_id, config)` | Register a Spark Worker (plug). |

### Deletion

| Method | Description |
|---|---|
| `delete_simple_table(org, sup, simple)` | Delete a table and its leaf key. |
| `delete_super_table(org, sup)` | Delete all Redis keys for a SuperTable (scan + batch delete). |

---

## RedisConnector

`redis_connector.py` manages the Redis connection lifecycle:

```python
from supertable.redis_connector import RedisConnector, RedisOptions

options = RedisOptions()            # Reads from Settings automatically
connector = RedisConnector(options)
client = connector.r                # redis.Redis instance
```

The connector supports:

- **Standalone mode**: direct `redis.Redis(host, port, db, password, ssl)` connection
- **Sentinel mode**: creates a `Sentinel` instance, discovers the master, returns a master connection
- **Health check**: `ping()` verifies connectivity
- **Connection pooling**: uses the default `redis-py` connection pool (thread-safe)

---

## Inspecting catalog state

Use `redis-cli` to inspect metadata directly:

```bash
# List all SuperTable roots for an organization
redis-cli KEYS "supertable:acme:*:meta:root"

# Get the root metadata for a SuperTable
redis-cli HGETALL "supertable:acme:example:meta:root"

# List all tables (leaves) in a SuperTable
redis-cli KEYS "supertable:acme:example:meta:leaf:*"

# Get a table's current snapshot pointer
redis-cli HGETALL "supertable:acme:example:meta:leaf:orders"

# List all roles
redis-cli SMEMBERS "supertable:acme:example:rbac:role:index"

# Get a role definition
redis-cli HGETALL "supertable:acme:example:rbac:role:doc:role_abc123"

# List all auth tokens
redis-cli HGETALL "supertable:acme:auth:tokens"

# Check the audit stream length
redis-cli XLEN "supertable:acme:audit:stream"
```

---

## Configuration

See the **Redis** section in the Configuration documentation. The key variables are:

| Environment variable | Default | Description |
|---|---|---|
| `SUPERTABLE_REDIS_HOST` | `localhost` | Redis host |
| `SUPERTABLE_REDIS_PORT` | `6379` | Redis port |
| `SUPERTABLE_REDIS_DB` | `0` | Redis database number |
| `SUPERTABLE_REDIS_PASSWORD` | (empty) | Redis auth password |
| `SUPERTABLE_REDIS_SSL` | `false` | Enable TLS |
| `SUPERTABLE_REDIS_SENTINEL` | `false` | Enable Sentinel mode |
| `SUPERTABLE_REDIS_SENTINELS` | (empty) | Sentinel addresses |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | `mymaster` | Sentinel master name |

---

## Module structure

```
supertable/
  redis_connector.py      RedisOptions, RedisConnector — connection management (130 lines)
  redis_catalog.py        RedisCatalog — all catalog operations, key helpers (1,448 lines)
```

---

## Frequently asked questions

**What Redis version is required?**
Redis 6.0 or higher. Redis 7+ is recommended for Streams with consumer group lag tracking (used by the audit module).

**Can I use a managed Redis service (ElastiCache, Azure Cache, Upstash)?**
Yes. Any Redis-compatible service that supports hashes, sets, sorted sets, streams, and the SET NX command works. Use `SUPERTABLE_REDIS_URL` to connect with a full URL.

**What happens if Redis goes down?**
Reads against already-loaded metadata may continue briefly (MetaReader has an optional TTL cache). Writes, RBAC checks, locking, and audit streaming will fail. Use Redis Sentinel in production for automatic failover.

**How much memory does the catalog use?**
Metadata is compact. A SuperTable with 100 tables, 50 roles, and 100 users uses approximately 1–2 MB of Redis memory. Staging and pipe metadata adds negligible overhead. The audit Redis Stream is the largest consumer — bounded by `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` (default: 100,000 entries ≈ 50 MB).

**Can multiple Data Island Core instances share a Redis?**
Yes. This is the expected production setup. All instances connect to the same Redis and use the same key namespace. Locking ensures write safety across instances.

**Are there any Redis Cluster considerations?**
Redis Cluster is not officially supported. The key naming uses `{org}:{sup}` as part of every key, which means keys for a single SuperTable land on different hash slots. Use Redis Sentinel for HA instead.

**How are tokens secured?**
Tokens are SHA-256 hashed before storage. The plaintext token is returned once at creation time and never persisted. Validation computes the hash of the provided token and compares against stored hashes.
