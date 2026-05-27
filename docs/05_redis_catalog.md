# 05 -- Redis Catalog

## Business Context

Redis serves as the metadata backbone of SuperTable. Every table pointer, version counter, RBAC identity, staging definition, auth token, and sharing link is stored in Redis as JSON strings. This gives the system sub-millisecond metadata lookups, atomic version increments via Lua scripts, and a single source of truth for coordination across concurrent writers.

The catalog is split into two layers:

- **RedisConnector** (`supertable/redis_connector.py`) -- handles connection establishment, supporting direct connections, Redis URL connections, and Redis Sentinel for high availability.
- **RedisCatalog** (`supertable/redis_catalog.py`) -- the application-level API that all other modules call. It owns key naming, Lua scripts, locking, RBAC, sharing, staging/pipe management, Spark cluster registration, and engine configuration.

---

## Connection Modes (RedisConnector)

**Module**: `supertable/redis_connector.py`

### RedisOptions Dataclass

```python
@dataclass(frozen=True)
class RedisOptions:
    host: str           # SUPERTABLE_REDIS_HOST (default: localhost)
    port: int           # SUPERTABLE_REDIS_PORT (default: 6379)
    db: int             # SUPERTABLE_REDIS_DB (default: 0)
    password: Optional[str]  # SUPERTABLE_REDIS_PASSWORD
    use_ssl: bool       # SUPERTABLE_REDIS_SSL (default: false)
    decode_responses: bool  # Always True (JSON text)

    # Sentinel fields
    is_sentinel: bool       # SUPERTABLE_REDIS_SENTINEL (true/false)
    sentinel_hosts: List[tuple]  # SUPERTABLE_REDIS_SENTINELS="host1:26379,host2:26379"
    sentinel_master: str    # SUPERTABLE_REDIS_SENTINEL_MASTER="mymaster"
    sentinel_password: Optional[str]  # SUPERTABLE_REDIS_SENTINEL_PASSWORD
    sentinel_strict: bool   # Always True (no silent fallback)
```

All fields are populated from environment variables via the `settings` object. The dataclass is frozen (immutable).

### Mode 1: Direct Connection (Default)

When Sentinel is not enabled, `create_redis_client()` creates a standard `redis.Redis` connection:

```python
redis.Redis(
    host=opts.host,
    port=opts.port,
    db=opts.db,
    password=opts.password,
    decode_responses=True,
    ssl=opts.use_ssl,
)
```

**Environment variables**: `SUPERTABLE_REDIS_HOST`, `SUPERTABLE_REDIS_PORT`, `SUPERTABLE_REDIS_DB`, `SUPERTABLE_REDIS_PASSWORD`, `SUPERTABLE_REDIS_SSL`.

### Mode 2: Redis Sentinel (High Availability)

When `SUPERTABLE_REDIS_SENTINEL=true` and sentinel hosts are configured:

1. Creates a `Sentinel` object with the configured sentinel nodes.
2. Obtains a master connection via `sentinel.master_for()`.
3. Performs a fail-fast probe with a 3-second deadline (repeated `ping()` calls with 200 ms sleep between retries).
4. If the probe succeeds, returns the sentinel-backed client.
5. If the probe fails and `sentinel_strict=True` (always), raises the error immediately.

**Sentinel environment variables**: `SUPERTABLE_REDIS_SENTINELS`, `SUPERTABLE_REDIS_SENTINEL_MASTER`, `SUPERTABLE_REDIS_SENTINEL_PASSWORD`.

**Password sharing**: When `SUPERTABLE_REDIS_PASSWORD` is not set but `SUPERTABLE_REDIS_SENTINEL_PASSWORD` is, the sentinel password is reused for Redis auth (common in single-password deployments).

### RedisConnector Class

```python
class RedisConnector:
    def __init__(self, options: Optional[RedisOptions] = None):
        self.r = create_redis_client(options)
```

A thin wrapper that holds the connected `redis.Redis` instance. Used by `RedisCatalog.__init__()`.

---

## Key Naming Taxonomy

All keys follow a hierarchical namespace. Below is the complete list of key patterns found in the codebase.

### Core Metadata Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:{sup}:meta:root` | STRING (JSON) | SuperTable root pointer -- version, timestamp, clone flags |
| `supertable:{org}:{sup}:meta:leaf:{simple}` | STRING (JSON) | SimpleTable leaf pointer -- version, timestamp, path, optional payload |
| `supertable:{org}:{sup}:meta:mirrors` | STRING (JSON) | Enabled mirror export formats (DELTA, ICEBERG, PARQUET) |
| `supertable:{org}:{sup}:meta:table_config:{simple}` | STRING (JSON) | Per-table config (dedup mode, primary keys, etc.) |

### Lock Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:{sup}:lock:leaf:{simple}` | STRING (token) | Distributed write lock for a SimpleTable (SET NX EX) |
| `supertable:{org}:{sup}:lock:stage:{stage_name}` | STRING (token) | Distributed lock for staging/pipe operations |

### RBAC Keys (UUID-Based Identity)

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:{sup}:rbac:users:meta` | HASH | User meta -- version counter, last_updated_ms, initialized flag |
| `supertable:{org}:{sup}:rbac:users:index` | SET | Set of all user_ids |
| `supertable:{org}:{sup}:rbac:users:doc:{user_id}` | HASH | User document -- username, roles (JSON array), timestamps |
| `supertable:{org}:{sup}:rbac:users:name_to_id` | HASH | Mapping: lowercase username -> user_id |
| `supertable:{org}:{sup}:rbac:roles:meta` | HASH | Role meta -- version counter, last_updated_ms, initialized flag |
| `supertable:{org}:{sup}:rbac:roles:index` | SET | Set of all role_ids |
| `supertable:{org}:{sup}:rbac:roles:doc:{role_id}` | HASH | Role document -- role_name, role type, tables, columns, filters |
| `supertable:{org}:{sup}:rbac:roles:type:{role_type}` | SET | Set of role_ids belonging to a specific role type (e.g., superadmin) |
| `supertable:{org}:{sup}:rbac:roles:name_to_id` | HASH | Mapping: lowercase role_name -> role_id |

### Auth Token Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:_system_:auth:tokens` | HASH | Mapping: token_id (sha256) -> JSON metadata (created_ms, created_by, label, enabled, username, user_id). Lives under the reserved `_system_` scope so it can never collide with a user-supplied supertable named `auth`. |

### Service Registry Keys (per organization)

| Pattern | Type | Purpose |
|---------|------|---------|
| `dataisland:{org}:registry:{service_type}:{host}:{pid}` | STRING (JSON) | Heartbeat record for one running service instance (TTL 30s, refreshed every 15s). `service_type` is one of `api`, `webui`, `odata`, `mcp`, `sdk`, `lighthouse`. Lives under the `dataisland:` prefix because the service registry is a platform concern, not a SuperTable one. Written by `supertable.service_registry.ServiceRegistry` (api/webui/odata/mcp) or `lighthouse.bootstrap.ServiceRegistrar` (Lighthouse, via REST `POST /api/v1/services/register`). Scanned via `RK.registry_pattern_for_org(org)` (per-tenant) or `RK.registry_pattern()` (cross-tenant). |
| `dataisland:apps:{app_name}:master_mcp` | STRING (JSON) | Bootstrap entry that tells an app (Lighthouse, Gatekeeper, …) which master MCP to connect to and with what shared service token. Single global key per app, set by an admin via the REST API (`POST /api/v1/apps/{app}/master-mcp`); read by the app on every boot via the matching GET. |

### Data Sharing Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:shares:doc:{share_id}` | STRING (JSON) | Share definition document (provider side) |
| `supertable:{org}:shares:index` | SET | Set of share_ids for an organization |
| `supertable:{org}:{sup}:linked_shares:doc:{link_id}` | STRING (JSON) | Linked share document (consumer side) |
| `supertable:{org}:{sup}:linked_shares:index` | SET | Set of link_ids for a SuperTable |

### Staging / Pipe Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:{sup}:meta:staging:{staging_name}` | STRING (JSON) | Staging area metadata |
| `supertable:{org}:{sup}:meta:staging:meta` | SET | Set of staging names for fast listing |
| `supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}` | STRING (JSON) | Pipe metadata within a staging area |
| `supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:meta` | SET | Set of pipe names for a staging area |

### Spark Cluster Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `spark:{org}:thrifts` | HASH | Mapping: cluster_id -> JSON cluster config (Spark Thrift servers) |
| `spark:{org}:plugs` | HASH | Mapping: plug_id -> JSON plug config (PySpark notebook runtimes) |

### Engine Configuration Keys

| Pattern | Type | Purpose |
|---------|------|---------|
| `supertable:{org}:{sup}:config:engine` | STRING (JSON) | DuckDB / engine runtime configuration (memory, threads, thresholds) |

---

## RedisCatalog Class

**Module**: `supertable/redis_catalog.py`

```python
class RedisCatalog:
    def __init__(self, options: Optional[RedisOptions] = None)
```

On initialization, the catalog:
1. Creates a Redis connection via `RedisConnector`.
2. Registers all Lua scripts (CAS leaf update, root bump, RBAC mutations).
3. Initializes a `RedisLocking` instance for distributed lock operations.

### Methods Grouped by Domain

---

#### Health Check

| Method | Signature | Description |
|--------|-----------|-------------|
| `ping` | `() -> bool` | Tests Redis connectivity via PING. |

---

#### Locking

| Method | Signature | Description |
|--------|-----------|-------------|
| `acquire_simple_lock` | `(org, sup, simple, ttl_s=30, timeout_s=30) -> Optional[str]` | Acquires a distributed lock on a SimpleTable. Returns a token string or None. |
| `release_simple_lock` | `(org, sup, simple, token) -> bool` | Releases a lock using compare-and-delete via Lua. |
| `acquire_stage_lock` | `(org, sup, stage_name, ttl_s=30, timeout_s=30) -> Optional[str]` | Acquires a distributed lock for staging/pipe operations. |

---

#### SuperTable Operations (Root)

| Method | Signature | Description |
|--------|-----------|-------------|
| `ensure_root` | `(org, sup) -> None` | Initializes `meta:root` with `{"version": 0, "ts": ...}` if missing. |
| `root_exists` | `(org, sup) -> bool` | Checks existence of the `meta:root` key. |
| `get_root` | `(org, sup) -> Optional[Dict]` | Reads and returns the root JSON document. |
| `update_root_flags` | `(org, sup, flags) -> bool` | Merges flags (e.g., `read_only`, `cloned_from`) into the root document. |
| `find_readonly_clones` | `(org, source_sup) -> List[str]` | Scans all roots to find SuperTables that are read-only clones of the source. |
| `bump_root` | `(org, sup, now_ms=None) -> int` | Atomically increments the root version via Lua script. Returns new version. |
| `delete_super_table` | `(org, sup, count=1000) -> int` | Deletes ALL Redis keys matching `supertable:{org}:{sup}:*` via SCAN. Returns count deleted. |

---

#### Replica Resolution

| Method | Signature | Description |
|--------|-----------|-------------|
| `_resolve_replica_info` | `(org, sup) -> Optional[tuple]` | If `sup` is a replica clone, returns `(source_name, allowed_tables)`. Returns None for non-replicas. Single-level only. |
| `_resolve_replica_source` | `(org, sup) -> Optional[str]` | Backward-compatible wrapper; returns only the source name. |

---

#### Leaf (SimpleTable) Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `leaf_exists` | `(org, sup, simple) -> bool` | Checks existence of the leaf key (replica-aware). |
| `get_leaf` | `(org, sup, simple) -> Optional[Dict]` | Reads the leaf pointer (replica-aware). Returns `{version, ts, path, payload}`. |
| `delete_leaf` | `(org, sup, simple) -> bool` | Deletes a leaf pointer key. |
| `set_leaf_path_cas` | `(org, sup, simple, path, now_ms=None) -> int` | Atomically sets the leaf pointer to a new path via Lua CAS script. Returns new version. |
| `set_leaf_payload_cas` | `(org, sup, simple, payload, path, now_ms=None) -> int` | Atomically sets the leaf pointer with path AND inline snapshot payload. Returns new version. |
| `delete_simple_table` | `(org, sup, simple) -> bool` | Deletes the leaf key and its associated lock key. |
| `scan_leaf_keys` | `(org, sup, count=1000) -> Iterator[str]` | Yields all `meta:leaf:*` keys via SCAN (replica-aware). |
| `scan_leaf_items` | `(org, sup, count=1000) -> Iterator[Dict]` | Iterates leaf keys and fetches values in pipeline batches. Yields `{simple, version, ts, path, payload}`. |

---

#### Mirror Format Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `get_mirrors` | `(org, sup) -> List[str]` | Reads enabled mirror formats. Validates against `DELTA`, `ICEBERG`, `PARQUET`. |
| `set_mirrors` | `(org, sup, formats, now_ms=None) -> List[str]` | Atomically sets the enabled mirror formats. |
| `enable_mirror` | `(org, sup, fmt) -> List[str]` | Adds a format to the enabled list (idempotent). |
| `disable_mirror` | `(org, sup, fmt) -> List[str]` | Removes a format from the enabled list. |

---

#### RBAC -- Role Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `rbac_init_role_meta` | `(org, sup) -> None` | Ensures the role meta HASH exists. Idempotent. |
| `rbac_create_role` | `(org, sup, role_id, role_data) -> None` | Persists a new role document, updates index SET and name-to-id HASH. Bumps role meta version. |
| `rbac_update_role` | `(org, sup, role_id, fields) -> None` | Updates specific fields of an existing role in-place. |
| `rbac_delete_role` | `(org, sup, role_id) -> bool` | Atomically deletes a role via Lua: strips from all users, removes from indexes and name-to-id map. |
| `rbac_role_exists` | `(org, sup, role_id) -> bool` | Checks if a role document key exists. |
| `rbac_get_role_ids_by_type` | `(org, sup, role_type) -> List[str]` | Returns role_ids belonging to a specific type (e.g., `"superadmin"`). |
| `rbac_get_superadmin_role_id` | `(org, sup) -> Optional[str]` | Shortcut: returns the first superadmin role_id. |
| `rbac_get_role_id_by_name` | `(org, sup, role_name) -> Optional[str]` | Looks up role_id from role_name (case-insensitive). |
| `get_roles` | `(org, sup) -> List[Dict]` | Gets all roles via pipeline batch (SMEMBERS + batched HGETALL). |
| `get_role_details` | `(org, sup, role_id) -> Optional[Dict]` | Gets detailed role info by role_id. |

---

#### RBAC -- User Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `rbac_init_user_meta` | `(org, sup) -> None` | Ensures the user meta HASH exists. Idempotent. |
| `rbac_create_user` | `(org, sup, user_id, user_data) -> None` | Persists a new user document, updates index SET and username-to-id HASH. |
| `rbac_update_user` | `(org, sup, user_id, fields) -> None` | Updates specific fields of an existing user in-place. |
| `rbac_rename_user` | `(org, sup, user_id, old_username, new_username) -> None` | Atomically updates the username-to-id mapping. |
| `rbac_delete_user` | `(org, sup, user_id) -> None` | Deletes user document and removes from all indexes. |
| `rbac_get_user_id_by_username` | `(org, sup, username) -> Optional[str]` | Looks up user_id from username (case-insensitive). |
| `rbac_list_user_ids` | `(org, sup) -> List[str]` | Returns all user_ids from the index SET. |
| `get_users` | `(org, sup) -> List[Dict]` | Gets all users via pipeline batch (SMEMBERS + batched HGETALL). |
| `get_user_details` | `(org, sup, user_id) -> Optional[Dict]` | Gets detailed user info by user_id. |

---

#### RBAC -- Role-User Mutations

| Method | Signature | Description |
|--------|-----------|-------------|
| `rbac_add_role_to_user` | `(org, sup, user_id, role_id) -> bool` | Atomically adds a role to a user's role list via Lua (no-op if already present). |
| `rbac_remove_role_from_user` | `(org, sup, user_id, role_id) -> bool` | Atomically removes a role from a user's role list via Lua. |

---

#### Auth Token Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `list_auth_tokens` | `(org) -> List[Dict]` | Lists all auth tokens for an org. Sorted by `created_ms` descending. Tokens are stored hashed. |
| `create_auth_token` | `(org, created_by, label=None, enabled=True, username="", user_id="") -> Dict` | Creates a new token. Returns the plaintext token ONLY once. Redis stores `sha256(token)` as the key. |
| `delete_auth_token` | `(org, token_id) -> bool` | Deletes a token by its token_id (sha256 hash). |
| `validate_auth_token` | `(org, token) -> bool` | Validates a plaintext token by hashing and checking existence. |
| `validate_auth_token_full` | `(org, token) -> Optional[Dict]` | Validates and returns full token metadata (including linked username/user_id). |

---

#### Data Sharing -- Provider Side

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_share` | `(org, share_id, share_doc) -> None` | Stores a share definition and adds to the org-level index SET. |
| `get_share` | `(org, share_id) -> Optional[Dict]` | Retrieves a share definition by ID. |
| `delete_share` | `(org, share_id) -> bool` | Deletes a share definition and removes from index. |
| `list_shares` | `(org) -> List[Dict]` | Lists all share definitions for an organization. |

---

#### Data Sharing -- Consumer Side (Linked Shares)

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_linked_share` | `(org, sup, link_id, link_doc) -> None` | Stores a linked share and adds to the SuperTable-level index SET. |
| `get_linked_share` | `(org, sup, link_id) -> Optional[Dict]` | Retrieves a linked share by ID. |
| `update_linked_share` | `(org, sup, link_id, doc) -> bool` | Replaces a linked share document. |
| `delete_linked_share` | `(org, sup, link_id) -> bool` | Deletes a linked share and removes from index. |
| `list_linked_shares` | `(org, sup) -> List[Dict]` | Lists all linked shares for a SuperTable. |

---

#### Staging / Pipe Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `upsert_staging_meta` | `(org, sup, staging_name, meta) -> bool` | Upserts staging metadata and adds to the staging index SET. |
| `get_staging_meta` | `(org, sup, staging_name) -> Optional[Dict]` | Retrieves staging metadata. |
| `list_stagings` | `(org, sup, count=1000) -> List[str]` | Lists staging names. Prefers index SET; falls back to SCAN. |
| `delete_staging_meta` | `(org, sup, staging_name, count=1000) -> int` | Deletes staging and all related keys (pipes, pipe meta). Returns count deleted. |
| `upsert_pipe_meta` | `(org, sup, staging_name, pipe_name, meta) -> bool` | Upserts pipe metadata and adds to the pipe index SET. |
| `get_pipe_meta` | `(org, sup, staging_name, pipe_name) -> Optional[Dict]` | Retrieves pipe metadata. |
| `list_pipes` | `(org, sup, staging_name, count=1000) -> List[str]` | Lists pipe names for a staging. Prefers index SET; falls back to SCAN. |
| `list_pipe_metas` | `(org, sup, staging_name, count=1000) -> List[Dict]` | Lists full pipe metadata objects for a staging area. |
| `delete_pipe_meta` | `(org, sup, staging_name, pipe_name) -> int` | Deletes a pipe meta key and removes from index. |

---

#### Spark Cluster Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `register_spark_cluster` | `(org, cluster_id, config) -> None` | Registers or updates a Spark Thrift cluster. Config includes host, port, name, min/max bytes, status, s3_enabled. |
| `list_spark_clusters` | `(org) -> List[Dict]` | Returns all registered Spark Thrift clusters for an org. |
| `select_spark_cluster` | `(org, job_bytes, force=False) -> Optional[Dict]` | Selects the best cluster for a job size. Filters by status/size; prefers tightest fit. |
| `register_spark_plug` | `(org, plug_id, config) -> None` | Registers a Spark Plug (PySpark notebook runtime). Config includes spark_master, ws_url, webui_url, status. |

---

#### Table Configuration

| Method | Signature | Description |
|--------|-----------|-------------|
| `set_table_config` | `(org, sup, simple, config) -> bool` | Stores per-table configuration (primary keys, dedup mode). Full replacement. |
| `get_table_config` | `(org, sup, simple) -> Optional[Dict]` | Retrieves per-table configuration. |

---

#### Engine Configuration

| Method | Signature | Description |
|--------|-----------|-------------|
| `set_engine_config` | `(org, sup, config) -> bool` | Stores engine runtime configuration. Only whitelisted fields are persisted. |
| `get_engine_config` | `(org, sup) -> Optional[Dict]` | Retrieves engine runtime configuration. |

**Whitelisted engine config fields** (defined in `ENGINE_CONFIG_FIELDS`):

| Field | Environment Variable Fallback |
|-------|-------------------------------|
| `engine_lite_max_bytes` | `SUPERTABLE_ENGINE_LITE_MAX_BYTES` |
| `engine_spark_min_bytes` | `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` |
| `engine_freshness_sec` | `SUPERTABLE_ENGINE_FRESHNESS_SEC` |
| `duckdb_memory_limit` | `SUPERTABLE_DUCKDB_MEMORY_LIMIT` |
| `duckdb_io_multiplier` | `SUPERTABLE_DUCKDB_IO_MULTIPLIER` |
| `duckdb_threads` | `SUPERTABLE_DUCKDB_THREADS` |
| `duckdb_http_timeout` | `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` |
| `duckdb_external_cache_size` | `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` |

---

## CAS (Compare-and-Swap) Operations

The catalog uses server-side Lua scripts to achieve atomic read-modify-write semantics without client-side locking.

### Leaf CAS Set (`_LUA_LEAF_CAS_SET`)

1. Reads the current value at the leaf key.
2. Extracts the current version (or defaults to -1 if the key does not exist).
3. Increments the version.
4. Writes `{version, ts, path}` atomically.
5. Returns the new version number.

### Leaf Payload CAS Set (`_LUA_LEAF_PAYLOAD_CAS_SET`)

Same as above, but also embeds a `payload` field containing the full snapshot data. This allows read operations to skip the storage round-trip for metadata-only queries.

### Root Bump (`_LUA_ROOT_BUMP`)

1. Reads the current root value.
2. Extracts the current version (or defaults to -1).
3. Increments the version.
4. Writes `{version, ts}` atomically.
5. Returns the new version number.

### RBAC Lua Scripts

| Script | Purpose |
|--------|---------|
| `_LUA_RBAC_BUMP_META` | Atomically increments the version field and updates `last_updated_ms` on an RBAC meta HASH. |
| `_LUA_RBAC_DELETE_ROLE` | Atomically deletes a role document, strips the role_id from all user documents, removes from index SET, type SET, and name-to-id HASH. All in a single atomic operation. |
| `_LUA_RBAC_REMOVE_ROLE_FROM_USER` | Atomically removes a role_id from a single user's roles JSON array, updates timestamps and meta version. |
| `_LUA_RBAC_ADD_ROLE_TO_USER` | Atomically adds a role_id to a user's roles JSON array (no-op if already present), updates timestamps and meta version. |

All Lua scripts are registered once during `RedisCatalog.__init__()` and invoked through Redis script objects, ensuring the scripts are cached server-side after the first call.
