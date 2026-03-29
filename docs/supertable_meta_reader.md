# Feature / Component Reverse Engineering: Supertable MetaReader (Metadata Reflection API)

## 1. Executive Summary

This code implements the **metadata reflection layer** for the Supertable platform — a read-only service that enables callers to discover, inspect, and understand the structure, schema, and statistics of SuperTables and their constituent SimpleTables.

- **Main responsibility:** Provide RBAC-filtered metadata queries: list SuperTables, list tables within a SuperTable, retrieve schemas, collect table statistics, and assemble comprehensive SuperTable overview ("super meta") — all from Redis-backed metadata with storage-based fallback.
- **Likely product capability:** The metadata API behind schema introspection, catalog browsing, data profiling, and admin dashboards. This is what powers the "what tables exist, what columns do they have, how big are they?" experience.
- **Business purpose:** Enables users, applications, and tooling (including LLM/AI agents like the Supertable MCP connector) to discover and understand the available data catalog without issuing data queries.
- **Technical role:** A read-only service layer sitting between the API/presentation layer and the underlying Redis catalog + object storage, with an optimized multi-tier caching and fallback strategy.

---

## 2. Functional Overview

### What can the product do because of this code?

1. **List all SuperTables** in an organization, filtered by the caller's RBAC role.
2. **List all tables** within a SuperTable, filtered by RBAC.
3. **Retrieve table schemas** — individual table schemas or aggregated union schemas across all tables in a SuperTable.
4. **Retrieve table statistics** — file count, row count, file size, last-updated timestamps — per table or aggregated.
5. **Assemble comprehensive SuperTable metadata** ("super meta") — a single-call composite view of all tables, their stats, and aggregate totals, with burst caching and detailed performance timing instrumentation.

### Target users / actors

| Actor | Interaction |
|---|---|
| **API caller / application** | Calls `MetaReader` methods or module-level functions to discover catalog structure |
| **Admin / operator** | Inspects table stats, schema, and super meta for operational visibility |
| **AI agent (MCP connector)** | Calls `list_tables` and schema endpoints to understand the data model before generating queries |
| **Dashboard / UI** | Consumes `get_super_meta()` to render table lists, row counts, sizes |
| **Data engineer** | Uses schema reflection to understand column types before building pipes or queries |

### Business workflows

1. **Catalog discovery:** User authenticates → calls `list_supers()` → picks a super → calls `list_tables()` → picks a table → calls `get_table_schema()` to understand columns.
2. **Dashboard assembly:** UI calls `get_super_meta()` → gets all tables with row counts, file counts, sizes, and version info in a single call.
3. **Schema introspection:** Query builder calls `get_table_schema(table_name=super_name)` → gets the union schema across all simple tables for cross-table queries.
4. **Stats inspection:** Operator calls `get_table_stats()` → gets snapshot data (minus heavy fields) for one or all tables.

### Strategic significance

This is the **catalog/reflection layer** — foundational for any data platform. It makes the data catalog self-describing, enabling:
- Schema-aware query builders (including AI-assisted query generation)
- Admin dashboards and data profiling UIs
- Programmatic data discovery for ETL and integration tooling
- RBAC-filtered views (users only see tables they have permission to access)

---

## 3. Technical Overview

### Architectural style

- **Redis-first with storage fallback:** Schema and snapshot data is first sought from Redis leaf metadata (`MGET` for bulk, `GET` for single). If Redis does not contain a parseable snapshot, the code falls back to reading `SimpleTable.get_simple_table_snapshot()` from object storage.
- **In-process burst cache:** `get_super_meta()` uses a thread-safe, version-keyed, TTL-bounded in-process cache (`_SUPER_META_CACHE`) to absorb bursty reflection calls (e.g., dashboard refreshes). Cache entries are invalidated when the root version changes.
- **RBAC-per-item filtering:** Every listing and read operation applies RBAC checks per item — inaccessible items are silently excluded (not errored).
- **Performance instrumentation:** `get_super_meta()` contains detailed `perf_counter` instrumentation gated by `SUPERTABLE_DEBUG_TIMINGS`, logging per-phase timings (access check, root fetch, scan, MGET, snapshot fallbacks).

### Major components

| Component | Role |
|---|---|
| `MetaReader` | Main class — schema, stats, and super-meta queries for a specific SuperTable |
| `list_supers()` | Module-level function — discovers all SuperTables in an organization |
| `list_tables()` | Module-level function — discovers all tables in a SuperTable |
| `_get_redis_items()` | Redis `SCAN`-based key discovery helper |
| `_try_parse_leaf_meta()` | Robust JSON parser for Redis leaf values (handles bytes, strings, nulls) |
| `_leaf_to_snapshot_like()` | Polymorphic snapshot extractor — navigates multiple Redis payload shapes |
| `_schema_to_dict()` | Schema format normalizer (dict, list-of-dicts, single-key-dicts) |
| `_prune_dict()` | Dict key stripper for removing heavy fields from stats output |
| `_SUPER_META_CACHE` | Module-level burst cache with version + TTL invalidation |

### Key design patterns

- **Multi-format tolerance:** `_leaf_to_snapshot_like()` handles at least 5 different Redis payload structures (direct `resources`, nested under `payload`, `data`, `snapshot`, or `payload.snapshot`). This indicates evolving metadata formats that must remain backward-compatible.
- **Bulk Redis fetch:** `get_super_meta()` and `get_table_schema()` use `MGET` to fetch all leaf metadata in a single Redis round-trip, then parse individually.
- **Union schema aggregation:** When `table_name == super_name`, schemas are unioned across all simple tables using a `Set[Tuple[str, Any]]` to produce a distinct column set.
- **Graceful degradation:** All Redis failures fall back to per-table `SimpleTable.get_simple_table_snapshot()` storage reads. Missing tables are skipped, not errored.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `supertable/meta_reader.py` | Read-only metadata reflection for the data catalog | `MetaReader`, `list_supers()`, `list_tables()`, `_leaf_to_snapshot_like()`, `_schema_to_dict()`, `_SUPER_META_CACHE` | Core catalog introspection API |

---

## 5. Detailed Functional Capabilities

### 5.1 List SuperTables in Organization

- **Description:** Discovers all SuperTables within an organization by scanning Redis for `meta:root` keys, filtered by RBAC.
- **Business purpose:** Catalog root-level discovery — "what datasets are available to me?"
- **Trigger:** `list_supers(organization=..., role_name=...)`.
- **Processing:**
  1. `_get_redis_items()` scans Redis with pattern `supertable:{org}:*:meta:root`.
  2. Extracts super_name from each key (3rd colon-delimited segment).
  3. Filters by `check_meta_access()` — silently excludes inaccessible supers.
  4. Returns sorted list.
- **Output:** `List[str]` — sorted super names.
- **Dependencies:** Redis (`SCAN`), `check_meta_access`.
- **Constraints:** `SCAN` is cursor-based and non-atomic — results may be inconsistent during concurrent writes.
- **Confidence:** Explicit.

### 5.2 List Tables in SuperTable

- **Description:** Discovers all SimpleTables within a SuperTable by scanning Redis for `meta:leaf:*` keys, filtered by RBAC.
- **Business purpose:** Dataset-level discovery — "what tables are in this dataset?"
- **Trigger:** `list_tables(organization=..., super_name=..., role_name=...)`.
- **Processing:** Same pattern as `list_supers` but with pattern `supertable:{org}:{super}:meta:leaf:*`, extracting the last segment as `table_name`.
- **Output:** `List[str]` — sorted table names.
- **Note:** Both the module-level `list_tables()` and the instance method `MetaReader.get_tables()` provide this functionality. `get_tables()` is the instance-method variant without sorting.
- **Confidence:** Explicit.

### 5.3 Table Schema Retrieval

- **Description:** Returns the schema (column names and types) for a specific table or the union schema across all tables.
- **Business purpose:** Schema introspection for query builders, UI column pickers, and AI agents generating SQL.
- **Trigger:** `meta_reader.get_table_schema(table_name=..., role_name=...)`.
- **Processing:**
  1. RBAC check.
  2. **If `table_name == super_name`** (asking for the super-level schema):
     a. Fetches all table names via `_get_all_tables()`.
     b. Bulk-fetches all leaf metadata via `MGET`.
     c. For each table: parses leaf → extracts snapshot → extracts schema → adds `(column, type)` tuples to a set.
     d. Falls back to `SimpleTable.get_simple_table_snapshot()` per table if Redis leaf is missing/unparseable.
     e. Returns the union of all schemas (deduped via set), sorted alphabetically.
  3. **If specific table:** Single `GET` for the leaf key, same parse/fallback logic, returns that table's schema.
- **Output:** `List[Dict[str, Any]]` — a single-element list containing a `{column_name: type}` dict. Returns `[{}]` on failure for a specific table, `None` on access denied.
- **Schema normalization:** `_schema_to_dict()` handles three formats: plain dict, list of `{name, type}` objects, and list of single-key dicts.
- **Confidence:** Explicit.

### 5.4 Table Statistics Retrieval

- **Description:** Returns snapshot-level statistics for one table or all tables, with heavy fields removed.
- **Business purpose:** Operational visibility — file counts, row counts, data sizes without the overhead of full snapshot data.
- **Trigger:** `meta_reader.get_table_stats(table_name=..., role_name=...)`.
- **Processing:**
  1. RBAC check.
  2. If `table_name == super_name`: iterates all tables, reads snapshots via `SimpleTable`, prunes `previous_snapshot`, `schema`, `location` keys.
  3. If specific table: reads single snapshot, prunes same keys.
- **Output:** `List[Dict[str, Any]]` — pruned snapshot dicts.
- **Note:** This method does NOT use the Redis-first optimization — it always reads from `SimpleTable.get_simple_table_snapshot()` (object storage). This contrasts with `get_table_schema()` and `get_super_meta()` which prefer Redis.
- **Confidence:** Explicit.

### 5.5 Comprehensive SuperTable Metadata ("Super Meta")

- **Description:** Assembles a complete overview of a SuperTable: all tables with individual and aggregate stats (files, rows, size), version, timestamps, and meta path.
- **Business purpose:** The single-call "dashboard API" — provides everything a UI or AI agent needs to understand the dataset at a glance.
- **Trigger:** `meta_reader.get_super_meta(role_name=...)`.
- **Processing:**
  1. RBAC check.
  2. Reads root metadata from Redis (`catalog.get_root()`) to get version.
  3. **Burst cache check:** If cache entry exists for `(org:super:role)`, matches `root_version`, and has not expired (TTL default 1s), returns cached result immediately.
  4. Scans all table names via `_get_all_tables()`.
  5. Bulk-fetches all leaf metadata via `MGET`.
  6. For each table: attempts to extract snapshot-like data from Redis leaf → falls back to `SimpleTable.get_simple_table_snapshot()`.
  7. Aggregates per-table stats: `files` (resource count), `rows` (sum of resource rows), `size` (sum of file sizes), `updated_utc`.
  8. Assembles result dict with aggregate totals and per-table breakdown.
  9. Stores result in burst cache keyed by `(org:super:role)` with `root_version` and TTL expiry.
  10. Logs detailed performance timings if `SUPERTABLE_DEBUG_TIMINGS` is enabled.
- **Output:** Dict with structure:
  ```json
  {
    "super": {
      "name": "...",
      "files": N, "rows": N, "size": N,
      "version": N, "updated_utc": N,
      "tables": [{"name", "files", "rows", "size", "updated_utc"}, ...],
      "meta_path": "redis://org/super"
    }
  }
  ```
- **Caching:** Version-keyed + TTL. If the root version changes (new write), cache is invalidated regardless of TTL. Default TTL is 1s — designed for burst absorption, not long-term caching.
- **Performance instrumentation:** When `SUPERTABLE_DEBUG_TIMINGS=1`, logs a structured timing string with: total_ms, access_ms, root_ms, scan_ms, mget_ms, table count, snapshot fallback count, max snapshot latency per table, cache hit flag.
- **Confidence:** Explicit.

### 5.6 Collect Simple Table Schema (Legacy)

- **Description:** Adds a table's schema to a mutable `set` passed by the caller.
- **Business purpose:** Likely used in older code paths for aggregated schema collection.
- **Trigger:** `meta_reader.collect_simple_table_schema(schemas=..., table_name=..., role_name=...)`.
- **Processing:** RBAC check → reads snapshot via `SimpleTable` → extracts schema → adds as tuple to the provided set.
- **Note:** Does NOT use the Redis-first optimization. Mutates a caller-provided set, unlike the other methods which return values. Appears to be a legacy pattern.
- **Confidence:** Explicit (code is clear, but the mutable-set-accumulation pattern suggests older code).

---

## 6. Classes, Functions, and Methods

### 6.1 Module-Level Utilities

| Function | Purpose | Input | Output | Importance |
|---|---|---|---|---|
| `_get_redis_items(pattern)` | SCAN-based Redis key discovery | Redis key pattern | `List[str]` of matching keys | Important — used by `list_supers`, `list_tables` |
| `_try_parse_leaf_meta(raw)` | Robust JSON decode of Redis leaf values | `bytes` / `str` / `None` | `Optional[Dict]` | Important — central parsing utility |
| `_leaf_to_snapshot_like(meta)` | Navigates multiple Redis payload shapes to extract snapshot data | `Dict` | `Optional[Dict]` with `resources` list | Critical — bridges Redis format evolution |
| `_schema_to_dict(schema_obj)` | Normalizes schema formats to `{name: type}` dict | `dict`, `list`, or other | `Dict[str, Any]` | Important — handles 3+ schema formats |
| `_prune_dict(d, keys_to_remove)` | Removes heavy keys from snapshot dicts | `Dict`, `Set[str]` | `Dict` (shallow copy) | Supporting |
| `_super_meta_cache_ttl_s()` | Reads cache TTL from env, defaults to 1.0s | None | `float` | Supporting |
| `list_supers(organization, role_name)` | Discovers all SuperTables in an org, RBAC-filtered | org, role | `List[str]` sorted | Critical — catalog root discovery |
| `list_tables(organization, super_name, role_name)` | Discovers all tables in a super, RBAC-filtered | org, super, role | `List[str]` sorted | Critical — table discovery |

### 6.2 `MetaReader` Class

| Attribute | Details |
|---|---|
| **Type** | Class |
| **Purpose** | Read-only metadata queries scoped to a specific SuperTable |
| **Constructor** | Creates `SuperTable` instance, instantiates `RedisCatalog` |
| **Instance state** | `self.super_table` (SuperTable), `self.catalog` (RedisCatalog) |
| **Importance** | Critical |

Methods:

| Method | Purpose | RBAC | Redis-First | Cache | Returns |
|---|---|---|---|---|---|
| `get_tables(role_name)` | List all tables, RBAC-filtered | Per-table `meta_access` | Yes (SCAN) | No | `List[str]` |
| `get_table_schema(table_name, role_name)` | Schema for one or all tables | `meta_access` | Yes (MGET/GET) | No | `List[Dict]` or `None` |
| `collect_simple_table_schema(schemas, table_name, role_name)` | Add schema to mutable set | `meta_access` | No | No | `None` (mutates `schemas`) |
| `get_table_stats(table_name, role_name)` | Snapshot stats, heavy fields pruned | `meta_access` | No | No | `List[Dict]` |
| `get_super_meta(role_name)` | Comprehensive super overview | `meta_access` | Yes (MGET) | Yes (burst) | `Dict` or `None` |
| `_get_all_tables()` | Internal: SCAN for all leaf keys | None | Yes | No | `List[str]` |

---

## 7. End-to-End Workflows

### 7.1 Catalog Discovery Workflow

1. **Trigger:** UI or agent calls `list_supers(organization="acme", role_name="analyst")`.
2. `_get_redis_items()` scans Redis with pattern `supertable:acme:*:meta:root`, count=1000 per batch.
3. Extracts super names from keys (e.g., `supertable:acme:sales_data:meta:root` → `sales_data`).
4. For each super name, calls `check_meta_access()`. Inaccessible supers silently excluded.
5. Returns sorted list: `["customers", "sales_data"]`.
6. Caller picks `sales_data`, calls `list_tables("acme", "sales_data", "analyst")`.
7. Same SCAN pattern with `supertable:acme:sales_data:meta:leaf:*`.
8. Extracts table names, RBAC-filters, sorts.
9. Returns: `["invoices", "line_items", "orders"]`.

### 7.2 Super Meta Assembly Workflow (with Burst Cache)

1. **Trigger:** Dashboard calls `meta_reader.get_super_meta(role_name="admin")`.
2. RBAC check passes.
3. Fetches root metadata from Redis: `catalog.get_root()` → `{"version": 42, "ts": 1709312456000}`.
4. **Cache check:** Looks up `acme:sales_data:admin` in `_SUPER_META_CACHE`.
   - If cached version == 42 AND not expired → **return cached result** (cache hit). Done.
   - Otherwise → continue (cache miss).
5. SCAN for all leaf keys → finds 3 tables.
6. MGET for all 3 leaf keys in one Redis round-trip.
7. For each table:
   - Parse leaf JSON → extract snapshot-like data with `_leaf_to_snapshot_like()`.
   - If Redis data sufficient: extract `resources` list, compute file count, row sum, size sum.
   - If Redis data missing: **fallback** to `SimpleTable.get_simple_table_snapshot()` (storage read). Log timing.
8. Aggregate totals: total_files=150, total_rows=2.5M, total_size=1.2GB.
9. Assemble result dict.
10. Store in `_SUPER_META_CACHE` with key `acme:sales_data:admin`, version=42, expires_at=now+1s.
11. If `SUPERTABLE_DEBUG_TIMINGS=1`: log structured timing line.
12. Return result.
13. **Next call within 1s with same role and no version change** → cache hit, skip steps 5–11.

### 7.3 Schema Introspection Workflow (Union Schema)

1. **Trigger:** Query builder calls `meta_reader.get_table_schema(table_name="sales_data", role_name="analyst")`.
2. `table_name == super_name` → triggers union-schema path.
3. SCAN for all leaf keys → finds tables: `["invoices", "line_items", "orders"]`.
4. MGET for all leaf keys.
5. For each table: parse leaf → extract snapshot → extract `schema` field → normalize with `_schema_to_dict()` → add `(column, type)` tuples to set.
6. Fallback to storage for any table where Redis leaf is missing/unparseable.
7. Union schema: `{"order_id": "INT64", "amount": "DOUBLE", "customer_name": "UTF8", "line_item_id": "INT64", ...}`.
8. Return `[{"amount": "DOUBLE", "customer_name": "UTF8", ...}]` (sorted alphabetically).

---

## 8. Data Model and Information Flow

### Core entities

| Entity | Storage | Description |
|---|---|---|
| **Root metadata** | Redis key `supertable:{org}:{super}:meta:root` | SuperTable-level metadata; contains `version` and `ts` (timestamp). Version is the cache invalidation key. |
| **Leaf metadata** | Redis key `supertable:{org}:{super}:meta:leaf:{table}` | Per-SimpleTable metadata. JSON payload with variable structure containing `resources`, `schema`, and snapshot data. |
| **SimpleTable snapshot** | Object storage (via `SimpleTable.get_simple_table_snapshot()`) | Authoritative snapshot data. Fallback when Redis leaf is missing or unparseable. Contains `resources` (file list), `schema`, `last_updated_ms`. |
| **Resource** | Nested in snapshot `resources[]` | Per-Parquet-file metadata: `rows`, `file_size`, and other fields. |
| **Schema** | Nested in snapshot or leaf | Column definitions. Multiple formats: `{name: type}` dict, `[{name, type}]` list, `[{col: type}]` single-key dicts. |

### Redis key anatomy

```
supertable:{org}:{super}:meta:root          ← SuperTable root (version, timestamp)
supertable:{org}:{super}:meta:leaf:{table}  ← Per-table snapshot/schema/resources
```

### Leaf metadata polymorphism (5+ shapes)

`_leaf_to_snapshot_like()` handles these Redis leaf structures to find `resources`:

| Shape | Path to `resources` |
|---|---|
| `{"resources": [...]}` | Direct |
| `{"payload": {"resources": [...]}}` | One level nested |
| `{"payload": {"snapshot": {"resources": [...]}}}` | Two levels nested |
| `{"data": {"resources": [...]}}` | Under `data` |
| `{"snapshot": {"resources": [...]}}` | Under `snapshot` |

This polymorphism strongly indicates **metadata format evolution** across multiple product versions, with backward-compatibility requirements.

### Information flow

```
Redis (authoritative fast path)
    │
    ├─ SCAN: key discovery (list supers/tables)
    ├─ MGET: bulk leaf fetch (schemas, super meta)
    └─ GET: single leaf fetch (single table schema)
    │
    │   ┌─── Redis leaf parseable? ───┐
    │   │                             │
    │   ▼ Yes                         ▼ No
    │   Use Redis data                Fallback to storage
    │                                     │
    │                                     ▼
    │                          SimpleTable.get_simple_table_snapshot()
    │                          (reads from object storage)
    │
    ▼
In-process burst cache (_SUPER_META_CACHE)
    │
    │   Version match + TTL valid?
    │   ▼ Yes → return cached
    │   ▼ No  → rebuild + cache
    │
    ▼
API response (Dict / List)
```

### Super Meta output schema

```json
{
  "super": {
    "name": "sales_data",
    "files": 150,
    "rows": 2500000,
    "size": 1200000000,
    "version": 42,
    "updated_utc": 1709312456000,
    "tables": [
      {
        "name": "orders",
        "files": 50,
        "rows": 1000000,
        "size": 400000000,
        "updated_utc": 1709312400000
      }
    ],
    "meta_path": "redis://acme/sales_data"
  }
}
```

---

## 9. Dependencies and Integrations

### Internal dependencies

| Module | Purpose | Usage |
|---|---|---|
| `supertable.rbac.access_control.check_meta_access` | RBAC enforcement for read/inspect operations | Every public method and function |
| `supertable.redis_catalog.RedisCatalog` | Redis metadata abstraction | Root/leaf reads, key scanning |
| `supertable.super_table.SuperTable` | SuperTable entity (creates storage backend) | Constructor dependency |
| `supertable.simple_table.SimpleTable` | SimpleTable snapshot reads | Fallback when Redis leaf is missing |

### Third-party dependencies

| Package | Purpose |
|---|---|
| `redis` (via `RedisCatalog.r`) | Key scanning (`SCAN`), bulk reads (`MGET`), single reads (`GET`) |

### External systems

| System | Role | Failure impact |
|---|---|---|
| **Redis** | Primary metadata source | Falls back to storage-based `SimpleTable` reads; `list_supers`/`list_tables` return empty lists |
| **Object storage** | Fallback metadata source (via `SimpleTable`) | If both Redis and storage fail, affected tables are silently skipped |

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SUPERTABLE_SUPER_META_CACHE_TTL_S` | `1.0` | Burst cache TTL for `get_super_meta()` results |
| `SUPERTABLE_DEBUG_TIMINGS` | disabled | Enables structured performance timing logs for `get_super_meta()` |

---

## 10. Architecture Positioning

### Where this code sits

```
┌─────────────────────────────────────────────────┐
│    API Layer / MCP Connector / Admin UI          │
│  (calls list_supers, list_tables, get_schema,   │
│   get_super_meta, get_table_stats)              │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│         meta_reader.py  [THIS CODE]              │
│  MetaReader + module-level discovery functions   │
└───────┬──────────────────────────┬──────────────┘
        │                          │
        ▼                          ▼
┌──────────────┐         ┌─────────────────────┐
│    Redis     │         │  Object Storage     │
│  (fast path) │         │  (fallback path)    │
│  SCAN/MGET   │         │  SimpleTable        │
│  root/leaf   │         │  snapshots          │
└──────────────┘         └─────────────────────┘
```

### Relationship to previously analyzed components

| Component | Relationship |
|---|---|
| **Staging Area** | Staging writes data that becomes SimpleTables, whose metadata is then reflected by MetaReader. Staging writes the leaf/root metadata that MetaReader reads. |
| **SuperPipe** | Pipe definitions reference `simple_name` — the same table names MetaReader discovers and reflects. |
| **MonitoringReader** | Similar architectural pattern (DuckDB-based reads), but for operational data not catalog metadata. |
| **MonitoringWriter** | Writes metrics that could reference tables MetaReader discovers (e.g., query plan monitoring per table). |
| **RBAC (`check_meta_access`)** | Shared across all components. MetaReader is the heaviest consumer, applying per-item RBAC filtering on every listing. |

### Boundary observations

- MetaReader is **purely read-only** — it never writes to Redis or storage.
- It has a **dual dependency on Redis** — for both key discovery (SCAN) and data retrieval (GET/MGET).
- The fallback to `SimpleTable.get_simple_table_snapshot()` means it implicitly depends on whatever storage backend `SuperTable` configures.
- The burst cache is **in-process only** — not shared across server instances. This is appropriate for absorbing rapid-fire calls within a single process but provides no cross-process benefit.
- The `meta_path` field in super meta output uses `redis://` pseudo-URIs, matching the pattern seen in `SuperPipe.create()` return values.

### Coupling considerations

- **Tight coupling to Redis key naming conventions:** The code directly constructs and parses Redis keys like `supertable:{org}:{super}:meta:leaf:{table}`. Any change to this key scheme requires coordinated changes.
- **Tight coupling to leaf metadata format:** `_leaf_to_snapshot_like()` must handle all historical and current formats. This is a deliberate backward-compatibility strategy but creates fragility.
- **Loose coupling to storage:** Accessed indirectly via `SimpleTable`, which abstracts the storage backend.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Users and systems need to know what data exists, what it looks like (schema), and how large it is — without running queries against the actual data. |
| **Why it matters** | Without a reflection/catalog layer, the platform is opaque. Users cannot build queries, AI agents cannot generate SQL, dashboards cannot render catalog views, and administrators cannot monitor dataset health. |
| **Revenue relevance** | Directly revenue-enabling. The MCP connector (AI agent interface) and any query builder UI depend on this code to function. It is also the entry point for the Supertable `list_tables` call referenced in user preferences. |
| **Efficiency relevance** | The Redis-first strategy with MGET bulk fetching and burst caching minimizes latency for high-frequency reflection calls. The fallback-to-storage design ensures correctness even when Redis is incomplete. |
| **What would be lost without it** | No catalog discovery. No schema introspection. No table stats. Query builders, dashboards, AI agents, and admin tools would all be non-functional. |
| **Differentiating vs commodity** | The multi-format tolerance (`_leaf_to_snapshot_like`) and performance instrumentation suggest a production-hardened system. The burst cache with version-based invalidation is a sophisticated optimization. The union-schema capability (querying across all tables) is particularly valuable for wide analytical workloads. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate-high. The multi-format leaf parsing, dual Redis/storage paths, burst cache logic, and SCAN-based discovery each add complexity. Combined, they form a carefully layered system. |
| **Architectural maturity** | High. The Redis-first/storage-fallback pattern, burst caching with version invalidation, and detailed performance instrumentation all indicate production-hardened code. |
| **Maintainability** | Moderate. `_leaf_to_snapshot_like()` is complex due to format evolution — any new format requires adding another parsing branch. The module has both class methods and module-level functions that partially overlap (`get_tables` vs `list_tables`). |
| **Extensibility** | Good. New metadata fields can be added to leaf payloads and consumed here. New reflection endpoints can be added to `MetaReader`. |
| **Operational sensitivity** | **High.** This is likely the most-called code in the system (every UI load, every AI query, every schema check). The burst cache and MGET optimization are essential for acceptable latency. |
| **Performance** | Carefully optimized: MGET for bulk leaf fetching (1 round-trip vs N), burst cache (1s TTL) for repeated calls, SCAN with count=1000 for efficient key iteration. Debug timings allow production profiling. |
| **Reliability** | Strong. Every Redis failure path has a fallback (storage read or empty result). Missing tables are skipped, not errored. The burst cache ensures that even Redis outages only affect the first call per TTL window. |
| **Security** | RBAC enforced per-item on every public method and function. Inaccessible items are silently excluded. Access denied returns `None` or empty lists, not errors (information hiding). |
| **Testing clues** | The `_try_parse_leaf_meta()` function handles bytes, strings, empty strings, and None — suggesting it was built to handle real-world Redis data diversity. No test files provided. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Redis keys follow the pattern `supertable:{org}:{super}:meta:root` and `supertable:{org}:{super}:meta:leaf:{table}`.
- Leaf metadata can have at least 5 different JSON structures; all must be tolerated.
- Schema can be represented as a dict, a list of `{name, type}` objects, or a list of single-key dicts.
- `get_super_meta()` uses a version-keyed, TTL-bounded, in-process burst cache.
- Default cache TTL is 1 second, configurable via environment variable.
- All public methods enforce RBAC via `check_meta_access()`.
- Per-item RBAC failures are silently excluded (not raised).
- `get_table_stats()` does NOT use Redis-first optimization; it always reads from `SimpleTable`.
- `get_super_meta()` has detailed performance instrumentation gated by `SUPERTABLE_DEBUG_TIMINGS`.
- `resources` is a list of per-file metadata objects containing `rows` and `file_size` fields.
- The root metadata has a `version` field used for cache invalidation.
- The code handles both bytes and string Redis key responses (compatibility with different Redis client configurations).

### Reasonable Inferences

- **This is the primary metadata API** consumed by the MCP connector, dashboard, and query builder. The `list_tables` function name matches the Supertable MCP protocol described in user preferences.
- **`SuperTable` is a container for `SimpleTables`** — the hierarchical data model is SuperTable (dataset) → SimpleTable (table) → Resources (Parquet files).
- **The leaf metadata is written by the data ingestion pipeline** (staging → merge → snapshot update → Redis leaf update). MetaReader only reads it.
- **The `version` field in root metadata** is incremented on each successful write/merge operation, serving as a monotonic version counter.
- **The `meta_path: redis://` field** is used by downstream systems to understand where metadata lives (as opposed to storage-only metadata).
- **The burst cache exists because `get_super_meta()` is called very frequently** — likely on every dashboard page load, every AI agent session start, and every schema reflection call.

### Unknown / Not Visible in Provided Snippet

- How leaf metadata is written/updated (which component writes `meta:leaf:*` keys).
- The full structure of `SimpleTable.get_simple_table_snapshot()` return value.
- How `SuperTable` is constructed and what storage backend it configures.
- Whether there are other metadata key types beyond `root` and `leaf` (e.g., `meta:staging:*`, `meta:pipe:*` — these are implied by previously analyzed staging/pipe code).
- The RBAC role hierarchy and what `meta_access` specifically checks.
- Whether the burst cache grows unboundedly (no eviction logic visible — entries are added but never removed unless overwritten).
- How many SimpleTables a typical SuperTable contains (affects SCAN and MGET performance).
- Whether there is a separate metadata write path that updates Redis leaves and roots.
- The `StageInfo` dataclass from `staging_area.py` could be related but is not referenced here.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `Supertable MetaReader (Metadata Reflection API)`
- **Alternative names / aliases:** meta reader, catalog reflection, schema introspection, list_supers, list_tables, get_table_schema, get_super_meta, get_table_stats, metadata API
- **Main responsibilities:** (1) Catalog discovery (list supers, list tables), (2) Schema introspection (per-table and union), (3) Table statistics retrieval, (4) Comprehensive super meta assembly with burst caching
- **Important entities:** `MetaReader`, `SuperTable`, `SimpleTable`, root metadata, leaf metadata, resources, schema, burst cache (`_SUPER_META_CACHE`)
- **Important workflows:** catalog discovery (SCAN → RBAC filter → sorted list), super meta assembly (root fetch → cache check → MGET → parse → aggregate → cache store), schema introspection (MGET/GET → parse → normalize → union)
- **Integration points:** Redis (SCAN, MGET, GET), object storage (via `SimpleTable` fallback), RBAC (`check_meta_access`), `SuperTable` entity, `RedisCatalog`
- **Business purpose keywords:** catalog, reflection, schema introspection, table discovery, data profiling, metadata API, dashboard data, AI agent catalog
- **Architecture keywords:** Redis-first, storage fallback, burst cache, version-keyed invalidation, RBAC-per-item filtering, multi-format tolerance, bulk fetch, SCAN-based discovery
- **Follow-up files for completeness:**
  - `supertable/super_table.py` — SuperTable entity (hierarchical parent)
  - `supertable/simple_table.py` — SimpleTable entity (snapshot reads, schema, resources)
  - `supertable/redis_catalog.py` — RedisCatalog implementation (key structures, read/write methods)
  - RBAC module — `supertable/rbac/access_control.py` (role model, permission checks)
  - API layer that exposes MetaReader to HTTP/MCP clients
  - Metadata write path — which component updates `meta:leaf:*` and `meta:root` keys

---

## 15. Suggested Documentation Tags

`metadata-reflection`, `catalog-api`, `schema-introspection`, `table-discovery`, `super-meta`, `burst-cache`, `version-invalidation`, `redis-first`, `storage-fallback`, `RBAC-filtering`, `MGET-bulk-fetch`, `SCAN-discovery`, `multi-format-tolerance`, `performance-instrumentation`, `read-only`, `data-catalog`, `union-schema`, `table-stats`, `backward-compatibility`, `leaf-metadata`, `root-metadata`, `SuperTable`, `SimpleTable`

---

## Merge Readiness

- **Suggested canonical name:** `Supertable MetaReader (Metadata Reflection API)`
- **Standalone or partial:** **Mostly standalone** for the read side of metadata, but understanding is incomplete without the write side (who creates/updates leaf and root metadata).
- **Related components to merge later:**
  - `SuperTable` and `SimpleTable` class definitions — the entities MetaReader reflects
  - `RedisCatalog` — the full Redis key structure and CRUD methods
  - RBAC module — the permission model
  - Metadata write path — what creates/updates `meta:root` and `meta:leaf:*`
  - API/MCP layer — how MetaReader is exposed to clients
  - Previously analyzed: `Supertable Monitoring Subsystem` (shares Redis/storage infra), `Supertable Staging Area & Pipe Management` (stages data that becomes SimpleTables)
- **What would most improve certainty:**
  1. `SimpleTable` class — `get_simple_table_snapshot()` return structure
  2. `RedisCatalog` — full Redis key schema and methods
  3. The metadata write path (who writes leaf/root keys and when)
  4. API layer exposing these functions to HTTP/MCP clients
- **Recommended merge key phrases:** `MetaReader`, `list_supers`, `list_tables`, `get_table_schema`, `get_super_meta`, `get_table_stats`, `meta:root`, `meta:leaf`, `leaf_to_snapshot`, `burst cache`, `catalog reflection`, `schema introspection`
