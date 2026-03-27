# Feature / Component Reverse Engineering

## 1. Executive Summary

This code implements the **Tables Command Center** — the primary admin/operator UI for browsing, inspecting, managing, and assessing data quality across all logical tables within a SuperTable data platform instance.

- **Main responsibility**: Provide a full-featured web interface for table discovery, schema inspection, per-file statistics exploration, column-level data quality analysis, per-table configuration management (compaction/memory limits), and table deletion.
- **Likely product capability**: The "Tables" page is a core navigation hub in the SuperTable admin console (called "Reflection UI"), enabling operators to understand the shape, health, and operational state of every data table managed by the platform.
- **Business purpose**: Enables data engineers, platform operators, and analysts to monitor data freshness, discover schema details, assess data quality (null rates, uniqueness, cardinality), tune storage-engine knobs per-table, and perform destructive administrative actions — all from a single pane of glass.
- **Technical role**: A Jinja2-rendered HTML template (`tables.html`) backed by a FastAPI route module (`tables.py`) that reads metadata from Redis, delegates to a `MetaReader` abstraction for schema/stats, and exposes JSON APIs consumed by the page's JavaScript. The backend also depends on shared infrastructure from `common.py` (auth, sessions, Redis, routing).

---

## 2. Functional Overview

### Product Capabilities Enabled

The Tables Command Center enables users to:

1. **Discover and browse all logical tables** within a selected SuperTable, including system tables (prefixed `__`), with search/filter, freshness indicators, and row/size summaries.
2. **Inspect table schema** — column names, data types, rendered with color-coded type pills (string, number, date, bool, other).
3. **Run and view data quality checks** — per-column completeness (null rate), uniqueness, distinct counts, min/max values, and anomaly detection. Quality can be computed via two paths: a direct SQL-based aggregation query, or a backend-managed quality engine with caching.
4. **Explore per-file statistics** — parquet-level file sizes, row counts, column counts, snapshot versions, and column min/max ranges per file. Includes a value search feature that finds which files/columns contain a given value within their min/max range.
5. **Manage per-table configuration** — override global defaults for `max_memory_chunk_size` (bytes loaded per merge chunk) and `max_overlapping_files` (compaction trigger threshold). Null values inherit the global default.
6. **Delete tables** — destructive operation with confirmation prompt (type-to-confirm pattern), removing both Redis metadata and storage artifacts.
7. **Switch SuperTables and roles** — top bar provides selectors for the active SuperTable instance and RBAC role, with role-aware data access throughout all APIs.
8. **View aggregate health** — right sidebar shows a health ring (freshness-weighted score), total tables/rows/size/files.

### Target Users / Actors

- **Data engineers / platform operators**: Primary audience — configure compaction, monitor freshness, inspect schemas, diagnose data quality issues.
- **Data analysts / consumers**: Inspect available tables, understand column types, verify data completeness before querying.
- **Superadmins**: Full access including table deletion and config changes.
- **RBAC roles**: The system passes `role_name` throughout, meaning schema/stats/quality results may be filtered by role permissions.

### Business Workflows Supported

- **Data catalog browsing**: Discover what tables exist, their sizes, freshness.
- **Schema exploration**: Understand table structure before writing queries.
- **Data quality assessment**: Proactive identification of NULL-heavy columns, low-cardinality fields, primary key candidates.
- **Operational tuning**: Adjust compaction and merge-chunk settings per table.
- **Data lifecycle management**: Delete obsolete tables.

### Strategic Significance

This page is a **core feature** of the SuperTable admin console. It is the entry point for table-level observability and the bridge between raw storage metadata (Redis keys, parquet files) and human-understandable table semantics. Without it, operators would need direct Redis/storage access to understand data shape.

---

## 3. Technical Overview

### Architectural Style

- **Server-rendered SPA hybrid**: The page is server-rendered via Jinja2 (FastAPI + `TemplateResponse`) for initial state injection (org, super, token, tenants), but operates as a client-side SPA thereafter, fetching all data via JSON APIs.
- **3-column "Command Center" layout**: Left panel (table list), center panel (detail view with tabs), right panel (summary/health).
- **Overlay pattern**: Schema, stats, and limits dialogs use modal overlays (currently partially migrated into inline tabs — both patterns coexist).

### Major Modules

| Module | Role |
|--------|------|
| `tables.html` | Jinja2 template — full UI: CSS, JS, HTML structure, all client-side logic |
| `tables.py` | FastAPI route registration — page endpoint + 7 JSON APIs |
| `common.py` | Shared infrastructure — auth, sessions, Redis, routing, settings |

### Key Control Flows

1. **Page load** → `tables.py:tables_page()` renders template with Jinja context → JS fires `api('/reflection/super')` → `onSuperMetaLoaded()` populates both the left-panel table list and the legacy DataTable.
2. **Table selection** → `ccSelectTable()` → populates detail header + metrics → triggers parallel async calls: `ccLoadSchema()`, `ccLoadStats()`, `ccLoadSettings()`.
3. **Quality check** → `ccRunQualityBackend()` POSTs to `/reflection/quality/run` → polls `/reflection/quality/latest` → renders cached results. Falls back to `ccRunQualityCheck()` which builds and executes a raw SQL aggregation query via `/reflection/execute`.
4. **Settings save** → `ccSaveSettings()` PUTs to `/reflection/table/config`.
5. **Table delete** → `deleteTable()` → prompt confirmation → DELETE `/reflection/table`.

### Key Data Flows

```
Redis (meta:root / meta:leaf keys)
   │
   ▼
MetaReader (get_super_meta, get_table_schema, get_table_stats)
   │
   ▼
FastAPI JSON APIs (/reflection/super, /schema, /stats, /table/config)
   │
   ▼
JavaScript (fetch → api() helper → DOM rendering)
```

### Major Design Patterns

- **Dependency injection via function parameters**: `attach_tables_routes()` receives all shared dependencies (auth, templates, catalog, etc.) as callable/object parameters — no global imports from `common.py` within `tables.py`.
- **Dual rendering**: Both a "Command Center" (cc-prefixed) 3-column layout and a legacy DataTable coexist in the same page, with the CC hooks wrapping the original `onSuperMetaLoaded`.
- **Graceful fallback**: Quality check tries the backend DQ engine first; on failure, falls back to a client-generated SQL aggregation query.
- **Session + token auth**: Every API checks `is_authorized(request)` (cookie-based session) and depends on `admin_guard_api` (likely token-based guard).

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Functions/Classes | Relevance |
|------|----------------------|--------------------------|-----------|
| `tables.html` | Full client-side UI — layout, styling, all JavaScript logic for table browsing, schema, stats, quality, settings, delete | `ccSelectTable`, `ccLoadSchema`, `ccLoadStats`, `ccLoadSettings`, `ccRunQualityCheck`, `ccRunQualityBackend`, `ccLoadQualityFromCache`, `deleteTable`, `saveLimits`, `ccUpdateSummary`, `renderTables`, `searchStatsByValue` | Core — this IS the feature UI |
| `tables.py` | FastAPI backend — page rendering + JSON API endpoints | `attach_tables_routes()`, `tables_page()`, `api_list_supers()`, `api_get_super_meta()`, `api_get_table_schema()`, `api_get_table_stats()`, `api_delete_table()`, `api_get_table_config()`, `api_put_table_config()` | Core — server-side API layer |
| `common.py` | Shared infrastructure — settings, Redis, auth, session management, routing | `Settings`, `get_session()`, `is_logged_in()`, `is_superuser()`, `_is_authorized()`, `discover_pairs()`, `resolve_pair()` | Foundational dependency |

---

## 5. Detailed Functional Capabilities

### 5.1 Table Discovery and Browsing

- **Description**: Lists all logical tables (Redis "leaves") within a selected SuperTable, with row count, size, file count, and freshness indicators.
- **Business purpose**: Data catalog — lets users see what data is available.
- **Trigger/input**: Page load or SuperTable selection change.
- **Processing**: Backend scans Redis keys matching `supertable:{org}:{sup}:meta:leaf:*`, or uses `catalog.scan_leaf_items()`. Frontend receives table array from `/reflection/super` API, renders both a left-panel list (CC layout) and a DataTable (legacy).
- **Output**: Sorted table list with name, row count, size, freshness dot (green < 1hr, amber < 24hr, red > 24hr).
- **Dependencies**: Redis (key scanning), MetaReader, catalog.
- **Constraints**: Tables prefixed `__` are system tables, hidden by default. Toggle reveals them.
- **Risks**: Redis SCAN may be slow with many keys. No explicit rate limiting visible.
- **Confidence**: Explicit.

### 5.2 Schema Inspection

- **Description**: Fetches and displays column names and data types for a selected table.
- **Business purpose**: Schema discovery — understand table structure before querying.
- **Trigger**: Table selection in left panel (auto-loads), or "Schema" button click.
- **Processing**: JS calls `/reflection/schema` → backend `MetaReader.get_table_schema(table, role)` → returns schema (array of objects or key-value map). Frontend `extractSchemaPairs()` normalizes to `[[col, type], ...]`.
- **Output**: Tabular display with column index, name, color-coded type pill.
- **Dependencies**: MetaReader, RBAC role filtering.
- **Constraints**: Schema format may vary (array of dicts or flat dict); `extractSchemaPairs()` handles both.
- **Confidence**: Explicit.

### 5.3 Data Quality Analysis

- **Description**: Computes per-column quality metrics: completeness (non-null rate), uniqueness, distinct count, min/max, and flags issues.
- **Business purpose**: Proactive data quality monitoring — identify NULL-heavy columns, low-cardinality fields, primary key candidates.
- **Trigger**: "Run quality check" button on the Quality tab.
- **Processing (backend path — preferred)**: POST to `/reflection/quality/run?table=...&mode=quick` → backend runs quality checks asynchronously → JS polls `/reflection/quality/latest` every 2s for up to 30s → renders cached results including anomaly severity pills.
- **Processing (SQL fallback)**: Builds a single `SELECT COUNT(*), COUNT(col), COUNT(DISTINCT col), MIN(col), MAX(col) FROM super.table` query → POSTs to `/reflection/execute` → computes quality score client-side.
- **Output**: Summary grid (total rows, avg completeness, avg uniqueness, issue count), per-column table with completeness bars, null %, distinct count, uniqueness %, min, max, issues/status.
- **Quality score formula**: `score = round(avgComplete * 0.7 + bonus)` where bonus is 30 if avgComplete > 90, 20 if > 70, else 10. Max is approximately 100.
- **Issue detection rules**:
  - Completeness < 50% → "High NULL rate" (error)
  - Completeness < 80% → "Moderate NULLs" (warning)
  - Uniqueness > 99.5% and > 100 rows and name doesn't contain "id" → "Primary key candidate?"
  - Uniqueness < 0.1% and > 100 rows and string type → "Very low cardinality — consider ENUM"
  - Uniqueness = 100% and > 10 rows → "All values unique"
- **Dependencies**: `/reflection/execute` endpoint (for SQL path), `/reflection/quality/run` + `/reflection/quality/latest` (for backend path).
- **Risks**: SQL fallback reads all data (potentially expensive). Backend path has 30s timeout before showing "still running" message.
- **Confidence**: Explicit.

### 5.4 Per-File Statistics Explorer

- **Description**: Shows per-file breakdown of a table: file sizes, row counts, column counts, snapshot version, and column-level min/max statistics.
- **Business purpose**: Storage-level observability — understand how data is physically distributed across parquet files.
- **Trigger**: Table selection (Stats tab), or "Stats" button.
- **Processing**: JS calls `/reflection/stats` → backend `MetaReader.get_table_stats(table, role)` → frontend `aggregateStats()` and `parseResourcesFromStats()` parse the payload.
- **Output**: Summary metrics (files, rows, columns, snapshot version), file-level DataTable, column min/max table for selected file.
- **Value search feature**: `searchStatsByValue()` lets users type a number/date/string and find all columns across files where the value falls within the column's min/max range. Supports numeric, date, and string comparison.
- **Dependencies**: MetaReader, DataTables jQuery plugin.
- **Confidence**: Explicit.

### 5.5 Per-Table Configuration Management

- **Description**: Read and update per-table compaction/merge configuration overrides.
- **Business purpose**: Operational tuning — allow different tables to have different memory and compaction thresholds based on their data characteristics.
- **Trigger**: "Settings" tab in detail view, or gear icon on table row.
- **Processing**: GET `/reflection/table/config` to load current config. PUT `/reflection/table/config` to merge overrides.
- **Configurable fields**:
  - `max_memory_chunk_size` — bytes loaded per merge chunk (displayed as MB in UI; stored as bytes). Null = inherit global `MAX_MEMORY_CHUNK_SIZE`.
  - `max_overlapping_files` — compaction trigger threshold. Null = inherit global `MAX_OVERLAPPING_FILES`.
- **Validation**: Backend enforces positive integers; frontend validates before submit.
- **Output**: Success/error status message. Settings are persisted via `catalog.set_table_config()`.
- **Dependencies**: catalog (Redis-backed config store).
- **Confidence**: Explicit.

### 5.6 Table Deletion

- **Description**: Permanently delete a logical table (leaf) from Redis metadata and underlying storage.
- **Business purpose**: Data lifecycle management — remove obsolete or test tables.
- **Trigger**: "Delete" button on table row or detail view.
- **Processing**: Browser `prompt()` requires typing exact table name → DELETE `/reflection/table` → backend `SimpleTable.delete(role_name=role)`.
- **Output**: Table list refreshes on success; error modal on failure.
- **Risks**: Destructive and irreversible. No soft-delete or undo visible.
- **Confidence**: Explicit.

### 5.7 RBAC-Aware Data Access

- **Description**: All API calls pass `role_name` parameter, and the backend resolves it via explicit param → session cookie fallback.
- **Business purpose**: Enforce role-based access control — different roles may see different schemas/data.
- **Processing**: `_resolve_role()` checks param first, then session `role_name`. MetaReader methods accept role. Frontend fetches available roles via `/reflection/rbac/roles` to populate the role selector.
- **Confidence**: Explicit.

### 5.8 SuperTable and Role Switching

- **Description**: Top bar provides selectors for switching the active SuperTable instance and RBAC role without full page reload (for role) or with navigation (for SuperTable).
- **Business purpose**: Multi-tenancy — operators manage multiple SuperTable instances from one console.
- **Processing**: SuperTable change navigates to `/reflection/tables?org=...&sup=...`. Role change updates global JS state and re-renders current table detail.
- **Confidence**: Explicit.

### 5.9 Deep-Linking

- **Description**: URL query parameters `?table=...&tab=...` allow direct linking to a specific table and tab.
- **Business purpose**: Cross-page navigation — e.g., the quality page can link back to a specific table's quality tab.
- **Processing**: IIFE polls `window.__cc_tables` every 200ms (up to 50 attempts / 10s) until the target table is found, then selects it and optionally switches to the target tab.
- **Confidence**: Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 Backend — `tables.py`

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|------|------|---------|------------|---------|-------------|------------|
| `attach_tables_routes()` | Function | Register all table-related routes on a FastAPI router | router, templates, is_authorized, no_store, get_provided_token, discover_pairs, resolve_pair, inject_session_into_ctx, list_users, fmt_ts, catalog, admin_guard_api, get_session | None | Registers 8 route handlers on router | Critical |
| `_resolve_role()` | Closure | Resolve RBAC role from param or session | request, role_name, user_hash | str | None | Important |
| `_get_redis_items()` | Closure | SCAN Redis keys by pattern | pattern: str | List[str] | Redis I/O | Supporting |
| `list_supers()` | Closure | List SuperTable names for an org | organization: str | List[str] | Redis I/O | Important |
| `_list_leaves()` | Closure | List logical tables with search/pagination | org, sup, q, page, page_size | Dict (total, items, etc.) | Redis I/O | Important |
| `tables_page()` | Route (GET `/reflection/tables`) | Render the tables.html page | request, org, sup, page, page_size | HTMLResponse | Template rendering | Critical |
| `api_list_supers()` | Route (GET `/reflection/supers`) | JSON: list SuperTable names | request, organization | JSON {ok, supers} | Redis read | Important |
| `api_get_super_meta()` | Route (GET `/reflection/super`) | JSON: full super-level metadata including table list | request, organization, super_name, role_name | JSON {ok, meta} | MetaReader read | Critical |
| `api_get_table_schema()` | Route (GET `/reflection/schema`) | JSON: table column schema | request, organization, super_name, table, role_name | JSON {ok, schema} | MetaReader read | Critical |
| `api_get_table_stats()` | Route (GET `/reflection/stats`) | JSON: per-file table statistics | request, organization, super_name, table, role_name | JSON {ok, stats} | MetaReader read | Important |
| `api_delete_table()` | Route (DELETE `/reflection/table`) | Delete a table permanently | request, organization, super_name, table, role_name | JSON {ok} | Deletes from Redis + storage | Critical |
| `api_get_table_config()` | Route (GET `/reflection/table/config`) | Read per-table config | request, organization, super_name, table | JSON {ok, config} | catalog read | Important |
| `api_put_table_config()` | Route (PUT `/reflection/table/config`) | Merge per-table config overrides | request, organization, super_name, table, body | JSON {ok, config} | catalog write | Important |

### 6.2 Frontend — `tables.html` (JavaScript)

| Name | Type | Purpose | Importance |
|------|------|---------|------------|
| `api(path, opts)` | Function | Central fetch wrapper; injects admin token header; parses JSON; throws on non-OK | Critical |
| `onSuperMetaLoaded(org, superName, superMeta)` | Callback | Entry point after super metadata loads; populates table list + DataTable | Critical |
| `ccRenderTableList(filter)` | Function | Renders the left-panel table list with filtering and system table visibility | Critical |
| `ccSelectTable(name, idx)` | Function | Selects a table: populates detail header, tabs, triggers schema/stats/settings loads | Critical |
| `ccLoadSchema(tableName)` | Async function | Fetches schema API, renders schema table + prepares quality tab | Critical |
| `ccRunQualityBackend(tableName)` | Async function | Triggers backend quality check, polls for results, falls back to SQL | Important |
| `ccRunQualityCheck(tableName)` | Async function | Builds and executes SQL aggregation query for quality metrics | Important |
| `ccLoadQualityFromCache(tableName)` | Async function | Loads cached quality results from DQ backend | Important |
| `ccLoadStats(tableName)` | Async function | Fetches stats API, renders per-file statistics | Important |
| `ccLoadSettings(tableName)` | Async function | Fetches table config API, renders settings form | Important |
| `ccSaveSettings(tableName)` | Async function | Validates and PUTs config overrides | Important |
| `ccUpdateSummary(tables)` | Function | Updates right-panel summary: health ring, totals | Important |
| `ccRefreshMeta()` | Function | Re-fetches super metadata and re-renders | Supporting |
| `renderTables(superName, tables)` | Function | Legacy DataTable rendering (hidden but functional) | Supporting |
| `deleteTable(simple)` | Function | Confirm-and-DELETE a table | Important |
| `searchStatsByValue(query, searchAcrossAll)` | Function | Searches column min/max ranges for a given value | Supporting |
| `goExecuteWithLeaf(simple)` | Function | Navigates to query execution page with pre-selected table | Supporting |
| `ccFreshness(updated_utc)` | Function | Computes freshness category: fresh (<1hr), aging (<24hr), stale | Supporting |
| `extractSchemaPairs(schema)` | Function | Normalizes schema response to `[[col, type], ...]` pairs | Supporting |
| `aggregateStats(statsPayload)` | Function | Aggregates per-resource stats into summary | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Page Load → Table List Population

1. Browser navigates to `/reflection/tables?org=...&sup=...`
2. `tables_page()` checks `is_authorized(request)` — redirects to login if unauthorized
3. `discover_pairs()` lists all org/super pairs; `resolve_pair()` picks the selected one
4. `_list_leaves()` scans Redis for leaf metadata (pagination supported but not used by frontend)
5. Jinja2 renders `tables.html` with context: `tenants`, `sel_org`, `sel_sup`, `has_tenant`, `token`
6. JS IIFE on load fetches `/reflection/super` via `api()` with a 3-second safety timer
7. `onSuperMetaLoaded()` fires → `ccRenderTableList()` builds left panel → `ccUpdateSummary()` updates health ring
8. If `?table=X` query param present, polling IIFE auto-selects that table (+ optional `?tab=Y`)

### 7.2 Schema + Quality Inspection

1. User clicks a table in left panel → `ccSelectTable(name, idx)`
2. Detail view appears with Schema tab active
3. `ccLoadSchema(tableName)` fetches `/reflection/schema` → builds schema table → stores `__cc_schemaPairs[tableName]`
4. Quality tab is pre-built with column count, row count, freshness metrics, and "Run quality check" button
5. `ccLoadQualityFromCache(tableName)` auto-fires to load cached backend results
6. If user clicks "Run quality check" → `ccRunQualityBackend()`:
   - POST `/reflection/quality/run?table=...&mode=quick`
   - On success: poll `/reflection/quality/latest` every 2s
   - On backend failure: fallback to `ccRunQualityCheck()` (SQL-based)
7. Results render: summary grid + per-column table with issue annotations

### 7.3 Table Deletion

1. User clicks "Delete" → `deleteTable(simple)` fires
2. Browser `prompt()` asks user to type the table name
3. If input matches exactly → DELETE `/reflection/table?organization=...&super_name=...&table=...`
4. Backend: `SimpleTable(super_table, simple_name).delete(role_name=role)` removes data from Redis + storage
5. On success: `loadTenantSuper(sup)` refreshes the table list
6. On failure: `showModal()` displays error

### 7.4 Per-Table Config Save

1. User navigates to Settings tab → `ccLoadSettings()` GETs `/reflection/table/config`
2. Current config values populate input fields (MB for chunk size, count for files)
3. User edits values → clicks Save → `ccSaveSettings()`:
   - Validates: positive numbers or blank (null = inherit default)
   - Converts MB to bytes for `max_memory_chunk_size`
   - PUTs to `/reflection/table/config` with JSON body
4. Backend: reads existing config via `catalog.get_table_config()`, merges new values, writes via `catalog.set_table_config()`
5. Success/error status displayed inline

---

## 8. Data Model and Information Flow

### Core Entities

| Entity | Shape | Source | Description |
|--------|-------|--------|-------------|
| SuperTable meta | `{ tables: [{name, rows, size, files, updated_utc}, ...], rows, size, files }` | MetaReader → `/reflection/super` | Aggregate metadata for a SuperTable instance |
| Table (leaf) | `{name, rows, size, files, updated_utc}` | Redis key `supertable:{org}:{sup}:meta:leaf:{name}` | Individual table metadata |
| Schema | Array of `{col: type}` or flat `{col: type}` dict | MetaReader → `/reflection/schema` | Column definitions |
| Stats | `{ resources: [{file, file_size, rows, columns, stats: {col: {min, max}}}], snapshot_version, last_updated_ms }` | MetaReader → `/reflection/stats` | Per-file parquet statistics |
| Table config | `{ max_memory_chunk_size: int|null, max_overlapping_files: int|null }` | catalog → `/reflection/table/config` | Per-table compaction/memory overrides |
| Quality result (backend) | `{ latest: { checked_at, quality_score, row_count, parsed: { total, columns: {col: {null_rate, uniqueness, distinct, min, max, status}} } }, anomalies: [{check_id, message, severity}] }` | `/reflection/quality/latest` | Cached quality check results |

### Key Transformations

- **Schema normalization**: `extractSchemaPairs(schema)` handles both `[{col: type}]` and `{col: type}` formats → normalizes to `[[col, type], ...]`.
- **Stats aggregation**: `aggregateStats(statsPayload)` sums rows/files across resources, extracts snapshot_version from first resource.
- **Quality score computation** (SQL path): Weighted formula from average completeness across columns.
- **MB ↔ bytes conversion**: Settings UI shows MB; API stores bytes. Conversion: `bytes = MB * 1024 * 1024`.

### State Management

- `window.__cc_tables`: Cached table array from latest super meta load.
- `window.__cc_selectedTable`: Currently selected table name.
- `window.__cc_schemaPairs`: Map of `tableName → [[col, type], ...]` for quality check query building.
- `window.__cc_showSystemTables`: Toggle state for `__*` system table visibility.
- `window.__st_active_role`: Current RBAC role.
- `sessionStorage` cache key `cc_quality:{org}:{sup}:{table}` (for client-side quality score caching, minimal usage).

---

## 9. Dependencies and Integrations

### Internal Dependencies

| Dependency | Used For | Module |
|-----------|----------|--------|
| `supertable.redis_catalog.RedisCatalog` | Redis key scanning, table config read/write | `tables.py` |
| `supertable.meta_reader.MetaReader` | Schema, stats, super-level metadata reading | `tables.py` |
| `supertable.storage.storage_factory.get_storage` | Imported but not directly used in visible code | `tables.py` |
| `supertable.simple_table.SimpleTable` | Table deletion | `tables.py` (lazy import) |
| `supertable.super_table.SuperTable` | Table deletion (parent reference) | `tables.py` (lazy import) |
| `common.py` shared functions | Auth, sessions, routing, Redis, settings, template engine | `tables.py` (injected via `attach_tables_routes`) |

### External Dependencies

| Dependency | Used For |
|-----------|----------|
| FastAPI + Jinja2Templates | HTTP routing, template rendering |
| Redis | Metadata storage (keys: `supertable:{org}:{sup}:meta:*`) |
| jQuery 3.6.0 | DOM manipulation, DataTables integration |
| DataTables 1.13.6 | Sortable/filterable table UI (legacy path) |
| Bootstrap 4.5.2 | Some CSS utilities, modal support |
| Font Awesome 6.5.1 | Icons throughout UI |
| Inter (Google Fonts) | Typography |

### API Endpoints Consumed by Frontend

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/reflection/super` | GET | Fetch super metadata + table list |
| `/reflection/supers` | GET | List available SuperTable names |
| `/reflection/schema` | GET | Table schema |
| `/reflection/stats` | GET | Per-file table statistics |
| `/reflection/table` | DELETE | Delete a table |
| `/reflection/table/config` | GET/PUT | Per-table configuration |
| `/reflection/execute` | POST | Execute SQL query (quality check fallback) |
| `/reflection/quality/run` | POST | Trigger backend quality check |
| `/reflection/quality/latest` | GET | Fetch cached quality results |
| `/reflection/rbac/roles` | GET | List available RBAC roles |

### Environment Variables (from `common.py`)

| Variable | Purpose |
|----------|---------|
| `SUPERTABLE_ORGANIZATION` | Default organization |
| `SUPERTABLE_SUPERTOKEN` | Admin authentication token |
| `SUPERTABLE_SUPERHASH` | Superuser identity hash |
| `SUPERTABLE_SESSION_SECRET` | HMAC secret for session cookies |
| `SUPERTABLE_REDIS_*` | Redis connection parameters |
| `SUPERTABLE_DEBUG_TIMINGS` | Enable Server-Timing headers on `/reflection/super` |
| `SECURE_COOKIES` | Enable Secure flag on cookies |

---

## 10. Architecture Positioning

### System Position

The Tables Command Center sits in the **presentation/admin layer** of the SuperTable platform:

```
Browser (tables.html JS)
   ↓ HTTP/JSON
FastAPI Router (tables.py + common.py)
   ↓ Internal calls
MetaReader → Redis (metadata)
Catalog → Redis (config)
SimpleTable → Redis + Storage (deletion)
/reflection/execute → Query Engine (quality SQL)
/reflection/quality/* → DQ Engine (quality backend)
```

### Upstream Components

- **`sidebar.html`**: Shared navigation sidebar (Jinja `{% include %}`)
- **`selection.html`**: Likely a shared SuperTable selector component (referenced in comments as calling `onSuperMetaLoaded`)
- **`common.py`**: Auth middleware, session management, routing infrastructure

### Downstream Dependencies

- **MetaReader**: Reads parquet-level metadata from storage (likely S3/GCS/local) via Redis-cached catalog
- **RedisCatalog**: Redis key management for metadata and configuration
- **Query execution engine** (`/reflection/execute`): Runs SQL against SuperTable data (DuckDB inference from user preferences)
- **Quality engine** (`/reflection/quality/*`): Backend data quality assessment service

### Boundaries

- This module does NOT contain the query execution engine, the quality backend, or the RBAC engine — it only consumes their APIs.
- Auth/session is fully delegated to `common.py`.
- All Redis key patterns follow convention: `supertable:{org}:{sup}:meta:root` and `supertable:{org}:{sup}:meta:leaf:{table}`.

### Coupling Considerations

- **Tightly coupled to Redis key conventions**: Changes to key naming would break `list_supers()` and `_list_leaves()`.
- **Loosely coupled to MetaReader**: Uses only three methods (`get_super_meta`, `get_table_schema`, `get_table_stats`).
- **Loosely coupled to quality backend**: Falls back gracefully if quality APIs are unavailable.

---

## 11. Business Value Assessment

- **What problem this solves**: Without this page, operators cannot see what tables exist, understand their schema, assess data quality, or tune per-table storage parameters — they would need direct Redis/storage access.
- **Revenue/efficiency impact**: This is an **operational efficiency** and **platform usability** feature. It makes the SuperTable product self-serviceable for data engineers, reducing support burden and enabling faster data issue diagnosis.
- **Data quality insight**: The quality check capability (NULL rates, uniqueness, cardinality analysis) is a differentiating feature — it goes beyond basic catalog browsing into active data profiling.
- **Per-table config**: The ability to override compaction/merge settings per table is an **advanced operational capability** that enables performance tuning for heterogeneous workloads.
- **Strategic value**: This is a **core product feature** — it's the primary table management interface. Losing it would significantly degrade the admin experience.
- **Differentiating vs commodity**: The combination of schema browsing + quality profiling + per-file stats with value search + per-table config tuning in a unified UI is **differentiating** compared to basic data catalog tools.

---

## 12. Technical Depth Assessment

- **Complexity**: Moderate-to-high. ~2700 lines of HTML/CSS/JS in a single template, plus ~470 lines of Python backend. Two coexisting rendering paths (CC layout + legacy DataTable).
- **Architectural maturity**: The dependency injection pattern in `attach_tables_routes()` is well-structured. The dual-rendering (CC + legacy) suggests an active migration that increases maintenance burden.
- **Maintainability**: The monolithic template with inline CSS and JS is difficult to maintain at scale. No module bundling, no component framework.
- **Extensibility**: New tabs can be added to the CC detail view relatively easily. New API endpoints follow the established pattern.
- **Performance considerations**: Redis SCAN for table discovery could be slow with thousands of tables. The SQL-based quality check reads full table data. The backend quality path with polling is more efficient but adds complexity.
- **Reliability**: Graceful degradation on quality check (backend → SQL fallback). Safety timer on meta load (3s timeout). Error handling present on all API calls with user-facing error messages.
- **Security**: Auth checks on every endpoint. Type-to-confirm deletion. RBAC role propagation. HMAC-signed session cookies with server-side expiry. `httponly` and `samesite=lax` cookie flags.
- **Testing**: No test code visible. The codebase would benefit from API-level integration tests.

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Tables are stored as Redis keys with pattern `supertable:{org}:{sup}:meta:leaf:{name}`.
- Schema, stats, and metadata are read via `MetaReader` class.
- Per-table config is stored/read via `catalog.get_table_config()` / `catalog.set_table_config()`.
- Table deletion calls `SimpleTable.delete(role_name=role)`.
- Auth uses HMAC-signed session cookies with 7-day expiry.
- The UI has both a "Command Center" 3-column layout and a legacy DataTable rendering path.
- Quality check has two execution paths: backend DQ engine and client-built SQL.
- Freshness is computed purely by `updated_utc` age: <1hr=fresh, <24hr=aging, >24hr=stale.

### Reasonable Inferences

- **MetaReader likely reads parquet file metadata** — stat fields include file_size, rows, columns, and per-column min/max, which are standard parquet footer statistics.
- **The query engine is likely DuckDB** — user preferences mention DuckDB SQL syntax; the quality SQL uses standard SQL that works with DuckDB.
- **`catalog` is likely a `RedisCatalog` instance** — both `catalog.get_table_config()` and `RedisCatalog()` appear; the catalog is passed as a dependency.
- **The system uses a lakehouse-style architecture** — parquet files, compaction thresholds, merge chunks, snapshot versioning all point to a write-optimized columnar storage engine.
- **`MAX_MEMORY_CHUNK_SIZE` and `MAX_OVERLAPPING_FILES` are global env vars** — the UI refers to them as "global defaults" that per-table config can override.

### Unknown / Not Visible in Provided Snippet

- Exact implementation of `MetaReader.get_super_meta()`, `get_table_schema()`, `get_table_stats()`.
- Exact implementation of `catalog.get_table_config()` / `set_table_config()`.
- How `SimpleTable.delete()` works internally (Redis cleanup + storage deletion).
- The `/reflection/execute` endpoint implementation (query execution engine).
- The `/reflection/quality/run` and `/reflection/quality/latest` endpoint implementations (DQ engine).
- How `admin_guard_api` dependency works (token validation logic).
- Whether `discover_pairs()` reads from Redis or another source.
- The full `sidebar.html` template and navigation structure.

---

## 14. AI Consumption Notes

- **Canonical feature name**: `Tables Command Center` (also: `Tables Page`, `Table Browser`, `Reflection Tables`)
- **Alternative names in code**: "Command Center" (CC prefix), "Tables view", "reflection/tables"
- **Main responsibilities**: Table discovery, schema inspection, data quality analysis, per-file stats, per-table config, table deletion
- **Important entities**: SuperTable, SimpleTable, table (leaf), schema pairs, stats resources, table config, quality results, RBAC role
- **Important workflows**: Page load → meta fetch → table list; Table select → schema + stats + settings load; Quality check (backend + SQL fallback); Config save; Table delete
- **Integration points**: Redis (metadata/config), MetaReader (schema/stats), Query engine (/reflection/execute), DQ engine (/reflection/quality/*), RBAC (/reflection/rbac/roles)
- **Business keywords**: data catalog, schema discovery, data quality, freshness monitoring, compaction tuning, table management, admin console
- **Architecture keywords**: Jinja2 template, FastAPI, Redis metadata, dependency injection, RBAC, session-based auth, SPA-hybrid
- **Follow-up files for completeness**:
  - `supertable/meta_reader.py` — schema/stats reading logic
  - `supertable/redis_catalog.py` — catalog and config persistence
  - `supertable/simple_table.py` — table deletion logic
  - `supertable/reflection/quality.py` — DQ backend engine
  - `supertable/reflection/common.py` (full) — auth, session, routing infrastructure
  - `templates/sidebar.html` — navigation structure
  - Execute endpoint implementation — query engine integration

---

## 15. Suggested Documentation Tags

`data-catalog`, `table-management`, `schema-inspection`, `data-quality`, `statistics-explorer`, `admin-console`, `reflection-ui`, `command-center`, `per-table-config`, `compaction-tuning`, `table-deletion`, `rbac-aware`, `redis-metadata`, `fastapi`, `jinja2-template`, `spa-hybrid`, `metareader`, `parquet-statistics`, `freshness-monitoring`, `health-score`, `multi-tenant`, `dependency-injection`, `session-auth`, `presentation-layer`, `operator-tooling`

---

## Merge Readiness

- **Suggested canonical name**: `reflection-tables-command-center`
- **Standalone or partial**: Partial — depends on MetaReader, RedisCatalog, quality engine, execute engine, and common.py infrastructure.
- **Related components to merge later**: `reflection-quality` (DQ engine), `reflection-execute` (query engine), `reflection-common` (auth/session/routing), `reflection-sidebar` (navigation), `meta-reader` (metadata access), `redis-catalog` (config storage), `simple-table` (deletion logic).
- **Additional files that would most improve certainty**: `meta_reader.py`, `quality.py`, `redis_catalog.py`, `simple_table.py`.
- **Recommended merge key phrases**: "tables page", "table browser", "schema inspection", "data quality check", "table config", "reflection/tables", "command center tables", "table list panel".
