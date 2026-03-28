# Feature / Component Reverse Engineering

## Supertable MCP Server — Complete Package Analysis

---

## 1. Executive Summary

This code implements the **Supertable MCP (Model Context Protocol) Server**, a production-grade integration layer that exposes a columnar data platform ("SuperTable") to AI agents — primarily Claude Desktop and Claude.ai — via the MCP standard. The package contains approximately 2,270 lines of server logic plus supporting client, proxy, web gateway, and testing infrastructure totaling ~4,500 lines across 9 files.

**Main responsibility:** Expose read-only SQL querying, metadata inspection, feedback collection, annotation persistence, app-state management, data profiling, and auto-discovery capabilities over a data warehouse to LLM-powered agents, with RBAC enforcement, concurrency control, and multi-transport support (stdio, Streamable HTTP, SSE).

**Product capability:** Enables an "Agentic BI" (Business Intelligence) product where AI assistants can autonomously query structured data, learn from user feedback, follow user-defined rules (annotations), save dashboards/widgets/queries, and progressively self-improve query accuracy over time.

**Business purpose:** Creates an AI-native data access layer that transforms a columnar data platform into an interactive, self-learning analytics assistant — a key monetizable differentiator in the BI/analytics market.

**Technical role:** Acts as the bridge between the MCP protocol (JSON-RPC 2.0 over stdio/HTTP) and the Supertable data platform (MetaReader, DataReader, DataWriter, RBAC, Redis catalog, DuckDB/Parquet storage).

---

## 2. Functional Overview

### Product Capabilities Enabled

The Supertable MCP package enables the following product-level capabilities:

**AI-Powered Data Querying:** An LLM agent (Claude) can execute read-only SQL queries against an organization's data warehouse, with automatic limit enforcement, timeout protection, and RBAC-based row/column filtering. The query engine supports DuckDB over Parquet files, with configurable engine selection.

**Self-Learning AI Context (Catalog + Annotations + Feedback Loop):** The system implements a three-layer AI memory mechanism:

1. **Catalog (`__catalog__`):** Human-readable table documentation (descriptions, column docs, join relationships, example queries) that is eagerly loaded into the AI's context window at session start. The AI is instructed to consult the catalog before writing any SQL.

2. **Annotations (`__annotations__`):** User-defined rules and preferences categorized as `sql`, `terminology`, `visualization`, `scope`, or `domain`. These are treated as "hard constraints" — the AI must follow them when generating SQL, choosing visualizations, or interpreting user terminology. Examples: "weekly means rolling 7 days", "always use bar charts for revenue", "exclude test accounts."

3. **Feedback (`__feedback__`):** Every AI response can be rated thumbs_up/thumbs_down with optional comments. Past feedback is loaded at session start so the AI avoids repeating poorly-rated approaches and builds on well-rated ones.

**Agentic BI Frontend State Persistence:** A generic key-value store (`__app_state__`) persists UI state for a companion frontend application. Namespaces include `chat`, `dashboard`, `widget`, `query`, and `note`. This enables saving SQL queries, pinning chart widgets, building dashboards, and maintaining conversation history.

**Data Discovery & Profiling:** Tools for schema inspection, column profiling (type, null%, distinct count, min/max, top-N values with frequencies), cross-table column search, data freshness checking (from Redis metadata), and full auto-discovery (crawl all tables to collect schema, samples, column profiles, and pattern detection).

**Multi-Transport Connectivity:** The server supports three MCP transport modes:
- **stdio** — for Claude Desktop local connections
- **Streamable HTTP** — for Claude Desktop remote connections and web integrations
- **SSE** — legacy Server-Sent Events transport

### Target Users / Actors

| Actor | Interaction | Value |
|---|---|---|
| Business analyst | Asks natural-language questions via Claude | Self-service analytics without SQL knowledge |
| Data engineer | Configures catalog entries, annotations, roles | Governs AI data access and query accuracy |
| Platform operator | Deploys/configures MCP server, sets RBAC | Controls security, concurrency, transport |
| LLM agent (Claude) | Calls MCP tools programmatically | Autonomous data exploration and reporting |
| Frontend application (AgenticBI) | Reads/writes `__app_state__` via tools | Persists dashboards, widgets, saved queries |

### Business Value

- **Revenue-related:** Core differentiator for an "Agentic BI" product — AI-native analytics that learns and improves
- **Efficiency-related:** Eliminates the need for SQL expertise; the AI writes correct queries by consulting catalog and profiling columns before executing
- **Compliance/governance-related:** RBAC enforcement ensures data access respects organizational policies; read-only SQL prevents data mutation
- **Strategic:** The feedback loop and annotation system create a flywheel: more usage → better annotations/catalog → higher query accuracy → more usage

---

## 3. Technical Overview

### Architecture

The system follows a **tool-oriented MCP server pattern** built on the FastMCP SDK. Each data operation is exposed as a named "tool" that an MCP client (Claude Desktop, a web proxy, or a CLI client) can invoke via JSON-RPC 2.0.

```
┌─────────────────┐     stdio/JSON-RPC      ┌──────────────────────────┐
│  Claude Desktop  │◄──────────────────────►│     mcp_server.py        │
│  (MCP client)    │                         │   (FastMCP + 22 tools)   │
└─────────────────┘                         │                          │
                                             │  ┌────────────────────┐ │
┌─────────────────┐     Streamable HTTP      │  │ MetaReader         │ │
│  Claude Desktop  │◄──────────────────────►│  │ DataReader         │ │
│  (remote)        │     via web_app.py      │  │ DataWriter         │ │
└─────────────────┘                         │  │ RBAC               │ │
                                             │  │ RedisCatalog       │ │
┌─────────────────┐     stdio proxy          │  │ SQLParser          │ │
│ mcp_stdio_proxy  │◄─────────────────────►│  └────────────────────┘ │
│ (HTTP bridge)    │                         └──────────────────────────┘
└─────────────────┘
                                             ┌──────────────────────────┐
┌─────────────────┐     HTTP REST API        │     web_app.py           │
│  Browser         │◄──────────────────────►│   (FastAPI gateway)      │
│  (web tester)    │                         │   + MCPWebClient (stdio) │
└─────────────────┘                         └──────────────────────────┘
```

### Major Design Patterns

1. **Concurrency Limiter (AnyIO CapacityLimiter):** Every tool call acquires a slot from a shared capacity limiter (default: 6 concurrent operations) before doing work. This prevents resource exhaustion.

2. **Thread Offloading:** All blocking I/O (MetaReader, DataReader, DataWriter, Redis) runs via `anyio.to_thread.run_sync()`, keeping the async event loop responsive.

3. **Consistent Error Envelopes:** Every tool returns a dict with `status` ("OK" or "ERROR") and `message`. The `query_sql` tool has a richer envelope with `columns`, `rows`, `rowcount`, `limit_applied`, `engine`, `elapsed_ms`, and `columns_meta`.

4. **Tombstone Deletion:** Annotations and app-state entries are soft-deleted by writing tombstone rows with `status='deleted'`. All reads filter out deleted rows.

5. **Lazy Import Resolution:** Heavy Supertable dependencies (MetaReader, DataReader, DataWriter) are imported lazily on first use via `_ensure_imports()`, allowing the module to load even when the data layer is unavailable.

6. **Eager Context Loading:** The `list_tables` tool (called at session start) eagerly loads catalog, feedback, and annotations from system tables and injects them — plus a detailed `system_hint` — into the response. This primes the AI with all necessary context in a single call.

7. **SDK Version Compatibility:** Multiple fallback paths handle different versions of the MCP SDK (FastMCP), including runtime introspection of constructor signatures and method availability.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `mcp_server.py` (2,268 lines) | Core MCP server with 22 tool definitions | `Config`, `_build_mcp()`, `log_tool`, `_exec_query_sync()`, all `@mcp.tool()` functions | **Critical** — the entire product capability |
| `web_app.py` (500 lines) | FastAPI HTTP gateway wrapping the MCP server | `MCPWebClient`, `_McpRouter`, `/mcp_v1`, `/api/*` endpoints | **Critical** — enables remote + browser access |
| `web_client.py` (~200 lines) | Async stdio MCP client for web app | `MCPWebClient`, `McpEvent` | **Important** — bridges web app to server subprocess |
| `mcp_client.py` (715 lines) | CLI test/debug client | `Wire`, `RpcIds`, `safe_query_with_fallback()` | **Supporting** — testing and diagnostics |
| `mcp_stdio_proxy.py` (~120 lines) | stdio-to-HTTP bridge for Claude Desktop | `post_json()`, `main()` | **Important** — enables remote server from Claude Desktop |
| `web_server.py` (~30 lines) | Uvicorn runner for web_app | `main()` | **Supporting** — deployment entry point |
| `web_tester.html` (887 lines) | Browser-based MCP tool tester UI | Full SPA with org/super/table selectors, SQL editor | **Supporting** — debugging/demo UI |
| `mcp_simulator.html` (631 lines) | Raw JSON-RPC MCP client simulator | Full MCP protocol simulator UI | **Supporting** — protocol-level debugging |
| `__init__.py` (1 line) | Package marker | N/A | **Infrastructure** |

---

## 5. Detailed Functional Capabilities

### 5.1 Read-Only SQL Query Execution

- **Description:** Executes user-provided SQL against the Supertable data warehouse, enforcing read-only constraints and RBAC.
- **Business purpose:** Core data access for AI-driven analytics — the AI writes SQL, the server executes it safely.
- **Trigger/input:** `query_sql` tool call with `super_name`, `organization`, `sql`, optional `limit`, `engine`, `query_timeout_sec`, `role`, `auth_token`.
- **Processing behavior:**
  1. Validates all input IDs (org, super) against `SAFE_ID_RE` regex.
  2. Authenticates via shared-secret token (`_check_token`).
  3. Validates RBAC role (`_resolve_role`).
  4. Strips SQL comments to prevent keyword-splitting evasion, then enforces read-only: must start with `SELECT` or `WITH`, must not contain `INSERT/UPDATE/DELETE/MERGE/CREATE/ALTER/DROP/TRUNCATE/GRANT/REVOKE`.
  5. Clamps limit to `[1, max_limit]` (default 200, max 5000).
  6. Acquires concurrency limiter slot.
  7. Executes query via `data_query_sql()` in a thread, with `anyio.fail_after()` timeout.
  8. Double-enforces limit by slicing rows server-side.
- **Output:** `{columns, rows, rowcount, limit_applied, engine, elapsed_ms, status, message, columns_meta}`.
- **Dependencies:** `supertable.data_reader.query_sql`, `supertable.data_reader.engine` enum, AnyIO.
- **Constraints:** Read-only only. SQL comment stripping prevents `INS/**/ERT` evasion. Maximum 5,000 rows per query.
- **Risks:** The regex-based read-only check could theoretically be bypassed by exotic SQL constructs not covered by the forbidden-keyword list (e.g., DuckDB-specific syntax). The system relies on downstream RBAC as a second layer of defense.
- **Confidence:** Explicit.

### 5.2 Session Context Priming (list_tables)

- **Description:** Returns available tables AND eagerly loads catalog, feedback, and annotations from system tables into a single response, along with a detailed `system_hint` that instructs the AI on query planning methodology.
- **Business purpose:** Single-call session initialization that primes the AI with all the context needed for accurate, rule-following query generation.
- **Trigger/input:** `list_tables` tool call.
- **Processing behavior:**
  1. Fetches table list via `MetaReader.get_tables()`.
  2. If `__catalog__` exists: fetches all rows (up to 500), emits `catalog` and `catalog_hint`.
  3. If `__feedback__` exists: fetches last 50 rows, emits `feedback` and `feedback_hint`.
  4. If `__annotations__` exists: fetches up to 200 non-deleted rows, emits `annotations` and `annotations_hint`.
  5. Always emits `system_hint` — a multi-paragraph instruction set covering catalog usage, query planning sequence (identify → profile → check annotations → validate → execute → visualize), annotation enforcement, feedback learning, and session memory rules.
- **Output:** `{result: [table_names], catalog, catalog_hint, feedback, feedback_hint, annotations, annotations_hint, system_hint}`.
- **Confidence:** Explicit.

### 5.3 Column Profiling

- **Description:** Profiles a single column: type, null%, distinct count, min, max, and top-N values with frequencies.
- **Business purpose:** Enables the AI to understand actual data values before writing WHERE/GROUP BY clauses. Prevents wrong enum values, wrong casing, and wasted queries.
- **Trigger/input:** `profile_column` tool call with `table`, `column`, optional `top_n` (default 10, max 50).
- **Processing behavior:** Executes two DuckDB queries in one thread-offloaded call: one for aggregate stats (`typeof`, `COUNT`, `COUNT(DISTINCT)`, `MIN`, `MAX`) and one for top-N value frequencies.
- **Confidence:** Explicit.

### 5.4 SQL Validation (Dry Run)

- **Description:** Validates a SQL query without executing it — checks read-only compliance, SQL parsing, table existence, and RBAC permissions.
- **Business purpose:** Allows the AI to verify a query will succeed before spending a real `query_sql` call, reducing wasted round-trips.
- **Trigger/input:** `validate_sql` tool call.
- **Processing behavior:**
  1. Read-only check.
  2. SQL parsing via `supertable.utils.sql_parser.SQLParser` to extract table references.
  3. Table existence check against `MetaReader.get_tables()`.
  4. RBAC check via `supertable.rbac.access_control.restrict_read_access`.
- **Output:** `{valid: bool, status, error, phase, tables, available_tables}`.
- **Confidence:** Explicit.

### 5.5 Feedback Collection

- **Description:** Records user satisfaction ratings (thumbs_up/thumbs_down/neutral) with the query that was asked, a summary of the AI's answer, the primary table queried, and optional free-text comments.
- **Business purpose:** Creates a feedback loop for AI self-improvement. Past feedback is loaded at session start to guide future behavior.
- **Trigger/input:** `submit_feedback` tool call.
- **Processing behavior:** Validates rating against allowed values, constructs a row with UTC timestamp, writes via `DataWriter` to `__feedback__` table using PyArrow RecordBatch.
- **Confidence:** Explicit.

### 5.6 Annotation Management (Store / Get / Delete)

- **Description:** CRUD operations for persistent user-defined rules and preferences.
- **Business purpose:** Allows users to teach the AI domain-specific rules that persist across sessions. Categories: `sql`, `terminology`, `visualization`, `scope`, `domain`.
- **Key behaviors:**
  - `store_annotation`: Writes a new annotation row to `__annotations__`.
  - `get_annotations`: Reads non-deleted annotations, optionally filtered by context.
  - `delete_annotation`: Soft-deletes by writing tombstone rows with `status='deleted'`. Supports filtering by `instruction_contains` substring.
- **Confidence:** Explicit.

### 5.7 App State Persistence (Store / Get / Delete)

- **Description:** Generic key-value store for frontend application state, partitioned by namespace.
- **Business purpose:** Powers the AgenticBI frontend's persistence layer — saved queries, dashboard configs, widget configs, chat history, data point notes.
- **Namespaces:** `chat`, `dashboard`, `widget`, `query`, `note`.
- **Key behaviors:**
  - `store_app_state`: Writes a row with namespace, key, JSON value (up to 50KB), and UTC timestamp.
  - `get_app_state`: Reads by namespace + optional key; excludes deleted entries.
  - `delete_app_state`: Soft-deletes via tombstone row.
- **Confidence:** Explicit.

### 5.8 Auto-Discovery

- **Description:** Crawls all user tables (excluding system tables with `__` prefix) and collects schema, sample rows, per-column stats (null%, distinct count), and detected patterns (date/time columns, identifier columns, low-cardinality/enum-like columns).
- **Business purpose:** Bootstrap tool for new SuperTables — generates the raw data needed to populate catalog entries and annotations.
- **Processing behavior:** For each table (up to 30, max 50): get schema → sample rows → single aggregate stats query for all columns (up to 40) → pattern detection heuristics.
- **Confidence:** Explicit.

### 5.9 Catalog Store

- **Description:** Writes/updates a catalog entry for a specific table in `__catalog__`.
- **Business purpose:** The AI calls this after `auto_discover` to populate human-readable documentation that will be loaded at session start.
- **Fields:** `table_name`, `description`, `columns`, `joins`, `example_queries`, `filters`.
- **Confidence:** Explicit.

### 5.10 Data Freshness

- **Description:** Returns data freshness metadata (last updated timestamp, snapshot version, file count, size) directly from Redis — no data scanning.
- **Business purpose:** Lets the AI answer "when was this data last updated?" without running a query. Can report on all tables at once.
- **Dependencies:** `supertable.redis_catalog.RedisCatalog`.
- **Confidence:** Explicit.

### 5.11 Cross-Table Column Search

- **Description:** Searches column names and types by case-insensitive substring across all non-system tables.
- **Business purpose:** Replaces multiple `describe_table` calls when the AI needs to find which table has a specific column type.
- **Confidence:** Explicit.

### 5.12 Metadata Inspection Tools

- **Tools:** `describe_table`, `get_table_stats`, `get_super_meta`, `list_supers`, `sample_data`.
- **Business purpose:** Schema exploration, statistics inspection, and data preview without full SQL.
- **Confidence:** Explicit.

### 5.13 Multi-Transport Server Deployment

- **Description:** The server can run in three modes: stdio (default for Claude Desktop local), Streamable HTTP (for remote access), or SSE (legacy).
- **Business purpose:** Flexible deployment — local development via stdio, production via HTTP behind a reverse proxy.
- **Key implementation:** `_serve_streamable_http_via_uvicorn()` builds an ASGI app from the FastMCP instance and serves it via Uvicorn with TrustedHostMiddleware.
- **Confidence:** Explicit.

### 5.14 Web Gateway (web_app.py)

- **Description:** FastAPI application that serves as both a REST API gateway to the MCP server (via stdio subprocess) and a Streamable HTTP MCP endpoint (mounted at `/mcp`).
- **Business purpose:** Enables browser-based testing, remote MCP connections, and HTTP API access to all MCP tools.
- **Key behaviors:**
  - Gateway auth via shared-secret token (Bearer, X-Auth-Code, or `?auth=` query param).
  - `/mcp_v1` endpoint: browser-friendly JSON-RPC proxy with role/auth header injection.
  - `/mcp` path: raw MCP Streamable HTTP transport via ASGI routing middleware (`_McpRouter`).
  - REST endpoints (`/api/*`) for each major tool.
  - Serves `web_tester.html` and `mcp_simulator.html` UIs.
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 mcp_server.py

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `Config` | Dataclass | Frozen config loaded from env vars; controls limits, auth, transport, concurrency | Critical |
| `_build_mcp()` | Function | Constructs FastMCP instance with SDK-version-adaptive kwargs (stateless_http, json_response, allowed_origins, transport_security) | Critical |
| `_safe_id()` | Function | Validates org/super/table identifiers against `^[A-Za-z0-9_.-]{1,128}$`; rejects path traversal | Critical (security) |
| `_read_only_sql()` | Function | Enforces read-only SQL: strips comments, checks SELECT/WITH prefix, rejects forbidden DDL/DML keywords | Critical (security) |
| `_strip_sql_comments()` | Function | Removes `/* */` and `-- ` comments from SQL to prevent keyword-splitting evasion | Critical (security) |
| `_safe_column_id()` | Function | Validates column name, escapes embedded double-quotes for safe SQL identifier use | Important (security) |
| `_sql_escape_literal()` | Function | Escapes values for SQL single-quoted literals; rejects null bytes | Important (security) |
| `_check_token()` | Function | HMAC-based shared-secret authentication using `hmac.compare_digest()` | Critical (security) |
| `_resolve_role()` | Function | Validates and resolves RBAC role; supports required-role policy, allowed-role whitelist, env-var fallback | Critical (security) |
| `_exec_query_sync()` | Function | Synchronous query executor; calls `data_query_sql()`, enforces limit, measures elapsed time | Critical |
| `_get_limiter()` | Function | Lazy-initializes and returns the AnyIO CapacityLimiter (thread-safe via threading.Lock) | Important |
| `_ensure_imports()` | Function | Lazy-imports MetaReader, DataReader, DataWriter, engine enum | Important |
| `log_tool` | Decorator | Wraps all tools with structured logging (request args, response timing, error envelope) | Important |
| `_resolve_engine()` | Function | Resolves query engine by name from the engine enum; falls back to string | Supporting |
| `_serve_streamable_http_via_uvicorn()` | Function | Builds MCP ASGI app and runs via Uvicorn for HTTP transport | Important |
| `query_sql` | Tool | Read-only SQL execution with timeout, limit, RBAC | Critical |
| `list_tables` | Tool | Table listing + eager catalog/feedback/annotations/system_hint loading | Critical |
| `describe_table` | Tool | Schema inspection via MetaReader | Important |
| `get_table_stats` | Tool | Table statistics via MetaReader | Important |
| `get_super_meta` | Tool | SuperTable metadata via MetaReader | Supporting |
| `list_supers` | Tool | List SuperTables in an organization | Supporting |
| `sample_data` | Tool | Quick data preview (SELECT * LIMIT N) | Supporting |
| `submit_feedback` | Tool | Write feedback row to `__feedback__` | Important |
| `store_annotation` | Tool | Write annotation to `__annotations__` | Important |
| `get_annotations` | Tool | Read annotations (with optional context filter) | Important |
| `delete_annotation` | Tool | Soft-delete annotations via tombstone rows | Important |
| `store_app_state` | Tool | Write key-value UI state to `__app_state__` | Important |
| `get_app_state` | Tool | Read UI state by namespace + optional key | Important |
| `delete_app_state` | Tool | Soft-delete UI state entries | Supporting |
| `profile_column` | Tool | Column statistics + top-N value frequencies | Important |
| `validate_sql` | Tool | Dry-run SQL validation (parse, table check, RBAC) | Important |
| `search_columns` | Tool | Cross-table column name/type search | Supporting |
| `data_freshness` | Tool | Redis-based data freshness metadata | Supporting |
| `auto_discover` | Tool | Full-table crawl for catalog/annotation bootstrapping | Important |
| `store_catalog` | Tool | Write catalog entry to `__catalog__` | Important |
| `health` | Tool | Health check | Supporting |
| `info` | Tool | Server configuration info | Supporting |
| `whoami` | Tool | Role resolution check | Supporting |

### 6.2 web_app.py

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `_McpRouter` | Class | ASGI middleware routing `/mcp*` to MCP SDK and everything else to FastAPI | Critical |
| `_build_mcp_streamable_app()` | Function | Builds the MCP SDK ASGI sub-app for Streamable HTTP | Important |
| `_require_gateway_auth()` | Function | Enforces shared-secret auth on HTTP endpoints (Bearer/header/query param) | Critical (security) |
| `mcp_http_gateway_v1` | Endpoint | JSON-RPC proxy at `/mcp_v1` with role/auth injection | Important |
| `/api/*` endpoints | Endpoints | REST wrappers for each MCP tool | Important |
| `lifespan()` | Function | FastAPI lifespan: starts MCP session manager + stdio client | Important |

### 6.3 web_client.py (MCPWebClient)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `MCPWebClient` | Class | Async stdio client managing a persistent MCP server subprocess | Critical |
| `start()` | Method | Spawns subprocess, performs MCP initialize handshake | Critical |
| `close()` | Method | Terminates subprocess, fails pending futures | Important |
| `tool()` | Method | Sends `tools/call` with auto-injected auth_token for AUTH_TOOLS | Critical |
| `jsonrpc()` | Method | Sends arbitrary JSON-RPC with id remapping | Important |
| `_reader_loop()` | Method | Async reader task matching responses to pending futures by id | Critical |
| `_request()` | Method | Core request/response with timeout (120s default) | Critical |

### 6.4 mcp_stdio_proxy.py

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `post_json()` | Function | HTTP POST of JSON-RPC message; handles SSE and JSON responses | Critical |
| `main()` | Function | stdin→HTTP→stdout relay loop | Critical |
| `make_ssl_context()` | Function | Optional insecure SSL context for self-signed certs | Supporting |

### 6.5 mcp_client.py

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `Wire` | Class | NDJSON/LSP wire protocol handler for subprocess stdio | Important |
| `safe_query_with_fallback()` | Function | Attempts SQL, falls back to sample query on table-less SQL | Important |
| `load_config()` | Function | Multi-source config loading (.env, .json, upward search) | Supporting |

---

## 7. End-to-End Workflows

### 7.1 AI Session Initialization (Claude Desktop → list_tables)

1. Claude Desktop starts a stdio subprocess (mcp_server.py) or connects via Streamable HTTP.
2. MCP handshake: `initialize` → `notifications/initialized`.
3. Claude calls `list_tables` with org, super_name, role.
4. Server validates inputs and auth.
5. Server fetches table list via `MetaReader.get_tables()`.
6. Server checks for `__catalog__`, `__feedback__`, `__annotations__` system tables.
7. For each found system table, server executes an internal SQL query to load rows.
8. Server constructs response with tables + catalog + feedback + annotations + `system_hint`.
9. Claude receives the response and holds catalog/annotations/feedback in memory for the session.
10. Claude follows `system_hint` instructions for all subsequent queries.

**Failure modes:** Missing org/super returns validation error. Auth token mismatch returns PermissionError. System table fetch failures are non-fatal (logged as warnings, empty data returned).

### 7.2 AI Query Planning & Execution

1. User asks a natural-language question.
2. AI consults catalog to identify relevant tables and columns.
3. AI calls `profile_column` for any filter/GROUP BY columns to discover exact values and casing.
4. AI checks annotations for rules about these tables/columns.
5. AI calls `validate_sql` to verify the query before execution.
6. AI calls `query_sql` with the validated SQL.
7. Server validates read-only compliance, resolves engine, acquires limiter slot, executes with timeout.
8. Server returns results with columns, rows, metadata.
9. AI visualizes results based on data shape and visualization annotations.
10. AI asks user for feedback (thumbs_up/thumbs_down).
11. On feedback, AI calls `submit_feedback` to persist the rating.

**Failure modes:** SQL validation failure at any step returns structured error with phase. Query timeout returns error with timeout duration. Concurrency exhaustion queues the request until a slot opens.

### 7.3 Remote MCP via Streamable HTTP (web_app.py)

1. Claude Desktop connects to `https://host:port/mcp` (configured in claude_desktop_config.json).
2. `_McpRouter` ASGI middleware intercepts `/mcp*` paths and routes to MCP SDK sub-app.
3. Origin header is stripped (CSRF protection handled by gateway auth, not MCP SDK).
4. MCP SDK handles the JSON-RPC session natively.
5. Alternatively, browser connects to `/mcp_v1` for JSON-RPC proxy via stdio subprocess.

### 7.4 Stdio Proxy for Remote Servers (mcp_stdio_proxy.py)

1. Claude Desktop starts `mcp_stdio_proxy.py` as a local stdio process.
2. Proxy reads NDJSON from stdin, POSTs to remote MCP server URL.
3. Proxy parses HTTP response (JSON or SSE), writes JSON-RPC response to stdout.
4. Maintains session ID via `Mcp-Session-Id` header.
5. Supports `--insecure` flag for self-signed TLS certificates.

---

## 8. Data Model and Information Flow

### Core Entities

| Entity | Storage | Schema | Lifecycle |
|---|---|---|---|
| **Table metadata** | MetaReader (read from Parquet/Redis) | Schema list, stats dict | Read-only via MCP |
| **Query results** | In-memory, returned per request | `{columns: [...], rows: [[...]], ...}` | Ephemeral per request |
| **Catalog entries** | `__catalog__` table (DataWriter) | `{ts, table_name, description, columns, joins, example_queries, filters, role}` | Append-only |
| **Feedback entries** | `__feedback__` table (DataWriter) | `{ts, rating, query, response_summary, table_name, comment, role}` | Append-only |
| **Annotations** | `__annotations__` table (DataWriter) | `{ts, category, context, instruction, role, status}` | Append-only with tombstones |
| **App state** | `__app_state__` table (DataWriter) | `{ts, namespace, key, value, role, status}` | Append-only with tombstones |
| **Freshness metadata** | Redis (RedisCatalog) | `{ts, version, payload: {resources, schema}}` | Managed by data pipeline |

### System Tables (dunder-prefixed)

All system tables use the `__name__` naming convention and are filtered out from user-facing table lists in tools like `auto_discover` and `search_columns`. They are managed exclusively through dedicated MCP tools.

### Data Flow

```
User question → Claude → list_tables → [catalog + annotations + feedback loaded]
                       → profile_column → [column stats + top values]
                       → validate_sql → [parse + table check + RBAC]
                       → query_sql → DataReader.query_sql() → DuckDB → Parquet → results
                       → submit_feedback → DataWriter.write() → __feedback__ → Parquet
```

### Write Semantics

All writes use `DataWriter.write()` with `overwrite_columns=["ts"]`. This suggests an append-only storage model where the timestamp column prevents row-level overwrites (each write creates a new row). Deletions use tombstone rows with `status='deleted'` rather than physical deletion.

---

## 9. Dependencies and Integrations

### External Dependencies

| Dependency | Usage | Required? |
|---|---|---|
| `mcp` (MCP SDK / FastMCP) | MCP protocol server framework | Yes (with fallback shim) |
| `anyio` | Async concurrency: CapacityLimiter, thread offloading, timeouts | Yes |
| `uvicorn` | ASGI server for HTTP transports | For HTTP transport only |
| `fastapi` | Web framework for gateway/REST API | For web_app.py only |
| `starlette` | TrustedHostMiddleware | For web_app.py only |
| `pyarrow` | RecordBatch construction for DataWriter | For write operations |
| `python-dotenv` | .env file loading | Yes |

### Internal Dependencies (Supertable Platform)

| Module | Usage |
|---|---|
| `supertable.meta_reader.MetaReader` | Table listing, schema, stats, super metadata |
| `supertable.meta_reader.list_supers` | Organization-level super listing |
| `supertable.meta_reader.list_tables` | Module-level table listing |
| `supertable.data_reader.query_sql` | SQL query execution |
| `supertable.data_reader.engine` | Query engine enum (AUTO, DuckDB, etc.) |
| `supertable.data_writer.DataWriter` | Writing to system tables (feedback, annotations, catalog, app_state) |
| `supertable.rbac.access_control.restrict_read_access` | RBAC permission enforcement |
| `supertable.utils.sql_parser.SQLParser` | SQL parsing for table reference extraction |
| `supertable.redis_catalog.RedisCatalog` | Data freshness metadata from Redis |

### Infrastructure Dependencies

| System | Role |
|---|---|
| Redis | Stores data freshness metadata (snapshot versions, timestamps, payload) |
| Parquet files | Underlying data storage format (queried via DuckDB) |
| DuckDB | Query engine for Parquet data |
| File system | Parquet file access, configuration files |

---

## 10. Architecture Positioning

### System Position

The MCP server sits at the **API/integration layer** between:
- **Upstream:** MCP clients (Claude Desktop, Claude.ai remote connections, browser-based tools, CLI clients)
- **Downstream:** Supertable data platform (MetaReader, DataReader, DataWriter, RBAC, RedisCatalog)

### Boundaries

- **Protocol boundary:** JSON-RPC 2.0 (MCP standard) — the server translates between MCP tool calls and internal Supertable API calls.
- **Security boundary:** Two-layer auth — gateway token (HTTP) + tool auth token (per-call HMAC). RBAC enforcement per query.
- **Transport boundary:** Supports stdio, Streamable HTTP, and SSE — decoupling protocol from transport.
- **Data access boundary:** Strictly read-only for user data (SQL enforcement); write-only for system tables (feedback, annotations, catalog, app_state) via controlled tool APIs.

### Coupling

The server is **tightly coupled** to the Supertable platform's internal APIs (MetaReader, DataReader, DataWriter, RBAC, RedisCatalog, SQLParser). It is **loosely coupled** to MCP clients via the standard MCP protocol.

### Scalability

- Concurrency is limited by `max_concurrency` (default 6) via AnyIO CapacityLimiter — a deliberate bottleneck to protect the data platform.
- Thread offloading prevents event loop blocking.
- Stateless HTTP mode (`stateless_http=True`) suggests horizontal scalability is architecturally possible for the HTTP transport.
- The `data_freshness` tool avoids data scanning by reading Redis directly — a performance-conscious design.

---

## 11. Business Value Assessment

### Core Business Problem

Traditional BI tools require SQL expertise. Supertable's MCP server transforms the data platform into an AI-native analytics interface where business users interact through natural language, and the AI autonomously writes correct SQL by consulting a self-maintaining knowledge base (catalog + annotations + feedback).

### Why It Matters

1. **Self-learning accuracy:** The feedback loop (thumbs_up/thumbs_down → stored → loaded at session start) creates a system that improves over time without engineering intervention. This is a significant competitive moat.

2. **Domain customization without code:** Annotations let data teams encode business rules ("weekly = rolling 7 days", "exclude test accounts") that the AI must follow. This bridges the gap between generic AI and domain-specific analytics.

3. **Zero-SQL analytics:** The `system_hint` instructs the AI to follow a rigorous query planning sequence (catalog → profile → validate → execute). This isn't just "text-to-SQL" — it's a **methodology** embedded in the protocol layer.

4. **Platform lock-in / stickiness:** Catalog entries, annotations, and feedback accumulate over time. This accumulated knowledge increases switching costs — the more a customer uses the system, the more valuable it becomes.

### Revenue/Strategic Classification

- **Revenue-related:** Core product capability that justifies SaaS pricing
- **Differentiating:** The three-layer AI memory system (catalog + annotations + feedback) with session-level context injection is architecturally distinctive
- **Not commodity:** Standard BI tools don't have MCP integration, self-learning feedback loops, or user-defined annotation systems

---

## 12. Technical Depth Assessment

### Complexity Level: **High**

The codebase manages multiple transport protocols, SDK version compatibility across multiple FastMCP versions, two authentication layers, RBAC enforcement, concurrency limiting, thread-to-async bridging, timeout enforcement, SQL injection prevention, tombstone-based soft deletion, and a context-priming system that orchestrates multiple system table queries within a single tool call.

### Architectural Maturity: **Production-ready**

- Consistent error envelopes across all 22 tools
- Structured logging with timing and truncated SQL
- Thread-safe limiter initialization
- Graceful degradation (SDK fallback shim, non-fatal system table fetch failures)
- Input validation defense-in-depth (regex, comment stripping, null byte rejection, SQL literal escaping)
- Multiple auth mechanisms (tool-level HMAC + gateway-level token)

### Security Considerations

- **SQL injection prevention:** Comment stripping + forbidden keyword regex + safe identifier/literal escaping. Defense-in-depth with RBAC as a second layer.
- **Auth:** HMAC-based timing-safe token comparison (`hmac.compare_digest`).
- **Input validation:** All identifiers validated against strict regex (`^[A-Za-z0-9_.-]{1,128}$`); path traversal rejected.
- **Potential concern:** The `_FORBIDDEN_SQL_RE` regex checks for known DML/DDL keywords but could miss engine-specific syntax (DuckDB extensions, COPY, EXPORT, ATTACH, etc.). The read-only enforcement is best-effort, with RBAC as the authoritative layer.

### Maintainability

- Clear separation: server (tools) → web_app (gateway) → web_client (subprocess manager)
- Consistent patterns: every tool follows validate → auth → resolve → limiter → thread-offload → respond
- Logging decorator provides uniform observability
- Config is centralized in a frozen dataclass with env-var defaults

### Performance Considerations

- DuckDB parquet queries leverage columnar scan efficiency
- `profile_column` uses parquet metadata for fast statistics
- `data_freshness` reads Redis directly (no data scan)
- Auto-discover limits to 40 columns per table and 30 tables per crawl
- 4MB subprocess pipe buffer prevents LimitOverrunError on large responses

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Read-only SQL enforcement via comment stripping + keyword regex + statement prefix check
- HMAC-based auth token validation with timing-safe comparison
- RBAC role resolution with required-role and allowed-role policies
- AnyIO capacity limiter with configurable max concurrency (default 6)
- Eager loading of catalog, feedback, and annotations during `list_tables`
- System tables use `__name__` convention and are excluded from user-facing tools
- Soft deletion via tombstone rows with `status='deleted'`
- DataWriter writes use PyArrow RecordBatch with `overwrite_columns=["ts"]`
- Multi-transport support: stdio, streamable-http, SSE
- ASGI middleware for routing `/mcp*` to MCP SDK sub-app
- Gateway auth supports Bearer token, X-Auth-Code header, and `?auth=` query parameter
- MCP SDK version compatibility via runtime signature introspection
- Web client manages a persistent subprocess with 120s request timeout and 4MB pipe buffer
- SQL comment stripping prevents `INS/**/ERT` evasion attacks

### Reasonable Inferences

- **DuckDB is the primary query engine.** The `profile_column` tool uses DuckDB-specific syntax (`typeof()`, `GREATEST()`, `::VARCHAR` casting). The `system_hint` instructs use of DuckDB SQL syntax. (Strong inference)
- **Parquet is the storage format.** References to "parquet metadata" in `profile_column` docstring, plus `data_reader`/`data_writer` module naming. (Strong inference)
- **The "SuperTable" is a logical namespace.** An organization contains multiple SuperTables, each containing multiple tables. This is a multi-tenant data warehouse. (Strong inference)
- **The product name "AgenticBI" is a companion frontend.** References to "Agentic BI frontend", "AgenticBI Widgets page", "AgenticBI Saved Queries page" in docstrings and user preferences suggest a web UI that consumes `__app_state__` data. (Strong inference)
- **Redis is used for data pipeline coordination.** `RedisCatalog` stores snapshot versions and timestamps, suggesting a batch data pipeline that writes snapshots and records metadata in Redis. (Reasonable inference)

### Unknown / Not Visible in Provided Snippet

- **MetaReader implementation details:** How schemas, stats, and table lists are resolved. Whether RBAC filtering happens at MetaReader or data_reader level.
- **DataReader.query_sql implementation:** How the query engine is selected, how DuckDB is configured, how Parquet files are discovered.
- **DataWriter.write implementation:** How `overwrite_columns` works, how tombstone deletion interacts with the storage layer, whether writes are transactional.
- **RBAC implementation:** How `restrict_read_access` and `check_write_access` work, what row/column-level policies are supported.
- **RedisCatalog structure:** Full Redis key schema, how snapshot versions are managed.
- **SQLParser implementation:** How robust the SQL parsing is, what table reference formats are supported.
- **AgenticBI frontend:** The web UI that consumes `__app_state__` is referenced but not provided.
- **Engine enum values:** What engines besides AUTO are available (DuckDB, Polars, etc.).
- **Deployment topology:** Whether the server runs as a sidecar, standalone service, or embedded in a larger application.

---

## 14. AI Consumption Notes

**Canonical feature name:** Supertable MCP Server

**Alternative names / aliases:** `supertable-mcp`, `mcp_server`, `supertable.mcp`, Agentic BI MCP layer

**Main responsibilities:**
- Expose Supertable data platform to LLM agents via MCP protocol
- Enforce read-only SQL access with RBAC
- Manage self-learning AI context (catalog, annotations, feedback)
- Persist frontend application state
- Support stdio, Streamable HTTP, and SSE transports

**Important entities:** SuperTable, Organization, Role, Table, Column, Catalog entry, Annotation (categories: sql/terminology/visualization/scope/domain), Feedback (rating: thumbs_up/thumbs_down/neutral), App state (namespaces: chat/dashboard/widget/query/note)

**Important workflows:**
- Session initialization via `list_tables` → catalog/feedback/annotations/system_hint
- Query planning: catalog → profile_column → validate_sql → query_sql → submit_feedback
- Annotation lifecycle: store → get → delete (tombstone)
- Auto-discovery → store_catalog bootstrap
- Multi-transport deployment (stdio/HTTP/SSE)

**Integration points:** MCP protocol (JSON-RPC 2.0), Supertable MetaReader, DataReader, DataWriter, RBAC access control, Redis catalog, DuckDB query engine, Parquet storage

**Business purpose keywords:** agentic BI, AI analytics, self-learning, feedback loop, domain annotations, read-only SQL, RBAC, multi-tenant data warehouse, catalog-driven query planning

**Architecture keywords:** MCP server, FastMCP, JSON-RPC 2.0, stdio transport, streamable HTTP, ASGI middleware, AnyIO capacity limiter, thread offloading, tombstone deletion, eager context loading, tool-oriented API

**Follow-up files for completeness:**
- `supertable/meta_reader.py` — MetaReader implementation, list_supers, list_tables
- `supertable/data_reader.py` — query_sql implementation, engine enum
- `supertable/data_writer.py` — DataWriter.write() implementation
- `supertable/rbac/access_control.py` — RBAC enforcement logic
- `supertable/redis_catalog.py` — RedisCatalog implementation
- `supertable/utils/sql_parser.py` — SQLParser implementation
- AgenticBI frontend code — web UI consuming `__app_state__`

---

## 15. Suggested Documentation Tags

- `business-domain:analytics`
- `business-domain:business-intelligence`
- `business-domain:agentic-bi`
- `feature-type:ai-integration`
- `feature-type:data-access-layer`
- `feature-type:mcp-server`
- `feature-type:feedback-loop`
- `feature-type:annotation-system`
- `feature-type:catalog-management`
- `feature-type:app-state-persistence`
- `feature-type:data-profiling`
- `feature-type:auto-discovery`
- `technical-layer:api`
- `technical-layer:integration`
- `technical-layer:protocol-bridge`
- `architecture-style:tool-oriented`
- `architecture-style:json-rpc`
- `architecture-style:multi-transport`
- `architecture-style:async-with-thread-offload`
- `dependency:mcp-sdk`
- `dependency:fastmcp`
- `dependency:anyio`
- `dependency:fastapi`
- `dependency:duckdb`
- `dependency:pyarrow`
- `dependency:redis`
- `operational:concurrency-limited`
- `operational:rbac-enforced`
- `operational:read-only-sql`
- `operational:multi-tenant`
- `security:hmac-auth`
- `security:sql-injection-prevention`
- `security:input-validation`

---

## Merge Readiness

- **Suggested canonical name:** `supertable-mcp-server`
- **Standalone or partial:** This is a **substantially complete** analysis of the MCP layer. The server, clients, proxy, gateway, and test UIs are fully covered. The internal Supertable platform modules (MetaReader, DataReader, DataWriter, RBAC, RedisCatalog, SQLParser) are referenced but not analyzed.
- **Related components for future merge:**
  - `supertable.meta_reader` — metadata access layer
  - `supertable.data_reader` — query execution layer
  - `supertable.data_writer` — data write layer
  - `supertable.rbac` — access control layer
  - `supertable.redis_catalog` — Redis-based metadata catalog
  - `supertable.utils.sql_parser` — SQL parsing utility
  - AgenticBI frontend — companion web UI
- **What would most improve certainty:** Analysis of `data_reader.py` (to understand query engine mechanics and engine enum), `data_writer.py` (to understand write semantics and tombstone handling), and `access_control.py` (to understand RBAC granularity).
- **Recommended merge key phrases:** `mcp-server`, `supertable-mcp`, `agentic-bi`, `catalog-annotations-feedback`, `query-sql`, `mcp-tools`, `streamable-http-transport`
