# Feature / Component Reverse Engineering

## 1. Executive Summary

This document reverse-engineers the **SuperTable Execute SQL** feature — a full-featured, browser-based SQL workbench embedded within the SuperTable Reflection admin UI. The primary artifact is `execute.html`, a ~4,600-line Jinja2/HTML template that implements a **Command Center** layout for interactive SQL query authoring, execution, visualization, pivoting, profiling, and RBAC role creation.

**Main responsibility:** Provide administrators, analysts, and data engineers with a rich IDE-like interface to write and run SQL queries against SuperTable data lakes, inspect results across five output tabs (Result, Chart, Pivot, Timings, Plan), manage multiple execution sessions, explore schemas with IntelliSense, and persist queries as RBAC roles.

**Likely product capability:** Core data exploration and governance workbench — the primary interactive surface for querying SuperTable datasets.

**Business purpose:** Enables self-service data exploration, ad-hoc analytics, query profiling, performance debugging, and inline security policy (role) creation — reducing dependence on external BI/SQL tools and embedding governance directly into the query workflow.

**Technical role:** Client-side SPA-like template served by a FastAPI/Starlette backend (`supertable.reflection.execute`), communicating via JSON REST API endpoints (`/reflection/execute`, `/reflection/schema`, `/reflection/super`, `/reflection/rbac/roles`).

---

## 2. Functional Overview

### Product Capabilities Enabled

| Capability | Description | Target Actor |
|---|---|---|
| **Interactive SQL execution** | Write, execute, and view results of SELECT/WITH queries against SuperTable datasets | Analyst, Admin, Data Engineer |
| **Multi-engine query dispatch** | Choose between Auto, DuckDB Pro, DuckDB Lite, or Spark SQL engines | Power User, Admin |
| **Schema-aware IntelliSense** | Context-sensitive autocomplete for table names, columns, SQL keywords, and functions | All query authors |
| **Multi-session tabs** | Open multiple independent query execution sessions, each preserving query text, results, timings, and state across browser refreshes | Power User |
| **Result visualization (Chart)** | Auto-detect numeric/date columns and render Line, Bar, Area, or Doughnut charts via Chart.js | Analyst |
| **Client-side Pivot Table** | Drag-and-drop OLAP-style pivot with SUM, COUNT, AVG, MIN, MAX, MEDIAN aggregations, column/row dimensions, and filters | Analyst |
| **Execution Timings** | Animated bar chart showing internal execution phase breakdown (ms, % of total) | Performance Engineer |
| **Query Plan visualization** | Render DuckDB operator trees and Spark SQL physical/optimized/analyzed/parsed plan trees as interactive cards | Performance Engineer |
| **RBAC Role creation from query** | Auto-parse SQL WHERE clause into FilterBuilder JSON, detect tables, and save as a new RBAC reader role | Admin, Security |
| **Schema browser** | Collapsible tree with search, system-table toggle, click-to-insert for tables/columns | All users |
| **Query history** | Session-scoped history of executed queries with status, row count, duration, and click-to-restore | All users |
| **Export** | Copy results to clipboard (TSV) or download as CSV | Analyst |
| **Role-scoped execution** | Select an RBAC role to scope the query context, with dynamic role dropdown populated from RBAC API | Admin, Security |
| **Tenant/SuperTable selector** | Switch between organizations and SuperTables inline without page navigation | Multi-tenant user |

### Business Value

- **Self-service analytics:** Eliminates the need for external SQL clients. Users can explore data, build charts, and pivot results without leaving the product.
- **Governance integration:** The "Save as Role" feature bridges ad-hoc exploration and security policy creation — users can prototype a data scope in SQL and promote it to a formal RBAC role in a single workflow.
- **Multi-engine flexibility:** Supports DuckDB and Spark SQL, enabling the product to serve both low-latency OLAP (DuckDB) and distributed compute (Spark) use cases from a single UI.
- **Operational observability:** Timings and query plan tabs provide production-grade profiling, helping users and platform teams optimize queries without external tools.

### Feature Classification

- **Core product feature** — this is a primary interactive surface, not a support feature.
- **Strategic:** Combines querying, visualization, pivoting, profiling, and governance in one screen — a key differentiator for a data platform.

---

## 3. Technical Overview

### Architecture Style

- **Server-rendered SPA hybrid:** A Jinja2 template renders the initial shell with server-side context (session, org, role, token, tenant list), then client-side JavaScript handles all interactivity.
- **3-column "Command Center" layout:** Left (Schema Explorer) + Center (Editor + Results) + Right (History + Profile).
- **REST API integration:** All data operations (execute, schema, meta, roles) use `fetch()` calls to `/reflection/*` endpoints.
- **Session persistence via `sessionStorage`:** Execution tab states, schema cache, and query history survive browser refreshes within the same tab.

### Major Modules (Client-Side)

| Module | Lines (approx.) | Responsibility |
|---|---|---|
| CSS / Layout | 1–554 | 3-column layout, CodeMirror theme, result tables, pivot styles, plan tree styles |
| API Helper + Globals | 559–630 | `api()` fetch wrapper, org/role/super globals, `onSuperMetaLoaded` callback |
| Execution Session Manager | 632–1024 | Multi-tab session create/activate/close/persist/restore |
| Tenant Summary | 1026–1088 | `updateTenantSummary()` — files, rows, size, last-updated metrics |
| Error & Status | 1106–1228 | `displayError()`, `setStatus()`, `renderError()` |
| Result Table Renderer | 1230–1280 | `renderTable()` — builds HTML `<table>` from JSON rows |
| Timings Renderer | 1282–1375 | `renderTimingsChart()` — animated bar rows |
| Query Plan Renderer | 1377–1709 | DuckDB + Spark plan parsers and tree renderers |
| Schema Cache & Loader | 1711–1796 | `loadSchema()`, `_applySchema()`, `sessionStorage` cache |
| SQL Pre-flight Validator | 1798–1913 | `_validateSqlPreflight()` — reserved-keyword-as-alias detection |
| Execute SQL | 1927–2225 | `executeSQL()` — main execution flow with abort, timing, error handling |
| Clipboard + CSV Export | 2226–2364 | `copyToClipboard()`, `downloadCSV()` |
| SQL→FilterBuilder Converter | 2368–2547 | `_whereToFilterJSON()`, `_parseExpression()`, `_parseLeafCondition()` |
| Save as Role Modal | 2549–2684 | `openSaveViewModal()`, `saveViewNow()` — RBAC role creation |
| Visualization (Chart.js) | 2685–2989 | `populateVizControls()`, `renderVisualization()`, column classification |
| Pivot Table Engine | 2991–3431 | Client-side OLAP cube: `_pivotCompute()`, `_pivotRender()`, drag-and-drop |
| Document Ready + Bindings | 3431–3493 | jQuery initialization, event bindings, drop zones |
| CodeMirror + IntelliSense | 3820–4004 | Editor init, custom `sqlEnhanced` hint helper, auto-trigger |
| Command Center Adapter | 4027–4199 | Monkey-patches `__renderExecTabs`, `switchTab`, `setStatus`, `renderTimingsChart` for the new 3-column layout |
| Schema Tree Browser | 4201–4310 | `ccRenderSchemaTree()`, `ccFilterSchema()`, system-table toggle |
| Query History | 4312–4381 | `ccAddToHistory()`, `ccRenderHistory()`, sessionStorage persistence |
| Panel Toggle + Shortcuts | 4383–4433 | `ccTogglePanel()`, `ccToggleShortcuts()`, resize handle |
| SuperTable Selector + Role Selector | 4440–4578 | Inline org/super/role initialization, RBAC role dropdown fetch |

### Key Design Patterns

- **Dependency injection for routes:** The Python backend uses `attach_execute_routes(router, ...)` with explicit dependency injection of shared services (templates, auth guards, session helpers).
- **Monkey-patching for UI migration:** The "Command Center Adapter" section (lines 4027+) wraps original functions (`switchTab`, `setStatus`, etc.) to adapt them to the new 3-column layout without rewriting the core logic.
- **Session-per-tab isolation:** Each execution tab has its own query, results, timings, error state, and abort controller.
- **Graceful degradation:** Meta fetch uses a 2-second safety timer; schema loads fall back to cache; role fetch failure is non-fatal.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Elements | Relevance |
|---|---|---|---|
| `execute.html` | Full Execute SQL UI template (4,583 lines) | Jinja2 + CSS + JS: editor, results, charts, pivot, plan, RBAC modal, schema browser | **Primary artifact** — the entire feature UI |
| `common.py` (lines 1367–1389) | Route registration for execute module | `attach_execute_routes()` call with DI of auth, session, template deps | Backend integration point |
| `not_common.py` | Route registration for other modules | Does **not** register execute routes (those are in `common.py`) | Contextual — shows execute is registered separately |

---

## 5. Detailed Functional Capabilities

### 5.1 Interactive SQL Execution

- **Description:** User writes SQL in a CodeMirror editor and executes it against a selected SuperTable, scoped to an organization, SuperTable name, RBAC role, and engine.
- **Business purpose:** Primary data exploration workflow. Enables ad-hoc analytics and debugging.
- **Trigger:** Click "Execute" button or press ⌘/Ctrl+Enter.
- **Processing:**
  1. Client-side validation: must be SELECT/WITH; must have org, super, role selected.
  2. Pre-flight syntax check (`_validateSqlPreflight`) — detects reserved-keyword-as-alias typos.
  3. POST to `/reflection/execute` with `{ organization, super_name, role_name, query, engine, page, page_size }`.
  4. Parse response: extract `result` (rows), `meta.timings`, `meta.query_profile`.
  5. Check for embedded error in `meta.result_1 === 'ERROR'`.
  6. Render results in Result tab, populate Chart/Pivot controls, render timings and plan.
- **Output:** Result table (HTML), status badge with row count and duration.
- **Dependencies:** `/reflection/execute` API, active session, CodeMirror editor.
- **Constraints:** Only SELECT/WITH allowed (enforced client-side). Page size fixed at 1000.
- **Risks:** Large result sets may cause browser memory pressure (all rows kept in `window.__curRows`).
- **Confidence:** Explicit.

### 5.2 Multi-Engine Support

- **Description:** Dropdown allows selecting query execution engine: Auto, DuckDB Pro, DuckDB Lite, Spark SQL.
- **Business purpose:** Lets users route queries to the optimal compute backend — DuckDB for low-latency OLAP, Spark for distributed workloads.
- **Trigger:** Engine selector in top bar; value sent as `engine` param in execute request.
- **Processing:** Server-side engine selection (not visible in this code).
- **Constraints:** Engine values are hardcoded in the dropdown: `auto`, `duckdb_pro`, `duckdb_lite`, `spark_sql`.
- **Confidence:** Explicit (dropdown values and POST body).

### 5.3 Multi-Session Execution Tabs

- **Description:** Users can open multiple execution tabs (sessions), each with independent query text, result state, timings, error state, and inner tab selection.
- **Business purpose:** Productivity — run multiple queries in parallel without losing context.
- **Trigger:** Click "+" button or ⌘T.
- **Processing:** Sessions stored in `__execSessions` array, persisted to `sessionStorage` under key `st_exec_sessions:{org}:{sup}`. Active session state saved on tab switch, restored on page load.
- **Output:** Tab bar with colored dots (idle/success/error).
- **Constraints:** Cancelling a session aborts its `AbortController`; switching away from a running query cancels it.
- **Confidence:** Explicit.

### 5.4 Schema-Aware IntelliSense

- **Description:** CodeMirror autocomplete that suggests table names, column names, and SQL keywords based on the loaded schema.
- **Business purpose:** Reduces errors, speeds up query authoring, helps users discover table/column names.
- **Trigger:** Tab, Ctrl+Space, or typing `.` `,` `(` `)` or space.
- **Processing:**
  1. Schema loaded via POST `/reflection/schema` with org, super_name, role_name.
  2. Cached in `sessionStorage` under `st_table_schemas:{org}:{sup}`.
  3. Applied to `window.__hintTables` as `{ tableName: { columns: [...] } }`.
  4. Custom `sqlEnhanced` hint helper provides context-sensitive completions: after SELECT (columns + functions), after FROM (tables), after `table.` (columns of that table), default (all).
- **Dependencies:** `/reflection/schema` API.
- **Confidence:** Explicit.

### 5.5 Result Visualization (Chart.js)

- **Description:** Auto-renders Chart.js charts from query results with configurable chart type, X-axis, and Y-axis (multi-select).
- **Business purpose:** Instant visual analytics without external BI tools.
- **Trigger:** Automatic after successful query execution; user can change chart type/axes and re-render.
- **Processing:**
  1. Columns classified as date (x-axis), numeric (y-axis), or categorical (x-axis) via heuristic sampling.
  2. Chart type auto-selected: line for date x-axis, doughnut for ≤8 rows, bar otherwise.
  3. Supports Line, Bar, Area, Doughnut chart types.
- **Output:** Interactive Chart.js chart in the "Chart" tab.
- **Constraints:** Needs ≥2 rows and ≥1 numeric column. All data in memory. No server-side charting.
- **Confidence:** Explicit.

### 5.6 Client-Side Pivot Table

- **Description:** Drag-and-drop OLAP pivot table with configurable rows, columns, values (with aggregation function), and filters.
- **Business purpose:** Enables Excel-like slice-and-dice analysis within the query UI.
- **Trigger:** User drags field chips into Rows, Columns, Values, or Filters zones.
- **Processing:**
  1. Operates on in-memory `window.__curRows`.
  2. Supports SUM, COUNT, AVG, MIN, MAX, MEDIAN aggregation functions.
  3. Computes row × column buckets, applies filter predicates, generates grand totals.
  4. Renders as an HTML table with row/column dimension headers.
- **Output:** Pivot HTML table in the "Pivot" tab.
- **Constraints:** Pure client-side — limited to the result set already loaded (max 1000 rows per page).
- **Confidence:** Explicit.

### 5.7 Query Plan Visualization

- **Description:** Renders DuckDB operator tree (with timing, cardinality, scanned rows, extra info) and Spark SQL textual plan trees (physical, optimized, analyzed, parsed) as interactive card hierarchies.
- **Business purpose:** Production-grade query profiling — helps users understand execution costs and optimize queries.
- **Trigger:** Automatic after execution if `meta.query_profile` is present.
- **Processing:**
  - **DuckDB:** Reads `profile.children` tree; for each operator node, displays name, timing (ms, %), cardinality, rows scanned, extra info. Summary pills show latency, CPU, bytes read, rows returned, result size.
  - **Spark:** Parses indented text plan (`+- ` prefix tree) into structured nodes. Shows 4 plan sections (Physical → Optimized → Analyzed → Parsed) as clickable pills.
- **Output:** Tree of cards in the "Plan" tab, plus summary stat pills.
- **Confidence:** Explicit.

### 5.8 RBAC Role Creation from SQL ("Save as Role")

- **Description:** Parse the current SQL query to auto-detect tables, auto-convert WHERE clause to FilterBuilder JSON, and submit as a new RBAC reader role.
- **Business purpose:** Bridges ad-hoc data exploration with security governance — analysts can prototype a data scope and an admin can promote it to a formal RBAC policy.
- **Trigger:** Click "Save role" button in top bar.
- **Processing:**
  1. `_detectTablesFromSQL()` — regex extracts table names from FROM/JOIN clauses.
  2. `_whereToFilterJSON()` — recursive-descent parser converts WHERE clause into FilterBuilder JSON structure supporting AND, OR, NOT, BETWEEN, IS NULL, comparison operators, LIKE/ILIKE.
  3. Modal shows role name, detected tables, columns (* by default), parsed filters, source query.
  4. POST to `/reflection/rbac/roles` with `{ organization, super_name, role_name, role: 'reader', tables, columns, filters, source_query }`.
- **Output:** New RBAC role persisted server-side.
- **Dependencies:** `/reflection/rbac/roles` API.
- **Constraints:** Only SELECT/WITH queries. Parser is heuristic — complex SQL may not parse correctly. Role is always `reader`.
- **Risks:** SQL→FilterBuilder conversion does not handle subqueries, CTEs in WHERE, or complex expressions (acknowledged by `['*']` fallback).
- **Confidence:** Explicit.

### 5.9 SQL Pre-Flight Validation

- **Description:** Client-side tokenizer detects common SQL typos before sending to server — specifically, reserved keywords used as bare table aliases (e.g., `FROM t1 select alias`).
- **Business purpose:** Faster feedback loop — catch trivial errors without a server round-trip.
- **Trigger:** Called at start of `executeSQL()`.
- **Processing:** Tokenizes SQL, walks tokens looking for `FROM/JOIN <table> <reserved_keyword>` patterns. Returns error with line/column for CodeMirror squiggly underline.
- **Output:** Wavy red underline on offending token + inline error message.
- **Confidence:** Explicit.

### 5.10 Tenant Summary Panel

- **Description:** Displays SuperTable metadata — file count, row count, data size, last updated — in metric cards.
- **Business purpose:** Quick at-a-glance dataset health/size information.
- **Trigger:** Loaded via `onSuperMetaLoaded` callback from `/reflection/super` API.
- **Confidence:** Explicit (not visible in HTML body — inference: rendered elsewhere or in metric cards in the right panel). **Correction:** metric-card CSS is defined (lines 502–509) and `updateTenantSummary()` references specific DOM element IDs, so these cards exist in the template.

---

## 6. Classes, Functions, and Methods

### 6.1 Core Execution

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `executeSQL(page)` | Function | Main query execution: validate → pre-flight → POST → render results/errors/timings/plan | **Critical** |
| `api(path, opts)` | Function | Authenticated fetch wrapper; injects `X-Admin-Token` header | **Critical** |
| `extractExecuteError(payload)` | Function | Detects embedded errors in `meta.result_1 === 'ERROR'` responses | Important |
| `displayError(title, message, statusTextOverride)` | Function | Centralized error display: updates status, error div, clears results | Important |
| `setStatus(text, cls)` | Function | Updates status indicator badge (Ready/Running/Success/Error) | Important |
| `renderTable(rows, total, page, pageSize, onPage, statusText)` | Function | Builds HTML table from JSON rows, stores in `window.__curRows/Headers` | **Critical** |
| `switchTab(tabName)` | Function | Activates a result tab (result/visualize/pivot/timings/plan) | Important |

### 6.2 Session Management

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `__createExecSession(opts)` | Function | Factory for new execution session objects | Important |
| `__activateExecSession(id)` | Function | Switch active session, save old, restore new | Important |
| `__addExecSession()` | Function | Create and activate a new blank session | Important |
| `__closeExecSession(id)` | Function | Close session, cancel if running, activate neighbor | Important |
| `__saveActiveExecSessionState()` | Function | Persist current editor text + inner tab to session object + sessionStorage | Important |
| `__applyExecSessionState(session)` | Function | Restore editor, results, timings, errors, chart from session object | Important |
| `_persistExecSessions()` | Function | Serialize all sessions to sessionStorage | Supporting |
| `_restoreExecSessions()` | Function | Deserialize sessions from sessionStorage on page load | Supporting |
| `__cancelSessionExecution(session)` | Function | Abort fetch, clear timer, reset execution state | Supporting |

### 6.3 Schema & IntelliSense

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `loadSchema(forceRefresh)` | Function | Fetch schema from `/reflection/schema`, cache in sessionStorage | **Critical** |
| `_applySchema(schemaMap)` | Function | Apply schema map to `window.__hintTables` and CodeMirror hint options | Important |
| `buildSqlHintOptions()` | Function | Construct CodeMirror hintOptions from current schema | Supporting |
| `CodeMirror.hint.sqlEnhanced` | Helper | Context-sensitive autocomplete: tables after FROM, columns after SELECT/dot | **Critical** |
| `ccRenderSchemaTree(filter)` | Function | Render collapsible schema tree in left panel with search filter | Important |

### 6.4 Visualization & Pivot

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `populateVizControls()` | Function | Classify columns, populate X/Y dropdowns, auto-select chart type | Important |
| `renderVisualization()` | Function | Build and render Chart.js chart from current data + controls | Important |
| `_vizClassifyColumns(rows, headers)` | Function | Heuristic: split columns into x-candidates (date/categorical) and y-candidates (numeric) | Supporting |
| `_pivotCompute(rows, config)` | Function | Core pivot engine: bucket data by row/col keys, apply aggregations | Important |
| `_pivotRender(pivot)` | Function | Render pivot table HTML with grand totals | Important |
| `populatePivotControls()` | Function | Reset pivot config and render field pool + drop zones | Supporting |

### 6.5 Query Plan

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `renderQueryPlan(profile)` | Function | Dispatch to DuckDB or Spark plan renderer | Important |
| `_renderDuckDBPlan(profile, ...)` | Function | Render DuckDB operator tree with timing/cardinality cards and summary pills | Important |
| `_renderSparkPlan(profile, ...)` | Function | Parse Spark text plans and render as tree cards with section tabs | Important |
| `_sparkParsePlanText(text)` | Function | Parse indented Spark plan text into structured tree nodes | Supporting |
| `_planRenderNode(node, totalTiming, depth)` | Function | Recursive HTML builder for plan tree cards | Supporting |

### 6.6 RBAC Role Creation

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `openSaveViewModal()` | Function | Populate and show the Save as Role modal; auto-detect tables and convert WHERE to FilterBuilder JSON | Important |
| `saveViewNow()` | Function | Validate inputs and POST to `/reflection/rbac/roles` | Important |
| `_whereToFilterJSON(sql)` | Function | Top-level: extract WHERE clause and parse to FilterBuilder JSON | Important |
| `_parseExpression(expr)` | Function | Recursive-descent parser for AND/OR/NOT/leaf conditions | Supporting |
| `_parseLeafCondition(expr)` | Function | Parse single `col OP value` condition into FilterBuilder dict | Supporting |
| `_detectTablesFromSQL(sql)` | Function | Regex-extract table names from FROM/JOIN clauses | Supporting |

### 6.7 SQL Pre-flight Validation

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `_validateSqlPreflight(sql)` | Function | Tokenize SQL and detect reserved-keyword-as-alias patterns | Supporting |
| `_clearSqlErrorMarkers()` | Function | Clear CodeMirror error markers | Supporting |

### 6.8 Utilities & UI

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `copyToClipboard()` | Function | Copy result rows as TSV to clipboard | Supporting |
| `downloadCSV()` | Function | Download result rows as CSV file | Supporting |
| `ccAddToHistory(sql, rows, dur, ok)` | Function | Append to query history; persist in sessionStorage | Supporting |
| `ccRenderHistory()` | Function | Render history list in right panel | Supporting |
| `ccTogglePanel(side)` | Function | Collapse/expand left or right panel | Supporting |
| `ccToggleShortcuts()` | Function | Toggle keyboard shortcuts overlay | Supporting |
| `ccOnSuperChange(val)` | Function | Navigate to new SuperTable (page reload with URL params) | Supporting |
| `ccOnRoleChange(val)` | Function | Change active role, clear cached schema, reload schema | Important |
| `updateTenantSummary(superMeta)` | Function | Display file count, rows, size, last updated from metadata | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Page Load / Initialization

1. Server renders `execute.html` with Jinja2 context: `session_org`, `sel_sup`, `session_role_name`, `tenants`, `token`, `has_tenant`.
2. jQuery `$(document).ready` fires: binds sidebar, marks "Execute" nav active, mounts inner tabs, initializes execution sessions.
3. CodeMirror editor initialized with SQL mode, keybindings (⌘Enter → execute, Tab/Ctrl+Space → autocomplete).
4. `__initExecSessions()` either restores sessions from `sessionStorage` or creates a default "Execution 1".
5. Schema tree and history rendered from cache.
6. IIFE fetches `/reflection/super` for metadata → calls `onSuperMetaLoaded()` → updates tenant summary + loads schema.
7. IIFE fetches `/reflection/rbac/roles` to populate role dropdown.

### 7.2 Query Execution Flow

1. User writes SQL in CodeMirror.
2. Triggers: click Execute button or ⌘/Ctrl+Enter.
3. `executeSQL(1)` called.
4. **Validation:** Must have org, super, role. Must start with SELECT/WITH.
5. **Pre-flight:** `_validateSqlPreflight()` tokenizes and checks for alias typos. If error: squiggly underline + inline error; abort.
6. Execution timer starts (updates status every 150ms).
7. `AbortController` created for cancellation.
8. `POST /reflection/execute` with body: `{ organization, super_name, role_name, query, engine, page: 1, page_size: 1000 }`.
9. On success: `renderTable()`, `populateVizControls()`, `populatePivotControls()`, `renderTimingsChart()`, `renderQueryPlan()`. History logged.
10. On embedded error (`meta.result_1 === 'ERROR'`): `displayError()` with parsed message.
11. On HTTP error: `displayError()` with status text.
12. On abort: set status to "Cancelled".
13. Session state persisted to `sessionStorage`.

### 7.3 Save as RBAC Role Flow

1. User clicks "Save role" in top bar.
2. `openSaveViewModal()`: validates query is SELECT/WITH, auto-detects tables via regex, converts WHERE clause to FilterBuilder JSON.
3. Modal displays: org, super, role name input, tables (editable), columns (* default), filters (editable JSON), source query (readonly).
4. User edits fields and clicks "Save Role".
5. `saveViewNow()`: validates inputs, parses tables/columns/filters.
6. `POST /reflection/rbac/roles` with `{ organization, super_name, role_name, role: 'reader', tables, columns, filters, source_query }`.
7. On success: "Role saved successfully." message, modal closes.
8. On error: inline error in modal.

### 7.4 Schema Loading Flow

1. `onSuperMetaLoaded()` triggers `loadSchema()`.
2. Check `sessionStorage` cache first; if hit and not forced, apply cached schema.
3. Otherwise, `POST /reflection/schema` with `{ organization, super_name, role_name }`.
4. Response: `{ status: 'ok', schema: [ { tableName: [col1, col2, ...] }, ... ] }`.
5. Convert to `{ tableName: { columns: [...] } }` map.
6. Cache in `sessionStorage`.
7. Apply to `window.__hintTables` + update CodeMirror hintOptions.
8. Re-render schema tree in left panel.

---

## 8. Data Model and Information Flow

### Core Client-Side Data Structures

| Structure | Shape | Purpose |
|---|---|---|
| `window.__curRows` | `Array<Object>` | Current query result rows (JSON objects keyed by column name) |
| `window.__curHeaders` | `Array<string>` | Column names of current result set |
| `window.__hintTables` | `{ tableName: { columns: string[] } }` | Schema map for IntelliSense |
| `__execSessions` | `Array<ExecSession>` | All execution tab states |
| `window.__pivotConfig` | `{ rows: string[], columns: string[], values: {field, agg}[], filters: {field, allowed}[] }` | Pivot table configuration |
| `window.__vizChartInstance` | `Chart` | Active Chart.js instance |
| `__queryHistory` | `Array<{ sql, rows, dur, ok, time }>` | Session-scoped query history |

### ExecSession Object

```
{
  id: string,              // 'exec-N'
  name: string,            // 'Execution N'
  query: string,           // SQL text
  innerTab: string,        // Active result tab name
  statusText: string,      // Status display text
  statusCls: string,       // CSS class (error/executing/'')
  rows: Array<Object>,     // Result rows
  timings: Array|null,     // Timing phases
  queryProfile: Object|null, // Query plan data
  errorVisible: boolean,
  errorTitle: string,
  errorMessage: string,
  abortController: AbortController|null,
  isExecuting: boolean
}
```

### API Request/Response Contracts

**Execute Query:**
- Request: `POST /reflection/execute` → `{ organization, super_name, role_name, query, engine, page, page_size }`
- Response: `{ status: 'ok'|'error', result: [...], total_count, duration_ms, meta: { timings, query_profile, result_1, result_2 }, message }`

**Load Schema:**
- Request: `POST /reflection/schema` → `{ organization, super_name, role_name }`
- Response: `{ status: 'ok', schema: [ { tableName: [col, ...] }, ... ] }`

**Save RBAC Role:**
- Request: `POST /reflection/rbac/roles` → `{ organization, super_name, role_name, role: 'reader', tables, columns, filters, source_query }`
- Response: Success or error.

**Fetch Roles:**
- Request: `GET /reflection/rbac/roles?org=...&sup=...`
- Response: `Array<{ role_name|name|role_id }>` or `{ roles: [...] }`

**Fetch Metadata:**
- Request: `GET /reflection/super?org=...&sup=...`
- Response: `{ meta: { files, rows, size, updated_utc } }` or flat object.

### FilterBuilder JSON Schema (produced by SQL→Filter converter)

```
// Top-level: array (AND semantics)
[
  { "column_name": { "operation": "=", "type": "value", "value": "EU" } },
  { "OR": [ condition, condition ] },
  { "NOT": condition },
  { "column_name": { "range": [ { type, operation, value }, ... ] } }
]
```

### Persistence

| Data | Storage | Scope | Key Pattern |
|---|---|---|---|
| Execution sessions | `sessionStorage` | Per org+super | `st_exec_sessions:{org}:{sup}` |
| Schema cache | `sessionStorage` | Per org+super | `st_table_schemas:{org}:{sup}` |
| Query history | `sessionStorage` | Per org+super | `st_query_history:{org}:{sup}` |
| Sidebar collapsed | `localStorage` | Global | `st_sidebar_collapsed` |

---

## 9. Dependencies and Integrations

### External Libraries (Client-Side)

| Library | Version | Purpose |
|---|---|---|
| jQuery | 3.6.0 | DOM manipulation, Bootstrap modal triggers |
| Bootstrap | 4.5.2 | Modal component, grid utilities |
| CodeMirror | 5.65.5 | SQL editor with syntax highlighting, bracket matching, autocomplete |
| Chart.js | 4.4.1 | Line/Bar/Area/Doughnut charts for result visualization |
| Font Awesome | 6.5.1 | Icons throughout the UI |
| Inter (Google Fonts) | Variable | UI typography |

### Internal API Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/reflection/execute` | POST | Execute SQL query |
| `/reflection/schema` | POST | Fetch table/column schemas |
| `/reflection/super` | GET | Fetch SuperTable metadata |
| `/reflection/rbac/roles` | GET/POST | List/create RBAC roles |
| `/reflection` | GET | Admin home (back navigation) |

### Backend Module Dependencies (from `common.py`)

| Dependency | Injected As | Purpose |
|---|---|---|
| `router` | FastAPI/Starlette router | HTTP route registration |
| `templates` | Jinja2Templates | Template rendering |
| `_is_authorized` | `is_authorized` | Request authorization check |
| `_no_store` | `no_store` | Cache-control header helper |
| `_get_provided_token` | `get_provided_token` | Extract auth token from request |
| `discover_pairs` | `discover_pairs` | Enumerate org/super pairs |
| `resolve_pair` | `resolve_pair` | Resolve specific org/super pair |
| `inject_session_into_ctx` | Template context enrichment | Session data in templates |
| `get_session` | `get_session` | Read session from request |
| `logged_in_guard_api` | API guard | Ensure user is logged in |
| `admin_guard_api` | API guard | Ensure user is admin |

---

## 10. Architecture Positioning

### System Position

```
Browser (execute.html)
    ├── POST /reflection/execute ──→ Execute Module (execute.py) ──→ DuckDB / Spark SQL Engine
    ├── POST /reflection/schema ───→ Schema endpoint ──→ Catalog / Metadata Store
    ├── GET  /reflection/super ────→ SuperTable metadata ──→ Catalog
    ├── GET  /reflection/rbac/roles → RBAC Module (rbac.py) ──→ Role Store
    └── POST /reflection/rbac/roles → RBAC Module (rbac.py) ──→ Role Store
```

### Upstream Components

- **Sidebar (`sidebar.html`):** Navigation; included via Jinja2 `{% include %}`.
- **Session/Auth system:** Provides `session_org`, `session_role_name`, `token` via Jinja2 context.
- **`common.py`:** Registers execute routes with shared infrastructure (auth, templates, session).

### Downstream Dependencies

- **Execute engine:** Server-side query dispatcher to DuckDB Pro, DuckDB Lite, or Spark SQL.
- **Catalog/Metadata:** Provides schema info and SuperTable metadata.
- **RBAC system:** Stores and enforces role-based access policies.
- **Redis:** Not directly used by execute routes (not injected), but used by adjacent modules.

### Boundary Analysis

- The execute feature is a **self-contained UI module** with its own route registration (`attach_execute_routes`).
- It is registered in `common.py` (not `not_common.py`), suggesting it is part of the "core" reflection UI.
- The backend `execute.py` module is not provided but is clearly the server-side counterpart.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Problem solved** | Self-service data exploration, ad-hoc analytics, query profiling, and RBAC role creation from a single UI |
| **Revenue relevance** | Core product feature — likely a key selling point for data platform buyers |
| **Differentiation** | Combines SQL IDE + visualization + pivot + profiling + RBAC governance in one screen. The SQL→FilterBuilder→Role workflow is unique |
| **Strategic value** | Reduces churn by keeping users inside the platform instead of using external SQL clients or BI tools |
| **Loss impact** | Without this, the product loses its primary interactive data exploration surface |
| **Category** | Revenue-related (core feature), Efficiency-related (self-service), Governance-related (RBAC integration) |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | High — ~4,600 lines of tightly integrated HTML/CSS/JS with 6+ subsystems |
| **Architectural maturity** | Medium-high — clear separation of concerns within the JS (sessions, rendering, schema, pivot, plan are logically isolated), but all in one file |
| **Maintainability** | Medium — monkey-patching adapter pattern adds fragility; single-file monolith makes parallel development hard |
| **Extensibility** | Medium — new result tabs can be added to the tab strip; new engines to the dropdown; new snippet types to the snippets section |
| **Operational sensitivity** | High — this is the primary interactive surface; failures here directly impact user experience |
| **Performance** | Risk: all result rows stored in memory (`window.__curRows`); pivot/chart computed client-side; 1000-row page size is a reasonable cap |
| **Reliability** | Good — graceful degradation with safety timers, abort controllers, try/catch on all storage operations, non-fatal meta/role fetch failures |
| **Security** | Auth token injected via Jinja2 `{{ token|e }}` and sent as `X-Admin-Token` header; `admin_guard_api` protects backend; client enforces SELECT/WITH only |
| **Testing** | No test code visible; the pre-flight validator and SQL→FilterBuilder parser are complex enough to warrant unit tests |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- The execute feature renders at `/reflection/execute` as a Jinja2 template.
- SQL execution is a POST to `/reflection/execute` with org, super_name, role_name, query, engine, page, page_size.
- Only SELECT/WITH queries are allowed (client-side enforcement).
- Four execution engines are offered: auto, duckdb_pro, duckdb_lite, spark_sql.
- Sessions are persisted in `sessionStorage` and restored on page load.
- Schema is loaded from `/reflection/schema` and cached in `sessionStorage`.
- RBAC roles are created via POST to `/reflection/rbac/roles` with FilterBuilder JSON.
- The SQL→FilterBuilder converter handles AND, OR, NOT, BETWEEN, IS NULL, comparison operators, LIKE/ILIKE.
- DuckDB and Spark SQL query plans are both supported with different rendering paths.
- The Pivot table supports SUM, COUNT, AVG, MIN, MAX, MEDIAN aggregation.
- Route registration uses explicit dependency injection via `attach_execute_routes()`.

### Reasonable Inferences

- The backend `execute.py` module contains the actual route handler implementations (page rendering + API handlers). It is not provided but is referenced.
- Server-side engine selection likely routes to different compute backends (DuckDB native vs. Spark cluster).
- The `superadmin` role is a privileged default — it is always ensured in the role dropdown.
- `FilterBuilder` is a shared internal abstraction used across RBAC, execute, and likely other modules for row-level security filtering.
- The "Command Center" layout is a recent redesign — evidenced by the monkey-patching adapter layer and comments like "replaces selection.html".
- The `X-Admin-Token` authentication suggests a token-based admin session mechanism, likely cookie-issued and forwarded as a header for API calls.

### Unknown / Not Visible in Provided Snippet

- The backend implementation of `attach_execute_routes()` and the `/reflection/execute` handler.
- Server-side query execution pipeline (engine dispatch, result pagination, profiling instrumentation).
- How `role_name` is enforced server-side (row/column filtering, table access).
- Whether result pagination beyond page 1 is actually used (the client always sends `page: 1`).
- The `renderTable` function in the backend — whether it does server-side HTML rendering or returns JSON only.
- Full authentication/authorization flow (JWT, session cookies, etc.).
- Rate limiting or query timeout enforcement.
- The `selection.html` component that was replaced by the inline SuperTable selector.
- How `has_tenant` is determined in the Jinja2 context.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `SuperTable Execute SQL` / `SQL Command Center`
- **Alternative names/aliases:** Execute tab, Command Center, SQL workbench, Execute SQL page
- **Main responsibilities:** SQL query authoring, execution, result rendering, charting, pivoting, timing analysis, query plan visualization, RBAC role creation, schema browsing, multi-session management
- **Important entities:** ExecSession, SuperTable (org + super_name), RBAC Role, FilterBuilder JSON, Query Plan (DuckDB/Spark), Schema Map
- **Important workflows:** Query execution (validate → preflight → execute → render), Save as Role (parse SQL → FilterBuilder JSON → POST role), Schema loading (fetch → cache → apply to IntelliSense)
- **Integration points:** `/reflection/execute` (POST), `/reflection/schema` (POST), `/reflection/super` (GET), `/reflection/rbac/roles` (GET/POST)
- **Business purpose keywords:** self-service analytics, ad-hoc SQL, data exploration, RBAC governance, multi-engine query, query profiling, pivot table, chart visualization
- **Architecture keywords:** Jinja2 template, SPA hybrid, CodeMirror, Chart.js, sessionStorage persistence, dependency injection, monkey-patching adapter, FastAPI/Starlette
- **Follow-up files for completeness:**
  - `supertable/reflection/execute.py` — backend route handlers (CRITICAL)
  - `supertable/reflection/common.py` — full session/auth infrastructure (partially analyzed)
  - `supertable/reflection/rbac.py` — RBAC role management backend
  - `sidebar.html` — navigation context
  - `selection.html` — legacy SuperTable selector (now replaced)
  - FilterBuilder implementation — shared filter abstraction

---

## 15. Suggested Documentation Tags

```
business-domain: data-platform, analytics, governance
feature-type: core-product, interactive-ui, sql-workbench
technical-layer: presentation, client-side, template
architecture-style: spa-hybrid, jinja2-template, rest-api-consumer
dependencies: codemirror, chartjs, jquery, bootstrap, font-awesome
compute-engines: duckdb, spark-sql
data-patterns: client-side-olap, pivot-table, sessionstorage-persistence
security: rbac, role-creation, token-auth, select-only-enforcement
operational: multi-session, query-profiling, execution-timing, query-plan
integration: reflection-api, rbac-api, schema-api, metadata-api
```

---

## Merge Readiness

- **Suggested canonical name:** `execute-sql-command-center`
- **Standalone or partial:** Partial — the backend `execute.py` is critical and not provided. Frontend is fully analyzed.
- **Related components for merge:**
  - `execute.py` — backend route handlers
  - `common.py` — shared auth/session infrastructure (partially covered)
  - `rbac.py` — RBAC role CRUD backend
  - `sidebar.html` — navigation shell
  - FilterBuilder subsystem — shared filter abstraction used by RBAC and execute
- **What would most improve certainty:**
  - `execute.py` backend code — would reveal server-side execution logic, engine dispatch, pagination, and profiling instrumentation
  - FilterBuilder implementation — would validate the SQL→filter JSON contract
- **Recommended merge key phrases:** `execute sql`, `command center`, `sql workbench`, `reflection execute`, `query execution`, `rbac role creation from sql`, `query plan visualization`, `pivot table engine`
