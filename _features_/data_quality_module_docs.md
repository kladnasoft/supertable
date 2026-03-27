# Feature / Component Reverse Engineering

## 1. Executive Summary

This code implements the **Data Quality (DQ) module** of SuperTable's "Reflection" admin UI — a full-featured, automated data profiling, anomaly detection, and quality scoring subsystem embedded directly into a data platform product.

**Main responsibility:** Continuously and on-demand assess the health of every table stored in a SuperTable instance by computing column-level statistics, detecting anomalies versus historical baselines, scoring data quality 0–100, enforcing user-defined custom validation rules, and persisting all results for trend analysis.

**Product capability:** Provides operators and data engineers a command-center dashboard for monitoring data quality across all tables — including scheduled background checks, post-ingest reactive checks, configurable thresholds, custom validation rules, schema drift detection, and a queryable audit history.

**Technical role:** A FastAPI route module plugged into the shared Reflection router, backed by Redis for configuration/caching, DuckDB SQL execution via `DataReader` for profiling, and Parquet-based history via `DataWriter`. It includes a daemon scheduler thread, an anomaly detection engine, and a ~2,000-line SPA-style HTML frontend.

**Business purpose:** Data observability and governance — enabling trust in analytical data, preventing bad data from reaching downstream consumers, and providing a quality audit trail queryable via the platform's MCP/AgenticBI interface.

---

## 2. Functional Overview

### What the product can do because of this code

1. **Automatic data profiling** — Every table is profiled on a configurable cron schedule (default: every 4 hours for quick, daily for deep). Profiles compute NULL rates, distinct counts, min/max, mean/stddev, zero/negative rates, and cardinality for every column in a single SQL query.

2. **Post-ingest quality checks** — When data is loaded into a table, a debounced quality check is triggered automatically (with lock and cooldown protection to avoid overloading).

3. **Anomaly detection** — Five built-in anomaly detectors (A1–A5) compare current profiles against the previous run, flagging: row-count spikes/drops, NULL rate spikes, mean drift via z-score, cardinality shifts, and min/max boundary breaches. Schema drift (column additions, removals, type changes) is also detected.

4. **Quality scoring** — A composite 0–100 score is computed per table, deducting points for NULL incompleteness and anomaly severity.

5. **Deep profiling** — Per-column deep profiles compute Shannon entropy, string length distributions, percentiles (IQR), top-N frequency analysis, placeholder detection, and histogram bucketing.

6. **Custom validation rules** — Users define rules (column min/max, NULL rate thresholds, row count minimums, allowed-value sets, arbitrary SQL) that are evaluated on their own schedule and contribute to the quality score.

7. **Configuration management** — Global defaults can be overridden per table. Check toggles, thresholds, schedule cron expressions, and incremental scope are all configurable via API and persisted in Redis.

8. **Quality history** — Every check result is persisted to a `__data_quality__` Parquet table (with Redis LIST fallback), making it queryable via MCP SQL for trend charts and AgenticBI summaries.

9. **Dashboard UI** — A rich single-page dashboard with: table list (filterable by status), quality score display, anomaly details, per-column drill-down, check configuration panel, custom rule editor, schedule configuration, history trend charts, and one-click "Run Now" / "Run All" actions.

### Target users/actors

| Actor | Usage |
|---|---|
| Data Engineer | Configure checks, set thresholds, create custom rules, investigate anomalies |
| Platform Admin (superuser) | Configure global defaults, schedules, run-all checks |
| Automated Scheduler | Background daemon executing periodic and post-ingest checks |
| AgenticBI / MCP Client | Queries `__data_quality__` for natural-language quality summaries and trend charts |

### Business value

- **Data trust** — Quantified quality scores let teams know if data is reliable before using it in analytics or ML.
- **Regression prevention** — Anomaly detection catches data pipeline issues (broken sources, schema changes, data loss) before they cascade downstream.
- **Compliance / audit** — Historical quality records in a queryable table provide an audit trail.
- **Self-service governance** — Non-SQL users can configure quality rules through the UI without writing code.
- **Platform differentiation** — Built-in data quality monitoring is a premium feature in the data platform market.

---

## 3. Technical Overview

### Architecture style
- **Modular monolith:** The DQ module is a sub-package (`supertable.reflection.quality`) plugged into a FastAPI router via dependency injection (`attach_quality_routes()`).
- **Dependency injection pattern:** All shared dependencies (router, templates, Redis, catalog, auth guards) are injected as function parameters, not imported globally — enabling testability and decoupling.
- **Background processing:** A singleton daemon thread runs a 60-second tick loop for cron-based and post-ingest checks, using Redis-based distributed locking.
- **Dual storage:** Redis for hot operational state (config, latest results, anomalies, locks); Parquet via DataWriter for durable history.

### Major modules

| Module | Role |
|---|---|
| `config.py` | Redis-backed CRUD for DQ configuration, custom rules, schedules, latest results, anomalies |
| `checker.py` | SQL generation (quick + deep profiles), result parsing, custom rule SQL/evaluation, quality score computation |
| `anomaly.py` | Anomaly detection engine (A1–A5), schema drift detection |
| `scheduler.py` | Background scheduler thread, post-ingest notification, distributed lock/cooldown, check orchestration |
| `routes.py` | FastAPI route handlers (page + 15+ API endpoints), manual run triggers |
| `history.py` | History persistence to `__data_quality__` Parquet table via DataWriter, Redis LIST fallback |
| `quality.html` | ~2,000 lines SPA dashboard with tabbed navigation, real-time data loading, SVG chart rendering |
| `__init__.py` | Package entry point; exports `attach_quality_routes` |

### Key control flows

1. **Scheduled quick check:** Scheduler tick → cron match → acquire Redis lock → read schema via MetaReader → build aggregation SQL → execute via DataReader → parse results → detect anomalies → compute score → store latest in Redis → store per-column results → write history to Parquet.

2. **Post-ingest check:** Ingest code calls `notify_ingest()` → sets Redis pending key → scheduler tick picks it up → respects cooldown/lock → executes quick/custom check → clears pending key.

3. **Manual run:** API `POST /reflection/quality/run` → spawns daemon thread → calls `_run_quick_check` or `_run_deep_check`.

### Key data flows

- **Schema:** MetaReader (Redis metadata) → column list → SQL builder
- **Profile data:** DataReader (DuckDB over Parquet) → single-row aggregation → parsed column stats
- **Anomaly data:** Previous latest (Redis) + current parsed → anomaly detector → anomaly list
- **History:** Latest result dict → Polars DataFrame → Arrow table → DataWriter → Parquet on MinIO/local

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Elements | Relevance |
|---|---|---|---|
| `config.py` | Redis configuration CRUD | `DQConfig` class, `BUILTIN_CHECKS` dict, key layout documentation | Core — all state management |
| `checker.py` | SQL generation + evaluation | `build_quick_sql()`, `build_deep_*_sql()`, `build_custom_rule_sql()`, `compute_quality_score()` | Core — profiling logic |
| `anomaly.py` | Anomaly detection | `detect_anomalies()`, `detect_schema_drift()` | Core — intelligence layer |
| `scheduler.py` | Background execution | `start_quality_scheduler()`, `_scheduler_tick()`, `_try_run_check()`, `_run_quick_check()`, `_run_deep_check()`, `_run_custom_check()`, `notify_ingest()` | Core — orchestration |
| `routes.py` | HTTP API + page serving | `attach_quality_routes()`, 15+ endpoints, background thread triggers | Core — API surface |
| `history.py` | Persistent audit trail | `write_history()`, `write_history_via_sql()`, `build_history_row()` | Important — durability |
| `quality.html` | Frontend dashboard | Tabbed SPA, SVG score chart, filter/search, config panels, rule editor | Important — user experience |
| `__init__.py` | Package export | `attach_quality_routes` | Supporting |
| `common.py` | Shared Reflection infrastructure | Settings, Redis, session/auth, catalog, router, auth guards | Foundation — shared by all modules |
| `not_common.py` | Route registration orchestrator | Calls `attach_quality_routes()` + 12 other module attachments | Supporting — wiring |

---

## 5. Detailed Functional Capabilities

### 5.1 Quick Profile Check

- **Description:** Single-SQL aggregation computing NULL count, distinct count, min, max, avg, stddev, zero count, negative count for every column in one query.
- **Business purpose:** Lightweight, fast assessment of table health suitable for frequent execution.
- **Trigger:** Cron schedule (default `0 */4 * * *`), post-ingest, or manual.
- **Processing:** `build_quick_sql()` generates DuckDB-compatible SQL → `DataReader.execute()` → `parse_quick_result()` extracts per-column stats → `detect_anomalies()` compares with previous → `compute_quality_score()` → store in Redis + Parquet history.
- **Output:** Table-level quality score (0–100), status (ok/warning/critical), per-column stats, anomaly list.
- **Dependencies:** MetaReader (schema), DataReader (SQL execution), Redis, DataWriter (history).
- **Constraints:** Skips `_sys_*` columns. Incremental mode supported via `WHERE` clause.
- **Risks:** Large tables may have slow aggregation; no query timeout visible.
- **Confidence:** Explicit.

### 5.2 Deep Profile Check

- **Description:** Per-column detailed analysis: Shannon entropy, string length stats, variance, percentiles (p25/p50/p75), top-N frequency, histogram buckets, placeholder rate, zero/null rate.
- **Business purpose:** Deep statistical understanding of column distributions for data scientists and advanced monitoring.
- **Trigger:** Cron schedule (default `0 2 * * *`), or manual.
- **Processing:** For each column: select appropriate template (string or numeric) → execute → store per-column result in Redis.
- **Output:** Rich per-column stats stored at `dq:latest:{table}:{column}`.
- **Dependencies:** Same as quick, plus category-specific SQL templates.
- **Constraints:** Only runs for `numeric` and `string` columns. Date/bool/other skipped.
- **Risks:** N queries for N columns — can be slow on wide tables.
- **Confidence:** Explicit.

### 5.3 Anomaly Detection (A1–A5 + Schema Drift)

- **Description:** Compares current profile with previous run to detect statistical anomalies.
- **Business purpose:** Automated alerting on data regressions without manual threshold monitoring.
- **Checks:**
  - A1: Row count spike/drop (configurable % threshold, default 30%)
  - A2: NULL rate spike (configurable pp threshold, default 5pp)
  - A3: Mean drift via z-score (configurable σ threshold, default 2.0)
  - A4: Cardinality shift (configurable % threshold, default 50%)
  - A5: Min/max boundary breach (any new value outside historical range)
  - A5_C5: Zero/negative rate spike
  - A_T3: Schema drift (column additions, removals, type changes)
- **Severity logic:** `critical` if the value exceeds 2× the threshold; `warning` otherwise.
- **Dependencies:** Previous check result from Redis.
- **Constraints:** No anomalies on the first run (no baseline). Pure computation — no SQL.
- **Confidence:** Explicit.

### 5.4 Custom Validation Rules

- **Description:** User-defined rules stored in Redis, evaluated via SQL, contributing to the quality score.
- **Rule types:** `column_min`, `column_max`, `null_rate_max`, `row_count_min`, `distinct_in` (allowed-value set), `custom_sql` (arbitrary SQL).
- **Business purpose:** Domain-specific data contracts (e.g., "price must be > 0", "status must be in [A, B, C]").
- **Trigger:** Own cron schedule (default `0 */6 * * *`), post-ingest, or manual.
- **Processing:** Build SQL per rule → execute → evaluate against threshold → merge results into latest → recompute score.
- **Output:** Rule results appended to latest, anomalies prefixed with `R_`.
- **Confidence:** Explicit.

### 5.5 Quality Score Computation

- **Description:** Composite 0–100 score starting at 100, deducting for NULL rates and anomaly severity.
- **Formula:** `100 - (100 - avg_completeness) - 10*critical_count - 5*warning_count`, clamped [0, 100].
- **Business purpose:** Single-number health indicator for dashboards and alerting.
- **Confidence:** Explicit.

### 5.6 Configurable Check Thresholds

- **Description:** 14 built-in check IDs (T1–T5, C1–C6, D1–D7) with toggle, threshold, and unit. Global config with per-table overrides merged at runtime.
- **Business purpose:** Tunability — teams can disable noisy checks or adjust sensitivity per table.
- **Confidence:** Explicit.

### 5.7 Schedule Management

- **Description:** Cron expressions for quick, deep, and custom checks. Global defaults with per-table overrides. Post-ingest trigger toggle. Enable/disable per table.
- **Default schedules:** Quick every 4h, deep daily at 2 AM, custom every 6h, post-ingest quick + custom on, post-ingest deep off.
- **Confidence:** Explicit.

### 5.8 History Persistence

- **Description:** After each check, a row is written to `__data_quality__` (a system Parquet table) with 16 columns including quality_score, anomaly count, and JSON-serialized details. Fallback to Redis LIST (capped at 1,000 entries).
- **Business purpose:** Enables trend analysis, quality dashboards over time, and MCP/AgenticBI natural-language queries about data quality.
- **Confidence:** Explicit.

### 5.9 Post-Ingest Reactive Checks

- **Description:** Three-layer protection (debounce → lock → cooldown) ensures ingest-triggered checks are rate-limited.
- **Mechanism:** `notify_ingest()` sets a Redis key with TTL. Scheduler picks it up on next tick. Redis `SET NX` for lock. Cooldown key (default 5 min TTL) after completion.
- **Business purpose:** Immediate quality validation after data loads without overwhelming the system.
- **Confidence:** Explicit.

### 5.10 Dashboard UI

- **Description:** ~2,000-line single-page HTML/CSS/JS dashboard with: overview tab (table list with scores, search, filter by status), table detail (column stats, anomalies, actions), checks configuration tab, custom rules tab (CRUD editor), schedule tab, history tab (SVG time-series chart).
- **Features:** Pagination, status filter pills, score color coding, time-ago formatting, truncation, tenant selector, role selector, toast notifications, inline SVG chart renderer.
- **Business purpose:** Self-service data quality monitoring without SQL or CLI.
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 `config.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `DQConfig` | Class | Central CRUD for all DQ state in Redis | Critical |
| `DQConfig.__init__` | Method | Takes Redis client, org, sup | Critical |
| `DQConfig.get_global_config` / `set_global_config` | Methods | Global check toggles + thresholds | Critical |
| `DQConfig.get_table_config` / `set_table_config` / `delete_table_config` | Methods | Per-table overrides | Important |
| `DQConfig.get_effective_config` | Method | Merges global + table config | Critical |
| `DQConfig.list_rules` / `create_rule` / `update_rule` / `delete_rule` | Methods | Custom rule CRUD | Important |
| `DQConfig.list_rules_for_table` | Method | Filters rules by table name (including `*` wildcard) | Important |
| `DQConfig.get_schedule` / `set_schedule` | Methods | Global schedule config | Important |
| `DQConfig.get_table_schedule` / `set_table_schedule` / `delete_table_schedule` | Methods | Per-table schedule overrides | Important |
| `DQConfig.get_latest` / `set_latest` | Methods | Fast UI cache per table | Critical |
| `DQConfig.get_latest_column` / `set_latest_column` | Methods | Per-column result cache | Important |
| `DQConfig.get_anomalies` / `set_anomalies` | Methods | Active anomalies per table | Important |
| `DQConfig.get_all_latest` | Method | Bulk scan for overview page (uses pipeline) | Important |
| `BUILTIN_CHECKS` | Dict | 14 built-in check definitions (T1–T5, C1–C6, D1–D7) | Critical |
| `_dq_key` | Function | Redis key builder: `supertable:{org}:{sup}:dq:{parts}` | Supporting |

### 6.2 `checker.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `build_quick_sql` | Function | Generates single-query column aggregation SQL | Critical |
| `parse_quick_result` | Function | Parses aggregation row into structured per-column stats | Critical |
| `build_deep_string_sql` | Function | Deep profile SQL for string columns (entropy, length, top-N, histogram) | Important |
| `build_deep_numeric_sql` | Function | Deep profile SQL for numeric columns (percentiles, IQR, variance) | Important |
| `build_custom_rule_sql` | Function | Generates SQL for user-defined rules (6 rule types) | Important |
| `evaluate_custom_rule` | Function | Evaluates SQL result against rule threshold, returns status/value/detail | Important |
| `compute_quality_score` | Function | Computes 0–100 score from column stats + anomalies | Critical |
| `_col_category` | Function | Classifies column type → numeric/date/bool/string/other | Supporting |

### 6.3 `anomaly.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `detect_anomalies` | Function | Compares current vs previous profile, returns anomaly list (A1–A5, C5) | Critical |
| `detect_schema_drift` | Function | Compares current vs previous schema, returns A_T3 anomaly | Important |

### 6.4 `scheduler.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `start_quality_scheduler` | Function | Starts singleton daemon thread | Critical |
| `notify_ingest` | Function | Public API for ingest path — sets pending flag in Redis | Critical |
| `_scheduler_loop` | Function | Main tick loop (60s interval) | Critical |
| `_scheduler_tick` | Function | Processes cron + pending checks for all org:sup pairs | Critical |
| `_try_run_check` | Function | Lock + cooldown guard, dispatches to check function | Critical |
| `_run_quick_check` | Function | Full quick check orchestration | Critical |
| `_run_deep_check` | Function | Full deep check orchestration | Important |
| `_run_custom_check` | Function | Custom rule execution + merge into latest | Important |
| `_cron_to_seconds` | Function | Simplified cron → interval converter | Supporting |
| `_discover_dq_pairs` | Function | Finds all org:sup pairs from Redis | Supporting |

### 6.5 `routes.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `attach_quality_routes` | Function | Registers all DQ routes on the shared router | Critical |
| `quality_page` | Route handler | `GET /reflection/quality` — serves dashboard HTML | Important |
| `api_quality_overview` | Route handler | `GET /reflection/quality/overview` — all tables' latest | Important |
| `api_quality_latest` | Route handler | `GET /reflection/quality/latest` — single table result | Important |
| `api_quality_column` | Route handler | `GET /reflection/quality/column` — single column result | Important |
| `api_run_check` | Route handler | `POST /reflection/quality/run` — manual trigger (background thread) | Important |
| `api_run_all` | Route handler | `POST /reflection/quality/run-all` — sequential check on all tables | Important |
| `api_quality_history` | Route handler | `GET /reflection/quality/history` — trend data (Parquet then Redis fallback) | Important |

### 6.6 `history.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `build_history_row` | Function | Pure function: flatten check result into 16-column row dict | Important |
| `write_history` | Function | Writes to `__data_quality__` Parquet via DataWriter | Important |
| `write_history_via_sql` | Function | Fallback: Redis LIST capped at 1,000 entries | Supporting |

### 6.7 `common.py` (selected relevant elements)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `Settings` | Class | All `SUPERTABLE_*` env config | Critical (foundation) |
| `_build_redis_client` | Function | Redis/Sentinel client builder | Critical (foundation) |
| `_FallbackCatalog` | Class | Redis-backed catalog when RedisCatalog unavailable | Important |
| `get_session` / `_set_session_cookie` | Functions | HMAC-signed session cookie management | Critical (auth) |
| `is_superuser` / `is_logged_in` | Functions | Auth checks | Critical (auth) |
| `logged_in_guard_api` / `admin_guard_api` | Functions | FastAPI dependency guards | Critical (auth) |
| `discover_pairs` / `resolve_pair` | Functions | Multi-tenant org:sup discovery | Important |
| `router` | APIRouter | Shared router for all Reflection routes | Critical |

---

## 7. End-to-End Workflows

### 7.1 Scheduled Quick Check Workflow

1. **Trigger:** `_scheduler_loop` wakes up every 60 seconds.
2. **Discovery:** `_discover_dq_pairs()` scans Redis for all `supertable:*:*:meta:root` keys.
3. **Schedule resolution:** For each org:sup, load `DQConfig.get_schedule()`. For each table, check for table-level override.
4. **Cron match:** Compare elapsed time since last run against `_cron_to_seconds(quick_cron)`.
5. **Lock acquisition:** `_try_run_check()` checks cooldown key → acquires `SET NX` running lock.
6. **Schema read:** `MetaReader.get_table_schema()` returns column name → type mapping.
7. **SQL generation:** `build_quick_sql()` constructs aggregation SQL.
8. **SQL execution:** `DataReader.execute()` runs SQL on DuckDB over Parquet.
9. **Result parsing:** `parse_quick_result()` computes null_rate, uniqueness, etc.
10. **Anomaly detection:** `detect_anomalies()` + `detect_schema_drift()` vs previous run.
11. **Scoring:** `compute_quality_score()` computes 0–100.
12. **Storage:** `dqc.set_latest()`, `dqc.set_anomalies()`, `dqc.set_latest_column()` in Redis.
13. **History:** `write_history()` to Parquet, fallback `write_history_via_sql()` to Redis LIST.
14. **Cleanup:** Release running lock, set cooldown key (default 5 min TTL).

**Failure modes:**
- Schema read failure → skip table, log warning.
- SQL execution failure → skip table, log error.
- History write failure → logged and swallowed (never fails the check).
- Lock contention → skip, retry next tick.

### 7.2 Post-Ingest Check Workflow

1. **Trigger:** Ingest code calls `notify_ingest(r, org, sup, table_name)`.
2. **Debounce:** Sets Redis key `dq:pending:{table}` with 10-min TTL.
3. **Scheduler picks up:** Next tick finds pending key exists.
4. **Tier selection:** Reads schedule config for `post_ingest_quick`, `post_ingest_custom`, `post_ingest_deep` flags.
5. **Execution:** Runs applicable check tiers via `_try_run_check()`.
6. **Cleanup:** Deletes pending key on success; retains on failure for next tick.

### 7.3 Manual Run Workflow

1. **Trigger:** User clicks "Run Now" → `POST /reflection/quality/run?table=X&mode=quick`.
2. **Auth check:** `admin_guard_api` enforces superuser.
3. **Background thread:** `threading.Thread(target=_run, daemon=True).start()`.
4. **Execution:** Calls `_run_quick_check()` or `_run_deep_check()` directly (bypasses scheduler lock/cooldown).
5. **Response:** Immediate JSON `{"ok": true, "message": "quick check started for X"}`.

### 7.4 History Query Workflow

1. **Trigger:** `GET /reflection/quality/history?table=X&days=7`.
2. **Strategy 1:** Query `__data_quality__` Parquet via DataReader SQL.
3. **Strategy 2 (fallback):** Read Redis LIST, filter by date and table.
4. **Response:** JSON with `source: "parquet"|"redis"|"none"`, rows array.

---

## 8. Data Model and Information Flow

### Core data structures

**Quick profile result (`parsed`):**
```
{
  "total": int,
  "columns": {
    "col_name": {
      "column_name": str, "column_type": str, "category": str,
      "total": int, "present": int, "null_count": int, "null_rate": float,
      "distinct": int, "uniqueness": float,
      // numeric/date:
      "min": any, "max": any,
      // numeric only:
      "avg": float, "stddev": float, "zero_rate": float, "negative_rate": float
    }
  }
}
```

**Latest result (Redis `dq:latest:{table}`):**
```
{
  "checked_at": ISO timestamp,
  "check_type": "quick"|"deep"|"custom",
  "row_count": int,
  "quality_score": 0-100,
  "status": "ok"|"warning"|"critical",
  "total_checks": int, "passed": int, "warnings": int, "critical": int,
  "anomalies": [anomaly_dict, ...],
  "rule_results": [rule_result_dict, ...],
  "parsed": quick_profile_result,
  "schema": [[col_name, col_type], ...]
}
```

**Anomaly dict:**
```
{
  "check_id": "A1"|"A2"|"A3"|"A4"|"A5"|"A5_C5"|"A_T3"|"R_{rule_id}",
  "check_name": str, "column": str|null,
  "severity": "warning"|"critical",
  "message": str, "value": any, "threshold": any,
  "detected_at": ISO timestamp
}
```

**History row (`__data_quality__` schema):**

| Column | Type | Description |
|---|---|---|
| dq_id | VARCHAR | UUID per check run |
| checked_at | VARCHAR (ISO) | UTC timestamp |
| table_name | VARCHAR | Checked table |
| check_type | VARCHAR | quick/deep/custom |
| quality_score | INT64 | 0–100 |
| status | VARCHAR | ok/warning/critical |
| row_count | INT64 | Total rows |
| total_checks | INT64 | Number of checks |
| passed | INT64 | Passed checks |
| warnings | INT64 | Warning count |
| critical_count | INT64 | Critical count |
| anomaly_count | INT64 | Anomaly count |
| anomalies_json | VARCHAR | JSON array |
| column_stats_json | VARCHAR | JSON object |
| rule_results_json | VARCHAR | JSON array |
| execution_ms | INT64 | Duration |

### Redis key namespace

| Key pattern | Type | Purpose |
|---|---|---|
| `dq:config:__global__` | STRING (JSON) | Global check config |
| `dq:config:{table}` | STRING (JSON) | Per-table config override |
| `dq:rules:index` | SET | Custom rule ID index |
| `dq:rules:doc:{rule_id}` | STRING (JSON) | Custom rule definition |
| `dq:schedule` | STRING (JSON) | Global schedule |
| `dq:schedule:{table}` | STRING (JSON) | Per-table schedule override |
| `dq:latest:{table}` | STRING (JSON) | Latest check result |
| `dq:latest:{table}:{column}` | STRING (JSON) | Per-column latest |
| `dq:anomalies:{table}` | STRING (JSON) | Active anomalies |
| `dq:pending:{table}` | STRING | Post-ingest pending flag (TTL 10 min) |
| `dq:running:{table}` | STRING | Distributed lock (TTL 5 min) |
| `dq:cooldown:{table}` | STRING | Post-check cooldown (TTL configurable) |
| `dq:history` | LIST | Fallback history (capped 1,000) |

All keys prefixed with `supertable:{org}:{sup}:`.

---

## 9. Dependencies and Integrations

### Internal dependencies

| Module | Purpose |
|---|---|
| `supertable.meta_reader.MetaReader` | Read table schema (column names + types) |
| `supertable.data_reader.DataReader` | Execute DuckDB SQL over Parquet data |
| `supertable.data_writer.DataWriter` | Write history rows as Parquet to storage backend |
| `supertable.redis_catalog.RedisCatalog` | Catalog access (tables, RBAC, mirrors) — with fallback |
| `supertable.redis_connector` | Create Redis client (used by scheduler) |
| `supertable.rbac.user_manager` / `role_manager` | RBAC resolution for session roles |
| `supertable.reflection.common` | Shared router, auth, session, templates, settings |

### External dependencies

| Dependency | Purpose |
|---|---|
| `redis` | Configuration store, caching, locks, fallback history |
| `fastapi` | HTTP routing, dependency injection, request/response handling |
| `polars` | DataFrame construction for DataWriter input |
| `json` (stdlib) | Serialization throughout |
| `threading` (stdlib) | Background scheduler, manual run threads |

### Infrastructure

| System | Role |
|---|---|
| Redis (+ Sentinel) | Hot state, distributed locking, pub/sub potential |
| DuckDB | SQL execution engine for profile queries |
| MinIO / local filesystem | Parquet storage for tables and quality history |

---

## 10. Architecture Positioning

### System context

```
                     ┌────────────────────┐
                     │   Ingest Pipeline   │
                     └────────┬───────────┘
                              │ notify_ingest()
                              ▼
┌──────────────┐    ┌─────────────────────┐    ┌──────────────┐
│  Reflection  │◄──►│   Data Quality      │◄──►│    Redis     │
│  Router      │    │   Module            │    │  (config,    │
│  (common.py) │    │  (scheduler,checker │    │   cache,     │
│              │    │   anomaly,config,   │    │   locks)     │
│              │    │   routes,history)   │    │              │
└──────┬───────┘    └────────┬────────────┘    └──────────────┘
       │                     │
       │              ┌──────┴──────┐
       │              ▼             ▼
       │     ┌──────────────┐  ┌──────────────┐
       │     │  MetaReader   │  │  DataReader   │
       │     │  (schema)     │  │  (DuckDB SQL) │
       │     └──────────────┘  └──────────────┘
       │                            │
       │                     ┌──────┴──────┐
       │                     │  DataWriter  │
       │                     │  (Parquet)   │
       │                     └─────────────┘
       ▼
┌──────────────┐
│  Browser UI  │
│  (quality.   │
│   html SPA)  │
└──────────────┘
```

### Upstream callers
- `not_common.py` attaches quality routes to the shared router.
- Ingest pipeline calls `notify_ingest()`.
- Application startup calls `start_quality_scheduler()`.
- Browser UI calls JSON API endpoints.
- AgenticBI/MCP queries `__data_quality__` table.

### Downstream dependencies
- MetaReader for schema.
- DataReader for SQL execution.
- DataWriter for history persistence.
- Redis for all operational state.

### Boundaries
- The DQ module owns the `dq:*` Redis key namespace.
- It owns the `__data_quality__` system table.
- It does not modify user data tables — read-only profiling.
- Auth is delegated to the shared session/guard infrastructure.

### Coupling
- Moderate coupling to the Reflection common infrastructure (injected).
- Tight coupling to Redis for all state.
- Optional coupling to DataWriter (graceful fallback to Redis).

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Problem solved** | Data observability — knowing whether data is trustworthy before using it |
| **Category** | Governance + Operational excellence |
| **Revenue relevance** | Premium/Enterprise feature — data quality monitoring is a paid tier feature in competitor products (Monte Carlo, Soda, Great Expectations) |
| **Efficiency** | Reduces manual data auditing effort; automated anomaly detection prevents cascading failures |
| **Compliance** | Queryable audit trail supports data governance requirements |
| **Differentiation** | Built-in to the platform (not a separate tool) with native integration to AgenticBI via MCP-queryable history |
| **Strategic value** | Data quality is a table-stakes expectation for enterprise data platforms. Without this, the product would lack a key category requirement |
| **Impact of removal** | Operators would have no automated way to detect data regressions, schema drift, or data loss. Manual auditing would be required. AgenticBI could not report on data health |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | High — multi-layered (config, SQL gen, anomaly detection, scheduling, persistence, UI) |
| **Architectural maturity** | Good — clean dependency injection, separation of concerns across modules, Redis key namespace discipline |
| **Maintainability** | Good — each module has a single clear responsibility. `BUILTIN_CHECKS` is data-driven. SQL templates are cleanly parameterized |
| **Extensibility** | Good — new check types can be added to `BUILTIN_CHECKS` + anomaly detection. Custom rules are fully dynamic. New rule types require code changes to `build_custom_rule_sql` / `evaluate_custom_rule` |
| **Operational sensitivity** | Medium — scheduler runs as an in-process daemon thread. No external job queue. DuckDB concurrency is addressed by sequential execution in `run-all` |
| **Performance** | Quick check is O(1) queries per table. Deep check is O(n) queries for n columns. Pipeline batching for Redis reads. Cooldown prevents over-execution |
| **Reliability** | Good — all history writes are fire-and-forget (logged and swallowed). Lock TTLs prevent deadlocks. Dual storage (Parquet + Redis) for history |
| **Security** | Admin actions require superuser (`admin_guard_api`). Read access requires login (`logged_in_guard_api`). SQL injection risk mitigated by quoting column names, but incremental `WHERE` clause uses string interpolation from config (low risk since config is admin-controlled) |
| **Testing** | No test files visible in provided snippet. `checker.py` functions are pure and highly testable. `anomaly.py` is pure. `history.build_history_row` is pure |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Redis is the sole state store for DQ configuration, latest results, anomalies, locks, and cooldowns.
- DuckDB SQL syntax is used (FILTER clause, NTILE, PERCENTILE_CONT, list aggregates).
- History is written to a `__data_quality__` Parquet table via DataWriter with Redis LIST fallback.
- The scheduler is a singleton daemon thread with 60-second tick interval.
- Post-ingest checks use a three-layer debounce/lock/cooldown pattern.
- Quality score formula deducts from 100 based on NULL rates and anomaly severity.
- Custom rules support 6 types including arbitrary SQL.
- All check writes are append-only (no dedup) to the history table.
- Admin operations require superuser; read operations require login.
- The frontend is a standalone SPA embedded in a Jinja2 template.

### Reasonable Inferences

- `DataReader` wraps DuckDB and operates over Parquet files stored on MinIO or local filesystem.
- `MetaReader` reads table schemas from Redis metadata (consistent with `meta:leaf:*` key pattern seen in `common.py`).
- The `__data_quality__` table is queryable via MCP SQL, enabling AgenticBI to answer natural-language questions about data quality trends.
- `start_quality_scheduler()` is called from `application.py` or equivalent startup hook (docstring says so).
- The `notify_ingest()` function is called from the data write/ingest path elsewhere in the codebase.
- The `super_name` in `DataReader`/`DataWriter` acts as a database-like namespace within an organization.

### Unknown / Not Visible in Provided Snippet

- Exact `DataReader` implementation and DuckDB connection management.
- `DataWriter` implementation and Parquet partitioning strategy.
- `MetaReader.get_table_schema()` return format details.
- How `start_quality_scheduler()` is invoked at application startup.
- Where `notify_ingest()` is called from in the ingest path.
- Whether there are unit or integration tests for the DQ module.
- The complete `quality.html` template (body HTML structure, tab content, JS chart rendering logic).
- Whether `_cron_to_seconds()` is used for approximate scheduling or if an actual cron parser is planned.
- Production performance characteristics under high table counts.

---

## 14. AI Consumption Notes

- **Canonical feature name:** Data Quality Module
- **Aliases:** DQ, quality checks, data profiling, anomaly detection, quality scoring
- **Package path:** `supertable.reflection.quality`
- **Main responsibilities:** Automated data profiling (quick + deep), anomaly detection (A1–A5 + schema drift), quality scoring (0–100), custom validation rules, scheduled + post-ingest execution, configurable thresholds, history persistence, dashboard UI
- **Important entities:** `DQConfig`, `BUILTIN_CHECKS`, quality score, anomaly dict, latest result, history row, `__data_quality__` table
- **Important workflows:** Scheduled quick check, post-ingest check (debounce/lock/cooldown), manual run, history query (Parquet → Redis fallback)
- **Integration points:** MetaReader (schema), DataReader (DuckDB SQL), DataWriter (Parquet), Redis (all state), ingest pipeline (notify_ingest), AgenticBI/MCP (history table queries)
- **Business keywords:** data quality, data observability, anomaly detection, data profiling, schema drift, data governance, quality scoring, data contracts, custom rules
- **Architecture keywords:** FastAPI, Redis, DuckDB, Parquet, daemon thread, distributed lock, cron scheduler, dependency injection, SPA dashboard
- **Follow-up files for completeness:**
  - `supertable/data_reader.py` — SQL execution engine
  - `supertable/data_writer.py` — Parquet write path
  - `supertable/meta_reader.py` — Schema reader
  - `supertable/redis_catalog.py` — Catalog implementation
  - `supertable/reflection/quality/` other templates or tests
  - Application startup file (where `start_quality_scheduler` is called)
  - Ingest pipeline code (where `notify_ingest` is called)

---

## 15. Suggested Documentation Tags

`data-quality`, `data-profiling`, `anomaly-detection`, `quality-scoring`, `schema-drift`, `custom-rules`, `data-governance`, `data-observability`, `background-scheduler`, `post-ingest-trigger`, `redis-state`, `duckdb-sql`, `parquet-history`, `fastapi-routes`, `spa-dashboard`, `distributed-locking`, `cron-scheduling`, `dependency-injection`, `multi-tenant`, `rbac-protected`, `reflection-ui`, `supertable-platform`, `mcp-queryable`, `agenticbi-integration`

---

## Merge Readiness

- **Canonical name:** `data-quality-module`
- **Standalone or partial:** Substantially standalone as a feature module. Missing external integration points (DataReader, DataWriter, MetaReader, ingest caller).
- **Related components for merge:** Reflection common infrastructure (`common.py`), ingest pipeline, MetaReader/DataReader/DataWriter, application startup, AgenticBI/MCP query layer.
- **Additional files for certainty:** `data_reader.py`, `data_writer.py`, `meta_reader.py`, application startup file, ingest write-path code, any test files.
- **Merge key phrases:** "data quality", "quality checks", "anomaly detection", "quality score", "`__data_quality__`", "`DQConfig`", "`attach_quality_routes`", "`notify_ingest`", "`start_quality_scheduler`", "reflection quality".
