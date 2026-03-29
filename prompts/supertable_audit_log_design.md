# Audit Logging Module — Technical Design Document

**Module**: `supertable/audit/`
**Version**: 1.0
**Status**: Design — pending implementation
**Compliance targets**: EU DORA (Regulation 2022/2554), SOC 2 Type II (Trust Services Criteria)

---

## 1. Purpose

This module provides an immutable, append-only audit trail for every security-relevant action in the Data Island Core platform. It captures who did what, to which resource, when, from where, and whether it succeeded — with tamper-evident chaining so deletions or modifications are detectable.

The audit log is not a debug log. It does not replace `supertable/logging.py` (structured request logs) or `monitoring_writer.py` (read/write metrics). It captures actions with compliance significance: authentication, authorization decisions, data access, configuration changes, and administrative operations.

---

## 2. Compliance mapping

### 2.1 DORA requirements addressed

| DORA Article | Requirement | How this module satisfies it |
|---|---|---|
| Art. 6(1) | ICT risk management framework with identification of ICT risks | Logs every access and mutation for risk pattern analysis |
| Art. 6(5) | Logging of ICT operations, access, and changes | Core purpose of this module |
| Art. 10(1) | Detection of anomalous activities | Event stream enables real-time anomaly detection |
| Art. 10(2) | Sufficient logging for incident investigation | Every event carries correlation_id, actor, resource, outcome |
| Art. 11(1) | Classification and reporting of ICT incidents | Severity classification in event schema; export format aligns with RTS reporting templates |
| Art. 12(1) | Data retention for supervisory access | Configurable retention with legal hold capability |
| Art. 19(1) | Record keeping of ICT-related incidents | Incident-class events with structured severity and impact fields |

### 2.2 SOC 2 Trust Services Criteria addressed

| Criterion | Category | How this module satisfies it |
|---|---|---|
| CC6.1 | Logical access security | Logs every authentication attempt (success + failure) |
| CC6.2 | Access authorization | Logs RBAC changes, role assignments, permission evaluations |
| CC6.3 | Access removal | Logs user deprovisioning, token revocation, session termination |
| CC7.1 | System monitoring | Event stream for real-time security monitoring |
| CC7.2 | Anomaly detection | Structured events with severity enable alerting rules |
| CC7.3 | Incident response | Full audit trail with correlation IDs for forensic investigation |
| CC8.1 | Change management | Logs configuration changes with before/after values |
| PI1.3 | Processing integrity | Logs data mutations with row counts, table names, timing |
| A1.2 | Availability monitoring | Logs system health events, service restarts, failovers |

---

## 3. Architecture

### 3.1 Module structure

```
supertable/
  audit/
    __init__.py          — public API: get_audit_logger(), AuditEvent, EventCategory
    events.py            — AuditEvent dataclass, EventCategory enum, Severity enum
    logger.py            — AuditLogger class (queue + background writer)
    chain.py             — hash-chain integrity (SHA-256 chaining)
    writer_redis.py      — hot-tier writer (Redis Streams)
    writer_parquet.py    — warm-tier writer (Parquet files via storage backend)
    reader.py            — query interface for audit events
    middleware.py        — FastAPI middleware for automatic request/response logging
    retention.py         — retention policy enforcement, legal holds
    export.py            — DORA/SOC 2 report export (JSON-lines, CSV)
```

### 3.2 Data flow

```
  Application code           AuditLogger              Storage tiers
  ─────────────────          ──────────────           ────────────────
                                                      
  api/api.py ──emit()──→  thread-safe queue           
  session.py ──emit()──→  ───────┬────────→  Redis Stream (hot, 24h TTL)
  middleware ──emit()──→         │                     
  rbac/ ──────emit()──→         │            ┌─→  Parquet (warm, configurable retention)
  data_writer ─emit()─→         └──worker──→ ┤
                                             └─→  Hash chain file (integrity proof)
```

### 3.3 Design principles

1. **Zero-impact on request latency**. `emit()` enqueues and returns immediately. Background worker handles storage. Same pattern as `monitoring_writer.py`.

2. **Append-only**. No update or delete operations exist on the audit log. Retention policy removes old partitions; individual events cannot be modified.

3. **Tamper-evident**. Each batch written to Parquet includes a SHA-256 chain hash linking it to the previous batch. A verification tool can detect gaps or modifications.

4. **Dual-tier storage**. Hot tier (Redis Streams) for real-time queries and alerting (last 24 hours). Warm tier (Parquet in the configured storage backend) for long-term retention and compliance reporting.

5. **Correlation**. Every event carries the `correlation_id` from the HTTP request (propagated by `supertable/logging.py` middleware). A single user action may produce multiple audit events; the correlation_id groups them.

6. **Fail-open for the application, fail-closed for the log**. If the audit system is unavailable, the application continues operating — but the failure is itself logged (to stderr and the structured log) and a gap marker is written when the system recovers.

---

## 4. Event schema

### 4.1 AuditEvent dataclass

```python
@dataclass(frozen=True)
class AuditEvent:
    # ── Identity ───────────────────────────────────────────
    event_id: str              # UUID v7 (time-ordered)
    timestamp_ms: int          # Unix epoch milliseconds (UTC)
    
    # ── Classification ─────────────────────────────────────
    category: str              # EventCategory enum value
    action: str                # Verb: "login", "query", "create_role", etc.
    severity: str              # "info" | "warning" | "critical"
    
    # ── Actor ──────────────────────────────────────────────
    actor_type: str            # "user" | "superuser" | "api_token" | "system" | "mcp"
    actor_id: str              # User hash, token_id, or "system"
    actor_username: str        # Human-readable (may be empty for tokens)
    actor_ip: str              # Client IP address
    actor_user_agent: str      # Truncated to 256 chars
    
    # ── Context ────────────────────────────────────────────
    organization: str          # Tenant organization
    super_name: str            # SuperTable name (empty if not applicable)
    correlation_id: str        # From X-Correlation-ID header
    session_id: str            # Session cookie hash (empty for API tokens)
    server: str                # "api" | "webui" | "mcp"
    
    # ── Resource ───────────────────────────────────────────
    resource_type: str         # "table" | "role" | "user" | "token" | "setting" | etc.
    resource_id: str           # Table name, role_id, user_id, setting key, etc.
    
    # ── Operation details ──────────────────────────────────
    detail: str                # JSON string — action-specific payload
    outcome: str               # "success" | "failure" | "denied"
    reason: str                # Failure/denial reason (empty on success)
    
    # ── Integrity ──────────────────────────────────────────
    chain_hash: str            # SHA-256 of (previous_chain_hash + this_event_hash)
```

### 4.2 EventCategory enum

```python
class EventCategory(str, Enum):
    AUTHENTICATION   = "authentication"     # Login, logout, token validation
    AUTHORIZATION    = "authorization"      # Permission checks, access denied
    DATA_ACCESS      = "data_access"        # SQL queries, table reads, metadata reads
    DATA_MUTATION    = "data_mutation"       # Writes, deletes, schema changes, ingestion
    RBAC_CHANGE      = "rbac_change"        # Role/user CRUD, permission changes
    CONFIG_CHANGE    = "config_change"      # Settings, engine config, staging/pipe config
    TOKEN_MGMT       = "token_management"   # Token create/delete/validate
    SYSTEM           = "system"             # Service start/stop, health, failover
    EXPORT           = "export"             # Data export, OData feed access
    SECURITY_ALERT   = "security_alert"     # Failed logins, brute force, anomalies
```

### 4.3 Severity levels

| Level | When to use | Examples |
|---|---|---|
| `info` | Normal successful operations | Login, query, role read |
| `warning` | Unusual but not immediately dangerous | Failed login, permission denied, config change |
| `critical` | Security-significant or potentially harmful | Multiple failed logins (brute force pattern), superuser token used, bulk delete, RBAC policy change, audit gap detected |

### 4.4 Action catalog

Each action is a specific verb within a category. This is the canonical list — the code must use these exact strings.

**Authentication**
- `login_success` — user logged in via UI
- `login_failure` — incorrect credentials
- `logout` — explicit sign-out
- `session_expired` — session timed out
- `token_auth_success` — API token validated
- `token_auth_failure` — invalid API token presented
- `mcp_auth_success` — MCP token validated
- `mcp_auth_failure` — invalid MCP token presented

**Authorization**
- `access_granted` — RBAC check passed
- `access_denied` — RBAC check failed (user lacks permission)
- `row_filter_applied` — row-level security filter activated
- `column_filter_applied` — column-level restriction activated

**Data access**
- `query_execute` — SQL query executed (detail includes SQL hash, row count, duration_ms)
- `table_read` — table metadata or schema read
- `table_list` — table listing
- `metadata_read` — MetaReader access
- `schema_read` — schema endpoint accessed

**Data mutation**
- `data_write` — rows written to table (detail includes table, row_count, file_count)
- `data_delete` — table data deleted
- `table_create` — new table created via ingestion
- `table_delete` — table dropped
- `table_config_change` — dedup/primary key config changed
- `staging_create` — staging area created
- `staging_delete` — staging area deleted
- `pipe_create` — data pipe created
- `pipe_update` — pipe configuration changed
- `pipe_delete` — pipe deleted
- `pipe_enable` — pipe activated
- `pipe_disable` — pipe deactivated
- `file_upload` — file uploaded via ingestion
- `supertable_create` — new SuperTable created
- `supertable_delete` — SuperTable deleted

**RBAC changes**
- `role_create` — new role created
- `role_update` — role permissions modified (detail includes before/after)
- `role_delete` — role deleted
- `user_create` — new user created
- `user_update` — user modified
- `user_delete` — user deleted
- `user_role_assign` — role assigned to user
- `user_role_remove` — role removed from user

**Configuration changes**
- `engine_config_change` — DuckDB/Spark engine settings changed (detail includes before/after)
- `mirror_enable` — mirror format enabled
- `mirror_disable` — mirror format disabled
- `setting_change` — runtime setting changed

**Token management**
- `token_create` — auth token created (detail includes label, never the token value)
- `token_delete` — auth token revoked

**System**
- `service_start` — API/WebUI/MCP server started
- `service_stop` — server shutting down
- `health_check` — health probe (logged only on failure, not on success)
- `audit_gap` — audit system detected missing events (integrity gap)

**Export**
- `odata_access` — OData feed queried
- `audit_export` — audit log exported (meta-event)

**Security alerts**
- `brute_force_detected` — N failed logins from same IP within window
- `privilege_escalation` — attempt to access resource above role level
- `unusual_access_pattern` — anomalous query volume or timing

---

## 5. Detail payload specification

The `detail` field is a JSON string containing action-specific data. Each action type has a defined schema. This prevents unstructured dumping and ensures every detail field is queryable.

### 5.1 Query execution detail

```json
{
  "sql_hash": "sha256_of_normalized_sql",
  "sql_preview": "SELECT ... FROM ... (first 200 chars)",
  "tables_accessed": ["orders", "customers"],
  "row_count": 1542,
  "duration_ms": 234,
  "engine": "duckdb_lite",
  "role_name": "analyst",
  "rbac_filters_applied": true
}
```

The full SQL text is NOT stored in the audit log (it may contain sensitive data in WHERE clauses). Only a SHA-256 hash and a truncated preview are stored. If full SQL retention is needed, it is stored separately with its own encryption and access controls.

### 5.2 Data mutation detail

```json
{
  "table": "orders",
  "operation": "append",
  "row_count": 5000,
  "file_count": 3,
  "bytes_written": 1248000,
  "duration_ms": 890,
  "source": "file_upload",
  "filename": "orders_2025_q1.csv"
}
```

### 5.3 RBAC change detail

```json
{
  "before": {
    "tables": ["orders", "customers"],
    "columns": {"orders": ["*"], "customers": ["id", "name"]},
    "filters": {}
  },
  "after": {
    "tables": ["orders", "customers", "payments"],
    "columns": {"orders": ["*"], "customers": ["id", "name"], "payments": ["*"]},
    "filters": {"payments": "amount < 10000"}
  },
  "changed_by": "superuser"
}
```

### 5.4 Configuration change detail

```json
{
  "setting": "duckdb_memory_limit",
  "before": "1GB",
  "after": "4GB",
  "scope": "tenant",
  "source": "api"
}
```

### 5.5 Authentication failure detail

```json
{
  "method": "password",
  "username_attempted": "admin",
  "failure_reason": "invalid_credentials",
  "consecutive_failures": 3,
  "lockout_triggered": false
}
```

---

## 6. Storage architecture

### 6.1 Hot tier — Redis Streams

**Purpose**: Real-time querying, alerting, and live dashboards. Retains the last 24 hours of events.

**Implementation**: Redis Streams (`XADD` / `XREAD` / `XRANGE`). One stream per organization.

```
Key: supertable:{org}:audit:stream
```

Each entry is a flat hash of the AuditEvent fields. Redis Streams provide automatic ID assignment (timestamp-based), consumer groups for multi-reader alerting, and trimming by time or count.

**Why Redis Streams over Lists**: The monitoring_writer uses `RPUSH` to a list. Streams are better for audit because they provide built-in time-based ID ordering, `XRANGE` for time-window queries, consumer groups for independent readers (alerting, dashboard, archival), and `XTRIM` with `MINID` for TTL-based eviction. Lists require manual cursor management and don't support concurrent independent consumers.

**TTL**: 24 hours, enforced via periodic `XTRIM MINID` in the background worker.

**Backpressure**: If the Redis Stream exceeds 100,000 entries (configurable), new events are still written but a `warning` is logged. The stream is never the bottleneck — Parquet is the durable store.

### 6.2 Warm tier — Parquet files

**Purpose**: Long-term retention, compliance reporting, forensic investigation. This is the system of record.

**Implementation**: Append-only Parquet files written to the configured storage backend (S3, MinIO, Azure, GCP, or local). Same storage infrastructure used by `data_writer.py`.

**Partitioning scheme**:

```
{storage_root}/{org}/__audit__/
  year=2025/
    month=01/
      day=15/
        audit_20250115_143022_a7b3c9.parquet    (batch file)
        audit_20250115_150107_e2f4d1.parquet
      day=16/
        ...
  _chain/
    chain_20250115.json     (daily chain proof)
    chain_20250116.json
  _retention/
    policy.json             (retention config)
    holds.json              (legal hold records)
```

Each Parquet file contains one batch of events (default: 1,000 events or 60 seconds, whichever comes first). Files are small and append-only — no compaction, no rewriting.

**Schema**: Parquet schema mirrors AuditEvent fields exactly. All fields are strings except `timestamp_ms` (INT64) for efficient range scanning.

**Why not DuckDB**: The audit log must be independent of the query engine it monitors. If DuckDB has a bug or is misconfigured, the audit log must still be writable and readable. Parquet files on the storage backend have zero dependency on DuckDB at write time.

### 6.3 Integrity tier — hash chain

**Purpose**: Detect tampering, deletions, or gaps in the audit trail.

**Implementation**: Each batch of events written to Parquet is hash-chained. The chain works as follows:

```
batch_hash = SHA-256(sorted_event_ids + parquet_file_hash)
chain_hash = SHA-256(previous_chain_hash + batch_hash)
```

The chain state is persisted in two places:
1. **Redis**: `supertable:{org}:audit:chain_head` — the current chain hash (for fast append)
2. **Storage**: `{org}/__audit__/_chain/chain_{date}.json` — daily chain proofs for verification

A daily verification job reads all Parquet files for the day, recomputes the chain, and compares against the stored proof. Mismatches trigger a `critical` severity `audit_gap` event.

### 6.4 Settings fields

New fields added to `config/settings.py`:

```python
# ── Audit ────────────────────────────────────────────────
SUPERTABLE_AUDIT_ENABLED: bool = True               # Master switch
SUPERTABLE_AUDIT_RETENTION_DAYS: int = 2555          # ~7 years (DORA minimum: 5 years)
SUPERTABLE_AUDIT_BATCH_SIZE: int = 1000              # Events per Parquet file
SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC: int = 60        # Max seconds before flush
SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS: int = 24    # Hot tier TTL
SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN: int = 100000   # Max stream entries
SUPERTABLE_AUDIT_HASH_CHAIN: bool = True             # Enable tamper-evident chaining
SUPERTABLE_AUDIT_LOG_QUERIES: bool = True            # Log SQL queries (detail: hash + preview)
SUPERTABLE_AUDIT_LOG_READS: bool = False             # Log table reads (high volume — disabled by default)
SUPERTABLE_AUDIT_ALERT_WEBHOOK: str = ""             # Webhook URL for critical alerts
SUPERTABLE_AUDIT_LEGAL_HOLD: bool = False            # Suspend retention enforcement globally
```

---

## 7. Public API

### 7.1 Core interface

```python
# supertable/audit/__init__.py

def get_audit_logger(organization: str) -> AuditLogger:
    """Return a cached AuditLogger for the organization.
    
    Thread-safe. One background worker per organization.
    Same caching pattern as monitoring_writer.py.
    """

# Direct emit (convenience wrapper)
def emit(
    *,
    category: EventCategory,
    action: str,
    organization: str,
    actor_type: str = "system",
    actor_id: str = "",
    actor_username: str = "",
    actor_ip: str = "",
    resource_type: str = "",
    resource_id: str = "",
    detail: dict | None = None,
    outcome: str = "success",
    reason: str = "",
    severity: str = "info",
    correlation_id: str = "",
    session_id: str = "",
    server: str = "",
    super_name: str = "",
    actor_user_agent: str = "",
) -> None:
    """Emit an audit event. Non-blocking (enqueues and returns)."""
```

### 7.2 Request context helper

```python
# supertable/audit/middleware.py

def audit_context(request: Request) -> dict:
    """Extract actor, correlation, and session info from a FastAPI request.
    
    Returns a dict that can be **unpacked into emit():
        audit.emit(**audit.audit_context(request), category=..., action=..., ...)
    """
```

### 7.3 FastAPI middleware

```python
class AuditMiddleware(BaseHTTPMiddleware):
    """Automatically logs request-level events.
    
    Captures:
    - Authentication outcomes (from session/token validation)
    - Authorization denials (403 responses)
    - Server errors (500 responses → severity: critical)
    
    Does NOT capture:
    - Successful 200 responses for read endpoints (too noisy)
    - Health checks (/healthz)
    
    Action-specific audit events (query execution, RBAC changes, etc.)
    are emitted by the endpoint handlers themselves, not by middleware.
    """
```

### 7.4 Query interface

```python
# supertable/audit/reader.py

def query_audit_log(
    organization: str,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
    category: str | None = None,
    action: str | None = None,
    actor_id: str | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    outcome: str | None = None,
    severity: str | None = None,
    correlation_id: str | None = None,
    limit: int = 500,
    source: str = "auto",           # "redis" | "parquet" | "auto"
) -> list[dict]:
    """Query audit events with filters.
    
    source="auto": uses Redis for queries within the last 24h,
    falls back to Parquet for older ranges.
    """

def verify_chain_integrity(
    organization: str,
    date: str,                      # "2025-01-15"
) -> dict:
    """Verify hash chain integrity for a specific day.
    
    Returns: {"valid": True/False, "batches": N, "gaps": [...], "first_event": ..., "last_event": ...}
    """
```

---

## 8. Integration points

### 8.1 Where to emit events

This is the exhaustive list of files that must call `audit.emit()`. Each integration is a single function call — no refactoring of business logic required.

| File | Action(s) | Integration method |
|---|---|---|
| `api/session.py` | `login_success`, `login_failure`, `logout`, `session_expired` | Add emit() calls in existing auth functions |
| `api/api.py` — auth token validation | `token_auth_success`, `token_auth_failure` | In the auth guard middleware |
| `api/api.py` — query endpoints | `query_execute` | After `_clean_sql_query()` returns results |
| `api/api.py` — RBAC endpoints | `role_create/update/delete`, `user_create/update/delete`, `user_role_assign/remove` | In each `@router.*` handler, after catalog operation succeeds |
| `api/api.py` — token endpoints | `token_create`, `token_delete` | After catalog token operations |
| `api/api.py` — table endpoints | `table_delete`, `table_config_change` | In delete/config handlers |
| `api/api.py` — ingestion endpoints | `staging_create/delete`, `pipe_create/update/delete/enable/disable`, `file_upload` | In each handler |
| `api/api.py` — engine config | `engine_config_change` | In set handler, with before/after |
| `api/api.py` — SuperTable CRUD | `supertable_create`, `supertable_delete` | In create/delete handlers |
| `data_writer.py` | `data_write` | In `DataWriter.__exit__()` after successful write |
| `data_reader.py` | `query_execute` (optional, if `AUDIT_LOG_READS=True`) | In DataReader after query completion |
| `mcp/mcp_server.py` | `mcp_auth_success/failure`, `query_execute` | In tool handlers |
| `webui/application.py` | `login_success`, `login_failure`, `logout` | In login/logout route handlers |
| `services/security.py` | `odata_access` | In OData endpoint handlers |
| `api/application.py` | `service_start` | At startup, after app is ready |

### 8.2 Middleware installation

```python
# In api/application.py and webui/application.py:
from supertable.audit.middleware import AuditMiddleware

app.add_middleware(AuditMiddleware, server="api")    # or server="webui"
```

Installed after `RequestLoggingMiddleware` (so correlation_id is already set).

### 8.3 Example integration in api/api.py

```python
# Before (current code):
@router.post("/reflection/rbac/roles")
async def rbac_role_create(request: Request, ...):
    ...
    catalog.rbac_create_role(org, sup, role_id, role_data)
    return JSONResponse({"role_id": role_id, ...})

# After (with audit):
@router.post("/reflection/rbac/roles")
async def rbac_role_create(request: Request, ...):
    ...
    catalog.rbac_create_role(org, sup, role_id, role_data)
    
    audit.emit(
        **audit.audit_context(request),
        category=EventCategory.RBAC_CHANGE,
        action="role_create",
        organization=org,
        super_name=sup,
        resource_type="role",
        resource_id=role_id,
        detail={"role_name": role_data.get("role_name"), "role_type": role_data.get("role")},
        severity="warning",     # RBAC changes are always warning+
    )
    
    return JSONResponse({"role_id": role_id, ...})
```

---

## 9. Retention policy

### 9.1 Default retention

| Tier | Retention | Reason |
|---|---|---|
| Redis Streams (hot) | 24 hours | Real-time alerting only |
| Parquet (warm) | 7 years (2,555 days) | DORA Art. 12 requires 5 years minimum; 7 years covers most EU member state extensions |

### 9.2 Retention enforcement

A background task (registered as a FastAPI lifespan hook) runs daily:

1. List all Parquet partitions older than `SUPERTABLE_AUDIT_RETENTION_DAYS`
2. If `SUPERTABLE_AUDIT_LEGAL_HOLD` is `True`, skip deletion and log a warning
3. Otherwise, delete the partition directories and record the deletion as an `audit_export` event
4. Trim the chain proof files for deleted dates

### 9.3 Legal hold

When `SUPERTABLE_AUDIT_LEGAL_HOLD=True`, no audit data is deleted regardless of retention policy. This is a global kill switch for deletion — typically activated during regulatory investigations or litigation.

Legal hold activation and deactivation are themselves audit events (`config_change` with `setting=audit_legal_hold`).

---

## 10. Alerting

### 10.1 Real-time alerting via webhook

When `SUPERTABLE_AUDIT_ALERT_WEBHOOK` is set, `critical` severity events are POSTed to the webhook URL within 5 seconds of occurrence.

Payload format:

```json
{
  "alert_type": "audit_critical",
  "event": { ... full AuditEvent as JSON ... },
  "platform": "data_island_core",
  "organization": "acme",
  "timestamp": "2025-03-29T14:22:00Z"
}
```

### 10.2 Built-in alert rules

These rules run in the background worker against the Redis Stream:

| Rule | Trigger | Severity | Action |
|---|---|---|---|
| Brute force | ≥5 `login_failure` from same IP within 5 minutes | critical | Emit `brute_force_detected` + webhook |
| Privilege escalation | `access_denied` on a resource the actor never accessed before | warning | Emit `privilege_escalation` |
| Bulk delete | `table_delete` or `supertable_delete` | critical | Webhook immediately |
| Audit gap | Chain verification detects missing batch | critical | Emit `audit_gap` + webhook |
| RBAC change | Any `rbac_change` event | warning | Webhook |

---

## 11. Export and reporting

### 11.1 DORA incident report export

```python
def export_dora_incident_report(
    organization: str,
    incident_id: str,
    start_ms: int,
    end_ms: int,
    output_format: str = "json",   # "json" | "csv"
) -> bytes:
    """Export audit events for a specific time window in DORA-aligned format.
    
    Aligns with RTS/ITS templates for ICT incident reporting
    (Commission Delegated Regulation 2024/1772).
    """
```

### 11.2 SOC 2 evidence export

```python
def export_soc2_evidence(
    organization: str,
    criteria: str,                  # "CC6.1", "CC7.2", etc.
    period_start: str,              # "2025-01-01"
    period_end: str,                # "2025-03-31"
    output_format: str = "json",
) -> bytes:
    """Export audit events relevant to a specific SOC 2 criterion.
    
    Filters events by category and action based on the criterion mapping
    defined in section 2.2 of this document.
    """
```

### 11.3 API endpoints

```
GET  /reflection/audit/events       — query events (filters via query params)
GET  /reflection/audit/export       — download events as JSON-lines or CSV
GET  /reflection/audit/verify       — run chain integrity check for a date range
GET  /reflection/audit/stats        — event counts by category, severity, day
```

These endpoints require `superuser` or a dedicated `auditor` role. Access to audit endpoints is itself audited.

---

## 12. Performance budget

| Metric | Target | Mechanism |
|---|---|---|
| `emit()` latency | < 50μs p99 | Queue-only, no I/O on emit path |
| Redis XADD latency | < 1ms p99 | Pipeline batching (same as monitoring_writer) |
| Parquet write latency | < 100ms per batch | Batches of 1,000 events; PyArrow in-memory table → single write |
| Memory per organization | < 5 MB | Queue bounded at 10,000 events; Parquet batches are small |
| Background worker threads | 1 per organization | Same singleton-cache pattern as monitoring_writer |
| Storage per 1M events | ~50 MB Parquet | Columnar compression; string fields compress well |

### 12.1 Volume estimation

| Deployment size | Events/day | Parquet/day | Annual storage |
|---|---|---|---|
| Small (1-5 users, light queries) | ~5,000 | ~0.25 MB | ~90 MB |
| Medium (10-50 users, regular queries) | ~100,000 | ~5 MB | ~1.8 GB |
| Large (100+ users, heavy queries) | ~1,000,000 | ~50 MB | ~18 GB |

With `AUDIT_LOG_READS=False` (default), read-heavy workloads don't inflate the audit log. Only mutations, auth events, and config changes are logged.

---

## 13. Testing strategy

### 13.1 Unit tests

```
supertable/audit/tests/
  test_events.py           — AuditEvent construction, validation, serialization
  test_chain.py            — hash chain computation and verification
  test_logger.py           — queue behavior, backpressure, flush
  test_writer_parquet.py   — Parquet writing, partitioning, schema
  test_retention.py        — retention enforcement, legal hold
  test_middleware.py        — request context extraction, auto-logging rules
  test_export.py           — DORA and SOC 2 export format validation
```

### 13.2 Integration tests

- End-to-end: emit event → verify in Redis Stream → verify in Parquet → verify chain
- Failure scenarios: Redis unavailable → events still written to Parquet
- Retention: write events → advance clock → run retention → verify deletion
- Legal hold: activate hold → run retention → verify nothing deleted
- Alert rules: inject brute force pattern → verify webhook fired

### 13.3 Compliance tests

- Verify every action in the action catalog (section 4.4) is emitted by at least one integration point
- Verify chain integrity across simulated multi-day runs
- Verify export format matches DORA RTS template schema
- Verify retention policy enforces minimum 5-year retention

---

## 14. Implementation plan

| Phase | Deliverables | Effort |
|---|---|---|
| **Phase 1 — Core** | `events.py`, `logger.py`, `chain.py`, `writer_redis.py`, settings fields, `__init__.py` | 3 days |
| **Phase 2 — Storage** | `writer_parquet.py`, partitioning, batch flush logic | 2 days |
| **Phase 3 — Middleware** | `middleware.py`, `audit_context()`, install in api + webui apps | 1 day |
| **Phase 4 — Integration** | Wire emit() calls into all 15 files from section 8.1 | 3 days |
| **Phase 5 — Query & export** | `reader.py`, `export.py`, API endpoints | 2 days |
| **Phase 6 — Retention** | `retention.py`, legal hold, background scheduler | 1 day |
| **Phase 7 — Alerting** | Webhook delivery, built-in alert rules | 1 day |
| **Phase 8 — Tests** | Unit, integration, and compliance test suites | 3 days |
| **Total** | | **~16 working days** |

---

## 15. Open decisions

These items require human input before implementation:

1. **Full SQL logging**: Should `detail.sql_preview` include the full SQL text (encrypted) or only a 200-char truncation + hash? Full text enables forensic replay but increases storage and may contain sensitive WHERE-clause values.

2. **Read-path auditing**: Should `AUDIT_LOG_READS` default to `True` for DORA-scoped deployments? This would increase event volume 10-50x but satisfies the strictest interpretation of Art. 6(5).

3. **Auditor role**: Should there be a dedicated `auditor` RBAC role type that can read audit logs but cannot modify data or RBAC? Or should audit access be superuser-only?

4. **Multi-region**: For deployments spanning multiple storage regions, should audit Parquet files be written to the same region as the data or to a dedicated audit region?

5. **External SIEM integration**: Should the Redis Stream support consumer groups for external SIEM tools (Splunk, Sentinel, ELK) to consume directly, or should export be the only external path?
