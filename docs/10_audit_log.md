# Data Island Core — Audit Log

## Overview

The audit log provides a complete, immutable record of every security-relevant action performed on the Data Island Core platform. It captures who did what, to which resource, when, from where, and whether the operation succeeded — with cryptographic chaining so any tampering, deletion, or reordering of events is detectable.

The audit log is built for two audiences. For operations teams, it answers "what happened and when" during incident investigation. For compliance teams, it provides the evidence packages required by EU DORA (Regulation 2022/2554) and SOC 2 Type II audits.

The system is always on by default. Every login, query, data mutation, permission change, and configuration update is recorded without any developer intervention. The audit module runs independently of the query engine it monitors — if DuckDB or Spark is down, audit events are still captured.

---

## How it works

Every audit event follows the same path:

1. Application code calls `emit()` — a non-blocking function that enqueues the event and returns in under 50 microseconds. The calling endpoint experiences zero added latency.

2. A background worker thread drains the queue in batches (default: up to 1,000 events or every 60 seconds, whichever comes first).

3. Each batch is written to two storage tiers simultaneously:
   - **Redis Streams** (hot tier) — the last 24 hours, queryable in real time from the Audit Log page and consumable by external SIEM tools via consumer groups.
   - **Parquet files** (warm tier) — the permanent record, written to the same storage backend used by your data tables (S3, MinIO, Azure Blob, GCP, or local filesystem). Retained for 7 years by default.

4. If hash chaining is enabled (default: yes), each batch is linked to the previous one via a SHA-256 chain. A daily Merkle proof aggregates all server instance chains into a single verifiable root.

---

## Accessing the audit log

### Audit Log page

Navigate to **Platform → Audit Log** in the sidebar. This page is visible only to superuser accounts (and the future `auditor` role).

The page has three sections:

**Event stream** — A searchable, filterable table of recent audit events. Filter by category, severity, outcome, actor, or resource. Click any row to expand the full event detail as JSON. Enable auto-refresh (10-second polling) to watch events arrive in real time.

**Chain integrity** — Shows whether the audit trail has been tampered with. Click "Verify today" to run a SHA-256 chain check against all events recorded today. A green checkmark means the chain is intact; a red alert means events were modified, deleted, or reordered.

**Export** — Download audit events as JSON-lines or CSV for a selected date range. Optionally filter by SOC 2 criteria (CC6.1, CC6.2, CC7.2, CC8.1) to generate evidence packages for auditors.

### API endpoints

All endpoints require superuser authentication.

**Query events**

```
GET /reflection/audit/events?organization=acme&category=authentication&severity=warning&limit=200
```

Returns a JSON array of matching events from the hot tier (last 24 hours). Supported filters: `category`, `severity`, `outcome`, `actor_id`, `resource_id`, `correlation_id`, `limit`.

**Verify chain integrity**

```
GET /reflection/audit/verify?organization=acme&date=2025-03-29
```

Returns `{"valid": true/false, "batches": N, "gaps": [...]}`.

**Export events**

```
GET /reflection/audit/export?organization=acme&start=2025-01-01&end=2025-03-31&format=json
GET /reflection/audit/export?organization=acme&start=2025-01-01&end=2025-03-31&format=csv&criteria=CC6.1
```

Returns a downloadable file (JSON-lines or CSV). When `criteria` is set, events are pre-filtered to the relevant SOC 2 trust service criterion.

**Logger statistics**

```
GET /reflection/audit/stats?organization=acme
```

Returns internal counters: `total_emitted`, `total_written`, `total_dropped`, `batches_written`.

---

## What gets logged

### Event categories

Every audit event belongs to exactly one category:

| Category | What it covers |
|---|---|
| `authentication` | Login, logout, session expiry, token validation |
| `authorization` | Permission checks, access grants and denials |
| `data_access` | SQL queries, table reads, metadata reads |
| `data_mutation` | Data writes, deletes, table creation, ingestion, staging, pipes |
| `rbac_change` | Role and user CRUD, role assignments |
| `config_change` | Engine settings, table config, mirror config |
| `token_management` | Auth token creation and deletion |
| `system` | Service start/stop, health check failures, audit gaps |
| `export` | OData feed access, audit log exports |
| `security_alert` | Brute force detection, privilege escalation, anomalous patterns |

### Complete action reference

#### Authentication

| Action | When it fires | Severity |
|---|---|---|
| `login_success` | User or superuser logs in via the UI | info (user), critical (superuser) |
| `login_failure` | Invalid credentials provided | warning |
| `logout` | User signs out | info |
| `session_expired` | Session cookie timed out | info |
| `token_auth_success` | API token validated | info |
| `token_auth_failure` | Invalid API token presented | warning |
| `mcp_auth_success` | MCP shared secret validated | info |
| `mcp_auth_failure` | Invalid MCP token presented | warning |

#### Authorization

| Action | When it fires | Severity |
|---|---|---|
| `access_granted` | RBAC check passed | info |
| `access_denied` | RBAC check failed (403 response) | warning |
| `row_filter_applied` | Row-level security filter activated | info |
| `column_filter_applied` | Column-level restriction activated | info |

#### Data access

| Action | When it fires | Severity |
|---|---|---|
| `query_execute` | SQL query executed (success or failure) | info |
| `table_read` | Table metadata or schema read | info |
| `table_list` | Table listing requested | info |
| `metadata_read` | MetaReader access | info |
| `schema_read` | Schema endpoint accessed | info |

#### Data mutation

| Action | When it fires | Severity |
|---|---|---|
| `data_write` | Rows written to a table | info |
| `data_delete` | Table data deleted | warning |
| `table_create` | New table created via ingestion | info |
| `table_delete` | Table dropped | critical |
| `table_config_change` | Dedup/primary key config changed | warning |
| `staging_create` | Staging area created | info |
| `staging_delete` | Staging area deleted | warning |
| `pipe_create` | Data pipe created | info |
| `pipe_update` | Pipe configuration changed | info |
| `pipe_delete` | Pipe deleted | warning |
| `pipe_enable` | Pipe activated | info |
| `pipe_disable` | Pipe deactivated | info |
| `file_upload` | File uploaded via ingestion | info |
| `supertable_create` | New SuperTable created | warning |
| `supertable_delete` | SuperTable deleted | critical |

#### RBAC changes

| Action | When it fires | Severity |
|---|---|---|
| `role_create` | New role created | warning |
| `role_update` | Role permissions modified | warning |
| `role_delete` | Role deleted | critical |
| `user_create` | New user created | warning |
| `user_update` | User modified | warning |
| `user_delete` | User deleted | warning |
| `user_role_assign` | Role assigned to user | warning |
| `user_role_remove` | Role removed from user | warning |
| `auditor_role_create` | Auditor role provisioned | warning |
| `auditor_role_revoke` | Auditor role removed | warning |

#### Configuration changes

| Action | When it fires | Severity |
|---|---|---|
| `engine_config_change` | DuckDB/Spark settings changed | warning |
| `mirror_enable` | Mirror format enabled | info |
| `mirror_disable` | Mirror format disabled | info |
| `setting_change` | Runtime setting changed | warning |

#### Token management

| Action | When it fires | Severity |
|---|---|---|
| `token_create` | Auth token created | warning |
| `token_delete` | Auth token revoked | warning |

#### System

| Action | When it fires | Severity |
|---|---|---|
| `service_start` | API/WebUI/MCP server started | info |
| `service_stop` | Server shutting down | info |
| `health_check_failure` | Health probe failed or 500+ error | critical |
| `audit_gap` | Missing events detected in chain verification | critical |

#### Export

| Action | When it fires | Severity |
|---|---|---|
| `odata_access` | OData feed queried | info |
| `audit_export` | Audit log exported (meta-event) | info |

#### Security alerts

| Action | When it fires | Severity |
|---|---|---|
| `brute_force_detected` | 5+ failed logins from same IP within 5 minutes | critical |
| `privilege_escalation` | Attempt to access resource above role level | warning |
| `unusual_access_pattern` | Anomalous query volume or timing | warning |

### Automatic middleware coverage

In addition to the explicit `emit()` calls listed above, the audit middleware automatically captures three classes of HTTP responses across all endpoints — no per-route code required:

- **401 responses** → `token_auth_failure` (severity: warning)
- **403 responses** → `access_denied` (severity: warning)
- **500+ responses** → `health_check_failure` (severity: critical)

Health checks (`/healthz`), static assets, and favicon requests are excluded from middleware logging.

---

## Event schema

Every audit event contains these fields:

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Time-ordered unique ID (UUID v7 format) |
| `timestamp_ms` | integer | Unix epoch milliseconds (UTC) |
| `category` | string | Event category (see table above) |
| `action` | string | Specific action verb (see tables above) |
| `severity` | string | `info`, `warning`, or `critical` |
| `actor_type` | string | `user`, `superuser`, `api_token`, `system`, or `mcp` |
| `actor_id` | string | User hash or token ID |
| `actor_username` | string | Human-readable username |
| `actor_ip` | string | Client IP address |
| `actor_user_agent` | string | Browser/client user agent (truncated to 256 chars) |
| `organization` | string | Tenant organization |
| `super_name` | string | SuperTable name (empty if not applicable) |
| `correlation_id` | string | Request correlation ID from `X-Correlation-ID` header |
| `session_id` | string | Session cookie hash |
| `server` | string | `api`, `webui`, or `mcp` |
| `resource_type` | string | `table`, `role`, `user`, `token`, `staging`, `pipe`, `query`, etc. |
| `resource_id` | string | Table name, role ID, user ID, etc. |
| `detail` | string | JSON object with action-specific fields (see below) |
| `outcome` | string | `success`, `failure`, or `denied` |
| `reason` | string | Failure/denial reason (empty on success) |
| `chain_hash` | string | SHA-256 chain hash (set by the writer) |
| `instance_id` | string | Server instance identifier (hostname-PID) |

### Detail payloads

The `detail` field is a JSON string with action-specific data. Each action type has a defined schema.

**Query execution** (`query_execute`):

```json
{
  "sql_preview": "SELECT * FROM orders WHERE...",
  "row_count": 1542,
  "engine": "duckdb_lite",
  "role_name": "analyst",
  "duration_ms": 234
}
```

When `SUPERTABLE_AUDIT_FERNET_KEY` is configured, the full SQL text is also stored encrypted in `sql_encrypted`. Only superuser and auditor roles can decrypt it via the API.

**Data write** (`data_write`):

```json
{
  "table": "orders",
  "row_count": 5000,
  "inserted": 5000,
  "deleted": 0,
  "duration_ms": 890,
  "role_name": "admin",
  "delete_only": false
}
```

**File upload** (`file_upload`):

```json
{
  "mode": "table",
  "table": "orders",
  "filename": "orders_2025_q1.csv",
  "rows": 5000,
  "inserted": 5000,
  "deleted": 0,
  "file_type": "csv",
  "duration_ms": 1200
}
```

**RBAC change** (`role_create`, `role_update`):

```json
{
  "role_name": "analyst",
  "role_type": "reader",
  "tables": ["orders", "customers"]
}
```

For `role_update` and `table_config_change`, the detail includes `before` and `after` objects showing the complete state change.

**Authentication failure** (`login_failure`):

```json
{
  "method": "token",
  "username_attempted": "admin"
}
```

---

## Storage architecture

### Hot tier — Redis Streams

The last 24 hours of events are stored in a Redis Stream, one per organization:

```
supertable:{org}:audit:stream
```

This enables sub-millisecond queries from the Audit Log page, real-time auto-refresh, and external SIEM consumption via consumer groups. The stream is bounded at 100,000 entries (configurable) and trimmed based on TTL once all consumer groups have acknowledged their events.

### Warm tier — Parquet files

Every batch of events is written as a Snappy-compressed Parquet file to the configured storage backend. The partition layout is:

```
{storage_root}/{org}/__audit__/
  year=2025/
    month=03/
      day=29/
        audit_20250329_142200_api-host1-12345_a7b3c9.parquet
        audit_20250329_142205_api-host2-67890_e2f4d1.parquet
```

File names include the server instance ID and a UUID fragment, so multiple server instances can write concurrently to the same partition without conflicts or locking.

Parquet files are the system of record. They use the same storage backend as your data tables (S3, MinIO, Azure Blob, GCP Cloud Storage, or local filesystem), so backup and disaster recovery procedures apply equally to audit data.

### Hash chain integrity

Each batch is linked to the previous one via SHA-256:

```
batch_hash  = SHA-256(sorted_event_ids + parquet_file_hash)
chain_hash  = SHA-256(previous_chain_hash + batch_hash)
```

Each server instance maintains its own chain. A daily Merkle proof aggregates all instance chains:

```json
{
  "date": "2025-03-29",
  "instances": {
    "api-host1-12345": {"head": "sha256...", "batches": 42},
    "api-host2-67890": {"head": "sha256...", "batches": 38}
  },
  "merkle_root": "sha256...",
  "total_events": 80000
}
```

The chain can be verified from the Audit Log page ("Verify today" button) or the API (`GET /reflection/audit/verify`). Any tampering — a deleted batch, a modified event, a reordered sequence — breaks the chain and produces a `critical` severity `audit_gap` alert.

---

## Configuration

All settings are configured via environment variables. Defaults are designed for DORA compliance — most deployments need zero configuration.

| Environment variable | Default | Description |
|---|---|---|
| `SUPERTABLE_AUDIT_ENABLED` | `true` | Master switch. Set to `false` to disable all audit logging. |
| `SUPERTABLE_AUDIT_RETENTION_DAYS` | `2555` | Parquet file retention (~7 years). DORA requires minimum 5 years. |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | `1000` | Maximum events per Parquet file. |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | `60` | Maximum seconds before flushing a partial batch. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | `24` | Hot tier retention. Events older than this are trimmed from Redis. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | `100000` | Hard cap on Redis Stream entries. |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | `true` | Enable SHA-256 tamper-evident chaining. |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | `true` | Log SQL query execution events. |
| `SUPERTABLE_AUDIT_LOG_READS` | `true` | Log table read events. Disable to reduce volume outside DORA scope. |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | (empty) | Webhook URL for critical severity alerts. |
| `SUPERTABLE_AUDIT_LEGAL_HOLD` | `false` | When `true`, no audit data is deleted regardless of retention policy. |
| `SUPERTABLE_AUDIT_FERNET_KEY` | (empty) | Fernet encryption key for full SQL text in query events. If empty, SQL is stored in plaintext. |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | `true` | Allow external SIEM tools to register consumer groups on the Redis Stream. |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | `10` | Maximum number of external consumer groups. |

### Generating a Fernet key

To enable encrypted SQL storage in audit events:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Set the output as `SUPERTABLE_AUDIT_FERNET_KEY` in your environment.

---

## SIEM integration

External security information and event management (SIEM) tools can consume audit events directly from the Redis Stream using consumer groups. Each consumer group tracks its own read position independently — multiple tools (Splunk, Sentinel, ELK, Datadog) can consume the same stream without interfering with each other or the internal archival process.

### How consumer groups work

The internal archival worker uses a reserved group (`__archival__`) that cannot be deleted. External tools register their own groups. Each group gets an independent cursor into the stream and processes events at its own pace.

Redis Streams guarantee at-least-once delivery per group: if a consumer crashes, unacknowledged events are re-delivered when it reconnects. The TTL trimming logic respects consumer acknowledgements — events are not deleted from the stream until all groups have processed them (or the hard cap is reached).

### Connecting a SIEM tool

Configure your SIEM's Redis connector to use:

- **Host/port**: your Redis instance
- **Stream key**: `supertable:{org}:audit:stream`
- **Consumer group**: create one via the API (see below) or directly with `XGROUP CREATE`
- **Consumer name**: any unique identifier for the SIEM instance

### Consumer group management API

These endpoints require superuser authentication.

**Create a consumer group:**

```
POST /reflection/audit/consumers
{
  "group_name": "splunk_prod",
  "start_from": "0"
}
```

`start_from` controls where reading begins: `"0"` reads all existing events, `"$"` reads only new events from this point forward.

**List consumer groups:**

```
GET /reflection/audit/consumers
```

Returns all groups with lag information (how far behind each group is).

**Delete a consumer group:**

```
DELETE /reflection/audit/consumers/{group_name}
```

The internal `__archival__` group cannot be deleted.

---

## Retention and legal hold

### Default retention

| Tier | Retention | Reason |
|---|---|---|
| Redis Streams | 24 hours | Real-time access only |
| Parquet files | 7 years (2,555 days) | DORA Art. 12 requires 5 years minimum; 7 years covers most EU member state extensions |

Retention is enforced by a daily background task that deletes Parquet partitions older than `SUPERTABLE_AUDIT_RETENTION_DAYS`. Each deletion is itself recorded as an audit event.

### Legal hold

When `SUPERTABLE_AUDIT_LEGAL_HOLD=true`, all retention enforcement is suspended. No audit data is deleted, regardless of age. This is a global kill switch intended for regulatory investigations or litigation.

Legal hold activation and deactivation are themselves audit events (`config_change` / `setting_change`).

---

## Severity levels

Every event carries a severity that drives alerting and prioritization:

| Level | Meaning | Examples |
|---|---|---|
| `info` | Normal successful operation | Login, query, role read |
| `warning` | Unusual or security-noteworthy | Failed login, permission denied, RBAC change, config change |
| `critical` | Immediately significant | Superuser login, bulk delete, multiple failed logins (brute force), audit chain gap, 500 errors |

When `SUPERTABLE_AUDIT_ALERT_WEBHOOK` is configured, all `critical` events are POSTed to the webhook within seconds.

---

## Multi-instance deployments

Data Island Core supports running multiple API, WebUI, and MCP server instances simultaneously against the same Redis and storage backend. The audit system handles this without coordination:

- **Parquet files** include the instance ID (`hostname-PID`) and a UUID in their filename. Two instances writing at the same millisecond produce two separate files — no conflict, no locking.
- **Hash chains** are per-instance. Each server process maintains its own chain head in Redis. The daily Merkle proof aggregates all instance chains into a single verifiable root.
- **Redis Streams** are shared. All instances write to the same stream. Consumer groups consume from all instances transparently.

This means you can horizontally scale your servers without any audit configuration changes.

---

## Compliance mapping

### EU DORA (Regulation 2022/2554)

| DORA Article | Requirement | How the audit log satisfies it |
|---|---|---|
| Art. 6(1) | ICT risk management framework | Logs every access and mutation for risk pattern analysis |
| Art. 6(5) | Logging of ICT operations, access, and changes | Core purpose of this module; `AUDIT_LOG_READS=true` by default |
| Art. 10(1) | Detection of anomalous activities | Real-time event stream enables anomaly detection |
| Art. 10(2) | Sufficient logging for incident investigation | Every event carries correlation_id, actor, resource, outcome |
| Art. 11(1) | Classification and reporting of ICT incidents | Severity classification; DORA-aligned export format |
| Art. 12(1) | Data retention for supervisory access | Configurable retention with 7-year default and legal hold capability |
| Art. 19(1) | Record keeping of ICT-related incidents | Incident-class events with structured severity and impact |

### SOC 2 Type II (Trust Services Criteria)

| Criterion | Category | How the audit log satisfies it |
|---|---|---|
| CC6.1 | Logical access security | Logs every authentication attempt (success and failure) |
| CC6.2 | Access authorization | Logs RBAC changes, role assignments, permission evaluations |
| CC6.3 | Access removal | Logs user deprovisioning, token revocation, session termination |
| CC7.1 | System monitoring | Real-time event stream for security monitoring |
| CC7.2 | Anomaly detection | Structured severity enables alerting rules |
| CC7.3 | Incident response | Full audit trail with correlation IDs for forensic investigation |
| CC8.1 | Change management | Logs all configuration changes with before/after values |
| PI1.3 | Processing integrity | Logs data mutations with row counts, tables, timing |
| A1.2 | Availability monitoring | Logs system health events, service restarts, 500 errors |

The export API supports filtering by SOC 2 criterion, so generating evidence for a specific trust service criteria requires a single API call or one click in the Audit Log page.

---

## Module structure

```
supertable/audit/
  __init__.py          Public API: emit(), audit_context(), re-exports
  events.py            AuditEvent dataclass, EventCategory, Severity, Actions
  logger.py            AuditLogger queue + background worker, singleton cache
  chain.py             Per-instance SHA-256 chain, Merkle aggregation, verification
  crypto.py            Fernet encryption for sensitive detail fields
  writer_redis.py      Redis Streams writer + consumer group management
  writer_parquet.py    Parquet writer with concurrency-safe naming
  middleware.py        FastAPI middleware for automatic 401/403/500 logging
  reader.py            Query interface (Redis hot tier + Parquet warm tier)
  export.py            DORA and SOC 2 export formatters
  retention.py         Retention enforcement and legal hold
  consumers.py         SIEM consumer group management
```

### Integration points

The following files emit audit events:

| File | Events emitted |
|---|---|
| `api/api.py` | Query execution, RBAC CRUD, token CRUD, SuperTable CRUD, table delete/config, staging CRUD, pipe CRUD, file upload |
| `webui/application.py` | Login (success/failure for superuser and regular users), logout |
| `data_writer.py` | Data write completion |
| `mcp/mcp_server.py` | MCP query execution, MCP auth failure |
| `audit/middleware.py` | 401/403/500 responses (automatic, all endpoints) |

---

## Performance characteristics

| Metric | Value |
|---|---|
| `emit()` latency | < 50μs p99 (queue-only, no I/O) |
| Redis XADD latency | < 1ms p99 (pipeline batched) |
| Parquet write latency | < 100ms per 1,000-event batch |
| Memory per organization | < 5 MB (queue bounded at 10,000 events) |
| Background threads | 1 per organization |
| Storage per 1M events | ~50 MB (Snappy-compressed Parquet) |

### Volume estimates (with `AUDIT_LOG_READS=true`)

| Deployment | Users | Events/day | Storage/year |
|---|---|---|---|
| Small | 1–5 | ~25,000 | ~450 MB |
| Medium | 10–50 | ~500,000 | ~9 GB |
| Large | 100+ | ~5,000,000 | ~90 GB |

Setting `SUPERTABLE_AUDIT_LOG_READS=false` reduces volume by 10–50x for deployments outside DORA scope.

---

## Frequently asked questions

**Can I disable audit logging entirely?**
Yes. Set `SUPERTABLE_AUDIT_ENABLED=false`. This returns a no-op logger — zero overhead, zero events. Not recommended for production deployments with compliance requirements.

**What happens if Redis is down?**
Events are still written to Parquet (warm tier). The Redis hot tier will have a gap for the outage period, but the permanent record is intact. When Redis recovers, new events flow to both tiers. The chain verification will detect the gap and emit an `audit_gap` event.

**What happens if storage is unavailable?**
The Parquet write fails and a warning is logged. The event is still in the Redis Stream. Once storage recovers, new batches write normally. The gap in Parquet is detectable via chain verification.

**Can audit events be modified or deleted?**
No. The module provides no update or delete operations. Parquet files are append-only. The hash chain makes any external tampering detectable. Even retention enforcement (which deletes old partitions) is itself audited.

**How do I rotate the Fernet encryption key?**
Generate a new key, update `SUPERTABLE_AUDIT_FERNET_KEY`, and restart your servers. Events encrypted with the old key remain readable only if you keep the old key available. There is no automatic re-encryption of historical events.

**Does the audit log affect query performance?**
No. The `emit()` call enqueues and returns in under 50 microseconds. All I/O happens in a background thread. The audit system has zero impact on the request/response path.

**How do I verify the audit trail hasn't been tampered with?**
Use the "Verify today" button on the Audit Log page, or call `GET /reflection/audit/verify?date=YYYY-MM-DD`. This recomputes the SHA-256 chain from the stored Parquet files and compares against the recorded chain proof. Any mismatch is reported with the exact batch where the discrepancy was found.

**Can I use the audit log with an external SIEM?**
Yes. Register a consumer group on the Redis Stream (via API or directly in Redis). Your SIEM tool consumes events independently of the internal archival process. Supported tools include Splunk, Microsoft Sentinel, Elastic/ELK, Datadog, and any tool with a Redis Streams connector.
