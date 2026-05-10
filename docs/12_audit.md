# 12 -- Audit Logging

SuperTable provides an immutable, append-only audit trail for every
security-relevant action.  The subsystem lives under `supertable.audit` and
is designed for regulated environments that must satisfy DORA, SOC 2, and
similar compliance frameworks.

---

## 12.1  AuditEvent Data Model

**Module:** `supertable.audit.events`

`AuditEvent` is a frozen dataclass (`@dataclass(frozen=True)`) -- events are
immutable once created.

### 12.1.1  Field Groups

| Group | Fields | Description |
|-------|--------|-------------|
| **Identity** | `event_id`, `timestamp_ms` | Time-ordered UUID v7-like ID and Unix-ms timestamp. |
| **Classification** | `category`, `action`, `severity` | What kind of event (see enums below). |
| **Actor** | `actor_type`, `actor_id`, `actor_username`, `actor_ip`, `actor_user_agent` | Who triggered the event. |
| **Context** | `organization`, `super_name`, `correlation_id`, `session_id`, `server` | Tenant, request trace, server identity. |
| **Resource** | `resource_type`, `resource_id` | What was acted upon. |
| **Operation** | `detail`, `outcome`, `reason` | Action-specific JSON payload, result, and failure reason. |
| **Integrity** | `chain_hash` | Set by the writer (not the emitter) for tamper detection. |
| **Instance** | `instance_id` | `hostname-PID`, stable per process. |

### 12.1.2  Event ID Generation (`_uuid7`)

Event IDs are time-ordered for lexicographic = chronological sorting:

```
{unix_ms_hex:012x}-{counter_hex:04x}-{random_hex:8}
```

### 12.1.3  Serialisation Methods

| Method | Output | Use Case |
|--------|--------|----------|
| `to_dict()` | Flat dict | Redis XADD, Parquet row. |
| `to_json()` | Compact JSON string | Log lines, export. |
| `event_hash()` | SHA-256 hex | Chain input (excludes `chain_hash` and `instance_id`). |
| `from_dict(d)` | `AuditEvent` | Reconstruct from storage. |

---

## 12.2  Classification Enums

### 12.2.1  EventCategory

```python
class EventCategory(str, Enum):
    AUTHENTICATION  = "authentication"
    AUTHORIZATION   = "authorization"
    DATA_ACCESS     = "data_access"
    DATA_MUTATION   = "data_mutation"
    RBAC_CHANGE     = "rbac_change"
    CONFIG_CHANGE   = "config_change"
    TOKEN_MGMT      = "token_management"
    SYSTEM          = "system"
    EXPORT          = "export"
    SECURITY_ALERT  = "security_alert"
```

### 12.2.2  Severity

```python
class Severity(str, Enum):
    INFO     = "info"
    WARNING  = "warning"
    CRITICAL = "critical"
```

Severity drives alerting thresholds and retention priority.

### 12.2.3  Outcome

```python
class Outcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED  = "denied"
```

### 12.2.4  ActorType

```python
class ActorType(str, Enum):
    USER      = "user"
    SUPERUSER = "superuser"
    API_TOKEN = "api_token"
    SYSTEM    = "system"
    MCP       = "mcp"
```

---

## 12.3  Actions Constants

The `Actions` class contains every canonical action verb grouped by category.
All `emit()` calls must reference one of these constants.

### Authentication
`LOGIN_SUCCESS`, `LOGIN_FAILURE`, `LOGOUT`, `SESSION_EXPIRED`,
`TOKEN_AUTH_SUCCESS`, `TOKEN_AUTH_FAILURE`, `MCP_AUTH_SUCCESS`,
`MCP_AUTH_FAILURE`

### Authorization
`ACCESS_GRANTED`, `ACCESS_DENIED`, `ROW_FILTER_APPLIED`,
`COLUMN_FILTER_APPLIED`

### Data Access
`QUERY_EXECUTE`, `TABLE_READ`, `TABLE_LIST`, `METADATA_READ`, `SCHEMA_READ`

### Data Mutation
`DATA_WRITE`, `DATA_DELETE`, `TABLE_CREATE`, `TABLE_DELETE`,
`TABLE_CONFIG_CHANGE`, `STAGING_CREATE`, `STAGING_DELETE`,
`PIPE_CREATE`, `PIPE_UPDATE`, `PIPE_DELETE`, `PIPE_ENABLE`, `PIPE_DISABLE`,
`PIPE_EXECUTE`, `FILE_UPLOAD`, `SUPERTABLE_CREATE`, `SUPERTABLE_DELETE`,
`SUPERTABLE_CLONE_READONLY`, `SUPERTABLE_CLONE_WRITABLE`,
`SUPERTABLE_CLONE_REPLICA`, `SUPERTABLE_TOGGLE_READONLY`,
`SUPERTABLE_PROMOTE`, `SUPERTABLE_DETACH`, `TABLE_CLONE`

### Data Sharing
`SHARE_CREATE`, `SHARE_REVOKE`, `SHARE_MANIFEST_ACCESS`, `SHARE_LINK`,
`SHARE_UNLINK`, `SHARE_MATERIALIZE`, `PUBLICATION_CREATE`,
`PUBLICATION_REVOKE`, `PUBLICATION_ACCEPT`

### RBAC Changes
`ROLE_CREATE`, `ROLE_UPDATE`, `ROLE_DELETE`, `ROLE_ENABLE`, `ROLE_DISABLE`,
`ROLE_CLONE`, `USER_CREATE`, `USER_UPDATE`, `USER_DELETE`, `USER_ENABLE`,
`USER_DISABLE`, `USER_ROLE_ASSIGN`, `USER_ROLE_REMOVE`,
`AUDITOR_ROLE_CREATE`, `AUDITOR_ROLE_REVOKE`

### Configuration Changes
`ENGINE_CONFIG_CHANGE`, `MIRROR_ENABLE`, `MIRROR_DISABLE`, `SETTING_CHANGE`

### Token Management
`TOKEN_CREATE`, `TOKEN_DELETE`, `TOKEN_REGENERATE`

### System
`SERVICE_START`, `SERVICE_STOP`, `HEALTH_CHECK_FAILURE`, `AUDIT_GAP`

### Export
`ODATA_ACCESS`, `AUDIT_EXPORT`

### Retention & Legal Hold
`RETENTION_EXECUTE`, `LEGAL_HOLD_CHANGE`

### Garbage Collection
`GC_EXECUTE`, `GC_PREVIEW`

### Snapshot History
`SNAPSHOT_HISTORY_READ`

### Security Alerts
`BRUTE_FORCE_DETECTED`, `PRIVILEGE_ESCALATION`, `UNUSUAL_ACCESS_PATTERN`

---

## 12.4  Emitting Events -- Public API

**Module:** `supertable.audit` (`__init__.py`)

The primary interface is the `emit()` convenience function:

```python
from supertable.audit import emit, EventCategory, Actions, Severity, make_detail

emit(
    category=EventCategory.RBAC_CHANGE,
    action=Actions.ROLE_CREATE,
    organization="acme",
    actor_type="superuser",
    actor_id="abc123",
    actor_username="admin",
    resource_type="role",
    resource_id="role_456",
    detail=make_detail(role_name="analyst", role_type="viewer"),
    severity=Severity.WARNING,
)
```

All parameters are keyword-only.  The function constructs an `AuditEvent` and
passes it to the background logger for the given organization.

### `audit_context(request)` Helper

Extracts actor, correlation, and session info from a FastAPI `Request`:

```python
emit(**audit_context(request), category=..., action=..., ...)
```

Extracted fields: `correlation_id`, `actor_ip`, `actor_user_agent`,
`actor_username`, `actor_id`, `session_id`, `actor_type`
(resolves `SUPERUSER` vs `USER` from session state).

### `make_detail(**kwargs)` Helper

Serialises action-specific fields to a compact JSON string for the `detail`
field:

```python
detail = make_detail(sql_hash="abc", row_count=42, duration_ms=123)
# -> '{"sql_hash":"abc","row_count":42,"duration_ms":123}'
```

`None` values are silently dropped.

---

## 12.5  Background Logger Worker

**Module:** `supertable.audit.logger`

### 12.5.1  Architecture

The `AuditLogger` follows a producer-consumer pattern identical to
`monitoring_writer.py`:

1. `emit(event)` enqueues to a bounded `queue.Queue` (max 10,000 entries)
   and returns immediately (< 50us).
2. A background daemon thread drains the queue in batches.
3. Batches are written to Redis Streams (hot tier) and Parquet (warm tier).
4. One `AuditLogger` instance per organization (singleton cache via
   `get_audit_logger(org)`).

### 12.5.2  `AuditConfig`

Configuration is loaded lazily from settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `SUPERTABLE_AUDIT_ENABLED` | `True` | Master switch. |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | `1000` | Events per write batch. |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | `60` | Max seconds between flushes. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | `24` | Hot-tier TTL. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | `100,000` | Max stream entries (approximate trimming). |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | `True` | Enable SHA-256 chain. |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | `True` | Log query executions. |
| `SUPERTABLE_AUDIT_LOG_READS` | `True` | Log read operations. |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | `""` | Webhook URL for critical alerts. |
| `SUPERTABLE_AUDIT_FERNET_KEY` | `""` | Fernet encryption key for sensitive fields. |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | `True` | Enable SIEM consumer groups. |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | `10` | Max external consumer groups. |

### 12.5.3  NullAuditLogger

When auditing is disabled, a `NullAuditLogger` is returned -- all methods
(`emit`, `flush`, `stop`) are no-ops.

### 12.5.4  Queue Backpressure

If the queue reaches 10,000 entries, `emit()` drops the event and logs a
warning.  The `_stats` dict tracks `total_emitted`, `total_written`,
`total_dropped`, and `batches_written`.

---

## 12.6  Hash Chain -- Tamper-Evident Integrity

**Module:** `supertable.audit.chain`

The hash chain provides cryptographic tamper detection for the audit trail.
If any event is modified, inserted, or deleted after the fact, the chain
verification will fail.

### 12.6.1  Chain Computation

```
batch_hash  = SHA-256(sorted(event_ids) + "\n" + parquet_file_hash)
chain_hash  = SHA-256(previous_chain_hash + batch_hash)
```

* Event IDs are sorted to ensure deterministic ordering.
* The chain starts from `GENESIS_HASH = "0" * 64`.

### 12.6.2  `InstanceChain`

Each server instance maintains its own chain:

```python
@dataclass
class InstanceChain:
    instance_id: str
    head: str = GENESIS_HASH    # Current chain head
    batch_count: int = 0

    def advance(self, event_ids: List[str], file_hash: str = "") -> str: ...
```

`advance()` appends a batch and returns the new head.  The `AuditLogger`
holds a lock when calling `advance()` for thread safety.

### 12.6.3  Daily Merkle Proof

`MerkleProof` aggregates all instance chains into a single verifiable root:

```python
@dataclass
class MerkleProof:
    date: str               # "2025-01-15"
    instances: Dict[str, Dict[str, Any]]
    merkle_root: str
    total_events: int
    created_ms: int
```

`compute_root()` sorts instance heads by `instance_id` and computes a
SHA-256 over the concatenation -- deterministic regardless of insertion
order.

### 12.6.4  Verification Functions

| Function | Input | Output |
|----------|-------|--------|
| `verify_batch_chain(batches, expected_head, starting_hash)` | List of batch dicts | `{"valid": bool, "batches_checked": int, "gaps": [...], "computed_head": str}` |
| `verify_merkle_proof(proof)` | `MerkleProof` | `{"valid": bool, "computed_root": str, "recorded_root": str}` |

`verify_batch_chain` replays the chain from `starting_hash` and reports any
gaps where `computed != recorded`.  It continues past gaps (using the
recorded hash) to detect further tampering.

---

## 12.7  Dual-Tier Storage

### 12.7.1  Hot Tier -- Redis Streams

**Module:** `supertable.audit.writer_redis`

Each organization gets its own Redis Stream:

```
supertable:{org}:audit:stream
```

**`RedisAuditWriter`** manages writes and consumer groups:

* `write_batch(events)` -- pipelined `XADD` with `MAXLEN~` (approximate
  trimming) for bounded memory.
* Chain head is persisted at `supertable:{org}:audit:chain_head:{instance_id}`.
* An `__archival__` consumer group is created automatically for the internal
  archival worker.
* External SIEM consumer groups are created on demand (see Section 12.9).

### 12.7.2  Warm/Cold Tier -- Parquet

**Module:** `supertable.audit.writer_parquet`

Parquet is the system of record.  Files are append-only, partitioned by date,
and named with `instance_id` + UUID for safe concurrent writes.

**Partition layout:**

```
{storage_root}/{org}/__audit__/year=YYYY/month=MM/day=DD/
    audit_{date}_{time}_{instance_id}_{uuid8}.parquet
```

**Chain proofs:**

```
{storage_root}/{org}/__audit__/_chain/chain_{date}.json
```

The Parquet schema mirrors `AuditEvent` exactly -- 21 columns, all string
or int64 types, built as a PyArrow schema.

`compute_file_hash(data: bytes)` produces a SHA-256 of the raw Parquet file
content, which feeds into the chain as `file_hash`.

---

## 12.8  HTTP Middleware

**Module:** `supertable.audit.middleware`

`AuditMiddleware` is a Starlette `BaseHTTPMiddleware` that provides a safety
net for events that might not have explicit audit calls in endpoint handlers.

**Captures:**
* Authentication failures (HTTP 401)
* Authorization denials (HTTP 403)
* Server errors (HTTP 500+, severity: `CRITICAL`)
* Service identification (`api` / `webui` / `mcp`)

**Excludes:** `/healthz`, `/health`, `/favicon.ico`, `/static/*`

**Organisation extraction** tries three sources in priority order:
1. `request.state.session_org`
2. Query parameters (`?organization=` or `?org=`)
3. Global default from `settings.SUPERTABLE_ORGANIZATION`

Installation:

```python
from supertable.audit.middleware import AuditMiddleware
app.add_middleware(AuditMiddleware, server="api")
```

---

## 12.9  SIEM Consumer Groups

**Module:** `supertable.audit.consumers`

External SIEM tools (Splunk, Microsoft Sentinel, ELK, Datadog) register
Redis Stream consumer groups to consume audit events independently of the
internal archival worker.

| Function | Signature | Description |
|----------|-----------|-------------|
| `create_consumer` | `(organization, group_name, start_from="$")` | Create an external consumer group. `start_from="$"` = new events only; `"0"` = replay from beginning. |
| `delete_consumer` | `(organization, group_name)` | Remove a consumer group. |
| `list_consumers` | `(organization)` | List all consumer groups with lag info. |

Each function instantiates a `RedisAuditWriter` and delegates to its
consumer group management methods.  The maximum number of external consumers
is governed by `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` (default: 10).

---

## 12.10  Retention Policies

**Module:** `supertable.audit.retention`

### 12.10.1  Default Retention

The default retention period is **2,555 days (approximately 7 years)**,
configured via `SUPERTABLE_AUDIT_RETENTION_DAYS`.  This satisfies the DORA
Art. 12 minimum of 5 years with a comfortable margin.

Retention enforcement deletes Parquet partitions (Hive-style
`year=YYYY/month=MM/day=DD`) older than the retention threshold.

Partition dates are parsed from paths using the regex:

```
year=(\d{4})[/\\]month=(\d{2})[/\\]day=(\d{2})/?$
```

### 12.10.2  Legal Hold

Legal hold is a global kill switch that prevents **all** audit deletions for
an organization.

**Resolution order:**
1. Redis runtime override at key `supertable:{org}:audit:legal_hold`
   (set by `set_legal_hold()`).
2. Settings default (`SUPERTABLE_AUDIT_LEGAL_HOLD`).

**Fail-safe:** if both lookups fail, legal hold defaults to **active** (True)
so that data is never accidentally deleted.

Legal hold state is persisted in Redis (not in the frozen Settings dataclass)
so it can be toggled at runtime without a restart.

All deletions are recorded as audit events (meta-event: the audit log audits
its own cleanup).

---

## 12.11  Fernet Encryption

When `SUPERTABLE_AUDIT_FERNET_KEY` is configured, sensitive fields within the
`detail` payload can be encrypted at rest using the Python `cryptography`
library's Fernet symmetric encryption (AES-128-CBC with HMAC-SHA256).

This protects PII and other sensitive data in the audit trail while still
allowing authorised tooling to decrypt and inspect events.

---

## 12.12  Export for Compliance Reporting

**Module:** `supertable.audit.export`

### 12.12.1  Generic Export

```python
def export_events(events: List[Dict], output_format: str = "json") -> bytes:
```

Supported formats:
* `"json"` -- JSON-lines (one JSON object per line).
* `"csv"` -- CSV with header row.

### 12.12.2  DORA Incident Reports

```python
def export_dora_incident_report(
    organization: str,
    incident_id: str,
    start_ms: int,
    end_ms: int,
    output_format: str = "json",
) -> bytes:
```

Exports audit events for a specific time window aligned to DORA RTS/ITS
incident reporting templates (Regulation 2024/1772).

### 12.12.3  SOC 2 Evidence Packages

```python
def export_soc2_evidence(
    organization: str,
    criteria: str,
    period_start_ms: int,
    period_end_ms: int,
    output_format: str = "json",
) -> bytes:
```

Maps each SOC 2 Trust Services Criterion to specific event categories and
actions.  Supported criteria:

| Criterion | Category Mapped |
|-----------|-----------------|
| `CC6.1` | `authentication` (all auth events) |
| `CC6.2` | `authorization` |
| `CC7.1` | Monitoring events |
| `CC7.3` | Incident response (spans all categories) |
| `CC8.1` | Change management |
| `PI1.3` | Processing integrity |
| `A1.2` | Availability |

---

## 12.13  Compliance Mapping

The audit subsystem is designed to satisfy the following regulatory
requirements:

### DORA (Digital Operational Resilience Act, Regulation 2022/2554)

| Article | Requirement | How SuperTable Satisfies It |
|---------|-------------|---------------------------|
| Art. 6(5) | ICT risk management documentation | All security-relevant events are captured with full actor/resource context. |
| Art. 10 | Detection and monitoring | Real-time Redis Streams with SIEM consumer groups for external monitoring tools. |
| Art. 11 | Response and recovery | DORA-aligned incident report export. |
| Art. 12 | Record keeping (5+ year retention) | Default 7-year retention; Parquet cold storage; legal hold prevents accidental deletion. |

### SOC 2 Type II

| Criterion | Requirement | How SuperTable Satisfies It |
|-----------|-------------|---------------------------|
| CC6.1 | Logical access security | Authentication events (login success/failure, token auth). |
| CC7.1 | System monitoring | AuditMiddleware captures all auth failures and server errors; background logger provides continuous monitoring. |
| CC7.3 | Forensic integrity | SHA-256 hash chain with daily Merkle proofs; `verify_batch_chain()` and `verify_merkle_proof()` for tamper detection. |
| CC8.1 | Change management | CONFIG_CHANGE and RBAC_CHANGE event categories track all configuration and permission changes. |
| A1.2 | Availability | SYSTEM events track service start/stop and health check failures. |

### Business Context

The audit subsystem provides:

* **Non-repudiation** -- every action is attributed to a specific actor
  (user, API token, system) with IP address and user agent.
* **Tamper evidence** -- the SHA-256 hash chain and daily Merkle proofs
  ensure that any modification to the audit trail is detectable.
* **Real-time visibility** -- Redis Streams provide sub-second event
  availability for monitoring dashboards and SIEM integrations.
* **Long-term archival** -- Parquet files provide efficient columnar storage
  for years of audit data, with date-based partitioning for fast range
  queries.

## 12.14  Enable / disable at runtime

Audit is **OFF by default** (`SUPERTABLE_AUDIT_ENABLED=false`).  Each
organization can be toggled independently from the WebUI:

> **WebUI → /ui/audit → Compliance tab → Audit logging card**

Behind the toggle, the master switch and four sub-toggles (`log_queries`,
`log_reads`, `hash_chain`, `siem_enabled`) are persisted in Redis at
`supertable:{org}:audit:config` (HASH), and surfaced via:

```
GET  /api/v1/audit/config?organization=<org>
POST /api/v1/audit/config   { "organization": "<org>", "enabled": true, ... }
```

Both endpoints require **superuser** authentication (same gate as legal
hold).  Flipping the toggle:

* **OFF → ON**: a new `AuditLogger` is lazily created on the next
  `emit()` and starts writing to `supertable:{org}:audit:stream`.
* **ON → OFF**: the running logger is drained and stopped, replaced
  with a `NullAuditLogger`; subsequent emits are no-ops.

Every config write emits a `CONFIG_CHANGE` audit event so that
disabling auditing is itself recorded.  In a multi-instance
deployment, a 30-second per-org cache TTL bounds how long peer
instances take to pick up the change; the responding instance applies
it immediately via cache invalidation.
