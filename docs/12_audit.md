# 12 -- Audit Logging

SuperTable has two audit lanes.  General access and telemetry events use the
configurable, non-blocking logger described below. Privileged RBAC and
organization auth-token state changes use a separate mandatory write-ahead
ledger: they cannot successfully commit without a durable audit record in the
same Redis transaction.

The subsystem lives under `supertable.audit` and provides the controls needed
to build regulated DORA, SOC 2, HIPAA, and SOX operating procedures.  External
WORM/Object-Lock configuration, retention policy, backup, and access to the
audit storage remain deployment responsibilities.

---

## 12.0  Mandatory Privileged Control-Plane Ledger

Role, user, role-assignment, and organization auth-token mutations do **not**
use the optional `emit()` queue. All ten catalog mutation boundaries append a
bounded digest-only record with `XADD` inside the same Lua script that changes
the security document/index/token record and namespace revision:

* `role_create`, `role_update`, `role_delete`
* `user_create`, `user_update`, `user_delete`
* `user_role_assign`, `user_role_remove`
* `token_create`, `token_delete`

The organization-level keys are:

```text
supertable:{org}:system:audit:privileged:outbox   # mandatory Redis Stream WAL; no MAXLEN
supertable:{org}:system:audit:privileged:meta     # exact sequence/head HASH
supertable:{org}:system:audit:privileged:delivery # verified archive ledger
supertable:{org}:system:audit:privileged:cascade:doc:{event_id}
                                                    # role-delete user manifest
```

This path is always enabled.  `SUPERTABLE_AUDIT_ENABLED=false`, queue
backpressure, or a disabled general logger cannot suppress it.  A malformed
record, wrong Redis key type, corrupt/overflowing revision counter, or failed
`XADD` raises and the privileged operation does not report success.  No-op and
lost-CAS operations never create false success records.  Instead, public
manager validation rejections and catalog outcomes known to have made no
privileged state write use a standalone Lua append that advances the same
organization ledger without mutating security state. Its explicit outcomes are
`denied`, `failure`, and `no_change`; the standalone path rejects `success`,
which remains reserved for the ten transactional mutation scripts. Unknown or
ambiguous backend exceptions are not relabeled as a definitive
privileged-operation failure.

A state-dependent `no_change` is appended only when a bounded predicate is
still true in that same Lua invocation. The public API accepts a closed,
action-and-cause-specific semantic grammar (resource presence, identity claim,
assignment membership, or token presence); callers cannot provide Redis keys
or arbitrary hash fields. The catalog derives every key from the audited
organization, SuperTable, resource, and assignment identity. If a predicate is
false, no `no_change` record is written; a separate state-neutral
`concurrent_modification` failure is durably appended and the caller must
retry.

Role/user bootstrap initialization is validation-only. An empty namespace does
not acquire an unaudited meta key or revision. Its first successful mutation
creates the `version=1` namespace head and the corresponding SYSTEM bootstrap
record atomically.

### Record contents and privacy

`PrivilegedAuditRecord` includes the immutable actor/session/correlation
context, action and resource identity, before/after document versions,
changed field names, assignment deltas, namespace version, cascade count, and
a strictly monotonic organization ledger sequence.  Large assignment deltas
store an ordered preview plus the exact total count and SHA-256 of the complete
role-ID set, so an existing user with many roles remains auditable without an
unbounded event.  Role policies, row-filter
literals, tokens, and user documents are never embedded.  The record contains
canonical SHA-256 digests of the exact pre/post Redis security documents.

For `token_create` and `token_delete`, `resource_id` is the lowercase SHA-256
token ID. The ledger commits canonical before/after digests of the exact token
metadata record, but never embeds that metadata JSON or the plaintext token.
The plaintext token is returned once by creation and never enters Redis or the
privileged stream.

A role deletion that strips assignments also creates one exact sidecar HASH
under its event ID in the same Lua boundary.  The parent record's
`affected_count` is the number of affected users;
`cascade_assignment_count` is the number of removed role occurrences (these
can differ for a corrupt legacy list containing duplicates).  Each sidecar row
contains only the user ID, before/after user-document version, removed
occurrence count, and before/after role counts.  It never contains usernames,
role lists, user documents, table policies, filters, or filter literals.  The
sidecar also binds the before/after user-namespace revision to the parent.

The atomic cascade is capped at **10,000 users**. Before `SMEMBERS` or any
document scan, Lua bounds both the authoritative user index and username map,
requires equal cardinality, then proves every indexed ID, username mapping,
and user document agree before writing a sidecar, event, or RBAC mutation.
Larger tenants must first use an operator-controlled migration
or a future fenced/chunked revocation workflow.  This bound limits Redis
script time and transient evidence footprint; evidence is never truncated to
make an oversized deletion appear successful.

Redis Lua preserves the validated event template byte-for-byte.  Commit-time
sequence, namespace revision, and affected count are stored as separate exact
decimal stream fields, avoiding Redis Lua JSON array coercion and numeric
rounding above 2^53.  `PrivilegedAuditOutbox` merges and revalidates that
envelope before returning or archiving an event.
Before every mutation, Lua also proves that the retained stream head and meta
head agree; a missing/empty stream with a positive sequence fails closed.

Callers should pass `PrivilegedActionContext` to every `RoleManager` and
`UserManager` mutation; auth-token create/delete requires the same context at
its catalog boundary. Missing legacy context is visibly recorded as
`system/legacy-unattributed`; bootstrap create/repair uses an explicit SYSTEM
context.

### Retrieval and verified archival

```python
from supertable.audit import get_privileged_audit_outbox

outbox = get_privileged_audit_outbox("acme")
events = outbox.query(newest_first=False)

# One bounded cycle for a separately supervised archival worker:
result = outbox.drain_once(
    "acme",
    consumer="audit-worker-01",
    count=500,
)
```

Backend or record errors raise explicit exceptions; `[]` means Redis
authoritatively returned no events.  Archival uses a deterministic batch ID
and a dedicated Parquet schema containing both the exact template JSON and
the fully committed canonical JSON plus indexed fields.  The bytes and rows
are read back and compared before the delivery ledger is marked and the
consumer group is acknowledged.  A crash or storage outage leaves entries
pending; retry converges on the same object.  Local archives use file fsync,
atomic same-filesystem replace, and bounded directory-chain fsync, so a crash
cannot publish a partial or unanchored target. Existing-object retries
re-establish file and directory durability before acknowledging the source.

The delivery ledger advances one organization-wide contiguous sequence
checkpoint. While a batch is active, its first-sequence claim and exact stream
membership are atomically created and compare-and-set through
`writing`/`written_verified`/`delivered`; stale workers cannot regress the
head or repartition abandoned work. Before delivery, each batch also writes a
canonical organization-scoped checkpoint manifest under
`{org}/__audit__/privileged/manifests/`. The manifest commits the exact
sequence/stream range, parent and cascade paths, byte counts, schemas/codecs,
SHA-256 hashes, and predecessor checkpoint hash. It is read back before Redis
delivery is finalized.

After verified source trimming, per-entry markers, completed batch records,
and obsolete sequence claims are atomically garbage-collected from Redis.
The immutable manifest/Parquet chain remains authoritative and Redis retains
only the bounded current head plus active anchor metadata. Reading a batch
whose Redis index has been collected requires its organization, for example
`read_archive_batch(batch_id, organization="acme")` or
`read_archive_cascades(batch_id, organization="acme")`.
Stream entries may be deleted only after verified delivery markers and
consumer acknowledgements exist, and the newest entry is retained as a
sequence/head integrity anchor.
Role-delete batches additionally write a row-oriented cascade Parquet sidecar.
Both Parquet objects are byte- and row-verified and their SHA-256 metadata is
stored in the delivery ledger before the parent stream entry is acknowledged.
A missing, altered, mis-scoped, or count-inconsistent cascade manifest blocks
delivery and acknowledgement.  Redis cascade manifests are removed only after
the archive marker and consumer acknowledgement are verified during trimming;
archived sidecars remain independently readable.

`verify_checkpoint_head(org)` is the bounded worker heartbeat: it verifies
the Redis head, latest immutable manifest and immediate predecessor, and the
latest Parquet artifacts. `verify_checkpoint_chain(org, max_batches=...)`
performs an explicitly bounded walk through every predecessor to genesis and
revalidates every artifact; use it for scheduled compliance verification.
The SHA-256 chain detects alteration but is not a KMS-backed signature and
does not by itself prevent an administrator from replacing both data and
hashes. Externally managed WORM/Object Lock, independent credentials, backup,
and retention are the authoritative immutability boundary.

Run `drain_once()` from a separately supervised worker and alert on outbox
length, pending age, archive errors, and Redis capacity.  The privileged
stream intentionally has no `MAXLEN`; configure Redis with durable AOF/replica
policy and no-eviction capacity appropriate to the maximum archive outage.
Privileged mutation scripts currently target standalone Redis or Sentinel. Their
per-SuperTable state keys and organization outbox key do not share a Redis
Cluster hash slot, so Redis Cluster mode is not supported for this boundary.
Restrict direct Redis RBAC and auth-token writes with ACLs: applications must
use the catalog mutation scripts, because an operator with unrestricted
`HSET`/`HDEL`/`DEL` access can bypass any application-level audit boundary. The
script service account must be allowed the complete command set used by the
registered Lua scripts.

### Activation on an existing estate

The mandatory ledger is complete **from its controlled activation point**; it
cannot reconstruct changes made by an older binary before that point.  Treat
activation as a security migration, not a rolling feature toggle:

1. Stop and drain every API process, worker, notebook, and maintenance job that
   can mutate RBAC or organization auth-token state.
2. Export a bounded, independently signed baseline of every role document,
   user document, assignment, index, auth-token ID/metadata digest, and
   namespace revision. Store only the canonical document/metadata hashes,
   token IDs, and assignment identities in the evidence package; keep it in
   the same WORM retention domain as the archive.
3. Give the audited release a distinct Redis ACL username in the authoritative
   direct `SUPERTABLE_REDIS_URL`, or via `SUPERTABLE_REDIS_USERNAME` when split
   settings/Sentinel are used. Revoke the legacy user and close its existing
   connections before accepting another privileged mutation.
4. Keep normal traffic closed, start one audited writer, and perform one
   approved, controlled privileged mutation tied to a cutover ticket. Confirm
   that it produced the expected next ledger sequence.
5. Archive that queued event with one bounded worker cycle. Omitting `--trim`
   here is intentional, so the Redis source remains available during cutover
   validation:

   ```bash
   supertable-privileged-audit-worker \
     --organization acme \
     --consumer audit-cutover \
     --once \
     --require-durable-redis
   ```

6. Verify the complete checkpoint/artifact chain and independently confirm the
   new objects' WORM/Object-Lock and retention settings through the storage
   provider control plane:

   ```bash
   supertable-privileged-audit-worker \
     --organization acme \
     --consumer audit-cutover-verifier \
     --verify-chain \
     --verify-max-batches 10000 \
     --require-durable-redis
   ```

7. Match the mutation's pre/post digest and checkpoint to the signed baseline,
   then start normal audited writers and the supervised archive worker.
8. Preserve the baseline, deployment manifest, ACL change evidence, controlled
   mutation ticket, and first checkpoint together for restore and forensic
   verification.

Running `--once` against an empty stream does not open or write archive storage
and is not a storage-credential canary. The controlled event above must be
present before the validation cycle.

Do not activate exclusion or audit-v2 policies while an older writer still has
Redis write credentials: old code does not know the new transaction boundary
and a marker key cannot constrain a client that can directly execute
`HSET`/`SADD`/`DEL`.  A future built-in genesis/fleet-fence workflow may
automate the baseline, but credential revocation and a quiescent cutover remain
the trust boundary.  Greenfield estates begin with an empty signed baseline.

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
    NO_CHANGE = "no_change"  # validated idempotent/state-neutral attempt
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
supertable:{org}:system:audit:stream
```

(built by `redis_keys.audit_stream(org)` — the `system:` segment
groups every org-level system surface alongside `shares:`, `auth:`,
and `spark:`).

**`RedisAuditWriter`** manages writes and consumer groups:

* `write_batch(events)` -- pipelined `XADD` with `MAXLEN~` (approximate
  trimming) for bounded memory.
* Chain head is persisted at `supertable:{org}:system:audit:chain_head:doc:{instance_id}` (built by `redis_keys.audit_chain_head(org, instance_id)`).
* An `__archival__` consumer group is created automatically for the internal
  archival worker.
* External SIEM consumer groups are created on demand (see Section 12.9).

### 12.7.2  General-event Warm/Cold Tier -- Parquet

**Module:** `supertable.audit.writer_parquet`

For the configurable general-event lane, Parquet is the long-term tier. Files
are append-only, partitioned by date, and named with `instance_id` + UUID for
safe concurrent writes. Privileged control-plane archival instead uses the
deterministic verified format in Section 12.0.

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
1. Redis runtime override at key `supertable:{org}:system:audit:legal_hold`
   (built by `redis_keys.audit_legal_hold(org)`, set by `set_legal_hold()`).
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
| Art. 6(5) | ICT risk management documentation | Mandatory RBAC and organization auth-token state changes are durably recorded; other event classes are recorded when their integration points call the general audit API. |
| Art. 10 | Detection and monitoring | Real-time Redis Streams with SIEM consumer groups for external monitoring tools. |
| Art. 11 | Response and recovery | DORA-aligned incident report export. |
| Art. 12 | Record keeping (5+ year retention) | Parquet cold storage and legal-hold controls support a deployment-defined retention policy; configure and verify the required period operationally. |

### SOC 2 Type II

| Criterion | Requirement | How SuperTable Satisfies It |
|-----------|-------------|---------------------------|
| CC6.1 | Logical access security | Authentication events (login success/failure, token auth). |
| CC7.1 | System monitoring | AuditMiddleware captures all auth failures and server errors; background logger provides continuous monitoring. |
| CC7.3 | Forensic integrity | SHA-256/Merkle verification utilities plus privileged-ledger sequence and archive checks; external immutable retention is required for an independent trust anchor. |
| CC8.1 | Change management | The mandatory ledger tracks committed role, user, assignment, and organization auth-token changes; CONFIG_CHANGE coverage depends on the emitting integration. |
| A1.2 | Availability | SYSTEM events track service start/stop and health check failures. |

### Business Context

The audit subsystem provides:

* **Actor evidence** -- privileged callers can bind an immutable actor,
  session, correlation ID, reason, and ticket to each committed privileged
  change. Legacy calls without context are explicitly marked
  `system/legacy-unattributed`; they are not presented as attributed actions.
* **Tamper checks** -- exact record/file hashes, sequence checkpoints, and
  read-back verification detect accidental or unprivileged modification.
  Strong non-repudiation requires deployment-managed WORM/Object Lock and
  independently controlled signing/retention credentials.
* **Real-time visibility** -- Redis Streams provide sub-second event
  availability for monitoring dashboards and SIEM integrations.
* **Long-term archival** -- Parquet files provide efficient columnar storage
  for years of audit data, with date-based partitioning for fast range
  queries.

## 12.14  Enable / disable at runtime

General asynchronous audit is **OFF by default**
(`SUPERTABLE_AUDIT_ENABLED=false`). The privileged control-plane mutation
ledger in Section 12.0 is mandatory and cannot be disabled by this setting.
General audit for each organization can be toggled independently from the WebUI:

> **WebUI → /ui/audit → Compliance tab → Audit logging card**

Behind the toggle, the master switch and four sub-toggles (`log_queries`,
`log_reads`, `hash_chain`, `siem_enabled`) are persisted in Redis at
`supertable:{org}:system:audit:config` (HASH, built by
`redis_keys.audit_config(org)`), and surfaced via:

```
GET  /api/v1/audit/config?organization=<org>
POST /api/v1/audit/config   { "organization": "<org>", "enabled": true, ... }
```

Both endpoints require **superuser** authentication (same gate as legal
hold).  Flipping the toggle:

* **OFF → ON**: a new `AuditLogger` is lazily created on the next
  `emit()` and starts writing to `supertable:{org}:system:audit:stream`.
* **ON → OFF**: the running logger is drained and stopped, replaced
  with a `NullAuditLogger`; subsequent emits are no-ops.

Every config write emits a `CONFIG_CHANGE` audit event so that
disabling auditing is itself recorded.  In a multi-instance
deployment, a 30-second per-org cache TTL bounds how long peer
instances take to pick up the change; the responding instance applies
it immediately via cache invalidation.
