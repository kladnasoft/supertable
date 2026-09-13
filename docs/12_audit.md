# Audit events

The audit package accepts structured events and attempts to write each batch independently to Redis Streams and Parquet storage. It is disabled by default. Delivery is asynchronous and best effort; an application operation can succeed even when its audit event is dropped or one storage destination fails.

Implementation: [audit package](../supertable/audit/__init__.py), [events](../supertable/audit/events.py), [logger](../supertable/audit/logger.py), [Redis writer](../supertable/audit/writer_redis.py), and [Parquet writer](../supertable/audit/writer_parquet.py).

## 1. Enable and emit events

Enable auditing before starting the process:

```sh
export SUPERTABLE_AUDIT_ENABLED=true
```

Applications can emit additional events explicitly:

```python
from supertable.audit import Actions, EventCategory, emit, get_audit_logger

emit(
    category=EventCategory.SYSTEM,
    action=Actions.SERVICE_START,
    organization="acme",
    actor_id="loader",
    resource_type="service",
    resource_id="daily_import",
    detail={"source": "orders.csv"},
)
get_audit_logger("acme").flush(timeout_s=2.0)
```

`emit` requires keyword arguments `category`, `action`, and `organization`. An empty organization is ignored. Optional fields include actor identity, username, IP and user agent, resource type and ID, `super_name`, correlation and session IDs, server, outcome, reason, severity, and detail. A dictionary detail is JSON-encoded; a string is retained. User agents are truncated to 256 characters. Defaults are system actor, successful outcome, and info severity.

`AuditEvent` is an immutable dataclass with an event ID, millisecond timestamp, and process instance ID (`hostname-pid`). The ID is built from a timestamp, counter, and random suffix. Treat it as an opaque string.

## 2. Event coverage in this package

The active call sites emit:

- `data_write` after table writes and compaction; the latter includes `operation="compact"` in detail.
- Role creation, update, and deletion events.
- User creation, update, deletion, and role assignment/removal events.
- Audit configuration changes, legal-hold changes, and retention execution events.

The many names in `Actions` are available vocabulary; defining an action does not install an event producer. `DataReader` does not emit read/query audit events, and staging and mirroring do not emit their corresponding action names automatically in this package. Write and RBAC helpers mostly leave the actor at its default and put role information in detail when provided.

`AuditMiddleware(app, server="api")` can be installed by a Starlette-compatible application. It emits authentication failure for HTTP 401, access denial for 403, and critical system events for exceptions and HTTP 5xx responses. It skips health, favicon, and static paths. It does not audit every successful request. `audit_context(request)` extracts session and request fields for application-level emitters. The current checkout does not include an application that installs this middleware.

## 3. Configuration and live changes

Defaults below are from [settings.py](../supertable/config/settings.py). Constructing `AuditConfig()` directly has different defaults for `hash_chain`, `log_queries`, `log_reads`, and `siem_enabled` (all false); `get_audit_logger` uses settings-based configuration.

| Environment variable | Default | Current use |
| --- | --- | --- |
| `SUPERTABLE_AUDIT_ENABLED` | `false` | Enables logger creation. |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | `1000` | Maximum normal worker batch. |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | `60` | Worker queue-wait setting, capped at five seconds. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | `100000` | Approximate stream length limit on `XADD`. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | `24` | Loaded into logger configuration; the logger does not schedule trimming from it. |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | `true` | Adds a per-instance batch hash chain. |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | `true` | Configurable flag; no core read/query audit producer consumes it. |
| `SUPERTABLE_AUDIT_LOG_READS` | `true` | Configurable flag; no core read/query audit producer consumes it. |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | empty | POST critical events to this URL. |
| `SUPERTABLE_AUDIT_FERNET_KEY` | empty | Key for explicit field-encryption helpers. |
| `SUPERTABLE_AUDIT_RETENTION_DAYS` | `2555` | Cutoff used by manual retention enforcement. |
| `SUPERTABLE_AUDIT_LEGAL_HOLD` | `false` | Fallback legal-hold state when no Redis override is available. |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | `true` | Configurable flag; consumer helpers do not enforce it. |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | `10` | Loaded setting; consumer helpers do not enforce this limit. |

[admin.py](../supertable/audit/admin.py) provides `get_audit_config(org)` and `set_audit_config(org, *, enabled=None, log_queries=None, log_reads=None, hash_chain=None, siem_enabled=None, updated_by="")`. Redis overrides are per organization and merged with settings defaults.

The logger caches resolved configuration for 30 seconds. `invalidate_audit_config_cache(org)` clears that resolution cache; `set_audit_config` does not call it. An already-running logger retains its original configuration for fields other than the enable/disable decision. Use a controlled `shutdown_all()` and subsequent logger creation to load a complete changed configuration. Disabling an organization stops its existing logger when `get_audit_logger` next observes the change.

These administration, read, export, and consumer functions have no role parameter or internal RBAC gate. They are APIs for trusted application code.

## 4. Delivery and storage

Each organization has a cached logger with a daemon worker and a queue of at most 10,000 events. A full queue drops the new event. The worker takes an event as soon as it is available, drains already queued events for up to 50 ms, and writes a batch. The flush interval is not a periodic archival schedule. `flush` drains queued events but is not a barrier for a batch already taken by the worker. `shutdown_all()` stops loggers and attempts a final flush; it is not automatically registered with `atexit` by the audit package.

Redis uses the organization's system audit stream, produced by `redis_keys.audit_stream(org)`, and creates the internal consumer group `__archival__` starting at `0`. Each event field is stored as a string. Writes use `XADD` with approximate `MAXLEN`; no `EXPIRE` is applied to the stream. The group name does not imply an archival worker: the logger writes Parquet directly, and no consumer loop for this group is implemented here.

Parquet batches use the configured storage backend and a fixed event schema, with Snappy compression:

```text
<organization>/__audit__/year=YYYY/month=MM/day=DD/
  audit_<timestamp>_<instance>_<random>.parquet
```

Partitions follow the UTC time of batch writing, not each event's timestamp. Redis failure does not prevent a Parquet attempt, and Parquet failure does not reverse the Redis write. Failed batches are not requeued. `total_written` counts Redis results only; `total_dropped` includes events absent from those results even if Parquet succeeded. The Parquet writer logs a failed `write_bytes` but still returns its intended path and byte count, so that return value is not a persistence acknowledgement.

Critical events additionally start a daemon thread that sends an HTTP POST to the configured webhook. This requires `httpx`, uses a ten-second timeout with a five-second connection timeout, and has no retry queue.

## 5. Read and export

```python
import time
from supertable.audit.reader import query_audit_log
from supertable.audit.export import export_events

now_ms = int(time.time() * 1000)
events = query_audit_log(
    "acme",
    start_ms=now_ms - 3600_000,
    end_ms=now_ms,
    category="data_mutation",
    limit=500,
    source="redis",
)
json_lines = export_events(events, output_format="json")
```

`query_audit_log` accepts `start_ms`, `end_ms`, `category`, `action`, `actor_id`, `resource_type`, `resource_id`, `outcome`, `severity`, `correlation_id`, `limit=500`, and `source="auto"`. Results are sorted newest first after filtering. Source choices are `redis`, `parquet`, and automatic selection.

Automatic selection uses a fixed 24-hour boundary. A missing or recent start time queries Redis only. Older requests read Parquet first and then use remaining capacity for recent Redis events, deduplicating by event ID. It does not fall back to Parquet when a recent Redis query fails.

Source reads are limited before most filters run, so filtered results can contain fewer than `limit` records even when more matches exist. Parquet scanning proceeds from the requested start day forward and stops after at most 367 calendar days or the raw event limit. Always supply a start time for Parquet reads: without it, scanning begins at the Unix epoch. Partition selection by query dates can also miss events written into a later partition than their event timestamp.

`export_events(events, output_format="json")` returns UTF-8 JSON Lines; `"csv"` returns CSV with columns taken from the first event. Other format strings also use JSON Lines. `export_dora_incident_report` is a bounded time-range event export; its `incident_id` is not used to filter the results. `export_soc2_evidence` filters by a built-in criteria mapping and uses a 50,000-event query limit. These functions produce event extracts, with the same query limits as the reader.

## 6. Integrity verification limits

[chain.py](../supertable/audit/chain.py) defines SHA-256 event, batch, and chain helpers. Event hashes exclude `chain_hash` and `instance_id`. The logger combines sorted event hashes into a content hash, combines that with sorted event IDs into a batch hash, and advances the process chain. Chain heads and batch counts are stored in Redis per organization and sanitized instance ID. Every event in a batch receives the same chain hash.

`verify_chain_integrity(organization, date)` reads Parquet batches for `YYYY-MM-DD` or `YYYYMMDD`. Its current implementation is not consistent with the logger: it reconstructs each batch hash using event IDs and an empty content hash. Multiple normally content-hashed batches can therefore be reported as invalid. It also accepts the first batch hash as an anchor, skips empty chain hashes, and returns `valid=True` when no data is found. It does not authenticate event contents or establish that all events were retained.

`MerkleProof`, `save_chain_proof`, and `verify_merkle_proof` are lower-level helpers. The root is a SHA-256 over sorted instance heads. The proof verifier recomputes the supplied proof's root; it does not bind those heads to the events read for a day. No daily proof-producing job is present in this package. Neither a successful proof check nor the high-level `valid` field establishes complete, tamper-proof audit history.

## 7. Retention, consumers, and encryption

`enforce_retention(organization)` is an explicit call. It skips deletion when legal hold is active, treats nonpositive retention days as disabled, and otherwise enumerates dated Parquet partitions older than the cutoff. It calls `storage.delete(partition_path)`, not `delete_tree`; complete cleanup depends on that backend's deletion behavior. It does not delete chain proofs or trim the Redis stream, and no retention scheduler is included.

`set_legal_hold(enabled, organization="")` stores the override in Redis and returns a result dictionary. Organization defaults to configured `SUPERTABLE_ORGANIZATION` when omitted. Redis hold values take precedence; failed lookups fall back to the settings value, and only failure to obtain both falls back to holding data.

`RedisAuditWriter.trim_acknowledged(ttl_hours=24)` explicitly trims stream IDs older than the cutoff. It skips trimming for a group only when that group has both pending entries and positive lag. This is not a comprehensive acknowledgement guarantee, and approximate `MAXLEN` trimming on writes applies independently. No code schedules this method automatically.

`create_consumer(org, group_name, start_from="$")`, `list_consumers(org)`, and `delete_consumer(org, group_name)` wrap Redis consumer-group administration. `$` starts at new events; `"0"` includes retained history. The internal `__archival__` group cannot be deleted by these helpers. External consumers must implement their own stream reading, acknowledgement, and recovery; the helpers provide no delivery loop.

[crypto.py](../supertable/audit/crypto.py) exposes `encrypt_field`, `decrypt_field`, and `is_encryption_available`. Encryption is opt-in per caller; the logger does not encrypt arbitrary event fields. A missing/invalid key, missing `cryptography` dependency, or encryption failure returns plaintext. Decryption failure returns the input unchanged. The key is loaded once per process. No core query emitter invokes these helpers automatically.
