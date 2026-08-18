# 13. Format Mirroring

## Overview

SuperTable stores all data natively as Parquet files managed by its own snapshot-based catalog. Format mirroring creates **secondary projections** of the same data in Delta Lake, Apache Iceberg, or plain Parquet layouts -- allowing external tools (Spark, Trino, Databricks, dbt) to read SuperTable data through their native connectors without any SuperTable-specific integration.

Mirroring is triggered automatically after every successful write. It is a **latest-only** projection: only the current snapshot is mirrored, not the full version history.

## Supported Formats

| Format | Directory Layout | Transaction Log | Spec Compliance |
|--------|-----------------|-----------------|-----------------|
| **Delta Lake** | `<org>/<super>/delta/<table>/` | `_delta_log/` with JSON commits | Full Delta spec (commitInfo, protocol, metaData, add, remove) |
| **Iceberg** | `<org>/<super>/iceberg/<table>/` | V2 metadata JSON + Avro manifests | Standard Iceberg V2 |
| **Parquet** | `<org>/<super>/parquet/<table>/files/` | None | Plain directory of Parquet files |

## Architecture

### Configuration

Mirror formats are configured per-SuperTable and stored in Redis:

```
Redis key: supertable:{org}:lakes:{super}:meta:mirrors
Value:     {"formats": ["DELTA", "ICEBERG", "PARQUET"], "ts": <epoch_ms>}
```

Built by `redis_keys.meta_mirrors(org, sup)`. See
[16 Redis Key Layout](16_redis_layout.md) for the full hierarchy.

The `MirrorFormats` class provides the configuration API:

```python
MirrorFormats.get_enabled(super_table) -> List[str]
MirrorFormats.set_with_lock(super_table, ["DELTA", "ICEBERG"])
MirrorFormats.enable_with_lock(super_table, "PARQUET")
MirrorFormats.disable_with_lock(super_table, "DELTA")
```

Format names are normalized to uppercase and deduplicated.

### Write-Time Dispatch

After every successful snapshot update, the caller invokes:

```python
MirrorFormats.mirror_if_enabled(
    super_table, table_name, simple_snapshot,
    mirrors=None, commit_id=commit_id, snapshot_path=snapshot_path,
    verify=True,
)
```

This function:
1. Reads enabled formats from Redis (or uses the provided `mirrors` list).
2. Ensures the required directory structure exists for each format.
3. Delegates to the per-format writer: `write_delta_table()`, `write_iceberg_table()`, or `write_parquet_table()`.
4. Verifies that each projection contains the exact committed snapshot before publication is acknowledged.

The caller should still hold the per-table lock to prevent concurrent mirroring of the same table.

The core snapshot, mirror formats, and active publisher are also recorded in a
durable Redis publication intent. Recovery requires a complete committed leaf
payload and proves that the JSON object at the recorded path has the exact same
canonical content before starting a mirror writer. It never trusts the mutable
path alone and never replays the original data write.

Only allowlisted states that are provably outside mirror I/O are marked
quiescent automatically: a core commit that failed before mirroring, or an
outbox-completion failure after all mirror I/O and verification succeeded.
Generic mirror/recovery storage errors remain ambiguous and retain their
original owner indefinitely, as do abandoned `prepared` or `core_committed`
publications. Neither a returned storage error nor lease expiry proves that a
remote request cannot later become visible. After externally stopping that
exact process and accounting for in-flight storage requests, an operator may
make the bounded takeover explicit:

```python
state = catalog.get_mirror_publication(org, super_name, table_name)
MirrorFormats.reconcile_publication(
    super_table,
    table_name,
    expected_commit_id=state["commit_id"],
    expected_previous_owner=state["publication_owner"],
    confirm_previous_owner_stopped=True,
)
```

The claim checks the exact commit, previous owner, current table-lock token,
and deletion fences atomically. Completion is fenced by that same durable
owner and live lock. Do not set `confirm_previous_owner_stopped=True` until the
previous process has been terminated and cannot resume its storage call; an
incorrect confirmation defeats the quiescence guarantee.

Deleting a SimpleTable removes all three fixed mirror prefixes (`delta`,
`iceberg`, and `parquet`) together with the core table prefix. The terminal
table-deletion tombstone remains after cleanup, so neither an abandoned mirror
publisher nor a stale data writer can make that path live again. Reuse of the
name requires exact-intent, operator-confirmed deletion recovery.

## Delta Lake Mirror

The Delta writer produces spec-compliant `_delta_log` entries with the following actions per commit:

| Action | Purpose |
|--------|---------|
| `commitInfo` | Commit metadata (timestamp, operation, engine string) |
| `protocol` | Min reader/writer version |
| `metaData` | Table schema (Spark StructType JSON), format, partition columns |
| `remove` | Files from the previous mirror no longer in the current snapshot |
| `add` | Files in the current snapshot |

Key behaviors:
- **Parquet files are physically copied** into the table folder under `delta/<table>/`. Obsolete copies are deleted.
- A newly enabled Delta mirror always bootstraps at `_delta_log/00000000000000000000.json`; mirror-owned versions then advance contiguously, independent of the source snapshot number.
- The stable core commit ID is stored as `commitInfo.txnId`, making recovery retries idempotent.
- Every copied artifact is checked by exact size and SHA-256; the digest is persisted in the Delta `add.tags` entry and revalidated during recovery. Commit JSON is also read back byte-for-byte before obsolete-file cleanup.
- The engine string is set to an Apache Spark-compatible value for maximum tool compatibility.
- Schema normalization maps SuperTable/Arrow/Polars types to Spark SQL types (e.g., `Datetime(time_unit='us')` becomes `timestamp`, `int64` becomes `long`).
- The writer uses PyArrow to infer schemas from Parquet file headers when the snapshot metadata lacks schema information.
- A legacy direct call without a durable commit ID may skip a commit if there are no data changes; durable publication intents still receive an identifiable commit.
- Path normalization handles various storage URL formats (local, S3, ABFSS) to avoid erroneous deletes.

### Schema Type Mapping

| Source Type | Delta/Spark Type |
|------------|-----------------|
| `string`, `varchar`, `text` | `string` |
| `int`, `int32`, `integer` | `integer` |
| `int64`, `long`, `bigint` | `long` |
| `float` | `float` |
| `double` | `double` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `timestamp`, `datetime(...)` | `timestamp` |
| `decimal(p,s)` | `decimal(p,s)` |
| `binary` | `binary` |

## Iceberg Mirror

The Iceberg writer produces:
- Iceberg V2 `metadata/vN.metadata.json`
- Binary Avro manifest lists and manifest files
- `metadata/version-hint.text` pointing to the current version
- `latest.json` as a convenience pointer to the same standard artifacts

Metadata generations are mirror-owned and begin at `v1` regardless of the source snapshot number. Every file and table location is obtained from the storage backend's canonical URI API, preserving configured cloud base prefixes and schemes. The Avro files carry the Iceberg v2 nested field IDs/logical-map schema and an explicit null codec; table metadata includes a default name mapping so pre-Iceberg SuperTable Parquet files without embedded field IDs remain readable. Copy failure aborts publication rather than falling back to source paths.

The writer uses a stable UUID derived from `uuid5(NAMESPACE_URL, "st://{org}/{super}/{table}")`. It records the core commit ID plus data/manifest SHA-256 seals in table properties, so retrying an already-valid commit verifies and returns; an incomplete or corrupt generation is rejected and rebuilt before the publication intent is closed.

## Parquet Mirror

The simplest mirror -- copies current snapshot Parquet files into a flat directory:

```
<org>/<super>/parquet/<table>/files/
```

Behavior:
- Files are copied through the storage adapter's `copy()` API (which may use a provider-native server-side operation) or a byte-level fallback.
- File names are prefixed with an MD5 hash of the source path to avoid collisions.
- An existing destination is reused only after its exact size and SHA-256 match the source; a stale or truncated same-named object is recopied and verified.
- Previously co-located files not in the current snapshot are **deleted**.
- No transaction log or JSON metadata is written.
- No-op if there are no file changes.

### Copy Strategy Priority

1. **Storage backend copy** -- Uses `storage.copy()` so base-prefix translation remains backend-owned.
2. **Byte copy fallback** -- Downloads via `read_bytes()` and uploads via `write_bytes()`.

## Format Selection Guidelines

| Use Case | Recommended Format |
|----------|-------------------|
| Spark / Databricks | Delta Lake |
| Trino / Athena / dbt | Iceberg |
| Simple file access / pandas | Parquet |
| Maximum compatibility | Enable all three |

## Source Files

- `supertable/mirroring/mirror_formats.py` -- Configuration, dispatch, `MirrorFormats` class.
- `supertable/mirroring/mirror_delta.py` -- Delta Lake writer with Spark schema normalization.
- `supertable/mirroring/mirror_iceberg.py` -- Standard Iceberg V2 metadata and Avro writer.
- `supertable/mirroring/mirror_parquet.py` -- Plain Parquet directory mirror with efficient copy strategies.
