# 13. Format Mirroring

## Overview

SuperTable stores all data natively as Parquet files managed by its own snapshot-based catalog. Format mirroring creates **secondary projections** of the same data in Delta Lake, Apache Iceberg, or plain Parquet layouts -- allowing external tools (Spark, Trino, Databricks, dbt) to read SuperTable data through their native connectors without any SuperTable-specific integration.

Mirroring is triggered automatically after every successful write. It is a **latest-only** projection: only the current snapshot is mirrored, not the full version history.

## Supported Formats

| Format | Directory Layout | Transaction Log | Spec Compliance |
|--------|-----------------|-----------------|-----------------|
| **Delta Lake** | `<org>/<super>/delta/<table>/` | `_delta_log/` with JSON commits | Full Delta spec (commitInfo, protocol, metaData, add, remove) |
| **Iceberg** | `<org>/<super>/iceberg/<table>/` | `metadata/` + `manifests/` JSON | Iceberg-lite (V2 metadata JSON, JSON manifest lists) + standard V2 Avro writer |
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
MirrorFormats.mirror_if_enabled(super_table, table_name, simple_snapshot, mirrors=None)
```

This function:
1. Reads enabled formats from Redis (or uses the provided `mirrors` list).
2. Ensures the required directory structure exists for each format.
3. Delegates to the per-format writer: `write_delta_table()`, `write_iceberg_table()`, or `write_parquet_table()`.

The caller should still hold the per-table lock to prevent concurrent mirroring of the same table.

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
- The engine string is set to an Apache Spark-compatible value for maximum tool compatibility.
- Schema normalization maps SuperTable/Arrow/Polars types to Spark SQL types (e.g., `Datetime(time_unit='us')` becomes `timestamp`, `int64` becomes `long`).
- The writer uses PyArrow to infer schemas from Parquet file headers when the snapshot metadata lacks schema information.
- A commit is **skipped** if there are no data changes (no adds and no removes).
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

The Iceberg writer provides two implementations:

### Iceberg-Lite (JSON)

A lightweight mirror that writes:
- `metadata/<version>.json` -- Minimal Iceberg V2 table metadata with schema, partition spec, and a single current snapshot.
- `manifests/<version>.json` -- JSON manifest listing data file paths and sizes.
- `latest.json` -- Convenience pointer to the current metadata and manifest files.

Key details:
- Uses a **stable UUID** derived from `uuid5(NAMESPACE_URL, "st://{org}/{super}/{table}")` so the table UUID is deterministic.
- Only the current snapshot is written (no snapshot history).
- Schema fields are mapped from the SuperTable catalog schema.

### Standard Iceberg V2 (Avro)

A full-compliance writer that produces:
- Iceberg V2 `metadata/vN.metadata.json`
- Binary Avro manifest lists and manifest files
- `metadata/version-hint.text` pointing to the current version

This writer follows the official Iceberg specification for manifest list fields, manifest entry fields, and table metadata fields.

## Parquet Mirror

The simplest mirror -- copies current snapshot Parquet files into a flat directory:

```
<org>/<super>/parquet/<table>/files/
```

Behavior:
- Files are copied using the most efficient method available: MinIO server-side `copy_object`, storage backend `copy()`, or byte-level read/write fallback.
- File names are prefixed with an MD5 hash of the source path to avoid collisions.
- Copy is skipped if the destination file already exists from a prior mirror run.
- Previously co-located files not in the current snapshot are **deleted**.
- No transaction log or JSON metadata is written.
- No-op if there are no file changes.

### Copy Strategy Priority

1. **MinIO server-side copy** -- Uses `CopySource` for zero-download, zero-upload copy within the same bucket.
2. **Storage backend copy** -- Uses `storage.copy()` if available.
3. **Byte copy fallback** -- Downloads via `read_bytes()` and uploads via `write_bytes()`.

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
- `supertable/mirroring/mirror_iceberg.py` -- Iceberg-lite JSON writer and standard V2 Avro writer.
- `supertable/mirroring/mirror_parquet.py` -- Plain Parquet directory mirror with efficient copy strategies.
