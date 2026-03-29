# Data Island Core — Mirroring

## Overview

Mirroring automatically exports table data to standard open formats after every write — Delta Lake, Apache Iceberg, or plain Parquet. When mirroring is enabled for a SuperTable, each successful `DataWriter.write()` triggers a mirror operation that writes the table's current state to a parallel directory structure in the chosen format. External tools (Databricks, Snowflake, Trino, Athena) can read the mirrored data directly without going through Data Island Core.

Mirroring is optional and disabled by default. It is enabled per-SuperTable, not per-table — all tables in a SuperTable are mirrored in the same formats.

---

## How it works

### Write integration

At the end of the `DataWriter.write()` pipeline, after the Redis pointer is updated and the lock is released:

```python
MirrorFormats.mirror_if_enabled(
    super_table=self.super_table,
    table_name=simple_name,
    simple_snapshot=new_snapshot_dict,
)
```

This checks which mirror formats are enabled (stored as a Redis set), then calls each enabled mirror's write method with the current snapshot.

### Mirror output structure

Each format writes to a separate directory under the SuperTable's storage path:

```
{org}/{super_name}/
  tables/{simple_name}/data/...        Original Parquet files
  __mirror__/
    parquet/{simple_name}/...           Parquet mirror (latest-only)
    delta/{simple_name}/...             Delta Lake table
    iceberg/{simple_name}/...           Iceberg table
```

### Supported formats

**Parquet mirror** (`mirror_parquet.py`, 184 lines) — Writes a single consolidated Parquet file containing the latest table data. The simplest mirror format — useful for tools that read plain Parquet files from S3/MinIO.

**Delta Lake** (`mirror_delta.py`, 641 lines) — Writes a Delta Lake table with transaction log (`_delta_log/`), checkpoints, and schema metadata. Supports schema evolution and is compatible with Spark, Databricks, and any Delta-compatible reader.

**Apache Iceberg** (`mirror_iceberg.py`, 791 lines) — Writes an Iceberg table with metadata files, manifest lists, and manifests. Compatible with Trino, Spark, Athena, Snowflake, and other Iceberg readers.

---

## Enabling mirroring

### Via the WebUI

Navigate to the Admin page → Mirror Configuration. Toggle formats on/off.

### Via the API

```
# Enable Delta Lake mirroring
POST /reflection/mirror/enable
{ "org": "acme", "sup": "example", "format": "delta" }

# Disable Delta Lake mirroring
POST /reflection/mirror/disable
{ "org": "acme", "sup": "example", "format": "delta" }

# Set all formats at once
POST /reflection/mirror/set
{ "org": "acme", "sup": "example", "formats": ["delta", "iceberg"] }
```

### Via Python

```python
from supertable.mirroring.mirror_formats import MirrorFormats

MirrorFormats.enable_with_lock(super_table, "delta")
MirrorFormats.disable_with_lock(super_table, "iceberg")
enabled = MirrorFormats.get_enabled(super_table)  # ["delta"]
```

---

## Configuration

Mirror formats are stored in Redis:

```
Key:   supertable:{org}:{sup}:meta:mirrors
Type:  Set
Members: "delta", "iceberg", "parquet" (whichever are enabled)
```

No additional environment variables are required. Mirrors use the same storage backend as the primary data.

---

## Module structure

```
supertable/mirroring/
  __init__.py            Package marker
  mirror_formats.py      FormatMirror enum, MirrorFormats orchestrator (118 lines)
  mirror_parquet.py      Parquet mirror — latest-only export (184 lines)
  mirror_delta.py        Delta Lake mirror — transaction log, checkpoints (641 lines)
  mirror_iceberg.py      Iceberg mirror — metadata, manifests (791 lines)
```

---

## Frequently asked questions

**Does mirroring slow down writes?**
Yes, modestly. Mirroring reads the snapshot's Parquet files and writes them in the new format. The impact is proportional to the data size. Mirroring runs after the write lock is released, so it doesn't block other writes to the same table.

**What happens if mirroring fails?**
The primary write succeeds regardless — mirroring failures are logged but do not roll back the data write. The next write will attempt mirroring again with the updated snapshot.

**Can I mirror to a different storage backend?**
No. Mirrors are written to the same storage backend as the primary data. If you need cross-cloud mirroring, use an external sync tool on the mirror output.

**Are mirrors incremental?**
The current implementation writes the full table state on each mirror operation. True incremental mirroring (append-only Delta log entries) is a future enhancement.
