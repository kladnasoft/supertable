# Data Island Core — Ingestion & Pipes

## Overview

Ingestion is how data enters Data Island Core. Files are uploaded to a staging area (a temporary holding zone on object storage), then moved into a table by a pipe — an automated rule that maps staging files to a target table with specified key columns and write semantics. The staging-and-pipe model separates "upload" from "commit," giving operators control over validation before data lands in production tables.

For simpler workflows, the API also supports direct-to-table uploads that bypass staging entirely.

---

## How it works

### The ingestion flow

1. **Create a staging area** — A named directory on object storage under `{org}/{super_name}/staging/{staging_name}/`. Metadata is stored in Redis.

2. **Upload files** — CSV, JSON, or Parquet files are uploaded to the staging area via the API. Each file is converted to Parquet on upload. A file index tracks what's in the staging area.

3. **Create a pipe** — A pipe defines the mapping: which staging area, which target table, which columns are overwrite keys, and whether the pipe is enabled. Pipe definitions are stored in Redis.

4. **Execute the pipe** — When triggered, the pipe reads all Parquet files from the staging area, writes them to the target table using `DataWriter.write()`, and respects the configured overwrite columns for merge/upsert semantics.

### Direct-to-table upload

The upload endpoint also supports a `mode=table` parameter that writes directly to a table without going through staging. The file is converted to Parquet, written via `DataWriter`, and no staging area is created. This is the fast path for one-shot data loads.

---

## Staging areas

### What a staging area is

A staging area is a named directory on object storage where files are accumulated before being committed to a table. It provides:

- A holding zone for validation and preview before data enters a table
- Accumulation of multiple file uploads into a single logical batch
- Isolation — staging files don't affect production tables until a pipe commits them

### Storage layout

```
{org}/{super_name}/staging/
  {staging_name}/
    {uuid}.parquet              Uploaded file (converted to Parquet)
    {uuid}.parquet
  {staging_name}_files.json     File index (list of uploaded files with metadata)
```

### Redis metadata

Each staging area has two Redis entries:

- `supertable:{org}:{sup}:meta:staging:{staging_name}` — Hash with staging metadata (name, created timestamp, file count)
- `supertable:{org}:{sup}:meta:staging:meta` — Set of all staging names for fast listing

### Staging operations

| Operation | API endpoint | Description |
|---|---|---|
| Create | `POST /reflection/staging/create` | Creates a staging area (storage dir + Redis entry) |
| List | `GET /reflection/ingestion/stagings` | Lists all staging areas with metadata |
| List files | `GET /reflection/ingestion/staging/files` | Lists files in a specific staging area |
| Delete | `POST /reflection/staging/delete` | Deletes a staging area, its files, and all attached pipes |
| Upload | `POST /reflection/ingestion/load/upload` | Uploads a file to a staging area (or directly to a table) |

### Locking

Staging area mutations (create, delete, upload) acquire a per-staging Redis lock (`supertable:{org}:{sup}:lock:stage:{staging_name}`) to prevent concurrent modifications. The lock has a 10-second TTL and uses the same compare-and-delete Lua script as table locks.

---

## Pipes

### What a pipe is

A pipe is a rule that connects a staging area to a target table. It defines:

- **Source:** which staging area to read from
- **Target:** which SimpleTable to write to
- **Overwrite columns:** key columns for merge/upsert semantics (empty = append-only)
- **Enabled/disabled:** whether the pipe is active

Pipe definitions are stored entirely in Redis — no files on storage.

### Pipe configuration

```json
{
  "staging_name": "raw_orders",
  "pipe_name": "orders_daily",
  "user_hash": "0b85b786b16d195...",
  "simple_name": "orders",
  "overwrite_columns": ["order_id"],
  "transformation": [],
  "updated_at_ns": 1711734000000000000,
  "enabled": true
}
```

- `overwrite_columns`: when set, the pipe performs an upsert — incoming rows with matching keys replace existing rows. When empty, new files are appended.
- `transformation`: reserved for future use (planned: SQL transforms between staging and target).
- `user_hash`: the user identity that created the pipe, used for RBAC when the pipe executes.

### Pipe operations

| Operation | API endpoint | Description |
|---|---|---|
| Create | `POST /reflection/pipes/save` | Creates or updates a pipe definition |
| Read | `GET /reflection/ingestion/pipe/meta` | Gets a single pipe's configuration |
| List | `GET /reflection/ingestion/pipes` | Lists all pipes across all staging areas |
| Enable | `POST /reflection/pipes/enable` | Enables a pipe |
| Disable | `POST /reflection/pipes/disable` | Disables a pipe |
| Delete | `POST /reflection/pipes/delete` | Deletes a pipe definition |

### Duplicate prevention

When creating a pipe, `SuperPipe` checks for existing pipes with the same `simple_name` + `overwrite_columns` combination. If a duplicate exists under a different pipe name, creation is rejected. This prevents conflicting write rules to the same table.

---

## File upload

The upload endpoint (`POST /reflection/ingestion/load/upload`) handles file conversion and routing:

### Supported formats

- **CSV** — auto-detected delimiter, encoding, and column types
- **JSON** — array of objects, one object per row
- **Parquet** — passed through directly (already in the target format)

### Upload modes

**Staging mode** (`mode=staging`): The file is converted to Parquet and saved in the staging area directory. The file index is updated. No table is modified.

**Table mode** (`mode=table`): The file is converted to Parquet and written directly to the target table via `DataWriter.write()`. No staging area is involved. The `overwrite_columns` parameter controls merge semantics.

### Upload flow

1. Receive multipart file upload
2. Detect file type (CSV, JSON, Parquet) from extension or content
3. Convert to PyArrow table (CSV: `pyarrow.csv.read_csv`, JSON: `pyarrow.json.read_json`)
4. If staging mode: write Parquet to staging directory, update file index
5. If table mode: call `DataWriter.write()` with the specified parameters
6. Return result (rows inserted, file path, metadata)

---

## Services layer

`services/ingestion.py` provides shared helper functions used by both the API and WebUI for staging and pipe management. It handles:

- Redis metadata read/write with JSON serialization
- Storage directory scanning for staging name discovery
- Pipe index loading and caching
- Cascade deletion (deleting a staging area removes all its pipes)
- Backward compatibility with file-based indexes

The helpers create a consistent abstraction over the dual storage model (Redis metadata + object storage files).

---

## Configuration

| Setting | Description |
|---|---|
| `STORAGE_TYPE` | Determines where staging files are stored |
| `DEFAULT_LOCK_DURATION_SEC` | Staging lock TTL (10 seconds hardcoded in SuperPipe) |
| `MAX_MEMORY_CHUNK_SIZE` | Maximum file size for in-memory processing during upload |

---

## Module structure

```
supertable/
  staging_area.py            Staging class — create, upload, list, delete (287 lines)
  super_pipe.py              SuperPipe class — pipe CRUD with locking (147 lines)
  services/ingestion.py      Shared helpers for staging/pipe operations (394 lines)
  api/api.py                 Ingestion API endpoints (staging CRUD, pipe CRUD, upload)
```

---

## Frequently asked questions

**Can I upload directly to a table without staging?**
Yes. Use `mode=table` on the upload endpoint. The file is converted and written directly via `DataWriter`.

**What happens when a pipe executes?**
The pipe reads all Parquet files from its staging area and writes them to the target table using `DataWriter.write()` with the pipe's configured overwrite columns. The write respects RBAC — the pipe's `user_hash` determines access.

**Can a staging area have multiple pipes?**
Yes. Each pipe targets a different table (or the same table with different overwrite columns). This allows fan-out from a single upload to multiple destination tables.

**What happens if I delete a staging area that has pipes?**
All pipes attached to the staging area are deleted automatically (cascade delete). Both the storage files and Redis metadata are removed.

**Are uploaded files validated before writing?**
The upload endpoint performs basic validation (file type detection, schema extraction). Column type inference is handled by PyArrow's CSV/JSON readers. No custom validation rules exist yet — all data that PyArrow can parse is accepted.

**Can I transform data between staging and table?**
Not yet. The `transformation` field in pipe definitions is reserved for future SQL-based transforms. Currently, data flows from staging to table without modification.

**What happens to staging files after a pipe executes?**
Staging files remain in place after pipe execution. They are not automatically deleted. Use the staging delete endpoint to clean up after a successful ingest.
