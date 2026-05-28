# Ingestion

## Business Context

SuperTable's ingestion subsystem provides a structured, auditable workflow for loading data into the platform. Rather than writing directly to tables, users upload files into **staging areas** -- temporary holding zones where data can be previewed, validated, and reviewed before committing to permanent storage. For automated, repeatable data flows, **pipes** define transformation and routing rules that map staged files to target tables.

This two-tier design (staging + pipes) separates the concerns of data arrival from data commitment, giving teams control over quality gates, schema validation, and approval workflows before data enters the production data lake.

---

## Module Locations

| Module | Purpose |
|---|---|
| `supertable/staging_area.py` | `Staging` class -- physical stage lifecycle (create, upload, list, delete) |
| `supertable/super_pipe.py` | `SuperPipe` class -- pipe definition management (CRUD in Redis) |

---

## Staging Areas

A staging area is a named, temporary folder within a SuperTable's storage namespace where files are uploaded before being committed to a target SimpleTable.

### Physical Layout

```
{organization}/{super_name}/staging/{staging_name}/          # uploaded files
{organization}/{super_name}/staging/{staging_name}_files.json # flat file index
```

### Redis Metadata

```
supertable:{org}:lakes:{sup}:meta:staging:doc:{staging_name}:meta   # per-stage metadata (STRING)
supertable:{org}:lakes:{sup}:meta:staging:index                     # index set of all staging names (SET)
```

Built by `redis_keys.staging_doc(org, sup, staging_name)` and
`redis_keys.staging_index(org, sup)`.

### Staging Class

```python
class Staging:
    def __init__(
        self,
        *,
        organization: str,
        super_name: Optional[str] = None,
        super_table: Any = None,       # backward-compat alternative to super_name
        staging_name: Optional[str] = None,
    )
```

The `Staging` class supports two modes of operation:

**Manager mode** (when `staging_name` is not provided):
- Used for inspecting the staging area structure and listing all stages.
- Call `.open(staging_name)` to get a stage-mode instance.
- Call `.get_directory_structure(role_name)` to retrieve a full inventory of all stages with file counts and Redis metadata.

**Stage mode** (when `staging_name` is provided):
- Used for file operations within a specific stage.
- On construction, the stage directory is created (if it does not exist), the stage is registered in Redis, and the file index JSON is initialized.

### Staging Lifecycle

```
create --> upload files --> review --> commit to table / discard
```

#### 1. Create

Instantiating `Staging(organization=..., super_name=..., staging_name="my_stage")` triggers `_init_stage()` under a Redis lock:

```python
def _init_stage(self) -> None:
    # 1. Create physical folder
    if not self.storage.exists(self.stage_dir):
        self.storage.makedirs(self.stage_dir)

    # 2. Register in Redis
    self.catalog.upsert_staging_meta(org, sup, staging_name,
        meta={"path": self.stage_dir, "created_at_ms": int(time.time() * 1000)})

    # 3. Initialize file index
    if not self.storage.exists(self.files_index_path):
        self.storage.write_json(self.files_index_path, [])
```

#### 2. Upload Files

```python
def save_as_parquet(
    self,
    *,
    role_name: str,
    arrow_table: pa.Table,
    base_file_name: str,
    source: str = "upload",
    duration_ms: float = 0,
    pipe_name: str = "",
    pipe_id: str = "",
) -> str
```

Writes a PyArrow table as a Parquet file into the stage directory. The file is named with a nanosecond timestamp suffix to avoid collisions: `{clean_name}_{timestamp_ns}.parquet`. After writing, the file index JSON is updated with metadata:

```json
{
  "file": "orders_1713192000000000000.parquet",
  "written_at_ns": 1713192000000000000,
  "rows": 50000,
  "source": "upload",
  "duration_ms": 1234,
  "pipe_name": null,
  "pipe_id": null,
  "status": "ok"
}
```

All operations are protected by a Redis lock on the stage (30-second TTL).

#### 3. Review

```python
def list_files(self, role_name: str) -> List[str]
```

Returns a list of file names from the file index. Requires meta-read access (RBAC).

The manager-mode method `get_directory_structure(role_name)` provides a comprehensive view across all stages:

```python
{
    "organization": "acme",
    "super_name": "analytics",
    "base_staging_dir": "acme/analytics/staging",
    "base_exists": True,
    "stages": [
        {
            "name": "q1_import",
            "path": "acme/analytics/staging/q1_import",
            "exists": True,
            "files_index_path": "acme/analytics/staging/q1_import_files.json",
            "files_index_exists": True,
            "file_count": 3,
            "files": ["orders_1713192000.parquet", ...],
            "redis_meta": {"path": "...", "created_at_ms": 1713192000000}
        }
    ],
    "stage_count": 1
}
```

#### 4. Commit

Committing staged data to a target table is handled by the API layer. The staging area provides the files; the `DataWriter.write()` method handles the actual table write with full overlap detection, dedup, and catalog update.

#### 5. Discard

```python
def delete(self, role_name: str) -> None
```

Deletes the entire stage: removes the physical directory (recursive), the file index JSON, and all Redis metadata. Runs under a Redis lock.

### Locking

All mutating stage operations (`_init_stage`, `save_as_parquet`, `delete`) are wrapped in `_with_lock()`:

```python
def _with_lock(self, fn):
    lock_key = f"supertable:{org}:{sup}:lock:stage:{staging_name}"
    token = uuid.uuid4().hex
    acquired = self.catalog.r.set(lock_key, token, nx=True, ex=30)
    # ... execute fn() ...
    # Release via Lua compare-and-delete script
```

The lock uses a 30-second TTL with a Lua-script-based atomic release (compare token, then delete).

---

## Pipes

Pipes are named, persistent configurations that define how staged data should be routed to target SimpleTables. They are stored entirely in Redis (no filesystem artifacts).

### SuperPipe Class

```python
class SuperPipe:
    def __init__(
        self,
        *,
        organization: str,
        super_name: str,
        staging_name: str,
    )
```

On construction, `SuperPipe` validates that the referenced staging area exists in Redis. All pipe operations are protected by the same staging-level Redis lock used by `Staging._with_lock()` (10-second TTL for pipe operations).

### Pipe Definition

A pipe definition stored in Redis contains:

```json
{
  "staging_name": "daily_import",
  "pipe_name": "orders_pipe",
  "user_hash": "abc123",
  "simple_name": "orders",
  "overwrite_columns": ["order_id"],
  "transformation": [],
  "updated_at_ns": 1713192000000000000,
  "enabled": true
}
```

### CRUD Operations

#### Create

```python
def create(
    self,
    *,
    role_name: str,
    pipe_name: str,
    simple_name: str,
    user_hash: str,
    overwrite_columns: List[str] = None,
    enabled: bool = True,
) -> str
```

Creates a new pipe definition. Before saving, it checks for semantic duplicates: if another pipe already targets the same `simple_name` with the same `overwrite_columns`, a `ValueError` is raised. Returns a URI string: `redis://{org}/{sup}/{staging}/{pipe_name}`.

#### Read

```python
def read(self, pipe_name: str, role_name: str) -> Dict[str, Any]
```

Returns the full pipe metadata from Redis. Raises `FileNotFoundError` if the pipe does not exist.

#### Enable/Disable

```python
def set_enabled(self, pipe_name: str, enabled: bool, role_name: str) -> None
```

Updates the `enabled` flag and `updated_at_ns` timestamp.

#### Delete

```python
def delete(self, pipe_name: str, role_name: str) -> bool
```

Removes the pipe from Redis. Returns `True` if the pipe existed and was deleted.

### Redis Keys for Pipes

```
supertable:{org}:lakes:{sup}:meta:staging:doc:{staging_name}:pipes:doc:{pipe_name}   # pipe definition (STRING)
supertable:{org}:lakes:{sup}:meta:staging:doc:{staging_name}:pipes:index             # index set of pipe names (SET)
```

---

## File Formats

SuperTable ingestion accepts data as PyArrow tables. `Staging.save_as_parquet()` writes files in Parquet format directly. Callers that start from another format (CSV, JSON, Excel) must convert to PyArrow before invoking the staging API.

All staged files are written as Parquet with the storage backend's default settings. The final commit to a target table goes through `DataWriter`, which applies zstd compression, dictionary encoding, and optimized row-group sizing (122,880 rows).

---

## Data Flow Summary

```
User/API                  Staging Area              Pipe Config           DataWriter
   |                          |                         |                    |
   |-- upload file ---------->|                         |                    |
   |   (CSV/JSON/Parquet)     |                         |                    |
   |                          |-- save_as_parquet() --->|                    |
   |                          |   (Parquet in stage)    |                    |
   |                          |                         |                    |
   |-- review files --------->|                         |                    |
   |   (list_files)           |                         |                    |
   |                          |                         |                    |
   |-- commit --------------->|-- read staged files --->|                    |
   |   (with pipe config)     |                         |-- resolve target ->|
   |                          |                         |   simple_name      |
   |                          |                         |   overwrite_cols   |
   |                          |                         |                    |
   |                          |                         |                    |-- write()
   |                          |                         |                    |   (full pipeline)
   |                          |                         |                    |
   |-- discard (optional) --->|                         |                    |
   |   (delete stage)         |-- delete() ------------>|                    |
```

---

## RBAC Integration

All staging and pipe operations enforce role-based access control:

- **Write operations** (`save_as_parquet`, `delete`, `create pipe`, `delete pipe`, `set_enabled`): require `check_write_access()`.
- **Read operations** (`list_files`, `get_directory_structure`, `read pipe`): require `check_meta_access()`.

Both checks verify the role has appropriate permissions on the SuperTable and target table.
