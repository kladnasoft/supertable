# Feature / Component Reverse Engineering — Supertable Storage Abstraction Layer

## 1. Executive Summary

This code implements a **multi-backend pluggable storage abstraction layer** for the **Supertable** product. It provides a single, unified `StorageInterface` ABC with concrete implementations for five storage backends: local filesystem, AWS S3, MinIO (S3-compatible), Google Cloud Storage (GCS), and Azure Blob Storage. A factory function (`get_storage`) dynamically selects and instantiates the correct backend at runtime based on environment variables or explicit configuration.

**Main responsibility:** Decouple all Supertable data-persistence operations from any specific storage technology, enabling the product to run identically on a developer laptop (local disk), in any major cloud (AWS, GCP, Azure), or on self-hosted S3-compatible infrastructure (MinIO).

**Likely product capability:** Supertable is a data-platform product (likely a columnar data engine or analytics/BI platform built on DuckDB and Apache Arrow/Parquet). This storage layer is the foundational I/O subsystem that every higher-level feature — table creation, metadata management, query execution, data ingestion — depends on.

**Business purpose:** Cloud-agnostic deployment. Customers can run Supertable wherever their data already lives without vendor lock-in. The layer also supports DuckDB-native path generation and presigned URL generation, indicating direct integration with the product's query engine.

**Technical role:** Infrastructure / platform plumbing. This is a core internal library consumed by every Supertable subsystem that reads or writes persistent state.

---

## 2. Functional Overview

### What can the product do because of this code?

| Capability | Description |
|---|---|
| **Cloud-agnostic deployment** | Supertable can run on local disk, AWS S3, GCS, Azure Blob, or MinIO without code changes above the storage layer. |
| **Unified data I/O** | All subsystems read/write JSON metadata, Parquet data files, raw bytes, and text through a single interface. |
| **DuckDB query-engine integration** | Storage backends generate `s3://`, `gcs://`, `azure://`, or `https://` paths consumable by DuckDB's built-in readers, enabling direct query pushdown to remote storage. |
| **Presigned URL generation** | Enables secure, time-limited sharing of stored objects (e.g., for download links in a UI or API). |
| **Atomic local writes** | The local backend uses `tempfile` + `os.replace()` + `fsync` for crash-safe metadata updates. |
| **Auto-configuration from environment** | Every cloud backend has a `from_env()` factory that reads standard and Supertable-specific environment variables, including support for Azure ABFSS URIs and GCP service-account JSON. |
| **Optional dependency model** | Cloud SDKs are lazy-imported; only the selected backend's dependencies need to be installed (`pip install 'supertable[s3]'`, etc.). |

### Target users / actors

- **Developers** deploying Supertable locally during development (LOCAL backend).
- **Platform operators / DevOps** deploying Supertable in production on AWS, GCP, Azure, or on-prem MinIO.
- **The Supertable engine itself** — every internal module that persists or retrieves data.

### Business value

- **Eliminates cloud vendor lock-in** — a strategic differentiator for enterprise sales.
- **Reduces onboarding friction** — works out of the box on local disk with zero cloud config.
- **Enables hybrid / multi-cloud deployments** — important for regulated industries.
- **Supports the DuckDB-centric query architecture** — the `to_duckdb_path()` methods are critical for query performance (DuckDB can read directly from object stores).

---

## 3. Technical Overview

### Architectural style
- **Strategy pattern** via ABC (`StorageInterface`) + concrete implementations.
- **Factory pattern** via `get_storage()` with lazy imports.
- **Adapter pattern** — each backend adapts a cloud SDK to the common interface.
- **Environment-driven configuration** — all backends support construction from `os.environ`.

### Major modules

| Module | Role |
|---|---|
| `storage_interface.py` | Abstract base class defining the contract (15 abstract methods + 2 optional). |
| `storage_factory.py` | Factory function `get_storage()` — selects and instantiates backends. |
| `local_storage.py` | Local filesystem backend. |
| `s3_storage.py` | AWS S3 backend (boto3). |
| `minio_storage.py` | MinIO / S3-compatible backend (minio-py SDK). |
| `gcp_storage.py` | Google Cloud Storage backend (google-cloud-storage SDK). |
| `azure_storage.py` | Azure Blob Storage backend (azure-storage-blob SDK). |
| `defaults.py` | Global configuration dataclass loaded from `.env` / environment; includes `STORAGE_TYPE`. |
| `homedir.py` | Resolves and creates the `SUPERTABLE_HOME` directory; sets CWD at import time. |
| `__init__.py` | Package exports: `StorageInterface`, `get_storage`. |

### Key control flow

1. A caller invokes `get_storage()` (or `get_storage(kind="S3")`).
2. The factory resolves the backend type from: explicit arg → `STORAGE_TYPE` env var → `default.STORAGE_TYPE` → `"LOCAL"`.
3. For cloud backends, it verifies the SDK is installed (`_require()`), lazy-imports the module, and calls either the constructor (if kwargs provided) or `from_env()`.
4. The returned `StorageInterface` instance is used uniformly by all callers.

### Key data flow

- **Write path:** caller → `write_json` / `write_parquet` / `write_bytes` → serialize in-memory → upload to backend.
- **Read path:** backend download → deserialize (JSON parse, Parquet decode) → return to caller.
- **DuckDB path:** caller → `to_duckdb_path()` → returns a URL string that DuckDB can open directly (bypassing this layer for reads).

### Design patterns

- **Base prefix isolation:** Every backend prepends `base_prefix` to all keys via `_with_base()`, enabling multi-tenant or namespaced storage within a single bucket/container.
- **One-level listing parity:** All backends implement `list_files()` to return only immediate children of a prefix, matching local `glob` semantics — critical for consistent directory traversal logic.
- **Recursive delete with fallback:** `delete()` first tries exact-object delete, then falls back to prefix-recursive delete — unifying file and directory deletion semantics across flat object stores and hierarchical filesystems.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes / Functions | Relevance |
|---|---|---|---|
| `storage_interface.py` | ABC contract for all backends | `StorageInterface` (15 abstract methods, 2 optional) | **Critical** — defines the entire storage API surface |
| `storage_factory.py` | Backend selection and instantiation | `get_storage()`, `_require()` | **Critical** — single entry point for obtaining storage |
| `local_storage.py` | Local disk backend | `LocalStorage` | **Critical** — default/development backend |
| `s3_storage.py` | AWS S3 backend | `S3Storage`, `_call()` (auto-redirect), `_probe_bucket_region()` | **Critical** — primary cloud backend |
| `minio_storage.py` | MinIO / S3-compatible backend | `MinioStorage`, `_ensure_bucket_exists()`, `_rebuild_with_region()` | **Important** — self-hosted / hybrid support |
| `gcp_storage.py` | GCS backend | `GCSStorage` | **Important** — GCP cloud support |
| `azure_storage.py` | Azure Blob backend | `AzureBlobStorage`, `_parse_abfss()` | **Important** — Azure cloud support |
| `defaults.py` | Global config from environment | `Default` dataclass, `load_defaults_from_env()`, `refresh_defaults()`, `print_config()` | **Important** — supplies `STORAGE_TYPE` default |
| `homedir.py` | Application home directory | `_resolve_app_home()`, `change_to_app_home()`, `app_home` | **Supporting** — used by `LocalStorage` callers |
| `__init__.py` | Package public API | Exports `StorageInterface`, `get_storage` | **Supporting** — public surface |

---

## 5. Detailed Functional Capabilities

### 5.1 Unified Object CRUD

- **Description:** Read, write, check existence, get size, and delete objects across all backends using identical method signatures.
- **Business purpose:** Any Supertable subsystem can persist and retrieve data without knowing the deployment target.
- **Trigger/input:** Method call with a logical path string.
- **Processing:** Each backend translates the path (prepends `base_prefix`), invokes the cloud SDK or filesystem operation.
- **Output:** Data (bytes, dict, PyArrow Table) or success/failure.
- **Dependencies:** Cloud SDK per backend; `pyarrow` for Parquet.
- **Constraints:** All paths are relative to `base_prefix`; paths are `/`-delimited regardless of OS.
- **Risks:** Large-file reads load entire objects into memory (`read_bytes`, `read_parquet`). No streaming reads.
- **Confidence:** Explicit.

### 5.2 JSON Metadata Persistence (Atomic on Local)

- **Description:** `read_json()` / `write_json()` for structured metadata.
- **Business purpose:** Supertable likely stores table schemas, manifests, lock files, or configuration as JSON.
- **Processing (local):** Write uses `tempfile` → `fsync` → `os.replace()` for atomicity. Read uses a 5-attempt retry loop with 20ms backoff to handle concurrent atomic replaces.
- **Processing (cloud):** Standard upload/download; cloud object stores provide atomic single-object writes natively.
- **Risks:** Local retry loop covers narrow race windows but does not use file locks; under extreme write contention, reads could still fail after 5 attempts.
- **Confidence:** Explicit.

### 5.3 Parquet Data File I/O

- **Description:** `read_parquet()` / `write_parquet()` operating on `pyarrow.Table` objects.
- **Business purpose:** Parquet is the primary columnar data format for Supertable's tables/datasets.
- **Processing:** Serializes PyArrow Table to in-memory buffer, uploads. Downloads to buffer, deserializes.
- **Constraints:** Entire file loaded into memory. No support for row-group-level or predicate-pushdown reads at this layer (that is delegated to DuckDB via `to_duckdb_path()`).
- **Confidence:** Explicit.

### 5.4 DuckDB Path Generation

- **Description:** `to_duckdb_path(key)` returns a backend-appropriate URI that DuckDB can open natively.
- **Business purpose:** Enables DuckDB to read Parquet files directly from object storage, bypassing the Python storage layer for query performance.
- **Output formats:** `s3://bucket/key`, `gcs://bucket/key`, `azure://container/key`, or `https://...` (when `SUPERTABLE_DUCKDB_USE_HTTPFS=true`).
- **Processing:** Constructs the URI from bucket name, base prefix, key, and URL style (vhost vs path for S3/MinIO).
- **Dependencies:** DuckDB must be separately configured with credentials (httpfs or native S3/GCS/Azure extensions).
- **Confidence:** Explicit.

### 5.5 Presigned URL Generation

- **Description:** `presign(key, expiry_seconds)` returns a time-limited, credential-embedded URL for GET access.
- **Business purpose:** Allows external consumers (browsers, APIs, third-party tools) to download objects without direct cloud credentials.
- **Implementation varies:** S3 uses `generate_presigned_url`, MinIO uses `presigned_get_object`, GCS uses `generate_signed_url`, Azure uses SAS token generation (supports both account-key and user-delegation-key for AAD).
- **Confidence:** Explicit.

### 5.6 Directory Listing with Local Parity

- **Description:** `list_files(path, pattern)` returns immediate children matching a glob pattern, sorted deterministically.
- **Business purpose:** Supertable needs to enumerate partitions, table files, or metadata files within a logical "directory."
- **Processing:** All cloud backends use delimiter-based listing (`Delimiter="/"` on S3, `walk_blobs` on Azure, etc.) to simulate one-level directory semantics on flat object stores.
- **Confidence:** Explicit. Code comments explicitly state "local parity" as a design goal.

### 5.7 Recursive Directory Structure Inspection

- **Description:** `get_directory_structure(path)` returns a nested dict mirroring the full tree under a path.
- **Business purpose:** Likely used for introspection, debugging, or admin UI display of stored data layout.
- **Risks:** Documented as "potentially expensive for large buckets" (GCS implementation comment).
- **Confidence:** Explicit.

### 5.8 Recursive Delete

- **Description:** `delete(path)` deletes a single object if it exists at the exact path; otherwise treats `path` as a prefix and deletes all children recursively.
- **Business purpose:** Supports both single-file and table/partition-level deletion.
- **Processing (S3):** Uses paginated `list_objects_v2` + batched `delete_objects` (1000 per batch) — never accumulates all keys in memory.
- **Processing (MinIO):** Uses `remove_objects()` with a generator stream.
- **Confidence:** Explicit.

### 5.9 Auto-Region Detection and Client Rebuild (S3, MinIO)

- **Description:** S3Storage's `_call()` wrapper catches `PermanentRedirect` / `AuthorizationHeaderMalformed` errors, extracts the correct region from the error response, and rebuilds the boto3 client to target the correct region. MinIO has similar logic in `_ensure_bucket_exists`.
- **Business purpose:** Prevents misconfigured region from causing hard failures; automatically self-corrects.
- **Processing:** Up to 1 retry. Extracts region from error body, headers, or via a dedicated `_probe_bucket_region()` using `GetBucketLocation`.
- **Confidence:** Explicit.

### 5.10 Optional Dependency Model

- **Description:** `_require()` in the factory checks for SDK availability and raises a friendly install hint (`pip install 'supertable[s3]'`).
- **Business purpose:** Keeps the base Supertable install lightweight; cloud SDKs are only needed when deploying to that cloud.
- **Confidence:** Explicit (factory docstring lists the extras).

### 5.11 Environment-Driven Configuration

- **Description:** Every backend's `from_env()` reads a standardized set of env vars (plus backend-specific fallbacks). The `defaults.py` module loads `.env` files and provides a module-level `Default` dataclass.
- **Business purpose:** Twelve-factor app compliance; easy container/Kubernetes deployment via env vars.
- **Environment variables (aggregated across all backends):**

| Variable | Used by | Purpose |
|---|---|---|
| `STORAGE_TYPE` | Factory | Backend selection (LOCAL, S3, MINIO, AZURE, GCS, GCP) |
| `STORAGE_BUCKET` | S3, MinIO, GCS, Azure | Bucket / container name (default: `supertable`) |
| `STORAGE_ENDPOINT_URL` | S3, MinIO, Azure | Custom endpoint URL |
| `STORAGE_ACCESS_KEY` | S3, MinIO, Azure | Access key / account key |
| `STORAGE_SECRET_KEY` | S3, MinIO | Secret key |
| `STORAGE_SESSION_TOKEN` | S3 | AWS STS session token |
| `STORAGE_REGION` | S3, MinIO | Region hint |
| `STORAGE_FORCE_PATH_STYLE` | S3, MinIO | Force path-style addressing |
| `SUPERTABLE_PREFIX` | All cloud | Base key prefix for all operations |
| `SUPERTABLE_HOME` | homedir, Azure | Root dir (local) or ABFSS URI (Azure) |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | All cloud | Prefer HTTPS URLs for DuckDB instead of native protocol |
| `GCS_BUCKET` | GCS | GCS-specific bucket override |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCS | Path to service account JSON |
| `GCP_SA_JSON` | GCS | Raw service-account JSON string |
| `GCP_PROJECT` | GCS | GCP project override |
| `AZURE_STORAGE_ACCOUNT` | Azure | Storage account name |
| `AZURE_STORAGE_CONNECTION_STRING` | Azure | Full connection string |
| `AZURE_STORAGE_KEY` | Azure | Account key fallback |
| `AZURE_SAS_TOKEN` | Azure | SAS token |
| `AZURE_CONTAINER` | Azure | Container name fallback |
| `AZURE_BLOB_ENDPOINT` | Azure | Blob endpoint fallback |
| `LOG_LEVEL` | defaults | Logging verbosity |

- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 `StorageInterface` (`storage_interface.py`)

| Method | Type | Purpose | Returns | Importance |
|---|---|---|---|---|
| `_with_base(path)` | Concrete helper | Prepend `base_prefix` to path | `str` | Supporting |
| `read_json(path)` | Abstract | Read JSON object | `Dict[str, Any]` | Critical |
| `write_json(path, data)` | Abstract | Write JSON object | `None` | Critical |
| `exists(path)` | Abstract | Check existence | `bool` | Critical |
| `size(path)` | Abstract | Get object size in bytes | `int` | Important |
| `makedirs(path)` | Abstract | Create directory (no-op on cloud) | `None` | Supporting |
| `list_files(path, pattern)` | Abstract | One-level listing with glob | `List[str]` | Critical |
| `delete(path)` | Abstract | Delete file or recursive prefix | `None` | Critical |
| `get_directory_structure(path)` | Abstract | Recursive nested dict of tree | `dict` | Important |
| `write_parquet(table, path)` | Abstract | Write PyArrow Table as Parquet | `None` | Critical |
| `read_parquet(path)` | Abstract | Read Parquet into PyArrow Table | `pa.Table` | Critical |
| `write_bytes(path, data)` | Abstract | Write raw bytes | `None` | Critical |
| `read_bytes(path)` | Abstract | Read raw bytes | `bytes` | Critical |
| `write_text(path, text, encoding)` | Abstract | Write text | `None` | Important |
| `read_text(path, encoding)` | Abstract | Read text | `str` | Important |
| `copy(src, dst)` | Abstract | Copy within same backend | `None` | Important |
| `to_duckdb_path(key, prefer_httpfs)` | Optional concrete | DuckDB-consumable path | `str` | Critical |
| `presign(key, expiry_seconds)` | Optional concrete | Presigned GET URL | `str` | Important |

### 6.2 `get_storage()` (`storage_factory.py`)

- **Type:** Module-level factory function.
- **Purpose:** Instantiate the correct `StorageInterface` backend.
- **Parameters:** `kind: Optional[str]`, `**kwargs`.
- **Returns:** `StorageInterface` instance.
- **Key logic:** Resolves backend type from args → env → defaults → LOCAL fallback. Uses `importlib.import_module()` for lazy loading. Calls `_require()` to verify SDK installation.
- **Called by:** All Supertable subsystems that need storage.
- **Importance:** Critical.

### 6.3 `LocalStorage` (`local_storage.py`)

- **Key methods:**
  - `write_json`: Atomic write via tempfile + fsync + os.replace.
  - `read_json`: 5-attempt retry with 20ms backoff for concurrent writer tolerance.
  - `delete`: Handles files (`os.remove`), symlinks, and directories (`shutil.rmtree`).
  - `list_files`: `glob.glob` with `os.path.join`.
  - `get_directory_structure`: `os.walk` based recursive tree.
- **Dependencies:** Only stdlib + pyarrow.
- **Importance:** Critical (default backend, development, and single-node deployments).

### 6.4 `S3Storage` (`s3_storage.py`)

- **Key methods:**
  - `_call(method, **kwargs)`: Wraps every boto3 call with auto-region-redirect handling (1 retry).
  - `_probe_bucket_region()`: Creates a separate us-east-1 client to call `GetBucketLocation`.
  - `_ensure_bucket_region()`: One-time `HeadBucket` to trigger redirect correction before first real operation.
  - `_rebuild_client()`: Reconstructs boto3 client after region/endpoint correction.
  - `_list_common_prefixes_and_objects_one_level`: Paginated one-level listing.
  - `delete`: Paginated batched delete (1000 keys/batch, never all in memory).
- **Dependencies:** `boto3`, `botocore`.
- **Importance:** Critical.

### 6.5 `MinioStorage` (`minio_storage.py`)

- **Key methods:**
  - `_ensure_bucket_exists`: Auto-creates bucket if missing; handles region-mismatch errors.
  - `_rebuild_with_region`: Reconstructs Minio client with corrected region.
  - `_get_object_safe`: Downloads with guaranteed `close()` + `release_conn()`.
  - `delete`: Uses generator-based streaming delete via `remove_objects`.
- **Exposes:** `endpoint_url`, `region`, `url_style`, `secure` — consumed by DuckDB configuration upstream.
- **Dependencies:** `minio` SDK.
- **Importance:** Important.

### 6.6 `GCSStorage` (`gcp_storage.py`)

- **Key methods:**
  - `from_env`: Supports `GOOGLE_APPLICATION_CREDENTIALS` (file), `GCP_SA_JSON` (raw JSON string), and ADC.
  - `list_files`: Uses `delimiter="/"` + page iteration to collect both blobs and common prefixes.
  - `to_duckdb_path`: Returns `gcs://` or `https://storage.googleapis.com/...`.
  - `presign`: Uses `generate_signed_url` (v4).
- **Dependencies:** `google-cloud-storage`, `google-api-core`, `google-oauth2`.
- **Importance:** Important.

### 6.7 `AzureBlobStorage` (`azure_storage.py`)

- **Key methods:**
  - `from_env`: Parses ABFSS URIs (`abfss://container@account.dfs.core.windows.net/prefix`). Supports connection string, account key, SAS token, and `DefaultAzureCredential` (managed identity / AAD).
  - `presign`: Generates SAS tokens; auto-detects whether to use account-key-based or user-delegation-key-based signing.
  - `_one_level_children`: Uses `walk_blobs` with delimiter.
  - `copy`: Download + re-upload (no server-side copy).
- **Dependencies:** `azure-storage-blob`, `azure-identity` (for AAD), `azure-core`.
- **Importance:** Important.

### 6.8 `Default` dataclass and `load_defaults_from_env()` (`defaults.py`)

- **Purpose:** Load global configuration including `STORAGE_TYPE` from `.env` and environment.
- **Key fields:** `STORAGE_TYPE`, `LOG_LEVEL`, `MAX_MEMORY_CHUNK_SIZE`, `MAX_OVERLAPPING_FILES`, `DEFAULT_TIMEOUT_SEC`, `DEFAULT_LOCK_DURATION_SEC`.
- **Notable:** `print_config()` masks sensitive keys (`STORAGE_SECRET_KEY`, `STORAGE_ACCESS_KEY`, `AZURE_STORAGE_KEY`, `AZURE_SAS_TOKEN`).
- **Importance:** Important.

### 6.9 `homedir.py`

- **Purpose:** Resolve `SUPERTABLE_HOME` (default `~/supertable`), create it, and `chdir` into it at import time.
- **Side effects:** Changes process CWD on module import.
- **Public API:** `app_home` (string), `get_app_home()`.
- **Importance:** Supporting.

---

## 7. End-to-End Workflows

### 7.1 Storage Initialization

1. **Trigger:** Application startup or first call to `get_storage()`.
2. `defaults.py` is imported → `.env` file discovered and loaded → `Default` dataclass populated (including `STORAGE_TYPE`).
3. `get_storage()` resolves backend type.
4. For cloud backends: `_require()` checks SDK availability.
5. Module lazy-imported via `importlib.import_module()`.
6. If no kwargs: `Backend.from_env()` called → reads env vars → constructs SDK client.
7. For MinIO: `_ensure_bucket_exists()` auto-creates bucket.
8. For S3: `_ensure_bucket_region()` called lazily before first real operation.
9. `StorageInterface` instance returned to caller.
- **Failure modes:** Missing SDK → `RuntimeError` with install hint. Missing required env vars → `RuntimeError` with descriptive message. Bucket creation failure (MinIO) → `S3Error`.

### 7.2 Read JSON Metadata (Cloud)

1. Caller invokes `storage.read_json("orgs/acme/meta.json")`.
2. `_with_base()` prepends base prefix → `"prefix/orgs/acme/meta.json"`.
3. Backend downloads object bytes.
4. Empty check → `ValueError` if 0 bytes.
5. `json.loads()` → `ValueError` if invalid JSON.
6. Returns `Dict[str, Any]`.
- **Failure:** `FileNotFoundError` if object missing. `ValueError` if empty or corrupt.

### 7.3 Read JSON Metadata (Local — Concurrent Write Scenario)

1. Caller invokes `storage.read_json("/path/to/meta.json")`.
2. Check `os.path.isfile()`.
3. Attempt 1: Check file size. If 0 (writer mid-replace), sleep 20ms, retry.
4. Open file and `json.load()`. If `JSONDecodeError` (reader raced with writer), sleep 20ms, retry.
5. Up to 5 attempts before raising.
- **Failure:** `FileNotFoundError`, `ValueError` (empty/invalid after all retries).

### 7.4 Write JSON Metadata (Local — Atomic)

1. Caller invokes `storage.write_json(path, data)`.
2. `os.makedirs()` ensures parent directory exists.
3. `tempfile.mkstemp()` creates temp file in same directory.
4. `json.dump()` → `flush()` → `fsync()`.
5. `os.replace()` atomically swaps temp into final path.
6. `os.fsync()` on directory fd (best-effort on POSIX).
7. Cleanup: remove temp file if replace failed.

### 7.5 S3 Auto-Region Correction

1. Caller invokes any S3 operation (e.g., `read_json`).
2. `_ensure_bucket_region()` issues `HeadBucket`.
3. If `PermanentRedirect` / `AuthorizationHeaderMalformed`: `_call()` catches it.
4. Extract correct region from error response headers/body, or probe via `GetBucketLocation`.
5. Extract correct endpoint URL from redirect `Location` header.
6. Rebuild boto3 client with corrected region/endpoint.
7. Retry the original operation (1 retry max).

### 7.6 DuckDB Query Path Resolution

1. Query planner (upstream) calls `storage.to_duckdb_path("orgs/acme/data/part-0.parquet")`.
2. Backend constructs full key with base prefix.
3. If `SUPERTABLE_DUCKDB_USE_HTTPFS` is set, returns HTTPS URL; otherwise native protocol URL.
4. DuckDB opens the returned URL directly using its own credential configuration.

---

## 8. Data Model and Information Flow

### Core entities

| Entity | Format | Description |
|---|---|---|
| JSON metadata files | `Dict[str, Any]` serialized as JSON | Table schemas, manifests, configs, lock files (inferred) |
| Parquet data files | `pyarrow.Table` serialized as Parquet | Columnar data — the product's primary dataset format |
| Raw bytes / text | `bytes` / `str` | Generic file storage (logs, exports, etc.) |
| Directory tree | Nested `dict` (`{name: None \| dict}`) | Introspection / admin output |

### Key data structures

- **`StorageInterface.base_prefix`**: String prefix prepended to all keys. Enables namespace isolation (multi-tenant within a single bucket).
- **`Default` dataclass**: Global config including `STORAGE_TYPE`, `LOG_LEVEL`, memory/timeout limits.
- **Path convention**: All paths are `/`-delimited, relative, no leading `/`. The `_with_base()` method normalizes and prepends the prefix.

### Transformations

- **JSON:** `dict` ↔ `json.dumps/loads` ↔ `bytes` ↔ cloud upload/download.
- **Parquet:** `pa.Table` ↔ `pq.write_table/read_table` ↔ `io.BytesIO` buffer ↔ cloud upload/download.

### Stateful behavior

- **S3Storage:** `_bucket_region_checked` flag prevents repeated `HeadBucket` calls. `region`, `endpoint_url`, `_endpoint_url_arg` mutate during auto-correction.
- **MinioStorage:** `_endpoint`, `_access_key`, `_secret_key` cached for safe client rebuilds.
- **homedir.py:** `_resolved_home` cached; CWD changed at import time (global side effect).
- **defaults.py:** `_env_loaded` / `_env_path_cache` cached to avoid repeated `.env` discovery.

### Persistence

The storage layer **is** the persistence layer. It does not use databases or queues. All state is stored as files/objects in the configured backend.

---

## 9. Dependencies and Integrations

### Standard library
`os`, `json`, `io`, `glob`, `shutil`, `tempfile`, `time`, `abc`, `fnmatch`, `re`, `importlib`, `logging`, `dataclasses`, `pathlib`, `sys`, `urllib.parse`, `datetime`, `itertools`

### Third-party (always required)
| Package | Purpose |
|---|---|
| `pyarrow` | Parquet read/write, in-memory Table representation |
| `colorlog` | Colored console logging |
| `python-dotenv` | `.env` file loading |

### Third-party (optional, per backend)
| Package | Backend | Install extra |
|---|---|---|
| `boto3` / `botocore` | S3 | `supertable[s3]` |
| `minio` | MinIO | `supertable[minio]` |
| `google-cloud-storage`, `google-auth` | GCS | `supertable[gcp]` |
| `azure-storage-blob`, `azure-identity` | Azure | `supertable[azure]` |

### Internal modules
| Module | Relationship |
|---|---|
| `supertable.config.defaults` | Supplies `STORAGE_TYPE` default and global config |
| `supertable.config.homedir` | Supplies `app_home` for local storage root |

### Integration points
| System | Integration |
|---|---|
| **DuckDB** | `to_duckdb_path()` generates URIs for DuckDB's native file readers |
| **Cloud IAM** | Azure supports `DefaultAzureCredential` (managed identity); GCS supports ADC; S3 supports STS session tokens |
| **ABFSS** | Azure backend parses Databricks-style `abfss://` URIs for `SUPERTABLE_HOME` |

---

## 10. Architecture Positioning

### Layer
This is the **infrastructure / platform layer** — the lowest-level persistence abstraction in the Supertable stack.

### Upstream callers (inferred)
- Table management / catalog service (reads/writes JSON metadata, Parquet data files)
- Query engine integration (calls `to_duckdb_path()`)
- Ingestion pipeline (writes Parquet files)
- API / UI layer (calls `presign()` for download links)
- Admin / diagnostic tools (calls `get_directory_structure()`, `print_config()`)

### Downstream dependencies
- Cloud provider SDKs (boto3, minio, google-cloud-storage, azure-storage-blob)
- Local filesystem
- PyArrow

### Boundaries
- **Inbound contract:** `StorageInterface` methods with path strings and Python objects.
- **Outbound contract:** Cloud SDK API calls; filesystem operations.
- **No cross-backend operations:** `copy()` works within a single backend only.

### Coupling
- **Low coupling:** The ABC + factory pattern means callers depend only on `StorageInterface` and `get_storage()`. Backend implementations are completely independent.
- **Environment coupling:** All backends are tightly coupled to specific env var names, but this is intentional for 12-factor compliance.

### Scalability significance
- Object store backends inherently scale with the cloud provider.
- Local backend is single-node only.
- No connection pooling visible (each `get_storage()` call likely creates a new client; caching presumably happens at a higher layer).
- S3 batch delete (1000/batch) and MinIO streaming delete prevent memory blowup on large prefix deletes.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Enables Supertable to deploy on any infrastructure without code changes. |
| **Revenue relevance** | Directly enables enterprise sales to AWS, GCP, Azure, and on-prem customers. |
| **Strategic significance** | **High** — cloud-agnostic storage is a differentiator vs. cloud-locked analytics products. |
| **Efficiency** | DuckDB path generation enables high-performance direct-from-storage queries, avoiding data copy. |
| **Compliance** | Azure managed identity and GCS ADC support enable credential-free deployments in secure environments. |
| **What would be lost** | Without this layer, Supertable would be locked to a single deployment target. Every subsystem would need cloud-specific code. |
| **Differentiating vs. commodity** | The abstraction itself is a common pattern, but the DuckDB integration, auto-region-correction, and ABFSS parsing are product-specific and non-trivial. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate-high. The S3 auto-region logic alone is ~200 lines of nuanced error handling. |
| **Architectural maturity** | High. Clean ABC, factory pattern, lazy imports, environment-driven config, optional dependencies. |
| **Maintainability** | Good. Consistent method signatures across all backends. Each backend is self-contained. |
| **Extensibility** | Easy to add new backends — implement `StorageInterface`, add a branch in `get_storage()`. |
| **Operational sensitivity** | High. Any bug here can corrupt or lose data. The atomic local write and retry-on-race patterns indicate awareness of this risk. |
| **Performance** | Adequate for metadata. Parquet I/O loads entire files into memory, which is acceptable for moderate file sizes but would bottleneck on very large files. DuckDB bypass (`to_duckdb_path`) is the performance escape hatch. |
| **Reliability** | Good. S3 auto-redirect, MinIO auto-bucket-creation, and local atomic writes all improve resilience. Resource cleanup (`_get_object_safe` patterns) prevents connection leaks. |
| **Security** | `print_config()` masks secrets. Presigned URLs are time-limited. Azure supports AAD/managed identity. No credentials logged. Credentials stored on MinIO instances are private attributes. |
| **Testing** | GCS constructor accepts a pre-built `client` parameter for DI/testing. S3 constructor accepts a pre-built `client`. No test files provided but DI hooks are present. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code
- Five storage backends exist: LOCAL, S3, MINIO, GCS, AZURE.
- Factory uses lazy imports and optional dependency checking.
- All backends implement identical `StorageInterface` methods.
- `base_prefix` provides namespace isolation within a single bucket.
- S3 backend auto-corrects region mismatches.
- MinIO backend auto-creates buckets.
- Local backend uses atomic JSON writes with retry-on-race reads.
- DuckDB path generation is a first-class concern (dedicated methods on every cloud backend).
- Default bucket name is `"supertable"` across all backends.
- The product name is **Supertable** (package name `supertable`).
- Install extras exist: `supertable[s3]`, `supertable[minio]`, `supertable[azure]`, `supertable[gcp]`, `supertable[all]`.

### Reasonable Inferences
- Supertable is a data-platform / analytics product built on DuckDB and Apache Arrow/Parquet.
- JSON files store metadata (schemas, manifests, configs); Parquet files store user data.
- The `base_prefix` is likely used for multi-tenant isolation within a shared bucket (e.g., one prefix per organization).
- `SUPERTABLE_HOME` with ABFSS support suggests compatibility with Databricks / Azure Synapse environments.
- The `presign()` method is used by a web API or UI to serve download links.
- `get_storage()` is called once at application startup and the result is reused (singleton pattern at a higher layer).
- Fields `DEFAULT_LOCK_DURATION_SEC` and `DEFAULT_TIMEOUT_SEC` in the `Default` dataclass suggest a locking/concurrency mechanism exists elsewhere.
- `MAX_OVERLAPPING_FILES` and `MAX_MEMORY_CHUNK_SIZE` suggest a merge/compaction process exists elsewhere.

### Unknown / Not Visible in Provided Snippet
- How `get_storage()` is called and whether the result is cached/shared.
- The higher-level modules that consume this layer (catalog, ingestion, query engine, API).
- How DuckDB is configured with credentials to match `to_duckdb_path()` output.
- Whether there is a caching layer above this.
- The locking mechanism hinted at by `DEFAULT_LOCK_DURATION_SEC`.
- The merge/compaction logic hinted at by `MAX_OVERLAPPING_FILES` and `MAX_MEMORY_CHUNK_SIZE`.
- Whether `delete_prefix` (GCS-only explicit method) is used distinctly from `delete`.
- Test coverage for this layer.
- Error monitoring / alerting integration.
- Whether `REDIS_URL` in `print_config()` connects to any caching or locking that interacts with storage.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `supertable.storage` — Multi-Backend Storage Abstraction Layer
- **Alternative names/aliases:** Storage layer, storage backend, storage interface, persistence layer
- **Main responsibilities:** Unified file/object CRUD across local disk, S3, MinIO, GCS, Azure; DuckDB path generation; presigned URL generation; environment-driven configuration.
- **Important entities:** `StorageInterface` (ABC), `LocalStorage`, `S3Storage`, `MinioStorage`, `GCSStorage`, `AzureBlobStorage`, `get_storage()` (factory), `Default` (config dataclass), `app_home` (home directory).
- **Important workflows:** Storage initialization, JSON read/write (with local atomicity), Parquet read/write, DuckDB path resolution, S3 auto-region correction, presigned URL generation.
- **Integration points:** DuckDB (path generation), cloud IAMs (managed identity, ADC, STS), ABFSS (Databricks/Azure).
- **Business purpose keywords:** cloud-agnostic, multi-cloud, vendor-neutral, data persistence, analytics storage, DuckDB integration, Parquet, metadata management.
- **Architecture keywords:** strategy pattern, factory pattern, adapter pattern, ABC, lazy imports, optional dependencies, 12-factor, environment-driven config.
- **Follow-up files for completeness:**
  - Catalog / table management module (consumer of `read_json` / `write_json` / `read_parquet` / `write_parquet`)
  - DuckDB configuration / query engine module (consumer of `to_duckdb_path`)
  - API / handler layer (consumer of `presign`)
  - Locking / concurrency module (hinted by `DEFAULT_LOCK_DURATION_SEC`)
  - Ingestion / compaction module (hinted by `MAX_OVERLAPPING_FILES`, `MAX_MEMORY_CHUNK_SIZE`)
  - `setup.py` / `pyproject.toml` for extras definitions

---

## 15. Suggested Documentation Tags

`storage`, `persistence`, `infrastructure`, `cloud-agnostic`, `multi-cloud`, `aws-s3`, `gcs`, `azure-blob`, `minio`, `local-filesystem`, `strategy-pattern`, `factory-pattern`, `adapter-pattern`, `abc`, `pyarrow`, `parquet`, `duckdb-integration`, `presigned-urls`, `environment-config`, `twelve-factor`, `lazy-imports`, `optional-dependencies`, `atomic-writes`, `auto-region-detection`, `supertable-core`, `data-platform`, `analytics-storage`, `metadata-persistence`, `namespace-isolation`, `base-prefix`

---

## Merge Readiness

- **Suggested canonical name:** `supertable.storage` — Multi-Backend Storage Abstraction Layer
- **Standalone or partial:** This analysis covers the complete storage layer as a self-contained subsystem. It is **standalone** within its boundary but **partial** in the context of the full Supertable product.
- **Related components that should be merged later:**
  - `supertable.catalog` or equivalent table/metadata management module
  - `supertable.engine` or DuckDB query integration module
  - `supertable.api` or web/handler layer
  - `supertable.lock` or concurrency/locking module
  - `supertable.ingest` or data ingestion/compaction module
- **What additional files would most improve certainty:**
  - Any module that calls `get_storage()` — would confirm how the layer is consumed.
  - `pyproject.toml` / `setup.cfg` — would confirm extras and package structure.
  - Any lock/concurrency module — would explain `DEFAULT_LOCK_DURATION_SEC`.
- **Recommended merge key phrases:** `StorageInterface`, `get_storage`, `storage backend`, `to_duckdb_path`, `base_prefix`, `STORAGE_TYPE`, `supertable.storage`.
