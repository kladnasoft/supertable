# Data Island Core — Storage Backends

## Overview

Data Island Core stores all data files on a pluggable object storage backend. The same abstract interface is used for local development (filesystem), cloud deployments (S3, Azure Blob, GCP Cloud Storage), and self-hosted object stores (MinIO). The storage layer handles Parquet files, JSON metadata, raw byte blobs, and directory structure traversal — everything the platform writes to persistent storage flows through `StorageInterface`.

The backend is selected at startup via the `STORAGE_TYPE` environment variable. All backends implement the same abstract interface, so switching from MinIO to S3 requires only changing configuration — no code changes.

---

## How it works

### Backend selection

`storage_factory.get_storage()` reads `STORAGE_TYPE` and returns the matching `StorageInterface` implementation. The selection order is:

1. Explicit `kind` argument (if called programmatically)
2. `STORAGE_TYPE` environment variable
3. Fallback to `LOCAL`

```python
from supertable.storage.storage_factory import get_storage

storage = get_storage()            # Uses STORAGE_TYPE env var
storage = get_storage(kind="S3")   # Explicit override
```

Each cloud backend has a `from_env()` classmethod that constructs the client from environment variables. Dependencies are lazy-imported — if you don't use Azure, you don't need `azure-storage-blob` installed.

### File path conventions

All data is stored under paths relative to the storage root (bucket root for cloud, base directory for local):

```
{organization}/{super_name}/{simple_name}/v{version}/{filename}.parquet
{organization}/{super_name}/__meta__/...
{organization}/{super_name}/__staging__/{staging_name}/...
{organization}/__audit__/year=YYYY/month=MM/day=DD/...
```

The `base_prefix` field on each backend instance allows prepending a common prefix to all paths. This is used when multiple environments share a single bucket.

---

## StorageInterface — abstract base class

Defined in `supertable/storage/storage_interface.py`. Every backend must implement these methods:

### Core operations

| Method | Signature | Description |
|---|---|---|
| `read_json` | `(path: str) → Dict` | Read and parse a JSON file. Raises `FileNotFoundError`. |
| `write_json` | `(path: str, data: Dict) → None` | Write a dict as JSON. Overwrites if exists. |
| `exists` | `(path: str) → bool` | Check if a path exists. |
| `size` | `(path: str) → int` | Return file size in bytes. Raises `FileNotFoundError`. |
| `makedirs` | `(path: str) → None` | Create directories. No-op if exists. No-op on object stores. |
| `list_files` | `(path: str, pattern: str = "*") → List[str]` | List files matching a glob pattern at one directory level. |
| `delete` | `(path: str) → None` | Delete a file or prefix. Raises `FileNotFoundError`. |
| `get_directory_structure` | `(path: str) → dict` | Build a nested dict representing the directory tree. |

### Parquet operations

| Method | Signature | Description |
|---|---|---|
| `write_parquet` | `(table: pa.Table, path: str) → None` | Write a PyArrow table as Parquet. |
| `read_parquet` | `(path: str) → pa.Table` | Read a Parquet file into a PyArrow table. |

### Byte and text operations

| Method | Signature | Description |
|---|---|---|
| `write_bytes` | `(path: str, data: bytes) → None` | Write raw bytes. Creates parent directories as needed. |
| `read_bytes` | `(path: str) → bytes` | Read raw bytes. Raises `FileNotFoundError`. |
| `write_text` | `(path: str, text: str, encoding: str = "utf-8") → None` | Write a string. |
| `read_text` | `(path: str, encoding: str = "utf-8") → str` | Read a string. |
| `copy` | `(src_path: str, dst_path: str) → None` | Copy within the same backend. |

### DuckDB integration (optional)

| Method | Signature | Description |
|---|---|---|
| `to_duckdb_path` | `(key: str, prefer_httpfs: bool = None) → str` | Return a path usable by DuckDB (`s3://...` or presigned URL). |
| `presign` | `(key: str, expiry_seconds: int = 3600) → str` | Generate a presigned GET URL. |

These are optional — `LocalStorage` does not implement them (DuckDB reads local files directly). Cloud backends implement both to support DuckDB's httpfs extension.

---

## Backend implementations

### LocalStorage

The simplest backend. Files are stored on the local filesystem relative to the working directory (`SUPERTABLE_HOME`).

**Configuration:** No additional env vars required. `STORAGE_TYPE=LOCAL` is the default.

**Behavior notes:**
- `makedirs` uses `os.makedirs(exist_ok=True)`
- `delete` removes files with `os.remove` and recursively removes directories with `shutil.rmtree`
- `list_files` uses `glob.glob` for pattern matching
- `write_json` writes atomically via `tempfile` + rename
- `to_duckdb_path` and `presign` are not implemented (DuckDB reads local paths directly)

**When to use:** Local development, single-machine deployments, testing.

### MinioStorage

S3-compatible object store backend using the `minio` Python client. This is the recommended backend for self-hosted deployments.

**Configuration:**

| Environment variable | Required | Description |
|---|---|---|
| `STORAGE_TYPE` | yes | Must be `MINIO` |
| `STORAGE_ENDPOINT_URL` | yes | MinIO server URL (e.g., `http://minio:9000`) |
| `STORAGE_ACCESS_KEY` | yes | MinIO access key |
| `STORAGE_SECRET_KEY` | yes | MinIO secret key |
| `STORAGE_BUCKET` | no | Bucket name (default: `supertable`) |
| `STORAGE_REGION` | no | Region (default: `eu-central-1`) |
| `STORAGE_FORCE_PATH_STYLE` | no | Path-style URLs (default: `true`, required for MinIO) |

**Behavior notes:**
- Bucket is auto-created on first use if it doesn't exist (`_ensure_bucket_exists`)
- Region mismatch errors are auto-detected and the client is rebuilt with the correct region
- `delete` handles both single objects and prefix-based recursive deletion
- `presign` generates presigned GET URLs via MinIO's `presigned_get_object`
- `to_duckdb_path` returns `s3://bucket/key` or a presigned HTTP URL depending on `prefer_httpfs`

**Dependencies:** `pip install minio`

### S3Storage

Native AWS S3 backend using `boto3`. The most complex implementation due to AWS region handling, endpoint resolution, and error recovery.

**Configuration:**

| Environment variable | Required | Description |
|---|---|---|
| `STORAGE_TYPE` | yes | Must be `S3` |
| `STORAGE_BUCKET` | yes | S3 bucket name |
| `STORAGE_ACCESS_KEY` or `AWS_ACCESS_KEY_ID` | yes | AWS access key |
| `STORAGE_SECRET_KEY` or `AWS_SECRET_ACCESS_KEY` | yes | AWS secret key |
| `STORAGE_REGION` or `AWS_DEFAULT_REGION` | no | AWS region (auto-detected from bucket if omitted) |
| `STORAGE_ENDPOINT_URL` | no | Custom endpoint (for S3-compatible services) |
| `STORAGE_SESSION_TOKEN` | no | STS session token |

**Behavior notes:**
- Auto-detects bucket region via `head_bucket` probe if not configured
- Rebuilds the boto3 client when region mismatches are detected
- Supports both global and regional AWS endpoints
- `_call` wrapper method handles automatic region correction and retry for common S3 errors
- `presign` uses `generate_presigned_url` with configurable expiry

**Dependencies:** `pip install boto3`

### AzureBlobStorage

Azure Blob Storage backend using the `azure-storage-blob` SDK.

**Configuration:**

| Environment variable | Required | Description |
|---|---|---|
| `STORAGE_TYPE` | yes | Must be `AZURE` |
| `AZURE_STORAGE_CONNECTION_STRING` | * | Full connection string (takes precedence) |
| `AZURE_STORAGE_ACCOUNT` | * | Storage account name |
| `AZURE_CONTAINER` or `STORAGE_BUCKET` | yes | Container name |
| `AZURE_BLOB_ENDPOINT` | no | Custom endpoint URL |
| `AZURE_STORAGE_KEY` | no | Storage access key |
| `AZURE_SAS_TOKEN` | no | Shared access signature |

\* Either `AZURE_STORAGE_CONNECTION_STRING` or `AZURE_STORAGE_ACCOUNT` is required.

**Behavior notes:**
- Supports connection string, account key, SAS token, and managed identity (DefaultAzureCredential)
- Container is auto-created if it doesn't exist
- `presign` generates SAS URLs with configurable expiry
- `to_duckdb_path` returns `azure://container/path` for DuckDB's Azure extension

**Dependencies:** `pip install azure-storage-blob azure-identity`

### GCSStorage

Google Cloud Storage backend using the `google-cloud-storage` SDK.

**Configuration:**

| Environment variable | Required | Description |
|---|---|---|
| `STORAGE_TYPE` | yes | Must be `GCP` or `GCS` |
| `GCS_BUCKET` or `STORAGE_BUCKET` | yes | GCS bucket name |
| `GOOGLE_APPLICATION_CREDENTIALS` | * | Path to service account JSON |
| `GCP_SA_JSON` | * | Inline service account JSON |
| `GCP_PROJECT` | no | GCP project ID |

\* Either `GOOGLE_APPLICATION_CREDENTIALS` (file path) or `GCP_SA_JSON` (inline JSON) is required, or use default application credentials (e.g., on GCE/GKE).

**Behavior notes:**
- Supports service account JSON (file or inline), application default credentials, and workload identity
- `delete` handles both single blobs and prefix-based recursive deletion via `delete_prefix`
- `presign` uses `generate_signed_url` with configurable expiry
- `to_duckdb_path` returns `gcs://bucket/path`

**Dependencies:** `pip install google-cloud-storage`

---

## DuckDB integration

DuckDB reads Parquet files directly from object storage using its httpfs extension. The query engine calls `to_duckdb_path()` on the storage backend to get a URL that DuckDB can read.

There are two modes:

**Direct S3 protocol** (`prefer_httpfs=False`, default): Returns `s3://bucket/key`. DuckDB uses its built-in S3 reader with the same credentials configured for the storage backend. Fastest, but requires DuckDB's httpfs extension and S3-compatible storage.

**Presigned HTTP URLs** (`prefer_httpfs=True`, enabled by `SUPERTABLE_DUCKDB_PRESIGNED=1`): Returns a presigned HTTPS URL. DuckDB fetches the file via plain HTTP — no S3 credentials needed in DuckDB. Works with any storage backend that supports presigning. Recommended for MinIO deployments.

---

## Module structure

```
supertable/storage/
  __init__.py              Package marker
  storage_interface.py     Abstract base class (174 lines)
  storage_factory.py       Backend selection by STORAGE_TYPE (89 lines)
  local_storage.py         Local filesystem (230 lines)
  minio_storage.py         MinIO / S3-compatible (377 lines)
  s3_storage.py            Amazon S3 with auto-region (650 lines)
  azure_storage.py         Azure Blob Storage (393 lines)
  gcp_storage.py           Google Cloud Storage (351 lines)
```

---

## Frequently asked questions

**Can I switch storage backends without migrating data?**
No. Data files are written to the configured backend. To switch from MinIO to S3, you need to copy all files from the MinIO bucket to the S3 bucket, then change `STORAGE_TYPE` and credentials. The file paths are identical across backends.

**Does Data Island Core support multi-cloud storage?**
Not within a single deployment. One `STORAGE_TYPE` is active at a time. Different organizations cannot use different backends — all data goes to the same backend.

**What happens if the storage backend is unavailable?**
Reads fail immediately with an exception. Writes fail after lock acquisition (the lock is released on failure). The platform does not queue or retry storage operations — the caller receives the error.

**Do I need to create the bucket manually?**
For MinIO and Azure, the bucket/container is auto-created on first use. For S3, the bucket must exist (auto-creation is not attempted due to AWS IAM complexity). For GCP, the bucket must exist.

**How are Parquet files compressed?**
By default, Parquet files use Snappy compression (the PyArrow default). The audit module explicitly sets Snappy. The data writer uses the PyArrow default (also Snappy).

**Can I use S3-compatible storage (Wasabi, DigitalOcean Spaces, Backblaze B2)?**
Yes. Use `STORAGE_TYPE=MINIO` with the provider's endpoint URL and credentials. The MinIO client is S3-compatible and works with any S3-compatible service.

**How does presigning work with MinIO?**
MinIO generates presigned URLs that DuckDB can fetch via HTTP. Set `SUPERTABLE_DUCKDB_PRESIGNED=1` to enable. The presigned URLs expire after 1 hour by default (configurable via the `presign()` method's `expiry_seconds` parameter).
