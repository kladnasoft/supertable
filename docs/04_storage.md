# 04 -- Storage Layer

## Business Context

SuperTable decouples metadata (Redis) from heavy data (Parquet files, snapshot JSON) through a pluggable storage abstraction. This design lets organizations start on local disk during development, move to MinIO for self-hosted object storage, and scale to managed cloud services (AWS S3, Azure Blob Storage, Google Cloud Storage) in production -- all without changing application code. The storage layer is the single point of integration for reading and writing data files, and every backend implements the same interface so the rest of the system remains backend-agnostic.

The factory pattern ensures that backend selection is driven by configuration (environment variables or explicit arguments), while lazy imports keep cloud SDK dependencies optional. Teams only install the packages they need.

---

## StorageInterface (Abstract Base Class)

**Module**: `supertable/storage/storage_interface.py`

All storage backends extend `StorageInterface`, which defines the complete contract for data operations. The class also provides a `base_prefix` mechanism -- when set, all paths are automatically prefixed, enabling a single bucket to host multiple SuperTable deployments.

### Helper Method

```python
def _with_base(self, path: str) -> str
```

Prepends `base_prefix` to the given path if set. No-op when `base_prefix` is empty. All backend implementations call this internally before operating on keys.

### Abstract Methods -- JSON Operations

```python
@abc.abstractmethod
def read_json(self, path: str) -> Dict[str, Any]
```
Reads and returns parsed JSON data from the given path. Raises `FileNotFoundError` or `ValueError` on error.

```python
@abc.abstractmethod
def write_json(self, path: str, data: Dict[str, Any]) -> None
```
Writes JSON data to the given path. Overwrites if the file already exists.

### Abstract Methods -- File System Operations

```python
@abc.abstractmethod
def exists(self, path: str) -> bool
```
Returns `True` if the given path exists. For cloud storage, the path may be a prefix or object key.

```python
@abc.abstractmethod
def size(self, path: str) -> int
```
Returns the size in bytes of the object at the given path. Raises `FileNotFoundError` if not found.

```python
@abc.abstractmethod
def makedirs(self, path: str) -> None
```
Creates directories (or the cloud equivalent) if needed. No-op if already present.

```python
@abc.abstractmethod
def list_files(self, path: str, pattern: str = "*") -> List[str]
```
Returns a list of files/objects found under `path` matching the given glob pattern. Operates at a single directory level.

```python
@abc.abstractmethod
def delete(self, path: str) -> None
```
Deletes a file/object at the given path. Raises `FileNotFoundError` if the path does not exist.

```python
@abc.abstractmethod
def get_directory_structure(self, path: str) -> dict
```
Recursively builds and returns a nested dictionary representing the folder structure under the given path. Files map to `None`, directories map to nested dicts.

### Abstract Methods -- Parquet Operations

```python
@abc.abstractmethod
def write_parquet(self, table: pa.Table, path: str) -> None
```
Writes a PyArrow Table in Parquet format to the given path.

```python
@abc.abstractmethod
def read_parquet(self, path: str) -> pa.Table
```
Reads and returns a PyArrow Table from the given Parquet path.

### Abstract Methods -- Byte / Text / Copy Operations

```python
@abc.abstractmethod
def write_bytes(self, path: str, data: bytes) -> None
```
Writes raw bytes to the given path. Creates parent directories/prefixes as needed.

```python
@abc.abstractmethod
def read_bytes(self, path: str) -> bytes
```
Reads and returns raw bytes from the given path.

```python
@abc.abstractmethod
def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None
```
Writes a string to the given path using the specified encoding.

```python
@abc.abstractmethod
def read_text(self, path: str, encoding: str = "utf-8") -> str
```
Reads and returns text from the given path using the specified encoding.

```python
@abc.abstractmethod
def copy(self, src_path: str, dst_path: str) -> None
```
Copies an object from `src_path` to `dst_path` within the same storage backend.

### Optional Methods (Not Abstract)

```python
def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str
```
Returns a path usable by DuckDB readers. Implementations may return `s3://bucket/key` or HTTP(S) URLs. Raises `NotImplementedError` by default.

```python
def presign(self, key: str, expiry_seconds: int = 3600) -> str
```
Returns a presigned GET URL for the object. Raises `NotImplementedError` by default.

---

## StorageFactory

**Module**: `supertable/storage/storage_factory.py`

The factory function `get_storage()` resolves and instantiates the correct backend.

```python
def get_storage(kind: Optional[str] = None, **kwargs: Any) -> StorageInterface
```

### Selection Logic (Priority Order)

| Priority | Source | Example |
|----------|--------|---------|
| 1 | Explicit `kind` argument | `get_storage(kind="S3")` |
| 2 | `settings.STORAGE_TYPE` (from env `STORAGE_TYPE`) | `STORAGE_TYPE=MINIO` |
| 3 | `default.STORAGE_TYPE` from defaults config | Configured at package level |
| 4 | Fallback | `LOCAL` |

### Backend Resolution

| `STORAGE_TYPE` | Class | Module | Required Package |
|----------------|-------|--------|------------------|
| `LOCAL` | `LocalStorage` | `supertable.storage.local_storage` | None (built-in) |
| `S3` | `S3Storage` | `supertable.storage.s3_storage` | `boto3` (`pip install 'supertable[s3]'`) |
| `MINIO` | `MinioStorage` | `supertable.storage.minio_storage` | `minio` (`pip install 'supertable[minio]'`) |
| `AZURE` | `AzureBlobStorage` | `supertable.storage.azure_storage` | `azure.storage.blob` (`pip install 'supertable[azure]'`) |
| `GCS` / `GCP` | `GCSStorage` | `supertable.storage.gcp_storage` | `google.cloud.storage` (`pip install 'supertable[gcp]'`) |

All cloud modules are lazily imported via `importlib.import_module()`. Before importing, a `_require()` helper checks that the necessary SDK is installed and raises a user-friendly error with an install hint if it is missing.

When `kwargs` are provided, they are forwarded directly to the backend constructor. When no `kwargs` are provided (the common case), cloud backends use their `from_env()` class method to build themselves from environment variables.

---

## Backend Details

### LocalStorage

**Module**: `supertable.storage.local_storage`
**Class**: `LocalStorage`

The default backend, requiring no external dependencies beyond PyArrow for Parquet support.

```python
class LocalStorage(StorageInterface):
    pass  # No __init__ arguments; uses app_home as base directory
```

**Key implementation details**:

- **Atomic JSON writes**: Uses `tempfile.mkstemp` in the same directory, writes + `fsync`, then `os.replace()` for an atomic swap. Directory is fsynced on POSIX systems for durability.
- **Retry on read**: A micro-retry loop (5 attempts, 20 ms backoff) handles transient races with concurrent writers performing atomic replace.
- **No authentication**: Operates on the local filesystem relative to `app_home`.

### S3Storage

**Module**: `supertable.storage.s3_storage`
**Class**: `S3Storage`

AWS S3 backend built on `boto3`.

```python
class S3Storage(StorageInterface):
    def __init__(
        self,
        bucket_name: str,
        client=None,
        endpoint_url: Optional[str] = None,
        region: Optional[str] = None,
        url_style: str = "vhost",
        secure: Optional[bool] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        base_prefix: str = "",
    )

    @classmethod
    def from_env(cls) -> "S3Storage"
```

**Authentication / Configuration via `from_env()`**:

| Environment Variable | Purpose |
|---------------------|---------|
| `STORAGE_BUCKET` | S3 bucket name |
| `STORAGE_REGION` | AWS region |
| `STORAGE_ENDPOINT_URL` | Custom endpoint (for S3-compatible services) |
| `STORAGE_ACCESS_KEY` | AWS access key ID |
| `STORAGE_SECRET_KEY` | AWS secret access key |
| `STORAGE_SESSION_TOKEN` | AWS session token (for temporary credentials) |
| `SUPERTABLE_PREFIX` | Base prefix within the bucket |
| `STORAGE_FORCE_PATH_STYLE` | Use path-style addressing (for S3-compatible services) |

**Notes**:
- Automatically normalizes bucket-prefixed AWS endpoints (e.g., `https://bucket.s3.amazonaws.com`).
- Supports both virtual-hosted and path-style addressing.
- Secure (HTTPS) is auto-detected from the endpoint URL scheme.

### MinioStorage

**Module**: `supertable.storage.minio_storage`
**Class**: `MinioStorage`

MinIO backend using the official MinIO Python SDK.

```python
class MinioStorage(StorageInterface):
    def __init__(self, bucket_name: str, client: Minio, base_prefix: str = "")

    @classmethod
    def from_env(cls) -> "MinioStorage"
```

**Authentication / Configuration via `from_env()`**:

| Environment Variable | Purpose |
|---------------------|---------|
| `STORAGE_BUCKET` | Bucket name |
| `STORAGE_ENDPOINT_URL` | MinIO endpoint (e.g., `http://localhost:9000`) |
| `STORAGE_ACCESS_KEY` | MinIO access key |
| `STORAGE_SECRET_KEY` | MinIO secret key |
| `STORAGE_REGION` | Region (optional) |
| `SUPERTABLE_PREFIX` | Base prefix within the bucket |

**Notes**:
- Auto-creates the bucket if it does not exist (with region-aware retry).
- Handles `AuthorizationHeaderMalformed` errors by extracting the expected region from the error message and rebuilding the client.
- Exposes `endpoint_url`, `region`, `url_style`, and `secure` properties for downstream DuckDB integration.
- Default `url_style` is `"path"`.

### AzureBlobStorage

**Module**: `supertable.storage.azure_storage`
**Class**: `AzureBlobStorage`

Azure Blob Storage backend using the Azure SDK.

```python
class AzureBlobStorage(StorageInterface):
    def __init__(self, container_name: str, blob_service_client: BlobServiceClient, base_prefix: str = "")

    @classmethod
    def from_env(cls) -> "AzureBlobStorage"
```

**Authentication / Configuration via `from_env()`**:

| Environment Variable | Purpose |
|---------------------|---------|
| `SUPERTABLE_HOME` | ABFSS URI: `abfss://{container}@{account}.dfs.core.windows.net/{prefix}` |
| `STORAGE_BUCKET` / `AZURE_CONTAINER` | Container name |
| `STORAGE_ENDPOINT_URL` / `AZURE_BLOB_ENDPOINT` | Blob endpoint URL |
| `STORAGE_ACCESS_KEY` / `AZURE_STORAGE_KEY` | Account key |
| `SUPERTABLE_PREFIX` | Base prefix within the container |
| `AZURE_STORAGE_CONNECTION_STRING` | Full connection string |
| `AZURE_STORAGE_ACCOUNT` | Storage account name |
| `AZURE_SAS_TOKEN` | Shared Access Signature token |
| `AZURE_AUTH_MODE` | When `AAD` with no secrets, uses `DefaultAzureCredential` |

**ABFSS URI parsing**: The helper `_parse_abfss(uri)` extracts `(account, container, blob_endpoint, prefix)` from an `abfss://` URI. This allows a single `SUPERTABLE_HOME` variable to configure the entire Azure connection.

**Authentication cascade**:
1. Connection string (if provided)
2. Account key
3. SAS token
4. `DefaultAzureCredential` (managed identity / AAD)

### GCSStorage

**Module**: `supertable.storage.gcp_storage`
**Class**: `GCSStorage`

Google Cloud Storage backend using the `google-cloud-storage` SDK.

```python
class GCSStorage(StorageInterface):
    def __init__(
        self,
        bucket: str,
        credentials_path: Optional[str] = None,
        client: Optional[storage.Client] = None,
        base_prefix: str = "",
    )

    @classmethod
    def from_env(cls) -> "GCSStorage"
```

**Authentication / Configuration via `from_env()`**:

| Environment Variable | Purpose |
|---------------------|---------|
| `GCS_BUCKET` / `STORAGE_BUCKET` | Bucket name (default: `supertable`) |
| `SUPERTABLE_PREFIX` | Base prefix within the bucket |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file |
| `GCP_SA_JSON` | Raw JSON string (alternative to file path) |
| `GCP_PROJECT` | Optional GCP project override |

**Authentication cascade**:
1. Service account JSON file (via `GOOGLE_APPLICATION_CREDENTIALS`)
2. Inline service account JSON string (via `GCP_SA_JSON`)
3. Application Default Credentials (ADC) -- works with GKE workload identity, Cloud Run, etc.

**Notes**:
- GCS uses a flat namespace; directories are simulated by common prefixes.
- One-level listing uses `delimiter="/"` for parity with local storage behavior.

---

## Configuration Summary

All backends share a common configuration pattern:

```
STORAGE_TYPE=<LOCAL|S3|MINIO|AZURE|GCS|GCP>
STORAGE_BUCKET=<bucket or container name>
STORAGE_ENDPOINT_URL=<endpoint URL>
STORAGE_ACCESS_KEY=<access key>
STORAGE_SECRET_KEY=<secret key>
SUPERTABLE_PREFIX=<optional base prefix>
```

Cloud-specific variables extend this base set. The `from_env()` class methods on each backend read these variables through the centralized `settings` object, ensuring consistent precedence and defaults across the system.
