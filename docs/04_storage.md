# Storage

SuperTable stores Parquet data and JSON snapshots through `StorageInterface`. Redis stores the current table pointers and coordination state. Selecting local storage does not remove the Redis dependency.

Implementation: [storage interface](../supertable/storage/storage_interface.py), [factory](../supertable/storage/storage_factory.py), [table layout](../supertable/simple_table.py). See [configuration](02_configuration.md) for settings and [catalog](05_redis_catalog.md) for publication semantics.

## Choose a backend

```python
from supertable.storage.storage_factory import get_storage

storage = get_storage()
storage.write_json("examples/run.json", {"state": "ready"})
assert storage.read_json("examples/run.json")["state"] == "ready"
```

`get_storage(kind=None, **kwargs)` chooses the explicit `kind`, then `settings.STORAGE_TYPE`. The factory recognizes these names:

| Kind | Implementation | Optional dependency extra | Construction from settings |
| --- | --- | --- | --- |
| `LOCAL` | `LocalStorage` | None specific to this backend | Uses filesystem paths and the application working directory |
| `S3` | `S3Storage` | `supertable[s3]` | Bucket, endpoint, region, access key, secret key, session token, addressing style, prefix |
| `MINIO` | `MinioStorage` | `supertable[minio]` | Bucket, required HTTP(S) endpoint and access/secret keys, region, prefix |
| `AZURE` | `AzureBlobStorage` | `supertable[azure]` | Blob endpoint/account, container, credentials, prefix |
| `GCS` or `GCP` | `GCSStorage` | `supertable[gcp]` | Bucket, service-account credentials or default credentials, project, prefix |

Unknown kinds raise `ValueError`. Missing provider dependencies raise an installation error. With no keyword arguments, the factory caches backend instances using backend and storage settings as the key. `reset_storage_cache()` clears that cache; it is also registered to run in forked children. Passing keyword arguments bypasses the cache and calls the backend constructor directly. Keyword arguments are constructor arguments, not environment-variable names.

## Paths and physical layout

Table code constructs relative logical paths:

```text
<organization>/<super_name>/
  super/
  tables/<simple_name>/
    data/
    snapshots/
    stats/
    tombstone/
  staging/
    <staging_name>/
    <staging_name>_files.json
```

`data/` contains Parquet resources; `snapshots/` contains JSON versions of a simple table. Statistics and tombstones are separate Parquet resources when produced by writes. Staging data and its JSON file index live outside the simple-table directories. The `super/` directory is created for local storage; the super-table root metadata itself lives in Redis.

Object backends prepend `SUPERTABLE_PREFIX`, if set, to the logical path. For example, prefix `warehouse` and path `acme/sales/tables/orders/data/part.parquet` address object key `warehouse/acme/sales/tables/orders/data/part.parquet` in the configured bucket or container. Supply paths without that prefix to ordinary read/write methods: `_with_base()` prepends it and does not detect an already-prefixed key.

Local storage uses the supplied path directly, so absolute paths remain absolute. Importing [homedir](../supertable/config/homedir.py) resolves a writable application home and changes the process working directory to it. Relative local paths consequently resolve there. Local storage does not apply `SUPERTABLE_PREFIX` and does not restrict callers to the application home.

## Common operations

| Operation | Behavior |
| --- | --- |
| `read_json(path)`, `write_json(path, data)` | Load/store JSON; absent reads raise `FileNotFoundError`; empty or invalid JSON produces an error |
| `read_bytes`, `write_bytes`, `read_text`, `write_text` | Read/store a complete payload; text defaults to UTF-8 |
| `read_parquet(path, columns=None)` | Return a PyArrow table; optionally project columns |
| `write_parquet(table, path)` | Write a PyArrow table using `pyarrow.parquet.write_table` |
| `exists(path)` | Filesystem existence locally; exact object existence remotely |
| `size(path)` | File or object bytes; missing resources raise `FileNotFoundError` |
| `makedirs(path)` | Create local directories; a no-op for every object backend |
| `list_files(path, pattern="*")` | Sorted immediate children matched by a glob pattern; may include child directories/prefixes |
| `get_directory_structure(path)` | Recursively construct nested dictionaries, with files represented by `None` |
| `copy(src_path, dst_path)` | Copy within the same storage backend |
| `delete(path)` | Delete an exact file/object, or a directory/prefix when no exact object exists |
| `delete_tree(path)` | Walk descendant objects and remove them; also remove an exact root object or remaining local directory; return a removal count |

Projection deliberately intersects requested names with available columns. If that intersection is empty, the implementation reads all columns. Passing `[]` also reads all columns. Callers needing strict schema validation must handle it before this method.

The remote `read_parquet` implementations download the complete object into memory before applying the PyArrow projection. Their `write_parquet` implementations also construct an in-memory Parquet buffer. These adapter methods do not provide streaming range reads. Query engines can use a separate direct path or signed URL where supported.

Remote listings include the configured base prefix in returned strings, while read/write methods add the prefix themselves. Do not blindly pass a returned listing path back into those methods when a prefix is configured. `delete_tree()` reconstructs logical descendant paths from `get_directory_structure()` to avoid this issue.

Remote `exists("folder")` can be false while objects exist under `folder/`. No object backend creates directory marker objects in `makedirs()`.

## Local filesystem behavior

[LocalStorage](../supertable/storage/local_storage.py) writes JSON to a temporary file in the destination directory, flushes and synchronizes the file, then replaces the destination with `os.replace()`. It attempts to synchronize the directory afterward. JSON reads retry empty files, parse failures, and files that disappear during the read up to five attempts with a 20 ms delay.

Parquet, bytes, and text writes write directly to the destination path; they do not use the JSON replacement procedure. Local Parquet reads disable Hive partition inference with `partitioning=None`. Copies use `shutil.copyfile`.

## S3

[S3Storage](../supertable/storage/s3_storage.py) accepts an injected client or creates a Boto3 S3 client. Without explicit credentials, it leaves credential resolution to Boto3. An endpoint without a scheme is normalized to HTTPS; a bucket prefix in the endpoint hostname is removed before client construction.

`STORAGE_FORCE_PATH_STYLE=true` selects path-style addressing when constructing from settings; otherwise virtual-host addressing is selected. S3 construction does not create the bucket. The implementation probes bucket location when needed and retries supported redirect/region errors once after rebuilding its client. A retried upload rewinds a seekable body; it refuses to retry a non-rewindable body.

Object reads close the response body after reading. Prefix deletion uses batches of at most 1,000 objects and checks the returned per-object errors. Partial deletion raises `OSError`. Copies use S3 `copy_object`.

## MinIO

[MinioStorage](../supertable/storage/minio_storage.py) requires `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY`, and `STORAGE_SECRET_KEY` in `from_env()`. The endpoint must start with `http://` or `https://`; its scheme determines transport security. The configured bucket is checked and created if absent. Region-mismatch handling can rebuild the client using credentials retained during environment construction.

An injected-client constructor requires `bucket_name` and `client`; it does not run the environment constructor's bucket creation and endpoint setup. Object reads close and release the connection. Prefix deletion consumes the MinIO removal error iterator and raises if any objects failed. Copies use the MinIO copy API.

## Azure Blob Storage

[AzureBlobStorage](../supertable/storage/azure_storage.py) resolves:

1. Container from `STORAGE_BUCKET`, then `AZURE_CONTAINER`, then `supertable`.
2. Endpoint from `STORAGE_ENDPOINT_URL`, then `AZURE_BLOB_ENDPOINT`; otherwise from `AZURE_STORAGE_ACCOUNT`.
3. Credentials from connection string, then `STORAGE_ACCESS_KEY`/`AZURE_STORAGE_KEY`, then SAS token, then `DefaultAzureCredential`.

The default `STORAGE_BUCKET=supertable` takes precedence over `AZURE_CONTAINER`. Set `STORAGE_BUCKET` explicitly to select an Azure container through the normal settings loader.

The adapter can also parse an `abfss://<container>@<account>.dfs.core.windows.net/<prefix>` value from `SUPERTABLE_HOME` for missing account/endpoint/prefix values. The effective container has already been resolved, so the URI container does not override it. Application-home resolution separately treats `SUPERTABLE_HOME` as a filesystem path; explicit Azure settings keep those two responsibilities clear.

The default-credential branch imports `azure.identity`; the declared `azure` package extra lists `azure-storage-blob`, so this branch also needs `azure.identity` available in the environment. The constructor obtains an existing container client and does not create the container. Writes overwrite blobs. `copy()` downloads the source bytes and uploads them to the destination.

## Google Cloud Storage

[GCSStorage](../supertable/storage/gcp_storage.py) resolves bucket from `GCS_BUCKET`, then `STORAGE_BUCKET`, then `supertable`. `from_env()` uses an existing `GOOGLE_APPLICATION_CREDENTIALS` file first, then inline `GCP_SA_JSON`, then default Google credentials. `GCP_PROJECT` is passed to the client when set.

The constructor obtains a bucket handle and does not create the bucket. Copies use the bucket copy API. `delete_prefix(path)` is an additional GCS method that removes descendants without requiring an exact object to exist.

## Engine paths and signed reads

Object adapters implement `to_duckdb_path(key, prefer_httpfs=None)` and `presign(key, expiry_seconds=3600)`. If `prefer_httpfs` is omitted, `SUPERTABLE_DUCKDB_USE_HTTPFS` selects URI style:

| Backend | Default path | HTTP-style path |
| --- | --- | --- |
| S3 | `s3://<bucket>/<key>` | Configured endpoint with virtual-host or path addressing |
| MinIO | `s3://<bucket>/<key>` | `<endpoint>/<bucket>/<key>` |
| Azure | `azure://<container>/<key>` | Blob-service URL, container, key |
| GCS | `gcs://<bucket>/<key>` | `https://storage.googleapis.com/<bucket>/<key>` |

The configured prefix is included in these paths. HTTP-style paths are ordinary resource URLs, not automatically signed URLs. `presign()` separately creates a read URL using provider credentials: S3 GET signing, MinIO GET signing, Azure read SAS, or GCS V4 GET signing. Azure can request a user-delegation key when an account key is unavailable. The provider may reject signing if the credential lacks the required capability.

`LocalStorage` inherits the interface's `NotImplementedError` for both optional methods; query code resolves local filesystem paths separately. A method that formats a cloud URI does not itself install a query-engine extension or configure authentication. See [query engines](09_query_engine.md).

## Deletion and consistency boundaries

`delete_tree()` removes descendants before an exact root object, including the case where both exist. It ignores `FileNotFoundError` for already-removed entries but propagates other errors. The count describes removals reported by this traversal; it is not a remote transaction result.

`SuperTable.delete()` removes storage first and then asks Redis to delete the namespace. `SimpleTable.delete()` follows the same storage-first ordering for its table. Neither operation is a transaction across storage and Redis. A storage failure can leave some objects removed while metadata remains. The storage interface provides no distributed transaction, compare-and-swap object write, or automatic rollback.
