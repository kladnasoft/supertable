# Format mirroring

Mirroring writes alternate representations of a table's current physical Parquet resources. Configuration applies to a whole SuperTable; the enabled names are `DELTA`, `ICEBERG`, and `PARQUET`. All are disabled when no Redis mirror configuration exists.

Implementation: [mirror_formats.py](../supertable/mirroring/mirror_formats.py), [mirror_delta.py](../supertable/mirroring/mirror_delta.py), [mirror_iceberg.py](../supertable/mirroring/mirror_iceberg.py), [mirror_parquet.py](../supertable/mirroring/mirror_parquet.py), and [DataWriter](../supertable/data_writer.py).

## 1. Configure a lake

```python
from supertable import SuperTable
from supertable.mirroring.mirror_formats import MirrorFormats

lake = SuperTable(
    super_name="warehouse",
    organization="acme",
    create_if_missing=False,
)
MirrorFormats.set_with_lock(lake, ["PARQUET", "DELTA"])
MirrorFormats.enable_with_lock(lake, "ICEBERG")
print(MirrorFormats.get_enabled(lake))
MirrorFormats.disable_with_lock(lake, "DELTA")
```

`set_with_lock` replaces the complete list, while `enable_with_lock` and `disable_with_lock` modify one format. All return the resulting list. Names are uppercased, duplicates removed, and unknown names ignored. Pass string values, such as `"DELTA"` or `FormatMirror.DELTA.value`. Passing an empty list disables every format.

The catalog stores `{"formats": [...], "ts": <milliseconds>}` at the lake's mirror key. Despite the method names, these configuration helpers do not acquire a lock or check a role. The enable/disable operations read and then replace the list, so concurrent updates can overwrite one another. These are trusted Python administration APIs.

Setting a format only changes configuration. It does not copy existing tables immediately, schedule a background job, or remove files belonging to formats that have been disabled.

`SimpleTable.delete` removes the native `tables/<name>` tree; it does not remove the sibling `parquet/<name>`, `delta/<name>`, or `iceberg/<name>` mirror directories. Deleting the entire SuperTable removes its enclosing storage tree, including those mirrors.

## 2. Publication order

A successful `DataWriter.write` publishes the native snapshot and catalog pointers, then calls `MirrorFormats.mirror_if_enabled`. `DataWriter.compact` also mirrors when it publishes a changed snapshot; a compaction with nothing to change does not rebuild a mirror.

The calls execute synchronously before the table write lock is released. Enabled writers run in the fixed order Delta, Iceberg, then Parquet. A raised exception stops that sequence; `DataWriter` logs the mirroring error and continues returning the native write result. The catalog commit is not rolled back and there is no persistent mirror status, retry queue, or transaction spanning all formats.

An administrator with a current snapshot dictionary can explicitly rebuild its enabled representations:

```python
MirrorFormats.mirror_if_enabled(
    super_table=lake,
    table_name="orders",
    simple_snapshot=current_snapshot,
)
```

Here `current_snapshot` must contain the current `resources` and schema metadata obtained from the native table. The optional `mirrors=[...]` argument bypasses reading the configured format list for that call. It does not normalize those supplied names or perform authorization.

## 3. Storage layout

All paths below are relative to the storage backend root.

| Format | Table location | Published files |
| --- | --- | --- |
| Parquet | `<org>/<super>/parquet/<table>/` | `files/<path-hash>_<source-name>.parquet` |
| Delta | `<org>/<super>/delta/<table>/` | Copied `files/` and `_delta_log/<20-digit-snapshot-version>.json` |
| Iceberg standard writer | `<org>/<super>/iceberg/<table>/` | Copied `data/`, Avro files and `v<version>.metadata.json` under `metadata/`, `version-hint.text`, `latest.json` |
| Iceberg fallback | Same Iceberg location | `manifests/<20-digit-version>.json`, `metadata/<20-digit-version>.json`, `latest.json` |

Data-copy helpers try storage copy operations and then byte reads/writes. Parquet and Delta additionally attempt a MinIO client-side `copy_object` call when that client shape is available. Destination names use the first eight hexadecimal characters of an MD5 of the source path plus its basename.

## 4. Parquet behavior

The Parquet writer copies each current resource into `files/`, reusing an existing destination path when present. It lists the directory to identify files no longer referenced, and deletes obsolete files after copying current ones. Listing failures are ignored and deletion failures only produce warnings.

This is a directory of physical data files with no transaction log. Readers can observe intermediate states while files are copied and removed. No historical snapshot index is maintained.

## 5. Delta behavior

Each commit contains `commitInfo`, protocol, metadata, removals, and additions as newline-delimited JSON. The log filename uses the native snapshot's `snapshot_version` directly. Native snapshots increment independently of when mirroring is enabled, so enabling Delta on an existing table does not generate missing earlier versions or a version-zero bootstrap.

The protocol declares `minReaderVersion=1` and `minWriterVersion=4`. The metadata contains an unpartitioned schema, a stable table ID, and configuration strings for change data feed and automatic optimization. Those strings do not cause this writer to run a change-data-feed pipeline or an optimizer. It writes no dedicated change-data-feed files.

The writer obtains schema from `schemaString`, `schema_string`, or the snapshot's schema list, with Parquet inference as a fallback. It maps primitive Arrow types, includes resource sizes, and converts available statistics into Delta statistics JSON.

Every current resource is copied again and emitted as an `add` action, even when it existed in the previous mirror directory. Files absent from the current resource set are physically removed before the JSON commit is written. This removes data needed by older log versions; historical reads are not preserved. An existing commit filename is not rewritten, but that check occurs after data copying and obsolete-file deletion. Checkpoint generation is disabled by the module's `WRITE_CHECKPOINT=False` constant.

The implementation writes these artifacts directly through the storage interface. It does not use a Delta transaction library, maintain a contiguous independent Delta version counter, or verify interoperability with an external reader during publication.

## 6. Iceberg behavior

The standard writer produces Iceberg format-version 2 metadata, a binary Avro manifest, and an Avro manifest list. Metadata version is native `snapshot_version + 1`; each invocation generates a new random snapshot ID. It preserves column IDs for names found in the previous metadata selected by `version-hint.text`, and preserves the prior table UUID and location when readable.

The table is unpartitioned and has no sort order. Primitive type conversion is limited; unsupported types become `string`. Data-file statistics fields are left null. Each metadata file lists one replacement snapshot, with no parent snapshot and an empty metadata history. Old data and metadata artifacts are not garbage-collected by this writer.

It first tries to copy data into the mirror's `data/` directory. If copying a resource fails, it logs a warning and references the original resource path. Paths already containing a URI scheme are kept; other paths become `s3://<bucket>/<path>` when storage exposes a bucket, or remain plain paths otherwise. There is no catalog registration or backend-specific URI validation.

If any standard-writing step raises, the public `write_iceberg_table` logs the failure and writes the fallback JSON representation. This fallback records resource paths in a custom manifest and identifies itself as `iceberg-lite`. It does not provide the standard Avro manifest structure, and a fallback can leave partially written standard artifacts beside the JSON output. Inspect `latest.json` and the generated metadata to determine what was published.

## 7. Logical and access-control limits

All mirror writers consume snapshot `resources`. They do not apply the native snapshot's tombstone/deletion-vector metadata, run row filters, project allowed columns, or remove internal physical columns. A mirrored resource can therefore include logically deleted rows until native compaction physically removes them. A successful native query and a scan of mirror files can return different rows.

Mirrors also do not enforce SuperTable RBAC when an external engine reads the files directly. Storage access and external-engine permissions must be configured independently. These representations are derived artifacts; the native snapshot and catalog remain the publication point for SuperTable reads. See [writing and compaction](06_data_writer.md) and [RBAC](11_rbac.md) for those behaviors.
