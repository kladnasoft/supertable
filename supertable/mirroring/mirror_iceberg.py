# supertable/mirroring/mirror_iceberg.py

import os
import uuid
from typing import Dict, Any, List
from datetime import datetime, timezone

from supertable.config.defaults import logger


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _stable_table_uuid(organization: str, super_name: str, table_name: str) -> str:
    # Stable UUID from logical identity (no need to persist a separate uuid file)
    seed = f"st://{organization}/{super_name}/{table_name}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def _schema_fields_from_catalog(schema_any: Any) -> List[Dict[str, Any]]:
    """
    Expecting SuperTable's collect_schema output (usually a list of dicts).
    We map minimally to Iceberg-ish fields. If unknown, we pass through name/type.
    """
    fields = []
    if isinstance(schema_any, list):
        for idx, col in enumerate(schema_any):
            # col can be {"name": "...", "type": "..."} or similar
            name = col.get("name") if isinstance(col, dict) else None
            typ = col.get("type") if isinstance(col, dict) else None
            if not name:
                continue
            fields.append({
                "id": idx + 1,
                "name": name,
                "required": False,
                "type": typ or "string",
            })
    return fields


def write_iceberg_table(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:
    """
    'Iceberg-lite' mirror:
      - metadata/<version>.json with a minimal, current-only spec
      - manifests/<version>.json listing data files (we don't produce true Avro manifests)
    """
    base = os.path.join(super_table.organization, super_table.super_name, "iceberg", table_name)
    metadata_dir = os.path.join(base, "metadata")
    manifests_dir = os.path.join(base, "manifests")
    super_table.storage.makedirs(metadata_dir)
    super_table.storage.makedirs(manifests_dir)

    version = int(simple_snapshot.get("snapshot_version", 0))
    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", [])
    schema_any = simple_snapshot.get("schema", [])

    now_ms = _now_ms()
    table_uuid = _stable_table_uuid(super_table.organization, super_table.super_name, table_name)
    fields = _schema_fields_from_catalog(schema_any)

    # "Manifest list" (JSON, not Avro) – latest only
    manifest_list_path = os.path.join(manifests_dir, f"{version:020d}.json")
    manifest_payload = {
        "version": version,
        "generated_at_ms": now_ms,
        "data_files": [
            {"path": r["file"], "file_size": int(r.get("file_size", 0)), "format": "parquet"}
            for r in resources
        ],
    }
    super_table.storage.write_json(manifest_list_path, manifest_payload)

    # Minimal metadata – latest only
    metadata_path = os.path.join(metadata_dir, f"{version:020d}.json")
    metadata_payload = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": base,
        "last-sequence-number": version,
        "last-updated-ms": now_ms,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": fields,
            }
        ],
        "current-schema-id": 0,
        "partition-spec": [],
        "default-spec-id": 0,
        "properties": {
            "created-by": "supertable",
            "mirror": "iceberg-lite",
        },
        "current-snapshot-id": version,
        "snapshots": [
            {
                "snapshot-id": version,
                "sequence-number": version,
                "timestamp-ms": now_ms,
                "summary": {"operation": "replace"},
                "manifest-list": manifest_list_path,
            }
        ],
    }
    super_table.storage.write_json(metadata_path, metadata_payload)

    # Convenience pointer
    super_table.storage.write_json(
        os.path.join(base, "latest.json"),
        {"version": version, "metadata": metadata_path, "manifest_list": manifest_list_path},
    )

    logger.debug(f"[mirror][iceberg] wrote {metadata_path} with {len(resources)} files")



# ---------------------------------------------------------------------------
# Standard Iceberg writer (v2 metadata + Avro manifests)
# ---------------------------------------------------------------------------
# Notes:
# - We keep the existing "iceberg-lite" JSON mirror above for backward compatibility.
# - The standard writer writes Iceberg V2 metadata and Avro manifests under:
#     <org>/<super>/iceberg/<table>/{data,metadata}
#   and updates metadata/version-hint.text to point to vN.metadata.json.
#
# References:
# - Iceberg spec: https://iceberg.apache.org/spec/
#   (manifest list fields, manifest entry/data file fields, table metadata fields)

import json as _json
import struct as _struct
import uuid as _uuid
from typing import Optional, Tuple


_AVRO_MAGIC = b"Obj\x01"


def _zigzag(n: int) -> int:
    # zigzag for signed longs
    return (n << 1) ^ (n >> 63)


def _encode_varint(n: int) -> bytes:
    # LEB128 encoding for unsigned int
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _encode_long(n: int) -> bytes:
    return _encode_varint(_zigzag(int(n)))


def _encode_int(n: int) -> bytes:
    return _encode_long(int(n))


def _encode_bool(v: bool) -> bytes:
    return b"\x01" if v else b"\x00"


def _encode_bytes(b: bytes) -> bytes:
    return _encode_long(len(b)) + b


def _encode_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return _encode_bytes(b)


def _encode_float(v: float) -> bytes:
    return _struct.pack("<f", float(v))


def _encode_double(v: float) -> bytes:
    return _struct.pack("<d", float(v))


def _encode_map_str_keys(val: Dict[str, Any], value_schema: Any) -> bytes:
    # Avro maps have string keys.
    if not val:
        return _encode_long(0)
    items = list(val.items())
    out = bytearray()
    out.extend(_encode_long(len(items)))
    for k, v in items:
        out.extend(_encode_string(str(k)))
        out.extend(_encode_datum(value_schema, v))
    out.extend(_encode_long(0))
    return bytes(out)


def _encode_array(items: List[Any], item_schema: Any) -> bytes:
    if not items:
        return _encode_long(0)
    out = bytearray()
    out.extend(_encode_long(len(items)))
    for it in items:
        out.extend(_encode_datum(item_schema, it))
    out.extend(_encode_long(0))
    return bytes(out)


def _normalize_schema(schema: Any) -> Any:
    # Allow schema to be referenced by name (string) inside the same file.
    return schema


def _encode_union(schemas: List[Any], value: Any) -> bytes:
    # Choose branch based on value type / None.
    if value is None:
        # prefer 'null' if present
        for idx, s in enumerate(schemas):
            if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
                return _encode_long(idx)
        # otherwise first branch
        return _encode_long(0) + _encode_datum(schemas[0], value)

    # Non-null: pick first non-null branch that matches roughly.
    for idx, s in enumerate(schemas):
        if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
            continue
        return _encode_long(idx) + _encode_datum(s, value)
    # fallback
    return _encode_long(0) + _encode_datum(schemas[0], value)


def _encode_record(schema: Dict[str, Any], value: Dict[str, Any]) -> bytes:
    out = bytearray()
    for f in schema.get("fields", []):
        name = f["name"]
        f_schema = f["type"]
        out.extend(_encode_datum(f_schema, value.get(name)))
    return bytes(out)


def _encode_datum(schema: Any, value: Any) -> bytes:
    schema = _normalize_schema(schema)
    if isinstance(schema, list):
        return _encode_union(schema, value)
    if isinstance(schema, str):
        if schema == "null":
            return b""
        if schema == "boolean":
            return _encode_bool(bool(value))
        if schema == "int":
            return _encode_int(int(value))
        if schema == "long":
            return _encode_long(int(value))
        if schema == "float":
            return _encode_float(float(value))
        if schema == "double":
            return _encode_double(float(value))
        if schema == "bytes":
            return _encode_bytes(value if isinstance(value, (bytes, bytearray)) else bytes(value))
        if schema == "string":
            return _encode_string("" if value is None else str(value))
        raise ValueError(f"Unsupported primitive schema: {schema}")

    if isinstance(schema, dict):
        t = schema.get("type")
        if isinstance(t, list):
            return _encode_union(t, value)
        if t == "record":
            return _encode_record(schema, value or {})
        if t == "array":
            return _encode_array(value or [], schema["items"])
        if t == "map":
            return _encode_map_str_keys(value or {}, schema["values"])
        if t == "fixed":
            size = int(schema["size"])
            b = value if isinstance(value, (bytes, bytearray)) else bytes(value)
            if len(b) != size:
                raise ValueError(f"Fixed size mismatch: expected {size}, got {len(b)}")
            return bytes(b)
        raise ValueError(f"Unsupported complex schema type: {t}")

    raise ValueError(f"Unsupported schema: {schema!r}")


def _encode_avro_header(schema: Dict[str, Any], metadata: Dict[str, bytes]) -> Tuple[bytes, bytes]:
    schema_json = _json.dumps(schema, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    meta_map: Dict[str, bytes] = {"avro.schema": schema_json, **metadata}

    out = bytearray()
    out.extend(_AVRO_MAGIC)

    # metadata map: map<string, bytes>
    out.extend(_encode_map_str_keys(meta_map, "bytes"))

    sync = _uuid.uuid4().bytes  # 16 bytes
    out.extend(sync)
    return bytes(out), sync


def _avro_ocf_dump(schema: Dict[str, Any], records: List[Dict[str, Any]], metadata: Optional[Dict[str, bytes]] = None) -> bytes:
    metadata = metadata or {}
    header, sync = _encode_avro_header(schema, metadata)
    body = bytearray()

    # Single data block
    block_payload = bytearray()
    for r in records:
        block_payload.extend(_encode_datum(schema, r))

    body.extend(_encode_long(len(records)))
    body.extend(_encode_long(len(block_payload)))
    body.extend(block_payload)
    body.extend(sync)

    return header + bytes(body)


def _new_snapshot_id() -> int:
    # Similar to Iceberg's Java reference suggestion: XOR halves of uuid4, keep positive.
    u = _uuid.uuid4().bytes
    hi = int.from_bytes(u[:8], "big", signed=False)
    lo = int.from_bytes(u[8:], "big", signed=False)
    val = (hi ^ lo) & ((1 << 63) - 1)
    # ensure non-zero
    return val or 1


def _storage_path_to_uri(storage: Any, object_path: str) -> str:
    # Iceberg requires file_path to be a full URI with FS scheme.
    if "://" in object_path:
        return object_path
    bucket = getattr(storage, "bucket", None) or getattr(storage, "bucket_name", None)
    if isinstance(bucket, str) and bucket:
        return f"s3://{bucket}/{object_path.lstrip('/')}"
    return object_path


def _binary_copy_if_possible(storage: Any, src_path: str, dst_path: str) -> None:
    storage.makedirs(os.path.dirname(dst_path))

    if hasattr(storage, "copy"):
        try:
            storage.copy(src_path, dst_path)
            return
        except Exception as e:
            logger.warning(f"[mirror][iceberg] storage.copy failed ({src_path} -> {dst_path}): {e}")

    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.write_bytes(dst_path, read_bytes(src_path))
            return
        except Exception as e:
            logger.warning(f"[mirror][iceberg] byte copy failed ({src_path} -> {dst_path}): {e}")

    raise RuntimeError("storage does not support copy() or read_bytes/write_bytes for Iceberg mirroring")


def _read_json_if_exists(storage: Any, path: str) -> Optional[Dict[str, Any]]:
    read_json = getattr(storage, "read_json", None)
    if callable(read_json):
        try:
            return read_json(path)
        except Exception:
            return None
    read_bytes = getattr(storage, "read_bytes", None)
    if callable(read_bytes):
        try:
            return _json.loads(read_bytes(path).decode("utf-8"))
        except Exception:
            return None
    return None


def _read_text_if_exists(storage: Any, path: str) -> Optional[str]:
    read_bytes = getattr(storage, "read_bytes", None)
    if callable(read_bytes):
        try:
            return read_bytes(path).decode("utf-8")
        except Exception:
            return None
    return None


def _iceberg_type_from_any(t: Any) -> str:
    # Normalize common/legacy type spellings into Iceberg types.
    if t is None:
        return "string"
    s = str(t).strip().lower()
    if s in {"str", "string", "utf8", "varchar", "text"}:
        return "string"
    if s in {"int8", "int16", "int32", "integer", "int"}:
        return "int"
    if s in {"int64", "long", "bigint"}:
        return "long"
    if s in {"float32", "float"}:
        return "float"
    if s in {"float64", "double"}:
        return "double"
    if s in {"bool", "boolean"}:
        return "boolean"
    if s in {"date"}:
        return "date"
    if s in {"timestamp", "datetime", "timestamp[us]", "timestamp_us"}:
        return "timestamp"
    # Polars / Arrow dtype repr variants
    if "datetime(" in s or "timestamp(" in s:
        return "timestamp"
    return "string"


def _iceberg_schema_from_snapshot(
    snapshot_schema: Any,
    prior_field_ids: Optional[Dict[str, int]] = None,
    start_id: int = 1,
) -> Tuple[Dict[str, Any], int, Dict[str, int]]:
    # snapshot_schema is typically a list of {"name":..., "type":...}
    prior_field_ids = prior_field_ids or {}
    fields: List[Dict[str, Any]] = []
    next_id = max([start_id - 1, *prior_field_ids.values()]) + 1 if prior_field_ids else start_id
    name_to_id: Dict[str, int] = dict(prior_field_ids)

    if isinstance(snapshot_schema, dict):
        # some callers may provide {"fields":[...]}
        snapshot_schema = snapshot_schema.get("fields", [])

    if not isinstance(snapshot_schema, list):
        snapshot_schema = []

    for col in snapshot_schema:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name", "")).strip()
        if not name:
            continue
        if name not in name_to_id:
            name_to_id[name] = next_id
            next_id += 1
        fields.append(
            {
                "id": int(name_to_id[name]),
                "name": name,
                "required": False,
                "type": _iceberg_type_from_any(col.get("type")),
            }
        )

    last_column_id = max(name_to_id.values(), default=0)
    schema_obj = {"type": "struct", "schema-id": 0, "fields": fields}
    return schema_obj, last_column_id, name_to_id


def _load_prior_schema_ids(storage: Any, metadata_dir: str) -> Tuple[Dict[str, int], Optional[str], Optional[str], int]:
    # Returns (name->id, table_uuid, base_location, last_sequence_number)
    hint_path = os.path.join(metadata_dir, "version-hint.text")
    hint = _read_text_if_exists(storage, hint_path)
    if hint is None:
        return {}, None, None, 0
    try:
        v = int(hint.strip())
    except Exception:
        return {}, None, None, 0
    meta_path = os.path.join(metadata_dir, f"v{v}.metadata.json")
    meta = _read_json_if_exists(storage, meta_path) or {}
    table_uuid = meta.get("table-uuid")
    base_location = meta.get("location")
    last_seq = int(meta.get("last-sequence-number", 0) or 0)
    name_to_id: Dict[str, int] = {}
    # Prefer v2 schemas list, fall back to deprecated schema
    schemas = meta.get("schemas") or []
    schema0 = schemas[0] if isinstance(schemas, list) and schemas else meta.get("schema") or {}
    for f in schema0.get("fields", []) if isinstance(schema0, dict) else []:
        try:
            name_to_id[str(f["name"])] = int(f["id"])
        except Exception:
            continue
    return name_to_id, table_uuid, base_location, last_seq


def _manifest_file_avro_schema(partition_record_name: str = "partition") -> Dict[str, Any]:
    # Minimal v2 manifest schema (data files only, unpartitioned).
    partition_schema = {"type": "record", "name": partition_record_name, "fields": []}

    data_file = {
        "type": "record",
        "name": "data_file",
        "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "type": partition_schema, "field-id": 102},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": None, "field-id": 108},
            {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": None, "field-id": 109},
            {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": None, "field-id": 110},
            {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": None, "field-id": 137},
            {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": None, "field-id": 125},
            {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": None, "field-id": 128},
            {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 131},
            {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": None, "field-id": 132},
            {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": None, "field-id": 135},
            {"name": "sort_order_id", "type": ["null", "int"], "default": None, "field-id": 140},
            {"name": "first_row_id", "type": ["null", "long"], "default": None, "field-id": 142},
            {"name": "referenced_data_file", "type": ["null", "string"], "default": None, "field-id": 143},
            {"name": "content_offset", "type": ["null", "long"], "default": None, "field-id": 144},
            {"name": "content_size_in_bytes", "type": ["null", "long"], "default": None, "field-id": 145},
        ],
    }

    manifest_entry = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": None, "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "default": None, "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": None, "field-id": 4},
            {"name": "data_file", "type": data_file, "field-id": 2},
        ],
    }
    return manifest_entry


def _manifest_list_avro_schema() -> Dict[str, Any]:
    field_summary = {
        "type": "record",
        "name": "field_summary",
        "fields": [
            {"name": "contains_null", "type": "boolean", "field-id": 509},
            {"name": "contains_nan", "type": ["null", "boolean"], "default": None, "field-id": 518},
            {"name": "lower_bound", "type": ["null", "bytes"], "default": None, "field-id": 510},
            {"name": "upper_bound", "type": ["null", "bytes"], "default": None, "field-id": 511},
        ],
    }

    manifest_file = {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_files_count", "type": "int", "field-id": 504},
            {"name": "existing_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
            {"name": "partitions", "type": ["null", {"type": "array", "items": field_summary}], "default": None, "field-id": 507},
            {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 519},
            {"name": "first_row_id", "type": ["null", "long"], "default": None, "field-id": 520},
        ],
    }
    return manifest_file


def _write_iceberg_standard(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:
    base = os.path.join(super_table.organization, super_table.super_name, "iceberg", table_name)
    metadata_dir = os.path.join(base, "metadata")
    data_dir = os.path.join(base, "data")

    storage = super_table.storage
    storage.makedirs(metadata_dir)
    storage.makedirs(data_dir)

    # Use Supertable snapshot version as Iceberg metadata version (monotonic).
    snapshot_version = int(simple_snapshot.get("snapshot_version", 0))
    metadata_version = snapshot_version + 1
    now_ms = _now_ms()

    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", []) or []
    schema_any = simple_snapshot.get("schema", []) or []

    # Load prior schema IDs (best-effort for stable Iceberg ids)
    prior_ids, prior_uuid, prior_location, prior_last_seq = _load_prior_schema_ids(storage, metadata_dir)
    table_uuid = prior_uuid or _stable_table_uuid(super_table.organization, super_table.super_name, table_name)

    schema_obj, last_column_id, name_to_id = _iceberg_schema_from_snapshot(schema_any, prior_field_ids=prior_ids, start_id=1)

    # Base location should be a URI when possible
    base_location = prior_location or _storage_path_to_uri(storage, base)

    # Derive a sequence number for snapshot ordering (v2 requires it)
    last_seq = max(int(prior_last_seq), snapshot_version)
    sequence_number = last_seq + 1 if prior_last_seq else metadata_version

    # Copy data files into Iceberg table's data/ folder (immutable history friendly).
    copied_files: List[Tuple[str, str, Dict[str, Any]]] = []
    for r in resources:
        src = str(r.get("file", "")).strip()
        if not src:
            continue
        fname = src.split("/")[-1]
        dst = os.path.join(data_dir, fname)
        try:
            _binary_copy_if_possible(storage, src, dst)
        except Exception as e:
            logger.warning(f"[mirror][iceberg] data copy failed ({src} -> {dst}): {e}")
            # Still proceed by referencing the original file path as a fallback.
            dst = src
        copied_files.append((src, dst, r))

    snapshot_id = _new_snapshot_id()
    manifest_uuid = _uuid.uuid4().hex
    manifest_path_obj = os.path.join(metadata_dir, f"{manifest_uuid}.avro")
    manifest_path_uri = _storage_path_to_uri(storage, manifest_path_obj)

    # Build manifest entries
    manifest_entry_schema = _manifest_file_avro_schema()
    entries: List[Dict[str, Any]] = []
    for _, dst_obj, r in copied_files:
        rows = int(r.get("rows", r.get("record_count", 0)) or 0)
        size = int(r.get("file_size", r.get("size", 0)) or 0)
        entries.append(
            {
                "status": 1,
                "snapshot_id": snapshot_id,
                "sequence_number": None,
                "file_sequence_number": None,
                "data_file": {
                    "content": 0,
                    "file_path": _storage_path_to_uri(storage, dst_obj),
                    "file_format": "parquet",
                    "partition": {},
                    "record_count": rows,
                    "file_size_in_bytes": size,
                    "column_sizes": None,
                    "value_counts": None,
                    "null_value_counts": None,
                    "nan_value_counts": None,
                    "lower_bounds": None,
                    "upper_bounds": None,
                    "key_metadata": None,
                    "split_offsets": None,
                    "equality_ids": None,
                    "sort_order_id": None,
                    "first_row_id": None,
                    "referenced_data_file": None,
                    "content_offset": None,
                    "content_size_in_bytes": None,
                },
            }
        )

    # Manifest file header metadata required by Iceberg (v2)
    manifest_header_meta = {
        "schema": _json.dumps(schema_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        "schema-id": b"0",
        "partition-spec": b"[]",
        "partition-spec-id": b"0",
        "format-version": b"2",
        "content": b"data",
    }
    manifest_bytes = _avro_ocf_dump(manifest_entry_schema, entries, metadata=manifest_header_meta)
    write_bytes = getattr(storage, "write_bytes", None)
    if not callable(write_bytes):
        raise RuntimeError("storage.write_bytes is required for standard Iceberg mirroring")
    write_bytes(manifest_path_obj, manifest_bytes)
    manifest_length = len(manifest_bytes)

    # Manifest list
    manifest_list_uuid = _uuid.uuid4().hex
    manifest_list_obj = os.path.join(metadata_dir, f"snap-{snapshot_id}-{metadata_version}-{manifest_list_uuid}.avro")
    manifest_list_uri = _storage_path_to_uri(storage, manifest_list_obj)

    added_files = len(entries)
    added_rows = sum(int(e["data_file"]["record_count"]) for e in entries)
    manifest_list_schema = _manifest_list_avro_schema()
    manifest_list_records = [
        {
            "manifest_path": manifest_path_uri,
            "manifest_length": manifest_length,
            "partition_spec_id": 0,
            "content": 0,
            "sequence_number": int(sequence_number),
            "min_sequence_number": int(sequence_number),
            "added_snapshot_id": snapshot_id,
            "added_files_count": int(added_files),
            "existing_files_count": 0,
            "deleted_files_count": 0,
            "added_rows_count": int(added_rows),
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
            "partitions": None,
            "key_metadata": None,
            "first_row_id": None,
        }
    ]
    manifest_list_bytes = _avro_ocf_dump(manifest_list_schema, manifest_list_records, metadata={})
    write_bytes(manifest_list_obj, manifest_list_bytes)

    # Iceberg v2 metadata json
    metadata_payload = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": base_location,
        "last-sequence-number": int(sequence_number),
        "last-updated-ms": int(now_ms),
        "last-column-id": int(last_column_id),
        "schemas": [schema_obj],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 1000,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "properties": {
            "created-by": "supertable",
            "write.format.default": "parquet",
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": [
            {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": None,
                "sequence-number": int(sequence_number),
                "timestamp-ms": int(now_ms),
                "summary": {"operation": "replace"},
                "manifest-list": manifest_list_uri,
                "schema-id": 0,
            }
        ],
        "snapshot-log": [{"timestamp-ms": int(now_ms), "snapshot-id": snapshot_id}],
        "metadata-log": [],
        "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
    }

    # Write vN.metadata.json and version-hint.text
    meta_path = os.path.join(metadata_dir, f"v{metadata_version}.metadata.json")
    storage.write_json(meta_path, metadata_payload)
    write_bytes(os.path.join(metadata_dir, "version-hint.text"), str(metadata_version).encode("utf-8"))

    # Convenience pointer (keep previous behavior, but point to the standard metadata)
    storage.write_json(
        os.path.join(base, "latest.json"),
        {
            "version": metadata_version,
            "metadata": meta_path,
            "manifest_list": manifest_list_obj,
            "iceberg": {"version_hint": os.path.join(metadata_dir, "version-hint.text")},
        },
    )

    logger.info(
        f"[mirror][iceberg] wrote standard v{metadata_version} "
        f"(files={len(entries)}, manifest={os.path.basename(manifest_path_obj)}, manifest_list={os.path.basename(manifest_list_obj)})"
    )


# Patch the original entrypoint to *also* attempt writing standard Iceberg.
_write_iceberg_table_iceberg_lite = write_iceberg_table


def write_iceberg_table(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:  # type: ignore[override]
    """
    Mirror as:
    1) Standard Iceberg v2 (metadata + Avro manifests) under metadata/ + data/
    2) Iceberg-lite JSON (legacy) under metadata/ + manifests/ (best-effort fallback)
    """
    try:
        _write_iceberg_standard(super_table, table_name, simple_snapshot)
    except Exception as e:
        logger.warning(f"[mirror][iceberg] standard writer failed, falling back to iceberg-lite: {e}")
    # Always write the legacy mirror for backward compatibility
    _write_iceberg_table_iceberg_lite(super_table, table_name, simple_snapshot)
