# supertable/mirroring/mirror_delta.py
"""
Delta Lake mirror (spec-compliant, latest-only projection)

Writes a proper Delta _delta_log with actions:
  - commitInfo
  - protocol
  - metaData
  - remove (for files removed vs previous mirror)
  - add (for current snapshot files)

Updates:
- Engine string set to: "Apache-Spark/3.4.3.5.3.20250511.1 Delta-Lake/2.4.0.24"
- metaData includes {"format":{"provider":"parquet","options":{}}} and a valid Spark StructType JSON schemaString.
- Do NOT write latest.json (removed).
- Parquet data files are **copied** under the table folder; obsolete ones are **deleted** from the table folder.
- Robust path normalization for listing/deleting co-located files (fixes erroneous deletes seen with abfss paths).
- Skip writing a commit if it would have no data changes (no adds and no removes).
"""

from __future__ import annotations

import io
import json
import os
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Set

from supertable.config.defaults import logger


# ---- Spark/Delta schema normalization helpers ----
_SPARK_TYPE_MAP = {
    "string": "string",
    "boolean": "boolean", "bool": "boolean",
    "byte": "byte", "short": "short",
    "integer": "integer", "int": "integer", "int32": "integer",
    "long": "long", "int64": "long", "bigint": "long",
    "float": "float", "double": "double",
    "decimal": "decimal",  # keep decimal(p,s) as-is
    "date": "date", "timestamp": "timestamp",
    "binary": "binary",
}

def _normalize_type(t: str) -> str:
    if not isinstance(t, str):
        return "string"
    ts = t.strip().lower()

    # Common non-Spark dtype strings we may get from Arrow/Polars helpers
    # Examples:
    #   "Datetime(time_unit='us', time_zone=None)"
    #   "datetime(time_unit='us', time_zone=none)"
    # Delta expects Spark SQL types, so normalize these to "timestamp".
    if ts.startswith("datetime") or "datetime(time_unit" in ts:
        return "timestamp"
    if ts.startswith("timestamp") or "timestamp(" in ts:
        return "timestamp"

    if ts.startswith("decimal(") and ts.endswith(")"):
        return ts
    return _SPARK_TYPE_MAP.get(ts, ts)


def _schema_to_structtype_json(schema_string: Any = None, schema_list: Any = None) -> str:
    """
    Return a valid Spark StructType JSON string for Delta metaData.schemaString.
    Prefers a provided schema_string (already in Spark StructType JSON) when present.
    Otherwise builds from a list of {name,type,(nullable,metadata)} dicts.
    """
    # If caller provided a schema_string, try to sanitize minimal types and return it
    if schema_string:
        try:
            parsed = json.loads(schema_string) if isinstance(schema_string, str) else schema_string
            if isinstance(parsed, dict):
                if isinstance(parsed.get("fields"), list):
                    for f in parsed["fields"]:
                        if isinstance(f, dict) and "type" in f:
                            if isinstance(f["type"], str):
                                f["type"] = _normalize_type(f["type"])
                            f.setdefault("nullable", True)
                            f.setdefault("metadata", {})
                    return json.dumps(parsed, separators=(",", ":"))
                if isinstance(parsed.get("fields"), dict):
                    fields_map = parsed["fields"]
                    fields = []
                    for name, typ in fields_map.items():
                        fields.append({"name": name, "type": _normalize_type(str(typ)), "nullable": True, "metadata": {}})
                    out = {"type": "struct", "fields": fields}
                    return json.dumps(out, separators=(",", ":"))
        except Exception:
            # fall through to schema_list
            pass

    if schema_list:
        fields = []
        for f in schema_list:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            typ = _normalize_type(str(f.get("type", "string")))
            fields.append({"name": name, "type": typ, "nullable": f.get("nullable", True), "metadata": f.get("metadata", {})})
        out = {"type": "struct", "fields": fields}
        return json.dumps(out, separators=(",", ":"))

    # empty struct fallback
    return json.dumps({"type": "struct", "fields": []}, separators=(",", ":"))



# ---- Best-effort schema inference (when snapshot lacks schema) ----

def _spark_type_from_pyarrow(pa_type: Any) -> str:
    """Map a PyArrow DataType to a Spark/Delta type string."""
    try:
        import pyarrow as pa  # type: ignore
    except Exception:  # pragma: no cover
        return "string"

    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "string"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if pa.types.is_int8(pa_type):
        return "byte"
    if pa.types.is_int16(pa_type):
        return "short"
    if pa.types.is_int32(pa_type):
        return "integer"
    if pa.types.is_int64(pa_type):
        return "long"
    if pa.types.is_uint8(pa_type):
        return "short"
    if pa.types.is_uint16(pa_type):
        return "integer"
    if pa.types.is_uint32(pa_type):
        return "long"
    if pa.types.is_uint64(pa_type):
        return "decimal(20,0)"
    if pa.types.is_float32(pa_type):
        return "float"
    if pa.types.is_float64(pa_type):
        return "double"
    if pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type):
        return "date"
    if pa.types.is_timestamp(pa_type):
        return "timestamp"
    if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
        return "binary"
    if pa.types.is_decimal(pa_type):
        return f"decimal({pa_type.precision},{pa_type.scale})"

    return "string"


def _schema_list_from_arrow_table(table: Any) -> List[Dict[str, Any]]:
    """Build a Delta-friendly schema list from a PyArrow Table."""
    try:
        schema = table.schema
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for field in schema:
        out.append(
            {
                "name": field.name,
                "type": _spark_type_from_pyarrow(field.type),
                "nullable": bool(getattr(field, "nullable", True)),
                "metadata": {},
            }
        )
    return out


def _infer_schema_list_from_any_parquet(storage, paths: List[str]) -> List[Dict[str, Any]]:
    """Best-effort: read schema from the first readable parquet file."""
    read_parquet = getattr(storage, "read_parquet", None)
    if callable(read_parquet):
        for p in paths:
            try:
                table = read_parquet(p)
                return _schema_list_from_arrow_table(table)
            except Exception:
                continue

    try:
        import pyarrow.parquet as pq  # type: ignore
        for p in paths:
            try:
                sch = pq.read_schema(p)
                fields: List[Dict[str, Any]] = []
                for f in sch:
                    fields.append(
                        {
                            "name": f.name,
                            "type": _spark_type_from_pyarrow(f.type),
                            "nullable": bool(getattr(f, "nullable", True)),
                            "metadata": {},
                        }
                    )
                return fields
            except Exception:
                continue
    except Exception:
        pass

    return []

# ---- Delta stats normalization helpers ----

def _normalize_delta_stats(stats: Any, *, num_records: Any = None) -> Optional[str]:
    """Return a Delta-compatible stats JSON string.

    Delta's add.stats is a JSON string with structure like:
        {"numRecords": N, "minValues": {...}, "maxValues": {...}, "nullCount": {...}}

    We accept legacy formats like:
        {"col": {"min": ..., "max": ...}, ...}
    and convert them.
    """
    if stats is None:
        return None

    # Accept JSON string
    if isinstance(stats, str):
        try:
            stats_obj = json.loads(stats)
        except Exception:
            return None
    else:
        stats_obj = stats

    if not isinstance(stats_obj, dict):
        return None

    # Already Delta-like
    if any(k in stats_obj for k in ("numRecords", "minValues", "maxValues", "nullCount")):
        out: Dict[str, Any] = dict(stats_obj)
        if num_records is not None and "numRecords" not in out:
            try:
                out["numRecords"] = int(num_records)
            except Exception:
                pass
        return json.dumps(out, separators=(",", ":"))

    # Legacy per-column {col: {min,max}}
    min_values: Dict[str, Any] = {}
    max_values: Dict[str, Any] = {}
    null_count: Dict[str, Any] = {}

    legacy = True
    for col, v in stats_obj.items():
        if not isinstance(v, dict):
            legacy = False
            break
        if "min" in v:
            min_values[col] = v.get("min")
        if "max" in v:
            max_values[col] = v.get("max")
        if "nullCount" in v:
            null_count[col] = v.get("nullCount")

    if not legacy:
        return None

    out2: Dict[str, Any] = {
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count,
    }
    if num_records is not None:
        try:
            out2["numRecords"] = int(num_records)
        except Exception:
            pass

    return json.dumps(out2, separators=(",", ":"))



# --------- Tunables -----------------------------------------------------------

# Always co-locate data files into the Delta table folder, as requested.
COPY_DATA_INTO_TABLE_DIR = True

# Do NOT emit any checkpoint files unless we implement full, spec-compliant checkpoints.
WRITE_CHECKPOINT = False

# -----------------------------------------------------------------------------


def _pad_version(version: int) -> str:
    return f"{int(version):020d}"


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _stable_table_id(organization: str, super_name: str, table_name: str) -> str:
    """
    Stable UUIDv5-like id from table coordinates to keep metaData.id consistent.
    Allows overriding via simple_snapshot['delta_meta_id'] or ['metadata_id'] if provided.
    """
    import uuid
    seed = f"{organization}/{super_name}/{table_name}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def _binary_copy_if_possible(storage, src_path: str, dst_path: str) -> bool:
    """Copy bytes from src_path to dst_path as efficiently as the backend allows.

    For MinIO, prefer server-side copy_object using CopySource (no download/upload).
    Falls back to storage.copy(), then read_bytes/write_bytes.
    """

    # --- MinIO fast-path (server-side copy) ---------------------------------
    client = getattr(storage, "client", None)
    if client is not None and hasattr(client, "copy_object"):
        bucket = (
            getattr(storage, "bucket", None)
            or getattr(storage, "bucket_name", None)
            or getattr(storage, "_bucket", None)
            or getattr(storage, "_bucket_name", None)
        )
        if isinstance(bucket, str) and bucket:
            try:
                from minio.commonconfig import CopySource  # type: ignore

                storage.makedirs(os.path.dirname(dst_path))
                client.copy_object(bucket, dst_path, CopySource(bucket, src_path))
                return True
            except Exception as e:
                logger.warning(f"[mirror][minio] copy_object failed ({src_path} -> {dst_path}): {e}")

    # --- Generic backend copy ------------------------------------------------
    if hasattr(storage, "copy"):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.copy(src_path, dst_path)
            return True
        except Exception as e:
            logger.warning(f"[mirror][delta] storage.copy failed ({src_path} -> {dst_path}): {e}")

    # --- Byte copy fallback --------------------------------------------------
    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.write_bytes(dst_path, read_bytes(src_path))
            return True
        except Exception as e:
            logger.warning(f"[mirror][delta] byte copy failed ({src_path} -> {dst_path}): {e}")

    return False


def _co_locate_or_reuse_path(storage, table_files_dir: str, catalog_file_path: str) -> str:
    """
    Copy parquet into table_files_dir, return the relative path 'files/<hash>_<basename>'.
    """
    base_name = catalog_file_path.rstrip("/").split("/")[-1]  # robust basename for URIs
    h = hashlib.md5(catalog_file_path.encode("utf-8")).hexdigest()[:8]
    rel_name = f"{h}_{base_name}"
    rel_path = "/".join(("files", rel_name))
    dst_path = os.path.join(table_files_dir, rel_name)
    ok = _binary_copy_if_possible(storage, catalog_file_path, dst_path)
    if not ok:
        raise RuntimeError(f"Failed to copy data file into Delta table dir: {catalog_file_path}")
    return rel_path


def _list_co_located_paths(storage, table_files_dir: str) -> Set[str]:
    """
    Return set of already present co-located files as 'files/<name>'.
    Uses string-splitting on '/' to avoid platform/os.path edge-cases with URIs.
    """
    rels: Set[str] = set()
    try:
        entries: List[str] = []
        if hasattr(storage, "ls"):
            entries = storage.ls(table_files_dir) or []
        elif hasattr(storage, "listdir"):
            # listdir should already return children of table_files_dir
            entries = ["/".join((table_files_dir.rstrip("/"), e)) for e in (storage.listdir(table_files_dir) or [])]
        for p in entries:
            # Normalize to filename and re-prefix with 'files/'
            fn = p.rstrip("/").split("/")[-1]
            if fn:
                rels.add("/".join(("files", fn)))
    except Exception:
        # ok to be empty
        pass
    return rels


def _write_checkpoint_if_possible(storage, log_dir: str, version: int, add_paths: List[str]) -> None:
    # Disabled by default; keep code for future full compliance
    if not WRITE_CHECKPOINT:
        return
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        return

    table = pa.table({"path": pa.array(add_paths, type=pa.string())})
    chk_path = os.path.join(log_dir, f"{_pad_version(version)}.checkpoint.parquet")
    buf = io.BytesIO()
    pq.write_table(table, buf)
    storage.makedirs(log_dir)
    storage.write_bytes(chk_path, buf.getvalue())
    logger.debug(f"[mirror][delta] wrote checkpoint {chk_path} with {len(add_paths)} entries")


def write_delta_table(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:
    """
    Produce a Delta commit for the current simple_snapshot.

    Directory layout:
      base = <org>/<super>/delta/<table>
        _delta_log/
        files/
    """
    base = os.path.join(super_table.organization, super_table.super_name, "delta", table_name)
    log_dir = os.path.join(base, "_delta_log")
    files_dir = os.path.join(base, "files")

    super_table.storage.makedirs(log_dir)
    super_table.storage.makedirs(files_dir)

    version = int(simple_snapshot.get("snapshot_version", 0))

    # Prefer an explicitly provided Spark StructType JSON if caller has one
    schema_string_from_snapshot = (
        simple_snapshot.get("schemaString")
        or simple_snapshot.get("schema_string")
        or None
    )
    schema_list = simple_snapshot.get("schema", [])

    # If snapshot doesn't include schema, infer it from the first available parquet resource.
    if not schema_string_from_snapshot and not schema_list:
        try:
            resource_paths: List[str] = []
            for r in simple_snapshot.get("resources", []) or []:
                if not isinstance(r, dict):
                    continue
                p = r.get("path") or r.get("file")
                if p:
                    resource_paths.append(str(p))
            schema_list = _infer_schema_list_from_any_parquet(super_table.storage, resource_paths)
        except Exception:
            pass

    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", [])

    # Derive/override meta id & createdTime if provided
    meta_id = (
        simple_snapshot.get("delta_meta_id")
        or simple_snapshot.get("metadata_id")
        or _stable_table_id(super_table.organization, super_table.super_name, table_name)
    )
    created_time_ms = int(simple_snapshot.get("createdTime", _now_ms()))

    # Previously co-located files (as relative 'files/<name>')
    prev_paths = _list_co_located_paths(super_table.storage, files_dir)

    # Build current file paths by co-locating under table dir
    current_paths: List[str] = []
    path_records: List[Tuple[str, int, Dict[str, Any]]] = []  # (relative path used in add, size, resource)
    for r in resources:
        src_file = r["file"]
        size = int(r.get("file_size", 0))
        used_rel_path = _co_locate_or_reuse_path(super_table.storage, files_dir, src_file)
        current_paths.append(used_rel_path)
        path_records.append((used_rel_path, size, r))

    current_set = set(current_paths)

    # If schema is still missing, infer it from the first co-located parquet file.
    if not schema_string_from_snapshot and not schema_list and current_paths:
        try:
            first_rel = current_paths[0]
            # current_paths are relative to delta table root (e.g., "files/<name>.parquet")
            first_full = os.path.join(base, first_rel)
            schema_list = _infer_schema_list_from_any_parquet(super_table.storage, [first_full])
        except Exception:
            pass

    # Files to remove = those present before but not now
    to_remove = sorted(list(prev_paths - current_set))
    to_add = path_records

    # If nothing changes, don't write a new Delta version
    if not to_add and not to_remove:
        logger.info(f"[mirror][delta] v{version} no-op (no add/remove); skipping commit")
        return

    # Delete obsolete co-located files (physically remove unused parquet from the delta folder)
    for rp in to_remove:
        # Normalize any weird inputs (e.g., absolute abfss strings accidentally captured)
        rel = rp
        if rel.startswith("abfss://"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        # Ensure we only ever delete under the table's files/ dir
        if not rel.startswith("files/"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        abs_path = os.path.join(base, rel)
        try:
            if hasattr(super_table.storage, "exists") and not super_table.storage.exists(abs_path):
                # Best-effort fallback: try raw filename under files_dir
                alt_abs = os.path.join(files_dir, rel.split("/")[-1])
                if super_table.storage.exists(alt_abs):
                    abs_path = alt_abs
            super_table.storage.delete(abs_path)
        except Exception as e:
            logger.warning(f"[mirror][delta] failed to delete obsolete {rel}: {e}")

    # Metrics
    num_files = len(to_add)
    num_output_bytes = sum(int(sz) for _, sz, _ in to_add) if to_add else 0
    num_output_rows = 0
    for rec in resources:
        val = rec.get("rows") or rec.get("numRecords") or 0
        try:
            num_output_rows += int(val)
        except Exception:
            pass

    # Compose Delta log (NDJSON)
    commit_path = os.path.join(log_dir, _pad_version(version) + ".json")

    # Guard: if this exact version file already exists, skip to avoid duplicate writes
    if hasattr(super_table.storage, "exists") and super_table.storage.exists(commit_path):
        logger.info(f"[mirror][delta] commit {commit_path} already exists; skipping rewrite")
        return

    with io.StringIO() as s:
        # 1) commitInfo
        commit_info = {
            "commitInfo": {
                "timestamp": _now_ms(),
                "operation": "WRITE",
                "operationParameters": {"mode": "Overwrite", "partitionBy": "[]"},
                "isolationLevel": "Serializable",
                "isBlindAppend": False,
                "operationMetrics": {
                    "numFiles": str(num_files),
                    "numOutputRows": str(num_output_rows),
                    "numOutputBytes": str(num_output_bytes),
                },
                "engineInfo": "Apache-Spark/3.4.3.5.3.20250511.1 Delta-Lake/2.4.0.24",
                "txnId": __import__("uuid").uuid4().hex,
            }
        }
        s.write(json.dumps(commit_info, separators=(",", ":")) + "\n")

        # 2) protocol (must be present in the first visible commit; safe to include every time)
        protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 4}}
        s.write(json.dumps(protocol, separators=(",", ":")) + "\n")

        # 3) metaData (include on every commit for robustness)
        metadata = {
            "metaData": {
                "id": meta_id,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": _schema_to_structtype_json(schema_string_from_snapshot, schema_list),
                "partitionColumns": [],
                "configuration": {
                    "delta.enableChangeDataFeed": "true",
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true",
                },
                "createdTime": created_time_ms,
            }
        }
        s.write(json.dumps(metadata, separators=(",", ":")) + "\n")

        # 4) remove actions
        remove_ts = _now_ms()
        for p in to_remove:
            # p here is relative 'files/<name>' due to normalization above
            s.write(json.dumps({"remove": {"path": p, "deletionTimestamp": remove_ts, "dataChange": True}},
                               separators=(",", ":")) + "\n")

        # 5) add actions
        add_ts = _now_ms()
        for p, size, res in to_add:
            stats_val = None
            try:
                rows_val = res.get("rows") or res.get("numRecords")
                raw_stats = None
                if isinstance(res.get("stats_json"), str):
                    raw_stats = res["stats_json"]
                elif isinstance(res.get("stats"), dict):
                    raw_stats = res["stats"]

                # Normalize to Delta stats format (or None)
                stats_val = _normalize_delta_stats(raw_stats, num_records=rows_val)

                # If no stats dict provided, but we have rows -> emit minimal Delta stats
                if stats_val is None and rows_val is not None:
                    stats_val = _normalize_delta_stats({"numRecords": int(rows_val)}, num_records=rows_val)
            except Exception:
                stats_val = None

            add_obj: Dict[str, Any] = {
                "add": {
                    "path": p,  # relative to table root (e.g., files/<name>.parquet)
                    "partitionValues": {},
                    "size": int(size),
                    "modificationTime": add_ts,
                    "dataChange": True,
                    "tags": {},
                }
            }
            if stats_val is not None:
                add_obj["add"]["stats"] = stats_val

            s.write(json.dumps(add_obj, separators=(",", ":")) + "\n")

        # Write the commit atomically
        super_table.storage.write_bytes(commit_path, s.getvalue().encode("utf-8"))

    # Optional checkpoint (disabled)
    try:
        _write_checkpoint_if_possible(super_table.storage, log_dir, version, current_paths)
    except Exception as e:
        logger.warning(f"[mirror][delta] checkpoint skipped: {e}")

    logger.info(
        f"[mirror][delta] v{version} wrote {_pad_version(version)}.json  "
        f"(add={len(to_add)}, remove={len(to_remove)}; co_located=yes)"
    )
