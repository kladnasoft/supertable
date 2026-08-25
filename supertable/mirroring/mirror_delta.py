# route: supertable.mirroring.mirror_delta
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
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Set

from supertable.config.defaults import logger
from supertable.mirroring.failure_safety import mirror_error_type


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


def _require_nonempty_delta_schema(schema_json: str) -> str:
    """Reject a non-empty snapshot whose Delta schema is not publishable."""
    error = (
        "Delta mirroring requires a valid non-empty schema for non-empty "
        "resources"
    )
    try:
        parsed = json.loads(schema_json)
    except Exception:
        raise RuntimeError(error) from None

    fields = parsed.get("fields") if isinstance(parsed, dict) else None
    if (
        not isinstance(parsed, dict)
        or parsed.get("type") != "struct"
        or not isinstance(fields, list)
        or not fields
    ):
        raise RuntimeError(error)

    names: Set[str] = set()
    for field in fields:
        if not isinstance(field, dict):
            raise RuntimeError(error)
        name = field.get("name")
        field_type = field.get("type")
        if (
            not isinstance(name, str)
            or not name
            or name.strip() != name
            or name in names
            or not isinstance(field_type, (str, dict))
            or not field_type
            or not isinstance(field.get("nullable"), bool)
            or not isinstance(field.get("metadata"), dict)
        ):
            raise RuntimeError(error)
        names.add(name)
    return schema_json



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

    The storage adapter owns physical-prefix translation and may implement
    ``copy`` server-side. Falls back to read_bytes/write_bytes.
    """

    # --- Generic backend copy ------------------------------------------------
    if hasattr(storage, "copy"):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.copy(src_path, dst_path)
            return True
        except Exception as e:
            logger.warning(
                "[mirror][delta] storage.copy failed; error_type=%s",
                mirror_error_type(e),
            )

    # --- Byte copy fallback --------------------------------------------------
    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.write_bytes(dst_path, read_bytes(src_path))
            return True
        except Exception as e:
            logger.warning(
                "[mirror][delta] byte copy failed; error_type=%s",
                mirror_error_type(e),
            )

    return False


def _co_locate_or_reuse_path(storage, table_files_dir: str, catalog_file_path: str) -> str:
    """
    Copy parquet into table_files_dir, return the relative path 'files/<hash>_<basename>'.
    """
    base_name = catalog_file_path.rstrip("/").split("/")[-1]  # robust basename for URIs
    h = hashlib.md5(
        catalog_file_path.encode("utf-8"), usedforsecurity=False,
    ).hexdigest()[:8]
    rel_name = f"{h}_{base_name}"
    rel_path = "/".join(("files", rel_name))
    dst_path = os.path.join(table_files_dir, rel_name)
    ok = _binary_copy_if_possible(storage, catalog_file_path, dst_path)
    if not ok:
        raise RuntimeError("Failed to copy data file into Delta table dir")
    return rel_path


def _list_co_located_paths(storage, table_files_dir: str) -> Set[str]:
    """
    Return set of already present co-located files as 'files/<name>'.
    Uses string-splitting on '/' to avoid platform/os.path edge-cases with URIs.
    """
    rels: Set[str] = set()
    if hasattr(storage, "ls"):
        entries = storage.ls(table_files_dir) or []
    elif hasattr(storage, "listdir"):
        entries = [
            "/".join((table_files_dir.rstrip("/"), e))
            for e in (storage.listdir(table_files_dir) or [])
        ]
    elif hasattr(storage, "list_files"):
        entries = storage.list_files(table_files_dir, "*") or []
    else:
        raise RuntimeError(
            "Delta mirroring requires a reliable directory-listing method"
        )
    for p in entries:
        # Normalize to filename and re-prefix with 'files/'
        fn = p.rstrip("/").split("/")[-1]
        if fn:
            rels.add("/".join(("files", fn)))
    return rels


_DELTA_LOG_NAME = re.compile(r"^(\d{20})\.json$")


def _list_delta_versions(storage, log_dir: str) -> List[int]:
    entries = storage.list_files(log_dir, "*.json") or []
    versions: List[int] = []
    for entry in entries:
        match = _DELTA_LOG_NAME.fullmatch(entry.rstrip("/").split("/")[-1])
        if match:
            versions.append(int(match.group(1)))
    versions = sorted(set(versions))
    if versions and versions != list(range(versions[-1] + 1)):
        raise RuntimeError(
            f"Delta log is not contiguous from version zero: {versions!r}"
        )
    return versions


def _read_delta_actions(storage, log_dir: str, version: int) -> List[Dict[str, Any]]:
    path = os.path.join(log_dir, f"{_pad_version(version)}.json")
    raw = storage.read_text(path)
    actions: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise RuntimeError("Invalid Delta action in commit log")
        actions.append(value)
    return actions


def _delta_commit_version(
        storage, log_dir: str, versions: List[int], commit_id: str,
) -> Tuple[Optional[int], Optional[int]]:
    if not commit_id:
        return None, None
    for version in reversed(versions):
        try:
            actions = _read_delta_actions(storage, log_dir, version)
        except Exception:
            # A post-write read-back failure can leave only the newest object
            # present but invalid. The durable publication intent and table
            # lock make that unacknowledged newest generation safe to repair
            # in place; corruption in older acknowledged history is fatal.
            if versions and version == versions[-1]:
                return None, version
            raise
        for action in actions:
            info = action.get("commitInfo")
            if isinstance(info, dict) and str(info.get("txnId") or "") == commit_id:
                return version, None
    return None, None


def _expected_co_located_paths(simple_snapshot: Dict[str, Any]) -> Set[str]:
    expected: Set[str] = set()
    for resource in simple_snapshot.get("resources", []) or []:
        source = str(resource.get("file") or "")
        if not source:
            continue
        digest = hashlib.md5(
            source.encode("utf-8"), usedforsecurity=False,
        ).hexdigest()[:8]
        expected.add(f"files/{digest}_{source.rstrip('/').split('/')[-1]}")
    return expected


def verify_delta_table(
        super_table, table_name: str, simple_snapshot: Dict[str, Any],
        *, commit_id: str = "",
) -> None:
    """Verify the Delta log and active files for an exact core snapshot."""
    base = os.path.join(
        super_table.organization, super_table.super_name, "delta", table_name,
    )
    log_dir = os.path.join(base, "_delta_log")
    files_dir = os.path.join(base, "files")
    versions = _list_delta_versions(super_table.storage, log_dir)
    if not versions or versions[0] != 0:
        raise RuntimeError("Delta mirror has no version-zero commit")

    active: Dict[str, Dict[str, Any]] = {}
    found_commit = not commit_id
    for version in versions:
        actions = _read_delta_actions(super_table.storage, log_dir, version)
        if version == 0:
            if not any("protocol" in action for action in actions):
                raise RuntimeError("Delta version zero is missing protocol")
            if not any("metaData" in action for action in actions):
                raise RuntimeError("Delta version zero is missing metadata")
        for action in actions:
            info = action.get("commitInfo")
            if isinstance(info, dict) and str(info.get("txnId") or "") == commit_id:
                found_commit = True
            add = action.get("add")
            if isinstance(add, dict) and add.get("path"):
                active[str(add["path"])] = add
            remove = action.get("remove")
            if isinstance(remove, dict) and remove.get("path"):
                active.pop(str(remove["path"]), None)
    if not found_commit:
        raise RuntimeError("Delta mirror does not contain the requested commit")

    expected = _expected_co_located_paths(simple_snapshot)
    physical = _list_co_located_paths(super_table.storage, files_dir)
    if set(active) != expected or physical != expected:
        raise RuntimeError(
            "Delta mirror does not match the committed snapshot; "
            f"log_count={len(active)}; file_count={len(physical)}; "
            f"expected_count={len(expected)}"
        )
    expected_by_path: Dict[str, Dict[str, Any]] = {}
    for resource in simple_snapshot.get("resources", []) or []:
        source = str(resource.get("file") or "")
        if not source:
            continue
        digest = hashlib.md5(
            source.encode("utf-8"), usedforsecurity=False,
        ).hexdigest()[:8]
        expected_by_path[
            f"files/{digest}_{source.rstrip('/').split('/')[-1]}"
        ] = resource
    for relative, resource in expected_by_path.items():
        add = active[relative]
        expected_size = int(resource.get("file_size") or 0)
        recorded_size = int(add.get("size") or 0)
        recorded_digest = str(
            (add.get("tags") or {}).get("supertable.sha256") or ""
        )
        if not recorded_digest:
            raise RuntimeError("Delta add action lacks an artifact digest")
        actual_size, actual_digest = super_table.storage.content_sha256(
            os.path.join(base, relative)
        )
        if (
            (expected_size and expected_size != recorded_size)
            or actual_size != recorded_size
            or actual_digest != recorded_digest
        ):
            raise RuntimeError(
                "Delta mirrored artifact failed its content seal"
            )


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
    logger.debug(
        "[mirror][delta] wrote checkpoint v%s with %s entries",
        version,
        len(add_paths),
    )


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

    versions = _list_delta_versions(super_table.storage, log_dir)
    # Delta transaction versions belong to the mirror, not to the source
    # catalog.  Enabling a mirror for a mature table must still bootstrap at
    # 000...000.json and then advance contiguously.
    version = (versions[-1] + 1) if versions else 0
    mirror_commit_id = str(simple_snapshot.get("_mirror_commit_id") or "")

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
        declared_size = int(r.get("file_size", 0))
        used_rel_path = _co_locate_or_reuse_path(super_table.storage, files_dir, src_file)
        source_size, source_digest = super_table.storage.content_sha256(src_file)
        mirror_size, mirror_digest = super_table.storage.content_sha256(
            os.path.join(base, used_rel_path)
        )
        if declared_size and source_size != declared_size:
            raise RuntimeError("Delta source size changed before mirroring")
        if mirror_size != source_size or mirror_digest != source_digest:
            raise RuntimeError("Delta copy failed its content seal")
        sealed_resource = dict(r)
        sealed_resource["_mirror_sha256"] = source_digest
        current_paths.append(used_rel_path)
        path_records.append((used_rel_path, source_size, sealed_resource))

    current_set = set(current_paths)

    # A recovery retry may arrive after the Delta commit became durable but
    # before the Redis outbox was acknowledged.  The stable core commit id in
    # commitInfo makes that retry a verification/cleanup operation rather than
    # a second Delta transaction.
    existing_commit_version, repair_version = _delta_commit_version(
        super_table.storage, log_dir, versions, mirror_commit_id,
    )
    if existing_commit_version is not None:
        for obsolete in sorted(prev_paths - current_set):
            target = os.path.join(base, obsolete)
            try:
                super_table.storage.delete(target)
            except FileNotFoundError:
                pass
        verify_delta_table(
            super_table, table_name, simple_snapshot,
            commit_id=mirror_commit_id,
        )
        logger.info(
            "[mirror][delta] requested commit already published at v%s",
            existing_commit_version,
        )
        return
    if repair_version is not None:
        version = repair_version

    # If schema is still missing, infer it from the first co-located parquet file.
    if not schema_string_from_snapshot and not schema_list and current_paths:
        try:
            first_rel = current_paths[0]
            # current_paths are relative to delta table root (e.g., "files/<name>.parquet")
            first_full = os.path.join(base, first_rel)
            schema_list = _infer_schema_list_from_any_parquet(super_table.storage, [first_full])
        except Exception:
            pass

    delta_schema_json = _schema_to_structtype_json(
        schema_string_from_snapshot, schema_list,
    )
    if resources:
        delta_schema_json = _require_nonempty_delta_schema(delta_schema_json)

    # Files to remove = those present before but not now
    to_remove = sorted(list(prev_paths - current_set))
    to_add = path_records

    # An empty initial table still needs version zero (protocol + metadata).
    # Once initialized, a direct legacy call without a durable commit id may
    # remain a no-op when there are no file changes.
    if versions and not mirror_commit_id and not to_add and not to_remove:
        logger.info(f"[mirror][delta] v{version} no-op (no add/remove); skipping commit")
        return

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
    if (
        repair_version is None
        and hasattr(super_table.storage, "exists")
        and super_table.storage.exists(commit_path)
    ):
        logger.info(
            "[mirror][delta] commit v%s already exists; skipping rewrite",
            version,
        )
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
                "txnId": mirror_commit_id or __import__("uuid").uuid4().hex,
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
                "schemaString": delta_schema_json,
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
                    "tags": {
                        "supertable.sha256": str(
                            res.get("_mirror_sha256") or ""
                        ),
                    },
                }
            }
            if stats_val is not None:
                add_obj["add"]["stats"] = stats_val

            s.write(json.dumps(add_obj, separators=(",", ":")) + "\n")

        # Write the commit atomically
        commit_bytes = s.getvalue().encode("utf-8")
        super_table.storage.write_bytes(commit_path, commit_bytes)
        visible_bytes = super_table.storage.read_bytes(commit_path)
        if visible_bytes != commit_bytes:
            raise RuntimeError(
                "Delta commit failed read-after-write verification"
            )

    # Only after the new Delta log is durable may obsolete physical files be
    # removed. Deleting first can permanently break the previously committed
    # mirror if the new log write fails. Cleanup remains best-effort because
    # Delta readers use remove actions in the log; an undeleted object is not
    # logically visible in the newly committed table.
    for rp in to_remove:
        rel = rp
        if rel.startswith("abfss://"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        if not rel.startswith("files/"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        abs_path = os.path.join(base, rel)
        try:
            if (
                hasattr(super_table.storage, "exists")
                and not super_table.storage.exists(abs_path)
            ):
                alt_abs = os.path.join(files_dir, rel.split("/")[-1])
                if super_table.storage.exists(alt_abs):
                    abs_path = alt_abs
            super_table.storage.delete(abs_path)
        except Exception as exc:
            logger.warning(
                "[mirror][delta] post-commit cleanup failed; error_type=%s",
                mirror_error_type(exc),
            )

    # Optional checkpoint (disabled)
    try:
        _write_checkpoint_if_possible(super_table.storage, log_dir, version, current_paths)
    except Exception as e:
        logger.warning(
            "[mirror][delta] checkpoint skipped; error_type=%s",
            mirror_error_type(e),
        )

    logger.info(
        f"[mirror][delta] v{version} wrote {_pad_version(version)}.json  "
        f"(add={len(to_add)}, remove={len(to_remove)}; co_located=yes)"
    )
