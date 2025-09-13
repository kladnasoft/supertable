# supertable/mirroring/mirror_delta.py
"""
Delta Lake mirror (spec-compliant, latest-only projection)

What this does:
- Writes Delta commit files under .../delta/<table>/_delta_log/<version>.json
  using *newline-delimited* JSON actions: commitInfo, protocol, metaData,
  remove (for files not in latest), add (for files in latest).
- Always uses a "complete overwrite" style commit so each version reflects the
  current SuperTable snapshot exactly (latest-only semantics).
- Optionally writes a small Parquet checkpoint (.../_delta_log/<version>.checkpoint.parquet)
  when pyarrow/pyarrow.parquet are available.

Notes:
- For strict engine compatibility (e.g., Synapse serverless SQL), engines typically expect
  data files to live *under the Delta table folder*. We try to "co-locate" data by copying
  files when COPY_DATA_INTO_TABLE_DIR = True AND the storage backend supports binary copy.
  Otherwise we fall back to referencing the original absolute paths. Some engines accept that,
  others may not; co-location is recommended if you intend to query the mirror directly.

- Minimal impact: no changes needed elsewhere; this module is invoked by MirrorFormats.
"""

import io
import json
import os
import sys
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Set

from supertable.config.defaults import logger

# --------- Tunables (keep minimal & local) -----------------------------------

# Try to copy data files under delta/<table>/files/ for stricter compatibility.
# If False, "add" actions will reference the original paths from the catalog.
COPY_DATA_INTO_TABLE_DIR = False  # set to True if your storage supports binary copy

# If True and pyarrow is available, write a lightweight Parquet checkpoint per version.
WRITE_CHECKPOINT = True

# -----------------------------------------------------------------------------


def _pad_version(version: int) -> str:
    # 20-digit, zero-padded according to Delta naming convention
    return f"{int(version):020d}"


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _to_spark_schema_json(schema_any: Any) -> str:
    """
    Convert SuperTable schema (list of {name, type}) to a Spark-like JSON schema string.
    This is a *minimal* mapper covering common primitive types.
    """
    def map_type(t: str) -> Dict[str, Any]:
        t = (t or "").lower()
        # Minimal, extend as needed
        if t in ("string", "str", "varchar", "text"):
            return {"type": "string", "nullable": True, "metadata": {}}
        if t in ("int", "integer"):
            return {"type": "integer", "nullable": True, "metadata": {}}
        if t in ("bigint", "long"):
            return {"type": "long", "nullable": True, "metadata": {}}
        if t in ("double", "float", "real", "numeric"):
            return {"type": "double", "nullable": True, "metadata": {}}
        if t in ("bool", "boolean"):
            return {"type": "boolean", "nullable": True, "metadata": {}}
        if t in ("timestamp", "datetime"):
            return {"type": {"type": "timestamp", "timezone": "UTC"}, "nullable": True, "metadata": {}}
        if t in ("date",):
            return {"type": "date", "nullable": True, "metadata": {}}
        # default fallback
        return {"type": "string", "nullable": True, "metadata": {}}

    fields = []
    if isinstance(schema_any, list):
        for col in schema_any:
            name = col.get("name") if isinstance(col, dict) else None
            ctype = col.get("type") if isinstance(col, dict) else None
            if not name:
                continue
            f = map_type(ctype)
            f["name"] = name
            fields.append(f)

    return json.dumps({"type": "struct", "fields": fields}, separators=(",", ":"))


def _read_previous_latest(storage, base_dir: str) -> Tuple[int, Set[str]]:
    """
    Read previous mirrored state from latest.json if exists.
    Returns (prev_version, set_of_paths_relative_or_absolute).
    """
    latest_path = os.path.join(base_dir, "latest.json")
    if not storage.exists(latest_path):
        return (-1, set())
    try:
        obj = storage.read_json(latest_path)
        version = int(obj.get("version", -1))
        files = obj.get("files", [])
        # latest.json format: {"files": [{"path": "...", "size": N}, ...]}
        prev_paths = {f.get("path") for f in files if f.get("path")}
        return (version, prev_paths)
    except Exception as e:
        logger.warning(f"[mirror][delta] Failed to read previous latest.json: {e}")
        return (-1, set())


def _binary_copy_if_possible(storage, src_path: str, dst_path: str) -> bool:
    """
    Try to copy a binary file using storage helper(s).
    Fallback: read bytes, write bytes if supported.
    Return True on success, False otherwise.
    """
    # Preferred: direct copy if backend has it
    if hasattr(storage, "copy"):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.copy(src_path, dst_path)
            return True
        except Exception as e:
            logger.warning(f"[mirror][delta] storage.copy failed ({src_path} -> {dst_path}): {e}")

    # Try generic read/write bytes API if present
    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            data = storage.read_bytes(src_path)
            storage.write_bytes(dst_path, data)
            return True
        except Exception as e:
            logger.warning(f"[mirror][delta] byte copy failed ({src_path} -> {dst_path}): {e}")

    return False


def _co_locate_or_reuse_path(storage, table_files_dir: str, catalog_file_path: str) -> Tuple[str, bool]:
    """
    If COPY_DATA_INTO_TABLE_DIR is True, copy the parquet into table_files_dir, return relative path.
    Otherwise return the original path (absolute). Also returns a bool indicating if path is relative.
    """
    if COPY_DATA_INTO_TABLE_DIR:
        # Stable target name from source path (avoid collisions)
        base_name = os.path.basename(catalog_file_path)
        # Include a short hash prefix to avoid dup basenames
        h = hashlib.md5(catalog_file_path.encode("utf-8")).hexdigest()[:8]
        rel_path = os.path.join("files", f"{h}_{base_name}")
        dst_path = os.path.join(table_files_dir, f"{h}_{base_name}")
        ok = _binary_copy_if_possible(storage, catalog_file_path, dst_path)
        if ok:
            return rel_path.replace("\\", "/"), True
        # If copy failed, fall back to absolute
        logger.warning(f"[mirror][delta] Falling back to original path (copy failed): {catalog_file_path}")
    return catalog_file_path, False


def _write_checkpoint_if_possible(storage, log_dir: str, version: int, add_paths: List[str]) -> None:
    """
    Optional Parquet checkpoint:
    - Requires pyarrow to be available in the runtime.
    - Stores a very small checkpoint with 'path' column so engines can skip JSON replay faster.
    This is NOT a full fidelity Delta checkpoint (no stats), but it's a valid Parquet file and
    harmless; engines that expect richer checkpoints will still be able to replay JSON logs.
    """
    if not WRITE_CHECKPOINT:
        return
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        # Silently skip if pyarrow is not present
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
    Produce a real Delta commit for the current simple_snapshot.

    Location layout:
      base = <org>/<super>/delta/<table>
        _delta_log/
        files/               (optional, when COPY_DATA_INTO_TABLE_DIR=True)
        latest.json          (convenience pointer; not part of Delta spec)
    """
    base = os.path.join(super_table.organization, super_table.super_name, "delta", table_name)
    log_dir = os.path.join(base, "_delta_log")
    files_dir = os.path.join(base, "files")  # used only if COPY_DATA_INTO_TABLE_DIR=True

    super_table.storage.makedirs(log_dir)
    if COPY_DATA_INTO_TABLE_DIR:
        super_table.storage.makedirs(files_dir)

    version = int(simple_snapshot.get("snapshot_version", 0))
    schema_any = simple_snapshot.get("schema", [])
    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", [])

    # Compute deltas vs previous mirror
    prev_version, prev_paths = _read_previous_latest(super_table.storage, base)

    # Build current file paths, optionally co-locate under table dir
    current_paths: List[str] = []
    path_records: List[Tuple[str, int]] = []  # (path_used_in_add, size)
    for r in resources:
        src_file = r["file"]
        size = int(r.get("file_size", 0))
        used_path, is_relative = _co_locate_or_reuse_path(super_table.storage, files_dir, src_file)
        current_paths.append(used_path)
        path_records.append((used_path, size))

    current_set = set(current_paths)

    # Actions to remove: those present before but not now
    to_remove = sorted(list(prev_paths - current_set))
    # Actions to add: those present now (we do full overwrite, but include removes for correctness)
    to_add = path_records  # (path, size)

    # Compose Delta JSON log (newline-delimited actions)
    commit_path = os.path.join(log_dir, _pad_version(version) + ".json")
    with io.StringIO() as s:
        # 1) commitInfo
        commit_info = {
            "commitInfo": {
                "timestamp": _now_ms(),
                "operation": "WRITE",
                "operationParameters": {"mode": "CompleteOverwrite"},
                "readVersion": prev_version if prev_version >= 0 else None,
                "isBlindAppend": False,
                "engineInfo": "supertable-mirror-delta/1.0",
            }
        }
        s.write(json.dumps(commit_info, separators=(",", ":")) + "\n")

        # 2) protocol
        protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
        s.write(json.dumps(protocol, separators=(",", ":")) + "\n")

        # 3) metaData (repeat each commit to be robust)
        metadata = {
            "metaData": {
                "id": f"{super_table.organization}:{super_table.super_name}:{table_name}",
                "name": table_name,
                # Delta expects Spark-like JSON schema string; we provide a minimal projection
                "schemaString": _to_spark_schema_json(schema_any),
                "partitionColumns": [],
                "configuration": {"created.by": "supertable", "mirror": "delta"},
                "createdTime": _now_ms(),
            }
        }
        s.write(json.dumps(metadata, separators=(",", ":")) + "\n")

        # 4) remove (for any files that disappeared vs previous state)
        remove_ts = _now_ms()
        for p in to_remove:
            s.write(json.dumps({"remove": {"path": p, "deletionTimestamp": remove_ts, "dataChange": True}},
                               separators=(",", ":")) + "\n")

        # 5) add (for all current files)
        add_ts = _now_ms()
        for p, size in to_add:
            s.write(json.dumps({"add": {
                "path": p,
                "size": int(size),
                "modificationTime": add_ts,
                "dataChange": True
            }}, separators=(",", ":")) + "\n")

        # Atomically write the commit file
        content = s.getvalue().encode("utf-8")
        super_table.storage.write_bytes(commit_path, content)

    # Optional small checkpoint (helps some engines skip JSON replay faster)
    try:
        _write_checkpoint_if_possible(super_table.storage, log_dir, version, current_paths)
    except Exception as e:
        logger.warning(f"[mirror][delta] checkpoint skipped: {e}")

    # Update convenience pointer (non-spec)
    latest_state = {
        "version": version,
        "updated_at_ms": _now_ms(),
        "schema": schema_any,  # original schema projection
        "files": [{"path": p, "size": int(sz)} for p, sz in to_add],
    }
    super_table.storage.write_json(os.path.join(base, "latest.json"), latest_state)

    logger.info(
        f"[mirror][delta] v{version} wrote {_pad_version(version)}.json  "
        f"(add={len(to_add)}, remove={len(to_remove)}; "
        f"co_located={'yes' if COPY_DATA_INTO_TABLE_DIR else 'no'})"
    )
