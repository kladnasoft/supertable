# supertable/simple_table.py

from __future__ import annotations

import os
from datetime import datetime

from supertable.config.defaults import logger
from supertable.errors import TableNotFoundError
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable
from supertable.utils.helper import collect_schema, generate_filename
from supertable.utils.profiler import Profiler, get_null_profiler
import json
from typing import Any, Dict, List, Optional


def _spark_type_from_polars_dtype(dtype: Any) -> str:
    """Best-effort mapping from Polars dtype to Spark/Delta type string."""
    try:
        import polars as pl
    except Exception:  # pragma: no cover
        return "string"

    if dtype in (pl.Utf8, pl.String):
        return "string"
    if dtype == pl.Boolean:
        return "boolean"

    if dtype == pl.Int8:
        return "byte"
    if dtype == pl.Int16:
        return "short"
    if dtype == pl.Int32:
        return "integer"
    if dtype == pl.Int64:
        return "long"

    if dtype == pl.UInt8:
        return "short"
    if dtype == pl.UInt16:
        return "integer"
    if dtype == pl.UInt32:
        return "long"
    if dtype == pl.UInt64:
        return "decimal(20,0)"

    if dtype == pl.Float32:
        return "float"
    if dtype == pl.Float64:
        return "double"

    if dtype == pl.Date:
        return "date"
    if dtype == pl.Datetime:
        return "timestamp"
    if dtype == pl.Binary:
        return "binary"

    # Decimal can be parametric; treat conservatively
    try:
        if isinstance(dtype, pl.Decimal):
            return f"decimal({dtype.precision},{dtype.scale})"
    except Exception:
        pass

    return "string"


def _schema_list_from_polars_df(model_df: Any) -> List[Dict[str, Any]]:
    """Build a Delta-friendly schema list from a Polars DataFrame."""
    try:
        schema = model_df.schema
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for name, dtype in schema.items():
        out.append(
            {
                "name": name,
                "type": _spark_type_from_polars_dtype(dtype),
                "nullable": True,
                "metadata": {},
            }
        )
    return out
from supertable.rbac.access_control import check_write_access


class SimpleTable:
    """
    Simple-table layout on storage (heavy data) + Redis leaf pointer (meta).

    Args:
        super_table: Parent ``SuperTable`` (already constructed).
        simple_name: Name of this simple table.
        create_if_missing: When True (default), bootstrap the simple
            table (mkdirs, initial empty snapshot JSON, Redis ``meta:leaf``
            pointer) if it does not exist. When False, raise
            ``TableNotFoundError`` instead. Read-side callers
            (``MetaReader`` and friends) pass ``False`` so a missing
            table surfaces as an error instead of being silently
            materialized as a side effect of constructing the Python
            object.
    """

    def __init__(
        self,
        super_table: SuperTable,
        simple_name: str,
        *,
        create_if_missing: bool = True,
    ):
        self.super_table = super_table
        self.identity = "tables"
        self.simple_name = simple_name

        # Storage is the same as SuperTable's
        self.storage = self.super_table.storage
        self.catalog = RedisCatalog()


        # Data layout
        self.simple_dir = os.path.join(
            super_table.organization, super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")

        # Fast path: if meta:leaf exists, don't touch storage
        if self.catalog.leaf_exists(
                self.super_table.organization, self.super_table.super_name, self.simple_name
        ):
            return

        # Read-only opt-out: refuse to bootstrap as a side effect. The
        # writer leaves the default so the first write to a new table
        # naturally creates it; readers opt out so a query against a
        # missing name fails fast rather than materializing an empty
        # table.
        if not create_if_missing:
            raise TableNotFoundError(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )

        self.init_simple_table()

    def init_simple_table(self) -> None:
        """
        Initialize simple table:
          * If Redis meta:leaf already exists -> skip any folder checks/creations and bootstrapping.
          * Otherwise, create folders and bootstrap an initial empty snapshot and leaf pointer.
        """

        # First-time initialization: ensure directories in storage (best-effort)
        for p in (self.simple_dir, self.data_dir, self.snapshot_dir):
            try:
                if not self.storage.exists(p):
                    self.storage.makedirs(p)
            except Exception:
                # Object storage may no-op; that's fine
                pass

        # Bootstrap leaf pointer in Redis (version=0, empty initial snapshot)
        initial_snapshot_file = generate_filename(alias=self.identity)
        new_simple_path = os.path.join(self.snapshot_dir, initial_snapshot_file)
        snapshot_data = {
            "simple_name": self.simple_name,
            "location": self.simple_dir,
            "snapshot_version": 0,
            "last_updated_ms": int(datetime.now().timestamp() * 1000),
            "previous_snapshot": None,
            "schema": [],
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "stats_file": None,
            "stats_rows": 0,
        }
        self.storage.write_json(new_simple_path, snapshot_data)

        # CAS set leaf to that path (version becomes 0)
        now_ms = int(datetime.now().timestamp() * 1000)
        try:
            # Prefer storing snapshot payload in Redis so readers can avoid storage reads.
            self.catalog.set_leaf_payload_cas(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
                snapshot_data,
                new_simple_path,
                now_ms=now_ms,
            )
        except Exception:
            # Backward compatible fallback.
            self.catalog.set_leaf_path_cas(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
                new_simple_path,
                now_ms=now_ms,
            )

    def delete(self, role_name: str) -> None:
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=self.simple_name,
        )

        # Remove folder (heavy data) from storage
        simple_table_folder = os.path.join(
            self.super_table.organization, self.super_table.super_name, self.identity, self.simple_name
        )
        try:
            if self.storage.exists(simple_table_folder):
                self.storage.delete(simple_table_folder)
        except FileNotFoundError:
            pass

        # Remove Redis meta (leaf pointer + lock)
        self.catalog.delete_simple_table(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
        )

        logger.info(f"Deleted Table (storage): {simple_table_folder}")

    def get_simple_table_snapshot(self):
        """
        Read the current heavy snapshot via the Redis leaf pointer.

        If the Redis leaf stores a snapshot payload, use it to avoid storage reads.
        """
        ptr = self.catalog.get_leaf(self.super_table.organization, self.super_table.super_name, self.simple_name)
        if not ptr or not ptr.get("path"):
            raise FileNotFoundError("No path found in simple table leaf pointer.")
        path = ptr["path"]

        payload = ptr.get("payload") if isinstance(ptr, dict) else None
        if isinstance(payload, dict):
            # Common: payload is the snapshot dict itself.
            if isinstance(payload.get("resources"), list):
                return payload, path
            # Also support nested snapshot shapes.
            snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else None
            if isinstance(snap, dict) and isinstance(snap.get("resources"), list):
                return snap, path

        data = self.storage.read_json(path)
        return data, path

    def export_to(self, target_dir: str, compression_level: int = 3, small_only: bool = False):
        """Write a standalone copy of the current data into ``target_dir``.

        Reads the current snapshot's parquet resources and re-writes them
        as memory-bounded parquet chunks (each ~``max_memory_chunk_size``,
        from the per-table config or the global default) under
        ``target_dir``.  This is a pure copy: it does NOT create a new
        snapshot, advance the Redis leaf, or touch ``data/``/``snapshots/``.

        Logically-deleted rows are physically dropped: the snapshot's
        deletion-vector (``tombstone`` pointer) is read and its
        ``__rowid__`` values are anti-joined out of the exported files, so
        a standalone export never contains tombstoned rows.

        Args:
            target_dir: destination directory for the exported parquet
                files (created if missing).  Typically an
                ``export/<timestamp>/`` folder next to ``data/``.
            compression_level: zstd level for the exported parquet.
            small_only: when False (default) every resource is read and
                re-chunked; when True only files smaller than
                ``max_memory_chunk_size`` are included.

        Returns:
            ``dict`` with ``files`` (list of written paths),
            ``files_written``, ``total_rows`` and ``total_bytes``.
        """
        from supertable.processing import compact_resources, ROWID_COL, _read_parquet_safe

        snapshot, _path = self.get_simple_table_snapshot()
        table_config = self.catalog.get_table_config(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
        ) or {}

        # Read the deletion-vector (if any) so its rows are dropped from
        # the export rather than copied verbatim.
        dead_rowids = None
        tombstone_path = snapshot.get("tombstone")
        if tombstone_path:
            tomb_df = _read_parquet_safe(tombstone_path)
            if tomb_df is not None and ROWID_COL in tomb_df.columns:
                dead_rowids = set(tomb_df.get_column(ROWID_COL).drop_nulls().to_list())

        _considered, total_rows, new_resources, _sunset = compact_resources(
            snapshot=snapshot,
            data_dir=target_dir,
            compression_level=compression_level,
            table_config=table_config,
            small_only=small_only,
            dead_rowids=dead_rowids,
        )

        files = [r.get("file") for r in new_resources if isinstance(r, dict) and r.get("file")]
        total_bytes = sum(
            int(r.get("file_size") or 0) for r in new_resources if isinstance(r, dict)
        )
        return {
            "files": files,
            "files_written": len(files),
            "total_rows": int(total_rows),
            "total_bytes": int(total_bytes),
        }

    def update(self, new_resources, sunset_files, model_df, last_snapshot=None, last_snapshot_path=None, lineage=None, profiler: Optional[Profiler] = None):
        """
        Build and write a new heavy snapshot on storage.
        Returns: (snapshot_dict, snapshot_path)

        If last_snapshot and last_snapshot_path are provided, skips the redundant
        snapshot read (caller already holds the data under lock).

        Args:
            model_df: Polars DataFrame whose schema becomes the new snapshot's
                ``schema`` / ``schemaString`` ("last write wins" — see
                docs/03_data_model.md "Schema Field Semantics").  Pass ``None``
                to PRESERVE the previous snapshot's schema unchanged — used by
                delete-only writes which must not alter the table shape.  Only
                update paths are allowed to change schema; deletes never are.
            lineage: Optional dict of data provenance metadata.  Stored in the
                snapshot JSON so historical versions carry their origin.
        """
        p = profiler or get_null_profiler()
        if last_snapshot is not None and last_snapshot_path is not None:
            last_simple_table = last_snapshot
            last_simple_table_path = last_snapshot_path
        else:
            # Fallback: read current snapshot (backward compatible)
            with p.span("simple_update.read_snapshot"):
                last_simple_table, last_simple_table_path = self.get_simple_table_snapshot()

        with p.span("simple_update.merge_resources"):
            current_resources = last_simple_table.get("resources", [])
            sunset_set = set(sunset_files)
            updated_resources = [res for res in current_resources if res.get("file") not in sunset_set]
            updated_resources.extend(new_resources)
            last_simple_table["resources"] = updated_resources

        # Update metadata
        last_simple_table["previous_snapshot"] = last_simple_table_path
        last_simple_table["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        last_simple_table["snapshot_version"] = int(last_simple_table.get("snapshot_version", 0)) + 1

        # Schema policy: only "update" callers (those that supply a model_df)
        # may change the snapshot schema.  Delete-only writers pass
        # model_df=None so the previous snapshot's schema / schemaString carry
        # forward verbatim — a delete must never shrink the metadata view of
        # the table even though the delete-predicate dataframe only carries
        # the key columns.
        if model_df is not None:
            with p.span("simple_update.collect_schema"):
                schema_list = collect_schema(model_df)
                if not schema_list:
                    # Fallback: derive schema from Polars dtypes if helper returns empty.
                    schema_list = _schema_list_from_polars_df(model_df)
                last_simple_table["schema"] = schema_list
                # Also store a Spark StructType JSON for downstream Delta mirrors.
                try:
                    last_simple_table["schemaString"] = json.dumps({"type": "struct", "fields": schema_list}, separators=(",", ":"))
                except Exception:
                    pass
        # else: leave last_simple_table["schema"] and ["schemaString"] untouched.

        # Data lineage — record provenance of this write
        if lineage and isinstance(lineage, dict):
            last_simple_table["lineage"] = lineage

        # Write new heavy snapshot file
        new_simple_path = os.path.join(self.snapshot_dir, generate_filename(alias=self.identity))
        with p.span("simple_update.write_json"):
            self.storage.write_json(new_simple_path, last_simple_table)

        p.add("snapshot_resources_count", len(updated_resources))
        p.add("snapshot_sunset_count", len(sunset_set))
        p.add("snapshot_new_resources_count", len(new_resources))

        return last_simple_table, new_simple_path