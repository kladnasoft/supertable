# supertable/simple_table.py

from __future__ import annotations

import os
from datetime import datetime

from supertable.config.defaults import logger
from supertable.errors import TableNotFoundError
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage
from supertable.super_table import SuperTable
from supertable.utils.helper import collect_schema, generate_filename
from supertable.utils.snapshot import (
    complete_snapshot_payload,
    snapshot_cache_payload,
)
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
from supertable.rbac.access_control import check_control_access, check_write_access


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
        catalog: Existing catalog client to reuse. Writers pass the client that
            owns their table lease; ordinary callers omit it.
        _live_leaf_verified: Internal writer fast path. The caller must hold the
            table lease and have just verified the exact live leaf plus both
            deletion-intent fences. It is valid only with
            ``create_if_missing=False``.
        _pinned_leaf: Exact leaf document returned by the writer's atomic
            mutation-context read. It is consumed by
            :meth:`get_simple_table_snapshot` without another catalog lookup.
    """

    def __init__(
        self,
        super_table: SuperTable,
        simple_name: str,
        *,
        create_if_missing: bool = True,
        catalog: Optional[RedisCatalog] = None,
        _live_leaf_verified: bool = False,
        _pinned_leaf: Optional[Dict[str, Any]] = None,
        _pinned_snapshot: Optional[Dict[str, Any]] = None,
    ):
        # ``super == simple`` is the public aggregate relation that unions the
        # parent's children.  A physical child with that name is therefore
        # unaddressable on its own and would be silently folded into the
        # aggregate scan.  Enforce the invariant at the table-creation boundary
        # as well as in DataWriter.validation(), including case-only aliases.
        if (
            create_if_missing
            and str(simple_name).casefold()
            == str(super_table.super_name).casefold()
        ):
            raise ValueError("SimpleTable name can't match with SuperTable name")

        self.super_table = super_table
        self.identity = "tables"
        self.simple_name = simple_name

        # Storage is the same as SuperTable's
        self.storage = self.super_table.storage
        # Writers already own a table lease and have just fenced the namespace,
        # deletion intents, root and leaf through their catalog. Reuse that
        # exact client and proof instead of constructing another catalog and
        # repeating the same Redis lifecycle reads. Ordinary/read-side callers
        # retain the independent fail-closed checks below.
        self.catalog = catalog if catalog is not None else RedisCatalog()

        if _live_leaf_verified and create_if_missing:
            raise ValueError(
                "_live_leaf_verified requires create_if_missing=False"
            )
        if _pinned_leaf is not None and not _live_leaf_verified:
            raise ValueError("_pinned_leaf requires _live_leaf_verified=True")
        if _pinned_snapshot is not None and _pinned_leaf is None:
            raise ValueError("_pinned_snapshot requires _pinned_leaf")
        if _pinned_leaf is not None and (
            not isinstance(_pinned_leaf, dict)
            or not isinstance(_pinned_leaf.get("path"), str)
            or not _pinned_leaf["path"]
        ):
            raise ValueError("_pinned_leaf must be a valid leaf document")
        self._pinned_leaf = (
            dict(_pinned_leaf) if _pinned_leaf is not None else None
        )
        self._pinned_snapshot = _pinned_snapshot
        if not _live_leaf_verified:
            deletion_guard = getattr(
                type(self.catalog), "check_deletion_intent_absent", None,
            )
            if callable(deletion_guard):
                self.catalog.check_deletion_intent_absent(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple=self.simple_name,
                )


        # Data layout
        self.simple_dir = os.path.join(
            super_table.organization, super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")

        if _live_leaf_verified:
            return

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

        org = self.super_table.organization
        sup = self.super_table.super_name
        # Only structural creation participates in the namespace lock; writes
        # to existing tables remain concurrent and are drained by their leaf
        # locks during SuperTable deletion. Acquiring this before any storage
        # write closes the new-child write-after-prefix-verification race.
        namespace_token = self.catalog.acquire_namespace_lock(
            org, sup, ttl_s=30, timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError(
                f"Could not acquire namespace creation lock for {org}/{sup}"
            )
        try:
            # Check before either fast-path return: stale metadata recreated
            # behind a terminal tombstone must not make the object live.
            self.catalog.check_initialization_allowed(
                org,
                sup,
                namespace_token=namespace_token,
                simple=self.simple_name,
            )
            if not self.catalog.root_exists(org, sup):
                raise TableNotFoundError(org, sup, self.simple_name)
            # Another initializer may have won while this constructor waited.
            if self.catalog.leaf_exists(org, sup, self.simple_name):
                return

            # First-time initialization: ensure directories in storage.
            for p in (self.simple_dir, self.data_dir, self.snapshot_dir):
                try:
                    if not self.storage.exists(p):
                        self.storage.makedirs(p)
                except Exception:
                    # Object storage may no-op; that's fine.
                    pass

            initial_snapshot_file = generate_filename(alias=self.identity)
            new_simple_path = os.path.join(
                self.snapshot_dir, initial_snapshot_file,
            )
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
                "tombstone_digest": None,
                "rowid_high_watermark": 0,
                "stats_file": None,
                "stats_rows": 0,
                "_row_filter": None,
            }
            self.storage.write_json(new_simple_path, snapshot_data)

            now_ms = int(datetime.now().timestamp() * 1000)
            payload_setter = getattr(
                self.catalog, "set_leaf_payload_cas", None,
            )
            if callable(payload_setter):
                # An ambiguous timeout may mean Redis committed the payload;
                # never delete or retry that immutable snapshot blindly.
                payload_setter(
                    org,
                    sup,
                    self.simple_name,
                    snapshot_data,
                    new_simple_path,
                    now_ms=now_ms,
                    namespace_token=namespace_token,
                )
            else:
                raise RuntimeError(
                    "Catalog does not support atomic initialize-only leaf "
                    "payloads; refusing ambiguous table creation"
                )
        finally:
            self.catalog.release_namespace_lock(
                org, sup, namespace_token,
            )

    def delete(self, role_name: str) -> str:
        """Start a new deletion, refusing any abandoned prior intent."""
        return self._delete_with_intent(role_name=role_name)

    def recover_delete(
            self,
            role_name: str,
            *,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Resume an abandoned deletion after external liveness proof.

        The caller must first prove that the previous process cannot resume an
        already-issued delete against this table's fixed storage prefix.  An
        ordinary :meth:`delete` never takes over a durable intent.
        """
        return self._delete_with_intent(
            role_name=role_name,
            recovery_intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    def _delete_with_intent(
            self,
            *,
            role_name: str,
            recovery_intent_id: Optional[str] = None,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        check_control_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=self.simple_name,
        )

        # Creation is fenced by the namespace lock, while writers are fenced
        # by the leaf lock. Acquire in the same namespace->leaf order as whole
        # SuperTable deletion and retain both through prefix verification and
        # atomic metadata cleanup so a concurrent initializer cannot recreate
        # the table inside a successful delete.
        namespace_token = self.catalog.acquire_namespace_lock(
            self.super_table.organization,
            self.super_table.super_name,
            ttl_s=30,
            timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError(
                f"Could not acquire namespace deletion lock for "
                f"{self.super_table.super_name!r}"
            )
        token = self.catalog.acquire_simple_lock(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
            ttl_s=30,
            timeout_s=60,
        )
        if not token:
            self.catalog.release_namespace_lock(
                self.super_table.organization,
                self.super_table.super_name,
                namespace_token,
            )
            raise TimeoutError(
                f"Could not acquire deletion lock for simple {self.simple_name!r}"
            )

        # The same auto-renewed leaf lock used by DataWriter covers physical
        # prefix verification and metadata removal as one mutation. A writer
        # can neither publish into the prefix mid-delete nor orphan a successor
        # between verification and leaf removal.
        simple_table_folder = os.path.join(
            self.super_table.organization, self.super_table.super_name, self.identity, self.simple_name
        )
        storage_prefixes = [
            simple_table_folder,
            *[
                os.path.join(
                    self.super_table.organization,
                    self.super_table.super_name,
                    mirror_name,
                    self.simple_name,
                )
                for mirror_name in ("delta", "iceberg", "parquet")
            ],
        ]
        try:
            if recovery_intent_id is None:
                intent = self.catalog.begin_simple_deletion(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    namespace_token=namespace_token,
                    lock_token=token,
                )
            else:
                intent = self.catalog.recover_simple_deletion(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    expected_intent_id=recovery_intent_id,
                    namespace_token=namespace_token,
                    lock_token=token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            intent_id = intent.get("intent_id") if isinstance(intent, dict) else None
            if not intent_id:
                raise RuntimeError("Catalog returned an invalid deletion intent")
            logger.info(
                "[deletion] SimpleTable cleanup started for %s/%s/%s; "
                "deletion_intent_id=%s; recovery=%s",
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
                intent_id,
                recovery_intent_id is not None,
            )

            # Prefixes on cloud stores usually do not have marker objects.
            # Delete and verify the full logical prefix before making it
            # undiscoverable in Redis. If this call or a lease becomes
            # ambiguous, the durable intent remains and recreation stays
            # blocked; an ordinary retry cannot clear it.
            for prefix in storage_prefixes:
                self.storage.delete_prefix(prefix)

            removed = self.catalog.delete_simple_table(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
                lock_token=token,
                namespace_token=namespace_token,
                intent_id=intent_id,
            )
            if not removed:
                raise RuntimeError(
                    f"Failed to remove catalog metadata for "
                    f"{self.simple_name!r} after storage deletion"
                )
            if recovery_intent_id is not None:
                self.catalog.clear_simple_deletion_tombstone(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    expected_intent_id=intent_id,
                    namespace_token=namespace_token,
                    lock_token=token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
        finally:
            try:
                self.catalog.release_simple_lock(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    token,
                )
            finally:
                self.catalog.release_namespace_lock(
                    self.super_table.organization,
                    self.super_table.super_name,
                    namespace_token,
                )

        logger.info(
            f"Deleted Table (storage): {simple_table_folder}; "
            f"deletion_intent_id={intent_id}"
        )
        return str(intent_id)

    @classmethod
    def recover_pending_delete(
            cls,
            *,
            organization: str,
            super_name: str,
            simple_name: str,
            role_name: str,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Recover a deleted leaf even if its parent root is already absent."""
        catalog = RedisCatalog()
        storage = get_storage()
        parent = SuperTable.__new__(SuperTable)
        parent.identity = "super"
        parent.super_name = super_name
        parent.organization = organization
        parent.storage = storage
        parent.catalog = catalog
        parent.super_dir = os.path.join(organization, super_name, "super")
        table = cls.__new__(cls)
        table.super_table = parent
        table.identity = "tables"
        table.simple_name = simple_name
        table.storage = storage
        table.catalog = catalog
        table.simple_dir = os.path.join(
            organization, super_name, "tables", simple_name,
        )
        table.data_dir = os.path.join(table.simple_dir, "data")
        table.snapshot_dir = os.path.join(table.simple_dir, "snapshots")
        return table.recover_delete(
            role_name,
            intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    def get_simple_table_snapshot(self):
        """
        Read the current heavy snapshot via the Redis leaf pointer.

        If the Redis leaf stores a snapshot payload, use it to avoid storage reads.
        """
        ptr = getattr(self, "_pinned_leaf", None)
        if ptr is None:
            ptr = self.catalog.get_leaf(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
            )
        if not ptr or not ptr.get("path"):
            raise FileNotFoundError("No path found in simple table leaf pointer.")
        # Preserve the exact leaf document that selected this snapshot.  The
        # writer uses its path/version as the compare-and-swap base when it
        # atomically publishes the successor.  Re-fetching later would create
        # a time-of-check/time-of-use gap after lock expiry or takeover.
        self._last_snapshot_leaf = dict(ptr)
        path = ptr["path"]

        payload = getattr(self, "_pinned_snapshot", None)
        if payload is None:
            payload = complete_snapshot_payload(
                ptr.get("payload") if isinstance(ptr, dict) else None,
                expected_version=ptr.get("version") if isinstance(ptr, dict) else None,
                require_policy_marker=True,
            )
        if payload is not None:
            return payload, path

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
        from supertable.processing import (
            compact_resources,
            ROWID_COL,
            TOMBSTONE_FILE_COL,
            load_tombstone,
            load_tombstone_manifest_from_storage,
            load_tombstone_segments,
            tombstone_cache_identity,
        )
        from supertable.data_classes import TombstoneSegmentDef
        from supertable.tombstone_manifest_v2 import (
            TOMBSTONE_FORMAT_V2,
            normalize_snapshot_tombstone_state,
        )

        snapshot, _path = self.get_simple_table_snapshot()
        table_config = self.catalog.get_table_config(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
        ) or {}

        # Read the deletion-vector (if any) so its rows are dropped from
        # the export rather than copied verbatim.
        dead_rowids_by_file = None
        tombstone_state = normalize_snapshot_tombstone_state(snapshot)
        tombstone_path = tombstone_state.pointer
        tombstone_format = tombstone_state.tombstone_format
        if tombstone_path:
            allowed_files = {
                resource.get("file")
                for resource in (snapshot.get("resources") or [])
                if isinstance(resource, dict) and resource.get("file")
            }
            if tombstone_format == TOMBSTONE_FORMAT_V2:
                manifest_prefix = (
                    f"{self.simple_dir.rstrip('/')}/tombstone/"
                )
                if not tombstone_path.startswith(manifest_prefix):
                    raise ValueError(
                        "Deletion-vector manifest pointer escapes the pinned "
                        "simple table"
                    )
                manifest = load_tombstone_manifest_from_storage(
                    self.storage,
                    tombstone_path,
                    expected_organization=self.super_table.organization,
                    expected_super_name=self.super_table.super_name,
                    expected_simple_name=self.simple_name,
                    pinned_snapshot_version=snapshot.get("snapshot_version"),
                    expected_total_rows=tombstone_state.rows,
                    expected_digest=tombstone_state.digest,
                    expected_segment_prefix=os.path.join(
                        self.simple_dir, "tombstone",
                    ),
                )
                segment_defs = tuple(
                    TombstoneSegmentDef(
                        cache_key=segment.file,
                        tombstone_path=segment.file,
                        expected_rows=segment.rows,
                        file_size=segment.file_size,
                        tombstone_digest=segment.digest,
                    )
                    for segment in manifest.segments
                )
                manifest_cache_identity = tombstone_cache_identity(
                    tombstone_path,
                    organization=self.super_table.organization,
                    storage=self.storage,
                )
                tomb_df = load_tombstone_segments(
                    segment_defs,
                    storage=self.storage,
                    cache_identity=(
                        "export-dv-v2:"
                        f"{manifest_cache_identity}:"
                        f"{tombstone_state.digest}"
                    ),
                    expected_rows=tombstone_state.rows,
                    allowed_files=allowed_files,
                    allow_cache=False,
                )
            else:
                tomb_df = load_tombstone(
                    tombstone_path,
                    cache_identity=tombstone_cache_identity(
                        tombstone_path,
                        organization=self.super_table.organization,
                        storage=self.storage,
                    ),
                    allow_cache=False,
                    required=True,
                    expected_rows=tombstone_state.rows,
                    expected_digest=tombstone_state.digest,
                    allowed_files=allowed_files,
                )
            if tomb_df is None:  # defensive: required=True must raise instead
                raise RuntimeError("Required deletion-vector could not be loaded")
            dead_rowids_by_file = {}
            for file_key, rowid in tomb_df.select(
                [TOMBSTONE_FILE_COL, ROWID_COL]
            ).iter_rows():
                dead_rowids_by_file.setdefault(str(file_key), set()).add(int(rowid))

        _considered, total_rows, new_resources, _sunset = compact_resources(
            snapshot=snapshot,
            data_dir=target_dir,
            compression_level=compression_level,
            table_config=table_config,
            small_only=small_only,
            dead_rowids_by_file=dead_rowids_by_file,
            required_reads=True,
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
        # An expected-absent one-shot write uses an in-memory compatibility
        # version-zero base and an empty CAS path. Its first durable snapshot is
        # version one but has no predecessor object; ordinary updates retain the
        # exact immutable predecessor pointer.
        last_simple_table["previous_snapshot"] = last_simple_table_path or None
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

        # Every newly published Redis cache must explicitly seal the linked-
        # share policy state. Preserve a valid inherited overlay; ordinary
        # unshared snapshots use JSON null as the canonical unrestricted value.
        normalized_snapshot = snapshot_cache_payload(last_simple_table)
        last_simple_table.clear()
        last_simple_table.update(normalized_snapshot)

        # Write new heavy snapshot file
        new_simple_path = os.path.join(self.snapshot_dir, generate_filename(alias=self.identity))
        with p.span("simple_update.write_json"):
            self.storage.write_json(new_simple_path, last_simple_table)

        p.add("snapshot_resources_count", len(updated_resources))
        p.add("snapshot_sunset_count", len(sunset_set))
        p.add("snapshot_new_resources_count", len(new_resources))

        return last_simple_table, new_simple_path
