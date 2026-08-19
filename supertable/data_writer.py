# supertable/data_writer.py
from __future__ import annotations

import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import re
from typing import Any

import polars
from polars import DataFrame

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.monitoring.partitions import MONITORING_SINK_TABLES
from supertable.monitoring_writer import (
    MonitoringDurabilityError,
    MonitoringPostCommitError,
    MonitoringWriter,
)
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.utils.profiler import Profiler
from supertable.processing import (
    find_overlapping_files,
    resolve_overwrite_writes,
    identify_all_rowids,
    build_tombstone_file,
    persist_tombstone_frame,
    reclaim_fully_dead_files,
    build_stats_file,
    extract_stats_rows,
    probe_ranges_from_df,
    prune_overlapping_files_by_stats,
    load_stats,
    cache_stats,
    load_tombstone,
    cache_tombstone,
    evict_tombstone,
    write_parquet_and_collect_resources,
    compact_resources,
    compact_tombstones,
    should_compact_small_files,
    _resolve_unified_dtype,
    _max_tombstone_rows,
    _read_parquet_safe,
    validate_tombstone_frame,
    tombstone_digest,
    stats_for_complete_files,
    resource_stats_seal,
    stats_cache_identity,
    tombstone_cache_identity,
    ROWID_COL,
    TOMBSTONE_FILE_COL,
)
from supertable.rbac.access_control import (  # noqa: F401
    check_create_access,
    check_write_access,
)
from supertable.redis_catalog import (
    RedisCatalog,
    persist_unresolved_quality_generation,
)
from supertable.mirroring.mirror_formats import (
    MirrorFormats,
    MirrorPublicationError,
)
from supertable.audit import emit as _audit_emit, EventCategory, Actions, Severity, make_detail


def _safe_json(obj):
    """JSON-dump helper that never raises.  Falls back to str on failure."""
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            return json.dumps(str(obj), ensure_ascii=False)
        except Exception:
            return "{}"


class DataWriter:
    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name, organization)
        self.catalog = RedisCatalog()
        self._table_config_cache: dict = {}

    timer = Timer()

    @staticmethod
    def _declared_tombstone_rows(snapshot: dict):
        """Return the sealed DV count, rejecting ambiguous active pointers."""
        if not snapshot.get("tombstone"):
            value = snapshot.get("tombstone_rows")
            if value not in (None, 0):
                raise ValueError(
                    "Snapshot has tombstone_rows without a deletion-vector pointer"
                )
            return None
        value = snapshot.get("tombstone_rows")
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(
                "Active deletion-vector is missing a valid tombstone_rows seal; "
                "explicit metadata recovery is required"
            )
        return value

    @staticmethod
    def _declared_tombstone_digest(snapshot: dict):
        """Return the canonical digest seal for an active deletion-vector."""
        if not snapshot.get("tombstone"):
            value = snapshot.get("tombstone_digest")
            if value not in (None, ""):
                raise ValueError(
                    "Snapshot has tombstone_digest without a deletion-vector pointer"
                )
            return None
        value = snapshot.get("tombstone_digest")
        if (
            not isinstance(value, str)
            or len(value) != 64
            or value != value.lower()
            or any(ch not in "0123456789abcdef" for ch in value)
        ):
            raise ValueError(
                "Active deletion-vector is missing a valid tombstone_digest seal; "
                "explicit metadata recovery is required"
            )
        return value

    @staticmethod
    def _snapshot_resource_files(snapshot: dict) -> set[str]:
        return {
            resource.get("file")
            for resource in (snapshot.get("resources") or [])
            if isinstance(resource, dict) and resource.get("file")
        }

    @staticmethod
    def _tombstone_referenced_files(frame: polars.DataFrame) -> set[str]:
        """Materialize only distinct file keys into Python memory."""
        return set(
            frame.get_column(TOMBSTONE_FILE_COL).unique().to_list()
        )

    def _get_enabled_mirrors(self, operation: str) -> list[str]:
        """Read mirror config once; an ambiguous failure aborts the mutation."""
        mirror_lookup = getattr(type(self.catalog), "get_mirrors", None)
        if not callable(mirror_lookup):
            # Compatibility for intentionally minimal third-party/test catalogs.
            return []
        try:
            configured = mirror_lookup(
                self.catalog,
                self.super_table.organization,
                self.super_table.super_name,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Cannot safely determine enabled mirrors for {operation}"
            ) from exc
        if not isinstance(configured, (list, tuple, set)):
            raise RuntimeError("Catalog returned an invalid mirror configuration")
        return list(configured)

    def _derive_legacy_rowid_high_watermark(
            self,
            snapshot: dict,
            *,
            profiler: Profiler,
    ) -> int:
        """Prove the row-id floor for a snapshot predating durable allocation.

        This migration is intentionally strict and paid only once: every live
        resource is read under the table lock, rowids must be canonical Int64,
        non-null and table-global unique, and the current deletion-vector must be
        readable/valid.  A partial scan cannot safely establish a recovery floor.
        """
        seen: set[int] = set()
        high = 0
        for resource in snapshot.get("resources") or []:
            file_path = resource.get("file") if isinstance(resource, dict) else None
            if not file_path:
                raise ValueError("Cannot derive rowid high-watermark from an invalid resource")
            frame = _read_parquet_safe(
                file_path,
                file_size=int(resource.get("file_size") or 0),
                columns=[ROWID_COL],
                required=True,
                profiler=profiler,
            )
            if frame is None or ROWID_COL not in frame.columns:
                raise ValueError(
                    f"Cannot derive rowid high-watermark: {file_path!r} lacks {ROWID_COL!r}"
                )
            rowids = frame.get_column(ROWID_COL)
            if rowids.dtype != polars.Int64 or rowids.null_count() > 0:
                raise ValueError(
                    f"Cannot derive rowid high-watermark: {file_path!r} has invalid rowids"
                )
            values = rowids.to_list()
            if (
                (values and min(values) <= 0)
                or len(values) != len(set(values))
                or seen.intersection(values)
            ):
                raise ValueError(
                    "Cannot derive rowid high-watermark: physical rowids must "
                    "be positive and table-global unique"
                )
            seen.update(values)
            if values:
                high = max(high, max(values))

        tombstone_path = snapshot.get("tombstone")
        if tombstone_path:
            tombstone_df = load_tombstone(
                tombstone_path,
                cache_identity=tombstone_cache_identity(
                    tombstone_path,
                    organization=self.super_table.organization,
                    storage=self.super_table.storage,
                ),
                allow_cache=True,
                required=True,
                expected_rows=self._declared_tombstone_rows(snapshot),
                expected_digest=self._declared_tombstone_digest(snapshot),
                allowed_files=self._snapshot_resource_files(snapshot),
                profiler=profiler,
            )
            if tombstone_df is not None and tombstone_df.height:
                high = max(high, int(tombstone_df.get_column(ROWID_COL).max()))
        if high < 0:
            raise ValueError("Cannot derive rowid high-watermark from negative rowids")
        return high

    def _reserve_snapshot_rowids(
            self,
            *,
            snapshot: dict,
            simple_name: str,
            count: int,
            profiler: Profiler,
            lock_token: str,
            require_floor: bool = False,
    ) -> tuple[int, int | None]:
        """Reserve a block above the durable floor and return start/new floor."""
        reserve_at_least = getattr(type(self.catalog), "reserve_rowids_at_least", None)
        raw_floor = snapshot.get("rowid_high_watermark")
        if count <= 0 and raw_floor is None and not require_floor:
            # A delete-only mutation allocates no IDs. Do not pay an all-resource
            # migration scan or persist an unproven zero; the next real insert
            # derives the legacy floor under lock (delete-all will leave no files).
            return 0, None
        if count > 0 and not callable(reserve_at_least):
            # A plain increment cannot atomically recover above the immutable
            # snapshot floor after Redis restore/reset. Even a returned value
            # above a persisted floor is unsafe for legacy snapshots whose
            # floor must first be derived from every physical row and DV.
            raise RuntimeError(
                "Catalog does not support floor-fenced rowid reservation"
            )
        if raw_floor is None:
            floor = self._derive_legacy_rowid_high_watermark(
                snapshot, profiler=profiler
            )
        else:
            if type(raw_floor) is not int:
                raise ValueError("Snapshot has an invalid rowid_high_watermark")
            floor = raw_floor
            if floor < 0:
                raise ValueError("Snapshot has a negative rowid_high_watermark")

        if count <= 0:
            return 0, floor

        start, new_high = self.catalog.reserve_rowids_at_least(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
            count,
            floor,
            lock_token=lock_token,
        )
        start, new_high = int(start), int(new_high)
        if start <= floor or new_high != start + count - 1:
            raise RuntimeError("Catalog returned an unsafe rowid reservation")
        return start, new_high

    @staticmethod
    def _unpack_tombstone_compaction(result):
        """Accept current four-field result and old unit-test doubles."""
        if len(result) == 4:
            return result
        if len(result) == 3:
            removed, resources, sunset = result
            residual = polars.DataFrame(
                schema={TOMBSTONE_FILE_COL: polars.Utf8, ROWID_COL: polars.Int64}
            )
            return removed, resources, sunset, residual
        raise RuntimeError("Invalid tombstone compaction result")

    def _publish_snapshot(
            self,
            *,
            simple_table: SimpleTable,
            simple_name: str,
            payload: dict,
            path: str,
            base_path: str,
            lock_token: str,
            commit_id: str,
        now_ms: int,
        mirrors: list[str] | None = None,
        mirror_pin_available: bool = False,
        mirror_pin: str | None = None,
        notify_quality: bool = False,
    ) -> None:
        """Publish a snapshot through the fenced atomic catalog primitive.

        Every mutation requires this primitive.  Falling back to separate leaf
        and root writes would lose the pinned base-version/lock-token fence and
        could let a late writer overwrite a newer deletion vector, resurrecting
        rows.  Catalog adapters must implement the same atomic contract.
        """
        atomic_commit = getattr(type(self.catalog), "commit_snapshot", None)
        if callable(atomic_commit):
            leaf = getattr(simple_table, "_last_snapshot_leaf", None)
            if not isinstance(leaf, dict):
                raise RuntimeError("Missing exact base leaf for fenced snapshot commit")
            expected_path = str(leaf.get("path") or "")
            if expected_path != str(base_path or ""):
                raise RuntimeError(
                    "Snapshot base pointer changed while loading the current snapshot"
                )
            try:
                expected_version = int(leaf.get("version", -1))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Current leaf has an invalid version") from exc
            mirror_formats = list(mirrors or [])
            if mirror_formats:
                prepare = getattr(
                    type(self.catalog), "prepare_mirror_publication", None
                )
                fail = getattr(type(self.catalog), "fail_mirror_publication", None)
                if not callable(prepare) or not callable(fail):
                    raise RuntimeError(
                        "Catalog does not support durable mirror publication state"
                    )
                self.catalog.prepare_mirror_publication(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    commit_id=commit_id,
                    snapshot_path=path,
                    mirrors=mirror_formats,
                    lock_token=lock_token,
                    now_ms=now_ms,
                )
            commit_kwargs: dict[str, Any] = {
                "expected_version": expected_version,
                "expected_path": expected_path,
                "lock_token": lock_token,
                "commit_id": commit_id,
                "now_ms": now_ms,
                "expected_mirrors": mirror_formats,
            }
            if mirror_formats:
                commit_kwargs["mirror_publication"] = True
            elif (
                mirror_pin_available
                and getattr(
                    self.catalog,
                    "supports_pinned_no_mirror_commit",
                    False,
                ) is True
            ):
                commit_kwargs["expected_mirror_pin"] = mirror_pin
            atomic_quality = bool(
                notify_quality
                and getattr(
                    self.catalog,
                    "supports_atomic_quality_generation",
                    False,
                ) is True
            )
            if atomic_quality:
                commit_kwargs["quality_generation"] = commit_id
            try:
                self.catalog.commit_snapshot(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    payload,
                    path,
                    **commit_kwargs,
                )
            except Exception as exc:
                if mirror_formats:
                    try:
                        self.catalog.fail_mirror_publication(
                            self.super_table.organization,
                            self.super_table.super_name,
                            simple_name,
                            commit_id=commit_id,
                            lock_token=lock_token,
                            failure_stage="core_commit",
                            error=exc,
                        )
                    except Exception as state_exc:
                        logger.error(
                            "Failed to persist mirror core-commit failure for "
                            f"{simple_name}: {state_exc}"
                        )
                raise
            if notify_quality and not atomic_quality:
                # Compatibility for catalog adapters that predate atomic DQ
                # hand-off. Keep this path lightweight (one fenced Lua call,
                # no scheduler/config imports) and best-effort, matching the
                # historical post-commit notification contract.
                try:
                    adapter_notify = getattr(
                        self.catalog, "persist_quality_generation", None,
                    )
                    if callable(adapter_notify):
                        adapter_notify(
                            self.super_table.organization,
                            self.super_table.super_name,
                            simple_name,
                            commit_id,
                        )
                    else:
                        redis_client = getattr(self.catalog, "r", None)
                        if redis_client is None:
                            raise RuntimeError(
                                "catalog adapter cannot persist DQ generations"
                            )
                        persist_unresolved_quality_generation(
                            redis_client,
                            self.super_table.organization,
                            self.super_table.super_name,
                            simple_name,
                            commit_id,
                        )
                except Exception as exc:
                    logger.warning(
                        "Post-commit data-quality notification was deferred: %s",
                        exc,
                    )
            return

        raise RuntimeError(
            "Catalog does not support fenced atomic snapshot commits; "
            "refusing an unsafe tombstone mutation"
        )

    def _fail_mirror_publication(
            self, *, simple_name: str, commit_id: str, lock_token: str,
            stage: str, error: Exception,
    ) -> None:
        fail = getattr(type(self.catalog), "fail_mirror_publication", None)
        if not callable(fail):
            raise RuntimeError(
                "Catalog does not support durable mirror failure state"
            )
        self.catalog.fail_mirror_publication(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
            commit_id=commit_id,
            lock_token=lock_token,
            failure_stage=stage,
            error=error,
        )

    def _complete_mirror_publication(
            self, *, simple_name: str, commit_id: str, lock_token: str,
    ) -> None:
        complete = getattr(type(self.catalog), "complete_mirror_publication", None)
        if not callable(complete):
            raise RuntimeError(
                "Catalog does not support durable mirror completion state"
            )
        self.catalog.complete_mirror_publication(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
            commit_id=commit_id,
            lock_token=lock_token,
        )

    # ---- Table configuration (per-table limits) -----------------------------

    def configure_table(
            self,
            role_name: str,
            simple_name: str,
            max_memory_chunk_size: int | None = None,
            max_decoded_compaction_bytes: int | None = None,
            max_overlapping_files: int | None = None,
            max_tombstone_rows: int | None = None,
            tombstone_compaction_workers: int | None = None,
    ) -> None:
        """Set per-table limit overrides.

        This is a one-time (or infrequent) operation.  The configuration is
        persisted in Redis.  A local copy is retained for observability, but
        operational reads refresh from Redis so changes made by another
        process are not hidden by a stale process-local cache.

        Args:
            role_name: RBAC role performing the configuration.
            simple_name: Name of the SimpleTable to configure.
            max_memory_chunk_size: Override MAX_MEMORY_CHUNK_SIZE (bytes) for
                this table. The legacy name denotes the compressed source-byte
                packing/output target; it is not a hard decoded-RAM limit.
                None leaves the existing value unchanged.
            max_decoded_compaction_bytes: Hard decoded-frame budget shared by
                the compaction coordinator and pending encoder workers. One
                indivisible row may exceed it. None keeps the existing value.
            max_overlapping_files: Override MAX_OVERLAPPING_FILES for this
                table.  None leaves the existing value unchanged.
            max_tombstone_rows: Maximum number of rows in the deletion-vector
                before physical compaction is triggered.  Overrides
                MAX_TOMBSTONE_ROWS (default 1,000,000) for this table.
                None leaves the existing value unchanged.
            tombstone_compaction_workers: Bounded worker count for independent
                tombstone file rewrites. Must be an integer from 1 through 8.
                The global default remains 2 for bounded memory use.
        """
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=simple_name,
        )

        updates: dict[str, int] = {}
        if max_memory_chunk_size is not None:
            if (
                type(max_memory_chunk_size) is not int
                or max_memory_chunk_size <= 0
            ):
                raise ValueError("max_memory_chunk_size must be a positive integer")
            updates["max_memory_chunk_size"] = max_memory_chunk_size
        if max_decoded_compaction_bytes is not None:
            if (
                type(max_decoded_compaction_bytes) is not int
                or max_decoded_compaction_bytes <= 0
            ):
                raise ValueError(
                    "max_decoded_compaction_bytes must be a positive integer"
                )
            updates["max_decoded_compaction_bytes"] = (
                max_decoded_compaction_bytes
            )
        if max_overlapping_files is not None:
            if (
                type(max_overlapping_files) is not int
                or max_overlapping_files <= 0
            ):
                raise ValueError("max_overlapping_files must be a positive integer")
            updates["max_overlapping_files"] = max_overlapping_files
        if max_tombstone_rows is not None:
            if (
                type(max_tombstone_rows) is not int
                or max_tombstone_rows <= 0
            ):
                raise ValueError("max_tombstone_rows must be a positive integer")
            updates["max_tombstone_rows"] = max_tombstone_rows
        if tombstone_compaction_workers is not None:
            if (
                type(tombstone_compaction_workers) is not int
                or not 1 <= tombstone_compaction_workers <= 8
            ):
                raise ValueError(
                    "tombstone_compaction_workers must be an integer from 1 to 8"
                )
            updates["tombstone_compaction_workers"] = tombstone_compaction_workers

        token = self.catalog.acquire_simple_lock(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
            ttl_s=30,
            timeout_s=60,
        )
        if not token:
            raise TimeoutError(
                f"Could not acquire configuration lock for {simple_name!r}"
            )
        try:
            # Read-modify-write under the same per-table lease used by every
            # writer.  Reading before the lease lets two disjoint partial
            # updates both derive from the same stale document and silently
            # lose whichever acknowledged update commits first.
            existing = self.catalog.get_table_config(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            ) or {}
            config = dict(existing)
            config.update(updates)
            self.catalog.set_table_config(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
                config,
                lock_token=token,
            )
            # Publish before releasing the lease.  Otherwise an older caller
            # can resume after a newer caller and overwrite this process cache
            # with a stale document even though Redis contains the merge.
            self._table_config_cache[simple_name] = dict(config)
        finally:
            self.catalog.release_simple_lock(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
                token,
            )

    def _get_table_config(self, simple_name: str) -> dict:
        """Return the authoritative table config and refresh the local copy."""
        config = self.catalog.get_table_config(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
        )
        refreshed = dict(config or {})
        self._table_config_cache[simple_name] = refreshed
        return dict(refreshed)

    # ── Schema derivation for ``compact()`` ─────────────────────────────
    #
    # Helper used by ``compact()`` only — kept here so both ``compact``
    # and the matching unit tests can monkey-patch the parquet-read step.

    # Snapshot ``schema`` entries are Spark-style strings
    # (``long``/``double``/``boolean``/...), polars dtypes are the
    # canonical Python types. This map converts the former to the
    # latter so ``_build_compact_model_df`` can reconstruct a Polars
    # frame from the snapshot's existing schema as a fallback.
    _SPARK_TYPE_TO_POLARS = {
        "string":    "Utf8",
        "boolean":   "Boolean",
        "byte":      "Int8",
        "short":     "Int16",
        "integer":   "Int32",
        "long":      "Int64",
        "float":     "Float32",
        "double":    "Float64",
        "date":      "Date",
        "timestamp": "Datetime",
        "binary":    "Binary",
    }

    def _build_compact_model_df(
        self, new_resources: list, last_snapshot: dict,
        footer_md_cache: dict | None = None,
    ) -> "DataFrame":
        """Build a zero-row Polars DataFrame whose schema mirrors the
        post-compaction file schema.

        ``simple_table.update()`` derives the snapshot's ``schema``
        field from this frame's dtypes — get this wrong and the
        snapshot metadata gets corrupted (e.g. every column reported
        as Utf8), even though the on-disk Parquet files are intact.

        Resolution order:

          1. Union the schemas of **every** new Parquet file. Within
             one chunk ``concat_with_union`` guarantees one schema; but
             ``compact_resources`` may produce **multiple chunks**
             (one per ``max_memory_chunk_size`` worth of data) and each
             chunk's schema is independent. Reading just the first
             chunk would miss columns that only appear in later chunks
             — a real risk with schema-evolving tables. We read **all**
             new files' schemas and union them.
          2. Reconstruct from the previous snapshot's ``schema``
             field. Compaction preserves logical schema by definition,
             so the pre-compaction schema is correct for the new
             snapshot as well.
          3. Empty frame — last resort.
        """
        # --- 1. Read the schemas of all new files and union them ------
        # ``concat_with_union`` semantics: column order is "a's columns
        # then b's new columns", types widened via _resolve_unified_dtype.
        # We mirror that here.
        if new_resources:
            union_schema: dict = {}
            success_count = 0
            for r in new_resources:
                first_path = r.get("file") if isinstance(r, dict) else None
                if not first_path:
                    continue
                cached_entry = (footer_md_cache or {}).get(first_path)
                cached_schema = getattr(cached_entry, "polars_schema", ())
                if cached_schema:
                    sample = polars.DataFrame(schema=dict(cached_schema))
                    success_count += 1
                    for col, dtype in sample.schema.items():
                        if col not in union_schema:
                            union_schema[col] = dtype
                        else:
                            union_schema[col] = _resolve_unified_dtype({
                                union_schema[col], dtype,
                            })
                    continue
                try:
                    arrow_tbl = self.super_table.storage.read_parquet(first_path)
                    sample = polars.from_arrow(arrow_tbl).limit(0)
                except Exception as e:
                    logger.debug(
                        f"[compact] could not read schema from {first_path}: {e}"
                    )
                    continue
                success_count += 1
                for col, dtype in zip(sample.columns, sample.dtypes):
                    if col not in union_schema:
                        union_schema[col] = dtype
                    else:
                        # Multiple target-sized outputs are independently
                        # packed and can legitimately carry different physical
                        # dtypes after schema evolution. Mirror the exact
                        # lossless widening rule used by concat_with_union;
                        # first-file-wins would silently narrow metadata.
                        union_schema[col] = _resolve_unified_dtype({
                            union_schema[col], dtype,
                        })
            if success_count > 0 and union_schema:
                return polars.DataFrame(schema=union_schema)
            # If we got AT LEAST one successful read but the resulting
            # schema is empty (zero-column files), fall through to the
            # snapshot-schema fallback rather than returning empty.

        # --- 2. Reconstruct from the prior snapshot's schema ------
        prior_schema = (last_snapshot or {}).get("schema")
        if isinstance(prior_schema, list) and prior_schema:
            schema_dict: dict = {}
            for col in prior_schema:
                if not isinstance(col, dict):
                    continue
                name = col.get("name")
                spark_type = (col.get("type") or "").lower()
                pl_name = self._SPARK_TYPE_TO_POLARS.get(spark_type)
                if name and pl_name and hasattr(polars, pl_name):
                    schema_dict[name] = getattr(polars, pl_name)
            if schema_dict:
                return polars.DataFrame(schema=schema_dict)

        # --- 3. Empty frame --------------------------------------
        return polars.DataFrame()

    def write(self, role_name, simple_name, data, overwrite_columns, compression_level=1, newer_than=None, delete_only=False, lineage=None):
        """
        Writes an Arrow table into the target SimpleTable with overlap handling.

        Monitoring is fully decoupled: we only enqueue a metric AFTER the lock is released.

        Args:
            lineage: Optional dict describing the upstream origin of this data.
                Stored as a JSON string in the monitoring stats for data lineage
                tracing.  Conventioned keys:

                    source_type     — "staging_ingest", "pipe_transform",
                                      "api_upload", "spark_job", "backfill",
                                      "manual"
                    source_id       — identifier of the upstream source
                    source_tables   — list of upstream table names
                    source_query    — SQL/transform that produced this data
                    staging_name    — staging area name (ingest path)
                    pipe_name       — pipe name (ingest path)
                    job_id          — batch job correlation id
                    run_id          — batch run correlation id
                    source_files    — list of upstream file paths/URIs
                    schema_version  — version tag of the incoming schema
                    tags            — free-form dict for filtering/grouping

        Raises:
            MirrorPublicationError: the authoritative core snapshot committed,
                but at least one configured mirror failed. Callers must not
                blindly retry the mutation; use the exception's snapshot and
                commit identifiers to reconcile the mirror.
        """
        qid = str(uuid.uuid4())
        lp = lambda msg: f"[write][qid={qid}][super={self.super_table.super_name}][table={simple_name}] {msg}"

        t0 = time.time()
        t_last = t0
        profiler = Profiler()
        # Convenience view: top-level stages still indexed by the original
        # short names ("lock", "snapshot", "overlap", ...) so the existing
        # log line keeps working unchanged.
        timings = profiler.timings

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        token = None
        result_tuple = None
        stats_payload = None
        mirror_error: Exception | None = None
        mirror_snapshot_path: str | None = None
        monitoring_error: MonitoringDurabilityError | None = None

        try:
            logger.debug(lp(f"➡️ Starting write(overwrite_cols={overwrite_columns}, compression={compression_level}, newer_than={newer_than}, delete_only={delete_only})"))

            # --- Access control ------------------------------------------------
            policy_columns = list(getattr(data, "column_names", ()) or ())
            if isinstance(overwrite_columns, (list, tuple)):
                policy_columns.extend(overwrite_columns)
            if isinstance(newer_than, str):
                policy_columns.append(newer_than)
            # Preserve first spelling/order while keeping policy work bounded
            # to the actual mutation dependencies.
            policy_columns = list(dict.fromkeys(str(c) for c in policy_columns))
            access_args = dict(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=simple_name,
                columns=policy_columns,
            )
            target_existed = self.catalog.leaf_exists(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )
            if target_existed:
                check_write_access(**access_args)
            else:
                check_create_access(**access_args)
            mark("access")

            # --- Convert input -------------------------------------------------
            dataframe: DataFrame = polars.from_arrow(data)
            incoming_rows = dataframe.height
            incoming_columns = dataframe.width
            mark("convert")

            # --- Validate ------------------------------------------------------
            # Runs before __rowid__ reservation so invalid input never burns a
            # range of the per-table Redis id counter.
            self.validation(dataframe, simple_name, overwrite_columns, newer_than, delete_only)
            mark("validate")

            # --- Inject __timestamp__ system column ---------------------------
            # Added to every non-delete write (drives the date-partitioned
            # layout and tight row-group zonemaps). Together with __rowid__ it
            # is hidden from query output by the read view's
            # ``EXCLUDE (__rowid__, __timestamp__)`` projection.
            #
            # System-owned, exactly like __rowid__ above: ALWAYS overwrite any
            # caller-supplied __timestamp__ instead of preserving it.  It is a
            # reserved internal column that is both the dedup ORDER BY key (newest
            # per key wins) and the source of the __p_year__/month/day partition
            # derivation (processing.py); letting a caller inject an arbitrary value
            # (wrong dtype, non-UTC, or chosen to game which row wins) would
            # silently corrupt partitioning and dedup.  ``newer_than`` is the
            # supported, explicit mechanism for caller-controlled conflict
            # resolution.
            if not delete_only:
                dataframe = dataframe.with_columns(
                    polars.lit(datetime.now(timezone.utc)).alias("__timestamp__")
                )
            mark("dedup_ts")

            # --- Per-simple Redis lock ----------------------------------------
            token = self.catalog.acquire_simple_lock(
                self.super_table.organization, self.super_table.super_name, simple_name,
                ttl_s=30, timeout_s=60
            )
            if not token:
                raise TimeoutError(f"Could not acquire lock for simple '{simple_name}'")
            mark("lock")
            reserve_count = 0 if delete_only else incoming_rows
            begin_mutation = getattr(
                type(self.catalog), "begin_table_mutation", None,
            )
            mutation_context: dict[str, Any] | None = None
            if callable(begin_mutation):
                mutation_context = self.catalog.begin_table_mutation(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    lock_token=token,
                    reserve_count=reserve_count,
                )
                locked_target_exists = mutation_context.get("leaf") is not None
            else:
                mutation_fence = getattr(
                    type(self.catalog), "check_table_mutation_allowed", None,
                )
                if callable(mutation_fence):
                    self.catalog.check_table_mutation_allowed(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        lock_token=token,
                    )

                # Compatibility path for catalog adapters without the fused
                # mutation-context primitive.
                locked_target_exists = self.catalog.leaf_exists(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                )

            # Pin the create-vs-update authorization decision at the table
            # lease boundary.  A concurrent creator can publish the leaf after
            # our optimistic preflight but before this writer acquires the
            # lease; CREATE permission must never authorize mutation of that
            # newly-existing table.
            if not target_existed and locked_target_exists:
                check_write_access(**access_args)

            # --- Read last snapshot (via leaf pointer) ------------------------
            # A target that existed at authorization time must not be
            # implicitly recreated if another actor deletes it before we take
            # the table lock.  Such a recreation requires CREATE, not WRITE.
            simple_table = SimpleTable(
                self.super_table,
                simple_name,
                create_if_missing=not locked_target_exists,
                catalog=self.catalog,
                _live_leaf_verified=bool(locked_target_exists),
                _pinned_leaf=(
                    mutation_context.get("leaf")
                    if mutation_context is not None and locked_target_exists
                    else None
                ),
            )
            if mutation_context is not None and not locked_target_exists:
                # Initialization publishes the first exact leaf under the
                # namespace fence. Re-enter the single atomic context boundary
                # after it releases that fence so the first write can pin and
                # reserve from the initialized snapshot too.
                mutation_context = self.catalog.begin_table_mutation(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    lock_token=token,
                    reserve_count=reserve_count,
                )
                pinned_leaf = mutation_context.get("leaf")
                if not isinstance(pinned_leaf, dict):
                    raise RuntimeError(
                        "Initialized table has no pinned Redis leaf"
                    )
                simple_table._pinned_leaf = dict(pinned_leaf)

            # Configuration updates use this same lease.  The fused context
            # reads it at the exact leaf boundary; adapters retain the strict
            # authoritative read under the lease.
            if mutation_context is not None:
                table_config = dict(
                    mutation_context.get("table_config") or {}
                )
                self._table_config_cache[simple_name] = dict(table_config)
            else:
                table_config = self._get_table_config(simple_name)

            last_simple_table, last_simple_table_path = simple_table.get_simple_table_snapshot()
            # A current deletion-vector is immutable correctness metadata.  Do
            # not let a mutation bless a legacy/unsealed pointer into a new
            # snapshot: recovery must first supply both independent seals.
            prior_tombstone_rows = self._declared_tombstone_rows(last_simple_table)
            prior_tombstone_digest = self._declared_tombstone_digest(last_simple_table)
            mark("snapshot")

            if mutation_context is not None:
                enabled_mirrors = list(mutation_context.get("mirrors") or [])
            else:
                enabled_mirrors = self._get_enabled_mirrors("this mutation")

            # Reserve only after acquiring the table lock and pinning the exact
            # base snapshot.  The immutable high-watermark recovers safely when
            # Redis is flushed/restored and prevents a new row from reusing an ID
            # still referenced by data or a deletion-vector.
            delete_all = bool(delete_only and not overwrite_columns)
            # A targeted delete allocates no new IDs but can publish a deletion
            # vector and keeps physical resources alive.  Legacy snapshots must
            # therefore derive and persist their exact global floor now.  Only
            # delete-all may skip the one-time scan because its successor keeps
            # no physical rows and emits no vector.
            pinned_floor = (
                mutation_context.get("rowid_floor")
                if mutation_context is not None else None
            )
            pinned_reservation = (
                mutation_context.get("rowid_reservation")
                if mutation_context is not None else None
            )
            snapshot_floor = last_simple_table.get("rowid_high_watermark")
            if (
                type(pinned_floor) is int
                and type(snapshot_floor) is int
                and pinned_floor == snapshot_floor
                and (
                    (reserve_count > 0 and pinned_reservation is not None)
                    or reserve_count == 0
                )
            ):
                if reserve_count > 0:
                    start_rowid, rowid_high_watermark = pinned_reservation
                else:
                    start_rowid, rowid_high_watermark = 0, pinned_floor
            else:
                start_rowid, rowid_high_watermark = self._reserve_snapshot_rowids(
                    snapshot=last_simple_table,
                    simple_name=simple_name,
                    count=reserve_count,
                    profiler=profiler,
                    lock_token=token,
                    require_floor=not delete_all,
                )
            if rowid_high_watermark is not None:
                last_simple_table["rowid_high_watermark"] = rowid_high_watermark
            if reserve_count > 0:
                dataframe = dataframe.with_columns(
                    polars.int_range(
                        start_rowid,
                        start_rowid + reserve_count,
                        dtype=polars.Int64,
                    ).alias(ROWID_COL)
                )
            mark("rowid")

            # --- Detect overlaps ----------------------------------------------
            overlapping_files = find_overlapping_files(
                last_simple_table, dataframe, overwrite_columns, locking=None,
                table_config=table_config,
                profiler=profiler,
            )
            mark("overlap")
            if overwrite_columns:
                _snap_files = len(last_simple_table.get("resources") or [])
                _cand = sum(1 for _, ov, _ in overlapping_files if ov)
                logger.debug(lp(
                    f"step[overlap]: {_cand}/{_snap_files} existing file(s) are overwrite "
                    f"candidates on {overwrite_columns} "
                    f"(snapshot has no per-file key stats → every file is suspect)"
                ))

            # --- Stats-driven file pruning (consumer 5a) ----------------------
            # Narrow the overwrite/delete candidate set using the external stats
            # artifact: drop files that provably hold none of the incoming keys.
            # Conservative — only removes files with zero possible matches, so
            # the tombstone result is identical to the unpruned path. The probe
            # is the in-memory dataframe range (df-probe): the file we are about
            # to write carries identical footer min/max, so comparing the df
            # range to the stored stats is equivalent to comparing footers,
            # without opening any file.
            if overwrite_columns:
                stats_file = last_simple_table.get("stats_file")
                if stats_file:
                    current_stats_cache_identity = stats_cache_identity(
                        stats_file,
                        organization=self.super_table.organization,
                        storage=self.super_table.storage,
                    )
                    stored_stats_df = load_stats(
                        stats_file,
                        allow_cache=True,
                        cache_identity=current_stats_cache_identity,
                        profiler=profiler,
                    )
                    if stored_stats_df is not None and "stats_rows" in last_simple_table:
                        expected_stats_rows = int(last_simple_table.get("stats_rows") or 0)
                        if stored_stats_df.height != expected_stats_rows:
                            logger.warning(lp(
                                "stats artifact row-count mismatch; overwrite pruning disabled"
                            ))
                            stored_stats_df = None
                    if stored_stats_df is not None:
                        stored_stats_df = stats_for_complete_files(
                            stored_stats_df,
                            {
                                r.get("file"): r.get("rows")
                                for r in (last_simple_table.get("resources") or [])
                                if isinstance(r, dict) and r.get("file")
                            },
                            {
                                r.get("file"): resource_stats_seal(r)
                                for r in (last_simple_table.get("resources") or [])
                                if isinstance(r, dict) and r.get("file")
                            },
                            stats_path=current_stats_cache_identity,
                        )
                    if stored_stats_df is not None and stored_stats_df.height > 0:
                        probe = probe_ranges_from_df(dataframe, overwrite_columns)
                        _probe_desc = {
                            c: (f"{v[0]}[{v[1]}..{v[2]}]" if v else "unconstrained(null/unsupported)")
                            for c, v in probe.items()
                        }
                        before = len(overlapping_files)
                        overlapping_files = prune_overlapping_files_by_stats(
                            overlapping_files,
                            stored_stats_df,
                            probe,
                            profiler=profiler,
                        )
                        pruned = before - len(overlapping_files)
                        logger.debug(lp(
                            f"step[stats-prune]: df-probe {_probe_desc} vs {stored_stats_df.height} "
                            f"stored stat row(s) → kept {len(overlapping_files)}/{before}, "
                            f"pruned {pruned} (no data file opened)"
                        ))
                        if pruned > 0:
                            logger.info(lp(f"stats pruning: skipped {pruned}/{before} candidate files"))
                    else:
                        logger.debug(lp(
                            "step[stats-prune]: stats artifact empty → no pruning, all candidates retained"
                        ))
                else:
                    logger.debug(lp(
                        "step[stats-prune]: snapshot has no stats_file → no pruning, all candidates retained"
                    ))
                mark("stats_prune")

            prev_tombstone_path = last_simple_table.get("tombstone")
            current_resources = last_simple_table.get("resources") or []
            # Conflict resolution must see the logical live relation.  A dead
            # physical row with a larger newer_than value cannot make a valid
            # reinsertion stale, and must not be tombstoned again.
            prev_dv_df = (
                load_tombstone(
                    prev_tombstone_path,
                    cache_identity=tombstone_cache_identity(
                        prev_tombstone_path,
                        organization=self.super_table.organization,
                        storage=self.super_table.storage,
                    ),
                    allow_cache=True,
                    required=True,
                    expected_rows=prior_tombstone_rows,
                    expected_digest=prior_tombstone_digest,
                    allowed_files=self._snapshot_resource_files(last_simple_table),
                    profiler=profiler,
                )
                if (
                    prev_tombstone_path
                    and not delete_all
                    and (overwrite_columns or delete_only or enabled_mirrors)
                ) else None
            )

            # File cache: used only by delete_only's identify_all_rowids below.
            file_cache = {}

            # --- Overwrite resolution: stale-row filtering + delete-pair -------
            # identification in one native pushdown probe over the overlapping
            # files (column projection, row-group skipping, null-safe SEMI JOIN)
            # instead of per-file eager Polars reads. Local files use the
            # Island-native scanner; explicit remote compatibility may use
            # DuckDB. Returns
            # the stale-filtered incoming df plus the (file, __rowid__) delete
            # pairs derived from the surviving keys; falls back to the polars
            # oracle on any probe/derive failure.  delete_only (no
            # overwrite_columns) is handled separately in the deletion block.
            resolved_delete_pairs = None
            if overwrite_columns:
                pre_filter_count = dataframe.height
                dataframe, resolved_delete_pairs = resolve_overwrite_writes(
                    incoming_df=dataframe,
                    overlapping_files=overlapping_files,
                    overwrite_columns=overwrite_columns,
                    newer_than_col=newer_than,
                    profiler=profiler,
                    existing_tombstones=prev_dv_df,
                    storage=self.super_table.storage,
                )
                mark("resolve_overwrite")
                _counts = profiler.counts
                _fallback = bool(_counts.get("overwrite_resolve_fallback"))
                _probe_backend = (
                    "polars-fallback" if _fallback else
                    "island-native" if _counts.get(
                        "overwrite_resolve_probe_island_native"
                    ) else
                    "duckdb-remote" if _counts.get(
                        "overwrite_resolve_probe_duckdb_remote"
                    ) else
                    "no-overlap"
                )
                logger.debug(lp(
                    f"step[probe-resolve] via {_probe_backend}: "
                    f"matched {_counts.get('probe_rows_matched', _counts.get('delete_rows_matched', 0))} "
                    f"existing row(s) on {overwrite_columns} → "
                    f"{len(resolved_delete_pairs or [])} (file,__rowid__) delete pair(s); "
                    f"{dataframe.height}/{pre_filter_count} incoming row(s) survive"
                ))
                if newer_than:
                    skipped = pre_filter_count - dataframe.height
                    if skipped > 0:
                        logger.info(lp(f"newer_than={newer_than}: skipped {skipped}/{pre_filter_count} stale rows"))
                    if dataframe.height == 0:
                        logger.info(lp("newer_than: all incoming rows are stale — skipping write"))
                        mark("newer_than")
                        total_columns = incoming_columns
                        result_tuple = (total_columns, 0, 0, 0)
                        stats_payload = {
                            "query_id": qid,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                            "organization": self.super_table.organization,
                            "super_name": self.super_table.super_name,
                            "role_name": role_name,
                            "table_name": simple_name,
                            "overwrite_columns": overwrite_columns,
                            "compression_level": compression_level,
                            "newer_than": newer_than,
                            "delete_only": delete_only,
                            "incoming_rows": incoming_rows,
                            "incoming_columns": incoming_columns,
                            "inserted": 0,
                            "deleted": 0,
                            "total_rows": 0,
                            "total_columns": total_columns,
                            "new_resources": 0,
                            "sunset_files": 0,
                            "skipped_stale": skipped,
                            "lineage": _safe_json(lineage or {}),
                            "duration": round(time.time() - t0, 6),
                            "timings": profiler.emit_timings(),
                            "counts": profiler.emit_counts(),
                        }
                        # Don't return here — fall through to finally (lock release)
                        # and the post-finally monitoring block.  Returning inside the
                        # try block would either skip monitoring or run it while the
                        # Redis data lock is still held.
                    else:
                        mark("newer_than")

            # --- Deletion-vector (tombstone) logic ----------------------------
            # Merge-on-read model: every write tombstones the __rowid__s of the
            # old matching rows (delete + upsert) and appends the incoming rows
            # as a brand-new immutable file — no in-place merge of old files.
            # The deletion-vector is carried forward and extended each write;
            # dead rows are physically dropped only at the compaction threshold.
            # Skip if early exit already set result_tuple (all rows were stale).
            if result_tuple is None:
                new_resources = []
                # A whole-table delete has a much stronger proof than a rowid
                # vector: the successor references none of the current physical
                # files.  Sunset them directly and clear the DV; reading every
                # rowid and uploading an intermediate vector is wasted O(N) I/O.
                sunset_files = {
                    r.get("file") for r in current_resources
                    if isinstance(r, dict) and r.get("file")
                } if delete_all else set()

                # Load the current deletion-vector once: used both to exclude
                # already-tombstoned rows from this write's deletes (below) and,
                # via prev_df, to extend the vector without a second read.
                # required=True: a DV that exists but cannot be read must abort
                # the write, never be treated as empty — silently dropping the
                # carried-forward vector would resurrect previously deleted rows.
                # Cache-first: if this process wrote the table on a prior loop
                # iteration, the current deletion-vector is already in memory
                # (seeded below after the pointer is pinned), so this is a hit
                # with no storage round-trip.  required=True preserves the
                # carry-forward safety on a genuine miss (abort, never truncate).
                # 1. Identify which existing rows this write deletes/replaces.
                #    overwrite_columns drives the anti-join key (delete + upsert);
                #    pure appends (no overwrite_columns) tombstone nothing.  The
                #    pairs were already derived (from the surviving keys) by the
                #    resolve_overwrite_writes probe above.
                new_delete_pairs = []
                if overwrite_columns:
                    new_delete_pairs = resolved_delete_pairs or []
                elif delete_only:
                    # delete-all is represented by sunsetting every resource,
                    # not by constructing an O(rows) intermediate vector.
                    new_delete_pairs = []

                # ``resolve_overwrite_writes`` anti-joins the physical matches
                # against the existing vector by composite (file, rowid), in
                # DuckDB and in its Polars fallback.  Do not build a second
                # table-wide Python set here: at the threshold that expands one
                # million compact Int64 values into tens of MiB of Python
                # objects while the table lock is held.
                if delete_all:
                    physical_rows = sum(
                        max(0, int(r.get("rows") or 0))
                        for r in current_resources if isinstance(r, dict)
                    )
                    previously_dead = max(
                        0, int(last_simple_table.get("tombstone_rows", 0) or 0)
                    )
                    deleted = max(0, physical_rows - previously_dead)
                else:
                    deleted = len(new_delete_pairs)
                mark("identify_deletes")
                logger.debug(lp(
                    f"step[deletes]: tombstoning {deleted} live row(s) this write "
                    "(existing deletion-vector applied by composite identity)"
                ))

                # 2. + 3.  Write the incoming rows as a new data file (insert/
                #    upsert side) AND carry-forward/extend the deletion-vector
                #    tombstone file.  These two object-store PUTs are independent:
                #    neither reads the other's output and they write to disjoint
                #    dirs (data/ vs tombstone/), so they run concurrently to
                #    overlap the two round-trips.  delete_only carries only
                #    predicate columns → nothing to insert.  No new deletes →
                #    build_tombstone reuses the previous file (combined_df=None).
                #
                #    Profiler is NOT thread-safe, so each branch records into its
                #    own sub-profiler which the parent merges after the join;
                #    each branch also measures its own wall time so the per-phase
                #    monitoring timings stay meaningful despite the overlap.
                #    Footers of files written via the write_bytes path are captured
                #    in footer_md_cache so stats extraction (step 6) reuses them
                #    instead of re-downloading each freshly-written file.
                footer_md_cache = {}
                tombstone_dir = os.path.join(simple_table.simple_dir, "tombstone")
                do_insert = (not delete_only and dataframe.height > 0)
                projected_tombstone_rows = (
                    (prev_dv_df.height if prev_dv_df is not None else 0)
                    + len(new_delete_pairs)
                )
                defer_tombstone_upload = bool(
                    new_delete_pairs
                    and (
                        projected_tombstone_rows
                        >= _max_tombstone_rows(table_config)
                        or enabled_mirrors
                    )
                )
                tombstone_validation_out: dict[str, Any] = {}

                def _write_data_branch():
                    sub = Profiler()
                    t = time.perf_counter()
                    write_parquet_and_collect_resources(
                        write_df=dataframe,
                        overwrite_columns=[],
                        data_dir=simple_table.data_dir,
                        new_resources=new_resources,
                        compression_level=compression_level,
                        profiler=sub,
                        footer_md_out=footer_md_cache,
                    )
                    return sub, time.perf_counter() - t

                def _write_tombstone_branch():
                    sub = Profiler()
                    t = time.perf_counter()
                    if delete_all:
                        return None, None, sub, time.perf_counter() - t
                    if not new_delete_pairs:
                        # Most writes are pure appends to a table without a DV.
                        # Carry an existing immutable pointer directly as well;
                        # no builder call, object-store read, or executor task.
                        return (
                            prev_tombstone_path, None, sub,
                            time.perf_counter() - t,
                        )
                    tp, cdf = build_tombstone_file(
                        tombstone_dir=tombstone_dir,
                        prev_tombstone_path=prev_tombstone_path,
                        new_pairs=new_delete_pairs,
                        compression_level=compression_level,
                        profiler=sub,
                        prev_df=prev_dv_df,
                        persist=not defer_tombstone_upload,
                        prev_df_validated=prev_dv_df is not None,
                        validation_out=(
                            tombstone_validation_out
                            if not defer_tombstone_upload else None
                        ),
                    )
                    return tp, cdf, sub, time.perf_counter() - t

                tombstone_work = bool(not delete_all and new_delete_pairs)
                if do_insert and tombstone_work:
                    with ThreadPoolExecutor(max_workers=2) as _ex:
                        _f_data = _ex.submit(_write_data_branch)
                        _f_tomb = _ex.submit(_write_tombstone_branch)
                        # .result() re-raises in the parent: a failure in either
                        # PUT aborts the write before any snapshot commit, exactly
                        # as the former sequential path did (an orphaned immutable
                        # file no snapshot references is harmless garbage).
                        data_sub, data_secs = _f_data.result()
                        tombstone_path, combined_tombstone_df, tomb_sub, tomb_secs = (
                            _f_tomb.result()
                        )
                    profiler.merge(data_sub)
                    profiler.merge(tomb_sub)
                    inserted = dataframe.height
                elif do_insert:
                    data_sub, data_secs = _write_data_branch()
                    profiler.merge(data_sub)
                    tombstone_path = prev_tombstone_path
                    combined_tombstone_df = None
                    tomb_secs = 0.0
                    inserted = dataframe.height
                else:
                    tombstone_path, combined_tombstone_df, tomb_sub, tomb_secs = (
                        _write_tombstone_branch()
                    )
                    profiler.merge(tomb_sub)
                    data_secs = 0.0
                    inserted = 0

                # Assign the two per-phase timings from each branch's own measured
                # wall time (they overlapped, so the serial mark() deltas would
                # misattribute the time), then advance the mark() baseline.
                timings["write_parquet"] = data_secs
                timings["build_tombstone"] = tomb_secs
                t_last = time.time()
                logger.debug(lp(
                    f"step[write]: appended {inserted} incoming row(s) as {len(new_resources)} "
                    f"new immutable file(s) (no existing data file rewritten)"
                ))

                # Track the live deletion-vector row count so meta reads can
                # deduct dead rows from the physical resource row totals.
                # New deletes → combined_tombstone_df is the full deduped DV
                # (exact count); pure carry-forward → reuse the previous count.
                tombstone_rows = (
                    0 if delete_all else (
                        combined_tombstone_df.height
                        if combined_tombstone_df is not None
                        else (
                            prev_dv_df.height
                            if (
                                prev_dv_df is not None
                                and prior_tombstone_rows is None
                            )
                            else int(prior_tombstone_rows or 0)
                        )
                    )
                )
                logger.debug(lp(
                    f"step[tombstone]: deletion-vector now {tombstone_rows} row(s) "
                    f"({'rewritten' if combined_tombstone_df is not None else 'carried forward unchanged'})"
                ))

                # 3b. Eager reclamation of fully-dead files.  Any existing data
                #     file whose every physical row is now tombstoned is 100%
                #     dead: drop it from the snapshot for free (no rewrite) and
                #     remove its rowids from the vector.  Without this, fully
                #     deleted files linger until the compaction threshold,
                #     bloating the snapshot and getting re-scanned by every later
                #     overwrite probe.  Only runs when the vector changed this
                #     write (combined_tombstone_df is not None) — a carry-forward
                #     can create no newly-dead file.
                if (
                    combined_tombstone_df is not None
                    and not delete_all
                    and projected_tombstone_rows < _max_tombstone_rows(table_config)
                    and not enabled_mirrors
                ):
                    reclaimed_files, reclaimed_tomb_path, reclaimed_dv = (
                        reclaim_fully_dead_files(
                            resources=last_simple_table.get("resources") or [],
                            combined_dv=combined_tombstone_df,
                            tombstone_dir=tombstone_dir,
                            compression_level=compression_level,
                            profiler=profiler,
                            persist=not defer_tombstone_upload,
                            assume_valid=True,
                        )
                    )
                    if reclaimed_files:
                        sunset_files |= reclaimed_files
                        tombstone_path = reclaimed_tomb_path
                        combined_tombstone_df = reclaimed_dv
                        tombstone_rows = (
                            reclaimed_dv.height if reclaimed_dv is not None else 0
                        )
                        logger.info(lp(
                            f"reclaimed {len(reclaimed_files)} fully-deleted "
                            f"file(s); deletion-vector now {tombstone_rows} rows"
                        ))
                mark("reclaim_dead_files")

                # 4. Threshold compaction (two triggers, same physical step):
                #      (a) the deletion-vector grew past max_tombstone_rows, or
                #      (b) the small files tripped the auto-compaction gate.
                #    Both must FIRST physically drop tombstoned rows (Phase A)
                #    and only THEN merge small files (Phase B): compact_resources
                #    rewrites data files WITHOUT consulting the deletion-vector,
                #    so sunsetting a vector-referenced file would orphan its dead
                #    rows (hidden on read, never reclaimable).  Draining first
                #    guarantees Phase B only ever sees vector-free survivors.
                post_write_resources = (
                    [r for r in (last_simple_table.get("resources") or [])
                     if r.get("file") not in sunset_files]
                    + new_resources
                )
                compaction_gate = should_compact_small_files(
                    post_write_resources, table_config
                )
                tombstone_threshold_hit = (
                    tombstone_rows >= _max_tombstone_rows(table_config)
                )

                # Snapshot the shared I/O counters BEFORE the compaction phases so
                # the reads/writes they perform can be attributed precisely (the
                # incoming-data write already ran above, so any delta below is
                # purely compaction).  Trackers default to "phase did not run".
                _cio_files0 = profiler.counts.get("files_written", 0)
                _cio_bw0 = profiler.counts.get("bytes_written", 0)
                _cio_fr0 = profiler.counts.get("files_read", 0)
                _cio_br0 = profiler.counts.get("bytes_read", 0)
                _live_before = len(post_write_resources)
                _tomb_phase_ran = False
                _tomb_removed = 0
                _tomb_files_touched = 0
                _tomb_files_written = 0
                _tomb_files_total = 0
                _comp_considered = 0
                _comp_small_consumed = 0
                _comp_large_tombstone_consumed = 0
                _comp_files_written = 0
                _comp_rows = 0
                residual_tombstone_files: set[str] = set()
                compaction_ran = False
                fused_compaction_ran = False

                # Phase A — drain the deletion-vector when either trigger fires
                # and a vector is actually live (freshly built this write OR
                # carried forward from a prior one).
                mirror_requires_drain = bool(enabled_mirrors and tombstone_rows > 0)
                if tombstone_threshold_hit or compaction_gate or mirror_requires_drain:
                    dv_to_drain = combined_tombstone_df
                    if dv_to_drain is None and tombstone_path:
                        # Pure carry-forward: the pointer is unchanged, so the
                        # live vector is exactly the one already loaded at the
                        # top of this block — reuse it instead of a second
                        # storage read (fall back to a read only if it wasn't
                        # loaded, which shouldn't happen when tombstone_path is
                        # set, but stays correct if it ever does).
                        dv_to_drain = (
                            prev_dv_df
                            if prev_dv_df is not None
                            else load_tombstone(
                                tombstone_path,
                                cache_identity=tombstone_cache_identity(
                                    tombstone_path,
                                    organization=self.super_table.organization,
                                    storage=self.super_table.storage,
                                ),
                                allow_cache=True,
                                required=True,
                                expected_rows=tombstone_rows,
                                expected_digest=(
                                    prior_tombstone_digest
                                    if tombstone_path == prev_tombstone_path
                                    else None
                                ),
                                allowed_files=self._snapshot_resource_files(
                                    last_simple_table
                                ),
                                profiler=profiler,
                            )
                        )
                    if dv_to_drain is not None and dv_to_drain.height > 0:
                        # When both physical phases are due, consume them in one
                        # source pass.  Partial-DV survivors flow directly into
                        # final ~chunk-sized outputs instead of being uploaded,
                        # downloaded, and encoded again by Phase B.  A mirror
                        # keeps the established targeted drain because its
                        # fail-closed residual handling happens before any
                        # unrelated small-file work.
                        if compaction_gate and not enabled_mirrors:
                            _small_consumed0 = profiler.counts.get(
                                "compact_small_files_consumed", 0,
                            )
                            _large_tombstone_consumed0 = profiler.counts.get(
                                "compact_large_tombstone_files_consumed", 0,
                            )
                            (
                                considered,
                                fused_rows,
                                fused_new,
                                fused_sunset,
                                residual_dv,
                            ) = compact_resources(
                                snapshot={"resources": post_write_resources},
                                tombstone_df=dv_to_drain,
                                return_residual=True,
                                data_dir=simple_table.data_dir,
                                compression_level=compression_level,
                                table_config=table_config,
                                small_only=True,
                                profiler=profiler,
                                footer_md_out=footer_md_cache,
                            )
                            residual_tombstone_files = (
                                self._tombstone_referenced_files(residual_dv)
                                if residual_dv.height else set()
                            )
                            all_tombstone_files = (
                                self._tombstone_referenced_files(dv_to_drain)
                            )
                            tomb_sunset = (
                                all_tombstone_files
                                - residual_tombstone_files
                            )
                            tomb_new = []
                            removed = dv_to_drain.height - residual_dv.height

                            sunset_files |= fused_sunset
                            new_resources = [
                                resource
                                for resource in (new_resources + fused_new)
                                if resource.get("file") not in sunset_files
                            ]
                            _comp_considered = considered
                            _comp_small_consumed = (
                                profiler.counts.get(
                                    "compact_small_files_consumed", 0,
                                ) - _small_consumed0
                            )
                            _comp_large_tombstone_consumed = (
                                profiler.counts.get(
                                    "compact_large_tombstone_files_consumed", 0,
                                ) - _large_tombstone_consumed0
                            )
                            _comp_files_written = len(fused_new)
                            _comp_rows = fused_rows
                            fused_compaction_ran = True
                            compaction_ran = bool(fused_new or fused_sunset)
                        else:
                            removed, tomb_new, tomb_sunset, residual_dv = (
                                self._unpack_tombstone_compaction(
                                    compact_tombstones(
                                        snapshot=last_simple_table,
                                        tombstone_df=dv_to_drain,
                                        data_dir=simple_table.data_dir,
                                        compression_level=compression_level,
                                        table_config=table_config,
                                        profiler=profiler,
                                        return_residual=True,
                                        footer_md_out=footer_md_cache,
                                    )
                                )
                            )
                            new_resources.extend(tomb_new)
                            sunset_files |= tomb_sunset
                        if residual_dv.height > 0:
                            residual_tombstone_files = self._tombstone_referenced_files(
                                residual_dv
                            )
                            if tomb_new or tomb_sunset or tombstone_path is None:
                                # Some groups were physically consumed. Publish a
                                # successor vector containing only the untouched
                                # groups; the old pointer must never be cleared.
                                tombstone_path, combined_tombstone_df = (
                                    persist_tombstone_frame(
                                        tombstone_dir=tombstone_dir,
                                        frame=residual_dv,
                                        compression_level=compression_level,
                                        profiler=profiler,
                                    )
                                )
                            else:
                                # Nothing was consumed. Keep the existing/newly
                                # built immutable vector as-is.
                                combined_tombstone_df = dv_to_drain
                            tombstone_rows = residual_dv.height
                        else:
                            tombstone_path = None
                            combined_tombstone_df = None
                            tombstone_rows = 0
                        if mirror_requires_drain and residual_dv.height > 0:
                            raise RuntimeError(
                                "Cannot publish a mirror while deletion-vector "
                                "entries remain physically undrained"
                            )
                        _tomb_phase_ran = True
                        _tomb_removed = removed
                        _tomb_files_touched = len(tomb_sunset)
                        _tomb_files_written = len(tomb_new)
                        # Distinct files named in the deletion-vector — set by
                        # compact_tombstones this call (one call per write).
                        _tomb_files_total = int(
                            profiler.counts.get("tombstone_files_total", 0)
                        )
                        logger.info(lp(
                            f"{'fused ' if fused_compaction_ran else ''}"
                            f"tombstone compaction removed {removed} rows "
                            f"from {len(tomb_sunset)} file(s)"
                        ))

                # 5. Pin the (carried-forward / new / cleared) tombstone pointer
                #    and its row count.
                last_simple_table["tombstone"] = tombstone_path
                last_simple_table["tombstone_rows"] = tombstone_rows
                if tombstone_path:
                    digest_frame = (
                        combined_tombstone_df
                        if combined_tombstone_df is not None else prev_dv_df
                    )
                    if (
                        tombstone_path == prev_tombstone_path
                        and combined_tombstone_df is None
                        and prior_tombstone_digest
                    ):
                        # The immutable pointer did not change and was already
                        # verified while loading. Preserve its seal instead of
                        # sorting and hashing the entire vector again.
                        last_simple_table["tombstone_digest"] = prior_tombstone_digest
                    elif digest_frame is not None:
                        if digest_frame is tombstone_validation_out.get("frame"):
                            last_simple_table["tombstone_digest"] = (
                                tombstone_validation_out["digest"]
                            )
                        else:
                            last_simple_table["tombstone_digest"] = tombstone_digest(
                                digest_frame, assume_valid=True
                            )
                    elif tombstone_path == prev_tombstone_path:
                        # Pure append below threshold: preserve the already
                        # validated immutable seal without loading a large DV.
                        last_simple_table["tombstone_digest"] = prior_tombstone_digest
                    else:
                        raise RuntimeError(
                            "Cannot seal successor deletion-vector without its frame"
                        )
                else:
                    last_simple_table["tombstone_digest"] = None
                # Seed the in-process cache so the NEXT write's carry-forward read
                # (prev_dv_df above) is a pure memory hit.  The frame is the fresh
                # build/reclaim result when this write changed the vector, else the
                # already-loaded prev frame for a pure carry-forward.  No-op when
                # the vector was fully consumed this write (tombstone_path is None).
                cache_tombstone(
                    tombstone_path,
                    combined_tombstone_df if combined_tombstone_df is not None else prev_dv_df,
                    cache_identity=(
                        tombstone_cache_identity(
                            tombstone_path,
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        )
                        if tombstone_path else None
                    ),
                    expected_rows=tombstone_rows if tombstone_path else None,
                    expected_digest=(
                        last_simple_table.get("tombstone_digest")
                        if tombstone_path else None
                    ),
                    assume_valid=bool(tombstone_path),
                )
                mark("compact_tombstones")

                # Phase B — auto small-file compaction.  Merge the accumulated
                # small files (existing survivors + the file just written) once
                # the gate is open so the file count stays bounded.  The vector
                # was drained above, so every surviving file is safe to sunset.
                # Result folds into the SAME snapshot commit below (new_resources
                # / sunset_files feed build_stats and simple_table.update).
                if compaction_gate and not fused_compaction_ran:
                    live_resources = [
                        r for r in (last_simple_table.get("resources") or [])
                        if r.get("file") not in sunset_files
                        and r.get("file") not in residual_tombstone_files
                    ]
                    live_resources += [
                        r for r in new_resources
                        if r.get("file") not in sunset_files
                        and r.get("file") not in residual_tombstone_files
                    ]
                    considered, comp_rows, comp_new, comp_sunset = compact_resources(
                        snapshot={"resources": live_resources},
                        data_dir=simple_table.data_dir,
                        compression_level=compression_level,
                        table_config=table_config,
                        small_only=True,
                        profiler=profiler,
                        footer_md_out=footer_md_cache,
                    )
                    if comp_new or comp_sunset:
                        _comp_considered = considered
                        _comp_small_consumed = considered
                        _comp_large_tombstone_consumed = 0
                        _comp_files_written = len(comp_new)
                        _comp_rows = comp_rows
                        sunset_files |= comp_sunset
                        # A file written above (incoming or tombstone survivor)
                        # may have been re-merged here; drop any new_resources
                        # entry that is now sunset so the snapshot never lists a
                        # file as both live and gone.
                        new_resources = [
                            r for r in (new_resources + comp_new)
                            if r.get("file") not in sunset_files
                        ]
                        compaction_ran = True
                        logger.info(lp(
                            f"auto-compaction merged {considered} small files "
                            f"into {len(comp_new)} file(s) ({comp_rows} rows)"
                        ))
                mark("compact_small")

                # Precise, single-line attribution of the compaction work done
                # INSIDE this write: which gate fired, what each phase touched,
                # and the I/O it cost.  Only emitted when a phase actually ran so
                # ordinary writes stay quiet.  This is the line that makes a
                # "compaction happened during the write" visible at a glance.
                if _tomb_phase_ran or compaction_ran:
                    _final_live = len(
                        [r for r in (last_simple_table.get("resources") or [])
                         if r.get("file") not in sunset_files]
                        + [r for r in new_resources
                           if r.get("file") not in sunset_files]
                    )
                    _triggers = []
                    if tombstone_threshold_hit:
                        _triggers.append("tombstone_threshold")
                    if compaction_gate:
                        _triggers.append("small_file_gate")
                    if mirror_requires_drain:
                        _triggers.append("mirror_requires_clean_files")
                    _cio_files = profiler.counts.get("files_written", 0) - _cio_files0
                    _cio_bw = profiler.counts.get("bytes_written", 0) - _cio_bw0
                    _cio_fr = profiler.counts.get("files_read", 0) - _cio_fr0
                    _cio_br = profiler.counts.get("bytes_read", 0) - _cio_br0
                    if _comp_large_tombstone_consumed:
                        _file_phase = (
                            "fused file phase consumed "
                            f"{_comp_small_consumed} small file(s) and "
                            f"{_comp_large_tombstone_consumed} "
                            "tombstone-referenced large file(s) -> "
                            f"{_comp_files_written} file(s) "
                            f"({_comp_rows} row(s)); unrelated large files "
                            "left untouched"
                        )
                    else:
                        _file_phase = (
                            "small-file phase merged "
                            f"{_comp_small_consumed} small file(s) -> "
                            f"{_comp_files_written} file(s) "
                            f"({_comp_rows} row(s)); large files left untouched"
                        )
                    logger.info(lp(
                        "compaction during write "
                        f"[trigger={'+'.join(_triggers) or 'none'}]: "
                        f"tombstone phase removed {_tomb_removed} row(s) from "
                        f"{_tomb_files_touched}/{_tomb_files_total} deletion-vector file(s), "
                        f"wrote {_tomb_files_written} file(s); "
                        f"{_file_phase}; live files {_live_before} -> {_final_live}; "
                        f"compaction io: read {_cio_fr} file(s)/{_cio_br / 1048576:.1f} MiB, "
                        f"wrote {_cio_files} file(s)/{_cio_bw / 1048576:.1f} MiB"
                    ))

                # 6. Carry forward + extend the external column-statistics parquet.
                #    Read the footers of the newly written data files, drop the
                #    rows of any sunset file, and append the new ones. No new
                #    files and nothing sunset → reuse the previous stats file.
                if delete_all:
                    # No successor resource can use prior footer statistics.
                    # Clearing the pointer is exact and avoids reading + writing
                    # an empty intermediate stats parquet.
                    stats_path, combined_stats_df, stats_rows = None, None, 0
                else:
                    stats_dir = os.path.join(simple_table.simple_dir, "stats")
                    new_data_files = [
                        r.get("file") for r in new_resources
                        if isinstance(r, dict) and r.get("file")
                    ]
                    new_stats_rows = extract_stats_rows(
                        new_data_files, profiler=profiler, footer_md_cache=footer_md_cache
                    )
                    stats_validation_out: dict[str, Any] = {}
                    stats_path, combined_stats_df = build_stats_file(
                        stats_dir=stats_dir,
                        prev_stats_path=last_simple_table.get("stats_file"),
                        new_rows=new_stats_rows,
                        removed_files=sunset_files,
                        compression_level=compression_level,
                        profiler=profiler,
                        prev_cache_identity=(
                            stats_cache_identity(
                                last_simple_table.get("stats_file"),
                                organization=self.super_table.organization,
                                storage=self.super_table.storage,
                            )
                            if last_simple_table.get("stats_file") else None
                        ),
                        validation_out=stats_validation_out,
                    )
                    stats_rows = (
                        combined_stats_df.height
                        if combined_stats_df is not None
                        else int(last_simple_table.get("stats_rows", 0) or 0)
                    )
                last_simple_table["stats_file"] = stats_path
                last_simple_table["stats_rows"] = stats_rows
                # Seed the in-process cache so the next read (this process's next
                # overwrite/delete or query) needs no storage round-trip.
                if combined_stats_df is not None:
                    cache_stats(
                        stats_path,
                        combined_stats_df,
                        cache_identity=stats_cache_identity(
                            stats_path,
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        ),
                        validation=stats_validation_out.get("validation"),
                    )
                mark("build_stats")

                total_rows = inserted
                # Logical (user-facing) width: the injected __rowid__/__timestamp__
                # system columns are hidden from query output, so they are not
                # counted here.
                total_columns = incoming_columns

                # --- Update snapshot on storage -----------------------------------
                # Build effective lineage — auto-generate if caller didn't provide
                eff_lineage = lineage if lineage and isinstance(lineage, dict) else {}
                if not eff_lineage:
                    eff_lineage = {
                        "source_type": "delete" if delete_only else "write",
                        "role_name": role_name,
                        "overwrite_columns": overwrite_columns or [],
                        "delete_only": delete_only,
                        "incoming_rows": incoming_rows,
                        "incoming_columns": incoming_columns,
                        "write_id": qid,
                    }

                # Schema policy: only true update paths change the snapshot
                # schema.  A delete-only write must preserve the previous
                # snapshot's schema because the incoming `dataframe` only
                # carries the delete-predicate columns — passing it as
                # model_df would shrink schema / schemaString to that partial
                # shape even though all parquet files still have full schema.
                # See docs/03_data_model.md "Schema Field Semantics".
                #
                # Physical compaction may retain legacy-only columns in Parquet,
                # but it must not change the logical last-write-wins contract.
                # The current incoming frame remains authoritative on every
                # non-delete write; delete-only preserves the prior metadata.
                schema_model_df = None if delete_only else dataframe
                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    new_resources, sunset_files, schema_model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                    profiler=profiler,
                )
                mark("update_simple")

                # --- Fenced atomic snapshot publication --------------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                payload = new_snapshot_dict

                with profiler.span("redis.set_leaf"):
                    self._publish_snapshot(
                        simple_table=simple_table,
                        simple_name=simple_name,
                        payload=payload,
                        path=new_snapshot_path,
                        base_path=last_simple_table_path,
                        lock_token=token,
                        commit_id=qid,
                        now_ms=now_ms,
                        mirrors=enabled_mirrors,
                        mirror_pin_available=mutation_context is not None,
                        mirror_pin=(
                            mutation_context.get("mirror_pin")
                            if mutation_context is not None else None
                        ),
                        notify_quality=not (
                            simple_name.startswith("__")
                            and simple_name.endswith("__")
                        ),
                    )
                if prev_tombstone_path and prev_tombstone_path != tombstone_path:
                    evict_tombstone(
                        prev_tombstone_path,
                        cache_identity=tombstone_cache_identity(
                            prev_tombstone_path,
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        ),
                    )
                mark("bump_root")

                # --- Optional mirroring ---------------------------------------
                # The core snapshot is already authoritative at this point.
                # Preserve it on mirror failure, but never return an ambiguous
                # success: after monitoring/audit and lock release we raise a
                # structured post-commit error carrying the reconciliation key.
                mirror_snapshot_path = new_snapshot_path
                if enabled_mirrors:
                    try:
                        MirrorFormats.mirror_if_enabled(
                            super_table=self.super_table,
                            table_name=simple_name,
                            simple_snapshot=new_snapshot_dict,
                            mirrors=enabled_mirrors,
                            commit_id=qid,
                            snapshot_path=new_snapshot_path,
                        )
                    except Exception as e:
                        mirror_error = e
                        failed_format = getattr(e, "failed_format", None)
                        stage = (
                            f"mirror:{failed_format}" if failed_format else "mirror"
                        )
                        try:
                            self._fail_mirror_publication(
                                simple_name=simple_name,
                                commit_id=qid,
                                lock_token=token,
                                stage=stage,
                                error=e,
                            )
                        except Exception as state_exc:
                            logger.error(lp(
                                "failed to persist mirror failure state: "
                                f"{state_exc}"
                            ))
                        logger.error(lp(f"mirroring failed after core commit: {e}"))
                    else:
                        try:
                            self._complete_mirror_publication(
                                simple_name=simple_name,
                                commit_id=qid,
                                lock_token=token,
                            )
                        except Exception as e:
                            mirror_error = e
                            try:
                                self._fail_mirror_publication(
                                    simple_name=simple_name,
                                    commit_id=qid,
                                    lock_token=token,
                                    stage="outbox_complete",
                                    error=e,
                                )
                            except Exception as state_exc:
                                logger.error(lp(
                                    "failed to persist mirror completion error: "
                                    f"{state_exc}"
                                ))
                            logger.error(lp(
                                "mirror data published but durable completion "
                                f"state failed: {e}"
                            ))
                mark("mirror")

                # Prepare monitoring payload (NOT writing, only enqueue later)
                stats_payload = {
                    "query_id": qid,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "organization": self.super_table.organization,
                    "super_name": self.super_table.super_name,
                    "role_name": role_name,
                    "table_name": simple_name,
                    "overwrite_columns": overwrite_columns,
                    "compression_level": compression_level,
                    "newer_than": newer_than,
                    "delete_only": delete_only,
                    "incoming_rows": incoming_rows,
                    "incoming_columns": incoming_columns,
                    "inserted": inserted,
                    "deleted": deleted,
                    "total_rows": total_rows,
                    "total_columns": total_columns,
                    "new_resources": len(new_resources),
                    "sunset_files": len(sunset_files),
                    "skipped_stale": 0,
                    "lineage": _safe_json(lineage or {}),
                    "duration": round(time.time() - t0, 6),
                    "timings": profiler.emit_timings(),
                    "counts": profiler.emit_counts(),
                }
                mark("prepare_monitor")

                total_duration = time.time() - t0
                logger.info(
                    lp(
                        "Timing(s): "
                        f"total={total_duration:.3f} | "
                        f"convert={timings.get('convert', 0):.3f} | dedup_ts={timings.get('dedup_ts', 0):.3f} | validate={timings.get('validate', 0):.3f} | "
                        f"lock={timings.get('lock', 0):.3f} | snapshot={timings.get('snapshot', 0):.3f} | "
                        f"overlap={timings.get('overlap', 0):.3f} | stats_prune={timings.get('stats_prune', 0):.3f} | resolve_overwrite={timings.get('resolve_overwrite', 0):.3f} | newer_than={timings.get('newer_than', 0):.3f} | "
                        f"identify_deletes={timings.get('identify_deletes', 0):.3f} | write_parquet={timings.get('write_parquet', 0):.3f} | "
                        f"build_tombstone={timings.get('build_tombstone', 0):.3f} | compact_tombstones={timings.get('compact_tombstones', 0):.3f} | compact_small={timings.get('compact_small', 0):.3f} | build_stats={timings.get('build_stats', 0):.3f} | "
                        f"update_simple={timings.get('update_simple', 0):.3f} | bump_root={timings.get('bump_root', 0):.3f} | "
                        f"mirror={timings.get('mirror', 0):.3f} | prepare_monitor={timings.get('prepare_monitor', 0):.3f}"
                    )
                )

                result_tuple = (total_columns, total_rows, inserted, deleted)

        except Exception as e:
            logger.error(lp(f"write() failed: {e!s}"))
            raise
        finally:
            # Release per-simple lock first
            if token:
                try:
                    ok = self.catalog.release_simple_lock(
                        self.super_table.organization, self.super_table.super_name, simple_name, token
                    )
                    if not ok:
                        logger.debug(lp("Lock release skipped (token mismatch or already expired)."))
                except Exception:
                    pass

        # ---------- LOCK IS RELEASED HERE ----------
        # Monitoring enqueue + flush is fully outside any data locks.
        # log_metric() fsyncs the bounded WAL before returning; __exit__ then
        # attempts an opportunistic Redis delivery without weakening that
        # durable local acknowledgement.
        #
        # Loop guard: writes targeted at a monitoring sink table
        # (``__writes__``/``__reads__``/``__mcp__``/``__plans__``)
        # are deliberately not measured — the external orchestrator
        # that drained the partition is *writing back* the metric,
        # and re-emitting it would create a 1:1 amplification cycle.
        try:
            if stats_payload is not None and simple_name not in MONITORING_SINK_TABLES:
                # Monitoring is org-wide as of SDK 2.2.0 — record the
                # touched supertable in the payload's ``supertables``
                # field. A DataWriter only touches one supertable, but
                # the list shape is uniform with cross-sup events.
                stats_payload["supertables"] = [self.super_table.super_name]
                with MonitoringWriter(
                    organization=self.super_table.organization,
                    monitor_type="writes",
                ) as monitor:
                    monitor.log_metric(stats_payload)
        except MonitoringDurabilityError as me:
            monitoring_error = me
            logger.error(lp(f"monitoring durability/backpressure failure: {me}"))
        except Exception as me:
            logger.error(lp(f"monitoring enqueue failed: {me}"))

        # ---------- AUDIT LOG ----------
        try:
            if result_tuple is not None:
                _cols, _rows, _ins, _del = result_tuple
                _audit_emit(
                    category=EventCategory.DATA_MUTATION, action=Actions.DATA_WRITE,
                    organization=self.super_table.organization,
                    super_name=self.super_table.super_name,
                    resource_type="table", resource_id=simple_name,
                    detail=make_detail(
                        table=simple_name, row_count=_rows, inserted=_ins, deleted=_del,
                        duration_ms=round((time.time() - t0) * 1000),
                        role_name=role_name, delete_only=delete_only,
                    ),
                )
        except Exception:
            pass  # Never fail a write due to audit

        if monitoring_error is not None:
            failure = MonitoringPostCommitError(
                organization=self.super_table.organization,
                super_name=self.super_table.super_name,
                table_name=simple_name,
                operation="write",
                core_result=result_tuple,
                cause=monitoring_error,
            )
            if mirror_error is not None:
                failure.mirror_error = mirror_error
            raise failure from monitoring_error
        if mirror_error is not None:
            failure = MirrorPublicationError(
                organization=self.super_table.organization,
                super_name=self.super_table.super_name,
                table_name=simple_name,
                mirrors=enabled_mirrors,
                commit_id=qid,
                snapshot_path=str(mirror_snapshot_path or ""),
                core_result=result_tuple,
                cause=mirror_error,
            )
            raise failure from mirror_error
        return result_tuple

    def compact(
        self,
        role_name: str,
        simple_name: str,
        force_tombstones: bool = True,
        small_only: bool = True,
        compression_level: int = 1,
        lineage: dict | None = None,
    ) -> dict:
        """Explicit, lock-protected compaction for one simple table.

        Does the same things ``write()`` would do for an empty input
        **without rewriting any file that doesn't need to be rewritten**:

          1. Acquire the per-table Redis lock (same TTL as ``write``).
          2. Read the current snapshot via the leaf pointer. The simple
             table must already exist — compaction is **not** a
             bootstrap.
          3. If a deletion-vector exists, drain it and merge eligible small
             files in one fused ``compact_resources`` pass, so survivors are
             never encoded and reread as intermediates. Mirror publication
             retains the established targeted drain before the small-file pass.
             The lazy ``max_tombstone_rows`` threshold gates only ``write``;
             ``compact()`` is explicit maintenance and always drains.
          4. ``compact_resources`` packs files toward the configured compressed
             byte target. When ``small_only=True`` (default),
             only files strictly smaller than ``max_memory_chunk_size``
             qualify — large files are left untouched. ``small_only=False``
             rewrites every resource regardless of size. Source Parquet is
             decoded under a separate hard decoded-byte budget; a single wide
             row is the indivisible lower bound and oversized output groups
             spill before publication.
          5. Commit the new snapshot via ``simple_table.update``,
             leaf-CAS, ``bump_root``, optional mirroring, and the same
             monitoring + audit pipeline as ``write``.

        Compaction does **not** garbage-collect: the small files it
        merges away are left in storage (still referenced by older
        snapshot versions for time-travel) for an external GC to
        reclaim later.

        Concurrency: compaction is serialised against writes through
        the same per-simple lock. A concurrent ``write()`` either
        runs first (compaction sees the updated snapshot) or waits.

        Args:
            role_name: RBAC role performing the compaction. Must have
                write access on the target table (checked via
                ``check_write_access`` — same as ``write``).
            simple_name: target table.
            force_tombstones: retained for API/lineage compatibility.
                ``compact()`` always drains an existing deletion-vector
                regardless of this flag (the vector must be consumed
                before small-file compaction to stay consistent).
            small_only: only consider files smaller than
                the compressed-byte target ``max_memory_chunk_size`` for
                compaction. Set to False to rewrite every file (e.g. for a
                full-table re-encode).
            compression_level: zstd compression level for new parquets.
            lineage: optional provenance dict — same conventions as
                ``write``. If omitted a minimal compaction lineage is
                auto-generated.

        Returns:
            ``dict`` with keys mirroring the ``write`` stats_payload
            plus a few compaction-specific fields. Safe to pass to
            ``json.dumps`` / monitoring sinks.

        Raises:
            TimeoutError: lock could not be acquired within 60 s.
            TableNotFoundError: the simple table does not exist.
                Compaction never bootstraps a missing table.
            PermissionError: RBAC check denied write access.
            MirrorPublicationError: the authoritative core snapshot committed,
                but at least one configured mirror failed. The exception
                identifies the committed snapshot for reconciliation.
        """
        qid = str(uuid.uuid4())
        lp = lambda msg: (
            f"[compact][qid={qid}]"
            f"[super={self.super_table.super_name}]"
            f"[table={simple_name}] {msg}"
        )

        t0 = time.time()
        t_last = t0
        timings: dict = {}
        compaction_profiler = Profiler()

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        token = None
        stats_payload: dict | None = None
        mirror_error: Exception | None = None
        mirror_snapshot_path: str | None = None
        monitoring_error: MonitoringDurabilityError | None = None
        result: dict = {
            "query_id": qid,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "organization": self.super_table.organization,
            "super_name": self.super_table.super_name,
            "role_name": role_name,
            "table_name": simple_name,
            "compression_level": compression_level,
            "force_tombstones": force_tombstones,
            "small_only": small_only,
            "files_before": 0,
            "files_after": 0,
            "files_compacted": 0,
            "tombstone_rows_removed": 0,
            "tombstone_files_consumed": 0,
            "tombstone_files_rewritten": 0,
            "tombstone_files_fully_dead": 0,
            "new_resources": 0,
            "sunset_files": 0,
            "total_rows_written": 0,
            "duration": 0.0,
            "lineage": _safe_json(lineage or {}),
        }

        try:
            logger.debug(lp(
                f"➡️ Starting compact(force_tombstones={force_tombstones}, "
                f"small_only={small_only}, compression={compression_level})"
            ))

            # --- Pre-flight: refuse to bootstrap on a missing table ----------
            # Same invariant as ``DataReader``: compaction must NEVER
            # mint catalog state on a missing target. ``check_write_access``
            # (next) builds ``RoleManager`` which bootstraps RBAC role
            # storage for the supertable if it doesn't exist. We must
            # verify existence FIRST so a compact() call against a ghost
            # name raises ``TableNotFoundError`` (or
            # ``SuperTableNotFoundError``) without side effects.
            org = self.super_table.organization
            sup = self.super_table.super_name
            if not self.catalog.root_exists(org, sup):
                raise SuperTableNotFoundError(org, sup)
            if not self.catalog.leaf_exists(org, sup, simple_name):
                raise TableNotFoundError(org, sup, simple_name)

            # --- Access control ------------------------------------------------
            # Compaction is a mutation — same write-access check as ``write``.
            # Safe to run now that we know the target exists.
            check_write_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=simple_name,
            )
            mark("access")

            # --- Per-simple Redis lock ----------------------------------------
            token = self.catalog.acquire_simple_lock(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
                ttl_s=30,
                timeout_s=60,
            )
            if not token:
                raise TimeoutError(f"Could not acquire lock for simple '{simple_name}'")
            mark("lock")
            mutation_fence = getattr(
                type(self.catalog), "check_table_mutation_allowed", None,
            )
            if callable(mutation_fence):
                self.catalog.check_table_mutation_allowed(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    lock_token=token,
                )

            # --- Read last snapshot — refuse to bootstrap ---------------------
            # Compaction is an in-place operation on existing data, so we
            # pass ``create_if_missing=False``. A missing leaf raises
            # ``TableNotFoundError`` rather than silently creating a fresh
            # empty table.
            simple_table = SimpleTable(
                self.super_table, simple_name, create_if_missing=False,
            )
            last_simple_table, last_simple_table_path = (
                simple_table.get_simple_table_snapshot()
            )
            prior_tombstone_rows = self._declared_tombstone_rows(last_simple_table)
            prior_tombstone_digest = self._declared_tombstone_digest(last_simple_table)
            table_config = self._get_table_config(simple_name)
            footer_md_cache = {}
            files_before = len(last_simple_table.get("resources") or [])
            result["files_before"] = files_before
            mark("snapshot")
            enabled_mirrors = self._get_enabled_mirrors("compaction")

            # --- Phase A: tombstone compaction -------------------------------
            # Physically remove tombstoned rows from the data files named in
            # the deletion-vector, then clear the pointer. This MUST run
            # whenever a deletion-vector exists — Phase B (compact_resources)
            # rewrites data files without consulting the vector, so a file
            # named in ``__file__`` that Phase B sunsets would carry its dead
            # rows into the new compacted file while the vector keeps pointing
            # at the (now gone) original. The composite anti-join would no
            # longer match them, so draining first is a correctness boundary.
            # Draining the vector first guarantees Phase B only ever sees clean
            # survivor files. The lazy ``max_tombstone_rows`` threshold gates
            # the *write* path; compact() is explicit maintenance and always
            # consumes the vector.
            tombstone_path = last_simple_table.get("tombstone")
            previous_tombstone_path = tombstone_path
            # required=True: a DV that exists but cannot be read must abort the
            # compaction, never be treated as empty. A swallowed read here would
            # set should_run_tombstones=False, skipping both Phase A and the
            # pointer-clear below, so Phase B would carry the dead rows into the
            # new file while the vector kept pointing at the sunset __file__ —
            # leaving them permanently unreclaimable. Failing loud leaves the
            # prior snapshot + vector intact for a retry, and matches the
            # write-path carry-forward read (required=True) above. Validation also
            # seals schema, non-null/uniqueness, and the snapshot-declared count.
            tombstone_df = (
                validate_tombstone_frame(
                    _read_parquet_safe(tombstone_path, required=True),
                    expected_rows=prior_tombstone_rows,
                    expected_digest=prior_tombstone_digest,
                    allowed_files=self._snapshot_resource_files(last_simple_table),
                    source=f"deletion-vector {tombstone_path}",
                )
                if tombstone_path else None
            )
            tombstone_rows = (
                tombstone_df.height if tombstone_df is not None else 0
            )

            tomb_new_resources: list = []
            tomb_sunset: set = set()
            tomb_compacted_rows = 0
            residual_dv = polars.DataFrame(
                schema={TOMBSTONE_FILE_COL: polars.Utf8, ROWID_COL: polars.Int64}
            )
            residual_tombstone_files: set[str] = set()
            fused_maintenance = False
            considered = 0
            small_total_rows = 0
            small_new_resources: list = []
            small_sunset: set = set()

            should_run_tombstones = tombstone_rows > 0
            if should_run_tombstones:
                if not enabled_mirrors:
                    (
                        considered,
                        small_total_rows,
                        small_new_resources,
                        small_sunset,
                        residual_dv,
                    ) = compact_resources(
                        snapshot=last_simple_table,
                        tombstone_df=tombstone_df,
                        return_residual=True,
                        data_dir=simple_table.data_dir,
                        compression_level=compression_level,
                        table_config=table_config,
                        small_only=small_only,
                        required_reads=False,
                        profiler=compaction_profiler,
                        footer_md_out=footer_md_cache,
                    )
                    residual_tombstone_files = (
                        self._tombstone_referenced_files(residual_dv)
                        if residual_dv.height else set()
                    )
                    tomb_sunset = (
                        self._tombstone_referenced_files(tombstone_df)
                        - residual_tombstone_files
                    )
                    tomb_compacted_rows = (
                        tombstone_df.height - residual_dv.height
                    )
                    fused_maintenance = True
                else:
                    (
                        tomb_compacted_rows,
                        tomb_new_resources,
                        tomb_sunset,
                        residual_dv,
                    ) = self._unpack_tombstone_compaction(
                        compact_tombstones(
                            snapshot=last_simple_table,
                            tombstone_df=tombstone_df,
                            data_dir=simple_table.data_dir,
                            compression_level=compression_level,
                            table_config=table_config,
                            profiler=compaction_profiler,
                            return_residual=True,
                            footer_md_out=footer_md_cache,
                        )
                    )
                if residual_dv.height:
                    residual_tombstone_files = (
                        self._tombstone_referenced_files(residual_dv)
                    )
                    if tomb_new_resources or tomb_sunset:
                        tombstone_path, residual_dv = persist_tombstone_frame(
                            tombstone_dir=os.path.join(
                                simple_table.simple_dir, "tombstone"
                            ),
                            frame=residual_dv,
                            compression_level=compression_level,
                        )
                else:
                    tombstone_path = None
                if enabled_mirrors and residual_dv.height > 0:
                    raise RuntimeError(
                        "Cannot publish a mirror while deletion-vector entries "
                        "remain physically undrained"
                    )
                logger.info(lp(
                    f"tombstone compaction removed {tomb_compacted_rows} rows "
                    f"from {len(tomb_sunset)} file(s)"
                ))
                # Apply the tombstone-step delta to the snapshot *in memory*
                # so the small-file step works against the post-tombstone
                # resource set.
                current_resources = last_simple_table.get("resources") or []
                survivors = [
                    r for r in current_resources
                    if r.get("file") not in tomb_sunset
                ]
                survivors.extend(tomb_new_resources)
                last_simple_table["resources"] = survivors

            result["tombstone_rows_removed"] = tomb_compacted_rows
            result["tombstone_files_consumed"] = len(tomb_sunset)
            result["tombstone_files_rewritten"] = min(
                len(tomb_sunset),
                int(
                    compaction_profiler.counts.get(
                        "tombstone_files_with_survivors", 0,
                    )
                ),
            )
            result["tombstone_files_fully_dead"] = max(
                0,
                len(tomb_sunset) - result["tombstone_files_rewritten"],
            )
            mark("tombstone_compact")

            # --- Phase B: small-file compaction ------------------------------
            if not fused_maintenance:
                (
                    considered,
                    small_total_rows,
                    small_new_resources,
                    small_sunset,
                ) = compact_resources(
                    snapshot={
                        "resources": [
                            r for r in (last_simple_table.get("resources") or [])
                            if r.get("file") not in residual_tombstone_files
                        ]
                    },
                    data_dir=simple_table.data_dir,
                    compression_level=compression_level,
                    table_config=table_config,
                    small_only=small_only,
                    profiler=compaction_profiler,
                    footer_md_out=footer_md_cache,
                )
            mark("small_file_compact")

            # --- Aggregate the two phases ------------------------------------
            #
            # Critical filter: a file produced by Phase A (tombstone
            # compaction) can be picked up as a small-file candidate
            # by Phase B (compact_resources) and then sunset there.
            # If we leave it in ``all_new_resources`` it would land in the
            # new snapshot **and** in ``all_sunset`` at the same time.
            # Filter the file out of new_resources when it also appears
            # in any sunset set so the snapshot stays consistent.
            all_sunset = set(tomb_sunset) | set(small_sunset)
            all_new_resources = [
                r for r in (list(tomb_new_resources) + list(small_new_resources))
                if r.get("file") not in all_sunset
            ]
            # ``all_new_resources`` is the full set of files written by THIS
            # compaction; it feeds stats extraction, the schema model_df and the
            # result metrics below, all of which need every new file.
            #
            # For ``simple_table.update`` it must NOT be reused verbatim, though:
            # Phase A's outputs were already spliced into
            # ``last_simple_table["resources"]`` (the in-memory baseline that
            # ``update`` starts from) right after Phase A ran.  ``update`` does
            # ``(baseline - sunset) + new_resources`` with no dedup, so any
            # Phase-A output that Phase B did NOT consume (left un-sunset because
            # it exceeded the ``small_only`` threshold, or its read failed) would
            # be counted once from the baseline AND once from new_resources —
            # i.e. the same file listed twice in the new snapshot.  Hand ``update``
            # only Phase B's brand-new files, which are the only resources genuinely
            # absent from that baseline.
            update_new_resources = [
                r for r in small_new_resources if r.get("file") not in all_sunset
            ]
            result["files_compacted"] = considered
            result["new_resources"] = len(all_new_resources)
            result["sunset_files"] = len(all_sunset)
            result["total_rows_written"] = small_total_rows

            # Short-circuit: nothing to do, skip the snapshot rewrite
            # and the leaf-CAS / root-bump / mirror path. BUT do
            # NOT return here — execution must fall through to the
            # ``finally`` (which releases the lock) AND the monitoring
            # + audit emission blocks after the try/finally so the
            # compaction attempt is still observable.
            if not all_new_resources and not all_sunset:
                result["files_after"] = files_before
                result["duration"] = round(time.time() - t0, 6)
                result["timings"] = {k: round(v, 6) for k, v in timings.items()}
                result["counts"] = compaction_profiler.emit_counts()
                stats_payload = dict(result)
                logger.info(lp(
                    f"compact: nothing to do (files_before={files_before})"
                ))
            else:
                # --- Update snapshot on storage -------------------------------
                eff_lineage = lineage if lineage and isinstance(lineage, dict) else {}
                if not eff_lineage:
                    eff_lineage = {
                        "source_type": "compact",
                        "role_name": role_name,
                        "force_tombstones": force_tombstones,
                        "small_only": small_only,
                        "files_compacted": considered,
                        "tombstone_rows_removed": tomb_compacted_rows,
                        "write_id": qid,
                    }

                # Publish either the residual vector or a cleared pointer.  An
                # entry is removed only after its exact source row was physically
                # omitted from a successor resource.
                if should_run_tombstones:
                    last_simple_table["tombstone"] = tombstone_path
                    last_simple_table["tombstone_rows"] = residual_dv.height
                    if tombstone_path:
                        last_simple_table["tombstone_digest"] = tombstone_digest(
                            residual_dv, assume_valid=True
                        )
                    else:
                        last_simple_table["tombstone_digest"] = None

                # Carry forward + extend the external column-statistics parquet
                # for the compacted file set: drop rows of every sunset file and
                # append footer stats for the newly written compacted files.
                stats_dir = os.path.join(simple_table.simple_dir, "stats")
                new_data_files = [
                    r.get("file") for r in all_new_resources
                    if isinstance(r, dict) and r.get("file")
                ]
                new_stats_rows = extract_stats_rows(
                    new_data_files, footer_md_cache=footer_md_cache,
                )
                stats_validation_out: dict[str, Any] = {}
                stats_path, combined_stats_df = build_stats_file(
                    stats_dir=stats_dir,
                    prev_stats_path=last_simple_table.get("stats_file"),
                    new_rows=new_stats_rows,
                    removed_files=all_sunset,
                    compression_level=compression_level,
                    prev_cache_identity=(
                        stats_cache_identity(
                            last_simple_table.get("stats_file"),
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        )
                        if last_simple_table.get("stats_file") else None
                    ),
                    validation_out=stats_validation_out,
                )
                last_simple_table["stats_file"] = stats_path
                last_simple_table["stats_rows"] = (
                    combined_stats_df.height
                    if combined_stats_df is not None
                    else int(last_simple_table.get("stats_rows", 0) or 0)
                )
                if combined_stats_df is not None:
                    cache_stats(
                        stats_path,
                        combined_stats_df,
                        cache_identity=stats_cache_identity(
                            stats_path,
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        ),
                        validation=stats_validation_out.get("validation"),
                    )
                mark("build_stats")

                # Explicit compaction is a physical rewrite, not a logical
                # schema update. Preserve the last-upload schema byte-for-byte;
                # deriving it from only the rewritten subset can resurrect a
                # legacy column or drop one held by an untouched large/residual
                # file. ``None`` is SimpleTable.update's preservation contract.
                model_df = None

                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    update_new_resources,
                    all_sunset,
                    model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                )
                mark("update_simple")

                # --- Fenced atomic snapshot publication -------------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                payload = new_snapshot_dict
                self._publish_snapshot(
                    simple_table=simple_table,
                    simple_name=simple_name,
                    payload=payload,
                    path=new_snapshot_path,
                    base_path=last_simple_table_path,
                    lock_token=token,
                    commit_id=qid,
                    now_ms=now_ms,
                    mirrors=enabled_mirrors,
                )
                if (
                    previous_tombstone_path
                    and previous_tombstone_path != tombstone_path
                ):
                    evict_tombstone(
                        previous_tombstone_path,
                        cache_identity=tombstone_cache_identity(
                            previous_tombstone_path,
                            organization=self.super_table.organization,
                            storage=self.super_table.storage,
                        ),
                    )
                mark("bump_root")

                # --- Mirroring ---------------------------------------------------
                mirror_snapshot_path = new_snapshot_path
                if enabled_mirrors:
                    try:
                        MirrorFormats.mirror_if_enabled(
                            super_table=self.super_table,
                            table_name=simple_name,
                            simple_snapshot=new_snapshot_dict,
                            mirrors=enabled_mirrors,
                            commit_id=qid,
                            snapshot_path=new_snapshot_path,
                        )
                    except Exception as e:
                        mirror_error = e
                        failed_format = getattr(e, "failed_format", None)
                        stage = (
                            f"mirror:{failed_format}" if failed_format else "mirror"
                        )
                        try:
                            self._fail_mirror_publication(
                                simple_name=simple_name,
                                commit_id=qid,
                                lock_token=token,
                                stage=stage,
                                error=e,
                            )
                        except Exception as state_exc:
                            logger.error(lp(
                                "failed to persist mirror failure state: "
                                f"{state_exc}"
                            ))
                        logger.error(lp(f"mirroring failed after core commit: {e}"))
                    else:
                        try:
                            self._complete_mirror_publication(
                                simple_name=simple_name,
                                commit_id=qid,
                                lock_token=token,
                            )
                        except Exception as e:
                            mirror_error = e
                            try:
                                self._fail_mirror_publication(
                                    simple_name=simple_name,
                                    commit_id=qid,
                                    lock_token=token,
                                    stage="outbox_complete",
                                    error=e,
                                )
                            except Exception as state_exc:
                                logger.error(lp(
                                    "failed to persist mirror completion error: "
                                    f"{state_exc}"
                                ))
                            logger.error(lp(
                                "mirror data published but durable completion "
                                f"state failed: {e}"
                            ))
                mark("mirror")

                # --- Final stats payload -----------------------------------------
                new_resources_list = new_snapshot_dict.get("resources") or []
                result["files_after"] = len(new_resources_list)
                result["duration"] = round(time.time() - t0, 6)
                result["timings"] = {k: round(v, 6) for k, v in timings.items()}
                result["counts"] = compaction_profiler.emit_counts()
                stats_payload = dict(result)

                logger.info(lp(
                    f"compact: files {files_before} → {len(new_resources_list)} "
                    f"(compacted={considered}, tomb_rows={tomb_compacted_rows}, "
                    f"duration={result['duration']:.3f}s)"
                ))

        except Exception as e:
            logger.error(lp(f"compact() failed: {e!s}"))
            raise
        finally:
            # Release per-simple lock first
            if token:
                try:
                    ok = self.catalog.release_simple_lock(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        token,
                    )
                    if not ok:
                        logger.debug(lp("Lock release skipped (token mismatch or already expired)."))
                except Exception:
                    pass

        # ---------- LOCK IS RELEASED HERE ----------
        # Monitoring + audit run after the lock is released so a slow
        # Redis ship doesn't hold up concurrent writers. Same loop
        # guard as ``write``: compaction targeting a monitoring sink
        # table skips the metric emission.
        try:
            if stats_payload is not None and simple_name not in MONITORING_SINK_TABLES:
                stats_payload["supertables"] = [self.super_table.super_name]
                with MonitoringWriter(
                    organization=self.super_table.organization,
                    monitor_type="compact",
                ) as monitor:
                    monitor.log_metric(stats_payload)
        except MonitoringDurabilityError as me:
            monitoring_error = me
            logger.error(lp(f"monitoring durability/backpressure failure: {me}"))
        except Exception as me:
            logger.error(lp(f"monitoring enqueue failed: {me}"))

        # ---------- AUDIT LOG ----------
        try:
            _audit_emit(
                category=EventCategory.DATA_MUTATION,
                action=Actions.DATA_WRITE,
                organization=self.super_table.organization,
                super_name=self.super_table.super_name,
                resource_type="table",
                resource_id=simple_name,
                detail=make_detail(
                    table=simple_name,
                    operation="compact",
                    files_before=result["files_before"],
                    files_after=result["files_after"],
                    files_compacted=result["files_compacted"],
                    tombstone_rows_removed=result["tombstone_rows_removed"],
                    duration_ms=round((time.time() - t0) * 1000),
                    role_name=role_name,
                ),
            )
        except Exception:
            pass

        if monitoring_error is not None:
            failure = MonitoringPostCommitError(
                organization=self.super_table.organization,
                super_name=self.super_table.super_name,
                table_name=simple_name,
                operation="compact",
                core_result=result,
                cause=monitoring_error,
            )
            if mirror_error is not None:
                failure.mirror_error = mirror_error
            raise failure from monitoring_error
        if mirror_error is not None:
            failure = MirrorPublicationError(
                organization=self.super_table.organization,
                super_name=self.super_table.super_name,
                table_name=simple_name,
                mirrors=enabled_mirrors,
                commit_id=qid,
                snapshot_path=str(mirror_snapshot_path or ""),
                core_result=result,
                cause=mirror_error,
            )
            raise failure from mirror_error
        return result

    def validation(self, dataframe: DataFrame, simple_name: str, overwrite_columns: list, newer_than: str = None, delete_only: bool = False):
        if len(simple_name) == 0 or len(simple_name) > 128:
            raise ValueError("SimpleTable name can't be empty or longer than 128")
        if simple_name.casefold() == self.super_table.super_name.casefold():
            raise ValueError("SimpleTable name can't match with SuperTable name")
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
        if not re.match(pattern, simple_name):
            raise ValueError(
                f"Invalid table name: '{simple_name}'. "
                "Table names must start with a letter/underscore and contain only alphanumeric/underscore characters."
            )
        if isinstance(overwrite_columns, str):
            raise ValueError("overwrite columns must be list")
        reserved = {ROWID_COL.casefold(), "__timestamp__", TOMBSTONE_FILE_COL.casefold()}
        reserved_collisions = [
            str(column) for column in dataframe.columns
            if (
                str(column).casefold() in reserved
                or str(column).casefold().startswith("__supertable_")
            )
        ]
        if reserved_collisions:
            raise ValueError(
                "Input contains reserved system column(s), including a "
                f"case-insensitive collision: {reserved_collisions!r}"
            )
        if overwrite_columns and not all(col in dataframe.columns for col in overwrite_columns):
            raise ValueError("Some overwrite columns are not present in the dataset")
        # delete_only with no overwrite_columns is the delete-all path (tombstone
        # every existing row); with overwrite_columns it deletes the matches.
        if newer_than is not None:
            if not isinstance(newer_than, str):
                raise ValueError("newer_than must be a column name string")
            if newer_than not in dataframe.columns:
                raise ValueError(f"newer_than column '{newer_than}' is not present in the dataset")
            if not overwrite_columns:
                raise ValueError("newer_than requires overwrite_columns to be set")
