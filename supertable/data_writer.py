# supertable/data_writer.py
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
import re

import polars
from polars import DataFrame

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.gc.queue import collect_old_snapshot_paths, enqueue_deletions
from supertable.monitoring.partitions import MONITORING_SINK_TABLES
from supertable.monitoring_writer import MonitoringWriter  # async monitoring
from supertable.super_table import SuperTable
from supertable import redis_keys as RK
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.processing import (
    process_overlapping_files,
    process_delete_only,
    find_overlapping_files,
    filter_stale_incoming_rows,
    extract_key_tuples,
    reconcile_tombstones,
    compact_resources,
    compact_tombstones,
    _tombstone_threshold,
)
from supertable.rbac.access_control import check_write_access  # noqa: F401
from supertable.redis_catalog import RedisCatalog
from supertable.mirroring.mirror_formats import MirrorFormats
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

    # ---- Table configuration (dedup-on-read) --------------------------------

    def configure_table(
            self,
            role_name: str,
            simple_name: str,
            primary_keys: list,
            dedup_on_read: bool = False,
            max_memory_chunk_size: int | None = None,
            max_overlapping_files: int | None = None,
            tombstone_compact_total: int | None = None,
    ) -> None:
        """Set table-level configuration for dedup-on-read behaviour and per-table limits.

        This is a one-time (or infrequent) operation.  The configuration is
        persisted in Redis so the read side can build the dedup view, and
        cached locally so subsequent ``write()`` calls can inject
        ``__timestamp__`` without an extra Redis round-trip.

        Args:
            role_name: RBAC role performing the configuration.
            simple_name: Name of the SimpleTable to configure.
            primary_keys: Column names that form the logical primary key.
            dedup_on_read: When True, the read side wraps queries with a
                ROW_NUMBER window to return only the latest row per key.
            max_memory_chunk_size: Override MAX_MEMORY_CHUNK_SIZE (bytes) for
                this table.  None leaves the existing value unchanged.
            max_overlapping_files: Override MAX_OVERLAPPING_FILES for this
                table.  None leaves the existing value unchanged.
            tombstone_compact_total: Maximum number of soft-deleted key
                tombstones before compaction is triggered.  Default 1000.
                None leaves the existing value unchanged.
        """
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=simple_name,
        )

        if not isinstance(primary_keys, list) or not primary_keys:
            raise ValueError("primary_keys must be a non-empty list of column names")

        # Only fetch existing Redis config when a limit override is being set,
        # preserving the cache-population guarantee for callers that omit them.
        if max_memory_chunk_size is not None or max_overlapping_files is not None or tombstone_compact_total is not None:
            existing = self.catalog.get_table_config(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            ) or {}
        else:
            existing = self._table_config_cache.get(simple_name) or {}
        config = dict(existing)
        config["primary_keys"] = primary_keys
        config["dedup_on_read"] = bool(dedup_on_read)
        if max_memory_chunk_size is not None:
            if max_memory_chunk_size <= 0:
                raise ValueError("max_memory_chunk_size must be a positive integer")
            config["max_memory_chunk_size"] = int(max_memory_chunk_size)
        if max_overlapping_files is not None:
            if max_overlapping_files <= 0:
                raise ValueError("max_overlapping_files must be a positive integer")
            config["max_overlapping_files"] = int(max_overlapping_files)
        if tombstone_compact_total is not None:
            if tombstone_compact_total <= 0:
                raise ValueError("tombstone_compact_total must be a positive integer")
            config["tombstone_compact_total"] = int(tombstone_compact_total)

        self.catalog.set_table_config(
            self.super_table.organization,
            self.super_table.super_name,
            simple_name,
            config,
        )

        # Update local cache so the next write() sees it immediately
        self._table_config_cache[simple_name] = config

    def _get_table_config(self, simple_name: str) -> dict:
        """Return table config from local cache, falling back to Redis once."""
        if simple_name not in self._table_config_cache:
            config = self.catalog.get_table_config(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )
            self._table_config_cache[simple_name] = config or {}
        return self._table_config_cache[simple_name]

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
                    # else: keep first-seen dtype (compaction-time dtypes
                    # are already resolved by concat_with_union, so they
                    # should be consistent across new files).
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
        """
        qid = str(uuid.uuid4())
        lp = lambda msg: f"[write][qid={qid}][super={self.super_table.super_name}][table={simple_name}] {msg}"

        t0 = time.time()
        t_last = t0
        timings = {}

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        token = None
        result_tuple = None
        stats_payload = None

        try:
            logger.debug(lp(f"➡️ Starting write(overwrite_cols={overwrite_columns}, compression={compression_level}, newer_than={newer_than}, delete_only={delete_only})"))

            # --- Access control ------------------------------------------------
            check_write_access(
                 super_name=self.super_table.super_name,
                 organization=self.super_table.organization,
                 role_name=role_name,
                 table_name=simple_name,
            )
            mark("access")

            # --- Convert input -------------------------------------------------
            dataframe: DataFrame = polars.from_arrow(data)
            incoming_rows = dataframe.height
            incoming_columns = dataframe.width
            mark("convert")

            # --- Dedup-on-read: inject __timestamp__ if needed -----------------
            table_config = self._get_table_config(simple_name)
            if table_config.get("dedup_on_read"):
                if "__timestamp__" not in dataframe.columns:
                    dataframe = dataframe.with_columns(
                        polars.lit(datetime.now(timezone.utc)).alias("__timestamp__")
                    )
            mark("dedup_ts")

            # --- Validate ------------------------------------------------------
            self.validation(dataframe, simple_name, overwrite_columns, newer_than, delete_only)
            mark("validate")

            # --- Per-simple Redis lock ----------------------------------------
            token = self.catalog.acquire_simple_lock(
                self.super_table.organization, self.super_table.super_name, simple_name,
                ttl_s=30, timeout_s=60
            )
            if not token:
                raise TimeoutError(f"Could not acquire lock for simple '{simple_name}'")
            mark("lock")

            # --- Read last snapshot (via leaf pointer) ------------------------
            simple_table = SimpleTable(self.super_table, simple_name)
            last_simple_table, last_simple_table_path = simple_table.get_simple_table_snapshot()
            mark("snapshot")

            # --- Detect overlaps ----------------------------------------------
            overlapping_files = find_overlapping_files(
                last_simple_table, dataframe, overwrite_columns, locking=None,
                table_config=table_config,
            )
            mark("overlap")

            # File cache: populated by newer-than filtering, reused by process step
            # to avoid double-reading overlapping parquet files from storage.
            file_cache = {}

            # --- Newer-than filtering (skip stale/replayed rows) ---------------
            if newer_than and overwrite_columns:
                pre_filter_count = dataframe.height
                dataframe = filter_stale_incoming_rows(
                    incoming_df=dataframe,
                    overlapping_files=overlapping_files,
                    overwrite_columns=overwrite_columns,
                    newer_than_col=newer_than,
                    file_cache=file_cache,
                )
                skipped = pre_filter_count - dataframe.height
                if skipped > 0:
                    logger.info(lp(f"newer_than={newer_than}: skipped {skipped}/{pre_filter_count} stale rows"))
                if dataframe.height == 0:
                    logger.info(lp("newer_than: all incoming rows are stale — skipping write"))
                    mark("newer_than")
                    total_columns = dataframe.width
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
                    }
                    # Don't return here — fall through to finally (lock release)
                    # and the post-finally monitoring block.  Returning inside the
                    # try block would either skip monitoring or run it while the
                    # Redis data lock is still held.
                else:
                    mark("newer_than")

            # --- Tombstone / soft-delete logic --------------------------------
            # Skip if early exit already set result_tuple (all rows were stale).
            if result_tuple is None:
                primary_keys = table_config.get("primary_keys") or []
                use_tombstones = bool(primary_keys) and table_config.get("dedup_on_read")
                existing_tombstones = (last_simple_table.get("tombstones") or {}).get("deleted_keys") or []
                tombstone_pk = (last_simple_table.get("tombstones") or {}).get("primary_keys") or primary_keys

                if delete_only and use_tombstones:
                    # --- Soft-delete path: append keys to tombstone list, no file I/O ---
                    new_delete_keys = extract_key_tuples(dataframe, primary_keys)
                    deleted = len(new_delete_keys)
                    merged_tombstones = list(existing_tombstones) + [list(k) for k in new_delete_keys]
                    # Deduplicate tombstone list (a key may be deleted more than once)
                    seen = set()
                    deduped = []
                    for k in merged_tombstones:
                        kt = tuple(k)
                        if kt not in seen:
                            seen.add(kt)
                            deduped.append(list(k))
                    merged_tombstones = deduped
                    inserted = 0
                    total_rows = 0
                    total_columns = dataframe.width
                    new_resources = []
                    sunset_files = set()

                    # Check if compaction threshold is breached
                    threshold = _tombstone_threshold(table_config)
                    if len(merged_tombstones) >= threshold:
                        logger.info(lp(f"tombstone threshold breached ({len(merged_tombstones)}>={threshold}), compacting"))
                        compacted, compact_resources, compact_sunset = compact_tombstones(
                            snapshot=last_simple_table,
                            primary_keys=primary_keys,
                            data_dir=simple_table.data_dir,
                            compression_level=compression_level,
                            table_config=table_config,
                        )
                        new_resources = compact_resources
                        sunset_files = compact_sunset
                        merged_tombstones = []  # clear after compaction
                        logger.info(lp(f"compaction removed {compacted} rows from {len(compact_sunset)} files"))

                    # Store tombstones on snapshot for update()
                    last_simple_table["tombstones"] = {
                        "primary_keys": primary_keys,
                        "deleted_keys": merged_tombstones,
                        "total_tombstones": len(merged_tombstones),
                    }
                    mark("process")

                else:
                    # --- Non-tombstone path (original) --------------------------------

                    # Reconcile: if incoming data (insert/overwrite/append) has keys
                    # matching existing tombstones, remove those from the tombstone list
                    # so the newly-written rows become visible.
                    if existing_tombstones and primary_keys:
                        incoming_keys = extract_key_tuples(dataframe, primary_keys)
                        reconciled = reconcile_tombstones(existing_tombstones, incoming_keys)
                        last_simple_table["tombstones"] = {
                            "primary_keys": tombstone_pk,
                            "deleted_keys": reconciled,
                            "total_tombstones": len(reconciled),
                        }
                        if len(reconciled) < len(existing_tombstones):
                            logger.debug(lp(
                                f"reconciled {len(existing_tombstones) - len(reconciled)} "
                                f"tombstones against incoming keys"
                            ))

                    if delete_only:
                        # delete_only without dedup — fall back to physical delete
                        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
                            dataframe,
                            overlapping_files,
                            overwrite_columns,
                            simple_table.data_dir,
                            compression_level,
                            file_cache=file_cache,
                        )
                    else:
                        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_overlapping_files(
                            dataframe,
                            overlapping_files,
                            overwrite_columns,
                            simple_table.data_dir,
                            compression_level,
                            file_cache=file_cache,
                            table_config=table_config,
                        )
                    mark("process")

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
                schema_model_df = None if delete_only else dataframe
                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    new_resources, sunset_files, schema_model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                )
                mark("update_simple")

                # --- CAS set leaf pointer + atomic root bump ----------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                # Strip per-file stats before caching in Redis.
                # Stats are only used by the write path, which reads them from the
                # full snapshot on storage via get_simple_table_snapshot() — never
                # from the Redis leaf.  estimate() only needs file + file_size +
                # schema, so storing stats in Redis wastes ~73% of the payload with
                # no benefit (~934 → ~172 bytes per resource entry).
                payload = {
                    **new_snapshot_dict,
                    "resources": [
                        {k: v for k, v in r.items() if k != "stats"}
                        for r in (new_snapshot_dict.get("resources") or [])
                    ],
                }

                try:
                    self.catalog.set_leaf_payload_cas(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        payload,
                        new_snapshot_path,
                        now_ms=now_ms,
                    )
                except Exception:
                    # Backward compatible fallback.
                    self.catalog.set_leaf_path_cas(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        new_snapshot_path,
                        now_ms=now_ms,
                    )

                self.catalog.bump_root(self.super_table.organization, self.super_table.super_name, now_ms=now_ms)
                mark("bump_root")

                # --- Enqueue deferred deletions (post-CAS, post-bump) ----------
                # Strict ordering matters: leaf-CAS + root-bump have committed
                # the new snapshot as the authoritative state. Only NOW is it
                # safe to schedule physical deletion of files that the old
                # snapshot still references — any reader that loaded the
                # previous leaf payload will finish before the cleaner's delay
                # window expires.
                #
                # Both blocks are best-effort: enqueue_deletions and
                # collect_old_snapshot_paths swallow Redis/storage errors and
                # log a warning. A GC failure must never fail the write.
                try:
                    _org = self.super_table.organization
                    _sup = self.super_table.super_name
                    if settings.SUPERTABLE_SUNSET_GC_ENABLED and sunset_files:
                        enqueue_deletions(
                            self.catalog,
                            _org, _sup, simple_name,
                            "parquet",
                            list(sunset_files),
                            write_id=qid,
                        )
                    if settings.SUPERTABLE_SNAPSHOT_RETENTION > 0:
                        old_paths = collect_old_snapshot_paths(
                            new_snapshot_dict,
                            self.super_table.storage,
                            settings.SUPERTABLE_SNAPSHOT_RETENTION,
                        )
                        if old_paths:
                            enqueue_deletions(
                                self.catalog,
                                _org, _sup, simple_name,
                                "snapshot",
                                old_paths,
                                write_id=qid,
                            )
                except Exception as e:
                    logger.warning(lp(f"GC enqueue failed (write still succeeded): {e}"))
                mark("gc_enqueue")

                # --- Store schema + table name in Redis (permanent, not cache) ---
                try:
                    schema_raw = new_snapshot_dict.get("schema", {})
                    if isinstance(schema_raw, dict):
                        schema_json = json.dumps(schema_raw, ensure_ascii=False)
                    elif isinstance(schema_raw, list):
                        merged = {}
                        for item in schema_raw:
                            if isinstance(item, dict):
                                merged.update(item)
                        schema_json = json.dumps(merged, ensure_ascii=False)
                    else:
                        schema_json = "{}"
                    _org, _sup = self.super_table.organization, self.super_table.super_name
                    self.catalog.r.set(RK.schema(_org, _sup, simple_name), schema_json)
                    self.catalog.r.sadd(RK.table_names(_org, _sup), simple_name)
                except Exception as e:
                    logger.debug(f"[data-writer] schema/table_names Redis write failed: {e}")

                # --- Optional mirroring -------------------------------------------
                try:
                    MirrorFormats.mirror_if_enabled(
                        super_table=self.super_table,
                        table_name=simple_name,
                        simple_snapshot=new_snapshot_dict,
                    )
                except Exception as e:
                    logger.error(lp(f"mirroring failed: {e}"))
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
                }
                mark("prepare_monitor")

                total_duration = time.time() - t0
                logger.info(
                    lp(
                        "Timing(s): "
                        f"total={total_duration:.3f} | "
                        f"convert={timings.get('convert', 0):.3f} | dedup_ts={timings.get('dedup_ts', 0):.3f} | validate={timings.get('validate', 0):.3f} | "
                        f"lock={timings.get('lock', 0):.3f} | snapshot={timings.get('snapshot', 0):.3f} | "
                        f"overlap={timings.get('overlap', 0):.3f} | newer_than={timings.get('newer_than', 0):.3f} | "
                        f"process={timings.get('process', 0):.3f} | "
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
        # MonitoringWriter.__exit__ calls request_flush() so the metric is
        # guaranteed to reach Redis before this scope closes.
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
        except Exception as me:
            logger.error(lp(f"monitoring enqueue failed: {me}"))

        # ---------- DATA QUALITY: notify scheduler of new data ----------
        # Sets a debounced "pending" flag in Redis.  The DQ scheduler will
        # pick it up on the next tick, respecting lock + cooldown rules.
        # Safe to call at any frequency — never blocks or fails the write.
        try:
            from supertable.services.quality.scheduler import notify_ingest
            notify_ingest(
                self.catalog.r,
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )
        except Exception:
            pass  # Never fail a write due to quality scheduling

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
          3. If primary keys + tombstones are present:

             * When ``force_tombstones=True`` (default), run
               ``compact_tombstones`` unconditionally — this is the
               whole point of calling ``compact()`` manually.
             * When ``force_tombstones=False``, only run if the
               natural ``tombstone_compact_total`` threshold is
               breached (same gate as ``write``).

             The compaction physically removes tombstoned rows from
             the affected parquet files and clears the tombstone list.
          4. Run ``compact_resources`` to merge small parquet files in
             memory-bounded chunks. When ``small_only=True`` (default),
             only files strictly smaller than ``max_memory_chunk_size``
             qualify — large files are left untouched. ``small_only=False``
             rewrites every resource regardless of size.
          5. Commit the new snapshot via ``simple_table.update``,
             leaf-CAS, ``bump_root``, and the same GC enqueue +
             monitoring + audit pipeline as ``write``.

        Concurrency: compaction is serialised against writes through
        the same per-simple lock. A concurrent ``write()`` either
        runs first (compaction sees the updated snapshot) or waits.

        Args:
            role_name: RBAC role performing the compaction. Must have
                write access on the target table (checked via
                ``check_write_access`` — same as ``write``).
            simple_name: target table.
            force_tombstones: bypass the ``tombstone_compact_total``
                threshold and compact whatever tombstones exist now.
                Set to False to honor the natural threshold.
            small_only: only consider files smaller than
                ``max_memory_chunk_size`` for compaction. Set to False
                to rewrite every file (e.g. for a full-table re-encode).
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

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        token = None
        stats_payload: dict | None = None
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
            "tombstone_files_rewritten": 0,
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
            table_config = self._get_table_config(simple_name)
            files_before = len(last_simple_table.get("resources") or [])
            result["files_before"] = files_before
            mark("snapshot")

            # --- Phase A: tombstone compaction -------------------------------
            # Physically remove soft-deleted rows from parquet files. Runs
            # unconditionally when force_tombstones=True; otherwise only
            # when the natural threshold is reached (same gate as the
            # delete-only path in ``write``).
            tomb_block = last_simple_table.get("tombstones") or {}
            tomb_keys = tomb_block.get("deleted_keys") or []
            primary_keys = (
                tomb_block.get("primary_keys")
                or table_config.get("primary_keys")
                or []
            )

            tomb_new_resources: list = []
            tomb_sunset: set = set()
            tomb_compacted_rows = 0

            should_run_tombstones = bool(tomb_keys) and bool(primary_keys) and (
                force_tombstones or len(tomb_keys) >= _tombstone_threshold(table_config)
            )
            if should_run_tombstones:
                tomb_compacted_rows, tomb_new_resources, tomb_sunset = compact_tombstones(
                    snapshot=last_simple_table,
                    primary_keys=primary_keys,
                    data_dir=simple_table.data_dir,
                    compression_level=compression_level,
                    table_config=table_config,
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
            result["tombstone_files_rewritten"] = len(tomb_sunset)
            mark("tombstone_compact")

            # --- Phase B: small-file compaction ------------------------------
            considered, small_total_rows, small_new_resources, small_sunset = (
                compact_resources(
                    snapshot=last_simple_table,
                    data_dir=simple_table.data_dir,
                    compression_level=compression_level,
                    table_config=table_config,
                    small_only=small_only,
                )
            )
            mark("small_file_compact")

            # --- Aggregate the two phases ------------------------------------
            #
            # Critical filter: a file produced by Phase A (tombstone
            # compaction) can be picked up as a small-file candidate
            # by Phase B (compact_resources) and then sunset there.
            # If we leave it in ``all_new_resources`` it lands in the
            # new snapshot **and** in ``all_sunset`` (slated for GC
            # deletion). 30 min later GC deletes the file while the
            # snapshot still references it — readers fail with
            # FileNotFoundError. Filter the file out of new_resources
            # when it also appears in any sunset set.
            all_sunset = set(tomb_sunset) | set(small_sunset)
            all_new_resources = [
                r for r in (list(tomb_new_resources) + list(small_new_resources))
                if r.get("file") not in all_sunset
            ]
            result["files_compacted"] = considered
            result["new_resources"] = len(all_new_resources)
            result["sunset_files"] = len(all_sunset)
            result["total_rows_written"] = small_total_rows

            # Short-circuit: nothing to do, skip the snapshot rewrite
            # and the leaf-CAS / root-bump / GC / mirror path. BUT do
            # NOT return here — execution must fall through to the
            # ``finally`` (which releases the lock) AND the monitoring
            # + audit emission blocks after the try/finally so the
            # compaction attempt is still observable.
            if not all_new_resources and not all_sunset:
                result["files_after"] = files_before
                result["duration"] = round(time.time() - t0, 6)
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

                # Clear the tombstone list now that they've been physically
                # applied (if applicable). ``simple_table.update`` is going
                # to write the updated snapshot.
                if should_run_tombstones:
                    last_simple_table["tombstones"] = {
                        "primary_keys": primary_keys,
                        "deleted_keys": [],
                        "total_tombstones": 0,
                    }

                # Derive the post-compaction schema for ``simple_table.update``.
                # update() builds the new snapshot's ``schema`` field from
                # the model_df's Polars dtypes, so we MUST hand it a frame
                # whose dtypes match the actual compacted data — otherwise
                # the snapshot's schema metadata gets corrupted (e.g. every
                # column reported as Utf8) and downstream consumers
                # (MetaReader, mirroring, query estimation) see a broken
                # schema even though the Parquet files on disk are intact.
                #
                # Strategy: read the schema (no data — n_rows=0) of the
                # first freshly-written compacted file. By construction
                # every compacted file in this call has the same union
                # schema (``concat_with_union`` enforces this in
                # ``compact_resources``), so any of them is representative.
                #
                # Fallback chain on read failure:
                #   1. Reconstruct from the previous snapshot's ``schema``
                #      field — compaction preserves logical schema, so the
                #      pre-compaction schema is the right answer.
                #   2. Empty frame — last resort, only on a pre-compaction
                #      schema that's already empty / missing.
                model_df = self._build_compact_model_df(
                    all_new_resources, last_simple_table,
                )

                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    all_new_resources,
                    all_sunset,
                    model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                )
                mark("update_simple")

                # --- CAS set leaf pointer + atomic root bump ---------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                payload = {
                    **new_snapshot_dict,
                    "resources": [
                        {k: v for k, v in r.items() if k != "stats"}
                        for r in (new_snapshot_dict.get("resources") or [])
                    ],
                }
                try:
                    self.catalog.set_leaf_payload_cas(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        payload,
                        new_snapshot_path,
                        now_ms=now_ms,
                    )
                except Exception:
                    self.catalog.set_leaf_path_cas(
                        self.super_table.organization,
                        self.super_table.super_name,
                        simple_name,
                        new_snapshot_path,
                        now_ms=now_ms,
                    )
                self.catalog.bump_root(
                    self.super_table.organization,
                    self.super_table.super_name,
                    now_ms=now_ms,
                )
                mark("bump_root")

                # --- GC enqueue (same flags as write) ----------------------------
                try:
                    _org = self.super_table.organization
                    _sup = self.super_table.super_name
                    if settings.SUPERTABLE_SUNSET_GC_ENABLED and all_sunset:
                        enqueue_deletions(
                            self.catalog,
                            _org, _sup, simple_name,
                            "parquet",
                            list(all_sunset),
                            write_id=qid,
                        )
                    if settings.SUPERTABLE_SNAPSHOT_RETENTION > 0:
                        old_paths = collect_old_snapshot_paths(
                            new_snapshot_dict,
                            self.super_table.storage,
                            settings.SUPERTABLE_SNAPSHOT_RETENTION,
                        )
                        if old_paths:
                            enqueue_deletions(
                                self.catalog,
                                _org, _sup, simple_name,
                                "snapshot",
                                old_paths,
                                write_id=qid,
                            )
                except Exception as e:
                    logger.warning(lp(f"GC enqueue failed (compact still succeeded): {e}"))
                mark("gc_enqueue")

                # --- Mirroring ---------------------------------------------------
                try:
                    MirrorFormats.mirror_if_enabled(
                        super_table=self.super_table,
                        table_name=simple_name,
                        simple_snapshot=new_snapshot_dict,
                    )
                except Exception as e:
                    logger.error(lp(f"mirroring failed: {e}"))
                mark("mirror")

                # --- Final stats payload -----------------------------------------
                new_resources_list = new_snapshot_dict.get("resources") or []
                result["files_after"] = len(new_resources_list)
                result["duration"] = round(time.time() - t0, 6)
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

        return result

    def validation(self, dataframe: DataFrame, simple_name: str, overwrite_columns: list, newer_than: str = None, delete_only: bool = False):
        if len(simple_name) == 0 or len(simple_name) > 128:
            raise ValueError("SimpleTable name can't be empty or longer than 128")
        if simple_name == self.super_table.super_name:
            raise ValueError("SimpleTable name can't match with SuperTable name")
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
        if not re.match(pattern, simple_name):
            raise ValueError(
                f"Invalid table name: '{simple_name}'. "
                "Table names must start with a letter/underscore and contain only alphanumeric/underscore characters."
            )
        if isinstance(overwrite_columns, str):
            raise ValueError("overwrite columns must be list")
        if overwrite_columns and not all(col in dataframe.columns for col in overwrite_columns):
            raise ValueError("Some overwrite columns are not present in the dataset")
        if delete_only and not overwrite_columns:
            raise ValueError("delete_only requires overwrite_columns to identify rows to delete")
        if newer_than is not None:
            if not isinstance(newer_than, str):
                raise ValueError("newer_than must be a column name string")
            if newer_than not in dataframe.columns:
                raise ValueError(f"newer_than column '{newer_than}' is not present in the dataset")
            if not overwrite_columns:
                raise ValueError("newer_than requires overwrite_columns to be set")