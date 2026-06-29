# supertable/data_writer.py
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
import re

import polars
from polars import DataFrame

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.monitoring.partitions import MONITORING_SINK_TABLES
from supertable.monitoring_writer import MonitoringWriter  # async monitoring
from supertable.super_table import SuperTable
from supertable import redis_keys as RK
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.utils.profiler import Profiler
from supertable.processing import (
    find_overlapping_files,
    resolve_overwrite_writes,
    identify_all_rowids,
    build_tombstone_file,
    reclaim_fully_dead_files,
    build_stats_file,
    extract_stats_rows,
    probe_ranges_from_df,
    prune_overlapping_files_by_stats,
    load_stats,
    cache_stats,
    write_parquet_and_collect_resources,
    compact_resources,
    compact_tombstones,
    should_compact_small_files,
    _max_tombstone_rows,
    _read_parquet_safe,
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

    # ---- Table configuration (per-table limits) -----------------------------

    def configure_table(
            self,
            role_name: str,
            simple_name: str,
            max_memory_chunk_size: int | None = None,
            max_overlapping_files: int | None = None,
            max_tombstone_rows: int | None = None,
    ) -> None:
        """Set per-table limit overrides.

        This is a one-time (or infrequent) operation.  The configuration is
        persisted in Redis and cached locally so subsequent ``write()`` /
        ``compact()`` calls pick up the overrides without an extra Redis
        round-trip.

        Args:
            role_name: RBAC role performing the configuration.
            simple_name: Name of the SimpleTable to configure.
            max_memory_chunk_size: Override MAX_MEMORY_CHUNK_SIZE (bytes) for
                this table.  None leaves the existing value unchanged.
            max_overlapping_files: Override MAX_OVERLAPPING_FILES for this
                table.  None leaves the existing value unchanged.
            max_tombstone_rows: Maximum number of rows in the deletion-vector
                before physical compaction is triggered.  Overrides
                MAX_TOMBSTONE_ROWS (default 1,000,000) for this table.
                None leaves the existing value unchanged.
        """
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=simple_name,
        )

        # Only fetch existing Redis config when a limit override is being set,
        # preserving the cache-population guarantee for callers that omit them.
        if max_memory_chunk_size is not None or max_overlapping_files is not None or max_tombstone_rows is not None:
            existing = self.catalog.get_table_config(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            ) or {}
        else:
            existing = self._table_config_cache.get(simple_name) or {}
        config = dict(existing)
        if max_memory_chunk_size is not None:
            if max_memory_chunk_size <= 0:
                raise ValueError("max_memory_chunk_size must be a positive integer")
            config["max_memory_chunk_size"] = int(max_memory_chunk_size)
        if max_overlapping_files is not None:
            if max_overlapping_files <= 0:
                raise ValueError("max_overlapping_files must be a positive integer")
            config["max_overlapping_files"] = int(max_overlapping_files)
        if max_tombstone_rows is not None:
            if max_tombstone_rows <= 0:
                raise ValueError("max_tombstone_rows must be a positive integer")
            config["max_tombstone_rows"] = int(max_tombstone_rows)

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

            # --- Validate ------------------------------------------------------
            # Runs before __rowid__ reservation so invalid input never burns a
            # range of the per-table Redis id counter.
            self.validation(dataframe, simple_name, overwrite_columns, newer_than, delete_only)
            mark("validate")

            # --- Assign table-unique __rowid__ --------------------------------
            # Reserve a contiguous block of ids from the per-table Redis
            # counter (INCRBY) so every inserted row gets a value unique
            # within the table, even across concurrent writers. Skipped for
            # delete_only writes, whose dataframe carries only delete-predicate
            # columns (no rows are inserted). with_columns overwrites any
            # caller-supplied __rowid__ so uniqueness is always enforced.
            if not delete_only and incoming_rows > 0:
                start_rowid = self.catalog.reserve_rowids(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple_name,
                    incoming_rows,
                )
                dataframe = dataframe.with_columns(
                    polars.int_range(
                        start_rowid, start_rowid + incoming_rows, dtype=polars.Int64
                    ).alias("__rowid__")
                )
            mark("rowid")

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
            table_config = self._get_table_config(simple_name)
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

            # --- Read last snapshot (via leaf pointer) ------------------------
            simple_table = SimpleTable(self.super_table, simple_name)
            last_simple_table, last_simple_table_path = simple_table.get_simple_table_snapshot()
            mark("snapshot")

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
                    stored_stats_df = load_stats(stats_file, allow_cache=True, profiler=profiler)
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

            # File cache: used only by delete_only's identify_all_rowids below.
            file_cache = {}

            # --- Overwrite resolution: stale-row filtering + delete-pair -------
            # identification in one DuckDB-pushdown probe over the overlapping
            # files (column projection, row-group skipping, ranged GETs, native
            # null-safe SEMI JOIN) instead of full-file polars reads.  Returns
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
                )
                mark("resolve_overwrite")
                _counts = profiler.counts
                _fallback = bool(_counts.get("overwrite_resolve_fallback"))
                logger.debug(lp(
                    f"step[probe-resolve] via {'polars-fallback' if _fallback else 'duckdb-pushdown'}: "
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
                prev_tombstone_path = last_simple_table.get("tombstone")
                new_resources = []
                sunset_files = set()

                # Load the current deletion-vector once: used both to exclude
                # already-tombstoned rows from this write's deletes (below) and,
                # via prev_df, to extend the vector without a second read.
                # required=True: a DV that exists but cannot be read must abort
                # the write, never be treated as empty — silently dropping the
                # carried-forward vector would resurrect previously deleted rows.
                prev_dv_df = (
                    _read_parquet_safe(prev_tombstone_path, profiler=profiler, required=True)
                    if prev_tombstone_path else None
                )
                prev_dv_rowids = set()
                if prev_dv_df is not None and "__rowid__" in prev_dv_df.columns:
                    prev_dv_rowids = set(prev_dv_df.get_column("__rowid__").to_list())

                # 1. Identify which existing rows this write deletes/replaces.
                #    overwrite_columns drives the anti-join key (delete + upsert);
                #    pure appends (no overwrite_columns) tombstone nothing.  The
                #    pairs were already derived (from the surviving keys) by the
                #    resolve_overwrite_writes probe above.
                new_delete_pairs = []
                if overwrite_columns:
                    new_delete_pairs = resolved_delete_pairs or []
                elif delete_only:
                    # delete-all: no overwrite_columns → tombstone every row.
                    new_delete_pairs = identify_all_rowids(
                        last_simple_table.get("resources", []),
                        file_cache=file_cache,
                        profiler=profiler,
                    )

                # Never re-tombstone rows already in the deletion-vector.  The
                # overlap probe (and identify_all_rowids) scan the *physical*
                # files, which still hold logically-deleted rows until
                # compaction; without this filter every write re-counts those
                # already-dead rows — inflating ``deleted`` and forcing a
                # needless tombstone rewrite even when nothing live was removed.
                # Excluding them makes ``deleted`` the true count of live rows
                # removed and lets unchanged writes carry the vector forward.
                if new_delete_pairs and prev_dv_rowids:
                    new_delete_pairs = [
                        (f, rid) for (f, rid) in new_delete_pairs
                        if rid not in prev_dv_rowids
                    ]
                deleted = len(new_delete_pairs)
                mark("identify_deletes")
                logger.debug(lp(
                    f"step[deletes]: tombstoning {deleted} live row(s) this write "
                    f"(excluded {len(prev_dv_rowids)} row(s) already in the deletion-vector)"
                ))

                # 2. Write the incoming rows as a new file (insert/upsert side).
                #    delete_only carries only predicate columns — nothing to insert.
                if not delete_only and dataframe.height > 0:
                    write_parquet_and_collect_resources(
                        write_df=dataframe,
                        overwrite_columns=[],
                        data_dir=simple_table.data_dir,
                        new_resources=new_resources,
                        compression_level=compression_level,
                        profiler=profiler,
                    )
                    inserted = dataframe.height
                else:
                    inserted = 0
                mark("write_parquet")
                logger.debug(lp(
                    f"step[write]: appended {inserted} incoming row(s) as {len(new_resources)} "
                    f"new immutable file(s) (no existing data file rewritten)"
                ))

                # 3. Carry forward + extend the deletion-vector tombstone file.
                #    No new deletes → reuse the previous file (combined_df=None).
                tombstone_dir = os.path.join(simple_table.simple_dir, "tombstone")
                tombstone_path, combined_tombstone_df = build_tombstone_file(
                    tombstone_dir=tombstone_dir,
                    prev_tombstone_path=prev_tombstone_path,
                    new_pairs=new_delete_pairs,
                    compression_level=compression_level,
                    profiler=profiler,
                    prev_df=prev_dv_df,
                )

                # Track the live deletion-vector row count so meta reads can
                # deduct dead rows from the physical resource row totals.
                # New deletes → combined_tombstone_df is the full deduped DV
                # (exact count); pure carry-forward → reuse the previous count.
                tombstone_rows = (
                    combined_tombstone_df.height
                    if combined_tombstone_df is not None
                    else int(last_simple_table.get("tombstone_rows", 0) or 0)
                )
                mark("build_tombstone")
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
                if combined_tombstone_df is not None:
                    reclaimed_files, reclaimed_tomb_path, reclaimed_dv = (
                        reclaim_fully_dead_files(
                            resources=last_simple_table.get("resources") or [],
                            combined_dv=combined_tombstone_df,
                            tombstone_dir=tombstone_dir,
                            compression_level=compression_level,
                            profiler=profiler,
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
                    combined_tombstone_df is not None
                    and combined_tombstone_df.height >= _max_tombstone_rows(table_config)
                )

                # Phase A — drain the deletion-vector when either trigger fires
                # and a vector is actually live (freshly built this write OR
                # carried forward from a prior one).
                if tombstone_threshold_hit or compaction_gate:
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
                            else _read_parquet_safe(tombstone_path, profiler=profiler)
                        )
                    if dv_to_drain is not None and dv_to_drain.height > 0:
                        removed, tomb_new, tomb_sunset = compact_tombstones(
                            snapshot=last_simple_table,
                            tombstone_df=dv_to_drain,
                            data_dir=simple_table.data_dir,
                            compression_level=compression_level,
                            table_config=table_config,
                            profiler=profiler,
                        )
                        new_resources.extend(tomb_new)
                        sunset_files |= tomb_sunset
                        tombstone_path = None  # deletion-vector fully consumed
                        tombstone_rows = 0
                        logger.info(lp(
                            f"tombstone compaction removed {removed} rows "
                            f"from {len(tomb_sunset)} files"
                        ))

                # 5. Pin the (carried-forward / new / cleared) tombstone pointer
                #    and its row count.
                last_simple_table["tombstone"] = tombstone_path
                last_simple_table["tombstone_rows"] = tombstone_rows
                mark("compact_tombstones")

                # Phase B — auto small-file compaction.  Merge the accumulated
                # small files (existing survivors + the file just written) once
                # the gate is open so the file count stays bounded.  The vector
                # was drained above, so every surviving file is safe to sunset.
                # Result folds into the SAME snapshot commit below (new_resources
                # / sunset_files feed build_stats and simple_table.update).
                compaction_ran = False
                if compaction_gate:
                    live_resources = [
                        r for r in (last_simple_table.get("resources") or [])
                        if r.get("file") not in sunset_files
                    ]
                    live_resources += [
                        r for r in new_resources if r.get("file") not in sunset_files
                    ]
                    considered, comp_rows, comp_new, comp_sunset = compact_resources(
                        snapshot={"resources": live_resources},
                        data_dir=simple_table.data_dir,
                        compression_level=compression_level,
                        table_config=table_config,
                        small_only=True,
                    )
                    if comp_new or comp_sunset:
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

                # 6. Carry forward + extend the external column-statistics parquet.
                #    Read the footers of the newly written data files, drop the
                #    rows of any sunset file, and append the new ones. No new
                #    files and nothing sunset → reuse the previous stats file.
                stats_dir = os.path.join(simple_table.simple_dir, "stats")
                new_data_files = [
                    r.get("file") for r in new_resources
                    if isinstance(r, dict) and r.get("file")
                ]
                new_stats_rows = extract_stats_rows(new_data_files, profiler=profiler)
                stats_path, combined_stats_df = build_stats_file(
                    stats_dir=stats_dir,
                    prev_stats_path=last_simple_table.get("stats_file"),
                    new_rows=new_stats_rows,
                    removed_files=sunset_files,
                    compression_level=compression_level,
                    profiler=profiler,
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
                    cache_stats(stats_path, combined_stats_df)
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
                # When auto-compaction merged files this write, derive the
                # schema from the compacted output instead: a merged file may
                # union in columns from older files that the incoming frame
                # lacks (schema-evolving tables), so `dataframe` would narrow
                # the metadata even though the Parquet is wider.
                if compaction_ran:
                    schema_model_df = self._build_compact_model_df(
                        new_resources, last_simple_table
                    )
                else:
                    schema_model_df = None if delete_only else dataframe
                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    new_resources, sunset_files, schema_model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                    profiler=profiler,
                )
                mark("update_simple")

                # --- CAS set leaf pointer + atomic root bump ----------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                payload = new_snapshot_dict

                with profiler.span("redis.set_leaf"):
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

                with profiler.span("redis.bump_root"):
                    self.catalog.bump_root(self.super_table.organization, self.super_table.super_name, now_ms=now_ms)
                mark("bump_root")

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
                    self.catalog.r.sadd(RK.meta_table_names(_org, _sup), simple_name)
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
          3. If a deletion-vector exists, run ``compact_tombstones``
             unconditionally — physically removing tombstoned rows from
             the affected parquet files and clearing the vector. This
             must happen before step 4 so the small-file step never
             rewrites a file the vector still references (which would
             resurrect dead rows onto disk and orphan the vector). The
             lazy ``max_tombstone_rows`` threshold gates only ``write``;
             ``compact()`` is explicit maintenance and always drains.
          4. Run ``compact_resources`` to merge small parquet files in
             memory-bounded chunks. When ``small_only=True`` (default),
             only files strictly smaller than ``max_memory_chunk_size``
             qualify — large files are left untouched. ``small_only=False``
             rewrites every resource regardless of size.
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
            # Physically remove tombstoned rows from the data files named in
            # the deletion-vector, then clear the pointer. This MUST run
            # whenever a deletion-vector exists — Phase B (compact_resources)
            # rewrites data files without consulting the vector, so a file
            # named in ``__file__`` that Phase B sunsets would carry its dead
            # rows into the new compacted file while the vector keeps pointing
            # at the (now gone) original. The rows would stay hidden on read
            # (anti-join is rowid-only) but become permanently unreclaimable.
            # Draining the vector first guarantees Phase B only ever sees clean
            # survivor files. The lazy ``max_tombstone_rows`` threshold gates
            # the *write* path; compact() is explicit maintenance and always
            # consumes the vector.
            tombstone_path = last_simple_table.get("tombstone")
            # required=True: a DV that exists but cannot be read must abort the
            # compaction, never be treated as empty. A swallowed read here would
            # set should_run_tombstones=False, skipping both Phase A and the
            # pointer-clear below, so Phase B would carry the dead rows into the
            # new file while the vector kept pointing at the sunset __file__ —
            # leaving them permanently unreclaimable. Failing loud leaves the
            # prior snapshot + vector intact for a retry, and matches the
            # write-path carry-forward read (required=True) above.
            tombstone_df = (
                _read_parquet_safe(tombstone_path, required=True)
                if tombstone_path else None
            )
            tombstone_rows = (
                tombstone_df.height if tombstone_df is not None else 0
            )

            tomb_new_resources: list = []
            tomb_sunset: set = set()
            tomb_compacted_rows = 0

            should_run_tombstones = tombstone_rows > 0
            if should_run_tombstones:
                tomb_compacted_rows, tomb_new_resources, tomb_sunset = compact_tombstones(
                    snapshot=last_simple_table,
                    tombstone_df=tombstone_df,
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

                # Clear the deletion-vector pointer now that the dead rows
                # have been physically dropped. ``simple_table.update`` writes
                # the updated snapshot (with tombstone=None) below.
                if should_run_tombstones:
                    last_simple_table["tombstone"] = None
                    last_simple_table["tombstone_rows"] = 0

                # Carry forward + extend the external column-statistics parquet
                # for the compacted file set: drop rows of every sunset file and
                # append footer stats for the newly written compacted files.
                stats_dir = os.path.join(simple_table.simple_dir, "stats")
                new_data_files = [
                    r.get("file") for r in all_new_resources
                    if isinstance(r, dict) and r.get("file")
                ]
                new_stats_rows = extract_stats_rows(new_data_files)
                stats_path, combined_stats_df = build_stats_file(
                    stats_dir=stats_dir,
                    prev_stats_path=last_simple_table.get("stats_file"),
                    new_rows=new_stats_rows,
                    removed_files=all_sunset,
                    compression_level=compression_level,
                )
                last_simple_table["stats_file"] = stats_path
                last_simple_table["stats_rows"] = (
                    combined_stats_df.height
                    if combined_stats_df is not None
                    else int(last_simple_table.get("stats_rows", 0) or 0)
                )
                if combined_stats_df is not None:
                    cache_stats(stats_path, combined_stats_df)
                mark("build_stats")

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
                    update_new_resources,
                    all_sunset,
                    model_df,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
                    lineage=eff_lineage,
                )
                mark("update_simple")

                # --- CAS set leaf pointer + atomic root bump ---------------------
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                payload = new_snapshot_dict
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
                result["timings"] = {k: round(v, 6) for k, v in timings.items()}
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
        # delete_only with no overwrite_columns is the delete-all path (tombstone
        # every existing row); with overwrite_columns it deletes the matches.
        if newer_than is not None:
            if not isinstance(newer_than, str):
                raise ValueError("newer_than must be a column name string")
            if newer_than not in dataframe.columns:
                raise ValueError(f"newer_than column '{newer_than}' is not present in the dataset")
            if not overwrite_columns:
                raise ValueError("newer_than requires overwrite_columns to be set")