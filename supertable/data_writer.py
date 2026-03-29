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
from supertable.monitoring_writer import MonitoringWriter  # async monitoring
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.processing import (
    process_overlapping_files,
    process_delete_only,
    find_overlapping_files,
    filter_stale_incoming_rows,
    extract_key_tuples,
    reconcile_tombstones,
    compact_tombstones,
    _tombstone_threshold,
)
from supertable.rbac.access_control import check_write_access  # noqa: F401
from supertable.redis_catalog import RedisCatalog
from supertable.mirroring.mirror_formats import MirrorFormats


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
                new_snapshot_dict, new_snapshot_path = simple_table.update(
                    new_resources, sunset_files, dataframe,
                    last_snapshot=last_simple_table,
                    last_snapshot_path=last_simple_table_path,
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
        try:
            if stats_payload is not None:
                with MonitoringWriter(
                    super_name=self.super_table.super_name,
                    organization=self.super_table.organization,
                    monitor_type="stats",
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

        return result_tuple

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