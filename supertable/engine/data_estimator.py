# supertable/engine/data_estimator.py

from __future__ import annotations

import os
from collections import defaultdict
from typing import Iterable, Set, List, Dict, Optional, Tuple
from urllib.parse import urlparse

import polars

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.super_table import SuperTable
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.utils.profiler import Profiler
from supertable.redis_catalog import RedisCatalog  # Redis leaf pointers for snapshots

from supertable.utils.sql_parser import TableDefinition
from supertable.processing import (
    load_stats,
    prune_files_by_predicates,
    ROWID_COL,
    TIMESTAMP_COL,
)


from typing import Dict, List, Optional, Set, Tuple


def get_missing_columns(
        tables: List[TableDefinition],
        selected: List[SuperSnapshot],
) -> List[Tuple[str, str, Set[str]]]:
    """
    Returns list of (super_name, simple_name, missing_columns),
    but only for tables where at least one requested column is missing.

    Semantics (updated):

      - `tables` (TableDefinition):
          * Represents what the query requests (from SQLParser).
          * Match key: (super_name, simple_name), case-insensitive.
          * columns == []  => SELECT * / t.*  => all columns requested,
                                but we DO NOT validate -> skip missing-check.
          * columns != [] => explicit requested columns that MUST exist.

      - `selected` (SuperSnapshot):
          * Represents what is actually available for that table/version.
          * columns: Set[str] of available columns.
          * Multiple snapshots for same table:
                union their columns.

      - Missing logic:
          * If TableDefinition.columns == []:
                - No validation (treated as "don't check SELECT *").
          * Else:
                - If there is no matching SuperSnapshot:
                      all requested columns are missing.
                - If there is a match:
                      missing = requested - available (case-insensitive).
          * Only tables with non-empty missing set are returned.
    """

    # Build availability index from selected snapshots:
    #   (super_name.lower(), simple_name.lower()) -> set(lowercase columns)
    available_index: Dict[Tuple[str, str], Set[str]] = {}

    for s in selected:
        key = (s.super_name.lower(), s.simple_name.lower())
        if key not in available_index:
            available_index[key] = set()
        # s.columns is a Set[str]; guard if it's empty/None
        for c in (s.columns or []):
            available_index[key].add(c.lower())

    results: List[Tuple[str, str, Set[str]]] = []

    # Check each requested table definition
    for t in tables:
        key = (t.super_name.lower(), t.simple_name.lower())

        # [] means SELECT * (or t.*) -> all columns requested,
        # but as per requirement: do NOT validate in this function.
        if not t.columns:
            continue

        requested_lower = {c.lower() for c in t.columns}
        available_lower = available_index.get(key)

        if available_lower is None:
            # No snapshot for this table -> everything requested is missing
            missing_lower = requested_lower
        else:
            # Only columns that are requested but not present
            missing_lower = requested_lower - available_lower

        if missing_lower:
            # Preserve original casing for reporting
            missing_original = {c for c in t.columns if c.lower() in missing_lower}
            if missing_original:
                results.append((t.super_name, t.simple_name, missing_original))

    return results


class DataEstimator:
    """
    Estimates which files will be read for a query and validates read access.
    Returns:
      {
        "STORAGE_TYPE": "<storage backend class name or identifier>",
        "BYTES_AFFECTED": <int>",
        "FILE_LIST": [<resolved_url_or_path>, ...]
      }
    """

    def __init__(
        self,
        organization: str,
        storage,
        tables: List[TableDefinition],
        predicate_constraints: Optional[Dict] = None,
        plan_stats: Optional[PlanStats] = None,
    ):
        self.organization = organization
        self.storage = storage
        self.tables = tables
        # (super_lower, simple_lower) -> List[Dict[col, PredInterval]] from the
        # query's WHERE clauses; used to prune files by the stats artifact.
        # None / empty ⇒ no read-path pruning.
        self.predicate_constraints = predicate_constraints or {}
        self.timer: Optional[Timer] = None
        # When the caller (DataReader) injects its PlanStats, estimator stats —
        # REFLECTIONS, REFLECTION_SIZE and the read-pruning counters — land on
        # the same object that flows to extend_execution_plan, so they reach the
        # read monitoring payload. Standalone callers get a fresh PlanStats.
        self.plan_stats: Optional[PlanStats] = plan_stats
        self.catalog = RedisCatalog()

    def _schema_to_dict(self, schema_obj) -> Dict[str, str]:
        """Normalize schema representations into a {name: type} dict."""
        if isinstance(schema_obj, dict):
            return schema_obj
        if isinstance(schema_obj, list):
            out: Dict[str, str] = {}
            for item in schema_obj:
                if isinstance(item, dict):
                    name = item.get("name")
                    if name is not None:
                        out[str(name)] = str(item.get("type", ""))
            return out
        return {}

    # ----------------------- storage helpers (matching original) -----------------------

    def _get_env(self, *names: str) -> Optional[str]:
        """Deprecated: retained for backward compatibility. Prefer os.getenv('STORAGE_*') directly."""
        for n in names:
            v = os.getenv(n)
            if v:
                return v
        return None

    def _storage_attr(self, *names: str) -> Optional[str]:
        for n in names:
            if hasattr(self.storage, n):
                v = getattr(self.storage, n)
                if v not in (None, "", False):
                    return str(v)
        return None

    def _normalize_endpoint_for_s3(self, ep: str) -> str:
        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ("" if ":" in ep else "")
        return f"{host}{port}"

    def _detect_endpoint(self) -> Optional[str]:
        # 1) storage object attributes (e.g. endpoint_url, endpoint)
        candidates = [
            "endpoint_url", "endpoint", "url", "api_url", "base_url",
            "s3_endpoint", "minio_endpoint", "public_endpoint",
        ]
        for name in candidates:
            val = self._storage_attr(name)
            if val:
                logger.debug(f"[estimate.env] storage.{name}='{val}'")
                return self._normalize_endpoint_for_s3(val)

        host = self._storage_attr("host", "hostname")
        port = self._storage_attr("port")
        if host:
            composed = f"{host}{':' + port if port else ''}"
            return self._normalize_endpoint_for_s3(composed)

        # 2) Environment variable
        if settings.STORAGE_ENDPOINT_URL:
            return self._normalize_endpoint_for_s3(env_single)

        return None

    def _detect_bucket(self) -> Optional[str]:
        for name in ("bucket", "bucket_name", "default_bucket"):
            v = self._storage_attr(name)
            if v:
                return v
        return settings.STORAGE_BUCKET or None

    def _detect_ssl(self) -> bool:
        val = (
                (str(getattr(self.storage, "secure", "")).lower() if hasattr(self.storage, "secure") else "")
                or str(settings.STORAGE_USE_SSL)
        ).lower()
        return val in ("1", "true", "yes", "on")

    def _to_duckdb_path(self, key: str) -> str:
        """
        Resolve a storage key to a usable path for DuckDB.
        If SUPERTABLE_DUCKDB_PRESIGNED=1, presign with an **object key** (never pass a URL to presign).
        """
        if not key:
            return key

        # 1) Presign first if requested
        if settings.SUPERTABLE_DUCKDB_PRESIGNED:
            presign_fn = getattr(self.storage, "presign", None)
            if callable(presign_fn):
                try:
                    url = presign_fn(key)  # key, not URL
                    if isinstance(url, str) and url:
                        logger.debug(f"[estimate.resolve] presigned → {url[:96]}...")
                        return url
                except Exception as e:
                    logger.warning(f"[estimate.resolve] presign failed; falling back: {e}")

        # 2) If already URL, return as-is.
        if "://" in key:
            logger.debug(f"[estimate.resolve] already URL: {key}")
            return key

        # 3) storage helpers
        for attr in ("to_duckdb_path", "make_duckdb_url", "make_url"):
            fn = getattr(self.storage, attr, None)
            if callable(fn):
                try:
                    url = fn(key)  # key in, URL out (not presigned)
                    if isinstance(url, str) and url:
                        logger.info(f"[estimate.resolve] storage.{attr} → {url}")
                        return url
                except Exception as e:
                    logger.debug(f"[estimate.resolve] storage.{attr} failed: {e}")

        # 4) Construct URL from endpoint/bucket
        endpoint_raw = self._detect_endpoint()
        bucket = self._detect_bucket()
        use_http = settings.SUPERTABLE_DUCKDB_USE_HTTPFS
        scheme = "https" if self._detect_ssl() else "http"
        key_norm = key.lstrip("/")

        if endpoint_raw and bucket:
            if use_http:
                return f"{scheme}://{endpoint_raw.rstrip('/')}/{bucket}/{key_norm}"
            else:
                return f"s3://{bucket}/{key_norm}"

        # 5) Fallback
        return key

    # ----------------------- snapshot discovery & filtering -----------------------

    def _collect_snapshots_from_redis(self, organization, super_name) -> List[Dict]:
        items = list(self.catalog.scan_leaf_items(organization, super_name, count=512))
        snapshots = []
        for it in items:
            if not it.get("path"):
                continue
            snapshots.append(
                {
                    "table_name": it["simple"],
                    "last_updated_ms": int(it.get("ts", 0)),
                    "path": it["path"],
                    "version": it['version'],
                    "payload": it.get("payload"),
                }
            )
        return snapshots

    def _filter_snapshots(self, super_name, simple_name, snapshots: List[Dict]) -> List[Dict]:
        if super_name.lower() == simple_name.lower():
            return [s for s in snapshots if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))]
        return [s for s in snapshots if s["table_name"].lower() == simple_name.lower()]

    def _get_supertable_map(self) -> List[Tuple[str, List[str]]]:
        grouped = defaultdict(list)

        for t in self.tables:  # t: TableDefinition
            grouped[t.super_name].append(t.simple_name)

        # optional: sort simple_names per supertable
        return [
            (super_name, sorted(simple_names))
            for super_name, simple_names in grouped.items()
        ]

    def _prune_files(
        self,
        super_name: str,
        simple_name: str,
        raw_keys: List[str],
        stats_df: Optional["polars.DataFrame"],
        profiler: Optional[Profiler] = None,
    ) -> List[str]:
        """Narrow *raw_keys* to those that could satisfy the query predicates.

        Takes the already-loaded *stats_df* (loaded once per table by
        :meth:`estimate` and shared with projection sizing) rather than a path,
        so the artifact is read at most once.  Returns *raw_keys* unchanged
        whenever pruning is disabled, there's no stats artifact, or the query
        carries no usable constraint for this table — and never raises (a
        pruning failure must not break a read).

        *profiler*, when supplied, accumulates the same IO/pruning counters the
        write path emits (``read_pruned_files``) so the read monitoring payload
        can surface them.
        """
        if not settings.SUPERTABLE_READ_PRUNING_ENABLED:
            return raw_keys
        if stats_df is None or not raw_keys:
            return raw_keys
        occurrences = self.predicate_constraints.get(
            (super_name.lower(), simple_name.lower())
        )
        if not occurrences:
            return raw_keys
        try:
            return prune_files_by_predicates(
                raw_keys, stats_df, occurrences, profiler=profiler,
            )
        except Exception as e:
            logger.warning(f"[estimate.prune] pruning skipped for {super_name}.{simple_name}: {e}")
            return raw_keys

    # ----------------------- projection-aware sizing -----------------------

    # Rough per-value byte widths for the type-width *fallback* (used only when
    # per-column ``compressed_bytes`` are unavailable — e.g. a stats file that
    # predates the column, or no stats artifact at all).  Substring-matched
    # against the stored (DuckDB/polars) type name, most-specific first.
    _STRING_AVG_WIDTH = 16

    def _type_width(self, type_name: Optional[str]) -> int:
        """Estimated on-disk bytes-per-value for a column type (fallback only)."""
        t = (type_name or "").strip().lower()
        if not t:
            return 8
        if "bool" in t:
            return 1
        if "timestamp" in t or "datetime" in t:
            return 8
        if "date" in t:
            return 4
        if any(s in t for s in ("varchar", "char", "utf8", "string", "text",
                                "json", "blob", "binary", "bytea")):
            return self._STRING_AVG_WIDTH
        if "double" in t or "float64" in t:
            return 8
        if "float" in t or "real" in t:
            return 4
        if "decimal" in t or "numeric" in t:
            return 8
        if "bigint" in t or "int64" in t or "long" in t:
            return 8
        if "smallint" in t or "int16" in t:
            return 2
        if "tinyint" in t or "int8" in t:
            return 1
        if "int" in t:
            return 4
        return 8

    def _selected_columns(self, super_name: str, simple_name: str) -> Optional[Set[str]]:
        """Lowercased set of user columns this query projects from the table.

        Returns ``None`` to mean "every column" — i.e. ``SELECT *`` / ``t.*``
        (an empty ``TableDefinition.columns``), an unrecognised table, or a
        projection that resolves to only system columns.  In every ``None`` case
        the caller uses the whole-file size (no projection savings), which is
        the safe over-estimate.  System columns (``__rowid__`` / ``__timestamp__``)
        are stripped: the read view hides them, so they're never scanned.
        """
        key = (super_name.lower(), simple_name.lower())
        selected: Set[str] = set()
        matched = False
        for t in self.tables:
            if (t.super_name.lower(), t.simple_name.lower()) != key:
                continue
            matched = True
            if not t.columns:          # [] => SELECT * / t.* => whole table
                return None
            for c in t.columns:
                cl = c.lower()
                if cl not in (ROWID_COL, TIMESTAMP_COL):
                    selected.add(cl)
        if not matched or not selected:
            return None
        return selected

    def _projected_bytes_index(
        self,
        stats_df: Optional["polars.DataFrame"],
        selected_cols: Set[str],
    ) -> Tuple[Set[str], Dict[str, int]]:
        """Sum per-column ``compressed_bytes`` for the selected columns per file.

        Returns ``(tier3_files, proj)`` where *proj* maps a file path to the
        summed on-disk bytes of its selected columns, and *tier3_files* is the
        subset of files for which that sum is *trustworthy* — every matched row
        carried a non-NULL ``compressed_bytes``.  A file with any NULL (an older
        carried-forward row) is omitted so the caller falls back to a whole-file
        ratio rather than under-counting it as zero.
        """
        if (
            stats_df is None
            or stats_df.height == 0
            or "compressed_bytes" not in stats_df.columns
        ):
            return set(), {}
        sel = (
            stats_df.select(["file_path", "column_name", "compressed_bytes"])
            .with_columns(polars.col("column_name").str.to_lowercase().alias("__cn"))
            .filter(polars.col("__cn").is_in(list(selected_cols)))
        )
        if sel.height == 0:
            return set(), {}
        agg = sel.group_by("file_path").agg(
            [
                polars.col("compressed_bytes").sum().alias("__b"),
                polars.col("compressed_bytes").is_null().sum().alias("__nulls"),
            ]
        )
        tier3_files: Set[str] = set()
        proj: Dict[str, int] = {}
        for r in agg.iter_rows(named=True):
            if int(r["__nulls"] or 0) == 0:
                fp = r["file_path"]
                tier3_files.add(fp)
                proj[fp] = int(r["__b"] or 0)
        return tier3_files, proj

    def _ratio_bytes(
        self,
        file_key: str,
        key_size: Dict[str, int],
        selected_cols: Set[str],
        schema_types: Dict[str, str],
    ) -> int:
        """Fallback size: scale the whole-file bytes by the selected columns'
        type-width share of the table schema.  Used only when precise
        per-column ``compressed_bytes`` are unavailable for *file_key*."""
        full = int(key_size.get(file_key, 0))
        if full <= 0 or not schema_types:
            return full
        all_cols = {
            c: ty for c, ty in schema_types.items()
            if c not in (ROWID_COL, TIMESTAMP_COL)
        }
        total_w = sum(self._type_width(ty) for ty in all_cols.values())
        if total_w <= 0:
            return full
        sel_w = sum(self._type_width(all_cols[c]) for c in selected_cols if c in all_cols)
        if sel_w <= 0:
            return full
        return int(full * sel_w / total_w)

    # ----------------------- main API -----------------------
    def estimate(self) -> Reflection:
        """
        Returns a dict with keys: STORAGE_TYPE, BYTES_AFFECTED, FILE_LIST.
        Performs RBAC check and column validation.
        """
        self.timer = Timer()
        if self.plan_stats is None:
            self.plan_stats = PlanStats()

        # One profiler for the whole estimate; threaded into _prune_files so the
        # stats-cache and pruned-file counters mirror the write-path convention.
        prune_profiler = Profiler()

        supers: List[SuperSnapshot] = []
        reflection_file_size = 0        # projected (selected-column) bytes — routing
        reflection_file_size_raw = 0    # whole-file bytes — physical footprint
        max_freshness_ms = 0
        files_before_prune = 0
        files_pruned = 0
        files_kept = 0

        super_map = self._get_supertable_map()

        # Discover snapshots
        for super_name, tables in super_map:
            # Collect snapshots ONCE per super_name (avoid redundant SCAN per simple table)
            all_snapshots = self._collect_snapshots_from_redis(organization=self.organization, super_name=super_name)

            for simple_name in tables:
                snapshots = self._filter_snapshots(super_name, simple_name, all_snapshots)
                # Defence in depth: the read path must never bootstrap a
                # missing supertable. ``DataReader._assert_targets_exist``
                # is the primary guard at the entry point; this kwarg
                # ensures any other caller of ``DataEstimator`` (or any
                # future code path) cannot accidentally side-effect a
                # creation through the SuperTable constructor.
                super_table = SuperTable(
                    super_name, self.organization, create_if_missing=False,
                )

                schema: Set[str] = set()
                schema_types: Dict[str, str] = {}
                raw_keys: List[str] = []
                key_size: Dict[str, int] = {}
                stats_file: Optional[str] = None

                current_version = 0
                for snapshot in snapshots:
                    ts = int(snapshot.get("last_updated_ms", 0))
                    if ts > max_freshness_ms:
                        max_freshness_ms = ts
                    current_snapshot_path = snapshot["path"]
                    current_snapshot_data = snapshot.get("payload")
                    if not (isinstance(current_snapshot_data, dict) and isinstance(
                            current_snapshot_data.get("resources"), list)):
                        current_snapshot_data = super_table.read_simple_table_snapshot(current_snapshot_path)

                    current_version = current_snapshot_data.get("snapshot_version", 0)
                    current_schema = self._schema_to_dict(current_snapshot_data.get("schema", {}))
                    lowered_schema = dict_keys_to_lowercase(current_schema)
                    schema.update(lowered_schema.keys())
                    # Retain name->type for the projection ratio fallback. First
                    # writer wins for a given column (schemas are stable per table).
                    for _cname, _ctype in lowered_schema.items():
                        schema_types.setdefault(_cname, _ctype)
                    sf = current_snapshot_data.get("stats_file")
                    if sf:
                        stats_file = sf

                    resources = current_snapshot_data.get("resources", []) or []
                    for resource in resources:
                        file_key = resource.get("file")
                        if not file_key:
                            continue
                        raw_keys.append(file_key)
                        key_size[file_key] = int(resource.get("file_size", 0))

                # Which columns does the query actually read? None => SELECT *
                # (whole table, no projection savings).
                selected_cols = self._selected_columns(super_name, simple_name)
                need_projection = (
                    selected_cols is not None
                    and settings.SUPERTABLE_READ_PROJECTION_SIZING_ENABLED
                )
                has_predicate = bool(
                    self.predicate_constraints.get((super_name.lower(), simple_name.lower()))
                )

                # Load the stats artifact ONCE per table and reuse it for BOTH
                # predicate pruning and projection sizing (cache-backed: a repeated
                # read of the same table is a memory hit). Only load when at least
                # one consumer needs it, so a SELECT * with no WHERE stays free.
                stats_df: Optional["polars.DataFrame"] = None
                if stats_file and (need_projection or has_predicate):
                    stats_df = load_stats(stats_file, allow_cache=True, profiler=prune_profiler)

                # Read-path pruning: drop raw keys whose stats prove they cannot
                # satisfy the query's WHERE before resolving them to scan URLs.
                # The span accumulates the wall-clock of the whole pruning step
                # (predicate eval) across every table in the query.
                with prune_profiler.span("read.prune"):
                    survivors = self._prune_files(
                        super_name, simple_name, raw_keys, stats_df,
                        profiler=prune_profiler,
                    )
                files_before_prune += len(raw_keys)
                files_pruned += len(raw_keys) - len(survivors)
                files_kept += len(survivors)

                # Projection-aware size: a query selecting specific columns scans
                # only those columns' on-disk (compressed) chunks, not the whole
                # multi-column file. Precise path sums per-column compressed_bytes
                # from the stats artifact; files predating that column (or with no
                # stats) fall back to a type-width ratio of the whole-file size.
                # SELECT * keeps every column (full file).
                tier3_files: Set[str] = set()
                proj: Dict[str, int] = {}
                if need_projection:
                    tier3_files, proj = self._projected_bytes_index(stats_df, selected_cols)

                parquet_files: List[str] = []
                for file_key in survivors:
                    parquet_files.append(self._to_duckdb_path(file_key))
                    full = int(key_size.get(file_key, 0))
                    reflection_file_size_raw += full
                    if not need_projection:
                        reflection_file_size += full
                    elif file_key in tier3_files:
                        reflection_file_size += proj.get(file_key, 0)
                    else:
                        reflection_file_size += self._ratio_bytes(
                            file_key, key_size, selected_cols, schema_types
                        )

                # SuperSnapshot is created ONCE per (super_name, simple_name) after
                # all snapshot iterations have accumulated their files and schema.
                # Creating it inside the loop caused duplicate SuperSnapshot entries
                # with cumulatively growing file lists (each iteration re-sharing the
                # same mutable parquet_files list), inflating total_reflections and
                # confusing the executor's snapshots_by_key lookup.
                if snapshots:
                    super_snapshot = SuperSnapshot(super_name=super_name, simple_name=simple_name,
                                                   simple_version=current_version, files=parquet_files, columns=schema)
                    supers.append(super_snapshot)

        # Validate requested columns
        missing_info = get_missing_columns(self.tables, supers)

        # Total parquet files across all selected snapshots
        total_reflections = sum(len(s.files) for s in supers)

        # Ensure every selected snapshot has at least one file
        all_have_files = all(bool(s.files) for s in supers)

        if not supers or missing_info or not all_have_files:
            if not supers:
                msg = "No snapshots selected."
            elif missing_info:
                # missing_info: List[(super_name, table_name, Set[missing_cols])]
                details = []
                for super_name, table_name, cols in missing_info:
                    cols_str = ", ".join(sorted(cols))
                    details.append(f"{super_name}.{table_name}: {cols_str}")
                msg = "Missing required column(s): " + " | ".join(details)
            else:  # not all_have_files
                msg = "No parquet files found for one or more selected tables."

            logger.warning(msg)
            raise RuntimeError(msg)

        self.timer.capture_and_reset_timing(event="ESTIMATE")

        self.plan_stats.add_stat({"REFLECTIONS": total_reflections})
        # REFLECTION_SIZE is the projected (selected-column) size that drives
        # engine routing; REFLECTION_SIZE_RAW is the whole-file footprint, kept
        # for observability so the two are comparable in the plans payload.
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_SIZE_RAW": reflection_file_size_raw})

        # Read-path pruning observability — only when pruning is engaged, so a
        # disabled-pruning read doesn't litter the payload with noise. Mirrors
        # the write path: surface the count effect plus the profiler's IO/cache
        # counters (stats_cache_hit/miss, read_pruned_files).
        if settings.SUPERTABLE_READ_PRUNING_ENABLED:
            self.plan_stats.add_stat({"FILES_BEFORE_PRUNE": files_before_prune})
            self.plan_stats.add_stat({"FILES_PRUNED": files_pruned})
            self.plan_stats.add_stat({"FILES_KEPT": files_kept})
            self.plan_stats.add_stat({
                "PRUNE_DURATION_MS": round(
                    prune_profiler.timings.get("read.prune", 0.0) * 1000, 3
                )
            })
            prune_counts = prune_profiler.emit_counts()
            if prune_counts:
                self.plan_stats.add_stat({"PRUNE_COUNTS": prune_counts})

        return Reflection(
            storage_type=type(self.storage).__name__,
            reflection_bytes=int(reflection_file_size),
            total_reflections=total_reflections,
            supers=supers,
            freshness_ms=max_freshness_ms,
        )