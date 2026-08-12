# supertable/engine/data_estimator.py

from __future__ import annotations

import logging
import os
import re
from collections import Counter, defaultdict
from typing import Iterable, Set, List, Dict, Optional, Tuple
from urllib.parse import urlparse, urlsplit, urlunsplit

import polars

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.super_table import SuperTable
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.utils.snapshot import complete_snapshot_payload
from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.utils.profiler import Profiler
from supertable.redis_catalog import RedisCatalog  # Redis leaf pointers for snapshots

from supertable.data_classes import JoinEdge
from supertable.utils.sql_parser import TableDefinition
from supertable.engine.join_pruner import prune_files_across_joins
from supertable.processing import (
    load_stats,
    prune_files_by_predicates,
    ROWID_COL,
    TIMESTAMP_COL,
)


def _safe_path_for_log(value: object) -> str:
    """Render a storage path without leaking presign/SAS credentials."""
    text = str(value or "")
    try:
        parsed = urlsplit(text)
        if parsed.scheme in ("http", "https") and (parsed.query or parsed.fragment):
            return urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, "<redacted>", "")
            )
    except Exception:
        pass
    return text


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
        join_edges: Optional[List[JoinEdge]] = None,
        plan_stats: Optional[PlanStats] = None,
        join_pruning_lanes: Optional[Set[str]] = None,
    ):
        self.organization = organization
        self.storage = storage
        self.tables = tables
        # (super_lower, simple_lower) -> List[Dict[col, PredInterval]] from the
        # query's WHERE clauses; used to prune files by the stats artifact.
        # None / empty ⇒ no read-path pruning.
        self.predicate_constraints = predicate_constraints or {}
        # Equi-join links between the query's tables.  After each table is
        # pruned by its own WHERE, a filtered table's surviving join-key range
        # is propagated across these edges to prune its partners (cross-table
        # file pruning).  Empty ⇒ no propagation (single-table or un-joined).
        self.join_edges: List[JoinEdge] = join_edges or []
        # Optional executor-specific whitelist passed to the join kernel.
        # Disabled lanes become unknown and therefore retain files.  ``None``
        # preserves the kernel's full safe-lane set for standalone callers.
        self.join_pruning_lanes = join_pruning_lanes
        self.timer: Optional[Timer] = None
        # When the caller (DataReader) injects its PlanStats, estimator stats —
        # REFLECTIONS, REFLECTION_SIZE and the read-pruning counters — land on
        # the same object that flows to extend_execution_plan, so they reach the
        # read monitoring payload. Standalone callers get a fresh PlanStats.
        self.plan_stats: Optional[PlanStats] = plan_stats
        self.catalog = RedisCatalog()
        # Lazily populated only when storage helpers cannot resolve keys.  These
        # settings/storage attributes are invariant for one estimator run; do
        # not rediscover them for every file in a large scan.
        self._fallback_url_config: Optional[Tuple[Optional[str], Optional[str], bool, bool]] = None

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
            return self._normalize_endpoint_for_s3(settings.STORAGE_ENDPOINT_URL)

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
                        logger.debug(
                            f"[estimate.resolve] presigned → {_safe_path_for_log(url)}"
                        )
                        return url
                except Exception as e:
                    logger.warning(
                        "[estimate.resolve] presign failed; falling back: "
                        f"{_safe_path_for_log(e)}"
                    )

        # 2) If already URL, return as-is.
        if "://" in key:
            logger.debug(f"[estimate.resolve] already URL: {_safe_path_for_log(key)}")
            return key

        # 3) storage helpers
        for attr in ("to_duckdb_path", "make_duckdb_url", "make_url"):
            fn = getattr(self.storage, attr, None)
            if callable(fn):
                try:
                    url = fn(key)  # key in, URL out (not presigned)
                    if isinstance(url, str) and url:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"[estimate.resolve] storage.{attr} → "
                                f"{_safe_path_for_log(url)}"
                            )
                        return url
                except Exception as e:
                    logger.debug(
                        f"[estimate.resolve] storage.{attr} failed: "
                        f"{_safe_path_for_log(e)}"
                    )

        # 4) Construct URL from endpoint/bucket
        fallback = getattr(self, "_fallback_url_config", None)
        if fallback is None:
            fallback = (
                self._detect_endpoint(),
                self._detect_bucket(),
                settings.SUPERTABLE_DUCKDB_USE_HTTPFS,
                self._detect_ssl(),
            )
            self._fallback_url_config = fallback
        endpoint_raw, bucket, use_http, use_ssl = fallback
        scheme = "https" if use_ssl else "http"
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
        if not self._has_potential_select_predicate(occurrences):
            return raw_keys
        try:
            return prune_files_by_predicates(
                raw_keys, stats_df, occurrences, profiler=profiler,
            )
        except Exception as e:
            logger.warning(f"[estimate.prune] pruning skipped for {super_name}.{simple_name}: {e}")
            return raw_keys

    @staticmethod
    def _has_potential_select_predicate(occurrences) -> bool:
        """Whether every shared scan occurrence has a usable SELECT lane.

        ``prune_files_by_predicates`` unions the files needed by physical-table
        occurrences.  One empty occurrence, or one occurrence containing only
        comparison lanes SELECT deliberately distrusts (string/double), makes
        file elimination impossible.  Detect that before loading or scanning a
        stats artifact.  This mirrors the processing layer's conservative lane
        gates; returning ``False`` can only skip a guaranteed no-op.
        """
        if not occurrences:
            return False
        try:
            for occurrence in occurrences:
                if not occurrence:
                    return False
                occurrence_can_prune = False
                for predicate in occurrence.values():
                    lane = predicate.lane
                    if lane in ("date", "timestamp", "timestamptz"):
                        occurrence_can_prune = True
                        break
                    if lane in ("numeric", "numeric_cast") and not any(
                        isinstance(bound, float)
                        for bound in (predicate.lo, predicate.hi)
                        if bound is not None
                    ):
                        occurrence_can_prune = True
                        break
                if not occurrence_can_prune:
                    return False
        except Exception:
            return False
        return True

    @staticmethod
    def _validated_file_subset(
        original: Iterable[str], candidate: Iterable[str],
    ) -> Optional[List[str]]:
        """Return the candidate list only when it is a multiset subset.

        File pruning is allowed to remove input occurrences, never to introduce
        or duplicate them.  Treating the lists as multisets also covers corrupt
        snapshots that happen to repeat a resource key.  ``None`` means the
        pruning result violated that contract and must be discarded wholesale.
        """
        try:
            # Every fail-open/no-op path intentionally returns the exact input
            # list.  Avoid allocating two Counters on the overwhelmingly common
            # unfiltered-table path.
            if candidate is original and isinstance(original, list):
                return original
            original_list = list(original)
            candidate_list = list(candidate)
            if Counter(candidate_list) - Counter(original_list):
                return None
        except Exception:
            # File keys are strings in valid snapshots.  Any unhashable or
            # otherwise malformed value is not safe to apply as a pruning plan.
            return None
        return candidate_list

    @staticmethod
    def _stats_for_complete_files(
        stats_df: Optional["polars.DataFrame"],
        resource_rows: Dict[str, Optional[int]],
    ) -> Optional["polars.DataFrame"]:
        """Keep stats only for files whose row-group manifest is complete.

        The table-level ``stats_rows`` count detects a truncated artifact, but
        not a same-height substitution (for example, one missing row-group row
        replaced by a duplicate belonging to another file).  Treat a file's
        stats as absence proof only when their structure agrees with the
        snapshot resource's independently recorded physical row count:

        * the resource row count is an exact non-negative integer;
        * row-group ids are exactly ``0..N-1``;
        * every row in a row group reports the same non-negative row count and
          those counts sum to the resource row count;
        * every row group has the identical, exact footer column-name set; and
        * every ``(row_group_id, column_name)`` slot occurs exactly once.

        Rows for an invalid or unmanifested file are removed.  All pruning
        consumers interpret an absent file as unknown and retain it; a join
        source with an absent file similarly exports an unknown range.  Any
        validation failure returns ``None`` and disables stats use table-wide.
        """
        if stats_df is None:
            return None
        if not isinstance(stats_df, polars.DataFrame):
            return None
        if stats_df.height == 0:
            return stats_df

        required = {
            "file_path", "row_group_id", "column_name", "row_group_rows",
        }
        if not required.issubset(stats_df.columns):
            return None

        manifest = {
            file_path: rows
            for file_path, rows in resource_rows.items()
            if isinstance(file_path, str)
            and isinstance(rows, int)
            and not isinstance(rows, bool)
            and rows >= 0
        }
        if not manifest:
            # Preserve the input schema so downstream consumers can take their
            # normal empty-frame fast path without special casing this guard.
            return stats_df.head(0)

        try:
            # Keep the validation columnar.  A stats artifact can have millions
            # of (row-group x column) rows; only the final trusted file names
            # need to cross into Python for the optional filter.
            manifested_stats = stats_df.filter(
                polars.col("file_path").is_in(list(manifest))
            )
            groups = (
                manifested_stats
                .select(list(required))
                .group_by(["file_path", "row_group_id"])
                .agg([
                    polars.len().alias("__slots"),
                    polars.col("column_name").n_unique().alias("__unique_slots"),
                    polars.col("column_name").unique().sort().alias("__columns"),
                    polars.col("row_group_rows").n_unique().alias("__row_counts"),
                    polars.col("row_group_rows").first().alias("__rows"),
                    polars.col("column_name").is_not_null().all().alias("__names_valid"),
                    (
                        polars.col("row_group_rows").is_not_null()
                        & (polars.col("row_group_rows") >= 0)
                    ).all().alias("__rows_valid"),
                ])
            )
            expected = polars.DataFrame({
                "file_path": list(manifest),
                "__expected_rows": list(manifest.values()),
            }, schema={"file_path": polars.Utf8, "__expected_rows": polars.Int64})
            trusted_frame = (
                groups
                .group_by("file_path")
                .agg([
                    polars.len().alias("__group_count"),
                    polars.col("row_group_id").min().alias("__min_group"),
                    polars.col("row_group_id").max().alias("__max_group"),
                    polars.col("row_group_id").is_not_null().all().alias("__ids_valid"),
                    (polars.col("row_group_id") >= 0).all().alias("__ids_nonnegative"),
                    polars.col("__rows").sum().alias("__total_rows"),
                    (polars.col("__slots") == polars.col("__unique_slots"))
                    .all().alias("__slots_valid"),
                    (polars.col("__row_counts") == 1).all().alias("__counts_valid"),
                    polars.col("__names_valid").all().alias("__all_names_valid"),
                    polars.col("__rows_valid").all().alias("__all_rows_valid"),
                    polars.col("__columns").n_unique().alias("__column_sets"),
                ])
                .join(expected, on="file_path", how="inner")
                .filter(
                    polars.col("__ids_valid")
                    & polars.col("__ids_nonnegative")
                    & polars.col("__slots_valid")
                    & polars.col("__counts_valid")
                    & polars.col("__all_names_valid")
                    & polars.col("__all_rows_valid")
                    & (polars.col("__min_group") == 0)
                    & (
                        polars.col("__max_group")
                        == polars.col("__group_count") - 1
                    )
                    & (polars.col("__column_sets") == 1)
                    & (
                        polars.col("__total_rows")
                        == polars.col("__expected_rows")
                    )
                )
                .select("file_path")
            )
            if trusted_frame.height == 0:
                return stats_df.head(0)
            trusted = trusted_frame.get_column("file_path").to_list()
            # The normal healthy-artifact path avoids a second frame filter and
            # allocation.  A resource absent from the artifact does not matter:
            # its data file is unknown and retained by downstream consumers.
            if (
                manifested_stats.height == stats_df.height
                and len(trusted) == groups.get_column("file_path").n_unique()
            ):
                return stats_df
            return manifested_stats.filter(
                polars.col("file_path").is_in(trusted)
            )
        except Exception:
            # Stats are optional and cannot be allowed to fail or narrow a read.
            return None

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
        file_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[Set[str], Dict[str, int]]:
        """Sum per-column ``compressed_bytes`` for the selected columns per file.

        Returns ``(tier3_files, proj)`` where *proj* maps a file path to the
        summed on-disk bytes of its selected columns, and *tier3_files* is the
        subset of files for which that sum is *trustworthy* — every matched row
        carried a non-NULL ``compressed_bytes``.  A file with any NULL (an older
        carried-forward row) is omitted so the caller falls back to a whole-file
        ratio rather than under-counting it as zero.  When *file_keys* is given,
        rows for files already removed by predicate/join pruning are discarded
        before aggregation.
        """
        if (
            stats_df is None
            or stats_df.height == 0
            or "compressed_bytes" not in stats_df.columns
        ):
            return set(), {}
        projected = stats_df.select(
            ["file_path", "column_name", "compressed_bytes"]
        )
        if file_keys is not None:
            survivor_keys = list(file_keys)
            if not survivor_keys:
                return set(), {}
            projected = projected.filter(
                polars.col("file_path").is_in(survivor_keys)
            )
        sel = (
            projected
            .with_columns(polars.col("column_name").str.to_lowercase().alias("__cn"))
            .filter(polars.col("__cn").is_in(list(selected_cols)))
        )
        if sel.height == 0:
            return set(), {}
        agg = sel.group_by("file_path").agg(
            [
                polars.col("compressed_bytes").sum().alias("__b"),
                (
                    polars.col("compressed_bytes").is_null()
                    | (polars.col("compressed_bytes") < 0)
                ).sum().alias("__invalid"),
            ]
        )
        tier3_files: Set[str] = set()
        proj: Dict[str, int] = {}
        for r in agg.iter_rows(named=True):
            projected_bytes = r["__b"]
            if (
                int(r["__invalid"] or 0) == 0
                and isinstance(projected_bytes, int)
                and not isinstance(projected_bytes, bool)
                and projected_bytes >= 0
            ):
                fp = r["file_path"]
                tier3_files.add(fp)
                proj[fp] = projected_bytes
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
        return self._ratio_bytes_with_widths(
            full,
            self._projection_widths(selected_cols, schema_types),
        )

    def _projection_widths(
        self,
        selected_cols: Set[str],
        schema_types: Dict[str, str],
    ) -> Optional[Tuple[int, int]]:
        """Return ``(selected_width, total_width)`` for fallback sizing.

        ``None`` means the schema cannot establish a useful projection share,
        so callers conservatively charge each file's full size.  The result is
        table-constant and should be computed once for a batch of files.
        """
        if not schema_types:
            return None
        all_cols = {
            c: ty for c, ty in schema_types.items()
            if c not in (ROWID_COL, TIMESTAMP_COL)
        }
        total_w = sum(self._type_width(ty) for ty in all_cols.values())
        if total_w <= 0:
            return None
        sel_w = sum(self._type_width(all_cols[c]) for c in selected_cols if c in all_cols)
        if sel_w <= 0:
            return None
        return sel_w, total_w

    @staticmethod
    def _ratio_bytes_with_widths(
        full: int,
        widths: Optional[Tuple[int, int]],
    ) -> int:
        """Apply precomputed projection *widths* to one whole-file size."""
        if full <= 0 or widths is None:
            return full
        sel_w, total_w = widths
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

        # Tables that participate in a *usable* join edge need their stats loaded
        # even for a bare SELECT * with no WHERE: their surviving files still
        # bound the join keys they export to partners during cross-table pruning.
        # Edges that cannot prune either endpoint (for example FULL OUTER JOIN)
        # consume no stats and never need to enter the kernel.  A physical table
        # mentioned more than once is also ambiguous here: records and join maps
        # use one normalized key, so collapsing occurrences could export the
        # wrong filtered range.  Disable every edge touching such a key.
        table_key_counts = Counter(
            (t.super_name.lower(), t.simple_name.lower()) for t in self.tables
        )
        duplicate_table_keys = {
            key for key, count in table_key_counts.items() if count > 1
        }
        candidate_join_edges: List[JoinEdge] = []
        if settings.SUPERTABLE_READ_PRUNING_ENABLED:
            candidate_join_edges = [
                edge for edge in self.join_edges
                if (edge.prune_left or edge.prune_right)
                and edge.left_table not in duplicate_table_keys
                and edge.right_table not in duplicate_table_keys
            ]

        join_table_keys: Set[Tuple[str, str]] = set()
        if candidate_join_edges:
            for _edge in candidate_join_edges:
                join_table_keys.add(_edge.left_table)
                join_table_keys.add(_edge.right_table)

        # ---- Pass 1: discover snapshots + per-table (own-WHERE) pruning --------
        # Each record carries everything Pass 2 needs to size files and build the
        # SuperSnapshot, plus the own-WHERE survivors + loaded stats that feed the
        # cross-table join propagation that runs between the two passes.
        records: List[Dict[str, object]] = []

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
                resource_rows: Dict[str, Optional[int]] = {}
                stats_file: Optional[str] = None
                expected_stats_rows: Optional[int] = None
                pinned_snapshot_metadata: List[Dict[str, object]] = []
                # A zero-resource manifest is a valid, committed table state
                # after metadata-only delete-all.  Keep this proof separate
                # from ``not raw_keys``: a corrupt/legacy snapshot that omits
                # ``resources`` must not be silently reinterpreted as empty.
                authoritative_empty = bool(snapshots)

                current_version = 0
                for snapshot in snapshots:
                    ts = int(snapshot.get("last_updated_ms", 0))
                    if ts > max_freshness_ms:
                        max_freshness_ms = ts
                    current_snapshot_path = snapshot["path"]
                    leaf_payload = snapshot.get("payload")
                    current_snapshot_data = complete_snapshot_payload(
                        leaf_payload,
                        expected_version=snapshot.get("version"),
                    )
                    if current_snapshot_data is None:
                        current_snapshot_data = super_table.read_simple_table_snapshot(current_snapshot_path)

                    # Pin deletion metadata from the exact snapshot document
                    # whose resources are accumulated below.  In particular,
                    # path-only Redis leaves take this branch and therefore do
                    # not lose tombstones that live only in the heavy JSON.
                    raw_tombstone = current_snapshot_data.get("tombstone")
                    if raw_tombstone is not None and not isinstance(raw_tombstone, str):
                        raise RuntimeError(
                            f"Invalid tombstone pointer for {super_name}.{simple_name}"
                        )
                    tombstone_key = raw_tombstone or None

                    raw_tombstone_rows = current_snapshot_data.get("tombstone_rows")
                    tombstone_rows: Optional[int]
                    if raw_tombstone_rows is None:
                        if tombstone_key:
                            raise RuntimeError(
                                f"Snapshot for {super_name}.{simple_name} references "
                                "a deletion vector without an exact row count"
                            )
                        tombstone_rows = None
                    elif (
                        isinstance(raw_tombstone_rows, int)
                        and not isinstance(raw_tombstone_rows, bool)
                        and raw_tombstone_rows >= 0
                    ):
                        tombstone_rows = int(raw_tombstone_rows)
                    else:
                        # Tombstone metadata is a sealed state machine even when
                        # the pointer is absent.  Treating malformed/positive
                        # counts as a legacy placeholder would erase evidence of
                        # deletes and expose their physical rows.  Only missing,
                        # None, or the exact non-bool integer zero is valid.
                        raise RuntimeError(
                            f"Invalid tombstone row count for "
                            f"{super_name}.{simple_name}"
                        )

                    if tombstone_key and not tombstone_rows:
                        # The writer never publishes an active pointer for an
                        # empty deletion vector.  Accepting pointer+0 would let
                        # an attacker/corrupt snapshot attach an unsealed file
                        # while claiming there is nothing to validate.
                        raise RuntimeError(
                            f"Snapshot for {super_name}.{simple_name} references "
                            "a deletion vector without a positive row count"
                        )

                    if not tombstone_key and tombstone_rows and tombstone_rows > 0:
                        raise RuntimeError(
                            f"Snapshot for {super_name}.{simple_name} records "
                            f"{tombstone_rows} tombstoned rows but no tombstone pointer"
                        )

                    raw_tombstone_digest = current_snapshot_data.get(
                        "tombstone_digest"
                    )
                    if tombstone_key:
                        if not (
                            isinstance(raw_tombstone_digest, str)
                            and re.fullmatch(r"[0-9a-f]{64}", raw_tombstone_digest)
                        ):
                            raise RuntimeError(
                                f"Snapshot for {super_name}.{simple_name} references "
                                "a deletion vector without a valid SHA-256 digest"
                            )
                        tombstone_digest = raw_tombstone_digest
                    else:
                        if raw_tombstone_digest not in (None, ""):
                            raise RuntimeError(
                                f"Snapshot for {super_name}.{simple_name} records "
                                "a deletion-vector digest without a pointer"
                            )
                        tombstone_digest = None

                    # Share policy is an overlay stored in the atomic Redis leaf
                    # payload.  Prefer it there even when resources must fall
                    # back to the heavy snapshot JSON.
                    share_row_filter = None
                    if isinstance(leaf_payload, dict):
                        candidate_filter = leaf_payload.get("_row_filter")
                        if isinstance(candidate_filter, str) and candidate_filter:
                            share_row_filter = candidate_filter
                    if share_row_filter is None:
                        candidate_filter = current_snapshot_data.get("_row_filter")
                        if isinstance(candidate_filter, str) and candidate_filter:
                            share_row_filter = candidate_filter

                    pinned_snapshot_metadata.append({
                        "path": current_snapshot_path,
                        "table_name": snapshot.get("table_name"),
                        "tombstone_key": tombstone_key,
                        "tombstone_rows": tombstone_rows,
                        "tombstone_digest": tombstone_digest,
                        "share_row_filter": share_row_filter,
                    })

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
                        recorded_stats_rows = current_snapshot_data.get(
                            "stats_rows"
                        )
                        expected_stats_rows = (
                            recorded_stats_rows
                            if isinstance(recorded_stats_rows, int)
                            and not isinstance(recorded_stats_rows, bool)
                            and recorded_stats_rows >= 0
                            else None
                        )

                    resources_value = current_snapshot_data.get("resources")
                    manifest_is_authoritative_empty = (
                        isinstance(resources_value, list)
                        and not resources_value
                        and bool(current_schema)
                        and not tombstone_key
                    )
                    authoritative_empty = (
                        authoritative_empty and manifest_is_authoritative_empty
                    )
                    resources = resources_value if isinstance(resources_value, list) else []
                    for resource in resources:
                        file_key = resource.get("file")
                        if not file_key:
                            continue
                        raw_keys.append(file_key)
                        key_size[file_key] = int(resource.get("file_size", 0))
                        rows_value = resource.get("rows")
                        valid_rows = (
                            rows_value
                            if isinstance(rows_value, int)
                            and not isinstance(rows_value, bool)
                            and rows_value >= 0
                            else None
                        )
                        if file_key in resource_rows:
                            # A repeated resource key with conflicting or
                            # missing cardinality has no unambiguous manifest.
                            if resource_rows[file_key] != valid_rows:
                                resource_rows[file_key] = None
                        else:
                            resource_rows[file_key] = valid_rows

                key = (super_name.lower(), simple_name.lower())

                # Which columns does the query actually read? None => SELECT *
                # (whole table, no projection savings).
                selected_cols = self._selected_columns(super_name, simple_name)
                need_projection = (
                    selected_cols is not None
                    and settings.SUPERTABLE_READ_PROJECTION_SIZING_ENABLED
                )
                has_predicate = self._has_potential_select_predicate(
                    self.predicate_constraints.get(key)
                )

                # Load the stats artifact ONCE per table and reuse it for predicate
                # pruning, projection sizing AND cross-table join propagation
                # (cache-backed: a repeated read of the same table is a memory
                # hit). Only load when at least one consumer needs it, so a
                # SELECT * that neither filters nor joins stays free.
                stats_df: Optional["polars.DataFrame"] = None
                if stats_file and (need_projection or has_predicate or key in join_table_keys):
                    try:
                        stats_df = load_stats(
                            stats_file, allow_cache=True, profiler=prune_profiler,
                        )
                    except Exception as stats_err:
                        # Stats are an optional optimisation artifact.  A stale,
                        # corrupt, unavailable, or malformed pointer must never
                        # turn a valid SELECT into an error or a narrower scan.
                        logger.warning(
                            f"[estimate.stats] stats unavailable for "
                            f"{super_name}.{simple_name}; pruning and precise "
                            f"projection sizing skipped: {stats_err}"
                        )
                        stats_df = None

                if stats_df is not None and expected_stats_rows is not None:
                    try:
                        actual_stats_rows = int(stats_df.height)
                    except Exception:
                        actual_stats_rows = -1
                    if actual_stats_rows != expected_stats_rows:
                        # A stats parquet may still be syntactically readable
                        # after a bad copy/truncation.  Its snapshot records the
                        # immutable artifact's exact row count; disagreement
                        # means it is incomplete/foreign and therefore cannot
                        # prove a data file absent.  Projection sizing also
                        # falls back rather than under-counting from it.
                        logger.warning(
                            f"[estimate.stats] stats row-count mismatch for "
                            f"{super_name}.{simple_name}: expected "
                            f"{expected_stats_rows}, got {actual_stats_rows}; "
                            f"pruning and precise projection sizing skipped"
                        )
                        stats_df = None

                if stats_df is not None:
                    # A table-level row count cannot identify a missing slot
                    # replaced by a duplicate elsewhere.  Remove every file
                    # whose stats do not form a complete per-resource manifest;
                    # absent stats always retain the data file.
                    stats_df = self._stats_for_complete_files(
                        stats_df, resource_rows,
                    )

                # Read-path pruning: drop raw keys whose stats prove they cannot
                # satisfy this table's own WHERE before any cross-table step.
                # The span accumulates the wall-clock of the per-table pruning
                # (predicate eval) across every table in the query.
                literal_count_before = prune_profiler.counts.get(
                    "read_pruned_files", 0,
                )
                with prune_profiler.span("read.prune"):
                    literal_survivors = self._prune_files(
                        super_name, simple_name, raw_keys, stats_df,
                        profiler=prune_profiler,
                    )
                # Validate again at the estimator boundary so even an overridden
                # or future _prune_files implementation cannot inject files or
                # make the aggregate counters negative.
                validated_literal = self._validated_file_subset(
                    raw_keys, literal_survivors,
                )
                invalid_literal = (
                    validated_literal is None
                    or (raw_keys and not validated_literal)
                )
                if invalid_literal:
                    logger.warning(
                        f"[estimate.prune] invalid survivor set for "
                        f"{super_name}.{simple_name}; keeping all candidate files"
                    )
                    literal_survivors = list(raw_keys)
                else:
                    literal_survivors = validated_literal
                # Reconcile observability from the validated boundary result,
                # rather than trusting an inner implementation to update the
                # profiler exactly once.  This also keeps custom/no-op pruners
                # from reporting removals that were never applied.
                literal_count_after = literal_count_before + (
                    len(raw_keys) - len(literal_survivors)
                )
                if literal_count_after:
                    prune_profiler.counts["read_pruned_files"] = literal_count_after
                else:
                    prune_profiler.counts.pop("read_pruned_files", None)
                files_before_prune += len(raw_keys)

                records.append({
                    "super_name": super_name,
                    "simple_name": simple_name,
                    "key": key,
                    "schema": schema,
                    "schema_types": schema_types,
                    "key_size": key_size,
                    "current_version": current_version,
                    "has_snapshots": bool(snapshots),
                    "pinned_snapshot_metadata": pinned_snapshot_metadata,
                    "stats_df": stats_df,
                    "selected_cols": selected_cols,
                    "need_projection": need_projection,
                    "files_before": len(raw_keys),
                    "snapshot_resource_keys": list(raw_keys),
                    "authoritative_empty": authoritative_empty and not raw_keys,
                    "survivors": literal_survivors,
                })

        # ---- Cross-table join pruning (ON by default under the read switch) ----
        # Propagate every surviving file-set's join-key min/max across the query's
        # equi-join edges to a fixpoint, so a table narrowed by its own WHERE also
        # narrows its join partners. ``allow_empty=False`` keeps the estimator's
        # "never empty a table" guard — a genuinely empty join surfaces at
        # execution time rather than as a zero-file estimate error.
        join_files_removed = 0
        join_iterations = 0
        # Re-check uniqueness on the concrete records before constructing maps:
        # dictionary comprehensions must never silently collapse two records.
        records_by_key: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
        for record in records:
            records_by_key[record["key"]].append(record)
        runnable_join_edges = [
            edge for edge in candidate_join_edges
            if len(records_by_key.get(edge.left_table, [])) == 1
            and len(records_by_key.get(edge.right_table, [])) == 1
        ]
        if runnable_join_edges:
            # Same contract as _prune_files: a pruning failure must degrade to
            # "no pruning" (keep the Pass-1 survivors), never break the read.
            committed_join_plan = False
            join_count_before = prune_profiler.counts.get(
                "read_join_pruned_files", 0,
            )
            try:
                joined_keys = {
                    key
                    for edge in runnable_join_edges
                    for key in (edge.left_table, edge.right_table)
                }
                joined_records = {
                    key: records_by_key[key][0] for key in joined_keys
                }
                table_files = {
                    key: list(record["survivors"])
                    for key, record in joined_records.items()
                }
                table_stats = {
                    key: record["stats_df"]
                    for key, record in joined_records.items()
                }
                with prune_profiler.span("read.join_prune"):
                    join_plan = prune_files_across_joins(
                        runnable_join_edges,
                        {},  # own-WHERE pruning already applied per-table above
                        table_files,
                        table_stats,
                        allow_empty=False,
                        allowed_lanes=getattr(
                            self, "join_pruning_lanes", None,
                        ),
                    )

                # Stage and validate every table before mutating any record.
                # If one endpoint violates the subset contract, reject the whole
                # propagation plan: applying a partial fixpoint can be unsound.
                if set(join_plan.survivors) != set(table_files):
                    raise ValueError(
                        "join pruner returned an incomplete or foreign table set"
                    )
                staged_survivors: Dict[Tuple[str, str], List[str]] = {}
                staged_removed = 0
                for key, record in joined_records.items():
                    original = table_files[key]
                    proposed = join_plan.survivors[key]
                    validated = self._validated_file_subset(original, proposed)
                    if validated is None or (original and not validated):
                        raise ValueError(
                            f"join pruner returned an invalid survivor set for "
                            f"{key[0]}.{key[1]}"
                        )
                    staged_survivors[key] = validated
                    staged_removed += len(original) - len(validated)

                staged_iterations = max(0, int(join_plan.iterations))
                staged_summary = (
                    join_plan.summary()
                    if join_plan.steps and logger.isEnabledFor(logging.DEBUG)
                    else None
                )

                # Atomic commit after all endpoints have passed validation.
                for key, survivors in staged_survivors.items():
                    joined_records[key]["survivors"] = survivors
                committed_join_plan = True
                join_files_removed = staged_removed
                join_iterations = staged_iterations
                prune_profiler.add("read_join_pruned_files", join_files_removed)
                if staged_summary:
                    logger.debug("[estimate.join_prune]\n" + staged_summary)
            except Exception as jp_err:
                if committed_join_plan:
                    # Even instrumentation/logging failures after assignment
                    # must roll the records back to their Pass-1 survivors.
                    for key, original in table_files.items():
                        joined_records[key]["survivors"] = original
                    if join_count_before:
                        prune_profiler.counts["read_join_pruned_files"] = join_count_before
                    else:
                        prune_profiler.counts.pop("read_join_pruned_files", None)
                join_files_removed = 0
                join_iterations = 0
                logger.warning(f"[estimate.join_prune] cross-table pruning skipped: {jp_err}")

        # ---- Pass 2: resolve survivors to scan URLs, size, build snapshots -----
        for r in records:
            super_name = r["super_name"]
            simple_name = r["simple_name"]
            survivors = r["survivors"]
            key_size = r["key_size"]
            schema = r["schema"]
            schema_types = r["schema_types"]
            selected_cols = r["selected_cols"]
            need_projection = r["need_projection"]
            stats_df = r["stats_df"]

            # Projection-aware size: a query selecting specific columns scans only
            # those columns' on-disk (compressed) chunks, not the whole
            # multi-column file. Precise path sums per-column compressed_bytes from
            # the stats artifact; files predating that column (or with no stats)
            # fall back to a type-width ratio of the whole-file size. SELECT *
            # keeps every column (full file).
            tier3_files: Set[str] = set()
            proj: Dict[str, int] = {}
            if need_projection:
                try:
                    tier3_files, proj = self._projected_bytes_index(
                        stats_df,
                        selected_cols,
                        file_keys=(
                            survivors
                            if len(survivors) < r["files_before"]
                            else None
                        ),
                    )
                except Exception as projection_err:
                    # Projection stats only improve routing precision.  Fall
                    # back to schema-width/whole-file sizing if a legacy or
                    # malformed artifact cannot provide a trustworthy index.
                    logger.warning(
                        f"[estimate.projection] precise sizing skipped for "
                        f"{super_name}.{simple_name}: {projection_err}"
                    )
                    tier3_files, proj = set(), {}

            # The fallback width share depends only on this table's schema and
            # selected columns, not on a file.  Hoist it out of the survivor
            # loop; wide schemas with thousands of files otherwise repeat the
            # same dictionary/filter/sum work for every file lacking tier-3
            # compressed-byte stats.
            projection_widths = (
                self._projection_widths(selected_cols, schema_types)
                if need_projection
                else None
            )

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
                    reflection_file_size += self._ratio_bytes_with_widths(
                        full, projection_widths,
                    )
            files_kept += len(survivors)

            # SuperSnapshot is created ONCE per (super_name, simple_name) after all
            # snapshot iterations have accumulated their files and schema. Creating
            # it inside the loop caused duplicate SuperSnapshot entries with
            # cumulatively growing file lists, inflating total_reflections and
            # confusing the executor's snapshots_by_key lookup.
            if r["has_snapshots"]:
                pinned_metadata = r["pinned_snapshot_metadata"]
                if len(pinned_metadata) == 1:
                    pinned = pinned_metadata[0]
                else:
                    # ``super == simple`` is the legacy all-simple-tables scan
                    # and can accumulate multiple independently-versioned
                    # snapshots into one reflection.  One rowid-only DV cannot
                    # safely represent that set (rowids are table-local), so a
                    # tombstone or share filter on any member must fail closed.
                    if any(
                        meta.get("tombstone_key") or meta.get("share_row_filter")
                        for meta in pinned_metadata
                    ):
                        raise RuntimeError(
                            f"Cannot safely combine independently filtered "
                            f"snapshots for {super_name}.{simple_name}"
                        )
                    pinned = {}

                super_snapshot = SuperSnapshot(
                    super_name=super_name,
                    simple_name=simple_name,
                    simple_version=r["current_version"],
                    files=parquet_files,
                    columns=schema,
                    column_types=dict(schema_types),
                    resource_keys=list(survivors),
                    snapshot_resource_keys=list(r["snapshot_resource_keys"]),
                    snapshot_path=pinned.get("path"),
                    tombstone_key=pinned.get("tombstone_key"),
                    tombstone_rows=pinned.get("tombstone_rows"),
                    tombstone_digest=pinned.get("tombstone_digest"),
                    share_row_filter=pinned.get("share_row_filter"),
                )
                supers.append(super_snapshot)

        # files_before_prune counts raw candidates; files_kept the final survivors
        # after BOTH own-WHERE and cross-table join pruning. The difference is the
        # total number of files eliminated.
        files_pruned = files_before_prune - files_kept
        if files_pruned < 0:  # defensive; subset validation above makes this unreachable
            logger.warning(
                "[estimate.prune] kept-file count exceeded candidates; "
                "reporting zero files pruned"
            )
            files_pruned = 0

        # Validate requested columns
        missing_info = get_missing_columns(self.tables, supers)

        # Total parquet files across all selected snapshots
        total_reflections = sum(len(s.files) for s in supers)

        # A committed zero-resource manifest with a preserved schema is a
        # valid table, not a missing-data error.  Only records that proved the
        # resources field was explicitly an empty list may take this path.
        authoritative_empty_keys = {
            record["key"] for record in records
            if record.get("authoritative_empty")
        }
        all_have_files = all(
            bool(s.files)
            or (s.super_name.lower(), s.simple_name.lower())
            in authoritative_empty_keys
            for s in supers
        )

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
            # Cross-table join pruning observability — only when the query
            # carried join edges, so single-table reads don't emit empty stats.
            if runnable_join_edges:
                self.plan_stats.add_stat({"JOIN_EDGES": len(runnable_join_edges)})
                self.plan_stats.add_stat({"JOIN_FILES_PRUNED": join_files_removed})
                self.plan_stats.add_stat({"JOIN_PRUNE_ITERATIONS": join_iterations})
                self.plan_stats.add_stat({
                    "JOIN_PRUNE_DURATION_MS": round(
                        prune_profiler.timings.get("read.join_prune", 0.0) * 1000, 3
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
