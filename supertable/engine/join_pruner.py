# supertable/engine/join_pruner.py
"""Join-aware (cross-table) file pruning via sideways information passing.

Ordinary read-path pruning (:func:`supertable.processing.prune_files_by_predicates`)
narrows each table by its *own* ``WHERE`` predicates only.  When a query joins
several tables, the files that survive on one table constrain the shared **join
keys**, and therefore which files can possibly match on the *other* tables.  A
``WHERE session.event_time = <day>`` that collapses ``session`` to a single file
also fixes that file's ``session_id`` min/max — which prunes every ``purchase``
file whose ``session_id`` range can't overlap it, whose surviving ``product_id`` /
``category_id`` ranges then prune ``product`` / ``category`` in turn.

This is the file-granularity analogue of **Dynamic File Pruning** / **semi-join
reduction**: a per-table zone-map is propagated across equi-join edges to a
fixpoint.  It is *conservative* — a file is dropped only when its stored
min/max **provably** cannot overlap the partner's surviving ranges (no false
negatives); any uncertainty (no stats, missing/unavailable stat, an
un-comparable lane, NaN bounds) retains the file.  It reuses the exact overlap
primitives the write/read paths already trust.

Soundness gating: ranges flow along every edge in both directions, but a
destination is pruned only when its :class:`JoinEdge` endpoint is flagged
*prunable*.  :meth:`SQLParser.get_join_edges` clears the flag for the preserved
side of outer/anti joins (whose non-matching rows appear in the result) and for
any physical table the query binds more than once (the executor scans ONE
shared file list for all occurrences).

Precision and performance: propagation keeps the exact active row-group
intervals in a deletion-only interval index.  Each destination file remembers
one source-file overlap witness and is reconsidered only if that source file is
later removed.  Thus disjoint bands remain disjoint without rebuilding a union
or rescanning every destination on every cyclic fixpoint wave.  All-NULL row
groups contribute no join keys (equi-joins never match NULL) and are skipped
rather than poisoning the export.

The public entry points:

* :func:`prune_files_across_joins` — the pure kernel: given the join edges, the
  literal ``WHERE`` constraints, and per-table ``{file: ...}`` / stats maps,
  return the surviving files per table plus an explain-style plan.
* :func:`plan_file_pruning_for_query` — parse a SQL string into those inputs and
  run the kernel (the caller still supplies the per-table files + stats, which
  it resolves from its snapshots).
"""

from __future__ import annotations

from bisect import bisect_right
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

import polars

from supertable.data_classes import JoinEdge, PredInterval
from supertable.processing import (
    _SELECT_OTHER_LANE_COL,
    _occurrence_excludes_file,
    _stored_select_lane_values,
)

TableKey = Tuple[str, str]  # (super_name.lower(), simple_name.lower())

# A union export fragmenting past this many disjoint intervals collapses to its
# convex hull — bounds the per-direction pruning cost while staying sound.
MAX_JOIN_INTERVALS = 16


# ---------------------------------------------------------------------------
# Lane handling
# ---------------------------------------------------------------------------

# The SELECT-safe stored lanes map one-to-one onto the PredInterval vocabulary
# consumed by ``_pred_overlaps_stored``.  In particular, doubles and strings
# are deliberately absent (NaN/cast/collation hazards), and temporal kinds stay
# distinct so executor timezone coercions cannot create false negatives.
_STORED_TO_PRED_LANE = {
    "bigint": "numeric",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
}


# ---------------------------------------------------------------------------
# Plan (explain output)
# ---------------------------------------------------------------------------

@dataclass
class JoinPrunePlan:
    """The result of a cross-table pruning run.

    ``survivors`` maps each table key to the files that could still contribute
    rows to the join; a table pruned to ``[]`` means the join provably yields no
    rows from it.  ``files_before`` / ``files_after`` and the human-readable
    ``steps`` (one per pruning action that actually removed files) make the run
    explainable — an execution/prediction plan for the filter propagation.
    ``converged`` is ``False`` when the safety cap stopped propagation before a
    true fixpoint (the result is then merely under-pruned, never unsound).
    """
    survivors: Dict[TableKey, List[str]]
    files_before: Dict[TableKey, int] = field(default_factory=dict)
    files_after: Dict[TableKey, int] = field(default_factory=dict)
    steps: List[str] = field(default_factory=list)
    iterations: int = 0
    converged: bool = True

    def summary(self) -> str:
        lines = ["JoinPrunePlan:"]
        for t in sorted(self.survivors):
            b = self.files_before.get(t, len(self.survivors[t]))
            a = self.files_after.get(t, len(self.survivors[t]))
            lines.append(f"  {t[0]}.{t[1]}: {b} -> {a} file(s)")
        if self.steps:
            if self.converged:
                lines.append(f"  reached fixpoint in {self.iterations} pass(es):")
            else:
                lines.append(
                    f"  stopped at the iteration cap after {self.iterations} "
                    f"pass(es) (sound, possibly under-pruned):"
                )
            lines.extend(f"    - {s}" for s in self.steps)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_StoredRange = Optional[Tuple[str, object, object]]
_RowGroupIndex = Dict[str, Dict[int, Dict[str, _StoredRange]]]
_FileRows = Dict[str, List[Tuple[_StoredRange, bool]]]
_ColumnProfile = Tuple[_RowGroupIndex, _FileRows]


class _ActiveIntervalTree:
    """Deletion-only interval index for one comparable predicate lane.

    Intervals are ordered by their lower endpoint.  Each segment-tree node
    stores the active interval with the greatest upper endpoint, so an overlap
    witness for ``[lo, hi]`` is found by asking whether the prefix whose lower
    endpoints are ``<= hi`` contains an upper endpoint ``>= lo``.  Source files
    only disappear during pruning; deleting every interval owned by a file is
    therefore enough to maintain the index incrementally.

    Returning a concrete interval position (rather than merely ``True``) lets
    each destination file watch the source file that currently justifies
    keeping it.  A destination is reconsidered only when that source file is
    removed.
    """

    def __init__(self, intervals: List[Tuple[object, object, str]]) -> None:
        ordered = sorted(intervals, key=lambda item: item[0])
        self._lows = [lo for lo, _hi, _file in ordered]
        self._highs = [hi for _lo, hi, _file in ordered]
        self._positions_by_file: Dict[str, List[int]] = {}
        for position, (_lo, _hi, file_key) in enumerate(ordered):
            self._positions_by_file.setdefault(file_key, []).append(position)

        size = 1
        while size < len(ordered):
            size *= 2
        self._size = size
        self._maxima: List[Optional[Tuple[object, int]]] = [None] * (2 * size)
        for position, (_low, high, _file) in enumerate(ordered):
            self._maxima[size + position] = (high, position)
        for node in range(size - 1, 0, -1):
            self._maxima[node] = self._greater(
                self._maxima[node * 2], self._maxima[node * 2 + 1]
            )
        self.active_count = len(ordered)

    @staticmethod
    def _greater(
        left: Optional[Tuple[object, int]],
        right: Optional[Tuple[object, int]],
    ) -> Optional[Tuple[object, int]]:
        if left is None:
            return right
        if right is None:
            return left
        return right if right[0] > left[0] else left

    def deactivate_file(self, file_key: str) -> None:
        for position in self._positions_by_file.get(file_key, ()):
            node = self._size + position
            if self._maxima[node] is None:
                continue
            self.active_count -= 1
            self._maxima[node] = None
            node //= 2
            while node:
                self._maxima[node] = self._greater(
                    self._maxima[node * 2], self._maxima[node * 2 + 1]
                )
                node //= 2

    def positions_for_file(self, file_key: str) -> Iterable[int]:
        return self._positions_by_file.get(file_key, ())

    def _prefix_maximum(
        self, prefix_end: int
    ) -> Optional[Tuple[object, int]]:
        """Greatest active upper endpoint among positions below prefix_end."""
        if prefix_end <= 0:
            return None
        left = self._size
        right = self._size + prefix_end
        best: Optional[Tuple[object, int]] = None
        while left < right:
            if left & 1:
                best = self._greater(best, self._maxima[left])
                left += 1
            if right & 1:
                right -= 1
                best = self._greater(best, self._maxima[right])
            left //= 2
            right //= 2
        return best

    def find_overlap(self, lo: object, hi: object) -> Optional[int]:
        """Return an active interval overlapping closed ``[lo, hi]``."""
        prefix_end = bisect_right(self._lows, hi)
        best = self._prefix_maximum(prefix_end)
        if best is not None and best[0] >= lo:
            return best[1]
        return None

    def find_container_of(self, position: int) -> Optional[int]:
        """Return an active interval containing a removed interval, if any.

        Every destination interval supported by the removed interval is also
        supported by a containing interval.  Its entire watcher set can thus
        be transferred without inspecting those destination files again.
        """
        lower = self._lows[position]
        upper = self._highs[position]
        prefix_end = bisect_right(self._lows, lower)
        best = self._prefix_maximum(prefix_end)
        if best is not None and best[0] >= upper:
            return best[1]
        return None


class _ActiveSourceColumn:
    """Incremental, conservative digest of one source join column."""

    def __init__(
        self,
        file_rows: _FileRows,
        files: Iterable[str],
        allowed_lanes: Optional[Set[str]] = None,
    ) -> None:
        self.active_files = set(files)
        self.blockers: Set[str] = set()
        by_lane: Dict[str, List[Tuple[object, object, str]]] = {}

        for file_key in self.active_files:
            rows = file_rows.get(file_key)
            if not rows:
                self.blockers.add(file_key)
                continue
            file_spans: List[Tuple[str, object, object]] = []
            unusable = False
            for stored, all_null in rows:
                if stored is None:
                    if all_null:
                        continue
                    unusable = True
                    break
                stored_lane, lower, upper = stored
                lane = _STORED_TO_PRED_LANE.get(stored_lane)
                if lane is None or (
                    allowed_lanes is not None and lane not in allowed_lanes
                ):
                    unusable = True
                    break
                file_spans.append((lane, lower, upper))
            # A malformed file must not partly constrain a partner.  Likewise,
            # mixed lanes within one physical column are uncertainty, not two
            # independently trustworthy value domains.
            if unusable or len({span[0] for span in file_spans}) > 1:
                self.blockers.add(file_key)
                continue
            for lane, lower, upper in file_spans:
                by_lane.setdefault(lane, []).append((lower, upper, file_key))

        self.trees: Dict[str, _ActiveIntervalTree] = {}
        try:
            self.trees = {
                lane: _ActiveIntervalTree(intervals)
                for lane, intervals in by_lane.items()
            }
        except Exception:
            # Comparability is a prerequisite for every disjointness proof.
            # Poisoning the column is conservative and should only be reachable
            # for a caller-supplied malformed stats frame.
            self.blockers.update(self.active_files)
            self.trees = {}

    def deactivate(self, file_keys: Iterable[str]) -> None:
        for file_key in file_keys:
            if file_key not in self.active_files:
                continue
            self.active_files.remove(file_key)
            self.blockers.discard(file_key)
            try:
                for tree in self.trees.values():
                    tree.deactivate_file(file_key)
            except Exception:
                # A custom/corrupt scalar may compare during one tree shape but
                # fail after deletion changes the maxima.  Propagation must
                # degrade to unknown, never break the SELECT.
                self.blockers.update(self.active_files)
                self.trees = {}
                return

    def state(self) -> Tuple[str, Optional[str], Optional[_ActiveIntervalTree]]:
        """Return ``empty`` / ``unknown`` / ``known`` plus lane and tree."""
        if not self.active_files:
            return "empty", None, None
        if self.blockers:
            return "unknown", None, None
        active = [
            (lane, tree) for lane, tree in self.trees.items()
            if tree.active_count
        ]
        if len(active) != 1:
            # No intervals means every remaining group was provably all NULL.
            # The established kernel contract deliberately treats that case as
            # unknown instead of using null counts to empty the other table.
            return "unknown", None, None
        lane, tree = active[0]
        return "known", lane, tree


class _DestinationColumn:
    """Lazy per-file destination ranges; ``None`` means retain permanently."""

    def __init__(self, rg_index: _RowGroupIndex, column: str) -> None:
        self._rg_index = rg_index
        self._column = column
        self._cache: Dict[
            str, Optional[List[Tuple[str, object, object]]]
        ] = {}

    def ranges(
        self, file_key: str
    ) -> Optional[List[Tuple[str, object, object]]]:
        if file_key in self._cache:
            return self._cache[file_key]
        row_groups = self._rg_index.get(file_key)
        if not row_groups:
            self._cache[file_key] = None
            return None
        ranges: List[Tuple[str, object, object]] = []
        for columns in row_groups.values():
            stored = columns.get(self._column)
            if stored is None:
                self._cache[file_key] = None
                return None
            stored_lane, lower, upper = stored
            lane = _STORED_TO_PRED_LANE.get(stored_lane)
            if lane is None:
                self._cache[file_key] = None
                return None
            ranges.append((lane, lower, upper))
        if not ranges:
            self._cache[file_key] = None
            return None
        self._cache[file_key] = ranges
        return ranges


_PERMANENT_SUPPORT = object()


@dataclass
class _DirectionSupports:
    """Support witnesses retained between dirty-queue waves."""

    source_version: int = -1
    lane: Optional[str] = None
    initialized: bool = False
    watched_by_position: Dict[int, Set[str]] = field(default_factory=dict)

    def clear(self) -> None:
        self.watched_by_position.clear()
        self.initialized = False
        self.lane = None


def _table_profiles(
    stats_df: Optional[polars.DataFrame],
    cols_lower: Iterable[str],
    *,
    file_keys: Optional[Iterable[str]] = None,
) -> Dict[str, _ColumnProfile]:
    """Build every requested join-column profile in one stats-frame pass.

    A table may participate in many joins on different keys.  Filtering and
    materialising its stats frame once per key is needlessly quadratic in the
    number of join columns, so this routine projects the narrow hot-path schema
    once and dispatches each row into its column profile while iterating.

    The returned per-column pair serves both roles used by the fixpoint:

    * ``row_group_index`` lets a partner range prune this table;
    * ``file_rows`` lets this table derive a range to export to partners.

    Missing frames/columns yield empty profiles.  Every stats uncertainty is
    represented as ``stored=None`` and therefore keeps files conservatively.
    """
    columns = set(cols_lower)
    profiles: Dict[str, _ColumnProfile] = {
        col: ({}, {}) for col in columns
    }
    if (
        stats_df is None
        or stats_df.height == 0
        or not columns
        or not {"file_path", "row_group_id", "column_name"}.issubset(
            stats_df.columns
        )
    ):
        return profiles

    # Project before materialising Python rows.  ``logical_type`` is required
    # by _stored_select_lane to distinguish safe temporal lanes.  Double and
    # string bounds are intentionally omitted: SELECT pruning rejects those
    # ordering domains, and an absent typed lane becomes the same conservative
    # ``stored=None`` without copying unused values into Python.
    needed = (
        "file_path", "row_group_id", "stats_available",
        "physical_type", "logical_type", "min_bigint", "max_bigint",
        "min_timestamp", "max_timestamp",
        "null_count", "row_group_rows",
    )
    expressions = [
        polars.col("column_name").str.to_lowercase().alias("__cn")
    ]
    expressions.extend(
        (
            polars.col(name)
            if name in stats_df.columns
            else polars.lit(None).alias(name)
        )
        for name in needed
    )
    rejected_lane_columns = [
        name for name in (
            "min_double", "max_double", "min_string", "max_string",
        )
        if name in stats_df.columns
    ]
    expressions.append(
        (
            polars.any_horizontal(
                [polars.col(name).is_not_null()
                 for name in rejected_lane_columns]
            )
            if rejected_lane_columns else polars.lit(False)
        ).alias(_SELECT_OTHER_LANE_COL)
    )
    # Filter before materialising the wide Python-facing projection.  A stats
    # artifact has one row per (row group x table column), while a join usually
    # touches only one or two columns; projecting every row first needlessly
    # copies all of the typed lane columns.
    predicate = polars.col("column_name").str.to_lowercase().is_in(
        list(columns)
    )
    if file_keys is not None:
        survivor_keys = list(file_keys)
        if not survivor_keys:
            return profiles
        predicate &= polars.col("file_path").is_in(survivor_keys)
    sub = stats_df.filter(predicate).select(expressions)
    # Position of each physical (file,row-group) slot in file_rows.  A duplicate
    # stats row poisons the slot to unknown instead of being unioned or letting
    # row order pick a bound.  Valid artifacts have exactly one row per slot.
    file_row_slots: Dict[str, Dict[Tuple[str, int], int]] = {
        col: {} for col in columns
    }
    for (
        col, fp, rg, stats_available, physical_type, logical_type,
        min_bigint, max_bigint, min_timestamp, max_timestamp,
        null_count, row_count, other_lane_populated,
    ) in sub.iter_rows():
        # A NULL column_name cannot pass is_in(), but retain this guard for
        # malformed/custom stats frames rather than failing a SELECT.
        if col not in profiles:
            continue
        stored = _stored_select_lane_values(
            stats_available,
            physical_type,
            logical_type,
            min_bigint,
            max_bigint,
            min_timestamp,
            max_timestamp,
            other_lane_populated,
        )
        rg_index, file_rows = profiles[col]
        rg_cols = rg_index.setdefault(fp, {}).setdefault(rg, {})
        if col in rg_cols:
            # Two footer names can collide after SQL's case-insensitive
            # normalization (for example externally produced ``ID`` + ``id``).
            # Never let row order select one physical column's range.
            rg_cols[col] = None
        else:
            rg_cols[col] = stored
        # Only an exact, non-negative footer cardinality is proof that this
        # row group contains no join keys.  Treating an impossible corrupt
        # value such as ``null_count > row_group_rows`` as "all NULL" would
        # silently erase an unknown range from the source export and could
        # prune a genuinely matching partner file.
        all_null = (
            isinstance(row_count, int)
            and not isinstance(row_count, bool)
            and row_count >= 0
            and isinstance(null_count, int)
            and not isinstance(null_count, bool)
            and null_count == row_count
        )
        rows_for_file = file_rows.setdefault(fp, [])
        slot = (fp, rg)
        previous_position = file_row_slots[col].get(slot)
        if previous_position is not None:
            rows_for_file[previous_position] = (None, False)
        else:
            file_row_slots[col][slot] = len(rows_for_file)
            rows_for_file.append((stored, all_null))
    return profiles


def _build_rg_index(
    stats_df: Optional[polars.DataFrame],
    cols_lower: List[str],
) -> _RowGroupIndex:
    """``file_path -> row_group_id -> column(lower) -> (lane,min,max)|None``.

    Column names are matched **case-insensitively** (the stored footer name may
    differ in case from the SQL identifier).  Only the requested columns are
    indexed.
    """
    index: _RowGroupIndex = {}
    columns = set(cols_lower)
    if (
        stats_df is None
        or stats_df.height == 0
        or not columns
        or not {"file_path", "row_group_id", "column_name"}.issubset(
            stats_df.columns
        )
    ):
        return index
    needed = (
        "file_path", "row_group_id", "stats_available",
        "physical_type", "logical_type",
        "min_bigint", "max_bigint", "min_timestamp", "max_timestamp",
    )
    expressions = [
        polars.col("column_name").str.to_lowercase().alias("__cn")
    ]
    expressions.extend(
        (
            polars.col(name)
            if name in stats_df.columns
            else polars.lit(None).alias(name)
        )
        for name in needed
    )
    rejected_lane_columns = [
        name for name in (
            "min_double", "max_double", "min_string", "max_string",
        )
        if name in stats_df.columns
    ]
    expressions.append(
        (
            polars.any_horizontal(
                [polars.col(name).is_not_null()
                 for name in rejected_lane_columns]
            )
            if rejected_lane_columns else polars.lit(False)
        ).alias(_SELECT_OTHER_LANE_COL)
    )
    sub = (
        stats_df.filter(
            polars.col("column_name").str.to_lowercase().is_in(list(columns))
        )
        .select(expressions)
    )
    for (
        col, fp, rg, stats_available, physical_type, logical_type,
        min_bigint, max_bigint, min_timestamp, max_timestamp,
        other_lane_populated,
    ) in sub.iter_rows():
        if col not in columns:
            continue
        rg_cols = index.setdefault(fp, {}).setdefault(rg, {})
        if col in rg_cols:
            rg_cols[col] = None
        else:
            rg_cols[col] = _stored_select_lane_values(
                stats_available,
                physical_type,
                logical_type,
                min_bigint,
                max_bigint,
                min_timestamp,
                max_timestamp,
                other_lane_populated,
            )
    return index


def _column_profile(
    stats_df: Optional[polars.DataFrame],
    col_lower: str,
) -> _ColumnProfile:
    """Single-pass, single-column stats digest serving both pruning roles.

    Returns ``(rg_index, file_rows)``:

    * ``rg_index`` — the :func:`_build_rg_index` shape for this one column,
      consumed by :func:`_prune_by_occurrences` when the table is a pruning
      *destination*;
    * ``file_rows`` — ``file -> [(stored|None, all_null), ...]`` (one entry per
      row group), consumed by :func:`_derive_join_intervals` when the table is
      a range *source*.  ``all_null`` marks a row group whose every value is
      NULL (``null_count >= row_group_rows``): it holds no join keys.
    """
    return _table_profiles(stats_df, [col_lower])[col_lower]


def _prune_by_occurrences(
    files: List[str],
    stats_df: Optional[polars.DataFrame],
    occurrences: List[Dict[str, PredInterval]],
    *,
    allow_empty: bool,
    index: Optional[Dict] = None,
) -> List[str]:
    """Return the files not excluded by *every* occurrence (union semantics).

    Mirrors :func:`prune_files_by_predicates` — a file is dropped only when it is
    excluded by *all* occurrences — but (a) matches column names
    case-insensitively and (b) may return ``[]`` when *allow_empty* (a join
    partner that provably shares no key is a legitimate empty result, unlike the
    estimator's "never empty a table" optimisation guard).  Occurrence keys must
    already be lowercased.  *index* short-circuits the stats scan with a
    pre-built :func:`_build_rg_index` result (the kernel builds every requested
    join-column profile together so the fixpoint never re-reads a stats frame).
    """
    if not occurrences or any(not occ for occ in occurrences):
        return list(files)
    if index is None:
        cols = sorted({c for occ in occurrences for c in occ})
        index = _build_rg_index(stats_df, cols)
    if not index:
        return list(files)  # no stats for any constrained column → retain all
    kept: List[str] = []
    for fk in files:
        rgs = index.get(fk)
        if not rgs:
            kept.append(fk)  # no stats for this file → cannot prove absence
            continue
        if all(_occurrence_excludes_file(occ, rgs) for occ in occurrences):
            continue  # excluded by every occurrence → drop
        kept.append(fk)
    if not kept and not allow_empty:
        return list(files)
    return kept


def _is_nan(v: object) -> bool:
    return isinstance(v, float) and v != v


def _derive_join_intervals(
    file_rows: _FileRows,
    files: List[str],
) -> Optional[List[PredInterval]]:
    """The union of inclusive ``[min, max]`` intervals a column spans over
    *files*, as merged ``PredInterval``s sorted ascending.

    This is the "sideways" fact a table exports to its join partners: the set
    of ranges its surviving files' join keys can occupy.  Keeping the bands
    separate (instead of one convex hull) lets a partner file that falls
    *between* two disjoint survivor bands be pruned.  Past
    :data:`MAX_JOIN_INTERVALS` fragments the union collapses to its hull —
    still sound, just coarser.

    Returns ``None`` — meaning *unknown/unbounded*, so the partner cannot be
    pruned on this edge — whenever the union can't be proven.  Returns ``[]``
    for a genuinely empty survivor set; callers may propagate that fact only
    when their ``allow_empty`` contract permits it.

    Uncertainty keeps propagation sound:

      * a surviving file absent from the stats;
      * **any** surviving row group with no usable stat for the column
        (``stats_available`` False, unsupported lane, NaN bounds): it could
        hold any value — EXCEPT an all-NULL row group, which holds no join
        keys at all (equi-joins never match NULL) and is skipped only when its
        non-negative ``null_count == row_group_rows`` exactly;
      * the surviving files disagree on lane (should not happen for one real
        column, but if it does the union is un-representable);
      * every row group was all-NULL (an empty export could prune the partner
        to nothing; asserting that from ``null_count`` alone is not worth the
        risk — stay unbounded).
    """
    if not files:
        return []
    pred_lane: Optional[str] = None
    # Merge only after collecting every span.  Collapsing to the hull as soon
    # as an *intermediate* insertion order exceeds MAX_JOIN_INTERVALS is sound,
    # but unnecessarily imprecise: a later wide span may bridge those fragments
    # so the final exact union is back under the cap.  Footer row order must not
    # decide how many data files the reader scans.
    spans: List[Tuple[object, object]] = []
    for fk in files:
        rows = file_rows.get(fk)
        if not rows:
            return None  # a surviving file with no stat for this column → unbounded
        for stored, all_null in rows:
            if stored is None:
                if all_null:
                    continue  # no join keys in this row group
                return None  # unknown row group → unbounded
            s_lane, s_min, s_max = stored
            lane = _STORED_TO_PRED_LANE.get(s_lane)
            if lane is None:
                return None
            if pred_lane is None:
                pred_lane = lane
            elif pred_lane != lane:
                return None  # mixed lanes on one column → give up
            if _is_nan(s_min) or _is_nan(s_max):
                return None  # NaN bounds are un-orderable → unbounded
            try:
                if s_min > s_max:
                    return None  # malformed footer range → retain everything
            except (TypeError, ValueError, OverflowError):
                # A malformed/mixed stats artifact that cannot be ordered is
                # unknown, never proof that a partner file is absent.
                return None
            spans.append((s_min, s_max))
    if not spans or pred_lane is None:
        return None
    try:
        spans.sort(key=lambda span: span[0])
        merged: List[List[object]] = [list(spans[0])]
        for lo, hi in spans[1:]:
            if lo <= merged[-1][1]:
                if hi > merged[-1][1]:
                    merged[-1][1] = hi
            else:
                merged.append([lo, hi])
    except (TypeError, ValueError, OverflowError):
        return None
    if len(merged) > MAX_JOIN_INTERVALS:
        merged = [[merged[0][0], merged[-1][1]]]
    return [PredInterval(pred_lane, lo, True, hi, True) for lo, hi in merged]


def _derive_join_range(
    stats_df: Optional[polars.DataFrame],
    files: List[str],
    column_lower: str,
) -> Optional[PredInterval]:
    """The single inclusive ``[min, max]`` hull of *column* over *files*.

    Convenience wrapper over :func:`_derive_join_intervals` (which the kernel
    uses directly for its tighter union-of-bands export); ``None`` means
    unbounded.
    """
    if stats_df is None or stats_df.height == 0:
        return None
    _rg_index, file_rows = _column_profile(stats_df, column_lower)
    intervals = _derive_join_intervals(file_rows, files)
    if not intervals:
        return None
    return PredInterval(
        intervals[0].lane, intervals[0].lo, True, intervals[-1].hi, True
    )


def _fmt_intervals(preds: List[PredInterval]) -> str:
    if len(preds) <= 3:
        return " u ".join(f"[{p.lo}, {p.hi}]" for p in preds)
    return f"{len(preds)} bands [{preds[0].lo} .. {preds[-1].hi}]"


# ---------------------------------------------------------------------------
# Public kernel
# ---------------------------------------------------------------------------

def prune_files_across_joins(
    join_edges: List[JoinEdge],
    literal_constraints: Optional[Dict[TableKey, List[Dict[str, PredInterval]]]],
    table_files: Dict[TableKey, List[str]],
    table_stats: Dict[TableKey, Optional[polars.DataFrame]],
    *,
    allow_empty: bool = True,
    max_iterations: Optional[int] = None,
    allowed_lanes: Optional[Set[str]] = None,
) -> JoinPrunePlan:
    """Cross-table file pruning via semi-join reduction to a fixpoint.

    Parameters
    ----------
    join_edges:
        Equi-join links from :meth:`SQLParser.get_join_edges`.  Each endpoint's
        ``prune_left`` / ``prune_right`` flag gates whether that side may be
        pruned (outer-join preserved sides and multi-occurrence tables must
        not be).
    literal_constraints:
        The per-table ``WHERE`` constraints from
        :meth:`SQLParser.get_predicate_constraints` (keyed the same way, column
        keys lowercased).  May be ``None``/empty — then only join propagation
        runs.
    table_files:
        ``table_key -> [file_key, ...]`` — the candidate files per table (the raw
        storage keys that match the stats ``file_path`` column).
    table_stats:
        ``table_key -> stats DataFrame`` (``STATS_SCHEMA`` shape) or ``None``.
    allow_empty:
        When ``True`` (default) a table with no surviving file stays ``[]`` — the
        correct answer for a join that shares no key.  ``False`` restores the
        estimator's "never empty a table" guard.
    max_iterations:
        Optional diagnostic cap on propagation waves.  By default the dirty
        work queue drains completely and therefore reaches a true fixpoint.
        A caller-supplied cap may stop early with ``converged=False``; the
        result is then merely under-pruned, never unsound.
    allowed_lanes:
        Optional predicate-lane whitelist for the eventual executor.  ``None``
        enables every intrinsically safe stored lane.  Callers that may route
        to an executor with different temporal coercion semantics can restrict
        propagation (for example, to ``{"numeric"}``).  A disabled lane is
        treated as unknown, so this option can only retain extra files.

    Returns
    -------
    JoinPrunePlan
        ``survivors`` (per-table file lists) plus an explainable trace.
    """
    survivors: Dict[TableKey, List[str]] = {
        t: list(fs) for t, fs in table_files.items()
    }
    files_before = {t: len(fs) for t, fs in survivors.items()}
    steps: List[str] = []

    # Phase 1 — literal WHERE pruning per table (independent, as today).
    for t, occ_list in (literal_constraints or {}).items():
        if t not in survivors:
            continue
        new = _prune_by_occurrences(
            survivors[t], table_stats.get(t), occ_list, allow_empty=allow_empty
        )
        if len(new) != len(survivors[t]):
            steps.append(
                f"where {t[0]}.{t[1]}: {len(survivors[t])} -> {len(new)} file(s)"
            )
            survivors[t] = new

    # Phase 2 — propagate join-key ranges across edges until a fixpoint.
    usable_edges = [
        e for e in join_edges
        if e.left_table in survivors and e.right_table in survivors
        and (e.prune_left or e.prune_right)
    ]
    # Materialise the safe propagation directions once and deduplicate them.
    # The parser normally deduplicates edges, but the kernel is public and
    # should not pay repeatedly for equivalent caller-provided edges.
    Direction = Tuple[TableKey, str, TableKey, str]
    directions: List[Direction] = []
    seen_directions: Set[Direction] = set()
    for edge in usable_edges:
        candidates = (
            (edge.left_table, edge.left_col,
             edge.right_table, edge.right_col, edge.prune_right),
            (edge.right_table, edge.right_col,
             edge.left_table, edge.left_col, edge.prune_left),
        )
        for src, src_col, dst, dst_col, dst_prunable in candidates:
            direction = (src, src_col, dst, dst_col)
            if dst_prunable and direction not in seen_directions:
                seen_directions.add(direction)
                directions.append(direction)

    # Build all requested profiles for a table together on first use.  This
    # keeps lazy behavior for irrelevant/unreached tables while ensuring each
    # stats frame is filtered and materialised at most once.
    requested_columns: Dict[TableKey, Set[str]] = {}
    for src, src_col, dst, dst_col in directions:
        requested_columns.setdefault(src, set()).add(src_col)
        requested_columns.setdefault(dst, set()).add(dst_col)
    table_profile_cache: Dict[TableKey, Dict[str, _ColumnProfile]] = {}

    def get_profile(t: TableKey, col: str) -> _ColumnProfile:
        if t not in table_profile_cache:
            table_profile_cache[t] = _table_profiles(
                table_stats.get(t),
                requested_columns.get(t, {col}),
                # Profiles are immutable for the run, so only restrict them
                # when this table has already shrunk.  Survivor sets never
                # grow; no omitted file can be needed by a later wave.
                file_keys=(
                    survivors[t]
                    if len(survivors[t]) < files_before[t]
                    else None
                ),
            )
        return table_profile_cache[t].get(col, ({}, {}))

    # A direction only needs to run initially and whenever its source table's
    # survivors shrink.  Queue membership suppresses redundant work: if the
    # source changes while its direction is already pending, that future run
    # observes the newest survivor version.
    outgoing: Dict[TableKey, List[int]] = {}
    for direction_index, (src, _src_col, _dst, _dst_col) in enumerate(directions):
        outgoing.setdefault(src, []).append(direction_index)
    pending: Deque[int] = deque(range(len(directions)))
    queued: Set[int] = set(range(len(directions)))

    version: Dict[TableKey, int] = {t: 0 for t in survivors}
    survivor_sets: Dict[TableKey, Set[str]] = {
        t: set(files) for t, files in survivors.items()
    }
    # Every version transition records exactly the files removed by it.  A
    # direction consumes each source removal once, which is what turns the
    # adversarial one-file-per-wave cycle from repeated O(F) scans into an
    # amortised O(F log F) sequence of witness repairs.
    removal_history: Dict[TableKey, List[Set[str]]] = {
        t: [] for t in survivors
    }

    source_indexes: Dict[Tuple[TableKey, str], _ActiveSourceColumn] = {}
    source_index_versions: Dict[Tuple[TableKey, str], int] = {}
    destination_indexes: Dict[
        Tuple[TableKey, str], _DestinationColumn
    ] = {}

    def get_source_index(t: TableKey, col: str) -> _ActiveSourceColumn:
        key = (t, col)
        source_index = source_indexes.get(key)
        if source_index is None:
            _rg_index, file_rows = get_profile(t, col)
            source_index = _ActiveSourceColumn(
                file_rows, survivors[t], allowed_lanes=allowed_lanes,
            )
            source_indexes[key] = source_index
            source_index_versions[key] = version[t]
            return source_index
        synced_version = source_index_versions[key]
        if synced_version < version[t]:
            for removed in removal_history[t][synced_version:version[t]]:
                source_index.deactivate(removed)
            source_index_versions[key] = version[t]
        return source_index

    def get_destination_index(t: TableKey, col: str) -> _DestinationColumn:
        key = (t, col)
        destination_index = destination_indexes.get(key)
        if destination_index is None:
            rg_index, _file_rows = get_profile(t, col)
            destination_index = _DestinationColumn(rg_index, col)
            destination_indexes[key] = destination_index
        return destination_index

    def find_support(
        source_lane: str,
        source_tree: _ActiveIntervalTree,
        destination: _DestinationColumn,
        file_key: str,
    ) -> object:
        ranges = destination.ranges(file_key)
        if ranges is None:
            # A missing/unavailable destination row group can contain any key.
            return _PERMANENT_SUPPORT
        for destination_lane, lower, upper in ranges:
            if destination_lane != source_lane:
                # Incomparable executor domains never prove disjointness.  One
                # such row group is enough to retain the whole file.
                return _PERMANENT_SUPPORT
            try:
                position = source_tree.find_overlap(lower, upper)
            except Exception:
                return _PERMANENT_SUPPORT
            if position is not None:
                return position
        return None

    direction_supports = {
        index: _DirectionSupports() for index in range(len(directions))
    }

    iterations = 0
    while pending and (
        max_iterations is None or iterations < max_iterations
    ):
        iterations += 1
        # Work queued by this wave is deferred to the next wave, preserving
        # ``max_iterations``' historical pass-like diagnostic semantics.
        wave_size = len(pending)
        for _ in range(wave_size):
            direction_index = pending.popleft()
            queued.remove(direction_index)
            src, src_col, dst, dst_col = directions[direction_index]
            supports = direction_supports[direction_index]
            source_index = get_source_index(src, src_col)
            source_state, source_lane, source_tree = source_index.state()

            if source_state == "empty":
                supports.source_version = version[src]
                if not allow_empty or not survivors[dst]:
                    continue
                new: List[str] = []
                rendered_ranges = "empty join-key set"
            elif source_state != "known":
                # Removing source files can turn an unknown export into a known
                # one (for example, after the only stats-missing file leaves).
                # Remember the version and let the next dirty wave retry.
                supports.clear()
                supports.source_version = version[src]
                continue
            else:
                assert source_lane is not None and source_tree is not None
                destination = get_destination_index(dst, dst_col)
                full_initialization = (
                    not supports.initialized or supports.lane != source_lane
                )
                if full_initialization:
                    supports.clear()
                    supports.initialized = True
                    supports.lane = source_lane
                    candidates = list(survivors[dst])
                else:
                    invalidated: Set[str] = set()
                    start_version = max(supports.source_version, 0)
                    for removed in removal_history[src][
                        start_version:version[src]
                    ]:
                        for source_file in removed:
                            for position in source_tree.positions_for_file(
                                source_file
                            ):
                                watchers = supports.watched_by_position.pop(
                                    position, None
                                )
                                if not watchers:
                                    continue
                                try:
                                    replacement = source_tree.find_container_of(
                                        position
                                    )
                                except Exception:
                                    replacement = None
                                if replacement is None:
                                    invalidated.update(watchers)
                                else:
                                    supports.watched_by_position.setdefault(
                                        replacement, set()
                                    ).update(watchers)
                    candidates = [
                        file_key for file_key in invalidated
                        if file_key in survivor_sets[dst]
                    ]

                supports.source_version = version[src]
                if not candidates:
                    continue

                unsupported: Set[str] = set()
                for file_key in candidates:
                    witness = find_support(
                        source_lane, source_tree, destination, file_key
                    )
                    if witness is None:
                        unsupported.add(file_key)
                        continue
                    if isinstance(witness, int):
                        supports.watched_by_position.setdefault(
                            witness, set()
                        ).add(file_key)

                if not unsupported:
                    continue
                new = [
                    file_key for file_key in survivors[dst]
                    if file_key not in unsupported
                ]
                if not new and not allow_empty:
                    # Match _prune_by_occurrences' estimator guard exactly: a
                    # proof that would empty the current destination is rolled
                    # back as one batch.  Future source deletions cannot create
                    # a support, so these guarded files need no repeated scan.
                    continue
                rendered_ranges = (
                    f"{source_tree.active_count} active row-group interval(s)"
                )

            if len(new) >= len(survivors[dst]):
                continue
            old_survivors = survivors[dst]
            steps.append(
                f"join {src[0]}.{src[1]}.{src_col} -> "
                f"{dst[0]}.{dst[1]}.{dst_col} {rendered_ranges}: "
                f"{len(old_survivors)} -> {len(new)} file(s)"
            )
            survivors[dst] = new
            new_set = set(new)
            removed_files = survivor_sets[dst] - new_set
            survivor_sets[dst] = new_set
            removal_history[dst].append(removed_files)
            version[dst] += 1
            for dirty_index in outgoing.get(dst, []):
                if dirty_index not in queued:
                    pending.append(dirty_index)
                    queued.add(dirty_index)

    files_after = {t: len(fs) for t, fs in survivors.items()}
    return JoinPrunePlan(
        survivors=survivors,
        files_before=files_before,
        files_after=files_after,
        steps=steps,
        iterations=iterations,
        converged=not pending,
    )


# ---------------------------------------------------------------------------
# Query-driven wrapper
# ---------------------------------------------------------------------------

def plan_file_pruning_for_query(
    super_name: str,
    query: str,
    dialect: str,
    table_files: Dict[TableKey, List[str]],
    table_stats: Dict[TableKey, Optional[polars.DataFrame]],
    *,
    allow_empty: bool = True,
) -> JoinPrunePlan:
    """Parse *query* and run :func:`prune_files_across_joins` on it.

    The caller supplies *table_files* / *table_stats* (resolved from its
    snapshots); this function only needs the query to extract the join edges and
    literal ``WHERE`` constraints.  Table keys are
    ``(super_name.lower(), simple_name.lower())``, matching what the parser emits
    and what the estimator uses.  Constraint column keys are lowercased here so
    the kernel's case-insensitive matching applies to them too.
    """
    # Imported here to avoid a package-level import cycle (utils <- engine).
    from supertable.utils.sql_parser import SQLParser

    parser = SQLParser(super_name=super_name, query=query, dialect=dialect)
    join_edges = parser.get_join_edges()
    literal_constraints = {
        t: [{c.lower(): p for c, p in occ.items()} for occ in occs]
        for t, occs in parser.get_predicate_constraints().items()
    }
    # This public convenience wrapper knows the eventual SQL dialect but does
    # not pass through DataReader's engine gate.  Spark reads Parquet NTZ
    # timestamps as LTZ in supertable and may rebase legacy temporal values;
    # raw footer ordering is therefore not absence proof for temporal joins.
    # Integral ranges remain exact across the supported executors.  Treating
    # every other lane as unknown can only retain additional files.
    wrapper_allowed_lanes = (
        {"numeric"} if str(dialect).lower().startswith("spark") else None
    )
    return prune_files_across_joins(
        join_edges,
        literal_constraints,
        table_files,
        table_stats,
        allow_empty=allow_empty,
        allowed_lanes=wrapper_allowed_lanes,
    )
