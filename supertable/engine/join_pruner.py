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

Precision: a table's export is a **union of merged [min,max] intervals** (one
per surviving row group, overlaps coalesced, capped at
:data:`MAX_JOIN_INTERVALS` then collapsed to the convex hull), so disjoint
surviving key bands don't spuriously retain partner files that fall between
the bands.  All-NULL row groups contribute no join keys (equi-joins never
match NULL) and are skipped rather than poisoning the export.

The public entry points:

* :func:`prune_files_across_joins` — the pure kernel: given the join edges, the
  literal ``WHERE`` constraints, and per-table ``{file: ...}`` / stats maps,
  return the surviving files per table plus an explain-style plan.
* :func:`plan_file_pruning_for_query` — parse a SQL string into those inputs and
  run the kernel (the caller still supplies the per-table files + stats, which
  it resolves from its snapshots).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import polars

from supertable.data_classes import JoinEdge, PredInterval
from supertable.processing import _occurrence_excludes_file, _stored_lane

TableKey = Tuple[str, str]  # (super_name.lower(), simple_name.lower())

# A union export fragmenting past this many disjoint intervals collapses to its
# convex hull — bounds the per-direction pruning cost while staying sound.
MAX_JOIN_INTERVALS = 16


# ---------------------------------------------------------------------------
# Lane handling
# ---------------------------------------------------------------------------

# A stored stats lane ("bigint"/"double"/"timestamp"/"string") maps onto the
# PredInterval lane vocabulary ("numeric"/"timestamp"/"string") that
# ``_pred_overlaps_stored`` compares against.  bigint and double both unify to
# "numeric" so an integer key can be compared against a double range and back.
_STORED_TO_PRED_LANE = {
    "bigint": "numeric",
    "double": "numeric",
    "timestamp": "timestamp",
    "string": "string",
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

def _build_rg_index(
    stats_df: Optional[polars.DataFrame],
    cols_lower: List[str],
) -> Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]]:
    """``file_path -> row_group_id -> column(lower) -> (lane,min,max)|None``.

    Column names are matched **case-insensitively** (the stored footer name may
    differ in case from the SQL identifier).  Only the requested columns are
    indexed.
    """
    index: Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]] = {}
    if stats_df is None or stats_df.height == 0 or not cols_lower:
        return index
    sub = (
        stats_df.with_columns(
            polars.col("column_name").str.to_lowercase().alias("__cn")
        )
        .filter(polars.col("__cn").is_in(list(cols_lower)))
    )
    for row in sub.iter_rows(named=True):
        fp = row["file_path"]
        rg = row["row_group_id"]
        cn = row["__cn"]
        index.setdefault(fp, {}).setdefault(rg, {})[cn] = _stored_lane(row)
    return index


def _column_profile(
    stats_df: Optional[polars.DataFrame],
    col_lower: str,
) -> Tuple[
    Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]],
    Dict[str, List[Tuple[Optional[Tuple[str, object, object]], bool]]],
]:
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
    rg_index: Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]] = {}
    file_rows: Dict[str, List[Tuple[Optional[Tuple[str, object, object]], bool]]] = {}
    if stats_df is None or stats_df.height == 0:
        return rg_index, file_rows
    # Project to the columns _stored_lane and the null census need before
    # materialising row dicts — the stats schema is wide and this loop is the
    # kernel's hot spot.
    needed = [
        "file_path", "row_group_id", "stats_available",
        "min_bigint", "max_bigint", "min_double", "max_double",
        "min_timestamp", "max_timestamp", "min_string", "max_string",
        "null_count", "row_group_rows",
    ]
    sub = stats_df.filter(
        polars.col("column_name").str.to_lowercase() == col_lower
    ).select([c for c in needed if c in stats_df.columns])
    for row in sub.iter_rows(named=True):
        fp = row["file_path"]
        rg = row["row_group_id"]
        stored = _stored_lane(row)
        rg_index.setdefault(fp, {}).setdefault(rg, {})[col_lower] = stored
        rgr = row.get("row_group_rows")
        nc = row.get("null_count")
        all_null = rgr is not None and nc is not None and nc >= rgr
        file_rows.setdefault(fp, []).append((stored, all_null))
    return rg_index, file_rows


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
    pre-built :func:`_build_rg_index` result (the kernel memoises one per
    (table, column) so the fixpoint loop never re-reads the stats frame).
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
    file_rows: Dict[str, List[Tuple[Optional[Tuple[str, object, object]], bool]]],
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

    Returns ``None`` — meaning *unbounded*, so the partner cannot be pruned on
    this edge — whenever the union can't be proven, which keeps propagation
    sound:

      * no files, or a surviving file absent from the stats;
      * **any** surviving row group with no usable stat for the column
        (``stats_available`` False, unsupported lane, NaN bounds): it could
        hold any value — EXCEPT an all-NULL row group, which holds no join
        keys at all (equi-joins never match NULL) and is skipped;
      * the surviving files disagree on lane (should not happen for one real
        column, but if it does the union is un-representable);
      * every row group was all-NULL (an empty export could prune the partner
        to nothing; asserting that from ``null_count`` alone is not worth the
        risk — stay unbounded).
    """
    if not files:
        return None
    pred_lane: Optional[str] = None
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
            spans.append((s_min, s_max))
    if not spans or pred_lane is None:
        return None
    spans.sort(key=lambda p: p[0])
    merged: List[List[object]] = [list(spans[0])]
    for lo, hi in spans[1:]:
        if lo <= merged[-1][1]:
            if hi > merged[-1][1]:
                merged[-1][1] = hi
        else:
            merged.append([lo, hi])
    if len(merged) > MAX_JOIN_INTERVALS:
        merged = [[merged[0][0], merged[-1][1]]]  # convex hull
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
        Safety cap on propagation passes.  The default covers every realistic
        join graph; an adversarial chain could in principle need more passes,
        in which case propagation stops early with ``converged=False`` — the
        survivors are then merely under-pruned, never unsound.

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
    cap = max_iterations if max_iterations is not None else (
        len(usable_edges) * 2 + len(survivors) + 2
    )

    # Stats digests are immutable across passes — memoise one per
    # (table, column) so the fixpoint loop never re-scans a stats frame.
    profile_cache: Dict[Tuple[TableKey, str], Tuple[Dict, Dict]] = {}

    def get_profile(t: TableKey, col: str) -> Tuple[Dict, Dict]:
        k = (t, col)
        if k not in profile_cache:
            profile_cache[k] = _column_profile(table_stats.get(t), col)
        return profile_cache[k]

    # Dirty tracking: a direction only needs to re-run when its SOURCE's
    # survivor set changed since it last ran (same source ⇒ same exported
    # intervals ⇒ idempotent pruning of the destination).
    version: Dict[TableKey, int] = {t: 0 for t in survivors}
    last_src_version: Dict[Tuple[int, int], int] = {}
    derived_cache: Dict[
        Tuple[TableKey, str, int], Optional[List[PredInterval]]
    ] = {}

    iterations = 0
    changed = True
    while changed and iterations < cap:
        changed = False
        iterations += 1
        for e_idx, edge in enumerate(usable_edges):
            directions = (
                (edge.left_table, edge.left_col,
                 edge.right_table, edge.right_col, edge.prune_right),
                (edge.right_table, edge.right_col,
                 edge.left_table, edge.left_col, edge.prune_left),
            )
            for d_idx, (src, src_col, dst, dst_col, dst_prunable) in enumerate(
                directions
            ):
                if not dst_prunable:
                    # Preserved outer-join side or multi-occurrence table —
                    # its files must never be dropped on account of this edge.
                    continue
                if not survivors[src]:
                    # An empty source can't bound the partner without asserting
                    # "no keys exist"; leave the partner to other edges/passes.
                    continue
                dkey = (e_idx, d_idx)
                if last_src_version.get(dkey) == version[src]:
                    continue  # source unchanged since last run → idempotent
                last_src_version[dkey] = version[src]
                ckey = (src, src_col, version[src])
                if ckey in derived_cache:
                    rngs = derived_cache[ckey]
                else:
                    _idx, file_rows = get_profile(src, src_col)
                    rngs = _derive_join_intervals(file_rows, survivors[src])
                    derived_cache[ckey] = rngs
                if not rngs:
                    continue
                dst_index, _rows = get_profile(dst, dst_col)
                new = _prune_by_occurrences(
                    survivors[dst], table_stats.get(dst),
                    [{dst_col: r} for r in rngs],
                    allow_empty=allow_empty, index=dst_index,
                )
                if len(new) < len(survivors[dst]):
                    steps.append(
                        f"join {src[0]}.{src[1]}.{src_col} -> "
                        f"{dst[0]}.{dst[1]}.{dst_col} {_fmt_intervals(rngs)}: "
                        f"{len(survivors[dst])} -> {len(new)} file(s)"
                    )
                    survivors[dst] = new
                    version[dst] += 1
                    changed = True

    files_after = {t: len(fs) for t, fs in survivors.items()}
    return JoinPrunePlan(
        survivors=survivors,
        files_before=files_before,
        files_after=files_after,
        steps=steps,
        iterations=iterations,
        converged=not changed,
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
    return prune_files_across_joins(
        join_edges,
        literal_constraints,
        table_files,
        table_stats,
        allow_empty=allow_empty,
    )
