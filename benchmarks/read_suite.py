"""Read performance suite.

Every scenario records three independent things:

* **latency distribution** — min/p50/p95/max over N iterations after warmup;
* **planner telemetry** — files before pruning, pruned, kept, and the engine
  that actually ran, taken from the library's own plan stats rather than
  inferred from the clock;
* **a correctness seal** — a hash of the logical result, so a future version
  that gets faster by returning different rows is caught rather than praised.

Selectivity is a property of the generated data (see ``dataset.py``), so the
30-day scenario really is ~10% of the table and the 24-hour scenario really is
under 1%; each records the row count it actually saw so the claim is checked
rather than asserted.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from benchmarks import _harness
from benchmarks._harness import (
    BENCH_ORG,
    BENCH_ROLE,
    ScenarioResult,
    expect,
    finish_run,
    new_run,
    run_scenario,
    seal_dataframe,
)
from benchmarks import dataset as ds

# Bulky plan entries that would dominate a result file without adding signal.
_DROP_PLAN_KEYS = ("FILE_LIST",)


def _flatten_plan(reader: Any) -> Dict[str, Any]:
    plan: Dict[str, Any] = {}
    stats = getattr(getattr(reader, "plan_stats", None), "stats", None) or []
    for entry in stats:
        if isinstance(entry, dict):
            plan.update(entry)
    for key in _DROP_PLAN_KEYS:
        value = plan.pop(key, None)
        if isinstance(value, list):
            plan["FILE_LIST_LEN"] = len(value)
    return plan


# Set by run(fullscan=True). When on, the estimator returns every file and
# prunes nothing, so the suite measures — and more importantly SEALS — the
# unpruned path. Pruning may only ever drop files that provably hold no
# matching row, so a fullscan seal and a pruned seal MUST be identical; any
# difference is a pruning bug that dropped real data.
FULLSCAN = False


def _execute(query: str) -> tuple:
    """Run one query through the public read path and return (df, plan)."""
    from supertable.data_reader import DataReader, engine

    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG, query=query, source="sdk",
    )
    df, status, message = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
        fullscan=FULLSCAN,
    )
    if not str(status).endswith("OK"):
        raise RuntimeError(f"read failed ({status}): {message}")
    return df, _flatten_plan(reader)


def _rows_of(df: Any) -> int:
    if df is None:
        return 0
    height = getattr(df, "height", None)
    return int(height) if height is not None else int(len(df))


def _query_body(
    query: str,
    *,
    ordered: bool = False,
    expectations: Optional[Callable[[Any, Dict[str, Any]], Dict[str, Any]]] = None,
) -> Callable[[], Dict[str, Any]]:
    def body() -> Dict[str, Any]:
        df, plan = _execute(query)
        payload: Dict[str, Any] = {
            "rows": _rows_of(df),
            "plan": plan,
            "seal": seal_dataframe(df, ordered=ordered),
            "metrics": {"query": query},
        }
        if expectations is not None:
            payload["expectations"] = expectations(df, plan) or {}
        return payload

    return body


def _selectivity_check(scale: ds.Scale, low: float, high: float):
    """Assert a scenario really has the selectivity the brief asked for."""
    def check(df: Any, plan: Dict[str, Any]) -> Dict[str, Any]:
        # The group-by collapses rows, so selectivity is read from the
        # matching-row count each scenario carries in its own aggregate.
        matched = _sum_column(df, "matched_rows")
        if matched is None:
            return {}
        pct = matched / scale.rows * 100.0
        return expect(
            "selectivity_pct", low <= pct <= high, round(pct, 4), f"{low}..{high}%",
        )

    return check


def _sum_column(df: Any, name: str) -> Optional[int]:
    """Total one integer column of a small result frame, or None if absent."""
    if df is None or _rows_of(df) == 0:
        return None
    try:
        columns = [str(c) for c in df.columns]
        if name not in columns:
            return None
        index = columns.index(name)
        if hasattr(df, "rows"):                  # polars
            return sum(int(row[index]) for row in df.rows())
        return int(df.iloc[:, index].sum())      # pandas
    except Exception:
        return None


def _pruning_body(scale: ds.Scale, predicate: str, max_files_kept: int):
    """A predicate whose prunability is a stated, checked expectation."""
    query = (
        f"SELECT COUNT(*) AS matched_rows FROM {scale.table} WHERE {predicate}"
    )

    def body() -> Dict[str, Any]:
        df, plan = _execute(query)
        before = plan.get("FILES_BEFORE_PRUNE")
        kept = plan.get("FILES_KEPT")
        pruned = plan.get("FILES_PRUNED")
        checks: Dict[str, Any] = {}
        if kept is not None:
            checks.update(expect(
                "files_kept_within_bound", kept <= max_files_kept, kept,
                f"<= {max_files_kept}",
            ))
        if before is not None and pruned is not None and kept is not None:
            checks.update(expect(
                "prune_arithmetic", before == pruned + kept,
                f"{before} != {pruned}+{kept}" if before != pruned + kept else before,
                "before == pruned + kept",
            ))
        return {
            "rows": _rows_of(df),
            "plan": plan,
            "seal": seal_dataframe(df),
            "metrics": {"query": query, "predicate": predicate},
            "expectations": checks,
        }

    return body


def scenarios(scale: ds.Scale) -> List[Dict[str, Any]]:
    """The read scenarios, in the order they are measured."""
    table = scale.table
    ts = ds.TIME_COLUMN
    key = ds.KEY_COLUMN

    start_30d = ds.sql_ts(ds.window_start(30))
    start_24h = ds.sql_ts(ds.window_start(1))
    # A window that sits inside a single file's slice — the best case for
    # pruning, and the floor this engine can achieve on a time filter.
    one_file_days = scale.days_per_file
    narrow_lo = ds.sql_ts(ds.window_start(one_file_days * 2))
    narrow_hi = ds.sql_ts(ds.window_start(one_file_days * 1.5))

    ids = ds.sample_ids(scale, 1000)
    id_list = ",".join(str(i) for i in ids)
    single_id = ids[len(ids) // 2]

    return [
        {
            "id": "random_1000_by_key",
            "description": "1000 reproducibly-random primary keys scattered "
                           "across the whole table (random access, not a range)",
            "body": _query_body(
                f"SELECT * FROM {table} WHERE {key} IN ({id_list})"
            ),
        },
        {
            "id": "agg_30d_by_country",
            "description": "past 30 days: SUM(measure) GROUP BY dimension "
                           "(~10% of the table)",
            "body": _query_body(
                f"SELECT dim_country, COUNT(*) AS matched_rows, "
                f"SUM(measure_amount) AS total_amount, "
                f"SUM(measure_qty) AS total_qty "
                f"FROM {table} WHERE {ts} >= TIMESTAMP '{start_30d}' "
                f"GROUP BY dim_country ORDER BY dim_country",
                expectations=_selectivity_check(scale, 8.0, 12.0),
            ),
        },
        {
            "id": "agg_24h_by_country",
            "description": "past 24 hours: SUM(measure) GROUP BY dimension "
                           "(<1% of the table)",
            "body": _query_body(
                f"SELECT dim_country, COUNT(*) AS matched_rows, "
                f"SUM(measure_amount) AS total_amount "
                f"FROM {table} WHERE {ts} >= TIMESTAMP '{start_24h}' "
                f"GROUP BY dim_country ORDER BY dim_country",
                expectations=_selectivity_check(scale, 0.0, 1.0),
            ),
        },
        {
            "id": "top_1000_by_date_desc",
            "description": "SELECT * ORDER BY date DESC LIMIT 1000 "
                           "(order is part of the seal)",
            "body": _query_body(
                f"SELECT * FROM {table} ORDER BY {ts} DESC, {key} DESC LIMIT 1000",
                ordered=True,
            ),
        },
        {
            "id": "top_10000_by_date_desc",
            "description": "SELECT * ORDER BY date DESC LIMIT 10000 "
                           "(order is part of the seal)",
            "body": _query_body(
                f"SELECT * FROM {table} ORDER BY {ts} DESC, {key} DESC LIMIT 10000",
                ordered=True,
            ),
        },
        # ---- additional coverage -------------------------------------------
        {
            "id": "count_star_full",
            "description": "full-table COUNT(*) — the unprunable floor, every "
                           "file must be touched",
            "body": _query_body(f"SELECT COUNT(*) AS matched_rows FROM {table}"),
        },
        {
            "id": "point_lookup_single_key",
            "description": "single-row lookup by primary key — best-case "
                           "pruning and latency floor",
            "body": _query_body(f"SELECT * FROM {table} WHERE {key} = {single_id}"),
        },
        {
            "id": "distinct_users_30d",
            "description": "COUNT(DISTINCT high-cardinality column) over 30 days "
                           "— hash-aggregate pressure, not I/O",
            "body": _query_body(
                f"SELECT COUNT(DISTINCT user_id) AS distinct_users "
                f"FROM {table} WHERE {ts} >= TIMESTAMP '{start_30d}'"
            ),
        },
        {
            "id": "dimension_filter_full_scan",
            "description": "non-time filter with no prunable predicate — "
                           "full scan, aggregated so the payload stays small",
            "body": _query_body(
                f"SELECT COUNT(*) AS matched_rows, "
                f"ROUND(SUM(measure_amount), 2) AS total_amount "
                f"FROM {table} WHERE dim_country = 'AT'"
            ),
        },
        {
            "id": "two_dim_group_by_30d",
            "description": "two-dimension group-by over 30 days — wider grouping "
                           "key than the single-dimension case",
            "body": _query_body(
                f"SELECT dim_country, dim_channel, COUNT(*) AS matched_rows, "
                f"ROUND(SUM(measure_amount), 2) AS total_amount "
                f"FROM {table} WHERE {ts} >= TIMESTAMP '{start_30d}' "
                f"GROUP BY dim_country, dim_channel "
                f"ORDER BY dim_country, dim_channel"
            ),
        },
        {
            "id": "narrow_window_single_file",
            "description": "time window contained in one file's slice — the "
                           "engine's best achievable prune on a time filter",
            "body": _query_body(
                f"SELECT COUNT(*) AS matched_rows, "
                f"ROUND(SUM(measure_amount), 2) AS total_amount FROM {table} "
                f"WHERE {ts} >= TIMESTAMP '{narrow_lo}' "
                f"AND {ts} < TIMESTAMP '{narrow_hi}'"
            ),
        },
        {
            "id": "filtered_order_limit",
            "description": "filter + ORDER BY + LIMIT together — pruning and "
                           "top-N in one plan",
            "body": _query_body(
                f"SELECT {key}, {ts}, dim_country, measure_amount FROM {table} "
                f"WHERE {ts} >= TIMESTAMP '{start_30d}' AND status = 'paid' "
                f"ORDER BY {ts} DESC, {key} DESC LIMIT 1000",
                ordered=True,
            ),
        },
        # ---- pruning, asserted against planner telemetry --------------------
        {
            "id": "prune_30d_window",
            "description": "pruning: a 30-day predicate may keep at most ~10% "
                           "of files",
            "body": _pruning_body(
                scale,
                f"{ts} >= TIMESTAMP '{start_30d}'",
                max_files_kept=max(1, round(scale.files * 0.15)),
            ),
        },
        {
            "id": "prune_24h_window",
            "description": "pruning: a 24-hour predicate should survive on a "
                           "single file",
            "body": _pruning_body(
                scale,
                f"{ts} >= TIMESTAMP '{start_24h}'",
                max_files_kept=max(1, round(scale.files * 0.03)),
            ),
        },
        {
            "id": "prune_key_point",
            "description": "pruning: an equality on the clustered key should "
                           "survive on a single file",
            "body": _pruning_body(
                scale, f"{key} = {single_id}", max_files_kept=1,
            ),
        },
        {
            "id": "prune_no_predicate",
            "description": "pruning control: with no predicate nothing may be "
                           "pruned — guards against over-eager pruning",
            "body": _pruning_body(
                scale, "1 = 1", max_files_kept=scale.files,
            ),
        },
    ]


def run(profile: str, scale: ds.Scale, *, iterations: int = 5, warmup: int = 1,
        dataset_record: Optional[Dict[str, Any]] = None, fullscan: bool = False,
        log=print):
    import time

    global FULLSCAN
    FULLSCAN = bool(fullscan)

    started = time.perf_counter()
    suite = new_run("read", profile, scale.name)
    suite.dataset = dataset_record or {}
    if FULLSCAN:
        # Tags the FILENAME only, so this cannot silently overwrite the pruned
        # result — while still comparing cleanly against it.
        suite.variant = "fullscan"

    log(f"\nread suite — profile={profile} scale={scale.name} "
        f"iterations={iterations} warmup={warmup}"
        f"{'  [FULLSCAN — pruning disabled]' if FULLSCAN else ''}")
    for spec in scenarios(scale):
        result: ScenarioResult = run_scenario(
            spec["id"], spec["description"], spec["body"],
            iterations=iterations, warmup=warmup, log=log,
        )
        suite.scenarios.append(result)

    return finish_run(suite, started)
