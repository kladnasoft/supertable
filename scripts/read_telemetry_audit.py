"""Read-path telemetry: where does a query's wall time actually go?

The write path emits 22 timed stages through its Profiler; the read path emits
two coarse Timer events. This wraps the real seams so a query breaks down into
phases without touching production code:

    parse      SQL -> sqlglot -> table refs + predicate intervals
    rbac       role resolution / column+row filters
    snapshots  catalog reads (Redis leaf payloads)
    stats      loading the column-stats artifact
    prune      dropping files by predicate vs stats
    estimate   the rest of DataEstimator.estimate (sizing, engine routing)
    connect    DuckDB connection init / pragmas
    view       reflection + RBAC + tombstone view construction
    execute    the query itself
    fetch      materialising the result

It also records PRUNING EFFECTIVENESS per query — files before and after —
because a phase being fast is not the same as it doing its job.

    python scripts/read_telemetry_audit.py --super perf_local --table perf_read_full
"""
from __future__ import annotations

import argparse
import os
import statistics as st
import time
from contextlib import contextmanager

os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import polars as pl

PHASES: dict[str, float] = {}
COUNTS: dict[str, int] = {}


@contextmanager
def phase(name: str):
    t = time.perf_counter()
    try:
        yield
    finally:
        PHASES[name] = PHASES.get(name, 0.0) + (time.perf_counter() - t) * 1000.0


def _wrap(obj, attr, name, *, count_files=None):
    """Time a method in place, optionally recording file counts."""
    orig = getattr(obj, attr)

    def inner(*a, **k):
        with phase(name):
            out = orig(*a, **k)
        if count_files:
            count_files(a, k, out)
        return out

    inner.__wrapped__ = orig
    setattr(obj, attr, inner)
    return orig


def install():
    """Patch the seams. Returns a restore callable."""
    # NOTE: `import supertable.engine.X` fails once `supertable` is imported —
    # supertable/__init__.py binds the name `engine` to the Engine ENUM, which
    # shadows the engine SUBPACKAGE as an attribute. importlib goes through the
    # module registry and is unaffected.
    from importlib import import_module
    est_mod = import_module("supertable.engine.data_estimator")
    ec = import_module("supertable.engine.engine_common")
    proc = import_module("supertable.processing")
    sp = import_module("supertable.utils.sql_parser")
    ac = import_module("supertable.rbac.access_control")

    undo = []

    def rec_prune(a, k, out):
        raw = a[0] if a else k.get("file_keys", [])
        COUNTS["files_before_prune"] = COUNTS.get("files_before_prune", 0) + len(raw)
        COUNTS["files_after_prune"] = COUNTS.get("files_after_prune", 0) + len(out)

    undo.append((proc, "prune_files_by_predicates",
                 _wrap(proc, "prune_files_by_predicates", "prune", count_files=rec_prune)))
    undo.append((proc, "load_stats", _wrap(proc, "load_stats", "stats")))
    # The estimator imported both by name, so its namespace needs patching too.
    # The estimator calls its OWN imported reference, so the counter has to be
    # attached here too — wrapping only processing.* times the phase but never
    # records how many files pruning actually removed.
    if hasattr(est_mod, "prune_files_by_predicates"):
        undo.append((est_mod, "prune_files_by_predicates",
                     _wrap(est_mod, "prune_files_by_predicates", "prune",
                           count_files=rec_prune)))
    if hasattr(est_mod, "load_stats"):
        undo.append((est_mod, "load_stats", _wrap(est_mod, "load_stats", "stats")))
    undo.append((est_mod.DataEstimator, "estimate",
                 _wrap(est_mod.DataEstimator, "estimate", "estimate")))
    undo.append((est_mod.DataEstimator, "_collect_snapshots_from_redis",
                 _wrap(est_mod.DataEstimator, "_collect_snapshots_from_redis", "snapshots")))
    undo.append((sp.SQLParser, "parse", _wrap(sp.SQLParser, "parse", "parse"))
                if hasattr(sp.SQLParser, "parse") else None)
    undo.append((ac, "restrict_read_access", _wrap(ac, "restrict_read_access", "rbac")))
    undo.append((ec, "create_reflection_view", _wrap(ec, "create_reflection_view", "view")))
    undo.append((ec, "new_duckdb_connection", _wrap(ec, "new_duckdb_connection", "connect")))
    undo.append((ec, "init_connection", _wrap(ec, "init_connection", "connect"))
                if hasattr(ec, "init_connection") else None)

    def restore():
        for item in undo:
            if item is None:
                continue
            obj, attr, orig = item
            setattr(obj, attr, orig)

    return restore


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", default="perf_bench")
    ap.add_argument("--super", dest="sup", default="perf_local")
    ap.add_argument("--table", default="perf_read_full")
    ap.add_argument("--role", default="superadmin")
    ap.add_argument("--key", default="event_id")
    ap.add_argument("--tcol", default="event_ts")
    ap.add_argument("--iterations", type=int, default=5)
    args = ap.parse_args()

    install()
    from supertable.data_reader import DataReader, engine

    T = args.table
    queries = {
        "count_star":       f"SELECT count(*) AS n FROM {T}",
        "point_lookup":     f"SELECT count(*) AS n FROM {T} WHERE {args.key} = 4242",
        "narrow_range":     f"SELECT count(*) AS n FROM {T} WHERE {args.key} BETWEEN 100 AND 200",
        "ts_cast":          f"SELECT count(*) AS n FROM {T} "
                            f"WHERE {args.tcol} >= TIMESTAMP '2025-12-01'",
        "ts_barestring":    f"SELECT count(*) AS n FROM {T} "
                            f"WHERE {args.tcol} >= '2025-12-01'",
        "dim_filter":       f"SELECT count(*) AS n FROM {T} WHERE dim_country = 'DE'",
    }

    rows = []
    for name, sql in queries.items():
        for i in range(args.iterations + 1):        # first is warmup
            PHASES.clear(); COUNTS.clear()
            t0 = time.perf_counter()
            df, status, msg = DataReader(
                super_name=args.sup, organization=args.org, query=sql,
            ).execute(role_name=args.role, with_scan=False, engine=engine.AUTO)
            wall = (time.perf_counter() - t0) * 1000.0
            if not str(status).endswith("OK"):
                print(f"  {name}: FAILED — {msg}")
                break
            if i == 0:
                continue                            # discard warmup
            rec = {"query": name, "wall_ms": wall}
            rec.update({f"t.{k}": v for k, v in PHASES.items()})
            rec.update({f"c.{k}": v for k, v in COUNTS.items()})
            rows.append(rec)

    if not rows:
        print("no successful queries")
        return 1

    df = pl.DataFrame(rows, infer_schema_length=None).fill_null(0.0)
    pl.Config.set_tbl_rows(40); pl.Config.set_tbl_width_chars(150)

    print("\nPER-QUERY PHASE BREAKDOWN (median ms over "
          f"{args.iterations} runs, warmup discarded)")
    tcols = [c for c in df.columns if c.startswith("t.")]
    agg = df.group_by("query").agg(
        [pl.col("wall_ms").median().round(1).alias("wall")]
        + [pl.col(c).median().round(1).alias(c.removeprefix("t.")) for c in tcols]
    )
    print(agg)

    print("\nSHARE OF WALL, all queries pooled")
    tot = df["wall_ms"].sum()
    share = sorted(((c.removeprefix("t."), df[c].sum()) for c in tcols),
                   key=lambda x: -x[1])
    for n, v in share:
        if v > 0.5:
            print(f"  {n:12s} {v / len(rows):8.1f}ms/query   {v / tot * 100:5.1f}%")

    if "c.files_before_prune" in df.columns:
        print("\nPRUNING EFFECTIVENESS")
        pr = df.group_by("query").agg(
            pl.col("c.files_before_prune").median().alias("before"),
            pl.col("c.files_after_prune").median().alias("after"),
        ).with_columns(
            ((1 - pl.col("after") / pl.col("before")) * 100).round(1).alias("pruned_pct")
        )
        print(pr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
