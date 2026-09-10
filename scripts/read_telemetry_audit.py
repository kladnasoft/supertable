"""Read-path telemetry, with exclusive-time accounting.

The first version of this script double-counted: it gave one label to three
nested functions, so a child's time was added to both itself and its parent,
and `connect` appeared to cost ~60ms on every query when the connection is in
fact built once per process. It also instrumented only duckdb_lite, so any
query routed to duckdb_pro reported zeros for every engine phase.

Both are fixed here:

  * every wrapped function has its OWN label — no label is reused;
  * timings are kept as INCLUSIVE and EXCLUSIVE (self) time. Exclusive time
    subtracts whatever nested wrapped calls consumed, so the exclusive column
    sums to the instrumented total and cannot double-count. Shares are
    reported on exclusive time only;
  * both engines are wrapped, and the engine that actually ran is recorded.

    STORAGE_TYPE=LOCAL python scripts/read_telemetry_audit.py
"""
from __future__ import annotations

import argparse
import os
import time
from contextlib import contextmanager
from importlib import import_module

os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import polars as pl

INCL: dict[str, float] = {}
EXCL: dict[str, float] = {}
COUNTS: dict[str, int] = {}
_STACK: list[float] = []          # per-frame accumulator of child time


def reset() -> None:
    INCL.clear(); EXCL.clear(); COUNTS.clear(); _STACK.clear()


@contextmanager
def phase(name: str):
    t0 = time.perf_counter()
    _STACK.append(0.0)
    try:
        yield
    finally:
        dt = (time.perf_counter() - t0) * 1000.0
        child = _STACK.pop()
        INCL[name] = INCL.get(name, 0.0) + dt
        EXCL[name] = EXCL.get(name, 0.0) + (dt - child)
        if _STACK:                       # charge our full span to the parent
            _STACK[-1] += dt


def _wrap(obj, attr, label, *, on_return=None):
    orig = getattr(obj, attr)

    def inner(*a, **k):
        with phase(label):
            out = orig(*a, **k)
        if on_return:
            on_return(a, k, out)
        return out

    setattr(obj, attr, inner)


def install() -> None:
    # `import supertable.engine.X` fails once supertable is imported: its
    # __init__ binds the name `engine` to the Engine ENUM, shadowing the engine
    # SUBPACKAGE attribute. importlib goes through the module registry instead.
    est = import_module("supertable.engine.data_estimator")
    ec = import_module("supertable.engine.engine_common")
    proc = import_module("supertable.processing")
    ac = import_module("supertable.rbac.access_control")
    duck = import_module("supertable.engine.duckdb")

    def rec_prune(a, k, out):
        raw = a[0] if a else k.get("file_keys", [])
        COUNTS["files_before"] = COUNTS.get("files_before", 0) + len(raw)
        COUNTS["files_after"] = COUNTS.get("files_after", 0) + len(out)

    _wrap(est.DataEstimator, "estimate", "estimate")
    _wrap(est.DataEstimator, "_collect_snapshots_from_redis", "catalog_read")
    if hasattr(est, "load_stats"):
        _wrap(est, "load_stats", "stats_load")
    if hasattr(est, "prune_files_by_predicates"):
        _wrap(est, "prune_files_by_predicates", "prune", on_return=rec_prune)
    _wrap(proc, "load_stats", "stats_load")
    _wrap(ac, "restrict_read_access", "rbac")

    for mod, cls_name, tag in ((duck, "DuckDBEngine", "duckdb"),):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue

        def mark(a, k, out, _t=tag):
            COUNTS[f"engine_{_t}"] = COUNTS.get(f"engine_{_t}", 0) + 1

        _wrap(cls, "execute", f"engine[{tag}]", on_return=mark)
        if hasattr(cls, "_get_connection"):
            _wrap(cls, "_get_connection", f"conn[{tag}]")
        if hasattr(cls, "_ensure_httpfs"):
            _wrap(cls, "_ensure_httpfs", f"httpfs[{tag}]")
        for fn in ("create_reflection_view_with_presign_retry",
                   "create_reflection_table_with_presign_retry"):
            if hasattr(mod, fn):
                _wrap(mod, fn, f"reflect[{tag}]")

    _wrap(ec, "init_connection", "conn_init")
    _wrap(ec, "create_reflection_view", "view_sql")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", default="perf_bench")
    ap.add_argument("--super", dest="sup", default="perf_local")
    ap.add_argument("--table", default="perf_read_full")
    ap.add_argument("--role", default="superadmin")
    ap.add_argument("--key", default="event_id")
    ap.add_argument("--tcol", default="event_ts")
    ap.add_argument("--iterations", type=int, default=4)
    args = ap.parse_args()

    install()
    from supertable.data_reader import DataReader, engine

    T = args.table
    queries = {
        "count_star":    f"SELECT count(*) AS n FROM {T}",
        "point_lookup":  f"SELECT count(*) AS n FROM {T} WHERE {args.key} = 4242",
        "narrow_range":  f"SELECT count(*) AS n FROM {T} WHERE {args.key} BETWEEN 100 AND 200",
        "ts_barestring": f"SELECT count(*) AS n FROM {T} WHERE {args.tcol} >= '2025-12-01'",
        "dim_filter":    f"SELECT count(*) AS n FROM {T} WHERE dim_country = 'DE'",
    }

    rows = []
    for name, sql in queries.items():
        for i in range(args.iterations + 1):          # first run warms caches
            reset()
            t0 = time.perf_counter()
            df, status, msg = DataReader(
                super_name=args.sup, organization=args.org, query=sql,
            ).execute(role_name=args.role, with_scan=False, engine=engine.AUTO)
            wall = (time.perf_counter() - t0) * 1000.0
            if not str(status).endswith("OK"):
                print(f"  {name}: FAILED — {msg}")
                break
            if i == 0:
                continue
            rec = {"query": name, "wall_ms": wall,
                   "engine": "duckdb" if COUNTS.get("engine_duckdb") else "?"}
            rec.update({f"x.{k}": v for k, v in EXCL.items()})
            rec.update({f"c.{k}": v for k, v in COUNTS.items()
                        if not k.startswith("engine_")})
            rows.append(rec)

    if not rows:
        print("no successful queries")
        return 1

    df = pl.DataFrame(rows, infer_schema_length=None).fill_null(0.0)
    pl.Config.set_tbl_rows(40); pl.Config.set_tbl_width_chars(170)
    xcols = sorted(c for c in df.columns if c.startswith("x."))

    print("\nEXCLUSIVE (self) TIME PER QUERY — median ms, warmup discarded")
    print(df.group_by("query", "engine").agg(
        [pl.col("wall_ms").median().round(1).alias("wall")]
        + [pl.col(c).median().round(1).alias(c.removeprefix("x.")) for c in xcols]
    ).sort("wall", descending=True))

    print("\nSHARE OF WALL (exclusive time — these sum, they do not overlap)")
    wall_tot = df["wall_ms"].sum()
    acc = 0.0
    for c in sorted(xcols, key=lambda c: -df[c].sum()):
        v = df[c].sum()
        acc += v
        if v > 0.5:
            print(f"  {c.removeprefix('x.'):20s} {v / len(rows):8.1f}ms/query  "
                  f"{v / wall_tot * 100:5.1f}%")
    print(f"  {'UNINSTRUMENTED':20s} {(wall_tot - acc) / len(rows):8.1f}ms/query  "
          f"{(wall_tot - acc) / wall_tot * 100:5.1f}%")

    if "c.files_before" in df.columns:
        print("\nPRUNING EFFECTIVENESS")
        print(df.group_by("query").agg(
            pl.col("c.files_before").median().alias("before"),
            pl.col("c.files_after").median().alias("after"),
        ).with_columns(
            ((1 - pl.col("after") / pl.col("before")) * 100).round(1).alias("pruned_pct")
        ).sort("pruned_pct", descending=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
