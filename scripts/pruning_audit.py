"""Differential audit: every generated query, pruned vs fullscan.

Fullscan is ground truth — it reads every file, so it cannot be wrong about
which rows match. Pruning is only allowed to skip files that provably hold no
matching row, so the two must return the same answer. Any disagreement is a
pruning bug.

The audit also records how many files each query actually pruned, because
"pruning is correct" is trivially satisfiable by not pruning at all. A fix that
makes the answers agree while dropping the prune rate to zero is not a fix, and
the report shows both numbers side by side so that cannot hide.

    STORAGE_TYPE=LOCAL python scripts/pruning_audit.py
    STORAGE_TYPE=LOCAL python scripts/pruning_audit.py --family join_2 --limit 50
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import numpy as np
import pandas as pd

# Floating-point SUM re-associates when files are read in a different order, so
# an exact compare would flag ~1 ULP differences as corruption. Measured drift
# on real data was 2.5e-16 relative; this is seven orders of magnitude looser
# and still far tighter than any real pruning bug, which drops whole rows.
FLOAT_RTOL = 1e-9

_PRUNE_STATS: Dict[str, int] = {"before": 0, "after": 0}


def _install_prune_spy() -> None:
    from importlib import import_module

    est = import_module("supertable.engine.data_estimator")
    orig = est.prune_files_by_predicates

    def spy(file_keys, *a, **k):
        out = orig(file_keys, *a, **k)
        _PRUNE_STATS["before"] += len(file_keys)
        _PRUNE_STATS["after"] += len(out)
        return out

    est.prune_files_by_predicates = spy


def run_one(sql: str, fullscan: bool, retries: int = 5):
    from supertable.data_reader import DataReader, engine
    from supertable.tests.pruning import dataset as D

    last = None
    for attempt in range(retries):
        try:
            reader = DataReader(super_name=D.SUPER, organization=D.ORG,
                                query=sql, source="sdk")
            df, status, message = reader.execute(
                role_name=D.ROLE, with_scan=False,
                engine=engine.AUTO, fullscan=fullscan,
            )
            if not str(status).endswith("OK"):
                raise RuntimeError(message)
            return df
        except Exception as e:
            last = e
            # Redis sentinel failover is transient and unrelated to pruning.
            if "MasterNotFound" in type(e).__name__ or "No master found" in str(e):
                time.sleep(2)
                continue
            raise
    raise last


def same(a: pd.DataFrame, b: pd.DataFrame) -> Optional[str]:
    """None when the two results are equal; otherwise why they differ."""
    if list(a.columns) != list(b.columns):
        return f"columns {list(a.columns)} vs {list(b.columns)}"
    if len(a) != len(b):
        return f"row count {len(a)} vs {len(b)}"
    if len(a) == 0:
        return None

    cols = list(a.columns)
    # The read path guarantees no ordering, so compare as sets of rows.
    sa = a.sort_values(cols, kind="mergesort").reset_index(drop=True)
    sb = b.sort_values(cols, kind="mergesort").reset_index(drop=True)

    for c in cols:
        x, y = sa[c], sb[c]
        if pd.api.types.is_float_dtype(x) or pd.api.types.is_float_dtype(y):
            xv = pd.to_numeric(x, errors="coerce").astype(float).to_numpy()
            yv = pd.to_numeric(y, errors="coerce").astype(float).to_numpy()
            if not np.allclose(xv, yv, rtol=FLOAT_RTOL, atol=1e-9,
                               equal_nan=True):
                bad = int(np.argmax(~np.isclose(xv, yv, rtol=FLOAT_RTOL,
                                                atol=1e-9, equal_nan=True)))
                return (f"column {c!r}: row {bad} {xv[bad]!r} vs {yv[bad]!r} "
                        f"(sum {np.nansum(xv)!r} vs {np.nansum(yv)!r})")
        else:
            if not x.equals(y):
                ne = (x != y) & ~(x.isna() & y.isna())
                if ne.any():
                    i = int(np.argmax(ne.to_numpy()))
                    return f"column {c!r}: row {i} {x.iloc[i]!r} vs {y.iloc[i]!r}"
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", default=None, help="restrict to one family")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--stride", type=int, default=1,
                    help="sample every Nth query (keeps family coverage)")
    ap.add_argument("--out", default="/tmp/pruning_audit.json")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--rebuild", action="store_true")
    args = ap.parse_args()

    logging.disable(logging.WARNING)
    from supertable.tests.pruning import dataset as D
    from supertable.tests.pruning.queries import all_queries

    D.build(rebuild=args.rebuild, log=print)
    _install_prune_spy()

    queries = all_queries()
    if args.family:
        queries = [q for q in queries if q[1] == args.family]
    if args.stride > 1:
        queries = queries[::args.stride]
    if args.limit:
        queries = queries[:args.limit]

    print(f"\nauditing {len(queries):,} queries "
          f"({len(set(q[1] for q in queries))} families)\n")

    by_family: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"n": 0, "mismatch": 0, "error": 0,
                 "files_before": 0, "files_after": 0})
    failures: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []
    started = time.perf_counter()

    for idx, (qid, family, sql) in enumerate(queries, 1):
        fam = by_family[family]
        fam["n"] += 1
        try:
            truth = run_one(sql, fullscan=True)
            _PRUNE_STATS["before"] = _PRUNE_STATS["after"] = 0
            got = run_one(sql, fullscan=False)
            fam["files_before"] += _PRUNE_STATS["before"]
            fam["files_after"] += _PRUNE_STATS["after"]
        except Exception as e:
            fam["error"] += 1
            errors.append({"id": qid, "family": family, "sql": sql,
                           "error": f"{type(e).__name__}: {str(e)[:200]}"})
            continue

        why = same(truth, got)
        if why:
            fam["mismatch"] += 1
            failures.append({"id": qid, "family": family, "sql": sql,
                             "why": why})
            if not args.quiet:
                print(f"  MISMATCH [{family}] {qid}\n    {sql[:150]}\n    {why}")

        if idx % 250 == 0:
            el = time.perf_counter() - started
            print(f"  ... {idx:,}/{len(queries):,}  "
                  f"{len(failures)} mismatches, {len(errors)} errors  "
                  f"({el:.0f}s, {idx / el:.1f} q/s)")

    elapsed = time.perf_counter() - started
    total = sum(f["n"] for f in by_family.values())
    mism = sum(f["mismatch"] for f in by_family.values())
    errs = sum(f["error"] for f in by_family.values())

    print(f"\n{'family':22s} {'n':>6s} {'mismatch':>9s} {'error':>6s} {'pruned':>8s}")
    for name in sorted(by_family):
        f = by_family[name]
        before, after = f["files_before"], f["files_after"]
        pr = f"{(1 - after / before) * 100:6.1f}%" if before else "     --"
        flag = "  " if not f["mismatch"] else "!!"
        print(f"{flag}{name:20s} {f['n']:6d} {f['mismatch']:9d} "
              f"{f['error']:6d} {pr:>8s}")

    print(f"\nchecked {total:,} queries in {elapsed:.0f}s "
          f"({total / max(elapsed, 1e-9):.1f}/s)")
    print(f"  identical to fullscan : {total - mism - errs:,}")
    print(f"  MISMATCHES            : {mism:,}")
    print(f"  errors                : {errs:,}")

    payload = {"total": total, "mismatches": mism, "errors": errs,
               "elapsed_s": elapsed,
               "by_family": {k: dict(v) for k, v in by_family.items()},
               "failures": failures, "error_detail": errors[:50]}
    with open(args.out, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"\nwrote {args.out}")
    return 1 if (mism or errs) else 0


if __name__ == "__main__":
    raise SystemExit(main())
