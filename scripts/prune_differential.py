"""Run every read-suite scenario with pruning ON and OFF and diff the results.

Pruning may only remove files that provably hold no matching row, so the two
runs must return identical data. The benchmark seals answer "same or not"; this
answers "how different, and in which direction" — which is what tells you
whether pruning is dropping rows or inventing them.

Both modes run in the same process against the same snapshot, so the dataset
cannot move between them.

    STORAGE_TYPE=LOCAL python scripts/prune_differential.py
"""
from __future__ import annotations

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import pandas as pd

QUERIES: dict[str, str] = {}


def collect_queries(scale) -> None:
    """Capture each scenario's SQL by intercepting the suite's own executor."""
    from benchmarks import read_suite as rs

    real = rs._execute
    current = {"id": None}

    def spy(query: str):
        if current["id"] and current["id"] not in QUERIES:
            QUERIES[current["id"]] = query
        # Return a cheap stand-in: we only want the SQL, not the timings.
        raise _Collected()

    class _Collected(Exception):
        pass

    rs._execute = spy
    for spec in rs.scenarios(scale):
        current["id"] = spec["id"]
        try:
            spec["body"]()
        except _Collected:
            pass
        except Exception:
            pass
    rs._execute = real


def run(query: str, fullscan: bool, retries: int = 6):
    from supertable.data_reader import DataReader, engine

    for attempt in range(retries):
        try:
            reader = DataReader(
                super_name="perf_local", organization="perf_bench",
                query=query, source="sdk",
            )
            df, status, message = reader.execute(
                role_name="superadmin", with_scan=False,
                engine=engine.AUTO, fullscan=fullscan,
            )
            if not str(status).endswith("OK"):
                raise RuntimeError(message)
            return df
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(3)


def compare(a: pd.DataFrame, b: pd.DataFrame) -> dict:
    """Diff two result sets as SETS of rows — order is not guaranteed."""
    if list(a.columns) != list(b.columns):
        return {"verdict": "SCHEMA", "detail": f"{list(a.columns)} vs {list(b.columns)}"}

    # Sort by every column so row order cannot register as a difference.
    cols = list(a.columns)
    sa = a.sort_values(cols).reset_index(drop=True)
    sb = b.sort_values(cols).reset_index(drop=True)
    if len(sa) != len(sb):
        return {"verdict": "ROWCOUNT", "detail": f"{len(sa)} -> {len(sb)}"}
    if sa.equals(sb):
        return {"verdict": "IDENTICAL", "detail": ""}

    # Same shape, different values: report the largest numeric divergence.
    worst = ""
    for c in cols:
        if pd.api.types.is_numeric_dtype(sa[c]):
            d = (sb[c] - sa[c])
            nz = d[d != 0]
            if len(nz):
                worst = (f"{c}: {len(nz)}/{len(d)} rows differ, "
                         f"sum {sa[c].sum():,.0f} -> {sb[c].sum():,.0f} "
                         f"({d.sum():+,.0f})")
                break
    return {"verdict": "VALUES", "detail": worst or "non-numeric difference"}


def main() -> int:
    logging.disable(logging.DEBUG)
    from benchmarks import dataset as ds

    scale = ds.SCALES["full"]
    collect_queries(scale)
    print(f"collected {len(QUERIES)} scenario queries\n")

    rows = []
    for name, q in QUERIES.items():
        try:
            pruned = run(q, False)
            full = run(q, True)
        except Exception as e:
            print(f"  {name:28s} ERROR {type(e).__name__}: {str(e)[:60]}")
            continue
        res = compare(pruned, full)
        rows.append((name, len(pruned), len(full), res["verdict"], res["detail"]))
        flag = "  " if res["verdict"] == "IDENTICAL" else "!!"
        print(f"{flag} {name:28s} {len(pruned):>7,} -> {len(full):>7,}  "
              f"{res['verdict']:10s} {res['detail']}")

    bad = [r for r in rows if r[3] != "IDENTICAL"]
    print(f"\n{len(rows) - len(bad)}/{len(rows)} scenarios identical; "
          f"{len(bad)} differ")
    if bad:
        print("\nPruning changed the answer for:")
        for name, _, _, verdict, detail in bad:
            print(f"  {name:28s} {verdict:10s} {detail}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
