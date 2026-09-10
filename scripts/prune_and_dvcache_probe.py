"""Two measurements that decide the next two optimisations.

A. Does the stats prefilter (stats_prune) exclude files, and what does the
   delete's SHAPE do to it?  Random keys vs a clustered key range.

B. What does a delete cost the read path?  The materialised deletion-vector
   table is cached under the tombstone path, so a scheme that changes that
   path on every delete would rebuild it every time.  Measures query latency
   on a stable snapshot versus straight after a delete.
"""
from __future__ import annotations

import os
import statistics as st
import time
import uuid

os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import numpy as np
import polars as pl

CAPTURED: list[dict] = []


class _Rec:
    def __init__(self, *_, **__): pass
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def log_metric(self, payload): CAPTURED.append(payload)


def main() -> int:
    import supertable.data_writer as dwmod
    dwmod.MonitoringWriter = _Rec
    from supertable.data_writer import DataWriter
    from supertable.data_reader import DataReader, engine
    from supertable.redis_catalog import RedisCatalog

    sfx = uuid.uuid4().hex[:8]
    org, sup, tbl, role = f"pr{sfx}", f"prsup{sfx}", "facts", "superadmin"
    w = DataWriter(super_name=sup, organization=org)
    cat = RedisCatalog()
    ROWS, BATCH = 2_000_000, 100_000

    def delete(keys) -> dict:
        CAPTURED.clear()
        t = time.perf_counter()
        w.write(role, tbl, pl.DataFrame({"id": pl.Series(keys, dtype=pl.Int64)}).to_arrow(),
                ["id"], delete_only=True)
        wall = (time.perf_counter() - t) * 1000
        p = CAPTURED[0] if CAPTURED else {}
        c, tm = p.get("counts", {}), p.get("timings", {})
        return {"wall": wall, "pruned": c.get("stats_pruned_files", 0),
                "resolve_ms": tm.get("resolve_overwrite", 0) * 1000,
                "prune_ms": tm.get("stats_prune", 0) * 1000}

    def q(sql: str) -> float:
        t = time.perf_counter()
        df, status, msg = DataReader(super_name=sup, organization=org,
                                     query=sql).execute(role_name=role, with_scan=False,
                                                        engine=engine.AUTO)
        if not str(status).endswith("OK"):
            raise RuntimeError(msg)
        return (time.perf_counter() - t) * 1000

    try:
        print(f"building {ROWS:,} rows ...")
        for i in range(ROWS // BATCH):
            base = i * BATCH
            w.write(role, tbl, pl.DataFrame({
                "id": pl.arange(base, base + BATCH, eager=True),
                "grp": pl.Series([f"g{j % 16}" for j in range(BATCH)]),
                "amt": (pl.arange(0, BATCH, eager=True).cast(pl.Float64) * 1.7) % 991.0,
            }).to_arrow(), [])

        rng = np.random.default_rng(4)

        print("\n" + "=" * 72)
        print("A. DOES THE STATS PREFILTER EXCLUDE FILES?")
        print("=" * 72)
        rand_keys = rng.choice(ROWS, 100_000, replace=False)
        r = delete(rand_keys)
        print(f"  RANDOM   100k keys spread over {ROWS:,} rows")
        print(f"    files pruned by stats : {r['pruned']}")
        print(f"    stats_prune           : {r['prune_ms']:.1f}ms")
        print(f"    resolve_overwrite     : {r['resolve_ms']:.1f}ms")

        lo = 1_200_000
        clus_keys = np.arange(lo, lo + 100_000)
        c = delete(clus_keys)
        print(f"  CLUSTERED 100k contiguous keys [{lo:,}..{lo + 100_000:,})")
        print(f"    files pruned by stats : {c['pruned']}")
        print(f"    stats_prune           : {c['prune_ms']:.1f}ms")
        print(f"    resolve_overwrite     : {c['resolve_ms']:.1f}ms"
              f"   -> {(c['resolve_ms'] - r['resolve_ms']) / r['resolve_ms'] * 100:+.0f}% vs random")

        print("\n" + "=" * 72)
        print("B. WHAT DOES A DELETE COST THE READ PATH?")
        print("=" * 72)
        sql = f"SELECT count(*) AS n, sum(amt) AS s FROM {tbl} WHERE grp = 'g3'"
        q(sql)                                    # warm everything
        warm = [q(sql) for _ in range(5)]
        print(f"  repeated query, snapshot unchanged : median {st.median(warm):7.1f}ms  "
              f"{[round(x) for x in warm]}")

        after = []
        for i in range(5):
            delete(rng.choice(ROWS, 5_000, replace=False))
            after.append(q(sql))
        print(f"  first query after each delete      : median {st.median(after):7.1f}ms  "
              f"{[round(x) for x in after]}")
        print(f"  delta                              : "
              f"{st.median(after) - st.median(warm):+7.1f}ms "
              f"({(st.median(after) / st.median(warm) - 1) * 100:+.0f}%)")
        print("\n  That delta is what a per-delete tombstone path change would cost reads")
        print("  on EVERY delete (the DV table is keyed by the tombstone path).")
        return 0
    finally:
        try:
            cat.delete_super_table(org, sup)
        except Exception as e:
            print("cleanup warning:", e)


if __name__ == "__main__":
    raise SystemExit(main())
