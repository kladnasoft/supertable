# IslandDB benchmark

This standalone harness generates deterministic, wide Parquet corpora and
compares explicit `DUCKDB_LITE` with explicit `ISLANDDB`. DuckDB is the result
oracle. Both engines must return the exact same canonical columns, dtypes, and
values before any timed worker starts.

The default run prepares the 512 KiB and 64 MiB tiers:

```bash
python -m supertable.engine.benchmarks --sizes kb,mb --repeats 5
```

Large tiers are deliberately opt-in and require ample free disk:

```bash
python -m supertable.engine.benchmarks \
  --sizes 1gib,10gib --allow-large --repeats 5
```

Generation can be separated from execution:

```bash
python -m supertable.engine.benchmarks \
  --sizes kb,mb,1gib,10gib --allow-large --prepare-only
```

Each engine/workload timing series runs in a fresh process. The first query is
reported as cold (fresh process, engine connection, and benchmark cache
namespace); subsequent samples are warm. OS page-cache state is `uncontrolled`
by default. `--cold-mode fadvise` requests best-effort eviction and is labelled
as such because the kernel does not guarantee it.

Result JSON records:

- target and actual physical source bytes;
- whole-file bytes retained after file min/max pruning;
- compressed selected-column bytes across retained files (routing estimate);
- selected-column bytes in predicate-eligible row groups (pushdown estimate);
- cold and warm wall/CPU timings, RSS, Arrow, and DuckDB profile metrics;
- cache footprint and any cache metrics emitted through `PlanStats`;
- exact result digest and parity status;
- paired warm-median IslandDB speedup over DuckDB.

IslandDB additionally records its validated row-group count, decoded working
set, adaptive CPU/I/O worker allocation, memory/result/spill budgets, external
spill activity, and persistent range-cache requests/hit/download bytes.  Cold
means the first engine query with an empty benchmark cache namespace; warm
means the identical query is repeated with the same engine and cache.  For
remote Ceph/MinIO runs, the range counters distinguish original object bytes
from the actual footer/column ranges transferred. Local generated corpora have
no remote-range traffic and should not be interpreted as a network benchmark.

Memory-heavy joins whose decoded hash state cannot fit the container budget
route to DuckDB or an available Spark fleet. Direct-column integer group-by and
sealed ordering shapes use IslandDB's hard-quota query-private spill files;
unsupported spill semantics route rather than silently materializing in RAM.

Wall-clock performance never controls the process exit status. Result parity,
worker failures, corrupt corpora, and invalid configuration do.

The checked-in measured baseline is in
[`RESULTS_2026-08-12.md`](RESULTS_2026-08-12.md). It includes all four physical
tiers and records the one workload where IslandDB was slightly slower, rather
than hiding it behind an aggregate score.

The row-group/range-cache/resource-governor implementation follow-up is in
[`RESULTS_2026-08-13_ROWGROUP_SPILL.md`](RESULTS_2026-08-13_ROWGROUP_SPILL.md),
including fresh cold/warm parity measurements at 512 KiB, 64 MiB, 1 GiB, and
10 GiB.
