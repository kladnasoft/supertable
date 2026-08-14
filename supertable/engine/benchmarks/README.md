# IslandDB benchmark

This standalone harness generates deterministic, wide Parquet corpora and
compares explicit `DUCKDB` with explicit `ISLANDDB`. DuckDB is the result
oracle. Both engines must return the exact same canonical columns, dtypes, and
values before any timed worker starts.

The default run prepares the 512 KiB and 64 MiB tiers:

```bash
python -m supertable.engine.benchmarks --sizes kb,mb --repeats 5
```

`spill_group` projects every public column, groups by the generated
low-cardinality `dimension`, and orders the 1,024-row result. IslandDB executes
this workload through its bounded Arrow streaming API so its conservative
generic grouped-result estimate cannot force pandas materialization. On a
large corpus this exercises IslandDB's query-private external group/sort path.
DuckDB keeps its normal materialized API because the result itself is small.
`COUNT(non-null column)` may be served from valid Parquet metadata by an engine,
so process/profile I/O counters—not projected bytes alone—show how many value
bytes were physically read.

Large tiers are deliberately opt-in and require ample free disk:

```bash
python -m supertable.engine.benchmarks \
  --sizes 1gib,10gib --allow-large --repeats 5
```

The 50-GiB full-table scan is also opt-in. `full_scan` reduces every public
column from every file and row group via bounded `MAX(column)` aggregates, so
both readers must consume value pages while the result and aggregate state stay
tiny. (`COUNT(column)` is not used because DuckDB can answer it from Parquet
null-count metadata.) The safety preflight requires 150 GiB free for this tier;
place `--corpus-root`, `--cache-root`, and `--home-root` on a suitably sized
benchmark volume:

```bash
systemd-run --user --scope \
  -p MemoryMax=8589934592 -p MemorySwapMax=0 \
  python -m supertable.engine.benchmarks \
  --sizes 50gib --allow-large --workloads full_scan \
  --payload-columns 26 --payload-width 64 \
  --engine-memory-limit 6GiB --threads 8 --disable-caches \
  --cold-mode fadvise --min-cold-read-fraction 0.99 --repeats 3 \
  --corpus-root /mnt/benchmark/corpora \
  --cache-root /mnt/benchmark/cache --home-root /mnt/benchmark/home \
  --worker-timeout 14400 --output /tmp/islanddb-50gib-8gib.json
```

When only the existing 10-GiB/30-public-column corpus fits on disk, the harness
can intentionally present its paths five times. This is a 50-GiB **logical**
scan over 10 GiB of unique backing, and the JSON keeps those quantities
separate (`logical_source_bytes`, `actual_source_bytes`, and `source_repeat`):

```bash
systemd-run --user --scope \
  -p MemoryMax=8589934592 -p MemorySwapMax=0 \
  python -m supertable.engine.benchmarks \
  --sizes 10gib --allow-large --source-repeat 5 \
  --payload-columns 26 --payload-width 64 --workloads full_scan \
  --engine-memory-limit 6GiB --threads 8 --disable-caches \
  --cold-mode fadvise \
  --min-cold-read-fraction 0.99 --repeats 3 \
  --corpus-root /tmp/island-telemetry-corpus \
  --cache-root /tmp/islanddb-50gib-cache \
  --home-root /tmp/islanddb-50gib-home \
  --worker-timeout 14400 --output /tmp/islanddb-logical-50gib-8gib.json
```

Duplicate paths are deliberate query inputs, not duplicate files. `COUNT(*)`
in the result proves that all five logical copies were visited, while bounded
`MAX(column)` reductions force value consumption. The cold physical-read gate
uses the unique projected bytes because later occurrences may be satisfied by
the kernel page cache within the same query; it never labels those cached
re-reads as 50 GiB of physical disk traffic.

`--engine-memory-limit 6GiB` configures `SUPERTABLE_DUCKDB_MEMORY_LIMIT=6GiB`
and `SUPERTABLE_ISLAND_MAX_MEMORY_BYTES=6442450944` inside every isolated
worker, leaving 2 GiB headroom below the cgroup cap. `--threads 8` sets DuckDB
threads, `POLARS_MAX_THREADS`, and IslandDB's CPU/I/O maxima to eight.
`--disable-caches` disables DuckDB's external file cache and IslandDB's
whole-object/range caches; local kernel page-cache behavior is still measured.
For this explicit benchmark limit, IslandDB's query/global fractions are set to
1.0 so the 6-GiB workspace is not reduced by the normal production fractions a
second time; the outer 8-GiB cgroup remains the hard process boundary.
Those are engine settings, not a kernel-enforced process limit; the surrounding
`systemd-run` scope supplies the actual 8-GiB cgroup ceiling and disables swap
for a deterministic memory-pressure comparison. The result records the active
DuckDB and IslandDB limits plus cgroup-v2 `memory.current`, `memory.peak`,
`memory.max`, `memory.events`, `memory.stat`, `memory.pressure`, `io.stat`, and
corresponding swap counters where supported.
The cgroup counters are cumulative for the containing scope; per-query
`rss_peak_bytes`/`rss_peak_delta_bytes` remain the engine-by-engine memory
comparison, while cgroup `memory.peak` and OOM events prove scope enforcement.
Workers record before/after event counters and fail the run if any OOM event is
introduced during an engine series.
Each sample also records `/proc/self/io` deltas (`read_bytes`, `rchar`, and
related counters). `read_bytes` is actual block I/O and may be zero for cached
pages; the manifest's compressed selected-column bytes remain the logical
Parquet coverage measurement.

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
set, process-pool admission widths, memory/result/spill budgets, external
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

The JSON also records the actual DuckDB connection thread setting and Polars
process-pool width. The harness does not silently pin either engine to one
thread; set `SUPERTABLE_DUCKDB_THREADS` explicitly only when that override is
part of the experiment.

Wall-clock performance never controls the normal comparison harness' process
exit status. Result parity, worker failures, corrupt corpora, and invalid
configuration do.

## Five-minute IslandDB spill regression gate

While tuning the 10-GiB spill implementation, do not rerun the 28-second
DuckDB oracle for every code change. The dedicated gate validates and reuses
the sealed DuckDB request/result, then launches only IslandDB in a fresh Docker
container:

```bash
python -m supertable.engine.benchmarks.spill_gate \
  --request-template /tmp/islanddb-spill-4g-Mq8AXiKd/islanddb/request.json \
  --oracle-request /tmp/islanddb-spill-4g-Mq8AXiKd/duckdb/request.json \
  --oracle-response /tmp/islanddb-spill-4g-Mq8AXiKd/duckdb/response.json \
  --corpus-root /tmp/island-telemetry-corpus/islanddb-wide-v1-10gib-8dd62e12aa33 \
  --output-root /tmp/islanddb-spill-iterations/change-001 \
  --image kladnasoft/dataisland-core:latest \
  --timeout 300 --target-seconds 100 --attempts 1
```

The gate is intentionally narrower and blocking. It accepts only the physical
10-GiB `spill_group` plan with one cold sample, four engine threads, and a
2-GiB internal workspace. Docker fixes the hard boundary at four CPUs, 4 GiB
RAM, and zero swap. A changed plan, invalid oracle digest, result/dtype/value
mismatch, OOM, missing telemetry, or wall time over 300 seconds fails the run.
A correct run between 100 and 300 seconds is retained as diagnostic evidence
but exits with the separate `target_missed` status.

Every attempt uses a new directory and never overwrites an older result.
`attempt.json` records exact parity, wall/CPU time, RSS, process and cgroup I/O,
memory/swap/OOM counters, spill high-water, the optimized plan, container
state, and a digest of the current engine diff. A host-side sampler continues
to record cgroup/RSS/I/O/spill telemetry even if the worker is killed at the
five-minute deadline and cannot write `response.json`. After recording the
high-water and post-worker footprint, the gate removes abandoned query-private
spill files so repeated attempts cannot exhaust the disk. Pass
`--retain-failed-spill` only when the actual run files are required for a
forensic inspection. Successful engine cleanup is verified by a zero-byte
post-worker spill footprint.

`--attempts N` runs independent fresh-container samples after a passing
attempt. It stops on the first timeout, error, parity failure, or target miss,
so a known failure cannot silently consume another five minutes or fill the
spill volume. After changing the implementation, select a new `--output-root`
and invoke the gate again.

## Five-minute two-engine material-spill gate

The aggregation gate above is intentionally able to avoid disk when a compact
group state fits memory. To test a genuinely external operator, the real-spill
gate projects all 30 columns and sorts the complete 10-GiB value stream by the
non-monotonic `(metric, id)` key. It streams and hashes both engines' outputs,
runs them in separate four-CPU/4-GiB/no-swap containers, requires material
spill, and stops either engine after five minutes:

```bash
python -m supertable.engine.benchmarks.real_spill_gate \
  --request-template /tmp/islanddb-spill-4g-Mq8AXiKd/islanddb/request.json \
  --corpus-root /tmp/island-telemetry-corpus/islanddb-wide-v1-10gib-8dd62e12aa33 \
  --output-root /tmp/islanddb-real-spill-next \
  --image kladnasoft/dataisland-core:latest \
  --timeout 300 --sample-interval 1
```

The output root must not already exist. A complete result requires strict
`(metric, id)` order and an exact, batch-independent digest for every projected
column. On timeout the host sampler retains the last RSS, cgroup, block-I/O,
and spill high-water evidence before the container and query-private files are
removed.

The checked-in measured baseline is in
[`RESULTS_2026-08-12.md`](RESULTS_2026-08-12.md). It includes all four physical
tiers and records the one workload where IslandDB was slightly slower, rather
than hiding it behind an aggregate score.

The row-group/range-cache/resource-governor implementation follow-up is in
[`RESULTS_2026-08-13_ROWGROUP_SPILL.md`](RESULTS_2026-08-13_ROWGROUP_SPILL.md),
including fresh cold/warm parity measurements at 512 KiB, 64 MiB, 1 GiB, and
10 GiB.

The latest selective 10-GiB result and adversarial audit are in
[`RESULTS_2026-08-13_10GIB_5_OF_30.md`](RESULTS_2026-08-13_10GIB_5_OF_30.md)
and [`RESULTS_2026-08-13_ISLANDDB_AUDIT.md`](RESULTS_2026-08-13_ISLANDDB_AUDIT.md).

The hard-memory full-value scan is in
[`RESULTS_2026-08-13_50GIB_8GIB.md`](RESULTS_2026-08-13_50GIB_8GIB.md).

The original hard 4-CPU/4-GiB external-spill run is in
[`RESULTS_2026-08-14_10GIB_4GIB_SPILL.md`](RESULTS_2026-08-14_10GIB_4GIB_SPILL.md).
The redesigned native-aggregate result and before/after comparison are in
[`RESULTS_2026-08-14_10GIB_4GIB_SPILL_OPTIMIZED.md`](RESULTS_2026-08-14_10GIB_4GIB_SPILL_OPTIMIZED.md).

The historical genuinely material wide-sort cutoff analysis is in
[`RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL.md`](RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL.md).
The native range-sort redesign and final exact comparison—IslandDB 94.397 s
versus DuckDB 227.170 s—are in
[`RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL_OPTIMIZED.md`](RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL_OPTIMIZED.md).
