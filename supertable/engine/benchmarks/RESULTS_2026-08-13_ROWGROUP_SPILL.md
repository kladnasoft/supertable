# IslandDB row-group / spill implementation benchmark — 2026-08-13

The production benchmark runner compared explicit `DUCKDB_LITE` with explicit
native `ISLANDDB`. DuckDB was the oracle and exact columns, pandas dtypes, row
order, and values matched before every timed series.

Workload: narrow full-range projection over deterministic ZSTD Parquet. Source
size is the sum of the original Parquet files. `cold` is the first query in a
fresh worker/engine; `warm` is the median of two repeats in the same worker.
The local OS page cache is uncontrolled, so these figures compare engine and
application-cache behavior, not guaranteed cold physical disk.

| Actual source | DuckDB cold | IslandDB cold | DuckDB warm | IslandDB warm | warm DuckDB / IslandDB |
|---:|---:|---:|---:|---:|---:|
| 529,705 B | 194.67 ms | 111.62 ms | 46.32 ms | 63.12 ms | 0.734x |
| 67,117,351 B | 220.30 ms | 124.72 ms | 73.90 ms | 79.41 ms | 0.931x |
| 1,073,747,303 B | 462.57 ms | 225.29 ms | 291.32 ms | 167.69 ms | **1.737x** |
| 10,737,422,693 B | 3,007.90 ms | 956.33 ms | 2,473.69 ms | 839.67 ms | **2.946x** |

Interpretation: IslandDB has higher repeat-query setup overhead at KB/MB scale;
its narrow parallel scanner wins once skipped columns dominate enough data.
This is not a universal engine ranking. On the 64 MiB corpus, point and 1%
range queries remained faster in DuckDB (49.67 vs 71.79 ms and 52.39 vs
63.63 ms warm respectively). AUTO therefore uses decoded working set, query
shape, spill risk, and cache state rather than original source size alone.

Remote range-cache validation used a synthetic 1.65 MB Parquet object: a cold
projected scan transferred about 595 KB through conditional ranges and the
identical warm scan transferred zero remote bytes. Ceph/MinIO production
figures depend on network, RG/chunk geometry, and proxy caches and must be run
against the deployment endpoint before setting a performance policy.

Correctness/performance gates also exercised:

- exact/stale row-group hint handling (stale means scan all);
- 128-file composite tombstone anti-join against DuckDB;
- forced hard-quota external GROUP BY + ORDER BY spill parity and cleanup;
- bounded Arrow streaming results;
- container CPU/memory detection and concurrent resource reservations;
- provider object mutation, cache corruption, capacity, TTL/LRU, and
  single-flight behavior.

Raw local artifacts used for this run were written under `/tmp` and are not
part of the repository.

## Post-hardening validation

After the measured run, the resource planner, cooperative stream cancellation,
spill merge, row-group footer seal, active-DV proof accounting, and AUTO routing
were hardened further. The final combined local verification completed with
588 engine/cache/row-group/benchmark tests passing (two optional GCS SDK tests
skipped), followed by 234 reader/settings/API tests. A fresh KB/64-MiB run of
all five workloads produced ten exact DuckDB/IslandDB parity matches; IslandDB
won every process-cold sample while DuckDB generally remained faster warm at
those small tiers. The large-tier table above remains the applicable measured
baseline for 1-GiB and 10-GiB narrow scans.
