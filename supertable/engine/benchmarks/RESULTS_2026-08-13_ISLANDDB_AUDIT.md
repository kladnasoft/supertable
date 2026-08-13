# IslandDB adversarial audit and 10 GiB optimization result

## Change

Local and whole-object-cache Parquet scans now stay on Polars' Rust-native
multi-file scanner even when the estimator supplies row-group hints. The
scanner performs its own conservative predicate/statistics pushdown. Scanning
a safe superset of an estimator hint cannot remove valid rows; remote objects
continue using exact sealed Arrow fragments so range reads cannot expand into
whole-object downloads.

The audit also closed a lifecycle leak: range-cache construction is now inside
the governor/telemetry cleanup boundary. A cache initialization exception can
no longer retain a resource reservation or sampler thread.

## Exact workload

- 10,737,429,031 bytes of deterministic local ZSTD Parquet.
- 6,413,677 rows, 81 files, 30 public columns.
- Five columns accessed and a contiguous one-percent `id` predicate.
- File pruning retained 2 files (268,433,931 physical bytes).
- Predicate/statistics pushdown selected 16 of 38 candidate row groups.
- Exact selected compressed chunks: 9,055,635 bytes.
- Exact DuckDB/IslandDB result parity checked before timing.

## Before versus after (fresh process, OS cache uncontrolled)

| IslandDB metric | Before | After | Change |
|---|---:|---:|---:|
| Cold wall | 287.38 ms | **168.41 ms** | **41.4% faster** |
| Warm median | 162.88 ms (2 runs) | **111.77 ms** (7 runs) | **31.4% faster** |
| Warm minimum | 153.45 ms | **81.85 ms** | 46.7% faster |
| Cold RSS delta | 64.14 MiB | **54.17 MiB** | 15.5% lower |

## After: DuckDB Lite versus IslandDB

| Wall time | DuckDB Lite | IslandDB | Winner |
|---|---:|---:|---:|
| Cold, fresh process | 219.04 ms | **168.41 ms** | IslandDB by 23.1% |
| Warm median (7 runs) | 117.54 ms | **111.77 ms** | IslandDB by 4.9% |
| Warm range | 100.89–141.71 ms | **81.85–121.75 ms** | IslandDB lower range |

Best-effort physical-cache eviction was also run separately. Both engines
successfully advised the same two files: DuckDB cold was 326.49 ms and IslandDB
cold was **166.07 ms**. Warm medians in that run were 70.29 ms and 120.25 ms,
respectively; this illustrates scheduler/cache variance and is why the primary
warm comparison above uses seven samples without eviction between repeats.

## Verification

- 526 engine, differential, routing, resource, cancellation, cache, spill,
  tombstone-integrity, telemetry, and benchmark tests passed.
- Exact result digest matched DuckDB before every timing series.
- Remote range-cache behavior was not changed by the local scanner fast path.
- Unsupported SQL, schema mismatches, stale/corrupt hints, object mutations,
  spill exhaustion, cancellation, and result-memory bounds remain fail-closed.

