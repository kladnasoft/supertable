# 10-GiB spill benchmark under a 4-GiB container — 2026-08-14

> Historical pre-redesign baseline. The optimized implementation and final
> 24.749-second result are documented in
> [`RESULTS_2026-08-14_10GIB_4GIB_SPILL_OPTIMIZED.md`](RESULTS_2026-08-14_10GIB_4GIB_SPILL_OPTIMIZED.md).

## Outcome

IslandDB completed the full-source grouped workload inside a hard 4-GiB,
no-swap cgroup and returned the exact DuckDB result. It did not OOM, it
honoured its spill quota, and it removed every query-private spill file after
the result stream closed.

The safety result is positive, but the performance result is not: IslandDB's
current conservative plan spilled a low-cardinality aggregation that DuckDB
kept in memory. IslandDB took 4 h 08 min 55 s versus 28.53 s for DuckDB and
generated about 95.65 GiB of block I/O.

## Isolation and workload

Each explicit engine ran in its own fresh Docker container using the same
read-only repository and corpus mounts. The relevant hard limits were:

- `--cpus=4` (`cpu.max=400000 100000`);
- `--memory=4g` (`memory.max=4294967296`);
- `--memory-swap=4g` (`memory.swap.max=0`, so no usable swap);
- read-only root filesystem, no network, and a 1,024-PID limit;
- four DuckDB/Polars/Island CPU workers and four Island I/O workers;
- a common 2-GiB internal engine workspace, leaving 2 GiB of cgroup headroom
  for Python/native allocations and filesystem page cache;
- DuckDB external cache and IslandDB whole-object/range caches disabled;
- best-effort `POSIX_FADV_DONTNEED` on all 81 source files before execution.

Corpus and scan:

- 10,737,429,031 physical source bytes (10.000010 GiB);
- 81 independent Parquet files, 1,521 row groups, and 6,413,677 rows;
- 30 public columns selected;
- 10,715,571,540 selected compressed bytes (9.979654 GiB, 99.796% of source);
- 10,851,941,484 exact generated decoded-value bytes (10.106658 GiB).

The query grouped by the generated 1,024-value `dimension` column, counted
every other public column, and ordered the bounded 1,024-row result by
`dimension`:

```sql
SELECT
    dimension,
    COUNT(id) AS id_count,
    COUNT(event_ts) AS event_ts_count,
    COUNT(metric) AS metric_count,
    COUNT(payload_00) AS payload_00_count,
    -- ... COUNT(payload_01) through COUNT(payload_25) ...
    COUNT(payload_25) AS payload_25_count
FROM events
GROUP BY dimension
ORDER BY dimension;
```

DuckDB may in general answer `COUNT(non-null column)` from Parquet metadata.
That did not make this run metadata-only: its process performed 10.012 GiB of
physical block reads after the successful fadvise request. IslandDB's process
I/O includes both its source read and subsequent spill rereads.

## Comparison

| Metric | DuckDB | IslandDB |
|---|---:|---:|
| Completion | Success | Success |
| Result parity | Oracle | Exact match |
| Result | 1,024 rows × 30 columns | 1,024 rows × 30 columns |
| Canonical SHA-256 | `aa8ee7939389b6be670d92edd9eda4755522a4c0ba8e230263cec109b0ec3407` | Same |
| Wall time | 28.529 s | 14,935.074 s (4:08:55.1) |
| Island / Duck wall ratio | 1.00× | 523.51× |
| CPU time | 93.883 s | 16,646.841 s |
| Mean utilised cores | 3.29 | 1.11 |
| Process physical reads | 10.012 GiB | 52.837 GiB |
| Process physical writes | 24 KiB | 42.809 GiB |
| Read + write block traffic | 10.012 GiB | 95.645 GiB |
| Engine spill high-water | 0 | 18.451 GiB |
| Spill admission quota | N/A | 22.740 GiB |
| DuckDB peak buffer memory | 153.642 MiB | N/A |
| Sampled process RSS peak | 355.688 MiB | 4,126.836 MiB |
| Sampled process RSS increase | 167.258 MiB | 3,938.852 MiB |
| Authoritative cgroup memory peak | 4.000 GiB | 4.000 GiB |
| Swap peak | 0 | 0 |
| OOM / OOM-kill events | 0 / 0 | 0 / 0 |
| Spill files after stream close | 0 | 0 |

The Island RSS sampler briefly reported about 30.8 MiB more than the cgroup
limit. Per-process RSS and cgroup accounting have different sampling and shared
page semantics; the kernel-enforced `memory.max`, `memory.peak`, swap, and OOM
counters are authoritative for the hard-boundary claim. Both cgroups reached
their ceiling mainly through filesystem page cache and triggered reclaim
(`memory.events:max`), not an OOM.

## IslandDB behaviour

IslandDB produced the native plan:

```text
Parquet SCAN [/corpus/part-00000.parquet, ... 80 other sources]
PROJECT 30/32 COLUMNS
EXTERNAL SPILL
```

Its 2-GiB internal plan was split into 512 MiB scan workspace, 1 GiB operator
workspace, and 512 MiB result allowance. The spill implementation generated
89 sorted Arrow runs and merged them with a four-way fan-in:

```text
89 -> 23 -> 6 -> 2 -> final streamed merge/aggregate
```

The hard spill high-water was 19,811,074,430 bytes, below the admitted
24,416,868,339-byte quota. Old runs were removed only after each replacement
run was durably completed; the largest old-plus-new overlap passed safely, and
all final runs were removed when the stream closed.

The query was unnecessarily expensive for this data. The planner currently
uses a conservative grouped-state estimate based on decoded bytes and does not
have a sealed distinct-value bound. It therefore external-sorted all 6.4
million wide rows even though only 1,024 group accumulators were needed.
During merge, utilisation averaged roughly one core because comparison and
group updates cross the Python scalar path row by row. Four-way fan-in forced
three materialised merge levels before the final read, amplifying disk I/O.

## Required performance work

1. Carry a validated per-column NDV/cardinality bound into the snapshot and
   resource estimator. This workload should use a bounded in-memory hash
   aggregate, not spill.
2. When the NDV is unknown, locally pre-aggregate each bounded batch/run and
   spill partial aggregate states. For COUNT/SUM/MIN/MAX this can reduce a
   many-million-row input to at most one row per group per partition.
3. Move run merge, key comparison, and aggregate-state updates out of Python
   scalar extraction into a vectorised Arrow/native implementation.
4. Decouple merge fan-in from the run-size formula, admit a higher safe fan-in,
   and parallelise independent merge groups up to the reserved CPU width.
5. Keep the current quota checks, fail-closed errors, private directories,
   atomic run replacement, and unconditional stream-close cleanup.

Raw worker requests and responses remain under
`/tmp/islanddb-spill-4g-Mq8AXiKd/{duckdb,islanddb}` on the benchmark host.
