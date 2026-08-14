# Optimized 10-GiB / 4-GiB IslandDB aggregation benchmark — 2026-08-14

## Outcome

The redesigned IslandDB path completed the exact same cold 10-GiB grouped
workload in **24.749 seconds** inside a hard 4-GiB, four-CPU, zero-swap Docker
container. The previous IslandDB implementation took 14,935.074 seconds
(4 h 08 min 55 s). This is a **603.47x speedup** and a **99.834% wall-time
reduction**.

The independently measured DuckDB oracle took 28.529 seconds under the same
limits. The final Island-only regression gate cryptographically validated and
reused that sealed request and result rather than rerunning DuckDB.
IslandDB was therefore 3.780 seconds faster: **13.25% lower wall time**, or
**1.153x DuckDB throughput**. A separate production-equivalent confirmation
run finished in 23.587 seconds with the same digest.

The result remained exact:

- 1,024 result groups;
- `SUM(id_count) = 6,413,677`, matching the source row count;
- canonical result SHA-256
  `aa8ee7939389b6be670d92edd9eda4755522a4c0ba8e230263cec109b0ec3407`,
  identical to DuckDB and the original IslandDB run;
- zero OOM kills, zero swap, and zero private spill files after cleanup.

## Workload and isolation

This is the same workload and corpus documented in
`RESULTS_2026-08-14_10GIB_4GIB_SPILL.md`:

- 10,737,429,031 physical source bytes;
- 81 Parquet files, 1,521 row groups, and 6,413,677 rows;
- all 30 public columns selected;
- 10,715,571,540 selected compressed bytes and 10,851,941,484 decoded bytes;
- `GROUP BY dimension`, 29 `COUNT` reductions, and `ORDER BY dimension`;
- exactly 1,024 output groups.

The final Island-only gate used a fresh Docker container with `--cpus=4`,
`--memory=4g`, `--memory-swap=4g` (zero usable swap), a 2-GiB internal Island
workspace, four Polars/Island CPU workers, four Island I/O workers, disabled
Island object/range caches, and best-effort `POSIX_FADV_DONTNEED` before the
scan. The gate had a hard five-minute kill and a blocking 100-second target.

## Before, after, and DuckDB

| Metric | Old IslandDB | Optimized IslandDB | DuckDB oracle |
|---|---:|---:|---:|
| Result parity | Exact | Exact | Reference |
| Wall time | 14,935.074 s | **24.749 s** | 28.529 s |
| Selected-byte throughput | 0.000717 GB/s | **0.433 GB/s** | 0.376 GB/s |
| CPU time | 16,646.841 s | **94.910 s** | 93.883 s |
| Mean utilised cores | 1.115 | **3.835** | 3.291 |
| Process physical reads | 52.837 GiB | **10.006 GiB** | 10.012 GiB |
| Process physical writes | 42.809 GiB | **248 KiB** | 24 KiB |
| Engine spill high-water | 18.450 GiB | **247,242 B** | 0 |
| Spill admission quota | 22.740 GiB | **11,685,888 B** | N/A |
| Process RSS peak | 4,126.836 MiB | **1,115.359 MiB** | 355.688 MiB |
| Cgroup memory peak | 4.000 GiB | 4.000 GiB | 4.000 GiB |
| Swap peak | 0 | 0 | 0 |
| OOM / OOM-kill | 0 / 0 | 0 / 0 | 0 / 0 |
| Spill files after close | 0 | 0 | 0 |

The cgroup reached 4 GiB mainly because Linux charged the source filesystem
page cache to the container. Optimized IslandDB process RSS was about 1.09 GiB,
72.97% below the old process peak, but still 3.14x DuckDB's process RSS. The
kernel reclaimed page-cache pages (`memory.events:max = 7,314`) without swap or
OOM.

## What changed

The old operator sorted and rewrote every wide input row, then performed
Python scalar comparison and aggregation through a fixed four-way merge:

```text
89 raw runs -> 23 -> 6 -> 2 -> final
```

That generated about 95.65 GiB of block traffic and used roughly one CPU core.
The redesign makes four related changes:

1. Fully validated sealed row-group statistics now derive a fail-closed integer
   domain bound. For this query the planner proves at most 1,024 group keys and
   estimates a 5,193,728-byte complete aggregate-plus-order state. Missing,
   malformed, partial, or unsealed evidence retains the conservative path.
2. The spill operator performs threaded native Arrow partial hash aggregation.
   It retains or merges compact aggregate states and spills only those compact
   states when required; it no longer raw-sorts and rewrites the 10-GiB input.
3. This safe local full-scan shape uses a direct Arrow Parquet projection,
   avoiding the Parquet-to-Polars-to-Arrow conversion. The final plan was:

   ```text
   Parquet SCAN [81 sealed local sources]
   PROJECT 30/32 COLUMNS
   ARROW NATIVE DIRECT PROJECTION
   EXTERNAL SPILL
   ```

4. A 300-second admitted-execution deadline is now enabled by default. Timeout,
   cancellation, quota failure, or result close releases the input, native
   state, query-private spill directory, governor reservation, and CPU slots.

`EXTERNAL SPILL` describes the bounded external operator, not an obligation to
write the whole input. Since the proven 1,024-group state fits safely in memory,
the correct behaviour is to avoid group-state spill. The measured 247,242-byte
write is the small final ordering run. Artificially forcing 10 GiB to disk for
this low-cardinality query would recreate the planner bug rather than test a
useful execution strategy. Unknown or high-cardinality cases retain a compact-
state spill fallback with conservative candidate-row quota accounting.

## Correctness and adversarial validation

The settled tree passed 285 targeted and broad tests in 146.90 seconds. The
suite includes randomized DuckDB differential checks, nullable and multi-key
groups, duplicate aliases, integer and decimal widening, overflow rejection,
oversized variable-state rejection, forced compact spill, quota failure,
cancellation, deadline cleanup, partial row-group fallback, RBAC/deletion-vector
fallback, and exact stream parity. `compileall` and `git diff --check` also
passed.

The direct Arrow fast path is deliberately narrow: sealed local immutable
sources, one physical table, a full unfiltered grouped scan, no RBAC predicate,
and no active deletion vector. Remote sources, filters, partial row-group
selection, RBAC, and deletion vectors continue through the established Polars
path. They preserve correctness but do not inherit this particular 24.7-second
performance result.

Raw independent gate artifacts are under
`/tmp/islanddb-spill-iterations/optimized-002/attempt-001`. The separate
23.587-second confirmation is under
`/tmp/islanddb-spill-iterations/optimized-004/attempt-001`.

The final blocking gate contains one Island cold sample and no warm sample;
the separate confirmation provides a second cold observation. Cold eviction is
best-effort rather than a kernel guarantee, but the final process performed
10.006 GiB of physical block reads, consistent with a complete physical scan.

Because the preserved pre-redesign request predates integer-domain seals, the
regression gate accepted the `0..1023` dimension proof only after validating it
against the sealed DuckDB oracle. Normal production execution does not use an
oracle: it derives the same bound from complete, footer-bound Parquet statistics
and falls back conservatively when that evidence is unavailable.

This is evidence for this exact low-cardinality `COUNT` workload, not a 10-GiB
sustained spill-throughput measurement and not a universal claim that IslandDB
is faster. A genuinely high-cardinality state that cannot fit still uses
compact-state external sorting; its merge is less parallel than the new
in-memory native hash path and may route to Spark or hit the cooperative query
deadline. That high-cardinality path is correctness-, quota-, cancellation-,
and cleanup-tested, but it was not given a 10-GiB performance claim here.
