# Optimized 10-GiB real external-spill comparison — 2026-08-14

## Outcome

IslandDB completed the full-width external sort in **94.397 seconds**, versus
**227.170 seconds** for DuckDB under the same fresh 4-CPU, 4-GiB, no-swap
container boundary. IslandDB was **2.407x faster**, saved **132.773 seconds**,
and reduced wall time by **58.45%**.

Both engines returned all 6,413,677 rows. Their batch-independent, per-column
streaming proof matched exactly:

`cceef358220650cce15e654276d19ed23d8de864ebf2510b387b15fcc1399b3d`

Strict `(metric, id)` ordering, schema, row count, and 10,851,941,484 logical
value bytes were also proven. Both engines materially spilled, neither used
swap or triggered an OOM, and both left zero spill files after completion.

This replaces the pre-redesign result in
[`RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL.md`](RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL.md),
where IslandDB reached its 295-second cooperative deadline without emitting a
row.

## Workload and isolation

```sql
SELECT
  id, event_ts, metric, dimension,
  payload_00, payload_01, payload_02, payload_03, payload_04,
  payload_05, payload_06, payload_07, payload_08, payload_09,
  payload_10, payload_11, payload_12, payload_13, payload_14,
  payload_15, payload_16, payload_17, payload_18, payload_19,
  payload_20, payload_21, payload_22, payload_23, payload_24,
  payload_25
FROM events
ORDER BY metric, id
```

- 81 immutable Parquet files and 1,521 row groups;
- 10,737,429,031 physical source bytes;
- 6,413,677 rows and 30 projected columns;
- 10,851,941,484 logical value bytes through the sort and result stream;
- fresh container per engine, four CPUs, 4-GiB hard cgroup limit, no swap;
- 2-GiB configured engine workspace and private spill directory;
- engine object/range caches disabled;
- best-effort `POSIX_FADV_DONTNEED`: 81 files advised, zero errors for both;
- 300-second host cutoff and 295-second IslandDB cooperative deadline.

The output is larger than either engine workspace, so the gate consumes it as
a bounded Arrow stream. It never materializes the 10.85-GB result in pandas.

## Final telemetry

| Metric | DuckDB | IslandDB |
|---|---:|---:|
| Status | Passed | Passed |
| Query execution wall | 227.170 s | **94.397 s** |
| Container attempt end-to-end | 233.878 s | **100.969 s** |
| Relative speed | 1.000x | **2.407x** |
| CPU time / mean cores | 426.540 s / 1.878 | **250.193 s / 2.650** |
| Peak process RSS | **2.869 GiB** | 3.008 GiB |
| Hard cgroup memory peak | 4.000 GiB | 4.000 GiB |
| Spill high-water | 15.559 GiB | **10.113 GiB** |
| Process block reads | 21.913 GiB | **19.080 GiB** |
| Process block writes | 21.478 GiB | **10.123 GiB** |
| Process write syscalls | 93,891 | **22,751** |
| Cgroup `memory.max` events | 73,864 | **38,928** |
| Cgroup full-pressure time | **6.100 s** | 8.353 s |
| Swap peak | 0 | 0 |
| OOM / OOM-kill | 0 / 0 | 0 / 0 |
| Rows / logical value bytes | 6,413,677 / 10,851,941,484 | Exact match |
| Strict order and digest | Reference | Exact match |
| Spill files after worker | 0 | 0 |

The cgroup peak includes process memory and filesystem page cache. Reaching
4 GiB is therefore not an OOM; the zero OOM counters and completed exact proof
are the relevant survival checks. DuckDB's profile reported 4,015,304,000
bytes of peak buffer memory despite its 2-GiB configured setting, so this gate
proves equal hard cgroup limits, not identical internal allocator behavior.

## What changed

The timed-out implementation produced 85 raw runs and merged them
`85 -> 22 -> 6 -> 2 -> output`. Every merge performed Python scalar
`.as_py()` comparisons and `heapq` operations per row, used roughly one CPU,
and repeatedly rewrote the complete 10.85-GB payload.

The replacement keeps Python as bounded orchestration but moves all row-heavy
work into native NumPy and Arrow kernels:

1. A snapshot-sealed integer domain for the first sort key creates deterministic
   half-open value ranges. NULL has a separate ordered bucket; stale values
   conservatively saturate into edge buckets and cannot be dropped.
2. Each bounded source block computes bucket IDs natively, performs one stable
   `argsort`, and applies exactly one full-table Arrow `take`. Contiguous bucket
   slices are written once to query-private IPC partitions.
3. Range writers use 512-KiB C-buffered I/O. Their aggregate 54-MiB allocation
   is charged below one-sixteenth of operator memory; physical flushes still
   pass through the existing quota, free-space, deadline, and error checks.
4. After input closes, up to four independent partitions use Arrow C++
   `sort_indices` and `take`. One aggregate workspace scheduler prevents four
   individually safe tasks from exceeding the query memory budget.
5. Partitions are emitted in global range order. Stable scatter and stable
   native sort preserve equal-key input order. A hot or oversized interval
   fails back to the existing bounded implementation before output.
6. Fixed-size Parquet payloads remain fixed-width internally. At the public
   stream boundary they become DuckDB-compatible Arrow Binary using small
   shared int32 offset buffers while retaining the payload value buffers
   zero-copy. The former conversion copied all 10,672,358,528 payload bytes.

Cancellation and the monotonic execution deadline are checked around scan,
native kernels, writes, and output. Every error path joins native workers before
removing query-private files. Quota exhaustion, cancellation, footer failure,
partial close, skew fallback, null ordering, signed/unsigned limits, source
mutation, and output buffer lifetime have focused regression coverage.

## Iteration evidence

| Implementation | IslandDB result |
|---|---:|
| Original Python four-way heap merge | Timed out at 295 s; no output |
| First native range scatter, per-bucket `take` | Timed out during partitioning |
| One native scatter per block | 106.289 s, exact parity |
| 64-MiB scatter-block experiment | 148.770 s; rejected |
| Bounded buffered writers | 104.997 s, exact parity |
| Buffered writers + zero-copy Binary output | **94.397 s, exact parity** |

The rejected 64-MiB setting reduced memory but multiplied IPC fan-out and was
therefore not retained.

## Artifacts and validation

The final local artifact is
`/tmp/islanddb-real-spill-20260814-009/comparison.json`; its SHA-256 at report
time was
`13a58d52d2cb24a2da14852d5329b3beb47d6e42dc1ca6a4984605d04a3e7da8`.
The `duckdb/` and `islanddb/` children contain requests, responses, host
samples, cgroup telemetry, and spill high-water observations. `/tmp` is local
benchmark evidence, not durable immutable storage.

Final affected regression run:

```text
239 passed in 72.50s
```

It covers spill primitives, adversarial quota/cancellation cleanup, resource
planning, IslandDB execution, routing, and the real-spill gate. The complete
IslandDB suite independently passed 123/123, and compilation plus
`git diff --check` were clean before this report was added.
