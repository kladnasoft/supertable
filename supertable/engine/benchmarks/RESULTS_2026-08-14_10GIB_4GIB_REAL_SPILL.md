# Real 10-GiB external-spill comparison — 2026-08-14

> Historical pre-redesign result. The completed optimized comparison is in
> [`RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL_OPTIMIZED.md`](RESULTS_2026-08-14_10GIB_4GIB_REAL_SPILL_OPTIMIZED.md):
> IslandDB 94.397 seconds versus DuckDB 227.170 seconds with exact parity.

## Outcome

This benchmark forced both engines to externalize a complete, wide 10-GiB
dataset under a hard 4-GiB memory limit. DuckDB completed the sorted stream in
**246.052 seconds**. IslandDB did not complete before the limit: its cooperative
execution deadline fired after **295 seconds**. The 302.760-second attempt time
also includes host sampling, container inspection, and removal; it is not an
IslandDB query-time measurement.

IslandDB reached 12.007 GiB of live spill, used no swap, and was not OOM-killed.
Its cooperative timeout and cleanup coincided with the host's 300-second stop
boundary, so the artifacts do not prove that `docker stop` played no role in
process termination. They do prove that the worker persisted
`IslandExecutionTimeout` and removed every query-private spill file while
unwinding. Because it emitted no result batch before the deadline, result
parity with DuckDB was **not attempted** and must not be claimed for this run.

Sequential disk bandwidth alone cannot explain the timeline. IslandDB's direct
Arrow scan and native per-run sorting had processed the full input after about
one minute. The remaining operator performs a Python, row-at-a-time, fixed
four-way heap merge and serializes each merge group. It repeatedly reads and
rewrites the complete wide dataset and recorded only about 155 MB/s of
read-plus-write traffic over the host observation window. Cgroup memory
pressure also stalled the process for 47.292 seconds, so no device-saturation
claim is made without device-utilisation and latency telemetry.

## Workload

The query projects every public column and sorts by a deliberately non-monotonic
key with a globally unique tie-breaker:

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

Corpus and result geometry:

- 81 immutable Parquet files and 1,521 row groups;
- 6,413,677 rows;
- 10,737,429,031 physical source bytes;
- 30 projected columns and exactly 1,692 fixed value bytes per row;
- 10,851,941,484 logical value bytes crossing the blocking sort and output;
- `metric = (id * 48271 + 17) % 1000003`, so physical `id` order cannot satisfy
  the requested ordering;
- globally unique `id` makes the complete output order deterministic.

The result itself is larger than either the 2-GiB engine workspace or the
4-GiB container. The gate therefore configured both engines to stream into a
bounded, batch-boundary-independent SHA-256 verifier instead of materializing
the result. DuckDB completed that stream; IslandDB timed out before emitting
its first batch.

## Isolation and acceptance rules

Each engine ran sequentially in a fresh Docker container with:

- four CPUs;
- 4 GiB hard cgroup memory;
- zero usable swap;
- 2 GiB configured engine memory;
- a private spill directory capped at 28 GiB;
- engine object/range caches disabled;
- best-effort `POSIX_FADV_DONTNEED` configured before each execution; DuckDB
  persisted 81 advised files and zero errors, while IslandDB's error response
  did not retain this outcome;
- no network;
- a 300-second host cutoff and a 295-second cooperative IslandDB deadline.

A successful engine result required all 6,413,677 rows, strict monotonic
`(metric, id)` order, a stable digest for every one of the 30 columns, and
material spill. DuckDB's completed reference digest is
`cceef358220650cce15e654276d19ed23d8de864ebf2510b387b15fcc1399b3d`.

## Results

| Metric | DuckDB | IslandDB |
|---|---:|---:|
| Status | **Completed** | **Timed out** |
| Timing | **246.052 s query** | 295 s deadline; 302.760 s attempt end-to-end |
| Complete output rows | 6,413,677 | 0 emitted |
| Strict output order | Proven | Not available |
| Cross-engine result parity | Reference proof | Not attempted |
| Logical value bytes | 10.107 GiB | Full input consumed; zero output emitted |
| Spill high-water | 15.347 GiB | 12.007 GiB at cutoff |
| Process block reads | 22.054 GiB | 21.395 GiB at cutoff |
| Process block writes | 20.979 GiB | 22.063 GiB at cutoff |
| Observed aggregate block rate | 187.8 MB/s query delta | 155.4 MB/s host-window sample |
| Peak process RSS, 1-s host samples | 2.915 GiB | 3.793 GiB |
| Cgroup memory peak | 4.000 GiB | 4.000 GiB |
| Cgroup `memory.max` events | 69,858 | 54,312 |
| Cgroup full-pressure time | 6.559 s | 47.292 s |
| CPU time / mean cores | 442.723 s / 1.799 | No final worker metric after timeout |
| Swap peak | 0 | 0 |
| OOM / OOM-kill | 0 / 0 | 0 / 0 |
| Spill files after worker unwind | 0 | 0 |

The process-I/O figures are worker deltas for completed DuckDB and the last
host-side process sample for timed-out IslandDB. The cgroup reached its hard
limit in both cases because resident process memory and filesystem page cache
are both charged. `memory.max` counts attempted charges at the max boundary; it
is not an OOM count. The throughput figures use different available windows
and are diagnostic, not a strict apples-to-apples device benchmark. DuckDB's
finer in-worker RSS sampler recorded a 2.980-GiB peak.

IslandDB's last sampled process counters already totalled 46.662 GB of block
I/O, versus DuckDB's 46.206 GB completed worker delta, but IslandDB had emitted
no output. It still needed the remainder of `22 -> 6`, all of `6 -> 2`, and the
final merge.

DuckDB's engine profile independently reported 16,473,227,264 bytes of peak
temporary data and 4,290,457,088 bytes of peak buffer memory. Its full result
proof includes one SHA-256 per column as well as row-count, schema, logical-byte,
and strict-order checks.

## IslandDB timeline at the cutoff

| Elapsed | Block reads | Block writes | Live spill | Files | Process RSS |
|---:|---:|---:|---:|---:|---:|
| 60 s | 10.07 GiB | 10.11 GiB | 10.11 GiB | 86 | 2.08 GiB |
| 120 s | 12.70 GiB | 13.11 GiB | 10.23 GiB | 68 | 2.24 GiB |
| 180 s | 15.76 GiB | 16.12 GiB | 10.38 GiB | 50 | 2.39 GiB |
| 240 s | 18.66 GiB | 19.11 GiB | 10.49 GiB | 32 | 2.45 GiB |
| 290 s | 20.94 GiB | 21.55 GiB | 11.50 GiB | 23 | 3.39 GiB |
| 300 s | 21.39 GiB | 22.06 GiB | 12.01 GiB | 23 | 3.67 GiB |

At roughly 60 seconds the 85 initial sorted runs were essentially complete.
The first `85 -> 22` merge level then took about 201 seconds and rewrote the
dataset at only about 54 MB/s. At the cutoff IslandDB had 22 completed runs
totalling 10.853 GB (10.108 GiB) plus one roughly 2.040-GB (1.899-GiB)
partial output from the next level. It had not reached the final output merge.

The first deadline exception occurred in `_merge_run_batches.flush_selection`
before it could yield the selected Arrow batch. During unwinding, closing the
partially written IPC writer observed the same deadline again. It propagated as
`IslandExecutionTimeout`, after which the session removed all partial and
complete query-private files. There was no OOM or OOM-kill.

The attempt artifact's host-side `cooperative_deadline` flag is false because
the 300-second host branch won a race with worker exit. The persisted response
and traceback prove that IslandDB's 295-second cooperative deadline fired.

## Root cause

The run geometry follows directly from the current implementation:

1. `run_target = operator_memory / 8`, which is 128 MiB for the 1-GiB sort
   workspace.
2. Merge input memory is limited to half the operator workspace.
3. The resulting effective fan-in is therefore always four for this shape.
4. The 85 runs require `85 -> 22 -> 6 -> 2 -> final`: three complete
   materialized merge passes followed by a final merge during output.
5. Every selected row in every pass enters Python's `heapq`. Each next key is
   converted with Arrow scalar `.as_py()`. Arrow `take` is used only after
   Python has selected a bounded batch of row indices.
6. Merge groups are executed one after another, so the four-CPU container does
   not provide four-way merge-group parallelism.

If allowed to finish, this topology would move roughly eight times the 10.85-GB
IPC run size through the spill path, in addition to reading the Parquet source.
That is an algorithmic amplification, not a single 10-GB sequential write.
Extrapolating the live first-level merge rate across the remaining complete
passes gives roughly 13–14 minutes total. That is an inference, not a completed
timing, but the five-minute cutoff already proves the current path unacceptable
for this workload.

## Required redesign

The external sort should not be tuned by merely raising the timeout. It needs
an operator-level replacement:

1. Replace scalar Python heap comparison with a native Arrow/Rust/C++ merge or
   a range/radix-partitioned external sort.
2. Use sampled, deterministic key ranges so independent partitions can be
   sorted and emitted in global order by all four CPUs.
3. Write each wide payload row once where possible. Carry compact keys plus row
   references through ordering rather than rewriting all 1,692 bytes on every
   merge level.
4. Choose fan-in from measured per-run cursor memory and file-descriptor limits,
   not the current algebra that fixes it at four; retain hard admission and
   spill-quota accounting.
5. Parallelize independent partition or merge work with bounded read/write
   queues and preserve the existing cancellation, deadline, integrity, and
   cleanup contracts.
6. Keep the five-minute gate blocking. A future result must complete, match all
   30 DuckDB column digests and strict order, use no swap/OOM, materially spill,
   and leave zero files before any performance claim is accepted.

## Artifacts and validation

The preserved local comparison artifact is
`/tmp/islanddb-real-spill-20260814-002/comparison.json`; per-engine artifacts
are in its `duckdb/` and `islanddb/` children. The failed sandbox-only launch at
`/tmp/islanddb-real-spill-20260814-001` did not start a benchmark and is not
part of these results. `/tmp` is not durable or immutable; the comparison file's
SHA-256 at report time was
`3bce1ec75037ae541e76c3ffe7fb3fec3459abc337f71ff35d009c20952ed34b`.

The new gate and streaming verifier are in `real_spill_gate.py` and
`real_spill_worker.py`. Focused gate tests passed 12/12. The production
streaming/capability and engine-routing suites passed 137/137; the combined
settled run passed 143 tests. Compilation and `git diff --check` were clean
before this documentation update.

DuckDB was configured with a 2-GiB engine limit, but its profile reported
4,290,457,088 bytes of peak buffer memory and the worker did not persist a
`current_setting` proof. The benchmark therefore proves identical hard 4-GiB
cgroup limits, not identical effective internal allocation. This does not
change the material-spill conclusion for either engine.
