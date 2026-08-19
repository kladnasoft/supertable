# SuperTable performance audit — 2026-08-19

## Post-fix current-state write A/B

The write regressions described later in this report were investigated and
fixed. The final causal comparison is deliberately **current state before the
fix versus the same current state after the fix**, rather than another
comparison with PyPI 2.4.0. Production inputs, worker code, dependencies,
container limits, and correctness oracles were held constant.

| Variant | Frozen identity |
|---|---|
| Current, pre-fix | `131a4e0abbbc4019e385b8e7ff711f299e61b842` (production based on `f22661d`) |
| Current, post-fix | `1649732cc0417fa7cfc52986112794f8d721f873` (parent `f22661d`) |

Both campaigns used five interleaved fresh containers per variant, four CPUs
pinned to 0–3, a hard 4 GiB memory cgroup, zero usable swap, no network, and
the same pinned image. Population standard deviation and CV are reported.

### Mixed random-write trace

Each sample ran the same 31 production writer calls: one 50,000-row initial
append followed by 14 appends, 11 upserts, and five deletes of 1,000 rows each.

| Metric | Pre-fix mean | Post-fix mean | Change |
|---|---:|---:|---:|
| Timed writer wall | 6.231 s | 4.638 s | **-25.6%** |
| Timed process CPU | 7.215 s | 5.320 s | **-26.3%** |
| Timed cgroup CPU | 7.795 s | 5.745 s | **-26.3%** |
| Timed process peak RSS | 286.5 MiB | 296.2 MiB | +3.4% |
| Whole-container cgroup peak | 218.4 MiB | 207.2 MiB | -5.1% |
| Process physical writes | 1.585 MiB | 1.330 MiB | -16.1% |
| Final logical storage | 1.424 MiB | 1.177 MiB | -17.4% |
| Whole-container elapsed | 14.587 s | 13.093 s | -10.2% |

| Writer wall distribution | Pre-fix | Post-fix | Change |
|---|---:|---:|---:|
| Minimum | 5.975 s | 4.305 s | -28.0% |
| Mean | 6.231 s | 4.638 s | -25.6% |
| Median | 6.118 s | 4.558 s | -25.5% |
| P95 | 6.607 s | 4.942 s | -25.2% |
| Maximum | 6.660 s | 4.956 s | -25.6% |
| Population standard deviation | 0.261 s | 0.247 s | -5.2% |
| CV | 4.19% | 5.33% | +1.14 pp |

| Operation kind | Pre-fix wall/run | Post-fix wall/run | Change |
|---|---:|---:|---:|
| Initial append (1) | 368.1 ms | 383.4 ms | **+4.2%** |
| Append (14) | 1,391.6 ms | 1,053.1 ms | **-24.3%** |
| Upsert (11) | 3,166.0 ms | 2,350.7 ms | **-25.8%** |
| Delete (5) | 1,305.3 ms | 850.4 ms | **-34.8%** |

The post-fix trace produced exactly the same state in all ten measured
containers: 59,000 rows, sum 30,393,196, binary64 average
`0x1.0191c7f99ceebp+9`, minimum -999,996, maximum 999,892, and full sorted
record digest
`804efb9ccb8b179509dc43648737a153ea8c433155be30b563fc614ef77d1a1f`.
All 31 per-step identities, expected row counts, and writer results also match.

### One-million-tombstone fused compaction

The shared immutable input contains 15 Parquet files calibrated to about
15.75 MiB each and exactly 1,000,000 tombstones. Both variants used the same
current fused-compaction algorithm; this isolates the production changes from
the earlier internal-candidate versus fused-algorithm comparison.

| Metric | Pre-fix mean | Post-fix mean | Change |
|---|---:|---:|---:|
| Compaction wall | 15.815 s | 12.932 s | **-18.2%** |
| Compaction CPU | 31.938 s | 29.996 s | **-6.1%** |
| Effective CPU cores | 2.02 | 2.32 | +14.9% |
| Compaction phase peak RSS | 1,315.3 MiB | 1,660.9 MiB | **+26.3%** |
| Final output files | 286 | 22 | **-92.3%** |
| Final encoded bytes | 250.97 MiB | 258.77 MiB | +3.1% |
| Whole-container elapsed | 76.013 s | 75.996 s | -0.02% |
| Whole-container cgroup CPU | 209.942 s | 212.620 s | +1.3% |

| Compaction wall distribution | Pre-fix | Post-fix | Change |
|---|---:|---:|---:|
| Minimum | 14.593 s | 11.627 s | -20.3% |
| Mean | 15.815 s | 12.932 s | -18.2% |
| Median | 15.479 s | 12.845 s | -17.0% |
| P95 | 17.280 s | 14.247 s | -17.6% |
| Maximum | 17.610 s | 14.578 s | -17.2% |
| Population standard deviation | 1.000 s | 0.947 s | -5.3% |
| CV | 6.32% | 7.32% | +1.00 pp |

All ten compaction samples returned the same 13,404,353 live rows and the
same expected aggregates, authoritative projection digest, physical-union
digest, and correctness fingerprint
`5042be32540c38539cbb4a53edddd230fe928007ac2d01d8f4ca3fc08e321da4`.
No sample swapped, OOMed, or was OOM-killed.

### Why Polars and parallelism had not delivered the expected gain

Polars was already active for every user-data output. The regression was in
the work around the codec:

1. System stats and deletion-vector objects were routed through an expensive
   footer-eligibility scan and then PyArrow, even though those objects do not
   need user-data footer statistics.
2. Cumulative stats and deletion-vector histories were repeatedly parsed,
   sorted, and cryptographically validated during one mutation.
3. Overwrite/delete resolution performed 201 sequential candidate-file reads
   and scanned about 985,000 rows in the mixed trace.
4. The strict local DuckDB overlap probe rechecked complete row-id columns on
   every invocation, preventing warm reuse.
5. Footer rows and seals were independently reconstructed and hashed more
   than once.
6. DataWriter and SimpleTable repeated root/leaf/deletion/catalog discovery
   after the writer already held the mutation lease.
7. The fused packer treated the 16 MiB encoded output target as its *total*
   decoded memory budget, divided that over five retained lanes, and therefore
   generated 286 tiny output files.
8. Local atomic publication added required file and directory durability
   fences. Those fences are not removed; their directory work is now safely
   reused without weakening replace durability.

The fix sends system objects through statistics-free Polars with a safe
PyArrow fallback; carries validated stats/tombstone metadata forward; computes
footer rows and seals once; adds an inode-pinned, mutation-fenced local row-id
integrity cache and an adaptive DuckDB probe threshold; threads the exact
storage backend through both probe and fallback; reuses the already fenced
catalog/leaf context; caches only fully fsynced directory ancestry; and sizes
the implicit fused decoded budget from the encoded target plus the effective
cgroup memory limit. Explicit table budgets remain authoritative.

The result is a substantial recovery, but not a universal 50% improvement.
The mixed workload is 25.6% faster and the measured compaction phase is 18.2%
faster. End-to-end tombstone-attempt time is effectively unchanged because the
independent full correctness readback dominates and was slower in these runs.
The remaining regressions are the one-off initial append (+4.2%), random-trace
process RSS (+3.4%), compaction RSS (+26.3%), and a small increase in final
compaction bytes (+3.1%). The higher compaction RSS is the intentional cost of
packing 22 useful files instead of 286 tiny files, and remains well inside the
hard 4 GiB boundary.

Post-fix artifacts:

| Artifact | Path |
|---|---|
| Random-write report | `/tmp/supertable-write-regression-afterfix-20260819/random/summary.md` |
| Random-write JSON/CSV | `/tmp/supertable-write-regression-afterfix-20260819/random/consolidated.json`, `comparison.csv` |
| Random-write manifest | `/tmp/supertable-write-regression-afterfix-20260819/random/manifest.json` |
| Tombstone campaign | `/tmp/supertable-write-regression-afterfix-20260819/tombstone/current-pre-vs-post-fused-20260819/campaign.json` |

The remainder of this report preserves the **pre-fix** read/public-version
audit and its then-current conclusions for provenance. Its recommendations
about the 286-file and random-write regressions have been superseded by this
section.

## Pre-fix executive verdict (historical)

The audited build is **correctness-clean in every completed cross-engine and
cross-version comparison**, but it is **not performance-clean enough to call an
unqualified release win**.

- DuckDB and IslandDB returned exactly identical results in all 27 completed
  read cases across 100 MiB, 1 GiB, and 10 GiB. IslandDB had the lower observed
  warm median in 26 of 27 cases.
- The exception was the 1 GiB grouped full-width scan: IslandDB's warm median
  was 1.442 s versus DuckDB's 0.935 s, 54.3% slower, and its median warm RSS was
  1,345 MiB versus 307 MiB. This workload spilled only 0.236 MiB, so it is a
  retained-state/grouping regression, not a material external-spill result.
- All 15 completed random-write traces—five public PyPI 2.4.0, five internal
  2.4.1-candidate, and five current-worktree runs—produced the same records,
  aggregates, and full digest. The current worktree was nevertheless 28.1%
  slower than the internal candidate and 73.4% slower than public 2.4.0 by mean
  timed writer wall.
- In the primary four-pair 1,000,000-tombstone campaign, current fused
  compaction used 49.9% less phase peak RSS and wrote 16.55% fewer encoded
  bytes, but took 49.4% longer and produced 286 files instead of 15. That file
  multiplication is a release concern.
- A bounded material-spill probe established that IslandDB can complete the
  1 GiB, 30-column external sort and repeatedly emits the same complete proof.
  DuckDB materially spilled first, then exhausted its configured allocator
  under every tested constrained workspace before returning a result. This
  subtest has no cross-engine parity proof or valid timing ratio and is reported
  as feasibility evidence only.

My release recommendation is to address or explicitly accept the random-write
regression and fused-compaction file geometry before publishing. The read path
is broadly favorable, with one concrete grouped-scan regression to investigate.

## Version identity

The version premise required correction. At audit time, public PyPI reports
`supertable==2.4.0` as latest; there is no public 2.4.1 artifact.

| Label used here | Identity | Public? | Measured scope |
|---|---|---:|---|
| Public 2.4.0 | Wheel SHA-256 `4d3e64c729862b33be749fe33c148ee92fe9a14d925e17be8a1685b7e823ec59` | yes | Random writes |
| Internal 2.4.1 candidate | `426e94b4040976c475c435c6e29a68085676c839` | no | Random writes and tombstone compaction |
| Current worktree | Based on `f22661d0261d05d2c3681f9f996e27f95ebd4189`, package string 2.4.1 | no | Reads, random writes, tombstone compaction, spill feasibility |

The public wheel matches the v2.4.0 repository code except for packaged version
metadata. The internal 2.4.1 candidate is the repository boundary named
`harden tombstone lifecycle - v2.4.1`; it is neither tagged nor published.

Read and tombstone attempt wrappers captured HEAD plus a digest of tracked
changes. Untracked benchmark-tool contents were listed, not recursively hashed.
The random-write worker unfortunately recorded `git_revision: null`; its
`requested_revision` is caller-supplied. Therefore the current random arm is
described as a dirty checkout based on f22661d, not as a commit-exact artifact.
The current dirty paths observed during the audit were benchmark tooling, not
the production writer implementation, but that is post-run corroboration rather
than a per-attempt source fence.

## Experimental boundary

All primary read and tombstone attempts used fresh containers with:

- 4 CPUs, pinned to host CPUs 0–3;
- 4,294,967,296 bytes of cgroup memory;
- zero usable swap;
- 1,024 PID limit;
- read-only root filesystem, no network, and no new privileges;
- image `kladnasoft/dataisland-core@sha256:4c14444ebaaba83c0536d38891a7ddd9cfd3f2073d6472356cc6215be6bace42`;
- Python 3.11.15, DuckDB 1.5.4, PyArrow 18.1.0, Polars 1.43.2,
  NumPy 2.4.6, Pandas 2.2.3, Redis client 5.3.1, and sqlglot 26.33.0.

The normal read matrix gave each engine a 2 GiB configured workspace inside the
hard 4 GiB container. Random-write reports retain affinity, cgroup counters, and
zero-OOM evidence, but their campaign did not retain Docker inspect/container
IDs or per-run `memory.max`/`swap.max`; its hard-limit statement is therefore a
campaign declaration rather than independently provable for every run.

The host is a VMware VM exposing an Intel Core i7-12700H, Linux 6.8, and a
rotational ext4 virtual disk that was approximately 96% full. Hardware
performance counters were unavailable (`perf_event_paranoid=4`). Telemetry
therefore uses wall time, process and cgroup CPU time, effective cores, sampled
RSS, Arrow allocation, `/proc/self/io`, cgroup memory/CPU/I/O/PSI, engine native
profiles, cache footprint, and spill-directory high-water marks.

## Correctness gates

| Campaign | Completed evidence | Correctness result |
|---|---:|---|
| Read matrix | 27 DuckDB/IslandDB pairs | Exact columns, dtypes, rows, values, and digests |
| Generated aggregate oracle | 3 tiers × 2 engines | Count/sum/min/max and derived average match formula oracle |
| Random writes | 15 runs, 31 writes each | Same trace, per-step counts/results, aggregates, and full-record digest |
| Tombstone compaction | 8 primary runs; 2 pilot runs | Expected live set, aggregates, authoritative digest, and physical-union digest match |
| Forced material spill | IslandDB completed; DuckDB did not | Island proof complete and repeatable; no cross-engine parity claim |

Random-write final state in every completed version run:

| Check | Result |
|---|---:|
| Rows | 59,000 |
| Sum | 30,393,196 |
| Average | 515.138915254237 (`0x1.0191c7f99ceebp+9`) |
| Minimum / maximum | -999,996 / 999,892 |
| Full sorted-record SHA-256 | `804efb9ccb8b179509dc43648737a153ea8c433155be30b563fc614ef77d1a1f` |

## DuckDB versus IslandDB reads

The corpora were deterministic, schema-identical, and physically sized as
follows. Units are IEC even where the original request said MB/GB.

| Tier | Physical bytes | Rows | Files |
|---|---:|---:|---:|
| 100 MiB | 104,862,568 | 62,553 | 13 |
| 1 GiB | 1,073,755,340 | 641,277 | 17 |
| 10 GiB | 10,737,429,031 | 6,413,677 | 81 |

Each timing series contains one fadvise-cold sample followed by five warm
samples in one fresh container. A separate fresh container performed parity
before timing. The table below is warm median; `D/I` is DuckDB time divided by
IslandDB time, so values above one favor IslandDB.

`full_scan` reads and consumes every public value but reduces the result to a
fixed-size aggregate; it is a full-input scan, not a 10 GiB client-result
materialization. The separate forced-spill gate streams full rows without
retaining the result in the harness.

| Tier | Workload | DuckDB | IslandDB | D/I |
|---|---|---:|---:|---:|
| 100 MiB | no match | 68.58 ms | 39.79 ms | 1.724× |
| 100 MiB | point | 48.18 ms | 40.18 ms | 1.199× |
| 100 MiB | range 1% | 52.04 ms | 34.18 ms | 1.522× |
| 100 MiB | range 1%, five columns | 59.14 ms | 35.12 ms | 1.684× |
| 100 MiB | range 10% | 56.02 ms | 36.86 ms | 1.520× |
| 100 MiB | projection | 68.14 ms | 43.95 ms | 1.550× |
| 100 MiB | aggregate stats | 93.50 ms | 53.39 ms | 1.751× |
| 100 MiB | full scan | 250.72 ms | 178.16 ms | 1.407× |
| 100 MiB | grouped full-width scan | 289.31 ms | 220.41 ms | 1.313× |
| 1 GiB | no match | 119.55 ms | 49.03 ms | 2.438× |
| 1 GiB | point | 56.65 ms | 39.37 ms | 1.439× |
| 1 GiB | range 1% | 64.20 ms | 39.50 ms | 1.625× |
| 1 GiB | range 1%, five columns | 72.33 ms | 41.47 ms | 1.744× |
| 1 GiB | range 10% | 72.60 ms | 48.81 ms | 1.487× |
| 1 GiB | projection | 131.48 ms | 65.79 ms | 1.998× |
| 1 GiB | aggregate stats | 132.82 ms | 70.16 ms | 1.893× |
| 1 GiB | full scan | 890.01 ms | 769.35 ms | 1.157× |
| 1 GiB | grouped full-width scan | 934.91 ms | 1,442.18 ms | 0.648× |
| 10 GiB | no match | 455.30 ms | 95.50 ms | 4.768× |
| 10 GiB | point | 61.96 ms | 42.07 ms | 1.473× |
| 10 GiB | range 1% | 77.19 ms | 54.85 ms | 1.407× |
| 10 GiB | range 1%, five columns | 100.18 ms | 53.36 ms | 1.878× |
| 10 GiB | range 10% | 141.64 ms | 77.73 ms | 1.822× |
| 10 GiB | projection | 625.50 ms | 328.10 ms | 1.906× |
| 10 GiB | aggregate stats | 689.20 ms | 335.53 ms | 2.054× |
| 10 GiB | full scan | 27.217 s | 24.172 s | 1.126× |
| 10 GiB | grouped full-width scan | 28.008 s | 24.186 s | 1.158× |

The two 100 MiB values shown for `point` and `range_1pct_5cols` are sealed
replacements for early timing pairs whose dirty-source digests differed across
engines. Their sealed speedups moved from 1.434× to 1.199× and from 1.168× to
1.684× respectively, demonstrating why the results should be read as observed
directions and rounded measurements rather than high-precision universal
ratios.

### Read telemetry highlights

- Across the 108 effective measured attempts there were zero OOMs, zero OOM
  kills, and zero swap bytes.
- Six 10 GiB wide attempts repeatedly reached `memory.max`, totaling 236,193
  reclaim events, but still completed. Five attempts were CPU-throttled for a
  combined 1.552 seconds.
- The 1 GiB grouped-scan regression is the clear outlier. IslandDB warm CPU
  median was 3.668 s versus DuckDB 3.068 s; effective cores were 2.817 versus
  3.262; process RSS median was 1,345 MiB versus 307 MiB. IslandDB's native
  spill was only 247,242 bytes. Retained Arrow/allocator state plus lower
  parallel utilization is a plausible explanation, not a proven root cause.
- At 10 GiB, full-width runs reached the hard cgroup boundary and experienced
  significant memory and I/O pressure. These are valid constrained-container
  results, not unconstrained engine peaks.
- The detailed read report contains cold and warm min/mean/median/max/p95,
  standard deviation, CV, CPU, effective cores, RSS, logical/physical I/O,
  throughput, PSI, cache, Arrow, native phase, and spill telemetry for every
  series.

The effective set is pairwise source-homogeneous for all 27 cases, but it is
not one globally identical dirty build: 100 effective attempts use the final
tracked-diff digest and eight early attempts use two older tracked-diff
digests. Their patch bodies and untracked contents were not archived.

## Material-spill feasibility

The normal workload named `spill_group` is not a material-spill benchmark. A
separate bounded gate therefore sorted all 641,277 rows and 30 public columns,
streamed a batch-independent digest in `(metric,id)` order, required at least
64 MiB of observed spill, capped each attempt at 4 GiB of temporary storage,
and retained the same hard 4 GiB/no-swap container boundary.

The existing 10 GiB real-spill gate safely requires 30 GiB free; only about
12 GiB was available, so it was not run. The new 1 GiB gate used lower
configured engine workspaces to force external execution. Those settings are
planning/allocator budgets, **not process RSS caps**; the symmetric hard memory
limit is the 4 GiB cgroup.

| Attempt | Engine settings | DuckDB | IslandDB | Cross-engine proof |
|---|---|---|---|---|
| 001 | 4 threads, 256 MiB | path-setup failure | path-setup failure | none; excluded |
| 002 | 4 threads, 256 MiB | allocator OOM before result | 17.083 s, 1.090 GB spill | none |
| 003 | 4 threads, 512 MiB | spilled ~962 MB, then allocator OOM | 12.679 s, 1.086 GB spill | none |
| 004 | 4 threads, 640 MiB | spilled ~912 MB, then allocator OOM | 12.518 s, 1.086 GB spill | none |
| 005 | 2 threads, 512 MiB | spilled ~980 MB, then allocator OOM | 12.131 s, 1.086 GB spill | none |

Attempt 005's IslandDB result contained exactly 641,277 rows and
1,085,040,684 logical value bytes with digest
`309e82d80e5257cb3d1e841352611143e42db2eea4820459a60200489983cf75`.
It used 21.000 s process CPU (1.731 effective cores), peaked at about 1.447 GB
worker RSS, and recorded no cgroup OOM. DuckDB's final attempt recorded about
818.5 MB RSS and 979.6 MB temporary high-water before its internal allocator
failed; it also recorded no cgroup OOM.

This is useful operational evidence—IslandDB completed a forced external sort
that DuckDB did not under the tested constrained workspaces—but it is not a
valid speed comparison and does not prove that both engines would return the
same values. IslandDB's repeatable digest is self-consistency evidence, not an
independent oracle. Attempts 001–005 are preserved as setup/tuning/feasibility
artifacts and excluded from the 27-case paired read performance table.

## Random-write comparison

Each run performed a 50,000-row initial append and 30 deterministic 1,000-row
operations: 14 appends, 11 upserts, and five deletes. Statistics below are over
five runs per version and sum only the 31 timed production `DataWriter` calls.

| Metric | Public 2.4.0 | Internal 2.4.1 | Current worktree | Current/internal |
|---|---:|---:|---:|---:|
| Writer wall mean | 4.045 s | 5.475 s | 7.015 s | +28.1% |
| Writer wall min / median / max | 3.884 / 3.967 / 4.374 s | 5.356 / 5.476 / 5.590 s | 6.840 / 7.023 / 7.144 s | — |
| Writer wall p95 / stddev / CV | 4.316 s / 0.177 s / 4.39% | 5.586 s / 0.095 s / 1.73% | 7.140 s / 0.113 s / 1.61% | — |
| Process CPU mean | 4.399 s | 6.267 s | 7.688 s | +22.7% |
| Cgroup CPU mean | 4.869 s | 6.805 s | 8.248 s | +21.2% |
| Timed peak RSS mean | 278.2 MiB | 275.7 MiB | 292.1 MiB | +6.0% |
| Process physical writes mean | 1.884 MiB | 1.780 MiB | 1.587 MiB | -10.8% |
| Final logical storage mean | 1.725 MiB | 1.611 MiB | 1.426 MiB | -11.5% |

Pairwise mean writer-wall ratios are internal/public 1.353× (+35.3%),
current/public 1.734× (+73.4%), and current/internal 1.281× (+28.1%). The
current/internal comparison is the strongest timing evidence because those ten
runs used an interleaved ABBA/AB schedule. Public 2.4.0 was measured later as a
separate block, so public ratios remain exposed to host-time and page-cache
bias.

| Operation | Public | Internal 2.4.1 | Current | Current/internal |
|---|---:|---:|---:|---:|
| Append, 14/run | 71.62 ms | 67.19 ms | 108.33 ms | 1.612× |
| Delete, 5/run | 173.25 ms | 272.88 ms | 308.78 ms | 1.132× |
| Initial append, 1/run | 185.73 ms | 169.81 ms | 394.13 ms | 2.321× |
| Upsert, 11/run | 180.97 ms | 272.71 ms | 323.67 ms | 1.187× |

Production profiler labels point to the largest changed areas, but many phases
are nested and semantics changed between revisions, so they must not be added
or treated as causal attribution. Current versus internal mean phase sums
include snapshot 252.4 versus 62.5 ms, `write_parquet` 559.7 versus 302.6 ms,
`build_stats` 1,180.5 versus 833.1 ms, and `write.upload_bytes` 213.7 versus
14.6 ms. Current also adds a 113.4 ms codec-check phase. These are the first
places to profile, not proof of root cause.

Public 2.4.0 required an offline DuckDB `httpfs` seed for final readback because
networking was disabled. It was installed before timed calls and excluded from
logical table storage. Delayed setup writeback contaminated public cgroup block
write telemetry, so process-level timed I/O—not public cgroup-I/O ratios—is the
primary comparison.

## One-million-tombstone compaction

The shared immutable input contained 15 Parquet files calibrated to about
15.75 MiB each, 247,629,517 encoded bytes total, 14,404,353 rows, and exactly
1,000,000 distinct tombstones touching every file. Every completed run returned
13,404,353 live rows with the same count, null counts, minima, maxima, decimal
averages, authoritative digest, and physical-union digest.

The primary distribution is the prespecified four-pair repeated campaign.
One earlier matching pilot pair is reported only as pooled sensitivity.

| Primary n=4 mean [min, max] | Internal 2.4.1 two-phase | Current fused | Change |
|---|---:|---:|---:|
| Elapsed excluding invalid final metadata | 77.765 s [76.003, 79.561] | 78.191 s [77.450, 78.760] | +0.55% |
| Compaction wall | 10.908 s [9.404, 12.355] | 16.296 s [15.911, 17.062] | +49.40% |
| Compaction CPU | 31.363 s [30.883, 31.694] | 33.040 s [32.409, 34.345] | +5.35% |
| Compaction phase peak RSS | 2.554 GiB [2.187, 2.871] | 1.280 GiB [1.265, 1.304] | -49.90% |
| Logical bytes read | 236.158 MiB | 236.158 MiB | 0% |
| Logical bytes written | 300.725 MiB | 250.967 MiB | -16.55% |
| Final files | 15 | 286 | +1,806.7% |

Pooling the one protocol-compatible pilot changes the headline deltas only to
+0.36% elapsed-ex-metadata, +47.65% compaction wall, +4.80% CPU, and -50.21%
phase peak RSS; the conclusion is unchanged.

Candidate phases averaged 11.025 s for tombstone rewrite and approximately
0.002 s for its subsequent small-file merge. Current fused compaction averaged
16.281 s in the pooled analysis. Candidate final metadata averaged 90.751 s,
but that is not a valid local comparison: its environment incorrectly selected
an unreachable MinIO endpoint and spent the time retrying existence checks.
The apparent 53.7% current end-to-end win must therefore not be published as a
data-plane speedup.

Current fused compaction halves its phase RSS and avoids intermediate encoded
bytes, but its approximately 3.2 MiB decoded packing slot produced 286 outputs
with a median encoded size near 1.28 MiB. The candidate produced 15 outputs,
all over the 16 MiB target. The file-count regression is likely to amplify
future metadata and read costs.

A current two-phase exploratory run was manually stopped after 1,244.77 s. It
was not a timeout or OOM: Docker `OOMKilled` and all cgroup OOM counters were
zero, with only 2.214 GiB peak cgroup memory. Phase B had emitted 28 files and
only 17.1% of live rows. Its observed output cadence projects roughly
1.96–3.48 hours, but that is a projection from an incomplete run and is not in
any completed distribution.

## Pre-fix release actions (historical)

1. **Treat write latency as a release regression unless explicitly accepted.**
   Profile the new snapshot, codec-check, upload, stats, authorization, and
   Redis publication work with a production-shaped Redis/storage setup.
2. **Fix fused output packing before making it the release default.** The
   current 286-file output defeats the 16 MiB compaction objective despite
   using much less memory and fewer encoded bytes.
3. **Do not fall back to the current two-phase phase-B implementation** for
   this geometry; its row-at-a-time clean-file path is operationally
   pathological.
4. **Investigate the 1 GiB grouped-scan retained-state regression** using heap
   or Arrow allocation profiles and fresh-container-per-warm-sample runs.
5. **Keep a separate forced-spill qualification gate.** The present evidence
   proves IslandDB completion and DuckDB allocator failure under several tight
   workspace configurations, not paired speed or correctness. A future run
   should use a larger disk and the existing 10 GiB/2 GiB-workspace gate, or a
   predeclared workload that forces both engines to finish under the same hard
   cgroup without iterative tuning.
6. **Publish from an immutable source artifact.** Rerun the final release wheel
   with per-attempt source-tree hashes and Docker inspect retained for writes.
7. Add release thresholds for correctness, random-write wall/CPU, compaction
   RSS, output file count/median size, zero OOM/swap, and material-spill
   completion. Compare medians and distributions, not a single ratio.

## Evidence and limitations

- Read headline evidence is 108 effective containers: 64 retained selective,
  36 wide, and eight sealed replacements. The stored read evidence is 116
  containers because it also retains eight superseded attempts. A broader
  inventory reaches 124 only when eight setup smoke attempts are included.
- “IslandDB had a lower observed median in 26/27” describes this run. Each
  series has five correlated warm samples in one process, not five independent
  containers, so it is not a statistical generalization.
- Fresh containers isolate process and cgroup state but do not flush the host
  page cache. Fadvise cold mode is best-effort; logical bytes and corpus hashes
  are more stable than physical-read deltas.
- The public random-write block was later and non-interleaved. Its raw reports
  lack per-run Docker inspect and observed Git revision. These provenance gaps
  are why current/public ratios are qualified.
- Tombstone comparison changes both revision and algorithm; it does not isolate
  a single code change. Whole-attempt RSS includes independent correctness
  scans; phase RSS is the relevant compaction comparison.
- No public 2.4.1 wheel existed, so no claim in this report calls the internal
  candidate an official release.

## Artifacts

The machine-readable artifacts contain the complete telemetry distributions
and raw attempt references.

| Artifact | Path |
|---|---|
| Read report | `/tmp/supertable-performance-audit-20260818/read/audit-report.md` |
| Read machine summary | `/tmp/supertable-performance-audit-20260818/read/audit-summary.json` |
| Sealed 100 MiB rerun | `/tmp/supertable-performance-audit-20260818/read/selective-100mib-sealed.json` |
| Final bounded-spill attempt | `/tmp/supertable-performance-audit-20260818/read/bounded-spill-1g-2threads-512m-005/comparison.json` |
| Three-way random-write report | `/tmp/supertable-performance-audit-20260818/write/random-public-240/three-way-consolidated.md` |
| Three-way random-write JSON/CSV | `/tmp/supertable-performance-audit-20260818/write/random-public-240/three-way-consolidated.json`, `.csv` |
| Tombstone pooled-sensitivity report | `/tmp/supertable-performance-audit-20260818/write/tombstone-consolidated.md` |
| Tombstone JSON/CSV | `/tmp/supertable-performance-audit-20260818/write/tombstone-consolidated.json`, `.csv` |
| Verified artifact manifest | `/tmp/supertable-performance-audit-20260818/verified-artifact-manifest.json` |
| Independent validity report | `/tmp/supertable-performance-audit-20260818/BENCHMARK_VALIDITY_AUDIT.md` |

Final artifact hashes are recorded in the verified manifest. The manifest does
not hash itself, avoiding recursive self-reference.
