# 50 GiB full-value scan under an 8 GiB hard memory limit

## Outcome

Both engines completed every cold and repeat-warm execution with exact result
parity, no spill, no swap, and no cgroup OOM event. IslandDB was faster, but it
used substantially more memory and came close to the kernel limit:

| Metric | DuckDB | IslandDB | IslandDB difference |
|---|---:|---:|---:|
| Fadvise-cold wall | 112.32 s | **96.94 s** | **13.7% lower / 1.16x** |
| Repeat-warm wall median | 117.40 s | **74.06 s** | **36.9% lower / 1.59x** |
| Repeat-warm wall samples | 123.00 / 117.40 / 110.27 s | **77.38 / 71.03 / 74.06 s** | — |
| Cold CPU | 507.04 s | **413.50 s** | **18.4% lower** |
| Warm CPU median | 516.07 s | **353.67 s** | **31.5% lower** |
| Maximum process RSS | **0.657 GiB** | 2.838 GiB | IslandDB **4.32x** |
| Cgroup memory peak | **5.759 GiB (72.0%)** | 7.845 GiB (98.1%) | IslandDB **1.36x** |
| Cgroup headroom at peak | **2,294 MiB** | 159 MiB | IslandDB nearly saturated |
| Cold process block reads | 44.86 GiB | **39.57 GiB** | see I/O caveat |
| Warm block-read median | 43.53 GiB | **28.00 GiB** | see I/O caveat |
| Engine spill / temp bytes | 0 | 0 | equal |
| Swap / OOM / OOM-kill events | 0 / 0 / 0 | 0 / 0 / 0 | equal |
| Exact result | DuckDB oracle | exact match | digest `51dd472a…401f80` |

IslandDB survived, and its process memory stayed bounded rather than growing
with input size. Its 159-MiB cgroup margin is nevertheless too small to treat
this configuration as a robust production guarantee: allocator, file-cache,
or workload-shape variance could consume that margin. DuckDB was slower but
had materially safer memory headroom.

## Workload and enforcement

- 53,687,145,155 logical Parquet bytes (50.000 GiB), 32,068,385 logical rows,
  and 405 distinct path/resource identities.
- All 30 public columns were projected. Selected compressed column chunks were
  53,577,857,700 bytes (49.898 GiB, 99.796% of the source); the exact generated
  decoded estimate was 54,259,707,420 bytes (50.533 GiB).
- The query returned one bounded row: `COUNT(*)` plus direct `MAX(column)` for
  every public column. This forces value-page consumption; `COUNT(column)` was
  rejected because DuckDB can answer it from Parquet null-count metadata.
- Binary extrema use a deliberately narrow, differential-tested unsigned
  lexicographic contract with exact maximum-value-width seals. Binary
  predicates, ordering, grouping, joins, and general projection remain outside
  that capability.
- Each engine ran in an independent `MemoryMax=8 GiB`, `MemorySwapMax=0`
  systemd cgroup. Both used eight workers, a 6-GiB internal workspace, private
  temp directories, and disabled data caches. The spare 2 GiB was reserved for
  Python/native/runtime overhead.
- `cold` is a new scope/process/connection plus best-effort `posix_fadvise`.
  Warm samples reuse the process and connection. A 50-GiB working set cannot
  fit in an 8-GiB page cache, so “warm” does not mean data-resident.

The scalar aggregate has constant-size reduction state and therefore does not
need spill. This result does not generalize to a high-cardinality group-by,
sort, or join; those shapes can require spill or route to Spark.

## Physical-backing limitation

This workstation had only about 31.6 GiB free, so it could not safely generate
a new 50-GiB unique corpus. The run used five distinct hard-link namespaces
over the existing immutable 10.000-GiB corpus. Both engines processed all five
logical copies—the exact row count is five times the unique corpus—but the OS
could reuse the same physical extents.

This is a valid execution, decode, aggregate, memory-limit, and parity test. It
is not a 50-GiB-unique-device benchmark. The high block-read totals prove it
was not a metadata-only scan: DuckDB read 44.86 GiB cold and IslandDB read
39.57 GiB cold. IslandDB's lower repeat I/O is coupled to its much larger page-
cache/cgroup footprint and partly explains its speed advantage. A genuinely
unique 50-GiB corpus may change the timing and I/O ratios.

The harness now also supports a native `50gib` tier. Its safety preflight asks
for 150 GiB free for source, staging, and cache/temp headroom; use a suitably
sized benchmark mount to obtain the unique-device result.

## Validation

The benchmark blocks timing unless DuckDB/IslandDB canonical values and dtypes
match exactly. It also rejects a claimed full scan unless cold process block
reads reach at least 99% of the unique projected bytes. Focused differential
tests cover Binary NULL/empty/prefix/high-bit ordering, multiple files,
all-null/empty inputs, Datetime(us) precision, malformed/missing width seals,
and fail-closed unsupported Binary semantics. On the final tree, all 564 engine
tests, 15 benchmark-harness tests, and 141 affected writer/estimator tests
passed; `compileall` and `git diff --check` were clean.

Raw engine artifacts:

- `/tmp/islanddb-50gib-8gib-duck.json`
- `/tmp/islanddb-50gib-8gib-island.json`
