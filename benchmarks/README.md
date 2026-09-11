# SuperTable performance suites

Read and write benchmarks that produce **comparable, sealed** telemetry, so the
behaviour and speed of one version can be measured against another instead of
argued about.

The suites live outside the `supertable` package on purpose — packaging only
includes `supertable*`, so nothing here ships in the wheel.

```
python -m benchmarks setup    --profile local --scale full     # build the dataset once
python -m benchmarks read     --profile local --scale full
python -m benchmarks write    --profile minio --duration 60
python -m benchmarks all      --profile minio --scale full
python -m benchmarks matrix   --scale full                     # both profiles, one process each
python -m benchmarks compare  <baseline.json> <candidate.json>
python -m benchmarks teardown --profile minio [--include-dataset]
```

## The two things every scenario records

**Latency / throughput.** Read scenarios run N iterations after a discarded
warmup and report min / p50 / p95 / max / stdev. Write throughput phases are
time-boxed and report rows per second.

**A correctness seal.** A hash of the logical result. This is what makes the
telemetry a *seal* rather than a stopwatch: a version that gets faster by
returning different rows fails the comparison instead of looking like a win.
The hash ignores row and column order (reads carry no ordering guarantee),
normalises `3` and `3.0` (the read path coerces ints to floats through numpy),
rounds floats to 6 places (the last bit of a sum depends on aggregation order),
and excludes `__rowid__` and `__timestamp__` (write-time and allocation-order
dependent, so they could never be stable).

Scenarios that pin an order (`ORDER BY ... LIMIT`) seal the ordered form, and
those queries carry a unique tiebreaker (`ORDER BY event_ts DESC, event_id
DESC`). Without it, ties at the LIMIT cutoff make *which* rows come back
arbitrary, and the seal would raise false alarms against unchanged code.

## Profiles

`local` and `minio` are measured **separately and never compared to each
other**. LOCAL isolates library and CPU cost; MINIO shows the object-store
cost you actually feel. Both matter: on this machine a serial append runs
~3.4× faster on LOCAL, while at 8-way parallelism the two are within a few
percent — parallelism hides most of the PUT latency, which is exactly the kind
of thing a single-backend suite would hide from you.

The backend is bound from `STORAGE_TYPE` **before** `supertable` is imported,
because the settings singleton is built at import time. That is why `matrix`
runs each profile in its own process rather than switching in-flight, and why
the CLI refuses to run if `supertable` was imported too early.

Each profile gets its own supertable (`perf_local`, `perf_minio`) under the
`perf_bench` organization. Redis is shared between backends while the data is
not, so a shared catalog namespace would leave one profile's pointers aimed at
paths that only exist in the other's storage.

## Read scenarios

Selectivity is a property of the generated data, not an assertion about it: the
rows span exactly 300 days ending at a **fixed** anchor (never `now()`, which
would move the window between runs and break every seal), and each file owns a
contiguous slice of that span.

| scenario | what it measures |
|---|---|
| `random_1000_by_key` | 1000 reproducibly-random keys scattered across the table — random access, prunes nothing |
| `agg_30d_by_country` | 30-day SUM + GROUP BY; **verified** ~10% of rows |
| `agg_24h_by_country` | 24-hour SUM + GROUP BY; **verified** <1% of rows |
| `top_1000_by_date_desc` | `SELECT * ORDER BY date DESC LIMIT 1000`, order sealed |
| `top_10000_by_date_desc` | same at 10000 |
| `count_star_full` | unprunable floor — every file must be read |
| `point_lookup_single_key` | single row by key — latency floor |
| `distinct_users_30d` | `COUNT(DISTINCT)` on a high-cardinality column — hash-aggregate pressure, not I/O |
| `dimension_filter_full_scan` | non-time filter, nothing prunable |
| `two_dim_group_by_30d` | wider grouping key |
| `narrow_window_single_file` | window inside one file — best achievable time prune |
| `filtered_order_limit` | filter + order + limit in one plan |
| `prune_30d_window` | **pruning**: may keep at most ~15% of files |
| `prune_24h_window` | **pruning**: should survive on one file |
| `prune_key_point` | **pruning**: key equality → one file |
| `prune_no_predicate` | **pruning control**: nothing may be pruned — catches over-eager pruning |

Pruning is asserted against the library's own plan stats (`FILES_BEFORE_PRUNE`,
`FILES_PRUNED`, `FILES_KEPT`), including the arithmetic identity
`before == pruned + kept`, rather than inferred from timings.

## Write scenarios

| scenario | what it measures |
|---|---|
| `parallel_8_distinct_tables` | 8 threads → own tables, 1000–10000 row batches, time-boxed; per-table and total ingest |
| `parallel_4_same_table` | 4 threads contending on ONE table; **verifies** rows written == rows readable == distinct keys |
| `serial_append_only` | uncontended single-threaded baseline |
| `lifecycle_append_delete_update_sealed` | fixed append + delete + update, then **hash the table** |
| `small_write_latency` | many single-row writes — per-write fixed cost |
| `large_batch_write` | one large append — per-row cost, fixed cost amortised |
| `upsert_existing_keys` | merge-on-read; row count must stay flat |

**Why the lifecycle phase exists.** Time-boxed phases cannot be sealed: how many
rows land depends on how fast the engine is, so their row counts are not
reproducible. What they *can* prove is consistency — a parallel writer that
loses or duplicates rows fails even when it is fast. Sealing therefore needs a
phase that does a *fixed* amount of deterministic work, which is what the
lifecycle phase is. Its hash is byte-identical across runs and across both
storage backends.

## Reading a comparison

```
python -m benchmarks compare benchmarks/results/local/read-3.0.2-full.json \
                             benchmarks/results/local/read-3.1.0-full.json
```

Exit code: `2` = correctness drift, `1` = performance regression, `0` = clean.

Correctness drift is reported **first and separately**, and a scenario whose
seal changed is not given a timing verdict at all — a speed delta on changed
behaviour is meaningless.

Three things stop the tool crying wolf:

* **Noise band.** A verdict must clear two standard deviations of the
  baseline's own run-to-run spread, not just a fixed percentage. Short
  scenarios on a busy machine swing tens of percent between identical runs.
* **Single-sample widening.** Write phases run once, so they have no measured
  spread; their bar is widened rather than guessed at.
* **Throughput, not wall time, for time-boxed phases.** A phase that runs for a
  fixed 5 seconds always takes ~5 seconds; what changed is how much work fit
  inside it. Those scenarios are judged on rows/second.

The tool refuses to compare quietly across different backends, CPUs or dataset
fingerprints — it prints the mismatch instead of a confident number. It also
flags a run measured on a dirty working tree, since that result corresponds to
no commit.

## Baselines

Result files are committed, so comparing two versions is just reading two files
out of git history:

```
benchmarks/results/<profile>/<suite>-<version>-<scale>.json
```

Re-running the same version overwrites its file, which is intended: the current
version's numbers should reflect the current machine. Historical baselines come
from git, e.g. `git show <rev>:benchmarks/results/local/read-3.0.2-full.json`.

### Seals before 3.0.8 are wrong for five read scenarios

Up to and including 3.0.7, predicate pruning dropped files whose timestamps fell
within one UTC offset of a naive literal, so these scenarios sealed answers that
were missing rows: `agg_30d_by_country`, `distinct_users_30d`,
`narrow_window_single_file`, `prune_30d_window`, `two_dim_group_by_30d`.

Comparing 3.0.8+ against an older baseline therefore reports correctness drift
on exactly those five. That is the fix landing, not a regression. Compare
against `read-3.0.8-full.json` or later.

The current seals are verified, not assumed: `read-3.0.8-full-fullscan.json` is
the same suite with pruning disabled, and all 16 seals match the pruned run —
so pruning provably returns what reading every file returns. Regenerate both
together and keep them in step.

A note on provenance: `read-3.0.3-full.json` was captured on a dirty working
tree (`compare` warns about this), so it does not correspond to any commit.
Baselines from 3.0.8 on are taken on a clean tree.

## Scales and cost

`full` is the specified shape — 10,000,000 rows across 100 files. `smoke`
(200,000 rows / 20 files) keeps the identical time-shape and selectivities and
exists to validate the harness quickly.

The read dataset is built once per machine and reused; its fingerprint is
recorded in every result file, and a run measured against different data is not
comparable. Write-suite tables are dropped after each run.
