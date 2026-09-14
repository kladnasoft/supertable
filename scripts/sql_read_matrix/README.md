# SQL engine and read-path matrix

This runner prepares a deterministic dataset and complete expected results before executing SQL. It writes real local Parquet through SuperTable and uses a fresh, disposable Redis Docker container. It covers DuckDB, AUTO with no Spark cluster, full scans, pruning, Arrow streaming, and the public `query_sql` helper.

## Expanded date, datetime, and timezone checks

The extended suite adds 354 date/naive-datetime cases, 332 timezone cases, and 98 temporal-coercion cases. Run all seven session zones in separate processes:

```bash
python -m scripts.sql_read_matrix.run_zones --output /tmp/supertable-temporal-results
```

Workers cover UTC, Europe/Budapest, America/New_York, Asia/Kathmandu, Australia/Lord_Howe, Pacific/Apia, and Pacific/Kiritimati. Each starts with `TZ` set before DuckDB imports, then checks the observed base and cursor timezones and a timezone-sensitive query in all five modes. This matters because changing the timezone of a pooled base connection does not change newly created cursor sessions in the tested DuckDB version. The first native query runs before the verification probe, preserving the normal initialization sequence.

Fixtures include leap days, pre-epoch/future dates, exact microsecond neighbors, nulls, repeated DST hours, missing DST hours, 30-minute DST transitions, Apia's skipped day, positive/negative/fractional offsets, timezone-aware Parquet columns, local-day bounds, and implicit/explicit temporal casts. Expectations use Python date arithmetic and ZoneInfo with explicit UTC-instant comparisons. Each zone also has a required file-pruning assertion. Source AST fingerprints are captured before and after instrumented runs to detect implementation changes during testing.

To run one zone or the separate GROUP BY alias follow-up:

```bash
python -m scripts.sql_read_matrix --groups timezones temporal_coercion \
  --session-timezone America/New_York --skip-lifecycle --output /tmp/supertable-newyork

python -m scripts.sql_read_matrix --groups alias_followup \
  --skip-lifecycle --output /tmp/supertable-alias-followup

python -m pytest scripts/sql_read_matrix/test_harness.py \
  scripts/sql_read_matrix/test_temporal_harness.py -q
```

`--expectations-from PATH` loads a saved case book, retaining its original expected values and success/rejection contract. Use it with `--case` to distinguish implementation fixes from changed test expectations. In the 2026-09-14 recheck, six table-free query cases were changed to expect rejection; rerunning their original positive expectations makes that remaining limitation visible.

Run from the repository root in the installed SuperTable Python environment, with Docker available:

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-sql-read-results
```

Use a fresh output directory for every run. The runner deliberately exits nonzero for a failed assertion, an oracle disagreement, or a fixture/lifecycle failure. It removes only its own Redis container and leaves the dataset, expectations, Parquet fixtures, reports, and logs for inspection. It does not modify application code or publish a package.

## Dataset and assertions

Nine logical tables include matched and unmatched relationships, duplicate grouping values, typed nulls, negative and zero values, Unicode and escaped text, leap-day dates, timestamps, fixed-precision decimals, an empty table, schema evolution, upserts, and deletions. A separate lifecycle table checks reads across warmed caches, successive mutations, stale-version rejection, compaction, path-only snapshot metadata, share filters, missing statistics, and missing required files.

Expected rows come from plain Python arithmetic, explicit fixtures, grouping, and joins with SQL null semantics. Direct DuckDB over the logical Arrow input independently checks the positive expectations. Its output is never used to manufacture an expectation. Ordered queries check every row position; unordered queries preserve duplicate counts. Column names and order are checked. Declared dtype assertions apply to native results. Integers and decimals are compared exactly, with a `1e-9` absolute/relative tolerance for floating-point values.

The predefined restricted role uses its canonical alphabetic column allowlist. Reference connections use DuckDB's `nocase` collation and UTC to match the tested configuration.

Every table first attempts ingestion through `DataWriter`. If that fails, the failure remains in the report and an explicitly recorded Arrow fixture fallback allows read testing to continue. Five separate decimal-encoding probes isolate ingestion/statistics compatibility. These probes do not turn an ingestion failure into a pass.

## Reproduction and artifacts

```bash
python -m scripts.sql_read_matrix --case advanced_date_date_diff \
  --skip-lifecycle --output /tmp/supertable-date-diff-reproduction

python -m scripts.sql_read_matrix --groups controls \
  --output /tmp/supertable-controls-reproduction

python -m pytest scripts/sql_read_matrix/test_harness.py -q
```

`--modes` selects any of `pruned`, `fullscan`, `stream`, `auto`, and `query_sql`. The main matrix uses all five. Ledger and schema-evolution cases are also checked after compaction in buffered and streaming modes. `--skip-lifecycle` omits the separate mutation and metadata-fault scenarios.

`REPORT.md` summarizes coverage and links each failure. `dataset.json` and `expectations.json` preserve the predefined inputs; dates, datetimes, and decimals have tagged JSON encodings. `results.json` preserves run modes, actual failing rows, exceptions, timings, plan statistics, dependency versions, and provenance. `case_results.csv` supports filtering. `lifecycle/` contains the mutation trace, expectations, and fault results.

Reviewed reruns can be consolidated without losing the original attempts:

```bash
python -m scripts.sql_read_matrix.consolidate \
  --runs /tmp/discovery /tmp/reviewed-controls /tmp/syntax-followup \
  --output /tmp/supertable-reviewed-results
```

Consolidation requires matching source revisions, package/dependency versions, and logical datasets. It checks that every selected observation used the current expectation, chooses the latest observation of each case, and copies the original runs into the report. It does not rerun queries.

## Scope

This matrix validates LOCAL storage and the DuckDB read path on a small but deliberately varied dataset. It does not validate Spark execution, cloud storage, concurrent writers, network fault recovery, or production-scale performance. Negative-query assertions distinguish an unexpected successful execution from a rejection with a different diagnostic message.
