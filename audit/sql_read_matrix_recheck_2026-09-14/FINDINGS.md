# Expanded SQL read-path recheck

Most previously reported defects are corrected. The expanded tests found **one new defect: GROUP BY aliases are mistaken for physical columns**. Timezone filtering passed the tested cases in all seven session zones. Table-free SELECTs remain unsupported; six existing tests were changed to expect their rejection, so their original positive expectations were also rerun separately.

## Results

| Test set | Cases | Query executions | Passed | Failed |
| --- | ---: | ---: | ---: | ---: |
| Original suite using its current expectations | 821 | 4,139 | 4,139 | 0 |
| Additional date and naive datetime cases | 354 | 1,770 | 1,760 | 10 |
| Timezone cases | 332 | 1,660 | 1,660 | 0 |
| Temporal coercion and pruning checks | 98 | 490 | 490 | 0 |
| GROUP BY alias follow-up | 10 | 50 | 25 | 25 |
| **Current-expectation total** | **1,615** | **8,109** | **8,074** | **35** |
| Original positive expectations for the six reclassified literal queries, reported separately | 6 | 30 | 0 | 30 |

The 35 current failures are seven queries across all five modes, with one shared alias-resolution cause. All **335 additional setup, lifecycle, source, and session checks passed** across the retained workers. All **1,578 positive expectations** in the current matrix agreed with direct DuckDB. The six original literal-query expectations also agreed with direct DuckDB, confirming that their continued rejection is a compatibility limitation rather than an incorrect result oracle.

Focused implementation regression tests: **209 passed**. Harness tests: **70 passed**, including exact temporal/Decimal comparisons, DST fixture invariants, saved-expectation contracts, and cross-process reproducibility.

## Previously reported issues

Of the original **102 failed query assertions**, **72 now meet their unchanged expectations**. The other **30 still reject table-free queries** that originally expected successful results. The only six changed case definitions switched `error_contains` from empty to `reads no table`; their SQL, expected values, roles, and ordering were unchanged. The [frozen-expectation rerun](historical_original_expectations/REPORT.md) preserves those original requirements and the resulting failures.

All **five original additional failures are fixed**: the decimal table now ingests through DataWriter, and path-only snapshots preserve deletions and share filters in both buffered and streaming reads.

Verified corrections include row-limit preservation, DATE_DIFF rewriting, grouping extensions with LIMIT, IS UNKNOWN, LIMIT ALL, trailing-comment admission, empty-table reads, and rejection diagnostics. Semicolon handling works for table-backed queries; its remaining literal-query sample is blocked by the table-free-query restriction. [Original issue verification](original_issue_verification.json) maps every original failure to its new observation.

The [issue index](../../issues/README.md) records the current status. STREAD-009 remains explicitly unsupported, and the new defect is tracked as STREAD-014.

## New defect: GROUP BY a SELECT alias

```sql
SELECT EXTRACT(year FROM event_date) AS yr,
       COUNT(*) AS n,
       MIN(event_date) AS first_value,
       MAX(event_date) AS last_value
FROM temporal_dates
WHERE event_date IS NOT NULL
GROUP BY yr
ORDER BY yr
```

Expected: 11 independently calculated year groups. Actual: `Missing required column(s): warehouse.temporal_dates: yr`. The datetime equivalent fails similarly. Follow-up tests confirm failures for quoted aliases, ordinary direct/expression aliases, and an alias inside ROLLUP.

The SQL parser recognizes SELECT aliases in ORDER BY, HAVING, and QUALIFY but omits GROUP BY from its alias scopes. It therefore requires the alias as a nonexistent physical column. This is a general dependency-resolution bug, rather than incorrect date arithmetic or timezone filtering.

**Verified alternatives:** `GROUP BY EXTRACT(year FROM event_date)` and `GROUP BY 1`, with corresponding datetime variants, passed all five native modes. A physical-column/alias-name collision also passed and should remain covered when fixing alias resolution.

[STREAD-014: reproductions, source pointers, and acceptance criteria](../../issues/014-group-by-select-alias-treated-as-column.md). The [full report](REPORT.md) links all seven failed cases and complete expected results.

## Date, datetime, and timezone coverage

The conflict-checked logical dataset contains **2,916 rows across 13 tables**, plus the separate mutation/lifecycle fixture. The new temporal tables include dates from 1900–2100, pre-epoch values, leap days, year/month ends, microseconds, duplicates, nulls, UTC instants, and timestamps stored with Europe/Budapest timezone metadata. Data and expected rows are saved before native execution.

Tested sessions: **UTC, Europe/Budapest, America/New_York, Asia/Kathmandu, Australia/Lord_Howe, Pacific/Apia, and Pacific/Kiritimati**.

- Inclusive/exclusive equality and range boundaries; BETWEEN, IN, reversed comparisons, Boolean combinations, and null filters.
- DATE, naive TIMESTAMP, and TIMESTAMPTZ comparisons, casts, date arithmetic, extraction, truncation, and filtering through CTEs/joins.
- Explicit offsets, ISO `T...Z`, `+00`, `+0000`, fractional offsets, bare date/datetime strings, and implicit comparisons between aware and naive values.
- Local-midnight boundaries; 23/25-hour days and Lord Howe's 23.5/24.5-hour days; spring gaps and both fall folds; Apia's skipped day.
- Exact microsecond neighbors and equivalent instants represented with different offsets.
- Pruned/full-scan equivalence, streaming, AUTO routing to DuckDB, and the public query helper.

Each zone ran in a fresh process with `TZ` set before DuckDB imported. The instrumented workers verified both the actual pooled base and newly created cursor timezone, plus a timezone-sensitive native query in all five modes. Changing only a pooled connection's setting would not have established the same coverage in the tested DuckDB version.

Every zone's required pruning check passed. The guards excluded **95–98 of 103 files** while preserving exact results. Some explicit-offset predicates conservatively retain files; passing these queries does not mean every temporal predicate is optimized. [Per-zone execution and pruning evidence](timezone_coverage.json) includes observed sessions.

## Reproduction and implementation identity

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-base-recheck
python -m scripts.sql_read_matrix.run_zones --output /tmp/supertable-temporal-recheck
python -m scripts.sql_read_matrix --groups alias_followup --skip-lifecycle --output /tmp/supertable-alias-recheck
python -m pytest scripts/sql_read_matrix/test_harness.py scripts/sql_read_matrix/test_temporal_harness.py -q
```

Choose fresh output directories and run from the repository root with Docker available. To preserve an earlier acceptance requirement, use `--expectations-from` with a saved case book and select cases with `--case`. The aggregate report retains the original case book and the separate frozen-expectation run.

Tested package: **3.4.1**, working tree based on revision `2417237a8b7e2e91900fbc7503c35059dba0e030`, including the user's uncommitted fixes. Dependencies: DuckDB **1.5.4**, SQLGlot **26.33.0**, Polars **1.42.1**, PyArrow **18.1.0**. Instrumented worker source fingerprints matched before/after and across runs. The earlier baseline's separately captured executable-source snapshot also matches the worker implementation; its external start/end comparison is retained with that run. No production implementation was changed during this recheck.

This covers LOCAL storage and DuckDB, with real isolated Redis metadata. Spark execution, cloud storage, concurrent writers, and production-scale performance remain outside the tested scope. Owned Redis containers were removed; fixtures and evidence remain in this audit folder.

Artifacts: [dataset](dataset.json), [expectations](expectations.json), [all results](results.json), [CSV](case_results.csv), [original issue verification](original_issue_verification.json), [source verification](source_verification.json), [harness instructions](../../scripts/sql_read_matrix/README.md).
