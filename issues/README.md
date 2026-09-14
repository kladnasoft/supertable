# SQL engine and read-path issues

## Latest recheck: 2026-09-14

The expanded recheck ran **1,615 cases / 8,109 query executions**: **8,074 passed and 35 failed**. All 35 current failures share the new GROUP BY alias defect, [STREAD-014](014-group-by-select-alias-treated-as-column.md). All **430 timezone/coercion cases** passed across seven session zones. [Reviewed findings](../audit/sql_read_matrix_recheck_2026-09-14/FINDINGS.md).

Of the original 102 failed assertions, **72 now satisfy their unchanged expectations**; **30 still reject table-free queries** that originally expected successful results. Those six cases were changed to expect rejection, so the original expectations were replayed separately. All five original additional setup/lifecycle failures now pass.

| Issue | Priority | Current status |
| --- | --- | --- |
| [STREAD-001: Snapshot fallback exposes deleted rows and ignores share filters](001-snapshot-fallback-loses-deletions-and-share-filters.md) | P1 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-002: query_sql silently overwrites valid row limits](002-query-helper-overwrites-row-limits.md) | P1 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-003: query_sql appends LIMIT after a statement terminator](003-query-helper-appends-limit-after-semicolon.md) | P2 | Semicolon fix verified; literal-query sample remains unsupported |
| [STREAD-004: SQL rewriting corrupts DuckDB DATE_DIFF arguments](004-date-diff-rewrite-corrupts-arguments.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-005: GROUPING SETS, ROLLUP, and CUBE with LIMIT are rejected](005-grouping-extensions-with-limit-rejected.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-006: Boolean IS UNKNOWN is rejected during SQL admission](006-is-unknown-rejected.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-007: LIMIT ALL is treated as a required table column](007-limit-all-treated-as-column.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-008: A trailing comment is counted as a second SQL statement](008-trailing-comment-counted-as-statement.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-009: Table-free SELECTs and literal CTEs cannot execute](009-table-free-selects-unsupported.md) | P3 | Unsupported in the current implementation |
| [STREAD-010: Existing empty tables fail projections and aggregates](010-empty-tables-cannot-be-read.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-011: Decimal ingestion fails while extracting Parquet statistics](011-decimal-ingestion-statistics-failure.md) | P2 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-012: query_sql changes forbidden-statement rejection diagnostics](012-query-helper-changes-rejection-diagnostics.md) | P3 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-013: Denied columns inside a CTE produce binder errors](013-denied-cte-column-produces-binder-error.md) | P3 | Verified fixed in the 2026-09-14 recheck |
| [STREAD-014: GROUP BY treats SELECT aliases as physical columns](014-group-by-select-alias-treated-as-column.md) | P2 | Open: seven cases, 35 failed assertions |

Current evidence: [full report](../audit/sql_read_matrix_recheck_2026-09-14/REPORT.md), [original issue verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json), [status manifest](recheck-status.json), [35 current failure mappings](recheck-failure-map.csv). STREAD-003's semicolon handling is corrected for table-backed queries; its remaining sample depends on the table-free capability recorded in STREAD-009.

Each issue includes reproduction commands, expected/actual behavior, source pointers, evidence, and acceptance criteria. Statuses refer to the tested working tree, with implementation fingerprints retained in the recheck artifacts.

## Historical discovery record

The initial audit found **102 query failures across 35 cases**, grouped into 11 query issues, plus four snapshot lifecycle failures and one decimal-ingestion failure grouped into two additional issues. These initial observations remain preserved in [failure-map.csv](failure-map.csv) and [failure-map.json](failure-map.json); they are historical mappings, not the current status list.

Nine initial failures were rejection-message inconsistencies and are now verified fixed. Two initial cases had mode-specific causes: `syntax_literal_select_semicolon` and `syntax_semicolon_then_comment`; the original map counted each failed case/mode exactly once.

Original audit: [findings](../audit/sql_read_matrix_2026-09-14/FINDINGS.md), [full report](../audit/sql_read_matrix_2026-09-14/REPORT.md), [dataset](../audit/sql_read_matrix_2026-09-14/dataset.json), [expectations](../audit/sql_read_matrix_2026-09-14/expectations.json).

## Reproduction

Run from the repository root with Docker available, using a fresh output directory each time:

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-base-recheck
python -m scripts.sql_read_matrix.run_zones --output /tmp/supertable-temporal-recheck
python -m scripts.sql_read_matrix --groups alias_followup --skip-lifecycle --output /tmp/supertable-alias-recheck
```

The tested implementation is SuperTable 3.4.1 with the working-tree fixes based on revision `2417237a8b7e2e91900fbc7503c35059dba0e030`. Dependencies: DuckDB 1.5.4, SQLGlot 26.33.0, Polars 1.42.1, PyArrow 18.1.0, redis-py 5.3.1. The scope is LOCAL storage and DuckDB with isolated real Redis; Spark, cloud storage, concurrency, and production-scale performance were not tested.

Use `--expectations-from` with an archived case book when verifying the original acceptance criteria. A passing reclassified rejection test does not demonstrate support for the earlier positive query. [Harness instructions](../scripts/sql_read_matrix/README.md).
