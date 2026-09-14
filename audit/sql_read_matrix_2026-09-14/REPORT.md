# SQL engine and read-path validation

Source revision: `2417237a8b7e2e91900fbc7503c35059dba0e030`. Package: `3.4.1`.

Read the [reviewed findings and priorities](FINDINGS.md) for the confirmed failure groups and working behavior.

## Method

The dataset and every expected row were constructed before queries ran. Expected results use plain Python arithmetic, grouping, joins, and explicit SQL null rules. They are not golden outputs captured from SuperTable. A direct DuckDB connection over the logical Arrow input independently checks each positive oracle; disagreements are reported separately and must be reviewed before treating a failure as a product defect.

Data ingestion was attempted through DataWriter into multiple real local Parquet files, with real Redis metadata and locks in an isolated disposable container. Setup failures are retained as failures, and affected tables use an explicit PyArrow fixture fallback to allow read coverage to continue; see the fixture manifest and additional checks. Ledger replacement/deletion and schema evolution were performed through the writer. Native queries ran with pruning, full scans, Arrow batches, and AUTO routing without Spark registrations. Ordered queries compare complete row order; other queries compare duplicate-preserving multisets. Column names/order are checked, with explicit dtype checks where declared. Floats use absolute and relative tolerances of 1e-9; integers and decimals remain exact.

## Results

| Measure | Count |
| --- | ---: |
| query cases | 821 |
| distinct sql | 816 |
| expected success cases | 790 |
| expected rejection cases | 31 |
| executions | 4139 |
| passed | 4037 |
| failed | 102 |
| failing query cases | 35 |
| independent oracle disagreements | 0 |
| additional checks | 35 |
| additional check passes | 30 |
| additional check failures | 5 |
| categories | 63 |
| positive query failures | 30 |
| rejection message mismatches | 9 |
| unexpected rejection case successes | 0 |

Of the failed executions, 9 rejected the query with a different error than expected. These are diagnostic inconsistencies, separately counted from forbidden queries that unexpectedly return results.

## Coverage by category

| Category | Queries | Failing queries |
| --- | ---: | ---: |
| aggregate | 7 | 0 |
| aliases | 3 | 0 |
| boolean | 16 | 1 |
| conditional_aggregate | 13 | 0 |
| cte_chain | 4 | 0 |
| cte_repeated | 8 | 0 |
| date_boundary | 3 | 0 |
| date_timestamp | 22 | 1 |
| decimal | 11 | 0 |
| distinct_aggregate | 7 | 0 |
| empty_results | 6 | 2 |
| group_by | 6 | 0 |
| group_expression | 3 | 0 |
| grouping_sets | 3 | 3 |
| having | 12 | 0 |
| joins_composite | 12 | 0 |
| joins_cross | 4 | 0 |
| joins_multiway | 3 | 0 |
| joins_non_equi | 16 | 0 |
| joins_null_safe | 3 | 0 |
| joins_one_to_many | 6 | 0 |
| joins_outer | 24 | 0 |
| joins_self | 5 | 0 |
| joins_semi_anti | 16 | 0 |
| native_tombstones | 10 | 0 |
| null_aggregate | 6 | 1 |
| null_semantics | 10 | 0 |
| numeric_expression | 22 | 0 |
| ordering | 10 | 0 |
| predicates | 211 | 0 |
| preflight | 5 | 0 |
| qualify | 6 | 0 |
| rbac_columns | 8 | 1 |
| rbac_rows | 12 | 0 |
| rbac_table_access | 7 | 0 |
| scalar_expressions | 8 | 0 |
| schema_evolution | 7 | 0 |
| set_operations | 54 | 0 |
| set_operations_multicolumn | 6 | 0 |
| sql_admission | 12 | 4 |
| sql_expression_alternative | 2 | 0 |
| sql_grouping_limit | 6 | 6 |
| sql_limit_syntax | 11 | 6 |
| sql_literal_source | 6 | 6 |
| sql_nested_limit | 3 | 0 |
| sql_quoted_identifier | 3 | 0 |
| sql_surface_syntax | 11 | 4 |
| statistical_aggregate | 7 | 0 |
| strings | 19 | 0 |
| subquery_exists | 12 | 0 |
| subquery_membership_nulls | 22 | 0 |
| subquery_scalar | 6 | 0 |
| subquery_scalar_correlated | 20 | 0 |
| system_columns | 3 | 0 |
| try_cast | 3 | 0 |
| typed_nulls | 7 | 0 |
| window_frame | 12 | 0 |
| window_frame_navigation | 12 | 0 |
| window_navigation | 21 | 0 |
| window_ntile | 18 | 0 |
| window_partition | 1 | 0 |
| window_range | 3 | 0 |
| window_rank | 6 | 0 |

## Reproducible failures

Each linked record includes SQL, the complete expected result, actual failing results, role, mode, and comparison detail.

| Case | Category | Failing modes | First failure |
| --- | --- | --- | --- |
| [advanced_grouping_sets](failures/advanced_grouping_sets.json) | grouping_sets | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 5, Col: 12.          FROM warehouse.orders GROUP BY GROUPING SETS ((region, status), (region), ())          LIMIT  |
| [advanced_grouping_rollup](failures/advanced_grouping_rollup.json) | grouping_sets | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 5, Col: 12.   ) AS total_amount             FROM warehouse.orders GROUP BY ROLLUP (region, status)          LIMIT  |
| [advanced_grouping_cube](failures/advanced_grouping_cube.json) | grouping_sets | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 5, Col: 12.   nt) AS total_amount             FROM warehouse.orders GROUP BY CUBE (region, status)          LIMIT  |
| [advanced_null_aggregate_empty_table](failures/advanced_null_aggregate_empty_table.json) | null_aggregate | pruned, fullscan, stream, auto, query_sql | RuntimeError: No parquet files found for one or more selected tables. |
| [advanced_date_date_diff](failures/advanced_date_date_diff.json) | date_timestamp | pruned, fullscan, stream, auto, query_sql | RuntimeError: Conversion Error: invalid date field format: "day", expected format is (YYYY-MM-DD)  LINE 1: SELECT eid, DATE_DIFF('2024-03-15', event_date, CAST('day' AS DATE)) AS result FROM tomb_st_ccb353026ace2db...    |
| [advanced_boolean_is_unknown](failures/advanced_boolean_is_unknown.json) | boolean | pruned, fullscan, stream, auto, query_sql | RuntimeError: could not parse query: Invalid expression / Unexpected token. Line 1, Col: 27.   SELECT eid, flag IS [4mUNKNOWN[0m AS result FROM warehouse.events ORDER BY eid |
| [controls_empty_physical_projection](failures/controls_empty_physical_projection.json) | empty_results | pruned, fullscan, stream, auto, query_sql | RuntimeError: No parquet files found for one or more selected tables. |
| [controls_empty_physical_count](failures/controls_empty_physical_count.json) | empty_results | pruned, fullscan, stream, auto, query_sql | RuntimeError: No parquet files found for one or more selected tables. |
| [controls_reject_insert](failures/controls_reject_insert.json) | sql_admission | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 5.   INSERT INTO orders (oid) VALUES (999) [4mLIMIT[0m 100000 |
| [controls_reject_create](failures/controls_reject_create.json) | sql_admission | query_sql | RuntimeError: Query execution failed: COMMAND is not permitted on the read path; only SELECT queries are |
| [controls_reject_drop](failures/controls_reject_drop.json) | sql_admission | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 5.   DROP TABLE matrix_admission_probe [4mLIMIT[0m 100000 |
| [controls_reject_describe](failures/controls_reject_describe.json) | sql_admission | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 5.   DESCRIBE orders [4mLIMIT[0m 100000 |
| [controls_rbac_denied_column_cte](failures/controls_rbac_denied_column_cte.json) | rbac_columns | pruned, fullscan, stream, auto, query_sql | RuntimeError: Binder Error: Referenced column "note" not found in FROM clause! Candidate bindings: "oid", "qty"  LINE 1: WITH hidden AS (SELECT oid, note FROM rbac_tomb_st_58ff5232248279c2_8ba5742a_8ba5742a...            |
| [syntax_terminal_semicolon](failures/syntax_terminal_semicolon.json) | sql_surface_syntax | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 12.   SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid; LIMIT [4m100000[0m |
| [syntax_semicolon_trailing_whitespace](failures/syntax_semicolon_trailing_whitespace.json) | sql_surface_syntax | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 3, Col: 12.   SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid;   	 LIMIT [4m100000[0m |
| [syntax_semicolon_then_comment](failures/syntax_semicolon_then_comment.json) | sql_surface_syntax | pruned, fullscan, stream, auto, query_sql | RuntimeError: only a single statement may be submitted; found 2 |
| [syntax_empty_statement_after_semicolon](failures/syntax_empty_statement_after_semicolon.json) | sql_surface_syntax | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 12.   SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid;; LIMIT [4m100000[0m |
| [syntax_literal_select](failures/syntax_literal_select.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: No tables found in SQL query. |
| [syntax_literal_select_semicolon](failures/syntax_literal_select_semicolon.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: No tables found in SQL query. |
| [syntax_literal_cte](failures/syntax_literal_cte.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | RuntimeError: No snapshots selected. |
| [syntax_literal_union](failures/syntax_literal_union.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: No tables found in SQL query. |
| [syntax_literal_scalar_subquery](failures/syntax_literal_scalar_subquery.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: No tables found in SQL query. |
| [syntax_literal_cte_declared_columns](failures/syntax_literal_cte_declared_columns.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | RuntimeError: No snapshots selected. |
| [syntax_offset_fetch_next](failures/syntax_offset_fetch_next.json) | sql_limit_syntax | query_sql | {"kind": "row_count", "expected_count": 3, "actual_count": 6} |
| [syntax_fetch_first](failures/syntax_fetch_first.json) | sql_limit_syntax | query_sql | {"kind": "row_count", "expected_count": 3, "actual_count": 8} |
| [syntax_limit_parenthesized](failures/syntax_limit_parenthesized.json) | sql_limit_syntax | query_sql | {"kind": "row_count", "expected_count": 3, "actual_count": 8} |
| [syntax_offset_parenthesized](failures/syntax_offset_parenthesized.json) | sql_limit_syntax | query_sql | {"kind": "row_count", "expected_count": 3, "actual_count": 6} |
| [syntax_limit_all](failures/syntax_limit_all.json) | sql_limit_syntax | pruned, fullscan, stream, auto | RuntimeError: Missing required column(s): warehouse.orders: ALL |
| [syntax_grouping_sets_no_limit](failures/syntax_grouping_sets_no_limit.json) | sql_grouping_limit | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 12.   FROM warehouse.orders WHERE oid <= 24 GROUP BY GROUPING SETS ((region, status), (region), ()) LIMIT  |
| [syntax_grouping_sets_explicit_limit](failures/syntax_grouping_sets_explicit_limit.json) | sql_grouping_limit | pruned, fullscan, stream, auto, query_sql | RuntimeError: could not parse query: Invalid expression / Unexpected token. Line 1, Col: 208.   FROM warehouse.orders WHERE oid <= 24 GROUP BY GROUPING SETS ((region, status), (region), ()) LIMIT [4m100[0m |
| [syntax_grouping_rollup_no_limit](failures/syntax_grouping_rollup_no_limit.json) | sql_grouping_limit | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 12.   s_total, COUNT(*) AS n FROM warehouse.orders WHERE oid <= 24 GROUP BY ROLLUP (region, status) LIMIT  |
| [syntax_grouping_rollup_explicit_limit](failures/syntax_grouping_rollup_explicit_limit.json) | sql_grouping_limit | pruned, fullscan, stream, auto, query_sql | RuntimeError: could not parse query: Invalid expression / Unexpected token. Line 1, Col: 185.   s_total, COUNT(*) AS n FROM warehouse.orders WHERE oid <= 24 GROUP BY ROLLUP (region, status) LIMIT [4m100[0m |
| [syntax_grouping_cube_no_limit](failures/syntax_grouping_cube_no_limit.json) | sql_grouping_limit | query_sql | RuntimeError: Query execution failed: could not parse query: Invalid expression / Unexpected token. Line 2, Col: 12.   tus_total, COUNT(*) AS n FROM warehouse.orders WHERE oid <= 24 GROUP BY CUBE (region, status) LIMIT  |
| [syntax_grouping_cube_explicit_limit](failures/syntax_grouping_cube_explicit_limit.json) | sql_grouping_limit | pruned, fullscan, stream, auto, query_sql | RuntimeError: could not parse query: Invalid expression / Unexpected token. Line 1, Col: 183.   tus_total, COUNT(*) AS n FROM warehouse.orders WHERE oid <= 24 GROUP BY CUBE (region, status) LIMIT [4m100[0m |
| [syntax_limit_comment_between_keyword_and_count](failures/syntax_limit_comment_between_keyword_and_count.json) | sql_limit_syntax | query_sql | {"kind": "row_count", "expected_count": 3, "actual_count": 8} |

## Oracle review

Every positive Python oracle agreed with direct DuckDB over the predefined logical input.

## Evidence and reviewed reruns

The counts above use the latest observation of each query case. Original attempts are preserved below; each result identifies its evidence run. No unchanged passing queries were discarded.

- [runs/01_supertable-sql-matrix-discovery](runs/01_supertable-sql-matrix-discovery/REPORT.md): 779 cases, 3929 executions.
- [runs/02_supertable-sql-controls-reviewed](runs/02_supertable-sql-controls-reviewed/REPORT.md): 80 cases, 434 executions.
- [runs/03_supertable-sql-syntax-followup](runs/03_supertable-sql-syntax-followup/REPORT.md): 42 cases, 210 executions.
- [runs/04_supertable-sql-numeric-review](runs/04_supertable-sql-numeric-review/REPORT.md): 5 cases, 25 executions.

Latest observations replace earlier observations of the same case; all original runs are retained.
The EU SELECT-star oracle and reference projection were corrected to the explicit canonical sorted role allowlist. The entire controls group was rerun with the corrected predefined expectation.
Exact integer/decimal expectations were strengthened to reject rounded floats. Decimal AVG was explicitly represented as a floating expectation; all five cases with exact Python expectations and reported floating native columns were rerun. Other passing cases had exact native numeric types or already used floating expectations.


## Scope and reproduction

Run from the repository root:

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-sql-read-results
```

Use `--case CASE_ID --skip-lifecycle` to reproduce one query across modes. Choose a fresh output directory on each run. Docker must be running; the runner creates and removes its own loopback-bound Redis container. The report records the Redis image ID, dependency versions, and dataset/expectation hashes. Fixtures and all expectations are saved before execution. A nonzero exit indicates native failures, oracle disagreements, or additional-check failures.

This run covers LOCAL storage and DuckDB. AUTO had no registered Spark cluster and therefore exercises DuckDB routing. It does not validate Spark execution, cloud/object-store protocols, concurrent writers, network fault recovery, or production-scale performance. No production application code is changed by the matrix.

Artifacts: [dataset](dataset.json), [expected results](expectations.json), [fixture manifest](fixture_manifest.json), [full results](results.json), [CSV](case_results.csv).

Decimal ingestion was also isolated across five Parquet encodings: [probe details](ingest_probes.json). These diagnostic probes are separate from the query-execution counts.

## Additional lifecycle checks

| Check | Status | Detail |
| --- | --- | --- |
| [ingest_orders](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_customers](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_items](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_numbers](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_events](runs/04_supertable-sql-numeric-review/results.json) | fail | ArrowNotImplementedError: Cannot extract statistics for type  |
| [ingest_ledger](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_evolving](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_nulls](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [ingest_empty_table](runs/04_supertable-sql-numeric-review/results.json) | pass |  |
| [compact_ledger](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [compact_evolving](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_initial_cold](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_initial_warmed_repeat](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_upsert](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_delete](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_append](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_stale_rejection](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_aggregate_after_mutations](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_path_only_leaf_pruned](runs/02_supertable-sql-controls-reviewed/results.json) | fail | {'kind': 'row_count', 'expected_count': 9, 'actual_count': 12} |
| [lifecycle_path_only_leaf_stream](runs/02_supertable-sql-controls-reviewed/results.json) | fail | {'kind': 'row_count', 'expected_count': 9, 'actual_count': 12} |
| [lifecycle_restored_after_path_only](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_inline_share_filter_pruned](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_inline_share_filter_stream](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_path_only_share_filter_pruned](runs/02_supertable-sql-controls-reviewed/results.json) | fail | {'kind': 'row_count', 'expected_count': 3, 'actual_count': 12} |
| [lifecycle_path_only_share_filter_stream](runs/02_supertable-sql-controls-reviewed/results.json) | fail | {'kind': 'row_count', 'expected_count': 3, 'actual_count': 12} |
| [lifecycle_missing_stats_fallback](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_missing_stats_stream](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_unreadable_stats_fallback](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_missing_tombstone_rejected](runs/02_supertable-sql-controls-reviewed/results.json) | pass | RuntimeError: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/tombstone/lifecycle_missing_tombstone.parquet"  LINE 1: ... EXISTS dv_e4161b5a9af69e2e AS SELECT DISTINCT __rowid__ FROM read_parquet(['sql |
| [lifecycle_missing_tombstone_stream_rejected](runs/02_supertable-sql-controls-reviewed/results.json) | pass | RuntimeError: stream failed: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/tombstone/lifecycle_missing_tombstone.parquet"  LINE 1: ... EXISTS dv_e4161b5a9af69e2e AS SELECT DISTINCT __rowid__ FROM rea |
| [lifecycle_missing_datafile_rejected](runs/02_supertable-sql-controls-reviewed/results.json) | pass | RuntimeError: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/data/lifecycle_missing_data.parquet"  LINE 1: ... value, COLUMNS(c -> c IN ('__rowid__', '__timestamp__')) FROM parquet_scan(['sqlmatrix/wa |
| [lifecycle_missing_datafile_stream_rejected](runs/02_supertable-sql-controls-reviewed/results.json) | pass | RuntimeError: stream failed: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/data/lifecycle_missing_data.parquet"  LINE 1: ... value, COLUMNS(c -> c IN ('__rowid__', '__timestamp__')) FROM parquet_scan |
| [lifecycle_after_compaction](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_compaction_stream](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
| [lifecycle_after_compaction_warmed_repeat](runs/02_supertable-sql-controls-reviewed/results.json) | pass |  |
