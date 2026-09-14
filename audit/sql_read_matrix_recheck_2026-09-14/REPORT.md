# SQL engine and read-path validation

Read the [reviewed recheck findings](FINDINGS.md), including the remaining unsupported queries and the new GROUP BY alias issue.

Source revision: `2417237a8b7e2e91900fbc7503c35059dba0e030`. Package: `3.4.1`.

## Method

The dataset and every expected row were constructed before queries ran. Expected results use plain Python arithmetic, grouping, joins, and explicit SQL null rules. They are not golden outputs captured from SuperTable. A direct DuckDB connection over the logical Arrow input independently checks each positive oracle; disagreements are reported separately and must be reviewed before treating a failure as a product defect.

Data ingestion was attempted through DataWriter into multiple real local Parquet files, with real Redis metadata and locks in an isolated disposable container. Setup failures are retained as failures, and affected tables use an explicit PyArrow fixture fallback to allow read coverage to continue; see the fixture manifest and additional checks. Ledger replacement/deletion and schema evolution were performed through the writer. Native queries ran with pruning, full scans, Arrow batches, and AUTO routing without Spark registrations. Ordered queries compare complete row order; other queries compare duplicate-preserving multisets. Column names/order are checked, with explicit dtype checks where declared. Floats use absolute and relative tolerances of 1e-9; integers and decimals remain exact.

## Results

| Measure | Count |
| --- | ---: |
| query cases | 1615 |
| distinct sql | 1465 |
| expected success cases | 1578 |
| expected rejection cases | 37 |
| executions | 8109 |
| passed | 8074 |
| failed | 35 |
| failing query cases | 7 |
| independent oracle disagreements | 0 |
| additional checks | 335 |
| additional check passes | 335 |
| additional check failures | 0 |
| categories | 97 |
| positive query failures | 7 |
| rejection message mismatches | 0 |
| unexpected rejection case successes | 0 |

## Coverage by category

| Category | Queries | Failing queries |
| --- | ---: | ---: |
| aggregate | 7 | 0 |
| aliases | 3 | 0 |
| boolean | 16 | 0 |
| conditional_aggregate | 13 | 0 |
| cte_chain | 4 | 0 |
| cte_repeated | 8 | 0 |
| date_boundary | 3 | 0 |
| date_timestamp | 22 | 0 |
| decimal | 11 | 0 |
| distinct_aggregate | 7 | 0 |
| empty_results | 6 | 0 |
| group_alias_followup | 10 | 5 |
| group_by | 6 | 0 |
| group_expression | 3 | 0 |
| grouping_sets | 3 | 0 |
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
| null_aggregate | 6 | 0 |
| null_semantics | 10 | 0 |
| numeric_expression | 22 | 0 |
| ordering | 10 | 0 |
| predicates | 211 | 0 |
| preflight | 5 | 0 |
| qualify | 6 | 0 |
| rbac_columns | 8 | 0 |
| rbac_rows | 12 | 0 |
| rbac_table_access | 7 | 0 |
| scalar_expressions | 8 | 0 |
| schema_evolution | 7 | 0 |
| set_operations | 54 | 0 |
| set_operations_multicolumn | 6 | 0 |
| sql_admission | 12 | 0 |
| sql_expression_alternative | 2 | 0 |
| sql_grouping_limit | 6 | 0 |
| sql_limit_syntax | 11 | 0 |
| sql_literal_source | 6 | 0 |
| sql_nested_limit | 3 | 0 |
| sql_quoted_identifier | 3 | 0 |
| sql_surface_syntax | 11 | 0 |
| statistical_aggregate | 7 | 0 |
| strings | 19 | 0 |
| subquery_exists | 12 | 0 |
| subquery_membership_nulls | 22 | 0 |
| subquery_scalar | 6 | 0 |
| subquery_scalar_correlated | 20 | 0 |
| system_columns | 3 | 0 |
| temporal_aggregates | 2 | 2 |
| temporal_cast_filters | 12 | 0 |
| temporal_comparison | 144 | 0 |
| temporal_correlated_filters | 2 | 0 |
| temporal_cte_filters | 2 | 0 |
| temporal_datediff_filters | 15 | 0 |
| temporal_datepart_filters | 40 | 0 |
| temporal_interval_filters | 8 | 0 |
| temporal_joins | 3 | 0 |
| temporal_membership | 32 | 0 |
| temporal_null_semantics | 16 | 0 |
| temporal_ranges | 48 | 0 |
| temporal_reversed_comparison | 8 | 0 |
| temporal_subsecond_filters | 10 | 0 |
| temporal_truncation_filters | 10 | 0 |
| temporal_windows | 2 | 0 |
| timezone_calendar_discontinuity | 1 | 0 |
| timezone_column_coercion | 14 | 0 |
| timezone_dst_fold | 12 | 0 |
| timezone_dst_gap | 3 | 0 |
| timezone_filters | 147 | 0 |
| timezone_literal_coercion | 35 | 0 |
| timezone_local_midnight | 7 | 0 |
| timezone_microseconds | 18 | 0 |
| timezone_naive_timestamps | 35 | 0 |
| timezone_nulls | 4 | 0 |
| timezone_offset_literals | 63 | 0 |
| timezone_offset_spelling | 28 | 0 |
| timezone_offsets | 14 | 0 |
| timezone_projection | 21 | 0 |
| timezone_pruning_guard | 7 | 0 |
| timezone_reverse_coercion | 14 | 0 |
| timezone_storage | 7 | 0 |
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
| [temporal_date_aggregate_year](failures/temporal_date_aggregate_year.json) | temporal_aggregates | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.temporal_dates: yr |
| [temporal_timestamp_aggregate_year](failures/temporal_timestamp_aggregate_year.json) | temporal_aggregates | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.temporal_naive: yr |
| [alias_date_quoted](failures/alias_date_quoted.json) | group_alias_followup | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.temporal_dates: Calendar Year |
| [alias_timestamp_quoted](failures/alias_timestamp_quoted.json) | group_alias_followup | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.temporal_naive: Calendar Year |
| [alias_number_direct](failures/alias_number_direct.json) | group_alias_followup | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.numbers: category |
| [alias_number_expression](failures/alias_number_expression.json) | group_alias_followup | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.numbers: category |
| [alias_number_rollup](failures/alias_number_rollup.json) | group_alias_followup | pruned, fullscan, stream, auto, query_sql | RuntimeError: Missing required column(s): warehouse.numbers: category |

## Oracle review

Every positive Python oracle agreed with direct DuckDB over the predefined logical input.

## Evidence and reviewed reruns

The counts above use the latest observation of each query case. Original attempts are preserved below; each result identifies its evidence run. No unchanged passing queries were discarded.

- [runs/01_supertable-sql-fixed-baseline-20260914](runs/01_supertable-sql-fixed-baseline-20260914/REPORT.md): 821 cases, 4139 executions.
- [runs/02_supertable-timezones-newyork-recheck](runs/02_supertable-timezones-newyork-recheck/REPORT.md): 38 cases, 190 executions.
- [runs/03_Asia_Kathmandu](runs/03_Asia_Kathmandu/REPORT.md): 33 cases, 165 executions.
- [runs/04_Australia_Lord_Howe](runs/04_Australia_Lord_Howe/REPORT.md): 38 cases, 190 executions.
- [runs/05_Europe_Budapest](runs/05_Europe_Budapest/REPORT.md): 38 cases, 190 executions.
- [runs/06_Pacific_Apia](runs/06_Pacific_Apia/REPORT.md): 34 cases, 170 executions.
- [runs/07_Pacific_Kiritimati](runs/07_Pacific_Kiritimati/REPORT.md): 33 cases, 165 executions.
- [runs/08_UTC](runs/08_UTC/REPORT.md): 472 cases, 2360 executions.
- [runs/09_America_New_York](runs/09_America_New_York/REPORT.md): 14 cases, 70 executions.
- [runs/10_Asia_Kathmandu](runs/10_Asia_Kathmandu/REPORT.md): 14 cases, 70 executions.
- [runs/11_Australia_Lord_Howe](runs/11_Australia_Lord_Howe/REPORT.md): 14 cases, 70 executions.
- [runs/12_Europe_Budapest](runs/12_Europe_Budapest/REPORT.md): 14 cases, 70 executions.
- [runs/13_Pacific_Apia](runs/13_Pacific_Apia/REPORT.md): 14 cases, 70 executions.
- [runs/14_Pacific_Kiritimati](runs/14_Pacific_Kiritimati/REPORT.md): 14 cases, 70 executions.
- [runs/15_UTC](runs/15_UTC/REPORT.md): 14 cases, 70 executions.
- [runs/16_supertable-group-alias-followup-recheck](runs/16_supertable-group-alias-followup-recheck/REPORT.md): 10 cases, 50 executions.

Current-expectation results are separated from the original acceptance requirements. 6 historical cases have changed expectations; the saved changes are recorded in results.json.
The six table-free SELECT/CTE tests now accept a 'reads no table' rejection. Re-running their original positive expectations produced 30 failed executions; those results are preserved in [historical_original_expectations](historical_original_expectations/REPORT.md) and are excluded from the current-expectation headline.
Of the original 102 failed executions, 72 now meet their original expectations and 30 remain unsupported. See [original issue verification](original_issue_verification.json).
Paired executable-source fingerprints are required and checked for the instrumented workers. The earlier baseline lacks paired fingerprints; its separate audited capture is retained without claiming within-run source stability.
Each worker retains its own logical dataset, expectations, physical fixtures, dependency versions, and metadata. The aggregate logical dataset is a conflict-checked union; the fixture manifest is indexed by evidence run. Physical paths inside copied metadata retain their original recorded values.


## Scope and reproduction

Run from the repository root:

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-sql-read-results
```

Use `--case CASE_ID --skip-lifecycle` to reproduce one query across modes. Choose a fresh output directory on each run. Docker must be running; the runner creates and removes its own loopback-bound Redis container. The report records the Redis image ID, dependency versions, and dataset/expectation hashes. Fixtures and all expectations are saved before execution. A nonzero exit indicates native failures, oracle disagreements, or additional-check failures.

This run covers LOCAL storage and DuckDB. AUTO had no registered Spark cluster and therefore exercises DuckDB routing. It does not validate Spark execution, cloud/object-store protocols, concurrent writers, network fault recovery, or production-scale performance. No production application code is changed by the matrix.

Artifacts: [dataset](dataset.json), [expected results](expectations.json), [fixture manifest](fixture_manifest.json), [full results](results.json), [CSV](case_results.csv).

## Additional lifecycle checks

| Check | Status | Detail |
| --- | --- | --- |
| [01_supertable-sql-fixed-baseline-20260914__ingest_orders](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_customers](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_items](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_numbers](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_events](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_ledger](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_evolving](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_nulls](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__ingest_empty_table](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_initial_cold](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_initial_warmed_repeat](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_upsert](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_delete](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_append](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_stale_rejection](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_aggregate_after_mutations](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_path_only_leaf_pruned](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_path_only_leaf_stream](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_restored_after_path_only](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_inline_share_filter_pruned](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_inline_share_filter_stream](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_path_only_share_filter_pruned](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_path_only_share_filter_stream](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_stats_fallback](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_stats_stream](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_unreadable_stats_fallback](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_tombstone_rejected](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass | RuntimeError: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/tombstone/lifecycle_missing_tombstone.parquet"  LINE 1: ... EXISTS dv_e4161b5a9af69e2e AS SELECT DISTINCT __rowid__ FROM read_parquet(['sql |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_tombstone_stream_rejected](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass | RuntimeError: stream failed: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/tombstone/lifecycle_missing_tombstone.parquet"  LINE 1: ... EXISTS dv_e4161b5a9af69e2e AS SELECT DISTINCT __rowid__ FROM rea |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_datafile_rejected](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass | RuntimeError: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/data/lifecycle_missing_data.parquet"  LINE 1: ... value, COLUMNS(c -> c IN ('__rowid__', '__timestamp__')) FROM parquet_scan(['sqlmatrix/wa |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_missing_datafile_stream_rejected](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass | RuntimeError: stream failed: IO Error: No files found that match the pattern "sqlmatrix/warehouse/tables/cachecheck/data/lifecycle_missing_data.parquet"  LINE 1: ... value, COLUMNS(c -> c IN ('__rowid__', '__timestamp__')) FROM parquet_scan |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_compaction](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_compaction_stream](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__lifecycle_after_compaction_warmed_repeat](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__compact_ledger](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [01_supertable-sql-fixed-baseline-20260914__compact_evolving](runs/01_supertable-sql-fixed-baseline-20260914/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_orders](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_customers](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_items](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_numbers](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_events](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_ledger](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_evolving](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_nulls](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_empty_table](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_temporal_dates](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_temporal_naive](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_tz_instants](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__ingest_tz_budapest_storage](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_timezone_pruned](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_timezone_fullscan](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_timezone_stream](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_timezone_auto](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_timezone_query_sql](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__session_base_and_cursor](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [02_supertable-timezones-newyork-recheck__source_stability](runs/02_supertable-timezones-newyork-recheck/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_orders](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_customers](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_items](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_numbers](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_events](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_ledger](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_evolving](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_nulls](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_empty_table](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_temporal_dates](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_temporal_naive](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_tz_instants](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__ingest_tz_budapest_storage](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_timezone_pruned](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_timezone_fullscan](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_timezone_stream](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_timezone_auto](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_timezone_query_sql](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__session_base_and_cursor](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [03_Asia_Kathmandu__source_stability](runs/03_Asia_Kathmandu/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_orders](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_customers](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_items](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_numbers](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_events](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_ledger](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_evolving](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_nulls](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_empty_table](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_temporal_dates](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_temporal_naive](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_tz_instants](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__ingest_tz_budapest_storage](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_timezone_pruned](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_timezone_fullscan](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_timezone_stream](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_timezone_auto](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_timezone_query_sql](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__session_base_and_cursor](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [04_Australia_Lord_Howe__source_stability](runs/04_Australia_Lord_Howe/results.json) | pass |  |
| [05_Europe_Budapest__ingest_orders](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_customers](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_items](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_numbers](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_events](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_ledger](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_evolving](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_nulls](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_empty_table](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_temporal_dates](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_temporal_naive](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_tz_instants](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__ingest_tz_budapest_storage](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_timezone_pruned](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_timezone_fullscan](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_timezone_stream](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_timezone_auto](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_timezone_query_sql](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__session_base_and_cursor](runs/05_Europe_Budapest/results.json) | pass |  |
| [05_Europe_Budapest__source_stability](runs/05_Europe_Budapest/results.json) | pass |  |
| [06_Pacific_Apia__ingest_orders](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_customers](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_items](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_numbers](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_events](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_ledger](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_evolving](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_nulls](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_empty_table](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_temporal_dates](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_temporal_naive](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_tz_instants](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__ingest_tz_budapest_storage](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_timezone_pruned](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_timezone_fullscan](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_timezone_stream](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_timezone_auto](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_timezone_query_sql](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__session_base_and_cursor](runs/06_Pacific_Apia/results.json) | pass |  |
| [06_Pacific_Apia__source_stability](runs/06_Pacific_Apia/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_orders](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_customers](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_items](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_numbers](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_events](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_ledger](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_evolving](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_nulls](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_empty_table](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_temporal_dates](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_temporal_naive](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_tz_instants](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__ingest_tz_budapest_storage](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_timezone_pruned](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_timezone_fullscan](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_timezone_stream](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_timezone_auto](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_timezone_query_sql](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__session_base_and_cursor](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [07_Pacific_Kiritimati__source_stability](runs/07_Pacific_Kiritimati/results.json) | pass |  |
| [08_UTC__ingest_orders](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_customers](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_items](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_numbers](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_events](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_ledger](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_evolving](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_nulls](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_empty_table](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_temporal_dates](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_temporal_naive](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_tz_instants](runs/08_UTC/results.json) | pass |  |
| [08_UTC__ingest_tz_budapest_storage](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_timezone_pruned](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_timezone_fullscan](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_timezone_stream](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_timezone_auto](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_timezone_query_sql](runs/08_UTC/results.json) | pass |  |
| [08_UTC__session_base_and_cursor](runs/08_UTC/results.json) | pass |  |
| [08_UTC__source_stability](runs/08_UTC/results.json) | pass |  |
| [09_America_New_York__ingest_orders](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_customers](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_items](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_numbers](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_events](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_ledger](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_evolving](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_nulls](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_empty_table](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_temporal_dates](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_temporal_naive](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_tz_instants](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__ingest_tz_budapest_storage](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_timezone_pruned](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_timezone_fullscan](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_timezone_stream](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_timezone_auto](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_timezone_query_sql](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__session_base_and_cursor](runs/09_America_New_York/results.json) | pass |  |
| [09_America_New_York__source_stability](runs/09_America_New_York/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_orders](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_customers](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_items](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_numbers](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_events](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_ledger](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_evolving](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_nulls](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_empty_table](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_temporal_dates](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_temporal_naive](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_tz_instants](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__ingest_tz_budapest_storage](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_timezone_pruned](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_timezone_fullscan](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_timezone_stream](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_timezone_auto](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_timezone_query_sql](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__session_base_and_cursor](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [10_Asia_Kathmandu__source_stability](runs/10_Asia_Kathmandu/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_orders](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_customers](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_items](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_numbers](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_events](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_ledger](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_evolving](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_nulls](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_empty_table](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_temporal_dates](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_temporal_naive](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_tz_instants](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__ingest_tz_budapest_storage](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_timezone_pruned](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_timezone_fullscan](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_timezone_stream](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_timezone_auto](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_timezone_query_sql](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__session_base_and_cursor](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [11_Australia_Lord_Howe__source_stability](runs/11_Australia_Lord_Howe/results.json) | pass |  |
| [12_Europe_Budapest__ingest_orders](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_customers](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_items](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_numbers](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_events](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_ledger](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_evolving](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_nulls](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_empty_table](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_temporal_dates](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_temporal_naive](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_tz_instants](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__ingest_tz_budapest_storage](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_timezone_pruned](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_timezone_fullscan](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_timezone_stream](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_timezone_auto](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_timezone_query_sql](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__session_base_and_cursor](runs/12_Europe_Budapest/results.json) | pass |  |
| [12_Europe_Budapest__source_stability](runs/12_Europe_Budapest/results.json) | pass |  |
| [13_Pacific_Apia__ingest_orders](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_customers](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_items](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_numbers](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_events](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_ledger](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_evolving](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_nulls](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_empty_table](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_temporal_dates](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_temporal_naive](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_tz_instants](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__ingest_tz_budapest_storage](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_timezone_pruned](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_timezone_fullscan](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_timezone_stream](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_timezone_auto](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_timezone_query_sql](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__session_base_and_cursor](runs/13_Pacific_Apia/results.json) | pass |  |
| [13_Pacific_Apia__source_stability](runs/13_Pacific_Apia/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_orders](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_customers](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_items](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_numbers](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_events](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_ledger](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_evolving](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_nulls](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_empty_table](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_temporal_dates](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_temporal_naive](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_tz_instants](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__ingest_tz_budapest_storage](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_timezone_pruned](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_timezone_fullscan](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_timezone_stream](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_timezone_auto](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_timezone_query_sql](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__session_base_and_cursor](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [14_Pacific_Kiritimati__source_stability](runs/14_Pacific_Kiritimati/results.json) | pass |  |
| [15_UTC__ingest_orders](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_customers](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_items](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_numbers](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_events](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_ledger](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_evolving](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_nulls](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_empty_table](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_temporal_dates](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_temporal_naive](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_tz_instants](runs/15_UTC/results.json) | pass |  |
| [15_UTC__ingest_tz_budapest_storage](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_timezone_pruned](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_timezone_fullscan](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_timezone_stream](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_timezone_auto](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_timezone_query_sql](runs/15_UTC/results.json) | pass |  |
| [15_UTC__session_base_and_cursor](runs/15_UTC/results.json) | pass |  |
| [15_UTC__source_stability](runs/15_UTC/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_orders](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_customers](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_items](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_numbers](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_events](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_ledger](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_evolving](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_nulls](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_empty_table](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_temporal_dates](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_temporal_naive](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_tz_instants](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__ingest_tz_budapest_storage](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_timezone_pruned](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_timezone_fullscan](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_timezone_stream](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_timezone_auto](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_timezone_query_sql](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__session_base_and_cursor](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |
| [16_supertable-group-alias-followup-recheck__source_stability](runs/16_supertable-group-alias-followup-recheck/results.json) | pass |  |

## Session timezone and pruning coverage

The table below includes all query groups; UTC includes baseline and alias tests. All 430 timezone/coercion-only cases pass; their separate counts are in timezone_coverage.json.

Each session runs in its own worker process. The case oracle uses Python datetime/ZoneInfo rules, and the worker separately verifies its actual session timezone.

| Session timezone | Cases | Executions | Failed executions | Cases with files pruned | Maximum files pruned |
| --- | ---: | ---: | ---: | ---: | ---: |
| America/New_York | 52 | 260 | 0 | 20 | 101 |
| Asia/Kathmandu | 47 | 235 | 0 | 20 | 101 |
| Australia/Lord_Howe | 52 | 260 | 0 | 20 | 100 |
| Europe/Budapest | 52 | 260 | 0 | 20 | 102 |
| Pacific/Apia | 48 | 240 | 0 | 20 | 100 |
| Pacific/Kiritimati | 47 | 235 | 0 | 20 | 99 |
| UTC | 1317 | 6619 | 35 | 413 | 102 |

Pruning counts include cases where at least one observed execution reported a positive FILES_PRUNED value. They report actual file selection, not an assumption that every temporal predicate is prunable.

| Evidence run | Requested timezone | Observed base | Observed new cursor |
| --- | --- | --- | --- |
| [runs/01_supertable-sql-fixed-baseline-20260914](runs/01_supertable-sql-fixed-baseline-20260914/REPORT.md) | UTC | not recorded | not recorded |
| [runs/02_supertable-timezones-newyork-recheck](runs/02_supertable-timezones-newyork-recheck/REPORT.md) | America/New_York | America/New_York | America/New_York |
| [runs/03_Asia_Kathmandu](runs/03_Asia_Kathmandu/REPORT.md) | Asia/Kathmandu | Asia/Kathmandu | Asia/Kathmandu |
| [runs/04_Australia_Lord_Howe](runs/04_Australia_Lord_Howe/REPORT.md) | Australia/Lord_Howe | Australia/Lord_Howe | Australia/Lord_Howe |
| [runs/05_Europe_Budapest](runs/05_Europe_Budapest/REPORT.md) | Europe/Budapest | Europe/Budapest | Europe/Budapest |
| [runs/06_Pacific_Apia](runs/06_Pacific_Apia/REPORT.md) | Pacific/Apia | Pacific/Apia | Pacific/Apia |
| [runs/07_Pacific_Kiritimati](runs/07_Pacific_Kiritimati/REPORT.md) | Pacific/Kiritimati | Pacific/Kiritimati | Pacific/Kiritimati |
| [runs/08_UTC](runs/08_UTC/REPORT.md) | UTC | UTC | UTC |
| [runs/09_America_New_York](runs/09_America_New_York/REPORT.md) | America/New_York | America/New_York | America/New_York |
| [runs/10_Asia_Kathmandu](runs/10_Asia_Kathmandu/REPORT.md) | Asia/Kathmandu | Asia/Kathmandu | Asia/Kathmandu |
| [runs/11_Australia_Lord_Howe](runs/11_Australia_Lord_Howe/REPORT.md) | Australia/Lord_Howe | Australia/Lord_Howe | Australia/Lord_Howe |
| [runs/12_Europe_Budapest](runs/12_Europe_Budapest/REPORT.md) | Europe/Budapest | Europe/Budapest | Europe/Budapest |
| [runs/13_Pacific_Apia](runs/13_Pacific_Apia/REPORT.md) | Pacific/Apia | Pacific/Apia | Pacific/Apia |
| [runs/14_Pacific_Kiritimati](runs/14_Pacific_Kiritimati/REPORT.md) | Pacific/Kiritimati | Pacific/Kiritimati | Pacific/Kiritimati |
| [runs/15_UTC](runs/15_UTC/REPORT.md) | UTC | UTC | UTC |
| [runs/16_supertable-group-alias-followup-recheck](runs/16_supertable-group-alias-followup-recheck/REPORT.md) | UTC | UTC | UTC |

Full details: [timezone coverage](timezone_coverage.json), [original issue verification](original_issue_verification.json).
