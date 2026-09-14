# SQL engine and read-path validation

Source revision: `2417237a8b7e2e91900fbc7503c35059dba0e030`. Package: `3.4.1`.

## Method

The dataset and every expected row were constructed before queries ran. Expected results use plain Python arithmetic, grouping, joins, and explicit SQL null rules. They are not golden outputs captured from SuperTable. A direct DuckDB connection over the logical Arrow input independently checks each positive oracle; disagreements are reported separately and must be reviewed before treating a failure as a product defect.

Data ingestion was attempted through DataWriter into multiple real local Parquet files, with real Redis metadata and locks in an isolated disposable container. Setup failures are retained as failures, and affected tables use an explicit PyArrow fixture fallback to allow read coverage to continue; see the fixture manifest and additional checks. Ledger replacement/deletion and schema evolution were performed through the writer. Native queries ran with pruning, full scans, Arrow batches, and AUTO routing without Spark registrations. Ordered queries compare complete row order; other queries compare duplicate-preserving multisets. Column names/order are checked, with explicit dtype checks where declared. Floats use absolute and relative tolerances of 1e-9; integers and decimals remain exact.

## Results

| Measure | Count |
| --- | ---: |
| query cases | 6 |
| distinct sql | 6 |
| expected success cases | 6 |
| expected rejection cases | 0 |
| executions | 30 |
| passed | 0 |
| failed | 30 |
| failing query cases | 6 |
| independent oracle disagreements | 0 |
| additional checks | 16 |
| additional check passes | 16 |
| additional check failures | 0 |
| categories | 1 |
| positive query failures | 6 |
| rejection message mismatches | 0 |
| unexpected rejection case successes | 0 |

## Coverage by category

| Category | Queries | Failing queries |
| --- | ---: | ---: |
| sql_literal_source | 6 | 6 |

## Reproducible failures

Each linked record includes SQL, the complete expected result, actual failing results, role, mode, and comparison detail.

| Case | Category | Failing modes | First failure |
| --- | --- | --- | --- |
| [syntax_literal_select](failures/syntax_literal_select.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |
| [syntax_literal_select_semicolon](failures/syntax_literal_select_semicolon.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |
| [syntax_literal_cte](failures/syntax_literal_cte.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |
| [syntax_literal_union](failures/syntax_literal_union.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |
| [syntax_literal_scalar_subquery](failures/syntax_literal_scalar_subquery.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |
| [syntax_literal_cte_declared_columns](failures/syntax_literal_cte_declared_columns.json) | sql_literal_source | pruned, fullscan, stream, auto, query_sql | ValueError: this query reads no table; SuperTable executes reads against its own tables only, so table-free SELECTs (literal projections, literal UNIONs, and CTEs built only from literals) are not supported |

## Oracle review

Every positive Python oracle agreed with direct DuckDB over the predefined logical input.

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
| ingest_orders | pass |  |
| ingest_customers | pass |  |
| ingest_items | pass |  |
| ingest_numbers | pass |  |
| ingest_events | pass |  |
| ingest_ledger | pass |  |
| ingest_evolving | pass |  |
| ingest_nulls | pass |  |
| ingest_empty_table | pass |  |
| session_timezone_pruned | pass |  |
| session_timezone_fullscan | pass |  |
| session_timezone_stream | pass |  |
| session_timezone_auto | pass |  |
| session_timezone_query_sql | pass |  |
| session_base_and_cursor | pass |  |
| source_stability | pass |  |
