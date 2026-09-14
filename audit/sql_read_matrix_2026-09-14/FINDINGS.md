# Reviewed SQL engine and read-path findings

Tested SuperTable **3.4.1** at revision `2417237a8b7e2e91900fbc7503c35059dba0e030`, using DuckDB 1.5.4, SQLGlot 26.33.0, Polars 1.42.1, PyArrow 18.1.0, LOCAL storage, and isolated real Redis. The existing version edits were preserved. No production implementation was changed.

## Results

| Measurement | Result |
| --- | ---: |
| Query cases / distinct SQL texts | 821 / 816 |
| Positive cases with independently checked Python expectations | 790 |
| Expected rejection cases | 31 |
| Query executions retained after reviewed reruns | 4,139 |
| Passed / failed assertions | 4,037 / 102 |
| Cases failing in at least one mode | 35 |
| Positive cases failing in at least one mode | 30 |
| Rejection-message mismatches, included in the 102 failures | 9 |
| Forbidden-query cases that unexpectedly executed successfully | 0 |
| Independent-oracle disagreements after review | 0 |
| Additional setup, lifecycle, and compaction checks | 35 |
| Additional checks passed / failed | 30 / 5 |
| Harness self-tests | 39 passed |

The additional failures are four path-only snapshot assertions and one decimal-ingestion failure. They are separate from the query execution counts. There were 4,598 raw query attempts across discovery and focused reruns; the consolidated report counts each case/mode's latest observation. All original attempts are retained under `runs/`.

The [full report](REPORT.md) lists every category and failure. [CSV results](case_results.csv) support filtering, and [machine-readable results](results.json) include the complete failing rows and exceptions.

## 1. Path-only snapshots lose deletions and share filters — highest priority

After real upserts, a deletion, an append, and a rejected stale update, `cachecheck` contains **nine logical rows**. Publishing the same existing snapshot through `set_leaf_path_cas` makes both buffered and streaming reads return **12 physical rows**, including superseded versions and the deleted row. Restoring the inline snapshot payload immediately restores the correct nine rows.

With snapshot `_row_filter = 'value >= 100'`, the inline snapshot correctly returns only `(2, 220, 2)`, `(6, 660, 2)`, and `(10, 100, 1)`. Publishing that snapshot by path returns all 12 physical rows in both modes. This is a demonstrated loss of row filtering, beyond a diagnostic inconsistency.

Resources load through a snapshot-path fallback in `DataEstimator.estimate` (`supertable/engine/data_estimator.py:725`). `DataReader.execute` loads tombstones and `_row_filter` only from `leaf.payload` (`supertable/data_reader.py:355`, `:357`, `:399`). The writer itself has path-only publication fallback calls at `supertable/data_writer.py:1160` and `:1730`, so the tested state corresponds to an implementation-supported publication path.

Evidence: [24 lifecycle assertions and exact rows](runs/02_supertable-sql-controls-reviewed/lifecycle/results.json), [mutation trace](runs/02_supertable-sql-controls-reviewed/lifecycle/mutations.json), [snapshot before faults](runs/02_supertable-sql-controls-reviewed/lifecycle/before_faults_leaf.json). All injected metadata was restored. Fix this before relying on snapshot fallback for deleted or restricted data.

## 2. The public query helper silently changes row limits

This query has three expected rows, with `oid` values 1, 2, and 3:

```sql
SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid FETCH FIRST 3 ROWS ONLY
```

Normal, full-scan, streaming, and AUTO execution return the expected three rows. `query_sql` returns **all eight**. `LIMIT (3)` and a comment between `LIMIT` and `3` have the same result. The two tested offset variants return six rows instead of three, retaining offset two while losing the requested bound.

`_ensure_sql_limit` (`supertable/data_reader.py:573`, regex at `:587`) recognizes only a narrow trailing numeric LIMIT/OFFSET shape. It appends `LIMIT 100000` for these valid forms. SQLGlot accepts the duplicate limit and replaces the original bound in its syntax tree, so the rewritten query requests more rows. This is a result correctness failure.

Evidence: [FETCH FIRST](failures/syntax_fetch_first.json), [parenthesized LIMIT](failures/syntax_limit_parenthesized.json), [commented LIMIT](failures/syntax_limit_comment_between_keyword_and_count.json), [OFFSET/FETCH](failures/syntax_offset_fetch_next.json), [parenthesized OFFSET](failures/syntax_offset_parenthesized.json).

The same helper appends LIMIT after terminal semicolons and breaks otherwise valid queries: [terminal semicolon](failures/syntax_terminal_semicolon.json), [trailing whitespace](failures/syntax_semicolon_trailing_whitespace.json), [extra empty statement](failures/syntax_empty_statement_after_semicolon.json). Ordinary numeric `LIMIT 3` and `LIMIT 3 OFFSET 2` pass the matrix.

## 3. DATE_DIFF arguments are corrupted during rewriting

```sql
SELECT eid, DATE_DIFF('day', event_date, DATE '2024-03-15') AS result
FROM warehouse.events ORDER BY eid
```

Direct DuckDB agrees with all 24 predefined Python date differences. Every native mode fails because the rewritten expression becomes `DATE_DIFF('2024-03-15', event_date, CAST('day' AS DATE))`.

`rewrite_query_with_hashed_tables` reparses the original SQL without `read='duckdb'` at `supertable/engine/engine_common.py:668`, then renders it as DuckDB at `:700`. The generic parse misinterprets the dialect-specific argument order. A minimal dependency probe reproduced this transformation; parsing with the DuckDB dialect preserves the expression.

Evidence: [original query and failures](failures/advanced_date_date_diff.json). The equivalent DATE-only expression `DATE '2024-03-15' - event_date` passed all five native modes in `syntax_date_subtraction_alternative`.

## 4. Additional valid DuckDB syntax is rejected or misclassified

| Syntax | Observed behavior | Evidence |
| --- | --- | --- |
| GROUPING SETS, ROLLUP, CUBE followed by LIMIT | SQLGlot rejects each before execution. Forms without LIMIT pass four native modes; the helper adds LIMIT and then fails. Explicit LIMIT variants fail all five modes. | [ROLLUP without explicit limit](failures/advanced_grouping_rollup.json), [ROLLUP with LIMIT](failures/syntax_grouping_rollup_explicit_limit.json) |
| Boolean `IS UNKNOWN` | Direct DuckDB accepts it; SQLGlot rejects it during read-only admission in every native mode. | [IS UNKNOWN](failures/advanced_boolean_is_unknown.json) |
| `LIMIT ALL` | Four modes treat `ALL` as a required data column and fail. The helper happens to pass after replacing that node with its own limit. | [LIMIT ALL](failures/syntax_limit_all.json) |
| Semicolon followed by a line comment | Native admission counts two statements and rejects a single SELECT accepted by direct DuckDB. | [terminated query with comment](failures/syntax_semicolon_then_comment.json) |
| Table-free SELECT, literal UNION, scalar subquery, literal CTE | All six tested cases fail in all five modes with no-table/no-snapshot errors or the separate semicolon helper error. | [literal SELECT](failures/syntax_literal_select.json), [literal CTE](failures/syntax_literal_cte.json) |

`assert_read_only` invokes SQLGlot at `supertable/system_query.py:149`; the installed parser independently reproduces the grouping and `IS UNKNOWN` rejections. `LIMIT ALL` becomes an SQLGlot column node, which the column traversal at `supertable/utils/sql_parser.py:613` includes among required columns. The no-table rejection is explicit at `supertable/utils/sql_parser.py:353`.

For the tested Boolean column, `flag IS NULL` is equivalent to `flag IS UNKNOWN` and passed all five modes in `syntax_boolean_is_null_alternative`. Table-free queries are recorded as a compatibility limitation; no claim is made that they currently have a supported native contract.

## 5. Existing empty tables cannot be queried

Writing an empty Arrow table succeeds and publishes a snapshot containing an `eid: Int64` schema with no resources. Nevertheless, projection, COUNT, and a broader aggregate query fail in all five modes with `No parquet files found for one or more selected tables.` Expected SQL results are an empty typed projection, COUNT zero, and null SUM/MIN/MAX/AVG values.

Evidence: [projection](failures/controls_empty_physical_projection.json), [COUNT](failures/controls_empty_physical_count.json), [aggregate](failures/advanced_null_aggregate_empty_table.json), [published empty-table schema](runs/02_supertable-sql-controls-reviewed/fixture_manifest.json).

Empty results from populated tables, false predicates, out-of-range predicates, typed null projections, and ordinary `LIMIT 0` all passed.

## 6. Decimal ingestion fails during Parquet statistics extraction

Writing the events table through `DataWriter` fails with `ArrowNotImplementedError: Cannot extract statistics for type`. The tested `Decimal(12,3)` values round-trip intact through all five isolated Parquet encoding probes. The failure occurs when PyArrow 18.1.0 accesses min/max statistics for integer-backed decimals emitted by the native Polars writer; the raw min/max values remain accessible.

`_route_stats` (`supertable/processing.py:1942`) accesses `stat.min`/`stat.max` before its decimal exclusion. Default PyArrow byte-array decimal encoding and Polars `use_pyarrow=True` avoid this specific statistics error. Disabling statistics also avoids the failing access. These are diagnostic comparisons, not production changes.

The matrix retains `ingest_events` as failed and uses an explicitly recorded PyArrow fixture fallback for that table so decimal and temporal reads can still be tested. The other eight logical tables use successful `DataWriter` ingestion. Evidence: [encoding probes](ingest_probes.json), [latest setup result](runs/04_supertable-sql-numeric-review/results.json).

## 7. Rejection diagnostics are inconsistent, while access remains denied

The helper produces parsing or generic COMMAND errors for forbidden INSERT, CREATE, DROP, and DESCRIBE instead of the expected specific read-path rejection. A denied column inside a CTE produces a DuckDB binder error in all five modes instead of the explicit column-permission error. These nine assertions fail the expected-message checks. None of these queries returned forbidden rows or successfully performed the forbidden operation.

Evidence: [INSERT](failures/controls_reject_insert.json), [CREATE](failures/controls_reject_create.json), [DROP](failures/controls_reject_drop.json), [DESCRIBE](failures/controls_reject_describe.json), [denied CTE column](failures/controls_rbac_denied_column_cte.json).

## What passed

- All 264 scalar cases and all 221 relational cases passed across the five modes: boundaries, null logic, comparisons, text operations, joins, correlated subqueries, CTEs, and duplicate-preserving set operations.
- Aggregations, windows, ranks, frames, most date operations, tested decimal arithmetic, aliases, schema evolution, and typed nulls passed except for the specific cases above.
- Ordinary row and column restrictions passed, including filters on unprojected columns, joins, CTEs, unions, disabled/missing roles, and denied tables. The snapshot share-filter failure is separately demonstrated above.
- Warmed reads refreshed after upserts, deletes, appends, and stale-version rejection. Logical results remained correct after compaction. Ledger and schema-evolution cases passed 34 additional buffered/streaming query executions after compaction.
- Missing or corrupt optional statistics fell back safely in the tested scenarios. Missing required data or tombstone files raised errors. A pruning assertion confirmed that an out-of-range file was actually excluded; streaming consumed real Arrow batches of up to 17 rows.

## Dataset, verification, and reproduction

The [logical dataset](dataset.json) contains 829 rows across nine tables, plus the separate lifecycle fixture. All [expected rows](expectations.json) are calculated before native execution using Python values and explicit SQL null behavior. Every one of the 790 positive expectations agrees with direct DuckDB over the logical Arrow input. Native results are not used as golden expectations.

One initial allowlist-order expectation was corrected to the role API's canonical alphabetic ordering, and all 80 controls were rerun. Exact integer/decimal comparisons were strengthened to reject rounded floats, with the five affected numeric cases rerun; Decimal AVG intentionally uses a floating expectation. Cross-process hash-seed tests now verify deterministic expectation artifacts, including unordered results. Earlier oracle disagreements remain visible in the archived discovery run and are excluded from confirmed product defects.

From the repository root:

```bash
python -m scripts.sql_read_matrix --output /tmp/supertable-full-reproduction
python -m scripts.sql_read_matrix --groups controls --output /tmp/supertable-lifecycle-reproduction
python -m scripts.sql_read_matrix --case syntax_fetch_first --skip-lifecycle --output /tmp/supertable-limit-reproduction
python -m pytest scripts/sql_read_matrix/test_harness.py -q
```

Use fresh output directories and the recorded dependency versions. Docker is required for isolated Redis. The runner exits nonzero while assertions or ingestion fail. [Harness instructions](../../scripts/sql_read_matrix/README.md) describe all switches and artifact formats. This validates LOCAL/DuckDB behavior; Spark execution, cloud storage, concurrency, and production-scale performance remain outside the tested scope.
