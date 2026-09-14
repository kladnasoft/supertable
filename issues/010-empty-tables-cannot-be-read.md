# STREAD-010: Existing empty tables fail projections and aggregates

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Empty relation correctness

**Recheck:** All 15 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **15 query assertions; 0 additional checks**.

DataWriter accepts an empty Arrow table and publishes its schema, but the read path refuses the resulting resource-free snapshot.

## Expected and actual behavior

**Expected:** A projection returns zero rows with the declared eid Int64 column. COUNT returns one row containing zero. The aggregate case returns zero counts, null SUM/AVG/MIN/MAX, and the explicit COALESCE default.

**Actual:** All five modes reject all three cases with `No parquet files found for one or more selected tables.`

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case controls_empty_physical_count \
  --skip-lifecycle --output /tmp/supertable-stread-010
```

Role: `superadmin`. Representative SQL:

```sql
SELECT COUNT(*) AS n FROM empty_table
```

## Investigation starting point

The fixture manifest confirms an existing snapshot with eid schema and no resources. The read path treats absence of Parquet resources as an execution error rather than a typed empty relation; inspect estimator/executor setup for that distinction.

Source locations at the audited revision:

- [supertable/data_reader.py:215](../supertable/data_reader.py#L215)
- [supertable/engine/data_estimator.py:670](../supertable/engine/data_estimator.py#L670)

## Acceptance criteria

- [ ] Build a typed empty relation from the declared snapshot schema when an existing table has zero resources.
- [ ] Match all three projection/aggregate expectations in every mode, including streaming schema.
- [ ] Keep missing-table and missing-required-file failures distinct from an existing empty table.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [advanced_null_aggregate_empty_table](../audit/sql_read_matrix_2026-09-14/failures/advanced_null_aggregate_empty_table.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [controls_empty_physical_projection](../audit/sql_read_matrix_2026-09-14/failures/controls_empty_physical_projection.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [controls_empty_physical_count](../audit/sql_read_matrix_2026-09-14/failures/controls_empty_physical_count.json) | pruned, fullscan, stream, auto, query_sql | 5 |

Related issues: [STREAD-009](009-table-free-selects-unsupported.md).

[Back to the issue index](README.md).
