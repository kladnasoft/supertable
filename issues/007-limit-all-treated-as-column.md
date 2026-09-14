# STREAD-007: LIMIT ALL is treated as a required table column

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Column extraction correctness

**Recheck:** All 4 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **4 query assertions; 0 additional checks**.

Four execution modes reject LIMIT ALL because the analyzer treats ALL as a source-column name.

## Expected and actual behavior

**Expected:** Return all eight rows matching the predicate, in oid order, without requiring a column named ALL.

**Actual:** The reader raises `Missing required column(s): warehouse.orders: ALL`. query_sql happens to pass after its appended limit overwrites the ALL node.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_limit_all \
  --skip-lifecycle --output /tmp/supertable-stread-007
```

Role: `superadmin`. Representative SQL:

```sql
SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT ALL
```

## Investigation starting point

The installed DuckDB SQLGlot parser represents ALL as a Column node under Limit. The unrestricted column traversal includes that node in required data columns, and the estimator reports it missing.

Source locations at the audited revision:

- [supertable/utils/sql_parser.py:410](../supertable/utils/sql_parser.py#L410)
- [supertable/utils/sql_parser.py:613](../supertable/utils/sql_parser.py#L613)
- [supertable/engine/data_estimator.py:816](../supertable/engine/data_estimator.py#L816)

## Acceptance criteria

- [ ] Distinguish SQL control syntax from actual table-column references.
- [ ] All modes return the expected rows, including after any query_sql limit-handling fix.
- [ ] Continue rejecting genuinely missing or unauthorized data columns.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [syntax_limit_all](../audit/sql_read_matrix_2026-09-14/failures/syntax_limit_all.json) | pruned, fullscan, stream, auto | 4 |

Related issues: [STREAD-002](002-query-helper-overwrites-row-limits.md).

[Back to the issue index](README.md).
