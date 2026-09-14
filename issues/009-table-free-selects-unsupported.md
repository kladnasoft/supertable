# STREAD-009: Table-free SELECTs and literal CTEs cannot execute

Status: **Unsupported in the current implementation** · Priority: **P3** · Type: Compatibility / feature decision

**Recheck:** The six literal SELECT/CTE cases were changed to expect a reads-no-table rejection. Replaying their original positive expectations still fails in all five modes. This issue accounts for 29 of those original failures; the remaining one was initially mapped to STREAD-003. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **29 query assertions; 0 additional checks**.

The read path cannot execute SELECT expressions without physical tables, including literal unions, scalar subqueries, and CTEs built only from literals.

## Expected and actual behavior

**Expected:** If this SQL capability is supported, return the predefined constant results, for example one row `(42, NULL, TRUE)` for the selected example.

**Actual:** Literal queries fail with `No tables found in SQL query`; literal CTEs fail with `No snapshots selected`.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_literal_select \
  --skip-lifecycle --output /tmp/supertable-stread-009
```

Role: `superadmin`. Representative SQL:

```sql
SELECT 42 AS answer, CAST(NULL AS INTEGER) AS missing, TRUE AS flag
```

## Investigation starting point

`SQLParser._extract_tables` explicitly rejects a query with no table nodes. CTE names can get past that check but still leave no physical snapshots for execution. Treat this as a capability decision rather than assuming a previously supported contract.

Source locations at the audited revision:

- [supertable/utils/sql_parser.py:331](../supertable/utils/sql_parser.py#L331)
- [supertable/utils/sql_parser.py:353](../supertable/utils/sql_parser.py#L353)
- [supertable/engine/data_estimator.py:670](../supertable/engine/data_estimator.py#L670)

## Acceptance criteria

- [ ] Decide explicitly whether table-free SELECTs belong in the supported read surface.
- [ ] If supported, allow safe read-only execution without fabricated tables or snapshots and make all six cases match their predefined results.
- [ ] If intentionally unsupported, record a consistent capability error and update the test classification explicitly; do not silently replace expected values with observed output.
- [ ] Coordinate the terminal-semicolon literal case with STREAD-003.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [syntax_literal_select](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_select.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_literal_select_semicolon](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_select_semicolon.json) | pruned, fullscan, stream, auto | 4 |
| [syntax_literal_cte](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_cte.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_literal_union](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_union.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_literal_scalar_subquery](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_scalar_subquery.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_literal_cte_declared_columns](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_cte_declared_columns.json) | pruned, fullscan, stream, auto, query_sql | 5 |

The failure map assigns each case/mode once. A case may be linked from another issue when different modes fail for different reasons. Fixing an earlier failure can reveal a second issue in the same SQL input.

Related issues: [STREAD-003](003-query-helper-appends-limit-after-semicolon.md).

[Back to the issue index](README.md).
