# STREAD-005: GROUPING SETS, ROLLUP, and CUBE with LIMIT are rejected

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Parser compatibility

**Recheck:** All 21 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **21 query assertions; 0 additional checks**.

Valid grouping extensions followed by LIMIT fail read-path admission. The public helper triggers this even when the caller supplies no limit.

## Expected and actual behavior

**Expected:** Preserve grouping-set detail rows, subtotals, grand totals, and GROUPING flags. The complete Python expectations agree with direct DuckDB.

**Actual:** Explicit-limit forms fail in all five modes. Forms without a limit pass four modes, but query_sql appends a limit and then fails. SQLGlot reports an unexpected token at the limit count.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_grouping_rollup_explicit_limit \
  --skip-lifecycle --output /tmp/supertable-stread-005
```

Role: `superadmin`. Representative SQL:

```sql
SELECT region, status, GROUPING(region) AS region_total, GROUPING(status) AS status_total, COUNT(*) AS n FROM warehouse.orders WHERE oid <= 24 GROUP BY ROLLUP (region, status) LIMIT 100
```

## Investigation starting point

Installed SQLGlot 26.33.0 accepts the tested grouping queries without LIMIT but rejects the same queries with LIMIT when parsing as DuckDB. This was reproduced independently of native execution.

Source locations at the audited revision:

- [supertable/system_query.py:149](../supertable/system_query.py#L149)
- [supertable/data_reader.py:573](../supertable/data_reader.py#L573)

## Acceptance criteria

- [ ] Accept all three grouping constructs with and without an explicit top-level LIMIT.
- [ ] All five modes match each complete expected result, including duplicate and null grouping rows.
- [ ] If changing parser version or handling, keep read-only admission and genuine multi-statement rejection intact.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [advanced_grouping_sets](../audit/sql_read_matrix_2026-09-14/failures/advanced_grouping_sets.json) | query_sql | 1 |
| [advanced_grouping_rollup](../audit/sql_read_matrix_2026-09-14/failures/advanced_grouping_rollup.json) | query_sql | 1 |
| [advanced_grouping_cube](../audit/sql_read_matrix_2026-09-14/failures/advanced_grouping_cube.json) | query_sql | 1 |
| [syntax_grouping_sets_no_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_sets_no_limit.json) | query_sql | 1 |
| [syntax_grouping_sets_explicit_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_sets_explicit_limit.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_grouping_rollup_no_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_rollup_no_limit.json) | query_sql | 1 |
| [syntax_grouping_rollup_explicit_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_rollup_explicit_limit.json) | pruned, fullscan, stream, auto, query_sql | 5 |
| [syntax_grouping_cube_no_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_cube_no_limit.json) | query_sql | 1 |
| [syntax_grouping_cube_explicit_limit](../audit/sql_read_matrix_2026-09-14/failures/syntax_grouping_cube_explicit_limit.json) | pruned, fullscan, stream, auto, query_sql | 5 |

Related issues: [STREAD-002](002-query-helper-overwrites-row-limits.md), [STREAD-004](004-date-diff-rewrite-corrupts-arguments.md), [STREAD-006](006-is-unknown-rejected.md).

[Back to the issue index](README.md).
