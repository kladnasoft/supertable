# STREAD-004: SQL rewriting corrupts DuckDB DATE_DIFF arguments

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Dialect correctness

**Recheck:** All 5 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **5 query assertions; 0 additional checks**.

A valid DuckDB DATE_DIFF query fails in all five native modes because rewriting changes the meaning and order of its arguments.

## Expected and actual behavior

**Expected:** For every event, return the Python date difference `(date(2024, 3, 15) - event_date).days`. Direct DuckDB agrees with all 24 predefined rows.

**Actual:** The expression becomes `DATE_DIFF('2024-03-15', event_date, CAST('day' AS DATE))`, then fails with an invalid date-format conversion error.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case advanced_date_date_diff \
  --skip-lifecycle --output /tmp/supertable-stread-004
```

Role: `superadmin`. Representative SQL:

```sql
SELECT eid, DATE_DIFF('day', event_date, DATE '2024-03-15') AS result FROM warehouse.events ORDER BY eid
```

## Investigation starting point

`rewrite_query_with_hashed_tables` reparses with `sqlglot.parse_one(original_sql)` without the input DuckDB dialect, then renders the misinterpreted tree as DuckDB. A dependency probe reproduced the transformation; parsing with `read='duckdb'` preserves the expression.

Source locations at the audited revision:

- [supertable/engine/engine_common.py:659](../supertable/engine/engine_common.py#L659)
- [supertable/engine/engine_common.py:668](../supertable/engine/engine_common.py#L668)
- [supertable/engine/engine_common.py:700](../supertable/engine/engine_common.py#L700)

**Verified alternative:** For the tested DATE column, `DATE '2024-03-15' - event_date` passed all five modes in `syntax_date_subtraction_alternative`.

## Acceptance criteria

- [ ] Preserve the source dialect when reparsing and rewriting the query.
- [ ] All five modes match the predefined DATE_DIFF results, including differences on both sides of the comparison date.
- [ ] Check related dialect-sensitive expressions when changing the shared rewriter.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [advanced_date_date_diff](../audit/sql_read_matrix_2026-09-14/failures/advanced_date_date_diff.json) | pruned, fullscan, stream, auto, query_sql | 5 |

Related issues: [STREAD-005](005-grouping-extensions-with-limit-rejected.md), [STREAD-006](006-is-unknown-rejected.md).

[Back to the issue index](README.md).
