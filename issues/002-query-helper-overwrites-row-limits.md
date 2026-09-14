# STREAD-002: query_sql silently overwrites valid row limits

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P1** · Type: Result correctness

**Recheck:** All 5 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **5 query assertions; 0 additional checks**.

The public helper expands results for valid FETCH, parenthesized LIMIT/OFFSET, and commented LIMIT syntax. Other four modes return the requested result.

## Expected and actual behavior

**Expected:** Return exactly three rows. For FETCH FIRST, LIMIT (3), and commented LIMIT, the expected oid values are 1, 2, 3. For the two offset variants, they are 3, 4, 5.

**Actual:** The first three forms return all eight matching rows. The offset variants return six rows, retaining the offset while losing the requested limit.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_fetch_first \
  --skip-lifecycle --output /tmp/supertable-stread-002
```

Role: `superadmin`. Representative SQL:

```sql
SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid FETCH FIRST 3 ROWS ONLY
```

## Investigation starting point

The trailing-limit regex recognizes only numeric LIMIT/OFFSET in a narrow format. `_ensure_sql_limit` appends `LIMIT 100000` to these queries. SQLGlot accepts the duplicate limit and replaces the original bound in the syntax tree.

Source locations at the audited revision:

- [supertable/data_reader.py:573](../supertable/data_reader.py#L573)
- [supertable/data_reader.py:587](../supertable/data_reader.py#L587)
- [supertable/data_reader.py:620](../supertable/data_reader.py#L620)

## Acceptance criteria

- [ ] Preserve the requested top-level row bound and offset for all five failed forms.
- [ ] Apply a default limit only when the query lacks a top-level limit, using syntax-aware handling.
- [ ] Keep ordinary LIMIT, OFFSET, nested limits, LIMIT 0, and helper metadata behavior passing.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [syntax_offset_fetch_next](../audit/sql_read_matrix_2026-09-14/failures/syntax_offset_fetch_next.json) | query_sql | 1 |
| [syntax_fetch_first](../audit/sql_read_matrix_2026-09-14/failures/syntax_fetch_first.json) | query_sql | 1 |
| [syntax_limit_parenthesized](../audit/sql_read_matrix_2026-09-14/failures/syntax_limit_parenthesized.json) | query_sql | 1 |
| [syntax_offset_parenthesized](../audit/sql_read_matrix_2026-09-14/failures/syntax_offset_parenthesized.json) | query_sql | 1 |
| [syntax_limit_comment_between_keyword_and_count](../audit/sql_read_matrix_2026-09-14/failures/syntax_limit_comment_between_keyword_and_count.json) | query_sql | 1 |

Related issues: [STREAD-003](003-query-helper-appends-limit-after-semicolon.md), [STREAD-005](005-grouping-extensions-with-limit-rejected.md), [STREAD-007](007-limit-all-treated-as-column.md), [STREAD-012](012-query-helper-changes-rejection-diagnostics.md).

[Back to the issue index](README.md).
