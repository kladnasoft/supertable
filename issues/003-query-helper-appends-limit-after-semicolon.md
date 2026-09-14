# STREAD-003: query_sql appends LIMIT after a statement terminator

Status: **Semicolon fix verified; literal-query sample remains unsupported** · Priority: **P2** · Type: SQL formatting correctness

**Recheck:** Four original assertions now pass. The remaining literal-query sample reaches the explicit table-free rejection tracked in STREAD-009; its original positive result is still unavailable. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **5 query assertions; 0 additional checks**.

A valid SELECT ending with a semicolon becomes invalid when the public helper adds its default limit after the terminator. Whitespace, trailing comments, and empty trailing statements expose variants of the same problem.

## Expected and actual behavior

**Expected:** A terminal semicolon and harmless trailing whitespace/comments should preserve the SELECT and its result. The named-table example returns eight rows.

**Actual:** The helper appends a new line containing `LIMIT 100000` after `SELECT ...;`, producing a parse error.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_terminal_semicolon \
  --skip-lifecycle --output /tmp/supertable-stread-003
```

Role: `superadmin`. Representative SQL:

```sql
SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid;
```

## Investigation starting point

`_ensure_sql_limit` strips terminators only for its regex check, then appends the suffix to the original SQL text rather than a normalized single SELECT.

Source locations at the audited revision:

- [supertable/data_reader.py:573](../supertable/data_reader.py#L573)
- [supertable/data_reader.py:590](../supertable/data_reader.py#L590)

## Acceptance criteria

- [ ] Place the default limit inside the single SELECT statement while preserving comments safely.
- [ ] Handle terminal semicolons, trailing whitespace, and empty trailing statements without introducing a second statement.
- [ ] Continue rejecting multiple executable statements.
- [ ] Coordinate the literal-query and trailing-comment cases with STREAD-009 and STREAD-008; fixing this helper failure can reveal those separate failures.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [syntax_terminal_semicolon](../audit/sql_read_matrix_2026-09-14/failures/syntax_terminal_semicolon.json) | query_sql | 1 |
| [syntax_semicolon_trailing_whitespace](../audit/sql_read_matrix_2026-09-14/failures/syntax_semicolon_trailing_whitespace.json) | query_sql | 1 |
| [syntax_semicolon_then_comment](../audit/sql_read_matrix_2026-09-14/failures/syntax_semicolon_then_comment.json) | query_sql | 1 |
| [syntax_empty_statement_after_semicolon](../audit/sql_read_matrix_2026-09-14/failures/syntax_empty_statement_after_semicolon.json) | query_sql | 1 |
| [syntax_literal_select_semicolon](../audit/sql_read_matrix_2026-09-14/failures/syntax_literal_select_semicolon.json) | query_sql | 1 |

The failure map assigns each case/mode once. A case may be linked from another issue when different modes fail for different reasons. Fixing an earlier failure can reveal a second issue in the same SQL input.

Related issues: [STREAD-002](002-query-helper-overwrites-row-limits.md), [STREAD-008](008-trailing-comment-counted-as-statement.md), [STREAD-009](009-table-free-selects-unsupported.md), [STREAD-012](012-query-helper-changes-rejection-diagnostics.md).

[Back to the issue index](README.md).
