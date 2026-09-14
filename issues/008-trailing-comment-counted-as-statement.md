# STREAD-008: A trailing comment is counted as a second SQL statement

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Statement admission correctness

**Recheck:** All 4 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **4 query assertions; 0 additional checks**.

A single SELECT terminated by a semicolon and followed by a line comment is counted as two statements.

## Expected and actual behavior

**Expected:** Treat the input as one executable SELECT and return the same eight rows as the uncommented query.

**Actual:** The four non-helper modes reject it with `only a single statement may be submitted; found 2`.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case syntax_semicolon_then_comment \
  --skip-lifecycle --output /tmp/supertable-stread-008
```

Role: `superadmin`. Representative SQL:

```sql
SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid; -- terminated named-table query
```

## Investigation starting point

The observed rejection occurs in statement classification. Inspect how comment-only trailing parse nodes are counted; the exact normalization change still needs implementation review.

Source locations at the audited revision:

- [supertable/system_query.py:104](../supertable/system_query.py#L104)
- [supertable/system_query.py:149](../supertable/system_query.py#L149)

## Acceptance criteria

- [ ] Ignore comment-only or empty trailing nodes when counting executable statements.
- [ ] Keep true multiple statements rejected, including statements separated by comments.
- [ ] Coordinate the public helper case with STREAD-003, which currently fails earlier for a different reason.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [syntax_semicolon_then_comment](../audit/sql_read_matrix_2026-09-14/failures/syntax_semicolon_then_comment.json) | pruned, fullscan, stream, auto | 4 |

The failure map assigns each case/mode once. A case may be linked from another issue when different modes fail for different reasons. Fixing an earlier failure can reveal a second issue in the same SQL input.

Related issues: [STREAD-003](003-query-helper-appends-limit-after-semicolon.md).

[Back to the issue index](README.md).
