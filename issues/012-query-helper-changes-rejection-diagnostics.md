# STREAD-012: query_sql changes forbidden-statement rejection diagnostics

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P3** · Type: Diagnostic consistency

**Recheck:** All 4 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **4 query assertions; 0 additional checks**.

The public helper rewrites forbidden SQL and reports a parser or generic COMMAND error instead of the specific read-path rejection returned by other modes.

## Expected and actual behavior

**Expected:** Reject INSERT, CREATE, DROP, and DESCRIBE with the same specific operation-not-permitted diagnostics as direct execution.

**Actual:** INSERT, DROP, and DESCRIBE acquire an appended LIMIT and fail parsing. CREATE reports COMMAND rather than CREATE. All four operations remain rejected; no forbidden operation succeeded.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case controls_reject_insert \
  --skip-lifecycle --output /tmp/supertable-stread-012
```

Role: `superadmin`. Representative SQL:

```sql
INSERT INTO orders (oid) VALUES (999)
```

## Investigation starting point

When initial classification raises ValueError, query_sql treats the input as a SELECT and runs its limit insertion before the reader classifies it again.

Source locations at the audited revision:

- [supertable/data_reader.py:593](../supertable/data_reader.py#L593)
- [supertable/data_reader.py:620](../supertable/data_reader.py#L620)

## Acceptance criteria

- [ ] Preserve the original read-only/admission error instead of rewriting a query whose classification already failed.
- [ ] Keep all four forbidden operations rejected in every mode and retain the expected specific diagnostics.
- [ ] Keep legitimate SELECT default-limit behavior working.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [controls_reject_insert](../audit/sql_read_matrix_2026-09-14/failures/controls_reject_insert.json) | query_sql | 1 |
| [controls_reject_create](../audit/sql_read_matrix_2026-09-14/failures/controls_reject_create.json) | query_sql | 1 |
| [controls_reject_drop](../audit/sql_read_matrix_2026-09-14/failures/controls_reject_drop.json) | query_sql | 1 |
| [controls_reject_describe](../audit/sql_read_matrix_2026-09-14/failures/controls_reject_describe.json) | query_sql | 1 |

These four failures concern error messages. They are not successful writes or an authorization bypass.

Related issues: [STREAD-002](002-query-helper-overwrites-row-limits.md), [STREAD-003](003-query-helper-appends-limit-after-semicolon.md).

[Back to the issue index](README.md).
