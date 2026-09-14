# STREAD-006: Boolean IS UNKNOWN is rejected during SQL admission

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Parser compatibility

**Recheck:** All 5 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **5 query assertions; 0 additional checks**.

DuckDB accepts `flag IS UNKNOWN`, but SuperTable rejects the query before execution in all five modes.

## Expected and actual behavior

**Expected:** Return true for null flags and false for true/false flags, matching all 24 predefined event rows.

**Actual:** Read-only admission fails with an unexpected token at UNKNOWN.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case advanced_boolean_is_unknown \
  --skip-lifecycle --output /tmp/supertable-stread-006
```

Role: `superadmin`. Representative SQL:

```sql
SELECT eid, flag IS UNKNOWN AS result FROM warehouse.events ORDER BY eid
```

## Investigation starting point

SQLGlot 26.33.0 rejects this expression even with the correct DuckDB input dialect. The failure occurs in `assert_read_only` before estimation or rewriting.

Source locations at the audited revision:

- [supertable/system_query.py:104](../supertable/system_query.py#L104)
- [supertable/system_query.py:149](../supertable/system_query.py#L149)

**Verified alternative:** For the tested Boolean column, `flag IS NULL` is equivalent and passed all five modes in `syntax_boolean_is_null_alternative`.

## Acceptance criteria

- [ ] Accept the expression while maintaining the existing read-only restrictions.
- [ ] Match the expected Boolean results for true, false, and null input flags in all five modes.
- [ ] Keep IS TRUE, IS FALSE, IS NULL, and existing three-valued Boolean tests passing.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [advanced_boolean_is_unknown](../audit/sql_read_matrix_2026-09-14/failures/advanced_boolean_is_unknown.json) | pruned, fullscan, stream, auto, query_sql | 5 |

Related issues: [STREAD-005](005-grouping-extensions-with-limit-rejected.md).

[Back to the issue index](README.md).
