# STREAD-013: Denied columns inside a CTE produce binder errors

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P3** · Type: Authorization diagnostic consistency

**Recheck:** All 5 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **5 query assertions; 0 additional checks**.

A query that references a denied column inside a CTE is blocked by the restricted engine view, but reaches DuckDB instead of receiving the explicit column-permission rejection.

## Expected and actual behavior

**Expected:** Under eu_reader, reject the reference to note with the same column-permission diagnostic as equivalent direct, predicate, ordering, aggregate, and join references.

**Actual:** All five modes raise a binder error that note is missing from the restricted relation. No forbidden rows are returned.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --case controls_rbac_denied_column_cte \
  --skip-lifecycle --output /tmp/supertable-stread-013
```

Role: `eu_reader`. Representative SQL:

```sql
WITH hidden AS (SELECT oid, note FROM orders) SELECT note FROM hidden
```

## Investigation starting point

The observed error establishes that the restricted view omits the column, while the earlier permission check does not emit the expected diagnostic for this CTE shape. Trace physical-column lineage and permission validation; the exact missed branch still needs investigation.

Source locations at the audited revision:

- [supertable/utils/sql_parser.py:410](../supertable/utils/sql_parser.py#L410)
- [supertable/data_reader.py:215](../supertable/data_reader.py#L215)

## Acceptance criteria

- [ ] Resolve CTE column references to their physical sources before reporting column-permission failures.
- [ ] Reject this query with the expected permission diagnostic in all five modes while retaining the engine-view restriction.
- [ ] Keep allowed CTE projections, aliases, and aggregates passing.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [controls_rbac_denied_column_cte](../audit/sql_read_matrix_2026-09-14/failures/controls_rbac_denied_column_cte.json) | pruned, fullscan, stream, auto, query_sql | 5 |

These five failures concern error messages. Access remained denied in every tested mode.

Related issues: [STREAD-012](012-query-helper-changes-rejection-diagnostics.md).

[Back to the issue index](README.md).
