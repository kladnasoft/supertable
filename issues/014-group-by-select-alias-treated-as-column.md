# STREAD-014: GROUP BY treats SELECT aliases as physical columns

Status: **Open** · Priority: **P2** · Type: SQL dependency resolution

Discovered in the expanded 2026-09-14 recheck. **Seven cases fail in all five modes: 35 failed query assertions.** The initial date/datetime cases exposed the same general alias-resolution defect as the focused ordinary-column and ROLLUP cases.

## Expected and actual behavior

**Expected:** Group by a SELECT alias and return the independently calculated aggregate rows. The date and datetime examples each produce 11 year groups with the expected counts and minimum/maximum values. Direct DuckDB agrees with every saved expectation.

**Actual:** The reader raises `Missing required column(s): warehouse.temporal_dates: yr` before execution. The datetime variant reports `warehouse.temporal_naive: yr`; quoted and ordinary aliases fail similarly.

```sql
SELECT EXTRACT(year FROM event_date) AS yr,
       COUNT(*) AS n,
       MIN(event_date) AS first_value,
       MAX(event_date) AS last_value
FROM temporal_dates
WHERE event_date IS NOT NULL
GROUP BY yr
ORDER BY yr
```

This also affects `SELECT grp AS category ... GROUP BY category`, expression aliases, quoted aliases, and the tested ROLLUP alias. It is an alias-dependency problem, rather than incorrect date arithmetic or timezone filtering.

## Reproduce

Run from the repository root with Docker available and choose fresh output directories:

```bash
python -m scripts.sql_read_matrix --groups temporal \
  --case temporal_date_aggregate_year --skip-lifecycle \
  --output /tmp/supertable-stread-014-date

python -m scripts.sql_read_matrix --groups alias_followup --skip-lifecycle \
  --output /tmp/supertable-stread-014-aliases
```

## Investigation starting point

`SQLParser._is_inside_alias_scope` recognizes ORDER BY, HAVING, and QUALIFY, but omits GROUP BY. The alias-skip condition therefore retains `yr` as a required physical column. The estimator rejects that nonexistent column before the engine can resolve the valid SELECT alias.

- [Alias scopes, sql_parser.py:486](../supertable/utils/sql_parser.py#L486)
- [Column collection, sql_parser.py:759](../supertable/utils/sql_parser.py#L759)
- [Missing-column rejection, data_estimator.py:850](../supertable/engine/data_estimator.py#L850)

Source line numbers refer to the implementation tested in this recheck.

## Verified alternatives

Replacing `GROUP BY yr` with `GROUP BY EXTRACT(year FROM event_date)` or `GROUP BY 1` passed all five native modes. Both corresponding datetime alternatives also passed. The separately tested physical-column/alias-name collision passed and should continue to work after a fix.

## Acceptance criteria

- [ ] Resolve GROUP BY aliases within the correct SELECT scope without requiring nonexistent physical columns.
- [ ] Pass all seven failing cases across pruned, full-scan, streaming, AUTO, and query_sql execution.
- [ ] Handle direct, expression, and quoted aliases, including aliases inside grouping extensions.
- [ ] Preserve DuckDB's physical-column precedence where a GROUP BY identifier also names a real input column; do not discard all matching names indiscriminately.
- [ ] Keep genuinely missing/unauthorized columns rejected and retain the working expression/ordinal alternatives.

## Evidence

| Case | Failed assertions |
| --- | ---: |
| [temporal_date_aggregate_year](../audit/sql_read_matrix_recheck_2026-09-14/failures/temporal_date_aggregate_year.json) | 5 |
| [temporal_timestamp_aggregate_year](../audit/sql_read_matrix_recheck_2026-09-14/failures/temporal_timestamp_aggregate_year.json) | 5 |
| [alias_date_quoted](../audit/sql_read_matrix_recheck_2026-09-14/failures/alias_date_quoted.json) | 5 |
| [alias_timestamp_quoted](../audit/sql_read_matrix_recheck_2026-09-14/failures/alias_timestamp_quoted.json) | 5 |
| [alias_number_direct](../audit/sql_read_matrix_recheck_2026-09-14/failures/alias_number_direct.json) | 5 |
| [alias_number_expression](../audit/sql_read_matrix_recheck_2026-09-14/failures/alias_number_expression.json) | 5 |
| [alias_number_rollup](../audit/sql_read_matrix_recheck_2026-09-14/failures/alias_number_rollup.json) | 5 |

[Recheck report](../audit/sql_read_matrix_recheck_2026-09-14/REPORT.md) · [Issue index](README.md)
