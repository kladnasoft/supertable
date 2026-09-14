# STREAD-001: Snapshot fallback exposes deleted rows and ignores share filters

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P1** · Type: Data correctness and access filtering

**Recheck:** All 4 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **0 query assertions; 4 additional checks**.

When Redis stores a snapshot path without an inline payload, reads load the physical resources but lose the deletion vector and snapshot share filter. This reproduces in buffered and streaming reads.

## Expected and actual behavior

**Expected:** After the prepared upserts, deletion, append, and stale-update rejection, return nine logical rows. With `_row_filter = 'value >= 100'`, return only `(2, 220, 2)`, `(6, 660, 2)`, and `(10, 100, 1)`.

**Actual:** Both modes return 12 physical rows, including superseded/deleted rows. With the share filter, they still return all 12 instead of three. Restoring the inline payload immediately restores the expected result.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --groups controls \
  --output /tmp/supertable-stread-001
```

The controls run also prepares and mutates the separate lifecycle table, then injects and restores the metadata variants. Representative read:

```sql
SELECT kid, value, revision FROM cachecheck ORDER BY kid
```

## Investigation starting point

The resource estimator reads the snapshot-path fallback, while `DataReader.execute` obtains tombstones and `_row_filter` only from `leaf.payload`. The writer has path-only publication fallbacks, so this is an implementation-supported metadata state.

Source locations at the audited revision:

- [supertable/data_reader.py:355](../supertable/data_reader.py#L355)
- [supertable/data_reader.py:399](../supertable/data_reader.py#L399)
- [supertable/engine/data_estimator.py:725](../supertable/engine/data_estimator.py#L725)
- [supertable/data_writer.py:1160](../supertable/data_writer.py#L1160)
- [supertable/data_writer.py:1730](../supertable/data_writer.py#L1730)

## Acceptance criteria

- [ ] Resolve one consistent snapshot for resources, tombstones, schema, and share filters, whether metadata is inline or stored by path.
- [ ] Both path-only modes return nine logical rows without the share filter and exactly the three allowed rows with it.
- [ ] Missing or unreadable required deletion/filter metadata must raise an error instead of exposing additional rows.
- [ ] Keep the existing mutation, cache refresh, compaction, and missing-required-file checks passing.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [lifecycle_path_only_leaf_pruned](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/results.json) | pruned | 1 |
| [lifecycle_path_only_leaf_stream](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/results.json) | stream | 1 |
| [lifecycle_path_only_share_filter_pruned](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/results.json) | pruned | 1 |
| [lifecycle_path_only_share_filter_stream](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/results.json) | stream | 1 |

Four additional lifecycle assertions; these are separate from the 102 failed query assertions. The fixture injects each metadata state into its disposable table and restores it afterward.

Additional details: [lifecycle results](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/lifecycle/results.json), [mutation trace](../audit/sql_read_matrix_2026-09-14/runs/02_supertable-sql-controls-reviewed/lifecycle/mutations.json).

Related issues: [STREAD-002](002-query-helper-overwrites-row-limits.md).

[Back to the issue index](README.md).
