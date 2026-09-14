# SuperTable correctness suite

A standing check that the write path keeps the table consistent and that the
read path answers SQL correctly. Self-contained and safe to run at any time.

```bash
./test_suite/test_suite.sh --all      # both halves (default)
./test_suite/test_suite.sh --write    # write + tombstone correctness
./test_suite/test_suite.sh --read     # SQL read correctness
```

Needs Docker and the project's Python environment. Each run starts its own
throwaway Redis container and writes to a throwaway directory, then removes
both — it never touches a developer's Redis, a deployment's storage, or the
repository. Every `SUPERTABLE_*`/cloud variable is cleared first, so a stray
`.env` cannot redirect it at real storage.

## What makes this a proof rather than a re-description

The expectations never come from SuperTable. If they did, the suite would only
confirm that the code agrees with itself.

- **Write half** — a plain-Python dict in [`shadow.py`](shadow.py) models what
  the table should contain, sharing no code with the library. The checksums
  asserted at the end are computed from the model.
- **Read half** — each query also runs against a bare DuckDB holding the same
  logical rows, with no reflection view, no RBAC view, no deletion-vector
  anti-join, no pruning and no engine routing. The only difference between the
  two runs is SuperTable's read path. Because that leaves DuckDB checking
  DuckDB for pure-SQL cases, the aggregates whose values are fixed by
  arithmetic are *also* asserted against hand-computed Python.

## Write half

120 transactions by default, drawn at random from inserts, multi-row inserts,
updates, multi-row updates, deletes, multi-row deletes, upserts of absent keys,
deletes of absent keys, and stale updates that `newer_than` must reject. Each
is applied to the real table and to the model; the model records a fingerprint
after every transaction, and the suite asserts once at the end — so when the
end disagrees, the trail names the transaction that diverged instead of leaving
a 120-transaction haystack.

A typical run allocates ~99 keys and leaves ~53 live rows across ~33 files with
~23 rows still in the deletion vector, having crossed the auto-compaction
threshold on the way. So the final checksum covers tombstones being applied to
newly written files, compaction physically removing dead rows, and pruning not
dropping live ones.

Randomized to explore interleavings nobody would write down; **seeded** so any
failure reproduces exactly. The seed is printed in the failure message:

```bash
./test_suite/test_suite.sh --write --seed 20260914
```

### Semantics the model reproduces

Each was measured against the real writer, not assumed. Two are easy to get
wrong from first principles and would make a *correct* implementation look
broken:

| behaviour | measured |
| --- | --- |
| `overwrite_columns=[]` | pure append; a duplicate key stays as two rows (there is no read-side dedup) |
| `overwrite_columns=[key]` | upsert — replaces the matching key, inserts when absent |
| `delete_only=True` | tombstones matching keys; a key that does not exist is a no-op |
| `newer_than=col` | **strictly** greater wins; re-writing at the current value is rejected |
| same key twice in one write | **both rows kept** — the writer does not dedupe within a batch |

The last one cannot be represented by a dict, so the workload never generates
it (see `WriteWorkload._keys_for`). That is a limit of the model, and it is
deliberate rather than accidental.

## Read half

~90 cases over a deterministic 200-row fact table plus a dimension table,
written in four batches and then partly updated and partly deleted — so every
read crosses multiple files, superseded rows and a live deletion vector. A
pristine single-file table would make the tombstone anti-join, the pruning and
the projection all no-ops.

Covered: scalar aggregates (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`/`STDDEV_SAMP`/
`STDDEV_POP`/`VAR_SAMP`/`VAR_POP`/`MEDIAN`/`QUANTILE_CONT`, `FILTER` clause),
`GROUP BY` (multi-column, `HAVING`, alias, expression, ordinal, `ALL`),
grouping extensions (`ROLLUP`/`CUBE`/`GROUPING SETS`/`GROUPING()`), window
functions (`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`/running totals/moving
averages/`NTILE`/`FIRST_VALUE`/`LAST_VALUE`/`PERCENT_RANK`/`CUME_DIST`/
`QUALIFY`), date and timestamp filters, `EXTRACT`, `DATE_TRUNC`, date
aggregates and date grouping, joins (inner/left/right/full/cross/self/`USING`/
semi/anti), set operations (`UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT`),
subqueries (scalar/`IN`/`NOT IN`/correlated/derived), CTEs (single, chained,
joined, windowed), `CASE`/`COALESCE`/`NULLIF`, NULL semantics including
three-valued logic and `IS DISTINCT FROM`, string functions, `LIKE`, unicode,
casts, `DISTINCT`, ordering and row bounds.

Every case runs buffered **and** full-scan, and the two must agree: pruning may
only drop files that provably hold no matching row, so a disagreement is an
unsound prune — a class of bug invisible to any single read. A representative
case per feature also runs through streaming, AUTO routing and the public
`query_sql` helper, which are separate code paths.

## Reading a failure

- **Write half** — the message carries the seed to reproduce with, the expected
  versus actual row count, a diagnosis (`rows missing` / `rows resurrected` /
  `a superseded version is being served`), and, when the final state matches an
  earlier expected state, the transaction that failed to take effect.
- **Read half** — the case id, the feature, the SQL, whether the comparison was
  positional or multiset, and either the first differing row or the missing and
  unexpected rows.

## Extending it

- A new SQL case: one `_c(...)` entry in [`read_cases.py`](read_cases.py). The
  oracle handles it automatically; nothing else to write. Set `all_modes=True`
  to have it checked in every execution mode too.
- A new write operation: a branch in `WriteWorkload._dispatch`, the matching
  transition on `Shadow`, and a weight in `OPERATIONS`. Measure the real
  behaviour first — `test_the_stream_actually_exercised_every_operation` will
  fail if an operation stops being generated, but nothing can tell you that the
  model's *rule* is wrong except checking it against the writer.
