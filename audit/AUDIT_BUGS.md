# SuperTable — correctness, design and performance audit

Six parallel specialist audits of `supertable/` at `1a52c79` (3.2.0). Scope: write
path, read path and engine, storage backends and the Redis key scheme, RBAC and
config, and performance. Security is reported separately in `AUDIT_SECURITY.md`.

Nothing here has been fixed. Every finding carries a proposed fix, not a patch.

Per-subsystem reports with full evidence and probe scripts: `audit/detail/`.

---

## How to read this

Each finding is marked with how strongly it is established:

| mark | meaning |
|---|---|
| **VERIFIED-HERE** | I reproduced or read the code myself, independently of the agent |
| **PROVEN** | the agent ran a probe that demonstrates it; evidence in `audit/detail/` |
| **MEASURED** | a performance number from a real run, not an estimate |
| **REASONED** | argued from code only — treat as a strong lead, not a fact |

`~2,700 tests pass` and a 4,388-query pruning corpus reports zero mismatches. Most
of what follows is therefore **not** caught by the existing suite, and several
findings explain *why* — the tests assert against mocks whose defaults differ from
the real backend, or cover only the shape the bug does not take.

---

## The pattern worth naming first

Five of the six critical findings are the same mistake in different clothes:

> **an "I don't know" answer is treated as "no", where the safe reading is "yes".**

- a folder that cannot be stat-ed is treated as *absent*, so the data is not deleted
- a Redis error is treated as *table does not exist*, so a live table is re-created
- an empty grant set is treated as *unrestricted*, so a revoke grants everything
- a failed catalog read is treated as *no deletion vector*, so deleted rows return
- a failed catalog read is treated as *no share filter*, so the full table is served

In four of the five, the code states the correct invariant in a comment or docstring
immediately above the line that violates it. This is not a knowledge gap in the
codebase; it is a systematic default-direction error. **Fixing them individually
will not stop the sixth one appearing.** The durable fix is a project-level rule:
*a failed check is not a negative result*, enforced by making the helpers return a
tri-state or raise rather than returning `False`.

---

## CRITICAL

### C1 — Arbitrary file and table access through the read path
**VERIFIED-HERE** · `supertable/system_query.py:84-95` · read_path.md

`classify_query` is documented as classifying a query into "an allowed read-path
command", but for anything that is not `EXPLAIN` or `SHOW STATS` it returns the raw
text untouched. There is no allow-list. Any statement that names one real table is
executed verbatim, so DuckDB's own file functions are reachable:

```sql
SELECT p.* FROM read_parquet(['<data file>']) p WHERE EXISTS (SELECT 1 FROM orders)
```

returned a restricted role the masked column, `__rowid__`, and a row the deletion
vector had removed. RBAC, share filters and the deletion vector are all bypassed
because none of them are in this path — they live in views the query never touches.
`read_csv('/etc/hostname')` and `COPY … TO '/tmp/x.csv'` also succeed.

On object storage this reads any tenant's data with the engine's own credentials.

**Fix.** The read path needs a parse-level allow-list, not a prefix check: reject any
query whose FROM/JOIN sources are not resolvable table references — no table
functions, no file paths, no `ATTACH`/`COPY`/`INSTALL`/`LOAD`. sqlglot already parses
every query here, so the check belongs in `classify_query` where the AST is in hand.
Defence in depth: DuckDB's `enable_external_access=false` on the read connection
removes the file functions outright, and should be set regardless.

### C2 — Dropping a table orphans all of its data on object storage
**VERIFIED-HERE** · `simple_table.py:216`, `super_table.py:127`, `staging_area.py:277`

```python
if self.storage.exists(simple_table_folder):   # False on S3/MinIO/Azure/GCS
    self.storage.delete(simple_table_folder)   # never runs
...
self.catalog.delete_simple_table(...)          # runs unconditionally
logger.info(f"Deleted Table (storage): {folder}")   # logs success
```

`exists()` on S3 is `head_object`, and a folder prefix is not an object, so it 404s.
On LOCAL it is `os.path.exists()`, which is `True` for a directory — so this works
in development and silently fails everywhere else. Production is Ceph.

The result: every parquet file is orphaned and billed indefinitely, the catalog
pointer is gone so nothing can find them again, and the log says the storage was
deleted. `super_table.py:126` states the intended invariant — *"if this fails (other
than missing), do not remove Redis meta"* — and the guard defeats it by making a
full bucket indistinguishable from a missing one.

The tests pass because `MagicMock().exists()` is truthy.

**Fix.** Delete by prefix listing, not by existence: `list_files(prefix)` then delete
each key, and treat "listed nothing" as the only success condition for "already
gone". Then remove the catalog entry only if that succeeded. The `MagicMock` in the
tests must be replaced by a fake with real object-store semantics, or this class of
bug remains invisible.

### C3 — A transient Redis error re-creates a live table empty
**VERIFIED-HERE** · `redis_catalog.py:393-398` + `data_writer.py:488` · write_path.md

`_leaf_exists_raw` catches `RedisError` and returns `False` — "I could not check"
becomes "it does not exist". The write path then constructs `SimpleTable(...)` with
the default `create_if_missing=True`, which bootstraps a fresh empty snapshot over
the live table. The agent reproduced it end to end: 6 rows → 1, version 3 → 1, three
files orphaned, and `write()` returned success.

Sentinel's 0.5 s socket timeout makes this reachable under ordinary load.

`SimpleTable`'s own docstring says read callers pass `create_if_missing=False` "so a
missing table surfaces as an error instead of being silently materialized" — the
read path is guarded and the write path is not.

**Fix.** Two independent changes, both needed. `_leaf_exists_raw` must propagate the
`RedisError` rather than answering `False`. And the write path must pass
`create_if_missing=False` on every path except explicit table creation — a write to
a table that does not exist is an error, not a cue to invent one.

### C4 — Revoking a role's access grants it everything
**VERIFIED-HERE** · `supertable/rbac/row_column_security.py:71-73` · rbac_config.md

```python
if not self.tables:
    self.tables = {"*": {"columns": ["*"], "filters": ["*"]}}
```

An empty grant set — exactly what "revoke all tables" produces — is rewritten to
*all tables, all columns, no filters*. The agent proved it: a role denied `secrets`
read `token='hunter2'` after the revoke.

The inconsistency is the tell. Two lines below, a **missing** `columns` key defaults
to `["*"]` but an **empty** list correctly denies. "Empty means none" is already the
house rule one level down; only the table level inverts it.

**Fix.** Delete the substitution. An empty `tables` means no grant, and the caller
that wanted "everything" should say so explicitly with `{"*": ...}`. Audit for
callers relying on the old behaviour first — a role created with no tables today is
silently an admin, so some may exist in the wild and will need an explicit grant.

### C5 — Unsound string pruning drops matching rows
**PROVEN** · `engine_common.py:728` vs `processing.py:2427` · read_path.md

`init_connection` sets DuckDB's `default_collation='nocase'`, so the engine compares
strings case-insensitively. `_pred_overlaps_stored` compares the same strings
byte-wise against per-file min/max. The two disagree, so pruning drops files the
engine would have matched: `WHERE name='BANANA'` returned 0 rows pruned against 1
with fullscan.

The 4,388-query pruning corpus misses this because its string values are all
lowercase and the collation never comes into play.

**Fix.** Make the pruner's comparison match the engine's: case-fold both sides in
the string lane when `default_collation` is `nocase`. Because the setting is a
connection-level property that could change, the safer shape is for the pruner to
fold unconditionally and accept slightly wider bounds — over-retaining a file is
sound, dropping one is not. Then extend the corpus with mixed-case values, which
would have caught this.

---

## HIGH

### H1 — int64 pruning is unsound above 2^53
**PROVEN** · `processing.py:2421` — `float(s_min)` coerces int64 bounds to float64,
which cannot represent consecutive integers past 2^53. `WHERE ts_ns > base` lost 2
of 3 rows. This is the epoch-nanosecond and keyset-pagination shape, so it is
reachable from the OData continuation path.
**Fix.** Compare ints as ints; only widen when one side is genuinely a float. The
same bug was fixed in the pandas→Arrow conversion for sums — this is its twin.

### H2 — The deletion vector is fail-open
**PROVEN** · `data_reader.py:327` raises, caught at `:352`, logged at **DEBUG** —
deleted rows come back and the query returns `Status.OK`. The same handler drops the
share `_row_filter`, so a Redis hiccup serves unfiltered data.
**Fix.** Fail the query. A read that cannot establish the deletion vector or the
share filter must not return rows. If a degraded mode is genuinely wanted it must be
opt-in per request and visible in the response, never the default on an exception.

### H3 — Streamed and buffered results diverge on duplicate column names
**PROVEN** · `arrow_result.py:108` — `SELECT * FROM a JOIN b ON a.id=b.id` raises
polars `DuplicateError` through `execute()` while `stream()` returns the rows. A
regression from the pandas→polars move; an ordinary join is a hard error.
**Fix.** Deduplicate the Arrow schema before constructing the frame, suffixing
collisions the way the previous path did.

### H4 — Row-level security only works with `SELECT *`
**PROVEN** (both agents, independently) · The reflection view projects only the
*requested* columns while the RBAC view filters on a column that may not be among
them, so `SELECT dept FROM emp` and `SELECT sum(amount) FROM orders` raise
BinderException for any role with a row filter. Fails closed, so not a leak — but
column-masked roles cannot run ordinary queries.
**Fix.** The reflection projection must include any column referenced by the RBAC or
share predicate, then drop it after filtering.

### H5 — S3 redirect retry stores a 0-byte object and reports success
**PROVEN** · `s3_storage.py:303-372` — the retry re-sends an already-consumed
`BytesIO` (`bytes sent: [6049, 0]`). Only `write_parquet` passes a file-like body.
**Fix.** Rewind with `seek(0)` before the retry, or pass `bytes` rather than a
stream. Add an assertion on the returned `ContentLength`.

### H6 — Spark row filters are inverted, not merely broken
**REASONED** · `spark_thrift.py:243` — `FilterBuilder` emits `"region" = 'EU'`, and
Spark reads double quotes as a *string literal*, so `=` matches nothing and `!=`
matches **everything**. A row filter that should restrict instead removes the
restriction. Also `:274-290` swallows a failed `DESCRIBE`, silently disabling the
deletion-vector anti-join.
**Fix.** Emit backticks for Spark identifiers; stop swallowing the `DESCRIBE`.
Neither is verifiable here — there is no Thrift server — so this needs a live run
before release.

### H7 — `delete_recursive()` is called but implemented by no backend
**PROVEN** · `staging_area.py:279` — `AttributeError` on LOCAL, silently skipped on
object stores.
**Fix.** Implement it per backend, or replace the call with prefix-listed deletion
(the same fix as C2).

---

## MEDIUM — correctness

| # | finding | where | status |
|---|---|---|---|
| M1 | `newer_than`'s stale filter ignores the deletion vector, so deleted rows veto re-inserts; the same op sequence gives different data depending on whether compaction ran | processing | PROVEN |
| M2 | `bump_root` runs unguarded *after* the leaf commit and re-raises — `write()` throws on a write that already landed, and the caller's retry double-applies it | write path | PROVEN |
| M3 | Lua round-trip rewrites the root doc: empty lists become `{}`, ints >2^53 become floats | `redis_catalog.py:155` | PROVEN |
| M4 | `list_files()` returns prefix-**inclusive** paths while every other method re-applies the prefix; chained into itself it returns empty, fed to `read_bytes` every read fails and is swallowed. `base_prefix` has **zero** test coverage | storage | PROVEN |
| M5 | S3 `delete()` ignores the `Errors` array — a partial delete reports success. Four backends, four different semantics | storage | PROVEN |
| M6 | `set_leaf_payload_cas` is a blind version-incrementing SET, not a CAS; `_assert_lock_still_held` only aborts on literal `False` and swallows a missing method — `FileLocking` has no `is_held`, so the fence silently disappears | write path | REASONED |
| M7 | `delete_simple_table` leaves 6 of 8 keys, including a stale `schema:doc:` and a `meta:table_names` member the UI reads | catalog | PROVEN |
| M8 | The audit chain verifier can never pass — producer hashes `(ids, content_hash)`, verifier hashes `(ids, "")`. Honest and tampered chains are indistinguishable; the function has zero production callers | audit | PROVEN |
| M9 | `SHOW STATS` bypassed the column mask — a role denied `SELECT salary` read its min/max/null-count. Also found while fixing: a **row**-filtered role read bounds spanning rows outside its filter (`region='eu'` saw a `us` salary via `max`). Statistics rows are now masked to the role's allowed columns, and the min/max values are withheld from a row-filtered role — bounds are an aggregate over rows it may not see, so there is no subset of them that corresponds to the rows it may. Shape (columns, types, counts, sizes) is kept. | rbac | **FIXED** — `data_reader._mask_stats_columns` / `_mask_stats_bounds`; sealed by `test_show_stats_rbac.py` |
| M10 | Hour-partitioned artifact paths broke the frame cache's one-entry-per-table invariant: 9 entries after 9 hours; a busy table evicts every other table from all 64 slots | processing | PROVEN |
| M11 | `stream()` drops `explain=`, so `EXPLAIN SELECT *` runs the full scan; `_ensure_sql_limit` emits invalid SQL for a trailing `;` | read path | PROVEN |
| M12 | `create_role` returns a *different* role on a name collision, discarding the requested type and tables | rbac | PROVEN |

---

## Redis key scheme

The scheme is coherent — `supertable:{org}:{scope}:...` with `_safe()` validating
every segment — but the guard around it is weaker than it looks.

- **The guard test catches only literal f-strings.** `.format()`, concatenation,
  `"prefix" + f"tail"`, `%`, `.join`, and Lua `..` all pass it. Three real offenders
  exist (`quality/scheduler.py:744-752`, `quality/config.py:51`,
  `redis_catalog.py:215`), all bypassing `_safe()`. Seven constructors have no test
  at all. **Fix:** assert on the *constructed key* at runtime in a debug mode, or
  match the prefix string rather than the f-string syntax.
- **`query:` is an undocumented fourth position-2 scope**, and
  `query_job_chunks`/`cancel` omit the `doc:` layer every other constructor uses, so
  `query_job_pattern` over-matches the job index SET. Zero callers today — a loaded
  foot-gun rather than a live bug. (This is mine, from the streaming work.)
- **Streaming job doc TTL is never refreshed**, so `reap()` can delete a live
  export's chunks mid-stream. Also mine.

Full constructor inventory: `audit/detail/storage_catalog.md`.

---

## Performance — measured, ranked

Small queries are dominated by fixed per-query overhead, not data work: a `count(*)`
spends **29% of its 77.7 ms in Redis alone**.

| # | finding | measured cost | fix |
|---|---|---|---|
| P1 | `get_storage()` is never memoized — rebuilt **N+1 times per query**, and the MinIO/S3 constructors do a live `bucket_exists` | **6.73 ms/call**, 27–80 ms/query | memoize per process+config |
| P2 | `query_sql` materialises every row twice; the redundant `list(...)` wrapper alone | **813 ms** conversion on a 197 ms query (96k×11); wrapper = **218 ms** | columnwise `zip` — 2.14× faster, byte-identical |
| P3 | `configure_httpfs_and_s3` re-runs per alias per query, bypassing its own thread-local gate | **14.4 ms each**, 43 ms on a 3-table join | honour the existing gate |
| P4 | Compaction is O(F²) — pairwise `concat_with_union` re-copies the accumulator | **700.6 ms vs 183.2 ms** at 100 files | `concat_many_with_union` **already exists, unused** |
| P5 | `compact_tombstones` is O(V×F) — full DV scan per file | **2.68 s vs 0.40 s**; 11.6 s vs 1.6 s at 500 files | hoist one `partition_by` |
| P6 | The pruning index is rebuilt in Python every query via `iter_rows(named=True)` | **235 ms/query** at 1000 files | cache the index beside the frame it derives from |
| P7 | `meta:root` read 7× and `EXISTS`-ed 6× per query; the estimator SCANs **every leaf in the supertable** for a single-table query | 100× byte waste, 13.4 ms at 100 tables | fetch once per query; scan one leaf |
| P8 | Compaction re-downloads the 9.69 MB file it just wrote, **twice**, both sites labelled "footer only" | ~32 MiB redundant GETs per 16 MiB chunk, under the lock | reuse the footer cache that already holds it |
| P9 | `_safe_exists` fires a HEAD before every read, 1:1 | **271 ms/compaction** on MinIO | drop it — the read already raises `FileNotFoundError` |
| P10 | sqlglot parses each query **twice**, the second time with a *different dialect* | — | parse once; the dialect mismatch is a latent correctness bug too |

**A negative result worth keeping:** Arrow `to_pylist()` is **6.6× slower** than the
current row conversion. Do not "fix" P2 in that direction.

18 further candidates were checked and dismissed with reasons in
`audit/detail/performance.md` — including the tombstone `DISTINCT` (already cached)
and every file-bound or column-bound `iter_rows` site (not row-scale).

---

## What was checked and found clean

This matters as much as the findings.

**Write path** — 48 concurrent appends, delete-vs-compact races, same-key upserts:
zero loss or duplication. A 240-operation fuzzer with compaction forced on nearly
every write kept deletion-vector accounting exact. Phase ordering, `required=True`
carry-forward discipline, heartbeat and release, rowid allocation, resource
bookkeeping and partition-column leakage all hold.

**Read path** — a 39-query pruned-vs-fullscan differential across every lane
(OR/NOT/IN, UNION branches, self-joins, CTEs including arithmetic-shifting ones,
correlated EXISTS, all four join types, casts) found **zero** mismatches. Caches are
version-safe, system columns do not leak, reflection-view name reuse is protected by
DuckDB MVCC.

**RBAC** — column masks are the strong part and resisted every shape tried:
`SELECT *`, `t.*`, CTEs, subqueries, set operations, aggregates. Masked references
are denied at parse time and blocked again at bind. The core role lookup correctly
fails **closed** when Redis is down.

**Config** — `test_settings_defaults.py` genuinely enforces declared-vs-built
defaults across all 145 fields, in a clean subprocess, and guards its own exemption
list. The agent initially reported this as missing and corrected itself.

**Storage/catalog** — glob injection is structurally impossible (`_safe` excludes
every metacharacter), `super_table_pattern` does not leak across lakes, `RENAMENX`
provably preserves TTL, connection pooling and local `write_json` atomicity hold.

---

## Suggested order of work

1. **C1** — it is a full bypass of every access control, reachable by any tenant.
2. **C2, C3** — silent data loss in production; both are small, contained fixes.
3. **C4** — a revoke that escalates. Audit existing roles for accidental admins.
4. **C5, H1** — unsound pruning returns wrong answers with no error. Extend the
   corpus with mixed-case strings and >2^53 integers *first*, so the fix is proved.
5. **H2** — fail-open on the deletion vector and share filter.
6. **H4** — column-masked roles cannot run ordinary queries; a functional blocker.
7. Then performance, where P1–P5 are large, contained and well-measured.

Before any of it: the mock-based storage tests need a fake with real object-store
semantics. C2 and M4 were both invisible because `MagicMock()` answers truthy.
