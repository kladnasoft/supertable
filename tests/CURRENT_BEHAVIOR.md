# CURRENT_BEHAVIOR.md — sealed read semantics of the pre-`__rowid` engine

> Derived from the **actual code and generated SQL**, not from intended/assumed
> behavior. Every claim cites the source it was read from. This document is the
> human-readable companion to the sealed golden corpus under `tests/golden/`.
>
> **The current implementation is the sole compatibility oracle.** Nothing here
> is a recommendation to change; it is a description of what the code does *now*,
> frozen before the internal-row-identifier + hybrid-deletion-vector + compaction
> migration.

The read path builds a stack of per-query DuckDB views and executes the user SQL
on top of them. Chain order (`supertable/engine/duckdb_lite.py:160-247`):

```
reflection (parquet scan) → RBAC → tombstone → dedup → user query
```

---

## The 14 inspection questions

### 1. Current catalog model
A Redis-backed catalog (`supertable/redis_catalog.py`, class `RedisCatalog`) with
Lua-scripted CAS operations. A `SuperTable` (`supertable/super_table.py`)
bootstraps `meta:root` + RBAC. Per logical table the catalog stores:
- **table config** — `{primary_keys, dedup_on_read}` (`RedisCatalog.set_table_config`);
- a **leaf snapshot payload** (`set_leaf_payload_cas`) holding `resources` (the
  active parquet file list), `tombstones` (`deleted_keys` + `primary_keys`),
  `schema`, `snapshot_version`, `location`.

The characterization fixtures reproduce this exact shape from a frozen
`catalog.json` (`tests/characterization/current_reader.py:44-93`,
`inject_catalog`), so the real catalog API — not a stub — feeds the read.

### 2. How active Parquet files are selected
The reader builds a `Reflection` via `DataEstimator.estimate()`
(`supertable/data_reader.py:189`). The active file list is the snapshot
payload's `resources[*].file` — i.e. **whatever the latest leaf payload lists**,
in stored order. There is no time-travel or file pruning by predicate; the read
scans the full active set. Reflection SQL
(`supertable/engine/engine_common.py:384-398`):

```sql
CREATE TABLE <t> AS
SELECT * FROM parquet_scan([<files…>], union_by_name=TRUE, HIVE_PARTITIONING=FALSE);
```

`union_by_name=TRUE` ⇒ columns are matched **by name** across files; a column
absent from one file reads as `NULL` there.

### 3. View / SQL generated for reads
Per-query, uniquely-suffixed views are created then dropped in a `finally`
(`supertable/engine/duckdb_lite.py`). The two semantically important ones:

- **Tombstone** (`engine_common.py:1124-1190`):
  ```sql
  SELECT * FROM <src>
  WHERE NOT EXISTS (
    SELECT 1 FROM (VALUES (…)) AS __tombstones__(pk…)
    WHERE <src>.pk = __tombstones__.pk AND …);
  ```
- **Dedup** (`engine_common.py:1073-1117`):
  ```sql
  SELECT * EXCLUDE (__rn__, __timestamp__) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY pk… ORDER BY __timestamp__ DESC) AS __rn__
    FROM <src>) sub
  WHERE __rn__ = 1;
  ```
The user's SQL runs on top of the final view. Result is materialized to pandas
(`con.execute(executing_query).fetchdf()`).

### 4. How inserts / updates / deletes are physically represented
- **Insert / update**: append-only. Every write is a new physical row in a new
  immutable parquet file, stamped with `__timestamp__`
  (`supertable/data_writer.py:314-320`). An "update" is simply a newer-timestamp
  row with the same primary key; the old row stays on disk and is hidden at read
  time by dedup.
- **Delete**: a **soft tombstone** — the primary-key tuple is added to the
  snapshot payload's `tombstones.deleted_keys` (a list of key-tuples). No physical
  row is removed and no delete-marker row is written. Exclusion happens only via
  the tombstone view's anti-join (Q3).

### 5. Business key columns
The table config's `primary_keys` list (`set_table_config` /
`DedupViewDef.primary_keys` / `TombstoneDef.primary_keys` in
`supertable/data_reader.py:196-235`). Both dedup partitioning and tombstone
matching use this exact column set. There is no implicit/surrogate key.

### 6. Internal timestamp column + type
Column **`__timestamp__`**. The writer injects it (when `dedup_on_read` and the
incoming frame lacks it) as `polars.lit(datetime.now(timezone.utc))`
(`data_writer.py:317-320`) ⇒ a **timezone-aware UTC** datetime at microsecond
precision (Arrow `timestamp[us, tz=UTC]` on disk). It is the dedup `ORDER BY`
column (`DedupViewDef.order_column="__timestamp__"`, `data_reader.py:208`).

### 7. How the latest record is selected
`ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY __timestamp__ DESC)`, keep
`__rn__ = 1` (`engine_common.py:1073-1117`). **DESC ⇒ the highest `__timestamp__`
per key wins**; all other physical versions of that key are dropped.

### 8. How delete markers are detected
There are **no delete-marker rows**. A key is deleted iff its tuple is present in
`tombstones.deleted_keys`, applied by the tombstone anti-join (Q3,
`engine_common.py:1124-1190`). The match uses `=` per key component.

### 9. Timestamps ascending or descending
The stored values are arbitrary instants; the **read orders DESC** to pick the
latest (Q7). Physical file order and physical row order are irrelevant to
correctness — only the `__timestamp__` value matters. (Sealed by
`ts_out_of_order_physical`, `multi_file_order_reversed`.)

### 10. How equal timestamps are resolved
**There is no secondary tiebreaker.** With two same-key rows sharing an identical
`__timestamp__`, `ROW_NUMBER()` picks one **nondeterministically**; which row's
non-key columns survive is undefined (`engine_common.py:1073-1117`). The suite
seals this *only* where the outcome is deterministic — equal-timestamp same-key
rows are compared on the **key/agreed columns**, never on the ambiguous non-key
value (`ts_equal_same_key_same_value`, `ts_equal_same_key_diff_value_keyonly`).
Per the brief, the test introduces **no** tiebreaker of its own.

### 11. How null keys are handled
- **Dedup**: `PARTITION BY` treats `NULL` as a normal group ⇒ all rows whose key
  (component) is `NULL` collapse into **one** partition and dedup to a single row
  (`key_full_null` → 1 row).
- **Tombstone**: the anti-join uses `=`. SQL `NULL = NULL` is `UNKNOWN`, never
  true ⇒ **a tombstone can never delete a NULL-keyed row** (the ghost survives:
  `delete_cannot_target_null_key`). A `NULL` in one component of a composite key
  likewise prevents that tombstone tuple from matching.

### 12. Is output ordering guaranteed?
**No.** Without an explicit `ORDER BY` in the user SQL, neither the views nor the
final `fetchdf()` guarantee row order. Comparison is therefore a **multiset**
(`tests/characterization/comparison.py`); ordered comparison is used only for
scenarios that seal an explicit `ORDER BY` (`scenario.ordered=True`).

### 13. Is schema evolution supported?
Partially, and asymmetrically:
- **Added column in a later file**: supported. `union_by_name=TRUE` fills the
  column with `NULL` for older files. **But** the snapshot `schema` map is the
  authority for column *existence*: a selected column absent from it is rejected
  in preflight, so the added column must also appear in the snapshot schema (the
  writer's evolving-append behavior; fixtures union all files'
  schemas — `fixtures_lib.py:107-126`). Sealed by `schema_added_column`,
  `schema_missing_column`.
- **Different physical column order across files**: harmless — matched by name
  (`schema_reordered_columns`).
- **Widened numeric type** across files: tolerated by `union_by_name`
  (`schema_widened_numeric`).
- **Incompatible types** for the same column name across files (e.g. `int` vs
  `str`): DuckDB's `union_by_name` reconciles by **upcasting to VARCHAR** rather
  than erroring — sealed as-is (`schema_incompatible_types`: `10`→`'10'`).

### 14. DuckDB vs Spark execution differences
Both engines exist. DuckDB (`supertable/engine/duckdb_lite.py`,
`duckdb_pro.py`) is the **default and the oracle that sealed the goldens**. Spark
(`supertable/engine/spark_thrift.py`) builds the *same logical* view chain
(reflection → RBAC → tombstone → dedup) but with engine-specific syntax and
caveats:
- **No `SELECT * EXCLUDE`** in Spark ⇒ dedup builds an explicit column list from
  `DESCRIBE`; if introspection fails it **falls back to exposing `__timestamp__`**
  (`spark_thrift.py:219-249`) — a divergence from DuckDB, which always excludes it.
- **Identifier quoting** differs (backticks vs double-quotes); SQL is transpiled
  via sqlglot (`spark_thrift.py:304-341`).
- **Timestamp typing**: DuckDB-written `TIMESTAMP(NANOS)` parquet needs Spark
  workarounds + a nanos→micros CAST wrapper (`spark_thrift.py:649-773`). (Our
  polars fixtures write `timestamp[us, UTC]`, which Spark reads directly.)
- **Routing**: AUTO never picks Spark unless an *active cluster is registered* and
  the job meets the fleet's min-bytes (`supertable/engine/executor.py:110-181`);
  explicit `engine="spark"` forces it and raises `RuntimeError("No active Spark
  cluster available…")` when none is registered.

Spark is exercised by `tests/characterization/test_cross_engine.py` (skip-tagged,
integration-only); the goldens are **engine-independent logical output** and the
Spark suite is held to the very same sealed bytes. No engine is forced to imitate
the other.

---

## Output coercions sealed as-is (non-obvious)
Rows are produced by `result_df.values.tolist()` (`data_reader.py:374`) then
`pd.NA/NaT/NaN → None`. Consequences frozen into the goldens:
- An **all-numeric** result frame upcasts ints to **float** via numpy (`id 1`→`1.0`;
  `key_int_float_coercion`); a frame containing any str/datetime column stays
  `object` and **preserves ints** exactly (`key_large_int_alone`).
- `__timestamp__` and `__rn__` are `EXCLUDE`d from `SELECT *` ⇒ a user query may
  still **explicitly** name `__timestamp__` (it is only hidden from `*`, not
  blocked — `query_timestamp_explicit_access`).
- `decimal`→float; `date`→`TS:…T00:00:00Z`; these are normalized canonically by
  `TableResult` (`tests/characterization/table_result.py`).

## Ambiguous / nondeterministic behavior (handled, not sealed on the unstable part)
- **Equal-timestamp same-key tiebreak** (Q10) — nondeterministic non-key
  survivor; sealed on key/agreed columns only.
- **NULL-keyed tombstone non-match** (Q11) — deterministic *survival*, sealed.
- **Row order** (Q12) — never sealed as order; multiset compare.

## Error behavior (category + stable substring + phase)
Sealed in `tests/golden/<id>/expected/error.json`; stack traces and absolute
paths are deliberately **not** sealed.

| scenario | category | substring | phase |
|---|---|---|---|
| `basic_zero_file_catalog` | `RuntimeError` | `No parquet files found` | execution |
| `error_missing_parquet` | `RuntimeError` | `No files found` | execution |
| `error_corrupt_parquet` | `RuntimeError` | `No magic bytes` | execution |
| `error_malformed_catalog` | `JSONDecodeError` | *(none)* | catalog |
| `error_missing_key_column` | `RuntimeError` | *(the key name)* | execution |
| `error_missing_timestamp_column` | `RuntimeError` | `__timestamp__` | execution |

> Brief items "invalid delete marker", "duplicate catalog file entry", and
> "unsupported schema mismatch" turned out **not** to raise in the current code
> (DuckDB tolerates extra `VALUES` columns / duplicate scans / `union_by_name`
> upcast). They are therefore sealed as **result** scenarios that capture the real
> non-erroring behavior — `delete_overlong_tombstone_tuple`,
> `key_duplicate_physical_rows`, `schema_incompatible_types` — never forced into
> an error the engine does not actually produce.
