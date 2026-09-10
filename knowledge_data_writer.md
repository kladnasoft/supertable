# SuperTable — Write Path Knowledge Base

**Derived from executable code only** (`supertable` v3.0.3). Every statement below is traced
to a statement, a control-flow branch, or a runtime-verified value, with `file:line` anchors.
No claim rests on a docstring, comment, or document. Where a symbol's *name* implies behaviour
its body does not have, that is stated as a code fact in
[§13 Names that do not match behaviour](#13-names-that-do-not-match-behaviour).

---

## 1. Scope and entry point

There is no method literally named `data_write`. The write path is:

| Layer | Symbol | File |
|---|---|---|
| Public class | `DataWriter` | [data_writer.py:63](supertable/data_writer.py:63) |
| Primary method | `DataWriter.write(...)` | [data_writer.py:253](supertable/data_writer.py:253) |
| Maintenance twin | `DataWriter.compact(...)` | [data_writer.py:1153](supertable/data_writer.py:1153) |
| Config mutator | `DataWriter.configure_table(...)` | [data_writer.py:73](supertable/data_writer.py:73) |
| Input guard | `DataWriter.validation(...)` | [data_writer.py:1646](supertable/data_writer.py:1646) |
| Storage/merge kernel | `supertable/processing.py` (2 475 lines) | [processing.py](supertable/processing.py) |
| Snapshot commit | `SimpleTable.update(...)` | [simple_table.py:323](supertable/simple_table.py:323) |

The audit event emitted at the end is literally `Actions.DATA_WRITE`
([data_writer.py:1138](supertable/data_writer.py:1138)) — that is the closest thing to a
"data_write" identifier in the codebase.

### Signature

```python
DataWriter(super_name: str, organization: str)

.write(role_name, simple_name, data, overwrite_columns,
       compression_level=1, newer_than=None, delete_only=False, lineage=None)
    -> tuple[total_columns, total_rows, inserted, deleted] | None
```

`data` is passed straight to `polars.from_arrow(data)`
([data_writer.py:312](supertable/data_writer.py:312)) — so it must be a
`pyarrow.Table` / `RecordBatch`, **not** a polars or pandas frame.

Return value is a 4-tuple, or **`None`** if an early code path left `result_tuple` unset
(only reachable when `delete_only` is false, `overwrite_columns` is empty and the incoming
frame is empty — see [§4.10](#410-quirk-the-none-return)).

---

## 2. Architecture in one paragraph

SuperTable is a **merge-on-read, copy-on-write-metadata** lake.
Data files are immutable Parquet objects. A write **never rewrites an existing data
file**: it appends one new Parquet file for the incoming rows and records the `__rowid__`
of every superseded row in a **deletion vector** (a "tombstone" Parquet with columns
`__file__` + `__rowid__`). Physical removal is deferred to compaction. Table state is a
**snapshot JSON** on storage, pointed at by a **Redis leaf pointer**; the pointer is the
commit. Redis also holds the per-table write lock and the `__rowid__` allocator.

```
                DataWriter.write()
                        │
      ┌─────────────────┼──────────────────────────────┐
      │                 │                              │
   Redis            Object store                    Redis
 (lock, rowid,      (parquet data,                (leaf pointer
  table config)      tombstone, stats,             = the COMMIT,
                     snapshot JSON)                 root version)
```

Three immutable, versioned artifact families per table, all carried forward on every write:

| Artifact | Path root | Content | Snapshot key |
|---|---|---|---|
| Data files | `<simple_dir>/data/year=/month=/day=/` | user rows + `__rowid__` + `__timestamp__` | `resources[]` |
| Deletion vector | `<simple_dir>/tombstone/year=/month=/day=/hour=/` | `__file__`, `__rowid__` | `tombstone`, `tombstone_rows` |
| Column stats | `<simple_dir>/stats/year=/month=/day=/hour=/` | footer min/max per (file × row-group × column) | `stats_file`, `stats_rows` |
| Snapshot | `<simple_dir>/snapshots/` | the JSON below | (pointed to by Redis leaf) |

---

## 3. Object graph built by the constructor

```python
def __init__(self, super_name, organization):          # data_writer.py:64
    self.super_table = SuperTable(super_name, organization)
    self.catalog = RedisCatalog()
    self._table_config_cache = {}
```

**`SuperTable(...)` is not inert — it bootstraps.**
[super_table.py:45](supertable/super_table.py:45):

1. `is_reserved_super_name(super_name)` → `ValueError` for underscore-wrapped names.
2. `self.storage = get_storage()` — the backend singleton.
3. `self.catalog = RedisCatalog()`.
4. `self.super_dir = os.path.join(organization, super_name, "super")`.
5. **Fast path**: `if catalog.root_exists(org, sup): return` — no storage I/O at all.
6. Otherwise (`create_if_missing=True`, the writer's default): `init_super_table()`
   (`storage.makedirs(super_dir)` + `catalog.ensure_root(...)`), then constructs
   **`RoleManager(...)` and `UserManager(...)`**
   ([super_table.py:83-84](supertable/super_table.py:83)) — RBAC scaffolding is
   materialised as a side effect of instantiating `DataWriter`.

This is why `compact()` performs an explicit `root_exists` / `leaf_exists` pre-flight
*before* calling `check_write_access`
([data_writer.py:1282-1285](supertable/data_writer.py:1282)): it must not mint catalog
state for a ghost table. **`write()` has no such pre-flight** — a write to a
non-existent table intentionally bootstraps it.

`import supertable` also has a global side effect: `config/homedir.py` creates the app home
directory and **`chdir`s the process into it** (observed at import time). All relative
storage paths are therefore resolved against the app home, not the caller's cwd.

---

## 4. `write()` — staged walkthrough

Every stage is timed via a closure `mark(stage)` writing into `profiler.timings`
([data_writer.py:289](supertable/data_writer.py:289)), and a `Profiler` collects
`counts` for I/O attribution.

### 4.0 Structure

The whole body is `try / except / finally`, and — critically — **nothing returns from
inside the `try`**. Early exits set `result_tuple` / `stats_payload` and fall through, so
that the `finally` (lock release) and the three post-lock blocks (monitoring, data quality,
audit) always run. This is an explicit invariant, sealed by
`test_stale_early_return_skips_monitoring_enqueue`.

### 4.1 `access` — RBAC

`check_write_access(super_name, organization, role_name, table_name=simple_name)`
([data_writer.py:303](supertable/data_writer.py:303)). Raises on denial; the exception
propagates out of `write()` (sealed by `test_permission_error_propagates`).

### 4.2 `convert` — Arrow → polars

`dataframe = polars.from_arrow(data)`; `incoming_rows = height`,
`incoming_columns = width` are captured **before** any system column is added, so the
returned `total_columns` is the *logical* (user-facing) width
([data_writer.py:925](supertable/data_writer.py:925)).

### 4.3 `validate`

`self.validation(...)` [data_writer.py:1646](supertable/data_writer.py:1646):

| Rule | Raise |
|---|---|
| `1 <= len(simple_name) <= 128` | `ValueError` |
| `simple_name != super_name` | `ValueError` |
| `^[A-Za-z_][A-Za-z0-9_]*$` | `ValueError` |
| `overwrite_columns` is not a `str` | `ValueError` |
| all `overwrite_columns` present in the frame | `ValueError` |
| `newer_than` is a `str` and present in the frame | `ValueError` |
| `newer_than` requires non-empty `overwrite_columns` | `ValueError` |

Runs **before** rowid reservation so invalid input never burns a range of the Redis
counter.

### 4.4 `rowid` — table-unique identity

```python
if not delete_only and incoming_rows > 0:                    # data_writer.py:330
    start_rowid = self.catalog.reserve_rowids(org, sup, simple_name, incoming_rows)
    dataframe = dataframe.with_columns(
        polars.int_range(start_rowid, start_rowid + incoming_rows,
                         dtype=polars.Int64).alias("__rowid__"))
```

A contiguous block is reserved from a per-table Redis counter (`INCRBY`, first id = 1), so ids
are unique across concurrent writers **and processes**. `with_columns` **overwrites** any
caller-supplied `__rowid__` — it is system-owned. `__rowid__` is the sole identity used by the
deletion vector and by the read-side anti-join.

Because reservation happens **before the lock**, an aborted write burns its range permanently —
rowid gaps are normal and carry no meaning. The counter also survives
`delete_simple_table`, so a recreated table continues from the old high-water mark.

### 4.5 `dedup_ts` — `__timestamp__` injection

```python
if not delete_only:                                          # data_writer.py:360
    dataframe = dataframe.with_columns(
        polars.lit(datetime.now(timezone.utc)).alias("__timestamp__"))
```

One constant value for the whole batch (the write's wall-clock), also always overwriting a
caller-supplied column. Its three actual uses in code are:

1. Primary **sort key** before encoding, for tight row-group zonemaps
   ([processing.py:675-681](supertable/processing.py:675));
2. Presence switch selecting the **Hive-style shard folder** write path
   ([processing.py:629](supertable/processing.py:629));
3. Excluded from query output on read
   ([engine_common.py:1280](supertable/engine/engine_common.py:1280)).

It is **not** used as a dedup ORDER BY key anywhere — see [§13](#13-names-that-do-not-match-behaviour).

`table_config = self._get_table_config(simple_name)` is fetched in the same stage: local
dict cache, falling back to one Redis `get_table_config` per process per table
([data_writer.py:141](supertable/data_writer.py:141)).

### 4.6 `lock` — the critical section opens

```python
token = self.catalog.acquire_simple_lock(org, sup, simple_name,
                                         ttl_s=30, timeout_s=60)   # data_writer.py:367
if not token: raise TimeoutError(...)
```

Per-**simple-table** lock (not per-super). Everything from here to the `finally` runs under
it. `compact()` takes the *same* lock with the *same* parameters, so compaction and writes are
mutually serialised. The 30 s TTL is **not** an operation deadline — a heartbeat thread extends
it every 15 s. See [§14.2](#142-the-lock--acquire_simple_lock) for the full algorithm and its
fairness/failure properties.

### 4.7 `snapshot` — read current state

`SimpleTable(self.super_table, simple_name)` — default `create_if_missing=True`, so a
first write bootstraps the table (dirs + a version-0 empty snapshot + leaf pointer,
[simple_table.py:151](supertable/simple_table.py:151)).

`get_simple_table_snapshot()` ([simple_table.py:234](supertable/simple_table.py:234)):

1. `catalog.get_leaf(...)` → `{"path": ..., "payload": ...}`; missing `path` →
   `FileNotFoundError`.
2. If `payload` is a dict with a list `resources` → **return it directly, no storage
   read** (the fast path).
3. Nested shape `payload["snapshot"]` supported.
4. Else `storage.read_json(path)`.

Returns `(snapshot_dict, snapshot_path)`.

**Snapshot shape** (bootstrapped at [simple_table.py:170](supertable/simple_table.py:170),
extended by `update`):

```jsonc
{
  "simple_name": "facts",
  "location": "<org>/<super>/tables/facts",
  "snapshot_version": 7,
  "last_updated_ms": 1757500000000,
  "previous_snapshot": "<...>/snapshots/<ms>_<hex>_tables.json",
  "schema": {"id": "Int64", "name": "String"},     // NOTE: a DICT (see §13)
  "schemaString": "{\"type\":\"struct\",\"fields\":{...}}",
  "resources": [ {"file": "...parquet", "file_size": 12345, "rows": 1000, "columns": 8} ],
  "tombstone": "<...>/tombstone/year=.../deleted.parquet" | null,
  "tombstone_rows": 0,
  "stats_file": "<...>/stats/year=.../stats.parquet" | null,
  "stats_rows": 0,
  "lineage": { ... }                                // present once any write ran
}
```

### 4.8 `overlap` — candidate selection

`find_overlapping_files(last_simple_table, dataframe, overwrite_columns, ...)`
([processing.py:352](supertable/processing.py:352)) returns a
`set[(file_path, has_overlap: bool, file_size: int)]`:

* **`overwrite_columns` given** → **every** existing resource is added with
  `has_overlap=True`. The snapshot carries no per-file key statistics, so non-overlap
  cannot be proven at this stage. This is O(files), not O(rows).
* **No `overwrite_columns`** (pure append) → only files with `file_size < max_mem` are
  added with `has_overlap=False` — these are *compaction* candidates, not overwrite
  candidates.

Then `prune_not_overlapping_files_by_threshold`
([processing.py:287](supertable/processing.py:287)): all `True` entries are always kept;
`False` entries are kept **only if** `total_size > max_mem` **or**
`count(False) >= max_files` — and when that gate opens, *all* `False` entries are included.

Limits resolve per-table then global (`_resolve_limits`,
[processing.py:33](supertable/processing.py:33)):

| Limit | Per-table key | Global default |
|---|---|---|
| `max_mem` | `max_memory_chunk_size` | `Default.MAX_MEMORY_CHUNK_SIZE` = 16 MiB |
| `max_files` | `max_overlapping_files` | `Default.MAX_OVERLAPPING_FILES` = 100 |
| DV threshold | `max_tombstone_rows` | `Default.MAX_TOMBSTONE_ROWS` = 1 000 000 |

### 4.9 `stats_prune` — provable candidate elimination (overwrite only)

Only when `overwrite_columns` and the snapshot has a `stats_file`
([data_writer.py:405](supertable/data_writer.py:405)):

1. `load_stats(stats_file, allow_cache=True)` — in-process cache first
   ([processing.py:2307](supertable/processing.py:2307)).
2. `probe_ranges_from_df(dataframe, overwrite_columns)`
   ([processing.py:1932](supertable/processing.py:1932)) → per key column
   `(lane, lo, hi)` or `None`. `None` (⇒ that column constrains nothing) whenever:
   the column has **any NULL** (because overwrite matching is `nulls_equal=True`, so a
   NULL key could match a file whose footer range excludes NULLs), the dtype is
   unsupported (`Decimal`, `UInt64`, binary), or the column is empty.
3. `prune_overlapping_files_by_stats(...)`
   ([processing.py:2000](supertable/processing.py:2000)) drops a file **only** when *every*
   row group has at least one constrained column whose range provably cannot overlap.
   Logic is **AND within a row group, OR across row groups**.

**Soundness contract**: every uncertainty (no stats for the file, `stats_available=False`,
lane mismatch, missing column stat) resolves to *retain*. Pruning is purely a performance
optimisation; the tombstone output is identical with or without it.

The probe is the **in-memory dataframe's** range, not a file read — the file about to be
written carries identical footer min/max, so comparing df-range vs stored stats is
equivalent to comparing footers **without opening any file**.

### 4.10 `resolve_overwrite` — stale filter + delete pairs in one pass

`resolve_overwrite_writes(...)` ([processing.py:1322](supertable/processing.py:1322))
returns `(filtered_incoming_df, delete_pairs)`.

Two interchangeable implementations with identical semantics:

**(a) DuckDB pushdown probe** — only when `settings.SUPERTABLE_DUCKDB_WRITE_PROBE` is
truthy (**default off**). `_duckdb_probe_overlap_matches`
([processing.py:1103](supertable/processing.py:1103)) issues one statement:

```sql
SELECT filename, "__rowid__", <overwrite cols>[, <newer_than>]
FROM parquet_scan([...], union_by_name=TRUE, filename=TRUE, hive_partitioning=FALSE) AS src
SEMI JOIN __st_ik_<uuid> AS k
  ON src.<c> IS NOT DISTINCT FROM k.<c> AND ...
```

* `IS NOT DISTINCT FROM` = null-safe equality, matching polars `nulls_equal=True`.
* Incoming unique keys are registered as an Arrow relation, then `unregister`ed in
  `finally`; the connection is a **thread-local pooled** DuckDB connection
  (`get_pooled_duckdb_connection`, [engine_common.py:795](supertable/engine/engine_common.py:795))
  — never closed, amortising ~150 ms warmup.
* Reactive **presign retry**: on an error containing any of
  `"HTTP Error", "HTTP GET error", "301", "Moved Permanently", "AccessDenied",
  "SignatureDoesNotMatch", "403", "400"`
  ([processing.py:1097](supertable/processing.py:1097)) the keys are re-resolved with
  `force_presign=True` and the scan retried once.
* DuckDB's `filename` is joined back to the original storage key so the tombstone stores
  **keys, not URLs**; any unmapped filename aborts the probe (returns `None`).

**(b) polars fallback** (the default path, and the semantic oracle). Reads each
overlapping file **projected to only** `overwrite_columns + [newer_than] + ["__rowid__"]`
([processing.py:1383](supertable/processing.py:1383)) via a shared `file_cache`, then:
`filter_stale_incoming_rows` ([processing.py:767](supertable/processing.py:767)) +
`identify_deleted_rowids` ([processing.py:935](supertable/processing.py:935)).
`profiler.counts["overwrite_resolve_fallback"]` marks this path.

**Ordering guarantee that matters**: delete pairs are derived from the **surviving**
incoming keys, not the raw ones
([processing.py:1311](supertable/processing.py:1311)) — a row rejected by `newer_than`
tombstones nothing.

**`newer_than` semantics** ([processing.py:1291-1305](supertable/processing.py:1291)):
group existing matched rows by key → `max(newer_than)`; keep an incoming row iff
`existing_max IS NULL` (new/legacy key) **or** `incoming > existing_max`. Strictly greater
— equal values are dropped as replays. Join uses `nulls_equal=True`.

If `newer_than` filtered **everything** ([data_writer.py:475](supertable/data_writer.py:475)),
`result_tuple = (incoming_columns, 0, 0, 0)` and a `stats_payload` with
`skipped_stale` is built; the whole tombstone/write/commit block is skipped
(`if result_tuple is None:` at [data_writer.py:519](supertable/data_writer.py:519)).
**No new snapshot is committed** — the write is a no-op.

#### 4.10 quirk: the `None` return

`result_tuple` stays `None` — and `write()` returns `None` — when the big block runs but
never reaches its final assignment. Concretely: `overwrite_columns` empty, `delete_only`
false, `dataframe.height == 0`. Then `do_insert` is false, `new_delete_pairs` is empty…
but the block *does* complete and assigns `result_tuple` at
[data_writer.py:1071](supertable/data_writer.py:1071). So in practice the only `None`
return is an exception path — which re-raises. Callers should still treat the return as
`tuple | None` (test `test_write_with_empty_dataframe_no_overwrite` pins the empty case).

### 4.11 Deletion-vector block

Guarded by `if result_tuple is None:` ([data_writer.py:519](supertable/data_writer.py:519)).

**Load the current DV once** ([data_writer.py:535](supertable/data_writer.py:535)):

```python
prev_dv_df = load_tombstone(prev_tombstone_path, allow_cache=True, required=True, ...)
```

`required=True` is the correctness lever: a DV that **exists but cannot be read** re-raises
and aborts the write ([processing.py:271](supertable/processing.py:271)). Swallowing it to
`None` would carry forward a truncated vector and **resurrect deleted rows**. Genuine
absence still returns `None`.

`prev_dv_rowids` (a Python `set`) is materialised **only** when this write actually
tombstones (`overwrite_columns or delete_only`) — a pure append skips building a
million-element set.

**Which rows die** ([data_writer.py:554](supertable/data_writer.py:554)):

* `overwrite_columns` → `resolved_delete_pairs` from §4.10.
* `delete_only` with no `overwrite_columns` → `identify_all_rowids(resources)`
  ([processing.py:997](supertable/processing.py:997)), which reads **only the
  `__rowid__` column chunk** of every file.
* Pure append → nothing.

**Idempotency filter** ([data_writer.py:573](supertable/data_writer.py:573)): pairs whose
rowid is already in `prev_dv_rowids` are dropped. Without it, every write would re-count
rows that are logically dead but still physically present, inflating `deleted` and forcing
a needless DV rewrite. With it, `deleted` is the true count of **live** rows removed.

### 4.12 `write_parquet` ∥ `build_tombstone` — two concurrent PUTs

```python
if do_insert:                                        # data_writer.py:632
    with ThreadPoolExecutor(max_workers=2) as _ex:
        _f_data = _ex.submit(_write_data_branch)
        _f_tomb = _ex.submit(_write_tombstone_branch)
        data_sub, data_secs = _f_data.result()
        tombstone_path, combined_tombstone_df, tomb_sub, tomb_secs = _f_tomb.result()
    profiler.merge(data_sub); profiler.merge(tomb_sub)
```

The two writes touch disjoint directories (`data/` vs `tombstone/`) and neither reads the
other's output, so the round-trips overlap. **`Profiler` is not thread-safe**, so each
branch gets its own `Profiler()` and the parent merges after the join
([data_writer.py:644](supertable/data_writer.py:644)); each branch also measures its own
wall time because serial `mark()` deltas would misattribute overlapped work.
`.result()` re-raises in the parent — either PUT failing aborts before any commit, leaving
an orphaned but unreferenced object (harmless garbage).

`do_insert = (not delete_only and dataframe.height > 0)`.

#### Data file write — `write_parquet_and_collect_resources`
[processing.py:583](supertable/processing.py:583)

* Returns immediately if `write_df.height == 0`.
* If `__timestamp__` present → target dir is
  `data_dir/year=YYYY/month=MM/day=DD/` derived from **`datetime.now(timezone.utc)`**, i.e.
  the *current write time*, **one bucket for the whole frame** — explicitly not per-row.
  Per-row bucketing would shred a memory-bounded compaction chunk into one tiny file per
  distinct row-day, permanently defeating compaction.
* The partition keys are **not** columns in the file, and the read side passes
  `partitioning=None`; the folder is pure sharding metadata.

`_write_single_parquet_file` ([processing.py:646](supertable/processing.py:646)):

1. `storage.makedirs(target_dir)` (best-effort, swallowed).
2. Filename `generate_filename("data", "parquet")` →
   `f"{epoch_ms}_{secrets.token_hex(8)}_data.parquet"`
   ([helper.py:26](supertable/utils/helper.py:26)) — collision-resistant, no coordination.
3. **Sort**: `["__timestamp__"] + [c for c in overwrite_columns if present]`. Note the
   caller passes `overwrite_columns=[]` here, so in practice the sort is on
   `__timestamp__` alone — which is a *constant* for the batch. The sort therefore only
   does real work during compaction (where mixed timestamps exist).
4. Encode in memory with **polars'** own writer — `write_df.write_parquet(buf,
   compression="zstd", compression_level=…, statistics=True, row_group_size=122_880)`
   ([processing.py:697](supertable/processing.py:697)). Not pyarrow: measured 5.5x faster and
   3.7x smaller at the same zstd level, with row-group statistics and per-column compressed
   sizes preserved. See [§24](#24-measured-performance-profile-100-appends--10m-rows).
5. Upload, by capability probe in this order:
   `write_bytes` → `write_parquet` → `polars.write_parquet` local fallback.
   Any exception in the whole block falls back to `write_df.write_parquet(...)`.
6. **Footer reuse**: only on the `write_bytes` path, `pq.read_metadata(BytesIO(data))` is
   stashed in `footer_md_out[path]` ([processing.py:711](supertable/processing.py:711)) —
   the uploaded bytes *are* `data`, so the footer is authoritative. Deliberately **not**
   done on the other paths, whose re-encode could produce a different row-group layout;
   reusing it there would mis-prune row groups on read.
7. Size: `storage.size()` → `os.path.getsize()` → `len(data)` → `0`.
8. Appends `{"file", "file_size", "rows", "columns"}` to `new_resources`.

#### Tombstone write — `build_tombstone_file`
[processing.py:1433](supertable/processing.py:1433)

* **No new pairs → `(prev_tombstone_path, None)`**: pure carry-forward, the new snapshot
  reuses the previous file, **zero I/O**. `None` as the second element is the caller's
  signal for "vector unchanged".
* Otherwise: `concat(prev[__file__, __rowid__], new)` then
  **`.unique(subset=["__rowid__"], keep="first")`** — dedup is on rowid alone, so a rowid
  can appear once regardless of file attribution.
* Written to `_partitioned_new_path(tombstone_dir, "deleted")` →
  `tombstone/year=YYYY/month=MM/day=DD/hour=HH/<ms>_<hex>_deleted.parquet`
  ([processing.py:1412](supertable/processing.py:1412), UTC via
  `hourly_partition_subpath`, [helper.py:35](supertable/utils/helper.py:35)). The hour
  partition bounds per-folder object counts under heavy write volume; the full path is
  stored in the snapshot so reads are unaffected and old flat-layout files need no
  migration.
* `_write_df_parquet` ([processing.py:883](supertable/processing.py:883)) is the minimal
  writer; when the backend has `write_bytes` it returns `len(data)` directly and **skips
  the `size()` HEAD** round-trip.

`tombstone_rows` = `combined_tombstone_df.height` when the vector changed, else the
carried-forward `last_simple_table["tombstone_rows"]`
([data_writer.py:670](supertable/data_writer.py:670)).

### 4.13 `reclaim_dead_files` — free deletions

Only when `combined_tombstone_df is not None` (the vector changed this write) —
a carry-forward cannot create a newly-dead file
([data_writer.py:689](supertable/data_writer.py:689)).

`reclaim_fully_dead_files` ([processing.py:1488](supertable/processing.py:1488)):
group the DV by `__file__`, and a resource is **fully dead** when
`dead_count >= resource["rows"]`. Such files are dropped from the snapshot for free — no
rewrite — and their rowids removed from the vector. If every DV row is reclaimed,
returns `(fully_dead, None, None)` and the vector is cleared entirely.

Without this, a 100 %-deleted file lingers until the compaction threshold, bloating the
resource list and getting re-scanned by every later overwrite probe.

### 4.14 Compaction inside the write — Phase A then Phase B

Two independent triggers, one ordered physical step
([data_writer.py:721-852](supertable/data_writer.py:721)):

```python
post_write_resources = [r for r in old_resources if r["file"] not in sunset_files] + new_resources
compaction_gate         = should_compact_small_files(post_write_resources, table_config)
tombstone_threshold_hit = combined_tombstone_df is not None and \
                          combined_tombstone_df.height >= _max_tombstone_rows(table_config)
```

`should_compact_small_files` ([processing.py:321](supertable/processing.py:321)):
"small" = `file_size < _small_file_threshold(max_mem)` — i.e. **0.75 × max_mem**, not max_mem.
The gate opens when `len(small) >= max_files` **or** `sum(small) > max_mem`. The hysteresis
band is what stops a freshly merged file from immediately re-qualifying and being rewritten by
the next small append; `compact_resources` applies the identical threshold so the gate and the
candidate set cannot disagree. See [§24.1a](#241-the-four-bottlenecks-this-workload-exposed).

**Phase A — drain the deletion vector** (`if tombstone_threshold_hit or compaction_gate`).
The vector to drain is `combined_tombstone_df`, or — for a pure carry-forward — the
already-loaded `prev_dv_df`, only re-reading from storage as a last resort
([data_writer.py:764](supertable/data_writer.py:764)).
`compact_tombstones` ([processing.py:2391](supertable/processing.py:2391)) is **targeted**:
it reads **only the files named in `__file__`**, anti-joins `__rowid__`, writes the
survivors as new files and sunsets the originals. Survivors keep their original
`__rowid__` (no remapping). `_read_parquet_safe(..., required=True)` here — a swallowed
error would skip a file's dead rows while the pointer is cleared anyway, resurrecting them.
Afterwards `tombstone_path = None`, `tombstone_rows = 0`.

**Ordering is a correctness requirement, not a preference**: `compact_resources`
(Phase B) rewrites data files **without consulting the deletion vector**. If Phase B
sunset a file the vector still referenced, that file's dead rows would be copied into the
new file while the vector kept pointing at the vanished original — hidden on read
(the anti-join is rowid-only) but **permanently unreclaimable**. Draining first guarantees
Phase B only ever sees vector-free survivors.

**Phase B — small-file merge** (`if compaction_gate`).
`compact_resources(snapshot={"resources": live_resources}, ..., small_only=True)`
([processing.py:414](supertable/processing.py:414)) buffers survivors with
`concat_with_union` and flushes a new Parquet whenever accumulated `file_size` reaches
`max_mem`. A source file enters `sunset_files` **only after** its rows are buffered — a
failed read leaves it in the snapshot for the next attempt. The merge is strictly
row-preserving (no dedup, no drops); missing columns are filled with typed nulls.

Then the result is **folded into the same snapshot commit**
([data_writer.py:843](supertable/data_writer.py:843)): a file written earlier in this very
write (incoming data, or a Phase-A survivor) may have been re-merged, so any
`new_resources` entry that is now sunset is filtered out — the snapshot must never list a
file as both live and gone.

**Pinning the pointers** ([data_writer.py:798-808](supertable/data_writer.py:798)):
`last_simple_table["tombstone"] / ["tombstone_rows"]` are set, then `cache_tombstone(...)`
seeds the in-process cache with the fresh frame (or `prev_dv_df` for a carry-forward) so
the *next* write in this process reads the vector from memory.

### 4.15 `build_stats` — the column-statistics artifact

`extract_stats_rows(new_data_files, footer_md_cache=footer_md_cache)`
([processing.py:1765](supertable/processing.py:1765)) → one row per
**(file × row-group × column)**, using the in-memory footer when available
(counter `stats_footer_cache_hit`) and otherwise `_read_footer_metadata`, which downloads
the object and parses only the footer.

`STATS_SCHEMA` ([processing.py:1564](supertable/processing.py:1564)) — sealed, order
significant:

`file_path, row_group_id, column_name, physical_type, logical_type, min_bigint, max_bigint,
min_double, max_double, min_timestamp, max_timestamp, min_string, max_string, null_count,
row_group_rows, compressed_bytes, stats_available, min_is_exact, max_is_exact`

* `__rowid__` / `__timestamp__` are **excluded** (`_STATS_SYSTEM_COLUMNS`).
* `_route_stats` ([processing.py:1628](supertable/processing.py:1628)) buckets footer
  min/max into four lanes: `bigint` (bool + all int widths), `double`, `timestamp`
  (date **and** datetime → tz-normalised naive µs), `string`. **Decimal is deliberately
  unsupported** (double routing is lossy → false negatives); binary/time also unsupported.
  Unsupported ⇒ `stats_available=False` ⇒ never used to exclude anything.
* `compressed_bytes` = `col.total_compressed_size`, nullable — drives projection-aware
  read estimation.

`build_stats_file` ([processing.py:1802](supertable/processing.py:1802)):
new stats = `(prev rows − rows whose file_path ∈ sunset_files) + new rows`.
Nothing new and nothing removed → `(prev_stats_path, None)` (pure carry-forward).
`_conform_stats_schema` ([processing.py:1747](supertable/processing.py:1747)) adds any
schema column an older artifact lacks as typed NULL, so a stats file written before
`compressed_bytes` existed carries forward without raising. `cache_stats(...)` seeds the
in-process cache.

### 4.16 `update_simple` — building the new snapshot

**Schema policy** ([data_writer.py:954](supertable/data_writer.py:954)) — three cases:

| Case | `schema_model_df` |
|---|---|
| Auto-compaction ran | `self._build_compact_model_df(new_resources, last_simple_table)` |
| `delete_only` | `None` → previous schema preserved verbatim |
| Normal write | `dataframe` |

`delete_only` must pass `None` because the incoming frame carries only the delete-predicate
columns; using it would shrink the recorded schema even though the Parquet files are still
full-width. When compaction ran, a merged file may union in columns from older files that
the incoming frame lacks, so the schema is derived from the **compacted output** instead
(`_build_compact_model_df` reads the schema of *all* new files and unions them —
`compact_resources` can emit multiple chunks with independent schemas, so reading only the
first would miss columns).

`SimpleTable.update(...)` ([simple_table.py:323](supertable/simple_table.py:323)) — note it
**mutates `last_snapshot` in place**:

1. `updated_resources = [r for r in current if r["file"] not in set(sunset_files)] + new_resources`
   — **no dedup**. (This is why `compact()` must be careful about what it hands in; see
   [§6](#6-compact--explicit-maintenance).)
2. `previous_snapshot = last_snapshot_path`; `last_updated_ms = now`;
   `snapshot_version += 1`.
3. If `model_df is not None`: `schema = collect_schema(model_df)` →
   `{col: str(dtype)}` — **a dict**, e.g. `{"id": "Int64"}`
   ([helper.py:18](supertable/utils/helper.py:18)). Only if that is empty (zero-column
   frame) does it fall back to `_schema_list_from_polars_df` (a list of Spark-typed field
   dicts). `schemaString = json.dumps({"type":"struct","fields": schema_list})`.
4. `lineage` stored if a dict.
5. `storage.write_json(snapshot_dir/generate_filename("tables"), snapshot)`.
6. Returns `(snapshot_dict, new_snapshot_path)`.

**Effective lineage** ([data_writer.py:929](supertable/data_writer.py:929)): if the caller
passed nothing, an auto lineage is synthesised with
`source_type` = `"delete"`/`"write"`, `role_name`, `overwrite_columns`, `delete_only`,
`incoming_rows`, `incoming_columns`, `write_id=qid`.

### 4.17 `bump_root` — the commit

```python
now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
try:    catalog.set_leaf_payload_cas(org, sup, simple, payload=snapshot_dict,
                                     new_snapshot_path, now_ms=now_ms)
except Exception:
        catalog.set_leaf_path_cas(org, sup, simple, new_snapshot_path, now_ms=now_ms)
catalog.bump_root(org, sup, now_ms=now_ms)
```

The **leaf write is the commit point** — the snapshot JSON is already durable on storage, but
invisible until the pointer moves. The payload variant embeds the whole snapshot in Redis
so readers skip a storage read; the path-only variant is the fallback (sealed by
`test_falls_back_to_set_leaf_path_cas_on_payload_error`), and it is the *hot* degradation path
for wide tables — the payload has **no size limit** in Python, so the bound is Redis's own.

Despite the name, **neither function is a compare-and-swap** — no expected version, no
comparison, no mismatch branch. See [§14.4](#144-the-leaf-cas-is-not-a-cas). Correctness comes
entirely from the lock held around it.

Then, best-effort and individually swallowed
([data_writer.py:999](supertable/data_writer.py:999)):
`catalog.r.set(RK.schema(org, sup, simple), schema_json)` and
`catalog.r.sadd(RK.meta_table_names(org, sup), simple)`. The schema is normalised to a JSON
object from either the dict or list shape.

### 4.18 `mirror`

`MirrorFormats.mirror_if_enabled(super_table, table_name, simple_snapshot)` —
**synchronous, inside the lock**, wrapped in `try/except` that logs and continues
([data_writer.py:1018](supertable/data_writer.py:1018)).

### 4.19 `finally` — lock release

Only `release_simple_lock(..., token)`. A `False` result (token mismatch / TTL expiry) is
logged at debug, not raised; any exception is swallowed. **Nothing else** happens in
`finally`.

---

## 5. After the lock — three post-commit blocks

All three run **outside** any data lock, each independently guarded.

**1. Monitoring** ([data_writer.py:1098](supertable/data_writer.py:1098))

```python
if stats_payload is not None and simple_name not in MONITORING_SINK_TABLES:
    stats_payload["supertables"] = [self.super_table.super_name]
    with MonitoringWriter(organization=..., monitor_type="writes") as monitor:
        monitor.log_metric(stats_payload)
```

`MONITORING_SINK_TABLES` = `{"__reads__", "__writes__", "__mcp__", "__compact__",
"__plans__"}` ([monitoring/partitions.py:93](supertable/monitoring/partitions.py:93)).
The guard breaks a **1:1 amplification cycle**: an orchestrator that drains the `writes`
partition writes it back into `__writes__` via this same `DataWriter`; re-emitting a metric
for that write would generate a fresh metric forever.

Failure is caught and logged — monitoring can never fail a write (sealed by
`test_monitoring_failure_is_swallowed`, and the ordering by
`test_monitoring_called_after_lock_release`).

**2. Data quality** ([data_writer.py:1122](supertable/data_writer.py:1122))
`from supertable.quality.scheduler import notify_ingest` then
`notify_ingest(self.catalog.r, org, sup, simple_name)` — the producer half of a
producer/consumer DQ pipeline. Notably the failure handler **logs a warning** rather than
silently `pass`ing, so a packaging regression is visible instead of a dead no-op.

**3. Audit** ([data_writer.py:1134](supertable/data_writer.py:1134))
`_audit_emit(category=EventCategory.DATA_MUTATION, action=Actions.DATA_WRITE, ...,
detail=make_detail(table, row_count, inserted, deleted, duration_ms, role_name,
delete_only))`, guarded by `if result_tuple is not None` and a bare
`except: pass` — "never fail a write due to audit".

`return result_tuple`.

---

## 6. `compact()` — explicit maintenance

Same lock, same commit machinery, no incoming data
([data_writer.py:1153](supertable/data_writer.py:1153)).

**Differences from `write()`:**

| Aspect | `write()` | `compact()` |
|---|---|---|
| Missing table | bootstraps it | `root_exists`/`leaf_exists` pre-flight → `SuperTableNotFoundError` / `TableNotFoundError`, **before** RBAC |
| `SimpleTable` | `create_if_missing=True` | `create_if_missing=False` |
| DV drain | only at threshold or when the small-file gate fires | **always**, whenever a vector exists |
| DV read | `load_tombstone(allow_cache=True)` | `_read_parquet_safe(required=True)` — **cache bypassed on purpose**: compact always drains, so there is no carry-forward hit to gain, and it never re-seeds after draining |
| `small_only` | forced `True` | caller-controlled (default `True`; `False` re-encodes every file) |
| Profiler | live profiler threaded through | not passed |
| Monitor type | `"writes"` | `"compact"` |
| Return | 4-tuple | a `dict` of metrics |

`force_tombstones` is **accepted and recorded in lineage/result but never branched on** —
the vector is drained whenever `tombstone_rows > 0`
([data_writer.py:1363](supertable/data_writer.py:1363)).

**The double-listing trap** ([data_writer.py:1413-1434](supertable/data_writer.py:1413)):
Phase A's outputs are spliced into `last_simple_table["resources"]` immediately
([data_writer.py:1385](supertable/data_writer.py:1385)), which is the baseline `update()`
starts from. Since `update()` computes `(baseline − sunset) + new_resources` **with no
dedup**, handing it `all_new_resources` would list any Phase-A output that Phase B did not
consume **twice**. Hence two distinct lists:

* `all_new_resources` (both phases, minus anything sunset) → stats extraction, schema
  model, result metrics;
* `update_new_resources` (**Phase B only**, minus sunset) → `simple_table.update`.

**Short-circuit**: if nothing was written and nothing sunset, the snapshot rewrite,
leaf-CAS, root-bump and mirror are all skipped — but execution still falls through to the
`finally` and the monitoring/audit blocks so the attempt stays observable
([data_writer.py:1446](supertable/data_writer.py:1446)).

Compaction does **not** garbage-collect: merged-away files stay in storage, still
referenced by older snapshot versions for time travel.

---

## 7. `configure_table()` — per-table limits

[data_writer.py:73](supertable/data_writer.py:73). RBAC-checked. Accepts
`max_memory_chunk_size`, `max_overlapping_files`, `max_tombstone_rows` — each validated
`> 0` (`ValueError` otherwise) and each `None` meaning "leave unchanged".

A subtlety: existing config is fetched from **Redis** only when at least one override is
being set; otherwise it reads the **local cache**
([data_writer.py:109-116](supertable/data_writer.py:109)) — preserving the
cache-population guarantee for callers that pass no limits. Writes to Redis and updates
`_table_config_cache` so the next `write()` sees it without a round-trip.

---

## 8. Schema union & alignment primitives

Used by every merge (`compact_resources`, `concat_many_with_union`).

`_resolve_unified_dtype` ([processing.py:66](supertable/processing.py:66)) — widening
lattice, evaluated in this order:

1. empty → `Utf8`; single → itself
2. any `Utf8` → `Utf8`
3. any `Datetime` → `Datetime("us", None)`
4. any `Date` → `Date`
5. any float → `Float64`
6. any int → `Int64`
7. else `Utf8`

`_align_to_schema` ([processing.py:116](supertable/processing.py:116)) — the contract that
makes positional concat safe:

* present with target dtype → keep; present with different dtype → `cast(strict=False)`
  (unconvertible values become **null**, silently); absent → typed null literal.
* Uses **`df.select(exprs)`, not `with_columns`** — `with_columns` preserves the input's
  column order and appends new columns at the end, which breaks the positional-concat
  contract of `polars.concat(how="vertical_relaxed")`. *(This is the fix for the historical
  GA4 column-order corruption bug.)*
* **Zero-row defence**: `df.height == 0` returns `polars.DataFrame(schema=target)` —
  otherwise `select([lit(None), ...])` on an empty frame broadcasts the literal into a
  single spurious null row.

`concat_with_union` short-circuits when either side is empty (returning the other frame
**unaligned** — a deliberate cheap path).

---

## 9. In-process caches

`_PathKeyedFrameCache` ([processing.py:2236](supertable/processing.py:2236)) — a
thread-safe LRU keyed by the artifact's **directory**, holding exactly one
`(path, DataFrame)` per table.

* A hit requires the cached path to equal the requested path **exactly**. Because artifact
  filenames are immutable and versioned, a hit can never be stale — a new write produces a
  new path, which misses.
* Cap resolved dynamically per call via a getter (so a settings change or test patch takes
  effect immediately); `<= 0` disables.
* Two instances: `_STATS_CACHE` (cap `settings.SUPERTABLE_STATS_CACHE_MAX_TABLES`) and
  `_TOMBSTONE_CACHE` (cap `settings.SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES`).

Correctness rests on the **table lock + immutable versioned paths**, so no invalidation
service is needed: a stale entry simply misses.

Time-travel reads must pass `allow_cache=False` so they read the old version fresh
**without evicting the cached latest**.

Effect on a looping single-process writer: after the first iteration, both the stats read
(§4.9) and the DV carry-forward read (§4.11) are memory hits — the per-write GETs disappear.

---

## 10. Concurrency & failure model

| Mechanism | Guarantee |
|---|---|
| Per-simple Redis lock (TTL 30 s, wait 60 s) | Serialises `write` vs `write` and `write` vs `compact` on one table. Cross-table writes are fully parallel. |
| `reserve_rowids` (Redis counter) | `__rowid__` unique table-wide across processes, allocated **before** the lock |
| Immutable data files + fresh filenames | No two writers can collide on a path; no in-place mutation |
| Leaf write | The commit point (last-write-wins; the lock, not a CAS, is what makes it safe) |
| Pre-commit lock fence | `verify_simple_lock` immediately before the commit; a lost lock raises `LockLostError` instead of clobbering the writer that holds it now |
| `sunset_files` added **after** successful buffering | A failed read leaves the file in the snapshot for retry |
| `required=True` on DV reads | A readable-but-broken vector aborts the write instead of resurrecting rows |
| `.result()` on both PUT futures | Either failing aborts before commit |

**Orphan semantics**: a crash between the Parquet PUT and the leaf CAS leaves an unreferenced
object. It is invisible (no snapshot lists it) and is left for external GC.

**What is deliberately *not* done**: no per-file locks
([processing.py:406](supertable/processing.py:406) — removed in favour of the table lock),
no retry/backoff around the storage PUTs, no GC of sunset files.

---

## 11. Observability

`Profiler` collects `timings` (stage → seconds) and `counts`. Counters produced by the
write path:

`resources_total`, `overlap_files_true`, `overlap_files_false`,
`overlap_files_total_bytes`, `stats_pruned_files`, `stats_cache_hit/miss`,
`tombstone_cache_hit/miss`, `probe_files`, `probe_rows_matched`,
`overwrite_resolve_fallback`, `delete_files_seen`, `delete_rows_matched`,
`files_read`, `bytes_read`, `rows_read`, `files_written`, `rows_written`,
`bytes_written`, `compact_small_candidates`, `tombstone_files_total`,
`tombstone_files_touched`, `reclaimed_dead_files`, `stats_rows_extracted`,
`stats_rows_total`, `stats_footer_cache_hit`, `read_pruned_files`,
`snapshot_resources_count`, `snapshot_sunset_count`, `snapshot_new_resources_count`.

Two structured log lines are the operational backbone:

1. **Per-stage timing** ([data_writer.py:1057](supertable/data_writer.py:1057)) — every
   `mark()` stage on one line.
2. **"compaction during write"** ([data_writer.py:875](supertable/data_writer.py:875)) —
   emitted **only** when a compaction phase actually ran, reporting the trigger
   (`tombstone_threshold` / `small_file_gate`), per-phase row and file counts, live files
   before → after, and the compaction-attributable I/O. The I/O is isolated by snapshotting
   `files_written / bytes_written / files_read / bytes_read` **before** the phases
   ([data_writer.py:738](supertable/data_writer.py:738)) and diffing — since the incoming
   data write already happened, any delta is purely compaction.

`stats_payload` fields: `query_id`, `recorded_at`, `organization`, `super_name`,
`role_name`, `table_name`, `overwrite_columns`, `compression_level`, `newer_than`,
`delete_only`, `incoming_rows`, `incoming_columns`, `inserted`, `deleted`, `total_rows`,
`total_columns`, `new_resources`, `sunset_files`, `skipped_stale`, `lineage` (JSON string
via `_safe_json`, which never raises), `duration`, `timings`, `counts`, `supertables`.

---

## 12. Write modes — decision table

| `overwrite_columns` | `delete_only` | `newer_than` | Behaviour |
|---|---|---|---|
| `[]` / `None` | `False` | — | **Append.** Nothing tombstoned; one new file; candidate set contains only small files (compaction path). |
| `[c…]` | `False` | `None` | **Upsert.** Matching existing rows tombstoned, all incoming rows appended. |
| `[c…]` | `False` | `col` | **Conditional upsert.** Rows with `incoming[col] <= max(existing[col])` per key are dropped and tombstone nothing. |
| `[c…]` | `True` | — | **Predicate delete.** Matching rows tombstoned; no rows inserted (`do_insert` false, no `__rowid__`/`__timestamp__` injected). |
| `[]` / `None` | `True` | — | **Delete-all.** `identify_all_rowids` tombstones every rowid in every file (reads only the `__rowid__` chunk). |

`delete_only` writes pass `model_df=None`, so the snapshot schema is preserved.

---

## 13. Names that do not match behaviour

Each item is a code fact about a symbol whose name (or the stage name it is filed under)
implies something the body does not do.

**(a) The `dedup_ts` stage does not enable any dedup.**
`__timestamp__` is injected at [data_writer.py:361](supertable/data_writer.py:361) under a
stage marked `dedup_ts`, but nothing consumes it as a dedup key:

* The read view is
  `SELECT COLUMNS(c -> c NOT IN ('__rowid__','__timestamp__')) FROM src ANTI JOIN dv ON …`
  ([engine_common.py:1280-1309](supertable/engine/engine_common.py:1280)). There is **no
  `ROW_NUMBER`, no `QUALIFY`, no `DISTINCT ON`** anywhere in `supertable/engine/` or
  `data_reader.py` — the only "newest wins" resolution in the system is the write-time
  tombstone, not a read-time window function.
* No `__p_year__`/`__p_month__`/`__p_day__` column exists in the tree. The shard folder is
  computed from `datetime.now(timezone.utc)` at write time
  ([processing.py:630-636](supertable/processing.py:630)), not from row values, and the read
  side passes `partitioning=None` ([local_storage.py:205](supertable/storage/local_storage.py:205))
  so the folder is never re-inferred as data.

  What `__timestamp__` is actually used for: the pre-encode sort key
  ([processing.py:676](supertable/processing.py:676)), the branch switch selecting the sharded
  write path ([processing.py:629](supertable/processing.py:629)), and exclusion from query
  output.

**(b) The snapshot `schema` is a dict, not a list of field structs.**
`collect_schema` ([helper.py:18](supertable/utils/helper.py:18)) returns
`{col: str(dtype)}` — verified at runtime: `{'a': 'Int64', 'b': 'String'}`.
`_schema_list_from_polars_df` (the list-of-`{name,type,nullable,metadata}` shape with Spark
type names) is only reached when `collect_schema` returns empty, i.e. a **zero-column**
frame — in which case it returns `[]` too. So the list shape is effectively unreachable.

Consequences:

* `schemaString` serialises as `{"type":"struct","fields":{...}}` — `fields` is an
  **object**, not the array a Spark/Delta consumer expects.
* `DataWriter._build_compact_model_df` fallback #2
  ([data_writer.py:236](supertable/data_writer.py:236)) tests
  `isinstance(prior_schema, list)` and reads `col["name"]` / `col["type"]` through
  `_SPARK_TYPE_TO_POLARS`. Against a normally-written table `schema` is a dict, so this
  branch **never fires**; it falls through to fallback #3 (`polars.DataFrame()`), and
  `update()` then writes `schema = []`, **wiping the schema**. Reachable only when every
  new compacted file's schema read fails — rare, but the fallback exists precisely for that
  case and does not work.

**(c) `force_tombstones` is inert.** The parameter is accepted by `compact()`, copied into
`result` and into the auto-generated lineage, and **never appears in a conditional**. The
vector is drained whenever `tombstone_rows > 0`
([data_writer.py:1363](supertable/data_writer.py:1363)); passing `False` changes nothing.

**(d) The `overwrite_columns` sort in `_write_single_parquet_file` is dead on the write
path.** Both call sites pass `overwrite_columns=[]`
([data_writer.py:610](supertable/data_writer.py:610),
[processing.py:555](supertable/processing.py:555)), so `sort_cols` reduces to
`["__timestamp__"]` — which is a single constant value for a fresh batch, making the sort a
no-op there. It does real work only during compaction, where merged chunks carry mixed
timestamps.

**(e) The Redis snapshot fast path silently disengages on an empty table.**
`get_simple_table_snapshot` trusts the leaf payload only when
`isinstance(payload["resources"], list)`. Redis's lua-cjson cannot distinguish an empty JSON
array from an empty object, so a snapshot whose `resources` is `[]` round-trips as `{}`, the
gate fails, and it falls back to `storage.read_json`. The defence is correct — but it means
the "zero storage I/O" claim only holds once the table has at least one file.

**(f) The `[write]` path uses two different partition conventions.** Data files build their
folder inline with `f"year={now.year}"` — **unpadded**
([processing.py:633](supertable/processing.py:633)) — while `hourly_partition_subpath` (used
for tombstone and stats artifacts) pads with `{dt.year:04d}`. Harmless today because both are
stored as full paths in the snapshot and never parsed, but they are not the same convention.

**(g) The table-denial message is malformed.** `_check_table_access` interpolates the
operation *label* into `f"You don't have permission to {label} table '{table_name}'."`, and the
caller passes `"write to this table"` — producing
`You don't have permission to write to this table table 'x'.` The same doubling occurs for
`check_meta_access` and `check_control_access`.

---

## 14. The Redis layer

Three `RedisCatalog()` instances are constructed per `write()` — one by `DataWriter`, one by
`SuperTable` ([super_table.py:67](supertable/super_table.py:67)), one by `SimpleTable`
([simple_table.py:121](supertable/simple_table.py:121)). They share one cached
`redis.Redis` (see §14.5) but each builds its own `RedisLocking`.

### 14.1 Key formats

Every user-supplied segment passes `_safe()`
([redis_keys.py:204](supertable/redis_keys.py:204)), enforcing
`^(__[a-z0-9][a-z0-9_-]{0,59}__|[a-z0-9][a-z0-9_-]{0,63})$` and rejecting the sentinel
pattern `^_[a-z0-9][a-z0-9_-]*_$`. **Lowercase only, ≤ 64 chars** — an uppercase
organisation or table name raises `ValueError` from deep inside the key builder.

| Purpose | Literal key |
|---|---|
| Root pointer | `supertable:{org}:lakes:{sup}:meta:root` |
| Leaf pointer (**the commit**) | `supertable:{org}:lakes:{sup}:meta:leaf:doc:{simple}` |
| Rowid allocator | `supertable:{org}:lakes:{sup}:meta:rowid_seq:doc:{simple}` |
| Per-table config | `supertable:{org}:lakes:{sup}:meta:table_config:doc:{simple}` |
| Table-name set | `supertable:{org}:lakes:{sup}:meta:table_names` (SET) |
| Schema doc | `supertable:{org}:lakes:{sup}:schema:doc:{simple}` (note: **not** under `meta:`) |
| Write lock | `supertable:{org}:lakes:{sup}:lock:leaf:doc:{simple}` |
| Monitoring partition | `supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}` (LIST) |
| Audit stream | `supertable:{org}:system:audit:stream` |

### 14.2 The lock — `acquire_simple_lock`

Delegated to `RedisLocking.acquire` ([locking/redis_lock.py:100](supertable/locking/redis_lock.py:100)):

```python
token = uuid.uuid4().hex
deadline = time.time() + max(1, int(timeout_s))          # +60 s
while time.time() < deadline:
    if self.r.set(key, token, nx=True, ex=max(1, int(ttl_s))):   # SET k v NX EX 30
        ... start heartbeat ...; return token
    time.sleep(retry_interval)                            # 0.05 s, FIXED
return None
```

Properties that matter operationally:

* **No backoff** — a constant 50 ms spin, up to ~1200 attempts over the 60 s window.
* **Not fair, not FIFO** — pure `SET NX` racing. A waiter that has spun for 59 s has no
  priority over one that just arrived; starvation is possible under contention.
* `redis.RedisError` during acquire is **swallowed** and the loop continues — a Redis outage
  surfaces as `TimeoutError` after the full 60 s, not as a connection error.
* **The 30 s TTL is a crash-recovery window, not an operation deadline.** On first successful
  acquire a **daemon heartbeat thread** starts and every `max(1.0, min_ttl_ms/2000)` = **15 s**
  runs the extend script. A ten-minute write keeps its lock alive indefinitely.
* If `extend` returns 0 (lock expired or stolen), the key is dropped from `_held` and
  debug-logged — **the writer is never notified and keeps writing without a lock**
  ([redis_lock.py:226](supertable/locking/redis_lock.py:226)).

Release and extend are Lua, both token-checked:

```lua
-- release
local cur = redis.call('GET', KEYS[1])
if cur and cur == ARGV[1] then redis.call('DEL', KEYS[1]); return 1 end
return 0
-- extend
local cur = redis.call('GET', KEYS[1])
if cur and cur == ARGV[1] then redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2])); return 1 end
return 0
```

`FileLocking` ([locking/file_lock.py](supertable/locking/file_lock.py)) is **not used by the
write path or any production code** — only `locking/__init__.py` re-exports it and two
benchmark scripts use it. It is a single-host `fcntl.flock` shim left over from the
file-based era.

### 14.3 `reserve_rowids`

```python
new_val = int(self.r.incrby(RK.meta_rowid_seq(org, sup, simple), count))
return new_val - count + 1          # the START of the block
```

`INCRBY` on a missing key starts from 0, so the first id is **1**. No overflow handling — an
int64 overflow raises `ResponseError` uncaught (it is the only catalog method with no
`try/except redis.RedisError`). No TTL and **not deleted by `delete_simple_table`**, which
removes only the leaf and the lock: a recreated table continues from the old high-water mark.
Because reservation happens **outside the lock**, an aborted write **burns its range
permanently** — rowid gaps are expected and carry no meaning.

### 14.4 The leaf "CAS" is not a CAS

`set_leaf_payload_cas` / `set_leaf_path_cas` take **no expected version**, perform **no
comparison**, and have **no mismatch branch**. Each is a read-modify-write made atomic only by
Redis's single-threaded Lua:

```lua
local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then old_version = tonumber(obj['version']) end
end
local new_version = old_version + 1
redis.call('SET', key, cjson.encode({version=new_version, ts=now_ms, path=new_path, payload=payload}))
return new_version
```

A concurrent writer that got there first is **silently overwritten**; only the counter
survives. **The property that actually prevents lost updates is the per-simple lock**, not
these scripts. Both call sites discard the returned version.

Consequences worth knowing:

* **No size limit is enforced anywhere** — `payload = new_snapshot_dict` verbatim. The bound
  is Redis's own (512 MB string / `proto-max-bulk-len` / Lua cjson buffer). This is why the
  `RedisError` → `set_leaf_path_cas` fallback is the *hot* degradation path for wide tables
  with many resources, not a theoretical one.
* A non-serialisable payload degrades to `"{}"` silently, and the write still "succeeds".
* **lua-cjson turns an empty JSON array into an empty object.** A snapshot whose `resources`
  is `[]` round-trips as `{}`, so `get_simple_table_snapshot`'s
  `isinstance(payload.get("resources"), list)` gate fails and it silently falls back to a
  storage read ([simple_table.py:248](supertable/simple_table.py:248)). Once `resources` is
  non-empty the fast path engages. Same for an empty `schema`.
* lua-cjson serialises numbers with `%.14g` — 13-digit epoch-ms is safe, but any integer above
  14 significant digits loses precision on the round trip.
* A corrupt existing value makes `pcall` fail → `old_version` resets to `-1` → the next write
  emits `version=0`. **The version counter is not monotonic across a corruption event.**

`bump_root` uses the same shape and therefore **destroys the rest of the root document**:
`cjson.encode({version, ts})` replaces the whole value, wiping every flag written by
`update_root_flags` (`read_only`, `cloned_from`, `clone_type`, `clone_ts`, `replica_tables`).
Since `_resolve_replica_info` reads exactly those fields, **replica resolution silently stops
working after any write to the source supertable**. Also note `ts` is caller-supplied
([data_writer.py:970](supertable/data_writer.py:970)) and therefore **not monotonic** under
clock skew, even though `version` is.

`bump_root` re-raises on `RedisError` and its call site is **not** wrapped
([data_writer.py:994](supertable/data_writer.py:994)) — a Redis failure there aborts the write
*after* the leaf has already been committed. The write is durable and visible; the caller sees
an exception.

### 14.5 Client construction

`RedisOptions` ([redis_connector.py:18](supertable/redis_connector.py:18)) is a frozen
dataclass populated from `settings` (not `os.getenv` directly). Reads
`SUPERTABLE_REDIS_HOST/PORT/DB/PASSWORD/SSL/SENTINEL/SENTINELS/SENTINEL_MASTER` and
`effective_redis_sentinel_password`. `decode_responses=True` and `sentinel_strict=True` are
**hardcoded**.

`create_redis_client` memoises into a module-level `_CLIENT_CACHE` keyed by the full options
tuple, under a lock — so all three `RedisCatalog()` instances in one write share a single
client and connection pool.

Two divergences worth flagging:

* **`SUPERTABLE_REDIS_URL` and `SUPERTABLE_REDIS_USERNAME` are ignored by the write path.**
  `redis_connector` never reads them; `redis_infra` (used by the audit subsystem) does. A
  deployment configured purely via `SUPERTABLE_REDIS_URL` has audit connect correctly while
  the writer silently connects to `localhost:6379`.
* The standard (non-sentinel) branch sets **no `socket_timeout`, no `socket_connect_timeout`,
  no `health_check_interval`, no `retry_on_timeout`, and no pool sizing** — redis-py defaults,
  i.e. an unbounded pool and blocking sockets with no timeout. There is no ping probe either,
  so connection errors first surface on the first real command.

`redis_infra.py`, despite the name, creates **no streams, consumer groups or indexes**. It is
a settings adapter plus a second, independently-configured client builder, executed at import
time — and importing it raises `RuntimeError` if `SUPERTABLE_ORGANIZATION` or
`SUPERTABLE_SUPERUSER_TOKEN` are unset. `DataWriter.write` never touches it.

---

## 15. The storage layer

### 15.1 Backend selection

`get_storage(kind=None, **kwargs)` ([storage_factory.py:32](supertable/storage/storage_factory.py:32)):

```python
storage_type = (kind or "").upper() or settings.STORAGE_TYPE or (default.STORAGE_TYPE or "LOCAL").upper()
```

Branches on the exact strings `"LOCAL"`, `"S3"`, `"MINIO"`, `"AZURE"`, `"GCS"|"GCP"`; anything
else raises `ValueError(f"Unknown storage type: {storage_type}")`. Backends are lazily
imported; a missing dependency raises
`RuntimeError("Missing dependency '…'. Install it with: pip install 'supertable[…]'")`.

Two notes: the third fallback is **dead** (`settings.STORAGE_TYPE` can never be empty because
`_env_str` substitutes the default), and the factory reads the frozen `settings` singleton —
mutating `os.environ` after import has no effect.

`processing.py` memoises the backend in a module global `_storage`
([processing.py:46-53](supertable/processing.py:46)) that is **never invalidated**.

### 15.2 What the write path actually calls

`_write_single_parquet_file` probes capabilities in order — and since **all five backends
define `write_bytes`**, the first branch always wins:

```python
storage.write_bytes(path, data)          # (path, bytes)  ← always taken
storage.write_parquet(arrow_tbl, path)   # (pa.Table, path) ← DEAD on the write path
```

This matters: **the Parquet encoding is done by `processing.py`, not by the backend.** The
zstd codec, `compression_level`, `statistics=True` and `row_group_size=122_880` are passed to
**polars' own writer** ([processing.py:697](supertable/processing.py:697)). Each backend's
`write_parquet` instead uses **pyarrow defaults (snappy)** and would silently change the
encoding if it were ever reached — another reason that dead branch matters. The only genuine
`storage.write_parquet(table, path)` call in the repo is
[staging_area.py:240](supertable/staging_area.py:240), on the staging drop-box path.

Also note the interface's argument orders differ: `write_parquet(table, path)` is table-first,
`write_bytes(path, data)` is path-first.

The "footer metadata read" is likewise implemented in `processing.py` by **downloading the
whole object** and parsing the footer in memory
([processing.py:1678](supertable/processing.py:1678)) — **no backend exposes a range or
footer read**.

### 15.3 There are no PUT guards

Across every backend:

* **No 0-byte check** before uploading. The only empty checks are on *read*.
* **No read-after-write, ETag/MD5 verification, `If-None-Match`, or GCS generation
  precondition.** Azure explicitly passes `overwrite=True`.
* **No temp-key + rename commit** on any object store. The only atomic write in the layer is
  `LocalStorage.write_json` (mkstemp → fsync → `os.replace` → directory fsync,
  [local_storage.py:70](supertable/storage/local_storage.py:70)) — which is what protects the
  snapshot JSON, the one file whose torn write would matter.
* **Retries**: only `S3Storage._call` ([s3_storage.py:303](supertable/storage/s3_storage.py:303)),
  and it retries **exactly once**, **only** for region/endpoint-redirect codes
  (`PermanentRedirect`, `301`, `Redirect`, `AuthorizationHeaderMalformed`,
  `IllegalLocationConstraintException`). **Not** for 5xx, throttling, or partial writes.
  MinIO retries only inside `_ensure_bucket_exists`. Azure/GCS/Local have no retry code.

The write path's own guard is a fallback, not a retry: `_write_single_parquet_file` catches
*any* exception from the storage write and re-writes with polars **to the same path string on
the local filesystem** ([processing.py:726-734](supertable/processing.py:726)). On an object
store this produces a **local file while the snapshot records the key as written** — a silent
data-loss shape worth knowing about.

### 15.4 Paths

Logical paths are built with `os.path.join(organization, super_name, "tables", simple_name, …)`
— relative, unschemed, no bucket. `LocalStorage` uses them verbatim (which works only because
importing `supertable` `chdir`s into the app home). Object backends prepend
`SUPERTABLE_PREFIX` via `_with_base` and return **bucket-relative keys including the prefix,
with no scheme**.

`to_duckdb_path(key)` converts a key to something DuckDB can read, switching on
`SUPERTABLE_DUCKDB_USE_HTTPFS` (default `False`): `s3://bucket/key`, `azure://…`, `gcs://…`, or
an `http(s)://` URL. `presign(key)` mints a signed GET URL — and **re-applies `base_prefix`**,
so callers must hand it a base-relative key. `LocalStorage` implements neither and raises
`NotImplementedError`.

`s3a://` conversion for Spark lives outside the storage package, in
[spark_thrift.py:64](supertable/engine/spark_thrift.py:64).

### 15.5 Cross-backend inconsistencies

| Issue | Where |
|---|---|
| ~~`GCSStorage.write_bytes` omits `content_type` → parquet objects land as `text/plain`~~ — **fixed**, now sets `application/octet-stream` to match `write_parquet` | [gcp_storage.py:335](supertable/storage/gcp_storage.py:335) |
| `GCSStorage.read_parquet` doesn't wrap parse errors in `RuntimeError`, and says "File not found" instead of "Parquet file not found" | [gcp_storage.py:314](supertable/storage/gcp_storage.py:314) |
| `LocalStorage.exists` returns `True` for **directories**; object stores are object-only | [local_storage.py:110](supertable/storage/local_storage.py:110) |
| Azure/GCS `delete` materialise the full key list and delete serially; S3/MinIO stream and batch (1000/call) | — |
| `MinioStorage.from_env` **creates the bucket** as a construction side effect | [minio_storage.py:137](supertable/storage/minio_storage.py:137) |
| `MinioStorage.to_duckdb_path` ignores `url_style` and always emits path-style | [minio_storage.py:142](supertable/storage/minio_storage.py:142) |
| `S3Storage.exists` is the only object-store method that skips `_ensure_bucket_region()` | [s3_storage.py:477](supertable/storage/s3_storage.py:477) |

---

## 16. Access control — `check_write_access`

```python
def check_write_access(super_name, organization, role_name, table_name) -> None:   # access_control.py:160
    _check_readonly_guard(super_name, organization, "write to this table")
    _check_operation_access(super_name, organization, role_name, table_name,
                            Permission.WRITE, "write to this table")
```

### 16.1 Read-only guard

Reads the root doc and raises if `root["read_only"]` is truthy, with a reason derived from
`clone_type` (`"live replica"` / `"read-only snapshot clone"` / `"read-only clone"` /
`"locked"`). **Every non-`PermissionError` exception is swallowed**
([access_control.py:136](supertable/rbac/access_control.py:136)) — a Redis outage does not
fail here; it fails one step later inside `RoleManager`.

### 16.2 The permission model

Three layers, **no deny rules anywhere** — absence of an entry is the only denial.

**Layer 1 — role type matrix** ([permissions.py:20](supertable/rbac/permissions.py:20)):

| RoleType | Permissions |
|---|---|
| `superadmin`, `admin` | CONTROL, CREATE, WRITE, READ, META |
| `writer` | META, READ, WRITE |
| `reader` | META, READ |
| `meta` | META |

**Layer 2 — per-table map.** `_normalize_tables` accepts a dict, coerces a legacy list to
`{t: {"columns": ["*"], "filters": ["*"]}}`, and turns **anything else (including a
non-decoded JSON string) into `{}`** — which denies everything.

**Layer 3 — table resolution**: `role_tables.get(table_name) or role_tables.get("*")`.
`"*"` is a **fallback default, not a glob** — no prefix, suffix or regex matching. For writes,
the entry's `columns` / `filters` are never read; only its existence matters (column and row
filtering is read-side only).

### 16.3 It bootstraps — confirmed

`RoleManager.__init__` unconditionally calls `_init_role_storage()`
([role_manager.py:61](supertable/rbac/role_manager.py:61)), which on a cold supertable writes:
`rbac:roles:meta` (HASH), a `rbac:roles:doc:{uuid}` superadmin role with
`tables={"*": {...}}`, `rbac:roles:index` (SET), `rbac:roles:type:doc:superadmin` (SET) and
`rbac:roles:name_to_id` (HASH) — under a short-lived `lock:leaf:doc:roles_init` lock.

But the bootstrap has **already happened earlier**: `DataWriter.__init__` builds
`SuperTable(create_if_missing=True)`, which itself constructs `RoleManager` and `UserManager`.
Therefore **`compact()`'s `SuperTableNotFoundError` guard can never fire** — by the time
`compact()` runs, the `DataWriter` constructor has minted the root. Only its `leaf_exists`
guard is live.

### 16.4 Failure modes

Every denial is a plain builtin **`PermissionError`** — `supertable/errors.py` is not used
here.

| Condition | Literal message |
|---|---|
| read-only supertable | `This SuperTable is <reason>. Cannot write to this table.` |
| role not found / `{}` | `Invalid or nonexistent role: {role_name}` |
| no `role` field, unknown `RoleType`, or role lacks WRITE | `You don't have permission to write to this table.` |
| table not in the role's map and no `"*"` | `You don't have permission to write to this table table '{table_name}'.` |

`role_name=None` does **not** raise `PermissionError` — it raises
**`AttributeError: 'NoneType' object has no attribute 'lower'`** from
`role_name.lower()` ([redis_catalog.py:817](supertable/redis_catalog.py:817)), with no
`try/except` at any level. `""` and unknown names give the `PermissionError`. Lookup is
case-insensitive.

### 16.5 No caching — 5 Redis round-trips per call

There is no `lru_cache`, no memo, no module dict. Each `check_write_access` constructs **two**
fresh `RedisCatalog` objects and issues:

1. `GET …:meta:root`
2. `EXISTS …:rbac:roles:meta`
3. `SMEMBERS …:rbac:roles:type:doc:superadmin`
4. `HGET …:rbac:roles:name_to_id {role.lower()}`
5. `HGETALL …:rbac:roles:doc:{role_id}`

This is a fixed 5-RTT tax on every write, before the lock is even taken.

---

## 17. Mirroring — the expensive optional step

`MirrorFormats.mirror_if_enabled(super_table, table_name, simple_snapshot)` runs
**synchronously, inside the per-table lock**.

**There is no env var or settings key.** Configuration is a single Redis STRING per
**supertable** (not per table): `supertable:{org}:lakes:{sup}:meta:mirrors` holding
`{"formats": ["DELTA","ICEBERG","PARQUET"], "ts": <ms>}`. Missing key or any exception →
`[]` → mirroring off. Because the writer never passes `mirrors=`, **`get_enabled` issues one
Redis GET on every write even when mirroring is disabled**.

Output trees, rooted at `os.path.join(org, sup)`:
`…/delta/{table}/_delta_log`, `…/iceberg/{table}/{metadata,manifests,data}`,
`…/parquet/{table}/files`.

| Format | Reads data files? | Copies data files? | Metadata written |
|---|---|---|---|
| PARQUET | No | Yes, **skipped if `exists(dst)`** | None at all |
| DELTA | Only if the snapshot has neither `schemaString` nor `schema` (then it reads parquet schemas until one succeeds) | Yes, **unconditionally on every write** — no `exists` skip | One NDJSON commit `_delta_log/{version:020d}.json` |
| ICEBERG | No (reads prior *metadata* only) | Yes, unconditionally | Manifest + manifest list (hand-rolled Avro OCF encoder), `v{N}.metadata.json`, `version-hint.text`, `latest.json` |

Destination filenames are `f"{md5(src_uri)[:8]}_{basename}"`.

**Cost per mirrored write, per enabled format**: several `makedirs`, a full listing of the
mirror directory, and a byte or server-side copy of **every file in the snapshot**. Iceberg
adds 2 metadata reads + 4 writes and **never prunes `data/`**. Since the lock TTL is 30 s, a
mirror slower than the heartbeat interval is the realistic way for a writer to lose its lock.

The Iceberg entry point is **rebound at import time**
([mirror_iceberg.py:776](supertable/mirroring/mirror_iceberg.py:776)): the standard writer is
tried first, and on **any** exception it logs a warning and falls back to a "lite" writer that
emits plain JSON manifests and does **not** copy data files (paths point at the originals).

**Failure mode**: both call sites wrap the call in `try/except Exception` and only
`logger.error` — a mirror failure never fails the write, and there is **no retry, no partial-
state cleanup and no failure marker**. A half-written Delta `files/` directory with no commit,
or an Iceberg tree with a stale `version-hint.text`, simply survives.

---

## 18. Data quality — the producer/consumer split

### 18.1 Producer: `notify_ingest(r, org, sup, table_name)`

Called after the lock is released ([data_writer.py:1124](supertable/data_writer.py:1124)).
Body ([scheduler.py:103](supertable/quality/scheduler.py:103)), in order:

1. **Loop guard** — return immediately if the table name starts *and* ends with `__`.
2. **In-process negative memo** — `_recently_disabled(org, sup)`, a 60 s TTL dict, so a lake
   with DQ off costs one Redis GET per minute instead of one per write. When the memo exceeds
   1024 entries it **`.clear()`s the whole map** rather than evicting LRU.
3. `DQConfig(r, org, sup).get_schedule()` — one GET. The gate is
   **`if schedule.get("enabled") is not True`**: a truthy `"true"` string, `1`, or an absent
   key are all **off**. The default schedule returns `False`, so an unconfigured lake never
   writes anything.
4. `if not schedule.get("post_ingest", True)` → return.
5. `r.set("supertable:{org}:lakes:{sup}:quality:pending:{table}", <iso now>, ex=600)`.

Debounce is **TTL-and-overwrite**, not timestamp comparison: N writes in a window collapse to
one key and each write resets the 600 s TTL. The whole body is inside `try/except Exception`,
so **it cannot raise**.

### 18.2 Consumer: `start_scheduler()`

A daemon thread ([scheduler.py:163](supertable/quality/scheduler.py:163)); `sleep(10)` once,
then a `_scheduler_tick` every **60 s**. **Nothing in this repo calls it** outside a
characterization test — the host application must start it. The `last_*_run` bookkeeping is
in-memory, so a restart makes every interval "due now".

A tick SCANs for lakes (`…:meta:root`), re-applies the same strict `is not True` gate, SCANs
each lake's tables (skipping `__…__`), and runs quick / deep / custom branches plus the
pending branch. `_cron_to_seconds` is **not a cron parser** — it maps `*/N` hours to `N*3600`,
`*/N` minutes to `N*60`, a fixed hour to `86400`, and **silently falls back to 4 h** for
anything else. Scheduling is interval-since-last-run, not wall-clock cron.

`_try_run_check` guards with two keys: `quality:cooldown:{table}` (default 300 s, set after
**any** return including early ones) and `quality:running:{table}` (`SET NX EX 300`). The lock
release is an **unconditional `DEL` with no token compare**
([scheduler.py:367](supertable/quality/scheduler.py:367)) — a check that outruns its 300 s TTL
will delete a different worker's lock.

### 18.3 The check re-enters the write path

`_run_quick_check` reads the schema via `MetaReader`, builds one aggregate SQL, and executes it
through a real `DataReader` — a **full table scan**. `_run_deep_check` issues **one SQL per
column**, each a 4-CTE cross join with `PERCENTILE_CONT` / `NTILE(10)` / `RANK() OVER` /
entropy — several full scans and sorts per column.

Results are then written back through `DataWriter.write`:

```python
DataWriter(super_name=sup, organization=org).write(
    role_name="superadmin", simple_name="__data_quality__",
    data=arrow_table, overwrite_columns=[],
    lineage={"source_type": "dq_check", ...})
```

So a quality check re-enters the entire write path — RBAC, lock, parquet write, mirroring,
monitoring. **The recursion terminator is `notify_ingest`'s `__…__` guard**, backed by
`MONITORING_SINK_TABLES` for the monitoring leg. On failure, `write_history_via_sql` falls back
to `LPUSH`+`LTRIM` on a Redis list capped at 1000 — which means **DQ history can silently
diverge between the table and the Redis fallback**.

Of the 17 `BUILTIN_CHECKS`, seven are actually evaluated (T1, T3, C1, C2, C3, C5, C6); T2, T5
and C4 have **no reader anywhere**, and D1–D7 only gate whether the deep SQL runs — their
thresholds are never compared. None of the result keys (`latest:*`, `anomalies:*`, `config:*`,
`schedule*`) carry a TTL.

---

## 19. Monitoring and audit mechanics

### 19.1 Monitoring

`MonitoringWriter(organization=…, monitor_type="writes")` is a **thin per-call façade** over a
**process-global singleton** keyed by `f"{organization}/{monitor_type}"` — deliberately
**without the date**, so the same logger serves every day. Entering/exiting the `with` block
starts and stops nothing.

`log_metric(payload)` is **enqueue-only**: `queue.put_nowait` on a `Queue(maxsize=10_000)`.
On `queue.Full` the metric is **dropped** with a warning. A daemon worker thread batches up to
**200** payloads with a **50 ms** accumulation window and ships them:

```python
target_key, expire_at = key.redis_partition_today()
pipe = redis.pipeline()
for payload in batch: pipe.rpush(target_key, json.dumps(payload, separators=(",", ":")))
pipe.expireat(target_key, expire_at)
pipe.execute()
```

Destination is a **Redis LIST**, not a stream: `supertable:{org}:monitor:writes:doc:{YYYY-MM-DD}`,
with an absolute `EXPIREAT` of the partition's midnight + **7 days**. Because "today" is
resolved fresh on every batch, past partitions are frozen and safe to drain.

**Delivery is not guaranteed by the `with` block.** `__exit__` calls `_try_flush()`, which
acquires the ship lock with a **100 ms timeout** and gives up if the worker holds it longer —
the payload is then left to the daemon. (A public `request_flush()` with a real blocking wait
exists, but no call site in `data_writer.py` invokes it.)

Master switch: `SUPERTABLE_MONITORING_ENABLED`, **default `True`**. `SUPERTABLE_MONITOR_CACHE_MAX`
(default 256) is frozen at import; when the cache is full the oldest logger is evicted and its
worker stopped, **discarding whatever was still queued**.

An **invalid `monitor_type` never raises** — it fails inside key construction, is swallowed,
and every metric is silently counted as dropped. Valid values are
`{"plans","writes","mcp","odata","errors","locks","compact"}`.

Nothing in the SDK drains the partitions; `monitoring/partitions.py` provides
`list_drainable_partitions` / `drain_partition` (a `RENAMENX` → `LRANGE` → `DEL` handshake, so a
crashed run's handle is recovered rather than overwritten) / `iter_partition_chunks` /
`read_recent` for an **external orchestrator**.

### 19.2 Audit

**`SUPERTABLE_AUDIT_ENABLED` defaults to `False`** — audit is off unless explicitly enabled, and
a per-org Redis hash `supertable:{org}:system:audit:config` (30 s cached) can override
`enabled` / `hash_chain` / `log_queries` / `log_reads` / `siem_enabled`.

`emit()` is synchronous-but-non-blocking (`put_nowait` on a 10 000-slot queue), except the
**first** call per org, which does a Redis `HGETALL` for config and constructs the logger.
Events go to **two tiers**: a Redis Stream `supertable:{org}:system:audit:stream`
(`XADD MAXLEN~`) and hourly Parquet under
`{org}/__audit__/year=/month=/day=/audit_<ts>_<instance>_<uuid>.parquet`.

For a write, the event is `category="data_mutation"`, `action="data_write"`,
`severity="info"`, `outcome="success"`, `actor_type="system"` — **no human actor is recorded**
(`actor_id`/`actor_username`/`actor_ip` are all `""`). `compact()` emits the *same*
category/action pair; the only discriminator is `detail.operation == "compact"`.

Because the audit block is guarded by `if result_tuple is not None` and the `except` clause
re-raises before reaching it, **a failed write emits no audit event at all**.

The **hash chain is unkeyed SHA-256, not a signature** — no HMAC, no private key:

```python
batch_hash = sha256("\n".join(sorted(event_ids)) + "\n" + file_hash)
chain_hash = sha256(previous_hash + batch_hash)
```

Two gaps worth recording: `logger.py:293` calls `advance(event_ids)` **without** the parquet
`file_hash` (which `ParquetAuditWriter` does compute and return), and
`AuditEvent.event_hash()` — which would cover event *content* — is never called outside tests.
So tampering with `detail`, `actor_id` or `outcome` leaves the chain valid as long as
`event_id` is preserved. Separately, the chain head is persisted under a key validated against
`^[a-z0-9][a-z0-9_-]{0,63}$` while `INSTANCE_ID` is `f"{hostname[:32]}-{pid}"` — **any host with
an uppercase letter or a dot in its hostname silently fails to persist**, restarting from
genesis on every process start.

`audit/crypto.py` (Fernet) has **zero callers** outside its own tests, and both
`encrypt_field`/`decrypt_field` **fail open to plaintext**. `detail` reaches Redis and Parquet
unencrypted.

### 19.3 Profiler semantics that affect metric interpretation

* `Profiler.add(name, value)` **returns early on a falsy value**
  ([profiler.py:62](supertable/utils/profiler.py:62)) — so a counter whose value is `0` is an
  **absent key**, not `0`. Any consumer must treat missing as zero.
* `span()` **accumulates** into `timings[name]` and also bumps `counts[name + ".n"]`, in a
  `finally` — a span that raises still records. `DataWriter`'s local `mark()` closure
  **assigns** (`timings[stage] = now - t_last`), uses `time.time()` rather than
  `perf_counter()`, and creates **no `.n` counter**. The two share one dict, so a stage name
  colliding with a span name would clobber it. `newer_than` is marked twice within `write()`
  (lines 477 and 510) — the second overwrites the first.
* `span()` does **no** automatic prefixing; the dotted hierarchy is a naming convention in the
  literal strings. Nested spans therefore double-count wall time — only the top-level `mark()`
  stages sum to wall. A hardcoded span name in a helper serving two callers mis-attributes
  cost outright; that happened with `tombstone.encode` (see
  [§24.5](#245-a-telemetry-trap-that-was-fixed)) and the span is now caller-labelled.
* **Not thread-safe** (plain dict read-modify-write, no lock). The write path works around this
  by giving each `ThreadPoolExecutor` branch a private `Profiler()` and merging in the parent.

---

## 20. Configuration

### 20.1 Two parallel config objects

* **`settings`** — `@dataclass(frozen=True)`, **118 fields**, built **once at import**
  ([settings.py:609](supertable/config/settings.py:609)). No reload API, no mutation. Modules
  that capture derived constants at import time freeze them permanently.
* **`default`** — a mutable `Default` dataclass with **8 fields**, copied *from* `settings`
  ([defaults.py:70](supertable/config/defaults.py:70)).

**The write path's limits go through `default`, not `settings`** — so monkey-patching
`default` at runtime changes behaviour (the tests rely on this), while `settings` cannot be
changed in place.

`.env` handling: `find_dotenv(usecwd=True)` + `load_dotenv(override=False)` at
[settings.py:42](supertable/config/settings.py:42) — real environment variables **win over**
`.env`, and the search starts at the process's **original** cwd, walking upward. This happens
*before* `homedir` `chdir`s, so the `.env` found is relative to where the process started, not
the app home. The `DOTENV_PATH` field is **never used** to locate the file.

Coercion is silent: `_env_int` / `_env_float` / `_env_bool` all fall back to the default on an
unparseable value, and `_env_str` treats an empty value as unset.

### 20.2 Write-path settings

| Env var | Coercion | Effective default | Used for |
|---|---|---|---|
| `MAX_MEMORY_CHUNK_SIZE` | int | **16 777 216** (16 MiB) | small-file threshold, compaction chunk cap |
| `MAX_OVERLAPPING_FILES` | int | **100** | small-file count gate |
| `MAX_TOMBSTONE_ROWS` | int | **1 000 000** | deletion-vector drain threshold |
| `DEFAULT_LOCK_DURATION_SEC` | int | 30 | `Default` only — **the writer hardcodes `ttl_s=30`** |
| `DEFAULT_TIMEOUT_SEC` | int | 60 | `Default` only — writer hardcodes `timeout_s=60` |
| `SUPERTABLE_DUCKDB_WRITE_PROBE` | bool | **`False`** | selects the DuckDB pushdown probe vs the polars fallback |
| `SUPERTABLE_DUCKDB_PRESIGNED` | bool | `False` | proactive presigning for the probe |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | bool | `False` | `to_duckdb_path` scheme |
| `SUPERTABLE_STATS_CACHE_MAX_TABLES` | int | 64 | `_STATS_CACHE` cap (`<= 0` disables) |
| `SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES` | int | 64 | `_TOMBSTONE_CACHE` cap |
| `STORAGE_TYPE` | str, upper | `"LOCAL"` | backend selection |
| `SUPERTABLE_PREFIX` | str | `""` | object-store key prefix (`base_prefix`) |
| `SUPERTABLE_HOME` | str | `"~/supertable"` | app home; **process `chdir`s here at import** |
| `SUPERTABLE_MONITORING_ENABLED` | bool | **`True`** | monitoring master switch |
| `SUPERTABLE_AUDIT_ENABLED` | bool | **`False`** | audit master switch |
| `IS_SHOW_TIMING` | bool | **`True`** | `Timer` prints to stdout (unused by the write path) |

**Not configurable at all** — hardcoded in `processing.py`:

* `_PARQUET_ROW_GROUP_SIZE = 122_880` ([processing.py:30](supertable/processing.py:30))
* `compression="zstd"` — the codec is fixed; only the *level* is a runtime argument
  (`DataWriter.write(compression_level=1)`, and the internal helpers default to `10`)
* No dedicated compaction-threshold var — it is derived from the three limits above
* No data-quality master switch in `settings` (DQ is gated purely by the Redis schedule doc)

Limits resolve **per-table Redis config → `default.<X>` → a hardcoded literal**, all joined
with `or` — so a stored value of `0` is indistinguishable from unset
([processing.py:41](supertable/processing.py:41), [processing.py:880](supertable/processing.py:880)).

### 20.3 Declared-vs-built default mismatches

The dataclass declaration and `_build_settings()` disagree in three places; the **builder
wins**:

| Field | Declared | Actual |
|---|---|---|
| `IS_SHOW_TIMING` | `False` | **`True`** |
| `STORAGE_REGION` | `"eu-central-1"` | **`"us-east-1"`** |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | `"5GB"` | **`""`** (builder passes no default) |

---

## 21. Who actually calls the write path

Grepped across the repo excluding `.venv` and `tests/`.

**Every one of the ~20 non-test call sites passes a `pyarrow.Table`** — never a
`RecordBatch`, never a polars or pandas frame. The three converters in use are
`pq.read_table`, `pa.Table.from_pandas`, and `polars.DataFrame.to_arrow`.

| Caller | Site | Notable arguments |
|---|---|---|
| `quality/history.py:169` | DQ history | `simple_name="__data_quality__"`, `overwrite_columns=[]`, `lineage={"source_type": "dq_check", …}` |
| `demo/medcenter/load.py:64` | 18 raw tables | `overwrite_columns=overwrite_columns_by_table[t]`, all other args default |
| `demo/medcenter/helpers.py:32` | 8 staging/mart tables | fresh `DataWriter` per call; `pa.Table.from_pandas(df, preserve_index=False)` |
| `demo/webshop/*`, `demo/quickstart/*` | demos | one uses `delete_only=True` |
| `benchmarks/write_suite.py`, `benchmarks/dataset.py` | perf suites | mostly `overwrite_columns=[]`; one `delete_only=True`; the only `configure_table` caller |

Facts that shape how the API is really exercised:

* **`compact()` has zero non-test callers.** No demo, benchmark or library module invokes it.
* **`configure_table()` has exactly one** — `benchmarks/dataset.py:251`, raising
  `max_memory_chunk_size` so auto-compaction leaves the generated layout alone.
* **`newer_than` has zero non-test callers**, and `compression_level` is never overridden
  from `1` anywhere.
* **`delete_only` with empty `overwrite_columns`** (the delete-all path) is exercised only by
  tests.
* **`lineage` is populated by exactly three call sites**, and the only `source_type` values
  any of them emit are `"dq_check"` and `"manual"`. Every other write relies on the
  auto-generated lineage (`source_type` = `"write"` / `"delete"`).

### 21.1 Staging and pipes do not feed the writer

* **`staging_area.py` never imports `DataWriter`.** It is a parquet drop-box: RBAC check →
  stage lock → `storage.write_parquet(arrow_table, path)` → append a record to a flat JSON
  index (read-modify-write of the whole index per file). No `overwrite_columns`, no
  `__rowid__`, no snapshot commit. Nothing in the repo consumes staged files and writes them.
* **`super_pipe.py` produces no data.** It is CRUD over Redis pipe-definition documents; its
  `"transformation"` field is hardcoded to `[]` and never read anywhere. **There is no pipe
  runner in this repo** — `SuperPipe` only *declares* the `(simple_name, overwrite_columns)`
  pair some external executor would pass to `write()`.
* **`system_query.py` is a pure string classifier** with no supertable imports and no write
  kind. Its `SHOW STATS` command names the stats artifact but neither reads nor writes it.

### 21.2 The medcenter demo as the end-to-end reference

`demo/medcenter/run.py` is the realistic full flow: generate → `load()` (18 raw tables, N files
each) → `transform()` (1 staging + 7 marts, each read back through `DataReader` as pandas and
re-written) → quality → showcase → export → **then `load()` and `transform()` again** and
assert the results are bit-identical.

That idempotency proof is only possible because **every table in the demo has a non-empty
`overwrite_columns`** (single-column natural keys like `["subscription_id"]`, or composites
like `["company_id","fiscal_year","sequence_no"]`) — so the second pass reports
`inserted == deleted` rather than doubling the data.

---

## 22. Defects found while tracing — and their fixes

Ten defects were found by this audit and **all ten are now fixed**. Items already covered in
[§13](#13-names-that-do-not-match-behaviour) are not repeated. Each subsection below states
the original behaviour first (so the failure mode stays on record) and the fix after it.

Regression seals live in `supertable/tests/test_defect_fixes.py` (22 tests),
`test_data_writer.py::TestLockFenceBeforeCommit` (3),
`test_redis_lock.py::TestIsHeld` (6) and the schema-fallback tests in
`test_data_writer_compact.py`. Suite: **2670 passing**.

### 22.0 Fix summary

| # | Defect | Fix |
|---|---|---|
| 1 | `RK.staging` / `RK.pipe` don't exist → `AttributeError` | 6 call sites repointed to `staging_doc` / `pipe_doc` |
| 2 | `bump_root` replaces the root doc, wiping clone/replica/read-only flags | Lua merges into the decoded doc |
| 3 | Lost lock detected but swallowed; the writer commits anyway | sticky loss tracking + `is_held` + a pre-commit fence raising `LockLostError` |
| 4 | Compaction schema fallback unreachable; falling through it wiped the schema | handle the dict shape; return `None` (preserve) instead of an empty frame |
| 5 | Encode-failure fallback wrote to local disk under an object-store key | fallback re-encodes with polars but PUTs through the active backend |
| 6 | `role_name=None` → `AttributeError`, not `PermissionError` | type/empty guard in `rbac_get_role_id_by_name` |
| 7 | DQ running-lock released with a token-less `DEL` | per-attempt token + compare-and-delete Lua |
| 8 | Audit chain head never persisted on FQDN/uppercase hostnames | `_key_safe_instance_id` normalises the key segment |
| 9 | Audit hash chain didn't cover event content | `compute_content_hash` folded into the batch hash |
| 10 | GCS stored every parquet as `text/plain` | `write_bytes` sets `application/octet-stream` |

Verified end-to-end against a real Redis + `LocalStorage`: append / upsert / delete-only return
the expected tuples, the snapshot schema stays intact, root flags survive a write
(`version` advanced with `cloned_from` / `clone_ts` / `replica_tables` / `read_only` all
preserved), `role_name=None` raises `PermissionError`, and a simulated lock loss raises
`LockLostError` leaving `snapshot_version` unchanged.

### 22.1 Correctness-affecting (all fixed)

1. **`bump_root` replaces the root document instead of merging it.** The Lua ends
   `redis.call('SET', key, cjson.encode({version=new_version, ts=now_ms}))`
   ([redis_catalog.py:168](supertable/redis_catalog.py:168)) — the whole value, not a field
   update. `update_root_flags` stores `read_only` / `cloned_from` / `clone_type` / `clone_ts` /
   `replica_tables` in that same key by read-merge-write
   ([redis_catalog.py:386](supertable/redis_catalog.py:386)), and `_resolve_replica_info`
   ([redis_catalog.py:443](supertable/redis_catalog.py:443)) plus `_check_readonly_guard`
   ([access_control.py:122](supertable/rbac/access_control.py:122)) read them back. Every
   successful `write()`/`compact()` calls `bump_root`
   ([data_writer.py:995](supertable/data_writer.py:995)), so those flags are destroyed.

   Reachability differs per flag. `read_only` largely self-protects — the guard runs first and
   blocks the write — **except** for the race where the flag is set after a concurrent write
   has passed the guard at [data_writer.py:303](supertable/data_writer.py:303) but before its
   `bump_root`; that window spans the whole locked section. `clone_type`/`cloned_from`/
   `replica_tables` set **without** `read_only` have no such protection: the first write to
   that supertable erases them, and `leaf_exists` silently stops redirecting to the source.

   **Fixed.** The Lua now decodes the existing document, sets `version` / `ts` on it, and
   re-encodes the whole thing — a merge, not a replacement. A non-table or unparseable value
   still resets cleanly to a fresh document.

2. **The local-fallback write can commit a snapshot referencing a non-existent object.**
   If a storage `write_bytes` raises, `_write_single_parquet_file` re-writes with polars **to
   the same path string on the local filesystem**
   ([processing.py:726](supertable/processing.py:726)). Storage paths are relative
   (`{org}/{sup}/tables/{t}/data/…`) and object-store `makedirs` is a `pass`, so the local
   directory usually does not exist and polars raises `FileNotFoundError` — verified against
   the installed polars 1.40: `write_parquet` does not create parents (`mkdir` defaults to
   `False`) — which aborts the write safely. **But where that relative tree does exist under
   the app home**, the local write succeeds and the size chain masks it: `storage.size()`
   fails, then `os.path.getsize()` **succeeds on the local file** and returns a plausible size
   ([processing.py:737-747](supertable/processing.py:737)). The resource is appended and the
   snapshot commits, pointing at a bucket key that was never uploaded.

   **Fixed** in both writers (`_write_single_parquet_file` and `_write_df_parquet`, which
   covers the tombstone and stats artifacts). The fallback still re-encodes with polars — it
   accepts dtype shapes pyarrow's writer rejects — but encodes into a buffer and PUTs it
   through the active backend, so the object cannot land outside the configured store. Only a
   backend with no `write_bytes` at all still writes a real local path.

3. **A lost lock is not detected.** If the heartbeat's `extend` returns 0 (TTL expired or the
   key was stolen), the key is dropped from `_held` and debug-logged; the writer is **not
   notified** and continues writing outside the lock
   ([redis_lock.py:226](supertable/locking/redis_lock.py:226)).

   **Fixed** in three parts: the heartbeat now logs at WARNING and records the loss in a
   bounded sticky `_lost` set; `RedisLocking.is_held(key, token)` reports ownership from that
   set **and** a fresh Redis read (a Redis error reports *not* held — "unknown" must never read
   as "safe to publish"); and `DataWriter._assert_lock_still_held` runs immediately before the
   leaf commit in both `write()` and `compact()`, raising `LockLostError` rather than
   publishing. Everything written up to that point is immutable and unreferenced, so aborting
   leaks only harmless garbage.

   **Residual risk, stated plainly:** a one-round-trip TOCTOU window remains between the check
   and the commit. Closing it fully needs the ownership test and the leaf `SET` in a single
   Lua script; that was not done here because the existing `except Exception` fallback around
   `set_leaf_payload_cas` would silently bypass a fence bolted onto it. What this does fix is
   the common case — a lock lost seconds or minutes earlier, during the parquet writes.

4. **`role_name=None` raises `AttributeError`, not `PermissionError`** — `role_name.lower()`
   with no guard at any level ([redis_catalog.py:817](supertable/redis_catalog.py:817)).

   **Fixed.** A non-`str` or empty role name resolves to "no such role", so the existing
   `_resolve_role` path raises `PermissionError: Invalid or nonexistent role: None`.

5. **DQ's running-lock release is token-less** — an unconditional `DEL`
   ([scheduler.py:367](supertable/quality/scheduler.py:367)). A check exceeding its 300 s TTL
   deletes a different worker's lock.

   **Fixed.** The lock value is now a per-attempt `uuid4().hex` token and the release is a
   compare-and-delete Lua script. If the release itself fails the key is left to its TTL —
   falling back to an unconditional `DEL` would reintroduce exactly the cross-worker deletion
   being guarded against.

6. **Audit chain head never persists on many hosts.** The key segment is validated against
   `^[a-z0-9][a-z0-9_-]{0,63}$` while `INSTANCE_ID` is `f"{hostname[:32]}-{pid}"`; any
   uppercase letter or dot (an FQDN) makes `_safe` raise, the exception is swallowed, and every
   restart begins from genesis.

   **Fixed.** `RedisAuditWriter._key_safe_instance_id` lowercases and remaps invalid characters
   before the key is built. It is an identity transform for ids that were already valid, and
   distinct hosts stay distinct.

7. **The audit hash chain does not cover event content.** `advance(event_ids)` is called
   without the parquet `file_hash` the writer computes, and `AuditEvent.event_hash()` is never
   called in production. Altering `detail`, `actor_id` or `outcome` leaves the chain valid.

   **Fixed.** `_write_batch` now folds `compute_content_hash([e.event_hash() for e in events])`
   into the batch hash, so the chain covers event fields, not just their ids. Hashing failures
   degrade to ids-only with a warning rather than killing the audit thread.

   **Compatibility note:** this changes how new chain links are computed. Previous heads remain
   valid (the chain extends from the stored head), but a verifier must record the same content
   digest in each batch's `file_hash` slot. In practice the exposure is nil — audit defaults to
   off, `verify_batch_chain` has no production caller, and defect 6 above meant most chain heads
   never persisted anyway.

### 22.2 Silent-failure surfaces

| Behaviour | Where |
|---|---|
| `_check_readonly_guard` swallows every non-`PermissionError` — a Redis outage does not fail the check | [access_control.py:136](supertable/rbac/access_control.py:136) |
| `root_exists` returns `False` on `RedisError` — an outage is indistinguishable from "missing supertable", so it re-bootstraps | [redis_catalog.py:353](supertable/redis_catalog.py:353) |
| `set_table_config` returns `False` on `RedisError` — the caller never checks | [redis_catalog.py:1633](supertable/redis_catalog.py:1633) |
| Schema + table-name Redis registration failures are debug-logged only | [data_writer.py:1014](supertable/data_writer.py:1014) |
| `get_mirrors` returns `[]` on any exception — silently disables mirroring | [redis_catalog.py:549](supertable/redis_catalog.py:549) |
| An invalid `monitor_type` drops every metric with no error | [monitoring_writer.py:457](supertable/monitoring_writer.py:457) |
| `_env_int`/`_env_bool` fall back to the default on unparseable input | [settings.py:56](supertable/config/settings.py:56) |
| A non-serialisable leaf payload degrades to `{}` and the write still "succeeds" | [redis_catalog.py:529](supertable/redis_catalog.py:529) |

### 22.3 Dead or unreachable code on the write path

* **`storage.write_parquet` is never reached** — all five backends define `write_bytes`, which
  the capability probe checks first.
* ~~**`_build_compact_model_df` fallback #2** requires a `list` schema the writer never
  produces~~ — **fixed**: it now handles the dict-of-polars-dtype-reprs shape that
  `collect_schema` actually writes (parameterised reprs like
  `Datetime(time_unit='us', time_zone='UTC')` resolve to the base dtype), keeps the list branch
  for the alternate shape, and returns `None` — update()'s "preserve the previous schema"
  signal — instead of a zero-column frame that would overwrite the real schema with an empty
  one.
* **`_spark_type_from_polars_dtype` / `_schema_list_from_polars_df`** are only reachable for a
  zero-column frame, where they also return empty.
* **`force_tombstones`** is never branched on.
* The `overwrite_columns` component of the pre-write sort — both call sites pass `[]`.
* **`DataWriter.timer = Timer()`** is a class attribute evaluated at import and **never
  referenced** anywhere in `data_writer.py`.
* **`FileLocking`** — no production caller.
* **`audit/crypto.py`** (Fernet) — no caller outside its own tests; both directions fail open
  to plaintext.
* The `ls` / `listdir` probes in the Parquet and Delta mirrors — **no storage backend
  implements either**, so the `list_files` branch always runs.
* `redis_infra`'s non-strict sentinel fallback block — unreachable under the hardcoded
  `sentinel_strict = True`.
* `RESERVED_SUPER_NAMES` is `frozenset()`, so the reservation check is the sentinel regex
  alone, and the `ValueError` message always prints `Reserved names: []`.

### 22.4 Was outright broken (off the write path) — fixed

**`RK.staging` and `RK.pipe` did not exist.** `redis_keys.py` defines `staging_doc` and
`pipe_doc`; `redis_catalog.py` called `RK.staging(...)` / `RK.pipe(...)` at six sites (1305,
1318, 1361, 1390, 1402, 1480). There was no alias and no module-level `__getattr__`, so every
one raised `AttributeError` at runtime — and `AttributeError` is not caught by the surrounding
`except redis.RedisError` handlers. `upsert_staging_meta`, `get_staging_meta`,
`delete_staging_meta`, `upsert_pipe_meta`, `get_pipe_meta` and `delete_pipe_meta` were all
non-functional, which meant **`SuperPipe` and the staging metadata layer could not work at
all**.

**Fixed** by repointing all six call sites to the real builders. Both metadata round-trips are
now sealed against fakeredis, plus a source-level guard asserting the dead names never come
back.

### 22.5 Resource growth

Every `RedisCatalog()` builds a `RedisLocking`, whose `__init__` registers an `atexit` handler
that is never unregistered. `write()` constructs **three** `RedisCatalog` instances (DataWriter,
SuperTable, SimpleTable), so a process doing *N* writes accumulates **3*N* `atexit` handlers and
3*N* `RedisLocking` objects**, each with a potential heartbeat thread. In a long-running writer
service this is an unbounded leak, and shutdown walks every registered handler.

Similarly, `_get_table_config` memoises per `DataWriter` instance **with no invalidation** — a
long-lived writer never observes an external `configure_table` change.

---

## 23. Quick reference — the write in one page

```
DataWriter(super, org)                    # bootstraps supertable + RBAC if absent
  └─ write(role, table, arrow_table, overwrite_columns, ...)

  OUTSIDE THE LOCK
   1  check_write_access            5 Redis RTTs; PermissionError on denial
   2  polars.from_arrow(data)       capture logical rows/cols here
   3  validation                    name regex, overwrite_columns, newer_than rules
   4  reserve_rowids                Redis INCRBY → contiguous __rowid__ block
   5  inject __timestamp__          one constant value for the whole batch

  ── acquire_simple_lock(ttl=30s, timeout=60s) ────────────────────────
   6  get_simple_table_snapshot     Redis leaf payload, else storage JSON
   7  find_overlapping_files        overwrite ⇒ every file is a candidate
   8  stats pruning                 df-probe vs stored footer stats; retain on doubt
   9  resolve_overwrite_writes      stale filter + (file, rowid) delete pairs
  10  load deletion vector          required=True — a broken DV aborts the write
  11  ║ write data parquet   ║ build tombstone parquet ║   (2 threads, disjoint dirs)
  12  reclaim_fully_dead_files      free drops, no rewrite
  13  Phase A: compact_tombstones   MUST precede Phase B
  14  Phase B: compact_resources    small-file merge, folded into the same commit
  15  build_stats_file              carry forward − sunset + new footers
  16  simple_table.update           writes snapshots/<ts>_<hex>_tables.json
  16b verify_simple_lock            fence: LockLostError if we no longer hold the lock
  17  set_leaf_payload_cas          ← THE COMMIT (not really a CAS; the lock is the guard)
  18  bump_root                     invalidates reader caches
  19  mirror_if_enabled             synchronous, inside the lock, failures swallowed
  ── release_simple_lock ──────────────────────────────────────────────

  20  MonitoringWriter              enqueue only; delivery not guaranteed by __exit__
  21  notify_ingest                 debounced Redis flag, cannot raise
  22  audit emit                    only when the write succeeded

  → (total_columns, total_rows, inserted, deleted)
```

**The five invariants to remember**

1. Data files are immutable; a write only ever **appends** and **tombstones**.
2. The **leaf pointer is the commit** — the snapshot JSON is durable before it, so a crash
   leaves an orphan file, never a dangling pointer. A writer that has lost its lock refuses to
   move the pointer (`LockLostError`) rather than clobbering the current holder.
3. **Phase A before Phase B**, always — `compact_resources` does not consult the deletion
   vector.
4. Every artifact (data, tombstone, stats, snapshot) is **versioned and never mutated**, which
   is what makes the in-process caches safe and time travel possible.
5. Stats pruning may only ever **remove files with zero possible matches** — every uncertainty
   retains. It is a performance optimisation and never changes the result.

---

## 24. Measured performance profile (100 appends → 10M rows)

Driven by `scripts/write_telemetry_audit.py`, which swaps `MonitoringWriter` for a recorder
and keeps the `Profiler` payload of every write; analysed with
`scripts/write_telemetry_report.py`. Workload: 100 appends × 100k rows, 7 columns, LOCAL
storage. Analysis is polars end to end.

**Reading the telemetry correctly.** The top-level `mark()` stages are mutually exclusive and
sum to wall time. The `write.*` / `io.*` / `<artifact>.encode` / `simple_update.*` / `redis.*`
entries are **nested spans inside those stages** and therefore double-count — summing
everything gives >100%. Judge cost from the top-level set only.

### 24.1 The four bottlenecks this workload exposed

**a) Compaction re-merged its own output — 53% of all write time.**
A merged file was written by accumulating *source* bytes up to `max_memory_chunk_size` and
re-encoding, so the output landed *below* the threshold that selected its inputs. With "small"
defined as `file_size < max_mem`, the fresh 15.8 MiB output immediately re-qualified, and the
next 1.6 MiB append dragged the whole file through another read-rewrite:

```
merged 10 small files -> 1 file (16.2 MiB read, 15.8 MiB written)   ← still < 16 MiB
merged  2 small files -> 1 file (17.4 MiB read, 17.3 MiB written)   ← rewrites what it just wrote
```

~35 MiB of I/O to absorb 1.6 MiB, on a perfectly regular 11-write cycle.
Fixed with hysteresis: `_COMPACTION_TARGET_RATIO` (0.75) sets a `_small_file_threshold`
below `max_mem`, applied to **both** `should_compact_small_files` (the gate) and
`compact_resources` (the candidate set) so the two can never disagree.

**b) `_build_compact_model_df` fully decoded files to read their schema — 6.8%.**
`storage.read_parquet()` on a 16 MiB / 1.1M-row merged chunk, then `.limit(0)`: ~260ms per
compacting write, every row discarded. Now a footer read (`read_bytes` + `pq.read_metadata` →
`to_arrow_schema().empty_table()`).

**c) The encoder was pyarrow.** Same frame, same zstd level, 9 runs:
`pq.write_table` 139.2ms / 648,779 bytes vs `DataFrame.write_parquet` **25.2ms / 175,664
bytes** — 5.5x faster, 3.7x smaller. Verified equivalent: round-trip identical to source and
to each other, same row-group count, statistics set on every column, per-column
`total_compressed_size` present (which `STATS_SCHEMA.compressed_bytes` records).

**d) `build_stats` was the only stage that grew.** Isolating non-compaction writes,
every stage was flat except `build_stats` (21.8 → 36.8ms, 1.69x) — the artifact is rewritten
whole on every write and grows monotonically. Still only ~5%, but it is the line that bends.

### 24.2 Before / after

| | before | after |
|---|---|---|
| total write wall | 68.5s | **15.3s** (−78%) |
| `compact_small` | 36.41s | 4.52s (−88%) |
| `write_parquet` | 18.07s | 3.60s (−80%) |
| `update_simple` | 5.38s | 0.74s (−86%) |
| `build_stats` | 3.37s | 1.37s (−59%) |
| writes that compacted | 18 | 3 |
| mean plain write | 276.6ms | 106.3ms |
| bytes written | 459.8 MiB | 112.5 MiB (−76%) |
| bytes read | 302.2 MiB | 49.4 MiB (−84%) |
| live size (10M rows) | 157.6 MiB | 63.1 MiB (−60%) |
| write amplification | 2.92x | 1.78x |

### 24.3 DuckDB pushdown — verified, and how to check it

Polars-written files **are** pruned by DuckDB: ~8x faster on a narrow predicate over a
163-row-group / 20M-row file (pyarrow 9.1x on the same data), and the polars file full-scans
faster too because it is 3.5x smaller. Full `DataReader` → DuckDB read checks pass on a
compacted 1.5M-row table (count, sum, range predicate, distinct, filter+agg).

**Do not verify this with `parquet_metadata()`.** That view surfaces only the deprecated v1
`min`/`max` columns and reports **NULL** for these files; polars writes the v2
`min_value`/`max_value` fields, which the reader actually prunes on. Checking the metadata view
would produce a false alarm. Verify with a timing or I/O measurement instead — and note DuckDB
answers unfiltered `count(*)` from metadata alone, so it is useless as a baseline.

### 24.4 Trade-offs taken, deliberately

* **More live files.** Hysteresis leaves files in the 12–16 MiB band alone, and smaller files
  mean more of them accumulate before the gate's `sum(small) > max_mem` trips. Final file
  count went 10 → 22 for the same 10M rows, oscillating in a healthy sawtooth (4 → 28 → 4)
  rather than being pinned low by constant rewriting.
* **Chunk memory is bounded by compressed bytes.** `compact_resources` caps a chunk on summed
  *file_size*. Better compression means a 16 MiB chunk now holds ~2.6M rows instead of ~1.1M,
  so peak memory during a merge rises proportionally. Still bounded, but `max_memory_chunk_size`
  is a weaker proxy for memory than it was.
* **Tombstone cost is unmeasured here.** An append-only load tombstones nothing —
  `build_tombstone` totalled 0.5ms across all 100 writes. Any claim about tombstone
  performance needs an upsert/delete workload.

### 24.5 A telemetry trap that was fixed

`_write_df_parquet` serves **both** system artifacts, and its encode span was hardcoded to
`tombstone.encode`. It therefore fired on 100/100 writes for 2.44s on a workload with **zero
deletes** — the stats parquet's encode wearing a tombstone label. Anyone reading that
telemetry would have concluded tombstones cost 3.6% here. The span is now labelled by the
caller (`tombstone` / `stats`).
