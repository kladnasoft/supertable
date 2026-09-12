# SuperTable — performance audit: Python round-trips & repeated work

**Scope:** investigation only. No library file was modified.
**Date:** 2026-09-11 · **Tree:** `/home/kladnasoft/dev/dataisland/supertable` @ `01cbb65` (clean)

## Measurement environment

| | |
|---|---|
| Interpreter | `.venv/bin/python` (3.10) |
| Storage | `STORAGE_TYPE=LOCAL` (`SUPERTABLE_HOME=/tmp/stprobe/home`) for CPU-bound work |
| Storage (I/O costs) | real MinIO dev box `192.168.168.130:9000` for per-call RTTs |
| Redis | **real** server on `localhost:6379`, isolated `DB 9` (median GET RTT **0.962 ms**, p90 1.66 ms) |
| Dataset | `supertable/tests/pruning/dataset.py` — 4 tables, 96k/30k/6k/2k rows, 24/12/6/4 files |
| Probes | `/tmp/stprobe/p1…p20*.py` (throwaway) |

`build/` was excluded throughout — it is a stale build artifact tree (its `redis_catalog.py` is 18,731 lines vs 1,803 in the real source) and is not importable code.

**Baselines on this box** (LOCAL storage, loopback Redis — these are *floors*; object storage and networked Redis make every I/O finding worse):

| query | wall |
|---|---|
| `SELECT count(*) FROM facts` (96k rows, 24 files) | **77.7 ms** |
| `SELECT count(*) … WHERE region='AT' AND amount>100` | 80–104 ms |
| 3-table join, GROUP BY | **171–214 ms** |
| `SELECT * FROM facts` → polars frame | 197 ms |

The headline result: **for small-to-medium queries, most of the wall time is fixed per-query overhead, not data work.** A `count(*)` over 96k rows spends 22.3 ms (29%) in Redis alone, and on an object-store deployment a further ~14 ms per alias re-configuring DuckDB's S3 layer and ~6.7 ms per referenced table rebuilding a storage client. Summing the removable fixed overhead below gives **≈100 ms/query on a 3-table join against S3/MinIO** — comparable to the entire query.

---

# Findings, highest impact first

---

## 1. `get_storage()` is not memoized — a fresh storage client **and a network round-trip** per referenced table, per query and per write

**IMPACT — MEASURED.** MinIO: `get_storage()` costs **6.73 ms median** (min 5.31), of which **5.06 ms is a `bucket_exists` network call**. Called **N+1 times per query** (N = tables named). Measured call counts: 2× for a 1-table query, 4× for a 3-table join.
→ **27 ms/query wasted on a 3-table join against MinIO.** On S3/Ceph (the production backend) it is worse: `boto3.client("s3")` construction alone measures **14.22 ms** with no network, and `S3Storage.from_env()` then adds a `head_bucket` round-trip on top → **≈60–80 ms/query**.

**Where**
- `supertable/storage/storage_factory.py:32` — `def get_storage(...)`: constructs a brand-new backend on every call. No cache of any kind.
- `supertable/storage/minio_storage.py:137` — `storage._ensure_bucket_exists(bucket, region)` → `client.bucket_exists(bucket)`, a live S3 `HeadBucket`.
- `supertable/storage/s3_storage.py:127` — `self._ensure_bucket_region()` → `head_bucket`.

**Callers that pay it**
- `supertable/data_reader.py:63` — once per `DataReader`.
- `supertable/super_table.py:64` — once per `SuperTable`, and `supertable/engine/data_estimator.py:706` builds one **inside the per-table loop** (measured: 3× for a 3-table join).
- `supertable/meta_reader.py:146`, plus `SimpleTable` on the write path.

**Cost shape:** scales with tables-per-query × queries. Not row-scale, but it is on the fixed cost of *every* query and *every* write.

**FIX PROPOSAL.** Memoize in the factory, keyed on `(resolved storage_type, frozenset(kwargs.items()))`, at module level — exactly the pattern `supertable/processing.py:50-54` (`_get_storage()`) already uses locally. Backend objects are stateless handles over a thread-safe SDK client, so one per process is correct; `kind=`/`**kwargs` callers keep their own cache slot. Sub-point: `_ensure_bucket_exists` / `_ensure_bucket_region` are one-time validations, so folding them behind the cache removes the network call as a side effect.
**Expected saving:** N+1 client builds and N+1 HeadBucket round-trips per query → one per process. ≈27 ms/query (MinIO) to ≈60–80 ms/query (S3) on a 3-table join, plus the same on the write path.

---

## 2. `query_sql()` drags every result row through Python **twice**

**IMPACT — MEASURED.** On a 96,000 × 11 result:

| step | time |
|---|---|
| `DataReader.execute()` — run the query, return a polars frame | **197 ms** |
| `[list(r) for r in result_df.rows()]` — the conversion | **813 ms** |
| `result_df.rows()` alone (tuples) | 595 ms |
| → cost of the redundant `list(...)` wrapper | **218 ms (+37%)** |
| columnwise `zip(*[c.to_list() …])` → tuples | **380 ms** |
| `query_sql` end-to-end | **1154 ms** |

**The Python conversion costs 4.1× the query itself.** The fastest correct alternative is **2.14× faster** than the current code.

**Where:** `supertable/data_reader.py:601`
```python
rows = [list(r) for r in result_df.rows()]
```
`.rows()` already materialises N Python tuples; the comprehension then allocates N more Python lists and holds both alive simultaneously (≈2× peak memory on top of the frame).

**Cost shape:** **ROW COUNT.** This is the real thing. `query_sql` is the SDK/MCP/API return path. `_ensure_sql_limit` (`data_reader.py:513`, applied at `:560`) bounds it, but the bound is caller-supplied — the OData/API export case passes a large limit.

**FIX PROPOSAL.** Two independent wins, in order of safety:
1. Drop the `list(...)` wrapper — return `result_df.rows()` directly. The declared type is `List[List[Any]]`, but every downstream use is JSON serialisation or indexing, where a tuple is indistinguishable. **Saves 218 ms / 96k rows (27%) for a one-token change.**
2. Build columnwise: `list(zip(*[c.to_list() for c in df.get_columns()]))`. Each `to_list()` is a vectorised Rust conversion; only the final `zip` is per-row, and it is C-level. **Verified byte-identical** to the current output after the existing `fill_nan(None)` (probe `p1d_equiv.py`: `current == columnwise → True`). *Caveat:* a naive comparison reports mismatches because `NaN != NaN`; they vanish once `fill_nan` has run, which production does at `:598`. **2.14× overall.**

Consider also documenting `DataReader.stream()` as the right call for large exports — it already exists and avoids this entirely.

---

## 3. `configure_httpfs_and_s3` re-runs **per alias per query**, bypassing the gate built to prevent exactly that

**IMPACT — MEASURED.** One call costs **≈14.4 ms** on an object-store deployment:

| component | measured |
|---|---|
| `SELECT name FROM duckdb_settings()` → Python set (160 rows) | **4.48 ms** |
| ~12 × `SET …` | 0.82 ms each → **9.8 ms** |
| `LOAD httpfs` (already loaded) | 0.81 ms |
| `os.makedirs(exist_ok=True)` | 0.02 ms |

Measured call counts per query: **1× per alias** — 1 for a single table, **3 for a 3-table join** (→ **43 ms/query**), 2 for a self-join.

**Where:** `supertable/engine/engine_common.py:542` — `configure_httpfs_and_s3(con, files)`, called from `create_reflection_view_with_presign_retry` ← `supertable/engine/duckdb.py:193` (`_build_view_chain`), once per alias. Same pattern at `engine_common.py:461` (MATERIALIZE path) and on presign retry.
The expensive scan is `engine_common.py:262`.

The connection-level gate already exists — `duckdb.py:339` calls `self._ensure_httpfs(...)` behind a thread-local flag (`duckdb.py:54`) — but the `:542` call sits outside it and re-does the whole setup.

**Cost shape:** ALIAS COUNT × queries, on any S3/MinIO/HTTP deployment. LOCAL escapes via the early return at `engine_common.py:219-220`, which is why this is invisible on a local test run and costly in production.

**FIX PROPOSAL.** Make `configure_httpfs_and_s3` self-memoizing per `(connection, credentials-fingerprint)` — store the fingerprint on the connection's thread-local state and return immediately when unchanged. The function is already documented as idempotent and re-reads env each call, so the fingerprint is exactly "the env values it would apply". Independently, hoist the `duckdb_settings()` capability scan to a module-level cache keyed by DuckDB build — the supported-setting set cannot change within a process, and `_external_file_cache_cappable` (`engine_common.py:618`) already demonstrates this pattern in the same file.
**Expected saving:** ≈14.4 ms × (aliases − 1) per query on object storage; 43 ms → ~14 ms on a 3-table join, and ~0 after the first query if fingerprinted per connection.

---

## 4. Compaction merges files with pairwise `concat_with_union` — **O(F²)** data movement

**IMPACT — MEASURED.** Merging F frames of 5,000 rows × 11 columns:

| files | pairwise (current) | `concat_many_with_union` | ratio |
|---:|---:|---:|---:|
| 10 | 70.8 ms | 27.3 ms | 2.6× |
| 25 | 130.4 ms | 57.2 ms | 2.3× |
| 50 | 294.6 ms | 105.7 ms | 2.8× |
| **100** (= default `MAX_OVERLAPPING_FILES`) | **700.6 ms** | **183.2 ms** | **3.8×** |
| 200 | 1713.0 ms | 353.5 ms | 4.8× |

**At the default setting this wastes 517 ms per compaction chunk**, and the ratio grows with F — the signature of O(F²) vs O(F). With GA4-style dynamic columns (the case the union helper was written for) the same pattern holds: 881.5 ms vs 258.6 ms at 100 files, output shapes verified identical.

**Where:** `supertable/processing.py:584` — inside `for file_path, file_size in candidates:`
```python
chunk_df = concat_with_union(chunk_df, existing_df)
```
`concat_with_union` (`processing.py:165`) re-derives the union schema and runs `_align_to_schema` on **both** inputs — a full `select` copy of the whole accumulated buffer — on every iteration.

**Cost shape:** FILE count, but each iteration copies the accumulated *data*, so the wasted work is O(F × total_rows). It fires precisely when there are many small files — i.e. whenever compaction is worth running.

**FIX PROPOSAL.** The fix is already in the file and unused: `concat_many_with_union` (`processing.py:175`) computes one union schema and issues a single `polars.concat`. Buffer the survivor frames in a list and concat once at each flush. The memory objection in that helper's own docstring does not apply here: the flush gate at `processing.py:590` is driven by `chunk_size_bytes` accumulated from `file_size` at `:587`, entirely independent of the concat, so the same set of frames is resident either way — and pairwise concat actually has *higher* peak memory, since `polars.concat` transiently holds accumulator + new + result.
**Expected saving:** 3.8× on the merge step at default settings (517 ms/chunk); more on wider tables.

---

## 5. `compact_tombstones` full-scans the entire deletion vector once per file — **O(V × F)**

**IMPACT — MEASURED.** Selecting each file's dead row-ids:

| DV rows (V) | files (F) | filter-in-loop (current) | `partition_by` once | ratio |
|---:|---:|---:|---:|---:|
| 50,000 | 50 | 279.9 ms | 62.2 ms | 4.5× |
| 200,000 | 100 | 790.0 ms | 277.0 ms | 2.9× |
| **1,000,000** (= default `MAX_TOMBSTONE_ROWS`) | **100** | **2680.6 ms** | **400.8 ms** | **6.7×** |
| 1,000,000 | 500 | 11,618.9 ms | 1601.9 ms | 7.3× |

**At the default vector size this wastes 2.28 s per tombstone compaction; at 500 files, 10 s.**

**Where:** `supertable/processing.py:2840` — inside `for file_path in files_with_deletes:` (loop opens at `:2818`)
```python
tombstone_df.filter(polars.col(TOMBSTONE_FILE_COL) == file_path)
```

**Cost shape:** the loop bound is FILE count but the per-iteration work is **ROW scale over the whole deletion vector**, so the product is what bites. `MAX_TOMBSTONE_ROWS` defaults to 1,000,000 (`config/settings.py`), and `compact()` always drains the vector regardless of the lazy threshold.

**FIX PROPOSAL.** Hoist one `tombstone_df.partition_by(TOMBSTONE_FILE_COL, as_dict=True)` above the loop and index into it. This also subsumes the `files_with_deletes` computation at `processing.py:2814` (the dict keys *are* that list), removing a `.unique().to_list()` pass. Output verified identical in the probe (`tot == tot2` asserted at every size).
**Expected saving:** O(V×F) → O(V). 6.7× at defaults.

---

## 6. `prune_files_by_predicates` rebuilds a Python dict index on **every query**

**IMPACT — MEASURED.** Cost of one call, by table size (3 constrained columns):

| files | row groups/file | stats rows | per query |
|---:|---:|---:|---:|
| 24 | 1 | 264 | 14.4 ms |
| 100 | 4 | 4,400 | 15.3 ms |
| 400 | 9 | 39,600 | **79.3 ms** |
| 1000 | 9 | 99,000 | **235.0 ms** |

**On a 1000-file table this is 235 ms of pure Python per query** — more than the entire measured 3-table join on the 24-file corpus.

**Where:** `supertable/processing.py:2530`
```python
for row in needed.iter_rows(named=True):
```
Each iteration allocates a Python dict for the row, then `_stored_lane(row)` (`processing.py:2206`) does up to 8 further dict probes. A 3-level nested dict `{file: {row_group: {column: lane}}}` is built from scratch. Called once per table per query from `data_estimator.py:530`.

**Cost shape:** FILES × ROW_GROUPS × CONSTRAINED_COLUMNS. Not data-row scale — but that product grows with the table's physical size, which is exactly what a data lake does over time.

**Already cached?** The *stats frame* is cached in-process (`_STATS_CACHE`, `processing.py:2560+`). The *index derived from it* is not — it is rebuilt every query.

**FIX PROPOSAL.** Two options, either sufficient:
1. **Memoize the index** keyed by `(stats_file_path, tuple(constrained_cols))`. The stats artifact is immutable and versioned, so the key is exact and a stale entry simply misses — the same correctness argument that already justifies `_STATS_CACHE`. This reduces a repeated query workload to one build per (table version, predicate column set).
2. **Columnarise the build** — resolve the lane with a polars `coalesce`/`when-then` chain over the whole frame, then emit via `partition_by`, so no per-row Python dict is allocated.
**Expected saving:** 235 ms → ~0 on repeat queries (option 1), or roughly an order of magnitude on first build (option 2). Option 1 is the bigger win for a serving workload.

---

## 7. Redundant Redis reads: `meta:root` fetched up to 7× and `EXISTS`-ed 6× **within one query**

**IMPACT — MEASURED.**

| query | Redis calls | Redis time | share of wall | redundant calls |
|---|---:|---:|---:|---:|
| `count(*)`, 1 table | 14 | **22.3 ms** | **29 %** of 77.7 ms | 3 |
| 3-table join | 26 | **31.4 ms** | 15 % of 214 ms | **11** |

For the 3-table join the duplicates are `GET …meta:root` **7×** and `EXISTS …meta:root` **6×** — 13 round-trips for one **33-byte** value. At the measured 0.96 ms loopback RTT that is ≈13 ms/query; on a networked or Sentinel-fronted Redis (the production config in `.env`) it scales directly with RTT.

**Where:** the root cause is `supertable/redis_catalog.py:473` — `_resolve_replica_info` calls `get_root(org, sup)` on **every** leaf access, so each `get_leaf` / `leaf_exists` / `scan_leaf_keys` silently prepends a `GET meta:root`. Call sites that each pay it:
- `supertable/data_reader.py:109` `root_exists` and `:111` `leaf_exists`
- `supertable/engine/data_estimator.py:460` (`scan_leaf_items`)
- `supertable/super_table.py:73` `root_exists` (once per `SuperTable`, i.e. per table)
- `supertable/data_reader.py:324` `catalog.get_leaf(...)` — once per **alias** (`data_reader.py:318` iterates `tables`, not physical tables, so a self-join doubles it)

**Cost shape:** aliases + tables per query. Pure fixed overhead.

**FIX PROPOSAL.** Give `RedisCatalog` a per-instance, per-operation request cache for `meta:root` (a dict keyed `(org, sup)`, populated on first read, cleared on any root bump). The root pointer is read-only for the whole duration of a read query, so one fetch is provably enough. Separately, the `EXISTS meta:root` calls are redundant with the `GET` that follows — a `GET` returning `None` is the same signal. Pairs naturally with finding 8, which removes the `get_leaf` calls entirely.
**Expected saving:** ~11 of 26 Redis round-trips on a 3-table join → ≈13 ms/query on loopback, proportionally more on a real network.

---

## 8. The estimator reads **every leaf in the supertable** to answer a single-table query

**IMPACT — MEASURED.** On a 100-table supertable:

| | |
|---|---|
| `scan_leaf_items(...)` per query | **13.44 ms** |
| payload bytes pulled per query | **134,530 B** |
| payload bytes actually needed | 1,346 B → **100× waste** |
| targeted `MGET` of the one needed key | **1.69 ms** |

End-to-end, a `SELECT count(*) FROM t0` grew from **45.2 ms → 67.5 ms** and its Redis time from 17.3 ms → 25.5 ms as the supertable went from 1 to 100 tables — *while touching exactly one table the whole time*.

**Where:** `supertable/engine/data_estimator.py:460`
```python
items = list(self.catalog.scan_leaf_items(organization, super_name, count=512))
```
`scan_leaf_items` (`redis_catalog.py:1118`) SCANs `…meta:leaf:doc:*` and pipeline-GETs **every** leaf's full payload; `_filter_snapshots` (`data_estimator.py:476`) then discards all but the requested table, in Python. The comment at `data_estimator.py:695` ("Collect snapshots ONCE per super_name — avoid redundant SCAN per simple table") optimised the wrong axis: it deduplicates across the query's tables but still reads the whole supertable.

**Cost shape:** number of tables in the supertable, and — because each leaf payload embeds the table's full resource list — the total **file count across all tables**. Measured leaf payload was 1,346 B for a 1-file table; a 500-file table's leaf is two orders of magnitude larger, so a mature multi-tenant supertable pulls megabytes per query.

**FIX PROPOSAL.** Replace the SCAN with a targeted `MGET` of the `K` leaf keys the query names — `physical_tables` already carries the names before `estimate()` is called. That makes it O(tables in query) instead of O(tables in lake). While there, return the fetched payloads on the `Reflection` so `data_reader.py:324`'s `get_leaf` (which wants only `payload["tombstone"]` and `payload["_row_filter"]`, both already present) disappears — that removes 2 more Redis round-trips per alias and makes finding 7 largely moot. A dict index also retires the linear `_filter_snapshots` scan at `data_estimator.py:476`.
**Expected saving:** 13.44 ms → 1.69 ms at 100 tables, and it stops growing with the lake. Removes 2 round-trips/alias on top.

---

## 9. Compaction re-downloads, **in full and twice**, the file it just wrote — to read a footer and a schema

**IMPACT — MEASURED.** Compacting a 40-file table (`small_only=False`), with a counting storage backend:

```
write_bytes : 2 calls,  9,698,758 B written
read_bytes  : 2 calls, 19,382,824 B read back   <- 2x the bytes just written
```
Both reads are the **same 9.69 MB output file**, downloaded whole, moments after it was produced:

| bytes | call site |
|---:|---|
| 9,691,412 | `processing.py:1904` `_read_footer_metadata` ← `processing.py:2016` `extract_stats_rows` ← `data_writer.py:1639` `compact` |
| 9,691,412 | `data_writer.py:263` `_build_compact_model_df` ← `data_writer.py:1678` `compact` |

MinIO cost of a whole-object read vs what a footer actually needs:

| | measured |
|---|---|
| `read_bytes` of a 16 MiB object | **107.92 ms** |
| ranged tail read, 64 KiB | **11.04 ms** (9.8× cheaper) |

A compaction producing ten 16 MiB chunks therefore downloads ~320 MiB it already had in memory ≈ **2.2 s**.

**Where**
- `supertable/processing.py:1904` — `data = _get_storage().read_bytes(path)`, then `pq.read_metadata(io.BytesIO(data))`. The docstring says "no data pages are decoded" — true, but they are all **downloaded**.
- `supertable/data_writer.py:263` — `raw = self.super_table.storage.read_bytes(first_path)`. The comment directly above it explains that `read_parquet` was replaced because it "fully decodes the file… ~260ms per compacting write" — the *decode* was optimised away, the *download* was not.
- The footer cache exists and works: `data_writer.py:732` passes `footer_md_out=footer_md_cache` on the incoming-data branch. But `compact_resources` calls `write_parquet_and_collect_resources` at `processing.py:594` and `:608`, and `compact_tombstones` at `processing.py:2856`, **all without `footer_md_out=`** — so compaction output never enters the cache.

**Cost shape:** bytes of compaction output — i.e. it grows with exactly the thing compaction exists to produce.

**FIX PROPOSAL.** Two complementary changes:
1. **Thread `footer_md_out` through compaction** (`processing.py:594`, `:608`, `:2856`) and consult that cache in `_build_compact_model_df` before falling back to a read. The bytes are already in hand; `processing.py:761-763` shows the capture is one line.
2. **Range-read the footer** in `_read_footer_metadata`: parquet's footer length lives in the last 8 bytes, so a tail GET of ~64 KiB (with one retry for an unusually large footer) suffices. Add a `read_range`/`read_tail` to `StorageInterface`; MinIO/S3/GCS/Azure all support it natively and LOCAL is a seek.
**Expected saving:** eliminates 2× the compaction output in downloads (measured 19.4 MB per compaction here); ~97 ms per 16 MiB file wherever a genuine footer read is still needed.

---

## 10. `_safe_exists` fires a HEAD before **every** parquet read, and the read already handles absence

**IMPACT — MEASURED.** The precheck tracks reads exactly 1:1 — during a 40-file compaction: `exists: 41`, `read_parquet: 40`. On MinIO, `exists()` measures **6.62 ms** → **271 ms of pure waste per compaction**, and it grows linearly with the number of files touched.

**Where:** `supertable/processing.py:249`
```python
if not _safe_exists(path, profiler=p, strict=required):
```
in `_read_parquet_safe` — but the read that follows already converts a missing object to `FileNotFoundError` (`minio_storage.py:330-337` maps `NoSuchKey`), and that is caught at `processing.py:267-269` with the identical "file vanished" semantics. The same shape repeats at **`processing.py:1899`** in `_read_footer_metadata`, whose `FileNotFoundError` handler sits at `:1906`.

Affected read sites: `compact_resources` (`:562`), `filter_stale_incoming_rows` (`:885`), `identify_deleted_rowids` (`:1142`), `identify_all_rowids` (`:1202`), `compact_tombstones` (`:2833`), and `load_tombstone_parts` (`:2752`, up to `SUPERTABLE_TOMBSTONE_MAX_PARTS` = **100** parts → 200 round-trips where 100 would do).

**Cost shape:** FILE count × one HEAD each. Negligible on LOCAL; a per-file network round-trip on object storage.

**FIX PROPOSAL.** Delete the precheck and rely on the existing `FileNotFoundError` handler. One nuance to preserve: `strict=True` callers currently re-raise a *probe* failure so a backend error is not mistaken for absence. That intent survives naturally — a backend error from the read itself falls into the generic `except Exception` at `:270` which already re-raises when `required`. Worth a test asserting that a transient 500 on a `required=True` deletion-vector read still aborts the write.
**Expected saving:** halves the round-trips of every write-path parquet read; 271 ms per 40-file compaction on MinIO, and up to 100 round-trips per deletion-vector load.

---

## 11. RBAC re-reads the role document from Redis on every query, uncached

**IMPACT — MEASURED.** `restrict_read_access` costs **6.00 ms and 4 Redis round-trips per query** (loopback). On the OData path it is paid **twice per page** (`odata/policy.py:100` and again inside `DataReader.execute`).

**Where:** `supertable/rbac/access_control.py:233`
```python
role_manager = RoleManager(super_name=super_name, organization=organization)
```
Each construction does, in order: `EXISTS rbac:roles:meta` → `rbac_get_superadmin_role_id` → `rbac_get_role_id_by_name` → `get_role_details` (observed as `exists`, `smembers`, `hget`, `hgetall` in the call trace).

**Cost shape:** fixed per query. Role documents change on the order of days.

**FIX PROPOSAL.** TTL-cache the resolved role document keyed `(org, super, role_name)`, invalidated by the RBAC meta version counter that `_LUA_RBAC_BUMP_META` already maintains — so a single `GET` of the version can validate the cache, or a short TTL (5–30 s) can be used if a bounded staleness window is acceptable for authorization. `meta_reader.py:23` (`_SUPER_META_CACHE`) is the precedent in-tree.
**Expected saving:** 4 round-trips → 0–1 per query; ≈6 ms/query on loopback, 12 ms on OData pages, and proportionally more with a networked Redis.

---

## 12. The query SQL is parsed by sqlglot **twice** per read — with two different dialects

**IMPACT — MEASURED.** `sqlglot.parse_one` is called **2× per query** (confirmed by instrumenting the module). Cost: **2.16 ms** for a single-table query, **4.31 ms** for a 3-table join (parse+generate: 3.07 / 5.96 ms).

**Where**
- Parse 1: `supertable/utils/sql_parser.py:282` — `sqlglot.parse_one(query, read=dialect)`, result retained in `self._parsed`.
- Parse 2: `supertable/engine/engine_common.py:577` — `parsed = sqlglot.parse_one(original_sql)` inside `rewrite_query_with_hashed_tables`, which is handed `parser.original_query` (`duckdb.py:242`) and re-parses the string from scratch.

**Correctness note worth flagging alongside the cost:** parse 2 passes **no dialect**, while parse 1 used the engine dialect. The two ASTs can legitimately differ for dialect-specific syntax, and the rewrite is what produces the executed SQL.

**Cost shape:** fixed per query, scaling with query complexity. ~2.5% of a simple query, ~2.5% of a join here — but it grows with SQL size, so analytics-style queries with many CTEs pay more.

**FIX PROPOSAL.** Pass the existing AST into `rewrite_query_with_hashed_tables` instead of the raw string. The rewrite mutates the tree, so hand it `parser._parsed.copy()`. This removes one full parse and closes the dialect discrepancy at the same time.
**Expected saving:** 2.2–4.3 ms/query, more on complex SQL; plus the dialect inconsistency.

---

## 13. `_ratio_bytes` recomputes two schema-wide sums for **every file**

**IMPACT — MEASURED.** With a 40-column schema:

| files | current (per file) | hoisted | speedup |
|---:|---:|---:|---:|
| 100 | 12.57 ms | 0.15 ms | 83× |
| 500 | 61.75 ms | 0.35 ms | 176× |
| 2000 | **245.87 ms** | 1.32 ms | 186× |

**Where:** `supertable/engine/data_estimator.py:661` and `:664`, called from inside the per-file loop at `data_estimator.py:800` (loop opens `:791`)
```python
total_w = sum(self._type_width(ty) for ty in all_cols.values())
sel_w   = sum(self._type_width(all_cols[c]) for c in selected_cols if c in all_cols)
```
Both depend only on `(schema_types, selected_cols)` — identical for every file of the table. `_type_width` (`:545`) is a chain of up to 14 substring tests per column. The `all_cols` dict is also rebuilt per file at `:657-660`.

**Cost shape:** FILES × COLUMNS where COLUMNS-once would do.

**IMPORTANT CAVEAT — this is the Tier-2 fallback, not the default path.** It only runs for files whose stats lack per-column `compressed_bytes` (Tier 3, `_projected_bytes_index`). Since `compressed_bytes` was added to `STATS_SCHEMA` with back-compat and **no migration**, every table whose stats predate that change takes this path for *all* of its files — so it is live, but on legacy tables only. Ranked here accordingly; it would rank higher if it were unconditional.

**FIX PROPOSAL.** Hoist `all_cols`, `total_w` and `sel_w` out of the loop (compute once per table in `estimate()`), and pass the ratio in. Optionally memoize `_type_width` with `functools.lru_cache` — the type-string domain is tiny.
**Expected saving:** O(F×C) → O(C). 246 ms → 1.3 ms on a 2000-file legacy table.

---

## 14. `RedisCatalog()` constructed 6–8× per query; `apply_runtime_pragmas` + `resolve_engine_configs` re-run per query

**IMPACT — MEASURED.**

| | measured |
|---|---|
| `RedisCatalog()` construction | **0.265 ms** each × **6** (1 table) to **8** (3-table join) = **1.6–2.1 ms/query** |
| of which `register_script` × 7 | 0.075 ms (client-side SHA-1 only, no round-trip) |
| `apply_runtime_pragmas` with byte-identical config | **2.73 ms/query** |
| `resolve_engine_configs` (Redis read + builds **both** lite and pro; pro discarded) | **1.66 ms/query** |

Combined: **≈6 ms/query** of provably repeated setup.

**Where**
- `supertable/redis_catalog.py:298` — `__init__` registers 7 Lua scripts plus `RedisLocking`. Constructed at `data_reader.py:96`, `data_reader.py:317`, `data_estimator.py:155`, `executor.py:46`, `rbac/role_manager.py:56`, and `super_table.py:67` (× tables — measured 3× for a 3-table join).
- `supertable/engine/duckdb.py:353` — `apply_runtime_pragmas(con, engine_config)` per query. Its docstring justifies this (live config adoption), which is legitimate — but it re-issues every pragma unconditionally even when nothing changed. Inside, `_parse_memory_limit_mb` does a per-call `import re` with an uncompiled pattern (`engine_common.py:127-128`).
- `supertable/engine/executor.py:225` — `resolve_engine_configs(...)` builds `{e: _build_runtime(cfg, e) for e in DUCKDB_ENGINES}` (`engine_config.py:229`); `executor.py:226` takes only `cfgs["lite"]`. Each `_build_runtime` calls `_effective` 8× with `os.getenv` (`engine_config.py:180`) — 16 `os.getenv` per query for values `config/settings.py` froze at import.

**FIX PROPOSAL.** (a) Thread one `RedisCatalog` through the request instead of constructing per component — `DataReader` already does this locally at `data_reader.py:96` ("One catalog handle for the whole loop"); extend that to the query. Or make the script registration a class-level constant, since the SHA-1s are content-addressed and never change. (b) Hash the resolved `EngineRuntimeConfig` onto the connection and skip `apply_runtime_pragmas` when unchanged — preserving the live-adoption contract while making the steady state free. (c) Build only the requested engine flavour, and read from `settings` rather than `os.getenv`.
**Expected saving:** ≈6 ms/query — individually small, collectively the same order as finding 12.

---

## 15. `QueryPlanManager` globs its temp directory on every query — cheap now, unbounded later

**IMPACT — MEASURED**, conditional on directory population:

| files in temp dir | `QueryPlanManager()` per query |
|---:|---:|
| 0 (observed in practice) | 0.166 ms |
| 200 | 0.832 ms |
| 2,000 | 5.21 ms |
| 10,000 | **25.21 ms** |

**Where:** `supertable/query_plan_manager.py:99` — `glob.glob(pattern)` + sort, called unconditionally from `__init__` (`:66`), i.e. once per query.

**Assessment — currently benign.** On the DuckDB path the directory stays empty: `init_connection` only enables profiling when `profile_path` is passed (`engine_common.py:725`), and `duckdb.py:122` never passes one, so no plan JSON is ever written. The retention policy keeps `max_keep=200` files **per distinct query hash**, so the scan is O(total files) while the cap is per-hash — the population is unbounded in query diversity if anything ever does write there (Spark path, or profiling enabled).

**FIX PROPOSAL.** Replace `glob` + full sort with `os.scandir` filtered by prefix, and/or run the cleanup on a cadence (every Nth construction, or a background sweep) rather than synchronously per query. Low priority — file a note, not a fix, unless profiling is turned on.

---

# Checked and DISMISSED

| Candidate | Verdict |
|---|---|
| `data_reader.py:598` `result_df.fill_nan(None)` — unconditional full-frame pass | **MEASURED 0.81 ms** on a 96k × 11 frame with **zero** float columns. Row-scale but fully vectorised. Micro-optimisation; not worth doing. |
| `utils/sql_parser.py:858` `traverse_scope(self._parsed)` — sqlglot scope builder per query | **MEASURED**: `SQLParser()` ctor = 5.55 ms; ctor + `get_predicate_constraints()` = 5.34 ms → delta within noise. It reuses the cached AST. Not a finding. (It *is* run even when read pruning is disabled — a correctness-of-gating nit, not a cost.) |
| `engine/data_estimator.py:637` `for r in agg.iter_rows(named=True)` | Bound is **FILE count** (post `group_by("file_path")`). Fine. |
| `processing.py:1747` `dead_map = {f: int(n) for f, n in dead_counts.iter_rows()}` | Bound is **FILE count** (post `group_by`). Fine. |
| `processing.py:2814` `…unique().get_column(TOMBSTONE_FILE_COL).to_list()` | Bound is **FILE count**. Fine — and it disappears for free with the finding-5 fix. |
| `processing.py:976` `[(f, int(r)) for f, r in frame.iter_rows()]` (`delete_pairs_to_list`) | **Genuinely ROW-scale**, but **dead in production** — grep finds callers only under `supertable/tests/`, and its own docstring warns it off the write path. Not a live defect; worth a guard so it cannot come back. |
| `engine/duckdb.py:336` `alias_to_files[td.alias] = list(sup.files)` | FILE count. Fine. |
| `engine/arrow_result.py:123` `batches = list(handle.batches())` | Row-scale **memory** by design (buffered = keep every batch), but no per-row Python. Correct as written; `stream()` is the alternative. |
| `engine/spark_thrift.py:276`, `:1008` — `[row[0] for row in cursor.fetchall()]` | COLUMN / plan-line count. Fine. |
| `_filter_snapshots` linear scan (`data_estimator.py:476`) | O(tables_in_lake × tables_in_query) with a `.lower()` per compare — real, but dominated by finding 8's I/O and **moot once that is fixed** by a targeted MGET. Folded into finding 8. |
| `is_file_in_overlapping_files` (`processing.py:281`) — O(n) scan where a set would do | **Dead code** (callers only in `supertable/tests/`). Cleanup, not performance. |
| `engine_common.py:1333` — un-gated `SELECT DISTINCT __rowid__` over the deletion vector | **Already cached.** `TombstoneCache` materialises `dv_table` once per DV version (`engine_common.py:1465`) and `data_reader.py:347` sets a stable `cache_key`. Only degrades if `SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE <= 0` (default 8). No action. |
| `_external_file_cache_cappable` (`engine_common.py:618`) | **Already memoized** per DuckDB build. Cited as the pattern finding 3 should follow. |
| `DataWriter._get_table_config` (`data_writer.py:184`) | **Already cached.** Correct. |
| `redis_connector` client construction | **Already pooled** via `_CLIENT_CACHE`. Measured `RedisConnector(None).r` = 0.008 ms. Only the `RedisCatalog` wrapper around it is rebuilt (finding 14). |
| polars ↔ pandas round-trips | **None exist** in the read or write path. `arrow_result.py` goes Arrow → polars directly. Previously-removed `.values.tolist()` is documented at `data_reader.py:588-596`. |
| `.to_pylist()` / `.to_dicts()` / `.item()` / `.apply()` / `map_elements` | **Zero occurrences** in non-test, non-demo source. |
| Arrow-based row conversion as an alternative for finding 2 | **MEASURED and rejected**: `df.to_arrow().to_pylist()` = 5356 ms vs 813 ms current — **6.6× slower**. Do not "optimise" in that direction. |

---

# Notes and caveats

1. **Every I/O number above is a floor.** LOCAL storage and loopback Redis are the cheapest possible backends. Production is Ceph (S3) with a Sentinel-fronted Redis, where per-call RTTs are larger and findings 1, 3, 7, 8, 9, 10 and 11 all scale with RTT.
2. **A dataset-builder bug, not a library one:** `supertable/tests/pruning/dataset.py:170` calls `df.iloc[0]["n"]` on a **polars** frame. `.iloc` does not exist, the `AttributeError` is swallowed by the bare `except Exception: return False`, so `exists()` always returns `False` and `build(rebuild=False)` always rebuilds the whole corpus. Harmless correctness-wise, but it silently defeats the reuse fast path in every benchmark that calls it.
3. **Column projection does not save network bytes** on object storage: `minio_storage.py` `_get_object_safe` downloads the whole object and projects afterwards, so the `read_columns` projection at `processing.py:1569-1573` saves decode time and memory only. Relevant when reasoning about the memory-bounded overwrite fallback.
4. **`SUPERTABLE_DUCKDB_WRITE_PROBE` defaults to `False`** (`config/settings.py:191`), so by default every overwrite takes the polars fallback that the module docstring (`processing.py:1226-1235`) describes as reading *"EVERY overlapping data file FULLY… cost O(table size)"*. Per the project's own history this default is deliberate (offline SDK environments cannot `INSTALL httpfs`), so it is recorded here as context for findings 4/5/10 rather than as a defect — but it is the single largest config-level lever on write cost.
5. **Probe scripts** are under `/tmp/stprobe/` (`p1`–`p20`). Redis `DB 9` and `/tmp/stprobe/home` were used for probe data and can be flushed; nothing outside them was touched.
