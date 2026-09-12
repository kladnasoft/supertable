# SuperTable write-path adversarial audit

Repo `/home/kladnasoft/dev/dataisland/supertable` @ `1a52c79` (v3.2.0).
Scope: `processing.py`, `data_writer.py`, `simple_table.py`, `super_table.py`,
`locking/`, compaction, tombstone/DV, stats artifact, rowid allocation, snapshot commit.

All repros run with `.venv/bin/python`, `STORAGE_TYPE=LOCAL`, real `RedisCatalog`
Lua on `fakeredis`+`lupa` (the repo's own characterization harness). Probe scripts are in
`/tmp/claude-1000/.../scratchpad/audit/`. **No library code was modified.**

Twelve findings. 8 PROVEN by a running repro, 4 REASONED from traced call paths.

---

## F1 — One transient Redis error makes `write()` erase a live table

**SEVERITY: CRITICAL** · **PROVEN** (`repro_leaf_exists.py`)

**Where**
- `supertable/redis_catalog.py:393-398` — `_leaf_exists_raw` catches `redis.RedisError` and returns `False`
- `supertable/simple_table.py:131-149` — bootstrap is gated on exactly that boolean
- `supertable/data_writer.py:488` — the writer constructs `SimpleTable(self.super_table, simple_name)` with the default `create_if_missing=True`
- Amplifier: `supertable/redis_connector.py:156,160` — Sentinel mode sets `socket_timeout=0.5`, and the repo `.env:32` has `SUPERTABLE_REDIS_SENTINEL=true`

**What goes wrong.** `leaf_exists()` conflates "the table does not exist" with "I could not ask".
A single `redis.TimeoutError` (a `RedisError` subclass — 500 ms is the Sentinel budget) on the
`EXISTS` at `data_writer.py:488` makes `SimpleTable.__init__` conclude the table is missing and call
`init_simple_table()` (`simple_table.py:149`), which writes a fresh snapshot with `resources: []`,
`snapshot_version: 0`, `tombstone: None`, `stats_file: None` and **publishes it over the live leaf
pointer** (`simple_table.py:189`). `write()` then reads that empty snapshot back at
`data_writer.py:489`, sees no resources to overlap, writes its one new file and commits.

Repro (3 prior writes, 6 rows, then one flaky `EXISTS`):

```
BEFORE: version=3 resources=3 rows=6 watermark=6
write() returned: (2, 1, 1, 0)  (no exception raised)
AFTER : version=1 resources=1 rows=1 watermark=7
RESOURCES SILENTLY DROPPED FROM THE SNAPSHOT: 3   (all still on disk, orphaned)
```

The write reports success. `snapshot_version` goes **backwards** (3 → 1) and the
`previous_snapshot` chain is re-rooted at the bootstrap snapshot, so nothing downstream can
detect the regression from the snapshot alone.

**Why existing guards do not catch it.** The table lock does not help — this is not a race, it is a
fail-open on an error, and the writer holds the lock the whole time. The pre-commit fence
(`_assert_lock_still_held`) checks lock ownership, not that the snapshot it is publishing descends
from a real one. `compact()` gets this right and is fail-*closed* — `data_writer.py:1437-1440` calls
`root_exists`/`leaf_exists` and raises `TableNotFoundError` — but that check is only reached when the
answer is `False`, and it is the *same* fail-open primitive, so a transient error there merely aborts
(safe) instead of destroying (unsafe). Only the `create_if_missing=True` writer path is dangerous.

**FIX PROPOSAL.** Separate "absent" from "unknown" at the source. `_leaf_exists_raw` (and
`root_exists`, `redis_catalog.py:374-381`) should let `redis.RedisError` propagate, or return a
tri-state, so that a backend error can never be read as absence. Bootstrap is an *irreversible,
destructive* decision and must fail closed: the only correct response to "I could not determine
whether this table exists" is to abort the write. This is the same shape as the `required=True`
contract already applied to the deletion-vector reads (`processing.py:230-247`) — apply it to
existence probes too. Secondarily, `init_simple_table` should refuse to publish a `version 0` leaf
over an existing pointer; the leaf SET should be a genuine create-only (`SET ... NX`) so that even a
mis-decided bootstrap cannot clobber a live table.

---

## F2 — `newer_than` consults physically-present rows, ignoring the deletion vector

**SEVERITY: HIGH** · **PROVEN** (`repro_newerthan_dv.py`)

**Where**
- `supertable/processing.py:843-930` — `filter_stale_incoming_rows` (default polars path)
- `supertable/processing.py:1456-1505` — `_derive_stale_and_deletes` (DuckDB-probe path); both compute `existing_max` over every row in the candidate files
- `supertable/data_writer.py:566-572` — `resolve_overwrite_writes` runs here, **before** the DV is even loaded at `data_writer.py:651`

**What goes wrong.** The stale filter drops an incoming row when some existing row with the same key
has a `newer_than` value `>=` the incoming one. It reads the *physical* files, which still hold
logically-deleted rows until compaction drains them. So a row that the user deleted keeps vetoing
re-inserts of that key — and stops vetoing them the moment compaction happens to run.

Repro: two rows in one file, delete `id=1`, then re-insert `id=1` with an older `ts`:

```
[nocompact] after delete, live rows: [(2, 'keep')]
[nocompact] write() -> (cols,rows,ins,del)=(3, 0, 0, 0)   live rows: [(2,'keep',100)]

[compacted] after delete, live rows: [(2, 'keep')]
[compacted] compact(): tombstone_rows_removed=1
[compacted] write() -> (cols,rows,ins,del)=(3, 1, 1, 0)   live rows: [(1,'reborn',50), (2,'keep',100)]

IDENTICAL OPERATION SEQUENCE, DIFFERENT DATA: True
```

The write silently returns `inserted=0` with no error. Because auto-compaction fires on its own
thresholds (`data_writer.py:862-868`), production behaviour is **non-deterministic**: whether a write
lands depends on internal file/DV state the caller cannot see. Worse, the veto is permanent for a key
whose dead row happens never to be drained (e.g. it lives in a file above the small-file threshold).

**Why existing guards do not catch it.** The writer *does* filter already-tombstoned rows out of the
delete pairs — `data_writer.py:689-694`, the `prev_dv_df` anti-join — but that runs on the *delete*
side only, and after the stale filter has already dropped the incoming rows. The DV is loaded at
`data_writer.py:651`, which is 80 lines *after* `resolve_overwrite_writes` is called. `supertable/tests/test_newer_than.py`
has 14 tests and none mention `delete_only` or `tombstone`, so the interaction is entirely uncovered.
`docs/06_data_writer.md:121` documents the physical-file semantics, so this is an unnoticed
consequence of the merge-on-read model, not a documented choice.

**FIX PROPOSAL.** Move the DV load above `resolve_overwrite_writes` (it is already loaded
unconditionally a few lines later, so this costs nothing) and pass it into the resolver, which should
anti-join `matched` / `existing_parts` on `__rowid__` before computing `existing_max`. The right shape
is: *the stale filter must see the same logical table the reader sees.* Doing it inside the resolver
also keeps both the probe and the polars-oracle paths consistent, which the current split cannot
guarantee.

---

## F3 — A failure *after* the leaf commit makes `write()` raise on a write that already landed

**SEVERITY: HIGH** · **PROVEN** (`repro_postcommit.py`)

**Where**
- `supertable/data_writer.py:1129-1147` — the leaf commit (`set_leaf_payload_cas`) is the visibility point
- `supertable/data_writer.py:1149-1151` — `bump_root` runs *after* it, unguarded
- `supertable/redis_catalog.py:456-461` — `bump_root` logs and **re-raises** `redis.RedisError`

**What goes wrong.** Everything after `set_leaf_payload_cas` is already published. The schema write
(`1154-1170`), mirroring (`1173-1181`), monitoring and audit are all individually `try/except`-wrapped
— but `bump_root` is not. A transient Redis error there propagates out of `write()`.

```
before: version=1 rows=1
write() RAISED: RedisError simulated transient failure on bump_root
after failed write: version=2 rows=2  <-- the write WAS committed
after retry:        version=3 rows=3  <-- id=2 applied TWICE
```

A caller doing the only reasonable thing on an exception — retry — duplicates an append. The same
gap exists in `compact()` at `data_writer.py:1718-1722`.

**Why existing guards do not catch it.** The pre-commit fence protects the window *before* the commit.
Nothing distinguishes "failed before the commit" (safe to retry) from "failed after it" (not safe).
Because the surrounding steps are all guarded, `bump_root` looks like an oversight rather than a
deliberate hard failure.

**FIX PROPOSAL.** Treat the leaf SET as the transaction boundary and make every step past it
best-effort-and-logged, exactly like its neighbours — `bump_root` is a monotonic cache-invalidation
hint, and a stale root version is self-healing on the next write, so it does not deserve to fail a
committed write. If the root bump must be reliable, it belongs *in the same Lua script* as the leaf
SET, not after it.

---

## F4 — The commit is not a CAS, and the fence that guards it is fail-open

**SEVERITY: MEDIUM** · **REASONED** (call paths traced; the fail-open matrix was reproduced by the
lock/catalog sub-audit)

**Where**
- `supertable/redis_catalog.py:128-153` — `_LUA_LEAF_PAYLOAD_CAS_SET` takes no expected version; it reads the doc, computes `new_version = old_version + 1`, then `redis.call('SET', key, new_val)` unconditionally. Same for `_LUA_LEAF_CAS_SET` at `109-126`.
- `supertable/data_writer.py:177` — `if still_held is False:` — only the exact singleton `False` aborts
- `supertable/data_writer.py:164-166` — a catalog without `verify_simple_lock` skips the check
- `supertable/data_writer.py:174-176` — any exception from `verify` is swallowed and the write proceeds
- `supertable/data_writer.py:152-156` — the acknowledged one-round-trip TOCTOU between fence and SET
- `supertable/data_writer.py:1139-1147` — `except Exception` around the commit falls back to `set_leaf_path_cas`

**What goes wrong.** Three separate weaknesses compound:

1. **"CAS" is a misnomer.** It is an atomic *version-incrementing blind SET*. The version always goes
   up, so a clobbered write is indistinguishable from a legitimate one. All mutual exclusion comes
   from the Redis lock — which is exactly why F1 is destructive rather than merely racy.
2. **The fence only fires on `False`.** `None`, `0`, `""`, a missing method, or any raised exception
   all fall through to the commit. With the real `RedisCatalog` the return is a genuine `bool`
   (`redis_catalog.py:332-340` → `redis_lock.py:146-168`, which is itself correctly fail-closed on
   `RedisError`), so the live path is currently safe. But `FileLocking` has **no `is_held` method at
   all** (`locking/file_lock.py` exposes `acquire/release/extend/who`) despite the docstring at
   `file_lock.py:5-6,33-40` promising API interchangeability — a `FileLocking`-backed catalog raises
   `AttributeError` inside `verify`, which `data_writer.py:174-176` swallows, and **the fence silently
   disappears**.
3. **The commit's `except Exception` is far wider than its stated purpose.** The declared-safe fallback
   trigger is `AttributeError` for old catalogs (`redis_catalog.py:562-564`), but the writer catches
   everything. If the Lua applied server-side and only the *reply* was lost (Sentinel's 0.5 s
   `socket_timeout`), the fallback issues a second blind SET: the version is bumped twice and the leaf
   is left **without a payload**, forcing every reader back to storage.

**Why existing guards do not catch it.** The fence is the only guard, and it is written to be
maximally tolerant of test doubles — which is precisely what makes it fail-open in production shapes
it was not written for.

**FIX PROPOSAL.** (a) Make the fence fail-closed: abort unless the check returned a definite `True`,
and treat a missing method or a raised exception as "cannot prove ownership → do not publish".
(b) Fold the ownership test into the commit script — one Lua that checks the lock token *and* SETs the
leaf — which closes the TOCTOU and makes the misnomer honest. (c) Narrow the commit's `except` to
`AttributeError`/`TypeError` so a network error cannot silently downgrade the leaf to path-only.

---

## F5 — The artifact caches leak one entry per table **hour**, not per table

**SEVERITY: MEDIUM** · **PROVEN** (`repro_cache_hour.py`)

**Where**
- `supertable/processing.py:2598-2601` — `_PathKeyedFrameCache._key` is `os.path.dirname(path)`
- `supertable/processing.py:2578-2587` — the class contract: *"Process-wide LRU of each table's latest artifact frame (one per table)"*; caps are named `SUPERTABLE_STATS_CACHE_MAX_TABLES` / `..._TOMBSTONE_CACHE_MAX_TABLES`, default 64 (`config/settings.py:323,367`)
- `supertable/processing.py:1598-1616` — `_partitioned_new_path` now writes artifacts under `year=/month=/day=/hour=` via `utils/helper.py:37-58`
- `supertable/processing.py:2726-2735` — `_parts_cache_key` inherits the same `dirname` basis

**What goes wrong.** The cache's "one entry per table" invariant relied on every version of a table's
artifact living in one directory. The hour-partitioning change broke that: the directory now rotates
every hour, so each hour mints a **new cache key** and the previous hour's entry is never read again
but is never evicted either — it sits in the LRU holding a full stats DataFrame.

```
_STATS_CACHE    : 9 entries for ONE table (cap=64)
    key=hour=00  rows=2
    ...
    key=hour=08  rows=2
```

Two consequences. (1) **Memory**: a long-lived writer retains up to 64 stale copies of a table's stats
frame (one row per file × row-group × column — megabytes for a wide, many-file table). (2) **Cache
starvation**: a single busy table fills all 64 slots within 64 hours and evicts every *other* table's
cache, so a multi-table writer process silently loses the optimisation the cache exists for.

**Why existing guards do not catch it.** The exact-path check in `get()` (`processing.py:2616`) makes
hits correct, so nothing is wrong-answer; the cap is enforced, so nothing is unbounded. The defect is
that the cap now counts the wrong thing, and no test asserts the entry count for a single table.

**FIX PROPOSAL.** Key the cache on the table's *stable artifact root* (the directory above the
`year=.../hour=...` partition), not on `dirname` of the leaf path. Then one table is one entry again,
the previous hour's frame is replaced rather than retained, and the `MAX_TABLES` cap means what its
name says. `_parts_cache_key` should derive its prefix the same way.

---

## F6 — A compacting write re-downloads every freshly written file **twice**, in full, to read its footer

**SEVERITY: MEDIUM** · **PROVEN** (`io_probe2.py`)

**Where**
- `supertable/data_writer.py:263-266` — `_build_compact_model_df` does `storage.read_bytes(first_path)` per new resource
- `supertable/processing.py:1891-1911` — `_read_footer_metadata` does `_safe_exists` (HEAD) **then** `read_bytes` (full object GET)
- `supertable/data_writer.py:1030-1036` — `extract_stats_rows(..., footer_md_cache=footer_md_cache)`
- `supertable/data_writer.py:718,732` — `footer_md_cache` is populated only by the incoming-data branch
- `supertable/processing.py:594-601`, `608-615`, `2856-2863` — every compaction write path calls `write_parquet_and_collect_resources` **without** `footer_md_out`

**What goes wrong.** `_write_single_parquet_file` already parses the footer from the exact bytes it
uploaded (`processing.py:761-765`) — but only when `footer_md_out` is passed, which compaction never
does. So each compacted output file is fetched again by `extract_stats_rows` and a **third** time by
`_build_compact_model_df`, each a whole-object `read_bytes`, to recover a footer that was in memory
moments earlier. Per-write storage trace, isolating the compacting write:

```
write  6: files=7  calls={'write_bytes': 2, 'size': 1} bytes_read=0
write  7: files=1  calls={'write_bytes': 3, 'size': 2, 'exists': 9,
                          'read_parquet': 8, 'read_bytes': 2}
          <== COMPACTING WRITE | read_bytes on files the snapshot NOW lists: {'...ecee': 2}
```

At the default `MAX_MEMORY_CHUNK_SIZE = 16 MiB` that is ~32 MiB of redundant object-store GETs per
compaction output chunk, plus one HEAD each, on the critical path **while the table lock is held**.
The comment at `data_writer.py:258-262` claims this is "footer only … no network round-trip", which is
true of the parse and false of the fetch.

**Why existing guards do not catch it.** The footer-cache mechanism exists and is correct; it is
simply not wired to the compaction writers. Nothing measures per-write byte counts, so the cost is
invisible.

**FIX PROPOSAL.** Thread `footer_md_out` through `compact_resources` and `compact_tombstones` into
`write_parquet_and_collect_resources`, so every file this write produces contributes its footer to the
shared cache. Then `extract_stats_rows` is a pure cache hit and `_build_compact_model_df` can take the
schema from the same `FileMetaData` instead of re-fetching — the right shape is *one cache, populated
at the single place where the bytes exist, consumed by both readers*.

---

## F7 — Table deletion steals a live writer's lock and wipes storage without one

**SEVERITY: MEDIUM** · **PROVEN** (key-glob match reproduced by the lock/catalog sub-audit)

**Where**
- `supertable/redis_catalog.py:1164-1170` — `delete_simple_table` does a blind `self.r.delete(*keys)` on `RK.lock_leaf(...)`, bypassing the token
- `supertable/redis_keys.py:434-439` — `super_table_pattern` = `supertable:{org}:lakes:{sup}:*`, which `delete_super_table`'s scan-delete uses and which **matches `lock:leaf:doc:*`**
- `supertable/simple_table.py:215-230` — `SimpleTable.delete()` removes the storage folder first, taking **no lock at all**

**What goes wrong.** `release_simple_lock` is correctly a token-checked Lua compare-and-delete
(`locking/redis_lock.py:46-55`), so writer A can never release writer B's lock. But the two catalog
delete paths sidestep that entirely: they `DEL` the lock key outright. A `delete()` concurrent with a
`write()` deletes the writer's data folder out from under it and then drops its lock, after which a
third writer can acquire the same lock while the first is still running.

**Why existing guards do not catch it.** The pre-commit fence turns the worst case into a loud
`LockLostError` rather than a silent clobber, which is why this is MEDIUM and not HIGH. But the fence
is one round-trip wide (F4) and, on the `delete_super_table` glob path, deletion also removes the
`rowid_seq` counter (`redis_keys.py:518-529` matches the pattern) — see F9.

**FIX PROPOSAL.** Deletion is a mutation and should take the same per-table lock as `write`/`compact`,
hold it across the storage wipe, and only then delete the Redis keys — releasing its own lock by
token. The scan-delete pattern should explicitly exclude the `lock:` namespace so a glob can never
evict a live lease.

---

## F8 — Every data-file write pays an extra `size()` HEAD it does not need

**SEVERITY: LOW** · **PROVEN** (`io_probe2.py` — `size: 1` on every plain write, `size: 2` on a compacting one)

**Where**
- `supertable/processing.py:812-823` — `_write_single_parquet_file` always calls `_get_storage().size(new_parquet_path)` after the upload
- `supertable/processing.py:1079-1090` — the sibling `_write_df_parquet` already solved this with a `wrote_exact_bytes` flag and returns `len(data)`

**What goes wrong.** On the `write_bytes` backend (every backend in the tree) the uploaded object *is*
`data`, so `len(data)` is the size. The extra `size()` is a pointless object-store HEAD per data file —
once per ordinary write, once more per compaction output chunk, all inside the lock.

**FIX PROPOSAL.** Lift the `wrote_exact_bytes` pattern from `_write_df_parquet` into
`_write_single_parquet_file`. They are the same function shape and should share the same contract:
*if we PUT exactly these bytes, we already know the size.*

---

## F9 — `rowid_high_watermark` is recorded but never used to defend the allocator

**SEVERITY: LOW (but a latent correctness cliff)** · **REASONED**

**Where**
- `supertable/redis_catalog.py:508-520` — `reserve_rowids` is a bare `INCRBY` on `supertable:{org}:lakes:{sup}:meta:rowid_seq:doc:{simple}`, a plain string key with **no TTL and no persistence contract**
- `supertable/data_writer.py:443` / `1101-1105` — the writer reserves from that counter and separately records `rowid_high_watermark` into the snapshot
- `supertable/odata/row_identity.py` — the watermark's only consumer is a read-side verdict; grep confirms nothing feeds it back into allocation

**What goes wrong.** Table-uniqueness of `__rowid__` is the load-bearing invariant of the whole
merge-on-read design: the DV dedups on `__rowid__` alone (`processing.py:1691,1693,2765`), the read
path anti-joins on it, and `reclaim_fully_dead_files` counts per-file rowids against physical row
counts (`processing.py:1744-1756`). If the counter ever regresses — Redis `maxmemory-policy` set to an
`allkeys-*` eviction policy, a restart without persistence, or `delete_super_table` called directly
against surviving storage (the glob at `redis_keys.py:434-439` matches `rowid_seq`) — new rows reuse
live ids. The DV then collapses two distinct rows into one entry and `compact_tombstones` drains only
one of the two files, resurrecting the other on read.

The snapshot already carries the number that would prevent this, and the writer already holds it.

**Why existing guards do not catch it.** `verify_stable_identity` only fires when
`live_rows > watermark`, and the watermark carries forward monotonically, so a reset counter producing
*duplicate* ids within the existing range would not trip it. Nothing checks the counter against the
snapshot at reservation time.

**FIX PROPOSAL.** Seed/repair the counter from the snapshot at reservation: reserve against
`max(redis_counter, snapshot_watermark)` — a single Lua `SET`-if-lower before the `INCRBY`. This makes
the invariant self-healing from data the writer already reads, and turns a silent corruption into a
no-op. Separately, document `noeviction` as a hard requirement for the Redis instance.

---

## F10 — Deleting every row leaves a snapshot with zero resources, which the read path rejects

**SEVERITY: LOW** · **PROVEN** (`p3.py`, and hit repeatedly by the fuzzer)

**Where**
- `supertable/data_writer.py:823-845` — `reclaim_fully_dead_files` sunsets every fully-dead file; after a delete-all that is *all* of them, and `survivors.height == 0` drops the DV too (`processing.py:1764-1765`)
- Result: `resources: []`, and a subsequent `SELECT` fails

```
STATUS <Status.ERROR: 'error'> MSG 'No parquet files found for one or more selected tables.'
```

**What goes wrong.** A logically-empty table is a normal state (delete-all, or a freshly bootstrapped
table), but it is indistinguishable from a broken one at the read boundary: the query errors instead
of returning an empty result set. The write path is arguably correct here — reclaiming the files is
the right thing — so the mismatch is a contract gap between the two halves.

**FIX PROPOSAL.** The read path should treat "zero resources" as an empty table and return a zero-row
frame with the snapshot's schema, reserving the error for "resources listed but not found". Noted here
because the write path is what produces the state.

---

## F11 — Pure-append writes do O(files) overlap work whose result is never read

**SEVERITY: LOW** · **REASONED**

**Where**
- `supertable/data_writer.py:493-497` — `find_overlapping_files` is called unconditionally
- `supertable/processing.py:410-421` — with no `overwrite_columns` it builds a set of every small file and then runs `prune_not_overlapping_files_by_threshold` over it
- The only two consumers, `prune_overlapping_files_by_stats` (`data_writer.py:517`) and `resolve_overwrite_writes` (`data_writer.py:564`), are both gated on `if overwrite_columns:` — and `resolve_overwrite_writes` itself returns immediately when `overwrite_columns` is falsy (`processing.py:1531-1532`)

The append path also uses a different smallness threshold (`file_size < max_mem`, `processing.py:416`)
than the rest of the compaction machinery (`0.75 * max_mem`, `processing.py:336-338`), so even if it
were consumed it would disagree with the gate.

**FIX PROPOSAL.** Skip the call entirely when `overwrite_columns` is empty. The dead branch is also
actively misleading — it implies appends participate in compaction selection, which they do not
(that is `should_compact_small_files` at `data_writer.py:862`).

---

## F12 — Compaction output is sorted only by `__timestamp__`, degrading key zonemaps

**SEVERITY: LOW** · **REASONED**

**Where**
- `supertable/processing.py:716-722` — `sort_cols = ["__timestamp__"] + overwrite_columns`
- `supertable/processing.py:594-601`, `608-615`, `2856-2863` — every compaction writer passes `overwrite_columns=[]`

**What goes wrong.** A fresh write sorts by `__timestamp__` (a per-write constant, so effectively a
no-op) and then by the overwrite keys, giving tight per-row-group min/max on the keys — which is
exactly what the stats artifact records and what both the write-path pruner
(`prune_overlapping_files_by_stats`) and the read-path pruner (`prune_files_by_predicates`) rely on.
Compacted output has no key in its sort list, so after compaction the merged file's key ranges span
everything and can no longer prune. Compaction therefore *reduces* the pruning power of the very
artifact it rewrites.

**FIX PROPOSAL.** Carry a per-table "clustering key" (the overwrite columns are the obvious default,
and the writer already knows them at `data_writer.py:493`) into `compact_resources` /
`compact_tombstones` and use it as the compaction sort key. The invariant worth stating is: *a
compacted file should never be less prunable than the files it replaced.*

---

## What I checked and found CLEAN

These were probed adversarially and held up; the absence of a finding here is a positive result.

- **Snapshot/resource bookkeeping across both compaction phases.** `simple_table.update` does
  `(baseline − sunset) + new_resources` with no dedup (`simple_table.py:356-361`), which is exactly the
  shape that duplicates entries — but `write()` never splices Phase-A output into the baseline, and
  `compact()` deliberately hands `update` only Phase-B's brand-new files (`data_writer.py:1588-1590`)
  precisely because it does. Both are correct, and `test_compaction_duplicate_resource_entry.py` seals it.
- **Phase ordering (drain-then-merge).** Phase B is gated on `compaction_gate`, which is a strict
  subset of Phase A's `tombstone_threshold_hit or compaction_gate` (`data_writer.py:891,954`), so the DV
  is always drained before any file it references can be merged. I traced every path by which
  `dv_to_drain` can be `None` and each corresponds to a genuinely empty vector.
- **Concurrency.** 48 concurrent appends across 6 threads: zero lost rows, zero duplicate `__rowid__`,
  snapshot version exactly `n+1` (`conc.py`). Deleter threads racing a compactor: nothing resurrected,
  nothing lost. Five threads upserting the same three keys: exactly one row per key, no errors
  (`conc2.py`). The per-`(org, super, simple)` lock plus a real token-checked Lua release does its job
  in-process. (Cross-process was not testable — no reachable Redis in this environment.)
- **Deletion-vector accounting.** A differential fuzzer (`fuzz2.py`, 8 seeds × 30 mixed
  upsert/append/delete/delete-all/compact ops) with thresholds cranked down so auto-compaction, the
  tombstone-row drain and DV checkpointing fire on almost every write, asserting after *every*
  operation: live rows == oracle, no duplicate physical `__rowid__`, `snapshot.tombstone_rows` ==
  actual DV height, and `verify_stable_identity` stable. **Zero failures.** The delta-part model,
  checkpointing, `reclaim_fully_dead_files` and the `keep="first"` rowid dedup are all consistent.
- **Carry-forward `required=True` discipline.** The DV reads that must not silently truncate
  (`data_writer.py:651-654`, `1506-1510`, `processing.py:1686`, `2833-2835`) all pass `required=True`
  and `_read_parquet_safe` honours it correctly, including the `strict` existence probe
  (`processing.py:216-227`). Sealed by three characterization tests.
- **Lock heartbeat.** `ttl_s=30` is genuinely renewed by a daemon thread at half the TTL
  (`locking/redis_lock.py:133-138,233-278`) via token-checked Lua extend, so a long compaction does not
  drop the lock. `release` is a correct compare-and-delete. `acquire` returning falsy always raises
  (`data_writer.py:483-484`, `1461-1462`) — there is no path where a failed acquire proceeds.
- **`reserve_rowids`.** A single atomic `INCRBY` returning a disjoint contiguous block; concurrent
  writers cannot collide; errors propagate rather than returning a bogus id. It runs before the lock,
  which is correct (it needs none) and only burns ids on a failed write.
- **`_align_to_schema` / `concat_with_union` positional contract.** The `df.select` fix is in place
  (`processing.py:143-152`) with the zero-row defence; column order and dtype widening are consistent
  across `compact_resources`.
- **Hive partition-column leak.** Already fixed at the root — `LocalStorage.read_parquet` passes
  `partitioning=None` (`storage/local_storage.py:196-207`), so compaction no longer bakes
  `year/month/day` into file bodies. Sealed.
- **Stats artifact carry-forward.** `build_stats_file`'s `_conform_stats_schema` back-compat
  (`processing.py:1973-1988`) and the `removed_files` filter are correct; new-file stats and sunset
  removal stay in step with the resource list on both the write and compact paths.
- **Validation.** `write()` validates before reserving rowids (`data_writer.py:431`), rejects
  `newer_than` without `overwrite_columns` (`data_writer.py:1830-1831`), and always overwrites
  caller-supplied `__rowid__`/`__timestamp__`.
