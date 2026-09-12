# Adversarial audit — storage backends & Redis catalog / key scheme

Repo: `/home/kladnasoft/dev/dataisland/supertable` @ `01cbb65` (master, clean)
Date: 2026-09-11 · Investigation only, no library code changed.
Probes: `scratchpad/audit/probe_{storage,delete,drop,redis_keys,lua,orphans}.py`
(real Redis 8.2.1 on db 11/12, flushed before and after; boto3 stubs for S3).

---

## Findings at a glance

| # | Title | Sev | Status |
|---|---|---|---|
| 1 | `exists(<folder>)` guard makes DROP TABLE a silent no-op on every object backend | **CRITICAL** | PROVEN |
| 2 | `S3Storage._call` redirect retry re-sends an exhausted `BytesIO` → 0-byte parquet, reported as success | **HIGH** | PROVEN |
| 3 | `StagingArea.delete()` calls `storage.delete_recursive()`, which no backend implements | **HIGH** | PROVEN |
| 4 | `_LUA_ROOT_BUMP` lossily re-encodes the whole root doc on every write (`[]`→`{}`, int64→float) | **HIGH** | PROVEN |
| 5 | `list_files()` returns base-prefixed paths that no other method accepts | **MEDIUM-HIGH** | PROVEN |
| 6 | S3 `delete()` discards `delete_objects` `Errors` → silent partial delete reported as success | **MEDIUM-HIGH** | PROVEN |
| 7 | `delete_simple_table` leaves 6 orphan keys incl. a stale schema and a stale `table_names` member | **MEDIUM** | PROVEN |
| 8 | Redis-key guard test is blind to everything except literal f-strings (and to 7 constructors) | **MEDIUM** | PROVEN |
| 9 | RBAC Lua flips `roles` from JSON array to JSON object; builds undeclared keys by Lua concat | **MEDIUM** | PROVEN |
| 10 | `query:*` is an undocumented 4th position-2 scope; `query_job_pattern` over-matches the index | **MEDIUM** | PROVEN |
| 11 | Streaming job doc TTL is never refreshed; `reap()` can delete a live job's spilled chunks | **MEDIUM** | REASONED |
| 12 | Quality `config:__global__` collides with a table literally named `__global__` | **LOW-MEDIUM** | PROVEN |
| 13 | Four backends, four different `delete()` failure semantics | **LOW-MEDIUM** | PROVEN |
| 14 | N+1 Redis round-trips in `list_shares` / `list_linked_shares` / `_FallbackCatalog` / `reap` | **LOW** | PROVEN |
| 15 | Scheme nits: `deletion-intent` hyphen, `schema:` depth, unused `os` import + wrong factory docstring | **LOW** | PROVEN |

---

## 1. `exists(<folder>)` guard makes DROP TABLE a silent no-op on object stores — CRITICAL — PROVEN

**Where**
- `supertable/simple_table.py:216-223` (drop one simple table)
- `supertable/super_table.py:126-131` (drop a whole supertable)
- `supertable/staging_area.py:277-281` (drop a staging area)

All three have the identical shape:

```
if self.storage.exists(simple_table_folder):
    self.storage.delete(simple_table_folder)
```

**What goes wrong.** `exists()` is `head_object` / `stat_object` / `get_blob_properties` /
`blob.exists()` on S3, MinIO, Azure and GCS respectively
(`s3_storage.py:477`, `minio_storage.py:240`, `azure_storage.py:262`, `gcp_storage.py:175`).
Object stores have no directories, so `exists("acme/sales/abc123/orders")` is **always False**
for a prefix — there is no object at that exact key. `LocalStorage.exists()` is
`os.path.exists()` (`local_storage.py:110`) and returns **True** for a directory.

Concrete scenario on the production backend (repo `.env` has `STORAGE_TYPE=MINIO`):
`SimpleTable.delete()` → `exists(folder)` is False → the storage wipe is skipped entirely →
`catalog.delete_simple_table(...)` removes the Redis leaf pointer → the table disappears from
every listing while **100 % of its parquet files, snapshots and stats stay in the bucket
forever**, now unreachable (no catalog entry points at them, so GC/compaction never sees
them either). No exception, no log line. `SuperTable.delete()` is worse: its comment says
*"Delete storage first; if this fails (other than missing), do not remove Redis meta"* —
the intended safety ordering is inverted in practice because the storage step is skipped, not failed.

Probe output (`probe_drop.py`):
```
objects in bucket under 'acme/sales/abc123/orders/' : 3
storage.exists('acme/sales/abc123/orders')          : False
objects deleted by the guarded drop : []
objects STILL in bucket             : [3 keys]
un-guarded delete() on the same prefix -> deleted 3 objects
LOCAL control: exists(dir)=True, dir gone=True
```

**Why guards miss it.** `supertable/tests/test_supertable_all.py:880-886` patches
`supertable.super_table.get_storage` with a `MagicMock()`. `mock_storage.exists(...)` returns a
truthy `Mock`, so the branch is taken and `mock_storage.delete.assert_called_once()` passes.
The suite has never exercised a real object-store `exists()` against a prefix. There is no
integration test with a real bucket, and `supertable/storage/tests/test_storage.py` tests
`exists()` on files only (`test_exists_true_for_directory` is in `TestLocalStorage`, so it
encodes the *local* semantics as the contract without asserting parity).

**Fix proposal.** `delete()` in every backend already handles the "exact key missing → treat as
prefix" case and raises `FileNotFoundError` when there is genuinely nothing there
(`s3_storage.py:516-552`, `minio_storage.py:270-291`, `azure_storage.py:296-311`,
`gcp_storage.py:244-264`). So the guard is redundant: drop it and rely on the
`except FileNotFoundError: pass` that already wraps all three call sites. Longer term, either
(a) add an explicit `exists_prefix(path) -> bool` to `StorageInterface` and make the directory
question unambiguous, or (b) document `exists()` as *object-only* in the interface docstring and
add a parity test asserting `exists(<dir>) is False` for **all** backends including Local —
whichever way it is settled, the two must agree, because callers cannot tell which backend they
hold.

---

## 2. `S3Storage._call` retry re-sends an exhausted `BytesIO` → 0-byte object, success returned — HIGH — PROVEN

**Where** `supertable/storage/s3_storage.py:303-372` (`_call`) used by `write_parquet`
(`s3_storage.py:588-599`, `Body=buf` where `buf` is an `io.BytesIO`).

**What goes wrong.** `_call` retries once when S3 answers `PermanentRedirect` / `301` /
`Redirect` / `AuthorizationHeaderMalformed` / `IllegalLocationConstraintException`. botocore has
already streamed the request body by the time the server returns that error, so the `BytesIO` is
at EOF. The retry passes the *same* object; botocore reads 0 bytes, sends
`Content-Length: 0`, and S3 stores an empty object. `_call` returns the successful response and
`write_parquet` returns normally.

Concrete scenario: a bucket in `eu-central-1` reached through the global
`s3.amazonaws.com` endpoint — exactly the case `_is_aws_global_endpoint`
(`s3_storage.py:233-244`) and the `_probe_bucket_region` fallback were written for. The first
data-file PUT of a fresh process redirects, gets retried, and lands a **0-byte
`.parquet` in the bucket**. The writer then records that path in the snapshot and commits the
Redis leaf pointer, so the corruption is committed to the catalog. Every later read of that file
fails with `RuntimeError: Failed to read Parquet` — after the write reported success.

Probe output (`probe_storage.py`, stub client raising `PermanentRedirect` once):
```
put_object attempts      : 2
bytes sent per attempt   : [6049, 0]
write_parquet raised?    : no
```
Control: `write_bytes()` passes `bytes` (re-readable) → `[512, 512]`, unaffected.
`write_json` (`bytes`) and `copy_object` (no body) are likewise safe. `write_parquet` is the
only `_call` site with a file-like body.

**Why guards miss it.** `TestS3Storage` in `supertable/storage/tests/test_storage.py` uses a fake
client that never raises a redirect on `put_object`; the redirect paths are only exercised
against `head_bucket`/`list_objects_v2`, which carry no body. No test asserts the byte count of
the retried request.

**Fix proposal.** Make the retry idempotent at the boundary rather than at the call site: have
`_call` detect a seekable body in `kwargs` and `seek(0)` before re-issuing (and refuse to retry
a non-seekable stream), or — simpler and it removes the whole class — materialise the parquet
buffer to `bytes` in `write_parquet` (`buf.getvalue()`) as the Azure and GCS backends already do
(`azure_storage.py:345`, `gcp_storage.py:310`). The MinIO backend passes a `BytesIO` too but has
no retry wrapper, so it is not affected today; a `getvalue()` there would future-proof it.

---

## 3. `StagingArea.delete()` calls a method no backend implements — HIGH — PROVEN

**Where** `supertable/staging_area.py:279` — `self.storage.delete_recursive(self.stage_dir)`.

**What goes wrong.** `delete_recursive` exists nowhere else in the repo: not on
`StorageInterface`, not on `LocalStorage`, `S3Storage`, `MinioStorage`, `AzureBlobStorage` or
`GCSStorage` (`grep -rn delete_recursive supertable/` returns exactly that one call site).
Consequently `StagingArea.delete()` is broken on **both** sides of the backend split, in two
different ways:

- **LOCAL** — `exists(stage_dir)` is True → `AttributeError: 'LocalStorage' object has no
  attribute 'delete_recursive'`. The exception propagates out of `_op()`, so the staging folder,
  its files index and its Redis keys are all left behind.
- **Object stores** — `exists(stage_dir)` is False (finding #1), so the line is never reached.
  The staged files are silently orphaned; `delete_staging_meta` then removes the Redis record,
  so nothing points at them any more.

Probe output (`probe_drop.py`): `hasattr(..., 'delete_recursive') = False` for all three classes;
`LocalStorage().delete_recursive(...)` → `AttributeError`.

**Why guards miss it.** Nothing in the suite calls `StagingArea.delete()` against a storage
object that is not a `MagicMock` — a mock answers `delete_recursive` happily. The name is also
outside the `StorageInterface` ABC, so the abstract-method check cannot catch it.

**Fix proposal.** `delete()` already *is* the recursive-prefix delete on every backend, so the
call should be `storage.delete(self.stage_dir)` wrapped in `except FileNotFoundError: pass`.
If an explicit recursive variant is wanted (GCS has a private `delete_prefix` at
`gcp_storage.py:266`), promote it to `StorageInterface` with a default implementation so the ABC
enforces it everywhere. Either way, add one non-mocked staging drop test.

---

## 4. `_LUA_ROOT_BUMP` lossily re-encodes the whole root document on every write — HIGH — PROVEN

**Where** `supertable/redis_catalog.py:155-183`, called by `bump_root`
(`redis_catalog.py:456-461`) on every supertable write.

**What goes wrong.** The script `cjson.decode`s the entire `meta:root` document, mutates two
fields and `cjson.encode`s the whole thing back. The comment at lines 175-178 explains this is
deliberate ("Merge, never replace") to preserve the flags written by `update_root_flags`
(`read_only`, `cloned_from`, `clone_type`, `clone_ts`, `replica_tables`). But the round trip
through Lua's cjson is not value-preserving:

1. **Empty JSON arrays become empty JSON objects.** Lua has one table type; cjson cannot tell
   `[]` from `{}` and emits `{}`.
2. **Integers above 2^53 become scientific-notation floats.** cjson encodes Lua numbers with
   `%.14g`.

Probe output (`probe_lua.py`, real Redis 8.2.1):
```
version         1                    -> 2
ts              1757620000000        -> 1757620999000
clone_ts        1757620000123        -> 1757620000123
nanos           1757620000123456789  -> 1.7576200001235e+18   <-- CHANGED
replica_tables  []                   -> {}                    <-- CHANGED
read_only       True                 -> True
cloned_from     'src'                -> 'src'
```

Impact today is contained but real: `_resolve_replica_info` (`redis_catalog.py:477-478`) happens
to be defensive — `isinstance(tables, list) and tables` treats `{}` and `[]` identically — so the
replica path survives. What does not survive is (a) any consumer that reads `meta:root` over the
API and expects `replica_tables` to be an array (`.length`/iteration on `{}` differs), and
(b) any future or platform-written flag holding an id/timestamp wider than 2^53 — it is silently
rounded and its JSON type changes from int to float, on the very next write to the table.
13-digit epoch-ms values are safe; nanosecond timestamps and snowflake ids are not.

**Why guards miss it.** `supertable/tests/test_defect_fixes.py:79` registers `_LUA_ROOT_BUMP` and
asserts the flag-preservation behaviour, but only with scalar flags. No test puts an empty list or
a large integer in the root document.

**Fix proposal.** Stop round-tripping the document. Two shapes both work: (a) keep the version
counter out of the JSON blob entirely — `HINCRBY` a `version` field on a hash, leaving the flags
untouched, which is what the RBAC meta scripts already do (`_LUA_RBAC_BUMP_META`,
`redis_catalog.py:187-193`); or (b) if the STRING+JSON shape must stay, patch the two fields
textually rather than decoding — or set `cjson.encode_number_precision(17)` and carry an explicit
array marker. (a) is cleaner and removes the class of bug rather than the instance.

---

## 5. `list_files()` returns base-prefixed paths that no other method accepts — MEDIUM-HIGH — PROVEN

**Where** `s3_storage.py:498-511`, `minio_storage.py:259-266`, `azure_storage.py:280-291`,
`gcp_storage.py:197-239`. All four do `path = self._with_base(path)` and then return
`[path + child]` — i.e. **output includes `base_prefix`, input must not**.

**What goes wrong.** `_with_base` (`storage_interface.py:14-19`) prepends
`settings.SUPERTABLE_PREFIX` on entry to every other method. Feeding `list_files` output into
`read_bytes` / `exists` / `read_parquet` / `list_files` therefore applies the prefix twice.

Probe output (`probe_storage.py`, `base_prefix="tenantA"`):
```
list_files('lake/t/year=2026','*.parquet') -> ['tenantA/lake/t/year=2026/f1.parquet', ...]
exists(listed[0])                          -> False
key head_object actually saw               -> 'tenantA/tenantA/lake/t/year=2026/f1.parquet'
```

Two real call sites are affected:
- `supertable/audit/writer_parquet.py:270-276` — `list_partitions()` chains `list_files` into
  `list_files` three levels deep. Probe: level 1 returns one entry, level 2 returns `[]`.
  Audit partition discovery yields nothing at all under a non-empty prefix.
- `supertable/audit/writer_parquet.py:249-258` + `:311` — `read_batch_events` feeds
  `list_partition_files()` output straight into `storage.read_bytes(file_path)`. Every read raises
  `FileNotFoundError`, swallowed by the per-file `except Exception` at `:344`, so audit-chain
  verification silently reports **zero events** instead of failing. (`list_partitions` is likewise
  wrapped in `except Exception: return []` at `:278-280`.)

`mirroring/mirror_parquet.py:88-92` and `mirror_delta.py:389-393` are immune — they take only the
basename of each entry.

**Why guards miss it.** `grep -c base_prefix supertable/storage/tests/test_storage.py` → **0**.
`base_prefix` is a fully documented feature (`docs/02_configuration.md:30`,
`docs/04_storage.md:229/261/292/332`) with zero coverage in the storage suite. The codebase
already knows about this trap in one place: `supertable/tests/test_spark_file_resolution.py:115-117`
says *"storage.presign() re-applies base_prefix, so the resolver must strip it"* — the same hazard,
worked around ad hoc there and unhandled here.

**Fix proposal.** Pick one convention and enforce it with a parity test across all four backends.
The lower-churn option is to make `list_files` return caller-space paths (strip `base_prefix`
before returning), which makes the output composable with every other method and is what the
`StorageInterface` docstring implies ("files/objects found in `path`"). The alternative — document
list output as absolute keys and add a `_strip_base` for callers — leaks the prefix into caller
code, which is precisely what `_with_base` exists to prevent. Either way, add
`test_list_files_output_round_trips_through_exists` parameterised over backends and prefixes.

---

## 6. S3 `delete()` discards `delete_objects` `Errors` — MEDIUM-HIGH — PROVEN

**Where** `supertable/storage/s3_storage.py:538-549`. Both `delete_objects` calls use
`Delete={"Objects": batch, "Quiet": True}` and **ignore the response entirely**.

**What goes wrong.** `Quiet=True` suppresses only the `Deleted` list; S3 still returns an
`Errors` array for objects it refused (`AccessDenied`, object-lock / legal hold, replication
governance, a transient 503 on one key). `S3Storage.delete()` returns `None` — success — with
objects still in the bucket. Callers treat that as "storage is gone, now drop the Redis meta"
(`simple_table.py:216-226`, `super_table.py:126-136`), so the catalog entry is removed while the
data survives, permanently orphaned.

Probe output (`probe_delete.py`): 3 objects under the prefix, 1 refused with `AccessDenied` →
`delete()` raised `None`, 2 deleted, 1 left behind, no signal to the caller.

MinIO does the opposite and correctly: `minio_storage.py:287-291` collects the `remove_objects`
error stream and raises `RuntimeError`.

**Why guards miss it.** The fake client in `TestS3Storage` returns `{}` from `delete_objects`;
no test injects an `Errors` payload.

**Fix proposal.** Capture the `delete_objects` response, accumulate `Errors` across batches, and
raise a single `RuntimeError` listing the failed keys once the loop finishes — matching the MinIO
behaviour, and deleting as much as possible before reporting (a fail-fast raise mid-batch would
leave a *less* predictable half-state than the current silent one).

---

## 7. `delete_simple_table` leaves six orphan keys — MEDIUM — PROVEN

**Where** `supertable/redis_catalog.py:1157-1174`. It deletes exactly
`meta:leaf:doc:{simple}` and `lock:leaf:doc:{simple}`.

**What goes wrong.** The SDK writes six other per-table keys that are never removed:

Probe output (`probe_orphans.py`, real Redis):
```
REMOVED  supertable:acme:lakes:demo:lock:leaf:doc:orders
REMOVED  supertable:acme:lakes:demo:meta:leaf:doc:orders
ORPHAN   supertable:acme:lakes:demo:meta:rowid_seq:doc:orders
ORPHAN   supertable:acme:lakes:demo:meta:table_config:doc:orders
ORPHAN   supertable:acme:lakes:demo:meta:table_names          (member 'orders' still present)
ORPHAN   supertable:acme:lakes:demo:quality:config:orders
ORPHAN   supertable:acme:lakes:demo:quality:latest:orders
ORPHAN   supertable:acme:lakes:demo:schema:doc:orders
```

`schema:doc:{simple}` and the `meta:table_names` membership are both written by
`data_writer.py:1167-1168` and have **no reader inside this repo** — they exist for the platform
layer (dataisland-core UI). So the observable damage lands outside the SDK: after a drop, the
platform still lists the table in `meta:table_names` and still serves its old column schema. Drop
`orders`, recreate it with a different schema, and the first UI read before the first write
returns the *previous* table's schema. `meta:rowid_seq` surviving is arguably desirable
(monotonic ids), but it is undocumented either way.

`delete_super_table` is complete — `super_table_pattern` (`supertable:{org}:lakes:{sup}:*`) sweeps
all of the above, verified in `probe_redis_keys.py` section D. Only the per-table path is partial.

**Why guards miss it.** `test_simple_table.py:438-481` asserts only that
`catalog.delete_simple_table` was *called*; the catalog is a mock, so what it deletes is never
checked. `test_supertable_all.py:712` likewise.

**Fix proposal.** Make `delete_simple_table` symmetric with the write path: one pipeline issuing
`DEL meta:leaf lock:leaf meta:rowid_seq meta:table_config schema:doc` +
`SREM meta:table_names {simple}` + a scoped `_delete_by_scan` for
`quality_prefix(org,sup) + "*:" + simple` (or have the quality module expose its own
`drop_table(table)`). Better still, derive the list from a single "per-simple-table key family"
helper in `redis_keys.py` so adding a new per-table key cannot silently skip the drop path again.

---

## 8. The Redis-key guard test is blind to almost every construction form — MEDIUM — PROVEN

**Where** `supertable/tests/test_redis_key_prefix.py:476-525`
(`test_no_raw_fstring_keys_outside_redis_keys`), regex:
`f["'](?:supertable|dataisland|monitor|spark|registry|audit|shares|lakes|_apps_):`

**What goes wrong.** The regex matches *only* a literal f-string whose first characters are one of
nine hard-coded segment names. Probe output (`probe_redis_keys.py` section G):

```
[CAUGHT] f-string literal            f"supertable:{org}:lakes:{sup}:meta:root"
[MISSED] .format()                   "supertable:{}:lakes:{}:meta:root".format(o, s)
[MISSED] concatenation               "supertable:" + org + ":lakes:" + sup
[MISSED] prefix + tail f-string      RK.quality_prefix(org, sup) + f"pending:{table}"
[MISSED] str.join                    ":".join(["supertable", org, "lakes", sup])
[MISSED] %-format                    'supertable:%s:lakes:%s' % (org, sup)
[MISSED] prefix-constant f-string    f"{SUPERTABLE_PREFIX}:{org}:lakes:{sup}"
[MISSED] Lua concatenation           local k = 'supertable:'..org..':lakes'
```

Two of the misses are live in the tree, not hypothetical:
- `supertable/quality/scheduler.py:744`, `:748`, `:752` — `RK.quality_prefix(org, sup) + f"pending:{table}"`
  (and `running:`/`cooldown:`). `{table}` is interpolated with **no `_safe()` validation**.
- `supertable/quality/config.py:51` — `RK.quality_prefix(org, sup) + ":".join(parts)`, with
  `parts` reaching `{table}` and `{column}` unvalidated (`config.py:335-361`).
- `supertable/redis_catalog.py:215` — key built inside Lua with `..` (see finding #9).

The module docstring's invariant 7 ("The only file … that may construct keys … is this one. The
regression test … enforces it") therefore overstates what is enforced.

A second, independent gap: seven public constructors are **absent from the test's
`_all_helpers()` table**, so their shape is unasserted —
`meta_namespace_deletion_intent`, `query_job_doc`, `query_job_chunks`, `query_job_cancel`,
`query_job_index`, `query_job_pattern`, `query_job_subkey_pattern`
(`probe_redis_keys.py` section F).

**Why the gap matters rather than being cosmetic.** The tail segments assembled outside
`redis_keys.py` skip `_safe()`, which is the only thing preventing a `:` in a user-supplied
identifier from escaping its namespace. Today table names cannot contain `:` because
`meta_leaf()` would have rejected them at creation — so the escape is not reachable *through the
SDK's own creation path*. It is reachable for any caller (including dataisland-core) that passes
a table name obtained from somewhere other than the catalog.

**Fix proposal.** Three changes, cheap and independent:
1. Broaden the regex to a **segment-anywhere** match — flag any string literal containing
   `supertable:` / `dataisland:` regardless of quoting style, plus `.format(`/`%`/`+`
   assembly onto an `RK.*_prefix(...)` result, plus `..` concatenation inside the `_LUA_*` script
   bodies.
2. Move the six quality tail keys into `redis_keys.py` as real constructors
   (`quality_pending`, `quality_running`, `quality_cooldown`, `quality_config`,
   `quality_latest`, `quality_anomalies`) so they get `_safe()` for free, and keep
   `quality_prefix` only for SCAN patterns.
3. Make `_all_helpers()` self-checking: enumerate `redis_keys` public functions reflectively and
   assert every one appears in the table, so a new constructor cannot be added untested.

---

## 9. RBAC Lua: `roles` array→object flip, and keys built by Lua concatenation — MEDIUM — PROVEN

**Where** `supertable/redis_catalog.py:230` and `:270` (`cjson.encode(new_roles)`), and
`:215` (`local ukey = user_doc_key_prefix .. uid`).

**(a) `roles` changes JSON type when the last role is removed.** Probe (`probe_lua.py`, real
Redis):
```
roles before : ["r1"]  -> list
roles after  : {}      -> dict     (via _LUA_RBAC_REMOVE_ROLE_FROM_USER)
roles after  : {}                  (via _LUA_RBAC_DELETE_ROLE)
control: removing 1 of 2 roles -> ["r2"]   (array preserved)
```
`redis_infra.py:209` and `redis_catalog.py:716` both `json.loads` this field and hand it to
callers; `rbac/user_manager.py:98,144` iterate it. Iterating `{}` yields nothing, same as `[]`,
and `_LUA_RBAC_ADD_ROLE_TO_USER` self-heals the shape on the next add (`#roles`=0 → index 1 →
array again). So there is no authorisation bypass. The damage is type instability escaping to
API consumers: a user document's `roles` is sometimes `[]`-shaped and sometimes `{}`-shaped, and
any JSON client doing `roles.length` / `roles.map(...)` breaks on the second form.

**(b) undeclared keys built in Lua.** `_LUA_RBAC_DELETE_ROLE` declares 6 KEYS but writes to one
additional key per member of the user index, constructed as
`user_doc_key_prefix .. uid`. Two consequences: the key never passes through
`redis_keys.py` (invisible to the guard test, finding #8), and accessing undeclared keys from a
script is unsupported on Redis Cluster — this script would be rejected or cross-slot the moment
the deployment moves off a single master. `rbac_user_doc_prefix` exists (`redis_keys.py:748-755`)
specifically to feed this concatenation, so the design is deliberate; the cluster-safety cost is
just not recorded anywhere.

**Why guards miss it.** `supertable/rbac/tests/test_rbac.py:273` stubs `register_script` with a
Python fake, so the real cjson behaviour is never executed. The only assertion about these keys
(`test_rbac.py:1790`) checks the prefix, not the KEYS declaration.

**Fix proposal.** For (a): emit `cjson.empty_array` when `#new_roles == 0`, or store roles as a
Redis SET member-per-role instead of a JSON string in a hash field — which also removes the
read-modify-write and the decode `pcall`s. For (b): either declare the user doc keys in KEYS
(the caller already has the user id list — it does `SMEMBERS` on the index before calling, at
`redis_catalog.py:811`), or add a one-line note in the script header that this script is
single-node only, so the constraint is at least visible.

---

## 10. `query:` is an undocumented 4th position-2 scope; `query_job_pattern` over-matches — MEDIUM — PROVEN

**Where** `supertable/redis_keys.py:136` (`QUERY_SCOPE`), `:645-676` (the six constructors).

**(a) Scheme documentation is wrong.** The module docstring's hierarchy (lines 16-90) lists only
`system:`, `lakes:` and `monitor:` at position 2. Design invariant 2 (line 96-100) states
*"Position 2 under `supertable:{org}:` is **always** a literal sentinel (`system` or `lakes`)"* —
which already omits `monitor`, and now also `query`. The guard test's own docstring (line 13-15)
says `system`, `lakes`, or `monitor`. There are in fact **four** position-2 literals. Nothing
enumerates them in one place, so nothing can assert the closed set.

**(b) `query_job_pattern` over-matches the index.** `supertable:{org}:query:job:*` matches
`supertable:{org}:query:job:index`, the SET used for listing and reaping. Probe
(`probe_redis_keys.py` section A, real Redis):
```
pattern : supertable:acme:query:job:*
MATCH supertable:acme:query:job:cancel:j1
MATCH supertable:acme:query:job:chunks:j1
MATCH supertable:acme:query:job:doc:j1
MATCH supertable:acme:query:job:index   <-- INDEX SET
```
The pattern currently has **zero callers**, so this is a loaded foot-gun, not an active bug: the
first caller that uses it the obvious way (scan + delete all job keys for an org) will wipe the
job index along with the jobs, and `reap()`/`list_jobs()` go blind.

The root cause is a departure from the rest of the scheme. Everywhere else, user input lives
behind a `doc:` token precisely so a literal sibling (`index`, `meta`) can never be confused with
it (redis_keys.py invariant 4, line 104-106). `query_job_doc` follows that rule; `query_job_chunks`
(`job:chunks:{id}`) and `query_job_cancel` (`job:cancel:{id}`) do not — they put the user id
directly after an attribute literal. Compare `meta:leaf:doc:{simple}` /
`meta:rowid_seq:doc:{simple}` / `meta:table_config:doc:{simple}`, which all nest under `doc:`.

**Fix proposal.** Rename to `job:chunks:doc:{id}` and `job:cancel:doc:{id}` (a
breaking key change, so it needs a release note — but these keys are TTL'd at 1 h, so a deploy
window covers the migration). Then `query_job_pattern` becomes
`supertable:{org}:query:job:*:doc:*`, which excludes `job:index` structurally. Separately: add
`QUERY_SCOPE` and `MONITOR_SCOPE` to the module docstring hierarchy, and export a
`POSITION_2_SCOPES` frozenset that the guard test asserts every constructor's third segment
belongs to.

---

## 11. Streaming job doc TTL never refreshed; `reap()` can delete a live job's chunks — MEDIUM — REASONED

**Where** `supertable/streaming/jobs.py:189-195` (create), `:205-211` (`update`, no `EXPIRE`),
`:229-235` (`append_chunk`, **does** refresh the chunks TTL), `:293-307` (`reap`).
Defaults: `SUPERTABLE_STREAM_JOB_TTL_SEC = 3600`, `SUPERTABLE_STREAM_DEADLINE_SEC = 3600`
(`config/settings.py:364,366`).

**What goes wrong.** Three TTLs that should agree do not:
- The job **doc** gets `EXPIRE ttl` once at creation. `update()` uses `HSET`, which does **not**
  reset a TTL, and nothing else refreshes it mid-run.
- The **chunks** list gets its TTL refreshed on *every* `append_chunk`, so it slides forward for
  as long as the producer is alive.
- The **index** SET has no TTL by design and is cleaned by `reap()`.

So a producer that outlives `SUPERTABLE_STREAM_JOB_TTL_SEC` loses its job document while its
chunk list is still being extended. This is not a corner case: `create()` explicitly supports
`deadline_ts = 0` meaning "no deadline" (`jobs.py:183-185` — *"an export may legitimately outlive
any fixed budget"*), and with the shipped defaults an ordinary deadline-bounded job expires its
doc at exactly the same second as its deadline.

Two consequences. (i) `runner.py:409-410` handles `rec is None` by returning silently
(*"expired or deleted out from under us"*), so a long export ends with a truncated result and no
error. (ii) `reap()` iterates the index, treats every job whose doc is missing as an orphan, and
calls `delete(..., storage=storage)` — which **deletes the spilled chunk files from object
storage** (`jobs.py:271-278`). Run the reaper while a >1 h export is in flight and it destroys
that export's already-written chunks from under the live producer. `reap()` cannot distinguish
"container died" from "doc TTL elapsed under a healthy producer", because those two states look
identical in Redis.

REASONED rather than PROVEN: reproducing it end-to-end needs a >1 h run or an overridden TTL plus
a live executor; the code path is unambiguous from reading, but I did not stand up the runner.

**Fix proposal.** Give the executor a heartbeat: refresh the doc TTL in the same pipeline as
`append_chunk` (one extra `EXPIRE`, no added round-trip), so doc and chunks expire together and a
live producer keeps both alive. Then make `reap()` require a *second* signal before destroying
storage — e.g. `owner` liveness, or a grace period keyed off `created_ts` — so an expired doc
alone is not sufficient grounds to delete spilled data. Also assert `JOB_TTL > DEADLINE` at
settings-build time (or derive one from the other); equal defaults guarantee the race.

---

## 12. Quality `config:__global__` collides with a table named `__global__` — LOW-MEDIUM — PROVEN

**Where** `supertable/quality/config.py:101,116` (`_key("config", "__global__")`) versus
`:126,137,145` (`_key("config", table)`).

**What goes wrong.** Both produce `supertable:{org}:lakes:{sup}:quality:config:{X}`.
`redis_keys._safe()` **accepts** `__global__` as a table name — the `_SAFE_SEGMENT` regex
explicitly allows double-underscore-wrapped names as the SDK-internal-table convention
(`redis_keys.py:187-189`), and only *single*-underscore sentinels are rejected. Probe
(`probe_redis_keys.py` section E):
```
global config key                    : supertable:acme:lakes:demo:quality:config:__global__
per-table config, table='__global__' : supertable:acme:lakes:demo:quality:config:__global__
COLLIDE: True      _safe('simple','__global__') -> accepted
```
Creating a table named `__global__` and setting per-table DQ config on it silently overwrites the
lake's global quality defaults for every other table; `delete_table_config` on it wipes them.

Low likelihood (nobody names a table `__global__` by accident) but it is a genuine collision
between two constructors, and the sentinel discipline that prevents every other such collision
does not cover it because `__global__` is a *double*-underscore name, which the scheme deliberately
allows.

**Why guards miss it.** The quality keys are assembled outside `redis_keys.py` (finding #8), so
the collision analysis in `test_redis_key_prefix.py` never sees them.

**Fix proposal.** Apply the same `doc:` discipline used everywhere else: per-table config at
`quality:config:doc:{table}`, global at `quality:config:global`. That is structurally
collision-free regardless of what the table is called, and it matches `meta:leaf:doc:`,
`shares:doc:`, `rbac:users:doc:`. Fold both into `redis_keys.py` constructors while doing it
(finding #8, item 2). Same treatment for `latest:` — `get_all_latest` currently disambiguates
table-level from column-level results with an ad-hoc `if ":" not in suffix` filter
(`config.py:397-400`), which a `doc:` layer would make unnecessary.

---

## 13. Four backends, four different `delete()` failure semantics — LOW-MEDIUM — PROVEN

Same operation, four behaviours on partial failure:

| Backend | Batching | Partial failure |
|---|---|---|
| Local (`local_storage.py:129-141`) | `shutil.rmtree` | raises whatever rmtree raises; may leave a partial tree |
| S3 (`s3_storage.py:534-552`) | streamed, 1000/batch | **silently ignored** (finding #6) |
| MinIO (`minio_storage.py:278-291`) | streamed generator | collects errors, raises `RuntimeError` |
| Azure (`azure_storage.py:305-311`) | **materialises full list**, one call per blob | first failure aborts, half-deleted prefix, raises |
| GCS (`gcp_storage.py:258-264`) | **materialises full list**, one call per blob | first failure aborts, half-deleted prefix, raises |

Azure and GCS additionally hold every key of the prefix in memory before deleting, which the S3
and MinIO implementations were deliberately changed to avoid (see the comments at
`s3_storage.py:523-524` and `minio_storage.py:276-277`) — deleting a large table on those two
backends has an unbounded memory profile the other two do not.

Callers cannot branch on backend, so the effective contract is the weakest one: "delete may
partially succeed and may or may not tell you". Combined with finding #1 this is what makes a
failed drop invisible.

**Fix proposal.** Settle one contract in the `StorageInterface.delete` docstring — *"deletes
everything under the path; raises on any object that could not be deleted, after attempting all
of them"* — and bring all five in line: stream the listing in Azure/GCS, accumulate per-object
errors everywhere, raise once at the end. A shared `_delete_many(keys) -> List[error]` helper on
the ABC would remove the four-way drift at the source.

---

## 14. N+1 Redis round-trips — LOW — PROVEN (by inspection, patterns are unambiguous)

- `redis_catalog.py:1245-1259` `list_shares` — `SMEMBERS` then one `GET` per share id in a Python
  loop. `get_users`/`get_roles` in the *same class* (`:633-686`) do exactly this correctly with a
  pipeline, so the divergence is internal to one file.
- `redis_catalog.py:1302-1315` `list_linked_shares` — identical shape, same fix.
- `redis_infra.py:196-240` `_FallbackCatalog.get_users` / `get_roles` — `HGETALL` per member with
  no pipeline, while `RedisCatalog.get_users` pipelines. Two implementations of the same concept
  with different round-trip costs; the fallback is the one used when the main catalog is
  unavailable, i.e. under duress.
- `redis_catalog.py:1196-1200` `_delete_by_scan` — a pipeline of single-key `DEL`s rather than one
  variadic `DEL k1 k2 … kn`. Correct, but N commands where 1 would do, on the
  delete-a-whole-supertable path.
- `streaming/jobs.py:299-303` `reap` — `HGETALL` per job id, then a 4-command pipeline per orphan.
  Could be one pipeline of `EXISTS` across the index.

**Fix proposal.** Mechanical: wrap each `SMEMBERS`-then-loop in `self.r.pipeline()` exactly as
`get_users` does, and collapse `_delete_by_scan` to `p.delete(*str_keys)`.

---

## 15. Scheme and config nits — LOW — PROVEN

- **`meta_namespace_deletion_intent` uses a hyphen** (`redis_keys.py:485`:
  `…:meta:deletion-intent`) where every other multi-word segment in the module is snake_case:
  `table_names`, `rowid_seq`, `table_config`, `name_to_id`, `chain_head`, `legal_hold`,
  `linked_shares`, `auth:tokens`. One-character fix, but it is the kind of thing that makes a
  hand-typed `redis-cli` key miss.
- **`schema:` sits at a different depth from its siblings.** `schema(org,sup,simple)` →
  `lakes:{sup}:schema:doc:{simple}`, but the other two per-simple-table attribute documents live
  under `meta:` — `meta:table_config:doc:{simple}`, `meta:rowid_seq:doc:{simple}`. Same kind of
  thing, two depths. (It is also the key most likely to be forgotten in a drop path — see
  finding #7.)
- **`storage_factory.py:15` imports `os` and never uses it**, and its docstring (line 38) promises
  *"process environment STORAGE_TYPE (live os.environ)"* while the code reads
  `settings.STORAGE_TYPE` (`:46`) — a module-level singleton frozen at import
  (`config/settings.py:671`). The precedence documented is not the precedence implemented; the
  value cannot be changed after first import. This is the same singleton that already forces
  `tests/` and `supertable/` to be run as separate pytest invocations. (I checked and *refuted*
  a related suspicion: `settings.STORAGE_TYPE` **is** uppercased at build time
  (`settings.py:494`), so a lowercase `STORAGE_TYPE=minio` env does not fall through to
  `ValueError: Unknown storage type`.)
- **`AzureBlobStorage._with_base` (`azure_storage.py:187-191`) is a byte-identical re-declaration**
  of `StorageInterface._with_base` (`storage_interface.py:14-19`). Dead override; if the base
  ever changes, Azure silently keeps the old behaviour.
- **`to_duckdb_path` bypasses `_with_base`** in all four object backends
  (`s3:116`, `minio:148`, `gcp:104`, `azure:130` — all `f"{self.base_prefix}/{key}"`) while
  `presign` uses `_with_base` (`s3:128`, `minio:158`, `gcp:116`, `azure:145`). The two differ on
  trailing slashes and on empty keys. Two ways to compute the same thing, one of which is not
  the shared helper.
- **`iter_partition_chunks` docstring contradicts its code.** `monitoring/partitions.py:466-470`
  says *"If the caller bailed early we delete a partially-consumed handle"*, but line 471 guards
  the delete with `if start >= total`, i.e. it does **not** delete on early `break`. The code is
  the safer of the two behaviours; the comment should follow it.

---

## KEY INVENTORY — every constructor in `supertable/redis_keys.py`

Generated by reflection over the module (see probe output). `Guarded` = appears in
`test_redis_key_prefix.py::_all_helpers`.

| Constructor | Scope | Shape (org=`org`, sup=`sup`, simple=`tbl`) | Type | Guarded | Note |
|---|---|---|---|---|---|
| `system_scope` | org | `supertable:org:system` | prefix | y | |
| `system_scope_pattern` | org | `supertable:org:system:*` | SCAN | y | no callers |
| `auth_tokens` | org | `supertable:org:system:auth:tokens` | HASH | y | |
| `audit_stream` | org | `supertable:org:system:audit:stream` | STREAM | y | |
| `audit_chain_head` | org | `supertable:org:system:audit:chain_head:doc:inst` | HASH | y | |
| `audit_config` | org | `supertable:org:system:audit:config` | HASH | y | |
| `audit_legal_hold` | org | `supertable:org:system:audit:legal_hold` | HASH | y | |
| `share_doc` | org | `supertable:org:system:shares:doc:shid` | STRING | y | |
| `share_index` | org | `supertable:org:system:shares:index` | SET | y | |
| `engine_thrifts` | org | `supertable:org:system:engine:thrifts` | HASH | y | |
| `engine_plugs` | org | `supertable:org:system:engine:plugs` | HASH | y | |
| `engine_duckdb` | org | `supertable:org:system:engine:duckdb` | STRING | y | |
| `lakes_scope` | org | `supertable:org:lakes` | prefix | y | no callers |
| `lakes_pattern` | org | `supertable:org:lakes:*` | SCAN | y | no callers |
| `super_table_pattern` | lake | `supertable:org:lakes:sup:*` | SCAN | y | correctly scoped (probe D) |
| `meta_root` | lake | `…:lakes:sup:meta:root` | STRING | y | |
| `meta_root_pattern_for_org` | org | `supertable:org:lakes:*:meta:root` | SCAN | y | |
| `meta_root_pattern_all_orgs` | global | `supertable:*:lakes:*:meta:root` | SCAN | y | |
| `meta_mirrors` | lake | `…:lakes:sup:meta:mirrors` | STRING | y | |
| `meta_namespace_deletion_intent` | lake | `…:lakes:sup:meta:deletion-intent` | STRING | **NO** | **hyphen, not snake_case (#15)** |
| `meta_table_names` | lake | `…:lakes:sup:meta:table_names` | SET | y | write-only in SDK; **not cleaned on drop (#7)** |
| `meta_leaf` | lake+table | `…:lakes:sup:meta:leaf:doc:tbl` | STRING | y | |
| `meta_leaf_pattern` | lake | `…:lakes:sup:meta:leaf:doc:*` | SCAN | y | |
| `meta_rowid_seq` | lake+table | `…:lakes:sup:meta:rowid_seq:doc:tbl` | STRING | y | **not cleaned on drop (#7)** |
| `meta_table_config` | lake+table | `…:lakes:sup:meta:table_config:doc:tbl` | STRING | y | **not cleaned on drop (#7)** |
| `staging_index` | lake | `…:lakes:sup:meta:staging:index` | SET | y | |
| `staging_doc` | lake+staging | `…:meta:staging:doc:stg:meta` | STRING | y | |
| `staging_pattern` | lake | `…:meta:staging:doc:*:meta` | SCAN | y | no callers |
| `staging_subkey_pattern` | lake+staging | `…:meta:staging:doc:stg:*` | SCAN | y | |
| `pipe_index` | staging | `…:meta:staging:doc:stg:pipes:index` | SET | y | |
| `pipe_doc` | staging+pipe | `…:meta:staging:doc:stg:pipes:doc:pipe` | STRING | y | |
| `pipe_pattern` | staging | `…:meta:staging:doc:stg:pipes:doc:*` | SCAN | y | |
| `query_job_doc` | org | `supertable:org:query:job:doc:job` | HASH | **NO** | undocumented scope (#10) |
| `query_job_chunks` | org | `supertable:org:query:job:chunks:job` | LIST | **NO** | **missing `doc:` layer (#10)** |
| `query_job_cancel` | org | `supertable:org:query:job:cancel:job` | STRING | **NO** | **missing `doc:` layer (#10)** |
| `query_job_index` | org | `supertable:org:query:job:index` | SET | **NO** | |
| `query_job_pattern` | org | `supertable:org:query:job:*` | SCAN | **NO** | **over-matches the index (#10)**; no callers |
| `query_job_subkey_pattern` | org+job | `supertable:org:query:job:*:job` | SCAN | **NO** | test-only caller |
| `lock_leaf` | lake+table | `…:lakes:sup:lock:leaf:doc:tbl` | STRING | y | |
| `lock_leaf_pattern` | lake | `…:lakes:sup:lock:leaf:doc:*` | SCAN | y | |
| `lock_leaf_prefix` | lake | `…:lakes:sup:lock:leaf:doc:` | prefix | y | no callers |
| `lock_stage` | lake+staging | `…:lakes:sup:lock:stage:doc:stg` | STRING | y | |
| `rbac_user_meta` | lake | `…:lakes:sup:rbac:users:meta` | HASH | y | |
| `rbac_user_index` | lake | `…:lakes:sup:rbac:users:index` | SET | y | |
| `rbac_username_to_id` | lake | `…:lakes:sup:rbac:users:name_to_id` | HASH | y | |
| `rbac_user_doc` | lake+user | `…:lakes:sup:rbac:users:doc:uid` | HASH | y | |
| `rbac_user_doc_prefix` | lake | `…:lakes:sup:rbac:users:doc:` | prefix | y | consumed by Lua concat (#9b) |
| `rbac_role_meta` | lake | `…:lakes:sup:rbac:roles:meta` | HASH | y | |
| `rbac_role_index` | lake | `…:lakes:sup:rbac:roles:index` | SET | y | |
| `rbac_rolename_to_id` | lake | `…:lakes:sup:rbac:roles:name_to_id` | HASH | y | |
| `rbac_role_doc` | lake+role | `…:lakes:sup:rbac:roles:doc:rid` | HASH | y | |
| `rbac_role_type_index` | lake+type | `…:lakes:sup:rbac:roles:type:doc:rtype` | SET | y | |
| `schema` | lake+table | `…:lakes:sup:schema:doc:tbl` | STRING | y | **sibling attrs live under `meta:` (#15)**; not cleaned on drop (#7) |
| `linked_share_index` | lake | `…:lakes:sup:linked_shares:index` | SET | y | |
| `linked_share_doc` | lake+link | `…:lakes:sup:linked_shares:doc:lid` | STRING | y | |
| `quality_prefix` | lake | `…:lakes:sup:quality:` | prefix | y | **only constructor handing out a raw prefix for external assembly (#8, #12)** |
| `monitor_partition` | org | `supertable:org:monitor:writes:doc:2026-01-01` | LIST | y | closed type set + date regex |
| `monitor_partition_drain` | org | `…:monitor:writes:doc:2026-01-01:_drain` | LIST | y | inherits TTL via RENAMENX (verified) |
| `monitor_partition_pattern` | org+type | `…:monitor:writes:doc:*` | SCAN | y | matches `_drain` too — defended by parser |
| `monitor_partition_pattern_for_org` | org | `…:monitor:*:doc:*` | SCAN | y | same |
| `registry` | org | `dataisland:org:registry:api:host:1` | STRING (TTL 30s) | y | `host` not `_safe`d, only `:`-checked |
| `registry_pattern_for_org` | org | `dataisland:org:registry:*` | SCAN | y | |
| `registry_pattern` | global | `dataisland:*:registry:*` | SCAN | y | |
| `app_master_mcp` | global | `dataisland:_apps_:doc:app:master_mcp` | STRING | y | |
| `app_scope_pattern` | global | `dataisland:_apps_:doc:*` | SCAN | y | no callers |

**Keys constructed OUTSIDE this module** (all under `quality_prefix`, all skipping `_safe()`):
`quality:config:__global__`, `quality:config:{table}`, `quality:rules:index`,
`quality:rules:doc:{rule_id}`, `quality:schedule`, `quality:schedule:{table}`,
`quality:latest:{table}`, `quality:latest:{table}:{column}`, `quality:anomalies:{table}`
(`quality/config.py:51` + `_key`), and `quality:pending:{table}`, `quality:running:{table}`,
`quality:cooldown:{table}` (`quality/scheduler.py:744-752`). Plus one built in Lua:
`rbac:users:doc:{uid}` at `redis_catalog.py:215`.

---

## What I checked and found CLEAN

I tried to refute each of the following and could not fault them.

**SCAN-pattern soundness.** Against a real Redis I verified `super_table_pattern("demo")` does
**not** leak into `demo2` — `_safe()`'s charset (`[a-z0-9_-]`) excludes every Redis glob
metacharacter (`*`, `?`, `[`, `]`, `\`), so glob injection through an org/sup/table name is
structurally impossible. `staging_pattern` and `staging_subkey_pattern` return exactly their
intended members and nothing else. `meta_leaf_pattern` does not catch `meta_rowid_seq` (the
regression at `test_redis_key_prefix.py:355` still holds). `query_job_subkey_pattern` cannot
cross-match a different job id, because `*` is anchored by a literal `:{job_id}` at the tail and
ids cannot contain `:`.

**Monitoring drain safety.** `monitor_partition_pattern*` does over-match the `:_drain` handle,
but `parse_monitor_partition_key` rejects it on segment count and
`list_drainable_partitions` filters through that parser (`partitions.py:233-234`), so the
orchestrator never sees a drain handle as drainable — the over-match is caught one layer down. I
also confirmed on a live server that `RENAMENX` **transfers the TTL** (`ttl src`=600 →
`ttl dst`=600), so a drain handle orphaned by a crashed run still self-expires under the 7-day
`EXPIREAT` backstop rather than leaking forever. The `RENAMENX`-over-`RENAME` choice documented at
`partitions.py:273-281` is correct and the crash-recovery reasoning holds.

**Monitoring TTL.** `_partition_expire_at` anchors to the partition's own midnight, so
re-applying `EXPIREAT` on every batch is genuinely idempotent and a continuously-written
partition cannot renew itself past 7 days. Key and expiry are resolved from a single date read
(`redis_partition_today`), so they cannot straddle midnight.

**Quality `latest:` scan.** `get_all_latest` filters column-level sub-keys with
`if ":" not in suffix` (`config.py:397-400`), so the `latest:*` pattern's over-match is handled.
`get_all_table_schedules`' `schedule:*` does not match the global `schedule` key (no trailing
colon), so it is clean as written.

**Replica resolution vs the cjson flip.** `_resolve_replica_info` already coerces with
`isinstance(tables, list) and tables` (`redis_catalog.py:478`), so `replica_tables` turning into
`{}` does not change replica behaviour. That is why finding #4's practical blast radius is the
API surface and large integers, not table visibility.

**`_SAFE_SEGMENT` / sentinel discipline.** Rejects uppercase, `:`, `/`, `.`, leading hyphen,
single-underscore wraps and >64 chars; accepts the documented `__internal__` convention.
`RESERVED_ORG_NAMES` covers `apps`, and position-2 sentinels prevent the `lakes:audit` /
`lakes:shares` class of collision that v1 had. I found no pair of constructors that can emit the
same string for different inputs **inside** `redis_keys.py` — the only collision (#12) is in the
quality keys assembled outside it.

**Redis connection handling.** `create_redis_client` caches one `redis.Redis` (and pool) per
effective `RedisOptions` under a double-checked lock; the cache key covers every field that
affects the connection. `close_all_redis_clients` is correctly the only teardown, and
`_evict_oldest_monitor` explicitly does *not* disconnect the shared pool
(`monitoring_writer.py:507-513`) — the comment there is accurate. `sentinel_strict` is hard-wired
`True` so there is no silent standalone fallback in production; the strict path raises the
original sentinel error rather than masking it.

**S3 retry scope.** `_call`'s retry is bounded to one attempt and to five specific redirect codes,
does not retry on generic 5xx, and `_ensure_bucket_region` sets its latch *before* the probe so it
cannot loop. Every `_call` site except `write_parquet` passes a re-readable body, so finding #2 is
one call site, not a class.

**`_get_object_safe`.** Both S3 (`s3_storage.py:394-404`) and MinIO
(`minio_storage.py:167-177`) close/release the stream in a `finally`, so a mid-read failure does
not leak a connection.

**Local atomicity.** `LocalStorage.write_json` is a proper temp-file + `fsync` + `os.replace` +
directory-`fsync`, and `read_json` has a matching retry window for the replace race.
`write_parquet`/`write_bytes` are *not* atomic on Local, but every parquet path in this codebase
is immutable and version-suffixed, and nothing is referenced until the catalog pointer flips — so
a torn file is never reachable. I treated this as intentional rather than a finding.

**Parquet projection parity.** `_project_columns` is shared on the ABC and all five backends route
through it; `LocalStorage.read_parquet` passes `partitioning=None` specifically to match the
object backends' BytesIO behaviour, and the comment explaining why (`local_storage.py:196-203`) is
correct. The one divergence is the exception type on an empty file — GCS raises `ValueError`,
the other three `RuntimeError` — noted but not worth a finding on its own.

**Storage factory.** `STORAGE_TYPE` *is* uppercased at settings-build time, so a lowercase env
value does not fall through to `ValueError` (I suspected it would and checked). Lazy imports and
the `_require(...)` extras hints are correct for all five backends.

**`delete_super_table`.** `super_table_pattern` genuinely sweeps every per-table key including the
ones `delete_simple_table` misses — the whole-lake drop is complete, which is why finding #7 is
scoped to the per-table path only.
