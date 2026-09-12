# SuperTable — pre-release defensive security review

**Repo:** `/home/kladnasoft/dev/dataisland/supertable` (branch `master`, `pyproject.toml` version 3.2.0)
**Date:** 2026-09-11
**Scope:** four safety properties — tenant isolation, generated-SQL well-formedness, RBAC row/column enforcement, secret containment.
**Method:** code reading plus executable probes. All probes run with `STORAGE_TYPE=LOCAL` and `.venv/bin/python`. **No library file was modified.**

Probe scripts (scratchpad only):

| probe | covers |
|---|---|
| `probe_p1_isolation.py` | every `redis_keys` constructor × hostile segments; LocalStorage traversal; quality key builders |
| `probe_p1b_e2e.py` | `SuperTable()` with hostile org/super; audit path builders; `_with_base` |
| `probe_p2_sql.py` | `quote_if_needed`, `create_rbac_view`, `create_tombstone_view`, `create_reflection_view`, `rewrite_query_with_hashed_tables` |
| `probe_p2b_filterbuilder.py` | RBAC `FilterBuilder` sanitizer confinement |
| `probe_p3_rbac_e2e.py` | 23 query shapes against a live table with a restricted role |
| `probe_p3c.py` | superadmin-vs-restricted isolation of the projection bug |
| `probe_p3d_share.py`, `probe_p3e_share_noparen.py` | share `_row_filter` injection, both merge paths |
| `probe_p4_secrets.py` | Spark credential logging, `settings` repr, presign truncation, redaction call sites |

---

## Findings summary

| # | Title | Severity | Status | Property |
|---|---|---|---|---|
| 1 | SQL injection via share `_row_filter` into the RBAC view | **critical** | VERIFIED | 2, 3 |
| 2 | Column-masked roles break on any query not naming the full allowed set | **high** | VERIFIED | 3 |
| 3 | Spark S3 secret key written to the log in plaintext | **high** | VERIFIED | 4 |
| 4 | `.env` with live credentials is tracked in git | **high** | VERIFIED | 4 |
| 5 | Exception text (presigned URLs, credential SQL) persisted to Redis + parquet | **medium** | REVIEWED | 4 |
| 6 | Spark `EXPLAIN EXTENDED` plan with presigned URLs persisted and returned by API | **medium** | REVIEWED | 4 |
| 7 | `FilterBuilder` interpolates non-`"value"` types unquoted → row-filter bypass | **medium** | VERIFIED | 2, 3 |
| 8 | `allowed_columns` containing `*` beside named columns leaks every column | **medium** | VERIFIED | 3 |
| 9 | No path containment in any storage backend; `_safe()` never applied to a path segment | **medium** | VERIFIED | 1 |
| 10 | Audit parquet writer bypasses the incidental org gate | **medium** | REVIEWED | 1, 4 |
| 11 | `settings` dataclass auto-`__repr__` dumps 15 secrets | **medium** | VERIFIED | 4 |
| 12 | Presign log truncation (`url[:96]`) leaks the access-key ID on short URLs | **low** | VERIFIED | 4 |
| 13 | `registry()` does not validate `host` | **low** | VERIFIED | 1 |
| 14 | Quality scheduler key builders skip `_safe()` | **low** | VERIFIED | 1 |
| 15 | `quote_if_needed("")` emits malformed SQL | **low** | VERIFIED | 2 |
| 16 | `rewrite_query_with_hashed_tables` falls back to the raw user SQL on parse failure | **low** | REVIEWED | 2 |
| 17 | Spark cluster credentials stored plaintext in Redis and returned unredacted | **low** | REVIEWED | 4 |
| 18 | `diagnostic_redaction.py` has zero production call sites | **informational** | VERIFIED | 4 |
| 19 | `_safe()` does not enforce `RESERVED_ORG_NAMES`, contradicting the module docstring | **informational** | VERIFIED | 1 |

---

## 1. SQL injection via share `_row_filter` into the RBAC view

**SEVERITY: critical — VERIFIED**

**File:** `supertable/data_reader.py:367` and `supertable/data_reader.py:369-372`; sink at `supertable/engine/engine_common.py:1224-1230`.

```python
# data_reader.py:364-372
if existing_rbac.where_clause:
    existing_rbac.where_clause = f"({existing_rbac.where_clause}) AND ({share_row_filter})"
else:
    existing_rbac.where_clause = share_row_filter          # <-- :367, RAW
else:
    reflection.rbac_views[td.alias] = RbacViewDef(
        allowed_columns=["*"],
        where_clause=share_row_filter,                      # <-- :371, RAW
    )
```

```python
# engine_common.py:1223-1230
where_sql = ""
if rbac_view_def.where_clause:
    where_sql = f" WHERE {rbac_view_def.where_clause}"
sql = (
    f"CREATE OR REPLACE VIEW {view_name} AS "
    f"SELECT {select_cols} FROM {base_table_name}{where_sql};"
)
con.execute(sql)
```

**Property affected: 2 and 3.** `_row_filter` is read from the catalog leaf payload (`data_reader.py:359`) — it is a *share-provider*-authored predicate, documented as such at `supertable/odata/policy.py:161-167`. It reaches `create_rbac_view` with no parsing, no quoting, and no character filtering, and `duckdb.DuckDBPyConnection.execute()` runs multi-statement strings.

**Verified.** `probe_p3e_share_noparen.py` set `_row_filter = "1=1; CREATE TABLE pwned_by_share AS SELECT 42 AS x; --"` on the leaf payload and ran an ordinary `DataReader.execute()`. Generated SQL:

```
CREATE OR REPLACE VIEW rbac_tomb_st_813c37dba4f12378_c3da6bd7_c3da6bd7 AS
SELECT * FROM tomb_st_813c37dba4f12378_c3da6bd7 WHERE 1=1; CREATE TABLE pwned_by_share AS SELECT 42 AS x; --;
```

Result: `status=Status.OK rows=200` (the row filter was fully bypassed — 200 is the unfiltered count), and

```
duckdb_tables() LIKE 'pwned%' -> [('pwned_by_share',)]
contents = [(42,)]
```

The injected table was created on the engine's **persistent, reused** DuckDB connection (`supertable/engine/duckdb.py:115-128`, `_shared_state()` at `:48-57`). That connection is thread-local, not per-org, so in a worker-pool server it is shared by every organization whose request lands on that thread. This directly contradicts the class docstring at `duckdb.py:83-85` ("No materialised TABLE state is retained between queries").

**Escalation.** Reflection view names are deterministic — `hashed_table_name()` at `engine_common.py:59-69` returns `st_<sha1(super_simple_version_cols)[:16]>`. An attacker with arbitrary DDL on the shared connection can pre-create a `TABLE` under another tenant's future reflection-view name, which would either break that tenant's `CREATE OR REPLACE VIEW` or shadow it. I did not exercise this second step; the arbitrary-DDL primitive is verified.

**Why existing validation does not cover it.** The RBAC-role filter path *is* sanitized (`rbac/filter_builder.py:21-28` blocks `;`, `--`, `/*`, `*/`). The share path never goes through `FilterBuilder` — `data_reader.py:359` pulls a finished SQL string straight out of Redis. Note the near-miss: the *merge* branch at `data_reader.py:365` wraps the predicate in parentheses, and my probe confirmed that accidentally defeats statement splitting (`Parser Error: syntax error at or near ";"`). Only the two raw-assignment branches (`:367`, `:371`) are exploitable. `supertable/odata/policy.py:150-151` repeats the same paren-merge and the same raw-assign at `:147-148`.

**FIX PROPOSAL.** Stop treating `_row_filter` as trusted SQL. Two changes, both needed:
- **Parse and re-serialize, do not string-concatenate.** Route every predicate destined for `create_rbac_view` through `sqlglot.parse_one(pred, into=exp.Condition, dialect="duckdb")` and emit `.sql(dialect="duckdb")`. A multi-statement or unbalanced payload fails to parse into a single condition and is rejected, and the re-serialization normalizes away comment tails. Fail **closed**: a filter that will not parse must deny the read, not be dropped.
- **Always parenthesize at the sink.** In `create_rbac_view`, emit `WHERE ({where_clause})` unconditionally rather than relying on callers to bracket. That makes the accidental protection at `data_reader.py:365` structural, and removes the difference between the "role has a filter" and "role has none" paths.

Longer term the better shape is to stop shipping SQL text across the trust boundary entirely: persist the share filter in the same JSON predicate form the roles use, and build it with `FilterBuilder`, so one sanitizer covers both sources.

---

## 2. Column-masked roles break on any query not naming the full allowed set

**SEVERITY: high (availability; fails closed) — VERIFIED**

**Files:** `supertable/engine/duckdb.py:327-337` (projection) vs `supertable/engine/engine_common.py:1214-1220` (RBAC projection).

`alias_to_columns[td.alias]` is built **only** from the parser's requested columns (`duckdb.py:327`: `cols = list(td.columns or [])`). The reflection view therefore projects only those. `create_rbac_view` then emits `SELECT <every allowed_column>` plus `WHERE <filter column>` over that narrower view.

**Verified** (`probe_p3c.py`, same query run as `superadmin` and as a role with `columns=["emp_id","dept","name"]`, `filters=[dept='eng']`):

| query | superadmin | restricted_reader |
|---|---|---|
| `SELECT name FROM emp` | OK / 200 | **ERROR / 0** |
| `SELECT dept FROM emp` | OK / 200 | **ERROR / 0** |
| `SELECT DISTINCT dept FROM emp` | OK / 2 | **ERROR / 0** |
| `SELECT emp_id, dept FROM emp` | OK / 200 | **ERROR / 0** |
| `SELECT emp_id, dept, name FROM emp` | OK / 200 | OK / 100 |
| `SELECT * FROM emp` | OK / 200 | OK / 100 |

Engine error: `Binder Error: Referenced column "dept" not found in FROM clause! Candidate bindings: "emp_id"` on
`... AS SELECT dept, emp_id, name FROM tomb_st_8f58dcec3f5fd99f_... WHERE "dept" = 'eng';`

Also breaks `INTERSECT`, `EXCEPT`, and self-joins for the same reason (a join branch that needs one column).

**Property affected: 3.** It fails *closed* — no mask leak, no filter bypass — so it is not a confidentiality bug. It is a release blocker of a different kind: any tenant using a column mask can only run `SELECT *` or a query that names every allowed column. It also means the mask/filter machinery is largely untested against realistic queries.

**Why existing validation does not cover it.** `access_control.restrict_read_access` validates requested columns against the allowlist (`rbac/access_control.py:291-299`) and is correct. Nothing reconciles the *allowlist* with the *projection* that the projection-aware estimator computes. This looks like a regression introduced with projection-aware read estimation: when reflection views projected every column, `SELECT <allowed_columns>` always bound.

**FIX PROPOSAL.** Make the reflection projection a superset of everything the view chain above it references. In `duckdb.py` (and the Spark equivalent), before computing `alias_to_table_name`/`alias_to_columns`, union in the alias's `RbacViewDef.allowed_columns` and the identifiers referenced by its `where_clause`. Keep the hashed table name derived from that widened set so the cache key stays honest. The alternative — narrowing `create_rbac_view`'s projection to `allowed ∩ requested` — is wrong: the RBAC view must not silently drop a column the outer query needs, and the `WHERE` column still has to be loaded even when it is never selected.

---

## 3. Spark S3 secret key written to the log in plaintext

**SEVERITY: high — VERIFIED**

**File:** `supertable/engine/spark_thrift.py:620-623` (function `_configure_spark_s3`).

```python
for key, value in settings:
    try:
        sql = f"SET {key}={value}"
        logger.debug(f"[spark.thrift] {sql}")
```

`settings` is populated at `spark_thrift.py:594-600` with `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` taken from the cluster config.

**Verified** (`probe_p4_secrets.py` invoked `_configure_spark_s3` with a fake cursor and a marker secret):

```
[spark.thrift] SET spark.hadoop.fs.s3a.secret.key=AKIA_SUPER_SECRET_VALUE_9999
```

**Property affected: 4.** DEBUG level, but `supertable/logging.py:86` ships DEBUG to the configured file/stdout sinks, and DEBUG is routinely enabled for query-engine troubleshooting — which is exactly when this line fires.

**Why existing validation does not cover it.** There is no redaction helper for secret *values* anywhere in the package (see finding 18).

**FIX PROPOSAL.** Log the key name and a value fingerprint, never the value: build a small `SECRET_SETTING_KEYS` set (`*.secret.key`, `*.access.key`, `*.session.token`, `password`) and emit `SET <key>=<redacted len=N sha8=...>` for members. Keep the non-secret settings logged verbatim so the line stays useful for debugging endpoint/region mistakes. The companion line at `spark_thrift.py:626` (`SET {key} failed: {e}`) should drop `{e}` or redact it — Thrift servers echo the offending statement back in the error.

---

## 4. `.env` with live credentials is tracked in git

**SEVERITY: high — VERIFIED**

`git ls-files` returns `.env`. `.gitignore` covers `TOKEN` but not `.env`. The file defines non-empty `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`, `SUPERTABLE_SUPERUSER_TOKEN`, `SUPERTABLE_MCP_TOKEN`, `SUPERTABLE_API_KEY`, `SUPERTABLE_REDIS_SENTINEL_PASSWORD` (confirmed non-empty via `probe_p4_secrets.py`, values not printed).

**Property affected: 4.** This was flagged in the v2.4.1 audit and is still present.

**FIX PROPOSAL.** Treat all six as compromised: rotate first, then `git rm --cached .env`, add `.env` to `.gitignore`, commit a `.env.example` with empty values, and purge history (`git filter-repo`) before the repo is published. Add a CI check that fails when `.env` appears in `git ls-files`.

---

## 5. Exception text (presigned URLs, credential SQL) persisted to Redis and parquet

**SEVERITY: medium — REVIEWED**

**Chain:**
- `supertable/data_reader.py:445-447` — `except Exception as e: message = str(e); logger.error(self._lp(f"Exception: {e}"))`
- → `supertable/data_reader.py:453-460` — `extend_execution_plan(..., message=message, ...)`
- → `supertable/plan_extender.py:132` — `"message": message,`
- → `supertable/plan_extender.py:158-162` → `supertable/monitoring_writer.py:452-453` — `s = json.dumps(payload, ...); pipe.rpush(target_key, s)`
- → drained to the `__plans__` sink supertable (parquet at rest).

The `try` at `data_reader.py` wraps the whole read, including `configure_httpfs_and_s3` (which executes `SET s3_secret_access_key='<secret>'` at `engine_common.py:276-293`) and `create_reflection_view_with_presign_retry` (whose `parquet_scan([...])` list is presigned URLs when `SUPERTABLE_DUCKDB_PRESIGNED=1`). DuckDB and httpfs quote the offending statement/URL in their error text.

**Property affected: 4.** Unlike a log line, this writes the secret to durable, org-retained storage and is the one path that is *selected for* credential-bearing errors. Not verified end-to-end because LOCAL storage never presigns; the format strings and the persistence chain are confirmed by reading.

Same shape, log-only, at `engine_common.py:472` and `engine_common.py:553`:
`logger.warning(f"{log_prefix}[duckdb.retry] presign fallback (view) for {view_name}: {msg}")` — and the gate tokens at `:549-552` include `SignatureDoesNotMatch`, `AccessDenied`, `403`, i.e. precisely the errors that name the signed URL. Write-path mirror at `processing.py:1389` and `processing.py:1396` (the latter at INFO, and it catches the failure of the *retry*, where paths are definitionally presigned).

**Why existing validation does not cover it.** `plan_extender.py:129` already bounds a neighbouring field (`getattr(query_plan_manager, "query", "")[:500]`); `message` got no treatment at all.

**FIX PROPOSAL.** Add a `redact_diagnostic_text(s)` helper next to `diagnostic_redaction.safe_exception_type` that (a) rewrites any `scheme://host/path?query` to `scheme://host/path?<redacted>`, (b) masks the values of a known secret-parameter/pragma set (`X-Amz-*`, `s3_secret_access_key`, `s3_access_key_id`, `s3_session_token`, `Signature`, `sig`, `token`), and (c) truncates. Apply it at the *persistence boundary* (`plan_extender.py:132`) so every producer is covered by one call, and at the four log sites above.

---

## 6. Spark query plan with presigned URLs persisted and returned by API

**SEVERITY: medium — REVIEWED**

**Files:** `supertable/engine/spark_thrift.py:1002-1041` (runs `EXPLAIN EXTENDED`, dumps `physical_plan` to `query_manager.query_plan_path`) → `supertable/plan_extender.py:86` → `:102` (`query_plan_manager.query_profile`, documented at `:99` as returned to API callers) → `:138` (`"query_profile": _safe_json(...)`) → `monitoring_writer.py:452-453` (Redis) → `__plans__` parquet.

Spark's `physical_plan` prints `FileScan parquet ... Location: InMemoryFileIndex(N paths)[<path>, ...]`, and those paths come from `_spark_create_parquet_view` (`spark_thrift.py:173-179`), fed by `_resolve_spark_file` (`spark_thrift.py:119-140`), which mints presigned HTTP URLs when `SUPERTABLE_SPARK_PRESIGNED` is on. A presigned URL is a bearer credential with a default 3600s lifetime.

**DuckDB is safe here only by accident**: `duckdb.py:122` calls `init_connection(con, temp_dir=temp_dir)` with no `profile_path`, so the `PRAGMA enable_profiling` branch at `engine_common.py:725-727` never runs and `base_plan` stays `{}`. `QueryPlanManager.profile_pragmas()` (`query_plan_manager.py:79-88`) exists and would re-open the hole.

**FIX PROPOSAL.** Scrub at the same persistence boundary as finding 5, and additionally strip query strings from any `http(s)://` token inside plan text before it is written or returned. Given that file lists are the point of a physical plan, the cleaner fix is to map presigned URLs back to bare object keys before serializing the plan — the key is what a human debugging the plan actually wants to see.

---

## 7. `FilterBuilder` interpolates non-`"value"` types unquoted

**SEVERITY: medium — VERIFIED**

**File:** `supertable/rbac/filter_builder.py:92-94` (and the same shape at `:76-77` in the `range` branch).

```python
else:
    value = _sanitize_value(str(val["value"]))
clauses.append(f"{safe_col} {operation} {value}")      # value NOT quoted
```

**Verified** (`probe_p2b_filterbuilder.py`), against a 2-row table:

| filter | generated WHERE | rows |
|---|---|---|
| `{"amount": {"operation": ">", "type": "value", "value": "..."}}` | `"amount" > '...'` | 0 (confined) |
| `{"amount": {"operation": ">", "type": "number", "value": "0 OR 1=1"}}` | `"amount" > 0 OR 1=1` | **2/2 — filter bypassed** |
| same via `"range"` | `"amount" > 0 OR 1=1` | **2/2 — filter bypassed** |
| `{"type": "number", "value": "(SELECT max(credit_limit) FROM base)"}` | `"amount" > (SELECT max(credit_limit) FROM base)` | subquery injected |

**Property affected: 2 and 3.** The row filter can be neutered, and a correlated subquery can read columns the mask hides. Statement splitting is *not* reachable — `_sanitize_value` (`filter_builder.py:26-27`) correctly blocks `;`, `--`, `/*`, `*/`.

**Why existing validation does not cover it.** `_sanitize_value` escapes quotes, which is only meaningful if the result is then wrapped in quotes. The `value`/`null` branches do wrap it (`:91`, `:84`); the fallback branch does not, so the escaping is inert and the value becomes bare SQL. `RowColumnSecurity.prepare()` (`rbac/row_column_security.py:70-82`) performs no validation of `filters` at all, so nothing upstream constrains `type`.

**Calibration:** the author of a role filter is an org admin, so this is privilege *retention*, not escalation — hence medium, not high. It becomes high if the platform ever exposes role editing to a non-admin.

**FIX PROPOSAL.** Make `type` a closed set (`value` | `null` | `number` | `bool`) and give each a typed emitter: `number` must `float()`/`int()` the value and emit the *parsed* number (so `"0 OR 1=1"` raises at parse time), `bool` must map to a literal `TRUE`/`FALSE`, anything unrecognised must raise rather than fall through. Delete the bare-interpolation fallback — it is the whole bug.

---

## 8. `allowed_columns` containing `*` beside named columns leaks every column

**SEVERITY: medium — VERIFIED**

**File:** `supertable/engine/engine_common.py:1215-1220`.

```python
if rbac_view_def.allowed_columns == ["*"]:
    select_cols = "*"
else:
    select_cols = ", ".join(quote_if_needed(c) for c in rbac_view_def.allowed_columns)
```

`quote_if_needed` (`engine_common.py:30-31`) passes `*` through unquoted, so `["id", "*"]` becomes `SELECT id, *`.

**Verified** (`probe_p2_sql.py`, base table `id,name,ssn`):

```
allowed_columns=['id','*']  ->  CREATE OR REPLACE VIEW v_probe AS SELECT id, * FROM base;
view columns = ['id', 'id_1', 'name', 'ssn']        # 'ssn' is supposed to be masked
allowed_columns=['*','id']  ->  SELECT *, id        -> ['id','name','ssn','id_1']
```

**Property affected: 3.** The exact-list comparison at `:1215` treats `["id","*"]` as "restricted", so the mask path is taken — and then emits a wildcard anyway.

**Why existing validation does not cover it.** `access_control.restrict_read_access` only short-circuits on `allowed_columns == ["*"]` (`access_control.py:279`); for `["id","*"]` it validates requested columns against `{"id","*"}`, which a `SELECT *` query skips entirely (`access_control.py:291`, `pt.columns` is `[]` for star queries). `RowColumnSecurity.prepare()` does not reject `*` mixed with names. Requires an admin misconfiguration — hence medium.

**FIX PROPOSAL.** Reject the mixed form where roles are *written*, in `RowColumnSecurity.prepare()`: if `"*"` appears in a `columns` list of length > 1, raise. Defense in depth at the sink: in `create_rbac_view`, treat `"*" in allowed_columns` as the wildcard case rather than comparing to the exact list `["*"]`, so a legacy role document already in Redis cannot produce a leaking view.

---

## 9. No path containment in any storage backend; `_safe()` never applied to a path segment

**SEVERITY: medium — VERIFIED**

**Files:** `supertable/storage/local_storage.py` (all methods), `supertable/storage/storage_interface.py:14-19` (`_with_base`).

`_with_base` is `path.strip("/")` followed by concatenation — it neutralizes a leading slash but passes `..` through verbatim. `LocalStorage` never calls `_with_base` at all, so `SUPERTABLE_PREFIX` is ignored on local disk, and every method hands the caller's string straight to `open`/`os.*`/`shutil.rmtree` (`local_storage.py:139`) / `os.replace` (`:90`).

**Verified** (`probe_p1_isolation.py`, `probe_p1b_e2e.py`):

```
app home = /home/kladnasoft/supertable
READ SUCCEEDED via relative ../ traversal: {'other_tenant': 'data'}      # ../../../tmp/victim_.../secret.json
READ SUCCEEDED via absolute path:          {'other_tenant': 'data'}      # /tmp/victim_.../secret.json
_with_base('/etc/passwd')                    = 'tenants/etc/passwd'
_with_base('../../other_tenant/secret.json') = 'tenants/../../other_tenant/secret.json'
```

`SUPERTABLE_HOME` is applied with `os.chdir` (`config/homedir.py:79,86`) — a working directory, not a jail.

**Property affected: 1.** Mitigating: the main SDK entry points *are* gated, but only **incidentally**. `probe_p1b_e2e.py` confirms every hostile org/super is rejected:

```
org='../escape_org'      -> ValueError: Invalid Redis key segment for 'org'
org='acme/../escape_org' -> ValueError: Invalid Redis key segment for 'org'
sup='../escape_sup'      -> ValueError: Invalid Redis key segment for 'sup'
org='acme:evil'          -> ValueError ; org='ACME' -> ValueError
```

…but the rejection comes from `redis_keys._safe()` inside `catalog.root_exists()`, **after** `SuperTable.__init__` has already built the path (`super_table.py:70` builds `super_dir`; `:73` is the first `_safe` contact). `probe_p1b_e2e.py` section D confirms **`_safe()` has zero call sites outside `redis_keys.py`**. `SuperTable.__init__` validates `super_name` via `is_reserved_super_name` (`super_table.py:53`) — which only blocks the `_foo_` sentinel pattern, not `../` — and does not validate `organization` at all (`:61`).

So tenant isolation currently rests on "every storage-touching flow happens to build a Redis key from the same name first, in the right order". Finding 10 is the case where that assumption breaks.

**FIX PROPOSAL.** Two independent layers, because either alone is brittle:
- **Validate at the boundary, not incidentally.** Export the `_SAFE_SEGMENT` check as a public `validate_name(label, value)` and call it explicitly in `SuperTable.__init__`, `SimpleTable.__init__`, `DataReader.__init__`, `DataWriter.__init__`, and `MetaReader` — *before* any path is built. This turns an ordering-dependent accident into a stated contract and gives callers a clean error instead of a Redis-flavoured one.
- **Contain at the sink.** Give `StorageInterface` a `_resolve(path)` that rejects absolute paths and any `..` segment after normalization, and have `LocalStorage` additionally assert the realpath is under `get_app_home()`. Add a traversal test to `supertable/storage/tests/test_storage.py`, which currently has none.

---

## 10. Audit parquet writer bypasses the incidental org gate

**SEVERITY: medium — REVIEWED (path builders VERIFIED)**

**Chain:**
- `supertable/audit/middleware.py:66-67` — `org = request.query_params.get("organization") or request.query_params.get("org")`, `.strip()` only.
- → `supertable/audit/logger.py:143` `_init_redis()` → `audit/writer_redis.py:53` `RK.audit_stream(org)` → `_safe()` raises `ValueError` for a hostile org — **but `logger.py:161-162` catches bare `Exception` and only logs**, leaving `self._redis_writer = None`.
- → `supertable/audit/logger.py:144` `_init_parquet()` succeeds unconditionally.
- → `supertable/audit/logger.py:331` → `supertable/audit/writer_parquet.py:196-200`:
  `partition = _partition_dir(org, now)` … `storage.write_bytes(full_path, parquet_bytes)`.

`_audit_base` at `writer_parquet.py:88-89` is `f"{org}/__audit__"` with no validation.

**Verified** (`probe_p1b_e2e.py` section C): `_partition_dir('../../../tmp/pwned', ...)` returns `'../../../tmp/pwned/__audit__/year=2026/month=01/day=01'`. Combined with finding 9 (LocalStorage honours `..`), an attacker-supplied `organization` query parameter writes parquet outside the tenant tree.

**Property affected: 1 and 4.** Not marked VERIFIED end-to-end because I did not stand up the audit middleware with auditing enabled; each link is confirmed by reading and the path builders are confirmed by probe. Mitigating: auditing is off by default (`audit/logger.py:429-449,474`), and `request.state.session_org` takes priority over the query parameter when a session middleware is installed (`middleware.py:60-64`).

**FIX PROPOSAL.** Two changes: make `_init_redis` re-raise `ValueError` (a rejected org name is a caller error, not a transient Redis outage — catching `redis.RedisError` specifically preserves the intended resilience while letting validation through), and validate `org` in `ParquetAuditWriter.write_batch` before `_partition_dir`. Validating in the middleware too is worthwhile but is the platform layer's call.

---

## 11. `settings` dataclass auto-`__repr__` dumps 15 secrets

**SEVERITY: medium (latent) — VERIFIED**

**File:** `supertable/config/settings.py:100-101` — `@dataclass(frozen=True) class Settings:` with no `field(repr=False)` on any secret.

**Verified** (`probe_p4_secrets.py` section B): all 15 secret fields appear in `repr(settings)` with their values; `any field(repr=False)? False`. On this machine `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`, `SUPERTABLE_SUPERUSER_TOKEN`, `SUPERTABLE_MCP_TOKEN`, `SUPERTABLE_API_KEY`, `SUPERTABLE_REDIS_SENTINEL_PASSWORD` are non-empty in the repr.

**Property affected: 4.** Latent, not active: there is currently no `f"{settings}"` / `repr(settings)` / `asdict(settings)` call in the package. It converts to a full credential dump the moment someone adds a config debug line, or a traceback formatter that renders locals.

**FIX PROPOSAL.** Add `field(repr=False)` to all 16 secret fields (the 15 above plus `GOOGLE_APPLICATION_CREDENTIALS`), and add a unit test asserting that `repr(settings)` contains none of their values — the test is what stops the next field from being added without the flag.

---

## 12. Presign log truncation leaks the access-key ID on short URLs

**SEVERITY: low — VERIFIED**

**File:** `supertable/engine/data_estimator.py:419` — `logger.debug(f"[estimate.resolve] presigned → {url[:96]}...")`

Truncation is length-based, not semantic. **Verified** (`probe_p4_secrets.py` section C): for a long bucket/key the 96-char cut lands before `X-Amz-Credential` and leaks nothing; for a short endpoint and key it does not:

```
http://s3:9000/b/k.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLEKEYID%2F
```

The full access-key ID is disclosed. `X-Amz-Signature` is never reached, so this is key-ID disclosure, not a usable bearer credential — hence low. Related, unbounded, and at INFO: `data_estimator.py:360` and `data_estimator.py:436` (`f"[estimate.resolve] storage.{attr} → {url}"`), plus `:426` which logs a `key` that may already be a presigned URL.

**FIX PROPOSAL.** Replace the slice with the `redact_diagnostic_text` helper from finding 5 — strip the query string and log `scheme://host/path?<redacted>`. Drop the two INFO-level URL logs to DEBUG while you are there.

---

## 13. `registry()` does not validate `host`

**SEVERITY: low — VERIFIED**

**File:** `supertable/redis_keys.py:992-993` — `if not isinstance(host, str) or not host or ":" in host: raise ValueError(...)`.

Only `:` is rejected. **Verified** (`probe_p1_isolation.py`): `host` accepts `*`, `/`, `..`, `\n`, `_apps_`, uppercase, and a 200-char string:

```
registry(host='*')      -> 'dataisland:acme:registry:api:*:1'
registry(host='a/b')    -> 'dataisland:acme:registry:api:a/b:1'
registry(host='..')     -> 'dataisland:acme:registry:api:..:1'
```

**Property affected: 1.** `host` is the OS hostname (server-controlled), so this is not attacker-reachable in normal deployments — low. It does violate the module's own stated invariant 6 at `redis_keys.py:108` ("Every constructor validates its segments with `_safe(label, value)`"), and a `*` in a literal key is a landmine for any future pattern-based cleanup.

**FIX PROPOSAL.** Apply `_safe('host', host)` — or, since hostnames legitimately contain dots and uppercase, a dedicated `_safe_host` regex `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. Either way the constructor should enforce it rather than the caller.

---

## 14. Quality scheduler key builders skip `_safe()`

**SEVERITY: low — VERIFIED**

**File:** `supertable/quality/scheduler.py:744-752`.

```python
def _pending_key(org, sup, table):  return RK.quality_prefix(org, sup) + f"pending:{table}"
def _running_key(org, sup, table):  return RK.quality_prefix(org, sup) + f"running:{table}"
def _cooldown_key(org, sup, table): return RK.quality_prefix(org, sup) + f"cooldown:{table}"
```

`org`/`sup` are validated inside `quality_prefix` (`redis_keys.py:842-845`); `table` is not. **Verified** (`probe_p1_isolation.py`): `_pending_key('acme','lake1','orders:evil*')` → `supertable:acme:lakes:lake1:quality:pending:orders:evil*`. Same gap in `quality/config.py:51` (`_dq_key` joins `*parts` unvalidated).

**Property affected: 1.** Low because `table` is sourced from catalog entries that already passed `_safe` when their leaf key was created — the escape needs a second bug to become reachable. It is a defense-in-depth gap and another violation of invariant 6.

**FIX PROPOSAL.** Move these three key shapes into `redis_keys.py` as proper constructors that call `_safe('simple', table)`, alongside `quality_prefix`. That also brings them under `test_no_raw_fstring_keys_outside_redis_keys`, which is what should have caught them.

---

## 15. `quote_if_needed("")` emits malformed SQL

**SEVERITY: low — VERIFIED**

**File:** `supertable/engine/engine_common.py:27-34`. For `col = ""` (or whitespace, after `.strip()`), `all(...)` over an empty string is `True`, so the bare empty string is returned.

**Verified** (`probe_p2_sql.py`): `quote_if_needed("") -> ''`; `create_rbac_view` with `allowed_columns=[""]` emits `CREATE OR REPLACE VIEW v_probe AS SELECT  FROM base;` → `ParserException: SELECT clause without selection list`.

**Property affected: 2.** Malformed, not injectable, and it fails closed — hence low. Reachable via a role whose `columns` list contains an empty string, which `RowColumnSecurity.prepare()` does not reject.

**FIX PROPOSAL.** Raise `ValueError` on an empty/whitespace identifier in `quote_if_needed` rather than returning it. An empty column name is never a legitimate input and the caller is better served by a named error than by a DuckDB parser message.

---

## 16. `rewrite_query_with_hashed_tables` falls back to the raw user SQL on parse failure

**SEVERITY: low — REVIEWED**

**File:** `supertable/engine/engine_common.py:576-580`.

```python
try:
    parsed = sqlglot.parse_one(original_sql)
except Exception as e:
    logger.warning(f"[duckdb] Failed to parse SQL for rewrite; using original. Error: {e}")
    return original_sql
```

On a sqlglot parse failure the *unrewritten* user SQL is executed against the connection, so no alias is remapped onto the tombstone/RBAC view chain. **Verified** (`probe_p2_sql.py`) that the fallback triggers and returns a non-single-statement string for `SELECT * FROM facts WHERE )(`.

**Property affected: 2.** Low in practice: such a query cannot reference a reflection view it did not cause to be created, and DuckDB rejects it. The concern is structural — the fallback is fail-*open* with respect to the view chain, sitting in front of a connection where finding 1 shows attacker-created objects can exist. Positive note: the same probe confirms the *success* path correctly drops trailing statements (`SELECT * FROM facts; DROP TABLE base;` → `SELECT * FROM rbac_st_x_1234 AS facts`).

**FIX PROPOSAL.** Fail closed: raise instead of returning `original_sql`. If a query cannot be parsed it cannot be safely rewritten, and refusing it is strictly better than executing an unmapped version.

---

## 17. Spark cluster credentials plaintext in Redis, returned unredacted

**SEVERITY: low — REVIEWED**

**File:** `supertable/redis_catalog.py:1533-1547` stores the cluster config dict — including `s3_access_key`, `s3_secret_key`, `password` (read back at `spark_thrift.py:594,598,708`) — as JSON in the `engine:thrifts` hash. `list_spark_clusters` (`redis_catalog.py:1554-1569`) returns the whole document.

**Property affected: 4.** Low because registering a Spark cluster is already an org-admin operation and the reader needs the secret to connect. It matters for blast radius: any Redis read grant, and any UI that renders `list_spark_clusters`, becomes credential disclosure.

**FIX PROPOSAL.** Have `list_spark_clusters` return a redacted projection by default (`s3_secret_key: "***"`), with the raw document behind a separate, explicitly-named method used only by the engine. Encrypting at rest with the existing `SUPERTABLE_AUDIT_FERNET_KEY` machinery is the stronger option if the deployment already provisions that key.

---

## 18. `diagnostic_redaction.py` has zero production call sites

**SEVERITY: informational — VERIFIED**

**File:** `supertable/utils/diagnostic_redaction.py`.

**Verified** (`probe_p4_secrets.py` section D): the only importer is its own test at `supertable/utils/tests/test_diagnostic_redaction.py:12`. Its docstring at `:4` asserts "`safe_exception_type` is called from request logging", which is not true in this repo — it is presumably called from the platform layer. Separately, `supertable/demo/medcenter/run.py:48` defines a private copy of the same function instead of importing it.

Worth stating plainly: even wired in everywhere, this module would not fix findings 3, 5, 6, or 12. It redacts an exception **type name**; those leaks are secret **values** and exception **messages**. The codebase is missing the primitive it actually needs.

**FIX PROPOSAL.** Grow the module into the redaction surface the code needs — add `redact_url(s)` and `redact_diagnostic_text(s)` (finding 5) — then wire them at the persistence and log boundaries. Have `demo/medcenter/run.py` import rather than re-declare, so there is one implementation to audit.

---

## 19. `_safe()` does not enforce `RESERVED_ORG_NAMES`

**SEVERITY: informational — VERIFIED**

**File:** `supertable/redis_keys.py:208-237` vs the module docstring at `:101-103` ("The `apps` org name is reserved (`RESERVED_ORG_NAMES`) as defence in depth") and `:108` ("Every constructor validates its segments with `_safe`").

**Verified** (`probe_p1_isolation.py`): `org="apps"` is accepted by every constructor — e.g. `registry("apps", ...)` → `dataisland:apps:registry:api:h1:1`. The reservation lives only in `is_reserved_org_name` (`:269-279`), which `probe_p1b_e2e.py` section D confirms has **no production caller** (only `super_table.py:53` uses the *super*-name variant).

No collision actually results: the real sentinel is `_apps_`, so `dataisland:apps:*` and `dataisland:_apps_:*` are disjoint. The reservation is genuinely belt-and-braces, so the impact is nil — but the docstring claims a guarantee the code does not provide, which is the kind of drift that makes the next reviewer trust the wrong thing.

**FIX PROPOSAL.** Either enforce it (have `_safe` reject `RESERVED_ORG_NAMES` when `label == 'org'`) or correct the docstring to say the reservation is advisory and must be applied by callers. Enforcing is cheaper and matches what the docstring already promises.

---

# CONTROLS VERIFIED AS HOLDING

### Property 1 — tenant isolation

| Control | Guard (file:line) | Evidence |
|---|---|---|
| `_safe()` rejects `:` separators, `*` wildcards, `/` and `../` traversal, newlines, uppercase, empty, >64 chars, and the `_foo_` sentinel pattern | `redis_keys.py:208-237`, regex at `:187-189`, sentinel at `:172` | `probe_p1_isolation.py` — every one of 12 hostile segments rejected by every constructor (only `"apps"`, finding 19, passes) |
| Every `redis_keys` constructor that takes `org`/`sup`/`simple`/`user_id`/`role_id`/`staging_name`/`pipe_name`/`share_id`/`link_id`/`job_id`/`instance_id`/`app_name` validates it | `redis_keys.py:340-1031` (all constructors) | `probe_p1_isolation.py` enumerated every public function by signature; no unguarded segment other than `registry(host=)` (finding 13) |
| `monitor_type` is a closed set | `redis_keys.py:866-882` (`_VALID_MONITOR_TYPES`, `_safe_monitor_type`) | probe: non-member rejected |
| Monitoring partition `date` must be ISO 8601 | `redis_keys.py:873,885-890` (`_DATE_RE`, `_safe_date`) | probe: malformed date rejected |
| `service_type` is a closed set | `redis_keys.py:973-990` | probe: non-member rejected |
| `SuperTable()` rejects every hostile org and super name | `super_table.py:73` → `redis_catalog.root_exists` → `redis_keys.meta_root` `:447-448` | `probe_p1b_e2e.py` section A — all 6 hostile combinations raise `ValueError` |
| Reserved sentinel super names are refused at the entry point | `super_table.py:53-57` (`is_reserved_super_name`) | read + `probe_p1b_e2e.py` section B |
| Redis namespace policy is enforced by a regression test (no raw key f-strings outside `redis_keys.py`; position-2 sentinel discipline) | `supertable/tests/test_redis_key_prefix.py` | read; the two gaps it misses are findings 13 and 14 |
| `assert_prefixed` refuses any key outside the two recognised roots | `redis_keys.py:240-257` | read |
| `StagingArea` validates via the catalog *before* building its path (correct ordering, unlike `SuperTable`) | `staging_area.py:89-90` then `:92` | read |
| Streaming job chunk prefixes are reachable only from a record already persisted through a `_safe`-validated key | `redis_keys.py:645-648`, `streaming/jobs.py:141-147` | read |

### Property 2 — generated SQL well-formedness

| Control | Guard (file:line) | Evidence |
|---|---|---|
| `quote_if_needed` doubles embedded `"` — a column name containing a quote, semicolon, comment marker, newline, or a literal `* , secret` stays exactly one identifier | `engine_common.py:27-34` | `probe_p2_sql.py` — 8 of 10 hostile identifiers produce a 1-statement, 1-select-expression SQL; the 2 failures are the empty-string case (finding 15) |
| `create_rbac_view` column list is quoted — semicolon/comment injection via `allowed_columns` produces a bound error, never a second statement | `engine_common.py:1218-1220` | probe: `['id; DROP TABLE base; --']` → `SELECT "id; DROP TABLE base; --"`, 1 statement, BinderException; `base` survived |
| `escape_parquet_path` doubles `'` — a tombstone or data path containing `'); DROP TABLE base; --` stays inside the string literal | `engine_common.py:50-52`, used at `:506`, `:1329`, `:1463` | probe: `create_tombstone_view` and `create_reflection_view` both emit exactly 1 statement with the payload escaped; `base` survived |
| `sanitize_sql_string` is applied to DuckDB S3 credential values | `engine_common.py:37-47`, used at `:276-293` | read |
| `rewrite_query_with_hashed_tables` drops trailing statements on the success path | `engine_common.py:576-609` (sqlglot parse → `.sql()` re-serialize) | probe: `SELECT * FROM facts; DROP TABLE base;` → `SELECT * FROM rbac_st_x_1234 AS facts` (1 statement) |
| Interpolated view/table names are never caller-controlled — `hashed_table_name` returns `st_<sha1[:16]>`, tombstone views `tomb_<name>_<uuid4[:8]>`, RBAC views `rbac_<name>_<uuid4[:8]>` | `engine_common.py:59-69`, `duckdb.py:205,216,237` | probe: `hashed_table_name('sup','simple',3,['a']) -> 'st_074dde6558009afe'` |
| Per-query uuid suffix prevents concurrent queries from clobbering each other's views | `duckdb.py:202-205` | read |
| `FilterBuilder._sanitize_value` blocks `;`, `--`, `/*`, `*/` | `filter_builder.py:21-28` | `probe_p2b_filterbuilder.py` — both semicolon and comment payloads raise `ValueError` |
| `FilterBuilder._sanitize_column` enforces `^[A-Za-z_][A-Za-z0-9_]*$` and quotes | `filter_builder.py:4,13-18` | read + probe (all generated predicates show `"col"`) |
| `FilterBuilder._sanitize_operation` is an allowlist of 16 operators | `filter_builder.py:5-10,31-36` | read |
| The parenthesised share-filter merge blocks statement splitting | `data_reader.py:365`, `odata/policy.py:151` | `probe_p3d_share.py` — the same payload that succeeds on the raw path fails here with `Parser Error: syntax error at or near ";"` |

### Property 3 — row filters and column masks

View chain ordering confirmed correct by inspecting the generated SQL: reflection → tombstone (system-column strip + deletion-vector anti-join) → RBAC (column mask + row filter). Observed:
`CREATE OR REPLACE VIEW rbac_tomb_st_9ea861dcfb0dd476_901148c0_901148c0 AS SELECT dept, emp_id, name FROM tomb_st_9ea861dcfb0dd476_901148c0 WHERE "dept" = 'eng';`
The anti-join runs *below* RBAC, so `__rowid__` is still available to it and RBAC never sees the system columns — the ordering the docstring at `engine_common.py:1289-1291` claims.

| Shape | Column mask | Row filter | Evidence |
|---|---|---|---|
| `SELECT *` | holds — `['dept','emp_id','name']`, no `salary`/`ssn` | holds — 100/200 | `probe_p3_rbac_e2e.py` |
| `SELECT e.*` (qualified star) | holds | holds — 100 | same |
| CTE `WITH c AS (SELECT * FROM t) SELECT * FROM c` | holds | holds — 100 | same |
| Subquery `SELECT * FROM (SELECT * FROM t) s` | holds | holds — 100 | same |
| `UNION ALL` | holds | holds — 200 = 2×100 | same |
| `SELECT count(*)` | n/a | holds — 100 | `probe_p3b_final.py` |
| CTE + `count(*)` | n/a | holds — 100 | same |
| Subquery + `count(*)` | n/a | holds — 100 | same |
| `UNION ALL` + `count(*)` | n/a | holds — 200 | same |
| `GROUP BY dept` | n/a | holds — only `eng` returned | `probe_p3_rbac_e2e.py` |
| Qualified masked ref `SELECT e.salary` | denied at parse time | — | `PermissionError: You don't have permission to columns: {'salary'}` |
| Bare masked ref `SELECT salary, ssn` | denied | — | `PermissionError` on both |
| Masked col in `UNION ALL` branch | denied | — | `PermissionError` |
| Masked col in aggregate `sum(salary)` | denied | — | `PermissionError` |
| Masked col in `WHERE salary > 100000` | denied | — | `PermissionError` |
| Masked col in `ORDER BY salary` | denied | — | `PermissionError` |
| Masked col in `HAVING max(salary) > 0` | denied | — | `PermissionError` |
| Masked col in a window `OVER (ORDER BY salary)` | denied | — | `PermissionError` |
| Masked col through a CTE `SELECT c.salary` | blocked at bind | — | `Binder Error: Values list "c" does not have a column named "salary"` |
| Masked col through a subquery `SELECT s.ssn` | blocked at bind | — | `Binder Error: ... does not have a column named "ssn"` |

The column-validation guard that produces the `PermissionError`s is `rbac/access_control.py:291-299` (merged, CTE-free physical-table column set from `SQLParser.get_physical_tables()`); the view that produces the bind errors is `engine_common.py:1227-1231`. **Both layers hold** — masked columns are unreachable through every shape tested, including the CTE and subquery shapes where parse-time validation alone would not have been enough.

Also holding: `__rowid__` and `__timestamp__` are stripped from user results by the tombstone view's static `COLUMNS(c -> c NOT IN (...))` predicate (`engine_common.py:1306-1308`); `SELECT __rowid__` from a restricted role does not return the column.

### Property 4 — secret containment

| Control | Guard (file:line) | Evidence |
|---|---|---|
| DuckDB S3 credential SQL (`SET s3_secret_access_key=...`) is **not** logged, and there is no `CREATE SECRET` anywhere | `engine_common.py:276-293` | read; no logging call on that path |
| All six storage backends contain zero logging calls — no credential can leak from the storage layer itself | `storage/*.py` | grep: zero `logger.`/`print` in all six files |
| `redis_connector` logs host/port/db/sentinel list and never `opts.password` | `redis_connector.py:149-231` | read |
| HTTP access logging uses `request.url.path`, excluding the query string (and therefore any token or presign parameter) | `logging.py:414-425` | read |
| `run_engine_diagnostics` reports version/memory/threads/temp-dir/cache state only — no credential, endpoint, or URL | `engine_common.py:954-1180` | read in full |
| `plan_stats` never receives file lists, so `profile_overview` cannot carry presigned URLs | `data_estimator.py:842-864`, `executor.py:267`, `plan_stats.py` | read |
| DuckDB profiling is off by default — `init_connection` is called without `profile_path`, so the plan JSON that would contain the `parquet_scan` file list is never produced | `duckdb.py:122` vs `engine_common.py:725-727` | read |
| `plan_extender` already bounds the `sql` field it persists (`[:500]`) | `plan_extender.py:129` | read |
| The write-path probe logs counts, not paths — the correct pattern to copy | `processing.py:1360-1363` | read |
| `settings.py` defines no `__str__`/`dict()`/`as_dict()`/`model_dump()`, and nothing in the package calls `repr(settings)` today | `config/settings.py` | grep: zero hits for `{settings}`, `repr(settings)`, `asdict(settings)` |
| Production `print()` is confined to `timer.py` (function name + elapsed) | `utils/timer.py:81,107` | grep |
| Presign truncation does hold for long bucket/key URLs | `data_estimator.py:419` | probe: 96-char cut lands before `X-Amz-Credential`; `X-Amz-Signature` never reached in any tested shape |

---

## Recommended release gate

Blockers: **1** (share-filter SQL injection), **2** (column-masked roles cannot run ordinary queries), **3** (plaintext Spark secret in logs), **4** (`.env` in git — rotate before anything else).

Fix-next: **5**, **6**, **7**, **8** — all four are small, well-localized changes, and 5 and 6 share one redaction helper.
