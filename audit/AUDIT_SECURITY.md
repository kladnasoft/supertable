# SuperTable — security review

Defensive pre-release review of `supertable/` at `1a52c79` (3.2.0), covering four
properties: tenant isolation, well-formed generated SQL, enforcement of row filters
and column masks, and secret containment.

Nothing has been fixed. Every finding carries a proposed fix, not a patch.

Threat model: an authenticated tenant who can submit arbitrary SQL through the read
path, a share *provider* who authors row filters, and an operator who can read logs.

Full evidence and probe scripts: `audit/detail/security.md`, `audit/detail/read_path.md`.

| mark | meaning |
|---|---|
| **VERIFIED-HERE** | I reproduced or read the code myself |
| **VERIFIED** | the reviewer ran a probe demonstrating it |
| **REVIEWED** | identified by reading; not executed |

---

## CRITICAL

### S1 — The read path executes arbitrary DuckDB, bypassing every control
**VERIFIED-HERE** · `supertable/system_query.py:84-95`

`classify_query` returns any non-`EXPLAIN`, non-`SHOW STATS` text untouched. There is
no allow-list, so DuckDB's file functions are reachable and the RBAC, share-filter
and deletion-vector views are simply not in the path:

```sql
SELECT p.* FROM read_parquet(['<data file>']) p WHERE EXISTS (SELECT 1 FROM orders)
```

A restricted role received the masked column, `__rowid__`, and a row the deletion
vector had removed. `read_csv('/etc/hostname')` reads local files;
`COPY … TO '/tmp/x.csv'` writes them.

**Impact.** This is the whole of access control, not one control. On object storage
the engine's own credentials are used, so any tenant can read any other tenant's
parquet directly, and can exfiltrate to a path it chooses.

**Fix.** An AST-level allow-list in `classify_query` — reject any query whose
FROM/JOIN sources are not resolvable table references; no table functions, no file
paths, no `ATTACH`/`COPY`/`INSTALL`/`LOAD`. sqlglot already parses every query here.
Independently, set `enable_external_access=false` on the read connection: it removes
the file functions at the engine level and costs nothing, since legitimate reads go
through views built by the library, not by the user.

### S2 — SQL injection through a share row filter
**VERIFIED-HERE** · `data_reader.py:367,371` → `engine_common.py:1224-1230`

Provider-authored filter text is interpolated raw into `CREATE OR REPLACE VIEW …
WHERE`. A filter of

```
1=1; CREATE TABLE pwned_by_share AS SELECT 42; --
```

returned `Status.OK`, bypassed the row filter entirely (200/200 rows), and left the
injected table on the engine's **persistent, shared DuckDB connection** — so it
outlives the query and is visible to other tenants' queries on that connection.

The detail that makes this subtle: the AND-merge branch at `:365` wraps the filter in
parentheses and is *accidentally* safe. The two raw-assign branches at `:367` and
`:371` do not, and those are the paths taken when the role has no filter of its own —
the common case.

**Fix.** Treat the filter as an expression, not a string: parse it with sqlglot,
require a single boolean expression, and re-serialize. Parenthesising is necessary
but not sufficient — it blocks statement injection while still allowing the filter to
reference anything in scope. Until parsed validation exists, parenthesise at all
three branches, not one.

---

## HIGH

### S3 — Storage backends do not contain paths
**VERIFIED** · `supertable/storage/*`

`_safe()` rejects every hostile segment — `:`, `*`, `../`, newline, uppercase, >64
chars — and `SuperTable()` refuses them all. But **`_safe` has zero call sites outside
`redis_keys.py`**: no storage backend does containment of its own. LocalStorage read
`../../../tmp/...` and absolute paths when called directly. Today the only reason a
tenant cannot reach that is that no code path passes user input to storage unchecked —
containment is incidental, not enforced.

Combined with S1, which *does* give the user control of a path, this stops being
theoretical.

**Fix.** Containment belongs in the backend, where it cannot be forgotten: resolve
the final path and assert it is under the configured base prefix. A defence that
depends on every caller remembering is not a defence.

### S4 — Plaintext S3 secret written to the log
**VERIFIED** · `spark_thrift.py:623`

`[spark.thrift] SET spark.hadoop.fs.s3a.secret.key=<secret>` is logged verbatim.
Anyone with log access holds the object-store credentials.

Separately: **`.env` with live credentials is tracked in git.** Rotate before release.

**Fix.** Redact the value at the logging site. More durably, add a log filter that
masks known-secret setting names anywhere they appear, so the next `SET` added does
not need to remember.

### S5 — Secrets reach persisted query plans and API responses
**REVIEWED** · `data_reader.py:446` → `plan_extender.py:132`

Exception text carrying presigned URLs or credential-bearing SQL is persisted to
Redis and to `__plans__` parquet. Spark `EXPLAIN EXTENDED` plans carry presigned URLs
into the same sink and into the API response. `settings`' auto-generated `__repr__`
exposes all 15 secret fields — latent, no caller today.

**Fix.** Redact on the way into the plan sink rather than at each producer. Give
`Settings` an explicit `__repr__` that masks secret fields; that is a two-line change
that removes a whole category of future leak.

---

## MEDIUM

| # | finding | where | status |
|---|---|---|---|
| S6 | `FilterBuilder` interpolates non-`"value"` types **unquoted** — verified row-filter bypass with `"amount" > 0 OR 1=1` | rbac filters | VERIFIED |
| S7 | `allowed_columns=["id","*"]` emits `SELECT id, *` and leaks every masked column | `create_rbac_view` | VERIFIED |
| S8 | Share row filters fail open three ways — a leaf-read exception, an empty string, or a filter in the house dict format all serve the full table (2 rows → 4) | `data_reader.py:373` | VERIFIED |
| S9 | Row filters that render to empty SQL (`[{}]`, `[{"AND": []}]`, `[]`) become no filter | rbac | VERIFIED |
| S10 | `SHOW STATS` bypasses the column mask — min/max/null-count of a denied column | read path | VERIFIED |
| S11 | `superadmin` is not a reserved name; a tenant can mint `role: "superadmin"`, and the type bypass at `access_control.py:249` discards any `tables` restriction stored with it | rbac | **FIXED** |
| S12 | `diagnostic_redaction.py` has **zero production call sites**, and redacts exception *type names* — not the secret *values* and messages that actually leak. The primitive the codebase needs does not exist | utils | VERIFIED |

### S11 — fixed, and wider than first reported

The reserved **name** was only half of it; the **type** is what enforcement
reads. Closed in `role_manager.py` (user-facing errors) *and* on the catalog
write path (`redis_catalog.py`, `RESERVED_ROLE_TYPE`), because `RedisCatalog`
is a documented public class and a check in the manager alone was skippable
by importing it.

Follow-up review of the fix found four more paths to the same outcome, each
verified exploitable and each now refused: promoting a role via
`rbac_update_role`, planting a second superadmin through the *public*
`allow_reserved` parameter, renaming the bootstrap role, and disabling it
(which was unrecoverable — `_resolve_role` denies a disabled role inside the
very check needed to re-enable it). See `docs/11_rbac.md` §11.2.

Separately, `Permission.RBAC` is now enforced: `RoleManager` and `UserManager`
take an `actor_role_name` and require it to mutate. Before that, RBAC
administration was unauthenticated regardless of the reservation.

`odata/policy.py:170-172` states share-filter failure is *"the one direction this must
never fail in"*. The fingerprint obeys that rule; the enforcement at
`data_reader.py:373` does not (S8). The intent is documented and the implementation
contradicts it.

---

## Controls verified as holding

Roughly 45 guards were checked and work; the ones that carried real weight:

**Column masks — the strongest part of the system.** Enforced as stacked SQL views and
correct through `SELECT *`, qualified star, CTEs, subqueries, `UNION ALL`, aggregates,
`count(*)` and `GROUP BY`. Masked references are denied at parse time *and* blocked
again at bind. Two independent layers, both working. Neither reviewer could defeat
them by query shape.

**Identifier quoting.** `quote_if_needed` (quote-doubling), `escape_parquet_path`, and
the sqlglot re-serialize in `rewrite_query_with_hashed_tables` confine hostile
identifiers to a single statement. The only unquoted sink in the view builders is
`where_clause` (S2).

**Redis key validation.** `_safe()` rejects every hostile segment tried; glob
injection is structurally impossible because the character class excludes every
metacharacter. `super_table_pattern` does not leak across lakes.

**Core role lookup fails closed.** Redis down → raises, no rows returned. This is the
correct direction, and it is worth noting because the *surrounding* code (read-only
guard, share filter, audit emit) fails open by explicit design — the contrast is
where the bugs are.

---

## Suggested order

1. **S1** — it subsumes most of the rest. Every other control is downstream of it.
2. **S2** — parenthesise all three branches today; parsed validation next.
3. **S4** — one log line, plus rotate the committed `.env`.
4. **S3** — containment in the backends, which also hardens S1's blast radius.
5. **S8/S9/S11** — fail-open paths in RBAC and shares.
6. **S5/S12** — build the redaction primitive, then apply it at the plan sink.

**Not covered.** Spark row filtering could not be exercised — no Thrift server in this
environment — and static reading suggests `FilterBuilder`'s double-quoted identifiers
make `!=` filters match *everything* on Spark. That is a row-filter bypass if it holds,
and it needs a live cluster to confirm before release.
