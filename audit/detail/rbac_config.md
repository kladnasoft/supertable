# Adversarial audit — RBAC / ROLES / SHARES / CONFIG / AUDIT

Repo: `/home/kladnasoft/dev/dataisland/supertable` @ `1a52c79` (master, clean tree).
Scope: `supertable/rbac/`, `supertable/audit/`, `supertable/config/`, `supertable/quality/`,
`supertable/monitoring_writer.py`, `supertable/odata/`.
No library files were modified. All probes are throwaway scripts under the session scratchpad,
run with `STORAGE_TYPE=LOCAL` against the repo's own hermetic fakeredis harness
(`tests/characterization/harness.py`), i.e. the real `RedisCatalog` Lua and the real read path.

18 findings. 17 PROVEN by reproduction, 1 REASONED. Ordered by severity.

---

## F1 — Revoking all table grants grants full access to everything

**SEVERITY: CRITICAL** — **PROVEN**

`supertable/rbac/row_column_security.py:72-73`, reached from
`supertable/rbac/role_manager.py:184-189`.

```python
# row_column_security.py:70-73
def prepare(self) -> None:
    if not self.tables:
        self.tables = {"*": {"columns": ["*"], "filters": ["*"]}}
```

### What goes wrong

`RoleManager.update_role` merges `data` over the existing document and pushes the result
through `RowColumnSecurity.prepare()`. An administrator revoking a role's last table grant
passes `{"tables": {}}`. `{}` is falsy, so `prepare()` replaces the empty grant set with the
**wildcard-everything** entry and persists it. The role goes from "one column of one table" to
"every column of every table in the SuperTable".

### Concrete scenario (reproduced)

Role `revokeme` = `{"employees": {"columns": ["id"], "filters": ["*"]}}`; a second table
`secrets(id, token)` exists that the role was never granted.

```
before: {'employees': {'columns': ['id'], 'filters': ['*']}}
  SELECT * FROM sup2.secrets      -> DENIED: You don't have permission to read table 'secrets'.

  rm.update_role(rid, {"tables": {}})        # operator intent: revoke everything

after revoke-all, stored tables: {'*': {'columns': ['*'], 'filters': ['*']}}
  SELECT * FROM sup2.employees    -> OK  cols=['id','name','salary','region'] rows=4
  SELECT * FROM sup2.secrets      -> OK  cols=['id','token'] rows=1     # token = 'hunter2'
```

The revoke is not merely ineffective — it is an escalation, and it widens the *column* mask on
the table the role legitimately had, too.

### Why guards miss it

`supertable/rbac/tests/test_rbac.py:373-378` (`test_empty_tables_defaults_to_wildcard`) and
`supertable/rbac/tests/test_rbac_per_table.py:110-118` (`test_rcs_defaults_when_empty`) both
*seal* the wildcard substitution — but only as a property of the `RowColumnSecurity` value
object, constructed directly, with no reader attached. Neither test asks what a `reader` role
carrying that document can then read. No test anywhere calls `update_role` with an empty
`tables` dict; the `update_role` tests (`test_rbac.py:676-700, 1746-1766`) always pass a
non-empty mapping. The dangerous transition is therefore invisible to the suite while the
mechanism that causes it is explicitly blessed by two green tests.

### Fix proposal

Split "absent" from "empty", because they are different operator intentions and today they
collapse into the most permissive one.

- In `prepare()`, apply the wildcard default **only when `tables is None`** (field genuinely
  not supplied), and treat `{}` as a valid, fully-restrictive grant set that is persisted
  verbatim. `_resolve_table_entry` already returns `None` for an empty mapping and
  `restrict_read_access` already denies on that, so deny-all works for free once the
  substitution stops firing.
- In `update_role`, distinguish `"tables" in data` from `data.get("tables")` so an explicit
  `{}` cannot be re-defaulted by the `existing.get("tables", default_tables)` fallback at
  `role_manager.py:186`.
- Because roles persisted before the fix already contain the substituted `{"*": ...}`, the
  change alters no stored document; only new writes behave differently. That makes it safe to
  ship without migration, but it also means the two sealing tests must be rewritten rather
  than deleted — invert them to assert deny.

---

## F2 — A role created with no policy can read everything

**SEVERITY: HIGH** — **PROVEN**

`supertable/rbac/row_column_security.py:72-73` (same substitution as F1, create path via
`supertable/rbac/role_manager.py:141-152`).

### What goes wrong

The audit's headline question: *what happens when a role has NO policy at all?* It is
**allow-all**, not deny.

```
rm.create_role({"role": "reader", "role_name": "nopolicy", "tables": {}})
stored doc: {'role': 'reader',
             'tables': {'*': {'columns': ['*'], 'filters': ['*']}},  # <- substituted
             ...}
SELECT * FROM sup1.employees -> OK  rows=4  cols=['id','name','salary','region']
```

A caller that builds a role incrementally ("create the role, then attach grants") has a fully
privileged reader in the window between the two calls. A caller whose grant-building code
throws before populating `tables` ships an omnipotent reader and gets no error.

Note the asymmetry that makes this clearly a defect rather than a design stance: an **empty
column list is correctly deny** (`access_control.py:283-286`), verified —
`{"employees": {"columns": []}}` denies both `SELECT *` and `SELECT salary`. So "empty means
none" is already the established semantic one level down; only the table level inverts it.

### Why guards miss it

Same two tests as F1 seal the substitution at the value-object layer. They use `role="admin"`
and `role="reader"` interchangeably, which hides the point: for an `admin` the wildcard is
harmless (admins bypass table filtering anyway, `access_control.py:249-250`), for a `reader`
it is a total grant.

### Fix proposal

Covered by F1's change. Additionally, make `create_role` reject a role whose effective grant
set is empty *unless* the caller passes an explicit opt-in (e.g. `allow_unscoped=True`), so
"I forgot to set tables" cannot silently mean "everything". Rationale: role creation is a
deliberate act with a known intent; guessing the most permissive interpretation of a missing
field is never the right guess for an authorization object.

---

## F3 — `create_role` silently returns a *different* existing role on a name collision

**SEVERITY: HIGH** — **PROVEN**

`supertable/rbac/role_manager.py:135-139`

```python
if role_name:
    existing_id = self._catalog.rbac_get_role_id_by_name(org, sup, role_name)
    if existing_id:
        return existing_id
```

### What goes wrong

The requested `role` type and `tables` are discarded and the *pre-existing* role's id is
returned. Names are matched case-insensitively (`redis_catalog.py:772` lowercases on write).
The built-in role created at bootstrap (`role_manager.py:84-89`) is named `superadmin`, so:

```
rm.create_role({"role": "reader", "role_name": "SuperAdmin",
                "tables": {"employees": {"columns": ["id"], "filters": ["*"]}}})
  -> bf97d1e87053419d8dad85e784101058
existing superadmin id = bf97d1e87053419d8dad85e784101058   same=True
resolved doc: {'role': 'superadmin',
               'tables': {'*': {'columns': ['*'], 'filters': ['*']}}, ...}
```

The caller asked for a reader limited to one column and received **the superadmin role id**,
with no error and no indication. It then assigns that id to a user
(`UserManager.create_user(roles=[rid])`), and that user is a superadmin.

This is not specific to `superadmin` — any name collision yields a role whose privileges are
whatever the *first* creator chose. But `superadmin` is the one name guaranteed to exist in
every SuperTable, and it is not reserved: `validate_role_name`
(`redis_catalog.py:43-57`) checks only the character class, never a reserved-word list.

### Why guards miss it

The idempotency is intended for retry-safety and is tested as such — the tests assert the same
id comes back, never that the *returned role matches the requested one*. Nothing tests
creating a role whose name collides with a role of a **different type**.

### Fix proposal

Two independent changes, both needed:

1. Make `create_role` raise `ValueError` on a name collision whose stored `role` type or
   `tables` differ from the request, and keep the idempotent return only when the existing
   document is equivalent to what was asked for. Retry-safety is preserved (a genuine retry
   sends identical content); silent substitution is not. Rationale: idempotency must key on the
   full request, not on the name alone, or it stops being idempotency and becomes aliasing.
2. Add a reserved-name list (`superadmin`, and whatever the host considers built-in) to
   `validate_role_name` so tenant-supplied names can never collide with a bootstrap role in
   the first place. Put it in `redis_catalog` next to `SAFE_ROLE_NAME_RE` so direct catalog
   writers are covered too, matching the existing defense-in-depth comment at
   `role_manager.py:99-104`.

---

## F4 — Role type `superadmin`/`admin` silently discards the role's table restrictions

**SEVERITY: HIGH** — **PROVEN**

`supertable/rbac/access_control.py:248-250`

```python
# Superadmin/admin: no filtering needed
if role_type in (RoleType.SUPERADMIN, RoleType.ADMIN):
    return {}
```

### What goes wrong

`restrict_read_access` returns before `role_tables` is even read. A role document that is
internally contradictory — type `superadmin` *and* a narrow `tables` map — resolves to
unrestricted, with the restriction accepted at write time and never surfaced:

```
rm.create_role({"role": "superadmin", "role_name": "totally_normal",
                "tables": {"employees": {"columns": ["id"], "filters": ["*"]}}})
SELECT * FROM sup2.secrets -> OK  cols=['id','token'] rows=1   # 'secrets' never granted
```

Yes, `superadmin` is a hardcoded bypass, and it is keyed on the `role` **type** string, not on
the role name — so it is reachable by anyone who can reach `create_role`/`update_role` with an
arbitrary `role` field. There is no allow-list on the `role` value beyond "is a valid
`RoleType`" (`RowColumnSecurity.__init__`, `row_column_security.py:46`).

The write-side checks have the same shape via `ROLE_PERMISSIONS[SUPERADMIN] = set(Permission)`
(`permissions.py:21`), but those at least still run `_check_table_access`
(`access_control.py:113-114`); only the read path returns early and skips the table gate
entirely.

### Why guards miss it

`test_rbac.py:1175-1181` (`test_restrict_read_access_disabled`) asserts exactly this early
return is correct, for a superadmin with no tables. It documents the bypass; it does not probe
a superadmin *with* tables, which is the contradictory state.

### Fix proposal

- Treat a `tables` map on a SUPERADMIN/ADMIN role as a caller error at write time: reject it in
  `RowColumnSecurity.prepare()` (or normalise it to the wildcard and log at WARNING), so the
  stored document can never assert a restriction the reader ignores. Rationale: the bug is not
  the bypass — an admin bypass is a legitimate design — it is that the system accepts and
  stores a grant it will not honour, which is what makes an operator believe a restriction
  exists.
- Separately, gate *who may mint* a role of type `superadmin`/`admin`. That gate belongs at the
  library boundary (a `create_role(..., allow_privileged=False)` default), not only in the host,
  because `RoleManager` is a public API surface.

---

## F5 — A share's row filter fails open: unreadable leaf, or wrong-typed filter, serves all rows

**SEVERITY: HIGH** — **PROVEN**

`supertable/data_reader.py:355-377` (injection), `:360` (type guard), `:373-374` and `:376-377`
(the two swallows).

```python
share_row_filter = payload.get("_row_filter")
if share_row_filter and isinstance(share_row_filter, str):      # data_reader.py:360
    ...
except Exception as rf_err:                                     # data_reader.py:373-374
    logger.debug(self._lp(f"[share-filter] row filter injection failed for {td.alias}: {rf_err}"))
except Exception as e:                                          # data_reader.py:376-377
    logger.warning(self._lp(f"[dedup] config lookup failed, skipping dedup: {e}"))
```

### What goes wrong

Two independent fail-open paths on the **only** row-level control a data-sharing provider has.

**(a) Exception → no filter.** The whole tombstone+share block sits under a bare
`except Exception`. Any failure reading the catalog leaf drops the share predicate and the query
returns the full table. Reproduced by making `RedisCatalog.get_leaf` raise once:

```
share _row_filter = "region = 'EU'"
  normal                 -> OK rows=2  [1, 3]
  get_leaf raises        -> OK rows=4  [1, 2, 3, 4]     # filter silently gone
```

The same swallow also drops the deletion vector, so a transient Redis error resurrects
tombstoned rows in the same breath — the read-side twin of the write-side defect already
guarded by `tests/characterization/test_tombstone_dv_read_failure.py`.

**(b) Wrong type / empty → no filter.** `if share_row_filter and isinstance(..., str)` treats
every non-string and the empty string as "no filter", not as "malformed policy, deny":

```
_row_filter={'region': {'operation': '=', 'type': 'value', 'value': 'EU'}}  -> rows=4
_row_filter=''                                                             -> rows=4
_row_filter=None                                                           -> rows=4
_row_filter=0                                                              -> rows=4
```

The dict form is the exact JSON filter shape RBAC roles use (`row_column_security.py:24-26`),
so a provider or host that writes the share filter in the house format gets a share with **no
restriction at all** and no warning.

### Why guards miss it

Zero tests reference `_row_filter` anywhere in `tests/` or `supertable/*/tests/` (grep: no
hits). The feature is entirely uncovered.

Worse, the codebase already knows this is the forbidden direction and says so, in the sibling
module that computes the *fingerprint* of the same predicate —
`supertable/odata/policy.py:170-172`:

> *"A leaf that cannot be read PROPAGATES rather than being skipped. Treating an unreadable leaf
> as 'no filter' would fingerprint a wider policy than the reader enforces — the one direction
> this must never fail in."*

`_share_row_filters` (`policy.py:159-184`) duly lets the error propagate. The **enforcement**
path, `data_reader.py:373`, does the opposite. So the OData mid-page policy-change check can
fail a page for safety while the reader that actually produces the rows quietly serves them
unfiltered. Fingerprint and enforcement disagree in precisely the direction the design note
forbids.

### Fix proposal

- Make share-filter resolution fail **closed**: move the `_row_filter` read out of the shared
  `try` that also covers tombstones, and let a leaf-read failure raise (converted to
  `PermissionError` so existing 403 translation applies). Match `policy.py`'s already-correct
  stance so the two agree by construction.
- Replace the `isinstance(str)` silent skip with explicit handling: accept `str`; accept the
  structured dict form by routing it through `FilterBuilder` exactly as role filters are; raise
  on anything else. A filter that cannot be interpreted must deny, never widen.
- Note the predicate is interpolated raw into `WHERE {share_row_filter}`
  (`data_reader.py:365-371` → `engine_common.py:1225`) with none of the sanitisation role
  filters get (`filter_builder.py:13-36`). Whatever writes `_row_filter` is therefore fully
  trusted for SQL; that trust boundary should be stated in the code or the value should be
  validated on the way in.

---

## F6 — A row filter that renders to empty SQL silently becomes "no filter"

**SEVERITY: HIGH** — **PROVEN**

`supertable/rbac/filter_builder.py:102-106` and `supertable/rbac/access_control.py:320-338`

```python
# filter_builder.py:105-106
predicates = self.json_to_sql_clause(filters)
where_clause = f"\nWHERE {predicates}" if predicates else ""
```
```python
# access_control.py:328-334
where_idx = generated.upper().find("WHERE ")
if where_idx >= 0:
    where_clause = generated[where_idx + 6:]
...
if allowed_columns != ["*"] or where_clause:     # no view created when both are empty
```

### What goes wrong

`json_to_sql_clause` returns `""` for several structurally-valid-looking inputs. `""` is falsy,
so no `WHERE` is emitted, so `where_idx` is `-1`, so `where_clause` stays `""`, so
`access_control.py:334` declines to create an RBAC view at all. A role-level row-security policy
that the operator believes is in force applies nothing:

```
filters=[{'AND': []}]  -> OK rows=4   (all rows)
filters=[{}]           -> OK rows=4
filters=[[]]           -> OK rows=4
filters=[]             -> OK rows=4
```

All four are distinct from the documented `["*"]` "unrestricted" sentinel
(`filter_builder.py:102`), so the author clearly meant *something*, and got nothing. The degrade
is total and silent: no exception, no log line, no view.

### Why guards miss it

`supertable/rbac/tests/test_filter_builder.py` exercises well-formed filter documents and
asserts the SQL they produce. It has no case for a filter that renders empty, and no test
anywhere asserts "a non-`["*"]` filter must produce a view".

### Fix proposal

- Treat "non-wildcard filters that render to an empty predicate" as a hard error in
  `FilterBuilder.build_filter_query`: if `filters != ["*"]` and the rendered predicate is empty,
  raise. A row-security policy is never legitimately a no-op; if the operator wants no filter
  the sentinel `["*"]` already says so unambiguously.
- Add a belt-and-braces assertion in `restrict_read_access`: when `filters != ["*"]`, require a
  non-empty `where_clause` before deciding not to build the view. Rationale: the current
  condition at `:334` optimises away the view based on the *rendered output*, so any rendering
  bug converts directly into a missing control — the decision should be driven by the *policy*
  (is it restrictive?) not by the *artifact* (did we manage to render it?).
- While here: `access_control.py:328-331` locates the WHERE by scanning the generated string for
  `"WHERE "`. That is fragile and would mis-slice if a column or literal ever contained the
  substring. `FilterBuilder` should expose the predicate directly rather than round-tripping it
  through a full `SELECT ... FROM __PLACEHOLDER__` statement and parsing it back out.

---

## F7 — `SHOW STATS` bypasses the column mask

**SEVERITY: HIGH** — **PROVEN**

`supertable/data_reader.py:141-190`, specifically the discarded return value at `:173-179`.

### What goes wrong

`_execute_show_stats` calls `restrict_read_access` for its `PermissionError` side effect only
and ignores the returned per-alias views, then returns the raw statistics artifact. That
artifact carries one row **per column** including `min_*`, `max_*` and `null_count`
(`STATS_SCHEMA`, `processing.py:1790-1814`). A role masked down to one column reads value
ranges for every column it was denied:

```
role idonly = {"employees": {"columns": ["id"], "filters": ["*"]}}

SELECT salary FROM sup1.employees
  -> DENIED: You don't have permission to columns: {'salary'} in table 'employees'.

SHOW STATS sup1.employees
  -> OK
  column_name  min_bigint  max_bigint  min_string  max_string  null_count
  id           1           4           null        null        0
  name         null        null        a           d           0
  salary       100         400         null        null        0      <-- masked column
  region       null        null        EU          US          0      <-- masked column
```

Min/max on a masked column is a direct confidentiality loss (salary bands, date-of-birth ranges,
identifier prefixes), and repeated over a table's write history it narrows individual values.
Row-level filters are bypassed identically — the stats describe all physical rows, including
those a `filters` predicate would exclude.

The docstring at `:144-149` states the design ("the statistics rows/columns themselves are
returned unfiltered … we don't filter the stats output, only gate table access"), so this is a
deliberate choice — but it is a deliberate choice that defeats the column mask, which is
unlikely to be what the mask's users expect.

### Why guards miss it

`supertable/tests/test_system_query.py` covers only the *parsing* of `SHOW STATS`. No test pairs
`SHOW STATS` with a restricted role. The RBAC suite never issues a `SHOW STATS`.

### Fix proposal

Filter the stats frame by the resolved policy instead of discarding it: capture the
`RbacViewDef` that `restrict_read_access` already returns for the alias and, when
`allowed_columns != ["*"]`, drop stats rows whose `column_name` is not in the allow-list
(case-insensitively, matching `access_control.py:292-293`). Rationale: the view is already
computed and free; ignoring it is what creates the gap. Separately, consider suppressing
min/max entirely when the role carries any row `filters`, since a row-filtered role must not see
aggregates over rows it cannot read — masking columns alone does not close that.

---

## F8 — Data-quality custom rules run arbitrary SQL as `superadmin`, ungated

**SEVERITY: HIGH** — **REASONED** (full chain proven in code; exploitation requires the host to
expose rule CRUD, which is its stated purpose)

`supertable/quality/config.py:196-208` → `supertable/quality/checker.py:385-386` →
`supertable/quality/scheduler.py:658-665`

```python
# checker.py:385-386
if rt == "custom_sql":
    return rule.get("sql")
```
```python
# scheduler.py:658-665
rule_sql = build_custom_rule_sql(rule, table_fqn)
...
dr = DataReader(super_name=sup, organization=org, query=rule_sql)
if dr.query_plan_manager:
    dr.query_plan_manager.source_type = "system"
r_df, _, _ = dr.execute(role_name="superadmin")
```

### What goes wrong

`QualityConfig.create_rule` performs **no permission check of any kind** — it writes the rule
document straight to Redis (`config.py:203-205`); `created_by` is an unvalidated free-text
string. A rule with `rule_type: "custom_sql"` carries verbatim SQL, and the scheduler executes
it through the normal read path with `role_name="superadmin"`, which (per F4) skips table and
column filtering entirely.

Net effect: whoever can create a quality rule can read any table in that SuperTable, including
tables and columns their own role is denied. The query is additionally stamped
`source_type="system"`, so it does not look like a user read in monitoring.

Two lesser injection points in the same builder, reachable without `custom_sql`:

- `checker.py:357`/`:361` — `threshold` is interpolated raw:
  `f"... WHERE {q} < {threshold}"`.
- `checker.py:378-383` — `expected_values` are wrapped in quotes with no escaping:
  `vals = ", ".join(f"'{v}'" for v in expected)`.

Column names are safe: `_quote` (`checker.py:49-50`) doubles embedded quotes correctly.

I did not stand up a scheduler run to execute a crafted rule end-to-end, hence REASONED — but
every link (no gate on write, verbatim SQL out of the builder, `role_name="superadmin"` at the
executor) is a direct read of the code with no conditional in between.

### Why guards miss it

The quality package was only recently moved into the library and its consumer loop is newly
wired; there is no test that asserts a rule cannot escalate. Nothing in `supertable/quality/`
imports `access_control`.

### Fix proposal

- Run rule SQL as the **rule owner's** role, not `superadmin`. Persist the creating role on the
  rule at `create_rule` time (a real, validated role name, not the free-text `created_by`) and
  pass it to `dr.execute(role_name=...)`. A check that cannot see the data the author can see is
  the correct semantics for a per-tenant quality rule anyway.
- Drop `custom_sql`, or confine it behind an explicitly privileged, separately configured path.
  An arbitrary-SQL rule type executed by a background daemon is an escalation primitive no
  matter how the role question is resolved.
- Parameterise or validate `threshold` (numeric coercion) and escape `expected_values` the way
  `filter_builder._sanitize_value` already does. Reuse that helper rather than adding a second
  escaping convention.
- Gate `QualityConfig.create_rule`/`update_rule` on `check_meta_access` (or a new CONTROL-level
  check) for the target table.

---

## F9 — The audit chain verifier can never succeed: producer and verifier hash different material

**SEVERITY: HIGH** — **PROVEN**

Producer `supertable/audit/logger.py:296-306`, verifier `supertable/audit/reader.py:385-388`.

```python
# logger.py:296-306  — producer
content_hash = compute_content_hash([e.event_hash() for e in events])
...
chain_hash = self._chain.advance(event_ids, content_hash)   # -> compute_batch_hash(ids, content_hash)
```
```python
# reader.py:385-388  — verifier
# ── Bug fix 1: the logger calls chain.advance(event_ids) without
# passing a file_hash, so batch_hash is computed with file_hash="".
# We must match that here — NOT use the Parquet file hash.
expected_batch_hash = compute_batch_hash(event_ids, "")
```

### What goes wrong

The comment is stale — the logger **does** pass a second argument. `compute_batch_hash`
(`chain.py:39-52`) mixes it in, so the two sides diverge for every batch:

```
content_hash        = a146f5be7e10e6b58215c6a8 ...
producer batch_hash = f38b2da091fefc837bab5d0c ...
verifier batch_hash = afd690b15b4e78be8eedf3ea ...
MATCH: False
```

`compute_content_hash` returns `""` only for an empty event list, and batches are never empty,
so the mismatch is unconditional. `verify_chain_integrity` reports `valid=False` with gaps on a
perfectly honest chain. A tamper detector that always fires carries no information: the operator
either ignores it or switches it off, and genuine tampering is indistinguishable from the
baseline noise.

Two structural weaknesses compound it, both confirmed by reading the code:

- **Tail truncation is undetectable.** `verify_chain_integrity` (`reader.py:273-437`) takes no
  `expected_head` and never calls `RedisAuditWriter.load_chain_head`
  (`writer_redis.py:145-159`); it only walks recorded hashes forward. Deleting the newest N
  batches leaves an internally consistent chain. The function that *does* take `expected_head`
  and would catch this — `verify_batch_chain` (`chain.py:184-228`) — has **zero production
  callers**; it is only reached from `audit/tests/test_chain.py`. The tested function is not the
  shipped function, and `verify_chain_integrity` has no test references at all.
- **No key.** There is no HMAC or signature anywhere in `supertable/audit/` — the chain is plain
  SHA-256 over public inputs, so anyone with write access to the audit objects can recompute it
  forward and re-anchor. `crypto.py` is Fernet field *encryption*, not signing, and is itself
  unused in production.

### Why guards miss it

No test performs a write→read→verify round trip; `AuditLogger` is never constructed in any test.
A single round-trip assertion would have caught the mismatch the day it was introduced. The
existing integrity tests operate on hand-built dicts passed to the unused verifier.

### Fix proposal

- Make the two sides share one function. Have the verifier call the same
  `compute_batch_hash(event_ids, content_hash)` the producer used, which means the batch's
  `content_hash` must be **persisted** alongside `chain_hash` in the event rows (it currently is
  not) — otherwise the verifier cannot reconstruct it without re-hashing every event, which is
  the stronger and better option: recompute `event_hash()` from the stored rows and derive
  `content_hash`, which also finally makes the content binding at `logger.py:296` actually
  enforced. Today no code path ever recomputes `event_hash()` during verification.
- Retire `verify_batch_chain` or promote it: one verifier, taking `expected_head` loaded from
  `load_chain_head`, so truncation is detected.
- Add the round-trip test (emit N batches through a real `AuditLogger` → verify → assert valid;
  then mutate one event / drop the last batch → assert invalid). That single test is worth more
  than the whole existing `test_chain.py`.
- If the chain is meant to resist an attacker with storage access, it needs a key it does not
  store next to the data (HMAC with a KMS-held key, or an external anchor). Unkeyed SHA-256 only
  detects accident.

---

## F10 — Role creation and every user/role-assignment mutation are unaudited

**SEVERITY: MEDIUM-HIGH** — **PROVEN**

`supertable/rbac/role_manager.py:106-154` (no emit), `supertable/rbac/user_manager.py`
(no audit code at all — `grep -c audit` returns 0), `supertable/rbac/access_control.py`
(no denial events).

### What goes wrong

`Actions` defines the full RBAC vocabulary (`audit/events.py:277-291`): `ROLE_CREATE`,
`USER_CREATE`, `USER_DELETE`, `USER_ROLE_ASSIGN`, `USER_ROLE_REMOVE`, `ROLE_ENABLE`,
`ROLE_DISABLE`. Only `ROLE_UPDATE` (`role_manager.py:209`) and `ROLE_DELETE`
(`role_manager.py:228`) are ever emitted. So:

- Minting a role — including a `superadmin`-type role (F4) — produces **no audit record**.
  Neither does the bootstrap superadmin creation at `role_manager.py:89`.
- Creating a user, deleting a user, and **assigning or revoking a role on a user** produce no
  record. The entire user↔role edge, which is what actually confers privilege, is invisible.
- Permission *denials* produce no record: `PermissionError` is raised at
  `access_control.py:73, 101, 107, 111, 132` with `logger.error` only. `Actions.ACCESS_DENIED`
  is emitted solely from `audit/middleware.py:180`, and that middleware is not wired into any
  app in this repo (`add_middleware(AuditMiddleware, ...)` appears only inside its own
  docstring at `middleware.py:84`). In-process denials are therefore entirely unaudited.

The two events that *do* fire carry **no actor**: `_audit_rbac` (`role_manager.py:18-33`) passes
no `actor_id`/`actor_username`, so `emit()`'s defaults apply (`audit/__init__.py:78-80`) and
every record reads `actor_type="system"`, `actor_id=""`. The trail records that a role was
deleted, never who deleted it.

Combined with F1/F3/F4, the escalation paths in this report are not merely possible — they are
unobservable after the fact.

### Why guards miss it

There is no test asserting "operation X emits event Y"; `test_emit.py` tests the emit plumbing
in isolation. A defined-but-never-emitted constant is invisible to every existing check.

### Fix proposal

- Emit on `create_role` (reusing `_audit_rbac`, `Severity.CRITICAL` when the created type is
  `superadmin`/`admin`), and add the equivalent helper to `UserManager` for `create_user`,
  `modify_user`, `delete_user`, `add_role`, `remove_role`.
- Thread an actor through. `RoleManager`/`UserManager` currently have no actor parameter at all;
  add an optional `actor` to the constructor (the host already knows the caller) and pass it to
  `emit`. An audit record without an actor answers the least interesting question.
- Emit `ACCESS_DENIED` from `access_control` at the point the `PermissionError` is raised, so
  denials are recorded regardless of whether an HTTP middleware is mounted.
- Add a meta-test that enumerates `Actions` and asserts every constant is either emitted
  somewhere in `supertable/` or listed in an explicit `_RESERVED_FOR_HOST` set. That converts
  the whole class of gap into a build failure. (77 of 85 constants are currently unemitted; most
  legitimately belong to the host, which is exactly why the exemption list must be explicit.)

---

## F11 — Table names in role grants are matched case-sensitively; the mismatch falls through to `"*"`

**SEVERITY: MEDIUM** — **PROVEN**

`supertable/rbac/access_control.py:57-63`

```python
def _resolve_table_entry(role_tables: dict, table_name: str) -> dict:
    return role_tables.get(table_name) or role_tables.get("*")
```

### What goes wrong

Every other identifier in this system is matched case-insensitively — role names
(`redis_catalog.py:772` lowercases), usernames (`user_manager.py:152`), and **column names
inside this very function** (`access_control.py:292-293` compares `requested_lower` against
`allowed_lower`). Table names alone are exact-match. When the case does not match, the specific
grant is skipped and the `"*"` default applies. If that default is permissive — which it is by
construction whenever the role was built through the paths in F1/F2 — the restriction evaporates:

```
role case_probe = {"Employees": {"columns": ["id", "name"], "filters": ["*"]},
                   "*":         {"columns": ["*"],          "filters": ["*"]}}

SELECT * FROM sup1.employees
  -> OK  cols=['id','name','salary','region']  rows=4     # 'Employees' grant never matched
```

The operator wrote a restriction, the system stored it, and it does nothing. Without a `"*"`
entry the same mismatch denies instead — so the failure mode flips on an unrelated part of the
document, which makes it very hard to reason about.

### Why guards miss it

`test_rbac_per_table.py` uses consistently lower-case table names throughout. No test mixes case
between the grant and the query.

### Fix proposal

Normalise table keys to a single case on write (in `RowColumnSecurity.prepare()`, alongside the
existing `sort_all()` normalisation at `row_column_security.py:51-56`) and lower-case the lookup
key in `_resolve_table_entry`. Do both: normalising only on read leaves already-stored mixed-case
documents ambiguous if two keys differ only by case. Reject a `tables` map containing two keys
that collide case-insensitively, rather than silently picking one. Rationale: the column path in
the same function already establishes case-insensitive as the house rule; the table path is
simply inconsistent with it.

---

## F12 — The role `enabled` flag recognises only two spellings of "off"

**SEVERITY: MEDIUM** — **PROVEN**

`supertable/rbac/access_control.py:22-31`

```python
enabled_val = role_info.get("enabled")
if isinstance(enabled_val, bytes):
    enabled_val = enabled_val.decode("utf-8")
if isinstance(enabled_val, str) and enabled_val.lower() in ("false", "0"):
    raise PermissionError(...)
if isinstance(enabled_val, bool) and not enabled_val:
    raise PermissionError(...)
```

### What goes wrong

Anything outside `{"false", "0"}` leaves the role **enabled**, so a disable that is written in
any other spelling silently does not take effect:

```
enabled=False     (bool) -> DISABLED     enabled=0          (int) -> ENABLED
enabled='false'   (str)  -> DISABLED     enabled='no'       (str) -> ENABLED
enabled='0'       (str)  -> DISABLED     enabled='off'      (str) -> ENABLED
enabled='False'   (str)  -> DISABLED     enabled='disabled' (str) -> ENABLED
                                         enabled=''         (str) -> ENABLED
```

Two aggravating factors:

1. **The library has no API that sets `enabled`.** `Actions.ROLE_ENABLE`/`ROLE_DISABLE` exist in
   `audit/events.py:280-281` but no `RoleManager` method writes the field. It is written only by
   the host, against an undocumented contract, and the parser accepts the narrowest possible
   vocabulary. (See the "unread config is ambiguous" principle — this is the mirror case: a field
   that is *read* but never *written* in-tree.)
2. **The codebase already owns a correct bool parser.** `config/settings.py::_env_bool` accepts
   `no`, `n`, `off`, `0`, `false` as falsy (tested at `config/tests/test_settings.py:173-176`).
   `_resolve_role` hand-rolls a weaker one.

Because role documents are stored as a Redis hash, values come back as strings
(`get_role_details`, `redis_catalog.py:691-702`), so the `isinstance(bool)` branch is unreachable
in production and the `int 0` case is not reachable through the catalog — but `'no'`, `'off'`,
`''` and any typo are all live.

### Fix proposal

Replace the two hand-rolled comparisons with a single shared truthiness helper (reuse or lift
`_env_bool`'s vocabulary) and invert the default for unrecognised values: an `enabled` field that
is *present but uninterpretable* should deny, not allow. Keep "field absent → enabled" for the
documented backward-compat case, which is a genuinely different state. Then add
`RoleManager.set_enabled()` so the field has one writer with one spelling, and emit
`ROLE_ENABLE`/`ROLE_DISABLE` from it (F10).

---

## F13 — Audit writes fail silently; the Parquet writer reports success after a failed write

**SEVERITY: MEDIUM** — **PROVEN** (code)

`supertable/audit/writer_parquet.py:200-215`

```python
try:
    storage.write_bytes(full_path, parquet_bytes)
    logger.debug(...)
except Exception as e:
    logger.error("[audit-parquet] write_bytes failed for %s: %s", full_path, e)

return {                      # <- unconditional, success-shaped
    "path": full_path,
    "file_hash": file_hash,
    "event_count": len(events),
    "bytes_written": len(parquet_bytes),
}
```

### What goes wrong

The `return` is outside the `try`. A failed write to the **system of record** returns a dict with
a populated `path`, and the caller's success check — `if result.get("path")` at
`logger.py:332-336` — passes, logging "Parquet batch written". The failure exists only as one
ERROR line that no counter or alarm observes.

Around it, the whole package is best-effort by design and nothing aggregates the failures:

- `AuditConfig.from_settings` (`logger.py:70-71`) has a bare `except Exception: return cls()`,
  and the all-defaults `AuditConfig` has `enabled=False` plus four further inverted defaults —
  see F15, which is the same swallow viewed as a default-contradiction rather than as a silent
  failure. A settings glitch silently disables auditing org-wide with no log line.
- Queue overflow drops events (`logger.py:201-207`) and increments `total_dropped`.
  `Actions.AUDIT_GAP` exists (`events.py:308`) for exactly this and is never emitted.
- The `_stats` counters (`logger.py:136-141`) have **no readers anywhere in the repo**, so
  `total_dropped` is write-only.
- `dropped = len(events) - written` (`logger.py:344`) counts Redis stream ids only: if Redis is
  down but Parquet succeeded the whole batch counts as dropped; if Parquet silently failed
  nothing counts. The one internal counter is wrong in both directions.
- `shutdown_all()` (`logger.py:496-505`) is never called and never registered with `atexit`,
  while the worker is a daemon thread — so up to `batch_size` queued events plus one in-flight
  batch are discarded at process exit, silently. (`streaming/runner.py:122` and
  `locking/redis_lock.py:105` do register `atexit` handlers, so the omission is inconsistent
  rather than deliberate.)
- `retention.py:154-155` and `:290-291` wrap the `LEGAL_HOLD_CHANGE` and `RETENTION_EXECUTE`
  emits in bare `except Exception: pass` — the record of a deletion can vanish with no log at all.

### Fix proposal

- Move the `return` inside the `try`, or return `{"path": None, "error": str(e)}` on failure, so
  the caller's existing `if result.get("path")` check becomes truthful. This is the one-line
  half of the fix and the one that matters most.
- Make `AuditConfig.from_settings` fail loudly: if audit is configured on and settings cannot be
  read, raise rather than silently returning `enabled=False`.
- Give the failures a surface: emit `AUDIT_GAP` on queue drop and on writer failure, and expose
  `_stats` through the existing monitoring path so "audit is broken" is detectable without
  reading logs.
- Register `shutdown_all` with `atexit`.

---

## F14 — Importing the library changes the host process's working directory

**SEVERITY: MEDIUM** — **PROVEN**

`supertable/config/homedir.py:84-87`

```python
# ---------- eager init (preserves original import-time behaviour) ----------
_app_home = _resolve_app_home()
change_to_app_home(_app_home)     # -> os.chdir(expanded_dir)   homedir.py:79
```

### What goes wrong

`os.chdir` runs at **import time**, and `homedir` is imported transitively by
`supertable/super_table.py:9` and `supertable/storage/local_storage.py:13` — i.e. by essentially
any use of the library. Observed directly while running an unrelated one-liner:

```
Ensured app home directory exists: /home/kladnasoft/supertable
Changed working directory to /home/kladnasoft/supertable
Current working directory: /home/kladnasoft/supertable
```

`chdir` is process-global and not thread-safe with respect to other threads doing relative I/O.
Embedded in a web service (which is how this library is consumed), importing it relocates the
CWD of the entire host process — silently invalidating every relative path the host had, and
racing any concurrent relative-path operation. The failure is also order-dependent: it happens
at whatever moment the first `supertable` import occurs.

The module knows this is undesirable — `supertable/logging.py:282-283` deliberately lazy-imports
`get_app_home` with the comment *"Lazy import to avoid import-time side effects from homedir"* —
but the eager block remains for the other callers, kept only to "preserve original import-time
behaviour".

### Fix proposal

Delete the eager `change_to_app_home` call at `homedir.py:86`; keep `_resolve_app_home()` lazy
(it is already memoised via `_resolved_home`). The library should never `chdir` its host. The
three `supertable/demo/*` entry points that genuinely want the behaviour already call
`change_to_app_home()` explicitly (`demo/medcenter/run.py:18` and siblings), so they are
unaffected. Anything that today depends on the CWD implicitly should take an absolute path
derived from `get_app_home()` instead — that is the same information without the global side
effect.

---

## F15 — The audit config declares four security defaults inverted, and silently adopts them on any settings failure

**SEVERITY: MEDIUM-HIGH** — **PROVEN**

`supertable/audit/logger.py:41-46` (declarations) vs `supertable/config/settings.py:391-397`,
with `supertable/audit/logger.py:70-71` as the path that makes it live.

```python
# audit/logger.py:41-46          settings.py declares:
hash_chain:   bool = False   #   SUPERTABLE_AUDIT_HASH_CHAIN    = True   (:391)
log_queries:  bool = False   #   SUPERTABLE_AUDIT_LOG_QUERIES   = True   (:392)
log_reads:    bool = False   #   SUPERTABLE_AUDIT_LOG_READS     = True   (:393)
siem_enabled: bool = False   #   SUPERTABLE_AUDIT_SIEM_ENABLED  = True   (:397)
```
```python
# audit/logger.py:55-71
@classmethod
def from_settings(cls) -> "AuditConfig":
    try:
        from supertable.config.settings import settings as _cfg
        return cls(..., hash_chain=getattr(_cfg, "SUPERTABLE_AUDIT_HASH_CHAIN", False), ...)
    except Exception:
        return cls()          # <- all four silently False, no log line
```

### What goes wrong

This is the "declared in one place, built with a different value" defect the audit was looking
for — but it lives in a **second, parallel default table** rather than in `settings.py`.
`AuditConfig` is the object the logger actually consults, and its declared defaults contradict
the settings module's for exactly the four fields that matter for integrity and visibility.

The bare `except Exception: return cls()` makes those declarations reachable. Any failure
importing or evaluating `supertable.config.settings` — a bad env value, a partially-initialised
module during interpreter start-up, a packaging problem — silently downgrades the audit
subsystem from "tamper-evident hash chain + query logging + read logging + SIEM export" to all
four off. Nothing is logged; the `except` swallows the reason. An operator who set nothing and
expected the documented defaults gets an audit trail with no chain, and no way to tell.

The `getattr(..., False)` fallbacks inside the `try` repeat the same inverted literals a third
time. They fire only if the attribute is missing, which is unlikely — the live path is the
`except`.

### Why guards miss it

`supertable/tests/test_settings_defaults.py` is a genuinely good guard (see the CLEAN section)
but it compares **`Settings` declaration ↔ `_build_settings` output** only. `AuditConfig` is a
different dataclass in a different package, so it is entirely outside the test's reach. Nothing
compares the two tables. The same blind spot covers the third default table in
`supertable/engine/engine_config.py:109-124`, which re-states engine defaults as strings and
resolves them with `os.getenv` directly (`engine_config.py:180`), bypassing the settings
singleton — those values happen to agree today, with one type divergence:
`SUPERTABLE_DUCKDB_IO_MULTIPLIER` is parsed by `_env_int` in settings and by `_to_float` in
`engine_config.py:213`, so `2.5` yields `3` in one and `2.5` in the other, in the same process.

### Fix proposal

- Delete the duplicated literals. `AuditConfig`'s field defaults should either be omitted
  (making the fields required, so `from_settings` is the only constructor) or set to the same
  values `settings.py` declares. Two tables of defaults for one concept will always drift; the
  only robust fix is to have one.
- Make `from_settings` fail loudly rather than returning an all-off config: if settings cannot
  be read, raise. An audit subsystem that silently turns itself off on an unexpected error is
  the worst available behaviour — it is indistinguishable from being configured off.
- Extend `test_settings_defaults.py`'s idea to the other two tables: assert that each
  `AuditConfig` field default equals the corresponding `Settings` default, and that
  `engine_config`'s spec defaults match theirs. The existing test already proves this style of
  check is cheap and effective; it just needs to cover the tables that currently escape it.

---

## F16 — `_env_str` discards the declared default for a whitespace-only env value

**SEVERITY: MEDIUM** — **PROVEN**

`supertable/config/settings.py:52-53`

```python
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default).strip()
```

### What goes wrong

`os.getenv` returns `"   "`, which is **truthy**, so `or default` never fires; `.strip()` then
reduces it to `""`. The declared default is lost and the field becomes empty — not the default,
and not the supplied value. This affects all 69 `str` fields. The sibling helpers do not have
the bug: `_env_int` (`:57`) and `_env_bool` (`:77`) strip *before* testing emptiness and
correctly fall back.

```
SUPERTABLE_AUDIT_FERNET_KEY="   "  ->  _env_str(...)                  -> ''
SUPERTABLE_HOME="  "               ->  _env_str(..., '~/supertable')  -> ''     # default LOST
X="   "                            ->  _env_int('X', 42)   -> 42   (correct)
X="   "                            ->  _env_bool('X', True) -> True (correct)
```

A trailing space in a Helm value, a k8s Secret with a newline artifact, or a hand-edited `.env`
is enough to trigger it, and the result is silently the *most permissive* value in several
security-relevant cases: `SUPERTABLE_AUDIT_FERNET_KEY` → `""` makes `crypto.encrypt_field`
return plaintext (`audit/crypto.py:38-40, 61-62`); `SUPERTABLE_SESSION_SECRET` → `""` is an
empty signing secret; `SUPERTABLE_API_KEY`/`SUPERTABLE_SUPERUSER_TOKEN` → `""` is an empty
credential.

The worst instance is `SUPERTABLE_HOME`, because `""` is not merely empty — `homedir.py:48` does
`os.path.abspath(os.path.expanduser(""))`, which resolves to **the current working directory**.
The data home silently becomes wherever the process happened to start.

### Why guards miss it

`TestEnvStr` (`config/tests/test_settings.py:104-118`) covers unset and empty-string, but not
whitespace-only. `test_strips_whitespace` (`:109-111`) asserts `"  bar  "` → `"bar"`, which
exercises the strip but never the case where stripping empties the value.

### Fix proposal

Strip first, then test, so the helper matches its own siblings:
`raw = (os.getenv(name) or "").strip(); return raw if raw else default`. Rationale: the three
numeric/bool helpers already establish that "blank after stripping means unset"; `_env_str` is
simply inconsistent with them. Add the whitespace-only case to `TestEnvStr`. Separately consider
rejecting a whitespace-only value for secret-bearing fields rather than defaulting — for a
credential, silently substituting the default is itself the wrong move.

---

## F17 — `SUPERTABLE_SHARE_PRESIGN_TTL` is declared and documented but nothing reads it

**SEVERITY: LOW-MEDIUM** — **PROVEN**

`supertable/config/settings.py:401` (declaration), `:665` (built) — zero readers.

### What goes wrong

```
settings.py:401  SUPERTABLE_SHARE_PRESIGN_TTL: int = 14400   # (seconds, default 4h)
settings.py:665  SUPERTABLE_SHARE_PRESIGN_TTL=_env_int("SUPERTABLE_SHARE_PRESIGN_TTL", 14400),
```

Every presign implementation hardcodes a different value and never consults the setting:

```
storage/minio_storage.py:156      def presign(self, key, expiry_seconds: int = 3600)
storage/s3_storage.py:126         def presign(self, key, expiry_seconds: int = 3600)
storage/azure_storage.py:140      def presign(self, key, expiry_seconds: int = 3600)
storage/storage_interface.py:190  def presign(self, key, expiry_seconds: int = 3600)
```

So the effective presigned-URL lifetime is **1 hour**, not the declared and documented 4 hours,
and an operator tightening `SUPERTABLE_SHARE_PRESIGN_TTL` changes nothing. A presigned URL is a
bearer capability — its lifetime is a security control, and this one is not wired to its knob.

This is the "unread config is ambiguous, not dead" case: the setting is not obsolete, it was
never connected. The hardcoded literal at the four call sites is the tell.

### Fix proposal

Wire it: default `expiry_seconds` to `settings.SUPERTABLE_SHARE_PRESIGN_TTL` at the four
implementations (or resolve it once in `storage_interface` and have the backends inherit),
keeping the explicit-argument override for callers that need a shorter-lived URL. Decide
deliberately whether 1 h or 4 h is the intended default and make the declaration, the docs and
the code agree on it — right now all three disagree pairwise.

---

## F18 — Permission resolution is uncached: 4-5 Redis round-trips per check, N+1 over tables

**SEVERITY: MEDIUM** — **PROVEN**

`supertable/rbac/access_control.py:95, 233` (fresh `RoleManager` per check),
`supertable/rbac/role_manager.py:61-95` (`_init_role_storage` on every construction),
`supertable/meta_reader.py:174-185` (the N+1).

### What goes wrong

Every permission check builds a new `RoleManager`, whose `__init__` unconditionally runs
`_init_role_storage()` — an `EXISTS` plus `rbac_get_superadmin_role_id`, which is an **`SMEMBERS`
over the superadmin role-type index** (`redis_catalog.py:832-835`) — before the actual
`HGET` (name→id) + `HGETALL` (role doc). Measured by counting commands on the live client:

```
1 x restrict_read_access only : 4 redis commands  (EXISTS, SMEMBERS, HGET, HGETALL)
1 x check_write_access  only  : 5 redis commands  (+ GET for the read-only guard)
1 x DataReader SELECT         : 13 redis commands total
```

So roughly **a third of a simple read's Redis traffic is permission resolution**, re-fetching a
document that is invariant for the whole request. The `SMEMBERS` is pure overhead: a set scan on
every check, only to confirm the bootstrap role exists.

`MetaReader.get_tables` (`meta_reader.py:174-185`) calls `check_meta_access` once **per table**,
each time repeating the whole sequence:

```
MetaReader.get_tables over 8 tables: 42 redis commands
  GET 9, EXISTS 8, SMEMBERS 8, HGET 8, HGETALL 8, SCAN 1
  -> 5.2 redis commands x 8 tables
```

That is a textbook N+1 over an invariant policy document, and it scales linearly with the
SuperTable's table count on a listing endpoint. Note also that its `except Exception`
(`meta_reader.py:183-184`) treats a Redis error as "no permission", so a flaky Redis silently
shortens the table list rather than erroring — fail-closed, but invisibly lossy.

### Fix proposal

- Resolve the role document **once per request** and pass it down. The cleanest shape: have
  `restrict_read_access` / `check_*_access` accept an optional pre-resolved role document (or a
  small `PolicyContext`), and have `MetaReader.get_tables` resolve once and loop over the cached
  `role_tables` map. That removes the N+1 entirely without introducing any cache-invalidation
  question, because the object does not outlive the request.
- Make `_init_role_storage` genuinely one-shot per process (a module-level set of
  `(org, super)` already initialised), so the `EXISTS`+`SMEMBERS` pair stops running on every
  check. The bootstrap is idempotent and guarded by a lock; re-verifying it per permission check
  buys nothing.
- If a longer-lived cache is wanted later, key it on the `rbac:role:meta` version counter that
  `_rbac_bump` (`redis_catalog.py:736-738`) already maintains — that is a one-command validity
  check rather than a TTL guess, and the bump already fires on every role write.

---

# Answers to the five specific questions

### 1. What happens when a role has NO policy at all — deny or allow?

**ALLOW EVERYTHING.** `RowColumnSecurity.prepare()` (`row_column_security.py:72-73`) rewrites an
empty `tables` map to `{"*": {"columns": ["*"], "filters": ["*"]}}` before it is persisted. A
`reader` role created with `tables={}` reads every column of every table in the SuperTable —
reproduced in F2. The same substitution turns an attempted revoke into a full grant (F1).

This is inconsistent with the level below it: an empty **column** list correctly denies
(`access_control.py:283-286`, verified — `columns: []` denies both `SELECT *` and a named
column). Only the table level inverts.

### 2. What happens when the RBAC lookup raises (Redis down, malformed payload) — deny or allow?

**The core role lookup fails CLOSED.** `restrict_read_access` and `_check_operation_access` wrap
nothing around `RoleManager(...)` / `_resolve_role`, so a Redis failure propagates out of
`DataReader.execute` and no rows are returned (verified: injected `RuntimeError("redis down")`
→ `RAISED RuntimeError`, no data). Malformed payloads are handled deliberately: a missing `role`
field or an unparseable `RoleType` both raise `PermissionError`
(`access_control.py:99-107, 236-243`), and a non-string role name resolves to "no such role"
rather than an `AttributeError` (`redis_catalog.py:840-844`, with a comment saying exactly why).
That part is well built.

**But three surrounding controls fail OPEN:**

- **Share row filters** — `data_reader.py:373-374` and `:376-377` swallow any exception and serve
  the table unfiltered (F5, reproduced: 2 rows → 4 rows). The same swallow also drops the
  deletion vector.
- **The read-only guard** — `access_control.py:135-138`, `except Exception: pass  # Never block
  on guard failures`. A catalog failure lets a write through to a read-only snapshot/replica.
  Lower impact than F5 (the real permission check still runs afterwards) but it is an explicit,
  commented fail-open. Related truthiness quirk in the same function: `root.get("read_only")` is
  evaluated for Python truthiness, so the *string* `"false"` blocks all writes while the integer
  `0` and `None` allow them — wrong in both directions, though the dangerous direction (an
  unrecognised truthy value) happens to fail closed.
- **Audit** — every emit is best-effort by explicit design (F10, F13); a failed permission-denial
  record is indistinguishable from no denial.

One caveat on the fail-closed path: it raises the *underlying* exception type, not
`PermissionError`. Callers that translate only `PermissionError` to 403 will surface a 500. That
is the safe direction, but worth knowing.

### 3. Is "superadmin" a hardcoded bypass, and is the name reserved?

**Yes to the bypass; no to the reservation — and the gap is exploitable two ways.**

The bypass is on the role **type**, not the name: `access_control.py:248-250` returns `{}`
(no filtering) for `RoleType.SUPERADMIN` and `RoleType.ADMIN` before `tables` is read at all, and
`ROLE_PERMISSIONS[SUPERADMIN] = set(Permission)` (`permissions.py:21`) grants every write-side
permission.

Nothing reserves the name. `validate_role_name` (`redis_catalog.py:43-57`) checks only a
character class. Consequences, both reproduced:

- **Anyone who can call `create_role` can mint a superadmin** by passing `role: "superadmin"`;
  any `tables` restriction supplied alongside it is silently ignored (F4).
- **Creating a role *named* `SuperAdmin` returns the built-in superadmin role's id** instead of
  creating anything, because name matching is case-insensitive and `create_role` is silently
  idempotent on name collision (F3). The caller believes it holds a restricted reader id and is
  holding the omnipotent one.

Deletion *is* protected (`role_manager.py:222-223` refuses to delete a `superadmin` role), and
the default superuser is protected (`user_manager.py:174-175`) — so the guards that exist are on
removal, not on creation or aliasing, which is the wrong half.

### 4. Are column masks applied to SELECT *, a qualified reference, and through a CTE?

**Yes — all three, correctly. This is the strongest part of the RBAC implementation.** Verified
against a role limited to `columns: ["id"]` on a 4-column table:

```
SELECT * FROM sup2.employees                                  -> cols=['id']   rows=4
SELECT e.* FROM sup2.employees e                              -> cols=['id']   rows=4
WITH x AS (SELECT * FROM sup2.employees) SELECT * FROM x      -> cols=['id']   rows=4
SELECT * FROM (SELECT * FROM sup2.employees) s                -> cols=['id']   rows=4
SELECT id FROM sup2.employees                                 -> cols=['id']   rows=4
SELECT salary FROM sup2.employees                             -> DENIED
SELECT max(salary) FROM sup2.employees                        -> DENIED
```

The two-phase design is sound: Phase 1 validates the merged, CTE-free physical column set
(`access_control.py:271-299`), and Phase 2 builds a per-alias view
(`access_control.py:308-338`) that the engine stacks on top of the tombstone view
(`engine/duckdb.py:230-240` → `engine_common.py:1195-1231`). Because the mask is a **view**, it
survives CTEs and subqueries structurally rather than by parser analysis. Row filters propagate
equally well (`region='EU'` yields 2 rows through a plain select, a CTE, and `count(*)`).

Two caveats worth recording:

- When the query uses `SELECT *`, `pt.columns` is empty and Phase 1 skips column validation
  **entirely, including columns referenced only in WHERE/ORDER BY**. Enforcement then rests
  solely on the view. That is safe in effect — `SELECT * FROM employees WHERE salary > 150`
  fails — but it fails as a DuckDB `Binder Error` returned as `Status.ERROR`, not as a
  `PermissionError`. Callers translating `PermissionError` to 403 will report that denial as a
  query error, which is a confusing (and slightly leaky) way to say "forbidden".
- The mask is bypassed entirely by `SHOW STATS`, which is F7.

### 5. Can a share's row_filter be empty/None and silently mean "no filter" where it should mean "deny"?

**Yes — and worse, so can a correctly-intended filter of the wrong type, and so can any
exception.** `data_reader.py:360` gates on `if share_row_filter and isinstance(share_row_filter, str)`.
Reproduced with a share filter installed on the catalog leaf payload:

```
_row_filter="region = 'EU'"                        -> rows=2   (enforced)
_row_filter=''                                     -> rows=4   (no filter)
_row_filter=None                                   -> rows=4   (no filter)
_row_filter=0                                      -> rows=4   (no filter)
_row_filter={'region': {'operation': '=', ...}}    -> rows=4   (no filter)
get_leaf raises                                    -> rows=4   (no filter)
```

The dict case is the most dangerous: it is the **exact JSON filter format role filters use**
(`row_column_security.py:24-26`), so a provider writing the share filter in the house format
produces an unrestricted share with no error anywhere. And the sibling module that fingerprints
this same predicate documents the correct stance in so many words —
*"the one direction this must never fail in"* (`odata/policy.py:170-172`) — while the enforcement
path does the opposite. Zero tests reference `_row_filter`. Full detail and fix in F5.

The role-level equivalent has the same shape: a `filters` document that renders to empty SQL is
silently no filter (F6).

---

# What I checked and found CLEAN

The column-masking and row-filtering **engine** is genuinely solid and I failed to break it.
Masks and row predicates are enforced as stacked SQL views rather than by parser rewriting, so
they hold through `SELECT *`, `t.*`, CTEs, derived tables and aggregates alike (question 4);
denied columns are rejected whether they appear in the projection, an aggregate, or a join
predicate; and `columns: []` correctly denies rather than defaulting open. The two-phase
validate-then-view split, with Phase 1 running over `get_physical_tables()` (merged and
CTE-free) and Phase 2 keyed per alias, is the right decomposition and the CTE-alias skip at
`access_control.py:310-315` is correctly justified by the transitive Phase-1 check.
`FilterBuilder`'s SQL sanitisation is careful and appears sound: `_sanitize_column`
(`filter_builder.py:13-18`) enforces a strict identifier class, `_sanitize_operation` uses a
closed allow-list of 15 operators, and `_sanitize_value` both doubles single quotes and rejects
`;`, `--`, `/*`, `*/` — I could not construct an injecting role filter through it. Quality's
`_quote` (`checker.py:49-50`) doubles embedded double-quotes correctly, so column names are not
an injection vector there either (only `threshold`/`expected_values`/`custom_sql` are, per F8).
The role-type permission matrix (`permissions.py:20-26`) is consistent — `META` cannot read data,
`READER` cannot write, `WRITER` cannot control — and `has_permission` denies on an unknown type
by returning an empty set rather than defaulting open. Name-validation is present, applied at
both the manager and catalog layers as deliberate defense-in-depth
(`role_manager.py:99-104`, `redis_catalog.py:754-761, 776-783`), and `rbac_get_role_id_by_name`
explicitly converts a non-string name into a denial rather than an `AttributeError`. Role and
user identities are stable UUIDs with mutable names layered on top, which is the right model and
is well tested. Deletion paths are protected (superadmin role, default superuser) and
`rbac_delete_role` strips the role from all users atomically in Lua.

On **config**, `supertable/config/settings.py` itself came back clean on every axis I could
mechanise: 145 declared fields, 145 built, zero default mismatches, zero env-var-name mismatches
(the scheme is 1:1 literal, no prefix), zero fields declared-but-never-built or
built-but-never-declared. The only two runtime divergences are `SUPERTABLE_API_HOST` and
`XDG_CONFIG_HOME`, both deliberate computed fallbacks. `Settings` is frozen, and the `_env_int` /
`_env_float` / `_env_bool` helpers are well tested across the whole fallback chain including
garbage input (only `_env_str` is defective — F16).

**I was wrong on first pass about the defaults test and want to record the correction**, because
it is the single best-built thing in the audited scope. I initially concluded no test enforces
"declared default == built default", having searched only `supertable/config/tests/`. There *is*
one, at `supertable/tests/test_settings_defaults.py`, and it is close to exemplary: it enumerates
`dataclasses.fields(Settings)` **dynamically** (so it cannot drift as fields are added), runs the
probe in a **subprocess** with `cwd` set to a fresh tempdir and a three-variable environment, so
neither the repo `.env` nor the developer's ambient env can pollute the comparison, and it keeps
a two-entry `_INTENTIONAL` allow-list with a written reason per entry. It then **guards the
guard**: `test_intentional_exceptions_are_still_divergent` fails if a computed default is ever
replaced by a literal, so a stale exemption cannot start masking a real contradiction. My own
independent AST and runtime measurements agreed with its `_INTENTIONAL` set exactly. The
hand-enumerated 12-field `test_documented_defaults` in `config/tests/test_settings.py:223-237` is
a separate, complementary intent-pinning test, not a failed attempt at this one. The real gap is
narrower than I first claimed and is recorded as F15: the test covers the `Settings` table only,
so the *second* default table in `audit/logger.py` and the *third* in `engine/engine_config.py`
escape it.

Security-relevant defaults are a mixed but mostly defensible picture:
`SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=False` and the three presign/write-probe switches are
correctly restrictive; `SUPERTABLE_AUDIT_ENABLED=False` makes auditing opt-in (defensible for a
library, though `docs/02_configuration.md:240` states `true`, so the docs and the code disagree);
`STORAGE_USE_SSL=False` and an empty `SUPERTABLE_AUDIT_FERNET_KEY` are the two I would change
first. Note that the auth, rate-limit and host-allowlist fields (`SUPERTABLE_AUTH_MODE`,
`SUPERTABLE_API_KEY`, `SUPERTABLE_ALLOWED_HOSTS`, `FORWARDED_ALLOW_IPS`,
`SUPERTABLE_API_RATE_LIMIT_*`) have no readers in this package at all — they are presumably
enforced by the out-of-tree server, so I have not scored them as defects here, but they are
inert if anyone expects this library to honour them.

On **audit**, the event schema and category taxonomy are thorough and well tested
(`test_events.py` pins the snake_case action contract), retention's legal-hold check correctly
fails *closed* (`retention.py:100-101` returns `True` if every lookup fails), and the hash
primitives themselves are correct and order-independent — `compute_content_hash` genuinely does
detect an edited event field, proven by `supertable/tests/test_defect_fixes.py:220-234`. The
problem is not the primitives but that the shipped verifier never invokes them (F9).

On **odata**, `policy.py` is the best-reasoned module in the audited scope: the fingerprints are
canonicalised before hashing (so reordering a grant is not a false conflict), versioned
(`_FINGERPRINT_VERSION`) so old and new material can never compare equal, cover role policy /
share filters / column masks together, and `_share_row_filters` deliberately propagates rather
than degrading. My only criticisms of it are that it resolves policy a second time per page
(`policy.py:99` re-runs `restrict_read_access`, compounding F18) and that its correct
fail-closed stance is contradicted by the reader it is supposed to be describing (F5).

I found nothing reportable in **`monitoring_writer.py`**: it is a bounded-queue, best-effort
metrics path with per-organization key scoping (`_MonitorKey`, `monitoring_writer.py:145-157`),
no permission decisions, and no cross-tenant key construction. Its swallowed exceptions are
appropriate for telemetry and do not gate access.

Finally, I tried and failed to break several plausible candidates: the `"*"` wildcard does not
match a *column* name (only the literal `["*"]` list is special-cased, so a column literally
named `*` cannot be smuggled in); role and user name lookups are consistently case-insensitive
on both read and write; `restrict_read_access` correctly denies when a physical table has no
entry and no `"*"` default; the read path's ordering comment at `data_reader.py:244-256` is
accurate and the existence pre-flight genuinely does run before RBAC can bootstrap catalog
scaffolding; and a Redis outage during the core role lookup denies rather than admits.
