# 11 -- Role-Based Access Control (RBAC)

SuperTable implements a full RBAC subsystem that enforces who can read, write,
or administrate data -- down to individual rows and columns.  The system is
designed for multi-tenant environments where compliance frameworks (GDPR,
HIPAA, SOX) demand fine-grained, auditable access control.

---

## 11.1  Permission Model

Permissions are defined in `supertable.rbac.permissions` as an `enum.Enum`:

| Permission | `auto()` value | Meaning |
|------------|---------------|---------|
| `CONTROL`  | 1             | Destructive DDL -- DROP TABLE, TRUNCATE, etc. |
| `CREATE`   | 2             | Create new tables or staging areas. |
| `WRITE`    | 3             | INSERT / UPDATE / DELETE on data rows. |
| `READ`     | 4             | SELECT -- query data. |
| `META`     | 5             | Read-only metadata and statistics. |

The helper function `has_permission(role_type, permission)` checks whether a
given `RoleType` includes the requested `Permission` by looking it up in the
static `ROLE_PERMISSIONS` map.

---

## 11.2  Role Types

Role types are the coarse-grained privilege tiers.  Each maps to a fixed set
of permissions:

| RoleType      | Enum value     | Permissions granted | Description |
|---------------|----------------|---------------------|-------------|
| `SUPERADMIN`  | `"superadmin"` | ALL (CONTROL, CREATE, WRITE, READ, META) | Unrestricted bootstrap role; bypasses table, row, and column policy. |
| `ADMIN`       | `"admin"`      | ALL (CONTROL, CREATE, WRITE, READ, META) | Has every operation permission, but remains subject to its table, row, and column policy. |
| `WRITER`      | `"writer"`     | META, READ, WRITE   | Read and mutate data; no DDL. |
| `READER`      | `"reader"`     | META, READ          | Read-only with row/column security applied. |
| `META`        | `"meta"`       | META                | Statistical / metadata access only. |

The `ROLE_PERMISSIONS` dict in `permissions.py` encodes this matrix:

```python
ROLE_PERMISSIONS = {
    RoleType.SUPERADMIN: set(Permission),
    RoleType.ADMIN:      set(Permission),
    RoleType.WRITER:     {Permission.META, Permission.READ, Permission.WRITE},
    RoleType.READER:     {Permission.META, Permission.READ},
    RoleType.META:       {Permission.META},
}
```

---

## 11.3  Role CRUD -- `RoleManager`

**Module:** `supertable.rbac.role_manager`

`RoleManager` is the business-logic layer for role lifecycle operations.
It is scoped to a `(super_name, organization)` pair and backed by a
`RedisCatalog` instance.

### 11.3.1  Initialisation

```python
RoleManager(super_name: str, organization: str, redis_catalog: Optional[RedisCatalog] = None)
```

On construction, `_init_role_storage()` runs a fast-path check: if the Redis
meta key `supertable:{org}:lakes:{sup}:rbac:roles:meta` already exists and a
superadmin role is present, initialisation is skipped entirely (avoiding 2-3
Redis round-trips). Otherwise, the namespace is validated without creating an
unaudited revision key, a distributed lock (`acquire_simple_lock`) is taken,
and the default **superadmin** role is created. That first role mutation
creates revision metadata and its SYSTEM bootstrap audit record in the same
Redis transaction:

```python
{
    "role": "superadmin",
    "role_name": "superadmin",
    "tables": {"*": {"columns": ["*"], "filters": ["*"]}}
}
```

### 11.3.2  `create_role(data: dict) -> str`

Creates a new role and returns its stable UUID (`role_id`).

* `data["role"]` -- a `RoleType` string (e.g. `"reader"`).
* `data["tables"]` -- per-table permission definitions (see Section 11.5).
* `data["role_name"]` -- optional; must match
  `^[A-Za-z_][A-Za-z0-9_\-. ]{0,126}$`.  A same-name create returns the
  existing `role_id` only when the role type and canonical table policy are
  identical.  Different content is rejected and must use `update_role()`
  explicitly.

Internally, a `RowColumnSecurity` value object is built, `prepare()` is
called (validates, normalises columns, computes `content_hash`), and the
resulting document is persisted via `RedisCatalog.rbac_create_role()`.

### 11.3.3  `update_role(role_id: str, data: dict) -> str`

Updates a role in-place.  Returns the new `content_hash`.

* The `role_id` remains stable -- all users referencing this role instantly
  see the new permissions.
* If `role_name` changes, uniqueness is validated and the
  `name_to_id` mapping in Redis is updated atomically.
* The update and its privileged audit record run in one isolated Redis Lua
  boundary.  The script preflights every supported key type, counter, CAS
  predicate, and audit envelope before `XADD` and the deterministic state
  writes.  Deploy it with the complete required Redis command ACL and the
  no-eviction/persistence settings in the audit runbook; Redis Lua does not
  provide general rollback for an externally induced post-append runtime
  error such as an intentionally incomplete command ACL.

### 11.3.4  `delete_role(role_id: str) -> bool`

Deletes a role and atomically strips it from all users who hold it.

* The **superadmin** role cannot be deleted -- attempting to do so raises
  `ValueError`.
* A critical `role_delete` record is written to the mandatory privileged
  audit outbox in the same commit.  Its exact sidecar identifies every
  affected user and records per-user version, role-count, and duplicate
  occurrence changes without copying user documents or role policies.
* `affected_count` counts users; `cascade_assignment_count` counts removed
  role occurrences.  The atomic operation checks the authoritative user-index
  cardinality before scanning and fails before any write when it exceeds
  10,000 users, so an operator must migrate larger tenants through a bounded
  revocation workflow before deleting the role.  Evidence is never truncated.

### 11.3.5  Lookup Methods

| Method | Signature | Notes |
|--------|-----------|-------|
| `get_role` | `(role_id: str) -> Dict` | Returns `{}` if not found. |
| `get_role_by_name` | `(role_name: str) -> Dict` | Case-insensitive lookup via `name_to_id` hash. |
| `list_roles` | `() -> List[Dict]` | All role documents for this SuperTable. |
| `get_roles_by_type` | `(role_type: str) -> List[Dict]` | Filter by type (e.g. `"reader"`). |
| `get_superadmin_role_id` | `() -> Optional[str]` | First superadmin role ID. |

---

## 11.4  User CRUD -- `UserManager`

**Module:** `supertable.rbac.user_manager`

`UserManager` manages RBAC user entities.  Each user has a stable UUID
(`user_id`) and a mutable, case-insensitive unique `username`.

### 11.4.1  Initialisation

On construction, `_init_user_storage()` ensures the default **superuser**
account exists and holds the superadmin role.  If the superuser account exists
but lacks the superadmin role (e.g. after a role reset), the role is
automatically re-attached. Empty user-namespace initialization is
validation-only; creating the bootstrap superuser creates the user revision
head and SYSTEM audit record in the same Redis transaction.

### 11.4.2  `create_user(data: dict) -> str`

Creates a user and returns its `user_id`.

* `data["username"]` is required.
* `data["roles"]` -- list of `role_id` strings; each is validated for
  existence.
* A same-name create returns the existing `user_id` only when the canonical
  role assignments are identical.  Conflicting assignments are rejected and
  must use `modify_user()` explicitly.
* The user document includes `created_ms` and `modified_ms` timestamps.

### 11.4.3  `modify_user(user_id: str, data: dict) -> None`

Modifiable fields: `username`, `display_name`, `roles`.

* Username, display name, and role changes commit together with the
  case-insensitive name mapping in one atomic operation.
* Role assignments are validated again inside that atomic commit, so a
  concurrently deleted role cannot leave an orphan assignment.

### 11.4.4  `delete_user(user_id: str) -> None`

Deletes a user.  The default **superuser** cannot be deleted.

### 11.4.5  Role Assignment Helpers

| Method | Signature | Description |
|--------|-----------|-------------|
| `add_role` | `(user_id, role_id) -> bool` | Atomic, idempotent role grant. |
| `remove_role` | `(user_id, role_id) -> bool` | Atomic role revocation. |
| `get_or_create_default_user` | `() -> Optional[str]` | Return or create the superuser. |

---

## 11.5  Row-Level Security (SQL WHERE Filters)

Row-level security is implemented through JSON filter definitions attached to
each table entry within a role.  The `FilterBuilder` class
(`supertable.rbac.filter_builder`) converts these JSON structures into
safe SQL `WHERE` clauses.

### 11.5.1  Per-Table Role Definition Format

```json
{
    "role": "reader",
    "role_name": "broad_except_sensitive",
    "tables": {
        "*": {
            "columns": ["*"],
            "filters": ["*"]
        },
        "account": {
            "columns": ["*"],
            "exclude_columns": ["name"],
            "filters": ["*"]
        },
        "card": {
            "columns": ["*"],
            "exclude_columns": ["pan", "cvv"],
            "filters": ["*"]
        },
        "archived_accounts": {
            "access": "deny"
        }
    }
}
```

Policy selection and precedence are deliberate:

* `"*"` as a table key is the fallback for a table without an exact entry.
* An exact table entry **replaces** the wildcard entry; fields are not merged.
  Matching is case-insensitive.  In the example, `account.name`, `card.pan`,
  and `card.cvv` are hidden, while `archived_accounts` is denied even though
  the wildcard allows tables.
* `"access": "deny"` denies every operation on that table.  A denied entry
  cannot also contain `columns`, `exclude_columns`, or `filters`.
* Conversely, an exact `"access": "allow"` entry can carve an allowed table
  out of a wildcard deny.  Because exact entries replace the fallback, the
  exact entry must state every row and column restriction it needs.
* `"columns": ["*"]` includes all columns, before exclusions are applied.
* `"exclude_columns": ["pan", "cvv"]` removes those columns from the
  inclusion set.  See Section 11.6 for query, metadata, and write behavior.
* `"filters": ["*"]` means no row-level predicate (all authorized rows are
  visible).  Omitted `columns`, `exclude_columns`, and `filters` default to
  `["*"]`, `[]`, and `["*"]`, respectively, on an allowed entry.

For a row-filtered table, replace `filters: ["*"]` with a filter definition:

```json
{
    "orders": {
        "columns": ["order_id", "amount", "status"],
        "filters": [
            {"status": {"operation": "=", "type": "value", "value": "completed"}}
        ]
    }
}
```

### 11.5.2  FilterBuilder

```python
class FilterBuilder:
    def __init__(self, table_name: str, columns: list, role_info: dict): ...
    def build_filter_query(self, table_name, columns, filters) -> str: ...
    def json_to_sql_clause(self, json_obj) -> str: ...
```

`FilterBuilder` produces a complete `SELECT ... FROM ... WHERE ...` statement
that the query engine wraps as a filtered view.  `table_name` must be one bare
SimpleTable identifier (1--128 characters matching
`^[A-Za-z_][A-Za-z0-9_]*$`).  It is stripped and canonically quoted using the
builder's configured identifier quote.  Qualified, pre-quoted, aliased, and
other raw SQL table expressions are rejected.

**Filter JSON grammar:**

* **Simple predicate:** `{"column_name": {"operation": "=", "type": "value", "value": "x"}}`
* **Range predicate:** `{"column_name": {"range": [{"operation": ">=", "type": "value", "value": "10"}, ...]}}`
* **Logical combinators:** `{"AND": [...]}`, `{"OR": [...]}`, `{"NOT": {...}}`
* **Null check:** `{"type": "null"}` with `"IS"` / `"IS NOT"` operations.
* **Pattern matching:** `LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE` with optional `ESCAPE` clause.

**Allowed SQL operations** (validated by `_sanitize_operation`):

```
=, !=, <>, <, >, <=, >=,
LIKE, NOT LIKE, ILIKE, NOT ILIKE,
IN, NOT IN, IS, IS NOT,
BETWEEN, NOT BETWEEN
```

### 11.5.3  SQL Injection Prevention

The filter builder applies four layers of sanitisation:

1. **`_sanitize_table_identifier(table_name)`** -- accepts one bare SimpleTable
   identifier and canonically quotes it; SQL fragments and qualified names are
   rejected.
2. **`_sanitize_column(col)`** -- validates against `^[A-Za-z_][A-Za-z0-9_]*$`
   and wraps in the builder's configured identifier quote.
3. **`_sanitize_value(val)`** -- escapes single quotes (SQL standard doubling)
   and blocks `;`, `--`, `/*`, `*/`.
4. **`_sanitize_operation(op)`** -- whitelist-only; rejects anything not in
   `_ALLOWED_OPS`.

---

## 11.6  Column-Level Security

Column-level security is the second axis of data filtering.  Each allowed
table entry can specify an inclusion list (`columns`) and a deny overlay
(`exclude_columns`):

* `["*"]` -- all columns are visible (unrestricted).
* `["order_id", "amount"]` -- only these columns can be queried.
* `"columns": ["*"], "exclude_columns": ["cvv"]` -- all columns except
  `cvv` are visible.

The effective set is **included columns minus excluded columns**.
`exclude_columns` always wins, including when the same name also appears in
an explicit `columns` list.  Table and column policy matching is
case-insensitive, so excluding `CVV` also excludes a schema column named
`cvv`.  Case-colliding definitions such as `cvv` and `CVV` in the same policy,
and schemas containing case-colliding column names, are rejected rather than
resolved ambiguously.

With `columns: ["*"]`, a new schema column becomes visible automatically
unless its name is excluded.  An exclusion may name a column that does not
exist yet; it remains a dormant deny and takes effect if that column appears
in a later schema.  This is useful for preventing a known sensitive name from
becoming visible through schema evolution.

When the query engine processes a SQL request, `restrict_read_access()` in
`access_control.py` validates that every column referenced in the query
(including SELECT, WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY, window
expressions, CTEs, and subqueries) is in the effective set.  An explicit
reference to a denied column raises `PermissionError`; `SELECT *` expands only
to visible columns.  If no readable column remains, the query fails closed.
An excluded column may still be used internally by a trusted row-filter
predicate without becoming selectable or appearing in the result.

The `format_column_list()` helper in `filter_builder.py` produces the SELECT
projection:

```python
def format_column_list(columns, quote_char='"'):
    if not isinstance(columns, list):
        raise ValueError("RBAC columns must be a list")
    if columns == ["*"]:
        return "*"
    if not columns or "*" in columns:
        raise ValueError("RBAC wildcard must be the only selected column")
    quoted = [_sanitize_column(column, quote_char) for column in columns]
    return ",".join(f'{column} as {column}' for column in quoted)
```

### 11.6.1  Metadata and Mutation Behavior

Column and table denies also apply outside ordinary `SELECT` execution:

* Table-listing APIs omit tables whose effective policy is denied.
* Schema, snapshot/resource statistics, aggregate SuperTable metadata, and
  `SHOW STATS` expose only effective visible columns.  Internal system columns
  are hidden as well.  Metadata caches include the effective role policy in
  their identity so one role's cached view is not reused for another policy.
* A write to an existing table requires `WRITE`; creating a missing table
  requires `CREATE`.  The incoming payload and conflict-key column names are
  checked case-insensitively before mutation.  Because writes publish the
  newest logical schema and row filters are not yet evaluated against old and
  new rows, a policy with an inclusion list, `exclude_columns`, or a row filter
  is deliberately mutation-read-only.  Mutation requires the effective entry
  to have `columns: ["*"]`, no exclusions, and `filters: ["*"]`.
  `access: "deny"` blocks every path.
* Table deletion and other destructive table-level operations require
  `CONTROL` plus an unrestricted effective table entry.  Deleting an entire
  SuperTable namespace is SUPERADMIN-only because it transitively deletes all
  child tables.  The data namespace cleanup retains its RBAC role/user keys:
  those security records can only be removed through their dedicated
  mandatory-audit mutations, and recreating a SuperTable cannot silently reset
  its prior access policy.  Internal compaction requires `WRITE` and an
  unrestricted entry, but does not introduce user-supplied logical columns.

Row filters remain read predicates.  A row-filtered `WRITER` or `ADMIN` may
read its filtered view but cannot write, create, compact, or delete through
that scoped entry.  A future mutation-specific policy must certify predicates
against both prior and replacement rows before this restriction can be
relaxed.

---

## 11.7  RowColumnSecurity Value Object

**Module:** `supertable.rbac.row_column_security`

```python
class RowColumnSecurity:
    def __init__(self, role: str, tables: Optional[Dict[str, dict]] = None,
                 role_name: Optional[str] = None): ...
    def prepare(self) -> None: ...
    def to_json(self) -> dict: ...
    def sort_all(self) -> None: ...
    def create_content_hash(self) -> None: ...
```

This value object validates and normalises role permission data:

* `prepare()` -- validates table entries, fills in defaults (`["*"]` for
  missing `columns`/`filters`, `[]` for missing `exclude_columns`), sorts and
  deduplicates exact column names, and computes an MD5 `content_hash`.
* `access` accepts only `"allow"` or `"deny"`; a denied entry cannot contain
  contradictory row/column fields.
* Unknown fields, malformed lists, mixed wildcards, case-colliding names, and
  over-budget policy/filter documents are rejected at the role persistence
  boundary.  Read-time validation also fails closed for malformed legacy or
  directly written documents.
* The `content_hash` is used for change detection and logging; it is **not**
  the role identity (that is the UUID `role_id`).

---

## 11.8  Access Control Enforcement -- `access_control.py`

**Module:** `supertable.rbac.access_control`

This module provides the enforcement functions called by API handlers and the
query engine.

### 11.8.1  Operation-Scoped Checks

| Function | Permission Required | Description |
|----------|-------------------|-------------|
| `check_control_access(super_name, org, role_name, table_name)` | `CONTROL` | DDL operations (DROP, TRUNCATE). |
| `check_write_access(super_name, org, role_name, table_name, columns=None)` | `WRITE` | Mutate an existing table; optional incoming columns are policy-checked. |
| `check_create_access(super_name, org, role_name, table_name, columns=None)` | `CREATE` | Create a missing table; optional incoming columns are policy-checked. |
| `check_meta_access(super_name, org, role_name, table_name)` | `META` | Read table metadata through guarded paths. |

Each function calls `_check_operation_access()`, which resolves the role,
validates the
   `RoleType`, checks the permission matrix, resolves the exact-or-wildcard
   table entry, applies `access: "deny"`, and validates supplied columns.
The mutation checks (`CONTROL`, `WRITE`, and `CREATE`) first call
`_check_readonly_guard()` to block changes to read-only SuperTables (snapshot
clones, replicas, and locked instances). `META` is a read operation and does
not invoke that mutation guard.

### 11.8.2  Read Access with Filtering

```python
def restrict_read_access(
    super_name: str,
    organization: str,
    role_name: str,
    tables: List[TableDefinition],
    physical_tables: List[TableDefinition],
) -> Dict[str, RbacViewDef]:
```

This is the core read-path enforcement function:

1. Resolves the role and validates `READ` permission.
2. **SUPERADMIN** returns `{}` (no filtering).  `ADMIN` has all coarse
   permissions but still receives its configured table, row, and column
   restrictions.
3. For other roles, validates every physical table and column against the
   role's per-table definitions.
4. Returns a dict of `{alias: RbacViewDef}` for each table alias that
   requires RBAC filtering.

### 11.8.3  RbacViewDef

```python
@dataclass
class RbacViewDef:
    allowed_columns: List[str] = field(default_factory=lambda: ["*"])
    where_clause: str = ""
    excluded_columns: List[str] = field(default_factory=list)
    filter_spec: object = None
```

This dataclass is produced by `restrict_read_access()` and consumed by query
executors to create a filtered view on top of each reflection table.  The
`where_clause` is the SQL predicate generated by `FilterBuilder`.
`excluded_columns` carries the deny overlay to the executor, while
`filter_spec` retains the validated structured predicate for engines that must
render the filter in their own SQL dialect.

### 11.8.4  Read-Only Guard

`_check_readonly_guard()` inspects the SuperTable root metadata for a
`read_only` flag and blocks mutations with context-specific error messages:

* `"live replica"` -- `clone_type == "replica"`
* `"read-only snapshot clone"` -- `clone_type == "readonly"`
* `"read-only clone"` -- has `cloned_from` attribute
* `"locked"` -- generic read-only lock

### 11.8.5  Role Resolution

`_resolve_role()` fetches a role by name and checks two conditions:

1. The role must exist (otherwise `PermissionError`).
2. The role must be enabled -- the `enabled` field supports `"false"`, `"0"`,
   `False`, and missing (defaults to enabled for backward compatibility).

### 11.8.6  Trusted `role_name` Boundary

The data, metadata, and mutation APIs receive `role_name` as an explicit
argument.  Core enforcement resolves that name in the target
`(organization, SuperTable)` namespace, but it does **not** authenticate an
end user or prove that the user was assigned that role.  Therefore
`role_name` is a trusted control-plane assertion, not an end-user-selectable
request parameter.

Before calling a SuperTable API, the service boundary must authenticate the
caller, load the caller's assignments, verify that the selected role is among
them, and pass only that verified role name.  Never forward a client-supplied
role name directly; doing so could let a caller select `superadmin` or another
more privileged role.  Assignment changes and privileged role management
must be protected by the same control-plane authorization boundary.

For a query spanning multiple SuperTables, the same role name is resolved
independently in every referenced namespace.  The role must exist, be enabled,
and authorize the referenced table in each SuperTable; a grant in the default
SuperTable does not authorize another one.

### 11.8.7  Mixed-Version Rollout Safety

`access` and `exclude_columns` are newer policy fields.  Older query,
metadata, mutation, or role-management processes may ignore them and interpret
the remaining/default policy as an allow.  The persisted role document does
not currently provide a runtime policy-version fence that can make such an
old process reject the new syntax.  Consequently, exclusion policies are not
safe to activate while legacy binaries can still serve traffic or rewrite
role documents.

Use a coordinated rollout:

1. Deploy exclusion-aware binaries to every reader, writer, metadata worker,
   and role-policy writer.
2. Drain old processes and verify the deployed capability before persisting
   any `access: "deny"` or `exclude_columns` policy.
3. Activate the new policies only after that fleet-wide gate succeeds.
4. Do not roll back to a binary that ignores these fields while such policies
   remain active.  First remove it from traffic or replace the policies with
   an equivalent legacy allowlist under controlled conditions.

A deployment platform may automate step 2 with a fleet capability/feature
gate, but the gate must cover every enforcement and policy-writing process;
checking only the API tier is insufficient.

---

## 11.9  Programmatic Management

`RoleManager` and `UserManager` (in `supertable/rbac/`) expose full CRUD
operations from Python code:

```python
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

rm = RoleManager(super_name=super_name, organization=organization)
rm.create_role({"role": "reader", "tables": {"facts": {"columns": ["*"], "filters": ["*"]}}})
rm.list_roles()
rm.get_role(role_id)
rm.update_role(role_id, {...})
rm.delete_role(role_id)

um = UserManager(super_name=super_name, organization=organization)
um.create_user({"username": "alice", "roles": [role_id]})
um.list_users()
um.get_user(user_id)
um.add_role(user_id, role_id)
um.modify_user(user_id, {"username": "alice2"})
um.delete_user(user_id)
um.get_or_create_default_user()
```

The deprecated `UserManager.remove_role_from_users()` bulk mutator is
intentionally unsupported because independent per-user commits cannot provide
one truthful aggregate outcome. It records a durable denied attempt without
changing assignments. Use `RoleManager.delete_role()` for its bounded atomic
cascade, or `remove_role()` for one user.

Every mutation accepts a keyword-only `action_context`.  Supply an immutable
actor/request context at the service boundary so the durable record contains
the administrator, session, correlation ID, reason, and change ticket:

```python
from supertable.audit import PrivilegedActionContext

ctx = PrivilegedActionContext(
    actor_type="user",
    actor_id="admin-42",
    username="alice",
    correlation_id="req-7f3",
    session_id="session-19",
    reason="Quarterly access review",
    ticket_id="IAM-2041",
)
rm.update_role(role_id, {"tables": new_policy}, action_context=ctx)
```

Calls that omit the context remain source-compatible but are explicitly
recorded as `system/legacy-unattributed` with `context_missing=true`; they are
never silently attributed to a human administrator.

Organization auth-token creation and deletion are stricter: their
keyword-only `action_context` is required and must resolve to a non-missing
actor. An explicitly missing or invalid context is durably denied and cannot
create or delete a token.

Valid role types: `superadmin`, `admin`, `writer`, `reader`, `meta`.

Role IDs and user IDs are 32-character hex strings matching `^[a-f0-9]{32}$`.

---

## 11.10  Redis Data Model

All RBAC state lives in Redis under a structured key namespace:

| Key Pattern | Type | Content |
|-------------|------|---------|
| `supertable:{org}:lakes:{sup}:rbac:roles:meta` | Hash | Version + last_updated_ms + initialized; first created by an audited role mutation. |
| `supertable:{org}:lakes:{sup}:rbac:roles:index` | Set | All `role_id` values. |
| `supertable:{org}:lakes:{sup}:rbac:roles:doc:{role_id}` | Hash | Role document fields. |
| `supertable:{org}:lakes:{sup}:rbac:roles:name_to_id` | Hash | `role_name.lower()` to `role_id`. |
| `supertable:{org}:lakes:{sup}:rbac:roles:type:doc:{role_type}` | Set | Role IDs grouped by type. |
| `supertable:{org}:lakes:{sup}:rbac:users:meta` | Hash | Version + last_updated_ms + initialized; first created by an audited user mutation. |
| `supertable:{org}:lakes:{sup}:rbac:users:index` | Set | All `user_id` values. |
| `supertable:{org}:lakes:{sup}:rbac:users:doc:{user_id}` | Hash | User document fields. |
| `supertable:{org}:lakes:{sup}:rbac:users:name_to_id` | Hash | `username.lower()` to `user_id`. |

Every key is built by the matching helper in
`supertable/redis_keys.py` (`rbac_role_*`, `rbac_user_*`). The
`tests/test_redis_key_prefix.py` regression suite enforces that no
other module constructs these literals inline.

---

## 11.11  Compliance Context

### GDPR (General Data Protection Regulation)

* **Row-level security** enables data minimisation (Art. 5(1)(c)) by
  restricting which records a given analyst can see (e.g. only their region).
* **Column-level security** implements purpose limitation (Art. 5(1)(b)) --
  PII columns such as email, phone, or address can be hidden from roles that
  do not require them.
* The META role type provides statistical-only access, supporting
  pseudonymisation and aggregation-only use cases.

### HIPAA (Health Insurance Portability and Accountability Act)

* The permission model enforces the **Minimum Necessary Rule** -- users
  receive only the access required for their job function.
* Row-level filters can restrict access to patient records by department,
  facility, or care team.
* Every committed role, user, and role-assignment change has a mandatory,
  sequenced audit record in the same transaction (see
  [12 Audit](12_audit.md)).

### SOX (Sarbanes-Oxley)

* Separation of duties: `READER` cannot mutate data; `WRITER` cannot
  perform DDL; only `ADMIN`/`SUPERADMIN` can manage roles and users.
* The default superadmin role is protected from deletion, ensuring at least
  one administrative account always exists.
