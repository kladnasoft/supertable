# 11 -- Role-Based Access Control (RBAC)

SuperTable implements a full RBAC subsystem that enforces who can read, write,
or administrate data -- down to individual rows and columns.  The system is
designed for multi-tenant environments where compliance frameworks (GDPR,
HIPAA, SOX) demand fine-grained, auditable access control.

---

## 11.1  Permission Model

Permissions are defined in `supertable.rbac.permissions` as an `enum.Enum`:

| Permission | Meaning | Enforced by |
|------------|---------|-------------|
| `RBAC`     | Create / update / delete roles and users. | `check_rbac_access` |
| `CONTROL`  | Drop an entire SuperTable. | `check_control_access` |
| `WRITE`    | INSERT / UPDATE / DELETE rows, **create and drop a table**, configure limits, compact. | `check_write_access` |
| `READ`     | SELECT -- query data. | `restrict_read_access` |
| `META`     | Read-only metadata and statistics. | `check_meta_access` |

The helper function `has_permission(role_type, permission)` checks whether a
given `RoleType` includes the requested `Permission` by looking it up in the
static `ROLE_PERMISSIONS` map.

### There is no CREATE permission

A table is created by writing to a name that does not exist, so `WRITE` is
what creates it. A role trusted to fill a table is also trusted to drop it,
so `WRITE` covers the whole table lifecycle. What bounds that power is the
role's **table grants**, not the permission tier: a writer granted
`{"orders": ...}` can only ever create or drop `orders`.

`CONTROL` exists for exactly one operation -- dropping the SuperTable --
because that is the only act which destroys things the writer never created:
every other table in the lake, and the RBAC configuration itself.

Earlier versions declared a `CREATE` permission and documented it as gating
table creation for the admin tiers only. It was never checked anywhere, and
the behaviour it described was never the behaviour: writers have always been
able to create tables. The enum member has been removed rather than enforced,
because enforcing it would have broken the intended model.

### Creating a SuperTable cannot be gated

`SuperTable(name, org)` creates the lake if it does not exist, and takes no
`role_name` at all. This is deliberate and unavoidable: constructing a
SuperTable is what bootstraps its `superadmin` role, so requiring a role to
authorise the call is unsatisfiable for the first call -- the role that would
grant permission is created *by* the operation needing it.

Deciding **whether a given tenant may create a SuperTable at all** is therefore
the host application's responsibility; this library cannot express it.
Deleting one is a different matter and *is* gated (`CONTROL`), because by then
the roles exist.

---

## 11.2  Role Types

Role types are the coarse-grained privilege tiers.  Each maps to a fixed set
of permissions:

| RoleType      | Enum value     | RBAC | CONTROL | WRITE | READ | META | Description |
|---------------|----------------|:----:|:-------:|:-----:|:----:|:----:|-------------|
| `SUPERADMIN`  | `"superadmin"` | YES | YES | YES | YES | YES | Unrestricted; bypasses row/column filters. |
| `ADMIN`       | `"admin"`      | YES | YES | YES | YES | YES | Identical to SUPERADMIN by design. |
| `WRITER`      | `"writer"`     | --  | --  | YES | YES | YES | Owns its granted tables: create, write, drop. |
| `READER`      | `"reader"`     | --  | --  | --  | YES | YES | Read-only with row/column security applied. |
| `META`        | `"meta"`       | --  | --  | --  | --  | YES | Statistical / metadata access only. |

`SUPERADMIN` and `ADMIN` hold identical permissions. That is a decision, not
an oversight: both tiers administer roles and users, and no operation is
reserved to one. Both names are kept because roles of type `admin` already
exist in deployments, and a host may want the distinction for its own
bookkeeping even though this library draws none.

Each tier is a superset of the one below, so no role can do something a
nominally higher role cannot.

The `ROLE_PERMISSIONS` dict in `permissions.py` encodes this matrix, with
every grant written out:

```python
ROLE_PERMISSIONS = {
    RoleType.SUPERADMIN: {Permission.RBAC, Permission.CONTROL,
                          Permission.WRITE, Permission.READ, Permission.META},
    RoleType.ADMIN:      {Permission.RBAC, Permission.CONTROL,
                          Permission.WRITE, Permission.READ, Permission.META},
    RoleType.WRITER:     {Permission.WRITE, Permission.READ, Permission.META},
    RoleType.READER:     {Permission.READ, Permission.META},
    RoleType.META:       {Permission.META},
}
```

The admin tiers are spelled out rather than written `set(Permission)`, which
is how `CREATE` came to be granted to them without anyone deciding it should
be: the set comprehension picked up every member the enum declared. A new
permission must now be granted deliberately, role by role, or it is granted
to no one.

### The superadmin role is immutable

`_init_role_storage` mints exactly one role of type `superadmin` per
SuperTable at bootstrap. It cannot be created, promoted to, demoted, or
deleted by any caller:

| Attempt | Result |
|---------|--------|
| `create_role({"role": "superadmin", ...})` | `ValueError` -- the type is reserved |
| `create_role({"role_name": "superadmin", ...})` | `ValueError` -- the name is reserved |
| `update_role(other_id, {"role": "superadmin"})` | `ValueError` -- no promotion |
| `update_role(superadmin_id, {"role": "reader"})` | `ValueError` -- no demotion |
| `update_role(superadmin_id, {"role_name": "other"})` | `ValueError` -- no rename |
| `update_role(superadmin_id, {"enabled": False})` | `ValueError` -- cannot be disabled |
| `delete_role(superadmin_id)` | `ValueError` -- cannot be deleted |
| `create_role({"role": "superadmin", ...}, allow_reserved=True)` | `ValueError` -- one per SuperTable |

Every rule above is enforced on the **catalog write path**, not only in
`RoleManager`. `RedisCatalog` is a documented public class (§15), so a check
that lived only in the manager was one import wide:

```python
RedisCatalog().rbac_update_role(org, sup, my_reader_id, {"role": "superadmin"})
```

promoted a narrowly-granted reader to unrestricted reads of every table *and*
the ability to administer roles. Authorization cannot live at that layer --
the catalog has no actor -- but these invariants hold regardless of who is
asking, so that is where they belong. Two write paths, one rule.

Three of the five were each independently sufficient to take over or brick a
lake:

* **rename** orphans every caller that addresses the role by name (the docs,
  the demo scripts and the package docstring all pass `role_name="superadmin"`),
  and no replacement can be created because the name is reserved and the type
  is already taken.
* **disable** was a strictly better delete: `_resolve_role` denies a disabled
  role *everywhere*, including inside the RBAC check needed to re-enable it.
  One call bricked the lake with no recovery path through any gated API.
* **`allow_reserved`** is a *public* parameter whose docstring merely asked
  tenant callers not to set it. Passing it with a fresh name planted a second
  superadmin -- which the immutability rules above then made undeletable and
  undemotable by anyone, including the real superadmin.

Both halves of the reservation matter. The **name** was reserved first, but
the **type** is what enforcement reads: `access_control` takes
`role_info["role"]` and, for `superadmin`, returns an empty view set -- no row
filters, no column masks. A role stored as
`{"role": "superadmin", "role_name": "quarterly_report_viewer"}` passed the
name check and then had its own `tables` restriction discarded at read time.

Demotion is blocked for a less obvious reason: `delete_role` refuses by
reading the document's type, so changing the type first made "cannot be
deleted" bypassable in two calls -- leaving a lake with no superadmin and no
way to mint a replacement.

---

## 11.3  Role CRUD -- `RoleManager`

**Module:** `supertable.rbac.role_manager`

`RoleManager` is the business-logic layer for role lifecycle operations.
It is scoped to a `(super_name, organization)` pair and backed by a
`RedisCatalog` instance.

### 11.3.1  Initialisation

```python
RoleManager(
    super_name: str,
    organization: str,
    redis_catalog: Optional[RedisCatalog] = None,
    actor_role_name: Optional[str] = None,
)
```

`actor_role_name` is the role on whose authority this instance administers
roles. It is **required to mutate** and unnecessary to read:

| Operation | Needs an actor? |
|-----------|-----------------|
| `create_role`, `update_role`, `delete_role` | yes — `Permission.RBAC` |
| `get_role`, `get_role_by_name`, `list_roles`, `get_roles_by_type`, `get_superadmin_role_id` | no |

Omitting it fails **closed**: mutations raise `PermissionError`. Treating a
missing actor as unrestricted would mean the gate only protected callers who
had already opted into being checked.

The check lives in the mutating methods rather than the constructor, for two
structural reasons:

* **`__init__` bootstraps.** It mints this SuperTable's `superadmin` role, so
  a constructor demanding RBAC could never run the first time — the role that
  would authorise it is created by the call that needs it.
* **`check_rbac_access` builds a `RoleManager`** to resolve the actor. A
  validating constructor would validate its own helper, without bound.

This is why the access-control layer can keep constructing throwaway
instances to resolve a role without supplying an actor.

> **Trust boundary.** The actor is a role-name string supplied by the caller.
> This library has no sessions and no identity of its own, so the gate is
> exactly as strong as the host's binding of authenticated principal to role
> name — the same trust model as every other gate here. It does not replace
> the host authenticating; it is what makes that authentication mean
> something inside SuperTable.

On construction, `_init_role_storage()` runs a fast-path check: if the Redis
meta key `supertable:{org}:lakes:{sup}:rbac:roles:meta` already exists and a
superadmin role is present, initialisation is skipped entirely (avoiding 2-3
Redis round-trips).  Otherwise, a distributed lock (`acquire_simple_lock`) is
taken and the default **superadmin** role is created:

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
* `data["role_name"]` -- optional; must match `^[A-Za-z_][A-Za-z0-9_\- ]{0,126}$`.
  A name collision raises `ValueError` **unless** the stored role is identical
  to the one requested (same `content_hash`), which keeps genuine retries
  idempotent. Returning a *different* role on a collision silently discarded
  the requested type and grants, and with `superadmin` reachable by name it
  handed the bootstrap superadmin's id to anyone who asked (S11 / M12).
* Requires an actor holding `Permission.RBAC` -- see §11.3.1.
* The `superadmin` *type* is refused, as is a second role of that type. See
  §11.2.

Internally, a `RowColumnSecurity` value object is built, `prepare()` is
called (validates, normalises columns, computes `content_hash`), and the
resulting document is persisted via `RedisCatalog.rbac_create_role()`.

### 11.3.3  `update_role(role_id: str, data: dict) -> str`

Updates a role in-place.  Returns the new `content_hash`.

* The `role_id` remains stable -- all users referencing this role instantly
  see the new permissions.
* If `role_name` changes, uniqueness is validated and the
  `name_to_id` mapping in Redis is updated atomically.
* An audit event (`Actions.ROLE_UPDATE`) is emitted via
  `_audit_rbac()`.

### 11.3.4  `delete_role(role_id: str) -> bool`

Deletes a role and atomically strips it from all users who hold it.

* The **superadmin** role cannot be deleted -- attempting to do so raises
  `ValueError`.
* An audit event (`Actions.ROLE_DELETE`, severity `CRITICAL`) is emitted.

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

```python
UserManager(
    super_name: str,
    organization: str,
    redis_catalog: Optional[RedisCatalog] = None,
    actor_role_name: Optional[str] = None,
)
```

On construction, `_init_user_storage()` ensures the default **superuser**
account exists and holds the superadmin role.  If the superuser account exists
but lacks the superadmin role (e.g. after a role reset), the role is
automatically re-attached.

`actor_role_name` follows the same rule as `RoleManager` (§11.3.1): required
to mutate, unnecessary to read, fails closed when absent.

| Operation | Needs an actor? |
|-----------|-----------------|
| `create_user`, `modify_user`, `delete_user` | yes — `Permission.RBAC` |
| `add_role`, `remove_role`, `remove_role_from_users` | yes — `Permission.RBAC` |
| `get_user`, `get_user_by_name`, `list_users` | no |
| `get_or_create_default_user` | no — bootstrap repair only (see below) |

`add_role` is the most powerful call in the subsystem: binding a role is how
a principal acquires every permission that role holds, and the superadmin
role's id is discoverable through the public `get_superadmin_role_id()`.
Ungated, `add_role(me, get_superadmin_role_id())` was a two-line takeover.
`modify_user` accepts a `roles` list, so it is a granting path too — gating
one without the other would have left the door open.

Revocation is gated as well as granting: stripping an administrator's role is
a denial of service on the lake's administration, and in the limit locks
everyone out.

`get_or_create_default_user` is deliberately ungated. It repairs the
bootstrap account and nothing else — the username is fixed, the role is
whatever bootstrap already minted, and no part of either comes from the
caller. Gating it would also make it unusable in the one situation it exists
for: a lake whose superuser is missing, where no actor can be resolved.

### 11.4.2  `create_user(data: dict) -> str`

Creates a user and returns its `user_id`.

* `data["username"]` is required.
* `data["roles"]` -- list of `role_id` strings; each is validated for
  existence.
* Idempotent: if a user with the same `username` already exists, the existing
  `user_id` is returned.
* The user document includes `created_ms` and `modified_ms` timestamps.

### 11.4.3  `modify_user(user_id: str, data: dict) -> None`

Modifiable fields: `username`, `display_name`, `roles`.

* Username renames update the `name_to_id` mapping atomically.
* Role assignments are validated: every `role_id` must exist.

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
    "role_name": "sales_analyst",
    "tables": {
        "orders": {
            "columns": ["order_id", "amount", "status"],
            "filters": [
                {"status": {"operation": "=", "type": "value", "value": "completed"}}
            ]
        },
        "customers": {
            "columns": ["*"],
            "filters": ["*"]
        }
    }
}
```

* `"*"` as a table key = default entry for tables not explicitly listed.
* `"columns": ["*"]` = unrestricted column access for that table.
* `"filters": ["*"]` = no row-level filter (all rows visible).

### 11.5.2  FilterBuilder

```python
class FilterBuilder:
    def __init__(self, table_name: str, columns: list, role_info: dict): ...
    def build_filter_query(self, table_name, columns, filters) -> str: ...
    def json_to_sql_clause(self, json_obj) -> str: ...
```

`FilterBuilder` produces a complete `SELECT ... FROM ... WHERE ...` statement
that the query engine wraps as a filtered view.

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

The filter builder applies three layers of sanitisation:

1. **`_sanitize_column(col)`** -- validates against `^[A-Za-z_][A-Za-z0-9_]*$`
   and wraps in double quotes.
2. **`_sanitize_value(val)`** -- escapes single quotes (SQL standard doubling)
   and blocks `;`, `--`, `/*`, `*/`.
3. **`_sanitize_operation(op)`** -- whitelist-only; rejects anything not in
   `_ALLOWED_OPS`.

---

## 11.6  Column-Level Security

Column-level security is the second axis of data filtering.  Each table entry
in a role specifies an `allowed_columns` list:

* `["*"]` -- all columns are visible (unrestricted).
* `["order_id", "amount"]` -- only these columns can be queried.

When the query engine processes a SQL request, `restrict_read_access()` in
`access_control.py` validates that every column referenced in the query
(SELECT, WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY) is within the allowed
set.  Denied columns trigger a `PermissionError` with a message listing the
forbidden column names.

The `format_column_list()` helper in `filter_builder.py` produces the SELECT
projection:

```python
def format_column_list(columns):
    if columns == ["*"]:
        return "*"
    else:
        return ",".join(f'"{column}" as "{column}"' for column in columns)
```

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

* `prepare()` -- fills in defaults (`["*"]` for missing `columns`/`filters`),
  sorts and deduplicates column lists, and computes an MD5 `content_hash`.
* The `content_hash` is used for change detection and logging; it is **not**
  the role identity (that is the UUID `role_id`).

---

## 11.8  Access Control Enforcement -- `access_control.py`

**Module:** `supertable.rbac.access_control`

This module provides the enforcement functions called by API handlers and the
query engine.

### 11.8.1  Operation-Scoped Checks

| Function | Permission Required | Table-scoped? | Description |
|----------|-------------------|:---:|-------------|
| `check_rbac_access(super_name, org, role_name)` | `RBAC` | no | Administer roles and users. |
| `check_control_access(super_name, org, role_name, table_name)` | `CONTROL` | yes | Drop a SuperTable (scoped `"*"`). |
| `check_write_access(super_name, org, role_name, table_name)` | `WRITE` | yes | INSERT / UPDATE / DELETE, create and drop a table. |
| `check_meta_access(super_name, org, role_name, table_name)` | `META` | yes | Metadata and statistics reads. |

Each function:
1. Calls `_check_readonly_guard()` to block mutations on read-only
   SuperTables (snapshot clones, replicas, locked instances).
2. Resolves the role, validates the `RoleType`, and checks the permission
   matrix — `_check_scope_access()`.
3. For the table-scoped checks only, verifies table coverage —
   `_check_operation_access()` adds this on top of step 2.

`check_rbac_access` stops at step 2. A role grants access *to tables*, so
"which table is this role change about" has no answer; inventing one would
mean checking the actor's grants against a table that does not exist.

### 11.8.1.1  What table scoping means

`_resolve_table_entry` is `role_tables.get(table_name) or role_tables.get("*")`
— a plain dict lookup with a wildcard **fallback**, not a glob. So:

* a role with `tables={"*": ...}` passes for any table name, including one
  that does not exist yet (which is how a writer creates tables);
* a role with `tables={"orders": ...}` passes only for `orders`;
* passing `"*"` as the `table_name` requires the role to hold the wildcard
  entry — which is why `SuperTable.delete` uses it. Destroying every table at
  once must not be reachable by a role granted one of them.

Passing a **SuperTable** name where a table name is expected is a namespace
confusion, not a lake-wide check: it grants access to any role holding a
table coincidentally named after the lake, and otherwise falls through to
`"*"` anyway. Staging areas (lake-level, scoped `"*"`) and pipes (scoped to
the table the pipe feeds, read from its definition) used to do this.

### 11.8.1.2  Denial semantics

A denial is an exception, not an empty result — with one deliberate
exception, list filtering:

| Shape | Behaviour on denial |
|-------|---------------------|
| Named single object (`get_table_schema`, `get_table_stats`, `get_super_meta`, `collect_simple_table_schema`) | raises `PermissionError` |
| Listing (`get_tables`, `list_supers`, `list_tables`) | omits the item, returns the rest |

Filtering a listing is correct: "which tables can I see" has a right answer
even when it is a subset, and the caller asked for a list rather than for one
named thing. Returning `None`/`[]` from the *single-object* calls was not —
a caller could not tell a denial from an absence, so an API layer mapping
exceptions to 403 answered `200` with an empty body and the client read
"forbidden" as "does not exist".

The listing filters catch `PermissionError` **only**. Catching every
exception made a Redis failure mid-check drop the item exactly as a narrow
grant would, so an outage was indistinguishable from a permission boundary
and the caller got a short list with no indication anything had failed.

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
2. **SUPERADMIN/ADMIN** roles return `{}` (no filtering).
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
```

This dataclass is produced by `restrict_read_access()` and consumed by query
executors to create a filtered view on top of each reflection table.  The
`where_clause` is the SQL predicate generated by `FilterBuilder`.

### 11.8.4  Read-Only Guard

`_check_readonly_guard()` inspects the SuperTable root metadata for a
`read_only` flag and blocks mutations with context-specific error messages:

* `"live replica"` -- `clone_type == "replica"`
* `"read-only snapshot clone"` -- `clone_type == "readonly"`
* `"read-only clone"` -- has `cloned_from` attribute
* `"locked"` -- generic read-only lock

It is called by `check_rbac_access`, `check_control_access`,
`check_write_access` and `check_meta_access`, but **not** by
`restrict_read_access` — reads are exactly what a read-only replica is for.

### 11.8.5  Role Resolution

`_resolve_role()` fetches a role by name and checks two conditions:

1. The role must exist (otherwise `PermissionError`).
2. The role must be enabled -- the `enabled` field supports `"false"`, `"0"`,
   `False`, and missing (defaults to enabled for backward compatibility).

---

## 11.9  Programmatic Management

`RoleManager` and `UserManager` (in `supertable/rbac/`) expose full CRUD
operations from Python code:

```python
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

# Mutating requires an actor holding Permission.RBAC. The bootstrap role is
# named "superadmin"; a host should pass whatever role it has bound to the
# authenticated principal, never a value the client chose.
rm = RoleManager(super_name=super_name, organization=organization,
                 actor_role_name="superadmin")
rm.create_role({"role": "reader", "tables": {"facts": {"columns": ["*"], "filters": []}}})
rm.update_role(role_id, {...})
rm.delete_role(role_id)

# Reads need no actor.
rm_ro = RoleManager(super_name=super_name, organization=organization)
rm_ro.list_roles()
rm_ro.get_role(role_id)

um = UserManager(super_name=super_name, organization=organization,
                 actor_role_name="superadmin")
um.create_user({"username": "alice", "roles": [role_id]})
um.list_users()
um.get_user(user_id)
um.add_role(user_id, role_id)
um.modify_user(user_id, {"username": "alice2"})
um.delete_user(user_id)
um.get_or_create_default_user()
```

Valid role types: `superadmin`, `admin`, `writer`, `reader`, `meta`. The
`superadmin` *type* is reserved — see §11.2. Only `superadmin` and `admin`
can administer roles and users at all.

Role IDs and user IDs are 32-character hex strings matching `^[a-f0-9]{32}$`.

---

## 11.10  Redis Data Model

All RBAC state lives in Redis under a structured key namespace:

| Key Pattern | Type | Content |
|-------------|------|---------|
| `supertable:{org}:lakes:{sup}:rbac:roles:meta` | Hash | Version + last_updated_ms. |
| `supertable:{org}:lakes:{sup}:rbac:roles:index` | Set | All `role_id` values. |
| `supertable:{org}:lakes:{sup}:rbac:roles:doc:{role_id}` | Hash | Role document fields. |
| `supertable:{org}:lakes:{sup}:rbac:roles:name_to_id` | Hash | `role_name.lower()` to `role_id`. |
| `supertable:{org}:lakes:{sup}:rbac:roles:type:doc:{role_type}` | Set | Role IDs grouped by type. |
| `supertable:{org}:lakes:{sup}:rbac:users:meta` | Hash | Version + last_updated_ms. |
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
* Audit events are emitted on every RBAC change (see [12 Audit](12_audit.md)),
  supporting the HIPAA audit trail requirement.

### SOX (Sarbanes-Oxley)

* Separation of duties: `READER` cannot mutate data; `WRITER` cannot
  perform DDL; only `ADMIN`/`SUPERADMIN` can manage roles and users.
* The default superadmin role is protected from deletion, ensuring at least
  one administrative account always exists.
