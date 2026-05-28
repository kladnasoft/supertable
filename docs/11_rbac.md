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
| `SUPERADMIN`  | `"superadmin"` | ALL (CONTROL, CREATE, WRITE, READ, META) | Unrestricted; bypasses row/column filters. |
| `ADMIN`       | `"admin"`      | ALL (CONTROL, CREATE, WRITE, READ, META) | Same permissions as SUPERADMIN. |
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
  If a role with the same name already exists, the existing `role_id` is
  returned (idempotent create).

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

On construction, `_init_user_storage()` ensures the default **superuser**
account exists and holds the superadmin role.  If the superuser account exists
but lacks the superadmin role (e.g. after a role reset), the role is
automatically re-attached.

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

| Function | Permission Required | Description |
|----------|-------------------|-------------|
| `check_control_access(super_name, org, role_name, table_name)` | `CONTROL` | DDL operations (DROP, TRUNCATE). |
| `check_write_access(super_name, org, role_name, table_name)` | `WRITE` | INSERT / UPDATE / DELETE. |
| `check_meta_access(super_name, org, role_name, table_name)` | `META` | ALTER / metadata changes. |

Each function:
1. Calls `_check_readonly_guard()` to block mutations on read-only
   SuperTables (snapshot clones, replicas, locked instances).
2. Calls `_check_operation_access()` which resolves the role, validates the
   `RoleType`, checks the permission matrix, and verifies table coverage.

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

rm = RoleManager(super_name=super_name, organization=organization)
rm.create_role({"role": "reader", "tables": {"facts": {"columns": ["*"], "filters": []}}})
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

Valid role types: `superadmin`, `admin`, `writer`, `reader`, `meta`.

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
