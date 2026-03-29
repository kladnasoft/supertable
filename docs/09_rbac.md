# Data Island Core — RBAC & Access Control

## Overview

Data Island Core uses role-based access control to govern who can read, write, and manage data. Every API call, query, and data write passes through RBAC checks. The system supports five role types with progressively narrower permissions, per-table column restrictions, per-table row-level filters, and token-based authentication for external tools.

RBAC state — roles, users, and token hashes — is stored in Redis, scoped per SuperTable. There is no external identity provider integration; Data Island Core manages its own user and role registry.

---

## How it works

### Role types and permissions

Five role types exist, each with a fixed set of permissions:

| Role type | Control | Create | Write | Read | Meta | Description |
|---|---|---|---|---|---|---|
| `superadmin` | yes | yes | yes | yes | yes | Full access to everything |
| `admin` | yes | yes | yes | yes | yes | Same as superadmin |
| `writer` | — | — | yes | yes | yes | Read and write data |
| `reader` | — | — | — | yes | yes | Read-only with row/column security |
| `meta` | — | — | — | — | yes | Metadata only (table lists, schemas, stats) |

Permissions are defined in `rbac/permissions.py`:

- **CONTROL** — delete tables, manage roles and users
- **CREATE** — create new tables
- **WRITE** — insert, update, delete data
- **READ** — query data (subject to row/column filters)
- **META** — view table metadata, schemas, and statistics

### Access check flow

Every operation calls one of four check functions in `rbac/access_control.py`:

1. `check_control_access()` — for destructive operations (delete table, manage RBAC)
2. `check_write_access()` — for data writes and ingestion
3. `check_meta_access()` — for metadata reads
4. `restrict_read_access()` — for SQL queries (returns filter definitions)

Each check resolves the role by name, verifies the role type has the required permission, and raises `PermissionError` if denied.

### Per-table security

Roles can define per-table column and row restrictions:

```json
{
  "role": "reader",
  "role_name": "analyst",
  "tables": {
    "*": {"columns": ["*"], "filters": ["*"]},
    "orders": {
      "columns": ["order_id", "customer", "amount", "status"],
      "filters": [{"status": {"operation": "=", "type": "value", "value": "completed"}}]
    },
    "customers": {
      "columns": ["customer_id", "name"],
      "filters": ["*"]
    }
  }
}
```

The `"*"` key is the default — it applies to tables not explicitly listed. In this example:

- The `analyst` role sees all columns on unlisted tables (default `"*"`)
- On `orders`, the role sees only four columns and only rows where `status = 'completed'`
- On `customers`, the role sees only two columns but all rows

### How filters become SQL

When `restrict_read_access()` runs, it builds an `RbacViewDef` for each table alias:

```python
RbacViewDef(
    allowed_columns=["order_id", "customer", "amount", "status"],
    where_clause="status = 'completed'"
)
```

The query engine wraps each table in a filtered view:

```sql
CREATE VIEW filtered_orders AS
SELECT order_id, customer, amount, status
FROM raw_orders
WHERE status = 'completed'
```

The user's query runs against these filtered views. Restricted columns and rows are invisible — the user cannot bypass the filter, even with subqueries or JOINs.

`FilterBuilder` in `rbac/filter_builder.py` converts the JSON filter definitions into SQL WHERE clauses. It supports equality, inequality, IN, NOT IN, LIKE, IS NULL, IS NOT NULL, and compound AND/OR conditions.

---

## Role management

`RoleManager` in `rbac/role_manager.py` provides CRUD operations for roles.

### Creating a role

```python
role_manager = RoleManager(super_name="example", organization="acme")
role_id = role_manager.create_role({
    "role": "reader",
    "role_name": "analyst",
    "tables": {
        "*": {"columns": ["*"], "filters": ["*"]},
        "orders": {"columns": ["order_id", "amount"], "filters": []}
    }
})
```

Internally, `RowColumnSecurity` validates and normalizes the role definition:

1. Validates the role type against `RoleType` enum
2. Sets defaults: missing `columns` defaults to `["*"]`, missing `filters` defaults to `["*"]`
3. Sorts column lists for consistency
4. Computes a content hash (MD5 of the JSON representation)

The role is stored in Redis:
- `supertable:{org}:{sup}:rbac:role:doc:{role_id}` — Hash with role definition
- `supertable:{org}:{sup}:rbac:role:index` — Set of all role IDs
- `supertable:{org}:{sup}:rbac:role:type:{type}` — Set of role IDs by type
- `supertable:{org}:{sup}:rbac:role:name_to_id` — Hash for name→ID lookup

### Role operations

| Method | Description |
|---|---|
| `create_role(data)` | Create a new role. Returns role_id. |
| `update_role(role_id, data)` | Update role fields (role type, tables, name). |
| `delete_role(role_id)` | Delete a role. Removes from all users. |
| `get_role(role_id)` | Get role definition by ID. |
| `get_role_by_name(role_name)` | Get role definition by name. |
| `list_roles()` | List all roles. |
| `get_roles_by_type(role_type)` | List roles of a specific type. |
| `get_superadmin_role_id()` | Get the superadmin role ID. |

---

## User management

`UserManager` in `rbac/user_manager.py` manages users and their role assignments.

### Default superuser

On initialization, `UserManager` creates a default superuser if one doesn't exist. The superuser's hash is derived from `SHA-256("{org}:superuser")` and is assigned the superadmin role. This user is used for supertoken-based login.

### User operations

| Method | Description |
|---|---|
| `create_user(data)` | Create a user. Returns user_id. |
| `get_user(user_id)` | Get user by ID. |
| `get_user_by_name(username)` | Get user by username. |
| `modify_user(user_id, data)` | Update user fields. |
| `delete_user(user_id)` | Delete a user. |
| `list_users()` | List all users. |
| `add_role(user_id, role_id)` | Assign a role to a user. |
| `remove_role(user_id, role_id)` | Remove a role from a user. |

Users are stored in Redis with a parallel structure to roles:
- `supertable:{org}:{sup}:rbac:user:doc:{user_id}` — Hash with user data
- `supertable:{org}:{sup}:rbac:user:index` — Set of all user IDs
- `supertable:{org}:{sup}:rbac:user:username_to_id` — Hash for name→ID lookup

---

## Token authentication

Auth tokens are created by superusers for programmatic API access. See the Redis Catalog documentation for the token API (`create_auth_token`, `validate_auth_token`, `delete_auth_token`).

Token flow:

1. Superuser creates a token via the UI or API — plaintext token is returned once
2. The token is SHA-256 hashed and stored in Redis (`supertable:{org}:auth:tokens`)
3. API callers pass the plaintext token in the `Authorization: Bearer {token}` header (or the configured header)
4. The server hashes the incoming token and compares against stored hashes
5. If valid, the request proceeds with the token's associated role

---

## Integration with DataReader and DataWriter

### Read path

`DataReader.execute()` calls `restrict_read_access()` which:
1. Resolves the role by name
2. Checks READ permission
3. For admin/superadmin: returns empty dict (no filtering)
4. For reader/writer: validates every table and column in the query against the role's allowed tables
5. Builds `RbacViewDef` objects for each alias needing filtering
6. The engine applies these as filtered views

### Write path

`DataWriter.write()` calls `check_write_access()` which:
1. Resolves the role by name
2. Checks WRITE permission
3. Raises `PermissionError` if denied
4. No column/row filtering on writes — write access is all-or-nothing per table

---

## Module structure

```
supertable/rbac/
  __init__.py                Package marker
  permissions.py             Permission and RoleType enums, role→permission mapping (33 lines)
  access_control.py          check_*_access() functions, restrict_read_access() (250+ lines)
  role_manager.py            RoleManager — CRUD for roles (190 lines)
  user_manager.py            UserManager — CRUD for users (200 lines)
  row_column_security.py     RowColumnSecurity — role definition validation (95 lines)
  filter_builder.py          FilterBuilder — JSON filter → SQL WHERE clause (80 lines)
```

---

## Frequently asked questions

**Can a user have multiple roles?**
Yes. Users can be assigned multiple roles. The effective permissions are the union of all assigned roles' permissions. For table-level filters, the least restrictive role's definition applies.

**What happens if a role is deleted that users are assigned to?**
The role is removed from all users before deletion (`remove_role_from_users()`). Users who had only that role will have no roles and will not be able to access anything.

**Can I restrict write access to specific tables?**
Write access is checked per-table using the role's table definitions. If a table is not in the role's allowed tables (and no `"*"` default exists), writes are denied.

**How do row-level filters work with JOINs?**
Each table in the query gets its own filtered view. If table A has a row filter and table B doesn't, the JOIN sees filtered rows from A and all rows from B. The filter is applied before the JOIN.

**Can I create custom role types?**
No. The five role types (superadmin, admin, writer, reader, meta) are hardcoded. Custom permissions are achieved by combining role types with per-table column and row restrictions.

**Is there audit logging for RBAC changes?**
Yes. Role creation, update, deletion, and user role assignments are all logged as audit events (category: `rbac_change`, severity: `warning`).
