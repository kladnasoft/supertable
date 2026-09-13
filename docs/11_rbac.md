# Roles, users, and access control

SuperTable stores named roles and users in Redis within an organization and SuperTable. A role has a permission type and table-specific column/filter definitions. Public read and write operations receive a role name; role-management and user-management mutations additionally require an actor role with the RBAC permission.

Implementation: [permissions](../supertable/rbac/permissions.py), [access checks](../supertable/rbac/access_control.py), [role manager](../supertable/rbac/role_manager.py), [user manager](../supertable/rbac/user_manager.py), [row/column definitions](../supertable/rbac/row_column_security.py), [filter builder](../supertable/rbac/filter_builder.py), and [Redis catalog](../supertable/redis_catalog.py).

## 1. Choose a role type

| Role type | READ | META | WRITE | CONTROL | RBAC |
| --- | --- | --- | --- | --- | --- |
| `superadmin` | Yes | Yes | Yes | Yes | Yes |
| `admin` | Yes | Yes | Yes | Yes | Yes |
| `writer` | Yes | Yes | Yes | No | No |
| `reader` | Yes | Yes | No | No | No |
| `meta` | No | Yes | No | No | No |

READ is used for data queries. META is used for metadata access. WRITE and CONTROL check their respective permissions and table membership. RBAC authorizes role and user administration at the SuperTable scope; it does not require a wildcard table grant.

Role lookup uses the supplied role name. Missing roles, missing/unknown role types, and disabled roles are rejected. The read check treats boolean `False` and string values `"false"` or `"0"` as disabled.

**Trust boundary:** these Python APIs receive an already-selected role name. They do not authenticate that a caller owns the role, and `DataReader` does not look up a username's role list. An API or application must authenticate its caller and restrict which role name it may supply. User records store assignments; they do not automatically combine multiple roles into one effective query policy.

## 2. Define table access

A role definition has this shape:

```python
role_data = {
    "role_name": "sales_reader",
    "role": "reader",
    "tables": {
        "orders": {
            "columns": ["order_id", "total"],
            "filters": {
                "region": {
                    "operation": "=",
                    "type": "value",
                    "value": "EU",
                }
            },
        }
    },
}
```

This permits queries on `orders` to return `order_id` and `total` for rows where `region = 'EU'`. The filter column can be used internally without being exposed as a result column.

Each table entry supports:

| Field | Default when omitted | Meaning |
| --- | --- | --- |
| `columns` | `["*"]` | All columns, or an explicit list of allowed columns |
| `filters` | `["*"]` | All rows, or a structured row predicate |

The key `"*"` grants a default table definition. A table-specific entry is chosen before that wildcard entry; the two definitions are not merged. Access-control lookup uses the simple table name, not a `super.table` key.

```python
role_data = {
    "role_name": "report_reader",
    "role": "reader",
    "tables": {
        "*": {"columns": ["*"], "filters": ["*"]},
        "payroll": {
            "columns": ["department", "headcount"],
            "filters": ["*"],
        },
    },
}
```

For a non-admin reader, an absent matching table entry denies access. `columns=[]` denies all columns. Mixing `"*"` with named columns is rejected when preparing a new or updated role. Existing mixed wildcard definitions are normalized toward their explicit names during read checks. Column comparisons in authorization are case-insensitive.

A stored legacy list of table names is accepted by access checks as unrestricted row/column entries, but new role creation should use the dictionary form shown above.

### Administrative read behavior

`admin` and `superadmin` bypass table, row, and column restrictions in `restrict_read_access()`. Their READ permission is still required and a disabled role is still rejected. Their WRITE, CONTROL, and META checks continue to require a matching table grant.

Non-admin roles with only one unrestricted `"*"` entry also require no restriction views. Otherwise each physical table is checked and restricted views are built per SQL alias.

Cross-SuperTable SQL resolves authorization from the reader's selected role scope and matches referenced simple table names. It does not independently resolve a same-named role in each referenced SuperTable. Keep this behavior in mind when exposing qualified table queries through an application.

## 3. Build row predicates

`FilterBuilder` converts structured definitions to a SQL predicate. A list of filters is joined with AND. A dictionary can contain column conditions, `AND`, `OR`, or `NOT` groups.

```python
filters = {
    "AND": [
        {"region": {"operation": "=", "type": "value", "value": "EU"}},
        {
            "OR": [
                {"status": {"operation": "=", "type": "value", "value": "paid"}},
                {"status": {"operation": "=", "type": "value", "value": "shipped"}},
            ]
        },
        {"deleted_at": {"operation": "IS", "type": "null"}},
    ]
}
```

Use a `range` list to combine comparisons on one column:

```python
filters = {
    "total": {
        "range": [
            {"operation": ">=", "type": "value", "value": 100},
            {"operation": "<", "type": "value", "value": 1000},
        ]
    }
}
```

Operand types are:

- `value`: renders an escaped, quoted SQL string literal, including when the supplied Python value is numeric. The engine handles any needed type conversion.
- `reference`: renders a quoted column identifier, for example comparing `owner_id` with another source column.
- `null`: renders SQL `NULL`, normally paired with `IS` or `IS NOT`.

Accepted operator names are `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`, `IN`, `NOT IN`, `IS`, `IS NOT`, `BETWEEN`, and `NOT BETWEEN`. The formatter still renders a single operand for each condition: there is no special list or two-endpoint operand renderer for IN/BETWEEN. Use OR conditions for membership and a `range` list for bounded intervals.

Column names in predicates must match an ASCII letter or underscore followed by ASCII letters, digits, or underscores. Values escape single quotes and reject semicolons and SQL comment delimiters. ILIKE/NOT ILIKE value operands can also specify `escape`.

`["*"]` is the explicit unrestricted filter. Empty lists, empty objects, malformed operands, and unknown operators fail to produce a usable read policy. Role creation normalizes and hashes the definition; full filter compilation occurs when the role is used for reading, so creation alone does not establish that a row predicate is executable.

## 4. Create and update roles

```python
from supertable.rbac.role_manager import RoleManager

roles = RoleManager(
    super_name="warehouse",
    organization="example_org",
    actor_role_name="superadmin",
)

role_id = roles.create_role({
    "role_name": "sales_reader",
    "role": "reader",
    "tables": {
        "orders": {
            "columns": ["order_id", "total"],
            "filters": {
                "region": {"operation": "=", "type": "value", "value": "EU"}
            },
        }
    },
})

stored = roles.get_role(role_id)
```

The example assumes a trusted administrator is authorized to act as `superadmin` for this SuperTable.

`RoleManager(super_name, organization, redis_catalog=None, actor_role_name=None)` provides:

| Method | Result/behavior |
| --- | --- |
| `create_role(data, allow_reserved=False)` | Creates a UUID-hex role ID |
| `update_role(role_id, data)` | Replaces supplied role type/table definition or name; returns the new content hash |
| `delete_role(role_id)` | Deletes a non-superadmin role and returns a boolean |
| `get_role(role_id)` | Returns the role dictionary or `{}` |
| `get_role_by_name(role_name)` | Returns the role dictionary or `{}` |
| `list_roles()` | Returns role dictionaries |
| `get_roles_by_type(role_type)` | Uses the role-type index and returns matching role dictionaries |
| `get_superadmin_role_id()` | Returns the initialized superadmin role ID |

Create, update, and delete require `actor_role_name`; without it they raise `PermissionError`. Read/list methods have no actor gate in the manager. Role names are indexed case-insensitively. When non-empty, a name must be 1–127 ASCII characters, begin with a letter or underscore, and otherwise contain letters, digits, underscores, hyphens, dots, or spaces.

Preparation fills missing column/filter defaults, sorts and deduplicates explicit columns, and computes an MD5 content hash of the role type and tables. The UUID is the role identity; the hash changes when policy changes. Re-creating the same name with the same normalized content returns the existing ID. Reusing a name with different content raises `ValueError` and requires an update instead.

`update_role(..., {"tables": ...})` replaces the table map rather than merging individual entries. Its supported policy fields are `role`, `tables`, and `role_name`; arbitrary fields such as `enabled` are not forwarded by this manager method. Catalog role updates contain additional low-level validation.

### Reserved defaults

Role initialization ensures a single `superadmin` role with unrestricted table access. Normal tenant creation rejects the reserved name and type. The catalog also rejects a second superadmin and promotion of an ordinary role to that type. The existing superadmin cannot be deleted, renamed away from `superadmin`, disabled, or changed to another role type.

`get_superadmin_role_hash()` is a compatibility method that returns the role ID, despite its name. It is not the content hash.

## 5. Manage users and assignments

```python
from supertable.rbac.user_manager import UserManager

users = UserManager(
    super_name="warehouse",
    organization="example_org",
    actor_role_name="superadmin",
)
user_id = users.create_user({
    "username": "analyst@example.org",
    "roles": [role_id],
})
users.modify_user(user_id, {"display_name": "Sales analyst"})
```

`UserManager(super_name, organization, redis_catalog=None, actor_role_name=None)` provides:

| Method | Behavior |
| --- | --- |
| `create_user(data)` | Requires username; validates role IDs; returns a UUID-hex user ID |
| `get_user(user_id)` / `get_user_by_name(username)` | Return details; raise `ValueError` when absent |
| `modify_user(user_id, data)` | Updates `roles`, `username`, or `display_name` |
| `delete_user(user_id)` | Deletes an existing non-superuser account |
| `list_users()` | Returns user dictionaries |
| `add_role(user_id, role_id)` / `remove_role(user_id, role_id)` | Grant/revoke an assignment; return a boolean |
| `remove_role_from_users(role_id)` | Revoke that role from every indexed user |

All these mutations require an actor with RBAC permission; getters and lists have no manager-level actor check. Usernames use a case-insensitive name index. They must be 1–127 ASCII characters, start with a letter or underscore, and otherwise contain letters, digits, underscores, hyphens, dots, or `@`.

Creating an existing username returns its current user ID without replacing its role assignments. `modify_user(..., {"roles": [...]})` replaces the complete role list and validates each ID. Adding a role validates that the role exists. The catalog's role deletion also removes assignments to that role.

Initialization creates a default `superuser` if the superadmin role exists and the default account is absent. `get_or_create_default_user()` returns or establishes that account. `delete_user()` refuses deletion when the stored username is `superuser`. `get_user_hash_by_name()` is a compatibility alias returning the user details dictionary, not a hash string.

These records contain IDs, names, role assignments, and timestamps. Creating a user here does not issue credentials or authenticate a session.

### Organization authentication tokens

`RedisCatalog` supplies organization-scoped token helpers, separate from user creation and role selection:

| Method | Behavior |
| --- | --- |
| `create_auth_token(org, created_by, label=None, enabled=True, username="", user_id="", expires_ms=None)` | Returns a newly generated token plus its metadata |
| `list_auth_tokens(org)` | Lists stored metadata, newest first |
| `validate_auth_token(org, token)` | Checks only whether the token's hash exists |
| `validate_auth_token_full(org, token)` | Returns enabled, unexpired token metadata or `None` |
| `delete_auth_token(org, token_id)` | Removes the stored hash entry and returns a boolean |

Creation generates `st_login_` followed by a random URL-safe value. The token ID is the SHA-256 digest of that full token. Redis stores the digest and metadata; the raw token is returned at creation. `expires_ms` is an absolute Unix timestamp in milliseconds, with zero meaning no expiry.

Use `validate_auth_token_full` when checking enabled state and expiry: the shorter validation method checks existence alone. Full validation checks the stored token document; it does not validate that the recorded user still exists, resolve its current roles, or check authority for a particular SuperTable. The `username`, `user_id`, and `created_by` fields are supplied metadata, not proof of authentication.

These catalog helpers do not require `actor_role_name` and must be called behind an application's authorization layer. Deleting a token by its `token_id` revokes its future catalog validation; it does not terminate an already-open query stream. See [Redis catalog](05_redis_catalog.md) for the broader catalog API.

## 6. Enforcement during reads and writes

For a restricted query, the read check validates physical-table membership and explicitly requested source columns. A denied column can fail with `PermissionError` even when it appears only in a predicate or join. Wildcard queries are constrained by the allowed-column view, so `SELECT *` exposes only permitted columns.

The engine places restrictions before user SQL:

1. Read the selected Parquet resources.
2. Remove deletion-vector row IDs and hide internal columns.
3. Apply allowed columns and the role's row predicate.
4. Execute the rewritten user query against those views.

DuckDB widens its internal projection when a role filter references a column absent from the user's projection, and removes columns used only for that filter from the public view. If a required filter column is known to be absent, view preparation fails. Spark translates filter expressions from DuckDB syntax to Spark syntax and raises if that translation fails; it does not intentionally discard the predicate.

A table payload's share `_row_filter` is combined with the role predicate using AND. Administrative READ bypass does not remove that separate share filter. Unusable configured role filters cause permission errors. Exceptions that escape the per-table deletion-control lookup cause a read error. This does not cover all lookup failures: the catalog converts Redis-specific getter errors to `None`, which the reader can treat as an absent payload and proceed without those controls. Path-only leaf payloads can also omit deletion/share filtering; see [reader control limitations](10_data_reader.md#4-read-arrow-batches).

WRITE, CONTROL, and RBAC operations also consult the root's `read_only` flag. A true flag blocks those operations for replicas, read-only clones, or locked roots; READ and META do not use that guard. The guard propagates explicit permission failures, refuses unexpected lookup failures, but currently returns without a read-only decision on a Redis-specific `RedisError`. The catalog can also swallow that error and return `None`, which likewise supplies no read-only flag. This check alone is therefore not a guarantee of immutability during Redis failures.

Table column and row rules are applied by the read path. The WRITE and CONTROL access helpers check role type and table membership; they do not enforce those read predicates on incoming writes.

Role/user mutations attempt to emit audit events when the audit package is available. Audit errors are caught and do not roll back an otherwise successful mutation.

## 7. Boundaries to account for in applications

- Supply role names only after application authentication and assignment checks. Direct use of manager getters, catalog methods, or stored-job helpers is not a replacement for an application authorization layer.
- Treat `admin` and `superadmin` as unrestricted readers regardless of table policy fields.
- Validate configured predicates by exercising an authorized read; filter compilation is deferred until that read.
- `SHOW STATS` authorizes reading the table but returns raw statistics without applying the role's row/column views; it can expose values or columns outside the normal query result.
- Read policy is evaluated for each invocation, not continuously rechecked for every batch of an already-open stream. OData's policy fingerprint helpers can detect changes between service requests, as described in [Data reader](10_data_reader.md#7-odata-service-helpers).
