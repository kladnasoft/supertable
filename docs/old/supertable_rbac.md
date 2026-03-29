# Feature / Component Reverse Engineering: Supertable RBAC Subsystem

## 1. Executive Summary

This code implements a **multi-tenant Role-Based Access Control (RBAC) subsystem** for the **Supertable** data platform. It provides fine-grained authorization at the table, column, and row level — controlling who can read, write, create, alter, or drop tables within an organization's data namespace.

- **Main responsibility:** Enforce per-role, per-table access control with column-level projection and row-level filtering on SQL queries.
- **Likely product capability:** Supertable is a managed analytical data product (likely a "super-table" / data lakehouse / virtual table layer). This RBAC subsystem enables multi-user, multi-role governance over shared datasets.
- **Business purpose:** Allow organizations to grant differentiated data access — e.g., analysts see only certain columns/rows, writers can ingest data, admins manage schema — without duplicating tables or data.
- **Technical role:** Authorization middleware sitting between the SQL query engine and the data catalog. It validates permissions before query execution and generates per-alias RBAC view definitions that the query engine uses to enforce column projection and row filtering at runtime.

---

## 2. Functional Overview

### Product Capability

Because of this code, the Supertable product can:

1. **Define named roles** (e.g., `sales_analyst`, `finance_reader`) scoped to an organization and "super" (a logical data namespace).
2. **Assign per-table column restrictions** — a role may only see `order_id` and `amount` on the `orders` table but all columns on `customers`.
3. **Assign per-table row-level filters** — a role's view of a table can be restricted by arbitrary SQL predicates (e.g., `status = 'completed'`), including `AND`/`OR`/`NOT` compositions, range conditions, `ILIKE` with escape characters, and `NULL` checks.
4. **Enforce permission types** — five discrete permissions (`CONTROL`, `CREATE`, `WRITE`, `READ`, `META`) mapped to five role types (`superadmin`, `admin`, `writer`, `reader`, `meta`).
5. **Manage users** with stable UUIDs, unique usernames, and many-to-many role assignments.
6. **Auto-bootstrap** a `superadmin` role and `superuser` account on first access, ensuring every namespace is always administrable.
7. **Transparently upgrade legacy role formats** — old list-based table definitions are normalized to the new per-table dict format at runtime.

### Target Actors

| Actor | Interaction |
|---|---|
| **Data consumer / Analyst** | Assigned `reader` role; sees only permitted columns/rows |
| **Data engineer / Ingester** | Assigned `writer` role; can read and write |
| **Admin / DBA** | Assigned `admin` or `superadmin`; full control including DDL |
| **Metadata consumer** | Assigned `meta` role; can only access statistical/meta information |
| **Platform operator** | Benefits from auto-bootstrap of superadmin/superuser |
| **MCP / API gateway** | Calls check functions before executing SQL; applies RBAC view defs |

### Business Value

- **Data governance & compliance** — enforce least-privilege data access for regulatory (GDPR, HIPAA) or internal policy needs.
- **Multi-tenancy enablement** — multiple teams/roles share the same physical tables with different visibility.
- **Self-service analytics safety** — allow broad SQL access (e.g., via an MCP connector or BI tool) without risk of exposing restricted data.
- **Operational simplicity** — no need for materialized per-role views; filtering is injected dynamically at query time.

---

## 3. Technical Overview

### Architectural Style

- **Layered authorization middleware** — pure Python business logic layer that wraps a Redis-backed catalog (`RedisCatalog`) for persistence.
- **Permission matrix pattern** — static enum-based permission mapping, checked at runtime.
- **Value-object pattern** — `RowColumnSecurity` normalizes and hashes role definitions before storage.
- **Builder pattern** — `FilterBuilder` converts JSON filter specifications into SQL `WHERE` clauses.
- **Multi-tenant key scoping** — all operations are scoped by `(organization, super_name)` tuple.

### Major Modules

| Module | Responsibility |
|---|---|
| `permissions.py` | Defines `Permission` and `RoleType` enums, static permission matrix |
| `role_manager.py` | CRUD for roles; auto-bootstraps superadmin; delegates to `RedisCatalog` |
| `user_manager.py` | CRUD for users; auto-bootstraps superuser; role assignment helpers |
| `row_column_security.py` | Value object: validates, normalizes, and hashes role permission data |
| `filter_builder.py` | Converts JSON filter specs into SQL `SELECT … FROM … WHERE …` strings |
| `access_control.py` | Authorization enforcement: checks permissions, builds RBAC view definitions |
| `__init__.py` | Package marker (empty) |

### Key Control Flows

1. **Write/Create/Control/Meta check:** Caller invokes `check_*_access()` → resolves role from Redis → checks permission matrix → checks table coverage → raises `PermissionError` or returns silently.
2. **Read access restriction:** Caller invokes `restrict_read_access()` → resolves role → if superadmin/admin, returns `{}` (unrestricted) → otherwise validates every physical table and column against the role's allowed set → builds per-alias `RbacViewDef` objects with column projections and row-level `WHERE` clauses → returns dict for the query engine.

### Key Data Flow

```
SQL Query → SQLParser → physical_tables + alias tables
                              ↓
                     restrict_read_access()
                              ↓
                     RoleManager → RedisCatalog (Redis)
                              ↓
                     Permission matrix check
                              ↓
                     Per-table column validation
                              ↓
                     FilterBuilder → WHERE clause generation
                              ↓
                     Dict[alias → RbacViewDef] → Query Engine
```

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes / Functions | Relevance |
|---|---|---|---|
| `permissions.py` | Permission and role type definitions; static permission matrix | `Permission`, `RoleType`, `ROLE_PERMISSIONS`, `has_permission()`, `get_role_permissions()` | Foundational — defines the entire permission model |
| `role_manager.py` | Role lifecycle management (CRUD) with Redis persistence | `RoleManager` (class) | Core — manages role definitions and auto-bootstraps superadmin |
| `user_manager.py` | User lifecycle management (CRUD), role assignment | `UserManager` (class) | Core — manages users and role bindings |
| `row_column_security.py` | Value object for role permission normalization and hashing | `RowColumnSecurity` (class) | Supporting — ensures consistent serialization and change detection |
| `filter_builder.py` | JSON-to-SQL filter translation | `FilterBuilder` (class), `format_column_list()` | Supporting — generates row-level security predicates |
| `access_control.py` | Authorization enforcement for all operation types | `check_control_access()`, `check_create_access()`, `check_write_access()`, `check_meta_access()`, `restrict_read_access()` | Critical — the primary enforcement surface |
| `__init__.py` | Package marker | (empty) | Structural |

---

## 5. Detailed Functional Capabilities

### 5.1 Permission Matrix Enforcement

- **Description:** A static mapping from five role types to sets of allowed permissions.
- **Business purpose:** Ensures a deterministic, code-level contract for what each role type can do. Prevents ad-hoc permission grants.
- **Trigger/input:** Any call to `has_permission(role_type, permission)`.
- **Processing:** Lookup in `ROLE_PERMISSIONS` dict.
- **Output:** Boolean.
- **Dependencies:** None (pure static data).
- **Constraints:** The matrix is hardcoded; adding a new permission or role type requires a code change.
- **Risks:** If `ROLE_PERMISSIONS` falls out of sync with actual enforcement code, access control gaps could emerge.
- **Confidence:** Explicit.

**Permission matrix (from code):**

| Role Type | CONTROL | CREATE | WRITE | READ | META |
|---|---|---|---|---|---|
| `superadmin` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `admin` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `writer` | ✗ | ✗ | ✓ | ✓ | ✓ |
| `reader` | ✗ | ✗ | ✗ | ✓ | ✓ |
| `meta` | ✗ | ✗ | ✗ | ✗ | ✓ |

### 5.2 Table-Level Access Gating (Write/Create/Control/Meta)

- **Description:** Before any mutating or DDL operation, the system validates that the caller's role has (a) the required permission type and (b) coverage for the target table.
- **Business purpose:** Prevents unauthorized schema changes, data writes, or destructive operations.
- **Trigger:** `check_write_access()`, `check_create_access()`, `check_control_access()`, `check_meta_access()`.
- **Processing:** Resolve role by name from Redis → extract `role` type → check permission matrix → normalize tables → check table entry exists (specific or wildcard `"*"`).
- **Output:** Silent return on success; `PermissionError` on failure.
- **Dependencies:** `RoleManager`, `RedisCatalog`.
- **Constraints:** Role must exist; role must have a `role` type string; table must be covered by role's table set or the `"*"` wildcard.
- **Confidence:** Explicit.

### 5.3 Read Access Restriction with Column and Row Filtering

- **Description:** For read (SELECT) operations, the system validates column-level access per physical table and constructs per-alias RBAC view definitions that the query engine uses to enforce column projection and row-level `WHERE` clauses.
- **Business purpose:** Enables fine-grained data access — an analyst might see only `order_id` and `amount` from the `orders` table, and only rows where `region = 'EU'`.
- **Trigger:** `restrict_read_access(super_name, organization, role_name, tables, physical_tables)`.
- **Processing:**
  1. Resolve role, check READ permission.
  2. Short-circuit: superadmin/admin → return `{}` (unrestricted).
  3. Short-circuit: single `"*"` entry with all-wildcard columns and filters → return `{}`.
  4. Phase 1 — validate physical tables: for each physical table in the query, verify it's in the role's allowed set. If column restrictions exist, verify every queried column is allowed. `SELECT *` defers column enforcement to execution time.
  5. Phase 2 — build `RbacViewDef` per alias: for each alias in the query, resolve its table entry, extract allowed columns and filters, generate `WHERE` clause via `FilterBuilder`, and emit an `RbacViewDef` if any restriction applies.
- **Output:** `Dict[str, RbacViewDef]` — mapping from SQL alias to view definition containing `allowed_columns` and `where_clause`.
- **Dependencies:** `RoleManager`, `FilterBuilder`, `RbacViewDef` (imported from `supertable.data_classes`), `SQLParser` (imported but used upstream to produce `tables` and `physical_tables`).
- **Constraints:** CTE aliases without physical table mappings are silently skipped (validated transitively). `SELECT *` columns cannot be validated at parse time.
- **Risks:** If `physical_tables` does not cover all columns from all query clauses, some column access may go unvalidated. The code comments suggest `get_physical_tables()` merges all columns, mitigating this.
- **Confidence:** Explicit.

### 5.4 Role Lifecycle Management

- **Description:** Full CRUD for role definitions with UUID-based stable identity, content hashing, idempotent creation, and atomic deletion that strips the role from all users.
- **Business purpose:** Allows admins to define, update, and remove data-access policies. Role updates take effect instantly for all assigned users.
- **Trigger:** `RoleManager.create_role()`, `update_role()`, `delete_role()`, `get_role()`, etc.
- **Processing:** Validates via `RowColumnSecurity`, persists to Redis via `RedisCatalog`.
- **Output:** Role ID (UUID hex string), role documents, content hashes.
- **Dependencies:** `RowColumnSecurity`, `RedisCatalog`, `uuid`.
- **Constraints:** Role names are unique (case-insensitive). Creation is idempotent — returns existing ID if name matches.
- **Confidence:** Explicit.

### 5.5 User Lifecycle Management

- **Description:** Full CRUD for users with UUID-based identity, unique usernames, many-to-many role assignments, timestamp tracking, and protection of the default superuser.
- **Business purpose:** Allows admins to manage who has access to the data platform and what roles they hold.
- **Trigger:** `UserManager.create_user()`, `modify_user()`, `delete_user()`, `add_role()`, `remove_role()`.
- **Processing:** Validates role existence before assignment; prevents deletion of superuser; persists to Redis.
- **Output:** User ID (UUID hex string), user documents.
- **Dependencies:** `RedisCatalog`, `uuid`, `time`.
- **Constraints:** Default `superuser` cannot be deleted. Username renames check uniqueness.
- **Confidence:** Explicit.

### 5.6 Auto-Bootstrap of Superadmin Role and Superuser

- **Description:** On first instantiation, `RoleManager` creates a default `superadmin` role with full wildcard access, and `UserManager` creates a `superuser` user bound to that role. Uses distributed locking to prevent race conditions.
- **Business purpose:** Guarantees every namespace is immediately administrable without manual setup. Critical for platform onboarding and disaster recovery.
- **Trigger:** Constructor of `RoleManager` / `UserManager`.
- **Processing:** Check if superadmin role/superuser exist → if not, acquire lock → double-check → create.
- **Dependencies:** `RedisCatalog.acquire_simple_lock()`, `release_simple_lock()`.
- **Constraints:** Lock TTL is 10 seconds; timeout is 30 seconds. If Redis is unavailable, bootstrap fails.
- **Confidence:** Explicit.

### 5.7 JSON-to-SQL Filter Translation

- **Description:** Converts a JSON filter specification into a SQL `WHERE` clause supporting equality, comparison, range, `ILIKE` with escaping, `NULL` checks, and boolean composition (`AND`/`OR`/`NOT`).
- **Business purpose:** Enables administrators to define row-level security policies declaratively as JSON, which the system translates to SQL at query time.
- **Trigger:** `FilterBuilder.__init__()` or `restrict_read_access()`.
- **Processing:** Recursive JSON traversal → SQL predicate string generation.
- **Output:** SQL string: `SELECT columns FROM table WHERE predicates`.
- **Constraints:** Relies on JSON structure correctness. No parameterized queries — filter values are interpolated directly into SQL strings.
- **Risks:** **SQL injection risk** if filter definitions are not validated/sanitized before storage. Since filters come from admin-defined role data (not end-user input), the risk is limited to compromised admin accounts or catalog corruption.
- **Confidence:** Explicit.

### 5.8 Legacy Format Migration

- **Description:** `_normalize_tables()` transparently converts old list-based table definitions (`["*"]`, `["t1", "t2"]`) to the current per-table dict format.
- **Business purpose:** Backward compatibility — roles created before the per-table RBAC redesign continue to work without migration.
- **Trigger:** Any access check or read restriction call.
- **Processing:** Type-check on `role_tables`; if list, convert each entry to `{"columns": ["*"], "filters": ["*"]}`.
- **Confidence:** Explicit (code comments reference "legacy" format).

---

## 6. Classes, Functions, and Methods

### 6.1 `permissions.py`

| Name | Type | Purpose | Params | Returns | Importance |
|---|---|---|---|---|---|
| `Permission` | Enum | Defines five permission types | — | — | Critical |
| `RoleType` | Enum | Defines five role types with string values | — | — | Critical |
| `ROLE_PERMISSIONS` | Dict | Static matrix mapping `RoleType` → `Set[Permission]` | — | — | Critical |
| `has_permission()` | Function | Checks if a role type has a specific permission | `role_type: RoleType, permission: Permission` | `bool` | Critical |
| `get_role_permissions()` | Function | Returns permission list(s) for role(s); supports single-role or all-roles queries | `role_name: Optional[str]` | `Optional[Dict[str, List[str]]]` | Important |

### 6.2 `role_manager.py` — `RoleManager`

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `__init__()` | Method | Initializes manager, bootstraps storage and superadmin role | Calls `_init_role_storage()` | Critical |
| `_init_role_storage()` | Method | Ensures meta key exists; creates default superadmin with distributed lock | Double-checked locking via `acquire_simple_lock()` | Critical |
| `create_role()` | Method | Creates a new role with UUID; idempotent on `role_name` | Validates via `RowColumnSecurity.prepare()`; stores to Redis | Critical |
| `update_role()` | Method | Updates role content in-place; returns new content hash | Merges fields; re-validates; `role_id` stays stable | Important |
| `delete_role()` | Method | Deletes role and atomically strips from all users | Delegates to `rbac_delete_role()` | Important |
| `get_role()` | Method | Retrieves role by ID | Returns `{}` if not found | Supporting |
| `get_role_by_name()` | Method | Retrieves role by unique name (case-insensitive) | Two-step: name→ID→details | Important |
| `list_roles()` | Method | Lists all role documents | — | Supporting |
| `get_roles_by_type()` | Method | Gets all roles of a given type | Iterates over IDs from Redis | Supporting |
| `get_superadmin_role_id()` | Method | Returns superadmin role ID | — | Important |

### 6.3 `user_manager.py` — `UserManager`

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `__init__()` | Method | Initializes manager, bootstraps storage and superuser | Calls `_init_user_storage()` | Critical |
| `_ensure_default_superuser()` | Method | Creates or repairs the default superuser | Checks existence; adds missing superadmin role; creates if absent | Critical |
| `create_user()` | Method | Creates user with UUID; idempotent on username | Validates all role IDs exist; records timestamps in ms | Critical |
| `get_user()` | Method | Retrieves user by ID | Raises `ValueError` if not found | Supporting |
| `get_user_by_name()` | Method | Retrieves user by username (case-insensitive) | — | Important |
| `modify_user()` | Method | Updates username and/or roles | Validates role existence; checks username uniqueness on rename | Important |
| `delete_user()` | Method | Deletes user; protects superuser | Raises `ValueError` if target is `superuser` | Important |
| `add_role()` | Method | Adds a role to a user (atomic, idempotent) | Validates role exists | Important |
| `remove_role()` | Method | Removes a role from a user (atomic) | — | Important |
| `get_or_create_default_user()` | Method | Returns superuser ID, creating if needed | — | Supporting |

### 6.4 `row_column_security.py` — `RowColumnSecurity`

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `__init__()` | Method | Initializes value object from role string and table defs | Converts role string to `RoleType` enum | Important |
| `sort_all()` | Method | Deduplicates and sorts per-table column lists | Ensures deterministic hashing | Supporting |
| `to_json()` | Method | Returns dict representation | — | Supporting |
| `create_content_hash()` | Method | Computes MD5 hash of sorted JSON | Used for change detection | Important |
| `prepare()` | Method | Validates, applies defaults, sorts, and hashes | Entry point for normalization pipeline | Important |

### 6.5 `filter_builder.py` — `FilterBuilder`

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `__init__()` | Method | Constructs filter query from table name, columns, and role filters | Calls `build_filter_query()` immediately | Important |
| `json_to_sql_clause()` | Method | Recursively converts JSON filter tree to SQL predicates | Handles `AND`/`OR`/`NOT`, range, value, null, ILIKE with escape | Critical |
| `build_filter_query()` | Method | Assembles full `SELECT … FROM … WHERE …` string | Combines `format_column_list()` with predicates | Important |
| `format_column_list()` | Function | Formats column list for SQL SELECT | Wildcards or quoted column names | Supporting |

### 6.6 `access_control.py`

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `_resolve_role()` | Function | Resolves role name to role document | Raises `PermissionError` if not found | Important |
| `_normalize_tables()` | Function | Converts legacy list format to dict format | Type-based branching | Important |
| `_resolve_table_entry()` | Function | Looks up table entry with `"*"` fallback | — | Important |
| `_check_table_access()` | Function | Raises if table not covered by role | — | Important |
| `_check_operation_access()` | Function | Shared enforcement for table-scoped operations | Combines role resolution, permission check, table check | Critical |
| `check_control_access()` | Function | Checks CONTROL permission | Wraps `_check_operation_access()` | Critical |
| `check_create_access()` | Function | Checks CREATE permission | Wraps `_check_operation_access()` | Critical |
| `check_write_access()` | Function | Checks WRITE permission | Wraps `_check_operation_access()` | Critical |
| `check_meta_access()` | Function | Checks META permission | Wraps `_check_operation_access()` | Critical |
| `restrict_read_access()` | Function | Validates read access and builds RBAC view definitions | Two-phase: validate physical tables, then build per-alias views | Critical |

---

## 7. End-to-End Workflows

### 7.1 Mutating Operation Authorization (Write/Create/Control/Meta)

1. **Trigger:** An upstream component (API handler / query engine) calls e.g. `check_write_access(super_name, org, role_name, table_name)`.
2. Instantiate `RoleManager(super_name, org)`.
3. Resolve `role_name` to role document from Redis.
4. Extract `role` type string from document; convert to `RoleType` enum.
5. Check `has_permission(role_type, Permission.WRITE)` against static matrix.
6. Normalize role's `tables` field (handle legacy list format).
7. Look up `table_name` in normalized tables (with `"*"` fallback).
8. **Success:** Return silently — operation is authorized.
9. **Failure at any step:** Raise `PermissionError` with descriptive message.

### 7.2 Read Access Restriction and View Definition Generation

1. **Trigger:** Query engine calls `restrict_read_access(super_name, org, role_name, tables, physical_tables)`.
2. Resolve role; check READ permission.
3. **Short-circuit:** If `superadmin` or `admin` → return `{}`.
4. Normalize and inspect role tables.
5. **Short-circuit:** If only `"*"` entry with all-wildcard → return `{}`.
6. **Phase 1 — Physical table validation:**
   - For each physical table in query, verify table entry exists.
   - If column restrictions exist and query specifies columns (not `*`), compute `requested - allowed` → if non-empty, raise `PermissionError`.
7. **Phase 2 — Per-alias view construction:**
   - For each alias, resolve table entry.
   - If column restrictions exist, record `allowed_columns`.
   - If filter restrictions exist, instantiate `FilterBuilder` → extract `WHERE` clause.
   - If any restriction applies, create `RbacViewDef(allowed_columns, where_clause)`.
8. **Output:** Return `Dict[alias, RbacViewDef]`.
9. **Downstream:** Query engine creates filtered views per alias before executing the user's SQL.

### 7.3 Role Creation

1. **Trigger:** Admin API call → `RoleManager.create_role(data)`.
2. If `role_name` given, check uniqueness via Redis lookup → if exists, return existing ID (idempotent).
3. Instantiate `RowColumnSecurity` with role type and table definitions.
4. Call `prepare()` → apply defaults, sort columns, compute content hash.
5. Generate UUID hex for `role_id`.
6. Build role document (role type, tables, role_id, content_hash, role_name).
7. Persist via `rbac_create_role()`.
8. Return `role_id`.

### 7.4 User Creation with Role Assignment

1. **Trigger:** Admin API call → `UserManager.create_user({"username": "analyst1", "roles": [role_id]})`.
2. Validate `username` is provided.
3. Check if username already exists → if yes, return existing ID (idempotent).
4. Validate every `role_id` in `roles` exists in Redis.
5. Generate UUID hex for `user_id`; record creation timestamp.
6. Persist user document via `rbac_create_user()`.
7. Return `user_id`.

### 7.5 Auto-Bootstrap (Superadmin + Superuser)

1. **Trigger:** First instantiation of `RoleManager` for a given `(org, super_name)`.
2. Initialize meta key in Redis.
3. Check if superadmin role ID exists → if not:
   a. Acquire distributed lock (`roles_init`, TTL=10s, timeout=30s).
   b. Double-check (another process may have created it).
   c. Create superadmin role with `{"*": {"columns": ["*"], "filters": ["*"]}}`.
   d. Release lock.
4. **Trigger:** First instantiation of `UserManager`.
5. Initialize user meta key.
6. Check if `superuser` username exists:
   - If exists but missing superadmin role → add role.
   - If not exists → create user with superadmin role.

---

## 8. Data Model and Information Flow

### Core Entities

| Entity | Identity | Key Fields | Storage |
|---|---|---|---|
| **Role** | `role_id` (UUID hex) | `role` (RoleType string), `role_name` (optional, unique), `tables` (dict), `content_hash` (MD5) | Redis via `RedisCatalog` |
| **User** | `user_id` (UUID hex) | `username` (unique, case-insensitive), `roles` (list of role_ids), `created_ms`, `modified_ms` | Redis via `RedisCatalog` |
| **RbacViewDef** | Per-alias (transient) | `allowed_columns` (list), `where_clause` (string) | In-memory, passed to query engine |

### Per-Table Definition Schema

```json
{
  "columns": ["col1", "col2"],      // or ["*"] for all
  "filters": [                       // or ["*"] for no filtering
    {
      "status": {
        "operation": "=",
        "type": "value",
        "value": "completed"
      }
    }
  ]
}
```

### Filter JSON Schema (Recursive)

Supports:
- **Simple comparison:** `{"column": {"operation": "=", "type": "value", "value": "x"}}`
- **Null check:** `{"column": {"operation": "IS", "type": "null"}}`
- **Column reference:** `{"column": {"operation": ">", "type": "column", "value": "other_col"}}`
- **Range:** `{"column": {"range": [{"type": "value", "operation": ">=", "value": "10"}, {"type": "value", "operation": "<=", "value": "100"}]}}`
- **ILIKE with escape:** `{"column": {"operation": "ILIKE", "type": "value", "value": "%pattern%", "escape": "\\"}}`
- **Boolean composition:** `{"AND": [...]}`, `{"OR": [...]}`, `{"NOT": {...}}`
- **List of conditions:** `[cond1, cond2]` → joined with `AND`

### Information Flow

```
Admin defines role (JSON) → RowColumnSecurity.prepare() → normalized + hashed → Redis
                                                                     ↓
User executes query → access_control → RoleManager.get_role_by_name() → Redis
                                                                     ↓
                                          Permission matrix check (in-memory)
                                                                     ↓
                                          Physical table/column validation
                                                                     ↓
                                          FilterBuilder → SQL WHERE clause
                                                                     ↓
                                          RbacViewDef per alias → Query Engine
```

### Stateful vs Stateless

- **Stateful:** Role and user definitions in Redis. Role changes take effect immediately for all assigned users.
- **Stateless:** Every authorization check re-reads from Redis. No in-process caching is visible in this code (caching may exist in `RedisCatalog`).

---

## 9. Dependencies and Integrations

### Internal Dependencies

| Dependency | Module | Why It Matters |
|---|---|---|
| `supertable.redis_catalog.RedisCatalog` | `role_manager.py`, `user_manager.py` | **All persistence.** Every role/user operation delegates to Redis via this catalog. |
| `supertable.config.defaults.logger` | Multiple | Centralized logging. |
| `supertable.data_classes.TableDefinition` | `access_control.py` | Carries parsed table metadata (name, alias, columns) from SQL parser. |
| `supertable.data_classes.RbacViewDef` | `access_control.py` | Output DTO for RBAC view definitions (lazy import). |
| `supertable.utils.sql_parser.SQLParser` | `access_control.py` (import only) | Imported but not directly called in this module; callers use it upstream. |

### Standard Library

| Package | Usage |
|---|---|
| `uuid` | Generating stable UUIDs for roles and users |
| `json` | Serialization for content hashing and role updates |
| `hashlib` | MD5 content hash for change detection |
| `time` | Millisecond timestamps for user creation |
| `enum` | Permission and role type definitions |

### External Systems

| System | Role |
|---|---|
| **Redis** | Primary persistent store for role and user data, distributed locking |

### Configuration / Environment

- `organization` and `super_name` — multi-tenant scoping keys passed to every operation.
- Redis connection configuration is encapsulated in `RedisCatalog` (not visible in these files).

---

## 10. Architecture Positioning

### System Context

```
┌──────────────┐     ┌───────────────┐     ┌─────────────────┐
│  API / MCP   │────▶│ Access Control│────▶│  Query Engine   │
│  Gateway     │     │  (this code)  │     │  (DuckDB/SQL)   │
└──────────────┘     └───────┬───────┘     └────────┬────────┘
                             │                      │
                     ┌───────▼───────┐     ┌────────▼────────┐
                     │ RedisCatalog  │     │  Data Storage   │
                     │ (roles/users) │     │ (Parquet/etc.)  │
                     └───────────────┘     └─────────────────┘
```

- **Upstream callers:** API handlers, MCP tool handlers, query execution pipeline.
- **Downstream dependencies:** `RedisCatalog` (Redis), and indirectly the query engine that consumes `RbacViewDef`.
- **Layer:** This is **authorization middleware / domain logic** — it does not execute queries itself, but shapes how queries are executed.
- **Boundaries:** Clean boundary at `PermissionError` (deny) and `Dict[str, RbacViewDef]` (allow with restrictions). The query engine is responsible for applying the view definitions.
- **Coupling:** Tightly coupled to `RedisCatalog`'s RBAC-specific methods. Loosely coupled to the query engine via the `RbacViewDef` data class.
- **Scalability:** Authorization checks hit Redis on every call (no in-memory cache visible). This could be a performance bottleneck at high query volume but ensures instant policy propagation.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Multi-user data access governance — ensuring the right people see the right data. |
| **Why it matters** | Without this, Supertable could only offer all-or-nothing access, making it unsuitable for enterprise, regulated, or multi-team environments. |
| **Revenue relation** | Likely a **gating feature for enterprise/team plans**. Per-table, per-column, per-row access control is a premium governance capability. |
| **Compliance relation** | Directly enables GDPR (column-level PII restriction), HIPAA (row-level patient filtering), SOX (write-access controls) compliance patterns. |
| **Integration relation** | Enables safe exposure of data via MCP/API to external BI tools, LLMs, and automated pipelines without data leakage risk. |
| **Product value if removed** | Supertable would be single-user or trust-everyone — unsuitable for any enterprise or multi-tenant deployment. |
| **Differentiating vs commodity** | The combination of column-level + row-level + JSON-defined filters on a SQL-based analytical platform is **differentiating** for the managed data product space. Commodity RBAC typically only covers table-level access. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate. The permission matrix is simple; the real complexity is in `restrict_read_access()` with its two-phase validation and per-alias view construction. |
| **Architectural maturity** | Good. Clean separation of concerns: permissions (static), role management (CRUD), user management (CRUD), access enforcement (policy engine), filter translation (builder). Backward-compatible aliases indicate an evolving system. |
| **Maintainability** | High. Well-structured helper functions, clear naming, docstrings. Legacy format handling is isolated. |
| **Extensibility** | Moderate. Adding a new permission requires enum + matrix update. Adding new filter operators requires `json_to_sql_clause()` modification. Per-table schema is flexible. |
| **Operational sensitivity** | High. Any bug in access control is a data leakage or access denial incident. The bootstrap locking mechanism is critical for multi-process deployments. |
| **Performance** | Every authorization check hits Redis. No visible caching layer. For high-throughput query scenarios, this could be a bottleneck. |
| **Reliability** | Depends on Redis availability. If Redis is down, all authorization checks fail. The distributed lock has a 30s timeout. |
| **Security** | Generally sound. However, `FilterBuilder` constructs SQL via string interpolation from stored filter definitions — safe as long as filter definitions are admin-only and validated at storage time. No visible input sanitization in `FilterBuilder` itself. Content hashing uses MD5 (not security-critical here; used only for change detection). |
| **Testing** | No test files provided. The code's structure (small functions, clear inputs/outputs, minimal side effects beyond Redis) is amenable to unit testing. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Five permission types (`CONTROL`, `CREATE`, `WRITE`, `READ`, `META`) and five role types (`superadmin`, `admin`, `writer`, `reader`, `meta`) with a fixed permission matrix.
- Roles are identified by stable UUIDs; role names are optional but unique (case-insensitive).
- Users are identified by stable UUIDs; usernames are unique (case-insensitive) and mutable.
- Per-table access control supports column-level projection and row-level filtering via JSON filter specs.
- Superadmin and admin roles bypass all read-time filtering.
- Default superadmin role and superuser are auto-created with distributed locking.
- Legacy list-based table definitions are transparently normalized.
- `FilterBuilder` outputs SQL via string interpolation (not parameterized).
- All persistence is delegated to `RedisCatalog`.
- The superuser cannot be deleted.
- Role deletion atomically removes the role from all users.

### Reasonable Inferences

- **Supertable is an analytical data platform** — the SQL-centric model, DuckDB hints (from user context), column/row-level security, and the concept of "supers" point to a managed analytical layer over tabular data.
- **`RedisCatalog` is a centralized metadata store** — all RBAC, and likely all catalog metadata, lives in Redis.
- **The query engine creates filtered views at execution time** — `RbacViewDef` is consumed by a downstream component that wraps the original table references in CTEs or views with column projection and `WHERE` clauses.
- **`SQLParser` is used upstream** — `access_control.py` imports it and expects its outputs (`TableDefinition` lists) as input parameters.
- **This RBAC system is multi-tenant** — the `(organization, super_name)` scoping suggests multiple organizations each with multiple "supers" (logical data namespaces).
- **Filter definitions are authored by admins via an API** — the JSON format is too complex for direct user input; it's likely generated by an admin UI or API.

### Unknown / Not Visible in Provided Snippet

- **Authentication mechanism** — how `role_name` is resolved from the caller's identity (e.g., JWT, API key, session). No auth logic is present.
- **`RedisCatalog` internals** — the exact Redis data structures (hashes, sets, sorted sets), key naming conventions, and atomic operation semantics.
- **`RbacViewDef` full definition** — only `allowed_columns` and `where_clause` fields are used here; other fields may exist.
- **`SQLParser` behavior** — how `physical_tables` and `tables` are derived from SQL.
- **Admin API surface** — how roles and users are created/modified via HTTP endpoints.
- **Audit logging** — whether access checks are logged for compliance.
- **Caching** — whether `RedisCatalog` implements any read caching.
- **Rate limiting or abuse protection** — not visible.
- **Filter definition validation at storage time** — whether the JSON filter spec is validated before being stored to Redis.
- **How `meta` permission is used** — the check exists (`check_meta_access`) but the business meaning of "META" access (beyond ALTER operations) is not fully clear.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `Supertable RBAC` (Role-Based Access Control)
- **Alternative names / aliases:** `rbac`, `access_control`, `row_column_security`, `permissions`
- **Package path:** `supertable.rbac`
- **Main responsibilities:** Permission enforcement, role CRUD, user CRUD, per-table column/row security, SQL filter generation, RBAC view definition construction
- **Important entities:** `Permission` (enum), `RoleType` (enum), `RoleManager`, `UserManager`, `RowColumnSecurity`, `FilterBuilder`, `RbacViewDef`, `TableDefinition`
- **Important workflows:** mutating-operation-authorization, read-access-restriction, role-creation, user-creation, auto-bootstrap
- **Integration points:** `RedisCatalog` (persistence), `SQLParser` (upstream query parsing), query engine (downstream view application)
- **Business purpose keywords:** data governance, access control, column-level security, row-level security, multi-tenant, compliance, RBAC, least-privilege
- **Architecture keywords:** authorization middleware, permission matrix, filter builder, view definition, Redis-backed catalog, distributed locking, multi-tenant scoping
- **Follow-up files for completeness:**
  - `supertable/redis_catalog.py` — persistence layer, key schemas, atomic operations
  - `supertable/data_classes.py` — `RbacViewDef`, `TableDefinition` definitions
  - `supertable/utils/sql_parser.py` — how `physical_tables` and `tables` are extracted from SQL
  - `supertable/rbac/` API endpoint handlers — how roles/users are managed via HTTP
  - Query engine integration — how `RbacViewDef` is applied to create filtered views

---

## 15. Suggested Documentation Tags

`rbac`, `access-control`, `authorization`, `permissions`, `role-management`, `user-management`, `column-level-security`, `row-level-security`, `filter-builder`, `sql-filter-generation`, `multi-tenant`, `redis`, `data-governance`, `compliance`, `enterprise-security`, `permission-matrix`, `domain-logic`, `middleware`, `auto-bootstrap`, `distributed-locking`, `supertable`, `data-platform`

---

## Merge Readiness

| Field | Value |
|---|---|
| **Suggested canonical name** | `Supertable RBAC Subsystem` |
| **Standalone or partial** | **Partial** — this is the authorization logic layer. Complete understanding requires the persistence layer (`RedisCatalog`), query engine integration, API endpoints, and data class definitions. |
| **Related components for merge** | `supertable.redis_catalog`, `supertable.data_classes`, `supertable.utils.sql_parser`, RBAC API handlers, query engine RBAC view application logic |
| **Files that would most improve certainty** | `redis_catalog.py` (persistence semantics), `data_classes.py` (DTO schemas), query engine code that consumes `RbacViewDef` |
| **Recommended merge key phrases** | `rbac`, `access_control`, `role_manager`, `user_manager`, `permissions`, `filter_builder`, `row_column_security`, `RbacViewDef`, `RedisCatalog` |
