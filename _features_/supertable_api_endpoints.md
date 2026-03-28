# SuperTable Reflection API — Endpoint Reference

Extracted from the Admin UI HTML templates. All endpoints are under the `/reflection` prefix.
Common query parameters `org` (organization) and `sup` (super_name) appear on most endpoints.

---

## 1. Authentication

### POST `/reflection/login`
Form-encoded login.

| Field | Type | Notes |
|-------|------|-------|
| `mode` | `"user"` \| `"super"` | Hidden field, set by UI toggle |
| `username` | string | User mode only |
| `token` | string | User mode — access token |
| `supertoken` | string | Superuser mode — SUPERTABLE_SUPERTOKEN |

### GET `/reflection/logout`
Ends the session.

### POST `/reflection/set-role`
Persists the active role to the server session.

**Body (JSON):**
```json
{ "role_name": "analyst" }
```

---

## 2. Tenant / SuperTable Management

### GET `/reflection/supers`
List available SuperTables for an organization.

| Param | Type |
|-------|------|
| `organization` | string |

### GET `/reflection/super-meta`
Fetch metadata for a specific SuperTable (tables, schema summary).

| Param | Type | Required |
|-------|------|----------|
| `organization` | string | yes |
| `super_name` | string | yes |
| `role_name` | string | no |

### GET `/reflection/super`
Fetch full super metadata (tables list, etc.).

| Param | Type | Required |
|-------|------|----------|
| `organization` | string | yes |
| `super_name` | string | yes |
| `role_name` | string | no |

### POST `/reflection/super`
Create a new SuperTable.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

### DELETE `/reflection/super`
Delete a SuperTable.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |
| `user_hash` | string |

### GET `/reflection/user-role-names`
Get role names assigned to the current user for a given super.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

---

## 3. Schema & Query Execution

### POST `/reflection/schema`
Fetch table + column schemas.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main",
  "role_name": "analyst"
}
```

### POST `/reflection/execute`
Execute a SQL query.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main",
  "role_name": "analyst",
  "query": "SELECT * FROM sales LIMIT 10",
  "engine": "auto",
  "page": 1,
  "page_size": 100
}
```

---

## 4. Table Management

### GET `/reflection/stats`
Fetch table statistics (row count, columns, resources).

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |
| `table` | string |
| `role_name` | string |

### GET `/reflection/table/config`
Get per-table configuration (memory chunk size, max overlapping files).

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |
| `table` | string |

### PUT `/reflection/table/config`
Update per-table configuration.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |
| `table` | string |

**Body (JSON):**
```json
{
  "max_memory_chunk_size": 16777216,
  "max_overlapping_files": 10
}
```
Values can be `null` to inherit global defaults.

### DELETE `/reflection/table`
Delete a table.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |
| `table` | string |
| `role_name` | string |

---

## 5. RBAC — Roles

### GET `/reflection/rbac/roles`
List all roles.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

(Also accepts `org` / `sup` shorthand.)

### POST `/reflection/rbac/roles`
Create a new role.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main",
  "role_name": "analyst",
  "role": "reader",
  "tables": ["sales", "products"],
  "columns": {},
  "filters": {}
}
```

### PUT `/reflection/rbac/roles/{role_id}`
Update an existing role.

**Body (JSON):** Same shape as create.

### DELETE `/reflection/rbac/roles/{role_id}`
Delete a role.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

---

## 6. RBAC — Users

### GET `/reflection/users-page/list`
List all users (with their roles).

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

### GET `/reflection/rbac/users`
Fallback user list endpoint.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

### POST `/reflection/users-page/create`
Create a new user.

**Body (JSON):**
```json
{
  "username": "alice",
  "role_type": "reader",
  "org": "acme",
  "sup": "main"
}
```

### DELETE `/reflection/users-page/user/{user_id}`
Delete a user.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

### POST `/reflection/users-page/user/{user_id}/role`
Add a role to a user.

**Body (JSON):**
```json
{
  "org": "acme",
  "sup": "main",
  "role_id": "role_abc"
}
```

### DELETE `/reflection/users-page/user/{user_id}/role/{role_id}`
Remove a role from a user.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

---

## 7. Tokens

### GET `/reflection/tokens`
List tokens.

| Param | Type |
|-------|------|
| `org` | string |

### POST `/reflection/tokens`
Create a new API token.

| Param | Type |
|-------|------|
| `org` | string |
| `label` | string |

**Response:** `{ "token": "...", "token_id": "..." }`

### DELETE `/reflection/tokens/{token_id}`
Delete a token.

| Param | Type |
|-------|------|
| `org` | string |

---

## 8. OData Endpoints

### GET `/reflection/odata/endpoints`
List OData endpoints.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

### POST `/reflection/odata/endpoints`
Create an OData endpoint.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main",
  "label": "Sales feed",
  "role_id": "role_abc",
  "role_name": "analyst"
}
```

### PUT `/reflection/odata/endpoints/{endpoint_id}`
Toggle enable/disable an OData endpoint.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main",
  "enabled": true
}
```

### POST `/reflection/odata/endpoints/{endpoint_id}/regenerate`
Regenerate the authentication token for an OData endpoint.

**Body (JSON):**
```json
{
  "organization": "acme",
  "super_name": "main"
}
```

**Response:** `{ "token": "...", "data": { "odata_url": "/odata/..." } }`

### DELETE `/reflection/odata/endpoints/{endpoint_id}`
Delete an OData endpoint.

| Param | Type |
|-------|------|
| `organization` | string |
| `super_name` | string |

---

## 9. Ingestion — Staging Areas

### GET `/reflection/ingestion/stagings`
List staging area names.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

**Response:** `{ "staging_names": ["stg_raw", ...] }`

### POST `/reflection/staging/create`
Create a new staging area.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |

### POST `/reflection/staging/delete`
Delete a staging area.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |

### GET `/reflection/ingestion/staging/files`
List files in a staging area.

| Param | Type | Default |
|-------|------|---------|
| `org` | string | |
| `sup` | string | |
| `staging_name` | string | |
| `offset` | int | 0 |
| `limit` | int | 20 |

**Response:** `{ "items": [{ "file": "...", "rows": 100, "written_at_ns": ... }], "total": 50 }`

---

## 10. Ingestion — Pipes

### GET `/reflection/ingestion/pipes`
List pipes for a staging area.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |

### GET `/reflection/ingestion/pipe/meta`
Get pipe metadata/definition.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |
| `pipe_name` | string |

### POST `/reflection/pipes/save`
Create or update a pipe definition.

**Body (JSON):** Full pipe definition object:
```json
{
  "organization": "acme",
  "super_name": "main",
  "staging_name": "stg_raw",
  "pipe_name": "pipe_01",
  "role_name": "admin",
  "simple_name": "...",
  "overwrite_columns": "day",
  "enabled": true
}
```

### POST `/reflection/pipes/enable`
Enable a pipe.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |
| `pipe_name` | string |

### POST `/reflection/pipes/disable`
Disable a pipe.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |
| `pipe_name` | string |

### POST `/reflection/pipes/delete`
Delete a pipe.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `staging_name` | string |
| `pipe_name` | string |

---

## 11. Ingestion — Data Load

### POST `/reflection/ingestion/load/upload`
Upload a file to load data into a table or staging area. **Multipart form data.**

| Field | Type | Notes |
|-------|------|-------|
| `org` | string | |
| `sup` | string | |
| `role_name` | string | |
| `mode` | `"table"` \| `"staging"` | |
| `table_name` | string | When mode = table |
| `staging_name` | string | When mode = staging |
| `overwrite_columns` | string | Optional, comma-separated column names |
| `file` | binary | The uploaded file |

**Response:** `{ "rows": 1000, "job_uuid": "...", "server_duration_ms": 123.4 }`

### GET `/reflection/ingestion/tables`
List available tables for the load target dropdown.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |
| `role_name` | string |

### GET `/reflection/ingestion/recent-writes`
List recent write operations.

| Param | Type | Default |
|-------|------|---------|
| `org` | string | |
| `sup` | string | |
| `limit` | int | 50 |

**Response:** `{ "items": [{ "inserted": 500, "duration": 1.234, ... }] }`

---

## 12. Compute / Engine Pools

### GET `/reflection/compute/list`
List compute engine pools.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

### POST `/reflection/compute/upsert`
Create or update a compute engine pool.

**Body (JSON):**
```json
{
  "org": "acme",
  "sup": "main",
  "item": {
    "id": "",
    "name": "spark-pool-1",
    "kind": "spark-thrift",
    "thrift_host": "spark.internal",
    "thrift_port": 10000,
    "status": "active",
    "s3_enabled": true,
    "s3_endpoint": "",
    "min_bytes": 0,
    "max_bytes": 0,
    "is_default": false,
    "size": "small",
    "max_concurrency": 1,
    "has_internet": false,
    "ws_url": ""
  }
}
```

### POST `/reflection/compute/test-connection`
Test connectivity to a compute engine.

**Body (JSON):**
```json
{
  "org": "acme",
  "sup": "main",
  "kind": "spark-thrift",
  "thrift_host": "spark.internal",
  "thrift_port": 10000
}
```

**Response:** `{ "ok": true, "message": "Connected" }`

### DELETE `/reflection/compute/{pool_id}`
Delete a compute engine pool.

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

---

## 13. Monitoring

### GET `/reflection/monitoring/reads`
List recent read operations.

| Param | Type | Notes |
|-------|------|-------|
| `org` | string | |
| `sup` | string | |
| `limit` | int | Default 500 |
| `from_ts` | timestamp | Optional lower bound |
| `to_ts` | timestamp | Optional upper bound |

**Response:** `{ "items": [...] }`

### GET `/reflection/monitoring/writes`
List recent write operations.

| Param | Type | Notes |
|-------|------|-------|
| `org` | string | |
| `sup` | string | |
| `limit` | int | Default 500 |
| `from_ts` | timestamp | Optional lower bound |
| `to_ts` | timestamp | Optional upper bound |

**Response:** `{ "items": [...] }`

---

## 14. Data Quality

All `/reflection/quality/*` endpoints automatically append `org` and `sup` query params.

### GET `/reflection/quality/overview`
Dashboard overview — aggregated quality scores.

### GET `/reflection/quality/tables`
List tables with their quality status.

### POST `/reflection/quality/run`
Run a quality check on a single table.

| Param | Type | Values |
|-------|------|--------|
| `table` | string | |
| `mode` | string | `"quick"` \| `"deep"` |

### POST `/reflection/quality/run-all`
Run quality checks on all tables.

| Param | Type | Values |
|-------|------|--------|
| `mode` | string | `"quick"` \| `"deep"` |

### GET `/reflection/quality/latest`
Get latest quality results for a table.

| Param | Type |
|-------|------|
| `table` | string |

### GET `/reflection/quality/history`
Get historical quality data.

| Param | Type | Default |
|-------|------|---------|
| `days` | int | 7 |
| `table` | string | Optional filter |

---

### Custom Rules

### GET `/reflection/quality/rules`
List all custom quality rules.

### POST `/reflection/quality/rules`
Create a new custom rule.

**Body (JSON):**
```json
{
  "table_name": "sales",
  "column_name": "revenue",
  "rule_type": "column_min",
  "threshold": 0.0,
  "severity": "warning",
  "description": "Revenue should never be negative",
  "expected_values": [],
  "sql": ""
}
```
`rule_type` values include: `column_min`, `column_max`, `not_null`, `distinct_in`, `custom_sql`, `freshness_check`.
`freshness_check` is internally converted to `custom_sql` with generated SQL.

### PUT `/reflection/quality/rules?rule_id={id}`
Update an existing rule.

**Body (JSON):** Same shape as create.

### DELETE `/reflection/quality/rules?rule_id={id}`
Delete a rule.

---

### Built-in Checks Configuration

### GET `/reflection/quality/global-config`
Get global quality check configuration (toggle checks, thresholds).

**Response:** `{ "ok": true, "config": { "checks": { ... } }, "builtin_checks": [...] }`

### PUT `/reflection/quality/global-config`
Save global quality check configuration.

**Body (JSON):**
```json
{
  "checks": {
    "null_ratio": { "enabled": true, "threshold": 0.05 },
    "duplicate_rows": { "enabled": false }
  }
}
```

---

### Schedule

### GET `/reflection/quality/schedule`
Get global quality schedule.

### PUT `/reflection/quality/schedule`
Update global quality schedule.

**Body (JSON):**
```json
{
  "quick_cron": "0 */4 * * *",
  "deep_cron": "0 2 * * *",
  "custom_cron": "0 */6 * * *",
  "post_ingest_quick": true,
  "post_ingest_custom": false,
  "post_ingest_deep": false,
  "post_ingest": true,
  "enabled": true,
  "cooldown_seconds": 300
}
```

### GET `/reflection/quality/table-schedules`
List all per-table schedule overrides.

### PUT `/reflection/quality/table-schedule`
Set a per-table schedule override.

| Param | Type |
|-------|------|
| `table` | string |

**Body (JSON):** Schedule override object (same fields as global, plus table-specific flags like `custom_enabled`).

### DELETE `/reflection/quality/table-schedule`
Remove a per-table schedule override.

| Param | Type |
|-------|------|
| `table` | string |

---

## 15. Miscellaneous

### GET `/reflection/roles`
List roles (used by ingestion page).

| Param | Type |
|-------|------|
| `org` | string |
| `sup` | string |

---

## Page Navigation (GET, returns HTML)

| Path | Description |
|------|-------------|
| `/reflection/home` | Home dashboard |
| `/reflection/execute` | SQL editor |
| `/reflection/tables` | Table management |
| `/reflection/ingestion` | Data ingestion |
| `/reflection/quality` | Data quality |
| `/reflection/monitoring` | Read/write monitoring |
| `/reflection/engine` | Compute engine pools |
| `/reflection/security` | RBAC, users, tokens, OData |
| `/reflection/studio` | Studio |
| `/reflection/notebook` | Notebook |
| `/reflection/jobs` | Jobs |
| `/reflection/vault` | Secrets vault |
| `/reflection/compute` | Compute pools |
| `/reflection/environments` | Environments |
| `/reflection/env` | Env variables |
