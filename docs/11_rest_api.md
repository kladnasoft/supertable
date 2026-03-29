# Data Island Core — REST API

## Overview

The REST API is the single point of access for all data operations in Data Island Core. It exposes 76 JSON endpoints covering SQL execution, table management, ingestion, RBAC, monitoring, data quality, engine configuration, OData, audit logging, and user management. The WebUI proxies all data operations through this API. External tools and scripts call it directly with bearer tokens.

The API is a FastAPI application defined in `api/application.py` with all route handlers in `api/api.py`. It runs on port 8051 by default (or port 8090 when launched via the `api` Docker Compose profile).

---

## Authentication

### Session cookies (WebUI)

The WebUI server handles login and sets two cookies:

- `st_session` — HMAC-signed JSON payload containing user identity, role, organization, and expiry. Signed with `SUPERTABLE_SESSION_SECRET`. Max age: 24 hours.
- `st_admin_token` — superuser token (set only on superuser login)

The API server validates these cookies on every request via `inject_session_into_ctx()` in `server_common.py`.

### Bearer tokens (API)

External tools authenticate using tokens created through the token management endpoints. The token is passed as:

```
Authorization: Bearer <token>
```

Or via the configured header (default `X-API-Key`):

```
X-API-Key: <token>
```

Tokens are validated by `catalog.validate_auth_token()` — the token is SHA-256 hashed and compared against stored hashes in Redis.

### Auth guards

Endpoints use FastAPI dependency injection for auth:

- `admin_guard_api` — requires superuser token. Used for admin operations.
- `session_guard` — requires a valid session cookie. Used for WebUI-proxied requests.
- No guard — public endpoints (`/healthz`).

---

## Endpoint reference

### Health

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/healthz` | none | Returns `ok` if Redis is reachable |

### Admin & SuperTable management

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/super-meta` | session | Get full SuperTable metadata (tables, schemas, stats) |
| `GET` | `/reflection/user-role-names` | session | Get the current user's role names |
| `GET` | `/reflection/roles` | session | List all roles |
| `POST` | `/reflection/set-role` | session | Set the active role for the session |
| `GET` | `/reflection/supers` | admin | List all SuperTables in the organization |
| `GET` | `/reflection/super` | admin | Get SuperTable info by name |
| `POST` | `/reflection/super` | admin | Create a new SuperTable |
| `DELETE` | `/reflection/super` | admin | Delete a SuperTable and all its data |

### SQL execution

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/reflection/execute` | session | Execute a SQL query. Returns JSON results + execution plan. |
| `POST` | `/reflection/schema` | session | Get schema for tables referenced in a SQL query |

The execute endpoint accepts:

```json
{
  "sql": "SELECT * FROM orders LIMIT 100",
  "org": "acme",
  "sup": "example",
  "engine": "auto",
  "limit": 10000
}
```

Response includes `data` (array of row objects), `columns`, `row_count`, `execution_plan`, `timings`, and `engine_used`.

### Tables

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/schema` | session | Get table schema (columns, types) |
| `GET` | `/reflection/stats` | session | Get table statistics (files, rows, size) |
| `DELETE` | `/reflection/table` | admin | Delete a table |
| `GET` | `/reflection/table/config` | session | Get table config (dedup, primary keys) |
| `PUT` | `/reflection/table/config` | admin | Update table config |

### Token management

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/tokens` | admin | List all tokens (metadata only, not values) |
| `POST` | `/reflection/tokens` | admin | Create a new auth token. Returns plaintext once. |
| `DELETE` | `/reflection/tokens/{token_id}` | admin | Revoke a token |

### Ingestion

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/reflection/ingestion/load/upload` | session | Upload a file (CSV/JSON/Parquet) to table or staging |
| `GET` | `/reflection/ingestion/recent-writes` | session | List recent write operations (monitoring) |
| `GET` | `/reflection/ingestion/stagings` | session | List all staging areas |
| `GET` | `/reflection/ingestion/tables` | session | List tables with ingestion-relevant metadata |
| `GET` | `/reflection/ingestion/staging/files` | session | List files in a staging area |
| `GET` | `/reflection/ingestion/pipes` | session | List all pipes |
| `GET` | `/reflection/ingestion/pipe/meta` | session | Get pipe configuration |
| `POST` | `/reflection/staging/create` | admin | Create a staging area |
| `POST` | `/reflection/staging/delete` | admin | Delete a staging area |
| `POST` | `/reflection/pipes/save` | admin | Create or update a pipe |
| `POST` | `/reflection/pipes/delete` | admin | Delete a pipe |
| `POST` | `/reflection/pipes/enable` | admin | Enable a pipe |
| `POST` | `/reflection/pipes/disable` | admin | Disable a pipe |

### Monitoring

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/monitoring/reads` | session | Get read operation metrics |
| `GET` | `/reflection/monitoring/writes` | session | Get write operation metrics |

### RBAC

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/rbac/roles` | admin | List all roles with details |
| `POST` | `/reflection/rbac/roles` | admin | Create a role |
| `PUT` | `/reflection/rbac/roles/{role_id}` | admin | Update a role |
| `DELETE` | `/reflection/rbac/roles/{role_id}` | admin | Delete a role |
| `GET` | `/reflection/rbac/users` | admin | List all users |

### User management

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/users-page/list` | admin | List users with role assignments |
| `POST` | `/reflection/users-page/create` | admin | Create a user |
| `DELETE` | `/reflection/users-page/user/{user_id}` | admin | Delete a user |
| `POST` | `/reflection/users-page/user/{user_id}/role` | admin | Assign a role to a user |
| `DELETE` | `/reflection/users-page/user/{user_id}/role/{role_id}` | admin | Remove a role from a user |

### OData

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/odata/endpoints` | admin | List OData endpoints |
| `POST` | `/reflection/odata/endpoints` | admin | Create an OData endpoint |
| `PUT` | `/reflection/odata/endpoints/{endpoint_id}` | admin | Update an OData endpoint |
| `POST` | `/reflection/odata/endpoints/{endpoint_id}/regenerate` | admin | Regenerate OData endpoint key |
| `DELETE` | `/reflection/odata/endpoints/{endpoint_id}` | admin | Delete an OData endpoint |

### Data quality

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/quality/overview` | session | Quality dashboard summary |
| `GET` | `/reflection/quality/tables` | session | Tables with quality scores |
| `GET` | `/reflection/quality/latest` | session | Latest quality check results |
| `GET` | `/reflection/quality/global-config` | admin | Get global quality configuration |
| `PUT` | `/reflection/quality/global-config` | admin | Update global quality configuration |
| `GET` | `/reflection/quality/schedule` | admin | Get quality check schedule |
| `PUT` | `/reflection/quality/schedule` | admin | Update quality check schedule |
| `PUT` | `/reflection/quality/table-schedule` | admin | Set per-table quality schedule |
| `DELETE` | `/reflection/quality/table-schedule` | admin | Remove per-table quality schedule |
| `GET` | `/reflection/quality/table-schedules` | admin | List per-table schedules |
| `GET` | `/reflection/quality/rules` | admin | List quality rules |
| `POST` | `/reflection/quality/rules` | admin | Create a quality rule |
| `PUT` | `/reflection/quality/rules` | admin | Update a quality rule |
| `DELETE` | `/reflection/quality/rules` | admin | Delete a quality rule |
| `POST` | `/reflection/quality/run` | admin | Run quality check on a table |
| `POST` | `/reflection/quality/run-all` | admin | Run quality checks on all tables |
| `GET` | `/reflection/quality/history` | session | Quality check result history |

### Engine & compute

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/engine-config` | admin | Get DuckDB/Spark engine config |
| `POST` | `/reflection/engine-config` | admin | Update engine config |
| `GET` | `/reflection/compute/list` | admin | List Spark clusters |
| `POST` | `/reflection/compute/upsert` | admin | Register/update a Spark cluster |
| `DELETE` | `/reflection/compute/{pool_id}` | admin | Remove a Spark cluster |
| `POST` | `/reflection/compute/test-connection` | admin | Test Spark Thrift connectivity |

### Audit log

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/reflection/audit/events` | admin (superuser) | Query audit events |
| `GET` | `/reflection/audit/verify` | admin (superuser) | Verify hash chain integrity |
| `GET` | `/reflection/audit/export` | admin (superuser) | Download audit events as JSON/CSV |
| `GET` | `/reflection/audit/stats` | admin (superuser) | Get audit logger statistics |

---

## Session management

Sessions are HMAC-signed JSON payloads stored in a cookie (`st_session`). The session contains:

```json
{
  "user_hash": "sha256hex...",
  "username": "analyst",
  "role_name": "analyst",
  "org": "acme",
  "is_superuser": false,
  "exp": 1711820400
}
```

The session is signed with `SUPERTABLE_SESSION_SECRET` using HMAC-SHA256. The cookie is HTTP-only and has a max age of 24 hours. When `SECURE_COOKIES=true`, the `Secure` flag is set (requires HTTPS).

Session functions in `api/session.py`:

| Function | Description |
|---|---|
| `_encode_session(data)` | Serialize and sign a session dict |
| `_decode_session(value)` | Verify signature and deserialize |
| `get_session(request)` | Extract session from request cookies |
| `is_logged_in(request)` | Check if the request has a valid session |
| `derive_superuser_hash(org)` | Compute `sha256("{org}:superuser")` |

---

## Correlation ID

Every request receives a correlation ID, either from the `X-Correlation-ID` header (if provided by the caller) or auto-generated as a UUID. The ID is propagated to all log entries, audit events, and downstream calls. The header name is configurable via `SUPERTABLE_CORRELATION_HEADER`.

---

## Error responses

All endpoints return errors in a consistent format:

```json
{
  "detail": "You don't have permission to read table 'employees'."
}
```

HTTP status codes:
- `400` — invalid request parameters
- `401` — missing or invalid authentication
- `403` — authenticated but insufficient permissions
- `404` — resource not found
- `500` — internal server error

---

## Module structure

```
supertable/api/
  application.py     FastAPI app, middleware, lifespan, startup (80 lines)
  api.py             All 76 endpoint handlers (2,860 lines)
  session.py         Cookie session management, HMAC signing (200 lines)
supertable/
  server_common.py   Auth guards, Redis client, catalog singleton (955 lines)
```

---

## Frequently asked questions

**Can I call the API without the WebUI?**
Yes. Use the `api` Docker Compose profile to run the API standalone on port 8090. Authenticate with bearer tokens or the superuser token.

**Is there rate limiting?**
No built-in rate limiting at the API level. Use a reverse proxy (Nginx, Caddy, Envoy) for rate limiting in production.

**Can I use the API from JavaScript/frontend?**
Yes. The API returns JSON and supports CORS. The WebUI's JavaScript code calls these endpoints directly via `fetch()`.

**How do I create a token for API access?**
Call `POST /reflection/tokens` with the superuser token. The response includes the plaintext token (shown once). Use this token as a bearer token for subsequent API calls.

**What is the maximum request body size?**
Limited by the ASGI server (Uvicorn). The default is 100 MB. For file uploads, the limit is governed by `SUPERTABLE_PROXY_TIMEOUT` (default: 60 seconds).
