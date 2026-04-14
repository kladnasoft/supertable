# Data Island Core — Comprehensive Security Assessment Plan

**Version:** 2.0  
**Classification:** CONFIDENTIAL  
**Compliance Frameworks:** SOC 2 Type II (Trust Services Criteria), DORA (Digital Operational Resilience Act)  
**Repository:** `https://github.com/kladnasoft/supertable/tree/master/supertable`  

---

## 1. Scope & Objectives

### 1.1 Assessment Scope

Full-spectrum security assessment of the Data Island Core platform (`supertable/` Python package): every module, every endpoint, every data flow — from user input to storage write, from agent SQL to DuckDB execution, from session cookie to Redis metadata.

**In scope:**
- 4 server processes: API (`api/application.py`), WebUI (`webui/application.py`), MCP (`mcp/web_app.py`), OData (`odata/application.py`)
- 103 Python modules (non-test), 17 HTML templates, 2 static JS/CSS bundles
- All external interfaces: REST API, WebSocket, MCP stdio/SSE, OData v4, browser UI
- All internal interfaces: Redis catalog, storage backends (local/S3/MinIO/Azure/GCP), DuckDB/Spark engines
- All trust boundaries: user → API, agent → MCP, provider → consumer (sharing), admin → RBAC config
- Supply chain: 28 direct dependencies, transitive dependency tree

**Out of scope:** Infrastructure provisioning (Kubernetes, Docker, cloud IAM), third-party SaaS integrations not in codebase, physical security controls.

### 1.2 Compliance Mapping

Every finding will be tagged with the applicable SOC 2 and DORA control:

| SOC 2 Criteria | DORA Article | Domain |
|---|---|---|
| CC6.1 — Logical access | Art. 9 — Access control | Authentication, RBAC |
| CC6.2 — Credentials | Art. 9.2 — Identity management | Secrets, tokens, sessions |
| CC6.3 — Authorization | Art. 9.3 — Least privilege | Endpoint guards, RBAC enforcement |
| CC6.6 — Boundary protection | Art. 10 — Network security | CORS, CSP, TLS, proxy |
| CC6.7 — Data transmission | Art. 10.2 — Encryption in transit | Cookie flags, TLS |
| CC6.8 — Unauthorized software | Art. 28 — Third-party risk | Dependencies, DuckDB extensions |
| CC7.1 — Monitoring | Art. 12 — Logging and monitoring | Audit completeness, alerting |
| CC7.2 — Anomaly detection | Art. 17 — ICT incident detection | Brute-force, anomaly |
| CC7.3 — Forensic integrity | Art. 12.2 — Tamper evidence | Audit chain, Merkle proofs |
| CC8.1 — Change management | Art. 8 — ICT change management | Config, deployment |
| PI1.3 — Data integrity | Art. 6 — Confidentiality | SQL injection, data isolation |

---

## 2. Module Dependency Map

### 2.1 Full Module Inventory (103 modules, 8 layers)

```
LAYER 0 — Foundation (zero or minimal internal deps):
  config/settings.py, config/defaults.py, config/homedir.py
  data_classes.py, engine/engine_enum.py, engine/plan_stats.py
  utils/helper.py, utils/timer.py

LAYER 1 — Storage & Lock Primitives:
  storage/storage_interface.py, storage/local_storage.py, storage/s3_storage.py
  storage/minio_storage.py, storage/azure_storage.py, storage/gcp_storage.py
  storage/storage_factory.py, locking/redis_lock.py, locking/file_lock.py
  redis_connector.py

LAYER 2 — Catalog, RBAC, Parsing, Audit:
  redis_catalog.py (→ redis_lock)
  rbac/permissions.py, rbac/filter_builder.py, rbac/row_column_security.py
  rbac/role_manager.py (→ redis_catalog), rbac/user_manager.py (→ redis_catalog)
  rbac/access_control.py (→ role_manager, filter_builder, permissions)
  utils/sql_parser.py (→ data_classes, sqlglot)
  audit/ (events.py, chain.py, crypto.py, logger.py, writer_*.py, reader.py, retention.py, middleware.py)

LAYER 3 — Core Data Objects:
  super_table.py (→ redis_catalog, storage, rbac)
  simple_table.py (→ super_table, redis_catalog, rbac)
  staging_area.py (→ redis_catalog, storage, rbac)
  meta_reader.py (→ redis_catalog, rbac, super_table)
  processing.py (→ storage), monitoring_writer.py, query_plan_manager.py, plan_extender.py

LAYER 4 — Query Engine:
  engine/engine_common.py (→ settings, homedir) — view creation, S3 config, sanitization
  engine/data_estimator.py (→ redis_catalog, super_table)
  engine/duckdb_lite.py, duckdb_pro.py, spark_thrift.py (→ engine_common)
  engine/executor.py (→ duckdb_lite, duckdb_pro)

LAYER 5 — Read/Write Paths:
  data_reader.py (→ engine, rbac, sql_parser, redis_catalog)
  data_writer.py (→ processing, rbac, redis_catalog, simple_table)
  mirroring/ (mirror_delta.py, mirror_iceberg.py, mirror_parquet.py, mirror_formats.py)

LAYER 6 — Business Logic (services/):
  compute.py, execute.py, ingestion.py, monitoring.py
  security.py (RBAC CRUD + OData token mgmt)
  sharing.py, publication.py, supertable_manager.py
  gc.py, summary.py, time_travel.py, rate_limit.py, error_codes.py
  quality/ (config.py, checker.py, scheduler.py, anomaly.py, history.py)
  super_pipe.py

LAYER 7 — Server Applications:
  api/session.py (zero internal deps), server_common.py (→ session, config)
  api/api.py (→ services, server_common, audit), api/application.py
  webui/web_auth.py, webui/application.py (→ server_common, audit)
  odata/odata_handler.py, odata_server.py, odata.py, application.py
  mcp/mcp_server.py, web_app.py, web_client.py, web_server.py, mcp_client.py, mcp_stdio_proxy.py
  service_registry.py, logging.py
  webui/templates/ (17 HTML files)
```

### 2.2 Trust Boundaries

```
BOUNDARY 1: External → Server
  Browser ──────► WebUI (session cookie) ──────► API proxy (httpx)
  Agent   ──────► MCP  (auth_token param)
  BI Tool ──────► OData (bearer token st_od_*)
  Script  ──────► API  (session cookie / admin cookie)

BOUNDARY 2: Server → Catalog
  API/WebUI/MCP/OData ──────► Redis (catalog, locks, sessions, registry)

BOUNDARY 3: Server → Engine
  DataReader ──────► DuckDB (SQL, parquet_scan, httpfs)
  DataReader ──────► Spark Thrift (SQL)

BOUNDARY 4: Server → Storage
  DataWriter/Reader ──────► S3/MinIO/Azure/GCP/Local (parquet files)

BOUNDARY 5: Provider → Consumer (Sharing)
  Provider manifest ──────► Consumer Redis leaf (row_filter, schema, resources)
```

---

## 3. Assessment Areas

### AREA 1: Authentication & Session Management
**SOC 2:** CC6.1, CC6.2 | **DORA:** Art. 9, 9.2

#### 1.1 Session Cookie Architecture
**Files:** `api/session.py`, `server_common.py`, `webui/application.py`

- [ ] Session signing key entropy: `SUPERTABLE_SESSION_SECRET` fallback to `sha256("st_session:" + superuser_token)` — cryptographic weakness?
- [ ] Cookie attributes: `HttpOnly`, `Secure` (defaults False!), `SameSite=lax`, `Path=/`, `max_age=7d`
- [ ] Server-side expiry: `_decode_session()` checks `exp` — clock skew handling?
- [ ] Session replay: no nonce/jti/server-side session ID — cookie reuse possible after logout
- [ ] Session revocation: NO server-side session store — stolen cookie valid for 7 days
- [ ] Session fixation: does `login_post()` rotate session on success?

#### 1.2 Superuser Authentication
**Files:** `webui/application.py:239-300`, `api/session.py:189-193`, `config/settings.py`

- [ ] Raw `SUPERTABLE_SUPERUSER_TOKEN` stored in `st_admin_token` cookie (line 283)
- [ ] `is_superuser()` dual-path: session flag AND raw token cookie — double attack surface
- [ ] No minimum token length/entropy enforcement at startup
- [ ] Constant-time comparison: `hmac.compare_digest()` — verify ALL comparison points

#### 1.3 User Token Authentication
**Files:** `redis_catalog.py` (`validate_auth_token_full`, `create_auth_token`, etc.), `api/api.py:741-780`, `services/security.py`

- [ ] Token generation: `secrets.token_urlsafe(24)` — 192 bits (adequate, verify)
- [ ] Token storage: raw or hashed in Redis?
- [ ] Token-to-user binding: is token tied to specific username? Can it be reused?
- [ ] Token revocation: immediate invalidation or cached?
- [ ] OData token: `st_od_*` prefix — generation and verification flow

#### 1.4 Brute-Force & Rate Limiting
**Files:** `webui/application.py:239`, `services/rate_limit.py`, `api/application.py:84-85`, `api/api.py:303-334`

- [ ] WebUI login: NO rate limiting (RateLimitMiddleware only on API server)
- [ ] No progressive delay or account lockout on failed attempts
- [ ] No per-IP tracking of failed authentication
- [ ] MCP: concurrency semaphore but no per-IP rate limiting
- [ ] OData: no rate limiting
- [ ] Timing oracle: do valid vs invalid usernames produce different response times?

#### 1.5 MCP Authentication
**Files:** `mcp/mcp_server.py:381-540`, `mcp/web_app.py:339`

- [ ] `require_token=False` grants full superadmin with zero auth — startup warning?
- [ ] Shared token: `hmac.compare_digest()` used (line 411) — verify
- [ ] Per-user token: `validate_auth_token_full()` → `_resolve_role_user_token()` — full chain
- [ ] Role escalation: can user request higher role than assigned?
- [ ] `allowed_roles` config enforcement
- [ ] SSE/WebSocket transport auth on connection upgrade
- [ ] stdio transport: `auth_token` required in every tool call?

---

### AREA 2: Authorization & RBAC
**SOC 2:** CC6.3 | **DORA:** Art. 9.3

#### 2.1 Endpoint Auth Guard Completeness
**Files:** `api/api.py` (~80+ endpoints), `webui/application.py` (~15 routes), `odata/odata_server.py` (4 routes), `mcp/mcp_server.py` (~20 tools)

- [ ] Enumerate EVERY `@router.*` in api/api.py — verify Depends(admin_guard_api) or Depends(logged_in_guard_api)
- [ ] KNOWN GAPS: `healthz_deep()` (line 507-509) — NO auth, leaks Redis topology
- [ ] Shares/publications manifest endpoints — bearer-only auth, no session
- [ ] WebUI: every `@app.get` page calls `_page_context()` with None check
- [ ] WebUI: superuser pages check `ctx.get("session_is_superuser")`
- [ ] OData: all routes use `Depends(_odata_auth)` + `_enforce_scope()`
- [ ] MCP: every `@mcp.tool()` calls `_auth(auth_token, role)`

#### 2.2 RBAC Enforcement on Data Paths
**Files:** `rbac/access_control.py`, `rbac/role_manager.py`, `rbac/permissions.py`, `rbac/filter_builder.py`, `rbac/row_column_security.py`

- [ ] Read: `DataReader.execute()` → `restrict_read_access()` — unconditional?
- [ ] Read: Column filtering (disallowed columns excluded from SELECT)
- [ ] Read: Row filtering (WHERE via DuckDB views)
- [ ] Read: CTE bypass — `get_physical_tables()` excludes CTEs — verify
- [ ] Write: `DataWriter.write()` → `check_write_access()` — before mutation?
- [ ] Write: `DataWriter.delete()` → `check_write_access()`
- [ ] Meta: `MetaReader.get_tables()` → `check_meta_access()` — filters by role?
- [ ] System tables (`__app_state__`, `__feedback__`, `__annotations__`) — RBAC on these?

#### 2.3 Privilege Escalation
- [ ] Horizontal: manipulate `org`/`sup` params to access other tenant data
- [ ] `resolve_pair()` fallback: empty org/sup returns first discovered pair — exploitable?
- [ ] Role hierarchy enforcement in `has_permission()`
- [ ] Role mutation: can `writer` create `superadmin` role?
- [ ] User self-mutation: can user change own role assignment?
- [ ] Redis-level: if attacker has Redis access, can they promote themselves?

---

### AREA 3: Injection Attacks
**SOC 2:** PI1.3, CC6.1 | **DORA:** Art. 6

#### 3.1 SQL Injection — All 5 Paths to DuckDB

**Path 1: API → DataReader → DuckDB**
- [ ] User SQL → `SQLParser(sqlglot)` → `DataEstimator` → `Executor` → `DuckDB con.execute()`
- [ ] `engine_common.py:create_reflection_table/view()` — f-string SQL with parquet paths, table names, columns
- [ ] `engine_common.py:create_rbac_view()` (line 662-666) — f-string with `where_clause`
- [ ] `engine_common.py:create_dedup_view()` — f-string with `primary_keys`
- [ ] `engine_common.py:create_tombstone_filter_view()` (line 770-793) — f-string with `values_sql` from Redis
- [ ] `engine_common.py:configure_s3_for_connection()` — SET with `sanitize_sql_string()`

**Path 2: MCP → DataReader → DuckDB**
- [ ] Agent SQL → `_read_only_sql()` regex check → `_exec_query_sync()` → DataReader
- [ ] `_read_only_sql()` bypasses: `SELECT...INTO`, `COPY...TO`, DuckDB `read_csv_auto()`, `INSTALL`, `LOAD`
- [ ] `get_app_state()` (line 1670-1672) — f-string SQL with `_sql_escape_literal()`

**Path 3: OData → DataReader → DuckDB**
- [ ] `_safe_filter()` denylist — bypasses: UNION SELECT, subqueries, DuckDB I/O functions
- [ ] `_quote_identifier()` / `_validate_identifier()` — allowlist approach (stronger)
- [ ] `$top` max 100000 — resource exhaustion?

**Path 4: Linked Share → DataReader → DuckDB (CRITICAL)**
- [ ] Provider manifest `row_filter` → Redis leaf `_row_filter` → `rbac_view_def.where_clause` → DuckDB
- [ ] NO validation at ANY point in this chain — cross-tenant SQL injection

**Path 5: RBAC Filter Config → DataReader → DuckDB**
- [ ] Admin filter JSON → `filter_builder._sanitize_column/value/operation()` → WHERE clause
- [ ] `_sanitize_value()` blocks `;--/**/` but not subqueries or function calls

#### 3.2 Command Injection
- [ ] `locking/benchmarks/benchmark_locking.py` — `subprocess.run()` (dev-only?)
- [ ] `mirroring/mirror_delta.py:560` — `__import__("uuid")` (unusual but safe)
- [ ] `staging_area.py:202` — `redis.eval(lua, ...)` — Lua injection via user input?
- [ ] Exhaustive scan: no other `eval/exec/os.system/subprocess` in production code

#### 3.3 Path Traversal
- [ ] User-supplied table/org/sup names with `../` in storage paths
- [ ] `super_table.py:38` — `os.path.join(org, sup, identity)` — validated?
- [ ] MCP `_safe_id()` blocks `/`, `\\`, `..` — applied everywhere?
- [ ] Linked share resource paths from manifest — validated?
- [ ] Iceberg mirror: `os.path.join()` with potentially user-controlled names

#### 3.4 Template Injection / XSS
- [ ] Search all 17 templates for `|safe`, `{% autoescape false %}`
- [ ] Jinja2 autoescaping: globally enabled?
- [ ] User-controlled data in template context (username, role_name, error messages)
- [ ] JavaScript blocks embedding template variables
- [ ] CSP `'unsafe-inline'` in `script-src` — XSS impact if reflected XSS exists

#### 3.5 Redis Key Injection
- [ ] All `_*_key()` functions: validate org/sup/simple_name?
- [ ] Table named `*` or `foo:bar:baz` — Redis key collisions?
- [ ] Org named `supertable` — namespace collision with internal keys?

---

### AREA 4: API Security
**SOC 2:** CC6.6 | **DORA:** Art. 10

- [ ] Input validation: all query params typed with defaults? Body payloads validated with Pydantic?
- [ ] Request body size limits: max content length?
- [ ] Error disclosure: 25+ `HTTPException(500, f"...{e}")` patterns leaking internals
- [ ] CORS: NO `CORSMiddleware` on any of 4 servers
- [ ] Security headers: `_security_headers()` sets CSP, X-Frame-Options, etc. — on all servers?
- [ ] HSTS: `Strict-Transport-Security` NOT set
- [ ] CSRF: `POST /ui/login` — NO CSRF token
- [ ] HTTP method enforcement: FastAPI prevents GET→POST (verify)

---

### AREA 5: OData Endpoint Security
**SOC 2:** CC6.1, CC6.3, PI1.3 | **DORA:** Art. 9, Art. 6

**Files:** `odata/odata_server.py`, `odata/odata_handler.py`, `odata/odata.py`, `odata/application.py`, `services/security.py`

- [ ] Bearer token generation (`st_od_*`) — `secrets.token_urlsafe()`?
- [ ] Token verification — constant-time comparison?
- [ ] Token scoping — tied to org/sup/role in Redis
- [ ] `$filter` denylist bypass (see Area 3.1)
- [ ] `$metadata` — reveals table names/columns regardless of RBAC?
- [ ] OData error responses — SQL errors, internal table names leaked?
- [ ] OData `/healthz` — exception details in error response

---

### AREA 6: MCP Server Security
**SOC 2:** CC6.1, CC6.3, PI1.3 | **DORA:** Art. 9, Art. 6, Art. 28

**Files:** `mcp/mcp_server.py` (2652 lines), `mcp/web_app.py`

#### 6.1 Tool-Level Security Matrix

| Tool | Type | Auth | RBAC | SQL Risk |
|---|---|---|---|---|
| `query_sql` | Read | ✓ | DataReader | **Raw SQL — Critical** |
| `get_app_state` | Read | ✓ | DataReader | **f-string SQL — High** |
| `insert_rows` | Write | ✓ | DataWriter | None |
| `upsert_rows` | Write | ✓ | DataWriter | None |
| `delete_rows` | Write | ✓ | DataWriter | None |
| `store_app_state` | Write | ✓ | DataWriter | None |
| `list_tables` | Read | ✓ | MetaReader | None |
| `validate_sql` | Read | ✓ | — | sqlglot only |

- [ ] Every write tool: `check_write_access()` called (directly or via DataWriter)
- [ ] `_read_only_sql()` bypass: `INSTALL`, `LOAD`, `COPY TO`, `SELECT INTO`, DuckDB file I/O functions
- [ ] Tool parameter validation: `_safe_id()`, `_safe_column_id()`, `_validate_role()` coverage

#### 6.2 Agent Prompt Injection
- [ ] Stored data (annotations, feedback) manipulating agent behavior
- [ ] `list_tables` returns `system_hint`, `catalog`, `annotations` — manipulable by non-admin?
- [ ] `store_annotation` — can inject instructions overriding RBAC?

#### 6.3 MCP Transport
- [ ] `TrustedHostMiddleware` — what hosts allowed? `*` possible?
- [ ] SSE connection authenticated?
- [ ] Concurrent query limit: `_get_limiter()` — per-client or global?
- [ ] Query timeout: default, max, client-settable?

---

### AREA 7: Data Layer
**SOC 2:** PI1.3, CC6.1 | **DORA:** Art. 6

#### 7.1 Read Path (`data_reader.py`)
- [ ] Query string stored as-is in `DataReader.__init__()`
- [ ] RBAC view injection: `_row_filter` → `where_clause` → `create_rbac_view()`
- [ ] Tombstone injection: `deleted_keys` from Redis → `create_tombstone_filter_view()`
- [ ] Dedup view: `primary_keys` from Redis → `create_dedup_view()` — PKs validated?
- [ ] `_ensure_sql_limit()` — regex LIMIT check — bypass via subquery?

#### 7.2 Write Path (`data_writer.py`)
- [ ] Schema validation: extra columns injected? Type overflow?
- [ ] System table protection: `__app_state__`, `__feedback__`, `__annotations__` — who can write?
- [ ] Snapshot pointer: can leaf `path` point to another table's files?
- [ ] Concurrent writes: Redis lock TTL vs write duration?
- [ ] Post-lock: monitoring/quality after lock release?

#### 7.3 Processing (`processing.py`)
- [ ] Column name injection in Parquet metadata
- [ ] `generate_filename()` — safe characters?
- [ ] Storage abstraction bypass: `open()` in `plan_extender.py:37`, `os.path.join` in `super_table.py:38`, extensive `os.path.join` in `mirroring/mirror_iceberg.py`

---

### AREA 8: Secrets & Configuration
**SOC 2:** CC6.2, CC6.7 | **DORA:** Art. 9.2

**Files:** `config/settings.py`, `engine/engine_common.py`, `server_common.py`

- [ ] `SUPERTABLE_SUPERUSER_TOKEN` default: empty string — startup validation?
- [ ] `SUPERTABLE_SESSION_SECRET` default: empty — falls back to weak derivation
- [ ] `SUPERTABLE_MCP_TOKEN` default: empty — behavior when `require_token=true`?
- [ ] `SUPERTABLE_REDIS_PASSWORD` default: empty — unauthenticated Redis allowed
- [ ] `SUPERTABLE_AUDIT_FERNET_KEY` default: empty — SQL stored in plaintext
- [ ] S3 keys in DuckDB SET statements (line 251-255) — logged at debug level?
- [ ] Search all modules for secrets logged in `logger.debug/info/warning/error` calls
- [ ] `.env` file: `python-dotenv` loads from predictable path — manipulable?

---

### AREA 9: Storage & File Access
**SOC 2:** CC6.1, PI1.3 | **DORA:** Art. 6

**Files:** `storage/*.py`, all non-storage modules with `open()/os.path/shutil` calls

- [ ] Storage abstraction bypass: `plan_extender.py:37`, `super_table.py:38`, `mirroring/mirror_iceberg.py` (20+ os.path.join)
- [ ] `local_storage.py`: base directory enforcement? `../` escape?
- [ ] S3: pre-signed URLs — TTL, logging?
- [ ] DuckDB `httpfs`: `parquet_scan('https://evil.com/...')` — access arbitrary URLs?
- [ ] Temp files: secure directory, cleanup?
- [ ] File permissions on created parquet files

---

### AREA 10: Network & Infrastructure
**SOC 2:** CC6.6 | **DORA:** Art. 10

- [ ] WebUI→API proxy: over TLS? `X-Forwarded-For` added?
- [ ] Redis: TLS support (`rediss://`)? Password required?
- [ ] Service registry: rogue service registration possible with Redis access
- [ ] Health endpoints: which are authenticated? Info disclosed?
- [ ] `discover_pairs()` SCAN — crafted key names pollute discovery?

---

### AREA 11: Audit & Compliance
**SOC 2:** CC7.1, CC7.2, CC7.3 | **DORA:** Art. 12, 12.2, 17

**Files:** `audit/__init__.py`, `audit/events.py`, `audit/chain.py`, `audit/crypto.py`, `audit/logger.py`, `audit/writer_*.py`, `audit/reader.py`, `audit/retention.py`, `audit/middleware.py`, `api/api.py`

#### 11.1 Audit Completeness
- [ ] Map every mutation endpoint to its `_audit()` call — identify gaps
- [ ] Table/SuperTable CRUD, data write/delete, RBAC CRUD, token CRUD
- [ ] Share/publication lifecycle, OData endpoint CRUD
- [ ] Login/logout, config changes, query execution
- [ ] Severity: mutations at WARNING+? Critical ops at CRITICAL?
- [ ] Actor identification: `actor_id`, `actor_username`, `actor_ip` on every event?
- [ ] Audit calls in try/except: never fail business operation

#### 11.2 Audit Integrity (SOC 2 CC7.3 / DORA Art. 12.2)
- [ ] Hash chain: `compute_chain_hash(prev, batch)` on every batch
- [ ] Chain continuity: can event be deleted without breaking chain?
- [ ] Merkle proof: daily generation and independent storage?
- [ ] File hash: `compute_file_hash()` verified on read?
- [ ] Redis audit records: deletable via direct Redis commands?
- [ ] Parquet audit files: modifiable on disk/S3 without detection?

#### 11.3 Audit Encryption (DORA Art. 6)
- [ ] Fernet encryption: only when `SUPERTABLE_AUDIT_FERNET_KEY` set
- [ ] What fields encrypted? Only SQL? PII in event details?
- [ ] Key rotation process documented?
- [ ] Plaintext fallback: full SQL in logs when encryption disabled

#### 11.4 Audit Retention (DORA Art. 12)
- [ ] Default retention period?
- [ ] Minimum retention enforced? (DORA: 5 years for ICT incidents)
- [ ] Can admin delete events early?

---

### AREA 12: Dependency & Supply Chain
**SOC 2:** CC6.8 | **DORA:** Art. 28

**Files:** `requirements.txt`, `requirements-docker.txt`, `pyproject.toml`

#### 12.1 Dependency Audit (28 direct packages)

| Package | Range | Critical? |
|---|---|---|
| `duckdb>=1.1,<2.0` | Wide | **SQL engine** |
| `sqlglot>=26,<27` | Tight | **SQL parser** |
| `redis>=5.0,<6.0` | Moderate | **Catalog** |
| `fastapi>=0.111,<0.117` | Moderate | HTTP framework |
| `jinja2>=3.1,<4.0` | Moderate | **XSS surface** |
| `mcp>=1.1.0` | **No upper bound!** | **MCP protocol** |
| `numpy>=1.26,<3.0` | Very wide | Data |
| `pyarrow>=16,<19` | Wide | Parquet I/O |

- [ ] `pip audit` / `safety check` on all dependencies
- [ ] `mcp>=1.1.0` — no upper bound — breaking/malicious release auto-installed
- [ ] Lock file existence: `requirements.lock`?
- [ ] Transitive dependency tree CVE scan

#### 12.2 DuckDB Extension Security
- [ ] `INSTALL`/`LOAD` blocked by `_read_only_sql()`? (regex may miss them)
- [ ] `httpfs` auto-loaded? Arbitrary URL access?
- [ ] Extension cache directory writable by application?

---

### AREA 13: Multi-Tenancy & Data Isolation
**SOC 2:** CC6.3, PI1.3 | **DORA:** Art. 6

- [ ] Organization isolation: logical (same Redis, same storage) — namespace-only separation
- [ ] Redis SCAN: can org A read org B's keys?
- [ ] Storage paths: can org A access org B's files via path manipulation?
- [ ] DuckDB: isolated connections per query? Persistent connections (duckdb_pro) carry state?
- [ ] Linked shares: provider files accessible to consumer DuckDB?
- [ ] Service registry: cross-org visibility?

---

### AREA 14: DORA Operational Resilience

#### 14.1 ICT Risk Management (Art. 6)
- [ ] Data classification scheme documented?
- [ ] Encryption requirements for data at rest/transit?
- [ ] Platform threat model documented?

#### 14.2 ICT Incident Detection (Art. 17)
- [ ] Alerts for: brute-force, privilege escalation, data exfiltration?
- [ ] Anomalous query pattern detection?
- [ ] Failed auth aggregation and alerting?
- [ ] Account quarantine mechanism?

#### 14.3 ICT Third-Party Risk (Art. 28)
- [ ] Dependency hashes for integrity verification?
- [ ] New dependency evaluation process?
- [ ] DuckDB extensions treated as third-party deps?

#### 14.4 Business Continuity (Art. 11)
- [ ] Redis Sentinel failover functional?
- [ ] Storage multi-backend failover?
- [ ] Lock recovery after crash (TTL-based)?
- [ ] Graceful degradation on component failure?

---

## 4. Methodology

### 4.1 Per-Module Static Analysis
For each of the 103 modules:
1. Read line 1 — extract `# route:` comment
2. Map all internal/external imports
3. Identify trust boundary crossings
4. Trace user-controlled input to execution points
5. Check compliance with architecture rules (auth, RBAC, audit, locking, monitoring)

### 4.2 Data Flow Tracing
For each trust boundary:
1. Input validation at boundary
2. Sanitization before use
3. Encoding for destination context (SQL, HTML, Redis, filesystem)
4. Error handling — internal details leaked?

### 4.3 Configuration Review
For each of 60+ settings in `config/settings.py`:
1. Default value — secure? Fail open or closed?
2. Startup validation?
3. Security implications documented?

### 4.4 Compliance Mapping
For each finding:
1. SOC 2 Trust Services Criteria
2. DORA Article
3. Severity: Critical / High / Medium / Low / Informational
4. Location (file:line), description, impact, code-level recommendation

---

## 5. Deliverables

1. **Executive Summary** — 1-page risk overview
2. **Findings Register** — ID, severity, SOC2/DORA mapping, location, description, recommendation, effort
3. **Module Security Map** — per-module pass/fail/finding count
4. **Data Flow Diagrams** — critical paths (user SQL → DuckDB, agent SQL → DuckDB, share → consumer)
5. **Dependency CVE Report** — `pip audit` results
6. **SOC 2 / DORA Compliance Gap Matrix** — control coverage
7. **Remediation Roadmap** — prioritized with effort estimates
8. **Zip archive** — all deliverables preserving folder structure

---

## 6. Execution Plan

| Phase | Duration | Focus |
|---|---|---|
| Phase 1: Foundation | Day 1-2 | Clone, dependency graph, enumerate endpoints/tools, CVE scan |
| Phase 2: Auth & RBAC | Day 3-5 | Areas 1-2: sessions, tokens, guards, RBAC, escalation |
| Phase 3: Injection | Day 6-9 | Area 3: all 5 SQL paths, OData bypass, path traversal, XSS |
| Phase 4: API & Transport | Day 10-11 | Areas 4-6: validation, errors, CORS, MCP, agent injection |
| Phase 5: Data & Storage | Day 12-13 | Areas 7-9: read/write, secrets, storage bypass, tenancy |
| Phase 6: Compliance | Day 14-15 | Areas 10-14: audit, chain integrity, DORA, dependency scan |
| Phase 7: Report | Day 16-17 | Consolidate, SOC2/DORA gap matrix, remediation roadmap |
