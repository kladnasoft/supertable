# Data Island Core — Security Assessment Plan

## Scope

Every attack surface in the `supertable/` codebase: authentication, authorization, input validation, injection vectors, secrets handling, storage access, network boundaries, and supply chain.

## Assessment Areas

### 1. Authentication & Session Management
- Login flow: brute-force protection, credential storage (hashing algorithm, salt), session token generation (entropy, predictability)
- Session lifecycle: expiration, revocation, fixation attacks, cookie flags (HttpOnly, Secure, SameSite)
- Token-based auth (API tokens): generation, storage, rotation, revocation
- Superuser escalation paths — can a regular user reach superuser-only routes?

### 2. Authorization & RBAC
- Every endpoint audited for missing auth guards (`Depends(admin_guard_api)`, `_page_context()`)
- RBAC enforcement: are `check_write_access()` / `check_meta_access()` called before every mutation?
- Role hierarchy bypass: can a read-only role write? Can a table-scoped role access other tables?
- Horizontal privilege escalation: can user A access user B's org/supertable by manipulating `org`/`sup` query params?
- Default role permissions — what does a freshly created role allow?

### 3. Injection Attacks
- **SQL injection**: every path where user input reaches DuckDB — `read_data.py`, `write_data.py`, query engine, OData parser, MCP tool queries. F-string/format-string SQL construction, parameterization gaps.
- **Command injection**: any `os.system()`, `subprocess`, `eval()`, `exec()` usage.
- **Path traversal**: file paths constructed from user input in storage layer, upload handlers, export endpoints.
- **Template injection**: Jinja2 templates with `|safe` or unescaped user content, XSS vectors in HTML responses.
- **Redis injection**: key construction from user input — can someone inject Redis commands or access other tenants' keys?

### 4. API Security
- Input validation: are all query params, body fields, and headers validated with types and bounds?
- Rate limiting: does it exist? Per-endpoint or global?
- Error disclosure: do 500 responses leak stack traces, internal paths, or config values?
- CORS configuration: overly permissive origins?
- HTTP method enforcement: can POST endpoints be called with GET?
- Request size limits: unbounded file uploads or request bodies?

### 5. OData Endpoint
- OData filter/orderby/select parsing — injection through OData operators
- Pagination abuse: can someone request `$top=999999999`?
- Schema leakage: does the OData metadata endpoint reveal internal structure?

### 6. MCP Server Security
- Tool input validation: are all MCP tool parameters schema-validated server-side?
- Agent-generated SQL: does it pass through `sql_parser`/`query_plan_manager` or can raw SQL reach DuckDB?
- Resource exposure: can an MCP client enumerate all orgs/supertables regardless of RBAC?
- Prompt injection via tool results: can stored data manipulate the agent's behavior?

### 7. Data Layer (`read_data.py`, `write_data.py`)
- SQL construction: parameterized or string-interpolated?
- Schema validation on writes: can someone inject extra columns, overflow types, or write to system tables?
- File format attacks: malicious Parquet/CSV files exploiting DuckDB parsing bugs
- Snapshot pointer manipulation: can someone point a table at another table's data files?

### 8. Secrets & Configuration
- Secrets in source code, env defaults, or logs
- `settings.py` defaults: do any ship with real credentials?
- Logging: are tokens, passwords, API keys, or PII ever logged?
- Redis connection: authenticated? TLS?

### 9. Storage & File Access
- Storage abstraction bypass: any direct `open()`, `os.path`, `shutil` calls outside `StorageInterface`?
- Symlink following, directory listing exposure
- Object storage (S3/MinIO) bucket policies, pre-signed URL leakage

### 10. Network & Infrastructure
- Inter-service communication: is API↔WebUI proxy authenticated?
- TLS termination: where and how?
- Health/readiness endpoints: do they expose sensitive info?
- Discovery mechanism: can a rogue service register itself?

### 11. Audit & Compliance
- Audit log completeness: are there mutations without audit events?
- Audit log integrity: can someone tamper with or delete audit records?
- Audit log injection: can user-controlled strings break log parsing?

### 12. Dependency & Supply Chain
- Known CVEs in pinned dependencies
- Unpinned or loose version ranges
- DuckDB extension loading: can someone load a malicious extension?

---

## Methodology

For each area I will: read the relevant source files, trace data flow from user input to execution, identify violations of the architecture rules in the system prompt, classify findings by severity (Critical / High / Medium / Low / Informational), and provide concrete fix recommendations with code.

## Deliverable

A structured report (markdown + zip) with: executive summary, findings table (ID, severity, location, description, recommendation), and detailed analysis per area.
