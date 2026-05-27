# 16  Redis Key Layout

This page is the canonical reference for **every** Redis key produced by
SuperTable.  It is the single source of truth that both the library
(`supertable/`) and the services (`dataisland-core/services/`) follow.

## 16.1  Rule

> **Every Redis key constructed via `supertable/redis_keys.py` MUST
> start with one of two recognised top-level prefixes:**
>
> * `supertable:` — SuperTable SDK state (catalog, RBAC, locks, audit,
>   share federation, table meta, …).
> * `dataisland:` — platform / dataisland-core state that is not
>   SuperTable's concern (service-registry heartbeats, app-bootstrap
>   configs).
>
> Per-app config keys (`lighthouse:*`, `gatekeeper:*`, `studio:*`, …)
> live under their own app-name prefix and are written via the MCP
> server's `store_app_config` tool — they do **not** go through
> `redis_keys.py`.
>
> The only module allowed to construct keys under `supertable:` or
> `dataisland:` is `supertable/redis_keys.py`. No other source file
> (in either repo) may contain a literal `f"supertable:..."`,
> `f"dataisland:..."`, `f"monitor:..."`, `f"spark:..."`, `f"audit:..."`,
> or `f"registry:..."` key. The regression test
> `tests/test_redis_key_prefix.py` enforces this on CI.

## 16.2  Hierarchy

```
dataisland:                                  ── platform / dataisland-core
  apps:{app_name}:master_mcp                 STRING  per-app bootstrap (master MCP info)
  {org}:
    registry:{service_type}:{host}:{pid}     STRING  service-instance heartbeat (TTL 30s)

supertable:                                  ── SuperTable SDK state
  {org}:                                    ── organization scope
    _system_:                               ── reserved system scope (SDK side)
      auth:tokens                            HASH    hashed login tokens
    shares:doc:{share_id}                    STRING  share definitions (provider)
    shares:index                             SET     all share IDs
    audit:stream                             STREAM  audit events (org-wide)
    audit:chain_head:{instance_id}           HASH    per-instance chain state
    audit:config                             HASH    runtime audit toggle
    spark:thrifts                            HASH    Spark Thrift clusters
    spark:plugs                              HASH    Spark Plug runtimes
    {sup}:                                  ── supertable scope
      meta:root                              STRING  root pointer + version
      meta:leaf:{simple}                     STRING  leaf snapshot pointer
      meta:mirrors                           STRING  enabled mirror formats
      meta:table_config:{simple}             STRING  per-table config
      meta:staging:{name}                    STRING  staging metadata
      meta:staging:meta                      SET     staging name index
      meta:staging:{name}:pipe:{pipe}        STRING  pipe definition
      meta:staging:{name}:pipe:meta          SET     pipe name index
      config:engine                          STRING  engine runtime config
      lock:leaf:{simple}                     STRING  per-table lock token
      lock:stage:{stage}                     STRING  per-staging lock token
      rbac:users:meta                        HASH    version, last_updated_ms
      rbac:users:index                       SET     all user_ids
      rbac:users:doc:{user_id}               HASH    user document
      rbac:users:name_to_id                  HASH    username → user_id
      rbac:roles:meta                        HASH    version, last_updated_ms
      rbac:roles:index                       SET     all role_ids
      rbac:roles:doc:{role_id}               HASH    role document
      rbac:roles:type:{role_type}            SET     role IDs by type
      rbac:roles:name_to_id                  HASH    role_name → role_id
      schema:{simple}                        STRING  table schema JSON
      table_names                            SET     all simple table names
      linked_shares:doc:{link_id}            STRING  consumer-side linked share
      linked_shares:index                    SET     linked share IDs
      monitor:{monitor_type}                 LIST    monitoring metrics
```

## 16.3  Scope discipline

| Scope                  | Pattern                                              | Reason |
|------------------------|------------------------------------------------------|--------|
| **Platform (global)**  | `dataisland:apps:{app_name}:master_mcp`              | Per-app master-MCP bootstrap entries — read by every app on boot before any org context exists. |
| **Platform (per org)** | `dataisland:{org}:registry:{service_type}:*`         | Service-instance heartbeats (TTL 30s). Written by `api`, `webui`, `odata`, `mcp`, `sdk`, plus SDK-driven apps like `lighthouse`. |
| **System (per org, SDK side)** | `supertable:{org}:_system_:auth:tokens`     | Organization-level login tokens (stays under `_system_` so it can never collide with a user-created supertable named `auth`). |
| **Organization**       | `supertable:{org}:shares:*`<br>`supertable:{org}:audit:*`<br>`supertable:{org}:spark:*` | Tenant-wide concerns: share federation, compliance audit feed, Spark cluster registry. |
| **SuperTable**         | `supertable:{org}:{sup}:meta:*`<br>`supertable:{org}:{sup}:rbac:*`<br>`supertable:{org}:{sup}:lock:*`<br>`supertable:{org}:{sup}:config:*`<br>`supertable:{org}:{sup}:monitor:*`<br>`supertable:{org}:{sup}:linked_shares:*` | Per-supertable metadata, security, locking, runtime config, telemetry. |
| **Entity**             | `supertable:{org}:{sup}:meta:leaf:{simple}`<br>`supertable:{org}:{sup}:rbac:users:doc:{user_id}` | One key per leaf table / user / role. |
| **Per-app config**     | `lighthouse:{org}:config:*`<br>`gatekeeper:{org}:config:*`<br>… | Each SDK app's own state (runtime config, encrypted registries, user prefs). Written by the MCP `store_app_config` tool — never via `redis_keys.py`. |

### 16.3.1  Reserved supertable names

`_system_` is **reserved**. `SuperTable(..., super_name="_system_")`
raises `ValueError`. The reservation guarantees that operational keys
(service registry, organization auth tokens, any future system-only
data) cannot ever clash with a user-supplied supertable name.

The full reserved set is exported as
`supertable.redis_keys.RESERVED_SUPER_NAMES`
(`frozenset({"_system_"})`) and tested via
`is_reserved_super_name()`.

### 16.3.2  Scanning the registry

Two helpers — pick depending on whether you want a single-tenant or
cross-tenant view:

```python
from supertable import redis_keys as RK

# One tenant — used by the per-org Monitoring tab:
pattern_one = RK.registry_pattern_for_org("kladna-soft")
# → "dataisland:kladna-soft:registry:*"

# Every tenant — used by superuser-only fleet views:
pattern_all = RK.registry_pattern()
# → "dataisland:*:registry:*"
```

`ServiceRegistry.scan(redis_client, organization=None)` accepts an
optional `organization` and dispatches to the right pattern.

## 16.4  Why these scopes

- **Audit at org level (not supertable level).**  External SIEM consumers
  expect a single XREAD per tenant.  Per-supertable streams would explode
  consumer-group fan-out and make compliance reporting harder.
- **Registry under `dataisland:` (was `supertable:_system_:registry`).**
  The service registry is a platform / dataisland-core concern, not a
  SuperTable SDK concern. Promoting it to its own top-level prefix
  cleanly separates "SDK state" from "platform state" and removes the
  `_system_` segment (which only existed to avoid colliding with user
  supertable names — and there are no user supertables under
  `dataisland:`).
- **Auth tokens under `_system_` (still on the SuperTable side).** A
  supertable literally named `auth` would have produced colliding
  keys (`supertable:{org}:auth:*`). Login tokens are RBAC plumbing —
  squarely SuperTable's responsibility — so they stay under
  `supertable:{org}:_system_:auth:tokens`. The `_system_` segment
  keeps the safety guarantee.
- **App bootstrap as a single global key.** `dataisland:apps:{app}:master_mcp`
  is intentionally not org-scoped: it's the first thing an app reads
  on boot, before any org context exists. Once the app has the
  response, every subsequent operation is org-scoped.
- **Monitor at supertable level (was at root).**  Avoid collision with
  any other product using `monitor:*`; align with the rest of the schema.
- **Spark at org level (was at root).**  Cluster configuration is per
  tenant; the `spark:` root prefix collided with other Spark tooling.

## 16.5  Audit toggle (runtime)

Audit is **OFF by default** (`SUPERTABLE_AUDIT_ENABLED=false`).  A
per-organization runtime override is persisted in
`supertable:{org}:audit:config` (HASH) with fields:

```
enabled       "true" | "false"
log_queries   "true" | "false"
log_reads     "true" | "false"
hash_chain    "true" | "false"
siem_enabled  "true" | "false"
updated_ms    str(int)
updated_by    str
```

Read/write via `supertable.audit.admin.{get,set}_audit_config`, which is
exposed over HTTP at `GET / POST /api/v1/audit/config` and bound to the
**Compliance** tab on `/ui/audit`.  Toggles propagate within the
in-process cache TTL (30 s); the API endpoint invalidates the cache for
the affected org so the change is immediate on the responding instance.

Every config write itself emits a `CONFIG_CHANGE` audit event so that
turning auditing OFF is recorded — DORA Art. 6 / SOC 2 CC8.1.

## 16.6  Adding a new key

1. Add a function to `supertable/redis_keys.py` that returns the key
   string.  Make sure it starts with `supertable:`.
2. Use the function from every call site (no inline f-strings).
3. Re-run `pytest supertable/tests/test_redis_key_prefix.py` — it must
   pass with the new helper enumerated.
4. Update §16.2 of this page.
