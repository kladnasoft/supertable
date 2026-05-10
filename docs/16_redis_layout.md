# 16  Redis Key Layout

This page is the canonical reference for **every** Redis key produced by
SuperTable.  It is the single source of truth that both the library
(`supertable/`) and the services (`dataisland-core/services/`) follow.

## 16.1  Rule

> **Every Redis key MUST start with `supertable:`.**
>
> The only module allowed to construct a Redis key string is
> `supertable/redis_keys.py`.  No other source file (in either repo) may
> contain a literal `f"supertable:..."`, `f"monitor:..."`, `f"spark:..."`,
> `f"audit:..."`, or `f"registry:..."` key.  The regression test
> `tests/test_redis_key_prefix.py` enforces this on CI.

## 16.2  Hierarchy

```
supertable:
  registry:{service_type}:{host}:{pid}      ── global service registry
  {org}:                                    ── organization scope
    auth:tokens                              HASH  hashed login tokens
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

| Scope            | Pattern                                        | Reason |
|------------------|------------------------------------------------|--------|
| **Global**       | `supertable:registry:*`                        | Process-level, multi-tenant; tenant lives in payload. |
| **Organization** | `supertable:{org}:auth:*`<br>`supertable:{org}:shares:*`<br>`supertable:{org}:audit:*`<br>`supertable:{org}:spark:*` | Tenant-wide concerns: auth, share federation, compliance audit feed, Spark cluster registry. |
| **SuperTable**   | `supertable:{org}:{sup}:meta:*`<br>`supertable:{org}:{sup}:rbac:*`<br>`supertable:{org}:{sup}:lock:*`<br>`supertable:{org}:{sup}:config:*`<br>`supertable:{org}:{sup}:monitor:*`<br>`supertable:{org}:{sup}:linked_shares:*` | Per-supertable metadata, security, locking, runtime config, telemetry. |
| **Entity**       | `supertable:{org}:{sup}:meta:leaf:{simple}`<br>`supertable:{org}:{sup}:rbac:users:doc:{user_id}` | One key per leaf table / user / role. |

## 16.4  Why these scopes

- **Audit at org level (not supertable level).**  External SIEM consumers
  expect a single XREAD per tenant.  Per-supertable streams would explode
  consumer-group fan-out and make compliance reporting harder.
- **Registry at root level (not org level).**  A single API/MCP/WebUI
  process serves every tenant; per-org duplication would be wrong.
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
