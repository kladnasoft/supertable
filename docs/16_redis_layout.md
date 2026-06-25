# 16  Redis Key Layout (v2)

This page is the canonical reference for **every** Redis key produced by
SuperTable + the platform layer (dataisland-core). v2 is the current
on-disk shape (SDK ≥ 2.1.0). There is no v1 ↔ v2 migration; v1 data is
inaccessible from v2 code.

## 16.1  Rule

> **Every Redis key in the platform is constructed inside
> `supertable/redis_keys.py`.**
>
> Two recognised root prefixes:
>
> * `supertable:` — SuperTable SDK state (catalog, RBAC, locks, audit,
>   share federation, table meta, lakes / supertables).
> * `dataisland:` — platform / dataisland-core state that is not the
>   SDK's concern (service registry, app bootstrap).
>
> Per-app config keys (`lighthouse:*`, `gatekeeper:*`, `studio:*`, …)
> live under their own app-name prefix and are written via the MCP
> server's `store_app_config` tool — they do **not** go through
> `redis_keys.py`.
>
> The regression test `tests/test_redis_key_prefix.py` enforces this:
> no other source file in the codebase may contain a literal
> `f"supertable:..."`, `f"dataisland:..."`, `f"monitor:..."`,
> `f"spark:..."`, `f"audit:..."`, `f"shares:..."`, `f"lakes:..."`,
> `f"_apps_:..."`, or `f"registry:..."` Redis key.

## 16.2  Hierarchy

```
dataisland:                                    ── platform / dataisland-core
  _apps_:                                      ── app-bootstrap sentinel (no org)
    doc:{app_name}:
      master_mcp                               STRING  per-app master MCP coords
  {org}:                                       ── organization scope
    registry:
      {service_type}:{host}:{pid}              STRING  service heartbeat (TTL 30s)

supertable:                                    ── SuperTable SDK state
  {org}:                                       ── organization scope
    system:                                    ── org-level system namespace
      auth:tokens                              HASH    org login tokens
      audit:
        stream                                 STREAM  audit events
        chain_head:doc:{instance_id}           HASH    per-instance chain state
        config                                 HASH    runtime audit toggle
      shares:
        doc:{share_id}                         STRING  share definition
        index                                  SET     share IDs
      engine:
        thrifts                                HASH    Spark Thrift clusters
        plugs                                  HASH    Spark Plug runtimes
        duckdb                                 STRING  DuckDB runtime config
    lakes:                                     ── user-data sentinel
      {sup}:                                   ── supertable scope (user-named)
        meta:
          root                                 STRING  root pointer
          mirrors                              STRING  enabled mirror formats
          table_names                          SET     all simple table names
          leaf:
            doc:{simple}                       STRING  leaf snapshot pointer
          table_config:
            doc:{simple}                       STRING  per-table config
          staging:
            index                              SET     staging names
            doc:{staging_name}:                ── per-staging scope
              meta                             STRING  staging metadata blob
              pipes:
                index                          SET     pipe names
                doc:{pipe_name}                STRING  pipe definition
        lock:
          leaf:
            doc:{simple}                       STRING  per-table lock token
          stage:
            doc:{stage_name}                   STRING  per-staging lock token
        rbac:
          users:
            meta                               HASH    version, last_updated_ms
            index                              SET     all user_ids
            name_to_id                         HASH    username → user_id
            doc:{user_id}                      HASH    user document
          roles:
            meta                               HASH    version, last_updated_ms
            index                              SET     all role_ids
            name_to_id                         HASH    role_name → role_id
            doc:{role_id}                      HASH    role document
            type:
              doc:{role_type}                  SET     role IDs by type
        schema:
          doc:{simple}                         STRING  table schema JSON
        linked_shares:
          index                                SET     linked share IDs
          doc:{link_id}                        STRING  consumer-side linked share
    monitor:                                   ── org-wide telemetry (position 2)
      {monitor_type}:                          ── plans|writes|mcp|odata|errors|locks
        doc:{YYYY-MM-DD}                       LIST    daily-partitioned metrics
        doc:{YYYY-MM-DD}:_drain                LIST    in-progress drain handle
                                                       Each entry's payload carries
                                                       ``supertables: [str]`` for
                                                       per-sup attribution under
                                                       cross-supertable queries.
```

### Why `monitor:` lives at position 2, not under `system:`

Monitoring is **org-wide runtime telemetry** — high-volume, append-only,
event-stream-y. Audit / shares / spark / auth are **org-wide system
state** — low-volume identity / federation / compliance. Different
shape, different lifecycle. Putting them under different position-2
labels makes the distinction explicit.

Cross-supertable queries are also why monitoring can't live under
`lakes:{sup}:`: a single query touching `sales` and `customers` is
one event, not two. The org-level shape lets us record it once and
attribute it via the `supertables` payload field. Per-supertable
views filter the org list at read time.

### Daily partitions under `monitor:`

The writer pushes JSON payloads to a key suffixed with the current UTC
date — `RPUSH supertable:{org}:monitor:{type}:doc:{YYYY-MM-DD} <json>`.
Once midnight passes the writer rolls into the new day's key
automatically; yesterday's partition is frozen and safe for an
external orchestrator to drain. The `:_drain` handle is created by
`drain_partition` / `iter_partition_chunks` via `RENAME` so the
drain is atomic against any straggler write (chap. 14). This bounds
Redis growth to roughly one day per (org, monitor_type) rather than
the unbounded append behaviour of the legacy single-LIST shape.

## 16.3  Design invariants

1. **Two root prefixes only.** `supertable:` (SDK state) and
   `dataisland:` (platform state). Both are enforced by
   `assert_prefixed()`.
2. **Position 2 under `supertable:{org}:`** is *always* a literal
   closed-set SDK literal (`system`, `lakes`, or `monitor`). User input never lives at
   position 2 — it lives at position 3, behind a sentinel that names
   it (`lakes:{sup}` for supertables).
3. **Position 1 under `dataisland:`** is *always* an org name or the
   `_apps_` sentinel. The `apps` org name is reserved
   (`RESERVED_ORG_NAMES`) as defence in depth.
4. **Where user input lives at the same level as a literal sibling,
   the literal is wrapped in `:doc:` / `:index`** to disambiguate.
   This is the pattern used by `shares:`, `linked_shares:`,
   `staging:`, `pipes:`, `rbac:*:doc:`, etc.
5. **Underscore-wrapped names (`^_..._$`)** are sentinels and are
   never accepted as user-supplied identifiers (org, sup, simple,
   staging_name, pipe_name, app_name, etc.).
6. **Every constructor validates its segments via `_safe(label, value)`.**
   Lowercase alphanumeric + `-` + `_`, leading char `[a-z0-9]`,
   length 1–64. Sentinel pattern rejected.
7. **Single source of truth.** The only file in the codebase that
   may construct keys under `supertable:` or `dataisland:` is
   `supertable/redis_keys.py`.

## 16.4  Reservations

### 16.4.1  Sentinel pattern (universal)

```python
SENTINEL_RE = re.compile(r"^_[a-z0-9][a-z0-9_-]*_$")
```

Anything matching this pattern is a sentinel and is rejected as a
user-supplied identifier (org, super_name, simple table, staging
name, pipe name, app name, role type, share/user/role/link/instance
id).

### 16.4.2  Explicit reservations

```python
RESERVED_ORG_NAMES   = frozenset({"apps"})            # plus sentinel pattern
RESERVED_SUPER_NAMES = frozenset()                    # sentinel regex covers it
```

- `apps` as an org name would land at `dataisland:apps:registry:*`
  — same first three segments as `dataisland:_apps_:doc:*`. The
  reservation is defence in depth.
- Underscore-wrapped names (`_foo_`) are rejected as super_name by the sentinel pattern;
  it stays in the explicit list for backward compat with
  `SuperTable(super_name=...).` callers.

### 16.4.3  Segment-validation regex

```python
_SAFE_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
```

Applied to: `org`, `sup`, `simple`, `staging_name`, `pipe_name`,
`app_name`, `share_id`, `link_id`, `user_id`, `role_id`, `role_type`,
`instance_id`.

Closed-set inputs (`service_type`, `monitor_type`) are validated
against their own enumerations, not via `_safe()`.

The `monitor_partition` family additionally validates the `date`
segment against `^\d{4}-\d{2}-\d{2}$` (`_DATE_RE`) — anything else is
rejected at key-build time so a malformed value can't land in storage
as a key the parser cannot recover.

## 16.5  Audit toggle (runtime)

Audit is **OFF by default** (`SUPERTABLE_AUDIT_ENABLED=false`). A
per-organization runtime override is persisted in
`supertable:{org}:system:audit:config` (HASH) with fields:

```
enabled       "true" | "false"
log_queries   "true" | "false"
log_reads     "true" | "false"
hash_chain    "true" | "false"
siem_enabled  "true" | "false"
updated_ms    str(int)
updated_by    str
```

Read/write via `supertable.audit.admin.{get,set}_audit_config`, which
is exposed over HTTP at `GET / POST /api/v1/audit/config` and bound
to the **Compliance** tab on `/ui/audit`. Toggles propagate within
the in-process cache TTL (30 s); the API endpoint invalidates the
cache for the affected org so the change is immediate on the
responding instance.

Every config write itself emits a `CONFIG_CHANGE` audit event so
that turning auditing OFF is recorded — DORA Art. 6 / SOC 2 CC8.1.

## 16.6  Adding a new key

1. Add a function to `supertable/redis_keys.py` that returns the key
   string. Validate every user-supplied segment with `_safe(label, value)`.
   Make sure the key starts with `supertable:` or `dataisland:`.
2. Add a parametric entry to `tests/test_redis_key_prefix.py` so the
   regression suite covers the new shape.
3. Use the function from every call site (no inline f-strings).
4. Re-run `pytest supertable/tests/test_redis_key_prefix.py` — it
   must pass with the new helper enumerated.
5. Update §16.2 of this page.

## 16.7  Out-of-policy namespaces in `dataisland-core` (Phase 2)

A small set of per-supertable namespaces currently live outside
`redis_keys.py` and are built via inline f-strings inside
`dataisland-core/services/`:

| Namespace                                | Where it's built                                            | Purpose |
|------------------------------------------|-------------------------------------------------------------|---------|
| `supertable:{org}:{sup}:catalog:*`       | `services/common/domain/catalog.py`                          | Per-supertable catalog cache |
| `supertable:{org}:{sup}:odata:*`         | `services/common/domain/security.py`                         | OData endpoint registry |
| `supertable:odata:token:{token_hash}`    | `services/common/domain/security.py`                         | OData bearer-token reverse index |
| `supertable:{org}:{sup}:dq:*`            | `services/common/domain/quality/config.py`                   | Data-quality config/runs |
| `supertable:{org}:{sup}:runs:*`          | `services/common/domain/runs.py`                             | Job-run tracking |
| `supertable:rate:{client_ip}:{minute}`   | `services/common/middleware/rate_limit.py`                   | Rate-limit buckets (root-level) |
| `supertable:{org}:{sup}:meta:users:*`    | `services/common/server.py` (`_list_users_legacy`)           | v0 legacy fallback (read-only) |
| `supertable:{org}:{sup}:meta:roles:*`    | `services/common/server.py` (`read_role` fallback)           | v0 legacy fallback (read-only) |

These do **not** collide with v2 SDK keys (they sit at position 2 =
`{sup}` rather than under `lakes:`), so they continue to function.
However, they remain inconsistent with v2 hierarchy discipline and
the position-2 user-input pattern.

**Phase 2 plan** (separate session): fold these namespaces into
`redis_keys.py` (under `lakes:{sup}:catalog:*`, `lakes:{sup}:odata:*`,
`lakes:{sup}:dq:*`, `lakes:{sup}:runs:*`), and add a `_globals_`
sentinel under `dataisland:` for the rate-limit buckets and the
OData token reverse index. The `meta:users:*` / `meta:roles:*`
fallbacks are deletable — primary code paths read from
`rbac:users:*` / `rbac:roles:*`.
