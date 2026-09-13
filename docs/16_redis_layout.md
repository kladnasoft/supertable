# Redis key layout

[redis_keys.py](../supertable/redis_keys.py) builds the key names used by the Python package. Prefixes are constants: `supertable` for table/runtime state and `dataisland` for service/application registry keys. `SUPERTABLE_PREFIX` configures object-storage keys and does not change Redis key names.

This reference gives exact builder output. A builder defines a name; it does not, by itself, create the key or enforce a Redis data type. Types below are identified from the catalog and subsystem operations that write them.

## Scope notation

The tables below use these abbreviations:

| Symbol | Expansion |
| --- | --- |
| `S` | `supertable:<org>:system` |
| `L` | `supertable:<org>:lakes:<sup>` |
| `Q` | `supertable:<org>:query` |
| `M` | `supertable:<org>:monitor` |

Angle-bracket names represent validated caller-supplied segments, except date and registry-host fields, which have their own validators. Actual examples are `supertable:acme:lakes:sales:meta:root` and `supertable:acme:query:job:doc:abc123`.

## Name validation

Most user-supplied segments must match:

```text
^(__[a-z0-9][a-z0-9_-]{0,59}__|[a-z0-9][a-z0-9_-]{0,63})$
```

Ordinary names start with a lowercase letter or digit and contain lowercase letters, digits, underscores, or hyphens, up to 64 characters. The separate double-underscore form permits internal names such as `__global__`. `_safe()` does not lowercase or trim names. Uppercase letters, spaces, colons, wildcard characters, empty strings, and nonstrings are rejected.

The sentinel pattern `^_[a-z0-9][a-z0-9_-]*_$` is reserved. `is_reserved_org_name()` also treats `apps` as reserved; `is_reserved_super_name()` recognizes sentinel names. These reservation helpers are distinct from `_safe()`: the generic segment validator alone does not reject the ordinary string `apps`.

`assert_prefixed()` checks only that a string begins with `supertable:` or `dataisland:`. It does not validate the remaining hierarchy. `parse_lake_key()` recognizes the lake prefix and returns `(org, sup)`; it is a parser, not full segment validation.

## Lake metadata

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `meta_root` | `L:meta:root` | JSON string: version, timestamp, optional root flags |
| `meta_mirrors` | `L:meta:mirrors` | JSON string: `formats`, `ts` |
| `meta_namespace_deletion_intent` | `L:meta:deletion-intent` | Builder-defined namespace marker; core table deletion does not currently set it |
| `meta_table_names` | `L:meta:table_names` | Set of simple-table names, populated by writer acceleration updates |
| `meta_leaf` | `L:meta:leaf:doc:<simple>` | JSON string: version, timestamp, snapshot path, optional inline payload |
| `meta_rowid_seq` | `L:meta:rowid_seq:doc:<simple>` | Integer string incremented with `INCRBY` |
| `meta_table_config` | `L:meta:table_config:doc:<simple>` | JSON string: per-table configuration and `modified_ms` |
| `schema` | `L:schema:doc:<simple>` | JSON string: schema acceleration data |

Root and leaf versions are independent. Snapshot history is linked by `previous_snapshot` inside storage JSON documents; there is no Redis history-list key for table versions. See [catalog](05_redis_catalog.md).

## Locks

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `lock_leaf` | `L:lock:leaf:doc:<simple>` | Expiring string: UUID-hex owner token |
| `lock_stage` | `L:lock:stage:doc:<stage>` | Expiring string: UUID-hex owner token |

`lock_leaf_prefix()` returns `L:lock:leaf:doc:`. Locks are leases, not version counters. Stage and pipe mutations share the stage key. See [locking](08_locking.md).

## Staging and pipes

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `staging_index` | `L:meta:staging:index` | Set of stage names |
| `staging_doc` | `L:meta:staging:doc:<stage>:meta` | JSON string: stage metadata |
| `pipe_index` | `L:meta:staging:doc:<stage>:pipes:index` | Set of pipe names |
| `pipe_doc` | `L:meta:staging:doc:<stage>:pipes:doc:<pipe>` | JSON string: pipe definition |

The stage's data-file index is a storage JSON file, `<org>/<sup>/staging/<stage>_files.json`, rather than a Redis list. See [ingestion](07_ingestion.md).

## RBAC

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `rbac_user_meta` | `L:rbac:users:meta` | Hash: version, last-updated timestamp, initialization state |
| `rbac_user_index` | `L:rbac:users:index` | Set of user IDs |
| `rbac_username_to_id` | `L:rbac:users:name_to_id` | Hash: lowercase username → user ID |
| `rbac_user_doc` | `L:rbac:users:doc:<user_id>` | Hash: user fields; list/dictionary fields are JSON strings |
| `rbac_role_meta` | `L:rbac:roles:meta` | Hash: version, last-updated timestamp, initialization state |
| `rbac_role_index` | `L:rbac:roles:index` | Set of role IDs |
| `rbac_rolename_to_id` | `L:rbac:roles:name_to_id` | Hash: lowercase role name → role ID |
| `rbac_role_doc` | `L:rbac:roles:doc:<role_id>` | Hash: role fields; structured fields are JSON strings |
| `rbac_role_type_index` | `L:rbac:roles:type:doc:<role_type>` | Set of role IDs of that type |

`rbac_user_doc_prefix()` and `rbac_role_type_index_prefix()` expose the respective prefixes for Lua operations. Human-readable usernames and role names are hash fields, not key segments, and have separate validators. See [RBAC](11_rbac.md).

## Organization system state

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `auth_tokens` | `S:auth:tokens` | Hash: SHA-256 token ID → JSON token metadata |
| `audit_stream` | `S:audit:stream` | Redis stream of audit event field maps |
| `audit_chain_head` | `S:audit:chain_head:doc:<instance_id>` | Hash: `head`, `batch_count`, `updated_ms` |
| `audit_config` | `S:audit:config` | Hash: audit setting overrides |
| `audit_legal_hold` | `S:audit:legal_hold` | String `1` or `0` |
| `share_doc` | `S:shares:doc:<share_id>` | JSON string: supplied share metadata |
| `share_index` | `S:shares:index` | Set of share IDs |
| `engine_thrifts` | `S:engine:thrifts` | Hash: Spark cluster ID → JSON configuration |
| `engine_plugs` | `S:engine:plugs` | Hash: Spark plug ID → JSON configuration |
| `engine_duckdb` | `S:engine:duckdb` | JSON string: engine configuration sections and modification time |

Audit instance IDs are normalized by the audit writer before calling the key builder. Audit consumer groups are Redis stream metadata, not independent top-level keys. The internal archival group is named `__archival__`. See [audit](12_audit.md).

Login token expiry is a field in the stored metadata and is checked by the full token validator; creating a token does not attach a Redis expiry to its hash field.

## Linked shares

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `linked_share_index` | `L:linked_shares:index` | Set of link IDs |
| `linked_share_doc` | `L:linked_shares:doc:<link_id>` | JSON string: linked-share metadata |

Organization share definitions and lake-local linked shares have different scopes and different indexes.

## Streaming query jobs

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `query_job_doc` | `Q:job:doc:<job_id>` | Hash: job record |
| `query_job_chunks` | `Q:job:chunks:<job_id>` | List: appended JSON chunk references |
| `query_job_cancel` | `Q:job:cancel:<job_id>` | Expiring string `1` requesting cancellation |
| `query_job_index` | `Q:jobs:index` | Set of job IDs |

[JobStore](../supertable/streaming/jobs.py) applies `SUPERTABLE_STREAM_JOB_TTL_SEC` to job records, chunk lists, and cancellation markers. The default is 3,600 seconds. Updates refresh record expiry and chunk appends refresh list expiry. The index itself has no TTL, so expired job hashes can leave index members until explicit cleanup. Chunk contents live in storage; the Redis list contains references.

## Quality state

`quality_prefix(org, sup)` returns `L:quality:`. `quality_doc()` joins one or more validated nonempty segments beneath that prefix. `quality_table_key()` generates `L:quality:<kind>:<table>`.

The current quality subsystem writes these shapes:

| Key | Redis type / contents |
| --- | --- |
| `L:quality:config:__global__` | JSON string: global checks configuration |
| `L:quality:config:<table>` | JSON string: table overrides |
| `L:quality:rules:index` | Set of rule IDs |
| `L:quality:rules:doc:<rule_id>` | JSON string: a rule definition |
| `L:quality:schedule` | JSON string: global schedule |
| `L:quality:schedule:<table>` | JSON string: table schedule |
| `L:quality:latest:<table>` | JSON string: latest table result |
| `L:quality:latest:<table>:<column>` | JSON string: latest column result |
| `L:quality:anomalies:<table>` | JSON string: anomaly results |
| `L:quality:history` | List: newest history rows pushed to the head |
| `L:quality:pending:<table>` | Expiring timestamp string; pending work, default 600 seconds |
| `L:quality:running:<table>` | Expiring owner-token string; active work, default 300 seconds |
| `L:quality:cooldown:<table>` | Expiring timestamp string; cooldown, default 300 seconds |

These names come from [quality configuration](../supertable/quality/config.py), [history](../supertable/quality/history.py), and [scheduler](../supertable/quality/scheduler.py). Table and column segments passed through these builders must satisfy the key validator.

## Monitoring partitions

| Builder | Key | Redis type / contents |
| --- | --- | --- |
| `monitor_partition` | `M:<type>:doc:<YYYY-MM-DD>` | List of JSON monitoring entries |
| `monitor_partition_drain` | `M:<type>:doc:<YYYY-MM-DD>:_drain` | Temporary list used while draining a partition |

Accepted types are `plans`, `writes`, `mcp`, `odata`, `errors`, `locks`, and `compact`. The date validator checks the shape `YYYY-MM-DD`; it does not validate calendar correctness. `parse_monitor_partition_key()` accepts the six-part primary partition key and rejects a drain key with its additional suffix. Partition processing and retention are described in [monitoring](14_monitoring.md).

## Service and application registry builders

| Builder | Key |
| --- | --- |
| `registry` | `dataisland:<org>:registry:<service_type>:<host>:<pid>` |
| `app_master_mcp` | `dataisland:_apps_:doc:<app_name>:master_mcp` |

Registry service types are `api`, `webui`, `odata`, `mcp`, `sdk`, and `lighthouse`. Host must be a nonempty string without a colon; it is not validated using the ordinary segment regex. PID must convert to a positive integer. `parse_registry_key()` extracts `(org, service_type, host, pid)` as strings from a matching prefix.

These builders do not choose a Redis type, expiry, heartbeat interval, or payload. A service using them must supply those behaviors.

## Scan patterns and deletion boundaries

| Builder | Output |
| --- | --- |
| `system_scope_pattern(org)` | `supertable:<org>:system:*` |
| `lakes_pattern(org)` | `supertable:<org>:lakes:*` |
| `super_table_pattern(org, sup)` | `L:*` |
| `meta_root_pattern_for_org(org)` | `supertable:<org>:lakes:*:meta:root` |
| `meta_root_pattern_all_orgs()` | `supertable:*:lakes:*:meta:root` |
| `meta_leaf_pattern(org, sup)` | `L:meta:leaf:doc:*` |
| `lock_leaf_pattern(org, sup)` | `L:lock:leaf:doc:*` |
| `staging_pattern(org, sup)` | `L:meta:staging:doc:*:meta` |
| `staging_subkey_pattern(org, sup, stage)` | `L:meta:staging:doc:<stage>:*` |
| `pipe_pattern(org, sup, stage)` | `L:meta:staging:doc:<stage>:pipes:doc:*` |
| `query_job_pattern(org)` | `Q:job:*` |
| `query_job_subkey_pattern(org, job)` | `Q:job:*:<job>` |
| `quality_table_pattern(org, sup, kind)` | `L:quality:<kind>:*` |
| `monitor_partition_pattern(org, type)` | `M:<type>:doc:*` |
| `monitor_partition_pattern_for_org(org)` | `M:*:doc:*` |
| `registry_pattern_for_org(org)` | `dataisland:<org>:registry:*` |
| `registry_pattern()` | `dataisland:*:registry:*` |
| `app_scope_pattern()` | `dataisland:_apps_:doc:*` |

`system_scope()` and `lakes_scope()` return scope prefixes without a trailing wildcard. Query-job patterns cover job keys but not `Q:jobs:index`. Monitoring patterns can match drain keys; the parser is used to identify primary partitions.

Deleting a lake namespace with `L:*` covers its metadata, locks, RBAC, staging/pipe metadata, linked shares, and quality state. It leaves organization system, query, monitor, service registry, and application keys. The catalog's simple-table delete is narrower: it deletes only the leaf and leaf lock. See [catalog deletion](05_redis_catalog.md) before relying on a cleanup operation to remove related indexes or configuration.
