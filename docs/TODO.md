# SuperTable SDK — Roadmap / TODO

Living document. Captures the plan distilled from the holistic review.
Scope: **SDK only** — anything that's a service / daemon belongs in the
layer above (the user's orchestrator), not here.

## TL;DR — If you can only do three things next

1. **Generic error handling** — standardise on typed exceptions + a single API-layer adapter. Half-day of work; prevents the next decade of bugs.
2. **DR rebuild method** — `rebuild_redis_from_storage(org, super)` callable from outside the SDK. Single most consequential item on this list. Without it, Redis loss = total data inaccessibility.
3. **Time-travel queries** — `DataReader(..., as_of_version=N)` / `as_of_timestamp_ms=...`. The snapshot chain already exists; just plumb the parameter through.

Everything below is the detailed version of the plan.

---

## Tier 1 — Foundation (correctness + reliability)

### 1. Generic error handling

**Problem.** Error handling is inconsistent across the SDK:
- Some paths raise typed exceptions.
- Some return `(empty_df, Status.ERROR, message)` tuples.
- Some return `None` or empty lists silently.
- Some swallow exceptions with broad `except Exception`.

This makes the API hard to reason about and is the root cause of footguns like "the call returned None but I don't know if that's no-data or an error".

**Design sketch.**
- Extend `supertable/errors.py` with a small typed exception hierarchy:
  - `SupertableError` (root)
    - `SupertableLookupError` (already exists)
      - `SuperTableNotFoundError`, `TableNotFoundError`
    - `SupertableValidationError` — schema, column, name validation failures
    - `SupertableConcurrencyError` — lock timeout, CAS failure
    - `SupertableStorageError` — backend I/O failure
    - `SupertableConfigError` — bad env var, missing credential
- One adapter at the API/UI boundary translates the hierarchy into HTTP / CLI responses.
- Internally **everything raises**. The `(df, Status, msg)` return shape on `DataReader.execute` is the **only** approved exception, kept for back-compat.
- Audit every `except Exception` and convert to specific catches. Document the legitimate "best-effort never-raise" places (monitoring writer, GC enqueue) explicitly.

**Effort.** 1 day for the new exception types + 2-3 days to audit and refactor existing call sites + a regression test that enumerates every public method's error contract.

**Tests needed.**
- Every typed exception has a `test_<class>_carries_attributes()` check.
- Every public method has a `test_<method>_raises_<typed>_on_<condition>()` test.
- One end-to-end test that drives a real failure (e.g. RBAC denied) and asserts the API adapter returns the right HTTP code.

---

### 2. DR rebuild method

**Problem.** Redis is the single point of failure. Parquet files are durable on S3/MinIO, but the leaf pointers, root versions, RBAC, tombstones, mirror config — all in Redis. If Redis is wiped, the data is *on storage but invisible*. Every Redis upgrade / migration / DR drill is a "fingers crossed" event.

**Design sketch.** New module `supertable/recovery/`:

```python
from supertable.recovery import rebuild_from_storage

result = rebuild_from_storage(
    organization="acme",
    super_name="warehouse",
    storage=get_storage(),
    catalog=RedisCatalog(),
    dry_run=False,         # set True to preview
    rebuild_rbac=True,     # restore a single bootstrap superadmin
)
# result: {"tables_rebuilt": 12, "snapshots_indexed": 47,
#          "rbac_restored": true, "errors": [...]}
```

What it does:
1. Walks `{org}/{super}/tables/` on storage to discover simple tables.
2. For each table, walks `{...}/snapshots/` to find the most recent JSON file (by filename timestamp prefix).
3. Validates that snapshot (resources exist on storage, schema is parseable).
4. Reconstructs in Redis:
   - `meta:root` (with `version` derived from snapshot count + a fresh timestamp)
   - `meta:leaf:doc:{simple}` (pointer + payload to most recent snapshot)
   - `meta:table_names` (SET of simple names)
   - `schema:doc:{simple}` (from snapshot.schema)
5. Optionally reconstructs RBAC with a single bootstrap superadmin role (the operator runs the tool, then manually adds users back).
6. Does NOT rebuild: per-table tombstones (we don't have a source for them), mirror config (operator re-enables manually), share definitions.

**Caller pattern.** The user's orchestrator decides when to run this — typically as a Kubernetes Job triggered by an ops runbook.

**Effort.** 2-3 days including a thorough integration test.

**Tests needed.**
- Unit: each helper (snapshot discovery, leaf reconstruction, RBAC bootstrap) tested independently.
- Integration: spin up a real Redis + LocalStorage, write a few tables, drop Redis, run `rebuild_from_storage`, verify all data is queryable again.
- Dry-run mode reports the same actions without executing.

---

### 3. Schema evolution validation

**Problem.** Today a write with an incompatible schema (was `Int64`, write has `Utf8`) silently coerces via `_resolve_unified_dtype` in `concat_with_union`. The snapshot's `schema` field gets updated to the *last write's* schema, even when older files in the same snapshot have a wider schema. Result: inconsistent metadata view.

**Design sketch.** New step in `DataWriter.write()` between validation and lock acquisition:

```python
def _validate_schema_compat(
    self,
    incoming_schema: dict,
    existing_schema: list,
    mode: str = "widen",   # "strict" | "widen" | "reject"
) -> None:
    """
    strict: reject any column-set or dtype difference.
    widen:  allow adding columns + widening dtypes; reject narrowing.
    reject: allow any change but record an entry in the schema migrations log.
    """
```

Configurable per-table via `configure_table(schema_evolution_mode="widen")`. Default `"widen"` matches current behaviour.

When a change happens:
- Append an entry to a new `schema_migrations` field on the snapshot:
  ```json
  {"ts_ms": 1700..., "write_id": "qid_42", "role_name": "admin",
   "changes": [{"op": "add", "column": "email", "type": "string"},
               {"op": "widen", "column": "amount", "from": "int", "to": "double"}]}
  ```
- Pairs naturally with the time-travel feature: a query "as of v5" can show the schema that was active at v5.

**Effort.** 1.5 days for the validator + 1 day for the migrations log + tests.

**Tests needed.**
- Each mode's accept/reject behaviour tested with concrete schema pairs.
- The migrations log entry shape is round-trippable through snapshot JSON.
- Backward compat: tables without `schema_migrations` field don't break MetaReader.

---

### 4. Time-travel queries

**Problem.** No way to query data as of a specific snapshot version or timestamp. The infrastructure exists (snapshot chain, `read_simple_table_snapshot`), but `DataReader` only consults the current leaf.

**Design sketch.** Two new optional kwargs on `DataReader`:

```python
dr = DataReader(
    super_name="warehouse", organization="acme",
    query="SELECT count(*) FROM orders",
    as_of_version=5,          # int snapshot_version
    # OR
    as_of_timestamp_ms=1700000000000,   # epoch ms — picks latest snapshot ≤ ts
)
df, status, msg = dr.execute(role_name="admin")
```

Implementation:
1. New helper `SimpleTable.resolve_snapshot(version=None, timestamp_ms=None)`:
   - If `version`: walk `previous_snapshot` chain until `snapshot_version == version`.
   - If `timestamp_ms`: walk chain until `last_updated_ms <= timestamp_ms`.
   - Raises `SnapshotNotFoundError` if version/timestamp is out of range.
2. `DataEstimator` accepts a resolved snapshot dict instead of always reading from leaf.
3. `DataReader._assert_targets_exist` stays unchanged (current leaf must still exist).
4. RBAC check uses the CURRENT permissions (not historical) — a deleted role can't be revived by time-travel.

**Effort.** 2-3 days.

**Tests needed.**
- `resolve_snapshot` finds the right version/timestamp; raises on miss.
- Time-travel query returns the resource list as of the chosen version, not the current one.
- A column dropped between v5 and v8 reappears in `SELECT * AS OF VERSION 5`.
- Permission denied at current time is denied at all historical times (no time-travel-into-permission-bypass).

---

## Tier 2 — Operational hygiene

### 5. Rate-limiting method

**Problem.** A runaway client can pummel Redis with snapshot churn. Without a rate limit, one bad batch job can take down monitoring for everyone.

**Design sketch.** Pure method, no scheduling. The orchestrator decides when to call it.

```python
from supertable.ratelimit import check_write_rate, RateLimitExceeded

try:
    check_write_rate(
        catalog=RedisCatalog(),
        organization="acme",
        simple_name="orders",
        max_writes_per_min=100,
    )
except RateLimitExceeded as e:
    # caller decides: queue, reject, sleep
    ...
DataWriter("warehouse", "acme").write(...)
```

Implementation: token bucket per `(org, simple)` in Redis using `INCR` + `EXPIRE` on a minute-bucket key.

**Effort.** 1 day.

---

### 6. RLS DSL

**Problem.** Current `RbacViewDef.where_clause` is free-form SQL. Hard to validate, easy to mis-quote.

**Design sketch.** Structured filter that the SDK compiles to safe SQL:

```python
filters = [
    {"column": "tenant_id", "op": "eq", "value": "{{session.tenant_id}}"},
    {"column": "region",    "op": "in", "value": ["EU", "UK"]},
]
```

Compiler in `supertable/rbac/filter_compiler.py`:
- Whitelists ops (`eq`, `ne`, `in`, `not_in`, `gt`, `gte`, `lt`, `lte`, `between`, `is_null`).
- Validates column names against the table's schema (rejects unknown columns).
- Renders to parameterised SQL fragments.

Keeps `where_clause` as a legacy escape hatch for now.

**Effort.** 2 days.

---

### 7. Snapshot rollback API

**Problem.** No way to undo a bad batch. The snapshot chain is preserved (GC respects retention) but there's no API.

**Design sketch.** New method on `SimpleTable`:

```python
SimpleTable(super_table, "orders").rollback_to(
    role_name="admin",
    version=5,                 # OR as_of_timestamp_ms=...
    reason="bad ETL run on 2026-06-09",
)
```

Implementation: writes a NEW snapshot with `resources` copied from v_5 and `previous_snapshot` pointing at the current leaf. So the chain becomes `v_8 → v_7 → ... → v_5 (rolled-back) → v_4 → ...`. The leaf-CAS swings to the new snapshot. GC eventually deletes orphaned parquet files from v_6/v_7 (only those not referenced by v_5).

Records a `lineage` entry with `source_type="rollback"` so audit can find it.

**Effort.** 1 day.

---

### 8. Storage tier archival

**Problem.** Cold data on hot storage costs money.

**Design sketch.**
- New resource field `tier`: `"hot"` (default) | `"cold"`.
- New module `supertable/archival/`:
  - `RetentionPolicy(max_hot_age_days=90, ...)` declarative config.
  - `archive_partitions(catalog, storage, org, super, policy) -> stats`:
    - Walks resources, finds ones older than `max_hot_age_days` (using `last_updated_ms` from the snapshot they were first written in — track via lineage).
    - Calls `storage.set_tier(path, "cold")` — S3 Glacier transition, Azure cool tier, MinIO no-op.
    - Updates the snapshot's resource entry to `tier: "cold"`.
- DataReader change (~10 LOC): when building `Reflection`, skip resources with `tier == "cold"` unless `include_cold=True` is passed. Emit a warning when cold resources are skipped from a query that matches them.

**Effort.** 2-3 days. Mostly additive.

**Tests needed.**
- Resource entries round-trip the `tier` field.
- DataReader by default excludes cold; with `include_cold=True` includes them.
- `archive_partitions` is idempotent.
- Storage backends without tier support (`LocalStorage`) treat `set_tier` as a no-op.

---

## Tier 3 — Hardening (lower urgency)

### 9. Property-based tests with Hypothesis

User-owned. The SDK side: optionally scaffold a `tests/property/` directory with sample generators (random Polars DataFrames, random write sequences) so the user has a starting point.

### 10. Chaos test harness

`tests/chaos/` that monkey-patches specific code points to fail at specific moments (between leaf-CAS and bump-root; in the middle of compact_resources flush; etc.) and asserts no data loss / no inconsistent state.

**Effort.** 1 day for the harness + 1 day per failure scenario.

### 11. Constructor refactor (deferred to next major version)

Replace `SuperTable.__init__` + `create_if_missing` kwarg with explicit factory methods:

```python
SuperTable.load(name, org)              # raises if missing
SuperTable.create(name, org)            # raises if exists
SuperTable.load_or_create(name, org)    # current default behaviour
```

Same for `SimpleTable`. `__init__` becomes private or does pure binding (no I/O).

**Why deferred.** The kwarg solution works. The refactor is structural cleanup, not a bug fix. Hold for a v3 major version when a breaking API change is acceptable.

---

## Tier 4 — Considered and not planned

Each rejected with rationale, recorded so future reviewers know they were thought about.

| Item | Rejected because |
|------|------------------|
| Lock heartbeating | User confirmed the existing model is intentional. |
| Metrics exporter (Prometheus / OTel) | This is a service concern, lives above the SDK. |
| Idempotency tokens for writes | Application-layer concern. |
| Orphan file detector | Out of scope for this iteration. |
| Table cloning / zero-copy clones | Not requested. |
| Streaming / CDC ingestion | Service-layer responsibility. User's orchestrator does this. |
| Materialized views / incremental aggregates | Out of scope. |
| Bloom filters in parquet | Out of scope. |
| Real-infrastructure integration tests in CI | Handled outside the SDK repo. |

---

## Bonus / parallel idea

### Schema migrations log

Mentioned under Tier 1 §3. Worth calling out separately because it composes well with multiple Tier 2 items:

- **Time-travel + schema migrations** = `SELECT * FROM orders AS OF VERSION 5` correctly returns the columns that existed at v_5.
- **Rollback + schema migrations** = rolling back to v_5 also rolls back the schema view.
- **Audit + schema migrations** = compliance log of "who changed what column when".

Couple with Tier 1 §3 in the same PR.

---

## Suggested 4-week sprint

### Week 1 — Foundation
- Generic error handling refactor (1 day)
- DR rebuild method (2 days)
- Schema evolution validation + migrations log (2 days)

### Week 2 — User-facing features
- Time-travel queries (2 days)
- Snapshot rollback API (1 day)
- Storage tier archival (2 days)

### Week 3 — Operational
- Rate limiting helper (1 day)
- RLS DSL (2 days)
- Chaos test harness (1 day)
- Property-based test skeleton (1 day)

### Week 4 — Buffer / quality
- Cover test gaps revealed by chaos + property runs.
- Documentation pass.
- Optional: 2-hour real-infra smoke test against staging.

---

## Reference: direct answers to questions raised during review

### "Is FOR VERSION AS OF / FOR TIME AS OF handled?"

**No.** Infrastructure is there (snapshot chain, `read_simple_table_snapshot`), but `DataReader` only reads the current leaf. Need to plumb a version/timestamp param through. See Tier 1 §4.

### "What does Streaming/CDC look like?"

The SDK side: a `StreamWriter` class that buffers records and calls `DataWriter.write` at configurable batch / flush thresholds. The actual Kafka/Debezium/Kinesis consumer lives in the orchestrator. Not planned for the SDK (Tier 4).

### "Storage tier archival — code change or new code?"

Mostly new code (`supertable/archival/`), plus a ~10 LOC change in `DataReader` to filter cold resources by default. See Tier 2 §8.

### "Constructor side effects — example?"

Constructing `MetaReader(super_name="ghost", organization="acme")` does six pieces of work:
1. `SuperTable.__init__` checks Redis root.
2. If missing: `storage.makedirs()` (creates dir).
3. If missing: `catalog.ensure_root()` (creates Redis pointer).
4. `RoleManager.__init__` → `_init_role_storage` (creates RBAC meta).
5. Creates default superadmin role.
6. `UserManager.__init__` (creates user storage).

So constructing a Python object for read-only inspection materialises six pieces of state. We fixed the immediate bug with `create_if_missing=False`, but the structural problem is "constructors do work". The cleaner fix is the factory split deferred to v3 (Tier 3 §11).

---

## Notes for future me / next reviewer

- Every item in Tier 1-3 was vetted in the holistic review. The Tier 4 items were considered and rejected with rationale.
- The Suggested Sprint above is a working estimate. Real velocity depends on test coverage of the existing code.
- Effort estimates assume **one full-time developer who knows this codebase**. Add 50% if onboarding a new contributor.
- This document should be updated as items are completed (move them to a `## Done` section at the bottom, don't delete — keep the audit trail).
