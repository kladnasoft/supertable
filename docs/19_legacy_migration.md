# Offline 2.4.0 metadata migration

SuperTable 2.5.9 keeps migration data-preserving: original data Parquet files,
their physical row IDs, and historical snapshots are retained. It writes a
sealed current-format deletion vector, rebuilt statistics, and a successor
snapshot, then publishes that snapshot with a fenced catalog compare-and-swap.
It does not compact data or physically remove deleted rows.

## What changed in 2.5.9

- Each genuine 2.4.0 data resource is fully scanned once per successful SDK
  migration invocation, instead of once in preflight and again in publication.
- The publication pass reuses the invocation's exact disk-backed row-ID index
  and integrity proofs only when the source snapshot, object identities, and
  footer-derived resource metadata are unchanged. It still revalidates deletion
  membership, statistics, the allocator, locks, and catalog publication.
- Known statistics and deletion-vector size/count failures are rejected before
  decoding the data resources. Existing limits are unchanged.
- The fixed-size-array validation worker also supports processes with more
  than 1,024 open file descriptors, without changing its memory or time limits.

This is deliberately an invocation-local optimization, not a persistent job
format. No proof is reused across processes or separate migration calls.
An application's separate eligibility invocation still performs its own scan;
polling or repeatedly invoking eligibility does not share these SDK proofs.

## Before running

Stop all old readers, writers, ingestion, compaction, and garbage collection.
Version 2.4.0 does not honor the new namespace lock. The confirmation below is
an operator assertion, not a mechanism that stops those processes.

Preserve a recoverable, coordinated catalog/allocator backup and protect the
referenced objects and old snapshots from cleanup. Verify the exact organization,
supertable, and complete table inventory. Mirror-enabled and read-only/replica
namespaces remain unsupported by this migration.

Size the **currently referenced** resources, not just the storage bucket. Per
simple table, existing limits include 10,000 files, one billion physical rows,
2 TiB compressed data, 100,000 aggregate footer column chunks, and 256 MiB of
aggregate footers. Statistics have a 100,000-row and 64 MiB working-envelope
limit; a statistics row represents a file/row-group/column, not a data row.
Active tombstones are limited to one million entries, 64 MiB compressed and
256 MiB decoded. Snapshot JSON must fit 8 MiB. A 200 GB table is not automatically
within these other limits.

The worker spills one compressed source file at a time and retains private
SQLite row-ID indexes until each table's publication pass. Plan conservative
scratch capacity for the largest source file plus 96 bytes per physical row
across all pending tables, plus fixed headroom (at least 256 MiB). Each new scan
also checks remaining free space. For 100 million physical rows, the row-index
allowance alone is 9.6 GB. Proof metadata is stored on scratch disk too; SQLite
connections are closed between table passes. This is not a second full data copy.

## Invocation

Only after the system is offline and the inventory is verified:

```python
from supertable import SuperTable

table = SuperTable(
    super_name="your-supertable",
    organization="your-organization",
    create_if_missing=False,
)
result = table.migrate_legacy_metadata(
    confirm_system_offline=True,
    expected_tables=["facts", "events"],  # Complete operator-approved inventory.
)
print(result["migrated_tables"])
```

Every table must pass preflight before any table is published. Catalog commits
are atomic **per table**, not across the namespace. Keep traffic stopped until
the complete migration and acceptance checks succeed.

## Failure and retry

On handled failure or interruption, temporary proof indexes are closed and
removed and locks are released. A forced process kill can leave unreferenced
scratch directories; they are not trusted as checkpoints on the next run.
Remove abandoned scratch only after verifying its owning process has stopped.

Retry the same SDK call while still offline. Already migrated tables are
validated and skipped; unfinished tables are fully rescanned. Newly written but
unpublished metadata artifacts can remain unreferenced after a failure, while
the last committed catalog snapshot remains authoritative. Do not remove old
objects or roll the allocator backwards to recover space.

Before reopening traffic, compare visible data/deletion behavior and verify
that a rerun reports no migrated tables. Retain old metadata through the
acceptance window. Once new-version writes begin, rollback is not merely a
pointer change. This patch does not add automatic rollback or migrate historical
snapshots, roles, or historical column-type changes.
