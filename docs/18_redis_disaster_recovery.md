# Redis disaster recovery

SuperTable data files are not sufficient to identify committed catalog state.
A writer uploads a new immutable snapshot before the fenced Redis commit, so a
higher-version loose snapshot may be an abandoned pre-commit object. The DR
tool therefore restores only a content-sealed catalog checkpoint made while
Redis was healthy. It never guesses a leaf pointer.

## Prepare and test checkpoints

Stop writers, catalog/RBAC administrators, and the privileged archive worker.
Drain the privileged WAL completely before stopping that worker, then create a
checkpoint:

```bash
supertable-redis-recovery --organization acme --checkpoint
```

The checkpoint contains every persistent Redis key in the organization's
SuperTable namespace, including table catalog, RBAC documents and auth-token
metadata. Protect `acme/__recovery__/redis_catalog/` with the same encryption,
access controls, object versioning and retention policy as security backups.
Expiring locks, monitoring partitions, and rebuildable audit runtime keys are
deliberately excluded. The immutable privileged-activation anchor is preserved.
The checkpoint also binds the exact delivered privileged archive batch,
sequence, stream ID, manifest path, and manifest hash; it fails if the Redis WAL
is ahead of or behind that archive position, has pending deliveries, or changes
during capture. The command reads the catalog twice and fails if it is changing.
It computes and records the exact byte size and SHA-256 of every selected data,
tombstone, and statistics artifact rather than accepting object existence or a
reused key as proof.

Run this after every catalog/control-plane change or from a supervised job.
Test restore into a disposable Redis instance regularly; a checkpoint is not a
backup until that drill succeeds.

## Rebuild

1. Stop every API, writer, scheduler, monitoring drain and privileged-audit
   worker. Keep them stopped until verification finishes.
2. Provision an empty Redis instance with AOF enabled, `appendfsync always`,
   `maxmemory-policy noeviction`, hostname-verified TLS, and the required
   replica policy.
3. Point SuperTable storage and Redis configuration at the recovery targets.
4. Verify without writing:

   ```bash
   supertable-redis-recovery --organization acme --dry-run
   ```

5. Apply the exact plan:

   ```bash
   supertable-redis-recovery --organization acme --apply
   ```

6. Run `--dry-run` again. It must report `already_current: true`.
7. Run the privileged worker with `--verify-chain`, then its health check,
   supplying the mandatory independently pinned activation baseline on both
   invocations as documented in the
   [privileged-worker runbook](17_privileged_audit_worker.md).
8. Execute application read-only smoke tests before allowing writes.

The restore validates every catalog checkpoint in the retained set and walks
the immutable privileged-audit checkpoint/Parquet chain from the checkpoint's
sealed tip to genesis. A later archive object is never combined with an older
catalog checkpoint: dry-run/apply rejects that mismatch because rolling back
the producer sequence would fork the audit chain. Create a new common catalog
checkpoint before the incident, or restore storage to the same point in time.
It restores the latest verified stream entry as the producer's monotonic
sequence anchor, its exact meta hash, the archive head, the archival
consumer-group cursor, and the original immutable activation anchor. Future
RBAC mutations therefore continue at the next sequence instead of being fenced
or silently restarting at one.

## Fail-closed behavior and limitations

- `--dry-run` is non-mutating. `--apply` is always explicit.
- The destination must be empty, or already match the exact recovery plan.
  Partial, stale, wrong-type or unexpected organization keys are rejected and
  are never deleted or overwritten.
- A repeated successful apply is an idempotent no-op.
- Missing/tampered checkpoints, snapshots, same-key data replacements, size
  drift, audit manifests, or audit Parquet files stop recovery.
- Loose snapshots created after the latest sealed checkpoint are not promoted.
  This is intentional: their commit status cannot be proven after Redis loss.
  The checkpoint cadence defines catalog RPO.
- General high-volume audit and monitoring partitions are not restored by this
  tool. Their storage sinks and explicit monitoring drain receipts are their
  separate recovery boundaries.
- Keep the deployment stopped if a Redis transaction or read-back verification
  fails. Inspect or replace that destination; the tool will not repair a
  partially populated target by overwriting it.
