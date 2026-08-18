# 17  Privileged Audit Archive Worker Runbook

The privileged audit worker is a separately supervised process that drains the
mandatory RBAC/auth-token control-plane Redis Stream into verified Parquet artifacts. It delegates all
archive verification, acknowledgement, and conservative trimming to
`PrivilegedAuditOutbox`; the worker never issues `XACK` or `XDEL` itself.

The installed command is:

```bash
supertable-privileged-audit-worker \
  --organization acme \
  --consumer audit-worker-01
```

Run one worker deployment per organization. Use a stable, unique consumer name
for each concurrently running process.

## 17.1  Deployment prerequisites

For an existing estate, first complete the
[controlled activation procedure](12_audit.md#activation-on-an-existing-estate),
including the quiescent cutover, signed baseline, named ACL rotation, and first
checkpoint evidence. This runbook assumes that trust boundary already exists.

Before enabling the worker:

1. Configure the same Redis and storage credentials used by SuperTable. Direct
   Redis deployments may use the authoritative `SUPERTABLE_REDIS_URL`;
   Sentinel deployments use the split `SUPERTABLE_REDIS_SENTINELS`, master,
   ACL username, and password variables. In Sentinel mode,
   `SUPERTABLE_REDIS_SSL=true` secures both discovery and resolved-master
   connections.
2. Run Redis in the supported standalone or Sentinel topology. The privileged Lua
   boundary does not currently support Redis Cluster cross-slot execution.
3. Enable Redis AOF (`appendonly yes`) with `appendfsync always` for the
   strict worker default and use `maxmemory-policy noeviction`. If the
   deployment deliberately accepts the Redis-documented `everysec` loss
   window, record that RPO decision and pass `--allow-everysec` explicitly.
4. Provision sufficient Redis capacity for the maximum archive outage and
   monitor stream length and pending age.
5. Configure archive storage with externally managed WORM/Object Lock,
   retention, backup, and independently controlled credentials. The worker's
   readback checks do not replace these deployment controls.
6. Restrict direct writes to RBAC/auth-token state, the privileged stream,
   delivery ledger, and archive paths. Applications must mutate privileged
   state through the catalog Lua boundaries.

The optional strict preflight verifies the Redis settings without changing
them:

```bash
supertable-privileged-audit-worker \
  --organization acme \
  --consumer audit-worker-01 \
  --require-durable-redis \
  --min-replicas-to-write 1 \
  --min-connected-replicas 1 \
  --health-check
```

Strict mode requires `appendfsync always` unless `--allow-everysec` explicitly
opts into the weaker persistence policy. It also verifies that the privileged
stream is a Redis Stream (or is
not created yet) and that the delivery ledger is a Hash (or is not created
yet). It requires `CONFIG GET`; a connected-replica threshold also requires
`INFO replication`. If a managed Redis service denies either command, strict
mode exits with configuration code 2 and names the missing permission. Omitting
the flag does not attempt `CONFIG GET`. Check these controls through the
provider control plane when the worker identity cannot receive CONFIG access.
This preflight is only a deployment check, not a substitute for configuring
and testing AOF, replicas, failover, and backups.

For the first archive-storage validation, keep normal traffic closed and first
perform one approved, controlled privileged mutation with an audited writer.
Then run one bounded archive cycle without `--trim`, followed by a full chain
verification:

```bash
supertable-privileged-audit-worker \
  --organization acme \
  --consumer audit-cutover \
  --once \
  --require-durable-redis

supertable-privileged-audit-worker \
  --organization acme \
  --consumer audit-cutover-verifier \
  --verify-chain \
  --verify-max-batches 10000 \
  --require-durable-redis
```

The missing `--trim` is intentional. Confirm the checkpoint and Parquet
artifacts, and independently verify WORM/Object-Lock and retention through the
storage provider control plane, before enabling normal traffic or trimming.
An empty `--once` run does not resolve or write archive storage and therefore
does not validate its credentials or write policy.

## 17.2  Modes and safety controls

Continuous mode is the default. Important options are:

| Option | Purpose |
|---|---|
| `--organization` | Required organization whose privileged stream is drained. |
| `--consumer` | Required stable consumer identity for the Redis group. |
| `--group` | Consumer group; defaults to `__privileged_archival__`. |
| `--count` | Entries per bounded drain, from 1 through 1,000. |
| `--reclaim-idle-ms` | Minimum idle time before abandoned pending work is reclaimed. |
| `--poll-seconds` | Idle polling interval. |
| `--heartbeat-seconds` | Interval for Redis health and worker-counter logs. |
| `--once` | Complete one bounded drain/optional-trim unit and exit 0. |
| `--health-check` | Check Redis/outbox health and exit without draining or trimming. |
| `--verify-chain` | Read and verify every immutable checkpoint and archive artifact from head through genesis, then exit. |
| `--verify-max-batches` | Positive full-chain traversal bound; defaults to 10,000 and cannot exceed 1,000,000. |
| `--trim` | Opt in to verified Redis source trimming after a successful drain. |
| `--trim-max-entries` | Hard bound (1–1000) passed to the conservative trim operation. |
| `--max-retries` | Consecutive backend/pending retries before exiting nonzero. |
| `--retry-initial-seconds`, `--retry-max-seconds`, `--retry-jitter` | Bounded exponential retry policy. |
| `--require-durable-redis` | Require AOF, `appendfsync always`, `noeviction`, and healthy key types at startup. |
| `--allow-everysec` | Explicitly weaken strict preflight to accept the `everysec` persistence window. |
| `--min-replicas-to-write`, `--min-connected-replicas` | Optional replica thresholds in strict mode. |

Trimming is deliberately disabled by default. `--trim` is appropriate only
after immutable archive retention has been configured and tested. The worker
derives a trim watermark only from a verified result returned by `drain_once()`
and calls `trim_delivered()`; it never deletes or acknowledges Redis entries
directly. A failed trim retains that watermark in memory and retries it before
draining more work.

`--once` is suitable for a CronJob or a supervised oneshot unit. It may archive
and acknowledge one unit when an event is available; an empty result does not
exercise archive storage. Use the read-only
`--health-check` mode for Kubernetes exec probes and systemd startup checks;
pass `--max-retries 0` when the supervisor already enforces the probe timeout
and retry policy.
Every startup and heartbeat verifies the current Redis archive-head pointer,
its immutable checkpoint, and the referenced Parquet artifact before another
drain begins. A missing head is empty only when Redis confirms that the
delivery ledger has no fields. The single exception is an exact, bounded
sequence-one batch in `writing` or `written_verified` state with its matching
claim and no delivery markers; the active worker reports it as pending and
resumes that immutable batch. Any orphan, extra field, delivered record, or
inconsistent artifact/checkpoint claim without a head is an integrity failure.

If a selected Parquet payload would exceed the 256 MiB artifact ceiling, or
its role-delete sidecars would exceed the 100,000-row aggregate in-memory
bound, the outbox rejects it before creating an immutable Redis sequence
claim. Reduce `--count` and restart after the existing PEL entries reach
`--reclaim-idle-ms`; this repartitions pending work without discarding or
acknowledging evidence.

Run the deeper read-only verification manually or on a scheduled control:

```bash
supertable-privileged-audit-worker \
  --organization acme \
  --consumer audit-chain-verifier \
  --verify-chain
```

`--verify-chain` is mutually exclusive with `--once` and `--health-check`,
rejects `--trim`, and never drains or acknowledges an entry. It walks every
immutable manifest back to sequence 1 and revalidates every referenced parent
and cascade Parquet artifact. This is intentionally much more expensive than
the bounded latest-head check used by routine heartbeats. The outbox applies a
10,000-batch default safety bound so a cyclic or damaged chain cannot create an
unbounded verifier process. For a valid longer-lived chain, set
`--verify-max-batches` to a sufficient positive bound, up to the hard maximum
of 1,000,000 batches.

## 17.3  systemd example

```ini
[Unit]
Description=SuperTable privileged audit archiver (acme)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/supertable/acme.env
ExecStartPre=/opt/supertable/bin/supertable-privileged-audit-worker --organization acme --consumer audit-worker-01 --health-check --max-retries 0 --require-durable-redis
ExecStart=/opt/supertable/bin/supertable-privileged-audit-worker --organization acme --consumer audit-worker-01 --count 500 --reclaim-idle-ms 300000 --heartbeat-seconds 60 --require-durable-redis
Restart=on-failure
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=120
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Set `TimeoutStopSec` above the maximum Redis/storage operation time. SIGTERM
and SIGINT request cooperative shutdown: the active outbox operation is allowed
to finish, then the worker exits before starting another unit.

## 17.4  Kubernetes example

Use a Deployment with one replica per organization, or intentionally assign a
different consumer name to each replica:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: supertable-privileged-audit-acme
spec:
  replicas: 1
  selector:
    matchLabels:
      app: supertable-privileged-audit-acme
  template:
    metadata:
      labels:
        app: supertable-privileged-audit-acme
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: privileged-audit-worker
          image: example/supertable:2.4.1
          command: ["supertable-privileged-audit-worker"]
          args:
            - --organization
            - acme
            - --consumer
            - audit-worker-01
            - --count
            - "500"
            - --require-durable-redis
          envFrom:
            - secretRef:
                name: supertable-acme
          livenessProbe:
            exec:
              command:
                - supertable-privileged-audit-worker
                - --organization
                - acme
                - --consumer
                - audit-worker-probe
                - --health-check
                - --max-retries
                - "0"
            periodSeconds: 30
            timeoutSeconds: 10
```

Add `--trim` only after the retention prerequisites above are satisfied.

## 17.5  Failure policy and alerts

| Exit | Meaning | Operator action |
|---:|---|---|
| 0 | Clean signal, successful health check, or completed oneshot unit. | None. |
| 2 | Invalid CLI/runtime configuration or failed strict Redis preflight. | Correct configuration or ACLs; do not restart-loop indefinitely. |
| 3 | Archive, checkpoint-chain, stream, or delivery-ledger integrity failure. | Stop automated trimming, preserve Redis/storage evidence, and investigate. |
| 4 | Backend or delivery-pending retry budget exhausted. | Check Redis/storage availability, latency, PEL ownership, and capacity. |
| 5 | Unexpected worker defect. | Capture traceback and escalate. |

Backend and delivery-pending errors are logged with their traceback and retried
with bounded exponential jitter. Archive-verification and malformed-record
errors are never placed into that retry loop: they are logged as critical and
terminate with exit 3. A supervisor may restart exit 4 after its own delay, but
should alert immediately on exits 2, 3, or 5.

Alert on at least:

- nonzero worker exits and restart frequency;
- privileged stream length and oldest pending-entry age;
- heartbeat absence or increasing `transient_failures`;
- Redis memory, AOF status, replica health, and persistence errors;
- archive storage write/read latency and integrity failures;
- delivery-versus-trim lag when `--trim` is enabled.

Heartbeat logs contain the organization, consumer, verified checkpoint head,
stream length, group count, completed cycles, archived/acknowledged counts,
trim count, and transient failure count. They intentionally contain no role
policy, row-filter literal, token, or user document.
