# route: supertable.monitoring
"""
Monitoring orchestration primitives.

The writer side (``supertable.monitoring_writer``) pushes JSON payloads
into daily-partitioned Redis LISTs:

    supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}

This package exposes the **drain side** as pure functions that an
external service (yours) calls from its own scheduling. The SDK does
not spawn background threads or run loops on its own; it just answers
"what is drainable?", "give me a verified claim", and "acknowledge this
claim only after my sink commit".

Typical drain orchestration::

    from supertable.monitoring import (
        acknowledge_partition,
        claim_partition_chunks,
        iter_claimed_partition_chunks,
        list_drainable_partitions,
    )

    for part in list_drainable_partitions(catalog, organization="acme"):
        claim = claim_partition_chunks(
            catalog,
            organization=part.organization,
            monitor_type=part.monitor_type,
            date=part.date,
        )
        if claim is None:
            continue
        for entries in iter_claimed_partition_chunks(catalog, claim):
            # Commit idempotently to __reads__/__writes__/__mcp__.
            durable_sink_write(entries)
        acknowledge_partition(
            catalog,
            organization=claim.organization,
            monitor_type=claim.monitor_type,
            date=claim.date,
            receipt=claim.receipt,
        )

Read the live tail (e.g. for a "recent N" UI)::

    from supertable.monitoring import read_recent

    last_100 = read_recent(
        catalog, organization="acme", monitor_type="writes", limit=100,
    )
    # newest first, parsed dicts, never mutates Redis state
"""

from supertable.monitoring.partitions import (
    MONITORING_SINK_TABLE_FOR,
    MONITORING_SINK_TABLES,
    MonitorChunkClaim,
    MonitorDrainClaim,
    MonitorPartition,
    MonitoringPartitionError,
    acknowledge_partition,
    claim_partition_chunks,
    claim_partition,
    drain_partition,
    iter_partition_chunks,
    iter_claimed_partition_chunks,
    list_drainable_partitions,
    read_recent,
)

__all__ = [
    "MONITORING_SINK_TABLE_FOR",
    "MONITORING_SINK_TABLES",
    "MonitorChunkClaim",
    "MonitorDrainClaim",
    "MonitorPartition",
    "MonitoringPartitionError",
    "acknowledge_partition",
    "claim_partition_chunks",
    "claim_partition",
    "drain_partition",
    "iter_partition_chunks",
    "iter_claimed_partition_chunks",
    "list_drainable_partitions",
    "read_recent",
]
