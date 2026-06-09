# route: supertable.monitoring
"""
Monitoring orchestration primitives.

The writer side (``supertable.monitoring_writer``) pushes JSON payloads
into daily-partitioned Redis LISTs:

    supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}

This package exposes the **drain side** as pure functions that an
external service (yours) calls from its own scheduling. The SDK does
not spawn background threads or run loops on its own; it just answers
"what is drainable?" and "give me this partition's contents (and
delete it)".

Typical drain orchestration::

    from supertable.monitoring import (
        list_drainable_partitions, drain_partition,
    )

    for part in list_drainable_partitions(catalog, organization="acme"):
        entries = drain_partition(
            catalog,
            organization=part.organization,
            monitor_type=part.monitor_type,
            date=part.date,
        )
        # entries is a list[dict] — write to your internal sink table:
        #   plans  -> __reads__
        #   writes -> __writes__
        #   mcp    -> __mcp__

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
    MonitorPartition,
    drain_partition,
    iter_partition_chunks,
    list_drainable_partitions,
    read_recent,
)

__all__ = [
    "MONITORING_SINK_TABLE_FOR",
    "MONITORING_SINK_TABLES",
    "MonitorPartition",
    "drain_partition",
    "iter_partition_chunks",
    "list_drainable_partitions",
    "read_recent",
]
