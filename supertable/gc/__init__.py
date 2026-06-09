# route: supertable.gc
"""
Deferred-deletion garbage collection for SuperTable storage.

The data writer never deletes files inline. When the GC flags are enabled
(``SUPERTABLE_SUNSET_GC_ENABLED`` / ``SUPERTABLE_SNAPSHOT_RETENTION``) the
writer XADDs paths to a per-table Redis STREAM after a successful leaf-CAS.
A long-running ``GCCleaner`` daemon (one per org) drains entries older than
``SUPERTABLE_GC_DELAY_SEC`` and calls ``storage.delete()`` on each path.

The delay window is what makes deletion safe under concurrent reads: a
query that resolved the leaf payload right before the writer committed
finishes long before the cleaner touches the file.
"""

from supertable.gc.queue import (
    collect_old_snapshot_paths,
    enqueue_deletions,
    nuke_stream,
)

__all__ = [
    "collect_old_snapshot_paths",
    "enqueue_deletions",
    "nuke_stream",
]
