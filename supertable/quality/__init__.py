# supertable/quality/__init__.py
"""
Data Quality module for SuperTable Reflection UI.

Provides:
  - Built-in quality checks (quick + deep profile)
  - User-defined custom rules
  - Scheduled background execution
  - Anomaly detection (A1-A5)
  - Redis-backed configuration + latest results
  - Parquet-backed history (__data_quality__ table)

Operational entry points:
  - ``notify_ingest``  — producer: called by the write path after data lands;
    sets a debounced Redis "pending" flag (never blocks or fails the write).
  - ``start_scheduler`` — consumer: starts the bounded background worker pool.
  - ``stop_scheduler`` — stops the coordinator and terminates bounded child
    checks that do not cooperate with cancellation.
  - ``scheduler_health`` — operational counters and liveness snapshot.
"""
from __future__ import annotations

from supertable.quality.scheduler import (
    notify_ingest,
    scheduler_health,
    start_scheduler,
    stop_scheduler,
)

__all__ = [
    "notify_ingest",
    "scheduler_health",
    "start_scheduler",
    "stop_scheduler",
]
