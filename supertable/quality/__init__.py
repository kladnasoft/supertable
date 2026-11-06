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
  - ``start_scheduler`` — consumer: starts the background daemon thread that
    drains pending flags and runs checks.  Call once at host startup.
  - ``run_check`` — runs one check now, under the same lock and cooldown as
    the scheduler, and returns a ``CheckRunOutcome``: success, failed or
    skipped.
"""
from __future__ import annotations

from supertable.quality.scheduler import (
    CheckRunOutcome,
    notify_ingest,
    run_check,
    start_scheduler,
)

__all__ = ["CheckRunOutcome", "notify_ingest", "run_check", "start_scheduler"]
