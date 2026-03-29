# supertable/reflection/quality/__init__.py
"""
Data Quality module for SuperTable Reflection UI.

Provides:
  - Built-in quality checks (quick + deep profile)
  - User-defined custom rules
  - Scheduled background execution
  - Anomaly detection (A1-A5)
  - Redis-backed configuration + latest results
  - Parquet-backed history (__data_quality__ table)
"""
from __future__ import annotations
