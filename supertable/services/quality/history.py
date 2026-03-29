# route: supertable.services.quality.history
"""
History writer for Data Quality check results.

After each quality check, appends a row to the __data_quality__ system table.
This table is stored as Parquet (standard SuperTable storage) and is queryable
via MCP SQL, enabling AgenticBI to chart quality trends, alert on regressions,
and provide natural-language summaries of data health over time.

Table schema:
  dq_id             VARCHAR   — UUID per check run
  checked_at        TIMESTAMP — UTC timestamp of check
  table_name        VARCHAR   — checked table name
  check_type        VARCHAR   — 'quick' or 'deep'
  quality_score     INTEGER   — 0-100 composite score
  status            VARCHAR   — 'ok', 'warning', 'critical'
  row_count         BIGINT    — total rows in the checked table
  total_checks      INTEGER   — number of checks evaluated
  passed            INTEGER   — checks that passed
  warnings          INTEGER   — warning count
  critical_count    INTEGER   — critical count
  anomaly_count     INTEGER   — number of anomalies detected
  anomalies_json    VARCHAR   — JSON array of anomaly details
  column_stats_json VARCHAR   — JSON summary of per-column metrics
  rule_results_json VARCHAR   — JSON array of custom rule results
  execution_ms      BIGINT    — check duration in milliseconds

Write strategy:
  1. Build a single-row Polars DataFrame from the check result
  2. Convert to Arrow table for DataWriter (standard SuperTable ingest → Parquet on MinIO/local)
  3. Never fail the check execution — all errors are logged and swallowed
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

HISTORY_TABLE = "__data_quality__"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_history_row(
    table_name: str,
    check_type: str,
    latest: Dict[str, Any],
    execution_ms: int = 0,
) -> Dict[str, Any]:
    """
    Flatten a check result (the 'latest' dict stored in Redis) into a single
    row suitable for insertion into __data_quality__.

    This is a pure function — no I/O, no side effects.
    """
    anomalies = latest.get("anomalies", [])
    parsed = latest.get("parsed", {})
    rule_results = latest.get("rule_results", [])

    # Build a compact column stats summary (strip heavy per-column data)
    col_stats = {}
    for col_name, col_data in parsed.get("columns", {}).items():
        col_stats[col_name] = {
            "type": col_data.get("column_type"),
            "category": col_data.get("category"),
            "null_rate": col_data.get("null_rate"),
            "distinct": col_data.get("distinct"),
            "uniqueness": col_data.get("uniqueness"),
        }
        # Include numeric stats if present
        if col_data.get("category") == "numeric":
            col_stats[col_name]["avg"] = col_data.get("avg")
            col_stats[col_name]["stddev"] = col_data.get("stddev")
            col_stats[col_name]["min"] = col_data.get("min")
            col_stats[col_name]["max"] = col_data.get("max")

    return {
        "dq_id": str(uuid.uuid4()),
        "checked_at": latest.get("checked_at", _now_iso()),
        "table_name": table_name,
        "check_type": check_type,
        "quality_score": latest.get("quality_score", 0),
        "status": latest.get("status", "ok"),
        "row_count": parsed.get("total", latest.get("row_count", 0)),
        "total_checks": latest.get("total_checks", 0),
        "passed": latest.get("passed", 0),
        "warnings": latest.get("warnings", 0),
        "critical_count": latest.get("critical", 0),
        "anomaly_count": len(anomalies),
        "anomalies_json": json.dumps(anomalies, default=str),
        "column_stats_json": json.dumps(col_stats, default=str),
        "rule_results_json": json.dumps(rule_results, default=str),
        "execution_ms": execution_ms,
    }


def write_history(
    org: str,
    sup: str,
    table_name: str,
    check_type: str,
    latest: Dict[str, Any],
    execution_ms: int = 0,
) -> bool:
    """
    Append a check result row to the __data_quality__ system table.

    Uses the standard SuperTable DataWriter ingest path so the data ends up
    as Parquet files in the configured storage backend (MinIO/local).

    Returns True on success, False on failure. Never raises — errors are
    logged but swallowed to avoid breaking the check execution flow.
    """
    try:
        import polars as pl
        from supertable.data_writer import DataWriter
    except ImportError as e:
        logger.debug(f"[dq-history] DataWriter/polars not available, skipping history write: {e}")
        return False

    try:
        row = build_history_row(table_name, check_type, latest, execution_ms)

        # Build Polars DataFrame with explicit schema for type safety
        df = pl.DataFrame({
            "dq_id":             [str(row["dq_id"])],
            "checked_at":        [str(row["checked_at"])],
            "table_name":        [str(row["table_name"])],
            "check_type":        [str(row["check_type"])],
            "quality_score":     [int(row["quality_score"] or 0)],
            "status":            [str(row["status"])],
            "row_count":         [int(row["row_count"] or 0)],
            "total_checks":      [int(row["total_checks"] or 0)],
            "passed":            [int(row["passed"] or 0)],
            "warnings":          [int(row["warnings"] or 0)],
            "critical_count":    [int(row["critical_count"] or 0)],
            "anomaly_count":     [int(row["anomaly_count"] or 0)],
            "anomalies_json":    [str(row["anomalies_json"])],
            "column_stats_json": [str(row["column_stats_json"])],
            "rule_results_json": [str(row["rule_results_json"])],
            "execution_ms":      [int(row["execution_ms"] or 0)],
        }, schema={
            "dq_id": pl.Utf8, "checked_at": pl.Utf8, "table_name": pl.Utf8,
            "check_type": pl.Utf8, "quality_score": pl.Int64, "status": pl.Utf8,
            "row_count": pl.Int64, "total_checks": pl.Int64, "passed": pl.Int64,
            "warnings": pl.Int64, "critical_count": pl.Int64,
            "anomaly_count": pl.Int64, "anomalies_json": pl.Utf8,
            "column_stats_json": pl.Utf8, "rule_results_json": pl.Utf8,
            "execution_ms": pl.Int64,
        })

        # Convert to Arrow table (DataWriter calls polars.from_arrow internally)
        arrow_table = df.to_arrow()

        writer = DataWriter(super_name=sup, organization=org)
        writer.write(
            role_name="superadmin",
            simple_name=HISTORY_TABLE,
            data=arrow_table,
            overwrite_columns=[],  # append-only, no dedup
            lineage={
                "source_type": "dq_check",
                "source_id": row["dq_id"],
                "source_tables": [table_name],
                "tags": {"check_type": check_type},
            },
        )

        logger.debug(
            f"[dq-history] Wrote history row: {table_name} "
            f"({check_type}, score={row['quality_score']}, "
            f"anomalies={row['anomaly_count']})"
        )
        return True

    except Exception as e:
        logger.warning(f"[dq-history] Failed to write history for {table_name}: {e}")
        return False


def write_history_via_sql(
    org: str,
    sup: str,
    table_name: str,
    check_type: str,
    latest: Dict[str, Any],
    execution_ms: int = 0,
) -> bool:
    """
    Fallback: store history row as a JSON entry in a Redis list.

    Used when DataWriter is not available or fails. Stores the last
    1000 check results per org:sup in a capped Redis list so history
    is available even without Parquet writes. AgenticBI can still
    query this via MCP tools.

    Key: supertable:{org}:{sup}:dq:history  (Redis LIST, newest first)

    Returns True on success, False on failure. Never raises.
    """
    try:
        from supertable.services.quality.config import _dq_key
        import redis as redis_mod
    except ImportError:
        logger.debug("[dq-history] Redis fallback not available")
        return False

    try:
        from supertable.redis_connector import create_redis_client
        r = create_redis_client()
    except Exception:
        logger.debug("[dq-history] Cannot connect to Redis for history fallback")
        return False

    try:
        row = build_history_row(table_name, check_type, latest, execution_ms)
        key = _dq_key(org, sup, "history")

        # Push to head of list (newest first), cap at 1000 entries
        r.lpush(key, json.dumps(row, default=str))
        r.ltrim(key, 0, 999)

        logger.debug(f"[dq-history] Wrote history row to Redis fallback: {table_name}")
        return True

    except Exception as e:
        logger.warning(f"[dq-history] Redis history fallback failed for {table_name}: {e}")
        return False
