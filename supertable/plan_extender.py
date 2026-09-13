import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Tuple

from supertable.query_plan_manager import QueryPlanManager
from supertable.engine.plan_stats import PlanStats
from supertable.monitoring.partitions import MONITORING_SINK_TABLES
from supertable.monitoring_writer import MonitoringWriter

logger = logging.getLogger(__name__)


#: Comfortably larger than any hand-written query while staying an order of
#: magnitude below a typical 512 KB per-entry monitoring budget.  The ceiling
#: exists so one generated statement cannot push a row past the reader-side
#: per-entry limit, which drops the *whole* record rather than just the SQL.
_DEFAULT_MONITOR_SQL_MAX_CHARS = 64_000


def _monitor_sql_max_chars() -> int:
    """Character ceiling for the ``sql`` field of a ``plans`` monitoring row.

    ``0`` or a negative value disables truncation entirely.
    """
    raw = str(os.environ.get("SUPERTABLE_MONITOR_SQL_MAX_CHARS", "")).strip()
    try:
        return int(raw) if raw else _DEFAULT_MONITOR_SQL_MAX_CHARS
    except (TypeError, ValueError):
        return _DEFAULT_MONITOR_SQL_MAX_CHARS


def _monitor_sql_raw_allowed(organization: str) -> bool:
    """Whether *organization* may store un-redacted SQL in monitoring.

    Monitoring rows live in a plain Redis partition with a 7-day TTL and are
    **not** passed through the audit encryption helper, so a literal in a
    ``WHERE`` clause is a literal on disk.  Storing the query shape instead is
    therefore the default, and recording the real statement is something an
    operator turns on for a named organization that has accepted it.

    ``SUPERTABLE_MONITOR_SQL_RAW`` takes a comma-separated list of
    organizations, or ``1``/``true``/``all`` to allow every one.  Unset (the
    default) allows none.
    """
    raw = str(os.environ.get("SUPERTABLE_MONITOR_SQL_RAW", "")).strip()
    if not raw:
        return False
    if raw.lower() in {"1", "true", "yes", "all", "*"}:
        return True
    return organization in {item.strip() for item in raw.split(",") if item.strip()}


def _sql_shape(text: str) -> str:
    """Return the SQL structure with every literal replaced by a placeholder.

    Falls back to the empty string rather than the raw statement: a shape that
    cannot be parsed must not silently degrade into the un-redacted query it
    was supposed to replace.
    """
    try:
        import sqlglot
        from sqlglot import exp

        def erase(node):
            if isinstance(node, (exp.Literal, exp.Boolean, exp.Null)):
                return exp.Placeholder()
            return node

        root = sqlglot.parse_one(text, read="duckdb")
        return root.transform(erase, copy=True).sql(dialect="duckdb", pretty=False)
    except Exception:  # noqa: BLE001 - any parse failure redacts completely
        return ""


def _monitor_sql(query_plan_manager: QueryPlanManager) -> str:
    """Return the SQL to record on the monitoring row, bounded and redacted.

    Redaction is decided per organization by :func:`_monitor_sql_raw_allowed`;
    the length ceiling applies either way.
    """
    sql = getattr(query_plan_manager, "query", "") or ""
    if not sql:
        return ""
    organization = getattr(query_plan_manager, "organization", "") or ""
    if not _monitor_sql_raw_allowed(organization):
        sql = _sql_shape(sql)
    limit = _monitor_sql_max_chars()
    if limit > 0 and len(sql) > limit:
        return sql[:limit]
    return sql


def _query_targets_sink_table(original_table: str) -> bool:
    """True if any of the comma-joined targets in ``original_table``
    is a monitoring sink table.

    ``original_table`` is built in ``data_reader.execute()`` as
    ``", ".join(t.simple_name for t in physical_tables)``. We split it
    back and check each name against :data:`MONITORING_SINK_TABLES`.
    Defensive against whitespace and empty strings.
    """
    if not original_table:
        return False
    for name in original_table.split(","):
        if name.strip() in MONITORING_SINK_TABLES:
            return True
    return False


def _safe_json(obj: Any) -> str:
    """
    JSON-dump helper that never raises (keeps monitoring path robust).
    Falls back to string representation on failure.
    """
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:  # noqa: BLE001
        try:
            return json.dumps(str(obj), ensure_ascii=False)
        except Exception:  # noqa: BLE001
            return "{}"


def _read_local_json(path: str) -> Dict[str, Any]:
    """
    Read a JSON file from the local filesystem.

    DuckDB writes its profile JSON to a local disk path (via PRAGMA
    profile_output).  This helper reads it back using stdlib I/O,
    avoiding the remote-storage backend which operates on object-store
    keys (S3, MinIO) and would never find a local temp file.
    """
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def extend_execution_plan(
    query_plan_manager: QueryPlanManager,
    role_name: str,
    timing: Dict[str, float] | None,
    plan_stats: PlanStats,
    status: str,
    message: str | None,
    result_shape: Tuple[int, int] | None,
) -> None:
    """
    Extend the DuckDB profile JSON with app timings & stats,
    log a single metric through MonitoringLogger, then delete the raw plan.

    Robustness goals:
    - Never raise from this function (monitoring must not break reads).
    - Handle missing/invalid JSON profile gracefully.
    - Avoid gigantic payloads by JSON-encoding nested parts into strings.

    Note: the plan JSON is read from the *local filesystem* (where DuckDB
    wrote it via PRAGMA profile_output), not from the remote storage backend.
    """

    # Load the raw plan from local disk, if present
    base_plan: Dict[str, Any] = {}
    plan_path = getattr(query_plan_manager, "query_plan_path", None) if query_plan_manager else None
    try:
        if plan_path and os.path.isfile(plan_path):
            base_plan = _read_local_json(plan_path)
        elif plan_path:
            logger.debug("Plan JSON does not exist at %s", plan_path)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not read plan JSON (%s): %s", plan_path or "?", e)
        base_plan = {}

    # Normalize inputs
    timing = timing or {}
    message = message or ""
    result_shape = result_shape or (0, 0)

    # Stash the parsed plan onto the query_plan_manager so upstream callers
    # (e.g. execute.py API) can include it in the response without re-reading
    # the file (which is deleted below).
    if query_plan_manager is not None:
        query_plan_manager.query_profile = base_plan

    # Build extended (in-memory) representation
    extended_plan = {
        "execution_timings": timing,
        "profile_overview": plan_stats.summary() if hasattr(plan_stats, "summary") else plan_stats.stats,
        "query_profile": base_plan,
    }

    # Prepare flat metric payload for the monitoring table
    try:
        # Extract engine from plan_stats (stored as {"ENGINE": "duckdb_lite"} entry)
        _engine_used = "unknown"
        for _entry in (plan_stats.stats if hasattr(plan_stats, "stats") else []):
            if isinstance(_entry, dict) and "ENGINE" in _entry:
                _engine_used = str(_entry["ENGINE"])
                break

        stats = {
            "query_id": getattr(query_plan_manager, "query_id", ""),
            "query_hash": getattr(query_plan_manager, "query_hash", ""),
            "organization": getattr(query_plan_manager, "organization", ""),
            "super_name": getattr(query_plan_manager, "super_name", ""),
            "role_name": role_name,
            "source_type": getattr(query_plan_manager, "source_type", "api"),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "table_name": getattr(query_plan_manager, "original_table", ""),
            "sql": _monitor_sql(query_plan_manager),
            "engine": _engine_used,
            "status": status,
            "message": message,
            "result_rows": int(result_shape[0]),
            "result_columns": int(result_shape[1]),
            # Store complex parts as JSON strings to keep row schema flat
            "execution_timings": _safe_json(extended_plan["execution_timings"]),
            "profile_overview": _safe_json(extended_plan["profile_overview"]),
            "query_profile": _safe_json(extended_plan["query_profile"]),
        }
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to build monitoring stats payload: %s", e)
        return  # nothing else to do safely

    # Log the metric (buffered; background writer flushes).
    # Monitoring is org-wide as of SDK 2.2.0 — the touched supertable
    # is recorded in the payload's ``supertables: [str]`` field.
    #
    # Loop guard: SELECTs that target a monitoring sink table
    # (``__writes__``/``__reads__``/``__mcp__``/``__plans__``) skip
    # the plans-metric emission. The orchestrator analysing the sink
    # tables would otherwise generate fresh ``plans`` partitions for
    # tomorrow's flush, leading to slow amplification.
    if _query_targets_sink_table(stats.get("table_name", "")):
        logger.debug("Skipping plans metric for sink-table query")
    else:
        try:
            stats["supertables"] = [query_plan_manager.super_name]
            with MonitoringWriter(
                organization=query_plan_manager.organization,
                monitor_type="plans",
            ) as monitor:
                monitor.log_metric(stats)
                logger.debug("Extended plan metrics queued for logging.")
        except Exception as e:  # noqa: BLE001
            logger.warning("Monitoring logging failed (non-fatal): %s", e)

    # Delete the raw plan JSON from local disk (best-effort)
    try:
        if plan_path and os.path.isfile(plan_path):
            os.remove(plan_path)
            logger.debug("Deleted plan JSON: %s", plan_path)
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to delete plan JSON (non-fatal): %s", e)
