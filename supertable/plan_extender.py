import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Tuple

from supertable.query_plan_manager import QueryPlanManager
from supertable.plan_stats import PlanStats
from supertable.storage.storage_factory import get_storage
from supertable.super_table import SuperTable
from supertable.monitoring_logger import MonitoringLogger

logger = logging.getLogger(__name__)


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


def extend_execution_plan(
    super_table: SuperTable,
    query_plan_manager: QueryPlanManager,
    user_hash: str,
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
    """
    storage = get_storage()

    # Load the raw plan, if present
    base_plan: Dict[str, Any] = {}
    try:
        if query_plan_manager and query_plan_manager.query_plan_path:
            if storage.exists(query_plan_manager.query_plan_path):
                base_plan = storage.read_json(query_plan_manager.query_plan_path)
            else:
                logger.debug("Plan JSON does not exist at %s", query_plan_manager.query_plan_path)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not read plan JSON (%s): %s", getattr(query_plan_manager, "query_plan_path", "?"), e)
        base_plan = {}

    # Normalize inputs
    timing = timing or {}
    message = message or ""
    result_shape = result_shape or (0, 0)

    # Build extended (in-memory) representation
    extended_plan = {
        "execution_timings": timing,
        "profile_overview": plan_stats.summary() if hasattr(plan_stats, "summary") else plan_stats.stats,
        "query_profile": base_plan,
    }

    # Prepare flat metric payload for the monitoring table
    try:
        stats = {
            "query_id": getattr(query_plan_manager, "query_id", ""),
            "query_hash": getattr(query_plan_manager, "query_hash", ""),
            "user_hash": user_hash,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "table_name": getattr(query_plan_manager, "original_table", ""),
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

    # Log the metric (buffered; background writer flushes)
    try:
        with MonitoringLogger(
            super_name=super_table.super_name,
            organization=super_table.organization,
            monitor_type="plans",
        ) as monitor:
            monitor.log_metric(stats)
            logger.debug("Extended plan metrics queued for logging.")
    except Exception as e:  # noqa: BLE001
        logger.warning("Monitoring logging failed (non-fatal): %s", e)

    # Delete the raw plan JSON (best-effort)
    try:
        if query_plan_manager and query_plan_manager.query_plan_path:
            if storage.exists(query_plan_manager.query_plan_path):
                storage.delete(query_plan_manager.query_plan_path)
                logger.debug("Deleted plan JSON: %s", query_plan_manager.query_plan_path)
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to delete plan JSON (non-fatal): %s", e)
