import json
import logging
from datetime import datetime
from typing import Optional

from supertable.query_plan_manager import QueryPlanManager
from supertable.plan_stats import PlanStats
from supertable.storage.storage_factory import get_storage
from supertable.super_table import SuperTable
from supertable.monitoring_logger import MonitoringLogger


def extend_execution_plan(
    super_table: SuperTable,
    query_plan_manager: QueryPlanManager,
    user_hash: str,
    timing: dict,
    plan_stats: PlanStats,
    status: str,
    message: str,
    result_shape: tuple[int, int]
) -> None:
    """
    Read an existing execution plan JSON, extend it with timing and profiling info,
    then log a single metric record via MonitoringLogger and delete the original plan.
    """

    # Lazily acquire a storage interface if not provided
    storage = get_storage()

    # Attempt to load the existing JSON plan; fall back to empty dict on error
    try:
        base_plan = storage.read_json(query_plan_manager.query_plan_path)
        logging.debug("Loaded existing execution plan.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"Could not read plan at {query_plan_manager.query_plan_path}: {e}")
        base_plan = {}

    # Build the extended plan payload
    extended_plan = {
        "execution_timings": timing,
        "profile_overview": plan_stats.stats,
        "query_profile": base_plan,
    }

    # Prepare the flat stats dictionary to send to MonitoringLogger
    stats = {
        "query_id":       query_plan_manager.query_id,
        "query_hash":     query_plan_manager.query_hash,
        "user_hash":      user_hash,
        "recorded_at":    datetime.utcnow().isoformat(),
        "table_name":     query_plan_manager.original_table,
        "status":         status,
        "message":        message,
        "result_rows":    result_shape[0],
        "result_columns": result_shape[1],
        # JSON‐serialize the nested objects
        "execution_timings": json.dumps(extended_plan["execution_timings"]),
        "profile_overview":  json.dumps(extended_plan["profile_overview"]),
        "query_profile":     json.dumps(extended_plan["query_profile"]),
    }

    # Instantiate and use MonitoringLogger within the function
    with MonitoringLogger(
        super_name=super_table.super_name,
        organization=super_table.organization,
        monitor_type="plans",
    ) as monitor:
        # Log the metric (this will handle buffering, flushing, etc.)
        monitor.log_metric(stats)
        logging.debug("Logged extended execution plan metrics.")

    # Clean up the original plan file once logged
    try:
        storage.delete(query_plan_manager.query_plan_path)
        logging.debug(f"Deleted original plan JSON: {query_plan_manager.query_plan_path}")
    except Exception as e:
        logging.warning(f"Failed to delete plan file: {e}")
