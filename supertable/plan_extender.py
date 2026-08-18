import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Tuple

from supertable.query_plan_manager import QueryPlanManager
from supertable.engine.plan_stats import PlanStats
from supertable.monitoring.partitions import MONITORING_SINK_TABLES
from supertable.monitoring_writer import MonitoringDurabilityError, MonitoringWriter
from supertable.engine.query_observations import (
    QueryObservation,
    QueryObservationStore,
    canonical_sql_shape,
    normalize_query_profile,
    redact_text,
    sanitize_profile,
)

logger = logging.getLogger(__name__)

_MAX_RAW_PROFILE_BYTES = 8 * 1024 * 1024


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
    size = os.path.getsize(path)
    if size > _MAX_RAW_PROFILE_BYTES:
        raise ValueError(
            f"profile JSON exceeds {_MAX_RAW_PROFILE_BYTES} byte safety cap"
        )
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
    - Redis outage remains non-fatal after the metric is fsynced to the bounded
      local spool. Spool durability/backpressure is deliberately raised so an
      enabled monitoring operation cannot be acknowledged with no record.
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
            try:
                os.chmod(plan_path, 0o600)
            except OSError:
                pass
            base_plan = _read_local_json(plan_path)
        elif plan_path:
            logger.debug("Plan JSON does not exist at %s", plan_path)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not read plan JSON (%s): %s", plan_path or "?", e)
        base_plan = {}

    # Normalize inputs
    timing = timing or {}
    message = message or ""
    # Preserve an unknown shape through normalization so a streaming engine's
    # measured output counters are not overwritten by a display-only `(0, 0)`.
    normalized_result_shape = result_shape
    display_result_shape = result_shape or (0, 0)

    # Build the normalized scalar view before sanitizing the diagnostic tree;
    # extractors need typed metrics such as DuckDB latency/bytes and IslandDB
    # elapsed/cache counters. Missing fields retain explicit provenance flags.
    normalized = normalize_query_profile(
        query=getattr(query_plan_manager, "query", ""),
        requested_engine=getattr(query_plan_manager, "requested_engine", ""),
        timing=timing,
        plan_stats=plan_stats,
        status=status,
        result_shape=normalized_result_shape,
        engine_profile=base_plan,
    )
    if normalized_result_shape is None:
        display_result_shape = (
            normalized.result_rows,
            normalized.result_columns,
        )
    safe_plan = sanitize_profile(base_plan)
    safe_overview = sanitize_profile(
        plan_stats.summary() if hasattr(plan_stats, "summary") else plan_stats.stats,
        max_bytes=32 * 1024,
    )
    safe_timing = sanitize_profile(timing, max_bytes=8 * 1024)

    # Stash only the credential-safe, bounded plan onto the manager so API
    # callers cannot accidentally return signed object-store URLs.
    # (e.g. execute.py API) can include it in the response without re-reading
    # the file (which is deleted below).
    if query_plan_manager is not None:
        query_plan_manager.query_profile = safe_plan

    # Build extended (in-memory) representation
    extended_plan = {
        "execution_timings": safe_timing,
        "profile_overview": safe_overview,
        "query_profile": safe_plan,
        "normalized_profile": normalized.as_dict(),
    }

    # Prepare flat metric payload for the monitoring table
    try:
        # Extract engine from plan_stats (stored as {"ENGINE": "duckdb"} entry)
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
            # Preserve workload structure for diagnostics, never literal data.
            "sql": canonical_sql_shape(
                getattr(query_plan_manager, "query", ""), limit=500,
            ),
            "query_shape_hash": normalized.query_shape_hash,
            "feature_signature": normalized.feature_signature,
            "requested_engine": normalized.requested_engine,
            "selected_engine": normalized.selected_engine,
            "engine": _engine_used,
            "forced_engine": normalized.forced,
            "engine_fallback": normalized.fallback,
            "status": status,
            "message": redact_text(message, limit=1_000),
            "result_rows": int(display_result_shape[0]),
            "result_columns": int(display_result_shape[1]),
            # Store complex parts as JSON strings to keep row schema flat
            "execution_timings": _safe_json(extended_plan["execution_timings"]),
            "profile_overview": _safe_json(extended_plan["profile_overview"]),
            "query_profile": _safe_json(extended_plan["query_profile"]),
            "normalized_profile": _safe_json(extended_plan["normalized_profile"]),
        }
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to build monitoring stats payload: %s", e)
        try:
            if plan_path and os.path.isfile(plan_path):
                os.remove(plan_path)
        except OSError:
            pass
        return  # nothing else to do safely

    # Durably enqueue the metric; Redis outage is absorbed by the bounded WAL.
    # Monitoring is org-wide as of SDK 2.2.0 — the touched supertable
    # is recorded in the payload's ``supertables: [str]`` field.
    #
    # Loop guard: SELECTs that target a monitoring sink table
    # (``__writes__``/``__reads__``/``__mcp__``/``__plans__``) skip
    # the plans-metric emission. The orchestrator analysing the sink
    # tables would otherwise generate fresh ``plans`` partitions for
    # tomorrow's flush, leading to slow amplification.
    try:
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
            except MonitoringDurabilityError:
                # Enabled monitoring is part of the acknowledged operation
                # boundary. Redis outage is already absorbed by the durable
                # spool; only spool durability/backpressure reaches here.
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning("Monitoring logging failed (non-fatal): %s", e)

        # Persist only exact, successful, non-fallback AUTO observations in the
        # compact router store. Forced/failed/fallback executions remain visible
        # but cannot train a pure-engine EWMA.
        try:
            observation = QueryObservation.from_profile(
                normalized,
                query_id=getattr(query_plan_manager, "query_id", ""),
            )
            if observation.feedback_eligible:
                store = getattr(
                    query_plan_manager, "query_observation_store", None,
                ) or QueryObservationStore(
                    getattr(query_plan_manager, "organization", ""),
                )
                store.record(observation)
        except Exception as e:  # noqa: BLE001
            logger.debug("Query observation persistence skipped: %s", e)
    finally:
        # Raw profiles can include operational details. Always remove them even
        # when monitoring durability/backpressure is propagated to the caller.
        try:
            if plan_path and os.path.isfile(plan_path):
                os.remove(plan_path)
                logger.debug("Deleted plan JSON: %s", plan_path)
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to delete plan JSON (non-fatal): %s", e)
