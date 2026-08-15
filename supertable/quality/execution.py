"""Fail-closed SQL execution boundary for data-quality checks.

Quality checks are system queries, but they use the same public SQL contract as
ordinary reads.  In particular, ``AUTO`` must certify the *whole* statement
before IslandDB is eligible; unsupported statements execute in DuckDB and an
explicit IslandDB request remains an error.  This module does not implement a
second router.  It consumes the authoritative decision made by
``engine.Executor`` and exposes it as a structured result so a check can never
mistake an engine error or a DuckDB fallback for IslandDB parity.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pandas as pd

from supertable.data_reader import DataReader, Status
from supertable.engine.engine_enum import Engine


class QualitySQLExecutionError(RuntimeError):
    """Raised when a caller requires a successful quality SQL result."""


def _latest_plan_stat(
    stats: Iterable[object], key: str,
) -> Optional[object]:
    value: Optional[object] = None
    for entry in stats:
        if isinstance(entry, dict) and key in entry:
            value = entry[key]
    return value


@dataclass(frozen=True)
class QualitySQLResult:
    """One quality SQL outcome with auditable engine provenance."""

    frame: pd.DataFrame
    ok: bool
    status: str
    message: Optional[str]
    requested_engine: str
    selected_engine: Optional[str]
    actual_engine: Optional[str]
    fallback: bool
    island_supported: Optional[bool]
    island_certification_scope: Optional[str]
    island_certification_reasons: Tuple[str, ...]
    plan_stats: Tuple[Dict[str, Any], ...]

    def require_success(self) -> pd.DataFrame:
        """Return the frame or raise; failed SQL must not become empty data."""
        if self.ok:
            return self.frame
        detail = self.message or "unknown execution error"
        raise QualitySQLExecutionError(
            "Quality SQL execution failed "
            f"(requested={self.requested_engine}, "
            f"selected={self.selected_engine or 'none'}, "
            f"actual={self.actual_engine or 'none'}): {detail}"
        )


def _normalise_engine(engine: Engine | str) -> Engine:
    if isinstance(engine, Engine):
        return engine
    return Engine(str(engine).strip().casefold())


def execute_quality_sql(
    *,
    organization: str,
    super_name: str,
    sql: str,
    role_name: str = "superadmin",
    engine: Engine | str = Engine.AUTO,
    reader_factory: Optional[Callable[..., DataReader]] = None,
) -> QualitySQLResult:
    """Execute one complete quality statement through the normal SQL router.

    The returned ``ok`` flag is authoritative.  Callers evaluating a check
    should use :meth:`QualitySQLResult.require_success` before inspecting the
    frame.  That prevents the historical failure mode where ``DataReader``'s
    empty error frame was interpreted as zero violations.

    ``island_supported`` describes static whole-query certification, not the
    engine that happened to win AUTO's performance decision.  ``actual_engine``
    records the executor that really produced the frame, including a physical
    pre-execution fallback.
    """
    requested = _normalise_engine(engine)
    factory = reader_factory or DataReader

    try:
        reader = factory(
            super_name=super_name,
            organization=organization,
            query=sql,
            source="quality",
        )
        frame, raw_status, message = reader.execute(
            role_name=role_name,
            engine=requested,
        )
    except Exception as exc:
        # RBAC and construction failures intentionally propagate out of
        # DataReader.  Quality scheduling needs the same structured failure as
        # an engine exception, never an implicit pass.
        frame = pd.DataFrame()
        raw_status = Status.ERROR
        message = str(exc)
        reader = locals().get("reader")

    status = getattr(raw_status, "value", str(raw_status)).casefold()
    ok = raw_status is Status.OK or status == Status.OK.value
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame() if frame is None else pd.DataFrame(frame)

    plan_stats_obj = getattr(reader, "plan_stats", None) if reader else None
    raw_stats = tuple(getattr(plan_stats_obj, "stats", ()) or ())
    plan_stats: Tuple[Dict[str, Any], ...] = tuple(
        dict(entry) for entry in raw_stats if isinstance(entry, dict)
    )

    request = _latest_plan_stat(plan_stats, "ENGINE_REQUEST")
    routing = _latest_plan_stat(plan_stats, "AUTO_ROUTING")
    outcome = _latest_plan_stat(plan_stats, "AUTO_ROUTING_OUTCOME")
    capability = _latest_plan_stat(plan_stats, "ENGINE_CAPABILITY")
    recorded_engine = _latest_plan_stat(plan_stats, "ENGINE")

    selected_engine: Optional[str] = None
    if isinstance(request, dict):
        selected_engine = str(request.get("selected_engine") or "") or None
    if selected_engine is None and isinstance(routing, dict):
        selected_engine = str(routing.get("selected_engine") or "") or None

    actual_engine: Optional[str] = None
    fallback = False
    if isinstance(outcome, dict):
        actual_engine = str(outcome.get("actual_engine") or "") or None
        fallback = bool(outcome.get("fallback", False))
    if actual_engine is None and isinstance(recorded_engine, str):
        actual_engine = recorded_engine
    # No successful frame means no engine may be credited as having produced
    # a result, even if an attempt/routing record exists.
    if not ok:
        actual_engine = None

    island_supported: Optional[bool] = None
    island_scope: Optional[str] = None
    island_reasons: Tuple[str, ...] = ()
    if (
        isinstance(capability, dict)
        and capability.get("engine") == Engine.ISLANDDB.value
    ):
        island_supported = capability.get("supported") is True
        island_scope = str(capability.get("scope") or "") or None
        reasons = capability.get("reasons")
        if isinstance(reasons, (list, tuple)):
            island_reasons = tuple(str(reason) for reason in reasons)

    # Older/custom executors may not publish the capability record.  Preserve
    # explicit IslandDB rejection as visible (but unscoped) evidence rather
    # than claiming support or silently calling it DuckDB parity.
    if (
        island_supported is None
        and requested is Engine.ISLANDDB
        and not ok
    ):
        island_supported = False
        island_reasons = (message or "IslandDB execution rejected",)

    return QualitySQLResult(
        frame=frame,
        ok=ok,
        status=status,
        message=message,
        requested_engine=requested.value,
        selected_engine=selected_engine,
        actual_engine=actual_engine,
        fallback=fallback,
        island_supported=island_supported,
        island_certification_scope=island_scope,
        island_certification_reasons=island_reasons,
        plan_stats=plan_stats,
    )


__all__ = [
    "QualitySQLExecutionError",
    "QualitySQLResult",
    "execute_quality_sql",
]
