# route: supertable.audit
"""
Audit logging module for Data Island Core.

Provides two deliberately different audit lanes:

* general security/telemetry events use the non-blocking, configurable logger;
* RBAC role, user, and assignment mutations plus organization auth-token
  create/delete use a mandatory Redis outbox committed atomically with the
  state change and a verified Parquet archive. Token IDs and metadata digests,
  never plaintext tokens, enter this ledger.

The second lane is never disabled by general audit configuration and never
silently drops a privileged state transition.

Quick start:

    from supertable.audit import emit, EventCategory, Actions, Severity, make_detail

    # Emit an event (non-blocking)
    emit(
        category=EventCategory.RBAC_CHANGE,
        action=Actions.ROLE_CREATE,
        organization="acme",
        actor_type="superuser",
        actor_id="abc123",
        actor_username="admin",
        resource_type="role",
        resource_id="role_456",
        detail=make_detail(role_name="analyst", role_type="viewer"),
        severity=Severity.WARNING,
    )

    # With request context (in a FastAPI handler):
    from supertable.audit import emit, audit_context
    emit(**audit_context(request), category=..., action=..., ...)

Compliance: DORA (Regulation 2022/2554), SOC 2 Type II.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from supertable.audit.events import (
    AuditEvent,
    EventCategory,
    Severity,
    Outcome,
    ActorType,
    Actions,
    make_detail,
)
from supertable.audit.logger import (
    get_audit_logger,
    shutdown_all,
)
from supertable.audit.privileged import (
    PrivilegedActionContext,
    PrivilegedAuditRecord,
    build_record as build_privileged_record,
    verify_records as verify_privileged_records,
)
from supertable.audit.privileged_outbox import (
    CascadeEvidenceRow,
    CascadeManifest,
    PrivilegedAuditOutbox,
    PrivilegedOutboxError,
)

logger = logging.getLogger(__name__)

__all__ = [
    "emit",
    "audit_context",
    "get_audit_logger",
    "shutdown_all",
    "AuditEvent",
    "EventCategory",
    "Severity",
    "Outcome",
    "ActorType",
    "Actions",
    "make_detail",
    "PrivilegedActionContext",
    "PrivilegedAuditRecord",
    "PrivilegedAuditOutbox",
    "CascadeEvidenceRow",
    "CascadeManifest",
    "PrivilegedOutboxError",
    "build_privileged_record",
    "verify_privileged_records",
    "get_privileged_audit_outbox",
]


def get_privileged_audit_outbox(
    organization: str,
    *,
    redis_client=None,
    storage=None,
) -> PrivilegedAuditOutbox:
    """Construct the mandatory privileged control-plane WAL reader/archiver.

    This is intentionally separate from :func:`get_audit_logger`: runtime
    audit configuration can disable the best-effort event logger, but it can
    never disable or redirect the privileged mutation ledger.
    """
    if not organization:
        raise ValueError("organization is required")
    if redis_client is None:
        from supertable.redis_connector import RedisConnector

        redis_client = RedisConnector().r
    from supertable import redis_keys as RK
    from supertable.audit.writer_parquet import ParquetAuditWriter

    return PrivilegedAuditOutbox(
        redis_client,
        stream_key=RK.audit_privileged_outbox(organization),
        delivery_ledger_key=RK.audit_privileged_delivery(organization),
        parquet_writer=ParquetAuditWriter(storage=storage),
    )


# ---------------------------------------------------------------------------
# Convenience emit() — the primary public API
# ---------------------------------------------------------------------------

def emit(
    *,
    category: str | EventCategory,
    action: str,
    organization: str,
    actor_type: str = ActorType.SYSTEM,
    actor_id: str = "",
    actor_username: str = "",
    actor_ip: str = "",
    actor_user_agent: str = "",
    resource_type: str = "",
    resource_id: str = "",
    detail: str | dict | None = None,
    outcome: str = Outcome.SUCCESS,
    reason: str = "",
    severity: str = Severity.INFO,
    correlation_id: str = "",
    session_id: str = "",
    server: str = "",
    super_name: str = "",
) -> None:
    """Emit an audit event. Non-blocking (enqueues and returns).

    This is the function you call from endpoint handlers, middleware,
    and business logic. It constructs an AuditEvent and hands it to
    the background worker for the given organization.

    All parameters are keyword-only to prevent positional mistakes.
    """
    if not organization:
        return

    # Serialize detail dict if needed
    detail_str = ""
    if isinstance(detail, dict):
        detail_str = make_detail(**detail)
    elif isinstance(detail, str):
        detail_str = detail
    elif detail is not None:
        detail_str = str(detail)

    # Truncate user agent
    ua = (actor_user_agent or "")[:256]

    event = AuditEvent(
        category=str(category.value if isinstance(category, EventCategory) else category),
        action=action,
        severity=str(severity.value if isinstance(severity, Severity) else severity),
        actor_type=str(actor_type.value if isinstance(actor_type, ActorType) else actor_type),
        actor_id=actor_id,
        actor_username=actor_username,
        actor_ip=actor_ip,
        actor_user_agent=ua,
        organization=organization,
        super_name=super_name,
        correlation_id=correlation_id,
        session_id=session_id,
        server=server,
        resource_type=resource_type,
        resource_id=resource_id,
        detail=detail_str,
        outcome=str(outcome.value if isinstance(outcome, Outcome) else outcome),
        reason=reason,
    )

    try:
        audit_logger = get_audit_logger(organization)
        audit_logger.emit(event)
    except Exception as e:
        logger.error("[audit] emit failed: %s — event: %s/%s", e, category, action)


# ---------------------------------------------------------------------------
# Request context helper
# ---------------------------------------------------------------------------

def audit_context(request) -> dict:
    """Extract actor, correlation, and session info from a FastAPI Request.

    Returns a dict that can be **unpacked into emit():

        from supertable.audit import emit, audit_context
        emit(**audit_context(request), category=..., action=..., ...)

    Gracefully handles missing attributes (e.g. in tests or non-HTTP contexts).
    """
    ctx: dict[str, Any] = {}

    # Correlation ID (set by RequestLoggingMiddleware)
    ctx["correlation_id"] = getattr(getattr(request, "state", None), "correlation_id", "")

    # Client IP
    client = getattr(request, "client", None)
    ctx["actor_ip"] = getattr(client, "host", "") if client else ""

    # User agent
    headers = getattr(request, "headers", {})
    ctx["actor_user_agent"] = (headers.get("user-agent") or "")[:256]

    # Session info (set by inject_session_into_ctx or stored on request.state)
    state = getattr(request, "state", None)
    if state:
        ctx["actor_username"] = getattr(state, "session_username", "") or ""
        ctx["actor_id"] = getattr(state, "session_user_hash", "") or ""
        ctx["session_id"] = getattr(state, "session_id", "") or ""
        is_super = getattr(state, "session_is_superuser", False)
        ctx["actor_type"] = ActorType.SUPERUSER if is_super else ActorType.USER
    else:
        ctx["actor_type"] = ActorType.SYSTEM

    return ctx
