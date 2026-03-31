# route: supertable.audit.events
"""
Audit event schema, classification enums, and action constants.

This module defines the canonical audit event structure used across all
SuperTable servers (API, WebUI, MCP). Every field is typed, documented,
and frozen — events are immutable once created.

Compliance: DORA Art. 6(5), 10, 11; SOC 2 CC6, CC7, CC8.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Classification enums
# ---------------------------------------------------------------------------

class EventCategory(str, Enum):
    """Top-level event classification aligned to compliance domains."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    DATA_MUTATION = "data_mutation"
    RBAC_CHANGE = "rbac_change"
    CONFIG_CHANGE = "config_change"
    TOKEN_MGMT = "token_management"
    SYSTEM = "system"
    EXPORT = "export"
    SECURITY_ALERT = "security_alert"


class Severity(str, Enum):
    """Event severity — drives alerting thresholds and retention priority."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class Outcome(str, Enum):
    """Operation result."""
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"


class ActorType(str, Enum):
    """Identity type of the actor who triggered the event."""
    USER = "user"
    SUPERUSER = "superuser"
    API_TOKEN = "api_token"
    SYSTEM = "system"
    MCP = "mcp"


# ---------------------------------------------------------------------------
# Instance identity (stable per process, used for concurrency-safe file naming)
# ---------------------------------------------------------------------------

def _build_instance_id() -> str:
    """hostname-PID — unique per process, stable for its lifetime."""
    try:
        host = socket.gethostname()[:32]
    except Exception:
        host = "unknown"
    return f"{host}-{os.getpid()}"


INSTANCE_ID: str = _build_instance_id()


# ---------------------------------------------------------------------------
# UUID v7-like time-ordered ID
# ---------------------------------------------------------------------------

_COUNTER = 0


def _uuid7() -> str:
    """Generate a time-ordered UUID (v7-like).

    Format: {unix_ms_hex}-{counter_hex}-{random_hex}
    Lexicographic sorting = chronological ordering.
    """
    global _COUNTER
    ms = int(time.time() * 1000)
    _COUNTER += 1
    rand = uuid.uuid4().hex[:8]
    return f"{ms:012x}-{_COUNTER:04x}-{rand}"


# ---------------------------------------------------------------------------
# AuditEvent dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AuditEvent:
    """Immutable audit event — the atomic unit of the audit trail.

    Fields are grouped by purpose:
      Identity    — event_id, timestamp_ms
      Class       — category, action, severity
      Actor       — who triggered it
      Context     — tenant, correlation, server
      Resource    — what was acted upon
      Operation   — detail payload, outcome, reason
      Integrity   — chain_hash (set by the writer, not the emitter)
    """

    # ── Identity ───────────────────────────────────────────
    event_id: str = field(default_factory=_uuid7)
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    # ── Classification ─────────────────────────────────────
    category: str = ""
    action: str = ""
    severity: str = Severity.INFO

    # ── Actor ──────────────────────────────────────────────
    actor_type: str = ActorType.SYSTEM
    actor_id: str = ""
    actor_username: str = ""
    actor_ip: str = ""
    actor_user_agent: str = ""

    # ── Context ────────────────────────────────────────────
    organization: str = ""
    super_name: str = ""
    correlation_id: str = ""
    session_id: str = ""
    server: str = ""

    # ── Resource ───────────────────────────────────────────
    resource_type: str = ""
    resource_id: str = ""

    # ── Operation details ──────────────────────────────────
    detail: str = ""
    outcome: str = Outcome.SUCCESS
    reason: str = ""

    # ── Integrity (set by writer, not by emitter) ──────────
    chain_hash: str = ""

    # ── Instance (set automatically) ───────────────────────
    instance_id: str = field(default_factory=lambda: INSTANCE_ID)

    # ── Serialization ──────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a flat dict suitable for Redis XADD or Parquet row."""
        return asdict(self)

    def to_json(self) -> str:
        """Compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    def event_hash(self) -> str:
        """SHA-256 hash of the event content (excluding chain_hash and instance_id).

        Used as input to the integrity chain. Deterministic for the same event.
        """
        d = self.to_dict()
        d.pop("chain_hash", None)
        d.pop("instance_id", None)
        canonical = json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AuditEvent":
        """Reconstruct from a dict (e.g. from Redis or Parquet)."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


# ---------------------------------------------------------------------------
# Detail payload helper
# ---------------------------------------------------------------------------

def make_detail(**kwargs: Any) -> str:
    """Serialize action-specific detail fields to a JSON string.

    Usage:
        detail = make_detail(sql_hash="abc", row_count=42, duration_ms=123)
    """
    clean = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, (list, dict, str, int, float, bool)):
            clean[k] = v
        else:
            clean[k] = str(v)
    return json.dumps(clean, separators=(",", ":"), ensure_ascii=False) if clean else ""


# ---------------------------------------------------------------------------
# Action constants (canonical strings — use these, not raw strings)
# ---------------------------------------------------------------------------

class Actions:
    """Canonical action verbs grouped by category.

    Every emit() call must use one of these constants.
    """

    # Authentication
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    SESSION_EXPIRED = "session_expired"
    TOKEN_AUTH_SUCCESS = "token_auth_success"
    TOKEN_AUTH_FAILURE = "token_auth_failure"
    MCP_AUTH_SUCCESS = "mcp_auth_success"
    MCP_AUTH_FAILURE = "mcp_auth_failure"

    # Authorization
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    ROW_FILTER_APPLIED = "row_filter_applied"
    COLUMN_FILTER_APPLIED = "column_filter_applied"

    # Data access
    QUERY_EXECUTE = "query_execute"
    TABLE_READ = "table_read"
    TABLE_LIST = "table_list"
    METADATA_READ = "metadata_read"
    SCHEMA_READ = "schema_read"

    # Data mutation
    DATA_WRITE = "data_write"
    DATA_DELETE = "data_delete"
    TABLE_CREATE = "table_create"
    TABLE_DELETE = "table_delete"
    TABLE_CONFIG_CHANGE = "table_config_change"
    STAGING_CREATE = "staging_create"
    STAGING_DELETE = "staging_delete"
    PIPE_CREATE = "pipe_create"
    PIPE_UPDATE = "pipe_update"
    PIPE_DELETE = "pipe_delete"
    PIPE_ENABLE = "pipe_enable"
    PIPE_DISABLE = "pipe_disable"
    FILE_UPLOAD = "file_upload"
    SUPERTABLE_CREATE = "supertable_create"
    SUPERTABLE_DELETE = "supertable_delete"
    SUPERTABLE_CLONE_READONLY = "supertable_clone_readonly"
    SUPERTABLE_CLONE_WRITABLE = "supertable_clone_writable"
    SUPERTABLE_TOGGLE_READONLY = "supertable_toggle_readonly"
    TABLE_CLONE = "table_clone"

    # RBAC changes
    ROLE_CREATE = "role_create"
    ROLE_UPDATE = "role_update"
    ROLE_DELETE = "role_delete"
    USER_CREATE = "user_create"
    USER_UPDATE = "user_update"
    USER_DELETE = "user_delete"
    USER_ROLE_ASSIGN = "user_role_assign"
    USER_ROLE_REMOVE = "user_role_remove"
    AUDITOR_ROLE_CREATE = "auditor_role_create"
    AUDITOR_ROLE_REVOKE = "auditor_role_revoke"

    # Configuration changes
    ENGINE_CONFIG_CHANGE = "engine_config_change"
    MIRROR_ENABLE = "mirror_enable"
    MIRROR_DISABLE = "mirror_disable"
    SETTING_CHANGE = "setting_change"

    # Token management
    TOKEN_CREATE = "token_create"
    TOKEN_DELETE = "token_delete"
    TOKEN_REGENERATE = "token_regenerate"

    # System
    SERVICE_START = "service_start"
    SERVICE_STOP = "service_stop"
    HEALTH_CHECK_FAILURE = "health_check_failure"
    AUDIT_GAP = "audit_gap"

    # Export
    ODATA_ACCESS = "odata_access"
    AUDIT_EXPORT = "audit_export"

    # Retention & legal hold
    RETENTION_EXECUTE = "retention_execute"
    LEGAL_HOLD_CHANGE = "legal_hold_change"

    # Garbage collection
    GC_EXECUTE = "gc_execute"
    GC_PREVIEW = "gc_preview"

    # Snapshot history
    SNAPSHOT_HISTORY_READ = "snapshot_history_read"

    # Security alerts
    BRUTE_FORCE_DETECTED = "brute_force_detected"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    UNUSUAL_ACCESS_PATTERN = "unusual_access_pattern"
