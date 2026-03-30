# route: supertable.audit.export
"""
Audit log export for compliance reporting.

Provides structured exports aligned to:
  - DORA RTS/ITS incident reporting templates
  - SOC 2 Trust Services Criteria evidence packages

Output formats: JSON-lines, CSV.

Full implementation: Phase 7.

Compliance: DORA Art. 11, 19; SOC 2 CC7.3.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def export_events(
    events: List[Dict[str, Any]],
    output_format: str = "json",
) -> bytes:
    """Export a list of audit event dicts to the specified format.

    Formats:
      "json"  — JSON-lines (one JSON object per line)
      "csv"   — CSV with header row
    """
    if output_format == "csv":
        return _to_csv(events)
    return _to_jsonlines(events)


def _to_jsonlines(events: List[Dict[str, Any]]) -> bytes:
    lines = []
    for event in events:
        lines.append(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _to_csv(events: List[Dict[str, Any]]) -> bytes:
    if not events:
        return b""
    buf = io.StringIO()
    fieldnames = list(events[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for event in events:
        writer.writerow(event)
    return buf.getvalue().encode("utf-8")


def export_dora_incident_report(
    organization: str,
    incident_id: str,
    start_ms: int,
    end_ms: int,
    output_format: str = "json",
) -> bytes:
    """Export audit events for a specific time window in DORA-aligned format.

    TODO Phase 7: align with RTS/ITS templates (Regulation 2024/1772).
    """
    from supertable.audit.reader import query_audit_log
    events = query_audit_log(organization, start_ms=start_ms, end_ms=end_ms, limit=50000)
    return export_events(events, output_format)


def export_soc2_evidence(
    organization: str,
    criteria: str,
    period_start_ms: int,
    period_end_ms: int,
    output_format: str = "json",
) -> bytes:
    """Export audit events relevant to a specific SOC 2 criterion.

    Maps each SOC 2 Trust Services Criterion to specific event categories
    and actions.  CC7.3 (incident response) spans all categories.

    Supported criteria: CC6.1, CC6.2, CC6.3, CC7.1, CC7.2, CC7.3,
    CC8.1, PI1.3, A1.2.
    """
    # Map SOC 2 criteria to (category, [actions]) tuples.
    # None category means "all categories" for that criterion.
    # Empty actions list means "all actions within the category".
    criteria_map: Dict[str, Dict[str, Any]] = {
        "CC6.1": {
            "category": "authentication",
            "actions": [],  # all auth events
        },
        "CC6.2": {
            "category": "authorization",
            "actions": [],
        },
        "CC6.3": {
            "category": "rbac_change",
            "actions": [],
        },
        "CC7.1": {
            "category": "system",
            "actions": [],
        },
        "CC7.2": {
            "categories": ["security_alert", "authentication"],
            "actions": [
                "brute_force_detected",
                "privilege_escalation",
                "unusual_access_pattern",
                "login_failure",
                "token_auth_failure",
                "mcp_auth_failure",
            ],
        },
        "CC7.3": {
            "category": None,  # all categories — incident response
            "actions": [],
        },
        "CC8.1": {
            "category": "config_change",
            "actions": [],
        },
        "PI1.3": {
            "category": "data_mutation",
            "actions": [],
        },
        "A1.2": {
            "categories": ["system", "config_change"],
            "actions": [
                "service_start",
                "service_stop",
                "health_check_failure",
                "retention_execute",
                "legal_hold_change",
                "engine_config_change",
            ],
        },
    }

    spec = criteria_map.get(criteria, {"category": None, "actions": []})
    category_filter = spec.get("category")
    action_filter_set = set(spec.get("actions", []))
    multi_categories = set(spec.get("categories", []))

    from supertable.audit.reader import query_audit_log

    if multi_categories:
        # Criteria spanning multiple categories: query each separately, merge
        all_events: List[Dict[str, Any]] = []
        seen_ids: set = set()
        for cat in multi_categories:
            cat_events = query_audit_log(
                organization,
                start_ms=period_start_ms,
                end_ms=period_end_ms,
                category=cat,
                limit=50000,
            )
            for ev in cat_events:
                eid = ev.get("event_id", "")
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    all_events.append(ev)
        events = all_events
    else:
        events = query_audit_log(
            organization,
            start_ms=period_start_ms,
            end_ms=period_end_ms,
            category=category_filter,
            limit=50000,
        )

    # Apply action-level filtering if the criterion specifies specific actions
    if action_filter_set:
        events = [e for e in events if e.get("action", "") in action_filter_set]

    return export_events(events, output_format)
