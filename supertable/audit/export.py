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

    TODO Phase 7: filter by category/action based on criterion mapping.
    """
    # Map SOC 2 criteria to event categories
    criteria_map: Dict[str, Optional[str]] = {
        "CC6.1": "authentication",
        "CC6.2": "authorization",
        "CC6.3": "rbac_change",
        "CC7.1": "system",
        "CC7.2": "security_alert",
        "CC7.3": None,  # all categories (incident response)
        "CC8.1": "config_change",
        "PI1.3": "data_mutation",
        "A1.2": "system",
    }
    category = criteria_map.get(criteria)

    from supertable.audit.reader import query_audit_log
    events = query_audit_log(
        organization,
        start_ms=period_start_ms,
        end_ms=period_end_ms,
        category=category,
        limit=50000,
    )
    return export_events(events, output_format)
