# supertable/reflection/quality/anomaly.py
"""
Anomaly detection — compares current check results with previous results.

A1: Row count spike/drop
A2: NULL rate spike
A3: Mean drift (z-score)
A4: Cardinality shift
A5: Min/Max boundary breach

No SQL — all computed from quick profile results stored in Redis.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_anomalies(
    current: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
    config_checks: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Compare current quick profile result with previous run.
    Returns list of anomaly dicts.

    current/previous: output of checker.parse_quick_result()
      {"total": int, "columns": {col_name: {...}, ...}}
    config_checks: effective check config (merged global + table)
    """
    if not previous:
        return []

    anomalies: List[Dict[str, Any]] = []

    # ── A1: Row count spike/drop ──────────────────────────────────
    t1_conf = config_checks.get("T1", {})
    if t1_conf.get("enabled") and previous.get("total"):
        cur_total = current.get("total", 0)
        prev_total = previous.get("total", 0)
        if prev_total > 0:
            delta_pct = ((cur_total - prev_total) / prev_total) * 100
            threshold = float(t1_conf.get("threshold", 30))
            if abs(delta_pct) > threshold:
                direction = "increased" if delta_pct > 0 else "decreased"
                anomalies.append({
                    "check_id": "A1",
                    "check_name": "Row count anomaly",
                    "column": None,
                    "severity": "critical" if abs(delta_pct) > threshold * 2 else "warning",
                    "message": f"Row count {direction} by {abs(delta_pct):.1f}% ({prev_total:,} → {cur_total:,})",
                    "value": round(delta_pct, 1),
                    "threshold": threshold,
                    "detected_at": _now_iso(),
                })

    cur_cols = current.get("columns", {})
    prev_cols = previous.get("columns", {})

    for col_name, cur_col in cur_cols.items():
        prev_col = prev_cols.get(col_name)
        if not prev_col:
            continue

        # ── A2: NULL rate spike ───────────────────────────────────
        c1_conf = config_checks.get("C1", {})
        if c1_conf.get("enabled"):
            cur_nr = cur_col.get("null_rate", 0)
            prev_nr = prev_col.get("null_rate", 0)
            delta_pp = cur_nr - prev_nr
            threshold = float(c1_conf.get("threshold", 5))
            if delta_pp > threshold:
                anomalies.append({
                    "check_id": "A2",
                    "check_name": "NULL rate spike",
                    "column": col_name,
                    "severity": "critical" if delta_pp > threshold * 2 else "warning",
                    "message": f"{col_name}: NULL rate increased by {delta_pp:.1f}pp ({prev_nr:.1f}% → {cur_nr:.1f}%)",
                    "value": round(delta_pp, 2),
                    "threshold": threshold,
                    "detected_at": _now_iso(),
                })

        # ── A3: Mean drift (z-score) ──────────────────────────────
        c6_conf = config_checks.get("C6", {})
        if c6_conf.get("enabled") and cur_col.get("category") == "numeric":
            cur_avg = cur_col.get("avg")
            prev_avg = prev_col.get("avg")
            prev_stddev = prev_col.get("stddev")
            if cur_avg is not None and prev_avg is not None and prev_stddev and prev_stddev > 0:
                z_score = abs(cur_avg - prev_avg) / prev_stddev
                threshold = float(c6_conf.get("threshold", 2.0))
                if z_score > threshold:
                    direction = "increased" if cur_avg > prev_avg else "decreased"
                    anomalies.append({
                        "check_id": "A3",
                        "check_name": "Mean drift",
                        "column": col_name,
                        "severity": "critical" if z_score > threshold * 2 else "warning",
                        "message": f"{col_name}: mean {direction} (z-score: {z_score:.2f}, {prev_avg:.2f} → {cur_avg:.2f})",
                        "value": round(z_score, 2),
                        "threshold": threshold,
                        "detected_at": _now_iso(),
                    })

        # ── A4: Cardinality shift ─────────────────────────────────
        c2_conf = config_checks.get("C2", {})
        if c2_conf.get("enabled"):
            cur_dist = cur_col.get("distinct", 0)
            prev_dist = prev_col.get("distinct", 0)
            if prev_dist > 0:
                delta_pct = ((cur_dist - prev_dist) / prev_dist) * 100
                threshold = float(c2_conf.get("threshold", 50))
                if abs(delta_pct) > threshold:
                    direction = "increased" if delta_pct > 0 else "decreased"
                    anomalies.append({
                        "check_id": "A4",
                        "check_name": "Cardinality shift",
                        "column": col_name,
                        "severity": "critical" if abs(delta_pct) > threshold * 2 else "warning",
                        "message": f"{col_name}: distinct count {direction} by {abs(delta_pct):.1f}% ({prev_dist:,} → {cur_dist:,})",
                        "value": round(delta_pct, 1),
                        "threshold": threshold,
                        "detected_at": _now_iso(),
                    })

        # ── A5: Min/Max boundary breach ───────────────────────────
        c3_conf = config_checks.get("C3", {})
        if c3_conf.get("enabled") and cur_col.get("category") in ("numeric", "date"):
            cur_min = cur_col.get("min")
            cur_max = cur_col.get("max")
            prev_min = prev_col.get("min")
            prev_max = prev_col.get("max")

            breaches = []
            if cur_min is not None and prev_min is not None:
                try:
                    if float(cur_min) < float(prev_min):
                        breaches.append(f"new min {cur_min} < previous {prev_min}")
                except (TypeError, ValueError):
                    if str(cur_min) < str(prev_min):
                        breaches.append(f"new min {cur_min} < previous {prev_min}")

            if cur_max is not None and prev_max is not None:
                try:
                    if float(cur_max) > float(prev_max):
                        breaches.append(f"new max {cur_max} > previous {prev_max}")
                except (TypeError, ValueError):
                    if str(cur_max) > str(prev_max):
                        breaches.append(f"new max {cur_max} > previous {prev_max}")

            if breaches:
                anomalies.append({
                    "check_id": "A5",
                    "check_name": "Min/Max breach",
                    "column": col_name,
                    "severity": "warning",
                    "message": f"{col_name}: {'; '.join(breaches)}",
                    "value": None,
                    "threshold": None,
                    "detected_at": _now_iso(),
                })

    # ── C5: Zero/negative rate spike ──────────────────────────────
    c5_conf = config_checks.get("C5", {})
    if c5_conf.get("enabled"):
        for col_name, cur_col in cur_cols.items():
            if cur_col.get("category") != "numeric":
                continue
            prev_col = prev_cols.get(col_name)
            if not prev_col:
                continue

            for rate_key, rate_label in [("zero_rate", "Zero rate"), ("negative_rate", "Negative rate")]:
                cur_rate = cur_col.get(rate_key, 0)
                prev_rate = prev_col.get(rate_key, 0)
                delta = cur_rate - prev_rate
                threshold = float(c5_conf.get("threshold", 5))
                if delta > threshold:
                    anomalies.append({
                        "check_id": "A5_C5",
                        "check_name": f"{rate_label} spike",
                        "column": col_name,
                        "severity": "warning",
                        "message": f"{col_name}: {rate_label} increased by {delta:.1f}pp ({prev_rate:.1f}% → {cur_rate:.1f}%)",
                        "value": round(delta, 2),
                        "threshold": threshold,
                        "detected_at": _now_iso(),
                    })

    return anomalies


def detect_schema_drift(
    current_schema: List[tuple],
    previous_schema: Optional[List[tuple]],
    config_checks: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Compare current vs previous schema (list of (col_name, col_type) tuples).
    Returns anomalies for T3 (schema drift).
    """
    t3_conf = config_checks.get("T3", {})
    if not t3_conf.get("enabled") or not previous_schema:
        return []

    prev_map = {name: ctype for name, ctype in previous_schema}
    cur_map = {name: ctype for name, ctype in current_schema}

    anomalies = []
    prev_names = set(prev_map.keys())
    cur_names = set(cur_map.keys())

    added = cur_names - prev_names
    removed = prev_names - cur_names
    common = cur_names & prev_names
    type_changed = [(c, prev_map[c], cur_map[c]) for c in common
                    if prev_map[c] != cur_map[c]]

    changes = []
    if added:
        changes.append(f"added: {', '.join(sorted(added))}")
    if removed:
        changes.append(f"removed: {', '.join(sorted(removed))}")
    if type_changed:
        tc_strs = [f"{c} ({old}→{new})" for c, old, new in type_changed]
        changes.append(f"type changed: {', '.join(tc_strs)}")

    if changes:
        anomalies.append({
            "check_id": "A_T3",
            "check_name": "Schema drift",
            "column": None,
            "severity": "warning",
            "message": f"Schema changed: {'; '.join(changes)}",
            "value": len(added) + len(removed) + len(type_changed),
            "threshold": None,
            "detected_at": _now_iso(),
        })

    return anomalies
