# supertable/quality/anomaly.py
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

import hashlib
import logging
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Sequence

from supertable.quality.checker import (
    DEEP_HISTOGRAM_BUCKET_LIMIT,
    DEEP_STRING_PREVIEW_CHARS,
    DEEP_TOP_VALUES_LIMIT,
)

logger = logging.getLogger(__name__)

_DEEP_CHECK_ORDER = ("D1", "D2", "D3", "D4", "D5", "D7")
_DEEP_SUPPORTED_CATEGORIES = {
    "D1": frozenset({"string"}),
    "D2": frozenset({"string", "numeric"}),
    "D3": frozenset({"string", "numeric"}),
    "D4": frozenset({"string", "numeric"}),
    "D5": frozenset({"numeric"}),
    "D7": frozenset({"string"}),
}


def _check_outcome(
    check_id: str,
    *,
    status: str,
    applicable: bool,
    evaluated: bool,
    message: str,
    value: Any = None,
    threshold: Any = None,
    column: Optional[str] = None,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    result = {
        "check_id": check_id,
        "status": status,
        "applicable": bool(applicable),
        "evaluated": bool(evaluated),
        "message": message,
        "value": value,
        "threshold": threshold,
        "column": column,
    }
    if reason:
        result["reason"] = reason
    return result


def _enabled(checks: Mapping[str, Any], check_id: str) -> Optional[Mapping[str, Any]]:
    config = checks.get(check_id)
    return config if isinstance(config, Mapping) and config.get("enabled") else None


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _exact_number(value: Any) -> Optional[Decimal]:
    """Parse an exact finite number for boundary comparisons.

    C3 must not route integral/decimal extrema through binary64: adjacent
    BIGINT values above 2**53 then compare equal, hiding real boundary
    breaches.  Decimal also accepts the string projections used to preserve
    wide DuckDB scalar values across ``fetchdf``.
    """

    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _threshold(config: Mapping[str, Any]) -> Optional[float]:
    value = _number(config.get("threshold"))
    return value if value is not None and value >= 0 else None


def _severity(config: Mapping[str, Any], amount: Optional[float], threshold: float) -> str:
    configured = str(config.get("severity", "warning")).lower()
    base = configured if configured in {"warning", "critical"} else "warning"
    if base == "warning" and amount is not None and amount > threshold * 2:
        return "critical"
    return base


def _parse_instant(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        # Snapshot/resource timestamps are commonly milliseconds or
        # nanoseconds.  Accept each explicit magnitude without confusing a
        # modern epoch-millisecond value for seconds.
        if abs(numeric) >= 1e17:
            numeric /= 1_000_000_000
        elif abs(numeric) >= 1e14:
            numeric /= 1_000_000
        elif abs(numeric) >= 1e11:
            numeric /= 1_000
        try:
            parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
        except ValueError:
            try:
                return _parse_instant(float(text))
            except ValueError:
                return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def evaluate_table_metadata_checks(
    checks: Mapping[str, Any],
    *,
    last_modified_at: Any,
    current_size_bytes: Any,
    previous_size_bytes: Any,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Evaluate T2 freshness and T5 physical-size trend.

    Inputs are snapshot metadata, never hidden table columns.  A missing
    baseline is a truthful skipped outcome, while missing *current* metadata
    is an error outcome; neither is counted as a pass.
    """

    outcomes: List[Dict[str, Any]] = []
    t2 = _enabled(checks, "T2")
    if t2 is not None:
        threshold = _threshold(t2)
        modified = _parse_instant(last_modified_at)
        reference = _parse_instant(now or datetime.now(timezone.utc))
        if threshold is None:
            outcomes.append(_check_outcome(
                "T2", status="error", applicable=True, evaluated=False,
                message="Freshness threshold must be a non-negative number of hours",
                threshold=t2.get("threshold"), reason="invalid_threshold",
            ))
        elif modified is None or reference is None:
            outcomes.append(_check_outcome(
                "T2", status="error", applicable=True, evaluated=False,
                message="Current snapshot has no valid last-modified timestamp",
                threshold=threshold, reason="missing_current_metadata",
            ))
        else:
            age_hours = max(0.0, (reference - modified).total_seconds() / 3600.0)
            breached = age_hours > threshold
            status = _severity(t2, age_hours, threshold) if breached else "ok"
            outcomes.append(_check_outcome(
                "T2", status=status, applicable=True, evaluated=True,
                message=(
                    f"Table is {age_hours:.2f} hours old (threshold: {threshold:g} hours)"
                    if breached else
                    f"Table freshness is {age_hours:.2f} hours"
                ),
                value=round(age_hours, 4), threshold=threshold,
            ))

    t5 = _enabled(checks, "T5")
    if t5 is not None:
        threshold = _threshold(t5)
        current_size = _number(current_size_bytes)
        previous_size = _number(previous_size_bytes)
        if threshold is None:
            outcomes.append(_check_outcome(
                "T5", status="error", applicable=True, evaluated=False,
                message="Table-size threshold must be a non-negative percentage",
                threshold=t5.get("threshold"), reason="invalid_threshold",
            ))
        elif current_size is None or current_size < 0:
            outcomes.append(_check_outcome(
                "T5", status="error", applicable=True, evaluated=False,
                message="Current snapshot has no valid physical table size",
                threshold=threshold, reason="missing_current_metadata",
            ))
        elif previous_size is None or previous_size < 0:
            outcomes.append(_check_outcome(
                "T5", status="skipped", applicable=True, evaluated=False,
                message="Table-size baseline recorded; comparison starts on the next run",
                value=int(current_size), threshold=threshold, reason="baseline_unavailable",
            ))
        elif previous_size == 0:
            if current_size == 0:
                delta_pct: Optional[float] = 0.0
                breached = False
            else:
                delta_pct = None
                breached = True
            status = _severity(t5, delta_pct, threshold) if breached else "ok"
            outcomes.append(_check_outcome(
                "T5", status=status, applicable=True, evaluated=True,
                message=(
                    f"Table size grew from zero to {int(current_size)} bytes"
                    if breached else "Table size remains zero bytes"
                ),
                value=delta_pct, threshold=threshold,
            ))
        else:
            delta_pct = (current_size - previous_size) / previous_size * 100.0
            breached = abs(delta_pct) > threshold
            status = _severity(t5, abs(delta_pct), threshold) if breached else "ok"
            outcomes.append(_check_outcome(
                "T5", status=status, applicable=True, evaluated=True,
                message=(
                    f"Table size changed by {delta_pct:+.2f}% "
                    f"({int(previous_size)} → {int(current_size)} bytes)"
                ),
                value=round(delta_pct, 4), threshold=threshold,
            ))

    return outcomes


def _not_applicable(check_id: str, column: str, category: str) -> Dict[str, Any]:
    return _check_outcome(
        check_id,
        status="not_applicable",
        applicable=False,
        evaluated=False,
        message=f"{check_id} does not apply to {category or 'unknown'} column {column}",
        column=column,
        reason="unsupported_column_type",
    )


def _deep_missing(
    check_id: str,
    column: str,
    threshold: Any = None,
    *,
    reason: str = "missing_metric",
) -> Dict[str, Any]:
    return _check_outcome(
        check_id,
        status="skipped" if reason in {
            "empty_input", "baseline_unavailable", "unsupported_precision",
        } else "error",
        applicable=True,
        evaluated=False,
        message=(
            f"No non-null values are available for {check_id} on {column}"
            if reason == "empty_input" else
            f"Baseline recorded for {check_id} on {column}; comparison starts on the next run"
            if reason == "baseline_unavailable" else
            f"{check_id} on {column} is skipped because exact numeric moments cannot be certified"
            if reason == "unsupported_precision" else
            f"Threshold for {check_id} on {column} must be a non-negative number"
            if reason == "invalid_threshold" else
            f"Deep profile is missing a valid metric required by {check_id} on {column}"
        ),
        threshold=threshold,
        column=column,
        reason=reason,
    )


def _comparison_outcome(
    check_id: str,
    *,
    column: str,
    current_value: float,
    previous_value: float,
    config: Mapping[str, Any],
    label: str,
) -> Dict[str, Any]:
    threshold = _threshold(config)
    if threshold is None:
        return _deep_missing(check_id, column, config.get("threshold"), reason="invalid_threshold")
    if previous_value == 0:
        delta_pct: Optional[float] = 0.0 if current_value == 0 else None
        breached = current_value != 0
    else:
        delta_pct = abs(current_value - previous_value) / abs(previous_value) * 100.0
        breached = delta_pct > threshold
    status = _severity(config, delta_pct, threshold) if breached else "ok"
    change = "changed from zero" if delta_pct is None else f"changed by {delta_pct:.2f}%"
    return _check_outcome(
        check_id,
        status=status,
        applicable=True,
        evaluated=True,
        message=f"{column}: {label} {change} ({previous_value:g} → {current_value:g})",
        value=None if delta_pct is None else round(delta_pct, 4),
        threshold=threshold,
        column=column,
    )


def _exact_nonnegative_integer(value: Any) -> Optional[int]:
    parsed = _exact_number(value)
    if parsed is None or parsed < 0 or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


_SHA256_HEX = re.compile(r"[0-9a-f]{64}")


def _valid_string_preview(item: Mapping[str, Any], prefix: str) -> bool:
    """Validate one bounded string preview and its complete-value identity."""

    preview = item.get(prefix)
    digest = item.get(f"{prefix}_sha256")
    char_length = _exact_nonnegative_integer(item.get(f"{prefix}_char_length"))
    byte_length = _exact_nonnegative_integer(item.get(f"{prefix}_byte_length"))
    truncated = item.get(f"{prefix}_truncated")
    if (
        not isinstance(preview, str)
        or not isinstance(digest, str)
        or _SHA256_HEX.fullmatch(digest) is None
        or char_length is None
        or byte_length is None
        or not isinstance(truncated, bool)
        or len(preview) > DEEP_STRING_PREVIEW_CHARS
    ):
        return False
    try:
        preview_bytes = preview.encode("utf-8")
    except UnicodeEncodeError:
        return False
    if char_length < len(preview) or byte_length < len(preview_bytes):
        return False
    expected_truncated = char_length > DEEP_STRING_PREVIEW_CHARS
    if truncated is not expected_truncated:
        return False
    if truncated:
        # LEFT(value, limit) is exactly limit characters for a longer value.
        return len(preview) == DEEP_STRING_PREVIEW_CHARS
    return (
        char_length == len(preview)
        and byte_length == len(preview_bytes)
        and digest == hashlib.sha256(preview_bytes).hexdigest()
    )


def _valid_top_values(
    value: Any,
    *,
    category: str,
    non_nulls: int,
    distinct_values: int,
    coverage: float,
) -> bool:
    expected_length = min(DEEP_TOP_VALUES_LIMIT, distinct_values)
    if (
        not isinstance(value, list)
        or len(value) != expected_length
        or expected_length == 0
    ):
        return False

    frequencies: List[int] = []
    numeric_values = []
    string_order = []
    identities = set()
    for item in value:
        if not isinstance(item, Mapping) or "value" not in item or "freq" not in item:
            return False
        frequency = _exact_nonnegative_integer(item.get("freq"))
        if frequency is None or frequency <= 0 or frequency > non_nulls:
            return False
        frequencies.append(frequency)

        if category == "numeric":
            number = _exact_number(item.get("value"))
            if number is None or number in identities:
                return False
            identities.add(number)
            numeric_values.append(number)
        elif category == "string":
            if not _valid_string_preview(item, "value"):
                return False
            identity = (
                item["value_sha256"],
                _exact_nonnegative_integer(item["value_char_length"]),
                _exact_nonnegative_integer(item["value_byte_length"]),
            )
            if identity in identities:
                return False
            identities.add(identity)
            string_order.append(str(item["value"]))
        else:
            return False

    if frequencies != sorted(frequencies, reverse=True):
        return False
    for index in range(1, len(frequencies)):
        if frequencies[index] != frequencies[index - 1]:
            continue
        if category == "numeric" and numeric_values[index] < numeric_values[index - 1]:
            return False
        if category == "string" and string_order[index] < string_order[index - 1]:
            return False

    frequency_sum = sum(frequencies)
    if frequency_sum <= 0 or frequency_sum > non_nulls:
        return False
    expected_coverage = round(frequency_sum * 100.0 / non_nulls, 2)
    return math.isclose(coverage, expected_coverage, rel_tol=0.0, abs_tol=0.011)


def _valid_histogram(
    value: Any,
    *,
    category: str,
    non_nulls: int,
) -> bool:
    expected_length = min(DEEP_HISTOGRAM_BUCKET_LIMIT, non_nulls)
    if (
        not isinstance(value, list)
        or len(value) != expected_length
        or expected_length == 0
    ):
        return False

    bucket_ids: List[int] = []
    frequencies: List[int] = []
    prior_max: Any = None
    for item in value:
        if not isinstance(item, Mapping):
            return False
        if not {"bucket_id", "bucket_min", "bucket_max", "freq"}.issubset(item):
            return False
        bucket_id = _exact_nonnegative_integer(item.get("bucket_id"))
        frequency = _exact_nonnegative_integer(item.get("freq"))
        if (
            bucket_id is None
            or frequency is None
            or frequency <= 0
            or frequency > non_nulls
        ):
            return False
        bucket_ids.append(bucket_id)
        frequencies.append(frequency)

        if category == "numeric":
            minimum = _exact_number(item.get("bucket_min"))
            maximum = _exact_number(item.get("bucket_max"))
            if minimum is None or maximum is None or maximum < minimum:
                return False
        elif category == "string":
            if (
                not _valid_string_preview(item, "bucket_min")
                or not _valid_string_preview(item, "bucket_max")
            ):
                return False
            minimum = str(item["bucket_min"])
            maximum = str(item["bucket_max"])
            if maximum < minimum:
                return False
        else:
            return False
        if prior_max is not None and minimum < prior_max:
            return False
        prior_max = maximum

    if bucket_ids != list(range(1, len(bucket_ids) + 1)):
        return False
    quotient, remainder = divmod(non_nulls, expected_length)
    expected_frequencies = [
        quotient + (1 if index < remainder else 0)
        for index in range(expected_length)
    ]
    return frequencies == expected_frequencies


def evaluate_deep_checks(
    current: Mapping[str, Any],
    previous: Optional[Mapping[str, Any]],
    category: str,
    checks: Mapping[str, Any],
    column_name: str,
) -> List[Dict[str, Any]]:
    """Evaluate every enabled deep check for one column.

    Tracking-only D1/D3/D4 are successful only when their native metric was
    actually produced.  D2/D5 require a prior baseline and D7 is an immediate
    threshold check.  Unsupported types are explicit ``not_applicable``
    outcomes, never invisible passes or SQL failures.
    """

    outcomes: List[Dict[str, Any]] = []
    cur = current if isinstance(current, Mapping) else {}
    prev = previous if isinstance(previous, Mapping) else None
    total_rows = _exact_nonnegative_integer(cur.get("total_rows"))
    non_nulls = _exact_nonnegative_integer(cur.get("non_nulls"))
    distinct_values = _exact_nonnegative_integer(cur.get("distinct_vals"))
    envelope_valid = (
        total_rows is not None
        and non_nulls is not None
        and distinct_values is not None
        and non_nulls <= total_rows
        and distinct_values <= non_nulls
        and (non_nulls == 0 or distinct_values > 0)
    )

    for check_id in _DEEP_CHECK_ORDER:
        config = _enabled(checks, check_id)
        if config is None:
            continue
        if category not in _DEEP_SUPPORTED_CATEGORIES[check_id]:
            outcomes.append(_not_applicable(check_id, column_name, category))
            continue
        if not envelope_valid:
            outcomes.append(_deep_missing(check_id, column_name))
            continue
        assert (
            total_rows is not None
            and non_nulls is not None
            and distinct_values is not None
        )
        if total_rows == 0 or non_nulls == 0:
            outcomes.append(_deep_missing(check_id, column_name, config.get("threshold"), reason="empty_input"))
            continue

        if check_id == "D1":
            fields = ("avg_length", "min_length", "max_length", "median_length")
            metrics = {field: _number(cur.get(field)) for field in fields}
            if any(value is None for value in metrics.values()):
                outcomes.append(_deep_missing(check_id, column_name))
            else:
                outcomes.append(_check_outcome(
                    check_id, status="ok", applicable=True, evaluated=True,
                    message=f"String-length statistics tracked for {column_name}",
                    value=metrics, column=column_name,
                ))
        elif check_id == "D2":
            current_entropy = _number(cur.get("shannon_entropy"))
            previous_entropy = _number(prev.get("shannon_entropy")) if prev is not None else None
            if current_entropy is None or current_entropy < -1e-12:
                outcomes.append(_deep_missing(check_id, column_name, config.get("threshold")))
            elif previous_entropy is None or previous_entropy < -1e-12:
                outcomes.append(_deep_missing(
                    check_id, column_name, config.get("threshold"), reason="baseline_unavailable",
                ))
            else:
                outcomes.append(_comparison_outcome(
                    check_id, column=column_name, current_value=current_entropy,
                    previous_value=previous_entropy,
                    config=config, label="Shannon entropy",
                ))
        elif check_id == "D3":
            coverage = _number(cur.get("topx_coverage_pct"))
            if (
                coverage is None
                or not 0 <= coverage <= 100
                or not _valid_top_values(
                    cur.get("topx_values"),
                    category=category,
                    non_nulls=non_nulls,
                    distinct_values=distinct_values,
                    coverage=coverage,
                )
            ):
                outcomes.append(_deep_missing(check_id, column_name))
            else:
                outcomes.append(_check_outcome(
                    check_id, status="ok", applicable=True, evaluated=True,
                    message=f"Top values tracked for {column_name}",
                    value={
                        "topx_values": cur.get("topx_values"),
                        "topx_coverage_pct": cur.get("topx_coverage_pct"),
                    },
                    column=column_name,
                ))
        elif check_id == "D4":
            if not _valid_histogram(
                cur.get("buckets"),
                category=category,
                non_nulls=non_nulls,
            ):
                outcomes.append(_deep_missing(check_id, column_name))
            else:
                outcomes.append(_check_outcome(
                    check_id, status="ok", applicable=True, evaluated=True,
                    message=f"Histogram tracked for {column_name}",
                    value=cur.get("buckets"), column=column_name,
                ))
        elif check_id == "D5":
            if not bool(cur.get("moments_certified", True)):
                outcomes.append(_deep_missing(
                    check_id,
                    column_name,
                    config.get("threshold"),
                    reason="unsupported_precision",
                ))
                continue
            cur_p25, cur_p75 = _number(cur.get("p25_value")), _number(cur.get("p75_value"))
            if cur_p25 is None or cur_p75 is None or cur_p75 < cur_p25:
                outcomes.append(_deep_missing(check_id, column_name, config.get("threshold")))
                continue
            if prev is None:
                outcomes.append(_deep_missing(
                    check_id, column_name, config.get("threshold"), reason="baseline_unavailable",
                ))
                continue
            prev_p25, prev_p75 = _number(prev.get("p25_value")), _number(prev.get("p75_value"))
            if prev_p25 is None or prev_p75 is None or prev_p75 < prev_p25:
                outcomes.append(_deep_missing(
                    check_id, column_name, config.get("threshold"), reason="baseline_unavailable",
                ))
                continue
            outcomes.append(_comparison_outcome(
                check_id, column=column_name,
                current_value=cur_p75 - cur_p25,
                previous_value=prev_p75 - prev_p25,
                config=config, label="IQR",
            ))
        elif check_id == "D7":
            threshold = _threshold(config)
            placeholder_fraction = _number(cur.get("placeholder_rate"))
            if threshold is None:
                outcomes.append(_deep_missing(check_id, column_name, config.get("threshold"), reason="invalid_threshold"))
            elif placeholder_fraction is None or not 0 <= placeholder_fraction <= 1:
                outcomes.append(_deep_missing(check_id, column_name, threshold))
            else:
                placeholder_pct = placeholder_fraction * 100.0
                breached = placeholder_pct > threshold
                outcomes.append(_check_outcome(
                    check_id,
                    status=_severity(config, placeholder_pct, threshold) if breached else "ok",
                    applicable=True,
                    evaluated=True,
                    message=(
                        f"{column_name}: placeholder rate {placeholder_pct:.2f}% "
                        f"(threshold: {threshold:g}%)"
                    ),
                    value=round(placeholder_pct, 4), threshold=threshold, column=column_name,
                ))

    return outcomes


def summarize_check_outcomes(outcomes: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    """Return truthful mutually-exclusive counters for configured outcomes."""

    summary = {
        "configured": len(outcomes),
        "passed": 0,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
    }
    for outcome in outcomes:
        status = outcome.get("status")
        if status == "ok" and outcome.get("evaluated"):
            summary["passed"] += 1
        elif status == "warning":
            summary["warnings"] += 1
        elif status == "critical":
            summary["critical"] += 1
        elif status == "error":
            summary["errors"] += 1
        else:
            summary["skipped"] += 1
            if status == "not_applicable" or not outcome.get("applicable", True):
                summary["not_applicable"] += 1
    return summary


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

    def count_value(value: Any) -> Optional[int]:
        numeric = _number(value)
        if numeric is None or numeric < 0 or not numeric.is_integer():
            return None
        return int(numeric)

    def rate_value(value: Any) -> Optional[float]:
        numeric = _number(value)
        return numeric if numeric is not None and 0 <= numeric <= 100 else None

    # ── A1: Row count spike/drop ──────────────────────────────────
    t1_conf = config_checks.get("T1", {})
    if t1_conf.get("enabled"):
        cur_total = count_value(current.get("total"))
        prev_total = count_value(previous.get("total"))
        threshold = _threshold(t1_conf)
        if cur_total is not None and prev_total is not None and prev_total > 0 and threshold is not None:
            delta_pct = ((cur_total - prev_total) / prev_total) * 100
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
            cur_nr = rate_value(cur_col.get("null_rate"))
            prev_nr = rate_value(prev_col.get("null_rate"))
            threshold = _threshold(c1_conf)
            if cur_nr is not None and prev_nr is not None and threshold is not None:
                delta_pp = cur_nr - prev_nr
            else:
                delta_pp = None
            if delta_pp is not None and delta_pp > threshold:
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
        if (
            c6_conf.get("enabled")
            and cur_col.get("category") == "numeric"
            and prev_col.get("category") == "numeric"
            and bool(cur_col.get("moments_certified", True))
            and bool(prev_col.get("moments_certified", True))
        ):
            cur_avg = _number(cur_col.get("avg"))
            prev_avg = _number(prev_col.get("avg"))
            prev_stddev = _number(prev_col.get("stddev"))
            threshold = _threshold(c6_conf)
            if (
                cur_avg is not None
                and prev_avg is not None
                and prev_stddev is not None
                and prev_stddev > 0
                and threshold is not None
            ):
                z_score = abs(cur_avg - prev_avg) / prev_stddev
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
            cur_dist = count_value(cur_col.get("distinct"))
            prev_dist = count_value(prev_col.get("distinct"))
            threshold = _threshold(c2_conf)
            if (
                cur_dist is not None
                and prev_dist is not None
                and prev_dist > 0
                and threshold is not None
            ):
                delta_pct = ((cur_dist - prev_dist) / prev_dist) * 100
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
        if (
            c3_conf.get("enabled")
            and cur_col.get("category") in ("numeric", "date")
            and prev_col.get("category") == cur_col.get("category")
        ):
            cur_min = cur_col.get("min")
            cur_max = cur_col.get("max")
            prev_min = prev_col.get("min")
            prev_max = prev_col.get("max")

            category = cur_col.get("category")
            if category == "numeric":
                cur_min, cur_max = _exact_number(cur_min), _exact_number(cur_max)
                prev_min, prev_max = _exact_number(prev_min), _exact_number(prev_max)
            else:
                cur_min, cur_max = _parse_instant(cur_min), _parse_instant(cur_max)
                prev_min, prev_max = _parse_instant(prev_min), _parse_instant(prev_max)

            breaches = []
            if cur_min is not None and prev_min is not None:
                if cur_min < prev_min:
                    breaches.append(f"new min {cur_min} < previous {prev_min}")

            if cur_max is not None and prev_max is not None:
                if cur_max > prev_max:
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
                cur_rate = rate_value(cur_col.get(rate_key))
                prev_rate = rate_value(prev_col.get(rate_key))
                threshold = _threshold(c5_conf)
                if cur_rate is None or prev_rate is None or threshold is None:
                    continue
                delta = cur_rate - prev_rate
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
    if not t3_conf.get("enabled") or previous_schema is None:
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
