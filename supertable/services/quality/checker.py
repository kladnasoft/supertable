# supertable/reflection/quality/checker.py
"""
Data Quality checker — generates and executes SQL for quick and deep profiles.

Quick profile:  single SQL per table, all columns, lightweight metrics
Deep profile:   one SQL per column, full stats (Docu Genie templates adapted for DuckDB)

Results are written to:
  - Redis  dq:latest:{table}  (fast UI cache)
  - Redis  dq:latest:{table}:{column}  (per-column cache)
  - Parquet __data_quality__ table  (history + MCP queryable)
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Column type classification (mirrors tables.html ccTypePillCls)
# ──────────────────────────────────────────────────────────────────────

def _col_category(col_type: str) -> str:
    t = (col_type or "").lower()
    # Check string-like types first to avoid substring false positives
    # (e.g. "VARCHAR" contains no numeric substring, but ordering matters
    #  for less common types).
    if re.search(r"(char|string|text|varchar)", t):
        return "string"
    if re.search(r"(date|time|timestamp|interval)", t):
        return "date"
    if re.search(r"(bool)", t):
        return "bool"
    if re.search(r"(int|bigint|smallint|long|decimal|float|double|numeric|real)", t):
        return "numeric"
    return "other"


def _safe_alias(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ──────────────────────────────────────────────────────────────────────
# Quick profile SQL builder
# ──────────────────────────────────────────────────────────────────────

def build_quick_sql(
    table_fqn: str,
    columns: List[Tuple[str, str]],
    incremental_column: Optional[str] = None,
    last_check_ts: Optional[str] = None,
) -> str:
    """
    Build a single aggregation SQL that profiles all columns at once.

    columns: list of (column_name, column_type) tuples
    Returns DuckDB-compatible SQL.
    """
    parts = ["COUNT(*) AS __total"]

    for col_name, col_type in columns:
        if col_name.startswith("_sys_"):
            continue
        q = _quote(col_name)
        s = _safe_alias(col_name)
        cat = _col_category(col_type)

        # Universal: present count + distinct count
        parts.append(f"COUNT({q}) AS __present_{s}")
        parts.append(f"COUNT(DISTINCT {q}) AS __distinct_{s}")

        # Numeric / date: min, max
        if cat in ("numeric", "date"):
            if cat == "numeric":
                # TRY_CAST guards against schema-vs-Parquet type mismatches
                parts.append(f"MIN(TRY_CAST({q} AS DOUBLE)) AS __min_{s}")
                parts.append(f"MAX(TRY_CAST({q} AS DOUBLE)) AS __max_{s}")
            else:
                parts.append(f"MIN({q}) AS __min_{s}")
                parts.append(f"MAX({q}) AS __max_{s}")

        # Numeric only: mean, stddev, zero count, negative count
        if cat == "numeric":
            parts.append(f"AVG(TRY_CAST({q} AS DOUBLE)) AS __avg_{s}")
            parts.append(f"STDDEV(TRY_CAST({q} AS DOUBLE)) AS __stddev_{s}")
            parts.append(f"COUNT(*) FILTER (WHERE TRY_CAST({q} AS DOUBLE) = 0) AS __zero_{s}")
            parts.append(f"COUNT(*) FILTER (WHERE TRY_CAST({q} AS DOUBLE) < 0) AS __neg_{s}")

    select_clause = ",\n  ".join(parts)
    sql = f"SELECT\n  {select_clause}\nFROM {table_fqn}"

    # Incremental scope
    if incremental_column and last_check_ts:
        q_inc = _quote(incremental_column)
        sql += f"\nWHERE {q_inc} > '{last_check_ts}'"

    return sql


def parse_quick_result(
    row: Dict[str, Any],
    columns: List[Tuple[str, str]],
) -> Dict[str, Any]:
    """Parse the single-row result of a quick SQL into structured per-column data."""
    total = row.get("__total", 0) or 0
    col_results = {}

    for col_name, col_type in columns:
        if col_name.startswith("_sys_"):
            continue
        s = _safe_alias(col_name)
        cat = _col_category(col_type)

        present = row.get(f"__present_{s}", 0) or 0
        distinct = row.get(f"__distinct_{s}", 0) or 0
        null_count = total - present
        null_rate = (null_count / total * 100) if total > 0 else 0
        uniqueness = (distinct / present * 100) if present > 0 else 0

        entry: Dict[str, Any] = {
            "column_name": col_name,
            "column_type": col_type,
            "category": cat,
            "total": total,
            "present": present,
            "null_count": null_count,
            "null_rate": round(null_rate, 2),
            "distinct": distinct,
            "uniqueness": round(uniqueness, 2),
        }

        if cat in ("numeric", "date"):
            entry["min"] = row.get(f"__min_{s}")
            entry["max"] = row.get(f"__max_{s}")

        if cat == "numeric":
            entry["avg"] = _safe_round(row.get(f"__avg_{s}"), 4)
            entry["stddev"] = _safe_round(row.get(f"__stddev_{s}"), 4)
            zero = row.get(f"__zero_{s}", 0) or 0
            neg = row.get(f"__neg_{s}", 0) or 0
            entry["zero_rate"] = round(zero / total * 100, 2) if total > 0 else 0
            entry["negative_rate"] = round(neg / total * 100, 2) if total > 0 else 0

        col_results[col_name] = entry

    return {"total": total, "columns": col_results}


def _safe_round(val: Any, decimals: int) -> Any:
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return val


# ──────────────────────────────────────────────────────────────────────
# Deep profile SQL templates (adapted from Docu Genie for DuckDB)
# ──────────────────────────────────────────────────────────────────────

def build_deep_string_sql(table_fqn: str, column: str) -> str:
    q = _quote(column)
    return f"""
WITH
  stats AS (
    SELECT
      COUNT(*)                                         AS total_rows,
      COUNT({q})                                       AS non_nulls,
      COUNT(DISTINCT {q})                              AS distinct_vals,
      AVG(LENGTH({q}))                                 AS avg_length,
      VARIANCE(LENGTH({q}))                            AS var_length,
      MIN(LENGTH({q})) FILTER (WHERE {q} IS NOT NULL)  AS min_length,
      MAX(LENGTH({q})) FILTER (WHERE {q} IS NOT NULL)  AS max_length,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH({q})) AS median_length,
      COUNT(*) FILTER (
        WHERE lower({q}) IN ('', 'n/a', 'unknown', 'undef', 'none', 'null', '-', 'tbd', 'na', 'not available')
      )::float / NULLIF(COUNT(*), 0)                   AS placeholder_rate
    FROM {table_fqn}
  ),
  freq_stats AS (
    SELECT
      SUM(CASE WHEN freq = 1 THEN 1 ELSE 0 END)                      AS unique_count,
      -SUM((freq::float / total_nn) * LOG2(freq::float / total_nn))   AS shannon_entropy,
      SUM(freq) FILTER (WHERE rnk <= 10)                               AS topx_count
    FROM (
      SELECT
        {q},
        COUNT(*)                          AS freq,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk,
        SUM(COUNT(*)) OVER ()             AS total_nn
      FROM {table_fqn}
      WHERE {q} IS NOT NULL
      GROUP BY {q}
    ) fq
  ),
  topx_json AS (
    SELECT list({{'value': {q}, 'freq': freq}}) AS topx_values
    FROM (
      SELECT {q}, COUNT(*) AS freq
      FROM {table_fqn} WHERE {q} IS NOT NULL
      GROUP BY {q} ORDER BY freq DESC LIMIT 10
    ) t
  ),
  histogram AS (
    SELECT list({{'bucket_id': bucket_id, 'bucket_min': bucket_min, 'bucket_max': bucket_max, 'freq': freq}}) AS buckets
    FROM (
      SELECT bucket_id, MIN({q}) AS bucket_min, MAX({q}) AS bucket_max, COUNT(*) AS freq
      FROM (
        SELECT {q}, NTILE(10) OVER (ORDER BY {q}) AS bucket_id
        FROM {table_fqn} WHERE {q} IS NOT NULL
      ) ordered
      GROUP BY bucket_id ORDER BY bucket_id
    ) t
  )
SELECT
  s.total_rows, s.non_nulls, s.distinct_vals,
  ROUND(s.avg_length, 4) AS avg_length,
  ROUND(s.var_length, 6) AS var_length,
  s.min_length, s.max_length, s.median_length,
  fs.shannon_entropy,
  fs.unique_count::float / NULLIF(s.non_nulls, 0) AS uniqueness,
  ROUND(fs.topx_count * 100.0 / NULLIF(s.non_nulls, 0), 2) AS topx_coverage_pct,
  s.placeholder_rate,
  tj.topx_values,
  h.buckets
FROM stats s
CROSS JOIN freq_stats fs
CROSS JOIN topx_json tj
CROSS JOIN histogram h
"""


def build_deep_numeric_sql(table_fqn: str, column: str) -> str:
    q = _quote(column)
    return f"""
WITH
  stats AS (
    SELECT
      COUNT(*)                                         AS total_rows,
      COUNT({q})                                       AS non_nulls,
      COUNT(DISTINCT {q})                              AS distinct_vals,
      AVG({q})                                         AS avg_value,
      VARIANCE({q})                                    AS var_value,
      STDDEV({q})                                      AS stddev_value,
      MIN({q}) FILTER (WHERE {q} IS NOT NULL)          AS min_value,
      MAX({q}) FILTER (WHERE {q} IS NOT NULL)          AS max_value,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {q})  AS median_value,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {q}) AS p25_value,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {q}) AS p75_value,
      COUNT(*) FILTER (WHERE {q} = 0 OR {q} IS NULL)::float / NULLIF(COUNT(*), 0) AS zero_or_null_rate,
      COUNT(*) FILTER (WHERE {q} < 0)::float / NULLIF(COUNT(*), 0)                AS negative_rate
    FROM {table_fqn}
  ),
  freq_stats AS (
    SELECT
      SUM(CASE WHEN freq = 1 THEN 1 ELSE 0 END)                      AS unique_count,
      -SUM((freq::float / total_nn) * LOG2(freq::float / total_nn))   AS shannon_entropy,
      SUM(freq) FILTER (WHERE rnk <= 10)                               AS topx_count
    FROM (
      SELECT
        {q},
        COUNT(*)                          AS freq,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk,
        SUM(COUNT(*)) OVER ()             AS total_nn
      FROM {table_fqn}
      WHERE {q} IS NOT NULL
      GROUP BY {q}
    ) fq
  ),
  topx_json AS (
    SELECT list({{'value': {q}, 'freq': freq}}) AS topx_values
    FROM (
      SELECT {q}, COUNT(*) AS freq
      FROM {table_fqn} WHERE {q} IS NOT NULL
      GROUP BY {q} ORDER BY freq DESC LIMIT 10
    ) t
  ),
  histogram AS (
    SELECT list({{'bucket_id': bucket_id, 'bucket_min': bucket_min, 'bucket_max': bucket_max, 'freq': freq}}) AS buckets
    FROM (
      SELECT bucket_id, MIN({q}) AS bucket_min, MAX({q}) AS bucket_max, COUNT(*) AS freq
      FROM (
        SELECT {q}, NTILE(10) OVER (ORDER BY {q}) AS bucket_id
        FROM {table_fqn} WHERE {q} IS NOT NULL
      ) ordered
      GROUP BY bucket_id ORDER BY bucket_id
    ) t
  )
SELECT
  s.total_rows, s.non_nulls, s.distinct_vals,
  ROUND(s.avg_value::numeric, 4) AS avg_value,
  ROUND(s.var_value::numeric, 4) AS var_value,
  ROUND(s.stddev_value::numeric, 4) AS stddev_value,
  s.min_value, s.max_value, s.median_value, s.p25_value, s.p75_value,
  fs.shannon_entropy,
  fs.unique_count::float / NULLIF(s.non_nulls, 0) AS uniqueness,
  ROUND(fs.topx_count * 100.0 / NULLIF(s.non_nulls, 0), 2) AS topx_coverage_pct,
  s.zero_or_null_rate, s.negative_rate,
  tj.topx_values,
  h.buckets
FROM stats s
CROSS JOIN freq_stats fs
CROSS JOIN topx_json tj
CROSS JOIN histogram h
"""


# ──────────────────────────────────────────────────────────────────────
# Custom rule SQL builder
# ──────────────────────────────────────────────────────────────────────

def build_custom_rule_sql(rule: Dict[str, Any], table_fqn: str) -> Optional[str]:
    """Generate SQL for a user-defined custom rule. Returns None if rule_type unknown."""
    rt = rule.get("rule_type", "")
    col = rule.get("column_name")
    threshold = rule.get("threshold")

    if rt == "column_min" and col and threshold is not None:
        q = _quote(col)
        return f"SELECT COUNT(*) AS violations FROM {table_fqn} WHERE {q} < {threshold}"

    if rt == "column_max" and col and threshold is not None:
        q = _quote(col)
        return f"SELECT COUNT(*) AS violations FROM {table_fqn} WHERE {q} > {threshold}"

    if rt == "null_rate_max" and col and threshold is not None:
        q = _quote(col)
        return (
            f"SELECT ROUND((COUNT(*) - COUNT({q})) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_rate "
            f"FROM {table_fqn}"
        )

    if rt == "row_count_min" and threshold is not None:
        return f"SELECT COUNT(*) AS row_count FROM {table_fqn}"

    if rt == "distinct_in" and col:
        # rule["expected_values"] should be a list of allowed values
        expected = rule.get("expected_values", [])
        if not expected:
            return None
        q = _quote(col)
        vals = ", ".join(f"'{v}'" for v in expected)
        return (
            f"SELECT DISTINCT {q} AS unexpected_value FROM {table_fqn} "
            f"WHERE {q} IS NOT NULL AND {q} NOT IN ({vals})"
        )

    if rt == "custom_sql":
        return rule.get("sql")

    return None


def evaluate_custom_rule(rule: Dict[str, Any], result: Any) -> Dict[str, Any]:
    """Evaluate a custom rule result and return status + detail."""
    rt = rule.get("rule_type", "")
    threshold = rule.get("threshold")
    severity = rule.get("severity", "warning")

    if rt in ("column_min", "column_max"):
        violations = 0
        if isinstance(result, list) and result:
            violations = result[0].get("violations", 0) or 0
        elif isinstance(result, dict):
            violations = result.get("violations", 0) or 0
        status = "ok" if violations == 0 else severity
        return {
            "status": status,
            "value": violations,
            "detail": f"{violations} rows violate rule" if violations > 0 else "All rows pass",
        }

    if rt == "null_rate_max":
        null_rate = 0
        if isinstance(result, list) and result:
            null_rate = result[0].get("null_rate", 0) or 0
        elif isinstance(result, dict):
            null_rate = result.get("null_rate", 0) or 0
        status = "ok" if null_rate <= threshold else severity
        return {
            "status": status,
            "value": null_rate,
            "detail": f"NULL rate: {null_rate}% (threshold: {threshold}%)",
        }

    if rt == "row_count_min":
        row_count = 0
        if isinstance(result, list) and result:
            row_count = result[0].get("row_count", 0) or 0
        elif isinstance(result, dict):
            row_count = result.get("row_count", 0) or 0
        status = "ok" if row_count >= threshold else severity
        return {
            "status": status,
            "value": row_count,
            "detail": f"Row count: {row_count} (minimum: {threshold})",
        }

    if rt == "distinct_in":
        unexpected = []
        if isinstance(result, list):
            unexpected = [r.get("unexpected_value") for r in result if r.get("unexpected_value")]
        status = "ok" if not unexpected else severity
        return {
            "status": status,
            "value": len(unexpected),
            "detail": f"Unexpected values: {unexpected[:5]}" if unexpected else "All values expected",
        }

    if rt == "custom_sql":
        # User defines threshold as max allowed value of first column in first row
        val = 0
        if isinstance(result, list) and result:
            first_row = result[0]
            val = list(first_row.values())[0] if first_row else 0
        elif isinstance(result, dict):
            val = list(result.values())[0] if result else 0
        if threshold is not None:
            status = "ok" if val <= threshold else severity
        else:
            status = "ok"
        return {
            "status": status,
            "value": val,
            "detail": f"Result: {val}" + (f" (threshold: {threshold})" if threshold is not None else ""),
        }

    return {"status": "ok", "value": None, "detail": "Unknown rule type"}


# ──────────────────────────────────────────────────────────────────────
# Quality score calculator
# ──────────────────────────────────────────────────────────────────────

def compute_quality_score(
    col_results: Dict[str, Dict[str, Any]],
    anomalies: List[Dict[str, Any]],
) -> int:
    """Compute a 0-100 quality score from column results and anomalies."""
    if not col_results:
        return 0

    # Start at 100, deduct points
    score = 100.0
    n_cols = len(col_results)

    # Deduct for NULL rates
    avg_completeness = 0
    for col in col_results.values():
        avg_completeness += (100 - col.get("null_rate", 0))
    avg_completeness /= max(n_cols, 1)
    # If avg completeness is 95%, deduct 5 points
    score -= max(0, 100 - avg_completeness)

    # Deduct for anomalies
    for a in anomalies:
        if a.get("severity") == "critical":
            score -= 10
        elif a.get("severity") == "warning":
            score -= 5

    return max(0, min(100, int(round(score))))
