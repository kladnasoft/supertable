# supertable/quality/checker.py
"""
Data Quality checker — generates and executes SQL for quick and deep profiles.

Quick profile:  single SQL per table, all columns, lightweight metrics
Deep profile:   one SQL per column, full stats (Docu Genie templates adapted for DuckDB)

Results are written to:
  - Redis  quality:latest:{table}  (fast UI cache)
  - Redis  quality:latest:{table}:{column}  (per-column cache)
  - Parquet __data_quality__ table  (history + MCP queryable)
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Column type classification (mirrors tables.html ccTypePillCls)
# ──────────────────────────────────────────────────────────────────────

_NESTED_TYPE_PREFIXES = frozenset({"array", "list", "map", "struct", "union"})
_STRING_TYPES = frozenset({
    "bpchar", "categorical", "char", "character", "enum", "large_string",
    "large_utf8", "nvarchar", "str", "string", "text", "utf8", "varchar",
})
_DATE_TYPES = frozenset({
    # Only absolute instants belong here.  TIME, DURATION, and INTERVAL cannot
    # be normalised by the C3 instant comparator and are deliberately treated
    # as unsupported instead of producing a baseline that can never compare.
    "date", "datetime", "timestamp", "timestamp_ntz", "timestamptz",
})
_BOOL_TYPES = frozenset({"bool", "boolean"})
_NUMERIC_TYPES = frozenset({
    "bigint", "bignum", "byte", "decimal", "double", "float", "float4", "float8",
    "float16", "float32", "float64", "half_float", "halffloat",
    "hugeint", "int", "int1", "int2", "int4", "int8", "int16", "int32",
    "int64", "integer", "long", "number", "numeric", "real", "short",
    "signed", "smallint", "tinyint", "ubigint", "uhugeint", "uint",
    "uint8", "uint16", "uint32", "uint64", "uinteger", "unsigned",
    "usmallint", "utinyint",
})
_FLOAT_NUMERIC_TYPES = frozenset({
    "double", "float", "float4", "float8", "float16", "float32", "float64",
    "half_float", "halffloat", "real",
})
_WIDE_EXACT_NUMERIC_TYPES = frozenset({
    # DuckDB's fetchdf converts these scalar values to float64.  Their extrema
    # are projected as text below so exact C3 boundaries survive that API.
    "bignum", "decimal", "hugeint", "number", "numeric", "uhugeint",
})
_MAX_CERTIFIED_INTEGER_MOMENT_MAGNITUDE = 1 << 52


def _top_level_type(col_type: str) -> str:
    """Return the normalised *outermost* SQL/Arrow type token.

    Type classification is intentionally exact.  Searching the complete type
    string for words such as ``int`` misclassifies ``LIST<INT64>`` and
    ``STRUCT(id INTEGER)`` as scalar numeric columns.  The quality profiler
    would then generate scalar casts/aggregates for nested values and fail the
    entire profile.  Arrays written as ``INTEGER[]`` are handled before the
    scalar token is extracted for the same reason.
    """

    text = str(col_type or "").strip().lower()
    if not text:
        return ""
    if re.search(r"(?:\[\s*\d*\s*\])+\s*$", text):
        return "array"
    match = re.match(r"([a-z_][a-z0-9_]*)", text)
    if not match:
        return ""
    token = match.group(1)
    if token in _NESTED_TYPE_PREFIXES:
        return token
    # Multi-word temporal spellings all start with one of these exact tokens
    # (TIMESTAMP WITH TIME ZONE, TIME WITHOUT TIME ZONE).
    return token


def _col_category(col_type: str) -> str:
    token = _top_level_type(col_type)
    if token in _STRING_TYPES:
        return "string"
    if token in _DATE_TYPES:
        return "date"
    if token in _BOOL_TYPES:
        return "bool"
    if token in _NUMERIC_TYPES:
        return "numeric"
    return "other"


def _numeric_profile_expression(column_sql: str, col_type: Optional[str]) -> str:
    """Return a finite metric expression without narrowing exact numerics.

    IEEE non-finite values exist only for floating dtypes.  Integers and
    decimals therefore stay in their native DuckDB type; routing all numerics
    through DOUBLE collapses adjacent BIGINTs above 2**53 and precise DECIMAL
    values before MIN/MAX, grouping, and top-N are computed.

    ``None`` retains the historical floating-safe default for direct callers
    of the two-argument deep builder.  Scheduler callers always pass the
    authoritative schema type.
    """

    token = _top_level_type(col_type or "double")
    if token in _FLOAT_NUMERIC_TYPES:
        cast = f"TRY_CAST({column_sql} AS DOUBLE)"
        return f"CASE WHEN isfinite({cast}) THEN {cast} ELSE NULL END"
    return column_sql


def _preserved_scalar_sql(expression: str, col_type: Optional[str], alias: str) -> str:
    """Project wide exact scalar boundaries without fetchdf float coercion."""

    if _top_level_type(col_type or "") in _WIDE_EXACT_NUMERIC_TYPES:
        return f"CAST({expression} AS VARCHAR) AS {alias}"
    return f"{expression} AS {alias}"


def _numeric_moment_certification_sql(
    column_sql: str,
    col_type: Optional[str],
) -> str:
    """Return a SQL proof that DOUBLE-backed moments are trustworthy enough.

    DuckDB's AVG/STDDEV implementations return DOUBLE for integral/decimal
    sources.  Around 2**53 even adjacent BIGINTs can collapse to the same
    value and yield a zero standard deviation.  A conservative 2**52 bound
    retains ordinary integer profiling with room for half-step means.  Wide
    exact/decimal types are not certified because their scale/range cannot be
    proven from the type token alone.  Floating inputs already have native
    IEEE precision, so finite filtering is their complete contract.
    """

    token = _top_level_type(col_type or "double")
    if token in _FLOAT_NUMERIC_TYPES:
        return "TRUE"
    if token in _WIDE_EXACT_NUMERIC_TYPES:
        return "FALSE"
    bound = _MAX_CERTIFIED_INTEGER_MOMENT_MAGNITUDE
    return (
        f"COALESCE(MIN({column_sql}) >= -{bound} "
        f"AND MAX({column_sql}) <= {bound}, TRUE)"
    )


def _safe_alias(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _profile_columns(
    columns: Iterable[Tuple[str, str]],
) -> List[Tuple[str, str, str]]:
    """Attach deterministic, collision-free SQL aliases to visible columns."""

    profiled: List[Tuple[str, str, str]] = []
    seen: set[str] = set()
    for name, col_type in filter_visible_columns(columns):
        base = _safe_alias(name) or "column"
        alias = base
        if alias.casefold() in seen:
            suffix = hashlib.sha256(name.encode("utf-8")).hexdigest()[:10]
            alias = f"{base}_{suffix}"
            counter = 2
            while alias.casefold() in seen:
                alias = f"{base}_{suffix}_{counter}"
                counter += 1
        seen.add(alias.casefold())
        profiled.append((name, col_type, alias))
    return profiled


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


_SIMPLE_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_quality_table_name(
    table_name: Any,
    *,
    super_name: Optional[str] = None,
    allow_wildcard: bool = False,
) -> ValidationResult:
    """Validate a rule table scope against DataWriter's name contract."""

    if table_name == "*":
        if allow_wildcard:
            return ValidationResult(True)
        return ValidationResult(
            False, "wildcard_table", "this quality rule cannot target wildcard table scope",
        )
    if not isinstance(table_name, str) or not table_name:
        return ValidationResult(False, "table_name", "quality rule requires table_name")
    if len(table_name) > 128 or _SIMPLE_TABLE_NAME_RE.fullmatch(table_name) is None:
        return ValidationResult(
            False,
            "table_name",
            "quality rule table_name must start with a letter/underscore and "
            "contain only alphanumeric/underscore characters",
        )
    folded = table_name.casefold()
    if folded.startswith("__") and folded.endswith("__"):
        return ValidationResult(
            False, "system_table", "quality rules cannot target a system table",
        )
    if isinstance(super_name, str) and table_name == super_name:
        return ValidationResult(
            False, "table_name", "quality rule table_name cannot equal its supertable name",
        )
    return ValidationResult(True)


def quality_table_fqn(super_name: str, table_name: str) -> str:
    """Return a DuckDB-qualified name with each identifier quoted separately."""

    if not isinstance(super_name, str) or not super_name:
        raise ValueError("quality supertable name must be a non-empty string")
    validation = validate_quality_table_name(table_name, super_name=super_name)
    if not validation.valid:
        raise ValueError(f"invalid quality table ({validation.code}): {validation.message}")
    return f"{_quote(super_name)}.{_quote(table_name)}"


# System columns used internally by the merge-on-read path.  They can appear in
# physical/legacy schemas, but the public read view strips all five.  A quality
# query that selects one through that view therefore fails at bind time.  Keep
# this SQL-builder dependency-light by mirroring the literals rather than
# importing engine_common; ``test_quality_checker`` seals this set against the
# canonical engine constants so future additions cannot drift silently.
SYSTEM_COLUMNS = frozenset({
    "__rowid__",
    "__timestamp__",
    "__file__",
    "__supertable_source_file__",
    "__supertable_scan_filename__",
})


def is_profilable_column(name: str) -> bool:
    """Return True for a column the DQ profiler may legitimately select.

    Excludes ``_sys_``-prefixed internal columns and the read-view-hidden
    system columns in :data:`SYSTEM_COLUMNS`: these can be present in stored or
    legacy schemas but are not exposed by the read path the profiler queries,
    so profiling them produces SQL that cannot bind.
    """
    if not isinstance(name, str) or not name:
        return False
    folded = name.casefold()
    return not folded.startswith("_sys_") and folded not in SYSTEM_COLUMNS


def filter_visible_columns(
    columns: Iterable[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    """Return the public logical schema in input order.

    This is the single boundary used by quick/deep/incremental/custom checks;
    callers should not independently reconstruct the hidden-column rule.
    """

    return [(name, col_type) for name, col_type in columns if is_profilable_column(name)]


@dataclass(frozen=True)
class ValidationResult:
    """Machine-readable, non-throwing validation result for DQ configuration."""

    valid: bool
    code: str = "ok"
    message: str = ""


def _column_type_map(columns: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    return {name: col_type for name, col_type in filter_visible_columns(columns)}


def validate_incremental_column(
    column: Optional[str],
    columns: Iterable[Tuple[str, str]],
) -> ValidationResult:
    """Validate the public temporal watermark used by legacy incremental DQ.

    The current incremental SQL compares the configured column with the last
    ISO check timestamp.  Consequently only a visible DATE/TIME/TIMESTAMP
    column is semantically valid.  Invalid configuration must cause a safe
    full logical scan, never a query against a hidden storage column.
    """

    if column is None or (isinstance(column, str) and not column.strip()):
        return ValidationResult(True, "disabled", "incremental profiling is disabled")
    if not isinstance(column, str):
        return ValidationResult(False, "invalid_column", "incremental_column must be a string")
    if not is_profilable_column(column):
        return ValidationResult(
            False,
            "hidden_column",
            f"incremental column {column!r} is not part of the public read schema",
        )
    visible = _column_type_map(columns)
    if column not in visible:
        return ValidationResult(
            False,
            "missing_column",
            f"incremental column {column!r} does not exist in the public read schema",
        )
    if _col_category(visible[column]) != "date":
        return ValidationResult(
            False,
            "unsupported_incremental_type",
            "incremental_column must be a public DATE/TIME/TIMESTAMP column",
        )
    return ValidationResult(True)


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
    all_columns = list(columns)
    parts = ["COUNT(*) AS __total"]

    for col_name, col_type, s in _profile_columns(all_columns):
        q = _quote(col_name)
        cat = _col_category(col_type)

        # Universal: present count + distinct count
        parts.append(f"COUNT({q}) AS __present_{s}")
        parts.append(f"COUNT(DISTINCT {q}) AS __distinct_{s}")

        # Numeric / date: min, max
        if cat in ("numeric", "date"):
            if cat == "numeric":
                metric_value = _numeric_profile_expression(q, col_type)
                parts.append(_preserved_scalar_sql(
                    f"MIN({metric_value})", col_type, f"__min_{s}",
                ))
                parts.append(_preserved_scalar_sql(
                    f"MAX({metric_value})", col_type, f"__max_{s}",
                ))
            else:
                parts.append(f"MIN({q}) AS __min_{s}")
                parts.append(f"MAX({q}) AS __max_{s}")

        # Numeric only: mean, stddev, zero count, negative count
        if cat == "numeric":
            moments_certified = _numeric_moment_certification_sql(q, col_type)
            parts.append(
                f"{moments_certified} AS __moments_certified_{s}"
            )
            parts.append(
                f"CASE WHEN {moments_certified} THEN AVG({metric_value}) "
                f"ELSE NULL END AS __avg_{s}"
            )
            parts.append(
                f"CASE WHEN {moments_certified} THEN STDDEV({metric_value}) "
                f"ELSE NULL END AS __stddev_{s}"
            )
            parts.append(f"COUNT(*) FILTER (WHERE {metric_value} = 0) AS __zero_{s}")
            parts.append(f"COUNT(*) FILTER (WHERE {metric_value} < 0) AS __neg_{s}")

    select_clause = ",\n  ".join(parts)
    sql = f"SELECT\n  {select_clause}\nFROM {table_fqn}"

    # Incremental scope
    if incremental_column and last_check_ts:
        validation = validate_incremental_column(incremental_column, all_columns)
        if not validation.valid:
            logger.warning(
                "[dq-checker] ignoring unsafe incremental scope: %s",
                validation.message,
            )
            return sql
        q_inc = _quote(incremental_column)
        timestamp_literal = str(last_check_ts).replace("'", "''")
        sql += f"\nWHERE {q_inc} > '{timestamp_literal}'"

    return sql


def parse_quick_result(
    row: Dict[str, Any],
    columns: List[Tuple[str, str]],
) -> Dict[str, Any]:
    """Parse the single-row result of a quick SQL into structured per-column data."""
    if "__total" not in row or row.get("__total") is None:
        raise ValueError("quick profile result is missing __total")
    total = _count_value(row.get("__total"), "__total")
    col_results = {}

    for col_name, col_type, s in _profile_columns(columns):
        cat = _col_category(col_type)

        present_key = f"__present_{s}"
        distinct_key = f"__distinct_{s}"
        if present_key not in row or distinct_key not in row:
            raise ValueError(f"quick profile result is missing metrics for {col_name!r}")
        present = _count_value(row.get(present_key), present_key)
        distinct = _count_value(row.get(distinct_key), distinct_key)
        if present > total or distinct > present:
            raise ValueError(f"quick profile counts are inconsistent for {col_name!r}")
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
            for metric in ("min", "max"):
                key = f"__{metric}_{s}"
                if key not in row:
                    raise ValueError(f"quick profile result is missing {key}")
            entry["min"] = row.get(f"__min_{s}")
            entry["max"] = row.get(f"__max_{s}")

        if cat == "numeric":
            for metric in ("avg", "stddev", "zero", "neg"):
                key = f"__{metric}_{s}"
                if key not in row:
                    raise ValueError(f"quick profile result is missing {key}")
            certified_value = row.get(f"__moments_certified_{s}", True)
            if certified_value is None:
                raise ValueError(
                    f"quick profile result has invalid moment certification for {col_name!r}"
                )
            entry["moments_certified"] = bool(certified_value)
            entry["avg"] = (
                _safe_round(row.get(f"__avg_{s}"), 4)
                if entry["moments_certified"] else None
            )
            entry["stddev"] = (
                _safe_round(row.get(f"__stddev_{s}"), 4)
                if entry["moments_certified"] else None
            )
            zero = _count_value(row.get(f"__zero_{s}"), f"__zero_{s}")
            neg = _count_value(row.get(f"__neg_{s}"), f"__neg_{s}")
            if zero > total or neg > total:
                raise ValueError(f"quick profile numeric counts are inconsistent for {col_name!r}")
            entry["zero_rate"] = round(zero / total * 100, 2) if total > 0 else 0
            entry["negative_rate"] = round(neg / total * 100, 2) if total > 0 else 0

        col_results[col_name] = entry

    return {"total": total, "columns": col_results}


def _count_value(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        exact = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if not exact.is_finite() or exact < 0 or exact != exact.to_integral_value():
        raise ValueError(f"{field} must be a non-negative integer")
    return int(exact)


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

DEEP_TOP_VALUES_LIMIT = 10
DEEP_HISTOGRAM_BUCKET_LIMIT = 10
DEEP_STRING_PREVIEW_CHARS = 256

def build_deep_string_sql(table_fqn: str, column: str) -> str:
    q = _quote(column)
    # D3/D4 group and order by the complete value, but never return a complete
    # arbitrary-size cell.  The preview has a fixed character limit (and thus
    # at most 4x UTF-8 bytes); hash + lengths retain deterministic identity.
    preview_chars = DEEP_STRING_PREVIEW_CHARS
    top_limit = DEEP_TOP_VALUES_LIMIT
    bucket_limit = DEEP_HISTOGRAM_BUCKET_LIMIT
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
      SUM(freq) FILTER (WHERE rnk <= {top_limit})                      AS topx_count
    FROM (
      SELECT
        {q},
        COUNT(*)                          AS freq,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, {q} ASC) AS rnk,
        SUM(COUNT(*)) OVER ()             AS total_nn
      FROM {table_fqn}
      WHERE {q} IS NOT NULL
      GROUP BY {q}
    ) fq
  ),
  topx_json AS (
    SELECT list({{
      'value': LEFT({q}, {preview_chars}),
      'value_sha256': SHA256({q}),
      'value_char_length': LENGTH({q}),
      'value_byte_length': CAST(BIT_LENGTH({q}) / 8 AS BIGINT),
      'value_truncated': LENGTH({q}) > {preview_chars},
      'freq': freq
    }} ORDER BY freq DESC, {q} ASC) AS topx_values
    FROM (
      SELECT {q}, COUNT(*) AS freq
      FROM {table_fqn} WHERE {q} IS NOT NULL
      GROUP BY {q} ORDER BY freq DESC, {q} ASC LIMIT {top_limit}
    ) t
  ),
  histogram AS (
    SELECT list(
      {{
        'bucket_id': bucket_id,
        'bucket_min': LEFT(bucket_min, {preview_chars}),
        'bucket_min_sha256': SHA256(bucket_min),
        'bucket_min_char_length': LENGTH(bucket_min),
        'bucket_min_byte_length': CAST(BIT_LENGTH(bucket_min) / 8 AS BIGINT),
        'bucket_min_truncated': LENGTH(bucket_min) > {preview_chars},
        'bucket_max': LEFT(bucket_max, {preview_chars}),
        'bucket_max_sha256': SHA256(bucket_max),
        'bucket_max_char_length': LENGTH(bucket_max),
        'bucket_max_byte_length': CAST(BIT_LENGTH(bucket_max) / 8 AS BIGINT),
        'bucket_max_truncated': LENGTH(bucket_max) > {preview_chars},
        'freq': freq
      }}
      ORDER BY bucket_id
    ) AS buckets
    FROM (
      SELECT bucket_id, MIN({q}) AS bucket_min, MAX({q}) AS bucket_max, COUNT(*) AS freq
      FROM (
        SELECT {q}, NTILE({bucket_limit}) OVER (ORDER BY {q}) AS bucket_id
        FROM {table_fqn} WHERE {q} IS NOT NULL
      ) ordered
      GROUP BY bucket_id ORDER BY bucket_id LIMIT {bucket_limit}
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


def build_deep_numeric_sql(
    table_fqn: str,
    column: str,
    column_type: Optional[str] = None,
) -> str:
    """Build numeric distribution SQL without narrowing exact source values.

    ``column_type`` should be the logical schema dtype.  It is optional for
    compatibility with external callers; an omitted type uses the historical
    floating/non-finite-safe path.  The scheduler always supplies it.
    """

    q = _quote(column)
    top_limit = DEEP_TOP_VALUES_LIMIT
    bucket_limit = DEEP_HISTOGRAM_BUCKET_LIMIT
    finite = _numeric_profile_expression(q, column_type)
    moments_certified = _numeric_moment_certification_sql(q, column_type)
    min_select = _preserved_scalar_sql("s.min_value", column_type, "min_value")
    max_select = _preserved_scalar_sql("s.max_value", column_type, "max_value")
    median_select = _preserved_scalar_sql(
        "s.median_value", column_type, "median_value",
    )
    p25_select = _preserved_scalar_sql("s.p25_value", column_type, "p25_value")
    p75_select = _preserved_scalar_sql("s.p75_value", column_type, "p75_value")
    return f"""
WITH
  stats AS (
    SELECT
      COUNT(*)                                         AS total_rows,
      COUNT({finite})                                  AS non_nulls,
      COUNT(DISTINCT {finite})                         AS distinct_vals,
      {moments_certified}                              AS moments_certified,
      CASE WHEN {moments_certified} THEN AVG({finite}) ELSE NULL END AS avg_value,
      CASE WHEN {moments_certified} THEN VARIANCE({finite}) ELSE NULL END AS var_value,
      CASE WHEN {moments_certified} THEN STDDEV({finite}) ELSE NULL END AS stddev_value,
      MIN({finite})                                    AS min_value,
      MAX({finite})                                    AS max_value,
      CASE WHEN {moments_certified} THEN
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {finite}) ELSE NULL END AS median_value,
      CASE WHEN {moments_certified} THEN
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {finite}) ELSE NULL END AS p25_value,
      CASE WHEN {moments_certified} THEN
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {finite}) ELSE NULL END AS p75_value,
      COUNT(*) FILTER (WHERE {finite} = 0 OR {q} IS NULL)::float / NULLIF(COUNT(*), 0) AS zero_or_null_rate,
      COUNT(*) FILTER (WHERE {finite} < 0)::float / NULLIF(COUNT(*), 0)                AS negative_rate
    FROM {table_fqn}
  ),
  freq_stats AS (
    SELECT
      SUM(CASE WHEN freq = 1 THEN 1 ELSE 0 END)                      AS unique_count,
      -SUM((freq::float / total_nn) * LOG2(freq::float / total_nn))   AS shannon_entropy,
      SUM(freq) FILTER (WHERE rnk <= {top_limit})                      AS topx_count
    FROM (
      SELECT
        {finite} AS finite_value,
        COUNT(*)                          AS freq,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, finite_value ASC) AS rnk,
        SUM(COUNT(*)) OVER ()             AS total_nn
      FROM {table_fqn}
      WHERE {finite} IS NOT NULL
      GROUP BY finite_value
    ) fq
  ),
  topx_json AS (
    SELECT list(
      {{'value': finite_value, 'freq': freq}}
      ORDER BY freq DESC, finite_value ASC
    ) AS topx_values
    FROM (
      SELECT {finite} AS finite_value, COUNT(*) AS freq
      FROM {table_fqn} WHERE {finite} IS NOT NULL
      GROUP BY finite_value ORDER BY freq DESC, finite_value ASC LIMIT {top_limit}
    ) t
  ),
  histogram AS (
    SELECT list(
      {{'bucket_id': bucket_id, 'bucket_min': bucket_min, 'bucket_max': bucket_max, 'freq': freq}}
      ORDER BY bucket_id
    ) AS buckets
    FROM (
      SELECT bucket_id, MIN(finite_value) AS bucket_min, MAX(finite_value) AS bucket_max, COUNT(*) AS freq
      FROM (
        SELECT {finite} AS finite_value, NTILE({bucket_limit}) OVER (ORDER BY {finite}) AS bucket_id
        FROM {table_fqn} WHERE {finite} IS NOT NULL
      ) ordered
      GROUP BY bucket_id ORDER BY bucket_id LIMIT {bucket_limit}
    ) t
  )
SELECT
  s.total_rows, s.non_nulls, s.distinct_vals, s.moments_certified,
  ROUND(s.avg_value, 4) AS avg_value,
  ROUND(s.var_value, 4) AS var_value,
  ROUND(s.stddev_value, 4) AS stddev_value,
  {min_select}, {max_select}, {median_select}, {p25_select}, {p75_select},
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

_CUSTOM_RULE_TYPES = frozenset({
    "column_min", "column_max", "null_rate_max", "row_count_min",
    "distinct_in", "custom_sql",
})
_COLUMN_RULE_TYPES = frozenset({
    "column_min", "column_max", "null_rate_max", "distinct_in",
})
_CUSTOM_SQL_MAX_BYTES = 32 * 1024
DISTINCT_IN_MAX_EXPECTED_VALUES = 256
DISTINCT_IN_MAX_LITERAL_BYTES = 16 * 1024


class _ExpectedValuesLimitError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _finite_decimal(value: Any) -> Optional[Decimal]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _number_sql(value: Any) -> str:
    parsed = _finite_decimal(value)
    if parsed is None:
        raise ValueError("threshold must be a finite number")
    return format(parsed, "f")


def _literal_sql(value: Any) -> str:
    if value is None:
        raise ValueError("NULL is not a valid expected value")
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return _number_sql(value)
    if isinstance(value, (datetime,)):
        value = value.isoformat()
    if not isinstance(value, str):
        raise ValueError("expected values must be scalar strings, numbers, or booleans")
    return "'" + value.replace("'", "''") + "'"


def _distinct_in_literals(expected: Sequence[Any]) -> List[str]:
    """Compile a bounded list of SQL literals for ``distinct_in``.

    The limit is enforced on both cardinality and the UTF-8 bytes of the
    escaped SQL literal list.  It therefore bounds parser work and generated
    query size even when a legacy rule bypassed configuration ingress; the
    scheduler calls the same validator again immediately before execution.
    """

    if len(expected) > DISTINCT_IN_MAX_EXPECTED_VALUES:
        raise _ExpectedValuesLimitError(
            "expected_values_count",
            "distinct_in expected_values exceeds the "
            f"{DISTINCT_IN_MAX_EXPECTED_VALUES}-element limit",
        )

    literals: List[str] = []
    encoded_bytes = 0
    for value in expected:
        # Avoid rendering an already-obviously-oversized scalar.  Decimal's
        # fixed-point formatter can otherwise expand a compact exponent into
        # a very large string before the byte budget is checked.
        if isinstance(value, str) and len(value) > DISTINCT_IN_MAX_LITERAL_BYTES:
            raise _ExpectedValuesLimitError(
                "expected_values_size",
                "distinct_in expected_values exceeds the aggregate literal-byte limit",
            )
        if isinstance(value, int) and not isinstance(value, bool):
            if value.bit_length() > DISTINCT_IN_MAX_LITERAL_BYTES * 4:
                raise _ExpectedValuesLimitError(
                    "expected_values_size",
                    "distinct_in expected_values exceeds the aggregate literal-byte limit",
                )
        if isinstance(value, Decimal):
            decimal_tuple = value.as_tuple()
            if (
                len(decimal_tuple.digits) + abs(decimal_tuple.exponent) + 3
                > DISTINCT_IN_MAX_LITERAL_BYTES
            ):
                raise _ExpectedValuesLimitError(
                    "expected_values_size",
                    "distinct_in expected_values exceeds the aggregate literal-byte limit",
                )

        literal = _literal_sql(value)
        try:
            literal_bytes = len(literal.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ValueError("expected values must be valid UTF-8") from exc
        encoded_bytes += literal_bytes + (2 if literals else 0)
        if encoded_bytes > DISTINCT_IN_MAX_LITERAL_BYTES:
            raise _ExpectedValuesLimitError(
                "expected_values_size",
                "distinct_in expected_values exceeds the "
                f"{DISTINCT_IN_MAX_LITERAL_BYTES}-byte aggregate literal limit",
            )
        literals.append(literal)
    return literals


def _distinct_expected_value_compatible(value: Any, col_type: str) -> bool:
    """Return whether one allow-list literal has the column's certified type."""

    category = _col_category(col_type)
    if category == "string":
        return isinstance(value, str)
    if category == "numeric":
        return (
            not isinstance(value, bool)
            and isinstance(value, (int, float, Decimal))
            and _finite_decimal(value) is not None
        )
    if category == "bool":
        return isinstance(value, bool)
    if category == "date":
        token = _top_level_type(col_type)
        if isinstance(value, datetime):
            return token != "date"
        if isinstance(value, date):
            return token == "date"
        if not isinstance(value, str) or not value.strip():
            return False
        try:
            if token == "date":
                date.fromisoformat(value.strip())
            else:
                text = value.strip()
                datetime.fromisoformat(
                    text[:-1] + "+00:00" if text.endswith("Z") else text
                )
            return True
        except ValueError:
            return False
    return False


def _custom_sql_hidden_reference(sql: str) -> Optional[str]:
    """Return a referenced private column, using the real SQL AST when possible."""

    try:
        import sqlglot
        from sqlglot import exp

        statements = sqlglot.parse(sql, read="duckdb")
        if len(statements) != 1:
            return "multiple_statements"
        statement = statements[0]
        # A quality rule is observational.  Requiring a Query node excludes
        # COPY/ATTACH/PRAGMA/SET/DDL as well as mutations, even if DataReader
        # happens to reject one of them later.
        if not isinstance(statement, exp.Query):
            return "non_read_statement"
        for ref in statement.find_all(exp.Column):
            name = ref.name
            if not is_profilable_column(name):
                return name
        return None
    except Exception:
        # Do not pretend an unparseable expression is safe.  DataReader errors
        # must never be converted into a passing quality result.
        return "unparseable_sql"


def _custom_sql_scope_issue(
    sql: str,
    table_fqn: Optional[str],
) -> Optional[Tuple[str, str]]:
    """Certify the deliberately small, bounded custom-SQL language.

    Custom rules execute as ``superadmin`` and DuckDB currently has no hard
    per-statement timeout.  Merely proving that a statement mentions its own
    table is not enough: an otherwise innocent scalar function such as
    ``repeat('x', 1000000000)`` can consume effectively unbounded resources.

    The accepted language is therefore one non-grouped aggregate ``SELECT``
    over exactly one ordinary table.  Its aggregate state is bounded to
    COUNT/SUM/AVG/MIN/MAX, aggregate arguments are a direct column or ``*``,
    and filters/projections may only use a small allow-list of scalar
    operators, predicates, and literals.  The query consequently returns
    exactly one row and cannot synthesize a relation or amplified scalar.

    ``table_fqn=None`` skips only the table-name comparison.  Shape and
    resource certification still run for dependency-light callers.
    """

    try:
        import sqlglot
        from sqlglot import exp

        statement = sqlglot.parse_one(sql, read="duckdb")

        if isinstance(statement, exp.SetOperation):
            return (
                "set_operation_query",
                "custom_sql set operations are not allowed",
            )
        if not isinstance(statement, exp.Select):
            return (
                "select_query_required",
                "custom_sql must be a single aggregate SELECT",
            )

        with_clause = statement.args.get("with")
        if with_clause is not None:
            if with_clause.args.get("recursive") is True:
                return (
                    "recursive_query",
                    "custom_sql recursive CTEs are not allowed",
                )
            return ("cte_query", "custom_sql CTEs are not allowed")

        # A nested SELECT can amplify work even when the outer query mentions
        # the attached table only once (for example through a scalar subquery).
        if any(True for _ in statement.find_all(exp.Subquery)) or sum(
            1 for _ in statement.find_all(exp.Select)
        ) != 1:
            return ("subquery_query", "custom_sql subqueries are not allowed")
        if any(True for _ in statement.find_all(exp.SetOperation)):
            return (
                "set_operation_query",
                "custom_sql set operations are not allowed",
            )

        if statement.args.get("group") is not None:
            return (
                "grouped_query",
                "custom_sql GROUP BY is not allowed; the query must return one row",
            )
        if statement.args.get("having") is not None or statement.args.get("qualify") is not None:
            return (
                "output_cardinality",
                "custom_sql HAVING/QUALIFY may remove the required result row",
            )
        if any(True for _ in statement.find_all(exp.Window)):
            return ("window_query", "custom_sql window functions are not allowed")
        if any(True for _ in statement.find_all(exp.Order)):
            return ("ordered_query", "custom_sql ORDER BY is not allowed")
        if (
            statement.args.get("limit") is not None
            or statement.args.get("offset") is not None
        ):
            return ("limited_query", "custom_sql LIMIT/OFFSET is not allowed")
        if statement.args.get("distinct") is not None:
            return (
                "distinct_query",
                "custom_sql SELECT DISTINCT is not allowed",
            )
        if any(True for _ in statement.find_all(exp.Join)):
            return (
                "cross_table_scope",
                "custom_sql joins are not allowed for a table-scoped quality rule",
            )
        if any(True for _ in statement.find_all(exp.UDTF)) or any(
            True for _ in statement.find_all(exp.GenerateSeries)
        ):
            return (
                "row_generator_scope",
                "custom_sql row-generating functions are not allowed",
            )

        allowed_select_args = {"expressions", "from", "where"}
        unsupported_clauses = sorted(
            key
            for key, value in statement.args.items()
            if value is not None and key not in allowed_select_args
        )
        if unsupported_clauses:
            return (
                "unsupported_query_clause",
                "custom_sql contains unsupported SELECT clause(s): "
                + ", ".join(unsupported_clauses),
            )

        # Bound parser/executor work even inside the allow-listed grammar.
        if sum(1 for _ in statement.walk()) > 256:
            return (
                "query_complexity",
                "custom_sql exceeds the maximum expression complexity",
            )

        from_clause = statement.args.get("from")
        actual = from_clause.this if from_clause is not None else None
        if isinstance(actual, exp.Table) and not isinstance(actual.this, exp.Identifier):
            return (
                "table_function_scope",
                "custom_sql table functions are not allowed",
            )
        if not isinstance(actual, exp.Table) or not actual.name:
            return (
                "table_scope",
                "custom_sql must read the attached table exactly once",
            )

        unsupported_table_args = sorted(
            key
            for key, value in actual.args.items()
            if value is not None and key not in {"this", "db", "catalog", "alias"}
        )
        alias = actual.args.get("alias")
        if unsupported_table_args or (
            alias is not None and alias.args.get("columns")
        ):
            return (
                "unsupported_from_shape",
                "custom_sql table sampling, pivots, and column aliases are not allowed",
            )

        physical_tables = list(statement.find_all(exp.Table))
        if len(physical_tables) != 1 or physical_tables[0] is not actual:
            return (
                "table_scope",
                "custom_sql must read the attached table exactly once",
            )

        if (
            not is_profilable_column(actual.name)
            or (actual.name.startswith("__") and actual.name.endswith("__"))
        ):
            return ("system_table", "custom_sql cannot read a system table")

        if table_fqn is not None:
            expected_query = sqlglot.parse_one(
                f"SELECT * FROM {table_fqn}",
                read="duckdb",
            )
            expected = next(expected_query.find_all(exp.Table), None)
            if expected is None or not expected.name:
                return ("invalid_table_scope", "attached quality table is invalid")

            actual_parts = (
                str(actual.catalog or "").casefold(),
                str(actual.db or "").casefold(),
                actual.name.casefold(),
            )
            expected_parts = (
                str(expected.catalog or "").casefold(),
                str(expected.db or "").casefold(),
                expected.name.casefold(),
            )
            if actual_parts != expected_parts:
                return (
                    "table_scope",
                    f"custom_sql may only read its attached table {table_fqn!r}",
                )

        projections = statement.expressions
        if not projections or len(projections) > 16:
            return (
                "projection_shape",
                "custom_sql must project between one and sixteen aggregate values",
            )

        safe_aggregates = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        safe_scalar_nodes = (
            exp.Paren,
            exp.Neg,
            exp.Not,
            exp.Add,
            exp.Sub,
            exp.Mul,
            exp.Div,
            exp.Mod,
            exp.EQ,
            exp.NEQ,
            exp.GT,
            exp.GTE,
            exp.LT,
            exp.LTE,
            exp.And,
            exp.Or,
            exp.Is,
            exp.In,
            exp.Between,
            exp.Like,
            exp.ILike,
        )

        def expression_issue(
            node: Any,
            *,
            allow_aggregates: bool,
            allow_columns: bool,
        ) -> Tuple[Optional[Tuple[str, str]], bool]:
            """Return ``(issue, contains_aggregate)`` for an AST subtree."""

            if isinstance(node, exp.Alias):
                return expression_issue(
                    node.this,
                    allow_aggregates=allow_aggregates,
                    allow_columns=allow_columns,
                )
            if isinstance(node, safe_aggregates):
                if not allow_aggregates:
                    return (
                        ("aggregate_predicate", "custom_sql WHERE cannot contain aggregates"),
                        False,
                    )
                argument = node.this
                extra_arguments = node.args.get("expressions") or []
                is_plain_star = isinstance(argument, exp.Star) and not any(
                    value is not None for value in argument.args.values()
                )
                is_direct_column = isinstance(argument, exp.Column) and isinstance(
                    argument.this,
                    exp.Identifier,
                )
                if isinstance(argument, exp.Func):
                    return (
                        (
                            "unsupported_function",
                            "custom_sql aggregate arguments cannot call scalar functions",
                        ),
                        False,
                    )
                if extra_arguments or not (
                    is_direct_column
                    or (isinstance(node, exp.Count) and is_plain_star)
                ):
                    return (
                        (
                            "aggregate_argument",
                            "custom_sql aggregates require one direct column argument; "
                            "COUNT may use *",
                        ),
                        False,
                    )
                return (None, True)
            if isinstance(node, exp.AggFunc):
                return (
                    (
                        "unsupported_aggregate",
                        "custom_sql only supports COUNT, SUM, AVG, MIN, and MAX",
                    ),
                    False,
                )
            if isinstance(node, exp.Column):
                if allow_columns and isinstance(node.this, exp.Identifier):
                    return (None, False)
                return (
                    (
                        "non_aggregate_projection",
                        "custom_sql output columns must be inside a supported aggregate",
                    ),
                    False,
                )
            if isinstance(node, (exp.Literal, exp.Null, exp.Boolean)):
                if isinstance(node, exp.Literal) and len(str(node.this)) > 4096:
                    return (
                        ("literal_size", "custom_sql literal exceeds 4096 characters"),
                        False,
                    )
                return (None, False)
            if isinstance(node, safe_scalar_nodes):
                contains_aggregate = False
                for child in node.iter_expressions():
                    issue, child_has_aggregate = expression_issue(
                        child,
                        allow_aggregates=allow_aggregates,
                        allow_columns=allow_columns,
                    )
                    if issue is not None:
                        return (issue, False)
                    contains_aggregate = contains_aggregate or child_has_aggregate
                return (None, contains_aggregate)
            if isinstance(node, exp.Func):
                return (
                    (
                        "unsupported_function",
                        "custom_sql scalar functions are not allowed",
                    ),
                    False,
                )
            return (
                (
                    "unsupported_expression",
                    f"custom_sql expression {type(node).__name__} is not allowed",
                ),
                False,
            )

        contains_aggregate = False
        for projection in projections:
            issue, projection_has_aggregate = expression_issue(
                projection,
                allow_aggregates=True,
                allow_columns=False,
            )
            if issue is not None:
                return issue
            contains_aggregate = contains_aggregate or projection_has_aggregate
        if not contains_aggregate:
            return (
                "aggregate_query_required",
                "custom_sql must contain a supported aggregate",
            )

        where_clause = statement.args.get("where")
        if where_clause is not None:
            issue, _ = expression_issue(
                where_clause.this,
                allow_aggregates=False,
                allow_columns=True,
            )
            if issue is not None:
                return issue
        return None
    except Exception:
        return ("unparseable_sql", "custom_sql could not be scope-validated")


def _custom_sql_output_issue(sql: str) -> Optional[Tuple[str, str]]:
    """Certify the single, named numeric output consumed by the evaluator."""

    try:
        import sqlglot
        from sqlglot import exp

        statement = sqlglot.parse_one(sql, read="duckdb")
        projections = statement.expressions
        if len(projections) != 1:
            return (
                "projection_shape",
                "custom_sql must project exactly one aggregate value",
            )
        projection = projections[0]
        if (
            not isinstance(projection, exp.Alias)
            or str(projection.alias or "").casefold() != "violations"
        ):
            return (
                "projection_alias",
                "custom_sql output must have the explicit alias violations",
            )

        numeric_scalar_nodes = (
            exp.Paren, exp.Neg, exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod,
        )
        numeric_aggregates = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)

        def is_numeric_expression(node: Any) -> bool:
            if isinstance(node, numeric_aggregates):
                return True
            if isinstance(node, exp.Literal):
                return bool(node.is_number)
            if isinstance(node, numeric_scalar_nodes):
                children = list(node.iter_expressions())
                return bool(children) and all(is_numeric_expression(child) for child in children)
            return False

        if not is_numeric_expression(projection.this):
            return (
                "non_numeric_projection",
                "custom_sql violations must be a numeric aggregate expression",
            )
        return None
    except Exception:
        return ("unparseable_sql", "custom_sql output could not be validated")


def validate_custom_rule(
    rule: Mapping[str, Any],
    columns: Optional[Iterable[Tuple[str, str]]] = None,
    table_fqn: Optional[str] = None,
) -> ValidationResult:
    """Validate a rule against the public logical schema without executing it."""

    if not isinstance(rule, Mapping):
        return ValidationResult(False, "invalid_rule", "rule must be an object")
    rule_type = rule.get("rule_type")
    if rule_type not in _CUSTOM_RULE_TYPES:
        return ValidationResult(False, "unknown_rule_type", f"unknown rule type {rule_type!r}")

    if rule_type == "custom_sql":
        sql = rule.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            return ValidationResult(False, "missing_sql", "custom_sql rule requires non-empty SQL")
        try:
            sql_size = len(sql.encode("utf-8"))
        except UnicodeEncodeError:
            return ValidationResult(False, "query_size", "custom_sql must be valid UTF-8")
        if sql_size > _CUSTOM_SQL_MAX_BYTES:
            return ValidationResult(
                False,
                "query_size",
                f"custom_sql exceeds the {_CUSTOM_SQL_MAX_BYTES}-byte limit",
            )
        hidden = _custom_sql_hidden_reference(sql)
        if hidden == "multiple_statements":
            return ValidationResult(False, hidden, "custom_sql must contain exactly one statement")
        if hidden == "non_read_statement":
            return ValidationResult(False, hidden, "custom_sql must be a read-only query")
        if hidden == "unparseable_sql":
            return ValidationResult(False, hidden, "custom_sql could not be parsed as DuckDB SQL")
        if hidden:
            return ValidationResult(
                False,
                "hidden_column",
                f"custom_sql references private column {hidden!r}",
            )
        scope_issue = _custom_sql_scope_issue(sql, table_fqn)
        if scope_issue is not None:
            code, message = scope_issue
            return ValidationResult(False, code, message)
        if columns is not None:
            # Configuration ingress does not have a table schema, but the
            # scheduler always does.  Resolve every reference at that second
            # boundary so a stale/misspelled column becomes an explicit rule
            # outcome rather than a DuckDB binder error and retry loop.
            try:
                import sqlglot
                from sqlglot import exp

                statement = sqlglot.parse_one(sql, read="duckdb")
                table = next(statement.find_all(exp.Table), None)
                visible_names = {
                    name.casefold(): name
                    for name, _ in filter_visible_columns(columns)
                }
                visible_types = {
                    name.casefold(): col_type
                    for name, col_type in filter_visible_columns(columns)
                }
                table_alias = str(table.alias or "").casefold() if table is not None else ""
                table_name = str(table.name or "").casefold() if table is not None else ""
                table_db = str(table.db or "").casefold() if table is not None else ""
                table_catalog = str(table.catalog or "").casefold() if table is not None else ""
                for reference in statement.find_all(exp.Column):
                    if reference.name.casefold() not in visible_names:
                        return ValidationResult(
                            False,
                            "missing_column",
                            f"custom_sql column {reference.name!r} does not exist "
                            "in the public read schema",
                        )
                    reference_table = str(reference.table or "").casefold()
                    reference_db = str(reference.db or "").casefold()
                    reference_catalog = str(reference.catalog or "").casefold()
                    expected_qualifier = table_alias or table_name
                    if (
                        (reference_table and reference_table != expected_qualifier)
                        or (reference_db and reference_db != table_db)
                        or (reference_catalog and reference_catalog != table_catalog)
                    ):
                        return ValidationResult(
                            False,
                            "column_scope",
                            f"custom_sql column {reference.sql()!r} is not qualified "
                            "by its attached table",
                        )
                # COUNT always returns a bounded integer.  SUM/AVG/MIN/MAX are
                # allowed only over numeric logical columns: in particular,
                # MIN/MAX(VARCHAR/BLOB) could otherwise return one attacker-
                # sized cell despite the one-row aggregate shape proof.
                for aggregate in statement.find_all(
                    exp.Sum, exp.Avg, exp.Min, exp.Max,
                ):
                    argument = aggregate.this
                    if (
                        isinstance(argument, exp.Column)
                        and _col_category(
                            visible_types.get(argument.name.casefold(), "")
                        ) != "numeric"
                    ):
                        return ValidationResult(
                            False,
                            "non_numeric_aggregate",
                            "custom_sql SUM/AVG/MIN/MAX require a numeric "
                            f"column; {argument.name!r} is not numeric",
                        )
            except Exception:
                return ValidationResult(
                    False,
                    "unparseable_sql",
                    "custom_sql could not be column-validated",
                )
        output_issue = _custom_sql_output_issue(sql)
        if output_issue is not None:
            code, message = output_issue
            return ValidationResult(False, code, message)
    elif rule_type in _COLUMN_RULE_TYPES:
        column = rule.get("column_name")
        if not isinstance(column, str) or not column:
            return ValidationResult(False, "missing_column", f"{rule_type} requires column_name")
        if not is_profilable_column(column):
            return ValidationResult(
                False,
                "hidden_column",
                f"custom rule column {column!r} is not part of the public read schema",
            )
        if columns is not None:
            visible_types = _column_type_map(columns)
            if column not in visible_types:
                return ValidationResult(
                    False,
                    "missing_column",
                    f"custom rule column {column!r} does not exist in the public read schema",
                )
            category = _col_category(visible_types[column])
            if category == "other":
                return ValidationResult(
                    False,
                    "unsupported_column_type",
                    f"custom rule {rule_type} does not support nested/opaque column {column!r}",
                )
            if rule_type in {"column_min", "column_max"} and category != "numeric":
                return ValidationResult(
                    False,
                    "unsupported_column_type",
                    f"custom rule {rule_type} requires a numeric column",
                )
            if rule_type == "distinct_in":
                expected = rule.get("expected_values")
                if (
                    isinstance(expected, Sequence)
                    and not isinstance(expected, (str, bytes))
                ):
                    try:
                        _distinct_in_literals(expected)
                    except _ExpectedValuesLimitError as exc:
                        return ValidationResult(False, exc.code, str(exc))
                    except ValueError as exc:
                        return ValidationResult(
                            False, "invalid_expected_value", str(exc),
                        )
                    for value in expected:
                        if not _distinct_expected_value_compatible(
                            value, visible_types[column],
                        ):
                            return ValidationResult(
                                False,
                                "expected_value_type",
                                "distinct_in expected_values must match the "
                                f"logical type of column {column!r}",
                            )

    threshold = rule.get("threshold")
    severity = rule.get("severity", "warning")
    if severity not in {"warning", "critical"}:
        return ValidationResult(False, "invalid_severity", "rule severity must be warning or critical")
    if rule_type in {"column_min", "column_max", "null_rate_max", "row_count_min"}:
        if _finite_decimal(threshold) is None:
            return ValidationResult(False, "invalid_threshold", "rule threshold must be a finite number")
        numeric_threshold = _finite_decimal(threshold)
        if rule_type == "null_rate_max" and not (Decimal(0) <= numeric_threshold <= Decimal(100)):
            return ValidationResult(False, "invalid_threshold", "null-rate threshold must be between 0 and 100")
        if rule_type == "row_count_min" and numeric_threshold < 0:
            return ValidationResult(False, "invalid_threshold", "row-count threshold cannot be negative")
    elif rule_type == "distinct_in":
        expected = rule.get("expected_values")
        if not isinstance(expected, Sequence) or isinstance(expected, (str, bytes)) or not expected:
            return ValidationResult(False, "missing_expected_values", "distinct_in requires expected_values")
        try:
            _distinct_in_literals(expected)
        except _ExpectedValuesLimitError as exc:
            return ValidationResult(False, exc.code, str(exc))
        except ValueError as exc:
            return ValidationResult(False, "invalid_expected_value", str(exc))
    elif threshold is not None and _finite_decimal(threshold) is None:
        return ValidationResult(False, "invalid_threshold", "custom_sql threshold must be a finite number")

    return ValidationResult(True)


def build_custom_rule_sql(
    rule: Dict[str, Any],
    table_fqn: str,
    columns: Optional[Iterable[Tuple[str, str]]] = None,
) -> Optional[str]:
    """Generate SQL for a validated user-defined rule.

    ``None`` denotes invalid/unsupported configuration.  This is deliberately
    not executable SQL: schedulers can record a skipped/error outcome instead
    of running a query that later becomes a false pass.
    """
    validation = validate_custom_rule(rule, columns, table_fqn=table_fqn)
    if not validation.valid:
        logger.warning("[dq-checker] custom rule rejected: %s", validation.message)
        return None
    rt = rule.get("rule_type", "")
    col = rule.get("column_name")
    threshold = rule.get("threshold")

    if rt == "column_min" and col and threshold is not None:
        q = _quote(col)
        return f"SELECT COUNT(*) AS violations FROM {table_fqn} WHERE {q} < {_number_sql(threshold)}"

    if rt == "column_max" and col and threshold is not None:
        q = _quote(col)
        return f"SELECT COUNT(*) AS violations FROM {table_fqn} WHERE {q} > {_number_sql(threshold)}"

    if rt == "null_rate_max" and col and threshold is not None:
        q = _quote(col)
        return (
            f"SELECT COALESCE(ROUND((COUNT(*) - COUNT({q})) * 100.0 "
            f"/ NULLIF(COUNT(*), 0), 2), 0.0) AS null_rate "
            f"FROM {table_fqn}"
        )

    if rt == "row_count_min" and threshold is not None:
        return f"SELECT COUNT(*) AS row_count FROM {table_fqn}"

    if rt == "distinct_in" and col:
        expected = rule.get("expected_values", [])
        if not expected:
            return None
        q = _quote(col)
        # Validation above guarantees both the element and encoded literal
        # budgets.  Returning one aggregate row prevents high-cardinality bad
        # data from materialising an unbounded Python/DataFrame result.
        vals = ", ".join(_distinct_in_literals(expected))
        return (
            f"SELECT COUNT(DISTINCT {q}) AS unexpected_count FROM {table_fqn} "
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

    def error(detail: str) -> Dict[str, Any]:
        return {
            "status": "error",
            "value": None,
            "detail": detail,
            "evaluated": False,
        }

    def single_row() -> Optional[Mapping[str, Any]]:
        if isinstance(result, Mapping):
            return result
        if (
            isinstance(result, list)
            and len(result) == 1
            and isinstance(result[0], Mapping)
        ):
            return result[0]
        return None

    def aggregate_value(field: str) -> Tuple[bool, Any]:
        row = single_row()
        if row is not None and field in row:
            return True, row.get(field)
        return False, None

    if rt in ("column_min", "column_max"):
        found, violations = aggregate_value("violations")
        numeric_violations = _finite_decimal(violations)
        if (
            not found
            or numeric_violations is None
            or numeric_violations < 0
            or numeric_violations != numeric_violations.to_integral_value()
        ):
            return error("Rule query did not return a numeric violations value")
        violations = int(numeric_violations)
        status = "ok" if violations == 0 else severity
        return {
            "status": status,
            "value": violations,
            "detail": f"{violations} rows violate rule" if violations > 0 else "All rows pass",
            "evaluated": True,
        }

    if rt == "null_rate_max":
        found, null_rate = aggregate_value("null_rate")
        numeric_rate = _finite_decimal(null_rate)
        numeric_threshold = _finite_decimal(threshold)
        if not found or numeric_rate is None or numeric_threshold is None:
            return error("Rule query did not return a numeric null_rate value")
        display_rate = float(numeric_rate)
        status = "ok" if numeric_rate <= numeric_threshold else severity
        return {
            "status": status,
            "value": display_rate,
            "detail": f"NULL rate: {display_rate}% (threshold: {threshold}%)",
            "evaluated": True,
        }

    if rt == "row_count_min":
        found, row_count = aggregate_value("row_count")
        numeric_count = _finite_decimal(row_count)
        numeric_threshold = _finite_decimal(threshold)
        if (
            not found
            or numeric_count is None
            or numeric_count < 0
            or numeric_count != numeric_count.to_integral_value()
            or numeric_threshold is None
        ):
            return error("Rule query did not return a numeric row_count value")
        display_count = int(numeric_count)
        status = "ok" if numeric_count >= numeric_threshold else severity
        return {
            "status": status,
            "value": display_count,
            "detail": f"Row count: {display_count} (minimum: {threshold})",
            "evaluated": True,
        }

    if rt == "distinct_in":
        if single_row() is None:
            return error("Rule query did not return exactly one aggregate row")
        found, unexpected_count = aggregate_value("unexpected_count")
        numeric_count = _finite_decimal(unexpected_count)
        if (
            not found
            or numeric_count is None
            or numeric_count < 0
            or numeric_count != numeric_count.to_integral_value()
        ):
            return error("Rule query did not return a numeric unexpected_count value")
        display_count = int(numeric_count)
        status = "ok" if display_count == 0 else severity
        return {
            "status": status,
            "value": display_count,
            "detail": (
                f"{display_count} unexpected distinct value"
                f"{'s' if display_count != 1 else ''}"
                if display_count else "All values expected"
            ),
            "evaluated": True,
        }

    if rt == "custom_sql":
        first_row = single_row()
        if not first_row:
            return error("Custom SQL did not return exactly one non-empty aggregate row")
        matching_values = [
            value
            for key, value in first_row.items()
            if isinstance(key, str) and key.casefold() == "violations"
        ]
        if len(matching_values) != 1:
            return error("Custom SQL result is missing its violations value")
        val = matching_values[0]
        numeric_value = _finite_decimal(val)
        if numeric_value is None:
            return error("Custom SQL violations value must be a finite number")
        if threshold is not None:
            numeric_threshold = _finite_decimal(threshold)
            if numeric_threshold is None:
                return error("Custom SQL threshold comparison requires a numeric first value")
            status = "ok" if numeric_value <= numeric_threshold else severity
        else:
            status = "ok"
        return {
            "status": status,
            "value": val,
            "detail": f"Result: {val}" + (f" (threshold: {threshold})" if threshold is not None else ""),
            "evaluated": True,
        }

    return error(f"Unknown rule type: {rt!r}")


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
