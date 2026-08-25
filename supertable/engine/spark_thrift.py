# supertable/engine/spark_thrift.py

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser, validate_read_query_ast
from supertable.utils.spark_security import validate_spark_storage_config
from supertable.utils.diagnostic_redaction import (
    local_path_metadata,
    safe_exception_type,
)
from supertable.data_classes import Reflection, RbacViewDef
from supertable.redis_catalog import RedisCatalog

from supertable.engine.engine_common import (
    quote_if_needed,
    rewrite_query_with_hashed_tables,
    safe_sql_diagnostic,
    snapshot_spark_type,
    tombstone_data_paths,
    validate_rbac_binding_stability,
)
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V2,
    validate_snapshot_tombstone_state,
)

from urllib.parse import urlparse, unquote

# Suppress verbose INFO logging from PyHive and Thrift libraries.
# PyHive logs every SQL statement (CREATE VIEW, DROP VIEW, SET, etc.)
# at INFO level, which floods the application log.
for _lib in ("pyhive", "pyhive.hive", "TCLIService", "thrift", "thrift_sasl"):
    logging.getLogger(_lib).setLevel(logging.WARNING)


# A Python DB-API driver can block inside its connection constructor before a
# connection object exists for the ordinary watchdog to close. Bound those
# attempts separately; an attempt that outlives its caller retains its slot
# until it actually unwinds and closes any late connection.
_SPARK_CONNECT_MAX_IN_FLIGHT = 8
_spark_connect_slots = threading.BoundedSemaphore(
    _SPARK_CONNECT_MAX_IN_FLIGHT,
)

# Cluster discovery is a synchronous Redis read and occurs before a Thrift
# connection exists for the ordinary watchdog to close.  Bound abandoned
# discovery workers independently from connection workers.
_SPARK_SELECTION_MAX_IN_FLIGHT = 8
_spark_selection_slots = threading.BoundedSemaphore(
    _SPARK_SELECTION_MAX_IN_FLIGHT,
)


# Spark SQL runs inside a long-lived JVM with access to cluster classes,
# Hadoop configuration and source-file metadata.  Unknown functions are not a
# compatibility feature on this boundary: they can resolve to cluster UDFs or
# newly added Spark built-ins with capabilities we have not reviewed.  Keep the
# user-query surface deliberately closed and version it through code review.
#
# Operators, literals, columns, joins, CASE predicates and window clauses are
# not Func nodes.  CASE/IF and CAST/TRY_CAST *are* Func nodes in sqlglot and are
# therefore listed explicitly alongside ordinary analytics functions.
_SPARK_SAFE_USER_FUNCTIONS = frozenset(
    """
    abs acos acosh add_months aggregate and any_value array array_append
    array_compact array_contains array_distinct array_except array_intersect array_join
    array_max array_min array_position array_prepend array_remove
    array_size array_sort array_union arrays_overlap arrays_zip ascii asin asinh
    atan atan2 atanh avg base64 bin bit_and bit_count bit_get bit_length bit_or
    bit_xor bitmap_bit_position bitmap_bucket_number bool_and bool_or bround
    btrim cardinality case cast cbrt ceil ceiling char char_length character_length
    chr coalesce contains conv corr cos
    cosh cot count count_if covar_pop covar_samp crc32 csc cube cume_dist
    current_date current_timestamp date_add date_diff date_format date_from_unix_date
    date_part date_sub date_trunc dateadd datediff day dayofmonth dayofweek
    dayofyear decode degrees dense_rank e element_at elt encode endswith every
    exp expm1 extract factorial filter find_in_set first first_value flatten floor
    forall from_unixtime get
    get_json_object greatest grouping grouping_id hash hex hour
    hypot if ifnull initcap instr kurtosis lag last last_day last_value lcase lead
    least left length levenshtein ln localtimestamp locate log log10 log1p log2
    lower ltrim make_date make_dt_interval make_interval make_timestamp
    make_timestamp_ltz make_timestamp_ntz make_ym_interval map map_concat
    map_contains_key map_entries map_filter map_from_arrays map_from_entries
    map_keys map_values map_zip_with max max_by md5 mean median min min_by minute
    mode month months_between named_struct nanvl negative next_day nth_value ntile
    nullif nullifzero nvl nvl2 octet_length overlay parse_url percent_rank
    percentile pi pmod position
    or positive pow power quarter radians rand randn rank reduce regexp_count
    regexp_extract regexp_extract_all regexp_instr regexp_like
    regexp_substr reverse right rint round row_number rtrim
    sec second sha sha1 sha2 shiftleft shiftright shiftrightunsigned
    shuffle sign signum sin sinh size slice some sort_array soundex split
    split_part sqrt startswith std stddev stddev_pop stddev_samp str_to_map struct
    substr substring substring_index sum tan tanh timestamp_add timestamp_diff timestamp_trunc
    timestamp_micros timestamp_millis timestamp_seconds to_binary to_char to_csv
    to_date to_json to_number to_timestamp to_timestamp_ltz to_timestamp_ntz
    transform translate trim trunc try_avg try_cast try_make_interval
    try_make_timestamp try_make_timestamp_ltz try_make_timestamp_ntz try_sum
    try_to_binary typeof ucase unbase64 unhex unix_date unix_micros unix_millis
    unix_seconds unix_timestamp upper url_decode url_encode var_pop var_samp
    variance weekday weekofyear width_bucket xxhash64 year zip_with zeroifnull
    """.split()
)


_SPARK_VARIABLE_SUBSTITUTION_RE = re.compile(r"\$\{")

# Snapshot versions originate in Redis/Lua catalog documents, whose integer
# contract is limited to values exactly representable by both runtimes.  Keep
# the same bound at the final SQL-identifier derivation boundary so malformed
# direct executor input cannot append arbitrary text to a temporary-view name.
_MAX_SAFE_SPARK_SNAPSHOT_VERSION = (1 << 53) - 1

# Several Spark SQL identity expressions are accepted without parentheses.
# In the sqlglot versions supported by SuperTable those tokens parse as plain,
# unqualified columns rather than Func nodes (for example ``SELECT USER``), so
# the function allowlist alone cannot see them.  Quoted identifiers and
# qualified data columns remain available (``SELECT `user``` / ``t.user``).
_SPARK_BARE_SESSION_IDENTIFIERS = frozenset({
    "user",
    "session_user",
    "system_user",
    "current_role",
    "current_catalog",
    "current_database",
    "current_schema",
    "current_namespace",
    "current_version",
    "version",
})
_SPARK_DUCKDB_ONLY_BOUNDED_COLLECTION_AGGREGATES = frozenset({
    "array_agg",
    "list",
})

# HiveServer2's result metadata maps both Spark ``TimestampType`` (an instant)
# and ``TimestampNTZType`` (a wall-clock value) to the same TIMESTAMP_TYPE.
# PyHive therefore cannot preserve that semantic distinction at the public
# JSON boundary. SuperTable pins Spark sessions to UTC/TIMESTAMP_LTZ below and
# rejects the explicit NTZ-producing surface before cluster selection. Under
# that closed contract, a TIMESTAMP_TYPE result can be marked as a UTC instant
# without mislabelling a wall-clock value.
_SPARK_UNREPRESENTABLE_NTZ_FUNCTIONS = frozenset({
    "localtimestamp",
    "make_timestamp_ntz",
    "to_timestamp_ntz",
    "try_make_timestamp_ntz",
})
_SPARK_BARE_UNREPRESENTABLE_NTZ_IDENTIFIERS = frozenset({"localtimestamp"})

# Private pandas attrs carried from the Thrift boundary to query_sql(). Type
# codes retain authoritative DB-API metadata even though dtype=object is used
# to prevent pandas from coercing nullable BIGINT values to float. Column
# indexes, rather than names, preserve duplicate result-column names.
SPARK_RESULT_TYPE_CODES_ATTR = "_supertable_spark_result_type_codes"
SPARK_UTC_TIMESTAMP_INDEXES_ATTR = (
    "_supertable_spark_utc_timestamp_indexes"
)

# PyHive 0.7 exposes HiveServer2's closed TTypeId vocabulary as the DB-API
# ``type_code`` strings below.  Reject missing or unknown metadata instead of
# silently guessing how a public scalar should be represented.
SPARK_THRIFT_TYPE_CODES = frozenset({
    "BOOLEAN_TYPE",
    "TINYINT_TYPE",
    "SMALLINT_TYPE",
    "INT_TYPE",
    "BIGINT_TYPE",
    "FLOAT_TYPE",
    "DOUBLE_TYPE",
    "STRING_TYPE",
    "TIMESTAMP_TYPE",
    "BINARY_TYPE",
    "ARRAY_TYPE",
    "MAP_TYPE",
    "STRUCT_TYPE",
    "UNION_TYPE",
    "USER_DEFINED_TYPE",
    "DECIMAL_TYPE",
    "NULL_TYPE",
    "DATE_TYPE",
    "VARCHAR_TYPE",
    "CHAR_TYPE",
    "INTERVAL_YEAR_MONTH_TYPE",
    "INTERVAL_DAY_TIME_TYPE",
})


def _redact_spark_sensitive_text(
    value: object, *, phase: str = "query",
) -> str:
    """Return phase/type/digest metadata without arbitrary backend prose."""

    error_type = (
        safe_exception_type(value)
        if isinstance(value, BaseException)
        else "str" if type(value) is str else "Exception"
    )
    try:
        raw = str(value)
    except Exception:
        raw = error_type
    diagnostic_id, diagnostic_bytes = safe_sql_diagnostic(raw)
    return (
        f"Spark {phase} failed; error_type={error_type}; "
        f"diagnostic_id={diagnostic_id}; diagnostic_bytes={diagnostic_bytes}"
    )


def _redact_spark_plan_text(value: object) -> str:
    """Return no attacker-controlled Spark plan text.

    Catalyst plans can render source paths, unquoted literals, configuration
    values and connector-specific fields whose grammar changes across Spark
    versions. Regex redaction cannot prove that every future rendering is safe,
    so persisted/API-visible profiles retain only the section's presence.
    """
    return "<spark-plan-redacted>" if str(value or "") else ""


def _spark_function_name(node: exp.Func) -> str:
    """Return the backend-visible function name used by the policy."""
    if isinstance(node, exp.Anonymous):
        name = node.name
    else:
        try:
            name = node.sql_name()
        except Exception:
            name = ""
    return str(name or "").strip().casefold()


def _validate_spark_expression_capabilities(parsed: exp.Expression) -> None:
    """Apply the closed Spark capability policy to one parsed expression."""
    forbidden_backend_nodes = tuple(
        node_type
        for name in ("QueryTransform", "Hint", "Lock")
        if isinstance((node_type := getattr(exp, name, None)), type)
    )
    if forbidden_backend_nodes and any(
        isinstance(node, forbidden_backend_nodes) for node in parsed.walk()
    ):
        raise ValueError(
            "Spark SQL scripts, locking clauses, and query hints are not allowed in "
            "SuperTable queries"
        )

    for column in parsed.find_all(exp.Column):
        identifier = column.this
        bare_name = (
            str(identifier.this or "").casefold()
            if isinstance(identifier, exp.Identifier)
            else ""
        )
        if (
            not column.table
            and isinstance(identifier, exp.Identifier)
            and not identifier.args.get("quoted")
            and bare_name in _SPARK_BARE_SESSION_IDENTIFIERS
        ):
            raise ValueError(
                "Spark SQL session identity expression is not allowed in "
                "SuperTable queries"
            )
        if (
            not column.table
            and isinstance(identifier, exp.Identifier)
            and not identifier.args.get("quoted")
            and bare_name in _SPARK_BARE_UNREPRESENTABLE_NTZ_IDENTIFIERS
        ):
            raise ValueError(
                "Spark TIMESTAMP_NTZ results are not supported by the "
                "HiveServer2 result protocol"
            )

    timestamp_ntz_type = getattr(exp.DataType.Type, "TIMESTAMPNTZ", None)
    if timestamp_ntz_type is not None and any(
        data_type.this == timestamp_ntz_type
        for data_type in parsed.find_all(exp.DataType)
    ):
        raise ValueError(
            "Spark TIMESTAMP_NTZ results are not supported by the "
            "HiveServer2 result protocol"
        )

    for function in parsed.find_all(exp.Func):
        name = _spark_function_name(function)
        if name in _SPARK_UNREPRESENTABLE_NTZ_FUNCTIONS:
            raise ValueError(
                "Spark TIMESTAMP_NTZ results are not supported by the "
                "HiveServer2 result protocol"
            )
        qualified = (
            isinstance(function.parent, exp.Dot)
            and function.parent.expression is function
        )
        if qualified or not name or name not in _SPARK_SAFE_USER_FUNCTIONS:
            rendered = name or "<unknown>"
            raise ValueError(
                "Spark SQL function is not allowed in SuperTable queries: "
                f"{rendered}"
            )


def _validate_spark_user_functions(parser: SQLParser) -> None:
    """Reject every Spark function not explicitly reviewed as data-only.

    This check intentionally lives in the Spark executor as well as the SQL
    parser boundary.  AUTO requests are initially parsed with DuckDB grammar
    and can later route to Spark; validating immediately before cluster
    selection prevents that route from bypassing the Spark-specific policy.
    """
    original_query = getattr(parser, "original_query", None)
    if isinstance(original_query, str) and _SPARK_VARIABLE_SUBSTITUTION_RE.search(
        original_query
    ):
        raise ValueError(
            "Spark SQL variable substitution is not allowed in SuperTable queries"
        )

    parsed = getattr(parser, "_parsed", None)
    if not isinstance(parsed, exp.Expression):
        query = getattr(parser, "original_query", None)
        if not isinstance(query, str) or not query.strip():
            raise RuntimeError("Spark query is missing a validated SQL expression")
        dialect = getattr(parser, "dialect", "spark")
        if dialect not in {"duckdb", "spark"}:
            dialect = "spark"
        try:
            parsed = sqlglot.parse_one(query, read=dialect)
        except Exception:
            raise RuntimeError(
                "Spark query could not be validated safely"
            ) from None

    # Built-in deep-quality SQL can opt into a narrowly bounded DuckDB LIST
    # aggregate. Spark SQL does not preserve that aggregate's ordered semantics
    # under the current transpiler. Reject it before cluster setup instead of
    # silently widening the public function surface or returning wrong data.
    if (
        getattr(parser, "allow_bounded_collection_aggregates", False) is True
        and any(
            _spark_function_name(function)
            in _SPARK_DUCKDB_ONLY_BOUNDED_COLLECTION_AGGREGATES
            for function in parsed.find_all(exp.Func)
        )
    ):
        raise ValueError(
            "Bounded collection aggregates require the DuckDB quality route "
            "and are not supported by Spark execution"
        )

    _validate_spark_expression_capabilities(parsed)


def _revalidate_spark_parser(
    parser: SQLParser,
    reflection: Reflection,
) -> SQLParser:
    """Rebuild the complete read policy from original SQL at the backend.

    A caller can supply a parser-like object whose ``_parsed`` tree does not
    match ``original_query`` or whose table helpers lie. Never use that object
    for Spark setup, bindings, or rewrite decisions.
    """
    # Reject obvious capabilities in the supplied tree first. This keeps the
    # failure before any attempt to infer missing parser context.
    _validate_spark_user_functions(parser)

    query = getattr(parser, "original_query", None)
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Spark execution requires original SQL text")
    dialect = getattr(parser, "dialect", "spark")
    if not isinstance(dialect, str):
        dialect = "spark"
    elif dialect not in {"duckdb", "spark"}:
        raise ValueError("Spark execution received an unsupported SQL dialect")
    try:
        original_expression = sqlglot.parse_one(query, read=dialect)
    except Exception:
        # SQLParser below owns the complete syntax/read-policy error contract.
        # This early pass exists only to reject Spark-specific capabilities
        # from the original text before a shared parser can normalize them.
        pass
    else:
        _validate_spark_expression_capabilities(original_expression)
    super_name = getattr(parser, "default_super_name", None)
    if not isinstance(super_name, str) or not super_name:
        reflected_supers = {
            str(getattr(snapshot, "super_name", ""))
            for snapshot in (getattr(reflection, "supers", None) or [])
            if str(getattr(snapshot, "super_name", ""))
        }
        if len(reflected_supers) != 1:
            raise ValueError("Spark execution requires a validated supertable scope")
        super_name = reflected_supers.pop()

    # SQLParser reasserts read-only roots, recursive DML/DDL rejection,
    # identifier-only managed sources, historical-snapshot denial and scope
    # analysis. It parses from the original text rather than accepting the
    # caller's AST.
    validated = SQLParser(
        super_name=super_name,
        query=query,
        dialect=dialect,
    )
    # Reassert the complete shared boundary explicitly at the selected backend,
    # even if a future SQLParser construction path becomes more permissive.
    validate_read_query_ast(validated._parsed, dialect)
    _validate_spark_user_functions(validated)

    reflected_keys = {
        (
            str(getattr(snapshot, "super_name", "")).casefold(),
            str(getattr(snapshot, "simple_name", "")).casefold(),
        )
        for snapshot in (getattr(reflection, "supers", None) or [])
    }
    requested_keys = {
        (str(table.super_name).casefold(), str(table.simple_name).casefold())
        for table in validated.get_physical_tables()
    }
    missing = requested_keys - reflected_keys
    if missing:
        raise ValueError(
            "Spark query sources do not match the authorized pinned reflection"
        )
    return validated


# =========================================================
# Timeout defaults (seconds) — overridable via env vars
# =========================================================

def _spark_timeout_seconds() -> int:
    """Overall timeout for a Spark Thrift query (connect + setup + execute + fetch).

    Default: 300 s (5 min).  Override with SUPERTABLE_SPARK_QUERY_TIMEOUT.
    """
    try:
        return settings.SUPERTABLE_SPARK_QUERY_TIMEOUT
    except (ValueError, TypeError):
        return 300


def _spark_statement_timeout_seconds() -> int:
    """Per-statement timeout for individual Thrift cursor.execute() calls.

    Default: 120 s (2 min).  Override with SUPERTABLE_SPARK_STATEMENT_TIMEOUT.
    Applies to each CREATE VIEW / SET / user-query individually.
    """
    try:
        return settings.SUPERTABLE_SPARK_STATEMENT_TIMEOUT
    except (ValueError, TypeError):
        return 120


def _to_s3a_path(file_path: str) -> str:
    """Convert a file path to an s3a:// path suitable for Spark.

    Handles three cases:
      1. s3://bucket/key          → s3a://bucket/key
      2. recognised provider HTTP URLs → their credential-less native URI
      3. s3a://… or local path    → returned as-is

    Custom S3-compatible endpoints are deliberately not inferred from their
    URL shape. Path-style and virtual-hosted forms are ambiguous without the
    backend's configured bucket and logical key; ``_resolve_spark_file`` uses
    ``storage.canonical_uri(raw_key)`` for those instead.
    """
    if file_path.startswith("s3://"):
        return "s3a://" + file_path[5:]

    if file_path.startswith("s3a://"):
        return file_path

    if file_path.startswith("gcs://"):
        return "gs://" + file_path[6:]

    if file_path.startswith(("gs://", "abfss://")):
        return file_path

    if file_path.startswith("http://") or file_path.startswith("https://"):
        parsed = urlparse(file_path)
        host = (parsed.hostname or "").lower()
        # path is /bucket/key — strip the leading slash, split into bucket + key.
        # urlparse leaves percent-escapes intact, and boto3 presigning encodes
        # the key (Hive partition dirs like ``year=2006`` become ``year%3D2006``).
        # Spark's S3A client expects the *raw* object key and re-encodes itself,
        # so we must decode here — otherwise Ceph RGW, which matches keys
        # literally, 404s on the ``%3D`` form. The presign query string is
        # dropped on purpose: Spark authenticates with its own fs.s3a.* creds.
        path = unquote(parsed.path).lstrip("/")
        if host == "storage.googleapis.com":
            return f"gs://{path}" if "/" in path else file_path
        if host.endswith(".storage.googleapis.com"):
            bucket = host[: -len(".storage.googleapis.com")]
            return f"gs://{bucket}/{path}" if bucket and path else file_path
        if host.endswith(".blob.core.windows.net"):
            account = host[: -len(".blob.core.windows.net")]
            if account and "/" in path:
                container, _, key = path.partition("/")
                return (
                    f"abfss://{container}@{account}.dfs.core.windows.net/{key}"
                )
            return file_path
        if host.endswith(".dfs.core.windows.net"):
            # An HTTPS ADLS endpoint still needs a container authority that is
            # not unambiguously recoverable from all URL shapes.  Leave it as
            # HTTPS (which may itself be signed) rather than misroute to S3.
            return file_path
        # AWS virtual-hosted style: bucket is in the hostname, not URL path.
        marker = ".s3."
        if marker in host and host.endswith(".amazonaws.com"):
            bucket = host.split(marker, 1)[0]
            return f"s3a://{bucket}/{path}" if bucket and path else file_path
        if host.endswith(".s3.amazonaws.com"):
            bucket = host[: -len(".s3.amazonaws.com")]
            return f"s3a://{bucket}/{path}" if bucket and path else file_path
        if host == "s3.amazonaws.com" or (
            host.startswith("s3.") and host.endswith(".amazonaws.com")
        ):
            return f"s3a://{path}"
        # Unknown/custom endpoint: only the storage backend can prove which
        # path segment is the bucket. Leave it untouched so the resolver can
        # use a pinned raw key or fail closed.
        return file_path

    return file_path


def _resolve_spark_file(
        storage, file_path: str, raw_key: Optional[str] = None,
) -> str:
    """Resolve one snapshot file path to the form Spark should scan.

    ``sup.files`` are resolved once by the estimator using the *DuckDB* presign
    setting (``SUPERTABLE_DUCKDB_PRESIGNED``) and shared by every engine, so
    Spark re-resolves them here. Spark user SQL runs in the same session that
    owns the temporary parquet views. A presigned source path is therefore a
    reusable bearer credential if any Spark metadata/introspection surface
    reveals it. The presigned mode is disabled until source provisioning is
    isolated from the user session.
    Existing signed estimator paths are converted back to credential-less
    backend URIs; an ambiguous signed URL fails closed.
    """
    if settings.SUPERTABLE_SPARK_PRESIGNED:
        raise RuntimeError(
            "SUPERTABLE_SPARK_PRESIGNED is disabled: configure Spark-side "
            "workload identity or a Hadoop credential provider"
        )

    try:
        source = urlparse(file_path)
    except Exception:
        raise RuntimeError("Spark source path could not be resolved safely") from None

    # A signed URL from a custom S3-compatible endpoint can be path-style
    # (endpoint/bucket/key) or virtual-hosted (bucket.endpoint/key). Guessing
    # from URL text can silently bind Spark to the wrong bucket/key. When the
    # estimator supplies the pinned logical object key, ask the owning storage
    # backend for its canonical URI and ignore the incidental signed URL shape.
    used_backend_canonical = False
    if source.scheme.casefold() in {"http", "https"} and raw_key is not None:
        canonical_uri = getattr(storage, "canonical_uri", None)
        if not callable(canonical_uri) or not isinstance(raw_key, str) or not raw_key:
            raise RuntimeError(
                "Spark HTTP source requires a pinned backend-owned object URI"
            )
        try:
            canonical = canonical_uri(raw_key)
        except Exception:
            raise RuntimeError(
                "Spark HTTP source requires a pinned backend-owned object URI"
            ) from None
        if not isinstance(canonical, str) or not canonical:
            raise RuntimeError(
                "Spark HTTP source requires a pinned backend-owned object URI"
            )
        direct = _to_s3a_path(canonical)
        used_backend_canonical = True
    else:
        direct = _to_s3a_path(file_path)

    parsed = urlparse(direct)
    scheme = parsed.scheme.casefold()
    if used_backend_canonical and scheme not in {"s3a", "gs", "abfss", "file"}:
        raise RuntimeError(
            "Spark backend returned an unsupported canonical object URI"
        )
    # ``container@account`` is required ABFSS authority syntax, not userinfo.
    has_userinfo = scheme in {"http", "https", "s3", "s3a", "gs"} and (
        parsed.username is not None or parsed.password is not None
    )
    if scheme in {"http", "https", "s3", "s3a", "gs", "abfss"} and (
        parsed.query or parsed.fragment or has_userinfo
    ):
        raise RuntimeError(
            "Spark source path contains a bearer credential that cannot be "
            "safely isolated from user SQL"
        )
    if scheme in {"http", "https"}:
        raise RuntimeError(
            "Spark HTTP source cannot be mapped to a pinned backend object"
        )
    return direct


# =========================================================
# Spark SQL helpers
# =========================================================

def _spark_quote_identifier(value: object) -> str:
    """Return one safely backtick-quoted Spark SQL identifier."""
    text = _spark_metadata_text(value, "Spark identifier")
    return "`" + text.replace("`", "``") + "`"


def _spark_metadata_text(value: object, label: str) -> str:
    """Validate metadata before embedding it in a Spark SQL statement.

    Spark performs variable substitution before parsing and accepts literal
    newlines/control bytes in surprising contexts.  Session substitution is
    disabled independently, but metadata is also fenced here so a later
    session-regression cannot turn a catalog path or column into SQL text.
    """
    if not isinstance(value, str):
        raise RuntimeError(f"{label} must be a string")
    text = value
    if not text or len(text) > 16_384:
        raise RuntimeError(f"{label} has an invalid length")
    if _SPARK_VARIABLE_SUBSTITUTION_RE.search(text):
        raise RuntimeError(f"{label} contains Spark variable substitution")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in text):
        raise RuntimeError(f"{label} contains a control character")
    return text


def _spark_string_literal(value: object, label: str = "Spark string") -> str:
    """Render a validated string using Spark's backslash-escape semantics."""
    text = _spark_metadata_text(value, label)
    return exp.Literal.string(text).sql(dialect="spark")


def _execute_spark_setup_sql(cursor, sql: str, phase: str) -> None:
    """Execute generated setup SQL without exposing backend-echoed SQL.

    Thrift errors commonly include the complete failed CREATE VIEW statement.
    Those statements contain storage paths, hidden columns, and protected RBAC
    literals, so setup errors cross the public boundary as phase-only messages.
    """
    try:
        cursor.execute(sql)
    except Exception:
        raise RuntimeError(f"Spark {phase} failed") from None


def _spark_table_name(super_name: str, simple_name: str, version: int) -> str:
    """Generate a deterministic Spark temp table name."""
    if (
        type(version) is not int
        or not 0 <= version <= _MAX_SAFE_SPARK_SNAPSHOT_VERSION
    ):
        raise RuntimeError("Spark snapshot version is invalid")
    key = f"{super_name}_{simple_name}"
    digest = hashlib.sha1(
        key.encode("utf-8"), usedforsecurity=False
    ).hexdigest()[:12]
    return f"spark_{digest}_v{version}"


def _spark_create_parquet_view_impl(
        cursor,
        table_name: str,
        files: List[str],
        column_types: Optional[Dict[str, str]] = None,
        *,
        _created_names: List[str],
) -> List[str]:
    """Register parquet files as a Spark temp view via Thrift.

    Spark SQL's ``UNION ALL`` is positional.  Raw ``SELECT *`` unions are
    therefore unsafe for schema-evolving parquet files: if one file stores
    ``(id, cvv)`` and another stores ``(cvv, id)``, the second file's sensitive
    ``cvv`` values are relabelled as ``id`` before the RBAC view gets a chance
    to exclude them.  Each file is consequently described and projected into
    one canonical, quoted column order before any batch/final positional union.

    ``column_types`` is the schema pinned from the same snapshot as ``files``.
    When present it defines the logical target set/order (the writer's
    last-schema-wins contract); fields absent from an older file become NULL
    and fields absent from the current schema are not resurrected.  The
    no-schema fallback derives a deterministic first-seen union for legacy
    callers.  Any describe ambiguity or case-colliding source name fails
    closed rather than risking a wrong binding.

    **Important**: intermediate views (part views and batch views) are NOT
    dropped here.  In Spark SQL, ``CREATE VIEW X AS SELECT * FROM Y`` stores
    a lazy reference to Y — dropping Y invalidates X.  Intermediate views
    must stay alive until the final query completes.

    Returns a list of all intermediate view names created, so the caller
    can drop them during cleanup.
    """
    if not files:
        raise ValueError(f"No files provided for Spark reflection {table_name!r}")

    # Validate every path before recording a possibly-created view.  Invalid
    # catalog metadata must not cause even cleanup/setup SQL to reach Spark.
    for file_path in files:
        _spark_string_literal(file_path, "Spark parquet path")

    snapshot_schema = dict(column_types or {})
    snapshot_by_fold: Dict[str, tuple[str, str]] = {}
    for raw_name, raw_type in snapshot_schema.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise RuntimeError("Pinned Spark schema contains an invalid column name")
        folded = raw_name.casefold()
        if folded in snapshot_by_fold:
            raise RuntimeError(
                "Pinned Spark schema contains case-colliding columns"
            )
        # Validate persisted types through the closed snapshot type grammar
        # before creating any view.  It is used below when a current-schema
        # column is absent from every surviving historical file.
        safe_type = snapshot_spark_type(raw_type)
        snapshot_by_fold[folded] = (raw_name, safe_type)

    intermediate_views: List[str] = []

    batch_size = settings.SUPERTABLE_SPARK_BATCH_SIZE
    batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    part_batches: List[List[str]] = []
    part_columns: Dict[str, Dict[str, str]] = {}
    source_spelling: Dict[str, str] = {}

    for batch_idx, batch in enumerate(batches):
        part_views: List[str] = []
        for file_idx, f in enumerate(batch):
            part_name = f"__{table_name}_b{batch_idx}p{file_idx}__"
            # Record before execution: a lost Thrift response is ambiguous and
            # may mean Spark created the view even though execute raised.
            _created_names.append(part_name)
            _execute_spark_setup_sql(
                cursor,
                f"CREATE OR REPLACE TEMPORARY VIEW {part_name} "
                f"USING parquet "
                f"OPTIONS (path {_spark_string_literal(f, 'Spark parquet path')})",
                "parquet source view creation",
            )
            part_views.append(part_name)
            intermediate_views.append(part_name)

            try:
                cursor.execute(f"DESCRIBE {part_name}")
                described = cursor.fetchall()
            except Exception:
                raise RuntimeError(
                    "Cannot resolve a parquet source schema for safe Spark union"
                ) from None

            if not isinstance(described, (list, tuple)):
                raise RuntimeError(
                    "Spark returned an invalid parquet source schema"
                )

            columns: Dict[str, str] = {}
            for row in described:
                if not isinstance(row, (list, tuple)) or not row:
                    raise RuntimeError(
                        "Spark returned an invalid parquet source schema row"
                    )
                if row[0] is None:
                    continue
                name = str(row[0])
                # Spark can append partition-information rows to DESCRIBE.
                if not name or name.startswith("#"):
                    continue
                folded = name.casefold()
                if folded in columns:
                    raise RuntimeError(
                        "Spark parquet source contains duplicate or "
                        "case-colliding columns"
                    )
                prior_spelling = source_spelling.get(folded)
                if prior_spelling is not None and prior_spelling != name:
                    raise RuntimeError(
                        "Spark parquet sources contain case-colliding columns"
                    )
                source_spelling[folded] = name
                columns[folded] = name
            if not columns:
                raise RuntimeError(
                    "Cannot resolve any parquet columns for safe Spark union"
                )
            part_columns[part_name] = columns

        part_batches.append(part_views)

    target_columns: List[tuple[str, str, Optional[str]]]
    if snapshot_by_fold:
        target_columns = [
            (
                folded,
                pinned_name,
                safe_type,
            )
            for folded, (pinned_name, safe_type) in snapshot_by_fold.items()
        ]
    else:
        # Dict insertion order follows the deterministic file/DESCRIBE order.
        target_columns = [
            (folded, spelling, None)
            for folded, spelling in source_spelling.items()
        ]

    if not target_columns:
        raise RuntimeError("Cannot construct a Spark reflection with no columns")

    def _aligned_select(part_name: str) -> str:
        available = part_columns[part_name]
        expressions = []
        for folded, output_name, fallback_type in target_columns:
            source_name = available.get(folded)
            if source_name is not None:
                expressions.append(
                    f"{_spark_quote_identifier(source_name)} AS "
                    f"{_spark_quote_identifier(output_name)}"
                )
            elif folded in source_spelling:
                # At least one sibling file supplies the concrete type.  Spark
                # safely coerces its NULL type during the aligned union.
                expressions.append(
                    f"NULL AS {_spark_quote_identifier(output_name)}"
                )
            else:
                # The current schema can legitimately be wider than every
                # pruned survivor.  Retain its safe persisted type.
                expressions.append(
                    f"CAST(NULL AS {fallback_type}) AS "
                    f"{_spark_quote_identifier(output_name)}"
                )
        return f"SELECT {', '.join(expressions)} FROM {part_name}"

    all_batch_views: List[str] = []

    for batch_idx, part_views in enumerate(part_batches):

        # Union all parts into one batch view
        batch_view = f"__{table_name}_batch{batch_idx}__"
        union_sql = " UNION ALL ".join(
            _aligned_select(part_name) for part_name in part_views
        )
        _created_names.append(batch_view)
        _execute_spark_setup_sql(
            cursor,
            f"CREATE OR REPLACE TEMPORARY VIEW {batch_view} AS {union_sql}",
            "schema-alignment view creation",
        )
        all_batch_views.append(batch_view)
        intermediate_views.append(batch_view)

        # Do NOT drop part views — Spark views are lazy references.
        # Dropping the part view would invalidate the batch view.

    # Final: union all batch views into the target table view
    if len(all_batch_views) == 1:
        union_sql = f"SELECT * FROM {all_batch_views[0]}"
    else:
        union_sql = " UNION ALL ".join(f"SELECT * FROM {v}" for v in all_batch_views)

    _created_names.append(table_name)
    _execute_spark_setup_sql(
        cursor,
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS {union_sql}",
        "reflection view creation",
    )

    # Do NOT drop batch views — the target view references them lazily.
    # Return them so the caller drops them after the query completes.
    return intermediate_views


def _spark_create_parquet_view(
        cursor,
        table_name: str,
        files: List[str],
        column_types: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Create one schema-aligned Spark reflection with failure cleanup.

    The implementation must create lazy intermediate views before it can
    DESCRIBE and align them.  If any later proof or CREATE fails, it cannot
    return those names to the executor's normal cleanup path.  Track every
    possibly-created name up front and drop it here without masking the
    original security/correctness failure.
    """
    created_names: List[str] = []
    try:
        return _spark_create_parquet_view_impl(
            cursor,
            table_name,
            files,
            column_types=column_types,
            _created_names=created_names,
        )
    except Exception:
        seen = set()
        for name in reversed(created_names):
            if name in seen:
                continue
            seen.add(name)
            try:
                cursor.execute(
                    f"DROP VIEW IF EXISTS {_spark_quote_identifier(name)}"
                )
            except Exception:
                pass
        raise


def _spark_create_empty_view(cursor, table_name: str, column_types: Dict[str, str]) -> None:
    """Create a typed empty Spark view for a snapshot with zero resources."""
    if not column_types:
        raise RuntimeError(
            "Cannot construct an empty Spark reflection: pinned snapshot has no schema"
        )
    expressions = []
    for name, raw_type in column_types.items():
        type_name = snapshot_spark_type(raw_type)
        expressions.append(
            f"CAST(NULL AS {type_name}) AS {_spark_quote_identifier(name)}"
        )
    _execute_spark_setup_sql(
        cursor,
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS SELECT "
        + ", ".join(expressions)
        + " WHERE 1 = 0",
        "empty reflection view creation",
    )


def _spark_create_rbac_view(
        cursor,
        base_table_name: str,
        view_name: str,
        rbac_view_def: RbacViewDef,
) -> None:
    """Create an RBAC-filtered view on top of a Spark table."""
    excluded = {
        str(c).casefold()
        for c in (getattr(rbac_view_def, "excluded_columns", None) or [])
    }
    actual_columns = None

    def _actual_columns() -> List[str]:
        nonlocal actual_columns
        if actual_columns is None:
            try:
                cursor.execute(f"DESCRIBE {base_table_name}")
                described = cursor.fetchall()
            except Exception:
                raise RuntimeError(
                    "Cannot resolve source schema for RBAC exclusions"
                ) from None
            actual_columns = [
                str(row[0]) for row in described
                if row and str(row[0]).strip() and not str(row[0]).startswith("#")
            ]
            folded = [name.casefold() for name in actual_columns]
            if len(set(folded)) != len(folded):
                raise RuntimeError(
                    "Cannot apply RBAC to a relation with case-colliding columns"
                )
        return actual_columns

    if rbac_view_def.allowed_columns == ["*"]:
        if excluded:
            visible = [
                name for name in _actual_columns()
                if name.casefold() not in excluded
            ]
            if not visible:
                raise PermissionError("RBAC policy excludes every visible column")
            select_cols = ", ".join(
                _spark_quote_identifier(c) for c in visible
            )
        else:
            select_cols = "*"
    else:
        allowed = {str(c).casefold() for c in rbac_view_def.allowed_columns}
        visible = [
            name for name in _actual_columns()
            if name.casefold() in allowed and name.casefold() not in excluded
        ]
        if not visible:
            raise PermissionError("RBAC policy excludes every allowed column")
        select_cols = ", ".join(
            _spark_quote_identifier(c) for c in visible
        )

    where_clause = rbac_view_def.where_clause
    if where_clause:
        # The clause may contain both a structured role predicate and a linked
        # share predicate merged by DataReader. Transpile the complete secured
        # expression as one AST so Spark identifier quoting is correct and no
        # conjunct can be discarded by regenerating only ``filter_spec``.
        try:
            if _SPARK_VARIABLE_SUBSTITUTION_RE.search(where_clause):
                raise ValueError("Spark variable substitution is not allowed")
            parsed_statements = sqlglot.parse(
                f"SELECT 1 WHERE {where_clause}", read="duckdb",
            )
            semicolon_type = getattr(exp, "Semicolon", None)
            statements = [
                statement
                for statement in parsed_statements
                if statement is not None
                and not (
                    isinstance(semicolon_type, type)
                    and isinstance(statement, semicolon_type)
                )
            ]
            if len(statements) != 1:
                raise ValueError("RBAC predicate must contain exactly one statement")
            secured = statements[0]
            where_expr = secured.args.get("where")
            if where_expr is None:
                raise ValueError("missing WHERE expression")
            scalar_forbidden = tuple(
                node_type
                for name in (
                    "Query", "Subquery", "Table", "QueryTransform", "Hint",
                    "Lateral", "AggFunc", "Window", "Parameter",
                    "Placeholder", "SessionParameter", "Var",
                )
                if isinstance((node_type := getattr(exp, name, None)), type)
            )
            if scalar_forbidden and any(
                isinstance(node, scalar_forbidden)
                for node in where_expr.this.walk()
            ):
                raise ValueError("RBAC predicate must be a table-local scalar")
            _validate_spark_expression_capabilities(where_expr.this)
            where_clause = where_expr.this.sql(dialect="spark")
        except Exception:
            raise RuntimeError("Unable to transpile protected Spark predicate") from None

    where_sql = ""
    if where_clause:
        where_sql = f" WHERE {where_clause}"

    sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
        f"SELECT {select_cols} FROM {base_table_name}{where_sql}"
    )
    _execute_spark_setup_sql(cursor, sql, "protected RBAC view creation")


_SPARK_SYSTEM_COLS = ("__rowid__", "__timestamp__", "__file__")
_SPARK_CANONICAL_RESERVED = {
    name.casefold(): name for name in _SPARK_SYSTEM_COLS
}


def _validate_spark_tombstone_definition(tombstone_def) -> tuple[bool, int]:
    """Return ``(active, format)`` after closing malformed direct states."""
    if tombstone_def is None:
        return False, 1
    pointer = getattr(tombstone_def, "tombstone_path", None)
    cache_key = getattr(tombstone_def, "cache_key", None)
    rows = getattr(tombstone_def, "expected_rows", None)
    digest = getattr(tombstone_def, "tombstone_digest", None)
    tombstone_format = getattr(tombstone_def, "tombstone_format", None)
    segments = getattr(tombstone_def, "segments", ())
    if pointer is not None and (
        not isinstance(pointer, str) or not pointer
    ):
        raise RuntimeError("Invalid Spark deletion-vector resolved path")
    try:
        normalized_format = validate_snapshot_tombstone_state(
            cache_key if pointer is not None else None,
            rows,
            digest,
            format_present=tombstone_format is not None,
            tombstone_format=tombstone_format,
        )
    except (TypeError, ValueError):
        raise RuntimeError(
            "Invalid Spark deletion-vector snapshot state"
        ) from None

    if pointer is None:
        if cache_key is not None or segments != ():
            raise RuntimeError(
                "Invalid empty Spark deletion-vector representation"
            )
        return False, normalized_format
    try:
        tombstone_data_paths(tombstone_def)
    except Exception:
        raise RuntimeError(
            "Invalid Spark deletion-vector representation"
        ) from None
    return True, normalized_format


def _spark_create_tombstone_view(
        cursor,
        source_table: str,
        view_name: str,
        tombstone_def,
        storage=None,
) -> None:
    """Create a view that hides system columns and drops tombstoned rows.

    Spark has no ``EXCLUDE`` syntax, so the source schema is introspected via
    ``DESCRIBE`` and an explicit column list is built that omits the system
    columns (``__rowid__`` / ``__timestamp__`` / ``__file__``). When the table
    has a deletion-vector (``tombstone_def.tombstone_path``), rows whose
    ``__rowid__`` appears in the vector parquet are removed with a
    LEFT ANTI JOIN before the columns are projected away.

    Created on top of the reflection table (before RBAC), so the anti-join
    still has ``__rowid__`` and RBAC never sees the system columns.
    """
    active_tombstone, tombstone_format = (
        _validate_spark_tombstone_definition(tombstone_def)
    )
    if tombstone_format == TOMBSTONE_FORMAT_V2:
        # Spark cannot bind its resolved input_file_name() values back to the
        # stable logical keys stored in the deletion vector. Never interpret
        # the JSON manifest as Parquet or silently ignore its segments.
        raise RuntimeError(
            "Spark does not support active tombstone_format=2 deletion vectors"
        )
    if active_tombstone:
        # Spark's input_file_name() exposes resolved s3a/gs/abfss or signed
        # URLs, while the DV stores raw catalog keys. Reject before DESCRIBE or
        # any other source setup rather than applying a rowid-only anti-join.
        raise RuntimeError(
            "Spark deletion-vector reads require composite source-file + "
            "row-id identity and are not supported safely"
        )

    describe_error = None
    src_desc = []
    try:
        cursor.execute(f"DESCRIBE {source_table}")
        src_desc = cursor.fetchall()
        src_cols = [str(row[0]) for row in src_desc]
    except Exception as exc:
        describe_error = exc
        src_cols = []

    tomb_path = getattr(tombstone_def, "tombstone_path", None) if tombstone_def else None
    if describe_error is not None:
        # Schema introspection is the security boundary for both deletion
        # vectors and stripping storage-only columns.  Falling back to ``*``
        # would expose reserved columns even on a table with no tombstones.
        raise RuntimeError(
            "Cannot validate source schema for protected projection"
        ) from None

    seen_reserved = set()
    for name in src_cols:
        folded = name.casefold()
        is_reserved = (
            folded in _SPARK_CANONICAL_RESERVED
            or folded.startswith("__supertable_")
        )
        if not is_reserved:
            continue
        expected = _SPARK_CANONICAL_RESERVED.get(folded)
        if expected is None or name != expected or folded in seen_reserved:
            raise RuntimeError(
                f"Invalid reserved system column in Spark reflection schema: {name!r}"
            )
        seen_reserved.add(folded)

    if tomb_path:
        rowid_rows = [row for row in src_desc if str(row[0]) == "__rowid__"]
        if len(rowid_rows) != 1 or str(rowid_rows[0][1]).lower() != "bigint":
            raise RuntimeError(
                "Cannot apply deletion vector: source requires canonical "
                "__rowid__ BIGINT"
            )
        # Spark's input_file_name() exposes resolved s3a/gs/abfss or signed
        # URLs, while the DV stores raw catalog keys.  Until an exact mapping is
        # carried into the Spark reflection, a rowid-only anti-join can hide a
        # live row in file B when DV contains (file A, same rowid).  Refuse the
        # read rather than violate the no-data-loss contract.
        raise RuntimeError(
            "Spark deletion-vector reads require composite source-file + "
            "row-id identity and are not supported safely"
        )

    user_cols = [c for c in src_cols if c not in _SPARK_SYSTEM_COLS]
    if user_cols:
        select_cols = ", ".join(
            f"src.{_spark_quote_identifier(c)}" for c in user_cols
        )
    else:
        raise RuntimeError("Protected Spark relation has no visible columns")

    has_rowid = "__rowid__" in src_cols

    if tomb_path:
        if not has_rowid:
            raise RuntimeError(
                "Cannot apply deletion vector: source table has no __rowid__ column"
            )
        # Resolve the deletion-vector pointer the same way as the data files
        # (see the data-file loop in the executor) so Spark reads it through the
        # same credential-less access method as the data files instead of a
        # bare key or a DuckDB-shaped presigned URL.
        resolved_path = _resolve_spark_file(
            storage,
            tomb_path,
            raw_key=getattr(tombstone_def, "cache_key", None),
        )
        relation = f"parquet.{_spark_quote_identifier(resolved_path)}"

        # Validate the immutable DV before exposing a view.  The engine cannot
        # safely use DV.__file__ for a composite join yet: Spark sees an s3a or
        # presigned filename while the writer stores the raw object key.  The
        # writer's table-global row-id invariant remains the conservative
        # boundary until a canonical source-file column is plumbed through.
        try:
            cursor.execute(f"DESCRIBE {relation}")
            dv_desc = cursor.fetchall()
            dv_schema = [
                (str(row[0]), str(row[1]).lower())
                for row in dv_desc
                if row and str(row[0]).strip() and not str(row[0]).startswith("#")
            ]
            required = [("__file__", "string"), ("__rowid__", "bigint")]
            if dv_schema != required:
                raise RuntimeError(
                    f"Invalid deletion-vector schema: expected exactly {required}, got {dv_schema}"
                )
            cursor.execute(
                "SELECT count(*), count(`__file__`), count(`__rowid__`), "
                "count(DISTINCT `__rowid__`), "
                "sum(CASE WHEN length(`__file__`) = 0 THEN 1 ELSE 0 END) "
                f"FROM {relation}"
            )
            count_rows = cursor.fetchall()
            if len(count_rows) != 1 or len(count_rows[0]) < 5:
                raise RuntimeError("Invalid deletion-vector count result")
            total, files, rowids, distinct_rowids, empty_files = count_rows[0][:5]
            total = int(total)
            empty_files = int(empty_files or 0)
            if (
                int(files) != total
                or int(rowids) != total
                or int(distinct_rowids) != total
                or empty_files != 0
            ):
                raise RuntimeError(
                    "Invalid deletion vector: non-null __file__/__rowid__ and "
                    "unique __rowid__ values are required"
                )
            expected_rows = getattr(tombstone_def, "expected_rows", None)
            if expected_rows is not None and total != int(expected_rows):
                raise RuntimeError(
                    f"Invalid deletion-vector row count: expected {int(expected_rows)}, got {total}"
                )
            raw_snapshot_keys = getattr(
                tombstone_def, "snapshot_resource_keys", None,
            )
            selected_keys = getattr(tombstone_def, "resource_keys", ()) or ()
            allowed_keys = (
                tuple(str(path) for path in raw_snapshot_keys)
                if raw_snapshot_keys is not None
                else (tuple(str(path) for path in selected_keys) or None)
            )
            if allowed_keys is not None:
                if not allowed_keys and total:
                    raise RuntimeError(
                        "Invalid deletion vector: __file__ contains resources "
                        "outside the pinned table snapshot"
                    )
                allowed_sql = ", ".join(
                    _spark_string_literal(path, "Spark snapshot resource key")
                    for path in sorted(set(allowed_keys))
                )
                if allowed_sql:
                    cursor.execute(
                        "SELECT count(DISTINCT `__file__`) "
                        f"FROM {relation} WHERE `__file__` NOT IN ({allowed_sql})"
                    )
                    foreign_rows = cursor.fetchall()
                    if (
                        len(foreign_rows) != 1
                        or not foreign_rows[0]
                        or int(foreign_rows[0][0]) != 0
                    ):
                        raise RuntimeError(
                            "Invalid deletion vector: __file__ contains resources "
                            "outside the pinned table snapshot"
                        )
        except RuntimeError:
            raise
        except Exception:
            raise RuntimeError("Unable to validate deletion vector safely") from None

        sql = (
            f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
            f"SELECT {select_cols} FROM {source_table} AS src "
            f"LEFT ANTI JOIN (SELECT DISTINCT `__rowid__` "
            f"FROM {relation}) AS __dv__ "
            f"ON src.`__rowid__` = __dv__.`__rowid__`"
        )
    else:
        sql = (
            f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
            f"SELECT {select_cols} FROM {source_table} AS src"
        )
    _execute_spark_setup_sql(cursor, sql, "protected projection view creation")


# ---------------------------------------------------------------------------
# Timestamp-cast wrapper: detect timestamp columns by PARQUET logical type
# ---------------------------------------------------------------------------
#
# When ``spark.sql.legacy.parquet.nanosAsLong=true`` Spark reads a
# TIMESTAMP(NANOS) parquet column as BIGINT (epoch nanoseconds) instead of
# failing, so we wrap the view to convert those back to TIMESTAMP.  The columns
# to convert are found by their ACTUAL parquet logical type (read from the file
# footer), NEVER by name: an epoch-integer column merely *named* like a
# timestamp (``created_at`` holding epoch-ms, ``source_ts_ms``) is a plain INT64
# with no timestamp logical type, so it must stay BIGINT.  Casting it — as the
# old name heuristic did — both corrupts its value and breaks expressions like
# ``FROM_UNIXTIME(created_at / 1000.0)`` that need a number, not a TIMESTAMP.


def _read_parquet_schema(
    storage,
    original_path: Optional[str],
    raw_key: Optional[str] = None,
):
    """Read one parquet file's schema (footer only); return a ``pyarrow.Schema``.

    Returns ``None`` if the schema can't be read.  *original_path* is a snapshot
    file path (``sup.files`` — a local path, an ``s3://``/``s3a://`` URL, or a
    presigned ``http(s)://`` URL), NOT the s3a form handed to Spark.

      * Local file   → footer-only read straight from disk.
      * Object store → the pinned logical ``raw_key`` is read through *storage*,
        which owns the endpoint/bucket mapping. Legacy direct s3/s3a paths can
        still recover a key, but ambiguous HTTP URL shapes never do.
    """
    if not original_path:
        return None
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:
        return None

    # Prefer the catalog-pinned logical key. This neither guesses a bucket from
    # a custom endpoint URL nor reapplies the backend's base prefix.
    if storage is not None and isinstance(raw_key, str) and raw_key:
        try:
            reader = _StorageRangeReader(storage, raw_key)
            return pq.read_schema(reader)
        except Exception:
            return None

    s3a = _to_s3a_path(original_path)
    if not s3a.startswith("s3a://"):
        # Local filesystem path: pyarrow reads only the footer from the path.
        try:
            if os.path.isfile(original_path):
                return pq.read_schema(original_path)
        except Exception:
            return None
        return None

    if storage is None:
        return None
    # s3a://bucket/full_key  →  bucket + object key (key already includes any
    # base_prefix).  ``storage.read_bytes`` re-applies base_prefix and uses its
    # own bucket, so strip base_prefix from the key here to avoid doubling it.
    rest = s3a[len("s3a://"):]
    _bucket, _, full_key = rest.partition("/")
    if not full_key:
        return None
    base = (getattr(storage, "base_prefix", "") or "").strip("/")
    rel_key = full_key
    if base and full_key.startswith(base + "/"):
        rel_key = full_key[len(base) + 1:]
    try:
        reader = _StorageRangeReader(storage, rel_key)
        return pq.read_schema(reader)
    except Exception:
        return None


class _StorageRangeReader:
    """Small seekable file-like adapter backed only by bounded range reads."""

    def __init__(self, storage, path: str) -> None:
        self.storage = storage
        self.path = path
        self.position = 0
        self.length = int(storage.size(path))
        self.closed = False

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            target = offset
        elif whence == 1:
            target = self.position + offset
        elif whence == 2:
            target = self.length + offset
        else:
            raise ValueError("invalid seek mode")
        if target < 0:
            raise ValueError("negative seek position")
        self.position = target
        return target

    def read(self, size: int = -1) -> bytes:
        if self.position >= self.length:
            return b""
        if size is None or size < 0:
            size = self.length - self.position
        size = min(size, self.length - self.position)
        data = self.storage.read_range(self.path, self.position, size)
        self.position += len(data)
        return data

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def close(self) -> None:
        self.closed = True


def _parquet_timestamp_units(
    storage,
    original_path: Optional[str],
    raw_key: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """Map ``column -> parquet timestamp unit`` ('s'/'ms'/'us'/'ns') for every
    column whose PARQUET logical type is a timestamp.

    Returns ``None`` when the footer can't be read — the caller then casts only
    ``__timestamp__`` (never the name heuristic).
    """
    schema = _read_parquet_schema(storage, original_path, raw_key=raw_key)
    if schema is None:
        return None
    try:
        import pyarrow.types as patypes
        units: Dict[str, str] = {}
        for i in range(len(schema)):
            field = schema.field(i)
            if patypes.is_timestamp(field.type):
                units[field.name] = getattr(field.type, "unit", "us") or "us"
        return units
    except Exception:
        return None


def _ts_to_timestamp_expr(col_name: str, unit: str) -> str:
    """Spark expression reinterpreting a BIGINT epoch column as TIMESTAMP.

    Uses the explicit ``timestamp_seconds/millis/micros`` builtins rather than
    ``CAST(x AS TIMESTAMP)``: the cast's numeric→timestamp behaviour is both
    seconds-based AND gated behind ``spark.sql.legacy.allowCastNumericToTimestamp``
    in Spark 3.x, whereas the builtins are always enabled and unambiguous about
    the source unit.  Nanoseconds have no Spark timestamp type, so they are
    reduced to microseconds with integer division (``DIV 1000``) first.
    """
    q = _spark_quote_identifier(col_name)
    if unit == "s":
        expr = f"timestamp_seconds({q})"
    elif unit == "ms":
        expr = f"timestamp_millis({q})"
    elif unit == "us":
        expr = f"timestamp_micros({q})"
    else:  # 'ns' or unknown: nanoseconds is the only unit Spark surfaces as BIGINT
        expr = f"timestamp_micros({q} DIV 1000)"
    return f"{expr} AS {q}"


def _build_tscast_select(desc_rows, ts_units: Optional[Dict[str, str]]):
    """Build the projection for the timestamp-cast wrapper view.

    *desc_rows* is Spark ``DESCRIBE`` output (rows whose first two items are the
    column name and Spark type).  *ts_units* maps each genuine parquet-timestamp
    column to its unit, or is ``None`` when the footer couldn't be read.

    A column is converted back to TIMESTAMP iff Spark surfaces it as BIGINT AND
    it is a genuine parquet timestamp (under ``nanosAsLong`` only NANOS columns
    appear as BIGINT); every other column passes through untouched.  When
    *ts_units* is ``None`` the only column treated as a timestamp is the system
    column ``__timestamp__`` (assumed nanoseconds — a non-nanos timestamp would
    already read as TIMESTAMP, not BIGINT), never the name heuristic, so
    epoch-integer columns are always left as BIGINT.

    Returns ``(select_parts, cast_cols)``.
    """
    if ts_units is None:
        ts_units = {"__timestamp__": "ns"}
    select_parts: List[str] = []
    cast_cols: List[str] = []
    for row in desc_rows or []:
        if not row:
            continue
        col_name = str(row[0])
        if not col_name or col_name.startswith("#"):
            # Skip blank / partition-metadata rows Spark DESCRIBE may append.
            continue
        col_type = (
            str(row[1]).strip().upper()
            if len(row) > 1 and row[1] is not None else ""
        )
        if col_type == "BIGINT" and col_name in ts_units:
            select_parts.append(_ts_to_timestamp_expr(col_name, ts_units[col_name]))
            cast_cols.append(col_name)
        else:
            select_parts.append(_spark_quote_identifier(col_name))
    return select_parts, cast_cols


def _spark_rewrite_query(
        original_sql: str,
        alias_to_table: Dict[str, str],
        parsed_expression=None,
        default_super_name: Optional[str] = None,
) -> str:
    """Rewrite table references and transpile SQL to Spark dialect.

    Two-step process:
    1. Delegate to ``rewrite_query_with_hashed_tables`` to replace table
       references with physical hashed names (shared with DuckDB path).
    2. Transpile the resulting SQL from DuckDB dialect to Spark dialect
       via sqlglot.  This converts identifier quoting from double-quotes
       (``"col"``) to backticks (`` `col` ``), and adapts other syntax
       differences (e.g. INTERVAL literals, type names).

    Any rewrite/transpile ambiguity fails closed. Executing the original
    catalog names would bypass the request-private reflection/RBAC views.
    """
    rewritten = rewrite_query_with_hashed_tables(
        original_sql,
        alias_to_table,
        parsed_expression=parsed_expression,
        default_super_name=default_super_name,
    )

    try:
        import sqlglot
        # Parse as DuckDB (which uses double-quote identifiers),
        # generate as Spark (which uses backtick identifiers).
        transpiled = sqlglot.transpile(
            rewritten,
            read="duckdb",
            write="spark",
            pretty=False,
        )
        if transpiled and transpiled[0]:
            return transpiled[0]
    except Exception:
        raise RuntimeError(
            "Unable to transpile protected query for Spark"
        ) from None
    raise RuntimeError("Unable to transpile protected query for Spark")


def _double_quotes_to_backticks(sql: str) -> str:
    """Replace double-quoted identifiers with backtick-quoted identifiers.

    Walks the SQL character-by-character, respecting single-quoted string
    literals (which must not be modified).  Only double-quoted sequences
    are converted.

    Example:
        ``SELECT "product_id", SUM("revenue") AS "total"``
        → ``SELECT `product_id`, SUM(`revenue`) AS `total```
    """
    result = []
    i = 0
    in_single = False
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_single:
            # Enter single-quoted string — copy verbatim until closing '
            in_single = True
            result.append(ch)
            i += 1
            while i < len(sql):
                c2 = sql[i]
                result.append(c2)
                if c2 == "'" and (i + 1 >= len(sql) or sql[i + 1] != "'"):
                    in_single = False
                    i += 1
                    break
                if c2 == "'" and i + 1 < len(sql) and sql[i + 1] == "'":
                    # Escaped single quote — copy both
                    result.append(sql[i + 1])
                    i += 2
                else:
                    i += 1
        elif ch == '"':
            # Double-quoted identifier — convert to backtick
            result.append('`')
            i += 1
            while i < len(sql) and sql[i] != '"':
                result.append(sql[i])
                i += 1
            result.append('`')
            if i < len(sql):
                i += 1  # skip closing "
        else:
            result.append(ch)
            i += 1
    return ''.join(result)


# =========================================================
# S3/MinIO configuration for Spark
# =========================================================

def _configure_spark_session_security(cursor) -> None:
    """Install mandatory fail-closed guards before metadata or user SQL.

    HiveServer2 erases Spark's LTZ/NTZ distinction in result metadata. Pinning
    the session and Parquet inference to UTC/LTZ is therefore part of the
    correctness boundary, not an optional compatibility workaround.
    """
    mandatory_settings = (
        ("spark.sql.variable.substitute", "false"),
        ("spark.sql.session.timeZone", "UTC"),
        ("spark.sql.timestampType", "TIMESTAMP_LTZ"),
        ("spark.sql.parquet.inferTimestampNTZ.enabled", "false"),
    )
    for key, value in mandatory_settings:
        try:
            cursor.execute(f"SET {key}={value}")
        except Exception:
            raise RuntimeError(
                "Spark session security configuration failed"
            ) from None


def _configure_spark_s3(cursor, cluster: Optional[Dict] = None) -> None:
    """Set S3/MinIO configuration on the Spark Thrift session.

    Inline object-store access keys are deliberately rejected.  Session SET
    values are readable through Spark/JVM introspection surfaces and share the
    lifetime of user SQL.  Configure workload identity or a Hadoop credential
    provider on the Spark cluster instead.  Non-secret endpoint/region options
    remain supported for compatibility.

    **Important**: when the cluster dict does not include non-secret overrides,
    the function does not fall back to host-side environment variables
    (e.g. ``STORAGE_ENDPOINT_URL``).  The Spark Thrift Server is typically
    launched with ``--conf spark.hadoop.fs.s3a.*`` set to Docker-internal
    addresses (e.g. ``http://minio:9000``).  Overriding them with the
    host-side ``localhost:9000`` breaks S3 access from within the container.

    To explicitly override non-secret S3 settings, use ``s3_endpoint``,
    ``s3_region``, ``s3_use_ssl`` and ``s3_path_style`` in the cluster
    registration.  Access keys in that document are rejected.
    """
    _cluster = cluster or {}

    try:
        safe_config = validate_spark_storage_config(_cluster)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from None

    # Only SET non-secret values that are explicitly provided in the cluster
    # config.
    # Falling back to host env vars (STORAGE_ENDPOINT_URL etc.) is wrong
    # because those point to localhost which is unreachable from Docker.
    spark_settings = []

    endpoint = safe_config.get("s3_endpoint")
    if endpoint:
        spark_settings.append(("spark.hadoop.fs.s3a.endpoint", endpoint, True))

    region = safe_config.get("s3_region")
    if region:
        spark_settings.append(("spark.hadoop.fs.s3a.endpoint.region", region, True))

    use_ssl = safe_config.get("s3_use_ssl")
    if use_ssl is not None:
        spark_settings.append((
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            use_ssl,
            True,
        ))

    path_style = safe_config.get("s3_path_style")
    if path_style is not None:
        spark_settings.append((
            "spark.hadoop.fs.s3a.path.style.access",
            path_style,
            True,
        ))

    # Always ensure the S3A filesystem impl is set (harmless if already set).
    spark_settings.append((
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
        False,
    ))

    if not spark_settings:
        logger.debug("[spark.thrift] no S3 overrides in cluster config; using spark-submit defaults")

    for key, value, required in spark_settings:
        try:
            sql = f"SET {key}={value}"
            # Values may include private infrastructure names.  Log the setting
            # key only and never echo the SQL or a backend exception that may
            # repeat it.
            logger.debug("[spark.thrift] applying session setting %s", key)
            cursor.execute(sql)
        except Exception as exc:
            logger.debug(
                "[spark.thrift] session setting %s failed (%s)",
                key,
                safe_exception_type(exc),
            )
            if required:
                raise RuntimeError(
                    f"Spark storage session configuration failed for {key}"
                ) from None


def _execute_with_stmt_timeout(
        cursor,
        sql: str,
        conn,
        stmt_timeout_s: int,
        timed_out_event: threading.Event,
        log_prefix: str = "",
) -> None:
    """Execute a single SQL statement with per-statement timeout enforcement.

    If the statement does not complete within *stmt_timeout_s* seconds the
    connection is forcibly closed (which unblocks any pending Thrift RPC)
    and the overall *timed_out_event* is set so upstream code can detect
    the timeout.

    Use this for heavyweight operations (user query, EXPLAIN) that may
    hang.  Lightweight DDL (CREATE VIEW, SET) can rely on the overall
    query-level watchdog instead.
    """
    if timed_out_event.is_set():
        raise RuntimeError("Query already timed out")

    def _on_timeout():
        logger.error(
            f"{log_prefix}[spark.thrift] statement timeout after {stmt_timeout_s}s — "
            "closing connection"
        )
        timed_out_event.set()
        try:
            conn.close()
        except Exception:
            pass

    timer = threading.Timer(stmt_timeout_s, _on_timeout)
    timer.daemon = True
    timer.start()
    try:
        cursor.execute(sql)
    finally:
        timer.cancel()


def _validated_spark_result_metadata(
    description: Any,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[int, ...]]:
    """Return positional PyHive metadata or fail closed on contract drift."""
    fields = tuple(description or ())
    if not fields:
        raise RuntimeError("Spark returned invalid result metadata")

    columns: List[str] = []
    type_codes: List[str] = []
    utc_timestamp_indexes: List[int] = []
    for index, field in enumerate(fields):
        if type(field) not in (list, tuple) or len(field) != 7:
            raise RuntimeError("Spark returned invalid result metadata")
        raw_name = field[0]
        raw_type_code = field[1]
        if type(raw_name) is not str or (
            type(raw_type_code) is not str
            or raw_type_code not in SPARK_THRIFT_TYPE_CODES
        ):
            raise RuntimeError("Spark returned invalid result metadata")
        columns.append(raw_name)
        type_codes.append(raw_type_code)
        if raw_type_code == "TIMESTAMP_TYPE":
            utc_timestamp_indexes.append(index)

    return (
        tuple(columns),
        tuple(type_codes),
        tuple(utc_timestamp_indexes),
    )


def _validated_spark_result_row(
    raw_row: Any,
    column_count: int,
) -> tuple[Any, ...]:
    """Return one exact DB-API row after validating its positional arity."""
    if type(raw_row) not in (list, tuple) or len(raw_row) != column_count:
        raise RuntimeError("Spark returned invalid result row")
    return raw_row if type(raw_row) is tuple else tuple(raw_row)


def _spark_result_dataframe(
    rows: List[Any],
    description: Any,
    *,
    preserve_scalar_identity: bool,
) -> pd.DataFrame:
    """Build a result frame without allowing pandas scalar coercion.

    In particular, pandas' ordinary constructor promotes ``BIGINT + NULL`` to
    float64, which both changes the public logical type and can lose integers
    above 2**53. DB-API rows already contain the correct Python scalars, so an
    object frame is the bounded public facade's lossless compatibility
    container. Unbounded direct executor callers retain pandas' historical
    inferred dtypes. Authoritative Thrift type codes and UTC-instant column
    indexes travel in positional attrs; positions also work with duplicate
    names.
    """
    columns, type_codes, utc_timestamp_indexes = (
        _validated_spark_result_metadata(description)
    )
    for row in rows:
        _validated_spark_result_row(row, len(columns))
    result = pd.DataFrame(
        rows,
        columns=columns,
        dtype=object if preserve_scalar_identity else None,
    )
    result.attrs[SPARK_RESULT_TYPE_CODES_ATTR] = type_codes
    result.attrs[SPARK_UTC_TIMESTAMP_INDEXES_ATTR] = utc_timestamp_indexes
    return result


# =========================================================
# SparkThrift executor
# =========================================================

class SparkThriftExecutor:
    """
    Executes SQL queries against a remote Spark Thrift Server.

    Connects via PyHive to a Spark SQL Thrift Server (HiveServer2 protocol).
    Tables are registered as temporary views from S3/MinIO parquet files.
    RBAC views are created per-query and dropped after execution.

    Cluster selection is automatic from Redis, or a specific cluster_id
    can be provided.
    """

    def __init__(self, storage: Optional[object] = None, organization: str = ""):
        self.storage = storage
        self.organization = organization
        # Redis construction can itself perform Sentinel discovery. Defer it
        # into the deadline-aware cluster-selection worker below.
        self.catalog: Optional[RedisCatalog] = None
        self._catalog_lock = threading.Lock()

    def _get_catalog(self) -> RedisCatalog:
        catalog = self.catalog
        if catalog is not None:
            return catalog
        with self._catalog_lock:
            if self.catalog is None:
                self.catalog = RedisCatalog()
            return self.catalog

    def _get_connection(self, cluster: Dict) -> Any:
        """Create a PyHive connection to the Thrift Server."""
        try:
            from pyhive import hive
        except ImportError:
            raise RuntimeError(
                "PyHive is required for SPARK engine. "
                "Install it with: pip install pyhive[hive]"
            ) from None

        host = cluster["thrift_host"]
        port = int(cluster.get("thrift_port", 10000))

        # Optional authentication
        auth = cluster.get("auth", "NONE")
        username = cluster.get("username")
        password = cluster.get("password")

        connect_kwargs = {
            "host": host,
            "port": port,
            "auth": auth,
        }
        if username:
            connect_kwargs["username"] = username
        if password:
            connect_kwargs["password"] = password

        # Socket timeout prevents the connection from hanging indefinitely
        # when the Thrift server is unreachable or overloaded.
        try:
            socket_timeout = settings.SUPERTABLE_SPARK_CONNECT_TIMEOUT
        except (ValueError, TypeError):
            socket_timeout = 30
        connect_kwargs["configuration"] = {
            "hive.server2.session.check.interval": str(socket_timeout * 1000),
        }

        logger.debug(f"[spark.thrift] connecting to {host}:{port} (auth={auth})")
        return hive.connect(**connect_kwargs)

    def _get_connection_with_deadline(
        self,
        cluster: Dict,
        *,
        deadline_monotonic: float,
        cancel_event: Optional[threading.Event],
    ) -> Any:
        """Create a Thrift connection without abandoning an unbounded worker."""
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("Spark query was cancelled during connection")
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Spark query deadline exceeded during connection")
            if _spark_connect_slots.acquire(timeout=min(0.05, remaining)):
                break

        completed = threading.Event()
        abandoned = threading.Event()
        outcome: Dict[str, Any] = {}
        outcome_lock = threading.Lock()

        def _abandon() -> None:
            late_connection = None
            with outcome_lock:
                abandoned.set()
                late_connection = outcome.pop("connection", None)
            if late_connection is not None:
                try:
                    late_connection.close()
                except Exception:
                    pass

        def _connect() -> None:
            try:
                connection = self._get_connection(cluster)
                close_connection = False
                with outcome_lock:
                    if abandoned.is_set():
                        close_connection = True
                    else:
                        outcome["connection"] = connection
                if close_connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
            except BaseException as exc:
                with outcome_lock:
                    outcome["error"] = exc
            finally:
                completed.set()
                _spark_connect_slots.release()

        worker = threading.Thread(
            target=_connect,
            daemon=True,
            name="spark-thrift-connect",
        )
        try:
            worker.start()
        except BaseException:
            _spark_connect_slots.release()
            raise

        while not completed.is_set():
            if cancel_event is not None and cancel_event.is_set():
                _abandon()
                raise RuntimeError("Spark query was cancelled during connection")
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                _abandon()
                raise RuntimeError("Spark query deadline exceeded during connection")
            completed.wait(min(0.05, remaining))

        error = outcome.get("error")
        if isinstance(error, BaseException):
            raise error
        connection = outcome.get("connection")
        if connection is None:
            raise RuntimeError("Spark connection did not return a connection")
        return connection

    def _select_cluster(self, job_bytes: int, force: bool = False) -> Dict:
        """Select the best cluster from Redis, or raise if none available."""
        cluster = self._get_catalog().select_spark_cluster(
            self.organization, job_bytes, force=force,
        )
        if not cluster:
            raise RuntimeError(
                f"No active Spark cluster available for job size {job_bytes} bytes. "
                "Register a cluster via RedisCatalog.register_spark_cluster()."
            )
        logger.debug(
            f"[spark.thrift] selected cluster: {cluster.get('name', cluster['cluster_id'])} "
            f"(host={cluster['thrift_host']}:{cluster.get('thrift_port', 10000)})"
        )
        return cluster

    def _select_cluster_with_deadline(
        self,
        job_bytes: int,
        *,
        force: bool,
        deadline_monotonic: float,
        cancel_event: Optional[threading.Event],
    ) -> Dict:
        """Select through Redis without extending the caller's query boundary."""
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError(
                    "Spark query was cancelled during cluster selection"
                )
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    "Spark query deadline exceeded during cluster selection"
                )
            if _spark_selection_slots.acquire(timeout=min(0.05, remaining)):
                break

        completed = threading.Event()
        outcome: Dict[str, object] = {}

        def select() -> None:
            try:
                outcome["cluster"] = self._select_cluster(
                    job_bytes, force=force,
                )
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                completed.set()
                _spark_selection_slots.release()

        worker = threading.Thread(
            target=select,
            daemon=True,
            name="spark-cluster-selection",
        )
        try:
            worker.start()
        except BaseException:
            _spark_selection_slots.release()
            raise

        while not completed.is_set():
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError(
                    "Spark query was cancelled during cluster selection"
                )
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    "Spark query deadline exceeded during cluster selection"
                )
            completed.wait(min(0.05, remaining))

        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("Spark query was cancelled during cluster selection")
        if time.monotonic() >= deadline_monotonic:
            raise RuntimeError(
                "Spark query deadline exceeded during cluster selection"
            )
        error = outcome.get("error")
        if isinstance(error, BaseException):
            raise error
        cluster = outcome.get("cluster")
        if not isinstance(cluster, dict):
            raise RuntimeError("Spark cluster selection returned invalid state")
        return cluster

    def execute(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
            force: bool = False,
            deadline_monotonic: Optional[float] = None,
            cancel_event: Optional[threading.Event] = None,
            max_result_rows: Optional[int] = None,
            max_result_bytes: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Execute a query against a Spark Thrift Server.

        Flow:
        1. Select cluster from Redis based on job size
        2. Connect to Thrift Server
        3. Configure S3 credentials on the Spark session
        4. Register parquet files as temp views
        5. Create RBAC-filtered views if needed
        6. Rewrite and execute the user query
        7. Fetch results as pandas DataFrame (incrementally when bounded)
        8. Clean up temp views

        The entire flow is guarded by the earlier of the caller deadline and
        SUPERTABLE_SPARK_QUERY_TIMEOUT (default 300 s), plus the caller's
        cancellation token. If either boundary fires, the connection is
        forcibly closed to unblock pending Thrift RPCs.
        """
        # SuperSnapshot is a compatibility dataclass and direct executor
        # callers can construct it without the catalog validators.  Derive
        # every metadata-backed name now so an invalid version fails before
        # cluster lookup, a Thrift connection, or any cursor SQL statement.
        for snapshot in getattr(reflection, "supers", ()) or ():
            _spark_table_name(
                snapshot.super_name,
                snapshot.simple_name,
                snapshot.simple_version,
            )

        for tombstone in (
            getattr(reflection, "tombstone_views", None) or {}
        ).values():
            active_tombstone, tombstone_format = (
                _validate_spark_tombstone_definition(tombstone)
            )
            if tombstone_format == TOMBSTONE_FORMAT_V2:
                # Preserve the current format-2 policy (including a valid
                # explicit empty state), but only after validating the full
                # discriminated state. Malformed hybrids must not reach setup.
                raise RuntimeError(
                    "Spark does not support active tombstone_format=2 "
                    "deletion vectors"
                )
            if active_tombstone:
                # Reject before parser work, fleet selection, credentials, or
                # a Thrift connection. AUTO already excludes active DVs.
                raise RuntimeError(
                    "Spark deletion-vector reads require composite source-file "
                    "+ row-id identity and are not supported safely"
                )
        # This must precede cluster selection, connection establishment and
        # storage configuration.  In particular, AUTO requests are parsed with
        # DuckDB grammar and only learn that Spark won during routing.
        if settings.SUPERTABLE_SPARK_PRESIGNED:
            raise RuntimeError(
                "SUPERTABLE_SPARK_PRESIGNED is disabled: configure Spark-side "
                "workload identity or a Hadoop credential provider"
            )
        caller_parser = parser
        parser = _revalidate_spark_parser(parser, reflection)
        validate_rbac_binding_stability(
            parser,
            getattr(reflection, "rbac_views", None) or {},
        )

        for value, label in (
            (max_result_rows, "max_result_rows"),
            (max_result_bytes, "max_result_bytes"),
        ):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ValueError(f"{label} must be a positive integer")

        configured_timeout = float(_spark_timeout_seconds())
        if configured_timeout <= 0:
            configured_timeout = 300.0
        now = time.monotonic()
        effective_deadline = now + configured_timeout
        if deadline_monotonic is not None:
            if (
                isinstance(deadline_monotonic, bool)
                or not isinstance(deadline_monotonic, (int, float))
                or not math.isfinite(float(deadline_monotonic))
            ):
                raise ValueError("Spark query deadline must be finite")
            effective_deadline = min(
                effective_deadline, float(deadline_monotonic),
            )
        query_timeout = effective_deadline - now
        if query_timeout <= 0:
            raise RuntimeError("Spark query deadline exceeded before execution")
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("Spark query was cancelled before execution")
        stmt_timeout = _spark_statement_timeout_seconds()

        # 1. Select cluster
        cluster = self._select_cluster_with_deadline(
            reflection.reflection_bytes,
            force=force,
            deadline_monotonic=effective_deadline,
            cancel_event=cancel_event,
        )
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("Spark query was cancelled during cluster selection")
        if time.monotonic() >= effective_deadline:
            raise RuntimeError(
                "Spark query deadline exceeded during cluster selection"
            )
        try:
            validate_spark_storage_config(cluster)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from None
        timer_capture("CONNECTING")

        conn = None
        cursor = None
        created_views: List[str] = []
        created_tables: List[str] = []

        # Watchdog: if the overall timeout fires we forcibly close the
        # connection from a background thread. This causes any blocked
        # cursor.execute / fetchall to raise an exception immediately.
        _timed_out = threading.Event()
        _cancelled = threading.Event()
        _finished = threading.Event()
        _conn_ref: List = []  # mutable container so watchdog can see conn

        def _close_connections() -> None:
            for active_connection in list(_conn_ref):
                try:
                    active_connection.close()
                except Exception:
                    pass

        def _watchdog():
            while not _finished.is_set():
                if cancel_event is not None and cancel_event.is_set():
                    _cancelled.set()
                    _close_connections()
                    return
                remaining = effective_deadline - time.monotonic()
                if remaining <= 0:
                    _timed_out.set()
                    logger.error(
                        f"{log_prefix}[spark.thrift] query deadline exceeded — "
                        "forcibly closing connection"
                    )
                    _close_connections()
                    return
                _finished.wait(min(0.05, remaining))

        watchdog = threading.Thread(target=_watchdog, daemon=True, name="spark-timeout")
        watchdog.start()

        try:
            # 2. Connect
            conn = self._get_connection_with_deadline(
                cluster,
                deadline_monotonic=effective_deadline,
                cancel_event=cancel_event,
            )
            _conn_ref.append(conn)
            if cancel_event is not None and cancel_event.is_set():
                _cancelled.set()
                raise RuntimeError("Spark query was cancelled")
            if _timed_out.is_set():
                raise RuntimeError("Spark query deadline exceeded")
            cursor = conn.cursor()

            # 3. Disable Spark/Hive/system/environment variable substitution
            # before issuing any statement containing metadata-derived values.
            # The lexical fence above is still required because this setting
            # is session-local and a compromised/misconfigured server could
            # refuse it.
            _configure_spark_session_security(cursor)

            # 3a. Configure non-secret S3 options. Credentials must already be
            # installed cluster-side through workload identity/provider APIs.
            _configure_spark_s3(cursor, cluster)

            # 3b. Work around Spark's inability to read TIMESTAMP(NANOS, false)
            #     parquet columns written by DuckDB.  DuckDB writes
            #     __timestamp__ and other timestamp columns as INT64
            #     nanosecond-precision non-UTC-adjusted timestamps.
            #     Spark 3.x rejects this type by default.
            #
            #     spark.sql.legacy.parquet.nanosAsLong=true  (Spark 3.4+)
            #       → reads nanos INT64 as LongType instead of crashing.
            #     inferTimestampNTZ=false is installed as a mandatory session
            #     correctness guard above, alongside UTC/TIMESTAMP_LTZ.
            #     spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
            #       → if Spark writes parquet, use micros (harmless if read-only).
            _timestamp_workarounds = [
                ("spark.sql.legacy.parquet.nanosAsLong", "true"),
                ("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"),
            ]
            for _tw_key, _tw_val in _timestamp_workarounds:
                try:
                    cursor.execute(f"SET {_tw_key}={_tw_val}")
                except Exception as _tw_err:
                    logger.debug(
                        f"{log_prefix}[spark.thrift] SET {_tw_key} failed "
                        f"(non-fatal, {safe_exception_type(_tw_err)})"
                    )

            # 4. Register reflection tables
            snapshots_by_key = {
                (sup.super_name, sup.simple_name): sup
                for sup in reflection.supers
            }
            table_defs = parser.get_table_tuples()
            alias_to_table_name: Dict[str, str] = {}
            # table_name -> a representative ORIGINAL (pre-s3a) snapshot file,
            # used to read the parquet footer once per table for type-based
            # timestamp detection in step 4a below.
            table_repr_file: Dict[str, tuple[str, Optional[str]]] = {}

            for td in table_defs:
                key = (td.super_name, td.simple_name)
                sup = snapshots_by_key.get(key)
                if not sup:
                    continue

                table_name = _spark_table_name(
                    sup.super_name, sup.simple_name, sup.simple_version,
                )

                # Resolve to a credential-less backend URI.  Estimator input may
                # itself be a DuckDB presigned URL; its query string is removed
                # during conversion and an ambiguous signed form fails closed.
                raw_keys = list(getattr(sup, "resource_keys", ()) or ())
                if raw_keys and len(raw_keys) != len(sup.files):
                    raise RuntimeError(
                        "Resolved Spark files and stable resource keys must "
                        "correspond one-for-one"
                    )
                # Keep one ORIGINAL path plus its catalog-pinned logical key for
                # the footer read that drives timestamp detection. The key is
                # authoritative for ambiguous custom endpoint URL shapes.
                if sup.files and table_name not in table_repr_file:
                    table_repr_file[table_name] = (
                        sup.files[0],
                        raw_keys[0] if raw_keys else None,
                    )
                files = [
                    _resolve_spark_file(
                        self.storage,
                        f,
                        raw_key=(raw_keys[idx] if raw_keys else None),
                    )
                    for idx, f in enumerate(sup.files)
                ]

                logger.debug(
                    f"{log_prefix}[spark.thrift] creating view {table_name} "
                    f"({len(files)} file(s))"
                )
                if files:
                    intermediates = _spark_create_parquet_view(
                        cursor,
                        table_name,
                        files,
                        column_types=dict(
                            getattr(sup, "column_types", {}) or {}
                        ),
                    )
                else:
                    _spark_create_empty_view(
                        cursor,
                        table_name,
                        dict(getattr(sup, "column_types", {}) or {}),
                    )
                    intermediates = []
                created_tables.append(table_name)
                # Track intermediate views (part + batch) for cleanup after query
                created_views.extend(intermediates)
                alias_to_table_name[td.alias] = table_name

                if _timed_out.is_set():
                    raise RuntimeError(
                        f"Spark query timed out after {query_timeout}s "
                        f"during view creation"
                    )

            timer_capture("CREATING_REFLECTION")

            # 4a. Wrap parquet views to convert nanosecond-epoch BIGINT columns
            #     back to TIMESTAMP.  When nanosAsLong=true, Spark reads
            #     TIMESTAMP(NANOS) parquet columns as BIGINT (epoch nanoseconds);
            #     genuine micros/millis timestamps already read as TIMESTAMP and
            #     need no wrapper.  Downstream queries expect DATE/TIMESTAMP
            #     semantics (e.g. WHERE stat_date >= CURRENT_DATE - INTERVAL '7'
            #     DAYS), so we wrap the view to convert the BIGINT-surfaced ones.
            #     Conversion targets are detected by ACTUAL parquet logical type
            #     (read once per table from the file footer), never by name — so
            #     epoch-integer columns named like timestamps (created_at,
            #     source_ts_ms) stay BIGINT.  If the footer can't be read we
            #     convert only the system column __timestamp__ (never the name
            #     heuristic).  The wrapper replaces the view name in
            #     alias_to_table_name so all downstream logic (RBAC, dedup,
            #     tombstone, user query) sees proper TIMESTAMP columns.
            _ts_units_cache: Dict[str, Optional[Dict[str, str]]] = {}
            for alias, table_name in list(alias_to_table_name.items()):
                try:
                    cursor.execute(f"DESCRIBE {table_name}")
                    desc_rows = cursor.fetchall()

                    if table_name not in _ts_units_cache:
                        representative = table_repr_file.get(table_name)
                        _ts_units_cache[table_name] = _parquet_timestamp_units(
                            self.storage,
                            representative[0] if representative else None,
                            raw_key=(representative[1] if representative else None),
                        )
                        if (
                            representative is not None
                            and _ts_units_cache[table_name] is None
                        ):
                            raise RuntimeError(
                                "Spark timestamp metadata could not be verified"
                            )
                    ts_units = _ts_units_cache[table_name]

                    select_parts, cast_cols = _build_tscast_select(desc_rows, ts_units)

                    if cast_cols:
                        wrapper_name = f"__{table_name}_tscast__"
                        wrapper_sql = (
                            f"CREATE OR REPLACE TEMPORARY VIEW {wrapper_name} AS "
                            f"SELECT {', '.join(select_parts)} FROM {table_name}"
                        )
                        cursor.execute(wrapper_sql)
                        created_views.append(wrapper_name)
                        alias_to_table_name[alias] = wrapper_name

                        logger.debug(
                            f"{log_prefix}[spark.thrift] timestamp wrapper for {table_name}: "
                            f"converted {len(cast_cols)} parquet-timestamp column(s) "
                            f"{cast_cols} back to TIMESTAMP"
                        )
                except Exception as cast_err:
                    logger.debug(
                        f"{log_prefix}[spark.thrift] timestamp wrapper "
                        f"failed for {table_name} "
                        f"({safe_exception_type(cast_err)})"
                    )
                    if _ts_units_cache.get(table_name):
                        raise RuntimeError(
                            "Spark timestamp normalization failed"
                        ) from None

            logger.info(
                f"{log_prefix}[spark.thrift] registered {len(alias_to_table_name)} table(s), "
                f"{len(created_views)} intermediate views"
            )

            # 5. Create RBAC views
            query_alias_to_name = dict(alias_to_table_name)
            # Keep the full request UUID: temporary-view name collisions can
            # replace another query's tombstone/RBAC view and alter its result.
            query_suffix = uuid.uuid4().hex

            # 5a. Tombstone / system-column views — created for EVERY alias so
            # the system columns (__rowid__/__timestamp__/__file__) are always stripped
            # and the deletion-vector (when present) is anti-joined out.  Built
            # on the reflection table directly, before RBAC.
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            for alias in list(query_alias_to_name.keys()):
                source = query_alias_to_name[alias]
                tomb_def = tombstone_views.get(alias)
                tomb_view = f"tomb_{source}_{query_suffix}"
                _spark_create_tombstone_view(cursor, source, tomb_view, tomb_def, self.storage)
                created_views.append(tomb_view)
                query_alias_to_name[alias] = tomb_view

            # 5b. RBAC views (column + row filtering) on top of stripped data.
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            if rbac_views:
                for alias in list(query_alias_to_name.keys()):
                    view_def = rbac_views.get(alias)
                    if view_def:
                        source = query_alias_to_name[alias]
                        view_name = f"rbac_{source}_{query_suffix}"
                        _spark_create_rbac_view(cursor, source, view_name, view_def)
                        created_views.append(view_name)
                        query_alias_to_name[alias] = view_name

            if _timed_out.is_set():
                raise RuntimeError(
                    f"Spark query timed out after {query_timeout}s "
                    f"during tombstone/RBAC view creation"
                )

            # 6. Rewrite and execute
            executing_query = _spark_rewrite_query(
                parser.original_query,
                query_alias_to_name,
                parsed_expression=getattr(parser, "_parsed", None),
                default_super_name=parser.default_super_name,
            )
            setattr(parser, "executing_query", executing_query)
            if caller_parser is not parser:
                setattr(caller_parser, "executing_query", executing_query)

            logger.debug(
                f"{log_prefix}[spark.thrift] protected user query prepared"
            )

            # 6a. Capture Spark query plan via EXPLAIN EXTENDED.
            #     Runs the Catalyst optimizer without executing the query
            #     (<50ms overhead).  The plan text is written to the same
            #     local path that DuckDB uses for its JSON profile, so
            #     plan_extender reads it uniformly.
            try:
                _execute_with_stmt_timeout(
                    cursor, f"EXPLAIN EXTENDED {executing_query}",
                    conn, stmt_timeout, _timed_out, log_prefix,
                )
                explain_rows = cursor.fetchall()
                explain_text = "\n".join(
                    str(row[0]) if row else "" for row in explain_rows
                )

                # Parse EXPLAIN output into sections delimited by
                # "== <Section Name> ==" headers.
                plan_sections = {}
                current_section = "raw"
                current_lines: List[str] = []
                for line in explain_text.split("\n"):
                    stripped = line.strip()
                    if stripped.startswith("== ") and stripped.endswith(" =="):
                        if current_lines:
                            plan_sections[current_section] = "\n".join(current_lines)
                        current_section = stripped[3:-3].strip().lower().replace(" ", "_")
                        current_lines = []
                    else:
                        current_lines.append(line)
                if current_lines:
                    plan_sections[current_section] = "\n".join(current_lines)

                spark_plan = {
                    "engine": "spark_sql",
                    "parsed_logical_plan": _redact_spark_plan_text(
                        plan_sections.get("parsed_logical_plan", "")
                    ),
                    "analyzed_logical_plan": _redact_spark_plan_text(
                        plan_sections.get("analyzed_logical_plan", "")
                    ),
                    "optimized_logical_plan": _redact_spark_plan_text(
                        plan_sections.get("optimized_logical_plan", "")
                    ),
                    "physical_plan": _redact_spark_plan_text(
                        plan_sections.get("physical_plan", "")
                    ),
                }

                # Write to the local plan path so plan_extender finds it.
                if query_manager and query_manager.query_plan_path:
                    try:
                        os.makedirs(os.path.dirname(query_manager.query_plan_path), exist_ok=True)
                        with open(query_manager.query_plan_path, "w", encoding="utf-8") as fh:
                            json.dump(spark_plan, fh, ensure_ascii=False)
                        logger.debug(
                            f"{log_prefix}[spark.thrift] query plan saved to "
                            f"{local_path_metadata(query_manager.query_plan_path)}"
                        )
                    except Exception as plan_write_err:
                        logger.debug(
                            f"{log_prefix}[spark.thrift] failed to write plan "
                            f"JSON ({safe_exception_type(plan_write_err)})"
                        )
            except Exception as explain_err:
                # EXPLAIN failure must never block the actual query.
                logger.debug(
                    f"{log_prefix}[spark.thrift] EXPLAIN EXTENDED failed "
                    f"(non-fatal, {safe_exception_type(explain_err)})"
                )

            _execute_with_stmt_timeout(
                cursor, executing_query,
                conn, stmt_timeout, _timed_out, log_prefix,
            )

            if _timed_out.is_set():
                raise RuntimeError(
                    f"Spark query timed out after {query_timeout}s "
                    f"during query execution"
                )

            # 7. Fetch results as pandas
            result_description = tuple(cursor.description or ())
            columns, _result_type_codes, _utc_timestamp_indexes = (
                _validated_spark_result_metadata(result_description)
            )
            if max_result_rows is None and max_result_bytes is None:
                rows = cursor.fetchall()
            else:
                fetchmany = getattr(cursor, "fetchmany", None)
                if not callable(fetchmany):
                    raise RuntimeError(
                        "Spark driver does not support bounded result fetching"
                    )
                upstream_fetch_rows = (
                    256
                    if max_result_rows is None
                    else max(1, min(256, max_result_rows + 1))
                )
                # PyHive's fetchmany(n) delegates to fetchone(), while
                # fetchone() fills its private buffer using cursor.arraysize
                # (1,000 by default).  Reduce that upstream Thrift request too,
                # otherwise a nominal 256-row fetch can silently prefetch 1,000
                # variable-width rows before the byte budget sees any of them.
                try:
                    cursor.arraysize = upstream_fetch_rows
                    if int(cursor.arraysize) != upstream_fetch_rows:
                        raise ValueError("cursor rejected bounded arraysize")
                except Exception:
                    raise RuntimeError(
                        "Spark driver does not support bounded result fetching"
                    ) from None
                rows = []
                serialized_bytes = len(json.dumps(
                    {"columns": columns, "rows": []},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8"))
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        _cancelled.set()
                        raise RuntimeError("Spark query was cancelled")
                    if _timed_out.is_set() or time.monotonic() >= effective_deadline:
                        _timed_out.set()
                        raise RuntimeError("Spark query deadline exceeded")
                    remaining_rows = (
                        256
                        if max_result_rows is None
                        else max_result_rows + 1 - len(rows)
                    )
                    batch = fetchmany(max(1, min(256, remaining_rows)))
                    if not batch:
                        break
                    for raw_row in batch:
                        if (
                            max_result_rows is not None
                            and len(rows) >= max_result_rows
                        ):
                            raise RuntimeError(
                                "Spark result exceeded the materialized row limit"
                            )
                        row = _validated_spark_result_row(
                            raw_row,
                            len(columns),
                        )
                        if max_result_bytes is not None:
                            try:
                                encoded = json.dumps(
                                    row,
                                    ensure_ascii=False,
                                    allow_nan=True,
                                    default=str,
                                    separators=(",", ":"),
                                ).encode("utf-8")
                            except Exception:
                                raise RuntimeError(
                                    "Spark result could not be bounded safely"
                                ) from None
                            serialized_bytes += len(encoded) + (1 if rows else 0)
                            if serialized_bytes > max_result_bytes:
                                raise RuntimeError(
                                    "Spark result memory exceeds the materialized "
                                    "byte limit"
                                )
                        rows.append(row)

            result = _spark_result_dataframe(
                rows,
                result_description,
                preserve_scalar_identity=(
                    max_result_rows is not None
                    or max_result_bytes is not None
                ),
            )

            logger.info(
                f"{log_prefix}[spark.thrift] query complete: "
                f"{len(rows)} rows, {len(columns)} cols"
            )

            return result

        except Exception as e:
            if _cancelled.is_set() or (
                cancel_event is not None and cancel_event.is_set()
            ):
                raise RuntimeError("Spark query was cancelled") from None
            if _timed_out.is_set():
                raise RuntimeError(
                    "Spark query deadline exceeded"
                ) from None
            # Direct executor callers deserve the same credential-safe error
            # boundary as DataReader/API callers.  Suppress the original cause
            # because some DB-API errors embed the failed SQL statement.
            safe_message = _redact_spark_sensitive_text(e)
            raise RuntimeError(safe_message or "Spark query failed") from None

        finally:
            # 8. Cleanup: drop RBAC views and temp tables
            if cursor:
                for view in created_views:
                    try:
                        cursor.execute(f"DROP VIEW IF EXISTS {view}")
                    except Exception:
                        pass
                for table in created_tables:
                    try:
                        cursor.execute(f"DROP VIEW IF EXISTS {table}")
                    except Exception:
                        pass
                try:
                    cursor.close()
                except Exception:
                    pass

            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            _finished.set()
            watchdog.join(timeout=0.2)
            if _cancelled.is_set() or (
                cancel_event is not None and cancel_event.is_set()
            ):
                raise RuntimeError("Spark query was cancelled") from None
            if _timed_out.is_set():
                raise RuntimeError("Spark query deadline exceeded") from None
