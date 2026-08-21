# route: supertable.engine.engine_common

from __future__ import annotations

import hashlib
import ast
import os
import re
import stat
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse, urlsplit, urlunsplit

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB as SQLGlotDuckDB
from sqlglot.generator import csv as sqlglot_csv
from sqlglot.optimizer.scope import traverse_scope

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.config.homedir import get_app_home
from supertable.data_classes import MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES
from supertable.engine.engine_config import normalize_memory_size
from supertable.tombstone_manifest_v2 import (
    MAX_JSON_EXACT_INTEGER,
    MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS,
    TOMBSTONE_FORMAT_V2,
    validate_logical_storage_path,
    validate_snapshot_tombstone_state,
)
from supertable.utils.sql_parser import (
    _build_scoped_table_bindings,
    validate_read_query_ast,
)


# =========================================================
# SQL helpers
# =========================================================

def quote_if_needed(col: str) -> str:
    """Quote a column name if it contains special characters."""
    col = col.strip()
    if col == "*":
        return "*"
    if all(ch.isalnum() or ch == "_" for ch in col):
        return col
    return '"' + col.replace('"', '""') + '"'


def validate_rbac_binding_stability(
    parser: object,
    rbac_views: object,
) -> None:
    """Deny column policies that can change SQL identifier binding.

    DuckDB accepts several schema-dependent unqualified forms: a physical
    input can take precedence over a same-named SELECT alias, and a relation
    alias can become a whole-row struct only when no physical column has that
    name.  Removing the physical column in an RBAC view must not silently turn
    the user's original query (including an originally ambiguous/invalid one)
    into different executable SQL.

    The parser records only supported lexical binding candidates.  Do not
    recurse through arbitrary AST or policy payloads here: malformed maps fail
    closed and unrestricted/row-filter-only views leave binding unchanged.
    """
    # An explicitly empty production view map cannot remove a column and thus
    # cannot affect binding.  Short-circuiting also keeps unrestricted callers
    # independent of this schema-dependent proof machinery.
    if isinstance(rbac_views, dict) and not rbac_views:
        return

    getter = getattr(parser, "get_binding_ambiguities", None)
    if not callable(getter):
        # Execution boundaries reparse with the production parser.  A direct
        # legacy/test parser lacking this proof cannot safely be paired with a
        # column-restricting view.
        ambiguities: object = {}
    else:
        ambiguities = getter()
    if not isinstance(ambiguities, dict):
        raise PermissionError("Unable to validate protected SQL name binding")
    if not ambiguities:
        return
    if not isinstance(rbac_views, dict):
        raise PermissionError("Unable to validate protected SQL name binding")

    views_by_alias = {
        str(alias).casefold(): view
        for alias, view in rbac_views.items()
    }
    for alias, raw_names in ambiguities.items():
        view = views_by_alias.get(str(alias).casefold())
        if view is None:
            continue
        if not isinstance(raw_names, (set, frozenset, list, tuple)):
            raise PermissionError("Unable to validate protected SQL name binding")
        allowed_raw = getattr(view, "allowed_columns", None)
        excluded_raw = getattr(view, "excluded_columns", None)
        if not isinstance(allowed_raw, (list, tuple)) or not isinstance(
            excluded_raw, (list, tuple)
        ):
            raise PermissionError("Unable to validate protected SQL name binding")
        allowed = {str(name).casefold() for name in allowed_raw}
        excluded = {str(name).casefold() for name in excluded_raw}
        wildcard = "*" in allowed
        for raw_name in raw_names:
            if not isinstance(raw_name, str) or not raw_name:
                raise PermissionError(
                    "Unable to validate protected SQL name binding"
                )
            name = raw_name.casefold()
            if name in excluded or (not wildcard and name not in allowed):
                raise PermissionError(
                    "Column policy would change SQL name binding; qualify the "
                    "reference or use a non-conflicting alias"
                )


def sanitize_sql_string(value_sql: str) -> str:
    """Sanitize a SQL string value by escaping single quotes.

    Only operates on already-quoted string literals (values that start with a
    single quote).  Bare SQL keywords (TRUE, FALSE) and numeric literals are
    returned unchanged so callers can embed them directly in SET statements.
    """
    if value_sql.startswith("'"):
        inner = value_sql[1:-1].replace("'", "''")
        return f"'{inner}'"
    return value_sql


def escape_parquet_path(path: str) -> str:
    """Escape a file path for use in SQL string literals."""
    return path.replace(chr(39), chr(39) + chr(39))


_URL_IN_TEXT_RE = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)


def redact_url_credentials(value: object) -> str:
    """Remove presign/SAS query strings before a value reaches a log."""
    text = str(value or "")

    def replace(match: re.Match) -> str:
        raw = match.group(0)
        url = raw.rstrip(").,;]}")
        suffix = raw[len(url):]
        try:
            parsed = urlsplit(url)
            netloc = parsed.netloc
            if parsed.username is not None or parsed.password is not None:
                host = parsed.hostname or ""
                if ":" in host and not host.startswith("["):
                    host = f"[{host}]"
                try:
                    port = parsed.port
                except ValueError:
                    port = None
                netloc = host + (f":{port}" if port is not None else "")
            if (
                parsed.query
                or parsed.fragment
                or netloc != parsed.netloc
            ):
                url = urlunsplit(
                    (
                        parsed.scheme,
                        netloc,
                        parsed.path,
                        "<redacted>" if parsed.query else "",
                        "",
                    )
                )
        except Exception:
            if "?" in url:
                url = url.split("?", 1)[0] + "?<redacted>"
        return url + suffix

    return _URL_IN_TEXT_RE.sub(replace, text)


_SNAPSHOT_TO_DUCKDB_TYPE = {
    "string": "VARCHAR",
    "boolean": "BOOLEAN",
    "byte": "TINYINT",
    "short": "SMALLINT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "binary": "BLOB",
    # Polars ``str(dtype)`` values emitted by collect_schema.
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    # Match DuckDB's types when it scans Parquet emitted by Polars.  Duration
    # is parameterized and is handled below after validating its time unit.
    "null": "INTEGER",
    "time": "TIME",
    "categorical": "VARCHAR",
    "string": "VARCHAR",
    "utf8": "VARCHAR",
    # Common pandas schema strings retained by older snapshots.
    "bool": "BOOLEAN",
    "object": "VARCHAR",
    "datetime64[ns]": "TIMESTAMP_NS",
}


class _PolarsDTypeParser:
    """Tiny closed grammar for persisted ``str(polars_dtype)`` values."""

    def __init__(self, text: str):
        self.text = text
        self.pos = 0

    def _ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def _take(self, token: str) -> None:
        self._ws()
        if not self.text.startswith(token, self.pos):
            raise ValueError(f"expected {token!r} at offset {self.pos}")
        self.pos += len(token)

    def _ident(self) -> str:
        self._ws()
        match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.text[self.pos:])
        if not match:
            raise ValueError(f"expected type identifier at offset {self.pos}")
        self.pos += len(match.group(0))
        return match.group(0)

    def _string(self) -> str:
        self._ws()
        if self.pos >= len(self.text) or self.text[self.pos] not in "'\"":
            raise ValueError(f"expected quoted field name at offset {self.pos}")
        start = self.pos
        quote = self.text[self.pos]
        self.pos += 1
        escaped = False
        while self.pos < len(self.text):
            ch = self.text[self.pos]
            self.pos += 1
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                value = ast.literal_eval(self.text[start:self.pos])
                if not isinstance(value, str):
                    raise ValueError("struct field name is not a string")
                return value
        raise ValueError("unterminated struct field name")

    def _balanced_args(self) -> str:
        self._take("(")
        start = self.pos
        depth = 1
        quote = None
        escaped = False
        while self.pos < len(self.text):
            ch = self.text[self.pos]
            if quote:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    value = self.text[start:self.pos]
                    self.pos += 1
                    return value
            self.pos += 1
        raise ValueError("unterminated type parameters")

    def parse_type(self):
        name = self._ident()
        lowered = name.casefold()
        self._ws()
        if lowered == "list":
            self._take("(")
            inner = self.parse_type()
            self._take(")")
            return ("list", inner)
        if lowered == "array":
            self._take("(")
            inner = self.parse_type()
            self._take(",")
            if self._ident().casefold() != "shape":
                raise ValueError("Array requires shape=")
            self._take("=")
            self._take("(")
            shape = []
            while True:
                self._ws()
                match = re.match(r"[1-9][0-9]*", self.text[self.pos:])
                if not match:
                    break
                shape.append(int(match.group(0)))
                self.pos += len(match.group(0))
                self._ws()
                if not self.text.startswith(",", self.pos):
                    break
                self.pos += 1
            self._take(")")
            self._take(")")
            if not shape:
                raise ValueError("Array shape must contain positive dimensions")
            return ("array", inner, tuple(shape))
        if lowered == "struct":
            self._take("(")
            self._take("{")
            fields = []
            self._ws()
            while not self.text.startswith("}", self.pos):
                field_name = self._string()
                self._take(":")
                fields.append((field_name, self.parse_type()))
                self._ws()
                if self.text.startswith(",", self.pos):
                    self.pos += 1
                    self._ws()
                    continue
                break
            self._take("}")
            self._take(")")
            if not fields:
                raise ValueError("empty Struct has no representable field schema")
            return ("struct", tuple(fields))
        if lowered in {"decimal", "datetime", "duration", "enum"}:
            args = self._balanced_args()
            return (lowered, args)
        if self.pos < len(self.text) and self.text[self.pos] == "(":
            raise ValueError(f"unsupported parameterized dtype {name!r}")
        return ("scalar", name)

    def parse(self):
        node = self.parse_type()
        self._ws()
        if self.pos != len(self.text):
            raise ValueError(f"unexpected dtype text at offset {self.pos}")
        return node


def _dtype_ast(type_name: str):
    return _PolarsDTypeParser(str(type_name or "").strip()).parse()


def _decimal_from_args(args: str) -> tuple[int, int]:
    normalized = args.strip().lower()
    match = re.fullmatch(r"(\d{1,3}),\s*(\d{1,3})", normalized)
    if not match:
        match = re.fullmatch(
            r"precision=(\d{1,3}),\s*scale=(\d{1,3})", normalized,
        )
    if not match:
        raise ValueError("invalid Decimal parameters")
    precision, scale = map(int, match.groups())
    if not (1 <= precision <= 38 and 0 <= scale <= precision):
        raise ValueError("Decimal precision/scale out of range")
    return precision, scale


def _datetime_has_timezone(args: str) -> bool:
    match = re.fullmatch(
        r"time_unit='(?:ns|us|ms)',\s*time_zone=(none|'[^']+')",
        args.strip().lower(),
    )
    if not match:
        raise ValueError("invalid Datetime parameters")
    return match.group(1) != "none"


def _duration_unit(args: str) -> str:
    """Validate the closed set of units emitted by ``str(pl.Duration)``."""
    match = re.fullmatch(
        r"time_unit\s*=\s*(['\"])(ns|us|ms)\1",
        args.strip().lower(),
    )
    if not match:
        raise ValueError("invalid Duration parameters")
    return match.group(2)


def _validate_enum_args(args: str) -> None:
    """Accept only the literal string category list emitted by Polars."""
    match = re.fullmatch(r"categories\s*=\s*(.+)", args.strip(), re.DOTALL)
    if not match:
        raise ValueError("invalid Enum parameters")
    try:
        categories = ast.literal_eval(match.group(1))
    except (SyntaxError, ValueError) as exc:
        raise ValueError("invalid Enum category list") from exc
    if (
        not isinstance(categories, list)
        or any(not isinstance(category, str) for category in categories)
        or len(categories) != len(set(categories))
    ):
        raise ValueError("invalid Enum category list")


def _render_dtype(node, dialect: str) -> str:
    kind = node[0]
    if kind == "scalar":
        normalized = node[1].casefold()
        if dialect == "duckdb":
            value = _SNAPSHOT_TO_DUCKDB_TYPE.get(normalized)
        else:
            # Deliberately omit Polars Null and Time.  Spark rejects their
            # Parquet logical annotations, so inventing an empty-only type
            # would make delete-all queries behave unlike non-empty snapshots.
            value = {
                "string": "string", "utf8": "string", "boolean": "boolean",
                "bool": "boolean", "byte": "byte", "int8": "byte",
                "short": "short", "int16": "short", "integer": "int",
                "int": "int", "int32": "int", "long": "long", "int64": "long",
                "uint8": "short", "uint16": "int", "uint32": "long",
                "uint64": "decimal(20,0)", "float": "float", "float32": "float",
                "double": "double", "float64": "double", "date": "date",
                "timestamp": "timestamp", "binary": "binary", "object": "string",
                "datetime64[ns]": "timestamp", "categorical": "string",
            }.get(normalized)
        if not value:
            raise ValueError(f"unsupported scalar dtype {node[1]!r}")
        return value
    if kind == "decimal":
        precision, scale = _decimal_from_args(node[1])
        return f"DECIMAL({precision},{scale})" if dialect == "duckdb" else f"decimal({precision},{scale})"
    if kind == "datetime":
        with_tz = _datetime_has_timezone(node[1])
        if dialect == "duckdb":
            return "TIMESTAMPTZ" if with_tz else "TIMESTAMP"
        return "timestamp"
    if kind == "duration":
        _duration_unit(node[1])
        # Polars writes Duration to Parquet as its signed INT64 storage value.
        # DuckDB and Spark expose that physical representation as BIGINT/long;
        # INTERVAL would silently change the non-empty table's query type.
        return "BIGINT" if dialect == "duckdb" else "long"
    if kind == "enum":
        _validate_enum_args(node[1])
        # Polars materializes both Enum and Categorical Parquet columns as
        # strings for these engines; category metadata is not a SQL enum type.
        return "VARCHAR" if dialect == "duckdb" else "string"
    if kind == "list":
        inner = _render_dtype(node[1], dialect)
        return f"{inner}[]" if dialect == "duckdb" else f"array<{inner}>"
    if kind == "array":
        inner = _render_dtype(node[1], dialect)
        if dialect == "duckdb":
            return inner + "".join(f"[{size}]" for size in node[2])
        # Spark has no fixed-size array type; its array preserves element
        # semantics for a typed empty relation, which is all we construct here.
        for _ in node[2]:
            inner = f"array<{inner}>"
        return inner
    if kind == "struct":
        if dialect == "duckdb":
            fields = ", ".join(
                f"{quote_if_needed(name)} {_render_dtype(child, dialect)}"
                for name, child in node[1]
            )
            return f"STRUCT({fields})"
        fields = ", ".join(
            f"`{name.replace('`', '``')}`:{_render_dtype(child, dialect)}"
            for name, child in node[1]
        )
        return f"struct<{fields}>"
    raise ValueError(f"unsupported dtype node {kind!r}")


def snapshot_duckdb_type(type_name: str) -> str:
    """Map persisted Spark/Polars types to safe DuckDB SQL."""
    normalized = str(type_name or "").strip().lower()
    direct = _SNAPSHOT_TO_DUCKDB_TYPE.get(normalized)
    if direct:
        return direct
    try:
        return _render_dtype(_dtype_ast(type_name), "duckdb")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Unsupported snapshot column type for empty table: {type_name!r}"
        ) from exc


def snapshot_spark_type(type_name: str) -> str:
    """Map persisted Spark/Polars types to a closed, safe Spark SQL type."""
    try:
        return _render_dtype(_dtype_ast(type_name), "spark")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Unsupported snapshot column type for empty Spark table: {type_name!r}"
        ) from exc


def create_typed_empty_view(
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        column_types: Dict[str, str],
        columns: Optional[List[str]] = None,
) -> None:
    """Create a typed zero-row view for a valid snapshot with no resources."""
    if not column_types:
        raise RuntimeError(
            "Cannot construct an empty reflection: pinned snapshot has no schema"
        )
    requested = None if not columns else {str(c).lower() for c in columns}
    expressions = []
    for name, type_name in column_types.items():
        if requested is not None and str(name).lower() not in requested:
            continue
        expressions.append(
            f"CAST(NULL AS {snapshot_duckdb_type(type_name)}) AS {quote_if_needed(str(name))}"
        )
    if not expressions:
        raise RuntimeError(
            "Cannot construct an empty reflection: requested columns are absent from schema"
        )
    con.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT "
        + ", ".join(expressions)
        + " WHERE FALSE;"
    )


# =========================================================
# Table naming
# =========================================================

def hashed_table_name(
        super_name: str,
        simple_name: str,
        simple_version: int,
        columns: Optional[List[str]] = None,
) -> str:
    """Generate a deterministic table name from (super, simple, version, columns)."""
    cols_part = ",".join(sorted(columns)) if columns else "*"
    key = f"{super_name}_{simple_name}_{simple_version}_{cols_part}"
    digest = hashlib.sha1(
        key.encode("utf-8"), usedforsecurity=False
    ).hexdigest()[:16]
    return f"st_{digest}"


def pro_table_name(
        super_name: str,
        simple_name: str,
        simple_version: int,
        file_signature: str = "",
) -> str:
    """Generate a deterministic IslandDB view name for an exact snapshot.

    A catalog version alone is insufficient: fail-open snapshot recovery and
    mocked/legacy catalogs can return a different file list at the same
    version.  Including the exact file signature prevents replacing an
    in-flight view with different data under the same DuckDB identifier.
    """
    key = f"{super_name}_{simple_name}_{file_signature}"
    # Full SHA-256 keeps this identifier a practical ownership boundary. A
    # truncated name can collide across survivor signatures; reusing or
    # replacing that view would return the wrong file set to a SELECT.
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"pro_{digest}_v{simple_version}"


# =========================================================
# S3 / httpfs detection
# =========================================================

def normalize_endpoint_for_s3(ep: str) -> str:
    """Strip scheme from endpoint, return host:port."""
    if not ep:
        return ep
    u = urlparse(ep if "://" in ep else f"//{ep}")
    host = u.hostname or ep
    port = f":{u.port}" if u.port else ""
    return f"{host}{port}"


def detect_endpoint() -> Optional[str]:
    if settings.STORAGE_ENDPOINT_URL:
        return normalize_endpoint_for_s3(settings.STORAGE_ENDPOINT_URL)
    return None


def detect_region() -> str:
    return settings.STORAGE_REGION


def detect_url_style() -> str:
    return "path" if settings.STORAGE_FORCE_PATH_STYLE else "vhost"


def detect_ssl() -> bool:
    return settings.STORAGE_USE_SSL


def detect_creds():
    ak = settings.STORAGE_ACCESS_KEY or None
    sk = settings.STORAGE_SECRET_KEY or None
    st = settings.STORAGE_SESSION_TOKEN or None
    return ak, sk, st


def detect_bucket() -> Optional[str]:
    return settings.STORAGE_BUCKET or None


# =========================================================
# Memory / thread helpers
# =========================================================

def _parse_memory_limit_mb(value: str) -> Optional[int]:
    """Parse a DuckDB memory limit string (e.g. '8GB', '512MB') into megabytes.

    Returns None if the value cannot be parsed so callers can fall back safely.
    Supported suffixes (case-insensitive): GB, GiB, MB, MiB.
    """
    import re as _re
    m = _re.fullmatch(r"\s*([0-9]+(?:\.[0-9]*)?)\s*(GB|GiB|MB|MiB)\s*", value.strip(), _re.IGNORECASE)
    if not m:
        return None
    amount = float(m.group(1))
    suffix = m.group(2).upper()
    if suffix in ("GB", "GIB"):
        return int(amount * 1024)
    return int(amount)


def _derive_thread_count(
        memory_limit_str: str,
        fallback: int = 2,
        multiplier: Optional[float] = None,
) -> int:
    """Derive a safe DuckDB thread count for remote parquet reads.

    DuckDB uses synchronous IO: each thread can make at most one HTTP/S3
    request at a time.  For remote file workloads the docs recommend
    2–5× CPU cores to saturate network bandwidth.  We use 3× as a
    practical middle ground (override with SUPERTABLE_DUCKDB_IO_MULTIPLIER).

    Formula:
      io_threads   = cpu_count * SUPERTABLE_DUCKDB_IO_MULTIPLIER  (default 3)
      memory_floor = max(1, memory_mb // 400)   ← ~400 MB per thread minimum
      result       = min(io_threads, memory_floor)

    The memory floor prevents OOM when a small memory limit is set on a
    large-CPU host.  400 MB is DuckDB's practical minimum working set per
    thread for a parquet scan with aggregation.

    Override everything with SUPERTABLE_DUCKDB_THREADS (checked by caller).
    Falls back to `fallback` (default 2) if memory string cannot be parsed.
    """
    import os as _os
    mb = _parse_memory_limit_mb(memory_limit_str)
    if mb is None:
        logger.warning(
            f"[duckdb.threads] could not parse memory limit '{memory_limit_str}'; "
            f"defaulting to {fallback} thread(s)"
        )
        return fallback

    cpu_count = _os.cpu_count() or fallback
    if multiplier is None:
        multiplier = settings.SUPERTABLE_DUCKDB_IO_MULTIPLIER

    io_threads = int(cpu_count * multiplier)
    # Safety ceiling: never allocate fewer than ~400 MB per thread.
    memory_floor = max(1, mb // 400)
    result = min(io_threads, memory_floor)

    logger.debug(
        f"[duckdb.threads] memory={memory_limit_str} ({mb}MB), "
        f"cpu={cpu_count}×{multiplier}={io_threads} io_threads, "
        f"memory_floor={memory_floor}, using={result}"
    )
    return result


# =========================================================
# httpfs / S3 configuration
# =========================================================

def _selected_s3_configuration(storage: Optional[object]) -> Dict[str, object]:
    """Resolve S3 auth from the selected server-side storage context.

    A DuckDB executor can be constructed with an injected storage backend whose
    credentials deliberately differ from process-global settings.  Falling
    back to those globals would silently widen the query's principal.  Built-in
    S3/MinIO instances expose the exact credentials they were constructed with;
    custom adapters may opt in through ``duckdb_s3_config()``.
    """
    if storage is None:
        access_key, secret_key, session_token = detect_creds()
        return {
            "endpoint": detect_endpoint(),
            "access_key": access_key,
            "secret_key": secret_key,
            "session_token": session_token,
            "region": detect_region(),
            "url_style": detect_url_style(),
            "use_ssl": detect_ssl(),
            # An explicitly selected process-global configuration with no
            # credential values is the existing anonymous/endpoint-only mode.
            # Partial credentials still fail the validation below.
            "anonymous": not any((access_key, secret_key, session_token)),
        }

    module = storage.__class__.__module__
    qualname = storage.__class__.__qualname__
    if module == "supertable.storage.s3_storage" and qualname == "S3Storage":
        config = {
            "endpoint": getattr(storage, "endpoint_url", None),
            "access_key": getattr(storage, "_aws_access_key_id", None),
            "secret_key": getattr(storage, "_aws_secret_access_key", None),
            "session_token": getattr(storage, "_aws_session_token", None),
            "region": getattr(storage, "region", None),
            "url_style": getattr(storage, "url_style", "vhost"),
            "use_ssl": getattr(storage, "secure", True),
            "anonymous": False,
        }
    elif (
        module == "supertable.storage.minio_storage"
        and qualname == "MinioStorage"
    ):
        config = {
            "endpoint": (
                getattr(storage, "_endpoint", None)
                or getattr(storage, "endpoint_url", None)
            ),
            "access_key": getattr(storage, "_access_key", None),
            "secret_key": getattr(storage, "_secret_key", None),
            "session_token": None,
            "region": getattr(storage, "region", None),
            "url_style": getattr(storage, "url_style", "path"),
            "use_ssl": getattr(storage, "secure", False),
            "anonymous": False,
        }
    else:
        provider = getattr(storage, "duckdb_s3_config", None)
        if not callable(provider):
            raise RuntimeError(
                "Selected storage does not expose a DuckDB S3 authorization context"
            )
        supplied = provider()
        if not isinstance(supplied, dict):
            raise RuntimeError(
                "Selected storage returned an invalid DuckDB S3 authorization context"
            )
        config = {
            "endpoint": supplied.get("endpoint"),
            "access_key": supplied.get("access_key"),
            "secret_key": supplied.get("secret_key"),
            "session_token": supplied.get("session_token"),
            "region": supplied.get("region"),
            "url_style": supplied.get("url_style", "vhost"),
            "use_ssl": supplied.get("use_ssl", True),
            "anonymous": supplied.get("anonymous") is True,
        }

    access_key = config["access_key"]
    secret_key = config["secret_key"]
    anonymous = config["anonymous"] is True
    if anonymous and (access_key or secret_key or config["session_token"]):
        raise RuntimeError("Anonymous S3 authorization cannot include credentials")
    if not anonymous and (not access_key or not secret_key):
        # SDK clients may hide refreshable IAM credentials.  DuckDB cannot be
        # safely bound to that opaque principal, so direct s3:// scans fail
        # closed. Operators can select the existing presigned-path mode.
        raise RuntimeError(
            "Selected storage credentials are unavailable for DuckDB S3 access"
        )
    if config["session_token"] and not (access_key and secret_key):
        raise RuntimeError("Incomplete DuckDB S3 authorization context")

    endpoint = config["endpoint"]
    config["endpoint"] = (
        normalize_endpoint_for_s3(str(endpoint)) if endpoint else None
    )
    config["region"] = str(config["region"] or "")
    url_style = str(config["url_style"] or "").casefold()
    if url_style not in {"path", "vhost"}:
        raise RuntimeError("Invalid DuckDB S3 URL style")
    config["url_style"] = url_style
    if not isinstance(config["use_ssl"], bool):
        raise RuntimeError("Invalid DuckDB S3 TLS configuration")
    return config


def configure_httpfs_and_s3(
        con: duckdb.DuckDBPyConnection,
        for_paths: List[str],
        *,
        storage: Optional[object] = None,
) -> None:
    """Load httpfs and configure an in-memory S3 secret plus HTTP caches.

    Both cache settings (HTTP metadata cache and external file cache) are
    registered by the httpfs extension, not DuckDB core.  They must be SET
    after LOAD httpfs — configuring them before causes a silent no-op.
    This is the single correct place to apply both.

    Load guard: tries LOAD first; only falls back to INSTALL+LOAD when the
    extension is not yet present.  Avoids a network round-trip to the
    extension repository on every call.
    """
    if not for_paths:
        return

    # Core DuckDB can read local parquet files without httpfs.  Loading the
    # extension for a local-only query is both wasted work and, on an offline
    # installation where the extension was not seeded, turns a valid local
    # read into an avoidable failure.
    any_s3 = any(str(p).lower().startswith(("s3://", "s3a://")) for p in for_paths)
    any_http = any(str(p).lower().startswith(("http://", "https://")) for p in for_paths)
    if not (any_s3 or any_http):
        return

    # Load httpfs.  It is baked into the image and seeded into the DuckDB
    # extension dir (see the container entrypoint), so LOAD normally succeeds
    # with no network access.
    #
    # Why this is NOT a blind ``INSTALL`` fallback: ``INSTALL httpfs`` performs
    # an HTTP GET to extensions.duckdb.org.  On an offline / firewalled node
    # that socket can stall for minutes — or hang indefinitely on a blackholed
    # route — turning a should-be-instant failure into an unbounded query hang.
    # So we fail fast instead:
    #   * SET autoinstall_known_extensions=false makes LOAD raise immediately
    #     when the extension is absent, rather than silently downloading it;
    #   * a network INSTALL is attempted ONLY when explicitly opted in via
    #     SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD;
    #   * otherwise we raise a clear, actionable error the caller returns.
    try:
        con.execute("SET autoinstall_known_extensions=false;")
    except Exception:
        pass

    try:
        con.execute("LOAD httpfs;")
    except Exception as load_err:
        if settings.SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD:
            # Operator explicitly allowed reaching the network for a one-off
            # install (e.g. an online dev box without a baked extension).
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
        else:
            raise RuntimeError(
                "DuckDB 'httpfs' extension is not available locally and network "
                "auto-download is disabled, so this query cannot run. Bake/seed "
                "httpfs into "
                f"'{get_app_home()}/.duckdb/extensions/v<duckdb_version>/<platform>/' "
                "(the container entrypoint restores it from /opt/duckdb-extensions), "
                "or set SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=true to permit a "
                f"one-time online install. Underlying DuckDB error: {load_err}"
            ) from load_err

    try:
        supported = {
            name for (name,) in con.execute(
                "SELECT name FROM duckdb_settings()"
            ).fetchall()
        }
    except Exception:
        supported = {
            "http_timeout", "enable_http_metadata_cache",
            "enable_external_file_cache", "external_file_cache_max_size",
            "external_file_cache_directory",
        }

    def set_if_supported(param: str, value_sql: str):
        if param in supported:
            con.execute(f"SET {param}={sanitize_sql_string(value_sql)};")

    if any_s3:
        # Never install credentials as SET variables. DuckDB exposes those
        # verbatim through current_setting(), which user SELECT expressions can
        # call. A TEMPORARY SECRET stays in memory, resolves S3 scans normally,
        # and redacts SECRET/SESSION_TOKEN in DuckDB's own secret catalog.
        #
        # Reset legacy variables as a defence for long-lived processes upgraded
        # in place or tests that reuse a previously configured connection.
        try:
            selected = _selected_s3_configuration(storage)
            endpoint = selected["endpoint"]
            access_key = selected["access_key"]
            secret_key = selected["secret_key"]
            session_token = selected["session_token"]
            region = selected["region"]
            url_style = selected["url_style"]
            use_ssl = selected["use_ssl"]
            for legacy_setting in (
                "s3_access_key_id", "s3_secret_access_key", "s3_session_token",
            ):
                con.execute(f"RESET {legacy_setting};")

            secret_options = ["TYPE S3", "PROVIDER CONFIG"]

            def add_secret_option(name: str, value: object) -> None:
                if value is None:
                    return
                if not isinstance(value, str):
                    raise RuntimeError(
                        f"Invalid DuckDB S3 {name.casefold()} configuration"
                    )
                if value:
                    quoted_value = sanitize_sql_string("'" + value + "'")
                    secret_options.append(f"{name} {quoted_value}")

            add_secret_option("KEY_ID", access_key)
            add_secret_option("SECRET", secret_key)
            add_secret_option("SESSION_TOKEN", session_token)
            add_secret_option("REGION", region)
            add_secret_option("ENDPOINT", endpoint)
            add_secret_option("URL_STYLE", url_style)
            secret_options.append("USE_SSL TRUE" if use_ssl else "USE_SSL FALSE")
            con.execute(
                "CREATE OR REPLACE TEMPORARY SECRET supertable_s3 ("
                + ", ".join(secret_options)
                + ");"
            )

            # Treat the readable setting interface as a postcondition, not an
            # assumption about DuckDB defaults or version behaviour.
            for legacy_setting in (
                "s3_access_key_id", "s3_secret_access_key", "s3_session_token",
            ):
                row = con.execute(
                    "SELECT current_setting(?)", [legacy_setting]
                ).fetchone()
                if row and row[0] not in (None, ""):
                    raise RuntimeError("readable credential setting remained active")
        except Exception:
            # Do not chain a backend exception: DuckDB parser/binder messages can
            # echo the CREATE SECRET statement, including its literal values.
            raise RuntimeError(
                "DuckDB could not configure protected in-memory S3 access"
            ) from None

    http_timeout_env = settings.SUPERTABLE_DUCKDB_HTTP_TIMEOUT
    if http_timeout_env:
        try:
            # DuckDB's http_timeout is in SECONDS (UBIGINT, default 30).
            # SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60 → SET http_timeout=60 → 60 s. No conversion needed.
            # Ref: duckdb_settings() description: "HTTP timeout read/write/connection/retry (in seconds)"
            con.execute(f"SET http_timeout={int(http_timeout_env)};")
        except Exception:
            pass

    # HTTP metadata cache — caches parquet footer (schema + row-group stats)
    # across queries on the same persistent connection.
    meta_cache_on = settings.SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE
    set_if_supported(
        "enable_http_metadata_cache",
        "true" if meta_cache_on else "false",
    )

    # External file cache — an in-memory cache of external (e.g. remote
    # Parquet) data blocks so repeated queries do not re-download the same row
    # groups.  Enabled whenever a cache size is configured.  The cache is
    # bounded by the global memory_limit (DuckDB enforces that bound), which is
    # the effective cap; a dedicated per-cache cap is applied only on builds
    # that expose external_file_cache_max_size (no released DuckDB through
    # 1.5.x does), so in practice memory_limit is the bound.
    cache_size = settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE
    can_cap = "external_file_cache_max_size" in supported
    if cache_size:
        set_if_supported("enable_external_file_cache", "true")
        if can_cap:
            set_if_supported("external_file_cache_max_size", f"'{cache_size}'")
            cache_dir_raw = settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
            if not cache_dir_raw:
                # Derive from SUPERTABLE_HOME — single env var controls all paths.
                cache_dir_raw = os.path.join(get_app_home(), "duckdb_cache")
            # Expand ~ so DuckDB receives an absolute path.
            cache_dir = os.path.expanduser(cache_dir_raw)
            os.makedirs(cache_dir, exist_ok=True)
            set_if_supported("external_file_cache_directory", f"'{cache_dir}'")
        logger.debug(
            "[duckdb.cache] external file cache enabled"
            + (f", capped at {cache_size}" if can_cap
               else f", bounded by memory_limit (size={cache_size}; "
                    "this DuckDB build has no dedicated cap)")
        )
    else:
        set_if_supported("enable_external_file_cache", "false")


# =========================================================
# Presign helpers
# =========================================================

def url_to_key(url: str, bucket: Optional[str]) -> Optional[str]:
    """Extract the object key from an S3/HTTP URL."""
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if parsed.scheme == "s3":
        return parsed.path.lstrip("/")

    if parsed.scheme in ("http", "https"):
        host = (parsed.netloc or "").lower()
        path = parsed.path.lstrip("/")

        if not bucket:
            return path

        bucket_lower = bucket.lower()
        if host.startswith(f"{bucket_lower}."):
            return path
        if path.startswith(f"{bucket_lower}/"):
            return path[len(bucket_lower) + 1:]

        return path
    return None


def make_presigned_list(storage, paths: List[str]) -> List[str]:
    """Attempt to presign each path; fall back to original on failure."""
    presign_fn = getattr(storage, "presign", None) if storage is not None else None
    if not callable(presign_fn):
        return paths

    bucket = detect_bucket()
    out: List[str] = []

    for p in paths:
        key = url_to_key(p, bucket)
        if key:
            try:
                out.append(presign_fn(key))
            except Exception as e:
                out.append(p)
                logger.warning(
                    f"[presign] failed for '{redact_url_credentials(p)}': "
                    f"{redact_url_credentials(e)}"
                )
        else:
            out.append(p)

    return out


# =========================================================
# Reflection table creation
# =========================================================

def _reflection_select_cols(columns: Optional[List[str]]) -> str:
    """Build the SELECT projection for a reflection table/view.

    With no explicit ``columns`` this is a bare ``*``. With an explicit
    projection the public columns are listed by name while the system
    columns (``__rowid__`` / ``__timestamp__``) are pulled via a tolerant
    ``COLUMNS(c -> c IN (...))`` tail rather than by name — so pre-migration
    parquet that predates ``__rowid__`` does not raise "Referenced column
    not found"; the absent system column is simply omitted from the scan.
    """
    if not columns:
        return "*"
    system = {ROWID_COL, TIMESTAMP_COL}
    internal = {SOURCE_FILE_COL, SCAN_FILENAME_COL}
    public = [
        c for c in columns
        if c and c.strip() and c not in system and c not in internal
    ]
    wants_system = any(c in system for c in columns)
    parts = [quote_if_needed(c) for c in public]
    if wants_system:
        parts.append(f"COLUMNS(c -> c IN ('{ROWID_COL}', '{TIMESTAMP_COL}'))")
    return ", ".join(parts) if parts else "*"


def _reflection_source_identity_sql(
        files: List[str], resource_keys: Optional[List[str]], select_cols: str,
) -> tuple[str, str]:
    """Return (projection, parquet filename option) for canonical file identity."""
    if not resource_keys:
        return select_cols, ""
    if len(files) != len(resource_keys) or any(not str(k) for k in resource_keys):
        raise RuntimeError(
            "Resolved reflection files and stable resource keys must correspond one-for-one"
        )
    path_to_key: Dict[str, str] = {}
    seen_keys = set()
    for path, raw_key in zip(files, resource_keys):
        path = str(path)
        raw_key = str(raw_key)
        prior = path_to_key.setdefault(path, raw_key)
        if prior != raw_key:
            raise RuntimeError(
                "Resolved reflection path maps to multiple stable resource keys; "
                "composite tombstone identity is ambiguous"
            )
        if raw_key in seen_keys:
            raise RuntimeError(
                "Snapshot contains a duplicate stable resource key; composite "
                "tombstone identity is ambiguous"
            )
        seen_keys.add(raw_key)
    scan_col = quote_if_needed(SCAN_FILENAME_COL)
    source_col = quote_if_needed(SOURCE_FILE_COL)
    if select_cols == "*":
        select_cols = (
            f"COLUMNS(c -> c NOT IN ('{SCAN_FILENAME_COL}', '{SOURCE_FILE_COL}'))"
        )
    cases = " ".join(
        f"WHEN ({scan_col} COLLATE \"binary\") = "
        f"('{escape_parquet_path(str(path))}' COLLATE \"binary\") "
        f"THEN '{escape_parquet_path(str(raw_key))}'"
        for path, raw_key in zip(files, resource_keys)
    )
    # Any filename DuckDB reports differently from the exact scan input makes
    # source identity unprovable.  Abort instead of silently retaining/deleting
    # the wrong row under a composite tombstone key.
    mapping = (
        f"CASE {cases} "
        "ELSE error('unrecognized reflection source filename') END "
        f"AS {source_col}"
    )
    return f"{select_cols}, {mapping}", f", filename='{SCAN_FILENAME_COL}'"


def create_reflection_table(
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        resource_keys: Optional[List[str]] = None,
) -> None:
    """CREATE TABLE ... AS SELECT ... FROM parquet_scan(...)."""
    if not files:
        raise ValueError(f"No files provided for reflection table '{table_name}'")

    parquet_files_str = ", ".join(f"'{escape_parquet_path(f)}'" for f in files)
    select_cols = _reflection_select_cols(columns)
    select_cols, filename_option = _reflection_source_identity_sql(
        files, resource_keys, select_cols,
    )

    sql = (
        f"CREATE TABLE {table_name} AS "
        f"SELECT {select_cols} "
        f"FROM parquet_scan([{parquet_files_str}], "
        f"union_by_name=TRUE, HIVE_PARTITIONING=FALSE{filename_option});"
    )
    con.execute(sql)


def create_reflection_table_with_presign_retry(
        con: duckdb.DuckDBPyConnection,
        storage,
        table_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        log_prefix: str = "",
        resource_keys: Optional[List[str]] = None,
) -> bool:
    """
    Create a reflection table with automatic presign fallback on HTTP errors.
    Returns True if presign retry was used.
    """
    configure_httpfs_and_s3(con, files, storage=storage)
    tried_presign = False

    try:
        create_reflection_table(con, table_name, files, columns, resource_keys)
    except Exception as e:
        msg = str(e)
        if any(tok in msg for tok in (
                "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                "AccessDenied", "SignatureDoesNotMatch", "403", "400",
        )):
            logger.warning(
                f"{log_prefix}[duckdb.retry] presign fallback for {table_name}: "
                f"{redact_url_credentials(msg)}"
            )
            tried_presign = True
            presigned_files = make_presigned_list(storage, files)
            configure_httpfs_and_s3(con, presigned_files, storage=storage)
            create_reflection_table(
                con, table_name, presigned_files, columns, resource_keys,
            )
        else:
            raise

    return tried_presign


# =========================================================
# Reflection VIEW creation (lazy — no upfront data read)
# =========================================================

def create_reflection_view(
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        resource_keys: Optional[List[str]] = None,
) -> None:
    """CREATE OR REPLACE VIEW ... AS SELECT ... FROM parquet_scan(...).

    Unlike ``create_reflection_table``, this does **not** materialise any
    data at creation time.  DuckDB reads only parquet footer metadata
    (schema + row-group statistics) and defers all I/O to query execution,
    where it can apply filter and projection pushdown.

    Use this in the transient executor to prevent OOM on large datasets.
    The pinned executor continues to use TABLEs for its cross-query cache.
    """
    if not files:
        raise ValueError(f"No files provided for reflection view '{view_name}'")

    parquet_files_str = ", ".join(f"'{escape_parquet_path(f)}'" for f in files)
    select_cols = _reflection_select_cols(columns)
    select_cols, filename_option = _reflection_source_identity_sql(
        files, resource_keys, select_cols,
    )

    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {select_cols} "
        f"FROM parquet_scan([{parquet_files_str}], "
        f"union_by_name=TRUE, HIVE_PARTITIONING=FALSE{filename_option});"
    )
    con.execute(sql)


def create_reflection_view_with_presign_retry(
        con: duckdb.DuckDBPyConnection,
        storage,
        view_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        log_prefix: str = "",
        resource_keys: Optional[List[str]] = None,
) -> bool:
    """
    Create a lazy reflection VIEW with automatic presign fallback on HTTP errors.

    Mirrors ``create_reflection_table_with_presign_retry`` but uses a VIEW so
    no data is read at creation time.  Controlled by the env var
    ``SUPERTABLE_DUCKDB_MATERIALIZE`` (default: ``view``; set to ``table`` to
    revert to the old eager-materialisation behaviour).

    Returns True if the presign fallback was used.
    """
    materialize = settings.SUPERTABLE_DUCKDB_MATERIALIZE
    if materialize == "table":
        return create_reflection_table_with_presign_retry(
            con, storage, view_name, files, columns, log_prefix, resource_keys
        )

    configure_httpfs_and_s3(con, files, storage=storage)
    tried_presign = False

    try:
        create_reflection_view(con, view_name, files, columns, resource_keys)
    except Exception as e:
        msg = str(e)
        if any(tok in msg for tok in (
                "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                "AccessDenied", "SignatureDoesNotMatch", "403", "400",
        )):
            logger.warning(
                f"{log_prefix}[duckdb.retry] presign fallback (view) for {view_name}: "
                f"{redact_url_credentials(msg)}"
            )
            tried_presign = True
            presigned_files = make_presigned_list(storage, files)
            configure_httpfs_and_s3(con, presigned_files, storage=storage)
            create_reflection_view(
                con, view_name, presigned_files, columns, resource_keys,
            )
        else:
            raise

    return tried_presign


# =========================================================
# Query rewriting
# =========================================================


class _ProtectedDuckDBGenerator(SQLGlotDuckDB.Generator):
    """Emit DuckDB's SELECT-level USING SAMPLE in its actual grammar slot.

    sqlglot 26.x parses DuckDB ``USING SAMPLE`` onto ``Select.sample`` but its
    generic generator emits that modifier after ORDER/LIMIT, which DuckDB
    rejects.  DuckDB accepts the sampling modifier after WHERE/GROUP/HAVING
    and before ORDER/OFFSET/LIMIT.  Keep the upstream modifier order intact
    except for that one dialect-specific placement.
    """

    def query_modifiers(self, expression: exp.Expression, *sqls: str) -> str:
        limit = expression.args.get("limit")
        if self.LIMIT_FETCH == "LIMIT" and isinstance(limit, exp.Fetch):
            limit = exp.Limit(
                expression=exp.maybe_copy(limit.args.get("count")),
            )
        elif self.LIMIT_FETCH == "FETCH" and isinstance(limit, exp.Limit):
            limit = exp.Fetch(
                direction="FIRST",
                count=exp.maybe_copy(limit.expression),
            )

        locks = self.expressions(expression, key="locks", sep=" ")
        locks = f" {locks}" if locks else ""
        return sqlglot_csv(
            *sqls,
            *[self.sql(join) for join in expression.args.get("joins") or []],
            self.sql(expression, "match"),
            *[
                self.sql(lateral)
                for lateral in expression.args.get("laterals") or []
            ],
            self.sql(expression, "prewhere"),
            self.sql(expression, "where"),
            self.sql(expression, "connect"),
            self.sql(expression, "group"),
            self.sql(expression, "having"),
            *[
                transform(self, expression)
                for transform in self.AFTER_HAVING_MODIFIER_TRANSFORMS.values()
            ],
            self.sql(expression, "sample"),
            self.sql(expression, "order"),
            *self.offset_limit_modifiers(
                expression, isinstance(limit, exp.Fetch), limit,
            ),
            locks,
            self.options_modifier(expression),
            self.for_modifiers(expression),
            sep="",
        )


def _protected_duckdb_sql(expression: exp.Expression) -> str:
    """Generate protected SQL, correcting sqlglot's DuckDB sample placement."""
    has_select_sample = any(
        isinstance(node, exp.Select) and node.args.get("sample") is not None
        for node in expression.walk()
    )
    if not has_select_sample:
        return expression.sql(dialect="duckdb")
    return _ProtectedDuckDBGenerator(
        dialect=SQLGlotDuckDB(),
    ).generate(expression, copy=False)

def rewrite_query_with_hashed_tables(
        original_sql: str,
        alias_to_table: Dict[str, str],
        *,
        parsed_expression: Optional[exp.Expression] = None,
        default_super_name: Optional[str] = None,
) -> str:
    """Replace table references in SQL with hashed physical table names.

    Callers that already hold the exact parsed expression may pass it to avoid
    reparsing the same immutable SQL.  The expression is always copied before
    rewriting, so the parser's canonical AST remains immutable and reusable by
    capability analysis, routing, and other engines.
    """
    if not alias_to_table:
        return original_sql

    if isinstance(parsed_expression, exp.Expression):
        parsed = parsed_expression.copy()
    else:
        try:
            parsed = sqlglot.parse_one(original_sql)
        except Exception as e:
            raise RuntimeError(
                "Unable to rewrite protected query table references"
            ) from e

    folded_targets: Dict[str, tuple[str, str]] = {}
    for alias, physical in alias_to_table.items():
        folded = str(alias).casefold()
        if folded in folded_targets:
            raise RuntimeError("Ambiguous protected query table aliases")
        folded_targets[folded] = (str(alias), physical)
    try:
        scopes = tuple(traverse_scope(parsed))
        layout = _build_scoped_table_bindings(
            parsed,
            default_super_name or "__supertable_default__",
            scopes=scopes,
        )
    except Exception as exc:
        raise RuntimeError(
            "Unable to prove protected query table bindings"
        ) from exc
    rewritten: set[str] = set()

    # Replacing ``schema.table`` with a request-private unqualified view also
    # changes how a valid three-part column reference must be spelled. Bind
    # such columns through the same lexical scope graph before mutating table
    # nodes. A global table-name lookup is unsafe when a correlated subquery
    # reuses the same table name from another schema.
    scopes_by_select = {
        id(scope.expression): scope
        for scope in scopes
        if isinstance(scope.expression, exp.Select)
    }
    sources_by_scope: Dict[
        int, tuple[Dict[str, Optional[exp.Table]], list[exp.Table]]
    ] = {}
    scope_by_table_id: Dict[int, object] = {}
    for scope in scopes:
        if not isinstance(scope.expression, exp.Select):
            continue
        source_aliases: Dict[str, Optional[exp.Table]] = {}
        direct_sources: list[exp.Table] = []
        try:
            selected_sources = scope.selected_sources.items()
        except Exception as exc:
            raise RuntimeError(
                "Unable to prove protected query column bindings"
            ) from exc
        for source_alias, selected in selected_sources:
            try:
                node, source = selected
            except (TypeError, ValueError):
                continue
            source_table = (
                node
                if isinstance(node, exp.Table) and isinstance(source, exp.Table)
                else None
            )
            source_aliases[str(source_alias).casefold()] = source_table
            if source_table is not None:
                direct_sources.append(node)
                scope_by_table_id[id(node)] = scope
        sources_by_scope[id(scope)] = (source_aliases, direct_sources)

    def identifier_name(node: object) -> str:
        if isinstance(node, exp.Identifier):
            return node.name
        if isinstance(node, exp.Expression):
            return node.name
        return ""

    def original_source_alias(source: exp.Table) -> str:
        alias_expr = source.args.get("alias")
        if isinstance(alias_expr, exp.TableAlias):
            alias_ident = alias_expr.this
            if isinstance(alias_ident, exp.Identifier) and alias_ident.name:
                return alias_ident.name
        return source.name

    original_alias_by_node_id = {
        source_id: original_source_alias(source)
        for _source_aliases, direct_sources in sources_by_scope.values()
        for source in direct_sources
        if (source_id := id(source)) in scope_by_table_id
    }

    # A request-wide catalog key is not automatically a SQL alias. Independent
    # scopes (notably UNION branches) may reuse the same spelling safely, and
    # changing it breaks DuckDB's bare-alias whole-row expression. Only a
    # descendant scope that can actually correlate to an ancestor needs unique
    # SQL aliases after schema qualifiers are stripped.
    sources_by_original_alias: Dict[str, list[exp.Table]] = {}
    recorded_source_ids: set[int] = set()
    for _source_aliases, direct_sources in sources_by_scope.values():
        for source_node in direct_sources:
            if id(source_node) in recorded_source_ids:
                continue
            recorded_source_ids.add(id(source_node))
            sources_by_original_alias.setdefault(
                original_source_alias(source_node).casefold(), []
            ).append(source_node)

    def can_correlate_to(descendant: object, ancestor: object) -> bool:
        current: Optional[object] = descendant
        while current is not None and current is not ancestor:
            if not bool(getattr(current, "can_be_correlated", False)):
                return False
            current = getattr(current, "parent", None)
        return current is ancestor

    correlation_alias_nodes: set[int] = set()
    for same_alias_sources in sources_by_original_alias.values():
        for index, left_source in enumerate(same_alias_sources):
            left_scope = scope_by_table_id.get(id(left_source))
            left_key = layout.by_node_id.get(id(left_source))
            for right_source in same_alias_sources[index + 1:]:
                right_scope = scope_by_table_id.get(id(right_source))
                right_key = layout.by_node_id.get(id(right_source))
                if (
                    left_scope is None
                    or right_scope is None
                    or left_key is None
                    or right_key is None
                    or left_key.casefold() == right_key.casefold()
                ):
                    continue
                if can_correlate_to(left_scope, right_scope) or can_correlate_to(
                    right_scope, left_scope
                ):
                    correlation_alias_nodes.update(
                        (id(left_source), id(right_source))
                    )

    def protected_alias(source: exp.Table) -> Optional[str]:
        binding_key = layout.by_node_id.get(id(source))
        target = (
            folded_targets.get(binding_key.casefold())
            if binding_key is not None else None
        )
        if target is None:
            return None
        if id(source) in correlation_alias_nodes:
            return target[0]
        return original_alias_by_node_id.get(id(source))

    for column in parsed.find_all(exp.Column):
        column_db = identifier_name(column.args.get("db"))
        column_catalog = identifier_name(column.args.get("catalog"))
        if column.is_star and (column_db or column_catalog):
            # DuckDB rejects schema/catalog-qualified stars even though
            # sqlglot accepts and regenerates them. Rewriting ``s.t.*`` to
            # ``alias.*`` would otherwise turn invalid input into a
            # data-bearing query.
            raise RuntimeError(
                "Schema-qualified stars are invalid in protected DuckDB queries"
            )
        nearest_select = column.find_ancestor(exp.Select)
        scope = (
            scopes_by_select.get(id(nearest_select))
            if nearest_select is not None else None
        )
        if not column.table and not column_db and not column_catalog and scope is not None:
            source_aliases, _direct_sources = sources_by_scope.get(
                id(scope), ({}, []),
            )
            whole_row_source = source_aliases.get(column.name.casefold())
            if (
                whole_row_source is not None
                and id(whole_row_source) in correlation_alias_nodes
                and protected_alias(whole_row_source)
                != original_source_alias(whole_row_source)
            ):
                raise RuntimeError(
                    "Protected query cannot safely rename a correlated table "
                    "alias used as an unqualified whole-row expression"
                )
        while scope is not None:
            source_aliases, direct_sources = sources_by_scope.get(
                id(scope), ({}, []),
            )

            # First prefer a real schema.table binding. DuckDB gives this
            # interpretation precedence over an identically spelled
            # alias.struct.field path in the same scope.
            if column_db and column.table:
                matches: list[exp.Table] = []
                for source in direct_sources:
                    # Once a table has an explicit alias DuckDB hides its
                    # original schema-qualified name. Rewriting that invalid
                    # spelling would incorrectly make a rejected query run.
                    if isinstance(source.args.get("alias"), exp.TableAlias):
                        continue
                    source_db = identifier_name(source.args.get("db"))
                    source_catalog = identifier_name(
                        source.args.get("catalog")
                    )
                    if (
                        source.name.casefold() == column.table.casefold()
                        and source_db
                        and source_db.casefold() == column_db.casefold()
                        and (
                            not column_catalog
                            or (
                                source_catalog
                                and source_catalog.casefold()
                                == column_catalog.casefold()
                            )
                        )
                    ):
                        matches.append(source)
                if matches:
                    if len(matches) != 1:
                        raise RuntimeError(
                            "Ambiguous schema-qualified column binding in "
                            "protected query"
                        )
                    replacement_alias = protected_alias(matches[0])
                    if replacement_alias is None:
                        raise RuntimeError(
                            "Protected query contains a qualified column whose "
                            "physical source was not replaced"
                        )
                    column.set("catalog", None)
                    column.set("db", None)
                    column.set("table", exp.to_identifier(replacement_alias))
                    break

                # A two-part source followed by a nested struct path uses all
                # four Column qualifier slots: ``schema.table.struct.field``
                # parses as catalog=schema, db=table, table=struct. Match the
                # physical relation prefix, then retain the struct/field tail.
                nested_matches: list[exp.Table] = []
                if column_catalog:
                    for source in direct_sources:
                        if isinstance(source.args.get("alias"), exp.TableAlias):
                            continue
                        source_db = identifier_name(source.args.get("db"))
                        source_catalog = identifier_name(
                            source.args.get("catalog")
                        )
                        if (
                            not source_catalog
                            and source.name.casefold() == column_db.casefold()
                            and source_db
                            and source_db.casefold() == column_catalog.casefold()
                        ):
                            nested_matches.append(source)
                if nested_matches:
                    if len(nested_matches) != 1:
                        raise RuntimeError(
                            "Ambiguous schema-qualified struct binding in "
                            "protected query"
                        )
                    replacement_alias = protected_alias(nested_matches[0])
                    if replacement_alias is None:
                        raise RuntimeError(
                            "Protected query contains a qualified struct whose "
                            "physical source was not replaced"
                        )
                    column.set("catalog", None)
                    column.set("db", exp.to_identifier(replacement_alias))
                    break

                # Otherwise ``alias.struct.field`` is represented by sqlglot
                # in the same three-part Column shape. Preserve the field path
                # and update only the physical relation alias if it was made
                # request-unique below.
                folded_db = column_db.casefold()
                if folded_db in source_aliases:
                    source = source_aliases[folded_db]
                    if source is not None:
                        replacement_alias = protected_alias(source)
                        if replacement_alias is None:
                            raise RuntimeError(
                                "Protected query contains a qualified column "
                                "whose physical source was not replaced"
                            )
                        column.set("db", exp.to_identifier(replacement_alias))
                    break

                # Four-or-more-part nested paths use the catalog slot for the
                # relation alias (``alias.struct.field.child``). Preserve the
                # nested tail while updating a request-unique alias.
                folded_catalog = column_catalog.casefold() if column_catalog else ""
                if folded_catalog and folded_catalog in source_aliases:
                    source = source_aliases[folded_catalog]
                    if source is not None:
                        replacement_alias = protected_alias(source)
                        if replacement_alias is None:
                            raise RuntimeError(
                                "Protected query contains a qualified column "
                                "whose physical source was not replaced"
                            )
                        column.set(
                            "catalog", exp.to_identifier(replacement_alias),
                        )
                    break

            # Ordinary ``alias.column`` binding, including correlated outer
            # references. A derived/CTE source is authoritative and stops the
            # search without being rewritten.
            elif column.table:
                folded_table = column.table.casefold()
                if folded_table in source_aliases:
                    source = source_aliases[folded_table]
                    if source is not None:
                        replacement_alias = protected_alias(source)
                        if replacement_alias is None:
                            raise RuntimeError(
                                "Protected query contains a qualified column "
                                "whose physical source was not replaced"
                            )
                        column.set(
                            "table", exp.to_identifier(replacement_alias),
                        )
                    break
            scope = getattr(scope, "parent", None)

    for table in parsed.find_all(exp.Table):
        if id(table) in layout.cte_reference_node_ids:
            # Query-local CTE reference; its physical leaf sources are handled
            # independently below/by their own Table nodes.
            continue
        alias_expr = table.args.get("alias")

        binding_key = layout.by_node_id.get(id(table))
        target = (
            folded_targets.get(binding_key.casefold())
            if binding_key is not None
            else None
        )
        if target is not None:
            canonical_alias, new_physical = target
            rewritten.add(canonical_alias.casefold())
            table.set("this", exp.to_identifier(new_physical))
            table.set("db", None)
            table.set("catalog", None)
            # Ensure the alias is always present. Independent scopes retain the
            # user's local alias; correlation-visible collisions use the
            # request-wide key selected above so stripped schema qualifiers do
            # not collapse distinct bindings.
            rewritten_alias = protected_alias(table)
            if rewritten_alias is None:
                raise RuntimeError(
                    "Protected query physical source was not replaced"
                )
            if isinstance(alias_expr, exp.TableAlias):
                alias_expr.set("this", exp.to_identifier(rewritten_alias))
            else:
                table.set(
                    "alias",
                    exp.TableAlias(this=exp.to_identifier(rewritten_alias)),
                )
        else:
            raise RuntimeError(
                "Protected query contains a physical table source that was "
                "not replaced by the catalog reflection"
            )

    if rewritten != set(folded_targets):
        raise RuntimeError(
            "Protected query reflection map did not match every physical source"
        )

    return _protected_duckdb_sql(parsed)


# =========================================================
# Connection initialization
# =========================================================

def _external_file_cache_cappable(con: duckdb.DuckDBPyConnection) -> bool:
    """True when this DuckDB build exposes a dedicated external-file-cache cap.

    No released DuckDB (through 1.5.x) exposes ``external_file_cache_max_size``;
    the cache is in-memory and bounded by ``memory_limit``, which DuckDB
    enforces.  This predicate gates only the *dedicated* per-cache size cap:
    when it returns False the cache still runs, bounded by ``memory_limit``
    rather than a separate cap.
    """
    try:
        return bool(con.execute(
            "SELECT 1 FROM duckdb_settings() "
            "WHERE name = 'external_file_cache_max_size'"
        ).fetchone())
    except Exception:
        return False


def init_connection(
        con: duckdb.DuckDBPyConnection,
        temp_dir: str,
        profile_path: Optional[str] = None,
        memory_limit: str = "1GB",
) -> None:
    """Apply standard PRAGMA settings to a DuckDB connection.

    Memory notes:
    - ``memory_limit`` defaults to 1 GB (overridable via
      ``SUPERTABLE_DUCKDB_MEMORY_LIMIT`` env var).  Keeping this well below
      the container's physical RAM is what enables DuckDB to spill to disk
      instead of raising an OOM error.
    - ``temp_directory`` is resolved to an absolute path; DuckDB silently
      ignores relative paths that it cannot resolve, which prevents spilling.
    - ``preserve_insertion_order=false`` reduces memory pressure during
      large Parquet scans at the cost of non-deterministic row order (ORDER
      BY in queries is unaffected).
    - Thread count defaults to 4 to limit concurrent buffer-pool competition
      inside a constrained container.  Override with ``SUPERTABLE_DUCKDB_THREADS``.
    """
    # Pin DuckDB's home directory to the (guaranteed-writable) app home before
    # anything else.  DuckDB otherwise derives it from the OS ``$HOME`` and uses
    # it for extension/secret lookups; under a restricted service user that path
    # may not exist, surfacing as "Can't find the home directory at '/home/app'".
    # The app home is created and made writable at import time, so this keeps all
    # home-dependent operations (LOAD/INSTALL httpfs, secrets) inside it.
    try:
        con.execute(f"SET home_directory='{get_app_home()}';")
    except Exception as e:
        logger.warning(f"[duckdb.init] home_directory pin failed: {e}")

    # Never let DuckDB auto-DOWNLOAD an extension.  Everything we need (httpfs)
    # is baked/seeded into the local extension dir; an implicit network install
    # would reach out to extensions.duckdb.org and can hang for minutes on an
    # offline/firewalled node — turning a should-be-instant error into an
    # unbounded query hang.  configure_httpfs_and_s3() owns the explicit,
    # opt-in install path (SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD).
    try:
        con.execute("SET autoinstall_known_extensions=false;")
    except Exception as e:
        logger.debug(f"[duckdb.init] disabling extension auto-install failed: {e}")

    # Resolve memory limit.
    # Single env var SUPERTABLE_DUCKDB_MEMORY_LIMIT controls both executors.
    # The `memory_limit` argument is the caller's fallback when the env var is absent.
    # normalize_memory_size guarantees a unit-suffixed value so DuckDB's parser
    # never rejects a bare number (e.g. a UI-supplied "2" -> "2GB").
    effective_memory_limit = normalize_memory_size(
        settings.SUPERTABLE_DUCKDB_MEMORY_LIMIT or memory_limit, default="1GB"
    )
    try:
        con.execute(f"PRAGMA memory_limit='{effective_memory_limit}';")
    except Exception as e:
        logger.warning(
            f"[duckdb.init] memory_limit='{effective_memory_limit}' rejected: {e}; "
            f"keeping DuckDB default"
        )

    # Absolute temp path is required for DuckDB to actually spill to disk.
    # Prefer a path rooted under the app home (~/supertable), which is
    # guaranteed to be created and writable at import time.  Absolute paths
    # from callers are used as-is; relative paths are re-rooted under the
    # app home so DuckDB can always resolve and write the spill directory.
    if os.path.isabs(temp_dir):
        abs_temp_dir = temp_dir
    else:
        abs_temp_dir = os.path.join(get_app_home(), "tmp", temp_dir)
    os.makedirs(abs_temp_dir, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{abs_temp_dir}';")

    if profile_path:
        con.execute("PRAGMA enable_profiling='json';")
        con.execute(f"PRAGMA profile_output='{profile_path}';")
    con.execute("PRAGMA default_collation='nocase';")

    # Reduce memory pressure during large parquet scans.
    # Row order is still deterministic for queries that include ORDER BY.
    try:
        con.execute("SET preserve_insertion_order=false;")
    except Exception:
        pass  # older DuckDB builds may not support this setting

    # External file cache baseline.  DuckDB (>=1.3.0) ships an in-memory cache
    # of external files, bounded by memory_limit (DuckDB enforces that bound).
    # Honour the configured default here: enable when a cache size is set,
    # otherwise off.  configure_httpfs_and_s3 / apply_runtime_pragmas refine
    # this (and apply a dedicated cap on builds that expose one).
    try:
        con.execute(
            "SET enable_external_file_cache="
            + ("true" if settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE else "false")
            + ";"
        )
    except Exception:
        pass

    # Thread count.
    # If SUPERTABLE_DUCKDB_THREADS is set explicitly, honour it exactly.
    # Otherwise derive from the effective memory limit using the IO-thread
    # formula: min(cpu * IO_MULTIPLIER, memory_mb // 400).
    # DuckDB uses synchronous IO — more threads = more parallel HTTP requests.
    explicit_threads = settings.SUPERTABLE_DUCKDB_THREADS
    if explicit_threads:
        try:
            thread_count = int(explicit_threads)
        except ValueError:
            logger.warning(
                f"[duckdb.threads] invalid SUPERTABLE_DUCKDB_THREADS='{explicit_threads}'; "
                f"falling back to auto-derive"
            )
            thread_count = _derive_thread_count(effective_memory_limit)
    else:
        thread_count = _derive_thread_count(effective_memory_limit)

    try:
        con.execute(f"SET threads={thread_count};")
        logger.debug(f"[duckdb.init] threads={thread_count}, memory={effective_memory_limit}")
    except Exception:
        pass


def new_duckdb_connection(
        temp_dir: str,
        for_paths: Optional[List[str]] = None,
        memory_limit: str = "1GB",
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection configured exactly like the read path.

    Single constructor for transient (write-side) connections so they apply the
    same ``init_connection`` pragmas as the persistent read executors — memory
    limit, thread count, ``temp_directory`` and, crucially, the pinned
    ``home_directory``.  This keeps the write-side probe from falling back to the
    OS home directory, which may be absent under a restricted service user.

    httpfs/S3 is configured only when *for_paths* contains a remote URL, matching
    the read path's lazy behaviour and avoiding a needless extension load for
    purely local scans.
    """
    con = duckdb.connect()
    try:
        init_connection(con, temp_dir=temp_dir, memory_limit=memory_limit)
        if for_paths and any("://" in str(p) for p in for_paths):
            configure_httpfs_and_s3(con, for_paths)
    except Exception:
        # Don't leak the half-initialised connection if a pragma / httpfs load
        # raises; re-raise so callers still fall back exactly as before.
        con.close()
        raise
    return con


# Thread-local pool for the write-side probe connection.  DuckDB connections are
# NOT thread-safe, so each thread keeps its own; reusing it amortises the
# ~150 ms init/warmup across writes on the same thread — the same reason the
# read executors hold a persistent connection.
_probe_pool = threading.local()


def get_pooled_duckdb_connection(
        temp_dir: str,
        for_paths: Optional[List[str]] = None,
        memory_limit: str = "1GB",
) -> duckdb.DuckDBPyConnection:
    """Return this thread's pooled probe connection, building it on first use.

    The cold build goes through ``new_duckdb_connection`` so the pinned
    ``home_directory`` / pragma contract is byte-for-byte identical to a
    transient connection.  On a *warm* connection httpfs/S3 is re-applied for
    remote paths so a connection first built for local paths can still serve a
    later remote probe and credentials always reflect the current environment
    (``configure_httpfs_and_s3`` re-reads env each call and is idempotent).
    """
    con = getattr(_probe_pool, "con", None)
    if con is None:
        con = new_duckdb_connection(
            temp_dir=temp_dir, for_paths=for_paths, memory_limit=memory_limit
        )
        _probe_pool.con = con
    elif for_paths and any("://" in str(p) for p in for_paths):
        configure_httpfs_and_s3(con, for_paths)
    return con


def reset_pooled_duckdb_connections() -> None:
    """Close and drop the calling thread's pooled probe connection.

    A no-op when the thread has none.  Used for test determinism and as an
    eviction hook; the pool slot is cleared before the close so a failing close
    still leaves the thread ready to rebuild.
    """
    con = getattr(_probe_pool, "con", None)
    if con is not None:
        _probe_pool.con = None
        try:
            con.close()
        except Exception:
            pass


def apply_runtime_pragmas(con: duckdb.DuckDBPyConnection, cfg) -> None:
    """Re-apply the session-settable DuckDB pragmas from a live engine config.

    DuckDB/IslandDB reuse a persistent connection, so settings applied once at
    ``init_connection`` time would otherwise freeze for the connection's life.
    Calling this immediately before each query makes the org's engine config
    (memory limit, thread count, HTTP timeout, external file cache) take effect
    live — the connection adopts the latest values without being torn down.

    ``cfg`` is an ``EngineRuntimeConfig``.  When None the connection keeps
    whatever ``init_connection`` applied (settings-based), so callers without a
    resolved config are unaffected.  Every pragma is best-effort: httpfs-only
    settings silently no-op on connections that have not loaded httpfs.
    """
    if cfg is None:
        return

    # normalize_memory_size guarantees a unit (UI sends a bare "2" -> "2GB");
    # a rejected value is logged rather than silently swallowed so a bad
    # config can never quietly leave the connection at the wrong limit.
    memory_limit = normalize_memory_size(cfg.duckdb_memory_limit, default="1GB")
    try:
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    except Exception as e:
        logger.warning(f"[duckdb.pragma] memory_limit='{memory_limit}' rejected: {e}")

    # Explicit thread count wins; otherwise derive from the live memory limit
    # and IO multiplier (same formula as init_connection).
    if cfg.duckdb_threads is not None:
        thread_count = cfg.duckdb_threads
    else:
        thread_count = _derive_thread_count(memory_limit, multiplier=cfg.duckdb_io_multiplier)
    try:
        con.execute(f"SET threads={thread_count};")
    except Exception:
        pass

    # httpfs settings — only effective once httpfs is loaded (remote reads).
    if cfg.duckdb_http_timeout is not None:
        try:
            con.execute(f"SET http_timeout={int(cfg.duckdb_http_timeout)};")
        except Exception:
            pass

    # External file cache: enable whenever the org configures a cache size.
    # The cache is in-memory and bounded by memory_limit (DuckDB enforces that
    # bound), which is the effective cap.  A dedicated per-cache cap is applied
    # only on builds that expose external_file_cache_max_size (no released
    # DuckDB through 1.5.x does); otherwise memory_limit is the bound.
    cache_size = normalize_memory_size(cfg.duckdb_external_cache_size, default="")
    if cache_size:
        try:
            con.execute("SET enable_external_file_cache=true;")
            if _external_file_cache_cappable(con):
                con.execute(
                    f"SET external_file_cache_max_size='{sanitize_sql_string(cache_size)}';"
                )
        except Exception as e:
            logger.warning(f"[duckdb.pragma] external file cache config failed: {e}")
    else:
        try:
            con.execute("SET enable_external_file_cache=false;")
        except Exception:
            pass


# =========================================================
# Engine self-diagnostics (UI "Diagnose" button)
# =========================================================

def _filesystem_type(path: str) -> str:
    """Best-effort filesystem type for ``path`` via /proc/mounts (Linux).

    Used to warn when the spill directory is RAM-backed (tmpfs/ramfs), where
    "spilling to disk" would actually consume memory instead of relieving it.
    Returns "" when the type cannot be determined.
    """
    try:
        target = os.path.abspath(path)
        best_mp = ""
        best_type = ""
        with open("/proc/mounts", "r") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) < 3:
                    continue
                mount_point, fstype = parts[1], parts[2]
                if (
                    target == mount_point
                    or target.startswith(mount_point.rstrip("/") + "/")
                    or mount_point == "/"
                ):
                    if len(mount_point) >= len(best_mp):
                        best_mp = mount_point
                        best_type = fstype
        return best_type
    except Exception:
        return ""


def run_engine_diagnostics(cfg=None, engine: str = "lite") -> Dict[str, Any]:
    """Deep runtime self-check for a DuckDB engine.

    Unlike a connection "test", this exercises the runtime to confirm the
    things that silently break in production:

      * the memory limit is actually applied,
      * the spill (``temp_directory``) exists, is writable, and is on real
        disk (not a RAM-backed tmpfs),
      * a query that exceeds memory genuinely spills to disk instead of OOMing,
      * the external file cache is in a memory-safe state.

    ``cfg`` is an ``EngineRuntimeConfig`` (or None to use init defaults); the
    connection is configured exactly like a live DuckDB/IslandDB query via
    ``init_connection`` + ``apply_runtime_pragmas``.  Returns a JSON-serialisable
    report and never raises.
    """
    import shutil
    import time
    import uuid

    checks: List[Dict[str, Any]] = []

    def add(cid, label, status, detail="", value=""):
        checks.append({
            "id": cid,
            "label": label,
            "status": status,
            "detail": str(detail),
            "value": "" if value is None else str(value),
        })

    # 1. Open + configure a connection the same way the engine does.
    con = None
    try:
        con = duckdb.connect()
        init_connection(con, temp_dir="diagnostics")
        if cfg is not None:
            apply_runtime_pragmas(con, cfg)
        add("connect", "Engine connection", "ok",
            "Opened and configured a DuckDB connection")
    except Exception as e:
        add("connect", "Engine connection", "fail", f"Could not initialise: {e}")
        return {"engine": engine, "duckdb_version": "", "overall": "fail", "checks": checks}

    # 2. DuckDB version + whether the file cache can be capped on this build.
    version = ""
    cappable = False
    try:
        version = con.execute("SELECT version()").fetchone()[0]
        cappable = _external_file_cache_cappable(con)
        add("version", "DuckDB version", "ok" if cappable else "warn",
            ("external_file_cache_max_size supported — the file cache can be capped"
             if cappable else
             "this build has no external_file_cache_max_size — the file cache "
             "runs bounded by memory_limit rather than a dedicated cap"),
            version)
    except Exception as e:
        add("version", "DuckDB version", "warn", f"version() failed: {e}")

    # 3. Memory limit effective?
    try:
        mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        low = str(mem).strip().lower()
        if not mem or low in ("0 bytes", "0", "-1") or "unlimited" in low:
            add("memory", "Memory limit", "warn",
                "No effective memory limit — a heavy query can consume all RAM", mem)
        else:
            add("memory", "Memory limit", "ok", "PRAGMA memory_limit is active", mem)
    except Exception as e:
        add("memory", "Memory limit", "fail", f"Could not read memory_limit: {e}")

    # 4. Thread count.
    try:
        th = con.execute("SELECT current_setting('threads')").fetchone()[0]
        add("threads", "Worker threads", "ok",
            "More threads add parallelism but also raise simultaneous memory use", th)
    except Exception as e:
        add("threads", "Worker threads", "warn", f"Could not read threads: {e}")

    # 5. Spill (temp) directory: set, exists, writable, on real disk?
    temp_dir = ""
    try:
        temp_dir = con.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()[0] or ""
    except Exception:
        temp_dir = ""

    if not temp_dir:
        add("temp_dir", "Spill directory", "fail",
            "temp_directory is empty — DuckDB cannot spill, so heavy queries OOM")
    else:
        mtds = ""
        try:
            mtds = con.execute(
                "SELECT current_setting('max_temp_directory_size')"
            ).fetchone()[0]
        except Exception:
            mtds = ""

        writable = False
        werr = ""
        try:
            os.makedirs(temp_dir, exist_ok=True)
            probe = os.path.join(temp_dir, f".st_spill_probe_{uuid.uuid4().hex}")
            with open(probe, "wb") as fh:
                fh.write(b"\0" * (1024 * 1024))  # 1 MiB
                fh.flush()
                os.fsync(fh.fileno())
            os.remove(probe)
            writable = True
        except Exception as e:
            werr = str(e)

        free_gb = None
        try:
            free_gb = shutil.disk_usage(temp_dir).free / (1024 ** 3)
        except Exception:
            free_gb = None
        fstype = _filesystem_type(temp_dir)
        ram_backed = fstype in ("tmpfs", "ramfs")

        parts = [f"path={temp_dir}"]
        if mtds:
            parts.append(f"cap={mtds}")
        if free_gb is not None:
            parts.append(f"free={free_gb:.1f} GB")
        if fstype:
            parts.append(f"fs={fstype}")
        summary = "; ".join(parts)

        if not writable:
            add("temp_dir", "Spill directory writable", "fail",
                f"Cannot write to the spill directory — queries OOM instead of "
                f"spilling. {werr} ({summary})", temp_dir)
        elif ram_backed:
            add("temp_dir", "Spill directory writable", "warn",
                f"Writable but RAM-backed ({fstype}) — spilling here consumes memory "
                f"instead of relieving it; mount a real disk volume. ({summary})",
                temp_dir)
        elif free_gb is not None and free_gb < 1.0:
            add("temp_dir", "Spill directory writable", "warn",
                f"Writable but low free space ({free_gb:.1f} GB) — large spills may "
                f"fail. ({summary})", temp_dir)
        else:
            add("temp_dir", "Spill directory writable", "ok",
                f"Wrote and removed a 1 MiB probe file. {summary}", temp_dir)

    # 6. Force a real disk spill under memory pressure (end-to-end proof).
    spill = None
    try:
        spill = duckdb.connect()
        init_connection(spill, temp_dir="diagnostics")
        spill.execute("PRAGMA memory_limit='256MB';")
        spill.execute("SET threads=4;")
        spill.execute("SET preserve_insertion_order=false;")
        t0 = time.perf_counter()
        # ~3M rows carrying a wide ~150-byte payload (~465 MB) sorted by a
        # scrambled key under a 256 MB cap: the working set cannot fit in
        # memory, so completion proves DuckDB spilled the payload to disk.
        # The cheap integer sort key keeps it fast (<1 s) while the 256 MB cap
        # sits far above the pinned-overhead floor, so a healthy disk never
        # false-fails.
        n = spill.execute(
            "SELECT count(*) FROM ("
            "SELECT hash(i) AS h, repeat('x', 140) || i::VARCHAR AS pad "
            "FROM range(3000000) t(i) ORDER BY h"
            ") q"
        ).fetchone()[0]
        ms = (time.perf_counter() - t0) * 1000.0
        add("spill", "Disk spill under pressure", "ok",
            f"Sorted {n:,} rows (~465 MB) under a 256 MB limit in {ms:.0f} ms — "
            "DuckDB spilled to disk instead of failing", f"{n:,} rows")
    except Exception as e:
        msg = str(e)
        if "out of memory" in msg.lower() or "failed to pin" in msg.lower():
            add("spill", "Disk spill under pressure", "fail",
                "A query that must spill ran out of memory instead — the spill "
                f"directory is not usable for spilling. {msg}")
        else:
            add("spill", "Disk spill under pressure", "warn",
                f"Spill probe did not complete: {msg}")
    finally:
        if spill is not None:
            try:
                spill.close()
            except Exception:
                pass

    # 7. External file cache memory safety.
    try:
        efc = con.execute(
            "SELECT current_setting('enable_external_file_cache')"
        ).fetchone()[0]
        efc_on = str(efc).strip().lower() in ("true", "1")
        cache_cfg = ""
        if cfg is not None:
            cache_cfg = normalize_memory_size(
                getattr(cfg, "duckdb_external_cache_size", ""), default=""
            )
        if efc_on and not cappable:
            add("cache", "External file cache", "ok",
                "Cache is ON, bounded by memory_limit — this build has no "
                "dedicated cap (external_file_cache_max_size), so memory_limit "
                "is the bound", "on · memory_limit")
        elif efc_on and cappable:
            add("cache", "External file cache", "ok",
                f"Cache is ON and capped at {cache_cfg or 'the configured size'}",
                "on · capped")
        else:
            add("cache", "External file cache", "ok",
                "Cache is OFF — remote files are re-fetched per query (set "
                "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE to enable, bounded by "
                "memory_limit)", "off")
    except Exception as e:
        add("cache", "External file cache", "warn", f"Could not read cache state: {e}")

    try:
        con.close()
    except Exception:
        pass

    rank = {"ok": 0, "warn": 1, "fail": 2}
    overall = "ok"
    for c in checks:
        if rank.get(c["status"], 0) > rank.get(overall, 0):
            overall = c["status"]

    return {
        "engine": engine,
        "duckdb_version": version,
        "overall": overall,
        "checks": checks,
    }


# =========================================================
# RBAC view creation
# =========================================================

def _validated_rbac_predicate_sql(raw_predicate: object) -> str:
    """Return one canonical, table-local DuckDB predicate or fail closed."""
    text = str(raw_predicate or "").strip()
    if not text:
        return ""
    try:
        statements = [
            statement
            for statement in sqlglot.parse(
                f"SELECT 1 WHERE ({text})", read="duckdb"
            )
            if statement is not None
        ]
    except Exception:
        raise ValueError("RBAC row predicate is invalid") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("RBAC row predicate must be one scalar expression")
    statement = statements[0]
    where = statement.args.get("where")
    predicate = getattr(where, "this", None)
    if not isinstance(predicate, exp.Expression):
        raise ValueError("RBAC row predicate must be one scalar expression")

    # Full validation is required here as well: DuckDB parses bare USER /
    # CURRENT_ROLE-style identity expressions as Columns rather than Funcs.
    validate_read_query_ast(statement, "duckdb")

    forbidden_types = tuple(
        expression_type
        for name in (
            "AggFunc", "Command", "From", "Join", "Query", "Subquery",
            "Table", "UDTF", "Window", "Parameter", "Placeholder",
            "SessionParameter", "Var",
        )
        if isinstance((expression_type := getattr(exp, name, None)), type)
    )
    for node in predicate.walk():
        if forbidden_types and isinstance(node, forbidden_types):
            raise ValueError(
                "RBAC row predicate may contain only scalar, table-local expressions"
            )
        if isinstance(node, exp.Dot):
            raise ValueError("Qualified RBAC row predicate expressions are not allowed")
        if isinstance(node, exp.Column) and (
            node.table or node.db or node.catalog
        ):
            raise ValueError("Qualified RBAC row predicate columns are not allowed")
        if isinstance(node, exp.Star):
            raise ValueError("RBAC row predicate wildcard is not allowed")
    return predicate.sql(dialect="duckdb")


def create_rbac_view(
        con: duckdb.DuckDBPyConnection,
        base_table_name: str,
        view_name: str,
        rbac_view_def,
) -> None:
    """
    Create a filtered view on top of a reflection table for RBAC enforcement.

    The view applies:
    - Column-level filtering: only allowed columns are visible
    - Row-level filtering: WHERE clause from role filters

    Args:
        con: DuckDB connection
        base_table_name: the underlying reflection table name
        view_name: the view name to create (query will reference this)
        rbac_view_def: RbacViewDef with allowed_columns and where_clause
    """
    # Validate before issuing DESCRIBE or DDL on the credential-bearing
    # connection. Policy text is another SQL channel and must meet the same
    # closed function boundary as user SELECTs.
    canonical_where = _validated_rbac_predicate_sql(
        getattr(rbac_view_def, "where_clause", "")
    )

    # Resolve the deny list against the actual post-tombstone relation on
    # every query.  This avoids dialect-specific ``EXCLUDE`` syntax, keeps an
    # absent excluded column harmless, and automatically hides it if schema
    # evolution introduces it later.  Case-colliding source names are
    # ambiguous under DuckDB's identifier rules and therefore fail closed.
    def _quote_policy_column(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    excluded = {
        str(c).casefold()
        for c in (getattr(rbac_view_def, "excluded_columns", None) or [])
    }
    described = None
    actual_columns = None

    def _actual_columns() -> List[str]:
        nonlocal described, actual_columns
        if actual_columns is None:
            described = _describe_relation(con, quote_if_needed(base_table_name))
            actual_columns = [str(row[0]) for row in described]
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
            select_cols = ", ".join(_quote_policy_column(c) for c in visible)
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
        select_cols = ", ".join(_quote_policy_column(c) for c in visible)

    # Row filter
    where_sql = ""
    if canonical_where:
        where_sql = f" WHERE {canonical_where}"

    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {select_cols} FROM {base_table_name}{where_sql};"
    )
    con.execute(sql)


def rbac_view_name(base_table_name: str) -> str:
    """Generate the RBAC view name for a given reflection table."""
    return f"rbac_{base_table_name}"


# =========================================================
# Tombstone (deletion-vector) view creation
# =========================================================

ROWID_COL = "__rowid__"
TIMESTAMP_COL = "__timestamp__"
TOMBSTONE_FILE_COL = "__file__"
SOURCE_FILE_COL = "__supertable_source_file__"
SCAN_FILENAME_COL = "__supertable_scan_filename__"


class ValidatedTombstoneTable(str):
    """Marker returned only after :class:`TombstoneCache` validates a DV.

    It deliberately remains a ``str`` so existing engine/view plumbing stays
    simple.  The marker lets ``create_tombstone_view`` avoid an O(N) validation
    scan on every cache hit. A plain direct-caller table remains valid for v1;
    v2 requires this marker because only the materializer can prove each sealed
    external segment without reading it a second time.
    """

    def __new__(
            cls, value: str, row_count: int = -1, digest: Optional[str] = None,
            referenced_files=None, *, root_digest: Optional[str] = None,
            cache_key: Optional[str] = None,
            segment_fingerprint: Optional[str] = None,
    ):
        obj = str.__new__(cls, value)
        obj.row_count = int(row_count)
        obj.digest = digest
        obj.referenced_files = frozenset(referenced_files or ())
        obj.root_digest = root_digest
        obj.cache_key = cache_key
        obj.segment_fingerprint = segment_fingerprint
        return obj


def _duckdb_parquet_path_is_glob(path: str) -> bool:
    """Return whether one resolved path can expand to multiple objects.

    Signed HTTP URLs legitimately use ``?`` (and may use brackets or stars)
    in their credential query, which DuckDB does not treat as the object path.
    Native provider URIs and local paths have no such query convention here,
    so inspect their full spelling instead of accidentally hiding a wildcard
    parsed as a URL query delimiter.
    """
    try:
        parsed = urlsplit(path)
    except ValueError as exc:
        raise RuntimeError("Invalid resolved deletion-vector path") from exc
    candidate = (
        parsed.path
        if parsed.scheme.casefold() in {"http", "https"}
        else path
    )
    return any(character in candidate for character in ("*", "?", "[", "]"))


def _v2_tombstone_segments(tombstone_def) -> Tuple[object, ...]:
    """Return a structurally closed v2 segment tuple or raise.

    Legacy definitions must not carry segments.  This rejects discriminator
    hybrids at the last SQL boundary even for direct engine callers that did
    not pass through :class:`DataReader`.
    """
    if tombstone_def is None:
        return ()
    tombstone_format = getattr(tombstone_def, "tombstone_format", None)
    tombstone_path = getattr(tombstone_def, "tombstone_path", None)
    cache_key = getattr(tombstone_def, "cache_key", None)
    segments = getattr(tombstone_def, "segments", ())
    if not isinstance(segments, tuple):
        raise RuntimeError("Invalid deletion-vector segment definition")
    if tombstone_path is not None and (
        not isinstance(tombstone_path, str) or not tombstone_path
    ):
        raise RuntimeError("Invalid resolved deletion-vector path")
    if (
        tombstone_format is not None
        and (
            isinstance(tombstone_format, bool)
            or not isinstance(tombstone_format, int)
            or tombstone_format not in (1, 2)
        )
    ):
        raise RuntimeError("Invalid deletion-vector format discriminator")
    if tombstone_format is not None:
        try:
            validate_snapshot_tombstone_state(
                cache_key if tombstone_path is not None else None,
                getattr(tombstone_def, "expected_rows", None),
                getattr(tombstone_def, "tombstone_digest", None),
                format_present=True,
                tombstone_format=tombstone_format,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Invalid deletion-vector snapshot state"
            ) from exc
    if tombstone_format != TOMBSTONE_FORMAT_V2:
        if segments:
            raise RuntimeError(
                "Deletion-vector segments require tombstone_format=2"
            )
        if str(cache_key or tombstone_path or "").endswith(".json"):
            raise RuntimeError(
                "A JSON deletion-vector pointer requires tombstone_format=2"
            )
        return ()
    if tombstone_path is None:
        if cache_key is not None or segments:
            raise RuntimeError("Invalid empty v2 deletion-vector definition")
        return ()
    if not segments:
        raise RuntimeError("Active v2 deletion vector has no sealed segments")
    if len(segments) > MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
        raise RuntimeError("Deletion vector contains too many sealed segments")

    manifest_key = cache_key
    try:
        validate_logical_storage_path(
            manifest_key,
            field_name="tombstone manifest cache key",
            required_suffix=".json",
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Invalid v2 deletion-vector manifest cache key") from exc

    keys: List[str] = []
    row_total = 0
    for segment in segments:
        key = getattr(segment, "cache_key", None)
        path = getattr(segment, "tombstone_path", None)
        rows = getattr(segment, "expected_rows", None)
        file_size = getattr(segment, "file_size", None)
        digest = getattr(segment, "tombstone_digest", None)
        provider_identity = getattr(segment, "provider_identity", None)
        if not isinstance(key, str) or not key or not isinstance(path, str) or not path:
            raise RuntimeError("Invalid deletion-vector segment path")
        if _duckdb_parquet_path_is_glob(path):
            raise RuntimeError(
                "Deletion-vector segment must resolve to one exact object path"
            )
        try:
            validate_logical_storage_path(
                key,
                field_name="tombstone segment cache key",
                required_suffix=".parquet",
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Invalid deletion-vector segment cache key") from exc
        if (
            not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0
            or rows > MAX_JSON_EXACT_INTEGER
            or not isinstance(file_size, int) or isinstance(file_size, bool)
            or file_size <= 0
            or file_size > MAX_JSON_EXACT_INTEGER
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise RuntimeError("Invalid deletion-vector segment seal")
        if provider_identity is not None and (
            not isinstance(provider_identity, str)
            or not provider_identity
            or "\x00" in provider_identity
            or len(provider_identity.encode("utf-8"))
            > MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES
        ):
            raise RuntimeError(
                "Invalid deletion-vector segment provider identity"
            )
        keys.append(key)
        row_total += rows
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise RuntimeError(
            "Deletion-vector segments are not uniquely and canonically ordered"
        )
    expected_rows = getattr(tombstone_def, "expected_rows", None)
    if (
        not isinstance(expected_rows, int)
        or isinstance(expected_rows, bool)
        or expected_rows <= 0
        or expected_rows > MAX_JSON_EXACT_INTEGER
        or row_total != expected_rows
    ):
        raise RuntimeError(
            "Deletion-vector segment rows do not match the pinned snapshot"
        )
    root_digest = getattr(tombstone_def, "tombstone_digest", None)
    if (
        not isinstance(root_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", root_digest) is None
    ):
        raise RuntimeError("Invalid v2 deletion-vector manifest root digest")
    return segments


def _v2_segment_fingerprint(segments: Tuple[object, ...]) -> str:
    """Seal the logical segment descriptors used to populate a cache entry.

    Resolved paths are deliberately excluded: presigned URLs may rotate while
    the manifest's stable key/row/size/digest descriptors remain identical.
    """
    digest = hashlib.sha256(b"supertable-dv-v2-segments\n")
    for segment in segments:
        fields = (
            str(segment.cache_key),
            str(int(segment.expected_rows)),
            str(int(segment.file_size)),
            str(segment.tombstone_digest),
            str(getattr(segment, "provider_identity", None) or ""),
        )
        for value in fields:
            encoded = value.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def tombstone_data_paths(tombstone_def) -> List[str]:
    """Return only Parquet paths an executor may consume for a DV."""
    segments = _v2_tombstone_segments(tombstone_def)
    if segments:
        return [str(segment.tombstone_path) for segment in segments]
    path = getattr(tombstone_def, "tombstone_path", None) if tombstone_def else None
    return [str(path)] if path else []


def _v2_union_relation(relations: List[str]) -> str:
    file_col = quote_if_needed(TOMBSTONE_FILE_COL)
    rowid_col = quote_if_needed(ROWID_COL)
    return "(" + " UNION ALL ".join(
        f"SELECT {file_col}, {rowid_col} FROM {relation}"
        for relation in relations
    ) + ")"


def _local_parquet_file_identity(path: str) -> Optional[tuple[int, ...]]:
    """Return a stable local-file identity, or ``None`` for provider paths.

    DuckDB opens provider-resolved S3/GCS/Azure/HTTP paths through its own
    filesystem implementations, so attempting to reinterpret those URLs as
    local paths would either inspect the wrong object or reject valid reads.
    Plain paths (the normal LocalStorage/cache representation) and local file
    URLs can be fenced with the host filesystem around the DuckDB statement.
    """
    parsed = urlsplit(path)
    if parsed.scheme:
        if parsed.scheme.casefold() != "file":
            return None
        if parsed.query or parsed.fragment or parsed.netloc not in ("", "localhost"):
            raise RuntimeError("Invalid local deletion-vector segment path")
        local_path = unquote(parsed.path)
    else:
        local_path = path

    try:
        observed = os.stat(local_path, follow_symlinks=True)
    except OSError as exc:
        raise RuntimeError(
            "Unable to inspect local deletion-vector segment"
        ) from exc
    if not stat.S_ISREG(observed.st_mode):
        raise RuntimeError("Local deletion-vector segment is not a regular file")
    return (
        int(observed.st_dev),
        int(observed.st_ino),
        int(observed.st_size),
        int(observed.st_mtime_ns),
        int(observed.st_ctime_ns),
    )


def _provider_parquet_file_identity(
        storage: object, cache_key: str,
) -> tuple[int, str]:
    """Observe one logical provider object without reading its row bytes."""
    stat_object = getattr(storage, "stat_object", None)
    if not callable(stat_object):
        raise RuntimeError(
            "Remote deletion-vector segments require provider identity support"
        )
    try:
        metadata = stat_object(cache_key)
        size = getattr(metadata, "size", None)
        identity_fn = getattr(metadata, "identity_token", None)
        identity = identity_fn() if callable(identity_fn) else None
    except Exception as exc:
        raise RuntimeError(
            "Unable to observe remote deletion-vector segment"
        ) from exc
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or not isinstance(identity, str)
        or not identity
        or "\x00" in identity
        or len(identity.encode("utf-8"))
        > MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES
    ):
        raise RuntimeError(
            "Remote deletion-vector segment has no stable provider identity"
        )
    return int(size), identity


def _materialize_v2_tombstone_table(
        con: duckdb.DuckDBPyConnection,
        tombstone_def,
        *,
        base_table_name: str,
        occupied_table_names: set[str],
        allowed_files: Optional[List[str]] = None,
        temporary: bool = False,
        storage: Optional[object] = None,
) -> tuple[str, int, str, frozenset[str]]:
    """Read each v2 segment exactly once into a private validated table.

    Each external Parquet object is first materialised into its own private
    staging table. Validation then runs against that immutable table, so the
    exact schema/count/logical digest seal belongs to the same bytes later
    unioned into the query-lifetime cache table. This deliberately avoids the
    unsafe validate-path-then-CTAS-path pattern: a mutable or inconsistent
    backend never gets a second external read in which to substitute rows or
    redistribute them between otherwise valid segments.
    """
    segments = _v2_tombstone_segments(tombstone_def)
    staging_tables: List[str] = []
    final_table: Optional[str] = None
    try:
        for index, segment in enumerate(segments):
            staging = (
                f"{base_table_name}_segment_{index}_{uuid.uuid4().hex}"
            )
            segment_path = str(segment.tombstone_path)
            escaped = escape_parquet_path(segment_path)
            relation = f"read_parquet('{escaped}', hive_partitioning=false)"
            expected_size = int(segment.file_size)
            local_identity = _local_parquet_file_identity(segment_path)
            provider_identity: Optional[tuple[int, str]] = None
            if local_identity is not None:
                if local_identity[2] != expected_size:
                    raise RuntimeError(
                        "Deletion-vector segment file_size does not match "
                        "the manifest"
                    )
            else:
                expected_provider_identity = getattr(
                    segment, "provider_identity", None,
                )
                if storage is None or expected_provider_identity is None:
                    raise RuntimeError(
                        "Remote deletion-vector segment lacks a pinned "
                        "provider identity"
                    )
                provider_identity = _provider_parquet_file_identity(
                    storage, str(segment.cache_key),
                )
                if provider_identity[0] != expected_size:
                    raise RuntimeError(
                        "Deletion-vector segment file_size does not match "
                        "the manifest"
                    )
                if provider_identity[1] != expected_provider_identity:
                    raise RuntimeError(
                        "Deletion-vector segment provider identity does not "
                        "match the pinned observation"
                    )
            con.execute(
                f"CREATE TEMPORARY TABLE {quote_if_needed(staging)} AS "
                f"SELECT * FROM {relation};"
            )
            staging_tables.append(staging)
            if local_identity is not None:
                if _local_parquet_file_identity(segment_path) != local_identity:
                    raise RuntimeError(
                        "Deletion-vector segment changed while being read"
                    )
            elif _provider_parquet_file_identity(
                storage, str(segment.cache_key),
            ) != provider_identity:
                raise RuntimeError(
                    "Deletion-vector segment changed while being read"
                )
            _validate_tombstone_relation_details(
                con,
                quote_if_needed(staging),
                expected_rows=int(segment.expected_rows),
                expected_digest=str(segment.tombstone_digest),
                allowed_files=allowed_files,
                validate_rows=True,
            )

        union_relation = _v2_union_relation([
            quote_if_needed(table) for table in staging_tables
        ])
        candidate = base_table_name
        if candidate in occupied_table_names:
            candidate = f"{base_table_name}_{uuid.uuid4().hex}"
        while True:
            try:
                temporary_sql = "TEMPORARY " if temporary else ""
                con.execute(
                    f"CREATE {temporary_sql}TABLE "
                    f"{quote_if_needed(candidate)} AS "
                    f"SELECT * FROM {union_relation};"
                )
                final_table = candidate
                break
            except duckdb.CatalogException as create_err:
                if "already exists" not in str(create_err).lower():
                    raise
                candidate = f"{base_table_name}_{uuid.uuid4().hex}"

        row_count, digest, referenced_files = (
            _validate_tombstone_relation_details(
                con,
                quote_if_needed(final_table),
                expected_rows=getattr(tombstone_def, "expected_rows", None),
                # The snapshot digest seals canonical manifest JSON, not the
                # logical union. Per-segment digests were checked above.
                expected_digest=None,
                allowed_files=allowed_files,
                validate_rows=True,
            )
        )
        return final_table, row_count, digest, referenced_files
    except Exception:
        if final_table is not None:
            try:
                con.execute(
                    f"DROP TABLE IF EXISTS {quote_if_needed(final_table)};"
                )
            except Exception:
                pass
        raise
    finally:
        for staging in reversed(staging_tables):
            try:
                con.execute(
                    f"DROP TABLE IF EXISTS {quote_if_needed(staging)};"
                )
            except Exception:
                pass


def _describe_relation(
        con: duckdb.DuckDBPyConnection, relation_sql: str,
) -> List[tuple]:
    try:
        return list(con.execute(f"DESCRIBE SELECT * FROM {relation_sql}").fetchall())
    except Exception as exc:
        raise RuntimeError(f"Unable to read deletion-vector schema: {exc}") from exc


def _validate_tombstone_relation_details(
        con: duckdb.DuckDBPyConnection,
        relation_sql: str,
        *,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        allowed_files: Optional[List[str]] = None,
        validate_rows: bool = True,
) -> tuple[int, str, frozenset[str]]:
    """Validate a deletion-vector relation, raising on any ambiguity.

    A malformed or truncated deletion vector must never silently become an
    empty/partial anti-join: that would resurrect deleted rows.  Current
    vectors have exactly ``(__file__ VARCHAR, __rowid__ BIGINT)`` and row ids
    are unique table-wide.  ``expected_rows`` is checked when a pinned
    snapshot supplies it; older snapshots lack that field, so the remaining
    structural/count invariants are still enforced.
    """
    rows = _describe_relation(con, relation_sql)
    actual = [(str(r[0]), str(r[1]).upper()) for r in rows]
    required = [(TOMBSTONE_FILE_COL, "VARCHAR"), (ROWID_COL, "BIGINT")]
    if actual != required:
        raise RuntimeError(
            "Invalid deletion-vector schema: expected exactly "
            f"{required}, got {actual}"
        )

    if not validate_rows:
        return -1, "", frozenset()

    file_col = quote_if_needed(TOMBSTONE_FILE_COL)
    rowid_col = quote_if_needed(ROWID_COL)
    # DuckDB's ordered string_agg can merge worker-local states in chunk order
    # when `threads > 1`, despite the aggregate ORDER BY.  That produced a
    # different digest from the writer for the same nine-row DV and made valid
    # SELECTs fail nondeterministically with the runtime thread setting.  Make
    # order a data operation instead: collect records in arbitrary order, sort
    # the completed fixed-format strings, then join.  Lexicographic record
    # order is exactly the v1 contract (base64 file, then 16-hex-digit rowid).
    digest_sql = (
        "sha256('supertable-tombstone-v1' || chr(10) || coalesce("
        "array_to_string(list_sort(list("
        f"to_base64(encode({file_col})) || ':' || "
        f"printf('%016x', {rowid_col}))), chr(10)), ''))"
    )
    allowed = None if allowed_files is None else {str(path) for path in allowed_files}
    try:
        (
            total, files, rowids, distinct_rowids, empty_files,
            invalid_rowids, actual_digest, referenced_file_values,
        ) = con.execute(
            "SELECT count(*), "
            f"count({file_col}), count({rowid_col}), "
            f"count(DISTINCT {rowid_col}), "
            f"count(*) FILTER (WHERE length({file_col}) = 0) "
            f", count(*) FILTER (WHERE {rowid_col} <= 0) "
            f", {digest_sql}, list(DISTINCT {file_col}) FROM {relation_sql}",
        ).fetchone()
    except Exception as exc:
        raise RuntimeError(f"Unable to validate deletion-vector rows: {exc}") from exc

    total = int(total)
    if (
        int(files) != total
        or int(rowids) != total
        or int(empty_files) != 0
        or int(invalid_rowids) != 0
    ):
        raise RuntimeError(
            "Invalid deletion vector: __file__ and __rowid__ must be non-null "
            "__file__ must be non-empty, and __rowid__ must be positive"
        )
    if int(distinct_rowids) != total:
        # The reader currently uses the writer's table-global row-id invariant.
        # Duplicate ids (especially across files) make that boundary ambiguous.
        raise RuntimeError("Invalid deletion vector: __rowid__ values are not unique")
    if expected_rows is not None:
        try:
            expected = int(expected_rows)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Invalid deletion-vector expected row count") from exc
        if expected < 0 or total != expected:
            raise RuntimeError(
                f"Invalid deletion-vector row count: expected {expected}, got {total}"
            )

    actual_digest = str(actual_digest).lower()
    if expected_digest is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", str(expected_digest)):
            raise RuntimeError("Invalid expected deletion-vector SHA-256 digest")
        if actual_digest != str(expected_digest):
            raise RuntimeError(
                "Invalid deletion-vector digest: immutable artifact does not "
                f"match the pinned snapshot (expected {expected_digest}, "
                f"got {actual_digest})"
            )

    referenced_files = frozenset(
        str(path) for path in (referenced_file_values or ())
    )
    if allowed is not None and not referenced_files.issubset(allowed):
        raise RuntimeError(
            "Invalid deletion vector: __file__ contains resources outside "
            "the pinned table snapshot"
        )
    return total, actual_digest, referenced_files


def validate_tombstone_relation(
        con: duckdb.DuckDBPyConnection,
        relation_sql: str,
        *,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        allowed_files: Optional[List[str]] = None,
        validate_rows: bool = True,
) -> tuple[int, str]:
    """Validate a deletion vector and return its exact count and digest.

    The private details variant also returns the referenced-file set so a
    cache miss can seal count, digest, and membership with one aggregate scan.
    Keeping this public return shape stable avoids making cache metadata part
    of the engine helper API.
    """
    count, digest, _files = _validate_tombstone_relation_details(
        con,
        relation_sql,
        expected_rows=expected_rows,
        expected_digest=expected_digest,
        allowed_files=allowed_files,
        validate_rows=validate_rows,
    )
    return count, digest


def _validate_tombstone_source_rowids(
        con: duckdb.DuckDBPyConnection,
        source_table: str,
        *,
        selected_resource_keys: List[str],
        referenced_dv_files: frozenset[str],
) -> None:
    """Fail closed when a DV key could identify more than one source row.

    The persisted deletion-vector identity is ``(__file__, __rowid__)``.  Its
    own schema/count/digest seal cannot prove that an older immutable data file
    does not contain the same row id twice.  If it does, anti-joining one DV
    entry removes both physical rows and silently loses the unrelated one.

    Scan only selected files that the pinned vector actually references.  The
    reflection exposes their exact stable keys through ``SOURCE_FILE_COL``;
    binary collation preserves case-sensitive object-key identity even on the
    pooled connections whose default collation is ``nocase``.  No process cache
    is used here because snapshots currently carry no immutable content seal
    (etag/digest) for data resources with which to fence such a cache safely.
    """
    selected_referenced = [
        str(path) for path in selected_resource_keys
        if str(path) in referenced_dv_files
    ]
    if not selected_referenced:
        return

    source = quote_if_needed(source_table)
    source_file = quote_if_needed(SOURCE_FILE_COL)
    rowid = quote_if_needed(ROWID_COL)
    try:
        invalid = con.execute(
            "SELECT count(*) AS total_rows, "
            f"count(src.{rowid}) AS nonnull_rows, "
            f"count(DISTINCT src.{rowid}) AS unique_rows, "
            f"min(src.{rowid}) AS min_rowid "
            f"FROM {source} AS src "
            "SEMI JOIN unnest(?) AS dv(__file__) ON "
            f"(src.{source_file} COLLATE \"binary\") = "
            "(dv.__file__ COLLATE \"binary\") "
            f"GROUP BY src.{source_file} COLLATE \"binary\" "
            f"HAVING count(*) <> count(src.{rowid}) "
            f"OR count(*) <> count(DISTINCT src.{rowid}) "
            f"OR min(src.{rowid}) IS NULL OR min(src.{rowid}) <= 0 "
            "LIMIT 1",
            [selected_referenced],
        ).fetchone()
    except Exception as exc:
        raise RuntimeError(
            "Unable to prove deletion-vector source row-id integrity"
        ) from exc

    if invalid is not None:
        total, nonnull, unique, minimum = invalid
        if int(nonnull) != int(total):
            defect = "NULL"
        elif minimum is None or int(minimum) <= 0:
            defect = "non-positive"
        else:
            defect = "duplicate"
        raise RuntimeError(
            "Cannot safely apply deletion vector: a referenced source file "
            f"contains {defect} __rowid__ values"
        )


def create_tombstone_view(
        con: duckdb.DuckDBPyConnection,
        source_table: str,
        view_name: str,
        tombstone_def,
        dv_table: Optional[str] = None,
) -> None:
    """
    Create a view that hides the system columns and drops tombstoned rows.

    Two responsibilities, applied to every reflected table:

      1. **Strip system columns** — ``__rowid__`` and ``__timestamp__`` are
         removed from the projection via a static DuckDB
         ``COLUMNS(c -> c NOT IN ('__rowid__', '__timestamp__'))`` so they
         never leak into user query results.  The predicate is fixed (no
         per-query schema introspection) yet tolerant: a column that is
         absent — e.g. pre-migration parquet that predates ``__rowid__`` —
         is simply not matched, instead of raising as a literal
         ``EXCLUDE`` list would.
      2. **Apply the deletion-vector** — rows whose ``__rowid__`` appears in
         the deletion-vector are removed with an ANTI JOIN *before* the
         columns are stripped.  The deletion-vector comes from one of two
         sources, in priority order:

         * *dv_table* — a validated, pre-materialised
           ``(__file__, __rowid__)`` table (see :class:`TombstoneCache`). Built
           once and reused across queries, avoiding a parquet re-read per query.
         * *tombstone_def.tombstone_path* — inline ``read_parquet`` of a legacy
           v1 deletion-vector parquet. Segmented v2 vectors always use a
           validated private table, including when persistent cache capacity
           is zero.

    This view sits directly on top of the reflection table (before RBAC),
    so the anti-join still has ``__rowid__`` available and RBAC never sees
    the system columns.

    Args:
        con: DuckDB connection
        source_table: the underlying reflection table or view
        view_name: the view name to create
        tombstone_def: TombstoneDef with ``tombstone_path`` (or None)
        dv_table: name of a pre-materialised deletion-vector table, or None
            to fall back to the inline ``read_parquet`` path
    """
    # Tolerant, static system-column strip: ``COLUMNS(c -> ...)`` silently
    # skips a system column that is absent (pre-``__rowid__`` parquet), where
    # a literal ``EXCLUDE`` list would raise. In an ANTI JOIN only the left
    # table's columns reach the output, so the unqualified ``COLUMNS()`` never
    # picks up the deletion-vector's ``__rowid__``.
    live_cols = (
        f"COLUMNS(c -> c NOT IN ('{ROWID_COL}', '{TIMESTAMP_COL}', "
        f"'{TOMBSTONE_FILE_COL}', '{SOURCE_FILE_COL}', '{SCAN_FILENAME_COL}'))"
    )

    tomb_path = getattr(tombstone_def, "tombstone_path", None) if tombstone_def else None
    expected_rows = getattr(tombstone_def, "expected_rows", None) if tombstone_def else None
    expected_digest = getattr(tombstone_def, "tombstone_digest", None) if tombstone_def else None
    v2_segments = _v2_tombstone_segments(tombstone_def)
    is_v2 = (
        tombstone_def is not None
        and getattr(tombstone_def, "tombstone_format", None)
        == TOMBSTONE_FORMAT_V2
    )
    active_v2 = is_v2 and tomb_path is not None
    resource_keys = list(getattr(tombstone_def, "resource_keys", ()) or ()) if tombstone_def else []
    raw_snapshot_keys = (
        getattr(tombstone_def, "snapshot_resource_keys", None)
        if tombstone_def else None
    )
    allowed_dv_files = (
        list(raw_snapshot_keys)
        if raw_snapshot_keys is not None
        else (resource_keys or None)
    )
    rid = quote_if_needed(ROWID_COL)
    file_col = quote_if_needed(TOMBSTONE_FILE_COL)
    source_file_col = quote_if_needed(SOURCE_FILE_COL)
    source_desc = _describe_relation(con, quote_if_needed(source_table))
    canonical_reserved = {
        ROWID_COL.casefold(): ROWID_COL,
        TIMESTAMP_COL.casefold(): TIMESTAMP_COL,
        SOURCE_FILE_COL.casefold(): SOURCE_FILE_COL,
        SCAN_FILENAME_COL.casefold(): SCAN_FILENAME_COL,
        TOMBSTONE_FILE_COL.casefold(): TOMBSTONE_FILE_COL,
    }
    seen_reserved = set()
    for row in source_desc:
        name = str(row[0])
        folded = name.casefold()
        is_reserved = folded in canonical_reserved or folded.startswith("__supertable_")
        if not is_reserved:
            continue
        expected = canonical_reserved.get(folded)
        if expected is None or name != expected or folded in seen_reserved:
            raise RuntimeError(
                f"Invalid reserved system column in reflection schema: {name!r}"
            )
        seen_reserved.add(folded)
    if tomb_path:
        rowid_rows = [row for row in source_desc if str(row[0]) == ROWID_COL]
        if len(rowid_rows) != 1 or str(rowid_rows[0][1]).upper() != "BIGINT":
            raise RuntimeError(
                "Cannot apply deletion vector: source requires canonical "
                "__rowid__ BIGINT"
            )
        if not resource_keys:
            raise RuntimeError(
                "Cannot apply deletion vector without positional canonical "
                "resource keys for a composite anti-join"
            )
    if tomb_path and resource_keys:
        source_schema = {
            str(row[0]) for row in source_desc
        }
        if SOURCE_FILE_COL not in source_schema:
            raise RuntimeError(
                "Cannot apply composite deletion vector: reflection has no "
                "canonical source-file identity"
            )

    referenced_dv_files: frozenset[str] = frozenset()
    owned_direct_v2_table: Optional[str] = None
    if active_v2 and not dv_table:
        # Direct helper callers do not have a TombstoneCache lifecycle. Keep a
        # private connection-local table behind the created view; it disappears
        # with the caller's connection. Executor callers always supply the
        # cache-owned (or query-lifetime capacity=0) table instead.
        table_name, row_count, digest, referenced_dv_files = (
            _materialize_v2_tombstone_table(
                con,
                tombstone_def,
                base_table_name=f"dv_direct_{uuid.uuid4().hex}",
                occupied_table_names=set(),
                allowed_files=allowed_dv_files,
                temporary=True,
            )
        )
        dv_table = ValidatedTombstoneTable(
            table_name,
            row_count,
            digest,
            referenced_dv_files,
            root_digest=expected_digest,
            cache_key=getattr(tombstone_def, "cache_key", None),
            segment_fingerprint=_v2_segment_fingerprint(v2_segments),
        )
        owned_direct_v2_table = table_name
    if dv_table:
        # Tables returned by TombstoneCache were fully checked once when they
        # were materialised.  Direct callers receive the same full validation
        # here instead of being able to smuggle a partial/malformed relation
        # into the anti-join.
        if not isinstance(dv_table, ValidatedTombstoneTable):
            if active_v2:
                # A plain table name has no proof tying its contents to every
                # manifest-sealed segment. Re-reading those external paths to
                # manufacture that proof would recreate the substitution race
                # this boundary is designed to remove.
                raise RuntimeError(
                    "V2 deletion vectors require a validated materialized table"
                )
            _, _, referenced_dv_files = _validate_tombstone_relation_details(
                con,
                quote_if_needed(str(dv_table)),
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                allowed_files=allowed_dv_files,
                validate_rows=True,
            )
        else:
            referenced_dv_files = dv_table.referenced_files
        if (
            isinstance(dv_table, ValidatedTombstoneTable)
            and expected_rows is not None
            and dv_table.row_count != int(expected_rows)
        ):
            raise RuntimeError(
                "Invalid deletion-vector row count: expected "
                f"{int(expected_rows)}, got {dv_table.row_count}"
            )
        if isinstance(dv_table, ValidatedTombstoneTable) and expected_digest is not None:
            cached_seal = (
                dv_table.root_digest if active_v2 else dv_table.digest
            )
            if cached_seal != expected_digest:
                raise RuntimeError(
                    "Invalid deletion-vector digest: cached artifact does not "
                    "match the pinned snapshot"
                )
        if (
            isinstance(dv_table, ValidatedTombstoneTable)
            and active_v2
            and dv_table.segment_fingerprint
            != _v2_segment_fingerprint(v2_segments)
        ):
            raise RuntimeError(
                "Invalid deletion-vector cache entry: segment descriptors "
                "do not match the pinned manifest"
            )
        if (
            isinstance(dv_table, ValidatedTombstoneTable)
            and allowed_dv_files is not None
        ):
            if not dv_table.referenced_files.issubset(
                {str(path) for path in allowed_dv_files}
            ):
                raise RuntimeError(
                    "Invalid deletion vector: __file__ contains resources "
                    "outside the pinned table snapshot"
                )
        # Cached deletion-vector: anti-join the already-materialised composite
        # identity table. It is semantically identical to the inline subquery.
        join_clause = f"{source_table}.{rid} = __dv__.{rid}"
        if resource_keys:
            join_clause += (
                f" AND ({source_table}.{source_file_col} COLLATE \"binary\") "
                f"= (__dv__.{file_col} COLLATE \"binary\")"
            )
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table} "
            f"ANTI JOIN {dv_table} AS __dv__ "
            f"ON {join_clause};"
        )
    elif tomb_path:
        escaped = escape_parquet_path(tomb_path)
        # Tombstones are physically stored below Hive-looking
        # year=/month=/day=/hour= directories. Those path components are
        # not DV columns; inference would widen the sealed schema.
        relation = (
            f"read_parquet('{escaped}', hive_partitioning=false)"
        )
        _, _, referenced_dv_files = _validate_tombstone_relation_details(
            con, relation, expected_rows=expected_rows,
            expected_digest=expected_digest,
            allowed_files=allowed_dv_files, validate_rows=True,
        )
        dv_projection = f"DISTINCT {rid}"
        join_clause = f"{source_table}.{rid} = __dv__.{rid}"
        if resource_keys:
            dv_projection = f"DISTINCT {file_col}, {rid}"
            join_clause += (
                f" AND ({source_table}.{source_file_col} COLLATE \"binary\") "
                f"= (__dv__.{file_col} COLLATE \"binary\")"
            )
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table} "
            f"ANTI JOIN (SELECT {dv_projection} FROM "
            f"{relation}) AS __dv__ "
            f"ON {join_clause};"
        )
    else:
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table};"
        )
    try:
        if tomb_path:
            _validate_tombstone_source_rowids(
                con,
                source_table,
                selected_resource_keys=resource_keys,
                referenced_dv_files=referenced_dv_files,
            )
        con.execute(sql)
    except Exception:
        if owned_direct_v2_table is not None:
            try:
                con.execute(
                    f"DROP TABLE IF EXISTS "
                    f"{quote_if_needed(owned_direct_v2_table)};"
                )
            except Exception:
                pass
        raise


# =========================================================
# Deletion-vector (tombstone) table cache
# =========================================================

@dataclass
class _DVCacheEntry:
    """One materialised deletion-vector table tracked by TombstoneCache."""
    table_name: str          # DuckDB table name (e.g. dv_a3f8c1...)
    cache_key: str           # stable tombstone path this DV was built from
    table_id: str            # logical table this version belongs to
    # Capacity-zero v2 tables are TEMPORARY and therefore visible only through
    # the cursor that created them.  Retaining that cursor both prevents its
    # ``id`` from being reused while the entry is live and lets eviction issue
    # the DROP against the correct connection context.
    owner_connection: object = None
    ref_count: int = 0       # in-flight queries currently anti-joining it
    last_used: int = 0       # monotonic tick — per-table LRU ordering
    expires_at: float = 0.0  # idle deadline; refreshed on every acquire


def dv_table_name(cache_key: str) -> str:
    """Deterministic DuckDB table name for a deletion-vector cache key."""
    # Keep the full digest.  A shortened name is not merely a cache miss risk:
    # CREATE OR REPLACE on a collision can swap the DV under an in-flight query
    # and either resurrect a deleted row or hide a live one.
    h = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
    return f"dv_{h}"


def dv_table_id(cache_key: str) -> str:
    """Group key identifying the logical table a deletion-vector belongs to.

    Every version of a table's deletion-vector is written to the same
    directory (``<simple_dir>/tombstone/<ts>_<rand>_deleted.parquet``); only
    the filename rotates when a write adds new deletes.  The parent directory
    is therefore a stable per-table identity derivable from the cache key
    alone — no extra plumbing from the engines.  Falls back to the whole key
    when there is no separator (each version then groups alone, which merely
    relaxes the per-table cap for that key — never a correctness issue, since
    the materialised DV table is keyed by the path hash regardless).
    """
    # New artifacts are hour-partitioned below ``tombstone/``.  Group all
    # hours/versions of one table together rather than treating each hour as a
    # different logical table and accidentally multiplying the cache cap.
    normalized = cache_key.replace("\\", "/")
    marker = "/tombstone/"
    marker_idx = normalized.lower().find(marker)
    if marker_idx >= 0:
        return normalized[:marker_idx + len("/tombstone")]
    idx = max(cache_key.rfind("/"), cache_key.rfind("\\"))
    return cache_key[:idx] if idx > 0 else cache_key


class TombstoneCache:
    """Materialises deletion-vectors as cached DuckDB tables, keyed by the
    *stable* tombstone path, with **per-table** eviction.

    Why a table and not the inline subquery?  The tombstone parquet is
    re-read on every query in the inline form.  Because the tombstone path is
    stable across pure appends (the writer carries forward the previous
    deletion-vector when a write adds no new deletes), the same validated
    composite identity relation is recomputed needlessly. Materialising it
    once lets every subsequent tombstone view ANTI JOIN the table directly.
    The anti-join result is bit-identical to the inline form.

    Eviction combines table-local fairness with a process-wide safety ceiling:

      * **Idle TTL.**  Every entry carries ``expires_at = now + ttl``, refreshed
        on every acquire.  The lazy sweep drops any unreferenced entry past its
        deadline, including a table's most-recent version: a table that stops
        being queried for ``ttl`` seconds reclaims its whole cache instead of
        lingering until the connection resets.  ``ttl <= 0`` keeps an entry only
        while a query references it (no persistence).
      * **Per-table cap.**  At most ``capacity`` most-recently-used versions are
        retained per table; the sweep evicts the least-recently-used
        unreferenced ones beyond that.  A burst of rewrites (e.g. 1000 updates
        in 5 minutes) keeps only the last ``capacity`` versions of *that* table
        and touches no other table.  ``capacity <= 0`` disables persistence;
        segmented v2 vectors are still materialised once for the lifetime of
        the query and dropped on release, while legacy v1 keeps its inline
        ``read_parquet`` fallback.

      * **Global cap.** At most ``global_capacity`` entries remain resident;
        the oldest unreferenced entry is evicted across tables when necessary.
        In-flight entries are never dropped, so a brief overage is allowed.

    Thread-safe: every registry mutation and the DDL it triggers is guarded
    by an internal lock.  The lock is only ever acquired *after* an engine's
    own connection lock, never before, so the two compose without deadlock.
    """

    def __init__(
            self,
            capacity: int,
            ttl_seconds: int = 0,
            global_capacity: int = 128,
            *,
            time_fn: Callable[[], float] = time.monotonic,
            storage: Optional[object] = None,
    ):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.global_capacity = global_capacity
        self._time = time_fn
        self.storage = storage
        self._lock = threading.Lock()
        self._registry: Dict[str, _DVCacheEntry] = {}   # cache_key -> entry
        self._tick = 0

    @property
    def enabled(self) -> bool:
        return self.capacity > 0

    def acquire(
            self,
            con: duckdb.DuckDBPyConnection,
            cache_key: Optional[str],
            duckdb_path: Optional[str],
            expected_rows: Optional[int] = None,
            expected_digest: Optional[str] = None,
            *,
            tombstone_def=None,
            allowed_files: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Return the DV table name for *cache_key*, materialising it on miss,
        refreshing its idle TTL, and incrementing its ref count.  Segmented v2
        vectors always get a query-lifetime table, even when persistent cache
        capacity is zero. Legacy v1 returns ``None`` when caching is disabled
        or its inputs are incomplete, preserving its inline fallback.
        """
        validated_segments = (
            _v2_tombstone_segments(tombstone_def)
            if tombstone_def is not None else ()
        )
        is_v2 = (
            tombstone_def is not None
            and getattr(tombstone_def, "tombstone_format", None)
            == TOMBSTONE_FORMAT_V2
        )
        active_v2 = (
            is_v2
            and getattr(tombstone_def, "tombstone_path", None) is not None
        )
        segment_fingerprint = None
        if active_v2:
            segment_fingerprint = _v2_segment_fingerprint(
                validated_segments
            )
            pinned_cache_key = getattr(tombstone_def, "cache_key", None)
            pinned_rows = getattr(tombstone_def, "expected_rows", None)
            pinned_digest = getattr(tombstone_def, "tombstone_digest", None)
            if cache_key != pinned_cache_key:
                raise RuntimeError(
                    "V2 deletion-vector cache key does not match the pinned "
                    "manifest"
                )
            if expected_rows is not None and (
                type(expected_rows) is not int
                or expected_rows != pinned_rows
            ):
                raise RuntimeError(
                    "V2 deletion-vector row count does not match the pinned "
                    "manifest"
                )
            if expected_digest != pinned_digest:
                raise RuntimeError(
                    "V2 deletion-vector digest does not match the pinned "
                    "manifest"
                )
            if (
                not isinstance(expected_digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", expected_digest) is None
            ):
                raise RuntimeError(
                    "Invalid v2 deletion-vector manifest root digest"
                )
        if (
            not cache_key
            or (not active_v2 and not duckdb_path)
            or (not active_v2 and not self.enabled)
        ):
            return None
        shared_registry_key = (
            f"dv-v2:{cache_key}:{expected_digest}"
            if active_v2 else cache_key
        )
        # DuckDB TEMP tables are cursor-local.  Persistent cache entries can be
        # shared by every cursor on the root connection, but capacity-zero v2
        # entries must only be reused within the cursor that materialised them.
        # Keeping the cursor on the entry prevents Python from recycling its id
        # before release and gives the eviction path the correct DROP context.
        connection_local = active_v2 and not self.enabled
        registry_key = (
            f"{shared_registry_key}:connection:{id(con):x}"
            if connection_local else shared_registry_key
        )
        with self._lock:
            entry = self._registry.get(registry_key)
            if entry is None:
                base_table_name = dv_table_name(registry_key)
                table_name = base_table_name
                occupied_names = {
                    str(existing.table_name)
                    for existing in self._registry.values()
                }
                if table_name in occupied_names:
                    table_name = f"{base_table_name}_{uuid.uuid4().hex}"
                if active_v2:
                    (
                        table_name,
                        row_count,
                        digest,
                        referenced_files,
                    ) = _materialize_v2_tombstone_table(
                        con,
                        tombstone_def,
                        base_table_name=table_name,
                        occupied_table_names=occupied_names,
                        allowed_files=allowed_files,
                        temporary=not self.enabled,
                        storage=self.storage,
                    )
                else:
                    rid = quote_if_needed(ROWID_COL)
                    file_col = quote_if_needed(TOMBSTONE_FILE_COL)
                    escaped = escape_parquet_path(duckdb_path)
                    relation = (
                        f"read_parquet('{escaped}', hive_partitioning=false)"
                    )
                    try:
                        # Materialise privately, validate, and only then
                        # publish a registry entry. CREATE (never CREATE OR
                        # REPLACE) is the ownership boundary: an unregistered
                        # table may still be referenced by an in-flight cursor.
                        while True:
                            try:
                                con.execute(
                                    f"CREATE TABLE {table_name} AS "
                                    f"SELECT {file_col}, {rid} FROM {relation};"
                                )
                                break
                            except duckdb.CatalogException as create_err:
                                if "already exists" not in str(create_err).lower():
                                    raise
                                table_name = (
                                    f"{base_table_name}_{uuid.uuid4().hex}"
                                )
                        (
                            row_count,
                            digest,
                            referenced_files,
                        ) = _validate_tombstone_relation_details(
                            con, quote_if_needed(table_name),
                            expected_rows=expected_rows,
                            expected_digest=expected_digest,
                            allowed_files=allowed_files,
                            validate_rows=True,
                        )
                    except Exception:
                        try:
                            con.execute(
                                f"DROP TABLE IF EXISTS "
                                f"{quote_if_needed(table_name)};"
                            )
                        except Exception:
                            pass
                        raise
                entry = _DVCacheEntry(
                    table_name=ValidatedTombstoneTable(
                        table_name, row_count, digest,
                        referenced_files,
                        root_digest=(expected_digest if active_v2 else None),
                        cache_key=registry_key,
                        segment_fingerprint=segment_fingerprint,
                    ),
                    cache_key=registry_key,
                    table_id=dv_table_id(cache_key),
                    owner_connection=(con if connection_local else None),
                )
                self._registry[registry_key] = entry

            if (
                active_v2
                and getattr(
                    entry.table_name, "segment_fingerprint", None,
                ) != segment_fingerprint
            ):
                raise RuntimeError(
                    "Invalid deletion-vector cache entry: segment descriptors "
                    "do not match the pinned manifest"
                )

            if expected_rows is not None:
                table_count = getattr(entry.table_name, "row_count", -1)
                try:
                    expected = int(expected_rows)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError("Invalid deletion-vector expected row count") from exc
                if expected < 0 or table_count != expected:
                    raise RuntimeError(
                        f"Invalid deletion-vector row count: expected {expected}, "
                        f"got {table_count}"
                    )

            if expected_digest is not None:
                cached_digest = getattr(
                    entry.table_name,
                    "root_digest" if active_v2 else "digest",
                    None,
                )
                if cached_digest != expected_digest:
                    raise RuntimeError(
                        "Invalid deletion-vector digest: cached artifact does "
                        "not match the pinned snapshot"
                    )

            if allowed_files is not None:
                referenced_files = getattr(
                    entry.table_name, "referenced_files", frozenset(),
                )
                if not referenced_files.issubset(
                    {str(path) for path in allowed_files}
                ):
                    raise RuntimeError(
                        "Invalid deletion vector: __file__ contains resources "
                        "outside the pinned table snapshot"
                    )

            entry.ref_count += 1
            self._tick += 1
            entry.last_used = self._tick
            entry.expires_at = self._deadline()   # refresh idle TTL on access
            self._sweep_locked(con)
            return entry.table_name

    def release(
            self,
            con: duckdb.DuckDBPyConnection,
            cache_key: Optional[str],
    ) -> None:
        """Decrement the ref count for *cache_key* and run the lazy sweep."""
        if not cache_key:
            return
        with self._lock:
            entry = self._registry.get(cache_key)
            if entry is not None:
                entry.ref_count = max(0, entry.ref_count - 1)
            self._sweep_locked(con)

    def _deadline(self) -> float:
        """Idle deadline for a just-accessed entry.

        ``ttl <= 0`` returns "now" so the entry is dropped as soon as it is
        unreferenced (no persistence beyond in-flight queries); otherwise it
        gets ``now + ttl``.
        """
        now = self._time()
        return now if self.ttl_seconds <= 0 else now + self.ttl_seconds

    def _drop_entry_locked(
            self, con: duckdb.DuckDBPyConnection, entry: _DVCacheEntry,
    ) -> None:
        try:
            drop_con = (
                entry.owner_connection
                if entry.owner_connection is not None
                else con
            )
            drop_con.execute(f"DROP TABLE IF EXISTS {entry.table_name};")
        except Exception:
            pass
        self._registry.pop(entry.cache_key, None)

    def _sweep_locked(self, con: duckdb.DuckDBPyConnection) -> None:
        """Apply the two table-local eviction rules; never drop an in-flight
        (``ref_count > 0``) entry.

          1. Idle TTL — drop every unreferenced entry past its deadline.
          2. Per-table cap — within each table keep at most ``capacity``
             most-recently-used versions, evicting the LRU unreferenced ones.
             Pinned entries are exempt, so a table may briefly exceed the cap
             while many of its versions are in flight; it shrinks back as they
             release.
        """
        now = self._time()
        for e in list(self._registry.values()):
            if e.ref_count == 0 and now >= e.expires_at:
                self._drop_entry_locked(con, e)

        by_table: Dict[str, List[_DVCacheEntry]] = {}
        for e in self._registry.values():
            by_table.setdefault(e.table_id, []).append(e)
        for entries in by_table.values():
            if len(entries) <= self.capacity:
                continue
            entries.sort(key=lambda e: e.last_used)   # oldest first
            excess = len(entries) - self.capacity
            for e in entries:
                if excess <= 0:
                    break
                if e.ref_count == 0:
                    self._drop_entry_locked(con, e)
                    excess -= 1

        if self.global_capacity > 0 and len(self._registry) > self.global_capacity:
            excess = len(self._registry) - self.global_capacity
            global_lru = sorted(
                self._registry.values(), key=lambda e: e.last_used,
            )
            for e in global_lru:
                if excess <= 0:
                    break
                if e.ref_count == 0:
                    self._drop_entry_locked(con, e)
                    excess -= 1

    def clear_registry(self) -> None:
        """Forget every entry *without* issuing DROPs.

        Called when the underlying connection has already been closed/reset —
        the tables vanished with it, so there is nothing to drop, only state
        to forget.
        """
        with self._lock:
            self._registry.clear()

    def snapshot(self) -> List[Dict]:
        """Diagnostics: a copy of the current registry state."""
        with self._lock:
            return [
                {
                    "table_name": e.table_name,
                    "cache_key": e.cache_key,
                    "table_id": e.table_id,
                    "ref_count": e.ref_count,
                    "last_used": e.last_used,
                    "expires_at": e.expires_at,
                }
                for e in self._registry.values()
            ]

# (ParquetMetadataCache removed: was write-only dead weight.
#  DuckDB's built-in enable_http_metadata_cache on the persistent connection
#  already handles parquet footer caching at the connection level.
#  The external file cache (enable_external_file_cache) is now configured
#  inside configure_httpfs_and_s3, after LOAD httpfs, where the setting
#  actually exists.)
