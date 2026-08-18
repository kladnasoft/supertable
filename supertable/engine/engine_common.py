# route: supertable.engine.engine_common

from __future__ import annotations

import hashlib
import ast
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlsplit, urlunsplit

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.config.homedir import get_app_home
from supertable.engine.engine_config import normalize_memory_size


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
            if parsed.query or parsed.fragment:
                url = urlunsplit(
                    (parsed.scheme, parsed.netloc, parsed.path, "<redacted>", "")
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
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
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

def configure_httpfs_and_s3(
        con: duckdb.DuckDBPyConnection, for_paths: List[str]
) -> None:
    """Load httpfs and configure S3 credentials + caches on the given connection.

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
            "s3_endpoint", "s3_region", "s3_access_key_id",
            "s3_secret_access_key", "s3_session_token",
            "s3_url_style", "s3_use_ssl",
            "http_timeout", "enable_http_metadata_cache",
            "enable_external_file_cache", "external_file_cache_max_size",
            "external_file_cache_directory",
        }

    def set_if_supported(param: str, value_sql: str):
        if param in supported:
            con.execute(f"SET {param}={sanitize_sql_string(value_sql)};")

    endpoint = detect_endpoint()
    access_key, secret_key, session_token = detect_creds()
    region = detect_region()
    url_style = detect_url_style()
    use_ssl = detect_ssl()

    if endpoint:
        set_if_supported("s3_endpoint", f"'{endpoint}'")
    if access_key:
        set_if_supported("s3_access_key_id", f"'{access_key}'")
    if secret_key:
        set_if_supported("s3_secret_access_key", f"'{secret_key}'")
    if session_token:
        set_if_supported("s3_session_token", f"'{session_token}'")
    if region:
        set_if_supported("s3_region", f"'{region}'")
    set_if_supported("s3_url_style", f"'{url_style}'")
    set_if_supported("s3_use_ssl", "TRUE" if use_ssl else "FALSE")

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
    configure_httpfs_and_s3(con, files)
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
            configure_httpfs_and_s3(con, presigned_files)
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

    configure_httpfs_and_s3(con, files)
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
            configure_httpfs_and_s3(con, presigned_files)
            create_reflection_view(
                con, view_name, presigned_files, columns, resource_keys,
            )
        else:
            raise

    return tried_presign


# =========================================================
# Query rewriting
# =========================================================

def rewrite_query_with_hashed_tables(
        original_sql: str,
        alias_to_table: Dict[str, str],
        *,
        parsed_expression: Optional[exp.Expression] = None,
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
    cte_reference_ids: set[int] = set()
    try:
        for scope in traverse_scope(parsed):
            for selected in scope.selected_sources.values():
                node, source = selected
                if isinstance(node, exp.Table) and not isinstance(source, exp.Table):
                    cte_reference_ids.add(id(node))
    except Exception as exc:
        raise RuntimeError(
            "Unable to prove protected query table bindings"
        ) from exc
    rewritten: set[str] = set()

    for table in parsed.find_all(exp.Table):
        if id(table) in cte_reference_ids:
            # Query-local CTE reference; its physical leaf sources are handled
            # independently below/by their own Table nodes.
            continue
        alias_expr = table.args.get("alias")
        alias_name = None

        if isinstance(alias_expr, exp.TableAlias):
            ident = alias_expr.this
            if isinstance(ident, exp.Identifier):
                alias_name = ident.name

        if not alias_name:
            alias_name = table.name

        target = folded_targets.get(str(alias_name).casefold())
        if target is not None:
            canonical_alias, new_physical = target
            rewritten.add(canonical_alias.casefold())
            table.set("this", exp.to_identifier(new_physical))
            table.set("db", None)
            # Ensure the alias is always present so qualified column references
            # (e.g. table_1.col) remain valid after the table is renamed.
            # When the user wrote an explicit alias we keep it; when there was
            # no alias the table name itself was used as the alias key, so we
            # set it explicitly here.
            if not isinstance(alias_expr, exp.TableAlias):
                table.set(
                    "alias",
                    exp.TableAlias(this=exp.to_identifier(alias_name)),
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

    return parsed.sql(dialect="duckdb")


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
    if rbac_view_def.where_clause:
        where_sql = f" WHERE {rbac_view_def.where_clause}"

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
    scan on every cache hit while still fully validating an arbitrary table
    name supplied by a direct caller.
    """

    def __new__(
            cls, value: str, row_count: int = -1, digest: Optional[str] = None,
            referenced_files=None,
    ):
        obj = str.__new__(cls, value)
        obj.row_count = int(row_count)
        obj.digest = digest
        obj.referenced_files = frozenset(referenced_files or ())
        return obj


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
         * *tombstone_def.tombstone_path* — inline ``read_parquet`` of the
           deletion-vector parquet.  Used when the cache is disabled or has
           no stable key.  This is the legacy path and is semantically
           identical to the cached one (same composite identities, same
           anti-join, same ``__dv__`` alias).

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
    if dv_table:
        # Tables returned by TombstoneCache were fully checked once when they
        # were materialised.  Direct callers receive the same full validation
        # here instead of being able to smuggle a partial/malformed relation
        # into the anti-join.
        if not isinstance(dv_table, ValidatedTombstoneTable):
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
        if (
            isinstance(dv_table, ValidatedTombstoneTable)
            and expected_digest is not None
            and dv_table.digest != expected_digest
        ):
            raise RuntimeError(
                "Invalid deletion-vector digest: cached artifact does not match "
                "the pinned snapshot"
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
        # year=/month=/day=/hour= directories.  Those path components are not
        # DV columns; inference would widen the relation's sealed two-column
        # schema and can make cached/inline validation disagree.
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
            f"read_parquet('{escaped}', hive_partitioning=false)) AS __dv__ "
            f"ON {join_clause};"
        )
    else:
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table};"
        )
    if tomb_path:
        _validate_tombstone_source_rowids(
            con,
            source_table,
            selected_resource_keys=resource_keys,
            referenced_dv_files=referenced_dv_files,
        )
    con.execute(sql)


# =========================================================
# Deletion-vector (tombstone) table cache
# =========================================================

@dataclass
class _DVCacheEntry:
    """One materialised deletion-vector table tracked by TombstoneCache."""
    table_name: str          # DuckDB table name (e.g. dv_a3f8c1...)
    cache_key: str           # stable tombstone path this DV was built from
    table_id: str            # logical table this version belongs to
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
        and touches no other table.  ``capacity <= 0`` disables the cache
        entirely (callers fall back to the inline ``read_parquet`` path).

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
    ):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.global_capacity = global_capacity
        self._time = time_fn
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
    ) -> Optional[str]:
        """Return the DV table name for *cache_key*, materialising it on miss,
        refreshing its idle TTL, and incrementing its ref count.  Returns
        ``None`` when caching is disabled or the inputs are incomplete — the
        caller then falls back to the inline ``read_parquet`` path, preserving
        exact legacy behaviour.
        """
        if not self.enabled or not cache_key or not duckdb_path:
            return None
        with self._lock:
            entry = self._registry.get(cache_key)
            if entry is None:
                base_table_name = dv_table_name(cache_key)
                table_name = base_table_name
                occupied_names = {
                    str(existing.table_name)
                    for existing in self._registry.values()
                }
                if table_name in occupied_names:
                    table_name = f"{base_table_name}_{uuid.uuid4().hex}"
                rid = quote_if_needed(ROWID_COL)
                file_col = quote_if_needed(TOMBSTONE_FILE_COL)
                escaped = escape_parquet_path(duckdb_path)
                relation = (
                    f"read_parquet('{escaped}', hive_partitioning=false)"
                )
                # Materialise privately, validate, and only then publish a
                # registry entry.  Validating the table (rather than scanning
                # the parquet first and then CTAS-ing it) keeps a cache miss to
                # one remote read. CTAS retains both fields because DuckDB
                # reflections expose the stable key required by the composite
                # anti-join.
                # CREATE (never CREATE OR REPLACE) is the ownership boundary:
                # an unregistered table can still be referenced by an
                # in-flight cursor after a connection/cache reset.  If the
                # deterministic name is already present, allocate a private
                # suffix and retry; never overwrite or DROP the unknown table.
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
                try:
                    (
                        row_count,
                        digest,
                        referenced_files,
                    ) = _validate_tombstone_relation_details(
                        con, quote_if_needed(table_name),
                        expected_rows=expected_rows,
                        expected_digest=expected_digest,
                        validate_rows=True,
                    )
                except Exception:
                    try:
                        con.execute(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception:
                        pass
                    raise
                entry = _DVCacheEntry(
                    table_name=ValidatedTombstoneTable(
                        table_name, row_count, digest,
                        referenced_files,
                    ),
                    cache_key=cache_key,
                    table_id=dv_table_id(cache_key),
                )
                self._registry[cache_key] = entry

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
                cached_digest = getattr(entry.table_name, "digest", None)
                if cached_digest != expected_digest:
                    raise RuntimeError(
                        "Invalid deletion-vector digest: cached artifact does "
                        "not match the pinned snapshot"
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
        if not self.enabled or not cache_key:
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
            con.execute(f"DROP TABLE IF EXISTS {entry.table_name};")
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
