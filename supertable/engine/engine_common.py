# route: supertable.engine.engine_common

from __future__ import annotations

import hashlib
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import duckdb
import sqlglot
from sqlglot import exp

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
) -> str:
    """Generate a deterministic table name for pro mode (all columns, version-scoped)."""
    key = f"{super_name}_{simple_name}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
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

    any_s3 = any(str(p).lower().startswith("s3://") for p in for_paths)
    any_http = any(str(p).lower().startswith(("http://", "https://")) for p in for_paths)

    if not (any_s3 or any_http):
        return

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
                logger.warning(f"[presign] failed for '{p}': {e}")
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
    public = [c for c in columns if c and c.strip() and c not in system]
    wants_system = any(c in system for c in columns)
    parts = [quote_if_needed(c) for c in public]
    if wants_system:
        parts.append(f"COLUMNS(c -> c IN ('{ROWID_COL}', '{TIMESTAMP_COL}'))")
    return ", ".join(parts) if parts else "*"


def create_reflection_table(
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
) -> None:
    """CREATE TABLE ... AS SELECT ... FROM parquet_scan(...)."""
    if not files:
        raise ValueError(f"No files provided for reflection table '{table_name}'")

    parquet_files_str = ", ".join(f"'{escape_parquet_path(f)}'" for f in files)
    select_cols = _reflection_select_cols(columns)

    sql = (
        f"CREATE TABLE {table_name} AS "
        f"SELECT {select_cols} "
        f"FROM parquet_scan([{parquet_files_str}], "
        f"union_by_name=TRUE, HIVE_PARTITIONING=FALSE);"
    )
    con.execute(sql)


def create_reflection_table_with_presign_retry(
        con: duckdb.DuckDBPyConnection,
        storage,
        table_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        log_prefix: str = "",
) -> bool:
    """
    Create a reflection table with automatic presign fallback on HTTP errors.
    Returns True if presign retry was used.
    """
    configure_httpfs_and_s3(con, files)
    tried_presign = False

    try:
        create_reflection_table(con, table_name, files, columns)
    except Exception as e:
        msg = str(e)
        if any(tok in msg for tok in (
                "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                "AccessDenied", "SignatureDoesNotMatch", "403", "400",
        )):
            logger.warning(f"{log_prefix}[duckdb.retry] presign fallback for {table_name}: {msg}")
            tried_presign = True
            presigned_files = make_presigned_list(storage, files)
            configure_httpfs_and_s3(con, presigned_files)
            create_reflection_table(con, table_name, presigned_files, columns)
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

    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {select_cols} "
        f"FROM parquet_scan([{parquet_files_str}], "
        f"union_by_name=TRUE, HIVE_PARTITIONING=FALSE);"
    )
    con.execute(sql)


def create_reflection_view_with_presign_retry(
        con: duckdb.DuckDBPyConnection,
        storage,
        view_name: str,
        files: List[str],
        columns: Optional[List[str]] = None,
        log_prefix: str = "",
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
            con, storage, view_name, files, columns, log_prefix
        )

    configure_httpfs_and_s3(con, files)
    tried_presign = False

    try:
        create_reflection_view(con, view_name, files, columns)
    except Exception as e:
        msg = str(e)
        if any(tok in msg for tok in (
                "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                "AccessDenied", "SignatureDoesNotMatch", "403", "400",
        )):
            logger.warning(f"{log_prefix}[duckdb.retry] presign fallback (view) for {view_name}: {msg}")
            tried_presign = True
            presigned_files = make_presigned_list(storage, files)
            configure_httpfs_and_s3(con, presigned_files)
            create_reflection_view(con, view_name, presigned_files, columns)
        else:
            raise

    return tried_presign


# =========================================================
# Query rewriting
# =========================================================

def rewrite_query_with_hashed_tables(
        original_sql: str,
        alias_to_table: Dict[str, str],
) -> str:
    """Replace table references in SQL with hashed physical table names."""
    if not alias_to_table:
        return original_sql

    try:
        parsed = sqlglot.parse_one(original_sql)
    except Exception as e:
        logger.warning(f"[duckdb] Failed to parse SQL for rewrite; using original. Error: {e}")
        return original_sql

    for table in parsed.find_all(exp.Table):
        alias_expr = table.args.get("alias")
        alias_name = None

        if isinstance(alias_expr, exp.TableAlias):
            ident = alias_expr.this
            if isinstance(ident, exp.Identifier):
                alias_name = ident.name

        if not alias_name:
            alias_name = table.name

        if alias_name in alias_to_table:
            new_physical = alias_to_table[alias_name]
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

    DuckDB Lite/Pro reuse a persistent connection, so settings applied once at
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
    connection is configured exactly like a live Lite/Pro query via
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
    # Column filter
    if rbac_view_def.allowed_columns == ["*"]:
        select_cols = "*"
    else:
        select_cols = ", ".join(
            quote_if_needed(c) for c in rbac_view_def.allowed_columns
        )

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

         * *dv_table* — a pre-materialised ``DISTINCT __rowid__`` table
           (see :class:`TombstoneCache`).  Built once and reused across
           queries, avoiding a parquet re-read per query.
         * *tombstone_def.tombstone_path* — inline ``read_parquet`` of the
           deletion-vector parquet.  Used when the cache is disabled or has
           no stable key.  This is the legacy path and is semantically
           identical to the cached one (same ``DISTINCT __rowid__`` set,
           same anti-join, same ``__dv__`` alias).

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
        f"COLUMNS(c -> c NOT IN ('{ROWID_COL}', '{TIMESTAMP_COL}'))"
    )

    tomb_path = getattr(tombstone_def, "tombstone_path", None) if tombstone_def else None
    rid = quote_if_needed(ROWID_COL)

    if dv_table:
        # Cached deletion-vector: anti-join the already-materialised table.
        # Semantically identical to the inline subquery below — the table is
        # exactly `SELECT DISTINCT __rowid__ FROM read_parquet(...)`.
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table} "
            f"ANTI JOIN {dv_table} AS __dv__ "
            f"ON {source_table}.{rid} = __dv__.{rid};"
        )
    elif tomb_path:
        escaped = escape_parquet_path(tomb_path)
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table} "
            f"ANTI JOIN (SELECT DISTINCT {rid} FROM read_parquet('{escaped}')) AS __dv__ "
            f"ON {source_table}.{rid} = __dv__.{rid};"
        )
    else:
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {live_cols} FROM {source_table};"
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
    h = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()[:16]
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
    idx = max(cache_key.rfind("/"), cache_key.rfind("\\"))
    return cache_key[:idx] if idx > 0 else cache_key


class TombstoneCache:
    """Materialises deletion-vectors as cached DuckDB tables, keyed by the
    *stable* tombstone path, with **per-table** eviction.

    Why a table and not the inline subquery?  The tombstone parquet is
    re-read on every query in the inline form.  Because the tombstone path is
    stable across pure appends (the writer carries forward the previous
    deletion-vector when a write adds no new deletes), the same
    ``DISTINCT __rowid__`` set is recomputed needlessly.  Materialising it
    once — ``CREATE TABLE dv_<hash> AS SELECT DISTINCT __rowid__ FROM
    read_parquet(path)`` — lets every subsequent tombstone view ANTI JOIN the
    table directly.  The anti-join result is bit-identical to the inline form.

    Eviction is entirely per logical table — a frequently-changing table can
    never evict a slowly-changing table's cached deletion-vector — and rests on
    two independent, table-local rules:

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

    There is no global ceiling: total residency is bounded by the number of
    tables queried within the TTL window times ``capacity`` versions each,
    reclaimed by the idle TTL or by a connection reset.

    Thread-safe: every registry mutation and the DDL it triggers is guarded
    by an internal lock.  The lock is only ever acquired *after* an engine's
    own connection lock, never before, so the two compose without deadlock.
    """

    def __init__(
            self,
            capacity: int,
            ttl_seconds: int = 0,
            *,
            time_fn: Callable[[], float] = time.monotonic,
    ):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
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
                table_name = dv_table_name(cache_key)
                rid = quote_if_needed(ROWID_COL)
                escaped = escape_parquet_path(duckdb_path)
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                    f"SELECT DISTINCT {rid} FROM read_parquet('{escaped}');"
                )
                entry = _DVCacheEntry(
                    table_name=table_name,
                    cache_key=cache_key,
                    table_id=dv_table_id(cache_key),
                )
                self._registry[cache_key] = entry

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
