# route: supertable.engine.engine_common

from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, List, Optional
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

    # Load httpfs; install only when not already available.
    try:
        con.execute("LOAD httpfs;")
    except Exception:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

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

    # External file cache — an in-memory cache of remote data blocks so
    # repeated queries do not re-download the same row groups.  It is only
    # enabled when a size cap can be enforced: DuckDB builds without
    # external_file_cache_max_size (e.g. 1.5.x) cannot bound it, and an
    # uncapped cache grows to memory_limit on the persistent connection.
    # Without an enforceable cap we keep it OFF to protect memory.
    cache_size = settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE
    can_cap = "external_file_cache_max_size" in supported
    if cache_size and can_cap:
        set_if_supported("enable_external_file_cache", "true")
        set_if_supported("external_file_cache_max_size", f"'{cache_size}'")
        cache_dir_raw = settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
        if not cache_dir_raw:
            # Derive from SUPERTABLE_HOME — single env var controls all paths.
            cache_dir_raw = os.path.join(get_app_home(), "duckdb_cache")
        # Expand ~ so DuckDB receives an absolute path.
        cache_dir = os.path.expanduser(cache_dir_raw)
        os.makedirs(cache_dir, exist_ok=True)
        set_if_supported("external_file_cache_directory", f"'{cache_dir}'")
        logger.info(
            "[duckdb.cache] external file cache enabled"
            + (f" size={cache_size}" if cache_size else "")
            + f", dir={cache_dir}"
        )
    else:
        # Uncappable (or disabled via empty size) — turn it off explicitly so
        # the DuckDB 1.5.x default-on cache cannot accumulate in memory.
        set_if_supported("enable_external_file_cache", "false")
        if cache_size and not can_cap:
            logger.info(
                "[duckdb.cache] external file cache disabled: this DuckDB build "
                "cannot cap it (no external_file_cache_max_size)"
            )


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
    select_cols = "*" if not columns else ", ".join(
        quote_if_needed(c) for c in columns if c and c.strip()
    )

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
    select_cols = "*" if not columns else ", ".join(
        quote_if_needed(c) for c in columns if c and c.strip()
    )

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
    """True when this DuckDB build can bound the external file cache size.

    DuckDB 1.5.x enables ``enable_external_file_cache`` by default but does
    not expose ``external_file_cache_max_size``.  An enabled-but-uncapped
    cache is held in memory (not on disk) and grows to ``memory_limit`` on
    the long-lived persistent connection — a sustained-memory / OOM hazard.
    When this returns False the cache is disabled outright rather than left
    running unbounded.
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

    # External file cache baseline.  DuckDB 1.5.x turns the cache ON by
    # default but cannot cap it, so an uncapped in-memory cache accumulates
    # remote data up to memory_limit on the persistent connection.  Disable
    # it here when uncappable; configure_httpfs_and_s3 / apply_runtime_pragmas
    # re-enable it (capped) only on builds that support a size cap.
    if not _external_file_cache_cappable(con):
        try:
            con.execute("SET enable_external_file_cache=false;")
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

    # External file cache: only run it when the size cap is enforceable.
    # On DuckDB builds without external_file_cache_max_size (e.g. 1.5.x) an
    # enabled cache is in-memory and unbounded — it grows to memory_limit on
    # the persistent connection — so we disable it instead of running uncapped.
    cache_size = normalize_memory_size(cfg.duckdb_external_cache_size, default="")
    if cache_size and _external_file_cache_cappable(con):
        try:
            con.execute("SET enable_external_file_cache=true;")
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
             "cannot be capped, so it is disabled to stay memory-safe"),
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
            add("cache", "External file cache", "fail",
                "Cache is ON but this build cannot cap it — it grows to the memory "
                "limit and causes OOM", "on · uncapped")
        elif efc_on and cappable:
            add("cache", "External file cache", "ok",
                f"Cache is ON and capped at {cache_cfg or 'the configured size'}",
                "on · capped")
        else:
            add("cache", "External file cache", "ok",
                "Cache is OFF — memory-safe; remote files are re-fetched per query "
                "(set a Disk cache size on a cap-capable build to speed up repeats)",
                "off")
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
) -> None:
    """
    Create a view that hides the system columns and drops tombstoned rows.

    Two responsibilities, applied to every reflected table:

      1. **Strip system columns** — ``__rowid__`` and ``__timestamp__`` are
         removed from the projection via a static DuckDB
         ``* EXCLUDE (__rowid__, __timestamp__)`` so they never leak into
         user query results.  Every written file carries both columns, so
         the exclude list is fixed.
      2. **Apply the deletion-vector** — when *tombstone_def* carries a
         ``tombstone_path``, rows whose ``__rowid__`` appears in the
         deletion-vector parquet are removed with an ANTI JOIN *before*
         the columns are stripped.

    This view sits directly on top of the reflection table (before RBAC),
    so the anti-join still has ``__rowid__`` available and RBAC never sees
    the system columns.

    Args:
        con: DuckDB connection
        source_table: the underlying reflection table or view
        view_name: the view name to create
        tombstone_def: TombstoneDef with ``tombstone_path`` (or None)
    """
    exclude = (
        f" EXCLUDE ({quote_if_needed(ROWID_COL)}, {quote_if_needed(TIMESTAMP_COL)})"
    )

    tomb_path = getattr(tombstone_def, "tombstone_path", None) if tombstone_def else None

    if tomb_path:
        rid = quote_if_needed(ROWID_COL)
        escaped = escape_parquet_path(tomb_path)
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {source_table}.*{exclude} FROM {source_table} "
            f"ANTI JOIN (SELECT DISTINCT {rid} FROM read_parquet('{escaped}')) AS __dv__ "
            f"ON {source_table}.{rid} = __dv__.{rid};"
        )
    else:
        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT *{exclude} FROM {source_table};"
        )
    con.execute(sql)

# (ParquetMetadataCache removed: was write-only dead weight.
#  DuckDB's built-in enable_http_metadata_cache on the persistent connection
#  already handles parquet footer caching at the connection level.
#  The external file cache (enable_external_file_cache) is now configured
#  inside configure_httpfs_and_s3, after LOAD httpfs, where the setting
#  actually exists.)
