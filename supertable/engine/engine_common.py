# route: supertable.engine.engine_common

from __future__ import annotations

import hashlib
import os
from typing import Dict, List, Optional
from urllib.parse import urlparse

import duckdb
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.config.homedir import get_app_home


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


def _derive_thread_count(memory_limit_str: str, fallback: int = 2) -> int:
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
    multiplier = settings.SUPERTABLE_DUCKDB_IO_MULTIPLIER

    io_threads = cpu_count * multiplier
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

    # External file cache (DuckDB >= 1.3) — caches remote data blocks on local
    # disk so repeated queries do not re-download the same row groups.
    # Only enabled when SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE is set.
    cache_size = settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE
    if cache_size:
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
    effective_memory_limit = settings.SUPERTABLE_DUCKDB_MEMORY_LIMIT or memory_limit
    con.execute(f"PRAGMA memory_limit='{effective_memory_limit}';")

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
# Dedup-on-read view creation
# =========================================================

def create_dedup_view(
        con: duckdb.DuckDBPyConnection,
        source_table: str,
        view_name: str,
        dedup_def,
) -> None:
    """
    Create a dedup view on top of a reflection table (or RBAC view).

    The view uses ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <ts> DESC)
    to keep only the latest row per primary key combination.

    Only ``visible_columns`` are projected in the outer SELECT so that
    internal columns (__timestamp__, extra PKs, __rn__) are hidden from
    the user query.  If visible_columns is empty or ["*"], all columns
    from the source are exposed except __rn__.

    Args:
        con: DuckDB connection
        source_table: the underlying table or view to dedup
        view_name: the view name to create
        dedup_def: DedupViewDef with primary_keys, order_column, visible_columns
    """
    pk_cols = ", ".join(quote_if_needed(c) for c in dedup_def.primary_keys)
    order_col = quote_if_needed(dedup_def.order_column)

    # Determine which columns to expose
    visible = dedup_def.visible_columns
    if not visible or visible == ["*"]:
        # Expose all source columns except internal dedup columns.
        # DuckDB EXCLUDE removes them from SELECT *.
        exclude_cols = "__rn__, " + quote_if_needed(dedup_def.order_column)
        outer_select = f"* EXCLUDE ({exclude_cols})"
    else:
        outer_select = ", ".join(quote_if_needed(c) for c in visible)

    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {outer_select} FROM ("
        f"SELECT *, ROW_NUMBER() OVER ("
        f"PARTITION BY {pk_cols} ORDER BY {order_col} DESC"
        f") AS __rn__ FROM {source_table}"
        f") sub WHERE __rn__ = 1;"
    )
    con.execute(sql)


# =========================================================
# Tombstone (soft-delete) view creation
# =========================================================

def create_tombstone_view(
        con: duckdb.DuckDBPyConnection,
        source_table: str,
        view_name: str,
        tombstone_def,
) -> None:
    """
    Create a filtering view that excludes soft-deleted (tombstoned) rows.

    The view anti-joins the source table against a VALUES list of
    tombstoned composite keys.  Because the tombstone list is bounded
    by the compaction threshold (typically ≤1000 keys), the VALUES
    list is small and DuckDB handles it efficiently in memory.

    This view should be inserted in the chain AFTER RBAC filtering
    and BEFORE the dedup view so that tombstoned rows are excluded
    before dedup's ROW_NUMBER window runs.

    Args:
        con: DuckDB connection
        source_table: the underlying table or view
        view_name: the view name to create
        tombstone_def: TombstoneDef with primary_keys and deleted_keys
    """
    pk = tombstone_def.primary_keys
    deleted = tombstone_def.deleted_keys
    if not pk or not deleted:
        # No tombstones — create a passthrough view
        con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM {source_table};"
        )
        return

    # Build VALUES rows: each key tuple becomes a row
    # Example: VALUES (1, 'US'), (2, 'EU')
    def _sql_literal(val) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, str):
            return "'" + val.replace("'", "''") + "'"
        return str(val)

    value_rows = []
    for key_tuple in deleted:
        vals = ", ".join(_sql_literal(v) for v in key_tuple)
        value_rows.append(f"({vals})")
    values_sql = ", ".join(value_rows)

    # Column aliases for the VALUES table
    pk_aliases = ", ".join(quote_if_needed(c) for c in pk)

    # Anti-join via NOT EXISTS (handles NULLs correctly, no column name collisions)
    conditions = " AND ".join(
        f"{source_table}.{quote_if_needed(c)} = __tombstones__.{quote_if_needed(c)}"
        for c in pk
    )

    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM {source_table} "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM (VALUES {values_sql}) AS __tombstones__({pk_aliases}) "
        f"WHERE {conditions}"
        f");"
    )
    con.execute(sql)

# (ParquetMetadataCache removed: was write-only dead weight.
#  DuckDB's built-in enable_http_metadata_cache on the persistent connection
#  already handles parquet footer caching at the connection level.
#  The external file cache (enable_external_file_cache) is now configured
#  inside configure_httpfs_and_s3, after LOAD httpfs, where the setting
#  actually exists.)
