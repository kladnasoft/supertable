# supertable/engine/spark_thrift.py

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import uuid
from typing import Dict, List, Optional

import pandas as pd

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection, RbacViewDef, DedupViewDef
from supertable.redis_catalog import RedisCatalog

from supertable.engine.engine_common import (
    quote_if_needed,
    rewrite_query_with_hashed_tables,
    escape_parquet_path,
)

from urllib.parse import urlparse

# Suppress verbose INFO logging from PyHive and Thrift libraries.
# PyHive logs every SQL statement (CREATE VIEW, DROP VIEW, SET, etc.)
# at INFO level, which floods the application log.
for _lib in ("pyhive", "pyhive.hive", "TCLIService", "thrift", "thrift_sasl"):
    logging.getLogger(_lib).setLevel(logging.WARNING)


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
      2. http(s)://endpoint/bucket/key?presign_params → s3a://bucket/key
      3. s3a://… or local path    → returned as-is
    """
    if file_path.startswith("s3://"):
        return "s3a://" + file_path[5:]

    if file_path.startswith("s3a://"):
        return file_path

    if file_path.startswith("http://") or file_path.startswith("https://"):
        parsed = urlparse(file_path)
        # path is /bucket/key — strip the leading slash, split into bucket + key
        path = parsed.path.lstrip("/")
        if "/" in path:
            return f"s3a://{path}"
        # Degenerate case: only bucket, no key — return as-is
        return file_path

    return file_path


# =========================================================
# Spark SQL helpers
# =========================================================

def _spark_table_name(super_name: str, simple_name: str, version: int) -> str:
    """Generate a deterministic Spark temp table name."""
    key = f"{super_name}_{simple_name}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"spark_{digest}_v{version}"


def _spark_create_parquet_view(cursor, table_name: str, files: List[str]) -> List[str]:
    """Register parquet files as a Spark temp view via Thrift.

    For a single file, uses ``USING parquet OPTIONS (path ...)`` directly.
    For multiple files, batches them: each batch creates individual temp
    views via ``USING parquet OPTIONS``, unions them into a batch view.
    Finally all batch views are unioned into the target view.

    **Important**: intermediate views (part views and batch views) are NOT
    dropped here.  In Spark SQL, ``CREATE VIEW X AS SELECT * FROM Y`` stores
    a lazy reference to Y — dropping Y invalidates X.  Intermediate views
    must stay alive until the final query completes.

    Returns a list of all intermediate view names created, so the caller
    can drop them during cleanup.
    """
    intermediate_views: List[str] = []

    if len(files) == 1:
        sql = (
            f"CREATE OR REPLACE TEMPORARY VIEW {table_name} "
            f"USING parquet "
            f"OPTIONS (path '{escape_parquet_path(files[0])}')"
        )
        cursor.execute(sql)
        return intermediate_views

    batch_size = settings.SUPERTABLE_SPARK_BATCH_SIZE
    batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    all_batch_views: List[str] = []

    for batch_idx, batch in enumerate(batches):
        # Create individual file views within this batch
        part_views: List[str] = []
        for file_idx, f in enumerate(batch):
            part_name = f"__{table_name}_b{batch_idx}p{file_idx}__"
            cursor.execute(
                f"CREATE OR REPLACE TEMPORARY VIEW {part_name} "
                f"USING parquet "
                f"OPTIONS (path '{escape_parquet_path(f)}')"
            )
            part_views.append(part_name)
            intermediate_views.append(part_name)

        # Union all parts into one batch view
        batch_view = f"__{table_name}_batch{batch_idx}__"
        union_sql = " UNION ALL ".join(f"SELECT * FROM {p}" for p in part_views)
        cursor.execute(
            f"CREATE OR REPLACE TEMPORARY VIEW {batch_view} AS {union_sql}"
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

    cursor.execute(
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS {union_sql}"
    )

    # Do NOT drop batch views — the target view references them lazily.
    # Return them so the caller drops them after the query completes.
    return intermediate_views


def _spark_create_rbac_view(
        cursor,
        base_table_name: str,
        view_name: str,
        rbac_view_def: RbacViewDef,
) -> None:
    """Create an RBAC-filtered view on top of a Spark table."""
    if rbac_view_def.allowed_columns == ["*"]:
        select_cols = "*"
    else:
        select_cols = ", ".join(
            f"`{c}`" for c in rbac_view_def.allowed_columns
        )

    where_sql = ""
    if rbac_view_def.where_clause:
        where_sql = f" WHERE {rbac_view_def.where_clause}"

    sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
        f"SELECT {select_cols} FROM {base_table_name}{where_sql}"
    )
    cursor.execute(sql)


def _spark_create_dedup_view(
        cursor,
        source_table: str,
        view_name: str,
        dedup_def: DedupViewDef,
) -> None:
    """Create a dedup-on-read view on top of a Spark table or RBAC view.

    Uses ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <ts> DESC) to keep
    only the latest row per primary key.  Only visible_columns are projected
    so that internal columns (__timestamp__, extra PKs, __rn__) are hidden.
    """
    pk_cols = ", ".join(f"`{c}`" for c in dedup_def.primary_keys)
    order_col = f"`{dedup_def.order_column}`"

    visible = dedup_def.visible_columns
    if not visible or visible == ["*"]:
        # Spark has no EXCLUDE syntax.  Query the source schema to build
        # an explicit column list that omits __rn__ and __timestamp__.
        hidden = {"__rn__", dedup_def.order_column}
        try:
            cursor.execute(f"DESCRIBE {source_table}")
            src_cols = [row[0] for row in cursor.fetchall()]
            keep = [c for c in src_cols if c not in hidden]
        except Exception:
            # Fallback: can't introspect — expose everything including __timestamp__.
            # __rn__ is still filtered via the WHERE clause so it won't appear,
            # but __timestamp__ will be visible.  Acceptable degraded behaviour.
            keep = []

        if keep:
            outer_select = ", ".join(f"sub.`{c}`" for c in keep)
        else:
            outer_select = "sub.*"
    else:
        outer_select = ", ".join(f"sub.`{c}`" for c in visible)

    sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
        f"SELECT {outer_select} FROM ("
        f"SELECT *, ROW_NUMBER() OVER ("
        f"PARTITION BY {pk_cols} ORDER BY {order_col} DESC"
        f") AS __rn__ FROM {source_table}"
        f") sub WHERE sub.__rn__ = 1"
    )
    cursor.execute(sql)


def _spark_create_tombstone_view(
        cursor,
        source_table: str,
        view_name: str,
        tombstone_def,
) -> None:
    """Create a tombstone-filtering view on top of a Spark table or RBAC view.

    Uses NOT EXISTS with a VALUES subquery to exclude soft-deleted keys.
    The tombstone list is small (bounded by compaction threshold) so the
    VALUES list fits comfortably in Spark's query plan.
    """
    pk = tombstone_def.primary_keys
    deleted = tombstone_def.deleted_keys
    if not pk or not deleted:
        # No tombstones — passthrough
        cursor.execute(
            f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
            f"SELECT * FROM {source_table}"
        )
        return

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

    # Spark supports VALUES ... AS alias(col1, col2)
    pk_aliases = ", ".join(f"`{c}`" for c in pk)
    conditions = " AND ".join(
        f"{source_table}.`{c}` = __tombstones__.`{c}`" for c in pk
    )

    sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS "
        f"SELECT * FROM {source_table} "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM VALUES {values_sql} AS __tombstones__({pk_aliases}) "
        f"WHERE {conditions}"
        f")"
    )
    cursor.execute(sql)


def _spark_rewrite_query(
        original_sql: str,
        alias_to_table: Dict[str, str],
) -> str:
    """Rewrite table references and transpile SQL to Spark dialect.

    Two-step process:
    1. Delegate to ``rewrite_query_with_hashed_tables`` to replace table
       references with physical hashed names (shared with DuckDB path).
    2. Transpile the resulting SQL from DuckDB dialect to Spark dialect
       via sqlglot.  This converts identifier quoting from double-quotes
       (``"col"``) to backticks (`` `col` ``), and adapts other syntax
       differences (e.g. INTERVAL literals, type names).

    Falls back to a simple double-quote → backtick replacement if sqlglot
    transpilation fails for any reason.
    """
    rewritten = rewrite_query_with_hashed_tables(original_sql, alias_to_table)

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
    except Exception as e:
        logger.debug(f"[spark.thrift] sqlglot transpile failed, using quote fallback: {e}")

    # Fallback: replace double-quoted identifiers with backtick-quoted ones.
    # This handles the common case where sqlglot can't parse the rewritten SQL
    # but the only issue is identifier quoting style.
    return _double_quotes_to_backticks(rewritten)


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

def _configure_spark_s3(cursor, cluster: Optional[Dict] = None) -> None:
    """Set S3/MinIO configuration on the Spark Thrift session.

    If the cluster registration includes s3_endpoint, s3_access_key, or
    s3_secret_key, those values are SET on the session.

    **Important**: when the cluster dict does NOT include these keys, the
    function no longer falls back to host-side environment variables
    (e.g. ``STORAGE_ENDPOINT_URL``).  The Spark Thrift Server is typically
    launched with ``--conf spark.hadoop.fs.s3a.*`` set to Docker-internal
    addresses (e.g. ``http://minio:9000``).  Overriding them with the
    host-side ``localhost:9000`` breaks S3 access from within the container.

    To explicitly override S3 settings, include them in the cluster
    registration dict (``s3_endpoint``, ``s3_access_key``, ``s3_secret_key``,
    ``s3_region``).
    """
    _cluster = cluster or {}

    # Only SET values that are explicitly provided in the cluster config.
    # Falling back to host env vars (STORAGE_ENDPOINT_URL etc.) is wrong
    # because those point to localhost which is unreachable from Docker.
    settings = []

    endpoint = _cluster.get("s3_endpoint")
    if endpoint:
        settings.append(("spark.hadoop.fs.s3a.endpoint", endpoint))

    access_key = _cluster.get("s3_access_key")
    if access_key:
        settings.append(("spark.hadoop.fs.s3a.access.key", access_key))

    secret_key = _cluster.get("s3_secret_key")
    if secret_key:
        settings.append(("spark.hadoop.fs.s3a.secret.key", secret_key))

    region = _cluster.get("s3_region")
    if region:
        settings.append(("spark.hadoop.fs.s3a.endpoint.region", region))

    use_ssl = _cluster.get("s3_use_ssl")
    if use_ssl is not None:
        settings.append(("spark.hadoop.fs.s3a.connection.ssl.enabled", str(use_ssl).lower()))

    path_style = _cluster.get("s3_path_style")
    if path_style is not None:
        settings.append(("spark.hadoop.fs.s3a.path.style.access", str(path_style).lower()))

    # Always ensure the S3A filesystem impl is set (harmless if already set).
    settings.append(("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))

    if not settings:
        logger.debug("[spark.thrift] no S3 overrides in cluster config; using spark-submit defaults")

    for key, value in settings:
        try:
            sql = f"SET {key}={value}"
            logger.debug(f"[spark.thrift] {sql}")
            cursor.execute(sql)
        except Exception as e:
            logger.debug(f"[spark.thrift] SET {key} failed: {e}")


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
            f"closing connection: {sql[:120]}"
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
        self.catalog = RedisCatalog()

    def _get_connection(self, cluster: Dict) -> object:
        """Create a PyHive connection to the Thrift Server."""
        try:
            from pyhive import hive
        except ImportError as e:
            raise RuntimeError(
                "PyHive is required for SPARK engine. "
                "Install it with: pip install pyhive[hive]"
            ) from e

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

    def _select_cluster(self, job_bytes: int, force: bool = False) -> Dict:
        """Select the best cluster from Redis, or raise if none available."""
        cluster = self.catalog.select_spark_cluster(self.organization, job_bytes, force=force)
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

    def execute(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
            force: bool = False,
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
        7. Fetch results as pandas DataFrame
        8. Clean up temp views

        The entire flow is guarded by SUPERTABLE_SPARK_QUERY_TIMEOUT (default 300 s).
        If the timeout fires the connection is forcibly closed, which unblocks any
        pending Thrift RPC and raises a RuntimeError to the caller.
        """
        query_timeout = _spark_timeout_seconds()
        stmt_timeout = _spark_statement_timeout_seconds()

        # 1. Select cluster
        cluster = self._select_cluster(reflection.reflection_bytes, force=force)
        timer_capture("CONNECTING")

        conn = None
        cursor = None
        created_views: List[str] = []
        created_tables: List[str] = []

        # Watchdog: if the overall timeout fires we forcibly close the
        # connection from a background thread. This causes any blocked
        # cursor.execute / fetchall to raise an exception immediately.
        _timed_out = threading.Event()
        _conn_ref: List = []  # mutable container so watchdog can see conn

        def _watchdog():
            if not _timed_out.wait(query_timeout):
                # Timeout expired and nobody cancelled us
                _timed_out.set()
                logger.error(
                    f"{log_prefix}[spark.thrift] query timeout after {query_timeout}s — "
                    f"forcibly closing connection"
                )
                for c in _conn_ref:
                    try:
                        c.close()
                    except Exception:
                        pass

        watchdog = threading.Thread(target=_watchdog, daemon=True, name="spark-timeout")
        watchdog.start()

        try:
            # 2. Connect
            conn = self._get_connection(cluster)
            _conn_ref.append(conn)
            cursor = conn.cursor()

            # 3. Configure S3
            _configure_spark_s3(cursor, cluster)

            # 3a. Work around Spark's inability to read TIMESTAMP(NANOS, false)
            #     parquet columns written by DuckDB.  DuckDB writes
            #     __timestamp__ and other timestamp columns as INT64
            #     nanosecond-precision non-UTC-adjusted timestamps.
            #     Spark 3.x rejects this type by default.
            #
            #     spark.sql.legacy.parquet.nanosAsLong=true  (Spark 3.4+)
            #       → reads nanos INT64 as LongType instead of crashing.
            #     spark.sql.parquet.inferTimestampNTZ.enabled=false
            #       → disables NTZ timestamp inference, falls back to
            #         treating all timestamps as UTC-adjusted.
            #     spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
            #       → if Spark writes parquet, use micros (harmless if read-only).
            _timestamp_workarounds = [
                ("spark.sql.legacy.parquet.nanosAsLong", "true"),
                ("spark.sql.parquet.inferTimestampNTZ.enabled", "false"),
                ("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"),
            ]
            for _tw_key, _tw_val in _timestamp_workarounds:
                try:
                    cursor.execute(f"SET {_tw_key}={_tw_val}")
                except Exception as _tw_err:
                    logger.debug(f"{log_prefix}[spark.thrift] SET {_tw_key} failed (non-fatal): {_tw_err}")

            # 4. Register reflection tables
            snapshots_by_key = {
                (sup.super_name, sup.simple_name): sup
                for sup in reflection.supers
            }
            table_defs = parser.get_table_tuples()
            alias_to_table_name: Dict[str, str] = {}

            for td in table_defs:
                key = (td.super_name, td.simple_name)
                sup = snapshots_by_key.get(key)
                if not sup:
                    continue

                table_name = _spark_table_name(
                    sup.super_name, sup.simple_name, sup.simple_version,
                )

                # Convert to s3a:// paths for Spark (handles s3://, presigned HTTP URLs, etc.)
                files = [_to_s3a_path(f) for f in sup.files]

                logger.debug(
                    f"{log_prefix}[spark.thrift] creating view {table_name} "
                    f"({len(files)} file(s))"
                )
                intermediates = _spark_create_parquet_view(cursor, table_name, files)
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

            # 4a. Wrap parquet views to CAST nanosecond-epoch BIGINT columns
            #     back to TIMESTAMP.  When nanosAsLong=true, Spark reads
            #     TIMESTAMP(NANOS, false) parquet columns as BIGINT (epoch
            #     nanoseconds).  Downstream queries expect DATE/TIMESTAMP
            #     semantics (e.g. WHERE stat_date >= CURRENT_DATE - INTERVAL
            #     '7' DAYS), so we create a wrapper view that converts them.
            #     The wrapper replaces the original view name in alias_to_table_name
            #     so all downstream logic (RBAC, dedup, tombstone, user query)
            #     sees proper TIMESTAMP columns.
            for alias, table_name in list(alias_to_table_name.items()):
                try:
                    cursor.execute(f"DESCRIBE {table_name}")
                    desc_rows = cursor.fetchall()

                    # Identify BIGINT columns that look like nanosecond timestamps.
                    # Heuristic: column name contains 'date', 'time', 'timestamp',
                    # '_at', '_ts', or is exactly '__timestamp__'.
                    _ts_patterns = ('date', 'time', 'timestamp', '_at', '_ts')
                    cast_cols = []
                    for row in desc_rows:
                        col_name = str(row[0])
                        col_type = str(row[1]).strip().upper()
                        if col_type != 'BIGINT':
                            continue
                        col_lower = col_name.lower()
                        if col_lower == '__timestamp__' or any(p in col_lower for p in _ts_patterns):
                            cast_cols.append(col_name)

                    if cast_cols:
                        # Build SELECT with CASTs for timestamp columns, pass-through for others
                        select_parts = []
                        for row in desc_rows:
                            col_name = str(row[0])
                            if col_name in cast_cols:
                                # nanos → micros → TIMESTAMP
                                select_parts.append(
                                    f"CAST(`{col_name}` / 1000000 AS TIMESTAMP) AS `{col_name}`"
                                )
                            else:
                                select_parts.append(f"`{col_name}`")

                        wrapper_name = f"__{table_name}_tscast__"
                        wrapper_sql = (
                            f"CREATE OR REPLACE TEMPORARY VIEW {wrapper_name} AS "
                            f"SELECT {', '.join(select_parts)} FROM {table_name}"
                        )
                        cursor.execute(wrapper_sql)
                        created_views.append(wrapper_name)
                        alias_to_table_name[alias] = wrapper_name

                        logger.debug(
                            f"{log_prefix}[spark.thrift] CAST wrapper for {table_name}: "
                            f"converted {len(cast_cols)} column(s): {cast_cols}"
                        )
                except Exception as cast_err:
                    # Non-fatal: if DESCRIBE or CAST fails, the original view
                    # stays in place.  The query may still fail with a type
                    # mismatch, but at least we don't break the happy path.
                    logger.debug(
                        f"{log_prefix}[spark.thrift] timestamp CAST wrapper "
                        f"failed for {table_name} (non-fatal): {cast_err}"
                    )

            logger.info(
                f"{log_prefix}[spark.thrift] registered {len(alias_to_table_name)} table(s), "
                f"{len(created_views)} intermediate views"
            )

            # 5. Create RBAC views
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            query_alias_to_name = dict(alias_to_table_name)

            if rbac_views:
                query_suffix = uuid.uuid4().hex[:8]
                for alias, table_name in alias_to_table_name.items():
                    view_def = rbac_views.get(alias)
                    if view_def:
                        view_name = f"rbac_{table_name}_{query_suffix}"
                        _spark_create_rbac_view(cursor, table_name, view_name, view_def)
                        created_views.append(view_name)
                        query_alias_to_name[alias] = view_name

            # 5b. Create tombstone views if soft-deleted keys exist
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            if tombstone_views:
                if not rbac_views:
                    query_suffix = uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    tomb_def = tombstone_views.get(alias)
                    if tomb_def and tomb_def.deleted_keys:
                        source = query_alias_to_name[alias]
                        tomb_view = f"tomb_{source}_{query_suffix}"
                        _spark_create_tombstone_view(cursor, source, tomb_view, tomb_def)
                        created_views.append(tomb_view)
                        query_alias_to_name[alias] = tomb_view

            # 5c. Create dedup views if dedup-on-read is configured
            dedup_views = getattr(reflection, "dedup_views", None) or {}
            if dedup_views:
                if not rbac_views and not tombstone_views:
                    query_suffix = uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    dedup_def = dedup_views.get(alias)
                    if dedup_def:
                        source = query_alias_to_name[alias]
                        dedup_view = f"dedup_{source}_{query_suffix}"
                        _spark_create_dedup_view(cursor, source, dedup_view, dedup_def)
                        created_views.append(dedup_view)
                        query_alias_to_name[alias] = dedup_view

            if _timed_out.is_set():
                raise RuntimeError(
                    f"Spark query timed out after {query_timeout}s "
                    f"during RBAC/tombstone/dedup view creation"
                )

            # 6. Rewrite and execute
            executing_query = _spark_rewrite_query(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[spark.thrift] SQL: {executing_query}")

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
                current_lines = []
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
                    "parsed_logical_plan": plan_sections.get("parsed_logical_plan", ""),
                    "analyzed_logical_plan": plan_sections.get("analyzed_logical_plan", ""),
                    "optimized_logical_plan": plan_sections.get("optimized_logical_plan", ""),
                    "physical_plan": plan_sections.get("physical_plan", ""),
                }

                # Write to the local plan path so plan_extender finds it.
                if query_manager and query_manager.query_plan_path:
                    try:
                        os.makedirs(os.path.dirname(query_manager.query_plan_path), exist_ok=True)
                        with open(query_manager.query_plan_path, "w", encoding="utf-8") as fh:
                            json.dump(spark_plan, fh, ensure_ascii=False)
                        logger.debug(
                            f"{log_prefix}[spark.thrift] query plan saved to "
                            f"{query_manager.query_plan_path}"
                        )
                    except Exception as plan_write_err:
                        logger.debug(
                            f"{log_prefix}[spark.thrift] failed to write plan JSON: "
                            f"{plan_write_err}"
                        )
            except Exception as explain_err:
                # EXPLAIN failure must never block the actual query.
                logger.debug(
                    f"{log_prefix}[spark.thrift] EXPLAIN EXTENDED failed (non-fatal): "
                    f"{explain_err}"
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
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()

            result = pd.DataFrame(rows, columns=columns) if columns else pd.DataFrame()

            logger.info(
                f"{log_prefix}[spark.thrift] query complete: "
                f"{len(rows)} rows, {len(columns)} cols"
            )

            return result

        except Exception as e:
            if _timed_out.is_set():
                raise RuntimeError(
                    f"Spark query timed out after {query_timeout}s"
                ) from e
            raise

        finally:
            # Cancel the watchdog so it doesn't fire after we're done.
            _timed_out.set()

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