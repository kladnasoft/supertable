# supertable/engine/spark_thrift.py

from __future__ import annotations

import hashlib
import logging
import os
import threading
import uuid
from typing import Dict, List, Optional

import pandas as pd

from supertable.config.defaults import logger
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
        return int(os.getenv("SUPERTABLE_SPARK_QUERY_TIMEOUT", "300"))
    except (ValueError, TypeError):
        return 300


def _spark_statement_timeout_seconds() -> int:
    """Per-statement timeout for individual Thrift cursor.execute() calls.

    Default: 120 s (2 min).  Override with SUPERTABLE_SPARK_STATEMENT_TIMEOUT.
    Applies to each CREATE VIEW / SET / user-query individually.
    """
    try:
        return int(os.getenv("SUPERTABLE_SPARK_STATEMENT_TIMEOUT", "120"))
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


def _spark_create_table_sql(
        table_name: str,
        files: List[str],
) -> str:
    """Generate Spark SQL to create a temp view from parquet files."""
    if not files:
        raise ValueError(f"No files for Spark table '{table_name}'")

    # Spark reads multiple parquet files via a path list
    escaped = [escape_parquet_path(f) for f in files]
    paths_str = ", ".join(f"'{f}'" for f in escaped)

    # Use CREATE OR REPLACE TEMPORARY VIEW with parquet datasource
    return (
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} "
        f"USING parquet "
        f"OPTIONS (path '{escaped[0]}', mergeSchema 'true')"
    ) if len(files) == 1 else (
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS "
        f"SELECT * FROM parquet.`{{}}`"
    )


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

    batch_size = int(os.getenv("SUPERTABLE_SPARK_BATCH_SIZE", "50"))
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


def _spark_rewrite_query(
        original_sql: str,
        alias_to_table: Dict[str, str],
) -> str:
    """Rewrite table references in SQL — delegates to shared helper."""
    return rewrite_query_with_hashed_tables(original_sql, alias_to_table)


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
            socket_timeout = int(os.getenv("SUPERTABLE_SPARK_CONNECT_TIMEOUT", "30"))
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

            # 5b. Create dedup views if dedup-on-read is configured
            dedup_views = getattr(reflection, "dedup_views", None) or {}
            if dedup_views:
                if not rbac_views:
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
                    f"during RBAC/dedup view creation"
                )

            # 6. Rewrite and execute
            executing_query = _spark_rewrite_query(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[spark.thrift] SQL: {executing_query}")

            cursor.execute(executing_query)

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