# supertable/engine/spark_thrift.py

from __future__ import annotations

import hashlib
import os
import uuid
from typing import Dict, List, Optional

import pandas as pd

from supertable.config.defaults import logger
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection, RbacViewDef
from supertable.redis_catalog import RedisCatalog

from supertable.engine.engine_common import (
    quote_if_needed,
    rewrite_query_with_hashed_tables,
    escape_parquet_path,
)


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


def _spark_create_parquet_view(cursor, table_name: str, files: List[str]) -> None:
    """Register parquet files as a Spark temp view via Thrift."""
    if len(files) == 1:
        sql = (
            f"CREATE OR REPLACE TEMPORARY VIEW {table_name} "
            f"USING parquet "
            f"OPTIONS (path '{escape_parquet_path(files[0])}')"
        )
        cursor.execute(sql)
    else:
        # Multiple files: union them
        # Spark can read a directory or a list via spark.read.parquet(...)
        # Through Thrift SQL, the most reliable approach is a UNION of single-file views
        # or using a common parent path. We use the SET + path approach.
        parts = []
        for i, f in enumerate(files):
            part_name = f"__{table_name}_part{i}__"
            cursor.execute(
                f"CREATE OR REPLACE TEMPORARY VIEW {part_name} "
                f"USING parquet "
                f"OPTIONS (path '{escape_parquet_path(f)}')"
            )
            parts.append(part_name)

        union_sql = " UNION ALL ".join(f"SELECT * FROM {p}" for p in parts)
        cursor.execute(
            f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS {union_sql}"
        )

        # Drop part views
        for p in parts:
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {p}")
            except Exception:
                pass


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


def _spark_rewrite_query(
        original_sql: str,
        alias_to_table: Dict[str, str],
) -> str:
    """Rewrite table references in SQL — delegates to shared helper."""
    return rewrite_query_with_hashed_tables(original_sql, alias_to_table)


# =========================================================
# S3/MinIO configuration for Spark
# =========================================================

def _configure_spark_s3(cursor) -> None:
    """Set S3/MinIO configuration on the Spark Thrift session."""
    endpoint = os.getenv("STORAGE_ENDPOINT_URL")
    access_key = os.getenv("STORAGE_ACCESS_KEY")
    secret_key = os.getenv("STORAGE_SECRET_KEY")
    region = os.getenv("STORAGE_REGION", "us-east-1")
    use_ssl = os.getenv("STORAGE_USE_SSL", "").lower() in ("1", "true", "yes", "on")
    path_style = os.getenv("STORAGE_FORCE_PATH_STYLE", "true").lower() in ("1", "true", "yes", "on")

    settings = []
    if endpoint:
        settings.append(("spark.hadoop.fs.s3a.endpoint", endpoint))
    if access_key:
        settings.append(("spark.hadoop.fs.s3a.access.key", access_key))
    if secret_key:
        settings.append(("spark.hadoop.fs.s3a.secret.key", secret_key))
    if region:
        settings.append(("spark.hadoop.fs.s3a.endpoint.region", region))

    settings.append(("spark.hadoop.fs.s3a.connection.ssl.enabled", str(use_ssl).lower()))
    settings.append(("spark.hadoop.fs.s3a.path.style.access", str(path_style).lower()))
    settings.append(("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))

    for key, value in settings:
        try:
            cursor.execute(f"SET {key}={value}")
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

    def __init__(self, storage: Optional[object] = None):
        self.storage = storage
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
        auth = cluster.get("auth", "NOSASL")
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

        logger.info(f"[spark.thrift] connecting to {host}:{port} (auth={auth})")
        return hive.connect(**connect_kwargs)

    def _select_cluster(self, job_bytes: int, force: bool = False) -> Dict:
        """Select the best cluster from Redis, or raise if none available."""
        cluster = self.catalog.select_spark_cluster(job_bytes, force=force)
        if not cluster:
            raise RuntimeError(
                f"No active Spark cluster available for job size {job_bytes} bytes. "
                "Register a cluster via RedisCatalog.register_spark_cluster()."
            )
        logger.info(
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
        """
        # 1. Select cluster
        cluster = self._select_cluster(reflection.reflection_bytes, force=force)
        timer_capture("CONNECTING")

        conn = None
        cursor = None
        created_views: List[str] = []
        created_tables: List[str] = []

        try:
            # 2. Connect
            conn = self._get_connection(cluster)
            cursor = conn.cursor()

            # 3. Configure S3
            _configure_spark_s3(cursor)

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

                # Convert s3:// to s3a:// for Spark
                files = [f.replace("s3://", "s3a://", 1) if f.startswith("s3://") else f for f in sup.files]

                _spark_create_parquet_view(cursor, table_name, files)
                created_tables.append(table_name)
                alias_to_table_name[td.alias] = table_name

            timer_capture("CREATING_REFLECTION")

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

            # 6. Rewrite and execute
            executing_query = _spark_rewrite_query(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[spark.thrift] executing: {executing_query}")

            cursor.execute(executing_query)

            # 7. Fetch results as pandas
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()

            result = pd.DataFrame(rows, columns=columns) if columns else pd.DataFrame()

            logger.info(
                f"{log_prefix}[spark.thrift] query complete: "
                f"{len(rows)} rows, {len(columns)} cols"
            )

            return result

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