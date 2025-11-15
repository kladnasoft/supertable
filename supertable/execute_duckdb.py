# supertable/execute_duckdb.py

from __future__ import annotations

import os
import hashlib
from typing import List, Optional, Dict

import duckdb
import pandas as pd
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection


def _quote_if_needed(col: str) -> str:
    col = col.strip()
    if col == "*":
        return "*"
    if all(ch.isalnum() or ch == "_" for ch in col):
        return col
    return '"' + col.replace('"', '""') + '"'


class DuckDBExecutor:
    """
    Executes the provided SQL against the list of Parquet files using DuckDB.

    IMPORTANT:
    - httpfs / S3 / MinIO / presign behavior is preserved.
    - Only table creation + query execution flow is customized so that:
        * We iterate over reflection.supers (multiple tables).
        * For each logical table we create a DuckDB table whose name is:
              hash({super_name}_{simple_name}_{simple_version}_{TableDefinition.columns})
          implemented via a stable SHA1-based identifier.
        * We rewrite the original SQL to reference these hashed table names,
          keeping the original aliases (f, c, etc.).
        * parser.executing_query holds the rewritten query, which is executed.
    """

    def __init__(self, storage: Optional[object] = None):
        # storage is used only for presign retry; executor works without it
        self.storage = storage

    # =========================================================
    # httpfs / S3 / MinIO configuration
    # =========================================================

    def _normalize_endpoint_for_s3(self, ep: str) -> str:
        from urllib.parse import urlparse

        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ""
        return f"{host}{port}"

    def _detect_endpoint(self) -> Optional[str]:
        env_single = (
            os.getenv("AWS_S3_ENDPOINT_URL")
            or os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("MINIO_ENDPOINT")
            or os.getenv("MINIO_URL")
            or os.getenv("S3_ENDPOINT")
            or os.getenv("S3_ENDPOINT_URL")
            or os.getenv("S3_URL")
            or os.getenv("AWS_S3_ENDPOINT")
            or os.getenv("AWS_S3_URL")
        )
        if env_single:
            return self._normalize_endpoint_for_s3(env_single)

        host_env = (
            os.getenv("MINIO_HOST")
            or os.getenv("S3_HOST")
            or os.getenv("AWS_S3_HOST")
        )
        port_env = (
            os.getenv("MINIO_PORT")
            or os.getenv("S3_PORT")
            or os.getenv("AWS_S3_PORT")
        )
        if host_env:
            full = f"{host_env}{':' + port_env if port_env else ''}"
            return self._normalize_endpoint_for_s3(full)

        return None

    def _detect_region(self) -> str:
        return (
            os.getenv("AWS_DEFAULT_REGION")
            or os.getenv("AWS_S3_REGION")
            or os.getenv("MINIO_REGION")
            or "us-east-1"
        )

    def _detect_url_style(self) -> str:
        # Prefer path-style as default / MinIO-friendly
        if (os.getenv("MINIO_FORCE_PATH_STYLE", "1") or "1").lower() in (
            "1",
            "true",
            "yes",
            "on",
        ):
            return "path"
        return os.getenv("S3_URL_STYLE", "path")

    def _detect_ssl(self) -> bool:
        return (
            (
                os.getenv("MINIO_SECURE", "")
                or os.getenv("S3_USE_SSL", "")
            ).lower()
            in ("1", "true", "yes", "on")
        )

    def _detect_creds(self):
        ak = (
            os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("MINIO_ACCESS_KEY")
            or os.getenv("MINIO_ROOT_USER")
        )
        sk = (
            os.getenv("AWS_SECRET_ACCESS_KEY")
            or os.getenv("MINIO_SECRET_KEY")
            or os.getenv("MINIO_ROOT_PASSWORD")
        )
        st = os.getenv("AWS_SESSION_TOKEN")
        return ak, sk, st

    def _configure_httpfs_and_s3(
        self, con: duckdb.DuckDBPyConnection, for_paths: List[str]
    ) -> None:
        """
        Load httpfs and configure S3 / MinIO settings if needed.
        """
        if not for_paths:
            return

        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        any_s3 = any(str(p).lower().startswith("s3://") for p in for_paths)
        any_http = any(
            str(p).lower().startswith(("http://", "https://")) for p in for_paths
        )

        logger.debug(
            f"[duckdb] httpfs loaded | any_s3={any_s3} | any_http={any_http}"
        )

        if not (any_s3 or any_http):
            return

        # Discover supported settings
        try:
            supported = {
                name
                for (name,) in con.execute(
                    "SELECT name FROM duckdb_settings()"
                ).fetchall()
            }
        except Exception:
            supported = {
                "s3_endpoint",
                "s3_region",
                "s3_access_key_id",
                "s3_secret_access_key",
                "s3_session_token",
                "s3_url_style",
                "s3_use_ssl",
                "http_timeout",
                "enable_http_metadata_cache",
            }

        def set_if_supported(param: str, value_sql: str):
            if param in supported:
                con.execute(f"SET {param}={value_sql};")

        endpoint = self._detect_endpoint()
        access_key, secret_key, session_token = self._detect_creds()
        region = self._detect_region()
        url_style = self._detect_url_style()
        use_ssl = self._detect_ssl()

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

        # Optional HTTP timeout
        http_timeout_env = os.getenv("SUPERTABLE_DUCKDB_HTTP_TIMEOUT")
        if http_timeout_env:
            try:
                con.execute(f"SET http_timeout={int(http_timeout_env)};")
            except Exception:
                pass

        # Optional HTTP metadata cache toggle
        meta_cache_on = (
            os.getenv("SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE", "1") or "1"
        ).lower() in ("1", "true", "yes", "on")
        set_if_supported(
            "enable_http_metadata_cache",
            "true" if meta_cache_on else "false",
        )

    # =========================================================
    # presign helper
    # =========================================================

    def _detect_bucket(self) -> Optional[str]:
        return (
            os.getenv("SUPERTABLE_BUCKET")
            or os.getenv("MINIO_BUCKET")
            or os.getenv("S3_BUCKET")
            or os.getenv("AWS_S3_BUCKET")
            or os.getenv("AWS_BUCKET")
            or os.getenv("BUCKET")
        )

    def _url_to_key(self, url: str, bucket: Optional[str]) -> Optional[str]:
        if url.startswith("s3://"):
            parts = url.split("/", 3)
            if len(parts) >= 4:
                return parts[3]
            return None

        if url.startswith(("http://", "https://")):
            import re as _re

            m = _re.match(r"^https?://[^/]+/(.+)$", url)
            if not m:
                return None
            tail = m.group(1)

            if bucket and tail.startswith(bucket + "/"):
                return tail[len(bucket) + 1 :]

            return tail.split("/", 1)[1] if "/" in tail else None

        return None

    def _make_presigned_list(self, paths: List[str]) -> List[str]:
        presign_fn = (
            getattr(self.storage, "presign", None)
            if self.storage is not None
            else None
        )
        if not callable(presign_fn):
            return paths

        bucket = self._detect_bucket()
        out: List[str] = []

        for p in paths:
            key = self._url_to_key(p, bucket)
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
    # Table naming / creation helpers
    # =========================================================

    def _hashed_table_name(
        self,
        super_name: str,
        simple_name: str,
        simple_version: int,
        columns: List[str],
    ) -> str:
        """
        Stable hashed table name based on:
            {super_name}_{simple_name}_{simple_version}_{TableDefinition.columns}

        Empty columns -> '*' marker.
        """
        cols_part = ",".join(sorted(columns)) if columns else "*"
        key = f"{super_name}_{simple_name}_{simple_version}_{cols_part}"
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return f"st_{digest}"

    def _create_reflection_table(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        files: List[str],
        columns: List[str],
    ) -> None:
        if not files:
            raise ValueError(
                f"No files provided for reflection table '{table_name}'"
            )

        parquet_files_str = ", ".join(f"'{f}'" for f in files)

        # [] -> all columns (*), otherwise explicit projection
        if not columns:
            select_cols = "*"
        else:
            select_cols = ", ".join(
                _quote_if_needed(c) for c in columns if c and c.strip()
            )

        logger.debug(
            f"[duckdb] creating reflection table {table_name} "
            f"from {len(files)} parquet file(s) | cols={select_cols}"
        )

        sql = (
            f"CREATE TABLE {table_name} AS "
            f"SELECT {select_cols} "
            f"FROM parquet_scan([{parquet_files_str}], "
            f"union_by_name=TRUE, HIVE_PARTITIONING=TRUE);"
        )
        con.execute(sql)

    def _rewrite_query_with_hashed_tables(
        self,
        original_sql: str,
        alias_to_table: Dict[str, str],
    ) -> str:
        """
        Rewrite SQL so that each table reference uses its hashed reflection
        table, while preserving the original aliases.

        alias_to_table: alias -> hashed table name
        """
        if not alias_to_table:
            return original_sql

        try:
            parsed = sqlglot.parse_one(original_sql)
        except Exception as e:
            logger.warning(
                f"[duckdb] Failed to parse SQL for rewrite; using original. Error: {e}"
            )
            return original_sql

        for table in parsed.find_all(exp.Table):
            alias_expr = table.args.get("alias")
            alias_name = None

            # Explicit alias: FROM foo_bar f
            if isinstance(alias_expr, exp.TableAlias):
                ident = alias_expr.this
                if isinstance(ident, exp.Identifier):
                    alias_name = ident.name

            # If no explicit alias, alias is the table name itself
            if not alias_name:
                alias_name = table.name

            if alias_name in alias_to_table:
                new_physical = alias_to_table[alias_name]
                table.set("this", exp.to_identifier(new_physical))
                # Remove schema/db if present
                table.set("db", None)

        return parsed.sql()

    # =========================================================
    # Core execution
    # =========================================================

    def execute(
        self,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer_capture,
        log_prefix: str = "",
    ) -> pd.DataFrame:
        con = duckdb.connect()
        tried_presign = False

        try:
            timer_capture("CONNECTING")

            # Baseline pragmas
            con.execute("PRAGMA memory_limit='2GB';")
            con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
            con.execute("PRAGMA enable_profiling='json';")
            con.execute(
                f"PRAGMA profile_output='{query_manager.query_plan_path}';"
            )
            con.execute("PRAGMA default_collation='nocase';")

            threads_env = os.getenv("SUPERTABLE_DUCKDB_THREADS")
            if threads_env:
                try:
                    con.execute(f"SET threads={int(threads_env)};")
                except Exception:
                    pass

            # -------------------------------------------------
            # 1) Map (super_name, simple_name) -> SuperSnapshot
            # -------------------------------------------------
            snapshots_by_key: Dict[tuple, object] = {}
            for sup in reflection.supers:
                key = (sup.super_name, sup.simple_name)
                # If multiple versions exist, keep the last one; caller controls order.
                snapshots_by_key[key] = sup

            # -------------------------------------------------
            # 2) From parser table definitions build hashed tables
            # -------------------------------------------------
            table_defs = parser.get_table_tuples()

            alias_to_table_name: Dict[str, str] = {}
            alias_to_files: Dict[str, List[str]] = {}
            alias_to_columns: Dict[str, List[str]] = {}

            for td in table_defs:
                key = (td.super_name, td.simple_name)
                sup = snapshots_by_key.get(key)

                if not sup:
                    logger.warning(
                        f"{log_prefix}[duckdb] No reflection.super "
                        f"for {td.super_name}.{td.simple_name}"
                    )
                    continue

                hashed_name = self._hashed_table_name(
                    sup.super_name,
                    sup.simple_name,
                    sup.simple_version,
                    td.columns,
                )

                alias_to_table_name[td.alias] = hashed_name
                alias_to_files[td.alias] = list(sup.files)
                alias_to_columns[td.alias] = list(td.columns or [])

            # Create physical DuckDB tables per alias
            for alias, table_name in alias_to_table_name.items():
                files = alias_to_files[alias]
                cols = alias_to_columns[alias]

                self._configure_httpfs_and_s3(con, files)

                try:
                    self._create_reflection_table(con, table_name, files, cols)
                except Exception as e:
                    msg = str(e)
                    if any(
                        tok in msg
                        for tok in (
                            "HTTP GET error",
                            "AccessDenied",
                            "SignatureDoesNotMatch",
                            "403",
                            "400",
                        )
                    ):
                        logger.warning(
                            f"{log_prefix}[duckdb.retry] presign fallback for "
                            f"{table_name} due to: {msg}"
                        )
                        tried_presign = True
                        presigned_files = self._make_presigned_list(files)
                        self._configure_httpfs_and_s3(con, presigned_files)
                        self._create_reflection_table(
                            con, table_name, presigned_files, cols
                        )
                    else:
                        raise

            timer_capture("CREATING_REFLECTION")

            # -------------------------------------------------
            # 3) Rewrite and execute query
            # -------------------------------------------------
            executing_query = self._rewrite_query_with_hashed_tables(
                parser.original_query,
                alias_to_table_name,
            )

            parser.executing_query = executing_query

            logger.debug(
                f"{log_prefix}[duckdb] executing query: {executing_query}"
            )

            result = con.execute(executing_query).fetchdf()

            if tried_presign:
                logger.debug(
                    f"{log_prefix}[duckdb.retry] presigned fallback succeeded"
                )

            return result

        finally:
            try:
                con.close()
            except Exception:
                pass
