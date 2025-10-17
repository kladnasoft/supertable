# supertable/execute_duckdb.py

from __future__ import annotations

import os
from typing import List, Optional

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser


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
    Mirrors your original httpfs/S3 logic and presign retry.
    """

    def __init__(self, storage: Optional[object] = None):
        # we accept storage optionally for presign retry; if omitted, executor still works
        self.storage = storage

    # -------- httpfs / S3 config --------

    def _normalize_endpoint_for_s3(self, ep: str) -> str:
        from urllib.parse import urlparse
        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ("" if ":" in ep else "")
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
        host_env = os.getenv("MINIO_HOST") or os.getenv("S3_HOST") or os.getenv("AWS_S3_HOST")
        port_env = os.getenv("MINIO_PORT") or os.getenv("S3_PORT") or os.getenv("AWS_S3_PORT")
        if host_env:
            return self._normalize_endpoint_for_s3(f"{host_env}{':' + port_env if port_env else ''}")
        return None

    def _detect_region(self) -> str:
        return os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_S3_REGION") or os.getenv("MINIO_REGION") or "us-east-1"

    def _detect_url_style(self) -> str:
        if (os.getenv("MINIO_FORCE_PATH_STYLE", "1") or "1").lower() in ("1", "true", "yes", "on"):
            return "path"
        return os.getenv("S3_URL_STYLE", "path")

    def _detect_ssl(self) -> bool:
        return ((os.getenv("MINIO_SECURE", "") or os.getenv("S3_USE_SSL", "")).lower() in ("1", "true", "yes", "on"))

    def _detect_creds(self):
        ak = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
        sk = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
        st = os.getenv("AWS_SESSION_TOKEN")
        return ak, sk, st

    def _configure_httpfs_and_s3(self, con: duckdb.DuckDBPyConnection, for_paths: List[str]) -> None:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        any_s3 = any(str(p).lower().startswith("s3://") for p in for_paths)
        any_http = any(str(p).lower().startswith(("http://", "https://")) for p in for_paths)
        logger.debug(f"[duckdb] httpfs loaded | any_s3={any_s3} | any_http=True" if any_http else f"[duckdb] httpfs loaded | any_s3={any_s3} | any_http=False")
        if not (any_s3 or any_http):
            return

        try:
            supported = {name for (name,) in con.execute("SELECT name FROM duckdb_settings()").fetchall()}
        except Exception:
            supported = {
                "s3_endpoint", "s3_region", "s3_access_key_id", "s3_secret_access_key", "s3_session_token",
                "s3_url_style", "s3_use_ssl", "http_timeout", "enable_http_metadata_cache"
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

        http_timeout_env = os.getenv("SUPERTABLE_DUCKDB_HTTP_TIMEOUT")
        if http_timeout_env:
            try:
                con.execute(f"SET http_timeout={int(http_timeout_env)};")
            except Exception:
                pass
        meta_cache_on = (os.getenv("SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE", "1") or "1").lower() in ("1", "true", "yes", "on")
        set_if_supported("enable_http_metadata_cache", "true" if meta_cache_on else "false")

    # -------- presign retry --------

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
        presign_fn = getattr(self.storage, "presign", None) if self.storage is not None else None
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
                    # Fall back to the original path on failure
                    out.append(p)
                    logger.warning(f"[presign] failed for '{p}': {e}")
            else:
                out.append(p)
        return out

    # -------- core execution --------

    def _run_parquet_scan(self, con: duckdb.DuckDBPyConnection, parser: SQLParser, files: List[str]) -> None:
        parquet_files_str = ", ".join(f"'{file}'" for file in files)
        if parser.columns_csv == "*":
            safe_columns_csv = "*"
        else:
            cols = [c for c in parser.columns_csv.split(",") if c.strip()]
            safe_columns_csv = ", ".join(_quote_if_needed(c) for c in cols)

        logger.debug(f"[duckdb] parquet_scan on {len(files)} file(s)")
        if files:
            logger.debug(f"[duckdb] first path → {files[0]}")

        create_table = f"""
CREATE TABLE {parser.reflection_table}
AS
SELECT {safe_columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=TRUE, HIVE_PARTITIONING=TRUE);
"""
        con.execute(create_table)

        # Safe RBAC view creation (guard against None/empty view_definition)
        view_sql = parser.view_definition.strip() if getattr(parser, "view_definition", None) else ""
        if not view_sql:
            view_sql = f"SELECT * FROM {parser.reflection_table}"

        create_view = f"""
CREATE VIEW {parser.rbac_view}
AS
{view_sql}
"""
        con.execute(create_view)

    def execute(
        self,
        parquet_files: List[str],
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer_capture,
        log_prefix: str = "",
    ) -> pd.DataFrame:
        con = duckdb.connect()
        tried_presign = False
        try:
            timer_capture("CONNECTING")
            con.execute("PRAGMA memory_limit='2GB';")
            con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
            con.execute("PRAGMA enable_profiling='json';")
            con.execute(f"PRAGMA profile_output = '{query_manager.query_plan_path}';")
            con.execute("PRAGMA default_collation='nocase';")

            threads_env = os.getenv("SUPERTABLE_DUCKDB_THREADS")
            if threads_env:
                try:
                    con.execute(f"SET threads={int(threads_env)};")
                except Exception:
                    pass

            self._configure_httpfs_and_s3(con, parquet_files)

            # First attempt
            try:
                self._run_parquet_scan(con, parser, parquet_files)
            except Exception as e:
                msg = str(e)
                # Retry once with presigned URLs by extracting keys
                if any(tok in msg for tok in ("HTTP GET error", "AccessDenied", "SignatureDoesNotMatch", "403", "400")):
                    logger.warning(f"{log_prefix}[duckdb.retry] switching to presigned URLs due to: {msg}")
                    tried_presign = True
                    presigned = self._make_presigned_list(parquet_files)
                    self._run_parquet_scan(con, parser, presigned)
                else:
                    raise

            timer_capture("CREATING_REFLECTION")
            result = con.execute(query=parser.executing_query).fetchdf()
            if tried_presign:
                logger.debug(f"{log_prefix}[duckdb.retry] presigned fallback succeeded")
            return result
        finally:
            try:
                con.close()
            except Exception:
                pass
