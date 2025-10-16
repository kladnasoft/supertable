# supertable/data_reader.py

from __future__ import annotations

import os
import tempfile
import re
from enum import Enum
from typing import Iterable, Set, Tuple, List, Dict, Optional
from urllib.parse import urlparse

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.super_table import SuperTable
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.plan_extender import extend_execution_plan
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access
from supertable.redis_catalog import RedisCatalog  # uses Redis leaf pointers for snapshots


class Status(Enum):
    OK = "ok"
    ERROR = "error"


def _lower_set(items: Iterable[str]) -> Set[str]:
    return {str(x).lower() for x in items}


def _quote_if_needed(col: str) -> str:
    col = col.strip()
    if col == "*":
        return "*"
    if all(ch.isalnum() or ch == "_" for ch in col):
        return col
    return '"' + col.replace('"', '""') + '"'


class DataReader:
    def __init__(self, super_name, organization, query):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.storage = self.super_table.storage
        self.parser = SQLParser(query)
        self.parser.parse_sql()
        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None
        self._log_ctx = ""
        self.catalog = RedisCatalog()

    def _lp(self, msg: str) -> str:
        return f"{self._log_ctx}{msg}"

    # -------------------------------------------------------------------------------------
    # Storage → DuckDB path resolution + S3/httpfs config
    # -------------------------------------------------------------------------------------

    def _get_env(self, *names: str) -> Optional[str]:
        for n in names:
            v = os.getenv(n)
            if v:
                return v
        return None

    def _storage_attr(self, *names: str) -> Optional[str]:
        for n in names:
            if hasattr(self.storage, n):
                v = getattr(self.storage, n)
                if v not in (None, "", False):
                    return str(v)
        return None

    def _normalize_endpoint_for_s3(self, ep: str) -> str:
        """DuckDB PRAGMA s3_endpoint expects host[:port] (no scheme)."""
        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ("" if ":" in ep else "")
        return f"{host}{port}"

    def _detect_endpoint(self) -> Optional[str]:
        candidates_storage = [
            "endpoint_url", "endpoint", "url", "api_url", "base_url",
            "s3_endpoint", "minio_endpoint", "public_endpoint",
        ]
        for name in candidates_storage:
            val = self._storage_attr(name)
            if val:
                logger.debug(self._lp(f"[env.candidates] storage.{name}='{val}'"))
                return self._normalize_endpoint_for_s3(val)

        host = self._storage_attr("host", "hostname")
        port = self._storage_attr("port")
        if host:
            composed = f"{host}{':' + port if port else ''}"
            logger.debug(self._lp(f"[env.candidates] storage host/port → '{composed}'"))
            return self._normalize_endpoint_for_s3(composed)

        host_env = self._get_env("MINIO_HOST", "S3_HOST", "AWS_S3_HOST")
        port_env = self._get_env("MINIO_PORT", "S3_PORT", "AWS_S3_PORT")
        if host_env:
            composed = f"{host_env}{':' + port_env if port_env else ''}"
            logger.debug(self._lp(f"[env.candidates] env host/port → '{composed}'"))
            return self._normalize_endpoint_for_s3(composed)

        env_single = self._get_env(
            "AWS_S3_ENDPOINT_URL",
            "MINIO_ENDPOINT",
            "MINIO_URL",
            "MINIO_SERVER",
            "MINIO_ADDRESS",
            "MINIO_API_URL",
            "MINIO_PUBLIC_URL",
            "S3_ENDPOINT",
            "S3_ENDPOINT_URL",
            "S3_URL",
            "AWS_ENDPOINT_URL",
            "AWS_S3_ENDPOINT",
            "AWS_S3_URL",
        )
        if env_single:
            logger.debug(self._lp(f"[env.candidates] single endpoint env → '{env_single}'"))
            return self._normalize_endpoint_for_s3(env_single)

        return None

    def _detect_bucket(self) -> Optional[str]:
        for name in ("bucket", "bucket_name", "default_bucket"):
            v = self._storage_attr(name)
            if v:
                logger.debug(self._lp(f"[env.candidates] storage.{name}='{v}'"))
                return v
        env = self._get_env("SUPERTABLE_BUCKET", "MINIO_BUCKET", "S3_BUCKET", "AWS_S3_BUCKET", "AWS_BUCKET", "BUCKET")
        if env:
            logger.debug(self._lp(f"[env.candidates] bucket env='{env}'"))
            return env
        return None

    def _detect_creds(self) -> Tuple[Optional[str], Optional[str]]:
        ak = (self._storage_attr("access_key", "access_key_id", "aws_access_key_id")
              or self._get_env("AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"))
        sk = (self._storage_attr("secret_key", "secret_access_key", "aws_secret_access_key")
              or self._get_env("AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY"))
        return ak, sk

    def _detect_region(self) -> str:
        return (self._storage_attr("region")
                or self._get_env("AWS_DEFAULT_REGION", "AWS_S3_REGION", "MINIO_REGION")
                or "us-east-1")

    def _detect_url_style(self) -> str:
        v = self._storage_attr("url_style") or self._get_env("S3_URL_STYLE")
        if v:
            return v
        force_path = (os.getenv("AWS_S3_FORCE_PATH_STYLE", "") or "").lower() in ("1", "true", "yes", "on")
        return "path" if force_path else "path"

    def _detect_ssl(self) -> bool:
        val = (
            (str(getattr(self.storage, "secure", "")).lower() if hasattr(self.storage, "secure") else "")
            or (self._get_env("MINIO_SECURE", "S3_USE_SSL") or "")
        ).lower()
        return val in ("1", "true", "yes", "on")

    def _first_segment(self, key: str) -> Tuple[str, str]:
        if "/" in key:
            first, rest = key.split("/", 1)
            return first, rest
        return key, ""

    def _strip_leading_bucket_dup(self, key: str, bucket: str) -> str:
        if key.startswith(bucket + "/"):
            return key[len(bucket) + 1 :]
        return key

    def _diagnose_env_once(self):
        endpoint_raw = self._detect_endpoint()
        bucket = self._detect_bucket()
        ak, sk = self._detect_creds()
        region = self._detect_region()
        url_style = self._detect_url_style()
        ssl_on = self._detect_ssl()
        logger.info(
            self._lp(
                f"[env] storage={type(self.storage).__name__} | "
                f"endpoint={'set' if endpoint_raw else 'MISSING'} | "
                f"bucket={bucket or 'MISSING'} | region={region} | "
                f"url_style={url_style} | ssl={'on' if ssl_on else 'off'} | "
                f"access_key={'set' if ak else 'MISSING'} | secret_key={'set' if sk else 'MISSING'}"
            )
        )

    def _to_duckdb_path(self, key: str) -> str:
        """
        Convert snapshot 'resources[].file' key into a DuckDB-readable URL/path.

        Preference order:
          1) If SUPERTABLE_DUCKDB_PRESIGNED=1 and storage.presign exists → presigned http(s)://...
          2) If already URL (://), return as-is.
          3) If storage provides helper (to_duckdb_path / make_duckdb_url / make_url / presign) → use it.
          4) Else build s3://bucket/key (or http(s)://host/bucket/key if SUPERTABLE_DUCKDB_USE_HTTPFS=1).
        """
        raw = key
        if not key:
            return key

        # 1) forced presigned mode
        use_presigned = (os.getenv("SUPERTABLE_DUCKDB_PRESIGNED", "") or "").lower() in ("1", "true", "yes", "on")
        if use_presigned:
            presign_fn = getattr(self.storage, "presign", None)
            if callable(presign_fn):
                try:
                    url = presign_fn(key)
                    if isinstance(url, str) and url:
                        logger.info(self._lp(f"[resolve] presigned URL → {url[:96]}..."))
                        return url
                except Exception as e:
                    logger.warning(self._lp(f"[resolve] presign failed, falling back to s3/http: {e}"))

        # 2) already URL
        if "://" in key:
            logger.debug(self._lp(f"[resolve.details] already URL: {key}"))
            return key

        # 3) storage helper
        for attr in ("to_duckdb_path", "make_duckdb_url", "make_url", "presign"):
            fn = getattr(self.storage, attr, None)
            if callable(fn):
                try:
                    url = fn(key)  # type: ignore[misc]
                    if isinstance(url, str) and url:
                        logger.info(self._lp(f"[resolve] storage.{attr} → {url}"))
                        return url
                except Exception as e:
                    logger.debug(self._lp(f"[resolve.details] storage.{attr} failed: {e}"))

        # 4) build from endpoint/bucket
        endpoint_raw = self._detect_endpoint()
        bucket = self._detect_bucket()
        use_http = (os.getenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "") or "").lower() in ("1", "true", "yes", "on")
        scheme = "https" if self._detect_ssl() else "http"

        if endpoint_raw:
            endpoint = self._normalize_endpoint_for_s3(endpoint_raw)
            key_norm = key.lstrip("/")

            if not bucket:
                first, rest = self._first_segment(key_norm)
                if rest:
                    bucket = first
                    key_in_bucket = rest
                    logger.info(self._lp(f"[resolve] bucket inferred from key → '{bucket}'"))
                else:
                    bucket = first
                    key_in_bucket = ""
                    logger.info(self._lp(f"[resolve] single-segment key; using as bucket → '{bucket}'"))
            else:
                key_in_bucket = self._strip_leading_bucket_dup(key_norm, bucket)

            url = (
                f"{scheme}://{endpoint.rstrip('/')}/{bucket}/{key_in_bucket}"
                if use_http
                else f"s3://{bucket}/{key_in_bucket}"
            )
            logger.info(self._lp(f"[resolve] key='{raw}' → url='{url}' (httpfs={'on' if use_http else 'off'})"))
            return url

        logger.warning(self._lp(
            "[resolve] NO endpoint detected; using raw path (likely wrong). "
            "Set AWS_S3_ENDPOINT_URL (e.g., http://localhost:9000)."
        ))
        return raw

    # -------------------------------------------------------------------------------------
    # S3/httpfs config — sets only supported params
    # -------------------------------------------------------------------------------------
    def _configure_duckdb_httpfs_and_s3(self, con: duckdb.DuckDBPyConnection, for_paths: List[str]) -> None:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        any_s3 = any(str(p).lower().startswith("s3://") for p in for_paths)
        any_http = any(str(p).lower().startswith(("http://", "https://")) for p in for_paths)
        logger.info(self._lp(f"[duckdb] httpfs loaded | any_s3={any_s3} | any_http={any_http}"))

        if not (any_s3 or any_http):
            return

        try:
            supported = {name for (name,) in con.execute("SELECT name FROM duckdb_settings()").fetchall()}
        except Exception:
            supported = {"s3_endpoint", "s3_region", "s3_access_key_id", "s3_secret_access_key",
                         "s3_url_style", "s3_use_ssl"}

        def set_if_supported(param: str, value_sql: str):
            if param in supported:
                con.execute(f"SET {param}={value_sql};")
            else:
                logger.debug(self._lp(f"[duckdb.config] skip unsupported param '{param}'"))

        endpoint_raw = self._detect_endpoint()
        endpoint = self._normalize_endpoint_for_s3(endpoint_raw) if endpoint_raw else None
        access_key, secret_key = self._detect_creds()
        region = self._detect_region()
        url_style = self._detect_url_style()
        use_ssl = self._detect_ssl()

        if endpoint:
            set_if_supported("s3_endpoint", f"'{endpoint}'")
        if access_key:
            set_if_supported("s3_access_key_id", f"'{access_key}'")
        if secret_key:
            set_if_supported("s3_secret_access_key", f"'{secret_key}'")
        if region:
            set_if_supported("s3_region", f"'{region}'")
        set_if_supported("s3_url_style", f"'{url_style}'")
        set_if_supported("s3_use_ssl", "TRUE" if use_ssl else "FALSE")

        logger.info(
            self._lp(
                "[duckdb.config] "
                f"endpoint={'set' if endpoint else 'MISSING'}, region={region}, "
                f"url_style={url_style}, ssl={'on' if use_ssl else 'off'}, "
                f"access_key={'set' if access_key else 'MISSING'}, secret_key={'set' if secret_key else 'MISSING'}"
            )
        )

    # -------------------------------------------------------------------------------------
    # Snapshot collection & filtering
    # -------------------------------------------------------------------------------------

    def _collect_snapshots_from_redis(self) -> List[Dict]:
        items = list(self.catalog.scan_leaf_items(self.super_table.organization, self.super_table.super_name, count=512))
        snapshots = []
        for it in items:
            if not it.get("path"):
                continue
            snapshots.append(
                {
                    "table_name": it["simple"],
                    "last_updated_ms": int(it.get("ts", 0)),
                    "path": it["path"],
                    "files": 0,
                    "rows": 0,
                    "file_size": 0,
                }
            )
        return snapshots

    def filter_snapshots(self, snapshots: List[Dict]) -> List[Dict]:
        if self.super_table.super_name.lower() == self.parser.original_table.lower():
            return [s for s in snapshots if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))]
        else:
            return [s for s in snapshots if s["table_name"].lower() == self.parser.original_table.lower()]

    # -------------------------------------------------------------------------------------

    def execute(self, user_hash: str, with_scan: bool = False):
        status = Status.ERROR
        message = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        try:
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                current_meta_path="redis://meta/root",
                parser=self.parser,
            )
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "

            self._diagnose_env_once()

            snapshots_all = self._collect_snapshots_from_redis()
            snapshots = self.filter_snapshots(snapshots_all)
            logger.info(self._lp(f"[snapshots] total={len(snapshots)} (post-filter)"))

            parquet_files, schema = self.process_snapshots(snapshots=snapshots, with_scan=with_scan)

            if parquet_files:
                preview = ", ".join(parquet_files[:3]) + (" ..." if len(parquet_files) > 3 else "")
                logger.info(self._lp(f"[paths] resolved for parquet_scan: {preview}"))

            missing_columns: Set[str] = set()
            if self.parser.columns_csv != "*":
                requested = _lower_set(self.parser.columns_list)
                missing_columns = requested - schema

            if len(snapshots) == 0 or missing_columns or not parquet_files:
                message = (
                    f"Missing column(s): {', '.join(missing_columns)}" if missing_columns else "No parquet files found"
                )
                logger.warning(self._lp(f"[guard] {message}"))
                return pd.DataFrame(), status, message

            restrict_read_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=self.parser.reflection_table,
                table_schema=schema,
                parsed_columns=self.parser.columns_list,
                parser=self.parser,
            )
            self.timer.capture_and_reset_timing(event="FILTERING")

            result = self.execute_with_duckdb(parquet_files=parquet_files, query_manager=self.query_plan_manager)
            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result = pd.DataFrame()

        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")

        try:
            extend_execution_plan(
                super_table=self.super_table,
                query_plan_manager=self.query_plan_manager,
                user_hash=user_hash,
                timing=self.timer.timings,
                plan_stats=self.plan_stats,
                status=str(status.value),
                message=message,
                result_shape=result.shape,
            )
        except Exception as e:
            logger.error(self._lp(f"extend_execution_plan exception: {e}"))

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        try:
            total = next((t["TOTAL_EXECUTE"] for t in self.timer.timings if "TOTAL_EXECUTE" in t), None)
            meta = next((t["META"] for t in self.timer.timings if "META" in t), 0.0)
            filt = next((t["FILTERING"] for t in self.timer.timings if "FILTERING" in t), 0.0)
            conn = next((t["CONNECTING"] for t in self.timer.timings if "CONNECTING" in t), 0.0)
            create = next((t["CREATING_REFLECTION"] for t in self.timer.timings if "CREATING_REFLECTION" in t), 0.0)
            execq = next((t["EXECUTING_QUERY"] for t in self.timer.timings if "EXECUTING_QUERY" in t), 0.0)
            extend = next((t["EXTENDING_PLAN"] for t in self.timer.timings if "EXTENDING_PLAN" in t), 0.0)

            logger.info(
                f"[read][qid={self.query_plan_manager.query_id}] Timing(s): "
                f"total={(total or 0.0):.3f} | meta={meta:.3f} | filter={filt:.3f} | connect={conn:.3f} | "
                f"create={create:.3f} | execute={execq:.3f} | extend={extend:.3f}"
            )
        except Exception:
            pass

        return result, status, message

    # -------------------------------------------------------------------------------------

    def process_snapshots(self, snapshots: List[Dict], with_scan: bool) -> Tuple[List[str], Set[str]]:
        parquet_files: List[str] = []
        reflection_file_size = 0
        reflection_rows = 0

        schema: Set[str] = set()
        for snapshot in snapshots:
            current_snapshot_path = snapshot["path"]
            current_snapshot_data = self.super_table.read_simple_table_snapshot(current_snapshot_path)

            current_schema = current_snapshot_data.get("schema", {})
            schema.update(dict_keys_to_lowercase(current_schema).keys())

            resources = current_snapshot_data.get("resources", []) or []
            for resource in resources:
                file_key = resource.get("file")
                if not file_key:
                    continue

                resolved = self._to_duckdb_path(file_key)
                if "://" not in resolved:
                    logger.warning(self._lp(f"[paths] unresolved to URL → '{resolved}' from key='{file_key}'"))
                parquet_files.append(resolved)

                reflection_file_size += int(resource.get("file_size", 0))
                reflection_rows += int(resource.get("rows", 0))

        self.plan_stats.add_stat({"REFLECTIONS": len(parquet_files)})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_ROWS": reflection_rows})

        return parquet_files, schema

    # -------------------------------------------------------------------------------------
    # Local-copy fallback (optional)
    # -------------------------------------------------------------------------------------
    def _maybe_copy_locally(self, parquet_files: List[str]) -> List[str]:
        """Copy remote files locally if SUPERTABLE_DUCKDB_FORCE_LOCAL_COPY=1 (last-resort)."""
        force = (os.getenv("SUPERTABLE_DUCKDB_FORCE_LOCAL_COPY", "") or "").lower() in ("1", "true", "yes", "on")
        if not force:
            return parquet_files

        tmpdir = tempfile.mkdtemp(prefix="st_duckdb_")
        local_paths: List[str] = []
        for p in parquet_files:
            try:
                key = p
                if p.startswith("s3://"):
                    parts = p.split("/", 3)
                    key = parts[3] if len(parts) >= 4 else ""
                elif p.startswith(("http://", "https://")):
                    parts = p.split("/", 3)
                    key = parts[3] if len(parts) >= 4 else ""

                data = self.storage.read_bytes(key)
                local_path = os.path.join(tmpdir, os.path.basename(key or "data.parquet"))
                with open(local_path, "wb") as f:
                    f.write(data)
                local_paths.append(local_path)
            except Exception as e:
                logger.warning(self._lp(f"[fallback] local copy failed for {p}: {e}"))
        if local_paths:
            logger.info(self._lp(f"[fallback] using local temp files: {len(local_paths)} in {tmpdir}"))
        return local_paths or parquet_files

    # -------------------------------------------------------------------------------------
    # Presign fallback utilities
    # -------------------------------------------------------------------------------------

    def _url_to_key(self, url: str, bucket: Optional[str]) -> Optional[str]:
        """Best-effort extraction of the object key from s3:// or http(s):// URL."""
        if url.startswith("s3://"):
            parts = url.split("/", 3)
            if len(parts) >= 4:
                return parts[3]
            return None
        if url.startswith(("http://", "https://")):
            # http(s)://host/bucket/key...
            m = re.match(r"^https?://[^/]+/(.+)$", url)
            if not m:
                return None
            tail = m.group(1)
            if bucket and tail.startswith(bucket + "/"):
                return tail[len(bucket) + 1 :]
            return tail.split("/", 1)[1] if "/" in tail else None
        return None

    def _make_presigned_list(self, paths: List[str]) -> List[str]:
        presign_fn = getattr(self.storage, "presign", None)
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
                    logger.warning(self._lp(f"[presign] failed for '{p}': {e}"))
                    out.append(p)
            else:
                out.append(p)
        return out

    # -------------------------------------------------------------------------------------

    def _run_parquet_scan(self, con: duckdb.DuckDBPyConnection, files: List[str]) -> None:
        parquet_files_str = ", ".join(f"'{file}'" for file in files)
        if self.parser.columns_csv == "*":
            safe_columns_csv = "*"
        else:
            cols = [c for c in self.parser.columns_csv.split(",") if c.strip()]
            safe_columns_csv = ", ".join(_quote_if_needed(c) for c in cols)

        logger.info(self._lp(f"[duckdb] parquet_scan on {len(files)} file(s)"))
        if files:
            logger.info(self._lp(f"[duckdb] first path → {files[0]}"))

        create_table = f"""
CREATE TABLE {self.parser.reflection_table}
AS
SELECT {safe_columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=TRUE, HIVE_PARTITIONING=TRUE);
"""
        con.execute(create_table)

        create_view = f"""
CREATE VIEW {self.parser.rbac_view}
AS
{self.parser.view_definition}
"""
        con.execute(create_view)

    # -------------------------------------------------------------------------------------

    def execute_with_duckdb(self, parquet_files, query_manager: QueryPlanManager):
        con = duckdb.connect()
        tried_presign = False
        try:
            self.timer.capture_and_reset_timing("CONNECTING")
            # Core pragmas
            con.execute("PRAGMA memory_limit='2GB';")
            con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
            con.execute("PRAGMA enable_profiling='json';")
            con.execute(f"PRAGMA profile_output = '{query_manager.query_plan_path}';")
            con.execute("PRAGMA default_collation='nocase';")

            # ---- NEW: concurrency & HTTP knobs (env-driven) ----
            threads_env = os.getenv("SUPERTABLE_DUCKDB_THREADS")
            if threads_env:
                try:
                    con.execute(f"SET threads={int(threads_env)};")
                except Exception as e:
                    logger.warning(self._lp(f"[duckdb] ignoring invalid SUPERTABLE_DUCKDB_THREADS='{threads_env}': {e}"))

            external_threads_env = os.getenv("SUPERTABLE_DUCKDB_EXTERNAL_THREADS")
            if external_threads_env:
                try:
                    con.execute(f"SET external_threads={int(external_threads_env)};")
                except Exception as e:
                    logger.warning(self._lp(f"[duckdb] ignoring invalid SUPERTABLE_DUCKDB_EXTERNAL_THREADS='{external_threads_env}': {e}"))

            http_timeout_env = os.getenv("SUPERTABLE_DUCKDB_HTTP_TIMEOUT")
            if http_timeout_env:
                try:
                    con.execute(f"SET http_timeout={int(http_timeout_env)};")
                except Exception as e:
                    logger.warning(self._lp(f"[duckdb] ignoring invalid SUPERTABLE_DUCKDB_HTTP_TIMEOUT='{http_timeout_env}': {e}"))

            meta_cache_on = (os.getenv("SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE", "1") or "1").lower() in ("1", "true", "yes", "on")
            con.execute(f"SET enable_http_metadata_cache={'true' if meta_cache_on else 'false'};")

            # Logging snapshot of concurrency settings
            try:
                settings = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN ('threads','external_threads','http_timeout','enable_http_metadata_cache')").fetchall())
                logger.info(self._lp(f"[duckdb.threads] threads={settings.get('threads')} | external_threads={settings.get('external_threads')} | http_timeout={settings.get('http_timeout')} | http_metadata_cache={settings.get('enable_http_metadata_cache')}"))
            except Exception:
                pass
            # ----------------------------------------------------

            self._configure_duckdb_httpfs_and_s3(con, parquet_files)

            parquet_files_local = self._maybe_copy_locally(parquet_files)

            try:
                self._run_parquet_scan(con, parquet_files_local)
            except Exception as e:
                msg = str(e)
                # Detect typical auth/HTTP failures and retry with presigned URLs once
                if ("HTTP GET error" in msg or "SignatureDoesNotMatch" in msg or "AccessDenied" in msg) and not (
                    (os.getenv("SUPERTABLE_DUCKDB_PRESIGNED", "") or "").lower() in ("1", "true", "yes", "on")
                ):
                    logger.warning(self._lp(f"[duckdb.retry] switching to presigned URLs due to: {msg}"))
                    tried_presign = True
                    presigned = self._make_presigned_list(parquet_files_local)
                    # Clean up partial artifacts if any
                    try:
                        con.execute(f"DROP VIEW IF EXISTS {self.parser.rbac_view};")
                        con.execute(f"DROP TABLE IF EXISTS {self.parser.reflection_table};")
                    except Exception:
                        pass
                    self._run_parquet_scan(con, presigned)
                else:
                    raise

            self.timer.capture_and_reset_timing("CREATING_REFLECTION")
            result = con.execute(query=self.parser.executing_query).fetchdf()
            if tried_presign:
                logger.info(self._lp("[duckdb.retry] presigned fallback succeeded"))
            return result
        finally:
            try:
                con.close()
            except Exception:
                pass
