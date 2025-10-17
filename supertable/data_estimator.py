# supertable/data_estimator.py

from __future__ import annotations

import os
from typing import Iterable, Set, List, Dict, Optional
from urllib.parse import urlparse

from supertable.config.defaults import logger
from supertable.super_table import SuperTable
from supertable.utils.sql_parser import SQLParser
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.rbac.access_control import restrict_read_access
from supertable.redis_catalog import RedisCatalog  # Redis leaf pointers for snapshots


def _lower_set(items: Iterable[str]) -> Set[str]:
    return {str(x).lower() for x in items}


class DataEstimator:
    """
    Estimates which files will be read for a query and validates read access.
    Returns:
      {
        "STORAGE_TYPE": "<storage backend class name or identifier>",
        "BYTES_AFFECTED": <int>",
        "FILE_LIST": [<resolved_url_or_path>, ...]
      }
    """

    def __init__(self, super_name: str, organization: str, query: str):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.storage = self.super_table.storage
        self.parser = SQLParser(query)
        self.parser.parse_sql()

        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.catalog = RedisCatalog()

    # ----------------------- storage helpers (matching original) -----------------------

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
        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ("" if ":" in ep else "")
        return f"{host}{port}"

    def _detect_endpoint(self) -> Optional[str]:
        candidates = [
            "endpoint_url", "endpoint", "url", "api_url", "base_url",
            "s3_endpoint", "minio_endpoint", "public_endpoint",
        ]
        for name in candidates:
            val = self._storage_attr(name)
            if val:
                logger.debug(f"[estimate.env] storage.{name}='{val}'")
                return self._normalize_endpoint_for_s3(val)

        host = self._storage_attr("host", "hostname")
        port = self._storage_attr("port")
        if host:
            composed = f"{host}{':' + port if port else ''}"
            return self._normalize_endpoint_for_s3(composed)

        env_single = self._get_env(
            "AWS_S3_ENDPOINT_URL", "AWS_ENDPOINT_URL",
            "MINIO_ENDPOINT", "MINIO_URL", "MINIO_SERVER", "MINIO_ADDRESS",
            "MINIO_API_URL", "MINIO_PUBLIC_URL",
            "S3_ENDPOINT", "S3_ENDPOINT_URL", "S3_URL",
            "AWS_S3_ENDPOINT", "AWS_S3_URL",
        )
        if env_single:
            return self._normalize_endpoint_for_s3(env_single)

        host_env = self._get_env("MINIO_HOST", "S3_HOST", "AWS_S3_HOST")
        port_env = self._get_env("MINIO_PORT", "S3_PORT", "AWS_S3_PORT")
        if host_env:
            composed = f"{host_env}{':' + port_env if port_env else ''}"
            return self._normalize_endpoint_for_s3(composed)

        return None

    def _detect_bucket(self) -> Optional[str]:
        for name in ("bucket", "bucket_name", "default_bucket"):
            v = self._storage_attr(name)
            if v:
                return v
        return self._get_env("SUPERTABLE_BUCKET", "MINIO_BUCKET", "S3_BUCKET", "AWS_S3_BUCKET", "AWS_BUCKET", "BUCKET")

    def _detect_ssl(self) -> bool:
        val = (
            (str(getattr(self.storage, "secure", "")).lower() if hasattr(self.storage, "secure") else "")
            or (self._get_env("MINIO_SECURE", "S3_USE_SSL") or "")
        ).lower()
        return val in ("1", "true", "yes", "on")

    def _to_duckdb_path(self, key: str) -> str:
        """
        Resolve a storage key to a usable path for DuckDB.
        If SUPERTABLE_DUCKDB_PRESIGNED=1, presign with an **object key** (never pass a URL to presign).
        """
        if not key:
            return key

        # 1) Presign first if requested
        if (os.getenv("SUPERTABLE_DUCKDB_PRESIGNED", "") or "").lower() in ("1", "true", "yes", "on"):
            presign_fn = getattr(self.storage, "presign", None)
            if callable(presign_fn):
                try:
                    url = presign_fn(key)  # key, not URL
                    if isinstance(url, str) and url:
                        logger.debug(f"[estimate.resolve] presigned → {url[:96]}...")
                        return url
                except Exception as e:
                    logger.warning(f"[estimate.resolve] presign failed; falling back: {e}")

        # 2) If already URL, return as-is.
        if "://" in key:
            logger.debug(f"[estimate.resolve] already URL: {key}")
            return key

        # 3) storage helpers
        for attr in ("to_duckdb_path", "make_duckdb_url", "make_url"):
            fn = getattr(self.storage, attr, None)
            if callable(fn):
                try:
                    url = fn(key)  # key in, URL out (not presigned)
                    if isinstance(url, str) and url:
                        logger.info(f"[estimate.resolve] storage.{attr} → {url}")
                        return url
                except Exception as e:
                    logger.debug(f"[estimate.resolve] storage.{attr} failed: {e}")

        # 4) Construct URL from endpoint/bucket
        endpoint_raw = self._detect_endpoint()
        bucket = self._detect_bucket()
        use_http = (os.getenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "") or "").lower() in ("1", "true", "yes", "on")
        scheme = "https" if self._detect_ssl() else "http"
        key_norm = key.lstrip("/")

        if endpoint_raw and bucket:
            if use_http:
                return f"{scheme}://{endpoint_raw.rstrip('/')}/{bucket}/{key_norm}"
            else:
                return f"s3://{bucket}/{key_norm}"

        # 5) Fallback
        return key

    # ----------------------- snapshot discovery & filtering -----------------------

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

    def _filter_snapshots(self, snapshots: List[Dict]) -> List[Dict]:
        if self.super_table.super_name.lower() == self.parser.original_table.lower():
            return [s for s in snapshots if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))]
        return [s for s in snapshots if s["table_name"].lower() == self.parser.original_table.lower()]

    # ----------------------- main API -----------------------

    def estimate(self, user_hash: str, with_scan: bool = False) -> Dict[str, object]:
        """
        Returns a dict with keys: STORAGE_TYPE, BYTES_AFFECTED, FILE_LIST.
        Performs RBAC check and column validation.
        """
        self.timer = Timer()
        self.plan_stats = PlanStats()

        # Discover snapshots
        snapshots_all = self._collect_snapshots_from_redis()
        snapshots = self._filter_snapshots(snapshots_all)
        logger.debug(f"[estimate] snapshots post-filter={len(snapshots)}")
        self.timer.capture_and_reset_timing(event="META")

        parquet_files: List[str] = []
        reflection_file_size = 0
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
                parquet_files.append(resolved)
                reflection_file_size += int(resource.get("file_size", 0))

        # Validate requested columns
        missing_columns: Set[str] = set()
        if self.parser.columns_csv != "*":
            requested = _lower_set(self.parser.columns_list)
            missing_columns = requested - schema

        if len(snapshots) == 0 or missing_columns or not parquet_files:
            msg = (
                f"Missing column(s): {', '.join(sorted(missing_columns))}"
                if missing_columns
                else ("No parquet files found" if not parquet_files else "No snapshots found")
            )
            logger.warning(msg)
            raise RuntimeError(msg)

        # RBAC check before returning
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

        self.plan_stats.add_stat({"REFLECTIONS": len(parquet_files)})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})

        return {
            "STORAGE_TYPE": type(self.storage).__name__,
            "BYTES_AFFECTED": int(reflection_file_size),
            "FILE_LIST": parquet_files,
        }
