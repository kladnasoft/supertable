# supertable/data_estimator.py

from __future__ import annotations

import os
from collections import defaultdict
from typing import Iterable, Set, List, Dict, Optional, Tuple
from urllib.parse import urlparse

from supertable.config.defaults import logger
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.super_table import SuperTable
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.rbac.access_control import restrict_read_access
from supertable.redis_catalog import RedisCatalog  # Redis leaf pointers for snapshots

from supertable.utils.sql_parser import TableDefinition

def _lower_set(items: Iterable[str]) -> Set[str]:
    return {str(x).lower() for x in items}


from typing import Dict, List, Optional, Set, Tuple


def get_missing_columns(
    tables: List[TableDefinition],
    selected: List[SuperSnapshot],
) -> List[Tuple[str, str, Set[str]]]:
    """
    Returns list of (super_name, simple_name, missing_columns),
    but only for tables where at least one requested column is missing.

    Semantics (updated):

      - `tables` (TableDefinition):
          * Represents what the query requests (from SQLParser).
          * Match key: (super_name, simple_name), case-insensitive.
          * columns == []  => SELECT * / t.*  => all columns requested,
                                but we DO NOT validate -> skip missing-check.
          * columns != [] => explicit requested columns that MUST exist.

      - `selected` (SuperSnapshot):
          * Represents what is actually available for that table/version.
          * columns: Set[str] of available columns.
          * Multiple snapshots for same table:
                union their columns.

      - Missing logic:
          * If TableDefinition.columns == []:
                - No validation (treated as "don't check SELECT *").
          * Else:
                - If there is no matching SuperSnapshot:
                      all requested columns are missing.
                - If there is a match:
                      missing = requested - available (case-insensitive).
          * Only tables with non-empty missing set are returned.
    """

    # Build availability index from selected snapshots:
    #   (super_name.lower(), simple_name.lower()) -> set(lowercase columns)
    available_index: Dict[Tuple[str, str], Set[str]] = {}

    for s in selected:
        key = (s.super_name.lower(), s.simple_name.lower())
        if key not in available_index:
            available_index[key] = set()
        # s.columns is a Set[str]; guard if it's empty/None
        for c in (s.columns or []):
            available_index[key].add(c.lower())

    results: List[Tuple[str, str, Set[str]]] = []

    # Check each requested table definition
    for t in tables:
        key = (t.super_name.lower(), t.simple_name.lower())

        # [] means SELECT * (or t.*) -> all columns requested,
        # but as per requirement: do NOT validate in this function.
        if not t.columns:
            continue

        requested_lower = {c.lower() for c in t.columns}
        available_lower = available_index.get(key)

        if available_lower is None:
            # No snapshot for this table -> everything requested is missing
            missing_lower = requested_lower
        else:
            # Only columns that are requested but not present
            missing_lower = requested_lower - available_lower

        if missing_lower:
            # Preserve original casing for reporting
            missing_original = {c for c in t.columns if c.lower() in missing_lower}
            if missing_original:
                results.append((t.super_name, t.simple_name, missing_original))

    return results



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

    def __init__(self, organization: str, storage, tables: List[TableDefinition]):
        self.organization = organization
        self.storage = storage
        self.tables = tables
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

    def _collect_snapshots_from_redis(self, organization, super_name) -> List[Dict]:
        items = list(self.catalog.scan_leaf_items(organization, super_name, count=512))
        snapshots = []
        for it in items:
            if not it.get("path"):
                continue
            snapshots.append(
                {
                    "table_name": it["simple"],
                    "last_updated_ms": int(it.get("ts", 0)),
                    "path": it["path"],
                    "version": it['version']
                }
            )
        return snapshots

    def _filter_snapshots(self, super_name, simple_name, snapshots: List[Dict]) -> List[Dict]:
        if super_name.lower() == simple_name.lower():
            return [s for s in snapshots if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))]
        return [s for s in snapshots if s["table_name"].lower() == simple_name.lower()]

    def _get_supertable_map(self) -> List[Tuple[str, List[str]]]:
        grouped = defaultdict(list)

        for t in self.tables:  # t: TableDefinition
            grouped[t.super_name].append(t.simple_name)

        # optional: sort simple_names per supertable
        return [
            (super_name, sorted(simple_names))
            for super_name, simple_names in grouped.items()
        ]

    # ----------------------- main API -----------------------
    def estimate(self) -> Reflection:
        """
        Returns a dict with keys: STORAGE_TYPE, BYTES_AFFECTED, FILE_LIST.
        Performs RBAC check and column validation.
        """
        self.timer = Timer()
        self.plan_stats = PlanStats()

        supers: List[SuperSnapshot] = []
        reflection_file_size = 0

        super_map = self._get_supertable_map()

        # Discover snapshots
        for super_name, tables in super_map:
            for simple_name in tables:
                snapshots = self._collect_snapshots_from_redis(organization=self.organization, super_name=super_name)
                snapshots = self._filter_snapshots(super_name, simple_name, snapshots)
                super_table = SuperTable(super_name, self.organization)

                parquet_files: List[str] = []
                schema: Set[str] = set()

                for snapshot in snapshots:
                    current_snapshot_path = snapshot["path"]
                    current_snapshot_data = super_table.read_simple_table_snapshot(current_snapshot_path)

                    current_version = current_snapshot_data.get("snapshot_version", 0)
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

                    super_snapshot = SuperSnapshot(super_name=super_name, simple_name=simple_name, simple_version=current_version, files=parquet_files, columns=schema)
                    supers.append(super_snapshot)

        # Validate requested columns
        missing_info = get_missing_columns(self.tables, supers)

        # Total parquet files across all selected snapshots
        total_reflections = sum(len(s.files) for s in supers)

        # Ensure every selected snapshot has at least one file
        all_have_files = all(bool(s.files) for s in supers)

        if not supers or missing_info or not all_have_files:
            if not supers:
                msg = "No snapshots selected."
            elif missing_info:
                # missing_info: List[(super_name, table_name, Set[missing_cols])]
                details = []
                for super_name, table_name, cols in missing_info:
                    cols_str = ", ".join(sorted(cols))
                    details.append(f"{super_name}.{table_name}: {cols_str}")
                msg = "Missing required column(s): " + " | ".join(details)
            else:  # not all_have_files
                msg = "No parquet files found for one or more selected tables."

            logger.warning(msg)
            raise RuntimeError(msg)

        self.timer.capture_and_reset_timing(event="ESTIMATE")

        self.plan_stats.add_stat({"REFLECTIONS": total_reflections})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})

        return Reflection(
            storage_type=type(self.storage).__name__,
            reflection_bytes=int(reflection_file_size),
            total_reflections=total_reflections,
            supers=supers,
        )
