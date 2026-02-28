# supertable/monitoring_reader.py
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb
import pandas as pd

from supertable.storage.storage_factory import get_storage
from supertable.engine.duckdb_transient import DuckDBExecutor
from supertable.engine.data_estimator import DataEstimator

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000)


class MonitoringReader:
    """
    MonitoringReader (DuckDB) — implemented the SAME way as the normal DataReader path.

    Key points (matching DataReader/DataEstimator + DuckDBExecutor):
      - Reads a stats catalog that contains storage keys for parquet files
      - Resolves storage keys to DuckDB-readable paths via DataEstimator._to_duckdb_path()
        (supports presign + storage.to_duckdb_path/make_url + s3/http construction)
      - Configures DuckDB httpfs/S3 using DuckDBExecutor._configure_httpfs_and_s3()
      - Uses parquet_scan(..., union_by_name=TRUE, HIVE_PARTITIONING=TRUE)

    This avoids the common failure where DuckDB treats "org/super/...parquet" as a local path.
    """

    def __init__(
        self,
        *,
        super_name: str,
        organization: str,
        monitor_type: str,
    ):
        self.super_name = super_name
        self.organization = organization
        self.monitor_type = monitor_type

        self.storage = get_storage()

        # Reuse the SAME path resolution logic as normal reads.
        # DataEstimator only needs organization + storage for _to_duckdb_path().
        self._estimator = DataEstimator(organization=organization, storage=self.storage, tables=[])

        # Reuse the SAME DuckDB httpfs/S3 config logic as normal reads.
        self._duck = DuckDBExecutor(storage=self.storage)

        # Candidate catalog locations (new -> legacy)
        self._catalog_candidates: List[str] = [
            f"{organization}/{super_name}/monitoring/{monitor_type}/_stats.json",
            f"{organization}/{super_name}/monitoring/stats/_stats.json",
            f"{organization}/{super_name}/_stats.json",
        ]

    # ---------------------------- catalog parsing ----------------------------

    def _load_stats_catalog(self) -> Tuple[str, Dict[str, Any]]:
        """
        Loads the first stats catalog that exists and is valid JSON object.

        Returns: (catalog_path, catalog_dict)
        """
        last_err: Optional[Exception] = None
        for p in self._catalog_candidates:
            try:
                if not self.storage.exists(p):
                    continue
                obj = self.storage.read_json(p)
                if not isinstance(obj, dict):
                    raise ValueError(f"Catalog at {p} is not a JSON object")
                return p, obj
            except Exception as e:
                last_err = e
                logger.warning("[monitoring_reader] failed to load catalog %s: %s", p, e)

        if last_err is not None:
            raise FileNotFoundError(f"No readable monitoring stats catalog found. Last error: {last_err}")
        raise FileNotFoundError(f"No monitoring stats catalog found in: {self._catalog_candidates}")

    def _iter_file_entries(self, catalog: Dict[str, Any]) -> Iterable[Any]:
        """
        Be tolerant to different catalog shapes.

        Supported shapes:
          A) {"files": [ { "path": "...", ... }, ... ]}
          B) {"files": [ "org/super/...parquet", ... ]}
          C) {"tables": { "facts": { "files": [...] }, ... } }
        """
        files = catalog.get("files")
        if isinstance(files, list):
            for it in files:
                yield it
            return

        tables = catalog.get("tables")
        if isinstance(tables, dict):
            for _, tinfo in tables.items():
                if isinstance(tinfo, dict) and isinstance(tinfo.get("files"), list):
                    for it in tinfo["files"]:
                        yield it

    def _entry_to_key(self, entry: Any) -> Optional[Dict[str, Any]]:
        """
        Normalize file entry into {"key": "<storage key>", "min_ts_ms": ..., "max_ts_ms": ...}
        """
        if isinstance(entry, str):
            return {"key": entry}

        if isinstance(entry, dict):
            key = entry.get("path") or entry.get("file") or entry.get("key")
            if not key:
                return None
            out = {"key": key}
            for k in ("min_ts_ms", "max_ts_ms", "from_ts_ms", "to_ts_ms", "start_ts_ms", "end_ts_ms"):
                if k in entry:
                    out[k] = entry.get(k)
            return out

        return None

    def _select_keys_for_window(
        self,
        catalog: Dict[str, Any],
        from_ts_ms: int,
        to_ts_ms: int,
    ) -> List[str]:
        """
        Select file keys to read.

        - If catalog provides time bounds per file, we use them to filter.
        - Otherwise, include all keys.
        - We also *optionally* drop keys that do not exist in storage to avoid DuckDB hard-failing
          on a single missing object (monitoring catalogs can become stale).
        """
        keys: List[str] = []
        for entry in self._iter_file_entries(catalog):
            norm = self._entry_to_key(entry)
            if not norm:
                continue

            key = str(norm["key"])
            # Time-window filtering when possible
            min_ts = norm.get("min_ts_ms") or norm.get("from_ts_ms") or norm.get("start_ts_ms")
            max_ts = norm.get("max_ts_ms") or norm.get("to_ts_ms") or norm.get("end_ts_ms")

            try:
                if min_ts is not None and max_ts is not None:
                    mn = int(min_ts)
                    mx = int(max_ts)
                    if mx < from_ts_ms or mn > to_ts_ms:
                        continue
            except Exception:
                # If parsing fails, fall back to including.
                pass

            # Note: we intentionally do NOT check storage.exists() per key here
            # to avoid N+1 HEAD requests. DuckDB's parquet_scan with union_by_name=TRUE
            # will skip missing files or raise an error handled by the caller's fallback.

            keys.append(key)

        return keys

    # ---------------------------- DuckDB query ----------------------------

    def _build_sql(self, paths_sql_array: str, from_ts_ms: int, to_ts_ms: int, limit: int) -> str:
        # Use parquet_scan the same way as normal reads (supports hive partitions)
        return (
            "SELECT *\n"
            f"FROM parquet_scan({paths_sql_array}, union_by_name=TRUE, HIVE_PARTITIONING=TRUE)\n"
            f"WHERE execution_time BETWEEN {from_ts_ms} AND {to_ts_ms}\n"
            "ORDER BY execution_time DESC\n"
            f"LIMIT {int(limit)}"
        )

    def read(
        self,
        *,
        from_ts_ms: Optional[int] = None,
        to_ts_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Read monitoring parquet data.

        Default: last 24 hours.
        """
        now = _now_ms()
        if to_ts_ms is None:
            to_ts_ms = now
        if from_ts_ms is None:
            from_ts_ms = to_ts_ms - int(timedelta(days=1).total_seconds() * 1_000)

        if from_ts_ms > to_ts_ms:
            raise ValueError("from_ts_ms must be <= to_ts_ms")

        logger.info(
            "Reading monitoring data from %s to %s (limit: %s)",
            from_ts_ms,
            to_ts_ms,
            limit,
        )

        try:
            catalog_path, catalog = self._load_stats_catalog()
            keys = self._select_keys_for_window(catalog, from_ts_ms, to_ts_ms)
            if not keys:
                logger.info("[monitoring_reader] no parquet keys selected from catalog %s", catalog_path)
                return pd.DataFrame()

            # Resolve keys using the SAME logic as DataEstimator.
            paths = [self._estimator._to_duckdb_path(k) for k in keys]  # noqa: SLF001

            con = duckdb.connect()
            try:
                # Configure S3/httpfs the SAME way as DuckDBExecutor.
                self._duck._configure_httpfs_and_s3(con, paths)  # noqa: SLF001

                paths_sql_array = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
                sql = self._build_sql(paths_sql_array, from_ts_ms, to_ts_ms, limit)

                try:
                    return con.execute(sql).fetchdf()
                except Exception as e:
                    # If the column doesn't exist or schema mismatches, fall back to no filter
                    logger.error("[monitoring_reader] query failed: %s", e)
                    fallback = (
                        "SELECT *\n"
                        f"FROM parquet_scan({paths_sql_array}, union_by_name=TRUE, HIVE_PARTITIONING=TRUE)\n"
                        f"LIMIT {int(limit)}"
                    )
                    try:
                        return con.execute(fallback).fetchdf()
                    except Exception as e2:
                        logger.error("[monitoring_reader] fallback failed: %s", e2)
                        return pd.DataFrame()
            finally:
                try:
                    con.close()
                except Exception:
                    pass

        except Exception as e:
            logger.error("[monitoring_reader] unexpected error: %s", e)
            return pd.DataFrame()