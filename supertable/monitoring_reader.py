# supertable/monitoring_reader.py
import os
import json
import uuid
import duckdb
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
from supertable.storage.storage_factory import get_storage

logger = logging.getLogger(__name__)


class MonitoringReader:
    def __init__(
            self,
            super_name: str,
            organization: str,
            monitor_type: str
    ):
        self.super_name = super_name
        self.organization = organization
        self.monitor_type = monitor_type
        self.identity = "monitoring"

        # storage interface
        self.storage = get_storage()

        # temp directory for DuckDB
        self.temp_dir = os.path.join(
            self.organization,
            self.super_name,
            "tmp"
        )

        # Use the same stats file that MonitoringWriter creates
        self.stats_path = os.path.join(
            self.organization,
            self.super_name,
            "_stats.json"
        )

    def _load_stats_catalog(self) -> Dict[str, Any]:
        """Load the stats catalog that MonitoringWriter maintains."""
        if not self.storage.exists(self.stats_path):
            raise FileNotFoundError(f"Stats catalog not found: {self.stats_path}")

        try:
            catalog = self.storage.read_json(self.stats_path)
            logger.debug(
                f"Loaded stats catalog with {catalog.get('file_count', 0)} files, {catalog.get('row_count', 0)} rows")
            return catalog
        except Exception as e:
            logger.error(f"Error loading stats catalog: {e}")
            raise

    def _collect_parquet_files(
            self,
            catalog: Dict[str, Any],
            from_ts_ms: int,
            to_ts_ms: int
    ) -> List[str]:
        """
        Collect parquet files from the stats catalog.
        Since we don't have min/max stats for execution_time in the catalog,
        we include all files and let DuckDB filter during query.
        """
        files: List[str] = []
        for file_info in catalog.get("files", []):
            file_path = file_info.get("path") or file_info.get("file")
            if file_path and self.storage.exists(file_path):
                files.append(file_path)
            else:
                logger.warning(f"Parquet file not found or invalid: {file_path}")

        if not files:
            logger.warning("No parquet files found in stats catalog")

        logger.debug(f"Collected {len(files)} parquet files for time range [{from_ts_ms}, {to_ts_ms}]")
        return files

    def _build_query(
            self,
            parquet_files_sql_array: str,
            from_ts_ms: int,
            to_ts_ms: int,
            limit: int
    ) -> str:
        return (
            "SELECT *\n"
            f"FROM read_parquet({parquet_files_sql_array}, union_by_name=TRUE, filename=TRUE)\n"
            f"WHERE execution_time BETWEEN {from_ts_ms} AND {to_ts_ms}\n"
            "ORDER BY execution_time DESC\n"
            f"LIMIT {limit}"
        )

    def read(
            self,
            from_ts_ms: Optional[int] = None,
            to_ts_ms: Optional[int] = None,
            limit: int = 1000
    ) -> pd.DataFrame:
        """
        Read monitoring data within the specified time range.

        Args:
            from_ts_ms: Start timestamp in milliseconds (inclusive)
            to_ts_ms: End timestamp in milliseconds (inclusive)
            limit: Maximum number of rows to return

        Returns:
            pandas.DataFrame with monitoring data
        """
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1_000)
        if to_ts_ms is None:
            to_ts_ms = now_ms
        if from_ts_ms is None:
            from_ts_ms = to_ts_ms - int(timedelta(days=1).total_seconds() * 1_000)
        if from_ts_ms > to_ts_ms:
            raise ValueError(f"from_ts_ms ({from_ts_ms}) must be <= to_ts_ms ({to_ts_ms})")

        logger.info(f"Reading monitoring data from {from_ts_ms} to {to_ts_ms} (limit: {limit})")

        try:
            catalog = self._load_stats_catalog()
            parquet_files = self._collect_parquet_files(catalog, from_ts_ms, to_ts_ms)

            if not parquet_files:
                logger.warning("No parquet files available for reading")
                return pd.DataFrame()

            # Create temporary directory for DuckDB if it doesn't exist
            try:
                if not self.storage.exists(self.temp_dir):
                    self.storage.makedirs(self.temp_dir)
            except Exception as e:
                logger.warning(f"Could not create temp directory {self.temp_dir}: {e}")

            con = duckdb.connect()
            con.execute("PRAGMA memory_limit='2GB';")
            if self.storage.exists(self.temp_dir):
                con.execute(f"PRAGMA temp_directory='{self.temp_dir}';")
            con.execute("PRAGMA default_collation='nocase';")

            # Use read_parquet instead of parquet_scan for better compatibility
            files_sql_array = "[" + ", ".join(f"'{f}'" for f in parquet_files) + "]"
            query = self._build_query(files_sql_array, from_ts_ms, to_ts_ms, limit)
            logger.debug("Executing DuckDB query:\n%s", query)

            try:
                df = con.execute(query).fetchdf()
                logger.info(f"Successfully fetched {len(df)} rows from monitoring data")
                return df
            except Exception as e:
                logger.error(f"Error executing monitoring query: {e}")
                # Try fallback approach without time filtering
                try:
                    fallback_query = (
                        "SELECT *\n"
                        f"FROM read_parquet({files_sql_array}, union_by_name=TRUE, filename=TRUE)\n"
                        f"LIMIT {limit}"
                    )
                    logger.debug("Trying fallback query without time filter")
                    df = con.execute(fallback_query).fetchdf()
                    logger.info(f"Fallback successful: fetched {len(df)} rows")
                    return df
                except Exception as fallback_error:
                    logger.error(f"Fallback query also failed: {fallback_error}")
                    raise e
            finally:
                try:
                    con.close()
                except Exception:
                    pass

        except FileNotFoundError as e:
            logger.error(f"Monitoring data not available: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error reading monitoring data: {e}")
            return pd.DataFrame()