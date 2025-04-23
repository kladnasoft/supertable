import os
import json
import uuid
import duckdb
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import pandas as pd
from supertable.storage.storage_factory import get_storage
from supertable.super_table import SuperTable

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

        # paths for snapshots and catalog
        self.base_dir = os.path.join(
            self.organization,
            self.super_name,
            self.identity,
            self.monitor_type
        )
        self.catalog_path = os.path.join(
            self.organization,
            self.super_name,
            f"_{self.monitor_type}.json"
        )

    def _load_current_snapshot(self) -> dict:
        """Use storage to load the latest snapshot JSON as dict."""
        if not self.storage.exists(self.catalog_path):
            raise FileNotFoundError(f"Catalog not found: {self.catalog_path}")
        catalog = self.storage.read_json(self.catalog_path)
        snapshot_path = catalog.get("current")
        if not snapshot_path or not self.storage.exists(snapshot_path):
            raise FileNotFoundError(f"No current snapshot at {snapshot_path}")
        return self.storage.read_json(snapshot_path)

    def _collect_parquet_files(
        self,
        snapshot: dict,
        to_ts_ms: int
    ) -> List[str]:
        """
        Collect files whose data may include timestamps <= to_ts_ms.
        """
        files: List[str] = []
        for res in snapshot.get("resources", []):
            stats = res.get("stats", {}).get("execution_time", {})
            min_ts = stats.get("min", float('-inf'))
            if min_ts <= to_ts_ms:
                files.append(res["file"])
        if not files:
            logger.warning("No parquet files found before %d", to_ts_ms)
        return files

    def _generate_table_name(self) -> str:
        return f"monitoring_{uuid.uuid4().hex[:16]}"

    def _build_query(
        self,
        table_name: str,
        from_ts_ms: int,
        to_ts_ms: int,
        limit: int
    ) -> str:
        """
        Build a SQL query filtering execution_time between from_ts_ms and to_ts_ms.
        """
        return (
            f"SELECT *\n"
            f"FROM {table_name}\n"
            f"WHERE execution_time BETWEEN {from_ts_ms} AND {to_ts_ms}\n"
            f"ORDER BY execution_time DESC\n"
            f"LIMIT {limit}"
        )

    def read(
        self,
        from_ts_ms: Optional[int] = None,
        to_ts_ms: Optional[int] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Read rows whose execution_time falls in [from_ts_ms, to_ts_ms].
        Defaults: to_ts_ms=now, from_ts_ms=now-1day.
        Returns a Pandas DataFrame.
        """
        # determine time window
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1_000)
        if to_ts_ms is None:
            to_ts_ms = now_ms
        if from_ts_ms is None:
            from_ts_ms = to_ts_ms - int(timedelta(days=1).total_seconds() * 1_000)
        if from_ts_ms > to_ts_ms:
            raise ValueError(
                f"from_ts_ms ({from_ts_ms}) must be <= to_ts_ms ({to_ts_ms})"
            )

        # load snapshot & collect files
        snapshot = self._load_current_snapshot()
        parquet_files = self._collect_parquet_files(snapshot, to_ts_ms)
        if not parquet_files:
            return pd.DataFrame()

        # setup DuckDB
        con = duckdb.connect()
        con.execute("PRAGMA memory_limit='2GB';")
        con.execute(f"PRAGMA temp_directory='{self.temp_dir}';")
        con.execute("PRAGMA default_collation='nocase';")

        # ingest parquet files
        table_name = self._generate_table_name()
        files_list = ", ".join(f"'{f}'" for f in parquet_files)
        create_sql = (
            f"CREATE TABLE {table_name} AS\n"
            f"  SELECT *\n"
            f"  FROM parquet_scan([{files_list}],\n"
            f"                       union_by_name=TRUE,\n"
            f"                       HIVE_PARTITIONING=TRUE);"
        )
        try:
            con.execute(create_sql)
        except Exception as e:
            logger.error("Error creating table:\n%s\n%s", create_sql, e)
            raise

        # build and execute range query
        query = self._build_query(table_name, from_ts_ms, to_ts_ms, limit)
        logger.debug("Executing Query:\n%s", query)
        df = con.execute(query).fetchdf()
        logger.debug("Result shape: %s", df.shape)
        return df
