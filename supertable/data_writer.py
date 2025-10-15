# supertable/data_writer.py
from __future__ import annotations

import time
import uuid
from datetime import datetime
import re

import polars
from polars import DataFrame

from supertable.config.defaults import logger
from supertable.monitoring_writer import get_monitoring_logger  # async monitoring (enqueue only)
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.processing import (
    process_overlapping_files,
    find_and_lock_overlapping_files,
)
from supertable.rbac.access_control import check_write_access  # noqa: F401
from supertable.redis_catalog import RedisCatalog
from supertable.mirroring.mirror_formats import MirrorFormats


class DataWriter:
    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name, organization)
        self.catalog = RedisCatalog()

    timer = Timer()

    def write(self, user_hash, simple_name, data, overwrite_columns, compression_level=1):
        """
        Writes an Arrow table into the target SimpleTable with overlap handling.

        Monitoring is fully decoupled: we only enqueue a metric AFTER the lock is released.
        """
        qid = str(uuid.uuid4())
        lp = lambda msg: f"[write][qid={qid}][super={self.super_table.super_name}][table={simple_name}] {msg}"

        t0 = time.time()
        t_last = t0
        timings = {}

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        token = None
        result_tuple = None
        stats_payload = None

        try:
            logger.debug(lp(f"➡️ Starting write(overwrite_cols={overwrite_columns}, compression={compression_level})"))

            # --- Access control ------------------------------------------------
            check_write_access(
                 super_name=self.super_table.super_name,
                 organization=self.super_table.organization,
                 user_hash=user_hash,
                 table_name=simple_name,
            )
            mark("access")

            # --- Convert input -------------------------------------------------
            dataframe: DataFrame = polars.from_arrow(data)
            mark("convert")

            # --- Validate ------------------------------------------------------
            self.validation(dataframe, simple_name, overwrite_columns)
            mark("validate")

            # --- Per-simple Redis lock ----------------------------------------
            token = self.catalog.acquire_simple_lock(
                self.super_table.organization, self.super_table.super_name, simple_name,
                ttl_s=30, timeout_s=60
            )
            if not token:
                raise TimeoutError(f"Could not acquire lock for simple '{simple_name}'")
            mark("lock")

            # --- Read last snapshot (via leaf pointer) ------------------------
            simple_table = SimpleTable(self.super_table, simple_name)
            last_simple_table, _ = simple_table.get_simple_table_snapshot()
            mark("snapshot")

            # --- Detect overlaps ----------------------------------------------
            overlapping_files = find_and_lock_overlapping_files(
                last_simple_table, dataframe, overwrite_columns, locking=None
            )
            mark("overlap")

            # --- Process & write data -----------------------------------------
            inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_overlapping_files(
                dataframe,
                overlapping_files,
                overwrite_columns,
                simple_table.data_dir,
                compression_level,
            )
            mark("process")

            # --- Update snapshot on storage -----------------------------------
            new_snapshot_dict, new_snapshot_path = simple_table.update(
                new_resources, sunset_files, dataframe
            )
            mark("update_simple")

            # --- CAS set leaf pointer + atomic root bump ----------------------
            self.catalog.set_leaf_path_cas(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
                new_snapshot_path,
                now_ms=int(datetime.now().timestamp() * 1000),
            )
            self.catalog.bump_root(self.super_table.organization, self.super_table.super_name)
            mark("bump_root")

            # --- Optional mirroring -------------------------------------------
            try:
                MirrorFormats.mirror_if_enabled(
                    super_table=self.super_table,
                    table_name=simple_name,
                    simple_snapshot=new_snapshot_dict,
                )
            except Exception as e:
                logger.error(lp(f"mirroring failed: {e}"))
            mark("mirror")

            # Prepare monitoring payload (NOT writing, only enqueue later)
            stats_payload = {
                "query_id": qid,
                "recorded_at": datetime.utcnow().isoformat(),
                "super_name": self.super_table.super_name,
                "table_name": simple_name,               # partitioning by table_name in monitor
                "overwrite_columns": overwrite_columns,
                "inserted": inserted,
                "deleted": deleted,
                "total_rows": total_rows,
                "total_columns": total_columns,
                "new_resources": len(new_resources),
                "sunset_files": len(sunset_files),
                "duration": round(time.time() - t0, 6),
            }
            mark("prepare_monitor")

            total_duration = time.time() - t0
            logger.info(
                lp(
                    "Timing(s): "
                    f"total={total_duration:.3f} | "
                    f"convert={timings.get('convert', 0):.3f} | validate={timings.get('validate', 0):.3f} | "
                    f"lock={timings.get('lock', 0):.3f} | snapshot={timings.get('snapshot', 0):.3f} | "
                    f"overlap={timings.get('overlap', 0):.3f} | process={timings.get('process', 0):.3f} | "
                    f"update_simple={timings.get('update_simple', 0):.3f} | bump_root={timings.get('bump_root', 0):.3f} | "
                    f"mirror={timings.get('mirror', 0):.3f} | prepare_monitor={timings.get('prepare_monitor', 0):.3f}"
                )
            )

            result_tuple = (total_columns, total_rows, inserted, deleted)

        except Exception as e:
            logger.error(lp(f"write() failed: {e!s}"))
            raise
        finally:
            # Release per-simple lock first
            if token:
                try:
                    ok = self.catalog.release_simple_lock(
                        self.super_table.organization, self.super_table.super_name, simple_name, token
                    )
                    if not ok:
                        logger.debug(lp("Lock release skipped (token mismatch or already expired)."))
                except Exception:
                    pass

        # ---------- LOCK IS RELEASED HERE ----------
        # Monitoring enqueue is fully outside any data locks
        try:
            if stats_payload is not None:
                monitor = get_monitoring_logger(
                    super_name=self.super_table.super_name,
                    organization=self.super_table.organization,
                    monitor_type="stats",
                )
                monitor.log_metric(stats_payload)  # enqueue only
        except Exception as me:
            logger.error(lp(f"monitoring enqueue failed: {me}"))

        return result_tuple

    def validation(self, dataframe: DataFrame, simple_name: str, overwrite_columns: list):
        if len(simple_name) == 0 or len(simple_name) > 128:
            raise ValueError("SimpleTable name can't be empty or longer than 128")
        if simple_name == self.super_table.super_name:
            raise ValueError("SimpleTable name can't match with SuperTable name")
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
        if not re.match(pattern, simple_name):
            raise ValueError(
                f"Invalid table name: '{simple_name}'. "
                "Table names must start with a letter/underscore and contain only alphanumeric/underscore characters."
            )
        if overwrite_columns and not all(col in dataframe.columns for col in overwrite_columns):
            raise ValueError("Some overwrite columns are not present in the dataset")
        if isinstance(overwrite_columns, str):
            raise ValueError("overwrite columns must be list")
