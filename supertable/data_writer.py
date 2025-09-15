import time
import uuid
from datetime import datetime
import re

import polars
from polars import DataFrame

from supertable.config.defaults import logger
from supertable.monitoring_logger import MonitoringLogger
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.processing import (
    process_overlapping_files,
    find_and_lock_overlapping_files,
)
from supertable.rbac.access_control import check_write_access


def _safe_release(lock_obj, name: str):
    """Best-effort lock release that won’t crash during interpreter shutdown."""
    if not lock_obj:
        return
    try:
        lock_obj.release_lock()
    except Exception as e:
        # Avoid noisy tracebacks on shutdown (e.g. builtins.open missing)
        logger.error(f"{name}: safe release failed: {e!s}")


class DataWriter:
    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name, organization)

    timer = Timer()

    #@timer
    def write(self, user_hash, simple_name, data, overwrite_columns, compression_level=1):
        """
        Writes an Arrow table into the target SimpleTable with overlap handling.

        Enhancements (logging & robustness):
        - Adds a per-call qid to correlate logs across threads/processes.
        - Rich DEBUG logs for each stage (access, convert, validate, snapshot, overlap, process, update, monitor).
        - Single INFO line at the end with timing breakdown and blue-highlighted total.
        - Graceful KeyboardInterrupt handling with safe lock release.
        """
        # ---- correlation id for this write ----
        qid = str(uuid.uuid4())
        lp = lambda msg: f"[write][qid={qid}][super={self.super_table.super_name}][table={simple_name}] {msg}"

        # ---- stage timing helpers ----
        t0 = time.time()
        t_last = t0
        timings = {}

        def mark(stage: str):
            nonlocal t_last
            now = time.time()
            timings[stage] = now - t_last
            t_last = now

        simple_table = None  # ensure visible in finally
        acquired_any_lock = False

        try:
            logger.debug(lp(f"➡️ Starting write(overwrite_cols={overwrite_columns}, compression={compression_level})"))

            # --- Access control ------------------------------------------------
            logger.debug(lp("Checking Write Access…"))
            check_write_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=simple_name,
            )
            logger.debug(lp("Write Access passed"))
            mark("access")

            # --- Convert input -------------------------------------------------
            logger.debug(lp("Converting Arrow input -> Polars DataFrame…"))
            dataframe: DataFrame = polars.from_arrow(data)
            logger.debug(lp(f"DataFrame shape: rows={dataframe.height}, cols={dataframe.width}"))
            mark("convert")

            # --- Validate ------------------------------------------------------
            logger.debug(lp("Validating input…"))
            self.validation(dataframe, simple_name, overwrite_columns)
            logger.debug(lp("Validation OK"))
            mark("validate")

            # --- Read last snapshot -------------------------------------------
            logger.debug(lp("Loading simple-table snapshot…"))
            simple_table = SimpleTable(self.super_table, simple_name)
            last_simple_table, _ = simple_table.get_simple_table_with_lock()
            logger.debug(lp(f"Snapshot loaded. Keys={list(last_simple_table.keys())}"))
            mark("snapshot")

            # --- Detect & lock overlaps ---------------------------------------
            logger.debug(lp("Finding & locking overlapping files…"))
            overlapping_files = find_and_lock_overlapping_files(
                last_simple_table, dataframe, overwrite_columns, simple_table.locking
            )
            acquired_any_lock = True
            logger.debug(lp(f"Locked {len(overlapping_files)} overlapping files"))
            mark("overlap")

            # --- Process overlaps / write data --------------------------------
            logger.debug(lp("Processing overlapping files (merge, filter, write)…"))
            inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_overlapping_files(
                dataframe,
                overlapping_files,
                overwrite_columns,
                simple_table.data_dir,
                compression_level,
            )
            logger.debug(
                lp(
                    f"Processed: inserted={inserted}, deleted={deleted}, "
                    f"rows={total_rows}, cols={total_columns}, "
                    f"new_files={len(new_resources)}, sunset_files={len(sunset_files)}"
                )
            )
            mark("process")

            # --- Update simple & super snapshots ------------------------------
            logger.debug(lp("Updating simple-table snapshot…"))
            new_simple_table_snapshot, new_simple_table_path = simple_table.update(
                new_resources, sunset_files, dataframe
            )
            logger.debug(lp(f"Simple-table snapshot updated at {new_simple_table_path}"))
            mark("update_simple")

            logger.debug(lp("Updating super-table meta…"))
            self.super_table.update_with_lock(
                simple_name, new_simple_table_path, new_simple_table_snapshot
            )
            logger.debug(lp("Super-table meta updated"))
            mark("update_super")

            # Release simple-table partition/file locks explicitly after successful update
            _safe_release(getattr(simple_table, "locking", None), name=simple_name)

            # --- Monitoring stats ---------------------------------------------
            stats = {
                "query_id": qid,  # correlate write metrics, too
                "recorded_at": datetime.utcnow().isoformat(),
                "super_name": self.super_table.super_name,
                "table_name": simple_name,
                "overwrite_columns": overwrite_columns,
                "inserted": inserted,
                "deleted": deleted,
                "total_rows": total_rows,
                "total_columns": total_columns,
                "new_resources": len(new_resources),
                "sunset_files": len(sunset_files),
                "duration": round(time.time() - t0, 6),
            }
            with MonitoringLogger(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                monitor_type="stats",
            ) as monitor:
                monitor.log_metric(stats)
            mark("monitor")

            # --- final summary -------------------------------------------------
            total_duration = time.time() - t0
            logger.debug(
                lp(
                    f"✅ write() complete: rows={total_rows}, cols={total_columns}, "
                    f"inserted={inserted}, deleted={deleted}, duration={total_duration:.3f}s"
                )
            )

            # Blue for the total number, then switch to dark green for the rest
            total_str = f"\033[94m{total_duration:.3f}\033[32m"
            logger.info(
                lp(
                    "Timing(s): "
                    f"total={total_str} | "
                    f"access={timings.get('access', 0):.3f} | "
                    f"convert={timings.get('convert', 0):.3f} | "
                    f"validate={timings.get('validate', 0):.3f} | "
                    f"snapshot={timings.get('snapshot', 0):.3f} | "
                    f"overlap={timings.get('overlap', 0):.3f} | "
                    f"process={timings.get('process', 0):.3f} | "
                    f"update_simple={timings.get('update_simple', 0):.3f} | "
                    f"update_super={timings.get('update_super', 0):.3f} | "
                    f"monitor={timings.get('monitor', 0):.3f}\033[0m"  # reset at the very end
                )
            )

            return total_columns, total_rows, inserted, deleted

        except KeyboardInterrupt:
            # Graceful stop: best-effort cleanup and return zeros, do not re-raise
            logger.warning(lp("⏹️ KeyboardInterrupt received — cleaning up locks gracefully…"))
            if simple_table and acquired_any_lock:
                _safe_release(simple_table.locking, name=simple_name)
            _safe_release(getattr(self.super_table, "locking", None), name=self.super_table.super_name)

            # INFO timing even on interrupt
            total_duration = time.time() - t0
            total_str = f"\033[94m{total_duration:.3f}\033[0m"
            logger.info(
                lp(
                    "Timing(s): "
                    f"total={total_str} | "
                    f"access={timings.get('access', 0):.3f} | "
                    f"convert={timings.get('convert', 0):.3f} | "
                    f"validate={timings.get('validate', 0):.3f} | "
                    f"snapshot={timings.get('snapshot', 0):.3f} | "
                    f"overlap={timings.get('overlap', 0):.3f} | "
                    f"process={timings.get('process', 0):.3f} | "
                    f"update_simple={timings.get('update_simple', 0):.3f} | "
                    f"update_super={timings.get('update_super', 0):.3f} | "
                    f"monitor={timings.get('monitor', 0):.3f} "
                )
            )
            return 0, 0, 0, 0

        except Exception as e:
            # On any failure, try to release locks before bubbling up
            logger.error(lp(f"write() failed: {e!s}"))
            if simple_table and acquired_any_lock:
                _safe_release(simple_table.locking, name=simple_name)
            _safe_release(getattr(self.super_table, "locking", None), name=self.super_table.super_name)
            raise

        finally:
            # Final safety net in case any lock survived; avoid noisy errors on teardown
            if simple_table:
                _safe_release(getattr(simple_table, "locking", None), name=f"{simple_name} (finalize)")
            _safe_release(getattr(self.super_table, "locking", None), name=f"{self.super_table.super_name} (finalize)")

    def validation(
        self, dataframe: DataFrame, simple_name: str, overwrite_columns: list
    ):
        if len(simple_name) == 0 or len(simple_name) > 128:
            raise ValueError("SimpleTable name can't be empty or longer than 128")

        if simple_name == self.super_table.super_name:
            raise ValueError("SimpleTable name can't match with SuperTable name")

        # Regular expression pattern for a valid table name
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
        if not re.match(pattern, simple_name):
            raise ValueError(
                f"Invalid table name: '{simple_name}'. Table names must start with a letter or underscore and contain only alphanumeric characters and underscores."
            )

        # Validate the overwrite columns
        if overwrite_columns and not all(
            col in dataframe.columns for col in overwrite_columns
        ):
            raise ValueError("Some overwrite columns are not present in the dataset")

        # Ensure overwrite_columns is a list
        if isinstance(overwrite_columns, str):
            raise ValueError("overwrite columns must be list")
