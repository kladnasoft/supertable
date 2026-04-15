# route: supertable.services.gc_scheduler
"""
Background scheduler for Garbage Collection of orphaned Parquet files.

Runs as a daemon thread inside the FastAPI process — no external service needed.
Reads the GC cron schedule from Redis and executes ``clean_obsolete_files``
for each table when the cron interval fires.

Protection layers (same pattern as the DQ scheduler):

  1. LOCK — ``clean_obsolete_files`` already acquires a per-table Redis lock.
     Only one GC run can happen per table at a time.

  2. COOLDOWN — after a successful GC run, a cooldown key prevents re-running
     the same table for a configurable period (default 1 hour).

  3. RUNNING — a per-table "gc:running" key (with TTL) prevents overlapping
     runs even if the lock is released early due to a crash.

Redis keys used:

  supertable:{org}:{sup}:gc:schedule           — schedule config (set by API)
  supertable:{org}:{sup}:gc:running:{table}    — SET NX by scheduler, TTL 10 min
  supertable:{org}:{sup}:gc:cooldown:{table}   — SET after clean, TTL configurable
  supertable:{org}:{sup}:gc:last_run           — JSON: last run summary
"""
from __future__ import annotations

import json
import logging
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

DEFAULT_COOLDOWN_SECONDS = 3600      # 1 hour between GC runs on same table
DEFAULT_RUNNING_TTL_SECONDS = 600    # safety: running lock expires after 10 min
TICK_INTERVAL_SECONDS = 60           # scheduler wakes up every 60 s

# Singleton state
_gc_thread: Optional[threading.Thread] = None
_gc_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────
# Public API — start the scheduler
# ──────────────────────────────────────────────────────────────────────

def start_gc_scheduler() -> None:
    """Start the GC background scheduler (idempotent — safe to call multiple times)."""
    global _gc_thread
    with _gc_lock:
        if _gc_thread is not None and _gc_thread.is_alive():
            return
        _gc_thread = threading.Thread(
            target=_scheduler_loop,
            name="gc-scheduler",
            daemon=True,
        )
        _gc_thread.start()
        logger.info("[gc-scheduler] Background thread started")


# ──────────────────────────────────────────────────────────────────────
# Scheduler loop
# ──────────────────────────────────────────────────────────────────────

def _scheduler_loop() -> None:
    """Main loop — checks every 60 s if the GC cron is due."""
    # Wait for the app to fully initialize
    time.sleep(15)

    last_run: Dict[str, float] = {}  # "org:sup" -> epoch of last GC cycle

    while True:
        try:
            _scheduler_tick(last_run)
        except Exception:
            logger.error(f"[gc-scheduler] Error in tick:\n{traceback.format_exc()}")
        time.sleep(TICK_INTERVAL_SECONDS)


def _scheduler_tick(last_run: Dict[str, float]) -> None:
    """Single scheduler tick — run GC for each org:sup whose cron is due."""
    try:
        from supertable.redis_connector import create_redis_client
    except ImportError:
        logger.debug("[gc-scheduler] Dependencies not available yet, skipping tick")
        return

    try:
        r = create_redis_client()
    except Exception as e:
        logger.warning(f"[gc-scheduler] Cannot connect to Redis: {e}")
        return

    pairs = _discover_pairs(r)
    if not pairs:
        return

    now = time.time()

    for org, sup in pairs:
        schedule = _get_schedule(r, org, sup)
        if not schedule.get("enabled", False):
            continue

        cron_expr = schedule.get("cron", "0 2 * * *")
        interval = _cron_to_seconds(cron_expr)
        pair_key = f"{org}:{sup}"

        last = last_run.get(pair_key, 0)
        if now - last < interval:
            continue

        # Time to run GC for this org:sup
        all_tables = schedule.get("all_tables", True)
        tables = _list_tables(r, org, sup) if all_tables else []

        if not tables:
            continue

        run_summary = {
            "started_at": _now_iso(),
            "tables_processed": 0,
            "total_data_deleted": 0,
            "total_snapshots_deleted": 0,
            "errors": [],
        }

        for table_name in tables:
            try:
                _run_gc_for_table(r, org, sup, table_name, run_summary)
            except Exception as e:
                run_summary["errors"].append(f"{table_name}: {e}")
                logger.error(
                    f"[gc-scheduler] Failed for {org}/{sup}/{table_name}:\n"
                    f"{traceback.format_exc()}"
                )

        run_summary["finished_at"] = _now_iso()
        last_run[pair_key] = now

        # Store last run summary in Redis for observability
        try:
            summary_key = f"supertable:{org}:{sup}:gc:last_run"
            r.set(summary_key, json.dumps(run_summary), ex=7 * 86400)  # TTL 7 days
        except Exception:
            pass

        logger.info(
            f"[gc-scheduler] Cycle done for {org}/{sup}: "
            f"tables={run_summary['tables_processed']}, "
            f"data_deleted={run_summary['total_data_deleted']}, "
            f"snap_deleted={run_summary['total_snapshots_deleted']}, "
            f"errors={len(run_summary['errors'])}"
        )


def _run_gc_for_table(
    r, org: str, sup: str, table_name: str, summary: Dict[str, Any],
) -> None:
    """Run GC for a single table with running-lock and cooldown protection."""

    # Check cooldown
    cooldown_key = f"supertable:{org}:{sup}:gc:cooldown:{table_name}"
    if r.exists(cooldown_key):
        return

    # Acquire running lock (SET NX with TTL)
    running_key = f"supertable:{org}:{sup}:gc:running:{table_name}"
    if not r.set(running_key, _now_iso(), nx=True, ex=DEFAULT_RUNNING_TTL_SECONDS):
        logger.debug(f"[gc-scheduler] Skipping {table_name}: already running")
        return

    try:
        from supertable.services.gc import clean_obsolete_files

        result = clean_obsolete_files(
            organization=org,
            super_name=sup,
            simple_name=table_name,
            role_name="superadmin",
        )

        data_deleted = result.get("deleted_data_files", 0)
        snap_deleted = result.get("deleted_snapshot_files", 0)
        errors = result.get("errors", [])

        summary["tables_processed"] += 1
        summary["total_data_deleted"] += data_deleted
        summary["total_snapshots_deleted"] += snap_deleted
        summary["errors"].extend(errors)

        if data_deleted > 0 or snap_deleted > 0:
            logger.info(
                f"[gc-scheduler] Cleaned {table_name}: "
                f"data={data_deleted}, snapshots={snap_deleted}"
            )

        # Set cooldown
        r.set(cooldown_key, _now_iso(), ex=DEFAULT_COOLDOWN_SECONDS)

    finally:
        # Always release running lock
        try:
            r.delete(running_key)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _get_schedule(r, org: str, sup: str) -> Dict[str, Any]:
    """Read GC schedule from Redis (same format as the API)."""
    key = f"supertable:{org}:{sup}:gc:schedule"
    try:
        raw = r.get(key)
        if raw:
            return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        pass
    return {"enabled": False, "cron": "0 2 * * *", "all_tables": True}


def _discover_pairs(r) -> List[Tuple[str, str]]:
    """Find all org:sup pairs that have root meta keys."""
    pairs = []
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match="supertable:*:*:meta:root", count=500)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                parts = k.split(":")
                if len(parts) >= 5:
                    pairs.append((parts[1], parts[2]))
            if cursor == 0:
                break
    except Exception as e:
        logger.warning(f"[gc-scheduler] discover pairs failed: {e}")
    return list(set(pairs))


def _list_tables(r, org: str, sup: str) -> List[str]:
    """List all table names for an org:sup (excludes system tables)."""
    tables = []
    try:
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:", 1)[-1]
                if simple and not simple.startswith("__"):
                    tables.append(simple)
            if cursor == 0:
                break
    except Exception as e:
        logger.warning(f"[gc-scheduler] list_tables failed: {e}")
    return sorted(set(tables))


def _cron_to_seconds(cron_expr: str) -> int:
    """Simplified cron-to-interval converter.

    Handles common patterns: ``*/N`` hours, daily, etc.
    Falls back to 24 hours for complex expressions.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return 24 * 3600

    minute, hour, dom, month, dow = parts

    if hour.startswith("*/"):
        try:
            return int(hour[2:]) * 3600
        except ValueError:
            pass

    if minute.startswith("*/"):
        try:
            return int(minute[2:]) * 60
        except ValueError:
            pass

    # Fixed hour, wildcard day/month/dow → daily
    if dom == "*" and month == "*" and dow == "*" and not hour.startswith("*"):
        return 24 * 3600

    if hour == "*/1":
        return 3600

    return 24 * 3600


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
