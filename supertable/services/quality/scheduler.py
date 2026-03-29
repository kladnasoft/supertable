# route: supertable.services.quality.scheduler
# supertable/reflection/quality/scheduler.py
"""
Lightweight background scheduler for Data Quality checks.

Runs as a daemon thread inside the FastAPI process — no external service needed.
Supports cron expressions for quick and deep profiles.

Post-ingest trigger uses a three-layer protection:

  1. DEBOUNCE — ingest sets a Redis "pending" key (not immediate execution).
     The scheduler loop picks it up on the next tick.  100 ingests in 5 min
     → 1 pending key → 1 quality check.

  2. LOCK — only one check can run per table at a time.
     If a check is already running, the pending flag stays for the next tick.

  3. COOLDOWN — after a check completes, a cooldown key is set (default 5 min).
     No new check can start for that table until cooldown expires.
     This prevents rapid re-triggering even if ingests keep coming.

Redis keys used:

  supertable:{org}:{sup}:dq:pending:{table}   — SET by ingest, TTL 10 min
  supertable:{org}:{sup}:dq:running:{table}    — SET NX by scheduler, TTL 5 min (safety)
  supertable:{org}:{sup}:dq:cooldown:{table}   — SET by scheduler after check, TTL configurable
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

DEFAULT_COOLDOWN_SECONDS = 300       # 5 minutes between checks on same table
DEFAULT_PENDING_TTL_SECONDS = 600    # pending flag expires after 10 min
DEFAULT_RUNNING_TTL_SECONDS = 300    # safety: running lock expires after 5 min
TICK_INTERVAL_SECONDS = 60           # scheduler wakes up every 60s

# Singleton state
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────
# Public API — called from ingest code
# ──────────────────────────────────────────────────────────────────────

def notify_ingest(r, org: str, sup: str, table_name: str) -> None:
    """
    Called by the ingest/write path after data is loaded into a table.

    Does NOT run a quality check — just sets a "pending" flag in Redis.
    The scheduler loop will pick it up on the next tick, respecting
    debounce, lock, and cooldown.

    This is safe to call at any frequency — 1000 calls/sec will still
    result in at most 1 quality check per cooldown period.
    """
    try:
        from supertable.services.quality.config import DQConfig
        dqc = DQConfig(r, org, sup)
        schedule = dqc.get_schedule()
        if not schedule.get("post_ingest", True):
            return
        if not schedule.get("enabled", True):
            return

        key = _pending_key(org, sup, table_name)
        # SET with TTL — overwrites previous pending (debounce reset)
        r.set(key, _now_iso(), ex=DEFAULT_PENDING_TTL_SECONDS)
        logger.debug(f"[dq-ingest] Pending flag set for {org}/{sup}/{table_name}")
    except Exception as e:
        # Never fail the ingest path due to quality scheduling
        logger.warning(f"[dq-ingest] Failed to set pending flag: {e}")


# ──────────────────────────────────────────────────────────────────────
# Scheduler thread
# ──────────────────────────────────────────────────────────────────────


def _scheduler_loop() -> None:
    """Main loop — checks every 60s if any cron expression is due."""
    # Wait for the app to fully initialize
    time.sleep(10)

    last_quick_run: Dict[str, float] = {}  # "org:sup:table" -> epoch
    last_deep_run: Dict[str, float] = {}
    last_custom_run: Dict[str, float] = {}

    while True:
        try:
            _scheduler_tick(last_quick_run, last_deep_run, last_custom_run)
        except Exception:
            logger.error(f"[dq-scheduler] Error in tick:\n{traceback.format_exc()}")
        time.sleep(TICK_INTERVAL_SECONDS)


def _scheduler_tick(
    last_quick_run: Dict[str, float],
    last_deep_run: Dict[str, float],
    last_custom_run: Dict[str, float],
) -> None:
    """Single scheduler tick — process cron-based and pending (post-ingest) checks."""
    try:
        from supertable.services.quality.config import DQConfig
        from supertable.redis_connector import create_redis_client
    except ImportError:
        logger.debug("[dq-scheduler] Dependencies not available yet, skipping tick")
        return

    try:
        r = create_redis_client()
    except Exception as e:
        logger.warning(f"[dq-scheduler] Cannot connect to Redis: {e}")
        return

    pairs = _discover_dq_pairs(r)
    if not pairs:
        return

    now = time.time()

    for org, sup in pairs:
        dqc = DQConfig(r, org, sup)
        schedule = dqc.get_schedule()

        if not schedule.get("enabled", True):
            continue

        quick_cron = schedule.get("quick_cron", "0 */4 * * *")
        deep_cron = schedule.get("deep_cron", "0 2 * * *")
        custom_cron = schedule.get("custom_cron", "0 */6 * * *")
        cooldown_sec = int(schedule.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS))

        tables = _list_tables(r, org, sup)

        for table_name in tables:
            tkey = f"{org}:{sup}:{table_name}"

            # Table-specific schedule override
            ts = dqc.get_table_schedule(table_name)
            if ts and not ts.get("enabled", True):
                continue

            table_quick_cron = (ts or {}).get("quick_cron") or quick_cron

            # ── CRON-BASED QUICK CHECK ────────────────────────────
            quick_interval = _cron_to_seconds(table_quick_cron)
            last_q = last_quick_run.get(tkey, 0)

            if now - last_q >= quick_interval:
                if _try_run_check(r, org, sup, table_name, "quick", dqc, cooldown_sec):
                    last_quick_run[tkey] = now

            # ── CRON-BASED DEEP CHECK ─────────────────────────────
            table_deep_cron = (ts or {}).get("deep_cron") or deep_cron
            deep_interval = _cron_to_seconds(table_deep_cron)
            last_d = last_deep_run.get(tkey, 0)
            deep_enabled = (ts or {}).get("deep_enabled", True)

            if deep_enabled and now - last_d >= deep_interval:
                eff_config = dqc.get_effective_config(table_name)
                has_deep = any(v.get("enabled") for k, v in (eff_config.get("checks") or {}).items()
                               if k.startswith("D"))
                if has_deep:
                    if _try_run_check(r, org, sup, table_name, "deep", dqc, cooldown_sec):
                        last_deep_run[tkey] = now

            # ── CRON-BASED CUSTOM RULES CHECK ─────────────────────
            table_custom_cron = (ts or {}).get("custom_cron") or custom_cron
            custom_interval = _cron_to_seconds(table_custom_cron)
            last_c = last_custom_run.get(tkey, 0)
            custom_enabled = (ts or {}).get("custom_enabled", True)

            if custom_enabled and now - last_c >= custom_interval:
                has_rules = bool(dqc.list_rules_for_table(table_name))
                if has_rules:
                    if _try_run_check(r, org, sup, table_name, "custom", dqc, cooldown_sec):
                        last_custom_run[tkey] = now

            # ── POST-INGEST PENDING CHECK ─────────────────────────
            pending_key = _pending_key(org, sup, table_name)
            if r.exists(pending_key):
                # Determine which tiers to run on ingest
                pi_quick = schedule.get("post_ingest_quick", schedule.get("post_ingest", True) not in (False,))
                pi_custom = schedule.get("post_ingest_custom", schedule.get("post_ingest", True) not in (False,))
                pi_deep = schedule.get("post_ingest_deep", False)

                ran_any = False
                if pi_quick:
                    if _try_run_check(r, org, sup, table_name, "quick", dqc, cooldown_sec):
                        ran_any = True
                if pi_custom and dqc.list_rules_for_table(table_name):
                    if _try_run_check(r, org, sup, table_name, "custom", dqc, cooldown_sec):
                        ran_any = True
                if pi_deep:
                    if _try_run_check(r, org, sup, table_name, "deep", dqc, cooldown_sec):
                        ran_any = True

                if ran_any:
                    r.delete(pending_key)
                # If nothing ran (locked or cooling down),
                # the pending key stays — we'll retry on the next tick.


def _try_run_check(
    r,
    org: str,
    sup: str,
    table_name: str,
    mode: str,
    dqc,
    cooldown_sec: int,
) -> bool:
    """
    Attempt to run a quality check with lock + cooldown protection.

    Returns True if the check was executed, False if skipped.
    """
    running_key = _running_key(org, sup, table_name)
    cooldown_key = _cooldown_key(org, sup, table_name)

    # ── Check cooldown ────────────────────────────────────────
    if r.exists(cooldown_key):
        ttl = r.ttl(cooldown_key)
        logger.debug(
            f"[dq-scheduler] Skipping {table_name} ({mode}): "
            f"cooldown active, {ttl}s remaining"
        )
        return False

    # ── Acquire lock (SET NX = only if not exists) ────────────
    acquired = r.set(
        running_key,
        _now_iso(),
        nx=True,                          # only set if key does not exist
        ex=DEFAULT_RUNNING_TTL_SECONDS,   # safety TTL in case of crash
    )
    if not acquired:
        logger.debug(
            f"[dq-scheduler] Skipping {table_name} ({mode}): "
            f"another check is already running"
        )
        return False

    # ── Execute the check ─────────────────────────────────────
    try:
        logger.info(f"[dq-scheduler] Running {mode} check: {org}/{sup}/{table_name}")

        if mode == "deep":
            _run_deep_check(r, org, sup, table_name, dqc)
        elif mode == "custom":
            _run_custom_check(r, org, sup, table_name, dqc)
        else:
            _run_quick_check(r, org, sup, table_name, dqc)

        # ── Set cooldown ──────────────────────────────────────
        r.set(cooldown_key, _now_iso(), ex=cooldown_sec)
        return True

    except Exception:
        logger.error(
            f"[dq-scheduler] {mode} check failed for {table_name}:\n"
            f"{traceback.format_exc()}"
        )
        return False

    finally:
        # ── Release lock ──────────────────────────────────────
        r.delete(running_key)


# ──────────────────────────────────────────────────────────────────────
# Check execution
# ──────────────────────────────────────────────────────────────────────

def _run_quick_check(r, org: str, sup: str, table_name: str, dqc) -> None:
    """Execute a quick profile check for one table."""
    _check_start_ms = int(time.time() * 1000)

    from supertable.services.quality.checker import (
        build_quick_sql, parse_quick_result, build_custom_rule_sql,
        evaluate_custom_rule, compute_quality_score,
    )
    from supertable.services.quality.anomaly import detect_anomalies, detect_schema_drift

    try:
        from supertable.meta_reader import MetaReader
    except ImportError:
        logger.error("[dq-scheduler] MetaReader not available")
        return

    eff = dqc.get_effective_config(table_name)
    checks = eff.get("checks", {})

    # Get schema
    try:
        mr = MetaReader(super_name=sup, organization=org)
        schema_raw = mr.get_table_schema(table_name, "superadmin")
        if not schema_raw or not schema_raw[0]:
            logger.warning(f"[dq-scheduler] No schema for {table_name}")
            return
        schema_dict = schema_raw[0]
        columns = [(name, ctype) for name, ctype in schema_dict.items()
                    if not name.startswith("_sys_")]
    except Exception as e:
        logger.error(f"[dq-scheduler] Schema read failed for {table_name}: {e}")
        return

    # Build and execute quick SQL
    table_fqn = f"{sup}.{table_name}"
    inc_col = eff.get("incremental_column")
    last_ts = None
    if inc_col and eff.get("scope") == "incremental":
        prev = dqc.get_latest(table_name)
        if prev:
            last_ts = prev.get("checked_at")

    sql = build_quick_sql(table_fqn, columns, inc_col, last_ts)

    try:
        from supertable.data_reader import DataReader
        dr = DataReader(super_name=sup, organization=org, query=sql)
        result_df, status, message = dr.execute(role_name="superadmin")
        if result_df is None or result_df.empty:
            logger.warning(f"[dq-scheduler] Quick SQL returned empty for {table_name}")
            return
        row = result_df.to_dict(orient="records")[0]
    except Exception as e:
        logger.error(f"[dq-scheduler] Quick SQL execution failed for {table_name}: {e}")
        return

    parsed = parse_quick_result(row, columns)

    # Anomaly detection
    previous = dqc.get_latest(table_name)
    prev_parsed = previous.get("parsed") if previous else None
    anomalies = detect_anomalies(parsed, prev_parsed, checks)

    # Schema drift
    prev_schema = previous.get("schema") if previous else None
    if prev_schema:
        prev_schema_tuples = [tuple(s) for s in prev_schema]
        schema_anomalies = detect_schema_drift(columns, prev_schema_tuples, checks)
        anomalies.extend(schema_anomalies)

    # Custom rules run on their own schedule (custom_cron) — not during quick check
    rule_results = []

    score = compute_quality_score(parsed.get("columns", {}), anomalies)

    n_warnings = sum(1 for a in anomalies if a.get("severity") == "warning")
    n_critical = sum(1 for a in anomalies if a.get("severity") == "critical")
    total_checks = len([c for c in checks.values() if c.get("enabled")])
    passed = total_checks - n_warnings - n_critical

    latest = {
        "checked_at": _now_iso(),
        "check_type": "quick",
        "row_count": parsed.get("total", 0),
        "quality_score": score,
        "status": "critical" if n_critical > 0 else ("warning" if n_warnings > 0 else "ok"),
        "total_checks": total_checks,
        "passed": max(0, passed),
        "warnings": n_warnings,
        "critical": n_critical,
        "anomalies": anomalies,
        "rule_results": rule_results,
        "parsed": parsed,
        "schema": [list(c) for c in columns],
    }

    dqc.set_latest(table_name, latest)
    dqc.set_anomalies(table_name, anomalies)

    for col_name, col_data in parsed.get("columns", {}).items():
        col_data["checked_at"] = latest["checked_at"]
        col_issues = [a for a in anomalies if a.get("column") == col_name]
        col_data["status"] = "critical" if any(a["severity"] == "critical" for a in col_issues) \
            else ("warning" if col_issues else "ok")
        col_data["issues"] = [a.get("message", "") for a in col_issues]
        dqc.set_latest_column(table_name, col_name, col_data)

    logger.info(
        f"[dq-scheduler] Quick check done: {table_name} — "
        f"score={score}, anomalies={len(anomalies)}"
    )

    # ── Write to __data_quality__ history table ───────────────
    _check_elapsed_ms = int(time.time() * 1000) - _check_start_ms
    try:
        from supertable.services.quality.history import write_history, write_history_via_sql
        if not write_history(org, sup, table_name, "quick", latest, _check_elapsed_ms):
            write_history_via_sql(org, sup, table_name, "quick", latest, _check_elapsed_ms)
    except Exception as e:
        logger.debug(f"[dq-scheduler] History write skipped for {table_name}: {e}")


def _run_deep_check(r, org: str, sup: str, table_name: str, dqc) -> None:
    """Execute deep profile checks for enabled columns."""
    _deep_start_ms = int(time.time() * 1000)

    from supertable.services.quality.checker import (
        build_deep_string_sql, build_deep_numeric_sql, _col_category,
    )

    try:
        from supertable.meta_reader import MetaReader
        from supertable.data_reader import DataReader
    except ImportError:
        logger.error("[dq-scheduler] MetaReader/DataReader not available")
        return

    eff = dqc.get_effective_config(table_name)
    checks = eff.get("checks", {})
    deep_enabled = any(v.get("enabled") for k, v in checks.items() if k.startswith("D"))
    if not deep_enabled:
        return

    try:
        mr = MetaReader(super_name=sup, organization=org)
        schema_raw = mr.get_table_schema(table_name, "superadmin")
        if not schema_raw or not schema_raw[0]:
            return
        schema_dict = schema_raw[0]
    except Exception as e:
        logger.error(f"[dq-scheduler] Deep schema read failed for {table_name}: {e}")
        return

    table_fqn = f"{sup}.{table_name}"

    for col_name, col_type in schema_dict.items():
        if col_name.startswith("_sys_"):
            continue

        cat = _col_category(col_type)
        if cat == "numeric":
            sql = build_deep_numeric_sql(table_fqn, col_name)
        elif cat == "string":
            sql = build_deep_string_sql(table_fqn, col_name)
        else:
            continue

        try:
            dr = DataReader(super_name=sup, organization=org, query=sql)
            result_df, status, message = dr.execute(role_name="superadmin")
            if result_df is None or result_df.empty:
                continue
            deep_result = result_df.to_dict(orient="records")[0]
            deep_result["check_type"] = "deep"
            deep_result["checked_at"] = _now_iso()
            deep_result["column_name"] = col_name
            deep_result["column_type"] = col_type
            deep_result["category"] = cat

            existing = dqc.get_latest_column(table_name, col_name) or {}
            existing["deep"] = deep_result
            existing["deep_checked_at"] = deep_result["checked_at"]
            dqc.set_latest_column(table_name, col_name, existing)

        except Exception as e:
            logger.warning(f"[dq-scheduler] Deep check failed for {table_name}.{col_name}: {e}")

    logger.info(f"[dq-scheduler] Deep check done: {table_name}")

    # ── Write to __data_quality__ history table ───────────────
    _deep_elapsed_ms = int(time.time() * 1000) - _deep_start_ms
    try:
        from supertable.services.quality.history import write_history, write_history_via_sql
        # Build a minimal latest-like dict for the deep check history row
        deep_latest = {
            "checked_at": _now_iso(),
            "check_type": "deep",
            "quality_score": 0,
            "status": "ok",
            "row_count": 0,
            "total_checks": 0,
            "passed": 0,
            "warnings": 0,
            "critical": 0,
            "anomalies": [],
            "rule_results": [],
            "parsed": {"total": 0, "columns": {}},
        }
        # Merge with existing quick check latest for context
        existing_latest = dqc.get_latest(table_name)
        if existing_latest:
            deep_latest["quality_score"] = existing_latest.get("quality_score", 0)
            deep_latest["status"] = existing_latest.get("status", "ok")
            deep_latest["row_count"] = existing_latest.get("row_count", 0)
        if not write_history(org, sup, table_name, "deep", deep_latest, _deep_elapsed_ms):
            write_history_via_sql(org, sup, table_name, "deep", deep_latest, _deep_elapsed_ms)
    except Exception as e:
        logger.debug(f"[dq-scheduler] History write skipped for deep {table_name}: {e}")


def _run_custom_check(r, org: str, sup: str, table_name: str, dqc) -> None:
    """Execute custom rules for one table on their own schedule.

    Runs all enabled custom rules, evaluates results, merges into the
    existing latest (from Redis), recomputes score, and stores back.
    """
    _custom_start_ms = int(time.time() * 1000)

    from supertable.services.quality.checker import (
        build_custom_rule_sql, evaluate_custom_rule, compute_quality_score,
    )

    try:
        from supertable.data_reader import DataReader
    except ImportError:
        logger.error("[dq-scheduler] DataReader not available")
        return

    custom_rules = dqc.list_rules_for_table(table_name)
    if not custom_rules:
        return

    table_fqn = f"{sup}.{table_name}"
    rule_results = []
    rule_anomalies = []

    for rule in custom_rules:
        rule_sql = build_custom_rule_sql(rule, table_fqn)
        if not rule_sql:
            continue
        try:
            dr = DataReader(super_name=sup, organization=org, query=rule_sql)
            r_df, _, _ = dr.execute(role_name="superadmin")
            r_result = r_df.to_dict(orient="records") if r_df is not None and not r_df.empty else []
            eval_result = evaluate_custom_rule(rule, r_result)
            rule_results.append({
                "rule_id": rule["rule_id"],
                "rule_type": rule["rule_type"],
                "description": rule.get("description", ""),
                **eval_result,
            })
            if eval_result["status"] != "ok":
                rule_anomalies.append({
                    "check_id": f"R_{rule['rule_id']}",
                    "check_name": f"Custom: {rule.get('description', rule['rule_type'])}",
                    "column": rule.get("column_name"),
                    "severity": rule.get("severity", "warning"),
                    "message": eval_result.get("detail", ""),
                    "value": eval_result.get("value"),
                    "detected_at": _now_iso(),
                })
        except Exception as e:
            logger.warning(f"[dq-scheduler] Custom rule {rule['rule_id']} failed: {e}")

    # ── Merge into existing latest ────────────────────────────
    existing = dqc.get_latest(table_name) or {}
    existing_parsed = existing.get("parsed", {"total": 0, "columns": {}})

    # Combine: keep built-in anomalies, replace custom rule anomalies
    builtin_anomalies = [a for a in existing.get("anomalies", [])
                         if not (a.get("check_id", "").startswith("R_"))]
    all_anomalies = builtin_anomalies + rule_anomalies

    # Recompute score with combined anomalies
    score = compute_quality_score(existing_parsed.get("columns", {}), all_anomalies)

    n_warnings = sum(1 for a in all_anomalies if a.get("severity") == "warning")
    n_critical = sum(1 for a in all_anomalies if a.get("severity") == "critical")
    builtin_count = existing.get("total_checks", 0) - len(existing.get("rule_results", []))
    total_checks = max(0, builtin_count) + len(custom_rules)
    passed = total_checks - n_warnings - n_critical

    # Update the latest record in place
    existing.update({
        "quality_score": score,
        "status": "critical" if n_critical > 0 else ("warning" if n_warnings > 0 else "ok"),
        "total_checks": total_checks,
        "passed": max(0, passed),
        "warnings": n_warnings,
        "critical": n_critical,
        "anomalies": all_anomalies,
        "rule_results": rule_results,
        "custom_checked_at": _now_iso(),
    })

    dqc.set_latest(table_name, existing)
    dqc.set_anomalies(table_name, all_anomalies)

    logger.info(
        f"[dq-scheduler] Custom rules check done: {table_name} — "
        f"{len(custom_rules)} rules, {len(rule_anomalies)} issues, score={score}"
    )

    # ── Write to __data_quality__ history table ───────────────
    _custom_elapsed_ms = int(time.time() * 1000) - _custom_start_ms
    try:
        from supertable.services.quality.history import write_history, write_history_via_sql
        if not write_history(org, sup, table_name, "custom", existing, _custom_elapsed_ms):
            write_history_via_sql(org, sup, table_name, "custom", existing, _custom_elapsed_ms)
    except Exception as e:
        logger.debug(f"[dq-scheduler] History write skipped for custom {table_name}: {e}")


# ──────────────────────────────────────────────────────────────────────
# Redis key helpers
# ──────────────────────────────────────────────────────────────────────

def _pending_key(org: str, sup: str, table: str) -> str:
    return f"supertable:{org}:{sup}:dq:pending:{table}"


def _running_key(org: str, sup: str, table: str) -> str:
    return f"supertable:{org}:{sup}:dq:running:{table}"


def _cooldown_key(org: str, sup: str, table: str) -> str:
    return f"supertable:{org}:{sup}:dq:cooldown:{table}"


# ──────────────────────────────────────────────────────────────────────
# General helpers
# ──────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _discover_dq_pairs(r) -> List[Tuple[str, str]]:
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
        logger.warning(f"[dq-scheduler] discover pairs failed: {e}")
    return list(set(pairs))


def _list_tables(r, org: str, sup: str) -> List[str]:
    """List all table names for an org:sup."""
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
        logger.warning(f"[dq-scheduler] list_tables failed: {e}")
    return sorted(set(tables))


def _cron_to_seconds(cron_expr: str) -> int:
    """
    Simplified cron-to-interval converter.
    Handles common patterns: */N hours, daily, etc.
    Falls back to 4 hours for complex expressions.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return 4 * 3600

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

    if dom == "*" and month == "*" and dow == "*" and not hour.startswith("*"):
        return 24 * 3600

    if hour == "*/1":
        return 3600

    return 4 * 3600
