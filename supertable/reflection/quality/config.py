# supertable/reflection/quality/config.py
"""
Redis-backed configuration for Data Quality checks.

Key layout (follows existing supertable:{org}:{sup}: pattern):

  dq:config:__global__          — default check toggles + thresholds
  dq:config:{table}             — per-table overrides
  dq:rules:index                — SET of custom rule IDs
  dq:rules:doc:{rule_id}        — HASH per custom rule
  dq:schedule                   — global schedule config
  dq:schedule:{table}           — per-table schedule override
  dq:latest:{table}             — last run summary (fast UI cache)
  dq:latest:{table}:{column}    — last column-level result
  dq:anomalies:{table}          — active anomalies list
"""
from __future__ import annotations

import json
import time
import uuid
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

import redis

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _now_ms() -> int:
    return int(time.time() * 1000)


# ──────────────────────────────────────────────────────────────────────
# Key helpers
# ──────────────────────────────────────────────────────────────────────

def _dq_key(org: str, sup: str, *parts: str) -> str:
    return f"supertable:{org}:{sup}:dq:{':'.join(parts)}"


# ──────────────────────────────────────────────────────────────────────
# Default built-in check definitions
# ──────────────────────────────────────────────────────────────────────

BUILTIN_CHECKS: Dict[str, Dict[str, Any]] = {
    # Table-level
    "T1": {"name": "Row count delta",       "level": "table",  "group": "quick", "enabled": True,  "threshold": 30,   "unit": "%",     "description": "Alert when row count changes by more than threshold %"},
    "T2": {"name": "Freshness",             "level": "table",  "group": "quick", "enabled": True,  "threshold": 24,   "unit": "hours", "description": "Alert when table not updated within threshold hours"},
    "T3": {"name": "Schema drift",          "level": "table",  "group": "quick", "enabled": True,  "threshold": None, "unit": None,    "description": "Alert on any column addition, removal, or type change"},
    "T5": {"name": "Table size trend",      "level": "table",  "group": "quick", "enabled": True,  "threshold": 50,   "unit": "%",     "description": "Alert when storage size changes by more than threshold %"},
    # Column-level quick
    "C1": {"name": "NULL rate",             "level": "column", "group": "quick", "enabled": True,  "threshold": 5,    "unit": "pp",    "description": "Alert when NULL rate spikes by more than threshold percentage points"},
    "C2": {"name": "Distinct count shift",  "level": "column", "group": "quick", "enabled": True,  "threshold": 50,   "unit": "%",     "description": "Alert when distinct value count changes by more than threshold %"},
    "C3": {"name": "Min/Max breach",        "level": "column", "group": "quick", "enabled": True,  "threshold": None, "unit": None,    "description": "Alert when values exceed previously observed min/max boundaries"},
    "C4": {"name": "Uniqueness ratio",      "level": "column", "group": "quick", "enabled": False, "threshold": None, "unit": None,    "description": "Track uniqueness ratio trend (distinct / non-null)"},
    "C5": {"name": "Zero/negative rate",    "level": "column", "group": "quick", "enabled": True,  "threshold": 5,    "unit": "pp",    "description": "Alert when zero or negative value rate spikes (numeric columns)"},
    "C6": {"name": "Mean + Stddev drift",   "level": "column", "group": "quick", "enabled": True,  "threshold": 2.0,  "unit": "σ",     "description": "Alert when mean drifts beyond threshold standard deviations"},
    # Column-level deep
    "D1": {"name": "String length stats",   "level": "column", "group": "deep",  "enabled": False, "threshold": None, "unit": None,    "description": "Track avg/min/max/median string length over time"},
    "D2": {"name": "Shannon entropy",       "level": "column", "group": "deep",  "enabled": False, "threshold": 20,   "unit": "%",     "description": "Alert when entropy changes by more than threshold %"},
    "D3": {"name": "Top N values shift",    "level": "column", "group": "deep",  "enabled": False, "threshold": None, "unit": None,    "description": "Track top 10 most frequent values and their coverage"},
    "D4": {"name": "Histogram",             "level": "column", "group": "deep",  "enabled": False, "threshold": None, "unit": None,    "description": "Track value distribution in 10 equal-frequency buckets"},
    "D5": {"name": "Percentiles (IQR)",     "level": "column", "group": "deep",  "enabled": False, "threshold": 30,   "unit": "%",     "description": "Alert when IQR changes by more than threshold % (numeric)"},
    "D7": {"name": "Placeholder rate",      "level": "column", "group": "deep",  "enabled": False, "threshold": 10,   "unit": "%",     "description": "Alert when placeholder values (n/a, unknown, etc.) exceed threshold %"},
}


class DQConfig:
    """
    CRUD operations for Data Quality configuration stored in Redis.
    """

    def __init__(self, r: redis.Redis, org: str, sup: str):
        self.r = r
        self.org = org
        self.sup = sup

    # ── Key shortcuts ─────────────────────────────────────────────────

    def _key(self, *parts: str) -> str:
        return _dq_key(self.org, self.sup, *parts)

    # ── Global config ─────────────────────────────────────────────────

    def get_global_config(self) -> Dict[str, Any]:
        """Return global check config.  Falls back to BUILTIN_CHECKS defaults."""
        try:
            raw = self.r.get(self._key("config", "__global__"))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_global_config error: {e}")
        # Return defaults
        return {
            "checks": {cid: {"enabled": c["enabled"], "threshold": c["threshold"]}
                        for cid, c in BUILTIN_CHECKS.items()},
        }

    def set_global_config(self, config: Dict[str, Any], updated_by: str = "") -> bool:
        try:
            config["updated_by"] = updated_by
            config["updated_at"] = _now_iso()
            self.r.set(self._key("config", "__global__"), json.dumps(config, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_global_config error: {e}")
            return False

    # ── Per-table config ──────────────────────────────────────────────

    def get_table_config(self, table: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("config", table))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_table_config error: {e}")
        return None

    def set_table_config(self, table: str, config: Dict[str, Any], updated_by: str = "") -> bool:
        try:
            config["updated_by"] = updated_by
            config["updated_at"] = _now_iso()
            self.r.set(self._key("config", table), json.dumps(config, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_table_config error: {e}")
            return False

    def delete_table_config(self, table: str) -> bool:
        try:
            self.r.delete(self._key("config", table))
            return True
        except Exception as e:
            logger.error(f"[dq-config] delete_table_config error: {e}")
            return False

    def get_effective_config(self, table: str) -> Dict[str, Any]:
        """Merge global defaults with table-specific overrides."""
        g = self.get_global_config()
        t = self.get_table_config(table)
        if not t:
            return g
        merged = deepcopy(g)
        # Merge check overrides
        for cid, overrides in (t.get("checks") or {}).items():
            if cid in merged.get("checks", {}):
                merged["checks"][cid].update(overrides)
            else:
                merged["checks"][cid] = overrides
        # Merge top-level fields
        for field in ("scope", "incremental_column"):
            if field in t:
                merged[field] = t[field]
        return merged

    # ── Custom rules ──────────────────────────────────────────────────

    def list_rules(self) -> List[Dict[str, Any]]:
        try:
            index_key = self._key("rules", "index")
            rule_ids = self.r.smembers(index_key) or set()
            rules = []
            for rid_raw in rule_ids:
                rid = rid_raw if isinstance(rid_raw, str) else rid_raw.decode("utf-8")
                raw = self.r.get(self._key("rules", "doc", rid))
                if raw:
                    rules.append(json.loads(raw))
            return sorted(rules, key=lambda x: x.get("created_at", ""))
        except Exception as e:
            logger.error(f"[dq-config] list_rules error: {e}")
            return []

    def get_rule(self, rule_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("rules", "doc", rule_id))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_rule error: {e}")
        return None

    def create_rule(self, rule: Dict[str, Any], created_by: str = "") -> Dict[str, Any]:
        rule_id = rule.get("rule_id") or f"rule_{uuid.uuid4().hex[:8]}"
        rule["rule_id"] = rule_id
        rule.setdefault("enabled", True)
        rule.setdefault("severity", "warning")
        rule["created_by"] = created_by
        rule["created_at"] = _now_iso()
        try:
            self.r.set(self._key("rules", "doc", rule_id), json.dumps(rule, default=str))
            self.r.sadd(self._key("rules", "index"), rule_id)
        except Exception as e:
            logger.error(f"[dq-config] create_rule error: {e}")
        return rule

    def update_rule(self, rule_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = self.get_rule(rule_id)
        if not existing:
            return None
        existing.update(updates)
        existing["updated_at"] = _now_iso()
        try:
            self.r.set(self._key("rules", "doc", rule_id), json.dumps(existing, default=str))
        except Exception as e:
            logger.error(f"[dq-config] update_rule error: {e}")
        return existing

    def delete_rule(self, rule_id: str) -> bool:
        try:
            self.r.delete(self._key("rules", "doc", rule_id))
            self.r.srem(self._key("rules", "index"), rule_id)
            return True
        except Exception as e:
            logger.error(f"[dq-config] delete_rule error: {e}")
            return False

    def list_rules_for_table(self, table: str) -> List[Dict[str, Any]]:
        all_rules = self.list_rules()
        return [r for r in all_rules
                if r.get("enabled") and r.get("table_name") in (table, "*")]

    # ── Schedule config ───────────────────────────────────────────────

    def get_schedule(self) -> Dict[str, Any]:
        try:
            raw = self.r.get(self._key("schedule"))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_schedule error: {e}")
        return {
            "quick_cron": "0 */4 * * *",
            "deep_cron": "0 2 * * *",
            "post_ingest": True,
            "enabled": True,
        }

    def set_schedule(self, schedule: Dict[str, Any]) -> bool:
        try:
            schedule["updated_at"] = _now_iso()
            self.r.set(self._key("schedule"), json.dumps(schedule, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_schedule error: {e}")
            return False

    def get_table_schedule(self, table: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("schedule", table))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_table_schedule error: {e}")
        return None

    def set_table_schedule(self, table: str, schedule: Dict[str, Any]) -> bool:
        try:
            schedule["updated_at"] = _now_iso()
            self.r.set(self._key("schedule", table), json.dumps(schedule, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_table_schedule error: {e}")
            return False

    def delete_table_schedule(self, table: str) -> bool:
        """Remove a per-table schedule override (reverts to global)."""
        try:
            self.r.delete(self._key("schedule", table))
            return True
        except Exception as e:
            logger.error(f"[dq-config] delete_table_schedule error: {e}")
            return False

    def get_all_table_schedules(self) -> Dict[str, Dict[str, Any]]:
        """Scan all dq:schedule:{table} keys and return {table_name: schedule}."""
        results = {}
        try:
            prefix = self._key("schedule") + ":"
            cursor = 0
            keys_found = []
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=prefix + "*", count=500)
                keys_found.extend(keys)
                if cursor == 0:
                    break
            if keys_found:
                pipe = self.r.pipeline(transaction=False)
                for key in keys_found:
                    pipe.get(key)
                values = pipe.execute()
                for key, raw in zip(keys_found, values):
                    k = key if isinstance(key, str) else key.decode("utf-8")
                    suffix = k[len(prefix):]
                    if raw:
                        results[suffix] = json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_all_table_schedules error: {e}")
        return results

    # ── Latest results (fast UI cache) ────────────────────────────────

    def get_latest(self, table: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("latest", table))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_latest error: {e}")
        return None

    def set_latest(self, table: str, result: Dict[str, Any]) -> bool:
        try:
            self.r.set(self._key("latest", table), json.dumps(result, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_latest error: {e}")
            return False

    def get_latest_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("latest", table, column))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_latest_column error: {e}")
        return None

    def set_latest_column(self, table: str, column: str, result: Dict[str, Any]) -> bool:
        try:
            self.r.set(self._key("latest", table, column), json.dumps(result, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_latest_column error: {e}")
            return False

    def get_anomalies(self, table: str) -> List[Dict[str, Any]]:
        try:
            raw = self.r.get(self._key("anomalies", table))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_anomalies error: {e}")
        return []

    def set_anomalies(self, table: str, anomalies: List[Dict[str, Any]]) -> bool:
        try:
            self.r.set(self._key("anomalies", table), json.dumps(anomalies, default=str))
            return True
        except Exception as e:
            logger.error(f"[dq-config] set_anomalies error: {e}")
            return False

    # ── Bulk listing (for overview page) ──────────────────────────────

    def get_all_latest(self) -> Dict[str, Dict[str, Any]]:
        """Scan all dq:latest:{table} keys and return {table_name: latest_result}."""
        results = {}
        try:
            prefix = self._key("latest") + ":"
            cursor = 0
            table_keys = []  # (redis_key, table_name)
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=prefix + "*", count=500)
                for key in keys:
                    k = key if isinstance(key, str) else key.decode("utf-8")
                    # Only table-level keys (no column sub-keys)
                    suffix = k[len(prefix):]
                    if ":" not in suffix:
                        table_keys.append((key, suffix))
                if cursor == 0:
                    break
            # Batch fetch with pipeline instead of N individual GETs
            if table_keys:
                pipe = self.r.pipeline(transaction=False)
                for rkey, _ in table_keys:
                    pipe.get(rkey)
                values = pipe.execute()
                for (_, table_name), raw in zip(table_keys, values):
                    if raw:
                        results[table_name] = json.loads(raw)
        except Exception as e:
            logger.error(f"[dq-config] get_all_latest error: {e}")
        return results
