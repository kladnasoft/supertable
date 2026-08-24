# route: supertable.quality.config
# supertable/quality/config.py
"""
Redis-backed configuration for Data Quality checks.

Key layout (under supertable:{org}:lakes:{sup}:quality:):

  config:__global__             — default check toggles + thresholds
  config:{table}                — per-table overrides
  rules:index                   — SET of custom rule IDs
  rules:doc:{rule_id}           — HASH per custom rule
  schedule                      — global schedule config
  schedule:{table}              — per-table schedule override
  latest:{table}                — last run summary (fast UI cache)
  latest:{table}:{column}       — last column-level result
  anomalies:{table}             — active anomalies list

The prefix is centralised in ``supertable.redis_keys.quality_prefix``.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import time
import uuid
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

import redis

from supertable.quality.serialization import normalize_json_value

logger = logging.getLogger(__name__)


class DQConfigReadError(RuntimeError):
    """Persisted DQ state could not be read with execution-safe certainty."""


class DQConfigConflictError(RuntimeError):
    """A quality-rule mutation was based on a stale authoritative document."""


_MISSING = object()
# Keep scheduler-controlled expirations in a conservative range supported by
# every Redis deployment we target.  Lua represents numbers as doubles and
# Redis converts EX seconds to signed millisecond deadlines; a smaller exact
# bound also prevents a late SET/EX error from turning a script into a partial
# success publication.
_MAX_REDIS_TTL_SECONDS = (1 << 31) - 1


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _now_ms() -> int:
    return int(time.time() * 1000)


# ──────────────────────────────────────────────────────────────────────
# Key helpers
# ──────────────────────────────────────────────────────────────────────

from supertable import redis_keys as RK


def _dq_key(org: str, sup: str, *parts: str) -> str:
    return RK.quality_prefix(org, sup) + ":".join(parts)


_LUA_SET_QUALITY_IF_LIVE = """
-- quality-config lifecycle-fenced set
if redis.call('get', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('exists', KEYS[2]) == 1 then return -2 end
if ARGV[2] == '1' then
  if redis.call('get', KEYS[3]) ~= ARGV[3] then return -3 end
  if redis.call('exists', KEYS[4]) == 1 then return -2 end
end
redis.call('set', KEYS[5], ARGV[4])
return 1
"""


_LUA_DELETE_QUALITY_IF_LIVE = """
-- quality-config lifecycle-fenced delete
if redis.call('get', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('exists', KEYS[2]) == 1 then return -2 end
if ARGV[2] == '1' then
  if redis.call('get', KEYS[3]) ~= ARGV[3] then return -3 end
  if redis.call('exists', KEYS[4]) == 1 then return -2 end
end
redis.call('del', KEYS[5])
return 1
"""


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


def _default_global_config() -> Dict[str, Any]:
    return {
        "checks": {
            check_id: {
                "enabled": definition["enabled"],
                "threshold": definition["threshold"],
            }
            for check_id, definition in BUILTIN_CHECKS.items()
        },
    }


def _merge_known_checks(
    base: Dict[str, Dict[str, Any]],
    overrides: Any,
) -> Dict[str, Dict[str, Any]]:
    """Merge only supported built-ins and only object-shaped overrides."""

    merged = deepcopy(base)
    if not isinstance(overrides, dict):
        return merged
    for check_id, override in overrides.items():
        if check_id not in BUILTIN_CHECKS or not isinstance(override, dict):
            continue
        merged.setdefault(check_id, {}).update(deepcopy(override))
    return merged


def _incremental_scope_requested(config: Dict[str, Any]) -> bool:
    """Return whether a config requests the unsupported row-level cursor."""

    return config.get("scope") == "incremental"


def _without_incremental_scope(config: Dict[str, Any]) -> Dict[str, Any]:
    """Fail legacy incremental configuration closed to a complete scan.

    A timestamp alone is not a lossless cursor: ``>`` drops rows sharing or
    arriving behind the watermark while ``>=`` processes boundary rows more
    than once.  Until a stable composite cursor exists, persisted legacy
    configuration must never activate that execution path.
    """

    sanitized = deepcopy(config)
    if _incremental_scope_requested(sanitized):
        sanitized["scope"] = "full"
        sanitized.pop("incremental_column", None)
    return sanitized


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

    def _valid_catalog_identity(self, raw: Any, *, leaf: bool) -> bool:
        """Validate the canonical catalog lifecycle document before fencing."""

        if raw is None:
            return False
        try:
            document = json.loads(raw)
            # Keep this boundary identical to RedisCatalog's authoritative
            # structural contract. Identity-only checks would accept corrupt
            # read-only/clone fields and acknowledge quality mutations under a
            # namespace every catalog operation treats as invalid.
            from supertable.redis_catalog import (
                _validate_leaf_document,
                _validate_root_document,
            )
            if leaf:
                _validate_leaf_document(document)
            else:
                _validate_root_document(
                    document, org=self.org, sup=self.sup,
                )
        except (ImportError, TypeError, ValueError, UnicodeError):
            return False
        return True

    def _snapshot_mutation_lifecycle(
        self,
        table: Optional[str],
    ) -> Optional[tuple[Any, Any]]:
        """Capture the exact namespace/table incarnation a write targets."""

        try:
            root_raw = self.r.get(RK.meta_root(self.org, self.sup))
            if not self._valid_catalog_identity(root_raw, leaf=False):
                return None
            leaf_raw = ""
            if table is not None:
                leaf_raw = self.r.get(RK.meta_leaf(self.org, self.sup, table))
                if not self._valid_catalog_identity(leaf_raw, leaf=True):
                    return None
            return root_raw, leaf_raw
        except Exception as exc:
            logger.error("[dq-config] catalog lifecycle read error: %s", exc)
            return None

    def _mutate_if_live(
        self,
        target_key: str,
        *,
        table: Optional[str],
        payload: Optional[str],
    ) -> bool:
        """Set/delete one quality key iff its catalog incarnation is live.

        The root/leaf values captured before the call are compared in the same
        Redis boundary as the mutation.  Namespace or table deletion, and a
        delete/recreate ABA, therefore cannot turn a stale DQConfig call into a
        write against a different logical table.
        """

        lifecycle = self._snapshot_mutation_lifecycle(table)
        if lifecycle is None:
            return False
        root_raw, leaf_raw = lifecycle
        has_table = table is not None
        leaf_key = (
            RK.meta_leaf(self.org, self.sup, table)
            if table is not None else RK.meta_root(self.org, self.sup)
        )
        simple_intent_key = (
            RK.meta_simple_deletion_intent(self.org, self.sup, table)
            if table is not None
            else RK.meta_namespace_deletion_intent(self.org, self.sup)
        )
        script = (
            _LUA_SET_QUALITY_IF_LIVE
            if payload is not None else _LUA_DELETE_QUALITY_IF_LIVE
        )
        try:
            result = int(self.r.eval(
                script,
                5,
                RK.meta_root(self.org, self.sup),
                RK.meta_namespace_deletion_intent(self.org, self.sup),
                leaf_key,
                simple_intent_key,
                target_key,
                root_raw,
                "1" if has_table else "0",
                leaf_raw if has_table else "",
                payload if payload is not None else "",
            ))
            return result == 1
        except Exception as exc:
            logger.error("[dq-config] lifecycle-fenced mutation error: %s", exc)
            return False

    def _require_watched_namespace_live(self, pipe: Any) -> None:
        """Validate namespace lifecycle after its keys have been WATCHed."""

        root_raw = pipe.get(RK.meta_root(self.org, self.sup))
        if not self._valid_catalog_identity(root_raw, leaf=False):
            raise RuntimeError("quality namespace is not live")
        if pipe.exists(RK.meta_namespace_deletion_intent(self.org, self.sup)):
            raise RuntimeError("quality namespace deletion is active")

    def _read_json(
        self,
        key: str,
        *,
        expected_type,
        missing: Any = _MISSING,
        label: str,
    ):
        """Read one persisted document without conflating absence and failure."""

        try:
            raw = self.r.get(key)
        except Exception as exc:
            logger.error("[dq-config] %s backend read error: %s", label, exc)
            raise DQConfigReadError(f"could not read {label}") from exc
        if raw is None:
            if missing is _MISSING:
                raise DQConfigReadError(f"required {label} is missing")
            return deepcopy(missing)
        try:
            document = json.loads(raw)
        except (TypeError, ValueError, UnicodeError) as exc:
            logger.error("[dq-config] %s contains malformed JSON", label)
            raise DQConfigReadError(f"persisted {label} is malformed") from exc
        if not isinstance(document, expected_type):
            expected_name = getattr(expected_type, "__name__", str(expected_type))
            logger.error(
                "[dq-config] %s has wrong shape (expected %s)",
                label,
                expected_name,
            )
            raise DQConfigReadError(f"persisted {label} has the wrong shape")
        return document

    @staticmethod
    def _validate_config_document(document: Dict[str, Any], label: str) -> None:
        if "checks" not in document:
            return
        checks = document["checks"]
        if not isinstance(checks, dict):
            raise DQConfigReadError(f"persisted {label} checks have the wrong shape")
        for check_id, override in checks.items():
            definition = BUILTIN_CHECKS.get(check_id)
            if definition is None:
                continue
            if not isinstance(override, dict):
                raise DQConfigReadError(
                    f"persisted {label} has malformed override for {check_id}"
                )
            if "enabled" in override and not isinstance(override["enabled"], bool):
                raise DQConfigReadError(
                    f"persisted {label} has non-boolean enabled for {check_id}"
                )
            if "threshold" not in override:
                continue
            threshold = override["threshold"]
            if definition["threshold"] is None:
                if threshold is not None:
                    raise DQConfigReadError(
                        f"persisted {label} requires a null threshold for {check_id}"
                    )
                continue
            if (
                isinstance(threshold, bool)
                or not isinstance(threshold, (int, float))
                or not math.isfinite(float(threshold))
                or threshold < 0
            ):
                raise DQConfigReadError(
                    f"persisted {label} has an invalid threshold for {check_id}"
                )

    @staticmethod
    def _validate_schedule_document(document: Dict[str, Any], label: str) -> None:
        boolean_fields = {
            "enabled",
            "post_ingest",
            "post_ingest_quick",
            "post_ingest_deep",
            "post_ingest_custom",
            "deep_enabled",
            "custom_enabled",
        }
        for field_name in boolean_fields.intersection(document):
            if not isinstance(document[field_name], bool):
                raise DQConfigReadError(
                    f"persisted {label} has non-boolean {field_name}"
                )
        if "cooldown_seconds" in document:
            cooldown = document["cooldown_seconds"]
            if (
                isinstance(cooldown, bool)
                or not isinstance(cooldown, int)
                or cooldown < 0
                or cooldown > _MAX_REDIS_TTL_SECONDS
            ):
                raise DQConfigReadError(
                    f"persisted {label} has an invalid cooldown_seconds"
                )
        timezone_name = document.get("timezone", "UTC")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise DQConfigReadError(
                f"persisted {label} has an invalid timezone"
            )
        try:
            from supertable.quality.cron import CronValidationError, validate_cron
        except ImportError as exc:
            raise DQConfigReadError(
                "timezone-aware cron support is unavailable"
            ) from exc
        try:
            # Validate the timezone even when this document inherits every cron
            # field from its parent schedule.
            validate_cron("0 0 * * *", timezone_name)
        except CronValidationError as exc:
            raise DQConfigReadError(
                f"persisted {label} has an invalid timezone: {exc}"
            ) from exc
        for field_name in ("quick_cron", "deep_cron", "custom_cron"):
            if field_name in document and (
                not isinstance(document[field_name], str)
                or not document[field_name].strip()
            ):
                raise DQConfigReadError(
                    f"persisted {label} has an invalid {field_name}"
                )
            if field_name in document:
                try:
                    validate_cron(document[field_name], timezone_name)
                except CronValidationError as exc:
                    raise DQConfigReadError(
                        f"persisted {label} has an invalid {field_name}: {exc}"
                    ) from exc

    # ── Global config ─────────────────────────────────────────────────

    def get_global_config(self) -> Dict[str, Any]:
        """Return global check config.  Falls back to BUILTIN_CHECKS defaults."""
        defaults = _default_global_config()
        stored = self._read_json(
            self._key("config", "__global__"),
            expected_type=dict,
            missing={},
            label="global quality config",
        )
        self._validate_config_document(stored, "global quality config")
        merged = deepcopy(defaults)
        for key, value in stored.items():
            if key != "checks":
                merged[key] = deepcopy(value)
        merged["checks"] = _merge_known_checks(
            defaults["checks"], stored.get("checks")
        )
        return _without_incremental_scope(merged)

    def set_global_config(self, config: Dict[str, Any], updated_by: str = "") -> bool:
        try:
            if not isinstance(config, dict) or _incremental_scope_requested(config):
                return False
            self._validate_config_document(config, "global quality config")
            stored = deepcopy(config)
            stored["updated_by"] = updated_by
            stored["updated_at"] = _now_iso()
            persisted = self._mutate_if_live(
                self._key("config", "__global__"),
                table=None,
                payload=json.dumps(stored, default=str),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_global_config error: {e}")
            return False

    # ── Per-table config ──────────────────────────────────────────────

    def get_table_config(self, table: str) -> Optional[Dict[str, Any]]:
        stored = self._read_json(
            self._key("config", table),
            expected_type=dict,
            missing=None,
            label=f"quality config for table {table!r}",
        )
        if stored is not None:
            self._validate_config_document(stored, f"quality config for table {table!r}")
            stored = _without_incremental_scope(stored)
        return stored

    def set_table_config(self, table: str, config: Dict[str, Any], updated_by: str = "") -> bool:
        try:
            if not isinstance(config, dict) or _incremental_scope_requested(config):
                return False
            self._validate_config_document(
                config,
                f"quality config for table {table!r}",
            )
            stored = deepcopy(config)
            stored["updated_by"] = updated_by
            stored["updated_at"] = _now_iso()
            persisted = self._mutate_if_live(
                self._key("config", table),
                table=table,
                payload=json.dumps(stored, default=str),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_table_config error: {e}")
            return False

    def delete_table_config(self, table: str) -> bool:
        try:
            return self._mutate_if_live(
                self._key("config", table), table=table, payload=None,
            )
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
        merged["checks"] = _merge_known_checks(merged.get("checks", {}), t.get("checks"))
        # Merge top-level fields
        for field in ("scope", "incremental_column"):
            if field in t:
                merged[field] = t[field]
        # A lone timestamp cursor cannot cover equal or late-arriving rows
        # exactly.  Degrade every legacy incremental config to a complete
        # profile until a stable composite public cursor is supported.
        return _without_incremental_scope(merged)

    # ── Custom rules ──────────────────────────────────────────────────

    def _validate_rule_inventory_document(
        self,
        rule_id: str,
        document: Dict[str, Any],
    ) -> None:
        """Fail closed on corrupt indexed rule documents.

        Disabled, recognisable legacy rules remain listable so an operator can
        repair or delete them.  Enabled rules must pass the complete
        schema-independent validator; the scheduler repeats validation with
        the actual public schema before execution.
        """

        from supertable.quality.checker import (
            quality_table_fqn,
            validate_custom_rule,
            validate_quality_table_name,
        )

        if document.get("rule_id") != rule_id:
            raise DQConfigReadError(
                f"quality rule {rule_id!r} document identity does not match its index"
            )
        enabled = document.get("enabled")
        if not isinstance(enabled, bool):
            raise DQConfigReadError(
                f"quality rule {rule_id!r} has non-boolean enabled"
            )
        rule_type = document.get("rule_type")
        known_types = {
            "column_min", "column_max", "null_rate_max", "row_count_min",
            "distinct_in", "custom_sql",
        }
        if rule_type not in known_types:
            raise DQConfigReadError(
                f"quality rule {rule_id!r} has unknown rule_type"
            )
        table_validation = validate_quality_table_name(
            document.get("table_name"),
            super_name=self.sup,
            allow_wildcard=rule_type != "custom_sql",
        )
        if not table_validation.valid:
            raise DQConfigReadError(
                f"quality rule {rule_id!r} has invalid table scope "
                f"({table_validation.code})"
            )

        required_fields = {
            "column_min": ("column_name", "threshold"),
            "column_max": ("column_name", "threshold"),
            "null_rate_max": ("column_name", "threshold"),
            "row_count_min": ("threshold",),
            "distinct_in": ("column_name", "expected_values"),
            "custom_sql": ("sql",),
        }
        if any(field not in document for field in required_fields[rule_type]):
            raise DQConfigReadError(
                f"quality rule {rule_id!r} is missing required fields"
            )

        if enabled:
            table_fqn = (
                None
                if document["table_name"] == "*"
                else quality_table_fqn(self.sup, document["table_name"])
            )
            validation = validate_custom_rule(document, table_fqn=table_fqn)
            if not validation.valid:
                raise DQConfigReadError(
                    f"quality rule {rule_id!r} is invalid "
                    f"({validation.code}): {validation.message}"
                )

    def list_rules(self) -> List[Dict[str, Any]]:
        try:
            index_key = self._key("rules", "index")
            rule_ids = self.r.smembers(index_key) or set()
        except Exception as exc:
            logger.error("[dq-config] list_rules index read error: %s", exc)
            raise DQConfigReadError("could not read quality rule index") from exc
        if not isinstance(rule_ids, (set, frozenset, list, tuple)):
            raise DQConfigReadError("quality rule index has the wrong shape")

        rules = []
        for rid_raw in rule_ids:
            try:
                rid = (
                    rid_raw
                    if isinstance(rid_raw, str)
                    else rid_raw.decode("utf-8")
                )
            except (AttributeError, UnicodeError) as exc:
                raise DQConfigReadError("quality rule index contains an invalid id") from exc
            document = self._read_json(
                self._key("rules", "doc", rid),
                expected_type=dict,
                label=f"quality rule {rid!r}",
            )
            self._validate_rule_inventory_document(rid, document)
            rules.append(document)
        return sorted(rules, key=lambda x: str(x.get("created_at", "")))

    def get_rule(self, rule_id: str) -> Optional[Dict[str, Any]]:
        return self._read_json(
            self._key("rules", "doc", rule_id),
            expected_type=dict,
            missing=None,
            label=f"quality rule {rule_id!r}",
        )

    @staticmethod
    def rule_fingerprint(rule: Dict[str, Any]) -> str:
        """Return a deterministic fingerprint for compare-and-set mutations."""
        if not isinstance(rule, dict):
            raise ValueError("quality rule fingerprint input must be an object")
        try:
            payload = json.dumps(
                rule,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ValueError("quality rule cannot be fingerprinted") from exc
        return hashlib.sha256(payload).hexdigest()

    def create_rule(self, rule: Dict[str, Any], created_by: str = "") -> Dict[str, Any]:
        if not isinstance(rule, dict):
            raise ValueError("quality rule must be an object")
        candidate = deepcopy(rule)
        rule_id = candidate.get("rule_id") or f"rule_{uuid.uuid4().hex[:8]}"
        candidate["rule_id"] = rule_id
        candidate.setdefault("enabled", True)
        candidate.setdefault("severity", "warning")
        if not isinstance(candidate.get("enabled"), bool):
            raise ValueError("quality rule enabled must be a boolean")
        from supertable.quality.checker import (
            quality_table_fqn,
            validate_custom_rule,
            validate_quality_table_name,
        )
        table_validation = validate_quality_table_name(
            candidate.get("table_name"),
            super_name=self.sup,
            allow_wildcard=candidate.get("rule_type") != "custom_sql",
        )
        if not table_validation.valid:
            raise ValueError(
                f"invalid quality rule ({table_validation.code}): "
                f"{table_validation.message}"
            )
        table_fqn = (
            None
            if candidate["table_name"] == "*"
            else quality_table_fqn(self.sup, candidate["table_name"])
        )
        validation = validate_custom_rule(
            candidate,
            table_fqn=table_fqn,
        )
        if not validation.valid:
            raise ValueError(f"invalid quality rule ({validation.code}): {validation.message}")
        candidate["created_by"] = created_by
        candidate["created_at"] = _now_iso()
        document_key = self._key("rules", "doc", rule_id)
        index_key = self._key("rules", "index")
        root_key = RK.meta_root(self.org, self.sup)
        namespace_intent_key = RK.meta_namespace_deletion_intent(
            self.org, self.sup,
        )
        payload = json.dumps(candidate, default=str)
        pipe = None
        try:
            # WATCH prevents a caller-supplied ID from turning create into an
            # undocumented upsert. Watching both authorities also treats an
            # indexed-but-missing legacy document as occupied instead of
            # silently repairing/overwriting uncertain state.
            pipe = self.r.pipeline()
            while True:
                try:
                    pipe.watch(
                        document_key,
                        index_key,
                        root_key,
                        namespace_intent_key,
                    )
                    self._require_watched_namespace_live(pipe)
                    index_type = pipe.type(index_key)
                    if isinstance(index_type, bytes):
                        index_type = index_type.decode("ascii", errors="strict")
                    if index_type not in ("none", "set"):
                        # EXEC does not roll back an earlier successful SET
                        # when the following SADD fails with WRONGTYPE.
                        raise DQConfigReadError(
                            "quality rule index has the wrong Redis type"
                        )
                    if pipe.exists(document_key) or pipe.sismember(index_key, rule_id):
                        raise ValueError(
                            f"quality rule {rule_id!r} already exists"
                        )
                    pipe.multi()
                    pipe.set(document_key, payload)
                    pipe.sadd(index_key, rule_id)
                    results = pipe.execute()
                    break
                except redis.WatchError:
                    # Re-read both watched authorities. A competing creator
                    # will be reported as a duplicate on the next iteration.
                    continue
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"[dq-config] create_rule error: {e}")
            raise RuntimeError(
                f"could not persist quality rule {rule_id!r}"
            ) from e
        finally:
            if pipe is not None:
                try:
                    pipe.reset()
                except Exception:
                    pass
        if (
            not isinstance(results, (list, tuple))
            or len(results) != 2
            or not results[0]
            or isinstance(results[1], bool)
            or not isinstance(results[1], int)
            or results[1] != 1
        ):
            logger.error(
                "[dq-config] create_rule returned invalid transaction result: %r",
                results,
            )
            raise RuntimeError(f"could not persist quality rule {rule_id!r}")
        return candidate

    def update_rule(
        self,
        rule_id: str,
        updates: Dict[str, Any],
        *,
        expected_fingerprint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(updates, dict):
            raise ValueError("quality rule updates must be an object")
        requested_updates = deepcopy(updates)
        document_key = self._key("rules", "doc", rule_id)
        index_key = self._key("rules", "index")
        root_key = RK.meta_root(self.org, self.sup)
        namespace_intent_key = RK.meta_namespace_deletion_intent(
            self.org, self.sup,
        )
        pipe = None
        try:
            pipe = self.r.pipeline()
            while True:
                try:
                    # Both the document and discoverability index are
                    # authorities. A delete or repair racing this update must
                    # invalidate EXEC so SET can never recreate an orphan.
                    pipe.watch(
                        document_key,
                        index_key,
                        root_key,
                        namespace_intent_key,
                    )
                    self._require_watched_namespace_live(pipe)
                    raw = pipe.get(document_key)
                    indexed = bool(pipe.sismember(index_key, rule_id))
                    if raw is None:
                        if indexed:
                            raise DQConfigReadError(
                                f"indexed quality rule {rule_id!r} is missing"
                            )
                        return None
                    if not indexed:
                        raise DQConfigReadError(
                            f"quality rule {rule_id!r} is not indexed"
                        )
                    try:
                        existing = json.loads(raw)
                    except (TypeError, ValueError, UnicodeError) as exc:
                        raise DQConfigReadError(
                            f"persisted quality rule {rule_id!r} is malformed"
                        ) from exc
                    if not isinstance(existing, dict):
                        raise DQConfigReadError(
                            f"persisted quality rule {rule_id!r} has the wrong shape"
                        )
                    if expected_fingerprint is not None:
                        actual_fingerprint = self.rule_fingerprint(existing)
                        if not hmac.compare_digest(
                            actual_fingerprint, str(expected_fingerprint),
                        ):
                            raise DQConfigConflictError(
                                f"quality rule {rule_id!r} changed before update"
                            )

                    candidate = deepcopy(existing)
                    candidate.update(deepcopy(requested_updates))
                    # The Redis document/index identity is immutable.
                    candidate["rule_id"] = rule_id
                    candidate.setdefault("severity", "warning")
                    if not isinstance(candidate.get("enabled"), bool):
                        raise ValueError("quality rule enabled must be a boolean")
                    from supertable.quality.checker import (
                        quality_table_fqn,
                        validate_custom_rule,
                        validate_quality_table_name,
                    )
                    # Always permit an invalid legacy document to be disabled.
                    # Enabling or otherwise updating an active rule requires
                    # full validation.
                    if candidate["enabled"]:
                        table_validation = validate_quality_table_name(
                            candidate.get("table_name"),
                            super_name=self.sup,
                            allow_wildcard=candidate.get("rule_type") != "custom_sql",
                        )
                        if not table_validation.valid:
                            raise ValueError(
                                f"invalid quality rule ({table_validation.code}): "
                                f"{table_validation.message}"
                            )
                        table_fqn = (
                            None
                            if candidate["table_name"] == "*"
                            else quality_table_fqn(self.sup, candidate["table_name"])
                        )
                        validation = validate_custom_rule(
                            candidate,
                            table_fqn=table_fqn,
                        )
                        if not validation.valid:
                            raise ValueError(
                                f"invalid quality rule ({validation.code}): "
                                f"{validation.message}"
                            )
                    # Disabling is the remediation path for recognisable
                    # legacy rules, not permission to persist an arbitrary
                    # document that poisons the indexed inventory.  Enforce
                    # identity, known type, table scope, and minimum shape for
                    # both enabled and disabled candidates; only the deeper
                    # execution validator above is relaxed while disabled.
                    self._validate_rule_inventory_document(rule_id, candidate)
                    candidate["updated_at"] = _now_iso()
                    pipe.multi()
                    pipe.set(
                        document_key,
                        json.dumps(candidate, default=str),
                    )
                    results = pipe.execute()
                    break
                except redis.WatchError:
                    # Re-read both authorities. A completed concurrent delete
                    # becomes a normal absent result on the next iteration.
                    continue
        except (DQConfigConflictError, DQConfigReadError, ValueError):
            raise
        except Exception as e:
            logger.error(f"[dq-config] update_rule error: {e}")
            raise RuntimeError(
                f"could not persist quality rule {rule_id!r} update"
            ) from e
        finally:
            if pipe is not None:
                try:
                    pipe.reset()
                except Exception:
                    pass
        if (
            not isinstance(results, (list, tuple))
            or len(results) != 1
            or not results[0]
        ):
            logger.error(
                "[dq-config] update_rule transaction did not acknowledge "
                "persistence for %s: %r",
                rule_id,
                results,
            )
            raise RuntimeError(
                f"could not persist quality rule {rule_id!r} update"
            )
        return candidate

    def delete_rule(
        self,
        rule_id: str,
        *,
        expected_fingerprint: Optional[str] = None,
    ) -> bool:
        pipe = None
        try:
            document_key = self._key("rules", "doc", rule_id)
            index_key = self._key("rules", "index")
            root_key = RK.meta_root(self.org, self.sup)
            namespace_intent_key = RK.meta_namespace_deletion_intent(
                self.org, self.sup,
            )
            pipe = self.r.pipeline()
            while True:
                try:
                    pipe.watch(
                        document_key,
                        index_key,
                        root_key,
                        namespace_intent_key,
                    )
                    self._require_watched_namespace_live(pipe)
                    index_type = pipe.type(index_key)
                    if isinstance(index_type, bytes):
                        index_type = index_type.decode("ascii", errors="strict")
                    if index_type not in ("none", "set"):
                        # Redis transactions do not roll back earlier commands
                        # after a later command returns WRONGTYPE.  Entering
                        # MULTI with a string-valued index would therefore let
                        # DEL remove the document before SREM failed.
                        raise DQConfigReadError(
                            "quality rule index has the wrong Redis type"
                        )
                    raw = pipe.get(document_key)
                    indexed = bool(pipe.sismember(index_key, rule_id))
                    if raw is None:
                        if indexed:
                            raise DQConfigReadError(
                                f"indexed quality rule {rule_id!r} is missing"
                            )
                        return False
                    if not indexed:
                        raise DQConfigReadError(
                            f"quality rule {rule_id!r} is not indexed"
                        )
                    if expected_fingerprint is not None:
                        try:
                            existing = json.loads(raw)
                        except (TypeError, ValueError, UnicodeError) as exc:
                            raise DQConfigReadError(
                                f"persisted quality rule {rule_id!r} is malformed"
                            ) from exc
                        if not isinstance(existing, dict):
                            raise DQConfigReadError(
                                f"persisted quality rule {rule_id!r} has the wrong shape"
                            )
                        actual_fingerprint = self.rule_fingerprint(existing)
                        if not hmac.compare_digest(
                            actual_fingerprint, str(expected_fingerprint),
                        ):
                            raise DQConfigConflictError(
                                f"quality rule {rule_id!r} changed before delete"
                            )
                    pipe.multi()
                    pipe.delete(document_key)
                    pipe.srem(index_key, rule_id)
                    results = pipe.execute()
                    break
                except redis.WatchError:
                    continue
        except DQConfigConflictError:
            raise
        except Exception as e:
            logger.error(f"[dq-config] delete_rule error: {e}")
            return False
        finally:
            if pipe is not None:
                try:
                    pipe.reset()
                except Exception:
                    pass
        return bool(
            isinstance(results, (list, tuple))
            and len(results) == 2
            and all(
                isinstance(result, int)
                and not isinstance(result, bool)
                and result == 1
                for result in results
            )
        )

    def list_rules_for_table(self, table: str) -> List[Dict[str, Any]]:
        all_rules = self.list_rules()
        return [r for r in all_rules
                if r.get("enabled") and r.get("table_name") in (table, "*")]

    # ── Schedule config ───────────────────────────────────────────────

    def get_schedule(self) -> Dict[str, Any]:
        defaults = {
            "quick_cron": "0 */4 * * *",
            "deep_cron": "0 2 * * *",
            "custom_cron": "0 */6 * * *",
            "post_ingest": True,
            "post_ingest_quick": True,
            "post_ingest_custom": True,
            "post_ingest_deep": False,
            "enabled": True,
            "timezone": "UTC",
        }
        stored = self._read_json(
            self._key("schedule"),
            expected_type=dict,
            missing={},
            label="global quality schedule",
        )
        self._validate_schedule_document(stored, "global quality schedule")
        defaults.update(stored)
        return defaults

    def set_schedule(self, schedule: Dict[str, Any]) -> bool:
        try:
            if not isinstance(schedule, dict):
                return False
            self._validate_schedule_document(schedule, "global quality schedule")
            stored = deepcopy(schedule)
            stored["updated_at"] = _now_iso()
            persisted = self._mutate_if_live(
                self._key("schedule"),
                table=None,
                payload=json.dumps(stored, default=str),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_schedule error: {e}")
            return False

    def get_table_schedule(self, table: str) -> Optional[Dict[str, Any]]:
        stored = self._read_json(
            self._key("schedule", table),
            expected_type=dict,
            missing=None,
            label=f"quality schedule for table {table!r}",
        )
        if stored is not None:
            self._validate_schedule_document(
                stored,
                f"quality schedule for table {table!r}",
            )
        return stored

    def set_table_schedule(self, table: str, schedule: Dict[str, Any]) -> bool:
        try:
            if not isinstance(schedule, dict):
                return False
            self._validate_schedule_document(
                schedule,
                f"quality schedule for table {table!r}",
            )
            stored = deepcopy(schedule)
            stored["updated_at"] = _now_iso()
            persisted = self._mutate_if_live(
                self._key("schedule", table),
                table=table,
                payload=json.dumps(stored, default=str),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_table_schedule error: {e}")
            return False

    def delete_table_schedule(self, table: str) -> bool:
        """Remove a per-table schedule override (reverts to global)."""
        try:
            return self._mutate_if_live(
                self._key("schedule", table), table=table, payload=None,
            )
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
                if (
                    not isinstance(values, (list, tuple))
                    or len(values) != len(keys_found)
                ):
                    raise DQConfigReadError(
                        "quality table schedule inventory read was incomplete"
                    )
                for key, raw in zip(keys_found, values):
                    k = key if isinstance(key, str) else key.decode("utf-8")
                    suffix = k[len(prefix):]
                    if raw is None:
                        raise DQConfigReadError(
                            f"quality schedule for table {suffix!r} disappeared "
                            "during inventory read"
                        )
                    try:
                        document = json.loads(raw)
                    except (TypeError, ValueError, UnicodeError) as exc:
                        raise DQConfigReadError(
                            f"quality schedule for table {suffix!r} is malformed"
                        ) from exc
                    if not isinstance(document, dict):
                        raise DQConfigReadError(
                            f"quality schedule for table {suffix!r} has the wrong shape"
                        )
                    self._validate_schedule_document(
                        document,
                        f"quality schedule for table {suffix!r}",
                    )
                    results[suffix] = document
        except DQConfigReadError:
            raise
        except Exception as exc:
            logger.error("[dq-config] get_all_table_schedules error: %s", exc)
            raise DQConfigReadError(
                "could not read quality table schedule inventory"
            ) from exc
        return results

    # ── Latest results (fast UI cache) ────────────────────────────────

    def get_latest(self, table: str) -> Optional[Dict[str, Any]]:
        return self._read_json(
            self._key("latest", table),
            expected_type=dict,
            missing=None,
            label=f"latest quality result for table {table!r}",
        )

    def set_latest(self, table: str, result: Dict[str, Any]) -> bool:
        try:
            persisted = self._mutate_if_live(
                self._key("latest", table),
                table=table,
                payload=json.dumps(
                    normalize_json_value(result),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_latest error: {e}")
            return False

    def get_latest_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        return self._read_json(
            self._key("latest", table, column),
            expected_type=dict,
            missing=None,
            label=f"latest quality result for {table!r}.{column!r}",
        )

    def set_latest_column(self, table: str, column: str, result: Dict[str, Any]) -> bool:
        try:
            persisted = self._mutate_if_live(
                self._key("latest", table, column),
                table=table,
                payload=json.dumps(
                    normalize_json_value(result),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
            return bool(persisted)
        except Exception as e:
            logger.error(f"[dq-config] set_latest_column error: {e}")
            return False

    def get_anomalies(self, table: str) -> List[Dict[str, Any]]:
        return self._read_json(
            self._key("anomalies", table),
            expected_type=list,
            missing=[],
            label=f"quality anomalies for table {table!r}",
        )

    def set_anomalies(self, table: str, anomalies: List[Dict[str, Any]]) -> bool:
        try:
            persisted = self._mutate_if_live(
                self._key("anomalies", table),
                table=table,
                payload=json.dumps(
                    normalize_json_value(anomalies),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
            return bool(persisted)
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
                if (
                    not isinstance(values, (list, tuple))
                    or len(values) != len(table_keys)
                ):
                    raise DQConfigReadError(
                        "latest quality result inventory read was incomplete"
                    )
                for (_, table_name), raw in zip(table_keys, values):
                    if raw is None:
                        raise DQConfigReadError(
                            f"latest quality result for table {table_name!r} "
                            "disappeared during inventory read"
                        )
                    try:
                        document = json.loads(raw)
                    except (TypeError, ValueError, UnicodeError) as exc:
                        raise DQConfigReadError(
                            f"latest quality result for table {table_name!r} "
                            "is malformed"
                        ) from exc
                    if not isinstance(document, dict):
                        raise DQConfigReadError(
                            f"latest quality result for table {table_name!r} "
                            "has the wrong shape"
                        )
                    results[table_name] = document
        except DQConfigReadError:
            raise
        except Exception as exc:
            logger.error("[dq-config] get_all_latest error: %s", exc)
            raise DQConfigReadError(
                "could not read latest quality result inventory"
            ) from exc
        return results
