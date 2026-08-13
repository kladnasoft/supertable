"""Safe, bounded cross-engine query telemetry for adaptive routing.

The normal monitoring record is intentionally diagnostic and may contain a
sanitized operator tree.  AUTO feedback is a separate scalar-only record: it
contains no SQL, identifiers, paths, literals, or error messages and is stored
in bounded Redis ZSETs.  Historical feedback can affect performance ranking,
never an engine's correctness/capability eligibility.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

from supertable import redis_keys as RK
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.engine.adaptive_router import EngineHistory
from supertable.engine.engine_enum import Engine


PROFILE_SCHEMA_VERSION = 1
OBSERVATION_SCHEMA_VERSION = 1
MAX_SANITIZED_PROFILE_BYTES = 64 * 1024
MAX_PROFILE_DEPTH = 8
MAX_PROFILE_ITEMS = 96
MAX_PROFILE_STRING = 2_048
MAX_OBSERVATION_SIGNATURES_HARD = 16_384
MAX_OBSERVATION_SAMPLES_HARD = 1_024
MAX_OBSERVATION_TTL_DAYS_HARD = 365
_EWMA_NEW_WEIGHT_PERMILLE = 350

_ENGINE_VALUES = frozenset(engine.value for engine in Engine if engine != Engine.AUTO)
_REQUESTED_ENGINE_VALUES = _ENGINE_VALUES | {Engine.AUTO.value, "unknown"}
_STATUS_VALUES = frozenset({"ok", "error", "timeout", "cancelled"})
_DURATION_SOURCES = frozenset({
    "engine_profile", "executing_query", "total_execute", "unknown",
})
_HEX_16_64 = re.compile(r"^[0-9a-f]{16,64}$")
_URL_RE = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)(x-amz-(?:signature|credential|security-token)|"
    r"sig|signature|sas|token|password|secret|access[_-]?key)="
    r"([^&\s,;]+)"
)
_SENSITIVE_KEY_RE = re.compile(
    r"(?i)(authorization|cookie|password|passwd|secret|token|signature|"
    r"credential|access[_-]?key|session[_-]?key|sas)"
)


def _finite_number(value: object) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: object, *, maximum: int = (1 << 63) - 1) -> int:
    number = _finite_number(value)
    if number is None:
        return 0
    return min(maximum, max(0, int(number)))


def _redact_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    trailing = raw[len(raw.rstrip(").,;]}")) :]
    url = raw[: len(raw) - len(trailing)] if trailing else raw
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname or ""
        if not hostname:
            raise ValueError("missing hostname")
        host = f"[{hostname}]" if ":" in hostname else hostname
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        query = "<redacted>" if parsed.query or parsed.fragment else ""
        clean = urlunsplit((parsed.scheme, host, parsed.path, query, ""))
    except Exception:
        clean = url.split("?", 1)[0]
        if "?" in url:
            clean += "?<redacted>"
    return clean + trailing


def redact_text(value: object, *, limit: int = MAX_PROFILE_STRING) -> str:
    """Remove URL credentials/query strings and common secret assignments."""
    text = str(value or "")
    text = _URL_RE.sub(_redact_url, text)
    text = _SECRET_ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}=<redacted>", text)
    if len(text) > max(0, int(limit)):
        text = text[: max(0, int(limit))] + "<truncated>"
    return text


def canonical_sql_shape(query: object, *, limit: int = 2_000) -> str:
    """Return SQL structure with literals erased; never retain raw fallback."""
    text = str(query or "")
    if not text:
        return ""
    try:
        import sqlglot
        from sqlglot import exp

        root = sqlglot.parse_one(text, read="duckdb")

        def erase(node):
            if isinstance(node, (exp.Literal, exp.Boolean, exp.Null)):
                return exp.Placeholder()
            return node

        text = root.transform(erase, copy=True).sql(
            dialect="duckdb", pretty=False,
        )
    except Exception:
        # A parse failure must not cause potentially sensitive literals to be
        # retained.  The digest still permits correlation without disclosure.
        text = f"<unparsed-sql sha256={hashlib.sha256(text.encode()).hexdigest()[:16]}>"
    return redact_text(text, limit=limit)


class _SanitizeBudget:
    def __init__(self, limit: int):
        self.remaining = max(256, int(limit))

    def take(self, amount: int) -> bool:
        amount = max(0, int(amount))
        if amount > self.remaining:
            self.remaining = 0
            return False
        self.remaining -= amount
        return True


def _sanitize(value: object, budget: _SanitizeBudget, depth: int) -> object:
    if depth > MAX_PROFILE_DEPTH or budget.remaining <= 0:
        return "<truncated>"
    if value is None or isinstance(value, bool):
        budget.take(5)
        return value
    if isinstance(value, int):
        budget.take(24)
        return max(-(1 << 63), min((1 << 63) - 1, value))
    if isinstance(value, float):
        budget.take(24)
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        clean = redact_text(value)
        if not budget.take(len(clean.encode("utf-8")) + 2):
            return "<truncated>"
        return clean
    if isinstance(value, Mapping):
        result: Dict[str, object] = {}
        for index, (raw_key, child) in enumerate(value.items()):
            if index >= MAX_PROFILE_ITEMS or budget.remaining <= 0:
                result["_truncated"] = True
                break
            key = redact_text(raw_key, limit=128)
            if not budget.take(len(key.encode("utf-8")) + 4):
                result["_truncated"] = True
                break
            result[key] = (
                "<redacted>"
                if _SENSITIVE_KEY_RE.search(key)
                else _sanitize(child, budget, depth + 1)
            )
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        result = []
        for index, child in enumerate(value):
            if index >= MAX_PROFILE_ITEMS or budget.remaining <= 0:
                result.append("<truncated>")
                break
            result.append(_sanitize(child, budget, depth + 1))
        return result
    return _sanitize(str(value), budget, depth + 1)


def sanitize_profile(
    value: object, *, max_bytes: int = MAX_SANITIZED_PROFILE_BYTES,
) -> object:
    """Recursively redact and cap an engine profile before persistence."""
    capped = min(MAX_SANITIZED_PROFILE_BYTES, max(256, int(max_bytes)))
    result = _sanitize(value, _SanitizeBudget(capped - 128), 0)
    try:
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return {"_truncated": True, "reason": "serialization_failed"}
    if len(encoded.encode("utf-8")) <= capped:
        return result
    return {
        "_truncated": True,
        "sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "original_bytes": len(encoded.encode("utf-8")),
    }


def flatten_plan_stats(plan_stats: object) -> Dict[str, object]:
    flattened: Dict[str, object] = {}
    entries = getattr(plan_stats, "stats", plan_stats)
    if not isinstance(entries, (list, tuple)):
        return flattened
    for entry in entries:
        if isinstance(entry, Mapping):
            for key, value in entry.items():
                flattened[str(key)] = value
    return flattened


def _timing_seconds(timing: object, name: str) -> Optional[float]:
    values: Iterable[object]
    if isinstance(timing, Mapping):
        values = (timing,)
    elif isinstance(timing, (list, tuple)):
        values = timing
    else:
        return None
    found: Optional[float] = None
    for item in values:
        if not isinstance(item, Mapping) or name not in item:
            continue
        number = _finite_number(item.get(name))
        if number is not None and number >= 0:
            found = number
    return found


def _engine(value: object, *, requested: bool = False) -> str:
    normalized = str(value or "").strip().casefold()
    allowed = _REQUESTED_ENGINE_VALUES if requested else _ENGINE_VALUES
    return normalized if normalized in allowed else "unknown"


def _status(value: object) -> str:
    normalized = str(value or "error").strip().casefold()
    if normalized in _STATUS_VALUES:
        return normalized
    if "timeout" in normalized:
        return "timeout"
    if "cancel" in normalized:
        return "cancelled"
    return "ok" if normalized in {"success", "successful"} else "error"


def _profile_metrics(profile: Mapping[str, object], actual_engine: str) -> Dict[str, object]:
    metrics: Dict[str, object] = {
        "engine_wall_us": 0,
        "duration_source": "unknown",
        "duration_measured": False,
        "cpu_time_us": 0,
        "actual_scan_bytes": 0,
        "actual_scan_bytes_measured": False,
        "result_bytes": 0,
        "result_bytes_measured": False,
        "peak_memory_bytes": 0,
        "spill_bytes": 0,
        "spill_bytes_measured": False,
        "rows_scanned": 0,
        "logical_scan_bytes": 0,
        "logical_scan_bytes_complete": False,
        "physical_read_bytes": 0,
        "physical_read_bytes_measured": False,
        "peak_memory_scope": "unknown",
    }
    if actual_engine.startswith("duckdb"):
        latency = _finite_number(profile.get("latency"))
        if latency is not None and latency >= 0:
            metrics.update(
                engine_wall_us=_integer(latency * 1_000_000),
                duration_source="engine_profile",
                duration_measured=True,
            )
        metrics["cpu_time_us"] = _integer(
            (_finite_number(profile.get("cpu_time")) or 0) * 1_000_000,
        )
        if _finite_number(profile.get("total_bytes_read")) is not None:
            metrics["actual_scan_bytes"] = _integer(profile.get("total_bytes_read"))
            metrics["actual_scan_bytes_measured"] = True
            metrics["physical_read_bytes"] = metrics["actual_scan_bytes"]
            metrics["physical_read_bytes_measured"] = True
        if _finite_number(profile.get("result_set_size")) is not None:
            metrics["result_bytes"] = _integer(profile.get("result_set_size"))
            metrics["result_bytes_measured"] = True
        metrics["peak_memory_bytes"] = _integer(
            profile.get("system_peak_buffer_memory"),
        )
        metrics["peak_memory_scope"] = "engine_buffer_manager"
        if _finite_number(profile.get("system_peak_temp_dir_size")) is not None:
            metrics["spill_bytes"] = _integer(profile.get("system_peak_temp_dir_size"))
            metrics["spill_bytes_measured"] = True
        metrics["rows_scanned"] = _integer(profile.get("cumulative_rows_scanned"))
    elif actual_engine == Engine.ISLANDDB.value:
        elapsed_ms = _finite_number(profile.get("elapsed_ms"))
        if elapsed_ms is not None and elapsed_ms >= 0:
            metrics.update(
                engine_wall_us=_integer(elapsed_ms * 1_000),
                duration_source="engine_profile",
                duration_measured=True,
            )
        metrics["cpu_time_us"] = _integer(
            (_finite_number(profile.get("cpu_time_ms")) or 0) * 1_000,
        )
        metrics["logical_scan_bytes"] = _integer(
            profile.get("logical_scan_bytes")
        )
        metrics["logical_scan_bytes_complete"] = bool(
            profile.get("logical_scan_bytes_complete", False)
        )
        if _finite_number(profile.get("physical_read_bytes")) is not None:
            metrics["physical_read_bytes"] = _integer(
                profile.get("physical_read_bytes")
            )
            metrics["physical_read_bytes_measured"] = bool(
                profile.get("physical_read_bytes_measured", False)
            )
        cache = profile.get("cache")
        if isinstance(cache, Mapping):
            remote = (
                _integer(cache.get("range_remote_bytes"))
                + _integer(cache.get("downloaded_bytes"))
            )
            if remote or (
                "range_remote_bytes" in cache or "downloaded_bytes" in cache
            ):
                metrics["actual_scan_bytes"] = remote
                metrics["actual_scan_bytes_measured"] = True
        resources = profile.get("resources")
        metrics["peak_memory_bytes"] = _integer(profile.get("peak_memory_bytes"))
        metrics["peak_memory_scope"] = redact_text(
            profile.get("peak_memory_scope"), limit=32,
        )
        if _finite_number(profile.get("spill_bytes")) is not None:
            metrics["spill_bytes"] = _integer(profile.get("spill_bytes"))
            metrics["spill_bytes_measured"] = bool(
                profile.get("spill_bytes_measured", False)
            )
        metrics["rows_scanned"] = _integer(profile.get("rows_scanned"))
    return metrics


@dataclass(frozen=True)
class NormalizedQueryProfile:
    schema_version: int
    query_shape_hash: str
    feature_signature: str
    requested_engine: str
    selected_engine: str
    actual_engine: str
    forced: bool
    fallback: bool
    status: str
    policy_version: str
    duration_source: str
    duration_measured: bool
    engine_wall_us: int
    total_wall_us: int
    cpu_time_us: int
    estimated_scan_bytes: int
    actual_scan_bytes: int
    actual_scan_bytes_measured: bool
    logical_scan_bytes: int
    logical_scan_bytes_complete: bool
    physical_read_bytes: int
    physical_read_bytes_measured: bool
    decoded_bytes: int
    decoded_bytes_complete: bool
    work_bytes: int
    total_files: int
    selected_row_groups: int
    result_rows: int
    result_columns: int
    result_bytes: int
    result_bytes_measured: bool
    peak_memory_bytes: int
    peak_memory_scope: str
    spill_bytes: int
    spill_bytes_measured: bool
    cache_hits: int
    cache_hit_bytes: int
    cache_remote_bytes: int
    rows_scanned: int
    routing_decision: object

    def as_dict(self) -> Dict[str, object]:
        return asdict(self)


def normalize_query_profile(
    *, query: object, requested_engine: object, timing: object,
    plan_stats: object, status: object, result_shape: object,
    engine_profile: object,
) -> NormalizedQueryProfile:
    stats = flatten_plan_stats(plan_stats)
    routing = stats.get("AUTO_ROUTING")
    routing = routing if isinstance(routing, Mapping) else {}
    outcome = stats.get("AUTO_ROUTING_OUTCOME")
    outcome = outcome if isinstance(outcome, Mapping) else {}
    features = routing.get("features")
    features = features if isinstance(features, Mapping) else {}

    request_stat = stats.get("ENGINE_REQUEST")
    request_stat = request_stat if isinstance(request_stat, Mapping) else {}
    attempt_stat = stats.get("ENGINE_ATTEMPT")
    attempt_stat = attempt_stat if isinstance(attempt_stat, Mapping) else {}
    actual = _engine(stats.get("ENGINE") or attempt_stat.get("engine"))
    requested = _engine(
        requested_engine or request_stat.get("requested_engine"),
        requested=True,
    )
    if requested == "unknown":
        requested = Engine.AUTO.value if routing else actual
    selected = _engine(
        outcome.get("selected_engine") or routing.get("selected_engine")
        or request_stat.get("selected_engine")
        or attempt_stat.get("engine") or actual,
    )
    outcome_actual = _engine(outcome.get("actual_engine"))
    if outcome_actual != "unknown":
        actual = outcome_actual
    fallback = bool(outcome.get("fallback", selected != actual))
    forced = bool(
        request_stat.get("forced", requested != Engine.AUTO.value)
    )

    profile = engine_profile if isinstance(engine_profile, Mapping) else {}
    metrics = _profile_metrics(profile, actual)
    if _finite_number(stats.get("RESULT_BYTES")) is not None:
        metrics["result_bytes"] = _integer(stats.get("RESULT_BYTES"))
        metrics["result_bytes_measured"] = True
    executing_s = _timing_seconds(timing, "EXECUTING_QUERY")
    total_s = _timing_seconds(timing, "TOTAL_EXECUTE")
    if not metrics["engine_wall_us"] and executing_s is not None:
        metrics.update(
            engine_wall_us=_integer(executing_s * 1_000_000),
            duration_source="executing_query",
            duration_measured=True,
        )
    elif not metrics["engine_wall_us"] and total_s is not None:
        metrics.update(
            engine_wall_us=_integer(total_s * 1_000_000),
            duration_source="total_execute",
            duration_measured=True,
        )

    scan = _integer(
        features.get("effective_scan_bytes")
        if features else stats.get("ROW_GROUP_SCAN_SIZE") or stats.get("REFLECTION_SIZE")
    )
    decoded = _integer(
        features.get("decoded_bytes") if features else stats.get("DECODED_SIZE")
    )
    decoded_complete = bool(
        features.get("decoded_bytes_complete")
        if features else stats.get("DECODED_SIZE_COMPLETE", False)
    )
    work_bytes = scan + (decoded if decoded_complete else max(decoded, scan))

    island_cache = stats.get("ISLAND_CACHE")
    island_cache = island_cache if isinstance(island_cache, Mapping) else {}
    cache_hits = _integer(stats.get("FILE_CACHE_HITS")) + _integer(
        island_cache.get("range_cache_hit_chunks")
        or island_cache.get("cache_hit_chunks")
    )
    cache_hit_bytes = _integer(
        island_cache.get("range_cache_hit_bytes")
        or island_cache.get("cache_hit_bytes")
    )
    cache_remote_bytes = _integer(
        island_cache.get("range_remote_bytes")
        or island_cache.get("remote_bytes")
    ) + _integer(stats.get("FILE_CACHE_DOWNLOADED_BYTES"))

    shape_hash = str(
        features.get("query_shape_hash") or routing.get("query_shape_hash") or ""
    )
    if not _HEX_16_64.fullmatch(shape_hash):
        from supertable.engine.adaptive_router import query_shape_hash
        shape_hash = query_shape_hash(query)
    signature = str(routing.get("feature_signature") or "")
    if not _HEX_16_64.fullmatch(signature):
        signature = ""
    rows, columns = (0, 0)
    if isinstance(result_shape, (tuple, list)) and len(result_shape) >= 2:
        rows, columns = _integer(result_shape[0]), _integer(result_shape[1])

    return NormalizedQueryProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        query_shape_hash=shape_hash,
        feature_signature=signature,
        requested_engine=requested,
        selected_engine=selected,
        actual_engine=actual,
        forced=forced,
        fallback=fallback,
        status=_status(status),
        policy_version=redact_text(routing.get("policy_version"), limit=64),
        duration_source=str(metrics["duration_source"]),
        duration_measured=bool(metrics["duration_measured"]),
        engine_wall_us=_integer(metrics["engine_wall_us"]),
        total_wall_us=_integer((total_s or 0) * 1_000_000),
        cpu_time_us=_integer(metrics["cpu_time_us"]),
        estimated_scan_bytes=scan,
        actual_scan_bytes=_integer(metrics["actual_scan_bytes"]),
        actual_scan_bytes_measured=bool(metrics["actual_scan_bytes_measured"]),
        logical_scan_bytes=_integer(metrics["logical_scan_bytes"]),
        logical_scan_bytes_complete=bool(metrics["logical_scan_bytes_complete"]),
        physical_read_bytes=_integer(metrics["physical_read_bytes"]),
        physical_read_bytes_measured=bool(metrics["physical_read_bytes_measured"]),
        decoded_bytes=decoded,
        decoded_bytes_complete=decoded_complete,
        work_bytes=work_bytes,
        total_files=_integer(
            features.get("total_files") if features else stats.get("REFLECTIONS")
        ),
        selected_row_groups=_integer(
            features.get("selected_row_groups")
            if features else stats.get("ISLAND_SELECTED_ROW_GROUPS")
        ),
        result_rows=rows,
        result_columns=columns,
        result_bytes=_integer(metrics["result_bytes"]),
        result_bytes_measured=bool(metrics["result_bytes_measured"]),
        peak_memory_bytes=_integer(metrics["peak_memory_bytes"]),
        peak_memory_scope=str(metrics["peak_memory_scope"]),
        spill_bytes=_integer(metrics["spill_bytes"]),
        spill_bytes_measured=bool(metrics["spill_bytes_measured"]),
        cache_hits=cache_hits,
        cache_hit_bytes=cache_hit_bytes,
        cache_remote_bytes=cache_remote_bytes,
        rows_scanned=_integer(metrics["rows_scanned"]),
        routing_decision=sanitize_profile(routing, max_bytes=16 * 1024),
    )


@dataclass(frozen=True)
class QueryObservation:
    schema_version: int
    observation_id: str
    query_id: str
    recorded_at_ms: int
    query_shape_hash: str
    feature_signature: str
    requested_engine: str
    selected_engine: str
    actual_engine: str
    forced: bool
    fallback: bool
    status: str
    feedback_eligible: bool
    duration_us: int
    duration_source: str
    duration_measured: bool
    work_bytes: int
    estimated_scan_bytes: int
    actual_scan_bytes: int
    actual_scan_bytes_measured: bool
    decoded_bytes: int
    result_rows: int
    result_bytes: int
    spill_bytes: int
    cache_hit_bytes: int
    cache_remote_bytes: int
    policy_version: str

    @classmethod
    def from_profile(
        cls, profile: NormalizedQueryProfile, *, query_id: object,
        recorded_at_ms: Optional[int] = None,
    ) -> "QueryObservation":
        eligible = bool(
            profile.requested_engine == Engine.AUTO.value
            and profile.status == "ok"
            and not profile.fallback
            and profile.actual_engine in _ENGINE_VALUES
            and _HEX_16_64.fullmatch(profile.query_shape_hash)
            and _HEX_16_64.fullmatch(profile.feature_signature)
            and profile.duration_measured
            and profile.engine_wall_us > 0
            and profile.work_bytes > 0
        )
        return cls(
            schema_version=OBSERVATION_SCHEMA_VERSION,
            observation_id=uuid.uuid4().hex,
            query_id=redact_text(query_id, limit=64),
            recorded_at_ms=(
                _integer(recorded_at_ms)
                if recorded_at_ms is not None else int(time.time() * 1000)
            ),
            query_shape_hash=profile.query_shape_hash,
            feature_signature=profile.feature_signature,
            requested_engine=profile.requested_engine,
            selected_engine=profile.selected_engine,
            actual_engine=profile.actual_engine,
            forced=profile.forced,
            fallback=profile.fallback,
            status=profile.status,
            feedback_eligible=eligible,
            duration_us=profile.engine_wall_us,
            duration_source=profile.duration_source,
            duration_measured=profile.duration_measured,
            work_bytes=profile.work_bytes,
            estimated_scan_bytes=profile.estimated_scan_bytes,
            actual_scan_bytes=profile.actual_scan_bytes,
            actual_scan_bytes_measured=profile.actual_scan_bytes_measured,
            decoded_bytes=profile.decoded_bytes,
            result_rows=profile.result_rows,
            result_bytes=profile.result_bytes,
            spill_bytes=profile.spill_bytes,
            cache_hit_bytes=profile.cache_hit_bytes,
            cache_remote_bytes=profile.cache_remote_bytes,
            policy_version=redact_text(profile.policy_version, limit=64),
        )

    def as_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def parse(cls, value: object) -> Optional["QueryObservation"]:
        try:
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            doc = json.loads(value) if isinstance(value, str) else value
            if not isinstance(doc, Mapping):
                return None
            allowed = set(cls.__dataclass_fields__)
            if set(doc) != allowed:
                return None
            item = cls(**doc)
        except Exception:
            return None
        if (
            item.schema_version != OBSERVATION_SCHEMA_VERSION
            or not isinstance(item.observation_id, str)
            or not isinstance(item.query_id, str)
            or len(item.observation_id) > 64
            or len(item.query_id) > 64
            or not _HEX_16_64.fullmatch(item.query_shape_hash)
            or not _HEX_16_64.fullmatch(item.feature_signature)
            or item.requested_engine not in _REQUESTED_ENGINE_VALUES
            or item.selected_engine not in _ENGINE_VALUES
            or item.actual_engine not in _ENGINE_VALUES
            or item.status not in _STATUS_VALUES
            or item.duration_source not in _DURATION_SOURCES
            or not isinstance(item.feedback_eligible, bool)
            or not isinstance(item.duration_measured, bool)
            or not isinstance(item.forced, bool)
            or not isinstance(item.fallback, bool)
            or any(
                not isinstance(getattr(item, name), int)
                or isinstance(getattr(item, name), bool)
                or getattr(item, name) < 0
                for name in (
                    "recorded_at_ms", "duration_us", "work_bytes",
                    "estimated_scan_bytes", "actual_scan_bytes",
                    "decoded_bytes", "result_rows", "result_bytes",
                    "spill_bytes", "cache_hit_bytes", "cache_remote_bytes",
                )
            )
            or len(item.policy_version) > 64
        ):
            return None
        return item


class QueryObservationStore:
    """Best-effort bounded Redis feedback store for :class:`Executor`."""

    def __init__(
        self, organization: str, redis_client: object = None, *,
        enabled: Optional[bool] = None, max_samples: Optional[int] = None,
        ttl_days: Optional[int] = None, min_samples: Optional[int] = None,
        max_signatures: Optional[int] = None,
    ):
        self.organization = str(organization or "")
        self.enabled = (
            settings.SUPERTABLE_QUERY_OBSERVATIONS_ENABLED
            if enabled is None else bool(enabled)
        )
        self.max_samples = min(
            MAX_OBSERVATION_SAMPLES_HARD,
            max(1, int(
                settings.SUPERTABLE_QUERY_OBSERVATION_MAX_SAMPLES
                if max_samples is None else max_samples
            )),
        )
        self.ttl_days = min(
            MAX_OBSERVATION_TTL_DAYS_HARD,
            max(1, int(
                settings.SUPERTABLE_QUERY_OBSERVATION_TTL_DAYS
                if ttl_days is None else ttl_days
            )),
        )
        self.min_samples = min(
            self.max_samples,
            max(3, int(
                settings.SUPERTABLE_QUERY_OBSERVATION_MIN_SAMPLES
                if min_samples is None else min_samples
            )),
        )
        configured_signatures = (
            settings.SUPERTABLE_QUERY_OBSERVATION_MAX_SIGNATURES
            if max_signatures is None else max_signatures
        )
        self.max_signatures = min(
            MAX_OBSERVATION_SIGNATURES_HARD,
            max(1, int(configured_signatures)),
        )
        self._redis = redis_client

    def _client(self):
        if self._redis is None:
            from supertable.redis_connector import create_redis_client
            self._redis = create_redis_client()
        return self._redis

    @property
    def ttl_seconds(self) -> int:
        return self.ttl_days * 24 * 60 * 60

    def record(self, observation: QueryObservation) -> bool:
        """Append one eligible AUTO result; return false on any failure."""
        if (
            not self.enabled
            or not self.organization
            or not isinstance(observation, QueryObservation)
            or not observation.feedback_eligible
        ):
            return False
        parsed = QueryObservation.parse(observation.as_dict())
        if parsed is None:
            return False
        try:
            key = RK.query_observation_samples(
                self.organization, parsed.feature_signature,
                parsed.actual_engine,
            )
            index_key = RK.query_observation_index(self.organization)
            now_score = time.time_ns() / 1_000_000
            cutoff = int(now_score) - self.ttl_seconds * 1000
            member = json.dumps(
                parsed.as_dict(), ensure_ascii=False,
                separators=(",", ":"), sort_keys=True,
            )
            r = self._client()
            pipe = r.pipeline(transaction=True)
            pipe.zadd(key, {member: parsed.recorded_at_ms})
            pipe.zremrangebyscore(key, "-inf", cutoff - 1)
            pipe.zremrangebyrank(key, 0, -(self.max_samples + 1))
            pipe.expire(key, self.ttl_seconds)
            # Index recency is the server-side append time, not a caller-owned
            # timestamp from the observation document. Fractional milliseconds
            # prevent equal-score lexical eviction during a rapid signature
            # burst.
            pipe.zadd(index_key, {parsed.feature_signature: now_score})
            pipe.zremrangebyscore(index_key, "-inf", cutoff - 1)
            pipe.expire(index_key, self.ttl_seconds)
            pipe.execute()
            self._trim_signature_index(r, index_key)
            return True
        except Exception as exc:
            logger.debug("[query-observations] record skipped: %s", exc)
            return False

    def _trim_signature_index(self, r: object, index_key: str) -> None:
        try:
            count = int(r.zcard(index_key) or 0)
            overflow = count - self.max_signatures
            if overflow <= 0:
                return
            victims = list(r.zrange(index_key, 0, overflow - 1) or ())
            if not victims:
                return
            signatures = [
                victim.decode("utf-8") if isinstance(victim, bytes) else str(victim)
                for victim in victims
            ]
            sample_keys = [
                RK.query_observation_samples(
                    self.organization, signature, engine.value,
                )
                for signature in signatures
                for engine in Engine
                if engine != Engine.AUTO
            ]
            pipe = r.pipeline(transaction=True)
            if sample_keys:
                pipe.delete(*sample_keys)
            pipe.zrem(index_key, *signatures)
            pipe.execute()
        except Exception as exc:
            logger.debug("[query-observations] cardinality trim skipped: %s", exc)

    def load_history(
        self, query_shape_hash: str, feature_signature: str,
    ) -> Dict[Engine, EngineHistory]:
        """Return exact-bucket chronological EWMAs keyed by execution engine."""
        if (
            not self.enabled
            or not self.organization
            or not _HEX_16_64.fullmatch(str(query_shape_hash or ""))
            or not _HEX_16_64.fullmatch(str(feature_signature or ""))
        ):
            return {}
        cutoff = int(time.time() * 1000) - self.ttl_seconds * 1000
        histories: Dict[Engine, EngineHistory] = {}
        try:
            r = self._client()
            for engine in Engine:
                if engine == Engine.AUTO:
                    continue
                key = RK.query_observation_samples(
                    self.organization, feature_signature, engine.value,
                )
                raw = r.zrangebyscore(
                    key, cutoff, "+inf", withscores=True,
                ) or ()
                valid = []
                for member, score in raw[-self.max_samples:]:
                    item = QueryObservation.parse(member)
                    if (
                        item is None
                        or not item.feedback_eligible
                        or item.status != "ok"
                        or item.requested_engine != Engine.AUTO.value
                        or item.actual_engine != engine.value
                        or item.query_shape_hash != query_shape_hash
                        or item.feature_signature != feature_signature
                        or item.duration_us <= 0
                        or item.work_bytes <= 0
                    ):
                        continue
                    valid.append((float(score), item))
                valid.sort(key=lambda pair: (pair[0], pair[1].observation_id))
                if len(valid) < self.min_samples:
                    continue
                duration = valid[0][1].duration_us
                work = valid[0][1].work_bytes
                for _, item in valid[1:]:
                    duration = (
                        duration * (1000 - _EWMA_NEW_WEIGHT_PERMILLE)
                        + item.duration_us * _EWMA_NEW_WEIGHT_PERMILLE
                        + 500
                    ) // 1000
                    work = (
                        work * (1000 - _EWMA_NEW_WEIGHT_PERMILLE)
                        + item.work_bytes * _EWMA_NEW_WEIGHT_PERMILLE
                        + 500
                    ) // 1000
                count = len(valid)
                histories[engine] = EngineHistory(
                    sample_count=count,
                    success_count=count,
                    ewma_duration_us=max(1, duration),
                    ewma_work_bytes=max(1, work),
                    feature_signature=feature_signature,
                )
        except Exception as exc:
            logger.debug("[query-observations] history lookup skipped: %s", exc)
            return {}
        return histories

    def history_provider(self, features: object) -> Dict[Engine, EngineHistory]:
        """Adapter matching ``Executor(auto_history_provider=...)``."""
        try:
            return self.load_history(
                str(getattr(features, "query_shape_hash", "") or ""),
                str(features.feature_signature()),
            )
        except Exception:
            return {}


__all__ = [
    "NormalizedQueryProfile", "QueryObservation", "QueryObservationStore",
    "canonical_sql_shape", "flatten_plan_stats", "normalize_query_profile",
    "redact_text", "sanitize_profile",
]
