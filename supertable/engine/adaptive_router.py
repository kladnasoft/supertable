"""Deterministic, auditable AUTO engine selection.

The estimator and each engine's capability/resource planner remain the
authorities for *what is safe*.  This module only chooses the lowest predicted
latency among engines that passed those gates.  Keeping the policy pure makes
AUTO decisions reproducible, unit-testable, and suitable for storing beside a
query profile in Redis.

The cost model deliberately uses integer microseconds.  It is a conservative
deployment baseline, not a claim about exact wall-clock time.  Optional
``EngineHistory`` records can blend bounded, successful observations into the
baseline without ever overriding a safety rejection.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from typing import Dict, Mapping, Optional, Tuple

from supertable.engine.engine_enum import Engine


GIB = 1024 ** 3
MIB = 1024 ** 2
POLICY_VERSION = "adaptive-v1"


def _non_negative(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _transfer_us(byte_count: int, bytes_per_second: int) -> int:
    """Integer ceiling of a byte transfer/decode duration in microseconds."""
    byte_count = _non_negative(byte_count)
    bytes_per_second = max(1, int(bytes_per_second))
    return (byte_count * 1_000_000 + bytes_per_second - 1) // bytes_per_second


def query_shape_hash(query: object) -> str:
    """Hash canonical SQL structure while erasing literal values.

    Different point/range values can reuse observations, but predicates on a
    different column, joins, projection, grouping, and ordering remain distinct
    shapes.  Parse failures hash the original text; this function never decides
    capability and therefore must not make a valid DuckDB query unavailable.
    """
    return analyze_query_shape(query).shape_hash


@dataclass(frozen=True)
class QueryShape:
    shape_hash: str = ""
    has_join: bool = False
    has_sort: bool = False
    has_group_by: bool = False
    has_aggregate: bool = False
    literal_limit: int = 0
    projected_expressions: int = 0


def analyze_query_shape(query: object) -> QueryShape:
    """Return non-authoritative operator features for profiling/costing."""
    text = str(query or "")
    if not text:
        return QueryShape()
    canonical = text
    has_join = has_sort = has_group = has_aggregate = False
    literal_limit = projected_expressions = 0
    try:
        import sqlglot
        from sqlglot import exp

        root = sqlglot.parse_one(text, read="duckdb")
        has_join = next(root.find_all(exp.Join), None) is not None
        has_sort = root.args.get("order") is not None
        has_group = root.args.get("group") is not None
        has_aggregate = next(root.find_all(exp.AggFunc), None) is not None
        if isinstance(root, exp.Select):
            projected_expressions = len(root.expressions)
        limit = root.args.get("limit")
        if limit is not None:
            expression = limit.expression
            if isinstance(expression, exp.Literal) and not expression.is_string:
                try:
                    literal_limit = max(0, int(expression.this))
                except (TypeError, ValueError):
                    literal_limit = 0

        def erase(node):
            if isinstance(node, exp.Literal):
                return exp.Placeholder()
            if isinstance(node, (exp.Boolean, exp.Null)):
                return exp.Placeholder()
            return node

        canonical = root.transform(erase, copy=True).sql(
            dialect="duckdb", pretty=False,
        )
    except Exception:
        pass
    return QueryShape(
        shape_hash=hashlib.sha256(
            canonical.encode("utf-8"),
        ).hexdigest()[:16],
        has_join=has_join,
        has_sort=has_sort,
        has_group_by=has_group,
        has_aggregate=has_aggregate,
        literal_limit=literal_limit,
        projected_expressions=projected_expressions,
    )


@dataclass(frozen=True)
class RoutingFeatures:
    """Estimator/capability facts consumed by the AUTO policy.

    All values are aggregate and contain no SQL, paths, credentials, or user
    data, so :meth:`RoutingDecision.as_dict` is safe to retain as profile
    metadata.
    """

    reflection_bytes: int
    effective_scan_bytes: int
    decoded_bytes: int
    total_files: int
    selected_row_groups: int = 0
    candidate_rows: int = 0
    source_bytes_complete: bool = True
    row_group_bytes_complete: bool = False
    decoded_bytes_complete: bool = False
    candidate_rows_complete: bool = False
    data_is_fresh: bool = False
    freshness_age_seconds: int = -1
    has_active_tombstone: bool = False
    streaming_result: bool = False
    island_plan_evaluated: bool = False
    island_advice: str = ""
    island_cpu_workers: int = 0
    island_io_workers: int = 0
    island_estimated_spill_bytes: int = 0
    query_shape_hash: str = ""
    cache_state: str = "unknown"
    has_join: bool = False
    has_sort: bool = False
    has_group_by: bool = False
    has_aggregate: bool = False
    literal_limit: int = 0
    projected_expressions: int = 0
    estimated_result_rows: int = 0
    estimated_result_bytes: int = 0
    result_estimate_complete: bool = False

    def __post_init__(self) -> None:
        for name in (
            "reflection_bytes", "effective_scan_bytes", "decoded_bytes",
            "total_files", "selected_row_groups", "candidate_rows",
            "island_cpu_workers", "island_io_workers",
            "island_estimated_spill_bytes",
            "literal_limit", "projected_expressions",
            "estimated_result_rows", "estimated_result_bytes",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative")

    @staticmethod
    def _magnitude_bucket(value: int) -> int:
        """Power-of-two bucket; stable under small append/selectivity drift."""
        return _non_negative(value).bit_length()

    def feature_signature(self) -> str:
        """Comparable-history signature without retaining query text.

        Query shape, scan/decode magnitude, fan-out, and cache warmth all alter
        engine break-even points.  Observations are blended only inside this
        bucket, preventing the same SQL shape at radically different predicate
        selectivity from teaching AUTO the wrong crossover.
        """
        cache_state = str(self.cache_state or "unknown").strip().casefold()
        if cache_state not in {"unknown", "cold", "partial", "warm"}:
            cache_state = "unknown"
        framed = "|".join((
            str(self.query_shape_hash or ""),
            str(self._magnitude_bucket(self.effective_scan_bytes)),
            str(self._magnitude_bucket(self.decoded_bytes)),
            str(self._magnitude_bucket(self.total_files)),
            str(self._magnitude_bucket(self.selected_row_groups)),
            str(self._magnitude_bucket(self.candidate_rows)),
            cache_state,
            "stream" if self.streaming_result else "materialized",
            "tombstone" if self.has_active_tombstone else "clean",
            "fresh" if self.data_is_fresh else "stable",
            "island_plan" if self.island_plan_evaluated else "no_island_plan",
            str(self.island_advice or "none"),
            str(self._magnitude_bucket(self.island_cpu_workers)),
            str(self._magnitude_bucket(self.island_io_workers)),
            "source_exact" if self.source_bytes_complete else "source_unknown",
            "rg_exact" if self.row_group_bytes_complete else "rg_unknown",
            "decoded_exact" if self.decoded_bytes_complete else "decoded_unknown",
            "rows_exact" if self.candidate_rows_complete else "rows_unknown",
            "result_exact" if self.result_estimate_complete else "result_unknown",
            "join" if self.has_join else "no_join",
            "sort" if self.has_sort else "no_sort",
            "group" if self.has_group_by else "no_group",
            "aggregate" if self.has_aggregate else "no_aggregate",
            str(self._magnitude_bucket(self.estimated_result_bytes)),
        ))
        return hashlib.sha256(framed.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class RoutingAvailability:
    """Runtime availability after external fleet and capability probes."""

    duckdb_available: bool = True
    island_enabled: bool = False
    island_supported: bool = False
    spark_available: bool = False
    spark_semantics_supported: bool = True
    fitting_spark_clusters: int = 0
    spark_min_scan_bytes: int = 0
    # Provider-issued linked bearer resources cannot be converted to Spark's
    # credential-less cluster URI without crossing authorization domains.
    spark_linked_bearer_safe: bool = True
    # IslandDB's storage/range layer must not reinterpret a provider-issued
    # linked-share bearer URL through the consumer's storage authority.
    island_linked_bearer_safe: bool = True


@dataclass(frozen=True)
class EngineHistory:
    """Optional bounded performance observation for one engine/query shape.

    ``ewma_duration_us`` is the observed end-to-end execution EWMA for
    ``ewma_work_bytes`` (compressed scan + decoded work).  At least three
    successful samples are required.  Historical influence grows gradually
    and is capped at 75%, so sparse or stale observations cannot make AUTO
    abandon the conservative baseline.  Failures add cost but never make an
    otherwise unsafe engine eligible.
    """

    sample_count: int
    success_count: int
    ewma_duration_us: int
    ewma_work_bytes: int
    feature_signature: str

    def usable(self, expected_signature: str) -> bool:
        return (
            self.sample_count >= 3
            and 3 <= self.success_count <= self.sample_count
            and self.ewma_duration_us > 0
            and self.ewma_work_bytes > 0
            and bool(self.feature_signature)
            and self.feature_signature == expected_signature
        )


@dataclass(frozen=True)
class EngineCandidate:
    engine: Engine
    eligible: bool
    estimated_cost_us: Optional[int]
    components_us: Mapping[str, int] = field(default_factory=dict)
    rejection_reasons: Tuple[str, ...] = ()
    history_weight_permille: int = 0

    def as_dict(self) -> dict:
        return {
            "engine": self.engine.value,
            "eligible": self.eligible,
            "estimated_cost_us": self.estimated_cost_us,
            "components_us": dict(self.components_us),
            "rejection_reasons": list(self.rejection_reasons),
            "history_weight_permille": self.history_weight_permille,
        }


@dataclass(frozen=True)
class RoutingDecision:
    engine: Engine
    reason: str
    features: RoutingFeatures
    availability: RoutingAvailability
    candidates: Tuple[EngineCandidate, ...]
    policy_version: str = POLICY_VERSION
    manual_policy: Optional[Mapping[str, object]] = None

    def as_dict(self) -> dict:
        return {
            "policy_version": self.policy_version,
            "selected_engine": self.engine.value,
            "reason": self.reason,
            "feature_signature": self.features.feature_signature(),
            "features": asdict(self.features),
            "availability": asdict(self.availability),
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "manual_policy": dict(self.manual_policy) if self.manual_policy else None,
        }

    def as_plan_stat(self) -> dict:
        return {"AUTO_ROUTING": self.as_dict()}


class AdaptiveEngineRouter:
    """Apply hard correctness gates, then choose minimum predicted latency."""

    # Aggregate throughput baselines.  They intentionally describe relative
    # break-even points rather than machine-specific peak bandwidth.
    _MODEL = {
        Engine.DUCKDB: {
            "startup_us": 8_000,
            "scan_bps": int(1.8 * GIB),
            "decode_bps": int(2.5 * GIB),
            "file_us": 1_200,
            "row_group_us": 40,
            "operator_bps": int(1.2 * GIB),
            "result_bps": int(2.0 * GIB),
        },
        Engine.ISLANDDB: {
            "startup_us": 30_000,
            "scan_bps": int(3.2 * GIB),
            "decode_bps": int(6.0 * GIB),
            "file_us": 90,
            "row_group_us": 25,
            "operator_bps": int(4.5 * GIB),
            "result_bps": int(5.0 * GIB),
        },
        Engine.SPARK_SQL: {
            "startup_us": 1_500_000,
            "scan_bps": int(8.0 * GIB),
            "decode_bps": int(12.0 * GIB),
            "file_us": 300,
            "row_group_us": 15,
            "operator_bps": int(10.0 * GIB),
            "result_bps": int(1.2 * GIB),
        },
    }

    # Prefer the mature local engines for an exact cost tie.
    _TIE_PRIORITY = {
        Engine.DUCKDB: 0,
        Engine.ISLANDDB: 1,
        Engine.SPARK_SQL: 2,
    }

    def __init__(self, *, island_min_bytes: int):
        self.island_min_bytes = _non_negative(island_min_bytes)

    @staticmethod
    def _normal_work_bytes(features: RoutingFeatures) -> Tuple[int, int]:
        scan = features.effective_scan_bytes
        # Unknown decoded size is not zero work.  DuckDB and Spark remain
        # usable, but are costed at least one decoded byte per compressed byte.
        decoded = (
            features.decoded_bytes
            if features.decoded_bytes_complete
            else max(features.decoded_bytes, scan)
        )
        return scan, decoded

    def _components(self, engine: Engine, features: RoutingFeatures) -> Dict[str, int]:
        model = self._MODEL[engine]
        scan, decoded = self._normal_work_bytes(features)
        components: Dict[str, int] = {
            "startup": model["startup_us"],
            "scan": _transfer_us(scan, model["scan_bps"]),
            "decode": _transfer_us(decoded, model["decode_bps"]),
            "file_fanout": features.total_files * model["file_us"],
            "row_group_fanout": features.selected_row_groups * model["row_group_us"],
        }

        operator_passes = (
            (2 if features.has_join else 0)
            + (1 if features.has_sort else 0)
            + (1 if features.has_group_by else 0)
            + (1 if features.has_aggregate and not features.has_group_by else 0)
        )
        if operator_passes:
            components["operators"] = _transfer_us(
                decoded * operator_passes, model["operator_bps"],
            )
        if features.result_estimate_complete:
            components["result_materialization"] = _transfer_us(
                features.estimated_result_bytes, model["result_bps"],
            )

        if engine == Engine.ISLANDDB and 0 < features.island_cpu_workers < 4:
            # The native baselines were calibrated around four useful cores.
            # Never extrapolate a speedup above that calibration point, but do
            # conservatively scale CPU-bound work down inside a constrained
            # one/two/three-core container.  The exact worker count is also in
            # the history signature, so observations cannot cross-teach hosts.
            cpu_bound = components["decode"] + components.get("operators", 0)
            components["constrained_cpu_capacity"] = (
                cpu_bound * (4 - features.island_cpu_workers)
                + features.island_cpu_workers - 1
            ) // features.island_cpu_workers

        if (
            engine in {Engine.DUCKDB, Engine.SPARK_SQL}
            and features.row_group_bytes_complete
            and features.reflection_bytes > scan > 0
        ):
            # IslandDB receives exact sealed fragments. Other engines still
            # coordinate the full selected file list and discover pushdown in
            # their own Parquet scan. Model that footer/planning amplification
            # logarithmically; it is overhead, not bytes claimed to be read.
            magnitude_gap = max(
                0,
                features.reflection_bytes.bit_length() - scan.bit_length(),
            )
            components["row_group_pruning_coordination"] = min(
                250_000, magnitude_gap * 8_000,
            )

        if engine == Engine.ISLANDDB and scan <= self.island_min_bytes:
            # The benchmarked native crossover is around the Lite boundary;
            # Arrow/Polars setup is not worthwhile for a tiny materialization.
            components["small_query_launch"] = 35_000

        if features.data_is_fresh:
            base = sum(components.values())
            if engine == Engine.ISLANDDB:
                components["fresh_metadata_churn"] = max(300_000, base // 2)

        if features.has_active_tombstone:
            if engine == Engine.DUCKDB:
                components["tombstone_anti_join"] = features.total_files * 700
            elif engine == Engine.ISLANDDB:
                components["tombstone_anti_join"] = features.total_files * 80

        if engine == Engine.ISLANDDB and features.island_advice == "island_spill":
            components["spill_setup"] = 250_000
            components["spill_io"] = _transfer_us(
                features.island_estimated_spill_bytes,
                800 * MIB,
            )
        return components

    @staticmethod
    def _historical_cost(
        baseline_us: int,
        work_bytes: int,
        history: Optional[EngineHistory],
        feature_signature: str,
    ) -> Tuple[int, int, int]:
        if history is None or not history.usable(feature_signature):
            return baseline_us, 0, 0

        # Scale only within a bounded 0.25x..4x window.  A query hash can see
        # normal append growth, but an extreme size change must fall back toward
        # the estimator model rather than extrapolating an old observation.
        scale_num = max(history.ewma_work_bytes // 4, min(
            work_bytes, history.ewma_work_bytes * 4,
        ))
        observed = max(1, (
            history.ewma_duration_us * scale_num
            + history.ewma_work_bytes - 1
        ) // history.ewma_work_bytes)
        success_permille = history.success_count * 1000 // history.sample_count
        sample_weight = min(750, (history.sample_count - 2) * 50)
        weight = sample_weight * success_permille // 1000
        blended = (
            baseline_us * (1000 - weight) + observed * weight + 999
        ) // 1000
        # Failed observations conservatively penalize the engine.  This can
        # change a performance choice, but never a correctness gate.
        failure_permille = 1000 - success_permille
        failure_penalty = baseline_us * failure_permille // 500
        return blended + failure_penalty, weight, observed

    def decide(
        self,
        features: RoutingFeatures,
        availability: RoutingAvailability,
        *,
        history: Optional[Mapping[Engine, EngineHistory]] = None,
        manual_engine: Optional[Engine] = None,
        manual_policy: Optional[Mapping[str, object]] = None,
    ) -> RoutingDecision:
        history = history or {}
        rejections: Dict[Engine, list[str]] = {
            engine: [] for engine in self._MODEL
        }

        if not availability.duckdb_available:
            rejections[Engine.DUCKDB].append("DuckDB unavailable")

        if (
            not availability.island_enabled
            and manual_engine is not Engine.ISLANDDB
        ):
            rejections[Engine.ISLANDDB].append("IslandDB AUTO disabled")
        if not availability.island_supported:
            rejections[Engine.ISLANDDB].append("query outside IslandDB capability subset")
        if not availability.island_linked_bearer_safe:
            rejections[Engine.ISLANDDB].append(
                "IslandDB cannot consume provider-linked bearer resources safely"
            )
        if not features.source_bytes_complete:
            rejections[Engine.ISLANDDB].append("source byte estimate incomplete")
        if (
            features.island_plan_evaluated
            and features.island_advice not in {
                "island_in_memory", "island_spill",
            }
        ):
            rejections[Engine.ISLANDDB].append(
                "IslandDB resource plan is not executable"
            )
        elif (
            availability.island_enabled
            and availability.island_supported
            and not features.island_plan_evaluated
        ):
            rejections[Engine.ISLANDDB].append(
                "IslandDB resource plan is unavailable"
            )

        if not availability.spark_available:
            rejections[Engine.SPARK_SQL].append("no safe fitting Spark cluster")
        if features.streaming_result:
            # The public Arrow result contract is implemented only by DuckDB
            # and IslandDB.  This is a capability boundary, not a cost hint:
            # allowing Spark into the race lets AUTO select it and fail only at
            # Executor.execute_stream(), after estimation/routing work has
            # already completed.
            rejections[Engine.SPARK_SQL].append(
                "Spark does not support streaming Arrow results"
            )
        if not availability.spark_semantics_supported:
            rejections[Engine.SPARK_SQL].append(
                "query outside the cross-engine semantic equivalence subset"
            )
        if not availability.spark_linked_bearer_safe:
            rejections[Engine.SPARK_SQL].append(
                "Spark cannot consume provider-linked bearer resources safely"
            )
        if not features.source_bytes_complete:
            rejections[Engine.SPARK_SQL].append("source byte estimate incomplete")
        if features.has_active_tombstone:
            rejections[Engine.SPARK_SQL].append(
                "Spark cannot prove composite tombstone source identity"
            )
        if (
            features.island_advice != "route_spark"
            and features.effective_scan_bytes < availability.spark_min_scan_bytes
        ):
            rejections[Engine.SPARK_SQL].append("candidate scan below Spark fleet floor")

        forced_reason = ""
        # An incomplete source estimate cannot safely participate in a cost
        # race.  Preserve a small/fresh Lite escape hatch, otherwise select the
        # persistent external-memory DuckDB engine.
        if not features.source_bytes_complete:
            preferred = Engine.DUCKDB
            forced_reason = "source byte estimate incomplete; conservative DuckDB route"
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by incomplete-estimate fallback")

        # The native resource planner has stronger decoded/operator knowledge
        # than compressed size.  Treat its route-away advice as a hard safety
        # constraint so a compressed object cannot be demoted to Lite.
        elif features.island_advice in {"route_duckdb", "stream_result"}:
            preferred = Engine.DUCKDB
            forced_reason = (
                f"IslandDB resource planner requires {features.island_advice}"
            )
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by native resource safety advice")
        elif features.island_advice == "route_spark":
            preferred = (
                Engine.SPARK_SQL
                if not rejections[Engine.SPARK_SQL]
                else Engine.DUCKDB
            )
            forced_reason = (
                "decoded/operator state requires distributed execution"
                if preferred == Engine.SPARK_SQL
                else "distributed route unavailable; use DuckDB"
            )
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by native resource safety advice")

        # Active deletion vectors cannot go to Spark. If the specialised
        # native path is also unavailable, keep the query on persistent Pro:
        # its cached DV relation avoids repeatedly rebuilding a potentially
        # large anti-join in Lite.
        elif manual_engine is not None and not rejections.get(manual_engine):
            preferred = manual_engine
            forced_reason = (
                f"Redis estimated-scan policy selected {manual_engine.value}"
            )
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by Redis estimated-scan policy")

        elif (
            features.has_active_tombstone
            and rejections[Engine.ISLANDDB]
        ):
            preferred = Engine.DUCKDB
            forced_reason = "active tombstone requires DuckDB"
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by tombstone execution policy")

        # The configured Lite ceiling is inclusive at its exact boundary.
        # Below it, normal cost/history adaptation remains active.
        elif (
            features.reflection_bytes == self.island_min_bytes
            and availability.duckdb_available
        ):
            preferred = Engine.DUCKDB
            forced_reason = "scan fits inclusive DuckDB ceiling"
            for engine in self._MODEL:
                if engine != preferred:
                    rejections[engine].append("excluded by DuckDB ceiling")

        candidates = []
        work_bytes = sum(self._normal_work_bytes(features))
        feature_signature = features.feature_signature()
        for engine in self._MODEL:
            reasons = tuple(dict.fromkeys(rejections[engine]))
            if reasons:
                candidates.append(EngineCandidate(
                    engine=engine,
                    eligible=False,
                    estimated_cost_us=None,
                    rejection_reasons=reasons,
                ))
                continue
            components = self._components(engine, features)
            baseline = sum(components.values())
            cost, weight, observed = self._historical_cost(
                baseline, work_bytes, history.get(engine), feature_signature,
            )
            if observed:
                components["historical_prediction"] = observed
                components["historical_adjustment"] = cost - baseline
            candidates.append(EngineCandidate(
                engine=engine,
                eligible=True,
                estimated_cost_us=cost,
                components_us=components,
                history_weight_permille=weight,
            ))

        eligible = [candidate for candidate in candidates if candidate.eligible]
        if not eligible:
            # Executor construction guarantees DuckDB, but keep this pure
            # policy total and deterministic for malformed integrations.
            raise RuntimeError("AUTO routing has no eligible execution engine")
        selected = min(
            eligible,
            key=lambda candidate: (
                candidate.estimated_cost_us or 0,
                self._TIE_PRIORITY[candidate.engine],
            ),
        )
        reason = forced_reason or (
            f"lowest predicted latency {selected.estimated_cost_us}us among "
            f"{len(eligible)} safe engine(s)"
        )
        return RoutingDecision(
            engine=selected.engine,
            reason=reason,
            features=features,
            availability=availability,
            candidates=tuple(candidates),
            manual_policy=manual_policy,
        )
