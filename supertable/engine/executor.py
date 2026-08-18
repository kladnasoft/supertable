# supertable/engine/executor.py

from __future__ import annotations

import os
import time
import hashlib
import math
import weakref
from contextlib import nullcontext
from typing import Callable, Mapping, Optional, Tuple

import pandas as pd

from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb_engine import DuckDB
from supertable.engine.engine_config import (
    AutoRoutingRule,
    EngineRuntimeConfig,
    match_auto_routing_policy,
    resolve_engine_bundle,
)
from supertable.engine.adaptive_router import (
    AdaptiveEngineRouter,
    EngineHistory,
    RoutingAvailability,
    RoutingFeatures,
    analyze_query_shape,
)
from supertable.engine.islanddb import (
    IslandCapability,
    IslandDB,
    IslandUnsupportedError,
)
from supertable.engine.island_resources import (
    ArrowBatchStream,
    IslandResourceError,
    ResultMemoryLimitExceeded,
)
from supertable.data_classes import Reflection
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.storage.storage_interface import StorageInterface


_duckdb_singleton: Optional[DuckDB] = None
_duckdb_singletons: "weakref.WeakValueDictionary[Tuple[str, tuple], DuckDB]" = (
    weakref.WeakValueDictionary()
)
_duckdb_lock = __import__("threading").Lock()


def _configured_query_timeout_sec() -> float:
    """Return a finite positive deadline; malformed config fails bounded."""
    try:
        value = float(
            getattr(settings, "SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC", 60.0)
        )
    except (TypeError, ValueError, OverflowError):
        return 60.0
    return value if math.isfinite(value) and value > 0 else 60.0


def _storage_supports_bounded_ranges(storage: Optional[object]) -> bool:
    """Return whether a backend advertises a real bounded-range method.

    The base StorageInterface method deliberately raises NotImplementedError.
    Treating its mere presence as range support can make AUTO select IslandDB
    and fail only after Arrow starts opening fragments. Custom duck-typed
    adapters remain supported when they provide a callable implementation.
    This is an availability check only; conditional identity mismatches during
    an actual read remain fail-closed and must never trigger AUTO fallback.
    """
    if storage is None:
        return False
    method = getattr(storage, "read_range", None)
    if not callable(method):
        return False
    return not (
        isinstance(storage, StorageInterface)
        and type(storage).read_range is StorageInterface.read_range
    )


def _fingerprint(values) -> str:
    """Hash a length-framed credential tuple without retaining its contents."""
    digest = hashlib.sha256()
    for value in values:
        if value is None:
            raw = b""
        elif isinstance(value, bytes):
            raw = value
        else:
            raw = str(value).encode("utf-8")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def _storage_identity(storage: Optional[object]) -> tuple:
    """Return a conservative identity for a storage backend.

    Connections carry credentials, endpoints, and cached views.  Sharing one
    merely because two requests both selected "Pro" can expose another org or
    backend's files.  Known immutable routing attributes allow safe reuse;
    opaque/custom storage objects are isolated by object identity.
    """
    if storage is None:
        return ("none",)
    module = storage.__class__.__module__
    qualname = storage.__class__.__qualname__
    parts = [module, qualname]
    for name in (
        "bucket_name", "container_name", "base_prefix", "endpoint_url",
        "region", "url_style", "secure", "project_id", "account_name",
    ):
        try:
            value = getattr(storage, name)
        except Exception:
            continue
        if value is None or isinstance(value, (str, int, float, bool)):
            parts.append((name, value))
    # Built-in local storage has no auth state, but its explicit root is a hard
    # namespace boundary.  Never key it by the mutable process CWD: factory
    # instances are rooted at SUPERTABLE_HOME without changing CWD.
    if module == "supertable.storage.local_storage" and qualname == "LocalStorage":
        root = getattr(storage, "root", os.getcwd())
        parts.append(("local_root", os.path.realpath(os.fspath(root))))
        return tuple(parts)

    # S3/MinIO connection state is completely represented by the route above
    # plus the full credential tuple below.  Fingerprinting the tuple makes
    # independently constructed equivalent storage objects reuse an engine,
    # while any access/secret/session-token change creates a hard boundary.
    if module in {
        "supertable.storage.s3_storage",
        "supertable.storage.minio_storage",
    }:
        credential_names = (
            "_aws_access_key_id", "_aws_secret_access_key",
            "_aws_session_token", "_access_key", "_secret_key",
        )
        values = []
        for name in credential_names:
            try:
                values.append(getattr(storage, name, None))
            except Exception:
                values.append(None)
        if any(value not in (None, "", b"") for value in values):
            parts.append(("auth_fingerprint", _fingerprint(values)))
        else:
            # Dependency-injected SDK clients may carry credentials that the
            # wrapper never received as constructor values.  Treat them as
            # opaque auth contexts rather than merging on route alone.
            parts.append(("client_object_id", id(getattr(storage, "client", storage))))
        return tuple(parts)

    # GCS/Azure/custom SDK clients can carry opaque refreshable credentials not
    # exposed by the storage wrapper.  Isolate those by client identity; this
    # sacrifices some reuse but never crosses authorization contexts.
    for name in ("client", "svc"):
        try:
            client = getattr(storage, name)
        except Exception:
            continue
        if client is not None:
            parts.append((f"{name}_object_id", id(client)))
    if len(parts) == 2:
        parts.append(("object_id", id(storage)))
    return tuple(parts)


def _get_duckdb(
        storage: Optional[object] = None, organization: str = "",
) -> DuckDB:
    global _duckdb_singleton
    key = (str(organization or ""), _storage_identity(storage))
    with _duckdb_lock:
        if _duckdb_singleton is None and _duckdb_singletons:
            _duckdb_singletons.clear()
        engine = _duckdb_singletons.get(key)
        if engine is None:
            engine = DuckDB(storage=storage)
            _duckdb_singletons[key] = engine
        _duckdb_singleton = engine
        return engine


class Executor:
    """
    Chooses execution engine and runs the query against the provided file list.
    """

    def __init__(
        self,
        storage: Optional[object] = None,
        organization: str = "",
        *,
        auto_history_provider: Optional[
            Callable[[RoutingFeatures], Mapping[Engine, EngineHistory]]
        ] = None,
    ):
        self.storage = storage
        self.organization = organization
        self.duckdb_exec = _get_duckdb(storage=storage, organization=organization)
        self.spark_exec = None
        self.island_exec = IslandDB(
            storage=storage, organization=organization,
        )
        self._file_cache = None
        self._catalog = None  # lazily created RedisCatalog for live config reads
        # Optional, read-only profile feedback.  The callable receives the
        # fully bucketed aggregate feature record and returns comparable
        # histories keyed by Engine. It is best-effort: Redis/provider failure
        # can never make AUTO or an explicit query unavailable.
        self._auto_history_provider = auto_history_provider

    @staticmethod
    def _publish_island_profile(
        plan_stats: PlanStats,
        query_manager: QueryPlanManager,
        log_prefix: str,
    ) -> None:
        """Publish only the current query's tokenized Island telemetry."""
        try:
            profile = getattr(query_manager, "_island_profile", None)
            expected_token = getattr(
                query_manager, "_island_profile_token", None,
            )
            if (
                profile is None
                or not expected_token
                or getattr(profile, "telemetry_query_id", None)
                != expected_token
            ):
                return
            profile_doc = profile.as_dict()
            if profile.resources:
                plan_stats.add_stat({"ISLAND_RESOURCES": profile.resources})
            if profile.spill:
                plan_stats.add_stat({"ISLAND_SPILL": profile.spill})
            if profile.cache:
                plan_stats.add_stat({"ISLAND_CACHE": profile.cache})
            plan_stats.add_stat({
                "ISLAND_SELECTED_ROW_GROUPS": profile.selected_row_groups,
            })
            # Keep one lossless, provenance-complete document. Large/duplicated
            # plan text and the three legacy top-level sections stay separate.
            for excluded in (
                "optimized_plan", "resources", "spill", "cache",
            ):
                profile_doc.pop(excluded, None)
            plan_stats.add_stat({"ISLAND_TELEMETRY": profile_doc})
        except Exception as telemetry_error:
            logger.debug(
                "%s[islanddb] plan telemetry unavailable: %s",
                log_prefix,
                telemetry_error,
            )

    @staticmethod
    def _publish_engine_capability(
        plan_stats: Optional[PlanStats],
        capability: object,
        *,
        analysis_error: Optional[BaseException] = None,
    ) -> None:
        """Publish IslandDB's whole-query semantic certification.

        AUTO is allowed to select IslandDB only when ``can_execute`` certifies
        the complete SQL statement.  Recording that decision lets system
        callers such as data-quality checks distinguish native execution from
        a correctness-preserving DuckDB route.  This is observability only;
        serialization failure must never affect execution.
        """
        if plan_stats is None:
            return
        try:
            supported = bool(
                analysis_error is None
                and getattr(capability, "supported", False) is True
            )
            reasons = [
                str(reason)
                for reason in (getattr(capability, "reasons", ()) or ())
                if str(reason)
            ]
            if analysis_error is not None:
                # Arbitrary exception text can contain a presigned URL.  The
                # type explains a fail-closed route without leaking it. Normal
                # unsupported SQL retains its exact, data-free reasons above.
                reasons = [
                    "IslandDB capability analysis failed: "
                    f"{type(analysis_error).__name__}"
                ]
            plan_stats.add_stat({
                "ENGINE_CAPABILITY": {
                    "engine": Engine.ISLANDDB.value,
                    "supported": supported,
                    "scope": "complete_query_static_semantics",
                    "reasons": reasons,
                },
            })
        except Exception:
            return

    def _get_file_cache(self):
        """Return the org/storage-scoped shared Parquet cache, lazily.

        Cache construction is an optimisation boundary.  If the directory is
        unavailable, existing DuckDB reads continue on their original paths;
        explicit IslandDB will subsequently reject a still-remote reflection.
        Integrity failures during an actual fill are never swallowed.
        """
        if not settings.SUPERTABLE_ISLAND_CACHE_ENABLED:
            return None
        if self._file_cache is False:
            return None
        if self._file_cache is None:
            try:
                from supertable.engine.file_cache import FileCache
                self._file_cache = FileCache(
                    self.storage,
                    self.organization,
                    root=(settings.SUPERTABLE_ISLAND_CACHE_DIR or None),
                    max_bytes=settings.SUPERTABLE_ISLAND_CACHE_MAX_BYTES,
                    ttl=settings.SUPERTABLE_ISLAND_CACHE_TTL_SEC,
                    workers=settings.SUPERTABLE_ISLAND_CACHE_WORKERS,
                )
            except Exception as exc:
                logger.warning("[file-cache] disabled for this executor: %s", exc)
                self._file_cache = False
                return None
        return self._file_cache

    def _get_catalog(self):
        """Lazily create the required catalog for live engine-config reads.

        Engine and routing policy are security/correctness configuration.  A
        catalog-construction failure is not evidence that no document exists,
        so callers must see it instead of silently executing with defaults.
        """
        if self._catalog is None:
            from supertable.redis_catalog import RedisCatalog
            self._catalog = RedisCatalog()
        return self._catalog or None

    def _active_spark_clusters(self) -> list:
        """Active Spark Thrift clusters registered for this org (best-effort).

        Returns ``[]`` when no catalog is reachable or none are active, which
        makes AUTO stay on DuckDB instead of routing to a fleet that cannot run
        the job.
        """
        try:
            catalog = self._get_catalog()
        except Exception:
            # Fleet discovery is merely an AUTO availability hint. Required
            # engine-policy acquisition has already happened at execute(); a
            # transient discovery failure keeps AUTO off Spark.
            return []
        if catalog is None:
            return []
        try:
            clusters = catalog.list_spark_clusters(self.organization) or []
        except Exception:
            return []
        return [
            c for c in clusters
            if isinstance(c, dict) and c.get("status") == "active"
        ]

    def _spark_min_bytes(self, cfg: EngineRuntimeConfig, active_clusters: Optional[list] = None) -> int:
        """Byte size at which AUTO hands a query to the Spark fleet.

        Fleet-driven: the **smallest** ``min_bytes`` across active clusters —
        the lowest job size any active cluster will accept.  A job at or above
        this triggers Spark; :meth:`RedisCatalog.select_spark_cluster` then
        picks (at random) one of the clusters whose ``[min_bytes, max_bytes]``
        window contains the job.

        Falls back to the ``engine_spark_min_bytes`` policy value only when no
        active cluster is known (catalog down / empty fleet).  In that case
        :meth:`_auto_pick` gates on an active cluster existing, so AUTO won't
        route to Spark regardless of the returned bound.
        """
        if active_clusters is None:
            active_clusters = self._active_spark_clusters()
        mins = []
        for c in active_clusters:
            try:
                mins.append(int(c.get("min_bytes", 0)))
            except (TypeError, ValueError):
                continue
        if mins:
            return min(mins)
        return cfg.engine_spark_min_bytes

    def _auto_pick(
        self,
        reflection: Reflection,
        cfg: EngineRuntimeConfig,
        parser=None,
        *,
        streaming_result: bool = False,
        plan_stats: Optional[PlanStats] = None,
        routing_policy: Tuple[AutoRoutingRule, ...] = (),
    ) -> Engine:
        """Choose the lowest predicted-cost engine after hard safety gates.

        The estimator's completeness flags, Spark fleet window, tombstone
        contract, IslandDB capability check, and decoded-memory advice are
        eligibility boundaries.  Only safe candidates enter the deterministic
        cost race.  ``AUTO_ROUTING`` records the complete, data-free decision
        payload when a :class:`PlanStats` collector is supplied.
        """
        bytes_total = max(0, int(reflection.reflection_bytes or 0))
        freshness_threshold_s = cfg.engine_freshness_sec
        active_clusters = self._active_spark_clusters()
        has_active_tombstone = any(
            getattr(tombstone, "tombstone_path", None)
            for tombstone in (
                getattr(reflection, "tombstone_views", None) or {}
            ).values()
        )
        size_is_complete = bool(
            getattr(reflection, "source_bytes_complete", True)
        )
        fitting_clusters = []
        for cluster in active_clusters:
            try:
                min_bytes = int(cluster.get("min_bytes", 0))
                max_bytes = int(cluster.get("max_bytes", 0))
            except (TypeError, ValueError):
                continue
            if bytes_total < min_bytes:
                continue
            if max_bytes > 0 and bytes_total > max_bytes:
                continue
            fitting_clusters.append(cluster)
        spark_available = (
            bool(fitting_clusters)
            and not has_active_tombstone
            and size_is_complete
        )
        spark_min = self._spark_min_bytes(cfg, fitting_clusters)

        if reflection.freshness_ms > 0:
            age_s = (time.time() * 1000 - reflection.freshness_ms) / 1000.0
            data_is_fresh = age_s < freshness_threshold_s
        else:
            # Unknown freshness — assume stable so Pro gets a chance to cache.
            age_s = -1
            data_is_fresh = False

        native = None
        island_plan = None
        routing_storage = getattr(self, "storage", None)
        island_range_available = bool(
            not settings.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED
            or routing_storage is None
            or _storage_supports_bounded_ranges(routing_storage)
        )
        # Production AUTO only routes to Spark for the same conservative SQL
        # semantics that passed IslandDB's DuckDB differential capability gate.
        # This is a correctness fence around cross-dialect transpilation; an
        # explicit Spark request remains an operator-controlled override.
        spark_semantics_supported = parser is None
        if (
            size_is_complete
            and parser is not None
            and getattr(self, "island_exec", None) is not None
        ):
            try:
                native = self.island_exec.can_execute(
                    reflection,
                    parser,
                    streaming_result=streaming_result,
                )
                self._publish_engine_capability(plan_stats, native)
                native_supported = (
                    getattr(native, "supported", False) is True
                )
                spark_semantics_supported = native_supported
                policy_enables_island = any(
                    rule.engine is Engine.ISLANDDB for rule in routing_policy
                )
                if native_supported and island_range_available and (
                    settings.SUPERTABLE_ISLAND_AUTO_ENABLED
                    or policy_enables_island
                ):
                    island_plan = self.island_exec.resource_plan(
                        reflection,
                        parser,
                        streaming_result=streaming_result,
                    )
            except Exception as exc:
                if native is None:
                    self._publish_engine_capability(
                        plan_stats, None, analysis_error=exc,
                    )
                logger.debug(
                    "[engine.auto] IslandDB resource probe skipped: %s", exc,
                )

        rg_size_complete = bool(
            getattr(reflection, "row_group_scan_bytes_complete", False)
        )
        effective_scan_bytes = (
            max(0, int(getattr(reflection, "row_group_scan_bytes", 0) or 0))
            if rg_size_complete else bytes_total
        )

        snapshots = tuple(getattr(reflection, "supers", None) or ())
        selected_row_groups = 0
        for snapshot in snapshots:
            try:
                selections = (
                    getattr(snapshot, "row_group_selections", None) or {}
                )
                selected_row_groups += sum(
                    len(selection.selected_ids)
                    for selection in selections.values()
                    if isinstance(selection.selected_ids, tuple)
                )
            except Exception:
                # Optional malformed hints fail open for execution and are
                # consequently absent from the performance estimate too.
                pass
        if selected_row_groups == 0:
            selected_row_groups = max(
                0, int(getattr(reflection, "total_reflections", 0) or 0),
            )
        candidate_rows_complete = bool(snapshots) and all(
            bool(getattr(snapshot, "candidate_rows_complete", False))
            for snapshot in snapshots
        )
        candidate_rows = (
            sum(
                max(0, int(getattr(snapshot, "candidate_rows", 0) or 0))
                for snapshot in snapshots
            )
            if candidate_rows_complete else 0
        )
        island_advice = getattr(
            getattr(island_plan, "advice", None), "value", "",
        )
        original_query = str(getattr(parser, "original_query", "") or "")
        query_shape = analyze_query_shape(original_query)
        shape_hash = query_shape.shape_hash
        result_rows_complete = bool(
            candidate_rows_complete and not query_shape.has_join
        )
        result_bytes_complete = False
        if query_shape.has_aggregate and not query_shape.has_group_by:
            estimated_result_rows = 1
            estimated_result_bytes = max(
                24, query_shape.projected_expressions * 24,
            )
            result_rows_complete = True
            # Aggregate cardinality is exact, but MIN/MAX over a variable-width
            # value can still exceed a width heuristic.  Keep the byte bound
            # explicitly incomplete unless the native planner supplies it.
        elif result_rows_complete:
            estimated_result_rows = candidate_rows
            if query_shape.literal_limit > 0:
                estimated_result_rows = min(
                    estimated_result_rows, query_shape.literal_limit,
                )
            selected_decoded_complete = bool(
                getattr(reflection, "selected_decoded_bytes_complete", False)
            )
            decoded_complete = bool(
                getattr(reflection, "decoded_bytes_complete", False)
            )
            if candidate_rows > 0 and (
                selected_decoded_complete or decoded_complete
            ):
                decoded_for_result = max(
                    0,
                    int(
                        getattr(
                            reflection,
                            "selected_decoded_bytes"
                            if selected_decoded_complete else "decoded_bytes",
                            0,
                        )
                        or 0
                    ),
                )
                result_row_width = max(
                    24,
                    query_shape.projected_expressions * 24,
                    (
                        decoded_for_result
                        + candidate_rows - 1
                    ) // candidate_rows,
                )
                result_bytes_complete = True
            else:
                result_row_width = max(
                    24, query_shape.projected_expressions * 24,
                )
            estimated_result_bytes = estimated_result_rows * result_row_width
        else:
            estimated_result_rows = 0
            estimated_result_bytes = 0
        storage_type = str(
            getattr(reflection, "storage_type", "") or ""
        ).casefold()
        cache_state = "warm" if storage_type == "local" else "unknown"
        features = RoutingFeatures(
            reflection_bytes=bytes_total,
            effective_scan_bytes=effective_scan_bytes,
            decoded_bytes=max(
                0, int(getattr(reflection, "decoded_bytes", 0) or 0),
            ),
            total_files=max(
                0, int(getattr(reflection, "total_reflections", 0) or 0),
            ),
            selected_row_groups=selected_row_groups,
            candidate_rows=candidate_rows,
            source_bytes_complete=size_is_complete,
            row_group_bytes_complete=rg_size_complete,
            decoded_bytes_complete=bool(
                getattr(reflection, "decoded_bytes_complete", False)
            ),
            candidate_rows_complete=candidate_rows_complete,
            data_is_fresh=data_is_fresh,
            freshness_age_seconds=(int(age_s) if age_s >= 0 else -1),
            has_active_tombstone=has_active_tombstone,
            streaming_result=streaming_result,
            island_advice=island_advice,
            island_cpu_workers=max(
                0, int(getattr(island_plan, "cpu_workers", 0) or 0),
            ),
            island_io_workers=max(
                0, int(getattr(island_plan, "io_workers", 0) or 0),
            ),
            island_estimated_spill_bytes=max(
                0,
                int(getattr(island_plan, "estimated_spill_bytes", 0) or 0),
            ),
            query_shape_hash=shape_hash,
            cache_state=cache_state,
            has_join=query_shape.has_join,
            has_sort=query_shape.has_sort,
            has_group_by=query_shape.has_group_by,
            has_aggregate=query_shape.has_aggregate,
            literal_limit=query_shape.literal_limit,
            projected_expressions=query_shape.projected_expressions,
            estimated_result_rows=estimated_result_rows,
            estimated_result_bytes=estimated_result_bytes,
            result_estimate_complete=(
                result_rows_complete and result_bytes_complete
            ),
        )
        availability = RoutingAvailability(
            island_enabled=bool(
                settings.SUPERTABLE_ISLAND_AUTO_ENABLED
                or any(rule.engine is Engine.ISLANDDB for rule in routing_policy)
            ),
            island_supported=bool(
                native is not None
                and getattr(native, "supported", False) is True
                and island_range_available
            ),
            spark_available=spark_available,
            spark_semantics_supported=spark_semantics_supported,
            fitting_spark_clusters=len(fitting_clusters),
            spark_min_scan_bytes=max(0, int(spark_min or 0)),
        )
        history = {}
        history_provider = getattr(self, "_auto_history_provider", None)
        if history_provider is not None:
            try:
                supplied = history_provider(features)
                if supplied:
                    history = dict(supplied)
            except Exception as exc:
                logger.debug(
                    "[engine.auto] historical profile lookup skipped: %s", exc,
                )
        matched_rule = match_auto_routing_policy(
            routing_policy, features.effective_scan_bytes,
        )
        policy_metadata = None
        if matched_rule is not None:
            policy_metadata = {
                **matched_rule.as_dict(),
                "estimated_scan_bytes": features.effective_scan_bytes,
                "estimate_complete": bool(
                    features.source_bytes_complete
                    and (
                        not features.row_group_bytes_complete
                        or features.effective_scan_bytes >= 0
                    )
                ),
            }
        decision = AdaptiveEngineRouter(
            island_min_bytes=cfg.engine_island_min_bytes,
        ).decide(
            features,
            availability,
            history=history,
            manual_engine=(matched_rule.engine if matched_rule else None),
            manual_policy=policy_metadata,
        )
        chosen = decision.engine
        if plan_stats is not None:
            plan_stats.add_stat(decision.as_plan_stat())

        if features.island_advice == "stream_result" and not streaming_result:
            if plan_stats is not None:
                plan_stats.add_stat({
                    "AUTO_ROUTING_BLOCKED": {
                        "reason_code": "streaming_result_required",
                        "estimated_result_bytes": features.estimated_result_bytes,
                    },
                })
            raise ResultMemoryLimitExceeded(
                "AUTO refused to materialize the estimated result in pandas; "
                "use Executor.execute_stream(), add a restrictive LIMIT, or "
                "explicitly force an engine to accept that allocation risk"
            )

        logger.info(
            f"[engine.auto] {chosen.value} — {decision.reason} "
            f"(files={reflection.total_reflections}, bytes={bytes_total})"
        )
        return chosen

    def execute(
        self,
        engine: Engine,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer: Timer,
        plan_stats: PlanStats,
        log_prefix: str,
        explain: bool = False,
        explain_options: str = "",
    ) -> Tuple[pd.DataFrame, str]:
        # Resolve engine config live (Redis → env → default) for this query so
        # UI changes take effect immediately without restart or cache.
        cfgs, routing_policy = resolve_engine_bundle(
            self.organization, self._get_catalog()
        )
        duckdb_cfg = cfgs["duckdb"]

        chosen = (
            engine if engine != Engine.AUTO
            else self._auto_pick(
                reflection,
                duckdb_cfg,
                parser=parser,
                plan_stats=plan_stats,
                routing_policy=routing_policy,
            )
        )
        auto_selected = chosen if engine == Engine.AUTO else None
        plan_stats.add_stat({
            "ENGINE_REQUEST": {
                "requested_engine": engine.value,
                "selected_engine": chosen.value,
                "forced": engine != Engine.AUTO,
            },
        })
        attempt_stage = "primary"

        island_prepared = None
        if chosen == Engine.ISLANDDB:
            # Capability analysis is pure and must run before a potentially
            # multi-gigabyte cache fill. Explicit unsupported queries fail
            # visibly without downloading anything.
            try:
                island_prepared = self.island_exec.prepare_execution(
                    reflection, parser, streaming_result=False,
                )
            except IslandUnsupportedError as exc:
                self._publish_engine_capability(
                    plan_stats, IslandCapability(False, (str(exc),)),
                )
                raise
            self._publish_engine_capability(
                plan_stats, island_prepared.capability,
            )

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        cache = self._get_file_cache()
        island_uses_ranges = bool(
            chosen == Engine.ISLANDDB
            and settings.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED
            and self.storage is not None
            and _storage_supports_bounded_ranges(self.storage)
        )
        if (
            chosen == Engine.ISLANDDB
            and engine != Engine.AUTO
            and cache is not None
            and not island_uses_ranges
            and not cache.can_populate_all(reflection)
        ):
            raise RuntimeError(
                "IslandDB cannot localize the complete reflection within the "
                "configured shared-cache byte cap"
            )
        populate_cache = (
            chosen == Engine.ISLANDDB
            and engine != Engine.AUTO
            and not island_uses_ranges
        )
        # Spark workers cannot see a coordinator-local path. DuckDB and
        # IslandDB both consume already-complete objects without forcing a cold
        # whole-object download. A selective/range Island query remains
        # hit-only here: misses stay remote and are served by the sealed range
        # cache, while complete hits are leased and exposed as ordinary local
        # files. This prevents the two immutable cache tiers from needlessly
        # downloading the same object twice.
        cache_is_useful = (
            cache is not None
            and chosen != Engine.SPARK_SQL
            # A LocalStorage reflection is already composed of ordinary local
            # files. Range mode gains no lease or locality from cloning it and
            # should not report a fictitious whole-object-cache hit.
            and (not island_uses_ranges or not cache.source_is_local)
            and (
                chosen == Engine.ISLANDDB
                or not cache.source_is_local
            )
        )
        cache_context = (
            cache.localized(
                reflection,
                populate=populate_cache,
                tolerate_corrupt_hits=(
                    engine == Engine.AUTO
                    or chosen != Engine.ISLANDDB
                    or island_uses_ranges
                ),
            )
            if cache_is_useful
            else nullcontext((reflection, None))
        )

        with cache_context as (execution_reflection, cache_metrics):
            if cache_metrics is not None:
                plan_stats.add_stat(cache_metrics.to_plan_stats())

            if (
                engine == Engine.AUTO
                and chosen == Engine.ISLANDDB
                and not island_uses_ranges
                and (
                    cache_metrics is None
                    or cache_metrics.coverage_ratio != 1.0
                )
            ):
                # Coverage was warm during routing but the leases are acquired
                # only here. If concurrent eviction won the race, retain AUTO's
                # no-download guarantee and use DuckDB on original paths.
                chosen = Engine.DUCKDB
                execution_reflection = reflection
                attempt_stage = "auto_fallback_cache"

            plan_stats.add_stat({
                "ENGINE_ATTEMPT": {
                    "engine": chosen.value,
                    "stage": attempt_stage,
                },
            })

            if chosen == Engine.DUCKDB:
                df = self.duckdb_exec.execute(
                    reflection=execution_reflection,
                    parser=parser,
                    query_manager=query_manager,
                    timer_capture=timer_capture,
                    log_prefix=log_prefix,
                    engine_config=duckdb_cfg,
                    explain=explain,
                    explain_options=explain_options,
                    timeout_sec=_configured_query_timeout_sec(),
                )
                used = "duckdb"

            elif chosen == Engine.ISLANDDB:
                if (
                    cache_metrics is not None
                    and cache_metrics.coverage_ratio != 1.0
                    and not island_uses_ranges
                ):
                    raise RuntimeError(
                        "IslandDB requires every selected data/tombstone object "
                        "to be localized; cache coverage was incomplete"
                    )
                try:
                    df = self.island_exec.execute(
                        reflection=execution_reflection,
                        parser=parser,
                        query_manager=query_manager,
                        timer_capture=timer_capture,
                        log_prefix=log_prefix,
                        engine_config=duckdb_cfg,
                        cache_metrics=cache_metrics,
                        _prepared=island_prepared,
                    )
                    used = "islanddb"
                    self._publish_island_profile(
                        plan_stats, query_manager, log_prefix,
                    )
                except (
                    IslandUnsupportedError,
                    IslandResourceError,
                    ResultMemoryLimitExceeded,
                ) as exc:
                    if engine != Engine.AUTO:
                        raise
                    # Static capability passed, but a physical-footer gate
                    # (for example mixed per-file types) rejected the native
                    # plan before user SQL execution. AUTO safely retains the
                    # already-localized files and runs the DuckDB oracle.
                    plan_stats.add_stat({
                        "ENGINE_ATTEMPT": {
                            "engine": Engine.DUCKDB.value,
                            "stage": "auto_fallback",
                            "reason_code": type(exc).__name__,
                        },
                    })
                    df = self.duckdb_exec.execute(
                        reflection=execution_reflection,
                        parser=parser,
                        query_manager=query_manager,
                        timer_capture=timer_capture,
                        log_prefix=log_prefix,
                        engine_config=duckdb_cfg,
                        explain=explain,
                        explain_options=explain_options,
                        timeout_sec=_configured_query_timeout_sec(),
                    )
                    used = "duckdb"

            elif chosen == Engine.SPARK_SQL:
                if self.spark_exec is None:
                    from supertable.engine.spark_thrift import SparkThriftExecutor
                    self.spark_exec = SparkThriftExecutor(
                        storage=self.storage, organization=self.organization,
                    )
                # force=True when user explicitly requested Spark (not via AUTO)
                df = self.spark_exec.execute(
                    reflection=execution_reflection,
                    parser=parser,
                    query_manager=query_manager,
                    timer_capture=timer_capture,
                    log_prefix=log_prefix,
                    force=(engine == Engine.SPARK_SQL),
                )
                used = "spark_sql"

            else:
                raise ValueError(f"Unsupported engine: {engine}")

        if auto_selected is not None:
            plan_stats.add_stat({
                "AUTO_ROUTING_OUTCOME": {
                    "selected_engine": auto_selected.value,
                    "actual_engine": used,
                    "fallback": used != auto_selected.value,
                },
            })
        plan_stats.add_stat({"ENGINE": used})
        return df, used

    def execute_stream(
        self,
        engine: Engine,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer: Timer,
        plan_stats: PlanStats,
        log_prefix: str,
        max_batch_rows: Optional[int] = None,
    ):
        """Return a one-shot Arrow batch stream without pandas.

        DuckDB and IslandDB preserve their query/cache resources until stream
        close. Spark remains explicit-only and materialized; the streaming API
        fails instead of silently changing its result-lifetime contract.
        """
        cfgs, routing_policy = resolve_engine_bundle(
            self.organization, self._get_catalog()
        )
        chosen = (
            engine
            if engine != Engine.AUTO
            else self._auto_pick(
                reflection,
                cfgs["duckdb"],
                parser=parser,
                streaming_result=True,
                plan_stats=plan_stats,
                routing_policy=routing_policy,
            )
        )
        if chosen not in {Engine.DUCKDB, Engine.ISLANDDB}:
            raise IslandUnsupportedError(
                f"streaming Arrow results do not support {chosen.value}"
            )

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        if chosen is Engine.DUCKDB:
            # Match materialized DuckDB's hit-only whole-object cache behavior,
            # but retain every lease for the complete stream lifetime.
            cache = self._get_file_cache()
            cache_is_useful = bool(
                cache is not None and not cache.source_is_local
            )
            cache_context = (
                cache.localized(
                    reflection,
                    populate=False,
                    tolerate_corrupt_hits=True,
                )
                if cache_is_useful
                else nullcontext((reflection, None))
            )
            entered = False
            try:
                execution_reflection, cache_metrics = cache_context.__enter__()
                entered = True
                if cache_metrics is not None:
                    plan_stats.add_stat(cache_metrics.to_plan_stats())
                duckdb_stream_kwargs = {}
                if max_batch_rows is not None:
                    duckdb_stream_kwargs["max_batch_rows"] = max_batch_rows
                inner = self.duckdb_exec.execute_stream(
                    reflection=execution_reflection,
                    parser=parser,
                    query_manager=query_manager,
                    timer_capture=timer_capture,
                    log_prefix=log_prefix,
                    engine_config=cfgs["duckdb"],
                    timeout_sec=_configured_query_timeout_sec(),
                    **duckdb_stream_kwargs,
                )
            except BaseException as exc:
                if entered:
                    cache_context.__exit__(type(exc), exc, exc.__traceback__)
                raise

            def close_duckdb_stream() -> None:
                try:
                    inner.close()
                finally:
                    cache_context.__exit__(None, None, None)

            stream = ArrowBatchStream(
                inner.schema,
                inner,
                close_callback=close_duckdb_stream,
            )
            if engine is Engine.AUTO:
                plan_stats.add_stat({
                    "AUTO_ROUTING_OUTCOME": {
                        "selected_engine": Engine.DUCKDB.value,
                        "actual_engine": Engine.DUCKDB.value,
                        "fallback": False,
                    },
                })
            plan_stats.add_stat({"ENGINE": "duckdb"})
            plan_stats.add_stat({"RESULT_MODE": "arrow_stream"})
            return stream, "duckdb"

        try:
            island_prepared = self.island_exec.prepare_execution(
                reflection, parser, streaming_result=True,
            )
        except IslandUnsupportedError as exc:
            self._publish_engine_capability(
                plan_stats, IslandCapability(False, (str(exc),)),
            )
            raise
        self._publish_engine_capability(
            plan_stats, island_prepared.capability,
        )

        # Keep whole-object cache leases alive for the complete lifetime of the
        # result stream. Remote built-in backends normally use IslandDB's range
        # reader; this path covers an explicitly disabled/unavailable range
        # layer and makes streaming obey the same localization contract as the
        # materialized facade.
        cache = self._get_file_cache()
        island_uses_ranges = bool(
            settings.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED
            and self.storage is not None
            and _storage_supports_bounded_ranges(self.storage)
        )
        # Match the materialized facade: range mode must never cold-populate a
        # complete object, but it should consume an already-complete immutable
        # hit under a lease for the entire stream lifetime.  Otherwise the same
        # object can be cached twice (whole-object and ranges) and streaming
        # behaves differently from execute().
        cache_is_useful = bool(
            cache is not None
            and (not island_uses_ranges or not cache.source_is_local)
        )
        if (
            cache_is_useful
            and not island_uses_ranges
            and not cache.source_is_local
            and not cache.can_populate_all(reflection)
        ):
            raise RuntimeError(
                "IslandDB cannot localize the complete streaming reflection "
                "within the configured shared-cache byte cap"
            )
        cache_context = (
            cache.localized(
                reflection,
                populate=not island_uses_ranges,
                tolerate_corrupt_hits=island_uses_ranges,
            )
            if cache_is_useful
            else nullcontext((reflection, None))
        )
        entered = False
        try:
            execution_reflection, cache_metrics = cache_context.__enter__()
            entered = True
            if (
                cache_metrics is not None
                and cache_metrics.coverage_ratio != 1.0
                and not island_uses_ranges
            ):
                raise RuntimeError(
                    "IslandDB streaming requires complete cache localization"
                )
            if cache_metrics is not None:
                plan_stats.add_stat(cache_metrics.to_plan_stats())
            island_stream_kwargs = {}
            if max_batch_rows is not None:
                island_stream_kwargs["max_batch_rows"] = max_batch_rows
            inner = self.island_exec.execute_stream(
                reflection=execution_reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=cfgs["duckdb"],
                cache_metrics=cache_metrics,
                _prepared=island_prepared,
                **island_stream_kwargs,
            )
        except BaseException as exc:
            if entered:
                cache_context.__exit__(type(exc), exc, exc.__traceback__)
            raise

        def close_stream() -> None:
            try:
                inner.close()
            finally:
                try:
                    self._publish_island_profile(
                        plan_stats, query_manager, log_prefix,
                    )
                finally:
                    cache_context.__exit__(None, None, None)

        stream = ArrowBatchStream(
            inner.schema,
            inner,
            close_callback=close_stream,
        )
        if engine == Engine.AUTO:
            plan_stats.add_stat({
                "AUTO_ROUTING_OUTCOME": {
                    "selected_engine": Engine.ISLANDDB.value,
                    "actual_engine": Engine.ISLANDDB.value,
                    "fallback": False,
                },
            })
        plan_stats.add_stat({"ENGINE": "islanddb"})
        plan_stats.add_stat({"RESULT_MODE": "arrow_stream"})
        return stream, "islanddb"
