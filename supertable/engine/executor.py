# supertable/engine/executor.py

from __future__ import annotations

import os
import time
import hashlib
import math
import threading
from collections import OrderedDict
from contextlib import nullcontext
from dataclasses import replace
from typing import Callable, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

import pandas as pd

from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb_engine import (
    DuckDB,
    DuckDBPresignRefreshRequired,
    _DuckDBResultLifecycleStream,
)
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
    ResourceReservationCancelled,
    ResultMemoryLimitExceeded,
)
from supertable.engine.stable_http_relay import (
    next_local_credential_generation,
)
from supertable.engine.remote_paths import is_remote_scan_path
from supertable.data_classes import Reflection
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.storage.storage_interface import StorageInterface


_duckdb_singleton: Optional[DuckDB] = None
_duckdb_singletons: "OrderedDict[Tuple[str, tuple], DuckDB]" = OrderedDict()
_duckdb_lock = __import__("threading").Lock()

# Provider SDK presign calls are synchronous and do not share one portable
# timeout/cancellation interface.  Bound the number of calls that may outlive a
# caller deadline.  The worker that actually owns the provider call releases
# its slot; a timed-out waiter must never pretend that still-running work ended.
_PRESIGN_REFRESH_MAX_IN_FLIGHT = 8
_presign_refresh_slots = threading.BoundedSemaphore(
    _PRESIGN_REFRESH_MAX_IN_FLIGHT
)


class _PresignAuthorityGate:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.users = 0


_presign_authority_gates_lock = threading.Lock()
_presign_authority_gates: dict[tuple[type, int, str], _PresignAuthorityGate] = {}


def _acquire_presign_authority_gate(
    presign,
    key: str,
    *,
    deadline_monotonic: Optional[float],
    cancel_event: Optional[threading.Event],
) -> tuple[tuple[type, int, str], _PresignAuthorityGate]:
    """Serialize issuance for one storage authority and immutable key.

    A process generation is useful only when its order matches provider
    invocation order. Keeping generation allocation and the synchronous SDK
    call under this keyed gate prevents a preempted older invocation from
    receiving an ordering marker newer than a credential issued in between.
    Unrelated storage instances and object keys remain fully concurrent.
    """
    authority = getattr(presign, "__self__", None)
    if authority is None:
        authority = presign
    registry_key = (type(authority), id(authority), str(key))
    with _presign_authority_gates_lock:
        gate = _presign_authority_gates.get(registry_key)
        if gate is None:
            gate = _PresignAuthorityGate()
            _presign_authority_gates[registry_key] = gate
        gate.users += 1

    acquired = False
    try:
        while True:
            _raise_if_query_cancelled(cancel_event)
            remaining = _remaining_query_timeout(deadline_monotonic)
            wait_for = 0.05 if remaining is None else min(0.05, remaining)
            if gate.lock.acquire(timeout=max(0.0, wait_for)):
                acquired = True
                return registry_key, gate
    finally:
        if not acquired:
            with _presign_authority_gates_lock:
                gate.users = max(0, gate.users - 1)
                if (
                    gate.users == 0
                    and _presign_authority_gates.get(registry_key) is gate
                ):
                    _presign_authority_gates.pop(registry_key, None)


def _release_presign_authority_gate(
    registry_key: tuple[type, int, str],
    gate: _PresignAuthorityGate,
) -> None:
    gate.lock.release()
    with _presign_authority_gates_lock:
        gate.users = max(0, gate.users - 1)
        if (
            gate.users == 0
            and _presign_authority_gates.get(registry_key) is gate
        ):
            _presign_authority_gates.pop(registry_key, None)


def _configured_query_timeout_sec() -> float:
    """Return a finite positive deadline; malformed config fails bounded."""
    try:
        value = float(
            getattr(settings, "SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC", 60.0)
        )
    except (TypeError, ValueError, OverflowError):
        return 60.0
    return value if math.isfinite(value) and value > 0 else 60.0


def _resolved_stream_limit(
    supplied: Optional[int],
    *,
    setting_name: str,
    fallback: int,
    ceiling: Optional[int] = None,
) -> int:
    try:
        configured = int(getattr(settings, setting_name, fallback))
    except (TypeError, ValueError, OverflowError):
        configured = fallback
    if configured <= 0:
        configured = fallback
    if ceiling is not None:
        configured = min(configured, ceiling)
    if supplied is None:
        return configured
    if (
        isinstance(supplied, bool)
        or not isinstance(supplied, int)
        or supplied <= 0
    ):
        public_name = (
            "max_batch_rows"
            if setting_name.endswith("_ROWS")
            else "max_batch_bytes"
        )
        raise ValueError(f"{public_name} must be a positive integer")
    return min(configured, supplied)


def _remaining_query_timeout(
    deadline_monotonic: Optional[float],
    *,
    fallback: Optional[float] = None,
) -> Optional[float]:
    """Return the positive time remaining on one absolute query deadline."""
    if deadline_monotonic is None:
        return fallback
    try:
        deadline = float(deadline_monotonic)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("query deadline must be finite") from exc
    if not math.isfinite(deadline):
        raise ValueError("query deadline must be finite")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Query deadline expired before engine execution")
    return remaining


def _raise_if_query_cancelled(
    cancel_event: Optional[threading.Event],
) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise ResourceReservationCancelled("query was cancelled before execution")


def _bounded_presign_call(
    presign,
    key: str,
    *,
    expiry_seconds: int,
    deadline_monotonic: Optional[float],
    cancel_event: Optional[threading.Event],
) -> object:
    """Run one blocking provider call behind a deadline-aware bounded gate.

    Python cannot safely kill a thread inside a provider SDK.  The caller may
    nevertheless stop waiting at its request boundary; a daemon worker retains
    the global slot until the SDK call really returns.  This caps abandoned
    workers and applies backpressure to subsequent refreshes without releasing
    resources that remain in use.
    """
    slots = _presign_refresh_slots
    while True:
        _raise_if_query_cancelled(cancel_event)
        remaining = _remaining_query_timeout(deadline_monotonic)
        wait_for = 0.05 if remaining is None else min(0.05, remaining)
        if slots.acquire(timeout=max(0.0, wait_for)):
            break

    try:
        _raise_if_query_cancelled(cancel_event)
        _remaining_query_timeout(deadline_monotonic)
    except BaseException:
        slots.release()
        raise

    done = threading.Event()
    outcome: dict[str, object] = {}

    def invoke() -> None:
        authority_gate = None
        try:
            authority_gate = _acquire_presign_authority_gate(
                presign,
                key,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            _raise_if_query_cancelled(cancel_event)
            _remaining_query_timeout(deadline_monotonic)
            # The keyed authority gate remains held from allocation through
            # the provider call, so generation order is invocation order even
            # if this thread is preempted on either side of allocation.
            outcome["credential_expires_ms"] = (
                int(time.time() * 1_000) + int(expiry_seconds) * 1_000
            )
            outcome["credential_generation"] = (
                next_local_credential_generation()
            )
            outcome["result"] = presign(
                key, expiry_seconds=expiry_seconds,
            )
        except BaseException as exc:
            outcome["error"] = exc
        finally:
            try:
                if authority_gate is not None:
                    _release_presign_authority_gate(*authority_gate)
            finally:
                try:
                    done.set()
                finally:
                    slots.release()

    worker = threading.Thread(
        target=invoke,
        name="supertable-presign-refresh",
        daemon=True,
    )
    try:
        worker.start()
    except BaseException:
        slots.release()
        raise

    while True:
        if done.is_set():
            break
        _raise_if_query_cancelled(cancel_event)
        remaining = _remaining_query_timeout(deadline_monotonic)
        wait_for = 0.05 if remaining is None else min(0.05, remaining)
        done.wait(max(0.0, wait_for))

    _raise_if_query_cancelled(cancel_event)
    _remaining_query_timeout(deadline_monotonic)
    error = outcome.get("error")
    if isinstance(error, BaseException):
        raise error
    return (
        outcome.get("result"),
        outcome.get("credential_generation"),
        outcome.get("credential_expires_ms"),
    )


def _is_remote_scan_path(path: object) -> bool:
    """Compatibility wrapper around the shared provider-scheme classifier."""
    return is_remote_scan_path(path)


def _path_has_bearer_query(path: object) -> bool:
    value = str(path or "").strip()
    if not value.casefold().startswith(("http://", "https://")):
        return False
    try:
        keys = {
            str(key).casefold()
            for key, _ in parse_qsl(
                urlsplit(value).query,
                keep_blank_values=True,
            )
        }
    except (TypeError, ValueError):
        return False
    return bool(keys & {
        "sig", "signature", "token", "access_token", "googleaccessid",
        "x-amz-credential", "x-amz-signature", "x-amz-security-token",
        "x-goog-credential", "x-goog-signature", "key-pair-id", "policy",
    })


def _reflection_has_bearer_paths(reflection: Reflection) -> bool:
    for snapshot in tuple(getattr(reflection, "supers", ()) or ()):
        if any(
            _path_has_bearer_query(path)
            for path in (getattr(snapshot, "files", ()) or ())
        ):
            return True
    for tombstone in (
        getattr(reflection, "tombstone_views", None) or {}
    ).values():
        if _path_has_bearer_query(
            getattr(tombstone, "tombstone_path", "")
        ):
            return True
        if any(
            _path_has_bearer_query(getattr(segment, "tombstone_path", ""))
            for segment in (getattr(tombstone, "segments", ()) or ())
        ):
            return True
    return False


def _snapshot_is_linked(snapshot: object) -> bool:
    return bool(getattr(snapshot, "share_policy_fingerprint", None))


def _reflection_has_linked_snapshots(reflection: Reflection) -> bool:
    return any(
        _snapshot_is_linked(snapshot)
        for snapshot in tuple(getattr(reflection, "supers", ()) or ())
    )


def _reflection_has_linked_remote_paths(reflection: Reflection) -> bool:
    return any(
        _snapshot_is_linked(snapshot)
        and any(
            _is_remote_scan_path(path)
            for path in (getattr(snapshot, "files", ()) or ())
        )
        for snapshot in tuple(getattr(reflection, "supers", ()) or ())
    )


def _reflection_has_refreshable_remote_paths(reflection: Reflection) -> bool:
    """Return whether the consumer storage authority owns any remote path."""
    for snapshot in tuple(getattr(reflection, "supers", ()) or ()):
        if _snapshot_is_linked(snapshot):
            continue
        if any(
            _is_remote_scan_path(path)
            for path in (getattr(snapshot, "files", ()) or ())
        ):
            return True
    # Linked shares currently reject active deletion state. Every accepted
    # tombstone path therefore belongs to the local storage authority.
    for tombstone in (
        getattr(reflection, "tombstone_views", None) or {}
    ).values():
        if _is_remote_scan_path(getattr(tombstone, "tombstone_path", "")):
            return True
        if any(
            _is_remote_scan_path(getattr(segment, "tombstone_path", ""))
            for segment in (getattr(tombstone, "segments", ()) or ())
        ):
            return True
    return False


def _validate_linked_share_credential_lifetimes(
    reflection: Reflection,
    deadline_monotonic: Optional[float],
) -> None:
    """Fail closed unless external bearer paths cover the admitted deadline."""
    linked_remote = [
        snapshot
        for snapshot in tuple(getattr(reflection, "supers", ()) or ())
        if _snapshot_is_linked(snapshot)
        and any(
            _is_remote_scan_path(path)
            for path in (getattr(snapshot, "files", ()) or ())
        )
    ]
    if not linked_remote:
        return
    required_expiry_ms = int(
        (time.time() + _presign_expiry_seconds(deadline_monotonic)) * 1000
    )
    for snapshot in linked_remote:
        expires_ms = getattr(snapshot, "share_credential_expires_ms", None)
        if (
            not isinstance(expires_ms, int)
            or isinstance(expires_ms, bool)
            or expires_ms < required_expiry_ms
        ):
            raise RuntimeError(
                "Provider-issued linked-share credentials do not cover the "
                "query deadline"
            )


def _presign_expiry_seconds(
    deadline_monotonic: Optional[float],
) -> int:
    remaining = _remaining_query_timeout(
        deadline_monotonic,
        fallback=_configured_query_timeout_sec(),
    )
    assert remaining is not None
    try:
        margin = int(
            getattr(settings, "SUPERTABLE_PRESIGN_EXPIRY_MARGIN_SEC", 120)
        )
    except (TypeError, ValueError, OverflowError):
        margin = 120
    try:
        maximum = int(
            getattr(settings, "SUPERTABLE_PRESIGN_MAX_EXPIRY_SEC", 604200)
        )
    except (TypeError, ValueError, OverflowError):
        maximum = 604200
    margin = max(30, min(margin, 3600))
    maximum = max(60, min(maximum, 604200))
    required = max(60, int(math.ceil(remaining)) + margin)
    if required > maximum:
        raise ValueError(
            "query deadline exceeds the configured presigned-credential lifetime"
        )
    return required


def _refresh_presigned_reflection(
    storage: object,
    reflection: Reflection,
    *,
    expiry_seconds: int = 3600,
    deadline_monotonic: Optional[float] = None,
    cancel_event: Optional[threading.Event] = None,
) -> Reflection:
    """Refresh only credential-bearing paths in one pinned reflection.

    Stable snapshot resource keys remain the authorization and tombstone
    identity.  The returned copy retains the exact snapshots, RBAC views,
    pruning selections, and deletion metadata; only executor-facing remote
    paths rotate.  Partial refresh is rejected so a retry never mixes stale and
    fresh authorization contexts.
    """
    presign = getattr(storage, "presign", None)
    if not callable(presign):
        raise RuntimeError("storage does not support credential refresh")

    refresh_deadline = deadline_monotonic
    if refresh_deadline is None:
        refresh_deadline = time.monotonic() + _configured_query_timeout_sec()

    refreshed_any = False

    def refresh_path_with_generation(
        path: object,
        key: object,
        *,
        consumer_authority: bool = True,
    ) -> Tuple[str, Optional[int], Optional[int]]:
        nonlocal refreshed_any
        current = str(path or "")
        if not _is_remote_scan_path(current):
            return current, None, None
        if not consumer_authority:
            # A linked-share URL is issued by the provider control plane. The
            # consumer storage adapter must never reinterpret it as a local
            # object key, even if that adapter returns a syntactically valid
            # URL for arbitrary strings.
            return current, None, None
        _raise_if_query_cancelled(cancel_event)
        _remaining_query_timeout(refresh_deadline)
        stable_key = str(key or "").strip()
        if not stable_key:
            raise RuntimeError("remote snapshot path has no stable resource key")
        try:
            (
                refreshed,
                credential_generation,
                credential_expires_ms,
            ) = _bounded_presign_call(
                presign,
                stable_key,
                expiry_seconds=expiry_seconds,
                deadline_monotonic=refresh_deadline,
                cancel_event=cancel_event,
            )
        except (ResourceReservationCancelled, TimeoutError):
            raise
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            raise RuntimeError("storage credential refresh failed") from None
        if not isinstance(refreshed, str) or not refreshed.strip():
            raise RuntimeError("storage credential refresh returned no path")
        _raise_if_query_cancelled(cancel_event)
        _remaining_query_timeout(refresh_deadline)
        refreshed_any = True
        if (
            not isinstance(credential_generation, int)
            or isinstance(credential_generation, bool)
            or credential_generation <= 0
        ):
            raise RuntimeError("storage credential refresh has no issuance order")
        if (
            not isinstance(credential_expires_ms, int)
            or isinstance(credential_expires_ms, bool)
            or credential_expires_ms <= 0
        ):
            raise RuntimeError("storage credential refresh has no expiry bound")
        return refreshed, credential_generation, credential_expires_ms

    def refresh_path(
        path: object,
        key: object,
        *,
        consumer_authority: bool = True,
    ) -> str:
        return refresh_path_with_generation(
            path, key, consumer_authority=consumer_authority,
        )[0]

    snapshots = []
    for snapshot in tuple(getattr(reflection, "supers", ()) or ()):
        files = list(getattr(snapshot, "files", ()) or ())
        keys = list(getattr(snapshot, "resource_keys", ()) or ())
        if len(files) != len(keys) and any(_is_remote_scan_path(p) for p in files):
            raise RuntimeError(
                "remote snapshot paths do not match stable resource keys"
            )
        existing_generations = list(
            getattr(snapshot, "resource_credential_generations", ()) or ()
        )
        existing_expiries = list(
            getattr(snapshot, "resource_credential_expires_ms", ()) or ()
        )
        refreshed_pairs = [
            refresh_path_with_generation(
                path,
                keys[index] if index < len(keys) else "",
                consumer_authority=not _snapshot_is_linked(snapshot),
            )
            for index, path in enumerate(files)
        ]
        refreshed_files = [
            path for path, _generation, _expiry in refreshed_pairs
        ]
        refreshed_generations = [
            (
                generation
                if generation is not None
                else (
                    existing_generations[index]
                    if index < len(existing_generations)
                    else None
                )
            )
            for index, (_path, generation, _expiry) in enumerate(refreshed_pairs)
        ]
        refreshed_expiries = [
            (
                expiry
                if expiry is not None
                else (
                    existing_expiries[index]
                    if index < len(existing_expiries)
                    else None
                )
            )
            for index, (_path, _generation, expiry) in enumerate(refreshed_pairs)
        ]
        snapshots.append(replace(
            snapshot,
            files=refreshed_files,
            resource_credential_generations=refreshed_generations,
            resource_credential_expires_ms=refreshed_expiries,
        ))

    tombstones = {}
    for alias, tombstone in (
        getattr(reflection, "tombstone_views", None) or {}
    ).items():
        refreshed_segments = tuple(
            replace(
                segment,
                tombstone_path=refresh_path(
                    getattr(segment, "tombstone_path", ""),
                    getattr(segment, "cache_key", ""),
                ),
            )
            for segment in (getattr(tombstone, "segments", ()) or ())
        )
        refreshed_tombstone_path = refresh_path(
            getattr(tombstone, "tombstone_path", ""),
            getattr(tombstone, "cache_key", ""),
        )
        tombstones[alias] = replace(
            tombstone,
            tombstone_path=(refreshed_tombstone_path or None),
            segments=refreshed_segments,
        )

    if not refreshed_any:
        raise RuntimeError("reflection has no refreshable remote paths")
    return replace(
        reflection,
        supers=snapshots,
        tombstone_views=tombstones,
    )


class _RetryBeforeFirstBatchStream:
    """Retry one DuckDB stream only before its first batch is observable."""

    def __init__(self, inner, retry_factory=None):
        self._inner = inner
        self._retry_factory = retry_factory
        self._retried = retry_factory is None
        self._emitted_batches = 0
        self._closed = False
        self.schema = inner.schema

    def __iter__(self):
        return self

    def _replace_inner(self) -> None:
        close = getattr(self._inner, "close", None)
        if callable(close):
            close()
        factory, self._retry_factory = self._retry_factory, None
        self._retried = True
        if factory is None:
            raise RuntimeError(
                "DuckDB remote authorization failed after credential refresh"
            )
        try:
            replacement = factory()
        except (ResourceReservationCancelled, TimeoutError):
            raise
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            raise RuntimeError(
                "DuckDB credential refresh failed before result delivery"
            ) from None
        equals = getattr(self.schema, "equals", None)
        same_schema = (
            bool(equals(replacement.schema, check_metadata=False))
            if callable(equals)
            else replacement.schema == self.schema
        )
        if not same_schema:
            cancel = getattr(replacement, "cancel", None)
            if callable(cancel):
                cancel()
            else:
                replacement.close()
            raise RuntimeError("DuckDB result schema changed during credential refresh")
        self._inner = replacement

    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            batch = next(self._inner)
        except DuckDBPresignRefreshRequired:
            if self._emitted_batches or self._retried:
                self.close()
                raise RuntimeError(
                    "DuckDB remote authorization failed during result delivery"
                ) from None
            self._replace_inner()
            try:
                batch = next(self._inner)
            except DuckDBPresignRefreshRequired:
                self.close()
                raise RuntimeError(
                    "DuckDB remote authorization failed after credential refresh"
                ) from None
        self._emitted_batches += 1
        return batch

    def cancel(self) -> None:
        self._closed = True
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            self._inner.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._inner.close()


def _record_engine_failure(
    plan_stats: PlanStats,
    *,
    engine: Engine,
    stage: str,
    exc: BaseException,
) -> None:
    """Publish only allowlisted, data-free failure dimensions."""
    plan_stats.add_stat({
        "ENGINE_FAILURE": {
            "engine": engine.value,
            "stage": stage,
            "reason_code": type(exc).__name__,
        },
    })


class _FailureTelemetryIterator:
    def __init__(
        self,
        inner,
        *,
        plan_stats: PlanStats,
        engine: Engine,
        stage: str,
    ):
        self._inner = inner
        self._plan_stats = plan_stats
        self._engine = engine
        self._stage = stage
        self._recorded = False
        self.schema = inner.schema

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._inner)
        except StopIteration:
            raise
        except BaseException as exc:
            if not self._recorded:
                self._recorded = True
                _record_engine_failure(
                    self._plan_stats,
                    engine=self._engine,
                    stage=self._stage,
                    exc=exc,
                )
            raise

    def cancel(self) -> None:
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            self._inner.close()

    def close(self) -> None:
        self._inner.close()


def _is_safe_island_auto_stream_fallback(exc: BaseException) -> bool:
    """Allow only capability/resource failures, never cancel/deadline errors."""
    if isinstance(exc, (ResourceReservationCancelled, TimeoutError)):
        return False
    return isinstance(exc, (
        IslandUnsupportedError,
        IslandResourceError,
        ResultMemoryLimitExceeded,
    ))


class _AutoIslandFallbackStream:
    """Replace IslandDB only when it fails before an observable first row."""

    def __init__(
        self,
        inner,
        *,
        fallback_factory,
        island_success,
    ) -> None:
        self._inner = inner
        self._fallback_factory = fallback_factory
        self._island_success = island_success
        self._replaced = False
        self._island_committed = False
        self._emitted_batches = 0
        self._closed = False
        self.schema = inner.schema

    def __iter__(self):
        return self

    def _commit_island(self) -> None:
        if not self._island_committed:
            self._island_committed = True
            self._island_success()

    def _replace(self, exc: BaseException) -> None:
        if self._replaced or self._emitted_batches:
            raise exc
        self._inner.close()
        replacement = self._fallback_factory(exc, "first_batch")
        equals = getattr(self.schema, "equals", None)
        same_schema = (
            bool(equals(replacement.schema, check_metadata=False))
            if callable(equals)
            else replacement.schema == self.schema
        )
        if not same_schema:
            cancel = getattr(replacement, "cancel", None)
            if callable(cancel):
                cancel()
            else:
                replacement.close()
            raise RuntimeError(
                "AUTO fallback result schema differs from IslandDB"
            )
        self._inner = replacement
        self._replaced = True
        self._fallback_factory = None

    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            batch = next(self._inner)
        except StopIteration:
            if not self._replaced:
                self._commit_island()
            raise
        except BaseException as exc:
            if (
                self._replaced
                or self._emitted_batches
                or not _is_safe_island_auto_stream_fallback(exc)
            ):
                raise
            self._replace(exc)
            batch = next(self._inner)
        if not self._replaced:
            self._commit_island()
        self._emitted_batches += 1
        return batch

    def cancel(self) -> None:
        self._closed = True
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            self._inner.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._inner.close()


class _FallbackPlanStats:
    """Forward Duck fallback telemetry without replacing AUTO request facts."""

    def __init__(self, target: PlanStats) -> None:
        self._target = target
        self.stats = target.stats

    def add_stat(self, stat) -> None:
        if not isinstance(stat, dict):
            self._target.add_stat(stat)
            return
        if any(
            key in stat
            for key in (
                "ENGINE_REQUEST",
                "ENGINE_ATTEMPT",
                "AUTO_ROUTING_OUTCOME",
                "RESULT_MODE",
                "RESULT_BATCH_LIMIT",
            )
        ):
            return
        self._target.add_stat(stat)


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


def _credential_attribute_fingerprint(
    credential: object,
    names: tuple[str, ...],
) -> Optional[str]:
    values = []
    present = False
    for name in names:
        try:
            value = getattr(credential, name, None)
        except Exception:
            value = None
        if value not in (None, "", b"", (), []):
            present = True
        values.append(value)
    if not present:
        return None
    return _fingerprint((
        credential.__class__.__module__,
        credential.__class__.__qualname__,
        *values,
    ))


def _credential_safe_url_identity(value: object) -> tuple[tuple, Optional[str]]:
    raw = str(value or "")
    try:
        parsed = urlsplit(raw)
        route = (
            parsed.scheme.casefold(),
            (parsed.hostname or "").casefold(),
            parsed.port,
            parsed.path.rstrip("/"),
        )
        auth = (
            _fingerprint((parsed.username, parsed.password, parsed.query))
            if parsed.query or parsed.username or parsed.password
            else None
        )
        return route, auth
    except (TypeError, ValueError):
        # Invalid/opaque endpoints remain isolated without retaining their raw
        # text, which might itself contain a credential.
        return (("opaque_url_fingerprint", _fingerprint((raw,))),), None


def _storage_identity_details(storage: Optional[object]) -> tuple[tuple, str]:
    """Return a conservative identity for a storage backend.

    Connections carry credentials, endpoints, and cached views.  Sharing one
    merely because two requests both selected "Pro" can expose another org or
    backend's files.  Known immutable routing attributes allow safe reuse;
    opaque/custom storage objects are isolated by object identity.
    """
    if storage is None:
        return ("none",), "none"
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
            if name == "endpoint_url" and isinstance(value, str):
                route, endpoint_auth = _credential_safe_url_identity(value)
                parts.append((name, route))
                if endpoint_auth is not None:
                    parts.append((
                        "endpoint_auth_fingerprint",
                        endpoint_auth,
                    ))
            else:
                parts.append((name, value))

    # Provider clients sometimes keep the account/project route on the client
    # rather than the storage wrapper. Include it without retaining a SAS query
    # or URL userinfo in the registry key.
    if module == "supertable.storage.azure_storage":
        svc = getattr(storage, "svc", None)
        svc_url = str(getattr(svc, "url", "") or "")
        svc_route, svc_auth = _credential_safe_url_identity(svc_url)
        parts.append((
            "azure_account_route",
            (
                str(getattr(svc, "account_name", "") or "").casefold(),
                svc_route,
            ),
        ))
        if svc_auth is not None:
            parts.append((
                "azure_url_auth_fingerprint",
                svc_auth,
            ))
    elif module == "supertable.storage.gcp_storage":
        client = getattr(storage, "client", None)
        parts.append((
            "gcp_project",
            str(getattr(client, "project", "") or ""),
        ))
    # Built-in local storage has no auth state, but its explicit root is a hard
    # namespace boundary.  Never key it by the mutable process CWD: factory
    # instances are rooted at SUPERTABLE_HOME without changing CWD.
    if module == "supertable.storage.local_storage" and qualname == "LocalStorage":
        root = getattr(storage, "root", os.getcwd())
        parts.append(("local_root", os.path.realpath(os.fspath(root))))
        return tuple(parts), "local_root"

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
        # Boto3 commonly stores provider-chain credentials on its request
        # signer rather than the wrapper. Include those values even when only
        # part of an explicit tuple was supplied, so an omitted/rotated secret
        # or token can never collapse two authorization contexts.
        client = getattr(storage, "client", None)
        credentials = getattr(
            getattr(client, "_request_signer", None),
            "_credentials", None,
        )
        try:
            frozen = credentials.get_frozen_credentials()
        except Exception:
            frozen = credentials
        hidden_values = [
            getattr(frozen, "access_key", None),
            getattr(frozen, "secret_key", None),
            getattr(frozen, "token", None),
        ]
        all_credential_values = [*values, *hidden_values]
        if any(
            value not in (None, "", b"")
            for value in all_credential_values
        ):
            parts.append((
                "auth_fingerprint", _fingerprint(all_credential_values),
            ))
            mode = (
                "explicit_credential_fingerprint"
                if any(value not in (None, "", b"") for value in values)
                else "provider_credential_fingerprint"
            )
        else:
            # Unknown injected clients remain isolated. Never merge them
            # merely because their bucket/endpoint route happens to match.
            parts.append(("client_object_id", id(client or storage)))
            mode = "opaque_client_identity"
        return tuple(parts), mode

    # GCS/Azure SDK clients expose their credential object separately.  Use a
    # stable fingerprint only when principal/secret/token attributes are
    # actually available; an opaque provider remains object-isolated.
    client = getattr(storage, "client", None)
    credential = getattr(client, "_credentials", None)
    if credential is None:
        svc = getattr(storage, "svc", None)
        credential = getattr(svc, "credential", None)
    if isinstance(credential, str):
        parts.append(("auth_fingerprint", _fingerprint((credential,))))
        return tuple(parts), "provider_credential_fingerprint"
    if credential is not None:
        fingerprint = _credential_attribute_fingerprint(
            credential,
            (
                "service_account_email", "signer_email", "quota_project_id",
                "universe_domain", "scopes", "_scopes", "token",
                "_refresh_token", "_client_id", "_client_secret",
                "tenant_id", "_tenant_id", "managed_identity_client_id",
                "account_key",
            ),
        )
        signer = getattr(credential, "signer", None)
        signer_key_id = getattr(signer, "key_id", None)
        if fingerprint is not None or signer_key_id:
            parts.append((
                "auth_fingerprint",
                _fingerprint((fingerprint, signer_key_id)),
            ))
            return tuple(parts), "provider_credential_fingerprint"
        parts.append(("credential_object_id", id(credential)))
        return tuple(parts), "opaque_credential_identity"

    # Custom SDK clients can carry opaque refreshable credentials. Isolate by
    # client identity rather than crossing authorization contexts.
    for name in ("client", "svc"):
        try:
            client = getattr(storage, name)
        except Exception:
            continue
        if client is not None:
            parts.append((f"{name}_object_id", id(client)))
    if len(parts) == 2:
        parts.append(("object_id", id(storage)))
    return tuple(parts), "opaque_client_identity"


def _storage_identity(storage: Optional[object]) -> tuple:
    return _storage_identity_details(storage)[0]


def _duckdb_cache_capacity() -> int:
    try:
        configured = int(
            getattr(settings, "SUPERTABLE_DUCKDB_ENGINE_CACHE_MAX_ENTRIES", 16)
        )
    except (TypeError, ValueError, OverflowError):
        configured = 16
    return max(1, min(configured, 256))


def _get_duckdb_with_status(
    storage: Optional[object] = None,
    organization: str = "",
) -> tuple[DuckDB, bool, str, int]:
    global _duckdb_singleton
    identity, identity_mode = _storage_identity_details(storage)
    key = (str(organization or ""), identity)
    capacity = _duckdb_cache_capacity()
    with _duckdb_lock:
        # Retain the historical test/operator reset hook, but close entries
        # safely instead of relying on weak-reference collection.
        if _duckdb_singleton is None and _duckdb_singletons:
            stale = list(_duckdb_singletons.values())
            _duckdb_singletons.clear()
            for old_engine in stale:
                old_engine.request_cache_eviction()
        engine = _duckdb_singletons.get(key)
        cache_hit = engine is not None
        if engine is None:
            engine = DuckDB(storage=storage, organization=organization)
            _duckdb_singletons[key] = engine
        else:
            _duckdb_singletons.move_to_end(key)
        while len(_duckdb_singletons) > capacity:
            _, evicted = _duckdb_singletons.popitem(last=False)
            evicted.request_cache_eviction()
        _duckdb_singleton = engine
        return engine, cache_hit, identity_mode, capacity


def _get_duckdb(
        storage: Optional[object] = None, organization: str = "",
) -> DuckDB:
    return _get_duckdb_with_status(storage, organization)[0]


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
        (
            self.duckdb_exec,
            self._duckdb_cache_hit,
            self._duckdb_identity_mode,
            self._duckdb_cache_capacity,
        ) = _get_duckdb_with_status(
            storage=storage, organization=organization,
        )
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

    def _publish_duckdb_connection_cache(self, plan_stats: PlanStats) -> None:
        try:
            state = self.duckdb_exec.cache_state()
        except Exception:
            state = {}
        plan_stats.add_stat({
            "DUCKDB_CONNECTION_CACHE": {
                "engine_reused": bool(self._duckdb_cache_hit),
                "connection_warm": bool(state.get("connection_open")),
                "identity_mode": self._duckdb_identity_mode,
                "capacity": self._duckdb_cache_capacity,
                "eviction_pending": bool(state.get("eviction_pending")),
            },
        })

    def _execute_duckdb_materialized(
        self,
        *,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer_capture,
        log_prefix: str,
        engine_config,
        plan_stats: PlanStats,
        explain: bool,
        explain_options: str,
        stage: str,
        deadline_monotonic: Optional[float] = None,
    ) -> pd.DataFrame:
        """Run DuckDB behind Executor's single atomic refresh boundary."""
        self._publish_duckdb_connection_cache(plan_stats)
        deadline = deadline_monotonic
        if deadline is None:
            deadline = time.monotonic() + _configured_query_timeout_sec()
        timeout_sec = _remaining_query_timeout(
            deadline,
            fallback=_configured_query_timeout_sec(),
        )
        assert timeout_sec is not None
        refresh_attempted = False

        def run(candidate: Reflection) -> pd.DataFrame:
            return self.duckdb_exec.execute(
                reflection=candidate,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=engine_config,
                explain=explain,
                explain_options=explain_options,
                timeout_sec=timeout_sec,
                deadline_monotonic=deadline,
            )

        def refresh(refresh_stage: str) -> Reflection:
            nonlocal refresh_attempted
            if refresh_attempted:
                raise RuntimeError(
                    "DuckDB credential refresh was already attempted"
                )
            refresh_attempted = True
            expiry_seconds = 0
            try:
                expiry_seconds = _presign_expiry_seconds(deadline)
                refreshed = _refresh_presigned_reflection(
                    self.storage,
                    reflection,
                    expiry_seconds=expiry_seconds,
                    deadline_monotonic=deadline,
                )
            except (ResourceReservationCancelled, TimeoutError) as exc:
                plan_stats.add_stat({
                    "DUCKDB_PRESIGN_REFRESH": {
                        "attempted": True,
                        "succeeded": False,
                        "before_rows": True,
                        "stage": refresh_stage,
                        "reason_code": type(exc).__name__,
                    },
                })
                raise
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    raise
                plan_stats.add_stat({
                    "DUCKDB_PRESIGN_REFRESH": {
                        "attempted": True,
                        "succeeded": False,
                        "before_rows": True,
                        "stage": refresh_stage,
                        "reason_code": type(exc).__name__,
                    },
                })
                raise RuntimeError(
                    "DuckDB credential refresh failed before result delivery"
                ) from None
            plan_stats.add_stat({
                "DUCKDB_PRESIGN_REFRESH": {
                    "attempted": True,
                    "succeeded": True,
                    "before_rows": True,
                    "stage": refresh_stage,
                    "expiry_seconds": expiry_seconds,
                },
            })
            return refreshed

        try:
            refreshable_remote = _reflection_has_refreshable_remote_paths(
                reflection
            )
            if refreshable_remote and (
                bool(getattr(settings, "SUPERTABLE_DUCKDB_PRESIGNED", False))
                or _reflection_has_bearer_paths(reflection)
            ):
                return run(refresh("deadline_ttl"))
            try:
                return run(reflection)
            except DuckDBPresignRefreshRequired:
                if not _reflection_has_refreshable_remote_paths(reflection):
                    raise RuntimeError(
                        "DuckDB provider credential failed before result delivery"
                    ) from None
                return run(refresh("query_setup"))
        except BaseException as exc:
            _record_engine_failure(
                plan_stats,
                engine=Engine.DUCKDB,
                stage=stage,
                exc=exc,
            )
            raise

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
        linked_share_reflection = _reflection_has_linked_snapshots(
            reflection
        )
        linked_bearer_reflection = _reflection_has_linked_remote_paths(
            reflection
        )
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
            and not linked_share_reflection
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
            island_linked_bearer_safe=(
                not linked_share_reflection
            ),
            spark_available=spark_available,
            spark_semantics_supported=spark_semantics_supported,
            fitting_spark_clusters=len(fitting_clusters),
            spark_min_scan_bytes=max(0, int(spark_min or 0)),
            spark_linked_bearer_safe=(
                not linked_bearer_reflection
            ),
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
        deadline_monotonic: Optional[float] = None,
        cancel_event: Optional[threading.Event] = None,
        materialized_row_limit: Optional[int] = None,
        materialized_result_bytes: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, str]:
        _raise_if_query_cancelled(cancel_event)
        configured_deadline = (
            time.monotonic() + _configured_query_timeout_sec()
        )
        if deadline_monotonic is None:
            query_deadline = configured_deadline
        else:
            # A caller may tighten, never extend, the engine's own ceiling.
            _remaining_query_timeout(deadline_monotonic)
            query_deadline = min(
                configured_deadline, float(deadline_monotonic),
            )
        # Authorization-bearing provider paths must cover the entire admitted
        # request before live config, AUTO routing, or cache state is touched.
        _validate_linked_share_credential_lifetimes(
            reflection, query_deadline,
        )
        linked_share_reflection = _reflection_has_linked_snapshots(
            reflection
        )
        linked_bearer_reflection = _reflection_has_linked_remote_paths(
            reflection
        )
        if engine is Engine.ISLANDDB and linked_share_reflection:
            raise RuntimeError(
                "IslandDB cannot consume provider-linked bearer resources safely"
            )
        if (
            engine is Engine.SPARK_SQL
            and linked_bearer_reflection
        ):
            raise RuntimeError(
                "Spark cannot consume provider-linked bearer resources safely"
            )
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
        if (
            chosen is Engine.SPARK_SQL
            and linked_bearer_reflection
        ):
            # Defense in depth if a custom/monkeypatched router bypasses the
            # availability fence above.
            raise RuntimeError(
                "AUTO selected Spark for an ineligible linked-share resource"
            )
        if chosen is Engine.ISLANDDB and linked_share_reflection:
            raise RuntimeError(
                "AUTO selected IslandDB for an ineligible linked-share resource"
            )
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
                df = self._execute_duckdb_materialized(
                    reflection=execution_reflection,
                    parser=parser,
                    query_manager=query_manager,
                    timer_capture=timer_capture,
                    log_prefix=log_prefix,
                    engine_config=duckdb_cfg,
                    plan_stats=plan_stats,
                    explain=explain,
                    explain_options=explain_options,
                    stage=attempt_stage,
                    deadline_monotonic=query_deadline,
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
                        deadline_monotonic=query_deadline,
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
                    _record_engine_failure(
                        plan_stats,
                        engine=Engine.ISLANDDB,
                        stage=attempt_stage,
                        exc=exc,
                    )
                    # IslandExecutionTimeout also derives from
                    # IslandUnsupportedError so callers can handle the native
                    # engine's cooperative timeout uniformly. Cancellation is
                    # likewise an explicit terminal request, not a capability
                    # rejection. Replaying either on DuckDB would exceed the
                    # request boundary and can duplicate work after IslandDB has
                    # already started.
                    if isinstance(
                        exc, (ResourceReservationCancelled, TimeoutError),
                    ):
                        raise
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
                    df = self._execute_duckdb_materialized(
                        reflection=execution_reflection,
                        parser=parser,
                        query_manager=query_manager,
                        timer_capture=timer_capture,
                        log_prefix=log_prefix,
                        engine_config=duckdb_cfg,
                        plan_stats=plan_stats,
                        explain=explain,
                        explain_options=explain_options,
                        stage="auto_fallback",
                        deadline_monotonic=query_deadline,
                    )
                    used = "duckdb"
                except BaseException as exc:
                    _record_engine_failure(
                        plan_stats,
                        engine=Engine.ISLANDDB,
                        stage=attempt_stage,
                        exc=exc,
                    )
                    raise

            elif chosen == Engine.SPARK_SQL:
                if self.spark_exec is None:
                    from supertable.engine.spark_thrift import SparkThriftExecutor
                    self.spark_exec = SparkThriftExecutor(
                        storage=self.storage, organization=self.organization,
                    )
                # force=True when user explicitly requested Spark (not via AUTO)
                try:
                    df = self.spark_exec.execute(
                        reflection=execution_reflection,
                        parser=parser,
                        query_manager=query_manager,
                        timer_capture=timer_capture,
                        log_prefix=log_prefix,
                        force=(engine == Engine.SPARK_SQL),
                        deadline_monotonic=query_deadline,
                        cancel_event=cancel_event,
                        max_result_rows=materialized_row_limit,
                        max_result_bytes=materialized_result_bytes,
                    )
                except BaseException as exc:
                    _record_engine_failure(
                        plan_stats,
                        engine=Engine.SPARK_SQL,
                        stage=attempt_stage,
                        exc=exc,
                    )
                    raise
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
        max_batch_bytes: Optional[int] = None,
        deadline_monotonic: Optional[float] = None,
        cancel_event: Optional[threading.Event] = None,
        _resolved_bundle=None,
        _linked_credentials_validated: bool = False,
    ):
        """Return a one-shot Arrow batch stream without pandas.

        DuckDB and IslandDB preserve their query/cache resources until stream
        close. Spark remains explicit-only and materialized; the streaming API
        fails instead of silently changing its result-lifetime contract.
        """
        # Fail before routing/cache work when a caller-provided end-to-end
        # deadline has already elapsed.  Engine-specific defaults remain in
        # force when no caller deadline is supplied.
        _raise_if_query_cancelled(cancel_event)
        if deadline_monotonic is None:
            deadline_monotonic = (
                time.monotonic() + _configured_query_timeout_sec()
            )
        _remaining_query_timeout(deadline_monotonic)
        if not _linked_credentials_validated:
            # This common admission boundary covers DuckDB and IslandDB,
            # including AUTO, before configuration, routing, or cache I/O.
            _validate_linked_share_credential_lifetimes(
                reflection, deadline_monotonic,
            )
            _linked_credentials_validated = True
        linked_share_reflection = _reflection_has_linked_snapshots(
            reflection
        )
        if engine is Engine.ISLANDDB and linked_share_reflection:
            raise RuntimeError(
                "IslandDB cannot consume provider-linked bearer resources safely"
            )
        max_batch_rows = _resolved_stream_limit(
            max_batch_rows,
            setting_name="SUPERTABLE_RESULT_STREAM_BATCH_ROWS",
            fallback=256,
            ceiling=4096,
        )
        max_batch_bytes = _resolved_stream_limit(
            max_batch_bytes,
            setting_name="SUPERTABLE_RESULT_STREAM_BATCH_BYTES",
            fallback=4 * 1024 * 1024,
        )
        plan_stats.add_stat({
            "RESULT_BATCH_LIMIT": {
                "max_rows": max_batch_rows,
                "max_bytes": max_batch_bytes,
            },
        })
        cfgs, routing_policy = (
            _resolved_bundle
            if _resolved_bundle is not None
            else resolve_engine_bundle(
                self.organization, self._get_catalog()
            )
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
        if chosen is Engine.ISLANDDB and linked_share_reflection:
            raise RuntimeError(
                "AUTO selected IslandDB for an ineligible linked-share resource"
            )
        plan_stats.add_stat({
            "ENGINE_REQUEST": {
                "requested_engine": engine.value,
                "selected_engine": chosen.value,
                "forced": engine != Engine.AUTO,
            },
        })
        plan_stats.add_stat({
            "ENGINE_ATTEMPT": {
                "engine": chosen.value,
                "stage": "primary_stream",
            },
        })
        if chosen not in {Engine.DUCKDB, Engine.ISLANDDB}:
            raise IslandUnsupportedError(
                f"streaming Arrow results do not support {chosen.value}"
            )

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        def start_auto_island_fallback(
            exc: BaseException,
            stage: str,
        ):
            if (
                engine is not Engine.AUTO
                or not _is_safe_island_auto_stream_fallback(exc)
            ):
                raise exc
            _raise_if_query_cancelled(cancel_event)
            _remaining_query_timeout(deadline_monotonic)
            plan_stats.add_stat({
                "ENGINE_ATTEMPT": {
                    "engine": Engine.DUCKDB.value,
                    "stage": f"auto_stream_fallback_{stage}",
                    "reason_code": type(exc).__name__,
                },
            })
            fallback_stats = _FallbackPlanStats(plan_stats)
            fallback_stream, _ = self.execute_stream(
                engine=Engine.DUCKDB,
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer=timer,
                plan_stats=fallback_stats,
                log_prefix=log_prefix,
                max_batch_rows=max_batch_rows,
                max_batch_bytes=max_batch_bytes,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                # Keep one immutable routing/config snapshot for the entire
                # request.  Besides avoiding a second catalog lookup, this
                # prevents a live config change from altering fallback
                # semantics after IslandDB has already been selected.
                _resolved_bundle=(cfgs, routing_policy),
                _linked_credentials_validated=True,
            )
            plan_stats.add_stat({
                "AUTO_ROUTING_OUTCOME": {
                    "selected_engine": Engine.ISLANDDB.value,
                    "actual_engine": Engine.DUCKDB.value,
                    "fallback": True,
                    "reason_code": type(exc).__name__,
                    "stage": stage,
                },
            })
            return fallback_stream

        if chosen is Engine.DUCKDB:
            duckdb_lifecycle_started = time.monotonic()
            duckdb_deadline = deadline_monotonic
            if duckdb_deadline is None:
                duckdb_deadline = (
                    duckdb_lifecycle_started + _configured_query_timeout_sec()
                )
            duckdb_timeout_value = max(
                0.0, duckdb_deadline - duckdb_lifecycle_started,
            )
            self._publish_duckdb_connection_cache(plan_stats)
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
                _raise_if_query_cancelled(cancel_event)
                _remaining_query_timeout(duckdb_deadline)
                if cache_metrics is not None:
                    plan_stats.add_stat(cache_metrics.to_plan_stats())

                def start_duckdb_stream(stream_reflection):
                    return self.duckdb_exec.execute_stream(
                        reflection=stream_reflection,
                        parser=parser,
                        query_manager=query_manager,
                        timer_capture=timer_capture,
                        log_prefix=log_prefix,
                        engine_config=cfgs["duckdb"],
                        timeout_sec=_remaining_query_timeout(
                            duckdb_deadline,
                            fallback=_configured_query_timeout_sec(),
                        ),
                        deadline_monotonic=duckdb_deadline,
                        cancel_event=cancel_event,
                        max_batch_rows=max_batch_rows,
                        max_batch_bytes=max_batch_bytes,
                    )

                refresh_attempted = False

                def start_refreshed_stream(stage: str):
                    nonlocal refresh_attempted
                    if refresh_attempted:
                        raise RuntimeError(
                            "DuckDB credential refresh was already attempted"
                        )
                    refresh_attempted = True
                    expiry_seconds = 0
                    try:
                        expiry_seconds = _presign_expiry_seconds(
                            duckdb_deadline,
                        )
                        refreshed = _refresh_presigned_reflection(
                            self.storage,
                            execution_reflection,
                            expiry_seconds=expiry_seconds,
                            deadline_monotonic=duckdb_deadline,
                            cancel_event=cancel_event,
                        )
                        refreshed_stream = start_duckdb_stream(refreshed)
                    except (ResourceReservationCancelled, TimeoutError) as exc:
                        plan_stats.add_stat({
                            "DUCKDB_PRESIGN_REFRESH": {
                                "attempted": True,
                                "succeeded": False,
                                "before_rows": True,
                                "stage": stage,
                                "reason_code": type(exc).__name__,
                            },
                        })
                        raise
                    except BaseException as exc:
                        if not isinstance(exc, Exception):
                            raise
                        plan_stats.add_stat({
                            "DUCKDB_PRESIGN_REFRESH": {
                                "attempted": True,
                                "succeeded": False,
                                "before_rows": True,
                                "stage": stage,
                                "reason_code": type(exc).__name__,
                            },
                        })
                        raise RuntimeError(
                            "DuckDB credential refresh failed before result delivery"
                        ) from None
                    plan_stats.add_stat({
                        "DUCKDB_PRESIGN_REFRESH": {
                            "attempted": True,
                            "succeeded": True,
                            "before_rows": True,
                            "stage": stage,
                            "expiry_seconds": expiry_seconds,
                        },
                    })
                    return refreshed_stream

                refreshable_remote = _reflection_has_refreshable_remote_paths(
                    execution_reflection
                )
                if refreshable_remote and (
                    bool(getattr(
                        settings, "SUPERTABLE_DUCKDB_PRESIGNED", False,
                    ))
                    or _reflection_has_bearer_paths(execution_reflection)
                ):
                    # Mint the only consumer-owned credential at the bounded
                    # DuckDB setup boundary. Existing bearer paths are also
                    # replaced so their TTL covers this request's deadline.
                    initial_inner = start_refreshed_stream("deadline_ttl")
                    retry_factory = None
                else:
                    try:
                        initial_inner = start_duckdb_stream(execution_reflection)
                    except DuckDBPresignRefreshRequired:
                        initial_inner = start_refreshed_stream("query_setup")
                        retry_factory = None
                    else:
                        retry_factory = (
                            (lambda: start_refreshed_stream("first_batch"))
                            if refreshable_remote else None
                        )
                inner = _RetryBeforeFirstBatchStream(
                    initial_inner, retry_factory=retry_factory,
                )
            except BaseException as exc:
                if entered:
                    cache_context.__exit__(type(exc), exc, exc.__traceback__)
                _record_engine_failure(
                    plan_stats,
                    engine=Engine.DUCKDB,
                    stage="stream_setup",
                    exc=exc,
                )
                raise

            inner = _FailureTelemetryIterator(
                inner,
                plan_stats=plan_stats,
                engine=Engine.DUCKDB,
                stage="stream_delivery",
            )

            def close_duckdb_stream() -> None:
                try:
                    inner.close()
                finally:
                    cache_context.__exit__(None, None, None)

            stream = ArrowBatchStream(
                inner.schema,
                inner,
                close_callback=close_duckdb_stream,
                cancel_event=cancel_event,
            )
            # The DuckDB engine owns its cursor lifecycle, but this outer stream
            # additionally owns the whole-object cache context. Drive the same
            # cooperative close state machine here so an idle deadline/cancel
            # cannot release the cursor while leaving its cache lease stranded.
            stream = _DuckDBResultLifecycleStream(
                stream,
                deadline_monotonic=duckdb_deadline,
                timeout_value=duckdb_timeout_value,
                cancel_event=cancel_event,
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
            _record_engine_failure(
                plan_stats,
                engine=Engine.ISLANDDB,
                stage="stream_prepare",
                exc=exc,
            )
            if engine is Engine.AUTO:
                return start_auto_island_fallback(exc, "prepare"), "duckdb"
            raise
        except (IslandResourceError, ResultMemoryLimitExceeded) as exc:
            _record_engine_failure(
                plan_stats,
                engine=Engine.ISLANDDB,
                stage="stream_prepare",
                exc=exc,
            )
            if engine is Engine.AUTO:
                return start_auto_island_fallback(exc, "prepare"), "duckdb"
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
            and engine is not Engine.AUTO
            and not island_uses_ranges
            and not cache.source_is_local
            and not cache.can_populate_all(reflection)
        ):
            raise IslandResourceError(
                "IslandDB cannot localize the complete streaming reflection "
                "within the configured shared-cache byte cap"
            )
        cache_context = (
            cache.localized(
                reflection,
                # AUTO routing is never allowed to turn a streaming request
                # into a cold whole-reflection download.  A complete warm hit
                # is leased below; an incomplete/corrupt hit falls back to
                # DuckDB before any Arrow batch becomes observable.  Explicit
                # IslandDB retains its opt-in whole-object localization.
                populate=(
                    engine is not Engine.AUTO and not island_uses_ranges
                ),
                tolerate_corrupt_hits=(
                    engine is Engine.AUTO or island_uses_ranges
                ),
            )
            if cache_is_useful
            else nullcontext((reflection, None))
        )
        entered = False
        try:
            execution_reflection, cache_metrics = cache_context.__enter__()
            entered = True
            _raise_if_query_cancelled(cancel_event)
            _remaining_query_timeout(deadline_monotonic)
            if (
                cache_metrics is not None
                and cache_metrics.coverage_ratio != 1.0
                and not island_uses_ranges
            ):
                raise IslandResourceError(
                    "IslandDB streaming requires complete cache localization"
                )
            if cache_metrics is not None:
                plan_stats.add_stat(cache_metrics.to_plan_stats())
            inner = self.island_exec.execute_stream(
                reflection=execution_reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=cfgs["duckdb"],
                cache_metrics=cache_metrics,
                _prepared=island_prepared,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                max_batch_rows=max_batch_rows,
                max_batch_bytes=max_batch_bytes,
            )
        except BaseException as exc:
            if entered:
                cache_context.__exit__(type(exc), exc, exc.__traceback__)
            _record_engine_failure(
                plan_stats,
                engine=Engine.ISLANDDB,
                stage="stream_setup",
                exc=exc,
            )
            if (
                engine is Engine.AUTO
                and _is_safe_island_auto_stream_fallback(exc)
            ):
                return start_auto_island_fallback(exc, "setup"), "duckdb"
            raise

        inner = _FailureTelemetryIterator(
            inner,
            plan_stats=plan_stats,
            engine=Engine.ISLANDDB,
            stage="stream_delivery",
        )

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

        island_stream = ArrowBatchStream(
            inner.schema,
            inner,
            close_callback=close_stream,
            cancel_event=cancel_event,
        )
        if engine == Engine.AUTO:
            def commit_island_success() -> None:
                plan_stats.add_stat({
                    "AUTO_ROUTING_OUTCOME": {
                        "selected_engine": Engine.ISLANDDB.value,
                        "actual_engine": Engine.ISLANDDB.value,
                        "fallback": False,
                    },
                })
                plan_stats.add_stat({"ENGINE": "islanddb"})

            auto_inner = _AutoIslandFallbackStream(
                island_stream,
                fallback_factory=start_auto_island_fallback,
                island_success=commit_island_success,
            )
            stream = ArrowBatchStream(
                auto_inner.schema,
                auto_inner,
                cancel_event=cancel_event,
            )
        else:
            plan_stats.add_stat({"ENGINE": "islanddb"})
            stream = island_stream
        plan_stats.add_stat({"RESULT_MODE": "arrow_stream"})
        return stream, "islanddb"
