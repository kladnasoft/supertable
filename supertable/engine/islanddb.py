"""IslandDB: a conservative, specialised Parquet SQL engine.

IslandDB is intentionally *not* a second general-purpose SQL implementation.
It accepts a statically checked subset of SELECT, registers the estimator's
exact surviving Parquet files as Polars lazy scans, and lets Polars push
projections and predicates into Parquet row groups.  Anything whose semantics
are not proven equivalent to SuperTable's DuckDB contract is rejected before
I/O with :class:`IslandUnsupportedError`.

Local and whole-object-cache paths use the native multi-file scanner.  Remote
objects use a version-sealed seekable range cache, so Arrow fetches only the
footer and compressed column ranges needed by the selected row groups.  Stable
``resource_keys`` remain attached to rows for the composite deletion-vector
anti join in both cases.
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import stat
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as papq
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.data_classes import (
    IntegerDomainBound,
    Reflection,
    ResourceObjectSeal,
    SuperSnapshot,
)
from supertable.engine.engine_common import (
    ROWID_COL,
    SOURCE_FILE_COL,
    TIMESTAMP_COL,
    rewrite_query_with_hashed_tables,
    tombstone_data_paths,
)
from supertable.processing import (
    TOMBSTONE_FILE_COL,
    load_tombstone,
    load_tombstone_segments,
    parquet_footer_sha256,
)
from supertable.engine.island_resources import (
    ArrowBatchStream,
    ContainerResources,
    ExecutionAdvice,
    QueryResourceEstimate,
    QueryResourcePlan,
    ResourceGovernor,
    ResourcePlanner,
    ResourcePolicy,
    ResultMemoryLimitExceeded,
)
from supertable.engine.island_spill import (
    AggregateSpec,
    IslandSpillError,
    SpillDeadlineExceeded,
    SpillSession,
    external_group_aggregate,
    external_sort,
)
from supertable.storage.storage_interface import ObjectMetadata
from supertable.tombstone_manifest_v2 import (
    validate_snapshot_tombstone_state,
)


_monotonic = time.monotonic


class IslandUnsupportedError(RuntimeError):
    """The query is outside IslandDB's proven-equivalent native subset."""


class IslandExecutionTimeout(IslandUnsupportedError, TimeoutError):
    """IslandDB exceeded its cooperative wall-clock execution deadline."""


class IslandIntegrityError(RuntimeError):
    """Pinned data or deletion metadata failed a correctness boundary."""


def _validate_island_tombstone_definition(tomb_def) -> bool:
    """Validate one direct executor definition and return whether it is active.

    Production definitions come from ``DataReader``, but IslandDB is also a
    callable engine boundary. Validate the snapshot discriminator before a
    truthiness check can silently reinterpret a malformed active/empty hybrid
    as "no tombstone".
    """
    if tomb_def is None:
        return False
    pointer = getattr(tomb_def, "tombstone_path", None)
    cache_key = getattr(tomb_def, "cache_key", None)
    rows = getattr(tomb_def, "expected_rows", None)
    digest = getattr(tomb_def, "tombstone_digest", None)
    tombstone_format = getattr(tomb_def, "tombstone_format", None)
    segments = getattr(tomb_def, "segments", ())
    try:
        validate_snapshot_tombstone_state(
            cache_key if pointer is not None else None,
            rows,
            digest,
            format_present=tombstone_format is not None,
            tombstone_format=tombstone_format,
        )
    except (TypeError, ValueError) as exc:
        raise IslandIntegrityError(
            "invalid deletion-vector snapshot state"
        ) from exc

    if pointer is None:
        if cache_key is not None or segments != ():
            raise IslandIntegrityError(
                "invalid empty deletion-vector representation"
            )
        return False
    try:
        tombstone_data_paths(tomb_def)
    except Exception as exc:
        raise IslandIntegrityError(
            "invalid deletion-vector representation"
        ) from exc
    return True


@dataclass(frozen=True)
class IslandCapability:
    supported: bool
    reasons: Tuple[str, ...] = ()

    def require(self) -> None:
        if not self.supported:
            raise IslandUnsupportedError("; ".join(self.reasons))


@dataclass(frozen=True)
class IslandPreparedQuery:
    """Pure, reusable validation and resource decision for one query.

    Executor prepares this before any optional cache localization.  Localizing
    immutable objects changes only physical paths, so the capability and byte
    budgets remain valid for the execution reflection.  Keeping this object
    explicit prevents the materialized facade from repeating the same semantic
    walk and resource plan inside ``execute_stream``.
    """

    capability: IslandCapability
    resource_plan: QueryResourcePlan
    streaming_result: bool


@dataclass
class IslandProfile:
    telemetry_query_id: str = ""
    native: bool = True
    source_bytes: int = 0
    estimated_scan_bytes: int = 0
    files: int = 0
    elapsed_ms: float = 0.0
    optimized_plan: str = ""
    cache: Dict[str, object] = field(default_factory=dict)
    resources: Dict[str, object] = field(default_factory=dict)
    spill: Dict[str, object] = field(default_factory=dict)
    estimated_candidate_files: int = 0
    estimated_candidate_files_complete: bool = False
    estimated_candidate_row_groups: int = 0
    estimated_candidate_row_groups_complete: bool = False
    # Physical scan units proven while constructing the executable relation.
    # These are plan facts, not claims about which units a native scanner
    # ultimately opened after its own runtime statistics pruning.
    planned_files: int = 0
    planned_files_complete: bool = False
    planned_row_groups: int = 0
    planned_row_groups_complete: bool = False
    planned_rows: int = 0
    planned_rows_complete: bool = False
    planned_units_scope: str = "scan_node_occurrences"
    # Polars/Arrow do not currently expose trustworthy runtime file/row-group
    # counters for every IslandDB path.  Keep unavailable observations nullable
    # rather than serialising a plausible-looking zero.
    observed_files: Optional[int] = None
    observed_files_measured: bool = False
    observed_row_groups: Optional[int] = None
    observed_row_groups_measured: bool = False
    observed_rows_scanned: Optional[int] = None
    observed_rows_scanned_measured: bool = False
    execution_outcome: str = "unknown"
    result_complete: bool = False
    # Deprecated compatibility alias for ``planned_row_groups``.  It remains
    # present for older readers but is explicitly labelled in the JSON profile.
    selected_row_groups: int = 0
    selected_row_groups_scope: str = "deprecated_planned_alias"
    cpu_time_ms: float = 0.0
    cpu_time_measured: bool = False
    cpu_time_scope: str = (
        "process_cpu_delta_after_admission_until_profile_finalize"
    )
    estimated_logical_scan_bytes: int = 0
    estimated_logical_scan_bytes_complete: bool = False
    logical_scan_bytes: int = 0
    logical_scan_bytes_complete: bool = False
    physical_read_bytes: int = 0
    physical_read_bytes_measured: bool = False
    physical_read_scope: str = (
        "linux_proc_self_io_block_read_delta_after_admission_"
        "until_profile_finalize"
    )
    estimated_decoded_bytes: int = 0
    estimated_decoded_bytes_complete: bool = False
    decoded_bytes: int = 0
    decoded_bytes_complete: bool = False
    estimated_candidate_rows: int = 0
    estimated_candidate_rows_complete: bool = False
    rows_scanned: int = 0
    rows_scanned_measured: bool = False
    result_rows: int = 0
    result_rows_scope: str = "arrow_output_rows"
    result_batches: int = 0
    result_batches_scope: str = "arrow_output_record_batches"
    result_bytes: int = 0
    result_bytes_scope: str = "arrow_output_batch_logical_nbytes"
    rss_baseline_bytes: Optional[int] = None
    rss_peak_bytes: Optional[int] = None
    rss_final_bytes: Optional[int] = None
    rss_peak_delta_bytes: Optional[int] = None
    rss_retained_delta_bytes: Optional[int] = None
    rss_measured: bool = False
    rss_sample_interval_ms: float = 10.0
    rss_scope: str = "unknown"
    peak_memory_bytes: int = 0
    peak_memory_scope: str = "unknown"
    elapsed_scope: str = "engine_after_admission_to_result_ready_excludes_profile_persist"
    phase_timings_ms: Dict[str, float] = field(default_factory=dict)
    phase_timings_scope: str = "monotonic_wall_nested_non_additive"
    profile_persist_ms: Optional[float] = None
    profile_persist_ms_measured: bool = False
    profile_persist_succeeded: Optional[bool] = None
    spill_bytes: int = 0
    spill_bytes_measured: bool = False
    spill_bytes_scope: str = "peak_query_temp_footprint"
    spill_occurred: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {
            "engine": "islanddb",
            "telemetry_query_id": self.telemetry_query_id,
            "native": self.native,
            "source_bytes": int(self.source_bytes),
            "estimated_scan_bytes": int(self.estimated_scan_bytes),
            "files": int(self.files),
            "elapsed_ms": round(float(self.elapsed_ms), 3),
            "optimized_plan": self.optimized_plan,
            "cache": dict(self.cache),
            "resources": dict(self.resources),
            "spill": dict(self.spill),
            "estimated_candidate_files": int(self.estimated_candidate_files),
            "estimated_candidate_files_complete": bool(
                self.estimated_candidate_files_complete
            ),
            "estimated_candidate_row_groups": int(
                self.estimated_candidate_row_groups
            ),
            "estimated_candidate_row_groups_complete": bool(
                self.estimated_candidate_row_groups_complete
            ),
            "planned_files": int(self.planned_files),
            "planned_files_complete": bool(self.planned_files_complete),
            "planned_row_groups": int(self.planned_row_groups),
            "planned_row_groups_complete": bool(self.planned_row_groups_complete),
            "planned_rows": int(self.planned_rows),
            "planned_rows_complete": bool(self.planned_rows_complete),
            "planned_units_scope": self.planned_units_scope,
            "observed_files": (
                int(self.observed_files) if self.observed_files is not None else None
            ),
            "observed_files_measured": bool(self.observed_files_measured),
            "observed_row_groups": (
                int(self.observed_row_groups)
                if self.observed_row_groups is not None else None
            ),
            "observed_row_groups_measured": bool(
                self.observed_row_groups_measured
            ),
            "observed_rows_scanned": (
                int(self.observed_rows_scanned)
                if self.observed_rows_scanned is not None else None
            ),
            "observed_rows_scanned_measured": bool(
                self.observed_rows_scanned_measured
            ),
            "execution_outcome": self.execution_outcome,
            "result_complete": bool(self.result_complete),
            "selected_row_groups": int(self.selected_row_groups),
            "selected_row_groups_scope": self.selected_row_groups_scope,
            "cpu_time_ms": round(float(self.cpu_time_ms), 3),
            "cpu_time_measured": bool(self.cpu_time_measured),
            "cpu_time_scope": self.cpu_time_scope,
            "estimated_logical_scan_bytes": int(
                self.estimated_logical_scan_bytes
            ),
            "estimated_logical_scan_bytes_complete": bool(
                self.estimated_logical_scan_bytes_complete
            ),
            "logical_scan_bytes": int(self.logical_scan_bytes),
            "logical_scan_bytes_complete": bool(self.logical_scan_bytes_complete),
            "physical_read_bytes": int(self.physical_read_bytes),
            "physical_read_bytes_measured": bool(self.physical_read_bytes_measured),
            "physical_read_scope": self.physical_read_scope,
            "estimated_decoded_bytes": int(self.estimated_decoded_bytes),
            "estimated_decoded_bytes_complete": bool(
                self.estimated_decoded_bytes_complete
            ),
            "decoded_bytes": int(self.decoded_bytes),
            "decoded_bytes_complete": bool(self.decoded_bytes_complete),
            "estimated_candidate_rows": int(self.estimated_candidate_rows),
            "estimated_candidate_rows_complete": bool(
                self.estimated_candidate_rows_complete
            ),
            "rows_scanned": int(self.rows_scanned),
            "rows_scanned_measured": bool(self.rows_scanned_measured),
            "result_rows": int(self.result_rows),
            "result_rows_scope": self.result_rows_scope,
            "result_batches": int(self.result_batches),
            "result_batches_scope": self.result_batches_scope,
            "result_bytes": int(self.result_bytes),
            "result_bytes_scope": self.result_bytes_scope,
            "rss_baseline_bytes": (
                int(self.rss_baseline_bytes)
                if self.rss_baseline_bytes is not None else None
            ),
            "rss_peak_bytes": (
                int(self.rss_peak_bytes) if self.rss_peak_bytes is not None else None
            ),
            "rss_final_bytes": (
                int(self.rss_final_bytes) if self.rss_final_bytes is not None else None
            ),
            "rss_peak_delta_bytes": (
                int(self.rss_peak_delta_bytes)
                if self.rss_peak_delta_bytes is not None else None
            ),
            "rss_retained_delta_bytes": (
                int(self.rss_retained_delta_bytes)
                if self.rss_retained_delta_bytes is not None else None
            ),
            "rss_measured": bool(self.rss_measured),
            "rss_sample_interval_ms": round(
                float(self.rss_sample_interval_ms), 3,
            ),
            "rss_scope": self.rss_scope,
            "peak_memory_bytes": int(self.peak_memory_bytes),
            "peak_memory_scope": self.peak_memory_scope,
            "elapsed_scope": self.elapsed_scope,
            "phase_timings_ms": {
                str(name): round(float(value), 3)
                for name, value in self.phase_timings_ms.items()
            },
            "phase_timings_scope": self.phase_timings_scope,
            # A profile cannot truthfully contain the duration of the atomic
            # write which is persisting that same payload.  The artifact marks
            # it unavailable; ``last_profile`` is updated with the exact
            # attempt duration immediately after the commit returns.
            "profile_persist_ms": (
                round(float(self.profile_persist_ms), 3)
                if self.profile_persist_ms is not None else None
            ),
            "profile_persist_ms_measured": bool(
                self.profile_persist_ms_measured
            ),
            "profile_persist_succeeded": self.profile_persist_succeeded,
            "spill_bytes": int(self.spill_bytes),
            "spill_bytes_measured": bool(self.spill_bytes_measured),
            "spill_bytes_scope": self.spill_bytes_scope,
            "spill_occurred": bool(self.spill_occurred),
        }


def _proc_counter(name: str) -> Optional[int]:
    """Read one Linux /proc/self/io counter; unavailable off Linux."""
    try:
        with open("/proc/self/io", "r", encoding="ascii") as handle:
            for line in handle:
                key, _, value = line.partition(":")
                if key == name:
                    return max(0, int(value.strip()))
    except (OSError, ValueError):
        return None
    return None


def _process_rss_bytes() -> Optional[int]:
    try:
        with open("/proc/self/status", "r", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return max(0, int(line.split()[1]) * 1024)
    except (OSError, ValueError, IndexError):
        return None
    return None


class _IslandTelemetry:
    """Low-overhead, explicitly process-scoped execution measurements."""

    def __init__(self) -> None:
        self.cpu_started = time.process_time()
        self.read_started = _proc_counter("read_bytes")
        self.rss_started = _process_rss_bytes()
        self.rss_peak = self.rss_started
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._sample_rss, name="islanddb-telemetry", daemon=True,
        )
        self._thread.start()

    def _sample_rss(self) -> None:
        while not self._stop.wait(0.01):
            value = _process_rss_bytes()
            if value is not None:
                self.rss_peak = max(self.rss_peak or 0, value)

    def finish(self) -> Dict[str, object]:
        self._stop.set()
        self._thread.join(timeout=0.1)
        rss = _process_rss_bytes()
        if rss is not None:
            self.rss_peak = max(self.rss_peak or 0, rss)
        read_finished = _proc_counter("read_bytes")
        read_measured = (
            self.read_started is not None
            and read_finished is not None
            and int(read_finished) >= int(self.read_started)
        )
        rss_measured = (
            self.rss_started is not None
            and self.rss_peak is not None
            and rss is not None
        )
        rss_peak_delta = (
            max(0, int(self.rss_peak) - int(self.rss_started))
            if rss_measured else None
        )
        rss_retained_delta = (
            int(rss) - int(self.rss_started) if rss_measured else None
        )
        return {
            "cpu_time_ms": max(0.0, (time.process_time() - self.cpu_started) * 1000.0),
            "cpu_time_measured": True,
            "physical_read_bytes": (
                max(0, int(read_finished) - int(self.read_started))
                if read_measured else 0
            ),
            "physical_read_bytes_measured": read_measured,
            "rss_baseline_bytes": self.rss_started,
            "rss_peak_bytes": self.rss_peak,
            "rss_final_bytes": rss,
            "rss_peak_delta_bytes": rss_peak_delta,
            "rss_retained_delta_bytes": rss_retained_delta,
            "rss_measured": rss_measured,
            # RSS is process-wide. Retain the historical delta for compatibility
            # but publish the absolute samples needed to interpret a warm
            # worker whose allocator already retains substantial memory.
            "peak_memory_bytes": (
                int(rss_peak_delta) if rss_peak_delta is not None else 0
            ),
            "rss_scope": (
                "process_rss_sampled_10ms_after_admission_until_profile_finalize"
                if rss_measured else "unknown"
            ),
            "peak_memory_scope": (
                "process_rss_peak_delta_after_admission_until_profile_finalize"
                if self.rss_started is not None else "unknown"
            ),
        }


class _QueryExecutionMetrics:
    """Query-private physical plan facts and phase durations.

    The counters describe scan nodes built by IslandDB.  They deliberately do
    not pretend to be native-runtime observations: neither Polars nor every
    Arrow path exposes comparable post-pruning counters today.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._planned_files = 0
        self._planned_row_groups = 0
        self._planned_rows = 0
        self._planned_complete = True
        self._relations = 0
        self._phase_timings_ms: Dict[str, float] = {}
        self._execution_outcome = "stream_open"
        self._result_complete = False

    def add_plan(
        self,
        *,
        files: int,
        row_groups: int,
        rows: int,
        complete: bool = True,
    ) -> None:
        with self._lock:
            self._relations += 1
            self._planned_files += max(0, int(files))
            self._planned_row_groups += max(0, int(row_groups))
            self._planned_rows += max(0, int(rows))
            self._planned_complete = self._planned_complete and bool(complete)

    def add_phase(self, name: str, elapsed_ms: float) -> None:
        with self._lock:
            key = str(name)
            self._phase_timings_ms[key] = (
                self._phase_timings_ms.get(key, 0.0)
                + max(0.0, float(elapsed_ms))
            )

    def mark_complete(self) -> None:
        with self._lock:
            self._execution_outcome = "completed"
            self._result_complete = True

    def mark_closed_early(self) -> None:
        with self._lock:
            if self._execution_outcome == "stream_open":
                self._execution_outcome = "closed_early"

    def mark_failed(self) -> None:
        with self._lock:
            self._execution_outcome = "failed"
            self._result_complete = False

    def mark_cancelled(self) -> None:
        with self._lock:
            if self._execution_outcome not in {"failed", "timed_out"}:
                self._execution_outcome = "cancelled"
                self._result_complete = False

    def mark_timed_out(self) -> None:
        with self._lock:
            self._execution_outcome = "timed_out"
            self._result_complete = False

    def mark_facade_failed(self) -> None:
        with self._lock:
            # A materialized facade sees producer exceptions too. Preserve the
            # more specific native terminal state instead of relabelling a
            # timeout/cancellation/producer failure as a conversion failure.
            if self._execution_outcome not in {
                "failed", "timed_out", "cancelled",
            }:
                self._execution_outcome = "facade_failed"
                self._result_complete = False

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            complete = self._relations > 0 and self._planned_complete
            return {
                "planned_files": self._planned_files,
                "planned_row_groups": self._planned_row_groups,
                "planned_rows": self._planned_rows,
                "planned_complete": complete,
                "phase_timings_ms": dict(self._phase_timings_ms),
                "execution_outcome": self._execution_outcome,
                "result_complete": self._result_complete,
            }


class _QueryRangeMetrics:
    """Concurrency-safe counters owned by exactly one IslandDB query."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: Dict[str, int] = {}

    def merge(self, metrics: object) -> None:
        try:
            values = metrics.as_dict()
        except Exception:
            return
        with self._lock:
            for name, value in values.items():
                self._values[str(name)] = (
                    self._values.get(str(name), 0) + max(0, int(value))
                )

    def as_dict(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._values)


_GOVERNOR_LOCK = threading.Lock()
_GOVERNORS: Dict[Tuple[object, ...], ResourceGovernor] = {}
# Polars and Arrow pools are process-global, while governors are deliberately
# policy/spill-root scoped.  One cross-policy slot prevents two otherwise
# independent governors from concurrently admitting scans that can each use
# the complete native pool.
_ISLAND_EXECUTION_SLOT = threading.BoundedSemaphore(1)
_ARROW_POOL_LOCK = threading.Lock()
_RANGE_CACHE_REGISTRY_LOCK = threading.Lock()
_RANGE_CACHE_REGISTRY: "OrderedDict[Tuple[object, ...], object]" = OrderedDict()
_RANGE_CACHE_REGISTRY_MAX_ENTRIES = 16
_ROWID_PROOF_LOCK = threading.Lock()
_ROWID_PROOFS: "OrderedDict[Tuple[object, ...], bool]" = OrderedDict()
_ROWID_PROOF_MAX_ENTRIES = 4096

# Local Parquet objects are immutable once published, but constructing a native
# scan currently reopens every footer merely to re-prove the same physical
# schema.  Cache only the validated *description* of a scan -- paths, identities,
# schema and filesystem seals -- never decoded pages, frames, or query results.
# Both limits are hard process bounds and deliberately module-local so changing
# the public settings/Redis schema is not required for this optimization.
_LOCAL_SCAN_PLAN_MAX_ENTRIES = 64
_LOCAL_SCAN_PLAN_MAX_BYTES = 64 * 1024 * 1024
_LOCAL_SCAN_PLAN_LOCK = threading.Lock()

# On small, narrow local scans Polars' automatic strategy can choose a
# pre-filtered row-group pipeline whose mask/worker state costs more than the
# compressed pages themselves.  Column-parallel decoding avoids that transient
# state for a bounded middle-width projection.  Keep the gate deliberately
# narrow: broader scans and very narrow/wide projections retain Polars' adaptive
# strategy, where row-group parallelism can be materially faster.
_LOCAL_COLUMN_PARALLEL_MAX_SCAN_BYTES = 32 * 1024 * 1024
_LOCAL_COLUMN_PARALLEL_MIN_COLUMNS = 4
_LOCAL_COLUMN_PARALLEL_MAX_COLUMNS = 8


@dataclass(frozen=True)
class _LocalFileSeal:
    source_path: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True)
class _LocalScanPlan:
    local_paths: Tuple[str, ...]
    local_to_resource: Tuple[Tuple[str, str], ...]
    physical_schema: pa.Schema
    file_seals: Tuple[_LocalFileSeal, ...]
    row_group_count: int
    row_count: int
    weight_bytes: int


@dataclass
class _LocalScanPlanBuild:
    completed: threading.Event = field(default_factory=threading.Event)


_LOCAL_SCAN_PLANS: "OrderedDict[str, _LocalScanPlan]" = OrderedDict()
_LOCAL_SCAN_PLAN_BUILDS: Dict[str, _LocalScanPlanBuild] = {}
_LOCAL_SCAN_PLAN_BYTES = 0


def _trim_local_scan_plans_locked() -> None:
    """Enforce both cache bounds while ``_LOCAL_SCAN_PLAN_LOCK`` is held."""
    global _LOCAL_SCAN_PLAN_BYTES
    max_entries = max(0, int(_LOCAL_SCAN_PLAN_MAX_ENTRIES))
    max_bytes = max(0, int(_LOCAL_SCAN_PLAN_MAX_BYTES))
    while _LOCAL_SCAN_PLANS and (
        len(_LOCAL_SCAN_PLANS) > max_entries
        or _LOCAL_SCAN_PLAN_BYTES > max_bytes
    ):
        _key, plan = _LOCAL_SCAN_PLANS.popitem(last=False)
        _LOCAL_SCAN_PLAN_BYTES = max(
            0, _LOCAL_SCAN_PLAN_BYTES - int(plan.weight_bytes),
        )


def _clear_local_scan_plan_cache() -> None:
    """Test/connection-reset hook; active builders remain safe and bounded."""
    global _LOCAL_SCAN_PLAN_BYTES
    with _LOCAL_SCAN_PLAN_LOCK:
        _LOCAL_SCAN_PLANS.clear()
        _LOCAL_SCAN_PLAN_BYTES = 0


def _clear_range_cache_registry() -> None:
    """Test hook for the bounded process-local RangeCache registry."""
    with _RANGE_CACHE_REGISTRY_LOCK:
        _RANGE_CACHE_REGISTRY.clear()


def _local_file_seal(source_path: str) -> Optional[_LocalFileSeal]:
    """Return a cheap mutation seal for one local path, or ``None`` on change."""
    try:
        info = os.stat(source_path, follow_symlinks=True)
    except OSError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return _LocalFileSeal(
        source_path=source_path,
        device=int(info.st_dev),
        inode=int(info.st_ino),
        size=int(info.st_size),
        mtime_ns=int(info.st_mtime_ns),
        ctime_ns=int(info.st_ctime_ns),
    )


def _local_scan_plan_is_current(plan: _LocalScanPlan) -> bool:
    """Detect a broken immutability contract without reopening any footer."""
    for expected in plan.file_seals:
        current = _local_file_seal(expected.source_path)
        if current != expected:
            return False
    return True


def _local_object_metadata(seal: _LocalFileSeal) -> ObjectMetadata:
    """Translate one validated filesystem seal without another syscall."""
    return ObjectMetadata(
        size=int(seal.size),
        version=f"{seal.device}:{seal.inode}:{seal.ctime_ns}",
        last_modified_ns=int(seal.mtime_ns),
    )


def _snapshot_expected_object_metadata(
    snapshot: SuperSnapshot,
) -> Dict[str, ObjectMetadata]:
    """Convert only exact, survivor-bound snapshot object seals.

    This is deliberately defensive even though DataEstimator already parses
    the catalog: tests and integrations may construct ``SuperSnapshot``
    directly. A foreign key, malformed type, or size disagreement is ignored so
    RangeCache performs its normal provider stat instead.
    """
    keys = list(getattr(snapshot, "resource_keys", None) or [])
    sizes = list(getattr(snapshot, "resource_sizes", None) or [])
    seals = getattr(snapshot, "resource_object_seals", None)
    if not isinstance(seals, dict):
        return {}
    declared_sizes = (
        dict(zip(keys, sizes)) if sizes and len(sizes) == len(keys) else {}
    )
    expected: Dict[str, ObjectMetadata] = {}
    for key in keys:
        seal = seals.get(key)
        if not isinstance(seal, ResourceObjectSeal):
            continue
        declared = declared_sizes.get(key)
        if declared is not None and (
            not isinstance(declared, int)
            or isinstance(declared, bool)
            or declared <= 0
            or seal.size != declared
        ):
            continue
        metadata = ObjectMetadata(
            size=seal.size,
            version=seal.version,
            etag=seal.etag,
            last_modified_ns=seal.last_modified_ns,
            checksum_sha256=seal.checksum_sha256,
        )
        if metadata.identity_token():
            expected[str(key)] = metadata
    return expected


def _get_or_reserve_local_scan_plan(
    key: Optional[str],
) -> Tuple[Optional[_LocalScanPlan], Optional[_LocalScanPlanBuild]]:
    """Return a current hit or reserve one single-flight cold validation."""
    if not key:
        return None, None
    while True:
        wait_for: Optional[_LocalScanPlanBuild] = None
        with _LOCAL_SCAN_PLAN_LOCK:
            _trim_local_scan_plans_locked()
            plan = _LOCAL_SCAN_PLANS.get(key)
            if plan is not None:
                _LOCAL_SCAN_PLANS.move_to_end(key)
            else:
                wait_for = _LOCAL_SCAN_PLAN_BUILDS.get(key)
                if wait_for is None:
                    build = _LocalScanPlanBuild()
                    _LOCAL_SCAN_PLAN_BUILDS[key] = build
                    return None, build
        if plan is not None:
            # Thousands of stat calls are still two orders of magnitude cheaper
            # than reopening/converting thousands of Parquet schemas, and catch
            # deletion, replacement, symlink retargeting and same-size rewrites.
            if _local_scan_plan_is_current(plan):
                return plan, None
            with _LOCAL_SCAN_PLAN_LOCK:
                current = _LOCAL_SCAN_PLANS.get(key)
                if current is plan:
                    global _LOCAL_SCAN_PLAN_BYTES
                    _LOCAL_SCAN_PLANS.pop(key, None)
                    _LOCAL_SCAN_PLAN_BYTES = max(
                        0, _LOCAL_SCAN_PLAN_BYTES - int(plan.weight_bytes),
                    )
            continue
        # A different query is validating the same immutable reflection. Avoid
        # duplicate footer storms, then retry the cache under the lock. A failed
        # builder publishes nothing, so one waiter safely becomes the next owner.
        assert wait_for is not None
        wait_for.completed.wait()


def _finish_local_scan_plan_build(
    key: Optional[str],
    build: Optional[_LocalScanPlanBuild],
    plan: Optional[_LocalScanPlan],
) -> None:
    """Publish a fully validated plan atomically and wake all waiters."""
    if not key or build is None:
        return
    global _LOCAL_SCAN_PLAN_BYTES
    with _LOCAL_SCAN_PLAN_LOCK:
        owner = _LOCAL_SCAN_PLAN_BUILDS.get(key)
        if owner is build:
            if (
                plan is not None
                and _LOCAL_SCAN_PLAN_MAX_ENTRIES > 0
                and _LOCAL_SCAN_PLAN_MAX_BYTES > 0
                and plan.weight_bytes <= _LOCAL_SCAN_PLAN_MAX_BYTES
            ):
                old = _LOCAL_SCAN_PLANS.pop(key, None)
                if old is not None:
                    _LOCAL_SCAN_PLAN_BYTES = max(
                        0, _LOCAL_SCAN_PLAN_BYTES - int(old.weight_bytes),
                    )
                _LOCAL_SCAN_PLANS[key] = plan
                _LOCAL_SCAN_PLAN_BYTES += int(plan.weight_bytes)
                _trim_local_scan_plans_locked()
            _LOCAL_SCAN_PLAN_BUILDS.pop(key, None)
            build.completed.set()


class _IslandFileSystemHandler(pafs.FileSystemHandler):
    """Read-only Arrow filesystem spanning local files and sealed ranges."""

    def __init__(self, entries: Dict[str, Dict[str, object]]):
        self.entries = entries

    def get_type_name(self):
        return "islanddb"

    def normalize_path(self, path):
        return str(path)

    def get_file_info(self, paths):
        result = []
        for path in paths:
            entry = self.entries.get(str(path))
            result.append(
                pafs.FileInfo(
                    str(path),
                    type=(pafs.FileType.File if entry else pafs.FileType.NotFound),
                    size=(int(entry["size"]) if entry else None),
                )
            )
        return result

    def get_file_info_selector(self, selector):
        return []

    def open_input_file(self, path):
        entry = self.entries.get(str(path))
        if entry is None:
            raise FileNotFoundError(str(path))
        local = entry.get("local")
        if local:
            return pa.OSFile(str(local), "r")
        cache = entry.get("cache")
        if cache is None:
            raise FileNotFoundError(str(path))
        reader = cache.open(
            str(entry["raw_key"]),
            expected=entry.get("metadata"),
            metrics_sink=entry.get("metrics_sink"),
        )
        return pa.PythonFile(reader, mode="r")

    def open_input_stream(self, path):
        return self.open_input_file(path)

    @staticmethod
    def _readonly(*args, **kwargs):
        raise NotImplementedError("IslandDB Arrow filesystem is read-only")

    create_dir = _readonly
    delete_dir = _readonly
    delete_dir_contents = _readonly
    delete_root_dir_contents = _readonly
    delete_file = _readonly
    move = _readonly
    copy_file = _readonly
    open_output_stream = _readonly
    open_append_stream = _readonly


# Deliberately small.  Adding syntax here is a correctness change and should be
# accompanied by a DuckDB differential test, not merely a Polars unit test.
_ALLOWED_NODES = {
    exp.Select, exp.From, exp.Table, exp.TableAlias, exp.Column,
    exp.Identifier, exp.Alias, exp.Star,
    exp.Where, exp.Group, exp.Order, exp.Ordered, exp.Limit,
    exp.Join,
    exp.Literal, exp.Boolean, exp.Null,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
    exp.And, exp.Or, exp.Not, exp.Paren, exp.Between, exp.In, exp.Is,
    exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg,
    exp.Neg,
}

_NUMERIC_TYPE_NAMES = frozenset({
    "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32",
    "uint64", "byte", "short", "integer", "int", "long", "bigint",
    "hugeint", "float32", "float64", "float", "real", "double",
})
_OUTPUT_SAFE_TYPE_NAMES = _NUMERIC_TYPE_NAMES | frozenset({
    "bool", "boolean", "string", "utf8",
})
_BINARY_EXTREMA_TYPE_NAME = "binary"
_DATETIME_US_EXTREMA_TYPE_NAME = (
    "datetime(time_unit='us', time_zone=none)"
)


def _local_path(path: str) -> str:
    raw = str(path or "")
    if raw.startswith("file://"):
        parsed = urlparse(raw)
        if parsed.netloc not in ("", "localhost"):
            raise IslandUnsupportedError(
                f"IslandDB cannot scan non-local file URL {raw!r}"
            )
        raw = unquote(parsed.path)
    if "://" in raw:
        raise IslandUnsupportedError(
            "IslandDB requires a localised Reflection; remote path remained"
        )
    resolved = os.path.realpath(raw)
    if not os.path.isfile(resolved):
        raise FileNotFoundError(f"IslandDB local Parquet file is missing: {raw}")
    return resolved


def _normalized_type(type_name: object) -> str:
    value = str(type_name or "").strip().casefold()
    # Persisted Polars parameterised values (Datetime/Decimal/Duration) remain
    # visibly outside the numeric whitelist.
    return value


def _is_numeric_type(type_name: object) -> bool:
    return _normalized_type(type_name) in _NUMERIC_TYPE_NAMES


def _numeric_family(type_name: object) -> str:
    normalized = _normalized_type(type_name)
    if normalized in {"float32", "float64", "float", "real", "double"}:
        return "float"
    return "integer" if normalized in _NUMERIC_TYPE_NAMES else "other"


def _is_output_safe_type(type_name: object) -> bool:
    return _normalized_type(type_name) in _OUTPUT_SAFE_TYPE_NAMES


def _is_stream_output_safe_type(type_name: object) -> bool:
    """Return whether one raw column has an exact public Arrow contract.

    Binary and timezone-free microsecond timestamps are deliberately absent
    from ``_is_output_safe_type`` because DuckDB ``fetchdf`` and Polars pandas
    conversion expose different Python/pandas representations.  A one-shot
    Arrow stream has no such conversion: both values have an exact physical
    representation which the spill result normalises below.  This helper is
    intentionally *only* consulted for direct projected output; predicates,
    grouping, ordering, joins, and the materialised facade retain their
    narrower differential gates.
    """
    return bool(
        _is_output_safe_type(type_name)
        or _is_binary_extrema_type(type_name)
        or _is_datetime_us_extrema_type(type_name)
    )


def _is_binary_extrema_type(type_name: object) -> bool:
    """Return whether bytewise extrema parity is certified for this type."""
    return _normalized_type(type_name) == _BINARY_EXTREMA_TYPE_NAME


def _is_datetime_us_extrema_type(type_name: object) -> bool:
    """Accept only the exact timezone-free microsecond timestamp contract."""
    return _normalized_type(type_name) == _DATETIME_US_EXTREMA_TYPE_NAME


def _canonical_physical_type(type_name: object) -> str:
    """Canonicalise only type spellings known to describe one physical type."""
    normalized = _normalized_type(type_name)
    aliases = {
        "byte": "int8", "short": "int16", "integer": "int32",
        "int": "int32", "long": "int64", "bigint": "int64",
        "real": "float32", "double": "float64",
        "boolean": "bool", "utf8": "string",
    }
    return aliases.get(normalized, normalized)


def _fixed_size_binary_to_binary(
    column: pa.Array,
    *,
    offsets_cache: Optional[Dict[Tuple[int, int], pa.Buffer]] = None,
) -> pa.BinaryArray:
    """Expose bounded fixed-width bytes as Arrow Binary without copying data.

    Arrow Binary needs an int32 offset for every value, while
    FixedSizeBinary stores only one contiguous value buffer.  Output batches
    are already bounded by the query resource plan, so a small offsets buffer
    is sufficient; the potentially large value buffer is sliced and retained
    zero-copy.  A per-batch cache lets equal-width projected columns share the
    same immutable offsets buffer.
    """
    if not isinstance(column, pa.Array) or not pa.types.is_fixed_size_binary(
        column.type
    ):
        raise TypeError("expected a FixedSizeBinary Arrow array")

    length = len(column)
    byte_width = int(column.type.byte_width)
    value_bytes = length * byte_width
    int32_max = int(np.iinfo(np.int32).max)
    if length > int32_max or value_bytes > int32_max:
        raise OverflowError(
            "fixed-size binary output exceeds Arrow Binary's int32 bounds"
        )

    cache_key = (length, byte_width)
    offsets_buffer = (
        offsets_cache.get(cache_key) if offsets_cache is not None else None
    )
    expected_offsets_bytes = (length + 1) * np.dtype(np.int32).itemsize
    if offsets_buffer is None:
        offsets = np.arange(length + 1, dtype=np.int32)
        if byte_width != 1:
            offsets *= byte_width
        offsets.setflags(write=False)
        offsets_buffer = pa.py_buffer(offsets)
        if offsets_cache is not None:
            offsets_cache[cache_key] = offsets_buffer
    elif offsets_buffer.size != expected_offsets_bytes:
        # The cache is query-private and populated only above.  Keep this
        # assertion at the consumption boundary so future refactors cannot
        # silently construct an invalid public Arrow array.
        raise AssertionError("cached fixed-binary offsets have invalid size")

    validity_source, data_source = column.buffers()
    data_start = int(column.offset) * byte_width
    data_end = data_start + value_bytes
    if data_source is None:
        if value_bytes:
            raise ValueError("fixed-size binary array has no value buffer")
        data_buffer = pa.py_buffer(b"")
    else:
        if data_start < 0 or data_end > data_source.size:
            raise ValueError("fixed-size binary value buffer is truncated")
        data_buffer = data_source.slice(data_start, value_bytes)

    null_count = int(column.null_count)
    validity_buffer = None
    if null_count:
        if validity_source is None:
            raise ValueError("fixed-size binary null bitmap is missing")
        # Array.from_buffers below has offset zero.  Reuse a byte-aligned
        # source bitmap when possible; otherwise Arrow's native is_valid
        # kernel cheaply rebases only the one-bit-per-row validity data.
        bit_offset = int(column.offset)
        bitmap_bytes = (length + 7) // 8
        if bit_offset % 8 == 0:
            bitmap_start = bit_offset // 8
            if bitmap_start + bitmap_bytes > validity_source.size:
                raise ValueError("fixed-size binary null bitmap is truncated")
            validity_buffer = validity_source.slice(
                bitmap_start, bitmap_bytes,
            )
        else:
            validity_buffer = column.is_valid().buffers()[1]
            if validity_buffer is None or validity_buffer.size < bitmap_bytes:
                raise ValueError("could not rebase fixed-size binary validity")

    return pa.Array.from_buffers(
        pa.binary(),
        length,
        [validity_buffer, offsets_buffer, data_buffer],
        null_count=null_count,
    )


class IslandDB:
    """Native lazy-Parquet executor for IslandDB's conservative SQL subset."""

    def __init__(
        self,
        storage: Optional[object] = None,
        *,
        organization: str = "",
        range_cache: Optional[object] = None,
    ):
        self.storage = storage
        self.organization = str(organization or "")
        try:
            storage_namespace = (
                storage.cache_namespace() if storage is not None else {"provider": "none"}
            )
            encoded_namespace = json.dumps(
                storage_namespace,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        except Exception:
            encoded_namespace = (
                f"{type(storage).__module__}.{type(storage).__qualname__}:"
                f"{id(storage)}"
            ).encode("utf-8")
        self._proof_namespace = hashlib.sha256(encoded_namespace).hexdigest()
        # Cache identities must include organization *and* authorization scope;
        # raw object keys alone can collide across buckets or principals. Reuse
        # FileCache's hardened namespace calculation without touching disk.
        try:
            from supertable.engine.file_cache import FileCache
            cache_identity = FileCache(
                storage, self.organization, max_bytes=0, workers=1,
            )
            self._artifact_cache_namespace = (
                f"{cache_identity._organization_hash}:"
                f"{cache_identity._storage_hash}"
            )
        except Exception:
            self._artifact_cache_namespace = hashlib.sha256(
                f"{self.organization}\0{self._proof_namespace}\0{id(storage)}".encode()
            ).hexdigest()
        self.range_cache = range_cache
        self.last_profile = IslandProfile()
        self._resources = self._detect_resources()
        self._policy = self._resource_policy()
        # Arrow owns process-global CPU/I/O pools. Bound them to the container's
        # cgroup/affinity capacity; plans account for their full possible widths
        # and the process slot serializes scans across differing policies.
        with _ARROW_POOL_LOCK:
            pa.set_cpu_count(max(1, self._resources.cpu_count))
            pa.set_io_thread_count(max(
                1,
                min(
                    self._resources.cpu_count * 2,
                    self._policy.max_io_workers,
                ),
            ))
        self._spill_root = Path(
            settings.SUPERTABLE_ISLAND_SPILL_DIR
            or os.path.join(get_app_home(), "island_spill")
        )
        self._planner = ResourcePlanner(
            self._resources,
            spill_root=self._spill_root,
            policy=self._policy,
        )
        # CPU quota, cgroup memory limit, and current memory availability are
        # live resource signals. They must refresh one process-wide admission
        # domain, not create a second governor that can reserve concurrently
        # with queries admitted under the previous limits.
        governor_key = (
            str(self._spill_root.resolve()),
            self._policy,
        )
        with _GOVERNOR_LOCK:
            self._governor = _GOVERNORS.get(governor_key)
            if self._governor is None:
                self._governor = ResourceGovernor(
                    self._resources,
                    spill_root=self._spill_root,
                    policy=self._policy,
                )
                _GOVERNORS[governor_key] = self._governor
            else:
                # ``memory_available`` is a live pressure signal and is not
                # part of the stable sharing key.  Never retain the first
                # process sample as the governor's capacity indefinitely.
                self._governor.refresh_resources(self._resources)

    @staticmethod
    def _detect_resources() -> ContainerResources:
        resources = ContainerResources.detect()
        cpu_limit = int(settings.SUPERTABLE_ISLAND_CPU_MAX or 0)
        memory_limit = int(settings.SUPERTABLE_ISLAND_MAX_MEMORY_BYTES or 0)
        if cpu_limit > 0:
            resources = replace(
                resources,
                cpu_count=max(1, min(resources.cpu_count, cpu_limit)),
                cpu_capacity=min(resources.cpu_capacity, float(cpu_limit)),
            )
        if memory_limit > 0:
            resources = replace(
                resources,
                memory_limit_bytes=min(
                    resources.memory_limit_bytes, memory_limit,
                ),
                memory_available_bytes=min(
                    resources.memory_available_bytes, memory_limit,
                ),
            )
        return resources

    @staticmethod
    def _resource_policy() -> ResourcePolicy:
        return ResourcePolicy(
            query_memory_fraction=min(
                1.0, max(0.05, settings.SUPERTABLE_ISLAND_MEMORY_FRACTION),
            ),
            global_memory_fraction=min(
                1.0,
                max(0.05, settings.SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION),
            ),
            max_query_memory_bytes=max(
                0, int(settings.SUPERTABLE_ISLAND_MAX_MEMORY_BYTES or 0),
            ),
            max_result_memory_bytes=max(
                1, int(settings.SUPERTABLE_ISLAND_MAX_RESULT_BYTES),
            ),
            max_spill_bytes=(
                max(0, int(settings.SUPERTABLE_ISLAND_SPILL_MAX_BYTES))
                if settings.SUPERTABLE_ISLAND_SPILL_ENABLED else 0
            ),
            min_spill_free_bytes=max(
                0, int(settings.SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES),
            ),
            max_io_workers=max(
                1, int(settings.SUPERTABLE_ISLAND_IO_WORKERS_MAX),
            ),
        )

    def _get_range_cache(self):
        if self.range_cache is False:
            return None
        if self.range_cache is None:
            if (
                not settings.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED
                or self.storage is None
            ):
                self.range_cache = False
                return None
            from supertable.engine.range_cache import RangeCache
            configured_root = (
                settings.SUPERTABLE_ISLAND_RANGE_CACHE_DIR
                or settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
                or os.path.join(get_app_home(), "duckdb_cache")
            )
            resolved_root = os.path.realpath(os.path.abspath(os.path.expanduser(
                str(configured_root),
            )))
            registry_key = (
                self.organization,
                self._artifact_cache_namespace,
                resolved_root,
                max(0, int(settings.SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES)),
                max(0.0, float(settings.SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC)),
                True,  # RangeCache.allow_cache_bypass
            )
            with _RANGE_CACHE_REGISTRY_LOCK:
                shared = _RANGE_CACHE_REGISTRY.get(registry_key)
                if shared is not None:
                    _RANGE_CACHE_REGISTRY.move_to_end(registry_key)
                    self.range_cache = shared
                    return shared
            candidate = RangeCache(
                self.storage,
                self.organization,
                root=(settings.SUPERTABLE_ISLAND_RANGE_CACHE_DIR or None),
                max_bytes=settings.SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES,
                ttl=settings.SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC,
            )
            with _RANGE_CACHE_REGISTRY_LOCK:
                shared = _RANGE_CACHE_REGISTRY.get(registry_key)
                if shared is None:
                    shared = candidate
                    _RANGE_CACHE_REGISTRY[registry_key] = shared
                    while (
                        len(_RANGE_CACHE_REGISTRY)
                        > max(0, int(_RANGE_CACHE_REGISTRY_MAX_ENTRIES))
                    ):
                        _RANGE_CACHE_REGISTRY.popitem(last=False)
                else:
                    _RANGE_CACHE_REGISTRY.move_to_end(registry_key)
                self.range_cache = shared
        return self.range_cache

    # ------------------------------------------------------------------
    # Capability boundary
    # ------------------------------------------------------------------

    @staticmethod
    def _query_root(parser) -> exp.Expression:
        """Return SQLParser's canonical AST without mutating it.

        ``SQLParser`` has already parsed and security-checked the query before
        engine selection.  Reusing that tree removes several complete sqlglot
        parses from every IslandDB query.  Lightweight/custom parser objects
        retain the old parse fallback.
        """
        parsed = getattr(parser, "_parsed", None)
        if isinstance(parsed, exp.Expression):
            return parsed
        return sqlglot.parse_one(parser.original_query, read="duckdb")

    @staticmethod
    def _snapshots(reflection: Reflection) -> Dict[Tuple[str, str], SuperSnapshot]:
        return {
            (s.super_name.casefold(), s.simple_name.casefold()): s
            for s in reflection.supers
        }

    @staticmethod
    def _candidate_result_upper_bound(
        root: exp.Select,
        parser,
        reflection: Reflection,
    ) -> Optional[int]:
        """Return a sealed row upper bound for the top-level join chain.

        Inner/CROSS joins are bounded by the Cartesian product.  Outer joins
        additionally retain the rows on their preserved side(s), including
        when the other input is empty.  Returning ``None`` on any
        shape/metadata mismatch keeps routing fail-closed.
        """
        definitions = list(parser.get_table_tuples())
        joins = list(root.args.get("joins") or ())
        if not definitions or len(definitions) != len(joins) + 1:
            return None

        snapshots = IslandDB._snapshots(reflection)
        occurrence_rows: List[int] = []
        for definition in definitions:
            snapshot = snapshots.get((
                definition.super_name.casefold(),
                definition.simple_name.casefold(),
            ))
            if snapshot is None or not bool(getattr(
                snapshot, "candidate_rows_complete", False,
            )):
                return None
            try:
                occurrence_rows.append(max(
                    0, int(getattr(snapshot, "candidate_rows", 0) or 0),
                ))
            except (TypeError, ValueError, OverflowError):
                return None

        upper_bound = occurrence_rows[0]
        for join, right_rows in zip(joins, occurrence_rows[1:]):
            side = str(join.args.get("side") or "").upper()
            if side == "LEFT":
                upper_bound *= max(1, right_rows)
            elif side == "RIGHT":
                upper_bound = max(1, upper_bound) * right_rows
            elif side == "FULL":
                # A FULL join may emit the Cartesian matches plus unmatched
                # rows from either input.  The sum is deliberately loose but
                # remains a sound resource upper bound for every zero-side case.
                upper_bound = (
                    upper_bound * right_rows + upper_bound + right_rows
                )
            else:
                upper_bound *= right_rows
        return upper_bound

    @staticmethod
    def _table_maps(parser, reflection: Reflection):
        snapshots = IslandDB._snapshots(reflection)
        aliases: Dict[str, SuperSnapshot] = {}
        for td in parser.get_table_tuples():
            snap = snapshots.get((td.super_name.casefold(), td.simple_name.casefold()))
            if snap is not None:
                aliases[td.alias.casefold()] = snap
                aliases.setdefault(td.simple_name.casefold(), snap)
        return aliases

    @staticmethod
    def _required_columns_by_snapshot(
        parser,
        reflection: Reflection,
    ) -> Dict[int, Optional[set[str]]]:
        """Return exact physical columns needed by every table snapshot.

        ``None`` means a star occurrence requires all public columns.  The SQL
        parser already includes predicate/join/group/order columns in each
        ``TableDefinition``; merging occurrence sets here lets the footer
        planner bound only the buffers the query can actually decode instead
        of rejecting a numeric projection merely because an unrelated string
        payload column exists in the same Parquet file.
        """
        snapshots = IslandDB._snapshots(reflection)
        required: Dict[int, Optional[set[str]]] = {}
        for definition in parser.get_table_tuples():
            snapshot = snapshots.get((
                definition.super_name.casefold(),
                definition.simple_name.casefold(),
            ))
            if snapshot is None:
                continue
            marker = id(snapshot)
            columns = list(definition.columns or [])
            if not columns:
                required[marker] = None
                continue
            if marker not in required:
                required[marker] = set()
            if required[marker] is not None:
                required[marker].update(str(column).casefold() for column in columns)
        return required

    @staticmethod
    def _local_scan_parallel_strategy(
        reflection: Reflection,
        required_columns: Optional[set[str]],
    ) -> str:
        """Choose a memory-lean scan strategy only inside a measured safe gate.

        This is a performance hint, never a pruning or correctness decision.
        An incomplete byte estimate, a star projection, or a shape outside the
        benchmark-backed middle-width window therefore leaves Polars on
        ``auto``.  The strategy is applied only by the local native scanner;
        remote Arrow fragments keep their exact sealed row-group ranges.
        """
        # A deletion vector adds source-identity/rowid columns, an eager DV
        # frame, a validation pass and an anti-join hash table.  The narrow
        # column-parallel benchmark gate did not cover that materially different
        # pipeline, so it must not override Polars' adaptive strategy.  Treat a
        # malformed optional mapping conservatively too; correctness validation
        # later still decides whether execution may proceed.
        try:
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            if not isinstance(tombstone_views, dict) or any(
                getattr(tombstone, "tombstone_path", None)
                for tombstone in tombstone_views.values()
            ):
                return "auto"
        except Exception:
            return "auto"
        if required_columns is None:
            return "auto"
        scan_bytes = max(
            0,
            int(
                getattr(reflection, "row_group_scan_bytes", 0)
                or getattr(reflection, "reflection_bytes", 0)
                or 0
            ),
        )
        if not bool(
            getattr(reflection, "row_group_scan_bytes_complete", False)
        ):
            return "auto"
        if not 0 < scan_bytes <= _LOCAL_COLUMN_PARALLEL_MAX_SCAN_BYTES:
            return "auto"
        column_count = len(required_columns)
        if not (
            _LOCAL_COLUMN_PARALLEL_MIN_COLUMNS
            <= column_count
            <= _LOCAL_COLUMN_PARALLEL_MAX_COLUMNS
        ):
            return "auto"
        return "columns"

    @staticmethod
    def _resolve_column_type(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[str]:
        wanted = column.name.casefold()
        if column.table:
            snap = aliases.get(column.table.casefold())
            if snap is None:
                return None
            matches = [
                value for name, value in (snap.column_types or {}).items()
                if str(name).casefold() == wanted
            ]
            return str(matches[0]) if len(matches) == 1 else None

        matches: List[str] = []
        seen = set()
        for snap in aliases.values():
            marker = id(snap)
            if marker in seen:
                continue
            seen.add(marker)
            for name, value in (snap.column_types or {}).items():
                if str(name).casefold() == wanted:
                    matches.append(str(value))
                    break
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _resolve_column_snapshot(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[SuperSnapshot]:
        """Resolve one already-validated column to exactly one snapshot."""
        wanted = column.name.casefold()
        if column.table:
            snapshot = aliases.get(column.table.casefold())
            if snapshot is None:
                return None
            matches = [
                name for name in (snapshot.column_types or {})
                if str(name).casefold() == wanted
            ]
            return snapshot if len(matches) == 1 else None

        matches: List[SuperSnapshot] = []
        seen: set[int] = set()
        for snapshot in aliases.values():
            if id(snapshot) in seen:
                continue
            seen.add(id(snapshot))
            names = [
                name for name in (snapshot.column_types or {})
                if str(name).casefold() == wanted
            ]
            if len(names) == 1:
                matches.append(snapshot)
        return matches[0] if len(matches) == 1 else None

    @classmethod
    def _sealed_integer_column_domain(
        cls,
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[IntegerDomainBound]:
        """Resolve one direct integer column to complete sealed extrema.

        The estimator publishes these bounds only after validating every
        selected row group's snapshot-pinned statistics.  Consumers still use
        them solely for conservative resource/partition planning: missing or
        ambiguous evidence disables the optimization, and execution never
        prunes rows from this metadata.
        """
        if _numeric_family(cls._resolve_column_type(column, aliases)) != "integer":
            return None
        snapshot = cls._resolve_column_snapshot(column, aliases)
        if snapshot is None or not bool(
            getattr(snapshot, "candidate_rows_complete", False)
        ):
            return None
        try:
            domains = getattr(snapshot, "integer_domain_bounds", None)
            if not isinstance(domains, dict):
                return None
            matches = [
                bound for name, bound in domains.items()
                if isinstance(name, str)
                and name.casefold() == column.name.casefold()
            ]
            if len(matches) != 1 or not isinstance(matches[0], IntegerDomainBound):
                return None
            return matches[0]
        except Exception:
            return None

    @classmethod
    def _sealed_integer_group_cardinality(
        cls,
        root: exp.Select,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[int]:
        """Return a complete upper bound for one direct integer GROUP key.

        Multi-key/expression groups and every missing or malformed proof retain
        the general external plan.  The bound is capped by the exact selected
        physical row count; tombstones/RBAC/WHERE can only reduce it.
        """
        group = root.args.get("group")
        if not isinstance(group, exp.Group) or len(group.expressions) != 1:
            return None
        column = group.expressions[0]
        if not isinstance(column, exp.Column):
            return None
        bound = cls._sealed_integer_column_domain(column, aliases)
        if bound is None:
            return None
        snapshot = cls._resolve_column_snapshot(column, aliases)
        if snapshot is None:
            return None
        try:
            rows = getattr(snapshot, "candidate_rows", None)
            if (
                not isinstance(rows, int)
                or isinstance(rows, bool)
                or rows < 0
            ):
                return None
            cardinality = bound.cardinality_upper_bound
            if cardinality < 0:
                return None
            return min(cardinality, rows)
        except Exception:
            return None

    @staticmethod
    def _binary_value_bound(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[int]:
        """Return one exact, snapshot-wide Binary value-width seal.

        Bounds are admission metadata, not semantic authority. Missing,
        differently-cased, Boolean, negative, or duplicate keys are unknown and
        therefore make the resource estimate incomplete.
        """
        snapshot = IslandDB._resolve_column_snapshot(column, aliases)
        if snapshot is None:
            return None
        return IslandDB._snapshot_binary_value_bound(snapshot, column.name)

    @staticmethod
    def _snapshot_binary_value_bound(
        snapshot: SuperSnapshot,
        column_name: str,
    ) -> Optional[int]:
        """Return the exact-case sealed Binary width on one snapshot."""
        raw = getattr(snapshot, "column_max_value_bytes", None)
        if not isinstance(raw, dict):
            return None
        matches = [
            (name, value) for name, value in raw.items()
            if str(name).casefold() == column_name.casefold()
        ]
        if len(matches) != 1 or str(matches[0][0]) != column_name:
            return None
        value = matches[0][1]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
        ):
            return None
        return int(value)

    @classmethod
    def _projected_result_row_width(
        cls,
        root: exp.Select,
        aliases: Dict[str, SuperSnapshot],
        reflection: Reflection,
        parser,
    ) -> Optional[int]:
        """Bound one logical Arrow result row from snapshot-sealed widths.

        The historical 24-byte field charge remains intentionally
        conservative for the fixed-width native contract.  Arrow Binary is
        variable-width: its write-time maximum value seal is authoritative,
        and nine extra bytes cover two int32 offsets plus worst-case validity
        overhead even for a one-row batch.  Missing or ambiguous Binary seals
        make admission incomplete rather than falling back to 24 bytes.
        """
        system_names = {ROWID_COL.casefold(), TIMESTAMP_COL.casefold()}

        def snapshot_fields(snapshot: SuperSnapshot) -> Optional[int]:
            total = 0
            for name, type_name in (snapshot.column_types or {}).items():
                column_name = str(name)
                if column_name.casefold() in system_names:
                    continue
                if _is_binary_extrema_type(type_name):
                    bound = cls._snapshot_binary_value_bound(
                        snapshot, column_name,
                    )
                    if bound is None:
                        return None
                    total += max(24, bound + 9)
                else:
                    total += 24
            return total

        width = 0
        for selected in root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            if isinstance(core, exp.Star):
                snapshots = cls._snapshots(reflection)
                # SQL star expands relation occurrences, not unique physical
                # snapshots. A self-join therefore contributes the same sealed
                # schema once per alias even though DataEstimator correctly
                # pins only one immutable SuperSnapshot.
                for definition in parser.get_table_tuples():
                    snapshot = snapshots.get((
                        definition.super_name.casefold(),
                        definition.simple_name.casefold(),
                    ))
                    if snapshot is None:
                        return None
                    star_width = snapshot_fields(snapshot)
                    if star_width is None:
                        return None
                    width += star_width
            elif isinstance(core, exp.Column) and core.is_star:
                snapshot = aliases.get(core.table.casefold()) if core.table else None
                if snapshot is None:
                    return None
                star_width = snapshot_fields(snapshot)
                if star_width is None:
                    return None
                width += star_width
            elif isinstance(core, exp.Column) and _is_binary_extrema_type(
                cls._resolve_column_type(core, aliases)
            ):
                bound = cls._binary_value_bound(core, aliases)
                if bound is None:
                    return None
                width += max(24, bound + 9)
            else:
                width += 24
        return max(24, width)

    @staticmethod
    def _column_spelling_is_exact(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> bool:
        """Return whether Polars can bind the identifier without case folding.

        DuckDB identifiers are case-insensitive while Polars SQL column lookup
        is case-sensitive.  Falling through on a differently-cased spelling is
        an availability bug in explicit mode and could become a wrong binding
        after schema evolution, so native execution requires the pinned schema
        spelling exactly.
        """
        if column.is_star:
            return True
        wanted = column.name.casefold()
        if column.table:
            snap = aliases.get(column.table.casefold())
            if snap is None:
                return False
            matches = [
                str(name) for name in (snap.column_types or {})
                if str(name).casefold() == wanted
            ]
            return len(matches) == 1 and matches[0] == column.name

        matches: List[str] = []
        seen = set()
        for snap in aliases.values():
            marker = id(snap)
            if marker in seen:
                continue
            seen.add(marker)
            matches.extend(
                str(name) for name in (snap.column_types or {})
                if str(name).casefold() == wanted
            )
        return len(matches) == 1 and matches[0] == column.name

    def can_execute(
        self,
        reflection: Reflection,
        parser,
        *,
        streaming_result: bool = False,
    ) -> IslandCapability:
        reasons: List[str] = []
        if getattr(reflection, "rbac_views", None):
            reasons.append("RBAC views are not yet native in IslandDB")
        if any(not s.files for s in reflection.supers):
            reasons.append("typed empty snapshots use DuckDB until native type parity is complete")

        try:
            root = self._query_root(parser)
        except Exception as exc:
            return IslandCapability(False, (f"SQL parse failed: {exc}",))
        if not isinstance(root, exp.Select):
            reasons.append("only one top-level SELECT is native")

        unsupported = sorted({
            type(node).__name__ for node in root.walk()
            if type(node) not in _ALLOWED_NODES
        })
        if unsupported:
            reasons.append("unsupported SQL nodes: " + ", ".join(unsupported))

        for join in root.find_all(exp.Join):
            side = str(join.args.get("side") or "").upper()
            kind = str(join.args.get("kind") or "").upper()
            method = str(join.args.get("method") or "").upper()
            if (
                side not in ("", "LEFT")
                or kind not in ("", "INNER", "CROSS")
                or method
                or join.args.get("using")
            ):
                reasons.append(
                    f"join form {method or side or kind or 'USING'} is not native"
                )

        # ``query_sql`` appends a response-safety LIMIT to every SELECT.  It is
        # harmless when sealed candidate-row counts prove that it cannot
        # truncate the relation.  A genuinely truncating LIMIT remains outside
        # the native subset: even ORDER BY does not identify a deterministic
        # DuckDB-equivalent subset when the final key contains ties.
        limit = root.args.get("limit")
        if limit is not None:
            redundant_limit = False
            try:
                expression = limit.expression
                limit_value = int(expression.this)
                if (
                    isinstance(expression, exp.Literal)
                    and not expression.is_string
                    and str(limit_value) == str(expression.this)
                    and limit_value >= 0
                    and root.args.get("offset") is None
                ):
                    upper_bound = self._candidate_result_upper_bound(
                        root, parser, reflection,
                    )
                    if upper_bound is not None:
                        scalar = bool(root.expressions) and all(
                            next(
                                selected.find_all(
                                    exp.Count, exp.Sum, exp.Min, exp.Max,
                                ),
                                None,
                            ) is not None
                            for selected in root.expressions
                        ) and root.args.get("group") is None
                        redundant_limit = limit_value >= (
                            1 if scalar else upper_bound
                        )
            except Exception:
                redundant_limit = False
            if not redundant_limit:
                reasons.append("LIMIT/OFFSET needs a proven unique total order")

        aliases = self._table_maps(parser, reflection)
        declared_aliases = {
            definition.alias.casefold(): definition.alias
            for definition in parser.get_table_tuples()
        }
        # Output naming is per SQL table occurrence, not per physical snapshot.
        # Two aliases of the same snapshot still produce duplicate columns.
        table_count = len(parser.get_table_tuples())

        def is_proven_boolean(node: object) -> bool:
            if not isinstance(node, exp.Expression):
                return False
            if isinstance(node, exp.Paren):
                return is_proven_boolean(node.this)
            if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
                return True
            if isinstance(node, (exp.Between, exp.In, exp.Boolean)):
                return True
            if isinstance(node, (exp.And, exp.Or)):
                return is_proven_boolean(node.this) and is_proven_boolean(
                    node.expression
                )
            if isinstance(node, exp.Not):
                return is_proven_boolean(node.this)
            return False

        for column in root.find_all(exp.Column):
            if column.table:
                declared = declared_aliases.get(column.table.casefold())
                if declared is None or declared != column.table:
                    reasons.append(
                        f"table qualifier {column.table!r} does not exactly match "
                        "its declared alias"
                    )
            if not self._column_spelling_is_exact(column, aliases):
                reasons.append(
                    f"column spelling {column.sql()} does not exactly match "
                    "the pinned Parquet schema"
                )

        where = root.args.get("where")
        if isinstance(where, exp.Where) and not is_proven_boolean(where.this):
            reasons.append("WHERE expression has unproven Boolean coercion")
        for join in root.find_all(exp.Join):
            on = join.args.get("on")
            if isinstance(on, exp.Expression) and not is_proven_boolean(on):
                reasons.append("JOIN condition has unproven Boolean coercion")
        for logical in root.find_all(exp.And, exp.Or, exp.Not):
            if not is_proven_boolean(logical):
                reasons.append(
                    f"logical expression {logical.sql()} has unproven Boolean coercion"
                )
        if next(root.find_all(exp.Is), None) is not None:
            reasons.append("IS TRUE/FALSE/NULL semantics are not yet native")

        # DuckDB fetchdf and Polars-to-pandas intentionally differ for several
        # rich physical types (for example DECIMAL is float64 vs Decimal
        # objects). Raw projection is native only for the closed set whose
        # pandas representation has differential coverage. Aggregate outputs
        # are normalized separately below.
        unique_snapshots = tuple(
            {id(snap): snap for snap in aliases.values()}.values()
        )
        output_names: set[str] = set()
        for selected in root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            if not isinstance(selected, exp.Alias) and not isinstance(
                core, (exp.Column, exp.Star)
            ):
                reasons.append(
                    f"computed projection {selected.sql()} requires an explicit alias"
                )
            if isinstance(core, (exp.Literal, exp.Boolean, exp.Null)):
                reasons.append(
                    f"constant projection {selected.sql()} has unproven output typing"
                )
            output_name = selected.alias_or_name
            if output_name and not isinstance(core, exp.Star) and not (
                isinstance(core, exp.Column) and core.is_star
            ):
                folded_output = str(output_name).casefold()
                if folded_output in output_names:
                    reasons.append(
                        f"duplicate output name {output_name!r} needs explicit aliases"
                    )
                output_names.add(folded_output)
            if isinstance(core, exp.Star):
                if table_count > 1:
                    reasons.append("joined SELECT * output naming is not native")
                for snap in unique_snapshots:
                    for name, type_name in (snap.column_types or {}).items():
                        if str(name).casefold() in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }:
                            continue
                        if not _is_output_safe_type(type_name):
                            reasons.append(
                                f"projected column {name} type {type_name} "
                                "has unproven pandas parity"
                            )
            elif isinstance(core, exp.Column) and core.is_star:
                if table_count > 1:
                    reasons.append("joined table-star output naming is not native")
                snap = aliases.get(core.table.casefold()) if core.table else None
                if snap is None:
                    reasons.append(f"cannot resolve projected star {core.sql()}")
                else:
                    for name, type_name in (snap.column_types or {}).items():
                        if str(name).casefold() in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }:
                            continue
                        if not _is_output_safe_type(type_name):
                            reasons.append(
                                f"projected column {name} type {type_name} "
                                "has unproven pandas parity"
                            )
            elif isinstance(core, exp.Column):
                type_name = self._resolve_column_type(core, aliases)
                if type_name is None:
                    reasons.append(f"cannot prove type of projected column {core.sql()}")
                elif not (
                    _is_output_safe_type(type_name)
                    or (
                        streaming_result
                        and _is_stream_output_safe_type(type_name)
                    )
                ):
                    reasons.append(
                        f"projected column {core.sql()} type {type_name} "
                        + (
                            "has unproven Arrow-stream parity"
                            if streaming_result
                            else "has unproven pandas parity"
                        )
                    )

        semantic_roots: List[exp.Expression] = []
        for key in ("where", "group", "order"):
            value = root.args.get(key)
            if isinstance(value, exp.Expression):
                semantic_roots.append(value)
        for join in root.find_all(exp.Join):
            on = join.args.get("on")
            if isinstance(on, exp.Expression):
                semantic_roots.append(on)
        # MIN/MAX have one additional, tightly fenced semantic lane below:
        # direct Binary values use unsigned lexicographic byte order and exact
        # Datetime(us, no timezone) values use integer microsecond order.  Keep
        # every other operator on the numeric-only path here.
        semantic_roots.extend(list(root.find_all(exp.Sum, exp.Avg)))

        dangerous_columns: List[exp.Column] = []
        for node in semantic_roots:
            dangerous_columns.extend(node.find_all(exp.Column))
        for column in dangerous_columns:
            type_name = self._resolve_column_type(column, aliases)
            if type_name is None:
                reasons.append(f"cannot prove type of semantic column {column.sql()}")
            elif not _is_numeric_type(type_name):
                reasons.append(
                    f"column {column.sql()} type {type_name} has unproven DuckDB semantics"
                )

        # Cross-type coercion is an engine semantic, not a Parquet semantic.
        # In particular BIGINT/DOUBLE can compare equal in DuckDB after lossy
        # rounding while Polars retains a different expression type.  Native
        # IslandDB accepts only same stored types for column-column comparison,
        # and literals from the same integer/float family as their column.
        comparison_types = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)

        def check_column_literal(column, literal) -> None:
            if not isinstance(column, exp.Column) or not isinstance(literal, exp.Literal):
                return
            column_type = self._resolve_column_type(column, aliases)
            if column_type is not None and literal.is_number:
                literal_family = "integer" if literal.is_int else "float"
                if _numeric_family(column_type) != literal_family:
                    reasons.append(
                        f"mixed literal comparison for {column.sql()} is not native"
                    )

        for comparison in root.find_all(*comparison_types):
            left = comparison.this
            right = comparison.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_type = self._resolve_column_type(left, aliases)
                right_type = self._resolve_column_type(right, aliases)
                if not _is_numeric_type(left_type) or not _is_numeric_type(right_type):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                if (
                    left_type is not None and right_type is not None
                    and _normalized_type(left_type) != _normalized_type(right_type)
                ):
                    reasons.append(
                        f"mixed comparison types {left_type}/{right_type} are not native"
                    )
            elif isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                if not _is_numeric_type(self._resolve_column_type(left, aliases)):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                check_column_literal(left, right)
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                if not _is_numeric_type(self._resolve_column_type(right, aliases)):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                check_column_literal(right, left)
            elif isinstance(left, exp.Column) or isinstance(right, exp.Column):
                reasons.append(
                    f"comparison {comparison.sql()} has an unproven operand"
                )
        for between in root.find_all(exp.Between):
            low = between.args.get("low")
            high = between.args.get("high")
            if not (
                isinstance(between.this, exp.Column)
                and isinstance(low, exp.Literal) and low.is_number
                and isinstance(high, exp.Literal) and high.is_number
            ):
                reasons.append(
                    f"BETWEEN expression {between.sql()} has mixed/unproven types"
                )
            check_column_literal(between.this, low)
            check_column_literal(between.this, high)
        for membership in root.find_all(exp.In):
            if not (
                isinstance(membership.this, exp.Column)
                and membership.expressions
                and all(
                    isinstance(value, exp.Literal) and value.is_number
                    for value in membership.expressions
                )
            ):
                reasons.append(
                    f"IN expression {membership.sql()} has mixed/unproven types"
                )
            for value in membership.expressions:
                check_column_literal(membership.this, value)

        # DuckDB connections use NOCASE by contract.  Polars string comparison
        # is binary, so even an unqualified string literal makes the predicate
        # ineligible until normalized footer/data lanes exist.
        for literal in root.find_all(exp.Literal):
            if literal.is_string:
                reasons.append("string predicate/ordering semantics differ from DuckDB NOCASE")
                break

        if next(root.find_all(exp.Neg), None) is not None:
            reasons.append("signed expression semantics are not yet native")
        for ordered in root.find_all(exp.Ordered):
            if ordered.args.get("desc") or ordered.args.get("nulls_first"):
                reasons.append(
                    "DESC/NULLS FIRST ordering differs from DuckDB NULL placement"
                )

        # Floating reductions are order-sensitive and the two vector engines
        # are free to reduce row groups in a different order. Integer AVG also
        # enters an engine-specific floating accumulator. Keep exact integer
        # SUM (widened to Int128 below), COUNT, MIN and MAX native for now.
        if next(root.find_all(exp.Avg), None) is not None:
            reasons.append("AVG reduction parity is not yet proven")
        aggregates = tuple(root.find_all(exp.Count, exp.Sum, exp.Min, exp.Max))
        if root.args.get("group") is not None and not aggregates:
            reasons.append("GROUP BY without an aggregate uses DuckDB's distinct path")
        has_grouped_reduction = root.args.get("group") is not None
        has_joined_reduction = next(root.find_all(exp.Join), None) is not None
        for aggregate in aggregates:
            argument = aggregate.this
            is_count_star = isinstance(aggregate, exp.Count) and isinstance(
                argument, exp.Star
            )
            if not isinstance(argument, exp.Column) and not is_count_star:
                reasons.append(
                    f"aggregate argument {aggregate.sql()} must be one direct column"
                )
                continue
            type_name = (
                self._resolve_column_type(argument, aliases)
                if isinstance(argument, exp.Column)
                else None
            )
            if isinstance(aggregate, exp.Sum):
                if type_name is not None and _numeric_family(type_name) == "float":
                    reasons.append("floating SUM reduction parity is not yet proven")
            elif isinstance(aggregate, (exp.Min, exp.Max)):
                family = _numeric_family(type_name)
                if family == "float":
                    reasons.append("floating MIN/MAX NaN semantics are not native")
                elif family == "integer":
                    pass
                elif _is_binary_extrema_type(type_name):
                    # DuckDB BLOB and Arrow/Polars Binary both order unsigned
                    # bytes lexicographically, with the shorter value first when
                    # one is a prefix.  Certify only the scalar direct-column
                    # reduction: grouping/join spill and Binary predicates keep
                    # their existing fail-closed contract.
                    if has_grouped_reduction or has_joined_reduction:
                        reasons.append(
                            "Binary MIN/MAX is native only as a scalar "
                            "direct-column reduction"
                        )
                elif _is_datetime_us_extrema_type(type_name):
                    # Both engines reduce this exact physical family as signed
                    # integer microseconds and ignore NULL. Other units/zones
                    # remain outside the certified contract.
                    if has_grouped_reduction or has_joined_reduction:
                        reasons.append(
                            "Datetime(us) MIN/MAX is native only as a scalar "
                            "direct-column reduction"
                        )
                elif type_name is not None:
                    reasons.append(
                        f"{aggregate.key.upper()} over {type_name} has "
                        "unproven DuckDB semantics"
                    )

        # Native joins are deliberately in-memory only for now.  The resource
        # planner routes a decoded hash table that exceeds its budget to
        # DuckDB/Spark; no hidden Polars materialization is allowed.  Sort/group
        # spill has the smaller sealed SQL subset implemented below.

        return IslandCapability(not reasons, tuple(dict.fromkeys(reasons)))

    @staticmethod
    def _query_shape(parser) -> Tuple[exp.Select, bool, bool, bool]:
        root = IslandDB._query_root(parser)
        if not isinstance(root, exp.Select):
            raise IslandUnsupportedError("only one top-level SELECT is native")
        has_join = next(root.find_all(exp.Join), None) is not None
        has_group = root.args.get("group") is not None
        has_sort = root.args.get("order") is not None
        return root, has_join, has_group, has_sort

    def prepare_execution(
        self,
        reflection: Reflection,
        parser,
        *,
        streaming_result: bool,
    ) -> IslandPreparedQuery:
        """Validate semantics and produce the one authoritative resource plan."""
        capability = self.can_execute(
            reflection,
            parser,
            streaming_result=streaming_result,
        )
        capability.require()
        return IslandPreparedQuery(
            capability=capability,
            resource_plan=self.resource_plan(
                reflection, parser, streaming_result=streaming_result,
            ),
            streaming_result=bool(streaming_result),
        )

    @staticmethod
    def _selected_row_group_count(reflection: Reflection) -> int:
        total = 0
        for snapshot in reflection.supers:
            try:
                selections = (
                    getattr(snapshot, "row_group_selections", None) or {}
                )
                values = selections.values()
                for selection in values:
                    ids = selection.selected_ids
                    if isinstance(ids, tuple):
                        total += len(ids)
            except Exception:
                # Observability must follow the scan hint's fail-open contract.
                # A malformed optional mapping cannot fail an otherwise valid
                # SELECT after execution has already chosen to scan all groups.
                continue
        return total

    @staticmethod
    def _declared_row_group_units(snapshot: SuperSnapshot) -> int:
        """Best-effort parallelism units; malformed hints mean one/file."""
        try:
            selections = getattr(snapshot, "row_group_selections", None) or {}
            selected = sum(
                len(selection.selected_ids)
                for selection in selections.values()
                if isinstance(selection.selected_ids, tuple)
            )
            return selected + max(0, len(snapshot.files) - len(selections))
        except Exception:
            return max(1, len(snapshot.files))

    def resource_plan(
        self,
        reflection: Reflection,
        parser,
        *,
        streaming_result: bool,
    ) -> QueryResourcePlan:
        """Return the bounded execution decision used by routing and execute.

        Missing decoded statistics never become a zero-byte estimate.  The
        planner routes such a query to the mature external-memory engines.
        Operator-state multipliers are deliberately conservative until the
        native spill subset proves a tighter bound.
        """
        root, has_join, has_group, has_sort = self._query_shape(parser)
        aliases = self._table_maps(parser, reflection)
        decoded = max(0, int(getattr(reflection, "decoded_bytes", 0) or 0))
        selected_decoded = decoded
        selected_decoded_complete = bool(
            getattr(reflection, "selected_decoded_bytes_complete", False)
        )
        if selected_decoded_complete:
            selected_decoded = max(
                0,
                int(getattr(reflection, "selected_decoded_bytes", 0) or 0),
            )
        tombstones = tuple(
            (getattr(reflection, "tombstone_views", None) or {}).values()
        )
        has_active_tombstone = any(
            getattr(tombstone, "tombstone_path", None)
            for tombstone in tombstones
        )
        compressed = max(
            0,
            int(
                getattr(reflection, "row_group_scan_bytes", 0)
                or reflection.reflection_bytes
                or 0
            ),
        )
        complete = bool(
            getattr(reflection, "decoded_bytes_complete", False)
            and getattr(reflection, "row_group_scan_bytes_complete", False)
        )
        selected_groups = 0
        if not complete and not has_active_tombstone:
            footer_estimate = self._footer_working_set(
                reflection,
                required_columns=self._required_columns_by_snapshot(
                    parser, reflection,
                ),
            )
            if footer_estimate is not None:
                compressed, decoded, selected_groups = footer_estimate
                selected_decoded = decoded
                selected_decoded_complete = True
                complete = True
        candidate_rows_complete = all(
            bool(getattr(snapshot, "candidate_rows_complete", False))
            for snapshot in reflection.supers
        )
        candidate_rows = sum(
            max(0, int(getattr(snapshot, "candidate_rows", 0) or 0))
            for snapshot in reflection.supers
        )
        scalar_aggregates = tuple(
            root.find_all(exp.Count, exp.Sum, exp.Min, exp.Max)
        )
        group_cardinality_bound = (
            self._sealed_integer_group_cardinality(root, aliases)
            if has_group and not has_join
            else None
        )
        bounded_group_operator = group_cardinality_bound is not None
        group_expression = root.args.get("group")
        group_key_count = (
            len(group_expression.expressions)
            if isinstance(group_expression, exp.Group)
            else 0
        )
        # Conservative fixed numeric compact-state charge. The 512-byte base
        # covers hash-table/control/allocator slack; every direct key and every
        # accumulator gets another 128 bytes. Duplicate aggregate aliases are
        # intentionally counted as independent state.
        group_state_bytes_per_key = (
            512
            + 128 * max(1, group_key_count)
            + 128 * max(1, len(scalar_aggregates))
        )
        scalar_aggregate = bool(root.expressions) and all(
            next(expression.find_all(exp.Count, exp.Sum, exp.Min, exp.Max), None)
            is not None
            for expression in root.expressions
        ) and not has_group
        binary_extrema_bounds: List[int] = []
        for aggregate in scalar_aggregates:
            argument = aggregate.this
            if (
                isinstance(aggregate, (exp.Min, exp.Max))
                and isinstance(argument, exp.Column)
                and _is_binary_extrema_type(
                    self._resolve_column_type(argument, aliases)
                )
            ):
                bound = self._binary_value_bound(argument, aliases)
                if bound is None:
                    # Decoded scan bytes do not bound one dictionary-expanded
                    # variable-width value. Semantic parity remains certified,
                    # but execution needs the snapshot's exact write-time bound.
                    complete = False
                else:
                    binary_extrema_bounds.append(bound)
        # A join needs build/probe hash state; ordering needs runs/merge state;
        # grouped aggregation needs a key-state table.  Spill integration below
        # supports only the sealed direct-column GROUP/ORDER subset; joins route.
        if has_join:
            # Polars hash joins carry keys, row indices and allocator/hash-table
            # overhead beyond decoded buffers. Unknown output cardinality never
            # becomes a small result estimate below.
            state = decoded * 4
        elif has_group:
            if bounded_group_operator:
                # Native partial aggregation retains one compact entry per
                # possible key, never the wide decoded input.  This deliberately
                # overcharges C++ hash/allocator state: 512 bytes/key plus 128
                # bytes for every accumulator. It is still orders of magnitude
                # below the old 10-GiB input-sized plan for a sealed 1,024-key
                # domain, while a high-cardinality id domain remains external.
                state = (
                    int(group_cardinality_bound)
                    * group_state_bytes_per_key
                )
            else:
                # Preserve the historical ORDER+GROUP fallback exactly when no
                # complete domain proof exists. Unknown cardinality must never
                # be reinterpreted as a small hash table.
                state = (
                    decoded if has_sort else max(decoded * 2, compressed)
                )
                if candidate_rows_complete:
                    # Without a sealed NDV the only safe group-count bound is
                    # one key per selected row. Narrow input with hundreds of
                    # aggregate aliases can otherwise create vastly more state
                    # than decoded bytes and exhaust spill quota mid-query.
                    state = max(
                        state,
                        candidate_rows * group_state_bytes_per_key,
                    )
        elif has_sort:
            state = decoded
        elif scalar_aggregate:
            # COUNT/SUM/MIN/MAX reduce incrementally. Scan buffers are budgeted
            # separately; every aggregate has O(1) state. Count the aggregate
            # nodes rather than only output expressions because one aliased
            # expression may contain several independent reductions.
            state = 4096 * max(1, len(scalar_aggregates))
            # Polars can maintain one global-extremum accumulator per native
            # worker before merging. Charge the exact maximum value plus Python/
            # Arrow scalar overhead for every Binary reduction and worker.
            state += max(1, int(self._resources.cpu_count)) * sum(
                bound + 128 for bound in binary_extrema_bounds
            )
        else:
            # A projection/filter stream has no blocking operator state. Its
            # source and result batches are governed by scan/result budgets;
            # charging the full decoded input here would incorrectly route a
            # bounded 100-GB stream away merely because all batches together do
            # not fit memory.
            state = 0
        if selected_groups == 0:
            for snapshot in reflection.supers:
                selected_groups += self._declared_row_group_units(snapshot)
        # No cardinality estimator currently seals result width.  A SELECT with
        # no reduction can return the decoded input; grouped/sorted results may
        # do the same, while scalar aggregates are tiny.
        if scalar_aggregate:
            # Result width still grows with the number of projected aggregate
            # expressions.  A fixed scalar charge can under-admit generated
            # "aggregate every column" queries by orders of magnitude even
            # though their operator state remains bounded while scanning.
            result_bytes = 4096 * max(1, len(root.expressions))
            result_bytes += sum(
                bound + 128 for bound in binary_extrema_bounds
            )
            estimated_result_rows = 1
        else:
            projected_fields = 0
            for selected in root.expressions:
                core = selected.this if isinstance(selected, exp.Alias) else selected
                if isinstance(core, exp.Star) or (
                    isinstance(core, exp.Column) and core.is_star
                ):
                    projected_fields += sum(
                        1 for snapshot in reflection.supers
                        for name in (snapshot.column_types or {})
                        if str(name).casefold() not in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }
                    )
                else:
                    projected_fields += 1
            projected_fields = max(1, projected_fields)
            result_row_width = self._projected_result_row_width(
                root, aliases, reflection, parser,
            )
            if result_row_width is None:
                # A Binary result has no sound width without its immutable
                # write-time maximum. Keep a diagnostic fallback value, but
                # route the query away before Island admission/execution.
                result_row_width = projected_fields * 24
                complete = False
            if has_join:
                bounded_rows = self._candidate_result_upper_bound(
                    root, parser, reflection,
                )
                if bounded_rows is None:
                    candidate_rows_complete = False
                    worst_rows = 0
                else:
                    worst_rows = bounded_rows
                result_bytes = worst_rows * result_row_width
                estimated_result_rows = worst_rows
            elif bounded_group_operator:
                result_bytes = int(group_cardinality_bound) * result_row_width
                estimated_result_rows = int(group_cardinality_bound)
            else:
                result_bytes = candidate_rows * result_row_width
                estimated_result_rows = candidate_rows
            if not candidate_rows_complete:
                complete = False

        if has_group and has_sort:
            # ORDER BY consumes the grouped output, not the wide source. Charge
            # its full conservative result bound for both sealed and unknown
            # group cardinalities. The external operator remains mandatory for
            # the low-NDV case because the normal Polars lazy GROUP can
            # transiently retain decoded batches.
            state += result_bytes

        # A sealed deletion vector is eager metadata and anti-join hash state.
        # Bound it from its exact row count and the longest permitted resource
        # key. This also covers targeted-rowid integrity proof state.
        tombstone_state = 0
        for tombstone in tombstones:
            rows = getattr(tombstone, "expected_rows", None)
            digest = getattr(tombstone, "tombstone_digest", None)
            if (
                not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0
                or not isinstance(digest, str) or len(digest) != 64
            ):
                complete = False
                continue
            keys = getattr(tombstone, "snapshot_resource_keys", None)
            if keys is None:
                keys = getattr(tombstone, "resource_keys", None) or ()
            max_key = max((len(str(key).encode("utf-8")) for key in keys), default=0)
            # UTF-8 bytes + offsets/rowid/validity + conservative hash/object
            # overhead for validation and anti-join tables. Count each SQL
            # table occurrence: a self-join currently builds one eager DV frame
            # and one anti-join hash state per alias even when their immutable
            # tombstone seal is identical. Only the source-rowid proof is shared.
            tombstone_state += rows * (max_key + 8 + 8 + 1 + 128)
        state += tombstone_state
        decoded += tombstone_state
        spillable = bool((has_sort or has_group) and not has_join)
        estimate = QueryResourceEstimate(
            compressed_scan_bytes=compressed,
            decoded_scan_bytes=decoded,
            result_bytes=result_bytes,
            operator_state_bytes=state,
            selected_files=max(1, int(reflection.total_reflections or 0)),
            selected_row_groups=max(1, selected_groups),
            estimated_rows=(candidate_rows if candidate_rows_complete else 0),
            estimated_result_rows=(
                estimated_result_rows if candidate_rows_complete else 0
            ),
            selected_decoded_bytes=selected_decoded,
            selected_decoded_bytes_complete=selected_decoded_complete,
            spillable=spillable,
            has_sort=has_sort,
            has_group_by=has_group,
            has_join=has_join,
            estimates_complete=complete,
            requires_bounded_group_operator=bounded_group_operator,
            group_state_bytes_per_key=(
                group_state_bytes_per_key if bounded_group_operator else 0
            ),
        )
        return self._planner.plan(estimate, streaming_result=streaming_result)

    def _footer_working_set(
        self,
        reflection: Reflection,
        *,
        required_columns: Optional[Dict[int, Optional[set[str]]]] = None,
    ) -> Optional[Tuple[int, int, int]]:
        """Seal a conservative working set from local Parquet footers.

        This compatibility path covers directly constructed/local reflections
        that predate estimator byte fields.  It sums every physical column in
        selected row groups, so it may route away earlier but cannot understate
        decoded memory.  Remote reflections use the estimator/range metadata
        path and never trigger a whole-object download here.
        """
        compressed = decoded = groups = 0
        try:
            for snapshot in reflection.supers:
                requested = (
                    required_columns.get(id(snapshot))
                    if required_columns is not None
                    and id(snapshot) in required_columns
                    else None
                )
                for path, resource_key in zip(
                    snapshot.files, snapshot.resource_keys,
                ):
                    local = _local_path(path)
                    parquet_file = papq.ParquetFile(local)
                    metadata = parquet_file.metadata
                    selected_indexes: List[int] = []
                    widths: List[int] = []
                    for field_index, field in enumerate(parquet_file.schema_arrow):
                        if (
                            requested is not None
                            and field.name.casefold() not in requested
                        ):
                            continue
                        width = __import__(
                            "supertable.engine.data_estimator",
                            fromlist=["DataEstimator"],
                        ).DataEstimator._decoded_fixed_width(str(field.type))
                        if width is None and str(field.type).casefold() == "binary":
                            bounds = getattr(
                                snapshot, "column_max_value_bytes", None,
                            )
                            matches = (
                                [
                                    (name, value)
                                    for name, value in bounds.items()
                                    if str(name).casefold()
                                    == field.name.casefold()
                                ]
                                if isinstance(bounds, dict) else []
                            )
                            if (
                                len(matches) == 1
                                and str(matches[0][0]) == field.name
                                and isinstance(matches[0][1], int)
                                and not isinstance(matches[0][1], bool)
                                and matches[0][1] >= 0
                            ):
                                # Arrow Binary has one 32-bit offset per value;
                                # the caller's extra byte covers validity and
                                # alignment slack.
                                width = int(matches[0][1]) + 4
                        if width is None:
                            return None
                        selected_indexes.append(field_index)
                        widths.append(width)
                    if not selected_indexes:
                        return None
                    selected = self._validated_row_group_ids(
                        snapshot,
                        str(resource_key),
                        metadata.num_row_groups,
                        parquet_footer_sha256(metadata),
                    )
                    ids = selected or tuple(range(metadata.num_row_groups))
                    groups += len(ids)
                    for group_id in ids:
                        group = metadata.row_group(group_id)
                        for index in selected_indexes:
                            column = group.column(index)
                            compressed += max(0, int(column.total_compressed_size))
                        decoded += int(group.num_rows) * sum(
                            width + 1 for width in widths
                        )
            return compressed, decoded, groups
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Physical relations
    # ------------------------------------------------------------------

    @staticmethod
    def _reserved_schema_guard(schema: pl.Schema, *, source: str) -> None:
        reserved = {
            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            SOURCE_FILE_COL.casefold(), TOMBSTONE_FILE_COL.casefold(),
        }
        seen: Dict[str, str] = {}
        for name in schema.names():
            folded = str(name).casefold()
            if folded in seen:
                raise IslandIntegrityError(
                    f"{source} has case-insensitive duplicate columns "
                    f"{seen[folded]!r}/{name!r}"
                )
            seen[folded] = str(name)
        # Source identity is internal and must never be supplied by a data file.
        if SOURCE_FILE_COL.casefold() in seen:
            raise IslandIntegrityError(
                f"{source} contains reserved internal column {SOURCE_FILE_COL!r}"
            )
        # Case variants of the two persisted system columns are ambiguous.
        for canonical in (ROWID_COL, TIMESTAMP_COL):
            actual = seen.get(canonical.casefold())
            if actual is not None and actual != canonical:
                raise IslandIntegrityError(
                    f"{source} contains non-canonical system column {actual!r}"
                )

    @staticmethod
    def _validated_row_group_ids(
        snapshot: SuperSnapshot,
        resource_key: str,
        actual_count: int,
        actual_footer_sha256: object,
    ) -> Optional[Tuple[int, ...]]:
        """Return a sound explicit RG subset, or ``None`` for scan-all.

        Estimator hints are an optimisation, never an authority boundary. A
        stale/corrupt footer hash or count, or a malformed subset, therefore
        disables the hint for this object instead of clamping IDs or excluding
        data. Callers must pass the hash of the metadata backing their scan so
        planning and execution cannot accidentally apply different rules.
        """
        try:
            selections = getattr(snapshot, "row_group_selections", None) or {}
            selection = selections.get(resource_key)
            if selection is None:
                return None
            expected = selection.expected_row_group_count
            raw_ids = selection.selected_ids
            footer_sha256 = selection.footer_sha256
        except Exception:
            return None
        if (
            not isinstance(expected, int)
            or isinstance(expected, bool)
            or not isinstance(raw_ids, tuple)
            or not isinstance(footer_sha256, str)
            or len(footer_sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in footer_sha256)
            or not isinstance(actual_footer_sha256, str)
            or len(actual_footer_sha256) != 64
            or any(
                ch not in "0123456789abcdef"
                for ch in actual_footer_sha256
            )
            or any(
                not isinstance(value, int) or isinstance(value, bool)
                for value in raw_ids
            )
        ):
            return None
        ids = raw_ids
        if (
            expected != actual_count
            or expected <= 0
            or not ids
            or ids != tuple(sorted(set(ids)))
            or any(value < 0 or value >= actual_count for value in ids)
            or footer_sha256 != actual_footer_sha256
        ):
            return None
        # Encoding the complete set is equivalent to scan-all and needlessly
        # couples execution to an estimator artifact.
        return None if len(ids) == actual_count else ids

    def _local_scan_plan_key(self, snapshot: SuperSnapshot) -> Optional[str]:
        """Digest every pinned input that can change a local physical relation.

        Modern reflections carry exact positive sizes. Legacy reflections do
        not provide a stable physical identity and deliberately retain cold
        validation on every query rather than caching a path-only assertion.
        """
        try:
            sizes = tuple(getattr(snapshot, "resource_sizes", None) or ())
            if (
                len(sizes) != len(snapshot.files)
                or any(
                    not isinstance(size, int)
                    or isinstance(size, bool)
                    or size <= 0
                    for size in sizes
                )
            ):
                return None

            digest = hashlib.sha256(b"supertable-island-local-plan-v2\0")

            def add(value: object) -> None:
                encoded_value = str(value).encode(
                    "utf-8", errors="surrogatepass",
                )
                digest.update(len(encoded_value).to_bytes(8, "big"))
                digest.update(encoded_value)

            def section(name: str, count: int) -> None:
                # Field order is fixed by this version. Length framing plus an
                # explicit section/count makes the stream collision-free
                # without hashing dynamic label strings for every file.
                add(name)
                add(count)

            section("identity", 6)
            add(self.organization)
            add(self._artifact_cache_namespace)
            add(snapshot.super_name)
            add(snapshot.simple_name)
            add(snapshot.simple_version)
            add(getattr(snapshot, "snapshot_path", None))

            # Order is semantically significant: include_file_paths is mapped
            # positionally to the exact canonical resource identity.
            section("files", len(snapshot.files))
            for path, resource_key, size in zip(
                snapshot.files, snapshot.resource_keys, sizes,
            ):
                # Relative paths acquire meaning from the process working
                # directory. Include their syscall-free absolute form so a
                # later chdir cannot reuse a descriptor for the old target.
                add(self._local_source_path(path))
                add(resource_key)
                add(size)

            snapshot_keys = getattr(snapshot, "snapshot_resource_keys", None)
            if snapshot_keys is None:
                section("snapshot_resources_none", 0)
            else:
                section("snapshot_resources", len(snapshot_keys))
                for resource_key in snapshot_keys:
                    add(resource_key)

            column_types = snapshot.column_types or {}
            section("column_types", len(column_types))
            for name, type_name in column_types.items():
                add(name)
                add(type_name)
            columns = sorted(str(value) for value in (snapshot.columns or ()))
            section("column_set", len(columns))
            for name in columns:
                add(name)

            # Row-group selections and statistics seals are query-planning
            # inputs, not part of this descriptor. The local scanner deliberately
            # reads their safe superset and the LazyFrame retains each query's
            # independent predicate. If exact local subsets are introduced,
            # bump the key format before adding that subset to the descriptor.
            return digest.hexdigest()
        except Exception:
            # Key construction is an optimization boundary. Malformed metadata
            # must take the existing cold validator, which will either widen the
            # optional hint or raise its normal integrity error.
            return None

    @staticmethod
    def _local_source_path(path: object) -> str:
        """Return an absolute, symlink-preserving path for cheap hit sealing."""
        raw = str(path or "")
        if raw.startswith("file://"):
            parsed = urlparse(raw)
            if parsed.netloc not in ("", "localhost"):
                raise IslandUnsupportedError(
                    f"IslandDB cannot scan non-local file URL {raw!r}"
                )
            raw = unquote(parsed.path)
        if "://" in raw:
            raise IslandUnsupportedError(
                "IslandDB requires a localised Reflection; remote path remained"
            )
        return raw if os.path.isabs(raw) else os.path.abspath(raw)

    @staticmethod
    def _local_scan_plan_weight(
        local_paths: Iterable[str],
        local_to_resource: Dict[str, str],
        physical_schema: pa.Schema,
        file_seals: Iterable[_LocalFileSeal],
    ) -> int:
        """Conservative retained-memory charge for one descriptor-only plan."""
        path_bytes = sum(
            len(str(path).encode("utf-8", errors="surrogatepass")) + 96
            for path in local_paths
        )
        mapping_bytes = sum(
            len(str(path).encode("utf-8", errors="surrogatepass"))
            + len(str(key).encode("utf-8", errors="surrogatepass"))
            + 128
            for path, key in local_to_resource.items()
        )
        seal_bytes = sum(
            len(seal.source_path.encode("utf-8", errors="surrogatepass")) + 128
            for seal in file_seals
        )
        try:
            schema_bytes = int(physical_schema.serialize().size)
        except Exception:
            schema_bytes = 4096
        return max(1, 512 + path_bytes + mapping_bytes + seal_bytes + schema_bytes)

    @staticmethod
    def _local_relation_from_plan(
        plan: _LocalScanPlan,
        object_metadata_out: Optional[Dict[str, object]] = None,
        expected_object_metadata: Optional[Dict[str, object]] = None,
        parallel_strategy: str = "auto",
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> pl.LazyFrame:
        """Create a query-private lazy node from a shared immutable descriptor."""
        if object_metadata_out is not None or expected_object_metadata:
            if len(plan.local_to_resource) != len(plan.file_seals):
                raise IslandIntegrityError(
                    "cached local scan plan has non-bijective object seals"
                )
            actual_metadata = {
                resource_key: _local_object_metadata(seal)
                for (_local_path_value, resource_key), seal in zip(
                    plan.local_to_resource, plan.file_seals,
                )
            }
            for resource_key, expected in (
                expected_object_metadata or {}
            ).items():
                actual = actual_metadata.get(str(resource_key))
                expected_identity = getattr(expected, "identity_token", None)
                actual_identity = getattr(actual, "identity_token", None)
                try:
                    expected_token = (
                        expected_identity()
                        if callable(expected_identity) else None
                    )
                    actual_token = (
                        actual_identity() if callable(actual_identity) else None
                    )
                except Exception:
                    expected_token = actual_token = None
                if (
                    not isinstance(expected_token, str)
                    or not expected_token
                    or expected_token != actual_token
                ):
                    raise IslandIntegrityError(
                        f"local object identity for {resource_key!r} changed"
                    )
            if object_metadata_out is not None:
                object_metadata_out.update(actual_metadata)
        if execution_metrics_out is not None:
            execution_metrics_out.add_plan(
                files=len(plan.local_paths),
                row_groups=plan.row_group_count,
                rows=plan.row_count,
            )
        return (
            pl.scan_parquet(
                list(plan.local_paths),
                schema=pl.Schema(plan.physical_schema),
                include_file_paths=SOURCE_FILE_COL,
                hive_partitioning=False,
                use_statistics=True,
                parallel=parallel_strategy,
                missing_columns="insert",
                extra_columns="raise",
            )
            .with_columns(
                pl.col(SOURCE_FILE_COL).replace_strict(
                    dict(plan.local_to_resource),
                    return_dtype=pl.String,
                )
            )
        )

    def _base_relation(
        self,
        snapshot: SuperSnapshot,
        *,
        row_group_hints: bool = True,
        batch_rows: int = 65_536,
        object_metadata_out: Optional[Dict[str, object]] = None,
        expected_object_metadata: Optional[Dict[str, object]] = None,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
        parallel_strategy: str = "auto",
    ) -> pl.LazyFrame:
        """Return a physical relation, reusing only fully validated local plans."""
        if not (
            len(snapshot.files) == len(snapshot.resource_keys)
            and len(snapshot.files) > 0
        ):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has a non-bijective "
                "file/resource mapping"
            )
        if len(set(snapshot.resource_keys)) != len(snapshot.resource_keys):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has duplicate "
                "canonical resource keys"
            )
        has_remote = any(
            "://" in str(path or "")
            and not str(path or "").startswith("file://")
            for path in snapshot.files
        )
        if has_remote and expected_object_metadata is None:
            expected_object_metadata = _snapshot_expected_object_metadata(snapshot)
        plan_key = None if has_remote else self._local_scan_plan_key(snapshot)
        cached, build = _get_or_reserve_local_scan_plan(plan_key)
        if cached is not None:
            return self._local_relation_from_plan(
                cached,
                object_metadata_out=object_metadata_out,
                expected_object_metadata=expected_object_metadata,
                parallel_strategy=parallel_strategy,
                execution_metrics_out=execution_metrics_out,
            )

        built_plan: List[_LocalScanPlan] = []
        try:
            relation = self._base_relation_uncached(
                snapshot,
                row_group_hints=row_group_hints,
                batch_rows=batch_rows,
                object_metadata_out=object_metadata_out,
                expected_object_metadata=expected_object_metadata,
                range_metrics_out=range_metrics_out,
                execution_metrics_out=execution_metrics_out,
                parallel_strategy=parallel_strategy,
                local_plan_out=built_plan,
            )
        except BaseException:
            _finish_local_scan_plan_build(plan_key, build, None)
            raise
        if (
            plan_key is not None
            and built_plan
            and self._local_scan_plan_key(snapshot) != plan_key
        ):
            _finish_local_scan_plan_build(plan_key, build, None)
            raise IslandIntegrityError(
                "snapshot metadata changed during local scan planning"
            )
        _finish_local_scan_plan_build(
            plan_key, build, built_plan[0] if built_plan else None,
        )
        return relation

    def _base_relation_uncached(
        self,
        snapshot: SuperSnapshot,
        *,
        row_group_hints: bool = True,
        batch_rows: int = 65_536,
        object_metadata_out: Optional[Dict[str, object]] = None,
        expected_object_metadata: Optional[Dict[str, object]] = None,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
        parallel_strategy: str = "auto",
        local_plan_out: Optional[List[_LocalScanPlan]] = None,
    ) -> pl.LazyFrame:
        if not (
            len(snapshot.files) == len(snapshot.resource_keys)
            and len(snapshot.files) > 0
        ):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has a non-bijective "
                "file/resource mapping"
            )
        if len(set(snapshot.resource_keys)) != len(snapshot.resource_keys):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has duplicate "
                "canonical resource keys"
            )
        resolved_to_key: Dict[str, str] = {}
        fragments: List[pads.ParquetFileFragment] = []
        file_entries: Dict[str, Dict[str, object]] = {}
        has_remote = any(
            "://" in str(path or "")
            and not str(path or "").startswith("file://")
            for path in snapshot.files
        )
        filesystem = (
            pafs.PyFileSystem(_IslandFileSystemHandler(file_entries))
            if has_remote else pafs.LocalFileSystem()
        )
        common_schema: Optional[Dict[str, Tuple[str, pl.DataType]]] = None
        common_arrow_types: Dict[str, Tuple[str, pa.DataType]] = {}
        ordered_fields: List[pa.Field] = []
        seen_fields: set[str] = set()
        local_paths: List[str] = []
        local_to_resource: Dict[str, str] = {}
        local_validation_seals: List[_LocalFileSeal] = []
        pinned_schema: Dict[str, Tuple[str, str]] = {}
        for name, type_name in (snapshot.column_types or {}).items():
            folded = str(name).casefold()
            if folded in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            }:
                continue
            if folded in pinned_schema:
                raise IslandIntegrityError(
                    "snapshot schema contains case-insensitive duplicate columns"
                )
            pinned_schema[folded] = (str(name), str(type_name))
        physical_union: set[str] = set()
        planned_row_groups = 0
        planned_rows = 0
        declared_sizes = list(getattr(snapshot, "resource_sizes", None) or [])
        if declared_sizes and len(declared_sizes) != len(snapshot.files):
            raise IslandIntegrityError("snapshot resource sizes are non-bijective")
        for file_index, (path, resource_key) in enumerate(zip(
            snapshot.files, snapshot.resource_keys,
        )):
            virtual_path = f"{file_index:016x}.parquet"
            raw_path = str(path or "")
            is_remote = "://" in raw_path and not raw_path.startswith("file://")
            if is_remote:
                cache = self._get_range_cache()
                if cache is None:
                    raise IslandUnsupportedError(
                        "remote IslandDB scan requires the sealed range reader"
                    )
                expected_metadata = (
                    (expected_object_metadata or {}).get(str(resource_key))
                )
                remote_reader = cache.open(
                    str(resource_key), expected=expected_metadata,
                )
                metadata = remote_reader.metadata
                file_size = int(remote_reader.size)
                remote_reader.close()
                if object_metadata_out is not None:
                    object_metadata_out[str(resource_key)] = metadata
                declared = int(declared_sizes[file_index]) if declared_sizes else 0
                if declared > 0 and declared != file_size:
                    raise IslandIntegrityError(
                        f"snapshot size for {resource_key!r} does not match object seal"
                    )
                file_entries[virtual_path] = {
                    "raw_key": str(resource_key),
                    "cache": cache,
                    "metadata": metadata,
                    "size": file_size,
                    "metrics_sink": (
                        range_metrics_out.merge
                        if range_metrics_out is not None else None
                    ),
                }
            else:
                local = _local_path(path)
                validation_seal = None
                if not has_remote:
                    validation_seal = _local_file_seal(
                        self._local_source_path(path),
                    )
                    if validation_seal is None:
                        raise IslandIntegrityError(
                            "a local Parquet file changed before scan validation"
                        )
                prior_key = resolved_to_key.setdefault(local, str(resource_key))
                if prior_key != str(resource_key):
                    raise IslandIntegrityError(
                        "multiple canonical resource keys resolve to one local Parquet file"
                    )
                file_size = os.path.getsize(local)
                declared = int(declared_sizes[file_index]) if declared_sizes else 0
                if declared > 0 and declared != file_size:
                    raise IslandIntegrityError(
                        f"snapshot size for {resource_key!r} does not match local object"
                    )
                local_paths.append(local)
                local_to_resource[local] = str(resource_key)
                if validation_seal is not None:
                    local_validation_seals.append(validation_seal)
                if has_remote:
                    file_entries[virtual_path] = {
                        "local": local,
                        "size": file_size,
                    }
            fragment_path = virtual_path if has_remote else local
            file_format = pads.ParquetFileFormat()
            base_fragment = file_format.make_fragment(
                fragment_path, filesystem=filesystem, file_size=file_size,
            )
            file_row_group_count = int(base_fragment.num_row_groups)
            file_metadata = base_fragment.metadata
            physical_arrow_schema = base_fragment.physical_schema
            for field in physical_arrow_schema:
                if field.name not in seen_fields:
                    ordered_fields.append(field)
                    seen_fields.add(field.name)
            physical_schema = pl.Schema(physical_arrow_schema)
            self._reserved_schema_guard(physical_schema, source=resource_key)
            current_arrow_types = {
                str(field.name).casefold(): (str(field.name), field.type)
                for field in physical_arrow_schema
                if str(field.name).casefold() not in {
                    ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                }
            }
            incompatible_arrow = sorted(
                name for name in set(common_arrow_types).intersection(
                    current_arrow_types,
                )
                if (
                    common_arrow_types[name][0]
                    != current_arrow_types[name][0]
                    or not common_arrow_types[name][1].equals(
                        current_arrow_types[name][1]
                    )
                )
            )
            if incompatible_arrow:
                raise IslandUnsupportedError(
                    "IslandDB native physical schema evolution requires identical "
                    "physical Arrow names and types for shared columns: "
                    + ", ".join(incompatible_arrow)
                )
            common_arrow_types.update(current_arrow_types)
            comparable_schema = {
                str(name).casefold(): (str(name), dtype)
                for name, dtype in physical_schema.items()
                if str(name).casefold() not in {
                    ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                }
            }
            physical_union.update(comparable_schema)
            metadata_mismatches: List[str] = []
            for folded, (physical_name, physical_type) in comparable_schema.items():
                pinned = pinned_schema.get(folded)
                if pinned is None:
                    metadata_mismatches.append(f"{physical_name} (not pinned)")
                    continue
                pinned_name, pinned_type = pinned
                if (
                    pinned_name != physical_name
                    or _canonical_physical_type(pinned_type)
                    != _canonical_physical_type(physical_type)
                ):
                    metadata_mismatches.append(
                        f"{physical_name}:{physical_type} != "
                        f"{pinned_name}:{pinned_type}"
                    )
            if metadata_mismatches:
                raise IslandUnsupportedError(
                    "IslandDB cannot prove pinned/physical schema equivalence: "
                    + ", ".join(metadata_mismatches)
                )
            if common_schema is None:
                common_schema = comparable_schema
            else:
                overlap = set(common_schema).intersection(comparable_schema)
                incompatible = sorted(
                    name for name in overlap
                    if common_schema[name] != comparable_schema[name]
                )
                if incompatible:
                    raise IslandUnsupportedError(
                        "IslandDB native schema evolution requires identical "
                        "physical names and types for shared columns: "
                        + ", ".join(incompatible)
                    )
                common_schema.update(comparable_schema)
            selected_ids = None
            # Local scans intentionally use Polars' conservative native footer
            # pruning below. Computing a SHA-256 metadata seal and constructing
            # a second exact Arrow fragment for every local file is therefore
            # dead work (severe at 10k+ files). Remote range reads still require
            # the exact sealed subset because expanding it changes physical I/O.
            if row_group_hints and has_remote:
                try:
                    live_footer_sha256 = parquet_footer_sha256(
                        file_metadata,
                    )
                except Exception:
                    live_footer_sha256 = ""
                selected_ids = self._validated_row_group_ids(
                    snapshot,
                    str(resource_key),
                    file_row_group_count,
                    live_footer_sha256,
                )
            if selected_ids is None:
                # Metadata already carries both totals. Avoid constructing and
                # walking every row-group id solely for observability on a
                # 10k-file/full-scan workload.
                planned_row_groups += file_row_group_count
                planned_rows += max(0, int(file_metadata.num_rows))
            else:
                planned_row_groups += len(selected_ids)
                planned_rows += sum(
                    max(0, int(file_metadata.row_group(group_id).num_rows))
                    for group_id in selected_ids
                )
            if has_remote:
                fragments.append(
                    file_format.make_fragment(
                        fragment_path,
                        filesystem=filesystem,
                        file_size=file_size,
                        row_groups=(list(selected_ids) if selected_ids else None),
                        partition_expression=(
                            pads.field(SOURCE_FILE_COL) == str(resource_key)
                        ),
                    )
                )
        missing_pinned = sorted(set(pinned_schema).difference(physical_union))
        if missing_pinned:
            raise IslandUnsupportedError(
                "IslandDB pinned columns are absent from every selected file: "
                + ", ".join(pinned_schema[name][0] for name in missing_pinned)
            )
        # Arrow's dataset scanner treats the canonical object key as a virtual
        # partition column.  This replaces one LazyFrame + one literal column
        # per file and lets 100+ resources participate in one parallel scan.
        # Shared columns were already proven exact above; missing evolution
        # columns are represented as NULL by the dataset schema.
        ordered_fields.append(pa.field(SOURCE_FILE_COL, pa.string()))
        dataset_schema = pa.schema(ordered_fields)
        if not has_remote:
            # Polars' Rust-native multi-file scanner is materially faster for a
            # full-row-group local/whole-object-cache scan.  It still performs
            # projection and predicate pushdown and attaches an exact path
            # identity, which is immediately translated to the pinned raw key.
            # For local immutable files the Rust scanner's own conservative
            # footer-statistics pruning is faster than crossing the Python
            # Arrow Dataset bridge.  An estimator row-group subset is only a
            # performance hint: scanning its safe superset preserves results.
            # Remote objects still require exact fragments below so range I/O
            # never expands from selected chunks to whole objects.
            physical_arrow_schema = pa.schema(ordered_fields[:-1])
            plan = _LocalScanPlan(
                local_paths=tuple(local_paths),
                local_to_resource=tuple(local_to_resource.items()),
                physical_schema=physical_arrow_schema,
                file_seals=tuple(local_validation_seals),
                row_group_count=planned_row_groups,
                row_count=planned_rows,
                weight_bytes=self._local_scan_plan_weight(
                    local_paths,
                    local_to_resource,
                    physical_arrow_schema,
                    local_validation_seals,
                ),
            )
            if not _local_scan_plan_is_current(plan):
                raise IslandIntegrityError(
                    "a validated local Parquet file changed during scan planning"
                )
            if local_plan_out is not None:
                local_plan_out.append(plan)
            return self._local_relation_from_plan(
                plan,
                object_metadata_out=object_metadata_out,
                expected_object_metadata=expected_object_metadata,
                parallel_strategy=parallel_strategy,
                execution_metrics_out=execution_metrics_out,
            )
        dataset = pads.FileSystemDataset(
            fragments,
            dataset_schema,
            pads.ParquetFileFormat(),
            filesystem=filesystem,
        )
        if execution_metrics_out is not None:
            execution_metrics_out.add_plan(
                files=len(snapshot.files),
                row_groups=planned_row_groups,
                rows=planned_rows,
            )
        return pl.scan_pyarrow_dataset(
            dataset,
            allow_pyarrow_filter=True,
            batch_size=max(1, int(batch_rows)),
        )

    def _load_tombstone(self, tomb_def) -> pl.DataFrame:
        if not _validate_island_tombstone_definition(tomb_def):
            raise IslandIntegrityError(
                "active deletion-vector definition has no path"
            )
        path = str(getattr(tomb_def, "tombstone_path", "") or "")
        cache_key = str(getattr(tomb_def, "cache_key", "") or "")
        if not path:
            raise IslandIntegrityError("active deletion-vector definition has no path")
        allowed = getattr(tomb_def, "snapshot_resource_keys", None)
        if allowed is None:
            allowed = getattr(tomb_def, "resource_keys", ())
        # The digest is snapshot-pinned and content-derived. Include it (plus
        # expected cardinality) so two independently constructed snapshots that
        # reuse the same raw key can never alias inside a long-lived process.
        digest = str(getattr(tomb_def, "tombstone_digest", "") or "")
        rows = getattr(tomb_def, "expected_rows", None)
        if getattr(tomb_def, "tombstone_format", None) == 2:
            segments = getattr(tomb_def, "segments", ())
            if self.storage is None or not cache_key:
                raise IslandIntegrityError(
                    "required v2 deletion vector cannot be read safely"
                )
            try:
                return load_tombstone_segments(
                    segments,
                    storage=self.storage,
                    cache_identity=(
                        f"islanddb-dv-v2:{self._artifact_cache_namespace}:"
                        f"{cache_key}:{digest}"
                    ),
                    expected_rows=rows,
                    allowed_files=set(allowed),
                )
            except Exception as exc:
                raise IslandIntegrityError(
                    "required v2 deletion vector failed validation"
                ) from exc
        identity_source = cache_key or os.path.realpath(path)
        cache_identity = (
            f"islanddb-dv-v1:{self._artifact_cache_namespace}:{identity_source}:"
            f"{rows}:{digest}"
        )
        if "://" not in path and os.path.isfile(path):
            read_path = path
            loader = lambda: pl.read_parquet(
                read_path, hive_partitioning=False,
            )
        elif self.storage is not None and cache_key:
            # DVs are bounded correctness metadata. Use the storage SDK only on
            # a scoped cache miss; rotating presigned URLs never enter identity.
            read_path = path
            loader = lambda: pl.from_arrow(self.storage.read_parquet(cache_key))
        else:
            raise IslandIntegrityError("required deletion vector cannot be read safely")
        frame = load_tombstone(
            read_path,
            cache_identity=cache_identity,
            loader=loader,
            allow_cache=True,
            required=True,
            expected_rows=getattr(tomb_def, "expected_rows", None),
            expected_digest=getattr(tomb_def, "tombstone_digest", None),
            allowed_files=set(allowed),
        )
        if frame is None:  # required=True should make this unreachable.
            raise IslandIntegrityError("required deletion vector was unavailable")
        return frame

    def _source_rowid_proof_key(
        self,
        resource_key: str,
        metadata: object,
        rowids: Iterable[int],
    ) -> Optional[Tuple[object, ...]]:
        """Key a proof only for a stable, immutable source-object identity."""
        identity = getattr(metadata, "identity_token", None)
        try:
            token = identity() if callable(identity) else None
        except Exception:
            token = None
        if not isinstance(token, str) or not token:
            return None
        digest = hashlib.sha256(b"st-island-rowid-proof-v1\0")
        try:
            values = sorted(int(value) for value in rowids)
        except Exception:
            return None
        for value in values:
            if value <= 0 or value > 2**63 - 1:
                return None
            digest.update(value.to_bytes(8, "big", signed=False))
        return (
            self.organization,
            self._proof_namespace,
            str(resource_key),
            token,
            digest.hexdigest(),
        )

    @staticmethod
    def _source_rowid_proof_hit(key: Optional[Tuple[object, ...]]) -> bool:
        if key is None:
            return False
        with _ROWID_PROOF_LOCK:
            if key not in _ROWID_PROOFS:
                return False
            _ROWID_PROOFS.move_to_end(key)
            return True

    @staticmethod
    def _store_source_rowid_proofs(
        keys: Iterable[Optional[Tuple[object, ...]]],
    ) -> None:
        with _ROWID_PROOF_LOCK:
            for key in keys:
                if key is None:
                    continue
                # The key is bound to immutable object identity and the exact
                # DV rowid set. Capacity pressure, not wall-clock age, is the
                # only valid reason to discard a completed integrity proof.
                _ROWID_PROOFS[key] = True
                _ROWID_PROOFS.move_to_end(key)
            while len(_ROWID_PROOFS) > _ROWID_PROOF_MAX_ENTRIES:
                _ROWID_PROOFS.popitem(last=False)

    def _validate_source_rowids(
        self,
        snapshot: SuperSnapshot,
        deletion_vector: pl.DataFrame,
        *,
        object_metadata: Optional[Dict[str, object]] = None,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> None:
        targets = deletion_vector.filter(
            pl.col(TOMBSTONE_FILE_COL).is_in(snapshot.resource_keys)
        ).select([TOMBSTONE_FILE_COL, ROWID_COL])
        if targets.height == 0:
            return
        target_keys = set(
            targets.get_column(TOMBSTONE_FILE_COL).unique().to_list()
        )
        proof_keys: Dict[str, Optional[Tuple[object, ...]]] = {}
        missed_keys: set[str] = set()
        for resource_key in target_keys:
            per_file_rowids = (
                targets
                .filter(pl.col(TOMBSTONE_FILE_COL) == resource_key)
                .get_column(ROWID_COL)
                .to_list()
            )
            proof_key = self._source_rowid_proof_key(
                resource_key,
                (object_metadata or {}).get(resource_key),
                per_file_rowids,
            )
            proof_keys[resource_key] = proof_key
            if not self._source_rowid_proof_hit(proof_key):
                missed_keys.add(resource_key)
        if not missed_keys:
            return
        targets = targets.filter(
            pl.col(TOMBSTONE_FILE_COL).is_in(list(missed_keys))
        )
        indexes = [
            index for index, key in enumerate(snapshot.resource_keys)
            if key in missed_keys
        ]
        if len(indexes) != len(missed_keys):
            raise IslandIntegrityError(
                "deletion-vector references a source outside the selected snapshot"
            )
        declared_sizes = list(getattr(snapshot, "resource_sizes", None) or [])
        proof_snapshot = replace(
            snapshot,
            files=[snapshot.files[index] for index in indexes],
            resource_keys=[snapshot.resource_keys[index] for index in indexes],
            resource_sizes=(
                [declared_sizes[index] for index in indexes]
                if declared_sizes else []
            ),
            row_group_selections={},
        )
        # One Arrow dataset covers 100+ resources and partition-prunes to the
        # exact DV-referenced object keys; unrelated survivor files are not
        # opened for this integrity pass. Row-group hints are intentionally
        # disabled: proving source rowid uniqueness is a whole-file integrity
        # boundary, not a query optimization.
        scan = self._base_relation(
            proof_snapshot,
            row_group_hints=False,
            expected_object_metadata={
                key: metadata
                for key, metadata in (object_metadata or {}).items()
                if key in missed_keys
            },
            range_metrics_out=range_metrics_out,
            execution_metrics_out=execution_metrics_out,
        )
        schema = scan.collect_schema()
        if ROWID_COL not in schema or schema[ROWID_COL] != pl.Int64:
            raise IslandIntegrityError(
                f"deletion-vector source lacks canonical Int64 {ROWID_COL}"
            )
        # Prove exactly the identity the anti join will consume. A duplicate
        # source row for an unrelated id cannot over-delete this query; every
        # DV-targeted (file,rowid) must resolve to exactly one physical row.
        # This bounds hash state by the sealed DV rather than all source rows.
        matches = (
            scan
            .select([SOURCE_FILE_COL, ROWID_COL])
            .join(
                targets.lazy(),
                left_on=[SOURCE_FILE_COL, ROWID_COL],
                right_on=[TOMBSTONE_FILE_COL, ROWID_COL],
                how="inner",
            )
            .group_by([SOURCE_FILE_COL, ROWID_COL])
            .agg(pl.len().alias("matches"))
            .collect(engine="streaming")
        )
        if (
            matches.height != targets.height
            or matches.filter(pl.col("matches") != 1).height
        ):
            raise IslandIntegrityError(
                "a deletion-vector (source file, rowid) does not identify "
                "exactly one physical row"
            )
        self._store_source_rowid_proofs(
            proof_keys.get(resource_key) for resource_key in missed_keys
        )

    def _apply_tombstone(
        self,
        relation: pl.LazyFrame,
        snapshot: SuperSnapshot,
        tomb_def,
        *,
        object_metadata: Optional[Dict[str, object]] = None,
        query_proofs: Optional[set[Tuple[object, ...]]] = None,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> pl.LazyFrame:
        if _validate_island_tombstone_definition(tomb_def):
            dv = self._load_tombstone(tomb_def)
            query_marker = (
                id(snapshot),
                getattr(tomb_def, "cache_key", None),
                getattr(tomb_def, "expected_rows", None),
                getattr(tomb_def, "tombstone_digest", None),
            )
            if query_proofs is None or query_marker not in query_proofs:
                self._validate_source_rowids(
                    snapshot,
                    dv,
                    object_metadata=object_metadata,
                    range_metrics_out=range_metrics_out,
                    execution_metrics_out=execution_metrics_out,
                )
                if query_proofs is not None:
                    query_proofs.add(query_marker)
            relation = relation.join(
                dv.lazy(),
                left_on=[SOURCE_FILE_COL, ROWID_COL],
                right_on=[TOMBSTONE_FILE_COL, ROWID_COL],
                how="anti",
                coalesce=True,
            )
        schema = relation.collect_schema()
        public = [
            name for name in schema.names()
            if str(name).casefold() not in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                SOURCE_FILE_COL.casefold(), TOMBSTONE_FILE_COL.casefold(),
            }
        ]
        return relation.select(public)

    @staticmethod
    def _normalize_aggregate_dtypes(
        result: pl.DataFrame,
        root: exp.Select,
    ) -> pl.DataFrame:
        casts: List[pl.Expr] = []
        for selected in root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            name = selected.alias_or_name
            if not name or name not in result.columns:
                continue
            if isinstance(core, exp.Count):
                casts.append(pl.col(name).cast(pl.Int64))
            elif isinstance(core, exp.Sum) and (
                result.schema[name].is_integer()
                or result.schema[name].is_decimal()
            ):
                # DuckDB SUM(integer) is HUGEINT; fetchdf exposes it as float64.
                casts.append(pl.col(name).cast(pl.Float64))
        return result.with_columns(casts) if casts else result

    def _rewrite_native_aggregates(
        self,
        sql: str,
        parser,
        reflection: Reflection,
    ) -> Tuple[str, exp.Select, exp.Select]:
        """Bridge documented aggregate edge semantics before Polars parses SQL.

        Polars returns integer zero for ``SUM`` over an empty/all-NULL input;
        SQL and DuckDB require NULL.  Rewriting each native SUM to a guarded
        aggregate preserves row-group/projection pushdown and the exact result.
        """
        parsed = self._query_root(parser) if sql == parser.original_query else None
        root = (
            parsed.copy()
            if isinstance(parsed, exp.Expression)
            else sqlglot.parse_one(sql, read="duckdb")
        )
        if not isinstance(root, exp.Select):  # already fenced by can_execute
            raise IslandUnsupportedError("only one top-level SELECT is native")
        original = root.copy()
        aliases = self._table_maps(parser, reflection)
        for aggregate in list(root.find_all(exp.Sum)):
            argument = aggregate.this.copy()
            widened = aggregate.copy()
            argument_type = (
                self._resolve_column_type(aggregate.this, aliases)
                if isinstance(aggregate.this, exp.Column)
                else None
            )
            if (
                _numeric_family(argument_type) == "integer"
                or isinstance(aggregate.this, exp.Literal)
                and aggregate.this.is_int
            ):
                widened.set(
                    "this",
                    exp.Cast(
                        this=argument.copy(),
                        to=exp.DataType.build("INT128"),
                    ),
                )
            condition = exp.EQ(
                this=exp.Count(this=argument.copy()),
                expression=exp.Literal.number(0),
            )
            guarded = exp.Case(
                ifs=[exp.If(this=condition, true=exp.Null())],
                default=widened,
            )
            aggregate.replace(guarded)
        return root.sql(dialect="duckdb"), original, root

    @staticmethod
    def _to_duckdb_pandas(result: pl.DataFrame) -> pd.DataFrame:
        """Match ``DuckDBPyRelation.fetchdf`` nullable scalar dtypes."""
        frame = result.to_pandas(use_pyarrow_extension_array=False)
        nullable_integer = {
            pl.Int8: "Int8", pl.Int16: "Int16", pl.Int32: "Int32",
            pl.Int64: "Int64", pl.UInt8: "UInt8", pl.UInt16: "UInt16",
            pl.UInt32: "UInt32", pl.UInt64: "UInt64",
        }
        for name, dtype in result.schema.items():
            series = result.get_column(name)
            if dtype == pl.Binary:
                # DuckDB fetchdf represents BLOB values as bytearray and NULL as
                # pandas.NA.  Polars emits bytes/None. Normalize the materialized
                # compatibility facade while leaving the public Arrow stream's
                # canonical Binary buffers untouched.
                frame[name] = pd.Series(
                    [
                        pd.NA if value is None else bytearray(value)
                        for value in series.to_list()
                    ],
                    index=frame.index,
                    dtype=object,
                )
                continue
            if series.null_count() == 0:
                continue
            pandas_dtype = nullable_integer.get(dtype)
            if pandas_dtype:
                frame[name] = pd.array(series.to_list(), dtype=pandas_dtype)
            elif dtype == pl.Boolean:
                frame[name] = pd.array(series.to_list(), dtype="boolean")
        return frame

    @staticmethod
    def _write_profile(path: str, profile: IslandProfile) -> Tuple[float, bool]:
        started = time.perf_counter()
        succeeded = False
        tmp: Optional[Path] = None
        try:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(profile.as_dict(), handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
            succeeded = True
        except Exception as exc:  # profiling must never break a read
            logger.debug("[islanddb] profile write skipped: %s", exc)
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
        return (time.perf_counter() - started) * 1000.0, succeeded

    # ------------------------------------------------------------------
    # Public execution API
    # ------------------------------------------------------------------

    def _prepare_lazy_query(
        self,
        reflection: Reflection,
        parser,
        timer_capture,
        log_prefix: str,
        *,
        sql_override: Optional[str] = None,
        batch_rows: int = 65_536,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> Tuple[pl.LazyFrame, exp.Select, str, str]:
        snapshots = self._snapshots(reflection)
        table_defs = parser.get_table_tuples()
        context = pl.SQLContext(eager=False)
        alias_to_physical: Dict[str, str] = {}
        query_rowid_proofs: set[Tuple[object, ...]] = set()
        required_columns = self._required_columns_by_snapshot(
            parser, reflection,
        )
        tombstone_views = {
            str(alias).casefold(): tombstone
            for alias, tombstone in (
                getattr(reflection, "tombstone_views", None) or {}
            ).items()
        }
        for index, td in enumerate(table_defs):
            snapshot = snapshots.get(
                (td.super_name.casefold(), td.simple_name.casefold())
            )
            if snapshot is None:
                raise IslandIntegrityError(
                    f"missing pinned snapshot for {td.super_name}.{td.simple_name}"
                )
            physical = f"island_{index}_{uuid.uuid4().hex}"
            object_metadata: Dict[str, object] = {}
            relation = self._base_relation(
                snapshot,
                batch_rows=batch_rows,
                object_metadata_out=object_metadata,
                range_metrics_out=range_metrics_out,
                execution_metrics_out=execution_metrics_out,
                parallel_strategy=self._local_scan_parallel_strategy(
                    reflection, required_columns.get(id(snapshot)),
                ),
            )
            tomb_def = tombstone_views.get(str(td.alias).casefold())
            relation = self._apply_tombstone(
                relation,
                snapshot,
                tomb_def,
                object_metadata=object_metadata,
                query_proofs=query_rowid_proofs,
                range_metrics_out=range_metrics_out,
                execution_metrics_out=execution_metrics_out,
            )
            context.register(physical, relation)
            alias_to_physical[td.alias] = physical

        timer_capture("CREATING_REFLECTION")
        native_sql, original_root, native_root = self._rewrite_native_aggregates(
            sql_override or parser.original_query, parser, reflection,
        )
        query = rewrite_query_with_hashed_tables(
            native_sql,
            alias_to_physical,
            parsed_expression=native_root,
            default_super_name=parser.default_super_name,
        )
        parser.executing_query = query
        lazy_result = context.execute(query, eager=False)
        if not isinstance(lazy_result, pl.LazyFrame):
            lazy_result = lazy_result.lazy()

        # Normalize the native Arrow stream itself, not only its materialized
        # pandas facade, so streaming and non-streaming callers share one type
        # contract.
        schema = lazy_result.collect_schema()
        casts: List[pl.Expr] = []
        for selected in original_root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            name = selected.alias_or_name
            if not name or name not in schema:
                continue
            if isinstance(core, exp.Count):
                casts.append(pl.col(name).cast(pl.Int64))
            elif isinstance(core, exp.Sum) and schema[name].is_integer():
                # Polars widens integer SUM to Int128, but the PyArrow build
                # supported by SuperTable cannot import Polars' private
                # ``_pli128`` C data-interface format. Decimal128(38, 0) is the
                # exact public Arrow representation DuckDB itself uses for a
                # HUGEINT result. The legacy pandas facade converts it to
                # DuckDB fetchdf's float64 only *after* bounded collection;
                # Arrow streaming therefore never loses integers above 2**53.
                casts.append(
                    pl.col(name).cast(pl.Decimal(precision=38, scale=0))
                )
        if casts:
            lazy_result = lazy_result.with_columns(casts)
        optimized_plan = lazy_result.explain(optimized=True)
        logger.debug("%s[islanddb] executing native query: %s", log_prefix, query)
        return lazy_result, original_root, query, optimized_plan

    @staticmethod
    def _lazy_batches(
        lazy_result: pl.LazyFrame,
        *,
        batch_rows: int,
    ) -> Tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        logical_schema = lazy_result.collect_schema()

        def canonical_schema(schema: pa.Schema) -> pa.Schema:
            fields = [
                pa.field(
                    field.name,
                    pa.binary()
                    if logical_schema.get(field.name) == pl.Binary
                    else field.type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in schema
            ]
            return pa.schema(fields, metadata=schema.metadata)

        def canonical_arrow(frame: pl.DataFrame) -> pa.Table:
            table = frame.to_arrow()
            # Polars' Binary C-data export currently uses large_binary even
            # when the Parquet/DuckDB BLOB contract is 32-bit Arrow binary.
            # Query batches are resource-bounded well below 2 GiB, so the safe
            # offset narrowing is exact and makes the public Arrow stream match
            # DuckDB as well as the materialized pandas facade.
            target = canonical_schema(table.schema)
            return table if target.equals(table.schema) else table.cast(
                target, safe=True,
            )

        frames = lazy_result.collect_batches(
            chunk_size=max(1, int(batch_rows)),
            maintain_order=True,
            engine="streaming",
        )
        try:
            first_frame = next(frames)
        except StopIteration:
            first_frame = None
        # Polars' logical String maps to Arrow string_view while materialized
        # batches currently expose large_string.  Anchor a non-empty stream to
        # its first physical batch; for an empty stream the logical schema is
        # the only schema available and no batch can violate it.
        schema = (
            canonical_arrow(first_frame).schema
            if first_frame is not None
            else canonical_schema(logical_schema.to_arrow())
        )

        def produce() -> Iterable[pa.RecordBatch]:
            try:
                if first_frame is not None:
                    yield from canonical_arrow(first_frame).to_batches(
                        max_chunksize=max(1, int(batch_rows)),
                    )
                for frame in frames:
                    yield from canonical_arrow(frame).to_batches(
                        max_chunksize=max(1, int(batch_rows)),
                    )
            finally:
                close = getattr(frames, "close", None)
                if callable(close):
                    close()

        return schema, produce()

    @staticmethod
    def _row_group_hints_are_full_scan(snapshot: SuperSnapshot) -> bool:
        """Prove that optional local row-group hints do not select a subset.

        The direct Arrow projection below deliberately has no SQL predicate
        compiler. A partial estimator hint normally accompanies a predicate,
        but treating the hint itself as the predicate would be a correctness
        bug. Admit only absent hints or the canonical complete sequence and
        leave every partial/malformed shape on the existing Polars path.
        """
        selections = getattr(snapshot, "row_group_selections", None)
        if not selections:
            return True
        try:
            if not isinstance(selections, dict):
                return False
            resource_keys = set(str(key) for key in snapshot.resource_keys)
            if any(str(key) not in resource_keys for key in selections):
                return False
            for selection in selections.values():
                expected = selection.expected_row_group_count
                selected = selection.selected_ids
                if (
                    not isinstance(expected, int)
                    or isinstance(expected, bool)
                    or expected <= 0
                    or not isinstance(selected, tuple)
                    # Check the already-retained tuple in-place. Constructing
                    # range(expected) as another tuple would let malformed
                    # optional metadata request an unbounded allocation.
                    or len(selected) != expected
                    or any(
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or value != index
                        for index, value in enumerate(selected)
                    )
                ):
                    return False
        except Exception:
            return False
        return True

    def _direct_local_projection_batches(
        self,
        reflection: Reflection,
        parser,
        root: exp.Select,
        *,
        column_names: Iterable[str],
        batch_rows: int,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> Optional[Tuple[pa.Schema, Iterable[pa.RecordBatch], str]]:
        """Return a sealed Arrow Dataset projection for one simple local scan.

        This is intentionally a narrow physical fast path, not another SQL
        implementation. GROUP/aggregate/order semantics remain in the bounded
        spill operators. Arrow replaces only the redundant Parquet -> Polars
        -> Arrow conversion for an unfiltered, unmodified single-table
        projection. Any uncertainty preserves the general native Polars path.
        """
        if (
            getattr(reflection, "rbac_views", None)
            or getattr(reflection, "tombstone_views", None)
            or len(parser.get_table_tuples()) != 1
        ):
            return None

        # A plain aggregate query has values only in these four Select slots.
        # Reject LIMIT too, even when capability checking proved it redundant:
        # this physical layer must never silently implement SQL semantics.
        allowed_args = {"expressions", "from", "from_", "group", "order"}
        if any(
            value is not None and name not in allowed_args
            for name, value in root.args.items()
        ):
            return None
        from_clause = root.args.get("from") or root.args.get("from_")
        if (
            not isinstance(from_clause, exp.From)
            or not isinstance(from_clause.this, exp.Table)
            or sum(isinstance(node, exp.Select) for node in root.walk()) != 1
        ):
            return None

        definition = parser.get_table_tuples()[0]
        snapshot = self._snapshots(reflection).get((
            definition.super_name.casefold(),
            definition.simple_name.casefold(),
        ))
        if snapshot is None or not self._row_group_hints_are_full_scan(snapshot):
            return None
        if any(
            "://" in str(path or "")
            and not str(path or "").startswith("file://")
            for path in snapshot.files
        ):
            # Remote fragments carry range-reader identities and exact sealed
            # row-group subsets. They continue through _base_relation.
            return None

        names = tuple(str(name) for name in column_names)
        if not names or len(set(names)) != len(names):
            return None

        # Build or hit the same descriptor cache as the Polars relation. This
        # performs the existing resource-size, physical-schema, pinned-schema,
        # path/resource bijection, and filesystem identity checks before Arrow
        # can open a data page. The LazyFrame returned here is never collected.
        plan_key = self._local_scan_plan_key(snapshot)
        if plan_key is None:
            return None
        self._base_relation(snapshot, batch_rows=batch_rows)
        with _LOCAL_SCAN_PLAN_LOCK:
            scan_plan = _LOCAL_SCAN_PLANS.get(plan_key)
            if scan_plan is not None:
                _LOCAL_SCAN_PLANS.move_to_end(plan_key)
        if (
            scan_plan is None
            or self._local_scan_plan_key(snapshot) != plan_key
            or not _local_scan_plan_is_current(scan_plan)
        ):
            return None

        field_indexes = [
            scan_plan.physical_schema.get_field_index(name) for name in names
        ]
        if any(index < 0 for index in field_indexes):
            # The validator should already reject a pinned-but-absent column.
            # Fall back rather than let a fast-path assumption weaken that
            # established error boundary.
            return None
        projection_schema = pa.schema([
            scan_plan.physical_schema.field(index) for index in field_indexes
        ])
        dataset = pads.FileSystemDataset.from_paths(
            list(scan_plan.local_paths),
            schema=scan_plan.physical_schema,
            format=pads.ParquetFileFormat(),
            filesystem=pafs.LocalFileSystem(),
        )
        scanner = pads.Scanner.from_dataset(
            dataset,
            columns=list(names),
            batch_size=max(1, int(batch_rows)),
            # Two batches/fragments overlap local I/O and decoding without the
            # multi-gigabyte read-ahead observed at Arrow's broad defaults on a
            # wide scan inside a 4 GiB cgroup.
            batch_readahead=2,
            fragment_readahead=2,
            use_threads=True,
        )
        if not scanner.projected_schema.equals(projection_schema):
            # Equality includes field order and physical Arrow types. Do not
            # cast a mismatch; the established path has richer evolution
            # handling and remains the safe fallback.
            return None
        optimized_plan = (
            f"Parquet SCAN [{len(scan_plan.local_paths)} sealed local sources]\n"
            f"PROJECT {len(names)}/{len(scan_plan.physical_schema)} COLUMNS\n"
            "ARROW NATIVE DIRECT PROJECTION"
        )
        if execution_metrics_out is not None:
            execution_metrics_out.add_plan(
                files=len(scan_plan.local_paths),
                row_groups=scan_plan.row_group_count,
                rows=scan_plan.row_count,
            )
        return projection_schema, scanner.to_batches(), optimized_plan

    def _prepare_spilled_stream(
        self,
        reflection: Reflection,
        parser,
        timer_capture,
        log_prefix: str,
        plan: QueryResourcePlan,
        session: SpillSession,
        range_metrics_out: Optional[_QueryRangeMetrics] = None,
        execution_metrics_out: Optional[_QueryExecutionMetrics] = None,
    ) -> Tuple[ArrowBatchStream, str, str]:
        """Compile the sealed direct-column GROUP/ORDER subset to spill ops."""
        root = sqlglot.parse_one(parser.original_query, read="duckdb")
        if (
            not isinstance(root, exp.Select)
            or len(parser.get_table_tuples()) != 1
            or next(root.find_all(exp.Join), None) is not None
        ):
            raise IslandUnsupportedError(
                "bounded native spill currently requires one physical table"
            )

        group = root.args.get("group")
        order = root.args.get("order")
        pre_root = root.copy()
        pre_root.set("order", None)
        aggregate_specs: List[AggregateSpec] = []
        group_names: List[str] = []
        direct_projection_names: Optional[List[str]] = None

        if isinstance(group, exp.Group):
            group_columns = list(group.expressions)
            if not group_columns or not all(
                isinstance(column, exp.Column) for column in group_columns
            ):
                raise IslandUnsupportedError(
                    "spill GROUP BY requires direct columns"
                )
            group_names = [column.name for column in group_columns]
            pre_columns: Dict[str, exp.Column] = {
                column.name: column.copy() for column in group_columns
            }
            seen_group_outputs: set[str] = set()
            for selected in root.expressions:
                core = selected.this if isinstance(selected, exp.Alias) else selected
                output_name = selected.alias_or_name
                if isinstance(core, exp.Column):
                    if core.name not in group_names or output_name != core.name:
                        raise IslandUnsupportedError(
                            "spill GROUP BY requires unaliased grouping columns"
                        )
                    seen_group_outputs.add(core.name)
                    continue
                if not isinstance(core, (exp.Count, exp.Sum, exp.Min, exp.Max)):
                    raise IslandUnsupportedError(
                        "spill GROUP BY projection must be a group column or sealed aggregate"
                    )
                if not isinstance(selected, exp.Alias) or not output_name:
                    raise IslandUnsupportedError(
                        "spill aggregate outputs require explicit aliases"
                    )
                argument = core.this
                if isinstance(core, exp.Count) and isinstance(argument, exp.Star):
                    aggregate_specs.append(AggregateSpec(output_name, "count_star"))
                elif isinstance(argument, exp.Column):
                    pre_columns.setdefault(argument.name, argument.copy())
                    function = {
                        exp.Count: "count", exp.Sum: "sum",
                        exp.Min: "min", exp.Max: "max",
                    }[type(core)]
                    aggregate_specs.append(AggregateSpec(
                        output_name,
                        function,
                        argument.name,
                    ))
                else:
                    raise IslandUnsupportedError(
                        "spill aggregate arguments must be direct columns"
                    )
            if seen_group_outputs != set(group_names):
                raise IslandUnsupportedError(
                    "every GROUP BY column must be projected by the spill subset"
                )
            pre_root.set("expressions", list(pre_columns.values()))
            pre_root.set("group", None)
            direct_projection_names = list(pre_columns)
        elif next(root.find_all(exp.Count, exp.Sum, exp.Min, exp.Max), None):
            raise IslandUnsupportedError(
                "global aggregate state is bounded in memory and is not a spill shape"
            )
        else:
            # A sealed ORDER-only scan can bypass the redundant
            # Parquet -> Polars -> Arrow conversion too, but only when the
            # projection is a literal list of unaliased physical columns.
            # Computed/aliased/star projections retain the SQL engine path;
            # this scanner is not a second expression implementation.
            projected_columns: List[str] = []
            for selected in root.expressions:
                if (
                    not isinstance(selected, exp.Column)
                    or selected.is_star
                    or selected.alias_or_name != selected.name
                ):
                    break
                projected_columns.append(selected.name)
            else:
                if projected_columns and len(set(projected_columns)) == len(
                    projected_columns
                ):
                    direct_projection_names = projected_columns

        pre_sql = pre_root.sql(dialect="duckdb")
        direct_input = (
            self._direct_local_projection_batches(
                reflection,
                parser,
                root,
                column_names=direct_projection_names,
                batch_rows=plan.batch_rows,
                execution_metrics_out=execution_metrics_out,
            )
            if direct_projection_names is not None else None
        )
        direct_raw_projection = direct_input is not None and not group_names
        if direct_input is not None:
            input_schema, input_batches, input_plan = direct_input
            parser.executing_query = pre_sql
            timer_capture("CREATING_REFLECTION")
            logger.debug(
                "%s[islanddb] executing direct Arrow projection: %s",
                log_prefix,
                pre_sql,
            )
        else:
            lazy_input, _, _, input_plan = self._prepare_lazy_query(
                reflection,
                parser,
                timer_capture,
                log_prefix,
                sql_override=pre_sql,
                batch_rows=plan.batch_rows,
                range_metrics_out=range_metrics_out,
                execution_metrics_out=execution_metrics_out,
            )
            input_schema, input_batches = self._lazy_batches(
                lazy_input, batch_rows=plan.batch_rows,
            )
        stream: ArrowBatchStream
        used_native_range_sort = False
        if group_names:
            stream = external_group_aggregate(
                input_batches,
                schema=input_schema,
                group_keys=group_names,
                aggregates=aggregate_specs,
                session=session,
                memory_budget_bytes=plan.operator_memory_bytes,
                output_batch_rows=max(1, plan.batch_rows),
            )
        else:
            stream = ArrowBatchStream(input_schema, input_batches)

        if isinstance(order, exp.Order):
            sort_keys: List[Tuple[str, str]] = []
            first_sort_column: Optional[exp.Column] = None
            for ordered in order.expressions:
                target = ordered.this if isinstance(ordered, exp.Ordered) else ordered
                if not isinstance(target, exp.Column):
                    stream.close()
                    raise IslandUnsupportedError(
                        "spill ORDER BY requires direct projected columns"
                    )
                if stream.schema.get_field_index(target.name) < 0:
                    stream.close()
                    raise IslandUnsupportedError(
                        f"spill ORDER BY key {target.name!r} is not projected"
                    )
                if first_sort_column is None:
                    first_sort_column = target
                direction = (
                    "descending"
                    if isinstance(ordered, exp.Ordered) and ordered.args.get("desc")
                    else "ascending"
                )
                sort_keys.append((target.name, direction))
            first_key_range: Optional[IntegerDomainBound] = None
            input_size_hint_bytes: Optional[int] = None
            input_rows_hint: Optional[int] = None
            if direct_raw_projection and first_sort_column is not None:
                aliases = self._table_maps(parser, reflection)
                domain = self._sealed_integer_column_domain(
                    first_sort_column, aliases,
                )
                first_index = stream.schema.get_field_index(
                    first_sort_column.name,
                )
                if (
                    domain is not None
                    and domain.minimum is not None
                    and first_index >= 0
                    and pa.types.is_integer(stream.schema.field(first_index).type)
                    and bool(getattr(reflection, "decoded_bytes_complete", False))
                    and all(
                        bool(getattr(snapshot, "candidate_rows_complete", False))
                        for snapshot in reflection.supers
                    )
                ):
                    decoded_hint = int(
                        max(0, getattr(reflection, "decoded_bytes", 0) or 0)
                    )
                    row_hint = sum(
                        int(max(0, getattr(snapshot, "candidate_rows", 0) or 0))
                        for snapshot in reflection.supers
                    )
                    if decoded_hint > 0:
                        first_key_range = domain
                        input_size_hint_bytes = decoded_hint
                        input_rows_hint = row_hint
                        used_native_range_sort = True
            try:
                stream = external_sort(
                    stream,
                    schema=stream.schema,
                    sort_keys=sort_keys,
                    session=session,
                    memory_budget_bytes=plan.operator_memory_bytes,
                    output_batch_rows=max(1, plan.batch_rows),
                    null_placement="at_end",
                    first_key_range=first_key_range,
                    input_size_hint_bytes=input_size_hint_bytes,
                    input_rows_hint=input_rows_hint,
                    parallelism=max(1, int(plan.cpu_workers)),
                )
            except BaseException:
                stream.close()
                raise
        desired_names = [selected.alias_or_name for selected in root.expressions]
        if (
            any(not name for name in desired_names)
            or len(set(desired_names)) != len(desired_names)
            or any(stream.schema.get_field_index(name) < 0 for name in desired_names)
        ):
            stream.close()
            raise IslandUnsupportedError(
                "spill output cannot prove the SQL projection order"
            )
        if desired_names != stream.schema.names:
            source_stream = stream
            output_schema = pa.schema([
                source_stream.schema.field(
                    source_stream.schema.get_field_index(name),
                )
                for name in desired_names
            ])

            def reorder_batches():
                try:
                    for batch in source_stream:
                        yield batch.select(desired_names)
                finally:
                    source_stream.close()

            stream = ArrowBatchStream(
                output_schema,
                reorder_batches(),
                cancel_event=source_stream.cancel_event,
            )
        if direct_raw_projection and any(
            pa.types.is_fixed_size_binary(field.type)
            or pa.types.is_large_binary(field.type)
            for field in stream.schema
        ):
            # DuckDB exposes Parquet FIXED_LEN_BYTE_ARRAY logical BLOB columns
            # as Arrow Binary. Keep the fixed-width representation internally
            # while sorting (it gives exact constant row-width accounting),
            # then normalise only bounded output batches at the public stream
            # boundary. Values and ordering are unchanged.
            source_stream = stream
            output_schema = pa.schema([
                pa.field(
                    field.name,
                    pa.binary()
                    if (
                        pa.types.is_fixed_size_binary(field.type)
                        or pa.types.is_large_binary(field.type)
                    )
                    else field.type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in source_stream.schema
            ], metadata=source_stream.schema.metadata)

            def canonical_raw_batches():
                try:
                    for batch in source_stream:
                        offsets_cache: Dict[
                            Tuple[int, int], pa.Buffer
                        ] = {}
                        arrays = []
                        for column in batch.columns:
                            if pa.types.is_fixed_size_binary(column.type):
                                arrays.append(_fixed_size_binary_to_binary(
                                    column,
                                    offsets_cache=offsets_cache,
                                ))
                            elif pa.types.is_large_binary(column.type):
                                # Safe narrowing remains delegated to Arrow:
                                # unlike fixed-width input, arbitrary offsets
                                # cannot be reconstructed without proving each
                                # value boundary fits the int32 Binary domain.
                                arrays.append(pc.cast(
                                    column, pa.binary(), safe=True,
                                ))
                            else:
                                arrays.append(column)
                        yield pa.RecordBatch.from_arrays(
                            arrays,
                            schema=output_schema,
                        )
                finally:
                    source_stream.close()

            stream = ArrowBatchStream(
                output_schema,
                canonical_raw_batches(),
                close_callback=source_stream.close,
                cancel_event=source_stream.cancel_event,
            )
        spill_plan = input_plan + "\nEXTERNAL SPILL"
        if used_native_range_sort:
            spill_plan += "\nARROW NATIVE RANGE PARTITION SORT"
        return stream, parser.original_query, spill_plan

    def execute_stream(
        self,
        reflection: Reflection,
        parser,
        query_manager,
        timer_capture,
        log_prefix: str = "",
        engine_config=None,
        cache_metrics=None,
        _defer_reservation_release: bool = False,
        _prepared: Optional[IslandPreparedQuery] = None,
        max_batch_rows: Optional[int] = None,
    ) -> ArrowBatchStream:
        """Execute natively and yield bounded Arrow batches.

        The resource reservation and any cache/spill leases are held until the
        one-shot stream is exhausted or explicitly closed.  This is the safe
        interface for a result larger than the configured collection budget.
        """
        prepare_started = time.perf_counter()
        prepared = _prepared or self.prepare_execution(
            reflection, parser, streaming_result=True,
        )
        prepare_inside_call_ms = (
            time.perf_counter() - prepare_started
        ) * 1000.0
        prepared.capability.require()
        plan = prepared.resource_plan
        if max_batch_rows is not None:
            if isinstance(max_batch_rows, bool) or not isinstance(
                max_batch_rows, int
            ) or max_batch_rows < 1:
                raise ValueError("max_batch_rows must be a positive integer")
            # Public JSON responses account bytes only after a batch reaches
            # Python. Limiting at the native producer prevents several
            # arbitrary-width values from being materialized ahead of that
            # guard. One row remains the indivisible memory lower bound.
            plan = replace(
                plan,
                batch_rows=min(max(1, int(plan.batch_rows)), max_batch_rows),
            )
        if plan.advice in {ExecutionAdvice.ROUTE_DUCKDB, ExecutionAdvice.ROUTE_SPARK}:
            raise IslandUnsupportedError(
                f"bounded IslandDB plan routes to {plan.advice.value}: {plan.reason}"
            )
        query_id = uuid.uuid4().hex
        # The QueryPlanManager is query-private, unlike ``last_profile`` on a
        # reusable IslandDB instance.  Clear and tokenise this handoff before
        # admission so Executor can never publish another query's profile when
        # telemetry finalisation fails or a following query starts promptly.
        try:
            query_manager._island_profile_token = query_id
            query_manager._island_profile = None
        except Exception:
            pass
        admission_started = time.perf_counter()
        admission_timeout = float(max(1, settings.DEFAULT_TIMEOUT_SEC))
        if not _ISLAND_EXECUTION_SLOT.acquire(timeout=admission_timeout):
            raise IslandUnsupportedError(
                "timed out waiting for the process-global IslandDB scan slot"
            )
        if not _ARROW_POOL_LOCK.acquire(timeout=admission_timeout):
            _ISLAND_EXECUTION_SLOT.release()
            raise IslandUnsupportedError(
                "timed out stabilizing the process-global Arrow worker pools"
            )
        slot_lock = threading.Lock()
        slot_held = True

        def release_execution_slot() -> None:
            nonlocal slot_held
            with slot_lock:
                if slot_held:
                    slot_held = False
                    try:
                        _ARROW_POOL_LOCK.release()
                    finally:
                        _ISLAND_EXECUTION_SLOT.release()

        try:
            reservation = self._governor.reserve(
                plan,
                query_id=query_id,
                timeout=admission_timeout,
            )
        except BaseException:
            release_execution_slot()
            raise
        admission_wait_ms = (
            time.perf_counter() - admission_started
        ) * 1000.0
        # Set the compatibility pointer only after the process-global slot is
        # held. A waiting request must not overwrite the active query's pointer
        # while that query persists or annotates its local profile.
        self.last_profile = IslandProfile(
            telemetry_query_id=query_id,
            execution_outcome="telemetry_pending",
        )
        started = time.perf_counter()
        configured_timeout = float(
            settings.SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC
        )
        if not math.isfinite(configured_timeout):
            # NaN/inf are configuration errors, not an opt-out.  Falling back
            # to the documented bound keeps a malformed environment variable
            # from silently disabling the production safety deadline.
            configured_timeout = 300.0
        execution_timeout = (
            configured_timeout
            if configured_timeout > 0
            else None
        )
        deadline_monotonic = (
            _monotonic() + execution_timeout
            if execution_timeout is not None else None
        )
        query_cancel_event = threading.Event()

        def check_execution_deadline() -> None:
            if (
                deadline_monotonic is not None
                and _monotonic() >= deadline_monotonic
            ):
                # The event is shared by every nested Arrow/spill stream.  The
                # active caller performs ordinary stack unwinding; no worker
                # thread or native iterator is closed asynchronously.
                query_cancel_event.set()
                raise IslandExecutionTimeout(
                    f"IslandDB query {query_id!r} timed out after "
                    f"{execution_timeout:g} seconds"
                )

        def execution_timeout_from(exc: BaseException) -> IslandExecutionTimeout:
            return IslandExecutionTimeout(
                f"IslandDB query {query_id!r} timed out after "
                f"{execution_timeout:g} seconds"
            )

        telemetry: Optional[_IslandTelemetry] = None
        try:
            telemetry = _IslandTelemetry()
        except BaseException as exc:
            # Diagnostics are fail-open even under thread/resource exhaustion.
            # The query already owns its reservation and global slots here, so
            # sampler construction must never escape this boundary.
            try:
                logger.debug(
                    "[islanddb] process telemetry could not start: %s", exc,
                )
            except BaseException:
                pass
        telemetry_finish_lock = threading.Lock()
        telemetry_result: Optional[Dict[str, object]] = None

        def finish_telemetry_no_throw() -> Dict[str, object]:
            """Stop process sampling exactly once without affecting a query."""
            nonlocal telemetry_result
            with telemetry_finish_lock:
                if telemetry_result is not None:
                    return dict(telemetry_result)
                unavailable: Dict[str, object] = {
                    "cpu_time_ms": 0.0,
                    "cpu_time_measured": False,
                    "physical_read_bytes": 0,
                    "physical_read_bytes_measured": False,
                    "rss_baseline_bytes": None,
                    "rss_peak_bytes": None,
                    "rss_final_bytes": None,
                    "rss_peak_delta_bytes": None,
                    "rss_retained_delta_bytes": None,
                    "rss_measured": False,
                    "peak_memory_bytes": 0,
                    "rss_scope": "unknown",
                    "peak_memory_scope": "unknown",
                }
                if telemetry is None:
                    telemetry_result = unavailable
                    return dict(unavailable)
                try:
                    measured = telemetry.finish()
                    if isinstance(measured, dict):
                        unavailable.update(measured)
                except BaseException as exc:
                    # Faulty diagnostics must neither mask the active engine
                    # error nor leave their sampler thread running.
                    try:
                        telemetry._stop.set()
                        telemetry._thread.join(timeout=0.1)
                    except BaseException:
                        pass
                    try:
                        logger.debug(
                            "[islanddb] process telemetry unavailable: %s", exc,
                        )
                    except BaseException:
                        pass
                telemetry_result = unavailable
                return dict(unavailable)

        range_cache = None
        query_range_metrics = _QueryRangeMetrics()
        query_execution_metrics = _QueryExecutionMetrics()
        query_execution_metrics.add_phase(
            "prepare_execution_inside_call_ms", prepare_inside_call_ms,
        )
        query_execution_metrics.add_phase(
            "admission_wait_ms", admission_wait_ms,
        )
        session: Optional[SpillSession] = None
        session_close_lock = threading.Lock()
        session_closed = False

        def close_session_once() -> None:
            nonlocal session_closed
            with session_close_lock:
                if session_closed or session is None:
                    return
                session_closed = True
                session.close()

        inner_stream: Optional[ArrowBatchStream] = None
        try:
            # Cache construction can touch directories/provider metadata. Keep
            # it inside the reservation/telemetry cleanup boundary so a bad
            # deployment cannot leak a governor slot or sampler thread.
            phase_started = time.perf_counter()
            range_cache = self._get_range_cache()
            query_execution_metrics.add_phase(
                "range_cache_setup_ms",
                (time.perf_counter() - phase_started) * 1000.0,
            )
            check_execution_deadline()
            timer_capture("CONNECTING")
            phase_started = time.perf_counter()
            if plan.advice == ExecutionAdvice.ISLAND_SPILL:
                if not settings.SUPERTABLE_ISLAND_SPILL_ENABLED:
                    raise IslandUnsupportedError("IslandDB spill is disabled")
                session = SpillSession(
                    self._spill_root,
                    budget_bytes=plan.spill_budget_bytes,
                    min_free_bytes=self._policy.min_spill_free_bytes,
                    query_id=query_id,
                    cancel_event=query_cancel_event,
                    deadline_monotonic=deadline_monotonic,
                    monotonic=_monotonic,
                )
                session.__enter__()
                inner_stream, query, optimized_plan = self._prepare_spilled_stream(
                    reflection,
                    parser,
                    timer_capture,
                    log_prefix,
                    plan,
                    session,
                    range_metrics_out=query_range_metrics,
                    execution_metrics_out=query_execution_metrics,
                )
                check_execution_deadline()
                schema, batches = inner_stream.schema, inner_stream
                query_execution_metrics.add_phase(
                    "spill_pipeline_prepare_or_eager_execution_ms",
                    (time.perf_counter() - phase_started) * 1000.0,
                )
            else:
                lazy_result, _, query, optimized_plan = self._prepare_lazy_query(
                    reflection,
                    parser,
                    timer_capture,
                    log_prefix,
                    batch_rows=plan.batch_rows,
                    range_metrics_out=query_range_metrics,
                    execution_metrics_out=query_execution_metrics,
                )
                check_execution_deadline()
                query_execution_metrics.add_phase(
                    "relation_prepare_and_eager_integrity_ms",
                    (time.perf_counter() - phase_started) * 1000.0,
                )
                first_batch_started = time.perf_counter()
                schema, batches = self._lazy_batches(
                    lazy_result, batch_rows=plan.batch_rows,
                )
                first_batch_ms = (
                    time.perf_counter() - first_batch_started
                ) * 1000.0
                query_execution_metrics.add_phase(
                    "first_batch_acquire_ms", first_batch_ms,
                )
                query_execution_metrics.add_phase(
                    "producer_active_ms", first_batch_ms,
                )
        except IslandSpillError as exc:
            finish_telemetry_no_throw()
            try:
                if inner_stream is not None:
                    inner_stream.close()
                close_session_once()
            finally:
                reservation.release()
                release_execution_slot()
            if isinstance(exc, SpillDeadlineExceeded):
                raise execution_timeout_from(exc) from exc
            raise IslandUnsupportedError(
                f"bounded spill could not be honored: {exc}"
            ) from exc
        except BaseException:
            finish_telemetry_no_throw()
            try:
                if inner_stream is not None:
                    inner_stream.close()
                close_session_once()
            finally:
                reservation.release()
                release_execution_slot()
            raise

        result_rows = 0
        result_batches = 0
        result_bytes = 0
        stream_ready_at = time.perf_counter()
        stream_closed_at: Optional[float] = None

        def measured_batches():
            nonlocal result_rows, result_batches, result_bytes
            batch_iterator = iter(batches)
            try:
                while True:
                    check_execution_deadline()
                    producer_started = time.perf_counter()
                    try:
                        batch = next(batch_iterator)
                    except StopIteration:
                        break
                    except SpillDeadlineExceeded as exc:
                        raise execution_timeout_from(exc) from exc
                    finally:
                        query_execution_metrics.add_phase(
                            "producer_active_ms",
                            (time.perf_counter() - producer_started) * 1000.0,
                        )
                    # Native calls are cooperative rather than force-killed.
                    # Check immediately after each call so a batch completed
                    # after the deadline is never exposed to the client.
                    check_execution_deadline()
                    result_rows += int(batch.num_rows)
                    result_batches += 1
                    result_bytes += int(batch.nbytes)
                    yield batch
            except GeneratorExit:
                if query_cancel_event.is_set():
                    query_execution_metrics.mark_cancelled()
                else:
                    query_execution_metrics.mark_closed_early()
                raise
            except IslandExecutionTimeout:
                query_execution_metrics.mark_timed_out()
                raise
            except BaseException:
                query_execution_metrics.mark_failed()
                raise
            else:
                query_execution_metrics.mark_complete()
            finally:
                close_batches = getattr(batch_iterator, "close", None)
                close_started = time.perf_counter()
                try:
                    if callable(close_batches):
                        close_batches()
                except BaseException:
                    query_execution_metrics.mark_failed()
                    raise
                finally:
                    query_execution_metrics.add_phase(
                        "producer_cleanup_ms",
                        (time.perf_counter() - close_started) * 1000.0,
                    )

        def finish_profile() -> None:
            # Per-reader sinks isolate this query even when the process-wide
            # RangeCache serves concurrent queries. A cumulative before/after
            # delta would incorrectly charge neighboring reads.
            range_metrics = {
                f"range_{name}": int(value)
                for name, value in query_range_metrics.as_dict().items()
            }
            try:
                combined_cache = (
                    cache_metrics.as_dict()
                    if cache_metrics is not None else {}
                )
            except Exception as exc:
                logger.debug("[islanddb] cache telemetry skipped: %s", exc)
                combined_cache = {}
            combined_cache.update(range_metrics)
            finished_at = time.perf_counter()
            engine_finished_at = stream_closed_at or finished_at
            query_execution_metrics.add_phase(
                "engine_setup_to_stream_ready_ms",
                (stream_ready_at - started) * 1000.0,
            )
            if stream_closed_at is not None:
                query_execution_metrics.add_phase(
                    "stream_lifetime_ms",
                    (stream_closed_at - stream_ready_at) * 1000.0,
                )
            query_execution_metrics.add_phase(
                "engine_elapsed_excluding_profile_persist_ms",
                (engine_finished_at - started) * 1000.0,
            )
            query_execution_metrics.add_phase(
                "total_execution_and_facade_excluding_profile_persist_ms",
                (finished_at - started) * 1000.0,
            )
            plan_metrics = query_execution_metrics.snapshot()
            execution_metrics = finish_telemetry_no_throw()
            scan_complete = bool(
                getattr(reflection, "row_group_scan_bytes_complete", False)
            )
            snapshots = tuple(getattr(reflection, "supers", ()) or ())
            estimated_candidate_files = max(
                0, int(getattr(reflection, "total_reflections", 0) or 0),
            )
            estimated_candidate_files_complete = bool(snapshots) and (
                estimated_candidate_files
                == sum(len(getattr(snapshot, "files", ()) or ()) for snapshot in snapshots)
            )
            estimated_candidate_row_groups_complete = bool(snapshots) and all(
                bool(getattr(snapshot, "candidate_row_groups_complete", False))
                for snapshot in snapshots
            )
            estimated_candidate_row_groups = (
                sum(
                    max(
                        0,
                        int(getattr(snapshot, "candidate_row_groups", 0) or 0),
                    )
                    for snapshot in snapshots
                )
                if estimated_candidate_row_groups_complete else 0
            )
            rows_complete = bool(snapshots) and all(
                bool(getattr(snapshot, "candidate_rows_complete", False))
                for snapshot in snapshots
            )
            estimated_candidate_rows = (
                sum(
                    max(0, int(getattr(snapshot, "candidate_rows", 0) or 0))
                    for snapshot in snapshots
                )
                if rows_complete else 0
            )
            decoded_complete = bool(
                getattr(reflection, "decoded_bytes_complete", False)
            )
            spill_peak = int(session.peak_used_bytes) if session is not None else 0
            profile = IslandProfile(
                telemetry_query_id=query_id,
                source_bytes=int(getattr(reflection, "source_bytes", 0) or 0),
                estimated_scan_bytes=int(
                    getattr(reflection, "row_group_scan_bytes", 0)
                    or reflection.reflection_bytes
                ),
                files=int(reflection.total_reflections),
                elapsed_ms=(engine_finished_at - started) * 1000.0,
                optimized_plan=optimized_plan,
                cache=combined_cache,
                resources={
                    **asdict(plan),
                    "advice": plan.advice.value,
                    "container_cpus": self._resources.cpu_count,
                    "container_memory_bytes": self._resources.memory_limit_bytes,
                },
                spill=(
                    {
                        "triggered": True,
                        "budget_bytes": plan.spill_budget_bytes,
                        "estimated_bytes": plan.estimated_spill_bytes,
                        "directory": str(self._spill_root),
                    }
                    if plan.advice == ExecutionAdvice.ISLAND_SPILL else
                    {"triggered": False}
                ),
                estimated_candidate_files=estimated_candidate_files,
                estimated_candidate_files_complete=(
                    estimated_candidate_files_complete
                ),
                estimated_candidate_row_groups=estimated_candidate_row_groups,
                estimated_candidate_row_groups_complete=(
                    estimated_candidate_row_groups_complete
                ),
                planned_files=int(plan_metrics["planned_files"]),
                planned_files_complete=bool(plan_metrics["planned_complete"]),
                planned_row_groups=int(plan_metrics["planned_row_groups"]),
                planned_row_groups_complete=bool(
                    plan_metrics["planned_complete"]
                ),
                planned_rows=int(plan_metrics["planned_rows"]),
                planned_rows_complete=bool(plan_metrics["planned_complete"]),
                # Compatibility alias: unlike the former estimator-hint
                # counter, full scans now report their physical planned RGs.
                selected_row_groups=(
                    int(plan_metrics["planned_row_groups"])
                    if bool(plan_metrics["planned_complete"]) else 0
                ),
                cpu_time_ms=float(execution_metrics["cpu_time_ms"]),
                cpu_time_measured=bool(
                    execution_metrics["cpu_time_measured"]
                ),
                cpu_time_scope=(
                    "process_cpu_delta_after_admission_until_profile_finalize"
                    if bool(execution_metrics["cpu_time_measured"])
                    else "unavailable"
                ),
                estimated_logical_scan_bytes=int(
                    getattr(reflection, "row_group_scan_bytes", 0) or 0
                ),
                estimated_logical_scan_bytes_complete=scan_complete,
                logical_scan_bytes=int(
                    getattr(reflection, "row_group_scan_bytes", 0) or 0
                ),
                logical_scan_bytes_complete=scan_complete,
                physical_read_bytes=int(execution_metrics["physical_read_bytes"]),
                physical_read_bytes_measured=bool(
                    execution_metrics["physical_read_bytes_measured"]
                ),
                physical_read_scope=(
                    "linux_proc_self_io_block_read_delta_after_admission_"
                    "until_profile_finalize"
                    if bool(execution_metrics["physical_read_bytes_measured"])
                    else "unavailable"
                ),
                estimated_decoded_bytes=int(
                    getattr(reflection, "decoded_bytes", 0) or 0
                ),
                estimated_decoded_bytes_complete=decoded_complete,
                decoded_bytes=int(getattr(reflection, "decoded_bytes", 0) or 0),
                decoded_bytes_complete=decoded_complete,
                estimated_candidate_rows=estimated_candidate_rows,
                estimated_candidate_rows_complete=rows_complete,
                # No native scanner counter is available across every path.
                # Do not relabel the estimator upper bound as a measurement.
                rows_scanned=0,
                rows_scanned_measured=False,
                execution_outcome=str(plan_metrics["execution_outcome"]),
                result_complete=bool(plan_metrics["result_complete"]),
                result_rows=result_rows,
                result_batches=result_batches,
                result_bytes=result_bytes,
                rss_baseline_bytes=execution_metrics["rss_baseline_bytes"],
                rss_peak_bytes=execution_metrics["rss_peak_bytes"],
                rss_final_bytes=execution_metrics["rss_final_bytes"],
                rss_peak_delta_bytes=execution_metrics[
                    "rss_peak_delta_bytes"
                ],
                rss_retained_delta_bytes=execution_metrics[
                    "rss_retained_delta_bytes"
                ],
                rss_measured=bool(execution_metrics["rss_measured"]),
                rss_scope=str(execution_metrics["rss_scope"]),
                peak_memory_bytes=int(execution_metrics["peak_memory_bytes"]),
                peak_memory_scope=str(execution_metrics["peak_memory_scope"]),
                # Preserve the historical/native engine boundary used by AUTO
                # feedback. Facade conversion is measured separately below so
                # old/new Island samples and DuckDB engine latency remain
                # comparable rather than silently changing semantics.
                elapsed_scope=(
                    "engine_after_admission_through_stream_close_"
                    "excludes_facade_and_profile_persist"
                ),
                phase_timings_ms=dict(plan_metrics["phase_timings_ms"]),
                spill_bytes=spill_peak,
                spill_bytes_measured=session is not None,
                spill_occurred=session is not None and spill_peak > 0,
            )
            # Query-local ownership is authoritative. ``last_profile`` remains
            # a compatibility pointer only and is never dereferenced below.
            self.last_profile = profile
            try:
                query_manager._island_profile = profile
            except Exception:
                pass
            # The persisted payload honestly marks its own current atomic-write
            # duration unavailable. Update only the in-memory profile after the
            # single commit returns; double-writing would make both timings
            # self-referential and would distort short-query benchmarks.
            persist_started = time.perf_counter()
            persist_succeeded: Optional[bool] = None
            try:
                persist_result = self._write_profile(
                    query_manager.query_plan_path, profile,
                )
                if (
                    isinstance(persist_result, tuple)
                    and len(persist_result) == 2
                ):
                    persist_succeeded = bool(persist_result[1])
            except Exception as exc:
                # Accommodate instrumentation monkeypatches as defensively as
                # the production writer: telemetry can never fail a query.
                logger.debug("[islanddb] profile persistence failed: %s", exc)
                persist_succeeded = False
            persist_ms = (time.perf_counter() - persist_started) * 1000.0
            profile.profile_persist_ms = persist_ms
            profile.profile_persist_ms_measured = True
            profile.profile_persist_succeeded = persist_succeeded
            profile.phase_timings_ms["profile_persist_ms"] = persist_ms
            self.last_profile = profile

        finish_lock = threading.Lock()
        finish_started = False

        def finish() -> None:
            nonlocal finish_started
            with finish_lock:
                if finish_started:
                    return
                finish_started = True
            if query_cancel_event.is_set():
                query_execution_metrics.mark_cancelled()
            else:
                # Covers explicit close before first next(), when Python does
                # not enter the unstarted measured_batches generator at all.
                query_execution_metrics.mark_closed_early()
            try:
                try:
                    finish_profile()
                except Exception as exc:
                    # Observability must not turn a successfully produced query
                    # into an application error or mask the original producer
                    # failure during ArrowBatchStream finalization.
                    logger.debug("[islanddb] profile finalization skipped: %s", exc)
            finally:
                try:
                    # Ensure a profile-construction failure cannot strand the
                    # process sampler. The call is cached after a normal
                    # finish_profile() path.
                    finish_telemetry_no_throw()
                finally:
                    try:
                        # Profile persistence is diagnostic and must never pin
                        # a query-private spill directory when it fails.
                        close_session_once()
                    finally:
                        if not _defer_reservation_release:
                            reservation.release()
                            release_execution_slot()

        def stream_closed() -> None:
            nonlocal stream_closed_at
            if stream_closed_at is None:
                stream_closed_at = time.perf_counter()
            if query_cancel_event.is_set():
                query_execution_metrics.mark_cancelled()
            else:
                query_execution_metrics.mark_closed_early()
            if _defer_reservation_release:
                # Preserve the pre-telemetry lifecycle: materialized queries
                # release query-private spill files as soon as the Arrow stream
                # closes, before facade conversion. Only the governor/slot stay
                # held through the existing materialization boundary.
                close_session_once()
            else:
                finish()

        stream = ArrowBatchStream(
            schema,
            measured_batches(),
            close_callback=stream_closed,
            cancel_event=query_cancel_event,
        )
        if _defer_reservation_release:
            # Private handoff used only by the pandas facade so Arrow -> Polars
            # -> pandas conversion remains inside the same governor reservation.
            def release_deferred_resources() -> None:
                try:
                    reservation.release()
                finally:
                    release_execution_slot()

            stream._island_release_reservation = release_deferred_resources
            stream._island_finish_profile = finish
            stream._island_execution_metrics = query_execution_metrics
        return stream

    def execute(
        self,
        reflection: Reflection,
        parser,
        query_manager,
        timer_capture,
        log_prefix: str = "",
        engine_config=None,
        cache_metrics=None,
        _prepared: Optional[IslandPreparedQuery] = None,
    ) -> pd.DataFrame:
        prepared = _prepared or self.prepare_execution(
            reflection, parser, streaming_result=False,
        )
        prepared.capability.require()
        plan = prepared.resource_plan
        if plan.advice == ExecutionAdvice.STREAM_RESULT:
            raise ResultMemoryLimitExceeded(
                f"{plan.reason}; call IslandDB.execute_stream()"
            )
        stream = self.execute_stream(
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer_capture=timer_capture,
            log_prefix=log_prefix,
            engine_config=engine_config,
            cache_metrics=cache_metrics,
            _defer_reservation_release=True,
            _prepared=prepared,
        )
        release = getattr(stream, "_island_release_reservation", lambda: None)
        finish_profile = getattr(stream, "_island_finish_profile", lambda: None)
        execution_metrics = getattr(stream, "_island_execution_metrics", None)
        facade_started = time.perf_counter()
        try:
            collect_started = time.perf_counter()
            try:
                with stream:
                    table = stream.collect_table(max_bytes=plan.result_memory_bytes)
            finally:
                if execution_metrics is not None:
                    execution_metrics.add_phase(
                        "facade_collect_arrow_table_ms",
                        (time.perf_counter() - collect_started) * 1000.0,
                    )
            convert_started = time.perf_counter()
            try:
                result = pl.from_arrow(table)
            finally:
                if execution_metrics is not None:
                    execution_metrics.add_phase(
                        "facade_arrow_to_polars_ms",
                        (time.perf_counter() - convert_started) * 1000.0,
                    )
            root = self._query_root(parser)
            normalize_started = time.perf_counter()
            if isinstance(root, exp.Select):
                result = self._normalize_aggregate_dtypes(result, root)
            if execution_metrics is not None:
                execution_metrics.add_phase(
                    "facade_dtype_normalize_ms",
                    (time.perf_counter() - normalize_started) * 1000.0,
                )
            pandas_started = time.perf_counter()
            try:
                pandas_result = self._to_duckdb_pandas(result)
            finally:
                if execution_metrics is not None:
                    execution_metrics.add_phase(
                        "facade_polars_to_pandas_ms",
                        (time.perf_counter() - pandas_started) * 1000.0,
                    )
            return pandas_result
        except BaseException:
            if execution_metrics is not None:
                execution_metrics.mark_facade_failed()
            raise
        finally:
            if execution_metrics is not None:
                execution_metrics.add_phase(
                    "facade_total_ms",
                    (time.perf_counter() - facade_started) * 1000.0,
                )
            try:
                finish_profile()
            finally:
                release()


__all__ = [
    "IslandDB", "IslandCapability", "IslandPreparedQuery", "IslandProfile",
    "IslandUnsupportedError", "IslandExecutionTimeout", "IslandIntegrityError",
    "ArrowBatchStream",
]
