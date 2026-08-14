"""Container-aware resource planning for IslandDB.

This module intentionally does not execute SQL.  It turns a conservative
working-set estimate into explicit CPU, I/O, memory, result, and spill
budgets, and arbitrates those budgets across concurrent queries.  Callers
must not exceed a reservation and must route away when ``advice`` is not an
IslandDB execution mode.
"""

from __future__ import annotations

import math
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional, Sequence

import pyarrow as pa


MIB = 1024 * 1024
GIB = 1024 * MIB


class IslandResourceError(RuntimeError):
    """Base class for a resource decision that cannot be honored safely."""


class ResourceReservationTimeout(IslandResourceError):
    """Raised when a query cannot obtain its bounded reservation in time."""


class ResourceReservationCancelled(IslandResourceError):
    """Raised when cancellation is requested while waiting or streaming."""


class ResultMemoryLimitExceeded(IslandResourceError):
    """Raised before an Arrow result stream would exceed its collection cap."""


class ExecutionAdvice(str, Enum):
    ISLAND_IN_MEMORY = "island_in_memory"
    ISLAND_SPILL = "island_spill"
    STREAM_RESULT = "stream_result"
    ROUTE_DUCKDB = "route_duckdb"
    ROUTE_SPARK = "route_spark"


def _positive_min(values: Iterable[Optional[int]], default: int) -> int:
    usable = [int(value) for value in values if value is not None and value > 0]
    return min(usable) if usable else int(default)


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
        return None


def parse_cpuset(value: str) -> tuple[int, ...]:
    """Parse Linux cpuset syntax (for example ``0-2,7``) strictly."""
    cpus: set[int] = set()
    text = str(value or "").strip()
    if not text:
        return ()
    for raw_part in text.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError(f"invalid empty cpuset component in {value!r}")
        if "-" in part:
            pieces = part.split("-")
            if len(pieces) != 2:
                raise ValueError(f"invalid cpuset range {part!r}")
            start, end = (int(piece) for piece in pieces)
            if start < 0 or end < start:
                raise ValueError(f"invalid cpuset range {part!r}")
            cpus.update(range(start, end + 1))
        else:
            cpu = int(part)
            if cpu < 0:
                raise ValueError(f"invalid cpuset CPU {cpu}")
            cpus.add(cpu)
    return tuple(sorted(cpus))


def _host_memory_bytes() -> int:
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        return GIB


def _mem_available(proc_root: Path) -> Optional[int]:
    text = _read_text(proc_root / "meminfo")
    if text is None:
        return None
    for line in text.splitlines():
        if line.startswith("MemAvailable:"):
            fields = line.split()
            if len(fields) >= 2:
                try:
                    return int(fields[1]) * 1024
                except ValueError:
                    return None
    return None


def _default_cgroup_dir(cgroup_root: Path, proc_root: Path) -> Path:
    """Resolve this process' unified cgroup, falling back to the mount root."""
    text = _read_text(proc_root / "self" / "cgroup")
    if text:
        for line in text.splitlines():
            if line.startswith("0::"):
                relative = line[3:].lstrip("/")
                return cgroup_root / relative
    return cgroup_root


@dataclass(frozen=True)
class ContainerResources:
    """Effective limits after intersecting host, affinity, and cgroup v2."""

    cpu_count: int
    cpu_capacity: float
    affinity_cpus: tuple[int, ...]
    cpuset_cpus: tuple[int, ...]
    memory_limit_bytes: int
    memory_available_bytes: int

    @classmethod
    def detect(
        cls,
        *,
        cgroup_root: Path | str = "/sys/fs/cgroup",
        proc_root: Path | str = "/proc",
        cgroup_dir: Path | str | None = None,
        affinity: Sequence[int] | None = None,
        host_memory_bytes: int | None = None,
        host_available_bytes: int | None = None,
    ) -> "ContainerResources":
        cgroup_root = Path(cgroup_root)
        proc_root = Path(proc_root)
        cg = Path(cgroup_dir) if cgroup_dir is not None else _default_cgroup_dir(cgroup_root, proc_root)

        if affinity is None:
            try:
                affinity_tuple = tuple(sorted(os.sched_getaffinity(0)))
            except (AttributeError, OSError):
                affinity_tuple = tuple(range(max(1, os.cpu_count() or 1)))
        else:
            affinity_tuple = tuple(sorted(set(int(cpu) for cpu in affinity)))
        if not affinity_tuple:
            affinity_tuple = (0,)

        cpuset_text = _read_text(cg / "cpuset.cpus.effective")
        if cpuset_text is None:
            cpuset_text = _read_text(cg / "cpuset.cpus")
        try:
            cpuset = parse_cpuset(cpuset_text or "")
        except (TypeError, ValueError):
            # A malformed kernel/control-plane file must reduce optimization,
            # not make query execution unavailable.
            cpuset = ()

        cpu_ceiling = len(affinity_tuple)
        if cpuset:
            intersection = set(affinity_tuple).intersection(cpuset)
            cpu_ceiling = len(intersection) if intersection else min(cpu_ceiling, len(cpuset))
        cpu_ceiling = max(1, cpu_ceiling)

        quota_capacity: Optional[float] = None
        cpu_max = _read_text(cg / "cpu.max")
        if cpu_max:
            fields = cpu_max.split()
            if len(fields) >= 2 and fields[0] != "max":
                try:
                    quota = int(fields[0])
                    period = int(fields[1])
                    if quota > 0 and period > 0:
                        quota_capacity = quota / period
                except ValueError:
                    quota_capacity = None
        cpu_capacity = min(float(cpu_ceiling), quota_capacity) if quota_capacity else float(cpu_ceiling)
        cpu_capacity = max(0.01, cpu_capacity)
        cpu_count = max(1, min(cpu_ceiling, int(math.floor(cpu_capacity))))

        host_total = int(
            _host_memory_bytes()
            if host_memory_bytes is None
            else host_memory_bytes
        )
        detected_available = _mem_available(proc_root)
        host_available = int(
            detected_available if host_available_bytes is None else host_available_bytes
        ) if (
            host_available_bytes is not None or detected_available is not None
        ) else host_total

        memory_max_text = _read_text(cg / "memory.max")
        cgroup_limit: Optional[int] = None
        if memory_max_text and memory_max_text != "max":
            try:
                parsed = int(memory_max_text)
                if parsed > 0:
                    cgroup_limit = parsed
            except ValueError:
                pass
        memory_limit = _positive_min((host_total, cgroup_limit), host_total)

        cgroup_available: Optional[int] = None
        if cgroup_limit is not None:
            current_text = _read_text(cg / "memory.current")
            try:
                current = max(0, int(current_text)) if current_text is not None else 0
                cgroup_available = max(0, cgroup_limit - current)
            except ValueError:
                cgroup_available = cgroup_limit
        available_candidates = [max(0, host_available)]
        if cgroup_available is not None:
            available_candidates.append(max(0, cgroup_available))
        memory_available = min(available_candidates) if available_candidates else memory_limit
        memory_available = min(memory_available, memory_limit)

        return cls(
            cpu_count=cpu_count,
            cpu_capacity=cpu_capacity,
            affinity_cpus=affinity_tuple,
            cpuset_cpus=cpuset,
            memory_limit_bytes=memory_limit,
            memory_available_bytes=memory_available,
        )


@dataclass(frozen=True)
class QueryResourceEstimate:
    compressed_scan_bytes: int
    decoded_scan_bytes: int
    result_bytes: int
    operator_state_bytes: int = 0
    selected_files: int = 1
    selected_row_groups: int = 1
    # Input physical rows and conservative output rows are distinct. Joins may
    # multiply output cardinality; scalar aggregates collapse it to one.
    estimated_rows: int = 0
    estimated_result_rows: int = 0
    spillable: bool = True
    has_sort: bool = False
    has_group_by: bool = False
    has_join: bool = False
    estimates_complete: bool = True
    # Trailing compatibility extension: the portion of ``decoded_scan_bytes``
    # belonging to the selected query scan. The total may additionally include
    # a streamed whole-file integrity proof; that work affects routing/CPU but
    # must not inflate bytes per selected row.
    selected_decoded_bytes: int = 0
    selected_decoded_bytes_complete: bool = False
    # A large scan with a provably small integer GROUP domain must still use
    # IslandDB's bounded batchwise aggregator: handing the whole lazy GROUP to
    # Polars can transiently materialize the wide input despite its small final
    # cardinality.  The planner therefore retains the external-operator advice
    # while budgeting only the sealed compact group state/result.
    requires_bounded_group_operator: bool = False
    group_state_bytes_per_key: int = 0

    def __post_init__(self) -> None:
        numeric = (
            self.compressed_scan_bytes,
            self.decoded_scan_bytes,
            self.result_bytes,
            self.operator_state_bytes,
            self.selected_files,
            self.selected_row_groups,
            self.estimated_rows,
            self.estimated_result_rows,
            self.selected_decoded_bytes,
            self.group_state_bytes_per_key,
        )
        if any(value < 0 for value in numeric):
            raise ValueError("resource estimates cannot be negative")
        if self.requires_bounded_group_operator and not self.has_group_by:
            raise ValueError(
                "a bounded group operator requires a GROUP BY estimate"
            )
        if (
            self.requires_bounded_group_operator
            and self.group_state_bytes_per_key <= 0
        ):
            raise ValueError(
                "a bounded group operator requires a positive per-key bound"
            )


@dataclass(frozen=True)
class ResourcePolicy:
    query_memory_fraction: float = 0.60
    global_memory_fraction: float = 0.80
    result_memory_fraction: float = 0.25
    operator_memory_fraction: float = 0.50
    min_query_memory_bytes: int = 32 * MIB
    max_query_memory_bytes: int = 0
    max_result_memory_bytes: int = 512 * MIB
    max_spill_bytes: int = 64 * GIB
    min_spill_free_bytes: int = 512 * MIB
    bytes_per_cpu_worker: int = 64 * MIB
    bytes_per_io_worker: int = 16 * MIB
    max_io_workers: int = 16
    min_batch_bytes: int = 1 * MIB
    max_batch_bytes: int = 32 * MIB
    spill_amplification: float = 2.25
    spark_spill_threshold_bytes: int = 0

    def __post_init__(self) -> None:
        fractions = (
            self.query_memory_fraction,
            self.global_memory_fraction,
            self.result_memory_fraction,
            self.operator_memory_fraction,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value <= 0
            or value > 1
            for value in fractions
        ):
            raise ValueError("resource fractions must be in (0, 1]")
        if self.result_memory_fraction + self.operator_memory_fraction >= 1:
            raise ValueError("result and operator fractions must leave scan memory")
        if (
            isinstance(self.spill_amplification, bool)
            or not isinstance(self.spill_amplification, (int, float))
            or not math.isfinite(float(self.spill_amplification))
            or self.spill_amplification < 1
        ):
            raise ValueError("spill amplification must be at least one")


@dataclass(frozen=True)
class QueryResourcePlan:
    advice: ExecutionAdvice
    cpu_workers: int
    io_workers: int
    batch_bytes: int
    batch_rows: int
    memory_budget_bytes: int
    scan_memory_bytes: int
    operator_memory_bytes: int
    result_memory_bytes: int
    spill_budget_bytes: int
    estimated_spill_bytes: int
    reason: str

    @property
    def runs_on_island(self) -> bool:
        return self.advice in {
            ExecutionAdvice.ISLAND_IN_MEMORY,
            ExecutionAdvice.ISLAND_SPILL,
        }


class ResourcePlanner:
    """Calculate a bounded query plan from conservative decoded estimates."""

    def __init__(
        self,
        resources: ContainerResources,
        *,
        spill_root: Path | str,
        policy: ResourcePolicy | None = None,
        disk_usage: Callable[[str | os.PathLike[str]], object] = shutil.disk_usage,
    ):
        self.resources = resources
        self.spill_root = Path(spill_root)
        self.policy = policy or ResourcePolicy()
        self._disk_usage = disk_usage

    def _disk_free(self) -> int:
        candidate = self.spill_root
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        return int(self._disk_usage(candidate).free)

    def plan(self, estimate: QueryResourceEstimate, *, streaming_result: bool = False) -> QueryResourcePlan:
        policy = self.policy
        available = min(self.resources.memory_available_bytes, self.resources.memory_limit_bytes)
        # A plan that is larger than the shared governor can never be admitted.
        # Keep unusual-but-valid configuration (query fraction > global
        # fraction) fail-safe instead of selecting Island and then raising at
        # reservation time.
        memory_budget = min(
            int(available * policy.query_memory_fraction),
            int(available * policy.global_memory_fraction),
        )
        if policy.max_query_memory_bytes > 0:
            memory_budget = min(memory_budget, policy.max_query_memory_bytes)
        if memory_budget < policy.min_query_memory_bytes:
            return QueryResourcePlan(
                advice=ExecutionAdvice.ROUTE_DUCKDB,
                cpu_workers=1,
                io_workers=1,
                batch_bytes=0,
                batch_rows=0,
                memory_budget_bytes=max(0, memory_budget),
                scan_memory_bytes=0,
                operator_memory_bytes=0,
                result_memory_bytes=0,
                spill_budget_bytes=0,
                estimated_spill_bytes=0,
                reason="available memory is below IslandDB's minimum bounded workspace",
            )

        result_memory = min(
            int(memory_budget * policy.result_memory_fraction),
            policy.max_result_memory_bytes,
        )
        operator_memory = int(memory_budget * policy.operator_memory_fraction)
        scan_memory = memory_budget - result_memory - operator_memory

        # Polars and Arrow own process-global worker pools; they cannot be
        # reduced safely for one query while unrelated queries are running.
        # Reserve their worst-case CPU width so the governor never admits
        # several queries on the fiction that each will use only the planner's
        # estimated subset.  This intentionally serializes in-process IslandDB
        # scans at the CPU admission boundary.  Hard per-query CPU isolation
        # would require a separately initialized worker process/cgroup.
        cpu_workers = max(1, self.resources.cpu_count)

        io_workers = max(
            1,
            min(policy.max_io_workers, self.resources.cpu_count * 2),
        )
        parallel_slots = max(1, cpu_workers + io_workers)
        per_slot_memory = max(1, scan_memory // parallel_slots)
        # min_batch_bytes is a throughput target, never authority to exceed the
        # scan workspace when many CPU/I/O slots share a small budget.
        batch_bytes = min(
            policy.max_batch_bytes,
            max(policy.min_batch_bytes, per_slot_memory),
            per_slot_memory,
        )
        batch_decoded_bytes = (
            estimate.selected_decoded_bytes
            if estimate.selected_decoded_bytes_complete
            else estimate.decoded_scan_bytes
        )
        if estimate.estimated_rows > 0 and batch_decoded_bytes > 0:
            # Bound both source and output batches. A query projecting one input
            # column into hundreds of distinct aliases can expand each output
            # row far beyond its scan width; joins can similarly multiply rows.
            input_width = (
                batch_decoded_bytes + estimate.estimated_rows - 1
            ) // estimate.estimated_rows
            output_width = (
                (
                    estimate.result_bytes
                    + estimate.estimated_result_rows - 1
                ) // estimate.estimated_result_rows
                if estimate.estimated_result_rows > 0
                else input_width
            )
            row_width = max(1, input_width, output_width)
            batch_rows = max(1, batch_bytes // row_width)
        else:
            batch_rows = 64 * 1024

        if not estimate.estimates_complete:
            return QueryResourcePlan(
                advice=ExecutionAdvice.ROUTE_DUCKDB,
                cpu_workers=cpu_workers,
                io_workers=io_workers,
                batch_bytes=batch_bytes,
                batch_rows=batch_rows,
                memory_budget_bytes=memory_budget,
                scan_memory_bytes=scan_memory,
                operator_memory_bytes=operator_memory,
                result_memory_bytes=result_memory,
                spill_budget_bytes=0,
                estimated_spill_bytes=0,
                reason="decoded/result estimates are incomplete",
            )

        oversized_result = estimate.result_bytes > result_memory
        state_excess = max(0, estimate.operator_state_bytes - operator_memory)
        # external_group_aggregate retains at most one quarter of its operator
        # workspace as live hash state, reserving the remainder for Arrow
        # conversion/merge transients. A sealed full-domain estimate that only
        # fits the larger admission budget can still flush on every batch.
        bounded_group_retained = bool(
            estimate.requires_bounded_group_operator
            and estimate.operator_state_bytes <= max(1, operator_memory // 4)
        )
        spill_basis = state_excess
        if bounded_group_retained:
            # Partial aggregation reduces every input batch before retaining
            # state.  The snapshot-sealed domain already bounds the complete
            # compact hash table; charge that full bound (plus the separately
            # bounded grouped result/order state) rather than rewriting the
            # decoded source.  Amplification below leaves room for an IPC run
            # and old+new overlap if the compact fallback does spill.
            spill_basis = max(
                spill_basis,
                estimate.operator_state_bytes,
                estimate.result_bytes,
            )
        elif estimate.requires_bounded_group_operator:
            # When the complete sealed domain does not fit the retained hash
            # target, the same key may recur in every input batch and therefore
            # in many partial runs. One domain-sized table is not a spill quota
            # proof. Charge the worst case of one compact state occurrence per
            # selected row; the normal disk-cap gate below may route it away.
            partial_occurrence_bytes = (
                estimate.estimated_rows * estimate.group_state_bytes_per_key
            )
            spill_basis = max(
                spill_basis,
                estimate.decoded_scan_bytes,
                estimate.operator_state_bytes,
                estimate.result_bytes,
                partial_occurrence_bytes,
            )
        elif estimate.has_sort or estimate.has_group_by:
            # External sort/group writes the full decoded input before merging,
            # regardless of how little the blocking state exceeds memory. A
            # high-cardinality group or many duplicate aggregate outputs can
            # also make compact state/result wider than its narrow source, so
            # neither streaming output nor compression may hide that quota.
            # Amplification covers run framing and old+new multipass overlap.
            spill_basis = max(
                spill_basis,
                estimate.decoded_scan_bytes,
                estimate.operator_state_bytes,
                estimate.result_bytes,
            )
        amplification_numerator, amplification_denominator = float(
            policy.spill_amplification
        ).as_integer_ratio()
        estimated_spill = (
            spill_basis * amplification_numerator
            + amplification_denominator - 1
        ) // amplification_denominator

        if oversized_result and not streaming_result:
            return QueryResourcePlan(
                advice=ExecutionAdvice.STREAM_RESULT,
                cpu_workers=cpu_workers,
                io_workers=io_workers,
                batch_bytes=batch_bytes,
                batch_rows=batch_rows,
                memory_budget_bytes=memory_budget,
                scan_memory_bytes=scan_memory,
                operator_memory_bytes=operator_memory,
                result_memory_bytes=result_memory,
                spill_budget_bytes=0,
                estimated_spill_bytes=estimated_spill,
                reason="estimated result exceeds bounded collection memory; use streaming output",
            )

        if state_excess == 0 and not estimate.requires_bounded_group_operator:
            advice = ExecutionAdvice.ISLAND_IN_MEMORY
            return QueryResourcePlan(
                advice=advice,
                cpu_workers=cpu_workers,
                io_workers=io_workers,
                batch_bytes=batch_bytes,
                batch_rows=batch_rows,
                memory_budget_bytes=memory_budget,
                scan_memory_bytes=scan_memory,
                operator_memory_bytes=operator_memory,
                result_memory_bytes=result_memory,
                spill_budget_bytes=0,
                estimated_spill_bytes=0,
                reason=(
                    "working state fits and the result will be streamed"
                    if oversized_result
                    else "working state fits the bounded in-memory plan"
                ),
            )

        if not estimate.spillable:
            advice = ExecutionAdvice.ROUTE_SPARK if estimate.has_join else ExecutionAdvice.ROUTE_DUCKDB
            return QueryResourcePlan(
                advice=advice,
                cpu_workers=cpu_workers,
                io_workers=io_workers,
                batch_bytes=batch_bytes,
                batch_rows=batch_rows,
                memory_budget_bytes=memory_budget,
                scan_memory_bytes=scan_memory,
                operator_memory_bytes=operator_memory,
                result_memory_bytes=result_memory,
                spill_budget_bytes=0,
                estimated_spill_bytes=estimated_spill,
                reason="operator state exceeds memory and the plan has no bounded spill implementation",
            )

        free_for_spill = max(0, self._disk_free() - policy.min_spill_free_bytes)
        spill_budget = min(policy.max_spill_bytes, free_for_spill)
        if estimated_spill > spill_budget:
            advice = (
                ExecutionAdvice.ROUTE_SPARK
                if policy.spark_spill_threshold_bytes == 0
                or estimated_spill >= policy.spark_spill_threshold_bytes
                else ExecutionAdvice.ROUTE_DUCKDB
            )
            return QueryResourcePlan(
                advice=advice,
                cpu_workers=cpu_workers,
                io_workers=io_workers,
                batch_bytes=batch_bytes,
                batch_rows=batch_rows,
                memory_budget_bytes=memory_budget,
                scan_memory_bytes=scan_memory,
                operator_memory_bytes=operator_memory,
                result_memory_bytes=result_memory,
                spill_budget_bytes=spill_budget,
                estimated_spill_bytes=estimated_spill,
                reason="the bounded spill estimate exceeds configured/free disk capacity",
            )

        return QueryResourcePlan(
            advice=ExecutionAdvice.ISLAND_SPILL,
            cpu_workers=cpu_workers,
            io_workers=io_workers,
            batch_bytes=batch_bytes,
            batch_rows=batch_rows,
            memory_budget_bytes=memory_budget,
            scan_memory_bytes=scan_memory,
            operator_memory_bytes=operator_memory,
            result_memory_bytes=result_memory,
            spill_budget_bytes=estimated_spill,
            estimated_spill_bytes=estimated_spill,
            reason=(
                "sealed group cardinality fits the retained bounded "
                "batchwise aggregation state"
                if bounded_group_retained
                else (
                    "sealed group cardinality requires partial runs whose "
                    "conservative quota fits spill capacity"
                    if estimate.requires_bounded_group_operator
                    else "operator state requires and fits a bounded spill plan"
                )
            ),
        )


class ResourceReservation:
    """Idempotent context-managed reservation returned by ResourceGovernor."""

    def __init__(self, governor: "ResourceGovernor", query_id: str, plan: QueryResourcePlan):
        self._governor = governor
        self.query_id = query_id
        self.plan = plan
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._governor._release(self.query_id)

    def __enter__(self) -> "ResourceReservation":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


class ResourceGovernor:
    """Weighted CPU/memory/spill semaphore shared by IslandDB queries."""

    def __init__(
        self,
        resources: ContainerResources,
        *,
        spill_root: Path | str,
        policy: ResourcePolicy | None = None,
        disk_usage: Callable[[str | os.PathLike[str]], object] = shutil.disk_usage,
    ):
        self.resources = resources
        self.spill_root = Path(spill_root)
        self.policy = policy or ResourcePolicy()
        self._disk_usage = disk_usage
        self._condition = threading.Condition()
        self._active: dict[str, tuple[int, int, int]] = {}
        self._cpu_capacity, self._memory_capacity = self._capacities(resources)

    def _capacities(self, resources: ContainerResources) -> tuple[int, int]:
        """Return current hard admission limits without inventing free memory.

        ``min_query_memory_bytes`` is a planner eligibility floor, not memory
        that the process actually owns.  In particular, a cgroup with zero
        available bytes must expose a zero governor capacity so a stale plan
        cannot be admitted during pressure.
        """
        return (
            max(1, int(resources.cpu_count)),
            max(
                0,
                int(
                    min(
                        resources.memory_available_bytes,
                        resources.memory_limit_bytes,
                    )
                    * self.policy.global_memory_fraction
                ),
            ),
        )

    def refresh_resources(self, resources: ContainerResources) -> None:
        """Refresh dynamic availability for a process-shared governor.

        IslandDB instances intentionally share one governor, while available
        cgroup/host memory is sampled for every new instance.  Keeping the
        first sample forever can over-admit concurrent queries after memory
        pressure rises.  A shrink is allowed below existing reservations;
        those leases remain valid, but no new work is admitted until usage is
        back under the refreshed capacity.
        """
        cpu_capacity, memory_capacity = self._capacities(resources)
        with self._condition:
            self.resources = resources
            self._cpu_capacity = cpu_capacity
            self._memory_capacity = memory_capacity
            self._condition.notify_all()

    def _disk_free(self) -> int:
        candidate = self.spill_root
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        return int(self._disk_usage(candidate).free)

    def snapshot(self) -> dict[str, int]:
        with self._condition:
            cpu = sum(item[0] for item in self._active.values())
            memory = sum(item[1] for item in self._active.values())
            spill = sum(item[2] for item in self._active.values())
            return {
                "active_queries": len(self._active),
                "cpu_reserved": cpu,
                "memory_reserved": memory,
                "spill_reserved": spill,
                "cpu_capacity": self._cpu_capacity,
                "memory_capacity": self._memory_capacity,
            }

    def reserve(
        self,
        plan: QueryResourcePlan,
        *,
        query_id: str | None = None,
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ResourceReservation:
        if not plan.runs_on_island:
            raise IslandResourceError(f"cannot reserve non-IslandDB plan: {plan.advice.value}")
        query_id = query_id or uuid.uuid4().hex
        cpu = max(1, plan.cpu_workers)
        memory = plan.memory_budget_bytes
        spill = plan.spill_budget_bytes

        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        with self._condition:
            if query_id in self._active:
                raise IslandResourceError(f"duplicate active query id {query_id!r}")
            while True:
                # Capacity can be refreshed while callers are waiting.  Check
                # under the same lock as admission so a request larger than a
                # newly reduced hard limit fails promptly instead of waiting
                # until its query timeout for an impossible reservation.
                if cpu > self._cpu_capacity or memory > self._memory_capacity:
                    raise IslandResourceError(
                        "query request exceeds the governor's hard capacity"
                    )
                if cancel_event is not None and cancel_event.is_set():
                    raise ResourceReservationCancelled(f"query {query_id!r} was cancelled")
                used_cpu = sum(item[0] for item in self._active.values())
                used_memory = sum(item[1] for item in self._active.values())
                used_spill = sum(item[2] for item in self._active.values())
                disk_ok = spill == 0 or (
                    self._disk_free() - used_spill - spill
                    >= self.policy.min_spill_free_bytes
                )
                if (
                    used_cpu + cpu <= self._cpu_capacity
                    and used_memory + memory <= self._memory_capacity
                    and disk_ok
                ):
                    self._active[query_id] = (cpu, memory, spill)
                    return ResourceReservation(self, query_id, plan)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ResourceReservationTimeout(f"timed out reserving resources for {query_id!r}")
                    self._condition.wait(min(remaining, 0.1))
                else:
                    self._condition.wait(0.1)

    def _release(self, query_id: str) -> None:
        with self._condition:
            self._active.pop(query_id, None)
            self._condition.notify_all()


class ArrowBatchStream(Iterator[pa.RecordBatch]):
    """Single-consumer, cancellable Arrow RecordBatch result contract.

    The stream closes its underlying producer on exhaustion, exception,
    explicit ``close()``, or context exit.  Collection is opt-in and bounded;
    the normal contract is incremental consumption.
    """

    def __init__(
        self,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
        *,
        close_callback: Callable[[], None] | None = None,
        cancel_event: threading.Event | None = None,
    ):
        self.schema = schema
        self._iterator = iter(batches)
        self._close_callback = close_callback
        self._cancel_event = cancel_event or threading.Event()
        self._closed = False
        self._close_requested = False
        self._active_next = 0
        self._state_lock = threading.RLock()

    @classmethod
    def from_table(
        cls,
        table: pa.Table,
        *,
        max_chunksize: int = 64 * 1024,
    ) -> "ArrowBatchStream":
        return cls(table.schema, table.to_batches(max_chunksize=max_chunksize))

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed

    @property
    def cancel_event(self) -> threading.Event:
        """Shared cooperative-cancellation signal for nested operators."""
        return self._cancel_event

    def __iter__(self) -> "ArrowBatchStream":
        return self

    def __next__(self) -> pa.RecordBatch:
        with self._state_lock:
            if self._closed:
                raise StopIteration
            if self._cancel_event.is_set():
                cancelled_before_next = True
                self._close_requested = True
                should_finalize = self._active_next == 0
                if should_finalize:
                    self._closed = True
            else:
                cancelled_before_next = False
                should_finalize = False
                if self._active_next:
                    raise RuntimeError("ArrowBatchStream is single-consumer")
                self._active_next += 1
        if cancelled_before_next:
            if should_finalize:
                self._finalize()
            raise ResourceReservationCancelled("Arrow result stream was cancelled")
        try:
            batch = next(self._iterator)
        except StopIteration:
            self._finish_next(close=True)
            raise
        except BaseException:
            self._finish_next(close=True)
            raise
        cancelled = self._cancel_event.is_set()
        close_requested = self._finish_next(close=cancelled)
        if cancelled:
            # Cancellation is cooperative: never release leases while a
            # producer is still inside next(), and never yield a batch that
            # completed after cancellation was requested.
            raise ResourceReservationCancelled("Arrow result stream was cancelled")
        if close_requested:
            # Explicit close racing an active next() abandons that result. It
            # must not escape after its backing cache/spill leases are released.
            raise StopIteration
        if not isinstance(batch, pa.RecordBatch):
            self.close()
            raise TypeError(f"result producer yielded {type(batch).__name__}, not RecordBatch")
        if not batch.schema.equals(self.schema, check_metadata=False):
            self.close()
            raise ValueError("result batch schema changed during execution")
        return batch

    def cancel(self) -> None:
        self._cancel_event.set()
        # Nested streams are common (Island execution -> spill/reorder stream).
        # Propagate cancellation so the deepest producer sees its own shared
        # event and can stop long merge/range work before the outer ``next``
        # returns. ``ArrowBatchStream.cancel`` is itself safe while next() is
        # active, unlike closing a raw Python generator cross-thread.
        cancel = getattr(self._iterator, "cancel", None)
        try:
            if callable(cancel):
                cancel()
        finally:
            self.close()

    def _finish_next(self, *, close: bool) -> bool:
        should_finalize = False
        with self._state_lock:
            self._active_next = max(0, self._active_next - 1)
            if close:
                self._close_requested = True
            close_requested = self._close_requested
            if self._close_requested and self._active_next == 0 and not self._closed:
                self._closed = True
                should_finalize = True
        if should_finalize:
            self._finalize()
        return close_requested

    def _finalize(self) -> None:
        close = getattr(self._iterator, "close", None)
        try:
            if callable(close):
                close()
        finally:
            callback = None
            with self._state_lock:
                if self._close_callback is not None:
                    callback, self._close_callback = self._close_callback, None
            if callback is not None:
                callback()

    def close(self) -> None:
        should_finalize = False
        with self._state_lock:
            if self._closed:
                return
            self._close_requested = True
            if self._active_next == 0:
                self._closed = True
                should_finalize = True
        if should_finalize:
            self._finalize()

    def collect_table(self, *, max_bytes: int) -> pa.Table:
        if max_bytes < 0:
            raise ValueError("max_bytes cannot be negative")
        batches: list[pa.RecordBatch] = []
        total = 0
        try:
            for batch in self:
                if total + batch.nbytes > max_bytes:
                    raise ResultMemoryLimitExceeded(
                        f"result exceeds bounded collection limit of {max_bytes} bytes"
                    )
                batches.append(batch)
                total += batch.nbytes
            return pa.Table.from_batches(batches, schema=self.schema)
        finally:
            self.close()

    def to_reader(self):
        """Return an owned Arrow reader whose close releases this stream."""
        return _OwnedRecordBatchReader(self)

    def __enter__(self) -> "ArrowBatchStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self):
        # Explicit close/context use remains the contract. This last-resort
        # guard prevents an abandoned stream from pinning a governor/cache lease
        # forever; interpreter-shutdown failures must not escape a finalizer.
        try:
            self.close()
        except Exception:
            pass


class _OwnedRecordBatchReader:
    """Ownership wrapper around ``RecordBatchReader.from_batches``.

    PyArrow does not close the Python iterable when its reader is explicitly
    closed. Without this wrapper, Island resource/cache/spill leases remain
    held. The wrapper deliberately exposes the common reader surface while
    tying every terminal operation to the source stream.
    """

    def __init__(self, stream: ArrowBatchStream):
        self._stream = stream
        self._reader = pa.RecordBatchReader.from_batches(stream.schema, stream)
        self._state_lock = threading.RLock()
        self._active_operation = False
        self._close_requested = False
        self._reader_closed = False

    @property
    def schema(self):
        return self._reader.schema

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._reader_closed

    def _begin_operation(self, *, iterator: bool = False) -> None:
        with self._state_lock:
            if self._close_requested or self._reader_closed:
                if iterator:
                    raise StopIteration
                raise ValueError("record batch reader is closed")
            if self._active_operation:
                raise RuntimeError("record batch reader is single-consumer")
            self._active_operation = True

    def _close_native_reader_if_idle(self) -> None:
        should_close = False
        with self._state_lock:
            self._close_requested = True
            if not self._active_operation and not self._reader_closed:
                self._reader_closed = True
                should_close = True
        if should_close:
            self._reader.close()

    def _finish_operation(self, *, terminal: bool) -> None:
        should_close = False
        close_source = False
        with self._state_lock:
            self._active_operation = False
            if terminal:
                self._close_requested = True
            if self._close_requested and not self._reader_closed:
                self._reader_closed = True
                should_close = True
            close_source = terminal or self._close_requested
        try:
            if should_close:
                self._reader.close()
        finally:
            if close_source:
                self._stream.close()

    def _raise_if_shutdown_requested(self) -> None:
        with self._state_lock:
            shutdown = self._close_requested
        if not shutdown:
            return
        if self._stream.cancel_event.is_set():
            raise ResourceReservationCancelled("Arrow record batch reader was cancelled")
        raise ValueError("record batch reader was closed during an active read")

    def read_next_batch(self):
        self._begin_operation(iterator=True)
        terminal = False
        try:
            return self._reader.read_next_batch()
        except StopIteration:
            terminal = True
            raise
        except BaseException:
            terminal = True
            raise
        finally:
            self._finish_operation(terminal=terminal)

    def read_all(self):
        self._begin_operation()
        try:
            result = self._reader.read_all()
            self._raise_if_shutdown_requested()
            return result
        finally:
            self._finish_operation(terminal=True)

    def close(self) -> None:
        try:
            # Closing the native C++ reader concurrently with an active Python
            # iterator callback is undefined. The source stream closes
            # cooperatively; defer the native close until the read returns.
            self._stream.close()
        finally:
            self._close_native_reader_if_idle()

    def cancel(self) -> None:
        try:
            self._stream.cancel()
        finally:
            self._close_native_reader_if_idle()

    def read_pandas(self, **options):
        self._begin_operation()
        try:
            result = self._reader.read_pandas(**options)
            self._raise_if_shutdown_requested()
            return result
        finally:
            self._finish_operation(terminal=True)

    def __iter__(self):
        return self

    def __next__(self):
        return self.read_next_batch()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
