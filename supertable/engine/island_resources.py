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
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Iterator, Optional, Sequence

import pyarrow as pa

from supertable.engine.disk_admission import (
    DiskAdmissionUnavailable,
    DiskReservation,
    reserve_disk,
)


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
                return _contained_cgroup_path(cgroup_root, line[3:])
    return cgroup_root


def _contained_cgroup_path(root: Path, relative: str) -> Path:
    """Resolve one proc-reported cgroup path without escaping its mount."""
    resolved_root = root.resolve()
    candidate = (resolved_root / str(relative or "").lstrip("/")).resolve()
    try:
        candidate.relative_to(resolved_root)
    except ValueError:
        return resolved_root
    return candidate


def _ancestor_cgroups(leaf: Path, root: Path) -> tuple[Path, ...]:
    """Return leaf-to-root hierarchy entries contained by one mount."""
    resolved_root = root.resolve()
    current = leaf.resolve()
    try:
        current.relative_to(resolved_root)
    except ValueError:
        return (current,)
    result = []
    while True:
        result.append(current)
        if current == resolved_root:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent
    return tuple(result)


def _proc_cgroup_paths(
    proc_root: Path,
) -> tuple[Optional[str], dict[str, str], bool]:
    """Return cgroup paths plus whether present process metadata was invalid."""
    unified: Optional[str] = None
    controllers: dict[str, str] = {}
    metadata_path = proc_root / "self" / "cgroup"
    try:
        metadata_path.stat()
    except (FileNotFoundError, NotADirectoryError):
        metadata_present = False
    except OSError:
        return None, {}, True
    else:
        metadata_present = True
    raw_text = _read_text(metadata_path)
    text = raw_text or ""
    malformed = bool(metadata_present and not text)
    for line in text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            malformed = True
            continue
        hierarchy, names, relative = fields
        if not hierarchy.isdigit() or not relative.startswith("/"):
            malformed = True
            continue
        if ".." in PurePosixPath(relative).parts:
            # A proc-reported path may never select data outside its controller
            # mount.  Falling back to the mount root remains conservative: its
            # limits are ancestors of every legitimate process leaf.
            relative = "/"
        if hierarchy == "0" and names == "":
            if unified is not None:
                malformed = True
            unified = relative
            continue
        if hierarchy == "0" or not names:
            malformed = True
            continue
        for name in names.split(","):
            name = name.strip()
            if name:
                controllers[name] = relative
            else:
                malformed = True
    return unified, controllers, malformed


def _v1_controller_mount(cgroup_root: Path, controller: str) -> Optional[Path]:
    """Find one conventional cgroup-v1 controller mount below the root."""
    root = cgroup_root.resolve()
    direct = root / controller
    if direct.is_dir():
        resolved = direct.resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            return None
        return resolved
    try:
        children = tuple(root.iterdir())
    except OSError:
        return None
    for child in children:
        if child.is_dir() and controller in child.name.split(","):
            resolved = child.resolve()
            try:
                resolved.relative_to(root)
            except ValueError:
                continue
            return resolved
    return None


def _mountinfo_cgroup_mounts(
    proc_root: Path,
    cgroup_root: Path,
) -> tuple[tuple[tuple[Path, str], ...], dict[str, tuple[tuple[Path, str], ...]]]:
    """Return contained cgroup mounts as ``(mountpoint, hierarchy_root)``."""

    def unescape(value: str) -> str:
        for encoded, decoded in (
            ("\\040", " "), ("\\011", "\t"),
            ("\\012", "\n"), ("\\134", "\\"),
        ):
            value = value.replace(encoded, decoded)
        return value

    allowed_root = cgroup_root.resolve()
    unified: list[tuple[Path, str]] = []
    legacy: dict[str, list[tuple[Path, str]]] = {}
    text = _read_text(proc_root / "self" / "mountinfo") or ""
    for line in text.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
            mount_root = unescape(fields[3])
            mountpoint = Path(unescape(fields[4])).resolve()
            filesystem = fields[separator + 1]
            source = fields[separator + 2]
            super_options = fields[separator + 3]
        except (ValueError, IndexError):
            continue
        try:
            mountpoint.relative_to(allowed_root)
        except ValueError:
            continue
        item = (mountpoint, mount_root)
        if filesystem == "cgroup2":
            unified.append(item)
            continue
        if filesystem != "cgroup":
            continue
        controller_tokens = set()
        for value in (source, super_options, mountpoint.name):
            controller_tokens.update(
                token.strip() for token in value.split(",") if token.strip()
            )
        for controller in ("cpu", "cpuacct", "cpuset", "memory"):
            if controller in controller_tokens:
                legacy.setdefault(controller, []).append(item)
    return (
        tuple(unified),
        {name: tuple(items) for name, items in legacy.items()},
    )


def _mounted_cgroup_leaf(
    mounts: Sequence[tuple[Path, str]],
    process_path: str,
) -> Optional[tuple[Path, Path]]:
    """Map a proc hierarchy path through its most-specific mount root."""
    process = PurePosixPath("/" + str(process_path or "").lstrip("/"))
    if ".." in process.parts:
        return None
    candidates = []
    for mountpoint, raw_mount_root in mounts:
        mount_root = PurePosixPath(
            "/" + str(raw_mount_root or "").lstrip("/")
        )
        if ".." in mount_root.parts:
            continue
        try:
            relative = process.relative_to(mount_root)
        except ValueError:
            continue
        candidates.append((len(mount_root.parts), mountpoint, relative))
    if not candidates:
        return None
    _specificity, mountpoint, relative = max(
        candidates, key=lambda item: item[0],
    )
    leaf = _contained_cgroup_path(mountpoint, str(relative))
    return leaf, mountpoint.resolve()


@dataclass(frozen=True)
class ContainerResources:
    """Effective limits after intersecting host, affinity, and cgroup state."""

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
        unified_path, v1_paths, controller_metadata_malformed = (
            _proc_cgroup_paths(proc_root)
        )
        resolved_root = cgroup_root.resolve()
        v2_mounts, v1_mounts = _mountinfo_cgroup_mounts(
            proc_root, resolved_root,
        )
        if cgroup_dir is not None:
            cg = Path(cgroup_dir).resolve()
            try:
                cg.relative_to(resolved_root)
                hierarchy_root = resolved_root
            except ValueError:
                # Explicit test/embedded controller directories are their own
                # hierarchy boundary; never walk arbitrary filesystem parents.
                hierarchy_root = cg
            v2_entries = _ancestor_cgroups(cg, hierarchy_root)
            use_v2 = True
        else:
            mounted_v2 = (
                _mounted_cgroup_leaf(v2_mounts, unified_path)
                if unified_path is not None else None
            )
            if mounted_v2 is not None:
                cg, hierarchy_root = mounted_v2
            else:
                hierarchy_root = resolved_root
                cg = (
                    _contained_cgroup_path(resolved_root, unified_path)
                    if unified_path is not None else resolved_root
                )
                conventional_entries = _ancestor_cgroups(cg, hierarchy_root)
                if unified_path is not None and not any(
                    (directory / marker).exists()
                    for directory in conventional_entries
                    for marker in (
                        "cgroup.controllers", "cgroup.procs", "cgroup.type",
                        "cpu.max", "memory.max",
                    )
                ):
                    # /proc declared a unified hierarchy, but neither
                    # mountinfo nor the configured conventional root proves a
                    # usable controller boundary. Treat it as untrusted rather
                    # than silently reverting to host resources.
                    controller_metadata_malformed = True
            use_v2 = bool(
                unified_path is not None
                or v2_mounts
                or (resolved_root / "cgroup.controllers").exists()
                or (resolved_root / "cpu.max").exists()
                or (resolved_root / "memory.max").exists()
            )
            v2_entries = (
                _ancestor_cgroups(cg, hierarchy_root) if use_v2 else ()
            )

        if affinity is None:
            try:
                affinity_tuple = tuple(sorted(os.sched_getaffinity(0)))
            except (AttributeError, OSError):
                affinity_tuple = tuple(range(max(1, os.cpu_count() or 1)))
        else:
            affinity_tuple = tuple(sorted(set(int(cpu) for cpu in affinity)))
        if not affinity_tuple:
            affinity_tuple = (0,)

        cpuset_sets: list[set[int]] = []
        quota_capacities: list[float] = []
        cgroup_memory: list[tuple[int, int]] = []
        cpu_control_malformed = controller_metadata_malformed
        memory_control_malformed = controller_metadata_malformed

        if use_v2:
            for directory in v2_entries:
                cpuset_text = _read_text(directory / "cpuset.cpus.effective")
                if cpuset_text is None:
                    cpuset_text = _read_text(directory / "cpuset.cpus")
                try:
                    parsed_cpuset = parse_cpuset(cpuset_text or "")
                except (TypeError, ValueError):
                    parsed_cpuset = ()
                    if cpuset_text not in (None, ""):
                        cpu_control_malformed = True
                if parsed_cpuset:
                    cpuset_sets.append(set(parsed_cpuset))

                cpu_max = _read_text(directory / "cpu.max")
                if cpu_max is not None:
                    fields = cpu_max.split()
                    valid_cpu_max = False
                    if len(fields) == 2:
                        try:
                            period = int(fields[1])
                            if fields[0] == "max" and period > 0:
                                valid_cpu_max = True
                            else:
                                quota = int(fields[0])
                                if quota > 0 and period > 0:
                                    valid_cpu_max = True
                                    quota_capacities.append(quota / period)
                        except ValueError:
                            pass
                    if not valid_cpu_max:
                        cpu_control_malformed = True

                memory_max_text = _read_text(directory / "memory.max")
                if memory_max_text is not None and memory_max_text != "max":
                    try:
                        limit = int(memory_max_text)
                    except ValueError:
                        cgroup_memory.append((0, 0))
                        continue
                    if limit < 0:
                        cgroup_memory.append((0, 0))
                        continue
                    current_text = _read_text(directory / "memory.current")
                    if current_text is None:
                        available = 0
                    else:
                        try:
                            current = int(current_text)
                            available = (
                                max(0, limit - current)
                                if current >= 0 else 0
                            )
                        except ValueError:
                            available = 0
                    cgroup_memory.append((
                        limit, available,
                    ))
        def v1_hierarchy(controller: str) -> Optional[tuple[Path, Path]]:
            relative = v1_paths.get(controller)
            if relative is None:
                return None
            mounted = _mounted_cgroup_leaf(
                v1_mounts.get(controller, ()), relative,
            )
            if mounted is not None:
                return mounted
            mount = _v1_controller_mount(resolved_root, controller)
            if mount is None:
                return None
            return _contained_cgroup_path(mount, relative), mount

        if v1_paths:
            cpuset_hierarchy = v1_hierarchy("cpuset")
            if cpuset_hierarchy is not None:
                leaf, cpuset_mount = cpuset_hierarchy
                for directory in _ancestor_cgroups(leaf, cpuset_mount):
                    try:
                        parsed_cpuset = parse_cpuset(
                            _read_text(directory / "cpuset.cpus") or ""
                        )
                    except (TypeError, ValueError):
                        parsed_cpuset = ()
                        cpuset_text = _read_text(directory / "cpuset.cpus")
                        if cpuset_text not in (None, ""):
                            cpu_control_malformed = True
                    if parsed_cpuset:
                        cpuset_sets.append(set(parsed_cpuset))
            elif "cpuset" in v1_paths:
                cpu_control_malformed = True

            cpu_hierarchy = v1_hierarchy("cpu")
            if cpu_hierarchy is not None:
                leaf, cpu_mount = cpu_hierarchy
                for directory in _ancestor_cgroups(leaf, cpu_mount):
                    quota_text = _read_text(
                        directory / "cpu.cfs_quota_us"
                    )
                    period_text = _read_text(
                        directory / "cpu.cfs_period_us"
                    )
                    if quota_text is None and period_text is None:
                        continue
                    try:
                        quota = int(quota_text) if quota_text is not None else 0
                        period = int(period_text) if period_text is not None else 0
                    except ValueError:
                        cpu_control_malformed = True
                        continue
                    if period <= 0 or quota == 0 or quota < -1:
                        cpu_control_malformed = True
                    elif quota > 0:
                        quota_capacities.append(quota / period)
            elif "cpu" in v1_paths:
                cpu_control_malformed = True

            memory_hierarchy = v1_hierarchy("memory")
            if memory_hierarchy is not None:
                leaf, memory_mount = memory_hierarchy
                for directory in _ancestor_cgroups(leaf, memory_mount):
                    limit_text = _read_text(
                        directory / "memory.limit_in_bytes"
                    )
                    if limit_text is None:
                        continue
                    try:
                        limit = int(limit_text)
                    except ValueError:
                        cgroup_memory.append((0, 0))
                        continue
                    if limit < 0:
                        cgroup_memory.append((0, 0))
                        continue
                    usage_text = _read_text(
                        directory / "memory.usage_in_bytes"
                    )
                    if usage_text is None:
                        available = 0
                    else:
                        try:
                            current = int(usage_text)
                            available = (
                                max(0, limit - current)
                                if current >= 0 else 0
                            )
                        except ValueError:
                            available = 0
                    cgroup_memory.append((
                        limit, available,
                    ))
            elif "memory" in v1_paths:
                memory_control_malformed = True

        if memory_control_malformed:
            cgroup_memory.append((0, 0))

        if cpuset_sets:
            effective_cpuset = set(cpuset_sets[0])
            for item in cpuset_sets[1:]:
                effective_cpuset.intersection_update(item)
            cpuset = tuple(sorted(effective_cpuset))
            if not cpuset:
                # Individually valid but contradictory ancestor controls are
                # a racing/corrupt controller state, not an absent constraint.
                cpu_control_malformed = True
        else:
            cpuset = ()

        cpu_ceiling = len(affinity_tuple)
        if cpuset:
            intersection = set(affinity_tuple).intersection(cpuset)
            if intersection:
                cpu_ceiling = len(intersection)
            else:
                # sched affinity and the effective cgroup cpuset must overlap
                # for a healthy process. Fail closed while their snapshots are
                # inconsistent instead of treating either set as authoritative.
                cpu_control_malformed = True
        if cpu_control_malformed:
            # A present but unreadable controller must not silently widen the
            # native worker pool to host affinity. One worker is the smallest
            # executable Polars configuration; valid tighter fractional quota
            # information below is still retained in ``cpu_capacity``.
            cpu_ceiling = min(cpu_ceiling, 1)
        cpu_ceiling = max(1, cpu_ceiling)

        quota_capacity = min(quota_capacities) if quota_capacities else None
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

        memory_limit = min(
            max(0, host_total),
            *(limit for limit, _available in cgroup_memory),
        ) if cgroup_memory else max(0, host_total)
        available_candidates = [max(0, host_available)]
        available_candidates.extend(
            max(0, available) for _limit, available in cgroup_memory
        )
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
    max_result_row_bytes: int = 0

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
        max_result_row_bytes = (
            (
                estimate.result_bytes
                + estimate.estimated_result_rows
                - 1
            ) // estimate.estimated_result_rows
            if estimate.estimated_result_rows > 0 else 0
        )
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
                max_result_row_bytes=max_result_row_bytes,
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
                max_result_row_bytes=max_result_row_bytes,
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
                max_result_row_bytes=max_result_row_bytes,
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
                max_result_row_bytes=max_result_row_bytes,
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
                max_result_row_bytes=max_result_row_bytes,
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
                max_result_row_bytes=max_result_row_bytes,
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
            max_result_row_bytes=max_result_row_bytes,
        )


class ResourceReservation:
    """Idempotent context-managed reservation returned by ResourceGovernor."""

    def __init__(
        self,
        governor: "ResourceGovernor",
        query_id: str,
        plan: QueryResourcePlan,
        disk_reservation: DiskReservation | None = None,
    ):
        self._governor = governor
        self.query_id = query_id
        self.plan = plan
        self._disk_reservation = disk_reservation
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            if self._disk_reservation is not None:
                self._disk_reservation.release()
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
                    disk_reservation = None
                    if spill:
                        try:
                            disk_reservation = reserve_disk(
                                self.spill_root,
                                spill,
                                min_free_bytes=self.policy.min_spill_free_bytes,
                            )
                        except DiskAdmissionUnavailable:
                            # Another process may have won the shared disk
                            # reservation between the cheap free-space check
                            # above and this atomic admission.
                            self._condition.wait(0.1)
                            continue
                    self._active[query_id] = (cpu, memory, spill)
                    return ResourceReservation(
                        self, query_id, plan, disk_reservation,
                    )
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


class ByteBoundedArrowBatchIterator(Iterator[pa.RecordBatch]):
    """Split producer batches at an explicit row and logical-byte boundary.

    Arrow cannot split an individual value without changing the result, so a
    single row larger than ``max_batch_bytes`` is rejected.  Slices retain the
    producer's owning buffers until that producer batch is exhausted; engines
    must therefore also choose a conservative upstream fetch size.  This
    iterator is the final hard boundary that prevents an oversized batch from
    reaching a response/page consumer when width estimates are imperfect.
    """

    def __init__(
        self,
        batches: Iterable[pa.RecordBatch],
        *,
        schema: pa.Schema,
        max_batch_rows: int,
        max_batch_bytes: int,
    ) -> None:
        for name, value in (
            ("max_batch_rows", max_batch_rows),
            ("max_batch_bytes", max_batch_bytes),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive integer")
        self.schema = schema
        self._inner = iter(batches)
        self._max_rows = max_batch_rows
        self._max_bytes = max_batch_bytes
        self._pending: pa.RecordBatch | None = None
        self._offset = 0
        self._closed = False

    def __iter__(self):
        return self

    @staticmethod
    def _rows_that_fit(
        batch: pa.RecordBatch,
        *,
        max_rows: int,
        max_bytes: int,
    ) -> int:
        candidate_rows = min(max_rows, batch.num_rows)
        if candidate_rows <= 0:
            return 0
        if batch.slice(0, candidate_rows).nbytes <= max_bytes:
            return candidate_rows
        if batch.slice(0, 1).nbytes > max_bytes:
            return 0
        low, high = 1, candidate_rows
        while low < high:
            middle = (low + high + 1) // 2
            if batch.slice(0, middle).nbytes <= max_bytes:
                low = middle
            else:
                high = middle - 1
        return low

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        while self._pending is None or self._offset >= self._pending.num_rows:
            self._pending = next(self._inner)
            self._offset = 0
            if not isinstance(self._pending, pa.RecordBatch):
                self.close()
                raise TypeError(
                    "Arrow batch producer yielded "
                    "an unexpected value, not RecordBatch"
                )
            if not self._pending.schema.equals(
                self.schema, check_metadata=False,
            ):
                self.close()
                raise ValueError("Arrow batch producer schema changed")
            if self._pending.num_rows == 0:
                # Preserve a producer's explicit empty batch without risking
                # an infinite local skip loop; it is already byte bounded.
                if self._pending.nbytes > self._max_bytes:
                    self.close()
                    raise ResultMemoryLimitExceeded(
                        "an empty Arrow result batch exceeds the configured "
                        "stream batch-byte budget"
                    )
                return self._pending

        remaining = self._pending.slice(self._offset)
        rows = self._rows_that_fit(
            remaining,
            max_rows=self._max_rows,
            max_bytes=self._max_bytes,
        )
        if rows <= 0:
            self.close()
            raise ResultMemoryLimitExceeded(
                "one Arrow result row exceeds the configured stream "
                "batch-byte budget"
            )
        result = remaining.slice(0, rows)
        self._offset += rows
        return result

    def cancel(self) -> None:
        if self._closed:
            return
        self._closed = True
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            close = getattr(self._inner, "close", None)
            if callable(close):
                close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._inner, "close", None)
        if callable(close):
            close()


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
        self._closed_event = threading.Event()
        self._completion_started = False
        self._finalization_error: BaseException | None = None
        self._finalization_callbacks: list[Callable[[], None]] = []

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

    @property
    def finalization_error(self) -> BaseException | None:
        """Return a completed cleanup failure without exposing partial state."""

        with self._state_lock:
            if not self._closed_event.is_set():
                return None
            return self._finalization_error

    def __iter__(self) -> "ArrowBatchStream":
        return self

    def __next__(self) -> pa.RecordBatch:
        with self._state_lock:
            if self._closed:
                if (
                    self._closed_event.is_set()
                    and self._finalization_error is not None
                ):
                    raise self._finalization_error
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
            raise TypeError(
                "result producer yielded an unexpected value, not RecordBatch"
            )
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
        deferred = False
        add_finalization_callback = getattr(
            self._iterator, "add_finalization_callback", None,
        )
        if callable(add_finalization_callback):
            try:
                deferred = (
                    add_finalization_callback(self._complete_finalization)
                    is not False
                )
            except Exception:
                deferred = False
        close = getattr(self._iterator, "close", None)
        try:
            if callable(close):
                close()
        finally:
            if not deferred:
                self._complete_finalization()
        # Registration may have completed synchronously (for example, when a
        # nested stream finalizes during ``close``). Surface that cleanup
        # failure to the initiating caller without waiting on async cleanup.
        completion_error = self.finalization_error
        if completion_error is not None:
            raise completion_error

    def _complete_finalization(self) -> None:
        """Finish this layer only after its iterator has fully finalized."""

        callback = None
        with self._state_lock:
            if self._closed_event.is_set() or self._completion_started:
                return
            self._completion_started = True
            if self._close_callback is not None:
                callback, self._close_callback = self._close_callback, None
        completion_error: BaseException | None = None
        try:
            if callback is not None:
                callback()
        except BaseException as exc:
            completion_error = exc
        finally:
            # A nested Arrow layer may have completed asynchronously. Preserve
            # its cleanup failure on this layer even when intermediary
            # diagnostic observers intentionally isolate callback exceptions.
            current: object | None = self._iterator
            seen: set[int] = set()
            while completion_error is None and current is not None:
                identity = id(current)
                if identity in seen:
                    break
                seen.add(identity)
                try:
                    nested_error = getattr(
                        current, "finalization_error", None,
                    )
                except Exception:
                    nested_error = None
                if isinstance(nested_error, BaseException):
                    completion_error = nested_error
                    break
                current = getattr(current, "_inner", None)
            # ``closed`` becomes true before producer/callback cleanup is
            # complete. Lifecycle observers use this event when terminal
            # telemetry must include cleanup-finalized measurements.
            with self._state_lock:
                self._finalization_error = completion_error
                callbacks, self._finalization_callbacks = (
                    self._finalization_callbacks,
                    [],
                )
                self._closed_event.set()
            for finalization_callback in callbacks:
                try:
                    finalization_callback()
                except Exception:
                    # Finalization observers are diagnostic only. They must
                    # never change the result-stream cleanup contract.
                    pass
        if completion_error is not None:
            raise completion_error

    def add_finalization_callback(self, callback: Callable[[], None]) -> bool:
        """Invoke ``callback`` after producer and close cleanup has finished."""

        invoke_now = False
        with self._state_lock:
            if self._closed_event.is_set():
                invoke_now = True
            else:
                self._finalization_callbacks.append(callback)
        if invoke_now:
            try:
                callback()
            except Exception:
                pass
        return True

    def wait_closed(self, timeout: float | None = None) -> bool:
        """Wait until producer and close-callback finalization has completed."""

        return self._closed_event.wait(timeout)

    def close(self) -> None:
        should_finalize = False
        with self._state_lock:
            if self._closed:
                if (
                    self._closed_event.is_set()
                    and self._finalization_error is not None
                ):
                    raise self._finalization_error
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
