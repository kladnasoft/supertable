"""Fresh-container runner for the parity-first engine benchmark.

``compare_manifest`` accepts any callable with the same shape as
``runner.run_isolated_worker``.  :class:`DockerWorkerRunner` implements that
protocol while making the process boundary a reproducible Docker cgroup:

* four pinned CPUs (quota and cpuset), four GiB RAM, and no usable swap;
* read-only repository and corpus mounts plus query-private writable roots;
* a new container for every parity or timing series;
* host-side RSS, process-I/O, spill, and cgroup-v2 sampling that survives a
  worker timeout; and
* immutable image, source, dependency, request, and Docker-inspect provenance.

The runner returns the normal engine-series mapping, augmented with a
``container_run`` record.  It also preserves the complete request, response,
logs, samples, and inspect data below the configured artifact root.  It does
not generate a corpus and never launches more than the one series requested by
its caller.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .runner import _cgroup_v2_memory_telemetry, _duckdb_memory_limit_text
from supertable.utils.diagnostic_redaction import safe_exception_type


GIB = 1024**3
CONTAINER_CPUS = 4
CONTAINER_MEMORY_BYTES = 4 * GIB
DEFAULT_ENGINE_MEMORY_BYTES = 2 * GIB
DEFAULT_PIDS_LIMIT = 1024
DEFAULT_SPILL_MAX_BYTES = 28 * GIB
ARTIFACT_FORMAT_VERSION = 1


class ContainerBenchmarkError(RuntimeError):
    """A container series failed after its diagnostic artifact was retained."""


class ContainerConfigurationError(ValueError):
    """The requested container boundary cannot be made reproducible."""


def _parse_cpuset(value: str) -> tuple[int, ...]:
    cpus: set[int] = set()
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            raise ContainerConfigurationError("cpuset contains an empty item")
        if "-" in item:
            first_raw, separator, last_raw = item.partition("-")
            if not separator or not first_raw.isdigit() or not last_raw.isdigit():
                raise ContainerConfigurationError(f"invalid cpuset range {item!r}")
            first, last = int(first_raw), int(last_raw)
            if first > last:
                raise ContainerConfigurationError(f"reversed cpuset range {item!r}")
            cpus.update(range(first, last + 1))
        else:
            if not item.isdigit():
                raise ContainerConfigurationError(f"invalid cpuset item {item!r}")
            cpus.add(int(item))
    if len(cpus) != CONTAINER_CPUS:
        raise ContainerConfigurationError(
            f"cpuset must contain exactly {CONTAINER_CPUS} CPUs, got {sorted(cpus)}"
        )
    return tuple(sorted(cpus))


def _canonical_cpuset(cpus: Sequence[int]) -> str:
    ordered = sorted(set(int(cpu) for cpu in cpus))
    if not ordered:
        return ""
    parts: list[str] = []
    start = previous = ordered[0]
    for current in ordered[1:]:
        if current == previous + 1:
            previous = current
            continue
        parts.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = current
    parts.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(parts)


@dataclass(frozen=True)
class ContainerRunnerConfig:
    """Immutable host/container configuration for a worker-runner instance."""

    repo_root: str | Path
    corpus_root: str | Path
    artifact_root: str | Path
    image: str
    docker: str = "docker"
    cpuset_cpus: str = "0-3"
    sample_interval_seconds: float = 0.25
    pids_limit: int = DEFAULT_PIDS_LIMIT
    spill_max_bytes: int = DEFAULT_SPILL_MAX_BYTES
    engine_memory_bytes: int = DEFAULT_ENGINE_MEMORY_BYTES

    def __post_init__(self) -> None:
        if not str(self.image).strip():
            raise ContainerConfigurationError("container image must be non-empty")
        if not str(self.docker).strip():
            raise ContainerConfigurationError("Docker executable must be non-empty")
        cpus = _parse_cpuset(self.cpuset_cpus)
        object.__setattr__(self, "cpuset_cpus", _canonical_cpuset(cpus))
        if not 0 < float(self.sample_interval_seconds) <= 30:
            raise ContainerConfigurationError(
                "sample_interval_seconds must be in (0, 30]"
            )
        if int(self.pids_limit) <= 0:
            raise ContainerConfigurationError("pids_limit must be positive")
        if int(self.spill_max_bytes) <= 0:
            raise ContainerConfigurationError("spill_max_bytes must be positive")
        if not 0 < int(self.engine_memory_bytes) < CONTAINER_MEMORY_BYTES:
            raise ContainerConfigurationError(
                "engine_memory_bytes must be positive and below the 4-GiB cgroup"
            )


def _read_mapping(path: Path) -> dict[str, int] | None:
    try:
        raw = path.read_text(encoding="ascii")[:65_536]
    except OSError:
        return None
    result: dict[str, int] = {}
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            result[fields[0]] = max(0, int(fields[1]))
        except ValueError:
            continue
    return result or None


def _read_scalar(path: Path) -> tuple[int | None, str | None]:
    try:
        raw = path.read_text(encoding="ascii").strip()
    except OSError:
        return None, None
    if raw == "max":
        return None, raw
    try:
        return max(0, int(raw)), raw
    except ValueError:
        return None, raw[:256]


def _read_text(path: Path, *, limit: int = 65_536) -> str | None:
    try:
        return path.read_text(encoding="ascii")[:limit]
    except OSError:
        return None


def _parse_pressure(raw: str | None) -> dict[str, dict[str, int | float]] | None:
    if raw is None:
        return None
    result: dict[str, dict[str, int | float]] = {}
    for line in raw.splitlines():
        fields = line.split()
        if not fields:
            continue
        values: dict[str, int | float] = {}
        for field in fields[1:]:
            name, separator, value = field.partition("=")
            if not separator:
                continue
            try:
                values[name] = int(value) if name == "total" else float(value)
            except ValueError:
                continue
        result[fields[0]] = values
    return result or None


def _cgroup_v2_extended_telemetry(
    *,
    proc_cgroup: str | Path = "/proc/self/cgroup",
    cgroup_root: str | Path = "/sys/fs/cgroup",
) -> dict[str, Any]:
    """Return memory, CPU, I/O-pressure, cpuset, and PID cgroup telemetry."""

    result = _cgroup_v2_memory_telemetry(
        proc_cgroup=proc_cgroup,
        cgroup_root=cgroup_root,
    )
    if not result.get("available"):
        return result
    try:
        root = Path(cgroup_root).resolve(strict=True)
        relative = str(result.get("path") or "/").lstrip("/")
        current = (root / relative).resolve(strict=True)
        current.relative_to(root)
    except (OSError, ValueError) as exc:
        result["extended_reason"] = (
            "cgroup_path_invalid; error_type="
            + safe_exception_type(exc)
        )
        return result

    def member(name: str) -> Path:
        candidate = current / name
        try:
            resolved = candidate.resolve(strict=False)
            resolved.relative_to(current)
        except (OSError, ValueError):
            raise ValueError(f"unsafe cgroup member {name!r}") from None
        return candidate

    cpu_pressure_raw = _read_text(member("cpu.pressure"))
    io_pressure_raw = _read_text(member("io.pressure"))
    memory_pressure_raw = result.get("memory_pressure")
    result.update(
        {
            "cpu_stat": _read_mapping(member("cpu.stat")),
            "cpu_stat_local": _read_mapping(member("cpu.stat.local")),
            "cpu_pressure": cpu_pressure_raw,
            "cpu_pressure_parsed": _parse_pressure(cpu_pressure_raw),
            "io_pressure": io_pressure_raw,
            "io_pressure_parsed": _parse_pressure(io_pressure_raw),
            "memory_pressure_parsed": _parse_pressure(
                memory_pressure_raw
                if isinstance(memory_pressure_raw, str)
                else None
            ),
            "pids_events": _read_mapping(member("pids.events")),
            "pids_events_local": _read_mapping(member("pids.events.local")),
            "cpuset_cpus_effective": (
                (_read_text(member("cpuset.cpus.effective")) or "").strip()
                or None
            ),
            "cpuset_mems_effective": (
                (_read_text(member("cpuset.mems.effective")) or "").strip()
                or None
            ),
            "cpu_max": (
                (_read_text(member("cpu.max")) or "").strip() or None
            ),
        }
    )
    for key, filename in (
        ("pids_current", "pids.current"),
        ("pids_peak", "pids.peak"),
        ("pids_max", "pids.max"),
    ):
        parsed, raw = _read_scalar(member(filename))
        result[f"{key}_count"] = parsed
        if raw is not None and parsed is None:
            result[f"{key}_raw"] = raw
    return result


def _read_rss(pid: int) -> int | None:
    try:
        lines = Path(f"/proc/{pid}/status").read_text(
            encoding="ascii"
        ).splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1]) * 1024
            except (IndexError, ValueError):
                return None
    return None


def _read_proc_io(pid: int) -> dict[str, int] | None:
    try:
        lines = Path(f"/proc/{pid}/io").read_text(encoding="ascii").splitlines()
    except OSError:
        return None
    result: dict[str, int] = {}
    for line in lines:
        key, separator, raw = line.partition(":")
        if not separator:
            continue
        try:
            result[key.strip()] = max(0, int(raw.strip()))
        except ValueError:
            continue
    return result or None


def _tree_footprint(root: Path) -> dict[str, int]:
    files = 0
    total = 0
    try:
        for path in root.rglob("*"):
            try:
                if path.is_file():
                    files += 1
                    total += path.stat().st_size
            except OSError:
                continue
    except OSError:
        pass
    return {"files": files, "bytes": total}


def _cgroup_io_totals(raw: Any) -> dict[str, int] | None:
    if not isinstance(raw, str):
        return None
    totals: dict[str, int] = {}
    for line in raw.splitlines():
        fields = line.split()
        for field in fields[1:]:
            name, separator, value = field.partition("=")
            if not separator:
                continue
            try:
                totals[name] = totals.get(name, 0) + max(0, int(value))
            except ValueError:
                continue
    return totals or None


def _counter_delta(
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
) -> dict[str, int] | None:
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return None
    result: dict[str, int] = {}
    for key in set(before) | set(after):
        try:
            result[str(key)] = max(
                0,
                int(after.get(key) or 0) - int(before.get(key) or 0),
            )
        except (TypeError, ValueError):
            continue
    return result or None


def _pressure_total_delta(
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
) -> dict[str, int] | None:
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return None
    result: dict[str, int] = {}
    for level in set(before) | set(after):
        first = before.get(level)
        last = after.get(level)
        if not isinstance(first, Mapping) or not isinstance(last, Mapping):
            continue
        try:
            result[str(level)] = max(
                0,
                int(last.get("total") or 0) - int(first.get("total") or 0),
            )
        except (TypeError, ValueError):
            continue
    return result or None


class _ContainerSampler:
    """Host-side sampler independent of the benchmark worker and its GIL."""

    def __init__(
        self,
        *,
        docker: str,
        container_name: str,
        spill_root: Path,
        started: float,
        interval_seconds: float,
        expected_cpuset: str,
    ) -> None:
        self.docker = docker
        self.container_name = container_name
        self.spill_root = spill_root
        self.started = started
        self.interval_seconds = interval_seconds
        self.expected_cpuset = expected_cpuset
        self.samples: list[dict[str, Any]] = []
        self.pid: int | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"benchmark-cgroup-{container_name[-12:]}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds * 2))

    def _container_pid(self) -> int | None:
        try:
            completed = subprocess.run(
                [
                    self.docker,
                    "inspect",
                    "--format",
                    "{{.State.Pid}}",
                    self.container_name,
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            value = int(completed.stdout.strip())
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
        return value if value > 0 else None

    def _sample(self) -> None:
        if self.pid is None:
            self.pid = self._container_pid()
        rss = None
        process_io = None
        cgroup: dict[str, Any] = {"available": False}
        if self.pid is not None:
            rss = _read_rss(self.pid)
            process_io = _read_proc_io(self.pid)
            cgroup = _cgroup_v2_extended_telemetry(
                proc_cgroup=f"/proc/{self.pid}/cgroup"
            )
        self.samples.append(
            {
                "elapsed_seconds": max(0.0, time.monotonic() - self.started),
                "container_pid": self.pid,
                "process_rss_bytes": rss,
                "process_io": process_io,
                "cgroup": {
                    **cgroup,
                    "io_totals": _cgroup_io_totals(cgroup.get("io_stat")),
                },
                "spill": _tree_footprint(self.spill_root),
            }
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._sample()
            except Exception as exc:  # pragma: no cover - defensive sampler guard
                self.samples.append(
                    {
                        "elapsed_seconds": max(
                            0.0, time.monotonic() - self.started
                        ),
                        "sampler_error": (
                            "sampler failed; "
                            f"error_type={safe_exception_type(exc)}"
                        ),
                    }
                )
            self._stop.wait(self.interval_seconds)

    def summary(self) -> dict[str, Any]:
        cgroups = [
            sample.get("cgroup")
            for sample in self.samples
            if isinstance(sample.get("cgroup"), Mapping)
            and sample["cgroup"].get("available")
        ]
        process_ios = [
            sample.get("process_io")
            for sample in self.samples
            if isinstance(sample.get("process_io"), Mapping)
        ]
        rss_values = [
            int(sample["process_rss_bytes"])
            for sample in self.samples
            if sample.get("process_rss_bytes") is not None
        ]
        spill_values = [
            int((sample.get("spill") or {}).get("bytes") or 0)
            for sample in self.samples
        ]
        memory_current = [
            int(cgroup["memory_current_bytes"])
            for cgroup in cgroups
            if cgroup.get("memory_current_bytes") is not None
        ]
        memory_peak = [
            int(cgroup["memory_peak_bytes"])
            for cgroup in cgroups
            if cgroup.get("memory_peak_bytes") is not None
        ]
        pids_current = [
            int(cgroup["pids_current_count"])
            for cgroup in cgroups
            if cgroup.get("pids_current_count") is not None
        ]
        pids_peak = [
            int(cgroup["pids_peak_count"])
            for cgroup in cgroups
            if cgroup.get("pids_peak_count") is not None
        ]
        first = cgroups[0] if cgroups else {}
        last = cgroups[-1] if cgroups else {}
        observed_cpusets = sorted(
            {
                str(cgroup["cpuset_cpus_effective"])
                for cgroup in cgroups
                if cgroup.get("cpuset_cpus_effective")
            }
        )
        first_io = first.get("io_totals")
        last_io = last.get("io_totals")
        return {
            "sample_interval_seconds": self.interval_seconds,
            "sample_count": len(self.samples),
            "container_pid": self.pid,
            "process_rss_peak_bytes": max(rss_values, default=None),
            "process_io_delta": _counter_delta(
                process_ios[0] if process_ios else None,
                process_ios[-1] if process_ios else None,
            ),
            "cgroup_memory_current_high_water_bytes": max(
                memory_current, default=None
            ),
            "cgroup_memory_peak_bytes": max(memory_peak, default=None),
            "cgroup_cpu_stat_delta": _counter_delta(
                first.get("cpu_stat"), last.get("cpu_stat")
            ),
            "cgroup_io_delta": _counter_delta(first_io, last_io),
            "cgroup_cpu_pressure_total_delta_usec": _pressure_total_delta(
                first.get("cpu_pressure_parsed"),
                last.get("cpu_pressure_parsed"),
            ),
            "cgroup_io_pressure_total_delta_usec": _pressure_total_delta(
                first.get("io_pressure_parsed"),
                last.get("io_pressure_parsed"),
            ),
            "cgroup_memory_pressure_total_delta_usec": _pressure_total_delta(
                first.get("memory_pressure_parsed"),
                last.get("memory_pressure_parsed"),
            ),
            "cgroup_memory_event_delta": _counter_delta(
                first.get("memory_events"), last.get("memory_events")
            ),
            "pids_current_high_water": max(pids_current, default=None),
            "pids_peak_high_water": max(pids_peak, default=None),
            "spill_high_water_bytes": max(spill_values, default=0),
            "observed_cpuset_cpus_effective": observed_cpusets,
            "effective_cpuset_verified": bool(observed_cpusets)
            and observed_cpusets == [self.expected_cpuset],
            "first_cgroup": first or None,
            "last_cgroup": last or None,
            "samples": self.samples,
        }


def _container_path(path: str, corpus_root: Path) -> str:
    raw = str(path)
    if raw == "/corpus":
        raise ContainerConfigurationError("benchmark source path names the corpus directory")
    if raw.startswith("/corpus/"):
        relative_raw = raw.removeprefix("/corpus/")
        host = (corpus_root / relative_raw).resolve(strict=True)
    else:
        host = Path(raw).expanduser().resolve(strict=True)
    try:
        relative = host.relative_to(corpus_root)
    except ValueError:
        raise ContainerConfigurationError(
            "benchmark source path escapes corpus root"
        ) from None
    return "/corpus/" + relative.as_posix()


def _containerize_request_paths(
    request: Mapping[str, Any], corpus_root: str | Path
) -> dict[str, Any]:
    """Copy a benchmark request and map every sealed source into /corpus."""

    root = Path(corpus_root).expanduser().resolve(strict=True)
    converted = copy.deepcopy(dict(request))
    plan = converted.get("plan")
    if not isinstance(plan, dict):
        raise ContainerConfigurationError("benchmark request has no mutable plan")
    replacements: dict[str, str] = {}
    for field in ("files", "original_files", "resource_keys"):
        values = plan.get(field)
        if values is None:
            continue
        if not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            raise ContainerConfigurationError(f"plan.{field} must be a string list")
        mapped: list[str] = []
        for value in values:
            replacement = _container_path(value, root)
            replacements[value] = replacement
            mapped.append(replacement)
        plan[field] = mapped
    if not plan.get("files"):
        raise ContainerConfigurationError("benchmark plan has no source files")

    def remap(value: Any) -> Any:
        if isinstance(value, str):
            return replacements.get(value, value)
        if isinstance(value, list):
            return [remap(item) for item in value]
        if isinstance(value, tuple):
            return [remap(item) for item in value]
        if isinstance(value, Mapping):
            return {
                replacements.get(key, key) if isinstance(key, str) else key: remap(item)
                for key, item in value.items()
            }
        return value

    converted["plan"] = remap(plan)
    return converted


def _normalize_request(
    request: Mapping[str, Any], config: ContainerRunnerConfig
) -> dict[str, Any]:
    normalized = _containerize_request_paths(request, config.corpus_root)
    configured_threads = normalized.get("threads")
    if configured_threads is None:
        normalized["threads"] = CONTAINER_CPUS
    elif int(configured_threads) != CONTAINER_CPUS:
        raise ContainerConfigurationError(
            f"container series requires exactly {CONTAINER_CPUS} engine threads"
        )
    configured_memory = normalized.get("memory_limit_bytes")
    if configured_memory is None:
        normalized["memory_limit_bytes"] = int(config.engine_memory_bytes)
    else:
        configured_memory = int(configured_memory)
        if not 0 < configured_memory < CONTAINER_MEMORY_BYTES:
            raise ContainerConfigurationError(
                "engine memory limit must be positive and below the 4-GiB cgroup"
            )
        normalized["memory_limit_bytes"] = configured_memory
    return normalized


def _safe_mount(path: Path, destination: str, *, readonly: bool = False) -> str:
    raw = str(path)
    if any(character in raw for character in (",", "\n", "\r")):
        raise ContainerConfigurationError(
            f"Docker --mount path contains an unsupported character: {raw!r}"
        )
    suffix = ",readonly" if readonly else ""
    return f"type=bind,src={raw},dst={destination}{suffix}"


def _docker_command(
    *,
    config: ContainerRunnerConfig,
    request: Mapping[str, Any],
    container_name: str,
    attempt_root: Path,
    cache_dir: Path,
    home_dir: Path,
) -> list[str]:
    memory_limit = int(request["memory_limit_bytes"])
    environments = {
        "HOME": "/bench-home",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONPATH": "/workspace",
        "POLARS_MAX_THREADS": str(CONTAINER_CPUS),
        "OMP_NUM_THREADS": str(CONTAINER_CPUS),
        "OPENBLAS_NUM_THREADS": str(CONTAINER_CPUS),
        "MKL_NUM_THREADS": str(CONTAINER_CPUS),
        "NUMEXPR_NUM_THREADS": str(CONTAINER_CPUS),
        "SUPERTABLE_HOME": "/bench-home",
        "SUPERTABLE_DUCKDB_MEMORY_LIMIT": _duckdb_memory_limit_text(memory_limit),
        "SUPERTABLE_DUCKDB_THREADS": str(CONTAINER_CPUS),
        "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES": str(memory_limit),
        "SUPERTABLE_ISLAND_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_CPU_MAX": str(CONTAINER_CPUS),
        "SUPERTABLE_ISLAND_IO_WORKERS_MAX": str(CONTAINER_CPUS),
        "SUPERTABLE_ISLAND_CACHE_DIR": "/bench-cache",
        "SUPERTABLE_ISLAND_RANGE_CACHE_DIR": "/bench-cache/ranges",
        "SUPERTABLE_ISLAND_SPILL_ENABLED": "true",
        "SUPERTABLE_ISLAND_SPILL_DIR": "/bench/spill",
        "SUPERTABLE_ISLAND_SPILL_MAX_BYTES": str(config.spill_max_bytes),
        "SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES": str(512 * 1024**2),
    }
    if bool(request.get("disable_caches", False)):
        environments.update(
            {
                "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE": "0",
                "SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE": "false",
                "SUPERTABLE_ISLAND_CACHE_ENABLED": "false",
                "SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED": "false",
            }
        )
    command = [
        config.docker,
        "run",
        "--name",
        container_name,
        "--pull",
        "never",
        "--cpus",
        str(CONTAINER_CPUS),
        "--cpuset-cpus",
        config.cpuset_cpus,
        "--memory",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swap",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swappiness",
        "0",
        "--pids-limit",
        str(config.pids_limit),
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--shm-size",
        "256m",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=268435456",
        "--workdir",
        "/workspace",
        "--mount",
        _safe_mount(
            Path(config.repo_root).expanduser().resolve(),
            "/workspace",
            readonly=True,
        ),
        "--mount",
        _safe_mount(
            Path(config.corpus_root).expanduser().resolve(),
            "/corpus",
            readonly=True,
        ),
        "--mount",
        _safe_mount(attempt_root, "/bench"),
        "--mount",
        _safe_mount(cache_dir, "/bench-cache"),
        "--mount",
        _safe_mount(home_dir, "/bench-home"),
        "--entrypoint",
        "python",
    ]
    for name, value in sorted(environments.items()):
        command.extend(("--env", f"{name}={value}"))
    command.extend(
        (
            config.image,
            "-m",
            "supertable.engine.benchmarks.container_worker",
            "/bench/request.json",
            "/bench/response.json",
        )
    )
    return command


def _json_command(arguments: Sequence[str], *, timeout: float = 30) -> Any:
    completed = subprocess.run(
        list(arguments),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise ContainerBenchmarkError(
            f"command failed ({completed.returncode}): {list(arguments)!r}; "
            f"stderr={completed.stderr[-2000:]!r}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        raise ContainerBenchmarkError(
            "benchmark command returned invalid JSON"
        ) from None


def _image_provenance(docker: str, image: str) -> dict[str, Any]:
    raw = _json_command([docker, "image", "inspect", image])
    if not isinstance(raw, list) or not raw or not isinstance(raw[0], Mapping):
        raise ContainerBenchmarkError("docker image inspect returned no image")
    inspect = dict(raw[0])
    repo_digests = list(inspect.get("RepoDigests") or [])
    requested_digest = image.partition("@sha256:")[2]
    digest = (
        f"sha256:{requested_digest}"
        if requested_digest
        else (
            str(repo_digests[0]).partition("@")[2]
            if repo_digests
            else None
        )
    )
    return {
        "reference": image,
        "id": inspect.get("Id"),
        "repo_digests": repo_digests,
        "content_digest": digest,
        "created": inspect.get("Created"),
        "architecture": inspect.get("Architecture"),
        "os": inspect.get("Os"),
        "labels": ((inspect.get("Config") or {}).get("Labels")),
        "inspect": inspect,
    }


def _container_inspect(docker: str, container_name: str) -> dict[str, Any] | None:
    try:
        raw = _json_command([docker, "inspect", container_name])
    except (OSError, subprocess.SubprocessError, ContainerBenchmarkError):
        return None
    if isinstance(raw, list) and raw and isinstance(raw[0], Mapping):
        return dict(raw[0])
    return None


def _run_git(repo_root: Path, *arguments: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout if completed.returncode == 0 else None


def _git_identity(repo_root: Path) -> dict[str, Any]:
    head = _run_git(repo_root, "rev-parse", "HEAD")
    branch = _run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
    describe = _run_git(repo_root, "describe", "--always", "--dirty", "--tags")
    status = _run_git(repo_root, "status", "--short", "--untracked-files=all")
    diff = _run_git(repo_root, "diff", "--binary", "HEAD", "--", ".")
    return {
        "head": head.strip() if head else None,
        "branch": branch.strip() if branch else None,
        "describe": describe.strip() if describe else None,
        "status": status.splitlines() if status is not None else None,
        "dirty": bool(status and status.strip()),
        "tracked_diff_sha256": (
            hashlib.sha256(diff.encode("utf-8")).hexdigest()
            if diff is not None
            else None
        ),
    }


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _write_text(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8", errors="backslashreplace")


def _stop_container(docker: str, container_name: str) -> None:
    try:
        subprocess.run(
            [docker, "stop", "--time", "5", container_name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def _remove_container(docker: str, container_name: str) -> None:
    try:
        subprocess.run(
            [docker, "rm", "--force", container_name],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def _validate_success_boundary(
    *,
    result: Mapping[str, Any],
    response: Mapping[str, Any],
    inspect: Mapping[str, Any] | None,
    config: ContainerRunnerConfig,
) -> None:
    errors: list[str] = []
    host_config = dict((inspect or {}).get("HostConfig") or {})
    state = dict((inspect or {}).get("State") or {})
    if int(host_config.get("Memory") or 0) != CONTAINER_MEMORY_BYTES:
        errors.append("Docker inspect did not retain the 4-GiB memory limit")
    if int(host_config.get("MemorySwap") or 0) != CONTAINER_MEMORY_BYTES:
        errors.append("Docker inspect did not retain zero additional swap")
    if int(host_config.get("NanoCpus") or 0) != CONTAINER_CPUS * 1_000_000_000:
        errors.append("Docker inspect did not retain the four-CPU quota")
    if str(host_config.get("CpusetCpus") or "") != config.cpuset_cpus:
        errors.append("Docker inspect did not retain the requested CPU set")
    if host_config.get("ReadonlyRootfs") is not True:
        errors.append("Docker inspect did not retain a read-only root filesystem")
    if str(host_config.get("NetworkMode") or "") != "none":
        errors.append("Docker inspect did not retain network=none")
    if state.get("OOMKilled") is True:
        errors.append("Docker reported OOMKilled=true")

    context = dict(result.get("execution_context") or {})
    cgroup = dict(context.get("cgroup_v2") or {})
    if cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
        errors.append("worker did not observe the 4-GiB cgroup")
    if cgroup.get("swap_max_bytes") != 0:
        errors.append("worker observed usable cgroup swap")
    memory_events = dict(context.get("cgroup_memory_event_delta") or {})
    if any(
        int(memory_events.get(key) or 0)
        for key in ("oom", "oom_kill", "oom_group_kill")
    ):
        errors.append(f"worker observed OOM events: {memory_events}")
    if int(context.get("configured_threads") or 0) != CONTAINER_CPUS:
        errors.append("worker did not retain four configured threads")

    provenance = response.get("worker_provenance")
    after = (
        provenance.get("after")
        if isinstance(provenance, Mapping)
        else None
    )
    if not isinstance(after, Mapping):
        errors.append("worker response has no dependency/runtime provenance")
    else:
        observed_affinity = tuple(
            int(cpu) for cpu in (after.get("cpu_affinity") or [])
        )
        if observed_affinity != _parse_cpuset(config.cpuset_cpus):
            errors.append(
                f"worker CPU affinity {observed_affinity} differs from "
                f"cpuset {config.cpuset_cpus}"
            )
        worker_cgroup = dict(after.get("cgroup_v2") or {})
        if worker_cgroup.get("cpuset_cpus_effective") != config.cpuset_cpus:
            errors.append(
                "worker did not observe the exact effective cgroup cpuset"
            )
    if errors:
        raise ContainerBenchmarkError("; ".join(errors))


def _attempt_label(request: Mapping[str, Any]) -> str:
    engine = str(request.get("engine") or "engine")
    purpose = str(request.get("purpose") or "series")
    raw = f"{purpose}-{engine}".lower()
    safe = re.sub(r"[^a-z0-9_.-]+", "-", raw).strip("-.") or "series"
    return safe[:48]


def run_container_series(
    request: Mapping[str, Any],
    *,
    config: ContainerRunnerConfig,
    cache_dir: str | Path,
    home_dir: str | Path,
    timeout_seconds: float = 3600,
) -> dict[str, Any]:
    """Run one complete engine series in a fresh constrained container."""

    if timeout_seconds <= 0:
        raise ContainerConfigurationError("timeout_seconds must be positive")
    repo_root = Path(config.repo_root).expanduser().resolve(strict=True)
    corpus_root = Path(config.corpus_root).expanduser().resolve(strict=True)
    artifact_root = Path(config.artifact_root).expanduser().resolve()
    if not repo_root.is_dir() or not corpus_root.is_dir():
        raise ContainerConfigurationError("repo_root and corpus_root must be directories")
    artifact_root.mkdir(parents=True, exist_ok=True)
    cache_path = Path(cache_dir).expanduser().resolve()
    home_path = Path(home_dir).expanduser().resolve()
    cache_path.mkdir(parents=True, exist_ok=True)
    home_path.mkdir(parents=True, exist_ok=True)

    normalized = _normalize_request(request, config)
    attempt_id = f"{_attempt_label(normalized)}-{uuid.uuid4().hex[:12]}"
    attempt_root = artifact_root / attempt_id
    attempt_root.mkdir(parents=False, exist_ok=False)
    spill_root = attempt_root / "spill"
    spill_root.mkdir()
    request_path = attempt_root / "request.json"
    response_path = attempt_root / "response.json"
    _atomic_write_json(request_path, normalized)
    request_digest = hashlib.sha256(request_path.read_bytes()).hexdigest()

    container_name = f"supertable-bench-{attempt_id}"[:120]
    image_provenance: dict[str, Any] | None = None
    image_error: str | None = None
    try:
        image_provenance = _image_provenance(config.docker, config.image)
    except Exception as exc:  # preserve a useful artifact for setup failures
        image_error = (
            "image provenance failed; "
            f"error_type={safe_exception_type(exc)}"
        )
    git = _git_identity(repo_root)
    command = _docker_command(
        config=config,
        request=normalized,
        container_name=container_name,
        attempt_root=attempt_root,
        cache_dir=cache_path,
        home_dir=home_path,
    )
    started_unix_ms = int(time.time() * 1000)
    started = time.monotonic()
    sampler = _ContainerSampler(
        docker=config.docker,
        container_name=container_name,
        spill_root=spill_root,
        started=started,
        interval_seconds=float(config.sample_interval_seconds),
        expected_cpuset=config.cpuset_cpus,
    )
    process: subprocess.Popen[str] | None = None
    stdout = ""
    stderr = ""
    returncode: int | None = None
    timed_out = False
    launch_error: str | None = image_error
    inspect: dict[str, Any] | None = None
    sampler_started = False
    if image_error is None:
        try:
            process = subprocess.Popen(
                command,
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            sampler.start()
            sampler_started = True
            try:
                stdout, stderr = process.communicate(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                timed_out = True
                _stop_container(config.docker, container_name)
                try:
                    stdout, stderr = process.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
            returncode = process.returncode
        except OSError as exc:
            launch_error = (
                "container launch failed; "
                f"error_type={safe_exception_type(exc)}"
            )
        finally:
            if sampler_started:
                sampler.stop()
            inspect = _container_inspect(config.docker, container_name)
            _remove_container(config.docker, container_name)
    elapsed_seconds = max(0.0, time.monotonic() - started)
    _write_text(attempt_root / "stdout.log", stdout)
    _write_text(attempt_root / "stderr.log", stderr)

    response: dict[str, Any] | None = None
    response_error: str | None = None
    if response_path.is_file():
        try:
            loaded = json.loads(response_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                response = loaded
            else:
                response_error = "response root is not an object"
        except (OSError, json.JSONDecodeError) as exc:
            response_error = (
                "container response unavailable; "
                f"error_type={safe_exception_type(exc)}"
            )

    status = "worker_failed"
    validation_error: str | None = None
    result: dict[str, Any] | None = None
    if image_error is not None:
        status = "image_inspect_failed"
        validation_error = image_error
    elif timed_out:
        status = "timeout"
        validation_error = f"container exceeded {timeout_seconds} seconds"
    elif launch_error is not None:
        status = "launch_failed"
        validation_error = launch_error
    elif response is None:
        status = "response_missing"
        validation_error = response_error or "worker produced no response.json"
    elif not response.get("ok"):
        status = "worker_failed"
        validation_error = str(response.get("error") or "worker failed")
    elif returncode != 0:
        status = "worker_failed"
        validation_error = f"docker run exited {returncode} despite a success response"
    elif not isinstance(response.get("result"), Mapping):
        status = "worker_failed"
        validation_error = "worker success response has no engine series"
    else:
        result = dict(response["result"])
        try:
            _validate_success_boundary(
                result=result,
                response=response,
                inspect=inspect,
                config=config,
            )
            status = "passed"
        except ContainerBenchmarkError as exc:
            status = "boundary_failed"
            validation_error = (
                "container boundary validation failed; "
                f"error_type={safe_exception_type(exc)}"
            )

    sampler_summary = sampler.summary()
    artifact = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "fresh_container_engine_series",
        "attempt_id": attempt_id,
        "status": status,
        "started_unix_ms": started_unix_ms,
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": timeout_seconds,
        "timed_out": timed_out,
        "returncode": returncode,
        "container_name": container_name,
        "limits": {
            "cpus": CONTAINER_CPUS,
            "cpuset_cpus": config.cpuset_cpus,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "pids": config.pids_limit,
            "engine_memory_bytes": normalized["memory_limit_bytes"],
            "spill_max_bytes": config.spill_max_bytes,
        },
        "request_sha256": request_digest,
        "request": normalized,
        "response": response,
        "response_error": response_error,
        "validation_error": validation_error,
        "launch_error": launch_error,
        "command": command,
        "docker_inspect": inspect,
        "host_sampler": sampler_summary,
        "provenance": {
            "git": git,
            "image": image_provenance,
            "worker": (
                response.get("worker_provenance")
                if isinstance(response, Mapping)
                else None
            ),
        },
        "spill_after_worker": _tree_footprint(spill_root),
        "artifacts": {
            "root": str(attempt_root),
            "request": str(request_path),
            "response": str(response_path) if response_path.is_file() else None,
            "stdout": str(attempt_root / "stdout.log"),
            "stderr": str(attempt_root / "stderr.log"),
        },
    }
    _atomic_write_json(attempt_root / "attempt.json", artifact)
    if status != "passed" or result is None:
        raise ContainerBenchmarkError(
            f"container benchmark {status}: {validation_error}"
        )

    # Keep the complete raw response in attempt.json without recursively
    # duplicating the returned series inside itself.  The comparison artifact
    # still carries all host samples, inspect data, and exact provenance.
    result["container_run"] = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "attempt_id": attempt_id,
        "status": status,
        "elapsed_seconds": elapsed_seconds,
        "artifact": str(attempt_root / "attempt.json"),
        "limits": artifact["limits"],
        "request_sha256": request_digest,
        "docker_inspect": inspect,
        "host_sampler": sampler_summary,
        "provenance": artifact["provenance"],
        "spill_after_worker": artifact["spill_after_worker"],
    }
    return result


class DockerWorkerRunner:
    """Drop-in callable for ``compare_manifest(..., worker_runner=...)``."""

    def __init__(self, config: ContainerRunnerConfig) -> None:
        self.config = config

    def __call__(
        self,
        request: Mapping[str, Any],
        *,
        cache_dir: str | Path,
        home_dir: str | Path,
        timeout_seconds: float = 3600,
    ) -> dict[str, Any]:
        return run_container_series(
            request,
            config=self.config,
            cache_dir=cache_dir,
            home_dir=home_dir,
            timeout_seconds=timeout_seconds,
        )


__all__ = [
    "ARTIFACT_FORMAT_VERSION",
    "CONTAINER_CPUS",
    "CONTAINER_MEMORY_BYTES",
    "ContainerBenchmarkError",
    "ContainerConfigurationError",
    "ContainerRunnerConfig",
    "DockerWorkerRunner",
    "run_container_series",
]
