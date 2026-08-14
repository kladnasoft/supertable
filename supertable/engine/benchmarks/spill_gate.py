"""Hard-bounded IslandDB-only regression gate for the 10-GiB spill workload.

The comparison harness normally launches DuckDB and IslandDB together.  That
is useful when establishing a new oracle, but it needlessly repeats the
DuckDB run while tuning IslandDB.  This module reuses a previously sealed
DuckDB request/response pair, runs only IslandDB in a fresh Docker container,
and always writes an attempt artifact -- including when the container exceeds
the deadline and has to be killed.

This is deliberately a benchmark tool, not an engine launcher.  It accepts
only the one-sample ``spill_group`` request, fixes the container to four CPUs,
4 GiB with no swap, and treats exact result parity as a blocking gate.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .runner import (
    ENGINE_DUCKDB,
    ENGINE_ISLAND,
    BenchmarkParityError,
    _cgroup_v2_memory_telemetry,
    assert_exact_parity,
    result_digest,
)


GIB = 1024**3
CONTAINER_CPUS = 4
CONTAINER_MEMORY_BYTES = 4 * GIB
ENGINE_MEMORY_BYTES = 2 * GIB
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_TARGET_SECONDS = 100.0
ARTIFACT_FORMAT_VERSION = 1
SEALED_ORACLE_DIGEST = (
    "aa8ee7939389b6be670d92edd9eda4755522a4c0ba8e230263cec109b0ec3407"
)
EXPECTED_RESULT_ROWS = 1_024
EXPECTED_INPUT_ROWS = 6_413_677

EXIT_SUCCESS = 0
EXIT_CONFIGURATION = 2
EXIT_TIMEOUT = 3
EXIT_WORKER_FAILURE = 4
EXIT_PARITY_FAILURE = 5
EXIT_TARGET_MISSED = 6


class SpillGateError(RuntimeError):
    """Base error for invalid or failed spill-gate attempts."""


class SpillGateConfigurationError(SpillGateError):
    """Raised before Docker starts when inputs do not describe the sealed run."""


@dataclass(frozen=True)
class GateInputs:
    request: dict[str, Any]
    oracle_series: dict[str, Any]
    oracle_digest: str
    plan_digest: str


def _read_json(path: str | Path) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SpillGateConfigurationError(
            f"cannot read JSON from {source}: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SpillGateConfigurationError(f"{source} must contain a JSON object")
    return value


def _strict_json_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _unwrap_success_response(
    response: Mapping[str, Any], *, label: str
) -> dict[str, Any]:
    if "ok" in response:
        if response.get("ok") is not True:
            raise SpillGateConfigurationError(
                f"{label} is not a successful worker response: "
                f"{response.get('error', 'unknown error')}"
            )
        series = response.get("result")
    else:
        series = response
    if not isinstance(series, Mapping):
        raise SpillGateConfigurationError(f"{label} has no worker result object")
    return dict(series)


def _same_request_contract(
    oracle_request: Mapping[str, Any], candidate: Mapping[str, Any]
) -> None:
    if oracle_request.get("engine") != ENGINE_DUCKDB:
        raise SpillGateConfigurationError("oracle request must use explicit duckdb")
    if candidate.get("engine") not in (ENGINE_DUCKDB, ENGINE_ISLAND):
        raise SpillGateConfigurationError(
            "Island request template must use an explicit benchmark engine"
        )
    if oracle_request.get("plan") != candidate.get("plan"):
        raise SpillGateConfigurationError(
            "Island request plan differs from the DuckDB oracle request"
        )
    for key in (
        "cold_mode",
        "disable_caches",
        "memory_limit_bytes",
        "minimum_cold_read_fraction",
        "threads",
        "warm_repeats",
    ):
        if oracle_request.get(key) != candidate.get(key):
            raise SpillGateConfigurationError(
                f"Island request {key!r} differs from the DuckDB oracle request"
            )


def _validate_expected_result(canonical: Mapping[str, Any], *, label: str) -> None:
    columns = canonical.get("columns")
    rows = canonical.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise SpillGateConfigurationError(f"{label} is not a canonical table")
    if len(rows) != EXPECTED_RESULT_ROWS:
        raise SpillGateConfigurationError(
            f"{label} returned {len(rows):,} rows, expected {EXPECTED_RESULT_ROWS:,}"
        )
    try:
        count_index = columns.index("id_count")
    except ValueError as exc:
        raise SpillGateConfigurationError(f"{label} has no id_count column") from exc
    counts: list[int] = []
    for row in rows:
        if not isinstance(row, list) or count_index >= len(row):
            raise SpillGateConfigurationError(f"{label} contains a malformed row")
        count = row[count_index]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SpillGateConfigurationError(
                f"{label} contains a non-integer id_count"
            )
        counts.append(count)
    observed = sum(counts)
    if observed != EXPECTED_INPUT_ROWS:
        raise SpillGateConfigurationError(
            f"{label} id_count sum is {observed:,}, expected {EXPECTED_INPUT_ROWS:,}"
        )


def _sealed_oracle_group_domain(canonical: Mapping[str, Any]) -> dict[str, Any]:
    """Derive the benchmark-only dimension proof from the sealed oracle.

    Older persisted requests predate ``integer_domain_bounds``.  Rebuilding
    the DuckDB oracle solely to add a planning hint would make iterative spill
    work slower and would weaken reproducibility.  The oracle's exact GROUP BY
    result already proves the complete domain for the identical scan: require
    every integer value 0..1023 exactly once and no NULL before injecting that
    hint into the Island-only request.  Production never uses this path; its
    proof comes from snapshot-sealed Parquet statistics.
    """
    columns = canonical.get("columns")
    rows = canonical.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise SpillGateConfigurationError("DuckDB oracle is not a canonical table")
    try:
        dimension_index = columns.index("dimension")
    except ValueError as exc:
        raise SpillGateConfigurationError(
            "DuckDB spill oracle has no dimension group column"
        ) from exc
    dimensions = []
    for row in rows:
        if not isinstance(row, list) or dimension_index >= len(row):
            raise SpillGateConfigurationError(
                "DuckDB spill oracle contains a malformed dimension row"
            )
        value = row[dimension_index]
        if isinstance(value, bool) or not isinstance(value, int):
            raise SpillGateConfigurationError(
                "DuckDB spill oracle dimension is not a non-NULL integer"
            )
        dimensions.append(value)
    if sorted(dimensions) != list(range(EXPECTED_RESULT_ROWS)):
        raise SpillGateConfigurationError(
            "DuckDB spill oracle does not prove the exact 0..1023 dimension domain"
        )
    return {
        "dimension": {
            "minimum": 0,
            "maximum": EXPECTED_RESULT_ROWS - 1,
            "has_null": False,
        }
    }


def load_gate_inputs(
    *,
    request_template: str | Path,
    oracle_request: str | Path,
    oracle_response: str | Path,
) -> GateInputs:
    """Load and cryptographically validate the reusable oracle contract."""
    template = _read_json(request_template)
    oracle_request_value = _read_json(oracle_request)
    oracle_response_value = _read_json(oracle_response)
    _same_request_contract(oracle_request_value, template)

    plan = template.get("plan")
    if not isinstance(plan, Mapping):
        raise SpillGateConfigurationError("request template has no plan object")
    if plan.get("name") != "spill_group":
        raise SpillGateConfigurationError("spill gate accepts only spill_group")
    if plan.get("island_streaming_result") is not True:
        raise SpillGateConfigurationError(
            "spill_group must use IslandDB's bounded streaming result"
        )
    if float(plan.get("projected_source_fraction") or 0.0) < 0.95:
        raise SpillGateConfigurationError(
            "spill_group must project at least 95% of the physical source"
        )
    if int(plan.get("source_bytes") or 0) < 10 * GIB:
        raise SpillGateConfigurationError(
            "spill gate requires the sealed physical 10-GiB workload"
        )
    if int(template.get("warm_repeats") or 0) != 0:
        raise SpillGateConfigurationError(
            "spill gate runs exactly one cold sample; warm_repeats must be zero"
        )
    if int(template.get("threads") or 0) != CONTAINER_CPUS:
        raise SpillGateConfigurationError("spill gate requires exactly four threads")
    if int(template.get("memory_limit_bytes") or 0) != ENGINE_MEMORY_BYTES:
        raise SpillGateConfigurationError(
            "spill gate requires a 2-GiB engine workspace inside the 4-GiB cgroup"
        )

    oracle_series = _unwrap_success_response(
        oracle_response_value, label="DuckDB oracle response"
    )
    if oracle_series.get("engine") != ENGINE_DUCKDB:
        raise SpillGateConfigurationError(
            "oracle response did not execute explicit duckdb"
        )
    canonical = oracle_series.get("result")
    if not isinstance(canonical, Mapping):
        raise SpillGateConfigurationError("oracle response has no canonical result")
    digest = result_digest(canonical)
    if digest != oracle_series.get("result_digest"):
        raise SpillGateConfigurationError(
            "DuckDB oracle digest does not match its canonical result"
        )
    if digest != SEALED_ORACLE_DIGEST:
        raise SpillGateConfigurationError(
            f"DuckDB oracle digest {digest} is not the sealed spill oracle "
            f"{SEALED_ORACLE_DIGEST}"
        )
    _validate_expected_result(canonical, label="DuckDB oracle")
    oracle_group_domain = _sealed_oracle_group_domain(canonical)
    samples = oracle_series.get("samples")
    if not isinstance(samples, list) or len(samples) != 1:
        raise SpillGateConfigurationError(
            "DuckDB oracle must contain exactly one cold sample"
        )
    if samples[0].get("result_digest") != digest:
        raise SpillGateConfigurationError("DuckDB cold sample changed from its oracle")

    request = copy.deepcopy(template)
    request["engine"] = ENGINE_ISLAND
    request["purpose"] = "spill-regression-gate"
    request_plan = request.get("plan")
    if not isinstance(request_plan, dict):
        raise SpillGateConfigurationError("request template plan must be mutable")
    existing_domain = request_plan.get("integer_domain_bounds")
    if existing_domain not in (None, {}, oracle_group_domain):
        raise SpillGateConfigurationError(
            "request template contains a group domain that conflicts with the oracle"
        )
    request_plan["integer_domain_bounds"] = oracle_group_domain
    return GateInputs(
        request=request,
        oracle_series=oracle_series,
        oracle_digest=digest,
        plan_digest=_strict_json_digest(request_plan),
    )


def _tree_footprint(root: Path) -> dict[str, int]:
    files = 0
    total = 0
    try:
        iterator = root.rglob("*")
        for path in iterator:
            try:
                if path.is_file():
                    files += 1
                    total += path.stat().st_size
            except OSError:
                continue
    except OSError:
        pass
    return {"files": files, "bytes": total}


def _read_rss(pid: int) -> int | None:
    try:
        lines = Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines()
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
    values: dict[str, int] = {}
    for line in lines:
        key, separator, raw = line.partition(":")
        if not separator:
            continue
        try:
            values[key.strip()] = max(0, int(raw.strip()))
        except ValueError:
            continue
    return values or None


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


class _ContainerSampler:
    """Sample a live container from the host, independent of the worker GIL."""

    def __init__(
        self,
        *,
        docker: str,
        container_name: str,
        spill_root: Path,
        started: float,
        interval_seconds: float,
    ) -> None:
        self.docker = docker
        self.container_name = container_name
        self.spill_root = spill_root
        self.started = started
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self.pid: int | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

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
        cgroup: dict[str, Any] = {"available": False}
        rss = None
        process_io = None
        if self.pid is not None:
            rss = _read_rss(self.pid)
            process_io = _read_proc_io(self.pid)
            cgroup = _cgroup_v2_memory_telemetry(
                proc_cgroup=f"/proc/{self.pid}/cgroup"
            )
        self.samples.append({
            "elapsed_seconds": max(0.0, time.monotonic() - self.started),
            "container_pid": self.pid,
            "process_rss_bytes": rss,
            "process_io": process_io,
            "cgroup": {
                "available": cgroup.get("available", False),
                "memory_current_bytes": cgroup.get("memory_current_bytes"),
                "memory_peak_bytes": cgroup.get("memory_peak_bytes"),
                "memory_max_bytes": cgroup.get("memory_max_bytes"),
                "swap_current_bytes": cgroup.get("swap_current_bytes"),
                "swap_peak_bytes": cgroup.get("swap_peak_bytes"),
                "swap_max_bytes": cgroup.get("swap_max_bytes"),
                "memory_events": cgroup.get("memory_events"),
                "memory_pressure": cgroup.get("memory_pressure"),
                "io": _cgroup_io_totals(cgroup.get("io_stat")),
            },
            "spill": _tree_footprint(self.spill_root),
        })

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval_seconds)

    def summary(self) -> dict[str, Any]:
        rss_values = [
            int(sample["process_rss_bytes"])
            for sample in self.samples
            if sample.get("process_rss_bytes") is not None
        ]
        spill_values = [int(sample["spill"]["bytes"]) for sample in self.samples]
        memory_values = [
            int(sample["cgroup"]["memory_peak_bytes"])
            for sample in self.samples
            if sample["cgroup"].get("memory_peak_bytes") is not None
        ]
        return {
            "sample_interval_seconds": self.interval_seconds,
            "sample_count": len(self.samples),
            "container_pid": self.pid,
            "process_rss_peak_bytes": max(rss_values, default=None),
            "cgroup_memory_peak_bytes": max(memory_values, default=None),
            "spill_high_water_bytes": max(spill_values, default=0),
            "last": self.samples[-1] if self.samples else None,
            "samples": self.samples,
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


def _cleanup_spill_root(spill_root: Path, *, attempt_root: Path) -> dict[str, Any]:
    """Remove only this attempt's abandoned query-private spill children."""
    errors: list[str] = []
    try:
        resolved_attempt = attempt_root.resolve(strict=True)
        resolved_spill = spill_root.resolve(strict=True)
        relative = resolved_spill.relative_to(resolved_attempt)
        if relative != Path("island-spill"):
            raise ValueError("spill root is not the attempt's island-spill directory")
    except (OSError, ValueError) as exc:
        return {"attempted": False, "errors": [f"unsafe_path:{type(exc).__name__}:{exc}"]}

    for child in list(resolved_spill.iterdir()):
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError as exc:
            errors.append(f"{child.name}:{type(exc).__name__}:{exc}")
    return {"attempted": True, "errors": errors}


def _docker_command(
    *,
    docker: str,
    image: str,
    container_name: str,
    repo_root: Path,
    corpus_root: Path,
    attempt_root: Path,
) -> list[str]:
    environments = {
        "HOME": "/bench/home",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONPATH": "/workspace",
        "POLARS_MAX_THREADS": str(CONTAINER_CPUS),
        "SUPERTABLE_HOME": "/bench/home",
        "SUPERTABLE_DUCKDB_MEMORY_LIMIT": "2GiB",
        "SUPERTABLE_DUCKDB_THREADS": str(CONTAINER_CPUS),
        "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE": "0",
        "SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE": "false",
        "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES": str(ENGINE_MEMORY_BYTES),
        "SUPERTABLE_ISLAND_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_CPU_MAX": str(CONTAINER_CPUS),
        "SUPERTABLE_ISLAND_IO_WORKERS_MAX": str(CONTAINER_CPUS),
        "SUPERTABLE_ISLAND_CACHE_ENABLED": "false",
        "SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED": "false",
        "SUPERTABLE_ISLAND_CACHE_DIR": "/bench/cache",
        "SUPERTABLE_ISLAND_RANGE_CACHE_DIR": "/bench/cache/ranges",
        "SUPERTABLE_ISLAND_SPILL_ENABLED": "true",
        "SUPERTABLE_ISLAND_SPILL_DIR": "/bench/island-spill",
        "SUPERTABLE_ISLAND_SPILL_MAX_BYTES": str(64 * GIB),
        "SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES": str(512 * 1024**2),
    }
    command = [
        docker,
        "run",
        "--name",
        container_name,
        "--pull",
        "never",
        "--cpus",
        str(CONTAINER_CPUS),
        "--memory",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swap",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swappiness",
        "0",
        "--pids-limit",
        "1024",
        # The bind-mounted corpus and attempt directory belong to the caller.
        # The image's named user can have a different numeric uid/gid, which
        # makes an otherwise valid benchmark fail before it can write its
        # response or spill files.  Preserve the caller's numeric identity;
        # the container remains unprivileged and no-new-privileges below.
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--network",
        "none",
        "--read-only",
        "--security-opt",
        "no-new-privileges:true",
        "--shm-size",
        "256m",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=268435456",
        "--workdir",
        "/workspace",
        "--mount",
        f"type=bind,src={repo_root},dst=/workspace,readonly",
        "--mount",
        f"type=bind,src={corpus_root},dst=/corpus,readonly",
        "--mount",
        f"type=bind,src={attempt_root},dst=/bench",
        "--entrypoint",
        "python",
    ]
    for name, value in sorted(environments.items()):
        command.extend(("--env", f"{name}={value}"))
    command.extend((
        image,
        "-m",
        "supertable.engine.benchmarks._worker",
        "/bench/request.json",
        "/bench/response.json",
    ))
    return command


def _docker_state(docker: str, container_name: str) -> Any:
    try:
        completed = subprocess.run(
            [docker, "inspect", container_name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if completed.returncode == 0:
            return json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        pass
    return None


def _stop_container(docker: str, container_name: str) -> None:
    try:
        subprocess.run(
            [docker, "stop", "--time", "2", container_name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        subprocess.run(
            [docker, "kill", container_name],
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


def _git_identity(repo_root: Path) -> dict[str, Any]:
    def git(*arguments: str) -> str | None:
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

    head = git("rev-parse", "HEAD")
    status = git("status", "--short")
    diff = git("diff", "--binary", "HEAD", "--", "supertable/engine")
    return {
        "head": head.strip() if head else None,
        "status": status.splitlines() if status is not None else None,
        "engine_diff_sha256": (
            hashlib.sha256(diff.encode("utf-8")).hexdigest()
            if diff is not None else None
        ),
    }


def _successful_result(
    response: Mapping[str, Any], inputs: GateInputs
) -> dict[str, Any]:
    series = _unwrap_success_response(response, label="IslandDB response")
    if series.get("engine") != ENGINE_ISLAND:
        raise SpillGateConfigurationError(
            "worker response did not execute explicit islanddb"
        )
    digest = assert_exact_parity(
        inputs.oracle_series,
        series,
        label="10gib/spill_group/reused-oracle",
    )
    if digest != inputs.oracle_digest:
        raise BenchmarkParityError("IslandDB result differs from the sealed digest")
    canonical = series.get("result")
    if not isinstance(canonical, Mapping):
        raise SpillGateConfigurationError("IslandDB response has no canonical result")
    _validate_expected_result(canonical, label="IslandDB result")
    samples = series.get("samples")
    if not isinstance(samples, list) or len(samples) != 1:
        raise SpillGateConfigurationError(
            "IslandDB gate must return exactly one cold sample"
        )
    sample = samples[0]
    if sample.get("temperature") != "cold":
        raise SpillGateConfigurationError("IslandDB gate sample is not cold")
    if sample.get("result_digest") != digest:
        raise BenchmarkParityError("IslandDB cold sample digest changed")
    context = series.get("execution_context") or {}
    cgroup = context.get("cgroup_v2") or {}
    if cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
        raise SpillGateConfigurationError("container did not enforce the 4-GiB limit")
    if cgroup.get("swap_max_bytes") != 0:
        raise SpillGateConfigurationError("container exposed usable swap")
    if int(context.get("configured_threads") or 0) != CONTAINER_CPUS:
        raise SpillGateConfigurationError("worker did not retain four configured threads")
    if int(context.get("polars_thread_pool_size") or 0) != CONTAINER_CPUS:
        raise SpillGateConfigurationError("Polars did not expose four worker threads")
    event_delta = context.get("cgroup_memory_event_delta") or {}
    if any(int(event_delta.get(key) or 0) for key in ("oom", "oom_kill", "oom_group_kill")):
        raise SpillGateConfigurationError(f"container recorded OOM events: {event_delta}")
    return series


def _result_metrics(series: Mapping[str, Any]) -> dict[str, Any]:
    sample = list(series.get("samples") or [None])[0] or {}
    profile = sample.get("engine_profile") or {}
    context = series.get("execution_context") or {}
    return {
        "wall_seconds": sample.get("wall_seconds"),
        "cpu_seconds": sample.get("cpu_seconds"),
        "mean_cpu_cores": (
            float(sample["cpu_seconds"]) / float(sample["wall_seconds"])
            if float(sample.get("wall_seconds") or 0.0) > 0.0 else None
        ),
        "rss_peak_bytes": sample.get("rss_peak_bytes"),
        "process_io_delta": sample.get("process_io_delta"),
        "spill_bytes": profile.get("spill_bytes"),
        "spill": profile.get("spill"),
        "rows_scanned": profile.get("rows_scanned"),
        "result_rows": profile.get("result_rows"),
        "optimized_plan": profile.get("optimized_plan"),
        "cgroup_v2": context.get("cgroup_v2"),
        "cgroup_memory_event_delta": context.get("cgroup_memory_event_delta"),
        "polars_thread_pool_size": context.get("polars_thread_pool_size"),
    }


def run_attempt(
    *,
    inputs: GateInputs,
    attempt_root: Path,
    repo_root: Path,
    corpus_root: Path,
    docker: str,
    image: str,
    timeout_seconds: float,
    target_seconds: float,
    sample_interval_seconds: float,
    retain_failed_spill: bool = False,
) -> tuple[int, dict[str, Any]]:
    """Run one fresh IslandDB container and return its blocking gate status."""
    attempt_root.mkdir(parents=True, exist_ok=False)
    for directory in ("home", "cache", "island-spill"):
        (attempt_root / directory).mkdir()
    _atomic_write_json(attempt_root / "request.json", inputs.request)

    container_name = f"islanddb-spill-gate-{uuid.uuid4().hex[:12]}"
    command = _docker_command(
        docker=docker,
        image=image,
        container_name=container_name,
        repo_root=repo_root,
        corpus_root=corpus_root,
        attempt_root=attempt_root,
    )
    started_unix_ms = int(time.time() * 1000)
    started = time.monotonic()
    sampler = _ContainerSampler(
        docker=docker,
        container_name=container_name,
        spill_root=attempt_root / "island-spill",
        started=started,
        interval_seconds=sample_interval_seconds,
    )
    timed_out = False
    stdout = ""
    stderr = ""
    returncode: int | None = None
    process: subprocess.Popen[str] | None = None
    launch_error: str | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        sampler.start()
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            _stop_container(docker, container_name)
            try:
                stdout, stderr = process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
        returncode = process.returncode
    except OSError as exc:
        launch_error = f"{type(exc).__name__}: {exc}"
    finally:
        sampler.stop()
        docker_state = _docker_state(docker, container_name)
        _remove_container(docker, container_name)

    elapsed_seconds = max(0.0, time.monotonic() - started)
    _write_text(attempt_root / "stdout.log", stdout)
    _write_text(attempt_root / "stderr.log", stderr)
    response_path = attempt_root / "response.json"
    response: dict[str, Any] | None = None
    response_error: str | None = None
    if response_path.is_file():
        try:
            response = _read_json(response_path)
        except SpillGateConfigurationError as exc:
            response_error = str(exc)

    status = "worker_failed"
    exit_code = EXIT_WORKER_FAILURE
    result_metrics = None
    parity_matched = False
    validation_error = None
    if timed_out:
        status = "timeout"
        exit_code = EXIT_TIMEOUT
    elif launch_error is not None:
        validation_error = launch_error
    elif returncode != 0:
        validation_error = (
            response.get("error") if response is not None else response_error
        ) or f"docker run exited {returncode}"
    elif response is None:
        validation_error = response_error or "worker produced no response.json"
    else:
        try:
            series = _successful_result(response, inputs)
            parity_matched = True
            result_metrics = _result_metrics(series)
            wall_seconds = float(result_metrics.get("wall_seconds") or 0.0)
            if wall_seconds > timeout_seconds:
                status = "timeout_contract_breached"
                exit_code = EXIT_TIMEOUT
            elif wall_seconds > target_seconds:
                status = "target_missed"
                exit_code = EXIT_TARGET_MISSED
            else:
                status = "passed"
                exit_code = EXIT_SUCCESS
        except BenchmarkParityError as exc:
            status = "parity_failed"
            exit_code = EXIT_PARITY_FAILURE
            validation_error = str(exc)
        except SpillGateError as exc:
            validation_error = str(exc)

    spill_after_worker = _tree_footprint(attempt_root / "island-spill")
    if exit_code == EXIT_SUCCESS and spill_after_worker["files"]:
        status = "spill_cleanup_failed"
        exit_code = EXIT_WORKER_FAILURE
        validation_error = (
            "successful IslandDB response left "
            f"{spill_after_worker['files']:,} spill files "
            f"({spill_after_worker['bytes']:,} bytes)"
        )

    spill_cleanup = {"attempted": False, "errors": []}
    if spill_after_worker["files"] and (
        exit_code != EXIT_SUCCESS and not retain_failed_spill
    ):
        spill_cleanup = _cleanup_spill_root(
            attempt_root / "island-spill", attempt_root=attempt_root
        )
    spill_after_gate_cleanup = _tree_footprint(attempt_root / "island-spill")
    if spill_cleanup["errors"]:
        suffix = "; ".join(spill_cleanup["errors"])
        validation_error = (
            f"{validation_error}; spill cleanup: {suffix}"
            if validation_error else f"spill cleanup: {suffix}"
        )

    oracle_metrics = _result_metrics(inputs.oracle_series)
    island_wall = (
        float(result_metrics["wall_seconds"])
        if result_metrics and result_metrics.get("wall_seconds") is not None
        else None
    )
    oracle_wall = float(oracle_metrics.get("wall_seconds") or 0.0)
    comparison = {
        "duckdb_oracle_wall_seconds": oracle_wall or None,
        "islanddb_wall_seconds": island_wall,
        "islanddb_over_duckdb_wall_ratio": (
            island_wall / oracle_wall
            if island_wall is not None and oracle_wall > 0 else None
        ),
        "islanddb_speedup_over_duckdb": (
            oracle_wall / island_wall
            if island_wall is not None and island_wall > 0 else None
        ),
        "islanddb_faster_than_duckdb": (
            island_wall < oracle_wall
            if island_wall is not None and oracle_wall > 0 else None
        ),
    }

    artifact = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "islanddb_10gib_4cpu_4gib_spill_gate",
        "status": status,
        "exit_code": exit_code,
        "started_unix_ms": started_unix_ms,
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": timeout_seconds,
        "target_seconds": target_seconds,
        "timed_out": timed_out,
        "parity": {
            "matched": parity_matched,
            "oracle": ENGINE_DUCKDB,
            "oracle_reused": True,
            "oracle_digest": inputs.oracle_digest,
            "plan_digest": inputs.plan_digest,
        },
        "limits": {
            "cpus": CONTAINER_CPUS,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "engine_memory_bytes": ENGINE_MEMORY_BYTES,
        },
        "container": {
            "name": container_name,
            "image": image,
            "returncode": returncode,
            "state_before_removal": docker_state,
            "launch_error": launch_error,
        },
        "validation_error": validation_error,
        "result_metrics": result_metrics,
        "comparison": comparison,
        "host_sampler": sampler.summary(),
        "spill_after_worker": spill_after_worker,
        "spill_cleanup": spill_cleanup,
        "spill_after_gate_cleanup": spill_after_gate_cleanup,
        "git": _git_identity(repo_root),
        "artifacts": {
            "request": str(attempt_root / "request.json"),
            "response": str(response_path) if response_path.is_file() else None,
            "stdout": str(attempt_root / "stdout.log"),
            "stderr": str(attempt_root / "stderr.log"),
        },
    }
    _atomic_write_json(attempt_root / "attempt.json", artifact)
    return exit_code, artifact


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m supertable.engine.benchmarks.spill_gate",
        description=(
            "Run only IslandDB's sealed 10-GiB spill workload in a hard "
            "4-CPU/4-GiB/no-swap Docker container."
        ),
    )
    parser.add_argument("--request-template", type=Path, required=True)
    parser.add_argument("--oracle-request", type=Path, required=True)
    parser.add_argument("--oracle-response", type=Path, required=True)
    parser.add_argument("--corpus-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[3],
    )
    parser.add_argument("--image", default="kladnasoft/dataisland-core:latest")
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--target-seconds", type=float, default=DEFAULT_TARGET_SECONDS
    )
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument(
        "--retain-failed-spill",
        action="store_true",
        help="retain abandoned spill files after a failed/timed-out attempt",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.attempts <= 0:
        parser.error("--attempts must be positive")
    if args.timeout <= 0 or args.timeout > DEFAULT_TIMEOUT_SECONDS:
        parser.error("--timeout must be in (0, 300]")
    if args.target_seconds <= 0 or args.target_seconds > args.timeout:
        parser.error("--target-seconds must be in (0, timeout]")
    if args.sample_interval <= 0 or args.sample_interval > 30:
        parser.error("--sample-interval must be in (0, 30]")
    if shutil.which(args.docker) is None:
        parser.error(f"Docker executable not found: {args.docker!r}")

    repo_root = args.repo_root.expanduser().resolve()
    corpus_root = args.corpus_root.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()
    if not repo_root.is_dir():
        parser.error(f"repository root does not exist: {repo_root}")
    if not corpus_root.is_dir():
        parser.error(f"corpus root does not exist: {corpus_root}")
    try:
        inputs = load_gate_inputs(
            request_template=args.request_template,
            oracle_request=args.oracle_request,
            oracle_response=args.oracle_response,
        )
    except SpillGateConfigurationError as exc:
        parser.error(str(exc))

    output_root.mkdir(parents=True, exist_ok=True)
    final_code = EXIT_SUCCESS
    for attempt_number in range(1, args.attempts + 1):
        attempt_root = output_root / f"attempt-{attempt_number:03d}"
        if attempt_root.exists():
            parser.error(
                f"attempt directory already exists; refusing to overwrite: {attempt_root}"
            )
        code, artifact = run_attempt(
            inputs=inputs,
            attempt_root=attempt_root,
            repo_root=repo_root,
            corpus_root=corpus_root,
            docker=args.docker,
            image=args.image,
            timeout_seconds=args.timeout,
            target_seconds=args.target_seconds,
            sample_interval_seconds=args.sample_interval,
            retain_failed_spill=args.retain_failed_spill,
        )
        metrics = artifact.get("result_metrics") or {}
        wall = metrics.get("wall_seconds")
        wall_text = f"{float(wall):.3f}s" if wall is not None else "n/a"
        print(
            f"attempt {attempt_number}: status={artifact['status']} "
            f"wall={wall_text} artifact={attempt_root / 'attempt.json'}"
        )
        if code != EXIT_SUCCESS:
            final_code = code
            # A failed/slow attempt is diagnostic evidence, not another timing
            # sample.  Stop immediately so --attempts cannot consume hours or
            # fill the spill volume with repeated known failures.
            break
    return final_code


if __name__ == "__main__":
    raise SystemExit(main())
