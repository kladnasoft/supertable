"""Fresh-container campaign for the tombstone-compaction write benchmark.

The benchmark implementation is intentionally supplied as an external,
read-only file.  Each container imports SuperTable from its own read-only
repository mount, while every revision sees the byte-identical prepared corpus
at ``/benchmark/corpus``.  This allows an older worktree which predates the
benchmark module to be measured by the current benchmark and oracle.

The campaign is conservative about disk cleanup.  It never removes a corpus or
an attempt artifact.  The only removable path is the exact ``work`` directory
created for a successful, parsed, correctness-verified attempt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from supertable.engine.benchmarks.container_runner import (
    CONTAINER_CPUS,
    CONTAINER_MEMORY_BYTES,
    DEFAULT_PIDS_LIMIT,
    _atomic_write_json,
    _canonical_cpuset,
    _container_inspect,
    _ContainerSampler,
    _git_identity,
    _image_provenance,
    _parse_cpuset,
    _remove_container,
    _safe_mount,
    _stop_container,
    _tree_footprint,
    _write_text,
)
from supertable.utils.diagnostic_redaction import safe_exception_type


FORMAT_VERSION = 1
CONTAINER_CORPUS_ROOT = "/benchmark/corpus"
CONTAINER_SCRIPT = "/benchmark-script/benchmark_tombstone_compaction.py"
DEFAULT_CANDIDATE_COMMIT = "426e94b"
DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.25


class WriteCampaignError(RuntimeError):
    """A campaign or one of its retained attempts failed."""


class WriteCampaignConfigurationError(ValueError):
    """A campaign cannot provide the requested reproducible boundary."""


@dataclass(frozen=True)
class RevisionSpec:
    """One repository revision and benchmark mode to measure."""

    label: str
    repo_root: str | Path
    mode: str = "two-phase"
    expected_commit_prefix: str | None = None

    def __post_init__(self) -> None:
        if not _safe_label(self.label):
            raise WriteCampaignConfigurationError("revision label must be non-empty")
        if self.mode not in {"two-phase", "fused"}:
            raise WriteCampaignConfigurationError(
                f"unsupported write benchmark mode {self.mode!r}"
            )
        if self.expected_commit_prefix is not None and not re.fullmatch(
            r"[0-9a-fA-F]{7,40}", self.expected_commit_prefix
        ):
            raise WriteCampaignConfigurationError(
                "expected_commit_prefix must contain 7 to 40 hexadecimal digits"
            )

    @property
    def variant_id(self) -> str:
        return f"{_safe_label(self.label)}--{self.mode}"


@dataclass(frozen=True)
class TombstoneWorkload:
    """Parameters for the production-sized write regression."""

    rows_per_file: int = 100_000
    file_count: int = 15
    tombstone_rows: int = 1_000_000
    compression_level: int = 1
    workers: int = CONTAINER_CPUS
    target_mib: float = 16.0
    input_file_target_mib: float = 15.75
    input_size_tolerance_pct: float = 1.0
    calibration_max_attempts: int = 8
    rss_sample_ms: float = 5.0

    def __post_init__(self) -> None:
        if self.rows_per_file < 4:
            raise WriteCampaignConfigurationError("rows_per_file must be at least 4")
        if self.file_count < 1:
            raise WriteCampaignConfigurationError("file_count must be positive")
        if self.tombstone_rows < self.file_count:
            raise WriteCampaignConfigurationError(
                "tombstone_rows must touch every input file"
            )
        if not 1 <= self.workers <= CONTAINER_CPUS:
            raise WriteCampaignConfigurationError(
                f"workers must be between 1 and {CONTAINER_CPUS}"
            )
        if not 0 < self.input_file_target_mib < self.target_mib:
            raise WriteCampaignConfigurationError(
                "input_file_target_mib must be positive and below target_mib"
            )
        if not 0 < self.input_size_tolerance_pct <= 100:
            raise WriteCampaignConfigurationError(
                "input_size_tolerance_pct must be in (0, 100]"
            )
        if self.calibration_max_attempts < 1 or self.rss_sample_ms <= 0:
            raise WriteCampaignConfigurationError(
                "calibration attempts and RSS sampling interval must be positive"
            )

    def arguments(self) -> list[str]:
        return [
            "--rows-per-file",
            str(self.rows_per_file),
            "--file-count",
            str(self.file_count),
            "--tombstone-rows",
            str(self.tombstone_rows),
            "--compression-level",
            str(self.compression_level),
            "--workers",
            str(self.workers),
            "--target-mib",
            str(self.target_mib),
            "--input-file-target-mib",
            str(self.input_file_target_mib),
            "--input-size-tolerance-pct",
            str(self.input_size_tolerance_pct),
            "--calibration-max-attempts",
            str(self.calibration_max_attempts),
            "--rss-sample-ms",
            str(self.rss_sample_ms),
        ]

    def manifest_configuration(self) -> dict[str, Any]:
        mib = 1024 * 1024
        return {
            "rows_per_file_seed": self.rows_per_file,
            "file_count": self.file_count,
            "tombstone_rows": self.tombstone_rows,
            "compression_level": self.compression_level,
            "target_bytes": int(self.target_mib * mib),
            "input_file_target_bytes": int(self.input_file_target_mib * mib),
            "input_size_tolerance": self.input_size_tolerance_pct / 100.0,
            "calibration_max_attempts": self.calibration_max_attempts,
        }


@dataclass(frozen=True)
class WriteCampaignConfig:
    """Host/container configuration for a complete comparison campaign."""

    revisions: tuple[RevisionSpec, ...]
    benchmark_script: str | Path
    benchmark_root: str | Path
    artifact_root: str | Path
    image: str
    repeats: int = 5
    docker: str = "docker"
    cpuset_cpus: str = "0-3"
    sample_interval_seconds: float = DEFAULT_SAMPLE_INTERVAL_SECONDS
    timeout_seconds: float = 7200.0
    pids_limit: int = DEFAULT_PIDS_LIMIT

    def __post_init__(self) -> None:
        revisions = tuple(self.revisions)
        object.__setattr__(self, "revisions", revisions)
        if len(revisions) < 2:
            raise WriteCampaignConfigurationError(
                "at least two revision/mode variants are required"
            )
        variant_ids = [revision.variant_id for revision in revisions]
        if len(set(variant_ids)) != len(variant_ids):
            raise WriteCampaignConfigurationError("revision/mode variants must be unique")
        if not str(self.image).strip() or not str(self.docker).strip():
            raise WriteCampaignConfigurationError("image and Docker executable are required")
        if self.repeats < 1:
            raise WriteCampaignConfigurationError("repeats must be positive")
        cpus = _parse_cpuset(self.cpuset_cpus)
        object.__setattr__(self, "cpuset_cpus", _canonical_cpuset(cpus))
        if not 0 < self.sample_interval_seconds <= 30:
            raise WriteCampaignConfigurationError(
                "sample_interval_seconds must be in (0, 30]"
            )
        if self.timeout_seconds <= 0 or self.pids_limit <= 0:
            raise WriteCampaignConfigurationError(
                "timeout_seconds and pids_limit must be positive"
            )


@dataclass(frozen=True)
class ScheduledAttempt:
    sequence: int
    repeat: int
    order_in_repeat: int
    revision: RevisionSpec


def _safe_label(value: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "-", str(value).lower()).strip("-.")[:64]


def _alternating_schedule(
    revisions: Sequence[RevisionSpec], repeats: int
) -> list[ScheduledAttempt]:
    """Reverse variant order on odd repeats to reduce ordering bias."""

    scheduled: list[ScheduledAttempt] = []
    sequence = 0
    for repeat in range(repeats):
        order = list(revisions)
        if repeat % 2:
            order.reverse()
        for order_in_repeat, revision in enumerate(order):
            scheduled.append(
                ScheduledAttempt(sequence, repeat, order_in_repeat, revision)
            )
            sequence += 1
    return scheduled


def _docker_command(
    *,
    config: WriteCampaignConfig,
    workload: TombstoneWorkload,
    revision: RevisionSpec,
    container_name: str,
    attempt_root: Path,
    prepare: bool,
) -> list[str]:
    """Build a command with an identical hard boundary for every variant."""

    repo_root = Path(revision.repo_root).expanduser().resolve(strict=True)
    benchmark_root = Path(config.benchmark_root).expanduser().resolve(strict=True)
    script = Path(config.benchmark_script).expanduser().resolve(strict=True)
    environments = {
        "HOME": "/attempt/home",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONPATH": "/workspace",
        "POLARS_MAX_THREADS": str(CONTAINER_CPUS),
        "OMP_NUM_THREADS": str(CONTAINER_CPUS),
        "OPENBLAS_NUM_THREADS": str(CONTAINER_CPUS),
        "MKL_NUM_THREADS": str(CONTAINER_CPUS),
        "NUMEXPR_NUM_THREADS": str(CONTAINER_CPUS),
        "SUPERTABLE_HOME": "/attempt/home",
    }
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
        _safe_mount(repo_root, "/workspace", readonly=True),
        "--mount",
        _safe_mount(script, CONTAINER_SCRIPT, readonly=True),
        "--mount",
        _safe_mount(benchmark_root, "/benchmark", readonly=not prepare),
        "--mount",
        _safe_mount(attempt_root, "/attempt"),
        "--entrypoint",
        "python",
    ]
    for name, value in sorted(environments.items()):
        command.extend(("--env", f"{name}={value}"))
    command.extend((config.image, CONTAINER_SCRIPT, *workload.arguments()))
    if prepare:
        command.extend(
            (
                "--prepare-corpus",
                CONTAINER_CORPUS_ROOT,
                "--label",
                "corpus-preparation",
                "--output",
                "/attempt/result.json",
            )
        )
    else:
        command.extend(
            (
                "--input-corpus",
                CONTAINER_CORPUS_ROOT,
                "--work-dir",
                "/attempt/work",
                "--label",
                revision.variant_id,
                "--output",
                "/attempt/result.json",
                "--fused" if revision.mode == "fused" else "--two-phase",
            )
        )
    return command


def _inspect_boundary_errors(
    inspect: Mapping[str, Any] | None,
    sampler: Mapping[str, Any],
    config: WriteCampaignConfig,
) -> list[str]:
    errors: list[str] = []
    host = dict((inspect or {}).get("HostConfig") or {})
    state = dict((inspect or {}).get("State") or {})
    expected = {
        "Memory": CONTAINER_MEMORY_BYTES,
        "MemorySwap": CONTAINER_MEMORY_BYTES,
        "NanoCpus": CONTAINER_CPUS * 1_000_000_000,
        "PidsLimit": config.pids_limit,
    }
    for field, value in expected.items():
        if int(host.get(field) or 0) != value:
            errors.append(f"Docker inspect {field}={host.get(field)!r}, expected {value}")
    if str(host.get("CpusetCpus") or "") != config.cpuset_cpus:
        errors.append("Docker inspect did not retain the requested cpuset")
    if host.get("ReadonlyRootfs") is not True:
        errors.append("Docker inspect did not retain read-only rootfs")
    if str(host.get("NetworkMode") or "") != "none":
        errors.append("Docker inspect did not retain network=none")
    if state.get("OOMKilled") is True:
        errors.append("Docker reported OOMKilled=true")
    if sampler.get("effective_cpuset_verified") is not True:
        errors.append(
            "host sampler did not observe the exact effective cpuset "
            f"{config.cpuset_cpus}"
        )
    cgroups = [
        sampler.get("first_cgroup"),
        sampler.get("last_cgroup"),
    ]
    observed = [value for value in cgroups if isinstance(value, Mapping)]
    if not observed:
        errors.append("host sampler captured no cgroup-v2 boundary")
    for cgroup in observed:
        if cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
            errors.append("container did not observe the 4-GiB memory cgroup")
            break
        if cgroup.get("swap_max_bytes") != 0:
            errors.append("container observed usable swap")
            break
    events = dict(sampler.get("cgroup_memory_event_delta") or {})
    if any(int(events.get(key) or 0) for key in ("oom", "oom_kill", "oom_group_kill")):
        errors.append(f"container observed OOM events: {events}")
    return errors


def _validate_corpus_result(
    result: Mapping[str, Any], workload: TombstoneWorkload
) -> list[str]:
    errors: list[str] = []
    if int(result.get("input_files") or -1) != workload.file_count:
        errors.append("prepared corpus file count differs from workload")
    if int(result.get("tombstone_rows") or -1) != workload.tombstone_rows:
        errors.append("prepared corpus tombstone count differs from workload")
    if str(result.get("root")) != CONTAINER_CORPUS_ROOT:
        errors.append("prepared corpus did not record /benchmark/corpus")
    if not isinstance(result.get("sha256"), Mapping):
        errors.append("prepared corpus has no checksum inventory")
    return errors


def _validate_benchmark_result(
    result: Mapping[str, Any], workload: TombstoneWorkload
) -> list[str]:
    errors: list[str] = []
    if result.get("benchmark") != "tombstone_compaction_v2":
        errors.append("unexpected benchmark result format")
    configuration = dict(result.get("configuration") or {})
    corpus = dict(result.get("corpus") or {})
    if int(configuration.get("file_count") or -1) != workload.file_count:
        errors.append("result file count differs from workload")
    if int(corpus.get("tombstone_rows") or -1) != workload.tombstone_rows:
        errors.append("result tombstone count differs from workload")
    if corpus.get("mode") != "shared_manifest":
        errors.append("benchmark did not use the shared immutable corpus")
    if configuration.get("input_corpus_dir") != CONTAINER_CORPUS_ROOT:
        errors.append("benchmark did not read /benchmark/corpus")
    calibration = dict(corpus.get("input_size_calibration") or {})
    if calibration.get("all_within_target") is not True:
        errors.append("one or more source files missed the calibrated size target")
    correctness = dict(result.get("correctness") or {})
    for name in ("authoritative_projection", "physical_union", "aggregates"):
        check = correctness.get(name)
        if not isinstance(check, Mapping) or check.get("match") is not True:
            errors.append(f"correctness.{name}.match is not true")
    environment = result.get("environment")
    if not isinstance(environment, Mapping):
        errors.append("result has no dependency/runtime environment")
    return errors


def _read_result(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, (
            "result artifact is unavailable; "
            f"error_type={safe_exception_type(exc)}"
        )
    if not isinstance(loaded, dict):
        return None, "result root is not a JSON object"
    return loaded, None


def _cleanup_generated_work(attempt_root: Path, work_root: Path) -> dict[str, Any]:
    """Remove only ``<attempt>/work`` after validating its exact identity."""

    attempt = attempt_root.resolve(strict=True)
    expected = attempt / "work"
    candidate = work_root.resolve(strict=False)
    if candidate != expected or candidate.parent != attempt:
        raise WriteCampaignError(f"refusing unsafe work cleanup target: {candidate}")
    before = _tree_footprint(candidate)
    existed = candidate.exists()
    if existed:
        if candidate.is_symlink() or not candidate.is_dir():
            raise WriteCampaignError(
                f"refusing non-directory work cleanup target: {candidate}"
            )
        shutil.rmtree(candidate)
    return {
        "target": str(candidate),
        "existed": existed,
        "before": before,
        "removed": existed and not candidate.exists(),
    }


def _result_fingerprint(result: Mapping[str, Any]) -> dict[str, Any]:
    correctness = dict(result.get("correctness") or {})
    value = {
        "corpus_sha256": dict(result.get("corpus") or {}).get("manifest_sha256"),
        "authoritative_projection": dict(
            correctness.get("authoritative_projection") or {}
        ).get("actual"),
        "physical_union": dict(correctness.get("physical_union") or {}).get("actual"),
        "aggregates": dict(correctness.get("aggregates") or {}).get("actual"),
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "value": value}


def _execute_attempt(
    *,
    config: WriteCampaignConfig,
    workload: TombstoneWorkload,
    revision: RevisionSpec,
    attempt_root: Path,
    container_name: str,
    prepare: bool,
    image: Mapping[str, Any] | None,
    git: Mapping[str, Any],
) -> dict[str, Any]:
    attempt_root.mkdir(parents=False, exist_ok=False)
    (attempt_root / "home").mkdir()
    request = {
        "format_version": FORMAT_VERSION,
        "kind": "prepare-corpus" if prepare else "benchmark",
        "revision": {
            "label": revision.label,
            "mode": revision.mode,
            "repo_root": str(Path(revision.repo_root).resolve()),
            "expected_commit_prefix": revision.expected_commit_prefix,
        },
        "workload": asdict(workload),
    }
    _atomic_write_json(attempt_root / "request.json", request)
    command = _docker_command(
        config=config,
        workload=workload,
        revision=revision,
        container_name=container_name,
        attempt_root=attempt_root,
        prepare=prepare,
    )
    started_unix_ms = int(time.time() * 1000)
    started = time.monotonic()
    sampler = _ContainerSampler(
        docker=config.docker,
        container_name=container_name,
        spill_root=attempt_root / "work",
        started=started,
        interval_seconds=config.sample_interval_seconds,
        expected_cpuset=config.cpuset_cpus,
    )
    stdout = ""
    stderr = ""
    returncode: int | None = None
    timed_out = False
    launch_error: str | None = None
    inspect: dict[str, Any] | None = None
    sampler_started = False
    try:
        process = subprocess.Popen(
            command,
            cwd=Path(revision.repo_root).resolve(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        sampler.start()
        sampler_started = True
        try:
            stdout, stderr = process.communicate(timeout=config.timeout_seconds)
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
            "campaign launch failed; "
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
    sampler_summary = sampler.summary()
    result_path = attempt_root / "result.json"
    result, result_error = _read_result(result_path)
    errors: list[str] = []
    if timed_out:
        errors.append(f"container exceeded {config.timeout_seconds} seconds")
    if launch_error:
        errors.append(launch_error)
    if returncode != 0:
        errors.append(f"docker run exited with {returncode}")
    if result is None:
        errors.append(result_error or "container produced no result")
    errors.extend(_inspect_boundary_errors(inspect, sampler_summary, config))
    if result is not None:
        errors.extend(
            _validate_corpus_result(result, workload)
            if prepare
            else _validate_benchmark_result(result, workload)
        )

    cleanup: dict[str, Any] | None = None
    if not prepare and result is not None and not errors:
        try:
            cleanup = _cleanup_generated_work(attempt_root, attempt_root / "work")
        except (OSError, WriteCampaignError) as exc:
            errors.append(
                "work cleanup failed; "
                f"error_type={safe_exception_type(exc)}"
            )

    status = "passed" if not errors else "failed"
    artifact = {
        "format_version": FORMAT_VERSION,
        "benchmark": "fresh_container_tombstone_write",
        "status": status,
        "kind": request["kind"],
        "started_unix_ms": started_unix_ms,
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": config.timeout_seconds,
        "timed_out": timed_out,
        "returncode": returncode,
        "container_name": container_name,
        "limits": {
            "cpus": CONTAINER_CPUS,
            "cpuset_cpus": config.cpuset_cpus,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "pids": config.pids_limit,
        },
        "request": request,
        "command": command,
        "validation_errors": errors,
        "result_error": result_error,
        "result": result,
        "docker_inspect": inspect,
        "host_sampler": sampler_summary,
        "work_cleanup": cleanup,
        "provenance": {
            "git": dict(git),
            "image": dict(image) if image is not None else None,
            "dependency_versions": (
                dict(result.get("environment") or {}) if result else None
            ),
        },
        "artifacts": {
            "root": str(attempt_root),
            "request": str(attempt_root / "request.json"),
            "result": str(result_path) if result_path.is_file() else None,
            "stdout": str(attempt_root / "stdout.log"),
            "stderr": str(attempt_root / "stderr.log"),
            "attempt": str(attempt_root / "attempt.json"),
        },
    }
    if result is not None and not prepare:
        artifact["correctness_fingerprint"] = _result_fingerprint(result)
    _atomic_write_json(attempt_root / "attempt.json", artifact)
    return artifact


def _distribution(values: Iterable[int | float]) -> dict[str, Any]:
    samples = [float(value) for value in values if math.isfinite(float(value))]
    if not samples:
        return {
            "count": 0,
            "values": [],
            "min": None,
            "mean": None,
            "median": None,
            "p95": None,
            "max": None,
            "stddev": None,
            "cv": None,
        }
    ordered = sorted(samples)
    rank = (len(ordered) - 1) * 0.95
    lower = math.floor(rank)
    upper = math.ceil(rank)
    p95 = ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)
    mean = statistics.fmean(ordered)
    stddev = statistics.pstdev(ordered)
    return {
        "count": len(ordered),
        "values": samples,
        "min": min(ordered),
        "mean": mean,
        "median": statistics.median(ordered),
        "p95": p95,
        "max": max(ordered),
        "stddev": stddev,
        "cv": stddev / abs(mean) if mean else None,
    }


def _numeric_leaves(value: Any, prefix: str = "") -> dict[str, float]:
    leaves: dict[str, float] = {}
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            leaves.update(_numeric_leaves(item, child))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if math.isfinite(numeric):
            leaves[prefix] = numeric
    return leaves


def _summarize_attempts(attempts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_variant: dict[str, list[Mapping[str, Any]]] = {}
    for attempt in attempts:
        request = dict(attempt.get("request") or {})
        revision = dict(request.get("revision") or {})
        variant = f"{_safe_label(str(revision.get('label') or 'unknown'))}--{revision.get('mode')}"
        by_variant.setdefault(variant, []).append(attempt)

    summaries: dict[str, Any] = {}
    for variant, records in by_variant.items():
        metric_values: dict[str, list[float]] = {}
        for record in records:
            result = dict(record.get("result") or {})
            sources = {
                "result.summary": result.get("summary") or {},
                "result.phases": result.get("phases") or {},
                "host": record.get("host_sampler") or {},
                "container.elapsed_seconds": record.get("elapsed_seconds"),
            }
            for source_name, source in sources.items():
                for key, value in _numeric_leaves(source, source_name).items():
                    metric_values.setdefault(key, []).append(value)
        summaries[variant] = {
            "attempt_count": len(records),
            "all_passed": all(record.get("status") == "passed" for record in records),
            "metrics": {
                key: _distribution(values) for key, values in sorted(metric_values.items())
            },
        }
    return summaries


def _validate_existing_manifest(path: Path, workload: TombstoneWorkload) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise WriteCampaignConfigurationError(
            "cannot read existing corpus manifest"
        ) from None
    if not isinstance(manifest, dict):
        raise WriteCampaignConfigurationError("corpus manifest root is not an object")
    if manifest.get("format") != "supertable-tombstone-corpus-v1":
        raise WriteCampaignConfigurationError("unsupported existing corpus manifest")
    if manifest.get("root") != CONTAINER_CORPUS_ROOT:
        raise WriteCampaignConfigurationError(
            "existing corpus was not prepared at /benchmark/corpus"
        )
    actual = dict(manifest.get("configuration") or {})
    expected = workload.manifest_configuration()
    if actual != expected:
        raise WriteCampaignConfigurationError(
            f"existing corpus configuration differs: expected={expected}, actual={actual}"
        )
    return manifest


def _verify_revision(revision: RevisionSpec, git: Mapping[str, Any]) -> None:
    expected = revision.expected_commit_prefix
    head = str(git.get("head") or "")
    if expected and not head.lower().startswith(expected.lower()):
        raise WriteCampaignConfigurationError(
            f"{revision.label} expected commit {expected}, observed {head or 'unknown'}"
        )


def run_write_campaign(
    config: WriteCampaignConfig,
    *,
    workload: TombstoneWorkload | None = None,
    campaign_id: str | None = None,
) -> dict[str, Any]:
    """Prepare/reuse one corpus and run all variants in fresh containers."""

    workload = workload or TombstoneWorkload()
    benchmark_script = Path(config.benchmark_script).expanduser().resolve(strict=True)
    if not benchmark_script.is_file():
        raise WriteCampaignConfigurationError("benchmark_script must be a file")
    benchmark_root = Path(config.benchmark_root).expanduser().resolve()
    artifact_root = Path(config.artifact_root).expanduser().resolve()
    benchmark_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)
    git_by_variant: dict[str, dict[str, Any]] = {}
    for revision in config.revisions:
        repo = Path(revision.repo_root).expanduser().resolve(strict=True)
        if not repo.is_dir():
            raise WriteCampaignConfigurationError(f"repo root is not a directory: {repo}")
        git = _git_identity(repo)
        _verify_revision(revision, git)
        git_by_variant[revision.variant_id] = git

    image = _image_provenance(config.docker, config.image)
    safe_campaign_id = _safe_label(campaign_id or "")
    if not safe_campaign_id:
        safe_campaign_id = (
            f"write-{int(time.time())}-{uuid.uuid4().hex[:10]}"
        )
    campaign_root = artifact_root / safe_campaign_id
    campaign_root.mkdir(parents=False, exist_ok=False)
    attempts_root = campaign_root / "attempts"
    attempts_root.mkdir()
    campaign_path = campaign_root / "campaign.json"
    attempts: list[dict[str, Any]] = []
    schedule = _alternating_schedule(config.revisions, config.repeats)
    campaign: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "benchmark": "tombstone_write_comparison_campaign",
        "campaign_id": safe_campaign_id,
        "status": "running",
        "configuration": {
            "workload": asdict(workload),
            "repeats": config.repeats,
            "benchmark_script": str(benchmark_script),
            "benchmark_root": str(benchmark_root),
            "artifact_root": str(campaign_root),
            "container_corpus_root": CONTAINER_CORPUS_ROOT,
            "limits": {
                "cpus": CONTAINER_CPUS,
                "cpuset_cpus": config.cpuset_cpus,
                "memory_bytes": CONTAINER_MEMORY_BYTES,
                "swap_bytes": 0,
                "pids": config.pids_limit,
            },
        },
        "schedule": [
            {
                "sequence": item.sequence,
                "repeat": item.repeat,
                "order_in_repeat": item.order_in_repeat,
                "variant": item.revision.variant_id,
            }
            for item in schedule
        ],
        "provenance": {
            "image": image,
            "revisions": git_by_variant,
            "benchmark_script_sha256": hashlib.sha256(
                benchmark_script.read_bytes()
            ).hexdigest(),
        },
        "corpus": None,
        "attempts": [],
        "parity": None,
        "summaries": None,
    }
    _atomic_write_json(campaign_path, campaign)

    manifest_path = benchmark_root / "corpus" / "corpus-manifest.json"
    if manifest_path.is_file():
        manifest = _validate_existing_manifest(manifest_path, workload)
        campaign["corpus"] = {
            "status": "reused",
            "manifest": str(manifest_path),
            "sha256": manifest.get("sha256"),
        }
    else:
        corpus_root = benchmark_root / "corpus"
        if corpus_root.exists():
            raise WriteCampaignConfigurationError(
                f"corpus directory exists without a valid manifest: {corpus_root}"
            )
        prepare_revision = config.revisions[-1]
        prepare_root = attempts_root / "0000-prepare-corpus"
        prepare_attempt = _execute_attempt(
            config=config,
            workload=workload,
            revision=prepare_revision,
            attempt_root=prepare_root,
            container_name=f"supertable-write-{safe_campaign_id}-prepare"[:120],
            prepare=True,
            image=image,
            git=git_by_variant[prepare_revision.variant_id],
        )
        campaign["corpus"] = {
            "status": prepare_attempt["status"],
            "manifest": str(manifest_path) if manifest_path.is_file() else None,
            "attempt": str(prepare_root / "attempt.json"),
            "result": prepare_attempt.get("result"),
        }
        _atomic_write_json(campaign_path, campaign)
        if prepare_attempt["status"] != "passed":
            campaign["status"] = "corpus_preparation_failed"
            _atomic_write_json(campaign_path, campaign)
            raise WriteCampaignError(
                f"corpus preparation failed; artifact={prepare_root / 'attempt.json'}"
            )

    for item in schedule:
        ordinal = item.sequence + 1
        attempt_name = (
            f"{ordinal:04d}-repeat-{item.repeat + 1:02d}-"
            f"{item.revision.variant_id}"
        )
        attempt_root = attempts_root / attempt_name
        attempt = _execute_attempt(
            config=config,
            workload=workload,
            revision=item.revision,
            attempt_root=attempt_root,
            container_name=f"supertable-write-{safe_campaign_id}-{ordinal:04d}"[:120],
            prepare=False,
            image=image,
            git=git_by_variant[item.revision.variant_id],
        )
        attempts.append(attempt)
        campaign["attempts"] = [
            {
                "status": record["status"],
                "variant": dict(record["request"]["revision"]),
                "artifact": record["artifacts"]["attempt"],
                "elapsed_seconds": record["elapsed_seconds"],
                "correctness_fingerprint": record.get("correctness_fingerprint"),
            }
            for record in attempts
        ]
        _atomic_write_json(campaign_path, campaign)
        if attempt["status"] != "passed":
            campaign["status"] = "attempt_failed"
            campaign["summaries"] = _summarize_attempts(attempts)
            _atomic_write_json(campaign_path, campaign)
            raise WriteCampaignError(
                f"write attempt failed; artifact={attempt['artifacts']['attempt']}"
            )

    fingerprints = {
        str(attempt["correctness_fingerprint"]["sha256"])
        for attempt in attempts
    }
    campaign["parity"] = {
        "match": len(fingerprints) == 1,
        "fingerprints": sorted(fingerprints),
        "reference": attempts[0]["correctness_fingerprint"]["value"],
    }
    campaign["summaries"] = _summarize_attempts(attempts)
    campaign["status"] = "passed" if campaign["parity"]["match"] else "parity_failed"
    _atomic_write_json(campaign_path, campaign)
    if campaign["status"] != "passed":
        raise WriteCampaignError(f"cross-version parity failed; artifact={campaign_path}")
    return campaign


def _parse_modes(raw: str) -> tuple[str, ...]:
    modes = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not modes or any(mode not in {"two-phase", "fused"} for mode in modes):
        raise argparse.ArgumentTypeError(
            "modes must be a comma-separated subset of two-phase,fused"
        )
    if len(set(modes)) != len(modes):
        raise argparse.ArgumentTypeError("modes must not contain duplicates")
    return modes


def build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-repo",
        "--candidate-426e-repo",
        dest="candidate_repo",
        required=True,
        help="Read-only worktree for the 2.4.1 candidate at commit 426e94b.",
    )
    parser.add_argument("--head-repo", default=str(repo_root))
    parser.add_argument("--candidate-label", default="2.4.1-426e94b")
    parser.add_argument("--candidate-commit", default=DEFAULT_CANDIDATE_COMMIT)
    parser.add_argument("--candidate-mode", choices=("two-phase", "fused"), default="two-phase")
    parser.add_argument(
        "--head-modes",
        type=_parse_modes,
        default=("two-phase", "fused"),
        help="Comma-separated current-revision modes (default: two-phase,fused).",
    )
    parser.add_argument("--head-label", default="head")
    parser.add_argument("--container-image", "--image", dest="image", required=True)
    parser.add_argument("--benchmark-root", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument(
        "--benchmark-script",
        default=str(repo_root / "supertable/benchmarks/benchmark_tombstone_compaction.py"),
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--cpuset-cpus", default="0-3")
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=7200.0)
    parser.add_argument("--pids-limit", type=int, default=DEFAULT_PIDS_LIMIT)
    parser.add_argument("--campaign-id")
    parser.add_argument("--rows-per-file", type=int, default=100_000)
    parser.add_argument("--file-count", type=int, default=15)
    parser.add_argument("--tombstone-rows", type=int, default=1_000_000)
    parser.add_argument("--compression-level", type=int, default=1)
    parser.add_argument("--workers", type=int, default=CONTAINER_CPUS)
    parser.add_argument("--target-mib", type=float, default=16.0)
    parser.add_argument("--input-file-target-mib", type=float, default=15.75)
    parser.add_argument("--input-size-tolerance-pct", type=float, default=1.0)
    parser.add_argument("--calibration-max-attempts", type=int, default=8)
    parser.add_argument("--rss-sample-ms", type=float, default=5.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    revisions = [
        RevisionSpec(
            label=args.candidate_label,
            repo_root=args.candidate_repo,
            mode=args.candidate_mode,
            expected_commit_prefix=args.candidate_commit,
        )
    ]
    revisions.extend(
        RevisionSpec(label=args.head_label, repo_root=args.head_repo, mode=mode)
        for mode in args.head_modes
    )
    config = WriteCampaignConfig(
        revisions=tuple(revisions),
        benchmark_script=args.benchmark_script,
        benchmark_root=args.benchmark_root,
        artifact_root=args.artifact_root,
        image=args.image,
        repeats=args.repeats,
        docker=args.docker,
        cpuset_cpus=args.cpuset_cpus,
        sample_interval_seconds=args.sample_interval_seconds,
        timeout_seconds=args.timeout_seconds,
        pids_limit=args.pids_limit,
    )
    workload = TombstoneWorkload(
        rows_per_file=args.rows_per_file,
        file_count=args.file_count,
        tombstone_rows=args.tombstone_rows,
        compression_level=args.compression_level,
        workers=args.workers,
        target_mib=args.target_mib,
        input_file_target_mib=args.input_file_target_mib,
        input_size_tolerance_pct=args.input_size_tolerance_pct,
        calibration_max_attempts=args.calibration_max_attempts,
        rss_sample_ms=args.rss_sample_ms,
    )
    campaign = run_write_campaign(config, workload=workload, campaign_id=args.campaign_id)
    print(json.dumps(campaign, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RevisionSpec",
    "TombstoneWorkload",
    "WriteCampaignConfig",
    "WriteCampaignConfigurationError",
    "WriteCampaignError",
    "build_parser",
    "run_write_campaign",
]
