"""Disk-bounded two-engine material-spill gate for the sealed 1-GiB corpus.

The 10-GiB real-spill benchmark needs at least 30 GiB of free host storage.
This smaller contract is intended for constrained audit hosts: it runs the
same full-width, externally sorted, streaming-digest workload over the sealed
1-GiB corpus with a 512-MiB configured engine workspace/budget.  DuckDB and
IslandDB run
sequentially in independent 4-CPU/4-GiB/no-swap containers.  Each engine has a
hard 4-GiB temporary-storage ceiling, and the gate refuses to launch unless at
least 8 GiB is free before each attempt.

The result is never materialized by the harness.  Both workers stream every
ordered value through the same batch-boundary-independent digest and the gate
requires the complete proofs to match exactly.  Spill directories are scoped
to one attempt and must be empty (naturally or after explicit bounded cleanup)
before another engine may start.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .container_runner import (
    _ContainerSampler,
    _atomic_write_json,
    _container_inspect,
    _git_identity,
    _image_provenance,
    _remove_container,
    _stop_container,
    _tree_footprint,
    _write_text,
)
from .real_spill_gate import (
    EXPECTED_SOURCE_TYPES,
    PUBLIC_COLUMNS,
    RealSpillGateError,
    _result_schema,
    _validate_worker_response,
)
from .runner import ENGINE_DUCKDB, ENGINE_ISLAND


GIB = 1024**3
MIB = 1024**2
CONTAINER_CPUS = 4
CPUSET_CPUS = "0-3"
ENGINE_THREADS = 2
CONTAINER_MEMORY_BYTES = 4 * GIB
ENGINE_MEMORY_BYTES = 512 * MIB
SPILL_CAP_BYTES = 4 * GIB
MIN_FREE_BYTES_BEFORE_RUN = 8 * GIB
MIN_MATERIAL_SPILL_BYTES = 64 * MIB
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.5
PIDS_LIMIT = 1024
ARTIFACT_FORMAT_VERSION = 1
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

EXIT_SUCCESS = 0
EXIT_CONFIGURATION = 2
EXIT_TIMEOUT = 3
EXIT_WORKER_FAILURE = 4
EXIT_PARITY_FAILURE = 5
EXIT_SPILL_NOT_MATERIAL = 6
EXIT_CLEANUP_FAILURE = 7


class BoundedSpillGateError(RuntimeError):
    """Base error for the bounded spill benchmark."""


class BoundedSpillConfigurationError(BoundedSpillGateError):
    """Raised before a container starts when an input is not sealed."""


@dataclass(frozen=True)
class BoundedSpillInputs:
    request: dict[str, Any]
    plan_digest: str
    expected_rows: int
    expected_value_bytes: int
    minimum_spill_bytes: dict[str, int]
    manifest_sha256: str
    corpus_content_sha256: str
    corpus_files: tuple[dict[str, Any], ...]


def _read_json(path: str | Path) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BoundedSpillConfigurationError(
            f"cannot read JSON from {source}: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise BoundedSpillConfigurationError(f"{source} is not a JSON object")
    return value


def _strict_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(4 * MIB):
            digest.update(block)
    return digest.hexdigest()


def _validate_corpus(
    plan: Mapping[str, Any], corpus_root: Path,
) -> tuple[int, int, str, str, tuple[dict[str, Any], ...]]:
    manifest_path = corpus_root / "manifest.json"
    manifest = _read_json(manifest_path)
    spec = manifest.get("spec")
    if manifest.get("generator") != "islanddb-wide-v1" or not isinstance(
        spec, Mapping
    ):
        raise BoundedSpillConfigurationError(
            "bounded spill requires an islanddb-wide-v1 manifest"
        )
    if str(spec.get("tier")) != "1gib":
        raise BoundedSpillConfigurationError(
            "bounded spill requires the sealed 1gib tier"
        )
    if int(spec.get("payload_columns") or 0) != 26 or int(
        spec.get("payload_width") or 0
    ) != 64:
        raise BoundedSpillConfigurationError(
            "bounded spill requires 26 fixed 64-byte payload columns"
        )

    rows = int(manifest.get("total_rows") or 0)
    source_bytes = int(manifest.get("actual_source_bytes") or 0)
    if rows <= 0 or source_bytes < GIB or source_bytes >= 2 * GIB:
        raise BoundedSpillConfigurationError(
            "manifest is not a physical 1-GiB corpus"
        )
    entries = manifest.get("files")
    request_files = [str(value) for value in plan.get("files") or ()]
    if not isinstance(entries, list) or len(entries) != len(request_files):
        raise BoundedSpillConfigurationError(
            "request and manifest file counts differ"
        )

    observed_bytes = 0
    expected_next_id = 0
    content: list[dict[str, Any]] = []
    root = corpus_root.resolve(strict=True)
    for request_path, raw_entry in zip(request_files, entries):
        if not isinstance(raw_entry, Mapping):
            raise BoundedSpillConfigurationError("malformed corpus file entry")
        relative = str(raw_entry.get("path") or "")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise BoundedSpillConfigurationError("unsafe corpus file path")
        if Path(request_path).name != relative_path.name:
            raise BoundedSpillConfigurationError(
                "request file order/identity differs from the manifest"
            )
        try:
            source = (root / relative_path).resolve(strict=True)
            source.relative_to(root)
            stat = source.stat()
        except (OSError, ValueError) as exc:
            raise BoundedSpillConfigurationError(
                f"cannot resolve corpus file {relative!r}: {exc}"
            ) from exc
        declared_bytes = int(raw_entry.get("bytes") or 0)
        file_rows = int(raw_entry.get("rows") or 0)
        minimum = int(raw_entry.get("min_id"))
        maximum = int(raw_entry.get("max_id"))
        if stat.st_size != declared_bytes:
            raise BoundedSpillConfigurationError(
                f"corpus file size changed for {relative!r}"
            )
        if (
            minimum != expected_next_id
            or maximum < minimum
            or maximum - minimum + 1 != file_rows
        ):
            raise BoundedSpillConfigurationError(
                "manifest does not prove globally unique contiguous ids"
            )
        digest = _sha256_file(source)
        content.append(
            {
                "path": relative,
                "bytes": stat.st_size,
                "rows": file_rows,
                "min_id": minimum,
                "max_id": maximum,
                "sha256": digest,
            }
        )
        observed_bytes += stat.st_size
        expected_next_id = maximum + 1
    if observed_bytes != source_bytes or expected_next_id != rows:
        raise BoundedSpillConfigurationError(
            "manifest totals differ from its files/id ranges"
        )
    return (
        rows,
        source_bytes,
        _sha256_file(manifest_path),
        _strict_digest(content),
        tuple(content),
    )


def load_bounded_spill_inputs(
    *, request_template: str | Path, corpus_root: str | Path,
) -> BoundedSpillInputs:
    """Seal a 1-GiB full projection as a forced external ordered stream."""
    template = _read_json(request_template)
    plan = template.get("plan")
    if not isinstance(plan, Mapping):
        raise BoundedSpillConfigurationError("request template has no plan")
    if plan.get("name") not in ("spill_group", "real_spill_sort"):
        raise BoundedSpillConfigurationError(
            "template must derive from the full-width spill_group plan"
        )
    if tuple(plan.get("required_columns") or ()) != PUBLIC_COLUMNS:
        raise BoundedSpillConfigurationError(
            "plan must project the exact 30 public columns"
        )
    schema = plan.get("schema")
    if not isinstance(schema, Mapping) or any(
        str(schema.get(name)) != expected
        for name, expected in EXPECTED_SOURCE_TYPES.items()
    ):
        raise BoundedSpillConfigurationError("source schema changed")
    table = str(plan.get("table") or "")
    if not SAFE_IDENTIFIER.fullmatch(table):
        raise BoundedSpillConfigurationError("unsafe table identifier")
    if float(plan.get("projected_source_fraction") or 0) < 0.95:
        raise BoundedSpillConfigurationError(
            "plan must cover at least 95% of physical source bytes"
        )
    if not bool(plan.get("decoded_estimate_complete")):
        raise BoundedSpillConfigurationError("decoded estimate is incomplete")
    if int(plan.get("source_repeat") or 1) != 1:
        raise BoundedSpillConfigurationError("repeated source paths are forbidden")
    if int(template.get("warm_repeats") or 0) != 0:
        raise BoundedSpillConfigurationError(
            "bounded spill requires exactly one cold sample"
        )
    if int(template.get("threads") or 0) != ENGINE_THREADS:
        raise BoundedSpillConfigurationError(
            f"bounded spill requires exactly {ENGINE_THREADS} engine threads"
        )
    if template.get("cold_mode") != "fadvise" or template.get(
        "disable_caches"
    ) is not True:
        raise BoundedSpillConfigurationError(
            "cold fadvise with benchmark caches disabled is required"
        )

    corpus = Path(corpus_root).expanduser().resolve()
    if not corpus.is_dir():
        raise BoundedSpillConfigurationError(f"missing corpus root: {corpus}")
    rows, source_bytes, manifest_sha, content_sha, files = _validate_corpus(
        plan, corpus
    )
    expected_value_bytes = rows * 1_692
    if int(plan.get("candidate_rows") or 0) != rows:
        raise BoundedSpillConfigurationError(
            "request row estimate differs from manifest"
        )
    if int(plan.get("source_bytes") or 0) != source_bytes:
        raise BoundedSpillConfigurationError(
            "request source bytes differ from manifest"
        )
    if int(plan.get("estimated_decoded_bytes") or 0) != expected_value_bytes:
        raise BoundedSpillConfigurationError(
            "request decoded bytes differ from exact full-width result"
        )

    request = copy.deepcopy(template)
    request["engine"] = ENGINE_DUCKDB
    request["purpose"] = "bounded-1gib-real-spill-two-engine-gate"
    request["memory_limit_bytes"] = ENGINE_MEMORY_BYTES
    request["warm_repeats"] = 0
    request["cold_mode"] = "fadvise"
    mutable_plan = request["plan"]
    mutable_plan["files"] = [
        "/corpus/" + Path(str(item["path"])).as_posix() for item in files
    ]
    mutable_plan["name"] = "real_spill_sort"
    mutable_plan["sql"] = (
        f"SELECT {', '.join(PUBLIC_COLUMNS)} FROM {table} "
        "ORDER BY metric, id"
    )
    mutable_plan["island_streaming_result"] = True
    mutable_plan["stream_result_digest"] = True
    mutable_plan["result_schema"] = _result_schema()
    mutable_plan["expected_result_rows"] = rows
    mutable_plan["expected_result_value_bytes"] = expected_value_bytes
    mutable_plan["integer_domain_bounds"] = {
        "id": {"minimum": 0, "maximum": rows - 1, "has_null": False},
        "metric": {"minimum": 0, "maximum": 1_000_002, "has_null": False},
        "dimension": {
            "minimum": 0,
            "maximum": min(1_023, rows - 1),
            "has_null": False,
        },
    }
    mutable_plan["real_spill_contract"] = {
        "tier": "1gib",
        "physical_source_bytes": source_bytes,
        "engine_memory_bytes": ENGINE_MEMORY_BYTES,
        "workspace_semantics": "configured_budget_not_process_rss_cap",
        "engine_threads": ENGINE_THREADS,
        "spill_cap_bytes": SPILL_CAP_BYTES,
        "minimum_material_spill_bytes": MIN_MATERIAL_SPILL_BYTES,
        "projected_columns": len(PUBLIC_COLUMNS),
        "order_keys": ["metric", "id"],
        "id_domain": [0, rows - 1],
        "payload_value_bytes": 64,
        "non_null": True,
        "manifest_sha256": manifest_sha,
        "corpus_content_sha256": content_sha,
    }
    return BoundedSpillInputs(
        request=request,
        plan_digest=_strict_digest(mutable_plan),
        expected_rows=rows,
        expected_value_bytes=expected_value_bytes,
        minimum_spill_bytes={
            ENGINE_DUCKDB: MIN_MATERIAL_SPILL_BYTES,
            ENGINE_ISLAND: MIN_MATERIAL_SPILL_BYTES,
        },
        manifest_sha256=manifest_sha,
        corpus_content_sha256=content_sha,
        corpus_files=files,
    )


def disk_preflight(path: str | Path) -> dict[str, int]:
    candidate = Path(path).expanduser().resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    if not candidate.exists():
        raise BoundedSpillConfigurationError(
            f"cannot locate output filesystem for {path}"
        )
    usage = shutil.disk_usage(candidate)
    result = {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "required_free_bytes": MIN_FREE_BYTES_BEFORE_RUN,
        "per_engine_spill_cap_bytes": SPILL_CAP_BYTES,
    }
    if usage.free < MIN_FREE_BYTES_BEFORE_RUN:
        raise BoundedSpillConfigurationError(
            f"bounded spill needs at least {MIN_FREE_BYTES_BEFORE_RUN:,} free "
            f"bytes; {usage.free:,} available at {candidate}"
        )
    return result


def _docker_command(
    *,
    docker: str,
    image: str,
    container_name: str,
    repo_root: Path,
    corpus_root: Path,
    attempt_root: Path,
    timeout_seconds: float,
) -> list[str]:
    cooperative_timeout = max(1, min(295, int(timeout_seconds) - 5))
    environments = {
        "HOME": "/bench/home",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONPATH": "/workspace",
        "POLARS_MAX_THREADS": str(ENGINE_THREADS),
        "SUPERTABLE_HOME": "/bench/home",
        "SUPERTABLE_DUCKDB_MEMORY_LIMIT": "512MiB",
        "SUPERTABLE_DUCKDB_THREADS": str(ENGINE_THREADS),
        "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE": "0",
        "SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE": "false",
        "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES": str(ENGINE_MEMORY_BYTES),
        "SUPERTABLE_ISLAND_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION": "1.0",
        "SUPERTABLE_ISLAND_CPU_MAX": str(ENGINE_THREADS),
        "SUPERTABLE_ISLAND_IO_WORKERS_MAX": str(ENGINE_THREADS),
        "SUPERTABLE_ISLAND_CACHE_ENABLED": "false",
        "SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED": "false",
        "SUPERTABLE_ISLAND_CACHE_DIR": "/bench/cache",
        "SUPERTABLE_ISLAND_RANGE_CACHE_DIR": "/bench/cache/ranges",
        "SUPERTABLE_ISLAND_SPILL_ENABLED": "true",
        "SUPERTABLE_ISLAND_SPILL_DIR": "/bench/engine-spill",
        "SUPERTABLE_ISLAND_SPILL_MAX_BYTES": str(SPILL_CAP_BYTES),
        "SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES": str(GIB),
        "SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC": str(cooperative_timeout),
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
        "--cpuset-cpus",
        CPUSET_CPUS,
        "--memory",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swap",
        str(CONTAINER_MEMORY_BYTES),
        "--memory-swappiness",
        "0",
        "--pids-limit",
        str(PIDS_LIMIT),
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
    command.extend(
        (
            image,
            "-m",
            "supertable.engine.benchmarks.real_spill_worker",
            "/bench/request.json",
            "/bench/response.json",
        )
    )
    return command


def _cleanup_spill_root(spill_root: Path, *, attempt_root: Path) -> dict[str, Any]:
    errors: list[str] = []
    try:
        resolved_attempt = attempt_root.resolve(strict=True)
        resolved_spill = spill_root.resolve(strict=True)
        if resolved_spill.relative_to(resolved_attempt) != Path("engine-spill"):
            raise ValueError("spill root is not this attempt's engine-spill")
    except (OSError, ValueError) as exc:
        return {
            "attempted": False,
            "errors": [f"unsafe_path:{type(exc).__name__}:{exc}"],
        }
    for child in list(resolved_spill.iterdir()):
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError as exc:
            errors.append(f"{child.name}:{type(exc).__name__}:{exc}")
    return {"attempted": True, "errors": errors}


def _container_absence_fence(
    docker: str, container_name: str, *, attempts: int = 3,
) -> dict[str, Any]:
    """Prove Docker no longer has the container before host spill cleanup.

    ``docker rm --force`` is synchronous when successful, but the shared
    removal helper intentionally suppresses command errors.  Treat only
    Docker's explicit "No such object/container" response as an absence proof;
    a present container or an ambiguous CLI failure fences the next engine.
    """
    observations: list[dict[str, Any]] = []
    for index in range(max(1, attempts)):
        try:
            completed = subprocess.run(
                [docker, "inspect", container_name],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            stdout = completed.stdout[-2000:]
            stderr = completed.stderr[-2000:]
            observation = {
                "attempt": index + 1,
                "returncode": completed.returncode,
                "stdout_tail": stdout,
                "stderr_tail": stderr,
            }
            observations.append(observation)
            missing_text = f"{stdout}\n{stderr}".casefold()
            if completed.returncode != 0 and any(
                marker in missing_text
                for marker in ("no such object", "no such container")
            ):
                return {
                    "verified_absent": True,
                    "observations": observations,
                }
        except (OSError, subprocess.SubprocessError) as exc:
            observations.append(
                {
                    "attempt": index + 1,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        if index + 1 < max(1, attempts):
            time.sleep(0.1)
    return {"verified_absent": False, "observations": observations}


def _boundary_errors(
    *,
    inspect: Mapping[str, Any] | None,
    sampler: Mapping[str, Any],
    series: Mapping[str, Any] | None,
) -> list[str]:
    errors: list[str] = []
    host = dict((inspect or {}).get("HostConfig") or {})
    state = dict((inspect or {}).get("State") or {})
    expected = {
        "Memory": CONTAINER_MEMORY_BYTES,
        "MemorySwap": CONTAINER_MEMORY_BYTES,
        "NanoCpus": CONTAINER_CPUS * 1_000_000_000,
        "PidsLimit": PIDS_LIMIT,
    }
    for key, value in expected.items():
        if int(host.get(key) or 0) != value:
            errors.append(f"inspect {key}={host.get(key)!r}, expected {value}")
    if host.get("CpusetCpus") != CPUSET_CPUS:
        errors.append("inspect cpuset is not 0-3")
    if host.get("ReadonlyRootfs") is not True:
        errors.append("inspect rootfs is not read-only")
    if host.get("NetworkMode") != "none":
        errors.append("inspect network is not disabled")
    if "ALL" not in list(host.get("CapDrop") or []):
        errors.append("inspect does not show all capabilities dropped")
    if "no-new-privileges:true" not in list(host.get("SecurityOpt") or []):
        errors.append("inspect does not show no-new-privileges")
    if state.get("OOMKilled") is not False:
        errors.append(f"inspect OOMKilled is {state.get('OOMKilled')!r}")

    if sampler.get("effective_cpuset_verified") is not True:
        errors.append("host sampler did not verify effective cpuset 0-3")
    cgroups = [sampler.get("first_cgroup"), sampler.get("last_cgroup")]
    observed = [item for item in cgroups if isinstance(item, Mapping)]
    if not observed:
        errors.append("host sampler captured no cgroup-v2 samples")
    for cgroup in observed:
        if cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
            errors.append("host cgroup memory.max is not 4 GiB")
            break
        if cgroup.get("swap_max_bytes") != 0:
            errors.append("host cgroup exposes usable swap")
            break
        if cgroup.get("cpu_max") != "400000 100000":
            errors.append("host cgroup cpu.max is not four CPUs")
            break
        if cgroup.get("cpuset_cpus_effective") != CPUSET_CPUS:
            errors.append("host cgroup cpuset is not 0-3")
            break
    events = dict(sampler.get("cgroup_memory_event_delta") or {})
    if any(int(events.get(key) or 0) for key in ("oom", "oom_kill", "oom_group_kill")):
        errors.append(f"host cgroup recorded OOM events: {events}")

    context = dict((series or {}).get("execution_context") or {})
    worker_cgroup = dict(context.get("cgroup_v2") or {})
    if worker_cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
        errors.append("worker did not observe the 4-GiB cgroup")
    if worker_cgroup.get("swap_max_bytes") != 0:
        errors.append("worker observed usable swap")
    if context.get("configured_threads") != ENGINE_THREADS:
        errors.append(
            f"worker did not configure {ENGINE_THREADS} engine threads"
        )
    if context.get("configured_memory_limit_bytes") != ENGINE_MEMORY_BYTES:
        errors.append("worker did not configure the 512-MiB engine workspace")
    if context.get("configured_spill_cap_bytes") != SPILL_CAP_BYTES:
        errors.append("worker did not configure the 4-GiB spill cap")
    if context.get("cpu_affinity") != [0, 1, 2, 3]:
        errors.append("worker affinity is not exactly CPUs 0-3")
    worker_events = dict(context.get("cgroup_memory_event_delta") or {})
    if any(
        int(worker_events.get(key) or 0)
        for key in ("oom", "oom_kill", "oom_group_kill")
    ):
        errors.append(f"worker recorded OOM events: {worker_events}")
    return errors


def run_engine_attempt(
    *,
    inputs: BoundedSpillInputs,
    engine: str,
    attempt_root: Path,
    repo_root: Path,
    corpus_root: Path,
    docker: str,
    image: str,
    timeout_seconds: float,
    sample_interval_seconds: float,
) -> tuple[int, dict[str, Any]]:
    if engine not in (ENGINE_DUCKDB, ENGINE_ISLAND):
        raise ValueError(f"unsupported engine {engine!r}")
    attempt_root.mkdir(parents=True, exist_ok=False)
    for name in ("home", "cache", "engine-spill"):
        (attempt_root / name).mkdir()
    request = copy.deepcopy(inputs.request)
    request["engine"] = engine
    _atomic_write_json(attempt_root / "request.json", request)
    request_sha = _sha256_file(attempt_root / "request.json")

    image_provenance: dict[str, Any] | None = None
    image_error: str | None = None
    try:
        image_provenance = _image_provenance(docker, image)
        requested_digest = image.partition("@sha256:")[2]
        if not requested_digest:
            raise BoundedSpillConfigurationError(
                "benchmark image must use an immutable @sha256 digest"
            )
        if image_provenance.get("content_digest") != f"sha256:{requested_digest}":
            raise BoundedSpillConfigurationError(
                "resolved image digest differs from requested digest"
            )
    except Exception as exc:  # preserve a diagnostic artifact
        image_error = f"{type(exc).__name__}: {exc}"

    container_name = f"bounded-spill-{engine}-{uuid.uuid4().hex[:12]}"
    command = _docker_command(
        docker=docker,
        image=image,
        container_name=container_name,
        repo_root=repo_root,
        corpus_root=corpus_root,
        attempt_root=attempt_root,
        timeout_seconds=timeout_seconds,
    )
    started_unix_ms = int(time.time() * 1000)
    started = time.monotonic()
    sampler = _ContainerSampler(
        docker=docker,
        container_name=container_name,
        spill_root=attempt_root / "engine-spill",
        started=started,
        interval_seconds=sample_interval_seconds,
        expected_cpuset=CPUSET_CPUS,
    )
    process: subprocess.Popen[str] | None = None
    sampler_started = False
    stdout = ""
    stderr = ""
    returncode: int | None = None
    timed_out = False
    launch_error = image_error
    inspect: dict[str, Any] | None = None
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
            if sampler_started:
                sampler.stop()
            inspect = _container_inspect(docker, container_name)
            _remove_container(docker, container_name)
    container_absence = _container_absence_fence(docker, container_name)
    elapsed_seconds = max(0.0, time.monotonic() - started)
    _write_text(attempt_root / "stdout.log", stdout)
    _write_text(attempt_root / "stderr.log", stderr)

    response_path = attempt_root / "response.json"
    response: dict[str, Any] | None = None
    response_error: str | None = None
    if response_path.is_file():
        try:
            response = _read_json(response_path)
        except BoundedSpillConfigurationError as exc:
            response_error = str(exc)
    sampler_summary = sampler.summary()
    host_spill = int(sampler_summary.get("spill_high_water_bytes") or 0)
    status = "worker_failed"
    exit_code = EXIT_WORKER_FAILURE
    validation_errors: list[str] = []
    series: dict[str, Any] | None = None
    result_metrics: dict[str, Any] | None = None
    result_proof: dict[str, Any] | None = None
    if timed_out:
        status, exit_code = "timeout", EXIT_TIMEOUT
        validation_errors.append(f"container exceeded {timeout_seconds} seconds")
    elif launch_error:
        validation_errors.append(launch_error)
    elif returncode != 0:
        validation_errors.append(
            str((response or {}).get("error") or response_error or f"docker exited {returncode}")
        )
    elif not isinstance(response, Mapping):
        validation_errors.append(response_error or "worker produced no response")
    else:
        try:
            series, result_metrics, spill_error = _validate_worker_response(
                response,
                engine=engine,
                inputs=inputs,
                host_spill_high_water=host_spill,
            )
            result_proof = dict(series.get("result") or {})
            validation_errors.extend(
                _boundary_errors(
                    inspect=inspect,
                    sampler=sampler_summary,
                    series=series,
                )
            )
            observed_spill = int(result_metrics.get("observed_spill_bytes") or 0)
            if observed_spill > SPILL_CAP_BYTES:
                validation_errors.append(
                    f"observed spill {observed_spill:,} exceeds cap {SPILL_CAP_BYTES:,}"
                )
            if spill_error:
                validation_errors.append(spill_error)
                status, exit_code = "spill_not_material", EXIT_SPILL_NOT_MATERIAL
            elif validation_errors:
                status, exit_code = "boundary_failed", EXIT_WORKER_FAILURE
            else:
                status, exit_code = "passed", EXIT_SUCCESS
        except (RealSpillGateError, ValueError, TypeError) as exc:
            validation_errors.append(str(exc))

    spill_after_worker = _tree_footprint(attempt_root / "engine-spill")
    cleanup = {"attempted": False, "errors": []}
    if (
        spill_after_worker["files"]
        and container_absence.get("verified_absent") is True
    ):
        cleanup = _cleanup_spill_root(
            attempt_root / "engine-spill", attempt_root=attempt_root
        )
    spill_after_cleanup = _tree_footprint(attempt_root / "engine-spill")
    cleanup_verified = (
        container_absence.get("verified_absent") is True
        and not cleanup["errors"]
        and spill_after_cleanup["files"] == 0
        and spill_after_cleanup["bytes"] == 0
    )
    if not cleanup_verified:
        validation_errors.extend(cleanup["errors"])
        if container_absence.get("verified_absent") is not True:
            validation_errors.append(
                "post-remove Docker inspect did not prove container absence; "
                "host cleanup and next-engine launch are fenced"
            )
        else:
            validation_errors.append(
                "attempt spill directory is not empty after bounded cleanup"
            )
        status, exit_code = "cleanup_failed", EXIT_CLEANUP_FAILURE

    artifact = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "1gib_4cpu_4gib_2threads_512mib_bounded_external_sort",
        "engine": engine,
        "status": status,
        "exit_code": exit_code,
        "started_unix_ms": started_unix_ms,
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": timeout_seconds,
        "timed_out": timed_out,
        "returncode": returncode,
        "plan_digest": inputs.plan_digest,
        "request_sha256": request_sha,
        "limits": {
            "cpus": CONTAINER_CPUS,
            "cpuset_cpus": CPUSET_CPUS,
            "engine_threads": ENGINE_THREADS,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "pids": PIDS_LIMIT,
            "configured_engine_workspace_bytes": ENGINE_MEMORY_BYTES,
            "workspace_semantics": "configured_budget_not_process_rss_cap",
            "spill_cap_bytes": SPILL_CAP_BYTES,
            "minimum_material_spill_bytes": MIN_MATERIAL_SPILL_BYTES,
        },
        "corpus": {
            "manifest_sha256": inputs.manifest_sha256,
            "content_inventory_sha256": inputs.corpus_content_sha256,
            "files": list(inputs.corpus_files),
        },
        "container": {
            "name": container_name,
            "command": command,
            "image": image,
            "inspect": inspect,
            "launch_error": launch_error,
            "post_remove_absence_fence": container_absence,
        },
        "provenance": {
            "git": _git_identity(repo_root),
            "image": image_provenance,
            "runtime": dict((series or {}).get("execution_context") or {}).get(
                "runtime"
            ),
        },
        "validation_errors": validation_errors,
        "result_metrics": result_metrics,
        "result_proof": result_proof,
        "host_sampler": sampler_summary,
        "spill_after_worker": spill_after_worker,
        "spill_cleanup": cleanup,
        "spill_after_cleanup": spill_after_cleanup,
        "cleanup_verified": cleanup_verified,
        "response": response,
        "response_error": response_error,
        "artifacts": {
            "request": str(attempt_root / "request.json"),
            "response": str(response_path) if response_path.is_file() else None,
            "stdout": str(attempt_root / "stdout.log"),
            "stderr": str(attempt_root / "stderr.log"),
        },
    }
    _atomic_write_json(attempt_root / "attempt.json", artifact)
    return exit_code, artifact


def run_comparison(
    *,
    inputs: BoundedSpillInputs,
    output_root: Path,
    repo_root: Path,
    corpus_root: Path,
    docker: str,
    image: str,
    timeout_seconds: float,
    sample_interval_seconds: float,
) -> tuple[int, dict[str, Any]]:
    initial_preflight = disk_preflight(output_root)
    output_root.mkdir(parents=True, exist_ok=False)
    attempts: dict[str, dict[str, Any]] = {}
    codes: dict[str, int] = {}
    preflights: dict[str, dict[str, int]] = {}
    aborted_before: str | None = None
    for engine in (ENGINE_DUCKDB, ENGINE_ISLAND):
        preflights[engine] = disk_preflight(output_root)
        code, artifact = run_engine_attempt(
            inputs=inputs,
            engine=engine,
            attempt_root=output_root / engine,
            repo_root=repo_root,
            corpus_root=corpus_root,
            docker=docker,
            image=image,
            timeout_seconds=timeout_seconds,
            sample_interval_seconds=sample_interval_seconds,
        )
        attempts[engine] = artifact
        codes[engine] = code
        if artifact.get("cleanup_verified") is not True:
            aborted_before = (
                ENGINE_ISLAND if engine == ENGINE_DUCKDB else "comparison"
            )
            break

    duck = attempts.get(ENGINE_DUCKDB, {})
    island = attempts.get(ENGINE_ISLAND, {})
    duck_proof = duck.get("result_proof")
    island_proof = island.get("result_proof")
    parity_attempted = isinstance(duck_proof, Mapping) and isinstance(
        island_proof, Mapping
    )
    parity_matched = parity_attempted and duck_proof == island_proof
    if aborted_before:
        status, exit_code = "cleanup_failed", EXIT_CLEANUP_FAILURE
    elif parity_attempted and not parity_matched:
        status, exit_code = "parity_failed", EXIT_PARITY_FAILURE
    elif any(code == EXIT_TIMEOUT for code in codes.values()):
        status, exit_code = "timeout", EXIT_TIMEOUT
    elif any(code == EXIT_SPILL_NOT_MATERIAL for code in codes.values()):
        status, exit_code = "spill_not_material", EXIT_SPILL_NOT_MATERIAL
    elif any(code != EXIT_SUCCESS for code in codes.values()):
        status, exit_code = "worker_failed", EXIT_WORKER_FAILURE
    elif not parity_matched:
        status, exit_code = "parity_unavailable", EXIT_PARITY_FAILURE
    else:
        status, exit_code = "passed", EXIT_SUCCESS

    metrics = {
        engine: dict(attempts.get(engine, {}).get("result_metrics") or {})
        for engine in (ENGINE_DUCKDB, ENGINE_ISLAND)
    }
    duck_wall = metrics[ENGINE_DUCKDB].get("wall_seconds")
    island_wall = metrics[ENGINE_ISLAND].get("wall_seconds")
    comparison = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "1gib_bounded_material_spill_comparison",
        "status": status,
        "exit_code": exit_code,
        "plan_digest": inputs.plan_digest,
        "timeout_seconds_per_engine": timeout_seconds,
        "run_order": [engine for engine in (ENGINE_DUCKDB, ENGINE_ISLAND) if engine in attempts],
        "aborted_before": aborted_before,
        "disk_preflight": {
            "initial": initial_preflight,
            "before_each_engine": preflights,
            "after": disk_preflight(output_root),
        },
        "limits": {
            "cpus": CONTAINER_CPUS,
            "cpuset_cpus": CPUSET_CPUS,
            "engine_threads": ENGINE_THREADS,
            "container_memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "configured_engine_workspace_bytes": ENGINE_MEMORY_BYTES,
            "workspace_semantics": "configured_budget_not_process_rss_cap",
            "spill_cap_bytes_per_engine": SPILL_CAP_BYTES,
            "minimum_material_spill_bytes": MIN_MATERIAL_SPILL_BYTES,
        },
        "corpus": {
            "root": str(corpus_root),
            "manifest_sha256": inputs.manifest_sha256,
            "content_inventory_sha256": inputs.corpus_content_sha256,
            "file_count": len(inputs.corpus_files),
            "source_bytes": sum(item["bytes"] for item in inputs.corpus_files),
            "rows": inputs.expected_rows,
            "logical_result_value_bytes": inputs.expected_value_bytes,
        },
        "parity": {
            "attempted": parity_attempted,
            "matched": parity_matched,
            "complete_proof_equal": parity_matched,
            "duckdb_digest": (
                duck_proof.get("digest") if isinstance(duck_proof, Mapping) else None
            ),
            "islanddb_digest": (
                island_proof.get("digest") if isinstance(island_proof, Mapping) else None
            ),
        },
        "engines": {
            engine: {
                "status": attempts.get(engine, {}).get("status", "not_run"),
                "exit_code": codes.get(engine),
                "attempt": (
                    str(output_root / engine / "attempt.json")
                    if engine in attempts
                    else None
                ),
                "cleanup_verified": attempts.get(engine, {}).get(
                    "cleanup_verified"
                ),
            }
            for engine in (ENGINE_DUCKDB, ENGINE_ISLAND)
        },
        "metrics": metrics,
        "wall_comparison": {
            "duckdb_seconds": duck_wall,
            "islanddb_seconds": island_wall,
            "islanddb_over_duckdb_ratio": (
                float(island_wall) / float(duck_wall)
                if duck_wall is not None
                and island_wall is not None
                and float(duck_wall) > 0
                else None
            ),
        },
    }
    _atomic_write_json(output_root / "comparison.json", comparison)
    return exit_code, comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m supertable.engine.benchmarks.bounded_spill_gate",
        description=(
            "Run exact DuckDB/IslandDB streaming parity over the sealed 1-GiB "
            "corpus with two engine threads, a 512-MiB configured workspace, "
            "and a 4-GiB temp cap."
        ),
    )
    parser.add_argument("--request-template", type=Path, required=True)
    parser.add_argument("--corpus-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[3]
    )
    parser.add_argument("--image", required=True)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--sample-interval", type=float, default=DEFAULT_SAMPLE_INTERVAL_SECONDS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.timeout <= 5 or args.timeout > DEFAULT_TIMEOUT_SECONDS:
        parser.error("--timeout must be in (5, 300]")
    if args.sample_interval <= 0 or args.sample_interval > 30:
        parser.error("--sample-interval must be in (0, 30]")
    if "@sha256:" not in args.image:
        parser.error("--image must be pinned with @sha256:<digest>")
    if shutil.which(args.docker) is None:
        parser.error(f"Docker executable not found: {args.docker!r}")
    repo_root = args.repo_root.expanduser().resolve()
    corpus_root = args.corpus_root.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()
    if not repo_root.is_dir():
        parser.error(f"repository root does not exist: {repo_root}")
    if not corpus_root.is_dir():
        parser.error(f"corpus root does not exist: {corpus_root}")
    if output_root.exists():
        parser.error(f"refusing to overwrite output root: {output_root}")
    try:
        disk_preflight(output_root)
        inputs = load_bounded_spill_inputs(
            request_template=args.request_template,
            corpus_root=corpus_root,
        )
    except BoundedSpillConfigurationError as exc:
        parser.error(str(exc))
    code, comparison = run_comparison(
        inputs=inputs,
        output_root=output_root,
        repo_root=repo_root,
        corpus_root=corpus_root,
        docker=args.docker,
        image=args.image,
        timeout_seconds=args.timeout,
        sample_interval_seconds=args.sample_interval,
    )
    print(
        f"status={comparison['status']} "
        f"duckdb={comparison['engines'][ENGINE_DUCKDB]['status']} "
        f"islanddb={comparison['engines'][ENGINE_ISLAND]['status']} "
        f"artifact={output_root / 'comparison.json'}"
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
