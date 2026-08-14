"""Two-engine, five-minute gate for a genuinely external 10-GiB sort.

DuckDB and IslandDB each run once in a fresh 4-CPU, 4-GiB, zero-swap Docker
container with a 2-GiB engine workspace.  The query projects all 30 public
columns and orders by the non-monotonic ``metric`` plus globally unique ``id``
tie-break, so the roughly 10-GiB result cannot fit in memory and cannot exploit
the corpus' physical id order.

The result never crosses the worker boundary.  The worker emits a fixed-size,
batch-boundary-independent streaming proof; this host gate compares those
proofs exactly and requires material spill from both engines.  A host sampler
keeps process/cgroup I/O, RSS, memory, and spill high-water telemetry even when
the worker is killed at the hard five-minute boundary.
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

from .runner import ENGINE_DUCKDB, ENGINE_ISLAND
from .spill_gate import (
    GIB,
    CONTAINER_CPUS,
    CONTAINER_MEMORY_BYTES,
    ENGINE_MEMORY_BYTES,
    _ContainerSampler,
    _atomic_write_json,
    _docker_state,
    _git_identity,
    _read_json,
    _remove_container,
    _stop_container,
    _strict_json_digest,
    _tree_footprint,
    _write_text,
)


DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_SAMPLE_INTERVAL_SECONDS = 1.0
DUCKDB_MIN_MATERIAL_SPILL_BYTES = 256 * 1024**2
ISLAND_MIN_MATERIAL_SPILL_BYTES = 1 * GIB
SHARED_SPILL_MAX_BYTES = 28 * GIB
MIN_FREE_BYTES_BEFORE_RUN = 30 * GIB
ARTIFACT_FORMAT_VERSION = 1

EXIT_SUCCESS = 0
EXIT_CONFIGURATION = 2
EXIT_TIMEOUT = 3
EXIT_WORKER_FAILURE = 4
EXIT_PARITY_FAILURE = 5
EXIT_SPILL_NOT_MATERIAL = 6

PUBLIC_COLUMNS = (
    "id", "event_ts", "metric", "dimension",
    *(f"payload_{index:02d}" for index in range(26)),
)
EXPECTED_SOURCE_TYPES = {
    "id": "Int64",
    "event_ts": "Datetime(time_unit='us', time_zone=None)",
    "metric": "Int64",
    "dimension": "Int32",
    **{f"payload_{index:02d}": "Binary" for index in range(26)},
}
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class RealSpillGateError(RuntimeError):
    """Base error for a failed real-spill benchmark contract."""


class RealSpillConfigurationError(RealSpillGateError):
    """Raised before Docker starts when the corpus/request is not sealed."""


class RealSpillParityError(RealSpillGateError):
    """Raised when the two complete streaming proofs differ."""


@dataclass(frozen=True)
class RealSpillInputs:
    request: dict[str, Any]
    plan_digest: str
    expected_rows: int
    expected_value_bytes: int
    minimum_spill_bytes: dict[str, int]


def _result_schema() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for name in PUBLIC_COLUMNS:
        if name == "dimension":
            type_name, fixed = "int32", 4
        elif name == "event_ts":
            type_name, fixed = "timestamp[us]", 8
        elif name.startswith("payload_"):
            type_name, fixed = "binary", 64
        else:
            type_name, fixed = "int64", 8
        result.append({
            "name": name,
            "type": type_name,
            "fixed_value_bytes": fixed,
            "nullable": False,
        })
    return result


def _validate_manifest_and_files(
    plan: Mapping[str, Any], corpus_root: Path,
) -> tuple[dict[str, Any], int, int]:
    manifest_path = corpus_root / "manifest.json"
    try:
        manifest = _read_json(manifest_path)
    except Exception as exc:
        raise RealSpillConfigurationError(str(exc)) from exc
    spec = manifest.get("spec")
    if not isinstance(spec, Mapping):
        raise RealSpillConfigurationError("corpus manifest has no generator spec")
    if manifest.get("generator") != "islanddb-wide-v1":
        raise RealSpillConfigurationError("corpus is not islanddb-wide-v1")
    if str(spec.get("tier")) != "10gib":
        raise RealSpillConfigurationError("real-spill gate requires the 10gib tier")
    if int(spec.get("payload_columns") or 0) != 26:
        raise RealSpillConfigurationError("real-spill corpus must have 26 payloads")
    if int(spec.get("payload_width") or 0) != 64:
        raise RealSpillConfigurationError("real-spill payload width must be 64 bytes")

    rows = int(manifest.get("total_rows") or 0)
    source_bytes = int(manifest.get("actual_source_bytes") or 0)
    if rows <= 0 or source_bytes < 10 * GIB:
        raise RealSpillConfigurationError("manifest is not a physical 10-GiB corpus")
    entries = manifest.get("files")
    request_files = [str(value) for value in plan.get("files") or ()]
    if not isinstance(entries, list) or len(entries) != len(request_files):
        raise RealSpillConfigurationError("request/manifest file counts differ")

    expected_next_id = 0
    observed_bytes = 0
    for request_path, entry in zip(request_files, entries):
        if not isinstance(entry, Mapping):
            raise RealSpillConfigurationError("manifest contains a malformed file")
        relative = str(entry.get("path") or "")
        if Path(request_path).name != Path(relative).name:
            raise RealSpillConfigurationError(
                "request file order/identity differs from the manifest"
            )
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise RealSpillConfigurationError("manifest file path is unsafe")
        host_path = (corpus_root / relative).resolve()
        try:
            host_path.relative_to(corpus_root)
            stat = host_path.stat()
        except (OSError, ValueError) as exc:
            raise RealSpillConfigurationError(
                f"cannot validate corpus file {relative!r}: {exc}"
            ) from exc
        declared_bytes = int(entry.get("bytes") or 0)
        if stat.st_size != declared_bytes:
            raise RealSpillConfigurationError(
                f"corpus file size changed for {relative!r}"
            )
        minimum = int(entry.get("min_id"))
        maximum = int(entry.get("max_id"))
        file_rows = int(entry.get("rows") or 0)
        if (
            minimum != expected_next_id
            or maximum < minimum
            or maximum - minimum + 1 != file_rows
        ):
            raise RealSpillConfigurationError(
                "manifest does not prove globally unique contiguous ids"
            )
        expected_next_id = maximum + 1
        observed_bytes += declared_bytes
    if expected_next_id != rows or observed_bytes != source_bytes:
        raise RealSpillConfigurationError(
            "manifest totals do not match its sealed file/id ranges"
        )
    return manifest, rows, source_bytes


def load_real_spill_inputs(
    *, request_template: str | Path, corpus_root: str | Path,
) -> RealSpillInputs:
    """Transform the sealed full-projection request into a real external sort."""
    try:
        template = _read_json(request_template)
    except Exception as exc:
        raise RealSpillConfigurationError(str(exc)) from exc
    plan = template.get("plan")
    if not isinstance(plan, Mapping):
        raise RealSpillConfigurationError("request template has no plan")
    if plan.get("name") not in ("spill_group", "real_spill_sort"):
        raise RealSpillConfigurationError(
            "real-spill template must derive from the sealed spill_group plan"
        )
    if tuple(plan.get("required_columns") or ()) != PUBLIC_COLUMNS:
        raise RealSpillConfigurationError(
            "real-spill plan must project the exact 30 public columns"
        )
    schema = plan.get("schema")
    if not isinstance(schema, Mapping) or any(
        str(schema.get(name)) != expected
        for name, expected in EXPECTED_SOURCE_TYPES.items()
    ):
        raise RealSpillConfigurationError("real-spill source schema changed")
    table = str(plan.get("table") or "")
    if not SAFE_IDENTIFIER.fullmatch(table):
        raise RealSpillConfigurationError("real-spill table identifier is unsafe")
    if float(plan.get("projected_source_fraction") or 0.0) < 0.95:
        raise RealSpillConfigurationError(
            "real-spill plan must cover at least 95% of physical source bytes"
        )
    if not bool(plan.get("decoded_estimate_complete")):
        raise RealSpillConfigurationError("decoded result estimate is incomplete")
    if int(plan.get("source_repeat") or 1) != 1:
        raise RealSpillConfigurationError("real-spill gate rejects repeated paths")
    if int(template.get("warm_repeats") or 0) != 0:
        raise RealSpillConfigurationError("real-spill gate runs one cold sample")
    if int(template.get("threads") or 0) != CONTAINER_CPUS:
        raise RealSpillConfigurationError("real-spill gate requires four threads")
    if int(template.get("memory_limit_bytes") or 0) != ENGINE_MEMORY_BYTES:
        raise RealSpillConfigurationError("real-spill gate requires 2-GiB engine RAM")
    if template.get("cold_mode") != "fadvise":
        raise RealSpillConfigurationError("real-spill gate requires cold fadvise mode")
    if template.get("disable_caches") is not True:
        raise RealSpillConfigurationError("real-spill gate requires caches disabled")

    corpus = Path(corpus_root).expanduser().resolve()
    if not corpus.is_dir():
        raise RealSpillConfigurationError(f"corpus root does not exist: {corpus}")
    _, rows, source_bytes = _validate_manifest_and_files(plan, corpus)
    expected_value_bytes = rows * 1_692
    if int(plan.get("candidate_rows") or 0) != rows:
        raise RealSpillConfigurationError("request row estimate differs from manifest")
    if int(plan.get("source_bytes") or 0) != source_bytes:
        raise RealSpillConfigurationError("request source bytes differ from manifest")
    if int(plan.get("estimated_decoded_bytes") or 0) != expected_value_bytes:
        raise RealSpillConfigurationError(
            "request decoded bytes differ from the exact fixed-width result"
        )

    request = copy.deepcopy(template)
    request["engine"] = ENGINE_DUCKDB
    request["purpose"] = "real-spill-two-engine-gate"
    mutable_plan = request["plan"]
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
    # The generator and validated contiguous-id manifest prove these complete
    # corpus-wide integer domains. Older sealed request templates predate the
    # field, so the real-spill gate attaches the same proof deterministically.
    # The bounds guide range partition sizing; they never prune input rows.
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
        "physical_source_bytes": source_bytes,
        "engine_memory_bytes": ENGINE_MEMORY_BYTES,
        "projected_columns": len(PUBLIC_COLUMNS),
        "order_keys": ["metric", "id"],
        "id_domain": [0, rows - 1],
        "payload_value_bytes": 64,
        "non_null": True,
    }
    return RealSpillInputs(
        request=request,
        plan_digest=_strict_json_digest(mutable_plan),
        expected_rows=rows,
        expected_value_bytes=expected_value_bytes,
        minimum_spill_bytes={
            ENGINE_DUCKDB: DUCKDB_MIN_MATERIAL_SPILL_BYTES,
            ENGINE_ISLAND: ISLAND_MIN_MATERIAL_SPILL_BYTES,
        },
    )


def _docker_command(
    *, docker: str, image: str, container_name: str, repo_root: Path,
    corpus_root: Path, attempt_root: Path,
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
        "SUPERTABLE_ISLAND_SPILL_DIR": "/bench/engine-spill",
        "SUPERTABLE_ISLAND_SPILL_MAX_BYTES": str(SHARED_SPILL_MAX_BYTES),
        "SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES": str(512 * 1024**2),
        # Let Island unwind/clean just before the host's non-negotiable kill.
        "SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC": "295",
    }
    command = [
        docker, "run", "--name", container_name,
        "--pull", "never",
        "--cpus", str(CONTAINER_CPUS),
        "--memory", str(CONTAINER_MEMORY_BYTES),
        "--memory-swap", str(CONTAINER_MEMORY_BYTES),
        "--memory-swappiness", "0",
        "--pids-limit", "1024",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--network", "none",
        "--read-only",
        "--security-opt", "no-new-privileges:true",
        "--shm-size", "256m",
        "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=268435456",
        "--workdir", "/workspace",
        "--mount", f"type=bind,src={repo_root},dst=/workspace,readonly",
        "--mount", f"type=bind,src={corpus_root},dst=/corpus,readonly",
        "--mount", f"type=bind,src={attempt_root},dst=/bench",
        "--entrypoint", "python",
    ]
    for name, value in sorted(environments.items()):
        command.extend(("--env", f"{name}={value}"))
    command.extend((
        image,
        "-m", "supertable.engine.benchmarks.real_spill_worker",
        "/bench/request.json", "/bench/response.json",
    ))
    return command


def _counter_delta(first: Mapping[str, Any], last: Mapping[str, Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for key in set(first) | set(last):
        try:
            result[str(key)] = max(0, int(last.get(key) or 0) - int(first.get(key) or 0))
        except (TypeError, ValueError):
            continue
    return result


def disk_preflight(path: Path) -> dict[str, int]:
    """Require host headroom beyond either engine's hard 28-GiB temp cap."""
    candidate = path.expanduser().resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    if not candidate.exists():
        raise RealSpillConfigurationError(
            f"cannot locate a filesystem for output path {path}"
        )
    usage = shutil.disk_usage(candidate)
    telemetry = {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "required_free_bytes": MIN_FREE_BYTES_BEFORE_RUN,
        "per_engine_spill_cap_bytes": SHARED_SPILL_MAX_BYTES,
    }
    if usage.free < MIN_FREE_BYTES_BEFORE_RUN:
        raise RealSpillConfigurationError(
            f"real-spill gate needs at least {MIN_FREE_BYTES_BEFORE_RUN:,} "
            f"free bytes before launch; {usage.free:,} available at {candidate}"
        )
    return telemetry


def _sampler_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    samples = list(summary.get("samples") or ())
    process = [
        sample.get("process_io") for sample in samples
        if isinstance(sample.get("process_io"), Mapping)
    ]
    cgroup = [
        (sample.get("cgroup") or {}).get("io") for sample in samples
        if isinstance((sample.get("cgroup") or {}).get("io"), Mapping)
    ]
    result = dict(summary)
    result["process_io_sample_delta"] = (
        _counter_delta(process[0], process[-1]) if process else None
    )
    result["cgroup_io_sample_delta"] = (
        _counter_delta(cgroup[0], cgroup[-1]) if cgroup else None
    )
    return result


def _cleanup_spill_root(spill_root: Path, *, attempt_root: Path) -> dict[str, Any]:
    errors: list[str] = []
    try:
        resolved_attempt = attempt_root.resolve(strict=True)
        resolved_spill = spill_root.resolve(strict=True)
        if resolved_spill.relative_to(resolved_attempt) != Path("engine-spill"):
            raise ValueError("spill path is not this attempt's engine-spill")
    except (OSError, ValueError) as exc:
        return {"attempted": False, "errors": [f"unsafe_path:{exc}"]}
    for child in list(resolved_spill.iterdir()):
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError as exc:
            errors.append(f"{child.name}:{type(exc).__name__}:{exc}")
    return {"attempted": True, "errors": errors}


def _profile_spill_bytes(engine: str, series: Mapping[str, Any]) -> int:
    samples = series.get("samples")
    if not isinstance(samples, list) or len(samples) != 1:
        raise RealSpillGateError(f"{engine} did not return one cold sample")
    profile = samples[0].get("engine_profile") or {}
    key = "system_peak_temp_dir_size" if engine == ENGINE_DUCKDB else "spill_bytes"
    value = profile.get(key)
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _validate_worker_response(
    response: Mapping[str, Any], *, engine: str, inputs: RealSpillInputs,
    host_spill_high_water: int,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    if response.get("ok") is not True or not isinstance(response.get("result"), Mapping):
        raise RealSpillGateError(
            str(response.get("error") or f"{engine} worker returned no result")
        )
    series = dict(response["result"])
    if series.get("engine") != engine:
        raise RealSpillGateError(
            f"explicit {engine} worker returned engine {series.get('engine')!r}"
        )
    proof = series.get("result")
    if not isinstance(proof, Mapping):
        raise RealSpillGateError(f"{engine} returned no streaming proof")
    digest = str(proof.get("digest") or "")
    unsigned_proof = dict(proof)
    unsigned_proof.pop("digest", None)
    recomputed_digest = hashlib.sha256(
        json.dumps(
            unsigned_proof,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    if (
        len(digest) != 64
        or series.get("result_digest") != digest
        or digest != recomputed_digest
    ):
        raise RealSpillGateError(f"{engine} returned an invalid streaming digest")
    if int(proof.get("row_count") or -1) != inputs.expected_rows:
        raise RealSpillGateError(f"{engine} result row count changed")
    if int(proof.get("logical_value_bytes") or -1) != inputs.expected_value_bytes:
        raise RealSpillGateError(f"{engine} result byte coverage changed")
    order = proof.get("order") or {}
    if order.get("keys") != ["metric", "id"] or order.get("strictly_monotonic") is not True:
        raise RealSpillGateError(f"{engine} result order was not proven")
    if proof.get("schema") != _result_schema():
        raise RealSpillGateError(f"{engine} result schema proof changed")
    column_proofs = proof.get("column_sha256") or {}
    if (
        not isinstance(column_proofs, Mapping)
        or set(column_proofs) != set(PUBLIC_COLUMNS)
        or any(len(str(value)) != 64 for value in column_proofs.values())
    ):
        raise RealSpillGateError(f"{engine} result has an incomplete column proof")
    samples = series.get("samples")
    if not isinstance(samples, list) or len(samples) != 1:
        raise RealSpillGateError(f"{engine} did not return one sample")
    sample = samples[0]
    if sample.get("temperature") != "cold" or sample.get("result_digest") != digest:
        raise RealSpillGateError(f"{engine} cold sample proof changed")
    context = series.get("execution_context") or {}
    cgroup = context.get("cgroup_v2") or {}
    if cgroup.get("memory_max_bytes") != CONTAINER_MEMORY_BYTES:
        raise RealSpillGateError(f"{engine} did not retain the 4-GiB cgroup")
    if cgroup.get("swap_max_bytes") != 0:
        raise RealSpillGateError(f"{engine} exposed usable swap")
    if int(context.get("configured_threads") or 0) != CONTAINER_CPUS:
        raise RealSpillGateError(f"{engine} did not retain four threads")
    events = context.get("cgroup_memory_event_delta") or {}
    if any(int(events.get(key) or 0) for key in ("oom", "oom_kill", "oom_group_kill")):
        raise RealSpillGateError(f"{engine} recorded OOM events: {events}")

    profile_spill = _profile_spill_bytes(engine, series)
    observed_spill = max(profile_spill, max(0, int(host_spill_high_water)))
    minimum = inputs.minimum_spill_bytes[engine]
    spill_error = None
    if observed_spill < minimum:
        spill_error = (
            f"{engine} spilled only {observed_spill:,} bytes; material floor is "
            f"{minimum:,} bytes"
        )
    metrics = {
        "wall_seconds": sample.get("wall_seconds"),
        "cpu_seconds": sample.get("cpu_seconds"),
        "mean_cpu_cores": sample.get("mean_cpu_cores"),
        "rss_peak_bytes": sample.get("rss_peak_bytes"),
        "process_io_delta": sample.get("process_io_delta"),
        "engine_profile": sample.get("engine_profile"),
        "profile_spill_bytes": profile_spill,
        "host_spill_high_water_bytes": host_spill_high_water,
        "observed_spill_bytes": observed_spill,
        "minimum_material_spill_bytes": minimum,
        "result_rows": proof.get("row_count"),
        "result_value_bytes": proof.get("logical_value_bytes"),
        "result_digest": digest,
    }
    return series, metrics, spill_error


def run_engine_attempt(
    *, inputs: RealSpillInputs, engine: str, attempt_root: Path,
    repo_root: Path, corpus_root: Path, docker: str, image: str,
    timeout_seconds: float, sample_interval_seconds: float,
    retain_failed_spill: bool = False,
) -> tuple[int, dict[str, Any]]:
    """Run one explicit engine, always preserving a diagnostic artifact."""
    if engine not in (ENGINE_DUCKDB, ENGINE_ISLAND):
        raise ValueError(f"unsupported benchmark engine {engine!r}")
    attempt_root.mkdir(parents=True, exist_ok=False)
    for directory in ("home", "cache", "engine-spill"):
        (attempt_root / directory).mkdir()
    request = copy.deepcopy(inputs.request)
    request["engine"] = engine
    _atomic_write_json(attempt_root / "request.json", request)

    container_name = f"real-spill-{engine}-{uuid.uuid4().hex[:12]}"
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
        spill_root=attempt_root / "engine-spill",
        started=started,
        interval_seconds=sample_interval_seconds,
    )
    timed_out = False
    stdout = ""
    stderr = ""
    returncode: int | None = None
    launch_error: str | None = None
    process: subprocess.Popen[str] | None = None
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
    response = None
    response_error = None
    if response_path.is_file():
        try:
            response = _read_json(response_path)
        except Exception as exc:
            response_error = str(exc)

    raw_sampler_summary = sampler.summary()
    host_sampler = _sampler_summary(raw_sampler_summary)
    host_spill = int(host_sampler.get("spill_high_water_bytes") or 0)
    status = "worker_failed"
    exit_code = EXIT_WORKER_FAILURE
    validation_error = None
    metrics = None
    proof = None
    cooperative_deadline = False
    if timed_out:
        status, exit_code = "timeout", EXIT_TIMEOUT
    elif launch_error:
        validation_error = launch_error
    elif returncode != 0:
        validation_error = (
            response.get("error") if isinstance(response, Mapping) else response_error
        ) or f"docker run exited {returncode}"
        if (
            engine == ENGINE_ISLAND
            and any(
                marker in str(validation_error).casefold()
                for marker in ("timeout", "timed out", "deadline")
            )
        ):
            # Island's cooperative 295-second deadline deliberately precedes
            # the host's 300-second kill so query-private spill can unwind.
            # It is still a benchmark timeout, not an implementation crash.
            cooperative_deadline = True
            status, exit_code = "cooperative_deadline", EXIT_TIMEOUT
    elif not isinstance(response, Mapping):
        validation_error = response_error or "worker produced no response.json"
    else:
        try:
            series, metrics, spill_error = _validate_worker_response(
                response,
                engine=engine,
                inputs=inputs,
                host_spill_high_water=host_spill,
            )
            proof = series.get("result")
            if spill_error:
                status, exit_code = "spill_not_material", EXIT_SPILL_NOT_MATERIAL
                validation_error = spill_error
            else:
                status, exit_code = "passed", EXIT_SUCCESS
        except RealSpillGateError as exc:
            validation_error = str(exc)

    spill_after_worker = _tree_footprint(attempt_root / "engine-spill")
    if exit_code == EXIT_SUCCESS and spill_after_worker["files"]:
        status, exit_code = "spill_cleanup_failed", EXIT_WORKER_FAILURE
        validation_error = (
            f"successful {engine} worker left {spill_after_worker['files']:,} "
            f"spill files ({spill_after_worker['bytes']:,} bytes)"
        )
    spill_cleanup = {"attempted": False, "errors": []}
    if spill_after_worker["files"] and (
        exit_code != EXIT_SUCCESS and not retain_failed_spill
    ):
        spill_cleanup = _cleanup_spill_root(
            attempt_root / "engine-spill", attempt_root=attempt_root,
        )
    spill_after_cleanup = _tree_footprint(attempt_root / "engine-spill")
    if spill_cleanup["errors"]:
        suffix = "; ".join(spill_cleanup["errors"])
        validation_error = (
            f"{validation_error}; spill cleanup: {suffix}"
            if validation_error else f"spill cleanup: {suffix}"
        )

    artifact = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "10gib_4cpu_4gib_real_external_sort",
        "engine": engine,
        "status": status,
        "exit_code": exit_code,
        "started_unix_ms": started_unix_ms,
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": timeout_seconds,
        "timed_out": timed_out,
        "cooperative_deadline": cooperative_deadline,
        "plan_digest": inputs.plan_digest,
        "limits": {
            "cpus": CONTAINER_CPUS,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "engine_memory_bytes": ENGINE_MEMORY_BYTES,
            "minimum_material_spill_bytes": inputs.minimum_spill_bytes[engine],
        },
        "container": {
            "name": container_name,
            "image": image,
            "returncode": returncode,
            "state_before_removal": docker_state,
            "launch_error": launch_error,
        },
        "validation_error": validation_error,
        "result_metrics": metrics,
        "result_proof": proof,
        "host_sampler": host_sampler,
        "spill_after_worker": spill_after_worker,
        "spill_cleanup": spill_cleanup,
        "spill_after_gate_cleanup": spill_after_cleanup,
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


def run_comparison(
    *, inputs: RealSpillInputs, output_root: Path, repo_root: Path,
    corpus_root: Path, docker: str, image: str, timeout_seconds: float,
    sample_interval_seconds: float, retain_failed_spill: bool = False,
) -> tuple[int, dict[str, Any]]:
    """Run both engines even if the first times out, then compare proofs."""
    initial_preflight = disk_preflight(output_root)
    output_root.mkdir(parents=True, exist_ok=False)
    artifacts: dict[str, dict[str, Any]] = {}
    codes: dict[str, int] = {}
    disk_preflights: dict[str, dict[str, int]] = {}
    for engine in (ENGINE_DUCKDB, ENGINE_ISLAND):
        disk_preflights[engine] = disk_preflight(output_root)
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
            retain_failed_spill=retain_failed_spill,
        )
        codes[engine] = code
        artifacts[engine] = artifact

    duck_proof = artifacts[ENGINE_DUCKDB].get("result_proof")
    island_proof = artifacts[ENGINE_ISLAND].get("result_proof")
    parity_attempted = isinstance(duck_proof, Mapping) and isinstance(
        island_proof, Mapping,
    )
    parity_matched = parity_attempted and duck_proof == island_proof
    parity_error = None
    if parity_attempted and not parity_matched:
        parity_error = (
            "DuckDB and IslandDB streaming column/order proofs differ: "
            f"duckdb={duck_proof.get('digest')}, "
            f"islanddb={island_proof.get('digest')}"
        )

    if parity_error:
        status, exit_code = "parity_failed", EXIT_PARITY_FAILURE
    elif any(code == EXIT_TIMEOUT for code in codes.values()):
        status, exit_code = "timeout", EXIT_TIMEOUT
    elif any(code == EXIT_WORKER_FAILURE for code in codes.values()):
        status, exit_code = "worker_failed", EXIT_WORKER_FAILURE
    elif any(code == EXIT_SPILL_NOT_MATERIAL for code in codes.values()):
        status, exit_code = "spill_not_material", EXIT_SPILL_NOT_MATERIAL
    elif not parity_matched:
        status, exit_code = "parity_unavailable", EXIT_PARITY_FAILURE
    else:
        status, exit_code = "passed", EXIT_SUCCESS

    duck_metrics = artifacts[ENGINE_DUCKDB].get("result_metrics") or {}
    island_metrics = artifacts[ENGINE_ISLAND].get("result_metrics") or {}
    duck_wall = duck_metrics.get("wall_seconds")
    island_wall = island_metrics.get("wall_seconds")
    comparison = {
        "format_version": ARTIFACT_FORMAT_VERSION,
        "benchmark": "10gib_4cpu_4gib_real_external_sort_comparison",
        "status": status,
        "exit_code": exit_code,
        "timeout_seconds_per_engine": timeout_seconds,
        "plan_digest": inputs.plan_digest,
        "disk_preflight": {
            "initial": initial_preflight,
            "before_each_engine": disk_preflights,
        },
        "parity": {
            "attempted": parity_attempted,
            "matched": parity_matched,
            "error": parity_error,
            "duckdb_digest": (
                duck_proof.get("digest") if isinstance(duck_proof, Mapping) else None
            ),
            "islanddb_digest": (
                island_proof.get("digest") if isinstance(island_proof, Mapping) else None
            ),
        },
        "engine_status": {
            engine: {
                "status": artifacts[engine]["status"],
                "exit_code": codes[engine],
                "attempt_artifact": str(output_root / engine / "attempt.json"),
            }
            for engine in (ENGINE_DUCKDB, ENGINE_ISLAND)
        },
        "metrics": {
            ENGINE_DUCKDB: duck_metrics,
            ENGINE_ISLAND: island_metrics,
        },
        "wall_comparison": {
            "duckdb_seconds": duck_wall,
            "islanddb_seconds": island_wall,
            "islanddb_over_duckdb_ratio": (
                float(island_wall) / float(duck_wall)
                if duck_wall is not None and island_wall is not None
                and float(duck_wall) > 0 else None
            ),
        },
    }
    _atomic_write_json(output_root / "comparison.json", comparison)
    return exit_code, comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m supertable.engine.benchmarks.real_spill_gate",
        description=(
            "Run DuckDB and IslandDB 10-GiB external sorts in independent "
            "4-CPU/4-GiB/no-swap containers with a hard five-minute cap."
        ),
    )
    parser.add_argument("--request-template", type=Path, required=True)
    parser.add_argument("--corpus-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root", type=Path,
        default=Path(__file__).resolve().parents[3],
    )
    parser.add_argument("--image", default="kladnasoft/dataisland-core:latest")
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--sample-interval", type=float,
        default=DEFAULT_SAMPLE_INTERVAL_SECONDS,
    )
    parser.add_argument("--retain-failed-spill", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.timeout > DEFAULT_TIMEOUT_SECONDS:
        parser.error("--timeout must be in (0, 300]")
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
    if output_root.exists():
        parser.error(f"refusing to overwrite output root: {output_root}")
    try:
        inputs = load_real_spill_inputs(
            request_template=args.request_template,
            corpus_root=corpus_root,
        )
        disk_preflight(output_root)
    except RealSpillConfigurationError as exc:
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
        retain_failed_spill=args.retain_failed_spill,
    )
    print(
        f"status={comparison['status']} "
        f"duckdb={comparison['engine_status'][ENGINE_DUCKDB]['status']} "
        f"islanddb={comparison['engine_status'][ENGINE_ISLAND]['status']} "
        f"artifact={output_root / 'comparison.json'}"
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
