"""Parity-first DuckDB Lite versus IslandDB benchmark runner.

Each engine series executes in a fresh Python process.  Process startup is not
timed: the first production Executor call is the cold engine/application-cache
sample and subsequent calls on the same Executor are warm samples.  OS page
cache state is explicitly reported as uncontrolled unless the caller asks for
best-effort ``posix_fadvise`` before a cold sample.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .corpus import build_workloads, normalize_workloads, plan_workload


RESULT_FORMAT_VERSION = 1
ENGINE_DUCKDB = "duckdb_lite"
ENGINE_ISLAND = "islanddb"
ENGINE_NAMES = (ENGINE_DUCKDB, ENGINE_ISLAND)


class BenchmarkUnavailableError(RuntimeError):
    """Raised when an explicitly requested benchmark engine is unavailable."""


class BenchmarkParityError(AssertionError):
    """Raised before timing when IslandDB differs from the DuckDB oracle."""


class BenchmarkWorkerError(RuntimeError):
    """Raised when an isolated benchmark worker fails."""


def _json_value(value: Any) -> Any:
    """Convert engine/profile values to deterministic strict JSON values."""
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {"$float": "nan"}
        if math.isinf(value):
            return {"$float": "+inf" if value > 0 else "-inf"}
        # Hex retains -0 and every IEEE bit represented by a Python float.
        return {"$float": value.hex()}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, dt.datetime):
        return {"$datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, dt.date):
        return {"$date": value.isoformat()}
    if isinstance(value, dt.time):
        return {"$time": value.isoformat(timespec="microseconds")}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$bytes": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    # NumPy/Pandas scalar types expose item(); avoid importing NumPy here.
    item = getattr(value, "item", None)
    if callable(item):
        try:
            converted = item()
        except Exception:
            converted = value
        if converted is not value:
            return _json_value(converted)
    return {"$repr": f"{type(value).__module__}.{type(value).__qualname__}:{value!r}"}


def canonical_frame(frame) -> dict[str, Any]:
    """Canonicalize a pandas result, including column order and exact dtypes."""
    columns = [str(column) for column in frame.columns]
    dtypes = [str(frame[column].dtype) for column in frame.columns]
    rows = [
        [_json_value(value) for value in row]
        for row in frame.itertuples(index=False, name=None)
    ]
    return {"columns": columns, "dtypes": dtypes, "rows": rows}


def result_digest(canonical: Mapping[str, Any]) -> str:
    raw = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def assert_exact_parity(
    duckdb_result: Mapping[str, Any], island_result: Mapping[str, Any], *, label: str
) -> str:
    """Return the shared digest or raise a compact, actionable parity error."""
    duck = duckdb_result.get("result")
    island = island_result.get("result")
    if duck != island:
        raise BenchmarkParityError(
            f"IslandDB result differs from DuckDB oracle for {label}: "
            f"duckdb_digest={duckdb_result.get('result_digest')}, "
            f"islanddb_digest={island_result.get('result_digest')}; "
            f"duckdb={json.dumps(duck, sort_keys=True)[:1000]}; "
            f"islanddb={json.dumps(island, sort_keys=True)[:1000]}"
        )
    digest = result_digest(duck or {})
    if digest != duckdb_result.get("result_digest"):
        raise BenchmarkParityError(f"DuckDB worker returned an invalid digest for {label}")
    if digest != island_result.get("result_digest"):
        raise BenchmarkParityError(f"IslandDB worker returned an invalid digest for {label}")
    return digest


def _resolve_engine(engine_name: str):
    from supertable.engine.engine_enum import Engine

    if engine_name == ENGINE_DUCKDB:
        return Engine.DUCKDB_LITE
    if engine_name == ENGINE_ISLAND:
        # ISLANDDB / islanddb is the public contract.  The second spelling is a
        # short compatibility bridge for development branches and can be
        # removed once all pre-merge branches have converged.
        for member_name in ("ISLANDDB", "ISLAND_DB"):
            member = getattr(Engine, member_name, None)
            if member is not None:
                return member
        raise BenchmarkUnavailableError(
            "IslandDB is unavailable: Engine.ISLANDDB is not implemented"
        )
    raise ValueError(f"unsupported benchmark engine {engine_name!r}")


def islanddb_available() -> bool:
    try:
        _resolve_engine(ENGINE_ISLAND)
    except (BenchmarkUnavailableError, ImportError):
        return False
    return True


def _build_reflection(plan: Mapping[str, Any]):
    from supertable.data_classes import (
        Reflection, RowGroupSelection, SuperSnapshot,
    )

    files = [str(path) for path in plan["files"]]
    original_files = [str(path) for path in plan.get("original_files") or files]
    snapshot = SuperSnapshot(
        super_name=str(plan["super_name"]),
        simple_name=str(plan["table"]),
        simple_version=1,
        files=files,
        columns=set(str(name) for name in plan["schema"]),
        resource_keys=list(files),
        column_types=dict(plan["schema"]),
        snapshot_resource_keys=original_files,
        resource_sizes=[os.path.getsize(path) for path in files],
        row_group_selections={
            str(path): RowGroupSelection(
                int(details["row_group_count"]),
                tuple(int(group_id) for group_id in details["eligible_ids"]),
                str(details["footer_sha256"]),
            )
            for path, details in (plan.get("row_group_selections") or {}).items()
            if details.get("eligible_ids")
            and len(details["eligible_ids"]) < int(details["row_group_count"])
        },
        candidate_rows=int(plan.get("candidate_rows") or 0),
        candidate_rows_complete=True,
    )
    return Reflection(
        storage_type="LocalBenchmarkCorpus",
        reflection_bytes=int(plan["estimated_reflection_bytes"]),
        total_reflections=len(files),
        supers=[snapshot],
        freshness_ms=0,
        source_bytes=int(plan["candidate_source_bytes"]),
        row_group_scan_bytes=int(plan.get("eligible_pushdown_bytes") or 0),
        row_group_scan_bytes_complete=True,
        # Benchmark manifests do not yet persist exact uncompressed chunks.
        # Use a conservative 8x upper planning estimate and label it complete
        # for the generated fixed-width/high-entropy corpus only.
        decoded_bytes=max(
            int(plan.get("eligible_pushdown_bytes") or 0) * 8,
            int(plan.get("estimated_reflection_bytes") or 0),
        ),
        decoded_bytes_complete=True,
    )


def _flatten_plan_stats(raw_stats: Sequence[Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for item in raw_stats:
        if not isinstance(item, Mapping):
            continue
        for key, value in item.items():
            key_text = str(key)
            if key_text in flattened:
                existing = flattened[key_text]
                if not isinstance(existing, list):
                    existing = [existing]
                existing.append(_json_value(value))
                flattened[key_text] = existing
            else:
                flattened[key_text] = _json_value(value)
    return flattened


def _cache_metrics_from_plan(plan_stats: Mapping[str, Any]) -> dict[str, Any]:
    """Retain every cache metric without binding to private IslandDB classes."""
    result: dict[str, Any] = {}
    known_bare = {
        "HITS",
        "MISSES",
        "DOWNLOADS",
        "DOWNLOADED_BYTES",
        "BYPASSES",
        "EVICTIONS",
        "EVICTED_BYTES",
        "COVERAGE_RATIO",
        "LOCALIZED_FILES",
        "LOCALIZED_BYTES",
        "FALLBACK_FILES",
    }
    for key, value in plan_stats.items():
        upper = key.upper()
        if "CACHE" in upper or upper in known_bare:
            result[key] = value
    return result


def _profile_metrics(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        profile = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"profile_error": f"{type(exc).__name__}: {exc}"}
    keys = (
        # Common/DuckDB JSON profiler fields.
        "latency",
        "cpu_time",
        "total_bytes_read",
        "total_bytes_written",
        "system_peak_buffer_memory",
        "system_peak_temp_dir_size",
        "rows_returned",
        "cumulative_rows_scanned",
        "result_set_size",
        # IslandDB's production profile fields.  The optimized plan is retained
        # so a benchmark artifact can prove projection/predicate pushdown rather
        # than inferring it from elapsed time.
        "engine",
        "native",
        "source_bytes",
        "estimated_scan_bytes",
        "files",
        "elapsed_ms",
        "optimized_plan",
        "cache",
        "resources",
        "spill",
        "selected_row_groups",
        "cpu_time_ms",
        "logical_scan_bytes",
        "logical_scan_bytes_complete",
        "physical_read_bytes",
        "physical_read_bytes_measured",
        "decoded_bytes",
        "decoded_bytes_complete",
        "rows_scanned",
        "rows_scanned_measured",
        "result_rows",
        "result_bytes",
        "peak_memory_bytes",
        "peak_memory_scope",
        "spill_bytes",
        "spill_bytes_measured",
    )
    return {key: _json_value(profile.get(key)) for key in keys if key in profile}


def _rss_bytes() -> int | None:
    status = Path("/proc/self/status")
    if status.is_file():
        try:
            for line in status.read_text(encoding="ascii").splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        except Exception:
            return None
    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Linux is KiB; macOS reports bytes.
        return value if sys.platform == "darwin" else value * 1024
    except Exception:
        return None


class _PeakRSS:
    def __init__(self, interval_s: float = 0.002):
        self.interval_s = interval_s
        self.baseline = _rss_bytes()
        self.peak = self.baseline
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self._stop.wait(self.interval_s):
            current = _rss_bytes()
            if current is not None and (self.peak is None or current > self.peak):
                self.peak = current

    def __enter__(self) -> "_PeakRSS":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        current = _rss_bytes()
        if current is not None and (self.peak is None or current > self.peak):
            self.peak = current
        self._stop.set()
        self._thread.join(timeout=1)

    @property
    def delta(self) -> int | None:
        if self.baseline is None or self.peak is None:
            return None
        return max(0, self.peak - self.baseline)


def _tree_footprint(root: str | Path | None) -> dict[str, int]:
    if not root:
        return {"files": 0, "bytes": 0}
    path = Path(root)
    if not path.exists():
        return {"files": 0, "bytes": 0}
    count = 0
    total = 0
    for current_root, _, names in os.walk(path):
        for name in names:
            file_path = Path(current_root) / name
            try:
                stat = file_path.stat()
            except OSError:
                continue
            if file_path.is_file():
                count += 1
                total += stat.st_size
    return {"files": count, "bytes": total}


def _drop_os_cache_best_effort(files: Sequence[str]) -> dict[str, Any]:
    advise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if not callable(advise) or advice is None:
        return {"mode": "fadvise_best_effort", "supported": False, "advised_files": 0}
    advised = 0
    errors = 0
    for raw in files:
        try:
            descriptor = os.open(raw, os.O_RDONLY)
            try:
                advise(descriptor, 0, 0, advice)
            finally:
                os.close(descriptor)
            advised += 1
        except OSError:
            errors += 1
    return {
        "mode": "fadvise_best_effort",
        "supported": True,
        "advised_files": advised,
        "errors": errors,
    }


def _execute_one(executor, engine, plan: Mapping[str, Any], sample_index: int) -> dict[str, Any]:
    import pyarrow as pa

    from supertable.engine.plan_stats import PlanStats
    from supertable.query_plan_manager import QueryPlanManager
    from supertable.utils.sql_parser import SQLParser
    from supertable.utils.timer import Timer

    parser = SQLParser(
        super_name=str(plan["super_name"]),
        query=str(plan["sql"]),
        dialect=engine.dialect,
    )
    reflection = _build_reflection(plan)
    query_manager = QueryPlanManager(
        super_name=str(plan["super_name"]),
        organization="islanddb-benchmark",
        current_meta_path=f"benchmark://{plan['name']}/{sample_index}",
        query=str(plan["sql"]),
    )
    timer = Timer(show_timing=False)
    plan_stats = PlanStats()
    cache_root = os.environ.get("SUPERTABLE_ISLAND_CACHE_DIR", "")
    cache_before = _tree_footprint(cache_root)
    arrow_pool = pa.default_memory_pool()
    arrow_before = int(arrow_pool.bytes_allocated())

    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    with _PeakRSS() as rss:
        frame, used = executor.execute(
            engine=engine,
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer=timer,
            plan_stats=plan_stats,
            log_prefix="[islanddb.benchmark] ",
        )
    wall_seconds = time.perf_counter() - wall_start
    cpu_seconds = time.process_time() - cpu_start
    arrow_after = int(arrow_pool.bytes_allocated())
    arrow_peak = int(arrow_pool.max_memory())
    cache_after = _tree_footprint(cache_root)
    canonical = canonical_frame(frame)
    flattened = _flatten_plan_stats(plan_stats.stats)
    profile_path = Path(query_manager.query_plan_path)
    profile = _profile_metrics(profile_path)
    try:
        profile_path.unlink()
    except OSError:
        pass

    expected_used = str(engine.value)
    if str(used) != expected_used:
        raise RuntimeError(
            f"explicit {expected_used} benchmark executed unexpected engine {used!r}"
        )
    return {
        "sample_index": sample_index,
        "engine": str(used),
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "rss_baseline_bytes": rss.baseline,
        "rss_peak_bytes": rss.peak,
        "rss_peak_delta_bytes": rss.delta,
        "arrow_bytes_before": arrow_before,
        "arrow_bytes_after": arrow_after,
        "arrow_peak_bytes_process": arrow_peak,
        "cache_footprint_before": cache_before,
        "cache_footprint_after": cache_after,
        "cache_footprint_delta_bytes": cache_after["bytes"] - cache_before["bytes"],
        "cache_metrics": _cache_metrics_from_plan(flattened),
        "plan_stats": flattened,
        "engine_profile": profile,
        "result": canonical,
        "result_digest": result_digest(canonical),
    }


def run_engine_series_in_process(request: Mapping[str, Any]) -> dict[str, Any]:
    """Worker-side entry point.  Call through :func:`run_isolated_worker`."""
    from supertable.engine.executor import Executor
    from supertable.storage.local_storage import LocalStorage

    engine_name = str(request["engine"])
    engine = _resolve_engine(engine_name)
    plan = dict(request["plan"])
    warm_repeats = int(request.get("warm_repeats", 0))
    if warm_repeats < 0:
        raise ValueError("warm_repeats cannot be negative")
    cold_mode = str(request.get("cold_mode", "process"))
    if cold_mode not in ("process", "fadvise"):
        raise ValueError("cold_mode must be process or fadvise")

    cold_advice: dict[str, Any]
    if cold_mode == "fadvise":
        cold_advice = _drop_os_cache_best_effort(plan.get("files") or [])
        os_cache_state = "best_effort_fadvise; eviction is not guaranteed"
    else:
        cold_advice = {"mode": "none", "supported": None, "advised_files": 0}
        os_cache_state = "uncontrolled"

    # The generated corpus is already the shared local object representation.
    # Passing the production LocalStorage adapter makes FileCache record this
    # honestly as local/no-copy coverage for both engines instead of treating a
    # storage-less benchmark as a remote-cache fallback.
    executor = Executor(storage=LocalStorage(), organization="islanddb-benchmark")
    # Avoid a Redis connection attempt while retaining the production Executor
    # and its normal env/default configuration resolution.
    executor._catalog = False
    samples = []
    for index in range(1 + warm_repeats):
        sample = _execute_one(executor, engine, plan, index)
        sample["temperature"] = "cold" if index == 0 else "warm"
        samples.append(sample)

    first_digest = samples[0]["result_digest"]
    if any(sample["result_digest"] != first_digest for sample in samples[1:]):
        raise RuntimeError(
            f"{engine_name} returned inconsistent results across cold/warm repeats"
        )
    return {
        "engine": engine_name,
        "engine_value": str(engine.value),
        "cold_definition": "fresh worker process and fresh engine connection",
        "os_cache_state": os_cache_state,
        "cold_advice": cold_advice,
        "result": samples[0]["result"],
        "result_digest": first_digest,
        "samples": samples,
    }


def worker_main(request_path: str | Path, response_path: str | Path) -> int:
    """JSON-file protocol used by the isolated ``_worker`` module."""
    request_file = Path(request_path)
    response_file = Path(response_path)
    try:
        request = json.loads(request_file.read_text(encoding="utf-8"))
        result = run_engine_series_in_process(request)
        response = {"ok": True, "result": result}
        code = 0
    except Exception as exc:  # noqa: BLE001 - preserve worker failure context
        response = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        code = 1
    response_file.write_text(
        json.dumps(response, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return code


def run_isolated_worker(
    request: Mapping[str, Any],
    *,
    cache_dir: str | Path,
    home_dir: str | Path,
    timeout_seconds: float = 3600,
) -> dict[str, Any]:
    """Run one engine series with env configuration fixed before imports."""
    control_root = Path(tempfile.mkdtemp(prefix="islanddb-benchmark-control-"))
    request_path = control_root / "request.json"
    response_path = control_root / "response.json"
    request_path.write_text(
        json.dumps(request, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    cache_path = Path(cache_dir).expanduser().resolve()
    home_path = Path(home_dir).expanduser().resolve()
    cache_path.mkdir(parents=True, exist_ok=True)
    home_path.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[3]
    environment = dict(os.environ)
    environment["SUPERTABLE_HOME"] = str(home_path)
    environment["SUPERTABLE_ISLAND_CACHE_DIR"] = str(cache_path)
    environment["SUPERTABLE_ISLAND_RANGE_CACHE_DIR"] = str(
        cache_path / "ranges"
    )
    environment.setdefault("SUPERTABLE_DUCKDB_THREADS", "1")
    existing_pythonpath = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        str(repo_root)
        if not existing_pythonpath
        else str(repo_root) + os.pathsep + existing_pythonpath
    )
    command = [
        sys.executable,
        "-m",
        "supertable.engine.benchmarks._worker",
        str(request_path),
        str(response_path),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        if not response_path.is_file():
            raise BenchmarkWorkerError(
                f"benchmark worker produced no response (exit={completed.returncode}); "
                f"stdout={completed.stdout[-2000:]!r}; stderr={completed.stderr[-2000:]!r}"
            )
        response = json.loads(response_path.read_text(encoding="utf-8"))
        if not response.get("ok"):
            raise BenchmarkWorkerError(
                f"benchmark worker failed (exit={completed.returncode}): "
                f"{response.get('error')}\n{response.get('traceback', '')}"
            )
        if completed.returncode != 0:
            raise BenchmarkWorkerError(
                f"benchmark worker returned exit {completed.returncode} despite success response"
            )
        return response["result"]
    finally:
        try:
            for child in control_root.iterdir():
                child.unlink()
            control_root.rmdir()
        except OSError:
            pass


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    location = (len(ordered) - 1) * percentile
    lower = int(math.floor(location))
    upper = int(math.ceil(location))
    if lower == upper:
        return ordered[lower]
    weight = location - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize_series(series: Mapping[str, Any]) -> dict[str, Any]:
    samples = list(series.get("samples") or [])
    cold = next((sample for sample in samples if sample.get("temperature") == "cold"), None)
    warm = [sample for sample in samples if sample.get("temperature") == "warm"]
    values = [float(sample["wall_seconds"]) for sample in warm]
    return {
        "cold_wall_seconds": cold.get("wall_seconds") if cold else None,
        "warm_samples": len(values),
        "warm_wall_seconds_min": min(values) if values else None,
        "warm_wall_seconds_median": statistics.median(values) if values else None,
        "warm_wall_seconds_p25": _percentile(values, 0.25),
        "warm_wall_seconds_p75": _percentile(values, 0.75),
        "warm_wall_seconds_p95": _percentile(values, 0.95),
        "max_rss_peak_bytes": max(
            (int(sample["rss_peak_bytes"]) for sample in samples if sample.get("rss_peak_bytes") is not None),
            default=None,
        ),
        "max_rss_peak_delta_bytes": max(
            (
                int(sample["rss_peak_delta_bytes"])
                for sample in samples
                if sample.get("rss_peak_delta_bytes") is not None
            ),
            default=None,
        ),
    }


WorkerRunner = Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class ComparisonConfig:
    warm_repeats: int = 5
    workloads: tuple[str, ...] = (
        "no_match",
        "point",
        "range_1pct",
        "range_10pct",
        "projection",
    )
    cold_mode: str = "process"
    timeout_seconds: float = 3600

    def __post_init__(self) -> None:
        if self.warm_repeats <= 0:
            raise ValueError("warm_repeats must be positive")
        if self.cold_mode not in ("process", "fadvise"):
            raise ValueError("cold_mode must be process or fadvise")


def compare_manifest(
    manifest: Mapping[str, Any],
    *,
    cache_root: str | Path,
    home_root: str | Path,
    config: ComparisonConfig,
    worker_runner: WorkerRunner = run_isolated_worker,
) -> dict[str, Any]:
    """Compare both explicit engines for every selected workload.

    The two parity workers always finish and compare before either timing
    worker is launched for that workload.
    """
    selected_names = normalize_workloads(config.workloads)
    workloads = build_workloads(int(manifest["total_rows"]))
    tier = str(manifest["spec"]["tier"])
    cache_base = Path(cache_root).expanduser().resolve()
    home_base = Path(home_root).expanduser().resolve()
    run_id = uuid.uuid4().hex
    records: list[dict[str, Any]] = []

    for workload_index, name in enumerate(selected_names):
        plan = plan_workload(manifest, workloads[name])
        label = f"{tier}/{name}"
        parity_results: dict[str, Any] = {}
        for engine_name in ENGINE_NAMES:
            request = {
                "purpose": "parity",
                "engine": engine_name,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
            }
            parity_results[engine_name] = worker_runner(
                request,
                cache_dir=cache_base / run_id / "parity" / name,
                home_dir=home_base / run_id / "parity" / engine_name / name,
                timeout_seconds=config.timeout_seconds,
            )
        digest = assert_exact_parity(
            parity_results[ENGINE_DUCKDB],
            parity_results[ENGINE_ISLAND],
            label=label,
        )

        # Alternate timing order across workloads to reduce a stable order bias.
        order = list(ENGINE_NAMES)
        if workload_index % 2:
            order.reverse()
        series: dict[str, Any] = {}
        shared_cache = cache_base / run_id / "timing" / name
        for engine_name in order:
            request = {
                "purpose": "timing",
                "engine": engine_name,
                "plan": plan,
                "warm_repeats": config.warm_repeats,
                "cold_mode": config.cold_mode,
            }
            result = worker_runner(
                request,
                cache_dir=shared_cache,
                home_dir=home_base / run_id / "timing" / engine_name / name,
                timeout_seconds=config.timeout_seconds,
            )
            for sample in result.get("samples") or []:
                if sample.get("result_digest") != digest:
                    raise BenchmarkParityError(
                        f"timed {engine_name} result changed after parity gate for {label}"
                    )
            series[engine_name] = result

        summaries = {engine: summarize_series(result) for engine, result in series.items()}
        duck_median = summaries[ENGINE_DUCKDB]["warm_wall_seconds_median"]
        island_median = summaries[ENGINE_ISLAND]["warm_wall_seconds_median"]
        speedup = (
            duck_median / island_median
            if duck_median is not None and island_median not in (None, 0)
            else None
        )
        records.append(
            {
                "workload": name,
                "query": plan["sql"],
                "input": {
                    key: plan[key]
                    for key in (
                        "source_bytes",
                        "candidate_source_bytes",
                        "estimated_reflection_bytes",
                        "estimated_pushdown_bytes",
                        "files_before_prune",
                        "files_after_prune",
                        "files_pruned",
                        "row_groups_after_file_prune",
                        "row_groups_pushdown_eligible",
                        "required_columns",
                    )
                },
                "parity": {
                    "matched": True,
                    "oracle": ENGINE_DUCKDB,
                    "result_digest": digest,
                    "checked_before_timing": True,
                },
                "timing_order": order,
                "engines": series,
                "summary": summaries,
                "islanddb_speedup_over_duckdb_warm_median": speedup,
            }
        )

    return {
        "tier": tier,
        "target_source_bytes": int(manifest["target_source_bytes"]),
        "actual_source_bytes": int(manifest["actual_source_bytes"]),
        "total_rows": int(manifest["total_rows"]),
        "file_count": len(manifest.get("files") or []),
        "run_id": run_id,
        "workloads": records,
    }


def environment_metadata() -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "islanddb_available": islanddb_available(),
    }
    try:
        import duckdb

        metadata["duckdb"] = duckdb.__version__
    except Exception as exc:
        metadata["duckdb_error"] = f"{type(exc).__name__}: {exc}"
    try:
        import pyarrow

        metadata["pyarrow"] = pyarrow.__version__
    except Exception as exc:
        metadata["pyarrow_error"] = f"{type(exc).__name__}: {exc}"
    return metadata


def build_artifact(
    comparisons: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "format_version": RESULT_FORMAT_VERSION,
        "generated_unix_ms": int(time.time() * 1000),
        "benchmark": "duckdb_lite_vs_islanddb",
        "oracle": ENGINE_DUCKDB,
        "parity_is_blocking": True,
        "performance_thresholds_are_blocking": False,
        "environment": environment_metadata(),
        "config": _json_value(config),
        "comparisons": list(comparisons),
    }


def write_artifact(path: str | Path, artifact: Mapping[str, Any]) -> Path:
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(artifact, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)
    return destination
