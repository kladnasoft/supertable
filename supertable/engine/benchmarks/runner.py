"""Parity-first DuckDB versus IslandDB benchmark runner.

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

from .corpus import (
    GIB,
    build_workloads,
    normalize_workloads,
    plan_workload,
    repeated_manifest_paths,
)


RESULT_FORMAT_VERSION = 1
ENGINE_DUCKDB = "duckdb"
ENGINE_ISLAND = "islanddb"
ENGINE_NAMES = (ENGINE_DUCKDB, ENGINE_ISLAND)
# The spill workload has only 1,024 generated groups (roughly 256 KiB for the
# normal 30-column corpus), while IslandDB's safe generic result estimator must
# assume one output row per input row.  Keep the benchmark escape hatch bounded
# independently of that conservative estimate.
ISLAND_STREAM_RESULT_MAX_BYTES = 64 * 1024**2


def _duckdb_memory_limit_text(limit_bytes: int) -> str:
    """Return an exact DuckDB-valid size without falling back to bare GB."""
    if limit_bytes <= 0:
        raise ValueError("memory limit must be positive")
    units = ((GIB, "GiB"), (1024**2, "MiB"), (1024, "KiB"))
    for divisor, suffix in units:
        if limit_bytes % divisor == 0:
            return f"{limit_bytes // divisor}{suffix}"
    # DuckDB accepts fractional GiB values. Seventeen significant digits are
    # sufficient to round-trip the integer-byte ratio used by this harness.
    return f"{limit_bytes / GIB:.17g}GiB"


def _cgroup_v2_memory_telemetry(
    *,
    proc_cgroup: str | Path = "/proc/self/cgroup",
    cgroup_root: str | Path = "/sys/fs/cgroup",
) -> dict[str, Any]:
    """Read this process' cgroup-v2 memory counters without escaping its mount."""
    telemetry: dict[str, Any] = {"available": False}
    try:
        proc_text = Path(proc_cgroup).read_text(encoding="utf-8")
    except OSError as exc:
        telemetry["reason"] = f"proc_cgroup_unavailable:{type(exc).__name__}"
        return telemetry

    relative: str | None = None
    for line in proc_text.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            relative = parts[2].lstrip("/")
            break
    if relative is None:
        telemetry["reason"] = "cgroup_v2_entry_missing"
        return telemetry

    try:
        root = Path(cgroup_root).resolve(strict=True)
        current = (root / relative).resolve(strict=True)
        current.relative_to(root)
    except (OSError, ValueError) as exc:
        telemetry["reason"] = f"cgroup_path_invalid:{type(exc).__name__}"
        return telemetry

    def safe_counter(name: str) -> Path:
        candidate = (current / name).resolve(strict=True)
        candidate.relative_to(current)
        return candidate

    def read_scalar(name: str) -> tuple[int | None, str | None]:
        try:
            raw = safe_counter(name).read_text(encoding="utf-8").strip()
        except (OSError, ValueError):
            return None, None
        if raw == "max":
            return None, raw
        try:
            return max(0, int(raw)), raw
        except ValueError:
            return None, raw[:128]

    def read_events(name: str) -> dict[str, int] | None:
        try:
            raw = safe_counter(name).read_text(encoding="utf-8")[:65_536]
        except (OSError, ValueError):
            return None
        values: dict[str, int] = {}
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) != 2:
                continue
            try:
                values[parts[0]] = max(0, int(parts[1]))
            except ValueError:
                continue
        return values

    def read_text(name: str, limit: int = 65_536) -> str | None:
        try:
            return safe_counter(name).read_text(encoding="utf-8")[:limit]
        except (OSError, ValueError):
            return None

    normalized_relative = current.relative_to(root)
    normalized_path = "/" if str(normalized_relative) == "." else f"/{normalized_relative}"
    telemetry.update({
        "available": True,
        "path": normalized_path,
        "semantics": "cumulative counters for the containing cgroup",
    })
    for prefix, filename in (
        ("memory_current", "memory.current"),
        ("memory_peak", "memory.peak"),
        ("memory_max", "memory.max"),
        ("swap_current", "memory.swap.current"),
        ("swap_peak", "memory.swap.peak"),
        ("swap_max", "memory.swap.max"),
    ):
        parsed, raw = read_scalar(filename)
        telemetry[f"{prefix}_bytes"] = parsed
        if raw is not None and parsed is None:
            telemetry[f"{prefix}_raw"] = raw
    telemetry["memory_events"] = read_events("memory.events")
    telemetry["memory_stat"] = read_events("memory.stat")
    telemetry["memory_pressure"] = read_text("memory.pressure")
    telemetry["io_stat"] = read_text("io.stat")
    return telemetry


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
        return Engine.DUCKDB
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
        IntegerDomainBound, Reflection, RowGroupSelection, SuperSnapshot,
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
        candidate_row_groups=int(
            plan.get("row_groups_pushdown_eligible") or 0
        ),
        candidate_row_groups_complete=(
            "row_groups_pushdown_eligible" in plan
            and plan.get("row_groups_pushdown_eligible") is not None
        ),
        column_max_value_bytes={
            str(column): int(plan["payload_width"])
            for column in plan["required_columns"]
            if str(column).startswith("payload_")
        },
        integer_domain_bounds={
            str(column).casefold(): IntegerDomainBound(
                minimum=values.get("minimum"),
                maximum=values.get("maximum"),
                has_null=values.get("has_null", False),
            )
            for column, values in (
                plan.get("integer_domain_bounds") or {}
            ).items()
            if isinstance(values, Mapping)
        },
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
        # The generator writes only non-null fixed-width Arrow fields. The
        # corpus planner derives an exact selected-row value-buffer width from
        # that sealed schema; this complete estimate is benchmark-only and must
        # not be generalized to arbitrary Parquet Binary/String columns.
        decoded_bytes=int(plan["estimated_decoded_bytes"]),
        decoded_bytes_complete=bool(plan["decoded_estimate_complete"]),
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
        "telemetry_query_id",
        "estimated_candidate_files",
        "estimated_candidate_files_complete",
        "estimated_candidate_row_groups",
        "estimated_candidate_row_groups_complete",
        "planned_files",
        "planned_files_complete",
        "planned_row_groups",
        "planned_row_groups_complete",
        "planned_rows",
        "planned_rows_complete",
        "planned_units_scope",
        "observed_files",
        "observed_files_measured",
        "observed_row_groups",
        "observed_row_groups_measured",
        "observed_rows_scanned",
        "observed_rows_scanned_measured",
        "execution_outcome",
        "result_complete",
        "selected_row_groups",
        "selected_row_groups_scope",
        "cpu_time_ms",
        "cpu_time_measured",
        "cpu_time_scope",
        "estimated_logical_scan_bytes",
        "estimated_logical_scan_bytes_complete",
        "logical_scan_bytes",
        "logical_scan_bytes_complete",
        "physical_read_bytes",
        "physical_read_bytes_measured",
        "physical_read_scope",
        "estimated_decoded_bytes",
        "estimated_decoded_bytes_complete",
        "decoded_bytes",
        "decoded_bytes_complete",
        "estimated_candidate_rows",
        "estimated_candidate_rows_complete",
        "rows_scanned",
        "rows_scanned_measured",
        "result_rows",
        "result_rows_scope",
        "result_batches",
        "result_batches_scope",
        "result_bytes",
        "result_bytes_scope",
        "rss_baseline_bytes",
        "rss_peak_bytes",
        "rss_final_bytes",
        "rss_peak_delta_bytes",
        "rss_retained_delta_bytes",
        "rss_measured",
        "rss_sample_interval_ms",
        "rss_scope",
        "peak_memory_bytes",
        "peak_memory_scope",
        "elapsed_scope",
        "phase_timings_ms",
        "phase_timings_scope",
        "profile_persist_ms",
        "profile_persist_ms_measured",
        "profile_persist_succeeded",
        "spill_bytes",
        "spill_bytes_measured",
        "spill_bytes_scope",
        "spill_occurred",
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


def _proc_io_counters(path: str | Path = "/proc/self/io") -> dict[str, int] | None:
    """Read Linux process-I/O counters; return ``None`` when unavailable."""
    try:
        raw = Path(path).read_text(encoding="ascii")
    except OSError:
        return None
    counters: dict[str, int] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        try:
            counters[key.strip()] = max(0, int(value.strip()))
        except ValueError:
            continue
    return counters or None


def _counter_delta(
    before: Mapping[str, int] | None,
    after: Mapping[str, int] | None,
) -> dict[str, int] | None:
    if before is None or after is None:
        return None
    return {
        key: max(0, int(after[key]) - int(before[key]))
        for key in sorted(before.keys() & after.keys())
    }


def _validate_cold_physical_read(
    *,
    engine_name: str,
    plan: Mapping[str, Any],
    cold_advice: Mapping[str, Any],
    sample: Mapping[str, Any],
    minimum_fraction: float,
) -> dict[str, Any]:
    """Fail rather than label a metadata/page-cache hit as a full cold scan."""
    if minimum_fraction <= 0 or minimum_fraction > 1:
        raise ValueError("minimum cold-read fraction must be in (0, 1]")
    if cold_advice.get("supported") is not True or int(cold_advice.get("errors", 0)):
        raise BenchmarkUnavailableError(
            "verified cold physical reads require successful POSIX_FADV_DONTNEED "
            "for every source file"
        )
    # Repeated source paths create a larger logical scan without a second copy
    # on disk. The kernel can satisfy repeats two..N from page cache inside one
    # query, so verified block I/O is bounded against unique backing chunks.
    expected = int(
        plan.get("unique_estimated_pushdown_bytes")
        or plan.get("estimated_pushdown_bytes")
        or 0
    )
    observed = int((sample.get("process_io_delta") or {}).get("read_bytes") or 0)
    fraction = observed / expected if expected > 0 else 0.0
    verification = {
        "expected_projected_bytes": expected,
        "observed_process_read_bytes": observed,
        "observed_fraction": fraction,
        "minimum_fraction": minimum_fraction,
        "passed": fraction >= minimum_fraction,
    }
    if not verification["passed"]:
        raise BenchmarkWorkerError(
            f"{engine_name} cold full scan read only {observed:,} physical bytes "
            f"for {expected:,} projected bytes ({fraction:.3%}); refusing to "
            "report a full-source cold benchmark"
        )
    return verification


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

    process_io_before = _proc_io_counters()
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    island_streaming_result = bool(plan.get("island_streaming_result")) and (
        str(engine.value) == ENGINE_ISLAND
    )
    with _PeakRSS() as rss:
        if island_streaming_result:
            stream, used = executor.execute_stream(
                engine=engine,
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer=timer,
                plan_stats=plan_stats,
                log_prefix="[islanddb.benchmark] ",
            )
            with stream:
                table = stream.collect_table(
                    max_bytes=ISLAND_STREAM_RESULT_MAX_BYTES,
                )
            frame = table.to_pandas()
            result_mode = "arrow_stream"
        else:
            frame, used = executor.execute(
                engine=engine,
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer=timer,
                plan_stats=plan_stats,
                log_prefix="[islanddb.benchmark] ",
            )
            result_mode = "pandas"
    wall_seconds = time.perf_counter() - wall_start
    cpu_seconds = time.process_time() - cpu_start
    process_io_after = _proc_io_counters()
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
        "result_mode": result_mode,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "rss_baseline_bytes": rss.baseline,
        "rss_peak_bytes": rss.peak,
        "rss_peak_delta_bytes": rss.delta,
        "process_io_before": process_io_before,
        "process_io_after": process_io_after,
        "process_io_delta": _counter_delta(process_io_before, process_io_after),
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

    cgroup_before = _cgroup_v2_memory_telemetry()

    engine_name = str(request["engine"])
    engine = _resolve_engine(engine_name)
    plan = dict(request["plan"])
    warm_repeats = int(request.get("warm_repeats", 0))
    if warm_repeats < 0:
        raise ValueError("warm_repeats cannot be negative")
    cold_mode = str(request.get("cold_mode", "process"))
    if cold_mode not in ("process", "fadvise"):
        raise ValueError("cold_mode must be process or fadvise")
    configured_memory_limit = request.get("memory_limit_bytes")
    if configured_memory_limit is not None:
        configured_memory_limit = int(configured_memory_limit)
        if configured_memory_limit <= 0:
            raise ValueError("memory_limit_bytes must be positive")
    configured_threads = request.get("threads")
    if configured_threads is not None:
        configured_threads = int(configured_threads)
        if configured_threads <= 0:
            raise ValueError("threads must be positive")
    minimum_cold_read_fraction = request.get("minimum_cold_read_fraction")
    if minimum_cold_read_fraction is not None:
        minimum_cold_read_fraction = float(minimum_cold_read_fraction)
        if minimum_cold_read_fraction <= 0 or minimum_cold_read_fraction > 1:
            raise ValueError("minimum_cold_read_fraction must be in (0, 1]")
        if cold_mode != "fadvise":
            raise ValueError(
                "minimum_cold_read_fraction requires cold_mode='fadvise'"
            )

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

    duckdb_connection = getattr(executor.duckdb_exec, "_con", None)
    if bool(request.get("disable_caches", False)) and duckdb_connection is not None:
        # Empty cannot override EngineConfig's historical non-empty default via
        # environment resolution. Assert the benchmark contract directly after
        # every production query has re-applied its live runtime settings.
        duckdb_connection.execute("SET enable_external_file_cache=false")

    first_digest = samples[0]["result_digest"]
    if any(sample["result_digest"] != first_digest for sample in samples[1:]):
        raise RuntimeError(
            f"{engine_name} returned inconsistent results across cold/warm repeats"
        )
    cold_read_verification = None
    if minimum_cold_read_fraction is not None:
        cold_read_verification = _validate_cold_physical_read(
            engine_name=engine_name,
            plan=plan,
            cold_advice=cold_advice,
            sample=samples[0],
            minimum_fraction=minimum_cold_read_fraction,
        )
    # Record the actual parallelism used by the production executors.  A
    # benchmark that silently pins one engine to a different worker count is
    # not reproducible and can reverse the result for narrow scans.
    import polars as pl

    cgroup_after = _cgroup_v2_memory_telemetry()
    cgroup_event_delta = _counter_delta(
        cgroup_before.get("memory_events"),
        cgroup_after.get("memory_events"),
    )
    if cgroup_event_delta and (
        cgroup_event_delta.get("oom", 0)
        or cgroup_event_delta.get("oom_kill", 0)
        or cgroup_event_delta.get("oom_group_kill", 0)
    ):
        raise BenchmarkWorkerError(
            f"{engine_name} triggered cgroup OOM events: {cgroup_event_delta}"
        )

    execution_context: dict[str, Any] = {
        "logical_cpu_count": os.cpu_count(),
        "polars_thread_pool_size": int(pl.thread_pool_size()),
        "configured_memory_limit_bytes": configured_memory_limit,
        "configured_threads": configured_threads,
        "caches_disabled": bool(request.get("disable_caches", False)),
        "duckdb_memory_limit_env": os.environ.get(
            "SUPERTABLE_DUCKDB_MEMORY_LIMIT", ""
        ),
        "duckdb_http_metadata_cache_env": os.environ.get(
            "SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE", ""
        ),
        "island_max_memory_bytes_env": os.environ.get(
            "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES", ""
        ),
        "duckdb_threads_override": os.environ.get(
            "SUPERTABLE_DUCKDB_THREADS", ""
        ),
        "cgroup_v2": cgroup_after,
        "cgroup_v2_before": cgroup_before,
        "cgroup_memory_event_delta": cgroup_event_delta,
    }
    if duckdb_connection is not None:
        try:
            execution_context["duckdb_threads"] = int(
                duckdb_connection.execute(
                    "SELECT current_setting('threads')"
                ).fetchone()[0]
            )
        except Exception:
            execution_context["duckdb_threads"] = None
        try:
            execution_context["duckdb_memory_limit"] = str(
                duckdb_connection.execute(
                    "SELECT current_setting('memory_limit')"
                ).fetchone()[0]
            )
        except Exception:
            execution_context["duckdb_memory_limit"] = None
        for setting in ("enable_external_file_cache", "temp_directory"):
            try:
                execution_context[f"duckdb_{setting}"] = _json_value(
                    duckdb_connection.execute(
                        f"SELECT current_setting('{setting}')"
                    ).fetchone()[0]
                )
            except Exception:
                execution_context[f"duckdb_{setting}"] = None
    island_executor = getattr(executor, "island_exec", None)
    island_resources = getattr(island_executor, "_resources", None)
    island_policy = getattr(island_executor, "_policy", None)
    execution_context["island_memory_limit_bytes"] = getattr(
        island_resources, "memory_limit_bytes", None
    )
    execution_context["island_memory_available_bytes"] = getattr(
        island_resources, "memory_available_bytes", None
    )
    execution_context["island_query_memory_fraction"] = getattr(
        island_policy, "query_memory_fraction", None
    )
    execution_context["island_global_memory_fraction"] = getattr(
        island_policy, "global_memory_fraction", None
    )

    return {
        "engine": engine_name,
        "engine_value": str(engine.value),
        "execution_context": execution_context,
        "cold_definition": "fresh worker process and fresh engine connection",
        "os_cache_state": os_cache_state,
        "cold_advice": cold_advice,
        "cold_physical_read_verification": cold_read_verification,
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
    configured_memory_limit = request.get("memory_limit_bytes")
    if configured_memory_limit is not None:
        configured_memory_limit = int(configured_memory_limit)
        if configured_memory_limit <= 0:
            raise ValueError("memory_limit_bytes must be positive")
    configured_threads = request.get("threads")
    if configured_threads is not None:
        configured_threads = int(configured_threads)
        if configured_threads <= 0:
            raise ValueError("threads must be positive")
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
    if configured_memory_limit is not None:
        environment["SUPERTABLE_DUCKDB_MEMORY_LIMIT"] = (
            _duckdb_memory_limit_text(configured_memory_limit)
        )
        environment["SUPERTABLE_ISLAND_MAX_MEMORY_BYTES"] = str(
            configured_memory_limit
        )
        # The CLI value is already the benchmark's conservative internal
        # workspace (normally 6GiB inside an 8GiB cgroup), so do not apply the
        # production 60%/80% admission fractions a second time.
        environment["SUPERTABLE_ISLAND_MEMORY_FRACTION"] = "1.0"
        environment["SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION"] = "1.0"
    if configured_threads is not None:
        environment["SUPERTABLE_DUCKDB_THREADS"] = str(configured_threads)
        environment["SUPERTABLE_ISLAND_CPU_MAX"] = str(configured_threads)
        environment["SUPERTABLE_ISLAND_IO_WORKERS_MAX"] = str(configured_threads)
        environment["POLARS_MAX_THREADS"] = str(configured_threads)
    if bool(request.get("disable_caches", False)):
        # EngineConfig treats an empty env value as absent and restores its 5GB
        # default. "0" normalizes to the explicit disabled representation and
        # apply_runtime_pragmas turns the cache off before user SQL executes.
        environment["SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE"] = "0"
        environment["SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE"] = "false"
        environment["SUPERTABLE_ISLAND_CACHE_ENABLED"] = "false"
        environment["SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED"] = "false"
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
    memory_limit_bytes: int | None = None
    minimum_cold_read_fraction: float | None = None
    source_repeat: int = 1
    threads: int | None = None
    disable_caches: bool = False

    def __post_init__(self) -> None:
        if self.warm_repeats <= 0:
            raise ValueError("warm_repeats must be positive")
        if self.cold_mode not in ("process", "fadvise"):
            raise ValueError("cold_mode must be process or fadvise")
        if self.memory_limit_bytes is not None and self.memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be positive")
        if self.minimum_cold_read_fraction is not None:
            if not 0 < self.minimum_cold_read_fraction <= 1:
                raise ValueError("minimum_cold_read_fraction must be in (0, 1]")
            if self.cold_mode != "fadvise":
                raise ValueError(
                    "minimum_cold_read_fraction requires cold_mode='fadvise'"
                )
        if self.source_repeat <= 0:
            raise ValueError("source_repeat must be positive")
        if self.threads is not None and self.threads <= 0:
            raise ValueError("threads must be positive")


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
    prepared_repeat = int(manifest.get("source_repeat") or 1)
    prepared_mode = str(manifest.get("source_repeat_mode") or "none")
    if prepared_mode == "none" and config.source_repeat > 1:
        with repeated_manifest_paths(manifest, config.source_repeat) as repeated:
            return compare_manifest(
                repeated,
                cache_root=cache_root,
                home_root=home_root,
                config=config,
                worker_runner=worker_runner,
            )
    if prepared_repeat != config.source_repeat:
        raise ValueError(
            "execution manifest source_repeat does not match comparison config"
        )
    selected_names = normalize_workloads(config.workloads)
    workloads = build_workloads(
        int(manifest["total_rows"]),
        payload_columns=int(manifest["spec"].get("payload_columns", 8)),
    )
    tier = str(manifest["spec"]["tier"])
    cache_base = Path(cache_root).expanduser().resolve()
    home_base = Path(home_root).expanduser().resolve()
    run_id = uuid.uuid4().hex
    records: list[dict[str, Any]] = []

    for workload_index, name in enumerate(selected_names):
        plan = plan_workload(manifest, workloads[name])
        if name in {"full_scan", "spill_group"} and float(
            plan["projected_source_fraction"]
        ) < 0.95:
            raise BenchmarkUnavailableError(
                f"{name} selected-column chunks cover less than 95% of the "
                "physical corpus; increase payload columns/width before claiming "
                "a full-source benchmark"
            )
        label = f"{tier}/{name}"
        parity_results: dict[str, Any] = {}
        for engine_name in ENGINE_NAMES:
            request = {
                "purpose": "parity",
                "engine": engine_name,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
                "memory_limit_bytes": config.memory_limit_bytes,
                "threads": config.threads,
                "disable_caches": config.disable_caches,
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
                "memory_limit_bytes": config.memory_limit_bytes,
                "minimum_cold_read_fraction": config.minimum_cold_read_fraction,
                "threads": config.threads,
                "disable_caches": config.disable_caches,
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
                        "unique_source_bytes",
                        "source_repeat",
                        "source_repeat_mode",
                        "candidate_source_bytes",
                        "estimated_reflection_bytes",
                        "estimated_pushdown_bytes",
                        "unique_estimated_reflection_bytes",
                        "unique_estimated_pushdown_bytes",
                        "estimated_decoded_bytes",
                        "decoded_row_width",
                        "decoded_estimate_complete",
                        "projected_source_fraction",
                        "files_before_prune",
                        "unique_files_before_prune",
                        "files_after_prune",
                        "files_pruned",
                        "row_groups_after_file_prune",
                        "row_groups_pushdown_eligible",
                        "required_columns",
                        "island_streaming_result",
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
        "logical_source_bytes": int(manifest["actual_source_bytes"])
        * config.source_repeat,
        "source_repeat": config.source_repeat,
        "total_rows": int(manifest["total_rows"]),
        "logical_total_rows": int(manifest["total_rows"]) * config.source_repeat,
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
        "benchmark": "duckdb_vs_islanddb",
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
