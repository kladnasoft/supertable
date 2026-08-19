#!/usr/bin/env python3
"""Deterministic, cross-version DataWriter benchmark and correctness gate.

This file is intentionally independent of the installed ``supertable``
benchmark package.  Mount the same file into each benchmark container and use
``--package-root`` (or the container's installed wheel) to select the version
under test.  Only public DataWriter/DataReader APIs and feature-detected enum
members are used.

The benchmark uses LocalStorage plus a process-local, Lua-capable fakeredis.
RBAC checks are replaced by an explicit benchmark-only allow-all boundary so
the measured work does not depend on an external deployment.  DataWriter's
real monitoring payload is intercepted after it is prepared, which exposes
the production profiler's phase timings and counters without a monitoring
worker or Redis/network noise.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import inspect
import json
import math
import os
import platform
import resource
import statistics
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPORT_SCHEMA = "supertable-random-write-trace-v1"
COMPARISON_SCHEMA = "supertable-random-write-comparison-v1"
MASK64 = (1 << 64) - 1


class SplitMix64:
    """Tiny stable PRNG; unlike ``random``, its stream is part of this file."""

    def __init__(self, seed: int) -> None:
        self.state = int(seed) & MASK64

    def next_u64(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK64
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
        return (value ^ (value >> 31)) & MASK64

    def bounded(self, upper: int) -> int:
        if upper <= 0:
            raise ValueError("upper must be positive")
        return self.next_u64() % upper


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sample_keys(rng: SplitMix64, state: Mapping[int, Any], count: int) -> list[int]:
    pool = sorted(state)
    limit = min(max(0, int(count)), len(pool))
    for index in range(limit):
        selected = index + rng.bounded(len(pool) - index)
        pool[index], pool[selected] = pool[selected], pool[index]
    return sorted(pool[:limit])


def _new_value(rng: SplitMix64) -> tuple[int, str]:
    value = int(rng.bounded(2_000_001)) - 1_000_000
    category = f"c{rng.bounded(17):02d}"
    return value, category


def _records_digest(state: Mapping[int, tuple[int, str]]) -> str:
    digest = hashlib.sha256()
    for row_id in sorted(state):
        value, category = state[row_id]
        digest.update(_canonical_json_bytes([row_id, value, category]))
        digest.update(b"\n")
    return digest.hexdigest()


def _expected_statistics(state: Mapping[int, tuple[int, str]]) -> dict[str, Any]:
    values = [item[0] for item in state.values()]
    if not values:
        raise ValueError("the deterministic trace must retain at least one row")
    total = sum(values)
    average = total / len(values)
    return {
        "row_count": len(values),
        "value_non_null_count": len(values),
        "value_null_count": 0,
        "value_sum": total,
        "value_avg": average,
        "value_avg_hex": float(average).hex(),
        "value_min": min(values),
        "value_max": max(values),
    }


def build_trace(
    *, seed: int, initial_rows: int, operations: int, batch_rows: int,
) -> dict[str, Any]:
    """Build one deterministic append/upsert/delete trace and its oracle."""
    if initial_rows <= 0:
        raise ValueError("initial_rows must be positive")
    if operations < 0:
        raise ValueError("operations cannot be negative")
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")

    rng = SplitMix64(seed)
    state: dict[int, tuple[int, str]] = {}
    trace: list[dict[str, Any]] = []
    next_id = 0

    def append_rows(count: int) -> list[list[Any]]:
        nonlocal next_id
        rows: list[list[Any]] = []
        for _ in range(count):
            value, category = _new_value(rng)
            row_id = next_id
            next_id += 1
            state[row_id] = (value, category)
            rows.append([row_id, value, category])
        return rows

    initial = append_rows(initial_rows)
    trace.append({
        "index": 0,
        "kind": "initial_append",
        "rows": initial,
        "expected_rows_after": len(state),
    })

    forced = ("append", "upsert", "delete")
    for offset in range(operations):
        if offset < len(forced):
            kind = forced[offset]
        else:
            choice = rng.bounded(100)
            kind = "append" if choice < 35 else "upsert" if choice < 75 else "delete"

        if kind == "append":
            rows = append_rows(batch_rows)
        elif kind == "upsert":
            keys = _sample_keys(rng, state, batch_rows)
            rows = []
            for row_id in keys:
                value, category = _new_value(rng)
                state[row_id] = (value, category)
                rows.append([row_id, value, category])
        else:
            # A delete batch may not erase the complete oracle estate.  This
            # also ensures every subsequent forced/random upsert is meaningful.
            delete_count = min(batch_rows, max(0, len(state) - 1))
            keys = _sample_keys(rng, state, delete_count)
            rows = [[row_id] for row_id in keys]
            for row_id in keys:
                del state[row_id]

        trace.append({
            "index": offset + 1,
            "kind": kind,
            "rows": rows,
            "expected_rows_after": len(state),
        })

    for operation in trace:
        operation["input_digest"] = _sha256_json({
            "kind": operation["kind"],
            "rows": operation["rows"],
        })

    trace_identity = [
        {
            "index": op["index"],
            "kind": op["kind"],
            "rows": op["rows"],
            "expected_rows_after": op["expected_rows_after"],
        }
        for op in trace
    ]
    return {
        "seed": int(seed),
        "initial_rows": int(initial_rows),
        "operations": int(operations),
        "batch_rows": int(batch_rows),
        "trace_digest": _sha256_json(trace_identity),
        "steps": trace,
        "expected": _expected_statistics(state),
        "expected_records_digest": _records_digest(state),
    }


def _read_text(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None


def _read_int(path: str) -> int | None:
    raw = _read_text(path)
    try:
        return None if raw in (None, "", "max") else int(raw)
    except (TypeError, ValueError):
        return None


def _proc_io() -> dict[str, int]:
    result: dict[str, int] = {}
    raw = _read_text("/proc/self/io") or ""
    for line in raw.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        try:
            result[key.strip()] = int(value.strip())
        except ValueError:
            continue
    return result


def _proc_rss_bytes() -> int | None:
    raw = _read_text("/proc/self/status") or ""
    for line in raw.splitlines():
        if line.startswith("VmRSS:"):
            fields = line.split()
            try:
                return int(fields[1]) * 1024
            except (IndexError, ValueError):
                return None
    return None


def _numeric_delta(after: Mapping[str, Any], before: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        if isinstance(old, (int, float)) and isinstance(new, (int, float)):
            result[key] = new - old
    return result


def _parse_flat_counter_file(path: str) -> dict[str, int]:
    result: dict[str, int] = {}
    raw = _read_text(path) or ""
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) == 2:
            try:
                result[fields[0]] = int(fields[1])
            except ValueError:
                pass
    return result


def _parse_pressure(path: str) -> dict[str, int]:
    result: dict[str, int] = {}
    raw = _read_text(path) or ""
    for line in raw.splitlines():
        fields = line.split()
        if not fields:
            continue
        scope = fields[0]
        for item in fields[1:]:
            key, separator, value = item.partition("=")
            if separator and key == "total":
                try:
                    result[f"{scope}_total_usec"] = int(value)
                except ValueError:
                    pass
    return result


def _parse_io_stat(path: str) -> dict[str, int]:
    totals: dict[str, int] = {}
    raw = _read_text(path) or ""
    for line in raw.splitlines():
        for item in line.split()[1:]:
            key, separator, value = item.partition("=")
            if not separator:
                continue
            try:
                totals[key] = totals.get(key, 0) + int(value)
            except ValueError:
                pass
    return totals


def _cgroup_snapshot() -> dict[str, Any]:
    root = Path("/sys/fs/cgroup")
    if not (root / "cgroup.controllers").exists():
        return {"available": False}
    return {
        "available": True,
        "cpu_stat": _parse_flat_counter_file(str(root / "cpu.stat")),
        "memory_current_bytes": _read_int(str(root / "memory.current")),
        "memory_peak_bytes": _read_int(str(root / "memory.peak")),
        "memory_events": _parse_flat_counter_file(str(root / "memory.events")),
        "io_stat": _parse_io_stat(str(root / "io.stat")),
        "cpu_pressure": _parse_pressure(str(root / "cpu.pressure")),
        "memory_pressure": _parse_pressure(str(root / "memory.pressure")),
        "io_pressure": _parse_pressure(str(root / "io.pressure")),
        "pids_current": _read_int(str(root / "pids.current")),
    }


def _cgroup_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    if not before.get("available") or not after.get("available"):
        return {"available": False}
    result: dict[str, Any] = {"available": True}
    for key in (
        "cpu_stat", "memory_events", "io_stat", "cpu_pressure",
        "memory_pressure", "io_pressure",
    ):
        result[key] = _numeric_delta(
            after.get(key, {}) or {}, before.get(key, {}) or {},
        )
    for key in ("memory_current_bytes", "memory_peak_bytes", "pids_current"):
        old = before.get(key)
        new = after.get(key)
        result[key] = {
            "before": old,
            "after": new,
            "delta": new - old if isinstance(old, int) and isinstance(new, int) else None,
        }
    return result


def _tree_footprint(root: Path) -> dict[str, Any]:
    count = 0
    total = 0
    suffixes: dict[str, dict[str, int]] = {}
    try:
        paths: Iterable[Path] = root.rglob("*")
        for path in paths:
            try:
                if not path.is_file():
                    continue
                size = path.stat().st_size
            except OSError:
                continue
            count += 1
            total += size
            suffix = path.suffix.casefold() or "<none>"
            bucket = suffixes.setdefault(suffix, {"files": 0, "bytes": 0})
            bucket["files"] += 1
            bucket["bytes"] += size
    except OSError:
        pass
    return {"files": count, "bytes": total, "by_suffix": suffixes}


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


class OperationTelemetry:
    """Measure one DataWriter call without including snapshot bookkeeping."""

    def __init__(self, storage_root: Path, sample_interval_ms: float) -> None:
        if sample_interval_ms <= 0:
            raise ValueError("sample_interval_ms must be positive")
        self.storage_root = storage_root
        self.interval = sample_interval_ms / 1000.0
        self.samples: list[tuple[int, int, int | None]] = []
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None
        self.before: dict[str, Any] = {}
        self.after: dict[str, Any] = {}

    def _sample(self) -> None:
        while True:
            self.samples.append(
                (time.perf_counter_ns(), time.process_time_ns(), _proc_rss_bytes())
            )
            if self.stop.wait(self.interval):
                return

    def start(self) -> None:
        self.before = {
            "process_io": _proc_io(),
            "cgroup": _cgroup_snapshot(),
            "storage": _tree_footprint(self.storage_root),
            "rusage": resource.getrusage(resource.RUSAGE_SELF),
            "rss_bytes": _proc_rss_bytes(),
        }
        self.wall_start_ns = time.perf_counter_ns()
        self.cpu_start_ns = time.process_time_ns()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()

    def finish(self) -> dict[str, Any]:
        wall_end_ns = time.perf_counter_ns()
        cpu_end_ns = time.process_time_ns()
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, self.interval * 4))
        self.samples.append((wall_end_ns, cpu_end_ns, _proc_rss_bytes()))
        self.after = {
            "process_io": _proc_io(),
            "cgroup": _cgroup_snapshot(),
            "storage": _tree_footprint(self.storage_root),
            "rusage": resource.getrusage(resource.RUSAGE_SELF),
            "rss_bytes": _proc_rss_bytes(),
        }

        wall = (wall_end_ns - self.wall_start_ns) / 1e9
        cpu = (cpu_end_ns - self.cpu_start_ns) / 1e9
        rss_values = [sample[2] for sample in self.samples if sample[2] is not None]
        sampled_cores: list[float] = []
        for previous, current in zip(self.samples, self.samples[1:]):
            elapsed = current[0] - previous[0]
            if elapsed > 0:
                sampled_cores.append((current[1] - previous[1]) / elapsed)

        before_usage = self.before["rusage"]
        after_usage = self.after["rusage"]
        storage_before = self.before["storage"]
        storage_after = self.after["storage"]
        rusage_delta = {
            "user_seconds": after_usage.ru_utime - before_usage.ru_utime,
            "system_seconds": after_usage.ru_stime - before_usage.ru_stime,
            "minor_faults": after_usage.ru_minflt - before_usage.ru_minflt,
            "major_faults": after_usage.ru_majflt - before_usage.ru_majflt,
            "block_inputs": after_usage.ru_inblock - before_usage.ru_inblock,
            "block_outputs": after_usage.ru_oublock - before_usage.ru_oublock,
            "voluntary_context_switches": after_usage.ru_nvcsw - before_usage.ru_nvcsw,
            "involuntary_context_switches": after_usage.ru_nivcsw - before_usage.ru_nivcsw,
        }
        return {
            "wall_seconds": wall,
            "cpu_seconds": cpu,
            "mean_cpu_cores": cpu / wall if wall > 0 else None,
            "sampled_cpu_cores": {
                "samples": len(sampled_cores),
                "min": min(sampled_cores) if sampled_cores else None,
                "mean": statistics.fmean(sampled_cores) if sampled_cores else None,
                "max": max(sampled_cores) if sampled_cores else None,
                "p95": _percentile(sampled_cores, 0.95),
            },
            "rss": {
                "samples": len(rss_values),
                "before_bytes": self.before["rss_bytes"],
                "after_bytes": self.after["rss_bytes"],
                "min_bytes": min(rss_values) if rss_values else None,
                "mean_bytes": statistics.fmean(rss_values) if rss_values else None,
                "max_bytes": max(rss_values) if rss_values else None,
                "delta_bytes": (
                    self.after["rss_bytes"] - self.before["rss_bytes"]
                    if isinstance(self.before["rss_bytes"], int)
                    and isinstance(self.after["rss_bytes"], int)
                    else None
                ),
                "process_high_water_before_kib": before_usage.ru_maxrss,
                "process_high_water_after_kib": after_usage.ru_maxrss,
            },
            "rusage_delta": rusage_delta,
            "process_io_before": self.before["process_io"],
            "process_io_after": self.after["process_io"],
            "process_io_delta": _numeric_delta(
                self.after["process_io"], self.before["process_io"],
            ),
            "cgroup_before": self.before["cgroup"],
            "cgroup_after": self.after["cgroup"],
            "cgroup_delta": _cgroup_delta(
                self.before["cgroup"], self.after["cgroup"],
            ),
            "storage_before": storage_before,
            "storage_after": storage_after,
            "storage_delta": {
                "files": storage_after["files"] - storage_before["files"],
                "bytes": storage_after["bytes"] - storage_before["bytes"],
            },
        }


class CapturedMonitoringWriter:
    """Context-manager compatible sink that stores production payloads."""

    records: list[dict[str, Any]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.context = {"args": list(args), "kwargs": kwargs}

    def __enter__(self) -> "CapturedMonitoringWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def log_metric(self, payload: Mapping[str, Any]) -> None:
        type(self).records.append({
            "context": copy.deepcopy(self.context),
            "payload": copy.deepcopy(dict(payload)),
        })

    def request_flush(self) -> None:
        return None


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    cleaned = [
        float(value) for value in values
        if isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    if not cleaned:
        return {"samples": 0}
    mean = statistics.fmean(cleaned)
    stddev = statistics.pstdev(cleaned)
    return {
        "samples": len(cleaned),
        "min": min(cleaned),
        "mean": mean,
        "median": statistics.median(cleaned),
        "max": max(cleaned),
        "p95": _percentile(cleaned, 0.95),
        "stddev": stddev,
        "cv": stddev / mean if mean != 0 else None,
    }


def _sanitize_json(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else repr(value)
    if isinstance(value, Mapping):
        return {str(key): _sanitize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_json(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _sanitize_json(value.item())
        except Exception:
            pass
    return str(value)


def _dependency_versions(names: Sequence[str]) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for name in names:
        try:
            result[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            result[name] = None
    return result


def _git_revision(module_path: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(module_path.parent), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return completed.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def _configure_hermetic_environment(home: Path, package_root: Path | None) -> None:
    home.mkdir(parents=True, exist_ok=True)
    if package_root is not None:
        resolved = str(package_root.resolve())
        sys.path[:] = [entry for entry in sys.path if entry != resolved]
        sys.path.insert(0, resolved)
    try:
        import dotenv

        dotenv.find_dotenv = lambda *args, **kwargs: ""  # type: ignore[assignment]
        dotenv.load_dotenv = lambda *args, **kwargs: False  # type: ignore[assignment]
    except Exception:
        pass
    environment = {
        "SUPERTABLE_HOME": str(home.resolve()),
        "STORAGE_TYPE": "LOCAL",
        "STORAGE_ENDPOINT_URL": "",
        "STORAGE_BUCKET": "supertable",
        "STORAGE_REGION": "us-east-1",
        "STORAGE_ACCESS_KEY": "",
        "STORAGE_SECRET_KEY": "",
        "STORAGE_SESSION_TOKEN": "",
        "STORAGE_FORCE_PATH_STYLE": "true",
        "STORAGE_USE_SSL": "false",
        "SUPERTABLE_DUCKDB_PRESIGNED": "0",
        "SUPERTABLE_DUCKDB_USE_HTTPFS": "0",
        "SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD": "0",
        "SUPERTABLE_REDIS_URL": "",
        "SUPERTABLE_REDIS_SENTINEL": "false",
        "SUPERTABLE_REDIS_SENTINELS": "",
        "SUPERTABLE_REDIS_HOST": "localhost",
        "SUPERTABLE_REDIS_PORT": "6379",
        "SUPERTABLE_REDIS_DB": "0",
        "SUPERTABLE_REDIS_PASSWORD": "",
        "SUPERTABLE_REDIS_USERNAME": "",
        "SUPERTABLE_REDIS_SSL": "false",
        "SUPERTABLE_ORGANIZATION": "",
        "SUPERTABLE_MONITORING_ENABLED": "false",
        "SUPERTABLE_LOG_LEVEL": "WARNING",
        "SUPERTABLE_MAX_LIMIT": "10000000",
        "LOCKING_BACKEND": "redis",
    }
    os.environ.update(environment)
    # Releases before 2.4.1 root LocalStorage at the process CWD.  Newer
    # releases root it explicitly at SUPERTABLE_HOME; chdir makes both agree.
    os.chdir(home)


def _install_runtime(package_root: Path | None, home: Path) -> dict[str, Any]:
    _configure_hermetic_environment(home, package_root)
    try:
        import fakeredis
        import lupa  # noqa: F401 - proves fakeredis can execute catalog Lua
    except Exception as exc:
        raise RuntimeError(
            "fakeredis and lupa are required for the hermetic write benchmark"
        ) from exc

    import supertable
    import supertable.data_reader as reader_module
    import supertable.data_writer as writer_module
    import supertable.processing as processing_module
    import supertable.redis_connector as redis_connector
    import supertable.super_table as super_table_module
    from supertable.engine.engine_enum import Engine

    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    cache = getattr(redis_connector, "_CLIENT_CACHE", None)
    if hasattr(cache, "clear"):
        cache.clear()
    redis_connector.create_redis_client = lambda options=None: fake

    # Explicit benchmark-only authorization boundary.  This avoids both an
    # external RBAC estate and version-specific privileged activation data.
    writer_module.check_write_access = lambda **kwargs: None
    if hasattr(writer_module, "check_create_access"):
        writer_module.check_create_access = lambda **kwargs: None
    reader_module.restrict_read_access = lambda **kwargs: {}
    super_table_module.RoleManager = lambda *args, **kwargs: None
    super_table_module.UserManager = lambda *args, **kwargs: None

    CapturedMonitoringWriter.records = []
    monitor_capture = False
    if hasattr(writer_module, "MonitoringWriter"):
        writer_module.MonitoringWriter = CapturedMonitoringWriter
        monitor_capture = True
    if hasattr(reader_module, "MonitoringWriter"):
        reader_module.MonitoringWriter = CapturedMonitoringWriter

    processing_module._storage = None
    engine_member = None
    for candidate in ("DUCKDB", "DUCKDB_PRO", "DUCKDB_LITE"):
        if hasattr(Engine, candidate):
            engine_member = candidate
            break
    if engine_member is None:
        raise RuntimeError("this SuperTable version exposes no DuckDB engine")

    writer_signature = inspect.signature(writer_module.DataWriter.write)
    reader_signature = inspect.signature(reader_module.DataReader.__init__)
    module_path = Path(supertable.__file__).resolve()
    return {
        "fake_redis": fake,
        "supertable": supertable,
        "writer_module": writer_module,
        "reader_module": reader_module,
        "DataWriter": writer_module.DataWriter,
        "DataReader": reader_module.DataReader,
        "engine": getattr(Engine, engine_member),
        "features": {
            "duckdb_engine_member": engine_member,
            "writer_signature": str(writer_signature),
            "reader_signature": str(reader_signature),
            "writer_has_compact": hasattr(writer_module.DataWriter, "compact"),
            "reader_bounded_aggregate_flag": (
                "_allow_bounded_collection_aggregates" in reader_signature.parameters
            ),
            "create_access_hook": hasattr(writer_module, "check_create_access"),
            "production_monitor_payload_capture": monitor_capture,
        },
        "provenance": {
            "label": None,
            "python": platform.python_version(),
            "python_executable": sys.executable,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "cpu_affinity": (
                sorted(os.sched_getaffinity(0))
                if hasattr(os, "sched_getaffinity") else None
            ),
            "supertable_version": getattr(supertable, "__version__", None),
            "supertable_module_path": str(module_path),
            "git_revision": _git_revision(module_path),
            "dependencies": _dependency_versions((
                "supertable", "pyarrow", "polars", "pandas", "duckdb",
                "redis", "fakeredis", "lupa",
            )),
        },
    }


def _arrow_for_operation(operation: Mapping[str, Any]) -> Any:
    import pyarrow as pa

    rows = operation["rows"]
    if operation["kind"] == "delete":
        return pa.table({
            "id": pa.array([row[0] for row in rows], type=pa.int64()),
        })
    return pa.table({
        "id": pa.array([row[0] for row in rows], type=pa.int64()),
        "value": pa.array([row[1] for row in rows], type=pa.int64()),
        "category": pa.array([row[2] for row in rows], type=pa.string()),
    })


def _result_sequence(value: Any) -> list[Any] | None:
    if isinstance(value, tuple):
        return [_sanitize_json(item) for item in value]
    if isinstance(value, list):
        return [_sanitize_json(item) for item in value]
    return None


def _execute_reader(runtime: Mapping[str, Any], super_name: str, query: str) -> Any:
    kwargs: dict[str, Any] = {
        "super_name": super_name,
        "organization": "benchmark_org",
        "query": query,
    }
    if runtime["features"]["reader_bounded_aggregate_flag"]:
        kwargs["_allow_bounded_collection_aggregates"] = True
    reader = runtime["DataReader"](**kwargs)
    frame, status, message = reader.execute(
        role_name="benchmark_role", engine=runtime["engine"],
    )
    status_value = getattr(status, "value", str(status))
    if str(status_value).casefold() not in {"ok", "status.ok"}:
        raise RuntimeError(f"DataReader failed: status={status_value!r}, message={message!r}")
    return frame


def _scalar_result(frame: Any) -> dict[str, Any]:
    if len(frame.index) != 1:
        raise RuntimeError(f"aggregate read returned {len(frame.index)} rows")
    row = frame.iloc[0]
    integer_columns = (
        "row_count", "value_non_null_count", "value_sum", "value_min", "value_max",
    )
    result: dict[str, Any] = {}
    for column in integer_columns:
        value = row[column]
        parsed = int(value)
        if not bool(value == parsed):
            raise RuntimeError(f"aggregate {column} is not an exact integer")
        result[column] = parsed
    result["value_null_count"] = result["row_count"] - result["value_non_null_count"]
    result["value_avg"] = float(row["value_avg"])
    result["value_avg_hex"] = result["value_avg"].hex()
    return result


def _actual_records_digest(frame: Any) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for row in frame.itertuples(index=False, name=None):
        if len(row) != 3:
            raise RuntimeError("record read returned an unexpected column count")
        canonical = [int(row[0]), int(row[1]), str(row[2])]
        digest.update(_canonical_json_bytes(canonical))
        digest.update(b"\n")
        count += 1
    return digest.hexdigest(), count


def _correctness(expected: Mapping[str, Any], actual: Mapping[str, Any], *,
                 expected_digest: str, actual_digest: str,
                 actual_record_count: int) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    for key in (
        "row_count", "value_non_null_count", "value_null_count", "value_sum",
        "value_min", "value_max", "value_avg_hex",
    ):
        if actual.get(key) != expected.get(key):
            mismatches.append({
                "field": key,
                "expected": expected.get(key),
                "actual": actual.get(key),
            })
    if actual_record_count != expected["row_count"]:
        mismatches.append({
            "field": "record_read_count",
            "expected": expected["row_count"],
            "actual": actual_record_count,
        })
    if actual_digest != expected_digest:
        mismatches.append({
            "field": "records_digest",
            "expected": expected_digest,
            "actual": actual_digest,
        })
    return {"oracle_match": not mismatches, "mismatches": mismatches}


def _series_summary(operations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = {
        "wall_seconds": [op["telemetry"]["wall_seconds"] for op in operations],
        "cpu_seconds": [op["telemetry"]["cpu_seconds"] for op in operations],
        "mean_cpu_cores": [op["telemetry"]["mean_cpu_cores"] for op in operations],
        "rss_peak_bytes": [op["telemetry"]["rss"]["max_bytes"] for op in operations],
        "storage_bytes_delta": [op["telemetry"]["storage_delta"]["bytes"] for op in operations],
        "read_bytes": [op["telemetry"]["process_io_delta"].get("read_bytes", 0) for op in operations],
        "write_bytes": [op["telemetry"]["process_io_delta"].get("write_bytes", 0) for op in operations],
    }
    result: dict[str, Any] = {
        "operations": len(operations),
        "all": {name: _distribution(values) for name, values in metrics.items()},
        "by_kind": {},
    }
    kinds = sorted({str(op["kind"]) for op in operations})
    for kind in kinds:
        selected = [op for op in operations if op["kind"] == kind]
        result["by_kind"][kind] = {
            name: _distribution([
                metrics[name][index]
                for index, operation in enumerate(operations)
                if operation["kind"] == kind
            ])
            for name in metrics
        }

    production: dict[str, list[float]] = {}
    counts: dict[str, float] = {}
    for operation in operations:
        payload = operation.get("production_monitor", {}).get("payload", {})
        for key, value in (payload.get("timings", {}) or {}).items():
            if isinstance(value, (int, float)):
                production.setdefault(str(key), []).append(float(value))
        for key, value in (payload.get("counts", {}) or {}).items():
            if isinstance(value, (int, float)):
                counts[str(key)] = counts.get(str(key), 0.0) + float(value)
    result["production_profiler"] = {
        "timings": {
            key: {**_distribution(values), "sum": sum(values)}
            for key, values in sorted(production.items())
        },
        "counter_totals": dict(sorted(counts.items())),
    }
    return result


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    work_root = Path(args.work_root).resolve()
    if work_root.exists() and any(work_root.iterdir()):
        raise ValueError(f"work root must be absent or empty: {work_root}")
    work_root.mkdir(parents=True, exist_ok=True)
    home = work_root / "home"
    trace = build_trace(
        seed=args.seed,
        initial_rows=args.initial_rows,
        operations=args.operations,
        batch_rows=args.batch_rows,
    )
    runtime = _install_runtime(
        Path(args.package_root).resolve() if args.package_root else None,
        home,
    )
    runtime["provenance"]["label"] = args.label
    runtime["provenance"]["requested_revision"] = args.revision
    try:
        script_path = Path(__file__).resolve()
        runtime["provenance"]["worker_script_sha256"] = hashlib.sha256(
            script_path.read_bytes()
        ).hexdigest()
    except OSError:
        runtime["provenance"]["worker_script_sha256"] = None

    writer = runtime["DataWriter"](
        super_name="write_benchmark", organization="benchmark_org",
    )
    operation_results: list[dict[str, Any]] = []
    for operation in trace["steps"]:
        arrow = _arrow_for_operation(operation)
        monitor_start = len(CapturedMonitoringWriter.records)
        telemetry = OperationTelemetry(home, args.sample_interval_ms)
        telemetry.start()
        try:
            result = writer.write(
                role_name="benchmark_role",
                simple_name="records",
                data=arrow,
                overwrite_columns=["id"] if operation["kind"] in {"upsert", "delete"} else [],
                compression_level=args.compression_level,
                delete_only=operation["kind"] == "delete",
                lineage={
                    "source_type": "benchmark",
                    "run_label": args.label,
                    "trace_digest": trace["trace_digest"],
                    "operation_index": operation["index"],
                },
            )
        finally:
            measured = telemetry.finish()
        captured = CapturedMonitoringWriter.records[monitor_start:]
        production_monitor = captured[-1] if captured else None
        operation_results.append({
            "index": operation["index"],
            "kind": operation["kind"],
            "input_rows": len(operation["rows"]),
            "input_digest": operation["input_digest"],
            "expected_rows_after": operation["expected_rows_after"],
            "writer_result": _sanitize_json(result),
            "telemetry": measured,
            "production_monitor": _sanitize_json(production_monitor),
        })

    aggregate_sql = (
        "SELECT COUNT(*) AS row_count, COUNT(value) AS value_non_null_count, "
        "SUM(value) AS value_sum, AVG(value) AS value_avg, "
        "MIN(value) AS value_min, MAX(value) AS value_max FROM records"
    )
    aggregate_frame = _execute_reader(runtime, "write_benchmark", aggregate_sql)
    actual = _scalar_result(aggregate_frame)
    record_limit = trace["expected"]["row_count"] + 1
    records_frame = _execute_reader(
        runtime,
        "write_benchmark",
        f"SELECT id, value, category FROM records ORDER BY id LIMIT {record_limit}",
    )
    actual_digest, actual_record_count = _actual_records_digest(records_frame)
    correctness = _correctness(
        trace["expected"], actual,
        expected_digest=trace["expected_records_digest"],
        actual_digest=actual_digest,
        actual_record_count=actual_record_count,
    )

    report = {
        "schema": REPORT_SCHEMA,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "configuration": {
            "seed": args.seed,
            "initial_rows": args.initial_rows,
            "operations": args.operations,
            "batch_rows": args.batch_rows,
            "compression_level": args.compression_level,
            "sample_interval_ms": args.sample_interval_ms,
            "organization": "benchmark_org",
            "super_name": "write_benchmark",
            "simple_name": "records",
            "storage": "LocalStorage",
            "redis": "fakeredis-lua",
            "authorization": "benchmark-only allow-all hooks",
        },
        "provenance": runtime["provenance"],
        "features": runtime["features"],
        "trace": {
            "digest": trace["trace_digest"],
            "step_count": len(trace["steps"]),
            "kinds": [operation["kind"] for operation in trace["steps"]],
        },
        "operations": operation_results,
        "summary": _series_summary(operation_results),
        "final_read": {
            "engine_member": runtime["features"]["duckdb_engine_member"],
            "aggregate_sql": aggregate_sql,
            "expected": trace["expected"],
            "actual": actual,
            "expected_records_digest": trace["expected_records_digest"],
            "actual_records_digest": actual_digest,
            "actual_record_count": actual_record_count,
            "correctness": correctness,
        },
        "final_storage": _tree_footprint(home),
        "final_cgroup": _cgroup_snapshot(),
    }
    return _sanitize_json(report)


def _ratio(candidate: float | int | None, baseline: float | int | None) -> dict[str, Any]:
    if not isinstance(candidate, (int, float)) or not isinstance(baseline, (int, float)):
        return {"baseline": baseline, "candidate": candidate}
    return {
        "baseline": baseline,
        "candidate": candidate,
        "candidate_over_baseline": candidate / baseline if baseline != 0 else None,
        "baseline_over_candidate": baseline / candidate if candidate != 0 else None,
        "percent_change": (
            (candidate - baseline) * 100.0 / baseline if baseline != 0 else None
        ),
    }


def compare_reports(baseline: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if baseline.get("schema") != REPORT_SCHEMA:
        blockers.append("baseline report schema is unsupported")
    if candidate.get("schema") != REPORT_SCHEMA:
        blockers.append("candidate report schema is unsupported")
    baseline_trace = baseline.get("trace", {})
    candidate_trace = candidate.get("trace", {})
    if baseline_trace.get("digest") != candidate_trace.get("digest"):
        blockers.append("baseline and candidate trace digests differ")

    for label, report in (("baseline", baseline), ("candidate", candidate)):
        correctness = report.get("final_read", {}).get("correctness", {})
        if correctness.get("oracle_match") is not True:
            blockers.append(
                f"{label} disagrees with the independent final-state oracle: "
                f"{correctness.get('mismatches', [])!r}"
            )
    baseline_digest = baseline.get("final_read", {}).get("actual_records_digest")
    candidate_digest = candidate.get("final_read", {}).get("actual_records_digest")
    if baseline_digest != candidate_digest:
        blockers.append("baseline and candidate final record digests differ")

    baseline_ops = list(baseline.get("operations", []))
    candidate_ops = list(candidate.get("operations", []))
    if len(baseline_ops) != len(candidate_ops):
        blockers.append("baseline and candidate operation counts differ")

    operation_comparison: list[dict[str, Any]] = []
    for old, new in zip(baseline_ops, candidate_ops):
        identity_old = (old.get("index"), old.get("kind"), old.get("input_digest"))
        identity_new = (new.get("index"), new.get("kind"), new.get("input_digest"))
        if identity_old != identity_new:
            blockers.append(
                f"operation identity differs at pair {len(operation_comparison)}"
            )
        old_telemetry = old.get("telemetry", {})
        new_telemetry = new.get("telemetry", {})
        operation_comparison.append({
            "index": old.get("index"),
            "kind": old.get("kind"),
            "wall_seconds": _ratio(
                new_telemetry.get("wall_seconds"), old_telemetry.get("wall_seconds"),
            ),
            "cpu_seconds": _ratio(
                new_telemetry.get("cpu_seconds"), old_telemetry.get("cpu_seconds"),
            ),
            "mean_cpu_cores": _ratio(
                new_telemetry.get("mean_cpu_cores"), old_telemetry.get("mean_cpu_cores"),
            ),
            "rss_peak_bytes": _ratio(
                new_telemetry.get("rss", {}).get("max_bytes"),
                old_telemetry.get("rss", {}).get("max_bytes"),
            ),
            "process_read_bytes": _ratio(
                new_telemetry.get("process_io_delta", {}).get("read_bytes"),
                old_telemetry.get("process_io_delta", {}).get("read_bytes"),
            ),
            "process_write_bytes": _ratio(
                new_telemetry.get("process_io_delta", {}).get("write_bytes"),
                old_telemetry.get("process_io_delta", {}).get("write_bytes"),
            ),
        })

    summary_comparison: dict[str, Any] = {}
    for metric in (
        "wall_seconds", "cpu_seconds", "mean_cpu_cores", "rss_peak_bytes",
        "storage_bytes_delta", "read_bytes", "write_bytes",
    ):
        old = baseline.get("summary", {}).get("all", {}).get(metric, {})
        new = candidate.get("summary", {}).get("all", {}).get(metric, {})
        summary_comparison[metric] = {
            statistic: _ratio(new.get(statistic), old.get(statistic))
            for statistic in ("min", "mean", "median", "max", "p95", "stddev", "cv")
        }

    old_timings = (
        baseline.get("summary", {}).get("production_profiler", {}).get("timings", {})
    )
    new_timings = (
        candidate.get("summary", {}).get("production_profiler", {}).get("timings", {})
    )
    profiler_comparison = {
        key: {
            statistic: _ratio(
                new_timings.get(key, {}).get(statistic),
                old_timings.get(key, {}).get(statistic),
            )
            for statistic in ("sum", "mean", "median", "max", "p95")
        }
        for key in sorted(set(old_timings) | set(new_timings))
    }

    return {
        "schema": COMPARISON_SCHEMA,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "baseline": baseline.get("provenance", {}),
        "candidate": candidate.get("provenance", {}),
        "trace_digest": baseline_trace.get("digest"),
        "gate_passed": not blockers,
        "blockers": blockers,
        "final_read": {
            "baseline": baseline.get("final_read", {}),
            "candidate": candidate.get("final_read", {}),
        },
        "operations": operation_comparison,
        "summary": summary_comparison,
        "production_profiler": profiler_comparison,
    }


def _write_json(path: str | Path, value: Mapping[str, Any]) -> None:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="execute one version's write trace")
    run.add_argument("--output", required=True)
    run.add_argument("--work-root", required=True)
    run.add_argument("--package-root")
    run.add_argument("--label", required=True)
    run.add_argument("--revision")
    run.add_argument("--seed", type=int, default=20260818)
    run.add_argument("--initial-rows", type=int, default=50_000)
    run.add_argument("--operations", type=int, default=30)
    run.add_argument("--batch-rows", type=int, default=1_000)
    run.add_argument("--compression-level", type=int, default=1)
    run.add_argument("--sample-interval-ms", type=float, default=5.0)

    compare = subparsers.add_parser("compare", help="compare two completed reports")
    compare.add_argument("--baseline", required=True)
    compare.add_argument("--candidate", required=True)
    compare.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "run":
            report = run_benchmark(args)
            _write_json(args.output, report)
            print(json.dumps({
                "output": str(Path(args.output).resolve()),
                "trace_digest": report["trace"]["digest"],
                "oracle_match": report["final_read"]["correctness"]["oracle_match"],
            }, sort_keys=True))
            return 0 if report["final_read"]["correctness"]["oracle_match"] else 3

        baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        candidate = json.loads(Path(args.candidate).read_text(encoding="utf-8"))
        comparison = compare_reports(baseline, candidate)
        _write_json(args.output, comparison)
        print(json.dumps({
            "output": str(Path(args.output).resolve()),
            "gate_passed": comparison["gate_passed"],
            "blockers": comparison["blockers"],
        }, sort_keys=True))
        return 0 if comparison["gate_passed"] else 3
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
