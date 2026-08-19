"""Bounded streaming worker for the two-engine real-spill benchmark.

The normal benchmark worker materializes DuckDB results as pandas frames and
collects IslandDB streams into a small Arrow table.  Neither is a valid result
contract for the real-spill workload: its ordered result is itself about
10 GiB.  This benchmark-only worker consumes both engines incrementally and
emits only fixed-size cryptographic evidence.

The digest is deliberately independent of engine batch boundaries.  Every
column is cast to a sealed Arrow physical type and hashed as one logical value
buffer.  The generated corpus is non-null and its Binary payloads have an
exact fixed width, so concatenating those buffers is unambiguous.  A second
streaming check proves strict ``(metric, id)`` order, with ``id`` as the unique
tie-break.  No result row or result batch survives beyond the next batch.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import struct
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from .runner import (
    ENGINE_DUCKDB,
    ENGINE_ISLAND,
    _PeakRSS,
    _build_reflection,
    _cgroup_v2_memory_telemetry,
    _counter_delta,
    _drop_os_cache_best_effort,
    _flatten_plan_stats,
    _proc_io_counters,
    _profile_metrics,
)


DIGEST_FORMAT_VERSION = 1
ORDER_KEYS = ("metric", "id")
GIB = 1024**3
MIB = 1024**2
DEFAULT_SPILL_CAP_BYTES = 28 * GIB


class RealSpillWorkerError(RuntimeError):
    """Raised when a worker cannot prove the sealed streaming contract."""


@dataclass(frozen=True)
class ResultColumn:
    name: str
    arrow_type: pa.DataType
    type_name: str
    fixed_value_bytes: int
    nullable: bool = False


def _strict_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _arrow_type(name: str) -> pa.DataType:
    normalized = str(name).strip().lower()
    types = {
        "int32": pa.int32(),
        "int64": pa.int64(),
        "timestamp[us]": pa.timestamp("us"),
        "binary": pa.binary(),
    }
    try:
        return types[normalized]
    except KeyError as exc:
        raise RealSpillWorkerError(
            f"unsupported result contract Arrow type {name!r}"
        ) from exc


def parse_result_contract(plan: Mapping[str, Any]) -> tuple[ResultColumn, ...]:
    raw = plan.get("result_schema")
    if not isinstance(raw, list) or not raw:
        raise RealSpillWorkerError("real-spill plan has no result_schema")
    columns: list[ResultColumn] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise RealSpillWorkerError("result_schema entry is not an object")
        name = str(item.get("name") or "")
        type_name = str(item.get("type") or "").lower()
        fixed = int(item.get("fixed_value_bytes") or 0)
        nullable = bool(item.get("nullable", False))
        if not name or fixed <= 0:
            raise RealSpillWorkerError("result_schema has an invalid field")
        arrow_type = _arrow_type(type_name)
        expected_fixed = (
            4 if pa.types.is_int32(arrow_type)
            else 8 if (
                pa.types.is_int64(arrow_type)
                or pa.types.is_timestamp(arrow_type)
            )
            else fixed
        )
        if fixed != expected_fixed:
            raise RealSpillWorkerError(
                f"result field {name!r} has inconsistent fixed width"
            )
        columns.append(ResultColumn(
            name=name,
            arrow_type=arrow_type,
            type_name=type_name,
            fixed_value_bytes=fixed,
            nullable=nullable,
        ))
    names = [column.name for column in columns]
    if len(set(names)) != len(names):
        raise RealSpillWorkerError("result_schema field names must be unique")
    if not all(key in names for key in ORDER_KEYS):
        raise RealSpillWorkerError("result_schema is missing metric/id order keys")
    if any(column.nullable for column in columns):
        raise RealSpillWorkerError(
            "real-spill corpus contract requires every result field non-null"
        )
    return tuple(columns)


def _logical_value_buffer(array: pa.Array, column: ResultColumn) -> memoryview:
    """Return only this array slice's canonical fixed-width value bytes."""
    if array.null_count:
        raise RealSpillWorkerError(
            f"result column {column.name!r} unexpectedly contains NULL"
        )
    if pa.types.is_binary(column.arrow_type):
        offsets = array.buffers()[1]
        values = array.buffers()[2]
        if offsets is None:
            if len(array):
                raise RealSpillWorkerError(
                    f"Binary column {column.name!r} has no offset buffer"
                )
            return memoryview(b"")
        offset_view = memoryview(offsets)
        first = struct.unpack_from("<i", offset_view, array.offset * 4)[0]
        last = struct.unpack_from(
            "<i", offset_view, (array.offset + len(array)) * 4,
        )[0]
        expected = len(array) * column.fixed_value_bytes
        if first < 0 or last < first or last - first != expected:
            raise RealSpillWorkerError(
                f"Binary column {column.name!r} violated its fixed "
                f"{column.fixed_value_bytes}-byte value contract"
            )
        if values is None:
            if expected:
                raise RealSpillWorkerError(
                    f"Binary column {column.name!r} has no value buffer"
                )
            return memoryview(b"")
        return memoryview(values)[first:last]

    values = array.buffers()[1]
    expected = len(array) * column.fixed_value_bytes
    if values is None:
        if expected:
            raise RealSpillWorkerError(
                f"fixed column {column.name!r} has no value buffer"
            )
        return memoryview(b"")
    first = array.offset * column.fixed_value_bytes
    return memoryview(values)[first:first + expected]


class StreamingResultDigest:
    """Batch-boundary-independent digest and order validator."""

    def __init__(self, columns: Sequence[ResultColumn]):
        self.columns = tuple(columns)
        self._hashers: dict[str, Any] = {}
        for column in self.columns:
            hasher = hashlib.sha256()
            hasher.update(_strict_json({
                "format_version": DIGEST_FORMAT_VERSION,
                "name": column.name,
                "type": column.type_name,
                "fixed_value_bytes": column.fixed_value_bytes,
                "nullable": column.nullable,
            }))
            hasher.update(b"\0")
            self._hashers[column.name] = hasher
        self.row_count = 0
        self.logical_value_bytes = 0
        self._last_order_key: tuple[int, int] | None = None

    def _normalize(self, batch: pa.RecordBatch) -> list[pa.Array]:
        expected_names = [column.name for column in self.columns]
        if batch.schema.names != expected_names:
            raise RealSpillWorkerError(
                f"result columns changed: {batch.schema.names!r} != "
                f"{expected_names!r}"
            )
        normalized: list[pa.Array] = []
        for index, column in enumerate(self.columns):
            array = batch.column(index)
            try:
                if not array.type.equals(column.arrow_type):
                    array = pc.cast(array, column.arrow_type, safe=True)
            except (pa.ArrowException, TypeError, ValueError) as exc:
                raise RealSpillWorkerError(
                    f"cannot normalize result column {column.name!r} from "
                    f"{array.type} to {column.arrow_type}: {exc}"
                ) from exc
            normalized.append(array)
        return normalized

    def _validate_order(self, arrays: Sequence[pa.Array]) -> None:
        if not arrays or not len(arrays[0]):
            return
        by_name = {
            column.name: arrays[index]
            for index, column in enumerate(self.columns)
        }
        metric = by_name["metric"].to_numpy(zero_copy_only=False)
        row_id = by_name["id"].to_numpy(zero_copy_only=False)
        first = (int(metric[0]), int(row_id[0]))
        if self._last_order_key is not None and first <= self._last_order_key:
            raise RealSpillWorkerError(
                "result is not strictly ordered by (metric, id) across batches"
            )
        if len(metric) > 1:
            import numpy as np

            valid = (metric[1:] > metric[:-1]) | (
                (metric[1:] == metric[:-1]) & (row_id[1:] > row_id[:-1])
            )
            if not bool(np.all(valid)):
                raise RealSpillWorkerError(
                    "result is not strictly ordered by (metric, id)"
                )
        self._last_order_key = (int(metric[-1]), int(row_id[-1]))

    def update(self, batch: pa.RecordBatch) -> None:
        if not isinstance(batch, pa.RecordBatch):
            raise RealSpillWorkerError(
                f"result yielded {type(batch).__name__}, expected RecordBatch"
            )
        arrays = self._normalize(batch)
        self._validate_order(arrays)
        for column, array in zip(self.columns, arrays):
            values = _logical_value_buffer(array, column)
            self._hashers[column.name].update(values)
            self.logical_value_bytes += len(values)
        self.row_count += batch.num_rows

    def finish(self) -> dict[str, Any]:
        column_digests = {
            column.name: self._hashers[column.name].hexdigest()
            for column in self.columns
        }
        schema = [
            {
                "name": column.name,
                "type": column.type_name,
                "fixed_value_bytes": column.fixed_value_bytes,
                "nullable": column.nullable,
            }
            for column in self.columns
        ]
        proof = {
            "format_version": DIGEST_FORMAT_VERSION,
            "schema": schema,
            "row_count": self.row_count,
            "logical_value_bytes": self.logical_value_bytes,
            "column_sha256": column_digests,
            "order": {
                "keys": list(ORDER_KEYS),
                "strictly_monotonic": True,
            },
        }
        proof["digest"] = hashlib.sha256(_strict_json(proof)).hexdigest()
        return proof


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duckdb_size(value: int) -> str:
    if value <= 0:
        raise RealSpillWorkerError("DuckDB byte limit must be positive")
    if value % GIB == 0:
        return f"{value // GIB}GiB"
    if value % MIB == 0:
        return f"{value // MIB}MiB"
    return f"{value}B"


def _duckdb_batches(
    plan: Mapping[str, Any],
    *,
    threads: int,
    memory_limit_bytes: int,
    spill_cap_bytes: int,
) -> tuple[Iterable[pa.RecordBatch], Any, Path]:
    """Start a raw DuckDB Arrow stream with an explicit bounded temp store."""
    import duckdb

    spill_root = Path("/bench/engine-spill")
    spill_root.mkdir(parents=True, exist_ok=True)
    profile_path = Path("/bench/profile.json")
    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads={threads}")
    connection.execute(
        f"SET memory_limit={_quote_literal(_duckdb_size(memory_limit_bytes))}"
    )
    connection.execute(f"SET temp_directory={_quote_literal(str(spill_root))}")
    connection.execute(
        "SET max_temp_directory_size="
        f"{_quote_literal(_duckdb_size(spill_cap_bytes))}"
    )
    connection.execute("SET enable_external_file_cache=false")
    connection.execute("SET preserve_insertion_order=false")

    files = [str(value) for value in plan.get("files") or ()]
    if not files:
        connection.close()
        raise RealSpillWorkerError("DuckDB real-spill plan has no files")
    file_sql = "[" + ",".join(_quote_literal(value) for value in files) + "]"
    table = str(plan.get("table") or "")
    connection.execute(
        f"CREATE TEMP VIEW {_quote_identifier(table)} AS "
        f"SELECT * FROM read_parquet({file_sql}, union_by_name=true)"
    )
    connection.execute("PRAGMA enable_profiling='json'")
    connection.execute(
        f"PRAGMA profile_output={_quote_literal(str(profile_path))}"
    )
    reader = connection.execute(str(plan["sql"])).fetch_record_batch(
        rows_per_batch=8 * 1024,
    )

    def batches():
        try:
            yield from reader
        finally:
            try:
                reader.close()
            except Exception:
                pass

    return batches(), connection, profile_path


def _island_batches(plan: Mapping[str, Any]):
    from supertable.engine.executor import Executor
    from supertable.engine.plan_stats import PlanStats
    from supertable.query_plan_manager import QueryPlanManager
    from supertable.storage.local_storage import LocalStorage
    from supertable.utils.sql_parser import SQLParser
    from supertable.utils.timer import Timer

    from .runner import _resolve_engine

    engine = _resolve_engine(ENGINE_ISLAND)
    parser = SQLParser(
        super_name=str(plan["super_name"]),
        query=str(plan["sql"]),
        dialect=engine.dialect,
    )
    reflection = _build_reflection(plan)
    query_manager = QueryPlanManager(
        super_name=str(plan["super_name"]),
        organization="islanddb-real-spill-benchmark",
        current_meta_path="benchmark://real-spill-sort",
        query=str(plan["sql"]),
    )
    timer = Timer(show_timing=False)
    stats = PlanStats()
    executor = Executor(
        storage=LocalStorage(), organization="islanddb-real-spill-benchmark",
    )
    executor._catalog = False
    stream, used = executor.execute_stream(
        engine=engine,
        reflection=reflection,
        parser=parser,
        query_manager=query_manager,
        timer=timer,
        plan_stats=stats,
        log_prefix="[islanddb.real-spill] ",
    )
    if str(used) != ENGINE_ISLAND:
        stream.close()
        raise RealSpillWorkerError(
            f"explicit IslandDB request executed unexpected engine {used!r}"
        )
    return stream, executor, stats, Path(query_manager.query_plan_path)


def run_worker(request: Mapping[str, Any]) -> dict[str, Any]:
    engine_name = str(request.get("engine") or "")
    if engine_name not in (ENGINE_DUCKDB, ENGINE_ISLAND):
        raise RealSpillWorkerError(f"unsupported explicit engine {engine_name!r}")
    plan = request.get("plan")
    if not isinstance(plan, Mapping) or plan.get("name") != "real_spill_sort":
        raise RealSpillWorkerError("worker accepts only real_spill_sort")
    if int(request.get("warm_repeats") or 0) != 0:
        raise RealSpillWorkerError("real-spill worker executes one cold query")
    configured_threads = int(request.get("threads") or 0)
    if configured_threads < 1 or configured_threads > 4:
        raise RealSpillWorkerError("real-spill worker threads must be in [1, 4]")
    contract = plan.get("real_spill_contract")
    if not isinstance(contract, Mapping):
        raise RealSpillWorkerError("real-spill plan has no sealed resource contract")
    memory_limit_bytes = int(request.get("memory_limit_bytes") or 0)
    if memory_limit_bytes < 64 * MIB or memory_limit_bytes > 2 * GIB:
        raise RealSpillWorkerError(
            "real-spill worker memory must be in [64 MiB, 2 GiB]"
        )
    if int(contract.get("engine_memory_bytes") or 0) != memory_limit_bytes:
        raise RealSpillWorkerError(
            "request memory differs from the sealed real-spill contract"
        )
    if int(contract.get("engine_threads") or configured_threads) != configured_threads:
        raise RealSpillWorkerError(
            "request threads differ from the sealed real-spill contract"
        )
    spill_cap_bytes = int(
        contract.get("spill_cap_bytes") or DEFAULT_SPILL_CAP_BYTES
    )
    if spill_cap_bytes < memory_limit_bytes or spill_cap_bytes > DEFAULT_SPILL_CAP_BYTES:
        raise RealSpillWorkerError("real-spill temp cap is outside the sealed bounds")

    columns = parse_result_contract(plan)
    cgroup_before = _cgroup_v2_memory_telemetry()
    cold_advice = _drop_os_cache_best_effort(plan.get("files") or ())
    process_io_before = _proc_io_counters()
    arrow_pool = pa.default_memory_pool()
    arrow_before = int(arrow_pool.bytes_allocated())
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    plan_stats: dict[str, Any] = {}
    profile_path: Path
    close_owner = None
    with _PeakRSS() as rss:
        digest = StreamingResultDigest(columns)
        if engine_name == ENGINE_DUCKDB:
            batches, connection, profile_path = _duckdb_batches(
                plan,
                threads=configured_threads,
                memory_limit_bytes=memory_limit_bytes,
                spill_cap_bytes=spill_cap_bytes,
            )
            close_owner = connection
            try:
                for batch in batches:
                    digest.update(batch)
            finally:
                # Exhaustion finalizes the DuckDB profile. Disabling profiling
                # must happen before connection close but after the Arrow reader.
                try:
                    connection.execute("PRAGMA disable_profiling")
                except Exception:
                    pass
        else:
            stream, executor, raw_stats, profile_path = _island_batches(plan)
            close_owner = executor
            with stream:
                for batch in stream:
                    digest.update(batch)
            plan_stats = _flatten_plan_stats(raw_stats.stats)
        result = digest.finish()
    wall_seconds = time.perf_counter() - wall_started
    cpu_seconds = time.process_time() - cpu_started
    process_io_after = _proc_io_counters()
    arrow_after = int(arrow_pool.bytes_allocated())
    profile = _profile_metrics(profile_path)
    try:
        profile_path.unlink()
    except OSError:
        pass
    if engine_name == ENGINE_DUCKDB and close_owner is not None:
        close_owner.close()

    expected_rows = int(plan.get("expected_result_rows") or 0)
    expected_bytes = int(plan.get("expected_result_value_bytes") or 0)
    if result["row_count"] != expected_rows:
        raise RealSpillWorkerError(
            f"stream returned {result['row_count']:,} rows, expected "
            f"{expected_rows:,}"
        )
    if result["logical_value_bytes"] != expected_bytes:
        raise RealSpillWorkerError(
            f"stream hashed {result['logical_value_bytes']:,} value bytes, "
            f"expected {expected_bytes:,}"
        )

    cgroup_after = _cgroup_v2_memory_telemetry()
    event_delta = _counter_delta(
        cgroup_before.get("memory_events"),
        cgroup_after.get("memory_events"),
    )
    if event_delta and any(
        int(event_delta.get(name) or 0)
        for name in ("oom", "oom_kill", "oom_group_kill")
    ):
        raise RealSpillWorkerError(f"container recorded OOM events: {event_delta}")

    return {
        "engine": engine_name,
        "result": result,
        "result_digest": result["digest"],
        "samples": [{
            "temperature": "cold",
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "mean_cpu_cores": (
                cpu_seconds / wall_seconds if wall_seconds > 0 else None
            ),
            "rss_baseline_bytes": rss.baseline,
            "rss_peak_bytes": rss.peak,
            "rss_peak_delta_bytes": rss.delta,
            "process_io_before": process_io_before,
            "process_io_after": process_io_after,
            "process_io_delta": _counter_delta(
                process_io_before, process_io_after,
            ),
            "arrow_bytes_before": arrow_before,
            "arrow_bytes_after": arrow_after,
            "arrow_peak_bytes_process": int(arrow_pool.max_memory()),
            "engine_profile": profile,
            "plan_stats": plan_stats,
            "result_digest": result["digest"],
            "result_rows": result["row_count"],
            "result_value_bytes": result["logical_value_bytes"],
        }],
        "execution_context": {
            "configured_threads": configured_threads,
            "configured_memory_limit_bytes": memory_limit_bytes,
            "configured_spill_cap_bytes": spill_cap_bytes,
            "cgroup_v2": cgroup_after,
            "cgroup_v2_before": cgroup_before,
            "cgroup_memory_event_delta": event_delta,
            "cold_advice": cold_advice,
            "python_pid": os.getpid(),
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "runtime": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "dependencies": {
                    name: importlib.metadata.version(name)
                    for name in (
                        "duckdb",
                        "numpy",
                        "pandas",
                        "polars",
                        "pyarrow",
                        "redis",
                        "sqlglot",
                        "supertable",
                    )
                },
            },
        },
    }


def worker_main(request_path: str | Path, response_path: str | Path) -> int:
    response_file = Path(response_path)
    try:
        request = json.loads(Path(request_path).read_text(encoding="utf-8"))
        response = {"ok": True, "result": run_worker(request)}
        code = 0
    except Exception as exc:  # noqa: BLE001 - preserve benchmark forensics
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


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("request")
    parser.add_argument("response")
    args = parser.parse_args(argv)
    return worker_main(args.request, args.response)


if __name__ == "__main__":
    raise SystemExit(main())
