"""Bounded, query-private spill primitives used by IslandDB.

The operators in this module have a deliberately small contract.  They never
fall back to an unbounded in-memory implementation: unsupported types, a
single oversized input batch, insufficient quota, and low disk space are hard
errors that the engine must route around.
"""

from __future__ import annotations

import heapq
import io
import math
import os
import shutil
import sys
import tempfile
import threading
import uuid
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from supertable.engine.island_resources import ArrowBatchStream


class IslandSpillError(RuntimeError):
    """Base class for an operation that cannot stay inside its spill bounds."""


class SpillBudgetExceeded(IslandSpillError):
    pass


class SpillDiskFull(IslandSpillError):
    pass


class SpillCancelled(IslandSpillError):
    pass


class SpillMemoryLimitExceeded(IslandSpillError):
    pass


class UnsupportedSpillOperation(IslandSpillError):
    pass


class _BudgetedFile(io.RawIOBase):
    """Sequential output whose every write is quota/free-space checked."""

    def __init__(self, session: "SpillSession", path: Path):
        super().__init__()
        self._session = session
        self.path = path
        self._file = open(path, "xb", buffering=0)
        self._position = 0

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._position

    def write(self, value) -> int:
        if self.closed:
            raise ValueError("write to closed spill file")
        view = memoryview(value)
        requested = len(view)
        self._session._account_write(requested)
        try:
            written = self._file.write(view)
        except BaseException:
            # A raw write may persist a prefix before raising. Keep that prefix
            # charged until write_ipc_batches removes the partial file;
            # otherwise cleanup subtracts it twice and undercounts older runs.
            try:
                actual_position = int(self._file.tell())
            except (OSError, ValueError):
                actual_position = self._position
            persisted = min(requested, max(0, actual_position - self._position))
            self._position += persisted
            self._session._release_bytes(requested - persisted)
            raise
        if written is None:
            written = requested
        if written < requested:
            self._session._release_bytes(requested - written)
        self._position += written
        return written

    def flush(self) -> None:
        if not self.closed:
            self._file.flush()

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._file.flush()
        finally:
            try:
                # RawIOBase.close() invokes ``flush`` before marking this
                # object closed, so the underlying file must remain open.
                super().close()
            finally:
                self._file.close()


class SpillSession:
    """Private per-query spill directory with a hard logical byte quota.

    The directory is mode 0700, is never shared between query IDs, and is
    recursively removed on every context exit.  Callers should also pass their
    cancellation event so long scans stop before allocating more disk.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        budget_bytes: int,
        min_free_bytes: int,
        query_id: str | None = None,
        cancel_event: threading.Event | None = None,
        disk_usage=shutil.disk_usage,
    ):
        if budget_bytes < 0 or min_free_bytes < 0:
            raise ValueError("spill budgets cannot be negative")
        self.root = Path(root)
        self.budget_bytes = int(budget_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.query_id = query_id or uuid.uuid4().hex
        self.cancel_event = cancel_event or threading.Event()
        self._disk_usage = disk_usage
        self._directory: Path | None = None
        self._used_bytes = 0
        self._peak_used_bytes = 0
        self._lock = threading.Lock()
        self._closed = False

    @property
    def directory(self) -> Path:
        if self._directory is None or self._closed:
            raise IslandSpillError("spill session is not active")
        return self._directory

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    @property
    def peak_used_bytes(self) -> int:
        """Largest simultaneously-accounted spill footprint for this query."""
        with self._lock:
            return self._peak_used_bytes

    def _check_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise SpillCancelled(f"spill query {self.query_id!r} was cancelled")

    def __enter__(self) -> "SpillSession":
        if self._directory is not None:
            raise IslandSpillError("spill session cannot be entered twice")
        self._check_cancelled()
        self.root.mkdir(parents=True, exist_ok=True)
        free = int(self._disk_usage(self.root).free)
        if free < self.min_free_bytes:
            raise SpillDiskFull(
                f"spill root has {free} free bytes, below reserve {self.min_free_bytes}"
            )
        safe_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in self.query_id)[:48]
        directory = Path(tempfile.mkdtemp(prefix=f"island-{safe_id}-", dir=self.root))
        os.chmod(directory, 0o700)
        self._directory = directory
        self._closed = False
        return self

    def _safe_path(self, relative_name: str) -> Path:
        if not relative_name or Path(relative_name).is_absolute():
            raise ValueError("spill filename must be a non-empty relative path")
        candidate = self.directory / relative_name
        resolved_parent = candidate.parent.resolve()
        if resolved_parent != self.directory.resolve():
            raise ValueError("nested or escaping spill paths are not allowed")
        return candidate

    def _account_write(self, size: int) -> None:
        if size < 0:
            raise ValueError("write size cannot be negative")
        self._check_cancelled()
        with self._lock:
            if self._used_bytes + size > self.budget_bytes:
                raise SpillBudgetExceeded(
                    f"spill quota {self.budget_bytes} bytes would be exceeded"
                )
            free = int(self._disk_usage(self.directory).free)
            if free - size < self.min_free_bytes:
                raise SpillDiskFull(
                    f"spill write would violate {self.min_free_bytes}-byte free-space reserve"
                )
            self._used_bytes += size
            self._peak_used_bytes = max(self._peak_used_bytes, self._used_bytes)

    def _release_bytes(self, size: int) -> None:
        with self._lock:
            self._used_bytes = max(0, self._used_bytes - max(0, size))

    def open_output(self, relative_name: str) -> _BudgetedFile:
        self._check_cancelled()
        return _BudgetedFile(self, self._safe_path(relative_name))

    def write_ipc_batches(
        self,
        relative_name: str,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
    ) -> Path:
        path = self._safe_path(relative_name)
        try:
            raw = self.open_output(relative_name)
            with raw:
                with pa.PythonFile(raw, mode="w") as sink:
                    with pa.ipc.new_file(sink, schema) as writer:
                        for batch in batches:
                            self._check_cancelled()
                            if not batch.schema.equals(schema, check_metadata=False):
                                raise ValueError("spill batch schema changed")
                            writer.write_batch(batch)
            return path
        except BaseException:
            self.remove(path)
            raise

    def remove(self, path: Path | str) -> None:
        path = Path(path)
        if self._directory is None:
            return
        try:
            path.resolve().relative_to(self._directory.resolve())
        except ValueError as exc:
            raise ValueError("refusing to remove a path outside this spill session") from exc
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return
        path.unlink()
        self._release_bytes(size)

    def cancel(self) -> None:
        self.cancel_event.set()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        directory, self._directory = self._directory, None
        if directory is not None:
            shutil.rmtree(directory, ignore_errors=False)
        with self._lock:
            self._used_bytes = 0

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


@dataclass(frozen=True)
class SortKey:
    name: str
    ascending: bool = True


def _normalize_sort_keys(sort_keys: Sequence[SortKey | tuple[str, str] | str]) -> tuple[SortKey, ...]:
    normalized: list[SortKey] = []
    for item in sort_keys:
        if isinstance(item, SortKey):
            normalized.append(item)
        elif isinstance(item, str):
            normalized.append(SortKey(item, True))
        else:
            name, direction = item
            direction = str(direction).lower()
            if direction not in {"ascending", "descending"}:
                raise ValueError(f"unsupported sort direction {direction!r}")
            normalized.append(SortKey(name, direction == "ascending"))
    if not normalized:
        raise ValueError("at least one sort key is required")
    if len({key.name for key in normalized}) != len(normalized):
        raise ValueError("duplicate sort keys are not allowed")
    return tuple(normalized)


def _sortable_type(dtype: pa.DataType) -> bool:
    return bool(
        pa.types.is_null(dtype)
        or pa.types.is_boolean(dtype)
        or pa.types.is_integer(dtype)
        or pa.types.is_string(dtype)
        or pa.types.is_large_string(dtype)
        or pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
        or pa.types.is_decimal(dtype)
        or pa.types.is_date(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_timestamp(dtype)
        or pa.types.is_duration(dtype)
    )


def _record_batch_row(batch: pa.RecordBatch, row_index: int) -> dict[str, object]:
    """Decode one row with exactly one scalar extraction per column."""
    return {
        name: batch.column(column_index)[row_index].as_py()
        for column_index, name in enumerate(batch.schema.names)
    }


class _Descending:
    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other: "_Descending") -> bool:
        return other.value < self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Descending) and self.value == other.value


class _RunCursor:
    def __init__(self, path: Path, run_order: int):
        self.path = path
        self.run_order = run_order
        self._source = pa.memory_map(str(path), "r")
        self._reader = pa.ipc.open_file(self._source)
        self._batch_index = 0
        self._row_index = 0
        self._batch: pa.RecordBatch | None = None
        self._columns: tuple[pa.Array, ...] = ()
        self._column_indices = {
            name: index for index, name in enumerate(self._reader.schema.names)
        }
        self._fixed_row_bytes = _fixed_width_row_bytes(self._reader.schema)
        self.exhausted = not self._load_batch()

    def _load_batch(self) -> bool:
        while self._batch_index < self._reader.num_record_batches:
            batch = self._reader.get_batch(self._batch_index)
            self._batch_index += 1
            self._row_index = 0
            if batch.num_rows:
                self._batch = batch
                self._columns = tuple(batch.columns)
                return True
        self._batch = None
        self._columns = ()
        return False

    def value(self, name: str):
        assert self._batch is not None
        return self._columns[self._column_indices[name]][self._row_index].as_py()

    def row_nbytes(self) -> int:
        assert self._batch is not None
        if self._fixed_row_bytes is not None:
            return self._fixed_row_bytes
        return self._batch.slice(self._row_index, 1).nbytes

    def row(self) -> dict[str, object]:
        assert self._batch is not None
        return _record_batch_row(self._batch, self._row_index)

    def advance(self) -> bool:
        assert self._batch is not None
        self._row_index += 1
        if self._row_index < self._batch.num_rows:
            return True
        if self._load_batch():
            return True
        self.exhausted = True
        return False

    def close(self) -> None:
        self._source.close()


def _fixed_width_row_bytes(schema: pa.Schema) -> int | None:
    """Cheap conservative row-buffer charge for fixed-width schemas."""
    total = 0
    for field in schema:
        dtype = field.type
        # One full byte per validity/boolean bit deliberately rounds upward.
        total += 1
        if pa.types.is_boolean(dtype) or pa.types.is_null(dtype):
            total += 1
        elif pa.types.is_fixed_size_binary(dtype):
            total += dtype.byte_width
        elif pa.types.is_decimal(dtype):
            total += dtype.bit_width // 8
        elif pa.types.is_primitive(dtype):
            total += max(1, dtype.bit_width // 8)
        else:
            return None
    return total


def _heap_key(cursor: _RunCursor, keys: tuple[SortKey, ...], null_placement: str) -> tuple:
    components = []
    null_last = null_placement == "at_end"
    for key in keys:
        value = cursor.value(key.name)
        if value is None:
            components.append((1 if null_last else 0, None))
        else:
            ordered = value if key.ascending else _Descending(value)
            components.append((0 if null_last else 1, ordered))
    return tuple(components)


def _merge_run_batches(
    paths: Sequence[Path],
    *,
    schema: pa.Schema,
    keys: tuple[SortKey, ...],
    null_placement: str,
    output_batch_rows: int,
    output_batch_bytes: int,
    cancel_event: threading.Event,
) -> Iterator[pa.RecordBatch]:
    cursors: list[_RunCursor] = []
    heap: list[tuple[tuple, int, int, _RunCursor]] = []
    serial = 0

    # Keep row ordering as compact global Arrow take-indices. This avoids the
    # former per-row dict decode/re-encode path, which was both orders of
    # magnitude slower and much harder to bound for Python object overhead.
    selected_batches: list[pa.RecordBatch] = []
    batch_offsets: dict[int, int] = {}
    selected_indices = array("q")
    selected_bytes = 0

    def flush_selection() -> list[pa.RecordBatch]:
        nonlocal selected_batches, batch_offsets, selected_indices, selected_bytes
        if not selected_indices:
            return []
        input_table = pa.Table.from_batches(selected_batches, schema=schema)
        indices = pa.array(selected_indices, type=pa.int64())
        output = pc.take(input_table, indices)
        result = output.to_batches(max_chunksize=output_batch_rows)
        selected_batches = []
        batch_offsets = {}
        selected_indices = array("q")
        selected_bytes = 0
        return result

    try:
        for run_order, path in enumerate(paths):
            cursor = _RunCursor(path, run_order)
            cursors.append(cursor)
            if not cursor.exhausted:
                heapq.heappush(heap, (_heap_key(cursor, keys, null_placement), run_order, serial, cursor))
                serial += 1
        while heap:
            if cancel_event.is_set():
                raise SpillCancelled("external sort was cancelled")
            _, run_order, _, cursor = heapq.heappop(heap)
            assert cursor._batch is not None
            batch = cursor._batch
            row_index = cursor._row_index
            next_row_bytes = cursor.row_nbytes()
            # The Python array and the Arrow index array coexist during take.
            # Charge 16 bytes per selected row in addition to output buffers.
            next_workspace = selected_bytes + next_row_bytes + 16 * (len(selected_indices) + 1)
            if next_row_bytes + 16 > output_batch_bytes:
                raise SpillMemoryLimitExceeded(
                    "one output row exceeds the bounded merge-batch workspace"
                )
            if selected_indices and next_workspace > output_batch_bytes:
                yield from flush_selection()

            identity = id(batch)
            offset = batch_offsets.get(identity)
            if offset is None:
                offset = sum(item.num_rows for item in selected_batches)
                batch_offsets[identity] = offset
                selected_batches.append(batch)
            selected_indices.append(offset + row_index)
            selected_bytes += next_row_bytes

            # Never retain an old run batch after its cursor advances. Current
            # cursor batches are already charged to merge fan-in; retaining old
            # ones would make memory grow with result length.
            batch_exhausted = row_index + 1 >= batch.num_rows
            if batch_exhausted or len(selected_indices) >= output_batch_rows:
                yield from flush_selection()
            if cursor.advance():
                heapq.heappush(
                    heap,
                    (_heap_key(cursor, keys, null_placement), run_order, serial, cursor),
                )
                serial += 1
        yield from flush_selection()
    finally:
        for cursor in cursors:
            cursor.close()


def external_sort(
    batches: Iterable[pa.RecordBatch],
    *,
    schema: pa.Schema,
    sort_keys: Sequence[SortKey | tuple[str, str] | str],
    session: SpillSession,
    memory_budget_bytes: int,
    output_batch_rows: int = 16 * 1024,
    null_placement: str = "at_end",
    max_open_runs: int = 32,
) -> ArrowBatchStream:
    """Sort Arrow batches with bounded runs and a bounded fan-in merge.

    Float keys are deliberately rejected because total NaN ordering differs
    among Arrow/Polars/DuckDB versions.  Callers must route such a query until
    one engine-wide ordering contract is sealed.
    """
    keys = _normalize_sort_keys(sort_keys)
    if null_placement not in {"at_start", "at_end"}:
        raise ValueError("null_placement must be 'at_start' or 'at_end'")
    if output_batch_rows <= 0 or max_open_runs < 2:
        raise ValueError("invalid external-sort batch/fan-in configuration")
    if memory_budget_bytes < 256 * 1024:
        raise SpillMemoryLimitExceeded("external sort needs at least 256 KiB of workspace")
    for key in keys:
        index = schema.get_field_index(key.name)
        if index < 0:
            raise ValueError(f"unknown sort key {key.name!r}")
        if not _sortable_type(schema.field(index).type):
            raise UnsupportedSpillOperation(
                f"external sorting of {schema.field(index).type} keys is not sealed"
            )

    # Leave explicit headroom for sort indices/take buffers and for the merge's
    # simultaneously open input batches. Arrow ``nbytes`` can be tiny for
    # bit-packed/RLE-like values while sort_indices still needs one integer per
    # row, so runs are bounded by both bytes and rows.
    run_target = max(64 * 1024, memory_budget_bytes // 8)
    merge_batch_bytes = run_target
    run_row_target = max(1, memory_budget_bytes // 32)
    effective_max_open_runs = min(
        max_open_runs,
        max(2, (memory_budget_bytes // 2) // run_target),
    )
    arrow_keys = [(key.name, "ascending" if key.ascending else "descending") for key in keys]
    pending: list[pa.RecordBatch] = []
    pending_bytes = 0
    pending_rows = 0
    runs: list[Path] = []
    owned_paths: set[Path] = set()
    run_number = 0

    def flush_run() -> None:
        nonlocal pending, pending_bytes, pending_rows, run_number
        if not pending:
            return
        table = pa.Table.from_batches(pending, schema=schema)
        indices = pc.sort_indices(table, sort_keys=arrow_keys, null_placement=null_placement)
        sorted_table = pc.take(table, indices)
        name = f"sort-{run_number:08d}.arrow"
        run_number += 1
        path = session.write_ipc_batches(
            name,
            schema,
            sorted_table.to_batches(
                max_chunksize=min(output_batch_rows, run_row_target),
            ),
        )
        runs.append(path)
        owned_paths.add(path)
        pending = []
        pending_bytes = 0
        pending_rows = 0

    input_iterator = iter(batches)
    try:
        for batch in input_iterator:
            session._check_cancelled()
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("external sort input must yield RecordBatch objects")
            if not batch.schema.equals(schema, check_metadata=False):
                raise ValueError("external sort input schema changed")
            if batch.nbytes > memory_budget_bytes:
                raise SpillMemoryLimitExceeded(
                    "one input RecordBatch exceeds the external-sort memory budget"
                )
            offset = 0
            while offset < batch.num_rows:
                remaining = batch.slice(offset)
                if remaining.nbytes <= run_target:
                    piece = remaining
                else:
                    rows = max(1, math.floor(remaining.num_rows * run_target / remaining.nbytes))
                    piece = remaining.slice(0, rows)
                    while piece.nbytes > run_target and rows > 1:
                        rows = max(1, rows // 2)
                        piece = remaining.slice(0, rows)
                    if piece.nbytes > run_target:
                        raise SpillMemoryLimitExceeded(
                            "one Arrow row exceeds the bounded sort-run workspace"
                        )
                if piece.num_rows > run_row_target:
                    piece = piece.slice(0, run_row_target)
                if pending and (
                    pending_bytes + piece.nbytes > run_target
                    or pending_rows + piece.num_rows > run_row_target
                ):
                    flush_run()
                pending.append(piece)
                pending_bytes += piece.nbytes
                pending_rows += piece.num_rows
                offset += piece.num_rows
        flush_run()

        # Limit both mapped pages and file descriptors.  Each pass replaces a
        # group of old runs with one run; the session quota accounts for the
        # temporary old+new overlap and fails instead of overcommitting disk.
        while len(runs) > effective_max_open_runs:
            next_runs: list[Path] = []
            for start in range(0, len(runs), effective_max_open_runs):
                group = runs[start : start + effective_max_open_runs]
                if len(group) == 1:
                    next_runs.extend(group)
                    continue
                name = f"merge-{run_number:08d}.arrow"
                run_number += 1
                merged = _merge_run_batches(
                    group,
                    schema=schema,
                    keys=keys,
                    null_placement=null_placement,
                    output_batch_rows=output_batch_rows,
                    output_batch_bytes=merge_batch_bytes,
                    cancel_event=session.cancel_event,
                )
                new_path = session.write_ipc_batches(name, schema, merged)
                next_runs.append(new_path)
                owned_paths.add(new_path)
                for old_path in group:
                    session.remove(old_path)
                    owned_paths.discard(old_path)
            runs = next_runs
    except BaseException:
        for path in tuple(owned_paths):
            session.remove(path)
        raise
    finally:
        close_input = getattr(input_iterator, "close", None)
        if callable(close_input):
            close_input()

    def cleanup_runs() -> None:
        for path in tuple(owned_paths):
            session.remove(path)
            owned_paths.discard(path)

    def produce() -> Iterator[pa.RecordBatch]:
        try:
            if runs:
                yield from _merge_run_batches(
                    runs,
                    schema=schema,
                    keys=keys,
                    null_placement=null_placement,
                    output_batch_rows=output_batch_rows,
                    output_batch_bytes=merge_batch_bytes,
                    cancel_event=session.cancel_event,
                )
        finally:
            cleanup_runs()

    # Closing an unstarted generator does not execute its finally block. The
    # callback is what reclaims eagerly built runs when no batch is requested.
    return ArrowBatchStream(
        schema,
        produce(),
        close_callback=cleanup_runs,
        cancel_event=session.cancel_event,
    )


@dataclass(frozen=True)
class AggregateSpec:
    output_name: str
    function: str
    input_column: str | None = None
    output_type: pa.DataType | None = None


def _estimated_python_row_bytes(row: dict[str, object]) -> int:
    """Conservative-enough admission estimate for the sealed scalar subset."""
    total = sys.getsizeof(row)
    for name, value in row.items():
        total += sys.getsizeof(name) + sys.getsizeof(value)
    return total


def _aggregate_output_type(spec: AggregateSpec, schema: pa.Schema) -> pa.DataType:
    function = spec.function.lower()
    if function == "count_star":
        if spec.input_column is not None:
            raise ValueError("count_star must not name an input column")
        return spec.output_type or pa.int64()
    if function == "count":
        if spec.input_column is None or schema.get_field_index(spec.input_column) < 0:
            raise ValueError("count requires an existing input column")
        return spec.output_type or pa.int64()
    if spec.input_column is None or schema.get_field_index(spec.input_column) < 0:
        raise ValueError(f"aggregate {function!r} requires an existing input column")
    source_type = schema.field(schema.get_field_index(spec.input_column)).type
    if function == "sum":
        if not (pa.types.is_integer(source_type) or pa.types.is_decimal(source_type)):
            raise UnsupportedSpillOperation("bounded SUM currently supports integer/decimal inputs")
        if spec.output_type is not None:
            return spec.output_type
        if pa.types.is_integer(source_type):
            # DuckDB widens integer SUM to HUGEINT. Decimal128 is Arrow's exact
            # portable representation and avoids silent int64/uint64 overflow.
            return pa.decimal128(38, 0)
        # DuckDB widens DECIMAL SUM to its maximum precision while preserving
        # scale. Keeping the input precision would fail valid sums as soon as
        # an additional carry digit is needed.
        if pa.types.is_decimal128(source_type):
            return pa.decimal128(38, source_type.scale)
        return pa.decimal256(76, source_type.scale)
    if function in {"min", "max"}:
        if not _sortable_type(source_type):
            raise UnsupportedSpillOperation(f"bounded {function.upper()} does not support {source_type}")
        return spec.output_type or source_type
    raise UnsupportedSpillOperation(f"aggregate {function!r} has no bounded implementation")


def external_group_aggregate(
    batches: Iterable[pa.RecordBatch],
    *,
    schema: pa.Schema,
    group_keys: Sequence[str],
    aggregates: Sequence[AggregateSpec],
    session: SpillSession,
    memory_budget_bytes: int,
    output_batch_rows: int = 16 * 1024,
    max_open_runs: int = 32,
) -> ArrowBatchStream:
    """Exact sort-based GROUP BY using bounded external-sort state.

    Supported functions are COUNT(*), COUNT(column), integer/decimal SUM,
    MIN, and MAX.  Only one group's accumulator is resident after sorting.
    """
    if not group_keys:
        raise UnsupportedSpillOperation("global aggregation should use the bounded scalar path")
    if len(set(group_keys)) != len(group_keys):
        raise ValueError("duplicate group keys are not allowed")
    for name in group_keys:
        index = schema.get_field_index(name)
        if index < 0:
            raise ValueError(f"unknown group key {name!r}")
        if not _sortable_type(schema.field(index).type):
            raise UnsupportedSpillOperation(f"group key type {schema.field(index).type} is not sealed")
    output_names = list(group_keys) + [spec.output_name for spec in aggregates]
    if len(set(output_names)) != len(output_names):
        raise ValueError("aggregate output names must be unique")

    output_schema = pa.schema(
        [schema.field(schema.get_field_index(name)) for name in group_keys]
        + [pa.field(spec.output_name, _aggregate_output_type(spec, schema)) for spec in aggregates]
    )
    sorted_stream = external_sort(
        batches,
        schema=schema,
        sort_keys=list(group_keys),
        session=session,
        memory_budget_bytes=memory_budget_bytes,
        output_batch_rows=output_batch_rows,
        null_placement="at_start",
        max_open_runs=max_open_runs,
    )

    def initial_states() -> list[object]:
        states: list[object] = []
        for spec in aggregates:
            function = spec.function.lower()
            states.append(0 if function in {"count", "count_star"} else None)
        return states

    def update(states: list[object], row: dict[str, object]) -> None:
        for index, spec in enumerate(aggregates):
            function = spec.function.lower()
            value = row.get(spec.input_column) if spec.input_column is not None else None
            if function == "count_star":
                states[index] = int(states[index]) + 1
            elif function == "count":
                if value is not None:
                    states[index] = int(states[index]) + 1
            elif value is not None and function == "sum":
                states[index] = value if states[index] is None else states[index] + value
            elif value is not None and function == "min":
                states[index] = value if states[index] is None or value < states[index] else states[index]
            elif value is not None and function == "max":
                states[index] = value if states[index] is None or value > states[index] else states[index]

    def produce() -> Iterator[pa.RecordBatch]:
        current_key = None
        have_group = False
        states: list[object] = []
        output_rows: list[dict[str, object]] = []
        output_bytes = 0
        max_output_bytes = max(64 * 1024, memory_budget_bytes // 4)

        def emit() -> dict[str, object]:
            assert current_key is not None or have_group
            result = {name: current_key[index] for index, name in enumerate(group_keys)}
            result.update({spec.output_name: states[index] for index, spec in enumerate(aggregates)})
            return result

        try:
            for batch in sorted_stream:
                # ``batch.to_pylist()`` materializes every row and nested scalar
                # at once, outside max_output_bytes. Scalar iteration is slower
                # but preserves the hard memory contract for wide batches.
                for row_index in range(batch.num_rows):
                    row = _record_batch_row(batch, row_index)
                    key = tuple(row[name] for name in group_keys)
                    if not have_group or key != current_key:
                        if have_group:
                            emitted = emit()
                            emitted_bytes = _estimated_python_row_bytes(emitted)
                            if emitted_bytes > max_output_bytes:
                                raise SpillMemoryLimitExceeded(
                                    "one aggregate result row exceeds the bounded output workspace"
                                )
                            if output_rows and output_bytes + emitted_bytes > max_output_bytes:
                                yield pa.RecordBatch.from_pylist(output_rows, schema=output_schema)
                                output_rows = []
                                output_bytes = 0
                            output_rows.append(emitted)
                            output_bytes += emitted_bytes
                            if len(output_rows) >= output_batch_rows:
                                yield pa.RecordBatch.from_pylist(output_rows, schema=output_schema)
                                output_rows = []
                                output_bytes = 0
                        current_key = key
                        states = initial_states()
                        have_group = True
                    update(states, row)
            if have_group:
                emitted = emit()
                emitted_bytes = _estimated_python_row_bytes(emitted)
                if emitted_bytes > max_output_bytes:
                    raise SpillMemoryLimitExceeded(
                        "one aggregate result row exceeds the bounded output workspace"
                    )
                if output_rows and output_bytes + emitted_bytes > max_output_bytes:
                    yield pa.RecordBatch.from_pylist(output_rows, schema=output_schema)
                    output_rows = []
                output_rows.append(emitted)
            if output_rows:
                yield pa.RecordBatch.from_pylist(output_rows, schema=output_schema)
        finally:
            sorted_stream.close()

    # Closing before the first output batch must still close the eagerly-built
    # nested sort stream and reclaim all runs.
    return ArrowBatchStream(
        output_schema,
        produce(),
        close_callback=sorted_stream.close,
        cancel_event=session.cancel_event,
    )
