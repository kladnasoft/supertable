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
import time
import uuid
from array import array
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

import numpy as np
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


class SpillDeadlineExceeded(IslandSpillError, TimeoutError):
    """The query crossed its monotonic execution deadline."""


class SpillMemoryLimitExceeded(IslandSpillError):
    pass


class SpillNumericOverflow(IslandSpillError):
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
        deadline_monotonic: float | None = None,
        disk_usage=shutil.disk_usage,
        monotonic=time.monotonic,
    ):
        if budget_bytes < 0 or min_free_bytes < 0:
            raise ValueError("spill budgets cannot be negative")
        if deadline_monotonic is not None and not math.isfinite(
            float(deadline_monotonic)
        ):
            raise ValueError("spill deadline must be finite")
        self.root = Path(root)
        self.budget_bytes = int(budget_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.query_id = query_id or uuid.uuid4().hex
        self.cancel_event = cancel_event or threading.Event()
        self.deadline_monotonic = (
            None if deadline_monotonic is None else float(deadline_monotonic)
        )
        self._disk_usage = disk_usage
        self._monotonic = monotonic
        self._deadline_expired = False
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
        if self._deadline_expired:
            raise SpillDeadlineExceeded(
                f"spill query {self.query_id!r} timed out at its execution deadline"
            )
        if self.cancel_event.is_set():
            raise SpillCancelled(f"spill query {self.query_id!r} was cancelled")
        if (
            self.deadline_monotonic is not None
            and self._monotonic() >= self.deadline_monotonic
        ):
            # Use the same event consumed by nested Arrow streams.  No worker
            # is killed or generator closed cross-thread: the operator that
            # observes the deadline unwinds normally through its cleanup.
            self._deadline_expired = True
            self.cancel_event.set()
            raise SpillDeadlineExceeded(
                f"spill query {self.query_id!r} timed out at its execution deadline"
            )

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
    session: SpillSession,
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
    processed_rows = 0

    def flush_selection() -> list[pa.RecordBatch]:
        nonlocal selected_batches, batch_offsets, selected_indices, selected_bytes
        if not selected_indices:
            return []
        session._check_cancelled()
        input_table = pa.Table.from_batches(selected_batches, schema=schema)
        indices = pa.array(selected_indices, type=pa.int64())
        output = pc.take(input_table, indices)
        session._check_cancelled()
        result = output.to_batches(max_chunksize=output_batch_rows)
        selected_batches = []
        batch_offsets = {}
        selected_indices = array("q")
        selected_bytes = 0
        return result

    try:
        for run_order, path in enumerate(paths):
            session._check_cancelled()
            cursor = _RunCursor(path, run_order)
            cursors.append(cursor)
            if not cursor.exhausted:
                heapq.heappush(heap, (_heap_key(cursor, keys, null_placement), run_order, serial, cursor))
                serial += 1
        while heap:
            # Preserve cheap, prompt explicit cancellation on every row while
            # sampling the monotonic clock only periodically.  Native/file
            # batch transitions and every output batch are checked below too.
            if not (processed_rows & 1023) or session.cancel_event.is_set():
                session._check_cancelled()
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
            processed_rows += 1

            # Never retain an old run batch after its cursor advances. Current
            # cursor batches are already charged to merge fan-in; retaining old
            # ones would make memory grow with result length.
            batch_exhausted = row_index + 1 >= batch.num_rows
            if batch_exhausted or len(selected_indices) >= output_batch_rows:
                yield from flush_selection()
            if cursor.advance():
                if batch_exhausted:
                    session._check_cancelled()
                heapq.heappush(
                    heap,
                    (_heap_key(cursor, keys, null_placement), run_order, serial, cursor),
                )
                serial += 1
        yield from flush_selection()
    finally:
        for cursor in cursors:
            cursor.close()


@dataclass
class _RangePartition:
    bucket: int
    path: Path
    rows: int = 0
    logical_bytes: int = 0


class _RangeIPCWriter:
    """One query-private IPC partition backed only by ``SpillSession`` I/O."""

    def __init__(
        self,
        session: SpillSession,
        relative_name: str,
        schema: pa.Schema,
        *,
        buffer_bytes: int = 0,
    ):
        if buffer_bytes < 0:
            raise ValueError("range writer buffer cannot be negative")
        self.session = session
        self.path = session._safe_path(relative_name)
        self.schema = schema
        self.buffer_bytes = int(buffer_bytes)
        self._raw: _BudgetedFile | None = None
        self._buffered: io.BufferedWriter | None = None
        self._sink: pa.PythonFile | None = None
        self._writer = None
        self.closed = False
        try:
            self._raw = session.open_output(relative_name)
            target = self._raw
            if self.buffer_bytes:
                self._buffered = io.BufferedWriter(
                    self._raw,
                    buffer_size=self.buffer_bytes,
                )
                target = self._buffered
            self._sink = pa.PythonFile(target, mode="w")
            self._writer = pa.ipc.new_file(self._sink, schema)
        except BaseException:
            self.abort()
            raise

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if self.closed or self._writer is None:
            raise ValueError("write to closed range partition")
        self.session._check_cancelled()
        if not batch.schema.equals(self.schema, check_metadata=False):
            raise ValueError("range partition schema changed")
        self._writer.write_batch(batch)
        # Buffered writes may not reach _BudgetedFile until close. Preserve a
        # prompt cooperative boundary after every complete IPC batch anyway.
        self.session._check_cancelled()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        error: BaseException | None = None
        try:
            if self._writer is not None:
                self._writer.close()
        except BaseException as exc:  # footer writes are quota/deadline checked
            error = exc
        finally:
            self._writer = None
            try:
                if self._sink is not None:
                    self._sink.close()
            except BaseException as exc:
                if error is None:
                    error = exc
            finally:
                self._sink = None
                try:
                    if self._buffered is not None:
                        self._buffered.close()
                except BaseException as exc:
                    if error is None:
                        error = exc
                finally:
                    self._buffered = None
                    try:
                        if self._raw is not None:
                            self._raw.close()
                    except BaseException as exc:
                        if error is None:
                            error = exc
                    finally:
                        self._raw = None
        if error is not None:
            raise error

    def abort(self) -> None:
        try:
            self.close()
        except BaseException:
            # A cancelled/quota-exhausted session may reject the IPC footer.
            # Handles are closed in ``close`` regardless; the partial file is
            # removed below and its actually-persisted bytes are released.
            pass
        self.session.remove(self.path)


def _normalize_integer_range(value) -> tuple[int, int, bool] | None:
    """Accept a sealed bound object, mapping, or ``(min, max[, null])`` tuple."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        minimum = value.get("minimum")
        maximum = value.get("maximum")
        has_null = value.get("has_null", False)
    elif isinstance(value, (tuple, list)):
        if len(value) not in {2, 3}:
            raise ValueError("first_key_range must contain two or three values")
        minimum, maximum = value[:2]
        has_null = value[2] if len(value) == 3 else False
    else:
        minimum = getattr(value, "minimum", None)
        maximum = getattr(value, "maximum", None)
        has_null = getattr(value, "has_null", False)
    if minimum is None or maximum is None:
        # Empty/all-NULL complete domains have no useful non-NULL range.  The
        # legacy path remains the conservative implementation for that shape.
        return None
    if (
        not isinstance(minimum, int)
        or isinstance(minimum, bool)
        or not isinstance(maximum, int)
        or isinstance(maximum, bool)
        or minimum > maximum
        or not isinstance(has_null, bool)
    ):
        raise ValueError("first_key_range must be an ordered integer domain")
    return int(minimum), int(maximum), has_null


def _integer_type_limits(dtype: pa.DataType) -> tuple[int, int]:
    if not pa.types.is_integer(dtype):
        raise TypeError(f"range partitioning requires an integer key, got {dtype}")
    bits = int(dtype.bit_width)
    if pa.types.is_unsigned_integer(dtype):
        return 0, (1 << bits) - 1
    return -(1 << (bits - 1)), (1 << (bits - 1)) - 1


def _integer_numpy_dtype(dtype: pa.DataType) -> np.dtype:
    prefix = "uint" if pa.types.is_unsigned_integer(dtype) else "int"
    return np.dtype(f"{prefix}{int(dtype.bit_width)}")


def _integer_range_cuts(
    dtype: pa.DataType,
    minimum: int,
    maximum: int,
    partition_count: int,
) -> np.ndarray:
    """Return exact exclusive interval starts without fixed-width overflow.

    All subtraction, multiplication, and ceiling division happen as Python
    arbitrary-precision integers.  Conversion to the physical NumPy type is
    performed only after every cut is proven inside that type's domain.
    """
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    type_minimum, type_maximum = _integer_type_limits(dtype)
    # A stale/conservative statistics lane may be wider than the physical
    # type. Clamp it safely. An impossible intersection falls back to the full
    # physical domain; execution-time values still saturate into edge buckets.
    lower = max(int(minimum), type_minimum)
    upper = min(int(maximum), type_maximum)
    if lower > upper:
        lower, upper = type_minimum, type_maximum
    span = upper - lower + 1
    count = min(int(partition_count), span)
    cuts = [
        lower + (span * index + count - 1) // count
        for index in range(1, count)
    ]
    if any(not (type_minimum <= cut <= type_maximum) for cut in cuts):
        raise AssertionError("integer partition cut escaped its physical type")
    return np.asarray(cuts, dtype=_integer_numpy_dtype(dtype))


def _range_sort_workspace_bytes(logical_bytes: int, rows: int) -> int:
    """Conservative live input + int64 indices + take-output workspace."""
    logical_bytes = max(0, int(logical_bytes))
    rows = max(0, int(rows))
    allocator_slack = max(64 * 1024, logical_bytes // 16)
    return 2 * logical_bytes + 8 * rows + allocator_slack


def _range_writer_buffer_bytes(
    memory_budget_bytes: int,
    possible_bucket_count: int,
) -> int:
    """Return a power-of-two buffer under one sixteenth aggregate memory."""
    if memory_budget_bytes < 0 or possible_bucket_count <= 0:
        raise ValueError("invalid range writer buffer inputs")
    aggregate_budget = memory_budget_bytes // 16
    maximum = min(
        512 * 1024,
        aggregate_budget // possible_bucket_count,
    )
    per_bucket = 0 if maximum <= 0 else 1 << (maximum.bit_length() - 1)
    # A smaller BufferedWriter adds allocation and copy overhead without
    # materially coalescing Arrow's metadata/value writes. Fall back to exact
    # unbuffered accounting when the aggregate reserve cannot afford 64 KiB
    # for every possible (including NULL) partition.
    if per_bucket < 64 * 1024:
        return 0
    if per_bucket * possible_bucket_count > aggregate_budget:
        raise AssertionError("range writer buffers exceed aggregate budget")
    return int(per_bucket)


def _native_sort_partition(
    partition: _RangePartition,
    *,
    schema: pa.Schema,
    arrow_keys: Sequence[tuple[str, str]],
    null_placement: str,
    session: SpillSession,
) -> pa.Table:
    session._check_cancelled()
    source = pa.memory_map(str(partition.path), "r")
    try:
        reader = pa.ipc.open_file(source)
        if not reader.schema.equals(schema, check_metadata=False):
            raise ValueError("range partition schema changed on disk")
        table = reader.read_all()
        if table.num_rows != partition.rows:
            raise ValueError("range partition row count changed on disk")
        session._check_cancelled()
        indices = pc.sort_indices(
            table,
            sort_keys=list(arrow_keys),
            null_placement=null_placement,
        )
        sorted_table = pc.take(table, indices)
        session._check_cancelled()
        return sorted_table
    finally:
        source.close()


def _integer_range_external_sort(
    batches: Iterable[pa.RecordBatch],
    *,
    schema: pa.Schema,
    keys: tuple[SortKey, ...],
    integer_range: tuple[int, int, bool],
    session: SpillSession,
    memory_budget_bytes: int,
    output_batch_rows: int,
    null_placement: str,
    max_open_runs: int,
    input_size_hint_bytes: int,
    input_rows_hint: int,
    parallelism: int,
) -> ArrowBatchStream:
    """One-write external range partition followed by bounded native sorts."""
    first_key = keys[0]
    key_index = schema.get_field_index(first_key.name)
    key_type = schema.field(key_index).type
    if not pa.types.is_integer(key_type):
        raise UnsupportedSpillOperation(
            f"native range sorting requires an integer first key, got {key_type}"
        )

    # Keep one eighth for output/coalescing transients. The rolling scheduler
    # below never lets completed or running native sorts collectively reserve
    # more than this remaining aggregate budget.
    sort_memory_budget = memory_budget_bytes * 7 // 8
    workers = max(1, min(int(parallelism), 32))
    per_worker_budget = max(1, sort_memory_budget // workers)
    hinted_workspace = _range_sort_workspace_bytes(
        input_size_hint_bytes, input_rows_hint,
    )
    desired_partitions = max(
        2,
        (hinted_workspace + per_worker_budget - 1) // per_worker_budget,
    )
    retained_output_headroom = memory_budget_bytes // 8
    retained_output_target = max(1, retained_output_headroom * 3 // 4)
    desired_partitions = max(
        desired_partitions,
        (
            input_size_hint_bytes + retained_output_target - 1
        ) // retained_output_target,
    )
    # Four times the established merge descriptor limit permits enough narrow
    # partitions for four-way native sorting without allowing hostile metadata
    # to request an unbounded number of open files.
    max_range_partitions = max(2, min(256, int(max_open_runs) * 4))
    desired_partitions = min(desired_partitions, max_range_partitions)
    minimum, maximum, _ = integer_range
    cuts = _integer_range_cuts(
        key_type, minimum, maximum, desired_partitions,
    )
    nonnull_partition_count = len(cuts) + 1
    null_bucket = nonnull_partition_count
    total_bucket_count = nonnull_partition_count + 1
    writer_buffer_bytes = _range_writer_buffer_bytes(
        memory_budget_bytes,
        total_bucket_count,
    )

    # Every possible non-empty interval owns one query-private IPC descriptor.
    # Fail over before consuming input if the process cannot retain a safety
    # reserve for scanners, telemetry, and the legacy fallback itself.
    try:
        import resource

        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        open_descriptors = (
            len(os.listdir("/proc/self/fd"))
            if Path("/proc/self/fd").is_dir()
            else 0
        )
        descriptor_reserve = 64
        insufficient_descriptors = bool(
            soft_limit != resource.RLIM_INFINITY
            and open_descriptors + total_bucket_count + descriptor_reserve
            > soft_limit
        )
    except (ImportError, OSError, ValueError):
        # Platforms without RLIMIT_NOFILE retain lazy writer creation. Actual
        # open failures still unwind through the same ownership cleanup.
        insufficient_descriptors = False
    if insufficient_descriptors:
        return external_sort(
            batches,
            schema=schema,
            sort_keys=keys,
            session=session,
            memory_budget_bytes=memory_budget_bytes,
            output_batch_rows=output_batch_rows,
            null_placement=null_placement,
            max_open_runs=max_open_runs,
            parallelism=parallelism,
        )

    arrow_keys = [
        (key.name, "ascending" if key.ascending else "descending")
        for key in keys
    ]
    partitions: dict[int, _RangePartition] = {}
    writers: dict[int, _RangeIPCWriter] = {}
    owned_paths: set[Path] = set()

    def cleanup_partitions() -> None:
        for writer in tuple(writers.values()):
            if not writer.closed:
                writer.abort()
        writers.clear()
        for path in tuple(owned_paths):
            session.remove(path)
            owned_paths.discard(path)

    def ensure_writer(bucket: int) -> _RangeIPCWriter:
        writer = writers.get(bucket)
        if writer is not None:
            return writer
        name = f"range-{bucket:08d}.arrow"
        writer = _RangeIPCWriter(
            session,
            name,
            schema,
            buffer_bytes=writer_buffer_bytes,
        )
        writers[bucket] = writer
        owned_paths.add(writer.path)
        partition = partitions.get(bucket)
        if partition is None:
            partitions[bucket] = _RangePartition(
                bucket=bucket, path=writer.path,
            )
        elif partition.path != writer.path:
            raise AssertionError("range partition path changed")
        return writer

    source_blocks: list[pa.RecordBatch] = []
    source_block_bytes = 0
    source_block_rows = 0
    # A combined block and its single permuted output coexist. The producer's
    # source buffers belong to the independently reserved scan lane; one fourth
    # leaves operator space for ids, stable indices, and allocator transients.
    source_block_byte_target = max(64 * 1024, memory_budget_bytes // 4)
    source_block_row_target = max(1, memory_budget_bytes // 64)

    def flush_source_block() -> None:
        nonlocal source_blocks, source_block_bytes, source_block_rows
        if not source_blocks:
            return
        session._check_cancelled()
        # Keep the source columns chunked. ``take(Table, ...)`` accepts
        # ChunkedArrays and emits a canonical reordered table, so eagerly
        # combining every wide source column would copy the complete input
        # block once for no semantic benefit. Only the small partition key is
        # coalesced below for NumPy range assignment.
        combined = pa.Table.from_batches(source_blocks, schema=schema)
        key_column = combined.column(key_index).combine_chunks()
        if key_column.null_count:
            valid = np.asarray(
                pc.is_valid(key_column).to_numpy(zero_copy_only=False),
                dtype=np.bool_,
            )
            filled = pc.fill_null(
                key_column, pa.scalar(0, type=key_type),
            )
            values = filled.to_numpy(zero_copy_only=False)
        else:
            valid = None
            values = key_column.to_numpy(zero_copy_only=False)
        partition_ids = np.searchsorted(
            cuts, values, side="right",
        ).astype(np.int32, copy=False)
        if valid is not None:
            partition_ids = partition_ids.copy()
            partition_ids[~valid] = null_bucket
        scatter_order = np.argsort(partition_ids, kind="stable")
        counts = np.bincount(
            partition_ids,
            minlength=total_bucket_count,
        )
        # Exactly one native full-block permutation. Every partition is now a
        # contiguous zero-copy table slice and is written synchronously before
        # this bounded block is released.
        reordered = pc.take(
            combined,
            pa.array(scatter_order, type=pa.int64()),
        )
        if not isinstance(reordered, pa.Table):
            raise TypeError("Arrow range scatter did not return a Table")
        start = 0
        for bucket, count in enumerate(counts):
            count = int(count)
            if not count:
                continue
            partition_table = reordered.slice(start, count)
            writer = ensure_writer(bucket)
            try:
                for output in partition_table.to_batches(
                    max_chunksize=max(1, count),
                ):
                    writer.write_batch(output)
            except BaseException:
                writer.abort()
                owned_paths.discard(writer.path)
                writers.pop(bucket, None)
                partitions.pop(bucket, None)
                raise
            partition = partitions[bucket]
            partition.rows += count
            partition.logical_bytes += partition_table.nbytes
            start += count
        session._check_cancelled()
        source_blocks = []
        source_block_bytes = 0
        source_block_rows = 0

    input_iterator = iter(batches)
    try:
        while True:
            session._check_cancelled()
            try:
                batch = next(input_iterator)
            except StopIteration:
                break
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
                bytes_room = source_block_byte_target - source_block_bytes
                rows_room = source_block_row_target - source_block_rows
                if source_blocks and (bytes_room <= 0 or rows_room <= 0):
                    flush_source_block()
                    continue
                rows = min(remaining.num_rows, max(1, rows_room))
                if remaining.nbytes > max(1, bytes_room):
                    rows = min(
                        rows,
                        max(
                            1,
                            math.floor(
                                remaining.num_rows
                                * max(1, bytes_room)
                                / remaining.nbytes
                            ),
                        ),
                    )
                piece = remaining.slice(0, rows)
                while piece.nbytes > source_block_byte_target and rows > 1:
                    rows = max(1, rows // 2)
                    piece = remaining.slice(0, rows)
                if piece.nbytes > source_block_byte_target:
                    raise SpillMemoryLimitExceeded(
                        "one Arrow row exceeds the bounded range-partition workspace"
                    )
                if source_blocks and (
                    source_block_bytes + piece.nbytes > source_block_byte_target
                    or source_block_rows + piece.num_rows > source_block_row_target
                ):
                    flush_source_block()
                    continue
                source_blocks.append(piece)
                source_block_bytes += piece.nbytes
                source_block_rows += piece.num_rows
                offset += piece.num_rows
                if (
                    source_block_bytes >= source_block_byte_target
                    or source_block_rows >= source_block_row_target
                ):
                    flush_source_block()

        flush_source_block()
        for writer in tuple(writers.values()):
            writer.close()
        # Every observed partition must have acquired a real writer during its
        # synchronous block scatter.
        if any(not partition.path.exists() for partition in partitions.values()):
            raise AssertionError("range partition was not materialized")
    except BaseException:
        cleanup_partitions()
        raise
    finally:
        close_input = getattr(input_iterator, "close", None)
        if callable(close_input):
            close_input()

    nonnull_buckets = sorted(
        (bucket for bucket in partitions if bucket != null_bucket),
        reverse=not first_key.ascending,
    )
    ordered_buckets = nonnull_buckets
    if null_bucket in partitions:
        if null_placement == "at_start":
            ordered_buckets = [null_bucket, *ordered_buckets]
        else:
            ordered_buckets = [*ordered_buckets, null_bucket]
    ordered_partitions = [partitions[bucket] for bucket in ordered_buckets]

    # An unexpectedly hot interval is never admitted to an over-budget native
    # kernel. Re-feed the stable, key-disjoint raw partitions into the proven
    # bounded legacy sorter. Files are deleted as it consumes them, so its new
    # runs overlap only the current raw input within the existing spill quota.
    if any(
        part.logical_bytes > retained_output_headroom
        or _range_sort_workspace_bytes(part.logical_bytes, part.rows)
        > sort_memory_budget
        for part in ordered_partitions
    ):
        def fallback_batches() -> Iterator[pa.RecordBatch]:
            try:
                for partition in ordered_partitions:
                    session._check_cancelled()
                    source = pa.memory_map(str(partition.path), "r")
                    try:
                        reader = pa.ipc.open_file(source)
                        for batch_index in range(reader.num_record_batches):
                            session._check_cancelled()
                            yield reader.get_batch(batch_index)
                    finally:
                        source.close()
                    session.remove(partition.path)
                    owned_paths.discard(partition.path)
            finally:
                cleanup_partitions()

        return external_sort(
            fallback_batches(),
            schema=schema,
            sort_keys=keys,
            session=session,
            memory_budget_bytes=memory_budget_bytes,
            output_batch_rows=output_batch_rows,
            null_placement=null_placement,
            max_open_runs=max_open_runs,
        )

    if not ordered_partitions:
        return ArrowBatchStream(
            schema,
            iter(()),
            close_callback=cleanup_partitions,
            cancel_event=session.cancel_event,
        )

    def produce() -> Iterator[pa.RecordBatch]:
        executor: ThreadPoolExecutor | None = None
        futures: dict[int, tuple[Future, int]] = {}
        reserved_workspace = 0
        next_submit = 0

        def submit_available() -> None:
            nonlocal next_submit, reserved_workspace
            while (
                next_submit < len(ordered_partitions)
                and len(futures) < workers
            ):
                partition = ordered_partitions[next_submit]
                workspace = _range_sort_workspace_bytes(
                    partition.logical_bytes, partition.rows,
                )
                if futures and reserved_workspace + workspace > sort_memory_budget:
                    break
                if workspace > sort_memory_budget:
                    raise SpillMemoryLimitExceeded(
                        "one range partition exceeds aggregate native-sort memory"
                    )
                future = executor.submit(
                    _native_sort_partition,
                    partition,
                    schema=schema,
                    arrow_keys=arrow_keys,
                    null_placement=null_placement,
                    session=session,
                )
                futures[next_submit] = (future, workspace)
                reserved_workspace += workspace
                next_submit += 1

        try:
            executor = ThreadPoolExecutor(
                max_workers=min(workers, len(ordered_partitions)),
                thread_name_prefix="island-range-sort",
            )
            submit_available()
            for index, partition in enumerate(ordered_partitions):
                session._check_cancelled()
                if index not in futures:
                    submit_available()
                future, workspace = futures.pop(index)
                try:
                    sorted_table = future.result()
                    try:
                        session._check_cancelled()
                        for batch in sorted_table.to_batches(
                            max_chunksize=output_batch_rows,
                        ):
                            session._check_cancelled()
                            yield batch
                    finally:
                        del sorted_table
                finally:
                    # A completed Future retains its result in ``_result``.
                    # Drop it before releasing the workspace reservation or
                    # admitting another native task.
                    del future
                    session.remove(partition.path)
                    owned_paths.discard(partition.path)
                    reserved_workspace = max(0, reserved_workspace - workspace)
                submit_available()
        finally:
            for future, _ in tuple(futures.values()):
                future.cancel()
            # Never unlink a memory-mapped input while a native worker may
            # still be using it. Bounded partition sizing caps this wait.
            try:
                if executor is not None:
                    executor.shutdown(wait=True, cancel_futures=True)
            finally:
                cleanup_partitions()

    return ArrowBatchStream(
        schema,
        produce(),
        close_callback=cleanup_partitions,
        cancel_event=session.cancel_event,
    )


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
    first_key_range=None,
    input_size_hint_bytes: int | None = None,
    input_rows_hint: int | None = None,
    parallelism: int = 1,
) -> ArrowBatchStream:
    """Sort Arrow batches with bounded runs and a bounded fan-in merge.

    Float keys are deliberately rejected because total NaN ordering differs
    among Arrow/Polars/DuckDB versions.  Callers must route such a query until
    one engine-wide ordering contract is sealed.

    A caller may additionally provide a snapshot-sealed ``first_key_range``
    plus input byte/row hints. For an integer first key that selects the
    query-private one-write range strategy and parallel Arrow C++ partition
    sorts. Calls without all required proofs retain the legacy bounded merge.
    """
    keys = _normalize_sort_keys(sort_keys)
    if null_placement not in {"at_start", "at_end"}:
        raise ValueError("null_placement must be 'at_start' or 'at_end'")
    if output_batch_rows <= 0 or max_open_runs < 2 or parallelism <= 0:
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

    integer_range = _normalize_integer_range(first_key_range)
    if input_size_hint_bytes is not None and input_size_hint_bytes < 0:
        raise ValueError("input_size_hint_bytes cannot be negative")
    if input_rows_hint is not None and input_rows_hint < 0:
        raise ValueError("input_rows_hint cannot be negative")
    first_type = schema.field(schema.get_field_index(keys[0].name)).type
    if (
        integer_range is not None
        and pa.types.is_integer(first_type)
        and input_size_hint_bytes is not None
        and input_size_hint_bytes > 0
        and input_rows_hint is not None
        and input_rows_hint >= 0
    ):
        return _integer_range_external_sort(
            batches,
            schema=schema,
            keys=keys,
            integer_range=integer_range,
            session=session,
            memory_budget_bytes=memory_budget_bytes,
            output_batch_rows=output_batch_rows,
            null_placement=null_placement,
            max_open_runs=max_open_runs,
            input_size_hint_bytes=int(input_size_hint_bytes),
            input_rows_hint=int(input_rows_hint),
            parallelism=int(parallelism),
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
        session._check_cancelled()
        table = pa.Table.from_batches(pending, schema=schema)
        indices = pc.sort_indices(table, sort_keys=arrow_keys, null_placement=null_placement)
        sorted_table = pc.take(table, indices)
        session._check_cancelled()
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
        while True:
            session._check_cancelled()
            try:
                batch = next(input_iterator)
            except StopIteration:
                break
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
                    session=session,
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
                    session=session,
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
    """Exact bounded GROUP BY with native partial aggregation before spill.

    Raw rows are *never* written directly to the group spill.  Bounded input
    chunks are first reduced by Arrow's native, threaded hash aggregators and
    their compact states are combined while those states fit the operator
    budget.  Only compact partial states cross the disk boundary.  This is an
    important distinction for wide, low-cardinality inputs: a ten-gigabyte
    scan with a thousand groups normally remains a few megabytes of state and
    consequently needs no spill at all.

    If the state cannot fit, compact states are externally sorted and reduced
    a second time.  That fallback remains bounded, quota checked, cancellable,
    and query private.  COUNT state is held as Decimal128 and integer/decimal
    SUM state is widened before the first reduction, avoiding Arrow's wrapping
    int64 hash-sum kernel.
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
    if output_batch_rows <= 0 or max_open_runs < 2:
        raise ValueError("invalid external-group batch/fan-in configuration")
    if memory_budget_bytes < 256 * 1024:
        raise SpillMemoryLimitExceeded(
            "external aggregation needs at least 256 KiB of workspace"
        )

    # Internal state names must not collide with either an input name or a SQL
    # output alias.  They are private to IPC files but a collision would make
    # positional Arrow aggregation dangerously ambiguous.
    reserved_names = set(schema.names) | set(output_names)
    state_names: list[str] = []
    for index in range(len(aggregates)):
        candidate = f"__island_aggregate_state_{index}"
        while candidate in reserved_names:
            candidate = f"_{candidate}"
        reserved_names.add(candidate)
        state_names.append(candidate)

    def state_type(spec: AggregateSpec) -> pa.DataType:
        function = spec.function.lower()
        if function in {"count", "count_star"}:
            # Merging int64 partial counts with hash_sum can wrap.  Decimal is
            # exact and the final safe cast enforces COUNT's output range.
            return pa.decimal128(38, 0)
        assert spec.input_column is not None
        source_type = schema.field(schema.get_field_index(spec.input_column)).type
        if function == "sum":
            if pa.types.is_integer(source_type):
                return pa.decimal128(38, 0)
            if pa.types.is_decimal128(source_type):
                return pa.decimal128(38, source_type.scale)
            return pa.decimal256(76, source_type.scale)
        return source_type

    state_types = [state_type(spec) for spec in aggregates]
    state_schema = pa.schema(
        [schema.field(schema.get_field_index(name)) for name in group_keys]
        + [
            pa.field(state_names[index], state_types[index])
            for index in range(len(aggregates))
        ]
    )

    # One eighth is input payload, with the remainder reserved for casts,
    # native hash tables, output buffers, the retained compact state, and the
    # producer's already-materialized RecordBatch.  The independent row cap
    # covers narrow/bit-packed data whose ``nbytes`` understates hash overhead.
    raw_target_bytes = max(64 * 1024, memory_budget_bytes // 8)
    raw_target_rows = max(1, memory_budget_bytes // 256)
    state_target_bytes = max(64 * 1024, memory_budget_bytes // 4)
    state_target_rows = max(1, memory_budget_bytes // 128)

    def fixed_state_width(dtype: pa.DataType) -> int:
        if pa.types.is_boolean(dtype) or pa.types.is_null(dtype):
            return 1
        if pa.types.is_fixed_size_binary(dtype):
            return dtype.byte_width
        if pa.types.is_decimal(dtype):
            return dtype.bit_width // 8
        if pa.types.is_primitive(dtype):
            return max(1, dtype.bit_width // 8)
        # Offsets/validity plus a conservative native scalar handle.  Actual
        # variable payload bytes are charged separately from the input array.
        return 24

    def raw_workspace_bound(table: pa.Table, *, resident_input_bytes: int) -> int:
        """Conservative pre-kernel bound for an all-distinct input chunk.

        Arrow does not expose a per-operation memory pool for hash_group_by.
        We therefore admit a native call only after charging the complete
        producer batch which owns the sliced buffers, copied group keys,
        aggregate hash state, result buffers, widening casts, and a generous
        per-group hash-table allowance.  Variable-width MIN/MAX inputs are
        charged once per alias, so duplicate aggregates cannot amplify memory
        behind a deceptively narrow input schema.
        """
        rows = table.num_rows
        total = int(resident_input_bytes) + rows * 128
        for name in group_keys:
            total += table.column(name).nbytes + rows * 8
        for index, spec in enumerate(aggregates):
            function = spec.function.lower()
            width = fixed_state_width(state_types[index])
            if function in {"count", "count_star"}:
                # Native int64 state/result plus the exact Decimal128 copy.
                total += rows * 32
            elif function == "sum":
                # Widened input, hash state, and output may coexist.
                total += rows * (3 * width + 8)
            else:
                assert spec.input_column is not None
                # Variable buffers may be copied into hash state and result.
                total += 2 * table.column(spec.input_column).nbytes + rows * 16
        return total

    def state_merge_workspace_bound(table: pa.Table) -> int:
        # Input compact buffers, hash state, and output can coexist.  The row
        # allowance covers hash buckets and per-aggregator native bookkeeping.
        fixed_state = sum(
            fixed_state_width(dtype) + 8
            for dtype in state_types
        )
        return 3 * table.nbytes + table.num_rows * (128 + fixed_state)

    def table_from_result(
        result: pa.Table,
        *,
        count_results_are_int64: bool,
    ) -> pa.Table:
        arrays: list[pa.ChunkedArray] = [
            result.column(index) for index in range(len(group_keys))
        ]
        for index, spec in enumerate(aggregates):
            values = result.column(len(group_keys) + index)
            if count_results_are_int64 and spec.function.lower() in {
                "count", "count_star",
            }:
                values = pc.cast(values, state_types[index], safe=True)
            elif not values.type.equals(state_types[index]):
                values = pc.cast(values, state_types[index], safe=True)
            arrays.append(values)
        return pa.Table.from_arrays(arrays, schema=state_schema)

    def ensure_exact_sum_bounds(
        table: pa.Table,
        columns: Sequence[tuple[object, pa.DataType]],
    ) -> None:
        """Reject a native decimal hash-sum that could silently wrap.

        Arrow 18's decimal hash_sum wraps its signed physical integer without
        raising once an intermediate exceeds the declared precision.  A cheap
        maximum-magnitude bound proves the safe common case.  Ambiguous extreme
        inputs fail closed instead of relying on cancellation to hide overflow.
        """
        for column_ref, dtype in columns:
            if not pa.types.is_decimal(dtype):
                continue
            column = table.column(column_ref)
            if not column.length() or column.null_count == column.length():
                continue
            extrema = pc.min_max(column).as_py()
            values = [extrema.get("min"), extrema.get("max")]
            maximum_unscaled = max(
                (
                    abs(int(value.scaleb(dtype.scale)))
                    for value in values
                    if value is not None
                ),
                default=0,
            )
            if maximum_unscaled * table.num_rows > 10**dtype.precision - 1:
                raise SpillNumericOverflow(
                    "bounded SUM cannot prove that the native decimal state "
                    f"fits {dtype}; refusing a potentially wrapping hash_sum"
                )

    def aggregate_raw(table: pa.Table) -> pa.Table:
        session._check_cancelled()
        working = table
        aggregate_functions: list[tuple[object, str]] = []
        for index, spec in enumerate(aggregates):
            function = spec.function.lower()
            if function == "count_star":
                aggregate_functions.append(([], "count_all"))
            elif function == "count":
                assert spec.input_column is not None
                aggregate_functions.append((spec.input_column, "count"))
            elif function == "sum":
                assert spec.input_column is not None
                cast_name = f"__island_sum_input_{index}"
                while working.schema.get_field_index(cast_name) >= 0:
                    cast_name = f"_{cast_name}"
                widened = pc.cast(
                    working.column(spec.input_column), state_types[index], safe=True,
                )
                working = working.append_column(cast_name, widened)
                aggregate_functions.append((cast_name, "sum"))
            else:
                assert spec.input_column is not None
                aggregate_functions.append((spec.input_column, function))
        ensure_exact_sum_bounds(
            working,
            [
                (aggregate_functions[index][0], state_types[index])
                for index, spec in enumerate(aggregates)
                if spec.function.lower() == "sum"
            ],
        )
        try:
            result = working.group_by(
                list(group_keys), use_threads=True,
            ).aggregate(aggregate_functions)
        except (pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
            raise UnsupportedSpillOperation(
                f"native bounded aggregation is not available: {exc}"
            ) from exc
        session._check_cancelled()
        return table_from_result(result, count_results_are_int64=True)

    def aggregate_states(table: pa.Table) -> pa.Table:
        session._check_cancelled()
        aggregate_functions = [
            (
                state_names[index],
                "sum" if spec.function.lower() in {"count", "count_star", "sum"}
                else spec.function.lower(),
            )
            for index, spec in enumerate(aggregates)
        ]
        ensure_exact_sum_bounds(
            table,
            [
                (state_names[index], state_types[index])
                for index, spec in enumerate(aggregates)
                if spec.function.lower() in {"count", "count_star", "sum"}
            ],
        )
        try:
            result = table.group_by(
                list(group_keys), use_threads=True,
            ).aggregate(aggregate_functions)
        except (pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
            raise UnsupportedSpillOperation(
                f"native compact-state aggregation is not available: {exc}"
            ) from exc
        session._check_cancelled()
        return table_from_result(result, count_results_are_int64=False)

    def sort_states(table: pa.Table) -> pa.Table:
        if table.num_rows <= 1:
            return table
        indices = pc.sort_indices(
            table,
            sort_keys=[(name, "ascending") for name in group_keys],
            null_placement="at_start",
        )
        return pc.take(table, indices)

    def finalize_states(table: pa.Table) -> pa.Table:
        arrays: list[pa.ChunkedArray] = [
            table.column(name) for name in group_keys
        ]
        for index, spec in enumerate(aggregates):
            values = table.column(state_names[index])
            target_type = output_schema.field(spec.output_name).type
            if not values.type.equals(target_type):
                values = pc.cast(values, target_type, safe=True)
            arrays.append(values)
        return pa.Table.from_arrays(arrays, schema=output_schema)

    partial_paths: list[Path] = []
    partial_number = 0

    def remove_partial_paths() -> None:
        for path in tuple(partial_paths):
            session.remove(path)
            partial_paths.remove(path)

    def spill_state(table: pa.Table) -> None:
        nonlocal partial_number
        if table.num_rows == 0:
            return
        name = f"group-partial-{partial_number:08d}.arrow"
        partial_number += 1
        # State tables admitted here are below the operator budget.  The row
        # chunk cap prevents one large IPC record batch from later defeating
        # external_sort's single-batch admission check.
        row_bytes = max(1, math.ceil(table.nbytes / max(1, table.num_rows)))
        batch_rows = max(
            1,
            min(output_batch_rows, memory_budget_bytes // row_bytes),
        )
        path = session.write_ipc_batches(
            name, state_schema, table.to_batches(max_chunksize=batch_rows),
        )
        partial_paths.append(path)

    # Keep compact partials as zero-copy table fragments and reduce them in a
    # native batch.  Reducing ``current + partial`` after every scan batch made
    # a 1,521-batch/1,024-group query invoke the same 30 aggregate kernels more
    # than 1,500 unnecessary times.  The retained fragments are charged below
    # and never cross the same hard state/workspace thresholds.
    state_fragments: list[pa.Table] = []
    state_fragment_bytes = 0
    state_fragment_rows = 0

    def spill_fragments() -> None:
        nonlocal state_fragments, state_fragment_bytes, state_fragment_rows
        if not state_fragments:
            return
        spill_state(
            state_fragments[0]
            if len(state_fragments) == 1
            else pa.concat_tables(state_fragments)
        )
        state_fragments = []
        state_fragment_bytes = 0
        state_fragment_rows = 0

    def compact_fragments(*, resident_input_bytes: int = 0) -> None:
        nonlocal state_fragments, state_fragment_bytes, state_fragment_rows
        if not state_fragments:
            return
        if len(state_fragments) == 1:
            compact = state_fragments[0]
        else:
            combined = pa.concat_tables(state_fragments)
            if (
                resident_input_bytes + state_merge_workspace_bound(combined)
                <= memory_budget_bytes
            ):
                compact = aggregate_states(combined)
            else:
                # The fragments are already exact compact states.  Spill them
                # as one chunked IPC input and let the bounded final merge
                # combine duplicates, rather than overcommitting native hash
                # memory or multiplying one file per producer batch.
                spill_fragments()
                return
        state_fragments = [compact]
        state_fragment_bytes = compact.nbytes
        state_fragment_rows = compact.num_rows
        if (
            state_fragment_bytes > state_target_bytes
            or state_fragment_rows > state_target_rows
        ):
            spill_fragments()

    def accept_partial(
        partial: pa.Table,
        *,
        resident_input_bytes: int = 0,
    ) -> None:
        nonlocal state_fragment_bytes, state_fragment_rows
        if partial.num_rows == 0:
            return
        if state_fragments:
            prospective = pa.concat_tables([*state_fragments, partial])
            if (
                resident_input_bytes + state_merge_workspace_bound(prospective)
                > memory_budget_bytes
            ):
                # Compact *before* one more fragment makes the native merge
                # inadmissible. The previous implementation waited for the
                # byte threshold, then had no legal choice except disk.
                compact_fragments(resident_input_bytes=resident_input_bytes)
                if state_fragments:
                    prospective = pa.concat_tables([*state_fragments, partial])
                    if (
                        resident_input_bytes
                        + state_merge_workspace_bound(prospective)
                        > memory_budget_bytes
                    ):
                        spill_fragments()
        if state_fragments and (
            state_fragment_bytes + partial.nbytes > state_target_bytes
            or state_fragment_rows + partial.num_rows > state_target_rows
        ):
            compact_fragments(resident_input_bytes=resident_input_bytes)

        state_fragments.append(partial)
        state_fragment_bytes += partial.nbytes
        state_fragment_rows += partial.num_rows
        if (
            state_fragment_bytes >= state_target_bytes
            or state_fragment_rows >= state_target_rows
        ):
            compact_fragments(resident_input_bytes=resident_input_bytes)

    pending: list[pa.RecordBatch] = []
    pending_bytes = 0
    pending_rows = 0
    pending_resident_bytes = 0

    def flush_pending() -> None:
        nonlocal pending, pending_bytes, pending_rows, pending_resident_bytes
        if not pending:
            return
        table = pa.Table.from_batches(pending, schema=schema)
        resident_input_bytes = pending_resident_bytes
        pending = []
        pending_bytes = 0
        pending_rows = 0
        pending_resident_bytes = 0

        def aggregate_admitted(piece: pa.Table) -> None:
            required = raw_workspace_bound(
                piece, resident_input_bytes=resident_input_bytes,
            )
            if required + state_fragment_bytes > memory_budget_bytes and state_fragments:
                compact_fragments(resident_input_bytes=resident_input_bytes)
                if required + state_fragment_bytes > memory_budget_bytes:
                    # Yield retained state to disk before recursively shrinking
                    # a raw chunk all the way to one row. This is essential for
                    # tiny forced-spill budgets and high-cardinality groups.
                    spill_fragments()
            if required + state_fragment_bytes <= memory_budget_bytes:
                partial = aggregate_raw(piece)
                accept_partial(
                    partial, resident_input_bytes=resident_input_bytes,
                )
                return
            if piece.num_rows <= 1:
                raise SpillMemoryLimitExceeded(
                    "one aggregate row/state exceeds the bounded native workspace"
                )
            midpoint = piece.num_rows // 2
            aggregate_admitted(piece.slice(0, midpoint))
            aggregate_admitted(piece.slice(midpoint))

        aggregate_admitted(table)

    input_iterator = iter(batches)
    try:
        while True:
            # Check before requesting the next producer batch as well as after
            # it returns.  This is the eager preparation path: no client-owned
            # result stream exists yet, so the production deadline must stop a
            # scan here rather than relying on client cancellation.
            session._check_cancelled()
            try:
                batch = next(input_iterator)
            except StopIteration:
                break
            session._check_cancelled()
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("external group input must yield RecordBatch objects")
            if not batch.schema.equals(schema, check_metadata=False):
                raise ValueError("external group input schema changed")
            if batch.nbytes > memory_budget_bytes:
                raise SpillMemoryLimitExceeded(
                    "one input RecordBatch exceeds the external-group memory budget"
                )
            # Accumulate small scan batches to the bounded raw target so one
            # threaded native reduction amortizes kernel setup across row
            # groups. Charge the full owning batch (not merely logical slices)
            # until the aggregate call completes.
            resident_target = max(raw_target_bytes, memory_budget_bytes // 2)
            if (
                pending
                and pending_resident_bytes + batch.nbytes > resident_target
            ):
                flush_pending()
            batch_charged_to_pending = False
            offset = 0
            while offset < batch.num_rows:
                remaining = batch.slice(offset)
                bytes_room = raw_target_bytes - pending_bytes
                rows_room = raw_target_rows - pending_rows
                if pending and (bytes_room <= 0 or rows_room <= 0):
                    flush_pending()
                    batch_charged_to_pending = False
                    continue
                rows = min(remaining.num_rows, max(1, rows_room))
                if remaining.nbytes > max(1, bytes_room):
                    rows = min(
                        rows,
                        max(
                            1,
                            math.floor(
                                remaining.num_rows
                                * max(1, bytes_room)
                                / remaining.nbytes
                            ),
                        ),
                    )
                piece = remaining.slice(0, rows)
                if pending and (
                    pending_bytes + piece.nbytes > raw_target_bytes
                    or pending_rows + piece.num_rows > raw_target_rows
                ):
                    flush_pending()
                    batch_charged_to_pending = False
                    continue
                if not batch_charged_to_pending:
                    pending_resident_bytes += batch.nbytes
                    batch_charged_to_pending = True
                pending.append(piece)
                pending_bytes += piece.nbytes
                pending_rows += piece.num_rows
                offset += piece.num_rows
                if (
                    pending_bytes >= raw_target_bytes
                    or pending_rows >= raw_target_rows
                ):
                    flush_pending()
                    batch_charged_to_pending = False
        flush_pending()
    except BaseException:
        remove_partial_paths()
        raise
    finally:
        close_input = getattr(input_iterator, "close", None)
        if callable(close_input):
            close_input()

    try:
        compact_fragments()
    except BaseException:
        remove_partial_paths()
        raise

    if not partial_paths:
        if not state_fragments:
            result = pa.Table.from_batches([], schema=output_schema)
        else:
            result = finalize_states(sort_states(state_fragments[0]))
        return ArrowBatchStream(
            output_schema,
            iter(result.to_batches(max_chunksize=output_batch_rows)),
            cancel_event=session.cancel_event,
        )

    if state_fragments:
        try:
            spill_state(state_fragments[0])
            state_fragments.clear()
        except BaseException:
            remove_partial_paths()
            raise

    def partial_batches() -> Iterator[pa.RecordBatch]:
        try:
            for path in tuple(partial_paths):
                session._check_cancelled()
                source = pa.memory_map(str(path), "r")
                try:
                    reader = pa.ipc.open_file(source)
                    for batch_index in range(reader.num_record_batches):
                        session._check_cancelled()
                        yield reader.get_batch(batch_index)
                finally:
                    source.close()
                session.remove(path)
                partial_paths.remove(path)
        finally:
            remove_partial_paths()

    try:
        sorted_stream = external_sort(
            partial_batches(),
            schema=state_schema,
            sort_keys=list(group_keys),
            session=session,
            memory_budget_bytes=memory_budget_bytes,
            output_batch_rows=output_batch_rows,
            null_placement="at_start",
            max_open_runs=max_open_runs,
        )
    except BaseException:
        remove_partial_paths()
        raise

    def produce_spilled() -> Iterator[pa.RecordBatch]:
        # Native reduction per sorted batch eliminates the old Python scalar
        # loop.  Retaining exactly one final group bridges groups split across
        # adjacent merge batches without retaining result-sized state.
        carry: pa.Table | None = None
        try:
            for batch in sorted_stream:
                state_batch = pa.Table.from_batches([batch], schema=state_schema)
                if carry is not None:
                    state_batch = pa.concat_tables([carry, state_batch])
                reduced = sort_states(aggregate_states(state_batch))
                if reduced.num_rows <= 1:
                    carry = reduced
                    continue
                completed = reduced.slice(0, reduced.num_rows - 1)
                carry = reduced.slice(reduced.num_rows - 1, 1)
                finalized = finalize_states(completed)
                yield from finalized.to_batches(max_chunksize=output_batch_rows)
            if carry is not None and carry.num_rows:
                yield from finalize_states(carry).to_batches(
                    max_chunksize=output_batch_rows,
                )
        finally:
            sorted_stream.close()

    return ArrowBatchStream(
        output_schema,
        produce_spilled(),
        close_callback=sorted_stream.close,
        cancel_event=session.cancel_event,
    )
