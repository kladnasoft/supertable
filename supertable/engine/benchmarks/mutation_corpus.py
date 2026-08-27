"""Deterministic insert/update/delete corpus for matched engine benchmarks."""

from __future__ import annotations

import hashlib
import datetime as dt
import json
import math
import os
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .corpus import (
    GIB,
    MIB,
    CorpusSpec,
    Workload,
    _arrow_schema,
    _fixed_binary_array,
    _manifest_schema,
    _write_json_atomic,
    _write_one_file,
    load_manifest,
    sha256_file,
)


MUTATION_CORPUS_FORMAT_VERSION = 1
MUTATION_GENERATOR_NAME = "islanddb-mutation-v3"
MUTATION_WORKLOAD_KIND = "mutation-pruning-v1"
MUTATION_CONSTRUCTION = {
    "mode": "deterministic_parquet_snapshot_simulation",
    "production_mutation_api_used": False,
    "production_compaction_executed": False,
    "tombstone_threshold_was_reached": False,
}
MUTATION_WORKLOAD_NAMES = tuple(
    f"prune_{percent}pct" for percent in (99, 90, 50, 10, 1)
)


def _split_rows(total: int, count: int) -> tuple[int, ...]:
    quotient, remainder = divmod(int(total), int(count))
    return tuple(
        quotient + (1 if index < remainder else 0)
        for index in range(count)
    )


@dataclass(frozen=True)
class MutationCorpusSpec:
    """Complete mutation schedule and physical shape of one corpus."""

    base_rows: int = 11_000_000
    base_files: int = 128
    updated_rows: int = 250_000
    update_operations: int = 5
    deleted_rows: int = 749_999
    delete_operations: int = 9
    tombstone_threshold: int = 1_000_000
    minimum_live_rows: int = 10_000_001
    minimum_snapshot_files: int = 100
    seed: int = 20260827
    payload_columns: int = 3
    payload_width: int = 16
    batch_rows: int = 16_384
    row_group_target_bytes: int = 8 * MIB
    compression: str = "zstd"
    compression_level: int = 1

    def __post_init__(self) -> None:
        positive = (
            self.base_rows,
            self.base_files,
            self.updated_rows,
            self.update_operations,
            self.deleted_rows,
            self.delete_operations,
            self.tombstone_threshold,
            self.minimum_live_rows,
            self.minimum_snapshot_files,
            self.payload_columns,
            self.payload_width,
            self.batch_rows,
            self.row_group_target_bytes,
        )
        if any(int(value) <= 0 for value in positive):
            raise ValueError("mutation corpus dimensions must be positive")
        if (
            self.base_files < 2
            or self.update_operations < 2
            or self.delete_operations < 2
        ):
            raise ValueError(
                "mutation corpus requires at least two insert, update, and "
                "delete operations"
            )
        if self.update_operations + self.delete_operations > self.base_files:
            raise ValueError("mutation operations require distinct source files")
        if self.base_rows < self.base_files:
            raise ValueError("every insert file must contain at least one row")
        if self.updated_rows < self.update_operations:
            raise ValueError("every update operation must contain at least one row")
        if self.deleted_rows < self.delete_operations:
            raise ValueError("every delete operation must contain at least one row")
        if self.tombstone_rows >= self.tombstone_threshold:
            raise ValueError("mutation corpus must stay below compaction threshold")
        if self.live_rows < self.minimum_live_rows:
            raise ValueError("mutation corpus does not meet its live-row minimum")
        if self.snapshot_files < self.minimum_snapshot_files:
            raise ValueError("mutation corpus does not meet its snapshot-file minimum")
        base_allocations = _split_rows(self.base_rows, self.base_files)
        update_allocations = _split_rows(
            self.updated_rows, self.update_operations,
        )
        delete_allocations = _split_rows(
            self.deleted_rows, self.delete_operations,
        )
        for operation, rows in enumerate(update_allocations):
            if rows >= base_allocations[operation]:
                raise ValueError("an update would consume a complete source file")
        for offset, rows in enumerate(delete_allocations):
            source_index = self.update_operations + offset
            if rows >= base_allocations[source_index]:
                raise ValueError("a delete would consume a complete source file")

    @property
    def tombstone_rows(self) -> int:
        return int(self.updated_rows + self.deleted_rows)

    @property
    def physical_rows(self) -> int:
        return int(self.base_rows + self.updated_rows)

    @property
    def live_rows(self) -> int:
        return int(self.base_rows - self.deleted_rows)

    @property
    def snapshot_files(self) -> int:
        return int(self.base_files + self.update_operations)

    @property
    def corpus_id(self) -> str:
        stable = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return f"{MUTATION_GENERATOR_NAME}-{hashlib.sha256(stable.encode()).hexdigest()[:12]}"

    def wide_spec(self) -> CorpusSpec:
        return CorpusSpec(
            tier="mutation-1gib",
            target_bytes=GIB,
            seed=self.seed,
            payload_columns=self.payload_columns,
            payload_width=self.payload_width,
            batch_rows=self.batch_rows,
            row_group_target_bytes=self.row_group_target_bytes,
            compression=self.compression,
            compression_level=self.compression_level,
        )


def _resource_key(spec: MutationCorpusSpec, filename: str) -> str:
    # Deletion vectors store this value in ``__file__`` and match it against
    # the snapshot's stable object key.  It is a logical storage key, not the
    # host/container path used to open the Parquet object.
    return f"benchmark/{spec.corpus_id}/{filename}"


def _random_range(
    *, domain_rows: int, seed: int, prune_percent: int,
) -> tuple[int, int]:
    selected_rows = max(
        1, math.ceil(int(domain_rows) * (100 - int(prune_percent)) / 100)
    )
    max_start = max(0, int(domain_rows) - selected_rows)
    digest = hashlib.sha256(
        f"{int(seed)}:{int(prune_percent)}".encode("ascii")
    ).digest()
    lower = int.from_bytes(digest[:8], "big") % (max_start + 1)
    return lower, lower + selected_rows


def _mutation_source_schedule(
    spec: MutationCorpusSpec,
) -> tuple[list[tuple[int, int]], list[int]]:
    """Spread mutations while forcing every benchmark range to hit one."""
    allocations = _split_rows(spec.base_rows, spec.base_files)
    starts = []
    cursor = 0
    for rows in allocations:
        starts.append(cursor)
        cursor += rows

    def source_for_id(value: int) -> int:
        for index, (start, rows) in enumerate(zip(starts, allocations)):
            if start <= value < start + rows:
                return index
        return len(allocations) - 1

    update_sources: list[tuple[int, int]] = []
    used: set[int] = set()
    for prune_percent in (99, 90, 50, 10, 1):
        lower, _upper = _random_range(
            domain_rows=spec.base_rows,
            seed=spec.seed,
            prune_percent=prune_percent,
        )
        source_index = source_for_id(lower)
        if source_index in used:
            continue
        update_sources.append((source_index, lower))
        used.add(source_index)
        if len(update_sources) == spec.update_operations:
            break

    deterministic_order = sorted(
        range(spec.base_files),
        key=lambda index: hashlib.sha256(
            f"{spec.seed}:mutation-source:{index}".encode("ascii")
        ).digest(),
    )
    for source_index in deterministic_order:
        if len(update_sources) == spec.update_operations:
            break
        if source_index not in used:
            update_sources.append((source_index, starts[source_index]))
            used.add(source_index)
    delete_sources = [
        source_index for source_index in deterministic_order
        if source_index not in used
    ][:spec.delete_operations]
    if (
        len(update_sources) != spec.update_operations
        or len(delete_sources) != spec.delete_operations
    ):
        raise ValueError("mutation schedule cannot select distinct source files")
    return update_sources, delete_sources


def _floor_sum(n: int, modulus: int, multiplier: int, offset: int) -> int:
    """Return sum(floor((multiplier*i+offset)/modulus)), i in [0,n)."""
    answer = 0
    while True:
        if multiplier >= modulus:
            answer += (n - 1) * n * (multiplier // modulus) // 2
            multiplier %= modulus
        if offset >= modulus:
            answer += n * (offset // modulus)
            offset %= modulus
        top = multiplier * n + offset
        if top < modulus:
            return answer
        n, offset = top // modulus, top % modulus
        multiplier, modulus = modulus, multiplier


def _metric_sum(lower: int, upper: int, offset: int) -> int:
    if upper <= lower:
        return 0
    count = upper - lower
    multiplier = 48_271
    modulus = 1_000_003
    shifted = multiplier * lower + offset
    unreduced = (
        multiplier * count * (count - 1) // 2
        + shifted * count
    )
    return unreduced - modulus * _floor_sum(
        count, modulus, multiplier, shifted,
    )


def _mutation_oracle(
    manifest: Mapping[str, Any], lower: int, upper: int,
) -> dict[str, Any]:
    operations = [
        item for item in manifest.get("operations") or []
        if isinstance(item, Mapping)
    ]
    deletes = []
    for item in operations:
        if item.get("kind") == "delete":
            first = int(item["first_id"])
            deletes.append((first, first + int(item["rows"])))
    survivors = [(lower, upper)]
    for deleted_lower, deleted_upper in sorted(deletes):
        next_survivors = []
        for start, end in survivors:
            if deleted_upper <= start or deleted_lower >= end:
                next_survivors.append((start, end))
                continue
            if start < deleted_lower:
                next_survivors.append((start, deleted_lower))
            if deleted_upper < end:
                next_survivors.append((deleted_upper, end))
        survivors = next_survivors
    row_count = sum(end - start for start, end in survivors)
    metric_sum = sum(_metric_sum(start, end, 17) for start, end in survivors)
    for item in operations:
        if item.get("kind") != "update":
            continue
        first = max(lower, int(item["first_id"]))
        last = min(upper, int(item["first_id"]) + int(item["rows"]))
        if last <= first:
            continue
        operation = int(item["operation"])
        metric_sum -= _metric_sum(first, last, 17)
        metric_sum += _metric_sum(
            first, last, 17 + operation * 104_729,
        )
    if not survivors:
        raise RuntimeError("mutation benchmark range has no live rows")
    id_max = max(end - 1 for _start, end in survivors)
    dimension_max = max(
        1023
        if end - start >= 1024 or start // 1024 != (end - 1) // 1024
        else (end - 1) % 1024
        for start, end in survivors
    )
    event_us = 1_700_000_000_000_000 + id_max * 1_000
    event_value = (
        dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=event_us)
    ).isoformat(timespec="microseconds")
    return {
        "kind": "mutation_metric_formula_v1",
        "columns": [
            "row_count", "metric_sum", "id_max",
            "event_ts_max", "dimension_max",
        ],
        "dtypes": [
            "int64", "object", "int64", "datetime64[us]", "int32",
        ],
        "row": [
            row_count,
            {"$decimal": str(metric_sum)},
            id_max,
            {"$datetime": event_value},
            dimension_max,
        ],
    }


def _write_update_file(
    destination: Path,
    *,
    wide_spec: CorpusSpec,
    first_id: int,
    rows: int,
    first_rowid: int,
    operation_index: int,
    row_group_rows: int,
    rng,
) -> dict[str, Any]:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = _arrow_schema(wide_spec)
    writer = pq.ParquetWriter(
        destination,
        schema,
        compression=wide_spec.compression,
        compression_level=wide_spec.compression_level,
        use_dictionary=False,
        write_statistics=True,
    )
    written = 0
    try:
        while written < rows:
            count = min(wide_spec.batch_rows, rows - written)
            ids = np.arange(
                first_id + written, first_id + written + count,
                dtype=np.int64,
            )
            rowids = np.arange(
                first_rowid + written, first_rowid + written + count,
                dtype=np.int64,
            )
            metric = (
                ids * np.int64(48_271)
                + np.int64(17 + (operation_index + 1) * 104_729)
            ) % np.int64(1_000_003)
            base_us = np.int64(1_800_000_000_000_000)
            arrays = [
                pa.array(ids, type=pa.int64()),
                pa.array(
                    np.int64(1_700_000_000_000_000) + ids * np.int64(1_000),
                    type=pa.timestamp("us"),
                ),
                pa.array(metric, type=pa.int64()),
                pa.array((ids % np.int64(1_024)).astype(np.int32), type=pa.int32()),
            ]
            for _ in range(wide_spec.payload_columns):
                arrays.append(_fixed_binary_array(
                    rng.bytes(count * wide_spec.payload_width),
                    count,
                    wide_spec.payload_width,
                ))
            arrays.extend([
                pa.array(rowids, type=pa.int64()),
                pa.array(base_us + rowids, type=pa.timestamp("us")),
            ])
            writer.write_batch(
                pa.RecordBatch.from_arrays(arrays, schema=schema),
                row_group_size=row_group_rows,
            )
            written += count
    finally:
        writer.close()
    metadata = pq.ParquetFile(destination).metadata
    return {
        "path": destination.name,
        "bytes": int(destination.stat().st_size),
        "rows": int(rows),
        "min_id": int(first_id),
        "max_id": int(first_id + rows - 1),
        "row_groups": int(metadata.num_row_groups),
        "mutation": "update",
    }


def _write_tombstone(
    destination: Path,
    segments: list[tuple[str, int, int]],
    *,
    batch_rows: int,
) -> dict[str, Any]:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("__file__", pa.string(), nullable=False),
        pa.field("__rowid__", pa.int64(), nullable=False),
    ])
    writer = pq.ParquetWriter(
        destination,
        schema,
        compression="zstd",
        compression_level=1,
        use_dictionary=True,
        write_statistics=False,
    )
    total = 0
    try:
        for resource_key, first_rowid, rows in segments:
            written = 0
            while written < rows:
                count = min(batch_rows, rows - written)
                writer.write_batch(pa.record_batch([
                    pa.array([resource_key] * count, type=pa.string()),
                    pa.array(
                        np.arange(
                            first_rowid + written,
                            first_rowid + written + count,
                            dtype=np.int64,
                        ),
                        type=pa.int64(),
                    ),
                ], schema=schema))
                written += count
                total += count
    finally:
        writer.close()
    return {
        "path": destination.name,
        "bytes": int(destination.stat().st_size),
        "rows": int(total),
        "sha256": sha256_file(destination),
        "format": 3,
    }


def validate_mutation_manifest(
    manifest: Mapping[str, Any],
    expected_spec: MutationCorpusSpec | None = None,
) -> list[str]:
    problems: list[str] = []
    if manifest.get("format_version") != MUTATION_CORPUS_FORMAT_VERSION:
        problems.append("unsupported mutation corpus format")
    if manifest.get("generator") != MUTATION_GENERATOR_NAME:
        problems.append("unexpected mutation corpus generator")
    if manifest.get("construction") != MUTATION_CONSTRUCTION:
        problems.append("mutation corpus construction provenance is invalid")
    if expected_spec is not None and manifest.get("mutation_spec") != asdict(
        expected_spec
    ):
        problems.append("mutation corpus spec differs from request")
    if expected_spec is not None:
        expected_fields = {
            "total_rows": expected_spec.physical_rows,
            "physical_rows": expected_spec.physical_rows,
            "id_domain_rows": expected_spec.base_rows,
            "live_rows": expected_spec.live_rows,
            "tombstone_rows": expected_spec.tombstone_rows,
            "tombstone_threshold": expected_spec.tombstone_threshold,
            "minimum_live_rows": expected_spec.minimum_live_rows,
            "minimum_snapshot_files": expected_spec.minimum_snapshot_files,
            "snapshot_version": (
                expected_spec.base_files
                + expected_spec.update_operations
                + expected_spec.delete_operations
            ),
        }
        for field, expected in expected_fields.items():
            if manifest.get(field) != expected:
                problems.append(f"derived mutation field differs: {field}")
        if manifest.get("spec") != expected_spec.wide_spec().manifest_spec():
            problems.append("derived physical schema differs from mutation spec")
    raw_manifest_path = manifest.get("manifest_path")
    if not raw_manifest_path:
        problems.append("manifest path is unavailable")
        return problems
    base = Path(str(raw_manifest_path)).parent
    rows = 0
    source_bytes = 0
    source_names: set[str] = set()
    resource_keys: set[str] = set()
    source_resource_by_name: dict[str, str] = {}
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        problems.append("PyArrow is unavailable for source integrity validation")
        return problems
    for entry in manifest.get("files") or []:
        if not isinstance(entry, Mapping):
            problems.append("source entry is malformed")
            continue
        name = str(entry.get("path") or "")
        if (
            not name
            or Path(name).is_absolute()
            or len(Path(name).parts) != 1
            or name in source_names
        ):
            problems.append(f"invalid or duplicate source path: {name!r}")
            continue
        source_names.add(name)
        resource_key = str(entry.get("resource_key") or "")
        if (
            not resource_key
            or resource_key.startswith("/")
            or "\x00" in resource_key
            or any(part in ("", ".", "..") for part in resource_key.split("/"))
            or not resource_key.endswith(".parquet")
            or resource_key in resource_keys
        ):
            problems.append(f"invalid or duplicate resource key: {resource_key!r}")
        resource_keys.add(resource_key)
        source_resource_by_name[name] = resource_key
        path = base / name
        if not path.is_file():
            problems.append(f"missing source file: {entry.get('path')}")
            continue
        size = int(path.stat().st_size)
        if size != int(entry.get("bytes", -1)):
            problems.append(f"source size mismatch: {entry.get('path')}")
        if sha256_file(path) != entry.get("sha256"):
            problems.append(f"source digest mismatch: {entry.get('path')}")
        try:
            parquet = pq.ParquetFile(path)
            metadata = parquet.metadata
            schema_names = [
                metadata.schema.column(index).name
                for index in range(metadata.num_columns)
            ]
            id_index = schema_names.index("id")
            id_minima = []
            id_maxima = []
            for row_group_index in range(metadata.num_row_groups):
                statistics = metadata.row_group(row_group_index).column(
                    id_index
                ).statistics
                if statistics is None or not statistics.has_min_max:
                    raise ValueError("id statistics are unavailable")
                id_minima.append(int(statistics.min))
                id_maxima.append(int(statistics.max))
            if metadata.num_rows != int(entry.get("rows", -1)):
                problems.append(f"source row count mismatch: {entry.get('path')}")
            if metadata.num_row_groups != int(entry.get("row_groups", -1)):
                problems.append(f"source row-group mismatch: {entry.get('path')}")
            if min(id_minima) != int(entry.get("min_id", -1)):
                problems.append(f"source minimum id mismatch: {entry.get('path')}")
            if max(id_maxima) != int(entry.get("max_id", -1)):
                problems.append(f"source maximum id mismatch: {entry.get('path')}")
        except (OSError, ValueError, IndexError, TypeError):
            problems.append(f"source Parquet metadata invalid: {entry.get('path')}")
        rows += int(entry.get("rows") or 0)
        source_bytes += size
    operations = list(manifest.get("operations") or [])
    tombstone = dict(manifest.get("tombstone") or {})
    tombstone_name = str(tombstone.get("path") or "")
    tombstone_path = base / tombstone_name
    tombstone_path_valid = bool(
        tombstone_name
        and not Path(tombstone_name).is_absolute()
        and len(Path(tombstone_name).parts) == 1
        and tombstone_name not in source_names
    )
    if not tombstone_path_valid:
        problems.append("tombstone artifact path is invalid")
    elif not tombstone_path.is_file():
        problems.append("tombstone artifact is missing")
    else:
        if int(tombstone.get("bytes", -1)) != int(tombstone_path.stat().st_size):
            problems.append("tombstone size mismatch")
        if sha256_file(tombstone_path) != tombstone.get("sha256"):
            problems.append("tombstone digest mismatch")
        if tombstone.get("format") != 3:
            problems.append("tombstone format is invalid")
        cache_key = str(tombstone.get("cache_key") or "")
        if (
            not cache_key
            or cache_key.startswith("/")
            or "\x00" in cache_key
            or any(part in ("", ".", "..") for part in cache_key.split("/"))
            or cache_key in resource_keys
        ):
            problems.append("tombstone cache key is invalid")
        expected_ranges: dict[str, list[tuple[int, int]]] = {}
        for operation in operations:
            if not isinstance(operation, Mapping) or operation.get("kind") not in {
                "update", "delete",
            }:
                continue
            resource_key = source_resource_by_name.get(
                str(operation.get("source_file") or "")
            )
            try:
                first_rowid = int(operation["first_id"]) + 1
                operation_rows = int(operation["rows"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if resource_key and operation_rows > 0:
                expected_ranges.setdefault(resource_key, []).append(
                    (first_rowid, first_rowid + operation_rows)
                )
        try:
            tombstone_parquet = pq.ParquetFile(tombstone_path)
            expected_schema = pa.schema([
                pa.field("__file__", pa.string(), nullable=False),
                pa.field("__rowid__", pa.int64(), nullable=False),
            ])
            if tombstone_parquet.schema_arrow != expected_schema:
                problems.append("tombstone Parquet schema is invalid")
            if tombstone_parquet.metadata.num_rows != int(
                tombstone.get("rows", -1)
            ):
                problems.append("tombstone physical row count mismatch")
            observed_rows = 0
            seen_rowids: set[int] = set()
            invalid_domain = False
            duplicate_rowid = False
            for batch in tombstone_parquet.iter_batches(
                batch_size=65_536,
                columns=["__file__", "__rowid__"],
            ):
                for resource_key, rowid in zip(
                    batch.column(0).to_pylist(),
                    batch.column(1).to_pylist(),
                ):
                    observed_rows += 1
                    if resource_key is None or rowid is None:
                        invalid_domain = True
                        continue
                    rowid = int(rowid)
                    ranges = expected_ranges.get(str(resource_key), ())
                    if not any(first <= rowid < last for first, last in ranges):
                        invalid_domain = True
                    if rowid in seen_rowids:
                        duplicate_rowid = True
                    seen_rowids.add(rowid)
            if observed_rows != int(tombstone.get("rows", -1)):
                problems.append("tombstone scanned row count mismatch")
            if invalid_domain:
                problems.append("tombstone key/row-ID domain is invalid")
            if duplicate_rowid:
                problems.append("tombstone row IDs are not unique")
        except (OSError, ValueError, TypeError, KeyError, IndexError):
            problems.append("tombstone Parquet content is invalid")
    if rows != int(manifest.get("physical_rows", -1)):
        problems.append("physical row total is inconsistent")
    if source_bytes != int(manifest.get("actual_source_bytes", -1)):
        problems.append("physical byte total is inconsistent")
    tombstone_rows = int(manifest.get("tombstone_rows", -1))
    if tombstone_rows != int(tombstone.get("rows", -2)):
        problems.append("tombstone row total is inconsistent")
    if tombstone_rows >= int(manifest.get("tombstone_threshold", 0)):
        problems.append("tombstone threshold was reached")
    if rows - tombstone_rows != int(manifest.get("live_rows", -1)):
        problems.append("live row arithmetic is inconsistent")
    if len(manifest.get("files") or []) < int(
        manifest.get("minimum_snapshot_files", 0)
    ):
        problems.append("snapshot file minimum was not met")
    operation_kinds = [
        str(item.get("kind")) for item in manifest.get("operations") or []
        if isinstance(item, Mapping)
    ]
    for kind in ("insert", "update", "delete"):
        if operation_kinds.count(kind) < 2:
            problems.append(f"mutation schedule lacks multiple {kind} operations")
    if int(manifest.get("snapshot_version", -1)) != len(operations):
        problems.append("snapshot version differs from mutation operations")
    expected_operation_counts = (
        {
            "insert": expected_spec.base_files,
            "update": expected_spec.update_operations,
            "delete": expected_spec.delete_operations,
        }
        if expected_spec is not None else None
    )
    if expected_operation_counts is not None:
        for kind, count in expected_operation_counts.items():
            selected = [
                item for item in operations
                if isinstance(item, Mapping) and item.get("kind") == kind
            ]
            if len(selected) != count:
                problems.append(f"mutation operation count differs: {kind}")
            if any(
                not isinstance(item.get("rows"), int)
                or isinstance(item.get("rows"), bool)
                or int(item["rows"]) <= 0
                for item in selected
            ):
                problems.append(f"mutation operation rows are invalid: {kind}")
        expected_row_totals = {
            "insert": expected_spec.base_rows,
            "update": expected_spec.updated_rows,
            "delete": expected_spec.deleted_rows,
        }
        for kind, expected in expected_row_totals.items():
            observed = sum(
                int(item.get("rows") or 0)
                for item in operations
                if isinstance(item, Mapping) and item.get("kind") == kind
            )
            if observed != expected:
                problems.append(f"mutation operation rows differ: {kind}")
    progress = [
        int(item.get("tombstone_rows_after", -1))
        for item in operations
        if isinstance(item, Mapping) and item.get("kind") in ("update", "delete")
    ]
    if (
        progress != sorted(progress)
        or len(set(progress)) != len(progress)
        or (progress and progress[-1] != tombstone_rows)
    ):
        problems.append("tombstone operation progress is inconsistent")
    return problems


def prepare_mutation_corpus(
    root: str | Path,
    spec: MutationCorpusSpec,
) -> dict[str, Any]:
    """Generate and atomically publish a bounded mutation-equivalent snapshot."""
    import numpy as np

    root_path = Path(root).expanduser().resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    destination = root_path / spec.corpus_id
    manifest_path = destination / "manifest.json"
    if manifest_path.is_file():
        existing = load_manifest(manifest_path)
        problems = validate_mutation_manifest(existing, spec)
        if problems:
            raise RuntimeError("existing mutation corpus is invalid: " + "; ".join(problems))
        existing["reused"] = True
        return existing
    if destination.exists():
        raise RuntimeError("mutation corpus destination exists without a valid manifest")

    stage = Path(tempfile.mkdtemp(prefix=f".{spec.corpus_id}.", dir=root_path))
    wide_spec = spec.wide_spec()
    rng = np.random.Generator(np.random.PCG64(spec.seed))
    row_group_rows = max(
        1, int(spec.row_group_target_bytes / wide_spec.approximate_row_bytes)
    )
    files: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    tombstone_segments: list[tuple[str, int, int]] = []
    try:
        first_id = 0
        base_allocations = _split_rows(spec.base_rows, spec.base_files)
        for index, rows in enumerate(base_allocations):
            name = f"insert-{index:03d}.parquet"
            entry = _write_one_file(
                stage / name,
                wide_spec,
                start_id=first_id,
                rows=rows,
                row_group_rows=row_group_rows,
                rng=rng,
            )
            entry["mutation"] = "insert"
            entry["resource_key"] = _resource_key(spec, name)
            entry["sha256"] = sha256_file(stage / name)
            files.append(entry)
            operations.append({
                "kind": "insert",
                "operation": index + 1,
                "rows": int(rows),
                "file": name,
                "tombstone_rows_after": 0,
            })
            first_id += rows

        next_rowid = spec.base_rows + 1
        tombstone_total = 0
        update_allocations = _split_rows(
            spec.updated_rows, spec.update_operations,
        )
        update_sources, delete_sources = _mutation_source_schedule(spec)
        for index, rows in enumerate(update_allocations):
            source_index, target_id = update_sources[index]
            source = files[source_index]
            first_update_id = min(
                max(int(source["min_id"]), int(target_id)),
                int(source["max_id"]) - rows + 1,
            )
            name = f"update-{index:03d}.parquet"
            entry = _write_update_file(
                stage / name,
                wide_spec=wide_spec,
                first_id=first_update_id,
                rows=rows,
                first_rowid=next_rowid,
                operation_index=index,
                row_group_rows=row_group_rows,
                rng=rng,
            )
            entry["resource_key"] = _resource_key(spec, name)
            entry["sha256"] = sha256_file(stage / name)
            files.append(entry)
            tombstone_segments.append((
                str(source["resource_key"]), first_update_id + 1, rows,
            ))
            tombstone_total += rows
            operations.append({
                "kind": "update",
                "operation": index + 1,
                "rows": int(rows),
                "source_file": source["path"],
                "source_index": int(source_index),
                "first_id": int(first_update_id),
                "file": name,
                "tombstone_rows_after": int(tombstone_total),
            })
            next_rowid += rows

        delete_allocations = _split_rows(
            spec.deleted_rows, spec.delete_operations,
        )
        for index, rows in enumerate(delete_allocations):
            source_index = delete_sources[index]
            source = files[source_index]
            first_delete_id = int(source["min_id"])
            tombstone_segments.append((
                str(source["resource_key"]), first_delete_id + 1, rows,
            ))
            tombstone_total += rows
            if tombstone_total >= spec.tombstone_threshold:
                raise RuntimeError("mutation schedule reached compaction threshold")
            operations.append({
                "kind": "delete",
                "operation": index + 1,
                "rows": int(rows),
                "source_file": source["path"],
                "source_index": int(source_index),
                "first_id": int(first_delete_id),
                "tombstone_rows_after": int(tombstone_total),
            })

        tombstone = _write_tombstone(
            stage / "tombstone-v3.parquet",
            tombstone_segments,
            batch_rows=spec.batch_rows,
        )
        tombstone["cache_key"] = _resource_key(
            spec, "tombstone-v3.parquet",
        )
        if tombstone_total != spec.tombstone_rows or tombstone["rows"] != spec.tombstone_rows:
            raise RuntimeError("mutation tombstone generation was not exact")
        actual_source_bytes = sum(int(entry["bytes"]) for entry in files)
        manifest = {
            "format_version": MUTATION_CORPUS_FORMAT_VERSION,
            "generator": MUTATION_GENERATOR_NAME,
            "workload_kind": MUTATION_WORKLOAD_KIND,
            "construction": dict(MUTATION_CONSTRUCTION),
            "generated_unix_ms": int(time.time() * 1000),
            "mutation_spec": asdict(spec),
            "spec": wide_spec.manifest_spec(),
            "table": "events",
            "super_name": "island_mutation_benchmark",
            "target_source_bytes": GIB,
            "actual_source_bytes": int(actual_source_bytes),
            "total_rows": int(spec.physical_rows),
            "physical_rows": int(spec.physical_rows),
            "id_domain_rows": int(spec.base_rows),
            "live_rows": int(spec.live_rows),
            "tombstone_rows": int(spec.tombstone_rows),
            "tombstone_threshold": int(spec.tombstone_threshold),
            "minimum_live_rows": int(spec.minimum_live_rows),
            "minimum_snapshot_files": int(spec.minimum_snapshot_files),
            "snapshot_version": len(operations),
            "schema": _manifest_schema(wide_spec),
            "files": files,
            "operations": operations,
            "tombstone": tombstone,
        }
        _write_json_atomic(stage / "manifest.json", manifest)
        staged = load_manifest(stage / "manifest.json")
        problems = validate_mutation_manifest(staged, spec)
        if problems:
            raise RuntimeError("generated mutation corpus is invalid: " + "; ".join(problems))
        os.replace(stage, destination)
        result = load_manifest(manifest_path)
        result["reused"] = False
        return result
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def build_mutation_workloads(manifest: Mapping[str, Any]) -> dict[str, Workload]:
    """Build equal-shape random ranges at five explicit pruning levels."""
    domain_rows = int(manifest["id_domain_rows"])
    required = (
        "id", "event_ts", "metric", "dimension", "__rowid__",
    )
    reductions = [
        "COUNT(*) AS row_count",
        "SUM(metric) AS metric_sum",
        "MAX(id) AS id_max",
        "MAX(event_ts) AS event_ts_max",
        "MAX(dimension) AS dimension_max",
    ]
    seed = int(manifest["mutation_spec"]["seed"])
    workloads: dict[str, Workload] = {}
    operations = [
        operation for operation in manifest.get("operations") or []
        if isinstance(operation, Mapping)
    ]

    def matching_rows(kind: str, lower: int, upper: int) -> int:
        total = 0
        for operation in operations:
            if operation.get("kind") != kind:
                continue
            first = int(operation.get("first_id") or 0)
            last = first + int(operation.get("rows") or 0)
            total += max(0, min(upper, last) - max(lower, first))
        return total

    for prune_percent in (99, 90, 50, 10, 1):
        selected_percent = 100 - prune_percent
        lower, upper = _random_range(
            domain_rows=domain_rows,
            seed=seed,
            prune_percent=prune_percent,
        )
        name = f"prune_{prune_percent}pct"
        updated_in_range = matching_rows("update", lower, upper)
        deleted_in_range = matching_rows("delete", lower, upper)
        selected_live_rows = (upper - lower) - deleted_in_range
        selected_live_percent = (
            100.0 * selected_live_rows / int(manifest["live_rows"])
        )
        workloads[name] = Workload(
            name=name,
            sql=(
                f"SELECT {', '.join(reductions)} FROM events "
                f"WHERE id >= {lower} AND id < {upper}"
            ),
            lower_id=lower,
            upper_id=upper,
            required_columns=required,
            arrow_stream_result=True,
            island_streaming_result=True,
            prune_percent=float(prune_percent),
            selected_percent=float(selected_percent),
            selection_basis="random_contiguous_base_id_domain_width",
            selected_live_rows=int(selected_live_rows),
            selected_live_percent=float(selected_live_percent),
            pruned_live_percent=float(100.0 - selected_live_percent),
            matched_update_rows=int(updated_in_range),
            matched_delete_rows=int(deleted_in_range),
            independent_oracle_kind="mutation_metric_formula_v1",
            independent_oracle=_mutation_oracle(manifest, lower, upper),
        )
    return workloads


def normalize_mutation_workloads(values) -> list[str]:
    selected: list[str] = []
    valid = set(MUTATION_WORKLOAD_NAMES)
    for raw in values:
        for name in str(raw).split(","):
            name = name.strip().lower()
            if not name:
                continue
            if name not in valid:
                raise ValueError(f"unknown mutation workload {name!r}")
            if name not in selected:
                selected.append(name)
    return selected


__all__ = [
    "MUTATION_GENERATOR_NAME",
    "MUTATION_WORKLOAD_KIND",
    "MUTATION_WORKLOAD_NAMES",
    "MutationCorpusSpec",
    "build_mutation_workloads",
    "normalize_mutation_workloads",
    "prepare_mutation_corpus",
    "validate_mutation_manifest",
]
