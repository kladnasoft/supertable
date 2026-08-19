"""Deterministic Parquet corpus generation and scan-size estimation.

The generated table is intentionally wide.  Benchmark queries reference a
small numeric subset while several fixed-width, high-entropy payload columns
make the original Parquet footprint substantial.  This makes projection and
row-group pushdown visible without constructing a large result in Python.

Generation is streaming and bounded by one Arrow batch.  GiB-scale tiers are
therefore feasible without materializing the corpus in memory, but the CLI
requires an explicit large-data opt-in before preparing any of them.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


KIB = 1024
MIB = 1024**2
GIB = 1024**3

CORPUS_FORMAT_VERSION = 1
GENERATOR_NAME = "islanddb-wide-v1"
TABLE_NAME = "events"
SUPER_NAME = "island_benchmark"

TIER_TARGET_BYTES: dict[str, int] = {
    "kb": 512 * KIB,
    "mb": 64 * MIB,
    "100mib": 100 * MIB,
    "1gib": GIB,
    "10gib": 10 * GIB,
    "50gib": 50 * GIB,
}

_TIER_ALIASES = {
    "kb": "kb",
    "kib": "kb",
    "512kb": "kb",
    "512kib": "kb",
    "mb": "mb",
    "mib": "mb",
    "64mb": "mb",
    "64mib": "mb",
    "100m": "100mib",
    "100mb": "100mib",
    "100mib": "100mib",
    "1gb": "1gib",
    "1g": "1gib",
    "1gib": "1gib",
    "gb": "1gib",
    "10gb": "10gib",
    "10g": "10gib",
    "10gib": "10gib",
    "50gb": "50gib",
    "50g": "50gib",
    "50gib": "50gib",
}

PUBLIC_COLUMN_TYPES: dict[str, str] = {
    "id": "Int64",
    "event_ts": "Datetime(time_unit='us', time_zone=None)",
    "metric": "Int64",
    "dimension": "Int32",
}
SYSTEM_COLUMN_TYPES: dict[str, str] = {
    "__rowid__": "Int64",
    "__timestamp__": "Datetime(time_unit='us', time_zone=None)",
}

_METRIC_MULTIPLIER = 48_271
_METRIC_OFFSET = 17
_METRIC_MODULUS = 1_000_003


def generated_metric_statistics(
    total_rows: int,
    *,
    source_repeat: int = 1,
) -> dict[str, Any]:
    """Return an engine-independent oracle for the generated metric column.

    The generator emits a permutation of every integer in
    ``[0, _METRIC_MODULUS)`` once per modulus-sized cycle.  Computing complete
    cycles algebraically bounds the residual loop to fewer than 1,000,003
    iterations even for the 50-GiB tier.  Repeated benchmark paths contain the
    same immutable rows, so count and sum scale while min/max do not change.
    """
    if total_rows <= 0:
        raise ValueError("total_rows must be positive")
    if source_repeat <= 0:
        raise ValueError("source_repeat must be positive")
    if math.gcd(_METRIC_MULTIPLIER, _METRIC_MODULUS) != 1:
        raise RuntimeError("generated metric sequence no longer has a full period")

    complete_cycles, remainder = divmod(total_rows, _METRIC_MODULUS)
    unique_sum = (
        complete_cycles
        * _METRIC_MODULUS
        * (_METRIC_MODULUS - 1)
        // 2
    )
    value = _METRIC_OFFSET % _METRIC_MODULUS
    remainder_min: int | None = None
    remainder_max: int | None = None
    for _ in range(remainder):
        unique_sum += value
        remainder_min = value if remainder_min is None else min(remainder_min, value)
        remainder_max = value if remainder_max is None else max(remainder_max, value)
        value = (value + _METRIC_MULTIPLIER) % _METRIC_MODULUS

    if complete_cycles:
        metric_min = 0
        metric_max = _METRIC_MODULUS - 1
    else:
        # total_rows is positive, so a zero-cycle input has a non-empty tail.
        assert remainder_min is not None and remainder_max is not None
        metric_min = remainder_min
        metric_max = remainder_max

    logical_rows = total_rows * source_repeat
    logical_sum = unique_sum * source_repeat
    if logical_sum > 2**53:
        raise ValueError(
            "generated metric sum exceeds the exact binary64 integer range; "
            "reduce source_repeat before benchmarking metric_avg"
        )
    return {
        "kind": "generated_metric_formula_v1",
        "formula": "(id * 48271 + 17) % 1000003",
        "average_method": "exact_integer_sum_then_binary64_division",
        "source_rows": int(total_rows),
        "source_repeat": int(source_repeat),
        "columns": [
            "row_count",
            "metric_non_null_count",
            "metric_null_count",
            "metric_sum",
            "metric_avg",
            "metric_min",
            "metric_max",
        ],
        "dtypes": [
            "int64",
            "int64",
            "int64",
            "int64",
            "float64",
            "int64",
            "int64",
        ],
        "row": [
            int(logical_rows),
            int(logical_rows),
            0,
            int(logical_sum),
            logical_sum / logical_rows,
            int(metric_min),
            int(metric_max),
        ],
    }


def normalize_tier(value: str) -> str:
    """Return a canonical tier name, accepting human-friendly aliases."""
    key = str(value or "").strip().lower().replace("_", "").replace("-", "")
    try:
        return _TIER_ALIASES[key]
    except KeyError as exc:
        choices = ", ".join(TIER_TARGET_BYTES)
        raise ValueError(f"unknown size tier {value!r}; choose one of {choices}") from exc


def normalize_tiers(values: Iterable[str]) -> list[str]:
    """Normalize and de-duplicate tiers while retaining caller order."""
    out: list[str] = []
    for raw in values:
        for part in str(raw).split(","):
            if not part.strip():
                continue
            tier = normalize_tier(part)
            if tier not in out:
                out.append(tier)
    if not out:
        raise ValueError("at least one size tier is required")
    return out


def parse_byte_size(value: str | int) -> int:
    """Parse a positive integer or a compact IEC/SI byte-size string."""
    if isinstance(value, bool):
        raise ValueError("byte size must be a positive integer")
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("byte size must be positive")
        return value
    text = str(value).strip().lower().replace(" ", "")
    units = {
        "": 1,
        "b": 1,
        "kb": 1000,
        "kib": KIB,
        "mb": 1000**2,
        "mib": MIB,
        "gb": 1000**3,
        "gib": GIB,
    }
    for suffix in sorted(units, key=len, reverse=True):
        if suffix and not text.endswith(suffix):
            continue
        number = text[: -len(suffix)] if suffix else text
        try:
            parsed = float(number)
        except ValueError:
            continue
        result = int(parsed * units[suffix])
        if result <= 0 or not math.isfinite(parsed):
            break
        return result
    raise ValueError(f"invalid byte size {value!r}")


def _default_shard_bytes(target_bytes: int) -> int:
    if target_bytes <= MIB:
        return target_bytes
    if target_bytes <= 128 * MIB:
        return 8 * MIB
    if target_bytes <= GIB:
        return 64 * MIB
    return 128 * MIB


@dataclass(frozen=True)
class CorpusSpec:
    """Inputs that fully determine a generated corpus."""

    tier: str
    target_bytes: int
    seed: int = 20260812
    payload_columns: int = 8
    payload_width: int = 64
    batch_rows: int = 16_384
    row_group_target_bytes: int = 8 * MIB
    shard_target_bytes: int | None = None
    compression: str = "zstd"
    compression_level: int = 1

    def __post_init__(self) -> None:
        if self.target_bytes <= 0:
            raise ValueError("target_bytes must be positive")
        if self.payload_columns <= 0 or self.payload_width <= 0:
            raise ValueError("payload shape must be positive")
        if self.batch_rows <= 0 or self.row_group_target_bytes <= 0:
            raise ValueError("batch and row-group sizes must be positive")
        if self.shard_target_bytes is not None and self.shard_target_bytes <= 0:
            raise ValueError("shard_target_bytes must be positive")

    @classmethod
    def for_tier(cls, tier: str, **overrides: Any) -> "CorpusSpec":
        normalized = normalize_tier(tier)
        return cls(
            tier=normalized,
            target_bytes=TIER_TARGET_BYTES[normalized],
            **overrides,
        )

    @property
    def effective_shard_bytes(self) -> int:
        return int(self.shard_target_bytes or _default_shard_bytes(self.target_bytes))

    @property
    def approximate_row_bytes(self) -> int:
        # Four public scalar columns + two system scalars + payload data.
        return 8 + 8 + 8 + 4 + 8 + 8 + self.payload_columns * self.payload_width

    @property
    def corpus_id(self) -> str:
        stable = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:12]
        return f"{GENERATOR_NAME}-{self.tier}-{digest}"

    def manifest_spec(self) -> dict[str, Any]:
        raw = asdict(self)
        raw["shard_target_bytes"] = self.effective_shard_bytes
        return raw


def _arrow_schema(spec: CorpusSpec):
    import pyarrow as pa

    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_ts", pa.timestamp("us"), nullable=False),
        pa.field("metric", pa.int64(), nullable=False),
        pa.field("dimension", pa.int32(), nullable=False),
    ]
    fields.extend(
        pa.field(f"payload_{idx:02d}", pa.binary(spec.payload_width), nullable=False)
        for idx in range(spec.payload_columns)
    )
    fields.extend(
        [
            pa.field("__rowid__", pa.int64(), nullable=False),
            pa.field("__timestamp__", pa.timestamp("us"), nullable=False),
        ]
    )
    return pa.schema(fields)


def _fixed_binary_array(raw: bytes, rows: int, width: int):
    import pyarrow as pa

    return pa.FixedSizeBinaryArray.from_buffers(
        pa.binary(width), rows, [None, pa.py_buffer(raw)]
    )


def _record_batch(spec: CorpusSpec, start_id: int, rows: int, rng):
    import numpy as np
    import pyarrow as pa

    ids = np.arange(start_id, start_id + rows, dtype=np.int64)
    # Values stay bounded, avoiding aggregate overflow even in the 50 GiB tier.
    metric = ((ids * np.int64(48_271) + np.int64(17)) % np.int64(1_000_003))
    dimension = (ids % np.int64(1_024)).astype(np.int32)
    base_us = np.int64(1_700_000_000_000_000)
    event_us = base_us + ids * np.int64(1_000)
    ingest_us = base_us + ids

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(event_us, type=pa.timestamp("us")),
        pa.array(metric, type=pa.int64()),
        pa.array(dimension, type=pa.int32()),
    ]
    for _ in range(spec.payload_columns):
        raw = rng.bytes(rows * spec.payload_width)
        arrays.append(_fixed_binary_array(raw, rows, spec.payload_width))
    arrays.extend(
        [
            pa.array(ids + np.int64(1), type=pa.int64()),
            pa.array(ingest_us, type=pa.timestamp("us")),
        ]
    )
    return pa.RecordBatch.from_arrays(arrays, schema=_arrow_schema(spec))


def _write_one_file(
    destination: Path,
    spec: CorpusSpec,
    start_id: int,
    rows: int,
    row_group_rows: int,
    rng,
) -> dict[str, Any]:
    import pyarrow.parquet as pq

    schema = _arrow_schema(spec)
    writer = pq.ParquetWriter(
        destination,
        schema,
        compression=spec.compression,
        compression_level=spec.compression_level,
        use_dictionary=False,
        write_statistics=True,
    )
    written = 0
    try:
        while written < rows:
            count = min(spec.batch_rows, rows - written)
            batch = _record_batch(spec, start_id + written, count, rng)
            writer.write_batch(batch, row_group_size=row_group_rows)
            written += count
    finally:
        writer.close()

    metadata = pq.ParquetFile(destination).metadata
    size = destination.stat().st_size
    return {
        "path": destination.name,
        "bytes": int(size),
        "rows": int(rows),
        "min_id": int(start_id),
        "max_id": int(start_id + rows - 1),
        "row_groups": int(metadata.num_row_groups),
    }


def _manifest_schema(spec: CorpusSpec) -> dict[str, str]:
    result = dict(PUBLIC_COLUMN_TYPES)
    result.update(
        {f"payload_{idx:02d}": "Binary" for idx in range(spec.payload_columns)}
    )
    result.update(SYSTEM_COLUMN_TYPES)
    return result


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temp, path)


def load_manifest(path_or_directory: str | Path) -> dict[str, Any]:
    path = Path(path_or_directory)
    if path.is_dir():
        path = path / "manifest.json"
    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest["manifest_path"] = str(path.resolve())
    return manifest


@contextmanager
def repeated_manifest_paths(
    manifest: Mapping[str, Any], source_repeat: int,
):
    """Yield an execution manifest with distinct hard-link aliases per repeat.

    Production engines reject duplicate canonical paths/resource keys because
    they make deletion-vector identity ambiguous. Benchmark repetition keeps
    that invariant: repeats two..N are unique directory entries pointing to the
    same immutable inode. They consume no second payload copy and are removed
    after all isolated workers finish.
    """
    if source_repeat <= 0:
        raise ValueError("source_repeat must be positive")
    if source_repeat == 1:
        yield dict(manifest)
        return

    manifest_path = Path(str(manifest["manifest_path"])).resolve()
    base = manifest_path.parent
    unique_entries = list(manifest.get("files") or [])
    if not unique_entries:
        raise ValueError("cannot repeat an empty benchmark manifest")
    alias_root = Path(tempfile.mkdtemp(prefix=".logical-repeat-", dir=base))
    repeated_entries = [dict(entry) for entry in unique_entries]
    try:
        for repeat_index in range(1, source_repeat):
            repeat_root = alias_root / f"repeat-{repeat_index:04d}"
            repeat_root.mkdir()
            for file_index, entry in enumerate(unique_entries):
                source = (base / str(entry["path"])).resolve(strict=True)
                alias = repeat_root / f"{file_index:05d}-{source.name}"
                os.link(source, alias)
                source_stat = source.stat()
                alias_stat = alias.stat()
                if (
                    source_stat.st_dev != alias_stat.st_dev
                    or source_stat.st_ino != alias_stat.st_ino
                    or source_stat.st_size != alias_stat.st_size
                ):
                    raise RuntimeError("source-repeat hard-link identity proof failed")
                repeated = dict(entry)
                repeated["path"] = str(alias)
                repeated_entries.append(repeated)

        execution_manifest = dict(manifest)
        execution_manifest["files"] = repeated_entries
        execution_manifest["source_repeat"] = source_repeat
        execution_manifest["source_repeat_mode"] = "distinct_hardlink_aliases"
        execution_manifest["unique_file_count"] = len(unique_entries)
        execution_manifest["unique_source_bytes"] = int(
            manifest["actual_source_bytes"]
        )
        yield execution_manifest
    finally:
        shutil.rmtree(alias_root, ignore_errors=True)


def validate_manifest(
    manifest: Mapping[str, Any], expected_spec: CorpusSpec | None = None
) -> list[str]:
    """Return validation problems; an empty list means reusable corpus."""
    problems: list[str] = []
    if manifest.get("format_version") != CORPUS_FORMAT_VERSION:
        problems.append("unsupported corpus format version")
    if manifest.get("generator") != GENERATOR_NAME:
        problems.append("unexpected corpus generator")
    if expected_spec is not None and manifest.get("spec") != expected_spec.manifest_spec():
        problems.append("corpus spec differs from requested spec")
    manifest_path = manifest.get("manifest_path")
    if not manifest_path:
        problems.append("manifest path is unavailable")
        return problems
    base = Path(str(manifest_path)).parent
    total = 0
    rows = 0
    for entry in manifest.get("files") or []:
        file_path = base / str(entry.get("path", ""))
        if not file_path.is_file():
            problems.append(f"missing parquet file: {entry.get('path')}")
            continue
        actual = file_path.stat().st_size
        expected = int(entry.get("bytes", -1))
        if actual != expected:
            problems.append(
                f"size mismatch for {entry.get('path')}: expected {expected}, got {actual}"
            )
        total += actual
        rows += int(entry.get("rows", 0))
    if total != int(manifest.get("actual_source_bytes", -1)):
        problems.append("actual_source_bytes does not match file sizes")
    if rows != int(manifest.get("total_rows", -1)):
        problems.append("total_rows does not match file rows")
    if not manifest.get("files"):
        problems.append("corpus contains no parquet files")
    return problems


def _disk_preflight(root: Path, spec: CorpusSpec) -> None:
    usage = shutil.disk_usage(root)
    # Corpus + transient writer overhead.  Large runs may also populate a local
    # Island cache, so require three times the source target for those tiers.
    if spec.target_bytes >= GIB:
        required = 3 * spec.target_bytes
    else:
        required = 2 * spec.target_bytes + 32 * MIB
    if usage.free < required:
        raise RuntimeError(
            f"insufficient free space for {spec.tier}: need at least {required} "
            f"bytes, have {usage.free} bytes at {root}"
        )


def prepare_corpus(
    root: str | Path,
    spec: CorpusSpec,
    *,
    check_disk: bool = True,
) -> dict[str, Any]:
    """Generate or validate and reuse a deterministic Parquet corpus.

    Corpus directories are content-addressed by the complete spec.  A partial
    generation is written under a private staging directory and atomically
    renamed only after its manifest validates.
    """
    import numpy as np

    root_path = Path(root).expanduser().resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    destination = root_path / spec.corpus_id
    manifest_path = destination / "manifest.json"
    if manifest_path.is_file():
        existing = load_manifest(manifest_path)
        problems = validate_manifest(existing, spec)
        if problems:
            raise RuntimeError(
                f"existing corpus {destination} is invalid: " + "; ".join(problems)
            )
        existing["reused"] = True
        return existing

    if destination.exists():
        raise RuntimeError(
            f"corpus destination exists without a valid manifest: {destination}"
        )
    if check_disk:
        _disk_preflight(root_path, spec)

    stage = Path(tempfile.mkdtemp(prefix=f".{spec.corpus_id}.", dir=root_path))
    rng = np.random.Generator(np.random.PCG64(spec.seed))
    files: list[dict[str, Any]] = []
    total_bytes = 0
    total_rows = 0
    observed_row_bytes = float(spec.approximate_row_bytes)
    row_group_rows = max(
        1, int(spec.row_group_target_bytes / spec.approximate_row_bytes)
    )
    try:
        index = 0
        while total_bytes < spec.target_bytes:
            remaining = spec.target_bytes - total_bytes
            desired = min(spec.effective_shard_bytes, remaining)
            rows = max(1, int(math.ceil(desired / max(1.0, observed_row_bytes))))
            file_path = stage / f"part-{index:05d}.parquet"
            entry = _write_one_file(
                file_path,
                spec,
                start_id=total_rows,
                rows=rows,
                row_group_rows=row_group_rows,
                rng=rng,
            )
            files.append(entry)
            total_bytes += entry["bytes"]
            total_rows += rows
            sample_bpr = entry["bytes"] / max(1, rows)
            # Smooth calibration so footer overhead from a small first shard
            # cannot make the following shard disproportionately small.
            observed_row_bytes = 0.25 * observed_row_bytes + 0.75 * sample_bpr
            index += 1
            if index > 100_000:
                raise RuntimeError("corpus generator exceeded its shard safety bound")

        manifest: dict[str, Any] = {
            "format_version": CORPUS_FORMAT_VERSION,
            "generator": GENERATOR_NAME,
            "generated_unix_ms": int(time.time() * 1000),
            "spec": spec.manifest_spec(),
            "table": TABLE_NAME,
            "super_name": SUPER_NAME,
            "target_source_bytes": int(spec.target_bytes),
            "actual_source_bytes": int(total_bytes),
            "total_rows": int(total_rows),
            "schema": _manifest_schema(spec),
            "files": files,
        }
        _write_json_atomic(stage / "manifest.json", manifest)
        staged = load_manifest(stage / "manifest.json")
        problems = validate_manifest(staged, spec)
        if problems:
            raise RuntimeError("generated corpus failed validation: " + "; ".join(problems))
        try:
            os.replace(stage, destination)
        except FileExistsError:
            # A concurrent identical generator won the race.  Reuse it only if
            # complete and valid; never merge two staging directories.
            winner = load_manifest(manifest_path)
            winner_problems = validate_manifest(winner, spec)
            if winner_problems:
                raise RuntimeError(
                    "concurrently generated corpus is invalid: "
                    + "; ".join(winner_problems)
                )
            shutil.rmtree(stage)
            winner["reused"] = True
            return winner
        result = load_manifest(manifest_path)
        result["reused"] = False
        return result
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


@dataclass(frozen=True)
class Workload:
    name: str
    sql: str
    lower_id: int | None
    upper_id: int | None
    required_columns: tuple[str, ...]
    file_pruning: bool = True
    # The stress harness consumes the grouped result through the bounded Arrow
    # API even though the generated dimension's sealed domain is small enough
    # to collect. This keeps benchmark result handling independent of pandas.
    island_streaming_result: bool = False
    independent_oracle_kind: str | None = None


WORKLOAD_NAMES = (
    "no_match",
    "point",
    "range_1pct",
    "range_1pct_5cols",
    "range_10pct",
    "projection",
    "aggregate_stats",
    "full_scan",
    "spill_group",
)


def build_workloads(
    total_rows: int,
    *,
    payload_columns: int = 8,
) -> dict[str, Workload]:
    if total_rows <= 0:
        raise ValueError("total_rows must be positive")
    if payload_columns <= 0:
        raise ValueError("payload_columns must be positive")

    def aggregate(name: str, lower: int, upper: int) -> Workload:
        return Workload(
            name=name,
            sql=(
                "SELECT COUNT(*) AS row_count, SUM(metric) AS metric_sum "
                f"FROM {TABLE_NAME} WHERE id >= {lower} AND id < {upper}"
            ),
            lower_id=lower,
            upper_id=upper,
            required_columns=("id", "metric"),
        )

    point = total_rows // 2
    one_pct = max(1, total_rows // 100)
    ten_pct = max(1, total_rows // 10)
    one_start = max(0, (total_rows - one_pct) // 2)
    ten_start = max(0, (total_rows - ten_pct) // 2)
    full_scan_columns = tuple(PUBLIC_COLUMN_TYPES) + tuple(
        f"payload_{idx:02d}" for idx in range(payload_columns)
    )
    full_scan_aggregates = ", ".join(
        f"MAX({column}) AS {column}_max" for column in full_scan_columns
    )
    full_scan_aggregates = f"COUNT(*) AS row_count, {full_scan_aggregates}"
    spill_group_aggregates = ", ".join(
        f"COUNT({column}) AS {column}_count"
        for column in full_scan_columns
        if column != "dimension"
    )
    return {
        # Keep physical files for this workload so both native engines prove
        # the empty result through Parquet row-group statistics.  Removing all
        # files would benchmark a typed-empty catalog snapshot, which IslandDB
        # deliberately rejects until its empty-schema coercions are complete.
        "no_match": Workload(
            name="no_match",
            sql=(
                "SELECT COUNT(*) AS row_count, SUM(metric) AS metric_sum "
                f"FROM {TABLE_NAME} WHERE id >= {total_rows + 10_000} "
                f"AND id < {total_rows + 20_000}"
            ),
            lower_id=total_rows + 10_000,
            upper_id=total_rows + 20_000,
            required_columns=("id", "metric"),
            file_pruning=False,
        ),
        "point": aggregate("point", point, point + 1),
        "range_1pct": aggregate("range_1pct", one_start, one_start + one_pct),
        "range_1pct_5cols": Workload(
            name="range_1pct_5cols",
            sql=(
                "SELECT SUM(metric) AS metric_sum, MAX(dimension) AS max_dimension, "
                "COUNT(payload_00) AS payload_00_count, "
                "COUNT(payload_01) AS payload_01_count "
                f"FROM {TABLE_NAME} WHERE id >= {one_start} "
                f"AND id < {one_start + one_pct}"
            ),
            lower_id=one_start,
            upper_id=one_start + one_pct,
            required_columns=(
                "id", "metric", "dimension", "payload_00", "payload_01",
            ),
        ),
        "range_10pct": aggregate("range_10pct", ten_start, ten_start + ten_pct),
        "projection": Workload(
            name="projection",
            sql=(
                "SELECT COUNT(metric) AS row_count, "
                f"SUM(metric) AS metric_sum FROM {TABLE_NAME}"
            ),
            lower_id=None,
            upper_id=None,
            required_columns=("metric",),
        ),
        # This scalar result is checked against the corpus generator's closed
        # arithmetic formula as well as against the other engine. IslandDB
        # deliberately rejects order-sensitive native AVG and computed
        # aggregate expressions. The benchmark therefore asks both engines for
        # exact primitive reductions, then derives NULL count and average from
        # their returned integer count/sum under a proven binary64-safe bound.
        "aggregate_stats": Workload(
            name="aggregate_stats",
            sql=(
                "SELECT COUNT(*) AS row_count, "
                "COUNT(metric) AS metric_non_null_count, "
                "SUM(metric) AS metric_sum, "
                "MIN(metric) AS metric_min, MAX(metric) AS metric_max "
                f"FROM {TABLE_NAME}"
            ),
            lower_id=None,
            upper_id=None,
            required_columns=("metric",),
            independent_oracle_kind="generated_metric_formula_v1",
        ),
        # A direct MAX reduction consumes values from every public column in
        # both engines. COUNT is intentionally not used: DuckDB can answer it
        # from Parquet null-count metadata without reading value pages. MAX has
        # constant state per column, so the corpus may still be much larger than
        # RAM without manufacturing a large result.
        "full_scan": Workload(
            name="full_scan",
            sql=f"SELECT {full_scan_aggregates} FROM {TABLE_NAME}",
            lower_id=None,
            upper_id=None,
            required_columns=full_scan_columns,
        ),
        # This deliberately drives IslandDB's sealed external GROUP BY + ORDER
        # path while keeping the final result bounded to the generated 1,024
        # dimension values.  COUNT over every other public field makes the
        # physical projection cover all public columns.  Engines remain free
        # to exploit valid Parquet metadata, so physical read counters—not the
        # projection alone—are authoritative for bytes actually transferred.
        "spill_group": Workload(
            name="spill_group",
            sql=(
                f"SELECT dimension, {spill_group_aggregates} "
                f"FROM {TABLE_NAME} GROUP BY dimension ORDER BY dimension"
            ),
            lower_id=None,
            upper_id=None,
            required_columns=full_scan_columns,
            island_streaming_result=True,
        ),
    }


def normalize_workloads(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    valid = set(WORKLOAD_NAMES)
    for raw in values:
        for part in raw.split(","):
            name = part.strip().lower()
            if not name:
                continue
            if name not in valid:
                raise ValueError(
                    f"unknown workload {name!r}; choose from {', '.join(WORKLOAD_NAMES)}"
                )
            if name not in result:
                result.append(name)
    if not result:
        raise ValueError("at least one workload is required")
    return result


def _range_overlaps(
    minimum: int,
    maximum: int,
    lower: int | None,
    upper: int | None,
) -> bool:
    if lower is not None and maximum < lower:
        return False
    # Workloads use a half-open upper bound.
    if upper is not None and minimum >= upper:
        return False
    return True


def _column_chunk_bytes(metadata, names: set[str], only_matching_groups, id_index: int) -> int:
    total = 0
    for rg_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_index)
        if only_matching_groups is not None:
            id_chunk = row_group.column(id_index)
            statistics = id_chunk.statistics
            if statistics is None or not statistics.has_min_max:
                # Unknown stats must fail open and keep the row group.
                overlaps = True
            else:
                overlaps = _range_overlaps(
                    int(statistics.min),
                    int(statistics.max),
                    only_matching_groups[0],
                    only_matching_groups[1],
                )
            if not overlaps:
                continue
        for col_index in range(metadata.num_columns):
            chunk = row_group.column(col_index)
            if chunk.path_in_schema in names:
                total += int(chunk.total_compressed_size or 0)
    return total


def _generated_decoded_row_width(
    manifest: Mapping[str, Any], required_columns: Sequence[str],
) -> int:
    """Exact value-buffer width for this harness' non-null generated schema."""
    schema = dict(manifest.get("schema") or {})
    payload_width = int(manifest["spec"]["payload_width"])
    widths = {
        "Int64": 8,
        "Int32": 4,
        "Datetime(time_unit='us', time_zone=None)": 8,
        "Binary": payload_width,
    }
    total = 0
    for column in required_columns:
        type_name = schema.get(column)
        width = widths.get(str(type_name))
        if width is None:
            raise RuntimeError(
                f"benchmark generated schema has no exact decoded width for "
                f"{column!r}: {type_name!r}"
            )
        total += width
    return total


def plan_workload(
    manifest: Mapping[str, Any],
    workload: Workload,
) -> dict[str, Any]:
    """Build one executor input and two explicit scan-size estimates.

    ``estimated_reflection_bytes`` mirrors production routing: compressed
    chunks for required columns across every row group in files retained by
    file-level min/max pruning.  ``estimated_pushdown_bytes`` narrows that to
    row groups whose ``id`` statistics overlap the predicate.  Both exclude
    footer/range-request overhead and are labelled estimates in result JSON.
    """
    import pyarrow.parquet as pq

    manifest_path = Path(str(manifest["manifest_path"]))
    base = manifest_path.parent
    original_files = list(manifest.get("files") or [])
    source_repeat = int(manifest.get("source_repeat") or 1)
    if source_repeat <= 0 or len(original_files) % source_repeat:
        raise ValueError("execution manifest has invalid source-repeat metadata")
    unique_file_count = int(
        manifest.get("unique_file_count") or len(original_files) // source_repeat
    )
    retained = [
        entry
        for entry in original_files
        if not workload.file_pruning
        or workload.lower_id is None
        or _range_overlaps(
            int(entry["min_id"]),
            int(entry["max_id"]),
            workload.lower_id,
            workload.upper_id,
        )
    ]
    selected = set(workload.required_columns)
    estimated_reflection = 0
    estimated_pushdown = 0
    row_groups_total = 0
    row_groups_eligible = 0
    candidate_rows = 0
    row_group_selections: dict[str, dict[str, Any]] = {}
    resolved_files: list[str] = []
    original_paths = [str((base / str(entry["path"])).resolve()) for entry in original_files]

    for entry in retained:
        path = (base / str(entry["path"])).resolve()
        resolved_files.append(str(path))
        metadata = pq.ParquetFile(path).metadata
        row_groups_total += metadata.num_row_groups
        schema_names = [metadata.schema.column(i).name for i in range(metadata.num_columns)]
        try:
            id_index = schema_names.index("id")
        except ValueError as exc:
            raise RuntimeError(f"benchmark parquet lacks id column: {path}") from exc
        estimated_reflection += _column_chunk_bytes(
            metadata, selected, None, id_index
        )
        estimated_pushdown += _column_chunk_bytes(
            metadata,
            selected,
            (workload.lower_id, workload.upper_id)
            if workload.lower_id is not None
            else None,
            id_index,
        )
        eligible_ids: list[int] = []
        for rg_index in range(metadata.num_row_groups):
            statistics = metadata.row_group(rg_index).column(id_index).statistics
            if workload.lower_id is None or statistics is None or not statistics.has_min_max:
                row_groups_eligible += 1
                eligible_ids.append(rg_index)
                candidate_rows += int(metadata.row_group(rg_index).num_rows)
            elif _range_overlaps(
                int(statistics.min),
                int(statistics.max),
                workload.lower_id,
                workload.upper_id,
            ):
                row_groups_eligible += 1
                eligible_ids.append(rg_index)
                candidate_rows += int(metadata.row_group(rg_index).num_rows)
        footer_payload = io.BytesIO()
        metadata.write_metadata_file(footer_payload)
        row_group_selections[str(path)] = {
            "row_group_count": int(metadata.num_row_groups),
            "eligible_ids": eligible_ids,
            "footer_sha256": hashlib.sha256(
                footer_payload.getvalue(),
            ).hexdigest(),
        }

    unique_source_bytes = int(
        manifest.get("unique_source_bytes") or manifest["actual_source_bytes"]
    )
    source_bytes = unique_source_bytes * source_repeat
    unique_estimated_reflection = estimated_reflection // source_repeat
    unique_estimated_pushdown = estimated_pushdown // source_repeat
    projected_source_fraction = (
        estimated_pushdown / source_bytes if source_bytes > 0 else 0.0
    )
    decoded_row_width = _generated_decoded_row_width(
        manifest, workload.required_columns,
    )
    estimated_decoded_bytes = candidate_rows * decoded_row_width
    independent_oracle = None
    if workload.independent_oracle_kind == "generated_metric_formula_v1":
        independent_oracle = generated_metric_statistics(
            int(manifest["total_rows"]),
            source_repeat=source_repeat,
        )
    return {
        "name": workload.name,
        "sql": workload.sql,
        "required_columns": list(workload.required_columns),
        "island_streaming_result": bool(workload.island_streaming_result),
        "result_postprocess": workload.independent_oracle_kind,
        "payload_width": int(manifest["spec"]["payload_width"]),
        "lower_id": workload.lower_id,
        "upper_id": workload.upper_id,
        "original_files": original_paths,
        "files": resolved_files,
        "resource_keys": resolved_files,
        "source_bytes": source_bytes,
        "unique_source_bytes": unique_source_bytes,
        "source_repeat": source_repeat,
        "candidate_source_bytes": sum(int(entry["bytes"]) for entry in retained),
        "estimated_reflection_bytes": int(estimated_reflection),
        "estimated_pushdown_bytes": int(estimated_pushdown),
        "unique_estimated_reflection_bytes": int(unique_estimated_reflection),
        "unique_estimated_pushdown_bytes": int(unique_estimated_pushdown),
        "estimated_decoded_bytes": int(estimated_decoded_bytes),
        "decoded_row_width": int(decoded_row_width),
        "decoded_estimate_complete": True,
        "projected_source_fraction": projected_source_fraction,
        "eligible_pushdown_bytes": int(estimated_pushdown),
        "row_group_selections": row_group_selections,
        "files_before_prune": len(original_files),
        "unique_files_before_prune": unique_file_count,
        "source_repeat_mode": str(
            manifest.get("source_repeat_mode") or "none"
        ),
        "files_after_prune": len(retained),
        "files_pruned": len(original_files) - len(retained),
        "row_groups_after_file_prune": int(row_groups_total),
        "row_groups_pushdown_eligible": int(row_groups_eligible),
        "candidate_rows": int(candidate_rows),
        # Benchmark-only generator proof. Every immutable row has contiguous
        # id, metric=(id*48271+17)%1000003, and dimension=id%1024. These
        # corpus-wide extrema remain conservative for every selected subset.
        "integer_domain_bounds": {
            "id": {
                "minimum": 0,
                "maximum": int(manifest["total_rows"]) - 1,
                "has_null": False,
            },
            "metric": {
                "minimum": 0,
                "maximum": 1_000_002,
                "has_null": False,
            },
            "dimension": {
                "minimum": 0,
                "maximum": min(1_023, int(manifest["total_rows"]) - 1),
                "has_null": False,
            },
        },
        "independent_oracle": independent_oracle,
        "schema": dict(manifest["schema"]),
        "super_name": str(manifest.get("super_name") or SUPER_NAME),
        "table": str(manifest.get("table") or TABLE_NAME),
    }


def sha256_file(path: str | Path, block_bytes: int = MIB) -> str:
    """Streaming digest helper used by smoke tests and optional tooling."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            block = handle.read(block_bytes)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()
