"""Deterministic tombstone-drain and small-file compaction benchmark.

By default this benchmark exercises the two historical data-plane phases used
by a write that crosses the deletion-vector threshold:

1. ``compact_tombstones`` rewrites every partially-dead source file.
2. ``compact_resources`` folds the survivor files toward a 16 MiB target.

``--fused`` instead calls ``compact_resources`` once with the deletion vector,
so decoded survivors flow directly into final target-sized outputs.  Both modes
share the exact corpus and oracle, making their wall/CPU/RSS/I/O deltas directly
comparable.

The default corpus has exactly twenty Parquet files.  Their columns are reordered,
some current columns are absent, and older files contain columns omitted by the
newest upload.  The newest file defines the authoritative public projection.
Both that projection and the complete physical union are compared against an
independent DuckDB oracle with byte-exact SHA-256 digests.

The benchmark does not require Redis or a SuperTable catalog.  That is
intentional: it isolates the Parquet decode/anti-join/encode/merge work which
dominates the slow write, while executing the real production compaction
helpers and local-storage implementation.

Quick baseline (roughly 50--90 MiB depending on codec versions)::

    python -m supertable.benchmarks.benchmark_tombstone_compaction \
        --label baseline --rows-per-file 100000 \
        --output /tmp/tombstone-compaction-baseline.json

Approximately 300 MiB on the versions used when this benchmark was added::

    taskset -c 0-3 python -m \
        supertable.benchmarks.benchmark_tombstone_compaction \
        --label baseline-300m --rows-per-file 380000 --workers 4 \
        --output /tmp/tombstone-compaction-baseline-300m.json

Fused equivalent::

    taskset -c 0-3 python -m \
        supertable.benchmarks.benchmark_tombstone_compaction --fused \
        --label fused-300m --rows-per-file 380000 --workers 4 \
        --output /tmp/tombstone-compaction-fused-300m.json

The production-sized write regression requested for version comparisons uses
fifteen independently calibrated small files and exactly one million deletion
vector entries::

    python -m supertable.benchmarks.benchmark_tombstone_compaction \
        --file-count 15 --rows-per-file 100000 \
        --tombstone-rows 1000000 --input-file-target-mib 15.75 \
        --input-size-tolerance-pct 1 --workers 4 --two-phase
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import os
import platform
import statistics
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Mapping, Sequence, TypeVar
from unittest.mock import patch

import duckdb
import polars as pl

from supertable import __version__ as SUPERTABLE_VERSION
from supertable.processing import (
    ROWID_COL,
    TOMBSTONE_FILE_COL,
    compact_resources,
    compact_tombstones,
    extract_stats_rows,
)
from supertable.storage.local_storage import LocalStorage
from supertable.utils.profiler import Profiler


MIB = 1024 * 1024
FILE_COUNT = 20
DEFAULT_TARGET_BYTES = 16 * MIB
PARQUET_ROW_GROUP_ROWS = 122_880
LATEST_PUBLIC_COLUMNS = ("tenant", "id", "payload", "amount", "active")
HIDDEN_COLUMNS = (ROWID_COL, "__timestamp__")
_T = TypeVar("_T")


def _proc_io_counters(path: str | Path = "/proc/self/io") -> dict[str, int] | None:
    """Return Linux process-I/O counters without adding a psutil dependency."""

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
    """Subtract cumulative counters, tolerating resets and missing platforms."""

    if before is None or after is None:
        return None
    return {
        key: max(0, int(after[key]) - int(before[key]))
        for key in sorted(before.keys() & after.keys())
    }


def _filter_compatible_kwargs(fn: Callable, kwargs: Mapping[str, object]) -> dict:
    """Filter optional HEAD-only keywords for an older installed revision.

    The external comparison worker mounts this benchmark file into both the
    candidate and current containers.  Candidate 426e94b lacks the fused
    ``compact_resources`` and footer-cache keywords, while the shared corpus,
    oracle, and telemetry must remain byte-for-byte identical.
    """

    parameters = inspect.signature(fn).parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
        return dict(kwargs)
    return {name: value for name, value in kwargs.items() if name in parameters}


def _call_compatible(fn: Callable[..., _T], **kwargs) -> _T:
    return fn(**_filter_compatible_kwargs(fn, kwargs))


def _allocate_tombstones(rows_by_file: Sequence[int], total: int) -> list[int]:
    """Allocate exactly *total* partial-file tombstones across every file.

    One row is reserved in every file so the workload always takes the costly
    survivor-rewrite lane rather than the fully-dead metadata fast path.  The
    remaining rows are assigned proportionally by capacity with deterministic
    largest-remainder rounding.
    """

    rows = [int(value) for value in rows_by_file]
    if not rows or any(value < 2 for value in rows):
        raise ValueError("every benchmark file must contain at least two rows")
    requested = int(total)
    capacity = sum(value - 1 for value in rows)
    if requested < len(rows) or requested > capacity:
        raise ValueError(
            "tombstone_rows must touch every file while leaving one survivor "
            f"per file ({len(rows)} <= value <= {capacity})"
        )

    allocation = [1] * len(rows)
    remaining = requested - len(rows)
    spare = [value - 2 for value in rows]
    spare_total = sum(spare)
    if remaining and spare_total:
        floors = [(remaining * value) // spare_total for value in spare]
        allocation = [base + floor for base, floor in zip(allocation, floors)]
        remainder = requested - sum(allocation)
        order = sorted(
            range(len(rows)),
            key=lambda index: (
                -((remaining * spare[index]) % spare_total),
                index,
            ),
        )
        for index in order[:remainder]:
            allocation[index] += 1

    if sum(allocation) != requested or any(
        dead <= 0 or dead >= physical for dead, physical in zip(allocation, rows)
    ):
        raise AssertionError("internal exact tombstone allocation failure")
    return allocation


def _current_rss_bytes() -> int:
    """Return resident bytes without adding a psutil dependency."""

    try:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * int(os.sysconf("SC_PAGE_SIZE"))
    except (FileNotFoundError, IndexError, OSError, TypeError, ValueError):
        # ``ru_maxrss`` is KiB on Linux and bytes on macOS.  It is a lifetime
        # peak rather than current RSS, but is still a useful portable fallback.
        import resource

        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return peak if sys.platform == "darwin" else peak * 1024


class _RSSSampler:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.001, float(interval_seconds))
        self.start_bytes = _current_rss_bytes()
        self.peak_bytes = self.start_bytes
        self.end_bytes = self.start_bytes
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._sample, name="compaction-rss-sampler", daemon=True
        )

    def _sample(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.peak_bytes = max(self.peak_bytes, _current_rss_bytes())

    def __enter__(self) -> "_RSSSampler":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.end_bytes = _current_rss_bytes()
        self.peak_bytes = max(self.peak_bytes, self.end_bytes)
        self._stop.set()
        self._thread.join()


@dataclass(frozen=True)
class _Measured:
    value: object
    metrics: dict


def _measure(fn: Callable[[], _T], sample_interval_seconds: float) -> _Measured:
    io_started = _proc_io_counters()
    process_started = os.times()
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    with _RSSSampler(sample_interval_seconds) as rss:
        value = fn()
    wall_seconds = time.perf_counter() - wall_started
    cpu_seconds = time.process_time() - cpu_started
    process_finished = os.times()
    io_finished = _proc_io_counters()
    return _Measured(
        value=value,
        metrics={
            "wall_seconds": round(wall_seconds, 6),
            "cpu_seconds": round(cpu_seconds, 6),
            "cpu_user_seconds": round(process_finished.user - process_started.user, 6),
            "cpu_system_seconds": round(
                process_finished.system - process_started.system, 6
            ),
            "cpu_equivalent_cores": round(
                cpu_seconds / wall_seconds if wall_seconds else 0.0, 4
            ),
            "rss_start_bytes": rss.start_bytes,
            "rss_end_bytes": rss.end_bytes,
            "rss_peak_bytes": rss.peak_bytes,
            "rss_peak_delta_bytes": max(0, rss.peak_bytes - rss.start_bytes),
            "proc_io_start": io_started,
            "proc_io_end": io_finished,
            "proc_io_delta": _counter_delta(io_started, io_finished),
        },
    )


def _payload_expr(seed_offset: int = 0) -> pl.Expr:
    # Four independent integer hashes make the payload poorly compressible
    # enough for encoded byte-size gates to be meaningful, without Python-row
    # construction or a random generator whose global state can leak.
    return pl.concat_str(
        [
            pl.col("id").hash(seed=seed_offset + seed).cast(pl.String)
            for seed in (11, 23, 37, 53)
        ],
        separator=":",
    ).alias("payload")


def _frame_for_file(
    file_index: int,
    rows_per_file: int,
    *,
    file_count: int = FILE_COUNT,
    first_id: int | None = None,
) -> pl.DataFrame:
    if first_id is None:
        first_id = file_index * rows_per_file + 1
    ids = pl.int_range(first_id, first_id + rows_per_file, eager=True, dtype=pl.Int64)
    frame = pl.DataFrame({"id": ids}).with_columns(
        pl.concat_str(
            [pl.lit("tenant-"), ((pl.col("id") * 17) % 101).cast(pl.String)]
        ).alias("tenant"),
        (((pl.col("id") * 31) % 10_000_019).cast(pl.Float64) / 100.0).alias("amount"),
        _payload_expr(file_index * 101),
        ((pl.col("id") + file_index) % 3 != 0).alias("active"),
        pl.col("id").alias(ROWID_COL),
        pl.lit(1_700_000_000_000_000 + file_index * 1_000_000)
        .cast(pl.Datetime("us"))
        .alias("__timestamp__"),
    )

    # Every historical shape is legal but deliberately awkward.  Missing
    # current columns must become NULL under union-by-name, while legacy-only
    # fields remain physically lossless but are not in the newest public view.
    if file_index != file_count - 1:
        if file_index % 4 == 0:
            frame = frame.drop("amount")
        if file_index % 5 == 0:
            frame = frame.drop("payload")
        if file_index % 6 == 0:
            frame = frame.drop("active")
        if file_index % 3 == 0:
            frame = frame.with_columns(
                pl.concat_str(
                    [pl.lit("legacy-"), pl.col("id").hash(seed=71).cast(pl.String)]
                ).alias("legacy_text")
            )
        if file_index % 4 == 1:
            frame = frame.with_columns(
                ((pl.col("id") * 7) % 997).cast(pl.Int32).alias("legacy_score")
            )

    if file_index == file_count - 1:
        public_order = list(LATEST_PUBLIC_COLUMNS)
    else:
        public = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
        shift = file_index % len(public)
        public_order = public[shift:] + public[:shift]
        if file_index % 2:
            public_order = list(reversed(public_order))
    return frame.select(public_order + list(HIDDEN_COLUMNS))


def _schema_json(frame: pl.DataFrame) -> list[dict[str, str]]:
    return [{"name": name, "dtype": str(dtype)} for name, dtype in frame.schema.items()]


def _diversity_record(
    schemas: Sequence[Sequence[str]], *, file_count: int = FILE_COUNT
) -> dict:
    latest = set(LATEST_PUBLIC_COLUMNS)
    missing = []
    extras = []
    reordered = []
    for index, schema in enumerate(schemas):
        public = [name for name in schema if name not in HIDDEN_COLUMNS]
        absent = sorted(latest.difference(public))
        legacy = sorted(set(public).difference(latest))
        if absent:
            missing.append({"file_index": index, "columns": absent})
        if legacy:
            extras.append({"file_index": index, "columns": legacy})
        shared = [name for name in public if name in latest]
        canonical_shared = [name for name in LATEST_PUBLIC_COLUMNS if name in shared]
        if shared != canonical_shared:
            reordered.append(index)
    return {
        "files_with_reordered_columns": reordered,
        "files_with_missing_latest_columns": missing,
        "files_with_legacy_extra_columns": extras,
        "newest_file_index": file_count - 1,
        "newest_public_columns": list(LATEST_PUBLIC_COLUMNS),
    }


def _write_source_frame(frame: pl.DataFrame, path: Path, compression_level: int) -> int:
    frame.write_parquet(
        path,
        compression="zstd",
        compression_level=compression_level,
        statistics=True,
        row_group_size=PARQUET_ROW_GROUP_ROWS,
    )
    return path.stat().st_size


def _calibration_bounds(
    input_target_bytes: int,
    tolerance_fraction: float,
    compaction_target_bytes: int,
) -> tuple[int, int]:
    if input_target_bytes <= 0:
        raise ValueError("input_file_target_bytes must be positive")
    if not 0 < tolerance_fraction < 1:
        raise ValueError("input_size_tolerance must be between 0 and 1")
    lower = max(1, int(input_target_bytes * (1.0 - tolerance_fraction)))
    # Source files must stay below the small-file threshold or phase B may
    # legitimately skip them.  This is an inclusive bound, hence ``- 1``.
    upper = min(
        int(input_target_bytes * (1.0 + tolerance_fraction)),
        int(compaction_target_bytes) - 1,
    )
    if lower > upper:
        raise ValueError(
            "input size tolerance cannot fit below the compaction target: "
            f"lower={lower}, upper={upper}, target={compaction_target_bytes}"
        )
    return lower, upper


def _write_calibrated_source(
    *,
    file_index: int,
    file_count: int,
    first_id: int,
    initial_rows: int,
    path: Path,
    compression_level: int,
    input_target_bytes: int | None,
    input_size_tolerance: float,
    compaction_target_bytes: int,
    max_attempts: int,
) -> tuple[pl.DataFrame, dict]:
    """Encode one deterministic source, optionally calibrating its byte size."""

    if input_target_bytes is None:
        frame = _frame_for_file(
            file_index,
            initial_rows,
            file_count=file_count,
            first_id=first_id,
        )
        size = _write_source_frame(frame, path, compression_level)
        return frame, {
            "attempts": 1,
            "target_bytes": None,
            "lower_bytes": None,
            "upper_bytes": None,
            "within_target": None,
        }

    lower_bytes, upper_bytes = _calibration_bounds(
        input_target_bytes, input_size_tolerance, compaction_target_bytes
    )
    desired_bytes = (lower_bytes + upper_bytes) // 2
    rows = max(4, int(initial_rows))
    low_rows: int | None = None
    high_rows: int | None = None
    attempts: list[dict[str, int]] = []
    seen_rows: set[int] = set()
    frame: pl.DataFrame | None = None
    size = 0

    for _attempt in range(max(1, int(max_attempts))):
        seen_rows.add(rows)
        frame = _frame_for_file(
            file_index,
            rows,
            file_count=file_count,
            first_id=first_id,
        )
        size = _write_source_frame(frame, path, compression_level)
        attempts.append({"rows": rows, "bytes": size})
        if lower_bytes <= size <= upper_bytes:
            return frame, {
                "attempts": len(attempts),
                "target_bytes": input_target_bytes,
                "lower_bytes": lower_bytes,
                "upper_bytes": upper_bytes,
                "within_target": True,
                "attempt_history": attempts,
            }

        if size < lower_bytes:
            low_rows = max(low_rows or 0, rows)
        else:
            high_rows = min(high_rows or rows, rows)

        if low_rows is not None and high_rows is not None:
            candidate = (low_rows + high_rows) // 2
        else:
            candidate = max(4, int(round(rows * desired_bytes / max(1, size))))
        if candidate == rows:
            candidate += 1 if size < lower_bytes else -1
        if candidate in seen_rows:
            if low_rows is not None and high_rows is not None:
                untried = [
                    value
                    for value in range(low_rows + 1, high_rows)
                    if value not in seen_rows
                ]
                if not untried:
                    break
                candidate = untried[len(untried) // 2]
            else:
                candidate = max(4, candidate + (1 if size < lower_bytes else -1))
        rows = max(4, candidate)

    observed = ", ".join(
        f"{entry['rows']} rows={entry['bytes']} bytes" for entry in attempts
    )
    raise RuntimeError(
        f"could not calibrate source file {file_index} to "
        f"[{lower_bytes}, {upper_bytes}] bytes in {len(attempts)} attempts; "
        f"observed: {observed}"
    )


def _tombstone_part(path: str, first_id: int, rows: int, dead: int) -> pl.DataFrame:
    # Evenly-spaced deterministic IDs exercise all row groups while retaining
    # at least one live row in every source.
    ordinal = pl.int_range(0, dead, eager=True, dtype=pl.Int64)
    rowids = first_id + ((ordinal * rows) // dead)
    return pl.DataFrame(
        {
            TOMBSTONE_FILE_COL: pl.repeat(path, dead, eager=True),
            ROWID_COL: rowids,
        }
    )


def _build_corpus(
    root: Path,
    rows_per_file: int,
    compression_level: int,
    *,
    file_count: int = FILE_COUNT,
    tombstone_rows: int | None = None,
    input_file_target_bytes: int | None = None,
    input_size_tolerance: float = 0.02,
    compaction_target_bytes: int = DEFAULT_TARGET_BYTES,
    calibration_max_attempts: int = 8,
) -> dict:
    source_dir = root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    resources = []
    schemas: list[list[str]] = []
    newest_schema: list[dict[str, str]] | None = None
    calibration: list[dict] = []
    first_ids: list[int] = []
    next_id = 1

    for file_index in range(file_count):
        first_id = next_id
        path = source_dir / f"part-{file_index:02d}.parquet"
        frame, calibration_record = _write_calibrated_source(
            file_index=file_index,
            file_count=file_count,
            first_id=first_id,
            initial_rows=rows_per_file,
            path=path,
            compression_level=compression_level,
            input_target_bytes=input_file_target_bytes,
            input_size_tolerance=input_size_tolerance,
            compaction_target_bytes=compaction_target_bytes,
            max_attempts=calibration_max_attempts,
        )
        next_id += frame.height
        first_ids.append(first_id)
        schemas.append(list(frame.columns))
        if file_index == file_count - 1:
            newest_schema = _schema_json(frame)
        size = path.stat().st_size
        resources.append(
            {
                "file": str(path),
                "file_size": size,
                "rows": frame.height,
                "columns": frame.width,
            }
        )
        calibration.append(
            {
                "file_index": file_index,
                "rows": frame.height,
                "bytes": size,
                **calibration_record,
            }
        )

    rows_by_file = [int(resource["rows"]) for resource in resources]
    if tombstone_rows is None:
        tombstones_per_file = [(rows + 3) // 4 for rows in rows_by_file]
    else:
        tombstones_per_file = _allocate_tombstones(rows_by_file, tombstone_rows)
    tombstone_parts = [
        _tombstone_part(str(resource["file"]), first_id, int(resource["rows"]), dead)
        for resource, first_id, dead in zip(resources, first_ids, tombstones_per_file)
    ]

    tombstones = pl.concat(tombstone_parts, how="vertical")
    tombstone_path = root / "deletion-vector.parquet"
    tombstones.write_parquet(
        tombstone_path,
        compression="zstd",
        compression_level=compression_level,
        statistics=True,
    )
    return {
        "snapshot": {"resources": resources},
        "tombstones": tombstones,
        "tombstone_path": str(tombstone_path),
        "source_paths": [resource["file"] for resource in resources],
        "physical_columns": sorted({column for schema in schemas for column in schema}),
        "newest_schema": newest_schema or [],
        "diversity": _diversity_record(schemas, file_count=file_count),
        "tombstones_per_file": tombstones_per_file,
        "rows_per_file": rows_by_file,
        "calibration": calibration,
    }


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parquet_list(paths: Sequence[str]) -> str:
    if not paths:
        raise ValueError("digest requires at least one Parquet file")
    return "[" + ",".join(_sql_string(path) for path in paths) + "]"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(MIB):
            digest.update(block)
    return digest.hexdigest()


CORPUS_MANIFEST_NAME = "corpus-manifest.json"


def _save_corpus_manifest(root: Path, corpus: dict, configuration: dict) -> dict:
    """Persist a source corpus for byte-identical cross-version container runs."""

    source_hashes = {
        resource["file"]: _sha256_file(Path(resource["file"]))
        for resource in corpus["snapshot"]["resources"]
    }
    manifest = {
        "format": "supertable-tombstone-corpus-v1",
        # Tombstone file keys deliberately contain this absolute path.  Both
        # comparison containers therefore mount the corpus at the same path.
        "root": str(root.resolve()),
        "configuration": configuration,
        "corpus": {key: value for key, value in corpus.items() if key != "tombstones"},
        "sha256": {
            "sources": source_hashes,
            "tombstones": _sha256_file(Path(corpus["tombstone_path"])),
        },
    }
    target = root / CORPUS_MANIFEST_NAME
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(target)
    return manifest


def _load_corpus(root: Path, *, verify_hashes: bool = True) -> dict:
    manifest_path = root / CORPUS_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != "supertable-tombstone-corpus-v1":
        raise ValueError(f"unsupported corpus manifest: {manifest_path}")
    if Path(manifest["root"]).resolve() != root.resolve():
        raise ValueError(
            "corpus must be mounted at the path recorded during preparation "
            f"({manifest['root']}); tombstone file keys are path-qualified"
        )
    corpus = dict(manifest["corpus"])
    paths = [Path(resource["file"]) for resource in corpus["snapshot"]["resources"]]
    tombstone_path = Path(corpus["tombstone_path"])
    missing = [str(path) for path in [*paths, tombstone_path] if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"corpus files are missing: {missing}")
    if verify_hashes:
        expected_sources = manifest["sha256"]["sources"]
        for path in paths:
            actual = _sha256_file(path)
            if actual != expected_sources[str(path)]:
                raise ValueError(f"source corpus checksum mismatch: {path}")
        if _sha256_file(tombstone_path) != manifest["sha256"]["tombstones"]:
            raise ValueError("tombstone corpus checksum mismatch")
    corpus["tombstones"] = pl.read_parquet(tombstone_path)
    corpus["manifest"] = manifest
    return corpus


def prepare_corpus(
    directory: str,
    *,
    rows_per_file: int,
    file_count: int,
    tombstone_rows: int | None,
    compression_level: int,
    target_bytes: int,
    input_file_target_bytes: int | None,
    input_size_tolerance: float,
    calibration_max_attempts: int,
    rss_sample_ms: float = 5.0,
) -> dict:
    """Prepare and checksum the immutable corpus without running compaction."""

    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=False)
    sample_interval = max(float(rss_sample_ms), 1.0) / 1000.0
    measured = _measure(
        lambda: _build_corpus(
            root,
            rows_per_file,
            compression_level,
            file_count=file_count,
            tombstone_rows=tombstone_rows,
            input_file_target_bytes=input_file_target_bytes,
            input_size_tolerance=input_size_tolerance,
            compaction_target_bytes=target_bytes,
            calibration_max_attempts=calibration_max_attempts,
        ),
        sample_interval,
    )
    corpus = measured.value
    configuration = {
        "rows_per_file_seed": rows_per_file,
        "file_count": file_count,
        "tombstone_rows": corpus["tombstones"].height,
        "compression_level": compression_level,
        "target_bytes": target_bytes,
        "input_file_target_bytes": input_file_target_bytes,
        "input_size_tolerance": input_size_tolerance,
        "calibration_max_attempts": calibration_max_attempts,
    }
    manifest = _save_corpus_manifest(root, corpus, configuration)
    resources = corpus["snapshot"]["resources"]
    return {
        "manifest": str(root / CORPUS_MANIFEST_NAME),
        "root": str(root),
        "configuration": configuration,
        "input_files": len(resources),
        "input_rows": sum(int(resource["rows"]) for resource in resources),
        "input_bytes": sum(int(resource["file_size"]) for resource in resources),
        "tombstone_rows": corpus["tombstones"].height,
        "tombstones_per_file": corpus["tombstones_per_file"],
        "sha256": manifest["sha256"],
        "generation": measured.metrics,
    }


def _canonical_digest(
    *,
    paths: Sequence[str],
    columns: Sequence[str],
    csv_path: Path,
    tombstone_path: str | None = None,
) -> dict:
    projection = ", ".join(f"src.{_quote_identifier(c)}" for c in columns)
    scan = (
        f"read_parquet({_parquet_list(paths)}, union_by_name=true, "
        f"filename={'true' if tombstone_path else 'false'}, "
        "hive_partitioning=false)"
    )
    anti = ""
    if tombstone_path:
        anti = (
            " WHERE NOT EXISTS (SELECT 1 FROM "
            f"read_parquet({_sql_string(tombstone_path)}, "
            "hive_partitioning=false) AS dv "
            f"WHERE dv.{_quote_identifier(TOMBSTONE_FILE_COL)} = src.filename "
            f"AND dv.{_quote_identifier(ROWID_COL)} = "
            f"src.{_quote_identifier(ROWID_COL)})"
        )
    query = f"SELECT {projection} FROM {scan} AS src{anti} ORDER BY src.id"
    connection = duckdb.connect()
    try:
        described = connection.execute(f"DESCRIBE {query}").fetchall()
        row_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM ({query}) AS canonical"
            ).fetchone()[0]
        )
        connection.execute(
            f"COPY ({query}) TO {_sql_string(csv_path)} "
            "(FORMAT CSV, HEADER false, DELIMITER '|', NULL '<NULL>')"
        )
    finally:
        connection.close()
    canonical_bytes = csv_path.stat().st_size
    sha256 = _sha256_file(csv_path)
    csv_path.unlink()
    return {
        "sha256": sha256,
        "canonical_bytes": canonical_bytes,
        "rows": row_count,
        "columns": list(columns),
        "schema": [{"name": str(row[0]), "type": str(row[1])} for row in described],
    }


def _aggregate_readback(
    *,
    paths: Sequence[str],
    columns: Sequence[str],
    tombstone_path: str | None = None,
) -> dict:
    """Return stable count/min/max/average values from an independent reader."""

    projection = ", ".join(f"src.{_quote_identifier(c)}" for c in columns)
    scan = (
        f"read_parquet({_parquet_list(paths)}, union_by_name=true, "
        f"filename={'true' if tombstone_path else 'false'}, "
        "hive_partitioning=false)"
    )
    anti = ""
    if tombstone_path:
        anti = (
            " WHERE NOT EXISTS (SELECT 1 FROM "
            f"read_parquet({_sql_string(tombstone_path)}, "
            "hive_partitioning=false) AS dv "
            f"WHERE dv.{_quote_identifier(TOMBSTONE_FILE_COL)} = src.filename "
            f"AND dv.{_quote_identifier(ROWID_COL)} = "
            f"src.{_quote_identifier(ROWID_COL)})"
        )
    query = f"SELECT {projection} FROM {scan} AS src{anti}"
    connection = duckdb.connect()
    try:
        described = connection.execute(f"DESCRIBE {query}").fetchall()
        numeric_prefixes = (
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
            "FLOAT",
            "DOUBLE",
            "REAL",
            "DECIMAL",
        )
        numeric_columns = [
            str(row[0])
            for row in described
            if str(row[1]).upper().startswith(numeric_prefixes)
        ]
        expressions = ["COUNT(*)"]
        for column in numeric_columns:
            quoted = _quote_identifier(column)
            expressions.extend(
                [
                    f"COUNT({quoted})",
                    f"CAST(MIN({quoted}) AS VARCHAR)",
                    f"CAST(MAX({quoted}) AS VARCHAR)",
                    # A fixed decimal accumulation is independent of output
                    # file grouping and floating-point reduction order.
                    f"CAST(AVG(CAST({quoted} AS DECIMAL(38, 12))) AS VARCHAR)",
                ]
            )
        values = connection.execute(
            f"SELECT {', '.join(expressions)} FROM ({query}) AS aggregate_source"
        ).fetchone()
    finally:
        connection.close()

    if values is None:
        raise AssertionError("aggregate query returned no result")
    result: dict = {"row_count": int(values[0]), "numeric_columns": {}}
    offset = 1
    for column in numeric_columns:
        non_null_count, minimum, maximum, average = values[offset : offset + 4]
        result["numeric_columns"][column] = {
            "non_null_count": int(non_null_count),
            "null_count": int(values[0]) - int(non_null_count),
            "min": minimum,
            "max": maximum,
            "avg_decimal_12": average,
        }
        offset += 4
    return result


def _size_distribution(resources: Sequence[dict], target_bytes: int) -> dict:
    sizes = sorted(int(resource.get("file_size") or 0) for resource in resources)
    if not sizes:
        return {
            "count": 0,
            "total_bytes": 0,
            "min_bytes": 0,
            "median_bytes": 0,
            "max_bytes": 0,
            "over_target_count": 0,
            "max_to_target_ratio": 0.0,
        }
    return {
        "count": len(sizes),
        "total_bytes": sum(sizes),
        "min_bytes": sizes[0],
        "median_bytes": int(statistics.median(sizes)),
        "max_bytes": sizes[-1],
        "over_target_count": sum(size > target_bytes for size in sizes),
        "max_to_target_ratio": round(sizes[-1] / target_bytes, 4),
    }


def _phase_record(measurement: _Measured, profiler: Profiler) -> dict:
    result = dict(measurement.metrics)
    result["telemetry"] = {
        "timings_seconds": profiler.emit_timings(),
        "counts": profiler.emit_counts(),
        "note": (
            "Per-span time is cumulative across workers and may exceed phase wall time."
        ),
    }
    counts = profiler.emit_counts()
    encode_seconds = float(profiler.timings.get("write.parquet_encode", 0.0))
    wall_seconds = float(result["wall_seconds"])
    input_bytes = int(counts.get("bytes_read", 0))
    output_bytes = int(counts.get("bytes_written", 0))
    result["throughput"] = {
        "input_mib_per_wall_second": round(
            input_bytes / MIB / wall_seconds if wall_seconds else 0.0, 3
        ),
        "output_mib_per_wall_second": round(
            output_bytes / MIB / wall_seconds if wall_seconds else 0.0, 3
        ),
        "output_mib_per_aggregate_encode_second": round(
            output_bytes / MIB / encode_seconds if encode_seconds else 0.0, 3
        ),
        "aggregate_parquet_encode_seconds": round(encode_seconds, 6),
    }
    return result


@contextmanager
def _benchmark_root(work_dir: str | None) -> Iterator[Path]:
    if work_dir:
        root = Path(work_dir).resolve()
        root.mkdir(parents=True, exist_ok=False)
        yield root
        return
    with tempfile.TemporaryDirectory(prefix="st-tombstone-benchmark-") as temporary:
        yield Path(temporary)


def run_benchmark(
    *,
    rows_per_file: int = 100_000,
    file_count: int = FILE_COUNT,
    tombstone_rows: int | None = None,
    compression_level: int = 1,
    workers: int = 4,
    target_bytes: int = DEFAULT_TARGET_BYTES,
    input_file_target_bytes: int | None = None,
    input_size_tolerance: float = 0.02,
    calibration_max_attempts: int = 8,
    rss_sample_ms: float = 5.0,
    work_dir: str | None = None,
    input_corpus_dir: str | None = None,
    verify_corpus_hashes: bool = True,
    label: str = "benchmark",
    fused: bool = False,
) -> dict:
    if rows_per_file < 4:
        raise ValueError("rows_per_file must be at least 4")
    if file_count < 1:
        raise ValueError("file_count must be positive")
    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")
    if fused and "tombstone_df" not in inspect.signature(compact_resources).parameters:
        raise RuntimeError(
            "this installed supertable revision does not support fused "
            "tombstone compaction; run it with --two-phase"
        )
    workers = max(1, min(int(workers), 8, file_count))
    sample_interval = max(float(rss_sample_ms), 1.0) / 1000.0

    with _benchmark_root(work_dir) as root:
        if input_corpus_dir:
            corpus_root = Path(input_corpus_dir).resolve()
            built = _measure(
                lambda: _load_corpus(corpus_root, verify_hashes=verify_corpus_hashes),
                sample_interval,
            )
            corpus_mode = "shared_manifest"
        else:
            built = _measure(
                lambda: _build_corpus(
                    root,
                    rows_per_file,
                    compression_level,
                    file_count=file_count,
                    tombstone_rows=tombstone_rows,
                    input_file_target_bytes=input_file_target_bytes,
                    input_size_tolerance=input_size_tolerance,
                    compaction_target_bytes=target_bytes,
                    calibration_max_attempts=calibration_max_attempts,
                ),
                sample_interval,
            )
            corpus_mode = "generated_for_run"
        corpus = built.value
        if corpus_mode == "shared_manifest":
            prepared = corpus["manifest"]["configuration"]
            if int(prepared["file_count"]) != file_count:
                raise ValueError(
                    "--file-count must match the shared corpus manifest "
                    f"({prepared['file_count']})"
                )
            if int(prepared["target_bytes"]) != target_bytes:
                raise ValueError(
                    "--target-mib must match the threshold used to calibrate "
                    f"the shared corpus ({prepared['target_bytes'] / MIB:.3f} MiB)"
                )
            rows_per_file = int(prepared["rows_per_file_seed"])
            tombstone_rows = int(prepared["tombstone_rows"])
            input_file_target_bytes = prepared.get("input_file_target_bytes")
            input_size_tolerance = float(prepared["input_size_tolerance"])
            calibration_max_attempts = int(prepared["calibration_max_attempts"])
        resources = corpus["snapshot"]["resources"]
        tombstones: pl.DataFrame = corpus["tombstones"]
        input_bytes = sum(int(resource["file_size"]) for resource in resources)
        input_rows = sum(int(resource["rows"]) for resource in resources)
        expected_live_rows = input_rows - tombstones.height
        if len(resources) != file_count:
            raise AssertionError(f"expected {file_count} source files")
        if tombstones.get_column(TOMBSTONE_FILE_COL).n_unique() != file_count:
            raise AssertionError("every source file must be touched by tombstones")

        # The local backend is root-confined; benchmark resources live under
        # the corpus directory, so scope the backend to that directory.
        storage_root = os.path.commonpath([
            str(root), os.path.abspath(str(resources[0]["file"])),
        ])
        local_storage = LocalStorage(storage_root)
        footer_md_cache: dict = {}
        table_config = {
            "max_memory_chunk_size": int(target_bytes),
            "max_overlapping_files": file_count,
            "max_tombstone_rows": max(1, tombstones.height // 2),
            "tombstone_compaction_workers": workers,
        }
        phase_measurements: dict[str, _Measured] = {}
        phase_profilers: dict[str, Profiler] = {}
        mode_summary: dict = {}

        if fused:
            fused_profiler = Profiler()
            phase_profilers["fused_compaction"] = fused_profiler
            with patch(
                "supertable.processing._get_storage", return_value=local_storage
            ):
                fused_measurement = _measure(
                    lambda: _call_compatible(
                        compact_resources,
                        snapshot=corpus["snapshot"],
                        data_dir=str(root / "fused-compacted"),
                        compression_level=compression_level,
                        table_config=table_config,
                        small_only=True,
                        required_reads=True,
                        profiler=fused_profiler,
                        footer_md_out=footer_md_cache,
                        tombstone_df=tombstones,
                        return_residual=True,
                    ),
                    sample_interval,
                )
            phase_measurements["fused_compaction"] = fused_measurement
            (
                considered,
                compacted_rows,
                compacted_resources,
                resource_sunset,
                residual,
            ) = fused_measurement.value
            untouched_sources = [
                resource
                for resource in resources
                if resource["file"] not in resource_sunset
            ]
            final_live_resources = untouched_sources + compacted_resources
            final_live_rows = sum(
                int(resource.get("rows") or 0) for resource in final_live_resources
            )
            if residual.height:
                raise AssertionError(
                    f"fused compaction left {residual.height} residual tombstones"
                )
            if considered != len(resource_sunset) or len(resource_sunset) != file_count:
                raise AssertionError(
                    "fused compaction must consume every tombstoned source: "
                    f"considered={considered}, sunset={len(resource_sunset)}"
                )
            if (
                compacted_rows != expected_live_rows
                or final_live_rows != expected_live_rows
            ):
                raise AssertionError(
                    "fused compaction row count changed: "
                    f"expected={expected_live_rows}, written={compacted_rows}, "
                    f"final={final_live_rows}"
                )
            mode_summary = {
                "fused_candidates": considered,
                "fused_rows_written": compacted_rows,
                "fused_replacement_files": len(compacted_resources),
                "fused_residual_tombstone_rows": residual.height,
                "untouched_source_files": len(untouched_sources),
                "intermediate_successor_files": 0,
                "intermediate_bytes_reread": 0,
                "phase_a_bytes_immediately_read_by_phase_b": 0,
                "fused_output_sizes": _size_distribution(
                    compacted_resources, target_bytes
                ),
            }
        else:
            phase_a_profiler = Profiler()
            phase_profilers["tombstone_rewrite"] = phase_a_profiler
            phase_a_dir = root / "phase-a-survivors"
            with patch(
                "supertable.processing._get_storage", return_value=local_storage
            ):
                phase_a = _measure(
                    lambda: _call_compatible(
                        compact_tombstones,
                        snapshot=corpus["snapshot"],
                        tombstone_df=tombstones,
                        data_dir=str(phase_a_dir),
                        compression_level=compression_level,
                        table_config=table_config,
                        profiler=phase_a_profiler,
                        return_residual=True,
                        footer_md_out=footer_md_cache,
                    ),
                    sample_interval,
                )
            phase_measurements["tombstone_rewrite"] = phase_a
            removed, survivor_resources, tombstone_sunset, residual = phase_a.value
            if removed != tombstones.height or residual.height:
                raise AssertionError(
                    "tombstone drain did not consume the exact deletion vector: "
                    f"removed={removed}, vector={tombstones.height}, "
                    f"residual={residual.height}"
                )
            if (
                len(tombstone_sunset) != file_count
                or len(survivor_resources) != file_count
            ):
                raise AssertionError(
                    "all partially-dead files must be rewritten exactly once"
                )

            phase_b_profiler = Profiler()
            phase_profilers["small_file_merge"] = phase_b_profiler
            phase_b_dir = root / "phase-b-compacted"
            with patch(
                "supertable.processing._get_storage", return_value=local_storage
            ):
                phase_b = _measure(
                    lambda: _call_compatible(
                        compact_resources,
                        snapshot={"resources": survivor_resources},
                        data_dir=str(phase_b_dir),
                        compression_level=compression_level,
                        table_config=table_config,
                        small_only=True,
                        required_reads=True,
                        profiler=phase_b_profiler,
                        footer_md_out=footer_md_cache,
                    ),
                    sample_interval,
                )
            phase_measurements["small_file_merge"] = phase_b
            considered, compacted_rows, compacted_resources, resource_sunset = (
                phase_b.value
            )
            untouched_survivors = [
                resource
                for resource in survivor_resources
                if resource["file"] not in resource_sunset
            ]
            final_live_resources = untouched_survivors + compacted_resources
            final_live_rows = sum(
                int(resource.get("rows") or 0) for resource in final_live_resources
            )
            if considered != len(resource_sunset):
                raise AssertionError(
                    "small-file compaction candidate/sunset counts disagree: "
                    f"considered={considered}, sunset={len(resource_sunset)}"
                )
            if compacted_rows != sum(
                int(resource.get("rows") or 0)
                for resource in survivor_resources
                if resource["file"] in resource_sunset
            ):
                raise AssertionError(
                    "small-file compaction row accounting is inconsistent"
                )
            if final_live_rows != expected_live_rows:
                raise AssertionError(
                    f"row count changed: expected {expected_live_rows}, "
                    f"got {final_live_rows}"
                )
            mode_summary = {
                "phase_a_survivor_files": len(survivor_resources),
                "phase_b_candidates": considered,
                "phase_b_compacted_rows": compacted_rows,
                "phase_b_replacement_files": len(compacted_resources),
                "skipped_large_phase_a_successors": len(untouched_survivors),
                "skipped_large_phase_a_successor_bytes": sum(
                    int(resource.get("file_size") or 0)
                    for resource in untouched_survivors
                ),
                "intermediate_successor_files": len(survivor_resources),
                "intermediate_bytes_reread": int(
                    phase_b_profiler.counts.get("bytes_read", 0)
                ),
                "phase_a_bytes_immediately_read_by_phase_b": int(
                    phase_b_profiler.counts.get("bytes_read", 0)
                ),
                "phase_a_output_sizes": _size_distribution(
                    survivor_resources, target_bytes
                ),
                "phase_b_replacement_sizes": _size_distribution(
                    compacted_resources, target_bytes
                ),
            }

        # Correctness verification happens after both measured phases so the
        # independent DuckDB oracle cannot inflate their RSS baselines.  Each
        # canonical CSV is hashed and removed immediately, bounding temporary
        # disk use to one result stream even for the ~300 MiB corpus.
        def _expected_readback() -> tuple[dict, dict, dict]:
            return (
                _canonical_digest(
                    paths=corpus["source_paths"],
                    columns=LATEST_PUBLIC_COLUMNS,
                    csv_path=root / "expected-authoritative.csv",
                    tombstone_path=corpus["tombstone_path"],
                ),
                _canonical_digest(
                    paths=corpus["source_paths"],
                    columns=corpus["physical_columns"],
                    csv_path=root / "expected-physical.csv",
                    tombstone_path=corpus["tombstone_path"],
                ),
                _aggregate_readback(
                    paths=corpus["source_paths"],
                    columns=corpus["physical_columns"],
                    tombstone_path=corpus["tombstone_path"],
                ),
            )

        final_paths = [resource["file"] for resource in final_live_resources]

        def _actual_readback() -> tuple[dict, dict, dict]:
            return (
                _canonical_digest(
                    paths=final_paths,
                    columns=LATEST_PUBLIC_COLUMNS,
                    csv_path=root / "actual-authoritative.csv",
                ),
                _canonical_digest(
                    paths=final_paths,
                    columns=corpus["physical_columns"],
                    csv_path=root / "actual-physical.csv",
                ),
                _aggregate_readback(
                    paths=final_paths,
                    columns=corpus["physical_columns"],
                ),
            )

        expected_readback = _measure(_expected_readback, sample_interval)
        actual_readback = _measure(_actual_readback, sample_interval)
        expected_authoritative, expected_physical, expected_aggregates = (
            expected_readback.value
        )
        actual_authoritative, actual_physical, actual_aggregates = actual_readback.value
        authoritative_match = expected_authoritative == actual_authoritative
        physical_match = expected_physical == actual_physical
        aggregate_match = expected_aggregates == actual_aggregates
        if not authoritative_match or not physical_match or not aggregate_match:
            raise AssertionError(
                "compaction result digest mismatch: "
                f"authoritative={authoritative_match}, physical={physical_match}, "
                f"aggregates={aggregate_match}"
            )

        metadata_profiler = Profiler()

        def _build_final_metadata():
            return _call_compatible(
                extract_stats_rows,
                file_paths=final_paths,
                profiler=metadata_profiler,
                footer_md_cache=footer_md_cache,
            )

        metadata = _measure(_build_final_metadata, sample_interval)
        stats_frame = metadata.value
        metadata_record = _phase_record(metadata, metadata_profiler)
        metadata_record.update(
            {
                "stats_rows": stats_frame.height,
                # Compaction is a physical rewrite; logical metadata remains the
                # newest upload's authoritative schema. The independent physical
                # union digest above validates every retained legacy column.
                "schema": corpus["newest_schema"],
                "footer_cache_entries": len(footer_md_cache),
                "final_footer_cache_hits": int(
                    metadata_profiler.counts.get("stats_footer_cache_hit", 0)
                ),
            }
        )

        phase_records = {
            name: _phase_record(measurement, phase_profilers[name])
            for name, measurement in phase_measurements.items()
        }
        compaction_wall = sum(
            float(record["wall_seconds"]) for record in phase_records.values()
        )
        compaction_cpu = sum(
            float(record["cpu_seconds"]) for record in phase_records.values()
        )
        affinity = (
            len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
        )
        total_bytes_read = sum(
            int(profiler.counts.get("bytes_read", 0))
            for profiler in phase_profilers.values()
        )
        total_bytes_written = sum(
            int(profiler.counts.get("bytes_written", 0))
            for profiler in phase_profilers.values()
        )
        final_live_bytes = sum(
            int(resource.get("file_size") or 0) for resource in final_live_resources
        )
        proc_io_deltas = [
            record.get("proc_io_delta")
            for record in phase_records.values()
            if record.get("proc_io_delta") is not None
        ]
        compaction_proc_io = {
            key: sum(int(delta.get(key, 0)) for delta in proc_io_deltas)
            for key in sorted({key for delta in proc_io_deltas for key in delta})
        }
        calibration_records = corpus["calibration"]
        calibrated_count = sum(
            record.get("within_target") is True for record in calibration_records
        )

        return {
            "benchmark": "tombstone_compaction_v2",
            "label": label,
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "polars": pl.__version__,
                "duckdb": duckdb.__version__,
                "supertable": SUPERTABLE_VERSION,
                "cpu_affinity_count": affinity,
            },
            "configuration": {
                "file_count": file_count,
                "rows_per_file": rows_per_file,
                "requested_tombstone_rows": tombstone_rows,
                "target_bytes": target_bytes,
                "target_mib": round(target_bytes / MIB, 3),
                "input_file_target_bytes": input_file_target_bytes,
                "input_file_target_mib": (
                    round(input_file_target_bytes / MIB, 3)
                    if input_file_target_bytes is not None
                    else None
                ),
                "input_size_tolerance_fraction": input_size_tolerance,
                "calibration_max_attempts": calibration_max_attempts,
                "compression": f"zstd:{compression_level}",
                "tombstone_workers": workers,
                "tombstone_fraction": round(tombstones.height / input_rows, 8),
                "max_tombstone_rows": table_config["max_tombstone_rows"],
                "fused": bool(fused),
                "input_corpus_dir": (
                    str(Path(input_corpus_dir).resolve()) if input_corpus_dir else None
                ),
                "verify_corpus_hashes": bool(verify_corpus_hashes),
            },
            "corpus": {
                "mode": corpus_mode,
                "input_files": file_count,
                "input_rows": input_rows,
                "rows_per_file": corpus["rows_per_file"],
                "input_bytes": input_bytes,
                "input_mib": round(input_bytes / MIB, 3),
                "tombstone_rows": tombstones.height,
                "tombstones_per_file": corpus["tombstones_per_file"],
                "tombstone_bytes": Path(corpus["tombstone_path"]).stat().st_size,
                "expected_live_rows": expected_live_rows,
                "input_file_sizes": _size_distribution(resources, target_bytes),
                "input_size_calibration": {
                    "enabled": input_file_target_bytes is not None,
                    "calibrated_files": calibrated_count,
                    "all_within_target": (
                        calibrated_count == file_count
                        if input_file_target_bytes is not None
                        else None
                    ),
                    "files": calibration_records,
                },
                "preparation_or_load": built.metrics,
                # Keep the old key for consumers of the original benchmark.
                "generation": built.metrics,
                "manifest_sha256": (
                    corpus.get("manifest", {}).get("sha256")
                    if corpus_mode == "shared_manifest"
                    else None
                ),
                "diversity": corpus["diversity"],
                "newest_authoritative_schema": corpus["newest_schema"],
                "physical_union_columns": corpus["physical_columns"],
            },
            "phases": {**phase_records, "final_metadata": metadata_record},
            "summary": {
                "compaction_wall_seconds": round(compaction_wall, 6),
                "compaction_cpu_seconds": round(compaction_cpu, 6),
                "compaction_cpu_equivalent_cores": round(
                    compaction_cpu / compaction_wall if compaction_wall else 0.0, 4
                ),
                "total_bytes_read": total_bytes_read,
                "total_bytes_written": total_bytes_written,
                "compaction_proc_io_delta": compaction_proc_io,
                "read_amplification_vs_final_bytes": round(
                    total_bytes_read / final_live_bytes if final_live_bytes else 0.0,
                    4,
                ),
                "write_amplification_vs_final_bytes": round(
                    total_bytes_written / final_live_bytes if final_live_bytes else 0.0,
                    4,
                ),
                "peak_rss_bytes": max(
                    int(record["rss_peak_bytes"]) for record in phase_records.values()
                ),
                "final_files": len(final_live_resources),
                "final_rows": final_live_rows,
                "final_output_sizes": _size_distribution(
                    final_live_resources, target_bytes
                ),
                **mode_summary,
            },
            "correctness": {
                "authoritative_projection": {
                    "expected": expected_authoritative,
                    "actual": actual_authoritative,
                    "match": authoritative_match,
                },
                "physical_union": {
                    "expected": expected_physical,
                    "actual": actual_physical,
                    "match": physical_match,
                },
                "aggregates": {
                    "expected": expected_aggregates,
                    "actual": actual_aggregates,
                    "match": aggregate_match,
                },
                "readback_telemetry": {
                    "oracle_sources_minus_tombstones": expected_readback.metrics,
                    "compacted_result": actual_readback.metrics,
                },
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-per-file", type=int, default=100_000)
    parser.add_argument("--file-count", type=int, default=FILE_COUNT)
    parser.add_argument(
        "--tombstone-rows",
        type=int,
        help="Exact total, distributed across every file while retaining survivors.",
    )
    parser.add_argument("--compression-level", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--target-mib", type=float, default=16.0)
    parser.add_argument(
        "--input-file-target-mib",
        type=float,
        help="Calibrate each source near this encoded size and below --target-mib.",
    )
    parser.add_argument("--input-size-tolerance-pct", type=float, default=2.0)
    parser.add_argument("--calibration-max-attempts", type=int, default=8)
    parser.add_argument("--rss-sample-ms", type=float, default=5.0)
    parser.add_argument("--work-dir")
    corpus_mode = parser.add_mutually_exclusive_group()
    corpus_mode.add_argument(
        "--prepare-corpus",
        help="Create a checksummed immutable corpus at this path, then exit.",
    )
    corpus_mode.add_argument(
        "--input-corpus",
        help="Reuse a prepared corpus mounted at the exact recorded path.",
    )
    parser.add_argument(
        "--skip-corpus-hash-verification",
        action="store_true",
        help="Skip source checksums when loading a prepared corpus.",
    )
    parser.add_argument("--label", default="benchmark")
    parser.add_argument("--output")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--fused",
        action="store_true",
        help="Fuse deletion-vector draining and final packing into one pass.",
    )
    mode.add_argument(
        "--two-phase",
        action="store_true",
        help="Explicitly select the revision-compatible rewrite-then-merge path.",
    )
    args = parser.parse_args()
    target_bytes = int(args.target_mib * MIB)
    input_file_target_bytes = (
        int(args.input_file_target_mib * MIB)
        if args.input_file_target_mib is not None
        else None
    )
    if args.prepare_corpus:
        result = prepare_corpus(
            args.prepare_corpus,
            rows_per_file=args.rows_per_file,
            file_count=args.file_count,
            tombstone_rows=args.tombstone_rows,
            compression_level=args.compression_level,
            target_bytes=target_bytes,
            input_file_target_bytes=input_file_target_bytes,
            input_size_tolerance=args.input_size_tolerance_pct / 100.0,
            calibration_max_attempts=args.calibration_max_attempts,
            rss_sample_ms=args.rss_sample_ms,
        )
    else:
        result = run_benchmark(
            rows_per_file=args.rows_per_file,
            file_count=args.file_count,
            tombstone_rows=args.tombstone_rows,
            compression_level=args.compression_level,
            workers=args.workers,
            target_bytes=target_bytes,
            input_file_target_bytes=input_file_target_bytes,
            input_size_tolerance=args.input_size_tolerance_pct / 100.0,
            calibration_max_attempts=args.calibration_max_attempts,
            rss_sample_ms=args.rss_sample_ms,
            work_dir=args.work_dir,
            input_corpus_dir=args.input_corpus,
            verify_corpus_hashes=not args.skip_corpus_hash_verification,
            label=args.label,
            fused=args.fused,
        )
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        temporary.replace(output)
    print(payload)


if __name__ == "__main__":
    main()
