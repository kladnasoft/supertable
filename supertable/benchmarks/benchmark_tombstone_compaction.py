"""Deterministic tombstone-drain and small-file compaction benchmark.

By default this benchmark exercises the two historical data-plane phases used
by a write that crosses the deletion-vector threshold:

1. ``compact_tombstones`` rewrites every partially-dead source file.
2. ``compact_resources`` folds the survivor files toward a 16 MiB target.

``--fused`` instead calls ``compact_resources`` once with the deletion vector,
so decoded survivors flow directly into final target-sized outputs.  Both modes
share the exact corpus and oracle, making their wall/CPU/RSS/I/O deltas directly
comparable.

The corpus has exactly twenty Parquet files.  Their columns are reordered,
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
"""

from __future__ import annotations

import argparse
import hashlib
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
from typing import Callable, Iterator, Sequence, TypeVar
from unittest.mock import patch

import duckdb
import polars as pl

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
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    with _RSSSampler(sample_interval_seconds) as rss:
        value = fn()
    wall_seconds = time.perf_counter() - wall_started
    cpu_seconds = time.process_time() - cpu_started
    return _Measured(
        value=value,
        metrics={
            "wall_seconds": round(wall_seconds, 6),
            "cpu_seconds": round(cpu_seconds, 6),
            "cpu_equivalent_cores": round(
                cpu_seconds / wall_seconds if wall_seconds else 0.0, 4
            ),
            "rss_start_bytes": rss.start_bytes,
            "rss_end_bytes": rss.end_bytes,
            "rss_peak_bytes": rss.peak_bytes,
            "rss_peak_delta_bytes": max(0, rss.peak_bytes - rss.start_bytes),
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


def _frame_for_file(file_index: int, rows_per_file: int) -> pl.DataFrame:
    first_id = file_index * rows_per_file + 1
    ids = pl.int_range(
        first_id, first_id + rows_per_file, eager=True, dtype=pl.Int64
    )
    frame = pl.DataFrame({"id": ids}).with_columns(
        pl.concat_str(
            [pl.lit("tenant-"), ((pl.col("id") * 17) % 101).cast(pl.String)]
        ).alias("tenant"),
        (((pl.col("id") * 31) % 10_000_019).cast(pl.Float64) / 100.0).alias(
            "amount"
        ),
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
    if file_index != FILE_COUNT - 1:
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

    if file_index == FILE_COUNT - 1:
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


def _diversity_record(schemas: Sequence[Sequence[str]]) -> dict:
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
        "newest_file_index": FILE_COUNT - 1,
        "newest_public_columns": list(LATEST_PUBLIC_COLUMNS),
    }


def _build_corpus(root: Path, rows_per_file: int, compression_level: int) -> dict:
    source_dir = root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    resources = []
    tombstone_parts = []
    schemas: list[list[str]] = []
    newest_schema: list[dict[str, str]] | None = None

    for file_index in range(FILE_COUNT):
        frame = _frame_for_file(file_index, rows_per_file)
        schemas.append(list(frame.columns))
        if file_index == FILE_COUNT - 1:
            newest_schema = _schema_json(frame)
        path = source_dir / f"part-{file_index:02d}.parquet"
        frame.write_parquet(
            path,
            compression="zstd",
            compression_level=compression_level,
            statistics=True,
            row_group_size=PARQUET_ROW_GROUP_ROWS,
        )
        size = path.stat().st_size
        resources.append(
            {
                "file": str(path),
                "file_size": size,
                "rows": frame.height,
                "columns": frame.width,
            }
        )
        # Exactly one quarter of every file is dead.  No file is wholly dead,
        # so all twenty take the expensive rewrite path (rather than metadata-
        # only reclamation), faithfully stressing the reported bottleneck.
        dead = frame.filter((pl.int_range(0, frame.height) % 4) == 0).select(
            pl.lit(str(path), dtype=pl.String).alias(TOMBSTONE_FILE_COL),
            pl.col(ROWID_COL),
        )
        tombstone_parts.append(dead)

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
        "diversity": _diversity_record(schemas),
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
            connection.execute(f"SELECT COUNT(*) FROM ({query}) AS canonical").fetchone()[0]
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
        "schema": [
            {"name": str(row[0]), "type": str(row[1])} for row in described
        ],
    }


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
    compression_level: int = 1,
    workers: int = 4,
    target_bytes: int = DEFAULT_TARGET_BYTES,
    rss_sample_ms: float = 5.0,
    work_dir: str | None = None,
    label: str = "benchmark",
    fused: bool = False,
) -> dict:
    if rows_per_file < 4:
        raise ValueError("rows_per_file must be at least 4")
    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")
    workers = max(1, min(int(workers), 8, FILE_COUNT))
    sample_interval = max(float(rss_sample_ms), 1.0) / 1000.0

    with _benchmark_root(work_dir) as root:
        built = _measure(
            lambda: _build_corpus(root, rows_per_file, compression_level),
            sample_interval,
        )
        corpus = built.value
        resources = corpus["snapshot"]["resources"]
        tombstones: pl.DataFrame = corpus["tombstones"]
        input_bytes = sum(int(resource["file_size"]) for resource in resources)
        input_rows = FILE_COUNT * rows_per_file
        expected_live_rows = input_rows - tombstones.height
        if len(resources) != FILE_COUNT:
            raise AssertionError(f"expected {FILE_COUNT} source files")
        if tombstones.get_column(TOMBSTONE_FILE_COL).n_unique() != FILE_COUNT:
            raise AssertionError("every source file must be touched by tombstones")

        local_storage = LocalStorage()
        footer_md_cache: dict = {}
        table_config = {
            "max_memory_chunk_size": int(target_bytes),
            "max_overlapping_files": FILE_COUNT,
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
                    lambda: compact_resources(
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
            considered, compacted_rows, compacted_resources, resource_sunset, residual = (
                fused_measurement.value
            )
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
            if considered != len(resource_sunset) or len(resource_sunset) != FILE_COUNT:
                raise AssertionError(
                    "fused compaction must consume all twenty tombstoned sources: "
                    f"considered={considered}, sunset={len(resource_sunset)}"
                )
            if compacted_rows != expected_live_rows or final_live_rows != expected_live_rows:
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
                    lambda: compact_tombstones(
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
                len(tombstone_sunset) != FILE_COUNT
                or len(survivor_resources) != FILE_COUNT
            ):
                raise AssertionError(
                    "all twenty partially-dead files must be rewritten exactly once"
                )

            phase_b_profiler = Profiler()
            phase_profilers["small_file_merge"] = phase_b_profiler
            phase_b_dir = root / "phase-b-compacted"
            with patch(
                "supertable.processing._get_storage", return_value=local_storage
            ):
                phase_b = _measure(
                    lambda: compact_resources(
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
        expected_authoritative = _canonical_digest(
            paths=corpus["source_paths"],
            columns=LATEST_PUBLIC_COLUMNS,
            csv_path=root / "expected-authoritative.csv",
            tombstone_path=corpus["tombstone_path"],
        )
        expected_physical = _canonical_digest(
            paths=corpus["source_paths"],
            columns=corpus["physical_columns"],
            csv_path=root / "expected-physical.csv",
            tombstone_path=corpus["tombstone_path"],
        )
        actual_authoritative = _canonical_digest(
            paths=[resource["file"] for resource in final_live_resources],
            columns=LATEST_PUBLIC_COLUMNS,
            csv_path=root / "actual-authoritative.csv",
        )
        actual_physical = _canonical_digest(
            paths=[resource["file"] for resource in final_live_resources],
            columns=corpus["physical_columns"],
            csv_path=root / "actual-physical.csv",
        )
        authoritative_match = expected_authoritative == actual_authoritative
        physical_match = expected_physical == actual_physical
        if not authoritative_match or not physical_match:
            raise AssertionError(
                "compaction result digest mismatch: "
                f"authoritative={authoritative_match}, physical={physical_match}"
            )

        final_paths = [resource["file"] for resource in final_live_resources]
        metadata_profiler = Profiler()

        def _build_final_metadata():
            return extract_stats_rows(
                final_paths,
                profiler=metadata_profiler,
                footer_md_cache=footer_md_cache,
            )

        metadata = _measure(_build_final_metadata, sample_interval)
        stats_frame = metadata.value
        metadata_record = _phase_record(metadata, metadata_profiler)
        metadata_record.update({
            "stats_rows": stats_frame.height,
            # Compaction is a physical rewrite; logical metadata remains the
            # newest upload's authoritative schema. The independent physical
            # union digest above validates every retained legacy column.
            "schema": corpus["newest_schema"],
            "footer_cache_entries": len(footer_md_cache),
            "final_footer_cache_hits": int(
                metadata_profiler.counts.get("stats_footer_cache_hit", 0)
            ),
        })

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

        return {
            "benchmark": "tombstone_compaction_20_file_v1",
            "label": label,
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "polars": pl.__version__,
                "duckdb": duckdb.__version__,
                "cpu_affinity_count": affinity,
            },
            "configuration": {
                "file_count": FILE_COUNT,
                "rows_per_file": rows_per_file,
                "target_bytes": target_bytes,
                "target_mib": round(target_bytes / MIB, 3),
                "compression": f"zstd:{compression_level}",
                "tombstone_workers": workers,
                "tombstone_fraction": 0.25,
                "max_tombstone_rows": table_config["max_tombstone_rows"],
                "fused": bool(fused),
            },
            "corpus": {
                "input_files": FILE_COUNT,
                "input_rows": input_rows,
                "input_bytes": input_bytes,
                "input_mib": round(input_bytes / MIB, 3),
                "tombstone_rows": tombstones.height,
                "tombstone_bytes": Path(corpus["tombstone_path"]).stat().st_size,
                "expected_live_rows": expected_live_rows,
                "input_file_sizes": _size_distribution(resources, target_bytes),
                "generation": built.metrics,
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
                "read_amplification_vs_final_bytes": round(
                    total_bytes_read / final_live_bytes if final_live_bytes else 0.0,
                    4,
                ),
                "write_amplification_vs_final_bytes": round(
                    total_bytes_written / final_live_bytes if final_live_bytes else 0.0,
                    4,
                ),
                "peak_rss_bytes": max(
                    int(record["rss_peak_bytes"])
                    for record in phase_records.values()
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
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-per-file", type=int, default=100_000)
    parser.add_argument("--compression-level", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--target-mib", type=float, default=16.0)
    parser.add_argument("--rss-sample-ms", type=float, default=5.0)
    parser.add_argument("--work-dir")
    parser.add_argument("--label", default="benchmark")
    parser.add_argument("--output")
    parser.add_argument(
        "--fused",
        action="store_true",
        help="Fuse deletion-vector draining and final packing into one pass.",
    )
    args = parser.parse_args()
    result = run_benchmark(
        rows_per_file=args.rows_per_file,
        compression_level=args.compression_level,
        workers=args.workers,
        target_bytes=int(args.target_mib * MIB),
        rss_sample_ms=args.rss_sample_ms,
        work_dir=args.work_dir,
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
