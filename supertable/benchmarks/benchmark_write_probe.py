#!/usr/bin/env python3
"""Focused local overwrite-probe microbenchmark.

This compares the two equivalent match primitives over the same immutable
Parquet files: the write-specific Island-native scanner and the retained
DuckDB compatibility implementation.  ``integrity_cold`` clears the shared
schema/rowid-proof caches before the timed call; ``integrity_warm`` proves the
files in an untimed call first. Cases are interleaved in AB/BA order and every
result is checked for exact ``(file, rowid, composite key, version)`` equality.

``--publication-metadata`` measures both sides of the trade: production append
with and without the metadata-cache insertion, then the first probe with only
writer-seeded schema/routing metadata against a fully cold and a scan-warm
probe. Rowid integrity remains cold in the writer-seeded case by design.

Example:

    python -m supertable.benchmarks.benchmark_write_probe \
        --files 1,4,8,16 --rows-per-file 10000 --repeats 9
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Callable
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq

from supertable import processing
from supertable.storage.local_storage import LocalStorage
from supertable.utils.profiler import Profiler


Probe = Callable[..., pl.DataFrame | None]


def _summary(samples: list[float]) -> dict[str, float]:
    return {
        "median": statistics.median(samples),
        "min": min(samples),
        "max": max(samples),
    }


def _difference(new: float, old: float) -> float:
    return ((new - old) / old * 100.0) if old else 0.0


def _canonical(frame: pl.DataFrame | None) -> list[tuple]:
    if frame is None:
        raise RuntimeError("probe declined the supported benchmark schema")
    columns = ["__file__", "__rowid__", "tenant", "user_id", "version"]
    return frame.select(columns).sort(columns).rows()


def _build_case(
    root: Path, file_count: int, rows_per_file: int, incoming_rows: int,
) -> tuple[list[tuple[str, int]], pl.DataFrame]:
    candidates: list[tuple[str, int]] = []
    total_rows = file_count * rows_per_file
    for file_index in range(file_count):
        first = file_index * rows_per_file
        rowids = pl.int_range(first + 1, first + rows_per_file + 1, eager=True)
        user_ids = pl.int_range(first, first + rows_per_file, eager=True)
        frame = pl.DataFrame({
            "__rowid__": rowids,
            "tenant": pl.Series(
                "tenant", [f"t{value % 64:02d}" for value in user_ids]
            ),
            "user_id": user_ids,
            "version": user_ids + 100,
            # Deliberately unprojected payload: probe cost must not scale with it.
            "payload": pl.Series(
                "payload", [f"payload-{value:012d}" for value in user_ids]
            ),
        })
        path = root / f"candidate-{file_index:03d}.parquet"
        pq.write_table(frame.to_arrow(), path, compression="zstd")
        candidates.append((str(path), os.stat(path).st_size))

    step = max(1, total_rows // incoming_rows)
    selected = list(range(0, total_rows, step))[:incoming_rows]
    incoming = pl.DataFrame({
        "tenant": [f"t{value % 64:02d}" for value in selected],
        "user_id": selected,
        "version": [value + 101 for value in selected],
    })
    return candidates, incoming


def _build_published_case(
    root: Path,
    file_count: int,
    rows_per_file: int,
    incoming_rows: int,
    storage: LocalStorage,
    *,
    seed_metadata: bool,
) -> tuple[list[tuple[str, int]], pl.DataFrame, float, float, float]:
    """Build through the production writer, optionally disabling cache seed."""
    resources: list[dict] = []
    frames: list[pl.DataFrame] = []
    for file_index in range(file_count):
        first = file_index * rows_per_file
        user_ids = pl.int_range(first, first + rows_per_file, eager=True)
        frames.append(pl.DataFrame({
            "__rowid__": pl.int_range(
                first + 1, first + rows_per_file + 1, eager=True,
            ),
            "tenant": pl.Series(
                "tenant", [f"t{value % 64:02d}" for value in user_ids],
            ),
            "user_id": user_ids,
            "version": user_ids + 100,
            "payload": pl.Series(
                "payload", [f"payload-{value:012d}" for value in user_ids],
            ),
        }))

    metadata_seconds = 0.0
    seed_context = (
        patch(
            "supertable.processing._seed_local_write_probe_metadata",
            lambda **_kwargs: None,
        )
        if not seed_metadata else nullcontext()
    )
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    with (
        patch("supertable.processing._get_storage", return_value=storage),
        seed_context,
    ):
        for frame in frames:
            profiler = Profiler()
            processing.write_parquet_and_collect_resources(
                write_df=frame,
                overwrite_columns=[],
                data_dir=str(root),
                new_resources=resources,
                compression_level=1,
                profiler=profiler,
            )
            metadata_seconds += profiler.timings.get(
                "write.probe_metadata_seed", 0.0,
            )
    wall = time.perf_counter() - wall_started
    cpu = time.process_time() - cpu_started

    total_rows = file_count * rows_per_file
    step = max(1, total_rows // incoming_rows)
    selected = list(range(0, total_rows, step))[:incoming_rows]
    incoming = pl.DataFrame({
        "tenant": [f"t{value % 64:02d}" for value in selected],
        "user_id": selected,
        "version": [value + 101 for value in selected],
    })
    candidates = [
        (str(resource["file"]), int(resource["file_size"]))
        for resource in resources
    ]
    return candidates, incoming, metadata_seconds, wall, cpu


def _published_rows(candidates: list[tuple[str, int]]) -> pl.DataFrame:
    return pl.concat([
        pl.read_parquet(path) for path, _size in candidates
    ]).sort("__rowid__")


def _call(
    probe: Probe,
    candidates: list[tuple[str, int]],
    incoming: pl.DataFrame,
    storage: LocalStorage,
) -> pl.DataFrame | None:
    return probe(
        candidates,
        ["tenant", "user_id"],
        "version",
        incoming.select(["tenant", "user_id"]).unique(),
        incoming_schema=dict(incoming.schema),
        storage=storage,
    )


def _measure(
    probe: Probe,
    candidates: list[tuple[str, int]],
    incoming: pl.DataFrame,
    storage: LocalStorage,
    *,
    warm: bool,
) -> tuple[float, float, list[tuple]]:
    processing._LOCAL_ROWID_INTEGRITY_CACHE.clear()
    processing._LOCAL_PROBE_SCHEMA_CACHE.clear()
    if warm:
        _call(probe, candidates, incoming, storage)
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    result = _call(probe, candidates, incoming, storage)
    wall = time.perf_counter() - wall_started
    cpu = time.process_time() - cpu_started
    return wall, cpu, _canonical(result)


def _measure_ready(
    probe: Probe,
    candidates: list[tuple[str, int]],
    incoming: pl.DataFrame,
    storage: LocalStorage,
) -> tuple[float, float, list[tuple]]:
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    result = _call(probe, candidates, incoming, storage)
    wall = time.perf_counter() - wall_started
    cpu = time.process_time() - cpu_started
    return wall, cpu, _canonical(result)


def run(
    *, file_counts: list[int], rows_per_file: int, incoming_rows: int,
    repeats: int,
) -> dict:
    probes: dict[str, Probe] = {
        "island_native": processing._island_probe_overlap_matches,
        "duckdb": processing._duckdb_probe_overlap_matches,
    }
    report: dict = {
        "schema": "supertable-write-probe-microbench-v1",
        "rows_per_file": rows_per_file,
        "incoming_rows": incoming_rows,
        "repeats": repeats,
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix="supertable-write-probe-") as tmp:
        root = Path(tmp)
        storage = LocalStorage()
        for file_count in file_counts:
            case_root = root / f"files-{file_count}"
            case_root.mkdir()
            candidates, incoming = _build_case(
                case_root, file_count, rows_per_file, incoming_rows,
            )
            samples = {
                mode: {
                    engine: {"wall_seconds": [], "cpu_seconds": []}
                    for engine in probes
                }
                for mode in ("integrity_cold", "integrity_warm")
            }
            expected: list[tuple] | None = None
            for iteration in range(repeats):
                order = (
                    ("island_native", "duckdb")
                    if iteration % 2 == 0 else ("duckdb", "island_native")
                )
                for mode in samples:
                    for engine in order:
                        wall, cpu, rows = _measure(
                            probes[engine], candidates, incoming, storage,
                            warm=mode == "integrity_warm",
                        )
                        if expected is None:
                            expected = rows
                        elif rows != expected:
                            raise AssertionError(
                                f"{engine} result differs for {file_count} files"
                            )
                        samples[mode][engine]["wall_seconds"].append(wall)
                        samples[mode][engine]["cpu_seconds"].append(cpu)

            modes = {}
            for mode, engines in samples.items():
                summaries = {
                    engine: {
                        metric: _summary(values)
                        for metric, values in metrics.items()
                    }
                    for engine, metrics in engines.items()
                }
                native_wall = summaries["island_native"][
                    "wall_seconds"
                ]["median"]
                duck_wall = summaries["duckdb"]["wall_seconds"]["median"]
                native_cpu = summaries["island_native"][
                    "cpu_seconds"
                ]["median"]
                duck_cpu = summaries["duckdb"]["cpu_seconds"]["median"]
                modes[mode] = {
                    **summaries,
                    "island_native_vs_duckdb_percent": {
                        "wall_seconds": _difference(native_wall, duck_wall),
                        "cpu_seconds": _difference(native_cpu, duck_cpu),
                    },
                }
            report["cases"].append({
                "files": file_count,
                "rows": file_count * rows_per_file,
                "matched_rows": len(expected or ()),
                "results_equal": True,
                "modes": modes,
            })
    return report


def run_publication(
    *, file_counts: list[int], rows_per_file: int, incoming_rows: int,
    repeats: int,
) -> dict:
    """Measure metadata seed append cost and the first probe it accelerates."""
    report: dict = {
        "schema": "supertable-write-probe-publication-metadata-microbench-v2",
        "rows_per_file": rows_per_file,
        "incoming_rows": incoming_rows,
        "repeats": repeats,
        "cases": [],
    }
    probe = processing._island_probe_overlap_matches
    with tempfile.TemporaryDirectory(
        prefix="supertable-write-probe-publication-",
    ) as tmp:
        root = Path(tmp)
        storage = LocalStorage()
        for file_count in file_counts:
            probe_samples = {
                mode: {"wall_seconds": [], "cpu_seconds": []}
                for mode in (
                    "writer_metadata_seeded", "unseeded_cold", "scan_warm",
                )
            }
            append_samples = {
                mode: {"wall_seconds": [], "cpu_seconds": []}
                for mode in ("metadata_seeded", "seed_disabled")
            }
            metadata_seconds: list[float] = []
            matched_rows: int | None = None
            for iteration in range(repeats):
                processing._LOCAL_ROWID_INTEGRITY_CACHE.clear()
                processing._LOCAL_PROBE_SCHEMA_CACHE.clear()
                built = {}
                order = (
                    ("metadata_seeded", "seed_disabled")
                    if iteration % 2 == 0
                    else ("seed_disabled", "metadata_seeded")
                )
                for mode in order:
                    case_root = root / (
                        f"files-{file_count}-run-{iteration}-{mode}"
                    )
                    case_root.mkdir()
                    built[mode] = _build_published_case(
                        case_root,
                        file_count,
                        rows_per_file,
                        incoming_rows,
                        storage,
                        seed_metadata=mode == "metadata_seeded",
                    )
                    _candidates, _incoming, seed, wall, cpu = built[mode]
                    append_samples[mode]["wall_seconds"].append(wall)
                    append_samples[mode]["cpu_seconds"].append(cpu)
                    if mode == "metadata_seeded":
                        metadata_seconds.append(seed)

                candidates, incoming, _seed, _wall, _cpu = built[
                    "metadata_seeded"
                ]
                disabled_candidates = built["seed_disabled"][0]
                if not _published_rows(candidates).equals(
                    _published_rows(disabled_candidates)
                ):
                    raise AssertionError("metadata seed changed written rows")

                wall, cpu, rows = _measure_ready(
                    probe, candidates, incoming, storage,
                )
                probe_samples["writer_metadata_seeded"][
                    "wall_seconds"
                ].append(wall)
                probe_samples["writer_metadata_seeded"][
                    "cpu_seconds"
                ].append(cpu)
                iteration_expected = rows
                if matched_rows is None:
                    matched_rows = len(rows)
                elif len(rows) != matched_rows:
                    raise AssertionError("writer-seeded row count differs")

                processing._LOCAL_ROWID_INTEGRITY_CACHE.clear()
                processing._LOCAL_PROBE_SCHEMA_CACHE.clear()
                wall, cpu, rows = _measure_ready(
                    probe, candidates, incoming, storage,
                )
                probe_samples["unseeded_cold"]["wall_seconds"].append(wall)
                probe_samples["unseeded_cold"]["cpu_seconds"].append(cpu)
                if rows != iteration_expected:
                    raise AssertionError("unseeded result differs")

                wall, cpu, rows = _measure_ready(
                    probe, candidates, incoming, storage,
                )
                probe_samples["scan_warm"]["wall_seconds"].append(wall)
                probe_samples["scan_warm"]["cpu_seconds"].append(cpu)
                if rows != iteration_expected:
                    raise AssertionError("scan-warm result differs")

            probe_summaries = {
                mode: {
                    metric: _summary(values)
                    for metric, values in metrics.items()
                }
                for mode, metrics in probe_samples.items()
            }
            append_summaries = {
                mode: {
                    metric: _summary(values)
                    for metric, values in metrics.items()
                }
                for mode, metrics in append_samples.items()
            }
            seeded_wall = probe_summaries["writer_metadata_seeded"][
                "wall_seconds"
            ]["median"]
            cold_wall = probe_summaries["unseeded_cold"][
                "wall_seconds"
            ]["median"]
            seeded_cpu = probe_summaries["writer_metadata_seeded"][
                "cpu_seconds"
            ]["median"]
            cold_cpu = probe_summaries["unseeded_cold"][
                "cpu_seconds"
            ]["median"]
            append_wall = append_summaries["metadata_seeded"][
                "wall_seconds"
            ]["median"]
            disabled_wall = append_summaries["seed_disabled"][
                "wall_seconds"
            ]["median"]
            append_cpu = append_summaries["metadata_seeded"][
                "cpu_seconds"
            ]["median"]
            disabled_cpu = append_summaries["seed_disabled"][
                "cpu_seconds"
            ]["median"]
            report["cases"].append({
                "files": file_count,
                "rows": file_count * rows_per_file,
                "matched_rows": int(matched_rows or 0),
                "results_equal": True,
                "publication_metadata_seconds": _summary(metadata_seconds),
                "append_modes": append_summaries,
                "append_seeded_vs_disabled_percent": {
                    "wall_seconds": _difference(append_wall, disabled_wall),
                    "cpu_seconds": _difference(append_cpu, disabled_cpu),
                },
                "probe_modes": probe_summaries,
                "metadata_seeded_vs_unseeded_cold_percent": {
                    "wall_seconds": _difference(seeded_wall, cold_wall),
                    "cpu_seconds": _difference(seeded_cpu, cold_cpu),
                },
            })
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", default="1,4,8,16")
    parser.add_argument("--rows-per-file", type=int, default=10_000)
    parser.add_argument("--incoming-rows", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument(
        "--publication-metadata", "--publication-proof",
        dest="publication_metadata", action="store_true",
    )
    args = parser.parse_args()
    file_counts = [int(value) for value in args.files.split(",")]
    runner = run_publication if args.publication_metadata else run
    print(json.dumps(runner(
        file_counts=file_counts,
        rows_per_file=args.rows_per_file,
        incoming_rows=args.incoming_rows,
        repeats=args.repeats,
    ), indent=2))


if __name__ == "__main__":
    main()
