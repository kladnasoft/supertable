"""Guarded 4-CPU/2-GiB DuckDB versus IslandDB mutation benchmark."""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Sequence

from .container_runner import ContainerRunnerConfig, DockerWorkerRunner
from .corpus import GIB, plan_workload, sha256_file
from .mutation_corpus import (
    MUTATION_WORKLOAD_NAMES,
    MutationCorpusSpec,
    build_mutation_workloads,
    prepare_mutation_corpus,
)
from .runner import (
    ComparisonConfig,
    compare_manifest,
    environment_metadata,
    write_artifact,
)


CONTAINER_MEMORY_BYTES = 2 * GIB
# Leave headroom for the process-wide allocator while staying below the
# benchmark container's 2 GiB cgroup limit.  The IslandDB guard uses this as
# its cooperative runtime budget.
ENGINE_MEMORY_BYTES = (7 * GIB) // 4
CONTAINER_CPUS = 4


def benchmark_plan_summary(manifest) -> dict[str, Any]:
    workloads = build_mutation_workloads(manifest)
    plans = [plan_workload(manifest, workloads[name]) for name in MUTATION_WORKLOAD_NAMES]
    tombstone_bytes = int((manifest.get("tombstone") or {}).get("bytes") or 0)
    for plan in plans:
        # Whole selected Parquet objects plus the complete deletion vector is
        # a conservative unique eligible-input footprint. It includes hidden
        # row IDs, footers, and columns beyond SQL projection, but it is not a
        # promise that an engine will never reread a range or a physical-disk
        # counter (the OS page cache is measured separately).
        plan["unique_input_footprint_upper_bound_bytes"] = (
            int(plan["candidate_source_bytes"]) + tombstone_bytes
        )
    maximum_read = max(
        int(plan["unique_input_footprint_upper_bound_bytes"])
        for plan in plans
    )
    if maximum_read > GIB:
        raise RuntimeError(
            "mutation benchmark exceeds the 1-GiB unique-input ceiling"
        )
    return {
        "workloads": [
            {
                "name": plan["name"],
                "prune_percent": plan["prune_percent"],
                "selected_percent": plan["selected_percent"],
                "selection_basis": plan["selection_basis"],
                "selected_live_rows": plan["selected_live_rows"],
                "selected_live_percent": plan["selected_live_percent"],
                "pruned_live_percent": plan["pruned_live_percent"],
                "matched_update_rows": plan["matched_update_rows"],
                "matched_delete_rows": plan["matched_delete_rows"],
                "lower_id": plan["lower_id"],
                "upper_id": plan["upper_id"],
                "files_before_prune": plan["files_before_prune"],
                "files_after_prune": plan["files_after_prune"],
                "row_groups_after_file_prune": plan[
                    "row_groups_after_file_prune"
                ],
                "row_groups_pushdown_eligible": plan[
                    "row_groups_pushdown_eligible"
                ],
                "estimated_pushdown_bytes": plan[
                    "estimated_pushdown_bytes"
                ],
                "unique_input_footprint_upper_bound_bytes": plan[
                    "unique_input_footprint_upper_bound_bytes"
                ],
            }
            for plan in plans
        ],
        "maximum_unique_input_footprint_bytes": maximum_read,
        "maximum_projected_column_bytes": max(
            int(plan["estimated_pushdown_bytes"]) for plan in plans
        ),
        "input_footprint_ceiling_bytes": GIB,
        "execution_read_volume_bounded": False,
        "execution_read_scope": (
            "unique selected Parquet objects plus the complete sealed tombstone; "
            "engine rereads and physical page-cache traffic are observed telemetry"
        ),
    }


def run_mutation_benchmark(
    *,
    root: str | Path,
    image: str,
    output: str | Path,
    repeats: int = 2,
    cpuset_cpus: str = "0-3",
    spec: MutationCorpusSpec | None = None,
    prepare_only: bool = False,
) -> dict[str, Any]:
    if repeats <= 0:
        raise ValueError("repeats must be positive")
    if not str(image).strip() and not prepare_only:
        raise ValueError("a pinned Docker image is required")
    if not prepare_only and re.search(
        r"(?:^sha256:|@sha256:)[0-9a-fA-F]{64}$", str(image).strip(),
    ) is None:
        raise ValueError("Docker image must be pinned by a SHA-256 digest or ID")
    started = time.time()
    base = Path(root).expanduser().resolve()
    corpus_root = base / "corpora"
    artifact_root = base / "container-attempts"
    cache_root = base / "shared-cache"
    home_root = base / "worker-home"
    for path in (corpus_root, artifact_root, cache_root, home_root):
        path.mkdir(parents=True, exist_ok=True)

    effective_spec = spec or MutationCorpusSpec()
    uncompressed_source_upper = (
        effective_spec.physical_rows
        * effective_spec.wide_spec().approximate_row_bytes
    )
    if uncompressed_source_upper > GIB:
        raise RuntimeError(
            "mutation benchmark schema can exceed the 1-GiB read ceiling "
            "before corpus generation"
        )
    manifest = prepare_mutation_corpus(corpus_root, effective_spec)
    plan_summary = benchmark_plan_summary(manifest)
    comparison = None
    if not prepare_only:
        runner = DockerWorkerRunner(ContainerRunnerConfig(
            repo_root=Path(__file__).resolve().parents[3],
            corpus_root=corpus_root,
            artifact_root=artifact_root,
            image=image,
            cpuset_cpus=cpuset_cpus,
            container_memory_bytes=CONTAINER_MEMORY_BYTES,
            engine_memory_bytes=ENGINE_MEMORY_BYTES,
        ))
        comparison = compare_manifest(
            manifest,
            cache_root=cache_root,
            home_root=home_root,
            config=ComparisonConfig(
                warm_repeats=repeats,
                workloads=MUTATION_WORKLOAD_NAMES,
                cold_mode="process",
                timeout_seconds=3600,
                memory_limit_bytes=ENGINE_MEMORY_BYTES,
                source_repeat=1,
                threads=CONTAINER_CPUS,
                disable_caches=True,
            ),
            worker_runner=runner,
        )
    artifact = {
        "format_version": 1,
        "benchmark": "duckdb_islanddb_mutation_arrow_v1",
        "generated_unix_ms": int(time.time() * 1000),
        "elapsed_seconds": time.time() - started,
        "container": {
            "cpus": CONTAINER_CPUS,
            "cpuset_cpus": cpuset_cpus,
            "memory_bytes": CONTAINER_MEMORY_BYTES,
            "swap_bytes": 0,
            "engine_memory_bytes": ENGINE_MEMORY_BYTES,
            "caches_disabled": True,
            "image": image,
        },
        "dataset": {
            "corpus_id": effective_spec.corpus_id,
            "construction": manifest["construction"],
            "manifest_path": manifest["manifest_path"],
            "manifest_sha256": sha256_file(manifest["manifest_path"]),
            "actual_source_bytes": manifest["actual_source_bytes"],
            "uncompressed_source_upper_bound_bytes": uncompressed_source_upper,
            "physical_rows": manifest["physical_rows"],
            "live_rows": manifest["live_rows"],
            "tombstone_rows": manifest["tombstone_rows"],
            "tombstone_threshold": manifest["tombstone_threshold"],
            "snapshot_files": len(manifest["files"]),
            "snapshot_version": manifest["snapshot_version"],
            "operations": manifest["operations"],
        },
        "plan_summary": plan_summary,
        "comparison": comparison,
        "environment": environment_metadata(),
    }
    write_artifact(output, artifact)
    return artifact


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m supertable.engine.benchmarks.mutation_benchmark",
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--image", default="")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--cpuset-cpus", default="0-3")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--allow-large",
        action="store_true",
        help="required acknowledgement for the >10-million-row corpus",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.allow_large:
        parser.error("--allow-large is required")
    artifact = run_mutation_benchmark(
        root=args.root,
        image=args.image,
        output=args.output,
        repeats=args.repeats,
        cpuset_cpus=args.cpuset_cpus,
        prepare_only=args.prepare_only,
    )
    print(json.dumps({
        "output": str(args.output.resolve()),
        "physical_rows": artifact["dataset"]["physical_rows"],
        "live_rows": artifact["dataset"]["live_rows"],
        "tombstone_rows": artifact["dataset"]["tombstone_rows"],
        "snapshot_files": artifact["dataset"]["snapshot_files"],
        "maximum_unique_input_footprint_bytes": artifact["plan_summary"][
            "maximum_unique_input_footprint_bytes"
        ],
        "compared": artifact["comparison"] is not None,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTAINER_CPUS",
    "CONTAINER_MEMORY_BYTES",
    "ENGINE_MEMORY_BYTES",
    "benchmark_plan_summary",
    "build_parser",
    "main",
    "run_mutation_benchmark",
]
