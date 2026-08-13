"""Command-line interface for the DuckDB Lite versus IslandDB benchmark."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Sequence

from .corpus import (
    GIB,
    CorpusSpec,
    normalize_tiers,
    normalize_workloads,
    parse_byte_size,
    prepare_corpus,
)
from .runner import (
    BenchmarkParityError,
    BenchmarkUnavailableError,
    ComparisonConfig,
    build_artifact,
    compare_manifest,
    islanddb_available,
    write_artifact,
)


def _default_root() -> Path:
    configured = os.environ.get("SUPERTABLE_BENCHMARK_ROOT")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".cache" / "supertable" / "benchmarks" / "islanddb"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m supertable.engine.benchmarks",
        description=(
            "Generate deterministic Parquet and compare explicit DuckDB Lite "
            "with explicit IslandDB. Exact result parity is checked before timing."
        ),
    )
    parser.add_argument(
        "--sizes",
        action="append",
        default=[],
        metavar="LIST",
        help="comma-separated tiers: kb,mb,1gib,10gib (default: kb,mb)",
    )
    parser.add_argument("--kb", action="store_true", help="include the 512 KiB tier")
    parser.add_argument("--mb", action="store_true", help="include the 64 MiB tier")
    parser.add_argument(
        "--one-gib",
        "--1gib",
        "--1gb",
        dest="one_gib",
        action="store_true",
        help="include the 1 GiB tier (requires --allow-large)",
    )
    parser.add_argument(
        "--ten-gib",
        "--10gib",
        "--10gb",
        dest="ten_gib",
        action="store_true",
        help="include the 10 GiB tier (requires --allow-large)",
    )
    parser.add_argument(
        "--allow-large",
        action="store_true",
        help="explicitly allow corpus tiers of 1 GiB or larger",
    )
    parser.add_argument(
        "--workloads",
        action="append",
        default=[],
        metavar="LIST",
        help=(
            "comma-separated workloads: no_match,point,range_1pct,range_1pct_5cols,"
            "range_10pct,projection "
            "(default: all)"
        ),
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="warm timing samples after the one cold sample (default: 5)",
    )
    parser.add_argument("--seed", type=int, default=20260812)
    parser.add_argument("--payload-columns", type=int, default=8)
    parser.add_argument("--payload-width", type=int, default=64)
    parser.add_argument(
        "--shard-bytes",
        help="override target Parquet shard size, e.g. 32MiB",
    )
    parser.add_argument(
        "--row-group-bytes",
        default="8MiB",
        help="target uncompressed bytes per row group (default: 8MiB)",
    )
    parser.add_argument(
        "--cold-mode",
        choices=("process", "fadvise"),
        default="process",
        help=(
            "process: fresh engine/app-cache only; fadvise: additionally request "
            "best-effort OS page eviction"
        ),
    )
    parser.add_argument(
        "--corpus-root",
        type=Path,
        default=None,
        help="corpus directory (default: ~/.cache/supertable/benchmarks/islanddb/corpora)",
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=None,
        help="shared cache directory passed as SUPERTABLE_ISLAND_CACHE_DIR",
    )
    parser.add_argument(
        "--home-root",
        type=Path,
        default=None,
        help="private SUPERTABLE_HOME root for isolated workers",
    )
    parser.add_argument("--output", type=Path, help="result JSON path")
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="generate/validate corpora without running either engine",
    )
    parser.add_argument(
        "--no-disk-check",
        action="store_true",
        help="skip free-space preflight (not recommended for large tiers)",
    )
    parser.add_argument(
        "--worker-timeout",
        type=float,
        default=3600,
        help="seconds allowed for one isolated engine series (default: 3600)",
    )
    return parser


def selected_tiers(args: argparse.Namespace) -> list[str]:
    raw = list(args.sizes or [])
    if args.kb:
        raw.append("kb")
    if args.mb:
        raw.append("mb")
    if args.one_gib:
        raw.append("1gib")
    if args.ten_gib:
        raw.append("10gib")
    if not raw:
        raw = ["kb", "mb"]
    tiers = normalize_tiers(raw)
    if any(tier in ("1gib", "10gib") for tier in tiers) and not args.allow_large:
        raise ValueError(
            "1 GiB and 10 GiB tiers are opt-in; pass --allow-large after "
            "confirming disk capacity"
        )
    return tiers


def _selected_workloads(args: argparse.Namespace) -> list[str]:
    raw = args.workloads or [
        "no_match",
        "point",
        "range_1pct",
        "range_10pct",
        "projection",
    ]
    return normalize_workloads(raw)


def _print_prepared(manifest: dict) -> None:
    spec = manifest["spec"]
    state = "reused" if manifest.get("reused") else "generated"
    print(
        f"[{state}] {spec['tier']}: {manifest['actual_source_bytes']:,} bytes, "
        f"{manifest['total_rows']:,} rows, {len(manifest['files'])} files"
    )


def _print_comparison(comparison: dict) -> None:
    print(
        f"\n{comparison['tier']}: {comparison['actual_source_bytes']:,} source bytes"
    )
    for record in comparison["workloads"]:
        duck = record["summary"]["duckdb_lite"]["warm_wall_seconds_median"]
        island = record["summary"]["islanddb"]["warm_wall_seconds_median"]
        speedup = record["islanddb_speedup_over_duckdb_warm_median"]
        duck_text = f"{duck * 1000:.2f} ms" if duck is not None else "n/a"
        island_text = f"{island * 1000:.2f} ms" if island is not None else "n/a"
        speedup_text = f"{speedup:.3f}x" if speedup is not None else "n/a"
        print(
            f"  {record['workload']:<14} parity=OK  duckdb={duck_text:<12} "
            f"islanddb={island_text:<12} speedup={speedup_text}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        tiers = selected_tiers(args)
        workloads = _selected_workloads(args)
        if args.repeats <= 0:
            raise ValueError("--repeats must be positive")
        if args.worker_timeout <= 0:
            raise ValueError("--worker-timeout must be positive")
        row_group_bytes = parse_byte_size(args.row_group_bytes)
        shard_bytes = parse_byte_size(args.shard_bytes) if args.shard_bytes else None
    except ValueError as exc:
        parser.error(str(exc))

    root = _default_root().expanduser().resolve()
    corpus_root = (args.corpus_root or root / "corpora").expanduser().resolve()
    cache_root = (args.cache_root or root / "shared-cache").expanduser().resolve()
    home_root = (args.home_root or root / "worker-home").expanduser().resolve()

    manifests = []
    for tier in tiers:
        spec = CorpusSpec.for_tier(
            tier,
            seed=args.seed,
            payload_columns=args.payload_columns,
            payload_width=args.payload_width,
            row_group_target_bytes=row_group_bytes,
            shard_target_bytes=shard_bytes,
        )
        manifest = prepare_corpus(
            corpus_root,
            spec,
            check_disk=not args.no_disk_check,
        )
        manifests.append(manifest)
        _print_prepared(manifest)

    if args.prepare_only:
        print("prepare-only complete; no engine queries were executed")
        return 0
    if not islanddb_available():
        raise BenchmarkUnavailableError(
            "IslandDB is not implemented in this build; corpus preparation succeeded"
        )

    comparison_config = ComparisonConfig(
        warm_repeats=args.repeats,
        workloads=tuple(workloads),
        cold_mode=args.cold_mode,
        timeout_seconds=args.worker_timeout,
    )
    comparisons = []
    for manifest in manifests:
        comparison = compare_manifest(
            manifest,
            cache_root=cache_root,
            home_root=home_root,
            config=comparison_config,
        )
        comparisons.append(comparison)
        _print_comparison(comparison)

    output = args.output
    if output is None:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        output = root / "results" / f"islanddb_{stamp}.json"
    artifact = build_artifact(
        comparisons,
        config={
            "tiers": tiers,
            "workloads": workloads,
            "warm_repeats": args.repeats,
            "cold_mode": args.cold_mode,
            "seed": args.seed,
            "payload_columns": args.payload_columns,
            "payload_width": args.payload_width,
            "row_group_target_bytes": row_group_bytes,
            "shard_target_bytes": shard_bytes,
            "corpus_root": str(corpus_root),
            "cache_root": str(cache_root),
        },
    )
    destination = write_artifact(output, artifact)
    print(f"\nresult: {destination}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BenchmarkParityError, BenchmarkUnavailableError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
