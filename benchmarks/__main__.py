"""Command line entry point for the SuperTable performance suites.

    python -m benchmarks setup    --profile local --scale smoke
    python -m benchmarks read     --profile local --scale full
    python -m benchmarks write    --profile minio --duration 60
    python -m benchmarks all      --profile minio --scale full
    python -m benchmarks matrix   --scale full          # both profiles
    python -m benchmarks compare  <baseline.json> <candidate.json>
    python -m benchmarks teardown --profile minio [--include-dataset]

The storage backend is selected by ``STORAGE_TYPE``, which the settings module
reads exactly once when it is first imported.  That is why the profile is bound
here, before anything touches ``supertable``, and why ``matrix`` runs each
profile in its own process instead of switching backends in-flight.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

PROFILE_STORAGE = {"local": "LOCAL", "minio": "MINIO"}


_bound_profile: Optional[str] = None


def _bind_profile(profile: str) -> None:
    """Pin the storage backend before the settings singleton is built.

    Idempotent for the profile already bound: ``all`` runs the read suite and
    then the write suite in one process, and the second call must not trip the
    import guard on modules the first suite legitimately loaded.  Switching to
    a *different* profile mid-process is refused outright — the settings
    singleton is built once, so the second profile would silently run against
    the first one's backend.
    """
    global _bound_profile
    if profile not in PROFILE_STORAGE:
        raise SystemExit(f"unknown profile {profile!r}; expected one of "
                         f"{', '.join(sorted(PROFILE_STORAGE))}")
    if _bound_profile == profile:
        return
    if _bound_profile is not None:
        raise SystemExit(
            f"cannot switch profile {_bound_profile!r} -> {profile!r} in one "
            f"process; the storage backend is fixed at import. Use 'matrix', "
            f"which runs each profile in its own process."
        )
    for module in list(sys.modules):
        if module == "supertable" or module.startswith("supertable."):
            raise SystemExit(
                "supertable was imported before the profile was bound; the "
                "storage backend would be wrong. Run this module directly."
            )
    os.environ["STORAGE_TYPE"] = PROFILE_STORAGE[profile]

    from benchmarks._harness import configure_profile

    configure_profile(profile)
    _bound_profile = profile


def _scale(name: str):
    from benchmarks.dataset import SCALES

    if name not in SCALES:
        raise SystemExit(f"unknown scale {name!r}; expected one of "
                         f"{', '.join(SCALES)}")
    return SCALES[name]


def cmd_setup(args) -> int:
    _bind_profile(args.profile)
    from benchmarks import dataset as ds

    ds.build(args.profile, _scale(args.scale), rebuild=args.rebuild)
    return 0


def cmd_read(args) -> int:
    _bind_profile(args.profile)
    from benchmarks import dataset as ds, read_suite
    from benchmarks._harness import save_run

    scale = _scale(args.scale)
    record = ds.build(args.profile, scale, rebuild=args.rebuild)
    run = read_suite.run(
        args.profile, scale, iterations=args.iterations, warmup=args.warmup,
        dataset_record=record, fullscan=getattr(args, "fullscan", False),
    )
    path = save_run(run)
    print(f"\nsaved {path}")
    return _report_status(run)


def cmd_write(args) -> int:
    _bind_profile(args.profile)
    from benchmarks import write_suite
    from benchmarks._harness import save_run

    run = write_suite.run(args.profile, args.scale, duration_s=args.duration)
    path = save_run(run)
    print(f"\nsaved {path}")
    return _report_status(run)


def cmd_all(args) -> int:
    read_code = cmd_read(args)
    write_code = cmd_write(args)
    return max(read_code, write_code)


def cmd_matrix(args) -> int:
    """Run the same suites on every profile, each in its own process."""
    worst = 0
    for profile in args.profiles.split(","):
        profile = profile.strip()
        if not profile:
            continue
        command = [
            sys.executable, "-m", "benchmarks", args.suite,
            "--profile", profile, "--scale", args.scale,
            "--iterations", str(args.iterations), "--warmup", str(args.warmup),
            "--duration", str(args.duration),
        ]
        print(f"\n{'=' * 72}\nprofile: {profile}\n{'=' * 72}")
        worst = max(worst, subprocess.call(command, cwd=str(Path(__file__).parent.parent)))
    return worst


def cmd_teardown(args) -> int:
    _bind_profile(args.profile)
    from benchmarks import dataset as ds, write_suite
    from benchmarks._harness import RESULTS_ROOT

    for index in range(16):
        write_suite.drop_table(f"perf_write_par_{index}")
    for name in ("perf_write_shared", "perf_write_serial", "perf_write_lifecycle",
                 "perf_write_small", "perf_write_large", "perf_write_upsert"):
        write_suite.drop_table(name)
    print("dropped write-suite tables")

    if args.include_dataset:
        for scale in ds.SCALES.values():
            ds._drop_table(scale.table)
            marker = RESULTS_ROOT / f".dataset-{args.profile}-{scale.name}.json"
            marker.unlink(missing_ok=True)
        print("dropped read datasets")
    return 0


def cmd_compare(args) -> int:
    from benchmarks.compare import compare, exit_code, render
    from benchmarks._harness import load_run

    report = compare(
        load_run(args.baseline), load_run(args.candidate),
        threshold_pct=args.threshold_pct,
    )
    print(render(report))
    return exit_code(report)


def _report_status(run) -> int:
    bad = [s for s in run.scenarios if s.status != "ok"]
    if bad:
        print(f"\n{len(bad)} scenario(s) not ok:")
        for scenario in bad:
            print(f"  {scenario.id}: {scenario.status} — {scenario.error}")
        return 1
    print(f"all {len(run.scenarios)} scenarios ok "
          f"({run.duration_s:.1f}s total)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="benchmarks", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p, *, with_scale=True, with_fullscan=False):
        p.add_argument("--profile", default="local",
                       choices=sorted(PROFILE_STORAGE))
        if with_scale:
            p.add_argument("--scale", default="full", help="full | smoke")
        p.add_argument("--iterations", type=int, default=5,
                       help="measured iterations per read scenario")
        p.add_argument("--warmup", type=int, default=1,
                       help="discarded iterations before measuring")
        p.add_argument("--duration", type=float, default=60.0,
                       help="seconds per write throughput phase")
        if with_fullscan:
            p.add_argument("--fullscan", action="store_true",
                           help="disable predicate pruning; every file is read. "
                                "The seals MUST match a pruned run.")
        p.add_argument("--rebuild", action="store_true",
                       help="force a fresh read dataset")

    p_setup = sub.add_parser("setup", help="build (or reuse) the read dataset")
    add_common(p_setup)
    p_setup.set_defaults(func=cmd_setup)

    p_read = sub.add_parser("read", help="run the read suite")
    add_common(p_read, with_fullscan=True)
    p_read.set_defaults(func=cmd_read)

    p_write = sub.add_parser("write", help="run the write suite")
    add_common(p_write)
    p_write.set_defaults(func=cmd_write)

    p_all = sub.add_parser("all", help="run read then write")
    add_common(p_all, with_fullscan=True)
    p_all.set_defaults(func=cmd_all)

    p_matrix = sub.add_parser("matrix", help="run a suite on every profile")
    p_matrix.add_argument("--profiles", default="local,minio")
    p_matrix.add_argument("--suite", default="all",
                          choices=["read", "write", "all"])
    p_matrix.add_argument("--scale", default="full")
    p_matrix.add_argument("--iterations", type=int, default=5)
    p_matrix.add_argument("--warmup", type=int, default=1)
    p_matrix.add_argument("--duration", type=float, default=60.0)
    p_matrix.set_defaults(func=cmd_matrix)

    p_down = sub.add_parser("teardown", help="drop tables the suite created")
    p_down.add_argument("--profile", default="local", choices=sorted(PROFILE_STORAGE))
    p_down.add_argument("--include-dataset", action="store_true")
    p_down.set_defaults(func=cmd_teardown)

    p_cmp = sub.add_parser("compare", help="compare two result files")
    p_cmp.add_argument("baseline", type=Path)
    p_cmp.add_argument("candidate", type=Path)
    p_cmp.add_argument("--threshold-pct", type=float, default=10.0)
    p_cmp.set_defaults(func=cmd_compare)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
