#!/usr/bin/env python3
"""Fail when production mypy diagnostics differ from the reviewed baseline.

The package predates an all-clean mypy gate.  Silencing whole modules would
allow new defects in exactly the release-critical paths this gate is meant to
cover, so this checker runs mypy over the entire production package and records
the existing debt as canonical, human-reviewable diagnostics grouped by file.
Any added, removed, moved, or changed error fails until the baseline change is
reviewed.  Tests and benchmarks are excluded; shipped demos and console entry
points are included.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent
BASELINE_PATH = ROOT / "mypy-baseline.json"
TARGET = "supertable"
EXCLUDED_PARTS = frozenset({"tests", "benchmarks"})
EXPECTED_EXCLUDE = [r"^supertable/(?:[^/]+/)*(?:tests|benchmarks)/"]
EXPECTED_CONFIG = {
    "python_version": "3.10",
    "files": [TARGET],
    "exclude": EXPECTED_EXCLUDE,
    "follow_imports": "skip",
    "ignore_missing_imports": True,
    "no_site_packages": True,
    "check_untyped_defs": True,
    "incremental": False,
    "show_error_codes": True,
    "warn_unused_ignores": True,
}
ERROR_RE = re.compile(
    r"^(?P<path>supertable/[^:]+\.py):(?P<line>\d+)"
    r"(?::(?P<column>\d+))?: error: (?P<message>.+)$"
)
SUPPRESSION_RE = re.compile(r"#\s*(?:type:\s*ignore\b|mypy:\s*)")
REQUIRED_RELEASE_PATHS = frozenset(
    {
        "supertable/__init__.py",
        "supertable/audit/privileged_worker.py",
        "supertable/data_reader.py",
        "supertable/data_writer.py",
        "supertable/engine/data_estimator.py",
        "supertable/engine/islanddb.py",
        "supertable/meta_reader.py",
        "supertable/mirroring/mirror_delta.py",
        "supertable/mirroring/mirror_formats.py",
        "supertable/mirroring/mirror_iceberg.py",
        "supertable/monitoring/partitions.py",
        "supertable/monitoring_writer.py",
        "supertable/processing.py",
        "supertable/quality/history.py",
        "supertable/quality/scheduler.py",
        "supertable/rbac/access_control.py",
        "supertable/recovery/redis_rebuild.py",
        "supertable/redis_catalog.py",
        "supertable/redis_connector.py",
        "supertable/redis_infra.py",
        "supertable/simple_table.py",
        "supertable/staging_area.py",
        "supertable/storage/storage_interface.py",
        "supertable/super_table.py",
    }
)


def _production_sources() -> list[str]:
    sources: list[str] = []
    for path in (ROOT / TARGET).rglob("*.py"):
        relative = path.relative_to(ROOT)
        if EXCLUDED_PARTS.intersection(relative.parts):
            continue
        sources.append(relative.as_posix())
    return sorted(sources)


def _validate_mypy_scope(sources: Iterable[str]) -> None:
    toml_module = importlib.import_module(
        "tomllib" if sys.version_info >= (3, 11) else "tomli"
    )
    with (ROOT / "pyproject.toml").open("rb") as handle:
        config = toml_module.load(handle)["tool"]["mypy"]
    if config.get("files") != [TARGET]:
        raise SystemExit("mypy gate must target the complete 'supertable' package")
    if config.get("exclude") != EXPECTED_EXCLUDE:
        raise SystemExit("mypy exclusions changed; only tests/benchmarks may be excluded")
    if config.get("check_untyped_defs") is not True:
        raise SystemExit("mypy gate must check bodies of untyped functions")
    if config.get("no_site_packages") is not True:
        raise SystemExit("mypy baseline must not depend on installed package versions")
    if config.get("ignore_errors") is True or config.get("disable_error_code"):
        raise SystemExit("global mypy error suppression is forbidden")
    if config.get("overrides"):
        raise SystemExit("mypy module-level overrides are forbidden")
    if config != EXPECTED_CONFIG:
        raise SystemExit("mypy configuration changed outside the reviewed gate policy")

    present = set(sources)
    missing = sorted(REQUIRED_RELEASE_PATHS - present)
    if missing:
        raise SystemExit(f"release-critical mypy targets are missing: {missing}")


def _run_mypy(target: str) -> tuple[int, str]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            target,
            "--no-pretty",
            "--no-color-output",
            "--no-error-summary",
            "--no-incremental",
            "--show-error-codes",
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return completed.returncode, completed.stdout


def _canonical_errors(output: str) -> dict[str, list[str]]:
    errors: dict[str, list[str]] = defaultdict(list)
    for raw_line in output.splitlines():
        line = raw_line.replace("\\", "/").removeprefix("./")
        match = ERROR_RE.match(line)
        if match is not None:
            errors[match.group("path")].append(line)
        elif ": error:" in line:
            raise SystemExit(f"unrecognized mypy diagnostic (fail closed): {line}")
    return {path: sorted(lines) for path, lines in sorted(errors.items())}


def _inline_suppressions(sources: Iterable[str]) -> dict[str, list[str]]:
    suppressions: dict[str, list[str]] = {}
    for source in sources:
        lines = [
            f"{source}:{line_number}:{line.strip()}"
            for line_number, line in enumerate(
                (ROOT / source).read_text(encoding="utf-8").splitlines(), start=1
            )
            if SUPPRESSION_RE.search(line)
        ]
        if lines:
            suppressions[source] = lines
    return suppressions


def _inventory(
    errors: dict[str, list[str]],
    suppressions: dict[str, list[str]],
    sources: list[str],
) -> dict[str, Any]:
    return {
        "schema": 1,
        "target": TARGET,
        "mypy_version": importlib.metadata.version("mypy"),
        "production_file_count": len(sources),
        "known_error_count": sum(len(lines) for lines in errors.values()),
        "known_suppression_count": sum(
            len(lines) for lines in suppressions.values()
        ),
        # Store the diagnostics themselves, rather than opaque hashes, so a
        # baseline change is reviewable in an ordinary source diff.
        "files": errors,
        "inline_suppressions": suppressions,
    }


def _load_baseline() -> dict[str, Any]:
    try:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read {BASELINE_PATH.name}: {exc}") from exc


def _print_mismatch(
    expected: dict[str, Any],
    observed: dict[str, Any],
    errors: dict[str, list[str]],
) -> None:
    expected_files = expected.get("files", {})
    observed_files = observed.get("files", {})
    changed = sorted(
        path
        for path in set(expected_files) | set(observed_files)
        if expected_files.get(path) != observed_files.get(path)
    )
    print("mypy baseline mismatch", file=sys.stderr)
    print(
        f"expected {expected.get('known_error_count')} diagnostics across "
        f"{expected.get('production_file_count')} production files; observed "
        f"{observed.get('known_error_count')} across "
        f"{observed.get('production_file_count')}",
        file=sys.stderr,
    )
    for path in changed:
        print(
            f"  {path}: expected={len(expected_files.get(path, []))} "
            f"observed={len(observed_files.get(path, []))}",
            file=sys.stderr,
        )
        for diagnostic in errors.get(path, []):
            print(f"    {diagnostic}", file=sys.stderr)
    expected_suppressions = expected.get("inline_suppressions", {})
    observed_suppressions = observed.get("inline_suppressions", {})
    for path in sorted(set(expected_suppressions) | set(observed_suppressions)):
        if expected_suppressions.get(path) != observed_suppressions.get(path):
            print(
                f"  inline suppressions {path}: "
                f"expected={expected_suppressions.get(path)!r} "
                f"observed={observed_suppressions.get(path)!r}",
                file=sys.stderr,
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=[TARGET])
    parser.add_argument(
        "--print-baseline",
        action="store_true",
        help="print a newly measured baseline for review; never writes files",
    )
    args = parser.parse_args()

    sources = _production_sources()
    _validate_mypy_scope(sources)

    # The gate implementation is kept clean rather than absorbed into debt.
    checker_rc, checker_output = _run_mypy(Path(__file__).name)
    if checker_rc != 0:
        sys.stderr.write(checker_output)
        return 1

    mypy_rc, output = _run_mypy(args.target)
    if mypy_rc not in (0, 1):
        sys.stderr.write(output)
        return mypy_rc
    errors = _canonical_errors(output)
    suppressions = _inline_suppressions(sources)
    observed = _inventory(errors, suppressions, sources)

    if args.print_baseline:
        print(json.dumps(observed, indent=2, sort_keys=True))
        return 0

    expected = _load_baseline()
    if expected != observed:
        _print_mismatch(expected, observed, errors)
        return 1

    print(
        f"mypy baseline matched: {observed['production_file_count']} production "
        f"files checked; {observed['known_error_count']} reviewed diagnostics "
        f"across {len(observed['files'])} files; "
        f"{observed['known_suppression_count']} reviewed inline suppressions"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
