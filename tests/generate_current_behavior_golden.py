"""DELIBERATE golden generator for the SuperTable characterization suite.

    python -m tests.generate_current_behavior_golden            # reseal everything
    python -m tests.generate_current_behavior_golden id1 id2    # reseal a subset

This is the ONLY supported way to (re)create sealed golden artifacts.  Running
it OVERWRITES the frozen expected outputs and the SEALED_MANIFEST checksums from
*whatever the current implementation does right now*.  Normal test runs never
call this; they compare against the sealed bytes and fail loudly on drift.

For every scenario it:
  1. materializes immutable inputs  -> golden/<id>/input/{*.parquet,catalog.json}
  2. runs the REAL read path        -> via tests.characterization.current_reader
  3. seals the logical result       -> golden/<id>/expected/result.json
     (or, for error scenarios, the sealed category/substring/phase -> error.json)
  4. recomputes SEALED_MANIFEST.json sha256 checksums for the sealed set.

The environment is pinned hermetic (local storage, fake Redis) *before* any
``supertable`` import, exactly like the pytest harness.
"""

from __future__ import annotations

import hashlib
import json
import sys
import traceback
from pathlib import Path
from typing import List, Optional

# --- pin hermetic env BEFORE importing supertable (mirrors conftest) ---------
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.characterization.harness import bootstrap_hermetic_env  # noqa: E402

bootstrap_hermetic_env()

from tests.characterization.fixtures_lib import Scenario, materialize_scenario  # noqa: E402
from tests.characterization.manifest import write_manifest  # noqa: E402
from tests.characterization.paths import GOLDEN_ROOT  # noqa: E402
from tests.characterization.scenarios import ALL_SCENARIOS  # noqa: E402
from tests.characterization.table_result import TableResult  # noqa: E402

_BANNER = r"""
================================================================================
  !!  DELIBERATE GOLDEN RESEAL  !!
  This OVERWRITES sealed expected outputs from the CURRENT implementation.
  Only run this when a behavior change is intentional and reviewed.
================================================================================
""".strip("\n")

ERROR_JSON = "error.json"
SCENARIO_MANIFEST_JSON = "manifest.json"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _target_primary_keys(scenario: Scenario) -> List[str]:
    name = scenario.target_table()
    for t in scenario.tables:
        if t.simple_name == name:
            return list(t.primary_keys)
    return []


def _write_scenario_manifest(
    scenario: Scenario, sdir: Path, result: Optional[TableResult]
) -> None:
    """Per-scenario human-readable contract metadata (Layer 3 of the brief).

    Records the business/comparison columns, expected shape, and content
    checksums of the sealed input + expected artifacts, plus the invariant
    ``contains_internal_rowid: false`` — the public projection must stay free of
    the future ``__rowid``.  All fields are deterministic and path-free so the
    manifest itself can be checksummed into SEALED_MANIFEST.
    """
    expected_dir = sdir / "expected"
    catalog_path = sdir / "input" / "catalog.json"
    is_error = result is None
    expected_name = ERROR_JSON if is_error else TableResult.RESULT_JSON
    expected_path = expected_dir / expected_name

    manifest = {
        "scenario": scenario.scenario_id,
        "category": scenario.category,
        "description": scenario.description,
        "business_key_columns": _target_primary_keys(scenario),
        "comparison_columns": list(result.columns) if result else [],
        "expected_kind": "error" if is_error else "result",
        "expected_row_count": None if is_error else result.row_count,
        "ordering": {
            "mode": "ordered" if scenario.ordered else "multiset",
            "stable_sort_columns": _target_primary_keys(scenario),
        },
        "input_checksum": "sha256:" + _sha256_file(catalog_path),
        "expected_checksum": "sha256:" + _sha256_file(expected_path),
        "created_from": "pre-rowid-current-reader",
        "contains_internal_rowid": False,
    }
    expected_dir.mkdir(parents=True, exist_ok=True)
    (expected_dir / SCENARIO_MANIFEST_JSON).write_bytes(
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    )


def _deepest_supertable_frame(exc: BaseException) -> str:
    """Filename of the deepest in-package frame (diagnostic only, not sealed)."""
    frames = traceback.extract_tb(exc.__traceback__)
    last = ""
    for fr in frames:
        f = fr.filename.replace("\\", "/")
        if "/supertable/" in f or "/tests/" in f:
            last = f
    return last


def _seal_error(scenario: Scenario, expected_dir: Path, exc: BaseException) -> None:
    """Validate the authored expectation against the REAL exception, then seal.

    The sealed contract is the *actual* exception category plus the author's
    stable message substring (verified to actually occur in the real message, so
    a future impl must reproduce a compatible error — not a circular echo of the
    author's guess) and the author's phase classification.
    """
    exp = scenario.expect_error
    actual_cat = type(exc).__name__
    actual_msg = str(exc)

    if exp is not None:
        if exp.category and exp.category != actual_cat:
            raise AssertionError(
                f"{scenario.scenario_id}: expected error category {exp.category!r} "
                f"but got {actual_cat!r}: {actual_msg!r}\n  (frame: "
                f"{_deepest_supertable_frame(exc)})"
            )
        if exp.message_substring and exp.message_substring not in actual_msg:
            raise AssertionError(
                f"{scenario.scenario_id}: expected substring {exp.message_substring!r} "
                f"not found in actual {actual_cat}: {actual_msg!r}"
            )

    expected_dir.mkdir(parents=True, exist_ok=True)
    sealed = {
        "category": actual_cat,
        "message_substring": exp.message_substring if exp else actual_msg[:120],
        "phase": exp.phase if exp else "execution",
    }
    (expected_dir / ERROR_JSON).write_bytes(
        json.dumps(sealed, ensure_ascii=False, indent=2).encode("utf-8")
    )


def seal_scenario(scenario: Scenario, engine: str = "duckdb") -> str:
    """Materialize + read + seal a single scenario.  Returns a status string."""
    from tests.characterization.current_reader import read_scenario

    sdir = GOLDEN_ROOT / scenario.scenario_id
    materialize_scenario(scenario, sdir)
    if scenario.corrupt_inputs is not None:
        scenario.corrupt_inputs(sdir / "input")
    expected_dir = sdir / "expected"

    # Remove any stale sealed outputs so a category flip (result<->error) is clean.
    for stale in (
        expected_dir / TableResult.RESULT_JSON,
        expected_dir / ERROR_JSON,
        expected_dir / SCENARIO_MANIFEST_JSON,
    ):
        if stale.exists():
            stale.unlink()

    try:
        result = read_scenario(scenario, GOLDEN_ROOT, engine=engine)
    except BaseException as exc:  # noqa: BLE001 - we are characterizing failures too
        if scenario.expect_error is None:
            raise
        _seal_error(scenario, expected_dir, exc)
        _write_scenario_manifest(scenario, sdir, result=None)
        return f"  sealed ERROR  {scenario.scenario_id}  ({type(exc).__name__})"

    if scenario.expect_error is not None:
        raise AssertionError(
            f"scenario {scenario.scenario_id!r} expected an error but the read "
            f"succeeded with {result.row_count} row(s)"
        )
    result.write_golden(expected_dir)
    _write_scenario_manifest(scenario, sdir, result=result)
    return f"  sealed result {scenario.scenario_id}  ({result.row_count} row(s))"


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    print(_BANNER)

    selected = ALL_SCENARIOS
    if argv:
        wanted = set(argv)
        selected = [s for s in ALL_SCENARIOS if s.scenario_id in wanted]
        missing = wanted - {s.scenario_id for s in selected}
        if missing:
            print(f"ERROR: unknown scenario id(s): {sorted(missing)}", file=sys.stderr)
            return 2

    print(f"Sealing {len(selected)} scenario(s) into {GOLDEN_ROOT}\n")
    failures: List[str] = []
    for s in selected:
        try:
            print(seal_scenario(s))
        except BaseException as exc:  # noqa: BLE001 - report all, don't abort batch
            failures.append(f"{s.scenario_id}: {type(exc).__name__}: {exc}")
            print(f"  FAILED        {s.scenario_id}  ({type(exc).__name__})")

    if failures:
        print(f"\n{len(failures)} scenario(s) FAILED to seal — manifest NOT written:")
        for f in failures:
            print(f"  - {f}")
        return 1

    ids = [s.scenario_id for s in selected]
    write_manifest(ids)
    print(f"\nWrote SEALED_MANIFEST.json for {len(ids)} scenario(s).")
    print("Review the golden diff before committing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
