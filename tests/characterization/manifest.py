"""SEALED_MANIFEST.json: content checksums for the frozen golden corpus.

The manifest records a sha256 for every sealed artifact (input parquet,
input/catalog.json, expected/result.json or expected/error.json, and the
per-scenario expected/manifest.json contract) keyed by its path **relative to
``tests/golden``** so the checksum content is free of environment-dependent
absolute paths.  It is validated before each test run; any drift (an edited
golden, a corrupted fixture, a stray/missing file) fails loudly instead of
silently passing a stale comparison.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List

from tests.characterization.paths import GOLDEN_ROOT, SEALED_MANIFEST

MANIFEST_VERSION = 1
# Files within each scenario dir that are sealed (checksummed).  Per-scenario a
# result scenario seals expected/result.json and an error scenario seals
# expected/error.json (the glob simply no-ops for the kind that is absent); both
# kinds also seal the human-readable expected/manifest.json contract.
_SEALED_GLOBS = (
    "input/*.parquet",
    "input/catalog.json",
    "expected/result.json",
    "expected/error.json",
    "expected/manifest.json",
)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _scenario_files(scenario_dir: Path) -> List[Path]:
    out: List[Path] = []
    for pat in _SEALED_GLOBS:
        out.extend(sorted(scenario_dir.glob(pat)))
    return out


def compute_manifest(scenario_ids: List[str]) -> dict:
    scenarios: Dict[str, dict] = {}
    for sid in sorted(scenario_ids):
        sdir = GOLDEN_ROOT / sid
        files: Dict[str, str] = {}
        for f in _scenario_files(sdir):
            rel = f.relative_to(GOLDEN_ROOT).as_posix()
            files[rel] = _sha256(f)
        scenarios[sid] = {"files": files}
    return {"version": MANIFEST_VERSION, "scenarios": scenarios}


def write_manifest(scenario_ids: List[str], *, merge: bool = True) -> dict:
    """Seal ``scenario_ids``, KEEPING every scenario already sealed.

    Without the merge, resealing a subset silently dropped every other
    scenario from the manifest and the next run failed the whole suite with
    "scenario X is not sealed in the manifest" — a resealing tool that breaks
    the seal for everything you did not name. Verified by
    test_manifest_subset_reseal_keeps_other_scenarios.

    ``merge=False`` reproduces the old whole-manifest rewrite, which is what a
    full reseal wants when a scenario has been deleted outright.
    """
    manifest = compute_manifest(scenario_ids)
    if merge and SEALED_MANIFEST.exists():
        try:
            previous = load_manifest()
        except Exception:
            previous = {}
        for key, value in previous.items():
            if not isinstance(value, dict) or not isinstance(manifest.get(key), dict):
                continue
            # Entries the caller did not reseal keep their existing checksums.
            merged = dict(value)
            merged.update(manifest[key])
            manifest[key] = merged
    SEALED_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    SEALED_MANIFEST.write_bytes(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    )
    return manifest


def load_manifest() -> dict:
    with open(SEALED_MANIFEST, "rb") as fh:
        return json.load(fh)


def validate_manifest(expected_scenario_ids: List[str] | None = None) -> List[str]:
    """Return a list of integrity problems (empty == intact)."""
    problems: List[str] = []
    if not SEALED_MANIFEST.exists():
        return [f"SEALED_MANIFEST.json missing at {SEALED_MANIFEST} — run "
                f"`python -m tests.generate_current_behavior_golden`"]
    manifest = load_manifest()
    sealed = manifest.get("scenarios", {})

    if expected_scenario_ids is not None:
        for sid in expected_scenario_ids:
            if sid not in sealed:
                problems.append(f"scenario {sid!r} is not sealed in the manifest")

    for sid, entry in sealed.items():
        for rel, want in entry["files"].items():
            f = GOLDEN_ROOT / rel
            if not f.exists():
                problems.append(f"sealed file missing on disk: {rel}")
                continue
            got = _sha256(f)
            if got != want:
                problems.append(
                    f"checksum mismatch for {rel}: disk={got[:12]}… sealed={want[:12]}…"
                )
    return problems
