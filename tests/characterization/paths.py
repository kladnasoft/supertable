"""Shared filesystem locations for the characterization suite."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / "tests"
GOLDEN_ROOT = TESTS_ROOT / "golden"
SEALED_MANIFEST = GOLDEN_ROOT / "SEALED_MANIFEST.json"
PERF_ROOT = TESTS_ROOT / "perf_results"
