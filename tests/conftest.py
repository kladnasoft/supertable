"""Top-level pytest harness for SuperTable characterization tests.

pytest imports this module before collecting any test module, so the hermetic
environment is pinned *before* the first ``supertable`` import anywhere in the
test process.  See ``tests/characterization/harness.py`` for why this matters.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root on sys.path first so both ``import tests.*`` and ``import supertable``
# resolve to this working tree (not an installed wheel).
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# CRITICAL: pin the hermetic env before anything imports supertable.
from tests.characterization.harness import (  # noqa: E402
    bootstrap_hermetic_env,
    install_fake_redis,
    reset_engine_singletons,
)

bootstrap_hermetic_env()

import pytest  # noqa: E402


def pytest_addoption(parser):
    parser.addoption(
        "--create-golden",
        action="store_true",
        default=False,
        help="DANGER: regenerate and RESEAL all golden outputs from the current "
        "implementation.  Normal runs must never pass this flag.",
    )
    parser.addoption(
        "--run-spark",
        action="store_true",
        default=False,
        help="Run cross-engine Spark characterization tests (requires a local "
        "Spark Thrift server; skipped by default).",
    )
    parser.addoption(
        "--run-perf",
        action="store_true",
        default=False,
        help="Run non-blocking performance benchmarks (skipped by default).",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "golden: characterization test sealed by a golden artifact")
    config.addinivalue_line("markers", "spark: cross-engine test requiring Spark")
    config.addinivalue_line("markers", "perf: non-blocking performance benchmark")


@pytest.fixture
def create_golden(request) -> bool:
    return bool(request.config.getoption("--create-golden"))


@pytest.fixture(scope="session")
def sealed_manifest_ok(request) -> None:
    """Validate sealed SEALED_MANIFEST checksums once per session.

    Shared by the result (``test_golden``) and error (``test_errors``) suites so
    a corrupted or hand-edited fixture fails as an integrity error before any
    logical/error comparison runs.  Skipped while resealing (``--create-golden``).
    """
    if request.config.getoption("--create-golden"):
        return
    # Imported lazily so conftest load (which precedes the first supertable
    # import) stays light and does not pull polars at collection start.
    from tests.characterization.manifest import validate_manifest
    from tests.characterization.scenarios import all_scenario_ids

    problems = validate_manifest(all_scenario_ids())
    if problems:
        report = "\n".join(f"  - {p}" for p in problems)
        raise AssertionError(
            f"SEALED_MANIFEST integrity check failed ({len(problems)} problem(s)):\n"
            f"{report}\n\nReseal deliberately with "
            f"`python -m tests.generate_current_behavior_golden` if this is intended."
        )


@pytest.fixture(autouse=True)
def hermetic_fakeredis():
    """Give every test a fresh, process-local fake Redis and clean engine state."""
    fake = install_fake_redis()
    try:
        yield fake
    finally:
        try:
            fake.flushall()
        except Exception:
            pass
        reset_engine_singletons()
