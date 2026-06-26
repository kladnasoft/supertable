"""Error-behavior characterization — seals HOW the read path fails.

For every scenario carrying an ``expect_error`` expectation this:

  * reads the **sealed** input (parquet + catalog.json on disk) through the REAL
    production read path (``current_reader.read_scenario``), asserting it RAISES;
  * loads the sealed ``expected/error.json`` (the oracle) and asserts the live
    exception's class name equals the sealed ``category`` and the sealed
    ``message_substring`` occurs in the live message.

The sealed ``error.json`` — not the scenario's authored guess — is the contract:
the live run is the candidate compared against frozen bytes, so a future impl
that changes the failure category or drops the stable message fragment fails
loudly here.  Stack traces and absolute paths are deliberately NEVER sealed
(they are environment-dependent); only the stable category + message substring +
phase classification are.  ``--create-golden`` resealing is owned by the golden
generator (``test_golden`` / ``generate_current_behavior_golden``); this suite is
pure comparison and never reseals.
"""

from __future__ import annotations

import json

import pytest

from tests.characterization.current_reader import read_scenario
from tests.characterization.paths import GOLDEN_ROOT
from tests.characterization.scenarios import ALL_SCENARIOS

ERROR_JSON = "error.json"
_VALID_PHASES = {"catalog", "planning", "execution"}

# Only scenarios that authored an error expectation participate here.
_ERROR_SCENARIOS = [s for s in ALL_SCENARIOS if s.expect_error is not None]


def _load_sealed_error(scenario_id: str) -> dict:
    path = GOLDEN_ROOT / scenario_id / "expected" / ERROR_JSON
    if not path.exists():
        raise AssertionError(
            f"sealed {ERROR_JSON} not found for {scenario_id!r} at {path}\n"
            f"Seal it with:  python -m tests.generate_current_behavior_golden"
        )
    with open(path, "rb") as fh:
        return json.load(fh)


@pytest.mark.golden
@pytest.mark.parametrize("scenario", _ERROR_SCENARIOS, ids=lambda s: s.scenario_id)
def test_error_scenario(scenario, sealed_manifest_ok, create_golden):
    if create_golden:
        # Resealing for error scenarios is performed by the golden generator
        # (which validates the authored category/substring against the real
        # exception before writing error.json), so this suite stays read-only.
        pytest.skip(f"{scenario.scenario_id} resealed via the golden generator")

    sealed = _load_sealed_error(scenario.scenario_id)

    # ---- the sealed artifact is internally well-formed ----
    assert sealed["phase"] in _VALID_PHASES, (
        f"{scenario.scenario_id}: sealed phase {sealed['phase']!r} is not one of "
        f"{sorted(_VALID_PHASES)}"
    )
    # The scenario's authored expectation must stay consistent with the seal so
    # the scenario table and the frozen bytes can never silently diverge.
    exp = scenario.expect_error
    assert exp.phase == sealed["phase"], (
        f"{scenario.scenario_id}: authored phase {exp.phase!r} != sealed "
        f"phase {sealed['phase']!r}"
    )

    # ---- the live run is the candidate; it MUST raise ----
    with pytest.raises(BaseException) as excinfo:  # noqa: PT011 - characterizing any failure
        read_scenario(scenario, GOLDEN_ROOT, engine="duckdb")
    exc = excinfo.value

    actual_cat = type(exc).__name__
    actual_msg = str(exc)

    # ---- category must match the sealed contract exactly ----
    assert actual_cat == sealed["category"], (
        f"{scenario.scenario_id}: error-category drift — live={actual_cat!r} "
        f"sealed={sealed['category']!r} (live message={actual_msg!r})"
    )

    # ---- stable message fragment must still be present (when one is sealed) ----
    substring = sealed["message_substring"]
    if substring:
        assert substring in actual_msg, (
            f"{scenario.scenario_id}: sealed message substring {substring!r} not "
            f"found in live {actual_cat} message {actual_msg!r}"
        )
