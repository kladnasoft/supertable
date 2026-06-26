"""Parameterized golden comparison — the core characterization contract.

For every sealed scenario this:

  * reads the **sealed** input (parquet + catalog.json on disk) through the REAL
    production read path (``current_reader.read_scenario``), and
  * compares the logical result against the **sealed** ``expected/result.json``
    via :func:`assert_table_result_matches_golden` (schema / row-count / multiset
    or ordered row contents).

Normal runs never regenerate goldens; they fail loudly with a structured diff on
any drift.  ``pytest --create-golden`` flips this module into reseal mode (the
same code path as ``python -m tests.generate_current_behavior_golden``) so the
two entry points can never diverge.

The SEALED_MANIFEST sha256 checksums are validated once per session *before* any
comparison, so a corrupted or edited fixture fails as an integrity error rather
than as a confusing logical diff.
"""

from __future__ import annotations

import pytest

from tests.characterization.comparison import assert_table_result_matches_golden
from tests.characterization.current_reader import read_scenario
from tests.characterization.paths import GOLDEN_ROOT
from tests.characterization.scenarios import ALL_SCENARIOS


@pytest.mark.golden
@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.scenario_id)
def test_golden_scenario(scenario, sealed_manifest_ok, create_golden):
    if create_golden:
        # Reseal path (shared with the module generator) — never asserts.
        from tests.generate_current_behavior_golden import seal_scenario

        seal_scenario(scenario)
        pytest.skip(f"resealed {scenario.scenario_id} (--create-golden)")

    if scenario.expect_error is not None:
        pytest.skip(f"{scenario.scenario_id} is an error scenario (see error suite)")

    actual = read_scenario(scenario, GOLDEN_ROOT, engine="duckdb")
    assert_table_result_matches_golden(
        actual,
        GOLDEN_ROOT / scenario.scenario_id / "expected",
        ordered=scenario.ordered,
    )
