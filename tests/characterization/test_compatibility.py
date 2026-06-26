"""Reader-compatibility contract — every TableReader must reproduce the goldens.

Parameterized over (reader x non-error scenario).  Each implemented reader must
turn the sealed input into a :class:`TableResult` that matches the sealed
``expected/result.json`` exactly (schema / row-count / multiset-or-ordered row
contents).  A reader that is not implemented yet (``available() is False``) is
skipped, so the future deletion-vector reader starts life as a visible pending
gate and becomes a hard conformance check the instant it is wired up — with no
edit to the sealed goldens.

This is the harness a future engineer runs to prove the post-migration read path
is byte-for-byte backward compatible with the pre-migration behavior.
"""

from __future__ import annotations

import pytest

from tests.characterization.comparison import assert_table_result_matches_golden
from tests.characterization.paths import GOLDEN_ROOT
from tests.characterization.readers import READERS
from tests.characterization.scenarios import ALL_SCENARIOS

# Error scenarios are covered by the error suite; the result goldens are the
# compatibility target here.
_RESULT_SCENARIOS = [s for s in ALL_SCENARIOS if s.expect_error is None]


@pytest.mark.golden
@pytest.mark.parametrize("reader", READERS, ids=lambda r: r.name)
@pytest.mark.parametrize("scenario", _RESULT_SCENARIOS, ids=lambda s: s.scenario_id)
def test_compatibility(reader, scenario, sealed_manifest_ok):
    if not reader.available():
        pytest.skip(f"reader {reader.name!r} is not implemented yet")
    actual = reader.read(scenario, GOLDEN_ROOT)
    assert_table_result_matches_golden(
        actual,
        GOLDEN_ROOT / scenario.scenario_id / "expected",
        ordered=scenario.ordered,
    )
