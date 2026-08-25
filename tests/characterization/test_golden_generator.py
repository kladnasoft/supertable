"""Regression tests for deliberate characterization resealing."""

from __future__ import annotations

from tests import generate_current_behavior_golden as generator


def test_subset_reseal_keeps_the_complete_manifest(monkeypatch):
    selected_id = generator.ALL_SCENARIOS[0].scenario_id
    sealed: list[str] = []
    manifests: list[list[str]] = []

    monkeypatch.setattr(
        generator,
        "seal_scenario",
        lambda scenario: sealed.append(scenario.scenario_id) or "sealed",
    )
    monkeypatch.setattr(
        generator,
        "write_manifest",
        lambda scenario_ids: manifests.append(list(scenario_ids)),
    )

    assert generator.main([selected_id]) == 0
    assert sealed == [selected_id]
    assert manifests == [
        [scenario.scenario_id for scenario in generator.ALL_SCENARIOS]
    ]
