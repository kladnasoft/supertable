# route: tests.characterization.test_manifest_reseal
"""Resealing a subset must not unseal everything else.

`generate_current_behavior_golden <ids>` rebuilt SEALED_MANIFEST.json from only
the named scenarios, so resealing two of sixty dropped the other fifty-eight and
the next run failed with "scenario X is not sealed in the manifest". A tool that
breaks the seal for everything you did not name is worse than no tool.
"""

from __future__ import annotations

import json

from tests.characterization import manifest as M


def test_subset_reseal_keeps_other_scenarios(tmp_path, monkeypatch):
    sealed = tmp_path / "SEALED_MANIFEST.json"
    monkeypatch.setattr(M, "SEALED_MANIFEST", sealed)

    # Stand in for a full manifest: two scenarios already sealed.
    sealed.write_text(json.dumps({
        "files": {"alpha/expected/result.json": "aaa",
                  "beta/expected/result.json": "bbb"},
        "scenarios": {"alpha": "a1", "beta": "b1"},
    }))

    monkeypatch.setattr(M, "compute_manifest", lambda ids: {
        "files": {"alpha/expected/result.json": "NEW"},
        "scenarios": {"alpha": "a2"},
    })

    out = M.write_manifest(["alpha"])

    assert out["scenarios"]["alpha"] == "a2", "the resealed scenario must update"
    assert out["scenarios"]["beta"] == "b1", "an untouched scenario must survive"
    assert out["files"]["beta/expected/result.json"] == "bbb"
    assert json.loads(sealed.read_text()) == out, "the merge must be persisted"


def test_full_reseal_can_still_drop_deleted_scenarios(tmp_path, monkeypatch):
    """merge=False is the escape hatch for a scenario that no longer exists."""
    sealed = tmp_path / "SEALED_MANIFEST.json"
    monkeypatch.setattr(M, "SEALED_MANIFEST", sealed)
    sealed.write_text(json.dumps({"scenarios": {"gone": "x", "kept": "y"}}))
    monkeypatch.setattr(M, "compute_manifest", lambda ids: {"scenarios": {"kept": "y2"}})

    out = M.write_manifest(["kept"], merge=False)
    assert "gone" not in out["scenarios"]
    assert out["scenarios"]["kept"] == "y2"
