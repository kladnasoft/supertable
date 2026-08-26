"""Adversarial checks for the release mypy regression gate."""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path

import pytest

import check_mypy_baseline as gate


def _write_config(root: Path, extra: str = "") -> None:
    root.joinpath("pyproject.toml").write_text(
        "\n".join(
            [
                "[tool.mypy]",
                'files = ["supertable"]',
                f"exclude = [{gate.EXPECTED_EXCLUDE[0]!r}]",
                "check_untyped_defs = true",
                "no_site_packages = true",
                extra,
            ]
        ),
        encoding="utf-8",
    )


def _complete_scope() -> list[str]:
    return sorted(gate.REQUIRED_RELEASE_PATHS)


def test_repository_scope_policy_is_complete() -> None:
    sources = gate._production_sources()
    gate._validate_mypy_scope(sources)
    assert gate.REQUIRED_RELEASE_PATHS <= set(sources)


def test_scope_rejects_narrowed_target(monkeypatch, tmp_path: Path) -> None:
    _write_config(tmp_path)
    text = tmp_path.joinpath("pyproject.toml").read_text(encoding="utf-8")
    tmp_path.joinpath("pyproject.toml").write_text(
        text.replace(
            'files = ["supertable"]',
            'files = ["supertable/data_reader.py"]',
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="complete 'supertable' package"):
        gate._validate_mypy_scope(_complete_scope())


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ("ignore_errors = true", "global mypy error suppression"),
        (
            "\n".join(
                [
                    "[[tool.mypy.overrides]]",
                    'module = "supertable.data_reader"',
                    "ignore_errors = true",
                ]
            ),
            "module-level overrides",
        ),
    ],
)
def test_scope_rejects_error_suppression(
    monkeypatch,
    tmp_path: Path,
    extra: str,
    message: str,
) -> None:
    _write_config(tmp_path, extra)
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match=message):
        gate._validate_mypy_scope(_complete_scope())


def test_scope_rejects_added_module_exclusion(monkeypatch, tmp_path: Path) -> None:
    _write_config(tmp_path)
    text = tmp_path.joinpath("pyproject.toml").read_text(encoding="utf-8")
    tmp_path.joinpath("pyproject.toml").write_text(
        text.replace("(?:tests|benchmarks)", "(?:tests|benchmarks|quality)"),
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="only tests/benchmarks"):
        gate._validate_mypy_scope(_complete_scope())


def test_new_production_file_is_discovered_and_changes_inventory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    package = tmp_path / "supertable"
    package.mkdir()
    package.joinpath("new_release_path.py").write_text("value = 1\n", encoding="utf-8")
    tests = package / "tests"
    tests.mkdir()
    tests.joinpath("test_ignored.py").write_text("value = 2\n", encoding="utf-8")
    benchmarks = package / "benchmarks"
    benchmarks.mkdir()
    benchmarks.joinpath("ignored.py").write_text("value = 3\n", encoding="utf-8")
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    # Keep this source-discovery unit independent of the dev-only mypy wheel;
    # the executable release gate still records and enforces the real pinned
    # mypy version.
    monkeypatch.setattr(gate.importlib.metadata, "version", lambda _: "test")

    sources = gate._production_sources()
    assert sources == ["supertable/new_release_path.py"]
    before = gate._inventory({}, {}, [])
    after = gate._inventory({}, {}, sources)
    assert before != after
    assert after["production_file_count"] == 1


def test_inline_suppression_is_part_of_reviewed_inventory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "supertable" / "critical.py"
    source.parent.mkdir()
    source.write_text("value = call()  # type: ignore[name-defined]\n", encoding="utf-8")
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    suppressions = gate._inline_suppressions(["supertable/critical.py"])
    assert suppressions == {
        "supertable/critical.py": [
            "supertable/critical.py:1:value = call()  # type: ignore[name-defined]"
        ]
    }


def test_release_workflow_requires_exact_stable_semver_tag() -> None:
    workflow = (gate.ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert '[[ "${VERSION}" =~ ^[0-9]+\\.[0-9]+\\.[0-9]+$ ]]' in workflow
    assert 'test "${GITHUB_REF_NAME}" = "v${VERSION}"' in workflow


def test_release_workflow_pins_and_verifies_the_ssh_signing_key() -> None:
    expected_fingerprint = "SHA256:sc3IvJewQy3tS7edt21CrS6Cx9oZymArvXs4Fj/s6kk"
    signer = (
        gate.ROOT / ".github" / "release_allowed_signers"
    ).read_text(encoding="utf-8")
    principal, key_type, encoded_key = signer.split()
    fingerprint = base64.b64encode(
        hashlib.sha256(base64.b64decode(encoded_key, validate=True)).digest()
    ).decode("ascii").rstrip("=")
    workflow = (gate.ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert principal == "kladnasoft@outlook.com"
    assert key_type == "ssh-ed25519"
    assert f"SHA256:{fingerprint}" == expected_fingerprint
    assert expected_fingerprint in workflow
    assert 'gpg.ssh.allowedSignersFile="${ALLOWED_SIGNERS}"' in workflow
    assert 'verify-tag "${GITHUB_REF}"' in workflow
    assert "gh api" not in workflow


def test_secret_scan_checks_only_commits_introduced_by_the_event() -> None:
    workflow = (
        gate.ROOT / ".github" / "workflows" / "secret-scan.yml"
    ).read_text(encoding="utf-8")

    assert "fetch-depth: 0" in workflow
    assert 'scan_range="${PR_BASE_SHA}..${PR_HEAD_SHA}"' in workflow
    assert 'scan_range="${PUSH_BEFORE}..${EVENT_SHA}"' in workflow
    assert (
        'base_sha="$(git merge-base "${EVENT_SHA}" '
        'refs/remotes/origin/master)"'
    ) in workflow
    assert '--log-opts="${scan_range}"' in workflow
    assert "Scan complete Git history" not in workflow
    assert "allowlist" not in workflow.lower()
