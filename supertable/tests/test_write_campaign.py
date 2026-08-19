from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from supertable.benchmarks import write_campaign as campaign


def _config(tmp_path: Path, revisions: tuple[campaign.RevisionSpec, ...]):
    script = tmp_path / "benchmark_tombstone_compaction.py"
    script.write_text("# external benchmark\n", encoding="utf-8")
    benchmark_root = tmp_path / "shared"
    benchmark_root.mkdir()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    return campaign.WriteCampaignConfig(
        revisions=revisions,
        benchmark_script=script,
        benchmark_root=benchmark_root,
        artifact_root=artifacts,
        image="benchmark@sha256:" + "a" * 64,
        repeats=2,
    )


def _result(*, correct: bool = True) -> dict:
    aggregate = {
        "row_count": 500_000,
        "numeric_columns": {
            "amount": {
                "non_null_count": 500_000,
                "null_count": 0,
                "min": "1",
                "max": "9",
                "avg_decimal_12": "5.000000000000",
            }
        },
    }
    return {
        "benchmark": "tombstone_compaction_v2",
        "environment": {
            "python": "3.12.1",
            "supertable": "2.4.1",
            "duckdb": "1.3.2",
            "polars": "1.32.0",
        },
        "configuration": {
            "file_count": 15,
            "input_corpus_dir": "/benchmark/corpus",
        },
        "corpus": {
            "mode": "shared_manifest",
            "tombstone_rows": 1_000_000,
            "manifest_sha256": {"sources": {"file": "abc"}, "tombstones": "def"},
            "input_size_calibration": {"all_within_target": True},
        },
        "phases": {
            "tombstone_rewrite": {
                "wall_seconds": 2.0,
                "cpu_seconds": 4.0,
                "proc_io_delta": {"read_bytes": 100, "write_bytes": 200},
            }
        },
        "summary": {
            "compaction_wall_seconds": 2.0,
            "compaction_cpu_seconds": 4.0,
            "peak_rss_bytes": 500,
        },
        "correctness": {
            "authoritative_projection": {
                "match": correct,
                "expected": {"sha256": "one", "rows": 500_000},
                "actual": {"sha256": "one", "rows": 500_000},
            },
            "physical_union": {
                "match": True,
                "expected": {"sha256": "two", "rows": 500_000},
                "actual": {"sha256": "two", "rows": 500_000},
            },
            "aggregates": {
                "match": True,
                "expected": aggregate,
                "actual": aggregate,
            },
        },
    }


def _inspect() -> dict:
    return {
        "HostConfig": {
            "Memory": 4 * 1024**3,
            "MemorySwap": 4 * 1024**3,
            "NanoCpus": 4_000_000_000,
            "PidsLimit": 1024,
            "CpusetCpus": "0-3",
            "ReadonlyRootfs": True,
            "NetworkMode": "none",
        },
        "State": {"OOMKilled": False},
    }


class _FakeSampler:
    def __init__(self, **kwargs) -> None:
        self.work = kwargs["spill_root"]

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def summary(self) -> dict:
        boundary = {
            "memory_max_bytes": 4 * 1024**3,
            "swap_max_bytes": 0,
        }
        return {
            "sample_count": 2,
            "effective_cpuset_verified": True,
            "observed_cpuset_cpus_effective": ["0-3"],
            "first_cgroup": boundary,
            "last_cgroup": boundary,
            "cgroup_memory_event_delta": {"oom": 0, "oom_kill": 0},
            "cgroup_cpu_stat_delta": {"usage_usec": 1_000_000},
            "cgroup_io_delta": {"rbytes": 100, "wbytes": 200},
            "cgroup_cpu_pressure_total_delta_usec": {"some": 10},
            "cgroup_io_pressure_total_delta_usec": {"some": 20},
            "process_rss_peak_bytes": 700,
        }


def _fake_popen_factory(result: dict):
    class FakePopen:
        def __init__(self, command, **_kwargs) -> None:
            self.command = command
            self.returncode = 0
            attempt = None
            for value in command:
                if isinstance(value, str) and ",dst=/attempt" in value:
                    source = value.split("src=", 1)[1].split(",dst=", 1)[0]
                    attempt = Path(source)
                    break
            assert attempt is not None
            (attempt / "work").mkdir()
            (attempt / "work" / "generated.parquet").write_bytes(b"output")
            (attempt / "result.json").write_text(
                json.dumps(result), encoding="utf-8"
            )

        def communicate(self, timeout=None):
            del timeout
            return json.dumps(result), ""

    return FakePopen


def test_command_has_fixed_boundary_and_stable_external_mounts(tmp_path: Path):
    candidate = tmp_path / "candidate"
    head = tmp_path / "head"
    candidate.mkdir()
    head.mkdir()
    revisions = (
        campaign.RevisionSpec("candidate", candidate),
        campaign.RevisionSpec("head", head, "fused"),
    )
    config = _config(tmp_path, revisions)
    attempt = tmp_path / "attempt"
    attempt.mkdir()

    command = campaign._docker_command(
        config=config,
        workload=campaign.TombstoneWorkload(),
        revision=revisions[1],
        container_name="write-test",
        attempt_root=attempt,
        prepare=False,
    )

    joined = " ".join(command)
    assert "--cpus 4" in joined
    assert "--cpuset-cpus 0-3" in joined
    assert f"--memory {4 * 1024**3}" in joined
    assert f"--memory-swap {4 * 1024**3}" in joined
    assert "--network none --read-only" in joined
    assert f"src={head.resolve()},dst=/workspace,readonly" in joined
    assert "dst=/benchmark-script/benchmark_tombstone_compaction.py,readonly" in joined
    assert f"src={(tmp_path / 'shared').resolve()},dst=/benchmark,readonly" in joined
    assert "--input-corpus /benchmark/corpus" in joined
    assert "--work-dir /attempt/work" in joined
    assert command[-1] == "--fused"


def test_schedule_reverses_every_other_repeat(tmp_path: Path):
    roots = []
    for name in ("old", "new-split", "new-fused"):
        root = tmp_path / name
        root.mkdir()
        roots.append(root)
    revisions = (
        campaign.RevisionSpec("old", roots[0]),
        campaign.RevisionSpec("new", roots[1]),
        campaign.RevisionSpec("new", roots[2], "fused"),
    )

    scheduled = campaign._alternating_schedule(revisions, 2)

    assert [item.revision.variant_id for item in scheduled] == [
        "old--two-phase",
        "new--two-phase",
        "new--fused",
        "new--fused",
        "new--two-phase",
        "old--two-phase",
    ]


def test_distribution_reports_requested_statistics():
    result = campaign._distribution([1, 2, 3, 4])

    assert result["count"] == 4
    assert result["min"] == 1
    assert result["mean"] == 2.5
    assert result["median"] == 2.5
    assert result["p95"] == pytest.approx(3.85)
    assert result["max"] == 4
    assert result["stddev"] == pytest.approx(math.sqrt(1.25))
    assert result["cv"] == pytest.approx(math.sqrt(1.25) / 2.5)


def test_successful_attempt_persists_telemetry_and_cleans_only_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    candidate = tmp_path / "candidate"
    head = tmp_path / "head"
    candidate.mkdir()
    head.mkdir()
    revisions = (
        campaign.RevisionSpec("candidate", candidate),
        campaign.RevisionSpec("head", head),
    )
    config = _config(tmp_path, revisions)
    attempt = tmp_path / "one-attempt"
    monkeypatch.setattr(campaign, "_ContainerSampler", _FakeSampler)
    monkeypatch.setattr(campaign.subprocess, "Popen", _fake_popen_factory(_result()))
    monkeypatch.setattr(campaign, "_container_inspect", lambda *_args: _inspect())
    monkeypatch.setattr(campaign, "_remove_container", lambda *_args: None)

    artifact = campaign._execute_attempt(
        config=config,
        workload=campaign.TombstoneWorkload(),
        revision=revisions[0],
        attempt_root=attempt,
        container_name="write-test",
        prepare=False,
        image={"content_digest": "sha256:" + "a" * 64},
        git={"head": "426e94b"},
    )

    assert artifact["status"] == "passed"
    assert artifact["work_cleanup"]["removed"] is True
    assert not (attempt / "work").exists()
    assert (attempt / "result.json").is_file()
    assert (attempt / "stdout.log").is_file()
    assert (attempt / "stderr.log").is_file()
    assert (attempt / "attempt.json").is_file()
    assert artifact["host_sampler"]["cgroup_cpu_stat_delta"]["usage_usec"] == 1_000_000
    assert artifact["provenance"]["dependency_versions"]["duckdb"] == "1.3.2"
    assert artifact["correctness_fingerprint"]["sha256"]


def test_failed_correctness_retains_generated_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    old = tmp_path / "old"
    head = tmp_path / "head"
    old.mkdir()
    head.mkdir()
    revisions = (
        campaign.RevisionSpec("old", old),
        campaign.RevisionSpec("head", head),
    )
    config = _config(tmp_path, revisions)
    attempt = tmp_path / "failed-attempt"
    monkeypatch.setattr(campaign, "_ContainerSampler", _FakeSampler)
    monkeypatch.setattr(
        campaign.subprocess, "Popen", _fake_popen_factory(_result(correct=False))
    )
    monkeypatch.setattr(campaign, "_container_inspect", lambda *_args: _inspect())
    monkeypatch.setattr(campaign, "_remove_container", lambda *_args: None)

    artifact = campaign._execute_attempt(
        config=config,
        workload=campaign.TombstoneWorkload(),
        revision=revisions[0],
        attempt_root=attempt,
        container_name="write-test-failed",
        prepare=False,
        image={},
        git={"head": "426e94b"},
    )

    assert artifact["status"] == "failed"
    assert artifact["work_cleanup"] is None
    assert (attempt / "work" / "generated.parquet").is_file()
    assert any("authoritative_projection" in error for error in artifact["validation_errors"])


def test_cli_defaults_to_426e_candidate_and_both_head_modes(tmp_path: Path):
    parser = campaign.build_parser()
    args = parser.parse_args(
        [
            "--candidate-426e-repo",
            str(tmp_path / "candidate"),
            "--container-image",
            "image",
            "--benchmark-root",
            str(tmp_path / "benchmark"),
            "--artifact-root",
            str(tmp_path / "artifacts"),
        ]
    )

    assert args.candidate_commit == "426e94b"
    assert args.candidate_mode == "two-phase"
    assert args.head_modes == ("two-phase", "fused")
    assert args.repeats == 5
