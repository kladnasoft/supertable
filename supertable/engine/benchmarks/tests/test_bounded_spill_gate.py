from __future__ import annotations

import json
import os
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import pytest

from supertable.engine.benchmarks import bounded_spill_gate as gate
from supertable.engine.benchmarks.bounded_spill_gate import (
    BoundedSpillConfigurationError,
    BoundedSpillInputs,
    _boundary_errors,
    _container_absence_fence,
    _docker_command,
    disk_preflight,
    load_bounded_spill_inputs,
    run_comparison,
)
from supertable.engine.benchmarks.real_spill_gate import (
    EXPECTED_SOURCE_TYPES,
    PUBLIC_COLUMNS,
)
from supertable.engine.benchmarks.real_spill_worker import (
    RealSpillWorkerError,
    _duckdb_size,
    run_worker,
)


def _request(*, rows: int, source_bytes: int) -> dict:
    return {
        "engine": "duckdb",
        "warm_repeats": 0,
        "cold_mode": "fadvise",
        "memory_limit_bytes": 2 * 1024**3,
        "threads": 2,
        "disable_caches": True,
        "plan": {
            "name": "spill_group",
            "table": "events",
            "super_name": "island_benchmark",
            "files": ["/corpus/part-00000.parquet"],
            "required_columns": list(PUBLIC_COLUMNS),
            "schema": dict(EXPECTED_SOURCE_TYPES),
            "source_bytes": source_bytes,
            "source_repeat": 1,
            "candidate_rows": rows,
            "estimated_decoded_bytes": rows * 1_692,
            "decoded_estimate_complete": True,
            "projected_source_fraction": 0.99,
        },
    }


def _fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, int, int]:
    # Exercise the physical size seal without allocating a real GiB in a unit
    # test.  Production constants are restored automatically by monkeypatch.
    monkeypatch.setattr(gate, "GIB", 1024)
    source_bytes = 1025
    rows = 7
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    source = corpus / "part-00000.parquet"
    source.write_bytes(b"x" * source_bytes)
    manifest = {
        "generator": "islanddb-wide-v1",
        "total_rows": rows,
        "actual_source_bytes": source_bytes,
        "spec": {
            "tier": "1gib",
            "payload_columns": 26,
            "payload_width": 64,
        },
        "files": [
            {
                "path": source.name,
                "bytes": source_bytes,
                "rows": rows,
                "min_id": 0,
                "max_id": rows - 1,
            }
        ],
    }
    (corpus / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    template = tmp_path / "request.json"
    template.write_text(
        json.dumps(_request(rows=rows, source_bytes=source_bytes)),
        encoding="utf-8",
    )
    return template, corpus, rows, source_bytes


def test_load_bounded_inputs_seals_hashes_and_forces_low_memory_cold_sort(
    tmp_path, monkeypatch
):
    template, corpus, rows, source_bytes = _fixture(tmp_path, monkeypatch)

    inputs = load_bounded_spill_inputs(
        request_template=template,
        corpus_root=corpus,
    )

    request = inputs.request
    plan = request["plan"]
    assert request["memory_limit_bytes"] == gate.ENGINE_MEMORY_BYTES
    assert request["warm_repeats"] == 0
    assert request["cold_mode"] == "fadvise"
    assert plan["files"] == ["/corpus/part-00000.parquet"]
    assert plan["name"] == "real_spill_sort"
    assert plan["sql"].endswith("ORDER BY metric, id")
    assert plan["real_spill_contract"]["spill_cap_bytes"] == gate.SPILL_CAP_BYTES
    assert inputs.expected_rows == rows
    assert inputs.expected_value_bytes == rows * 1_692
    assert inputs.corpus_files[0]["bytes"] == source_bytes
    assert len(inputs.corpus_files[0]["sha256"]) == 64
    assert len(inputs.corpus_content_sha256) == 64
    assert len(inputs.manifest_sha256) == 64


def test_load_bounded_inputs_rejects_wrong_tier_and_file_identity(
    tmp_path, monkeypatch
):
    template, corpus, _, _ = _fixture(tmp_path, monkeypatch)
    manifest_path = corpus / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["spec"]["tier"] = "10gib"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(BoundedSpillConfigurationError, match="1gib"):
        load_bounded_spill_inputs(request_template=template, corpus_root=corpus)

    manifest["spec"]["tier"] = "1gib"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    request = json.loads(template.read_text())
    request["plan"]["files"] = ["/corpus/wrong.parquet"]
    template.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(BoundedSpillConfigurationError, match="identity"):
        load_bounded_spill_inputs(request_template=template, corpus_root=corpus)


def test_load_bounded_inputs_rejects_unsealed_temperature_or_repeats(
    tmp_path, monkeypatch
):
    template, corpus, _, _ = _fixture(tmp_path, monkeypatch)
    request = json.loads(template.read_text())
    request["cold_mode"] = "process"
    template.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(BoundedSpillConfigurationError, match="cold fadvise"):
        load_bounded_spill_inputs(request_template=template, corpus_root=corpus)

    request["cold_mode"] = "fadvise"
    request["warm_repeats"] = 1
    template.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(BoundedSpillConfigurationError, match="one cold"):
        load_bounded_spill_inputs(request_template=template, corpus_root=corpus)


def test_docker_command_has_both_hard_resource_and_temp_caps(tmp_path):
    command = _docker_command(
        docker="docker",
        image="example/image@sha256:" + "a" * 64,
        container_name="bounded-spill-duckdb-test",
        repo_root=tmp_path / "repo",
        corpus_root=tmp_path / "corpus",
        attempt_root=tmp_path / "attempt",
        timeout_seconds=300,
    )

    assert command[command.index("--cpus") + 1] == "4"
    assert command[command.index("--cpuset-cpus") + 1] == "0-3"
    assert command[command.index("--memory") + 1] == str(4 * 1024**3)
    assert command[command.index("--memory-swap") + 1] == str(4 * 1024**3)
    assert command[command.index("--pids-limit") + 1] == "1024"
    assert command[command.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert command[command.index("--cap-drop") + 1] == "ALL"
    assert "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES=536870912" in command
    assert "SUPERTABLE_ISLAND_SPILL_MAX_BYTES=4294967296" in command
    assert "SUPERTABLE_DUCKDB_MEMORY_LIMIT=512MiB" in command
    assert "SUPERTABLE_DUCKDB_THREADS=2" in command
    assert command[-4:] == [
        "-m",
        "supertable.engine.benchmarks.real_spill_worker",
        "/bench/request.json",
        "/bench/response.json",
    ]


def test_disk_preflight_requires_twice_the_per_engine_cap(tmp_path, monkeypatch):
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(
        gate.shutil,
        "disk_usage",
        lambda path: usage(20 * 1024**3, 12 * 1024**3, 8 * 1024**3),
    )
    assert disk_preflight(tmp_path)["free_bytes"] == 8 * 1024**3

    monkeypatch.setattr(
        gate.shutil,
        "disk_usage",
        lambda path: usage(20 * 1024**3, 13 * 1024**3, 7 * 1024**3),
    )
    with pytest.raises(BoundedSpillConfigurationError, match="at least"):
        disk_preflight(tmp_path)


def _valid_boundary() -> tuple[dict, dict, dict]:
    inspect = {
        "HostConfig": {
            "Memory": 4 * 1024**3,
            "MemorySwap": 4 * 1024**3,
            "NanoCpus": 4_000_000_000,
            "PidsLimit": 1024,
            "CpusetCpus": "0-3",
            "ReadonlyRootfs": True,
            "NetworkMode": "none",
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges:true"],
        },
        "State": {"OOMKilled": False},
    }
    cgroup = {
        "memory_max_bytes": 4 * 1024**3,
        "swap_max_bytes": 0,
        "cpu_max": "400000 100000",
        "cpuset_cpus_effective": "0-3",
    }
    sampler = {
        "effective_cpuset_verified": True,
        "first_cgroup": cgroup,
        "last_cgroup": cgroup,
        "cgroup_memory_event_delta": {"oom": 0, "oom_kill": 0, "oom_group_kill": 0},
    }
    series = {
        "execution_context": {
            "configured_threads": 2,
            "configured_memory_limit_bytes": 512 * 1024**2,
            "configured_spill_cap_bytes": 4 * 1024**3,
            "cpu_affinity": [0, 1, 2, 3],
            "cgroup_v2": {"memory_max_bytes": 4 * 1024**3, "swap_max_bytes": 0},
            "cgroup_memory_event_delta": {"oom": 0, "oom_kill": 0, "oom_group_kill": 0},
        }
    }
    return inspect, sampler, series


def test_boundary_validation_requires_inspect_host_cgroup_and_worker_proof():
    inspect, sampler, series = _valid_boundary()
    assert not _boundary_errors(inspect=inspect, sampler=sampler, series=series)

    series["execution_context"]["configured_memory_limit_bytes"] = 2 * 1024**3
    sampler["cgroup_memory_event_delta"]["oom_kill"] = 1
    errors = _boundary_errors(inspect=inspect, sampler=sampler, series=series)
    assert any("512-MiB" in error for error in errors)
    assert any("OOM" in error for error in errors)


def test_comparison_runs_sequentially_and_requires_exact_proof(tmp_path, monkeypatch):
    proof = {"digest": "a" * 64, "row_count": 7}
    inputs = BoundedSpillInputs(
        request={},
        plan_digest="b" * 64,
        expected_rows=7,
        expected_value_bytes=7 * 1_692,
        minimum_spill_bytes={"duckdb": 64 * 1024**2, "islanddb": 64 * 1024**2},
        manifest_sha256="c" * 64,
        corpus_content_sha256="d" * 64,
        corpus_files=({"bytes": 1025},),
    )
    order: list[str] = []

    monkeypatch.setattr(
        gate,
        "disk_preflight",
        lambda path: {
            "free_bytes": 9 * 1024**3,
            "required_free_bytes": 8 * 1024**3,
            "per_engine_spill_cap_bytes": 4 * 1024**3,
        },
    )

    def fake_attempt(*, engine, **kwargs):
        order.append(engine)
        return 0, {
            "status": "passed",
            "cleanup_verified": True,
            "result_proof": dict(proof),
            "result_metrics": {"wall_seconds": 2 if engine == "duckdb" else 3},
        }

    monkeypatch.setattr(gate, "run_engine_attempt", fake_attempt)
    code, result = run_comparison(
        inputs=inputs,
        output_root=tmp_path / "output",
        repo_root=tmp_path,
        corpus_root=tmp_path,
        docker="docker",
        image="example@sha256:" + "e" * 64,
        timeout_seconds=300,
        sample_interval_seconds=1,
    )

    assert code == 0
    assert order == ["duckdb", "islanddb"]
    assert result["parity"]["complete_proof_equal"] is True
    assert result["engines"]["duckdb"]["cleanup_verified"] is True
    assert result["engines"]["islanddb"]["cleanup_verified"] is True


def test_comparison_fences_second_engine_when_cleanup_is_unproven(
    tmp_path, monkeypatch
):
    inputs = BoundedSpillInputs(
        request={},
        plan_digest="b" * 64,
        expected_rows=7,
        expected_value_bytes=7 * 1_692,
        minimum_spill_bytes={"duckdb": 64 * 1024**2, "islanddb": 64 * 1024**2},
        manifest_sha256="c" * 64,
        corpus_content_sha256="d" * 64,
        corpus_files=({"bytes": 1025},),
    )
    order: list[str] = []
    monkeypatch.setattr(
        gate,
        "disk_preflight",
        lambda path: {
            "free_bytes": 9 * 1024**3,
            "required_free_bytes": 8 * 1024**3,
            "per_engine_spill_cap_bytes": 4 * 1024**3,
        },
    )

    def fake_attempt(*, engine, **kwargs):
        order.append(engine)
        return gate.EXIT_CLEANUP_FAILURE, {
            "status": "cleanup_failed",
            "cleanup_verified": False,
            "result_proof": None,
            "result_metrics": None,
        }

    monkeypatch.setattr(gate, "run_engine_attempt", fake_attempt)
    code, result = run_comparison(
        inputs=inputs,
        output_root=tmp_path / "output",
        repo_root=tmp_path,
        corpus_root=tmp_path,
        docker="docker",
        image="example@sha256:" + "e" * 64,
        timeout_seconds=300,
        sample_interval_seconds=1,
    )

    assert code == gate.EXIT_CLEANUP_FAILURE
    assert order == ["duckdb"]
    assert result["aborted_before"] == "islanddb"
    assert result["engines"]["islanddb"]["status"] == "not_run"


def test_post_remove_fence_accepts_only_explicit_docker_absence(monkeypatch):
    responses = iter(
        [
            SimpleNamespace(returncode=0, stdout="[{\"State\":{}}]", stderr=""),
            SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="Error: No such object: bounded-spill-duckdb-test",
            ),
        ]
    )
    monkeypatch.setattr(gate.subprocess, "run", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(gate.time, "sleep", lambda value: None)

    result = _container_absence_fence(
        "docker", "bounded-spill-duckdb-test", attempts=3
    )

    assert result["verified_absent"] is True
    assert [item["returncode"] for item in result["observations"]] == [0, 1]


def test_post_remove_fence_rejects_ambiguous_inspect_failure(monkeypatch):
    monkeypatch.setattr(
        gate.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="daemon unavailable"
        ),
    )
    monkeypatch.setattr(gate.time, "sleep", lambda value: None)

    result = _container_absence_fence("docker", "container", attempts=2)

    assert result["verified_absent"] is False
    assert len(result["observations"]) == 2


def test_worker_size_format_and_sealed_memory_mismatch_rejection():
    assert _duckdb_size(256 * 1024**2) == "256MiB"
    assert _duckdb_size(4 * 1024**3) == "4GiB"
    request = {
        "engine": "duckdb",
        "warm_repeats": 0,
        "threads": 4,
        "memory_limit_bytes": 256 * 1024**2,
        "plan": {
            "name": "real_spill_sort",
            "real_spill_contract": {
                "engine_memory_bytes": 512 * 1024**2,
                "spill_cap_bytes": 4 * 1024**3,
            },
        },
    }
    with pytest.raises(RealSpillWorkerError, match="differs"):
        run_worker(request)
