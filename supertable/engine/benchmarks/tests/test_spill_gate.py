from __future__ import annotations

import json
import importlib
import os
import subprocess
from pathlib import Path

import pytest

from supertable.engine.benchmarks.runner import result_digest
from supertable.engine.benchmarks.spill_gate import (
    CONTAINER_MEMORY_BYTES,
    ENGINE_MEMORY_BYTES,
    EXIT_TIMEOUT,
    GateInputs,
    SpillGateConfigurationError,
    _cgroup_io_totals,
    _docker_command,
    _result_metrics,
    _successful_result,
    load_gate_inputs,
    run_attempt,
)


def _canonical() -> dict:
    quotient, remainder = divmod(6_413_677, 1_024)
    return {
        "columns": ["dimension", "id_count"],
        "dtypes": ["int32", "int64"],
        "rows": [
            [dimension, quotient + (1 if dimension < remainder else 0)]
            for dimension in range(1_024)
        ],
    }


def _plan() -> dict:
    return {
        "name": "spill_group",
        "source_bytes": 10 * 1024**3 + 1,
        "projected_source_fraction": 0.99,
        "island_streaming_result": True,
        "files": ["/corpus/part-00000.parquet"],
        "sql": "SELECT dimension, COUNT(id) FROM events GROUP BY dimension",
    }


def _request(engine: str) -> dict:
    return {
        "purpose": "parity",
        "engine": engine,
        "plan": _plan(),
        "warm_repeats": 0,
        "cold_mode": "fadvise",
        "memory_limit_bytes": ENGINE_MEMORY_BYTES,
        "threads": 4,
        "disable_caches": True,
        "minimum_cold_read_fraction": None,
    }


def _series(engine: str, *, wall_seconds: float = 80.0) -> dict:
    canonical = _canonical()
    digest = result_digest(canonical)
    sample = {
        "temperature": "cold",
        "result_digest": digest,
        "wall_seconds": wall_seconds,
        "cpu_seconds": wall_seconds * 3,
        "rss_peak_bytes": 700,
        "process_io_delta": {"read_bytes": 1000, "write_bytes": 500},
        "engine_profile": {
            "spill_bytes": 500,
            "rows_scanned": 21,
            "result_rows": 2,
            "optimized_plan": "EXTERNAL SPILL",
        },
    }
    return {
        "engine": engine,
        "result": canonical,
        "result_digest": digest,
        "samples": [sample],
        "execution_context": {
            "configured_threads": 4,
            "polars_thread_pool_size": 4,
            "cgroup_memory_event_delta": {"oom": 0, "oom_kill": 0},
            "cgroup_v2": {
                "memory_max_bytes": CONTAINER_MEMORY_BYTES,
                "swap_max_bytes": 0,
            },
        },
    }


def _write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def test_load_gate_inputs_reuses_validated_duckdb_oracle(tmp_path, monkeypatch):
    spill_gate = importlib.import_module("supertable.engine.benchmarks.spill_gate")
    monkeypatch.setattr(
        spill_gate, "SEALED_ORACLE_DIGEST", result_digest(_canonical())
    )
    template = tmp_path / "island-request.json"
    oracle_request = tmp_path / "duck-request.json"
    oracle_response = tmp_path / "duck-response.json"
    _write_json(template, _request("islanddb"))
    _write_json(oracle_request, _request("duckdb"))
    _write_json(oracle_response, {"ok": True, "result": _series("duckdb")})

    inputs = load_gate_inputs(
        request_template=template,
        oracle_request=oracle_request,
        oracle_response=oracle_response,
    )

    assert inputs.request["engine"] == "islanddb"
    assert inputs.request["purpose"] == "spill-regression-gate"
    assert inputs.request["plan"]["integer_domain_bounds"] == {
        "dimension": {"minimum": 0, "maximum": 1023, "has_null": False}
    }
    assert inputs.oracle_digest == result_digest(_canonical())
    assert len(inputs.plan_digest) == 64


def test_load_gate_inputs_rejects_changed_plan_and_corrupt_digest(
    tmp_path, monkeypatch,
):
    spill_gate = importlib.import_module("supertable.engine.benchmarks.spill_gate")
    monkeypatch.setattr(
        spill_gate, "SEALED_ORACLE_DIGEST", result_digest(_canonical())
    )
    template_value = _request("islanddb")
    oracle_request_value = _request("duckdb")
    oracle_request_value["plan"]["sql"] += " ORDER BY dimension"
    template = tmp_path / "island-request.json"
    oracle_request = tmp_path / "duck-request.json"
    oracle_response = tmp_path / "duck-response.json"
    _write_json(template, template_value)
    _write_json(oracle_request, oracle_request_value)
    _write_json(oracle_response, {"ok": True, "result": _series("duckdb")})

    with pytest.raises(SpillGateConfigurationError, match="plan differs"):
        load_gate_inputs(
            request_template=template,
            oracle_request=oracle_request,
            oracle_response=oracle_response,
        )

    _write_json(oracle_request, _request("duckdb"))
    corrupt = _series("duckdb")
    corrupt["result_digest"] = "0" * 64
    _write_json(oracle_response, {"ok": True, "result": corrupt})
    with pytest.raises(SpillGateConfigurationError, match="digest"):
        load_gate_inputs(
            request_template=template,
            oracle_request=oracle_request,
            oracle_response=oracle_response,
        )


def test_docker_command_seals_four_cpu_four_gib_no_swap(tmp_path):
    repo = tmp_path / "repo"
    corpus = tmp_path / "corpus"
    attempt = tmp_path / "attempt"
    command = _docker_command(
        docker="docker",
        image="benchmark:test",
        container_name="spill-attempt",
        repo_root=repo,
        corpus_root=corpus,
        attempt_root=attempt,
    )

    assert command[:4] == ["docker", "run", "--name", "spill-attempt"]
    assert command[command.index("--cpus") + 1] == "4"
    assert command[command.index("--memory") + 1] == str(4 * 1024**3)
    assert command[command.index("--memory-swap") + 1] == str(4 * 1024**3)
    assert command[command.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES=2147483648" in command
    assert "SUPERTABLE_ISLAND_SPILL_DIR=/bench/island-spill" in command
    assert "POLARS_MAX_THREADS=4" in command
    assert command[-4:] == [
        "-m",
        "supertable.engine.benchmarks._worker",
        "/bench/request.json",
        "/bench/response.json",
    ]


def test_cgroup_io_totals_aggregates_all_devices():
    assert _cgroup_io_totals(
        "8:0 rbytes=10 wbytes=20 rios=1\n8:16 rbytes=30 wbytes=40 rios=2\n"
    ) == {"rbytes": 40, "wbytes": 60, "rios": 3}
    assert _cgroup_io_totals(None) is None


def test_success_gate_checks_parity_limits_and_extracts_metrics():
    duck = _series("duckdb")
    inputs = GateInputs(
        request=_request("islanddb"),
        oracle_series=duck,
        oracle_digest=duck["result_digest"],
        plan_digest="f" * 64,
    )
    island = _series("islanddb", wall_seconds=77.5)

    validated = _successful_result({"ok": True, "result": island}, inputs)
    metrics = _result_metrics(validated)

    assert metrics["wall_seconds"] == 77.5
    assert metrics["mean_cpu_cores"] == 3.0
    assert metrics["spill_bytes"] == 500
    assert metrics["process_io_delta"]["write_bytes"] == 500

    island["execution_context"]["cgroup_v2"]["swap_max_bytes"] = 1
    with pytest.raises(SpillGateConfigurationError, match="swap"):
        _successful_result({"ok": True, "result": island}, inputs)


def test_timeout_always_writes_diagnostic_artifact(tmp_path, monkeypatch):
    spill_gate = importlib.import_module("supertable.engine.benchmarks.spill_gate")

    class FakeProcess:
        returncode = None

        def __init__(self, *args, **kwargs):
            self.calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired("docker", timeout)
            self.returncode = 137
            return "partial stdout", "partial stderr"

        def kill(self):
            self.returncode = 137

    class FakeSampler:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def stop(self):
            pass

        def summary(self):
            return {
                "sample_count": 1,
                "process_rss_peak_bytes": 123,
                "cgroup_memory_peak_bytes": 456,
                "spill_high_water_bytes": 789,
                "samples": [],
            }

    monkeypatch.setattr(spill_gate.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(spill_gate, "_ContainerSampler", FakeSampler)
    monkeypatch.setattr(spill_gate, "_stop_container", lambda *args: None)
    monkeypatch.setattr(
        spill_gate,
        "_docker_state",
        lambda *args: {"State": {"ExitCode": 137}},
    )
    monkeypatch.setattr(spill_gate, "_remove_container", lambda *args: None)
    monkeypatch.setattr(spill_gate, "_git_identity", lambda *args: {"head": "abc"})
    duck = _series("duckdb")
    inputs = GateInputs(
        request=_request("islanddb"),
        oracle_series=duck,
        oracle_digest=duck["result_digest"],
        plan_digest="f" * 64,
    )
    attempt = tmp_path / "attempt-001"

    code, artifact = run_attempt(
        inputs=inputs,
        attempt_root=attempt,
        repo_root=tmp_path,
        corpus_root=tmp_path,
        docker="docker",
        image="benchmark:test",
        timeout_seconds=0.01,
        target_seconds=0.005,
        sample_interval_seconds=1.0,
    )

    assert code == EXIT_TIMEOUT
    assert artifact["status"] == "timeout"
    assert artifact["timed_out"] is True
    assert artifact["host_sampler"]["spill_high_water_bytes"] == 789
    assert (attempt / "attempt.json").is_file()
    assert (attempt / "stdout.log").read_text() == "partial stdout"
    assert json.loads((attempt / "attempt.json").read_text())["exit_code"] == 3
