from __future__ import annotations

import json
import importlib
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pytest

from supertable.engine.benchmarks.real_spill_gate import (
    CONTAINER_MEMORY_BYTES,
    DUCKDB_MIN_MATERIAL_SPILL_BYTES,
    ENGINE_MEMORY_BYTES,
    EXIT_TIMEOUT,
    PUBLIC_COLUMNS,
    RealSpillConfigurationError,
    RealSpillInputs,
    _docker_command,
    _result_schema,
    load_real_spill_inputs,
    run_engine_attempt,
)
from supertable.engine.benchmarks.real_spill_worker import (
    RealSpillWorkerError,
    ResultColumn,
    StreamingResultDigest,
)


ROWS = 6_413_677
SOURCE_BYTES = 10 * 1024**3 + 1
RESULT_BYTES = ROWS * 1_692


def _schema() -> dict[str, str]:
    return {
        "id": "Int64",
        "event_ts": "Datetime(time_unit='us', time_zone=None)",
        "metric": "Int64",
        "dimension": "Int32",
        **{f"payload_{index:02d}": "Binary" for index in range(26)},
    }


def _request() -> dict:
    return {
        "engine": "islanddb",
        "warm_repeats": 0,
        "cold_mode": "fadvise",
        "memory_limit_bytes": ENGINE_MEMORY_BYTES,
        "threads": 4,
        "disable_caches": True,
        "minimum_cold_read_fraction": None,
        "plan": {
            "name": "spill_group",
            "table": "events",
            "super_name": "island_benchmark",
            "files": ["/corpus/part-00000.parquet"],
            "required_columns": list(PUBLIC_COLUMNS),
            "schema": _schema(),
            "source_bytes": SOURCE_BYTES,
            "source_repeat": 1,
            "candidate_rows": ROWS,
            "estimated_decoded_bytes": RESULT_BYTES,
            "decoded_estimate_complete": True,
            "projected_source_fraction": 0.99,
        },
    }


def _write_sealed_fixture(tmp_path: Path) -> tuple[Path, Path]:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    parquet = corpus / "part-00000.parquet"
    # Sparse allocation keeps this validation fixture cheap while exercising
    # the exact physical-size seal used before Docker starts.
    with parquet.open("wb") as handle:
        handle.seek(SOURCE_BYTES - 1)
        handle.write(b"\0")
    manifest = {
        "format_version": 1,
        "generator": "islanddb-wide-v1",
        "total_rows": ROWS,
        "actual_source_bytes": SOURCE_BYTES,
        "spec": {
            "tier": "10gib",
            "payload_columns": 26,
            "payload_width": 64,
        },
        "files": [{
            "path": parquet.name,
            "bytes": SOURCE_BYTES,
            "rows": ROWS,
            "min_id": 0,
            "max_id": ROWS - 1,
        }],
    }
    (corpus / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    template = tmp_path / "request.json"
    template.write_text(json.dumps(_request()), encoding="utf-8")
    return template, corpus


def test_load_real_spill_inputs_seals_full_width_ordered_stream(tmp_path):
    template, corpus = _write_sealed_fixture(tmp_path)

    inputs = load_real_spill_inputs(
        request_template=template,
        corpus_root=corpus,
    )

    plan = inputs.request["plan"]
    assert plan["name"] == "real_spill_sort"
    assert plan["sql"] == (
        f"SELECT {', '.join(PUBLIC_COLUMNS)} FROM events ORDER BY metric, id"
    )
    assert plan["stream_result_digest"] is True
    assert plan["result_schema"] == _result_schema()
    assert plan["integer_domain_bounds"] == {
        "id": {"minimum": 0, "maximum": ROWS - 1, "has_null": False},
        "metric": {"minimum": 0, "maximum": 1_000_002, "has_null": False},
        "dimension": {
            "minimum": 0,
            "maximum": min(1_023, ROWS - 1),
            "has_null": False,
        },
    }
    assert inputs.expected_rows == ROWS
    assert inputs.expected_value_bytes == RESULT_BYTES
    assert len(inputs.plan_digest) == 64


def test_load_real_spill_inputs_rejects_changed_file_and_nonunique_id_manifest(
    tmp_path,
):
    template, corpus = _write_sealed_fixture(tmp_path)
    request = json.loads(template.read_text())
    request["plan"]["files"] = ["/corpus/wrong.parquet"]
    template.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(RealSpillConfigurationError, match="identity"):
        load_real_spill_inputs(request_template=template, corpus_root=corpus)

    request = _request()
    template.write_text(json.dumps(request), encoding="utf-8")
    manifest_path = corpus / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["min_id"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RealSpillConfigurationError, match="unique contiguous"):
        load_real_spill_inputs(request_template=template, corpus_root=corpus)


def _digest_columns() -> tuple[ResultColumn, ...]:
    return (
        ResultColumn("id", pa.int64(), "int64", 8),
        ResultColumn("metric", pa.int64(), "int64", 8),
        ResultColumn("payload_00", pa.binary(), "binary", 4),
    )


def _batch(start: int, stop: int, *, mutate: bool = False) -> pa.RecordBatch:
    row_ids = [1, 2, 0, 3]
    metrics = [0, 0, 1, 1]
    payloads = [b"aaaa", b"bbbb", b"cccc", b"dddd"]
    if mutate:
        payloads[2] = b"zzzz"
    return pa.record_batch(
        [
            pa.array(row_ids[start:stop], type=pa.int64()),
            pa.array(metrics[start:stop], type=pa.int64()),
            pa.array(payloads[start:stop], type=pa.binary()),
        ],
        names=["id", "metric", "payload_00"],
    )


def _streaming_proof(splits: list[tuple[int, int]], *, mutate=False) -> dict:
    digest = StreamingResultDigest(_digest_columns())
    for start, stop in splits:
        digest.update(_batch(start, stop, mutate=mutate))
    return digest.finish()


def test_streaming_digest_is_batch_boundary_independent_and_value_exact():
    whole = _streaming_proof([(0, 4)])
    split = _streaming_proof([(0, 1), (1, 3), (3, 4)])
    changed = _streaming_proof([(0, 2), (2, 4)], mutate=True)

    assert whole == split
    assert whole["row_count"] == 4
    assert whole["logical_value_bytes"] == 80
    assert whole["digest"] != changed["digest"]
    assert whole["column_sha256"]["id"] == changed["column_sha256"]["id"]
    assert (
        whole["column_sha256"]["payload_00"]
        != changed["column_sha256"]["payload_00"]
    )


def test_streaming_digest_rejects_out_of_order_and_wrong_binary_width():
    digest = StreamingResultDigest(_digest_columns())
    bad_order = pa.record_batch(
        [pa.array([2, 1]), pa.array([0, 0]), pa.array([b"bbbb", b"aaaa"])],
        names=["id", "metric", "payload_00"],
    )
    with pytest.raises(RealSpillWorkerError, match="strictly ordered"):
        digest.update(bad_order)

    digest = StreamingResultDigest(_digest_columns())
    bad_width = pa.record_batch(
        [pa.array([1]), pa.array([0]), pa.array([b"short"])],
        names=["id", "metric", "payload_00"],
    )
    with pytest.raises(RealSpillWorkerError, match="fixed 4-byte"):
        digest.update(bad_width)


def test_docker_command_enforces_fresh_four_cpu_four_gib_no_swap(tmp_path):
    command = _docker_command(
        docker="docker",
        image="benchmark:test",
        container_name="real-spill-duckdb",
        repo_root=tmp_path / "repo",
        corpus_root=tmp_path / "corpus",
        attempt_root=tmp_path / "attempt",
    )

    assert command[:4] == ["docker", "run", "--name", "real-spill-duckdb"]
    assert command[command.index("--cpus") + 1] == "4"
    assert command[command.index("--memory") + 1] == str(CONTAINER_MEMORY_BYTES)
    assert command[command.index("--memory-swap") + 1] == str(
        CONTAINER_MEMORY_BYTES
    )
    assert command[command.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert "SUPERTABLE_ISLAND_SPILL_DIR=/bench/engine-spill" in command
    assert "SUPERTABLE_ISLAND_SPILL_MAX_BYTES=30064771072" in command
    assert "SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC=295" in command
    assert command[-4:] == [
        "-m",
        "supertable.engine.benchmarks.real_spill_worker",
        "/bench/request.json",
        "/bench/response.json",
    ]


def test_timeout_preserves_sampler_io_rss_and_spill_artifact(tmp_path, monkeypatch):
    gate = importlib.import_module(
        "supertable.engine.benchmarks.real_spill_gate"
    )

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
                "sample_count": 2,
                "process_rss_peak_bytes": 123,
                "cgroup_memory_peak_bytes": 456,
                "spill_high_water_bytes": 789,
                "samples": [
                    {
                        "process_io": {"read_bytes": 10, "write_bytes": 20},
                        "cgroup": {"io": {"rbytes": 30, "wbytes": 40}},
                    },
                    {
                        "process_io": {"read_bytes": 110, "write_bytes": 220},
                        "cgroup": {"io": {"rbytes": 330, "wbytes": 440}},
                    },
                ],
            }

    monkeypatch.setattr(gate.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(gate, "_ContainerSampler", FakeSampler)
    monkeypatch.setattr(gate, "_stop_container", lambda *args: None)
    monkeypatch.setattr(gate, "_docker_state", lambda *args: {"State": {}})
    monkeypatch.setattr(gate, "_remove_container", lambda *args: None)
    monkeypatch.setattr(gate, "_git_identity", lambda *args: {"head": "abc"})
    inputs = RealSpillInputs(
        request=_request(),
        plan_digest="f" * 64,
        expected_rows=ROWS,
        expected_value_bytes=RESULT_BYTES,
        minimum_spill_bytes={
            "duckdb": DUCKDB_MIN_MATERIAL_SPILL_BYTES,
            "islanddb": 1024**3,
        },
    )

    code, artifact = run_engine_attempt(
        inputs=inputs,
        engine="duckdb",
        attempt_root=tmp_path / "attempt",
        repo_root=tmp_path,
        corpus_root=tmp_path,
        docker="docker",
        image="benchmark:test",
        timeout_seconds=0.01,
        sample_interval_seconds=1.0,
    )

    assert code == EXIT_TIMEOUT
    assert artifact["status"] == "timeout"
    assert artifact["host_sampler"]["spill_high_water_bytes"] == 789
    assert artifact["host_sampler"]["process_io_sample_delta"] == {
        "read_bytes": 100,
        "write_bytes": 200,
    }
    assert artifact["host_sampler"]["cgroup_io_sample_delta"] == {
        "rbytes": 300,
        "wbytes": 400,
    }
    assert json.loads((tmp_path / "attempt" / "attempt.json").read_text())[
        "timed_out"
    ] is True
