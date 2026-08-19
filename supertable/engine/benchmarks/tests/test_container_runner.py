from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from supertable.engine.benchmarks import container_runner
from supertable.engine.benchmarks import islanddb as benchmark_cli
from supertable.engine.benchmarks.container_runner import (
    CONTAINER_MEMORY_BYTES,
    ContainerConfigurationError,
    ContainerRunnerConfig,
    DockerWorkerRunner,
    _cgroup_v2_extended_telemetry,
    _containerize_request_paths,
    _docker_command,
)


def _request(source: Path) -> dict:
    return {
        "purpose": "timing",
        "engine": "duckdb",
        "threads": 4,
        "memory_limit_bytes": 2 * 1024**3,
        "disable_caches": True,
        "plan": {
            "name": "aggregate_stats",
            "files": [str(source)],
            "original_files": [str(source)],
            "resource_keys": [str(source)],
            "row_group_selections": {
                str(source): {"row_group_count": 1, "eligible_ids": [0]},
            },
        },
    }


def _config(tmp_path: Path, corpus: Path) -> ContainerRunnerConfig:
    repo = tmp_path / "repo"
    repo.mkdir()
    artifacts = tmp_path / "artifacts"
    return ContainerRunnerConfig(
        repo_root=repo,
        corpus_root=corpus,
        artifact_root=artifacts,
        image="benchmark@sha256:" + "a" * 64,
    )


def test_request_paths_are_copied_mapped_and_confined(tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    source = corpus / "nested" / "part.parquet"
    source.parent.mkdir()
    source.write_bytes(b"PAR1")
    request = _request(source)

    converted = _containerize_request_paths(request, corpus)

    assert converted is not request
    assert request["plan"]["files"] == [str(source)]
    assert converted["plan"]["files"] == ["/corpus/nested/part.parquet"]
    assert converted["plan"]["original_files"] == [
        "/corpus/nested/part.parquet"
    ]
    assert converted["plan"]["resource_keys"] == [
        "/corpus/nested/part.parquet"
    ]
    assert list(converted["plan"]["row_group_selections"]) == [
        "/corpus/nested/part.parquet"
    ]

    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"PAR1")
    with pytest.raises(ContainerConfigurationError, match="escapes corpus"):
        _containerize_request_paths(_request(outside), corpus)


def test_extended_cgroup_snapshot_includes_cpu_pressure_io_and_pids(tmp_path):
    proc = tmp_path / "proc.cgroup"
    proc.write_text("0::/bench\n", encoding="ascii")
    current = tmp_path / "cgroup" / "bench"
    current.mkdir(parents=True)
    files = {
        "memory.current": "100\n",
        "memory.peak": "200\n",
        "memory.max": str(CONTAINER_MEMORY_BYTES) + "\n",
        "memory.swap.current": "0\n",
        "memory.swap.peak": "0\n",
        "memory.swap.max": "0\n",
        "memory.events": "oom 0\noom_kill 0\n",
        "memory.stat": "anon 80\nfile 20\n",
        "memory.pressure": "some avg10=0.10 avg60=0.20 avg300=0.30 total=10\n",
        "io.stat": "8:0 rbytes=11 wbytes=22 rios=1 wios=2\n",
        "cpu.stat": (
            "usage_usec 100\nuser_usec 60\nsystem_usec 40\n"
            "nr_periods 2\nnr_throttled 1\nthrottled_usec 5\n"
        ),
        "cpu.stat.local": "throttled_usec 5\n",
        "cpu.pressure": "some avg10=1.00 avg60=0.50 avg300=0.10 total=99\n",
        "io.pressure": (
            "some avg10=2.00 avg60=1.00 avg300=0.20 total=88\n"
            "full avg10=0.10 avg60=0.05 avg300=0.01 total=7\n"
        ),
        "pids.current": "9\n",
        "pids.peak": "12\n",
        "pids.max": "1024\n",
        "pids.events": "max 0\n",
        "pids.events.local": "max 0\n",
        "cpuset.cpus.effective": "0-3\n",
        "cpuset.mems.effective": "0\n",
        "cpu.max": "400000 100000\n",
    }
    for name, value in files.items():
        (current / name).write_text(value, encoding="ascii")

    snapshot = _cgroup_v2_extended_telemetry(
        proc_cgroup=proc,
        cgroup_root=tmp_path / "cgroup",
    )

    assert snapshot["available"] is True
    assert snapshot["cpu_stat"]["usage_usec"] == 100
    assert snapshot["cpu_pressure_parsed"]["some"]["total"] == 99
    assert snapshot["io_pressure_parsed"]["full"]["total"] == 7
    assert snapshot["pids_current_count"] == 9
    assert snapshot["pids_peak_count"] == 12
    assert snapshot["pids_max_count"] == 1024
    assert snapshot["cpuset_cpus_effective"] == "0-3"
    assert snapshot["cpu_max"] == "400000 100000"


def test_docker_command_enforces_quota_cpuset_memory_swap_and_read_only_mounts(
    tmp_path,
):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    source = corpus / "part.parquet"
    source.write_bytes(b"PAR1")
    config = _config(tmp_path, corpus)
    attempt = tmp_path / "attempt"
    cache = tmp_path / "cache"
    home = tmp_path / "home"
    for directory in (attempt, cache, home):
        directory.mkdir()
    command = _docker_command(
        config=config,
        request=_request(source),
        container_name="fresh-series",
        attempt_root=attempt,
        cache_dir=cache,
        home_dir=home,
    )

    assert command[:4] == ["docker", "run", "--name", "fresh-series"]
    assert command[command.index("--cpus") + 1] == "4"
    assert command[command.index("--cpuset-cpus") + 1] == "0-3"
    assert command[command.index("--memory") + 1] == str(4 * 1024**3)
    assert command[command.index("--memory-swap") + 1] == str(4 * 1024**3)
    assert command[command.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert "--read-only" in command
    assert command[command.index("--network") + 1] == "none"
    assert command[command.index("--cap-drop") + 1] == "ALL"
    mounts = [
        command[index + 1]
        for index, value in enumerate(command)
        if value == "--mount"
    ]
    assert any("dst=/workspace,readonly" in mount for mount in mounts)
    assert any("dst=/corpus,readonly" in mount for mount in mounts)
    assert any("dst=/bench" in mount and "readonly" not in mount for mount in mounts)
    assert command[-4:] == [
        "-m",
        "supertable.engine.benchmarks.container_worker",
        "/bench/request.json",
        "/bench/response.json",
    ]


def test_drop_in_runner_preserves_response_and_embeds_complete_provenance(
    tmp_path,
    monkeypatch,
):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    source = corpus / "part.parquet"
    source.write_bytes(b"PAR1")
    config = _config(tmp_path, corpus)

    class FakeProcess:
        returncode = 0

        def __init__(self, command, **_kwargs):
            mounts = [
                command[index + 1]
                for index, value in enumerate(command)
                if value == "--mount"
            ]
            bench_mount = next(mount for mount in mounts if "dst=/bench" in mount)
            source_field = next(
                item for item in bench_mount.split(",") if item.startswith("src=")
            )
            self.attempt = Path(source_field.removeprefix("src="))

        def communicate(self, timeout=None):
            del timeout
            result = {
                "engine": "duckdb",
                "engine_value": "duckdb",
                "execution_context": {
                    "configured_threads": 4,
                    "cgroup_v2": {
                        "memory_max_bytes": CONTAINER_MEMORY_BYTES,
                        "swap_max_bytes": 0,
                    },
                    "cgroup_memory_event_delta": {
                        "oom": 0,
                        "oom_kill": 0,
                        "oom_group_kill": 0,
                    },
                },
                "result": {"columns": ["count"], "dtypes": ["int64"], "rows": [[1]]},
                "result_digest": "d" * 64,
                "samples": [],
            }
            response = {
                "ok": True,
                "result": result,
                "worker_provenance": {
                    "before": {"dependencies": {"duckdb": "1.5.4"}},
                    "after": {
                        "dependencies": {"duckdb": "1.5.4"},
                        "cpu_affinity": [0, 1, 2, 3],
                        "cgroup_v2": {"cpuset_cpus_effective": "0-3"},
                    },
                },
            }
            (self.attempt / "response.json").write_text(
                json.dumps(response), encoding="utf-8"
            )
            return "worker stdout", ""

        def kill(self):
            self.returncode = 137

    class FakeSampler:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            pass

        def stop(self):
            pass

        def summary(self):
            return {
                "sample_count": 2,
                "cgroup_cpu_stat_delta": {"usage_usec": 123},
                "cgroup_cpu_pressure_total_delta_usec": {"some": 4},
                "cgroup_io_pressure_total_delta_usec": {"some": 5},
                "pids_peak_high_water": 8,
                "effective_cpuset_verified": True,
                "samples": [],
            }

    inspect = {
        "Image": "sha256:image",
        "State": {"OOMKilled": False, "ExitCode": 0},
        "HostConfig": {
            "Memory": CONTAINER_MEMORY_BYTES,
            "MemorySwap": CONTAINER_MEMORY_BYTES,
            "NanoCpus": 4_000_000_000,
            "CpusetCpus": "0-3",
            "ReadonlyRootfs": True,
            "NetworkMode": "none",
        },
    }
    image = {
        "reference": config.image,
        "id": "sha256:image",
        "repo_digests": [config.image],
        "content_digest": "sha256:" + "a" * 64,
        "inspect": {"Id": "sha256:image"},
    }
    monkeypatch.setattr(container_runner.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(container_runner, "_ContainerSampler", FakeSampler)
    monkeypatch.setattr(container_runner, "_container_inspect", lambda *_: inspect)
    monkeypatch.setattr(container_runner, "_image_provenance", lambda *_: image)
    monkeypatch.setattr(
        container_runner,
        "_git_identity",
        lambda *_: {"head": "f" * 40, "dirty": False},
    )
    monkeypatch.setattr(container_runner, "_remove_container", lambda *_: None)

    runner = DockerWorkerRunner(config)
    result = runner(
        _request(source),
        cache_dir=tmp_path / "cache",
        home_dir=tmp_path / "home",
        timeout_seconds=30,
    )

    record = result["container_run"]
    assert result["engine"] == "duckdb"
    assert record["status"] == "passed"
    assert record["limits"]["cpuset_cpus"] == "0-3"
    assert record["host_sampler"]["cgroup_cpu_stat_delta"] == {
        "usage_usec": 123
    }
    assert record["provenance"]["git"]["head"] == "f" * 40
    assert record["provenance"]["image"]["content_digest"] == (
        "sha256:" + "a" * 64
    )
    assert record["provenance"]["worker"]["after"]["dependencies"] == {
        "duckdb": "1.5.4"
    }
    artifact = json.loads(Path(record["artifact"]).read_text(encoding="utf-8"))
    assert artifact["response"]["result"]["engine"] == "duckdb"
    assert artifact["docker_inspect"]["HostConfig"]["CpusetCpus"] == "0-3"
    assert artifact["request"]["plan"]["files"] == ["/corpus/part.parquet"]
    assert Path(artifact["artifacts"]["stdout"]).read_text() == "worker stdout"


def test_config_requires_an_exact_four_cpu_cpuset(tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ContainerConfigurationError, match="exactly 4"):
        ContainerRunnerConfig(
            repo_root=repo,
            corpus_root=corpus,
            artifact_root=tmp_path / "artifacts",
            image="benchmark:test",
            cpuset_cpus="0-2",
        )


def test_cli_container_mode_injects_drop_in_runner_and_effective_limits(
    tmp_path,
    monkeypatch,
):
    captured = {}
    manifest = {
        "spec": {"tier": "100mib"},
        "actual_source_bytes": 1,
        "total_rows": 1,
        "files": [{"path": "part.parquet", "bytes": 1, "rows": 1}],
    }

    monkeypatch.setenv("SUPERTABLE_BENCHMARK_ROOT", str(tmp_path / "root"))
    monkeypatch.setattr(benchmark_cli, "prepare_corpus", lambda *_a, **_k: manifest)
    monkeypatch.setattr(benchmark_cli, "islanddb_available", lambda: True)
    monkeypatch.setattr(benchmark_cli, "_print_prepared", lambda *_: None)
    monkeypatch.setattr(benchmark_cli, "_print_comparison", lambda *_: None)

    def fake_compare(_manifest, **kwargs):
        captured.update(kwargs)
        return {"tier": "100mib", "workloads": []}

    monkeypatch.setattr(benchmark_cli, "compare_manifest", fake_compare)
    image = "benchmark@sha256:" + "b" * 64
    artifacts = tmp_path / "container-artifacts"
    output = tmp_path / "result.json"

    assert benchmark_cli.main(
        [
            "--sizes",
            "100mib",
            "--repeats",
            "1",
            "--container-image",
            image,
            "--container-artifact-root",
            str(artifacts),
            "--cpuset-cpus",
            "0-3",
            "--output",
            str(output),
        ]
    ) == 0

    assert isinstance(captured["worker_runner"], DockerWorkerRunner)
    assert captured["worker_runner"].config.image == image
    assert captured["worker_runner"].config.artifact_root == artifacts
    assert captured["worker_runner"].config.cpuset_cpus == "0-3"
    assert captured["config"].threads == 4
    assert captured["config"].memory_limit_bytes == 2 * 1024**3
    artifact = json.loads(output.read_text(encoding="utf-8"))
    assert artifact["config"]["container"] == {
        "enabled": True,
        "image": image,
        "artifact_root": str(artifacts),
        "cpus": 4,
        "cpuset_cpus": "0-3",
        "memory_bytes": 4 * 1024**3,
        "swap_bytes": 0,
    }
