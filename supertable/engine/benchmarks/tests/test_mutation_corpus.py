from __future__ import annotations

import copy

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.engine.benchmarks.corpus import plan_workload, sha256_file
from supertable.engine.benchmarks.mutation_corpus import (
    MUTATION_WORKLOAD_NAMES,
    MutationCorpusSpec,
    _mutation_source_schedule,
    _random_range,
    build_mutation_workloads,
    prepare_mutation_corpus,
    validate_mutation_manifest,
)
from supertable.engine.benchmarks.mutation_benchmark import (
    CONTAINER_CPUS,
    CONTAINER_MEMORY_BYTES,
    ENGINE_MEMORY_BYTES,
    run_mutation_benchmark,
)
from supertable.engine.benchmarks.runner import _build_reflection
from supertable.engine.benchmarks.runner import (
    assert_arrow_schema_parity,
    assert_exact_parity,
    assert_independent_oracle,
    islanddb_available,
    run_isolated_worker,
)


def _smoke_spec() -> MutationCorpusSpec:
    return MutationCorpusSpec(
        base_rows=1_000,
        base_files=5,
        updated_rows=40,
        update_operations=2,
        deleted_rows=59,
        delete_operations=2,
        tombstone_threshold=100,
        minimum_live_rows=900,
        minimum_snapshot_files=7,
        payload_columns=2,
        payload_width=8,
        batch_rows=64,
        row_group_target_bytes=4 * 1024,
    )


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"base_rows": 4, "base_files": 5}, "insert file"),
        ({"updated_rows": 1, "update_operations": 2}, "update operation"),
        ({"deleted_rows": 1, "delete_operations": 2}, "delete operation"),
    ],
)
def test_mutation_spec_rejects_zero_row_operations(overrides, message):
    values = {
        "base_rows": 100,
        "base_files": 5,
        "updated_rows": 4,
        "update_operations": 2,
        "deleted_rows": 5,
        "delete_operations": 2,
        "tombstone_threshold": 20,
        "minimum_live_rows": 90,
        "minimum_snapshot_files": 7,
    }
    values.update(overrides)
    with pytest.raises(ValueError, match=message):
        MutationCorpusSpec(**values)


@pytest.mark.parametrize(
    "overrides",
    [
        {"base_files": 1},
        {"update_operations": 1},
        {"delete_operations": 1},
    ],
)
def test_mutation_spec_requires_multiple_operations(overrides):
    values = {
        "base_rows": 100,
        "base_files": 5,
        "updated_rows": 4,
        "update_operations": 2,
        "deleted_rows": 5,
        "delete_operations": 2,
        "tombstone_threshold": 20,
        "minimum_live_rows": 90,
        "minimum_snapshot_files": 7,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match="at least two"):
        MutationCorpusSpec(**values)


def test_production_mutation_shape_stays_below_compaction_threshold():
    spec = MutationCorpusSpec()

    assert spec.base_rows == 11_000_000
    assert spec.updated_rows == 250_000
    assert spec.deleted_rows == 749_999
    assert spec.tombstone_rows == 999_999
    assert spec.tombstone_rows < spec.tombstone_threshold == 1_000_000
    assert spec.physical_rows == 11_250_000
    assert spec.live_rows == 10_250_001
    assert spec.snapshot_files == 133
    update_sources, delete_sources = _mutation_source_schedule(spec)
    assert len({index for index, _target in update_sources}) == 5
    assert len(set(delete_sources)) == 9
    assert not {index for index, _target in update_sources} & set(delete_sources)
    rows_per_file = [
        spec.base_rows // spec.base_files
        + (1 if index < spec.base_rows % spec.base_files else 0)
        for index in range(spec.base_files)
    ]
    starts = []
    cursor = 0
    for rows in rows_per_file:
        starts.append(cursor)
        cursor += rows
    update_rows = spec.updated_rows // spec.update_operations
    update_ranges = []
    for source_index, target in update_sources:
        first = min(
            max(starts[source_index], target),
            starts[source_index] + rows_per_file[source_index] - update_rows,
        )
        update_ranges.append((first, first + update_rows))
    for prune_percent in (99, 90, 50, 10, 1):
        lower, upper = _random_range(
            domain_rows=spec.base_rows,
            seed=spec.seed,
            prune_percent=prune_percent,
        )
        assert any(first < upper and last > lower for first, last in update_ranges)


def test_smoke_mutation_corpus_is_exact_reusable_and_tombstone_bound(tmp_path):
    spec = _smoke_spec()
    manifest = prepare_mutation_corpus(tmp_path, spec)

    assert validate_mutation_manifest(manifest, spec) == []
    assert manifest["physical_rows"] == 1_040
    assert manifest["tombstone_rows"] == 99
    assert manifest["live_rows"] == 941
    assert manifest["construction"] == {
        "mode": "deterministic_parquet_snapshot_simulation",
        "production_mutation_api_used": False,
        "production_compaction_executed": False,
        "tombstone_threshold_was_reached": False,
    }
    assert len(manifest["files"]) == 7
    assert [
        sum(item["rows"] for item in manifest["operations"] if item["kind"] == kind)
        for kind in ("insert", "update", "delete")
    ] == [1_000, 40, 59]
    assert max(
        item["tombstone_rows_after"] for item in manifest["operations"]
    ) == 99

    reused = prepare_mutation_corpus(tmp_path, spec)
    assert reused["reused"] is True
    assert reused["tombstone"]["sha256"] == manifest["tombstone"]["sha256"]


def test_mutation_manifest_rejects_same_size_source_and_pruning_tamper(
    tmp_path,
):
    spec = _smoke_spec()
    manifest = prepare_mutation_corpus(tmp_path, spec)
    first = manifest["files"][0]
    source = tmp_path / spec.corpus_id / first["path"]
    payload = bytearray(source.read_bytes())
    payload[100] ^= 1
    source.write_bytes(payload)

    problems = validate_mutation_manifest(manifest, spec)

    assert any("source digest mismatch" in problem for problem in problems)
    tampered = dict(manifest)
    tampered["files"] = [dict(entry) for entry in manifest["files"]]
    tampered["files"][1]["min_id"] += 1
    metadata_problems = validate_mutation_manifest(tampered, spec)
    assert any("source minimum id mismatch" in problem for problem in metadata_problems)

    derived = dict(manifest)
    derived["id_domain_rows"] += 1
    derived["snapshot_version"] -= 1
    derived_problems = validate_mutation_manifest(derived, spec)
    assert "derived mutation field differs: id_domain_rows" in derived_problems
    assert "derived mutation field differs: snapshot_version" in derived_problems


def test_mutation_manifest_seals_tombstone_metadata_and_row_domain(tmp_path):
    spec = _smoke_spec()
    manifest = prepare_mutation_corpus(tmp_path, spec)
    metadata_tamper = copy.deepcopy(manifest)
    metadata_tamper["tombstone"]["bytes"] += 1
    metadata_tamper["tombstone"]["format"] = 2

    metadata_problems = validate_mutation_manifest(metadata_tamper, spec)

    assert "tombstone size mismatch" in metadata_problems
    assert "tombstone format is invalid" in metadata_problems

    content_tamper = copy.deepcopy(manifest)
    path = tmp_path / spec.corpus_id / manifest["tombstone"]["path"]
    table = pq.read_table(path)
    keys = table["__file__"].to_pylist()
    rowids = table["__rowid__"].to_pylist()
    keys[0] = "benchmark/outside-snapshot.parquet"
    rowids[1] = rowids[0]
    rewritten = pa.Table.from_arrays(
        [
            pa.array(keys, type=pa.string()),
            pa.array(rowids, type=pa.int64()),
        ],
        schema=table.schema,
    )
    pq.write_table(rewritten, path, compression="zstd")
    content_tamper["tombstone"]["bytes"] = path.stat().st_size
    content_tamper["tombstone"]["sha256"] = sha256_file(path)

    content_problems = validate_mutation_manifest(content_tamper, spec)

    assert "tombstone key/row-ID domain is invalid" in content_problems
    assert "tombstone row IDs are not unique" in content_problems


def test_mutation_pruning_plans_are_random_bounded_matched_arrow_streams(
    tmp_path,
):
    manifest = prepare_mutation_corpus(tmp_path, _smoke_spec())
    workloads = build_mutation_workloads(manifest)

    assert tuple(workloads) == MUTATION_WORKLOAD_NAMES
    selected = [workloads[name].selected_percent for name in workloads]
    assert selected == [1.0, 10.0, 50.0, 90.0, 99.0]
    for workload in workloads.values():
        assert 0 <= workload.lower_id < workload.upper_id <= 1_000
        assert workload.arrow_stream_result is True
        assert workload.island_streaming_result is True
        assert "MAX(payload_00)" not in workload.sql
        assert "__rowid__" in workload.required_columns
        assert workload.selection_basis == "random_contiguous_base_id_domain_width"
        assert workload.selected_live_rows is not None
        assert workload.selected_live_percent is not None

    plan = plan_workload(manifest, workloads["prune_90pct"])
    reflection = _build_reflection(plan)

    assert plan["arrow_stream_result"] is True
    assert plan["prune_percent"] == 90.0
    assert plan["selected_percent"] == 10.0
    assert len(plan["original_resource_keys"]) == 7
    assert all(
        key.startswith("benchmark/islanddb-mutation-v3-")
        for key in plan["resource_keys"]
    )
    tombstone = reflection.tombstone_views["events"]
    assert tombstone.expected_rows == 99
    assert tombstone.tombstone_format == 3
    assert tombstone.cache_key.startswith(
        "benchmark/islanddb-mutation-v3-"
    )
    assert tuple(tombstone.snapshot_resource_keys) == tuple(
        plan["original_resource_keys"]
    )


def test_prepare_only_benchmark_writes_guarded_resource_and_plan_artifact(
    tmp_path,
):
    output = tmp_path / "result.json"
    artifact = run_mutation_benchmark(
        root=tmp_path / "benchmark",
        image="",
        output=output,
        repeats=1,
        spec=_smoke_spec(),
        prepare_only=True,
    )

    assert output.is_file()
    assert artifact["comparison"] is None
    assert artifact["container"] == {
        "cpus": CONTAINER_CPUS,
        "cpuset_cpus": "0-3",
        "memory_bytes": CONTAINER_MEMORY_BYTES,
        "swap_bytes": 0,
        "engine_memory_bytes": ENGINE_MEMORY_BYTES,
        "caches_disabled": True,
        "image": "",
    }
    assert artifact["dataset"]["tombstone_rows"] == 99
    assert artifact["dataset"]["construction"] == {
        "mode": "deterministic_parquet_snapshot_simulation",
        "production_mutation_api_used": False,
        "production_compaction_executed": False,
        "tombstone_threshold_was_reached": False,
    }
    assert len(artifact["dataset"]["manifest_sha256"]) == 64
    assert (
        artifact["plan_summary"]["maximum_unique_input_footprint_bytes"]
        <= 1024**3
    )
    assert artifact["plan_summary"]["execution_read_volume_bounded"] is False


def test_benchmark_rejects_over_gib_schema_before_generation(tmp_path):
    spec = MutationCorpusSpec(
        base_rows=1_000_000,
        base_files=10,
        updated_rows=10,
        update_operations=2,
        deleted_rows=10,
        delete_operations=2,
        tombstone_threshold=100,
        minimum_live_rows=999_990,
        minimum_snapshot_files=12,
        payload_columns=2,
        payload_width=1024,
    )

    with pytest.raises(RuntimeError, match="before corpus generation"):
        run_mutation_benchmark(
            root=tmp_path / "benchmark",
            image="",
            output=tmp_path / "result.json",
            repeats=1,
            spec=spec,
            prepare_only=True,
        )

    assert not (tmp_path / "benchmark" / "corpora" / spec.corpus_id).exists()


def test_mutation_benchmark_rejects_mutable_image_reference(tmp_path):
    with pytest.raises(ValueError, match="pinned by a SHA-256"):
        run_mutation_benchmark(
            root=tmp_path / "benchmark",
            image="benchmark:latest",
            output=tmp_path / "result.json",
            repeats=1,
            spec=_smoke_spec(),
        )

    assert not (tmp_path / "benchmark").exists()


@pytest.mark.skipif(
    not islanddb_available(), reason="Engine.ISLANDDB is not implemented",
)
def test_smoke_mutation_arrow_stream_matches_both_real_engines(tmp_path):
    manifest = prepare_mutation_corpus(tmp_path / "corpus", _smoke_spec())
    plan = plan_workload(
        manifest, build_mutation_workloads(manifest)["prune_1pct"],
    )

    def run(engine):
        return run_isolated_worker(
            {
                "purpose": "mutation-arrow-smoke",
                "engine": engine,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
                "memory_limit_bytes": 256 * 1024**2,
                "threads": 2,
                "disable_caches": True,
            },
            cache_dir=tmp_path / "cache" / engine,
            home_dir=tmp_path / "home" / engine,
            timeout_seconds=120,
        )

    results = {engine: run(engine) for engine in ("duckdb", "islanddb")}

    assert_exact_parity(
        results["duckdb"], results["islanddb"], label="mutation-smoke",
    )
    assert_arrow_schema_parity(
        results["duckdb"], results["islanddb"], label="mutation-smoke",
    )
    assert assert_independent_oracle(
        results, plan, label="mutation-smoke",
    )["matched_engines"] == ["duckdb", "islanddb"]
    assert all(
        result["samples"][0]["result_mode"] == "arrow_stream"
        for result in results.values()
    )
