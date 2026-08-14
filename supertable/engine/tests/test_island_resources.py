from __future__ import annotations

import threading
from collections import namedtuple

import pyarrow as pa
import pytest

from supertable.engine.island_resources import (
    ArrowBatchStream,
    ContainerResources,
    ExecutionAdvice,
    QueryResourceEstimate,
    ResourceGovernor,
    ResourcePlanner,
    ResourcePolicy,
    ResourceReservationCancelled,
    ResourceReservationTimeout,
    ResultMemoryLimitExceeded,
    parse_cpuset,
)


DiskUsage = namedtuple("DiskUsage", "total used free")
MIB = 1024 * 1024
GIB = 1024 * MIB


def _resources(cpus: int = 4, memory: int = 4 * GIB) -> ContainerResources:
    return ContainerResources(
        cpu_count=cpus,
        cpu_capacity=float(cpus),
        affinity_cpus=tuple(range(cpus)),
        cpuset_cpus=tuple(range(cpus)),
        memory_limit_bytes=memory,
        memory_available_bytes=memory,
    )


def test_parse_cpuset_is_strict_and_deduplicates():
    assert parse_cpuset("0-2,2,7") == (0, 1, 2, 7)
    with pytest.raises(ValueError):
        parse_cpuset("2-1")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_resource_policy_rejects_non_finite_or_boolean_amplification(value):
    with pytest.raises(ValueError):
        ResourcePolicy(spill_amplification=value)


def test_detect_intersects_affinity_cpuset_quota_and_memory(tmp_path):
    cg = tmp_path / "cg"
    proc = tmp_path / "proc"
    cg.mkdir()
    proc.mkdir()
    (cg / "cpu.max").write_text("400000 100000\n")
    (cg / "cpuset.cpus.effective").write_text("0-7\n")
    (cg / "memory.max").write_text(str(2 * GIB))
    (cg / "memory.current").write_text(str(512 * MIB))
    (proc / "meminfo").write_text("MemAvailable:       3145728 kB\n")

    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.cpu_count == 4
    assert resources.cpu_capacity == 4.0
    assert resources.memory_limit_bytes == 2 * GIB
    assert resources.memory_available_bytes == 1536 * MIB


def test_detect_preserves_zero_cgroup_memory_available(tmp_path):
    cg = tmp_path / "cg"
    cg.mkdir()
    (cg / "memory.max").write_text(str(GIB))
    (cg / "memory.current").write_text(str(GIB))
    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=tmp_path / "missing-proc",
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )
    assert resources.memory_available_bytes == 0


def test_detect_preserves_explicit_zero_host_memory_available(tmp_path):
    resources = ContainerResources.detect(
        cgroup_dir=tmp_path / "missing-cgroup",
        proc_root=tmp_path / "missing-proc",
        affinity=(0,),
        host_memory_bytes=GIB,
        host_available_bytes=0,
    )
    assert resources.memory_available_bytes == 0


def test_large_fragmented_scan_can_use_all_four_container_cpus(tmp_path):
    planner = ResourcePlanner(_resources(), spill_root=tmp_path)
    plan = planner.plan(
        QueryResourceEstimate(
            compressed_scan_bytes=512 * MIB,
            decoded_scan_bytes=1024 * MIB,
            result_bytes=MIB,
            operator_state_bytes=16 * MIB,
            selected_files=120,
            selected_row_groups=500,
            estimated_rows=20_000_000,
        )
    )
    assert plan.advice == ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.cpu_workers == 4
    assert plan.io_workers >= 4
    assert 0 < plan.batch_rows


def test_fifty_gib_incremental_aggregate_needs_no_input_sized_spill(tmp_path):
    resources = _resources(cpus=8, memory=8 * GIB)
    plan = ResourcePlanner(resources, spill_root=tmp_path).plan(
        QueryResourceEstimate(
            compressed_scan_bytes=50 * GIB,
            decoded_scan_bytes=50 * GIB,
            result_bytes=31 * 4096,
            operator_state_bytes=31 * 4096,
            selected_files=405,
            selected_row_groups=7695,
            estimated_rows=32_000_000,
            estimated_result_rows=1,
            selected_decoded_bytes=50 * GIB,
            selected_decoded_bytes_complete=True,
            spillable=False,
        )
    )

    assert plan.advice is ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.memory_budget_bytes == int(8 * GIB * 0.60)
    assert plan.spill_budget_bytes == 0
    assert plan.estimated_spill_bytes == 0
    assert 0 < plan.batch_bytes <= plan.scan_memory_bytes


def test_tiny_query_accounts_for_process_global_worker_pools(tmp_path):
    plan = ResourcePlanner(_resources(), spill_root=tmp_path).plan(
        QueryResourceEstimate(
            compressed_scan_bytes=32 * 1024,
            decoded_scan_bytes=128 * 1024,
            result_bytes=1024,
            selected_files=1,
            selected_row_groups=1,
        )
    )
    # Polars/Arrow pools are process-global and cannot be narrowed per query.
    # Planning therefore accounts for their full possible widths even here.
    assert (plan.cpu_workers, plan.io_workers) == (4, 8)


def test_tight_operator_memory_produces_explicit_spill_budget(tmp_path):
    policy = ResourcePolicy(
        max_query_memory_bytes=512 * MIB,
        min_spill_free_bytes=64 * MIB,
        max_spill_bytes=4 * GIB,
    )
    planner = ResourcePlanner(
        _resources(memory=GIB),
        spill_root=tmp_path,
        policy=policy,
        disk_usage=lambda _: DiskUsage(10 * GIB, 0, 10 * GIB),
    )
    plan = planner.plan(
        QueryResourceEstimate(
            compressed_scan_bytes=128 * MIB,
            decoded_scan_bytes=512 * MIB,
            result_bytes=MIB,
            operator_state_bytes=384 * MIB,
            selected_files=100,
            selected_row_groups=100,
            has_sort=True,
        )
    )
    assert plan.advice == ExecutionAdvice.ISLAND_SPILL
    assert plan.estimated_spill_bytes > 0
    assert plan.spill_budget_bytes == plan.estimated_spill_bytes


def test_compact_group_state_keeps_bounded_operator_without_input_sized_spill(
    tmp_path,
):
    planner = ResourcePlanner(
        _resources(memory=4 * GIB),
        spill_root=tmp_path,
        disk_usage=lambda _: DiskUsage(10 * GIB, 0, 10 * GIB),
    )
    compact_state = 8 * MIB
    grouped_result = MIB
    plan = planner.plan(QueryResourceEstimate(
        compressed_scan_bytes=10 * GIB,
        decoded_scan_bytes=10 * GIB,
        result_bytes=grouped_result,
        operator_state_bytes=compact_state,
        selected_files=81,
        selected_row_groups=1_521,
        estimated_rows=6_413_677,
        estimated_result_rows=1_024,
        spillable=True,
        has_sort=True,
        has_group_by=True,
        requires_bounded_group_operator=True,
        group_state_bytes_per_key=8 * 1024,
    ))

    assert plan.advice is ExecutionAdvice.ISLAND_SPILL
    assert plan.estimated_spill_bytes == int(compact_state * 2.25)
    assert plan.estimated_spill_bytes < 32 * MIB
    assert plan.spill_budget_bytes == plan.estimated_spill_bytes
    assert "sealed group cardinality" in plan.reason


def test_sealed_domain_above_retained_target_budgets_partial_occurrences(tmp_path):
    planner = ResourcePlanner(
        _resources(memory=GIB),
        spill_root=tmp_path,
        disk_usage=lambda _: DiskUsage(10 * GIB, 0, 10 * GIB),
    )
    per_key = 1024
    selected_rows = 1_000_000
    plan = planner.plan(QueryResourceEstimate(
        compressed_scan_bytes=64 * MIB,
        decoded_scan_bytes=128 * MIB,
        result_bytes=16 * MIB,
        # Fits operator admission (~307 MiB) but exceeds its retained hash
        # target (~76 MiB), so one-domain compact budgeting is unsafe.
        operator_state_bytes=200 * MIB,
        estimated_rows=selected_rows,
        estimated_result_rows=200_000,
        spillable=True,
        has_group_by=True,
        requires_bounded_group_operator=True,
        group_state_bytes_per_key=per_key,
    ))

    worst_partials = selected_rows * per_key
    assert plan.advice is ExecutionAdvice.ISLAND_SPILL
    assert plan.estimated_spill_bytes == int(worst_partials * 2.25)


def test_disk_exhaustion_routes_away_instead_of_starting(tmp_path):
    policy = ResourcePolicy(
        max_query_memory_bytes=512 * MIB,
        min_spill_free_bytes=64 * MIB,
    )
    planner = ResourcePlanner(
        _resources(memory=GIB),
        spill_root=tmp_path,
        policy=policy,
        disk_usage=lambda _: DiskUsage(100 * MIB, 0, 100 * MIB),
    )
    plan = planner.plan(
        QueryResourceEstimate(
            compressed_scan_bytes=128 * MIB,
            decoded_scan_bytes=512 * MIB,
            result_bytes=MIB,
            operator_state_bytes=384 * MIB,
            selected_files=100,
            selected_row_groups=100,
            has_join=True,
        )
    )
    assert plan.advice == ExecutionAdvice.ROUTE_SPARK
    assert "disk" in plan.reason


def test_oversized_result_requires_streaming_then_plans_normally(tmp_path):
    planner = ResourcePlanner(_resources(memory=GIB), spill_root=tmp_path)
    estimate = QueryResourceEstimate(
        compressed_scan_bytes=32 * MIB,
        decoded_scan_bytes=128 * MIB,
        result_bytes=400 * MIB,
        operator_state_bytes=MIB,
        selected_files=10,
        selected_row_groups=10,
    )
    assert planner.plan(estimate).advice == ExecutionAdvice.STREAM_RESULT
    streamed = planner.plan(estimate, streaming_result=True)
    assert streamed.advice == ExecutionAdvice.ISLAND_IN_MEMORY
    assert streamed.runs_on_island


def test_output_expansion_reduces_batch_rows_before_execution(tmp_path):
    planner = ResourcePlanner(_resources(memory=GIB), spill_root=tmp_path)
    estimate = QueryResourceEstimate(
        compressed_scan_bytes=4 * MIB,
        decoded_scan_bytes=8 * MIB,
        result_bytes=800 * MIB,
        operator_state_bytes=0,
        selected_files=8,
        selected_row_groups=64,
        estimated_rows=1_000_000,
        estimated_result_rows=1_000_000,
    )

    plan = planner.plan(estimate, streaming_result=True)

    assert plan.advice == ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.batch_rows <= plan.batch_bytes // 800


def test_streamed_proof_work_does_not_collapse_selected_scan_batch_rows(tmp_path):
    planner = ResourcePlanner(_resources(memory=GIB), spill_root=tmp_path)
    selected_rows = 4
    selected_bytes = 76
    proof_bytes = 10_000_004 * 9
    estimate = QueryResourceEstimate(
        compressed_scan_bytes=128 * MIB,
        decoded_scan_bytes=selected_bytes + proof_bytes,
        result_bytes=selected_rows * 24,
        operator_state_bytes=16 * MIB,
        selected_files=1,
        selected_row_groups=1,
        estimated_rows=selected_rows,
        estimated_result_rows=selected_rows,
        selected_decoded_bytes=selected_bytes,
        selected_decoded_bytes_complete=True,
    )

    plan = planner.plan(estimate, streaming_result=True)

    assert plan.advice is ExecutionAdvice.ISLAND_IN_MEMORY
    # max(ceil(76/4), ceil(96/4)) == 24 bytes per selected output row.
    assert plan.batch_rows == plan.batch_bytes // 24
    assert plan.batch_rows > 1


def test_governor_serializes_process_global_scan_pools(tmp_path):
    policy = ResourcePolicy(
        query_memory_fraction=0.40,
        global_memory_fraction=0.80,
        max_query_memory_bytes=256 * MIB,
        min_spill_free_bytes=0,
    )
    resources = _resources(cpus=4, memory=GIB)
    planner = ResourcePlanner(resources, spill_root=tmp_path, policy=policy)
    plan = planner.plan(
        QueryResourceEstimate(
            compressed_scan_bytes=128 * MIB,
            decoded_scan_bytes=128 * MIB,
            result_bytes=MIB,
            selected_files=2,
            selected_row_groups=2,
        )
    )
    assert plan.cpu_workers == 4
    governor = ResourceGovernor(resources, spill_root=tmp_path, policy=policy)
    first = governor.reserve(plan, query_id="one")
    assert governor.snapshot()["cpu_reserved"] == 4
    with pytest.raises(ResourceReservationTimeout):
        governor.reserve(plan, query_id="two", timeout=0)
    first.release()
    with governor.reserve(plan, query_id="two", timeout=0):
        assert governor.snapshot()["active_queries"] == 1
    assert governor.snapshot()["active_queries"] == 0


def test_shared_governor_refresh_blocks_overcommit_after_memory_pressure(tmp_path):
    policy = ResourcePolicy(
        query_memory_fraction=0.40,
        global_memory_fraction=0.80,
        max_query_memory_bytes=256 * MIB,
        min_spill_free_bytes=0,
    )
    plentiful = _resources(cpus=4, memory=GIB)
    governor = ResourceGovernor(plentiful, spill_root=tmp_path, policy=policy)
    first_plan = ResourcePlanner(
        plentiful, spill_root=tmp_path, policy=policy,
    ).plan(QueryResourceEstimate(MIB, MIB, 1))
    first = governor.reserve(first_plan, query_id="before-pressure")

    pressured = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=tuple(range(4)),
        cpuset_cpus=tuple(range(4)),
        memory_limit_bytes=GIB,
        memory_available_bytes=320 * MIB,
    )
    governor.refresh_resources(pressured)
    second_plan = ResourcePlanner(
        pressured, spill_root=tmp_path, policy=policy,
    ).plan(QueryResourceEstimate(MIB, MIB, 1))

    assert governor.snapshot()["memory_capacity"] == 256 * MIB
    with pytest.raises(ResourceReservationTimeout):
        governor.reserve(second_plan, query_id="during-pressure", timeout=0)

    first.release()
    with governor.reserve(second_plan, query_id="after-release", timeout=0):
        assert governor.snapshot()["memory_reserved"] == 128 * MIB


def test_governor_zero_available_memory_has_zero_hard_capacity(tmp_path):
    resources = ContainerResources(
        cpu_count=1,
        cpu_capacity=1.0,
        affinity_cpus=(0,),
        cpuset_cpus=(0,),
        memory_limit_bytes=GIB,
        memory_available_bytes=0,
    )
    governor = ResourceGovernor(resources, spill_root=tmp_path)

    assert governor.snapshot()["memory_capacity"] == 0


def test_cancelled_governor_wait_fails_without_reserving(tmp_path):
    policy = ResourcePolicy(max_query_memory_bytes=256 * MIB, min_spill_free_bytes=0)
    resources = _resources(cpus=1, memory=GIB)
    planner = ResourcePlanner(resources, spill_root=tmp_path, policy=policy)
    plan = planner.plan(
        QueryResourceEstimate(MIB, MIB, 1, selected_files=1, selected_row_groups=1)
    )
    governor = ResourceGovernor(resources, spill_root=tmp_path, policy=policy)
    first = governor.reserve(plan)
    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(ResourceReservationCancelled):
        governor.reserve(plan, cancel_event=cancelled)
    first.release()


def test_arrow_stream_is_bounded_and_closes_producer():
    schema = pa.schema([("id", pa.int64())])
    closed = []
    stream = ArrowBatchStream(
        schema,
        [pa.record_batch([[1, 2, 3]], schema=schema)],
        close_callback=lambda: closed.append(True),
    )
    with pytest.raises(ResultMemoryLimitExceeded):
        stream.collect_table(max_bytes=1)
    assert stream.closed
    assert closed == [True]


def test_arrow_reader_streams_identical_batches():
    table = pa.table({"id": list(range(25))})
    reader = ArrowBatchStream.from_table(table, max_chunksize=4).to_reader()
    assert reader.read_all().equals(table)


def test_owned_arrow_reader_partial_close_releases_source_once():
    table = pa.table({"id": list(range(25))})
    closed = []
    stream = ArrowBatchStream(
        table.schema,
        table.to_batches(max_chunksize=4),
        close_callback=lambda: closed.append(True),
    )
    reader = stream.to_reader()

    assert reader.read_next_batch().num_rows == 4
    reader.close()
    reader.close()

    assert stream.closed
    assert closed == [True]


@pytest.mark.parametrize(
    ("operation", "expected_error"),
    [
        ("close", StopIteration),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_owned_arrow_reader_concurrent_shutdown_is_cooperative(
    operation, expected_error,
):
    schema = pa.schema([("id", pa.int64())])
    entered = threading.Event()
    release = threading.Event()
    cleaned = []
    batches = []
    errors = []

    def producer():
        try:
            entered.set()
            assert release.wait(5)
            yield pa.record_batch([[1]], schema=schema)
        finally:
            cleaned.append("producer")

    stream = ArrowBatchStream(
        schema,
        producer(),
        close_callback=lambda: cleaned.append("callback"),
    )
    reader = stream.to_reader()

    def consume():
        try:
            batches.append(reader.read_next_batch())
        except BaseException as exc:  # thread result is asserted below
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(5)

    getattr(reader, operation)()
    assert not reader.closed
    assert cleaned == []
    release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert batches == []
    assert len(errors) == 1
    assert isinstance(errors[0], expected_error)
    assert reader.closed
    assert stream.closed
    assert cleaned == ["producer", "callback"]


@pytest.mark.parametrize(
    ("operation", "expected_error"),
    [
        ("close", ValueError),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_owned_arrow_reader_read_all_never_returns_partial_on_shutdown(
    operation, expected_error,
):
    schema = pa.schema([("id", pa.int64())])
    entered_second = threading.Event()
    release_second = threading.Event()
    results = []
    errors = []

    def producer():
        yield pa.record_batch([[1]], schema=schema)
        entered_second.set()
        assert release_second.wait(5)
        yield pa.record_batch([[2]], schema=schema)

    reader = ArrowBatchStream(schema, producer()).to_reader()

    def consume():
        try:
            results.append(reader.read_all())
        except BaseException as exc:  # thread result is asserted below
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered_second.wait(5)
    getattr(reader, operation)()
    release_second.set()
    worker.join(5)

    assert not worker.is_alive()
    assert results == []
    assert len(errors) == 1
    assert isinstance(errors[0], expected_error)
    assert reader.closed


def test_concurrent_cancel_waits_for_next_and_never_yields_post_cancel_batch():
    schema = pa.schema([("id", pa.int64())])
    entered = threading.Event()
    release = threading.Event()
    cleaned = []
    batches = []
    errors = []

    def producer():
        try:
            entered.set()
            assert release.wait(5)
            yield pa.record_batch([[1]], schema=schema)
        finally:
            cleaned.append("producer")

    stream = ArrowBatchStream(
        schema,
        producer(),
        close_callback=lambda: cleaned.append("callback"),
    )

    def consume():
        try:
            batches.append(next(stream))
        except BaseException as exc:  # thread result is asserted below
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(5)

    stream.cancel()
    assert not stream.closed
    assert cleaned == []
    release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert batches == []
    assert len(errors) == 1
    assert isinstance(errors[0], ResourceReservationCancelled)
    assert stream.closed
    assert cleaned == ["producer", "callback"]


def test_cancel_propagates_through_nested_arrow_streams():
    schema = pa.schema([("id", pa.int64())])
    inner_cancelled = threading.Event()
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def producer():
        entered.set()
        assert release.wait(5)
        yield pa.record_batch([[1]], schema=schema)

    inner = ArrowBatchStream(
        schema,
        producer(),
        cancel_event=inner_cancelled,
    )
    outer = ArrowBatchStream(schema, inner)

    def consume():
        try:
            next(outer)
        except BaseException as exc:  # thread result is asserted below
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(5)

    outer.cancel()
    assert inner_cancelled.wait(1)
    release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ResourceReservationCancelled)
    assert inner.closed
    assert outer.closed


def test_query_plan_never_exceeds_global_governor_memory_capacity(tmp_path):
    resources = _resources(cpus=2, memory=GIB)
    policy = ResourcePolicy(
        query_memory_fraction=0.90,
        global_memory_fraction=0.50,
        result_memory_fraction=0.20,
        operator_memory_fraction=0.30,
        min_spill_free_bytes=0,
    )
    planner = ResourcePlanner(resources, spill_root=tmp_path, policy=policy)
    plan = planner.plan(QueryResourceEstimate(MIB, MIB, 1))
    governor = ResourceGovernor(resources, spill_root=tmp_path, policy=policy)

    assert plan.memory_budget_bytes <= governor.snapshot()["memory_capacity"]
    with governor.reserve(plan, timeout=0):
        assert governor.snapshot()["active_queries"] == 1


def test_planned_delivery_batches_fit_conservative_scan_workspace(tmp_path):
    resources = _resources(cpus=32, memory=128 * MIB)
    policy = ResourcePolicy(
        query_memory_fraction=1.0,
        global_memory_fraction=1.0,
        result_memory_fraction=0.49,
        operator_memory_fraction=0.50,
        min_query_memory_bytes=32 * MIB,
        min_batch_bytes=16 * MIB,
        max_batch_bytes=32 * MIB,
        bytes_per_cpu_worker=MIB,
        bytes_per_io_worker=MIB,
        max_io_workers=32,
        min_spill_free_bytes=0,
    )
    plan = ResourcePlanner(
        resources, spill_root=tmp_path, policy=policy,
    ).plan(QueryResourceEstimate(
        compressed_scan_bytes=GIB,
        decoded_scan_bytes=GIB,
        result_bytes=1,
        selected_files=128,
        selected_row_groups=128,
    ))

    assert plan.runs_on_island
    assert plan.batch_bytes >= 1
    # This bounds planned delivered batches against every possible native pool
    # slot. It is conservative admission arithmetic, not a claim that the
    # Polars allocator exposes a hard per-query scan-memory ceiling.
    assert plan.batch_bytes * (plan.cpu_workers + plan.io_workers) <= plan.scan_memory_bytes
