from __future__ import annotations

import threading
from collections import namedtuple

import pyarrow as pa
import pytest

from supertable.engine.island_resources import (
    ArrowBatchStream,
    ByteBoundedArrowBatchIterator,
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


def test_byte_bounded_iterator_splits_wide_batches_without_losing_rows():
    batch = pa.record_batch(
        [pa.array([b"x" * MIB] * 5)], names=["payload"],
    )
    bounded = ByteBoundedArrowBatchIterator(
        [batch],
        schema=batch.schema,
        max_batch_rows=256,
        max_batch_bytes=2 * MIB + 32,
    )

    result = list(bounded)

    assert [item.num_rows for item in result] == [2, 2, 1]
    assert sum(item.num_rows for item in result) == 5
    assert all(item.nbytes <= 2 * MIB + 32 for item in result)


def test_byte_bounded_iterator_rejects_one_oversized_indivisible_row():
    batch = pa.record_batch(
        [pa.array([b"x" * (2 * MIB)])], names=["payload"],
    )
    source = ArrowBatchStream(batch.schema, [batch])
    bounded = ByteBoundedArrowBatchIterator(
        source,
        schema=batch.schema,
        max_batch_rows=256,
        max_batch_bytes=MIB,
    )

    with pytest.raises(ResultMemoryLimitExceeded, match="one Arrow result row"):
        next(bounded)
    assert source.closed is True


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


@pytest.mark.parametrize(
    ("controller_file", "malformed"),
    [
        ("cpu.max", "not-a-quota"),
        ("cpuset.cpus.effective", "0-bad"),
    ],
)
def test_detect_malformed_v2_cpu_control_fails_to_one_worker(
    tmp_path, controller_file, malformed,
):
    cg = tmp_path / "cg"
    cg.mkdir()
    (cg / controller_file).write_text(malformed)

    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=tmp_path / "missing-proc",
        affinity=tuple(range(8)),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0


def test_detect_intersects_every_cgroup_v2_ancestor(tmp_path):
    root = tmp_path / "cgroup"
    parent = root / "parent.slice"
    leaf = parent / "worker.scope"
    proc = tmp_path / "proc"
    leaf.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text(
        "0::/parent.slice/worker.scope\n",
    )
    (root / "cgroup.controllers").write_text("cpu cpuset memory\n")
    (root / "cpuset.cpus.effective").write_text("0-7\n")
    (parent / "cpuset.cpus.effective").write_text("0-3\n")
    (leaf / "cpuset.cpus.effective").write_text("0-5\n")
    (root / "cpu.max").write_text("max 100000\n")
    (parent / "cpu.max").write_text("200000 100000\n")
    (leaf / "cpu.max").write_text("400000 100000\n")
    (root / "memory.max").write_text("max\n")
    (parent / "memory.max").write_text(str(512 * MIB))
    (parent / "memory.current").write_text(str(128 * MIB))
    (leaf / "memory.max").write_text(str(GIB))
    (leaf / "memory.current").write_text(str(64 * MIB))

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.cpuset_cpus == (0, 1, 2, 3)
    assert resources.cpu_capacity == 2.0
    assert resources.cpu_count == 2
    assert resources.memory_limit_bytes == 512 * MIB
    assert resources.memory_available_bytes == 384 * MIB


@pytest.mark.parametrize(
    ("root_cpuset", "leaf_cpuset", "affinity"),
    [
        ("0-1", "2-3", (0, 1, 2, 3)),
        ("4-5", "4-5", (0, 1)),
    ],
)
def test_detect_contradictory_cpuset_state_fails_to_one_worker(
    tmp_path, root_cpuset, leaf_cpuset, affinity,
):
    root = tmp_path / "cgroup"
    leaf = root / "worker.scope"
    proc = tmp_path / "proc"
    leaf.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("0::/worker.scope\n")
    (root / "cgroup.controllers").write_text("cpuset\n")
    (root / "cpuset.cpus.effective").write_text(root_cpuset)
    (leaf / "cpuset.cpus.effective").write_text(leaf_cpuset)

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=affinity,
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0


def test_detect_supports_cgroup_v1_controller_hierarchies(tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text(
        "2:cpu,cpuacct:/parent/worker\n"
        "3:cpuset:/parent/worker\n"
        "4:memory:/parent/worker\n",
    )
    cpu_parent = root / "cpu,cpuacct" / "parent"
    cpu_leaf = cpu_parent / "worker"
    cpuset_parent = root / "cpuset" / "parent"
    cpuset_leaf = cpuset_parent / "worker"
    memory_parent = root / "memory" / "parent"
    memory_leaf = memory_parent / "worker"
    for directory in (cpu_leaf, cpuset_leaf, memory_leaf):
        directory.mkdir(parents=True)
    (cpu_parent / "cpu.cfs_quota_us").write_text("200000\n")
    (cpu_parent / "cpu.cfs_period_us").write_text("100000\n")
    (cpu_leaf / "cpu.cfs_quota_us").write_text("400000\n")
    (cpu_leaf / "cpu.cfs_period_us").write_text("100000\n")
    (cpuset_parent / "cpuset.cpus").write_text("0-3\n")
    (cpuset_leaf / "cpuset.cpus").write_text("0-5\n")
    (memory_parent / "memory.limit_in_bytes").write_text(str(512 * MIB))
    (memory_parent / "memory.usage_in_bytes").write_text(str(256 * MIB))
    (memory_leaf / "memory.limit_in_bytes").write_text(str(GIB))
    (memory_leaf / "memory.usage_in_bytes").write_text(str(128 * MIB))

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.cpuset_cpus == (0, 1, 2, 3)
    assert resources.cpu_capacity == 2.0
    assert resources.cpu_count == 2
    assert resources.memory_limit_bytes == 512 * MIB
    assert resources.memory_available_bytes == 256 * MIB


@pytest.mark.parametrize("controller", ["cpu", "cpuset"])
def test_detect_malformed_v1_cpu_control_fails_to_one_worker(
    tmp_path, controller,
):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    leaf = root / controller / "worker"
    leaf.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text(
        f"2:{controller}:/worker\n",
    )
    if controller == "cpu":
        (leaf / "cpu.cfs_quota_us").write_text("not-a-quota")
        (leaf / "cpu.cfs_period_us").write_text("100000")
    else:
        (leaf / "cpuset.cpus").write_text("0-bad")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0


def test_detect_intersects_hybrid_v2_and_v1_controller_limits(tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    unified = root / "unified"
    memory = root / "memory" / "legacy"
    unified.mkdir(parents=True)
    memory.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text(
        "0::/unified\n4:memory:/legacy\n",
    )
    (unified / "memory.max").write_text(str(GIB))
    (unified / "memory.current").write_text(str(128 * MIB))
    (memory / "memory.limit_in_bytes").write_text(str(512 * MIB))
    (memory / "memory.usage_in_bytes").write_text(str(256 * MIB))

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.memory_limit_bytes == 512 * MIB
    assert resources.memory_available_bytes == 256 * MIB


def test_detect_translates_mountinfo_roots_and_arbitrary_mountpoints(tmp_path):
    root = tmp_path / "cgroup"
    unified_mount = root / "unified-mount"
    unified_leaf = unified_mount / "worker"
    legacy_mount = root / "legacy-memory-mount"
    legacy_leaf = legacy_mount / "worker"
    proc = tmp_path / "proc"
    unified_leaf.mkdir(parents=True)
    legacy_leaf.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text(
        "0::/parent/worker\n4:memory:/legacy-root/worker\n",
    )
    (proc / "self" / "mountinfo").write_text(
        f"29 23 0:26 /parent {unified_mount} rw - cgroup2 cgroup rw\n"
        f"30 23 0:27 /legacy-root {legacy_mount} rw - "
        "cgroup cgroup rw,memory\n",
    )
    (unified_mount / "memory.max").write_text(str(GIB))
    (unified_mount / "memory.current").write_text(str(128 * MIB))
    (unified_leaf / "memory.max").write_text(str(768 * MIB))
    (unified_leaf / "memory.current").write_text(str(128 * MIB))
    (legacy_mount / "memory.limit_in_bytes").write_text(str(512 * MIB))
    (legacy_mount / "memory.usage_in_bytes").write_text(str(128 * MIB))
    (legacy_leaf / "memory.limit_in_bytes").write_text(str(256 * MIB))
    (legacy_leaf / "memory.usage_in_bytes").write_text(str(128 * MIB))

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.memory_limit_bytes == 256 * MIB
    assert resources.memory_available_bytes == 128 * MIB


def test_detect_honours_zero_cgroup_v2_memory_limit(tmp_path):
    cg = tmp_path / "cg"
    cg.mkdir()
    (cg / "memory.max").write_text("0\n")
    (cg / "memory.current").write_text("0\n")

    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=tmp_path / "missing-proc",
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


@pytest.mark.parametrize("current", [None, "malformed", "-1"])
def test_detect_missing_or_malformed_memory_usage_fails_availability_closed(
    tmp_path, current,
):
    cg = tmp_path / "cg"
    cg.mkdir()
    (cg / "memory.max").write_text(str(512 * MIB))
    if current is not None:
        (cg / "memory.current").write_text(current)

    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=tmp_path / "missing-proc",
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.memory_limit_bytes == 512 * MIB
    assert resources.memory_available_bytes == 0


@pytest.mark.parametrize("limit", ["", "malformed", "-1"])
def test_detect_empty_or_malformed_v2_memory_limit_fails_closed(
    tmp_path, limit,
):
    cg = tmp_path / "cg"
    cg.mkdir()
    (cg / "memory.max").write_text(limit)

    resources = ContainerResources.detect(
        cgroup_dir=cg,
        proc_root=tmp_path / "missing-proc",
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


@pytest.mark.parametrize(
    ("limit", "usage", "expected_limit"),
    [
        ("-1", "0", 0),
        (str(512 * MIB), "-1", 512 * MIB),
    ],
)
def test_detect_negative_v1_memory_controls_fail_closed(
    tmp_path, limit, usage, expected_limit,
):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    leaf = root / "memory" / "worker"
    leaf.mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("4:memory:/worker\n")
    (leaf / "memory.limit_in_bytes").write_text(limit)
    (leaf / "memory.usage_in_bytes").write_text(usage)

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=8 * GIB,
    )

    assert resources.memory_limit_bytes == expected_limit
    assert resources.memory_available_bytes == 0


def test_detect_rejects_v1_controller_mount_symlink_escape(tmp_path):
    root = tmp_path / "cgroup"
    outside = tmp_path / "outside"
    proc = tmp_path / "proc"
    root.mkdir()
    outside.mkdir()
    (root / "memory").symlink_to(outside, target_is_directory=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("4:memory:/\n")
    (outside / "memory.limit_in_bytes").write_text(str(16 * MIB))
    (outside / "memory.usage_in_bytes").write_text("0\n")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=(0,),
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


def test_detect_declared_unresolved_v1_cpu_controller_fails_closed(tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    root.mkdir()
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("2:cpu:/worker\n")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0


def test_detect_malformed_present_proc_cgroup_metadata_fails_closed(tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    root.mkdir()
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("not:a:valid:entry\n")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0
    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


@pytest.mark.parametrize("metadata_kind", ["empty", "unreadable"])
def test_detect_present_unusable_proc_cgroup_metadata_fails_closed(
    tmp_path, metadata_kind,
):
    root = tmp_path / "cgroup"
    proc_entry = tmp_path / "proc" / "self" / "cgroup"
    root.mkdir()
    proc_entry.parent.mkdir(parents=True)
    if metadata_kind == "empty":
        proc_entry.write_text("")
    else:
        proc_entry.mkdir()

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=tmp_path / "proc",
        affinity=tuple(range(8)),
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0
    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


def test_detect_declared_unresolved_v2_hierarchy_fails_closed(tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    root.mkdir()
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("0::/worker.scope\n")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=tuple(range(8)),
        host_memory_bytes=GIB,
        host_available_bytes=GIB,
    )

    assert resources.cpu_count == 1
    assert resources.cpu_capacity == 1.0
    assert resources.memory_limit_bytes == 0
    assert resources.memory_available_bytes == 0


def test_detect_rejects_proc_cgroup_path_escape(tmp_path):
    root = tmp_path / "cgroup"
    outside = tmp_path / "outside"
    proc = tmp_path / "proc"
    root.mkdir()
    outside.mkdir()
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("0::/../../outside\n")
    (root / "cgroup.controllers").write_text("memory\n")
    (root / "memory.max").write_text(str(512 * MIB))
    (root / "memory.current").write_text(str(128 * MIB))
    (outside / "memory.max").write_text(str(16 * MIB))
    (outside / "memory.current").write_text("0\n")

    resources = ContainerResources.detect(
        cgroup_root=root,
        proc_root=proc,
        affinity=(0,),
        host_memory_bytes=8 * GIB,
        host_available_bytes=3 * GIB,
    )

    assert resources.memory_limit_bytes == 512 * MIB
    assert resources.memory_available_bytes == 384 * MIB


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
