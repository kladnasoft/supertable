"""Safety and packing contract for fused tombstone + small-file compaction.

These tests deliberately exercise ``compact_resources`` directly.  They do
not prescribe snapshot schema publication (the writer remains authoritative
for that); they only require the physical compaction to preserve the current
union-of-source-columns behaviour while consuming deletion-vector groups.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
from collections import Counter
from types import SimpleNamespace
from typing import Dict, Iterable, List, Tuple

import polars as pl
import pytest


os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")


MIB = 1024 * 1024
TARGET_BYTES = 16 * MIB


def _dv(rows: Iterable[Tuple[str, int]]) -> pl.DataFrame:
    pairs = list(rows)
    return pl.DataFrame(
        {
            "__file__": pl.Series(
                [path for path, _rowid in pairs], dtype=pl.String,
            ),
            "__rowid__": pl.Series(
                [rowid for _path, rowid in pairs], dtype=pl.Int64,
            ),
        }
    )


def _union(frames: Iterable[pl.DataFrame]) -> pl.DataFrame:
    materialized = [frame for frame in frames if frame.height]
    if not materialized:
        return pl.DataFrame()
    return pl.concat(materialized, how="diagonal_relaxed")


def _canonical_digest(frame: pl.DataFrame) -> str:
    """Independent, order-insensitive digest for the scalar test corpus."""
    columns = sorted(frame.columns)
    ordered = frame.select(columns).sort("__rowid__")
    payload = json.dumps(
        {"columns": columns, "rows": ordered.rows()},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _row_multiset(frame: pl.DataFrame, columns: List[str]) -> Counter:
    return Counter(frame.select(columns).iter_rows())


def _twenty_mixed_sources() -> Tuple[List[Dict], Dict[str, pl.DataFrame]]:
    """Twenty sources with reordered, missing, and legacy-only columns."""
    resources: List[Dict] = []
    sources: Dict[str, pl.DataFrame] = {}

    for file_index in range(20):
        path = f"source-{file_index:02d}.parquet"
        rowids = list(range(file_index * 12 + 1, file_index * 12 + 13))
        values = {
            "__rowid__": pl.Series(rowids, dtype=pl.Int64),
            "file_index": [file_index] * 12,
            "slot": list(range(12)),
            "payload": [f"payload-{slot % 4}" for slot in range(12)],
            "current_value": [f"current-{file_index}-{slot}" for slot in range(12)],
        }

        variant = file_index % 4
        if variant == 0:
            frame = pl.DataFrame(values).with_columns(
                pl.lit(f"legacy-{file_index}").alias("legacy_only")
            )
        elif variant == 1:
            # Same fields, deliberately different physical column order.
            frame = pl.DataFrame(values).select(
                "payload", "slot", "__rowid__", "current_value", "file_index",
            )
        elif variant == 2:
            # An older file lacks a column present in the latest physical schema.
            frame = pl.DataFrame(values).drop("current_value")
        else:
            frame = (
                pl.DataFrame(values)
                .with_columns(pl.lit(f"legacy-{file_index}").alias("legacy_only"))
                .select(
                    "legacy_only", "file_index", "__rowid__", "payload",
                    "current_value", "slot",
                )
            )

        sources[path] = frame
        resources.append(
            {
                "file": path,
                "file_size": 4 * MIB,
                "rows": frame.height,
            }
        )

    return resources, sources


def _forbid_compaction_io(monkeypatch, processing) -> None:
    """Make any source read or successor write fail the current test."""

    def fail_read(*_args, **_kwargs):
        pytest.fail("argument/snapshot validation performed a source read")

    def fail_write(*_args, **_kwargs):
        pytest.fail("argument/snapshot validation minted an output")

    monkeypatch.setattr(processing, "_read_parquet_safe", fail_read)
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", fail_write,
    )


def test_fused_tombstones_require_residual_return_before_any_io(monkeypatch):
    from supertable import processing

    _forbid_compaction_io(monkeypatch, processing)

    with pytest.raises(ValueError, match="requires return_residual=True"):
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": "source.parquet", "file_size": 1, "rows": 1}
                ]
            },
            data_dir="data",
            compression_level=1,
            tombstone_df=_dv([("source.parquet", 1)]),
        )


@pytest.mark.parametrize(
    "legacy_kwargs",
    [
        {"dead_rowids": {1}},
        {"dead_rowids_by_file": {"source.parquet": {1}}},
    ],
)
def test_fused_tombstones_reject_legacy_delete_inputs_before_any_io(
    monkeypatch, legacy_kwargs,
):
    from supertable import processing

    _forbid_compaction_io(monkeypatch, processing)

    with pytest.raises(ValueError, match="cannot be combined"):
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": "source.parquet", "file_size": 1, "rows": 1}
                ]
            },
            data_dir="data",
            compression_level=1,
            tombstone_df=_dv([("source.parquet", 1)]),
            return_residual=True,
            **legacy_kwargs,
        )


def test_fused_duplicate_resource_identity_rejects_before_any_io(monkeypatch):
    from supertable import processing

    _forbid_compaction_io(monkeypatch, processing)

    duplicate = {"file": "same.parquet", "file_size": 1, "rows": 1}
    with pytest.raises(ValueError, match="duplicate resource path"):
        processing.compact_resources(
            snapshot={"resources": [duplicate, dict(duplicate)]},
            data_dir="data",
            compression_level=1,
            tombstone_df=_dv([("same.parquet", 1)]),
            return_residual=True,
        )


@pytest.mark.parametrize(
    ("dead_rowids", "expected_sunset", "expected_residual"),
    [
        ([1, 2, 3], {"full.parquet"}, []),
        ([1, 2, 999], set(), [1, 2, 999]),
    ],
)
def test_fused_fully_dead_fast_path_requires_exact_projected_identity(
    monkeypatch, dead_rowids, expected_sunset, expected_residual,
):
    """Equal row counts admit a projection, but only exact IDs sunset."""
    from supertable import processing

    physical = pl.DataFrame(
        {"__rowid__": pl.Series([1, 2, 3], dtype=pl.Int64)}
    )
    reads = []

    def read_projected(path, **kwargs):
        reads.append((path, kwargs.get("columns"), kwargs.get("required")))
        assert kwargs.get("columns") == ["__rowid__"]
        return physical.select(kwargs["columns"])

    monkeypatch.setattr(processing, "_read_parquet_safe", read_projected)
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **_kwargs: pytest.fail("fully-dead candidate was rewritten"),
    )

    considered, rows, resources, sunset, residual = processing.compact_resources(
        snapshot={
            "resources": [
                {"file": "full.parquet", "file_size": 10_000, "rows": 3}
            ]
        },
        data_dir="data",
        compression_level=1,
        table_config={"max_memory_chunk_size": 100},
        small_only=True,
        tombstone_df=_dv(
            [("full.parquet", rowid) for rowid in dead_rowids]
        ),
        return_residual=True,
    )

    assert considered == len(expected_sunset)
    assert rows == 0
    assert resources == []
    assert sunset == expected_sunset
    assert residual.get_column("__rowid__").to_list() == expected_residual
    assert reads == [("full.parquet", ["__rowid__"], True)]


def test_fused_twenty_file_drain_writes_only_final_exact_packed_outputs(
    monkeypatch,
):
    """Partial-DV survivors go straight into final ~16 MiB files.

    A two-phase implementation would write twenty survivor files, read those
    successors, and then write the final packed files.  The fused contract is
    one read per immutable source and four final writes for this 60 MiB live
    byte estimate, with no intermediate successor ever becoming an input.
    """
    from supertable import processing

    resources, sources = _twenty_mixed_sources()
    source_rows = {path: frame.height for path, frame in sources.items()}
    source_bytes = {
        resource["file"]: resource["file_size"] for resource in resources
    }

    dead_pairs = []
    for path, frame in sources.items():
        dead_pairs.extend(
            (path, rowid)
            for rowid in frame.filter(pl.col("slot").is_in([1, 5, 9]))
            .get_column("__rowid__")
            .to_list()
        )
    tombstones = _dv(dead_pairs)
    dead_ids = set(tombstones.get_column("__rowid__").to_list())

    reads: List[Tuple[str, Tuple[str, ...]]] = []

    def read_source(path, **kwargs):
        # Generated successors are intentionally absent from this mapping: a
        # reread of an intermediate output fails immediately and diagnostically.
        assert path in sources, f"intermediate successor was reread: {path}"
        columns = tuple(kwargs.get("columns") or ())
        reads.append((path, columns))
        frame = sources[path].clone()
        return frame.select(columns) if columns else frame

    outputs: Dict[str, pl.DataFrame] = {}
    output_estimates: List[float] = []

    def write_final(**kwargs):
        frame = kwargs["write_df"].clone()
        path = f"final-{len(outputs)}.parquet"
        estimated_bytes = sum(
            source_bytes[f"source-{int(file_index):02d}.parquet"]
            / source_rows[f"source-{int(file_index):02d}.parquet"]
            for file_index in frame.get_column("file_index").to_list()
        )
        outputs[path] = frame
        output_estimates.append(estimated_bytes)
        kwargs["new_resources"].append(
            {
                "file": path,
                "file_size": int(round(estimated_bytes)),
                "rows": frame.height,
            }
        )

    monkeypatch.setattr(processing, "_read_parquet_safe", read_source)
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_final,
    )

    considered, rows, new_resources, sunset, residual = (
        processing.compact_resources(
            snapshot={"resources": resources},
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": TARGET_BYTES},
            small_only=True,
            required_reads=True,
            tombstone_df=tombstones,
            return_residual=True,
        )
    )

    expected = _union(
        frame.filter(~pl.col("__rowid__").is_in(dead_ids))
        for frame in sources.values()
    )
    actual = _union(outputs.values())
    columns = sorted(expected.columns)

    assert considered == 20
    assert rows == expected.height == 20 * 9
    assert sunset == set(sources)
    assert residual.height == 0
    assert len(new_resources) == len(outputs) == 4

    assert actual.columns != []
    assert set(actual.columns) == set(expected.columns)
    assert _row_multiset(actual, columns) == _row_multiset(expected, columns)
    assert _canonical_digest(actual) == _canonical_digest(expected)
    assert actual.get_column("__rowid__").dtype == pl.Int64
    assert actual.get_column("__rowid__").null_count() == 0
    assert actual.get_column("__rowid__").n_unique() == actual.height
    assert set(actual.get_column("__rowid__")) == (
        set(range(1, 20 * 12 + 1)) - dead_ids
    )

    # Exactly one full read of every original, and no projected validation pass
    # followed by a second body read.  Outputs can therefore never be the old
    # per-file DV successors consumed by a later merge phase.
    assert Counter(path for path, _columns in reads) == Counter(
        {path: 1 for path in sources}
    )
    assert all(columns == () for _path, columns in reads)

    live_estimate = 20 * 4 * MIB * (9 / 12)
    assert sum(output_estimates) == pytest.approx(live_estimate)
    assert len(output_estimates) == math.ceil(live_estimate / TARGET_BYTES)

    # The packer estimates compressed bytes proportionally by source rows and
    # may split a survivor frame at a row boundary.  Every non-final output must
    # therefore be within one declared source-row of 16 MiB.  Actual Parquet
    # compression can vary, so the contract is intentionally estimate-based.
    row_tolerance = (4 * MIB) / 12
    for estimate in output_estimates[:-1]:
        assert TARGET_BYTES - row_tolerance <= estimate <= TARGET_BYTES + row_tolerance
    assert 0 < output_estimates[-1] <= TARGET_BYTES + row_tolerance


def test_fused_drain_retains_entire_unprovable_group_under_original_identity(
    monkeypatch,
):
    """One missing rowid retains its whole file group and original source."""
    from supertable import processing
    from supertable.utils.profiler import Profiler

    sources = {
        "good-large.parquet": pl.DataFrame(
            {
                "__rowid__": pl.Series([1, 2, 3], dtype=pl.Int64),
                "kind": ["dead", "live-a", "live-b"],
                "newer": [None, "yes", "yes"],
            }
        ),
        "bad-large.parquet": pl.DataFrame(
            {
                "kind": ["still-logically-dead", "live-c"],
                "__rowid__": pl.Series([10, 11], dtype=pl.Int64),
                "legacy_only": ["old", "old"],
            }
        ),
        "clean-small.parquet": pl.DataFrame(
            {
                "__rowid__": pl.Series([20, 21], dtype=pl.Int64),
                "kind": ["live-d", "live-e"],
            }
        ),
    }
    resources = [
        {"file": "good-large.parquet", "file_size": 101, "rows": 3},
        {"file": "bad-large.parquet", "file_size": 101, "rows": 2},
        {"file": "clean-small.parquet", "file_size": 30, "rows": 2},
    ]
    tombstones = _dv(
        [
            ("good-large.parquet", 1),
            ("bad-large.parquet", 10),
            ("bad-large.parquet", 999),
        ]
    )
    reads = []

    def read_source(path, **kwargs):
        reads.append(path)
        frame = sources[path].clone()
        columns = kwargs.get("columns")
        return frame.select(columns) if columns else frame

    outputs: Dict[str, pl.DataFrame] = {}

    def write_final(**kwargs):
        path = f"final-{len(outputs)}.parquet"
        outputs[path] = kwargs["write_df"].clone()
        kwargs["new_resources"].append(
            {"file": path, "file_size": 97, "rows": outputs[path].height}
        )

    monkeypatch.setattr(processing, "_read_parquet_safe", read_source)
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_final,
    )

    profiler = Profiler()
    considered, rows, _new_resources, sunset, residual = (
        processing.compact_resources(
            snapshot={"resources": resources},
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 100},
            small_only=True,
            required_reads=True,
            tombstone_df=tombstones,
            return_residual=True,
            profiler=profiler,
        )
    )

    assert considered == 2
    assert rows == 4
    assert sunset == {"good-large.parquet", "clean-small.parquet"}
    assert Counter(reads) == Counter({path: 1 for path in sources})

    # A partially matching group is never split into "consumed" and residual
    # entries: doing so would leave rowid 999 naming a source that had already
    # been sunset.  The valid rowid 10 remains logically deleted by the residual.
    assert Counter(residual.rows()) == Counter(
        [("bad-large.parquet", 10), ("bad-large.parquet", 999)]
    )
    assert not residual.filter(pl.col("__file__") == "good-large.parquet").height
    assert not any(
        frame.get_column("__rowid__").is_in([10, 11]).any()
        for frame in outputs.values()
    )

    # Reconstruct the logical snapshot: fused outputs plus the untouched bad
    # source, filtered by the residual that still carries its original identity.
    output_rows = _union(outputs.values())
    untouched_bad = sources["bad-large.parquet"].filter(
        ~pl.col("__rowid__").is_in(
            residual.filter(pl.col("__file__") == "bad-large.parquet")
            .get_column("__rowid__")
            .to_list()
        )
    )
    logical_after = _union([output_rows, untouched_bad])
    logical_expected = _union(
        [
            sources["good-large.parquet"].filter(pl.col("__rowid__") != 1),
            sources["bad-large.parquet"].filter(pl.col("__rowid__") != 10),
            sources["clean-small.parquet"],
        ]
    )
    assert _canonical_digest(logical_after) == _canonical_digest(logical_expected)

    counts = profiler.emit_counts()
    assert counts["compact_candidates_total"] == 3
    assert counts["compact_small_candidates"] == 1
    assert counts["compact_large_tombstone_candidates"] == 2
    assert counts["compact_files_consumed_total"] == 2
    assert counts["compact_small_files_consumed"] == 1
    assert counts["compact_large_tombstone_files_consumed"] == 1
    # Capacity is configuration; observed concurrency is a separate fact.
    assert counts["compact_encode_worker_capacity"] == 2
    assert counts["compact_encode_workers"] == 1
    assert counts["compact_encode_calls"] == 1


def test_fused_precommit_failure_cleans_only_outputs_minted_by_that_call(
    monkeypatch,
):
    """A later final-output failure removes earlier unpublished outputs."""
    from supertable import processing

    sources: Dict[str, pl.DataFrame] = {}
    resources = []
    dead_pairs = []
    for file_index in range(6):
        path = f"source-{file_index}.parquet"
        first = file_index * 4 + 1
        frame = pl.DataFrame(
            {
                "__rowid__": pl.Series(range(first, first + 4), dtype=pl.Int64),
                "file_index": [file_index] * 4,
                "value": [f"v-{file_index}-{slot}" for slot in range(4)],
            }
        )
        sources[path] = frame
        resources.append(
            {"file": path, "file_size": 768 * 1024, "rows": frame.height}
        )
        dead_pairs.append((path, first))

    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda path, **_kwargs: sources[path].clone(),
    )

    write_calls = 0
    minted = []

    def fail_second_final_write(**kwargs):
        nonlocal write_calls
        write_calls += 1
        if write_calls == 2:
            raise RuntimeError("injected final parquet failure")
        path = f"data/final-{write_calls}.parquet"
        minted.append(path)
        kwargs["footer_md_out"][path] = f"footer-{write_calls}"
        kwargs["new_resources"].append(
            {"file": path, "file_size": MIB, "rows": kwargs["write_df"].height}
        )

    deleted = []
    caller_footer_cache = {"already-published.parquet": "existing-footer"}
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", fail_second_final_write,
    )
    monkeypatch.setattr(
        processing, "_get_storage", lambda: SimpleNamespace(delete=deleted.append),
    )

    with pytest.raises(RuntimeError, match="injected final parquet failure"):
        processing.compact_resources(
            snapshot={"resources": resources},
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": MIB},
            small_only=True,
            required_reads=True,
            tombstone_df=_dv(dead_pairs),
            return_residual=True,
            footer_md_out=caller_footer_cache,
        )

    assert write_calls == 2
    assert minted == ["data/final-1.parquet"]
    assert deleted == minted
    assert not set(deleted).intersection(sources)
    assert caller_footer_cache == {
        "already-published.parquet": "existing-footer"
    }


def test_ordinary_compaction_failure_cleans_every_minted_output_in_best_effort_mode(
    monkeypatch,
):
    """Maintenance compaction owns its unpublished outputs even when non-strict."""
    from supertable import processing

    source_paths = [f"source-{index}.parquet" for index in range(3)]
    original_error = OSError("third source became unavailable")
    reads = iter(
        [
            pl.DataFrame({"value": ["first"]}),
            pl.DataFrame({"value": ["second"]}),
            original_error,
        ]
    )

    def read_source(*_args, **_kwargs):
        result = next(reads)
        if isinstance(result, BaseException):
            raise result
        return result

    minted = []

    def write_output(**kwargs):
        path = f"data/minted-{len(minted) + 1}.parquet"
        minted.append(path)
        kwargs["new_resources"].append(
            {"file": path, "file_size": 1, "rows": kwargs["write_df"].height}
        )

    deleted = []

    def delete_with_failure(path):
        deleted.append(path)
        # Cleanup is best effort: one failed DELETE must not hide the mutation
        # error or prevent the second exact minted path from being attempted.
        if len(deleted) == 1:
            raise RuntimeError("first cleanup acknowledgement lost")

    monkeypatch.setattr(processing, "_read_parquet_safe", read_source)
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_output,
    )
    monkeypatch.setattr(
        processing,
        "_get_storage",
        lambda: SimpleNamespace(delete=delete_with_failure),
    )

    with pytest.raises(OSError) as caught:
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": path, "file_size": 2, "rows": 1}
                    for path in source_paths
                ]
            },
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 1},
            small_only=False,
            required_reads=False,
        )

    assert caught.value is original_error
    assert minted == ["data/minted-1.parquet", "data/minted-2.parquet"]
    assert deleted == minted
    assert not set(deleted).intersection(source_paths)
    assert "data/preexisting.parquet" not in deleted


def test_ordinary_compaction_rolls_back_output_on_post_write_failure(monkeypatch):
    """The rollback boundary includes post-loop accounting, not only I/O."""
    from supertable import processing

    original_error = RuntimeError("post-write profiler failure")

    class FailingProfiler:
        def add(self, name, _value):
            if name == "compact_files_consumed_total":
                raise original_error

    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *_args, **_kwargs: pl.DataFrame({"value": ["written"]}),
    )
    minted = "data/post-write-minted.parquet"

    def write_output(**kwargs):
        kwargs["new_resources"].append(
            {"file": minted, "file_size": 1, "rows": 1}
        )

    deleted = []
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_output,
    )
    monkeypatch.setattr(
        processing,
        "_get_storage",
        lambda: SimpleNamespace(delete=deleted.append),
    )

    with pytest.raises(RuntimeError) as caught:
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": "source.parquet", "file_size": 1, "rows": 1}
                ]
            },
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 2},
            small_only=False,
            required_reads=False,
            profiler=FailingProfiler(),
        )

    assert caught.value is original_error
    assert deleted == [minted]
    assert "source.parquet" not in deleted


def test_fused_compaction_rolls_back_output_when_footer_publish_fails(monkeypatch):
    """The caller footer cache is published only after all outputs are safe."""
    from supertable import processing

    source = pl.DataFrame(
        {
            "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
            "value": ["dead", "live"],
        }
    )
    monkeypatch.setattr(
        processing, "_read_parquet_safe", lambda *_args, **_kwargs: source,
    )

    minted = "data/fused-footer-minted.parquet"

    def write_output(**kwargs):
        kwargs["new_resources"].append(
            {"file": minted, "file_size": 1, "rows": 1}
        )
        kwargs["footer_md_out"][minted] = "sealed-footer"

    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_output,
    )
    deleted = []
    monkeypatch.setattr(
        processing,
        "_get_storage",
        lambda: SimpleNamespace(delete=deleted.append),
    )

    original_error = RuntimeError("caller footer cache rejected publication")

    class RejectingFooterCache(dict):
        def update(self, *_args, **_kwargs):
            raise original_error

    with pytest.raises(RuntimeError) as caught:
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": "source.parquet", "file_size": 2, "rows": 2}
                ]
            },
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 4},
            tombstone_df=_dv([("source.parquet", 1)]),
            return_residual=True,
            footer_md_out=RejectingFooterCache(),
        )

    assert caught.value is original_error
    assert deleted == [minted]
    assert "source.parquet" not in deleted


def test_parallel_tombstone_failure_harvests_and_cleans_later_worker_outputs(
    monkeypatch,
):
    """An early ordered future cannot hide successors uploaded by later ones."""
    from supertable import processing

    frames = {
        "source-a.parquet": pl.DataFrame(
            {
                "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
                "source": ["a", "a"],
            }
        ),
        "source-b.parquet": pl.DataFrame(
            {
                "__rowid__": pl.Series([3, 4], dtype=pl.Int64),
                "source": ["b", "b"],
            }
        ),
        "source-c.parquet": pl.DataFrame(
            {
                "__rowid__": pl.Series([5, 6], dtype=pl.Int64),
                "source": ["c", "c"],
            }
        ),
    }
    tombstones = _dv(
        [
            ("source-a.parquet", 1),
            ("source-b.parquet", 3),
            ("source-c.parquet", 5),
        ]
    )
    grouped = tombstones.partition_by(
        "__file__", as_dict=True, maintain_order=False,
    )
    ordered_paths = [
        key[0] if isinstance(key, tuple) else key for key in grouped
    ]
    failing_path = ordered_paths[0]
    successful_paths = ordered_paths[1:]
    uploaded = {path: threading.Event() for path in successful_paths}
    original_error = OSError("first ordered source read failed")

    def read_source(path, **_kwargs):
        if path == failing_path:
            assert all(event.wait(5) for event in uploaded.values())
            raise original_error
        return frames[path].clone()

    minted = []

    def write_successor(**kwargs):
        source = kwargs["write_df"].get_column("source").item(0)
        source_path = f"source-{source}.parquet"
        path = f"data/successor-{source}.parquet"
        minted.append(path)
        kwargs["new_resources"].append(
            {"file": path, "file_size": 1, "rows": kwargs["write_df"].height}
        )
        kwargs["footer_md_out"][path] = f"footer-{source}"
        uploaded[source_path].set()

    deleted = []

    def delete_with_failure(path):
        deleted.append(path)
        raise RuntimeError("cleanup acknowledgement lost")

    monkeypatch.setattr(processing, "_read_parquet_safe", read_source)
    monkeypatch.setattr(
        processing, "write_parquet_and_collect_resources", write_successor,
    )
    monkeypatch.setattr(
        processing,
        "_get_storage",
        lambda: SimpleNamespace(delete=delete_with_failure),
    )
    caller_footer_cache = {"preexisting.parquet": "published-footer"}

    with pytest.raises(OSError) as caught:
        processing.compact_tombstones(
            snapshot={
                "resources": [
                    {"file": path, "file_size": 1, "rows": 2}
                    for path in frames
                ]
            },
            tombstone_df=tombstones,
            data_dir="data",
            compression_level=1,
            table_config={"tombstone_compaction_workers": 3},
            return_residual=True,
            footer_md_out=caller_footer_cache,
        )

    expected_minted = {
        f"data/successor-{frames[path].get_column('source').item(0)}.parquet"
        for path in successful_paths
    }
    assert caught.value is original_error
    assert set(minted) == expected_minted
    assert set(deleted) == expected_minted
    assert len(deleted) == len(expected_minted)
    assert not set(deleted).intersection(frames)
    assert "data/preexisting.parquet" not in deleted
    assert caller_footer_cache == {
        "preexisting.parquet": "published-footer"
    }
