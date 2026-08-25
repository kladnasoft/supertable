"""Adversarial write/compaction deletion-vector safety regressions."""
from __future__ import annotations

import os
import base64
import hashlib
import random
import threading
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing
from supertable.data_writer import DataWriter
from supertable.utils.profiler import Profiler


def _dv(pairs):
    return pl.DataFrame(
        {
            processing.TOMBSTONE_FILE_COL: [file for file, _ in pairs],
            processing.ROWID_COL: [rowid for _, rowid in pairs],
        },
        schema=processing.TOMBSTONE_SCHEMA,
    )


def test_required_parquet_not_found_fails_closed(monkeypatch):
    monkeypatch.setattr(processing, "_safe_exists", lambda *a, **k: False)
    with pytest.raises(FileNotFoundError, match="Required parquet"):
        processing._read_parquet_safe("missing.parquet", required=True)
    assert processing._read_parquet_safe("missing.parquet", required=False) is None


@pytest.mark.parametrize(
    "frame,match",
    [
        (pl.DataFrame({"__file__": ["f"]}), "invalid columns"),
        (_dv([("f", 1)]).with_columns(pl.lit(None).cast(pl.Int64).alias("__rowid__")), "NULL"),
        (_dv([("f", 1), ("f", 1)]), "duplicate"),
        (_dv([("a", 1), ("b", 1)]), "reuses a rowid"),
        (_dv([("f", 0)]), "non-positive"),
    ],
)
def test_tombstone_validation_rejects_malformed_frames(frame, match):
    with pytest.raises(ValueError, match=match):
        processing.validate_tombstone_frame(frame)


def test_tombstone_validation_seals_count_and_resource_membership():
    frame = _dv([("current.parquet", 7)])
    with pytest.raises(ValueError, match="row-count mismatch"):
        processing.validate_tombstone_frame(frame, expected_rows=2)
    with pytest.raises(ValueError, match="outside the current snapshot"):
        processing.validate_tombstone_frame(
            frame, allowed_files={"other.parquet"}
        )


def test_tombstone_digest_is_canonical_and_detects_same_count_substitution():
    frame = _dv([("z/ü.parquet", 9), ("a.parquet", 2)])
    records = sorted([
        (base64.b64encode("z/ü.parquet".encode()).decode("ascii"), 9),
        (base64.b64encode(b"a.parquet").decode("ascii"), 2),
    ])
    payload = b"supertable-tombstone-v1\n" + b"\n".join(
        f"{name}:{rowid:016x}".encode("ascii") for name, rowid in records
    )
    expected = hashlib.sha256(payload).hexdigest()
    assert processing.tombstone_digest(frame.reverse()) == expected
    processing.validate_tombstone_frame(frame, expected_digest=expected)

    substituted = _dv([("z/ü.parquet", 10), ("a.parquet", 2)])
    with pytest.raises(ValueError, match="digest mismatch"):
        processing.validate_tombstone_frame(
            substituted, expected_rows=2, expected_digest=expected
        )


def test_tombstone_digest_updates_once_per_exact_canonical_record(monkeypatch):
    frame = _dv([
        ("z/ü.parquet", 9),
        ("a.parquet", 11),
        ("a.parquet", 2),
    ])
    records = sorted([
        (base64.b64encode(file.encode("utf-8")).decode("ascii"), rowid)
        for file, rowid in frame.iter_rows()
    ])
    record_bytes = [
        (("" if index == 0 else "\n") + f"{name}:{rowid:016x}").encode(
            "ascii"
        )
        for index, (name, rowid) in enumerate(records)
    ]
    payload = b"supertable-tombstone-v1\n" + b"".join(record_bytes)

    real_sha256 = hashlib.sha256
    updates = []

    class RecordingDigest:
        def __init__(self, initial=b""):
            self._digest = real_sha256(initial)

        def update(self, value):
            updates.append(value)
            self._digest.update(value)

        def hexdigest(self):
            return self._digest.hexdigest()

    monkeypatch.setattr(processing.hashlib, "sha256", RecordingDigest)

    assert processing.tombstone_digest(frame) == real_sha256(payload).hexdigest()
    assert updates == record_bytes


def test_residual_file_set_deduplicates_before_python_materialization(monkeypatch):
    frame = _dv([
        ("many.parquet", 1),
        ("many.parquet", 2),
        ("other.parquet", 3),
        ("many.parquet", 4),
    ])
    materialized_lengths = []
    original_to_list = pl.Series.to_list

    def tracking_to_list(series, *args, **kwargs):
        if series.name == processing.TOMBSTONE_FILE_COL:
            materialized_lengths.append(series.len())
        return original_to_list(series, *args, **kwargs)

    monkeypatch.setattr(pl.Series, "to_list", tracking_to_list)

    assert DataWriter._tombstone_referenced_files(frame) == {
        "many.parquet", "other.parquet",
    }
    assert materialized_lengths == [2]


def test_active_tombstone_metadata_must_be_counted_and_digest_sealed():
    with pytest.raises(ValueError, match="tombstone_rows"):
        DataWriter._declared_tombstone_rows({"tombstone": "dv.parquet"})
    with pytest.raises(ValueError, match="tombstone_digest"):
        DataWriter._declared_tombstone_digest({
            "tombstone": "dv.parquet", "tombstone_rows": 1,
        })
    with pytest.raises(ValueError, match="without a deletion-vector pointer"):
        DataWriter._declared_tombstone_rows({"tombstone_rows": 1})
    with pytest.raises(ValueError, match="without a deletion-vector pointer"):
        DataWriter._declared_tombstone_digest({"tombstone_digest": "a" * 64})


def test_conflicting_compaction_schema_aborts_instead_of_nulling_value(monkeypatch):
    sources = {
        "a.parquet": pl.DataFrame({"value": [1]}),
        "b.parquet": pl.DataFrame({"value": [b"\xff"]}),
    }
    monkeypatch.setattr(
        processing, "_read_parquet_safe", lambda path, **kwargs: sources[path]
    )
    writes = []
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **kwargs: writes.append(kwargs["write_df"]),
    )
    with pytest.raises(Exception):
        processing.compact_resources(
            snapshot={"resources": [
                {"file": "a.parquet", "file_size": 1},
                {"file": "b.parquet", "file_size": 1},
            ]},
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 100},
            small_only=False,
            required_reads=True,
        )
    assert writes == []


def test_compaction_rejects_valid_but_lossy_numeric_cast():
    source = pl.DataFrame({"value": [2**53 + 1]}, schema={"value": pl.Int64})
    with pytest.raises(ValueError, match="would change values"):
        processing._align_to_schema(source, {"value": pl.Float64})


def test_partial_drain_returns_entire_group_as_residual(monkeypatch):
    frame = pl.DataFrame({"__rowid__": [1, 2], "value": ["a", "b"]})
    monkeypatch.setattr(processing, "_read_parquet_safe", lambda *a, **k: frame)
    writes = []
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **kwargs: writes.append(kwargs),
    )

    removed, resources, sunset, residual = processing.compact_tombstones(
        snapshot={"resources": [{"file": "f.parquet", "rows": 2}]},
        tombstone_df=_dv([("f.parquet", 1), ("f.parquet", 999)]),
        data_dir="data",
        compression_level=1,
        return_residual=True,
    )

    assert (removed, resources, sunset) == (0, [], set())
    assert residual.rows() == [("f.parquet", 1), ("f.parquet", 999)]
    assert writes == []


def test_fully_dead_drain_reads_only_rowid_and_writes_nothing(monkeypatch):
    calls = []

    def read(path, **kwargs):
        calls.append(kwargs.get("columns"))
        return pl.DataFrame({"__rowid__": pl.Series([1, 2], dtype=pl.Int64)})

    monkeypatch.setattr(processing, "_read_parquet_safe", read)
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **kwargs: pytest.fail("fully-dead file was rewritten"),
    )
    removed, resources, sunset, residual = processing.compact_tombstones(
        snapshot={"resources": [{"file": "f.parquet", "rows": 2}]},
        tombstone_df=_dv([("f.parquet", 1), ("f.parquet", 2)]),
        data_dir="data",
        compression_level=1,
        return_residual=True,
    )
    assert (removed, resources, sunset, residual.height) == (
        2, [], {"f.parquet"}, 0
    )
    assert calls == [["__rowid__"]]


def test_tombstone_files_drain_with_bounded_parallel_workers(monkeypatch):
    barrier = threading.Barrier(2, timeout=5)
    thread_ids = set()

    def read(path, **kwargs):
        thread_ids.add(threading.get_ident())
        barrier.wait()
        base = 1 if path == "a.parquet" else 3
        return pl.DataFrame({
            "__rowid__": pl.Series([base, base + 1], dtype=pl.Int64),
            "value": ["dead", "live"],
        })

    monkeypatch.setattr(processing, "_read_parquet_safe", read)

    def write(**kwargs):
        kwargs["new_resources"].append({
            "file": f"new-{threading.get_ident()}.parquet",
            "rows": kwargs["write_df"].height,
        })

    monkeypatch.setattr(processing, "write_parquet_and_collect_resources", write)
    removed, resources, sunset, residual = processing.compact_tombstones(
        snapshot={"resources": [
            {"file": "a.parquet", "rows": 2},
            {"file": "b.parquet", "rows": 2},
        ]},
        tombstone_df=_dv([("a.parquet", 1), ("b.parquet", 3)]),
        data_dir="data",
        compression_level=1,
        table_config={"tombstone_compaction_workers": 2},
        return_residual=True,
    )
    assert removed == 2
    assert len(resources) == 2
    assert sunset == {"a.parquet", "b.parquet"}
    assert residual.height == 0
    assert len(thread_ids) == 2


def test_parallel_tombstone_writes_publish_footer_cache_in_parent(monkeypatch):
    """Worker-local footer maps are merged without concurrent shared writes."""
    barrier = threading.Barrier(2, timeout=5)

    def read(path, **_kwargs):
        barrier.wait()
        base = 1 if path == "a.parquet" else 3
        return pl.DataFrame({
            "__rowid__": pl.Series([base, base + 1], dtype=pl.Int64),
            "value": ["dead", "live"],
        })

    def write(**kwargs):
        path = f"new-{kwargs['write_df'].get_column('__rowid__')[0]}.parquet"
        kwargs["new_resources"].append({"file": path, "rows": 1})
        kwargs["footer_md_out"][path] = f"footer-{path}"

    monkeypatch.setattr(processing, "_read_parquet_safe", read)
    monkeypatch.setattr(processing, "write_parquet_and_collect_resources", write)
    cache = {}

    removed, resources, sunset, residual = processing.compact_tombstones(
        snapshot={"resources": [
            {"file": "a.parquet", "rows": 2},
            {"file": "b.parquet", "rows": 2},
        ]},
        tombstone_df=_dv([("a.parquet", 1), ("b.parquet", 3)]),
        data_dir="data",
        compression_level=1,
        table_config={"tombstone_compaction_workers": 2},
        return_residual=True,
        footer_md_out=cache,
    )

    assert removed == 2
    assert len(resources) == 2
    assert sunset == {"a.parquet", "b.parquet"}
    assert residual.height == 0
    assert set(cache) == {resource["file"] for resource in resources}


def test_randomized_threshold_drain_matches_logical_delete_model(monkeypatch):
    """Every consumed DV entry removes exactly its physical row, never more."""
    rng = random.Random(20260812)

    for seed in range(50):
        sources = {}
        resources = []
        all_rows = []
        next_rowid = 1
        for file_index in range(rng.randint(1, 6)):
            path = f"seed-{seed}-file-{file_index}.parquet"
            row_count = rng.randint(1, 10)
            rowids = list(range(next_rowid, next_rowid + row_count))
            next_rowid += row_count
            frame = pl.DataFrame({
                "__rowid__": pl.Series(rowids, dtype=pl.Int64),
                "value": [f"v-{rowid}" for rowid in rowids],
            })
            sources[path] = frame
            resources.append({"file": path, "rows": row_count, "file_size": 1})
            all_rows.extend((path, rowid, f"v-{rowid}") for rowid in rowids)

        dead_pairs = [
            (path, rowid)
            for path, rowid, _value in all_rows
            if rng.random() < 0.45
        ]
        if not dead_pairs:
            path, rowid, _value = rng.choice(all_rows)
            dead_pairs = [(path, rowid)]
        rng.shuffle(dead_pairs)
        dead = set(dead_pairs)

        def read(path, **kwargs):
            frame = sources[path]
            columns = kwargs.get("columns")
            return frame.select(columns) if columns else frame.clone()

        written = {}

        def write(**kwargs):
            path = f"replacement-{seed}-{len(written)}.parquet"
            frame = kwargs["write_df"].clone()
            written[path] = frame
            kwargs["new_resources"].append({
                "file": path,
                "rows": frame.height,
                "file_size": 1,
            })

        monkeypatch.setattr(processing, "_read_parquet_safe", read)
        monkeypatch.setattr(
            processing, "write_parquet_and_collect_resources", write,
        )
        removed, new_resources, sunset, residual = processing.compact_tombstones(
            snapshot={"resources": resources},
            tombstone_df=_dv(dead_pairs),
            data_dir="data",
            compression_level=1,
            table_config={"tombstone_compaction_workers": 1},
            return_residual=True,
        )

        touched = {path for path, _rowid in dead}
        actual = []
        for path, frame in sources.items():
            if path not in sunset:
                actual.extend(frame.select(["__rowid__", "value"]).iter_rows())
        for resource in new_resources:
            actual.extend(
                written[resource["file"]]
                .select(["__rowid__", "value"])
                .iter_rows()
            )
        expected = [
            (rowid, value)
            for path, rowid, value in all_rows
            if (path, rowid) not in dead
        ]

        assert removed == len(dead)
        assert sunset == touched
        assert residual.height == 0
        assert sorted(actual) == sorted(expected)


def test_reclaim_requires_physical_rowid_coverage(monkeypatch):
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *a, **k: pl.DataFrame({"__rowid__": [1, 2, 3]}),
    )
    result = processing.reclaim_fully_dead_files(
        resources=[{"file": "f.parquet", "rows": 2, "file_size": 10}],
        combined_dv=_dv([("f.parquet", 1), ("f.parquet", 2)]),
        tombstone_dir="dv",
        compression_level=1,
    )
    assert result == (set(), None, None)


def test_reclaim_does_not_sunset_when_dv_has_extra_ghost_rowid(monkeypatch):
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *a, **k: pl.DataFrame(
            {"__rowid__": pl.Series([10], dtype=pl.Int64)}
        ),
    )
    result = processing.reclaim_fully_dead_files(
        resources=[{"file": "f.parquet", "rows": 1, "file_size": 10}],
        combined_dv=_dv([("f.parquet", 10), ("f.parquet", 20)]),
        tombstone_dir="dv",
        compression_level=1,
    )
    assert result == (set(), None, None)


def test_reclaim_counts_v2_nonpersisted_dead_files(monkeypatch):
    profiler = processing.Profiler()
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *a, **k: pl.DataFrame({
            "__rowid__": pl.Series([10], dtype=pl.Int64),
        }),
    )

    result = processing.reclaim_fully_dead_files(
        resources=[{"file": "f.parquet", "rows": 1, "file_size": 10}],
        combined_dv=_dv([
            ("f.parquet", 10),
            ("survivor.parquet", 20),
        ]),
        tombstone_dir="dv",
        compression_level=1,
        profiler=profiler,
        persist=False,
    )

    assert result[0] == {"f.parquet"}
    assert result[1] is None
    assert result[2].to_dict(as_series=False) == {
        "__file__": ["survivor.parquet"],
        "__rowid__": [20],
    }
    assert profiler.counts["reclaimed_dead_files"] == 1


def test_compaction_rejects_duplicate_physical_rowids(monkeypatch):
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *a, **k: pl.DataFrame({
            "__rowid__": pl.Series([7, 7], dtype=pl.Int64),
            "value": ["one", "two"],
        }),
    )
    with pytest.raises(ValueError, match="duplicate rowids"):
        processing.compact_tombstones(
            snapshot={"resources": [{"file": "f.parquet", "rows": 2}]},
            tombstone_df=_dv([("f.parquet", 7)]),
            data_dir="data",
            compression_level=1,
            return_residual=True,
        )


@pytest.mark.parametrize(
    "rowids,match",
    [
        (pl.Series([7, 7], dtype=pl.Int64), "duplicate rowids"),
        (pl.Series([0, 8], dtype=pl.Int64), "non-positive"),
        (pl.Series([7, None], dtype=pl.Int64), "NULL rowids"),
        (pl.Series([7, 8], dtype=pl.Int32), "non-Int64"),
    ],
)
def test_targeted_delete_rejects_ambiguous_source_rowids(
        monkeypatch, rowids, match,
):
    """One matching key may not emit a DV that hides another physical row."""
    source = pl.DataFrame({
        "id": [1, 2],
        "__rowid__": rowids,
    })
    monkeypatch.setattr(
        processing, "_read_parquet_safe", lambda *a, **k: source,
    )

    with pytest.raises(ValueError, match=match):
        processing.resolve_overwrite_writes(
            incoming_df=pl.DataFrame({"id": [1]}),
            overlapping_files={("legacy.parquet", True, 1)},
            overwrite_columns=["id"],
            required=True,
        )


def test_small_file_compaction_never_collapses_duplicate_legacy_rowids(
        monkeypatch,
):
    sources = {
        "a.parquet": pl.DataFrame({"__rowid__": [7], "id": [1]}),
        "b.parquet": pl.DataFrame({"__rowid__": [7], "id": [2]}),
    }
    monkeypatch.setattr(
        processing, "_read_parquet_safe",
        lambda path, **kwargs: sources[path],
    )
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **kwargs: pytest.fail("ambiguous compacted file was written"),
    )

    with pytest.raises(ValueError, match="over-delete live rows"):
        processing.compact_resources(
            snapshot={"resources": [
                {"file": "a.parquet", "file_size": 1},
                {"file": "b.parquet", "file_size": 1},
            ]},
            data_dir="data",
            compression_level=1,
            table_config={"max_memory_chunk_size": 100},
            small_only=False,
        )


def test_remote_upload_failure_never_falls_back_to_local(tmp_path, monkeypatch):
    class BrokenStorage:
        def write_bytes(self, path, data):
            raise OSError("remote PUT failed")

    monkeypatch.setattr(processing, "_get_storage", lambda: BrokenStorage())
    target = tmp_path / "should-not-exist.parquet"
    with pytest.raises(OSError, match="remote PUT failed"):
        processing._write_df_parquet(
            pl.DataFrame({"x": [1]}), str(target), 1
        )
    assert not target.exists()


def test_logical_materialization_requires_rowid_when_dv_exists(monkeypatch):
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda *a, **k: pl.DataFrame({"value": ["would leak"]}),
    )
    with pytest.raises(ValueError, match="missing canonical '__rowid__'"):
        processing.compact_resources(
            snapshot={"resources": [{"file": "legacy.parquet", "file_size": 1}]},
            data_dir="export",
            compression_level=1,
            small_only=False,
            dead_rowids={1},
            required_reads=True,
        )


def test_required_materialization_cleans_only_outputs_from_failed_call(monkeypatch):
    reads = iter([
        pl.DataFrame({"__rowid__": [1], "value": ["first"]}),
        OSError("second source unavailable"),
    ])

    def read(*args, **kwargs):
        value = next(reads)
        if isinstance(value, Exception):
            raise value
        return value

    deleted = []
    storage = SimpleNamespace(delete=deleted.append)
    monkeypatch.setattr(processing, "_read_parquet_safe", read)
    monkeypatch.setattr(processing, "_get_storage", lambda: storage)

    def write(**kwargs):
        kwargs["new_resources"].append({"file": "export/new-1.parquet"})

    monkeypatch.setattr(processing, "write_parquet_and_collect_resources", write)
    with pytest.raises(OSError, match="second source unavailable"):
        processing.compact_resources(
            snapshot={
                "resources": [
                    {"file": "source-1.parquet", "file_size": 2},
                    {"file": "source-2.parquet", "file_size": 2},
                ]
            },
            data_dir="export",
            compression_level=1,
            table_config={"max_memory_chunk_size": 1},
            small_only=False,
            required_reads=True,
        )
    assert deleted == ["export/new-1.parquet"]


def test_materialization_applies_deletes_by_composite_file_identity(monkeypatch):
    sources = {
        "a.parquet": pl.DataFrame({"__rowid__": [1], "value": ["dead-a"]}),
        "b.parquet": pl.DataFrame({"__rowid__": [1], "value": ["live-b"]}),
    }
    monkeypatch.setattr(
        processing,
        "_read_parquet_safe",
        lambda path, **kwargs: sources[path],
    )
    outputs = []
    monkeypatch.setattr(
        processing,
        "write_parquet_and_collect_resources",
        lambda **kwargs: outputs.append(kwargs["write_df"].clone()),
    )
    processing.compact_resources(
        snapshot={
            "resources": [
                {"file": "a.parquet", "file_size": 1},
                {"file": "b.parquet", "file_size": 1},
            ]
        },
        data_dir="export",
        compression_level=1,
        table_config={"max_memory_chunk_size": 10},
        small_only=False,
        dead_rowids_by_file={"a.parquet": {1}},
        required_reads=True,
    )
    result = pl.concat(outputs, how="vertical_relaxed")
    assert result.select("value").to_series().to_list() == ["live-b"]


def test_partitioned_versions_share_one_cache_key():
    key = processing._PathKeyedFrameCache._key
    assert key("t/dv/year=2026/month=08/day=12/hour=10/a.parquet") == "t/dv"
    assert key("t/dv/year=2026/month=08/day=13/hour=11/b.parquet") == "t/dv"
    assert key("t/dv/legacy.parquet") == "t/dv"


def test_tombstone_lru_enforces_estimated_byte_budget():
    first = pl.DataFrame({"payload": ["a" * 128]})
    second = pl.DataFrame({"payload": ["b" * 128]})
    budget = first.estimated_size() + second.estimated_size() - 1
    cache = processing._PathKeyedFrameCache(lambda: 10, lambda: budget)
    cache.put("table-a/dv/a.parquet", first)
    cache.put("table-b/dv/b.parquet", second)

    assert cache.get("table-a/dv/a.parquet") is None
    assert cache.get("table-b/dv/b.parquet") is second
    assert cache._bytes <= budget
    cache.clear()
    assert cache._bytes == 0


@pytest.mark.parametrize(
    "column",
    [
        "__rowid__", "__ROWID__", "__Timestamp__", "__FILE__",
        "__SUPERTABLE_SOURCE_FILE__", "__supertable_scan_filename__",
    ],
)
def test_writer_rejects_casefolded_reserved_columns(column):
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(super_name="super")
    with pytest.raises(ValueError, match="reserved system"):
        writer.validation(pl.DataFrame({column: [1]}), "table", [], None, False)


def test_snapshot_rowid_reservation_honors_durable_floor():
    class Catalog:
        def reserve_rowids_at_least(
                self, org, sup, simple, count, floor, *, lock_token,
        ):
            assert (org, sup, simple, count, floor) == ("o", "s", "t", 2, 10)
            assert lock_token == "token"
            return 11, 12

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = Catalog()
    assert writer._reserve_snapshot_rowids(
        snapshot={"rowid_high_watermark": 10},
        simple_name="t",
        count=2,
        profiler=Profiler(),
        lock_token="token",
    ) == (11, 12)


@pytest.mark.parametrize("invalid_floor", [True, 10.5, "10"])
def test_snapshot_rowid_reservation_rejects_noninteger_floor(invalid_floor):
    class Catalog:
        def reserve_rowids_at_least(self, *_args, **_kwargs):
            pytest.fail("invalid floor reached the catalog allocator")

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = Catalog()

    with pytest.raises(ValueError, match="invalid rowid_high_watermark"):
        writer._reserve_snapshot_rowids(
            snapshot={"rowid_high_watermark": invalid_floor},
            simple_name="t",
            count=1,
            profiler=Profiler(),
            lock_token="token",
        )


def test_legacy_rowid_reservation_without_atomic_floor_is_rejected():
    class Catalog:
        def reserve_rowids(self, org, sup, simple, count):
            return 5

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = Catalog()
    with pytest.raises(RuntimeError, match="floor-fenced rowid reservation"):
        writer._reserve_snapshot_rowids(
            snapshot={"rowid_high_watermark": 10},
            simple_name="t",
            count=2,
            profiler=Profiler(),
            lock_token="token",
        )


def test_legacy_rowid_reservation_is_rejected_even_if_return_is_above_floor():
    class Catalog:
        def reserve_rowids(self, org, sup, simple, count):
            return 1_000

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = Catalog()
    with pytest.raises(RuntimeError, match="floor-fenced rowid reservation"):
        writer._reserve_snapshot_rowids(
            snapshot={"resources": [{"file": "legacy.parquet"}]},
            simple_name="t",
            count=1,
            profiler=Profiler(),
            lock_token="token",
        )


def test_legacy_delete_only_does_not_scan_to_derive_rowid_floor(monkeypatch):
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = SimpleNamespace()
    monkeypatch.setattr(
        writer,
        "_derive_legacy_rowid_high_watermark",
        lambda *a, **k: pytest.fail("delete-only performed legacy full scan"),
    )
    assert writer._reserve_snapshot_rowids(
        snapshot={"resources": [{"file": "large.parquet"}]},
        simple_name="t",
        count=0,
        profiler=Profiler(),
        lock_token="token",
    ) == (0, None)


def test_legacy_targeted_delete_derives_and_carries_rowid_floor(monkeypatch):
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = SimpleNamespace()
    calls = []
    def derive(*args, **kwargs):
        calls.append((args, kwargs))
        return 91
    monkeypatch.setattr(writer, "_derive_legacy_rowid_high_watermark", derive)

    assert writer._reserve_snapshot_rowids(
        snapshot={"resources": [{"file": "legacy.parquet"}]},
        simple_name="t",
        count=0,
        profiler=Profiler(),
        lock_token="token",
        require_floor=True,
    ) == (0, 91)
    assert len(calls) == 1


def test_legacy_highwater_rejects_nonpositive_physical_rowid(monkeypatch):
    writer = DataWriter.__new__(DataWriter)
    storage = SimpleNamespace(
        iter_parquet_batches=lambda *_args, **_kwargs: iter((
            pa.table({"__rowid__": pa.array([0, 2], type=pa.int64())}),
        )),
    )
    writer.super_table = SimpleNamespace(
        organization="o", super_name="s", storage=storage,
    )
    with pytest.raises(ValueError, match="positive"):
        writer._derive_legacy_rowid_high_watermark(
            {"resources": [{"file": "f.parquet", "file_size": 1, "rows": 2}]},
            simple_name="t",
            profiler=Profiler(),
        )


def test_legacy_highwater_uses_bounded_disk_backed_global_proof(monkeypatch):
    writer = DataWriter.__new__(DataWriter)
    calls = []
    rows_per_file = 50_000

    class StreamingStorage:
        def iter_parquet_batches(
            self, path, *, max_decoded_bytes, columns,
        ):
            calls.append((path, max_decoded_bytes, columns))
            offset = 0 if path == "a.parquet" else rows_per_file
            for start in range(0, rows_per_file, 5_000):
                yield pa.table({
                    "__rowid__": pa.array(
                        range(offset + start + 1, offset + start + 5_001),
                        type=pa.int64(),
                    ),
                })

    writer.super_table = SimpleNamespace(
        organization="o",
        super_name="s",
        storage=StreamingStorage(),
    )
    monkeypatch.setattr(
        "supertable.data_writer._read_parquet_safe",
        lambda *_args, **_kwargs: pytest.fail(
            "legacy rowid proof materialized an entire Parquet resource"
        ),
    )

    assert writer._derive_legacy_rowid_high_watermark(
        {
            "resources": [
                {"file": "a.parquet", "file_size": 1, "rows": rows_per_file},
                {"file": "b.parquet", "file_size": 1, "rows": rows_per_file},
            ],
        },
        simple_name="t",
        profiler=Profiler(),
    ) == rows_per_file * 2
    assert calls == [
        ("a.parquet", 8 * 1024 * 1024, ["__rowid__"]),
        ("b.parquet", 8 * 1024 * 1024, ["__rowid__"]),
    ]


def test_legacy_highwater_rejects_duplicate_across_bounded_batches():
    writer = DataWriter.__new__(DataWriter)

    class DuplicateStorage:
        def iter_parquet_batches(self, *_args, **_kwargs):
            yield pa.table({"__rowid__": pa.array([1, 2], type=pa.int64())})
            yield pa.table({"__rowid__": pa.array([2, 3], type=pa.int64())})

    writer.super_table = SimpleNamespace(
        organization="o",
        super_name="s",
        storage=DuplicateStorage(),
    )
    with pytest.raises(ValueError, match="table-global unique"):
        writer._derive_legacy_rowid_high_watermark(
            {"resources": [{"file": "f.parquet", "file_size": 1, "rows": 4}]},
            simple_name="t",
            profiler=Profiler(),
        )


def test_mirror_configuration_failure_aborts_mutation():
    class Catalog:
        def get_mirrors(self, org, sup):
            raise OSError("redis unavailable")

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="o", super_name="s")
    writer.catalog = Catalog()
    with pytest.raises(RuntimeError, match="enabled mirrors"):
        writer._get_enabled_mirrors("test mutation")


def test_deferred_tombstone_build_does_not_upload(monkeypatch):
    monkeypatch.setattr(
        processing,
        "_write_df_parquet",
        lambda *a, **k: pytest.fail("deferred artifact was uploaded"),
    )
    path, frame = processing.build_tombstone_file(
        tombstone_dir="dv",
        prev_tombstone_path=None,
        new_pairs=[("f.parquet", 1)],
        compression_level=1,
        persist=False,
    )
    assert path is None
    assert frame.rows() == [("f.parquet", 1)]


def test_tombstone_successor_validates_once_and_hands_off_digest(monkeypatch):
    previous = _dv([("old.parquet", 1)])
    validations = 0
    digests = 0
    real_validate = processing.validate_tombstone_frame
    real_digest = processing.tombstone_digest

    def counted_validate(*args, **kwargs):
        nonlocal validations
        validations += 1
        return real_validate(*args, **kwargs)

    def counted_digest(*args, **kwargs):
        nonlocal digests
        digests += 1
        return real_digest(*args, **kwargs)

    monkeypatch.setattr(processing, "validate_tombstone_frame", counted_validate)
    monkeypatch.setattr(processing, "tombstone_digest", counted_digest)
    validation_out = {}
    path, frame = processing.build_tombstone_file(
        tombstone_dir="dv",
        prev_tombstone_path="dv/previous.parquet",
        new_pairs=[("new.parquet", 2)],
        compression_level=1,
        prev_df=previous,
        persist=False,
        prev_df_validated=True,
        validation_out=validation_out,
    )

    assert path is None
    assert validations == 1  # exact combined successor only; not previous again
    assert digests == 1
    assert validation_out["frame"] is frame
    assert validation_out["digest"] == real_digest(frame, assume_valid=True)


def test_validated_previous_flag_does_not_skip_successor_integrity(monkeypatch):
    previous = _dv([("old.parquet", 1)])
    with pytest.raises(ValueError, match="reuses a rowid"):
        processing.build_tombstone_file(
            tombstone_dir="dv",
            prev_tombstone_path="dv/previous.parquet",
            new_pairs=[("new.parquet", 1)],
            compression_level=1,
            prev_df=previous,
            persist=False,
            prev_df_validated=True,
        )


def test_incomplete_same_height_stats_cannot_hide_overwrite_candidate(
    tmp_path, monkeypatch,
):
    from supertable.storage.local_storage import LocalStorage

    path = str(tmp_path / "two-groups.parquet")
    pl.DataFrame({"__rowid__": [1, 100], "key": [1, 100]}).write_parquet(
        path, row_group_size=1, statistics=True
    )
    monkeypatch.setattr(processing, "_get_storage", lambda: LocalStorage(str(tmp_path)))
    healthy = processing.extract_stats_rows([path])
    key_rows = healthy.filter(pl.col("column_name") == "key").sort("row_group_id")
    assert key_rows.height == 2

    # Preserve artifact height while replacing f's RG1 slot with a foreign row.
    foreign = key_rows.row(0, named=True)
    foreign["file_path"] = "replacement.parquet"
    corrupt = pl.concat([
        key_rows.head(1),
        pl.DataFrame([foreign], schema=processing.STATS_SCHEMA),
    ])
    trusted = processing.stats_for_complete_files(corrupt, {path: 2})
    candidates = {(path, True, os.path.getsize(path))}
    survivors = processing.prune_overlapping_files_by_stats(
        candidates, trusted, {"key": ("bigint", 100, 100)}
    )
    assert survivors == candidates

    incoming = pl.DataFrame({"key": [100]})
    filtered, pairs = processing.resolve_overwrite_writes(
        incoming, survivors, ["key"], required=True
    )
    assert filtered.rows() == [(100,)]
    assert pairs == [(path, 100)]


def test_date_vs_datetime_write_stats_mismatch_retains_candidate():
    incoming = pl.DataFrame({"key": [date(2026, 1, 1)]})
    probe = processing.probe_ranges_from_df(incoming, ["key"])
    assert probe["key"][3] == "date"

    row = {name: None for name in processing.STATS_SCHEMA}
    row.update({
        "file_path": "datetime.parquet",
        "row_group_id": 0,
        "column_name": "key",
        "physical_type": "INT64",
        "logical_type": "TIMESTAMP_NTZ_MICROS",
        "min_timestamp": datetime(2030, 1, 1),
        "max_timestamp": datetime(2030, 1, 1),
        "null_count": 0,
        "row_group_rows": 1,
        "compressed_bytes": 8,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    candidates = {("datetime.parquet", True, 1)}
    assert processing.prune_overlapping_files_by_stats(
        candidates,
        pl.DataFrame([row], schema=processing.STATS_SCHEMA),
        probe,
    ) == candidates


def test_float_width_mismatch_write_stats_retains_candidate():
    incoming = pl.DataFrame({
        "key": pl.Series([100.0], dtype=pl.Float64),
    })
    row = {name: None for name in processing.STATS_SCHEMA}
    row.update({
        "file_path": "float32.parquet",
        "row_group_id": 0,
        "column_name": "key",
        "physical_type": "FLOAT",
        "logical_type": "",
        "min_double": 1.0,
        "max_double": 1.0,
        "null_count": 0,
        "row_group_rows": 1,
        "compressed_bytes": 8,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    candidates = {("float32.parquet", True, 1)}
    assert processing.prune_overlapping_files_by_stats(
        candidates,
        pl.DataFrame([row], schema=processing.STATS_SCHEMA),
        processing.probe_ranges_from_df(incoming, ["key"]),
    ) == candidates
