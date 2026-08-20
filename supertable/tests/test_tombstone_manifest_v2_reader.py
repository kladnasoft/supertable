"""Focused parity and fail-closed tests for v2 deletion-vector readers."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import polars as pl
import pytest

from supertable.data_classes import (
    Reflection,
    SuperSnapshot,
    TombstoneDef,
    TombstoneSegmentDef,
)
from supertable.engine.engine_common import TombstoneCache, create_tombstone_view
from supertable.engine.duckdb_engine import _tombstone_source_paths
from supertable.engine.file_cache import FileCache, FileCacheIntegrityError
from supertable.engine.islanddb import IslandDB, IslandIntegrityError
from supertable.engine.spark_thrift import SparkThriftExecutor
import supertable.processing as processing
from supertable.processing import (
    load_tombstone_manifest_from_storage,
    load_tombstone_segments,
    tombstone_digest,
)
from supertable.tombstone_manifest_v2 import (
    MAX_TOMBSTONE_MANIFEST_V2_BYTES,
    MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
)
from supertable.simple_table import SimpleTable
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata


class _MemoryStorage:
    def __init__(self, *, blobs=None, frames=None, sizes=None):
        self.blobs = dict(blobs or {})
        self.frames = dict(frames or {})
        self.sizes = dict(sizes or {})
        self.read_bytes_calls = []
        self.read_parquet_calls = []

    def size(self, key):
        return self.sizes[key]

    def stat_object(self, key):
        size = self.sizes[key]
        return ObjectMetadata(size=size, version=f"memory:{key}:{size}")

    def read_range(self, key, offset, length, *, expected=None):
        observed = self.stat_object(key)
        if expected is not None and observed != expected:
            raise OSError("object identity changed")
        return self.read_bytes(key)[offset:offset + length]

    def read_bytes(self, key):
        self.read_bytes_calls.append(key)
        return self.blobs[key]

    def read_parquet(self, key, columns=None):
        self.read_parquet_calls.append(key)
        frame = self.frames[key]
        if columns is not None:
            frame = frame.select(columns)
        return frame.to_arrow()


def _frame(file_key: str, rowids: list[int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "__file__": pl.Series([file_key] * len(rowids), dtype=pl.String),
            "__rowid__": pl.Series(rowids, dtype=pl.Int64),
        }
    )


def _memory_v2():
    manifest_key = "org/s/tables/t/tombstone/manifest.json"
    keys = (
        "org/s/tables/t/tombstone/segment-a.parquet",
        "org/s/tables/t/tombstone/segment-b.parquet",
    )
    frames = {
        keys[0]: _frame("data/a.parquet", [1]),
        keys[1]: _frame("data/b.parquet", [3]),
    }
    sizes = {keys[0]: 101, keys[1]: 202}
    manifest = TombstoneManifestV2(
        organization="org",
        super_name="s",
        simple_name="t",
        base_snapshot_version=4,
        snapshot_version=5,
        total_rows=2,
        segments=tuple(
            TombstoneSegment(
                file=key,
                rows=frames[key].height,
                file_size=sizes[key],
                digest=tombstone_digest(frames[key]),
            )
            for key in keys
        ),
    )
    body = manifest.canonical_bytes()
    storage = _MemoryStorage(
        blobs={manifest_key: body},
        frames=frames,
        sizes={manifest_key: len(body), **sizes},
    )
    definitions = tuple(
        TombstoneSegmentDef(
            cache_key=segment.file,
            tombstone_path=segment.file,
            expected_rows=segment.rows,
            file_size=segment.file_size,
            tombstone_digest=segment.digest,
        )
        for segment in manifest.segments
    )
    return manifest_key, manifest, storage, definitions


def test_bounded_canonical_manifest_and_segment_union_parity() -> None:
    manifest_key, expected, storage, definitions = _memory_v2()
    loaded = load_tombstone_manifest_from_storage(
        storage,
        manifest_key,
        expected_organization="org",
        expected_super_name="s",
        expected_simple_name="t",
        pinned_snapshot_version=7,
        expected_total_rows=2,
        expected_digest=expected.digest(),
        expected_segment_prefix="org/s/tables/t/tombstone",
    )
    assert loaded == expected

    union = load_tombstone_segments(
        definitions,
        storage=storage,
        cache_identity=f"{manifest_key}:{expected.digest()}",
        expected_rows=2,
        allowed_files={"data/a.parquet", "data/b.parquet"},
        allow_cache=False,
    )
    assert union.sort("__rowid__").to_dict(as_series=False) == {
        "__file__": ["data/a.parquet", "data/b.parquet"],
        "__rowid__": [1, 3],
    }


def test_manifest_loader_rejects_noncanonical_and_oversized_before_read() -> None:
    manifest_key, manifest, storage, _definitions = _memory_v2()
    noncanonical = b"\n" + manifest.canonical_bytes()
    storage.blobs[manifest_key] = noncanonical
    storage.sizes[manifest_key] = len(noncanonical)
    with pytest.raises(TombstoneManifestV2Error, match="canonical form"):
        load_tombstone_manifest_from_storage(storage, manifest_key)

    storage.sizes[manifest_key] = MAX_TOMBSTONE_MANIFEST_V2_BYTES + 1
    storage.read_bytes_calls.clear()
    with pytest.raises(TombstoneManifestV2Error, match="size"):
        load_tombstone_manifest_from_storage(storage, manifest_key)
    assert storage.read_bytes_calls == []


def test_manifest_loader_normalizes_malformed_uri_parser_error() -> None:
    storage = _MemoryStorage()
    with pytest.raises(TombstoneManifestV2Error, match="logical storage path"):
        load_tombstone_manifest_from_storage(storage, "//[.json")
    assert storage.read_bytes_calls == []


def test_segment_union_rejects_cross_segment_duplicate_and_size_mismatch() -> None:
    manifest_key, manifest, storage, definitions = _memory_v2()
    duplicate_key = definitions[1].cache_key
    storage.frames[duplicate_key] = _frame("data/b.parquet", [1])
    duplicate_definitions = (
        definitions[0],
        replace(
            definitions[1],
            tombstone_digest=tombstone_digest(storage.frames[duplicate_key]),
        ),
    )
    with pytest.raises(ValueError, match="rowid.*not unique|reuses a rowid"):
        load_tombstone_segments(
            duplicate_definitions,
            storage=storage,
            cache_identity=f"duplicate:{manifest_key}:{manifest.digest()}",
            expected_rows=2,
            allowed_files={"data/a.parquet", "data/b.parquet"},
            allow_cache=False,
        )

    with pytest.raises(ValueError, match="size does not match"):
        load_tombstone_segments(
            (replace(definitions[0], file_size=102), definitions[1]),
            storage=storage,
            cache_identity=f"size:{manifest_key}:{manifest.digest()}",
            expected_rows=2,
            allowed_files={"data/a.parquet", "data/b.parquet"},
            allow_cache=False,
        )


def test_segment_union_rejects_unbounded_direct_descriptor_set() -> None:
    _manifest_key, _manifest, storage, definitions = _memory_v2()
    oversized = tuple(
        replace(
            definitions[0],
            cache_key=(
                "org/s/tables/t/tombstone/"
                f"segment-{index:03d}.parquet"
            ),
        )
        for index in range(MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS + 1)
    )
    with pytest.raises(ValueError, match="too many segments"):
        load_tombstone_segments(
            oversized,
            storage=storage,
            cache_identity="oversized",
            expected_rows=len(oversized),
        )


def test_allow_cache_false_revalidates_changed_segment() -> None:
    manifest_key, manifest, storage, definitions = _memory_v2()
    identity = f"export:{manifest_key}:{manifest.digest()}"
    load_tombstone_segments(
        definitions,
        storage=storage,
        cache_identity=identity,
        expected_rows=2,
        allowed_files={"data/a.parquet", "data/b.parquet"},
    )
    storage.frames[definitions[0].cache_key] = _frame("data/a.parquet", [9])
    with pytest.raises(ValueError, match="digest mismatch"):
        load_tombstone_segments(
            definitions,
            storage=storage,
            cache_identity=identity,
            expected_rows=2,
            allowed_files={"data/a.parquet", "data/b.parquet"},
            allow_cache=False,
        )


def _memory_definition(manifest_key, manifest, definitions) -> TombstoneDef:
    return TombstoneDef(
        tombstone_path=manifest_key,
        cache_key=manifest_key,
        expected_rows=manifest.total_rows,
        tombstone_digest=manifest.digest(),
        resource_keys=("data/a.parquet", "data/b.parquet"),
        snapshot_resource_keys=("data/a.parquet", "data/b.parquet"),
        tombstone_format=2,
        segments=definitions,
    )


def test_islanddb_v2_union_parity_and_missing_or_tampered_segment() -> None:
    manifest_key, manifest, storage, definitions = _memory_v2()
    engine = IslandDB.__new__(IslandDB)
    engine.storage = storage
    engine._artifact_cache_namespace = "reader-test"
    definition = _memory_definition(manifest_key, manifest, definitions)

    with pytest.raises(IslandIntegrityError, match="representation"):
        engine._load_tombstone(replace(definition, segments=()))

    loaded = engine._load_tombstone(definition)
    assert loaded.sort("__rowid__").to_dict(as_series=False) == {
        "__file__": ["data/a.parquet", "data/b.parquet"],
        "__rowid__": [1, 3],
    }

    missing = replace(
        definition,
        cache_key="org/s/tables/t/tombstone/missing-manifest.json",
        tombstone_digest="e" * 64,
    )
    storage.sizes.pop(definitions[1].cache_key)
    with pytest.raises(IslandIntegrityError, match="failed validation"):
        engine._load_tombstone(missing)

    storage.sizes[definitions[1].cache_key] = definitions[1].file_size
    storage.frames[definitions[1].cache_key] = _frame("data/b.parquet", [9])
    tampered = replace(
        definition,
        cache_key="org/s/tables/t/tombstone/tampered-manifest.json",
        tombstone_digest="d" * 64,
    )
    with pytest.raises(IslandIntegrityError, match="failed validation"):
        engine._load_tombstone(tampered)


def _duckdb_v2(tmp_path):
    frames = (
        _frame("data/a.parquet", [1]),
        _frame("data/b.parquet", [3]),
    )
    paths = (tmp_path / "segment-a.parquet", tmp_path / "segment-b.parquet")
    for frame, path in zip(frames, paths):
        frame.write_parquet(path)
    segments = tuple(
        TombstoneSegmentDef(
            cache_key=f"org/s/tables/t/tombstone/{path.name}",
            tombstone_path=str(path),
            expected_rows=frame.height,
            file_size=path.stat().st_size,
            tombstone_digest=tombstone_digest(frame),
        )
        for frame, path in zip(frames, paths)
    )
    root = "f" * 64  # Deliberately not the union's logical row digest.
    definition = TombstoneDef(
        tombstone_path="/manifest/must-never-be-opened.json",
        cache_key="org/s/tables/t/tombstone/manifest.json",
        expected_rows=2,
        tombstone_digest=root,
        resource_keys=("data/a.parquet", "data/b.parquet"),
        snapshot_resource_keys=("data/a.parquet", "data/b.parquet"),
        tombstone_format=2,
        segments=segments,
    )
    return definition


def _source(con) -> None:
    con.execute(
        "CREATE TABLE src(id BIGINT, __rowid__ BIGINT, "
        "__timestamp__ TIMESTAMP, __supertable_source_file__ VARCHAR)"
    )
    con.execute(
        "INSERT INTO src VALUES "
        "(10, 1, now(), 'data/a.parquet'), "
        "(20, 2, now(), 'data/a.parquet'), "
        "(30, 3, now(), 'data/b.parquet'), "
        "(40, 4, now(), 'data/b.parquet')"
    )


def test_duckdb_v2_inline_and_cached_paths_are_equal(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    con = duckdb.connect()
    _source(con)

    create_tombstone_view(con, "src", "inline_live", definition)
    inline = con.execute("SELECT id FROM inline_live ORDER BY id").fetchall()

    cache = TombstoneCache(capacity=2, ttl_seconds=60)
    cached_table = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=list(definition.snapshot_resource_keys or ()),
    )
    assert cached_table is not None
    create_tombstone_view(
        con, "src", "cached_live", definition, dv_table=cached_table,
    )
    cached = con.execute("SELECT id FROM cached_live ORDER BY id").fetchall()
    assert inline == cached == [(20,), (40,)]
    cache.release(con, cached_table.cache_key)


def test_duckdb_cache_hit_rejects_changed_segment_descriptors(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    con = duckdb.connect()
    cache = TombstoneCache(capacity=2, ttl_seconds=60)
    table = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
    )
    assert table is not None
    altered = replace(
        definition,
        segments=(
            replace(definition.segments[0], file_size=999),
            definition.segments[1],
        ),
    )
    with pytest.raises(RuntimeError, match="segment descriptors"):
        cache.acquire(
            con,
            altered.cache_key,
            altered.tombstone_path,
            expected_rows=altered.expected_rows,
            expected_digest=altered.tombstone_digest,
            tombstone_def=altered,
        )
    cache.release(con, table.cache_key)


def test_direct_duckdb_caller_rejects_missing_v2_segments() -> None:
    malformed = TombstoneDef(
        tombstone_path="manifest.json",
        cache_key="manifest.json",
        expected_rows=1,
        tombstone_digest="0" * 64,
        resource_keys=("data/a.parquet",),
        tombstone_format=2,
    )
    con = duckdb.connect()
    _source(con)
    with pytest.raises(RuntimeError, match="no sealed segments"):
        create_tombstone_view(con, "src", "live", malformed)


def test_duckdb_path_collection_uses_every_segment_not_manifest(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    paths = (
        "https://objects.example/segment-a.parquet?token=one",
        "https://objects.example/segment-b.parquet?token=two",
    )
    definition = replace(
        definition,
        segments=tuple(
            replace(segment, tombstone_path=path)
            for segment, path in zip(definition.segments, paths)
        ),
    )
    reflection = Reflection(
        storage_type="object",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1)],
        tombstone_views={"t": definition},
    )
    assert _tombstone_source_paths(reflection) == list(paths)
    assert definition.tombstone_path not in _tombstone_source_paths(reflection)


def test_explicit_spark_rejects_v2_before_cluster_or_connection(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)

    reflection = Reflection(
        storage_type="local",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1)],
        tombstone_views={"t": definition},
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()
    executor._get_connection = MagicMock()
    with pytest.raises(RuntimeError, match="tombstone_format=2"):
        executor.execute(
            reflection,
            parser=None,
            query_manager=None,
            timer_capture=lambda _phase: None,
        )
    executor._select_cluster.assert_not_called()
    executor._get_connection.assert_not_called()


def test_explicit_spark_rejects_fake_empty_v2_before_connection() -> None:
    malformed = TombstoneDef(
        tombstone_path=None,
        cache_key=None,
        expected_rows=0,
        tombstone_digest=None,
        tombstone_format=2,
    )
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=0,
        total_reflections=0,
        supers=[SuperSnapshot("s", "t", 5)],
        tombstone_views={"t": malformed},
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()
    executor._get_connection = MagicMock()

    with pytest.raises(RuntimeError, match="tombstone_format=2"):
        executor.execute(
            reflection,
            parser=None,
            query_manager=None,
            timer_capture=lambda _phase: None,
        )

    executor._select_cluster.assert_not_called()
    executor._get_connection.assert_not_called()


class _LocalStorage:
    @staticmethod
    def is_local_storage():
        return True

    @staticmethod
    def cache_namespace():
        return {"provider": "test-local"}


def test_file_cache_localizes_segments_but_never_manifest(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    definition = replace(
        definition,
        segments=tuple(
            replace(segment, cache_key=segment.tombstone_path)
            for segment in definition.segments
        ),
    )
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=0,
        total_reflections=0,
        supers=[SuperSnapshot("s", "t", 5)],
        tombstone_views={"t": definition},
    )
    localized, metrics = FileCache(
        _LocalStorage(), "org", root=str(tmp_path / "cache"), workers=1,
    ).localize_reflection(reflection)

    localized_def = localized.tombstone_views["t"]
    assert localized_def.cache_key == definition.cache_key
    assert localized_def.tombstone_path == definition.tombstone_path
    assert tuple(segment.cache_key for segment in localized_def.segments) == tuple(
        segment.cache_key for segment in definition.segments
    )
    assert all(
        segment.tombstone_path == str(path.resolve())
        for segment, path in zip(
            localized_def.segments,
            (tmp_path / "segment-a.parquet", tmp_path / "segment-b.parquet"),
        )
    )
    assert metrics.requested_files == metrics.localized_files == 2


def test_file_cache_rejects_local_v2_segment_size_mismatch(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    local_segments = tuple(
        replace(
            segment,
            cache_key=segment.tombstone_path,
            file_size=(
                segment.file_size + 1
                if index == 0 else segment.file_size
            ),
        )
        for index, segment in enumerate(definition.segments)
    )
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=0,
        total_reflections=0,
        supers=[SuperSnapshot("s", "t", 5)],
        tombstone_views={
            "t": replace(definition, segments=local_segments),
        },
    )
    cache = FileCache(
        _LocalStorage(), "org", root=str(tmp_path / "cache"), workers=1,
    )
    with pytest.raises(FileCacheIntegrityError, match="size"):
        cache.localize_reflection(reflection)


def _local_export_table(tmp_path, *, missing_segment: bool = False):
    storage = LocalStorage(root=tmp_path)
    simple_dir = "org/s/tables/t"
    data_key = f"{simple_dir}/data/source.parquet"
    data = pl.DataFrame({
        "id": pl.Series([10, 20, 30, 40], dtype=pl.Int64),
        "__rowid__": pl.Series([1, 2, 3, 4], dtype=pl.Int64),
        "__timestamp__": pl.Series([1, 2, 3, 4], dtype=pl.Int64).cast(
            pl.Datetime("ns")
        ),
    })
    storage.write_parquet(data.to_arrow(), data_key)

    segment_frames = (
        _frame(data_key, [1]),
        _frame(data_key, [3]),
    )
    segment_keys = (
        f"{simple_dir}/tombstone/segment-a.parquet",
        f"{simple_dir}/tombstone/segment-b.parquet",
    )
    segment_sizes = []
    for index, (key, frame) in enumerate(zip(segment_keys, segment_frames)):
        if not (missing_segment and index == 1):
            storage.write_parquet(frame.to_arrow(), key)
            segment_sizes.append(storage.size(key))
        else:
            segment_sizes.append(321)

    manifest = TombstoneManifestV2(
        organization="org",
        super_name="s",
        simple_name="t",
        base_snapshot_version=4,
        snapshot_version=5,
        total_rows=2,
        segments=tuple(
            TombstoneSegment(
                file=key,
                rows=frame.height,
                file_size=size,
                digest=tombstone_digest(frame),
            )
            for key, frame, size in zip(
                segment_keys, segment_frames, segment_sizes
            )
        ),
    )
    manifest_key = f"{simple_dir}/tombstone/manifest.json"
    storage.write_bytes(manifest_key, manifest.canonical_bytes())
    snapshot = {
        "snapshot_version": 5,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='ns', time_zone=None)",
        },
        "resources": [{
            "file": data_key,
            "file_size": storage.size(data_key),
            "rows": data.height,
        }],
        "tombstone": manifest_key,
        "tombstone_rows": 2,
        "tombstone_digest": manifest.digest(),
        "tombstone_format": 2,
    }

    table = SimpleTable.__new__(SimpleTable)
    table.super_table = SimpleNamespace(organization="org", super_name="s")
    table.simple_name = "t"
    table.simple_dir = simple_dir
    table.storage = storage
    table.catalog = MagicMock()
    table.catalog.get_table_config.return_value = {
        "max_memory_chunk_size": 1024 * 1024,
        "max_overlapping_files": 100,
    }
    table.get_simple_table_snapshot = lambda: (
        snapshot,
        f"{simple_dir}/snapshots/v5.json",
    )
    return table, storage


def test_simple_table_export_v2_matches_logical_live_rows(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path)
    monkeypatch.setattr(processing, "_storage", storage)

    result = table.export_to("exports/good")

    exported = pl.concat([
        pl.from_arrow(storage.read_parquet(path))
        for path in result["files"]
    ])
    assert result["total_rows"] == 2
    assert exported.sort("id").get_column("id").to_list() == [20, 40]


def test_simple_table_export_missing_v2_segment_writes_nothing(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path, missing_segment=True)
    monkeypatch.setattr(processing, "_storage", storage)
    target = tmp_path / "exports" / "missing"

    with pytest.raises(ValueError, match="segment size"):
        table.export_to("exports/missing")

    assert not target.exists()
