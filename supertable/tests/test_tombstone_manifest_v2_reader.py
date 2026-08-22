"""Focused parity and fail-closed tests for v2 deletion-vector readers."""

from __future__ import annotations

from dataclasses import replace
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest

from supertable.data_classes import (
    MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES,
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
    TOMBSTONE_FORMAT_V3,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
    tombstone_v3_artifact_digest,
)
from supertable.simple_table import SimpleTable
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.utils.snapshot import read_bounded_tombstone_manifest_bytes


engine_common = importlib.import_module("supertable.engine.engine_common")


class _MemoryStorage:
    def __init__(self, *, blobs=None, frames=None, sizes=None):
        self.blobs = dict(blobs or {})
        self.frames = dict(frames or {})
        self.sizes = dict(sizes or {})
        self.read_bytes_calls = []
        self.read_range_calls = []
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

    def stat_object(self, key):
        body = self.blobs.get(key, b"")
        return ObjectMetadata(
            size=self.sizes[key],
            version=f"memory:{key}:{body.hex()}",
        )

    def read_range(self, key, offset, length, *, expected=None):
        self.read_range_calls.append((key, offset, length, expected))
        current = self.stat_object(key)
        if current != expected:
            raise OSError("object identity changed")
        return self.blobs[key][offset:offset + length]

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
    storage.read_range_calls.clear()
    with pytest.raises(TombstoneManifestV2Error, match="size"):
        load_tombstone_manifest_from_storage(storage, manifest_key)
    assert storage.read_bytes_calls == []
    assert storage.read_range_calls == []


def test_manifest_loader_normalizes_malformed_uri_parser_error() -> None:
    storage = _MemoryStorage()
    with pytest.raises(TombstoneManifestV2Error, match="logical storage path"):
        load_tombstone_manifest_from_storage(storage, "//[.json")
    assert storage.read_bytes_calls == []


def test_manifest_loader_requires_conditional_bounded_storage() -> None:
    class WholeObjectOnly:
        reads = 0

        def size(self, _key):
            return 1

        def read_bytes(self, _key):
            self.reads += 1
            raise AssertionError("whole-object fallback must not run")

    storage = WholeObjectOnly()
    with pytest.raises(
        TombstoneManifestV2Error,
        match="stat_object and read_range",
    ):
        read_bounded_tombstone_manifest_bytes(
            storage,
            "org/s/tables/t/tombstone/manifest.json",
        )
    with pytest.raises(
        TombstoneManifestV2Error,
        match="stat_object and read_range",
    ):
        load_tombstone_manifest_from_storage(
            storage,
            "org/s/tables/t/tombstone/manifest.json",
        )
    assert storage.reads == 0


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


def _island_source_relation() -> pl.LazyFrame:
    return pl.DataFrame({
        "id": pl.Series([10], dtype=pl.Int64),
        "__rowid__": pl.Series([1], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
        "__supertable_source_file__": pl.Series(
            ["data/a.parquet"], dtype=pl.String,
        ),
    }).lazy()


def test_islanddb_accepts_only_exact_empty_v2_state() -> None:
    engine = IslandDB.__new__(IslandDB)
    engine._load_tombstone = MagicMock(
        side_effect=AssertionError("empty state must not load a tombstone"),
    )
    empty = TombstoneDef(
        tombstone_path=None,
        cache_key=None,
        expected_rows=0,
        tombstone_digest=None,
        tombstone_format=2,
        segments=(),
    )

    result = engine._apply_tombstone(
        _island_source_relation(),
        SuperSnapshot("s", "t", 1),
        empty,
    ).collect()

    assert result.to_dict(as_series=False) == {"id": [10]}
    engine._load_tombstone.assert_not_called()


@pytest.mark.parametrize(
    "malformed",
    [
        TombstoneDef(tombstone_format=2),
        TombstoneDef(
            expected_rows=0,
            tombstone_format=2,
            segments=(
                TombstoneSegmentDef(
                    cache_key="org/s/tables/t/tombstone/a.parquet",
                    tombstone_path="/tmp/a.parquet",
                    expected_rows=1,
                    file_size=1,
                    tombstone_digest="0" * 64,
                ),
            ),
        ),
        TombstoneDef(
            tombstone_path="/resolved/manifest.json",
            cache_key="org/s/tables/t/tombstone/manifest.json",
            expected_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=2,
            segments=(),
        ),
        TombstoneDef(
            tombstone_path="/resolved/dv.parquet",
            cache_key="org/s/tables/t/tombstone/dv.parquet",
            expected_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=3,
        ),
        TombstoneDef(
            tombstone_path="",
            cache_key="org/s/tables/t/tombstone/dv.parquet",
            expected_rows=1,
            tombstone_digest="0" * 64,
        ),
    ],
)
def test_islanddb_rejects_malformed_empty_and_active_states_before_io(
    malformed,
) -> None:
    engine = IslandDB.__new__(IslandDB)
    engine._load_tombstone = MagicMock(
        side_effect=AssertionError("malformed state must fail before I/O"),
    )

    with pytest.raises(IslandIntegrityError, match="invalid"):
        engine._apply_tombstone(
            _island_source_relation(),
            SuperSnapshot("s", "t", 1),
            malformed,
        )
    engine._load_tombstone.assert_not_called()


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


@pytest.mark.parametrize(
    "identity",
    ["", 7, "x" * (MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES + 1)],
)
def test_duckdb_v2_provider_identity_is_bounded(identity, tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    with pytest.raises(ValueError, match="bounded non-empty string"):
        replace(definition.segments[0], provider_identity=identity)


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


class _TrackedDuckDBConnection:
    def __init__(self, connection, paths):
        self.connection = connection
        self.path_reads = {str(path): 0 for path in paths}

    def execute(self, sql, *args, **kwargs):
        statement = str(sql)
        for path in self.path_reads:
            if path in statement:
                self.path_reads[path] += 1
        return self.connection.execute(sql, *args, **kwargs)


class _SequencedProviderStorage:
    def __init__(self, observations):
        self.observations = {
            key: list(values) for key, values in observations.items()
        }
        self.calls = []

    def stat_object(self, key):
        self.calls.append(key)
        values = self.observations[key]
        if len(values) > 1:
            return values.pop(0)
        return values[0]


def _with_provider_identity(definition, version="v1"):
    return replace(
        definition,
        segments=tuple(
            replace(
                segment,
                provider_identity=ObjectMetadata(
                    size=segment.file_size,
                    version=version,
                ).identity_token(),
            )
            for segment in definition.segments
        ),
    )


def test_duckdb_v2_provider_proof_fences_one_segment_read(tmp_path) -> None:
    definition = _with_provider_identity(_duckdb_v2(tmp_path))
    observations = {
        segment.cache_key: [
            ObjectMetadata(size=segment.file_size, version="v1"),
            ObjectMetadata(size=segment.file_size, version="v1"),
        ]
        for segment in definition.segments
    }
    storage = _SequencedProviderStorage(observations)
    paths = [segment.tombstone_path for segment in definition.segments]
    raw_connection = duckdb.connect()
    con = _TrackedDuckDBConnection(raw_connection, paths)

    with patch.object(
        engine_common, "_local_parquet_file_identity", return_value=None,
    ):
        table = TombstoneCache(
            capacity=2, ttl_seconds=60, storage=storage,
        ).acquire(
            con,
            definition.cache_key,
            definition.tombstone_path,
            expected_rows=definition.expected_rows,
            expected_digest=definition.tombstone_digest,
            tombstone_def=definition,
        )

    assert table is not None
    assert con.path_reads == {path: 1 for path in paths}
    assert storage.calls == [
        definition.segments[0].cache_key,
        definition.segments[0].cache_key,
        definition.segments[1].cache_key,
        definition.segments[1].cache_key,
    ]


def test_duckdb_v2_provider_identity_change_after_read_fails_closed(
    tmp_path,
) -> None:
    definition = _with_provider_identity(_duckdb_v2(tmp_path))
    first = definition.segments[0]
    storage = _SequencedProviderStorage({
        first.cache_key: [
            ObjectMetadata(size=first.file_size, version="v1"),
            ObjectMetadata(size=first.file_size, version="v2"),
        ],
    })
    paths = [segment.tombstone_path for segment in definition.segments]
    raw_connection = duckdb.connect()
    con = _TrackedDuckDBConnection(raw_connection, paths)
    cache = TombstoneCache(
        capacity=2, ttl_seconds=60, storage=storage,
    )

    with patch.object(
        engine_common, "_local_parquet_file_identity", return_value=None,
    ), pytest.raises(RuntimeError, match="changed while being read"):
        cache.acquire(
            con,
            definition.cache_key,
            definition.tombstone_path,
            expected_rows=definition.expected_rows,
            expected_digest=definition.tombstone_digest,
            tombstone_def=definition,
        )

    assert con.path_reads == {paths[0]: 1, paths[1]: 0}
    assert cache.snapshot() == []


def test_duckdb_v2_provider_proof_mismatch_rejects_before_read(
    tmp_path,
) -> None:
    definition = _with_provider_identity(_duckdb_v2(tmp_path))
    first = definition.segments[0]
    storage = _SequencedProviderStorage({
        first.cache_key: [
            ObjectMetadata(size=first.file_size, version="other"),
        ],
    })
    con = MagicMock()

    with patch.object(
        engine_common, "_local_parquet_file_identity", return_value=None,
    ), pytest.raises(RuntimeError, match="provider identity does not match"):
        TombstoneCache(
            capacity=2, ttl_seconds=60, storage=storage,
        ).acquire(
            con,
            definition.cache_key,
            definition.tombstone_path,
            expected_rows=definition.expected_rows,
            expected_digest=definition.tombstone_digest,
            tombstone_def=definition,
        )

    con.execute.assert_not_called()


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


@pytest.mark.parametrize("capacity", [2, 0])
def test_duckdb_v2_cache_enforces_actual_parquet_file_size(
    tmp_path, capacity,
) -> None:
    definition = _duckdb_v2(tmp_path)
    raw_connection = duckdb.connect()
    con = MagicMock(wraps=raw_connection)
    cache = TombstoneCache(capacity=capacity, ttl_seconds=60)

    accepted = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=list(definition.snapshot_resource_keys or ()),
    )
    assert accepted is not None
    cache.release(con, accepted.cache_key)
    con.execute.reset_mock()

    altered = replace(
        definition,
        segments=(
            replace(
                definition.segments[0],
                file_size=definition.segments[0].file_size + 1,
            ),
            definition.segments[1],
        ),
    )
    rejecting_cache = TombstoneCache(capacity=capacity, ttl_seconds=60)
    with pytest.raises(RuntimeError, match="file_size does not match"):
        rejecting_cache.acquire(
            con,
            altered.cache_key,
            altered.tombstone_path,
            expected_rows=altered.expected_rows,
            expected_digest=altered.tombstone_digest,
            tombstone_def=altered,
            allowed_files=list(altered.snapshot_resource_keys or ()),
        )
    assert rejecting_cache.snapshot() == []
    con.execute.assert_not_called()


def test_direct_duckdb_v2_rejects_actual_parquet_file_size_mismatch(
    tmp_path,
) -> None:
    definition = _duckdb_v2(tmp_path)
    altered = replace(
        definition,
        segments=(
            replace(
                definition.segments[0],
                file_size=definition.segments[0].file_size + 1,
            ),
            definition.segments[1],
        ),
    )
    con = duckdb.connect()
    _source(con)

    with pytest.raises(RuntimeError, match="file_size does not match"):
        create_tombstone_view(con, "src", "live", altered)


@pytest.mark.parametrize(
    "pattern",
    ["segment-*.parquet", "segment-?.parquet", "segment-[ab].parquet"],
)
def test_direct_duckdb_v2_rejects_segment_globs_before_io(
    tmp_path, pattern,
) -> None:
    definition = _duckdb_v2(tmp_path)
    altered = replace(
        definition,
        segments=(
            replace(
                definition.segments[0],
                tombstone_path=str(tmp_path / pattern),
            ),
            definition.segments[1],
        ),
    )
    con = MagicMock()
    with pytest.raises(RuntimeError, match="one exact object path"):
        TombstoneCache(capacity=2, ttl_seconds=60).acquire(
            con,
            altered.cache_key,
            altered.tombstone_path,
            expected_rows=altered.expected_rows,
            expected_digest=altered.tombstone_digest,
            tombstone_def=altered,
        )
    con.execute.assert_not_called()


def test_direct_duckdb_v2_rejects_remote_segment_without_provider_proof(
    tmp_path,
) -> None:
    definition = _duckdb_v2(tmp_path)
    altered = replace(
        definition,
        segments=(
            replace(
                definition.segments[0],
                tombstone_path=(
                    "https://objects.example/segment-a.parquet?token=secret"
                ),
            ),
            definition.segments[1],
        ),
    )
    con = MagicMock()

    with pytest.raises(RuntimeError, match="pinned provider identity"):
        TombstoneCache(capacity=2, ttl_seconds=60).acquire(
            con,
            altered.cache_key,
            altered.tombstone_path,
            expected_rows=altered.expected_rows,
            expected_digest=altered.tombstone_digest,
            tombstone_def=altered,
        )
    con.execute.assert_not_called()


def test_duckdb_v2_capacity_zero_reads_once_and_survives_replacement(
    tmp_path,
) -> None:
    definition = _duckdb_v2(tmp_path)
    paths = [segment.tombstone_path for segment in definition.segments]
    raw_connection = duckdb.connect()
    con = _TrackedDuckDBConnection(raw_connection, paths)
    _source(con)
    cache = TombstoneCache(capacity=0, ttl_seconds=60)

    table = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=list(definition.snapshot_resource_keys or ()),
    )
    assert table is not None
    create_tombstone_view(
        con, "src", "zero_capacity_live", definition, dv_table=table,
    )

    # Replace both external files after the view exists. A path-backed view
    # would now resurrect the originally deleted rows; the validated private
    # table must remain bound to the bytes read during acquire().
    _frame("data/a.parquet", [2]).write_parquet(paths[0])
    _frame("data/b.parquet", [4]).write_parquet(paths[1])
    assert raw_connection.execute(
        "SELECT id FROM zero_capacity_live ORDER BY id"
    ).fetchall() == [(20,), (40,)]
    assert con.path_reads == {path: 1 for path in paths}

    table_name = str(table)
    cache.release(con, table.cache_key)
    assert cache.snapshot() == []
    with pytest.raises(duckdb.CatalogException):
        raw_connection.execute(f'SELECT * FROM "{table_name}"')


def test_duckdb_v2_capacity_zero_isolates_two_cursor_tables(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    root = duckdb.connect()
    _source(root)
    first_con = root.cursor()
    second_con = root.cursor()
    cache = TombstoneCache(capacity=0, ttl_seconds=60)

    first = cache.acquire(
        first_con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=list(definition.snapshot_resource_keys or ()),
    )
    second = cache.acquire(
        second_con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=list(definition.snapshot_resource_keys or ()),
    )

    assert first is not None and second is not None
    assert first != second
    create_tombstone_view(
        first_con, "src", "first_live", definition, dv_table=first,
    )
    create_tombstone_view(
        second_con, "src", "second_live", definition, dv_table=second,
    )
    assert first_con.execute(
        "SELECT id FROM first_live ORDER BY id"
    ).fetchall() == [(20,), (40,)]
    assert second_con.execute(
        "SELECT id FROM second_live ORDER BY id"
    ).fetchall() == [(20,), (40,)]

    # A capacity-zero table is TEMPORARY: neither sibling cursor may receive a
    # registry hit for a table that exists only in the creating cursor.
    with pytest.raises(duckdb.CatalogException):
        second_con.execute(f'SELECT * FROM "{first}"')
    with pytest.raises(duckdb.CatalogException):
        first_con.execute(f'SELECT * FROM "{second}"')

    first_con.execute("DROP VIEW first_live")
    second_con.execute("DROP VIEW second_live")
    cache.release(first_con, first.cache_key)
    cache.release(second_con, second.cache_key)
    assert cache.snapshot() == []


def test_duckdb_v2_capacity_zero_refcounts_per_cursor_and_drops_owner_table(
    tmp_path,
) -> None:
    definition = _duckdb_v2(tmp_path)
    root = duckdb.connect()
    first_con = root.cursor()
    second_con = root.cursor()
    cache = TombstoneCache(capacity=0, ttl_seconds=60)

    def acquire(con):
        return cache.acquire(
            con,
            definition.cache_key,
            definition.tombstone_path,
            expected_rows=definition.expected_rows,
            expected_digest=definition.tombstone_digest,
            tombstone_def=definition,
            allowed_files=list(definition.snapshot_resource_keys or ()),
        )

    first = acquire(first_con)
    first_again = acquire(first_con)
    second = acquire(second_con)
    assert first is not None and second is not None
    assert first_again == first
    assert first.cache_key == first_again.cache_key
    assert first.cache_key != second.cache_key
    assert sorted(entry["ref_count"] for entry in cache.snapshot()) == [1, 2]

    cache.release(first_con, first.cache_key)
    by_key = {entry["cache_key"]: entry for entry in cache.snapshot()}
    assert by_key[first.cache_key]["ref_count"] == 1
    assert first_con.execute(f'SELECT count(*) FROM "{first}"').fetchone() == (2,)

    cache.release(second_con, second.cache_key)
    assert {entry["cache_key"] for entry in cache.snapshot()} == {
        first.cache_key,
    }
    with pytest.raises(duckdb.CatalogException):
        second_con.execute(f'SELECT * FROM "{second}"')
    assert first_con.execute(f'SELECT count(*) FROM "{first}"').fetchone() == (2,)

    cache.release(first_con, first.cache_key)
    assert cache.snapshot() == []
    with pytest.raises(duckdb.CatalogException):
        first_con.execute(f'SELECT * FROM "{first}"')


def test_duckdb_v2_persistent_cache_reads_each_segment_once(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    paths = [segment.tombstone_path for segment in definition.segments]
    raw_connection = duckdb.connect()
    con = _TrackedDuckDBConnection(raw_connection, paths)
    cache = TombstoneCache(capacity=2, ttl_seconds=60)

    first = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
    )
    second = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
    )
    assert first == second
    assert con.path_reads == {path: 1 for path in paths}
    cache.release(con, first.cache_key)
    cache.release(con, second.cache_key)


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


def test_direct_duckdb_rejects_resolved_json_hybrid_and_accepts_empty_v2() -> None:
    con = duckdb.connect()
    _source(con)
    hybrid = TombstoneDef(
        tombstone_path="https://objects.example/manifest?signature=secret",
        cache_key="org/s/tables/t/tombstone/manifest.json",
        expected_rows=1,
        tombstone_digest="0" * 64,
    )
    with pytest.raises(RuntimeError, match="requires tombstone_format=2"):
        create_tombstone_view(con, "src", "hybrid_live", hybrid)

    empty_v2 = TombstoneDef(
        tombstone_path=None,
        cache_key=None,
        expected_rows=0,
        tombstone_digest=None,
        tombstone_format=2,
        segments=(),
    )
    create_tombstone_view(con, "src", "empty_v2_live", empty_v2)
    assert con.execute(
        "SELECT id FROM empty_v2_live ORDER BY id"
    ).fetchall() == [(10,), (20,), (30,), (40,)]


def test_duckdb_path_collection_uses_every_segment_not_manifest(tmp_path) -> None:
    definition = _duckdb_v2(tmp_path)
    paths = (
        "https://objects.example/segment-a.parquet?token=[one]*?",
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


@pytest.mark.parametrize(
    "malformed",
    [
        TombstoneDef(
            tombstone_path="/resolved/dv.parquet",
            cache_key="org/s/tables/t/tombstone/dv.parquet",
            expected_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=True,
        ),
        TombstoneDef(
            tombstone_path="/resolved/manifest.json",
            cache_key="org/s/tables/t/tombstone/manifest.json",
            expected_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=None,
        ),
        TombstoneDef(
            tombstone_path="/resolved/manifest.json",
            cache_key="org/s/tables/t/tombstone/manifest.json",
            expected_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=2,
            segments=(),
        ),
        TombstoneDef(
            tombstone_path="",
            cache_key="org/s/tables/t/tombstone/dv.parquet",
            expected_rows=1,
            tombstone_digest="0" * 64,
        ),
    ],
)
def test_spark_rejects_malformed_tombstone_hybrid_before_setup(
    malformed,
) -> None:
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1)],
        tombstone_views={"t": malformed},
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()
    executor._get_connection = MagicMock()

    with pytest.raises(RuntimeError, match="Invalid Spark deletion-vector"):
        executor.execute(
            reflection,
            parser=None,
            query_manager=None,
            timer_capture=lambda _phase: None,
        )

    executor._select_cluster.assert_not_called()
    executor._get_connection.assert_not_called()


def test_data_reader_rejects_manifest_root_outside_pinned_table() -> None:
    import importlib
    import supertable.data_reader as reader_module

    observations_module = importlib.import_module(
        "supertable.engine.query_observations"
    )

    table = SimpleNamespace(alias="t", super_name="s", simple_name="t")
    parser = MagicMock(
        original_query="SELECT * FROM t",
    )
    parser.get_table_tuples.return_value = [table]
    parser.get_physical_tables.return_value = [table]
    parser.get_predicate_constraints.return_value = {}
    parser.get_join_edges.return_value = []
    foreign_manifest = "org/s/tables/other/tombstone/manifest.json"
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot(
            super_name="s",
            simple_name="t",
            simple_version=7,
            files=["org/s/tables/t/data/a.parquet"],
            columns={"id"},
            tombstone_key=foreign_manifest,
            tombstone_rows=1,
            tombstone_digest="0" * 64,
            tombstone_format=2,
        )],
    )
    estimator = MagicMock()
    estimator.estimate.return_value = reflection
    estimator._to_duckdb_path.side_effect = AssertionError(
        "out-of-scope manifest must fail before path resolution"
    )
    executor = MagicMock()
    storage = MagicMock()

    with (
        patch.object(reader_module, "get_storage", return_value=storage),
        patch.object(reader_module, "SQLParser", return_value=parser),
        patch.object(reader_module, "DataEstimator", return_value=estimator),
        patch.object(reader_module, "Executor", return_value=executor),
        patch.object(reader_module, "restrict_read_access", return_value={}),
        patch.object(reader_module, "validate_rbac_binding_stability"),
        patch.object(reader_module, "QueryPlanManager") as query_plan,
        patch.object(reader_module, "Timer") as timer,
        patch.object(reader_module, "PlanStats") as plan_stats,
        patch.object(reader_module, "extend_execution_plan"),
        patch.object(
            observations_module, "QueryObservationStore",
        ) as observations,
    ):
        query_plan.return_value = MagicMock(query_id="q", query_hash="h")
        timer.return_value = MagicMock(timings=[])
        plan_stats.return_value = MagicMock()
        observations.return_value = MagicMock(enabled=False)
        reader = reader_module.DataReader("s", "org", "SELECT * FROM t")
        reader._assert_targets_exist = MagicMock()
        _frame_result, status, message = reader.execute("admin")

    assert status is reader_module.Status.ERROR
    assert "manifest pointer escapes the pinned table" in str(message)
    estimator._to_duckdb_path.assert_not_called()
    executor.execute.assert_not_called()


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


def test_simple_table_export_v3_matches_logical_live_rows(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path)
    snapshot, snapshot_path = table.get_simple_table_snapshot()
    data_key = snapshot["resources"][0]["file"]
    tombstone_key = f"{table.simple_dir}/tombstone/deleted-v3.parquet"
    storage.write_parquet(_frame(data_key, [1, 3]).to_arrow(), tombstone_key)
    payload = storage.read_bytes(tombstone_key)
    snapshot.update({
        "tombstone": tombstone_key,
        "tombstone_rows": 2,
        "tombstone_digest": tombstone_v3_artifact_digest(payload),
        "tombstone_format": TOMBSTONE_FORMAT_V3,
    })
    table.get_simple_table_snapshot = lambda: (snapshot, snapshot_path)
    monkeypatch.setattr(processing, "_storage", storage)

    result = table.export_to("exports/good-v3")

    exported = pl.concat([
        pl.from_arrow(storage.read_parquet(path))
        for path in result["files"]
    ])
    assert result["total_rows"] == 2
    assert exported.sort("id").get_column("id").to_list() == [20, 40]


def test_simple_table_export_rejects_v3_artifact_outside_tombstone_prefix(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path)
    snapshot, snapshot_path = table.get_simple_table_snapshot()
    data_key = snapshot["resources"][0]["file"]
    tombstone_key = f"{table.simple_dir}/data/not-a-tombstone.parquet"
    storage.write_parquet(_frame(data_key, [1]).to_arrow(), tombstone_key)
    snapshot.update({
        "tombstone": tombstone_key,
        "tombstone_rows": 1,
        "tombstone_digest": tombstone_v3_artifact_digest(
            storage.read_bytes(tombstone_key)
        ),
        "tombstone_format": TOMBSTONE_FORMAT_V3,
    })
    table.get_simple_table_snapshot = lambda: (snapshot, snapshot_path)
    monkeypatch.setattr(processing, "_storage", storage)

    with pytest.raises(ValueError, match="escapes the pinned simple table"):
        table.export_to("exports/foreign-v3")


def test_simple_table_export_accepts_authoritative_pre_dv_snapshot(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path)
    snapshot, snapshot_path = table.get_simple_table_snapshot()
    for field in (
        "tombstone",
        "tombstone_rows",
        "tombstone_digest",
        "tombstone_format",
    ):
        snapshot.pop(field, None)
    table.get_simple_table_snapshot = lambda: (snapshot, snapshot_path)
    monkeypatch.setattr(processing, "_storage", storage)

    result = table.export_to("exports/pre-dv")

    exported = pl.concat([
        pl.from_arrow(storage.read_parquet(path))
        for path in result["files"]
    ])
    assert result["total_rows"] == 4
    assert exported.sort("id").get_column("id").to_list() == [10, 20, 30, 40]


def test_simple_table_export_missing_v2_segment_writes_nothing(
        tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path, missing_segment=True)
    monkeypatch.setattr(processing, "_storage", storage)
    target = tmp_path / "exports" / "missing"

    with pytest.raises(ValueError, match="segment size"):
        table.export_to("exports/missing")

    assert not target.exists()


def test_simple_table_export_rejects_manifest_root_outside_table(
    tmp_path, monkeypatch,
) -> None:
    table, storage = _local_export_table(tmp_path)
    snapshot, snapshot_path = table.get_simple_table_snapshot()
    original_manifest = snapshot["tombstone"]
    foreign_manifest = "org/s/tables/other/tombstone/manifest.json"
    storage.write_bytes(
        foreign_manifest,
        storage.read_bytes(original_manifest),
    )
    snapshot["tombstone"] = foreign_manifest
    table.get_simple_table_snapshot = lambda: (snapshot, snapshot_path)
    monkeypatch.setattr(processing, "_storage", storage)
    target = tmp_path / "exports" / "foreign-root"

    with pytest.raises(ValueError, match="escapes the pinned simple table"):
        table.export_to("exports/foreign-root")

    assert not target.exists()
