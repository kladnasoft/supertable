from __future__ import annotations

import hashlib
import json
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.processing import (
    LoadedTombstoneState,
    TOMBSTONE_SCHEMA,
    build_tombstone_v2,
    load_tombstone,
    persist_tombstone_segment_v2,
    persist_tombstone_v2_frame,
    tombstone_digest,
)
from supertable.data_writer import DataWriter
from supertable.errors import SnapshotCommitConflictError
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.tombstone_manifest_v2 import (
    MAX_TOMBSTONE_MANIFEST_V2_BYTES,
    MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS,
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V2,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
    load_tombstone_manifest_v2,
)
from supertable.utils.profiler import Profiler


_PREFIX = "org/lake/tables/table/tombstone"


def _frame(*pairs: tuple[str, int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "__file__": [pair[0] for pair in pairs],
            "__rowid__": [pair[1] for pair in pairs],
        },
        schema=TOMBSTONE_SCHEMA,
    )


def _build(
    storage: object,
    previous_state: LoadedTombstoneState | None,
    new_pairs: list[tuple[str, int]],
    *,
    base_snapshot_version: int = 4,
    profiler: Profiler | None = None,
):
    return build_tombstone_v2(
        tombstone_dir=_PREFIX,
        previous_state=previous_state,
        new_pairs=new_pairs,
        compression_level=1,
        organization="org",
        super_name="lake",
        simple_name="table",
        base_snapshot_version=base_snapshot_version,
        snapshot_version=base_snapshot_version + 1,
        profiler=profiler,
        storage=storage,
    )


def test_precommit_snapshot_validation_accepts_exact_empty_v2():
    DataWriter._validate_snapshot_for_publish(
        {
            "snapshot_version": 5,
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "tombstone_format": TOMBSTONE_FORMAT_V2,
        },
        simple_table=SimpleNamespace(
            simple_dir="org/lake/tables/table",
        ),
        expected_version=4,
    )


def test_pinned_snapshot_validation_normalizes_pre_dv_document():
    snapshot = {"snapshot_version": 1, "schema": {}, "resources": []}

    DataWriter._validate_pinned_tombstone_state(snapshot)

    assert snapshot["tombstone"] is None
    assert snapshot["tombstone_rows"] == 0
    assert snapshot["tombstone_digest"] is None
    assert "tombstone_format" not in snapshot


@pytest.mark.parametrize(
    "partial_state",
    [
        {"tombstone_rows": 0},
        {"tombstone": None, "tombstone_rows": 0},
        {"tombstone_format": TOMBSTONE_FORMAT_V1},
    ],
)
def test_pinned_snapshot_validation_rejects_partial_empty_state(partial_state):
    with pytest.raises(TombstoneManifestV2Error, match="all present"):
        DataWriter._validate_pinned_tombstone_state(partial_state)


@pytest.mark.parametrize(
    "payload,match",
    [
        (
            {
                "snapshot_version": 5,
                "tombstone": None,
                "tombstone_rows": 0,
            },
            "incomplete",
        ),
        (
            {
                "snapshot_version": 4,
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
            },
            "advance",
        ),
        (
            {
                "snapshot_version": 5,
                "tombstone": "org/other/tombstone/manifest.json",
                "tombstone_rows": 1,
                "tombstone_digest": "a" * 64,
                "tombstone_format": TOMBSTONE_FORMAT_V2,
            },
            "expected table prefix",
        ),
    ],
)
def test_precommit_snapshot_validation_rejects_unsafe_payload(payload, match):
    with pytest.raises(ValueError, match=match):
        DataWriter._validate_snapshot_for_publish(
            payload,
            simple_table=SimpleNamespace(
                simple_dir="org/lake/tables/table",
            ),
            expected_version=4,
        )


@pytest.mark.parametrize(
    "commit_error,retained",
    [
        (SnapshotCommitConflictError("definite conflict"), False),
        (TimeoutError("ambiguous catalog timeout"), True),
    ],
)
def test_writer_durability_cleanup_distinguishes_rejection_from_ambiguity(
    tmp_path, commit_error, retained,
):
    storage = LocalStorage(tmp_path)
    snapshot_path = "org/lake/tables/table/snapshots/v5.json"
    base_path = "org/lake/tables/table/snapshots/v4.json"

    class Catalog:
        def commit_snapshot(self, *_args, **_kwargs):
            raise commit_error

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        organization="org", super_name="lake", storage=storage,
    )
    writer.catalog = Catalog()
    simple_table = SimpleNamespace(
        simple_dir="org/lake/tables/table",
        _last_snapshot_leaf={"version": 4, "path": base_path},
    )
    payload = {
        "snapshot_version": 5,
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "tombstone_format": TOMBSTONE_FORMAT_V2,
    }

    with pytest.raises(type(commit_error), match=str(commit_error)):
        with storage.durability_batch() as batch:
            storage.write_bytes(snapshot_path, b"{}")
            batch.barrier()
            writer._publish_snapshot(
                simple_table=simple_table,
                simple_name="table",
                payload=payload,
                path=snapshot_path,
                base_path=base_path,
                lock_token="token",
                commit_id="commit",
                now_ms=1,
                durability_batch=batch,
            )

    assert storage.exists(snapshot_path) is retained


def test_first_v2_delta_writes_logical_segment_and_canonical_manifest(tmp_path):
    storage = LocalStorage(tmp_path)
    profiler = Profiler()

    path, union, state = _build(
        storage,
        None,
        [("org/lake/tables/table/data/a.parquet", 11)],
        profiler=profiler,
    )

    assert path is not None and path.endswith(".json")
    assert union is not None and union.rows() == [
        ("org/lake/tables/table/data/a.parquet", 11)
    ]
    assert state is not None
    assert state.tombstone_format == TOMBSTONE_FORMAT_V2
    assert state.tombstone_path == path
    assert len(state.segments) == 1
    assert state.segments[0].digest == tombstone_digest(union)

    raw = storage.read_bytes(path)
    manifest = load_tombstone_manifest_v2(
        raw,
        expected_organization="org",
        expected_super_name="lake",
        expected_simple_name="table",
        pinned_snapshot_version=5,
        expected_total_rows=1,
        expected_digest=state.root_digest,
        expected_segment_prefix=_PREFIX,
        require_canonical_json=True,
    )
    assert raw == manifest.canonical_bytes()
    assert state.root_digest == hashlib.sha256(raw).hexdigest()
    assert state.root_digest != state.segments[0].digest
    assert profiler.counts["tombstone_v2_delta_rows"] == 1
    assert profiler.counts["tombstone_v2_segment_count"] == 1
    assert profiler.counts["tombstone_v2.segment_digest.n"] == 1
    assert profiler.counts["tombstone_v2.manifest_write.n"] == 1


def test_v1_base_segment_is_reused_without_rewrite_on_transition(tmp_path):
    storage = LocalStorage(tmp_path)
    prior_frame = _frame(("org/lake/tables/table/data/a.parquet", 11))
    prior_segment = persist_tombstone_segment_v2(
        _PREFIX,
        prior_frame,
        1,
        storage=storage,
    )
    prior_state = LoadedTombstoneState(
        frame=prior_frame,
        tombstone_format=TOMBSTONE_FORMAT_V1,
        tombstone_path=prior_segment.file,
        root_digest=prior_segment.digest,
        segments=(prior_segment,),
    )
    original_bytes = storage.read_bytes(prior_segment.file)
    original_hash = hashlib.sha256(original_bytes).hexdigest()
    original_metadata = storage.stat_object(prior_segment.file)
    original_inode = (tmp_path / prior_segment.file).stat().st_ino
    parquet_before = set(tmp_path.rglob("*.parquet"))

    _path, union, state = _build(
        storage,
        prior_state,
        [("org/lake/tables/table/data/b.parquet", 12)],
    )

    assert union is not None and union.height == 2
    assert state is not None and len(state.segments) == 2
    assert prior_segment in state.segments
    assert storage.exists(prior_segment.file)
    assert storage.read_bytes(prior_segment.file) == original_bytes
    assert hashlib.sha256(
        storage.read_bytes(prior_segment.file)
    ).hexdigest() == original_hash
    assert storage.stat_object(prior_segment.file) == original_metadata
    assert (tmp_path / prior_segment.file).stat().st_ino == original_inode
    parquet_after = set(tmp_path.rglob("*.parquet"))
    assert len(parquet_after - parquet_before) == 1


def _synthetic_prior_state(segment_count: int) -> LoadedTombstoneState:
    frames = [
        _frame((f"org/lake/tables/table/data/{index:02d}.parquet", index + 1))
        for index in range(segment_count)
    ]
    segments = tuple(
        TombstoneSegment(
            file=f"{_PREFIX}/segment-{index:02d}.parquet",
            rows=1,
            file_size=1,
            digest=tombstone_digest(frame),
        )
        for index, frame in enumerate(frames)
    )
    union = pl.concat(frames, how="vertical")
    return LoadedTombstoneState(
        frame=union,
        tombstone_format=TOMBSTONE_FORMAT_V2,
        tombstone_path=f"{_PREFIX}/prior.json",
        root_digest="0" * 64,
        segments=segments,
    )


def test_segment_cap_is_exactly_32_then_consolidates(tmp_path):
    storage = LocalStorage(tmp_path)

    _path, _union, at_cap = _build(
        storage,
        _synthetic_prior_state(MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS - 1),
        [("org/lake/tables/table/data/new-31.parquet", 32)],
    )
    assert at_cap is not None
    assert len(at_cap.segments) == MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS

    profiler = Profiler()
    _path, union, consolidated = _build(
        storage,
        _synthetic_prior_state(MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS),
        [("org/lake/tables/table/data/new-32.parquet", 33)],
        profiler=profiler,
    )
    assert union is not None and union.height == 33
    assert consolidated is not None and len(consolidated.segments) == 1
    assert consolidated.segments[0].rows == 33
    assert consolidated.segments[0].digest == tombstone_digest(union)
    assert profiler.counts["tombstone_v2_consolidations"] == 1
    assert profiler.counts["tombstone_v2_segment_count"] == 1


@pytest.mark.parametrize(
    "previous,new_pairs",
    [
        (None, [("data/a.parquet", 1), ("data/a.parquet", 1)]),
        (
            LoadedTombstoneState(
                frame=_frame(("data/a.parquet", 1)),
                tombstone_format=TOMBSTONE_FORMAT_V2,
                tombstone_path=f"{_PREFIX}/prior.json",
                root_digest="0" * 64,
                segments=(
                    TombstoneSegment(
                        file=f"{_PREFIX}/prior.parquet",
                        rows=1,
                        file_size=1,
                        digest=tombstone_digest(_frame(("data/a.parquet", 1))),
                    ),
                ),
            ),
            [("data/b.parquet", 1)],
        ),
    ],
)
def test_duplicate_delta_or_prior_union_pair_is_rejected_before_persist(
    tmp_path, previous, new_pairs,
):
    storage = LocalStorage(tmp_path)

    with pytest.raises(ValueError, match="duplicate|reuses a rowid"):
        _build(storage, previous, new_pairs)

    assert not list(tmp_path.rglob("*.json"))
    assert not list(tmp_path.rglob("*.parquet"))


def test_exact_empty_v2_successor_writes_no_segment_or_manifest(tmp_path):
    storage = LocalStorage(tmp_path)
    empty = pl.DataFrame(schema=TOMBSTONE_SCHEMA)

    path, validated, state = persist_tombstone_v2_frame(
        _PREFIX,
        empty,
        1,
        organization="org",
        super_name="lake",
        simple_name="table",
        base_snapshot_version=8,
        snapshot_version=9,
        storage=storage,
    )

    assert path is None
    assert validated.height == 0
    assert state == LoadedTombstoneState(
        frame=validated,
        tombstone_format=TOMBSTONE_FORMAT_V2,
        tombstone_path=None,
        root_digest=None,
        segments=(),
    )
    assert not list(tmp_path.rglob("*.json"))
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("value", [True, 1.0, 2.0, "2"])
def test_tombstone_format_discriminator_requires_an_exact_integer(value):
    with pytest.raises(TombstoneManifestV2Error, match="integer 1 or 2"):
        load_tombstone(None, tombstone_format=value)


class _BoundedManifestStorage:
    def __init__(
        self,
        manifest_path: str,
        raw: bytes,
        segments: tuple[TombstoneSegment, ...],
    ) -> None:
        self.manifest_path = manifest_path
        self.raw = raw
        self.range_calls = 0
        self.manifest_version = "manifest-v1"
        self._segment_sizes = {
            segment.file: segment.file_size for segment in segments
        }

    def stat_object(self, path: str) -> ObjectMetadata:
        if path == self.manifest_path:
            return ObjectMetadata(
                size=len(self.raw), version=self.manifest_version,
            )
        return ObjectMetadata(
            size=self._segment_sizes[path], version=f"segment:{path}",
        )

    def read_range(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> bytes:
        assert path == self.manifest_path
        assert offset == 0
        assert expected == self.stat_object(path)
        self.range_calls += 1
        return self.raw[:length]

    def size(self, path: str) -> int:
        return self.stat_object(path).size


def _manifest_fixture():
    frames = {
        f"{_PREFIX}/a.parquet": _frame(("data/a.parquet", 1)),
        f"{_PREFIX}/b.parquet": _frame(("data/b.parquet", 2)),
    }
    segments = tuple(
        TombstoneSegment(
            file=path,
            rows=frame.height,
            file_size=100 + index,
            digest=tombstone_digest(frame),
        )
        for index, (path, frame) in enumerate(frames.items())
    )
    manifest = TombstoneManifestV2(
        organization="org",
        super_name="lake",
        simple_name="table",
        base_snapshot_version=4,
        snapshot_version=5,
        total_rows=2,
        segments=segments,
    )
    path = f"{_PREFIX}/manifest.json"
    return path, manifest, frames


def test_v2_load_is_bounded_canonical_and_cached_only_after_complete_union():
    path, manifest, frames = _manifest_fixture()
    storage = _BoundedManifestStorage(
        path, manifest.canonical_bytes(), manifest.segments,
    )
    identity = f"v2-writer-test/{uuid.uuid4()}"
    attempts = 0

    def fail_second(segment: TombstoneSegment):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise OSError("second segment unavailable")
        return frames[segment.file], segment.file_size

    common = dict(
        cache_identity=identity,
        allow_cache=True,
        required=True,
        expected_rows=manifest.total_rows,
        expected_digest=manifest.digest(),
        allowed_files={"data/a.parquet", "data/b.parquet"},
        tombstone_format=TOMBSTONE_FORMAT_V2,
        storage=storage,
        expected_organization="org",
        expected_super_name="lake",
        expected_simple_name="table",
        pinned_snapshot_version=5,
        expected_segment_prefix=_PREFIX,
    )
    with pytest.raises(OSError, match="second segment unavailable"):
        load_tombstone(path, segment_loader=fail_second, **common)
    assert storage.range_calls == 1

    state_out: dict[str, LoadedTombstoneState] = {}
    loaded = load_tombstone(
        path,
        segment_loader=lambda segment: (
            frames[segment.file], segment.file_size,
        ),
        state_out=state_out,
        **common,
    )
    assert loaded is not None and loaded.height == 2
    assert state_out["state"].root_digest == manifest.digest()
    assert storage.range_calls == 2

    cached_state: dict[str, LoadedTombstoneState] = {}
    cached = load_tombstone(
        path,
        segment_loader=lambda _segment: pytest.fail("cache miss"),
        state_out=cached_state,
        **common,
    )
    assert cached is loaded
    assert cached_state["state"] is state_out["state"]
    assert storage.range_calls == 2


def test_v2_manifest_size_is_rejected_before_range_allocation():
    path, manifest, _frames = _manifest_fixture()
    storage = _BoundedManifestStorage(
        path, manifest.canonical_bytes(), manifest.segments,
    )

    def oversized_stat(candidate: str) -> ObjectMetadata:
        if candidate == path:
            return ObjectMetadata(
                size=MAX_TOMBSTONE_MANIFEST_V2_BYTES + 1,
                version="oversized",
            )
        return ObjectMetadata(size=storage.size(candidate), version="segment")

    storage.stat_object = oversized_stat  # type: ignore[method-assign]
    with pytest.raises(TombstoneManifestV2Error, match="size"):
        load_tombstone(
            path,
            required=True,
            allow_cache=False,
            tombstone_format=TOMBSTONE_FORMAT_V2,
            storage=storage,
        )
    assert storage.range_calls == 0


def test_v2_manifest_rejects_noncanonical_storage_bytes():
    path, manifest, frames = _manifest_fixture()
    raw = json.dumps(manifest.to_dict(), indent=2).encode("utf-8")
    storage = _BoundedManifestStorage(path, raw, manifest.segments)

    with pytest.raises(TombstoneManifestV2Error, match="canonical"):
        load_tombstone(
            path,
            required=True,
            allow_cache=False,
            expected_rows=2,
            expected_digest=manifest.digest(),
            tombstone_format=TOMBSTONE_FORMAT_V2,
            storage=storage,
            segment_loader=lambda segment: (
                frames[segment.file], segment.file_size,
            ),
        )


def test_v2_manifest_rejects_short_exact_range_read():
    path, manifest, _frames = _manifest_fixture()
    storage = _BoundedManifestStorage(
        path, manifest.canonical_bytes(), manifest.segments,
    )
    original_read = storage.read_range

    def short_read(*args, **kwargs):
        return original_read(*args, **kwargs)[:-1]

    storage.read_range = short_read  # type: ignore[method-assign]
    with pytest.raises(TombstoneManifestV2Error, match="short or oversized"):
        load_tombstone(
            path,
            required=True,
            allow_cache=False,
            tombstone_format=TOMBSTONE_FORMAT_V2,
            storage=storage,
        )


def test_v2_manifest_rejects_provider_identity_change_during_read():
    path, manifest, _frames = _manifest_fixture()
    storage = _BoundedManifestStorage(
        path, manifest.canonical_bytes(), manifest.segments,
    )
    original_read = storage.read_range

    def replacing_read(*args, **kwargs):
        raw = original_read(*args, **kwargs)
        storage.manifest_version = "manifest-v2"
        return raw

    storage.read_range = replacing_read  # type: ignore[method-assign]
    with pytest.raises(TombstoneManifestV2Error, match="changed"):
        load_tombstone(
            path,
            required=True,
            allow_cache=False,
            tombstone_format=TOMBSTONE_FORMAT_V2,
            storage=storage,
        )


def test_durability_batch_enrolls_data_segment_manifest_and_snapshot(tmp_path):
    storage = LocalStorage(tmp_path)
    data_path = "org/lake/tables/table/data/data.parquet"
    snapshot_path = "org/lake/tables/table/snapshots/snapshot.json"

    with storage.durability_batch() as batch:
        storage.write_bytes(data_path, b"data")
        manifest_path, _union, state = _build(
            storage,
            None,
            [(data_path, 1)],
        )
        storage.write_bytes(snapshot_path, b"{}")
        assert manifest_path is not None and state is not None
        assert len(batch._publications) == 4
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_succeeded()

    assert storage.exists(data_path)
    assert storage.exists(state.segments[0].file)
    assert storage.exists(manifest_path)
    assert storage.exists(snapshot_path)


def test_durability_batch_removes_partial_v2_publication_on_abort(tmp_path):
    storage = LocalStorage(tmp_path)
    published: list[str] = []

    with pytest.raises(RuntimeError, match="abort mutation"):
        with storage.durability_batch():
            manifest_path, _union, state = _build(
                storage,
                None,
                [("org/lake/tables/table/data/a.parquet", 1)],
            )
            assert manifest_path is not None and state is not None
            published = [state.segments[0].file, manifest_path]
            raise RuntimeError("abort mutation")

    assert published
    assert all(not storage.exists(path) for path in published)
