from __future__ import annotations

import io
from types import SimpleNamespace

import polars as pl
import pytest

import supertable.processing as processing
from supertable.data_writer import DataWriter
from supertable.processing import (
    LoadedTombstoneState,
    TOMBSTONE_SCHEMA,
    build_tombstone_v3,
    cache_tombstone,
    evict_tombstone,
    load_tombstone,
    persist_tombstone_v3_frame,
    resolve_overwrite_writes,
)
from supertable.storage.local_storage import LocalStorage
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V3,
    TombstoneManifestV2Error,
    tombstone_v3_artifact_digest,
)
from supertable.utils.profiler import Profiler


_PREFIX = "org/lake/tables/table/tombstone"
_A = "org/lake/tables/table/data/a.parquet"
_B = "org/lake/tables/table/data/b.parquet"


def _frame(*pairs: tuple[str, int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "__file__": [pair[0] for pair in pairs],
            "__rowid__": [pair[1] for pair in pairs],
        },
        schema=TOMBSTONE_SCHEMA,
    )


def _encoded(frame: pl.DataFrame) -> bytes:
    target = io.BytesIO()
    frame.write_parquet(target, compression="zstd", compression_level=1)
    return target.getvalue()


def _first(storage: LocalStorage):
    return build_tombstone_v3(
        _PREFIX,
        None,
        [(_A, 11)],
        1,
        allowed_files={_A, _B},
        storage=storage,
    )


def test_v3_writes_one_parquet_and_pins_its_exact_bytes(tmp_path):
    storage = LocalStorage(tmp_path)

    path, frame, state = _first(storage)

    assert path is not None and path.endswith(".parquet")
    assert frame is not None and frame.rows() == [(_A, 11)]
    assert state is not None
    assert state.tombstone_format == TOMBSTONE_FORMAT_V3
    assert state.tombstone_path == path
    assert state.segments == ()
    assert state.referenced_files == frozenset({_A})
    assert state.root_digest == tombstone_v3_artifact_digest(
        storage.read_bytes(path)
    )


def test_v3_snapshot_publication_pins_one_table_local_parquet():
    DataWriter._validate_snapshot_for_publish(
        {
            "snapshot_version": 5,
            "tombstone": f"{_PREFIX}/deleted-v3.parquet",
            "tombstone_rows": 1,
            "tombstone_digest": "a" * 64,
            "tombstone_format": TOMBSTONE_FORMAT_V3,
        },
        simple_table=SimpleNamespace(simple_dir="org/lake/tables/table"),
        expected_version=4,
    )

    with pytest.raises(ValueError, match="expected table prefix"):
        DataWriter._validate_snapshot_for_publish(
            {
                "snapshot_version": 5,
                "tombstone": "org/lake/tables/other/tombstone/x.parquet",
                "tombstone_rows": 1,
                "tombstone_digest": "a" * 64,
                "tombstone_format": TOMBSTONE_FORMAT_V3,
            },
            simple_table=SimpleNamespace(simple_dir="org/lake/tables/table"),
            expected_version=4,
        )


@pytest.mark.parametrize("required", [False, True])
def test_v3_cold_load_rejects_same_count_object_substitution(
    tmp_path, monkeypatch, required,
):
    storage = LocalStorage(tmp_path)
    path, _frame_out, state = _first(storage)
    assert path is not None and state is not None
    storage.write_bytes(path, _encoded(_frame((_A, 99))))
    monkeypatch.setattr(
        processing.polars,
        "read_parquet",
        lambda *_args, **_kwargs: pytest.fail(
            "mismatched v3 bytes reached the Parquet decoder"
        ),
    )

    with pytest.raises(
        TombstoneManifestV2Error,
        match="bytes do not match",
    ):
        load_tombstone(
            path,
            allow_cache=False,
            required=required,
            expected_rows=1,
            expected_digest=state.root_digest,
            allowed_files={_A},
            tombstone_format=TOMBSTONE_FORMAT_V3,
            storage=storage,
        )


def test_v3_successor_validates_only_delta_and_never_uses_v1_digest(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(tmp_path)
    path, previous, state = _first(storage)
    assert path is not None and previous is not None and state is not None
    original_validate = processing.validate_tombstone_frame
    calls: list[int] = []

    def counted(frame, *args, **kwargs):
        calls.append(frame.height)
        return original_validate(frame, *args, **kwargs)

    monkeypatch.setattr(processing, "validate_tombstone_frame", counted)
    monkeypatch.setattr(
        processing,
        "tombstone_digest",
        lambda *_args, **_kwargs: pytest.fail("v1 logical digest was used"),
    )

    successor_path, successor, successor_state = build_tombstone_v3(
        _PREFIX,
        path,
        [(_B, 12)],
        1,
        prev_df=previous,
        previous_state=state,
        expected_previous_rows=1,
        allowed_files={_A, _B},
        persist=False,
        storage=storage,
    )

    assert successor_path is None and successor_state is None
    assert successor is not None and successor.rows() == [(_A, 11), (_B, 12)]
    assert calls == [1]


def test_v3_writer_resolver_proof_elides_predecessor_scan_and_is_mutation_safe(
    tmp_path,
):
    storage = LocalStorage(tmp_path)
    source_path = tmp_path / "source.parquet"
    source_key = "source.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": [11, 12],
    }).write_parquet(source_path)
    previous = _frame((source_key, 11))
    prior_state = LoadedTombstoneState(
        frame=previous,
        tombstone_format=TOMBSTONE_FORMAT_V3,
        tombstone_path=f"{_PREFIX}/old.parquet",
        root_digest="a" * 64,
        referenced_files=frozenset({source_key}),
    )
    _incoming, pairs = resolve_overwrite_writes(
        incoming_df=pl.DataFrame({"id": [2]}),
        overlapping_files={(source_key, True, source_path.stat().st_size)},
        overwrite_columns=["id"],
        existing_tombstones=previous,
        storage=storage,
        require_global_tombstone_disjoint_proof=True,
    )
    assert pairs == [(source_key, 12)]
    profiler = Profiler()

    _path, successor, _state = build_tombstone_v3(
        _PREFIX,
        prior_state.tombstone_path,
        pairs,
        1,
        profiler=profiler,
        prev_df=previous,
        previous_state=prior_state,
        expected_previous_rows=1,
        allowed_files={source_key},
        persist=False,
    )

    assert successor is not None and successor.height == 2
    assert profiler.counts["tombstone_v3_prior_scan_elided"] == 1

    # A mutated issued list loses its exact provenance and takes the safe
    # intersection fallback, which catches the old rowid.
    pairs.append((source_key, 11))
    with pytest.raises(ValueError, match="repeats an existing"):
        build_tombstone_v3(
            _PREFIX,
            prior_state.tombstone_path,
            pairs,
            1,
            prev_df=previous,
            previous_state=prior_state,
            expected_previous_rows=1,
            allowed_files={source_key},
            persist=False,
        )


def test_v3_resolver_proof_rejects_cross_file_global_rowid_reuse(tmp_path):
    storage = LocalStorage(tmp_path)
    source_path = tmp_path / "source-b.parquet"
    source_key = "source-b.parquet"
    pl.DataFrame({
        "id": [2],
        "__rowid__": [11],
    }).write_parquet(source_path)
    predecessor = _frame((_A, 11))

    with pytest.raises(ValueError, match="globally tombstoned rowid"):
        resolve_overwrite_writes(
            incoming_df=pl.DataFrame({"id": [2]}),
            overlapping_files={(source_key, True, source_path.stat().st_size)},
            overwrite_columns=["id"],
            existing_tombstones=predecessor,
            storage=storage,
            require_global_tombstone_disjoint_proof=True,
        )


def test_v3_successor_requires_real_pinned_predecessor_state(tmp_path):
    storage = LocalStorage(tmp_path)
    previous = _frame((_A, 11))

    with pytest.raises(ValueError, match="pinned predecessor state"):
        build_tombstone_v3(
            _PREFIX,
            f"{_PREFIX}/old.parquet",
            [(_B, 12)],
            1,
            prev_df=previous,
            expected_previous_rows=1,
            allowed_files={_A, _B},
            persist=False,
            storage=storage,
        )


def test_v3_delta_rejects_cross_file_reuse_and_foreign_files(tmp_path):
    storage = LocalStorage(tmp_path)
    path, previous, state = _first(storage)
    assert path is not None and previous is not None and state is not None

    with pytest.raises(ValueError, match="repeats an existing"):
        build_tombstone_v3(
            _PREFIX,
            path,
            [(_B, 11)],
            1,
            prev_df=previous,
            previous_state=state,
            expected_previous_rows=1,
            allowed_files={_A, _B},
            persist=False,
            storage=storage,
        )

    with pytest.raises(ValueError, match="outside the current snapshot"):
        build_tombstone_v3(
            _PREFIX,
            path,
            [("org/lake/tables/table/data/foreign.parquet", 12)],
            1,
            prev_df=previous,
            previous_state=state,
            expected_previous_rows=1,
            allowed_files={_A, _B},
            persist=False,
            storage=storage,
        )


def test_v3_delta_keeps_set_semantics_for_identical_pairs(tmp_path):
    storage = LocalStorage(tmp_path)

    _path, frame, _state = build_tombstone_v3(
        _PREFIX,
        None,
        [(_A, 11), (_A, 11)],
        1,
        allowed_files={_A},
        persist=False,
        storage=storage,
    )

    assert frame is not None and frame.rows() == [(_A, 11)]


def test_v3_cache_hit_retains_exact_state_and_reference_fence(tmp_path):
    storage = LocalStorage(tmp_path)
    path, frame, state = _first(storage)
    assert path is not None and frame is not None and state is not None
    identity = "test-v3-cache"
    cache_tombstone(
        path,
        frame,
        cache_identity=identity,
        expected_rows=1,
        expected_digest=state.root_digest,
        assume_valid=True,
        loaded_state=state,
        tombstone_format=TOMBSTONE_FORMAT_V3,
    )
    # The cache owns an O(1) copy-on-write clone, not the caller's mutable
    # DataFrame handle.
    frame.replace_column(
        1, pl.Series("__rowid__", [99], dtype=pl.Int64),
    )
    state_out = {}
    try:
        loaded = load_tombstone(
            path,
            cache_identity=identity,
            required=True,
            expected_rows=1,
            expected_digest=state.root_digest,
            allowed_files={_A},
            tombstone_format=TOMBSTONE_FORMAT_V3,
            state_out=state_out,
            storage=storage,
        )
        assert loaded is not frame
        assert loaded.rows() == [(_A, 11)]
        assert state_out["state"] is not state
        assert state_out["state"].frame is loaded

        # A same-shape mutation of a returned cache hit must not poison the
        # cache-owned immutable frame used by the next reader/writer.
        loaded.replace_column(
            1, pl.Series("__rowid__", [77], dtype=pl.Int64),
        )
        next_state = {}
        next_loaded = load_tombstone(
            path,
            cache_identity=identity,
            required=True,
            expected_rows=1,
            expected_digest=state.root_digest,
            allowed_files={_A},
            tombstone_format=TOMBSTONE_FORMAT_V3,
            state_out=next_state,
            storage=storage,
        )
        assert next_loaded is not loaded
        assert next_loaded.rows() == [(_A, 11)]
        assert next_state["state"].frame is next_loaded

        with pytest.raises(ValueError, match="outside the current snapshot"):
            load_tombstone(
                path,
                cache_identity=identity,
                required=True,
                expected_rows=1,
                expected_digest=state.root_digest,
                allowed_files={_B},
                tombstone_format=TOMBSTONE_FORMAT_V3,
                storage=storage,
            )
    finally:
        evict_tombstone(path, cache_identity=identity)


def test_v3_cache_entry_cannot_bypass_v1_logical_digest(tmp_path):
    storage = LocalStorage(tmp_path)
    path, frame, state = _first(storage)
    assert path is not None and frame is not None and state is not None
    identity = "test-v3-cache-format-fence"
    cache_tombstone(
        path,
        frame,
        cache_identity=identity,
        expected_rows=1,
        expected_digest=state.root_digest,
        assume_valid=True,
        loaded_state=state,
        tombstone_format=TOMBSTONE_FORMAT_V3,
    )
    try:
        # A v3 snapshot seals exact Parquet bytes, while v1 seals the canonical
        # logical row stream. Reusing the former under the latter format would
        # make acceptance depend on whether this process happened to be warm.
        with pytest.raises(ValueError, match="digest mismatch"):
            load_tombstone(
                path,
                cache_identity=identity,
                required=True,
                expected_rows=1,
                expected_digest=state.root_digest,
                allowed_files={_A},
                tombstone_format=TOMBSTONE_FORMAT_V1,
                storage=storage,
            )
    finally:
        evict_tombstone(path, cache_identity=identity)


def test_v3_compaction_persist_retains_full_integrity_boundary(tmp_path):
    storage = LocalStorage(tmp_path)

    with pytest.raises(ValueError, match="reuses a rowid across files"):
        persist_tombstone_v3_frame(
            _PREFIX,
            _frame((_A, 11), (_B, 11)),
            1,
            storage=storage,
        )
