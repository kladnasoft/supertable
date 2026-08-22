from __future__ import annotations

import io
import uuid

import duckdb
import polars as pl
import pytest

import supertable.engine.engine_common as engine_common
from supertable.data_classes import TombstoneDef
from supertable.engine.engine_common import TombstoneCache
from supertable.engine.islanddb import IslandDB, IslandIntegrityError
from supertable.processing import TOMBSTONE_FILE_COL, TOMBSTONE_SCHEMA
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V3,
    tombstone_v3_artifact_digest,
)
from supertable.utils.snapshot import referenced_snapshot_artifacts


RESOURCE = "org/lake/tables/events/data/part.parquet"


def _payload(rowid: int = 7) -> tuple[pl.DataFrame, bytes]:
    frame = pl.DataFrame(
        {TOMBSTONE_FILE_COL: [RESOURCE], "__rowid__": [rowid]},
        schema=TOMBSTONE_SCHEMA,
    )
    sink = io.BytesIO()
    frame.write_parquet(sink, compression="zstd", compression_level=1)
    return frame, sink.getvalue()


def _definition(path: str, payload: bytes) -> TombstoneDef:
    key = (
        "org/lake/tables/events/tombstone/"
        f"{uuid.uuid4().hex}.parquet"
    )
    return TombstoneDef(
        tombstone_path=path,
        cache_key=key,
        expected_rows=1,
        tombstone_digest=tombstone_v3_artifact_digest(payload),
        resource_keys=(RESOURCE,),
        snapshot_resource_keys=(RESOURCE,),
        tombstone_format=TOMBSTONE_FORMAT_V3,
    )


def test_duckdb_v3_materializes_once_without_v1_logical_validation(
    tmp_path, monkeypatch,
) -> None:
    _frame, payload = _payload()
    path = tmp_path / "dv-v3.parquet"
    path.write_bytes(payload)
    definition = _definition(str(path), payload)
    con = duckdb.connect()
    cache = TombstoneCache(capacity=1, ttl_seconds=300)

    monkeypatch.setattr(
        engine_common,
        "_validate_tombstone_relation_details",
        lambda *_args, **_kwargs: pytest.fail(
            "trusted v3 must not use the v1 logical digest/uniqueness scan"
        ),
    )
    first = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=[RESOURCE],
    )
    assert first is not None
    assert con.execute(f"SELECT count(*) FROM {first}").fetchone() == (1,)

    monkeypatch.setattr(
        engine_common._PinnedLocalTombstoneStorage,
        "read_bytes",
        lambda *_args, **_kwargs: pytest.fail("sealed cache hit reread v3 bytes"),
    )
    second = cache.acquire(
        con,
        definition.cache_key,
        definition.tombstone_path,
        expected_rows=definition.expected_rows,
        expected_digest=definition.tombstone_digest,
        tombstone_def=definition,
        allowed_files=[RESOURCE],
    )
    assert second == first

    cache.release(con, first.cache_key)
    cache.release(con, second.cache_key)
    con.close()


def test_duckdb_v3_rejects_same_count_byte_substitution(tmp_path) -> None:
    _frame, payload = _payload(7)
    path = tmp_path / "dv-v3.parquet"
    path.write_bytes(payload)
    definition = _definition(str(path), payload)
    _replacement, replacement_payload = _payload(8)
    path.write_bytes(replacement_payload)

    with duckdb.connect() as con, pytest.raises(
        Exception, match="bytes do not match",
    ):
        TombstoneCache(capacity=0).acquire(
            con,
            definition.cache_key,
            definition.tombstone_path,
            expected_rows=definition.expected_rows,
            expected_digest=definition.tombstone_digest,
            tombstone_def=definition,
            allowed_files=[RESOURCE],
        )


def test_v3_remote_read_uses_logical_artifact_key() -> None:
    _frame, payload = _payload()
    definition = _definition("https://example.invalid/signed-object", payload)

    class Storage:
        calls: list[str] = []

        def read_bytes(self, path: str) -> bytes:
            self.calls.append(path)
            return payload

    storage = Storage()
    loaded, referenced = engine_common._load_v3_tombstone_frame(
        definition,
        storage=storage,
        allowed_files=[RESOURCE],
        cache_identity=f"reader-v3-{uuid.uuid4().hex}",
    )

    assert loaded.height == 1
    assert referenced == frozenset({RESOURCE})
    assert storage.calls == [definition.cache_key]


def test_v3_snapshot_retains_one_direct_parquet_without_manifest_loader() -> None:
    _frame, payload = _payload()
    key = "org/lake/tables/events/tombstone/direct-v3.parquet"
    references = referenced_snapshot_artifacts({
        "snapshot_version": 2,
        "resources": [{
            "file": RESOURCE,
            "file_size": 123,
        }],
        "tombstone": key,
        "tombstone_rows": 1,
        "tombstone_digest": tombstone_v3_artifact_digest(payload),
        "tombstone_format": TOMBSTONE_FORMAT_V3,
    }, organization="org", super_name="lake", simple_name="events")

    by_path = {reference.path: reference for reference in references}
    assert set(by_path) == {RESOURCE, key}
    assert by_path[key].kind == "tombstone_v3"
    assert by_path[key].declared_digest == tombstone_v3_artifact_digest(payload)


def test_islanddb_v3_uses_exact_byte_seal_and_shallow_state(tmp_path) -> None:
    _frame, payload = _payload()
    path = tmp_path / "island-v3.parquet"
    path.write_bytes(payload)
    definition = _definition(str(path), payload)
    engine = IslandDB(organization=f"reader-{uuid.uuid4().hex}")

    loaded = engine._load_tombstone(definition)
    assert loaded.select("__rowid__").to_series().to_list() == [7]

    path.write_bytes(_payload(9)[1])
    changed = _definition(str(path), payload)
    with pytest.raises(IslandIntegrityError, match="exact-byte validation"):
        engine._load_tombstone(changed)
