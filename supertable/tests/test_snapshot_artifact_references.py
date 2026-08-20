from __future__ import annotations

import json

import pytest

from supertable.tombstone_manifest_v2 import (
    MAX_TOMBSTONE_MANIFEST_V2_BYTES,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
)
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
)
from supertable.storage.local_storage import LocalStorage
from supertable.utils.snapshot import (
    read_bounded_tombstone_manifest_bytes,
    referenced_snapshot_artifacts,
)


ORG = "acme"
SUPER = "lake"
TABLE = "events"
TABLE_PREFIX = f"{ORG}/{SUPER}/tables/{TABLE}"
MANIFEST_PATH = f"{TABLE_PREFIX}/tombstone/generation-4/manifest.json"
SEGMENT_A = f"{TABLE_PREFIX}/tombstone/generation-4/segment-0001.parquet"
SEGMENT_B = f"{TABLE_PREFIX}/tombstone/generation-4/segment-0002.parquet"


def _manifest() -> TombstoneManifestV2:
    return TombstoneManifestV2(
        organization=ORG,
        super_name=SUPER,
        simple_name=TABLE,
        base_snapshot_version=3,
        snapshot_version=4,
        total_rows=3,
        segments=(
            TombstoneSegment(
                file=SEGMENT_A,
                rows=1,
                file_size=11,
                digest="a" * 64,
            ),
            TombstoneSegment(
                file=SEGMENT_B,
                rows=2,
                file_size=22,
                digest="b" * 64,
            ),
        ),
    )


def _v2_snapshot(manifest: TombstoneManifestV2) -> dict:
    return {
        "snapshot_version": 4,
        "resources": [{
            "file": f"{TABLE_PREFIX}/data/part.parquet",
            "file_size": 101,
        }],
        "tombstone": MANIFEST_PATH,
        "tombstone_rows": 3,
        "tombstone_digest": manifest.digest(),
        "tombstone_format": 2,
        "stats_file": f"{TABLE_PREFIX}/stats/stats.parquet",
    }


def test_gc_traversal_expands_v2_manifest_and_every_segment_deterministically():
    manifest = _manifest()
    snapshot = _v2_snapshot(manifest)

    references = referenced_snapshot_artifacts(
        snapshot,
        organization=ORG,
        super_name=SUPER,
        simple_name=TABLE,
        manifest_loader=lambda path: (
            manifest.canonical_bytes()
            if path == MANIFEST_PATH else pytest.fail(f"unexpected path {path}")
        ),
    )

    assert tuple(reference.path for reference in references) == tuple(sorted({
        f"{TABLE_PREFIX}/data/part.parquet",
        MANIFEST_PATH,
        SEGMENT_A,
        SEGMENT_B,
        f"{TABLE_PREFIX}/stats/stats.parquet",
    }))
    by_path = {reference.path: reference for reference in references}
    assert by_path[MANIFEST_PATH].kind == "tombstone_manifest"
    assert by_path[MANIFEST_PATH].declared_digest == manifest.digest()
    assert by_path[SEGMENT_A].kind == "tombstone_segment"
    assert by_path[SEGMENT_A].declared_size == 11
    assert by_path[SEGMENT_A].declared_digest == "a" * 64


def test_gc_traversal_never_returns_an_unexpanded_v2_root():
    manifest = _manifest()

    with pytest.raises(TombstoneManifestV2Error, match="manifest loader"):
        referenced_snapshot_artifacts(
            _v2_snapshot(manifest),
            organization=ORG,
            super_name=SUPER,
            simple_name=TABLE,
        )


def test_gc_storage_loader_checks_manifest_size_before_reading():
    manifest = _manifest()

    class OversizeStorage:
        reads = 0

        def stat_object(self, _path):
            return ObjectMetadata(
                size=MAX_TOMBSTONE_MANIFEST_V2_BYTES + 1,
                version="oversize",
            )

        def read_range(self, _path, _offset, _length, *, expected=None):
            self.reads += 1
            raise AssertionError("oversize manifest must not be read")

    storage = OversizeStorage()
    with pytest.raises(TombstoneManifestV2Error, match="size"):
        referenced_snapshot_artifacts(
            _v2_snapshot(manifest),
            storage,
            organization=ORG,
            super_name=SUPER,
            simple_name=TABLE,
        )
    assert storage.reads == 0


def test_bounded_manifest_reader_rejects_legacy_whole_object_adapter():
    class WholeObjectStorage:
        reads = 0

        def size(self, _path):
            return 1

        def read_bytes(self, _path):
            self.reads += 1
            raise AssertionError("whole-object fallback must not be used")

    storage = WholeObjectStorage()
    with pytest.raises(TombstoneManifestV2Error, match="stat_object and read_range"):
        read_bounded_tombstone_manifest_bytes(storage, MANIFEST_PATH)
    assert storage.reads == 0


def test_bounded_manifest_reader_accepts_production_storage_interface(tmp_path):
    storage = LocalStorage(root=tmp_path)
    payload = _manifest().canonical_bytes()
    storage.write_bytes(MANIFEST_PATH, payload)

    assert read_bounded_tombstone_manifest_bytes(
        storage, MANIFEST_PATH,
    ) == payload


def test_bounded_manifest_reader_never_follows_stat_read_growth():
    manifest = _manifest().canonical_bytes()

    class GrowingStorage:
        range_lengths = []

        def __init__(self):
            self.payload = manifest

        def stat_object(self, _path):
            observed = self.payload
            metadata = ObjectMetadata(
                size=len(observed),
                version=str(hash(observed)),
            )
            self.payload = b"x" * (MAX_TOMBSTONE_MANIFEST_V2_BYTES + 1)
            return metadata

        def read_range(self, _path, _offset, length, *, expected=None):
            self.range_lengths.append(length)
            current = ObjectMetadata(
                size=len(self.payload),
                version=str(hash(self.payload)),
            )
            if current.identity_token() != expected.identity_token():
                raise ObjectIdentityMismatch("conditional version mismatch")
            raise AssertionError("changed manifest must not be returned")

    storage = GrowingStorage()
    with pytest.raises(TombstoneManifestV2Error, match="conditionally read"):
        read_bounded_tombstone_manifest_bytes(storage, MANIFEST_PATH)
    assert storage.range_lengths == [len(manifest)]
    assert storage.range_lengths[0] <= MAX_TOMBSTONE_MANIFEST_V2_BYTES


def test_bounded_manifest_reader_reseals_identity_after_range_read():
    manifest = _manifest().canonical_bytes()

    class PostReadSwapStorage:
        def __init__(self):
            self.payload = manifest

        def stat_object(self, _path):
            return ObjectMetadata(
                size=len(self.payload),
                version=str(hash(self.payload)),
            )

        def read_range(self, _path, offset, length, *, expected=None):
            current = self.stat_object(_path)
            assert current == expected
            payload = self.payload[offset:offset + length]
            self.payload = payload.replace(b'"total_rows":3', b'"total_rows":4')
            return payload

    with pytest.raises(TombstoneManifestV2Error, match="changed during"):
        read_bounded_tombstone_manifest_bytes(
            PostReadSwapStorage(), MANIFEST_PATH,
        )


@pytest.mark.parametrize("mutation", ["noncanonical", "wrong_table", "wrong_count"])
def test_gc_traversal_rejects_untrusted_v2_manifest_roots(mutation):
    manifest = _manifest()
    snapshot = _v2_snapshot(manifest)
    raw = manifest.canonical_bytes()
    if mutation == "noncanonical":
        raw = json.dumps(manifest.to_dict(), indent=2).encode()
    else:
        document = manifest.to_dict()
        if mutation == "wrong_table":
            document["simple_name"] = "other"
        else:
            document["total_rows"] = 4
            document["segments"][1]["rows"] = 3
        replacement = TombstoneManifestV2(
            organization=document["organization"],
            super_name=document["super_name"],
            simple_name=document["simple_name"],
            base_snapshot_version=document["base_snapshot_version"],
            snapshot_version=document["snapshot_version"],
            total_rows=document["total_rows"],
            segments=tuple(TombstoneSegment(**item) for item in document["segments"]),
        )
        raw = replacement.canonical_bytes()
        snapshot["tombstone_digest"] = replacement.digest()

    with pytest.raises(TombstoneManifestV2Error):
        referenced_snapshot_artifacts(
            snapshot,
            organization=ORG,
            super_name=SUPER,
            simple_name=TABLE,
            manifest_loader=lambda _path: raw,
        )


def test_gc_traversal_preserves_legacy_single_vector_shape():
    snapshot = {
        "snapshot_version": 4,
        "resources": [{
            "file": f"{TABLE_PREFIX}/data/part.parquet",
            "file_size": 101,
        }],
        "tombstone": f"{TABLE_PREFIX}/tombstone/deleted.parquet",
        "tombstone_rows": 2,
        "tombstone_digest": "c" * 64,
        "stats_file": None,
    }

    references = referenced_snapshot_artifacts(
        snapshot,
        organization=ORG,
        super_name=SUPER,
        simple_name=TABLE,
    )

    assert [(item.path, item.kind) for item in references] == [
        (f"{TABLE_PREFIX}/data/part.parquet", "data"),
        (f"{TABLE_PREFIX}/tombstone/deleted.parquet", "tombstone"),
    ]
