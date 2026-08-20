"""Adversarial contract tests for the standalone tombstone manifest v2."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import FrozenInstanceError

import pytest

from supertable.data_classes import SuperSnapshot, TombstoneDef
from supertable.tombstone_manifest_v2 import (
    MAX_JSON_EXACT_INTEGER,
    MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS,
    TOMBSTONE_MANIFEST_V2_FORMAT,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
    load_tombstone_manifest_v2,
    validate_logical_storage_path,
    validate_tombstone_manifest_v2,
    validate_tombstone_segment_observation,
)
from supertable.utils.snapshot import complete_snapshot_payload


_DIGEST_A = hashlib.sha256(b"logical segment a").hexdigest()
_DIGEST_B = hashlib.sha256(b"logical segment b").hexdigest()


def _document() -> dict:
    return {
        "format": TOMBSTONE_MANIFEST_V2_FORMAT,
        "organization": "acme",
        "super_name": "sales",
        "simple_name": "events",
        "base_snapshot_version": 4,
        "snapshot_version": 5,
        "total_rows": 3,
        "segments": [
            {
                "file": (
                    "acme/sales/tables/events/tombstone/"
                    "segment-a.parquet"
                ),
                "rows": 1,
                "file_size": 101,
                "digest": _DIGEST_A,
            },
            {
                "file": (
                    "acme/sales/tables/events/tombstone/"
                    "segment-b.parquet"
                ),
                "rows": 2,
                "file_size": 202,
                "digest": _DIGEST_B,
            },
        ],
    }


def _load(document: dict | None = None) -> TombstoneManifestV2:
    return load_tombstone_manifest_v2(document or _document())


def test_manifest_is_frozen_canonical_and_round_trips_with_all_pins() -> None:
    manifest = _load()
    canonical = manifest.canonical_bytes()

    assert b" " not in canonical
    assert b"\n" not in canonical
    assert canonical == manifest.canonical_bytes()
    assert manifest.digest() == hashlib.sha256(canonical).hexdigest()
    assert json.loads(canonical) == manifest.to_dict()
    assert load_tombstone_manifest_v2(
        canonical,
        expected_organization="acme",
        expected_super_name="sales",
        expected_simple_name="events",
        pinned_snapshot_version=5,
        expected_total_rows=3,
        expected_digest=manifest.digest(),
        expected_segment_prefix="acme/sales/tables/events/tombstone",
        require_canonical_json=True,
    ) == manifest
    assert validate_tombstone_manifest_v2(manifest) is manifest

    with pytest.raises(FrozenInstanceError):
        manifest.total_rows = 4  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        manifest.segments[0].rows = 2  # type: ignore[misc]


def test_noncanonical_json_can_validate_but_strict_recovery_can_reject_it() -> None:
    raw = json.dumps(_document(), indent=2).encode("utf-8")
    manifest = load_tombstone_manifest_v2(raw)
    assert manifest.total_rows == 3
    with pytest.raises(TombstoneManifestV2Error, match="canonical form"):
        load_tombstone_manifest_v2(raw, require_canonical_json=True)


def test_manifest_creation_lineage_allows_pure_append_carry_forward() -> None:
    manifest = _load()
    assert load_tombstone_manifest_v2(
        manifest.canonical_bytes(), pinned_snapshot_version=9,
    ) == manifest
    with pytest.raises(TombstoneManifestV2Error, match="created after"):
        load_tombstone_manifest_v2(
            manifest.canonical_bytes(), pinned_snapshot_version=4,
        )


@pytest.mark.parametrize(
    "mutation,error",
    [
        (lambda value: value.update(extra=True), "invalid fields"),
        (lambda value: value.pop("total_rows"), "invalid fields"),
        (
            lambda value: value.update(format="supertable-tombstone-manifest-v3"),
            "format",
        ),
        (lambda value: value.update(organization="ACME"), "organization"),
        (lambda value: value.update(super_name="sales/other"), "super_name"),
        (lambda value: value.update(simple_name=True), "simple_name"),
        (lambda value: value.update(base_snapshot_version=True), "integer"),
        (lambda value: value.update(snapshot_version=4), "immediate successor"),
        (lambda value: value.update(snapshot_version=6), "immediate successor"),
        (lambda value: value.update(total_rows=0), "between 1"),
        (lambda value: value.update(total_rows=4), "sum of segment rows"),
        (lambda value: value.update(segments=[]), "at least one segment"),
        (lambda value: value.update(segments={}), "JSON array"),
    ],
)
def test_manifest_schema_identity_and_counts_are_strict(mutation, error) -> None:
    document = _document()
    mutation(document)
    with pytest.raises(TombstoneManifestV2Error, match=error):
        load_tombstone_manifest_v2(document)


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("rows", True, "integer"),
        ("rows", 0, "between 1"),
        ("file_size", False, "integer"),
        ("file_size", 0, "between 1"),
        ("digest", "A" * 64, "lowercase SHA-256"),
        ("digest", "a" * 63, "lowercase SHA-256"),
        ("file", "https://bucket/segment.parquet", "URI"),
        ("file", "/absolute/segment.parquet", "namespace"),
        ("file", "table/../segment.parquet", "namespace"),
        ("file", "table\\segment.parquet", "canonical"),
        ("file", "table/segment.json", "end with"),
    ],
)
def test_segment_path_count_size_and_digest_fields_are_strict(
    field, value, error,
) -> None:
    document = _document()
    document["segments"][0][field] = value
    with pytest.raises(TombstoneManifestV2Error, match=error):
        load_tombstone_manifest_v2(document)


def test_segments_must_be_unique_and_in_canonical_file_order() -> None:
    duplicate = _document()
    duplicate["segments"][1]["file"] = duplicate["segments"][0]["file"]
    with pytest.raises(TombstoneManifestV2Error, match="unique"):
        load_tombstone_manifest_v2(duplicate)

    reordered = _document()
    reordered["segments"].reverse()
    with pytest.raises(TombstoneManifestV2Error, match="strictly ordered"):
        load_tombstone_manifest_v2(reordered)


def test_manifest_segment_count_has_a_hard_format_bound() -> None:
    document = _document()
    template = document["segments"][0]
    document["segments"] = []
    for index in range(MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS + 1):
        segment = copy.deepcopy(template)
        segment["file"] = f"table/segment-{index:03d}.parquet"
        document["segments"].append(segment)
    document["total_rows"] = len(document["segments"])
    with pytest.raises(TombstoneManifestV2Error, match="too many segments"):
        load_tombstone_manifest_v2(document)


def test_manifest_integer_fields_use_redis_cjson_round_trip_ceiling() -> None:
    document = _document()
    document.update({
        "base_snapshot_version": MAX_JSON_EXACT_INTEGER - 1,
        "snapshot_version": MAX_JSON_EXACT_INTEGER,
        "total_rows": MAX_JSON_EXACT_INTEGER,
        "segments": [{
            "file": (
                "acme/sales/tables/events/tombstone/segment-max.parquet"
            ),
            "rows": MAX_JSON_EXACT_INTEGER,
            "file_size": MAX_JSON_EXACT_INTEGER,
            "digest": _DIGEST_A,
        }],
    })
    assert load_tombstone_manifest_v2(document).total_rows == (
        MAX_JSON_EXACT_INTEGER
    )

    document["segments"][0]["file_size"] = MAX_JSON_EXACT_INTEGER + 1
    with pytest.raises(TombstoneManifestV2Error, match="between"):
        load_tombstone_manifest_v2(document)


def test_raw_json_rejects_duplicate_keys_bom_and_non_json_numbers() -> None:
    canonical = _load().canonical_bytes().decode("utf-8")
    duplicate = canonical.replace(
        '"format":', '"format":"duplicate","format":', 1,
    )
    with pytest.raises(TombstoneManifestV2Error, match="duplicate object key"):
        load_tombstone_manifest_v2(duplicate)
    with pytest.raises(TombstoneManifestV2Error, match="BOM"):
        load_tombstone_manifest_v2(b"\xef\xbb\xbf" + canonical.encode())

    nan_document = canonical.replace('"total_rows":3', '"total_rows":NaN')
    with pytest.raises(TombstoneManifestV2Error, match="non-JSON numeric"):
        load_tombstone_manifest_v2(nan_document)


@pytest.mark.parametrize(
    "expectation,value,error",
    [
        ("expected_organization", "other", "organization"),
        ("expected_super_name", "other", "super_name"),
        ("expected_simple_name", "other", "simple_name"),
        ("expected_total_rows", 2, "total_rows"),
        ("expected_digest", "0" * 64, "canonical SHA-256"),
        (
            "expected_segment_prefix",
            "acme/sales/tables/other/tombstone",
            "expected table prefix",
        ),
    ],
)
def test_snapshot_and_table_expectations_are_fail_closed(
    expectation, value, error,
) -> None:
    with pytest.raises(TombstoneManifestV2Error, match=error):
        load_tombstone_manifest_v2(_document(), **{expectation: value})


def test_segment_observation_checks_size_rows_and_logical_digest() -> None:
    segment = _load().segments[0]
    assert validate_tombstone_segment_observation(
        segment,
        file_size=segment.file_size,
        rows=segment.rows,
        digest=segment.digest,
    ) is segment

    with pytest.raises(TombstoneManifestV2Error, match="file_size"):
        validate_tombstone_segment_observation(
            segment, file_size=102, rows=1, digest=segment.digest,
        )
    with pytest.raises(TombstoneManifestV2Error, match="row count"):
        validate_tombstone_segment_observation(
            segment, file_size=101, rows=2, digest=segment.digest,
        )
    with pytest.raises(TombstoneManifestV2Error, match="logical digest"):
        validate_tombstone_segment_observation(
            segment, file_size=101, rows=1, digest="0" * 64,
        )


def _snapshot_state(
    pointer=None,
    rows=0,
    digest=None,
    *,
    tombstone_format="absent",
) -> dict:
    payload = {
        "snapshot_version": 5,
        "schema": [],
        "resources": [],
        "tombstone": pointer,
        "tombstone_rows": rows,
        "tombstone_digest": digest,
    }
    if tombstone_format != "absent":
        payload["tombstone_format"] = tombstone_format
    return payload


@pytest.mark.parametrize(
    "payload",
    [
        _snapshot_state(),
        _snapshot_state(tombstone_format=1),
        _snapshot_state(tombstone_format=2),
        _snapshot_state("table/tombstone/deleted.parquet", 1, "0" * 64),
        _snapshot_state(
            "table/tombstone/deleted.parquet", 1, "0" * 64,
            tombstone_format=1,
        ),
        _snapshot_state(
            "table/tombstone/manifest.json", 2, "0" * 64,
            tombstone_format=2,
        ),
    ],
)
def test_python_snapshot_completeness_accepts_v1_and_explicit_v2(payload) -> None:
    assert complete_snapshot_payload(payload, expected_version=5) == payload


@pytest.mark.parametrize(
    "payload",
    [
        _snapshot_state(tombstone_format=None),
        _snapshot_state(tombstone_format=True),
        _snapshot_state(tombstone_format="2"),
        _snapshot_state(tombstone_format=3),
        _snapshot_state("table/tombstone/manifest.json", 1, "0" * 64),
        _snapshot_state(
            "table/tombstone/manifest.json", 1, "0" * 64,
            tombstone_format=1,
        ),
        _snapshot_state(
            "table/tombstone/deleted.parquet", 1, "0" * 64,
            tombstone_format=2,
        ),
        _snapshot_state(
            "https://bucket/manifest.json", 1, "0" * 64,
            tombstone_format=2,
        ),
        _snapshot_state(
            "table/tombstone/manifest.json", 0, "0" * 64,
            tombstone_format=2,
        ),
        _snapshot_state(
            "table/tombstone/manifest.json", 1, "A" * 64,
            tombstone_format=2,
        ),
        _snapshot_state(None, 1, None, tombstone_format=2),
        _snapshot_state(None, 0, "0" * 64, tombstone_format=2),
    ],
)
def test_python_snapshot_completeness_rejects_malformed_hybrids(payload) -> None:
    assert complete_snapshot_payload(payload, expected_version=5) is None


def test_malformed_bracketed_path_uses_manifest_error_boundary() -> None:
    with pytest.raises(TombstoneManifestV2Error, match="valid logical"):
        validate_logical_storage_path("//[.json", required_suffix=".json")

    payload = _snapshot_state(
        "//[.json", 1, "0" * 64, tombstone_format=2,
    )
    assert complete_snapshot_payload(payload, expected_version=5) is None


def test_v2_dataclass_fields_are_trailing_for_positional_compatibility() -> None:
    snapshot = SuperSnapshot("sales", "events", 5, ["data.parquet"], {"id"})
    tombstone = TombstoneDef("deleted.parquet", "deleted.parquet", 1, "0" * 64)
    assert snapshot.tombstone_format is None
    assert tombstone.tombstone_format is None

    snapshot.tombstone_format = 2
    tombstone.tombstone_format = 2
    assert snapshot.tombstone_format == tombstone.tombstone_format == 2
