"""Write/snapshot boundary tests for immutable provider object identities."""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pytest

from supertable.data_classes import PredInterval, ResourceObjectSeal
from supertable import processing
from supertable.processing import (
    resource_object_seal,
    write_parquet_and_collect_resources,
)
from supertable.storage.storage_interface import ObjectMetadata


class _WriteStorage:
    def __init__(self, metadata_factory):
        self.payloads = {}
        self.metadata_factory = metadata_factory
        self.stat_calls = []

    def is_local_storage(self):
        return False

    def makedirs(self, _path):
        return None

    def write_bytes(self, path, data):
        self.payloads[path] = bytes(data)

    def stat_object(self, path):
        self.stat_calls.append(path)
        return self.metadata_factory(path, self.payloads[path])


def _write_with(storage):
    resources = []
    with (
        patch("supertable.processing._get_storage", return_value=storage),
        patch(
            "supertable.processing.generate_filename",
            return_value="unique-data.parquet",
        ),
    ):
        write_parquet_and_collect_resources(
            pl.DataFrame({"id": [2, 1], "payload": ["b", "a"]}),
            ["id"],
            "table/data",
            resources,
            compression_level=1,
        )
    assert len(resources) == 1
    return resources[0]


def test_writer_records_exact_stable_remote_object_seal():
    storage = _WriteStorage(lambda _path, payload: ObjectMetadata(
        size=len(payload),
        version="generation-42",
        etag="etag-42",
        last_modified_ns=123456789,
        checksum_sha256="a" * 64,
    ))

    resource = _write_with(storage)

    assert storage.stat_calls == [resource["file"]]
    assert resource["file_size"] == len(storage.payloads[resource["file"]])
    assert resource_object_seal(resource) == ResourceObjectSeal(
        size=resource["file_size"],
        version="generation-42",
        etag="etag-42",
        last_modified_ns=123456789,
        checksum_sha256="a" * 64,
    )


@pytest.mark.parametrize(
    "metadata_factory",
    [
        lambda _path, payload: ObjectMetadata(
            size=len(payload) + 1, version="wrong-sized-version",
        ),
        lambda _path, payload: ObjectMetadata(size=len(payload)),
    ],
    ids=["size-mismatch", "no-stable-identity"],
)
def test_writer_omits_unsafe_object_seal_without_corrupting_resource(
    metadata_factory,
):
    storage = _WriteStorage(metadata_factory)

    resource = _write_with(storage)

    payload = storage.payloads[resource["file"]]
    assert resource["file_size"] == len(payload)
    assert resource["rows"] == 2
    assert resource["columns"] == 2
    assert "object_seal" not in resource
    assert resource_object_seal(resource) is None


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {},
        {"size": 100},
        {"size": 101, "version": "v1"},
        {"size": 100, "version": "v1", "checksum_sha256": "BAD"},
        {"size": True, "version": "v1"},
    ],
)
def test_malformed_or_legacy_resource_object_seal_fails_open(raw):
    resource = {"file": "f.parquet", "file_size": 100}
    if raw is not None:
        resource["object_seal"] = raw

    assert resource_object_seal(resource) is None


def test_local_writer_does_not_add_redundant_provider_seal(tmp_path, monkeypatch):
    from supertable.storage.local_storage import LocalStorage

    storage = LocalStorage()
    monkeypatch.setattr(processing, "_storage", storage)
    resources = []

    write_parquet_and_collect_resources(
        pl.DataFrame({"id": [1]}), [], str(tmp_path), resources,
    )

    assert len(resources) == 1
    assert "object_seal" not in resources[0]


def _seal_world_resources(snapshots):
    for snapshot in snapshots:
        for index, resource in enumerate(snapshot["payload"]["resources"]):
            resource["object_seal"] = {
                "size": resource["file_size"],
                "version": f"{snapshot['table_name']}-v{index}",
            }


def test_estimator_carries_object_seals_for_exact_survivors_only(monkeypatch):
    from supertable.tests.test_data_estimator_join_pruning import (
        SUPER,
        _make_estimator,
    )
    category_key = (SUPER, "category")

    estimator = _make_estimator(
        monkeypatch,
        join_edges=[],
        predicate_constraints={
            category_key: [{
                "category_id": PredInterval(
                    "numeric", 51, True, 51, True,
                ),
            }],
        },
    )
    snapshots = estimator._collect_snapshots_from_redis("org", SUPER)
    _seal_world_resources(snapshots)
    estimator._collect_snapshots_from_redis = (
        lambda organization, super_name: list(snapshots)
    )

    reflection = estimator.estimate()
    category = next(
        snapshot for snapshot in reflection.supers
        if snapshot.simple_name == "category"
    )

    assert category.resource_keys == ["category/f10.parquet"]
    assert set(category.resource_object_seals) == set(category.resource_keys)
    seal = category.resource_object_seals["category/f10.parquet"]
    assert seal == ResourceObjectSeal(size=1000, version="category-v10")


def test_estimator_unseals_malformed_and_conflicting_duplicate_keys(monkeypatch):
    from supertable.tests.test_data_estimator_join_pruning import (
        SUPER,
        _make_estimator,
    )

    estimator = _make_estimator(
        monkeypatch, join_edges=[], predicate_constraints={},
    )
    snapshots = estimator._collect_snapshots_from_redis("org", SUPER)
    category_payload = next(
        snapshot["payload"] for snapshot in snapshots
        if snapshot["table_name"] == "category"
    )
    resources = category_payload["resources"]
    resources[0]["object_seal"] = {"size": 1000}  # size is not identity
    resources[1]["object_seal"] = {"size": 1000, "version": "v1"}
    duplicate = dict(resources[1])
    duplicate["object_seal"] = {"size": 1000, "version": "v2"}
    resources.append(duplicate)
    estimator._collect_snapshots_from_redis = (
        lambda organization, super_name: list(snapshots)
    )

    reflection = estimator.estimate()
    category = next(
        snapshot for snapshot in reflection.supers
        if snapshot.simple_name == "category"
    )

    assert "category/f00.parquet" not in category.resource_object_seals
    assert "category/f01.parquet" not in category.resource_object_seals
