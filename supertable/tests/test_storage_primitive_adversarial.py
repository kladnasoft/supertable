"""Adversarial contracts for staging, pipe, and restore storage primitives.

These tests deliberately exercise SDK boundaries directly.  Service-layer
validation is not an acceptable substitute because SDK users and background
workers can call these primitives without passing through the HTTP API.
"""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pyarrow as pa
import pytest

import supertable.staging_area as staging_module
from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.staging_area import Staging
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_pipe import SuperPipe


def _catalog() -> RedisCatalog:
    return RedisCatalog(
        redis_client=fakeredis.FakeStrictRedis(decode_responses=True),
    )


def _seed_root(catalog: RedisCatalog) -> None:
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )


def _stage_shell(*, events: list[str] | None = None) -> Staging:
    """Build a stage without constructor I/O and with an empty live file map."""

    stage = Staging.__new__(Staging)
    stage.organization = "acme"
    stage.super_name = "lake"
    stage.staging_name = "uploads"
    stage._is_manager = False
    stage.base_staging_dir = "acme/lake/staging"
    stage.stage_dir = "acme/lake/staging/uploads"
    stage.files_index_path = "acme/lake/staging/uploads_files.json"
    stage.storage = MagicMock()
    stage.catalog = MagicMock()
    stage.catalog.get_staging_meta.return_value = {
        "organization": "acme",
        "super_name": "lake",
        "staging_name": "uploads",
        "path": stage.stage_dir,
        "files": {},
    }

    def with_lock(operation):
        if events is not None:
            events.append("lock-acquired")
        return operation("stage-lock-token")

    stage._with_lock = with_lock
    return stage


def _successor_shell() -> SimpleTable:
    table = SimpleTable.__new__(SimpleTable)
    table.super_table = SimpleNamespace(
        organization="acme",
        super_name="lake",
    )
    table.identity = "tables"
    table.simple_name = "events"
    table.simple_dir = "acme/lake/tables/events"
    table.data_dir = f"{table.simple_dir}/data"
    table.snapshot_dir = f"{table.simple_dir}/snapshots"
    table.storage = MagicMock()
    table.storage.exists.return_value = True
    table.catalog = MagicMock()
    table.catalog.acquire_simple_lock.return_value = "table-lock-token"
    table.catalog.get_leaf.return_value = {
        "version": 3,
        "ts": 10,
        "path": f"{table.snapshot_dir}/head.json",
        "payload": {
            "snapshot_version": 3,
            "schema": {"id": "long"},
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "_row_filter": None,
        },
    }
    table.catalog.commit_snapshot.return_value = (4, 8)
    table.catalog.get_mirrors.return_value = []
    return table


def _source_snapshot(resource_path: str) -> dict:
    return {
        "snapshot_version": 1,
        "schema": {"id": "long"},
        "resources": [{
            "file": resource_path,
            "rows": 1,
            "file_size": 128,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }


def _pipe_shell() -> SuperPipe:
    pipe = SuperPipe.__new__(SuperPipe)
    pipe.organization = "acme"
    pipe.super_name = "lake"
    pipe.staging_name = "uploads"
    pipe.catalog = MagicMock()
    pipe.catalog.get_pipe_meta.return_value = None
    pipe.catalog.list_pipe_metas.return_value = []
    pipe.catalog.upsert_pipe_meta.return_value = True
    pipe._check_target_access = MagicMock()
    pipe._with_lock = lambda operation: operation("stage-lock-token")
    return pipe


def test_staging_rows_require_read_permission_not_meta_permission(monkeypatch):
    stage = _stage_shell()
    meta_gate = MagicMock()
    read_gate = MagicMock(side_effect=PermissionError("READ denied"))
    monkeypatch.setattr(staging_module, "check_meta_access", meta_gate)
    # ``check_read_access`` is the missing safe primitive this contract asks
    # the SDK to expose and use. ``raising=False`` keeps the failure behavioural
    # while production is still missing it.
    monkeypatch.setattr(
        staging_module, "check_read_access", read_gate, raising=False,
    )

    with pytest.raises(PermissionError, match="READ denied"):
        stage.read_parquet_files("meta-only-role")

    read_gate.assert_called()
    meta_gate.assert_not_called()


def test_staging_read_reauthorizes_after_waiting_for_stage_lock(monkeypatch):
    events: list[str] = []
    stage = _stage_shell(events=events)

    def fresh_role() -> str:
        events.append("fresh-authorization")
        return "live-role"

    read_gate = MagicMock()
    monkeypatch.setattr(
        staging_module, "check_read_access", read_gate, raising=False,
    )
    monkeypatch.setattr(staging_module, "check_meta_access", MagicMock())

    assert stage.read_parquet_files(
        "stale-role",
        authorization_callback=fresh_role,
    ) == []
    assert events == ["lock-acquired", "fresh-authorization"]
    assert read_gate.call_args_list[-1].kwargs["role_name"] == "live-role"


@pytest.mark.parametrize("stat_mode", ["error", "identity-less"])
def test_staging_upload_does_not_publish_or_orphan_without_stable_identity(
    tmp_path,
    stat_mode,
):
    catalog = _catalog()
    _seed_root(catalog)
    storage = LocalStorage(root=tmp_path)

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stage = Staging(
            organization="acme",
            super_name="lake",
            staging_name="uploads",
        )

    if stat_mode == "error":
        stat_result = OSError("stat unavailable")
    else:
        stat_result = ObjectMetadata(size=1)

    with (
        patch("supertable.staging_area.check_create_access"),
        patch.object(storage, "stat_object", side_effect=[stat_result]),
    ):
        with pytest.raises((OSError, RuntimeError), match="stat|identity"):
            stage.save_as_parquet(
                role_name="creator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="upload.parquet",
            )

    assert catalog.get_staging_meta("acme", "lake", "uploads") is None
    physical_stage = tmp_path / stage.stage_dir
    assert not physical_stage.exists() or not list(
        physical_stage.glob("*.parquet")
    )


@pytest.mark.parametrize(
    "resource_path",
    [
        pytest.param(
            os.path.abspath(
                "acme/lake/tables/events/data/part.parquet",
            ),
            id="absolute-logical-key",
        ),
        pytest.param(
            "acme/lake/tables/events/data/./part.parquet",
            id="dot-component",
        ),
        pytest.param(
            "acme/lake/tables/events/data/nested/../part.parquet",
            id="parent-component-even-when-normalized-inside",
        ),
    ],
)
def test_successor_rejects_noncanonical_resource_paths_before_storage_write(
    resource_path,
):
    table = _successor_shell()

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="path|canonical|absolute"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=_source_snapshot(resource_path),
            )

    table.storage.write_json.assert_not_called()
    table.catalog.commit_snapshot.assert_not_called()


def test_successor_requires_mirror_workflow_before_writing_snapshot():
    table = _successor_shell()
    table.catalog.get_mirrors.return_value = ["PARQUET"]
    table.catalog.commit_snapshot.side_effect = RuntimeError(
        "Corrupt mirror configuration during snapshot publication",
    )

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(RuntimeError, match="mirror"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=_source_snapshot(
                    "acme/lake/tables/events/data/part.parquet",
                ),
            )

    # A caller may eventually provide a complete mirror-publication workflow.
    # Until then, fail before leaving an immutable orphan in storage.
    table.catalog.get_mirrors.assert_called_once_with("acme", "lake")
    table.storage.write_json.assert_not_called()
    table.catalog.commit_snapshot.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"overwrite_columns": [f"column_{i}" for i in range(4097)]},
            id="column-fanout",
        ),
        pytest.param(
            {"overwrite_columns": ["x" * 1025]},
            id="oversized-column-name",
        ),
        pytest.param(
            {"user_hash": "u" * 4097},
            id="oversized-user-identity",
        ),
        pytest.param(
            {"enabled": "true"},
            id="non-boolean-enabled",
        ),
    ],
)
def test_pipe_payload_bounds_are_enforced_before_catalog_mutation(overrides):
    pipe = _pipe_shell()
    arguments = {
        "role_name": "writer",
        "pipe_name": "daily",
        "simple_name": "events",
        "user_hash": "user-1",
        "overwrite_columns": ["id"],
        "enabled": True,
    }
    arguments.update(overrides)

    with pytest.raises((TypeError, ValueError)):
        pipe.create(**arguments)

    pipe.catalog.upsert_pipe_meta.assert_not_called()


def test_pipe_fallback_scan_clamps_attacker_controlled_count(monkeypatch):
    catalog = _catalog()
    observed_counts: list[int] = []

    def bounded_scan(*, cursor, match, count):
        observed_counts.append(count)
        return 0, []

    monkeypatch.setattr(catalog.r, "scan", bounded_scan)
    assert catalog.list_pipes(
        "acme", "lake", "uploads", count=10**9,
    ) == []
    assert observed_counts
    assert max(observed_counts) <= 1000


def test_staging_index_cannot_reference_missing_metadata():
    catalog = _catalog()
    catalog.r.sadd(RK.staging_index("acme", "lake"), "uploads")

    with pytest.raises(RuntimeError, match="missing|disagree|corrupt"):
        catalog.list_stagings("acme", "lake")


def test_pipe_index_cannot_reference_missing_metadata():
    catalog = _catalog()
    catalog.r.sadd(
        RK.pipe_index("acme", "lake", "uploads"), "daily",
    )

    with pytest.raises(RuntimeError, match="missing|disagree|corrupt"):
        catalog.list_pipes("acme", "lake", "uploads")
