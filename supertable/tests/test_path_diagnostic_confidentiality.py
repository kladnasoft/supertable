from __future__ import annotations

import json
import logging
import os
import threading
import traceback
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import supertable.mirroring.mirror_delta as mirror_delta
import supertable.mirroring.mirror_iceberg as mirror_iceberg
import supertable.mirroring.mirror_parquet as mirror_parquet
import supertable.plan_extender as plan_extender
import supertable.query_plan_manager as query_plan_manager
from supertable.engine.executor import (
    Executor,
    _safe_island_failure_message,
)
from supertable.engine.islanddb import IslandCapability, IslandUnsupportedError
from supertable.engine.plan_stats import PlanStats
from supertable.storage import local_storage
from supertable.storage.storage_factory import get_storage
from supertable.super_table import SuperTable


_REMOTE_ERROR = (
    "https://URL_USER:URL_PASSWORD@storage.invalid/REMOTE_PATH_TOKEN/"
    "data.json?QUERY_TOKEN=yes#FRAGMENT_TOKEN"
)
_NON_HTTP_ERROR = (
    "s3a://URL_USER:URL_PASSWORD@bucket.invalid/REMOTE_PATH_TOKEN/"
    "data.parquet?X-Amz-Signature=QUERY_TOKEN#FRAGMENT_TOKEN"
)


def _assert_no_path_secrets(rendered: str) -> None:
    for secret in (
        "APP_HOME_TOKEN", "PLAN_FILE_TOKEN", "URL_USER", "URL_PASSWORD",
        "REMOTE_PATH_TOKEN", "data.json", "QUERY_TOKEN", "FRAGMENT_TOKEN",
        "/srv/private",
    ):
        assert secret not in rendered


def _assert_safe_exception(caught: pytest.ExceptionInfo[BaseException]) -> str:
    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    _assert_no_path_secrets(rendered)
    assert "bucket.invalid" not in rendered
    assert caught.value.__cause__ is None
    return rendered


def test_query_plan_manager_logs_path_metadata_and_error_type_only(
    monkeypatch, caplog,
) -> None:
    secret_home = "/srv/private/APP_HOME_TOKEN"
    monkeypatch.setattr(query_plan_manager, "get_app_home", lambda: secret_home)
    monkeypatch.setattr(query_plan_manager.os, "makedirs", lambda *_a, **_k: None)
    monkeypatch.setattr(query_plan_manager.os, "chmod", lambda *_a, **_k: None)
    monkeypatch.setattr(
        query_plan_manager.QueryPlanManager,
        "_cleanup_old_plans",
        MagicMock(side_effect=OSError(f"cleanup failed at {_REMOTE_ERROR}")),
    )
    caplog.set_level(logging.DEBUG)

    manager = query_plan_manager.QueryPlanManager(
        "table", "org", "meta", "SELECT 'SQL_LITERAL'",
    )

    assert secret_home in manager.temp_dir
    _assert_no_path_secrets(caplog.text)
    assert "path_bytes=" in caplog.text
    assert "path_sha256=" in caplog.text
    assert "error_type=OSError" in caplog.text


def test_query_plan_cleanup_delete_failure_never_logs_filename_or_os_error(
    monkeypatch, caplog,
) -> None:
    manager = query_plan_manager.QueryPlanManager.__new__(
        query_plan_manager.QueryPlanManager,
    )
    manager.temp_dir = "/srv/private/APP_HOME_TOKEN"
    old_path = manager.temp_dir + "/0_PLAN_FILE_TOKEN_plan.json"
    monkeypatch.setattr(query_plan_manager.glob, "glob", lambda _pattern: [old_path])
    monkeypatch.setattr(
        query_plan_manager.os,
        "remove",
        MagicMock(side_effect=OSError(f"delete failed at {_REMOTE_ERROR}")),
    )
    caplog.set_level(logging.DEBUG)

    manager._cleanup_old_plans(max_keep=0)

    _assert_no_path_secrets(caplog.text)
    assert "path_sha256=" in caplog.text
    assert "error_type=OSError" in caplog.text


def _plan_manager(path: str) -> SimpleNamespace:
    return SimpleNamespace(
        query_plan_path=path,
        query="SELECT id FROM orders WHERE tenant = 'SQL_LITERAL'",
        requested_engine="duckdb",
        query_id="query-id",
        query_hash="query-hash",
        organization="org",
        super_name="lake",
        source_type="api",
        original_table="orders",
        query_observation_store=SimpleNamespace(enabled=False),
        query_profile=None,
    )


def test_plan_extender_read_and_delete_failures_are_path_and_error_safe(
    monkeypatch, caplog,
) -> None:
    secret_path = "/srv/private/APP_HOME_TOKEN/PLAN_FILE_TOKEN.json"
    monkeypatch.setattr(plan_extender.os.path, "isfile", lambda _path: True)
    monkeypatch.setattr(
        plan_extender,
        "_read_local_json",
        MagicMock(side_effect=OSError(f"read failed at {_REMOTE_ERROR}")),
    )
    monkeypatch.setattr(
        plan_extender.os,
        "remove",
        MagicMock(side_effect=OSError(f"delete failed at {_REMOTE_ERROR}")),
    )
    monitor = MagicMock()
    monitor.return_value.__enter__.return_value = MagicMock()
    monkeypatch.setattr(plan_extender, "MonitoringWriter", monitor)
    caplog.set_level(logging.DEBUG)

    plan_extender.extend_execution_plan(
        query_plan_manager=_plan_manager(secret_path),
        role_name="reader",
        timing={},
        plan_stats=PlanStats(),
        status="error",
        message=f"backend failed at {_REMOTE_ERROR}",
        result_shape=(0, 0),
    )

    _assert_no_path_secrets(caplog.text)
    assert "path_sha256=" in caplog.text
    assert caplog.text.count("error_type=OSError") >= 2


def test_plan_extender_success_log_never_names_deleted_plan(
    tmp_path, monkeypatch, caplog,
) -> None:
    plan_path = (
        tmp_path / "APP_HOME_TOKEN" / "PLAN_FILE_TOKEN.json"
    )
    plan_path.parent.mkdir()
    plan_path.write_text(json.dumps({"latency": 0.25}), encoding="utf-8")
    monitor = MagicMock()
    monitor.return_value.__enter__.return_value = MagicMock()
    monkeypatch.setattr(plan_extender, "MonitoringWriter", monitor)
    caplog.set_level(logging.DEBUG)

    plan_extender.extend_execution_plan(
        query_plan_manager=_plan_manager(str(plan_path)),
        role_name="reader",
        timing={},
        plan_stats=PlanStats(),
        status="ok",
        message="",
        result_shape=(1, 1),
    )

    assert not plan_path.exists()
    _assert_no_path_secrets(caplog.text)
    assert "Deleted plan JSON; path_bytes=" in caplog.text


def test_island_capability_failure_retains_only_type_and_digest() -> None:
    hostile = IslandUnsupportedError(
        "Authorization: Bearer HEADER_TOKEN " + _REMOTE_ERROR
    )

    safe = _safe_island_failure_message(
        hostile, phase="stream preparation",
    )
    stats = PlanStats()
    Executor._publish_engine_capability(
        stats, IslandCapability(False, (safe,)),
    )
    rendered = json.dumps(stats.stats)

    _assert_no_path_secrets(rendered)
    assert "HEADER_TOKEN" not in rendered
    assert "Authorization" not in rendered
    assert "error_type=IslandUnsupportedError" in rendered
    assert "diagnostic_id=" in rendered
    assert "diagnostic_bytes=" in rendered


@pytest.mark.parametrize(
    "copy_function, expected_message",
    [
        (
            mirror_parquet._co_locate_or_reuse_path,
            "Failed to copy data file into Parquet table dir",
        ),
        (
            mirror_delta._co_locate_or_reuse_path,
            "Failed to copy data file into Delta table dir",
        ),
    ],
)
def test_mirror_copy_failure_never_discloses_source_path(
    copy_function, expected_message,
) -> None:
    local_directory = "/srv/private/LOCAL_PATH_TOKEN"
    with pytest.raises(RuntimeError) as caught:
        copy_function(object(), local_directory, _NON_HTTP_ERROR)

    rendered = _assert_safe_exception(caught)
    assert expected_message in rendered


def test_delta_invalid_action_never_discloses_log_path() -> None:
    storage = SimpleNamespace(read_text=lambda _path: "[]")
    private_log_path = "/srv/private/LOCAL_PATH_TOKEN"

    with pytest.raises(RuntimeError) as caught:
        mirror_delta._read_delta_actions(storage, private_log_path, 1)

    rendered = _assert_safe_exception(caught)
    assert "Invalid Delta action in commit log" in rendered


@pytest.mark.parametrize("failure_mode", ["invalid", "exception"])
def test_iceberg_canonical_uri_failure_never_retains_backend_value(
    failure_mode,
) -> None:
    backend_message = "Authorization: Bearer " + "HEADER_TOKEN " + _NON_HTTP_ERROR
    private_path = "/srv/private/LOCAL_PATH_TOKEN"

    def canonical_uri(_path):
        if failure_mode == "exception":
            raise RuntimeError(backend_message)
        return private_path

    with pytest.raises(RuntimeError) as caught:
        mirror_iceberg._storage_path_to_uri(
            SimpleNamespace(canonical_uri=canonical_uri),
            private_path,
        )

    rendered = _assert_safe_exception(caught)
    assert "HEADER_TOKEN" not in rendered
    assert "canonical URI" in rendered


class _IcebergVerifyStorage:
    def __init__(self, *, metadata, latest, visible):
        self.metadata = metadata
        self.latest = latest
        self.visible = visible

    def read_bytes(self, path):
        return b"1" if str(path).endswith("version-hint.text") else b""

    def read_json(self, path):
        return self.latest if str(path).endswith("latest.json") else self.metadata

    def canonical_uri(self, _path):
        return "s3://mirror.invalid/<redacted-path>"

    def exists(self, _path):
        return True

    def list_files(self, _path, _pattern):
        return list(self.visible)

    def content_sha256(self, _path):
        return 1, "different"


@pytest.mark.parametrize("failure_mode", ["missing", "invalid_seal", "bad_seal"])
def test_iceberg_verification_never_discloses_missing_or_sealed_path(
    failure_mode,
) -> None:
    import hashlib

    organization = "org"
    super_name = "lake"
    table_name = "orders"
    source = _NON_HTTP_ERROR
    base = os.path.join(organization, super_name, "iceberg", table_name)
    data_path = os.path.join(
        base,
        "data",
        mirror_iceberg._hashlib.md5(
            source.encode("utf-8"), usedforsecurity=False,
        ).hexdigest()[:8] + "_" + source.rstrip("/").split("/")[-1],
    )
    empty_digest = hashlib.sha256(b"").hexdigest()
    if failure_mode == "missing":
        visible = set()
        data_seals = {}
    else:
        visible = {data_path}
        data_seals = {
            data_path: (
                "invalid"
                if failure_mode == "invalid_seal"
                else {"size": 1, "sha256": "expected"}
            ),
        }
    metadata = {
        "format-version": 2,
        "location": "s3://mirror.invalid/<redacted-path>",
        "properties": {
            "supertable.manifest-sha256": empty_digest,
            "supertable.manifest-list-sha256": empty_digest,
            "supertable.data-seals": json.dumps(data_seals),
        },
        "snapshots": [{"manifest-list": "s3://mirror.invalid/list"}],
    }
    latest = {
        "version": 1,
        "metadata": "/srv/private/LOCAL_PATH_TOKEN/metadata.json",
        "manifest": "/srv/private/LOCAL_PATH_TOKEN/manifest.avro",
        "manifest_list": "/srv/private/LOCAL_PATH_TOKEN/list.avro",
    }
    storage = _IcebergVerifyStorage(
        metadata=metadata,
        latest=latest,
        visible=visible,
    )
    table = SimpleNamespace(
        organization=organization,
        super_name=super_name,
        storage=storage,
    )

    with pytest.raises(RuntimeError) as caught:
        mirror_iceberg.verify_iceberg_table(
            table,
            table_name,
            {"resources": [{"file": source, "file_size": 1}]},
        )

    _assert_safe_exception(caught)


def test_storage_factory_unknown_kind_never_echoes_input() -> None:
    with pytest.raises(ValueError) as caught:
        get_storage(_NON_HTTP_ERROR)

    rendered = _assert_safe_exception(caught)
    assert "Unknown storage type" in rendered


@pytest.mark.parametrize("empty", [False, True])
def test_simple_snapshot_failure_never_echoes_path(empty) -> None:
    table = SuperTable.__new__(SuperTable)
    table.storage = SimpleNamespace(
        exists=lambda _path: empty,
        size=lambda _path: 0,
    )
    expected = ValueError if empty else FileNotFoundError

    with pytest.raises(expected) as caught:
        table.read_simple_table_snapshot(_NON_HTTP_ERROR)

    _assert_safe_exception(caught)


def test_local_publication_type_failure_never_echoes_physical_path(tmp_path) -> None:
    publication = local_storage._BatchedPublication(
        "/srv/private/LOCAL_PATH_TOKEN/object.parquet",
        "/srv/private/LOCAL_PATH_TOKEN",
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY)
    try:
        with pytest.raises(OSError) as caught:
            publication.pin_published_file(directory_fd)
    finally:
        os.close(directory_fd)

    _assert_safe_exception(caught)


def test_local_durability_anchor_failure_never_echoes_path() -> None:
    storage = local_storage.LocalStorage.__new__(local_storage.LocalStorage)
    storage._durability_lock = threading.RLock()
    private_anchor = "/srv/private/LOCAL_PATH_TOKEN/anchor"
    private_publication = "/srv/private/LOCAL_PATH_TOKEN/publication"
    storage._deepest_durable_anchor_locked = (
        lambda _path: private_anchor
    )
    storage._valid_durable_handle_locked = lambda _path: None

    with pytest.raises(OSError) as caught:
        storage._fsync_logical_publication(private_publication)

    _assert_safe_exception(caught)
