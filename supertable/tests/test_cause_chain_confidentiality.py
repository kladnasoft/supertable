"""Hostile checks for fixed public wrappers around untrusted failures."""

from __future__ import annotations

import json
import traceback
from types import SimpleNamespace

import pytest

import supertable.redis_catalog as redis_catalog
import supertable.storage.local_storage as local_storage_module
from supertable.data_writer import DataWriter
from supertable.engine import file_cache
from supertable.engine.engine_config import normalize_auto_routing_policy
from supertable.engine.range_cache import RangeCache, RangeCacheUnavailable
from supertable.quality.checker import _count_value
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import (
    ObjectMetadata,
    storage_error_type,
    validate_range_request,
)
from supertable.utils.spark_security import _endpoint


_SECRET = (
    "https://user:CAUSE_CHAIN_SECRET@backend.invalid/private?sig=sentinel"
)


def _assert_suppressed(caught: pytest.ExceptionInfo[BaseException]) -> None:
    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert _SECRET not in rendered
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True


def test_data_writer_mirror_lookup_suppresses_backend_cause() -> None:
    class Catalog:
        def get_mirrors(self, *_args):
            raise RuntimeError(_SECRET)

    writer = DataWriter.__new__(DataWriter)
    writer.catalog = Catalog()
    writer.super_table = SimpleNamespace(
        organization="tenant", super_name="lake",
    )

    with pytest.raises(RuntimeError) as caught:
        writer._get_enabled_mirrors("write")

    _assert_suppressed(caught)


class _PoisonInteger:
    def __int__(self) -> int:
        raise ValueError(_SECRET)


def test_numeric_policy_and_range_wrappers_suppress_caller_values() -> None:
    with pytest.raises(ValueError) as policy_error:
        normalize_auto_routing_policy([{
            "min_bytes": _PoisonInteger(),
            "max_bytes": None,
            "engine": "islanddb",
        }])
    _assert_suppressed(policy_error)

    with pytest.raises(ValueError) as range_error:
        validate_range_request(_PoisonInteger(), 1, None)  # type: ignore[arg-type]
    _assert_suppressed(range_error)


def test_quality_count_wrapper_suppresses_hostile_conversion_cause() -> None:
    class PoisonValue:
        def __str__(self) -> str:
            raise ValueError(_SECRET)

    with pytest.raises(ValueError) as caught:
        _count_value(PoisonValue(), "quick profile count")

    _assert_suppressed(caught)


def test_spark_endpoint_wrapper_suppresses_url_parser_cause() -> None:
    with pytest.raises(ValueError) as caught:
        _endpoint("https://storage.invalid:" + _SECRET)

    _assert_suppressed(caught)


def test_redis_json_wrapper_suppresses_poisoned_decoder_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PoisonDecoder:
        def __init__(self, **_kwargs) -> None:
            pass

        def decode(self, _raw: str):
            raise json.JSONDecodeError(_SECRET, _SECRET, 0)

    monkeypatch.setattr(redis_catalog.json, "JSONDecoder", PoisonDecoder)

    with pytest.raises(ValueError) as caught:
        redis_catalog._strict_json_object_with_tokens("{}", field="root")

    _assert_suppressed(caught)


def test_file_cache_metadata_wrapper_suppresses_backend_value() -> None:
    with pytest.raises(file_cache.FileCacheUnavailable) as caught:
        file_cache._metadata_size(SimpleNamespace(size=_SECRET))

    _assert_suppressed(caught)


def test_range_cache_stat_wrapper_suppresses_backend_cause(tmp_path) -> None:
    class PoisonStorage:
        def cache_namespace(self):
            return {"backend": "poison-test"}

        def is_local_storage(self) -> bool:
            return False

        def stat_object(self, _key: str):
            raise RuntimeError(_SECRET)

    cache = RangeCache(
        PoisonStorage(), "tenant", root=str(tmp_path), max_bytes=4096,
    )
    with pytest.raises(RangeCacheUnavailable) as caught:
        cache.open("table/data.parquet")

    _assert_suppressed(caught)


def test_sealed_fallback_suppresses_object_path_and_adapter_cause(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = LocalStorage(tmp_path)
    monkeypatch.setattr(
        storage,
        "stat_object",
        lambda _path: ObjectMetadata(size=1, version="immutable-v1"),
    )

    def unsupported_download(*_args, **_kwargs):
        raise NotImplementedError(_SECRET)

    monkeypatch.setattr(storage, "download_to_file", unsupported_download)
    monkeypatch.setattr(storage, "read_bytes", lambda _path: b"")

    with pytest.raises(OSError) as caught:
        storage.content_sha256(_SECRET)

    _assert_suppressed(caught)


def test_local_json_missing_path_suppresses_os_context(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_secret = "private/CAUSE_CHAIN_SECRET.json"
    storage = LocalStorage(tmp_path)
    monkeypatch.setattr(local_storage_module.os.path, "isfile", lambda _path: True)

    def disappear(_path: str) -> int:
        raise FileNotFoundError(path_secret)

    monkeypatch.setattr(local_storage_module.os.path, "getsize", disappear)
    monkeypatch.setattr(local_storage_module.time, "sleep", lambda _delay: None)

    with pytest.raises(FileNotFoundError) as caught:
        storage.read_json(path_secret)

    rendered = "".join(traceback.format_exception(caught.value))
    assert "CAUSE_CHAIN_SECRET" not in rendered
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True


def test_storage_error_type_rejects_dynamic_class_name() -> None:
    class_secret = "StorageBackend_DYNAMIC_CLASS_SECRET"
    hostile_error = type(class_secret, (OSError,), {})("safe-message")

    assert storage_error_type(hostile_error) == "OSError"
    assert class_secret not in storage_error_type(hostile_error)
