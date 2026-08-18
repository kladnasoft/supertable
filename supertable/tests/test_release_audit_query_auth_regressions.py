"""Focused regressions for the 2.4 release-audit query/auth findings."""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import supertable.data_reader as data_reader
import supertable.meta_reader as meta_reader
import supertable.rbac.access_control as access_control
from supertable.config import settings as settings_module
from supertable.data_classes import Reflection, SuperSnapshot, TableDefinition
from supertable.engine import duckdb_engine
from supertable.engine.engine_config import resolve_engine_bundle
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor
from supertable.engine.plan_stats import PlanStats
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.utils.sql_parser import SQLParser


def test_same_named_child_creation_is_rejected_case_insensitively():
    parent = SimpleNamespace(super_name="Sales")

    with pytest.raises(ValueError, match="can't match"):
        SimpleTable(parent, "sales", create_if_missing=True)


def test_writer_validation_rejects_case_only_aggregate_name():
    from supertable.data_writer import DataWriter

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(super_name="Sales")

    with pytest.raises(ValueError, match="can't match"):
        writer.validation(pl.DataFrame({"id": [1]}), "sALES", [], None, False)


def _install_inclusion_role(monkeypatch, tables):
    role = {
        "role": "reader",
        "role_name": "included-reader",
        "tables": tables,
    }

    class StaticRoleManager:
        def __init__(self, **_kwargs):
            pass

        def get_role_by_name(self, _name):
            return role

    monkeypatch.setattr(access_control, "RoleManager", StaticRoleManager)


def test_aggregate_read_authorizes_every_expanded_child(monkeypatch):
    _install_inclusion_role(
        monkeypatch,
        {"public": {"columns": ["id"], "filters": ["*"]}},
    )
    aggregate = TableDefinition("shop", "shop", "shop", ["id"])

    with pytest.raises(PermissionError, match="secret"):
        access_control.restrict_read_access(
            "shop",
            "org",
            "included-reader",
            [aggregate],
            [aggregate],
            aggregate_children={("shop", "shop"): ("public", "secret")},
        )


def test_aggregate_read_uses_conservative_child_column_policy(monkeypatch):
    _install_inclusion_role(
        monkeypatch,
        {
            "one": {"columns": ["id", "name"], "filters": ["*"]},
            "two": {
                "columns": ["id", "name"],
                "exclude_columns": ["name"],
                "filters": ["*"],
            },
        },
    )
    aggregate = TableDefinition("shop", "shop", "shop", [])

    views = access_control.restrict_read_access(
        "shop",
        "org",
        "included-reader",
        [aggregate],
        [aggregate],
        aggregate_children={("shop", "shop"): ("one", "two")},
    )

    assert views["shop"].allowed_columns == ["id", "name"]
    assert views["shop"].excluded_columns == ["name"]


def test_inclusion_only_metadata_exposes_parent_through_visible_child(monkeypatch):
    reader = meta_reader.MetaReader.__new__(meta_reader.MetaReader)
    reader.super_table = SimpleNamespace(super_name="shop", organization="org")
    reader.catalog = MagicMock()
    reader._get_all_tables = lambda: ["visible", "hidden"]
    reader.catalog.get_root.return_value = {"version": 7, "ts": 1}
    snapshot = json.dumps({
        "schema": {"id": "int"},
        "resources": [{"file": "f", "rows": 3, "file_size": 10}],
    })
    reader.catalog.r.mget.return_value = [snapshot]
    checked = []
    context = SimpleNamespace(
        role_type=access_control.RoleType.READER,
        role_info={"role_id": "reader-id"},
        fingerprint="policy-1",
    )

    def check_meta_access(**kwargs):
        table = kwargs["table_name"]
        checked.append(table)
        if table != "visible":
            raise PermissionError(table)
        return context, {"columns": ["id"], "filters": ["*"]}

    monkeypatch.setattr(meta_reader, "check_meta_access", check_meta_access)
    monkeypatch.setattr(meta_reader, "_super_meta_cache_ttl_s", lambda: 0.0)

    result = reader.get_super_meta("included-reader")

    assert [item["name"] for item in result["super"]["tables"]] == ["visible"]
    assert result["super"]["rows"] == 3
    assert checked == ["visible", "hidden"]
    assert "shop" not in checked


@pytest.mark.parametrize(
    "metadata",
    [
        {"enabled": False, "expires_ms": 0},
        {"enabled": "false", "expires_ms": 0},
        {"enabled": True, "expires_ms": 999},
        {"enabled": True, "expires_ms": "invalid"},
        {"enabled": True, "expires_ms": -1},
    ],
)
def test_boolean_token_validation_fails_closed_for_state_and_expiry(
    monkeypatch, metadata,
):
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = MagicMock()
    catalog.r.hget.return_value = json.dumps(metadata)
    monkeypatch.setattr("supertable.redis_catalog._now_ms", lambda: 1000)

    assert catalog.validate_auth_token("org", "plaintext-token") is False


def test_boolean_token_validation_accepts_only_current_enabled_token(monkeypatch):
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = MagicMock()
    catalog.r.hget.return_value = json.dumps({
        "enabled": True,
        "expires_ms": 1001,
        "username": "reader",
    })
    monkeypatch.setattr("supertable.redis_catalog._now_ms", lambda: 1000)

    assert catalog.validate_auth_token("org", "plaintext-token") is True


@pytest.mark.parametrize("raw", ["maybe", "enabled", "2", "true-ish"])
def test_malformed_redis_security_boolean_is_rejected(monkeypatch, raw):
    monkeypatch.setenv("AUDIT_REDIS_BOOL", raw)

    with pytest.raises(ValueError, match="AUDIT_REDIS_BOOL"):
        settings_module._env_bool_strict("AUDIT_REDIS_BOOL", False)


@pytest.mark.parametrize("raw", ["not-a-port", "0", "65536"])
def test_malformed_redis_port_is_rejected(monkeypatch, raw):
    monkeypatch.setenv("AUDIT_REDIS_PORT", raw)

    with pytest.raises(ValueError, match="AUDIT_REDIS_PORT"):
        settings_module._env_int_strict(
            "AUDIT_REDIS_PORT", 6379, minimum=1, maximum=65535,
        )


def _redis_options(**overrides):
    values = {
        "host": "redis.example",
        "port": 6380,
        "db": 0,
        "username": "reader",
        "password": "secret",
        "use_ssl": True,
        "ssl_ca_certs": "/etc/ssl/certs/ca-certificates.crt",
        "is_sentinel": False,
        "sentinel_hosts": [],
        "sentinel_master": "mymaster",
        "sentinel_password": "sentinel-secret",
        "sentinel_strict": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_direct_redis_tls_requires_ca_validation_and_hostname(monkeypatch):
    from supertable import redis_connector

    captured = {}
    client = object()

    def fake_redis(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(redis_connector.redis, "Redis", fake_redis)

    assert redis_connector._build_redis_client(
        _redis_options(), True,
    ) is client
    assert captured["ssl"] is True
    assert captured["ssl_cert_reqs"] == "required"
    assert captured["ssl_check_hostname"] is True
    assert captured["ssl_ca_certs"].endswith("ca-certificates.crt")


def test_sentinel_and_master_share_strict_tls_policy(monkeypatch):
    from supertable import redis_connector

    sentinel_args = {}
    master_args = {}
    master = SimpleNamespace(ping=lambda: True)

    class FakeSentinel:
        def __init__(self, _hosts, **kwargs):
            sentinel_args.update(kwargs)

        def master_for(self, _name, **kwargs):
            master_args.update(kwargs)
            return master

    monkeypatch.setattr(redis_connector, "Sentinel", FakeSentinel)
    result = redis_connector._build_redis_client(
        _redis_options(
            is_sentinel=True,
            sentinel_hosts=[("sentinel.example", 26379)],
        ),
        True,
    )

    assert result is master
    for options in (
        sentinel_args["sentinel_kwargs"], sentinel_args, master_args,
    ):
        assert options["ssl"] is True
        assert options["ssl_cert_reqs"] == "required"
        assert options["ssl_check_hostname"] is True
        assert options["ssl_ca_certs"].endswith("ca-certificates.crt")


@pytest.mark.parametrize("legacy_section", ["lite", "pro"])
def test_engine_config_reads_24_sections_without_restoring_public_modes(
    legacy_section,
):
    document = {
        legacy_section: {
            "duckdb_memory_limit": "3GB",
            "duckdb_threads": "2",
        },
        "engine_lite_max_bytes": "1234",
        "auto_policy": [
            {"min_bytes": 0, "max_bytes": None, "engine": legacy_section},
        ],
    }
    catalog = SimpleNamespace(get_engine_config=lambda _org: document)

    configs, policy = resolve_engine_bundle("org", catalog)

    assert set(configs) == {"duckdb"}
    assert configs["duckdb"].duckdb_memory_limit == "3GB"
    assert configs["duckdb"].duckdb_threads == 2
    assert configs["duckdb"].engine_island_min_bytes == 1234
    assert policy[0].engine is Engine.DUCKDB


def test_current_duckdb_config_wins_over_legacy_sections():
    catalog = SimpleNamespace(get_engine_config=lambda _org: {
        "duckdb": {"duckdb_memory_limit": "4GB"},
        "lite": {"duckdb_memory_limit": "1GB"},
        "pro": {"duckdb_memory_limit": "8GB"},
    })

    configs, _policy = resolve_engine_bundle("org", catalog)

    assert configs["duckdb"].duckdb_memory_limit == "4GB"


def test_outer_limit_is_clamped_by_server_ceiling():
    bounded = data_reader._ensure_sql_limit(
        "SELECT * FROM events LIMIT 999999999", 999999999,
    )

    assert bounded == "SELECT * FROM events LIMIT 5000"


def test_nested_limit_does_not_bypass_outer_limit():
    bounded = data_reader._ensure_sql_limit(
        "SELECT * FROM (SELECT * FROM events LIMIT 1) AS nested", 999999999,
    )

    assert bounded.endswith("LIMIT 5000")


def test_query_sql_rejects_result_over_serialized_byte_cap(monkeypatch):
    reader = MagicMock()
    stream = duckdb_engine.ArrowBatchStream.from_table(
        pa.table({"payload": ["x" * 1024]}), max_chunksize=1,
    )
    reader.execute_stream.return_value = (
        stream,
        data_reader.Status.OK,
        None,
    )
    monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)
    monkeypatch.setattr(
        data_reader,
        "settings",
        SimpleNamespace(
            SUPERTABLE_MAX_LIMIT=5000,
            SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES=128,
        ),
    )

    with pytest.raises(RuntimeError, match="MAX_SERIALIZED_RESULT_BYTES"):
        data_reader.query_sql(
            "org", "shop", "SELECT * FROM events", 100,
            Engine.DUCKDB, "reader",
        )
    reader.execute.assert_not_called()
    assert reader.execute_stream.call_args.kwargs["max_batch_rows"] == 1
    assert stream.closed is True


def test_duckdb_public_response_fetches_one_wide_row_before_byte_guard(tmp_path):
    source = tmp_path / "wide.parquet"
    payload = "x" * 100_000
    pq.write_table(
        pa.table({
            "payload": [payload] * 256,
            "__rowid__": list(range(1, 257)),
            "__timestamp__": [1] * 256,
        }),
        source,
        compression="zstd",
    )
    snapshot = SuperSnapshot(
        super_name="shop",
        simple_name="events",
        simple_version=1,
        files=[str(source)],
        columns={"payload", "__rowid__", "__timestamp__"},
        resource_keys=[str(source)],
        snapshot_resource_keys=[str(source)],
    )
    reflection = Reflection(
        "local", source.stat().st_size, 1, [snapshot],
    )
    parser = SQLParser("shop", "SELECT payload FROM events", "duckdb")
    manager = SimpleNamespace(
        temp_dir=str(tmp_path),
        query_plan_path=str(tmp_path / "wide-plan.json"),
    )

    stream = duckdb_engine.DuckDB().execute_stream(
        reflection,
        parser,
        manager,
        lambda _event: None,
        max_batch_rows=1,
    )
    try:
        first = next(stream)
        assert first.num_rows == 1
        assert 100_000 <= first.nbytes < 200_000
    finally:
        stream.close()


def test_query_sql_stream_retains_only_exact_json_safe_values(monkeypatch):
    reader = MagicMock()
    stream = duckdb_engine.ArrowBatchStream.from_table(pa.table({
        "number": pa.array([float("nan")], type=pa.float64()),
        "when": pa.array(
            [datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "amount": pa.array([Decimal("12.30")], type=pa.decimal128(8, 2)),
        "payload": pa.array([b"abc"], type=pa.binary()),
        "day": pa.array([date(2026, 8, 18)], type=pa.date32()),
        "wall_clock": pa.array(
            [datetime(2026, 8, 18, 9, 0)], type=pa.timestamp("us"),
        ),
    }))
    reader.execute_stream.return_value = (
        stream, data_reader.Status.OK, None,
    )
    monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)

    _columns, rows, _meta = data_reader.query_sql(
        "org", "shop", "SELECT * FROM events", 100,
        Engine.DUCKDB, "reader",
    )

    assert rows == [[
        None,
        "2026-08-18T09:00:00.000000Z",
        "12.30",
        "b'abc'",
        "2026-08-18",
        "2026-08-18T09:00:00.000000",
    ]]
    json.dumps(rows, ensure_ascii=False, allow_nan=False)
    assert stream.closed is True


def test_query_sql_timestamptz_rows_and_metadata_are_session_timezone_stable(
        monkeypatch,
):
    observed = []
    for zone in ("UTC", "Europe/Budapest", "America/New_York"):
        connection = duckdb_engine.duckdb.connect()
        try:
            connection.execute(f"SET TimeZone = '{zone}'")
            table = connection.sql(
                "SELECT TIMESTAMPTZ '2026-01-01 10:00:00+00' AS observed_at"
            ).to_arrow_table()
        finally:
            connection.close()

        reader = MagicMock()
        stream = duckdb_engine.ArrowBatchStream.from_table(table)
        reader.execute_stream.return_value = (
            stream, data_reader.Status.OK, None,
        )
        monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)

        columns, rows, metadata = data_reader.query_sql(
            "org", "shop", "SELECT observed_at FROM events", 100,
            Engine.DUCKDB, "reader",
        )
        observed.append((columns, rows, metadata))

    assert observed == [
        (
            ["observed_at"],
            [["2026-01-01T10:00:00.000000Z"]],
            [{
                "name": "observed_at",
                "type": "timestamp[us, tz=UTC]",
                "nullable": True,
            }],
        ),
    ] * 3


def test_data_reader_public_duckdb_stream_uses_normal_bounded_preflight(monkeypatch):
    parser = MagicMock()
    parser.original_query = "SELECT * FROM events\nLIMIT 5000"
    parser.get_table_tuples.return_value = []
    parser.get_physical_tables.return_value = []
    monkeypatch.setattr(data_reader, "SQLParser", MagicMock(return_value=parser))
    monkeypatch.setattr(data_reader, "get_storage", lambda: MagicMock())
    monkeypatch.setattr(data_reader, "restrict_read_access", lambda **_kwargs: {})
    # ``supertable.engine`` is the historical public Engine enum alias, so
    # resolve the implementation submodule explicitly instead of asking
    # pytest's dotted-name walker to traverse that alias.
    observation_module = importlib.import_module(
        "supertable.engine.query_observations"
    )
    monkeypatch.setattr(
        observation_module,
        "QueryObservationStore",
        lambda _org: SimpleNamespace(enabled=False),
    )
    qpm = MagicMock(query_id="qid", query_hash="hash", source_type="")
    monkeypatch.setattr(data_reader, "QueryPlanManager", MagicMock(return_value=qpm))
    plan_stats = MagicMock()
    monkeypatch.setattr(data_reader, "PlanStats", MagicMock(return_value=plan_stats))
    timer = MagicMock(timings=[])
    monkeypatch.setattr(data_reader, "Timer", MagicMock(return_value=timer))
    reflection = Reflection(
        "local", 1, 1,
        [SuperSnapshot("shop", "events", 1, ["f.parquet"], {"id"})],
    )
    estimator = MagicMock()
    estimator.estimate.return_value = reflection
    monkeypatch.setattr(data_reader, "DataEstimator", MagicMock(return_value=estimator))
    stream = SimpleNamespace(schema=["id"], closed=False, close=MagicMock())
    executor = MagicMock()
    executor.execute_stream.return_value = (stream, "duckdb")
    monkeypatch.setattr(data_reader, "Executor", MagicMock(return_value=executor))
    monkeypatch.setattr(data_reader, "extend_execution_plan", MagicMock())

    reader = data_reader.DataReader("shop", "org", "SELECT * FROM events")
    result, status, message = reader.execute_stream(
        "reader", engine=Engine.DUCKDB,
    )

    assert isinstance(result, data_reader._MonitoredResultStream)
    assert result._inner is stream
    assert result.schema == stream.schema
    assert status is data_reader.Status.OK
    assert message is None
    executor.execute.assert_not_called()
    executor.execute_stream.assert_called_once()
    assert data_reader.SQLParser.call_args.kwargs["query"].endswith("LIMIT 5000")
    # Finalize while monkeypatches are live; abandoning the wrapper would defer
    # monitoring/engine cleanup to __del__ after fixture teardown.
    result.close()
    stream.close.assert_called_once()


def _duckdb_stream_fixture(tmp_path):
    source = tmp_path / "events.parquet"
    temp_dir = tmp_path / "duckdb-tmp"
    temp_dir.mkdir()
    writer = duckdb_engine.duckdb.connect()
    try:
        writer.execute(
            "COPY (SELECT i::BIGINT AS id, (i + 1)::BIGINT AS __rowid__, "
            "1::BIGINT AS __timestamp__ FROM range(100000) t(i)) "
            "TO ? (FORMAT PARQUET)",
            [str(source)],
        )
    finally:
        writer.close()
    snapshot = SuperSnapshot(
        super_name="shop",
        simple_name="events",
        simple_version=1,
        files=[str(source)],
        columns={"id", "__rowid__", "__timestamp__"},
        resource_keys=[str(source)],
        snapshot_resource_keys=[str(source)],
    )
    reflection = Reflection(
        "local", source.stat().st_size, 1, [snapshot],
    )
    parser = SQLParser(
        "shop", "SELECT id FROM events ORDER BY id", "duckdb",
    )
    manager = SimpleNamespace(
        temp_dir=str(temp_dir),
        query_plan_path=str(tmp_path / "plan.json"),
    )
    return reflection, parser, manager


def test_duckdb_arrow_stream_owns_views_and_watchdog_until_early_close(
    tmp_path, monkeypatch,
):
    reflection, parser, manager = _duckdb_stream_fixture(tmp_path)
    timers = []

    class ControlledTimer:
        def __init__(self, seconds, callback):
            self.seconds = seconds
            self.callback = callback
            self.daemon = False
            self.started = False
            self.cancelled = False
            timers.append(self)

        def start(self):
            self.started = True

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(duckdb_engine.threading, "Timer", ControlledTimer)
    executor = duckdb_engine.DuckDB()

    stream = executor.execute_stream(
        reflection,
        parser,
        manager,
        lambda _event: None,
        timeout_sec=5,
    )

    assert timers[0].started is True
    assert timers[0].cancelled is False
    assert stream.closed is False
    assert executor._con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'st_%' OR view_name LIKE 'tomb_%'"
    ).fetchone()[0] == 2

    stream.close()

    assert stream.closed is True
    assert timers[0].cancelled is True
    assert executor._con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'st_%' OR view_name LIKE 'tomb_%'"
    ).fetchone()[0] == 0


def test_duckdb_arrow_stream_timeout_interrupts_and_releases_views(
    tmp_path, monkeypatch,
):
    reflection, parser, manager = _duckdb_stream_fixture(tmp_path)
    timers = []

    class ControlledTimer:
        def __init__(self, _seconds, callback):
            self.callback = callback
            self.daemon = False
            self.cancelled = False
            timers.append(self)

        def start(self):
            pass

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(duckdb_engine.threading, "Timer", ControlledTimer)
    executor = duckdb_engine.DuckDB()
    stream = executor.execute_stream(
        reflection,
        parser,
        manager,
        lambda _event: None,
        timeout_sec=5,
    )

    timers[0].callback()

    with pytest.raises(TimeoutError, match="timed out"):
        next(stream)
    assert stream.closed is True
    assert timers[0].cancelled is True
    assert executor._con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'st_%' OR view_name LIKE 'tomb_%'"
    ).fetchone()[0] == 0


def test_executor_auto_to_duckdb_stream_returns_incremental_arrow_batches(
    tmp_path, monkeypatch,
):
    reflection, parser, manager = _duckdb_stream_fixture(tmp_path)
    executor = Executor(storage=None, organization="org")
    executor.duckdb_exec = duckdb_engine.DuckDB()
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(executor, "_get_file_cache", lambda: None)
    monkeypatch.setattr(
        executor, "_auto_pick", lambda *_args, **_kwargs: Engine.DUCKDB,
    )

    stream, used = executor.execute_stream(
        Engine.AUTO,
        reflection,
        parser,
        manager,
        Timer(),
        PlanStats(),
        "",
    )
    try:
        first = next(stream)
        assert used == "duckdb"
        assert first.num_rows == 256
        assert first.column(0)[0].as_py() == 0
        assert stream.closed is False
    finally:
        stream.close()
    assert stream.closed is True


def test_duckdb_deadline_interrupts_query_cursor(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value = connection
    connection.fetchdf.return_value = pd.DataFrame({"id": [1]})
    root = MagicMock()
    root.cursor.return_value = connection
    engine = duckdb_engine.DuckDB()
    monkeypatch.setattr(engine, "_get_connection", lambda **_kwargs: root)
    monkeypatch.setattr(engine, "_ensure_httpfs", lambda *_args: None)
    monkeypatch.setattr(duckdb_engine, "hashed_table_name", lambda *_args: "ref")
    monkeypatch.setattr(
        duckdb_engine,
        "create_reflection_view_with_presign_retry",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(duckdb_engine, "create_tombstone_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        duckdb_engine,
        "rewrite_query_with_hashed_tables",
        lambda *_args, **_kwargs: "SELECT * FROM ref",
    )
    monkeypatch.setattr(duckdb_engine, "apply_runtime_pragmas", lambda *_args: None)

    timers = []

    class ImmediateTimer:
        def __init__(self, seconds, callback):
            self.seconds = seconds
            self.callback = callback
            self.daemon = False
            self.cancelled = False
            timers.append(self)

        def start(self):
            self.callback()

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(duckdb_engine.threading, "Timer", ImmediateTimer)
    parser = MagicMock(original_query="SELECT * FROM events")
    parser.get_table_tuples.return_value = [
        TableDefinition("shop", "events", "events", ["id"]),
    ]
    reflection = Reflection(
        "local", 1, 1,
        [SuperSnapshot("shop", "events", 1, ["f.parquet"], {"id"})],
    )
    manager = SimpleNamespace(temp_dir="/tmp", query_plan_path="/tmp/p.json")

    with pytest.raises(TimeoutError, match="timed out"):
        engine.execute(
            reflection,
            parser,
            manager,
            lambda _event: None,
            timeout_sec=0.01,
        )

    connection.interrupt.assert_called_once_with()
    assert timers[0].seconds == 0.01
    assert timers[0].cancelled is True
