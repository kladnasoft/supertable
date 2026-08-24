from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import inspect
from types import SimpleNamespace
from uuid import UUID

import duckdb
import pytest

from supertable.data_classes import RbacViewDef, TombstoneDef
from supertable import data_reader as data_reader_module
from supertable.data_reader import _odata_identity_binding
from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb_engine import (
    _append_protected_odata_identity,
    _prepare_protected_odata_query,
)
from supertable.engine.engine_common import (
    SOURCE_FILE_COL,
    create_rbac_view,
    create_tombstone_view,
)
from supertable.row_identity import (
    ODATA_INTERNAL_ROWID_COLUMN,
    snapshot_proves_stable_rowids,
)
from supertable.odata_continuation import (
    bind_odata_continuation_boundary,
    validate_odata_continuation_boundary,
)
from supertable.utils.sql_parser import SQLParser


def _modern_snapshot(**updates):
    snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "int64", "tenant": "int64"},
        "resources": [
            {"file": "org/lake/tables/t/data/a.parquet", "rows": 2},
            {"file": "org/lake/tables/t/data/b.parquet", "rows": 1},
        ],
        "rowid_high_watermark": 5,
    }
    snapshot.update(updates)
    return snapshot


def test_stable_rowid_contract_is_modern_local_and_manifest_complete():
    assert snapshot_proves_stable_rowids(_modern_snapshot()) is True
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(rowid_high_watermark=None),
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(rowid_high_watermark=2),
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(), {"payload": {"_linked_share": "link-1"}},
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(resources=[{"file": "x", "rows": True}]),
    ) is False


def test_tombstone_view_exposes_only_fixed_odata_identity_when_opted_in():
    con = duckdb.connect()
    con.execute(
        'CREATE TABLE src(id BIGINT, tenant BIGINT, "__rowid__" BIGINT, '
        '"__timestamp__" BIGINT)'
    )
    con.execute("INSERT INTO src VALUES (10, 1, 101, 1), (20, 2, 202, 1)")

    create_tombstone_view(con, "src", "ordinary", None)
    assert [row[0] for row in con.execute("DESCRIBE ordinary").fetchall()] == [
        "id", "tenant",
    ]

    create_tombstone_view(
        con,
        "src",
        "keyed",
        None,
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
    )
    assert con.execute(
        f'SELECT id, "{ODATA_INTERNAL_ROWID_COLUMN}" FROM keyed ORDER BY id'
    ).fetchall() == [(10, 101), (20, 202)]
    described = [row[0] for row in con.execute("DESCRIBE keyed").fetchall()]
    assert "__rowid__" not in described
    assert "__timestamp__" not in described


def test_protected_identity_survives_rbac_after_row_and_column_filters():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE keyed(id BIGINT, tenant BIGINT, secret VARCHAR, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute("INSERT INTO keyed VALUES (10, 1, 'a', 101), (20, 2, 'b', 202)")
    create_rbac_view(
        con,
        "keyed",
        "scoped",
        RbacViewDef(
            allowed_columns=["id", "tenant"],
            excluded_columns=["tenant"],
            where_clause='"tenant" = 1',
        ),
        required_internal_columns=[ODATA_INTERNAL_ROWID_COLUMN],
    )
    assert con.execute("SELECT * FROM scoped").fetchall() == [(10, 101)]
    assert [row[0] for row in con.execute("DESCRIBE scoped").fetchall()] == [
        "id", ODATA_INTERNAL_ROWID_COLUMN,
    ]


def test_protected_identity_is_projected_only_after_tombstone_filtering():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE src(id BIGINT, "__rowid__" BIGINT, '
        f'"{SOURCE_FILE_COL}" VARCHAR)'
    )
    con.execute("INSERT INTO src VALUES (10, 101, 'f'), (20, 202, 'f')")
    con.execute('CREATE TABLE dv("__file__" VARCHAR, "__rowid__" BIGINT)')
    con.execute("INSERT INTO dv VALUES ('f', 101)")
    create_tombstone_view(
        con,
        "src",
        "live",
        TombstoneDef(
            tombstone_path="validated-by-private-table",
            expected_rows=1,
            resource_keys=("f",),
            snapshot_resource_keys=("f",),
        ),
        dv_table="dv",
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
    )
    assert con.execute("SELECT * FROM live").fetchall() == [(20, 202)]


def test_continuation_seek_runs_over_tombstone_then_rbac_protected_rows():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE src(name VARCHAR, tenant BIGINT, "__rowid__" BIGINT, '
        f'"{SOURCE_FILE_COL}" VARCHAR)'
    )
    con.execute(
        "INSERT INTO src VALUES "
        "('a', 1, 1, 'f'), "
        "('a', 1, 2, 'f'), "  # deleted, otherwise after the boundary
        "('a', 2, 3, 'f'), "  # unauthorized, otherwise after the boundary
        "('b', 1, 4, 'f'), "
        "('c', 2, 5, 'f')"    # unauthorized later order value
    )
    con.execute('CREATE TABLE dv("__file__" VARCHAR, "__rowid__" BIGINT)')
    con.execute("INSERT INTO dv VALUES ('f', 2)")
    create_tombstone_view(
        con,
        "src",
        "live",
        TombstoneDef(
            tombstone_path="validated-by-private-table",
            expected_rows=1,
            resource_keys=("f",),
            snapshot_resource_keys=("f",),
        ),
        dv_table="dv",
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
    )
    create_rbac_view(
        con,
        "live",
        "scoped",
        RbacViewDef(
            allowed_columns=["name", "tenant"],
            excluded_columns=["tenant"],
            where_clause='"tenant" = 1',
        ),
        required_internal_columns=[ODATA_INTERNAL_ROWID_COLUMN],
    )
    sql, parameters = _prepare_protected_odata_query(
        "SELECT name FROM scoped ORDER BY name ASC LIMIT 50",
        ODATA_INTERNAL_ROWID_COLUMN,
        _boundary([{
            "column": "name",
            "direction": "asc",
            "value": {"type": "string", "value": "a"},
        }], row_identity=1),
    )
    # The seek is evaluated over ``scoped``.  Neither the deleted id=2 nor
    # tenant-2 ids 3/5 can influence or enter the continuation result.
    assert con.execute(sql, parameters).fetchall() == [("b", 4)]


def test_protected_query_adds_identity_as_final_deterministic_tie_breaker():
    sql = _append_protected_odata_identity(
        'SELECT "name" FROM reflected ORDER BY "name" DESC LIMIT 3 OFFSET 2',
        ODATA_INTERNAL_ROWID_COLUMN,
    )
    assert (
        f'SELECT "name", "{ODATA_INTERNAL_ROWID_COLUMN}" FROM reflected '
        f'ORDER BY "name" DESC NULLS LAST, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" ASC NULLS LAST LIMIT 3 OFFSET 2'
        == sql
    )
    with pytest.raises(RuntimeError):
        _append_protected_odata_identity(
            "SELECT count(*) FROM reflected",
            ODATA_INTERNAL_ROWID_COLUMN,
        )
    with pytest.raises(RuntimeError):
        create_tombstone_view(
            duckdb.connect(),
            "missing",
            "bad",
            SimpleNamespace(tombstone_path=None),
            preserve_rowid_as="__supertable_attacker_column__",
        )


def test_odata_identity_sql_contract_accepts_only_one_direct_row_projection():
    parser = SQLParser(
        "lake",
        'SELECT "id" FROM "orders" WHERE "id" > 1 ORDER BY "id" LIMIT 5',
        "duckdb",
    )
    assert _odata_identity_binding(parser) == "orders"

    for sql in (
        "SELECT count(*) FROM orders",
        "SELECT o.id FROM orders o JOIN users u ON o.id = u.id",
        "SELECT id + 1 FROM orders",
        "SELECT id FROM lake",
    ):
        candidate = SQLParser("lake", sql, "duckdb")
        with pytest.raises(ValueError):
            _odata_identity_binding(candidate)


def _boundary(order, row_identity=2):
    return validate_odata_continuation_boundary({
        "version": 1,
        "order": order,
        "row_identity": row_identity,
    })


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {},
        {"version": True, "order": [], "row_identity": 1},
        {"version": 1, "order": (), "row_identity": 1},
        {"version": 1, "order": [], "row_identity": 0},
        {
            "version": 1,
            "order": [{
                "column": "name",
                "direction": "ascending",
                "value": {"type": "string", "value": "a"},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "int64", "value": "01"},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "float64", "value": 1},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "float64", "value": float("inf")},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "null", "value": None},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [
                {
                    "column": "Name",
                    "direction": "asc",
                    "value": {"type": "null"},
                },
                {
                    "column": "name",
                    "direction": "desc",
                    "value": {"type": "null"},
                },
            ],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "created",
                "direction": "asc",
                "value": {
                    "type": "datetime", "value": "2026-08-24T12:00:00",
                },
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "created",
                "direction": "asc",
                "value": {
                    "type": "timestamp",
                    "value": "2026-08-24T12:00:00+00:00",
                },
            }],
            "row_identity": 1,
        },
    ],
)
def test_odata_continuation_rejects_malformed_or_ambiguous_state(raw):
    if raw is None:
        assert validate_odata_continuation_boundary(raw) is None
    else:
        with pytest.raises(ValueError):
            validate_odata_continuation_boundary(raw)


def test_odata_continuation_typed_values_bind_without_text_interpolation():
    boundary = _boundary([
        {
            "column": "nullable",
            "direction": "asc",
            "value": {"type": "null"},
        },
        {
            "column": "local_time",
            "direction": "asc",
            "value": {
                "type": "timestamp", "value": "2026-08-24T12:34:56.123456",
            },
        },
        {
            "column": "instant",
            "direction": "desc",
            "value": {
                "type": "datetime", "value": "2026-08-24T12:34:56+00:00",
            },
        },
        {
            "column": "amount",
            "direction": "asc",
            "value": {"type": "decimal", "value": "1234.50"},
        },
        {
            "column": "uid",
            "direction": "asc",
            "value": {
                "type": "uuid",
                "value": "12345678-1234-5678-9234-567812345678",
            },
        },
        {
            "column": "payload",
            "direction": "desc",
            "value": {"type": "binary", "value": "AAEC/w=="},
        },
    ])
    assert boundary is not None
    values = [term.value.value for term in boundary.order]
    assert values == [
        None,
        datetime(2026, 8, 24, 12, 34, 56, 123456),
        datetime(2026, 8, 24, 12, 34, 56, tzinfo=timezone.utc),
        Decimal("1234.50"),
        UUID("12345678-1234-5678-9234-567812345678"),
        b"\x00\x01\x02\xff",
    ]
    con = duckdb.connect()
    row = con.execute(
        "SELECT ?::TIMESTAMP, ?::TIMESTAMPTZ, ?::DECIMAL(10, 2), "
        "?::UUID, ?::BLOB",
        values[1:],
    ).fetchone()
    assert row[0] == values[1]
    assert row[1] == values[2]
    assert row[2:] == tuple(values[3:])


@pytest.mark.parametrize(
    ("sql", "order", "row_identity", "expected_ids"),
    [
        (
            "SELECT name FROM reflected ORDER BY name ASC LIMIT 50",
            [{
                "column": "name", "direction": "asc",
                "value": {"type": "string", "value": "a"},
            }],
            2,
            [3, 4, 5, 6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name DESC LIMIT 50",
            [{
                "column": "name", "direction": "desc",
                "value": {"type": "string", "value": "b"},
            }],
            4,
            [1, 2, 3, 5, 6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name ASC LIMIT 50",
            [{
                "column": "name", "direction": "asc",
                "value": {"type": "null"},
            }],
            5,
            [6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name DESC LIMIT 50",
            [{
                "column": "name", "direction": "desc",
                "value": {"type": "null"},
            }],
            5,
            [6],
        ),
    ],
)
def test_odata_seek_handles_ties_nulls_and_both_directions(
    sql, order, row_identity, expected_ids,
):
    prepared, parameters = _prepare_protected_odata_query(
        sql,
        ODATA_INTERNAL_ROWID_COLUMN,
        _boundary(order, row_identity=row_identity),
    )
    assert prepared.count("NULLS LAST") == len(order) + 1
    con = duckdb.connect()
    # Prove the query's explicit semantics do not inherit mutable connection
    # defaults from a prior tenant/query.
    con.execute("SET default_null_order='NULLS_FIRST'")
    con.execute(
        f'CREATE TABLE reflected(name VARCHAR, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute(
        "INSERT INTO reflected VALUES "
        "('a', 1), ('a', 2), ('a', 3), ('b', 4), (NULL, 5), (NULL, 6)"
    )
    rows = con.execute(prepared, parameters).fetchall()
    assert [row[-1] for row in rows] == expected_ids


def test_odata_seek_mixed_direction_tuple_uses_identity_only_for_exact_ties():
    boundary = _boundary([
        {
            "column": "name", "direction": "asc",
            "value": {"type": "string", "value": "a"},
        },
        {
            "column": "score", "direction": "desc",
            "value": {"type": "int64", "value": "2"},
        },
    ], row_identity=2)
    sql, parameters = _prepare_protected_odata_query(
        "SELECT name, score FROM reflected "
        "ORDER BY name ASC, score DESC LIMIT 50",
        ODATA_INTERNAL_ROWID_COLUMN,
        boundary,
    )
    assert "'a'" not in sql
    assert parameters == ["a", "a", 2, 2, 2]
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE reflected(name VARCHAR, score BIGINT, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute(
        "INSERT INTO reflected VALUES "
        "('a', 3, 1), ('a', 2, 2), ('a', 2, 3), ('a', 1, 4), "
        "('b', 9, 5), (NULL, 9, 6)"
    )
    assert [row[-1] for row in con.execute(sql, parameters).fetchall()] == [
        3, 4, 5, 6,
    ]


def test_odata_boundary_must_exactly_match_reparsed_order_and_has_no_offset():
    parser = SQLParser(
        "lake",
        "SELECT name FROM orders ORDER BY name DESC LIMIT 5",
        "duckdb",
    )
    matching = _boundary([{
        "column": "NAME", "direction": "desc",
        "value": {"type": "string", "value": "a"},
    }])
    rebound = bind_odata_continuation_boundary(parser._parsed, matching)
    assert rebound is not None and rebound.order[0].column == "name"

    for boundary in (
        _boundary([]),
        _boundary([{
            "column": "other", "direction": "desc",
            "value": {"type": "string", "value": "a"},
        }]),
        _boundary([{
            "column": "name", "direction": "asc",
            "value": {"type": "string", "value": "a"},
        }]),
    ):
        with pytest.raises(ValueError, match="order"):
            bind_odata_continuation_boundary(parser._parsed, boundary)

    offset_parser = SQLParser(
        "lake",
        "SELECT name FROM orders ORDER BY name DESC LIMIT 5 OFFSET 1",
        "duckdb",
    )
    with pytest.raises(ValueError, match="OFFSET"):
        bind_odata_continuation_boundary(offset_parser._parsed, matching)

    for sql in (
        "SELECT name FROM orders ORDER BY lower(name)",
        "SELECT name FROM orders ORDER BY name NULLS FIRST",
        "SELECT name FROM orders ORDER BY name, name",
    ):
        with pytest.raises(ValueError):
            _odata_identity_binding(SQLParser("lake", sql, "duckdb"))


def test_ordinary_sql_stream_cannot_activate_continuation_or_hidden_identity():
    assert "continuation_boundary" not in inspect.signature(
        data_reader_module.query_sql_stream
    ).parameters
    assert "continuation_boundary" in inspect.signature(
        data_reader_module.query_odata_sql_stream
    ).parameters
    boundary = _boundary([])
    with pytest.raises(ValueError, match="trusted OData stream"):
        data_reader_module.query_sql_stream(
            organization="org",
            super_name="lake",
            sql="SELECT name FROM orders",
            engine=Engine.DUCKDB,
            role_name="reader",
            max_total_rows=10,
            timeout_sec=10,
            _odata_identity=True,
            _odata_continuation_boundary=boundary,
        )
    with pytest.raises(RuntimeError, match="present in user SQL"):
        _append_protected_odata_identity(
            f'SELECT "{ODATA_INTERNAL_ROWID_COLUMN}" FROM reflected',
            ODATA_INTERNAL_ROWID_COLUMN,
        )
