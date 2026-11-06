"""``MetaReader.get_odata_table_schema``: the keyed schema Core's OData serves.

dataisland-core's OData service lists a table in ``$metadata``, and serves its
pages, only through this method, and keys every entity on ``__rowid__``. So it
answers two questions at once: can this snapshot's rowids be trusted as keys
(``verify_stable_identity``), and which columns may this role see, the
``__rowid__`` the service selects among them.

These run the real writer, the catalog's Lua on fakeredis, LOCAL storage and
the real RBAC. The fixture's table has a deletion vector: four rowids issued,
three rows live.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from supertable import meta_reader as meta_reader_module
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.meta_reader import MetaReader
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.redis_keys import meta_leaf, rbac_role_doc
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "odata"
ROLE = "superadmin"

ROLES = {
    "narrow": {"orders": {"columns": ["__rowid__", "id", "name"]}},
    "masked": {"orders": {"columns": ["*"], "exclude_columns": ["secret"]}},
    "no_rowid": {"orders": {"columns": ["id", "name"]}},
    "rowid_excluded": {"orders": {"columns": ["*"], "exclude_columns": ["__rowid__"]}},
    "denied": {"*": {"columns": ["*"]}, "orders": {"access": "deny"}},
    # RoleManager stores these as given.
    "null_columns": {"orders": {"columns": None}},
    "null_exclusions": {"orders": {"columns": ["*"], "exclude_columns": None}},
    "string_exclusion": {"orders": {"columns": ["*"], "exclude_columns": "secret"}},
}


@pytest.fixture
def reader():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw.write(role_name=ROLE, simple_name="orders", overwrite_columns=[],
             data=pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"],
                                "secret": [7, 8, 9]}))
    dw.write(role_name=ROLE, simple_name="orders", overwrite_columns=["id"],
             data=pl.DataFrame({"id": [2], "name": ["b2"], "secret": [6]}))
    roles = RoleManager(super_name=SUPER, organization=ORG, actor_role_name=ROLE)
    for role_name, tables in ROLES.items():
        roles.create_role({"role": "reader", "role_name": role_name, "tables": tables})
    return MetaReader(SUPER, ORG)


def _leaf():
    return RedisCatalog().get_leaf(ORG, SUPER, "orders")


def _republish(**changes):
    """Publish the current snapshot again with *changes*; None drops a key."""
    leaf = _leaf()
    payload = dict(leaf["payload"])
    for key, value in changes.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    RedisCatalog().set_leaf_payload_cas(ORG, SUPER, "orders", payload, leaf["path"])


def test_the_fixture_is_a_proven_table_with_a_deletion_vector(reader):
    payload = _leaf()["payload"]
    assert payload["rowid_high_watermark"] == 4
    assert payload["tombstone_rows"] == 1
    assert {"__rowid__", "__timestamp__"} <= set(payload["schema"])


def test_a_proven_table_lists_every_column_but_the_system_ones(reader):
    assert reader.get_odata_table_schema("orders", ROLE) == [
        {"id": "Int64", "name": "String", "secret": "Int64"}]


def test_a_path_only_leaf_reads_the_snapshot_from_storage(reader):
    RedisCatalog().set_leaf_path_cas(ORG, SUPER, "orders", _leaf()["path"])
    assert "payload" not in _leaf()
    assert reader.get_odata_table_schema("orders", ROLE) == [
        {"id": "Int64", "name": "String", "secret": "Int64"}]


def test_a_leaf_whose_snapshot_file_is_gone_is_not_keyed(reader):
    RedisCatalog().set_leaf_path_cas(
        ORG, SUPER, "orders", _leaf()["path"] + ".missing")
    assert reader.get_odata_table_schema("orders", ROLE) is None


def test_a_column_scoped_role_sees_only_its_columns(reader):
    expected = [{"id": "Int64", "name": "String"}]
    assert reader.get_odata_table_schema("orders", "narrow") == expected
    assert reader.get_odata_table_schema("orders", "masked") == expected


def test_a_null_column_list_is_read_as_the_read_path_reads_it(reader):
    """columns None is every column, as the SQL read path takes it, and
    exclude_columns None excludes none. Both were iterated as stored, and the
    TypeError failed the role's whole $metadata in Core's OData service."""
    every_column = [{"id": "Int64", "name": "String", "secret": "Int64"}]
    assert reader.get_odata_table_schema("orders", "null_columns") == every_column
    assert reader.get_odata_table_schema("orders", "null_exclusions") == every_column
    _, status, _ = DataReader(
        super_name=SUPER, organization=ORG, query="SELECT secret FROM orders",
    ).execute(role_name="null_columns")
    assert status == Status.OK


def test_an_exclusion_that_is_not_a_list_leaves_the_table_out(reader):
    """A string was read letter by letter: "secret" excluded s, e, c, r and t,
    and the column it named was listed."""
    assert "orders" in reader.get_tables("string_exclusion")
    assert reader.get_odata_table_schema("orders", "string_exclusion") is None


def test_a_column_list_the_read_path_refuses_leaves_the_table_out(
        reader, hermetic_fakeredis):
    """RoleManager cannot store ``columns: 5``, but a role document can hold
    it; normalize_allowed_columns refuses it, and iterating it raised."""
    role_id = RoleManager(super_name=SUPER, organization=ORG).get_role_by_name(
        "narrow")["role_id"]
    hermetic_fakeredis.hset(rbac_role_doc(ORG, SUPER, role_id), "tables",
                            json.dumps({"orders": {"columns": 5}}))
    assert reader.get_odata_table_schema("orders", "narrow") is None


def test_a_role_that_cannot_read_rowid_gets_no_keyed_schema(reader):
    """The service selects __rowid__ with every page, so a role whose policy
    does not grant it cannot be served keyed pages of the table at all."""
    assert reader.get_odata_table_schema("orders", "no_rowid") is None
    assert reader.get_odata_table_schema("orders", "rowid_excluded") is None
    # Plain metadata is not column-scoped and still lists the table.
    assert reader.get_table_schema("orders", "no_rowid")[0]


def test_the_aggregate_is_never_keyed(reader):
    """Each table counts its own rowids, so the lake has no key namespace."""
    assert reader.get_odata_table_schema(SUPER, ROLE) is None


def test_a_snapshot_written_before_the_watermark_is_not_keyed(reader):
    _republish(rowid_high_watermark=None)
    assert reader.get_odata_table_schema("orders", ROLE) is None


def test_more_live_rows_than_reserved_ids_is_not_keyed(reader):
    _republish(rowid_high_watermark=2)  # 3 live rows
    assert reader.get_odata_table_schema("orders", ROLE) is None
    _republish(rowid_high_watermark=3)
    assert reader.get_odata_table_schema("orders", ROLE)


def test_a_schema_without_the_rowid_column_is_not_keyed(reader):
    """An empty first write declares its columns but writes no __rowid__."""
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE, simple_name="empty", overwrite_columns=[],
        data=pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)}))
    payload = RedisCatalog().get_leaf(ORG, SUPER, "empty")["payload"]
    assert payload["rowid_high_watermark"] == 0
    assert "__rowid__" not in payload["schema"]
    assert reader.get_odata_table_schema("empty", ROLE) is None


@pytest.mark.parametrize(
    "marker", ["_linked_share", "_provider_org", "_row_filter", "_allowed_columns"])
def test_a_linked_share_payload_is_not_keyed(reader, marker):
    _republish(**{marker: "provider"})
    assert reader.get_odata_table_schema("orders", ROLE) is None


def test_a_linked_share_marker_on_the_leaf_itself_is_not_keyed(reader, hermetic_fakeredis):
    leaf = _leaf()
    leaf["_linked_share"] = "link-1"
    hermetic_fakeredis.set(meta_leaf(ORG, SUPER, "orders"), json.dumps(leaf))
    assert reader.get_odata_table_schema("orders", ROLE) is None


def test_a_denied_table_is_left_out_though_get_tables_lists_it(reader):
    assert "orders" in reader.get_tables("denied")
    assert reader.get_odata_table_schema("orders", "denied") is None


def test_a_missing_table_is_not_keyed(reader):
    assert reader.get_odata_table_schema("nosuch", ROLE) is None


def test_an_unknown_role_raises(reader):
    with pytest.raises(PermissionError):
        reader.get_odata_table_schema("orders", "nosuchrole")


def test_a_case_colliding_schema_is_left_out_rather_than_denied(reader):
    schema = dict(_leaf()["payload"]["schema"])
    schema["NAME"] = "String"
    _republish(schema=schema)
    assert reader.get_odata_table_schema("orders", ROLE) is None


def test_the_role_is_resolved_once_per_reader(reader, monkeypatch):
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE, simple_name="lines", overwrite_columns=[],
        data=pl.DataFrame({"id": [1]}))
    calls = []
    real = meta_reader_module.resolve_role_access_context

    def counting(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(meta_reader_module, "resolve_role_access_context", counting)
    assert reader.get_odata_table_schema("orders", ROLE)
    assert reader.get_odata_table_schema("lines", ROLE)
    assert len(calls) == 1
    assert MetaReader(SUPER, ORG).get_odata_table_schema("lines", ROLE)
    assert len(calls) == 2
