# route: supertable.odata.tests.test_odata_service_contract
"""The contract Core's OData service actually depends on.

Written against the service's real call sites in
``dataisland-core/services/odata/odata_server.py``, not against an invented
API. The service imports two names from the package root and calls them with
specific keywords; if either drifts, the service fails at request time with an
ImportError or a TypeError, which no test in this repo would otherwise catch.
"""

from __future__ import annotations

import inspect
import threading
import uuid

import polars as pl
import pytest

import supertable
from supertable.odata import stream as S
from supertable.odata.row_identity import ROWID_COLUMN

ROLE = "superadmin"


# --------------------------------------------------------------------------
# The import surface — how the service reaches these
# --------------------------------------------------------------------------

def test_service_imports_resolve_from_the_package_root():
    """`from supertable import query_odata_sql_stream` — odata_server.py:527."""
    assert callable(supertable.query_odata_sql_stream)
    assert callable(supertable.query_sql_policy_fingerprint)


def test_unknown_attribute_still_raises_attribute_error():
    """The lazy __getattr__ must not swallow genuine typos."""
    with pytest.raises(AttributeError, match="no attribute"):
        supertable.query_odata_sql_streem       # noqa: B018 - deliberate typo


def test_stream_accepts_every_keyword_the_service_passes():
    """odata_server.py builds query_kwargs then calls with **kwargs.

    A missing keyword is a TypeError at request time, so the signature is
    asserted directly rather than inferred from a happy-path call.
    """
    params = inspect.signature(supertable.query_odata_sql_stream).parameters
    for name in ("organization", "super_name", "sql", "role_name",
                 "max_total_rows", "timeout_sec", "source", "out",
                 "cancel_event", "expected_effective_policy_fingerprint",
                 "continuation_boundary"):
        assert name in params, f"service passes {name!r}, signature lacks it"


def test_policy_change_raises_permission_error():
    """The service catches PermissionError to turn this into a continuation
    conflict rather than a 500 — so the subclass must remain one."""
    assert issubclass(S.ODataPolicyChanged, PermissionError)


# --------------------------------------------------------------------------
# Keyset resumption — the service's boundary shape
# --------------------------------------------------------------------------

def _boundary(order, identity=41234):
    """The shape odata_handler.py emits: order terms plus a row identity."""
    return {"version": 1, "order": order, "row_identity": identity}


def test_first_page_has_no_predicate():
    assert S.keyset_predicate(None) is None
    assert S.keyset_predicate({}) is None


def test_identity_only_boundary():
    got = S.keyset_predicate(_boundary([], 100))
    assert got == f'("{ROWID_COLUMN}" > 100)'


def test_single_ascending_column_uses_identity_as_tiebreaker():
    got = S.keyset_predicate(_boundary(
        [{"column": "name", "direction": "asc", "value": "bob"}], 7))
    assert got == ('("name" > \'bob\') OR '
                   f'("name" = \'bob\' AND "{ROWID_COLUMN}" > 7)')


def test_descending_column_compares_the_other_way():
    """A DESC term compared with > would page backwards and repeat rows."""
    got = S.keyset_predicate(_boundary(
        [{"column": "ts", "direction": "desc", "value": "2026-01-01"}], 7))
    assert '"ts" < \'2026-01-01\'' in got


def test_multi_column_order_is_lexicographic():
    got = S.keyset_predicate(_boundary([
        {"column": "a", "direction": "asc", "value": 1},
        {"column": "b", "direction": "desc", "value": 2},
    ], 9))
    # strictly greater on a; equal a and strictly less on b; then identity.
    assert '("a" > 1)' in got
    assert '("a" = 1 AND "b" < 2)' in got
    assert f'("a" = 1 AND "b" = 2 AND "{ROWID_COLUMN}" > 9)' in got


def test_boundary_without_identity_is_refused():
    """ORDER BY columns are rarely unique; without the tiebreaker a page edge
    inside a run of equal values repeats or drops rows."""
    with pytest.raises(ValueError, match="row_identity"):
        S.keyset_predicate({"order": [], "row_identity": None})


def test_null_boundary_value_is_refused_not_guessed():
    """NULL cannot be compared with > or <; the predicate would be neither
    true nor false and would end the feed early and silently."""
    with pytest.raises(ValueError, match="NULL"):
        S.keyset_predicate(_boundary(
            [{"column": "a", "direction": "asc", "value": None}]))


def test_string_values_are_escaped():
    got = S.keyset_predicate(_boundary(
        [{"column": "n", "direction": "asc", "value": "O'Brien"}], 1))
    assert "'O''Brien'" in got


def test_unknown_direction_is_refused():
    with pytest.raises(ValueError, match="direction"):
        S.keyset_predicate(_boundary(
            [{"column": "a", "direction": "sideways", "value": 1}]))


# --------------------------------------------------------------------------
# Against a live table
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def live():
    from supertable.data_writer import DataWriter
    from supertable.redis_catalog import RedisCatalog
    from supertable.super_table import SuperTable

    suffix = uuid.uuid4().hex[:8]
    org = sup = f"odsvc{suffix}"
    table = "orders"
    try:
        SuperTable(super_name=sup, organization=org)
        DataWriter(super_name=sup, organization=org).write(
            role_name=ROLE, simple_name=table,
            data=pl.DataFrame({
                "id": list(range(1, 13)),
                "grp": ["a"] * 6 + ["b"] * 6,
            }).to_arrow(),
            overwrite_columns=[])
        yield org, sup, table
    except Exception as e:
        pytest.skip(f"live stack unavailable ({type(e).__name__}: {str(e)[:90]})")
    finally:
        try:
            RedisCatalog().delete_super_table(org, sup)
        except Exception:
            pass


def _collect(stream):
    rows = []
    for batch in stream:
        rows += batch.to_pylist()
    return rows


def test_stream_yields_arrow_with_schema_and_closes(live):
    org, sup, table = live
    out = {}
    stream = supertable.query_odata_sql_stream(
        organization=org, super_name=sup, sql=f"SELECT * FROM {table}",
        role_name=ROLE, max_total_rows=5, timeout_sec=30, source="odata",
        out=out, cancel_event=threading.Event(),
        expected_effective_policy_fingerprint=None,
    )
    try:
        assert stream.schema is not None
        assert ROWID_COLUMN in stream.schema.names, (
            "the service needs the identity column to build entity keys")
        rows = _collect(stream)
    finally:
        stream.close()
    assert len(rows) == 5, "max_total_rows must bound the read"


def test_out_carries_what_the_service_re_verifies(live):
    """odata_server.py reads exactly these three keys back out."""
    org, sup, table = live
    out = {}
    supertable.query_odata_sql_stream(
        organization=org, super_name=sup, sql=f"SELECT * FROM {table}",
        role_name=ROLE, max_total_rows=1, out=out,
    ).close()
    for key in ("role_policy_fingerprint", "effective_policy_fingerprint",
                "selected_engine"):
        assert out.get(key), f"service reads query_out[{key!r}]"


def test_mismatched_policy_fingerprint_refuses_before_reading(live):
    """Checked BEFORE the read, so a revoked grant cannot leak one row."""
    org, sup, table = live
    with pytest.raises(PermissionError, match="policy changed"):
        supertable.query_odata_sql_stream(
            organization=org, super_name=sup, sql=f"SELECT * FROM {table}",
            role_name=ROLE, max_total_rows=5,
            expected_effective_policy_fingerprint="not-the-current-one",
        )


def test_matching_fingerprint_is_accepted(live):
    org, sup, table = live
    fp = supertable.query_sql_policy_fingerprint(
        org, sup, f"SELECT * FROM {table}", ROLE)
    stream = supertable.query_odata_sql_stream(
        organization=org, super_name=sup, sql=f"SELECT * FROM {table}",
        role_name=ROLE, max_total_rows=2,
        expected_effective_policy_fingerprint=fp["effective_policy_fingerprint"],
    )
    try:
        assert len(_collect(stream)) == 2
    finally:
        stream.close()


def test_keyset_resumption_walks_the_table_once(live):
    """A full feed driven the way the service drives it: order by a
    non-unique column, resume from the boundary, never repeat a row."""
    org, sup, table = live
    sql = f"SELECT * FROM {table} ORDER BY grp ASC"
    seen, boundary, pages = [], None, 0

    while pages < 10:
        stream = supertable.query_odata_sql_stream(
            organization=org, super_name=sup, sql=sql, role_name=ROLE,
            max_total_rows=4, continuation_boundary=boundary,
        )
        try:
            rows = _collect(stream)
        finally:
            stream.close()
        if not rows:
            break
        pages += 1
        seen += [r["id"] for r in rows]
        last = rows[-1]
        boundary = {"version": 1,
                    "order": [{"column": "grp", "direction": "asc",
                               "value": last["grp"]}],
                    "row_identity": last[ROWID_COLUMN]}

    assert sorted(seen) == list(range(1, 13)), seen
    assert len(set(seen)) == len(seen), "a row was served twice"


def test_cancel_event_stops_the_stream(live):
    org, sup, table = live
    cancel = threading.Event()
    cancel.set()                       # already cancelled before the first pull
    stream = supertable.query_odata_sql_stream(
        organization=org, super_name=sup, sql=f"SELECT * FROM {table}",
        role_name=ROLE, cancel_event=cancel,
    )
    try:
        assert _collect(stream) == []
    finally:
        stream.close()
