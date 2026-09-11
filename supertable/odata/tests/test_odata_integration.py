# route: supertable.odata.tests.test_odata_integration
"""OData helpers against a live table.

The unit tests prove the token logic. This proves the part that only shows up
against the real read path: that ``__rowid__`` can be selected at all (the
tombstone view strips it by default), and that paging holds when someone writes
to the table mid-export — the exact case OFFSET paging gets wrong.
"""

from __future__ import annotations

import uuid

import polars as pl
import pytest

import supertable.odata as O

ROLE = "superadmin"


@pytest.fixture(scope="module")
def live_table():
    from supertable.data_writer import DataWriter
    from supertable.redis_catalog import RedisCatalog
    from supertable.super_table import SuperTable

    suffix = uuid.uuid4().hex[:8]
    org = sup = f"odata{suffix}"
    table = "orders"
    try:
        SuperTable(super_name=sup, organization=org)
        writer = DataWriter(super_name=sup, organization=org)
        writer.write(role_name=ROLE, simple_name=table,
                     data=pl.DataFrame({"id": list(range(1, 21)),
                                        "name": [f"r{i}" for i in range(1, 21)]}).to_arrow(),
                     overwrite_columns=[])
        yield org, sup, table, writer
    except Exception as e:
        pytest.skip(f"live stack unavailable ({type(e).__name__}: {str(e)[:100]})")
    finally:
        try:
            RedisCatalog().delete_super_table(org, sup)
        except Exception:
            pass


def _page(org, sup, table, sql, cursor):
    from supertable.data_reader import DataReader
    from supertable.engine.arrow_result import materialize

    handle = DataReader(super_name=sup, organization=org,
                        query=O.apply_to_sql(sql, cursor), source="sdk"
                        ).stream(role_name=ROLE, expose_rowid=True)
    return O.split_page(materialize(handle), cursor)


def test_rowid_is_selectable_only_when_asked_for(live_table):
    """The key is hidden by default and exposed on request.

    Without the opt-in a server cannot page at all — the column it needs as a
    cursor has been stripped by the tombstone view. With it always on, lake
    bookkeeping would leak into every ordinary query's schema.
    """
    from supertable.data_reader import DataReader
    from supertable.engine.arrow_result import materialize

    org, sup, table, _ = live_table
    hidden = materialize(DataReader(super_name=sup, organization=org,
                                    query=f"SELECT * FROM {table}", source="sdk"
                                    ).stream(role_name=ROLE))
    assert "__rowid__" not in hidden.columns

    shown = materialize(DataReader(super_name=sup, organization=org,
                                   query=f"SELECT * FROM {table}", source="sdk"
                                   ).stream(role_name=ROLE, expose_rowid=True))
    assert "__rowid__" in shown.columns


def test_paging_covers_every_row_exactly_once(live_table):
    org, sup, table, _ = live_table
    sql = f"SELECT * FROM {table}"
    cursor, seen = O.first_page(table, sql, 7), []
    while cursor is not None:
        page = _page(org, sup, table, sql, cursor)
        assert page["count"] <= 7, "a page must never exceed page_size"
        seen += page["rows"]["__rowid__"].to_list()
        cursor = page["next_cursor"]

    assert len(seen) == 20
    assert len(set(seen)) == 20, "a row was served twice"
    assert seen == sorted(seen), "pages must be ordered by the cursor"


def test_a_write_between_pages_neither_skips_nor_repeats(live_table):
    """The reason this is not OFFSET.

    With OFFSET, rows inserted mid-export shift every later row and the client
    silently loses one or sees one twice. A cursor resumes from a row identity,
    so already-served rows are unaffected by anything written after them.
    """
    org, sup, table, writer = live_table
    sql = f"SELECT * FROM {table}"

    cursor = O.first_page(table, sql, 7)
    first = _page(org, sup, table, sql, cursor)
    served = first["rows"]["__rowid__"].to_list()

    # Someone writes while the export is in flight.
    writer.write(role_name=ROLE, simple_name=table,
                 data=pl.DataFrame({"id": [101, 102, 103],
                                    "name": ["x", "y", "z"]}).to_arrow(),
                 overwrite_columns=[])

    cursor = first["next_cursor"]
    while cursor is not None:
        page = _page(org, sup, table, sql, cursor)
        served += page["rows"]["__rowid__"].to_list()
        cursor = page["next_cursor"]

    assert len(set(served)) == len(served), "a row was served twice"
    assert served == sorted(served)
    # The 20 original rows must all appear; the 3 new ones may or may not,
    # depending on where they landed relative to the cursor — both are correct,
    # neither is a loss.
    assert len(served) >= 20


def test_discovery_reports_the_live_table_as_servable(live_table):
    from supertable.simple_table import SimpleTable
    from supertable.super_table import SuperTable

    org, sup, table, _ = live_table
    snap, _path = SimpleTable(SuperTable(super_name=sup, organization=org),
                              table).get_simple_table_snapshot()

    described = O.describe_table(table, snap)
    assert isinstance(described, O.EntitySet), getattr(described, "reason", "")
    assert described.key == "__rowid__"
    assert {c["name"] for c in described.columns} == {"id", "name"}
    # Not a fixed count: the fixture is module-scoped and a sibling test writes
    # to the table mid-export on purpose. Compare against the snapshot instead,
    # which is the claim that actually matters — discovery reports what is
    # there, not what the test expected to be there.
    assert described.rows == O.snapshot_live_rows(snap) >= 20


def test_written_snapshot_proves_stable_identity(live_table):
    from supertable.simple_table import SimpleTable
    from supertable.super_table import SuperTable

    org, sup, table, _ = live_table
    snap, _path = SimpleTable(SuperTable(super_name=sup, organization=org),
                              table).get_simple_table_snapshot()
    verdict = O.verify_stable_identity(snap)
    assert verdict, verdict.reason
    assert verdict.watermark >= verdict.live_rows > 0
