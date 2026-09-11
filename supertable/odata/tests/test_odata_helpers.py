# route: supertable.odata.tests.test_odata_helpers
"""OData helpers: identity, discovery, and continuation paging.

These are helpers for a server, so the tests are about the questions a server
cannot answer for itself — is this key trustworthy, which tables can be served,
and does paging survive a concurrent write.
"""

from __future__ import annotations

import pytest

import supertable.odata as O
from supertable.odata import continuation as C
from supertable.odata import row_identity as RI


# --------------------------------------------------------------------------
# Stable identity
# --------------------------------------------------------------------------

def _snap(rows=10, watermark=10, deleted=0, **extra):
    snap = {"resources": [{"rows": rows}], "tombstone_rows": deleted,
            "snapshot_version": 3, "schema": [{"name": "id", "type": "BIGINT"}]}
    if watermark is not None:
        snap[RI.WATERMARK_KEY] = watermark
    snap.update(extra)
    return snap


def test_identity_is_stable_when_rows_fit_the_watermark():
    v = RI.verify_stable_identity(_snap(rows=10, watermark=10))
    assert v and v.live_rows == 10 and v.watermark == 10


def test_deleted_rows_do_not_count_against_the_watermark():
    """A delete lowers live rows but never lowers the watermark."""
    v = RI.verify_stable_identity(_snap(rows=10, watermark=10, deleted=4))
    assert v and v.live_rows == 6


def test_missing_watermark_fails_closed():
    """A snapshot that cannot prove identity must be refused, not assumed good.

    Serving an unstable key silently is worse than refusing: the URLs a client
    stores would quietly start addressing different rows.
    """
    v = RI.verify_stable_identity(_snap(watermark=None))
    assert not v
    assert "predates" in v.reason


def test_more_live_rows_than_reserved_ids_is_refused():
    """The whole proof: ids are handed out by INCRBY and never reused, so more
    live rows than ids issued means a duplicate exists."""
    v = RI.verify_stable_identity(_snap(rows=12, watermark=10))
    assert not v and "not unique" in v.reason


def test_watermark_is_monotonic_across_writes():
    prev = _snap(rows=10, watermark=10)
    # An insert of 5 rows starting at 11.
    assert RI.next_watermark(prev, 11, 5) == 15
    # A delete-only write reserves nothing and must not lower it.
    assert RI.next_watermark(_snap(rows=3, watermark=15), 0, 0) == 15
    # A stale reservation cannot drag it backwards either.
    assert RI.next_watermark(_snap(rows=3, watermark=99), 11, 5) == 99


def test_first_write_on_an_empty_table():
    assert RI.next_watermark(None, 1, 3) == 3


# --------------------------------------------------------------------------
# Keyed discovery
# --------------------------------------------------------------------------

def test_servable_table_is_keyed_by_rowid():
    got = O.describe_table("orders", _snap())
    assert isinstance(got, O.EntitySet)
    assert got.key == "__rowid__" and got.name == "orders"


def test_system_columns_are_not_declared_as_fields():
    """__rowid__ is the KEY, not a field; __timestamp__ is neither.

    Declaring them would put lake bookkeeping into every client's model.
    """
    snap = _snap(schema=[{"name": "id", "type": "BIGINT"},
                         {"name": "__rowid__", "type": "BIGINT"},
                         {"name": "__timestamp__", "type": "TIMESTAMP"}])
    got = O.describe_table("orders", snap)
    assert [c["name"] for c in got.columns] == ["id"]


def test_unservable_tables_say_why():
    """A table missing from $metadata with no reason is an operator mystery."""
    got = O.describe_table("orders", _snap(watermark=None))
    assert isinstance(got, O.Unservable)
    assert got.reason and "predates" in got.reason


def test_table_with_only_system_columns_is_not_servable():
    snap = _snap(schema=[{"name": "__rowid__", "type": "BIGINT"}])
    assert isinstance(O.describe_table("t", snap), O.Unservable)


def test_discover_splits_servable_from_skipped():
    out = O.discover({"good": _snap(), "old": _snap(watermark=None),
                      "missing": None})
    assert [e["name"] for e in out["entity_sets"]] == ["good"]
    assert {s["name"] for s in out["skipped"]} == {"old", "missing"}
    assert all(s["reason"] for s in out["skipped"])


# --------------------------------------------------------------------------
# Continuation tokens
# --------------------------------------------------------------------------

SQL = "SELECT * FROM orders"


def test_token_round_trips():
    cur = O.first_page("orders", SQL, 50)
    assert O.decode(cur.encode(), table="orders", query=SQL) == cur


def test_token_from_another_query_is_rejected():
    """Resuming query B from query A's token would return plausible, wrong rows."""
    token = O.first_page("orders", SQL, 50).encode()
    with pytest.raises(O.InvalidContinuation, match="different query"):
        O.decode(token, table="orders", query="SELECT * FROM orders WHERE x=1")


def test_token_from_another_table_is_rejected():
    token = O.first_page("orders", SQL, 50).encode()
    with pytest.raises(O.InvalidContinuation, match="not 'items'"):
        O.decode(token, table="items", query=SQL)


def test_tampered_token_is_rejected_by_its_checksum():
    import base64, json
    cur = O.advance(O.first_page("orders", SQL, 50), 100, 50)
    blob = json.loads(base64.urlsafe_b64decode(cur.encode() + "=="))
    blob["d"]["a"] = 999_999                      # skip ahead by hand
    forged = base64.urlsafe_b64encode(
        json.dumps(blob, separators=(",", ":")).encode()).decode().rstrip("=")
    with pytest.raises(O.InvalidContinuation, match="checksum"):
        O.decode(forged, table="orders", query=SQL)


def test_empty_and_garbage_tokens_are_rejected_distinctly():
    with pytest.raises(O.InvalidContinuation, match="empty"):
        O.decode("", table="orders", query=SQL)
    with pytest.raises(O.InvalidContinuation, match="not decodable"):
        O.decode("!!!not-base64!!!", table="orders", query=SQL)


def test_first_page_has_no_predicate():
    """An always-true __rowid__ > 0 would still be planned against every file."""
    assert O.page_predicate(O.first_page("orders", SQL, 10)) is None


def test_later_pages_carry_a_prunable_predicate():
    cur = O.advance(O.first_page("orders", SQL, 10), 41234, 10)
    assert O.page_predicate(cur) == "__rowid__ > 41234"


def test_page_size_must_be_positive():
    with pytest.raises(ValueError, match="must be positive"):
        O.first_page("orders", SQL, 0)


def test_apply_to_sql_wraps_rather_than_rewrites():
    """The caller's SQL is untouched — it may already have ORDER BY or LIMIT."""
    cur = O.advance(O.first_page("orders", SQL, 10), 5, 10)
    out = O.apply_to_sql("SELECT a FROM t ORDER BY a LIMIT 3", cur)
    assert "(SELECT a FROM t ORDER BY a LIMIT 3) AS _odata_page" in out
    assert out.rstrip().endswith("LIMIT 11")       # page_size + 1


def test_split_page_hides_the_over_fetched_row():
    rows = [{"__rowid__": i} for i in range(1, 7)]      # 6 for a page of 5
    out = O.split_page(rows, O.first_page("orders", SQL, 5))
    assert out["count"] == 5 and out["has_more"] is True
    assert [r["__rowid__"] for r in out["rows"]] == [1, 2, 3, 4, 5]


def test_next_cursor_resumes_after_the_last_SERVED_row():
    """The bug this shape prevents: advancing on the over-fetched row.

    Row 6 is fetched but not served. A cursor built from the untrimmed result
    would resume at > 6 and lose row 6 entirely, with nothing reporting it.
    """
    rows = [{"__rowid__": i} for i in range(1, 7)]
    out = O.split_page(rows, O.first_page("orders", SQL, 5))
    assert out["next_cursor"].after_rowid == 5, "must resume AT the served edge"
    assert out["next_token"]


def test_last_page_has_no_next_token():
    """`next_token is None` is what a server puts in @odata.nextLink."""
    rows = [{"__rowid__": i} for i in range(1, 4)]
    out = O.split_page(rows, O.first_page("orders", SQL, 5))
    assert out["has_more"] is False and out["next_token"] is None


def test_rows_served_accumulates_across_pages():
    cur = O.first_page("orders", SQL, 5)
    for _ in range(3):
        cur = O.split_page([{"__rowid__": i} for i in range(1, 7)], cur)["next_cursor"]
    assert cur.rows_served == 15
