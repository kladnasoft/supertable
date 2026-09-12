# route: supertable.tests.test_read_controls_fail_closed
"""A read that cannot establish its controls must not return rows.

Audit H2. The deletion vector and a share's row filter are both enforced by
views the reader builds from the catalog leaf. Failing to read that leaf does
not degrade the result — it removes the control entirely, and the rows the
control would have hidden are exactly the rows nobody is supposed to see:
rows that were deleted, or rows belonging to another tenant.

Both sites used to catch Exception and log at DEBUG, so a single Redis hiccup
served resurrected or unfiltered rows under Status.OK with no visible signal.
``odata/policy.py`` calls the share-filter case "the one direction this must
never fail in" — the fingerprint obeyed that; the enforcement did not.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from supertable.tests.pruning import dataset as D

_MOD = "supertable.data_reader"


@pytest.fixture(scope="module", autouse=True)
def _dataset():
    try:
        D.build(log=lambda *a, **k: None)
    except Exception as e:
        pytest.skip(f"live stack unavailable ({type(e).__name__}: {str(e)[:90]})")


class _LeafFails:
    """A catalog that works, except the one call that establishes the control.

    Delegates everything to a real catalog so the read gets as far as the
    tombstone lookup; only ``get_leaf`` raises, which is what a Redis blip
    actually looks like from here.
    """

    def __init__(self):
        from supertable.redis_catalog import RedisCatalog

        self._real = RedisCatalog()

    def get_leaf(self, *a, **k):
        raise ConnectionError("redis is having a moment")

    def __getattr__(self, name):
        return getattr(self._real, name)


def _read(sql="SELECT count(*) AS n FROM facts"):
    from supertable.data_reader import DataReader, engine

    return DataReader(super_name=D.SUPER, organization=D.ORG,
                      query=sql, source="sdk").execute(
        role_name=D.ROLE, with_scan=False, engine=engine.AUTO)


def test_healthy_read_still_works():
    """The control: nothing here should make an ordinary read fail."""
    _df, status, msg = _read()
    assert str(status).endswith("OK"), msg


def test_unreadable_leaf_fails_the_read_rather_than_resurrecting_rows():
    # Only the leaf read fails. A stub that breaks every catalog call would
    # trip earlier code and prove nothing about this handler.
    #
    # execute() reports every failure as Status.ERROR rather than raising, so
    # what matters is that NO ROWS come back and the reason is legible — not
    # which mechanism carries it.
    with patch(f"{_MOD}.RedisCatalog", return_value=_LeafFails()):
        df, status, message = _read()

    assert str(status).endswith("ERROR"), "a read without its control returned OK"
    assert "deletion vector" in str(message)
    assert len(df) == 0, "rows were returned despite the control being unavailable"


def test_the_stream_path_also_refuses():
    """stream() turns a non-OK status into a raise, so it fails closed too —
    a caller that switched from execute() to stream() must not lose the guard.
    """
    from supertable.data_reader import DataReader

    with patch(f"{_MOD}.RedisCatalog", return_value=_LeafFails()):
        with pytest.raises(RuntimeError, match="deletion vector"):
            DataReader(super_name=D.SUPER, organization=D.ORG,
                       query="SELECT count(*) AS n FROM facts", source="sdk"
                       ).stream(role_name=D.ROLE)


def test_the_error_names_the_table_it_could_not_protect():
    """An operator needs to know WHICH table served nothing, not that 'a read
    failed' — the answer decides whether one tenant or all of them are down."""
    from supertable.data_reader import DeletionVectorUnavailable

    with patch(f"{_MOD}.RedisCatalog", return_value=_LeafFails()):
        _df, _status, message = _read()
    assert "facts" in str(message)


def test_the_two_controls_raise_distinguishable_errors():
    """'Stale deletes may be visible' and 'another tenant's rows may be
    visible' are not the same page at 3am."""
    from supertable.data_reader import (
        DeletionVectorUnavailable,
        ReadAccessUnavailable,
        ShareFilterUnavailable,
    )

    assert issubclass(DeletionVectorUnavailable, ReadAccessUnavailable)
    assert issubclass(ShareFilterUnavailable, ReadAccessUnavailable)
    assert not issubclass(DeletionVectorUnavailable, ShareFilterUnavailable)
    # RuntimeError so an existing broad handler still catches it rather than
    # letting it escape as something unrecognised.
    assert issubclass(ReadAccessUnavailable, RuntimeError)
