"""
A snapshot is published in one round trip, or in the three it always took.

``DataWriter._publish_snapshot`` is where ``write()`` and ``compact()`` make a
new snapshot the table's current one.  It asks the catalog for the one-script
commit (``commit_leaf_and_bump_root``: lock check, leaf, root bump), and goes
the old way, ``verify_simple_lock`` then ``set_leaf_payload_cas`` then
``bump_root``, whenever that is not to be had: a catalog without the method, a
catalog whose answer is not the two versions the script returns (every test
double), a write without a lock token, or a failure of the call.

The catalog here is a double that records what it is asked, so that which way
was taken, in which order, and what a failure at each step leads to can be
read off exactly.  The real catalog is on the other side of this:
``test_a_commit_checks_the_lock_and_bumps_the_root_in_one_script.py``.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.data_writer import DataWriter  # noqa: E402
from supertable.errors import LockLostError  # noqa: E402
from supertable.utils.profiler import Profiler  # noqa: E402

ORG, LAKE, TABLE = "org", "sup", "orders"
SNAPSHOT = {"resources": [{"file": "f1"}], "snapshot_version": 4}
PATH = "org/sup/tables/orders/snapshots/s4.json"
TOKEN = "tok"


class _Catalog:
    """Records every call; each method answers what the test set for it."""

    def __init__(self, one_script="absent", verify=True, payload=7, path_only=8, bump=3):
        self.calls = []
        self._answers = {"commit_leaf_and_bump_root": one_script, "verify_simple_lock": verify,
                         "set_leaf_payload_cas": payload, "set_leaf_path_cas": path_only,
                         "bump_root": bump}
        if one_script == "absent":
            del self._answers["commit_leaf_and_bump_root"]

    def __getattr__(self, name):
        answers = self.__dict__.get("_answers", {})
        if name not in answers:
            raise AttributeError(name)

        def method(*args, **options):
            self.calls.append((name, args, options))
            answer = answers[name]
            if isinstance(answer, BaseException):
                raise answer
            return answer

        return method

    def names(self):
        return [name for name, _args, _options in self.calls]


def _writer(catalog) -> DataWriter:
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization=ORG, super_name=LAKE)
    writer.catalog = catalog
    return writer


def _publish(catalog, token=TOKEN, profiler=None):
    return _writer(catalog)._publish_snapshot(
        TABLE, token, SNAPSHOT, PATH, lambda message: message, profiler=profiler)


THREE_STEPS = ["verify_simple_lock", "set_leaf_payload_cas", "bump_root"]


# ---------------------------------------------------------------------------
# One round trip
# ---------------------------------------------------------------------------

def test_the_one_script_is_all_that_is_asked_when_it_answers():
    catalog = _Catalog(one_script=(5, 9))

    assert _publish(catalog) is None

    assert catalog.names() == ["commit_leaf_and_bump_root"]
    name, args, options = catalog.calls[0]
    assert args == (ORG, LAKE, TABLE, SNAPSHOT, PATH)
    assert options["lock_token"] == TOKEN
    assert isinstance(options["now_ms"], int) and options["now_ms"] > 1_700_000_000_000


def test_a_lost_lock_is_raised_and_nothing_else_is_tried():
    """Not followed by the three steps: the script has answered, and what it
    said is that this writer may not publish."""
    catalog = _Catalog(one_script=LockLostError(ORG, LAKE, TABLE))

    with pytest.raises(LockLostError):
        _publish(catalog)

    assert catalog.names() == ["commit_leaf_and_bump_root"]


def test_both_timings_are_still_reported():
    """A write's monitoring record names the leaf commit and the root bump;
    the root is bumped by the script that commits, and its timing stays."""
    profiler = Profiler()

    _publish(_Catalog(one_script=(5, 9)), profiler=profiler)

    assert set(profiler.timings) == {"redis.set_leaf", "redis.bump_root"}
    assert profiler.counts == {"redis.set_leaf.n": 1, "redis.bump_root.n": 1}


# ---------------------------------------------------------------------------
# Three, when the one is not to be had
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("answer", [
    None, MagicMock(), [5, 9], (5,), (5, 9, 1), 5, "OK", {"leaf": 5, "root": 9},
], ids=["nothing", "a mock", "a list", "one version", "three values", "a number", "text", "a dict"])
def test_an_answer_that_is_not_the_two_versions_is_followed_by_the_three_steps(answer):
    """What a test double answers.  The writer's tests that pin the three
    calls on a mocked catalog rest on this."""
    catalog = _Catalog(one_script=answer)

    _publish(catalog)

    assert catalog.names() == ["commit_leaf_and_bump_root"] + THREE_STEPS


def test_a_catalog_without_the_one_script_is_asked_the_three_steps():
    catalog = _Catalog()

    _publish(catalog)

    assert catalog.names() == THREE_STEPS
    assert catalog.calls[0][1] == (ORG, LAKE, TABLE, TOKEN)
    assert catalog.calls[1][1] == (ORG, LAKE, TABLE, SNAPSHOT, PATH)
    assert catalog.calls[2][1] == (ORG, LAKE)
    assert catalog.calls[1][2]["now_ms"] == catalog.calls[2][2]["now_ms"]


@pytest.mark.parametrize("failure", [
    redis.exceptions.TimeoutError("Timeout reading from socket"),
    redis.ResponseError("leaf payload holds a number that is not finite (NaN or Infinity)"),
    AttributeError("_leaf_commit_fenced"),
    RuntimeError("anything"),
], ids=["a timeout", "a number that is not finite", "a catalog half built", "anything else"])
def test_a_failure_of_the_one_script_is_followed_by_the_three_steps(failure):
    catalog = _Catalog(one_script=failure)

    _publish(catalog)

    assert catalog.names() == ["commit_leaf_and_bump_root"] + THREE_STEPS


def test_without_a_lock_token_the_three_steps_are_taken_and_nothing_is_checked():
    catalog = _Catalog(one_script=(5, 9))

    _publish(catalog, token=None)

    assert catalog.names() == ["set_leaf_payload_cas", "bump_root"]


def test_the_three_steps_report_the_same_two_timings():
    profiler = Profiler()

    _publish(_Catalog(), profiler=profiler)

    assert set(profiler.timings) == {"redis.set_leaf", "redis.bump_root"}
    assert profiler.counts == {"redis.set_leaf.n": 1, "redis.bump_root.n": 1}


def test_a_compaction_has_no_profiler_and_publishes_all_the_same():
    catalog = _Catalog(one_script=(5, 9))

    _publish(catalog, profiler=None)

    assert catalog.names() == ["commit_leaf_and_bump_root"]


# ---------------------------------------------------------------------------
# The three steps are what they were
# ---------------------------------------------------------------------------

def test_a_lock_that_is_not_held_stops_them_before_anything_is_published():
    catalog = _Catalog(verify=False)

    with pytest.raises(LockLostError) as raised:
        _publish(catalog)

    assert catalog.names() == ["verify_simple_lock"]
    assert str(raised.value) == str(LockLostError(ORG, LAKE, TABLE))


def test_a_lock_check_that_fails_to_run_does_not_stop_them():
    catalog = _Catalog(verify=RuntimeError("no answer"))

    _publish(catalog)

    assert catalog.names() == THREE_STEPS


def test_a_leaf_that_cannot_hold_the_snapshot_is_stored_as_its_path():
    catalog = _Catalog(payload=redis.ResponseError("not finite"))

    _publish(catalog)

    assert catalog.names() == ["verify_simple_lock", "set_leaf_payload_cas", "set_leaf_path_cas",
                               "bump_root"]
    assert catalog.calls[2][1] == (ORG, LAKE, TABLE, PATH)


def test_a_leaf_that_cannot_be_stored_either_way_raises_and_the_root_is_left():
    catalog = _Catalog(payload=redis.ResponseError("refused"),
                       path_only=redis.exceptions.ConnectionError("gone"))

    with pytest.raises(redis.exceptions.ConnectionError):
        _publish(catalog)

    assert "bump_root" not in catalog.names()


def test_a_root_that_cannot_be_bumped_raises():
    catalog = _Catalog(bump=redis.exceptions.ConnectionError("gone"))

    with pytest.raises(redis.exceptions.ConnectionError):
        _publish(catalog)

    assert catalog.names() == THREE_STEPS


def test_a_mocked_catalog_is_published_to_in_three_steps():
    """The shape every mock-based test of the writer has."""
    catalog = MagicMock()
    catalog.verify_simple_lock.return_value = True

    _publish(catalog)

    catalog.commit_leaf_and_bump_root.assert_called_once()
    catalog.verify_simple_lock.assert_called_once_with(ORG, LAKE, TABLE, TOKEN)
    catalog.set_leaf_payload_cas.assert_called_once()
    catalog.set_leaf_path_cas.assert_not_called()
    catalog.bump_root.assert_called_once()
