# route: supertable.tests.test_deletion_intent_key
"""The namespace deletion-intent key.

Set before a supertable teardown begins and checked by readers, so a request
arriving mid-deletion is refused rather than served from a half-removed
catalog. It guards ``meta:root``, so it is lake-scoped beside it.
"""

from __future__ import annotations

import pytest

from supertable import redis_keys as RK


def test_it_is_lake_scoped_beside_meta_root():
    key = RK.meta_namespace_deletion_intent("acme", "sales")
    root = RK.meta_root("acme", "sales")
    assert key.rsplit(":", 1)[0] == root.rsplit(":", 1)[0], (
        "deletion intent must live beside the root it guards")
    assert key.endswith(":meta:deletion-intent")


def test_it_is_distinct_per_supertable():
    a = RK.meta_namespace_deletion_intent("acme", "sales")
    b = RK.meta_namespace_deletion_intent("acme", "finance")
    c = RK.meta_namespace_deletion_intent("other", "sales")
    assert len({a, b, c}) == 3


def test_it_rejects_an_unsafe_segment():
    """Same validator as every other key: a wildcard here would let a deletion
    check match keys it does not own."""
    with pytest.raises(ValueError):
        RK.meta_namespace_deletion_intent("acme", "sales:*")
