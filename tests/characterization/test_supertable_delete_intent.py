"""``SuperTable.delete`` fences the lake with its namespace deletion intent.

``redis_keys.meta_namespace_deletion_intent`` was documented as "set before a
namespace teardown begins and checked by readers", but nothing set it: the
delete removed storage, then SCAN-deleted the lake's keys and swallowed any
Redis error. So dataisland-core, whose inventory, quality, garbage collection
and lake-mutation fence all read that key, kept serving a lake mid-teardown.

The delete now sets the intent before storage goes and removes it with the root
as its very last step; a delete that stops part way leaves the lake fenced and
still openable, and running it again finishes. Real writer, catalog, RBAC and
LOCAL storage on fakeredis.
"""

from __future__ import annotations

import json
import re

import polars as pl
import pytest
import redis

from supertable import redis_keys as RK
from supertable.data_writer import DataWriter
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "teardown"
ROLE = "superadmin"
ROOT = RK.meta_root(ORG, SUPER)
INTENT = RK.meta_namespace_deletion_intent(ORG, SUPER)


def _lake() -> SuperTable:
    st = SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE, simple_name="orders", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2]}))
    return st


def _lake_keys(fake):
    return sorted(fake.scan_iter(match=RK.super_table_pattern(ORG, SUPER)))


def test_the_intent_is_set_before_storage_goes(hermetic_fakeredis, monkeypatch):
    fake = hermetic_fakeredis
    st = _lake()
    seen = []
    real_delete_tree = st.storage.delete_tree

    def delete_tree(path):
        seen.append((fake.exists(ROOT), fake.get(INTENT)))
        return real_delete_tree(path)

    monkeypatch.setattr(st.storage, "delete_tree", delete_tree)
    st.delete(role_name=ROLE)

    ((root_present, raw_intent),) = seen
    intent = json.loads(raw_intent)
    assert root_present
    assert re.fullmatch(r"[0-9a-f]{32}", intent["intent_id"])
    assert isinstance(intent["started_at_ms"], int) and intent["started_at_ms"] > 0


def test_the_root_and_intent_go_last_and_together(hermetic_fakeredis, monkeypatch):
    fake = hermetic_fakeredis
    st = _lake()
    deletes = []
    real_delete = fake.delete

    def delete(*names):
        deletes.append(set(names))
        return real_delete(*names)

    monkeypatch.setattr(fake, "delete", delete)
    st.delete(role_name=ROLE)

    assert deletes[-1] == {ROOT, INTENT}
    assert all(not ({ROOT, INTENT} & names) for names in deletes[:-1])
    assert _lake_keys(fake) == []


def test_a_storage_failure_leaves_the_lake_fenced_and_a_rerun_finishes(
        hermetic_fakeredis, monkeypatch):
    fake = hermetic_fakeredis
    st = _lake()

    def down(path):
        raise OSError("object store unavailable")

    with monkeypatch.context() as m:
        m.setattr(st.storage, "delete_tree", down)
        with pytest.raises(OSError):
            st.delete(role_name=ROLE)
    assert fake.exists(ROOT) and fake.exists(INTENT)
    assert fake.exists(RK.meta_leaf(ORG, SUPER, "orders")), "nothing else went either"

    SuperTable(SUPER, ORG).delete(role_name=ROLE)
    assert _lake_keys(fake) == []


def test_a_redis_error_during_the_scan_raises_and_a_rerun_finishes(
        hermetic_fakeredis, monkeypatch):
    fake = hermetic_fakeredis
    st = _lake()

    def scan(*args, **kwargs):
        raise redis.exceptions.TimeoutError("Timeout reading from socket")

    with monkeypatch.context() as m:
        m.setattr(fake, "scan", scan)
        with pytest.raises(redis.exceptions.TimeoutError):
            st.delete(role_name=ROLE)
    assert fake.exists(ROOT) and fake.exists(INTENT)

    SuperTable(SUPER, ORG).delete(role_name=ROLE)
    assert _lake_keys(fake) == []


def test_a_delete_stopped_before_its_last_step_is_finished_by_a_rerun(
        hermetic_fakeredis, monkeypatch):
    """Every other key, RBAC included, is gone; the root still opens the lake
    and the superadmin role is re-created, so the rerun is authorized."""
    fake = hermetic_fakeredis
    st = _lake()
    real_delete = fake.delete

    def delete(*names):
        if set(names) == {ROOT, INTENT}:
            raise redis.exceptions.ConnectionError("Connection reset by peer")
        return real_delete(*names)

    with monkeypatch.context() as m:
        m.setattr(fake, "delete", delete)
        with pytest.raises(redis.exceptions.ConnectionError):
            st.delete(role_name=ROLE)
    assert _lake_keys(fake) == sorted([INTENT, ROOT])

    SuperTable(SUPER, ORG).delete(role_name=ROLE)
    assert _lake_keys(fake) == []


def test_a_role_without_control_sets_no_intent(hermetic_fakeredis):
    st = _lake()
    RoleManager(super_name=SUPER, organization=ORG, actor_role_name=ROLE).create_role(
        {"role": "writer", "role_name": "loader", "tables": {"*": {"columns": ["*"]}}})

    with pytest.raises(PermissionError):
        st.delete(role_name="loader")
    assert hermetic_fakeredis.get(INTENT) is None


def test_a_deleted_lake_can_be_created_again(hermetic_fakeredis):
    _lake().delete(role_name=ROLE)

    _lake()
    assert hermetic_fakeredis.exists(ROOT)
    assert hermetic_fakeredis.get(INTENT) is None
