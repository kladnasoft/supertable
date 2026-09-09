"""Real catalog/RBAC bootstrap against empty Redis, without seeded activation."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.audit.privileged_worker import ActivationBaselineError
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable


@pytest.fixture
def empty_backend(tmp_path, monkeypatch):
    r = fakeredis.FakeStrictRedis(decode_responses=True)
    storage = LocalStorage(root=tmp_path)
    monkeypatch.setattr(
        "supertable.redis_catalog.RedisConnector", lambda *_args: SimpleNamespace(r=r),
    )
    monkeypatch.setattr("supertable.super_table.get_storage", lambda: storage)
    yield r, RedisCatalog()
    r.close()


def _default_ids(catalog, name="lake"):
    role = catalog.rbac_get_role_id_by_name("org", name, "superadmin")
    user = catalog.rbac_get_user_id_by_username("org", name, "superuser")
    assert role and user
    assert role in catalog.get_user_details("org", name, user)["roles"]
    return role, user


def test_empty_redis_bootstraps_again_after_redis_is_cleared(empty_backend):
    from supertable.data_writer import DataWriter

    r, catalog = empty_backend
    for _ in range(2):
        assert r.dbsize() == 0
        table = DataWriter("lake", "org").super_table
        assert table.catalog.root_exists("org", "lake")
        _default_ids(catalog)
        assert r.exists(RK.audit_privileged_activation("org"))
        assert r.xlen(RK.audit_privileged_outbox("org")) == 2
        r.flushdb()


def test_root_left_by_old_failed_creation_is_completed(empty_backend):
    r, catalog = empty_backend
    # Reproduce the old constructor's partial state, with no audit or RBAC seed.
    catalog.ensure_root("org", "lake")
    assert not r.exists(RK.audit_privileged_activation("org"))
    assert r.keys(RK.rbac_pattern_for_org("org")) == []

    SuperTable("lake", "org")

    _default_ids(catalog)
    assert r.xlen(RK.audit_privileged_outbox("org")) == 2


def test_retry_after_user_bootstrap_failure_preserves_created_role(
    empty_backend, monkeypatch,
):
    r, catalog = empty_backend
    import supertable.super_table as module

    real_manager = module.UserManager

    def unavailable_user_manager(**_kwargs):
        raise ConnectionError("simulated interrupted user bootstrap")

    monkeypatch.setattr(module, "UserManager", unavailable_user_manager)
    with pytest.raises(ConnectionError, match="interrupted user bootstrap"):
        SuperTable("lake", "org")
    role = catalog.rbac_get_role_id_by_name("org", "lake", "superadmin")
    assert role
    assert catalog.root_exists("org", "lake")
    assert r.xlen(RK.audit_privileged_outbox("org")) == 1

    monkeypatch.setattr(module, "UserManager", real_manager)
    SuperTable("lake", "org")

    assert _default_ids(catalog)[0] == role
    assert r.xlen(RK.audit_privileged_outbox("org")) == 2


def test_healthy_reopen_preserves_all_security_state(empty_backend):
    r, catalog = empty_backend
    SuperTable("lake", "org")
    ids = _default_ids(catalog)
    before = {key: r.dump(key) for key in r.scan_iter()}

    SuperTable("lake", "org")

    assert _default_ids(catalog) == ids
    assert {key: r.dump(key) for key in r.scan_iter()} == before


def test_read_only_open_does_not_bootstrap_partial_or_missing_namespace(empty_backend):
    r, catalog = empty_backend
    from supertable.errors import SuperTableNotFoundError

    with pytest.raises(SuperTableNotFoundError):
        SuperTable("lake", "org", create_if_missing=False)
    assert r.dbsize() == 0

    catalog.ensure_root("org", "lake")
    before = {key: r.dump(key) for key in r.scan_iter()}
    SuperTable("lake", "org", create_if_missing=False)
    assert {key: r.dump(key) for key in r.scan_iter()} == before


def test_concurrent_creators_share_one_role_and_user(empty_backend):
    r, catalog = empty_backend
    with ThreadPoolExecutor(max_workers=4) as executor:
        tables = list(executor.map(lambda _: SuperTable("lake", "org"), range(4)))
    assert len(tables) == 4
    _default_ids(catalog)
    assert r.scard(RK.rbac_role_index("org", "lake")) == 1
    assert r.scard(RK.rbac_user_index("org", "lake")) == 1
    assert r.xlen(RK.audit_privileged_outbox("org")) == 2


def test_second_namespace_reuses_organization_activation(empty_backend):
    r, catalog = empty_backend
    SuperTable("first", "org")
    anchor = r.get(RK.audit_privileged_activation("org"))
    SuperTable("second", "org")
    _default_ids(catalog, "first")
    _default_ids(catalog, "second")
    assert r.get(RK.audit_privileged_activation("org")) == anchor
    assert r.xlen(RK.audit_privileged_outbox("org")) == 4


def test_role_initialization_timeout_is_retryable(empty_backend, monkeypatch):
    r, catalog = empty_backend
    acquire = RedisCatalog.acquire_simple_lock

    def fail_role_lock(self, org, sup, simple, **kwargs):
        if simple == "roles_init":
            return None
        return acquire(self, org, sup, simple, **kwargs)

    monkeypatch.setattr(RedisCatalog, "acquire_simple_lock", fail_role_lock)
    with pytest.raises(TimeoutError, match="role initialization lock"):
        SuperTable("lake", "org")
    assert not catalog.rbac_get_role_id_by_name("org", "lake", "superadmin")

    monkeypatch.setattr(RedisCatalog, "acquire_simple_lock", acquire)
    SuperTable("lake", "org")
    _default_ids(catalog)
    assert r.xlen(RK.audit_privileged_outbox("org")) == 2


def test_missing_anchor_over_existing_security_state_is_not_replaced(empty_backend):
    r, catalog = empty_backend
    SuperTable("existing", "org")
    r.delete(RK.audit_privileged_activation("org"))
    before = {key: r.dump(key) for key in r.scan_iter()}
    with pytest.raises(ActivationBaselineError):
        SuperTable("new", "org")
    assert not catalog.root_exists("org", "new")
    assert {key: r.dump(key) for key in r.scan_iter()} == before
