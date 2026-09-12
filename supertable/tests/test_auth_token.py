"""Auth-token mint and validation.

The token lifecycle had no coverage at all before this file, which is a poor
place to have a gap: ``create_auth_token`` is a public catalog API with no
callers inside this repo, so nothing here would have noticed it breaking.

The property that matters most is the one about legacy tokens. Verification
hashes whatever plaintext the caller presents, so adding a prefix to newly
minted tokens must not invalidate tokens issued before the prefix existed.
That is asserted directly rather than argued for in a comment.
"""
from __future__ import annotations

import hashlib
import json
import os

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.redis_catalog import LOGIN_TOKEN_PREFIX  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402


class _FakeRedis:
    """The two hash operations the token paths use, and nothing else."""

    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hexists(self, key, field):
        return field in self.hashes.get(key, {})

    def hdel(self, key, field):
        return 1 if self.hashes.get(key, {}).pop(field, None) is not None else 0


@pytest.fixture()
def catalog():
    """A catalog whose Redis is fake, so no server is required."""
    from supertable.redis_catalog import RedisCatalog

    cat = RedisCatalog.__new__(RedisCatalog)   # bypass __init__/connection
    cat.r = _FakeRedis()
    return cat


ORG = "acme"


def test_minted_token_carries_the_login_prefix(catalog):
    out = catalog.create_auth_token(ORG, created_by="admin")
    assert out["token"].startswith(LOGIN_TOKEN_PREFIX)


def test_prefix_is_the_documented_literal():
    """Pinned: a scanner rule and the host both key off this exact string."""
    assert LOGIN_TOKEN_PREFIX == "st_login_"


def test_prefix_adds_no_entropy_but_costs_none_either(catalog):
    """The secret is the 24 random bytes after the marker, not the marker."""
    out = catalog.create_auth_token(ORG, created_by="admin")
    body = out["token"][len(LOGIN_TOKEN_PREFIX):]
    assert len(body) >= 32, f"token body unexpectedly short: {body!r}"


def test_two_tokens_are_never_equal(catalog):
    tokens = {catalog.create_auth_token(ORG, created_by="a")["token"]
              for _ in range(50)}
    assert len(tokens) == 50


def test_stored_digest_is_over_the_full_plaintext(catalog):
    """The prefix is part of what gets hashed, not stripped before hashing."""
    out = catalog.create_auth_token(ORG, created_by="admin")
    expected = hashlib.sha256(out["token"].encode("utf-8")).hexdigest()
    assert out["token_id"] == expected
    assert catalog.r.hexists(RK.auth_tokens(ORG), expected)


def test_minted_token_validates(catalog):
    out = catalog.create_auth_token(ORG, created_by="admin", username="alice")
    assert catalog.validate_auth_token(ORG, out["token"]) is True
    meta = catalog.validate_auth_token_full(ORG, out["token"])
    assert meta is not None and meta["username"] == "alice"


def test_a_legacy_unprefixed_token_still_validates(catalog):
    """The whole reason validation does not require the prefix.

    Simulates a token issued before the marker existed by storing its digest
    directly. If verification ever starts demanding the prefix, this fails —
    and that failure is the signal that the change logs out every existing
    session.
    """
    legacy = "8Wd3nQhTqZ1rLpXv0KcYbA2f"          # no st_login_ marker
    digest = hashlib.sha256(legacy.encode("utf-8")).hexdigest()
    catalog.r.hset(RK.auth_tokens(ORG), digest, json.dumps({
        "token_id": digest, "enabled": True, "username": "bob",
        "user_id": "u1", "expires_ms": 0,
    }))

    assert catalog.validate_auth_token(ORG, legacy) is True
    meta = catalog.validate_auth_token_full(ORG, legacy)
    assert meta is not None and meta["username"] == "bob"


def test_the_prefix_alone_is_not_a_valid_token(catalog):
    """Guard against a prefix check ever standing in for verification."""
    catalog.create_auth_token(ORG, created_by="admin")
    assert catalog.validate_auth_token(ORG, LOGIN_TOKEN_PREFIX) is False
    assert catalog.validate_auth_token(ORG, LOGIN_TOKEN_PREFIX + "x") is False


def test_unknown_and_empty_tokens_are_refused(catalog):
    assert catalog.validate_auth_token(ORG, "") is False
    assert catalog.validate_auth_token(ORG, "st_login_totally_made_up") is False
    assert catalog.validate_auth_token_full(ORG, "") is None


def test_disabled_token_is_refused(catalog):
    out = catalog.create_auth_token(ORG, created_by="admin", enabled=False)
    assert catalog.validate_auth_token_full(ORG, out["token"]) is None


def test_expired_token_is_refused(catalog):
    out = catalog.create_auth_token(ORG, created_by="admin", expires_ms=1)
    assert catalog.validate_auth_token_full(ORG, out["token"]) is None


def test_deleted_token_stops_validating(catalog):
    out = catalog.create_auth_token(ORG, created_by="admin")
    assert catalog.delete_auth_token(ORG, out["token_id"]) is True
    assert catalog.validate_auth_token(ORG, out["token"]) is False


def test_tokens_are_filed_under_the_org_auth_key(catalog):
    """Tokens are company-level state, not per-supertable."""
    catalog.create_auth_token(ORG, created_by="admin")
    assert list(catalog.r.hashes) == [RK.auth_tokens(ORG)]
    assert RK.auth_tokens(ORG).split(":")[2] == "system"   # org-level scope
