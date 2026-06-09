"""
Tests for supertable/errors.py — the public exception hierarchy.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.errors import (  # noqa: E402
    SuperTableNotFoundError,
    SupertableLookupError,
    TableNotFoundError,
)


class TestHierarchy:

    def test_super_table_not_found_is_lookup_error(self):
        e = SuperTableNotFoundError("org", "sup")
        assert isinstance(e, LookupError)
        assert isinstance(e, SupertableLookupError)

    def test_table_not_found_is_lookup_error(self):
        e = TableNotFoundError("org", "sup", "tbl")
        assert isinstance(e, LookupError)
        assert isinstance(e, SupertableLookupError)

    def test_base_class_is_lookup_error(self):
        # Defensive — if someone catches LookupError in legacy code,
        # the new errors must be caught.
        try:
            raise SuperTableNotFoundError("org", "sup")
        except LookupError:
            pass
        else:
            pytest.fail("SuperTableNotFoundError should be caught by LookupError")

        try:
            raise TableNotFoundError("org", "sup", "tbl")
        except LookupError:
            pass
        else:
            pytest.fail("TableNotFoundError should be caught by LookupError")


class TestMessageAndAttributes:

    def test_super_table_message_format(self):
        e = SuperTableNotFoundError("acme", "demo")
        assert str(e) == "SuperTable not found: acme/demo"
        assert e.organization == "acme"
        assert e.super_name == "demo"

    def test_table_message_format(self):
        e = TableNotFoundError("acme", "demo", "orders")
        assert str(e) == "Table not found: acme/demo/orders"
        assert e.organization == "acme"
        assert e.super_name == "demo"
        assert e.simple_name == "orders"

    def test_base_message_format(self):
        e = SupertableLookupError("custom message", organization="acme")
        assert str(e) == "custom message"
        assert e.organization == "acme"


class TestPublicReExport:

    def test_top_level_package_exports(self):
        import supertable
        assert supertable.SupertableLookupError is SupertableLookupError
        assert supertable.SuperTableNotFoundError is SuperTableNotFoundError
        assert supertable.TableNotFoundError is TableNotFoundError
        # And they're in __all__
        assert "SuperTableNotFoundError" in supertable.__all__
        assert "TableNotFoundError" in supertable.__all__
        assert "SupertableLookupError" in supertable.__all__
