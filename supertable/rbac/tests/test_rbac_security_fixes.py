"""Regression suite for the RBAC security findings C4 / S6 / S7 / S9 / S11.

Every test in this file was observed FAILING against the pre-fix tree before the
corresponding source change was made.  Each one encodes a *fail-closed* rule:

  C4  an empty grant set is **no grant**, never "everything"          (row_column_security)
  S9  a configured row filter that renders to empty SQL **denies**    (filter_builder / access_control)
  S11 ``superadmin`` is a reserved role name; a name collision is an
      error, not a silent substitution of a different role            (role_manager)
  S6  non-``value`` filter operands are identifiers, not raw SQL      (filter_builder)
  S7  ``["id", "*"]`` never widens back to ``SELECT *``               (row_column_security / access_control)

Run:
  python -m pytest supertable/rbac/tests/test_rbac_security_fixes.py -v
"""

import unittest
from unittest.mock import patch

from supertable.data_classes import TableDefinition
from supertable.rbac.access_control import restrict_read_access
from supertable.rbac.filter_builder import FilterBuilder
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.row_column_security import RowColumnSecurity

# Reuse the in-memory Redis fake and constants from the main RBAC suite so this
# file needs no fixture of its own.
from supertable.rbac.tests.test_rbac import ORG, SUP, fresh_catalog


def _td(simple_name: str, alias: str = None, columns=None) -> TableDefinition:
    return TableDefinition(
        super_name=SUP,
        simple_name=simple_name,
        alias=alias or simple_name,
        columns=list(columns or []),
    )


# ═══════════════════════════════════════════════════════════════════════════ #
#  C4 — revoking a role's access must not grant it everything                #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestC4EmptyGrantSetIsNoGrant(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat, actor_role_name="superadmin")

    def _patch_manager(self):
        return patch("supertable.rbac.access_control.RoleManager", return_value=self.rm)

    def test_empty_tables_stays_empty(self):
        """``prepare()`` must not substitute a wildcard for an empty grant set."""
        rcs = RowColumnSecurity(role="reader", tables={})
        rcs.prepare()
        self.assertEqual(rcs.tables, {})
        self.assertEqual(rcs.to_json()["tables"], {})

    def test_missing_tables_stays_empty(self):
        """A role document that never supplied ``tables`` is also no grant."""
        rcs = RowColumnSecurity(role="reader")
        rcs.prepare()
        self.assertEqual(rcs.tables, {})

    def test_revoke_all_tables_denies_every_table(self):
        """The headline escalation: revoke-all must deny, not widen."""
        rid = self.rm.create_role({
            "role": "reader",
            "role_name": "revokeme",
            "tables": {"employees": {"columns": ["id"], "filters": ["*"]}},
        })

        # Operator intent: revoke every table grant.
        self.rm.update_role(rid, {"tables": {}})
        self.assertEqual(self.rm.get_role(rid)["tables"], {})

        with self._patch_manager():
            # The table the role legitimately had ...
            with self.assertRaises(PermissionError):
                restrict_read_access(SUP, ORG, "revokeme", [_td("employees")], [_td("employees")])
            # ... and the table it never had.
            with self.assertRaises(PermissionError):
                restrict_read_access(SUP, ORG, "revokeme", [_td("secrets")], [_td("secrets")])

    def test_role_created_with_no_tables_cannot_read(self):
        """A role built incrementally is *not* an admin in the meantime."""
        self.rm.create_role({"role": "reader", "role_name": "nopolicy", "tables": {}})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                restrict_read_access(SUP, ORG, "nopolicy", [_td("employees")], [_td("employees")])

    def test_update_of_a_tableless_document_does_not_widen(self):
        """``update_role`` must not invent a wildcard grant for a doc that has none.

        A role document written without a ``tables`` field (direct catalog write,
        migration, partially-failed create) used to be widened to
        ``{"*": ...}`` by *any* unrelated update.
        """
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "notables", {
            "role": "reader", "role_id": "notables", "role_name": "notables",
        })
        self.rm.update_role("notables", {"role": "reader"})
        self.assertEqual(self.rm.get_role("notables")["tables"], {})

        with self._patch_manager():
            with self.assertRaises(PermissionError):
                restrict_read_access(SUP, ORG, "notables", [_td("employees")], [_td("employees")])

    def test_explicit_wildcard_still_grants_everything(self):
        """The escape hatch still works — callers must just say so explicitly."""
        self.rm.create_role({
            "role": "reader",
            "role_name": "everything",
            "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        })
        with self._patch_manager():
            result = restrict_read_access(
                SUP, ORG, "everything", [_td("employees")], [_td("employees")],
            )
        self.assertEqual(result, {})


# ═══════════════════════════════════════════════════════════════════════════ #
#  S9 — a filter that renders to empty SQL must deny, never widen            #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestS9EmptyRenderedFilterDenies(unittest.TestCase):

    EMPTY_RENDERINGS = (
        [{}],
        [{"AND": []}],
        [{"OR": []}],
        [[]],
        [],
        {},
    )

    def test_filter_builder_raises_on_empty_rendering(self):
        for spec in self.EMPTY_RENDERINGS:
            with self.subTest(filters=spec):
                with self.assertRaises(ValueError):
                    FilterBuilder("t1", ["*"], {"filters": spec})

    def test_wildcard_sentinel_is_still_unrestricted(self):
        fb = FilterBuilder("t1", ["*"], {"filters": ["*"]})
        self.assertEqual(fb.filter_query, "SELECT *\nFROM t1")

    def test_missing_filters_key_is_still_unrestricted(self):
        fb = FilterBuilder("t1", ["*"], {})
        self.assertEqual(fb.filter_query, "SELECT *\nFROM t1")


class TestS9RestrictReadAccessDenies(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat, actor_role_name="superadmin")

    def _patch_manager(self):
        return patch("supertable.rbac.access_control.RoleManager", return_value=self.rm)

    def test_unrenderable_filter_denies_the_read(self):
        """A configured row filter that produces no predicate must not serve rows."""
        self.rm.create_role({
            "role": "reader",
            "role_name": "brokenfilter",
            "tables": {"employees": {"columns": ["*"], "filters": [{"AND": []}]}},
        })
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                restrict_read_access(
                    SUP, ORG, "brokenfilter", [_td("employees")], [_td("employees")],
                )

    def test_malformed_filter_document_denies_the_read(self):
        """A filter that raises while rendering must deny, not escape as a 500.

        ``{"client": "client1"}`` is the shorthand shape an operator reaches for
        — the builder expects ``{"client": {"operation": ..., "type": ...}}``
        and raises ``TypeError`` on it.
        """
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "malformed", {
            "role": "reader", "role_id": "malformed", "role_name": "malformed",
            "tables": {"employees": {"columns": ["*"], "filters": {"client": "client1"}}},
        })
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                restrict_read_access(
                    SUP, ORG, "malformed", [_td("employees")], [_td("employees")],
                )

    def test_working_filter_still_builds_a_view(self):
        self.rm.create_role({
            "role": "reader",
            "role_name": "okfilter",
            "tables": {"employees": {
                "columns": ["*"],
                "filters": [{"region": {"operation": "=", "type": "value", "value": "EU"}}],
            }},
        })
        with self._patch_manager():
            views = restrict_read_access(
                SUP, ORG, "okfilter", [_td("employees")], [_td("employees")],
            )
        self.assertIn("employees", views)
        self.assertEqual(views["employees"].where_clause, "\"region\" = 'EU'")


# ═══════════════════════════════════════════════════════════════════════════ #
#  S11 / M12 — reserved role names and name collisions                       #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestS11ReservedNamesAndCollisions(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat, actor_role_name="superadmin")

    def test_cannot_mint_a_role_named_superadmin(self):
        for name in ("superadmin", "SuperAdmin", "SUPERADMIN", " superadmin "):
            with self.subTest(role_name=name):
                with self.assertRaises(ValueError):
                    self.rm.create_role({
                        "role": "reader", "role_name": name,
                        "tables": {"employees": {"columns": ["id"], "filters": ["*"]}},
                    })

    def test_name_collision_is_an_error_not_a_substitution(self):
        first = self.rm.create_role({
            "role": "reader", "role_name": "analyst", "tables": {"t1": {"columns": ["a"]}},
        })
        with self.assertRaises(ValueError):
            self.rm.create_role({
                "role": "writer", "role_name": "analyst", "tables": {"t2": {"columns": ["b"]}},
            })
        # The original role is untouched.
        self.assertEqual(self.rm.get_role(first)["role"], "reader")

    def test_identical_recreate_stays_idempotent(self):
        """Retry-safety is preserved when the request really is the same."""
        data = {"role": "reader", "role_name": "retry_me", "tables": {"t1": {"columns": ["a"]}}}
        first = self.rm.create_role(dict(data))
        second = self.rm.create_role(dict(data))
        self.assertEqual(first, second)

    def test_bootstrap_superadmin_is_still_created(self):
        """The reservation must not break the library's own bootstrap role."""
        sa_id = self.rm.get_superadmin_role_id()
        self.assertIsNotNone(sa_id)
        self.assertEqual(self.rm.get_role(sa_id)["role_name"], "superadmin")

    def test_cannot_rename_a_role_to_a_reserved_name(self):
        rid = self.rm.create_role({
            "role": "reader", "role_name": "sneaky", "tables": {"t1": {"columns": ["a"]}},
        })
        # Must be refused *as reserved* — not merely because the bootstrap
        # role happens to already own the name.
        with self.assertRaisesRegex(ValueError, "reserved"):
            self.rm.update_role(rid, {"role_name": "superadmin"})


# ═══════════════════════════════════════════════════════════════════════════ #
#  S6 — non-"value" filter operands must be identifiers, not raw SQL         #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestS6FilterOperandInjection(unittest.TestCase):

    def test_reference_operand_cannot_inject_a_disjunction(self):
        """The verified bypass: ``"amount" > 0 OR 1=1``."""
        role_info = {"filters": {
            "amount": {"operation": ">", "type": "reference", "value": "0 OR 1=1"},
        }}
        with self.assertRaises(ValueError):
            FilterBuilder("t1", ["*"], role_info)

    def test_range_reference_operand_cannot_inject(self):
        role_info = {"filters": {"price": {"range": [
            {"operation": ">=", "type": "reference", "value": "0 OR 1=1"},
        ]}}}
        with self.assertRaises(ValueError):
            FilterBuilder("t1", ["*"], role_info)

    def test_reference_operand_cannot_be_a_literal_number(self):
        """A bare number is not a column reference — the ``value`` type is."""
        role_info = {"filters": {
            "amount": {"operation": ">", "type": "reference", "value": "0"},
        }}
        with self.assertRaises(ValueError):
            FilterBuilder("t1", ["*"], role_info)

    def test_unknown_operand_type_is_rejected(self):
        for bad_type in ("raw", "sql", "expression", ""):
            with self.subTest(type=bad_type):
                role_info = {"filters": {
                    "amount": {"operation": ">", "type": bad_type, "value": "1"},
                }}
                with self.assertRaises(ValueError):
                    FilterBuilder("t1", ["*"], role_info)

    def test_legitimate_column_reference_still_works(self):
        role_info = {"filters": {
            "start_date": {"operation": "<", "type": "reference", "value": "end_date"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"start_date" < "end_date"', fb.filter_query)
        self.assertNotIn("'end_date'", fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  S7 — ["id", "*"] must never widen back to SELECT *                        #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestS7MixedWildcardColumns(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat, actor_role_name="superadmin")

    def _patch_manager(self):
        return patch("supertable.rbac.access_control.RoleManager", return_value=self.rm)

    def test_mixed_wildcard_rejected_at_write_time(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["id", "*"]}})
        with self.assertRaises(ValueError):
            rcs.prepare()

    def test_pure_wildcard_still_accepted(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["*"]}})
        rcs.prepare()
        self.assertEqual(rcs.tables["t1"]["columns"], ["*"])

    def test_stored_mixed_wildcard_does_not_leak_at_read_time(self):
        """Documents already in Redis must be narrowed, not honoured."""
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "mixed", {
            "role": "reader", "role_id": "mixed", "role_name": "mixed",
            "tables": {"employees": {"columns": ["id", "*"], "filters": ["*"]}},
        })
        with self._patch_manager():
            views = restrict_read_access(
                SUP, ORG, "mixed", [_td("employees")], [_td("employees")],
            )
        self.assertIn("employees", views)
        self.assertEqual(views["employees"].allowed_columns, ["id"])
        self.assertNotIn("*", views["employees"].allowed_columns)

    def test_stored_mixed_wildcard_still_denies_a_masked_column(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "mixed2", {
            "role": "reader", "role_id": "mixed2", "role_name": "mixed2",
            "tables": {"employees": {"columns": ["id", "*"], "filters": ["*"]}},
        })
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                restrict_read_access(
                    SUP, ORG, "mixed2",
                    [_td("employees", columns=["salary"])],
                    [_td("employees", columns=["salary"])],
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
