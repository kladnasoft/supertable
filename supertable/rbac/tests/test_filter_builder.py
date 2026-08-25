"""
FilterBuilder test suite.

Covers:
  1. format_column_list (wildcard, single, multiple columns)
  2. Wildcard / no-filter scenarios
  3. Simple equality filters (value type)
  4. Multiple fields in a single dict (AND join)
  5. Explicit AND / OR logical operators
  6. NOT operator
  7. Range filters
  8. Null type
  9. Reference type (column-to-column)
 10. ILIKE with ESCAPE clause
 11. List-of-filters (top-level list)
 12. Nested combinations
 13. Edge cases (empty filters, empty dict, empty list)

Run:
  python -m pytest test_filter_builder.py -v
"""

import unittest

import duckdb

from supertable.rbac.filter_builder import FilterBuilder, format_column_list


# ═══════════════════════════════════════════════════════════════════════════ #
#  1. format_column_list                                                     #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestFormatColumnList(unittest.TestCase):

    def test_wildcard(self):
        self.assertEqual(format_column_list(["*"]), "*")

    def test_single_column(self):
        self.assertEqual(format_column_list(["name"]), '"name" as "name"')

    def test_multiple_columns(self):
        result = format_column_list(["a", "b", "c"])
        self.assertEqual(result, '"a" as "a","b" as "b","c" as "c"')

    def test_columns_use_selected_identifier_quote(self):
        result = format_column_list(["select", "value"], "`")
        self.assertEqual(result, "`select` as `select`,`value` as `value`")

    def test_empty_list_rejected(self):
        with self.assertRaises(ValueError):
            format_column_list([])

    def test_non_string_column_rejected(self):
        with self.assertRaisesRegex(
                ValueError, "^Invalid column name in RBAC filter$",
        ):
            format_column_list([1])


# ═══════════════════════════════════════════════════════════════════════════ #
#  2. Wildcard / no-filter scenarios                                         #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestWildcardFilters(unittest.TestCase):

    def test_wildcard_filter_no_where(self):
        fb = FilterBuilder("t1", ["*"], {"filters": ["*"]})
        self.assertEqual(fb.filter_query, 'SELECT *\nFROM "t1"')

    def test_missing_filters_key_defaults_to_wildcard(self):
        fb = FilterBuilder("t1", ["*"], {})
        self.assertEqual(fb.filter_query, 'SELECT *\nFROM "t1"')

    def test_wildcard_with_specific_columns(self):
        fb = FilterBuilder("t1", ["a", "b"], {"filters": ["*"]})
        self.assertIn('"a" as "a"', fb.filter_query)
        self.assertNotIn("WHERE", fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  3. Simple equality filters                                                #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestSimpleFilters(unittest.TestCase):

    def test_single_equality(self):
        role_info = {"filters": {
            "status": {"operation": "=", "value": "active", "type": "value"},
        }}
        fb = FilterBuilder("users", ["*"], role_info)
        # FilterBuilder always double-quotes column identifiers to handle
        # reserved keywords; literal values keep single quotes.
        self.assertEqual(fb.filter_query,
                         'SELECT *\nFROM "users"\nWHERE "status" = \'active\'')

    def test_not_equal(self):
        role_info = {"filters": {
            "status": {"operation": "!=", "value": "deleted", "type": "value"},
        }}
        fb = FilterBuilder("users", ["*"], role_info)
        self.assertIn('"status" != \'deleted\'', fb.filter_query)

    def test_greater_than(self):
        role_info = {"filters": {
            "age": {"operation": ">", "value": "18", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"age" > \'18\'', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  4. Multiple fields in a single dict                                       #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestMultipleFields(unittest.TestCase):

    def test_two_fields_joined_with_and(self):
        role_info = {"filters": {
            "col1": {"operation": "=", "value": "x", "type": "value"},
            "col2": {"operation": ">", "value": "5", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"col1" = \'x\'', fb.filter_query)
        self.assertIn('"col2" > \'5\'', fb.filter_query)
        self.assertIn(" AND ", fb.filter_query)

    def test_three_fields(self):
        role_info = {"filters": {
            "a": {"operation": "=", "value": "1", "type": "value"},
            "b": {"operation": "=", "value": "2", "type": "value"},
            "c": {"operation": "=", "value": "3", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        # Should have two AND separators for three clauses
        where = fb.filter_query.split("WHERE ", 1)[1]
        self.assertEqual(where.count(" AND "), 2)


# ═══════════════════════════════════════════════════════════════════════════ #
#  5. AND / OR logical operators                                             #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestLogicalOperators(unittest.TestCase):

    def test_explicit_and(self):
        role_info = {"filters": {"AND": [
            {"col1": {"operation": "=", "value": "a", "type": "value"}},
            {"col2": {"operation": "=", "value": "b", "type": "value"}},
        ]}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('("col1" = \'a\') AND ("col2" = \'b\')', fb.filter_query)

    def test_explicit_or(self):
        role_info = {"filters": {"OR": [
            {"col1": {"operation": "=", "value": "a", "type": "value"}},
            {"col2": {"operation": "=", "value": "b", "type": "value"}},
        ]}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('("col1" = \'a\') OR ("col2" = \'b\')', fb.filter_query)

    def test_and_with_three_conditions(self):
        role_info = {"filters": {"AND": [
            {"x": {"operation": "=", "value": "1", "type": "value"}},
            {"y": {"operation": "=", "value": "2", "type": "value"}},
            {"z": {"operation": "=", "value": "3", "type": "value"}},
        ]}}
        fb = FilterBuilder("t1", ["*"], role_info)
        where = fb.filter_query.split("WHERE ", 1)[1]
        self.assertEqual(where.count(" AND "), 2)


# ═══════════════════════════════════════════════════════════════════════════ #
#  6. NOT operator                                                           #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestNotOperator(unittest.TestCase):

    def test_not(self):
        role_info = {"filters": {"NOT": {
            "status": {"operation": "=", "value": "deleted", "type": "value"},
        }}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('NOT ("status" = \'deleted\')', fb.filter_query)

    def test_not_with_or(self):
        role_info = {"filters": {"NOT": {"OR": [
            {"a": {"operation": "=", "value": "1", "type": "value"}},
            {"b": {"operation": "=", "value": "2", "type": "value"}},
        ]}}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('NOT (("a" = \'1\') OR ("b" = \'2\'))', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  7. Range filters                                                          #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRangeFilters(unittest.TestCase):

    def test_basic_range(self):
        role_info = {"filters": {"age": {"range": [
            {"operation": ">=", "value": "18", "type": "value"},
            {"operation": "<=", "value": "65", "type": "value"},
        ]}}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"age" >= \'18\' AND "age" <= \'65\'', fb.filter_query)

    def test_range_with_reference_type(self):
        role_info = {"filters": {"price": {"range": [
            {"operation": ">=", "value": "min_price", "type": "reference"},
            {"operation": "<=", "value": "max_price", "type": "reference"},
        ]}}}
        fb = FilterBuilder("t1", ["*"], role_info)
        # Both identifiers are validated and quoted.
        self.assertIn('"price" >= "min_price" AND "price" <= "max_price"', fb.filter_query)

    def test_single_bound_range(self):
        role_info = {"filters": {"score": {"range": [
            {"operation": ">", "value": "0", "type": "value"},
        ]}}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"score" > \'0\'', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  8. Null type                                                              #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestNullType(unittest.TestCase):

    def test_is_null(self):
        role_info = {"filters": {
            "deleted_at": {"operation": "IS", "value": "", "type": "null"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"deleted_at" IS NULL', fb.filter_query)

    def test_is_not_null(self):
        role_info = {"filters": {
            "email": {"operation": "IS NOT", "value": "", "type": "null"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"email" IS NOT NULL', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
#  9. Reference type (column-to-column)                                      #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestReferenceType(unittest.TestCase):

    def test_column_reference(self):
        role_info = {"filters": {
            "start_date": {"operation": "<", "value": "end_date", "type": "reference"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        # Both identifiers are validated and quoted; the RHS is not a literal.
        self.assertIn('"start_date" < "end_date"', fb.filter_query)
        # Reference should NOT be string-literal quoted
        self.assertNotIn("'end_date'", fb.filter_query)

    def test_non_string_reference_rejected(self):
        role_info = {"filters": {
            "start_date": {"operation": "<", "value": 1, "type": "reference"},
        }}
        with self.assertRaisesRegex(
                ValueError, "^Invalid column name in RBAC filter$",
        ):
            FilterBuilder("t1", ["*"], role_info)

    def test_non_string_range_reference_rejected(self):
        role_info = {"filters": {"price": {"range": [
            {"operation": ">=", "value": False, "type": "reference"},
        ]}}}
        with self.assertRaisesRegex(
                ValueError, "^Invalid column name in RBAC filter$",
        ):
            FilterBuilder("t1", ["*"], role_info)


# ═══════════════════════════════════════════════════════════════════════════ #
# 10. ILIKE with ESCAPE                                                      #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestIlikeEscape(unittest.TestCase):

    def test_ilike_with_escape(self):
        role_info = {"filters": {
            "name": {"operation": "ILIKE", "value": "%test%", "type": "value", "escape": "\\"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"name" ILIKE \'%test%\'', fb.filter_query)
        self.assertIn("ESCAPE", fb.filter_query)

    def test_ilike_without_escape(self):
        role_info = {"filters": {
            "name": {"operation": "ILIKE", "value": "%test%", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"name" ILIKE \'%test%\'', fb.filter_query)
        self.assertNotIn("ESCAPE", fb.filter_query)

    def test_non_ilike_rejects_escape_key(self):
        role_info = {"filters": {
            "name": {"operation": "=", "value": "test", "type": "value", "escape": "\\"},
        }}
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            FilterBuilder("t1", ["*"], role_info)


# ═══════════════════════════════════════════════════════════════════════════ #
# 11. List-of-filters (top-level list)                                       #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestListOfFilters(unittest.TestCase):

    def test_list_of_two_dicts(self):
        role_info = {"filters": [
            {"col1": {"operation": "=", "value": "a", "type": "value"}},
            {"col2": {"operation": "=", "value": "b", "type": "value"}},
        ]}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"col1" = \'a\'', fb.filter_query)
        self.assertIn('"col2" = \'b\'', fb.filter_query)
        self.assertIn(" AND ", fb.filter_query)

    def test_list_of_one_dict(self):
        role_info = {"filters": [
            {"col1": {"operation": "=", "value": "a", "type": "value"}},
        ]}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"col1" = \'a\'', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #
# 12. Nested combinations                                                    #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestNestedCombinations(unittest.TestCase):

    def test_and_inside_or(self):
        role_info = {"filters": {"OR": [
            {"AND": [
                {"a": {"operation": "=", "value": "1", "type": "value"}},
                {"b": {"operation": "=", "value": "2", "type": "value"}},
            ]},
            {"c": {"operation": "=", "value": "3", "type": "value"}},
        ]}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn(" OR ", fb.filter_query)
        self.assertIn(" AND ", fb.filter_query)

    def test_not_inside_and(self):
        role_info = {"filters": {"AND": [
            {"NOT": {"x": {"operation": "=", "value": "bad", "type": "value"}}},
            {"y": {"operation": ">", "value": "0", "type": "value"}},
        ]}}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('NOT ("x" = \'bad\')', fb.filter_query)
        self.assertIn('"y" > \'0\'', fb.filter_query)

    def test_range_plus_equality(self):
        """Dict with a range field and a simple equality field."""
        role_info = {"filters": {
            "age": {"range": [
                {"operation": ">=", "value": "18", "type": "value"},
                {"operation": "<=", "value": "65", "type": "value"},
            ]},
            "status": {"operation": "=", "value": "active", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["*"], role_info)
        self.assertIn('"age" >= \'18\' AND "age" <= \'65\'', fb.filter_query)
        self.assertIn('"status" = \'active\'', fb.filter_query)
        # The two clauses should be AND-joined
        where = fb.filter_query.split("WHERE ", 1)[1]
        parts = where.split(" AND ")
        self.assertGreaterEqual(len(parts), 2)


# ═══════════════════════════════════════════════════════════════════════════ #
# 13. Edge cases                                                             #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestEdgeCases(unittest.TestCase):

    def test_valid_table_names_are_stripped_and_canonically_quoted(self):
        cases = (
            ("items", '"items"'),
            (" _items ", '"_items"'),
            ("Select", '"Select"'),
            ("a" * 128, f'"{"a" * 128}"'),
        )
        for table_name, expected in cases:
            with self.subTest(table_name=table_name):
                fb = FilterBuilder(table_name, ["*"], {"filters": ["*"]})
                self.assertEqual(fb.filter_query, f"SELECT *\nFROM {expected}")

    def test_invalid_table_names_fail_closed_without_reflection(self):
        invalid_names = (
            None,
            False,
            1,
            "",
            "   ",
            "a" * 129,
            "catalog.items",
            '"items"',
            "`items`",
            "items alias",
            "items, victim",
            "items JOIN victim ON TRUE",
            "items; DROP TABLE victim;--",
            "items--comment",
            "items/*comment*/",
            "items\nWHERE 1=1",
            "\x00items",
        )
        for table_name in invalid_names:
            with self.subTest(table_name=table_name):
                with self.assertRaises(ValueError) as raised:
                    FilterBuilder(table_name, ["*"], {"filters": ["*"]})
                self.assertEqual(
                    str(raised.exception), "Invalid table name in RBAC filter",
                )
                self.assertNotIn("victim", str(raised.exception))

    def test_direct_build_filter_query_validates_table_name(self):
        builder = FilterBuilder("items", ["*"], {"filters": ["*"]})
        with self.assertRaisesRegex(
                ValueError, "^Invalid table name in RBAC filter$",
        ):
            builder.build_filter_query(
                "items; DROP TABLE victim;--", ["*"], ["*"],
            )

    def test_backtick_quote_applies_to_table_projection_and_filter(self):
        role_info = {"filters": {
            "value": {"operation": "=", "value": "select", "type": "reference"},
        }}
        builder = FilterBuilder(
            "table_name", ["select", "value"], role_info,
            identifier_quote="`",
        )
        self.assertEqual(
            builder.filter_query,
            "SELECT `select` as `select`,`value` as `value`\n"
            "FROM `table_name`\nWHERE `value` = `select`",
        )

    def test_table_injection_rejection_preserves_duckdb_tables(self):
        connection = duckdb.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE items (id INTEGER)")
        connection.execute("CREATE TABLE victim (id INTEGER)")
        connection.execute("INSERT INTO items VALUES (1)")

        valid_query = FilterBuilder(
            "items", ["id"], {"filters": ["*"]},
        ).filter_query
        self.assertEqual(connection.execute(valid_query).fetchall(), [(1,)])

        with self.assertRaisesRegex(
                ValueError, "^Invalid table name in RBAC filter$",
        ):
            injected_query = FilterBuilder(
                "items; DROP TABLE victim;--", ["*"], {"filters": ["*"]},
            ).filter_query
            connection.execute(injected_query)

        remaining = {
            row[0]
            for row in connection.execute("SHOW TABLES").fetchall()
        }
        self.assertEqual(remaining, {"items", "victim"})

    def test_empty_dict_filter_rejected(self):
        with self.assertRaises(ValueError):
            FilterBuilder("t1", ["*"], {"filters": {}})

    def test_empty_list_filter_rejected(self):
        with self.assertRaises(ValueError):
            FilterBuilder("t1", ["*"], {"filters": []})

    def test_query_starts_with_select(self):
        fb = FilterBuilder("t1", ["a"], {"filters": ["*"]})
        self.assertTrue(fb.filter_query.startswith("SELECT"))

    def test_query_contains_from(self):
        fb = FilterBuilder("my_table", ["*"], {})
        self.assertIn('FROM "my_table"', fb.filter_query)

    def test_specific_columns_with_filter(self):
        role_info = {"filters": {
            "x": {"operation": "=", "value": "1", "type": "value"},
        }}
        fb = FilterBuilder("t1", ["a", "b"], role_info)
        self.assertIn('"a" as "a"', fb.filter_query)
        self.assertIn('"b" as "b"', fb.filter_query)
        self.assertIn('WHERE "x" = \'1\'', fb.filter_query)


# ═══════════════════════════════════════════════════════════════════════════ #

if __name__ == "__main__":
    unittest.main(verbosity=2)
