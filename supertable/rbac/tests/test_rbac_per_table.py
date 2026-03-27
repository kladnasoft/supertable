# route: tests.test_rbac_per_table
"""
Unit tests for the redesigned per-table RBAC structure.

Covers:
  - RowColumnSecurity with per-table columns/filters
  - FilterBuilder with per-table filter extraction
  - get_physical_tables() integration with RBAC scenarios
  - Column validation against per-table definitions
  - Join scenarios: LEFT JOIN, INNER JOIN, self-join with RBAC
"""

from __future__ import annotations

import sys
import os
import types
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Stub modules when running outside the project tree
# ---------------------------------------------------------------------------
try:
    from supertable.rbac.row_column_security import RowColumnSecurity
    from supertable.rbac.filter_builder import FilterBuilder
    from supertable.utils.sql_parser import SQLParser
    from supertable.data_classes import TableDefinition, RbacViewDef
except ImportError:
    supertable = types.ModuleType("supertable")
    supertable.data_classes = types.ModuleType("supertable.data_classes")
    supertable.config = types.ModuleType("supertable.config")
    supertable.config.defaults = types.ModuleType("supertable.config.defaults")
    supertable.rbac = types.ModuleType("supertable.rbac")
    sys.modules["supertable"] = supertable
    sys.modules["supertable.data_classes"] = supertable.data_classes
    sys.modules["supertable.config"] = supertable.config
    sys.modules["supertable.config.defaults"] = supertable.config.defaults
    sys.modules["supertable.rbac"] = supertable.rbac

    import logging
    supertable.config.defaults.logger = logging.getLogger("supertable.test")

    # Import the real permissions module from the local file
    import importlib.util
    perm_spec = importlib.util.spec_from_file_location(
        "supertable.rbac.permissions",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "permissions.py"),
    )
    perm_mod = importlib.util.module_from_spec(perm_spec)
    sys.modules["supertable.rbac.permissions"] = perm_mod
    perm_spec.loader.exec_module(perm_mod)

    @dataclass
    class TableDefinition:
        super_name: str
        simple_name: str
        alias: str
        columns: List[str] = field(default_factory=list)

    @dataclass
    class RbacViewDef:
        allowed_columns: List[str] = field(default_factory=lambda: ["*"])
        where_clause: str = ""

    supertable.data_classes.TableDefinition = TableDefinition
    supertable.data_classes.RbacViewDef = RbacViewDef

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from row_column_security import RowColumnSecurity  # noqa: E402
    from filter_builder import FilterBuilder  # noqa: E402
    from sql_parser import SQLParser  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════════
# Tests: RowColumnSecurity
# ═══════════════════════════════════════════════════════════════════════════

def test_rcs_per_table_basic():
    """Per-table structure is preserved through prepare() and to_json()."""
    rcs = RowColumnSecurity(
        role="reader",
        tables={
            "orders": {
                "columns": ["amount", "order_id", "status"],
                "filters": [{"status": {"operation": "=", "type": "value", "value": "completed"}}],
            },
            "customers": {
                "columns": ["customer_id", "name", "region"],
                "filters": ["*"],
            },
        },
    )
    rcs.prepare()
    j = rcs.to_json()

    assert j["role"] == "reader"
    assert "orders" in j["tables"]
    assert "customers" in j["tables"]
    # Columns should be sorted
    assert j["tables"]["orders"]["columns"] == ["amount", "order_id", "status"]
    assert j["tables"]["customers"]["columns"] == ["customer_id", "name", "region"]
    # Filters preserved
    assert j["tables"]["orders"]["filters"] != ["*"]
    assert j["tables"]["customers"]["filters"] == ["*"]
    # Hash computed
    assert rcs.content_hash is not None


def test_rcs_defaults_when_empty():
    """Empty tables defaults to wildcard everything."""
    rcs = RowColumnSecurity(role="reader", tables={})
    rcs.prepare()
    j = rcs.to_json()

    assert "*" in j["tables"]
    assert j["tables"]["*"]["columns"] == ["*"]
    assert j["tables"]["*"]["filters"] == ["*"]


def test_rcs_defaults_per_entry():
    """Missing columns/filters within a table entry get defaults."""
    rcs = RowColumnSecurity(
        role="reader",
        tables={"orders": {}},
    )
    rcs.prepare()
    j = rcs.to_json()

    assert j["tables"]["orders"]["columns"] == ["*"]
    assert j["tables"]["orders"]["filters"] == ["*"]


def test_rcs_wildcard_default_plus_specific():
    """'*' entry coexists with specific table entries."""
    rcs = RowColumnSecurity(
        role="reader",
        tables={
            "*": {"columns": ["*"], "filters": ["*"]},
            "orders": {
                "columns": ["order_id", "amount"],
                "filters": [{"status": {"operation": "=", "type": "value", "value": "active"}}],
            },
        },
    )
    rcs.prepare()
    j = rcs.to_json()

    assert "*" in j["tables"]
    assert "orders" in j["tables"]
    assert j["tables"]["*"]["columns"] == ["*"]
    assert j["tables"]["orders"]["columns"] == ["amount", "order_id"]  # sorted


def test_rcs_column_dedup_and_sort():
    """Duplicate columns are removed and remaining are sorted."""
    rcs = RowColumnSecurity(
        role="reader",
        tables={
            "t": {"columns": ["z", "a", "z", "m", "a"]},
        },
    )
    rcs.prepare()
    assert rcs.tables["t"]["columns"] == ["a", "m", "z"]


def test_rcs_content_hash_deterministic():
    """Same content produces same hash regardless of input order."""
    rcs1 = RowColumnSecurity(
        role="reader",
        tables={"t": {"columns": ["b", "a"]}},
    )
    rcs1.prepare()

    rcs2 = RowColumnSecurity(
        role="reader",
        tables={"t": {"columns": ["a", "b"]}},
    )
    rcs2.prepare()

    assert rcs1.content_hash == rcs2.content_hash


def test_rcs_content_hash_differs_for_different_content():
    """Different per-table columns produce different hashes."""
    rcs1 = RowColumnSecurity(
        role="reader",
        tables={"t": {"columns": ["a", "b"]}},
    )
    rcs1.prepare()

    rcs2 = RowColumnSecurity(
        role="reader",
        tables={"t": {"columns": ["a", "c"]}},
    )
    rcs2.prepare()

    assert rcs1.content_hash != rcs2.content_hash


# ═══════════════════════════════════════════════════════════════════════════
# Tests: FilterBuilder per-table integration
# ═══════════════════════════════════════════════════════════════════════════

def test_filter_builder_per_table_filter():
    """FilterBuilder works when given a per-table filter via role_info."""
    orders_filters = [
        {"status": {"operation": "=", "type": "value", "value": "completed"}}
    ]
    fb = FilterBuilder(
        table_name="orders",
        columns=["*"],
        role_info={"filters": orders_filters},
    )
    assert "WHERE" in fb.filter_query.upper()
    assert "status" in fb.filter_query
    assert "'completed'" in fb.filter_query


def test_filter_builder_different_tables_different_filters():
    """Different per-table filters produce different WHERE clauses."""
    orders_fb = FilterBuilder(
        table_name="orders",
        columns=["*"],
        role_info={"filters": [{"status": {"operation": "=", "type": "value", "value": "completed"}}]},
    )
    customers_fb = FilterBuilder(
        table_name="customers",
        columns=["*"],
        role_info={"filters": [{"region": {"operation": "ILIKE", "type": "value", "value": "EU%"}}]},
    )
    assert "status" in orders_fb.filter_query
    assert "status" not in customers_fb.filter_query
    assert "region" in customers_fb.filter_query
    assert "region" not in orders_fb.filter_query


def test_filter_builder_wildcard_no_where():
    """Wildcard filters produce no WHERE clause."""
    fb = FilterBuilder(
        table_name="t",
        columns=["*"],
        role_info={"filters": ["*"]},
    )
    assert "WHERE" not in fb.filter_query.upper()


# ═══════════════════════════════════════════════════════════════════════════
# Tests: RBAC validation logic (extracted, no Redis dependency)
# ═══════════════════════════════════════════════════════════════════════════

def _validate_columns(
    physical_tables: List[TableDefinition],
    role_tables: dict,
) -> Optional[str]:
    """
    Simulate the Phase 1 validation from restrict_read_access.

    Returns None on success, error message string on failure.
    """
    default_entry = role_tables.get("*")

    for pt in physical_tables:
        table_entry = role_tables.get(pt.simple_name) or default_entry
        if table_entry is None:
            return f"No access to table '{pt.simple_name}'"

        allowed_columns = table_entry.get("columns", ["*"])
        if allowed_columns == ["*"]:
            continue

        if not allowed_columns:
            return f"No columns allowed in '{pt.simple_name}'"

        if pt.columns:
            requested_lower = {c.lower() for c in pt.columns}
            allowed_lower = {c.lower() for c in allowed_columns}
            denied = requested_lower - allowed_lower
            if denied:
                return f"Denied columns in '{pt.simple_name}': {denied}"

    return None


def test_validate_left_join_per_table():
    """LEFT JOIN: each table validated against its own column set."""
    sql = """
        SELECT o.order_id, o.amount, o.status,
               c.customer_id, c.name, c.region
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed' AND c.region = 'EU'
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "orders": {
            "columns": ["order_id", "customer_id", "amount", "status"],
            "filters": ["*"],
        },
        "customers": {
            "columns": ["customer_id", "name", "region"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is None, f"Expected pass, got: {result}"


def test_validate_left_join_denied_column():
    """LEFT JOIN: denied column on one table doesn't affect the other."""
    sql = """
        SELECT o.order_id, o.amount, o.status,
               c.customer_id, c.name, c.email
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "orders": {
            "columns": ["order_id", "customer_id", "amount", "status"],
            "filters": ["*"],
        },
        "customers": {
            # email is NOT allowed
            "columns": ["customer_id", "name", "region"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is not None
    assert "email" in result
    assert "customers" in result


def test_validate_inner_join_per_table():
    """INNER JOIN: per-table column validation with overlapping column names."""
    sql = """
        SELECT o.order_id, o.amount, o.created_at,
               c.customer_id, c.name, c.tier
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.amount > 100 AND c.region IN ('EU', 'US')
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "orders": {
            "columns": ["order_id", "customer_id", "amount", "created_at"],
            "filters": ["*"],
        },
        "customers": {
            "columns": ["customer_id", "name", "region", "tier"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is None, f"Expected pass, got: {result}"


def test_validate_self_join_merged():
    """Self-join: same table referenced twice, columns merged for validation."""
    sql = """
        SELECT a.amount, b.status
        FROM orders a
        JOIN orders b ON a.parent_id = b.order_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    # Should be one entry for 'orders' with merged columns
    assert len(physical) == 1
    assert physical[0].simple_name == "orders"

    role_tables = {
        "orders": {
            "columns": ["order_id", "parent_id", "amount", "status"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is None, f"Expected pass, got: {result}"


def test_validate_self_join_denied():
    """Self-join: column used by one alias denied, fails on merged set."""
    sql = """
        SELECT a.amount, b.secret_col
        FROM orders a
        JOIN orders b ON a.parent_id = b.order_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "orders": {
            "columns": ["order_id", "parent_id", "amount", "status"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is not None
    assert "secret_col" in result


def test_validate_table_not_in_role():
    """Query touches a table the role has no access to."""
    sql = """
        SELECT o.order_id, s.secret
        FROM orders o
        JOIN secrets s ON o.id = s.order_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "orders": {"columns": ["*"], "filters": ["*"]},
        # 'secrets' not in role
    }

    result = _validate_columns(physical, role_tables)
    assert result is not None
    assert "secrets" in result


def test_validate_wildcard_default_covers_unknown_table():
    """'*' default entry covers tables not explicitly listed."""
    sql = """
        SELECT o.order_id, s.data
        FROM orders o
        JOIN some_other_table s ON o.id = s.ref
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    role_tables = {
        "*": {"columns": ["*"], "filters": ["*"]},
        "orders": {"columns": ["order_id", "id"], "filters": ["*"]},
    }

    result = _validate_columns(physical, role_tables)
    assert result is None, f"Expected pass, got: {result}"


def test_validate_cte_excluded_from_physical():
    """CTE alias does not appear in physical tables for validation."""
    sql = """
        WITH top AS (SELECT order_id FROM orders WHERE amount > 1000)
        SELECT o.order_id, o.status
        FROM orders o
        WHERE o.order_id IN (SELECT order_id FROM top)
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    # Only 'orders' should appear, not 'top'
    table_names = {t.simple_name for t in physical}
    assert "top" not in table_names
    assert "orders" in table_names


def test_validate_cte_join_real_table():
    """CTE joined with real table — CTE excluded, real tables validated."""
    sql = """
        WITH recent AS (
            SELECT o.order_id, o.customer_id
            FROM orders o
            WHERE o.created_at > '2024-01-01'
        )
        SELECT r.order_id, c.name
        FROM recent r
        JOIN customers c ON r.customer_id = c.customer_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    table_names = {t.simple_name for t in physical}
    assert "recent" not in table_names
    assert "orders" in table_names
    assert "customers" in table_names

    role_tables = {
        "orders": {
            "columns": ["order_id", "customer_id", "created_at"],
            "filters": ["*"],
        },
        "customers": {
            "columns": ["customer_id", "name"],
            "filters": ["*"],
        },
    }

    result = _validate_columns(physical, role_tables)
    assert result is None, f"Expected pass, got: {result}"


# ═══════════════════════════════════════════════════════════════════════════
# Tests: Per-table filter WHERE clause generation
# ═══════════════════════════════════════════════════════════════════════════

def test_per_table_where_clause():
    """Different tables get different WHERE clauses from their own filters."""
    orders_entry = {
        "columns": ["order_id", "amount", "status"],
        "filters": [{"status": {"operation": "=", "type": "value", "value": "completed"}}],
    }
    customers_entry = {
        "columns": ["customer_id", "name", "region"],
        "filters": [{"region": {"operation": "ILIKE", "type": "value", "value": "EU%"}}],
    }

    # Build WHERE for orders
    fb_orders = FilterBuilder("__PLACEHOLDER__", ["*"], {"filters": orders_entry["filters"]})
    gen_orders = fb_orders.filter_query
    idx = gen_orders.upper().find("WHERE ")
    orders_where = gen_orders[idx + 6:] if idx >= 0 else ""

    # Build WHERE for customers
    fb_customers = FilterBuilder("__PLACEHOLDER__", ["*"], {"filters": customers_entry["filters"]})
    gen_customers = fb_customers.filter_query
    idx = gen_customers.upper().find("WHERE ")
    customers_where = gen_customers[idx + 6:] if idx >= 0 else ""

    # Orders gets status filter, NOT region filter
    assert "status" in orders_where
    assert "region" not in orders_where

    # Customers gets region filter, NOT status filter
    assert "region" in customers_where
    assert "status" not in customers_where


# ═══════════════════════════════════════════════════════════════════════════
# Tests: Full RBAC scenario walkthroughs (role JSON → validation)
# ═══════════════════════════════════════════════════════════════════════════

def test_scenario_left_join_overlapping_columns():
    """
    Full scenario: LEFT JOIN between orders and customers.
    Overlapping column: customer_id, region.
    Different columns: order_id/amount/status vs name/email/tier.
    Per-table filters: orders filtered by status, customers filtered by region.
    """
    role_data = {
        "role": "reader",
        "tables": {
            "orders": {
                "columns": ["order_id", "customer_id", "amount", "status", "created_at"],
                "filters": [{"status": {"operation": "=", "type": "value", "value": "completed"}}],
            },
            "customers": {
                "columns": ["customer_id", "name", "region", "tier"],
                "filters": [{"region": {"operation": "ILIKE", "type": "value", "value": "EU%"}}],
            },
        },
    }

    rcs = RowColumnSecurity(**role_data)
    rcs.prepare()
    stored = rcs.to_json()

    # Verify stored structure
    assert stored["tables"]["orders"]["columns"] == sorted(
        ["order_id", "customer_id", "amount", "status", "created_at"]
    )
    assert stored["tables"]["customers"]["columns"] == sorted(
        ["customer_id", "name", "region", "tier"]
    )

    sql = """
        SELECT o.order_id, o.amount, o.status,
               c.customer_id, c.name, c.region
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed' AND c.region = 'EU'
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    # Validation passes
    result = _validate_columns(physical, stored["tables"])
    assert result is None

    # email is NOT in the role — requesting it should fail
    sql_denied = """
        SELECT o.order_id, c.email
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
    """
    p2 = SQLParser("s", sql_denied, "duckdb")
    physical2 = p2.get_physical_tables()
    result2 = _validate_columns(physical2, stored["tables"])
    assert result2 is not None
    assert "email" in result2


def test_scenario_inner_join_overlapping_columns():
    """
    Full scenario: INNER JOIN between orders and customers.
    region exists in both tables — orders.region allowed, customers.region allowed.
    Independent per-table validation.
    """
    role_data = {
        "role": "reader",
        "tables": {
            "orders": {
                "columns": ["order_id", "customer_id", "amount", "region"],
                "filters": ["*"],
            },
            "customers": {
                "columns": ["customer_id", "name", "region"],
                "filters": ["*"],
            },
        },
    }

    rcs = RowColumnSecurity(**role_data)
    rcs.prepare()
    stored = rcs.to_json()

    sql = """
        SELECT o.order_id, o.amount, o.region,
               c.customer_id, c.name, c.region AS c_region
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
    """
    p = SQLParser("s", sql, "duckdb")
    physical = p.get_physical_tables()

    result = _validate_columns(physical, stored["tables"])
    assert result is None


def test_scenario_region_allowed_in_orders_denied_in_customers():
    """
    Overlapping column 'region' — allowed in orders but NOT in customers.
    Selecting c.region should fail while o.region passes.
    """
    role_data = {
        "role": "reader",
        "tables": {
            "orders": {
                "columns": ["order_id", "customer_id", "amount", "region"],
                "filters": ["*"],
            },
            "customers": {
                # region NOT in allowed columns
                "columns": ["customer_id", "name"],
                "filters": ["*"],
            },
        },
    }

    rcs = RowColumnSecurity(**role_data)
    rcs.prepare()
    stored = rcs.to_json()

    # Query only uses o.region — should pass
    sql_ok = """
        SELECT o.order_id, o.region, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
    """
    p1 = SQLParser("s", sql_ok, "duckdb")
    physical1 = p1.get_physical_tables()
    result1 = _validate_columns(physical1, stored["tables"])
    assert result1 is None, f"Expected pass, got: {result1}"

    # Query uses c.region — should fail
    sql_denied = """
        SELECT o.order_id, c.name, c.region
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
    """
    p2 = SQLParser("s", sql_denied, "duckdb")
    physical2 = p2.get_physical_tables()
    result2 = _validate_columns(physical2, stored["tables"])
    assert result2 is not None
    assert "region" in result2
    assert "customers" in result2


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

_ALL_TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_")]

if __name__ == "__main__":
    passed = 0
    failed = 0
    for test_fn in _ALL_TESTS:
        name = test_fn.__name__
        try:
            test_fn()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  FAIL [{name}]: {e}")

    print("=" * 72)
    print(f"RESULTS: {passed} passed, {failed} failed, {len(_ALL_TESTS)} total")
    print("=" * 72)
    sys.exit(0 if failed == 0 else 1)
