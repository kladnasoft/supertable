"""Adversarial contract tests for wildcard RBAC exclusions.

The policy exercised here deliberately grants the wildcard default and then
narrows two named tables::

    {
        "*": {"columns": ["*"], "filters": ["*"]},
        "account": {"access": "deny"},
        "card": {
            "columns": ["*"],
            "exclude_columns": ["pan", "cvv"],
            "filters": ["*"],
        },
    }

These are security tests, not just serialization tests.  They drive the SQL
parser, access-control decision, and a real DuckDB RBAC view together so that
an omission in any one layer cannot turn into a column disclosure.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest

import supertable.rbac.access_control as access_control
from supertable.data_classes import (
    RbacViewDef,
    Reflection,
    SuperSnapshot,
    TableDefinition,
)
from supertable.data_reader import _validated_share_row_filter
from supertable.engine.engine_common import (
    create_rbac_view,
    rewrite_query_with_hashed_tables,
)
from supertable.engine.spark_thrift import _spark_create_rbac_view
from supertable.rbac.filter_builder import FilterBuilder
from supertable.rbac.permissions import RoleType
from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.utils.sql_parser import SQLParser


ORG = "org"
SUPER = "shop"
ROLE = "wildcard_except_secrets"


def _tables_policy() -> dict:
    return {
        "*": {"columns": ["*"], "filters": ["*"]},
        "account": {"access": "deny"},
        "card": {
            "columns": ["*"],
            # ``not_yet_created`` seals the schema-evolution contract: a deny
            # for an absent column is valid and starts applying if it appears.
            "exclude_columns": ["pan", "cvv", "not_yet_created"],
            "filters": ["*"],
        },
    }


def _role(
    role_type: str = "reader",
    *,
    tables: dict | None = None,
    content_hash: str = "policy-v1",
) -> dict:
    return {
        "role": role_type,
        "role_name": ROLE,
        "tables": _tables_policy() if tables is None else tables,
        "content_hash": content_hash,
    }


class _StaticRoleManager:
    def __init__(self, role_info: dict):
        self.role_info = role_info

    def get_role_by_name(self, _role_name: str) -> dict:
        return self.role_info


def _install_roles(monkeypatch, roles_by_super: dict[str, dict]) -> None:
    """Patch access control with per-super role documents.

    A factory instead of one static mock is important for the cross-super test:
    it proves policy is resolved in the namespace of every physical table.
    """

    folded = {str(k).casefold(): v for k, v in roles_by_super.items()}

    def factory(*, super_name: str, organization: str):
        assert organization == ORG
        info = folded.get(super_name.casefold(), {})
        return _StaticRoleManager(info)

    monkeypatch.setattr(access_control, "RoleManager", factory)
    # Operation tests exercise RBAC only; clone/replica state has its own suite.
    monkeypatch.setattr(
        access_control, "_check_readonly_guard", lambda *_args, **_kwargs: None,
    )


def _parser(query: str, *, super_name: str = SUPER) -> SQLParser:
    return SQLParser(super_name, query, "duckdb")


def _restrict(query: str, monkeypatch, role_info: dict | None = None):
    _install_roles(monkeypatch, {SUPER: role_info or _role()})
    parser = _parser(query)
    views = access_control.restrict_read_access(
        SUPER,
        ORG,
        ROLE,
        parser.get_table_tuples(),
        parser.get_physical_tables(),
    )
    return parser, views


def _base_table_for(simple_name: str) -> str | None:
    return {
        "account": "base_account",
        "card": "base_card",
        "ledger": "base_ledger",
    }.get(simple_name.casefold())


@contextmanager
def _duckdb_catalog():
    con = duckdb.connect()
    try:
        con.execute(
            'CREATE TABLE base_account '
            '(id INTEGER, name VARCHAR, email VARCHAR)'
        )
        con.execute(
            "INSERT INTO base_account VALUES "
            "(1, 'alice', 'alice@example.test'), "
            "(2, 'bob', 'bob@example.test')"
        )
        con.execute(
            'CREATE TABLE base_card '
            '(id INTEGER, label VARCHAR, pan VARCHAR, cvv VARCHAR)'
        )
        con.execute(
            "INSERT INTO base_card VALUES "
            "(1, 'personal', '4111111111111111', '123'), "
            "(2, 'business', '5555555555554444', '999')"
        )
        con.execute(
            'CREATE TABLE base_ledger '
            '(id INTEGER, label VARCHAR, cvv VARCHAR)'
        )
        con.execute(
            "INSERT INTO base_ledger VALUES "
            "(1, 'credit', 'ledger-visible'), (2, 'debit', 'ledger-two')"
        )
        yield con
    finally:
        con.close()


def _execute_secured(query: str, monkeypatch, role_info: dict | None = None):
    """Execute a query through parser -> policy -> real RBAC views."""

    parser, views = _restrict(query, monkeypatch, role_info)
    alias_to_view: dict[str, str] = {}

    with _duckdb_catalog() as con:
        for ordinal, td in enumerate(parser.get_table_tuples()):
            base = _base_table_for(td.simple_name)
            if base is None:  # CTE/derived-table alias, not a physical source.
                continue
            view_def = views.get(td.alias)
            if view_def is None:
                alias_to_view[td.alias] = base
                continue
            view_name = f"rbac_{ordinal}_{td.alias.casefold()}"
            create_rbac_view(con, base, view_name, view_def)
            alias_to_view[td.alias] = view_name

        sql = rewrite_query_with_hashed_tables(
            query,
            alias_to_view,
            parsed_expression=parser._parsed,
        )
        return con.execute(sql).fetchdf()


# ---------------------------------------------------------------------------
# Policy normalization and precedence
# ---------------------------------------------------------------------------


def test_prepare_preserves_wildcard_exclusion_contract():
    rcs = RowColumnSecurity(role="reader", tables=_tables_policy())
    rcs.prepare()

    policy = rcs.to_json()["tables"]
    assert policy["account"]["access"] == "deny"
    assert policy["card"]["columns"] == ["*"]
    assert {c.casefold() for c in policy["card"]["exclude_columns"]} == {
        "pan", "cvv", "not_yet_created",
    }
    assert rcs.content_hash


def test_exact_allow_entry_replaces_wildcard_deny(monkeypatch):
    tables = {
        "*": {"access": "deny"},
        "ledger": {"columns": ["*"], "filters": ["*"]},
    }
    parser, views = _restrict(
        "SELECT * FROM ledger", monkeypatch, _role(tables=tables),
    )
    assert parser.get_physical_tables()[0].simple_name == "ledger"
    assert views == {}


@pytest.mark.parametrize("spelling", ["account", "ACCOUNT", '"AcCoUnT"'])
def test_exact_table_deny_overrides_wildcard_case_insensitively(
    spelling, monkeypatch,
):
    with pytest.raises(PermissionError):
        _restrict(f"SELECT * FROM {spelling}", monkeypatch)


def test_reader_wildcard_still_grants_unmentioned_table(monkeypatch):
    result = _execute_secured("SELECT id, label FROM ledger ORDER BY id", monkeypatch)
    assert result.to_dict("list") == {
        "id": [1, 2],
        "label": ["credit", "debit"],
    }


def test_exclusion_wins_over_explicit_column_inclusion(monkeypatch):
    tables = {
        "card": {
            "columns": ["id", "label", "cvv"],
            "exclude_columns": ["CVV"],
            "filters": ["*"],
        }
    }
    with pytest.raises(PermissionError):
        _restrict(
            "SELECT cvv FROM card", monkeypatch, _role(tables=tables),
        )


# ---------------------------------------------------------------------------
# SQL attack matrix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "SELECT cvv FROM card",
        "SELECT c.cvv AS harmless FROM card AS c",
        "SELECT id FROM card WHERE cvv = '123'",
        "SELECT id FROM card ORDER BY cvv",
        "SELECT cvv, COUNT(*) FROM card GROUP BY cvv",
        "SELECT id FROM card GROUP BY id HAVING MAX(cvv) = '999'",
        "SELECT SHA256(cvv) FROM card",
        "SELECT LAG(cvv) OVER (ORDER BY id) FROM card",
        "SELECT c.id FROM card c JOIN ledger l ON c.cvv = l.cvv",
        "SELECT a.id FROM card a JOIN card b ON a.id = b.id WHERE b.cvv = '123'",
    ],
)
def test_denied_column_is_rejected_in_every_explicit_clause(query, monkeypatch):
    with pytest.raises(PermissionError):
        _restrict(query, monkeypatch)


@pytest.mark.parametrize(
    "query",
    [
        "WITH x AS (SELECT cvv FROM card) SELECT * FROM x",
        "SELECT * FROM (SELECT cvv FROM card) AS x",
        "SELECT (SELECT cvv FROM card LIMIT 1) FROM ledger",
        "SELECT * FROM card c JOIN ledger l USING (cvv)",
    ],
)
def test_scope_or_star_cannot_bypass_projection_even_if_parser_cannot_enumerate(
    query, monkeypatch,
):
    # Some star/CTE shapes intentionally carry columns=[] from SQLParser.  The
    # execution boundary remains authoritative: the denied field is absent
    # from the RBAC view and the query must not return any result.
    with pytest.raises((PermissionError, duckdb.Error)):
        _execute_secured(query, monkeypatch)


def test_parser_attributes_unqualified_scalar_subquery_column_to_inner_table():
    parser = _parser("SELECT (SELECT cvv FROM card LIMIT 1) FROM ledger")
    physical = {
        td.simple_name.casefold(): {c.casefold() for c in td.columns}
        for td in parser.get_physical_tables()
    }
    assert "cvv" in physical["card"]


@pytest.mark.parametrize("spelling", ["cvv", "CVV", '"CvV"'])
def test_column_exclusion_is_case_insensitive_and_quote_safe(spelling, monkeypatch):
    with pytest.raises(PermissionError):
        _restrict(f"SELECT {spelling} FROM card", monkeypatch)


def test_select_star_projects_everything_except_denied_columns(monkeypatch):
    result = _execute_secured("SELECT * FROM card ORDER BY id", monkeypatch)
    assert list(result.columns) == ["id", "label"]
    assert result.to_dict("list") == {
        "id": [1, 2],
        "label": ["personal", "business"],
    }


def test_table_star_projects_everything_except_denied_columns(monkeypatch):
    result = _execute_secured("SELECT c.* FROM card AS c ORDER BY c.id", monkeypatch)
    assert list(result.columns) == ["id", "label"]


def test_allowed_alias_cte_and_join_still_execute(monkeypatch):
    result = _execute_secured(
        "WITH c AS (SELECT id, label FROM card) "
        "SELECT c.label, l.label AS ledger_label "
        "FROM c JOIN ledger l ON c.id = l.id ORDER BY c.id",
        monkeypatch,
    )
    assert result.to_dict("records") == [
        {"label": "personal", "ledger_label": "credit"},
        {"label": "business", "ledger_label": "debit"},
    ]


def test_same_named_cte_inner_physical_source_is_visible_to_rbac(monkeypatch):
    query = (
        "WITH account AS (SELECT name FROM account) "
        "SELECT name FROM account ORDER BY name"
    )
    parser = _parser(query)

    # The inner account is a catalog table. Only the outer account is a CTE
    # reference, even though both nodes have the same spelling.
    assert [
        (td.simple_name.casefold(), {c.casefold() for c in td.columns})
        for td in parser.get_physical_tables()
    ] == [("account", {"name"})]

    # The exact-table deny must therefore be evaluated instead of the source
    # disappearing behind name-only CTE filtering.
    with pytest.raises(PermissionError):
        _restrict(query, monkeypatch)


def test_same_named_cte_rewrite_secures_leaf_and_preserves_cte_reference():
    query = (
        "WITH account AS (SELECT name FROM account) "
        "SELECT name FROM account ORDER BY name"
    )
    parser = _parser(query)
    rewritten = rewrite_query_with_hashed_tables(
        query,
        {"account": "base_account"},
        parsed_expression=parser._parsed,
    )

    assert rewritten == (
        "WITH account AS (SELECT name FROM base_account AS account) "
        "SELECT name FROM account ORDER BY name"
    )
    # DuckDB resolves the rewritten leaf to the protected catalog object while
    # continuing to resolve the outer source to the query-local CTE.
    with _duckdb_catalog() as con:
        assert con.execute(rewritten).fetchall() == [("alice",), ("bob",)]


def test_row_filter_may_use_a_hidden_column_without_exposing_it(monkeypatch):
    tables = {
        "card": {
            "columns": ["*"],
            "exclude_columns": ["cvv", "pan"],
            "filters": [
                {"cvv": {"operation": "=", "type": "value", "value": "123"}}
            ],
        }
    }
    result = _execute_secured(
        "SELECT * FROM card", monkeypatch, _role(tables=tables),
    )
    assert list(result.columns) == ["id", "label"]
    assert result.to_dict("records") == [{"id": 1, "label": "personal"}]


def _execute_with_reader_style_projection(
    query: str,
    monkeypatch,
    role_info: dict,
):
    """Model the executor's query-column projection before its RBAC view."""

    _install_roles(monkeypatch, {SUPER: role_info})
    parser = _parser(query)
    physical = parser.get_physical_tables()
    views = access_control.restrict_read_access(
        SUPER, ORG, ROLE, parser.get_table_tuples(), physical,
    )
    assert len(physical) == 1 and physical[0].simple_name.casefold() == "card"

    with _duckdb_catalog() as con:
        needed = list(physical[0].columns)
        # DuckDB/Spark executors must widen before they materialize the base
        # relation whenever a role/share row filter has hidden dependencies.
        if views["card"].where_clause:
            needed = []
        if needed:
            projected = ", ".join('"' + c.replace('"', '""') + '"' for c in needed)
        else:
            projected = "*"
        con.execute(
            f"CREATE VIEW projected_card AS SELECT {projected} FROM base_card"
        )
        create_rbac_view(con, "projected_card", "secured_card", views["card"])
        rewritten = rewrite_query_with_hashed_tables(
            query,
            {"card": "secured_card"},
            parsed_expression=parser._parsed,
        )
        return con.execute(rewritten).fetchdf(), needed


def test_explicit_allowlist_does_not_require_unrequested_columns_in_reflection(
    monkeypatch,
):
    policy = _role(
        tables={
            "card": {
                "columns": ["id", "label"],
                "filters": ["*"],
            }
        }
    )
    result, _needed = _execute_with_reader_style_projection(
        "SELECT id FROM card", monkeypatch, policy,
    )
    assert result["id"].tolist() == [1, 2]


def test_row_filter_dependency_is_present_before_rbac_view_creation(monkeypatch):
    policy = _role(
        tables={
            "card": {
                "columns": ["id", "label"],
                "exclude_columns": ["cvv"],
                "filters": [
                    {
                        "cvv": {
                            "operation": "=",
                            "type": "value",
                            "value": "123",
                        }
                    }
                ],
            }
        }
    )
    result, needed = _execute_with_reader_style_projection(
        "SELECT id FROM card", monkeypatch, policy,
    )
    # Implementations may add just the dependency or conservatively request all
    # columns (the parser's [] sentinel).  Both preserve correctness.
    assert needed == [] or "cvv" in {c.casefold() for c in needed}
    assert result["id"].tolist() == [1]


def test_real_duckdb_executor_loads_hidden_row_filter_dependency(tmp_path):
    """Exercise the production source-projection/view/rewrite pipeline."""
    from supertable.engine.duckdb_engine import DuckDB

    parquet_path = tmp_path / "card.parquet"
    pl.DataFrame(
        {
            "id": [1, 2],
            "label": ["personal", "business"],
            "pan": ["4111", "5555"],
            "cvv": ["123", "999"],
        }
    ).write_parquet(parquet_path)

    parser = _parser("SELECT id FROM card ORDER BY id")
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=parquet_path.stat().st_size,
        total_reflections=1,
        supers=[
            SuperSnapshot(
                SUPER,
                "card",
                1,
                files=[str(parquet_path)],
                columns={"id", "label", "pan", "cvv"},
            )
        ],
        rbac_views={
            "card": RbacViewDef(
                allowed_columns=["id", "label"],
                where_clause='"cvv" = \'123\'',
                excluded_columns=["pan", "cvv"],
            )
        },
    )
    query_manager = SimpleNamespace(
        temp_dir=str(tmp_path),
        query_plan_path=str(tmp_path / "duckdb-profile.json"),
    )
    executor = DuckDB(storage=MagicMock())
    try:
        result = executor.execute(
            reflection,
            parser,
            query_manager,
            lambda _stage: None,
        )
    finally:
        executor._reset_connection()

    assert result.to_dict("records") == [{"id": 1}]


def test_cte_output_alias_is_not_projected_from_physical_parquet(tmp_path):
    """Scope hardening must not turn derived aliases into source columns."""
    from supertable.engine.duckdb_engine import DuckDB

    parquet_path = tmp_path / "card-cte.parquet"
    pl.DataFrame(
        {
            "id": [1, 2],
            "label": ["personal", "business"],
            "cvv": ["123", "999"],
            "__rowid__": [1, 2],
            "__timestamp__": [1, 2],
        }
    ).write_parquet(parquet_path)
    parser = _parser(
        "WITH summary AS ("
        "SELECT label, COUNT(*) AS total FROM card GROUP BY label"
        ") SELECT label, total FROM summary ORDER BY label"
    )
    physical = {
        td.simple_name.casefold(): {name.casefold() for name in td.columns}
        for td in parser.get_physical_tables()
    }
    assert "total" not in physical["card"]

    reflection = Reflection(
        storage_type="local",
        reflection_bytes=parquet_path.stat().st_size,
        total_reflections=1,
        supers=[
            SuperSnapshot(
                SUPER,
                "card",
                1,
                files=[str(parquet_path)],
                columns={"id", "label", "cvv"},
            )
        ],
        rbac_views={
            "card": RbacViewDef(
                allowed_columns=["*"],
                excluded_columns=["cvv"],
            )
        },
    )
    executor = DuckDB(storage=MagicMock())
    try:
        result = executor.execute(
            reflection,
            parser,
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "cte-profile.json"),
            ),
            lambda _stage: None,
        )
    finally:
        executor._reset_connection()

    assert result.to_dict("records") == [
        {"label": "business", "total": 1},
        {"label": "personal", "total": 1},
    ]


def test_spark_row_filter_uses_identifier_not_string_literal_quoting():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("id",), ("cvv",)]
    _spark_create_rbac_view(
        cursor,
        "base_card",
        "secured_card",
        RbacViewDef(
            allowed_columns=["id"],
            where_clause='"cvv" = \'123\'',
            excluded_columns=["cvv"],
            filter_spec=[
                {"cvv": {"operation": "=", "type": "value", "value": "123"}}
            ],
        ),
    )
    sql = cursor.execute.call_args_list[-1][0][0]
    assert "WHERE `cvv` = '123'" in sql


def test_spark_role_filter_cannot_discard_merged_share_filter():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("id",), ("tenant",), ("shared",)]
    _spark_create_rbac_view(
        cursor,
        "base_card",
        "secured_card",
        RbacViewDef(
            allowed_columns=["id"],
            where_clause='("tenant" = \'acme\') AND ("shared" = TRUE)',
            filter_spec=[
                {
                    "tenant": {
                        "operation": "=",
                        "type": "value",
                        "value": "acme",
                    }
                }
            ],
        ),
    )
    sql = cursor.execute.call_args_list[-1][0][0]
    assert "`tenant` = 'acme'" in sql
    assert "shared" in sql.casefold()


def test_linked_share_filter_is_ast_validated_and_canonicalized():
    predicate = _validated_share_row_filter(
        '"tenant" = \'acme\' AND ("shared" = TRUE)'
    )
    assert "tenant" in predicate
    assert "shared" in predicate


@pytest.mark.parametrize(
    "predicate",
    [
        "TRUE; DROP TABLE card",
        "id IN (SELECT id FROM account)",
        "EXISTS (SELECT 1 FROM account)",
        "",
    ],
)
def test_linked_share_filter_rejects_statements_and_other_sources(predicate):
    with pytest.raises(RuntimeError, match="row filter"):
        _validated_share_row_filter(predicate)


@pytest.mark.parametrize(
    ("operation", "value", "expected"),
    [
        ("IN", [1, 2], [1, 2]),
        ("NOT IN", [1, 2], [3]),
        ("BETWEEN", [1, 2], [1, 2]),
        ("NOT BETWEEN", [1, 2], [3]),
    ],
)
def test_complex_filter_operator_is_rejected_or_has_valid_typed_grammar(
    operation, value, expected,
):
    filters = [
        {"id": {"operation": operation, "type": "value", "value": value}}
    ]
    try:
        query = FilterBuilder("items", ["id"], {"filters": filters}).filter_query
    except (TypeError, ValueError):
        return  # Explicit rejection is safer than emitting malformed SQL.

    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE items (id INTEGER)")
        con.execute("INSERT INTO items VALUES (1), (2), (3)")
        assert [row[0] for row in con.execute(query).fetchall()] == expected
    finally:
        con.close()


def test_filter_reference_rejects_sql_expression_injection():
    with pytest.raises(ValueError):
        FilterBuilder(
            "items",
            ["id"],
            {
                "filters": [
                    {
                        "id": {
                            "operation": "=",
                            "type": "reference",
                            "value": "id OR 1=1",
                        }
                    }
                ]
            },
        )


def test_absent_excluded_column_does_not_break_view_and_future_column_is_hidden():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE evolving (id INTEGER, label VARCHAR)")
        view_def = RbacViewDef(
            allowed_columns=["*"],
            where_clause="",
            excluded_columns=["future_secret"],
        )
        create_rbac_view(con, "evolving", "rbac_before", view_def)
        assert [r[0] for r in con.execute("DESCRIBE rbac_before").fetchall()] == [
            "id", "label",
        ]

        con.execute("ALTER TABLE evolving ADD COLUMN future_secret VARCHAR")
        create_rbac_view(con, "evolving", "rbac_after", view_def)
        assert [r[0] for r in con.execute("DESCRIBE rbac_after").fetchall()] == [
            "id", "label",
        ]
    finally:
        con.close()


def test_projection_quotes_reserved_and_embedded_quote_identifiers():
    con = duckdb.connect()
    try:
        con.execute(
            'CREATE TABLE awkward ("select" INTEGER, "quoted""name" VARCHAR, secret VARCHAR)'
        )
        create_rbac_view(
            con,
            "awkward",
            "rbac_awkward",
            RbacViewDef(allowed_columns=["*"], excluded_columns=["secret"]),
        )
        assert [r[0] for r in con.execute("DESCRIBE rbac_awkward").fetchall()] == [
            "select", 'quoted"name',
        ]
    finally:
        con.close()


def test_all_columns_excluded_fails_closed_instead_of_emitting_invalid_sql():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE only_secret (secret VARCHAR)")
        with pytest.raises((PermissionError, ValueError)):
            create_rbac_view(
                con,
                "only_secret",
                "rbac_empty",
                RbacViewDef(
                    allowed_columns=["*"], excluded_columns=["secret"],
                ),
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Mutations, role types, and cross-super isolation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("checker", "role_type"),
    [
        (access_control.check_write_access, "writer"),
        (access_control.check_meta_access, "reader"),
        (access_control.check_control_access, "admin"),
    ],
)
def test_explicit_table_deny_blocks_every_operation_under_wildcard(
    checker, role_type, monkeypatch,
):
    _install_roles(monkeypatch, {SUPER: _role(role_type)})
    with pytest.raises(PermissionError):
        checker(SUPER, ORG, ROLE, "ACCOUNT")


def test_scoped_column_policy_is_read_only_for_writes(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role("writer")})
    # Publishing an incoming schema can indirectly alter an omitted hidden
    # field, so the current mutation pipeline cannot safely support even an
    # otherwise-allowed subset under a scoped column policy.
    with pytest.raises(PermissionError):
        access_control.check_write_access(
            SUPER, ORG, ROLE, "card", columns=["id", "label"],
        )
    with pytest.raises(PermissionError):
        access_control.check_write_access(
            SUPER, ORG, ROLE, "CARD", columns=["id", "CvV"],
        )


def _writer_shell(*, table_exists: bool):
    from supertable.data_writer import DataWriter

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        super_name=SUPER,
        organization=ORG,
        storage=MagicMock(),
    )
    writer.catalog = MagicMock()
    writer.catalog.leaf_exists.return_value = table_exists
    writer._table_config_cache = {}
    return writer


def test_real_data_writer_rejects_excluded_incoming_column_before_mutation(
    monkeypatch,
):
    import pyarrow as pa

    _install_roles(monkeypatch, {SUPER: _role("writer")})
    writer = _writer_shell(table_exists=True)
    incoming = pa.table({"id": [1], "CvV": ["123"]})

    with pytest.raises(PermissionError):
        writer.write(ROLE, "CARD", incoming, overwrite_columns=[])

    writer.catalog.acquire_simple_lock.assert_not_called()
    writer.super_table.storage.write_parquet.assert_not_called()


def test_real_data_writer_rejects_omission_driven_schema_change_under_scope(
    monkeypatch,
):
    """Omitting hidden fields must not let a scoped writer republish schema."""
    import pyarrow as pa

    _install_roles(monkeypatch, {SUPER: _role("writer")})
    writer = _writer_shell(table_exists=True)

    # Every supplied column is visible, but DataWriter publishes the incoming
    # schema as the latest schema. Accepting this would implicitly remove or
    # reshape excluded fields that the writer is not allowed to control.
    incoming = pa.table({"id": [1], "label": ["replacement"]})
    with pytest.raises(PermissionError):
        writer.write(ROLE, "card", incoming, overwrite_columns=[])

    writer.catalog.acquire_simple_lock.assert_not_called()
    writer.super_table.storage.write_parquet.assert_not_called()


def test_real_data_writer_rejects_row_filtered_write_before_mutation(monkeypatch):
    import pyarrow as pa

    tables = {
        "card": {
            "columns": ["*"],
            "filters": [
                {"id": {"operation": "=", "type": "value", "value": 1}},
            ],
        },
    }
    _install_roles(monkeypatch, {SUPER: _role("writer", tables=tables)})
    writer = _writer_shell(table_exists=True)

    # DataWriter cannot currently certify an update against both the old and
    # new row sets, so a row-filtered role is read-only even for matching data.
    with pytest.raises(PermissionError):
        writer.write(
            ROLE,
            "card",
            pa.table({"id": [1], "label": ["visible-row"]}),
            overwrite_columns=[],
        )

    writer.catalog.acquire_simple_lock.assert_not_called()
    writer.super_table.storage.write_parquet.assert_not_called()


def test_real_data_writer_applies_create_and_column_policy_before_bootstrap(
    monkeypatch,
):
    import pyarrow as pa

    # WRITER intentionally has WRITE but not CREATE in the permission matrix.
    _install_roles(monkeypatch, {SUPER: _role("writer")})
    writer = _writer_shell(table_exists=False)
    with pytest.raises(PermissionError):
        writer.write(
            ROLE,
            "ledger",
            pa.table({"id": [1]}),
            overwrite_columns=[],
        )
    writer.catalog.acquire_simple_lock.assert_not_called()

    # ADMIN may CREATE, but the exact table's deny overlay still wins before
    # SimpleTable can mint its initial snapshot.
    _install_roles(monkeypatch, {SUPER: _role("admin")})
    writer = _writer_shell(table_exists=False)
    with pytest.raises(PermissionError):
        writer.write(
            ROLE,
            "card",
            pa.table({"id": [1], "cvv": ["123"]}),
            overwrite_columns=[],
        )
    writer.catalog.acquire_simple_lock.assert_not_called()


def test_write_authorization_cannot_race_into_implicit_table_creation(
    monkeypatch,
):
    import pyarrow as pa
    import supertable.data_writer as writer_module

    class ReachedConstructor(RuntimeError):
        pass

    _install_roles(
        monkeypatch,
        {
            SUPER: _role(
                "writer",
                tables={"card": {"columns": ["*"], "filters": ["*"]}},
            )
        },
    )
    writer = _writer_shell(table_exists=True)
    writer.catalog.get_table_config.return_value = {}
    writer.catalog.acquire_simple_lock.return_value = "lock-token"
    writer.catalog.release_simple_lock.return_value = True
    simple_table = MagicMock(side_effect=ReachedConstructor)
    monkeypatch.setattr(writer_module, "SimpleTable", simple_table)

    with pytest.raises(ReachedConstructor):
        writer.write(
            ROLE,
            "card",
            pa.table({"id": [1]}),
            overwrite_columns=[],
        )

    # This request was authorized for WRITE on an existing target, not CREATE.
    # If the leaf disappears before construction, fail with TableNotFound
    # instead of silently minting a replacement under weaker permission.
    assert simple_table.call_args.kwargs == {"create_if_missing": False}


def _simple_table_shell(*, simple_name: str = "account"):
    from supertable.simple_table import SimpleTable

    table = SimpleTable.__new__(SimpleTable)
    table.super_table = SimpleNamespace(
        super_name=SUPER,
        organization=ORG,
        storage=MagicMock(),
    )
    table.identity = "tables"
    table.simple_name = simple_name
    table.storage = table.super_table.storage
    table.catalog = MagicMock()
    return table


def test_simple_table_delete_honours_exact_control_deny_before_mutation(
    monkeypatch,
):
    _install_roles(monkeypatch, {SUPER: _role("admin")})
    table = _simple_table_shell()

    with pytest.raises(PermissionError):
        table.delete(ROLE)

    table.storage.exists.assert_not_called()
    table.storage.delete.assert_not_called()
    table.catalog.delete_simple_table.assert_not_called()


def test_row_filtered_table_cannot_be_deleted_outside_visible_rows(monkeypatch):
    tables = {
        "card": {
            "columns": ["*"],
            "filters": [
                {"id": {"operation": "=", "type": "value", "value": 1}},
            ],
        },
    }
    # ADMIN has CONTROL in the role matrix, so this denial proves the row
    # scope itself makes mutations read-only rather than relying on role type.
    _install_roles(monkeypatch, {SUPER: _role("admin", tables=tables)})
    table = _simple_table_shell(simple_name="card")

    with pytest.raises(PermissionError):
        table.delete(ROLE)

    table.storage.exists.assert_not_called()
    table.storage.delete.assert_not_called()
    table.catalog.delete_simple_table.assert_not_called()


def test_super_table_delete_requires_superadmin_even_for_unrestricted_admin(
    monkeypatch,
):
    from supertable.super_table import SuperTable

    policy = {
        "*": {"columns": ["*"], "filters": ["*"]},
    }
    _install_roles(
        monkeypatch,
        {SUPER: _role("admin", tables=policy)},
    )
    table = SuperTable.__new__(SuperTable)
    table.super_name = SUPER
    table.organization = ORG
    table.storage = MagicMock()
    table.catalog = MagicMock()

    with pytest.raises(PermissionError):
        table.delete(ROLE)

    table.storage.exists.assert_not_called()
    table.storage.delete.assert_not_called()
    table.catalog.delete_super_table.assert_not_called()


def test_superadmin_can_delete_super_table_namespace(monkeypatch):
    from supertable.super_table import SuperTable

    _install_roles(monkeypatch, {SUPER: _role("superadmin")})
    table = SuperTable.__new__(SuperTable)
    table.super_name = SUPER
    table.organization = ORG
    table.storage = MagicMock()
    table.storage.exists.return_value = False
    table.catalog = MagicMock()

    table.delete(ROLE)

    table.storage.delete.assert_not_called()
    table.catalog.delete_super_table.assert_called_once_with(ORG, SUPER)


def test_super_table_delete_cannot_bypass_a_denied_child_table(monkeypatch):
    """A parent delete is also a CONTROL operation on every child it erases."""
    from supertable import redis_keys as RK
    from supertable.super_table import SuperTable

    _install_roles(monkeypatch, {SUPER: _role("admin")})
    table = SuperTable.__new__(SuperTable)
    table.super_name = SUPER
    table.organization = ORG
    table.storage = MagicMock()
    table.catalog = MagicMock()
    table.catalog.scan_leaf_keys.return_value = iter(
        [
            RK.meta_leaf(ORG, SUPER, "account"),
            RK.meta_leaf(ORG, SUPER, "card"),
        ]
    )

    with pytest.raises(PermissionError):
        table.delete(ROLE)

    table.storage.exists.assert_not_called()
    table.storage.delete.assert_not_called()
    table.catalog.delete_super_table.assert_not_called()


def test_staging_constructor_and_open_are_read_only(monkeypatch):
    import supertable.staging_area as staging_module

    storage = MagicMock()
    catalog = MagicMock()
    catalog.root_exists.return_value = True
    monkeypatch.setattr(staging_module, "get_storage", lambda: storage)
    monkeypatch.setattr(staging_module, "RedisCatalog", lambda: catalog)

    manager = staging_module.Staging(organization=ORG, super_name=SUPER)
    stage = manager.open("incoming")

    assert stage.stage_dir.endswith("/staging/incoming")
    assert catalog.root_exists.call_count == 2
    storage.makedirs.assert_not_called()
    storage.write_json.assert_not_called()
    catalog.upsert_staging_meta.assert_not_called()
    catalog.r.set.assert_not_called()


def _user_manager_shell(catalog):
    from supertable.rbac.user_manager import UserManager

    manager = UserManager.__new__(UserManager)
    manager.super_name = SUPER
    manager.organization = ORG
    manager._catalog = catalog
    return manager


def test_existing_default_superuser_is_repaired_during_manager_bootstrap():
    from supertable.rbac.user_manager import UserManager

    catalog = MagicMock()
    catalog.r.exists.return_value = True
    catalog.rbac_get_user_id_by_username.return_value = "user-1"
    catalog.rbac_get_superadmin_role_id.return_value = "role-sa"
    catalog.get_user_details.return_value = {
        "user_id": "user-1",
        "username": "superuser",
        "roles": [],
    }

    UserManager(SUPER, ORG, redis_catalog=catalog)

    catalog.rbac_add_role_to_user.assert_called_once()
    args, kwargs = catalog.rbac_add_role_to_user.call_args
    assert args == (ORG, SUPER, "user-1", "role-sa")
    context = kwargs["action_context"]
    assert context.actor_type == "system"
    assert context.cause == "rbac_bootstrap_repair"
    assert context.context_missing is False


def test_default_superuser_cannot_be_renamed_or_deprivileged():
    catalog = MagicMock()
    catalog.get_user_details.return_value = {
        "user_id": "user-1",
        "username": "superuser",
        "roles": ["role-sa"],
    }
    catalog.rbac_get_superadmin_role_id.return_value = "role-sa"
    manager = _user_manager_shell(catalog)

    with pytest.raises(ValueError):
        manager.modify_user("user-1", {"username": "operator"})
    with pytest.raises(ValueError):
        manager.modify_user("user-1", {"roles": []})
    with pytest.raises(ValueError):
        manager.remove_role("user-1", "role-sa")

    catalog.rbac_rename_user.assert_not_called()
    catalog.rbac_update_user.assert_not_called()
    catalog.rbac_remove_role_from_user.assert_not_called()


def test_bulk_role_removal_cannot_strip_default_superadmin():
    catalog = MagicMock()
    catalog.rbac_list_user_ids.return_value = ["user-1"]
    catalog.get_user_details.return_value = {
        "user_id": "user-1",
        "username": "superuser",
        "roles": ["role-sa"],
    }
    catalog.rbac_get_superadmin_role_id.return_value = "role-sa"
    manager = _user_manager_shell(catalog)

    with pytest.raises(ValueError):
        manager.remove_role_from_users("role-sa")

    catalog.rbac_remove_role_from_user.assert_not_called()


def test_admin_is_powerful_but_scoped_by_explicit_policy(monkeypatch):
    admin = _role("admin")
    with pytest.raises(PermissionError):
        _restrict("SELECT * FROM account", monkeypatch, admin)
    _, views = _restrict("SELECT * FROM card", monkeypatch, admin)
    assert views["card"].excluded_columns


def test_superadmin_is_the_only_unconditional_bypass(monkeypatch):
    superadmin = _role("superadmin")
    parser, views = _restrict("SELECT cvv FROM card", monkeypatch, superadmin)
    assert parser.get_physical_tables()[0].columns == ["cvv"]
    assert views == {}


def test_cross_super_uses_foreign_policy_not_home_wildcard(monkeypatch):
    home = _role(tables={"*": {"columns": ["*"], "filters": ["*"]}})
    foreign = _role(
        tables={
            "card": {
                "columns": ["*"],
                "exclude_columns": ["cvv"],
                "filters": ["*"],
            }
        },
    )
    _install_roles(monkeypatch, {SUPER: home, "vault": foreign})
    parser = _parser("SELECT v.cvv FROM vault.card AS v")
    with pytest.raises(PermissionError):
        access_control.restrict_read_access(
            SUPER,
            ORG,
            ROLE,
            parser.get_table_tuples(),
            parser.get_physical_tables(),
        )


def test_cross_super_missing_role_fails_closed(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role()})
    parser = _parser("SELECT * FROM vault.ledger")
    with pytest.raises(PermissionError):
        access_control.restrict_read_access(
            SUPER,
            ORG,
            ROLE,
            parser.get_table_tuples(),
            parser.get_physical_tables(),
        )


# ---------------------------------------------------------------------------
# Malformed/corrupt policy documents must never degrade into a wildcard grant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tables",
    [
        ["*"],
        {"card": None},
        {"card": []},
        {"card": {"access": "sometimes"}},
        {"card": {"access": "deny", "columns": ["id"]}},
        {"card": {"columns": "*"}},
        {"card": {"columns": ["*", "id"]}},
        {"card": {"columns": ["id", 1]}},
        {"card": {"exclude_columns": "cvv"}},
        {"card": {"exclude_columns": ["cvv", 1]}},
        {"card": {"exclude_columns": ["*", "cvv"]}},
        {"card": {"exclude_column": ["cvv"]}},  # typo must not grant
        {
            "Card": {"columns": ["*"]},
            "card": {"access": "deny"},
        },
    ],
)
def test_prepare_rejects_malformed_or_ambiguous_policies(tables):
    with pytest.raises((TypeError, ValueError)):
        RowColumnSecurity(role="reader", tables=tables).prepare()


@pytest.mark.parametrize(
    "bad_entry",
    [
        {"columns": "*"},
        {"columns": ["*", "cvv"]},
        {"exclude_columns": "cvv"},
        {"access": "unknown"},
    ],
)
def test_corrupt_persisted_role_fails_closed_at_enforcement(bad_entry, monkeypatch):
    # Bypass RowColumnSecurity to model an old/tampered Redis role document.
    corrupt = _role(tables={"*": bad_entry})
    with pytest.raises((PermissionError, TypeError, ValueError)):
        _restrict("SELECT * FROM card", monkeypatch, corrupt)


def test_case_colliding_persisted_table_keys_fail_closed(monkeypatch):
    corrupt = _role(
        tables={
            "*": {"columns": ["*"], "filters": ["*"]},
            "Card": {"columns": ["*"], "filters": ["*"]},
            "card": {"access": "deny"},
        }
    )
    with pytest.raises((PermissionError, ValueError)):
        _restrict("SELECT * FROM CARD", monkeypatch, corrupt)


def test_role_update_cannot_reuse_broader_read_policy(monkeypatch):
    current = _role(
        tables={"card": {"columns": ["*"], "filters": ["*"]}},
        content_hash="broad",
    )
    manager = _StaticRoleManager(current)
    monkeypatch.setattr(access_control, "RoleManager", lambda **_kwargs: manager)

    parser = _parser("SELECT * FROM card")
    first = access_control.restrict_read_access(
        SUPER, ORG, ROLE, parser.get_table_tuples(), parser.get_physical_tables(),
    )
    assert first == {}

    manager.role_info = _role(
        tables={
            "card": {
                "columns": ["*"],
                "exclude_columns": ["cvv"],
                "filters": ["*"],
            }
        },
        content_hash="narrow",
    )
    second = access_control.restrict_read_access(
        SUPER, ORG, ROLE, parser.get_table_tuples(), parser.get_physical_tables(),
    )
    assert second["card"].excluded_columns == ["cvv"]


# ---------------------------------------------------------------------------
# Metadata is data: names, schema, and column statistics obey the same policy
# ---------------------------------------------------------------------------


def _meta_reader(*, tables: tuple[str, ...] = ("account", "card", "ledger")):
    from supertable.meta_reader import MetaReader
    from supertable import redis_keys as RK

    reader = MetaReader.__new__(MetaReader)
    reader.super_table = SimpleNamespace(super_name=SUPER, organization=ORG)
    reader.catalog = MagicMock()
    keys = [RK.meta_leaf(ORG, SUPER, name) for name in tables]
    reader.catalog.scan_leaf_keys.side_effect = lambda *_args, **_kwargs: iter(keys)
    reader.catalog.get_root.return_value = {"version": 7, "ts": 100}
    return reader


def _leaf(schema: dict, *, rows: int = 1, columns: list[str] | None = None):
    return json.dumps(
        {
            "schema": schema,
            "resources": [
                {
                    "file": "f.parquet",
                    "rows": rows,
                    "file_size": 10,
                    "columns": columns or list(schema),
                    "column_max_value_bytes": {
                        name: 32 for name in (columns or list(schema))
                    },
                }
            ],
        }
    ).encode()


def _set_leaf_payloads(reader, payloads: dict[str, bytes]) -> None:
    """Make the Redis mock behave like ``MGET`` for the requested leaf keys."""
    from supertable import redis_keys as RK

    by_key = {
        RK.meta_leaf(ORG, SUPER, table_name): payload
        for table_name, payload in payloads.items()
    }

    def mget(keys):
        return [by_key.get(key.decode() if isinstance(key, bytes) else key) for key in keys]

    def get(key):
        return by_key.get(key.decode() if isinstance(key, bytes) else key)

    reader.catalog.r.mget.side_effect = mget
    reader.catalog.r.get.side_effect = get


def test_metadata_table_listing_hides_explicitly_denied_table(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role()})
    reader = _meta_reader()
    assert reader.get_tables(ROLE) == ["card", "ledger"]


def test_metadata_schema_hides_excluded_columns(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role()})
    reader = _meta_reader(tables=("card",))
    _set_leaf_payloads(
        reader,
        {
            "card": _leaf(
                {"id": "int", "label": "str", "pan": "str", "cvv": "str"}
            )
        },
    )
    assert reader.get_table_schema("card", ROLE) == [
        {"id": "int", "label": "str"}
    ]


def test_aggregate_metadata_excludes_denied_tables_and_columns(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role()})
    reader = _meta_reader()
    _set_leaf_payloads(
        reader,
        {
            "account": _leaf(
                {"account_only": "str", "name": "str"}, rows=100,
            ),
            "card": _leaf(
                {"id": "int", "label": "str", "cvv": "str"}, rows=10,
            ),
            "ledger": _leaf({"ledger_only": "str"}, rows=5),
        },
    )

    schema = reader.get_table_schema(SUPER, ROLE)
    assert schema == [{"id": "int", "label": "str", "ledger_only": "str"}]

    meta = reader.get_super_meta(ROLE)
    assert [t["name"] for t in meta["super"]["tables"]] == ["card", "ledger"]
    assert meta["super"]["rows"] == 15


def test_metadata_resource_stats_do_not_disclose_excluded_names(monkeypatch):
    _install_roles(monkeypatch, {SUPER: _role()})
    reader = _meta_reader(tables=("card",))
    snapshot = {
        "schema": {"id": "int", "cvv": "str"},
        "resources": [
            {
                "file": "f.parquet",
                "rows": 1,
                "columns": ["id", "cvv"],
                "column_max_value_bytes": {"id": 8, "cvv": 3},
            }
        ],
    }
    with patch("supertable.meta_reader.SimpleTable") as simple:
        simple.return_value.get_simple_table_snapshot.return_value = (snapshot, "p")
        reader.catalog.r.get.return_value = None
        result = reader.get_table_stats("card", ROLE)

    rendered = json.dumps(result).casefold()
    assert "cvv" not in rendered
    assert '"id"' in rendered


def test_show_stats_removes_rows_for_excluded_columns(monkeypatch):
    from supertable.data_reader import DataReader, Status
    from supertable.processing import STATS_SCHEMA
    from supertable.system_query import classify_query

    _install_roles(monkeypatch, {SUPER: _role()})
    rows = []
    for column_name in ("id", "cvv"):
        row = {name: None for name in STATS_SCHEMA}
        row.update(
            {
                "file_path": "f.parquet",
                "footer_sha256": "a" * 64,
                "row_group_id": 0,
                "column_name": column_name,
                "physical_type": "INT64",
                "logical_type": "",
                "row_group_rows": 2,
                "stats_available": True,
                "min_is_exact": True,
                "max_is_exact": True,
            }
        )
        rows.append(row)
    stats = pl.DataFrame(rows, schema=STATS_SCHEMA)

    reader = DataReader.__new__(DataReader)
    reader.organization = ORG
    reader.super_name = SUPER
    reader.storage = MagicMock()
    reader._assert_targets_exist = MagicMock()
    reader._resolve_latest_stats_file = MagicMock(return_value="stats.parquet")
    command = classify_query("SHOW STATS card", SUPER)

    with patch("supertable.processing.load_stats", return_value=stats):
        result, status, message = reader._execute_show_stats(command, ROLE)

    assert status is Status.OK
    assert message is None
    assert result["column_name"].tolist() == ["id"]


def test_super_meta_cache_cannot_leak_across_roles_or_policy_update(monkeypatch):
    import supertable.meta_reader as meta_module

    broad = _role(
        tables={"*": {"columns": ["*"], "filters": ["*"]}},
        content_hash="broad-v1",
    )
    narrow = _role(content_hash="narrow-v1")
    current = {"broad": broad, "narrow": narrow}

    class NamedManager:
        def __init__(self, **_kwargs):
            pass

        def get_role_by_name(self, role_name: str):
            return current.get(role_name, {})

    monkeypatch.setattr(access_control, "RoleManager", NamedManager)
    monkeypatch.setattr(
        access_control, "_check_readonly_guard", lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(meta_module, "_super_meta_cache_ttl_s", lambda: 60.0)
    with meta_module._SUPER_META_CACHE_LOCK:
        meta_module._SUPER_META_CACHE.clear()

    reader = _meta_reader(tables=("account", "card"))
    _set_leaf_payloads(
        reader,
        {
            "account": _leaf({"name": "str"}, rows=100),
            "card": _leaf({"id": "int", "cvv": "str"}, rows=10),
        },
    )

    broad_result = reader.get_super_meta("broad")
    narrow_result = reader.get_super_meta("narrow")
    assert [t["name"] for t in broad_result["super"]["tables"]] == [
        "account", "card",
    ]
    assert [t["name"] for t in narrow_result["super"]["tables"]] == ["card"]

    # Narrow the same role without changing the data/root version.  A cache
    # keyed only by role name + root version would return the broad result.
    current["broad"] = _role(content_hash="broad-v2-now-narrow")
    updated = reader.get_super_meta("broad")
    assert [t["name"] for t in updated["super"]["tables"]] == ["card"]
