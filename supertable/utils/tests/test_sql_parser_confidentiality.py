import traceback

import pytest
from sqlglot.errors import ParseError

from supertable.utils import sql_parser as parser_module
from supertable.utils.sql_parser import SQLParser


def _render(error: BaseException) -> str:
    return "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


def test_sql_parse_error_does_not_reflect_query_or_parser_context(monkeypatch):
    secret = "signed-url-token-DO-NOT-LOG"
    failure = ParseError(
        f"backend parser detail {secret}",
        errors=[{
            "description": f"unexpected literal {secret}",
            "line": 2,
            "col": 17,
            "start_context": f"SELECT '{secret}",
            "highlight": "'",
            "end_context": " FROM events",
        }],
    )
    monkeypatch.setattr(parser_module.sqlglot, "parse", lambda *_a, **_k: (_ for _ in ()).throw(failure))

    with pytest.raises(ValueError) as caught:
        SQLParser._parse_query(f"SELECT '{secret}'", "duckdb")

    assert str(caught.value) == (
        "Failed to parse SQL query: Invalid SQL syntax. Line 2, Col: 17."
    )
    assert secret not in _render(caught.value)


def test_unexpected_parser_failure_exposes_only_exception_type(monkeypatch):
    secret = "redis-password-DO-NOT-LOG"
    monkeypatch.setattr(
        parser_module.sqlglot,
        "parse",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError(secret)),
    )

    with pytest.raises(ValueError) as caught:
        SQLParser._parse_query("SELECT 1", "duckdb")

    assert str(caught.value) == (
        "An unexpected error occurred while parsing SQL query; "
        "error_type=RuntimeError"
    )
    assert secret not in _render(caught.value)
