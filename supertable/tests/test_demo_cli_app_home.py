"""Demo CLIs opt into legacy app-home CWD semantics at their boundary."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEMO_CLI_MODULES = (
    "demo/webshop/core.py",
    "demo/webshop/generate.py",
    "demo/webshop/load.py",
    "demo/webshop/stream_demo.py",
    "demo/webshop/topup.py",
    "demo/medcenter/generate.py",
    "demo/medcenter/load.py",
    "demo/medcenter/run.py",
    "demo/medcenter/transform.py",
    "demo/medcenter/quality.py",
    "demo/medcenter/export_accounting.py",
)


def _first_main_action(path: Path) -> ast.stmt:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    main = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "main"
    )
    body = list(main.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body.pop(0)
    while body and isinstance(body[0], ast.Global):
        body.pop(0)
    return body[0]


@pytest.mark.parametrize("relative_path", DEMO_CLI_MODULES)
def test_demo_cli_explicitly_initializes_app_home_first(
    relative_path: str,
) -> None:
    first = _first_main_action(PACKAGE_ROOT / relative_path)
    assert isinstance(first, ast.Expr)
    assert isinstance(first.value, ast.Call)
    assert isinstance(first.value.func, ast.Name)
    assert first.value.func.id == "initialize_app_home"
    assert any(
        keyword.arg == "change_cwd"
        and isinstance(keyword.value, ast.Constant)
        and keyword.value.value is True
        for keyword in first.value.keywords
    )


@pytest.mark.parametrize(
    "relative_path",
    DEMO_CLI_MODULES + ("demo/medcenter/helpers.py",),
)
def test_demo_modules_have_no_homedir_side_effect_import(
    relative_path: str,
) -> None:
    path = PACKAGE_ROOT / relative_path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(
        isinstance(node, ast.Import)
        and any(alias.name == "supertable.config.homedir" for alias in node.names)
        for node in tree.body
    )
