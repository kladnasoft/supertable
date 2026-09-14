from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path


class _ExecutableOnly(ast.NodeTransformer):
    def _visit_body(self, node):
        self.generic_visit(node)
        while node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant) and isinstance(node.body[0].value.value, str):
            node.body.pop(0)
        if not node.body:
            node.body.append(ast.Pass())
        return node

    visit_Module = _visit_body
    visit_FunctionDef = _visit_body
    visit_AsyncFunctionDef = _visit_body
    visit_ClassDef = _visit_body


def capture(repo):
    repo = Path(repo)
    files = {}
    for path in sorted((repo / "supertable").rglob("*.py")):
        relative = path.relative_to(repo)
        if "tests" in relative.parts or "benchmarks" in relative.parts:
            continue
        tree = _ExecutableOnly().visit(ast.parse(path.read_text()))
        normalized = ast.parse(ast.unparse(ast.fix_missing_locations(tree)))
        executable = ast.dump(normalized, include_attributes=False).encode()
        files[str(relative)] = hashlib.sha256(executable).hexdigest()
    return {"algorithm": "sha256 of Python AST excluding comments, docstrings, and location attributes",
            "files": files, "combined_sha256": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()}
