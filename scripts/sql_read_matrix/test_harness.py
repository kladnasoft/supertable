from __future__ import annotations

from collections import Counter
from copy import deepcopy
from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys

import pytest

from . import cases_advanced, cases_controls, cases_relational, cases_scalar, cases_syntax
from .compare import compare_result
from .dataset import arrow_schemas, build_dataset
from .models import QueryCase


def _case(expected, *, columns=("value",), ordered=False, expected_types=None):
    return QueryCase(
        case_id="harness_example",
        category="harness",
        sql="SELECT value FROM example",
        columns=columns,
        expected=expected,
        ordered=ordered,
        expected_types=expected_types or {},
    )


def test_unordered_comparison_preserves_duplicates_and_ignores_position():
    case = _case([(2,), (None,), (2,), (1,)])
    assert compare_result(case, ["value"], [(2,), (1,), (2,), (None,)]) is None


def test_unordered_comparison_rejects_wrong_duplicate_multiplicity():
    case = _case([(1,), (1,), (2,)])
    mismatch = compare_result(case, ["value"], [(1,), (2,), (2,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"
    assert mismatch["missing_row"] == (1,)


def test_comparison_rejects_dropped_duplicate_row():
    case = _case([(1,), (1,), (2,)])
    mismatch = compare_result(case, ["value"], [(1,), (2,)])
    assert mismatch is not None
    assert mismatch["kind"] == "row_count"


def test_ordered_comparison_requires_the_requested_row_order():
    case = _case([(1,), (2,), (None,)], ordered=True)
    assert compare_result(case, ["value"], [(1,), (2,), (None,)]) is None
    mismatch = compare_result(case, ["value"], [(2,), (1,), (None,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"
    assert mismatch["row_index"] == 0


@pytest.mark.parametrize("columns", [("right", "left"), ("left", "wrong"), ("left",)])
def test_comparison_requires_exact_column_names_and_order(columns):
    case = _case([], columns=("left", "right"))
    mismatch = compare_result(case, columns, [])
    assert mismatch is not None
    assert mismatch["kind"] == "columns"


def test_none_matches_none_in_a_nested_result_row():
    case = _case([(None, [1, None])], columns=("missing", "values"), ordered=True)
    assert compare_result(case, ["missing", "values"], [[None, [1, None]]]) is None


@pytest.mark.parametrize("replacement", [0, False, "", float("nan")])
def test_none_is_distinct_from_non_null_values(replacement):
    case = _case([(None,)])
    mismatch = compare_result(case, ["value"], [(replacement,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"


@pytest.mark.parametrize("expected,actual", [(True, 1), (False, 0), (1, True), (0, False)])
def test_boolean_values_are_not_interchangeable_with_integers(expected, actual):
    case = _case([(expected,)])
    mismatch = compare_result(case, ["value"], [(actual,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"
    assert compare_result(case, ["value"], [(expected,)]) is None


def test_decimal_comparison_retains_precision_beyond_float_resolution():
    exact = Decimal("9007199254740992.01")
    rounded_differently = Decimal("9007199254740992.02")
    case = _case([(exact,)])
    assert compare_result(case, ["value"], [(Decimal("9007199254740992.010"),)]) is None
    mismatch = compare_result(case, ["value"], [(rounded_differently,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"


@pytest.mark.parametrize("expected,actual", [
    (Decimal("9007199254740992.01"), 9007199254740992.0),
    (9007199254740993, 9007199254740992.0),
    (Decimal("0.1"), 0.1),
])
def test_exact_expectations_cannot_be_satisfied_by_rounded_floats(expected, actual):
    assert compare_result(_case([(expected,)]), ["value"], [(actual,)]) is not None


@pytest.mark.parametrize("expected,actual", [(0.0, 5e-10), (2.0, 2.0 + 1e-10), (1e8, 1e8 + 0.01)])
def test_floating_comparison_accepts_small_absolute_or_relative_error(expected, actual):
    case = _case([(expected,)])
    assert compare_result(case, ["value"], [(actual,)]) is None


@pytest.mark.parametrize("expected,actual", [(0.0, 1e-6), (2.0, 2.001), (1e8, 1e8 + 10)])
def test_floating_comparison_rejects_meaningful_numeric_error(expected, actual):
    case = _case([(expected,)])
    mismatch = compare_result(case, ["value"], [(actual,)])
    assert mismatch is not None
    assert mismatch["kind"] == "values"


def test_declared_dtype_matches_without_requiring_undeclared_types():
    case = _case([(1, "a")], columns=("value", "label"), expected_types={"value": "Int64"})
    assert compare_result(case, case.columns, [(1, "a")], {"value": "Int64", "label": "String"}) is None


@pytest.mark.parametrize("actual_types", [{"value": "Float64"}, {}, {"other": "Int64"}])
def test_declared_dtype_mismatch_is_reported_even_when_values_match(actual_types):
    case = _case([(1,)], expected_types={"value": "Int64"})
    mismatch = compare_result(case, ["value"], [(1,)], actual_types)
    assert mismatch is not None
    assert mismatch["kind"] == "types"
    assert "value" in mismatch["mismatch"]


def test_dataset_is_deterministic_and_each_build_is_independent():
    first = build_dataset()
    second = build_dataset()
    assert first == second
    first["orders"][0]["oid"] = -1
    first["customers"].clear()
    assert second["orders"][0]["oid"] == 1
    assert len(second["customers"]) == 16


def test_ledger_oracle_represents_replacements_deletions_and_appends():
    ledger = build_dataset()["ledger"]
    by_id = {row["lid"]: row for row in ledger}
    assert len(ledger) == len(by_id) == 24
    assert set(by_id) == set(range(1, 27)) - {5, 11}
    assert by_id[3] == {"lid": 3, "value": 1030, "revision": 2}
    assert by_id[9] == {"lid": 9, "value": 1090, "revision": 2}
    assert by_id[18] == {"lid": 18, "value": 1180, "revision": 2}
    assert by_id[1] == {"lid": 1, "value": 10, "revision": 1}
    assert by_id[25] == {"lid": 25, "value": 250, "revision": 1}
    assert by_id[26] == {"lid": 26, "value": 260, "revision": 1}


def test_dataset_tables_have_declared_row_keys_and_unique_primary_keys():
    dataset = build_dataset()
    schemas = arrow_schemas()
    primary_keys = {
        "orders": "oid", "customers": "cid", "items": "iid", "numbers": "rid",
        "events": "eid", "ledger": "lid", "evolving": "eid", "nulls": "nid",
        "empty_table": "eid",
    }
    assert dataset.keys() == schemas.keys() == primary_keys.keys()
    for table, rows in dataset.items():
        values = [row[primary_keys[table]] for row in rows]
        assert None not in values, table
        assert len(values) == len(set(values)), table
        assert all(set(row) == set(schemas[table].names) for row in rows), table
    assert dataset["empty_table"] == []
    assert schemas["empty_table"].names == ["eid"]


def test_dataset_contains_null_and_unmatched_foreign_keys_for_outer_joins():
    dataset = build_dataset()
    customer_ids = {row["cid"] for row in dataset["customers"]}
    order_customer_ids = {row["cid"] for row in dataset["orders"]}
    order_ids = {row["oid"] for row in dataset["orders"]}
    assert None in order_customer_ids
    assert 99 in order_customer_ids and 99 not in customer_ids
    assert customer_ids - order_customer_ids == {13, 14, 15, 16}
    assert any(row["oid"] not in order_ids for row in dataset["items"])


def test_evolving_oracle_fills_missing_old_columns_with_none():
    rows = build_dataset()["evolving"]
    assert [(row["eid"], row["extra"]) for row in rows] == [
        (1, None), (2, None), (3, None),
        (4, "extra_4"), (5, "extra_5"), (6, "extra_6"),
    ]
    schema = arrow_schemas()["evolving"]
    assert schema.names == ["eid", "value", "extra"]
    assert str(schema.field("extra").type) == "string"


@pytest.fixture(scope="module")
def generated_cases():
    dataset = build_dataset()
    original = deepcopy(dataset)
    result = []
    for module in (cases_scalar, cases_relational, cases_advanced, cases_controls, cases_syntax):
        cases = module.build_cases(dataset)
        assert cases, module.__name__
        result.extend(cases)
    assert dataset == original
    return result


def test_all_generated_case_ids_are_unique(generated_cases):
    counts = Counter(case.case_id for case in generated_cases)
    duplicates = {case_id: count for case_id, count in counts.items() if count > 1}
    assert not duplicates
    assert all(case.case_id and case.sql and case.category for case in generated_cases)


def test_generated_expectations_have_the_declared_result_width(generated_cases):
    for case in generated_cases:
        assert all(len(row) == len(case.columns) for row in case.expected), case.case_id
        assert set(case.expected_types) <= set(case.columns), case.case_id


def test_case_definitions_are_reproducible_from_a_fresh_dataset(generated_cases):
    rebuilt = []
    dataset = build_dataset()
    for module in (cases_scalar, cases_relational, cases_advanced, cases_controls, cases_syntax):
        rebuilt.extend(module.build_cases(dataset))
    assert rebuilt == generated_cases


def test_expected_artifacts_are_reproducible_across_python_hash_seeds():
    probe = """
import hashlib, importlib, json
from scripts.sql_read_matrix.dataset import build_dataset
from scripts.sql_read_matrix.compare import json_default
cases = []
for name in ('scalar', 'relational', 'advanced', 'controls', 'syntax'):
    cases.extend(importlib.import_module('scripts.sql_read_matrix.cases_' + name).build_cases(build_dataset()))
print(hashlib.sha256(json.dumps([c.__dict__ for c in cases], default=json_default, sort_keys=True).encode()).hexdigest())
"""
    digests = [subprocess.check_output(
        [sys.executable, "-c", probe], cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "PYTHONHASHSEED": seed}, text=True, timeout=60,
    ).strip() for seed in ("1", "991")]
    assert digests[0] == digests[1]
