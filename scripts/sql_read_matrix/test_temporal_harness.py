from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from . import cases_temporal, cases_temporal_coercion, cases_timezones
from .compare import compare_result, value_equal
from .dataset import arrow_schemas, build_dataset
from .models import QueryCase, load_saved_cases
from .source_fingerprint import capture


UTC = timezone.utc


@pytest.fixture(scope="module")
def temporal_dataset():
    return build_dataset(True)


@pytest.fixture(scope="module")
def temporal_cases(temporal_dataset):
    before = deepcopy(temporal_dataset)
    cases = (cases_temporal.build_cases(temporal_dataset) + cases_timezones.build_cases(temporal_dataset)
             + cases_temporal_coercion.build_cases(temporal_dataset))
    assert temporal_dataset == before
    return cases


def test_expanded_dataset_retains_original_fixtures_and_adds_declared_tables(temporal_dataset):
    original = build_dataset()
    assert all(temporal_dataset[name] == rows for name, rows in original.items())
    schemas = arrow_schemas(True)
    assert temporal_dataset.keys() == schemas.keys()
    for name, key, count in (("temporal_dates", "eid", 99), ("temporal_naive", "eid", 162),
                             ("tz_instants", "tid", 1231), ("tz_budapest_storage", "tid", 595)):
        rows = temporal_dataset[name]
        assert len(rows) == count
        assert len({row[key] for row in rows}) == count
        assert all(set(row) == set(schemas[name].names) for row in rows)
        arrow = pa.Table.from_pylist(rows, schema=schemas[name])
        assert arrow.schema == schemas[name]
        for expected, actual in zip(rows, arrow.to_pylist()):
            assert all(value_equal(expected[field], actual[field]) for field in expected)


def test_temporal_arrow_types_retain_date_microseconds_and_timezone():
    schemas = arrow_schemas(True)
    assert schemas["temporal_dates"].field("event_date").type == pa.date32()
    assert schemas["temporal_naive"].field("event_time").type == pa.timestamp("us")
    assert schemas["tz_instants"].field("instant").type == pa.timestamp("us", tz="UTC")
    assert schemas["tz_budapest_storage"].field("instant").type == pa.timestamp("us", tz="Europe/Budapest")


def test_date_fixture_has_century_leap_cases_duplicates_and_nulls(temporal_dataset):
    values = [row["event_date"] for row in temporal_dataset["temporal_dates"]]
    required = {date(1900, 2, 28), date(1900, 3, 1), date(1969, 12, 31), date(1970, 1, 1),
                date(2000, 2, 29), date(2023, 2, 28), date(2023, 3, 1), date(2024, 2, 29),
                date(2038, 1, 19), date(2100, 2, 28), date(2100, 3, 1)}
    assert required <= set(values)
    assert Counter(values)[date(2024, 2, 29)] == 3
    assert values.count(None) == 4
    assert values[:-4] == sorted(values[:-4])


def test_naive_timestamp_fixture_retains_epoch_and_midnight_microsecond_neighbors(temporal_dataset):
    values = [row["event_time"] for row in temporal_dataset["temporal_naive"]]
    required = {
        datetime(1969, 12, 31, 23, 59, 59, 999999), datetime(1970, 1, 1),
        datetime(1970, 1, 1, 0, 0, 0, 1), datetime(2024, 2, 28, 23, 59, 59, 999999),
        datetime(2024, 2, 29), datetime(2024, 2, 29, 0, 0, 0, 1),
        datetime(2024, 2, 29, 12, 34, 56, 123456), datetime(2024, 2, 29, 12, 34, 56, 123457),
        datetime(2024, 2, 29, 23, 59, 59, 999999), datetime(2024, 3, 1),
    }
    assert required <= set(values)
    assert Counter(values)[datetime(2024, 2, 29, 12, 34, 56, 123456)] == 2
    assert values.count(None) == 5
    assert all(value.tzinfo is None for value in values if value is not None)


@pytest.mark.parametrize("before,after,zone,before_wall,after_wall", [
    ("2024-03-10T06:59:59.999999+00:00", "2024-03-10T07:00:00+00:00", "America/New_York",
     "2024-03-10T01:59:59.999999", "2024-03-10T03:00:00"),
    ("2024-03-31T00:59:59.999999+00:00", "2024-03-31T01:00:00+00:00", "Europe/Budapest",
     "2024-03-31T01:59:59.999999", "2024-03-31T03:00:00"),
    ("2024-10-05T15:29:59.999999+00:00", "2024-10-05T15:30:00+00:00", "Australia/Lord_Howe",
     "2024-10-06T01:59:59.999999", "2024-10-06T02:30:00"),
    ("2011-12-30T09:59:59.999999+00:00", "2011-12-30T10:00:00+00:00", "Pacific/Apia",
     "2011-12-29T23:59:59.999999", "2011-12-31T00:00:00"),
])
def test_timezone_fixture_straddles_spring_gaps_and_skipped_day(
        temporal_dataset, before, after, zone, before_wall, after_wall):
    instants = {row["instant"] for row in temporal_dataset["tz_instants"]}
    earlier, later = datetime.fromisoformat(before), datetime.fromisoformat(after)
    assert {earlier, later} <= instants
    assert later - earlier == timedelta(microseconds=1)
    assert earlier.astimezone(ZoneInfo(zone)).replace(tzinfo=None) == datetime.fromisoformat(before_wall)
    assert later.astimezone(ZoneInfo(zone)).replace(tzinfo=None) == datetime.fromisoformat(after_wall)


@pytest.mark.parametrize("early,late,zone,wall,elapsed_minutes", [
    ("2024-11-03T05:30:00+00:00", "2024-11-03T06:30:00+00:00", "America/New_York", "2024-11-03T01:30:00", 60),
    ("2024-10-27T00:30:00+00:00", "2024-10-27T01:30:00+00:00", "Europe/Budapest", "2024-10-27T02:30:00", 60),
    ("2024-04-06T14:30:00+00:00", "2024-04-06T15:00:00+00:00", "Australia/Lord_Howe", "2024-04-07T01:30:00", 30),
])
def test_timezone_fixture_retains_both_instants_of_repeated_wall_time(
        temporal_dataset, early, late, zone, wall, elapsed_minutes):
    instants = {row["instant"] for row in temporal_dataset["tz_instants"]}
    earlier, later = datetime.fromisoformat(early), datetime.fromisoformat(late)
    assert {earlier, later} <= instants
    assert later - earlier == timedelta(minutes=elapsed_minutes)
    first, second = (instant.astimezone(ZoneInfo(zone)) for instant in (earlier, later))
    assert first.replace(tzinfo=None) == second.replace(tzinfo=None) == datetime.fromisoformat(wall)
    assert first.fold == 0 and second.fold == 1
    assert not value_equal(first, second)


def test_budapest_storage_retains_both_folds_and_same_instants_as_utc_source(temporal_dataset):
    source = {row["tid"]: row["instant"] for row in temporal_dataset["tz_instants"]}
    storage = temporal_dataset["tz_budapest_storage"]
    assert {row["instant"].fold for row in storage} == {0, 1}
    assert {row["instant"].utcoffset() for row in storage} == {timedelta(hours=1), timedelta(hours=2)}
    assert all(row["instant"].tzinfo.key == "Europe/Budapest" for row in storage)
    assert all(value_equal(row["instant"], source[row["tid"]]) for row in storage)


@pytest.mark.parametrize("offset", [-600, 0, 120, 345, 630, 840])
def test_timestamp_comparator_matches_equal_instants_across_offsets(offset):
    instant = datetime(2024, 10, 27, 1, 0, 0, 1, tzinfo=UTC)
    displayed = instant.astimezone(timezone(timedelta(minutes=offset)))
    assert value_equal(instant, displayed)
    assert not value_equal(instant, displayed + timedelta(microseconds=1))


def test_timestamp_comparator_rejects_same_wall_clock_at_different_instants():
    utc = datetime(2024, 1, 1, 12, tzinfo=UTC)
    kathmandu = datetime(2024, 1, 1, 12, tzinfo=ZoneInfo("Asia/Kathmandu"))
    assert not value_equal(utc, kathmandu)


@pytest.mark.parametrize("replacement", [None, date(2024, 2, 29), "2024-02-29T00:00:00", 0])
def test_temporal_comparator_preserves_null_and_value_type_distinctions(replacement):
    stamp = datetime(2024, 2, 29)
    assert not value_equal(stamp, replacement)
    assert not value_equal(replacement, stamp)
    assert value_equal(None, None)


def test_result_comparison_cannot_drop_one_fold_and_repeat_the_other():
    zone = ZoneInfo("Europe/Budapest")
    first = datetime(2024, 10, 27, 2, 30, tzinfo=zone, fold=0)
    second = datetime(2024, 10, 27, 2, 30, tzinfo=zone, fold=1)
    case = QueryCase("fold", "harness", "SELECT instant FROM example", ("instant",), [(first,), (second,)])
    assert compare_result(case, case.columns, [(second,), (first,)]) is None
    mismatch = compare_result(case, case.columns, [(first,), (first,)])
    assert mismatch is not None and mismatch["kind"] == "values"


def test_all_new_case_ids_and_result_widths_are_valid(temporal_cases):
    assert len(temporal_cases) == 784
    assert len({case.case_id for case in temporal_cases}) == 784
    assert all(case.sql and case.category and case.session_timezone for case in temporal_cases)
    for case in temporal_cases:
        assert all(len(row) == len(case.columns) for row in case.expected), case.case_id
        ZoneInfo(case.session_timezone)


def test_temporal_oracles_have_nonempty_and_empty_results_and_all_session_zones(temporal_cases):
    assert any(not case.expected for case in temporal_cases)
    assert any(len(case.expected) > 1000 for case in temporal_cases)
    assert {case.session_timezone for case in temporal_cases} == {
        "UTC", "Europe/Budapest", "America/New_York", "Asia/Kathmandu",
        "Australia/Lord_Howe", "Pacific/Apia", "Pacific/Kiritimati",
    }


def test_new_fixture_and_expected_artifacts_are_reproducible_across_hash_seeds():
    probe = """
import hashlib, json
from scripts.sql_read_matrix.dataset import build_dataset
from scripts.sql_read_matrix import cases_temporal, cases_temporal_coercion, cases_timezones
from scripts.sql_read_matrix.compare import json_default
data = build_dataset(True)
cases = cases_temporal.build_cases(data) + cases_timezones.build_cases(data) + cases_temporal_coercion.build_cases(data)
payload = {'data': data, 'cases': [case.__dict__ for case in cases]}
print(hashlib.sha256(json.dumps(payload, default=json_default, sort_keys=True).encode()).hexdigest())
"""
    hashes = [subprocess.check_output(
        [sys.executable, "-c", probe], cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "PYTHONHASHSEED": seed}, text=True, timeout=60,
    ).strip() for seed in ("1", "991")]
    assert len(hashes[0]) == 64
    assert hashes[0] == hashes[1]


def test_saved_casebook_preserves_exact_temporal_decimal_and_null_values(tmp_path):
    path = tmp_path / "expectations.json"
    path.write_text(json.dumps([{
        "case_id": "saved_temporal", "category": "saved_oracle",
        "sql": "SELECT day, instant, wall, amount FROM fixture ORDER BY instant",
        "columns": ["day", "instant", "wall", "amount"], "ordered": True,
        "session_timezone": "Europe/Budapest", "error_contains": [],
        "expected": [
            [{"type": "date", "value": "2024-10-27"},
             {"type": "datetime", "value": "2024-10-27T02:30:00.000001+02:00"},
             {"type": "datetime", "value": "1969-12-31T23:59:59.999999"},
             {"type": "decimal", "value": "9007199254740992.000001"}],
            [{"type": "date", "value": "2024-10-27"},
             {"type": "datetime", "value": "2024-10-27T02:30:00.000001+01:00"},
             None, {"type": "decimal", "value": "9007199254740992.000002"}],
        ],
    }]))
    case, = load_saved_cases(path)
    assert case.columns == ("day", "instant", "wall", "amount")
    assert case.session_timezone == "Europe/Budapest"
    expected = [
        (date(2024, 10, 27), datetime(2024, 10, 27, 0, 30, 0, 1, tzinfo=UTC),
         datetime(1969, 12, 31, 23, 59, 59, 999999), Decimal("9007199254740992.000001")),
        (date(2024, 10, 27), datetime(2024, 10, 27, 1, 30, 0, 1, tzinfo=UTC),
         None, Decimal("9007199254740992.000002")),
    ]
    assert compare_result(case, case.columns, expected) is None
    assert type(case.expected[0][0]) is date
    assert case.expected[0][1].utcoffset() == timedelta(hours=2)
    assert case.expected[1][1].utcoffset() == timedelta(hours=1)
    assert case.expected[0][2].tzinfo is None
    assert isinstance(case.expected[0][3], Decimal)
    assert not value_equal(case.expected[0][1], case.expected[1][1])
    shifted = [tuple(row) for row in expected]
    shifted[0] = (shifted[0][0], shifted[0][1] + timedelta(microseconds=1), *shifted[0][2:])
    assert compare_result(case, case.columns, shifted) is not None


def test_saved_casebook_preserves_original_success_and_rejection_contracts(tmp_path):
    path = tmp_path / "expectations.json"
    path.write_text(json.dumps([
        {"case_id": "saved_positive_literal", "category": "saved_oracle",
         "sql": "SELECT 2 AS answer", "columns": ["answer"], "expected": [[2]],
         "ordered": True, "error_contains": []},
        {"case_id": "saved_expected_rejection", "category": "saved_oracle",
         "sql": "DELETE FROM fixture", "columns": [], "expected": [],
         "error_contains": ["Only read-only queries are allowed"], "role": "eu_reader"},
    ]))
    positive, negative = load_saved_cases(path)
    assert not positive.error_contains
    assert positive.sql == "SELECT 2 AS answer"
    assert positive.expected == [(2,)]
    assert positive.session_timezone == "UTC"
    assert compare_result(positive, ("answer",), [(2,)]) is None
    assert compare_result(positive, ("answer",), []) is not None
    assert list(negative.error_contains) == ["Only read-only queries are allowed"]
    assert negative.expected == []
    assert negative.role == "eu_reader"


def test_source_fingerprint_ignores_nonexecutable_text_but_detects_changed_behavior(tmp_path):
    source = tmp_path / "supertable"
    source.mkdir()
    module = source / "example.py"
    module.write_text('"""First module description."""\ndef calculate(value):\n    """First function description."""\n    return value + 1\n')
    original = capture(tmp_path)
    module.write_text('# Changed explanation.\n"""Replacement module description."""\n\ndef calculate(value):\n    """Replacement function description."""\n    return value+1\n')
    assert capture(tmp_path) == original
    module.write_text('def calculate(value):\n    return value + 2\n')
    changed = capture(tmp_path)
    assert changed["files"].keys() == original["files"].keys()
    assert changed["combined_sha256"] != original["combined_sha256"]


def test_source_fingerprint_excludes_tests_and_benchmarks(tmp_path):
    source = tmp_path / "supertable"
    source.mkdir()
    (source / "example.py").write_text("VALUE = 1\n")
    original = capture(tmp_path)
    for name in ("tests", "benchmarks"):
        (source / name).mkdir()
        (source / name / "example.py").write_text("VALUE = 999\n")
    assert capture(tmp_path) == original
