"""Security boundaries for the medcenter accounting export month."""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from supertable.demo.medcenter import export_accounting as exporter
from supertable.demo.medcenter import run as demo_run
from supertable.demo.medcenter.validation import require_canonical_month


_SQL_AND_PATH_PAYLOAD = "2026-07' OR 1=1 --/../../../escaped"
_INVALID_MONTHS = (
    _SQL_AND_PATH_PAYLOAD,
    "../2026-07",
    "2026-00",
    "2026-13",
    "2026-7",
    "02026-07",
    "2026-07\n",
    "２０２６-０７",
    202607,
    None,
    True,
)


@pytest.mark.parametrize("value", _INVALID_MONTHS)
def test_month_validation_is_strict_and_does_not_reflect_input(value) -> None:
    with pytest.raises(ValueError) as raised:
        require_canonical_month(value)

    message = str(raised.value)
    assert message == (
        "month must use canonical YYYY-MM with a month from 01 through 12"
    )
    assert str(value) not in message


@pytest.mark.parametrize("value", ("0000-01", "2026-01", "2026-12", "9999-12"))
def test_month_validation_accepts_canonical_months(value: str) -> None:
    assert require_canonical_month(value) == value


def test_export_rejects_payload_before_query_or_filesystem(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "accounting"
    query_calls: list[str] = []

    def record_query(query: str) -> pd.DataFrame:
        query_calls.append(query)
        return pd.DataFrame()

    monkeypatch.setattr(exporter, "run_query", record_query)

    with pytest.raises(ValueError) as raised:
        exporter.export_accounting_import(
            month=_SQL_AND_PATH_PAYLOAD,
            output_dir=str(output_dir),
        )

    assert str(raised.value) == (
        "month must use canonical YYYY-MM with a month from 01 through 12"
    )
    assert query_calls == []
    assert not output_dir.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "builder",
    (
        exporter._chargebee_frame,
        exporter._stripe_weight_frame,
        exporter._eigenprodukte_frame,
    ),
)
def test_private_sql_builders_cannot_bypass_month_validation(
    builder, monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_calls: list[str] = []

    def record_query(query: str) -> pd.DataFrame:
        query_calls.append(query)
        return pd.DataFrame()

    monkeypatch.setattr(exporter, "run_query", record_query)

    with pytest.raises(ValueError):
        builder(_SQL_AND_PATH_PAYLOAD)

    assert query_calls == []


def test_export_keeps_canonical_month_in_one_filename_component(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_export = pd.DataFrame(columns=exporter.EXPORT_COLUMNS)
    monkeypatch.setattr(
        exporter,
        "EXPORT_BUILDERS",
        {"chargebee": lambda month: empty_export},
    )

    written = exporter.export_accounting_import(
        month="2026-07", output_dir=str(tmp_path)
    )

    expected = tmp_path / "accounting_import_chargebee_2026-07.csv"
    assert written == [str(expected)]
    assert [path.name for path in tmp_path.iterdir()] == [expected.name]


def test_export_cli_rejects_payload_without_reflection_or_export_side_effect(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture,
) -> None:
    output_dir = tmp_path / "accounting"
    query_calls: list[str] = []

    monkeypatch.setattr(exporter, "initialize_app_home", lambda **_kwargs: "")
    monkeypatch.setattr(
        exporter,
        "run_query",
        lambda query: query_calls.append(query),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "supertable-demo-medcenter-export",
            "--month",
            _SQL_AND_PATH_PAYLOAD,
            "--output-dir",
            str(output_dir),
        ],
    )

    with pytest.raises(SystemExit) as raised:
        exporter.main()

    assert raised.value.code == 2
    assert _SQL_AND_PATH_PAYLOAD not in capsys.readouterr().err
    assert query_calls == []
    assert not output_dir.exists()


@pytest.mark.parametrize("scope_args", (("--month", _SQL_AND_PATH_PAYLOAD), ("--year", "10000")))
def test_run_cli_rejects_invalid_export_scope_before_pipeline_side_effects(
    scope_args: tuple[str, str],
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "accounting"
    pipeline_calls: list[str] = []

    monkeypatch.setattr(demo_run, "initialize_app_home", lambda **_kwargs: "")
    monkeypatch.setattr(
        demo_run,
        "generate_months",
        lambda *_args, **_kwargs: pipeline_calls.append("generate"),
    )
    monkeypatch.setattr(
        demo_run,
        "load",
        lambda *_args, **_kwargs: pipeline_calls.append("load"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "supertable-demo-medcenter-run",
            *scope_args,
            "--data-dir",
            str(data_dir),
            "--export-dir",
            str(output_dir),
        ],
    )

    with pytest.raises(SystemExit) as raised:
        demo_run.main()

    assert raised.value.code == 2
    assert scope_args[1] not in capsys.readouterr().err
    assert pipeline_calls == []
    assert not data_dir.exists()
    assert not output_dir.exists()
