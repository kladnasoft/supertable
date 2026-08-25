from supertable.audit.export import export_events


def test_csv_export_neutralizes_spreadsheet_formulas() -> None:
    payload = export_events(
        [{"action": "=HYPERLINK(\"http://attacker\")", "count": 1}],
        "csv",
    ).decode("utf-8")
    assert "'=HYPERLINK" in payload
