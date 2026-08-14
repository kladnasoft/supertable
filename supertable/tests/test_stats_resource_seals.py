"""Correctness boundary for snapshot-pinned per-resource statistics seals."""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from supertable.data_classes import ResourceStatsSeal, RowGroupSelection
from supertable import processing
from supertable.processing import (
    STATS_SCHEMA,
    compact_resources,
    extract_stats_rows,
    integer_domains_from_complete_stats,
    resource_stats_seal,
    stats_for_complete_files,
    stats_resource_seals,
    write_parquet_and_collect_resources,
)


def _stat_row(file_path: str, value: int, footer: str) -> dict:
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "footer_sha256": footer,
        "row_group_id": 0,
        "column_name": "id",
        "physical_type": "INT64",
        "logical_type": "",
        "min_bigint": value,
        "max_bigint": value,
        "null_count": 0,
        "row_group_rows": 1,
        "compressed_bytes": 8,
        "uncompressed_bytes": 8,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    return row


def _frame(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=STATS_SCHEMA)


def test_complete_integer_domain_bounds_range_and_null_group():
    footer = "a" * 64
    first = _stat_row("one.parquet", 0, footer)
    first.update({"max_bigint": 1023, "row_group_rows": 20})
    second = _stat_row("two.parquet", 100, footer)
    second.update({
        "max_bigint": 900,
        "null_count": 1,
        "row_group_rows": 20,
    })

    bounds = integer_domains_from_complete_stats(
        _frame(first, second),
        ["one.parquet", "two.parquet"],
    )

    assert bounds["id"].minimum == 0
    assert bounds["id"].maximum == 1023
    assert bounds["id"].has_null is True
    assert bounds["id"].cardinality_upper_bound == 1025


def test_integer_domain_bound_uses_only_selected_groups():
    footer = "b" * 64
    low = _stat_row("one.parquet", 10, footer)
    high = _stat_row("one.parquet", 1_000_000, footer)
    high["row_group_id"] = 1
    selection = RowGroupSelection(2, (0,), footer)

    bounds = integer_domains_from_complete_stats(
        _frame(low, high),
        ["one.parquet"],
        {"one.parquet": selection},
    )

    assert bounds["id"].cardinality_upper_bound == 1


def test_integer_domain_bound_accounts_for_all_null_group():
    row = _stat_row("one.parquet", 0, "d" * 64)
    row.update({
        "min_bigint": None,
        "max_bigint": None,
        "null_count": 7,
        "row_group_rows": 7,
        "stats_available": False,
    })

    bound = integer_domains_from_complete_stats(
        _frame(row), ["one.parquet"],
    )["id"]

    assert bound.minimum is None
    assert bound.maximum is None
    assert bound.has_null is True
    assert bound.cardinality_upper_bound == 1


@pytest.mark.parametrize(
    "mutation",
    [
        {"min_is_exact": False},
        {"max_double": 1.0},
        {"physical_type": "BYTE_ARRAY"},
        {"stats_available": False},
    ],
)
def test_integer_domain_bound_rejects_ambiguous_or_inexact_lane(mutation):
    row = _stat_row("one.parquet", 1, "e" * 64)
    row.update(mutation)

    assert integer_domains_from_complete_stats(
        _frame(row), ["one.parquet"],
    ) == {}


def test_integer_domain_bound_fails_closed_on_incomplete_or_malformed_slot():
    footer = "c" * 64
    valid = _stat_row("one.parquet", 1, footer)
    missing_file = integer_domains_from_complete_stats(
        _frame(valid), ["one.parquet", "missing.parquet"],
    )
    malformed = dict(valid)
    malformed["null_count"] = 2
    malformed["row_group_rows"] = 1

    assert missing_file == {}
    assert integer_domains_from_complete_stats(
        _frame(malformed), ["one.parquet"],
    ) == {}


def test_primary_write_resource_seals_match_exact_extracted_rows():
    storage = MagicMock()
    uploaded = {}
    storage.write_bytes.side_effect = lambda path, data: uploaded.update({path: data})
    resources = []
    footer_cache = {}

    with (
        patch("supertable.processing._get_storage", return_value=storage),
        patch("supertable.processing.generate_filename", return_value="data.parquet"),
    ):
        write_parquet_and_collect_resources(
            pl.DataFrame({"id": [2, 1], "payload": ["b", "a"]}),
            ["id"],
            "/table/data",
            resources,
            compression_level=1,
            footer_md_out=footer_cache,
        )
        stats = extract_stats_rows(
            [resources[0]["file"]], footer_md_cache=footer_cache,
        )

    expected = stats_resource_seals(stats)[resources[0]["file"]]
    assert resource_stats_seal(resources[0]) == expected
    assert resources[0]["file_size"] == len(uploaded[resources[0]["file"]])


def test_compaction_outputs_carry_exact_resource_seals(tmp_path, monkeypatch):
    from supertable.storage.local_storage import LocalStorage

    storage = LocalStorage()
    monkeypatch.setattr(processing, "_storage", storage)
    source_dir = str(tmp_path / "source")
    compacted_dir = str(tmp_path / "compacted")
    source_resources = []
    for offset in (0, 10):
        write_parquet_and_collect_resources(
            pl.DataFrame({"id": [offset + 1, offset + 2]}),
            [],
            source_dir,
            source_resources,
            compression_level=1,
        )

    considered, rows, new_resources, sunset = compact_resources(
        {"resources": source_resources},
        compacted_dir,
        compression_level=1,
        table_config={"max_memory_chunk_size": 1024 * 1024},
        small_only=False,
        required_reads=True,
    )

    assert considered == 2
    assert rows == 4
    assert sunset == {resource["file"] for resource in source_resources}
    assert new_resources
    stats = extract_stats_rows([resource["file"] for resource in new_resources])
    observed = stats_resource_seals(stats)
    assert observed is not None
    assert {
        resource["file"]: resource_stats_seal(resource)
        for resource in new_resources
    } == observed


def test_same_height_corruption_fails_open_only_for_affected_resource():
    genuine = _frame(
        _stat_row("f.parquet", 1, "a" * 64),
        _stat_row("g.parquet", 2, "b" * 64),
    )
    expected = stats_resource_seals(genuine)
    corrupt = genuine.with_columns(
        pl.when(pl.col("file_path") == "f.parquet")
        .then(pl.lit(999))
        .otherwise(pl.col("min_bigint"))
        .alias("min_bigint")
    )
    assert corrupt.height == genuine.height

    safe = stats_for_complete_files(
        corrupt,
        {"f.parquet": 1, "g.parquet": 1},
        expected,
    )
    assert safe is not None
    assert safe.get_column("file_path").to_list() == ["g.parquet"]


def test_foreign_same_shape_stats_and_mismatched_seals_are_never_trusted():
    genuine = _frame(_stat_row("f.parquet", 1, "a" * 64))
    expected = stats_resource_seals(genuine)
    foreign = _frame(_stat_row("f.parquet", 100, "c" * 64))

    assert stats_for_complete_files(
        foreign, {"f.parquet": 1}, expected,
    ).height == 0

    wrong_footer = dataclasses.replace(
        expected["f.parquet"], footer_sha256="d" * 64,
    )
    wrong_count = dataclasses.replace(
        expected["f.parquet"], stats_rows=2,
    )
    for seal in (wrong_footer, wrong_count):
        assert stats_for_complete_files(
            genuine, {"f.parquet": 1}, {"f.parquet": seal},
        ).height == 0


def test_legacy_unsealed_resource_cannot_supply_absence_proof():
    stats = _frame(_stat_row("legacy.parquet", 1, "a" * 64))
    assert stats_for_complete_files(
        stats, {"legacy.parquet": 1}, {},
    ).height == 0
    assert stats_for_complete_files(
        stats, {"legacy.parquet": 1}, None,
    ).height == 0


def test_cached_immutable_stats_validation_skips_warm_rehash_and_groupby(monkeypatch):
    stats = _frame(_stat_row("f.parquet", 1, "a" * 64))
    expected = stats_resource_seals(stats)
    stats_path = "sealed-table/stats/hour=01/v1.parquet"
    processing.cache_stats(stats_path, stats)
    calls = 0
    original = processing._validate_stats_frame_once

    def counted(frame):
        nonlocal calls
        calls += 1
        return original(frame)

    monkeypatch.setattr(processing, "_validate_stats_frame_once", counted)
    try:
        for _ in range(2):
            safe = stats_for_complete_files(
                stats,
                {"f.parquet": 1},
                expected,
                stats_path=stats_path,
            )
            assert safe.height == 1
        assert calls == 1
    finally:
        processing._STATS_CACHE.discard(processing.stats_cache_identity(stats_path))


def test_stats_cache_identity_isolates_organization_and_auth_scope():
    class OpaqueStorage:
        pass

    path = "same/table/stats/v1.parquet"
    storage_a = OpaqueStorage()
    storage_b = OpaqueStorage()
    key_a = processing.stats_cache_identity(
        path, organization="org-a", storage=storage_a,
    )
    assert key_a == processing.stats_cache_identity(
        path, organization="org-a", storage=storage_a,
    )
    assert key_a != processing.stats_cache_identity(
        path, organization="org-b", storage=storage_a,
    )
    assert key_a != processing.stats_cache_identity(
        path, organization="org-a", storage=storage_b,
    )


def test_stats_cache_hard_byte_cap_rejects_oversized_frame(monkeypatch):
    stats = _frame(_stat_row("f.parquet", 1, "a" * 64))
    path = "bounded/stats/v1.parquet"
    constrained = dataclasses.replace(
        processing.settings,
        SUPERTABLE_STATS_CACHE_MAX_BYTES=1,
    )
    monkeypatch.setattr(processing, "settings", constrained)
    processing.cache_stats(path, stats)
    with patch("supertable.processing._read_parquet_safe", return_value=stats) as read:
        processing.load_stats(path)
        processing.load_stats(path)
    assert read.call_count == 2
