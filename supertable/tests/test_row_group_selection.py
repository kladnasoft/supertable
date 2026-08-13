"""Conservative row-group selection and decoded-byte estimation contracts."""

from __future__ import annotations

import random
import dataclasses
import types

import polars as pl
import pytest

from supertable.data_classes import PredInterval, RowGroupSelection, SuperSnapshot
from supertable.engine.data_estimator import DataEstimator
from supertable.engine import data_estimator as estimator_module
from supertable.engine.plan_stats import PlanStats
from supertable.config.settings import settings
from supertable.processing import STATS_SCHEMA, select_row_groups_by_predicates


_FOOTER_SEAL = "a" * 64


def _stat_row(
    file_path: str,
    group_id: int,
    column: str,
    lo: int,
    hi: int,
    *,
    rows: int = 4,
    compressed: int = 10,
    uncompressed: int = 40,
    available: bool = True,
) -> dict:
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "footer_sha256": _FOOTER_SEAL,
        "row_group_id": group_id,
        "column_name": column,
        "physical_type": "INT64",
        "logical_type": "",
        "min_bigint": lo,
        "max_bigint": hi,
        "null_count": 0,
        "row_group_rows": rows,
        "compressed_bytes": compressed,
        "uncompressed_bytes": uncompressed,
        "stats_available": available,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    return row


def _stats(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def _eq(value: int) -> PredInterval:
    return PredInterval("numeric", value, True, value, True)


def _string_row(file_path: str, group_id: int, lo: str, hi: str) -> dict:
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "footer_sha256": _FOOTER_SEAL,
        "row_group_id": group_id,
        "column_name": "label",
        "physical_type": "BYTE_ARRAY",
        "logical_type": "STRING",
        "min_string": lo,
        "max_string": hi,
        "null_count": 0,
        "row_group_rows": 4,
        "compressed_bytes": 10,
        "uncompressed_bytes": 40,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    return row


@pytest.mark.parametrize(
    "count,ids",
    [
        (0, (0,)),
        (True, (0,)),
        (2, ()),
        (2, [0]),
        (2, (1, 0)),
        (2, (0, 0)),
        (2, (-1,)),
        (2, (2,)),
        (2, (False,)),
    ],
)
def test_row_group_selection_rejects_noncanonical_values(count, ids):
    with pytest.raises(ValueError):
        RowGroupSelection(count, ids, _FOOTER_SEAL)


def test_snapshot_mapping_is_trailing_and_absence_means_all():
    # Existing positional construction remains valid; no entry is the explicit
    # ALL-groups representation.
    snapshot = SuperSnapshot("s", "t", 1, ["resolved"], {"id"})
    assert snapshot.row_group_selections == {}


def test_occurrences_are_unioned_for_one_shared_scan():
    frame = _stats([
        _stat_row("f", 0, "id", 0, 9),
        _stat_row("f", 1, "id", 10, 19),
        _stat_row("f", 2, "id", 20, 29),
    ])
    result = select_row_groups_by_predicates(
        ["f"], frame, [{"id": _eq(3)}, {"id": _eq(24)}],
    )
    assert result == {"f": RowGroupSelection(3, (0, 2), _FOOTER_SEAL)}


def test_unfiltered_or_unknown_occurrence_means_all():
    frame = _stats([
        _stat_row("f", 0, "id", 0, 9),
        _stat_row("f", 1, "id", 10, 19, available=False),
    ])
    assert select_row_groups_by_predicates(
        ["f"], frame, [{"id": _eq(3)}, {}],
    ) == {}
    # One unavailable group is not silently discarded; the resource is ALL.
    assert select_row_groups_by_predicates(
        ["f"], frame, [{"id": _eq(3)}],
    ) == {}


def test_unknown_group_is_retained_without_poisoning_other_groups():
    frame = _stats([
        _stat_row("f", 0, "id", 0, 9),
        _stat_row("f", 1, "id", 10, 19, available=False),
        _stat_row("f", 2, "id", 20, 29),
    ])

    assert select_row_groups_by_predicates(
        ["f"], frame, [{"id": _eq(3)}],
    ) == {"f": RowGroupSelection(3, (0, 1), _FOOTER_SEAL)}


def test_unsupported_conjunct_does_not_hide_numeric_disjoint_proof():
    frame = _stats([
        _stat_row("f", 0, "id", 0, 9),
        _string_row("f", 0, "a", "z"),
        _stat_row("f", 1, "id", 10, 19),
        _string_row("f", 1, "a", "z"),
    ])

    assert select_row_groups_by_predicates(
        ["f"],
        frame,
        [{
            "id": _eq(3),
            "label": PredInterval("string", "x", True, "x", True),
        }],
    ) == {"f": RowGroupSelection(2, (0,), _FOOTER_SEAL)}


def test_partial_missing_footer_seal_cannot_borrow_another_groups_seal():
    frame = _stats([
        # A corrupt/stale unsealed slot says group 0 is disjoint.  Group 1 has
        # the valid live-footer seal and appears to match.  Accepting the one
        # non-null unique seal would select only group 1 and could omit a live
        # match that actually resides in group 0.
        {**_stat_row("f", 0, "id", 0, 9), "footer_sha256": None},
        _stat_row("f", 1, "id", 10, 19),
    ])

    assert select_row_groups_by_predicates(
        ["f"], frame, [{"id": _eq(15)}],
    ) == {}


def test_all_groups_disjoint_rolls_back_table_wide():
    frame = _stats([
        _stat_row("f1", 0, "id", 0, 9),
        _stat_row("f2", 0, "id", 10, 19),
    ])
    assert select_row_groups_by_predicates(
        ["f1", "f2"], frame, [{"id": _eq(99)}],
    ) == {}


def test_corrupt_complete_manifest_fails_open_before_selection():
    valid = [
        _stat_row("f", 0, "id", 0, 9),
        _stat_row("f", 1, "id", 10, 19),
    ]
    corruptions = [
        valid[:1],  # missing group
        valid + [dict(valid[1])],  # duplicate slot
        [dict(valid[0]), {**valid[1], "row_group_id": 2}],  # id gap
    ]
    for rows in corruptions:
        conformed = DataEstimator._stats_for_complete_files(
            _stats(rows), {"f": 8},
        )
        assert conformed is None or conformed.height == 0
        assert select_row_groups_by_predicates(
            ["f"], conformed, [{"id": _eq(3)}],
        ) == {}


def test_random_exact_stats_kept_groups_cover_every_matching_row_group():
    rng = random.Random(20260813)
    for _case in range(200):
        values_by_group: list[list[int]] = []
        rows = []
        for group_id in range(rng.randint(1, 8)):
            values = [rng.randint(-50, 50) for _ in range(rng.randint(1, 12))]
            values_by_group.append(values)
            rows.append(_stat_row(
                "f", group_id, "id", min(values), max(values), rows=len(values),
            ))
        total_rows = sum(map(len, values_by_group))
        frame = DataEstimator._stats_for_complete_files(
            _stats(rows), {"f": total_rows},
        )
        assert frame is not None and frame.height == len(rows)

        lo, hi = sorted((rng.randint(-60, 60), rng.randint(-60, 60)))
        predicate = PredInterval("numeric", lo, True, hi, True)
        plan = select_row_groups_by_predicates(
            ["f"], frame, [{"id": predicate}],
        )
        kept = (
            set(plan["f"].selected_ids)
            if "f" in plan
            else set(range(len(values_by_group)))
        )
        contributing = {
            group_id
            for group_id, values in enumerate(values_by_group)
            if any(lo <= value <= hi for value in values)
        }
        assert kept.issuperset(contributing)


def test_row_group_compressed_and_decoded_estimates_use_selected_chunks():
    frame = _stats([
        _stat_row("f", 0, "id", 0, 9, compressed=10, uncompressed=40),
        _stat_row("f", 0, "payload", 0, 9, compressed=100, uncompressed=500),
        _stat_row("f", 1, "id", 10, 19, compressed=11, uncompressed=44),
        _stat_row("f", 1, "payload", 10, 19, compressed=101, uncompressed=501),
    ])
    selection = {"f": RowGroupSelection(2, (1,), _FOOTER_SEAL)}
    assert DataEstimator._row_group_byte_estimate(
        frame, ["f"], {"id"}, selection, "compressed_bytes",
    ) == (11, True)
    assert DataEstimator._row_group_byte_estimate(
        frame, ["f"], {"id"}, selection, "uncompressed_bytes",
    ) == (44, True)


def test_legacy_missing_uncompressed_bytes_is_explicitly_incomplete():
    frame = _stats([_stat_row("f", 0, "id", 0, 9)])
    frame = frame.with_columns(pl.lit(None).cast(pl.Int64).alias("uncompressed_bytes"))
    assert DataEstimator._row_group_byte_estimate(
        frame, ["f"], {"id"}, {}, "uncompressed_bytes",
    ) == (0, False)


def test_estimate_wires_raw_key_selection_and_separate_byte_estimates(monkeypatch):
    frame = _stats([
        _stat_row(
            "raw/f.parquet", 0, "id", 0, 9, rows=5_000_000,
        ),
        _stat_row("raw/f.parquet", 1, "id", 10, 19),
        _stat_row(
            "raw/f.parquet", 2, "id", 20, 29, rows=5_000_000,
        ),
    ])
    snapshot = {
        "table_name": "t",
        "last_updated_ms": 123,
        "path": "snapshot.json",
        "version": 1,
        "payload": {
            "snapshot_version": 1,
            "schema": {"id": "BIGINT"},
            "stats_file": "stats.parquet",
            "stats_rows": 3,
            "resources": [{
                "file": "raw/f.parquet",
                "file_size": 1_000,
                "rows": 10_000_004,
            }],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
        },
    }
    estimator = DataEstimator.__new__(DataEstimator)
    estimator.organization = "org"
    estimator.storage = types.SimpleNamespace()
    estimator.tables = [types.SimpleNamespace(
        super_name="s", simple_name="t", columns=["id"],
    )]
    estimator.predicate_constraints = {("s", "t"): [{"id": _eq(15)}]}
    estimator.join_edges = []
    estimator.join_pruning_lanes = None
    estimator.plan_stats = PlanStats()
    estimator.timer = None
    estimator.catalog = None
    estimator._collect_snapshots_from_redis = (
        lambda organization, super_name: [snapshot]
    )
    estimator._to_duckdb_path = lambda key: f"resolved://{key}"

    class _DummySuper:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(estimator_module, "SuperTable", _DummySuper)
    monkeypatch.setattr(
        estimator_module, "load_stats",
        lambda *args, **kwargs: frame,
    )
    monkeypatch.setattr(
        estimator_module,
        "settings",
        dataclasses.replace(
            settings,
            SUPERTABLE_READ_PRUNING_ENABLED=True,
            SUPERTABLE_READ_PROJECTION_SIZING_ENABLED=True,
        ),
    )

    reflection = estimator.estimate()
    super_snapshot = reflection.supers[0]
    assert super_snapshot.files == ["resolved://raw/f.parquet"]
    assert super_snapshot.row_group_selections == {
        "raw/f.parquet": RowGroupSelection(3, (1,), _FOOTER_SEAL),
    }
    # Existing engines retain file-level projection/source semantics.
    assert reflection.reflection_bytes == 30
    assert reflection.source_bytes == 1_000
    # Island-native estimates charge only row group 1's id chunk.
    assert reflection.row_group_scan_bytes == 10
    assert reflection.row_group_scan_bytes_complete is True
    assert reflection.decoded_bytes == 40
    assert reflection.decoded_bytes_complete is True
    assert reflection.selected_decoded_bytes == 40
    assert reflection.selected_decoded_bytes_complete is True
    assert reflection.proof_decoded_bytes == 0
    assert reflection.proof_decoded_bytes_complete is True

    monkeypatch.setattr(
        estimator_module,
        "settings",
        dataclasses.replace(
            settings,
            SUPERTABLE_READ_PRUNING_ENABLED=False,
            SUPERTABLE_READ_PROJECTION_SIZING_ENABLED=True,
        ),
    )
    disabled = estimator.estimate()
    assert disabled.supers[0].row_group_selections == {}
    assert disabled.row_group_scan_bytes == 30

    # Active deletion vectors require a selected-group rowid anti join plus a
    # full-resource rowid identity proof. System chunks are omitted from stats,
    # so the estimator charges the whole file as a compressed upper bound and
    # manifest rows as the decoded Int64+validity bound for both scans.
    snapshot["payload"].update({
        "tombstone": "dv.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": "a" * 64,
    })
    monkeypatch.setattr(
        estimator_module,
        "settings",
        dataclasses.replace(
            settings,
            SUPERTABLE_READ_PRUNING_ENABLED=True,
            SUPERTABLE_READ_PROJECTION_SIZING_ENABLED=True,
        ),
    )
    with_tombstone = estimator.estimate()
    assert with_tombstone.supers[0].row_group_selections == {
        "raw/f.parquet": RowGroupSelection(3, (1,), _FOOTER_SEAL),
    }
    assert with_tombstone.row_group_scan_bytes == 10 + 1_000
    assert with_tombstone.row_group_scan_bytes_complete is True
    # The selected scan remains four rows wide even though first use must stream
    # a ten-million-row full-file rowid proof. Both stay in the conservative
    # total work bound, but only selected buffers drive per-batch row width.
    assert with_tombstone.selected_decoded_bytes == 40 + (4 * 9)
    assert with_tombstone.selected_decoded_bytes_complete is True
    assert with_tombstone.proof_decoded_bytes == 10_000_004 * 9
    assert with_tombstone.proof_decoded_bytes_complete is True
    assert with_tombstone.decoded_bytes == (
        with_tombstone.selected_decoded_bytes
        + with_tombstone.proof_decoded_bytes
    )
    assert with_tombstone.decoded_bytes_complete is True


def test_rle_page_size_does_not_underestimate_fixed_width_decoded_memory():
    # One million repeated BIGINT values can have a tiny RLE/dictionary encoded
    # Parquet page. Decoded memory is nevertheless at least the logical Int64
    # buffer plus conservative validity/alignment slack.
    frame = _stats([
        _stat_row(
            "f", 0, "id", 7, 7,
            rows=1_000_000, compressed=128, uncompressed=256,
        ),
    ])
    estimator = DataEstimator.__new__(DataEstimator)
    decoded, complete = estimator._decoded_row_group_estimate(
        frame, ["f"], {"id"}, {}, {"id": "BIGINT"},
    )
    assert complete is True
    assert decoded == 1_000_000 * 9
    assert decoded > frame["uncompressed_bytes"].item()


def test_variable_width_rle_decoded_memory_is_unknown_not_page_sized():
    frame = _stats([
        _stat_row(
            "f", 0, "payload", 1, 1,
            rows=1_000_000, compressed=128, uncompressed=256,
        ),
    ])
    estimator = DataEstimator.__new__(DataEstimator)
    assert estimator._decoded_row_group_estimate(
        frame, ["f"], {"payload"}, {}, {"payload": "VARCHAR"},
    ) == (0, False)
