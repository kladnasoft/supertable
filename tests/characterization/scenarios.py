"""Declarative registry of sealed characterization scenarios.

Each :class:`Scenario` is materialized to ``tests/golden/<scenario_id>/input``
(byte-frozen parquet + catalog.json) and its expected read is sealed to
``tests/golden/<scenario_id>/expected/result.json`` (or, for failures,
``expected/error.json``).  This module is the single source of truth for *what*
is sealed; the generator and the pytest matrix both import :data:`ALL_SCENARIOS`.

Authoring rules (kept deliberately strict so goldens are reproducible):

* Every physical row carries an explicit ``__timestamp__`` built from
  :func:`fixtures_lib.ts` (offsets from the frozen EPOCH) — never the wall clock.
  The writer only injects ``__timestamp__`` when absent, so pre-populating it is
  how we pin dedup ordering.
* Public columns are authored exactly as a future ``__rowid`` world must keep
  reading them: only the user columns plus the existing internal
  ``__timestamp__``.  No ``__rowid``/``__rn__`` ever appears in a fixture.
* Where the current implementation is genuinely non-deterministic (equal
  ``__timestamp__`` for the same key with *different* non-key values; an arbitrary
  ROW_NUMBER tie-break), the read is shaped so the **sealed** result is stable
  regardless of the tie-break (project only the key, or COUNT), and the
  non-determinism is called out in the description and in CURRENT_BEHAVIOR.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from tests.characterization.fixtures_lib import (
    ErrorExpectation,
    FileSpec,
    Scenario,
    TableSpec,
    ts,
)

TS = "__timestamp__"


# --------------------------------------------------------------------------
# Small authoring helpers
# --------------------------------------------------------------------------

def _file(name: str, columns: Dict[str, str], rows: List[dict]) -> FileSpec:
    return FileSpec(name=name, columns=columns, rows=rows)


def _single(
    scenario_id: str,
    category: str,
    description: str,
    *,
    columns: Dict[str, str],
    rows: List[dict],
    primary_keys: List[str],
    sql: str | None = None,
    projection: List[str] | None = None,
    filters: List[str] | None = None,
    ordered: bool = False,
    dedup_on_read: bool = True,
    deleted_keys: List[list] | None = None,
    simple_name: str = "t",
    expect_error: ErrorExpectation | None = None,
    corrupt_inputs=None,
) -> Scenario:
    """A one-table scenario from a single inline parquet file."""
    return Scenario(
        scenario_id=scenario_id,
        category=category,
        description=description,
        tables=[
            TableSpec(
                simple_name=simple_name,
                primary_keys=primary_keys,
                files=[_file("f1", columns, rows)],
                dedup_on_read=dedup_on_read,
                deleted_keys=deleted_keys or [],
            )
        ],
        sql=sql,
        projection=projection,
        filters=filters,
        ordered=ordered,
        expect_error=expect_error,
        corrupt_inputs=corrupt_inputs,
    )


# Common column shapes
_C_IDVAL = {"id": "int", "val": "str", TS: "timestamp"}


# --------------------------------------------------------------------------
# Post-materialize corruption hooks (error scenarios only)
# --------------------------------------------------------------------------

def _delete_first_parquet(input_dir: Path) -> None:
    files = sorted(input_dir.glob("*.parquet"))
    if files:
        files[0].unlink()


def _scramble_first_parquet(input_dir: Path) -> None:
    files = sorted(input_dir.glob("*.parquet"))
    if files:
        files[0].write_bytes(b"this is definitely not a parquet file\n" * 4)


def _malform_catalog(input_dir: Path) -> None:
    (input_dir / "catalog.json").write_bytes(b'{ "organization": "x", NOT VALID JSON')


# ==========================================================================
# 1. BASIC operations
# ==========================================================================

def _basic() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "basic_insert_single", "basic",
        "Single inserted row reads back verbatim.",
        columns=_C_IDVAL,
        rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="basic_update_delete", category="basic",
        description="One key updated to a newer version, one key soft-deleted.",
        tables=[TableSpec(
            simple_name="events", primary_keys=["id"],
            files=[_file("f1", _C_IDVAL, [
                {"id": 1, "val": "a", TS: ts(seconds=0)},
                {"id": 1, "val": "b", TS: ts(seconds=10)},   # update: b wins
                {"id": 2, "val": "x", TS: ts(seconds=0)},     # tombstoned out
                {"id": 3, "val": "y", TS: ts(seconds=0)},
            ])],
            deleted_keys=[[2]],
        )],
        sql="SELECT id, val FROM events ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "basic_reinsert_reconciled", "basic",
        "Re-insert after delete with the tombstone reconciled away (key not in "
        "deleted_keys) — newest version is visible again.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "v1", TS: ts(seconds=0)},
            {"id": 1, "val": "v2", TS: ts(seconds=10)},
        ],
        primary_keys=["id"], deleted_keys=[],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "basic_reinsert_tombstone_persists", "basic",
        "Re-insert after delete while the tombstone PERSISTS (key still in "
        "deleted_keys) — tombstone is unconditional, key stays excluded even "
        "though a newer row exists.",
        columns=_C_IDVAL,
        rows=[{"id": 1, "val": "v2", TS: ts(seconds=10)}],
        primary_keys=["id"], deleted_keys=[[1]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "basic_empty_table", "basic",
        "A materialized parquet with zero rows reads back as zero rows.",
        columns=_C_IDVAL, rows=[],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    # Zero-file catalog: a table whose snapshot lists NO resources.
    out.append(Scenario(
        scenario_id="basic_zero_file_catalog", category="basic",
        description="A table whose snapshot lists no parquet resources at all.",
        tables=[TableSpec(
            simple_name="t", primary_keys=["id"], files=[],
            schema={"id": "Int64", "val": "String"},
        )],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
        expect_error=ErrorExpectation("RuntimeError", "No parquet files found", "execution"),
    ))

    return out


# ==========================================================================
# 2. MULTI-FILE behavior
# ==========================================================================

def _multi_file() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(Scenario(
        scenario_id="multi_file_dedup", category="multi_file",
        description="Latest version of a key spans two files; newest wins.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", _C_IDVAL, [
                {"id": 1, "val": "old", TS: ts(seconds=0)},
                {"id": 2, "val": "keep", TS: ts(seconds=0)},
            ]),
            _file("f2", _C_IDVAL, [
                {"id": 1, "val": "new", TS: ts(seconds=5)},
            ]),
        ])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="multi_file_disjoint_keys", category="multi_file",
        description="Two files with disjoint key ranges union with no overlap.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", _C_IDVAL, [
                {"id": 1, "val": "a", TS: ts(seconds=0)},
                {"id": 2, "val": "b", TS: ts(seconds=0)},
            ]),
            _file("f2", _C_IDVAL, [
                {"id": 3, "val": "c", TS: ts(seconds=0)},
                {"id": 4, "val": "d", TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="multi_file_update_chain_3", category="multi_file",
        description="A key updated across three files; the newest timestamp wins.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", _C_IDVAL, [{"id": 1, "val": "v0", TS: ts(seconds=0)},
                                   {"id": 9, "val": "anchor", TS: ts(seconds=0)}]),
            _file("f2", _C_IDVAL, [{"id": 1, "val": "v1", TS: ts(seconds=5)}]),
            _file("f3", _C_IDVAL, [{"id": 1, "val": "v2", TS: ts(seconds=10)}]),
        ])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="multi_file_delete_marker_other_file", category="multi_file",
        description="Tombstone is catalog-level; it excludes a key whose newest "
        "version lives in a different file.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", _C_IDVAL, [{"id": 1, "val": "old", TS: ts(seconds=0)},
                                   {"id": 2, "val": "keep", TS: ts(seconds=0)}]),
            _file("f2", _C_IDVAL, [{"id": 1, "val": "new", TS: ts(seconds=5)}]),
        ], deleted_keys=[[1]])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="multi_file_order_reversed", category="multi_file",
        description="Resource list orders the NEWER file first; dedup is by "
        "__timestamp__ not file/resource order, so newest still wins.",
        tables=[TableSpec("m", ["id"], [
            _file("f_new", _C_IDVAL, [{"id": 1, "val": "new", TS: ts(seconds=10)}]),
            _file("f_old", _C_IDVAL, [{"id": 1, "val": "old", TS: ts(seconds=0)}]),
        ])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="multi_file_interleaved_keys", category="multi_file",
        description="Keys interleaved across files; one key updated in the later "
        "file, others unique per file.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", _C_IDVAL, [{"id": 1, "val": "a0", TS: ts(seconds=0)},
                                   {"id": 3, "val": "c", TS: ts(seconds=0)}]),
            _file("f2", _C_IDVAL, [{"id": 2, "val": "b", TS: ts(seconds=0)},
                                   {"id": 1, "val": "a1", TS: ts(seconds=5)}]),
        ])],
        sql="SELECT id, val FROM m ORDER BY id", ordered=True,
    ))

    return out


# ==========================================================================
# 3. TIMESTAMP behavior
# ==========================================================================

def _timestamp() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "ts_out_of_order_physical", "timestamp",
        "Newest __timestamp__ wins regardless of physical row order (newest row "
        "appears physically first).",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "new", TS: ts(seconds=10)},
            {"id": 1, "val": "old", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_microsecond_precision", "timestamp",
        "Two versions 1 microsecond apart; the later microsecond wins.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(micros=1)},
            {"id": 1, "val": "b", TS: ts(micros=2)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_equal_same_key_same_value", "timestamp",
        "Equal timestamp, same key, identical value — two appends with no "
        "overwrite. Neither row supersedes the other (equal __timestamp__ is not "
        "an overwrite), so both physical rows survive.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "same", TS: ts(seconds=5)},
            {"id": 1, "val": "same", TS: ts(seconds=5)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_equal_same_key_diff_value_keyonly", "timestamp",
        "Equal timestamp, same key, DIFFERENT value — two appends, neither "
        "supersedes the other (equal __timestamp__ is not an overwrite). Both "
        "rows survive; projecting only the key yields the key twice.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "x", TS: ts(seconds=5)},
            {"id": 1, "val": "y", TS: ts(seconds=5)},
        ],
        primary_keys=["id"],
        sql="SELECT id FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_equal_different_key", "timestamp",
        "Equal timestamp on different keys — both survive independently.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=5)},
            {"id": 2, "val": "b", TS: ts(seconds=5)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_null_timestamp", "timestamp",
        "A row with a NULL __timestamp__ competes with a non-null version for "
        "the same key; seals DuckDB's NULL ordering under ORDER BY ... DESC.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "nullts", TS: None},
            {"id": 1, "val": "realts", TS: ts(seconds=5)},
            {"id": 2, "val": "onlynull", TS: None},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_naive_timezone", "timestamp",
        "__timestamp__ authored as a tz-naive datetime; dedup ordering still "
        "resolves newest-wins.",
        columns={"id": "int", "val": "str", TS: "timestamp_naive"},
        rows=[
            {"id": 1, "val": "old", TS: ts(seconds=0, tz=False)},
            {"id": 1, "val": "new", TS: ts(seconds=10, tz=False)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "ts_future_then_earlier", "timestamp",
        "A far-future version then an earlier one; the future timestamp is the "
        "'latest' and wins.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "present", TS: ts(seconds=0)},
            {"id": 1, "val": "future", TS: ts(seconds=10_000_000)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    return out


# ==========================================================================
# 4. KEY behavior
# ==========================================================================

def _keys() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "key_string", "keys",
        "String primary key dedups newest-wins per distinct string.",
        columns={"code": "str", "val": "str", TS: "timestamp"},
        rows=[
            {"code": "a", "val": "a0", TS: ts(seconds=0)},
            {"code": "a", "val": "a1", TS: ts(seconds=5)},
            {"code": "b", "val": "b0", TS: ts(seconds=0)},
        ],
        primary_keys=["code"],
        sql="SELECT code, val FROM t ORDER BY code", ordered=True,
    ))

    out.append(_single(
        "key_composite", "keys",
        "Composite (region,id) key; same id under different region stays "
        "distinct.",
        columns={"region": "str", "id": "int", "val": "str", TS: "timestamp"},
        rows=[
            {"region": "US", "id": 1, "val": "us1a", TS: ts(seconds=0)},
            {"region": "US", "id": 1, "val": "us1b", TS: ts(seconds=5)},
            {"region": "EU", "id": 1, "val": "eu1", TS: ts(seconds=0)},
        ],
        primary_keys=["region", "id"],
        sql="SELECT region, id, val FROM t ORDER BY region, id", ordered=True,
    ))

    out.append(_single(
        "key_null_component", "keys",
        "Composite key where one component is NULL; rows with a NULL component "
        "still partition together for dedup.",
        columns={"region": "str", "id": "int", "val": "str", TS: "timestamp"},
        rows=[
            {"region": None, "id": 1, "val": "n0", TS: ts(seconds=0)},
            {"region": None, "id": 1, "val": "n1", TS: ts(seconds=5)},
            {"region": "US", "id": 1, "val": "us", TS: ts(seconds=0)},
        ],
        primary_keys=["region", "id"],
        sql="SELECT region, id, val FROM t ORDER BY region NULLS FIRST, id",
        ordered=True,
    ))

    out.append(_single(
        "key_full_null", "keys",
        "Single NULL key value: all-NULL keys form one dedup partition.",
        columns=_C_IDVAL,
        rows=[
            {"id": None, "val": "a", TS: ts(seconds=0)},
            {"id": None, "val": "b", TS: ts(seconds=5)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id NULLS FIRST", ordered=True,
    ))

    out.append(_single(
        "key_empty_string", "keys",
        "Empty-string key is distinct from any other and from NULL.",
        columns={"code": "str", "val": "str", TS: "timestamp"},
        rows=[
            {"code": "", "val": "e0", TS: ts(seconds=0)},
            {"code": "", "val": "e1", TS: ts(seconds=5)},
            {"code": "x", "val": "x0", TS: ts(seconds=0)},
        ],
        primary_keys=["code"],
        sql="SELECT code, val FROM t ORDER BY code", ordered=True,
    ))

    out.append(_single(
        "key_case_sensitive", "keys",
        "Keys 'A' and 'a' are distinct (case-sensitive).",
        columns={"code": "str", "val": "str", TS: "timestamp"},
        rows=[
            {"code": "A", "val": "upper", TS: ts(seconds=0)},
            {"code": "a", "val": "lower", TS: ts(seconds=0)},
        ],
        primary_keys=["code"],
        sql="SELECT code, val FROM t ORDER BY code", ordered=True,
    ))

    out.append(_single(
        "key_unicode", "keys",
        "Unicode keys (accented, CJK, emoji) are preserved and distinct.",
        columns={"code": "str", "val": "str", TS: "timestamp"},
        rows=[
            {"code": "café", "val": "fr", TS: ts(seconds=0)},
            {"code": "日本", "val": "jp", TS: ts(seconds=0)},
            {"code": "🚀", "val": "rocket", TS: ts(seconds=0)},
        ],
        primary_keys=["code"],
        sql="SELECT code, val FROM t ORDER BY code", ordered=True,
    ))

    out.append(_single(
        "key_large_int_alone", "keys",
        "Large integer key beyond 2^53 selected ALONE (all-int result frame) is "
        "preserved exactly.",
        columns=_C_IDVAL,
        rows=[
            {"id": 9007199254740993, "val": "big", TS: ts(seconds=0)},
            {"id": 1, "val": "small", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "key_int_float_coercion", "keys",
        "Selecting an int key ALONGSIDE a float column triggers numpy "
        "homogeneous-array coercion in result_df.values.tolist(): the int key is "
        "returned as a float. Sealed as-is (a current quirk).",
        columns={"id": "int", "amount": "float", TS: "timestamp"},
        rows=[
            {"id": 1, "amount": 1.5, TS: ts(seconds=0)},
            {"id": 2, "amount": 2.5, TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, amount FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "key_duplicate_physical_rows", "keys",
        "Identical physical rows (same key, ts, value) appended 3x. With no "
        "overwrite/tombstone, all three physical rows are legitimate appends and "
        "all survive (read removes only tombstoned __rowid__s, never duplicates).",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "dup", TS: ts(seconds=0)},
            {"id": 1, "val": "dup", TS: ts(seconds=0)},
            {"id": 1, "val": "dup", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    return out


# ==========================================================================
# 5. DELETE behavior
# ==========================================================================

def _delete() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "delete_nonexistent", "delete",
        "A tombstone for a key that has no rows has no effect.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 2, "val": "b", TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[99]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "delete_twice_idempotent", "delete",
        "The same key tombstoned twice still simply excludes that key.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 2, "val": "b", TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[2], [2]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "delete_unconditional_vs_newer_row", "delete",
        "Tombstones carry no version; a tombstoned key is excluded even when a "
        "far-future row exists for it.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "future", TS: ts(seconds=10_000_000)},
            {"id": 2, "val": "keep", TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[1]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "delete_null_nonkey_value", "delete",
        "Deleting a key whose row has NULL non-key columns works (match is by "
        "key only); a different key with a NULL value remains visible.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": None, TS: ts(seconds=0)},
            {"id": 2, "val": None, TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[1]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "delete_overlong_tombstone_tuple", "delete",
        "A tombstone tuple with MORE components than primary_keys does not "
        "error: DuckDB tolerates the extra unaliased VALUES column and the "
        "anti-join matches on the leading (aliased) component only, so [1, 999] "
        "deletes key id=1 and ignores the trailing 999.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 2, "val": "b", TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[1, 999]],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "delete_cannot_target_null_key", "delete",
        "A tombstone with a NULL key component cannot delete a NULL-keyed row: "
        "the anti-join uses '=', and NULL = NULL is never true, so the row "
        "survives.",
        columns=_C_IDVAL,
        rows=[
            {"id": None, "val": "ghost", TS: ts(seconds=0)},
            {"id": 1, "val": "real", TS: ts(seconds=0)},
        ],
        primary_keys=["id"], deleted_keys=[[None]],
        sql="SELECT id, val FROM t ORDER BY id NULLS FIRST", ordered=True,
    ))

    return out


# ==========================================================================
# 6. QUERY behavior
# ==========================================================================

def _query() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "query_select_star", "query",
        "SELECT * exposes only public columns (no __timestamp__ / __rn__).",
        columns={"id": "int", "name": "str", "amount": "float", TS: "timestamp"},
        rows=[
            {"id": 1, "name": "alice", "amount": 1.5, TS: ts(seconds=0)},
            {"id": 2, "name": "bob", "amount": 2.5, TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT * FROM t ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="projection_read", category="query",
        description="Projection over id only; non-projected/internal cols absent.",
        tables=[TableSpec("p", ["id"], [_file("f1", {"id": "int", "name": "str", TS: "timestamp"}, [
            {"id": 1, "name": "alice", TS: ts(seconds=0)},
            {"id": 2, "name": "bob", TS: ts(seconds=0)},
        ])])],
        projection=["id"], ordered=False,
    ))

    out.append(_single(
        "query_filter_by_key", "query",
        "WHERE on the primary key returns just that key's current row.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 2, "val": "b", TS: ts(seconds=0)},
            {"id": 3, "val": "c", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t WHERE id = 2 ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "query_filter_by_nonkey", "query",
        "WHERE on a non-key column filters post-dedup rows.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "keep", TS: ts(seconds=0)},
            {"id": 2, "val": "drop", TS: ts(seconds=0)},
            {"id": 3, "val": "keep", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t WHERE val = 'keep' ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "query_timestamp_explicit_access", "query",
        "The internal __timestamp__ is hidden from the read view (stripped along "
        "with __rowid__), so it is NOT accessible even when named EXPLICITLY: the "
        "reference fails to bind. Sealed as an error to lock in that system "
        "columns never leak into a user query.",
        columns=_C_IDVAL,
        rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, __timestamp__ FROM t ORDER BY id", ordered=True,
        expect_error=ErrorExpectation("RuntimeError", "__timestamp__", "execution"),
    ))

    out.append(_single(
        "query_aggregation_sum", "query",
        "GROUP BY + SUM over post-dedup rows.",
        columns={"region": "str", "amount": "float", "id": "int", TS: "timestamp"},
        rows=[
            {"region": "US", "id": 1, "amount": 10.0, TS: ts(seconds=0)},
            {"region": "US", "id": 2, "amount": 5.0, TS: ts(seconds=0)},
            {"region": "EU", "id": 3, "amount": 7.0, TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT region, SUM(amount) AS total FROM t GROUP BY region ORDER BY region",
        ordered=True,
    ))

    out.append(_single(
        "query_count_star", "query",
        "COUNT(*) counts current (post-dedup, post-tombstone) rows only.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 1, "val": "a2", TS: ts(seconds=5)},  # dedups to 1
            {"id": 2, "val": "b", TS: ts(seconds=0)},
            {"id": 3, "val": "c", TS: ts(seconds=0)},   # tombstoned
        ],
        primary_keys=["id"], deleted_keys=[[3]],
        sql="SELECT COUNT(*) AS n FROM t",
    ))

    out.append(_single(
        "query_count_distinct", "query",
        "COUNT(DISTINCT region) over post-dedup rows.",
        columns={"region": "str", "id": "int", TS: "timestamp"},
        rows=[
            {"region": "US", "id": 1, TS: ts(seconds=0)},
            {"region": "US", "id": 2, TS: ts(seconds=0)},
            {"region": "EU", "id": 3, TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT COUNT(DISTINCT region) AS regions FROM t",
    ))

    out.append(Scenario(
        scenario_id="query_join_two_tables", category="query",
        description="Inner join of two deduped tables on a shared key.",
        tables=[
            TableSpec("orders", ["id"], [_file("f1", {"id": "int", "cust": "int", TS: "timestamp"}, [
                {"id": 1, "cust": 10, TS: ts(seconds=0)},
                {"id": 2, "cust": 20, TS: ts(seconds=0)},
            ])]),
            TableSpec("customers", ["cust"], [_file("f1", {"cust": "int", "name": "str", TS: "timestamp"}, [
                {"cust": 10, "name": "alice", TS: ts(seconds=0)},
                {"cust": 20, "name": "bob", TS: ts(seconds=0)},
            ])]),
        ],
        table_name="orders",
        sql="SELECT o.id, c.name FROM orders o JOIN customers c ON o.cust = c.cust "
            "ORDER BY o.id",
        ordered=True,
    ))

    out.append(_single(
        "query_limit_order", "query",
        "ORDER BY + LIMIT returns the first N in the sealed order.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "a", TS: ts(seconds=0)},
            {"id": 2, "val": "b", TS: ts(seconds=0)},
            {"id": 3, "val": "c", TS: ts(seconds=0)},
            {"id": 4, "val": "d", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id LIMIT 2", ordered=True,
    ))

    out.append(_single(
        "query_no_rows_predicate", "query",
        "An always-false predicate yields an empty result with the queried schema.",
        columns=_C_IDVAL,
        rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t WHERE 1 = 0", ordered=True,
    ))

    out.append(_single(
        "query_predicate_obsolete_version", "query",
        "A predicate matching an OBSOLETE version's value returns nothing — the "
        "older version is tombstoned at write, so only the current version is "
        "visible to filters.",
        columns=_C_IDVAL,
        rows=[
            {"id": 1, "val": "old", TS: ts(seconds=0)},
            {"id": 1, "val": "new", TS: ts(seconds=10)},
        ],
        primary_keys=["id"],
        sql="SELECT id, val FROM t WHERE val = 'old' ORDER BY id", ordered=True,
    ))

    # Formerly error scenarios whose failure mode (read-time dedup binding) no
    # longer exists: reads never PARTITION BY primary_keys nor ORDER BY
    # __timestamp__, so neither a bogus primary key nor an absent __timestamp__
    # affects the read.  Characterized as the (now benign) results.
    out.append(_single(
        "config_unknown_primary_key_harmless", "query",
        "primary_keys references a column absent from the parquet. Reads no "
        "longer dedup (no PARTITION BY on the key), so the bogus config is inert "
        "and the row reads back verbatim.",
        columns=_C_IDVAL, rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["nope"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    out.append(_single(
        "table_without_timestamp_reads", "query",
        "A table whose parquet has no __timestamp__ column reads back fine: the "
        "read path never orders by __timestamp__ (no read-time dedup), so its "
        "absence is harmless.",
        columns={"id": "int", "val": "str"},
        rows=[{"id": 1, "val": "a"}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t ORDER BY id", ordered=True,
    ))

    return out


# ==========================================================================
# 7. SCHEMA behavior
# ==========================================================================

def _schema() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "schema_all_types", "schema",
        "All supported logical types round-trip through the read in canonical "
        "form (int/bigint/str/bool/float/double/date/timestamp/decimal).",
        columns={
            "id": "int", "n32": "int32", "big": "bigint", "s": "str",
            "b": "bool", "f": "float", "d": "double", "dt": "date",
            "tsv": "timestamp", "dec": "decimal", TS: "timestamp",
        },
        rows=[
            {"id": 1, "n32": 7, "big": 9007199254740993, "s": "hi", "b": True,
             "f": 1.5, "d": 2.25, "dt": ts(seconds=0).date(),
             "tsv": ts(seconds=0), "dec": "12.345000000", TS: ts(seconds=0)},
            {"id": 2, "n32": 8, "big": 1, "s": "bye", "b": False,
             "f": 3.5, "d": 4.75, "dt": ts(seconds=86400).date(),
             "tsv": ts(seconds=1), "dec": "0.000000001", TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, n32, big, s, b, f, d, dt, tsv, dec FROM t ORDER BY id",
        ordered=True,
    ))

    out.append(_single(
        "schema_nullable_nulls", "schema",
        "Nullable columns preserve NULLs through the read (None stays None).",
        columns={"id": "int", "s": "str", "f": "float", TS: "timestamp"},
        rows=[
            {"id": 1, "s": None, "f": None, TS: ts(seconds=0)},
            {"id": 2, "s": "x", "f": 1.0, TS: ts(seconds=0)},
        ],
        primary_keys=["id"],
        sql="SELECT id, s, f FROM t ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="schema_added_column", category="schema",
        description="A column present only in the later file; earlier-file rows "
        "read back NULL for it (union_by_name).",
        tables=[TableSpec("m", ["id"], [
            _file("f1", {"id": "int", "a": "str", TS: "timestamp"}, [
                {"id": 1, "a": "a1", TS: ts(seconds=0)},
            ]),
            _file("f2", {"id": "int", "a": "str", "b": "str", TS: "timestamp"}, [
                {"id": 2, "a": "a2", "b": "b2", TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, a, b FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="schema_missing_column", category="schema",
        description="A column present only in the earlier file; later-file rows "
        "read back NULL for it.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", {"id": "int", "a": "str", "b": "str", TS: "timestamp"}, [
                {"id": 1, "a": "a1", "b": "b1", TS: ts(seconds=0)},
            ]),
            _file("f2", {"id": "int", "a": "str", TS: "timestamp"}, [
                {"id": 2, "a": "a2", TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, a, b FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="schema_reordered_columns", category="schema",
        description="Two files with the same columns in different physical order; "
        "union_by_name matches by name so values stay aligned.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", {"id": "int", "a": "str", "b": "str", TS: "timestamp"}, [
                {"id": 1, "a": "a1", "b": "b1", TS: ts(seconds=0)},
            ]),
            _file("f2", {"b": "str", "a": "str", "id": "int", TS: "timestamp"}, [
                {"id": 2, "a": "a2", "b": "b2", TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, a, b FROM m ORDER BY id", ordered=True,
    ))

    out.append(Scenario(
        scenario_id="schema_widened_numeric", category="schema",
        description="Same column typed int32 in one file and int64 in another; "
        "union_by_name promotes to the wider type.",
        tables=[TableSpec("m", ["id"], [
            _file("f1", {"id": "int", "x": "int32", TS: "timestamp"}, [
                {"id": 1, "x": 10, TS: ts(seconds=0)},
            ]),
            _file("f2", {"id": "int", "x": "bigint", TS: "timestamp"}, [
                {"id": 2, "x": 20, TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, x FROM m ORDER BY id", ordered=True,
    ))

    # Incompatible across files: same column int vs str. Characterize whatever
    # union_by_name does (DuckDB tends to widen to VARCHAR rather than error).
    out.append(Scenario(
        scenario_id="schema_incompatible_types", category="schema",
        description="Same column typed int in one file and str in another; seals "
        "DuckDB union_by_name's resolution (cast vs error).",
        tables=[TableSpec("m", ["id"], [
            _file("f1", {"id": "int", "x": "int", TS: "timestamp"}, [
                {"id": 1, "x": 10, TS: ts(seconds=0)},
            ]),
            _file("f2", {"id": "int", "x": "str", TS: "timestamp"}, [
                {"id": 2, "x": "twenty", TS: ts(seconds=0)},
            ]),
        ])],
        sql="SELECT id, x FROM m ORDER BY id", ordered=True,
    ))

    return out


# ==========================================================================
# 8. ERROR behavior
# ==========================================================================

def _errors() -> List[Scenario]:
    out: List[Scenario] = []

    out.append(_single(
        "error_missing_parquet", "error",
        "A resource file referenced by the catalog is missing on disk.",
        columns=_C_IDVAL, rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t",
        expect_error=ErrorExpectation("RuntimeError", "No files found", "execution"),
        corrupt_inputs=_delete_first_parquet,
    ))

    out.append(_single(
        "error_corrupt_parquet", "error",
        "A resource file exists but is not a valid parquet.",
        columns=_C_IDVAL, rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t",
        expect_error=ErrorExpectation("RuntimeError", "No magic bytes", "execution"),
        corrupt_inputs=_scramble_first_parquet,
    ))

    out.append(_single(
        "error_malformed_catalog", "error",
        "catalog.json is not valid JSON; the adapter fails to load it.",
        columns=_C_IDVAL, rows=[{"id": 1, "val": "a", TS: ts(seconds=0)}],
        primary_keys=["id"],
        sql="SELECT id, val FROM t",
        expect_error=ErrorExpectation("JSONDecodeError", "", "catalog"),
        corrupt_inputs=_malform_catalog,
    ))

    return out


# ==========================================================================
# Registry
# ==========================================================================

ALL_SCENARIOS: List[Scenario] = [
    *_basic(),
    *_multi_file(),
    *_timestamp(),
    *_keys(),
    *_delete(),
    *_query(),
    *_schema(),
    *_errors(),
]

_BY_ID: Dict[str, Scenario] = {s.scenario_id: s for s in ALL_SCENARIOS}
assert len(_BY_ID) == len(ALL_SCENARIOS), "duplicate scenario_id detected"


def all_scenario_ids() -> List[str]:
    return [s.scenario_id for s in ALL_SCENARIOS]


def get_scenario(scenario_id: str) -> Scenario:
    return _BY_ID[scenario_id]


def scenarios_by_category(category: str) -> List[Scenario]:
    return [s for s in ALL_SCENARIOS if s.category == category]
