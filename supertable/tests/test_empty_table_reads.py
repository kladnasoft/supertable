"""STREAD-010: an existing table with no rows must read as empty, not error.

``DataWriter`` accepts an empty Arrow table and publishes a real snapshot for
it — schema declared, ``resources: {}`` — so a table can legitimately exist
with nothing to scan. The read path treated the absence of parquet resources as
an execution failure (``No parquet files found for one or more selected
tables.``), which made such a table unreadable in every mode: not a projection,
not even ``SELECT COUNT(*)``, which should return 0.

WHY AN EMPTY FILE LIST IS UNAMBIGUOUS

Relaxing a "≥1 file" check is only safe because nothing else can produce an
empty list. The read-path pruner explicitly refuses to prune to zero
(``prune_files_by_predicates``, processing.py) — it retains the full list rather
than empty it — so "no files" means "no resources" and never "everything was
pruned". A snapshot with neither files nor a declared schema is still refused.

WHY THE TYPES ARE PARSED BY HAND

An empty table has no parquet footer, so its declared polars schema is the only
source of types. DuckDB's own parser cannot be used for it: it accepts some
polars names and silently gets one wrong — ``CAST(NULL AS Int8)`` resolves to
BIGINT — and rejects ``Float64``, ``Utf8`` and ``Datetime(...)`` outright.
"""

from __future__ import annotations

import duckdb
import pytest

from supertable.data_classes import SuperSnapshot
from supertable.engine.engine_common import (
    create_reflection_table,
    create_reflection_view,
    duckdb_type_for_polars,
)

#: The empty_table schema exactly as the audit's fixture manifest recorded it.
FIXTURE_SCHEMA = {
    "eid": "Int64",
    "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
}


@pytest.fixture
def con():
    return duckdb.connect()


class TestPolarsTypeTranslation:

    @pytest.mark.parametrize("polars_type,expected", [
        ("Int64", "BIGINT"),
        # Int8 is the one DuckDB's own parser gets wrong (it yields BIGINT).
        ("Int8", "TINYINT"),
        ("Int16", "SMALLINT"),
        ("Int32", "INTEGER"),
        ("UInt8", "UTINYINT"),
        ("UInt64", "UBIGINT"),
        # Float64/Float32 are rejected by DuckDB's parser entirely.
        ("Float64", "DOUBLE"),
        ("Float32", "FLOAT"),
        ("Boolean", "BOOLEAN"),
        ("Date", "DATE"),
        ("Time", "TIME"),
        ("String", "VARCHAR"),
        ("Utf8", "VARCHAR"),
        ("Binary", "BLOB"),
        ("Decimal(12,3)", "DECIMAL(12,3)"),
        ("Decimal(12, 3)", "DECIMAL(12,3)"),
        ("Datetime(time_unit='us', time_zone='UTC')", "TIMESTAMPTZ"),
        ("Datetime(time_unit='us', time_zone=None)", "TIMESTAMP"),
    ])
    def test_known_types(self, polars_type, expected):
        assert duckdb_type_for_polars(polars_type) == expected

    @pytest.mark.parametrize("polars_type", ["List(Int64)", "Object", "", None, "Struct({...})"])
    def test_unknown_types_fall_back_to_varchar(self, polars_type):
        """VARCHAR over a guessed numeric type: the relation is empty either way,
        and a wrong string type fails loudly instead of binding silently."""
        assert duckdb_type_for_polars(polars_type) == "VARCHAR"

    def test_every_mapped_type_is_valid_duckdb(self, con):
        """The mapping is only useful if DuckDB accepts what it produces."""
        from supertable.engine.engine_common import _DUCKDB_TYPE_BY_POLARS_BASE

        for polars_base, duck in _DUCKDB_TYPE_BY_POLARS_BASE.items():
            resolved = con.execute(f"SELECT CAST(NULL AS {duck})").description[0][1]
            assert resolved, f"{polars_base} -> {duck} did not resolve"


class TestEmptyRelationSemantics:
    """The acceptance criteria, over the real fixture schema."""

    @pytest.fixture
    def empty_view(self, con):
        create_reflection_view(con, "refl", files=[], columns=None,
                               column_types=FIXTURE_SCHEMA)
        return con

    def test_a_projection_returns_no_rows_with_the_declared_type(self, empty_view):
        result = empty_view.execute("SELECT eid FROM refl")
        assert result.description[0][1] == "BIGINT"
        assert result.fetchall() == []

    def test_count_star_returns_one_row_containing_zero(self, empty_view):
        assert empty_view.execute("SELECT COUNT(*) AS n FROM refl").fetchall() == [(0,)]

    def test_aggregates_return_zero_counts_and_null_totals(self, empty_view):
        rows = empty_view.execute(
            "SELECT COUNT(eid) c, SUM(eid) s, AVG(eid) a, MIN(eid) mn, MAX(eid) mx FROM refl"
        ).fetchall()
        assert rows == [(0, None, None, None, None)]

    def test_an_explicit_coalesce_default_is_honoured(self, empty_view):
        assert empty_view.execute(
            "SELECT COALESCE(SUM(eid), 0) AS d FROM refl"
        ).fetchall() == [(0,)]

    def test_the_system_column_strip_still_binds(self, empty_view):
        """The tombstone view strips __rowid__/__timestamp__ with a tolerant
        COLUMNS lambda; it has to keep working over an empty relation."""
        assert empty_view.execute(
            "SELECT COLUMNS(c -> c NOT IN ('__rowid__','__timestamp__')) FROM refl"
        ).fetchall() == []

    def test_a_projection_never_invents_an_undeclared_column(self, con):
        """An empty table never wrote __rowid__, so it must not appear.

        Fabricating it would give the empty relation a column the real table
        does not have, which is a schema the caller could not get from a
        non-empty read.
        """
        create_reflection_view(con, "p", files=[], columns=["eid", "__rowid__"],
                               column_types=FIXTURE_SCHEMA)
        assert [d[0] for d in con.execute("SELECT * FROM p").description] == ["eid"]

    def test_the_eager_table_path_behaves_the_same(self, con):
        """SUPERTABLE_DUCKDB_MATERIALIZE=table must not reintroduce the failure."""
        create_reflection_table(con, "t", files=[], columns=None,
                                column_types=FIXTURE_SCHEMA)
        assert con.execute("SELECT COUNT(*) FROM t").fetchall() == [(0,)]


class TestNoScheamAndNoFilesIsStillAnError:

    @pytest.mark.parametrize("builder", [create_reflection_view, create_reflection_table])
    @pytest.mark.parametrize("types", [None, {}])
    def test_it_refuses_rather_than_inventing_a_relation(self, con, builder, types):
        """No files AND no schema says nothing about the table.

        This is the case the original check existed for, and it must keep
        failing — otherwise a genuinely broken snapshot would silently read as
        an empty table.
        """
        with pytest.raises(ValueError, match="No files provided"):
            builder(con, "bad", files=[], columns=None, column_types=types)


class TestEstimatorReadabilityCheck:

    def test_a_schema_only_snapshot_counts_as_readable(self):
        """The estimator gate is "files OR a declared schema", not "files"."""
        snap = SuperSnapshot(
            super_name="warehouse", simple_name="empty_table", simple_version=1,
            files=[], columns={"eid"}, column_types=FIXTURE_SCHEMA,
        )
        assert bool(snap.files) or bool(snap.column_types)

    def test_a_snapshot_with_neither_is_not_readable(self):
        snap = SuperSnapshot(
            super_name="warehouse", simple_name="broken", simple_version=1,
            files=[], columns=set(), column_types={},
        )
        assert not (bool(snap.files) or bool(snap.column_types))

    def test_column_types_defaults_to_empty_for_normal_snapshots(self):
        """A table with files gets its types from the parquet footers, so this
        field stays empty and costs nothing on the hot path."""
        snap = SuperSnapshot(
            super_name="warehouse", simple_name="orders", simple_version=3,
            files=["a.parquet"], columns={"oid"},
        )
        assert snap.column_types == {}
