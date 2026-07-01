"""Data-Quality profiler must never reference read-view-hidden system columns.

Regression for the binder error seen in monitoring during a normal write::

    Binder Error: Referenced column "__rowid__" not found in FROM clause!
    SELECT COUNT(*) AS __total, COUNT("__rowid__") AS __present___rowid__, ...

Root cause: the merge-on-read write path injects two MATERIALISED system
columns into every data file — ``__rowid__`` (deletion-vector anti-join key)
and ``__timestamp__`` (dedup / partition key) — and they land in the STORED
table schema.  The read view strips both
(``COLUMNS(c -> c NOT IN ('__rowid__','__timestamp__'))``), so a profiler that
queries THROUGH that view can never select them.  The DQ quick check built its
column list straight from the stored schema, filtering only ``_sys_``-prefixed
names, so ``__rowid__`` slipped in and the single all-columns SQL failed to
bind — killing the whole table's quick profile, not just those columns.

Contract under test:
  * ``is_profilable_column`` rejects ``__rowid__`` / ``__timestamp__`` and
    ``_sys_*`` internals, accepts genuine user columns;
  * ``build_quick_sql`` emits SQL that references neither hidden system column
    (reproducing and sealing the exact failure) while still profiling users;
  * ``parse_quick_result`` mirrors the same exclusion (build/parse stay in
    lock-step — a divergence would KeyError or mis-map columns);
  * the hidden set stays identical to the read view's EXCLUDE
    (engine_common.ROWID_COL / TIMESTAMP_COL) so the two can never drift.
"""
from __future__ import annotations

from supertable.quality.checker import (
    SYSTEM_COLUMNS,
    is_profilable_column,
    build_quick_sql,
    parse_quick_result,
)

# The exact schema shape MetaReader.get_table_schema returns for a normally
# written table: real user columns PLUS the injected system columns.
_SCHEMA = [
    ("client", "VARCHAR"),
    ("amount", "BIGINT"),
    ("__rowid__", "BIGINT"),
    ("__timestamp__", "TIMESTAMP"),
    ("_sys_internal", "VARCHAR"),
]


class TestIsProfilableColumn:

    def test_rejects_read_view_hidden_system_columns(self):
        assert not is_profilable_column("__rowid__")
        assert not is_profilable_column("__timestamp__")

    def test_rejects_sys_prefixed_internal_columns(self):
        assert not is_profilable_column("_sys_anything")

    def test_accepts_genuine_user_columns(self):
        for name in ("client", "amount", "grp", "created_at", "__custom"):
            # NB: a user column that merely *starts* with "__" (e.g. "__custom")
            # is still profilable — only the two reserved system names are hidden.
            assert is_profilable_column(name), name


class TestBuildQuickSql:

    def test_omits_hidden_system_columns(self):
        """The core regression: no reference to __rowid__/__timestamp__."""
        sql = build_quick_sql("demo.facts_probe_audit", _SCHEMA)
        assert '"__rowid__"' not in sql
        assert '"__timestamp__"' not in sql
        # Not even as an aggregate alias (the shape from the reported error).
        assert "__present___rowid__" not in sql
        assert "__present___timestamp__" not in sql

    def test_still_profiles_user_columns(self):
        """Real columns must remain fully profiled (fix is not over-broad)."""
        sql = build_quick_sql("demo.facts_probe_audit", _SCHEMA)
        assert 'COUNT("client") AS __present_client' in sql
        assert 'COUNT("amount") AS __present_amount' in sql
        # Numeric column gets the numeric-only aggregates.
        assert 'AVG(TRY_CAST("amount" AS DOUBLE)) AS __avg_amount' in sql

    def test_sql_binds_only_visible_columns(self):
        """No hidden/internal column name appears anywhere in the projection."""
        sql = build_quick_sql("demo.t", _SCHEMA)
        for hidden in ("__rowid__", "__timestamp__", "_sys_internal"):
            assert f'"{hidden}"' not in sql, hidden


class TestParseQuickResult:

    def test_skips_system_columns_in_parse(self):
        """parse must ignore the same columns build skips — else it reads
        aliases (e.g. __present___rowid__) that build never emitted."""
        # A realistic single-row result for the user columns only.
        row = {
            "__total": 10,
            "__present_client": 9, "__distinct_client": 4,
            "__present_amount": 10, "__distinct_amount": 7,
            "__min_amount": 0, "__max_amount": 100,
            "__avg_amount": 42.0, "__stddev_amount": 3.0,
            "__zero_amount": 1, "__neg_amount": 0,
        }
        parsed = parse_quick_result(row, _SCHEMA)
        cols = parsed["columns"]
        assert "__rowid__" not in cols
        assert "__timestamp__" not in cols
        assert "_sys_internal" not in cols
        assert set(cols) == {"client", "amount"}
        assert parsed["total"] == 10


class TestReadViewContract:

    def test_hidden_set_matches_read_view_exclude(self):
        """Seal against drift: the DQ hidden set must equal the columns the
        read view strips. If someone adds a third hidden system column to the
        read path, this fails until the DQ profiler is taught to skip it too."""
        from supertable.engine.engine_common import ROWID_COL, TIMESTAMP_COL
        assert SYSTEM_COLUMNS == {ROWID_COL, TIMESTAMP_COL}
