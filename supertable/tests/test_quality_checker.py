"""Data-Quality profiler must never reference read-view-hidden system columns.

Regression for the binder error seen in monitoring during a normal write::

    Binder Error: Referenced column "__rowid__" not found in FROM clause!
    SELECT COUNT(*) AS __total, COUNT("__rowid__") AS __present___rowid__, ...

Root cause: the merge-on-read path's physical/legacy schema can contain five
internal columns that the public read view strips.  A profiler that queries
THROUGH that view can never select them.  The DQ quick check built its column
list straight from the stored schema, filtering only ``_sys_``-prefixed names,
so an internal column slipped in and the single all-columns SQL failed to bind
— killing the whole table's quick profile, not just that column.

Contract under test:
  * ``is_profilable_column`` rejects every read-view-hidden system column and
    ``_sys_*`` internals, accepts genuine user columns;
  * ``build_quick_sql`` emits SQL that references neither hidden system column
    (reproducing and sealing the exact failure) while still profiling users;
  * ``parse_quick_result`` mirrors the same exclusion (build/parse stay in
    lock-step — a divergence would KeyError or mis-map columns);
  * the hidden set stays identical to the read view's EXCLUDE
    (engine_common's five canonical constants) so the two can never drift.
"""
from __future__ import annotations

import pytest

from supertable.quality.checker import (
    SYSTEM_COLUMNS,
    _col_category,
    _profile_columns,
    is_profilable_column,
    filter_visible_columns,
    validate_incremental_column,
    validate_custom_rule,
    build_quick_sql,
    build_custom_rule_sql,
    build_deep_numeric_sql,
    evaluate_custom_rule,
    parse_quick_result,
    quality_table_fqn,
    validate_quality_table_name,
)

# The exact schema shape MetaReader.get_table_schema returns for a normally
# written table: real user columns PLUS the injected system columns.
_SCHEMA = [
    ("client", "VARCHAR"),
    ("amount", "BIGINT"),
    ("__rowid__", "BIGINT"),
    ("__timestamp__", "TIMESTAMP"),
    ("__file__", "VARCHAR"),
    ("__supertable_source_file__", "VARCHAR"),
    ("__supertable_scan_filename__", "VARCHAR"),
    ("_sys_internal", "VARCHAR"),
]

_READ_VIEW_HIDDEN = (
    "__rowid__",
    "__timestamp__",
    "__file__",
    "__supertable_source_file__",
    "__supertable_scan_filename__",
)


class TestIsProfilableColumn:

    def test_rejects_read_view_hidden_system_columns(self):
        for name in _READ_VIEW_HIDDEN:
            assert not is_profilable_column(name)
            # DuckDB identifiers are case-insensitive, including quoted ones.
            assert not is_profilable_column(name.upper())

    def test_rejects_sys_prefixed_internal_columns(self):
        assert not is_profilable_column("_sys_anything")

    def test_accepts_genuine_user_columns(self):
        for name in ("client", "amount", "grp", "created_at", "__custom"):
            # NB: a user column that merely *starts* with "__" (e.g. "__custom")
            # is still profilable — only the reserved system names are hidden.
            assert is_profilable_column(name), name


class TestBuildQuickSql:

    def test_omits_hidden_system_columns(self):
        """The core regression: no reference to any hidden system column."""
        sql = build_quick_sql("demo.facts_probe_audit", _SCHEMA)
        for hidden in _READ_VIEW_HIDDEN:
            assert f'"{hidden}"' not in sql
            # Not even as an aggregate alias (the reported error's shape).
            assert f"__present_{hidden}" not in sql

    def test_still_profiles_user_columns(self):
        """Real columns must remain fully profiled (fix is not over-broad)."""
        sql = build_quick_sql("demo.facts_probe_audit", _SCHEMA)
        assert 'COUNT("client") AS __present_client' in sql
        assert 'COUNT("amount") AS __present_amount' in sql
        # Numeric column gets the numeric-only aggregates.
        assert 'THEN AVG("amount") ELSE NULL END AS __avg_amount' in sql
        assert 'TRY_CAST("amount" AS DOUBLE)' not in sql

    def test_float_aliases_keep_nonfinite_filter_while_exact_types_do_not_narrow(self):
        sql = build_quick_sql("demo.t", [
            ("f32", "Float32"),
            ("f64", "Float64"),
            ("large", "BIGINT"),
            ("precise", "DECIMAL(38,10)"),
        ])
        assert 'TRY_CAST("f32" AS DOUBLE)' in sql
        assert 'TRY_CAST("f64" AS DOUBLE)' in sql
        assert 'TRY_CAST("large" AS DOUBLE)' not in sql
        assert 'TRY_CAST("precise" AS DOUBLE)' not in sql
        assert 'CAST(MIN("precise") AS VARCHAR) AS __min_precise' in sql

        deep_integer = build_deep_numeric_sql("demo.t", "large", "BIGINT")
        deep_decimal = build_deep_numeric_sql(
            "demo.t", "precise", "DECIMAL(38,10)",
        )
        deep_float = build_deep_numeric_sql("demo.t", "f64", "Float64")
        assert 'TRY_CAST("large" AS DOUBLE)' not in deep_integer
        assert 'GROUP BY finite_value' in deep_integer
        assert 'CAST(s.min_value AS VARCHAR) AS min_value' in deep_decimal
        assert 'TRY_CAST("f64" AS DOUBLE)' in deep_float

    def test_sql_binds_only_visible_columns(self):
        """No hidden/internal column name appears anywhere in the projection."""
        sql = build_quick_sql("demo.t", _SCHEMA)
        for hidden in (*_READ_VIEW_HIDDEN, "_sys_internal"):
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
        for hidden in _READ_VIEW_HIDDEN:
            assert hidden not in cols
        assert "_sys_internal" not in cols
        assert set(cols) == {"client", "amount"}
        assert parsed["total"] == 10


class TestReadViewContract:

    def test_hidden_set_matches_read_view_exclude(self):
        """Seal against drift: the DQ hidden set must equal the columns the
        read view strips. If someone adds a third hidden system column to the
        read path, this fails until the DQ profiler is taught to skip it too."""
        from supertable.engine.engine_common import (
            ROWID_COL,
            SCAN_FILENAME_COL,
            SOURCE_FILE_COL,
            TIMESTAMP_COL,
            TOMBSTONE_FILE_COL,
        )
        assert SYSTEM_COLUMNS == {
            ROWID_COL,
            TIMESTAMP_COL,
            TOMBSTONE_FILE_COL,
            SOURCE_FILE_COL,
            SCAN_FILENAME_COL,
        }


class TestExactTopLevelTypes:

    def test_scalar_types_are_classified(self):
        assert _col_category("DECIMAL(18, 2)") == "numeric"
        assert _col_category("Float32") == "numeric"
        assert _col_category("Float64") == "numeric"
        assert _col_category("TIMESTAMP WITH TIME ZONE") == "date"
        assert _col_category("VARCHAR") == "string"
        assert _col_category("Categorical") == "string"
        assert _col_category("Enum('a', 'b')") == "string"
        assert _col_category("BOOLEAN") == "bool"

    def test_relative_temporal_types_are_not_misreported_as_c3_instants(self):
        for dtype in ("TIME", "TIME WITH TIME ZONE", "Duration(us)", "INTERVAL"):
            assert _col_category(dtype) == "other"

    def test_nested_inner_scalar_does_not_leak_into_category(self):
        for dtype in (
            "INTEGER[]",
            "LIST(INTEGER)",
            "list<item: int64>",
            "STRUCT(id INTEGER, name VARCHAR)",
            "struct<id: int64>",
            "MAP(VARCHAR, DOUBLE)",
        ):
            assert _col_category(dtype) == "other", dtype

    def test_sanitized_aliases_are_collision_free_and_stable(self):
        columns = [("a-b", "VARCHAR"), ("a b", "VARCHAR"), ("a_b", "VARCHAR")]
        first = _profile_columns(columns)
        assert first == _profile_columns(columns)
        assert len({alias.casefold() for _, _, alias in first}) == 3


class TestVisibleColumnValidation:

    def test_filter_is_the_shared_public_schema_boundary(self):
        assert filter_visible_columns(_SCHEMA) == [
            ("client", "VARCHAR"),
            ("amount", "BIGINT"),
        ]

    def test_incremental_rejects_hidden_missing_and_non_temporal_columns(self):
        for hidden in _READ_VIEW_HIDDEN:
            assert validate_incremental_column(hidden, _SCHEMA).code == "hidden_column"
        assert validate_incremental_column("missing", _SCHEMA).code == "missing_column"
        assert validate_incremental_column("amount", _SCHEMA).code == "unsupported_incremental_type"

    def test_invalid_incremental_config_degrades_to_a_full_scan(self):
        sql = build_quick_sql(
            "demo.t", _SCHEMA,
            incremental_column="__timestamp__",
            last_check_ts="2026-08-14T00:00:00+00:00",
        )
        assert "\nWHERE " not in sql
        assert '"__timestamp__"' not in sql

    def test_public_temporal_incremental_column_is_escaped(self):
        schema = _SCHEMA + [("event_time", "TIMESTAMP")]
        sql = build_quick_sql(
            "demo.t", schema,
            incremental_column="event_time",
            last_check_ts="2026-08-14T00:00:00+00:00' OR TRUE --",
        )
        assert 'WHERE "event_time" >' in sql
        assert "00:00'' OR TRUE --'" in sql

    def test_table_fqn_quotes_each_identifier_and_rejects_inert_scopes(self):
        assert quality_table_fqn("lake-with-hyphen", "facts") == (
            '"lake-with-hyphen"."facts"'
        )
        for table_name in (
            "facts.other", "facts;drop", "two words", "__data_quality__",
        ):
            assert not validate_quality_table_name(table_name).valid


class TestCustomRuleSafety:

    def test_structured_rules_validate_against_visible_schema(self):
        valid = {"rule_type": "column_min", "column_name": "amount", "threshold": 0}
        assert validate_custom_rule(valid, _SCHEMA).valid
        for column_name in _READ_VIEW_HIDDEN:
            hidden = {
                "rule_type": "column_min",
                "column_name": column_name,
                "threshold": 0,
            }
            assert validate_custom_rule(hidden, _SCHEMA).code == "hidden_column"
        missing = {"rule_type": "column_min", "column_name": "other", "threshold": 0}
        assert validate_custom_rule(missing, _SCHEMA).code == "missing_column"

    def test_custom_sql_is_read_only_and_cannot_reference_hidden_columns(self):
        assert validate_custom_rule({
            "rule_type": "custom_sql",
            "sql": "SELECT COUNT(*) AS violations FROM demo.t",
        }, _SCHEMA).valid
        for column_name in _READ_VIEW_HIDDEN:
            result = validate_custom_rule({
                "rule_type": "custom_sql",
                "sql": f'SELECT COUNT("{column_name.upper()}") AS violations FROM demo.t',
            }, _SCHEMA, table_fqn="demo.t")
            assert result.code == "hidden_column"
        assert validate_custom_rule({
            "rule_type": "custom_sql", "sql": "DELETE FROM demo.t",
        }, _SCHEMA).code == "non_read_statement"

    @pytest.mark.parametrize(
        ("sql", "code"),
        [
            ("SELECT COUNT(*) FROM demo.other", "table_scope"),
            ("SELECT COUNT(*) FROM __data_quality__", "system_table"),
            ("SELECT 1", "table_scope"),
            (
                "SELECT COUNT(*) FROM demo.t a CROSS JOIN demo.t b",
                "cross_table_scope",
            ),
            ("SELECT * FROM read_parquet('foreign.parquet')", "table_function_scope"),
            (
                "WITH RECURSIVE counter(x) AS ("
                "SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < 1000000000"
                ") SELECT COUNT(*) + (SELECT COUNT(*) FROM demo.t) FROM counter",
                "recursive_query",
            ),
            (
                "SELECT COUNT(*) FROM "
                "UNNEST(GENERATE_SERIES(1, 1000000000)) AS generated(value)",
                "row_generator_scope",
            ),
        ],
    )
    def test_custom_sql_is_confined_to_its_attached_table(self, sql, code):
        rule = {"rule_type": "custom_sql", "sql": sql}
        result = validate_custom_rule(rule, _SCHEMA, table_fqn="demo.t")
        assert not result.valid
        assert result.code == code

    def test_custom_sql_cte_over_its_attached_table_is_rejected(self):
        rule = {
            "rule_type": "custom_sql",
            "sql": (
                "WITH source AS (SELECT amount FROM demo.t) "
                "SELECT MAX(amount) FROM source"
            ),
        }
        result = validate_custom_rule(
            rule,
            _SCHEMA,
            table_fqn="demo.t",
        )
        assert not result.valid
        assert result.code == "cte_query"

    def test_custom_sql_accepts_bounded_aggregate_and_filter_expressions(self):
        rule = {
            "rule_type": "custom_sql",
            "sql": (
                "SELECT COUNT(*) + SUM(amount) AS violations "
                "FROM demo.t WHERE amount < -100 OR client IS NULL"
            ),
        }
        assert validate_custom_rule(
            rule,
            _SCHEMA,
            table_fqn="demo.t",
        ).valid

    @pytest.mark.parametrize(
        ("sql", "code"),
        [
            ("SELECT COUNT(*) FROM demo.t", "projection_alias"),
            ("SELECT COUNT(*) AS other FROM demo.t", "projection_alias"),
            (
                "SELECT COUNT(*) AS violations, SUM(amount) AS total FROM demo.t",
                "projection_shape",
            ),
            (
                "SELECT MAX(amount) > 0 AS violations FROM demo.t",
                "non_numeric_projection",
            ),
        ],
    )
    def test_custom_sql_requires_one_named_numeric_violations_projection(
        self, sql, code,
    ):
        result = validate_custom_rule(
            {"rule_type": "custom_sql", "sql": sql},
            _SCHEMA,
            table_fqn="demo.t",
        )
        assert not result.valid
        assert result.code == code

    @pytest.mark.parametrize(
        ("sql", "code"),
        [
            ("SELECT COUNT(missing) FROM demo.t", "missing_column"),
            (
                "SELECT COUNT(*) FROM demo.t WHERE missing > 0",
                "missing_column",
            ),
            ("SELECT COUNT(other.amount) FROM demo.t", "column_scope"),
        ],
    )
    def test_custom_sql_columns_are_resolved_before_execution(self, sql, code):
        result = validate_custom_rule(
            {"rule_type": "custom_sql", "sql": sql},
            _SCHEMA,
            table_fqn="demo.t",
        )
        assert not result.valid
        assert result.code == code

    @pytest.mark.parametrize("aggregate", ["MIN", "MAX", "SUM", "AVG"])
    def test_custom_sql_rejects_unbounded_or_nonnumeric_string_aggregates(
        self, aggregate,
    ):
        result = validate_custom_rule({
            "rule_type": "custom_sql",
            "sql": f"SELECT {aggregate}(client) FROM demo.t",
        }, _SCHEMA, table_fqn="demo.t")
        assert not result.valid
        assert result.code == "non_numeric_aggregate"

    def test_custom_sql_text_size_is_bounded_before_parsing(self):
        result = validate_custom_rule({
            "rule_type": "custom_sql",
            "sql": "SELECT COUNT(*) FROM demo.t -- " + ("padding" * 5000),
        }, _SCHEMA, table_fqn="demo.t")
        assert not result.valid
        assert result.code == "query_size"

    @pytest.mark.parametrize(
        ("sql", "code"),
        [
            (
                "SELECT LENGTH(REPEAT('x', 1000000000)) FROM demo.t",
                "unsupported_function",
            ),
            (
                "SELECT COUNT(*) + LENGTH(REPEAT('x', 1000000000)) FROM demo.t",
                "unsupported_function",
            ),
            (
                "SELECT COUNT(*) FROM demo.t WHERE HASH(client) = 1",
                "unsupported_function",
            ),
            ("SELECT STRING_AGG(client, ',') FROM demo.t", "unsupported_aggregate"),
            ("SELECT SUM(amount * amount) FROM demo.t", "aggregate_argument"),
            ("SELECT COUNT(DISTINCT amount) FROM demo.t", "aggregate_argument"),
            ("SELECT amount FROM demo.t", "non_aggregate_projection"),
            ("SELECT 1 FROM demo.t", "aggregate_query_required"),
            ("SELECT COUNT(*) FROM demo.t GROUP BY amount", "grouped_query"),
            ("SELECT COUNT(*) OVER () FROM demo.t", "window_query"),
            (
                "SELECT COUNT(*) FROM (SELECT * FROM demo.t) source",
                "subquery_query",
            ),
            (
                "SELECT COUNT(*) FROM demo.t UNION SELECT COUNT(*) FROM demo.t",
                "set_operation_query",
            ),
            ("SELECT COUNT(*) FROM demo.t ORDER BY COUNT(*)", "ordered_query"),
            ("SELECT COUNT(*) FROM demo.t LIMIT 1", "limited_query"),
            (
                "SELECT COUNT(*) FROM demo.t HAVING COUNT(*) > 0",
                "output_cardinality",
            ),
        ],
    )
    def test_custom_sql_rejects_unbounded_or_multirow_shapes(self, sql, code):
        result = validate_custom_rule(
            {"rule_type": "custom_sql", "sql": sql},
            _SCHEMA,
            table_fqn="demo.t",
        )
        assert not result.valid
        assert result.code == code

    def test_expected_string_values_are_sql_escaped(self):
        sql = build_custom_rule_sql({
            "rule_type": "distinct_in",
            "column_name": "client",
            "expected_values": ["O'Reilly", "0", "false"],
        }, "demo.t", _SCHEMA)
        assert "'O''Reilly'" in sql
        assert "'0'" in sql
        assert "'false'" in sql
        assert 'COUNT(DISTINCT "client") AS unexpected_count' in sql
        assert "SELECT DISTINCT" not in sql

    @pytest.mark.parametrize(
        ("column", "col_type", "expected_values", "valid"),
        [
            ("value", "BIGINT", [0, 1.5], True),
            ("value", "BIGINT", ["1"], False),
            ("value", "BOOLEAN", [True, False], True),
            ("value", "BOOLEAN", [0, 1], False),
            ("value", "VARCHAR", ["0", "false"], True),
            ("value", "VARCHAR", [0, False], False),
            ("value", "DATE", ["2026-08-15"], True),
            ("value", "DATE", ["not-a-date"], False),
            ("value", "TIMESTAMP", ["2026-08-15T12:00:00Z"], True),
            ("value", "TIMESTAMP", ["2026-08-15"], True),
        ],
    )
    def test_distinct_in_literals_are_certified_against_runtime_column_type(
        self, column, col_type, expected_values, valid,
    ):
        result = validate_custom_rule({
            "rule_type": "distinct_in",
            "column_name": column,
            "expected_values": expected_values,
        }, [(column, col_type)])
        assert result.valid is valid
        if not valid:
            assert result.code == "expected_value_type"

    def test_distinct_in_expected_values_are_bounded_before_sql_generation(self):
        too_many = validate_custom_rule({
            "rule_type": "distinct_in",
            "column_name": "client",
            "expected_values": list(range(257)),
        }, _SCHEMA)
        assert not too_many.valid
        assert too_many.code == "expected_values_count"
        assert build_custom_rule_sql({
            "rule_type": "distinct_in",
            "column_name": "client",
            "expected_values": list(range(257)),
        }, "demo.t", _SCHEMA) is None

        too_large = validate_custom_rule({
            "rule_type": "distinct_in",
            "column_name": "client",
            "expected_values": ["x" * (16 * 1024)],
        }, _SCHEMA)
        assert not too_large.valid
        assert too_large.code == "expected_values_size"
        assert build_custom_rule_sql({
            "rule_type": "distinct_in",
            "column_name": "client",
            "expected_values": ["x" * (16 * 1024)],
        }, "demo.t", _SCHEMA) is None

    def test_empty_or_malformed_aggregate_result_is_never_a_false_pass(self):
        rule = {"rule_type": "column_min", "threshold": 0}
        assert evaluate_custom_rule(rule, [])["status"] == "error"
        assert not evaluate_custom_rule(rule, [])["evaluated"]
        assert evaluate_custom_rule(rule, [{"other": 0}])["status"] == "error"

    @pytest.mark.parametrize(
        ("rule", "field"),
        [
            ({"rule_type": "column_min", "threshold": 0}, "violations"),
            ({"rule_type": "column_max", "threshold": 0}, "violations"),
            ({"rule_type": "null_rate_max", "threshold": 5}, "null_rate"),
            ({"rule_type": "row_count_min", "threshold": 1}, "row_count"),
            ({"rule_type": "distinct_in"}, "unexpected_count"),
            ({"rule_type": "custom_sql", "threshold": 10}, "violations"),
        ],
    )
    def test_every_custom_evaluator_rejects_multirow_results(self, rule, field):
        result = evaluate_custom_rule(rule, [{field: 0}, {field: 0}])
        assert result["status"] == "error"
        assert result["evaluated"] is False

    def test_distinct_in_evaluates_the_exact_bounded_aggregate(self):
        rule = {"rule_type": "distinct_in", "severity": "warning"}
        result = evaluate_custom_rule(rule, [{"unexpected_count": 2}])
        assert result["status"] == "warning"
        assert result["value"] == 2
        assert "2 unexpected distinct values" in result["detail"]
        assert evaluate_custom_rule(
            rule, [{"unexpected_count": 0}],
        )["status"] == "ok"

    def test_custom_sql_evaluator_reads_only_named_violations_alias(self):
        rule = {"rule_type": "custom_sql", "threshold": 1, "severity": "warning"}
        assert evaluate_custom_rule(
            rule, [{"Violations": 2}],
        )["status"] == "warning"
        for result in ([{"other": 0}], [{"violations": "not numeric"}]):
            outcome = evaluate_custom_rule(rule, result)
            assert outcome["status"] == "error"
            assert outcome["evaluated"] is False

    @pytest.mark.parametrize("bad_result", [
        [],
        [{"unexpected_count": 1}, {"unexpected_count": 2}],
        [{"unexpected_value": "old-shape"}],
        [{"unexpected_count": -1}],
        [{"unexpected_count": 1.5}],
        [{"unexpected_count": float("nan")}],
    ])
    def test_distinct_in_rejects_malformed_or_non_count_results(self, bad_result):
        evaluated = evaluate_custom_rule(
            {"rule_type": "distinct_in", "severity": "warning"},
            bad_result,
        )
        assert evaluated["status"] == "error"
        assert evaluated["evaluated"] is False

    def test_empty_table_null_rate_has_defined_zero_percent_semantics(self):
        rule = {
            "rule_type": "null_rate_max",
            "column_name": "client",
            "threshold": 5,
        }
        sql = build_custom_rule_sql(rule, "demo.t", _SCHEMA)
        assert "COALESCE" in sql
        result = evaluate_custom_rule(rule, [{"null_rate": 0.0}])
        assert result["status"] == "ok"
        assert result["evaluated"] is True
