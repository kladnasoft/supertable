"""DuckDB-oracle regressions for join-edge pruning directionality.

Each synthetic data row is one physical file with an exact signed-bigint zone
map.  Queries run once against every file and once against the parser/kernel
survivors; the result multisets must be identical.  This exercises relational
join preservation semantics without duplicating them in a Python oracle.
"""

from __future__ import annotations

from collections import Counter
import random

import duckdb
import polars
import pytest

from supertable.engine.join_pruner import plan_file_pruning_for_query
from supertable.processing import STATS_SCHEMA


SUPER = "s"
TABLES = ("a", "b", "c")


def _stats_row(file_path: str, key: int, column: str = "k") -> dict:
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "row_group_id": 0,
        "column_name": column,
        "physical_type": "INT64",
        "logical_type": "",
        "min_bigint": key,
        "max_bigint": key,
        "null_count": 0,
        "row_group_rows": 1,
        "compressed_bytes": 10,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    return row


def _random_world(seed: int):
    rng = random.Random(seed)
    rows = {
        table: [
            (rng.randrange(-2, 5), f"{table}/f{index}")
            for index in range(5)
        ]
        for table in TABLES
    }
    files = {
        (SUPER, table): [file_path for _key, file_path in table_rows]
        for table, table_rows in rows.items()
    }
    stats = {
        (SUPER, table): polars.DataFrame(
            [_stats_row(file_path, key) for key, file_path in table_rows],
            schema=STATS_SCHEMA,
        )
        for table, table_rows in rows.items()
    }
    return rows, files, stats


def _install_world(con, rows) -> None:
    con.execute("DROP SCHEMA IF EXISTS s CASCADE")
    con.execute("DROP SCHEMA IF EXISTS p CASCADE")
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE SCHEMA p")
    for table in TABLES:
        con.execute(f"CREATE TABLE s.{table}(k BIGINT, fp VARCHAR)")
        con.executemany(f"INSERT INTO s.{table} VALUES (?, ?)", rows[table])


def _install_pruned_views(con, survivors) -> None:
    for table in TABLES:
        allowed = survivors[(SUPER, table)]
        predicate = (
            "fp IN (" + ", ".join(
                "'" + file_path.replace("'", "''") + "'"
                for file_path in allowed
            ) + ")"
            if allowed
            else "FALSE"
        )
        con.execute(
            f"CREATE OR REPLACE VIEW p.{table} AS "
            f"SELECT * FROM s.{table} WHERE {predicate}"
        )


def _result_multiset(con, query: str) -> Counter:
    return Counter(con.execute(query).fetchall())


JOIN_SHAPES = [
    (
        "flat-inner",
        "SELECT * FROM s.a a INNER JOIN s.b b ON a.k=b.k "
        "INNER JOIN s.c c ON b.k=c.k",
    ),
    (
        "flat-left",
        "SELECT * FROM s.a a LEFT JOIN s.b b ON a.k=b.k "
        "LEFT JOIN s.c c ON b.k=c.k",
    ),
    (
        "flat-right",
        "SELECT * FROM s.a a RIGHT JOIN s.b b ON a.k=b.k "
        "RIGHT JOIN s.c c ON b.k=c.k",
    ),
    (
        "flat-full",
        "SELECT * FROM s.a a FULL OUTER JOIN s.b b ON a.k=b.k "
        "FULL OUTER JOIN s.c c ON b.k=c.k",
    ),
    (
        "flat-semi",
        "SELECT * FROM s.a a SEMI JOIN s.b b ON a.k=b.k "
        "SEMI JOIN s.c c ON a.k=c.k",
    ),
    (
        "flat-anti",
        "SELECT * FROM s.a a ANTI JOIN s.b b ON a.k=b.k "
        "ANTI JOIN s.c c ON a.k=c.k",
    ),
]

for _name, _operator in [
    ("inner", "INNER JOIN"),
    ("left", "LEFT JOIN"),
    ("right", "RIGHT JOIN"),
    ("full", "FULL OUTER JOIN"),
    ("semi", "SEMI JOIN"),
    ("anti", "ANTI JOIN"),
]:
    JOIN_SHAPES.append((
        f"nested-rhs-{_name}",
        f"SELECT * FROM s.a a {_operator} "
        "(s.b b INNER JOIN s.c c ON b.k=c.k) ON a.k=b.k",
    ))
    JOIN_SHAPES.append((
        f"nested-lhs-{_name}",
        "SELECT * FROM (s.a a INNER JOIN s.b b ON a.k=b.k) "
        f"{_operator} s.c c ON b.k=c.k",
    ))

JOIN_SHAPES.extend([
    (
        "comma",
        "SELECT * FROM s.a a, s.b b, s.c c "
        "WHERE a.k=b.k AND b.k=c.k",
    ),
    (
        "outer-then-where-equality",
        "SELECT * FROM s.a a FULL OUTER JOIN s.b b ON a.k=b.k, s.c c "
        "WHERE b.k=c.k",
    ),
    (
        "left-then-inner",
        # Chained USING requires schema-aware accumulated-left binding and is
        # intentionally rejected at the parser boundary.  Keep this oracle's
        # mixed-direction coverage with the unambiguous explicit equivalent.
        "SELECT * FROM s.a a LEFT JOIN s.b b ON a.k=b.k "
        "INNER JOIN s.c c ON a.k=c.k",
    ),
    (
        "right-then-inner",
        "SELECT * FROM s.a a RIGHT JOIN s.b b ON a.k=b.k "
        "INNER JOIN s.c c ON b.k=c.k",
    ),
    (
        "derived-table",
        "SELECT * FROM s.a a JOIN "
        "(SELECT b.k, b.fp AS bfp, c.fp AS cfp "
        " FROM s.b b JOIN s.c c ON b.k=c.k) q ON a.k=q.k",
    ),
    (
        "cte",
        "WITH q AS (SELECT b.k, b.fp AS bfp, c.fp AS cfp "
        " FROM s.b b JOIN s.c c ON b.k=c.k) "
        "SELECT * FROM s.a a JOIN q ON a.k=q.k",
    ),
    (
        "self-alias",
        "SELECT * FROM s.a a1 JOIN s.a a2 ON a1.k=a2.k "
        "JOIN s.b b ON a1.k=b.k, s.c c WHERE b.k=c.k",
    ),
])


@pytest.mark.parametrize("seed", [731, 1907])
def test_join_pruning_matches_duckdb_across_three_table_shapes(seed: int):
    rows, files, stats = _random_world(seed)
    con = duckdb.connect()
    _install_world(con, rows)

    for shape, query in JOIN_SHAPES:
        expected = _result_multiset(con, query)
        plan = plan_file_pruning_for_query(
            SUPER,
            query,
            "duckdb",
            files,
            stats,
            allow_empty=False,
        )
        _install_pruned_views(con, plan.survivors)
        actual = _result_multiset(con, query.replace("s.", "p."))
        assert actual == expected, (
            f"seed={seed}, shape={shape}, query={query}, "
            f"survivors={plan.survivors}, "
            f"missing={expected - actual}, extra={actual - expected}"
        )


def test_repeated_table_inside_aliased_join_group_keeps_all_occurrences():
    """A grouped table occurrence shares the same physical read as its peer.

    sqlglot scopes an aliased ``(b JOIN c) x`` separately with a non-SELECT
    expression.  Missing that scope in the occurrence census lets ``b2=d``
    prune B globally to key 1, deleting B(key=100) even though it contributes
    through the grouped occurrence.
    """
    b_key = (SUPER, "b")
    c_key = (SUPER, "c")
    d_key = (SUPER, "d")
    table_rows = {
        "b": [(1, 10, "b/low"), (100, 20, "b/group-contributor")],
        "c": [(10, 0, "c/low"), (20, 0, "c/high")],
        "d": [(1, 0, "d/driver")],
    }
    table_files = {
        b_key: [row[2] for row in table_rows["b"]],
        c_key: [row[2] for row in table_rows["c"]],
        d_key: [row[2] for row in table_rows["d"]],
    }
    table_stats = {
        key: polars.DataFrame(
            [_stats_row(file_path, value) for value, _z, file_path in table_rows[name]],
            schema=STATS_SCHEMA,
        )
        for key, name in [(b_key, "b"), (c_key, "c"), (d_key, "d")]
    }
    query = (
        "SELECT b2.fp AS driver_fp, x.fp AS grouped_b_fp "
        "FROM s.b b2 JOIN s.d d ON b2.k=d.k "
        "CROSS JOIN (s.b b JOIN s.c c ON b.z=c.k) x"
    )

    plan = plan_file_pruning_for_query(
        SUPER,
        query,
        "duckdb",
        table_files,
        table_stats,
        allow_empty=False,
    )
    assert plan.survivors[b_key] == table_files[b_key]

    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE SCHEMA p")
    for table, rows in table_rows.items():
        con.execute(f"CREATE TABLE s.{table}(k BIGINT, z BIGINT, fp VARCHAR)")
        con.executemany(f"INSERT INTO s.{table} VALUES (?, ?, ?)", rows)
        allowed = plan.survivors[(SUPER, table)]
        allowed_sql = ", ".join(f"'{value}'" for value in allowed)
        con.execute(
            f"CREATE VIEW p.{table} AS SELECT * FROM s.{table} "
            f"WHERE fp IN ({allowed_sql})"
        )

    expected = _result_multiset(con, query)
    actual = _result_multiset(con, query.replace("s.", "p."))
    assert actual == expected
    assert ("b/low", "b/group-contributor") in actual


def test_where_filter_cannot_starve_repeated_table_in_aliased_join_group():
    """Predicate occurrence census includes non-SELECT joined-table scopes."""
    b_key = (SUPER, "b")
    c_key = (SUPER, "c")
    table_rows = {
        "b": [(1, "b/filtered-alias"), (100, "b/group-contributor")],
        "c": [(1, "c/low"), (100, "c/high")],
    }
    table_files = {
        b_key: [file_path for _key, file_path in table_rows["b"]],
        c_key: [file_path for _key, file_path in table_rows["c"]],
    }
    table_stats = {
        b_key: polars.DataFrame(
            [_stats_row(file_path, key) for key, file_path in table_rows["b"]],
            schema=STATS_SCHEMA,
        ),
        c_key: polars.DataFrame(
            [_stats_row(file_path, key) for key, file_path in table_rows["c"]],
            schema=STATS_SCHEMA,
        ),
    }
    query = (
        "SELECT b1.fp, x.fp "
        "FROM s.b b1 CROSS JOIN "
        "(s.b b2 JOIN s.c c ON b2.k=c.k) x "
        "WHERE b1.k=1"
    )

    plan = plan_file_pruning_for_query(
        SUPER, query, "duckdb", table_files, table_stats, allow_empty=False,
    )
    assert plan.survivors[b_key] == table_files[b_key]

    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE SCHEMA p")
    for table, rows in table_rows.items():
        con.execute(f"CREATE TABLE s.{table}(k BIGINT, fp VARCHAR)")
        con.executemany(f"INSERT INTO s.{table} VALUES (?, ?)", rows)
        allowed = plan.survivors[(SUPER, table)]
        allowed_sql = ", ".join(f"'{value}'" for value in allowed)
        con.execute(
            f"CREATE VIEW p.{table} AS SELECT * FROM s.{table} "
            f"WHERE fp IN ({allowed_sql})"
        )

    expected = _result_multiset(con, query)
    actual = _result_multiset(con, query.replace("s.", "p."))
    assert actual == expected
    assert ("b/filtered-alias", "b/group-contributor") in actual
