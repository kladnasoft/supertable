"""A row predicate that is not the caller's SQL is admitted before it is used.

A linked share's ``_row_filter`` is text the reader writes into the ``WHERE``
of a view. The read gate judges the caller's statement and never saw it, so it
ran as it came: a second statement in it was executed, a parenthesis in it
closed the one it was merged into, and a subquery in it read whatever stood on
the reader's connection.

``admit_row_predicate`` is what stands in front of it now. Three things are
pinned here:

  * what is refused, by each of the two readings that judge it;
  * that what is admitted comes back UNCHANGED, so a predicate means what it
    meant before the check existed (``~`` is the case that shows why the text
    is not rewritten);
  * that a refusal never quotes the predicate, and that an admitted predicate
    is parsed once.

``tests/characterization/test_share_row_filter_admission_end_to_end.py`` reads
a real linked share through the real reader.
"""

from __future__ import annotations

import duckdb
import pytest

from supertable import system_query
from supertable.data_classes import RbacViewDef
from supertable.engine.engine_common import create_rbac_view
from supertable.system_query import admit_row_predicate

# ---------------------------------------------------------------------------
# What a provider writes
# ---------------------------------------------------------------------------

ADMITTED = [
    # comparisons and boolean logic
    "region = 'eu'", "region <> 'eu'", "region != 'eu'", "amount > 100",
    "amount >= 100.5 AND flag", "NOT flag", "true", "1 = 1",
    "(region = 'eu' OR region = 'us') AND amount > 10",
    "region = 'eu' OR region = 'us' AND amount > 50",
    "NOT (region = 'eu' OR amount < 0)", "id > 0 AND (flag OR amount IS NULL)",
    # membership, ranges, nulls
    "region IN ('eu', 'us')", "region NOT IN ('eu', 'us')",
    "id BETWEEN 2 AND 4", "id NOT BETWEEN 2 AND 4",
    "region IS NULL", "region IS NOT NULL", "flag IS TRUE", "flag IS NOT TRUE",
    "amount IS DISTINCT FROM 20.0", "region IS NOT DISTINCT FROM NULL",
    "id = ANY([1, 3])", "id > ALL([0, 1])", "id IN (VALUES (1), (2))",
    "(id, region) = (1, 'eu')", "(id, region) IN ((1, 'eu'), (2, 'us'))",
    # text
    "name LIKE 'A%'", "name NOT LIKE '%e'", "name ILIKE 'a%'", "name NOT ILIKE 'a%'",
    "code LIKE 'a\\_b' ESCAPE '\\'", "name SIMILAR TO '[A-D].*'", "name GLOB 'A*'",
    "name ~ '^[A-Z]'", "name ~~ 'A%'", "name !~~ 'A%'", "name ^@ 'A'",
    "regexp_matches(name, '^[a-z]+$')", "regexp_full_match(name, '[a-z]+')",
    "lower(region) = 'eu'", "upper(name) = 'BOB'", "length(name) > 3",
    "starts_with(name, 'A')", "contains(name, 'o')", "name || '_x' = 'bob_x'",
    "concat(name, region) = 'bobus'", "name = 'O''Brien'", '"region" = \'eu\'',
    "REGION = 'eu'", "name COLLATE NOCASE = 'alice'", "name[1:2] = 'Al'",
    # numbers
    "amount * 2 + 1 > 200", "amount / 2 < 20", "id % 2 = 0", "abs(amount) > 10",
    "round(amount) = 101", "n // 2 = 3", "n ** 2 = 36", "n & 2 = 2", "~n = -7",
    "coalesce(amount, 0) > 50", "greatest(id, 3) = 3", "nullif(region, 'eu') IS NULL",
    "CASE WHEN region = 'eu' THEN amount > 50 ELSE FALSE END",
    "CAST(id AS VARCHAR) = '3'", "id::VARCHAR = '3'", "TRY_CAST(code AS INTEGER) IS NULL",
    "amount = 1e2 + 0.5",
    # dates and times
    "d >= DATE '2024-01-01'", "d < '2024-03-01'",
    "ts >= TIMESTAMP '2024-02-01 00:00:00'",
    "d BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'", "ts > d",
    "d + INTERVAL 1 MONTH > DATE '2024-03-01'",
    "date_diff('day', d, DATE '2024-03-15') < 30", "date_part('year', d) = 2024",
    "extract(month FROM d) = 3", "year(d) = 2024",
    "strftime(d, '%Y-%m') = '2024-01'", "date_trunc('month', d) = DATE '2024-01-01'",
    "ts::DATE = d", "ts < now()", "d < current_date", "epoch(ts) > 1700000000",
    # nested values
    "list_contains(tags, 3)", "len(tags) > 1", "tags[1] = 1", "tags = [3]",
    "list_filter(tags, x -> x > 1) = [2]", "st.k = 2", "st = {'k': 2}",
    # a comment inside the expression is part of neither reading
    "region = 'eu' /* europe */ AND amount > 0",
]

#: text -> why it is refused.  ``one`` is "not a single SQL expression",
#: ``elsewhere`` is "reads something other than the row", ``function`` names a
#: function the read gate refuses a caller as well, ``unreadable`` is text one
#: of the two parsers cannot read at all.
REFUSED = {
    # a second statement
    "1=1; CREATE TABLE planted AS SELECT 1": "one",
    "1=1; ATTACH 'x.db'": "one",
    "1=1 --\n; DROP TABLE t": "one",
    "1=1 /* */ ; COPY t TO 'x'": "one",
    "region = 'eu';": "one",
    # text that closes the parenthesis it is written into
    "1=1) OR (1=1": "unreadable",
    ") OR (1=1": "unreadable",
    "TRUE) OR __st_row_predicate__(TRUE": "unreadable",
    # a clause, a set operation, a second argument
    "1=1 UNION SELECT 1": "one",
    "1=1 GROUP BY id": "one",
    "1=1 ORDER BY id": "one",
    "1=1 LIMIT 1": "one",
    "1=1 HAVING 1=1": "one",
    "1=1 QUALIFY row_number() OVER () = 1": "one",
    "1=1 WINDOW w AS (PARTITION BY id)": "one",
    "1=1, 2=2": "unreadable",
    "DISTINCT id": "unreadable",
    # a line comment would swallow whatever is written after the text
    "region = 'eu' -- europe": "one",
    # another relation
    "id IN (SELECT id FROM t)": "elsewhere",
    "EXISTS (SELECT 1 FROM t)": "elsewhere",
    "id = (SELECT max(id) FROM t)": "elsewhere",
    "id = (SELECT 1)": "elsewhere",
    "lower(name) IN (SELECT 'x')": "elsewhere",
    "id IN (SELECT unnest([1, 2]) FROM other_view)": "elsewhere",
    "(SELECT count(*) FROM duckdb_views()) > 0": "function",
    "id IN (SELECT * FROM read_text('/etc/hostname'))": "function",
    # functions that read the engine, a file or the JVM
    "current_setting('threads') = '1'": "function",
    "getenv('HOME') IS NOT NULL": "function",
    "read_text('/etc/hostname') IS NOT NULL": "function",
    "glob('/etc/*') IS NOT NULL": "function",
    "reflect('java.lang.System', 'getenv', 'X') = 'y'": "function",
    "\"current_setting\"('threads') = '1'": "function",
    # not SQL
    "WHERE region = 'eu'": "unreadable",
    "region = 'eu' /* unterminated": "unreadable",
    "region = 'eu": "unreadable",
    "1=1 OR": "unreadable",
    "1=1 FROM t": "unreadable",
    "1=1 INTO OUTFILE 'x'": "unreadable",
    # valid for DuckDB alone; the read path's parser has to read it as well
    "name !~ '^[A-Z]'": "unreadable",
}

_MESSAGE = {
    "one": "not a single SQL expression",
    "elsewhere": "subqueries are not permitted",
    "function": "not permitted on the read path",
}


@pytest.fixture(autouse=True)
def _nothing_remembered():
    system_query._admit_row_predicate.cache_clear()
    yield
    system_query._admit_row_predicate.cache_clear()


class TestWhatIsAdmitted:

    @pytest.mark.parametrize("predicate", ADMITTED)
    def test_it_comes_back_unchanged(self, predicate):
        assert admit_row_predicate(predicate) == predicate

    @pytest.mark.parametrize("predicate", ADMITTED)
    def test_each_reading_admits_it_on_its_own(self, predicate):
        system_query._judge_row_predicate_as_sqlglot_reads_it(predicate)
        system_query._judge_row_predicate_as_duckdb_reads_it(predicate)

    def test_surrounding_whitespace_is_kept_as_written(self):
        assert admit_row_predicate("  region = 'eu'\n") == "  region = 'eu'\n"


class TestWhatIsRefused:

    @pytest.mark.parametrize("predicate", list(REFUSED))
    def test_it_is_refused(self, predicate):
        with pytest.raises(ValueError) as refused:
            admit_row_predicate(predicate)
        why = REFUSED[predicate]
        if why in _MESSAGE:
            assert _MESSAGE[why] in str(refused.value)

    @pytest.mark.parametrize("value", [None, "", "   ", "\n\t", 5, 0, True, False,
                                       {"region": "eu"}, {}, ["region = 'eu'"], [],
                                       ("a",), b"region = 'eu'", 1.5, object()])
    def test_what_is_not_text_is_not_a_predicate(self, value):
        with pytest.raises(ValueError, match="non-empty SQL expression"):
            admit_row_predicate(value)

    @pytest.mark.parametrize("predicate", [
        "1=1; CREATE TABLE planted AS SELECT 1", "1=1 UNION SELECT 1",
        "1=1 GROUP BY id", "1=1) OR (1=1", "id IN (SELECT id FROM t)",
        "current_setting('threads') = '1'", "region = 'eu' -- europe",
    ])
    def test_duckdb_s_reading_refuses_it_by_itself(self, predicate):
        """The text runs on DuckDB as it is, so DuckDB's own parse has to
        refuse it, whatever SQLGlot makes of it."""
        with pytest.raises(ValueError):
            system_query._judge_row_predicate_as_duckdb_reads_it(predicate)

    @pytest.mark.parametrize("predicate", [
        "1=1; CREATE TABLE planted AS SELECT 1", "1=1 UNION SELECT 1",
        "1=1 GROUP BY id", "1=1) OR (1=1", "id IN (SELECT id FROM t)",
        "current_setting('threads') = '1'", "reflect('a', 'b') = 'c'",
    ])
    def test_sqlglot_s_reading_refuses_it_by_itself(self, predicate):
        """Spark runs SQLGlot's writing of the text, so that reading has to
        refuse it, whatever DuckDB makes of it."""
        with pytest.raises(ValueError):
            system_query._judge_row_predicate_as_sqlglot_reads_it(predicate)

    def test_a_subquery_over_a_table_function_is_refused_by_duckdb_s_reading_too(self):
        with pytest.raises(ValueError):
            system_query._judge_row_predicate_as_duckdb_reads_it(
                "id IN (SELECT * FROM read_text('/etc/hostname'))")

    def test_a_select_from_nothing_may_still_not_call_a_refused_function(self):
        with pytest.raises(ValueError, match="not permitted on the read path"):
            system_query._judge_row_predicate_as_duckdb_reads_it(
                "id = ANY([length(getenv('HOME'))])")


class TestARefusalDoesNotQuoteThePredicate:
    """The predicate is the provider's policy. Whoever is refused the read is
    the consumer, and the message reaches them."""

    @pytest.mark.parametrize("predicate", [
        "customer_tier = 'PLATINUM-ONLY' AND (",
        "secret_margin_pct > 0.37 GROUP BY secret_margin_pct",
        "internal_flag = 'XJ9-INTERNAL'; DROP TABLE t",
        "internal_flag = 'XJ9-INTERNAL' -- note",
        "id IN (SELECT id FROM vip_customers_2024)",
        "internal_flag = 'XJ9-INTERNAL",
    ])
    def test_no_identifier_or_literal_of_it_is_in_the_message(self, predicate):
        with pytest.raises(ValueError) as refused:
            admit_row_predicate(predicate)
        message = str(refused.value)
        for fragment in ("customer_tier", "PLATINUM", "secret_margin_pct", "0.37",
                         "internal_flag", "XJ9", "vip_customers_2024"):
            assert fragment not in message
        assert refused.value.__cause__ is None
        assert refused.value.__suppress_context__ or refused.value.__context__ is None

    def test_a_refused_function_is_named_and_nothing_else_is(self):
        with pytest.raises(ValueError) as refused:
            admit_row_predicate("getenv(secret_column_name) = 'XJ9'")
        assert "getenv()" in str(refused.value)
        assert "secret_column_name" not in str(refused.value)
        assert "XJ9" not in str(refused.value)


class TestAnAdmittedPredicateIsParsedOnce:

    def test_the_second_admission_parses_nothing(self, monkeypatch):
        parses = []
        real_sqlglot = system_query._judge_row_predicate_as_sqlglot_reads_it
        real_duckdb = system_query._judge_row_predicate_as_duckdb_reads_it
        monkeypatch.setattr(
            system_query, "_judge_row_predicate_as_sqlglot_reads_it",
            lambda text: (parses.append("sqlglot"), real_sqlglot(text))[1])
        monkeypatch.setattr(
            system_query, "_judge_row_predicate_as_duckdb_reads_it",
            lambda text: (parses.append("duckdb"), real_duckdb(text))[1])

        for _ in range(50):
            assert admit_row_predicate("region = 'eu'") == "region = 'eu'"

        assert parses == ["sqlglot", "duckdb"]

    def test_a_refused_predicate_is_not_remembered_as_admitted(self):
        for _ in range(3):
            with pytest.raises(ValueError):
                admit_row_predicate("1=1; DROP TABLE t")
        assert system_query._admit_row_predicate.cache_info().currsize == 0

    def test_two_predicates_are_remembered_apart(self):
        assert admit_row_predicate("a = 1") == "a = 1"
        assert admit_row_predicate("a = 2") == "a = 2"
        assert system_query._admit_row_predicate.cache_info().currsize == 2


class TestDuckDBCannotBeAsked:

    def test_the_predicate_is_refused_when_duckdb_cannot_say_how_it_reads_it(self, monkeypatch):
        def broken(*_args, **_kwargs):
            raise duckdb.Error("connection failed for 'region = ''XJ9'''")

        monkeypatch.setattr(duckdb, "connect", broken)
        with pytest.raises(ValueError) as refused:
            admit_row_predicate("region = 'eu'")
        assert "could not be asked" in str(refused.value)
        assert "XJ9" not in str(refused.value)

    def test_a_parse_tree_it_does_not_recognise_is_refused(self, monkeypatch):
        """A DuckDB whose serialised tree has another shape is not guessed at."""
        monkeypatch.setattr(
            system_query, "_duckdb_reading",
            lambda con, sql: {"node": {"type": "SELECT_NODE", "where": "?"}})
        with pytest.raises(ValueError, match="not a single SQL expression"):
            admit_row_predicate("region = 'eu'")


# ---------------------------------------------------------------------------
# Why: what the text did when it was written into the view unread
# ---------------------------------------------------------------------------

@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE base AS SELECT * FROM (VALUES "
        "(1, 'eu', 'Alice', 10), (2, 'us', 'bob', 20), (3, 'eu', 'eve', 30)) "
        "v(id, region, name, amount)")
    yield connection
    connection.close()


def _ids(con, view="v"):
    return [row[0] for row in con.execute(f"SELECT id FROM {view} ORDER BY id").fetchall()]


class TestWhatTheTextDidUnread:
    """``create_rbac_view`` writes its ``where_clause`` into SQL as it is given.
    These show what three kinds of text do there, and that none of the three
    is admitted any more."""

    def test_a_second_statement_ran(self, con):
        hostile = "1=1; CREATE TABLE planted AS SELECT 42 AS x"
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], hostile))
        assert con.execute("SELECT x FROM planted").fetchall() == [(42,)]
        with pytest.raises(ValueError, match="not a single SQL expression"):
            admit_row_predicate(hostile)

    def test_a_parenthesis_undid_the_role_filter_beside_it(self, con):
        hostile = "1=1) OR (1=1"
        merged = f"(region = 'eu') AND ({hostile})"   # how the reader merges the two
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], merged))
        assert _ids(con) == [1, 2, 3]                 # the 'us' row came through
        with pytest.raises(ValueError):
            admit_row_predicate(hostile)

    def test_a_subquery_read_another_relation_on_the_connection(self, con):
        con.execute("CREATE TABLE another_tenant AS SELECT 2 AS id")
        hostile = "id IN (SELECT id FROM another_tenant)"
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], hostile))
        assert _ids(con) == [2]
        with pytest.raises(ValueError, match="subqueries are not permitted"):
            admit_row_predicate(hostile)


class TestAnAdmittedPredicateMeansWhatItMeant:

    @pytest.mark.parametrize("predicate, ids", [
        ("region = 'eu'", [1, 3]),
        ("region <> 'eu' OR amount >= 30", [2, 3]),
        ("name LIKE 'A%'", [1]),
        ("id = ANY([1, 3])", [1, 3]),
        ("id IN (VALUES (2), (3))", [2, 3]),
        ("lower(name) IN ('alice', 'bob') AND amount BETWEEN 5 AND 15", [1]),
    ])
    def test_the_view_filters_as_the_text_says(self, con, predicate, ids):
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], admit_row_predicate(predicate)))
        assert _ids(con) == ids

    def test_duckdb_s_tilde_still_matches_the_whole_string(self, con):
        """Why the admitted text is not SQLGlot's writing of it.

        DuckDB's ``~`` matches the whole string. SQLGlot writes it back out as
        ``REGEXP_MATCHES``, which matches any part of it: rewritten, this
        filter would have let two more rows through.
        """
        import sqlglot

        predicate = "name ~ '[a-z]+'"
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], admit_row_predicate(predicate)))
        assert _ids(con) == [2, 3]                       # 'bob', 'eve'; not 'Alice'

        rewritten = sqlglot.parse_one(
            f"SELECT 1 FROM base WHERE {predicate}", read="duckdb",
        ).args["where"].this.sql(dialect="duckdb")
        create_rbac_view(con, "base", "w", RbacViewDef(["*"], rewritten))
        assert _ids(con, "w") == [1, 2, 3]

    def test_merged_with_a_role_filter_it_narrows_and_never_widens(self, con):
        merged = f"(region = 'eu') AND ({admit_row_predicate('amount > 15 OR id = 2')})"
        create_rbac_view(con, "base", "v", RbacViewDef(["*"], merged))
        assert _ids(con) == [3]
