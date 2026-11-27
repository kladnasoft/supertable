"""
A small file that holds none of the incoming keys is not read.

With overwrite columns every file of a table is a candidate, and the stored
minimum and maximum rule out the files whose key range the incoming keys miss.
They cannot rule out the file a small upsert leaves behind: a hundred keys
drawn from the whole table have the table's minimum and maximum.  Every later
upsert read every such file to find nothing in it, until a merge collected
them: a 100-key upsert took 0.12 s with under twenty live files and 0.33 s
with eighty or more.

The process now keeps the distinct key values of small files (at most
``_KEY_VALUES_MAX_ROWS`` rows): from the frame a file was written from, when
the write had overwrite columns, or from the frame it was read into the first
time it had to be read.  A file none of whose values of some key column is
among the incoming ones is dropped from the candidates before anything opens
it (``drop_files_without_incoming_keys``, called by
``resolve_overwrite_writes``).

Integers are compared as integers and strings byte for byte, as the join
compares them; no other type is compared, and never one with the other.

A wrong drop loses a delete, so the contract is the one of the range test: a
file is dropped only when it is proven to hold none of the incoming keys.
Here that is checked against the join itself: over random tables and keys, no
dropped file has a row the semi join matches, the files dropped are exactly
the ones the rule names, and what a write resolves to is the same with the
kept values and without them.
"""
from __future__ import annotations

import dataclasses
import os
import random
import threading
import tracemalloc
from array import array
from typing import Dict, List, Tuple
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    ROWID_COL,
    TOMBSTONE_FILE_COL,
    drop_files_without_incoming_keys,
    identify_deleted_rowids,
    remember_file_key_values,
    resolve_overwrite_writes,
    write_parquet_and_collect_resources,
)
from supertable.storage.local_storage import LocalStorage  # noqa: E402
from supertable.utils.profiler import Profiler  # noqa: E402

KNOWN = proc._FILE_KEY_VALUES
MIB = 1024 * 1024

# What an entry is counted as (see the constants in processing.py).
ENTRY = proc._KEY_VALUES_ENTRY_BYTES
COLUMN = proc._KEY_VALUES_COLUMN_BYTES
NO_COLUMN = proc._KEY_VALUES_NO_COLUMN_BYTES
A_STRING = proc._KEY_VALUES_STRING_BYTES


def b(*texts: str) -> frozenset:
    """Strings as they are kept: their bytes, in a set."""
    return frozenset(t.encode() for t in texts)


def not_utf8(*values) -> pl.Series:
    """A string column holding *values* as they are: bytes that need not be UTF-8.

    Arrow does not check the strings it reads from a file, and polars takes
    them as they come."""
    column = pa.array(list(values), pa.binary()).view(pa.string())
    return pl.from_arrow(pa.table({"code": column})).get_column("code")

INT_TYPES = [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32]


@pytest.fixture(autouse=True)
def nothing_known():
    """Every test starts with a process that knows no file, and the default bound."""
    KNOWN.clear()
    with patch.object(KNOWN, "_cap_getter", lambda: 32 * MIB):
        yield
    KNOWN.clear()


def bound(nbytes: int):
    return patch.object(KNOWN, "_cap_getter", lambda: nbytes)


def ints(values, dtype=pl.Int64, name="id") -> pl.DataFrame:
    return pl.DataFrame({name: values}, schema={name: dtype})


def words(values, name="code") -> pl.DataFrame:
    return pl.DataFrame({name: values}, schema={name: pl.Utf8})


def known_file(path: str, frame: pl.DataFrame, columns, size: int = 100) -> Tuple[str, bool, int]:
    remember_file_key_values(path, size, frame, list(columns))
    return (path, True, size)


def dropped(candidates, incoming: pl.DataFrame, columns, profiler=None) -> List[str]:
    kept = drop_files_without_incoming_keys(
        candidates, incoming.select(columns).unique(), list(columns), profiler=profiler)
    left = {entry[0] for entry in kept}
    return sorted(entry[0] for entry in candidates if entry[0] not in left)


# ---------------------------------------------------------------------------
# 1. What is kept of a column
# ---------------------------------------------------------------------------

class TestTheValuesOfAColumn:

    @pytest.mark.parametrize("dtype", INT_TYPES)
    def test_integers_distinct_sorted_without_nulls(self, dtype):
        family, values, nbytes = proc._distinct_key_values(
            pl.Series("id", [7, 3, None, 7, 0, 3], dtype=dtype))
        assert family == "int"
        assert values == array("q", [0, 3, 7])
        assert nbytes == COLUMN + 8 * 3

    def test_negative_and_extreme_integers(self):
        lo, hi = -(2 ** 63), 2 ** 63 - 1
        _, values, _ = proc._distinct_key_values(pl.Series("id", [hi, -1, lo, 0], dtype=pl.Int64))
        assert list(values) == [lo, -1, 0, hi]

    def test_strings_distinct_without_nulls(self):
        family, values, nbytes = proc._distinct_key_values(
            pl.Series("code", ["b", None, "a", "b", "", "ä"], dtype=pl.Utf8))
        assert family == "str"
        assert values == b("", "a", "b", "ä") and type(values) is frozenset     # as bytes
        assert nbytes == COLUMN + A_STRING * 4 + (0 + 1 + 1 + 2)       # "ä" is two bytes

    def test_strings_are_not_folded_or_trimmed(self):
        _, values, _ = proc._distinct_key_values(
            pl.Series("code", ["a", "A", " a", "a ", "é", "é"], dtype=pl.Utf8))
        assert set(values) == set(b("a", "A", " a", "a ", "é", "é"))

    def test_strings_that_are_not_utf8_are_kept_as_the_bytes_they_are(self):
        """Turned into Python strings they would end the write: polars 1.40 panics
        on ``to_list`` with an exception that is not an ``Exception``."""
        column = not_utf8(b"a", b"m\xff", None, b"z", b"m\xff")
        family, values, nbytes = proc._distinct_key_values(column)
        assert family == "str" and values == frozenset((b"a", b"m\xff", b"z"))
        assert nbytes == COLUMN + A_STRING * 3 + 4

    def test_integers_come_out_sorted_whatever_their_order_and_number_of_chunks(self):
        """The search needs them sorted; the column is asked for its values, not for an order."""
        column = pl.concat([pl.Series("id", [9, 2, 2], dtype=pl.Int32),
                            pl.Series("id", [None, 5, -1], dtype=pl.Int32)], rechunk=False)
        assert column.n_chunks() == 2
        _, values, _ = proc._distinct_key_values(column)
        assert values == array("q", [-1, 2, 5, 9])
        with patch.object(pl.Series, "unique", side_effect=AssertionError("polars asked to de-duplicate")):
            _, values, _ = proc._distinct_key_values(pl.Series("id", [3, 1, 3]))
            assert values == array("q", [1, 3])
            _, values, _ = proc._distinct_key_values(pl.Series("code", ["b", "a", "b"]))
            assert values == b("a", "b")

    def test_a_column_of_nulls_has_no_values(self):
        family, values, _ = proc._distinct_key_values(pl.Series("id", [None, None], dtype=pl.Int64))
        assert family == "int" and len(values) == 0

    @pytest.mark.parametrize("column", [
        pl.Series("k", [1, 2], dtype=pl.UInt64),
        pl.Series("k", [1.0, 2.0], dtype=pl.Float64),
        pl.Series("k", [1.0, 2.0], dtype=pl.Float32),
        pl.Series("k", [True, False], dtype=pl.Boolean),
        pl.Series("k", ["a", "b"], dtype=pl.Categorical),
        pl.Series("k", [b"a", b"b"], dtype=pl.Binary),
        pl.Series("k", [1, 2], dtype=pl.Int64).cast(pl.Date),
        pl.Series("k", [1, 2], dtype=pl.Int64).cast(pl.Datetime("us")),
        pl.Series("k", [1, 2], dtype=pl.Int64).cast(pl.Decimal(10, 2)),
        pl.Series("k", [[1], [2]], dtype=pl.List(pl.Int64)),
    ], ids=lambda s: str(s.dtype))
    def test_other_types_are_not_kept(self, column):
        assert proc._distinct_key_values(column) is None

    def test_strings_over_the_bound_are_not_kept(self):
        with patch.object(proc, "_KEY_VALUES_MAX_STRING_BYTES", 10):
            assert proc._distinct_key_values(pl.Series("c", ["aaaaa", "bbbbb"])) is not None
            assert proc._distinct_key_values(pl.Series("c", ["aaaaa", None, "bbbbb"])) is not None
            assert proc._distinct_key_values(pl.Series("c", ["aaaaa", "bbbbbb"])) is None
            assert proc._distinct_key_values(pl.Series("c", ["ääääää"])) is None     # 12 bytes
            # the bound is on the bytes of the column, counted before any of
            # them is turned into a Python object
            assert proc._distinct_key_values(pl.Series("c", ["aaaaa"] * 3)) is None
            with patch.object(proc, "_key_values_as_python", side_effect=AssertionError("turned")):
                assert proc._distinct_key_values(pl.Series("c", ["a" * 11])) is None

    def test_the_bounds(self):
        assert proc._KEY_VALUES_MAX_ROWS == 4096
        assert proc._KEY_VALUES_MAX_STRING_BYTES == 256 * 1024
        assert proc._KEY_VALUES_MAX_INCOMING == 16384


# ---------------------------------------------------------------------------
# 2. What the process keeps
# ---------------------------------------------------------------------------

class TestWhatIsKept:

    def test_a_small_file_is_known_by_path_and_size(self):
        remember_file_key_values("a.parquet", 10, ints([5, 1, 5]), ["id"])
        assert KNOWN.get("a.parquet", 10) == {"id": ("int", array("q", [1, 5]))}
        assert KNOWN.get("a.parquet", 11) is None
        assert KNOWN.get("b.parquet", 10) is None

    def test_a_missing_size_is_zero(self):
        remember_file_key_values("a.parquet", None, ints([1]), ["id"])
        assert KNOWN.get("a.parquet", 0) is not None

    def test_a_file_of_more_rows_than_the_bound_is_not_kept(self):
        with patch.object(proc, "_KEY_VALUES_MAX_ROWS", 3):
            remember_file_key_values("three", 1, ints([1, 2, 3]), ["id"])
            remember_file_key_values("four", 1, ints([1, 1, 1, 1]), ["id"])
        assert KNOWN.get("three", 1) is not None
        assert KNOWN.get("four", 1) is None

    def test_a_column_the_frame_lacks_is_left_unknown(self):
        remember_file_key_values("a", 1, ints([1]), ["id", "other"])
        assert KNOWN.get("a", 1) == {"id": ("int", array("q", [1]))}
        assert KNOWN.unknown("a", 1, ["id", "other"]) == ["other"]

    def test_a_column_that_cannot_be_kept_is_remembered_as_such(self):
        frame = pl.DataFrame({"id": [1, 2], "amount": [1.5, 2.5]})
        remember_file_key_values("a", 1, frame, ["id", "amount"])
        assert KNOWN.get("a", 1) == {"id": ("int", array("q", [1, 2])), "amount": None}
        assert KNOWN.unknown("a", 1, ["id", "amount"]) == []

    def test_columns_are_added_to_a_known_file_and_never_replaced(self):
        frame = pl.DataFrame({"id": [1, 2], "code": ["x", "y"]})
        remember_file_key_values("a", 1, frame, ["id"])
        first = KNOWN.get("a", 1)
        remember_file_key_values("a", 1, frame, ["id", "code"])
        assert KNOWN.get("a", 1) == {"id": ("int", array("q", [1, 2])), "code": ("str", b("x", "y"))}
        assert first == {"id": ("int", array("q", [1, 2]))}       # what a reader held did not change
        # another frame under the same name and size changes nothing that is known
        remember_file_key_values("a", 1, pl.DataFrame({"id": [9], "code": ["z"]}), ["id", "code"])
        assert KNOWN.get("a", 1)["id"] == ("int", array("q", [1, 2]))

    def test_what_is_known_of_a_column_is_never_replaced(self):
        """Two threads may meet a file at once; the second answer changes nothing."""
        remember_file_key_values("a", 1, ints([1, 2]), ["id"])
        files, nbytes = KNOWN.stats()
        KNOWN.add("a", 1, {"id": (("int", array("q", [9])), 144)})
        assert KNOWN.get("a", 1) == {"id": ("int", array("q", [1, 2]))}
        assert KNOWN.stats() == (files, nbytes)
        # of two answers that come together, only the new one is taken, and counted
        KNOWN.add("a", 1, {"id": (None, 16), "code": (("str", b("x")), 178)})
        assert KNOWN.get("a", 1) == {"id": ("int", array("q", [1, 2])), "code": ("str", b("x"))}
        assert KNOWN.stats() == (files, nbytes + 178)

    def test_a_known_file_is_not_looked_at_again(self):
        remember_file_key_values("a", 1, ints([1]), ["id"])
        with patch.object(proc, "_distinct_key_values", side_effect=AssertionError("looked again")):
            remember_file_key_values("a", 1, ints([1]), ["id"])

    def test_only_a_polars_frame_is_kept(self):
        class Frame:
            """Looks like a frame, and would be kept were it taken for one."""
            height = 1
            columns = ["id"]

            def get_column(self, name):
                return pl.Series(name, [1])

        remember_file_key_values("a", 1, Frame(), ["id"])
        remember_file_key_values("b", 1, ints([1]).lazy(), ["id"])
        assert KNOWN.stats() == (0, 0)

    def test_nothing_without_a_file_or_columns(self):
        remember_file_key_values("", 1, ints([1]), ["id"])
        remember_file_key_values(None, 1, ints([1]), ["id"])
        remember_file_key_values("a", 1, ints([1]), [])
        assert KNOWN.stats() == (0, 0)

    @pytest.mark.parametrize("failure", [
        RuntimeError("boom"),
        pl.exceptions.PanicException("not an Exception: polars failed in its native code"),
    ], ids=["an exception", "a polars panic"])
    def test_keeping_never_raises(self, failure):
        with patch.object(proc, "_distinct_key_values", side_effect=failure):
            remember_file_key_values("a", 1, ints([1]), ["id"])
        assert KNOWN.get("a", 1) is None

    def test_a_polars_panic_is_caught_by_name_and_nothing_else_that_is_not_an_exception(self):
        assert proc._KEY_VALUES_FAILURES == (Exception, pl.exceptions.PanicException)
        with patch.object(proc, "_distinct_key_values", side_effect=KeyboardInterrupt()):
            with pytest.raises(KeyboardInterrupt):
                remember_file_key_values("a", 1, ints([1]), ["id"])

    def test_a_file_whose_strings_are_not_utf8_is_known_like_any_other(self):
        frame = pl.DataFrame({"code": not_utf8(b"a", b"m\xff", b"z")})
        remember_file_key_values("a", 1, frame, ["code"])
        assert KNOWN.get("a", 1) == {"code": ("str", frozenset((b"a", b"m\xff", b"z")))}

    def test_the_bytes_are_counted(self):
        remember_file_key_values("a", 1, ints(list(range(10))), ["id"])
        assert KNOWN.stats() == (1, ENTRY + 1 + COLUMN + 80)
        remember_file_key_values("bb", 1, words(["ab", "c"]), ["code"])
        assert KNOWN.stats() == (2, ENTRY + 1 + COLUMN + 80 + ENTRY + 2 + COLUMN + 2 * A_STRING + 3)
        remember_file_key_values("ccc", 1, pl.DataFrame({"amount": [1.5]}), ["amount"])
        assert KNOWN.stats()[1] == (ENTRY + 1 + COLUMN + 80 + ENTRY + 2 + COLUMN + 2 * A_STRING + 3
                                    + ENTRY + 3 + NO_COLUMN)
        KNOWN.clear()
        assert KNOWN.stats() == (0, 0)

    @pytest.mark.parametrize("kind", ["five integers", "a hundred integers", "a hundred strings",
                                      "a column that cannot be kept"])
    def test_what_is_counted_is_about_what_is_held(self, kind):
        """The bound is in bytes of the process, so an entry is counted as what it
        takes: its path, its place in the table and its values.  Measured here,
        and required to be no less than three quarters of it."""
        def frame(i: int) -> pl.DataFrame:
            if kind == "five integers":
                return ints([i, i + 1, i + 2, i + 3, i + 4])
            if kind == "a hundred integers":
                return ints(list(range(i, i + 100)))
            if kind == "a hundred strings":
                return words([f"{i:08d}-{j:04d}-0123456789012345678901234567" for j in range(100)])
            return pl.DataFrame({"id": [float(i)]})

        column = "code" if kind == "a hundred strings" else "id"
        frames = [frame(i) for i in range(300)]
        paths = ["organization/lake/tables/orders/data/year=2026/month=11/day=26/"
                 f"{1795726413476 + i}_{i:016x}_data.parquet" for i in range(300)]
        tracemalloc.start()
        try:
            before, _ = tracemalloc.get_traced_memory()
            for i in range(300):
                remember_file_key_values(paths[i], 1000 + i, frames[i], [column])
            held = tracemalloc.get_traced_memory()[0] - before
        finally:
            tracemalloc.stop()
        files, counted = KNOWN.stats()
        assert files == 300
        # the paths were allocated before the measurement began; they are held all the same
        held += sum(len(path) + 49 for path in paths)
        assert counted >= 0.75 * held, (counted, held)
        assert counted <= 1.5 * held, (counted, held)

    def test_the_least_recently_used_file_goes_first(self):
        one = ENTRY + 1 + COLUMN + 8 * 100
        with bound(3 * one):
            for name in ("a", "b", "c"):
                remember_file_key_values(name, 1, ints(list(range(100))), ["id"])
            assert KNOWN.stats() == (3, 3 * one)
            assert KNOWN.get("a", 1) is not None                 # a is used: b is now the oldest
            remember_file_key_values("d", 1, ints(list(range(100))), ["id"])
            assert KNOWN.get("b", 1) is None
            assert [KNOWN.get(n, 1) is not None for n in ("a", "c", "d")] == [True, True, True]
            assert KNOWN.stats() == (3, 3 * one)

    def test_a_file_larger_than_the_bound_is_not_kept_and_empties_nothing_else(self):
        with bound(ENTRY + 5 + COLUMN + 8 * 10):
            remember_file_key_values("small", 1, ints(list(range(10))), ["id"])
            remember_file_key_values("large", 1, ints(list(range(50))), ["id"])
            assert KNOWN.get("large", 1) is None
            assert KNOWN.get("small", 1) is not None
            assert KNOWN.stats() == (1, ENTRY + 5 + COLUMN + 8 * 10)

    @pytest.mark.parametrize("nbytes", [0, -1])
    def test_a_bound_of_zero_keeps_nothing_and_looks_at_nothing(self, nbytes):
        with bound(nbytes), patch.object(proc, "_distinct_key_values") as looked:
            remember_file_key_values("a", 1, ints([1]), ["id"])
            assert KNOWN.stats() == (0, 0)
            assert not looked.called

    def test_a_bound_that_cannot_be_read_keeps_nothing(self):
        def broken():
            raise RuntimeError("no settings")

        with patch.object(KNOWN, "_cap_getter", broken):
            assert KNOWN.cap() == 0
            remember_file_key_values("a", 1, ints([1]), ["id"])
            assert KNOWN.stats() == (0, 0)

    @pytest.mark.parametrize("value", [None, "1000000", 1.5e6, True, object()])
    def test_a_bound_that_is_not_an_integer_keeps_nothing(self, value):
        with patch.object(KNOWN, "_cap_getter", lambda: value):
            assert KNOWN.cap() == 0
            remember_file_key_values("a", 1, ints([1]), ["id"])
            assert KNOWN.stats() == (0, 0)

    def test_the_default_bound_is_the_setting(self):
        from supertable.config.settings import settings

        assert settings.SUPERTABLE_KEY_VALUES_CACHE_MAX_BYTES == 32 * MIB
        assert proc._key_values_cache_cap() == 32 * MIB

    def test_several_threads(self):
        errors: List[BaseException] = []

        def work(t: int) -> None:
            try:
                for i in range(300):
                    name = f"f{(t * 7 + i) % 40}"
                    remember_file_key_values(name, 1, ints([i % 5, t]), ["id"])
                    KNOWN.get(name, 1)
                    drop_files_without_incoming_keys(
                        [(name, True, 1)], ints([99]), ["id"])
            except BaseException as e:      # noqa: BLE001
                errors.append(e)

        with bound(12000):
            threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert not errors
            files, nbytes = KNOWN.stats()
            assert 0 < nbytes <= 12000
            assert nbytes == sum(entry[1] for entry in KNOWN._entries.values())
            assert files == len(KNOWN._entries)


# ---------------------------------------------------------------------------
# 3. No common value
# ---------------------------------------------------------------------------

class TestNoCommonValue:

    @staticmethod
    def incoming(values, family="int"):
        return proc._IncomingKeyValues(family, list(values))

    def test_a_file_without_values_has_none_in_common(self):
        assert proc._no_common_value(array("q"), self.incoming([1])) is True
        assert proc._no_common_value(frozenset(), self.incoming([b"a"], "str")) is True

    @pytest.mark.parametrize("at", [0, 1, 49, 98, 99])
    def test_few_incoming_values_are_searched_for(self, at):
        file_values = array("q", range(0, 200, 2))                # 100 even numbers
        with patch.object(proc, "bisect_left", wraps=proc.bisect_left) as search:
            assert proc._no_common_value(file_values, self.incoming([file_values[at]])) is False
            assert proc._no_common_value(file_values, self.incoming([file_values[at] + 1])) is True
            assert proc._no_common_value(file_values, self.incoming([-5, 1001])) is True
        assert search.call_count >= 3

    def test_many_incoming_values_are_looked_up_in(self):
        file_values = array("q", range(0, 20, 2))
        with patch.object(proc, "bisect_left", side_effect=AssertionError("searched")):
            assert proc._no_common_value(file_values, self.incoming(range(1, 40, 2))) is True
            assert proc._no_common_value(file_values, self.incoming(list(range(1, 40, 2)) + [18])) is False

    @pytest.mark.parametrize("seed", range(60))
    def test_both_ways_agree_with_the_sets(self, seed):
        rng = random.Random(seed)
        text = rng.random() < 0.4
        pool = (list(b("", "a", "A", "b", "ab", "ä", "a ", "10", "9", "é", "z", "zz")) + [b"m\xff"]
                if text else list(range(-20, 60)))
        of_file = sorted(set(rng.choices(pool, k=rng.randint(0, 60))))
        of_incoming = rng.choices(pool, k=rng.randint(1, 12))
        kept = frozenset(of_file) if text else array("q", of_file)
        answer = proc._no_common_value(kept, self.incoming(of_incoming, "str" if text else "int"))
        assert answer is (not (set(of_file) & set(of_incoming)))

    def test_strings_are_never_searched_for(self):
        """Two sets walk the smaller of the two by themselves."""
        file_values = frozenset(f"{i:05d}".encode() for i in range(500))
        with patch.object(proc, "bisect_left", side_effect=AssertionError("searched")):
            assert proc._no_common_value(file_values, self.incoming([b"00007"], "str")) is False
            assert proc._no_common_value(file_values, self.incoming([b"7"], "str")) is True

    def test_incoming_values_are_counted_once(self):
        incoming = self.incoming([3, 3, 3, 1])
        assert len(incoming) == 2 and incoming.in_order() == [1, 3]


# ---------------------------------------------------------------------------
# 4. Which candidates are dropped
# ---------------------------------------------------------------------------

class TestWhichCandidatesAreDropped:

    def test_a_known_file_without_the_keys_is_dropped(self):
        files = [known_file("a", ints([1, 2, 3]), ["id"]), known_file("b", ints([7, 8]), ["id"])]
        prof = Profiler()
        assert dropped(files, ints([8, 9]), ["id"], profiler=prof) == ["a"]
        assert prof.emit_counts() == {"key_values_pruned_files": 1}

    def test_a_file_that_is_not_known_is_kept(self):
        files = [("a", True, 100)]
        assert dropped(files, ints([8]), ["id"]) == []

    def test_a_file_known_under_another_size_is_kept(self):
        remember_file_key_values("a", 100, ints([1]), ["id"])
        assert dropped([("a", True, 101)], ints([8]), ["id"]) == []
        assert dropped([("a", True, 100)], ints([8]), ["id"]) == ["a"]

    def test_a_file_known_for_other_columns_is_kept(self):
        files = [known_file("a", pl.DataFrame({"id": [1], "code": ["x"]}), ["code"])]
        assert dropped(files, ints([8]), ["id"]) == []

    def test_a_candidate_that_does_not_overlap_is_left_alone(self):
        remember_file_key_values("a", 100, ints([1]), ["id"])
        files = [("a", False, 100)]
        assert drop_files_without_incoming_keys(files, ints([8]), ["id"]) is files

    def test_what_was_given_is_returned_when_nothing_is_dropped(self):
        remember_file_key_values("a", 100, ints([8]), ["id"])
        for files in ([("a", True, 100)], {("a", True, 100)}, (("a", True, 100),)):
            prof = Profiler()
            assert drop_files_without_incoming_keys(files, ints([8]), ["id"], profiler=prof) is files
            assert prof.emit_counts() == {}

    def test_a_set_for_a_set_and_a_list_in_order_for_anything_else(self):
        for name, values in (("a", [1]), ("b", [8]), ("c", [2]), ("d", [8, 9])):
            remember_file_key_values(name, 100, ints(values), ["id"])
        entries = [(n, True, 100) for n in "abcd"]
        incoming = ints([8])
        assert drop_files_without_incoming_keys(list(entries), incoming, ["id"]) == [
            ("b", True, 100), ("d", True, 100)]
        assert drop_files_without_incoming_keys(tuple(entries), incoming, ["id"]) == [
            ("b", True, 100), ("d", True, 100)]
        as_set = drop_files_without_incoming_keys(set(entries), incoming, ["id"])
        assert type(as_set) is set and as_set == {("b", True, 100), ("d", True, 100)}
        frozen = drop_files_without_incoming_keys(frozenset(entries), incoming, ["id"])
        assert type(frozen) is frozenset and frozen == {("b", True, 100), ("d", True, 100)}

    def test_every_candidate_can_be_dropped(self):
        files = {known_file("a", ints([1]), ["id"]), known_file("b", ints([2]), ["id"])}
        assert drop_files_without_incoming_keys(files, ints([8]), ["id"]) == set()

    def test_the_incoming_keys_are_not_touched_when_no_candidate_is_known(self):
        class Untouchable:
            def __getattr__(self, name):
                raise AssertionError(f"incoming keys asked for {name}")

        files = [("a", True, 100), ("b", True, 5)]
        assert drop_files_without_incoming_keys(files, Untouchable(), ["id"]) is files

    def test_without_overwrite_columns_nothing_is_dropped(self):
        files = [known_file("a", ints([1]), ["id"])]
        assert drop_files_without_incoming_keys(files, ints([8]), []) is files

    @pytest.mark.parametrize("nbytes", [0, -1])
    def test_a_bound_of_zero_drops_nothing(self, nbytes):
        files = [known_file("a", ints([1]), ["id"])]
        with bound(nbytes):
            assert drop_files_without_incoming_keys(files, ints([8]), ["id"]) is files

    # -- NULLs ---------------------------------------------------------------

    def test_an_incoming_null_keeps_the_file(self):
        files = [known_file("a", ints([1, None]), ["id"]), known_file("b", ints([1]), ["id"])]
        assert dropped(files, ints([8, None]), ["id"]) == []

    def test_a_null_in_the_file_does_not_keep_it(self):
        files = [known_file("a", ints([1, None]), ["id"]), known_file("b", ints([None]), ["id"])]
        assert dropped(files, ints([8]), ["id"]) == ["a", "b"]

    def test_a_file_without_rows_is_dropped(self):
        files = [known_file("a", ints([]), ["id"])]
        assert dropped(files, ints([8]), ["id"]) == ["a"]

    # -- types ---------------------------------------------------------------

    @pytest.mark.parametrize("of_file", INT_TYPES)
    @pytest.mark.parametrize("of_incoming", INT_TYPES)
    def test_integers_of_any_two_widths_are_compared(self, of_file, of_incoming):
        files = [known_file("has", ints([1, 100], of_file), ["id"]),
                 known_file("has not", ints([2, 101], of_file), ["id"])]
        assert dropped(files, ints([100, 7], of_incoming), ["id"]) == ["has not"]

    @pytest.mark.parametrize("incoming", [
        pl.DataFrame({"id": [8.0]}),
        pl.DataFrame({"id": ["8"]}),
        pl.DataFrame({"id": [True]}),
        pl.DataFrame({"id": [8]}, schema={"id": pl.UInt64}),
        pl.DataFrame({"id": [8]}).cast({"id": pl.Decimal(10, 0)}),
        pl.DataFrame({"id": [8]}).cast({"id": pl.Date}),
    ], ids=lambda f: str(f.schema["id"]))
    def test_an_incoming_type_that_is_not_compared_keeps_the_file(self, incoming):
        files = [known_file("a", ints([1, 2]), ["id"])]
        assert dropped(files, incoming, ["id"]) == []

    @pytest.mark.parametrize("incoming", [
        pl.DataFrame({"code": [8]}),
        pl.DataFrame({"code": ["8"]}, schema={"code": pl.Categorical}),
        pl.DataFrame({"code": [b"8"]}),
    ], ids=lambda f: str(f.schema["code"]))
    def test_strings_are_compared_with_strings_only(self, incoming):
        files = [known_file("a", words(["1", "2"]), ["code"])]
        assert dropped(files, incoming, ["code"]) == []
        assert dropped(files, words(["8"]), ["code"]) == ["a"]

    def test_a_file_whose_key_cannot_be_kept_is_kept(self):
        files = [known_file("a", pl.DataFrame({"id": [1.0, 2.0]}), ["id"])]
        assert KNOWN.get("a", 100) == {"id": None}
        assert dropped(files, pl.DataFrame({"id": [8.0]}), ["id"]) == []
        assert dropped(files, ints([8]), ["id"]) == []

    def test_strings_are_compared_exactly(self):
        files = [known_file("a", words(["a", "é"]), ["code"])]
        for other in ("A", " a", "a ", "é", "", "aa"):
            assert dropped(files, words([other]), ["code"]) == ["a"], other
        for same in ("a", "é"):
            assert dropped(files, words([same]), ["code"]) == []

    # -- more than one key column ---------------------------------------------

    def test_one_column_without_a_common_value_is_enough(self):
        frame = pl.DataFrame({"tenant": [1, 1], "id": [10, 11]})
        files = [known_file("a", frame, ["tenant", "id"])]
        assert dropped(files, pl.DataFrame({"tenant": [2], "id": [10]}), ["tenant", "id"]) == ["a"]
        assert dropped(files, pl.DataFrame({"tenant": [1], "id": [12]}), ["tenant", "id"]) == ["a"]

    def test_common_values_in_every_column_keep_the_file_even_if_no_row_matches(self):
        frame = pl.DataFrame({"tenant": [1, 2], "id": [10, 11]})
        files = [known_file("a", frame, ["tenant", "id"])]
        incoming = pl.DataFrame({"tenant": [1], "id": [11]})        # no row is (1, 11)
        assert dropped(files, incoming, ["tenant", "id"]) == []

    def test_a_null_in_one_column_leaves_the_other_to_decide(self):
        frame = pl.DataFrame({"tenant": [1, 1], "id": [10, 11]})
        files = [known_file("a", frame, ["tenant", "id"])]
        nulls = pl.DataFrame({"tenant": [None, 1], "id": [99, 98]},
                             schema={"tenant": pl.Int64, "id": pl.Int64})
        assert dropped(files, nulls, ["tenant", "id"]) == ["a"]     # id has no null and no common value
        both = pl.DataFrame({"tenant": [None, 2], "id": [None, 98]},
                            schema={"tenant": pl.Int64, "id": pl.Int64})
        assert dropped(files, both, ["tenant", "id"]) == []

    def test_a_column_of_another_type_leaves_the_other_to_decide(self):
        frame = pl.DataFrame({"id": [10, 11], "code": ["x", "y"]})
        files = [known_file("a", frame, ["id", "code"])]
        incoming = pl.DataFrame({"id": [10.0], "code": ["z"]})
        assert dropped(files, incoming, ["id", "code"]) == ["a"]

    # -- many incoming values ---------------------------------------------------

    def test_more_incoming_values_than_the_bound_keep_the_file(self):
        files = [known_file("a", ints([1000]), ["id"])]
        with patch.object(proc, "_KEY_VALUES_MAX_INCOMING", 5):
            assert dropped(files, ints([1, 2, 3, 4, 5]), ["id"]) == ["a"]
            assert dropped(files, ints([1, 2, 3, 4, 5, 6]), ["id"]) == []

    def test_many_incoming_rows_of_few_values_in_one_of_two_columns(self):
        frame = pl.DataFrame({"tenant": [1, 1], "id": [1000, 1001]})
        files = [known_file("a", frame, ["tenant", "id"])]
        incoming = pl.DataFrame({"tenant": [2] * 50, "id": list(range(50))})
        with patch.object(proc, "_KEY_VALUES_MAX_INCOMING", 5):
            # 50 ids are over the bound; the one tenant is not, and rules the file out
            assert dropped(files, incoming, ["tenant", "id"]) == ["a"]
            same_tenant = incoming.with_columns(pl.lit(1, dtype=pl.Int64).alias("tenant"))
            assert dropped(files, same_tenant, ["tenant", "id"]) == []

    def test_under_a_settings_object_that_is_a_mock_nothing_is_dropped_or_kept(self):
        """Tests elsewhere replace the settings of the module by a mock, whose
        every attribute is a mock: that is not a bound."""
        files = [known_file("a", ints([1]), ["id"])]
        assert dropped(files, ints([8]), ["id"]) == ["a"]
        with patch.object(KNOWN, "_cap_getter", proc._key_values_cache_cap), \
                patch("supertable.processing.settings"):
            assert KNOWN.cap() == 0
            assert drop_files_without_incoming_keys(files, ints([8]), ["id"]) is files
            remember_file_key_values("b", 1, ints([1]), ["id"])
            assert KNOWN.get("b", 1) is None

    @pytest.mark.parametrize("failure", [
        RuntimeError("boom"),
        pl.exceptions.PanicException("not an Exception: polars failed in its native code"),
    ], ids=["an exception", "a polars panic"])
    def test_dropping_never_raises_and_then_drops_nothing(self, failure):
        files = [known_file("a", ints([1]), ["id"]), known_file("b", ints([2]), ["id"])]
        prof = Profiler()
        with patch.object(proc, "_incoming_key_values", side_effect=failure):
            assert drop_files_without_incoming_keys(files, ints([8]), ["id"], profiler=prof) is files
        assert prof.emit_counts() == {}

    def test_incoming_strings_that_are_not_utf8(self):
        """They are compared as the bytes they are, and never end the write."""
        files = [known_file("has", pl.DataFrame({"code": not_utf8(b"a", b"m\xff")}), ["code"]),
                 known_file("has not", words(["a", "m"]), ["code"])]
        incoming = pl.DataFrame({"code": not_utf8(b"m\xff", b"q")})
        assert dropped(files, incoming, ["code"]) == ["has not"]
        # the join itself agrees
        keys = incoming.unique()
        has = pl.DataFrame({"code": not_utf8(b"a", b"m\xff")})
        assert has.join(keys, on="code", how="semi", nulls_equal=True).height == 1
        assert words(["a", "m"]).join(keys, on="code", how="semi", nulls_equal=True).height == 0


# ---------------------------------------------------------------------------
# 5. Against the join itself
# ---------------------------------------------------------------------------

WORDS = ["", "a", "A", "b", "ab", "ä", "a ", "10", "9", "é", "é", "z"]
SHAPES = {"int": ["k"], "str": ["s"], "int+str": ["k", "s"], "int+int": ["k", "k2"]}


def _family(dtype) -> str:
    return "str" if dtype == pl.Utf8 else "int"


# Integers at and around the edges of every width, as 64-bit integers.
WIDE = [-2 ** 62, -70000, -129, -128, -1, 0, 1, 127, 128, 255, 256, 65535, 65536,
        2 ** 31 - 1, 2 ** 31, 2 ** 40, 2 ** 62]


def _random_frame(rng: random.Random, columns: List[str], rows: int, nulls: bool,
                  wide: bool = False) -> pl.DataFrame:
    data, schema = {}, {}
    for column in columns:
        pool = WORDS if column == "s" else (WIDE if wide else list(range(40)))
        data[column] = [None if (nulls and rng.random() < 0.15) else rng.choice(pool)
                        for _ in range(rows)]
        schema[column] = (pl.Utf8 if column == "s"
                          else pl.Int64 if wide else rng.choice(INT_TYPES))
    return pl.DataFrame(data, schema=schema)


def _random_case(seed: int):
    """A table of small files, some of them known, and incoming keys."""
    rng = random.Random(seed)
    columns = SHAPES[rng.choice(sorted(SHAPES))]
    wide = rng.random() < 0.3                                   # 64-bit integers of every size
    frames: Dict[str, pl.DataFrame] = {}
    next_rowid = 0
    for i in range(rng.randint(1, 8)):
        rows = rng.choice([0, 1, 2, 5, 12, 20, 21, 25])        # over 20 rows: not kept
        frame = _random_frame(rng, columns, rows, nulls=rng.random() < 0.4, wide=wide)
        frame = frame.with_columns(
            pl.Series(ROWID_COL, list(range(next_rowid, next_rowid + rows)), dtype=pl.Int64))
        next_rowid += rows
        frames[f"file-{seed}-{i}"] = frame
    incoming = _random_frame(rng, columns, rng.randint(1, 12), nulls=rng.random() < 0.3, wide=wide)
    met = {name for name in frames if rng.random() < 0.8}       # the files this process has met
    return columns, frames, incoming, met


def _the_rule(columns, frames, incoming, met, max_rows) -> List[str]:
    """The files the rule names, said again without the code under test."""
    out = []
    for name, frame in frames.items():
        if name not in met or frame.height > max_rows:
            continue
        for column in columns:
            if _family(frame.schema[column]) != _family(incoming.schema[column]):
                continue
            if incoming.get_column(column).null_count():
                continue
            of_file = set(frame.get_column(column).drop_nulls().to_list())
            if not (of_file & set(incoming.get_column(column).to_list())):
                out.append(name)
                break
    return sorted(out)


@pytest.mark.parametrize("seed", range(400))
def test_no_dropped_file_has_a_row_the_join_matches(seed):
    columns, frames, incoming, met = _random_case(seed)
    with patch.object(proc, "_KEY_VALUES_MAX_ROWS", 20):
        for name in met:
            remember_file_key_values(name, 100, frames[name], columns)
        candidates = [(name, True, 100) for name in frames]
        gone = dropped(candidates, incoming, columns)

    keys = incoming.select(columns).unique()
    for name in gone:
        matched = frames[name].join(keys, on=columns, how="semi", nulls_equal=True)
        assert matched.height == 0, (name, matched)
    assert gone == _the_rule(columns, frames, incoming, met, 20)

    # and what the matching finds is what it found with every file
    kept = [entry for entry in candidates if entry[0] not in gone]
    with_all = identify_deleted_rowids(incoming, candidates, columns, file_cache=dict(frames))
    with_kept = identify_deleted_rowids(incoming, kept, columns, file_cache=dict(frames))
    assert with_kept.equals(with_all)


def test_the_random_cases_drop_files_and_keep_files():
    """The test above is not empty: it meets dropped files, kept known files and matches."""
    gone = kept_known = matched = 0
    for seed in range(400):
        columns, frames, incoming, met = _random_case(seed)
        rule = _the_rule(columns, frames, incoming, met, 20)
        gone += len(rule)
        kept_known += len([n for n in met if n not in rule and frames[n].height <= 20])
        keys = incoming.select(columns).unique()
        matched += sum(
            1 for frame in frames.values()
            if frame.join(keys, on=columns, how="semi", nulls_equal=True).height)
    assert gone > 300 and kept_known > 300 and matched > 300, (gone, kept_known, matched)


# ---------------------------------------------------------------------------
# 6. Where the values come from: a file that had to be read
# ---------------------------------------------------------------------------

class TestAFileThatWasReadIsKnown:

    def _frame(self, ids, rowid_from=0, rev=None):
        data = {"id": ids, ROWID_COL: list(range(rowid_from, rowid_from + len(ids)))}
        if rev is not None:
            data["rev"] = [rev] * len(ids)
        return pl.DataFrame(data)

    @staticmethod
    def _reads(frames):
        """A storage that holds *frames*, and the names of the files read from it."""
        read: List[str] = []

        def read_parquet(path, profiler=None, file_size=0, columns=None, required=False):
            read.append(path)
            return frames.get(path)

        return patch.object(proc, "_read_parquet_safe", read_parquet), read

    def test_the_files_the_matching_read_are_known_afterwards(self):
        frames = {"a": self._frame([1, 2, 2]), "b": self._frame([5], 10)}
        reading, read = self._reads(frames)
        with reading:
            identify_deleted_rowids(ints([2]), [("a", True, 7), ("b", True, 8)], ["id"])
        assert read == ["a", "b"]
        assert KNOWN.get("a", 7) == {"id": ("int", array("q", [1, 2]))}
        assert KNOWN.get("b", 8) == {"id": ("int", array("q", [5]))}

    def test_the_files_the_stale_filter_read_are_known_afterwards(self):
        frames = {"a": self._frame([1, 2, 2], rev=1), "b": self._frame([5], 10, rev=1)}
        incoming = pl.DataFrame({"id": [2], "rev": [3]})
        reading, read = self._reads(frames)
        with reading:
            cache: Dict[str, pl.DataFrame] = {}
            proc.filter_stale_incoming_rows(
                incoming_df=incoming, overlapping_files=[("a", True, 7), ("b", True, 8)],
                overwrite_columns=["id"], newer_than_col="rev", file_cache=cache)
            # the matching that follows finds them in the cache and reads nothing
            identify_deleted_rowids(incoming, [("a", True, 7), ("b", True, 8)], ["id"], file_cache=cache)
        assert read == ["a", "b"]
        assert KNOWN.get("a", 7) == {"id": ("int", array("q", [1, 2]))}
        assert KNOWN.get("b", 8) == {"id": ("int", array("q", [5]))}

    def test_the_values_of_deleted_rows_are_values_of_the_file(self):
        """What is kept is what the file holds, whatever the deletion vector says of it."""
        frames = {"a": self._frame([1, 2, 2], rev=1)}             # row ids 0, 1, 2
        incoming = pl.DataFrame({"id": [2], "rev": [3]})
        reading, _ = self._reads(frames)
        with reading:
            proc.filter_stale_incoming_rows(
                incoming_df=incoming, overlapping_files=[("a", True, 7)], overwrite_columns=["id"],
                newer_than_col="rev", file_cache={}, dead_rowids=[0])
        assert KNOWN.get("a", 7) == {"id": ("int", array("q", [1, 2]))}

    def test_a_frame_the_caller_handed_in_is_not_taken_for_the_file(self):
        """What is in ``file_cache`` is the caller's, and may be less than the file."""
        frames = {"a": self._frame([1, 2, 2]), "b": self._frame([5], 10)}
        with patch.object(proc, "_read_parquet_safe", side_effect=AssertionError("read")):
            pairs = identify_deleted_rowids(ints([2]), [("a", True, 7), ("b", True, 8)], ["id"],
                                            file_cache=dict(frames))
        assert pairs.height == 2
        assert KNOWN.stats() == (0, 0)

    def test_a_large_file_is_not(self):
        frames = {"a": self._frame(list(range(30)))}
        reading, _ = self._reads(frames)
        with reading, patch.object(proc, "_KEY_VALUES_MAX_ROWS", 29):
            identify_deleted_rowids(ints([2]), [("a", True, 7)], ["id"])
        assert KNOWN.get("a", 7) is None

    def test_a_file_that_is_gone_or_does_not_overlap_is_not_kept(self):
        reading, read = self._reads({})
        with reading:
            identify_deleted_rowids(ints([1]), [("gone", True, 1), ("no overlap", False, 1)], ["id"])
        assert read == ["gone"]
        assert KNOWN.stats() == (0, 0)

    def test_what_is_kept_of_a_file_that_cannot_be_matched(self):
        """Its values are its values, whether or not it has row ids to delete by."""
        frames = {"no row id": pl.DataFrame({"id": [1]}),
                  "no key": pl.DataFrame({"other": [1], ROWID_COL: [0]})}
        reading, _ = self._reads(frames)
        with reading:
            pairs = identify_deleted_rowids(
                ints([1]), [(name, True, 1) for name in frames], ["id"])
        assert pairs.height == 0
        assert KNOWN.get("no row id", 1) == {"id": ("int", array("q", [1]))}
        assert KNOWN.get("no key", 1) is None                     # nothing is known of a column it lacks

    def test_matching_itself_drops_nothing(self):
        """Only resolve_overwrite_writes drops candidates; the matching reads what it is given."""
        frames = {"a": self._frame([1]), "b": self._frame([2], 10)}
        candidates = [("a", True, 1), ("b", True, 1)]
        reading, read = self._reads(frames)
        with reading:
            for _ in range(2):
                prof = Profiler()
                pairs = identify_deleted_rowids(ints([2]), candidates, ["id"], profiler=prof)
                assert prof.emit_counts()["delete_files_seen"] == 2
                assert pairs.to_dicts() == [{TOMBSTONE_FILE_COL: "b", ROWID_COL: 10}]
        assert read == ["a", "b", "a", "b"]


# ---------------------------------------------------------------------------
# 7. Through real files, as a write resolves them
# ---------------------------------------------------------------------------

@pytest.fixture
def local_storage():
    with patch("supertable.processing._get_storage", return_value=LocalStorage()):
        yield


def _write(directory, name, df):
    path = str(directory / name)
    pq.write_table(df.to_arrow(), path)
    return (path, True, (directory / name).stat().st_size)


def _resolve(incoming, files, columns, newer_than=None):
    prof = Profiler()
    filtered, pairs = resolve_overwrite_writes(
        incoming_df=incoming, overlapping_files=files, overwrite_columns=list(columns),
        newer_than_col=newer_than, profiler=prof)
    return filtered, pairs, prof.emit_counts()


# Per round of the random tables scanned by DuckDB: files dropped, scans run again, pairs found.
_PROBE_ROUNDS: List[tuple] = []


def _in_order(pairs: pl.DataFrame) -> pl.DataFrame:
    return pairs.sort([TOMBSTONE_FILE_COL, ROWID_COL])


class TestThroughRealFiles:

    def _table(self, tmp_path, n=12):
        """n small files of 6 rows; file i holds ids 3i .. 3i+5 at revision i."""
        files = []
        for i in range(n):
            df = pl.DataFrame({
                "id": [i * 3 + j for j in range(6)],
                "rev": [i for _ in range(6)],
                "payload": [f"row-{i}-{j}" for j in range(6)],
                ROWID_COL: [i * 100 + j for j in range(6)],
            })
            files.append(_write(tmp_path, f"data-{i:02d}.parquet", df))
        return files

    INCOMING = pl.DataFrame({"id": [0, 4, 999], "rev": [9, 9, 9], "payload": ["n"] * 3})

    def test_the_second_upsert_reads_only_the_files_that_hold_its_keys(self, tmp_path, local_storage):
        files = self._table(tmp_path)
        with bound(0):
            filtered0, pairs0, counts0 = _resolve(self.INCOMING, files, ["id"])
        assert counts0["files_read"] == 12 and "key_values_pruned_files" not in counts0

        filtered1, pairs1, counts1 = _resolve(self.INCOMING, files, ["id"])     # meets the files
        assert counts1["files_read"] == 12 and "key_values_pruned_files" not in counts1
        assert KNOWN.stats()[0] == 12

        filtered2, pairs2, counts2 = _resolve(self.INCOMING, files, ["id"])     # knows them
        assert counts2["key_values_pruned_files"] == 10
        assert counts2["files_read"] == 2 and counts2["delete_files_seen"] == 2  # ids 0 and 4: files 0, 1

        assert pairs0.height == 3
        assert pairs1.equals(pairs0) and pairs2.equals(pairs0)
        assert filtered1.equals(filtered0) and filtered2.equals(filtered0)

    def test_a_set_of_candidates_resolves_to_the_same_pairs(self, tmp_path, local_storage):
        files = set(self._table(tmp_path))
        with bound(0):
            _, pairs0, _ = _resolve(self.INCOMING, files, ["id"])
        _resolve(self.INCOMING, files, ["id"])
        _, pairs2, counts2 = _resolve(self.INCOMING, files, ["id"])
        assert counts2["key_values_pruned_files"] == 10
        assert _in_order(pairs2).equals(_in_order(pairs0))

    def test_a_newer_than_upsert_reads_and_resolves_the_same(self, tmp_path, local_storage):
        files = self._table(tmp_path)
        incoming = pl.DataFrame({"id": [0, 4, 7, 999], "rev": [0, 5, 1, 1], "payload": ["n"] * 4})
        with bound(0):
            filtered0, pairs0, counts0 = _resolve(incoming, files, ["id"], newer_than="rev")
        _resolve(incoming, files, ["id"], newer_than="rev")
        filtered2, pairs2, counts2 = _resolve(incoming, files, ["id"], newer_than="rev")
        assert counts0["files_read"] == 12
        # ids 0, 4 and 7 are in files 0, 1 and 2; the stale filter reads them once, the matching none
        assert counts2["files_read"] == 3 and counts2["key_values_pruned_files"] == 9
        assert filtered0.height < incoming.height                 # something was stale
        assert filtered2.equals(filtered0) and pairs2.equals(pairs0)

    def test_when_no_file_holds_a_key_nothing_is_read(self, tmp_path, local_storage):
        files = self._table(tmp_path)
        incoming = pl.DataFrame({"id": [500, 501], "rev": [1, 1], "payload": ["n", "n"]})
        with bound(0):
            filtered0, pairs0, _ = _resolve(incoming, files, ["id"], newer_than="rev")
        _resolve(incoming, files, ["id"])
        with patch.object(proc, "_read_parquet_safe", side_effect=AssertionError("read")), \
                patch.object(proc, "_duckdb_probe_overlap_matches", side_effect=AssertionError("probed")):
            for newer_than in (None, "rev"):
                filtered, pairs, counts = _resolve(incoming, files, ["id"], newer_than=newer_than)
                assert filtered is incoming
                assert pairs.equals(pairs0) and pairs.height == 0
                assert counts == {"key_values_pruned_files": 12}
        assert filtered0.equals(incoming)

    def test_the_probe_is_given_only_the_files_that_are_left(self, tmp_path, local_storage):
        files = self._table(tmp_path)
        _resolve(self.INCOMING, files, ["id"])
        seen = []

        def probe(overlap_true, overwrite_columns, newer_than_col, incoming_keys, profiler=None):
            seen.append(sorted(os.path.basename(f) for f, _ in overlap_true))
            return None                                           # and the files are then read

        class ProbeOn:
            def __getattr__(self, name):
                from supertable.config.settings import settings
                return True if name == "SUPERTABLE_DUCKDB_WRITE_PROBE" else getattr(settings, name)

        with patch.object(proc, "settings", ProbeOn()), \
                patch.object(proc, "_duckdb_probe_overlap_matches", probe):
            _, pairs, counts = _resolve(self.INCOMING, files, ["id"])
        # a probe that fails over the files left is given every file before it is
        # given up (see TestWithTheProbeOn); the files then read are the ones left
        assert seen[0] == ["data-00.parquet", "data-01.parquet"]
        assert len(seen) == 2 and len(seen[1]) == 12
        assert pairs.height == 3 and counts["files_read"] == 2

    def test_a_large_file_is_read_every_time(self, tmp_path, local_storage):
        files = self._table(tmp_path, n=4)
        with patch.object(proc, "_KEY_VALUES_MAX_ROWS", 5):       # the files have 6 rows
            for _ in range(3):
                _, pairs, counts = _resolve(self.INCOMING, files, ["id"])
                assert counts["files_read"] == 4 and pairs.height == 3
        assert KNOWN.stats() == (0, 0)

    def test_a_file_met_for_one_key_column_is_met_again_for_another(self, tmp_path, local_storage):
        files = self._table(tmp_path, n=4)
        by_rev = pl.DataFrame({"id": [1], "rev": [77], "payload": ["n"]})
        _resolve(self.INCOMING, files, ["id"])
        _, _, first = _resolve(by_rev, files, ["rev"])            # known by id only: all read
        _, pairs, second = _resolve(by_rev, files, ["rev"])
        assert first["files_read"] == 4 and "key_values_pruned_files" not in first
        assert second == {"key_values_pruned_files": 4} and pairs.height == 0

    @staticmethod
    def _random_rounds(tmp_path, seed, newer_than) -> int:
        """Three writes over a random table, each resolved without the kept values
        and with them: the same rows survive, the same pairs come out, no more
        files are read.  Returns how many files were dropped."""
        rng = random.Random(seed)
        files, next_rowid = [], 0
        for i in range(rng.randint(2, 9)):
            rows = rng.choice([1, 3, 8, 30])                      # 30 rows: over the bound below
            ids = [None if rng.random() < 0.05 else rng.randrange(60) for _ in range(rows)]
            df = pl.DataFrame({
                "id": ids,
                "rev": [rng.randrange(10) for _ in range(rows)],
                ROWID_COL: list(range(next_rowid, next_rowid + rows)),
            }, schema={"id": pl.Int64, "rev": pl.Int64, ROWID_COL: pl.Int64})
            next_rowid += rows
            files.append(_write(tmp_path, f"data-{i:02d}.parquet", df))
        gone = 0
        with patch.object(proc, "_KEY_VALUES_MAX_ROWS", 20):
            for _ in range(3):
                n = rng.randint(1, 6)
                incoming = pl.DataFrame({
                    "id": [None if rng.random() < 0.05 else rng.randrange(60) for _ in range(n)],
                    "rev": [rng.randrange(10) for _ in range(n)],
                }, schema={"id": pl.Int64, "rev": pl.Int64})
                with bound(0):
                    filtered0, pairs0, counts0 = _resolve(incoming, files, ["id"], newer_than)
                filtered, pairs, counts = _resolve(incoming, files, ["id"], newer_than)
                assert filtered.equals(filtered0)
                assert pairs.equals(pairs0)
                assert counts.get("files_read", 0) <= counts0.get("files_read", 0)
                if counts.get("key_values_pruned_files"):
                    gone += counts["key_values_pruned_files"]
                    assert counts.get("files_read", 0) < counts0["files_read"]
        return gone

    @pytest.mark.parametrize("seed", range(40))
    @pytest.mark.parametrize("newer_than", [None, "rev"])
    def test_random_tables_resolve_the_same_with_and_without(self, tmp_path, local_storage,
                                                             seed, newer_than):
        self._random_rounds(tmp_path, seed, newer_than)

    def test_the_random_tables_drop_files(self, tmp_path, local_storage):
        """The parity above is over cases in which files are dropped."""
        gone = 0
        for seed in range(10):
            (tmp_path / str(seed)).mkdir()
            gone += self._random_rounds(tmp_path / str(seed), seed, None)
        assert gone > 30, gone


# ---------------------------------------------------------------------------
# 7b. With the DuckDB probe on: the real scan, over the files that are left
# ---------------------------------------------------------------------------

@pytest.fixture
def probe_on(monkeypatch, local_storage):
    """``SUPERTABLE_DUCKDB_WRITE_PROBE=true``: one DuckDB scan finds the matching rows."""
    from supertable.config.settings import settings

    monkeypatch.setattr(
        proc, "settings", dataclasses.replace(settings, SUPERTABLE_DUCKDB_WRITE_PROBE=True))


def _meet(files, columns) -> None:
    """What a process knows of files it wrote or read: with the probe on a file
    is not read into a frame here, so a process knows the files it wrote."""
    for path, _, size in files:
        remember_file_key_values(path, size, pl.read_parquet(path), list(columns))


class TestWithTheProbeOn:

    INCOMING = pl.DataFrame({"id": [0, 4, 999], "rev": [9, 9, 9], "payload": ["n"] * 3})

    def test_the_scan_is_over_the_files_that_are_left_and_finds_the_same(self, tmp_path, probe_on):
        files = TestThroughRealFiles()._table(tmp_path)
        _, pairs0, counts0 = _resolve(self.INCOMING, files, ["id"])
        assert counts0["probe_files"] == 12 and "overwrite_resolve_fallback" not in counts0

        _meet(files, ["id"])
        filtered, pairs, counts = _resolve(self.INCOMING, files, ["id"])
        assert counts["probe_files"] == 2 and counts["key_values_pruned_files"] == 10
        assert "overwrite_resolve_fallback" not in counts and "probe_retry_all_files" not in counts
        assert "files_read" not in counts                         # the scan read them, no frame was
        assert pairs.height == 3 and _in_order(pairs).equals(_in_order(pairs0))
        assert filtered.equals(self.INCOMING)

    def test_a_newer_than_upsert_finds_the_same(self, tmp_path, probe_on):
        files = TestThroughRealFiles()._table(tmp_path)
        incoming = pl.DataFrame({"id": [0, 4, 7, 999], "rev": [0, 5, 1, 1], "payload": ["n"] * 4})
        filtered0, pairs0, counts0 = _resolve(incoming, files, ["id"], newer_than="rev")
        _meet(files, ["id"])
        filtered, pairs, counts = _resolve(incoming, files, ["id"], newer_than="rev")
        assert counts0["probe_files"] == 12 and counts["probe_files"] == 3
        assert filtered0.height == 2
        assert filtered.equals(filtered0) and _in_order(pairs).equals(_in_order(pairs0))

    def test_when_no_file_holds_a_key_nothing_is_scanned(self, tmp_path, probe_on):
        files = TestThroughRealFiles()._table(tmp_path)
        _meet(files, ["id"])
        incoming = pl.DataFrame({"id": [500, 501], "rev": [1, 1], "payload": ["n", "n"]})
        filtered, pairs, counts = _resolve(incoming, files, ["id"])
        assert filtered is incoming and pairs.height == 0
        assert counts == {"key_values_pruned_files": 12}

    def _evolved(self, tmp_path):
        """A table whose key was widened: the older file has no ``region``."""
        newer = _write(tmp_path, "newer.parquet", pl.DataFrame(
            {"id": [1, 100], "region": ["eu", "us"], ROWID_COL: [0, 1]}))
        older = _write(tmp_path, "older.parquet", pl.DataFrame(
            {"id": [7, 8], ROWID_COL: [10, 11]}))
        incoming = pl.DataFrame({"id": [7], "region": [None]},
                                schema={"id": pl.Int64, "region": pl.Utf8})
        return newer, older, incoming

    def test_a_scan_that_fails_over_the_files_left_is_given_every_file(self, tmp_path, probe_on):
        """The scan binds its columns by name over the files it is given.  Scanned
        with the newer file, the older one's missing ``region`` is NULL and its
        row matches the incoming NULL; scanned alone it has no such column and
        the scan fails.  Dropping the newer file must not change what is found."""
        newer, older, incoming = self._evolved(tmp_path)
        files = [newer, older]
        _, pairs0, counts0 = _resolve(incoming, files, ["id", "region"])
        assert pairs0.to_dicts() == [{TOMBSTONE_FILE_COL: older[0], ROWID_COL: 10}]
        assert counts0["probe_files"] == 2 and "overwrite_resolve_fallback" not in counts0

        _meet([newer], ["id", "region"])                          # ids 1 and 100: not 7
        _, pairs, counts = _resolve(incoming, files, ["id", "region"])
        assert pairs.equals(pairs0)
        assert counts["key_values_pruned_files"] == 1 and counts["probe_retry_all_files"] == 1
        assert counts["probe_files"] == 2 and "overwrite_resolve_fallback" not in counts

    def test_a_scan_that_fails_with_every_file_too_falls_back_to_the_files_left(self, tmp_path, probe_on):
        newer, older, incoming = self._evolved(tmp_path)
        files = [newer, older]
        _meet([newer], ["id", "region"])
        scans = []

        def failing(overlap_true, *rest, **more):
            scans.append(sorted(os.path.basename(f) for f, _ in overlap_true))
            return None

        with patch.object(proc, "_duckdb_probe_overlap_matches", failing):
            _, pairs, counts = _resolve(incoming, files, ["id", "region"])
        assert scans == [["older.parquet"], ["newer.parquet", "older.parquet"]]
        assert counts["overwrite_resolve_fallback"] == 1 and counts["probe_retry_all_files"] == 1
        assert counts["files_read"] == 1                          # the older file only
        assert pairs.height == 0                                  # read as a frame, a file without a key column is skipped

    def test_a_scan_that_fails_is_not_run_again_when_nothing_was_dropped(self, tmp_path, probe_on):
        newer, older, incoming = self._evolved(tmp_path)
        scans = []

        def failing(overlap_true, *rest, **more):
            scans.append(len(overlap_true))
            return None

        with patch.object(proc, "_duckdb_probe_overlap_matches", failing):
            _, _, counts = _resolve(incoming, [newer, older], ["id", "region"])
        assert scans == [2] and "probe_retry_all_files" not in counts

    @pytest.mark.parametrize("seed", range(60))
    @pytest.mark.parametrize("newer_than", [None, "rev"])
    def test_random_tables_resolve_the_same_with_and_without(self, tmp_path, probe_on, seed, newer_than):
        """Tables some of whose files lack the second key column, written and scanned for real."""
        rng = random.Random(1000 + seed)
        keys = ["id", "grp"] if rng.random() < 0.6 else ["id"]
        files, next_rowid = [], 0
        for i in range(rng.randint(2, 8)):
            rows = rng.choice([1, 3, 8])
            data = {
                "id": [None if rng.random() < 0.05 else rng.randrange(40) for _ in range(rows)],
                "rev": [rng.randrange(10) for _ in range(rows)],
                ROWID_COL: list(range(next_rowid, next_rowid + rows)),
            }
            schema = {"id": pl.Int64, "rev": pl.Int64, ROWID_COL: pl.Int64}
            if rng.random() < 0.75:                               # the others were written before `grp`
                data["grp"] = [None if rng.random() < 0.1 else rng.choice(["a", "b", "c"])
                               for _ in range(rows)]
                schema["grp"] = pl.Utf8
            next_rowid += rows
            files.append(_write(tmp_path, f"data-{i:02d}.parquet", pl.DataFrame(data, schema=schema)))
        for round_ in range(3):
            n = rng.randint(1, 5)
            incoming = pl.DataFrame({
                "id": [None if rng.random() < 0.05 else rng.randrange(40) for _ in range(n)],
                "grp": [None if rng.random() < 0.3 else rng.choice(["a", "b", "c"]) for _ in range(n)],
                "rev": [rng.randrange(10) for _ in range(n)],
            }, schema={"id": pl.Int64, "grp": pl.Utf8, "rev": pl.Int64})
            with bound(0):
                filtered0, pairs0, counts0 = _resolve(incoming, files, keys, newer_than)
            if round_ == 1:
                _meet(files, keys)                                # from here on the files are known
            filtered, pairs, counts = _resolve(incoming, files, keys, newer_than)
            assert filtered.sort(["id", "grp", "rev"]).equals(filtered0.sort(["id", "grp", "rev"]))
            assert _in_order(pairs).equals(_in_order(pairs0)), (counts0, counts)
            _PROBE_ROUNDS.append((counts.get("key_values_pruned_files", 0),
                                  counts.get("probe_retry_all_files", 0), pairs0.height))

    def test_the_random_tables_drop_files_and_run_the_scan_again(self):
        """The parity above meets dropped files, scans that failed and were run again, and matches."""
        if len(_PROBE_ROUNDS) < 300:
            pytest.skip("the random tables did not all run in this process")
        assert sum(1 for gone, _, _ in _PROBE_ROUNDS if gone) > 100
        assert sum(again for _, again, _ in _PROBE_ROUNDS) > 0
        assert sum(1 for _, _, matched in _PROBE_ROUNDS if matched) > 100


# ---------------------------------------------------------------------------
# 8. Where the values come from: a file this process wrote
# ---------------------------------------------------------------------------

class TestAFileThatWasWrittenIsKnown:

    def _write_file(self, tmp_path, frame, key_columns=None):
        resources: List[dict] = []
        write_parquet_and_collect_resources(
            write_df=frame, overwrite_columns=[], data_dir=str(tmp_path), new_resources=resources,
            compression_level=1, key_columns=key_columns)
        return resources

    def test_a_small_file_written_with_key_columns(self, tmp_path, local_storage):
        frame = pl.DataFrame({"id": [5, 3, 5], "code": ["x", "y", None], "amount": [1.0, 2.0, 3.0]})
        (resource,) = self._write_file(tmp_path, frame, ["id", "code"])
        assert KNOWN.get(resource["file"], resource["file_size"]) == {
            "id": ("int", array("q", [3, 5])), "code": ("str", b("x", "y"))}
        assert resource["file_size"] == os.path.getsize(resource["file"])
        # and it is what the file holds
        on_disk = pl.read_parquet(resource["file"])
        assert sorted(on_disk.get_column("id").unique().to_list()) == [3, 5]

    def test_under_a_time_bucket_too(self, tmp_path, local_storage):
        frame = pl.DataFrame({"id": [2, 1], "__timestamp__": [20, 10]})
        (resource,) = self._write_file(tmp_path, frame, ["id"])
        assert "year=" in resource["file"]
        assert KNOWN.get(resource["file"], resource["file_size"]) == {"id": ("int", array("q", [1, 2]))}

    @pytest.mark.parametrize("key_columns", [None, []])
    def test_nothing_is_kept_without_key_columns(self, tmp_path, local_storage, key_columns):
        self._write_file(tmp_path, pl.DataFrame({"id": [1]}), key_columns)
        assert KNOWN.stats() == (0, 0)

    def test_a_large_file_is_not_kept(self, tmp_path, local_storage):
        with patch.object(proc, "_KEY_VALUES_MAX_ROWS", 2):
            self._write_file(tmp_path, pl.DataFrame({"id": [1, 2, 3]}), ["id"])
        assert KNOWN.stats() == (0, 0)

    def test_an_upsert_does_not_read_the_small_files_this_process_wrote(self, tmp_path, local_storage):
        resources: List[dict] = []
        for i in range(6):
            frame = pl.DataFrame({"id": [i * 10, i * 10 + 1], ROWID_COL: [i * 2, i * 2 + 1]})
            resources += self._write_file(tmp_path, frame, ["id"])
        files = [(r["file"], True, r["file_size"]) for r in resources]
        incoming = pl.DataFrame({"id": [21, 77]})
        with bound(0):
            _, pairs0, counts0 = _resolve(incoming, files, ["id"])
        _, pairs, counts = _resolve(incoming, files, ["id"])
        assert counts0["files_read"] == 6
        assert counts["files_read"] == 1 and counts["key_values_pruned_files"] == 5
        assert pairs.equals(pairs0) and pairs.to_dicts() == [
            {TOMBSTONE_FILE_COL: resources[2]["file"], ROWID_COL: 5}]
