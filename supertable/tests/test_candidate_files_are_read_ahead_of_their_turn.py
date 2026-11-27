"""
The files an upsert or delete could not rule out are read ahead of their turn.

Such a write reads every candidate file and uses them in the order given.
The reads do not depend on one another, and most of what one costs is
waiting: on an object store an existence check and a download for every file.
So while the file whose turn it is is in use, the ones after it are already
being read, by a few threads the process keeps (``_CandidateReads`` in
``processing.py``; ``SUPERTABLE_WRITE_READ_AHEAD_THREADS``).

Reading ahead is a faster way to the same frames and never anything else:

  * a file's frame is used for that file, in its turn, whichever read
    finishes first;
  * every file that can be read is read once, by ``_read_parquet_safe``, with
    the arguments it always had;
  * a read that did not return its file while others were on their way is
    not believed, whether it failed or found no file: ``_read_parquet_safe``
    takes a file that cannot be read for one that is not there, and a local
    ``stat`` that fails says "not there" too.  The caller reads the file
    again itself, alone, and the files after it one by one;
  * what a read raises that is not caught is raised when its file's turn
    comes, not before, with what the read had counted;
  * a read is counted when its file is used;
  * never more files at a time than the setting says, nor more bytes ahead
    than ``_READ_AHEAD_BYTES``;
  * a busy pool never makes the caller wait for a read it can do itself;
  * one write at a time: a write reads ahead only when no other write of the
    process is reading its candidate files;
  * nothing of a write is being read once it has returned or raised, and a
    caller that is being stopped is stopped at once, not when a read returns.

The last part runs ``identify_deleted_rowids``, ``filter_stale_incoming_rows``
and ``resolve_overwrite_writes`` over random tables of real files, one file
after another and with 2, 3 and 8 threads over a storage that answers late and
out of order: the frames, the counters and what is raised must be the same.
"""
from __future__ import annotations

import dataclasses
import os
import random
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import List, Optional, Tuple
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    ROWID_COL,
    _CandidateReads,
    filter_stale_incoming_rows,
    identify_deleted_rowids,
    resolve_overwrite_writes,
)
from supertable.storage.local_storage import LocalStorage  # noqa: E402
from supertable.utils.profiler import Profiler, get_null_profiler  # noqa: E402

MAIN = threading.main_thread().name
A_WHILE = 10.0      # seconds a test waits for something that must happen


def threads(n):
    """The setting, for the length of a ``with``."""
    return patch.object(
        proc, "settings",
        dataclasses.replace(proc.settings, SUPERTABLE_WRITE_READ_AHEAD_THREADS=n))


@pytest.fixture(autouse=True)
def no_key_values():
    """The files are read at every call: nothing is ruled out by what an earlier call saw."""
    proc._FILE_KEY_VALUES.clear()
    with patch.object(proc._FILE_KEY_VALUES, "_cap_getter", lambda: 0):
        yield
    proc._FILE_KEY_VALUES.clear()


@pytest.fixture(autouse=True)
def no_write_under_way():
    """Every test starts with no write reading candidate files, and must leave none."""
    proc._candidate_passes = 0
    yield
    left = proc._candidate_passes
    proc._candidate_passes = 0
    assert left == 0, f"{left} write(s) were left reading candidate files"


def frame_of(path: str) -> pl.DataFrame:
    return pl.DataFrame({"read_from": [path]})


class Reads:
    """Stands in for ``_read_parquet_safe``: says who read what, and can hold a read."""

    def __init__(self, late=None):
        self._lock = threading.Lock()
        self.calls: List[tuple] = []          # (path, file_size, columns, thread name)
        self.at_once = 0
        self.most_at_once = 0
        self.at_once_when_started: List[int] = []
        self._gates = {}
        self._started = {}
        self.raises = {}
        self.fails_on_a_thread = set()        # these fail unless the caller reads them itself
        self.absent_on_a_thread = set()       # these are "not there" unless the caller reads them itself
        self.before = {}                      # path -> called with ``required`` once its read has started
        self.required = []                    # (path, thread name, required)
        self._late = late                     # path -> seconds the read takes

    def started(self, path) -> threading.Event:
        with self._lock:
            return self._started.setdefault(path, threading.Event())

    def hold(self, path) -> threading.Event:
        """The read of *path* waits until the event returned is set."""
        gate = self._gates[path] = threading.Event()
        return gate

    def __call__(self, path, profiler=None, file_size=0, columns=None, required=False):
        with self._lock:
            self.calls.append((path, file_size, columns, threading.current_thread().name))
            self.required.append((path, threading.current_thread().name, required))
            self.at_once += 1
            self.most_at_once = max(self.most_at_once, self.at_once)
            self.at_once_when_started.append(self.at_once)
        self.started(path).set()
        try:
            gate = self._gates.get(path)
            if gate is not None:
                assert gate.wait(A_WHILE), f"the read of {path} was never let go"
            if self._late is not None:
                time.sleep(self._late(path))
            if path in self.before:
                self.before[path](required)
            if path in self.raises:
                raise self.raises[path]
            if path in self.fails_on_a_thread and threading.current_thread().name != MAIN:
                raise RuntimeError(f"503 slow down: {path}")
            if path in self.absent_on_a_thread and threading.current_thread().name != MAIN:
                return None
            if profiler is not None:
                profiler.add("files_read", 1)
                profiler.add("bytes_read", file_size if type(file_size) is int else 0)
            return frame_of(path)
        finally:
            with self._lock:
                self.at_once -= 1

    def paths(self) -> List[str]:
        return [c[0] for c in self.calls]

    def readers(self) -> List[str]:
        return [c[3] for c in self.calls]


def some_files(n, size=100) -> List[Tuple[str, int]]:
    return [(f"data-{i:02d}.parquet", size) for i in range(n)]


def read_all(files, columns=None, profiler=None):
    reads = _CandidateReads(files, columns, profiler or get_null_profiler())
    try:
        return [reads.read(path, size) for path, size in files]
    finally:
        reads.close()


def late_and_out_of_order(seed=0, most=0.003):
    """Each file takes its own time, the same in every run: later files often come first."""
    def seconds(path):
        return random.Random(f"{seed}:{path}").random() * most
    return seconds


# ---------------------------------------------------------------------------
# How many files at a time
# ---------------------------------------------------------------------------

class TestHowManyAtATime:

    def _with(self, threads_value, storage_type="LOCAL"):
        return patch.object(proc, "settings", SimpleNamespace(
            SUPERTABLE_WRITE_READ_AHEAD_THREADS=threads_value, STORAGE_TYPE=storage_type))

    def test_the_setting_as_it_ships_is_to_choose(self):
        from supertable.config.settings import Settings
        assert Settings.__dataclass_fields__["SUPERTABLE_WRITE_READ_AHEAD_THREADS"].default == 0

    @pytest.mark.parametrize("storage_type", ["LOCAL", "local", " Local "])
    def test_chosen_for_a_local_disk(self, storage_type):
        with self._with(0, storage_type):
            assert proc._candidate_read_threads() == proc._READ_AHEAD_THREADS_LOCAL

    @pytest.mark.parametrize("storage_type", ["MINIO", "S3", "AZURE", "GCP", "something-else", "", None])
    def test_chosen_for_anything_else(self, storage_type):
        with self._with(0, storage_type):
            assert proc._candidate_read_threads() == proc._READ_AHEAD_THREADS_REMOTE

    def test_what_is_chosen_is_within_the_pool(self):
        assert 1 <= proc._READ_AHEAD_THREADS_LOCAL <= proc._READ_AHEAD_MAX_THREADS
        assert 1 <= proc._READ_AHEAD_THREADS_REMOTE <= proc._READ_AHEAD_MAX_THREADS

    def test_what_is_chosen_as_it_ships(self):
        """Two at a time on a local disk, where it was measured and nothing refuses a second
        reader.  One file after another on an object store: a store that limits requests refuses
        sooner when asked for several files at once, and a refused read is taken for a missing
        file; how many at a time is the operator's to set there."""
        assert proc._READ_AHEAD_THREADS_LOCAL == 2
        assert proc._READ_AHEAD_THREADS_REMOTE == 1

    @pytest.mark.parametrize("given, taken", [(1, 1), (2, 2), (5, 5), (8, 8), (9, 8), (1000, 8)])
    def test_a_number_is_taken_up_to_the_pool(self, given, taken):
        with self._with(given, "S3"):
            assert proc._candidate_read_threads() == taken
        with self._with(given, "LOCAL"):
            assert proc._candidate_read_threads() == taken

    @pytest.mark.parametrize("given", [-1, -8, "4", 4.0, None, True, object()])
    def test_anything_else_reads_one_after_another(self, given):
        with self._with(given):
            assert proc._candidate_read_threads() == 1

    def test_settings_that_cannot_be_read_read_one_after_another(self):
        class Broken:
            @property
            def SUPERTABLE_WRITE_READ_AHEAD_THREADS(self):
                raise RuntimeError("no settings")
        with patch.object(proc, "settings", Broken()):
            assert proc._candidate_read_threads() == 1

    def test_settings_a_test_made_up_read_one_after_another(self):
        from unittest.mock import MagicMock
        with patch.object(proc, "settings", MagicMock()):
            assert proc._candidate_read_threads() == 1

    @pytest.mark.parametrize("written, taken", [
        (None, 0), ("", 0), ("   ", 0), ("0", 0), ("1", 1), (" 3 ", 3), ("8", 8), ("-2", -2),
        # not a whole number: one file after another, never "choose"
        ("off", 1), ("false", 1), ("no", 1), ("one", 1), ("1.0", 1), ("1 # one at a time", 1), ("2x", 1),
    ])
    def test_the_setting_as_it_is_read_from_the_environment(self, written, taken):
        from supertable.config import settings as settings_module
        env = dict(os.environ)
        env.pop("SUPERTABLE_WRITE_READ_AHEAD_THREADS", None)
        if written is not None:
            env["SUPERTABLE_WRITE_READ_AHEAD_THREADS"] = written
        with patch.dict(os.environ, env, clear=True):
            assert settings_module._env_read_ahead_threads() == taken


# ---------------------------------------------------------------------------
# One file after another: exactly what it was
# ---------------------------------------------------------------------------

class TestOneAfterAnother:

    def _no_pool(self):
        def refuse():
            raise AssertionError("the pool was asked for")
        return patch.object(proc, "_read_ahead_executor", refuse)

    def test_with_one_thread_every_file_is_read_by_the_caller_in_order(self):
        files, fake, prof = some_files(6), Reads(), Profiler()
        with threads(1), self._no_pool(), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files, columns=["id", ROWID_COL], profiler=prof)
        assert fake.calls == [(p, s, ["id", ROWID_COL], MAIN) for p, s in files]
        assert [required for _, _, required in fake.required] == [False] * 6      # the read it always was
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert prof.emit_counts() == {"files_read": 6, "bytes_read": 600}

    def test_one_file_is_read_by_the_caller_whatever_the_setting(self):
        fake = Reads()
        with threads(8), self._no_pool(), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(some_files(1))
        assert fake.readers() == [MAIN] and frames[0]["read_from"][0] == "data-00.parquet"

    def test_no_file_reads_nothing(self):
        fake = Reads()
        with threads(8), self._no_pool(), patch.object(proc, "_read_parquet_safe", fake):
            assert read_all([]) == []
        assert fake.calls == []

    def test_the_profiler_handed_to_the_read_is_the_callers(self):
        seen = []

        def fake(path, profiler=None, file_size=0, columns=None, required=False):
            seen.append(profiler)
            return frame_of(path)
        prof = Profiler()
        with threads(1), patch.object(proc, "_read_parquet_safe", fake):
            read_all(some_files(3), profiler=prof)
        assert seen == [prof, prof, prof]


# ---------------------------------------------------------------------------
# Ahead of their turn
# ---------------------------------------------------------------------------

class TestAheadOfTheirTurn:

    @pytest.mark.parametrize("n_threads", [2, 3, 4, 8])
    @pytest.mark.parametrize("seed", range(6))
    def test_a_frame_is_the_frame_of_the_file_asked_for(self, n_threads, seed):
        files = some_files(30)
        fake = Reads(late=late_and_out_of_order(seed))
        with threads(n_threads), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files, columns=["id"])
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert sorted(fake.paths()) == [p for p, _ in files]            # each once
        assert all(c[1] == 100 and c[2] == ["id"] for c in fake.calls)  # as it always was read
        assert fake.most_at_once <= n_threads

    @pytest.mark.parametrize("n_threads", [2, 3, 8])
    def test_never_more_files_at_a_time_than_the_setting(self, n_threads):
        fake = Reads(late=lambda path: 0.002)
        with threads(n_threads), patch.object(proc, "_read_parquet_safe", fake):
            read_all(some_files(40))
        assert 2 <= fake.most_at_once <= n_threads

    def test_the_files_after_the_one_in_use_are_being_read(self):
        files, fake = some_files(6), Reads()
        first = fake.hold(files[0][0])

        def let_go_once_the_next_two_have_started():
            both = fake.started(files[1][0]).wait(A_WHILE) and fake.started(files[2][0]).wait(A_WHILE)
            first.set()
            assert both
        helper = threading.Thread(target=let_go_once_the_next_two_have_started)
        with threads(3), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                helper.start()
                frame = reads.read(*files[0])        # returns only once files 1 and 2 are being read
                helper.join(A_WHILE)
                assert frame["read_from"][0] == files[0][0]
                assert fake.started(files[1][0]).is_set() and fake.started(files[2][0]).is_set()
                # three at a time: the fourth is not touched before the second is asked for
                assert not fake.started(files[3][0]).is_set()
                assert reads.read(*files[1])["read_from"][0] == files[1][0]
                assert fake.started(files[3][0]).wait(A_WHILE)
            finally:
                reads.close()

    def test_the_threads_are_the_pools_and_are_kept(self):
        fake = Reads(late=lambda path: 0.002)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            read_all(some_files(20))
            pool = proc._read_ahead_executor()
            read_all(some_files(20))
            assert proc._read_ahead_executor() is pool
        assert all(r == MAIN or r.startswith("supertable-read") for r in fake.readers())
        assert any(r.startswith("supertable-read") for r in fake.readers())
        assert len(pool._threads) == proc._READ_AHEAD_MAX_THREADS     # all of them, from the first use on
        assert all(t.name.startswith("supertable-read") for t in pool._threads)

    def test_no_more_bytes_ahead_than_the_bound(self):
        fake = Reads(late=lambda path: 0.002)
        files = some_files(20, size=1000)
        with threads(8), patch.object(proc, "_READ_AHEAD_BYTES", 2500), \
                patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        # the file in use, and 2,000 bytes ahead of it; 3,000 would be over
        assert fake.most_at_once == 3
        assert max(fake.at_once_when_started[-6:]) == 3      # to the end: a file used is no longer ahead

    def test_files_that_add_up_to_the_bound_exactly_are_read_ahead_together(self):
        fake = Reads(late=lambda path: 0.002)
        files = some_files(12, size=1000)
        with threads(8), patch.object(proc, "_READ_AHEAD_BYTES", 2000), \
                patch.object(proc, "_read_parquet_safe", fake):
            read_all(files)
        assert fake.most_at_once == 3

    def test_a_file_larger_than_the_bound_is_read_on_its_own(self):
        fake = Reads(late=lambda path: 0.002)
        files = [("big-0", 5000), ("big-1", 5000), ("big-2", 5000), ("big-3", 5000)]
        with threads(8), patch.object(proc, "_READ_AHEAD_BYTES", 2500), \
                patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert fake.paths() == [p for p, _ in files] and fake.most_at_once == 1

    def test_small_files_are_read_ahead_of_a_large_one_in_use(self):
        fake = Reads(late=lambda path: 0.002)
        files = [("big", 5000)] + [(f"small-{i}", 100) for i in range(6)]
        with threads(4), patch.object(proc, "_READ_AHEAD_BYTES", 2500), \
                patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert fake.most_at_once == 4

    @pytest.mark.parametrize("size", [None, 0, -5, "many", float("nan"), float("inf")])
    def test_a_file_whose_size_is_not_stored_is_read_on_its_own(self, size):
        """What is not known cannot be bounded: such a file is never read ahead."""
        fake = Reads(late=lambda path: 0.002)
        files = [(f"data-{i}", size) for i in range(6)]
        with threads(8), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert fake.paths() == [p for p, _ in files] and fake.most_at_once == 1

    @pytest.mark.parametrize("size", [None, 0, "many", "12", 3.5, float("inf")])
    def test_a_size_is_passed_on_as_it_is(self, size):
        seen, lock = [], threading.Lock()

        def fake(path, profiler=None, file_size=0, columns=None, required=False):
            with lock:
                seen.append((path, file_size))
            return frame_of(path)
        files = [(f"data-{i}", size) for i in range(4)]
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert sorted(seen, key=lambda call: call[0]) == files

    def test_a_thread_is_asked_not_to_take_a_failure_for_an_absent_file(self):
        fake = Reads(late=lambda path: 0.001)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            read_all(some_files(12))
        on_threads = [required for _, name, required in fake.required if name != MAIN]
        assert on_threads and all(required is True for required in on_threads)

    def test_the_same_file_twice_is_read_twice_as_it_was(self):
        fake = Reads()
        files = [("a", 10), ("b", 10), ("a", 10), ("c", 10)]
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == ["a", "b", "a", "c"]
        assert sorted(fake.paths()) == ["a", "a", "b", "c"]


# ---------------------------------------------------------------------------
# Asked for otherwise than expected
# ---------------------------------------------------------------------------

class TestAskedOutOfTurn:

    def test_a_file_asked_for_out_of_turn_is_read_by_the_caller_and_gets_its_own_frame(self):
        fake = Reads()
        files = some_files(5)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                assert reads.read(*files[3])["read_from"][0] == files[3][0]     # not its turn
                assert fake.calls == [(files[3][0], 100, None, MAIN)]
                for path, size in files:                                         # the turns are kept
                    assert reads.read(path, size)["read_from"][0] == path
            finally:
                reads.close()

    def test_a_file_asked_for_out_of_turn_does_not_get_the_frame_that_is_ready(self):
        """File 1 is being read by a thread when file 3 is asked for: file 3 is read, by the caller."""
        fake = Reads()
        files = some_files(6)
        with threads(3), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                reads.read(*files[0])
                assert fake.started(files[1][0]).wait(A_WHILE)                  # a thread has file 1
                assert reads.read(*files[3])["read_from"][0] == files[3][0]
                assert (files[3][0], 100, None, MAIN) in fake.calls
                assert reads.read(*files[1])["read_from"][0] == files[1][0]     # and file 1 is still file 1
            finally:
                reads.close()

    def test_a_file_asked_for_with_another_size_does_not_get_the_frame_that_is_ready(self):
        fake = Reads()
        files = some_files(6)
        with threads(3), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                reads.read(*files[0])
                assert fake.started(files[1][0]).wait(A_WHILE)
                reads.read(files[1][0], 101)                                    # the same file, another size
                assert (files[1][0], 101, None, MAIN) in fake.calls
            finally:
                reads.close()

    def test_a_file_that_was_not_listed_is_read_by_the_caller(self):
        fake = Reads()
        files = some_files(4)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                assert reads.read("elsewhere.parquet", 7)["read_from"][0] == "elsewhere.parquet"
                assert fake.calls == [("elsewhere.parquet", 7, None, MAIN)]
                assert reads.read(*files[0])["read_from"][0] == files[0][0]
            finally:
                reads.close()

    def test_a_file_asked_for_with_another_size_is_read_with_that_size(self):
        fake = Reads()
        files = some_files(4)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                reads.read(files[0][0], 101)
                assert fake.calls == [(files[0][0], 101, None, MAIN)]
            finally:
                reads.close()

    def test_asking_past_the_last_file_reads_as_it_always_did(self):
        fake = Reads()
        files = some_files(3)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                for path, size in files:
                    reads.read(path, size)
                assert reads.read(*files[0])["read_from"][0] == files[0][0]
            finally:
                reads.close()
        assert fake.paths().count(files[0][0]) == 2


# ---------------------------------------------------------------------------
# A pool that is busy, or takes no work
# ---------------------------------------------------------------------------

class TestABusyPool:

    def test_a_read_no_thread_has_started_on_is_done_by_the_caller(self):
        """One thread, and it is busy: nothing waits for it."""
        busy = ThreadPoolExecutor(max_workers=1)
        let_go = threading.Event()
        blocker = busy.submit(let_go.wait, A_WHILE)
        fake, files = Reads(), some_files(6)
        try:
            with threads(4), patch.object(proc, "_read_ahead_executor", lambda: busy), \
                    patch.object(proc, "_read_parquet_safe", fake):
                started = time.perf_counter()
                frames = read_all(files)
                took = time.perf_counter() - started
            assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
            assert fake.calls == [(p, s, None, MAIN) for p, s in files]      # each once, by the caller
            assert took < A_WHILE / 2
        finally:
            let_go.set()
            blocker.result(A_WHILE)
            busy.shutdown(wait=True)
        assert len(fake.calls) == 6                                          # and none afterwards

    def test_a_pool_that_takes_no_work_leaves_the_files_to_the_caller(self):
        class Shut:
            asked = 0

            def submit(self, *a, **k):
                Shut.asked += 1
                raise RuntimeError("cannot schedule new futures after interpreter shutdown")
        fake, files = Reads(), some_files(5)
        with threads(4), patch.object(proc, "_read_ahead_executor", lambda: Shut()), \
                patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert fake.calls == [(p, s, None, MAIN) for p, s in files]
        assert Shut.asked == 1                                               # and it is not asked again

    def test_a_pool_that_stops_taking_work_keeps_what_it_took(self):
        real = ThreadPoolExecutor(max_workers=4)

        class StopsAfterTwo:
            taken = refused = 0

            def submit(self, *a, **k):
                if StopsAfterTwo.taken == 2:
                    StopsAfterTwo.refused += 1
                    raise RuntimeError("cannot schedule new futures after shutdown")
                StopsAfterTwo.taken += 1
                return real.submit(*a, **k)
        fake, files = Reads(late=lambda path: 0.002), some_files(7)
        try:
            with threads(4), patch.object(proc, "_read_ahead_executor", lambda: StopsAfterTwo()), \
                    patch.object(proc, "_read_parquet_safe", fake):
                frames = read_all(files)
        finally:
            real.shutdown(wait=True)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert sorted(fake.paths()) == [p for p, _ in files]
        assert StopsAfterTwo.taken == 2 and StopsAfterTwo.refused == 1       # refused once, not asked again

    def test_writers_that_share_the_pool_each_get_their_own_frames(self):
        """Eight writers at once, eight threads in the pool between them."""
        fake = Reads(late=late_and_out_of_order(3, most=0.002))
        wrong, done = [], []

        def writer(w):
            files = [(f"writer-{w}/data-{i:02d}", 10) for i in range(25)]
            frames = read_all(files)
            if [f["read_from"][0] for f in frames] != [p for p, _ in files]:
                wrong.append(w)
            done.append(w)
        with threads(8), patch.object(proc, "_read_parquet_safe", fake):
            ts = [threading.Thread(target=writer, args=(w,)) for w in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join(A_WHILE * 3)
        assert sorted(done) == list(range(8)) and wrong == []
        assert len(fake.calls) == 200 and len(set(fake.paths())) == 200
        pool_threads = {r for r in fake.readers() if r.startswith("supertable-read")}
        assert len(pool_threads) <= proc._READ_AHEAD_MAX_THREADS


# ---------------------------------------------------------------------------
# One write at a time
# ---------------------------------------------------------------------------

class TestOneWriteAtATime:
    """Writers that run at once already compete for the interpreter.  With reading threads on
    top, each of four such writes took 15% longer; a writer on its own gains."""

    def test_a_write_beside_another_reads_one_file_after_another(self):
        fake = Reads(late=lambda path: 0.003)
        first = [(f"first/{i}", 10) for i in range(8)]
        second = [(f"second/{i}", 10) for i in range(8)]
        third = [(f"third/{i}", 10) for i in range(8)]
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads(first, None, get_null_profiler()) as one:
                one.read(*first[0])
                with _CandidateReads(second, None, get_null_profiler()) as other:      # beside the first
                    frames = [other.read(p, sz) for p, sz in second]
                for path, size in first[1:]:
                    one.read(path, size)
            read_all(third)                                                            # on its own again
        by = lambda prefix: {r for p, _, _, r in fake.calls if p.startswith(prefix)}
        assert [f["read_from"][0] for f in frames] == [p for p, _ in second]
        assert by("second/") == {MAIN}
        assert any(r.startswith("supertable-read") for r in by("first/"))
        assert any(r.startswith("supertable-read") for r in by("third/"))

    def test_of_two_writers_that_start_together_one_reads_ahead(self):
        fake = Reads(late=lambda path: 0.003)
        together = threading.Barrier(2)

        def writer(name):
            files = [(f"{name}/{i}", 10) for i in range(10)]
            together.wait(A_WHILE)
            read_all(files)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            ts = [threading.Thread(target=writer, args=(name,), name=name) for name in ("w1", "w2")]
            for t in ts:
                t.start()
            for t in ts:
                t.join(A_WHILE)
        ahead = {p.split("/")[0] for p, _, _, r in fake.calls if r.startswith("supertable-read")}
        assert len(ahead) == 1                                   # one of the two, not both, not neither
        (the_other,) = {"w1", "w2"} - ahead
        assert {r for p, _, _, r in fake.calls if p.startswith(the_other)} == {the_other}
        assert len(fake.calls) == 20

    def test_a_write_of_one_file_keeps_nobody_from_reading_ahead(self):
        fake = Reads(late=lambda path: 0.003)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads([("single", 10)], None, get_null_profiler()) as single:
                single.read("single", 10)
                read_all(some_files(8))
        assert any(r.startswith("supertable-read") for r in fake.readers())

    @pytest.mark.parametrize("ends_with", [None, ValueError, KeyboardInterrupt, SystemExit])
    def test_a_write_that_has_ended_is_no_longer_beside_anyone(self, ends_with):
        fake = Reads(late=lambda path: 0.003)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            try:
                with _CandidateReads(some_files(6), None, get_null_profiler()) as reads:
                    reads.read("data-00.parquet", 100)
                    assert proc._candidate_passes == 1
                    if ends_with is not None:
                        raise ends_with()
            except BaseException as e:                           # noqa: BLE001 - the test's own
                assert type(e) is ends_with
            assert proc._candidate_passes == 0
            deadline = time.time() + A_WHILE                     # a stopped write does not wait for its reads
            while fake.at_once and time.time() < deadline:
                time.sleep(0.005)

    def test_closing_twice_counts_once(self):
        fake = Reads(late=lambda path: 0.003)
        later = [(f"later/{i}", 10) for i in range(8)]
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads(some_files(6), None, get_null_profiler()):       # still under way below
                other = _CandidateReads([("x/0", 10), ("x/1", 10)], None, get_null_profiler())
                other.close()
                other.close()
                assert proc._candidate_passes == 1
                read_all(later)                                  # beside the first: one after another
        assert {r for p, _, _, r in fake.calls if p.startswith("later/")} == {MAIN}

    def test_a_forked_child_has_no_write_under_way(self):
        with _CandidateReads(some_files(3), None, get_null_profiler()):
            assert proc._candidate_passes == 1
            kept = proc._read_ahead_pool, proc._read_ahead_pool_lock, proc._read_ahead_no_pool_until
            lock = proc._candidate_passes_lock
            try:
                proc._forget_read_ahead_pool()                   # what a fork does in the child
                assert proc._candidate_passes == 0
            finally:
                proc._read_ahead_pool, proc._read_ahead_pool_lock, proc._read_ahead_no_pool_until = kept
                proc._candidate_passes, proc._candidate_passes_lock = 1, lock


class Stop(BaseException):
    """What ``except Exception`` does not catch (a polars panic is one)."""


# ---------------------------------------------------------------------------
# A read that fails on a thread is not believed
# ---------------------------------------------------------------------------

class TestAReadThatFailsOnAThread:
    """``_read_parquet_safe`` takes a file that cannot be read for one that is not there, and a
    write then matches nothing in it.  A store asked for several files at once may refuse some."""

    def test_the_caller_reads_the_file_itself_and_the_rest_one_after_another(self):
        files = some_files(12)
        fake = Reads(late=lambda path: 0.003)     # a read takes a while: one handed over would be a thread's
        fake.fails_on_a_thread.add(files[1][0])
        with threads(2), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads(files, None, get_null_profiler()) as reads:
                frames = [reads.read(*files[0])]
                assert fake.started(files[1][0]).wait(A_WHILE)                   # a thread has tried file 1
                frames += [reads.read(path, size) for path, size in files[1:]]
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]      # file 1 too
        by_file = {}
        for path, _, _, reader in fake.calls:
            by_file.setdefault(path, []).append(reader)
        assert sorted(r == MAIN for r in by_file[files[1][0]]) == [False, True]   # tried on a thread, read here
        # two at a time: file 2 had been handed over; from file 3 on the caller reads alone
        assert all(by_file[p] == [MAIN] for p, _ in files[3:])
        assert all(len(readers) == 1 for p, readers in by_file.items() if p != files[1][0])

    def test_a_file_found_absent_on_a_thread_is_looked_for_again_by_the_caller(self):
        """A local ``stat`` that fails says "not there" exactly like one that finds nothing."""
        files = some_files(12)
        fake = Reads(late=lambda path: 0.003)
        fake.absent_on_a_thread.add(files[1][0])
        prof = Profiler()
        with threads(2), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads(files, None, prof) as reads:
                frames = [reads.read(*files[0])]
                assert fake.started(files[1][0]).wait(A_WHILE)                   # a thread has looked for file 1
                frames += [reads.read(path, size) for path, size in files[1:]]
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]      # file 1 is there
        readers = [r for p, _, _, r in fake.calls if p == files[1][0]]
        assert sorted(r == MAIN for r in readers) == [False, True]
        assert all(r == MAIN for p, _, _, r in fake.calls if p in {q for q, _ in files[3:]})
        assert prof.emit_counts() == {"files_read": 12, "bytes_read": 1200}      # counted once

    def test_a_file_that_is_absent_stays_absent(self):
        """Believed in the end, as it always was: on the caller's own word."""
        files = some_files(6)
        asked = []

        def fake(path, profiler=None, file_size=0, columns=None, required=False):
            asked.append((path, threading.current_thread().name, required))
            time.sleep(0.002)
            return None if path == files[2][0] else frame_of(path)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [None if f is None else f["read_from"][0] for f in frames] == \
               [p if p != files[2][0] else None for p, _ in files]
        assert [(name, required) for p, name, required in asked if p == files[2][0]][-1] == (MAIN, False)

    def test_the_callers_own_read_has_no_read_of_this_write_beside_it(self):
        """A thread's read of file 1 has failed while a thread is still reading file 2.  The caller
        reads file 1 itself only when the read of file 2 has ended."""
        files = some_files(6)
        fake = Reads()
        fake.fails_on_a_thread.add(files[1][0])
        second = fake.hold(files[2][0])                       # a thread's read of file 2, held
        beside = []
        fake.before[files[1][0]] = lambda required: (
            beside.append(fake.at_once - 1) if threading.current_thread().name == MAIN else None)
        let_go = threading.Timer(0.2, second.set)
        try:
            with threads(3), patch.object(proc, "_read_parquet_safe", fake):
                with _CandidateReads(files, None, get_null_profiler()) as reads:
                    frames = [reads.read(*files[0])]
                    assert fake.started(files[1][0]).wait(A_WHILE) and fake.started(files[2][0]).wait(A_WHILE)
                    let_go.start()
                    frames += [reads.read(path, size) for path, size in files[1:]]
        finally:
            second.set()
            let_go.cancel()
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert beside == [0]                                  # no other read was running

    def test_a_read_the_caller_makes_beside_others_is_not_believed_either(self):
        """No thread has started on file 0 when its turn comes, so the caller reads it, while
        files 1 and 2 start on their threads: one read of several at once.  The store refuses it.
        The caller lets the others end and reads file 0 again, alone; that read decides."""
        files, fake = some_files(4), Reads()
        pool = ThreadPoolExecutor(max_workers=2)
        let_go = threading.Event()
        blockers = [pool.submit(let_go.wait, A_WHILE) for _ in range(2)]
        attempts = []

        def the_store(required):
            attempts.append([threading.current_thread().name, required, None])
            if len(attempts) == 1:
                let_go.set()                                         # the threads are free: files 1 and 2 start
                assert fake.started(files[1][0]).wait(A_WHILE)
                raise RuntimeError("503 slow down")
            attempts[-1][2] = fake.at_once                           # reads running at the second attempt
        fake.before[files[0][0]] = the_store
        try:
            with threads(3), patch.object(proc, "_read_ahead_executor", lambda: pool), \
                    patch.object(proc, "_read_parquet_safe", fake):
                frames = read_all(files)
        finally:
            let_go.set()
            pool.shutdown(wait=True)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert attempts == [[MAIN, True, None], [MAIN, False, 1]]    # refused beside others; then alone, as always
        assert all(b.done() for b in blockers)

    def test_what_the_callers_own_read_raises_is_raised(self):
        """The stand-in raises where ``_read_parquet_safe`` would have returned None: either way
        it is the caller's read that decides."""
        files, fake = some_files(6), Reads()
        fake.raises[files[2][0]] = ValueError("bad file")
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            with _CandidateReads(files, None, get_null_profiler()) as reads:
                reads.read(*files[0])
                reads.read(*files[1])
                with pytest.raises(ValueError):
                    reads.read(*files[2])
        readers = [r for p, _, _, r in fake.calls if p == files[2][0]]
        assert readers[-1] == MAIN

    def test_a_failed_read_is_not_counted_twice(self):
        files, fake, prof = some_files(8), Reads(), Profiler()
        fake.fails_on_a_thread.update(p for p, _ in files[1:4])
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            read_all(files, profiler=prof)
        assert prof.emit_counts() == {"files_read": 8, "bytes_read": 800}


# ---------------------------------------------------------------------------
# A caller that is being stopped
# ---------------------------------------------------------------------------

class Interrupted(KeyboardInterrupt):
    """What a signal handler raises in the caller."""


@pytest.mark.skipif(not hasattr(signal, "pthread_kill") or threading.current_thread() is not threading.main_thread(),
                    reason="needs a signal sent to the main thread")
class TestACallerThatIsStopped:

    @pytest.fixture
    def interrupts(self):
        """SIGUSR1 raises ``Interrupted`` in the caller.  The handler is installed the way polars
        installs its own for SIGINT: a wait on a lock is restarted, not interrupted."""
        def handler(signum, frame):
            raise Interrupted()
        before = signal.signal(signal.SIGUSR1, handler)
        signal.siginterrupt(signal.SIGUSR1, False)
        yield lambda: signal.pthread_kill(threading.main_thread().ident, signal.SIGUSR1)
        signal.signal(signal.SIGUSR1, before)

    def test_the_caller_is_stopped_at_once_and_does_not_wait_for_the_read(self, interrupts):
        files, fake = some_files(6), Reads()
        second = fake.hold(files[1][0])                       # the read the caller will wait for

        def interrupt_once_it_waits():
            time.sleep(0.05)
            interrupts()
        helper = threading.Thread(target=interrupt_once_it_waits)
        try:
            with threads(3), patch.object(proc, "_read_parquet_safe", fake):
                with pytest.raises(Interrupted):
                    with _CandidateReads(files, None, get_null_profiler()) as reads:
                        reads.read(*files[0])
                        assert fake.started(files[1][0]).wait(A_WHILE)     # a thread has file 1, and is held
                        began = time.perf_counter()
                        helper.start()
                        reads.read(*files[1])                 # only the signal ends this wait
                took = time.perf_counter() - began
                assert took < 2.0, took                       # not when the read returned
                assert not second.is_set() and fake.at_once >= 1     # the read is still running
        finally:
            second.set()
            helper.join(A_WHILE)
        deadline = time.time() + A_WHILE
        while fake.at_once and time.time() < deadline:
            time.sleep(0.01)
        assert fake.at_once == 0                              # and it ends on its own

    @pytest.mark.parametrize("stopped_by, waits", [
        (KeyboardInterrupt, False), (SystemExit, False), (Interrupted, False),
        (ValueError, True), (Stop, True), (None, True),
    ])
    def test_which_callers_wait_for_the_reads_that_are_running(self, stopped_by, waits):
        files, fake = some_files(6), Reads()
        second = fake.hold(files[1][0])
        left = threading.Event()

        def use():
            try:
                with _CandidateReads(files, None, get_null_profiler()) as reads:
                    reads.read(*files[0])
                    assert fake.started(files[1][0]).wait(A_WHILE)
                    if stopped_by is not None:
                        raise stopped_by()
            except BaseException:                             # noqa: BLE001 - the test's own
                pass
            left.set()
        with threads(3), patch.object(proc, "_read_parquet_safe", fake):
            user = threading.Thread(target=use)
            user.start()
            try:
                assert left.wait(0.4) is (not waits)
            finally:
                second.set()
            user.join(A_WHILE)
            assert left.is_set()


# ---------------------------------------------------------------------------
# Threads that cannot be started
# ---------------------------------------------------------------------------

class TestThreadsThatCannotBeStarted:

    @pytest.fixture
    def no_pool_yet(self):
        """The process as it is before its first write: no pool."""
        kept, until = proc._read_ahead_pool, proc._read_ahead_no_pool_until
        proc._read_ahead_pool, proc._read_ahead_no_pool_until = None, 0.0
        yield
        made = proc._read_ahead_pool
        if made is not None and made is not kept:
            made.shutdown(wait=True)
        proc._read_ahead_pool, proc._read_ahead_no_pool_until = kept, until

    def _thread_limit(self, allowed):
        """``Thread.start`` as it is under a limit: the pool's first *allowed* threads start."""
        real_start, started = threading.Thread.start, []

        def start(thread):
            if thread.name.startswith("supertable-read"):
                if len(started) >= allowed:
                    raise RuntimeError("can't start new thread")
                started.append(thread)
            return real_start(thread)
        return patch.object(threading.Thread, "start", start), started

    @pytest.mark.parametrize("allowed", [0, 1, 5, 7])
    def test_the_files_are_read_one_after_another_and_none_is_left_behind(self, no_pool_yet, allowed):
        limit, started = self._thread_limit(allowed)
        files, fake = some_files(10), Reads()
        with threads(8), limit, patch.object(proc, "_read_parquet_safe", fake):
            frames = read_all(files)
        assert [f["read_from"][0] for f in frames] == [p for p, _ in files]
        assert fake.calls == [(p, s, None, MAIN) for p, s in files]      # each once, by the caller
        assert proc._read_ahead_pool is None
        for thread in started:                                           # what did start has ended
            thread.join(A_WHILE)
            assert not thread.is_alive()
        time.sleep(0.05)
        assert len(fake.calls) == 10                                     # and nothing is read afterwards

    def test_the_threads_are_not_asked_for_again_at_every_write(self, no_pool_yet):
        limit, _ = self._thread_limit(0)
        made = []
        real = proc.ThreadPoolExecutor

        def counted(*a, **k):
            made.append(1)
            return real(*a, **k)
        fake = Reads()
        with threads(8), limit, patch.object(proc, "ThreadPoolExecutor", counted), \
                patch.object(proc, "_read_parquet_safe", fake):
            for _ in range(5):
                read_all(some_files(4))
        assert len(made) == 1 and len(fake.calls) == 20
        assert all(r == MAIN for r in fake.readers())

    def test_they_are_asked_for_again_later(self, no_pool_yet):
        limit, _ = self._thread_limit(0)
        fake = Reads(late=lambda path: 0.002)
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            with limit:
                read_all(some_files(4))
            assert proc._read_ahead_pool is None
            proc._read_ahead_no_pool_until = time.monotonic() - 1       # the time has passed, the limit is gone
            read_all(some_files(8))
        assert proc._read_ahead_pool is not None
        assert any(r.startswith("supertable-read") for r in fake.readers())


# ---------------------------------------------------------------------------
# What a read raises, and what is left when the caller is done
# ---------------------------------------------------------------------------

class TestRaisedAndLeft:

    @pytest.mark.parametrize("error", [Stop("no further"), ValueError("bad file")])
    def test_what_a_read_raises_is_raised_when_its_file_is_asked_for(self, error):
        fake, files = Reads(late=lambda path: 0.001), some_files(6)
        fake.raises[files[2][0]] = error
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            try:
                assert reads.read(*files[0])["read_from"][0] == files[0][0]
                assert reads.read(*files[1])["read_from"][0] == files[1][0]   # file 2 has failed by now
                with pytest.raises(type(error)) as raised:
                    reads.read(*files[2])
                assert raised.value is error
                assert reads.read(*files[3])["read_from"][0] == files[3][0]   # and the rest is as it was
            finally:
                reads.close()

    def test_close_waits_for_a_read_that_is_running(self):
        fake, files = Reads(), some_files(6)
        second = fake.hold(files[1][0])
        with threads(3), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            reads.read(*files[0])
            assert fake.started(files[1][0]).wait(A_WHILE)          # a thread is on file 1
            closed = threading.Event()
            closer = threading.Thread(target=lambda: (reads.close(), closed.set()))
            closer.start()
            assert not closed.wait(0.15)                            # it waits for that read
            second.set()
            assert closed.wait(A_WHILE)
            closer.join(A_WHILE)
        assert fake.at_once == 0
        assert set(fake.paths()) <= {p for p, _ in files[:3]}       # nothing beyond what was handed over

    def test_close_takes_back_what_no_thread_has_started_on(self):
        busy = ThreadPoolExecutor(max_workers=1)
        fake, files = Reads(), some_files(5)
        second = fake.hold(files[1][0])
        try:
            with threads(4), patch.object(proc, "_read_ahead_executor", lambda: busy), \
                    patch.object(proc, "_read_parquet_safe", fake):
                reads = _CandidateReads(files, None, get_null_profiler())
                reads.read(*files[0])                               # read by the caller; file 1 by the one thread
                assert fake.started(files[1][0]).wait(A_WHILE)
                closer = threading.Thread(target=reads.close)
                closer.start()
                time.sleep(0.05)
                second.set()
                closer.join(A_WHILE)
                assert not closer.is_alive()
        finally:
            second.set()
            busy.shutdown(wait=True)
        assert sorted(fake.paths()) == [files[0][0], files[1][0]]   # files 2 and 3 were taken back

    def test_what_a_dropped_read_raised_is_not_raised(self):
        fake, files = Reads(), some_files(4)
        fake.raises[files[1][0]] = Stop("dropped")
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            reads.read(*files[0])
            assert fake.started(files[1][0]).wait(A_WHILE)           # a thread read file 1, and it failed
            reads.close()                                            # nobody asked for it
        assert fake.at_once == 0

    def test_after_close_a_file_is_read_by_the_caller_and_nothing_ahead_of_it(self):
        fake, files = Reads(), some_files(8)
        with threads(2), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, get_null_profiler())
            reads.read(*files[0])
            reads.close()
            before = len(fake.calls)
            assert reads.read(*files[1])["read_from"][0] == files[1][0]
            assert reads.read(*files[2])["read_from"][0] == files[2][0]
            assert not fake.started(files[3][0]).wait(0.2)           # nothing is read ahead any more
            assert fake.calls[before:] == [(files[1][0], 100, None, MAIN), (files[2][0], 100, None, MAIN)]
            reads.close()

    def test_a_read_is_counted_when_its_file_is_used(self):
        fake, files, prof = Reads(late=lambda path: 0.001), some_files(8), Profiler()
        with threads(4), patch.object(proc, "_read_parquet_safe", fake):
            reads = _CandidateReads(files, None, prof)
            reads.read(*files[0])
            reads.read(*files[1])
            reads.close()
        assert len(fake.calls) > 2                                   # more were read
        assert prof.emit_counts() == {"files_read": 2, "bytes_read": 200}

    def test_a_forked_child_has_no_pool(self):
        with threads(4), patch.object(proc, "_read_parquet_safe", Reads()):
            read_all(some_files(4))
        assert proc._read_ahead_pool is not None
        if not hasattr(os, "fork"):
            pytest.skip("no fork here")
        pid = os.fork()
        if pid == 0:                                                 # the child: look and leave
            os._exit(0 if proc._read_ahead_pool is None else 1)
        _, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
        assert proc._read_ahead_pool is not None                     # the parent keeps its own

    def test_forgetting_the_pool_makes_the_next_use_start_one(self):
        with threads(4), patch.object(proc, "_read_parquet_safe", Reads()):
            read_all(some_files(4))
            old = proc._read_ahead_pool
            try:
                proc._forget_read_ahead_pool()
                assert proc._read_ahead_pool is None
                frames = read_all(some_files(4))
                assert proc._read_ahead_pool is not None and proc._read_ahead_pool is not old
                assert len(frames) == 4
            finally:
                if proc._read_ahead_pool is not old:
                    proc._read_ahead_pool.shutdown(wait=True)
                proc._read_ahead_pool = old


# ---------------------------------------------------------------------------
# Through real files: the same frames, counters and errors at any number of threads
# ---------------------------------------------------------------------------

class Late(LocalStorage):
    """A local disk that answers late and out of order, the same way in every run."""

    def __init__(self, seed, slow=(), stops=(), refuses_over=None, stat_fails_beside_others=False):
        super().__init__()
        self._stat_fails = stat_fails_beside_others   # os.path.exists: False for any failure of stat
        self._refuses_over = refuses_over   # more requests than this at once: the newest is refused
        self.refused = 0
        self.failed = 0                     # reads that raised
        self._seconds = late_and_out_of_order(seed, most=0.002)
        self._lock = threading.Lock()
        self.read = []                  # (path, the columns asked for)
        self.readers = set()            # the threads that read
        self.asked = []
        self.at_once = 0
        self._slow = set(slow)          # these take a quarter of a second
        self._stops = set(stops)        # these raise what ``except Exception`` does not catch

    def exists(self, path):
        with self._lock:
            self.asked.append(path)
            self.at_once += 1
            refuse = self._refuses_over is not None and self.at_once > self._refuses_over
            self.refused += refuse
            stat_fails = self._stat_fails and self.at_once > 1
        try:
            if refuse:
                raise RuntimeError("503 SlowDown: reduce your request rate")
            time.sleep(0.001)
            return False if stat_fails else super().exists(path)
        finally:
            with self._lock:
                self.at_once -= 1

    def read_parquet(self, path, columns=None):
        with self._lock:
            self.read.append((path, tuple(columns) if columns else None))
            self.readers.add(threading.current_thread().name)
            self.at_once += 1
            refuse = self._refuses_over is not None and self.at_once > self._refuses_over
            self.refused += refuse
        try:
            if refuse:
                raise RuntimeError("503 SlowDown: reduce your request rate")
            time.sleep(0.25 if path in self._slow else self._seconds(path))
            if path in self._stops:
                raise Stop(path)
            return super().read_parquet(path, columns=columns)
        except BaseException:
            with self._lock:
                self.failed += 1
            raise
        finally:
            with self._lock:
                self.at_once -= 1


def random_table(directory, rng: random.Random):
    """A table of small files as writes leave them, and a few they should not.

    Most tables resolve.  One in five has a file whose key is a string, whose
    join raises; one in four has a file that cannot be read, which the write
    takes for a file that is not there.
    """
    two_keys = rng.random() < 0.3
    n_files = rng.randint(2, 14)
    span = rng.choice([20, 60, 400])
    string_key_in = rng.randrange(n_files) if rng.random() < 0.2 else None
    unreadable = rng.randrange(n_files) if rng.random() < 0.25 else None
    entries = []
    for i in range(n_files):
        rows = rng.randint(0, 30)
        kind = rng.random()
        key_type = pl.Utf8 if i == string_key_in else rng.choice([pl.Int64, pl.Int64, pl.Int64, pl.Int32])
        ids = [rng.randrange(span) for _ in range(rows)]
        columns = {
            "id": pl.Series("id", [str(v) for v in ids] if key_type == pl.Utf8 else ids, dtype=key_type),
            "region": pl.Series("region", [rng.choice(["n", "s", None]) for _ in range(rows)], dtype=pl.Utf8),
            "rev": pl.Series("rev", [rng.randrange(6) for _ in range(rows)], dtype=pl.Int64),
            "payload": pl.Series("payload", [f"f{i}r{r}" for r in range(rows)], dtype=pl.Utf8),
            ROWID_COL: pl.Series(ROWID_COL, [i * 1000 + r for r in range(rows)], dtype=pl.Int64),
        }
        if kind < 0.08:
            del columns[ROWID_COL]                       # written before row ids existed
        elif kind < 0.14:
            del columns["id"]                            # a file without the key
        elif kind < 0.20:
            del columns["rev"]                           # a file without the newer_than column
        path = directory / f"data-{i:02d}.parquet"
        if i == unreadable:
            path.write_bytes(b"PAR1 this is no parquet file PAR1")
            size = path.stat().st_size
        elif kind < 0.92:
            pq.write_table(pl.DataFrame(columns).to_arrow(), str(path))
            size = path.stat().st_size
        else:
            size = 321                                   # listed, and gone from the store
        entries.append((str(path), rng.random() < 0.9, size))
    if rng.random() < 0.2:
        entries.append(entries[rng.randrange(len(entries))])     # listed twice
    keys = ["id", "region"] if two_keys else ["id"]
    n_in = rng.randint(1, 25)
    incoming = pl.DataFrame({
        "id": pl.Series("id", [rng.randrange(span) for _ in range(n_in)], dtype=pl.Int64),
        "region": pl.Series("region", [rng.choice(["n", "s", None]) for _ in range(n_in)], dtype=pl.Utf8),
        "rev": pl.Series("rev", [rng.randrange(6) for _ in range(n_in)], dtype=pl.Int64),
        "payload": pl.Series("payload", ["new"] * n_in, dtype=pl.Utf8),
    })
    cannot_be_read = {entries[unreadable][0]} if unreadable is not None else set()
    return entries, keys, incoming, cannot_be_read


def same_reads(storage, reference, cannot_be_read) -> bool:
    """Every file that can be read was read as often as one after another: once.  A file that
    cannot be read may have been tried on a thread before the caller tried it itself."""
    def of(st, which):
        return sorted(e for e in st.read if (e[0] in cannot_be_read) == which)
    tried, tried_before = of(storage, True), of(reference, True)
    return (of(storage, False) == of(reference, False)
            and set(tried) == set(tried_before)
            and len(tried_before) <= len(tried) <= 2 * len(tried_before))


def outcome(call):
    """What a call returns or raises, with what it counted."""
    prof = Profiler()
    try:
        value = call(prof)
        raised = None
    except Exception as e:                               # noqa: BLE001 - compared, not handled
        value, raised = None, (type(e).__name__, str(e))
    counts = prof.emit_counts()
    spans = sorted(prof.emit_timings())
    return value, raised, counts, spans


def same_frames(a, b) -> bool:
    if a is None or b is None:
        return a is b
    return a.schema == b.schema and a.equals(b)


class TestThroughRealFiles:

    @pytest.mark.parametrize("seed", range(60))
    def test_the_matching_finds_the_same_pairs(self, tmp_path, seed):
        rng = random.Random(seed)
        entries, keys, incoming, cannot_be_read = random_table(tmp_path, rng)
        columns = keys + [ROWID_COL]
        cached = {}
        if rng.random() < 0.4:                           # some files are in the caller's hands already
            for path, _, _ in rng.sample(entries, k=max(1, len(entries) // 3)):
                if os.path.exists(path) and path not in cannot_be_read:
                    cached[path] = pl.read_parquet(path)

        def run(n_threads):
            storage = Late(seed)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                result = outcome(lambda prof: identify_deleted_rowids(
                    incoming, entries, keys, file_cache=dict(cached) if cached else None,
                    profiler=prof, read_columns=columns))
            return result, storage

        (pairs1, raised1, counts1, spans1), storage1 = run(1)
        assert all(asked == tuple(columns) for _, asked in storage1.read)    # only what the matching needs
        assert storage1.readers <= {MAIN}
        if raised1 is None:
            assert counts1.get("files_read", 0) == len(storage1.read) - storage1.failed   # every read is counted
        for n_threads in (2, 3, 8):
            (pairs, raised, counts, spans), storage = run(n_threads)
            assert all(asked == tuple(columns) for _, asked in storage.read)
            assert raised == raised1
            assert same_frames(pairs, pairs1)
            assert counts == counts1 and spans == spans1
            if raised1 is None:
                assert same_reads(storage, storage1, cannot_be_read)
                assert set(storage.asked) == set(storage1.asked)
            else:
                assert set(storage1.read) <= set(storage.read)                # more may have been read ahead

    @pytest.mark.parametrize("seed", range(60))
    def test_the_stale_filter_keeps_the_same_rows(self, tmp_path, seed):
        rng = random.Random(1000 + seed)
        entries, keys, incoming, cannot_be_read = random_table(tmp_path, rng)
        columns = keys + ["rev", ROWID_COL]
        dead = None
        if rng.random() < 0.6:                           # a deletion vector: a frame of row ids, or a set
            gone = [rng.randrange(14) * 1000 + rng.randrange(30) for _ in range(60)]
            dead = pl.DataFrame({ROWID_COL: gone}, schema={ROWID_COL: pl.Int64}) if rng.random() < 0.5 else set(gone)

        def run(n_threads):
            storage, cache = Late(seed), {}
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                result = outcome(lambda prof: filter_stale_incoming_rows(
                    incoming, entries, keys, "rev", file_cache=cache, profiler=prof,
                    read_columns=columns, dead_rowids=dead))
            return result, storage, cache

        (kept1, raised1, counts1, spans1), storage1, cache1 = run(1)
        assert all(asked == tuple(columns) for _, asked in storage1.read)    # only what the filter needs
        assert storage1.readers <= {MAIN}
        if raised1 is None:
            assert counts1.get("files_read", 0) == len(storage1.read) - storage1.failed   # every read is counted
        for n_threads in (2, 3, 8):
            (kept, raised, counts, spans), storage, cache = run(n_threads)
            assert all(asked == tuple(columns) for _, asked in storage.read)
            assert raised == raised1
            assert same_frames(kept, kept1)
            assert counts == counts1 and spans == spans1
            if raised1 is None:
                assert same_reads(storage, storage1, cannot_be_read)
                assert list(cache) == list(cache1)                           # filled in the same order
                assert all(same_frames(cache[p], cache1[p]) for p in cache1)

    @pytest.mark.parametrize("seed", range(60))
    @pytest.mark.parametrize("newer_than", [None, "rev"])
    def test_a_write_resolves_to_the_same(self, tmp_path, seed, newer_than):
        rng = random.Random(2000 + seed)
        entries, keys, incoming, cannot_be_read = random_table(tmp_path, rng)

        def run(n_threads):
            storage = Late(seed)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                result = outcome(lambda prof: resolve_overwrite_writes(
                    incoming_df=incoming, overlapping_files=entries, overwrite_columns=keys,
                    newer_than_col=newer_than, profiler=prof))
            return result, storage

        (value1, raised1, counts1, spans1), storage1 = run(1)
        for n_threads in (2, 8):
            (value, raised, counts, spans), storage = run(n_threads)
            assert raised == raised1
            assert counts == counts1 and spans == spans1
            if raised1 is None:
                assert same_frames(value[0], value1[0]) and same_frames(value[1], value1[1])
                assert same_reads(storage, storage1, cannot_be_read)

    def test_most_of_the_random_tables_resolve(self, tmp_path):
        """What the three tests above compare is mostly results, not errors."""
        resolved = with_a_file_that_cannot_be_read = 0
        for seed in range(60):
            directory = tmp_path / str(seed)
            directory.mkdir()
            entries, keys, incoming, cannot_be_read = random_table(directory, random.Random(seed))
            with threads(1), patch("supertable.processing._get_storage", return_value=LocalStorage()):
                _, raised, _, _ = outcome(lambda prof: identify_deleted_rowids(
                    incoming, entries, keys, profiler=prof, read_columns=keys + [ROWID_COL]))
            resolved += raised is None
            with_a_file_that_cannot_be_read += bool(cannot_be_read) and raised is None
        assert resolved >= 40 and with_a_file_that_cannot_be_read >= 5

    @pytest.mark.parametrize("again", range(5))
    def test_a_store_that_refuses_several_requests_at_once_loses_no_row(self, tmp_path, again):
        """Forty files with one matching row each, and a store that refuses a request when more
        than three are on their way.  A refused request is a file that "is not there" to the code
        that reads it; believed, the delete would miss its row."""
        entries = []
        for i in range(40):
            path = tmp_path / f"data-{i:02d}.parquet"
            pq.write_table(pl.DataFrame({"id": [i, 1000 + i], ROWID_COL: [i * 10, i * 10 + 1]}).to_arrow(), str(path))
            entries.append((str(path), True, path.stat().st_size))
        incoming = pl.DataFrame({"id": list(range(40))})

        def run(n_threads):
            storage = Late(5 + again, refuses_over=3)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                pairs = identify_deleted_rowids(incoming, entries, ["id"], read_columns=["id", ROWID_COL])
            return pairs, storage

        pairs1, storage1 = run(1)
        assert pairs1.height == 40 and storage1.refused == 0
        for n_threads in (2, 4, 8):
            pairs, storage = run(n_threads)
            assert same_frames(pairs, pairs1), (n_threads, pairs.height)
        assert storage.refused > 0                                   # at eight it did refuse

    @pytest.mark.parametrize("newer_than", [None, "rev"])
    def test_a_stat_that_fails_beside_another_request_loses_no_row(self, tmp_path, newer_than):
        """``os.path.exists`` is False for any failure of ``stat`` (EIO, ESTALE on a network mount),
        so on LOCAL a failure cannot be told from a file that is not there.  Here it fails whenever
        another request is on its way."""
        entries = []
        for i in range(40):
            path = tmp_path / f"data-{i:02d}.parquet"
            frame = pl.DataFrame({"id": [i, 1000 + i], "rev": [1, 1], ROWID_COL: [i * 10, i * 10 + 1]})
            pq.write_table(frame.to_arrow(), str(path))
            entries.append((str(path), True, path.stat().st_size))
        incoming = pl.DataFrame({"id": list(range(40)), "rev": [2] * 40})

        def run(n_threads):
            storage = Late(9, stat_fails_beside_others=True)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                return resolve_overwrite_writes(incoming_df=incoming, overlapping_files=entries,
                                                overwrite_columns=["id"], newer_than_col=newer_than)

        filtered1, pairs1 = run(1)
        assert pairs1.height == 40 and filtered1.height == 40
        for n_threads in (2, 4, 8):
            filtered, pairs = run(n_threads)
            assert same_frames(pairs, pairs1), (n_threads, pairs.height)
            assert same_frames(filtered, filtered1)

    def test_a_set_of_candidates_resolves_to_the_same_pairs(self, tmp_path):
        rng = random.Random(7)
        entries, keys, incoming, cannot_be_read = random_table(tmp_path, rng)
        as_set = set(entries)
        with patch("supertable.processing._get_storage", return_value=LocalStorage()):
            with threads(1):
                pairs1 = identify_deleted_rowids(incoming, as_set, keys, read_columns=keys + [ROWID_COL])
            with threads(8):
                pairs8 = identify_deleted_rowids(incoming, as_set, keys, read_columns=keys + [ROWID_COL])
        assert same_frames(pairs8, pairs1)

    def test_a_write_that_fails_at_a_file_has_counted_the_files_up_to_it(self, tmp_path):
        """File 3 has a string key where the incoming one is a number: the join of it raises.

        A file is joined once the next file with other types has been read
        (files are matched several at a time), so the write stops after file 4.
        """
        entries = []
        for i in range(8):
            ids = [str(i * 10 + j) for j in range(5)] if i == 3 else [i * 10 + j for j in range(5)]
            frame = pl.DataFrame({"id": ids, ROWID_COL: [i * 100 + j for j in range(5)]})
            path = tmp_path / f"data-{i}.parquet"
            pq.write_table(frame.to_arrow(), str(path))
            entries.append((str(path), True, path.stat().st_size))
        incoming = pl.DataFrame({"id": [1, 31, 71]})

        def run(n_threads):
            storage = Late(0)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                return outcome(lambda prof: identify_deleted_rowids(
                    incoming, entries, ["id"], profiler=prof, read_columns=["id", ROWID_COL])), storage

        (_, raised1, counts1, _), storage1 = run(1)
        assert raised1 is not None and counts1["files_read"] == 5       # files 0 to 4, of eight
        for n_threads in (2, 8):
            (_, raised, counts, _), storage = run(n_threads)
            assert raised == raised1 and counts == counts1
            assert len(storage.read) >= 5                               # more may have been read, none counted

    @pytest.mark.parametrize("n_threads", [2, 8])
    def test_both_loops_read_ahead(self, tmp_path, n_threads):
        """Not only the same result: the files are read by the pool's threads, in both functions."""
        entries = self._eight_files(tmp_path)
        incoming = pl.DataFrame({"id": [1], "rev": [2]})
        for call in (
            lambda: identify_deleted_rowids(incoming, entries, ["id"], read_columns=["id", ROWID_COL]),
            lambda: filter_stale_incoming_rows(incoming, entries, ["id"], "rev", file_cache={},
                                               read_columns=["id", "rev", ROWID_COL]),
        ):
            storage = Late(4)
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                call()
            assert len(storage.read) == 8
            assert any(name.startswith("supertable-read") for name in storage.readers)

    def _eight_files(self, tmp_path, odd_one=None):
        entries = []
        for i in range(8):
            ids = [str(j) for j in range(5)] if i == odd_one else list(range(5))
            frame = pl.DataFrame({"id": ids, "rev": [1] * 5, ROWID_COL: [i * 100 + j for j in range(5)]})
            path = tmp_path / f"data-{i}.parquet"
            pq.write_table(frame.to_arrow(), str(path))
            entries.append((str(path), True, path.stat().st_size))
        return entries

    def test_nothing_is_being_read_once_the_matching_has_failed(self, tmp_path):
        """The join of file 1 raises when file 2 has been read; files 5 to 7 are slow and on their way."""
        entries = self._eight_files(tmp_path, odd_one=1)
        storage = Late(1, slow=[e[0] for e in entries[5:]])
        with threads(8), patch("supertable.processing._get_storage", return_value=storage):
            with pytest.raises(Exception):
                identify_deleted_rowids(pl.DataFrame({"id": [1]}), entries, ["id"],
                                        read_columns=["id", ROWID_COL])
            assert storage.at_once == 0
            read_when_it_failed = len(storage.read)
            time.sleep(0.05)
            assert len(storage.read) == read_when_it_failed

    def test_nothing_is_being_read_once_the_stale_filter_has_stopped(self, tmp_path):
        """The read of file 2 raises what is not caught; files 5 to 7 are slow and on their way."""
        entries = self._eight_files(tmp_path)
        storage = Late(2, slow=[e[0] for e in entries[5:]], stops=[entries[2][0]])
        incoming = pl.DataFrame({"id": [1], "rev": [2]})
        with threads(8), patch("supertable.processing._get_storage", return_value=storage):
            with pytest.raises(Stop):
                filter_stale_incoming_rows(incoming, entries, ["id"], "rev", file_cache={},
                                           read_columns=["id", "rev", ROWID_COL])
            assert storage.at_once == 0
            read_when_it_stopped = len(storage.read)
            time.sleep(0.05)
            assert len(storage.read) == read_when_it_stopped

    def test_what_is_not_caught_is_raised_at_any_number_of_threads(self, tmp_path):
        entries = self._eight_files(tmp_path)
        counted = {}
        for n_threads in (1, 2, 8):
            storage = Late(3, stops=[entries[4][0]])
            prof = Profiler()
            with threads(n_threads), patch("supertable.processing._get_storage", return_value=storage):
                with pytest.raises(Stop) as raised:
                    identify_deleted_rowids(pl.DataFrame({"id": [1]}), entries, ["id"], profiler=prof,
                                            read_columns=["id", ROWID_COL])
            assert str(raised.value) == entries[4][0]
            counted[n_threads] = prof.emit_counts()
            assert counted[n_threads]["files_read"] == 4                # the four before it
            # the read that stopped was made once, not tried again by the caller
            assert [p for p, _ in storage.read].count(entries[4][0]) == 1
        # everything the reads counted, the read that stopped included (it had asked whether the file exists)
        assert counted[2] == counted[1] and counted[8] == counted[1]
        assert counted[1]["io.exists.n"] == 5 and counted[1]["io.read_parquet.n"] == 5
