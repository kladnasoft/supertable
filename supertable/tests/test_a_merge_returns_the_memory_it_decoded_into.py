"""
A merge hands the memory it decoded its files into back to the system.

The files a merge reads are decoded into Arrow's memory pool.  The pool keeps
freed memory for its own reuse and returns nothing by itself, and nothing in a
write allocates on that scale again until the next merge, so a writer process
stayed as large as its largest merge so far.  A merge of 2,000,000 rows left
350 MB of freed buffers with the process; ``release_unused`` returned all of
it in 50 ms.

``compact_resources`` asks for that once, after its last file is written and
its own references to the frames are gone, and only if it read something.
"""
from __future__ import annotations

import os
from typing import Dict, List
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import _release_merge_memory, compact_resources  # noqa: E402
from supertable.utils.profiler import Profiler  # noqa: E402


@pytest.fixture
def lake(tmp_path):
    from supertable.storage.local_storage import LocalStorage

    data_dir = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(data_dir, exist_ok=True)
    proc._storage = None
    events: List[str] = []
    real_write = proc.write_parquet_and_collect_resources

    def recording_write(**kwargs):
        events.append("write")
        return real_write(**kwargs)

    with patch.object(proc, "get_storage", return_value=LocalStorage()), \
            patch.object(proc, "write_parquet_and_collect_resources", recording_write), \
            patch.object(proc, "_release_merge_memory",
                         side_effect=lambda: events.append("release")):
        yield type("Lake", (), {"data_dir": data_dir, "events": events})
    proc._storage = None


def _file(lake, name: str, first: int, rows: int = 5, *, size: int | None = None) -> Dict:
    frame = pl.DataFrame({"id": list(range(first, first + rows))})
    path = os.path.join(lake.data_dir, name)
    frame.write_parquet(path)
    return {"file": path, "file_size": size if size is not None else os.path.getsize(path),
            "rows": rows, "columns": 1}


class TestWhenAMergeAsksForIt:

    def test_once_after_the_file_is_written(self, lake):
        files = [_file(lake, f"f{i}.parquet", i * 5) for i in range(3)]

        compact_resources(snapshot={"resources": files}, data_dir=lake.data_dir,
                          compression_level=1)

        assert lake.events == ["write", "release"]

    def test_once_for_a_merge_of_several_chunks_after_the_last_of_them(self, lake):
        files = [_file(lake, f"f{i}.parquet", i * 5, size=6000) for i in range(5)]

        _, _, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10_000})

        assert len(written) == 3
        assert lake.events == ["write", "write", "write", "release"]

    def test_not_when_there_was_nothing_to_merge(self, lake):
        large = _file(lake, "large.parquet", 0, size=50_000_000)

        for snapshot in ({"resources": []}, {"resources": None}, {"resources": [large]}):
            assert compact_resources(snapshot=snapshot, data_dir=lake.data_dir,
                                     compression_level=1) == (0, 0, [], set())

        assert lake.events == []

    def test_not_when_no_file_could_be_read(self, lake):
        missing = {"file": os.path.join(lake.data_dir, "gone.parquet"), "file_size": 100,
                   "rows": 5, "columns": 1}

        considered, rows, written, sunset = compact_resources(
            snapshot={"resources": [missing]}, data_dir=lake.data_dir, compression_level=1)

        assert (considered, rows, written, sunset) == (0, 0, [], set())
        assert lake.events == []

    def test_the_time_it_takes_is_recorded_with_the_write_s_other_timings(self, lake):
        files = [_file(lake, f"f{i}.parquet", i * 5) for i in range(2)]
        profiler = Profiler()

        compact_resources(snapshot={"resources": files}, data_dir=lake.data_dir,
                          compression_level=1, profiler=profiler)

        assert profiler.counts.get("compact.release_memory.n") == 1
        assert "compact.release_memory" in profiler.timings


class TestTheRequestItself:

    def test_it_asks_arrow_s_default_pool_to_release_what_it_does_not_use(self):
        pool = MagicMock()
        with patch.object(proc.pa, "default_memory_pool", return_value=pool):
            _release_merge_memory()
        pool.release_unused.assert_called_once_with()

    def test_a_pool_that_fails_does_not_fail_the_merge(self):
        pool = MagicMock()
        pool.release_unused.side_effect = RuntimeError("not supported")
        with patch.object(proc.pa, "default_memory_pool", return_value=pool):
            _release_merge_memory()                      # no exception
        with patch.object(proc.pa, "default_memory_pool", side_effect=RuntimeError("no pool")):
            _release_merge_memory()                      # no exception

    def test_the_real_pool_accepts_it_and_buffers_in_use_are_untouched(self):
        """Frames still referenced must read the same after a release."""
        import pyarrow as pa
        table = pa.table({"id": list(range(10_000)), "name": [f"r{i}" for i in range(10_000)]})
        frame = pl.from_arrow(table)

        _release_merge_memory()

        assert frame["id"].sum() == sum(range(10_000))
        assert frame["name"][9_999] == "r9999"
        assert table.column("id").to_pylist()[-1] == 9_999
