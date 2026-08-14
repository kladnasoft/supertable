"""In-process cache for the tombstone (deletion-vector) artifact.

Symmetric to the stats cache: the deletion-vector parquet is read on every
overwrite/delete to carry the vector forward.  ``load_tombstone`` keeps the
*latest* version of each table's DV in memory so a process writing in a loop
skips that carry-forward read, while time-travel reads of older versions stay
uncached and never evict the cached latest.

Contract under test mirrors test_stats_cache.py, plus:
  * ``required`` is forwarded to the underlying reader (a genuine read failure
    of an existing object must abort, never silently truncate the vector);
  * a cache hit never re-reads, so it can never truncate.
"""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pytest

from supertable import processing
from supertable.processing import (
    load_tombstone,
    cache_tombstone,
    tombstone_cache_identity,
    _TOMBSTONE_CACHE,
)

_MOD = "supertable.processing"


def _dv(n: int = 1) -> pl.DataFrame:
    """A minimal deletion-vector frame: (__file__, __rowid__)."""
    return pl.DataFrame({"__file__": [f"f{n}.parquet"], "__rowid__": [n]})


@pytest.fixture(autouse=True)
def _clear_cache():
    _TOMBSTONE_CACHE.clear()
    yield
    _TOMBSTONE_CACHE.clear()


class TestLoadTombstone:

    def test_none_path_returns_none_without_read(self):
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_tombstone(None) is None
            mock_read.assert_not_called()

    def test_first_read_misses_then_caches(self):
        df = _dv(1)
        with patch(f"{_MOD}._read_parquet_safe", return_value=df) as mock_read:
            out1 = load_tombstone("t/tombstone/v1.parquet")
            out2 = load_tombstone("t/tombstone/v1.parquet")
        assert out1 is df and out2 is df
        # Second call served from cache → storage read happened exactly once.
        assert mock_read.call_count == 1

    def test_cache_hit_returns_same_object(self):
        df = _dv(1)
        cache_tombstone("t/tombstone/v1.parquet", df)
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            out = load_tombstone("t/tombstone/v1.parquet")
            mock_read.assert_not_called()
        assert out is df

    def test_sealed_cache_hit_does_not_rescan_or_rehash_vector(self):
        df = _dv(1)
        digest = processing.tombstone_digest(df)
        cache_tombstone(
            "t/tombstone/v1.parquet",
            df,
            expected_rows=1,
            expected_digest=digest,
            assume_valid=True,
        )

        with (
            patch(f"{_MOD}.validate_tombstone_frame", side_effect=AssertionError("scan")),
            patch(f"{_MOD}._tombstone_digest_validated", side_effect=AssertionError("hash")),
            patch(f"{_MOD}._read_parquet_safe", side_effect=AssertionError("read")),
        ):
            out = load_tombstone(
                "t/tombstone/v1.parquet",
                required=True,
                expected_rows=1,
                expected_digest=digest,
                allowed_files={"f1.parquet"},
            )
        assert out is df

    def test_sealed_cache_hit_rejects_conflicting_snapshot_contract(self):
        df = _dv(1)
        digest = processing.tombstone_digest(df)
        cache_tombstone(
            "t/tombstone/v1.parquet",
            df,
            expected_rows=1,
            expected_digest=digest,
            assume_valid=True,
        )

        with pytest.raises(ValueError, match="row-count mismatch"):
            load_tombstone("t/tombstone/v1.parquet", expected_rows=2)
        with pytest.raises(ValueError, match="digest mismatch"):
            load_tombstone(
                "t/tombstone/v1.parquet", expected_digest="0" * 64,
            )
        with pytest.raises(ValueError, match="outside the current snapshot"):
            load_tombstone(
                "t/tombstone/v1.parquet", allowed_files={"other.parquet"},
            )

    def test_new_version_misses_and_replaces_latest(self):
        v1, v2 = _dv(1), _dv(2)
        cache_tombstone("t/tombstone/v1.parquet", v1)
        with patch(f"{_MOD}._read_parquet_safe", return_value=v2) as mock_read:
            out = load_tombstone("t/tombstone/v2.parquet")  # newer version → miss
            assert mock_read.call_count == 1
        assert out is v2
        # v2 is now the cached latest; v1's path no longer hits.
        with patch(f"{_MOD}._read_parquet_safe", return_value=v1) as mock_read:
            again = load_tombstone("t/tombstone/v1.parquet")
            assert mock_read.call_count == 1  # had to re-read the old version
        assert again is v1

    def test_allow_cache_false_does_not_evict_latest(self):
        latest, old = _dv(2), _dv(1)
        cache_tombstone("t/tombstone/v2.parquet", latest)
        with patch(f"{_MOD}._read_parquet_safe", return_value=old) as mock_read:
            out = load_tombstone("t/tombstone/v1.parquet", allow_cache=False)
            assert out is old and mock_read.call_count == 1
        # Latest still cached → no read.
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_tombstone("t/tombstone/v2.parquet") is latest
            mock_read.assert_not_called()

    def test_required_is_forwarded_to_reader(self):
        with patch(f"{_MOD}._read_parquet_safe", return_value=_dv(1)) as mock_read:
            load_tombstone("t/tombstone/v1.parquet", required=True)
        _, kwargs = mock_read.call_args
        assert kwargs.get("required") is True

    def test_required_defaults_false(self):
        with patch(f"{_MOD}._read_parquet_safe", return_value=_dv(1)) as mock_read:
            load_tombstone("t/tombstone/v1.parquet")
        _, kwargs = mock_read.call_args
        assert kwargs.get("required") is False

    def test_required_failure_propagates(self):
        # A genuine read failure with required=True must re-raise (abort the
        # write) rather than be swallowed to None (which would truncate the DV).
        with patch(f"{_MOD}._read_parquet_safe", side_effect=OSError("corrupt")):
            with pytest.raises(OSError):
                load_tombstone("t/tombstone/v1.parquet", required=True)

    def test_failed_read_not_cached(self):
        with patch(f"{_MOD}._read_parquet_safe", return_value=None) as mock_read:
            assert load_tombstone("t/tombstone/v1.parquet") is None
            assert load_tombstone("t/tombstone/v1.parquet") is None
            assert mock_read.call_count == 2  # None never cached → re-read

    def test_per_table_keys_do_not_collide(self):
        a, b = _dv(1), _dv(2)
        cache_tombstone("tableA/tombstone/v1.parquet", a)
        cache_tombstone("tableB/tombstone/v1.parquet", b)
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_tombstone("tableA/tombstone/v1.parquet") is a
            assert load_tombstone("tableB/tombstone/v1.parquet") is b
            mock_read.assert_not_called()

    def test_profiler_counts_hit_and_miss(self):
        from supertable.utils.profiler import Profiler
        df = _dv(1)
        prof = Profiler()
        with patch(f"{_MOD}._read_parquet_safe", return_value=df):
            load_tombstone("t/tombstone/v1.parquet", profiler=prof)  # miss
            load_tombstone("t/tombstone/v1.parquet", profiler=prof)  # hit
        counts = prof.emit_counts()
        assert counts.get("tombstone_cache_miss") == 1
        assert counts.get("tombstone_cache_hit") == 1

    def test_cache_tombstone_none_args_are_noops(self):
        cache_tombstone(None, _dv(1))                       # no path → no-op
        cache_tombstone("t/tombstone/v1.parquet", None)     # no frame → no-op
        with patch(f"{_MOD}._read_parquet_safe", return_value=_dv(9)) as mock_read:
            load_tombstone("t/tombstone/v1.parquet")
            assert mock_read.call_count == 1  # nothing was cached


class TestTombstoneCacheBounds:

    def test_lru_eviction_caps_table_count(self):
        with patch(f"{_MOD}.settings") as mock_settings:
            mock_settings.SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES = 2
            cache_tombstone("t1/tombstone/v.parquet", _dv(1))
            cache_tombstone("t2/tombstone/v.parquet", _dv(2))
            cache_tombstone("t3/tombstone/v.parquet", _dv(3))  # evicts t1 (LRU)
            with patch(f"{_MOD}._read_parquet_safe", return_value=_dv(9)) as mock_read:
                load_tombstone("t1/tombstone/v.parquet")       # evicted → read
                assert mock_read.call_count == 1
            with patch(f"{_MOD}._read_parquet_safe") as mock_read:
                load_tombstone("t3/tombstone/v.parquet")       # still cached → hit
                mock_read.assert_not_called()

    def test_zero_cap_disables_cache(self):
        with patch(f"{_MOD}.settings") as mock_settings:
            mock_settings.SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES = 0
            cache_tombstone("t/tombstone/v1.parquet", _dv(1))  # no-op
            with patch(f"{_MOD}._read_parquet_safe", return_value=_dv(1)) as mock_read:
                load_tombstone("t/tombstone/v1.parquet")
                load_tombstone("t/tombstone/v1.parquet")
                assert mock_read.call_count == 2  # never cached
def test_explicit_cache_identity_reuses_frame_across_rotating_read_paths(monkeypatch):
    frame = _dv(1)
    calls = []

    def loader():
        calls.append(1)
        return frame

    first = load_tombstone(
        "https://signed-one.invalid/dv",
        cache_identity="org:storage:raw/dv:digest",
        loader=loader,
    )
    second = load_tombstone(
        "https://signed-two.invalid/dv",
        cache_identity="org:storage:raw/dv:digest",
        loader=lambda: (_ for _ in ()).throw(AssertionError("cache miss")),
    )

    assert first is second
    assert calls == [1]


def test_tombstone_cache_identity_isolates_organization_and_auth_scope():
    class ScopedStorage:
        def __init__(self, access_key: str):
            self._aws_access_key_id = access_key

        @staticmethod
        def cache_namespace():
            return {"provider": "test", "bucket": "shared"}

        @staticmethod
        def is_local_storage():
            return False

    path = "same/table/tombstone/v1.parquet"
    storage_a = ScopedStorage("principal-a")
    storage_b = ScopedStorage("principal-b")
    key_a = tombstone_cache_identity(
        path, organization="org-a", storage=storage_a,
    )
    assert key_a == tombstone_cache_identity(
        path, organization="org-a", storage=storage_a,
    )
    assert key_a != tombstone_cache_identity(
        path, organization="org-b", storage=storage_a,
    )
    assert key_a != tombstone_cache_identity(
        path, organization="org-a", storage=storage_b,
    )


def test_same_raw_tombstone_key_cannot_alias_across_auth_scopes():
    class ScopedStorage:
        def __init__(self, access_key: str):
            self._aws_access_key_id = access_key

        @staticmethod
        def cache_namespace():
            return {"provider": "test", "bucket": "shared"}

        @staticmethod
        def is_local_storage():
            return False

    path = "same/table/tombstone/v1.parquet"
    identity_a = tombstone_cache_identity(
        path, organization="org", storage=ScopedStorage("principal-a"),
    )
    identity_b = tombstone_cache_identity(
        path, organization="org", storage=ScopedStorage("principal-b"),
    )
    frame_a, frame_b = _dv(1), _dv(2)
    cache_tombstone(path, frame_a, cache_identity=identity_a)
    calls = []

    def load_b():
        calls.append(1)
        return frame_b

    first_b = load_tombstone(
        path, cache_identity=identity_b, loader=load_b,
    )
    second_b = load_tombstone(
        path,
        cache_identity=identity_b,
        loader=lambda: (_ for _ in ()).throw(AssertionError("cache miss")),
    )
    still_a = load_tombstone(
        path,
        cache_identity=identity_a,
        loader=lambda: (_ for _ in ()).throw(AssertionError("cache miss")),
    )

    assert first_b is frame_b
    assert second_b is frame_b
    assert still_a is frame_a
    assert calls == [1]
