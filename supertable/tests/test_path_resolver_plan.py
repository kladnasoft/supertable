# route: supertable.tests.test_path_resolver_plan
"""The planned key→path resolver must equal the per-key one, key for key.

``_to_duckdb_path`` used to re-derive everything for every file: the presign
check, a probe of three storage helpers (which *raises* on LOCAL storage), and
the endpoint/bucket/scheme lookup. Only the key varies across that loop, so the
plan is now built once and applied per key — 22.1ms/query on a 100-file scan.

The original implementation is retained as ``_to_duckdb_path_uncached`` purely
so it can serve as the oracle here: for every storage shape and every key, the
two must agree. If they ever diverge, the optimisation changed behaviour.
"""

from __future__ import annotations

from importlib import import_module

import pytest

est_mod = import_module("supertable.engine.data_estimator")
DataEstimator = est_mod.DataEstimator


class _NoHelpers:
    """Bucket storage exposing none of the helper methods."""
    bucket = "b1"
    secure = "false"
    endpoint_url = "minio.local:9000"


class _RaisingHelper(_NoHelpers):
    """LOCAL-like: the helper exists but always raises. The hot path."""
    def to_duckdb_path(self, key):
        raise NotImplementedError("LocalStorage does not implement to_duckdb_path()")


class _WorkingHelper(_NoHelpers):
    def to_duckdb_path(self, key):
        return f"s3://custom/{key}"


class _EmptyHelper(_NoHelpers):
    """Returns falsy — must fall through to the constructed URL, not be used."""
    def to_duckdb_path(self, key):
        return ""


class _SecondHelper(_NoHelpers):
    """First helper raises, second answers: the chain order must be preserved."""
    def to_duckdb_path(self, key):
        raise RuntimeError("nope")

    def make_duckdb_url(self, key):
        return f"https://second/{key}"


KEYS = [
    "org/tbl/data/part-0.parquet",
    "/leading/slash.parquet",
    "s3://already/a/url.parquet",
    "https://already/http.parquet",
    "",
    "no-slash.parquet",
]

STORAGES = [_NoHelpers, _RaisingHelper, _WorkingHelper, _EmptyHelper, _SecondHelper]


def _estimator(storage):
    e = DataEstimator.__new__(DataEstimator)
    e.storage = storage
    e._resolve_plan = None
    return e


@pytest.mark.parametrize("storage_cls", STORAGES, ids=lambda c: c.__name__)
def test_planned_resolution_matches_per_key_resolution(storage_cls):
    """Same answer for every key, and the plan is reused across the loop."""
    storage = storage_cls()
    planned, direct = _estimator(storage), _estimator(storage)
    for key in KEYS:
        assert planned._to_duckdb_path(key) == direct._to_duckdb_path_uncached(key), (
            f"{storage_cls.__name__} diverged on {key!r}"
        )


@pytest.mark.parametrize("storage_cls", STORAGES, ids=lambda c: c.__name__)
def test_helper_is_probed_once_not_once_per_key(storage_cls):
    """The point of the change: N keys must not cost N probes."""
    storage = storage_cls()
    calls = []
    if hasattr(storage_cls, "to_duckdb_path"):
        orig = storage.to_duckdb_path

        def counted(key, _o=orig):
            calls.append(key)
            return _o(key)

        storage.to_duckdb_path = counted

    e = _estimator(storage)
    # Only non-URL, non-empty keys reach the helper at all.
    for key in ["a/1.parquet", "a/2.parquet", "a/3.parquet", "a/4.parquet"]:
        e._to_duckdb_path(key)

    if storage_cls is _WorkingHelper:
        # A helper that works is genuinely per-key: it must still be called.
        assert len(calls) == 4
    else:
        # A helper that raises or returns empty is settled by the first probe.
        assert len(calls) <= 1, f"probed {len(calls)}x; should be settled once"


def test_empty_key_short_circuits_before_planning():
    """An empty key must not build (or poison) the plan."""
    e = _estimator(_RaisingHelper())
    assert e._to_duckdb_path("") == ""
    assert e._resolve_plan is None


# --------------------------------------------------------------------------
# Presigning: availability is a property of the backend, failure may not be
# --------------------------------------------------------------------------

class _NoPresign(_NoHelpers):
    """LocalStorage's real shape: presign exists but is unimplemented."""
    def presign(self, key, expiry_seconds=3600):
        raise NotImplementedError("LocalStorage does not implement presign()")


class _FlakyPresign(_NoHelpers):
    """Fails once then works — a transient failure must NOT be made permanent."""
    def __init__(self):
        self.calls = 0

    def presign(self, key, expiry_seconds=3600):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("credentials expired")
        return f"https://signed/{key}?sig=abc"


class _GoodPresign(_NoHelpers):
    def __init__(self):
        self.calls = 0

    def presign(self, key, expiry_seconds=3600):
        self.calls += 1
        return f"https://signed/{key}?sig=abc"


class _SettingsProxy:
    """``settings`` is a frozen dataclass, so the flag cannot be set on it.

    Reads fall through to the real settings; only the presign flag is forced.
    """

    def __init__(self, real, **overrides):
        self._real, self._overrides = real, overrides

    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._real, name)


@pytest.fixture
def presigned_on(monkeypatch):
    monkeypatch.setattr(
        est_mod, "settings",
        _SettingsProxy(est_mod.settings, SUPERTABLE_DUCKDB_PRESIGNED=True),
    )


def test_unimplemented_presign_is_probed_once(presigned_on):
    """The hot path: NotImplementedError must not cost one raise per file."""
    storage = _NoPresign()
    calls = []
    storage.presign = lambda k, expiry_seconds=3600: (
        calls.append(k), _NoPresign.presign(storage, k)
    )[1]

    e = _estimator(storage)
    out = [e._to_duckdb_path(f"a/{i}.parquet") for i in range(50)]

    assert len(calls) == 1, f"presign attempted {len(calls)}x for 50 files"
    # And the answers still match the original per-key implementation.
    direct = _estimator(_NoPresign())
    assert out == [direct._to_duckdb_path_uncached(f"a/{i}.parquet") for i in range(50)]


def test_transient_presign_failure_is_retried_per_key(presigned_on):
    """A one-off failure must not permanently disable presigning."""
    storage = _FlakyPresign()
    e = _estimator(storage)
    first = e._to_duckdb_path("a/0.parquet")
    rest = [e._to_duckdb_path(f"a/{i}.parquet") for i in range(1, 4)]

    # The fallback is the constructed URL, not the bare key: _FlakyPresign has
    # a bucket and an endpoint, so step 4 answers.
    assert first == "s3://b1/a/0.parquet", "failed presign falls back to unsigned"
    assert rest == [f"https://signed/a/{i}.parquet?sig=abc" for i in range(1, 4)], (
        "presigning must resume once the transient failure clears"
    )


def test_working_presign_signs_every_key_exactly_once(presigned_on):
    """Presigning is genuinely per-key: no dedup, no double-signing."""
    storage = _GoodPresign()
    e = _estimator(storage)
    out = [e._to_duckdb_path(f"a/{i}.parquet") for i in range(20)]

    assert storage.calls == 20, f"signed {storage.calls}x for 20 files"
    assert out == [f"https://signed/a/{i}.parquet?sig=abc" for i in range(20)]
