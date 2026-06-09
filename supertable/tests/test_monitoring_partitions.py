"""
Tests for supertable/monitoring/partitions.py — the daily-partitioned
monitoring drain orchestration.

Covers:
  1. MONITORING_SINK_TABLES content invariants
  2. _today_utc / _decode / _parse_entries helpers
  3. list_drainable_partitions
     - scan yields keys, today is excluded, older returned sorted
     - missing organization → []
     - missing catalog.r → []
     - monitor_type filter narrows pattern
     - scan failure → returns [] (no raise)
     - malformed keys ignored
  4. drain_partition
     - happy path: rename → lrange → del; returns parsed dicts
     - source missing, drain missing → []
     - source missing, drain has data → resumes (no rename, returns drain data)
     - rename succeeds, lrange empty → still deletes drain key
     - lrange fails → leaves drain key for retry, returns []
     - del fails after successful read → returns parsed data, warns
     - malformed entries are silently skipped
     - catalog without .r → ValueError
  5. iter_partition_chunks
     - happy path: yields chunks, deletes drain at end
     - chunk_size clamped to [1, 1_000_000]
     - empty partition → no yields, deletes drain
     - lrange in mid-iteration fails → stops, leaves drain
     - caller bails early via break → drain not deleted
  6. monitoring_writer integration
     - _today_utc_date returns valid ISO date
     - _MonitorKey.redis_list_key_today uses today's date
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_keys as RK  # noqa: E402
from supertable.monitoring import partitions as pmod  # noqa: E402
from supertable.monitoring.partitions import (  # noqa: E402
    MONITORING_SINK_TABLE_FOR,
    MONITORING_SINK_TABLES,
    MonitorPartition,
    _decode,
    _parse_entries,
    _today_utc,
    drain_partition,
    iter_partition_chunks,
    list_drainable_partitions,
    read_recent,
)


ORG = "acme"


def _mock_catalog():
    """Mock that mimics RedisCatalog.r ducktyping."""
    cat = MagicMock()
    cat.r = MagicMock()
    return cat


# ===========================================================================
# 1. Sink-table set invariants
# ===========================================================================


class TestSinkTables:

    def test_set_contains_expected_names(self):
        assert "__writes__" in MONITORING_SINK_TABLES
        assert "__reads__" in MONITORING_SINK_TABLES
        assert "__mcp__" in MONITORING_SINK_TABLES
        assert "__plans__" in MONITORING_SINK_TABLES

    def test_set_does_not_contain_user_table_names(self):
        assert "users" not in MONITORING_SINK_TABLES
        assert "orders" not in MONITORING_SINK_TABLES
        # Single-underscore-wrapped sentinel pattern is also rejected
        assert "_writes_" not in MONITORING_SINK_TABLES

    def test_mapping_keys_are_valid_monitor_types(self):
        valid = {"plans", "writes", "mcp", "odata", "errors", "locks"}
        for mt in MONITORING_SINK_TABLE_FOR.keys():
            assert mt in valid

    def test_mapping_values_appear_in_sink_tables(self):
        for sink in MONITORING_SINK_TABLE_FOR.values():
            assert sink in MONITORING_SINK_TABLES


# ===========================================================================
# 2. Helpers
# ===========================================================================


class TestHelpers:

    def test_today_utc_iso_format(self):
        s = _today_utc()
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", s)

    def test_today_utc_uses_utc(self):
        # Within a small clock skew, _today_utc matches datetime.utcnow.
        s = _today_utc()
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert s == now_utc

    def test_decode_bytes(self):
        assert _decode(b"hello") == "hello"
        assert _decode("hello") == "hello"

    def test_parse_entries_happy(self):
        raw = [
            json.dumps({"a": 1}).encode(),
            json.dumps({"b": 2}),
        ]
        out = _parse_entries(raw)
        assert out == [{"a": 1}, {"b": 2}]

    def test_parse_entries_skips_malformed(self):
        raw = [
            json.dumps({"a": 1}),
            "not json",
            json.dumps([1, 2, 3]),  # not a dict
            json.dumps({"b": 2}),
        ]
        out = _parse_entries(raw)
        assert out == [{"a": 1}, {"b": 2}]

    def test_parse_entries_empty(self):
        assert _parse_entries([]) == []
        assert _parse_entries(None) == []  # type: ignore[arg-type]


# ===========================================================================
# 3. list_drainable_partitions
# ===========================================================================


class TestListDrainable:

    def test_empty_org_returns_empty(self):
        cat = _mock_catalog()
        assert list_drainable_partitions(cat, organization="") == []
        cat.r.scan_iter.assert_not_called()

    def test_no_redis_returns_empty(self):
        cat = _mock_catalog()
        cat.r = None
        assert list_drainable_partitions(cat, organization=ORG) == []

    def test_today_excluded_older_returned_sorted(self):
        cat = _mock_catalog()
        today = _today_utc()
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        two_days_ago = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

        cat.r.scan_iter.return_value = iter([
            RK.monitor_partition(ORG, "writes", today),       # today — excluded
            RK.monitor_partition(ORG, "writes", yesterday),   # drainable
            RK.monitor_partition(ORG, "plans", two_days_ago), # drainable
            RK.monitor_partition(ORG, "mcp", yesterday),      # drainable
        ])

        out = list_drainable_partitions(cat, organization=ORG)
        # Sorted by NamedTuple field order (org, monitor_type, date)
        assert out == sorted(out)
        assert MonitorPartition(ORG, "writes", today) not in out
        assert MonitorPartition(ORG, "writes", yesterday) in out
        assert MonitorPartition(ORG, "plans", two_days_ago) in out
        assert MonitorPartition(ORG, "mcp", yesterday) in out
        assert len(out) == 3

    def test_monitor_type_filter_narrows_scan_pattern(self):
        cat = _mock_catalog()
        cat.r.scan_iter.return_value = iter([])
        list_drainable_partitions(cat, organization=ORG, monitor_type="writes")
        # The pattern passed to scan_iter should be the narrowed one
        args, kwargs = cat.r.scan_iter.call_args
        assert kwargs.get("match") == RK.monitor_partition_pattern(ORG, "writes")

    def test_no_filter_uses_org_wide_pattern(self):
        cat = _mock_catalog()
        cat.r.scan_iter.return_value = iter([])
        list_drainable_partitions(cat, organization=ORG)
        args, kwargs = cat.r.scan_iter.call_args
        assert kwargs.get("match") == RK.monitor_partition_pattern_for_org(ORG)

    def test_scan_failure_returns_empty(self):
        cat = _mock_catalog()
        cat.r.scan_iter.side_effect = RuntimeError("redis down")
        # Must not raise — orchestrator's loop should never crash on
        # a transient redis hiccup.
        assert list_drainable_partitions(cat, organization=ORG) == []

    def test_malformed_keys_ignored(self):
        cat = _mock_catalog()
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        cat.r.scan_iter.return_value = iter([
            "supertable:acme:lakes:demo:meta:root",     # not monitor
            "supertable:acme:monitor:writes:doc:nope",  # bad date
            "garbage:foo:bar",
            RK.monitor_partition(ORG, "writes", yesterday),
        ])
        out = list_drainable_partitions(cat, organization=ORG)
        assert out == [MonitorPartition(ORG, "writes", yesterday)]


# ===========================================================================
# 4. drain_partition
# ===========================================================================


class TestDrainPartition:

    def test_no_redis_raises_value_error(self):
        cat = _mock_catalog()
        cat.r = None
        with pytest.raises(ValueError):
            drain_partition(cat, organization=ORG, monitor_type="writes", date="2026-06-09")

    def test_happy_path_rename_lrange_del(self):
        cat = _mock_catalog()
        # Rename succeeds (returns None on redis-py)
        cat.r.rename.return_value = None
        cat.r.lrange.return_value = [
            json.dumps({"q": 1}).encode(),
            json.dumps({"q": 2}).encode(),
        ]
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )

        assert out == [{"q": 1}, {"q": 2}]
        src = RK.monitor_partition(ORG, "writes", "2026-06-09")
        drain = RK.monitor_partition_drain(ORG, "writes", "2026-06-09")
        cat.r.rename.assert_called_once_with(src, drain)
        cat.r.lrange.assert_called_once_with(drain, 0, -1)
        cat.r.delete.assert_called_once_with(drain)

    def test_source_missing_drain_missing_returns_empty(self):
        cat = _mock_catalog()
        cat.r.rename.side_effect = Exception("no such key")  # source missing
        cat.r.lrange.return_value = []
        # delete still safe to call but optional
        cat.r.delete.return_value = 0
        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == []

    def test_resumes_from_existing_drain_handle(self):
        """Crash-recovery path: previous attempt left a populated drain
        key behind. The retry should still read it without a fresh rename."""
        cat = _mock_catalog()
        # Rename fails because drain already exists
        cat.r.rename.side_effect = Exception("drain already exists")
        cat.r.lrange.return_value = [json.dumps({"resumed": True}).encode()]
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == [{"resumed": True}]
        # Even though rename failed, drain was read and deleted
        cat.r.lrange.assert_called_once()
        cat.r.delete.assert_called_once()

    def test_rename_ok_but_lrange_empty_still_deletes_drain(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.lrange.return_value = []
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == []
        # We renamed src→drain creating an empty key; clean up.
        cat.r.delete.assert_called_once()

    def test_lrange_failure_leaves_drain_for_retry(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.lrange.side_effect = RuntimeError("transient")

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        # Return empty, don't DEL — next call will retry the read.
        assert out == []
        cat.r.delete.assert_not_called()

    def test_del_failure_after_read_returns_parsed_data(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.lrange.return_value = [json.dumps({"x": 1}).encode()]
        cat.r.delete.side_effect = RuntimeError("transient")

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        # We delivered the data — caller writes it to sink. Next call
        # to drain_partition will redeliver because drain wasn't DEL'd;
        # caller must keep sink writes idempotent.
        assert out == [{"x": 1}]


# ===========================================================================
# 5. iter_partition_chunks
# ===========================================================================


class TestIterPartitionChunks:

    def test_no_redis_raises_value_error(self):
        cat = _mock_catalog()
        cat.r = None
        with pytest.raises(ValueError):
            list(iter_partition_chunks(
                cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            ))

    def test_chunk_size_clamped_low(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 0
        cat.r.delete.return_value = 0
        # chunk_size=0 → clamped to 1, doesn't blow up
        list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=0,
        ))

    def test_chunk_size_clamped_high(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 0
        # chunk_size > 1M → clamped, doesn't blow up
        list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=10_000_000,
        ))

    def test_empty_partition_yields_nothing_deletes_drain(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 0

        chunks = list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        ))
        assert chunks == []
        cat.r.delete.assert_called()  # empty drain handle cleaned up

    def test_happy_path_yields_chunks_and_deletes(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 5

        # LRANGE returns slices of the right size
        entries = [
            json.dumps({"i": i}).encode() for i in range(5)
        ]

        def _lrange(_key, start, stop):
            return entries[start:stop + 1]

        cat.r.lrange.side_effect = _lrange

        chunks = list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=2,
        ))
        assert chunks == [
            [{"i": 0}, {"i": 1}],
            [{"i": 2}, {"i": 3}],
            [{"i": 4}],
        ]
        cat.r.delete.assert_called_once()

    def test_lrange_failure_mid_iteration_stops_leaves_drain(self):
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 10

        call_count = {"n": 0}

        def _lrange(_key, start, stop):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise RuntimeError("redis hiccup")
            return [json.dumps({"i": i}).encode() for i in range(start, stop + 1)]

        cat.r.lrange.side_effect = _lrange

        chunks = list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=2,
        ))
        # First chunk OK; second raises → iterator stops, drain not deleted
        assert chunks == [
            [{"i": 0}, {"i": 1}],
        ]
        cat.r.delete.assert_not_called()

    def test_caller_bails_early_drain_not_deleted(self):
        """When the orchestrator breaks out of the iterator early, the
        drain handle is left in place. The next call resumes from it —
        intentional behaviour (replays already-yielded chunks, which
        the orchestrator must handle idempotently)."""
        cat = _mock_catalog()
        cat.r.rename.return_value = None
        cat.r.llen.return_value = 6

        def _lrange(_key, start, stop):
            return [json.dumps({"i": i}).encode() for i in range(start, stop + 1)]

        cat.r.lrange.side_effect = _lrange

        first_only = []
        for chunk in iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=2,
        ):
            first_only.append(chunk)
            break  # bail after one

        assert first_only == [[{"i": 0}, {"i": 1}]]
        cat.r.delete.assert_not_called()


# ===========================================================================
# 6. monitoring_writer integration
# ===========================================================================


class TestMonitorKeyIntegration:

    def test_today_utc_date_helper(self):
        from supertable.monitoring_writer import _today_utc_date
        s = _today_utc_date()
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", s)

    def test_monitor_key_redis_list_key_today_uses_partition(self):
        from supertable.monitoring_writer import _MonitorKey, _today_utc_date
        mk = _MonitorKey(organization="acme", monitor_type="writes")
        key = mk.redis_list_key_today()
        # Same shape as RK.monitor_partition for today
        expected = RK.monitor_partition("acme", "writes", _today_utc_date())
        assert key == expected

    def test_path_key_unchanged_across_days(self):
        """The in-process cache key is per (org, type) — never per date —
        so one logger handles the daily rollover by recomputing the
        Redis key per ship."""
        from supertable.monitoring_writer import _MonitorKey
        mk = _MonitorKey(organization="acme", monitor_type="writes")
        assert mk.path_key == "acme/writes"


# ===========================================================================
# 7. read_recent — newest-first tail across partitions
# ===========================================================================


def _mock_partition_data(cat, *, data_by_date):
    """Configure ``cat.r.lrange`` so that each date in ``data_by_date``
    holds the given list of dict payloads (in insertion / RPUSH order).

    Args:
        cat: a mock catalog (already with ``.r`` set).
        data_by_date: ``{"YYYY-MM-DD": [dict, dict, ...]}``. Earlier
            items were RPUSH-ed first.
    """
    # Pre-serialise to bytes (matches what Redis returns).
    serialised = {
        d: [json.dumps(p).encode() for p in lst]
        for d, lst in data_by_date.items()
    }
    # Build a lookup keyed by the actual Redis key string the function
    # will request.
    key_to_list = {
        RK.monitor_partition(ORG, "writes", d): lst
        for d, lst in serialised.items()
    }

    def _lrange(key, start, stop):
        # We get key as the Redis key string.
        items = key_to_list.get(key, [])
        if not items:
            return []
        # Python list slice with redis-py negative-index semantics:
        # LRANGE -N -1 → last N items in insertion order.
        # We translate to Python's list[start:stop+1] with negative
        # indices handled normally.
        if stop == -1:
            stop_idx = len(items)
        else:
            stop_idx = stop + 1 if stop >= 0 else len(items) + stop + 1
        if start < 0:
            start_idx = max(0, len(items) + start)
        else:
            start_idx = start
        return items[start_idx:stop_idx]

    cat.r.lrange.side_effect = _lrange
    return key_to_list


class TestReadRecent:

    def test_zero_limit_returns_empty(self):
        cat = _mock_catalog()
        assert read_recent(cat, organization=ORG, monitor_type="writes", limit=0) == []
        cat.r.lrange.assert_not_called()

    def test_negative_limit_returns_empty(self):
        cat = _mock_catalog()
        assert read_recent(cat, organization=ORG, monitor_type="writes", limit=-5) == []

    def test_empty_org_returns_empty(self):
        cat = _mock_catalog()
        assert read_recent(cat, organization="", monitor_type="writes", limit=10) == []

    def test_invalid_monitor_type_returns_empty(self):
        cat = _mock_catalog()
        assert read_recent(cat, organization=ORG, monitor_type="garbage", limit=10) == []
        cat.r.lrange.assert_not_called()

    def test_no_redis_returns_empty(self):
        cat = _mock_catalog()
        cat.r = None
        assert read_recent(cat, organization=ORG, monitor_type="writes", limit=10) == []

    def test_only_today_has_data(self):
        cat = _mock_catalog()
        today = _today_utc()
        _mock_partition_data(cat, data_by_date={
            today: [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}, {"i": 4}],
        })
        out = read_recent(cat, organization=ORG, monitor_type="writes", limit=3)
        # Newest first → reversed insertion order of last 3 items
        assert out == [{"i": 4}, {"i": 3}, {"i": 2}]

    def test_walks_yesterday_when_today_insufficient(self):
        cat = _mock_catalog()
        today = _today_utc()
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        _mock_partition_data(cat, data_by_date={
            today: [{"d": "today", "i": 0}, {"d": "today", "i": 1}],  # 2 items
            yesterday: [{"d": "yesterday", "i": 0}, {"d": "yesterday", "i": 1}, {"d": "yesterday", "i": 2}],
        })
        out = read_recent(cat, organization=ORG, monitor_type="writes", limit=4)
        # Today contributes 2 (newest first), then yesterday contributes 2 (newest of its 3)
        assert out == [
            {"d": "today", "i": 1},
            {"d": "today", "i": 0},
            {"d": "yesterday", "i": 2},
            {"d": "yesterday", "i": 1},
        ]

    def test_walks_multiple_days_back(self):
        cat = _mock_catalog()
        today = _today_utc()
        d1 = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        d2 = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        _mock_partition_data(cat, data_by_date={
            today: [{"day": 0}],
            d1: [{"day": 1}],
            d2: [{"day": 2}],
        })
        out = read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=3,
        )
        # Newest first across days
        assert out == [{"day": 0}, {"day": 1}, {"day": 2}]

    def test_max_days_back_clamps_walk(self):
        cat = _mock_catalog()
        today = _today_utc()
        d1 = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        d2 = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        _mock_partition_data(cat, data_by_date={
            today: [{"day": 0}],
            d1: [{"day": 1}],
            d2: [{"day": 2}],
        })
        out = read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=2,
        )
        # Only today + yesterday — d2 is outside the window
        assert out == [{"day": 0}, {"day": 1}]

    def test_missing_partitions_silently_skipped(self):
        """Partitions that have been drained / never existed are
        silently skipped; the walk continues."""
        cat = _mock_catalog()
        today = _today_utc()
        d2 = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        # Yesterday is missing entirely — drained by orchestrator
        _mock_partition_data(cat, data_by_date={
            today: [{"day": 0}],
            d2: [{"day": 2}],
        })
        out = read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=3,
        )
        assert out == [{"day": 0}, {"day": 2}]

    def test_limit_respected_exactly(self):
        cat = _mock_catalog()
        today = _today_utc()
        _mock_partition_data(cat, data_by_date={
            today: [{"i": i} for i in range(1000)],
        })
        out = read_recent(cat, organization=ORG, monitor_type="writes", limit=100)
        assert len(out) == 100
        # Newest first: indices 999, 998, ..., 900
        assert out[0] == {"i": 999}
        assert out[-1] == {"i": 900}

    def test_limit_clamped_to_million(self):
        """A pathological limit (e.g. INT_MAX) must not crash the walk."""
        cat = _mock_catalog()
        today = _today_utc()
        _mock_partition_data(cat, data_by_date={
            today: [{"i": i} for i in range(5)],
        })
        # 100M should clamp to 1M — but we only have 5 items so we get 5
        out = read_recent(cat, organization=ORG, monitor_type="writes", limit=100_000_000)
        assert len(out) == 5

    def test_max_days_back_clamped_to_safety_cap(self):
        cat = _mock_catalog()
        cat.r.lrange.return_value = []
        # 10_000 should clamp to _MAX_READ_RECENT_DAYS_BACK = 90
        read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=10_000,
        )
        assert cat.r.lrange.call_count == 90

    def test_max_days_back_clamped_to_at_least_one(self):
        cat = _mock_catalog()
        cat.r.lrange.return_value = []
        read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=0,
        )
        # Must inspect today even when caller asks for 0 days
        assert cat.r.lrange.call_count == 1

    def test_malformed_entries_silently_skipped(self):
        cat = _mock_catalog()
        today = _today_utc()
        today_key = RK.monitor_partition(ORG, "writes", today)

        def _lrange(key, start, stop):
            # Today's partition has the malformed payloads; older days empty.
            if key == today_key:
                return [
                    json.dumps({"good": 1}).encode(),
                    b"not json",
                    json.dumps([1, 2, 3]).encode(),  # not a dict
                    json.dumps({"also_good": 2}).encode(),
                ]
            return []

        cat.r.lrange.side_effect = _lrange
        out = read_recent(cat, organization=ORG, monitor_type="writes", limit=10)
        # 2 valid entries — reversed (newest first)
        assert out == [{"also_good": 2}, {"good": 1}]

    def test_lrange_failure_skips_day_keeps_walking(self):
        cat = _mock_catalog()
        today = _today_utc()
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        key_today = RK.monitor_partition(ORG, "writes", today)
        key_yesterday = RK.monitor_partition(ORG, "writes", yesterday)

        def _lrange(key, start, stop):
            if key == key_today:
                raise RuntimeError("today key broken")
            if key == key_yesterday:
                return [json.dumps({"day": "yesterday"}).encode()]
            return []

        cat.r.lrange.side_effect = _lrange
        out = read_recent(
            cat, organization=ORG, monitor_type="writes",
            limit=10, max_days_back=2,
        )
        # Today's lrange raised → skipped silently; yesterday returned
        assert out == [{"day": "yesterday"}]

    def test_read_recent_does_not_mutate_redis(self):
        """``read_recent`` is read-only — no RENAME, no DEL, no XADD."""
        cat = _mock_catalog()
        today = _today_utc()
        _mock_partition_data(cat, data_by_date={
            today: [{"i": 0}, {"i": 1}],
        })
        read_recent(cat, organization=ORG, monitor_type="writes", limit=10)
        cat.r.rename.assert_not_called()
        cat.r.delete.assert_not_called()
        cat.r.xadd.assert_not_called()
        cat.r.xdel.assert_not_called()
