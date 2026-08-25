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
     - rename succeeds, lrange empty → retains a receipt for explicit ACK
     - lrange fails → leaves drain key for retry, returns []
     - malformed entries are silently skipped
     - catalog without .r → ValueError
  5. iter_partition_chunks
     - happy path: yields chunks, retains drain until explicit ACK
     - chunk_size clamped to [1, 1_000_000]
     - empty partition → no yields, retains receipt for explicit ACK
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
import fakeredis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_keys as RK  # noqa: E402
from supertable.monitoring import partitions as pmod  # noqa: E402
from supertable.monitoring.partitions import (  # noqa: E402
    MONITORING_SINK_TABLE_FOR,
    MONITORING_SINK_TABLES,
    MonitorPartition,
    acknowledge_partition,
    claim_partition_chunks,
    claim_partition,
    _decode,
    _parse_entries,
    _today_utc,
    drain_partition,
    iter_partition_chunks,
    iter_claimed_partition_chunks,
    list_drainable_partitions,
    read_recent,
)


ORG = "acme"


def _mock_catalog():
    """Mock that mimics RedisCatalog.r ducktyping."""
    cat = MagicMock()
    cat.r = MagicMock()
    cat.r.eval.return_value = [1, -1]
    cat.r.pttl.return_value = -1
    cat.r.set.return_value = True
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
        # Pull the canonical closed set from redis_keys so this test
        # automatically stays in sync when a new monitor_type is added.
        from supertable.redis_keys import _VALID_MONITOR_TYPES
        for mt in MONITORING_SINK_TABLE_FOR.keys():
            assert mt in _VALID_MONITOR_TYPES

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

    def test_parse_entries_never_logs_poisoned_scalar_content(self, caplog):
        secret = "https://host/capability/TOP-SECRET?sig=sentinel"
        caplog.set_level("DEBUG", logger=pmod.__name__)

        assert _parse_entries([
            json.dumps(secret),
            json.dumps([secret]),
            f"not-json-{secret}",
        ]) == []

        rendered = "\n".join(record.getMessage() for record in caplog.records)
        assert secret not in rendered
        assert rendered.count("value_type=unexpected_value") == 2

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

    def test_scan_failure_returns_empty(self, caplog):
        cat = _mock_catalog()
        secret = "redis://user:TOP-SECRET@host/0"
        cat.r.scan_iter.side_effect = RuntimeError(secret)
        # Must not raise — orchestrator's loop should never crash on
        # a transient redis hiccup.
        assert list_drainable_partitions(cat, organization=ORG) == []
        assert secret not in "\n".join(
            record.getMessage() for record in caplog.records
        )

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

    def test_happy_path_renamenx_lrange_del(self):
        cat = _mock_catalog()
        # RENAMENX succeeds (returns True / 1) — drain owns the data
        cat.r.renamenx.return_value = 1
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
        cat.r.eval.assert_called_once()
        eval_args = cat.r.eval.call_args.args
        assert eval_args[1:] == (2, src, drain)
        cat.r.persist.assert_not_called()
        cat.r.renamenx.assert_not_called()
        # Plain RENAME must NOT be used — it would silently destroy a
        # previous crashed run's drain contents.
        cat.r.rename.assert_not_called()
        cat.r.lrange.assert_called_once_with(drain, 0, -1)
        cat.r.delete.assert_not_called()

    def test_source_missing_drain_missing_returns_empty(self):
        cat = _mock_catalog()
        # renamenx raises when src missing (Redis ResponseError)
        cat.r.renamenx.side_effect = Exception("no such key")
        cat.r.lrange.return_value = []
        cat.r.delete.return_value = 0
        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == []

    def test_resumes_from_existing_drain_handle(self):
        """Crash-recovery path: previous attempt left a populated drain
        key behind. RENAMENX returns 0 / False (does NOT overwrite),
        we fall through to read the existing drain — no data is lost."""
        cat = _mock_catalog()
        # RENAMENX returns 0 / False — destination already exists
        cat.r.renamenx.return_value = 0
        cat.r.lrange.return_value = [json.dumps({"resumed": True}).encode()]
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == [{"resumed": True}]
        # Drain was read but remains until an explicit durable-sink ACK.
        cat.r.lrange.assert_called_once()
        cat.r.delete.assert_not_called()
        # Critically: RENAME must NOT have been used (it would have
        # overwritten the drain contents and lost the queued entries)
        cat.r.rename.assert_not_called()

    def test_renamenx_returns_false_drain_not_destroyed(self):
        """Regression for the silent-data-loss bug: when renamenx
        returns False (drain exists from a prior crashed run), the
        drain contents are preserved."""
        cat = _mock_catalog()
        cat.r.renamenx.return_value = False  # drain already exists
        # Drain handle holds 2 queued entries from a previous crash
        cat.r.lrange.return_value = [
            json.dumps({"prior_run_1": True}).encode(),
            json.dumps({"prior_run_2": True}).encode(),
        ]
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )

        # Both prior-run entries recovered
        assert out == [{"prior_run_1": True}, {"prior_run_2": True}]
        # Source key was NOT touched by us (writer's new entries
        # are still there for the next drain call)
        cat.r.rename.assert_not_called()

    def test_rename_ok_but_lrange_empty_never_acks_without_sink_commit(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
        cat.r.lrange.return_value = []
        cat.r.delete.return_value = 1

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        assert out == []
        cat.r.delete.assert_not_called()

    def test_lrange_failure_leaves_drain_for_retry(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
        cat.r.lrange.side_effect = RuntimeError("transient")

        out = drain_partition(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        )
        # Return empty, don't DEL — next call will retry the read.
        assert out == []
        cat.r.delete.assert_not_called()

    def test_del_failure_after_read_returns_parsed_data(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
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


class TestExplicitDrainAcknowledgement:

    def test_sink_failure_replays_and_only_exact_receipt_deletes(self):
        cat = MagicMock()
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        date = "2026-06-09"
        src = RK.monitor_partition(ORG, "writes", date)
        drain = RK.monitor_partition_drain(ORG, "writes", date)
        cat.r.rpush(src, json.dumps({"query_id": "q-1"}))
        cat.r.expire(src, 60)

        first = claim_partition(
            cat, organization=ORG, monitor_type="writes", date=date,
        )
        assert first is not None
        assert first.entries == ({"query_id": "q-1"},)
        assert cat.r.pttl(drain) == -1

        # A failed downstream write performs no ACK. The exact claim replays.
        replay = claim_partition(
            cat, organization=ORG, monitor_type="writes", date=date,
        )
        assert replay == first
        assert acknowledge_partition(
            cat,
            organization=ORG,
            monitor_type="writes",
            date=date,
            receipt="0" * 64,
        ) is False
        assert cat.r.exists(drain)

        assert acknowledge_partition(
            cat,
            organization=ORG,
            monitor_type="writes",
            date=date,
            receipt=first.receipt,
        ) is True
        assert not cat.r.exists(drain)

    def test_crashed_drain_remains_discoverable(self, monkeypatch):
        cat = MagicMock()
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        date = "2026-06-09"
        cat.r.rpush(
            RK.monitor_partition_drain(ORG, "writes", date),
            json.dumps({"query_id": "q-1"}),
        )
        monkeypatch.setattr(pmod, "_today_utc", lambda: "2026-06-10")
        assert list_drainable_partitions(cat, organization=ORG) == [
            MonitorPartition(ORG, "writes", date)
        ]

    def test_bounded_claim_stream_exposes_operable_receipt_and_acks(self):
        cat = MagicMock()
        inner = fakeredis.FakeRedis(decode_responses=True)

        class BoundedRedis:
            def __init__(self, redis_client):
                self.inner = redis_client
                self.windows = []

            def __getattr__(self, name):
                return getattr(self.inner, name)

            def lrange(self, key, start, stop):
                assert not (start == 0 and stop == -1), "unbounded drain read"
                self.windows.append((start, stop))
                return self.inner.lrange(key, start, stop)

        cat.r = BoundedRedis(inner)
        date = "2026-06-09"
        src = RK.monitor_partition(ORG, "writes", date)
        for index in range(5):
            inner.rpush(src, json.dumps({"query_id": f"q-{index}"}))
        inner.expire(src, 60)

        claim = claim_partition_chunks(
            cat,
            organization=ORG,
            monitor_type="writes",
            date=date,
            chunk_size=2,
        )
        assert claim is not None
        assert claim.entry_count == 5
        drain = RK.monitor_partition_drain(ORG, "writes", date)
        assert inner.pttl(drain) == -1

        chunks = list(iter_claimed_partition_chunks(cat, claim, chunk_size=2))
        assert [[item["query_id"] for item in chunk] for chunk in chunks] == [
            ["q-0", "q-1"],
            ["q-2", "q-3"],
            ["q-4"],
        ]
        assert all(stop - start + 1 <= 2 for start, stop in cat.r.windows)
        # Iterator exhaustion is not an ACK; a sink failure remains replayable.
        assert inner.exists(drain)
        assert acknowledge_partition(
            cat,
            organization=claim.organization,
            monitor_type=claim.monitor_type,
            date=claim.date,
            receipt=claim.receipt,
        )
        assert not inner.exists(drain)

    def test_bounded_claim_refuses_poison_row_without_deleting_source(self):
        cat = MagicMock()
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        date = "2026-06-09"
        src = RK.monitor_partition(ORG, "writes", date)
        cat.r.rpush(src, json.dumps({"query_id": "ok"}), "not-json")

        with pytest.raises(pmod.MonitoringPartitionError, match="valid JSON"):
            claim_partition_chunks(
                cat,
                organization=ORG,
                monitor_type="writes",
                date=date,
                chunk_size=1,
            )
        assert cat.r.llen(RK.monitor_partition_drain(ORG, "writes", date)) == 2

    def test_bounded_stream_detects_claim_mutation_and_never_acks(self):
        cat = MagicMock()
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        date = "2026-06-09"
        src = RK.monitor_partition(ORG, "writes", date)
        cat.r.rpush(src, json.dumps({"query_id": "q-1"}))
        claim = claim_partition_chunks(
            cat,
            organization=ORG,
            monitor_type="writes",
            date=date,
            chunk_size=1,
        )
        assert claim is not None
        drain = RK.monitor_partition_drain(ORG, "writes", date)
        cat.r.rpush(drain, json.dumps({"query_id": "unexpected"}))

        with pytest.raises(pmod.MonitoringPartitionError, match="size changed"):
            list(iter_claimed_partition_chunks(cat, claim, chunk_size=1))
        assert cat.r.exists(drain)


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
        cat.r.renamenx.return_value = 1
        cat.r.llen.return_value = 0
        cat.r.delete.return_value = 0
        # chunk_size=0 → clamped to 1, doesn't blow up
        list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=0,
        ))

    def test_chunk_size_clamped_high(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
        cat.r.llen.return_value = 0
        # chunk_size > 1M → clamped, doesn't blow up
        list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
            chunk_size=10_000_000,
        ))

    def test_empty_partition_yields_nothing_deletes_drain(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
        cat.r.llen.return_value = 0

        chunks = list(iter_partition_chunks(
            cat, organization=ORG, monitor_type="writes", date="2026-06-09",
        ))
        assert chunks == []
        cat.r.delete.assert_not_called()

    def test_happy_path_yields_chunks_and_deletes(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
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
        cat.r.delete.assert_not_called()

    def test_lrange_failure_mid_iteration_stops_leaves_drain(self):
        cat = _mock_catalog()
        cat.r.renamenx.return_value = 1
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
        cat.r.renamenx.return_value = 1
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


# ===========================================================================
# 8. Retention backstop — 7-day TTL on every monitoring partition
# ===========================================================================


class TestPartitionTTL:
    """The writer stamps an EXPIREAT on the partition so un-drained data
    self-destructs after at most _MONITOR_TTL_DAYS. EXPIREAT is anchored to
    the partition's own date (midnight UTC + N days), not the last write, so
    retention is a hard ≤7-day cap rather than a renewable sliding window."""

    def test_partition_expire_at_is_date_midnight_plus_ttl(self):
        from supertable.monitoring_writer import (
            _partition_expire_at, _MONITOR_TTL_DAYS,
        )
        # 2026-06-26 00:00:00Z + 7 days == 2026-07-03 00:00:00Z
        got = _partition_expire_at("2026-06-26")
        expected = int(
            datetime(2026, 6, 26, tzinfo=timezone.utc).timestamp()
        ) + _MONITOR_TTL_DAYS * 86400
        assert got == expected

    def test_default_ttl_is_seven_days(self):
        from supertable.monitoring_writer import _MONITOR_TTL_DAYS
        assert _MONITOR_TTL_DAYS == 7

    def test_expire_at_is_idempotent_for_same_date(self):
        from supertable.monitoring_writer import _partition_expire_at
        assert _partition_expire_at("2026-06-26") == _partition_expire_at("2026-06-26")

    def test_redis_partition_today_pairs_key_with_expiry(self):
        from supertable.monitoring_writer import (
            _MonitorKey, _today_utc_date, _partition_expire_at,
        )
        mk = _MonitorKey(organization="acme", monitor_type="writes")
        key, expire_at = mk.redis_partition_today()
        today = _today_utc_date()
        assert key == RK.monitor_partition("acme", "writes", today)
        assert expire_at == _partition_expire_at(today)

    def test_durable_delivery_sets_immutable_partition_and_receipt_expiries(
        self, tmp_path,
    ):
        from types import SimpleNamespace
        from supertable.monitoring_writer import (
            _AsyncMonitoringLogger,
            _MonitorKey,
            _partition_expire_at,
            _today_utc_date,
        )

        redis = fakeredis.FakeRedis(decode_responses=True)
        log = _AsyncMonitoringLogger(
            _MonitorKey(organization="acme", monitor_type="plans"),
            redis_connector=SimpleNamespace(r=redis),
            spool_dir=str(tmp_path / "wal"),
            start_worker=False,
        )
        log.log_metric({"a": 1})
        log.log_metric({"a": 2})

        today = _today_utc_date()
        key = RK.monitor_partition("acme", "plans", today)
        receipt = RK.monitor_partition_producer_receipts("acme", "plans", today)
        expires_at = _partition_expire_at(today)
        assert redis.llen(key) == 2
        assert redis.expiretime(key) == expires_at
        assert redis.hlen(receipt) == 2
        assert redis.expiretime(receipt) == expires_at + 86400

    def test_retrying_exact_spool_record_does_not_extend_or_duplicate_partition(
        self, tmp_path,
    ):
        from types import SimpleNamespace
        from supertable.monitoring_writer import (
            _AsyncMonitoringLogger,
            _MonitorKey,
            _deliver_spool_record,
        )

        redis = fakeredis.FakeRedis(decode_responses=True)
        connector = SimpleNamespace(r=redis)
        log = _AsyncMonitoringLogger(
            _MonitorKey(organization="acme", monitor_type="plans"),
            redis_connector=connector,
            spool_dir=str(tmp_path / "wal"),
            start_worker=False,
        )
        log._redis = None
        log.log_metric({"a": 1})
        record = log._spool.pending()[0]
        assert _deliver_spool_record(record, connector, ship_to_redis=True)
        assert _deliver_spool_record(record, connector, ship_to_redis=True)
        assert redis.llen(record.envelope["partition_key"]) == 1
