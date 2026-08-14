# supertable/engine/tests/test_engine_config.py
"""Tests for supertable.engine.engine_config.

Focus: ``normalize_memory_size`` (the unit "extender") and that the resolver
emits unit-suffixed, DuckDB-parseable memory values regardless of how the UI /
Redis stored them.  The core guarantee is that a bare number can never reach
``PRAGMA memory_limit`` and raise ``Unknown unit for memory: ''``.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import duckdb
import pytest

from supertable.engine.engine_config import (
    match_auto_routing_policy,
    normalize_auto_routing_policy,
    normalize_memory_size,
    resolve_engine_configs,
)
from supertable.engine.engine_enum import Engine
from supertable.redis_catalog import RedisCatalog


class TestNormalizeMemorySize:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2", "2GB"),          # bare number -> GB
            ("8", "8GB"),
            ("50", "50GB"),
            (2, "2GB"),            # non-string input
            ("2.5", "2.5GB"),
            ("2GB", "2GB"),        # already valid -> passthrough
            ("2gb", "2GB"),        # lowercase -> canonical
            ("2gib", "2GiB"),      # binary unit canonicalised
            ("512mb", "512MB"),
            ("  4 GB ", "4GB"),    # whitespace tolerant
        ],
    )
    def test_extends_and_canonicalises(self, value, expected):
        assert normalize_memory_size(value) == expected

    @pytest.mark.parametrize("value", ["", "   ", None, "0", "-5", "abc", "GB", "1XB"])
    def test_invalid_falls_back_to_default(self, value):
        assert normalize_memory_size(value, default="1GB") == "1GB"

    def test_empty_default_for_cache(self):
        # Cache uses ""(=disabled) as the fallback rather than a size.
        assert normalize_memory_size("", default="") == ""
        assert normalize_memory_size("0", default="") == ""
        assert normalize_memory_size("50", default="") == "50GB"

    @pytest.mark.parametrize(
        "raw", ["2", "8", "50", "0", "", None, "abc", "2gib", "512mb", "-1"]
    )
    def test_output_always_accepted_by_duckdb(self, raw):
        """The whole point: the normalized value never raises a ParserException."""
        con = duckdb.connect(":memory:")
        normalized = normalize_memory_size(raw, default="1GB")
        con.execute(f"PRAGMA memory_limit='{normalized}';")  # must not raise
        con.close()


class TestResolverNormalizes:
    def test_bare_numbers_become_unit_suffixed(self):
        # Simulates the exact Redis doc the user reported (bare 2 / 8 / 50).
        stored = {
            "duckdb": {
                "duckdb_memory_limit": "8",
                "duckdb_external_cache_size": "50",
            },
        }

        class _Catalog:
            def get_engine_config(self, org):
                return stored

        cfgs = resolve_engine_configs("kladna-soft", _Catalog())
        assert cfgs["duckdb"].duckdb_memory_limit == "8GB"
        assert cfgs["duckdb"].duckdb_external_cache_size == "50GB"

        # And the resolved values are all DuckDB-parseable.
        con = duckdb.connect(":memory:")
        con.execute(f"PRAGMA memory_limit='{cfgs['duckdb'].duckdb_memory_limit}';")
        con.close()


class TestAutoRoutingPolicy:
    def test_half_open_ranges_and_unbounded_tail(self):
        policy = normalize_auto_routing_policy([
            {"min_bytes": 100, "max_bytes": None, "engine": "spark_sql"},
            {"min_bytes": 0, "max_bytes": 100, "engine": "islanddb"},
        ])
        assert match_auto_routing_policy(policy, 0).engine is Engine.ISLANDDB
        assert match_auto_routing_policy(policy, 99).engine is Engine.ISLANDDB
        assert match_auto_routing_policy(policy, 100).engine is Engine.SPARK_SQL

    @pytest.mark.parametrize("rules", [
        [{"min_bytes": 0, "max_bytes": 0, "engine": "islanddb"}],
        [{"min_bytes": 0, "max_bytes": 100, "engine": "auto"}],
        [
            {"min_bytes": 0, "max_bytes": 100, "engine": "islanddb"},
            {"min_bytes": 99, "max_bytes": 200, "engine": "duckdb"},
        ],
    ])
    def test_invalid_policy_is_rejected_as_one_document(self, rules):
        with pytest.raises(ValueError):
            normalize_auto_routing_policy(rules)

    def test_catalog_persists_canonical_policy_and_preserves_config(self):
        catalog = RedisCatalog.__new__(RedisCatalog)
        catalog.r = MagicMock()
        catalog.get_engine_config = lambda _org: {"duckdb": {"duckdb_threads": "2"}}

        assert catalog.set_auto_routing_policy("acme", [
            {"min_bytes": 100, "max_bytes": None, "engine": "spark_sql"},
            {"min_bytes": 0, "max_bytes": 100, "engine": "islanddb"},
        ])

        stored = json.loads(catalog.r.set.call_args.args[1])
        assert stored["duckdb"] == {"duckdb_threads": "2"}
        assert stored["auto_policy"] == [
            {"min_bytes": 0, "max_bytes": 100, "engine": "islanddb"},
            {"min_bytes": 100, "max_bytes": None, "engine": "spark_sql"},
        ]

    def test_catalog_does_not_mutate_redis_on_invalid_policy(self):
        catalog = RedisCatalog.__new__(RedisCatalog)
        catalog.r = MagicMock()
        with pytest.raises(ValueError):
            catalog.set_auto_routing_policy("acme", [
                {"min_bytes": 10, "max_bytes": 5, "engine": "islanddb"},
            ])
        catalog.r.set.assert_not_called()
