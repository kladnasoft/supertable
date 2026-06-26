# supertable/engine/tests/test_engine_config.py
"""Tests for supertable.engine.engine_config.

Focus: ``normalize_memory_size`` (the unit "extender") and that the resolver
emits unit-suffixed, DuckDB-parseable memory values regardless of how the UI /
Redis stored them.  The core guarantee is that a bare number can never reach
``PRAGMA memory_limit`` and raise ``Unknown unit for memory: ''``.
"""

from __future__ import annotations

import duckdb
import pytest

from supertable.engine.engine_config import (
    normalize_memory_size,
    resolve_engine_configs,
)


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
            "lite": {"duckdb_memory_limit": "2", "duckdb_external_cache_size": "50"},
            "pro": {"duckdb_memory_limit": "8", "duckdb_external_cache_size": "50"},
        }

        class _Catalog:
            def get_engine_config(self, org):
                return stored

        cfgs = resolve_engine_configs("kladna-soft", _Catalog())
        assert cfgs["lite"].duckdb_memory_limit == "2GB"
        assert cfgs["pro"].duckdb_memory_limit == "8GB"
        assert cfgs["lite"].duckdb_external_cache_size == "50GB"

        # And the resolved values are all DuckDB-parseable.
        con = duckdb.connect(":memory:")
        con.execute(f"PRAGMA memory_limit='{cfgs['pro'].duckdb_memory_limit}';")
        con.close()
