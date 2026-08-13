"""Standalone query-engine benchmarks.

The benchmark package is deliberately separate from the production routing
code.  Importing it does not generate data or start a benchmark; use
``python -m supertable.engine.benchmarks`` explicitly.
"""

from .corpus import TIER_TARGET_BYTES, CorpusSpec, prepare_corpus

__all__ = ["TIER_TARGET_BYTES", "CorpusSpec", "prepare_corpus"]
