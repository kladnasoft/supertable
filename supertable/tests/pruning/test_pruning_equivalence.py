# route: supertable.tests.pruning.test_pruning_equivalence
"""Pruning must return what a full scan returns — for every shape of SQL.

Pruning may only skip a file that provably holds no matching row. Fullscan
reads every file, so it cannot be wrong about which rows match; running the
same query both ways turns that rule into a check.

This is the regression net for the pruning bugs found by the audit:

  * a naive timestamp literal compared against UTC stats, which lost rows
    within one UTC offset of the bound (fixed 3.0.8);
  * a bare string compared against a DATE column, where DuckDB casts the
    string to DATE and drops the time, while the pruner kept the full
    ``23:59:59`` and pruned the matching day away.

It also covers two reader bugs this corpus surfaced — an alias produced by a
subquery's window function being attributed to the base table, and a column
referenced through a derived-table alias never reaching the reflection view.
Both made the query fail outright, with pruning and without.

The corpus is generated (thousands of queries), so the default run takes a
STRATIFIED SAMPLE — every family is represented, and the sample is
deterministic so a failure is reproducible. Run the whole corpus with:

    STORAGE_TYPE=LOCAL python scripts/pruning_audit.py

The dataset is built once and reused; it is small on purpose so this stays a
test rather than a benchmark.
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Dict, List

import pytest

from supertable.tests.pruning import dataset as D
from supertable.tests.pruning.queries import Query, all_queries

# Per family, so a green run means every mechanism was exercised — not just
# whichever family happens to be longest.
PER_FAMILY = int(os.environ.get("SUPERTABLE_PRUNE_SAMPLE", "6"))


def _sample() -> List[Query]:
    by_family: Dict[str, List[Query]] = defaultdict(list)
    for q in all_queries():
        by_family[q[1]].append(q)
    out: List[Query] = []
    for family in sorted(by_family):
        qs = by_family[family]
        # Spread across the family rather than taking the first N: the first
        # few share a literal form and would leave the rest uncovered.
        step = max(1, len(qs) // PER_FAMILY)
        out.extend(qs[::step][:PER_FAMILY])
    return out


SAMPLE = _sample()


@pytest.fixture(scope="session", autouse=True)
def _dataset():
    """Build the dataset, or skip the module if the live stack is unavailable.

    These are integration tests: they need a real Redis catalog and real
    storage. Run inside the full suite they can land after a test that has
    installed a FakeRedis, which leaks through the module-level client and
    fails with a missing command rather than anything to do with pruning.
    Skipping keeps that from reading as a pruning regression; running this
    directory on its own exercises them for real.
    """
    try:
        D.build(log=lambda *a, **k: None)
    except Exception as e:
        pytest.skip(f"live catalog/storage unavailable ({type(e).__name__}: "
                    f"{str(e)[:120]}); run supertable/tests/pruning on its own")


def _run(sql: str, fullscan: bool):
    from supertable.data_reader import DataReader, engine

    df, status, message = DataReader(
        super_name=D.SUPER, organization=D.ORG, query=sql, source="sdk",
    ).execute(role_name=D.ROLE, with_scan=False,
              engine=engine.AUTO, fullscan=fullscan)
    assert str(status).endswith("OK"), f"query failed: {message}\n{sql}"
    return df


@pytest.mark.parametrize("qid,family,sql", SAMPLE,
                         ids=[q[0] for q in SAMPLE])
def test_pruned_result_equals_fullscan(qid, family, sql):
    """The invariant, one query at a time."""
    import numpy as np
    import pandas as pd

    truth = _run(sql, fullscan=True)
    got = _run(sql, fullscan=False)

    assert list(got.columns) == list(truth.columns), sql
    assert len(got) == len(truth), (
        f"pruning changed the row count: {len(truth)} -> {len(got)}\n{sql}"
    )
    if len(truth) == 0:
        return

    cols = list(truth.columns)
    # polars compares frames directly, types included. The float tolerance that
    # used to live here existed because a SUM re-associates over a different
    # file order; that is still true, so floats are compared with a tolerance
    # while everything else must match exactly.
    import math

    a = truth.sort(cols)
    b = got.sort(cols)
    if a.equals(b):
        return
    for c in cols:
        av, bv = a[c].to_list(), b[c].to_list()
        for i, (x, y) in enumerate(zip(av, bv)):
            if isinstance(x, float) and isinstance(y, float):
                assert math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9) or (
                    math.isnan(x) and math.isnan(y)), (
                    f"column {c!r} row {i}: {x} vs {y}\n{sql}")
            else:
                assert x == y, f"column {c!r} row {i}: {x!r} vs {y!r}\n{sql}"


def test_sample_covers_every_family():
    """Guard the guard: a family that stops generating must fail loudly."""
    sampled = {q[1] for q in SAMPLE}
    every = {q[1] for q in all_queries()}
    assert sampled == every, f"families missing from the sample: {every - sampled}"


def test_pruning_is_actually_happening():
    """Equivalence is trivially satisfiable by never pruning anything.

    Without this, disabling pruning outright would make every other test in
    this file pass.
    """
    from importlib import import_module

    est = import_module("supertable.engine.data_estimator")
    seen = {"before": 0, "after": 0}
    orig = est.prune_files_by_predicates

    def spy(file_keys, *a, **k):
        out = orig(file_keys, *a, **k)
        seen["before"] += len(file_keys)
        seen["after"] += len(out)
        return out

    est.prune_files_by_predicates = spy
    try:
        _run("SELECT count(*) AS n FROM facts "
             "WHERE event_ts >= TIMESTAMP '2025-11-01 00:00:00'", fullscan=False)
    finally:
        est.prune_files_by_predicates = orig

    assert seen["before"] > 0, "pruning never ran"
    assert seen["after"] < seen["before"], (
        f"nothing was pruned ({seen['after']}/{seen['before']} files kept); "
        f"equivalence would be meaningless"
    )
