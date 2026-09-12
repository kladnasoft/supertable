# route: supertable.tests.pruning.dataset
"""A multi-table dataset built to make pruning observable and checkable.

Every table is written one file at a time, and each file owns a CONTIGUOUS
slice of the time span. That is what makes a date or timestamp predicate
prunable to a known number of files — and therefore what makes a pruning bug
visible as a wrong answer rather than as noise.

Shapes are deliberately varied because pruning is per-column-type:

  * ``facts``     — the big table, 24 files, joins to both dimensions
  * ``customers`` — dimension, joins on ``cust_id``
  * ``products``  — dimension, joins on ``prod_id``
  * ``events``    — a second fact table, for fact-to-fact joins

Columns cover every lane the pruner has: bigint, double, string, date, naive
timestamp, and timezone-aware timestamp. The tz-aware column exists because a
naive literal compared against it is exactly the case that silently lost rows
before 3.0.8 — see test_prune_timezone_soundness.

Nulls are present on purpose: a column that is entirely null, and one that is
partly null, are different pruning cases from a dense one.

Two shapes exist purely because the pruner compares values differently from the
way the ENGINE compares them, and the corpus could not express the difference:

  * ``vendor`` / ``contact`` — string values whose CASE varies from file to
    file. DuckDB reads with ``default_collation='nocase'``, so it matches
    ``'BOREALIS'`` against a stored ``'borealis'``; a byte-wise pruner does not.
    Every other string column here is uniformly lowercase, so the collation
    never came into play and the bug was invisible.
  * ``ts_ns`` — epoch NANOSECONDS, i.e. int64 values around 1.77e18, far above
    2^53. Every other integer here fits in a float64 exactly, so coercing the
    bounds to float was lossless and the bug was invisible. Values are spaced a
    round 1 ms apart so a query can name a file's exact edge, which is where a
    rounding error decides the wrong way.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict

import numpy as np
import pyarrow as pa

ORG = "prune_audit"
SUPER = "prune_audit"
ROLE = "superadmin"

# Fixed so the corpus is reproducible; never wall-clock.
ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)
SPAN_DAYS = 360

# Large enough that auto-compaction leaves the file layout alone, so the table
# really has the file count the queries are written against.
NO_COMPACT_CHUNK_BYTES = 64 * 1024 * 1024

COUNTRIES = ["AT", "DE", "CH", "HU", "SK", "CZ", "PL", "IT"]
TIERS = ["free", "silver", "gold", "platinum"]
STATUSES = ["paid", "open", "refunded", "cancelled"]
CATEGORIES = ["tools", "media", "food", "apparel", "misc"]
KINDS = ["click", "view", "purchase", "return"]

# --- mixed case ------------------------------------------------------------
# The same names, written in a different case in every third file — the shape
# a column ingested from two source systems really has. Each file's byte range
# therefore sits in a different part of the ASCII map ('A'=65 < 'Z'=90 < 'a'=97
# < 'z'=122) while the engine, reading with a case-insensitive collation, sees
# one single set of values. A file whose byte range does not straddle the
# literal is the case a byte-wise pruner drops and the engine would have
# matched.
VENDORS = ["acme", "borealis", "cygnus", "delta", "zenith"]
CONTACTS = ["anna", "bela", "csaba", "diana", "zoltan"]

#: How a file's string values are cased, by ``file_index % 3``.
CASE_STYLES = (str.lower, str.upper, str.capitalize)


def cased(values, file_index: int):
    """The pool *values* written in the case this file uses."""
    style = CASE_STYLES[file_index % len(CASE_STYLES)]
    return [style(v) for v in values]


# --- epoch nanoseconds -----------------------------------------------------
# 2026-01-01T00:00:00Z as epoch NANOSECONDS. ~1.77e18, i.e. between 2^60 and
# 2^61, where a float64 ULP is 256 — so 256 consecutive int64 values collapse
# onto one double and any comparison done in float cannot tell them apart.
NS_ANCHOR = 1_767_225_600_000_000_000
#: One millisecond between consecutive rows, so a file's edge is a round number
#: a query can name exactly.
NS_ROW_STEP = 1_000_000

FACT_FILES, FACT_ROWS = 24, 4_000
EVENT_FILES, EVENT_ROWS = 12, 2_500
CUST_FILES, CUST_ROWS = 6, 1_000
PROD_FILES, PROD_ROWS = 4, 500

TABLES = ("facts", "events", "customers", "products")


def _slice_bounds(file_index: int, n_files: int):
    """The contiguous time slice this file owns."""
    days_per_file = SPAN_DAYS / n_files
    lo = ANCHOR - timedelta(days=SPAN_DAYS - file_index * days_per_file)
    hi = ANCHOR - timedelta(days=SPAN_DAYS - (file_index + 1) * days_per_file)
    return lo, hi


def ns_bounds(file_index: int):
    """The ``ts_ns`` range file *file_index* of ``facts`` owns, inclusive.

    Exported because the query generator needs the exact edges: a literal one
    nanosecond off a file's max is the value a float64 comparison rounds onto
    the max itself, and that is the whole point of the column.
    """
    lo = NS_ANCHOR + file_index * FACT_ROWS * NS_ROW_STEP
    hi = lo + (FACT_ROWS - 1) * NS_ROW_STEP
    return lo, hi


def _ns_column(file_index: int, n: int) -> np.ndarray:
    lo, _ = ns_bounds(file_index)
    return lo + np.arange(n, dtype=np.int64) * NS_ROW_STEP


def _sorted_times(rng, lo: datetime, hi: datetime, n: int) -> np.ndarray:
    """Microsecond timestamps inside [lo, hi), sorted so stats are tight."""
    span_us = max(1, int((hi - lo).total_seconds() * 1_000_000))
    offs = rng.integers(0, span_us, size=n, dtype=np.int64)
    offs.sort()
    return int(lo.timestamp() * 1_000_000) + offs


def build_facts(i: int) -> pa.Table:
    rng = np.random.default_rng(90_001 + i)
    lo, hi = _slice_bounds(i, FACT_FILES)
    n = FACT_ROWS
    ts = _sorted_times(rng, lo, hi, n)

    # A column that is null in SOME files only: pruning must not treat an
    # all-null file's stats as a range that excludes everything.
    score = rng.normal(50, 15, size=n)
    if i % 5 == 0:
        score = np.full(n, np.nan)
    elif i % 3 == 0:
        score[rng.random(n) < 0.4] = np.nan

    vendors = cased(VENDORS, i)

    return pa.table({
        "fact_id": pa.array(np.arange(i * n, (i + 1) * n, dtype=np.int64)),
        "cust_id": pa.array(rng.integers(0, CUST_FILES * CUST_ROWS, size=n, dtype=np.int64)),
        "prod_id": pa.array(rng.integers(0, PROD_FILES * PROD_ROWS, size=n, dtype=np.int64)),
        # int64 far above 2^53 — a float64 cannot hold these exactly.
        "ts_ns": pa.array(_ns_column(i, n)),
        # Case varies per file; the engine's collation does not care, the
        # pruner's byte comparison does.
        "vendor": pa.array([vendors[k] for k in rng.integers(0, len(vendors), n)]),
        # Naive and tz-aware views of the same instant — the pruner must agree
        # with the engine on both.
        "event_ts": pa.array(ts, pa.timestamp("us")),
        "event_tstz": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "event_date": pa.array((ts // 86_400_000_000).astype(np.int32), pa.date32()),
        "amount": pa.array(np.round(rng.gamma(2.0, 45.0, size=n), 2)),
        "qty": pa.array(rng.integers(1, 40, size=n, dtype=np.int64)),
        "status": pa.array([STATUSES[k] for k in rng.integers(0, len(STATUSES), n)]),
        "region": pa.array([COUNTRIES[k] for k in rng.integers(0, len(COUNTRIES), n)]),
        "score": pa.array(score),
    })


def build_events(i: int) -> pa.Table:
    rng = np.random.default_rng(70_001 + i)
    lo, hi = _slice_bounds(i, EVENT_FILES)
    n = EVENT_ROWS
    ts = _sorted_times(rng, lo, hi, n)
    return pa.table({
        "ev_id": pa.array(np.arange(i * n, (i + 1) * n, dtype=np.int64)),
        "cust_id": pa.array(rng.integers(0, CUST_FILES * CUST_ROWS, size=n, dtype=np.int64)),
        "occurred_ts": pa.array(ts, pa.timestamp("us")),
        "occurred_date": pa.array((ts // 86_400_000_000).astype(np.int32), pa.date32()),
        "kind": pa.array([KINDS[k] for k in rng.integers(0, len(KINDS), n)]),
        "value": pa.array(np.round(rng.random(n) * 500, 2)),
    })


def build_customers(i: int) -> pa.Table:
    rng = np.random.default_rng(50_001 + i)
    lo, _ = _slice_bounds(i, CUST_FILES)
    n = CUST_ROWS
    signup = _sorted_times(rng, lo - timedelta(days=400), lo, n)
    contacts = cased(CONTACTS, i)
    return pa.table({
        "cust_id": pa.array(np.arange(i * n, (i + 1) * n, dtype=np.int64)),
        "signup_ts": pa.array(signup, pa.timestamp("us")),
        "signup_date": pa.array((signup // 86_400_000_000).astype(np.int32), pa.date32()),
        "country": pa.array([COUNTRIES[k] for k in rng.integers(0, len(COUNTRIES), n)]),
        # Mixed case on the DIMENSION side, so a cross-table string filter goes
        # through the same lane as a single-table one.
        "contact": pa.array([contacts[k] for k in rng.integers(0, len(contacts), n)]),
        "tier": pa.array([TIERS[k] for k in rng.integers(0, len(TIERS), n)]),
        "credit_limit": pa.array(np.round(rng.random(n) * 10_000, 2)),
    })


def build_products(i: int) -> pa.Table:
    rng = np.random.default_rng(30_001 + i)
    lo, _ = _slice_bounds(i, PROD_FILES)
    n = PROD_ROWS
    launch = _sorted_times(rng, lo - timedelta(days=500), lo, n)
    return pa.table({
        "prod_id": pa.array(np.arange(i * n, (i + 1) * n, dtype=np.int64)),
        "launch_date": pa.array((launch // 86_400_000_000).astype(np.int32), pa.date32()),
        "category": pa.array([CATEGORIES[k] for k in rng.integers(0, len(CATEGORIES), n)]),
        "price": pa.array(np.round(rng.random(n) * 900 + 5, 2)),
    })


_SPECS = {
    "facts": (FACT_FILES, build_facts),
    "events": (EVENT_FILES, build_events),
    "customers": (CUST_FILES, build_customers),
    "products": (PROD_FILES, build_products),
}


def table_row_counts() -> Dict[str, int]:
    return {
        "facts": FACT_FILES * FACT_ROWS,
        "events": EVENT_FILES * EVENT_ROWS,
        "customers": CUST_FILES * CUST_ROWS,
        "products": PROD_FILES * PROD_ROWS,
    }


#: One column per table that must be present for the build to count as current.
#: A row count alone cannot see a column being ADDED to the generator, so a
#: corpus extension would silently keep querying the old data and report a
#: green run against a dataset that cannot express the bug.
SCHEMA_SENTINELS = {
    "facts": ("ts_ns", "vendor"),
    "events": ("kind",),
    "customers": ("contact",),
    "products": ("category",),
}


def exists() -> bool:
    """True when every table is present with the expected rows AND columns."""
    from supertable.data_reader import DataReader, engine as engine_enum

    want = table_row_counts()
    for name, rows in want.items():
        cols = ", ".join(f"count({c}) AS c{i}"
                         for i, c in enumerate(SCHEMA_SENTINELS[name]))
        try:
            df, status, _ = DataReader(
                super_name=SUPER, organization=ORG,
                query=f"SELECT count(*) AS n, {cols} FROM {name}", source="sdk",
            ).execute(role_name=ROLE, with_scan=False, engine=engine_enum.AUTO)
            # polars frame; ``row(0, named=True)`` also works on the pandas
            # shim the reader used before 3.2, so this stays version-agnostic.
            got = int(df["n"][0])
            if not str(status).endswith("OK") or got != rows:
                return False
        except Exception:
            # A missing sentinel column makes the query fail to bind — which is
            # exactly the "schema moved on" signal we want, so rebuild.
            return False
    return True


def build(*, rebuild: bool = False, log=print) -> Dict[str, int]:
    """Create the dataset, or reuse it when it is already correct."""
    if not rebuild and exists():
        log("prune dataset: reusing existing build")
        return table_row_counts()

    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    SuperTable(super_name=SUPER, organization=ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)

    for name, (n_files, builder) in _SPECS.items():
        _drop(name, log=log)
        # Appends below would stack on top of a leftover table and double the
        # row count instead of replacing it.
        writer.configure_table(
            role_name=ROLE, simple_name=name,
            max_memory_chunk_size=NO_COMPACT_CHUNK_BYTES,
        )
        for i in range(n_files):
            writer.write(role_name=ROLE, simple_name=name,
                         data=builder(i), overwrite_columns=[])
        log(f"  {name}: {n_files} files")

    return table_row_counts()


def _drop(name: str, log=print) -> None:
    try:
        from supertable.redis_catalog import RedisCatalog
        RedisCatalog().delete_simple_table(ORG, SUPER, name)
    except Exception:
        pass
