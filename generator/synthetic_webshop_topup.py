"""
synthetic_webshop_topup.py

Continuous top-up runner for the synthetic webshop dataset.

Can be run standalone from scratch — if dimension parquet files are absent it
generates categories, products and customers automatically, writes them to
SuperTable, saves them to disk, then fills in all transactional data from
(now - cold_start_years) up to now before entering the normal sleep rhythm.

Workflow (repeats every --sleep-minutes, default 5):
  1. Ensure dimensions exist (generate if absent — cold-start path).
  2. Query SuperTable for max timestamps and max IDs across all transactional tables.
  3. Scale event counts to the time gap using a random TPM in [--min-tpm, --max-tpm].
  4. Reuse WebshopDataGenerator internals to generate new rows (last_ts → now).
  5. Write directly to SuperTable via DataWriter — no intermediate files.

Usage:
    python synthetic_webshop_topup.py                        # normal / resume
    python synthetic_webshop_topup.py --cold-start-years 1  # force cold start
    python synthetic_webshop_topup.py --sleep-minutes 1 --min-tpm 3 --max-tpm 8
"""

from __future__ import annotations

import argparse
import time
import traceback
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa

import supertable.config.homedir  # noqa: F401 — required side-effect import
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter

from synthetic_webshop_data_core import GenerationConfig, WebshopDataGenerator
from synthetic_webshop_defaults import (
    generated_data_dir,
    organization,
    overwrite_columns,
    role_name,
    super_name,
)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TopUpConfig:
    """Runtime settings for the top-up runner."""
    sleep_minutes: float = 5.0          # sleep duration between iterations
    min_tpm: float = 1.0                # minimum simulated transactions/minute
    max_tpm: float = 10.0               # maximum simulated transactions/minute
    data_dir: str = generated_data_dir  # directory containing bulk parquet files
    seed: int = 99                      # RNG seed (distinct from bulk-gen seed of 42)
    cold_start_years: float = 1.0       # how far back to seed on a cold start


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _fmt(seconds: float) -> str:
    return str(timedelta(seconds=max(0.0, float(seconds))))


def _query_scalar(sql: str, fallback=None):
    """
    Run a single-cell SQL query against SuperTable and return the value.
    Returns `fallback` on error or empty result.
    """
    reader = DataReader(organization=organization, super_name=super_name, query=sql)
    df, status, _ = reader.execute(role_name=role_name)
    if status == Status.ERROR or df.empty:
        return fallback
    val = df.iloc[0, 0]
    return fallback if pd.isna(val) else val


def _to_arrow(df: pd.DataFrame) -> pa.Table:
    """
    Convert a DataFrame to a PyArrow Table, ensuring no column has dtype
    pa.null().  When every value in a column is None, PyArrow infers the
    type as null — Polars then refuses to run min/max statistics on it
    inside DataWriter.  We cast such columns to a concrete type derived
    from the pandas dtype so the writer always receives a typed schema.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)

    new_columns: list[pa.ChunkedArray] = []
    new_fields:  list[pa.Field]        = []

    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_null(field.type):
            # Infer a concrete Arrow type from the pandas dtype
            pd_dtype = df[field.name].dtype
            if pd.api.types.is_integer_dtype(pd_dtype):
                target = pa.int64()
            elif pd.api.types.is_float_dtype(pd_dtype):
                target = pa.float64()
            elif pd.api.types.is_bool_dtype(pd_dtype):
                target = pa.bool_()
            else:
                target = pa.string()   # object / datetime-like all-null → string
            col   = col.cast(target)
            field = pa.field(field.name, target, nullable=True)
        new_columns.append(col)
        new_fields.append(field)

    return pa.table(
        {f.name: c for f, c in zip(new_fields, new_columns)},
        schema=pa.schema(new_fields),
    )


def _write(writer: DataWriter, name: str, df: pd.DataFrame) -> Tuple[int, int, int]:
    """Write a DataFrame to SuperTable and print a one-line summary."""
    if df.empty:
        print(f"  [skip]  {name:<26} — nothing to write")
        return 0, 0, 0
    _, rows, inserted, deleted = writer.write(
        role_name=role_name,
        simple_name=name,
        data=_to_arrow(df),
        overwrite_columns=overwrite_columns,
    )
    print(f"  [write] {name:<26} rows={rows:,}  inserted={inserted:,}  deleted={deleted:,}")
    return rows, inserted, deleted


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap: read high-water marks from SuperTable
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SystemState:
    """Current high-water marks read from SuperTable."""
    last_order_ts: pd.Timestamp
    last_session_ts: pd.Timestamp
    last_snapshot_date: pd.Timestamp
    max_order_id: int
    max_order_item_id: int
    max_session_id: int
    max_pageview_id: int


def bootstrap_state(config: TopUpConfig) -> SystemState:
    """
    Query SuperTable for the latest timestamps and max IDs across all
    transactional tables.  Falls back to (now - sleep_minutes) for timestamps
    and 0 for IDs when a table is empty or unreachable (e.g. first cold run).
    """
    print("  Bootstrapping state from SuperTable...")

    now = pd.Timestamp.now().floor("s")
    ts_fallback = now - pd.Timedelta(days=int(config.cold_start_years * 365))

    def _ts(val, fallback: pd.Timestamp) -> pd.Timestamp:
        return fallback if val is None else pd.Timestamp(val)

    def _int(val, fallback: int = 0) -> int:
        return fallback if val is None else int(val)

    state = SystemState(
        last_order_ts      = _ts(_query_scalar('SELECT MAX("order_timestamp") FROM "orders"'),             ts_fallback),
        last_session_ts    = _ts(_query_scalar('SELECT MAX("session_start")    FROM "sessions"'),           ts_fallback),
        last_snapshot_date = _ts(_query_scalar('SELECT MAX("snapshot_date")    FROM "inventory_snapshots"'), ts_fallback),
        max_order_id       = _int(_query_scalar('SELECT MAX("order_id")       FROM "orders"')),
        max_order_item_id  = _int(_query_scalar('SELECT MAX("order_item_id")  FROM "order_items"')),
        max_session_id     = _int(_query_scalar('SELECT MAX("session_id")     FROM "sessions"')),
        max_pageview_id    = _int(_query_scalar('SELECT MAX("pageview_id")    FROM "pageviews"')),
    )

    print(f"    last_order_ts      = {state.last_order_ts}")
    print(f"    last_session_ts    = {state.last_session_ts}")
    print(f"    last_snapshot_date = {state.last_snapshot_date.date()}")
    print(f"    max IDs            = orders:{state.max_order_id:,}  "
          f"items:{state.max_order_item_id:,}  "
          f"sessions:{state.max_session_id:,}  "
          f"pageviews:{state.max_pageview_id:,}")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Dimension bootstrap (generate if absent, load otherwise)
# ─────────────────────────────────────────────────────────────────────────────

def _dim_files_exist(data_dir: str) -> bool:
    base = Path(data_dir)
    return (
        (base / "customers" / "customers.parquet").exists() and
        (base / "products"  / "products.parquet").exists()
    )


def ensure_dimensions(
    config: TopUpConfig,
    writer: DataWriter,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (customers, products) DataFrames, generating them from scratch if
    the dimension parquet files don't exist yet (cold-start path).

    Cold-start path:
      1. Generate categories, products, customers via WebshopDataGenerator.
      2. Write all three to SuperTable.
      3. Save parquet files to disk for future runs.

    Normal path: just read the existing parquet files.
    """
    base = Path(config.data_dir)

    if _dim_files_exist(config.data_dir):
        customers = pd.read_parquet(base / "customers" / "customers.parquet")
        products  = pd.read_parquet(base / "products"  / "products.parquet")
        print(f"  Loaded dimensions: {len(customers):,} customers  {len(products):,} products")
        return customers, products

    # ── Cold start: generate dimensions ──────────────────────────────────────
    print("  No dimension files found — cold start: generating dimensions...")
    now = pd.Timestamp.now().floor("s")
    cold_start = now - pd.Timedelta(days=int(config.cold_start_years * 365))

    dim_cfg = GenerationConfig(
        output_dir=config.data_dir,
        seed=config.seed,
        n_customers=10_000,
        n_categories=12,
        n_products=500,
        n_orders=0,            # no transactions yet — handled by catchup
        n_sessions=0,
        n_inventory_days=0,
        start_date=str(cold_start.date()),
        end_date=str(now.date()),
        n_workers=1,
    )
    gen = WebshopDataGenerator(dim_cfg)

    print("  Generating categories...")
    categories = gen.generate_categories()

    print("  Generating products...")
    products = gen.generate_products(categories)

    print("  Generating customers...")
    customers = gen.generate_customers()

    # Write to SuperTable
    print("  Writing dimensions to SuperTable...")
    for name, df in [("categories", categories), ("products", products), ("customers", customers)]:
        _write(writer, name, df)

    # Save parquet files so future runs skip generation
    print("  Saving dimension parquet files...")
    for name, df in [("categories", categories), ("products", products), ("customers", customers)]:
        table_dir = base / name
        table_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(table_dir / f"{name}.parquet", index=False)
        print(f"    saved {config.data_dir}/{name}/{name}.parquet  ({len(df):,} rows)")

    print(f"  ✓ Dimensions ready: {len(customers):,} customers  {len(products):,} products")
    return customers, products


# ─────────────────────────────────────────────────────────────────────────────
# Batch generation
# ─────────────────────────────────────────────────────────────────────────────

def _event_counts(gap_seconds: float, config: TopUpConfig, rng: np.random.Generator) -> Tuple[int, int]:
    """
    Scale event counts to the gap window.

    n_sessions = gap_minutes × random TPM in [min_tpm, max_tpm]
    n_orders   = n_sessions × 11% (matches the 11% conversion rate in the core generator)
    """
    tpm        = float(rng.uniform(config.min_tpm, config.max_tpm))
    gap_minutes = gap_seconds / 60.0
    n_sessions = max(1, int(gap_minutes * tpm))
    n_orders   = max(1, int(n_sessions * 0.11))
    print(f"  gap={_fmt(gap_seconds)}  tpm={tpm:.2f}  n_sessions={n_sessions:,}  n_orders={n_orders:,}")
    return n_sessions, n_orders


def _make_generator(
    start: pd.Timestamp,
    end: pd.Timestamp,
    n_orders: int,
    n_sessions: int,
    n_inventory_days: int,
    rng: np.random.Generator,
) -> WebshopDataGenerator:
    """
    Build a WebshopDataGenerator scoped to the gap window.
    We use a fresh random seed each batch so the generated data looks organic.

    WebshopDataGenerator.__init__ requires end_date > start_date strictly at
    the date-string level.  When start and end share the same calendar date
    (common for sub-day windows) we pass end_date + 1 day for construction,
    then immediately override gen.start_date / gen.end_date with the real
    sub-day Timestamps so all generation methods use the correct window.
    """
    start_date_str = str(start.date())
    end_date_str   = str(end.date())

    # Ensure the constructor receives end_date > start_date
    if end_date_str <= start_date_str:
        end_date_str = str((end + pd.Timedelta(days=1)).date())

    cfg = GenerationConfig(
        output_dir="",                          # not used — we write directly
        seed=int(rng.integers(0, 2 ** 31)),
        n_orders=n_orders,
        n_sessions=n_sessions,
        n_inventory_days=n_inventory_days,
        max_items_per_order=10,
        n_workers=1,                            # single-threaded for top-up
        start_date=start_date_str,
        end_date=end_date_str,
    )
    gen = WebshopDataGenerator(cfg)
    # Restore exact sub-day timestamps — these are what all generation methods read
    gen.start_date = start
    gen.end_date   = end
    return gen


def generate_batch(
    state: SystemState,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    config: TopUpConfig,
    rng: np.random.Generator,
    window_end: pd.Timestamp,
) -> Dict[str, pd.DataFrame]:
    """
    Generate all transactional tables for the window [last_ts → window_end].

    `window_end` is always capped to at most (last_ts + sleep_minutes) by the
    caller, so each batch stays a manageable size regardless of the total gap.

    Tables produced:
      orders, order_items        — new transactions
      sessions, pageviews        — new web sessions
      inventory_snapshots        — one row per product per new calendar day
      product_daily_stats        — derived aggregate for the new period
    """
    # Use the most recent transactional timestamp as the window start
    last_ts     = max(state.last_order_ts, state.last_session_ts)
    gap_seconds = max(1.0, (window_end - last_ts).total_seconds())

    n_sessions, n_orders = _event_counts(gap_seconds, config, rng)

    gen = _make_generator(
        start=last_ts, end=window_end,
        n_orders=n_orders, n_sessions=n_sessions,
        n_inventory_days=0,   # inventory handled separately below
        rng=rng,
    )

    # ── Orders + order items ──────────────────────────────────────────────────
    orders, order_items = gen._generate_orders_and_items_range(
        customers=customers,
        products=products,
        start_id=state.max_order_id + 1,
        stop_id=state.max_order_id + n_orders + 1,
        show_progress=False,
    )
    # Assign sequential order_item_ids continuing from the max already stored
    order_items.insert(
        0, "order_item_id",
        np.arange(
            state.max_order_item_id + 1,
            state.max_order_item_id + len(order_items) + 1,
            dtype=np.int64,
        ),
    )

    # ── Sessions + pageviews ─────────────────────────────────────────────────
    sessions, pageviews = gen._generate_sessions_and_pageviews_range(
        customers=customers,
        products=products,
        start_id=state.max_session_id + 1,
        stop_id=state.max_session_id + n_sessions + 1,
        show_progress=False,
    )
    pageviews.insert(
        0, "pageview_id",
        np.arange(
            state.max_pageview_id + 1,
            state.max_pageview_id + len(pageviews) + 1,
            dtype=np.int64,
        ),
    )

    # ── Inventory snapshots (daily, only missing calendar days) ──────────────
    last_snap  = state.last_snapshot_date.normalize()
    today      = window_end.normalize()
    new_days   = pd.date_range(last_snap + pd.Timedelta(days=1), today, freq="D")

    if len(new_days) > 0:
        print(f"  Generating inventory snapshots for {len(new_days)} new day(s): "
              f"{new_days[0].date()} → {new_days[-1].date()}")
        snap_gen = _make_generator(
            start=new_days[0], end=new_days[-1],
            n_orders=0, n_sessions=0,
            n_inventory_days=len(new_days),
            rng=rng,
        )
        # _generate_inventory_snapshot_chunk derives its date range from
        # (end_date - n_inventory_days + 1 days → end_date), which matches new_days
        inventory = snap_gen._generate_inventory_snapshot_chunk(
            products=products, show_progress=False,
        )
    else:
        print("  Inventory snapshots: no new calendar days, skipping")
        inventory = pd.DataFrame()

    # ── Product daily stats (derived aggregate for the new period) ───────────
    if not pageviews.empty and not orders.empty:
        product_daily_stats = gen.generate_product_daily_stats(
            products=products,
            pageviews=pageviews,
            order_items=order_items,
            orders=orders,
        )
    else:
        product_daily_stats = pd.DataFrame()

    return {
        "orders":              orders,
        "order_items":         order_items,
        "sessions":            sessions,
        "pageviews":           pageviews,
        "inventory_snapshots": inventory,
        "product_daily_stats": product_daily_stats,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run(config: TopUpConfig) -> None:
    rng    = np.random.default_rng(config.seed)
    writer = DataWriter(super_name, organization)
    window = pd.Timedelta(minutes=config.sleep_minutes)

    print("=" * 72)
    print("Synthetic Webshop Top-Up Runner")
    print(f"  sleep={config.sleep_minutes} min  |  tpm={config.min_tpm}–{config.max_tpm}")
    print(f"  data_dir={config.data_dir}")
    print("=" * 72)

    # Ensure dimensions exist — generates them on cold start, loads from disk otherwise
    customers, products = ensure_dimensions(config, writer)

    outer = 0
    while True:
        outer += 1
        print(f"\n{'═' * 72}")
        print(f"[cycle {outer}]  {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # Bootstrap once per cycle — re-read real state from SuperTable
            state = bootstrap_state(config)
            last_ts = max(state.last_order_ts, state.last_session_ts)
            now     = pd.Timestamp.now().floor("s")
            behind  = now - last_ts

            if behind > window:
                total_windows = int(behind / window) + 1
                print(f"  ⚡ Catchup mode: {_fmt(behind.total_seconds())} behind "
                      f"≈ {total_windows} windows to process")
            else:
                total_windows = 1

            # ── Catchup: accumulate all windows in memory, then write once ───
            now     = pd.Timestamp.now().floor("s")
            last_ts = max(state.last_order_ts, state.last_session_ts)

            # Collect per-table lists of DataFrames across all windows
            accumulated: Dict[str, list] = {
                "orders": [], "order_items": [], "sessions": [],
                "pageviews": [], "inventory_snapshots": [], "product_daily_stats": [],
            }

            inner = 0
            t_gen = time.time()
            while True:
                now        = pd.Timestamp.now().floor("s")
                last_ts    = max(state.last_order_ts, state.last_session_ts)
                remaining  = (now - last_ts).total_seconds()

                if remaining <= 30:
                    break

                inner     += 1
                window_end = min(last_ts + window, now)

                if inner % 100 == 1:
                    pct = 100.0 * (1 - remaining / behind.total_seconds())
                    print(f"  generating window {inner}/{total_windows}  "
                          f"{last_ts.strftime('%m-%d %H:%M')} → "
                          f"{window_end.strftime('%m-%d %H:%M')}  "
                          f"({pct:.1f}% done)", flush=True)

                tables = generate_batch(
                    state, customers, products, config, rng,
                    window_end=window_end,
                )

                for name, df in tables.items():
                    if not df.empty:
                        accumulated[name].append(df)

                # Advance state locally — no SuperTable round-trip needed
                state.last_order_ts      = window_end
                state.last_session_ts    = window_end
                state.last_snapshot_date = window_end
                state.max_order_id      += len(tables["orders"])
                state.max_order_item_id += len(tables["order_items"])
                state.max_session_id    += len(tables["sessions"])
                state.max_pageview_id   += len(tables["pageviews"])

            gen_elapsed = time.time() - t_gen
            print(f"\n  ✓ Generated {inner} window(s) in {_fmt(gen_elapsed)} — writing...")

            # ── Single write pass ─────────────────────────────────────────────
            t_write = time.time()
            total_rows = total_inserted = 0
            for table_name, frames in accumulated.items():
                if not frames:
                    print(f"  [skip]  {table_name:<26} — nothing to write")
                    continue
                merged = pd.concat(frames, ignore_index=True)
                rows, inserted, _ = _write(writer, table_name, merged)
                total_rows     += rows
                total_inserted += inserted

            write_elapsed = time.time() - t_write
            print(f"  ✓ Caught up — rows={total_rows:,}  inserted={total_inserted:,}  "
                  f"write={_fmt(write_elapsed)}")

        except Exception as exc:
            print(f"  [ERROR] {exc}")
            traceback.print_exc()

        sleep_secs = config.sleep_minutes * 60
        next_run   = pd.Timestamp.now() + pd.Timedelta(seconds=sleep_secs)
        print(f"\n  Sleeping {config.sleep_minutes} min "
              f"(next run ≈ {next_run.strftime('%H:%M:%S')})...")
        time.sleep(sleep_secs)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> TopUpConfig:
    ap = argparse.ArgumentParser(
        description="Continuous synthetic webshop top-up — runs forever, Ctrl-C to stop",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--sleep-minutes", "-s", type=float, default=5.0,
                    help="Minutes to sleep between iterations")
    ap.add_argument("--min-tpm", type=float, default=1.0,
                    help="Minimum simulated transactions per minute")
    ap.add_argument("--max-tpm", type=float, default=10.0,
                    help="Maximum simulated transactions per minute")
    ap.add_argument("--data-dir", default=generated_data_dir,
                    help="Directory containing bulk-generated parquet files")
    ap.add_argument("--cold-start-years", type=float, default=1.0,
                    help="How many years back to seed history on a cold start (no existing data)")
    ap.add_argument("--seed", type=int, default=99,
                    help="RNG seed (use a different value from the bulk-gen seed 42)")
    args = ap.parse_args()
    return TopUpConfig(
        sleep_minutes=args.sleep_minutes,
        min_tpm=args.min_tpm,
        max_tpm=args.max_tpm,
        data_dir=args.data_dir,
        seed=args.seed,
        cold_start_years=args.cold_start_years,
    )


if __name__ == "__main__":
    run(parse_args())
