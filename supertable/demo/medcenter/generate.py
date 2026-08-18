"""One-shot generation of the medcenter demo fixtures — all eight sources.

Per month, writes one file per raw table under ``medcenter_demo_data/``:
semicolon CSVs for the file-based sources (Mobimed, bank camt.053 entries,
BMD journal, Hobex settlements) and parquet for the API sources
(Chargebee, Stripe, Zoho, Domonda), mirroring how each feed would really
arrive. Deterministic under the configured seed: re-running reproduces
byte-identical files.

Single month (default) or a full year::

    supertable-demo-medcenter-generate                 # one month (2026-07)
    supertable-demo-medcenter-generate --month 2026-03
    supertable-demo-medcenter-generate --year 2026     # twelve months

Seeds and document-sequence offsets derive from the calendar month itself,
so every month's files are identical whether generated alone or as part of
a year — single-month and full-year runs mix freely without key
collisions, and re-generating any month is an idempotent refresh.
"""

import argparse
from pathlib import Path

import pandas as pd

from supertable.config.homedir import initialize_app_home
from supertable.demo.medcenter.core import (
    GenerationConfig,
    MedcenterDataGenerator,
)
from supertable.demo.medcenter.defaults import (
    category_default,
    category_prefix_rules,
    csv_tables,
    demo_month,
    generated_data_dir,
)
from supertable.demo.medcenter.finance_sources import FinanceSourcesGenerator
from supertable.demo.medcenter.defaults import (
    raw_mobimed_invoices,
    raw_mobimed_payments,
)


def _write_table(
    output_root: Path, table_name: str, df: pd.DataFrame, month: str
) -> Path:
    table_dir = output_root / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    if table_name in csv_tables:
        file_path = table_dir / f"{table_name}_{month}.csv"
        df.to_csv(file_path, sep=";", index=False, float_format="%.2f")
    else:
        file_path = table_dir / f"{table_name}_{month}.parquet"
        df.to_parquet(file_path, index=False)
    return file_path


def _write_category_rules(output_root: Path) -> Path:
    rules = pd.DataFrame(
        [
            {"prefix": prefix, "category": category}
            for prefix, category in category_prefix_rules.items()
        ]
        + [{"prefix": "DEFAULT", "category": category_default}]
    )
    rules_path = output_root / "demo_category_rules.csv"
    rules.to_csv(rules_path, sep=";", index=False)
    return rules_path


def _month_ordinal(month: str) -> int:
    """Absolute month number (year*12 + month-1) — the mode-independent
    basis for seeds and sequence offsets."""
    year_str, month_str = month.split("-")
    return int(year_str) * 12 + int(month_str) - 1


def generate_months(
    months: list[str],
    output_dir: str = generated_data_dir,
    seed: int = 42,
    n_invoices: int = 500,
    n_payments: int = 700,
) -> dict:
    """Generate all source tables, one file set per month.

    Seeds and year-scoped sequence offsets are derived from the CALENDAR
    month, not from the position in the requested list — so
    ``--month 2026-07`` produces byte-identical files to the July slice of
    ``--year 2026``. Single-month and full-year runs therefore never
    collide: re-generating one month on top of a year load is a pure
    idempotent refresh of exactly that month.
    """
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    all_paths: dict = {}
    for month in months:
        ordinal = _month_ordinal(month)
        month_in_year = ordinal % 12  # 0-based position inside its year
        config = GenerationConfig(
            output_dir=output_dir,
            seed=seed + ordinal,  # distinct, reproducible, mode-independent
            month=month,
            n_invoices=n_invoices,
            n_payments=n_payments,
            invoice_sequence_start=1 + month_in_year * n_invoices,
            payment_sequence_start=1 + month_in_year * n_payments,
            month_index=month_in_year,
        )
        mobimed = MedcenterDataGenerator(config).run()
        finance = FinanceSourcesGenerator(
            config,
            mobimed[raw_mobimed_invoices],
            mobimed[raw_mobimed_payments],
        ).run()
        tables = {**mobimed, **finance}

        print(f"Generated medcenter demo fixtures for {month}:")
        paths = {}
        for name, df in tables.items():
            paths[name] = _write_table(output_root, name, df, month)
            print(f"- {name}: {len(df):,} rows")
        all_paths[month] = paths

    rules_path = _write_category_rules(output_root)
    print(f"- category rules -> {rules_path}")
    return all_paths


def generate(
    output_dir: str = generated_data_dir,
    month: str = demo_month,
    seed: int = 42,
    n_invoices: int = 500,
    n_payments: int = 700,
) -> dict:
    return generate_months(
        [month],
        output_dir=output_dir,
        seed=seed,
        n_invoices=n_invoices,
        n_payments=n_payments,
    )[month]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate medcenter demo fixtures")
    ap.add_argument("--output-dir", default=generated_data_dir)
    scope = ap.add_mutually_exclusive_group()
    scope.add_argument("--month", default=None, help="YYYY-MM (one month)")
    scope.add_argument(
        "--year", type=int, default=None, help="Generate all 12 months of YYYY"
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--invoices", type=int, default=500,
        help="Mobimed invoice rows per month",
    )
    ap.add_argument(
        "--payments", type=int, default=700,
        help="Mobimed payment rows per month",
    )
    return ap.parse_args()


def main() -> None:
    initialize_app_home(change_cwd=True)
    args = parse_args()
    if args.year is not None:
        months = [f"{args.year}-{m:02d}" for m in range(1, 13)]
    else:
        months = [args.month or demo_month]
    generate_months(
        months,
        output_dir=args.output_dir,
        seed=args.seed,
        n_invoices=args.invoices,
        n_payments=args.payments,
    )


if __name__ == "__main__":
    main()
