"""End-to-end medcenter demo: all eight finance sources on one platform.

    generate -> load raw (idempotent) -> staging + marts -> quality tests
    -> showcase views (revenue, reconciliation, open items, deferred,
    doctor billing, category slicer) -> accounting exports
    -> idempotency proof

``--teardown`` drops the demo tables so the whole run can be repeated from
zero — re-running after a teardown reproduces identical results, because
the generator is seeded and every load/transform is an upsert.
"""

import argparse
import sys

import supertable.config.homedir  # noqa: F401  (resolves the app home)
from supertable.demo.medcenter import defaults
from supertable.demo.medcenter.export_accounting import (
    export_accounting_import,
)
from supertable.demo.medcenter.generate import generate_months
from supertable.demo.medcenter.helpers import run_query
from supertable.demo.medcenter.load import RAW_TABLES, load
from supertable.demo.medcenter.quality import run_quality
from supertable.demo.medcenter.transform import transform

DEMO_TABLES = RAW_TABLES + [
    defaults.stg_invoices_table,
    defaults.mart_monthly_table,
    defaults.mart_revenue_monthly,
    defaults.mart_bank_reconciliation,
    defaults.mart_open_invoices,
    defaults.mart_doctor_monthly,
    defaults.mart_deferred_revenue,
    defaults.mart_settlement_recon,
]


def banner(step: str) -> None:
    print("\n" + "=" * 78)
    print(step)
    print("=" * 78)


def teardown() -> None:
    from supertable.simple_table import SimpleTable
    from supertable.super_table import SuperTable

    super_table = SuperTable(
        super_name=defaults.super_name, organization=defaults.organization
    )
    for table_name in DEMO_TABLES:
        try:
            SimpleTable(
                super_table=super_table, simple_name=table_name
            ).delete(role_name=defaults.role_name)
            print(f"Deleted {table_name}")
        except Exception as exc:  # table may simply not exist yet
            print(f"Skipping {table_name}: {exc}")


def show(title: str, query: str) -> None:
    print(f"\n{title}:")
    print(run_query(query).to_string(index=False))


def showcase() -> None:
    show(
        "Revenue by month, source system and legal entity",
        f"SELECT * FROM {defaults.mart_revenue_monthly} "
        f"ORDER BY month, system, entity",
    )
    show(
        "Bank reconciliation summary (every statement line classified)",
        f"SELECT matched_system, match_method, "
        f"CAST(COUNT(*) AS INTEGER) AS lines, "
        f"ROUND(SUM(amount), 2) AS amount "
        f"FROM {defaults.mart_bank_reconciliation} "
        f"GROUP BY matched_system, match_method "
        f"ORDER BY matched_system, match_method",
    )
    show(
        "Review queue — bank lines nobody could match automatically",
        f"SELECT entry_id, booking_date, amount, counterparty_name, "
        f"remittance_unstructured "
        f"FROM {defaults.mart_bank_reconciliation} "
        f"WHERE match_method = 'review' ORDER BY booking_date",
    )
    show(
        "Open invoices by system",
        f"SELECT system, CAST(COUNT(*) AS INTEGER) AS open_invoices, "
        f"ROUND(SUM(amount_open), 2) AS amount_open "
        f"FROM {defaults.mart_open_invoices} GROUP BY system ORDER BY system",
    )
    show(
        "Revenue timing — billed now vs earned later (service periods)",
        f"SELECT * FROM {defaults.mart_deferred_revenue} "
        f"ORDER BY month, system",
    )
    show(
        "Doctor billing (top 5 by gross)",
        f"SELECT * FROM {defaults.mart_doctor_monthly} "
        f"ORDER BY gross_total DESC LIMIT 5",
    )
    show(
        "Card settlements — Hobex vs Mobimed vs bank (first 5 days)",
        f"SELECT settlement_date, transaction_count, gross_amount, "
        f"mobimed_card_gross, delta_gross, net_amount, bank_credit, "
        f"delta_net "
        f"FROM {defaults.mart_settlement_recon} "
        f"ORDER BY settlement_date LIMIT 5",
    )

    mart = defaults.mart_monthly_table
    show(
        "Category slicer — full mart (month x category)",
        f"SELECT * FROM {mart} ORDER BY invoice_month, category",
    )
    show(
        "Isolate UNIQA",
        f"SELECT invoice_month, category, invoice_count, gross_total "
        f"FROM {mart} WHERE category = 'UNIQA' ORDER BY invoice_month",
    )
    show(
        "Exclude UNIQA",
        f"SELECT invoice_month, "
        f"CAST(SUM(invoice_count) AS INTEGER) AS invoice_count, "
        f"ROUND(SUM(gross_total), 2) AS gross_total "
        f"FROM {mart} WHERE category != 'UNIQA' GROUP BY invoice_month "
        f"ORDER BY invoice_month",
    )


def snapshot_for_idempotency() -> tuple[int, str, str]:
    stg_count = int(run_query(
        f"SELECT COUNT(*) FROM {defaults.stg_invoices_table}"
    ).iloc[0, 0])
    mart_csv = run_query(
        f"SELECT * FROM {defaults.mart_monthly_table} "
        f"ORDER BY invoice_month, category"
    ).to_csv(index=False)
    recon_csv = run_query(
        f"SELECT matched_system, match_method, COUNT(*) AS n "
        f"FROM {defaults.mart_bank_reconciliation} "
        f"GROUP BY matched_system, match_method ORDER BY 1, 2"
    ).to_csv(index=False)
    return stg_count, mart_csv, recon_csv


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the full medcenter demo")
    ap.add_argument("--data-dir", default=defaults.generated_data_dir)
    ap.add_argument("--export-dir", default=defaults.export_dir)
    scope = ap.add_mutually_exclusive_group()
    scope.add_argument("--month", default=None, help="YYYY-MM (one month)")
    scope.add_argument(
        "--year", type=int, default=None,
        help="Run the demo over all 12 months of YYYY",
    )
    ap.add_argument(
        "--teardown",
        action="store_true",
        help="Drop the demo tables and exit (then re-run to prove "
        "reproducibility)",
    )
    ap.add_argument(
        "--skip-generate",
        action="store_true",
        help="Reuse existing fixtures instead of regenerating",
    )
    args = ap.parse_args()

    if args.teardown:
        banner("TEARDOWN — dropping demo tables")
        teardown()
        return

    if args.year is not None:
        months = [f"{args.year}-{m:02d}" for m in range(1, 13)]
    else:
        months = [args.month or defaults.demo_month]
    export_month = months[-1] if args.year is None else months[6]

    if not args.skip_generate:
        banner("STEP 1 — generate synthetic fixtures for all 8 sources")
        generate_months(months, output_dir=args.data_dir)

    banner("STEP 2 — load fixtures into raw_* tables (idempotent upsert)")
    load(data_dir=args.data_dir)

    banner("STEP 3 — build staging + marts")
    transform()

    banner("STEP 4 — data-quality tests")
    if not run_quality(data_dir=args.data_dir):
        print("Quality tests failed — aborting demo run.")
        sys.exit(1)

    banner("STEP 5 — showcase: the views finance actually asks for")
    showcase()

    banner(
        f"STEP 6 — 12-column accounting-import exports ({export_month})"
    )
    export_accounting_import(month=export_month, output_dir=args.export_dir)

    banner("STEP 7 — idempotency proof: re-load + re-transform, compare")
    before = snapshot_for_idempotency()
    load(data_dir=args.data_dir)
    transform()
    after = snapshot_for_idempotency()

    if before == after:
        print(
            f"\nIdempotency PASS: staging still {after[0]} rows, monthly "
            f"mart and reconciliation byte-identical after the second run."
        )
    else:
        print(
            f"\nIdempotency FAIL: staging {before[0]} -> {after[0]}, "
            f"mart {'unchanged' if before[1] == after[1] else 'CHANGED'}, "
            f"recon {'unchanged' if before[2] == after[2] else 'CHANGED'}"
        )
        sys.exit(1)

    banner("Demo complete")


if __name__ == "__main__":
    main()
