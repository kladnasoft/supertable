"""Data-quality test suite for the medcenter demo (the dbt-test analogue).

Three layers:

  1. Schema + arithmetic tests on the Mobimed staging table (uniqueness,
     not-null, accepted values, gross = sum of parts, VAT = rate x net).
  2. Cross-system referential tests — the links the reconciliation views
     depend on: every Chargebee transaction and Stripe charge resolves to
     its invoice, Stripe payouts equal the sum of their balance
     transactions, Hobex settlements tie out against Mobimed card receipts
     and the bank credit, every booked BMD AR row points at a real source
     invoice, and every bank line got a match classification.
  3. The demo acceptance check: mart totals recomputed independently
     (pandas over the generated CSV fixtures) and compared row by row.

Exits non-zero when any test fails — that exit code is the hook a
scheduler's run-failure alert attaches to.
"""

import argparse
import os
import sys
from dataclasses import dataclass

import pandas as pd

from supertable.config.homedir import initialize_app_home
from supertable.demo.medcenter.defaults import (
    category_default,
    category_prefix_rules,
    generated_data_dir,
    mart_bank_reconciliation,
    mart_deferred_revenue,
    mart_monthly_table,
    mart_settlement_recon,
    payment_categories,
    raw_bmd_journal,
    raw_chargebee_invoices,
    raw_chargebee_transactions,
    raw_mobimed_invoices,
    raw_mobimed_payments,
    raw_stripe_balance_transactions,
    raw_stripe_charges,
    raw_stripe_payouts,
    raw_zoho_invoices,
    stg_invoices_table,
)
from supertable.demo.medcenter.helpers import run_query

# Amounts are rounded to 2 decimals at generation and staging time; 0.006
# covers the maximum rounding distance plus float noise. Sums over many
# rounded rows get a slightly wider budget.
ROUNDING_TOLERANCE = 0.006
SUM_TOLERANCE = 0.02


@dataclass
class TestResult:
    name: str
    passed: bool
    detail: str = ""


def _failure_count_test(name: str, query: str) -> TestResult:
    failures = int(run_query(query).iloc[0, 0])
    return TestResult(
        name=name,
        passed=failures == 0,
        detail="" if failures == 0 else f"{failures} failing row(s)",
    )


def mobimed_tests() -> list[TestResult]:
    accepted_categories = ", ".join(
        f"'{c}'"
        for c in list(category_prefix_rules.values()) + [category_default]
    )
    accepted_payment_categories = ", ".join(
        f"'{c}'" for c in payment_categories
    )
    amount_parts = (
        "net_vat0 + net_vat10 + net_vat20 + vat10_amount + vat20_amount"
    )

    results = [
        _failure_count_test(
            "unique__stg__invoice_number",
            f"SELECT COUNT(*) - COUNT(DISTINCT invoice_number) "
            f"FROM {stg_invoices_table}",
        ),
        _failure_count_test(
            "not_null__stg__key_columns",
            f"SELECT COUNT(*) FROM {stg_invoices_table} "
            f"WHERE invoice_number IS NULL OR invoice_date IS NULL "
            f"OR gross_total IS NULL OR category IS NULL "
            f"OR legal_entity IS NULL",
        ),
        _failure_count_test(
            "accepted_values__stg__category",
            f"SELECT COUNT(*) FROM {stg_invoices_table} "
            f"WHERE category NOT IN ({accepted_categories})",
        ),
        _failure_count_test(
            "arithmetic__stg__gross_equals_sum_of_parts",
            f"SELECT COUNT(*) FROM {stg_invoices_table} "
            f"WHERE ABS(gross_total - ({amount_parts})) > {ROUNDING_TOLERANCE}",
        ),
        _failure_count_test(
            "arithmetic__stg__vat10_is_10_percent",
            f"SELECT COUNT(*) FROM {stg_invoices_table} "
            f"WHERE ABS(vat10_amount - 0.10 * net_vat10) > {ROUNDING_TOLERANCE}",
        ),
        _failure_count_test(
            "arithmetic__stg__vat20_is_20_percent",
            f"SELECT COUNT(*) FROM {stg_invoices_table} "
            f"WHERE ABS(vat20_amount - 0.20 * net_vat20) > {ROUNDING_TOLERANCE}",
        ),
        _failure_count_test(
            "unique__raw_payments__belegnr",
            f"SELECT COUNT(*) - COUNT(DISTINCT belegnr) "
            f"FROM {raw_mobimed_payments}",
        ),
        _failure_count_test(
            "accepted_values__raw_payments__payment_category",
            f"SELECT COUNT(*) FROM {raw_mobimed_payments} "
            f"WHERE Payment_Category NOT IN ({accepted_payment_categories})",
        ),
    ]

    raw_count = int(
        run_query(f"SELECT COUNT(*) FROM {raw_mobimed_invoices}").iloc[0, 0]
    )
    stg_count = int(
        run_query(f"SELECT COUNT(*) FROM {stg_invoices_table}").iloc[0, 0]
    )
    results.append(
        TestResult(
            name="reconcile__raw_vs_stg__row_count",
            passed=raw_count == stg_count,
            detail=f"raw={raw_count} stg={stg_count}",
        )
    )

    stg_gross = float(
        run_query(
            f"SELECT COALESCE(SUM(gross_total), 0) FROM {stg_invoices_table}"
        ).iloc[0, 0]
    )
    mart_gross = float(
        run_query(
            f"SELECT COALESCE(SUM(gross_total), 0) FROM {mart_monthly_table}"
        ).iloc[0, 0]
    )
    results.append(
        TestResult(
            name="reconcile__stg_vs_mart__gross_total",
            passed=abs(stg_gross - mart_gross) <= SUM_TOLERANCE,
            detail=f"stg={stg_gross:.2f} mart={mart_gross:.2f}",
        )
    )
    return results


def cross_system_tests() -> list[TestResult]:
    return [
        _failure_count_test(
            "arithmetic__chargebee__total_equals_line_sum",
            f"SELECT COUNT(*) FROM ("
            f"  SELECT invoice_number FROM {raw_chargebee_invoices}"
            f"  GROUP BY invoice_number, total"
            f"  HAVING ABS(total - SUM(line_amount + line_tax_amount)) > 0.011)",
        ),
        _failure_count_test(
            "relationship__chargebee_transaction__invoice_exists",
            f"SELECT COUNT(*) FROM {raw_chargebee_transactions} t"
            f" LEFT JOIN (SELECT DISTINCT invoice_number"
            f"            FROM {raw_chargebee_invoices}) i"
            f" ON t.invoice_number = i.invoice_number"
            f" WHERE i.invoice_number IS NULL",
        ),
        _failure_count_test(
            "relationship__stripe_charge__source_invoice_exists",
            f"SELECT COUNT(*) FROM {raw_stripe_charges} c"
            f" LEFT JOIN (SELECT DISTINCT invoice_number"
            f"            FROM {raw_chargebee_invoices}) cb"
            f"   ON c.source_invoice_number = cb.invoice_number"
            f" WHERE c.source_class = 'chargebee'"
            f"   AND cb.invoice_number IS NULL",
        ),
        _failure_count_test(
            "arithmetic__stripe_balance__net_equals_amount_minus_fee",
            f"SELECT COUNT(*) FROM {raw_stripe_balance_transactions} "
            f"WHERE ABS(net - (amount - fee)) > 0.011",
        ),
        _failure_count_test(
            "reconcile__stripe_payout__equals_balance_net_sum",
            f"SELECT COUNT(*) FROM ("
            f"  SELECT p.payout_id FROM {raw_stripe_payouts} p"
            f"  JOIN {raw_stripe_balance_transactions} b"
            f"    ON p.payout_id = b.payout_id"
            f"  GROUP BY p.payout_id, p.amount"
            f"  HAVING ABS(p.amount - SUM(b.net)) > {SUM_TOLERANCE})",
        ),
        _failure_count_test(
            "reconcile__hobex__settlements_tie_out",
            f"SELECT COUNT(*) FROM {mart_settlement_recon} "
            f"WHERE ABS(delta_gross) > 0.011 OR ABS(delta_net) > 0.011",
        ),
        _failure_count_test(
            "relationship__bmd_ar__document_resolves_to_source_invoice",
            f"SELECT COUNT(*) FROM {raw_bmd_journal} j"
            f" WHERE j.booking_symbol = 'AR'"
            f" AND NOT EXISTS (SELECT 1 FROM {raw_chargebee_invoices} c"
            f"                 WHERE c.invoice_number = j.document_number)"
            f" AND NOT EXISTS (SELECT 1 FROM {raw_zoho_invoices} z"
            f"                 WHERE z.invoice_number = j.document_number)"
            f" AND NOT EXISTS (SELECT 1 FROM {raw_mobimed_invoices} m"
            f"                 WHERE m.Rechnungsnummer = j.document_number)",
        ),
        _failure_count_test(
            "reconcile__bank__every_line_classified",
            f"SELECT COUNT(*) FROM {mart_bank_reconciliation} "
            f"WHERE match_method IS NULL OR matched_system IS NULL",
        ),
        _failure_count_test(
            "reconcile__deferred__split_sums_to_billed",
            f"SELECT COUNT(*) FROM {mart_deferred_revenue} "
            f"WHERE ABS(billed_total - revenue_current_month"
            f"          - deferred_to_future) > {SUM_TOLERANCE}",
        ),
    ]


def review_queue_present() -> TestResult:
    """The reconciliation demo depends on the review queue being non-empty
    (deliberately mangled references) but small."""
    counts = run_query(
        f"SELECT "
        f"SUM(CASE WHEN match_method = 'review' THEN 1 ELSE 0 END) AS review,"
        f"SUM(CASE WHEN credit_debit = 'CRDT' THEN 1 ELSE 0 END) AS credits "
        f"FROM {mart_bank_reconciliation}"
    )
    review = int(counts["review"].iloc[0])
    credits = int(counts["credits"].iloc[0])
    return TestResult(
        name="reconcile__bank__review_queue_small_but_present",
        passed=0 < review <= max(1, int(credits * 0.10)),
        detail=f"review={review} of {credits} credit lines",
    )


def acceptance_test(data_dir: str) -> TestResult:
    """Demo acceptance: mart totals must equal totals computed independently
    (plain pandas) over the generated raw CSV fixtures."""
    invoices_dir = os.path.join(data_dir, raw_mobimed_invoices)
    if not os.path.isdir(invoices_dir):
        return TestResult(
            name="acceptance__mart_matches_independent_totals",
            passed=False,
            detail=f"fixture folder {invoices_dir!r} not found",
        )

    frames = [
        pd.read_csv(os.path.join(invoices_dir, f), sep=";")
        for f in sorted(os.listdir(invoices_dir))
        if f.endswith(".csv")
    ]
    raw = pd.concat(frames, ignore_index=True)
    raw["category"] = (
        raw["Rechnungsnummer"].str[:3]
        .map(category_prefix_rules)
        .fillna(category_default)
    )
    raw["invoice_month"] = pd.to_datetime(
        raw["Datum"], format="%d.%m.%Y"
    ).dt.strftime("%Y-%m")

    expected = (
        raw.groupby(["invoice_month", "category"])
        .agg(
            invoice_count=("Rechnungsnummer", "count"),
            net_vat0=("Umsatz0", "sum"),
            net_vat10=("Umsatz10", "sum"),
            net_vat20=("Umsatz20", "sum"),
            vat10_amount=("MwSt10", "sum"),
            vat20_amount=("MwSt20", "sum"),
            gross_total=("SummeUmsatzinklUSt", "sum"),
        )
        .round(2)
        .reset_index()
    )

    mart = run_query(
        f"SELECT invoice_month, category, invoice_count, net_vat0, "
        f"net_vat10, net_vat20, vat10_amount, vat20_amount, gross_total "
        f"FROM {mart_monthly_table} ORDER BY invoice_month, category"
    )

    if len(mart) != len(expected):
        return TestResult(
            name="acceptance__mart_matches_independent_totals",
            passed=False,
            detail=f"mart has {len(mart)} rows, expected {len(expected)}",
        )

    amount_columns = [
        "net_vat0", "net_vat10", "net_vat20",
        "vat10_amount", "vat20_amount", "gross_total",
    ]
    mismatches = []
    mart_indexed = mart.set_index(["invoice_month", "category"])
    for _, exp_row in expected.iterrows():
        key = (exp_row["invoice_month"], exp_row["category"])
        if key not in mart_indexed.index:
            mismatches.append(f"{key} missing from mart")
            continue
        mart_row = mart_indexed.loc[key]
        if int(mart_row["invoice_count"]) != int(exp_row["invoice_count"]):
            mismatches.append(
                f"{key} invoice_count {int(mart_row['invoice_count'])} "
                f"!= {int(exp_row['invoice_count'])}"
            )
        for col in amount_columns:
            if abs(float(mart_row[col]) - float(exp_row[col])) > SUM_TOLERANCE:
                mismatches.append(
                    f"{key} {col} {float(mart_row[col]):.2f} "
                    f"!= {float(exp_row[col]):.2f}"
                )

    return TestResult(
        name="acceptance__mart_matches_independent_totals",
        passed=not mismatches,
        detail="; ".join(mismatches[:5]),
    )


def run_quality(data_dir: str | None = generated_data_dir) -> bool:
    results = mobimed_tests()
    results.extend(cross_system_tests())
    results.append(review_queue_present())
    if data_dir is not None:
        results.append(acceptance_test(data_dir))

    print("\nData-quality results:")
    width = max(len(r.name) for r in results)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        suffix = f"  ({r.detail})" if r.detail else ""
        print(f"  [{status}] {r.name:<{width}}{suffix}")

    ok = all(r.passed for r in results)
    print(
        f"\n{sum(r.passed for r in results)}/{len(results)} tests passed"
        + ("" if ok else " — FAILING")
    )
    return ok


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run medcenter demo quality tests")
    ap.add_argument("--data-dir", default=generated_data_dir)
    ap.add_argument(
        "--skip-acceptance",
        action="store_true",
        help="Skip the CSV-vs-mart acceptance check (no fixtures needed)",
    )
    return ap.parse_args()


def main() -> None:
    initialize_app_home(change_cwd=True)
    args = parse_args()
    ok = run_quality(data_dir=None if args.skip_acceptance else args.data_dir)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
