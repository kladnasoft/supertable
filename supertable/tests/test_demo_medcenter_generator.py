"""Offline tests for the medcenter demo generator (no backend required).

The generator is a permanent fixture of the medcenter demo: these tests seal
the exact Mobimed column layout, the internal arithmetic consistency
(gross = net buckets + VAT amounts, VAT = 10%/20% of its bucket), the
invoice-number scheme, the category mix, and determinism under a fixed
seed.
"""

import re

import pandas as pd
import pytest

from supertable.demo.medcenter.core import (
    GenerationConfig,
    MedcenterDataGenerator,
    INVOICE_COLUMNS,
    PAYMENT_COLUMNS,
)
from supertable.demo.medcenter.defaults import (
    category_default,
    category_prefix_rules,
    payment_categories,
    raw_invoices_table,
    raw_payments_table,
)

ROUNDING_TOLERANCE = 0.006


@pytest.fixture(scope="module")
def tables():
    config = GenerationConfig(seed=42, month="2026-07")
    return MedcenterDataGenerator(config).run()


@pytest.fixture(scope="module")
def invoices(tables):
    return tables[raw_invoices_table]


@pytest.fixture(scope="module")
def payments(tables):
    return tables[raw_payments_table]


def test_exact_column_layouts(invoices, payments):
    assert list(invoices.columns) == INVOICE_COLUMNS
    assert list(payments.columns) == PAYMENT_COLUMNS


def test_row_counts(invoices, payments):
    assert len(invoices) == 500
    assert len(payments) == 700


def test_invoice_number_scheme_and_uniqueness(invoices):
    pattern = re.compile(r"^[A-Z]{3}2026-\d{5}$")
    assert invoices["Rechnungsnummer"].map(
        lambda n: bool(pattern.match(n))
    ).all()
    assert invoices["Rechnungsnummer"].is_unique


def test_gross_equals_sum_of_parts(invoices):
    parts = (
        invoices["Umsatz0"]
        + invoices["Umsatz10"]
        + invoices["Umsatz20"]
        + invoices["MwSt10"]
        + invoices["MwSt20"]
    )
    assert (
        (invoices["SummeUmsatzinklUSt"] - parts).abs() <= ROUNDING_TOLERANCE
    ).all()


def test_vat_amounts_match_rates(invoices):
    vat10_delta = (invoices["MwSt10"] - 0.10 * invoices["Umsatz10"]).abs()
    vat20_delta = (invoices["MwSt20"] - 0.20 * invoices["Umsatz20"]).abs()
    assert (vat10_delta <= ROUNDING_TOLERANCE).all()
    assert (vat20_delta <= ROUNDING_TOLERANCE).all()


def test_category_mix_roughly_10_10_10_70(invoices):
    categories = (
        invoices["Rechnungsnummer"].str[:3]
        .map(category_prefix_rules)
        .fillna(category_default)
    )
    shares = categories.value_counts(normalize=True)
    for name in category_prefix_rules.values():
        assert 0.05 <= shares[name] <= 0.16, f"{name} share {shares[name]}"
    assert 0.60 <= shares[category_default] <= 0.80


def test_dates_inside_demo_month(invoices, payments):
    invoice_dates = pd.to_datetime(invoices["Datum"], format="%d.%m.%Y")
    assert (invoice_dates.dt.strftime("%Y-%m") == "2026-07").all()
    # Payments may settle a few days after month end, but never before it.
    payment_dates = pd.to_datetime(payments["belegdatum"], format="%d.%m.%Y")
    assert (payment_dates >= pd.Timestamp("2026-07-01")).all()
    assert (payment_dates <= pd.Timestamp("2026-08-15")).all()


def test_payment_fields(payments):
    assert payments["belegnr"].is_unique
    assert payments["Payment_Category"].isin(payment_categories).all()
    beleg = pd.to_datetime(payments["belegdatum"], format="%d.%m.%Y")
    clearing = pd.to_datetime(payments["ClearingDate"], format="%d.%m.%Y")
    assert (clearing >= beleg).all()
    assert (payments["betrag"] > 0).all()


def test_cancelled_invoices_are_not_open(invoices):
    cancelled = invoices[invoices["Stornogrund"] != ""]
    assert len(cancelled) > 0
    assert not cancelled["Invoice_open"].any()


def test_generation_is_deterministic():
    config = GenerationConfig(seed=42, month="2026-07")
    first = MedcenterDataGenerator(config).run()
    second = MedcenterDataGenerator(config).run()
    for name in (raw_invoices_table, raw_payments_table):
        pd.testing.assert_frame_equal(first[name], second[name])


def test_invalid_month_rejected():
    with pytest.raises(ValueError, match="YYYY-MM"):
        MedcenterDataGenerator(GenerationConfig(month="July 2026"))


# ---------------------------------------------------------------------------
# Multi-month (--year) generation
# ---------------------------------------------------------------------------


def _generate_year_months(n_months=3, n_invoices=50, n_payments=70):
    """Mirror generate.generate_months() without touching the filesystem."""
    per_month = []
    for index in range(n_months):
        config = GenerationConfig(
            seed=42 + index,
            month=f"2026-{index + 1:02d}",
            n_invoices=n_invoices,
            n_payments=n_payments,
            invoice_sequence_start=1 + index * n_invoices,
            payment_sequence_start=1 + index * n_payments,
        )
        per_month.append(MedcenterDataGenerator(config).run())
    return per_month


def test_multi_month_numbers_never_collide():
    per_month = _generate_year_months()
    invoices = pd.concat(
        [m[raw_invoices_table] for m in per_month], ignore_index=True
    )
    payments = pd.concat(
        [m[raw_payments_table] for m in per_month], ignore_index=True
    )
    assert invoices["Rechnungsnummer"].is_unique
    assert payments["belegnr"].is_unique


def test_multi_month_sequences_continue():
    per_month = _generate_year_months(n_months=2, n_invoices=50)
    second = per_month[1][raw_invoices_table]
    sequences = second["Rechnungsnummer"].str[-5:].astype(int)
    assert sequences.min() == 51
    assert sequences.max() == 100


def test_multi_month_data_differs_between_months():
    per_month = _generate_year_months(n_months=2)
    first = per_month[0][raw_invoices_table]
    second = per_month[1][raw_invoices_table]
    # Different seeds per month: amounts must not repeat 1:1.
    assert not first["SummeUmsatzinklUSt"].equals(
        second["SummeUmsatzinklUSt"]
    )


# ---------------------------------------------------------------------------
# Cross-system consistency (all eight sources)
# ---------------------------------------------------------------------------

from supertable.demo.medcenter import defaults as d  # noqa: E402
from supertable.demo.medcenter.finance_sources import (  # noqa: E402
    FinanceSourcesGenerator,
)


@pytest.fixture(scope="module")
def all_tables(tables):
    config = GenerationConfig(seed=42, month="2026-07")
    finance = FinanceSourcesGenerator(
        config,
        tables[raw_invoices_table],
        tables[raw_payments_table],
    ).run()
    return {**tables, **finance}


def test_chargebee_totals_and_links(all_tables):
    inv = all_tables[d.raw_chargebee_invoices]
    grouped = inv.groupby("invoice_number").agg(
        total=("total", "first"),
        lines=("line_amount", "sum"),
        tax=("line_tax_amount", "sum"),
    )
    assert (
        (grouped["total"] - (grouped["lines"] + grouped["tax"])).abs() < 0.011
    ).all()
    txn = all_tables[d.raw_chargebee_transactions]
    assert txn["invoice_number"].isin(inv["invoice_number"]).all()
    notes = all_tables[d.raw_chargebee_credit_notes]
    assert notes["reference_invoice_number"].isin(inv["invoice_number"]).all()


def test_stripe_money_flow_ties_out(all_tables):
    charges = all_tables[d.raw_stripe_charges]
    balance = all_tables[d.raw_stripe_balance_transactions]
    payouts = all_tables[d.raw_stripe_payouts]
    cb_invoices = all_tables[d.raw_chargebee_invoices]

    chargebee_charges = charges[charges["source_class"] == "chargebee"]
    assert chargebee_charges["source_invoice_number"].isin(
        cb_invoices["invoice_number"]
    ).all()
    assert (
        (balance["net"] - (balance["amount"] - balance["fee"])).abs() < 0.011
    ).all()
    net_by_payout = balance.groupby("payout_id")["net"].sum().round(2)
    merged = payouts.set_index("payout_id").join(net_by_payout.rename("nets"))
    assert ((merged["amount"] - merged["nets"]).abs() < 0.02).all()


def test_bank_references_resolve_and_review_queue_exists(all_tables):
    bank = all_tables[d.raw_erste_camt053]
    known = set(
        all_tables[d.raw_chargebee_invoices]["invoice_number"]
    ) | set(all_tables[d.raw_zoho_invoices]["invoice_number"])
    refs = (
        bank["remittance_structured"]
        .str.extract(r"((?:MR|FK)\d{4}-\d{5})")[0]
        .dropna()
    )
    assert len(refs) > 0
    assert refs.isin(known).all()

    credits = bank[bank["credit_debit"] == "CRDT"]
    review = credits[
        ~credits["remittance_structured"].str.contains("MR|FK", na=False)
        & ~credits["counterparty_name"].str.startswith(("HOBEX", "STRIPE"))
    ]
    assert 0 < len(review) <= len(credits) * 0.10


def test_hobex_settlements_tie_to_mobimed_and_bank(all_tables):
    settlements = all_tables[d.raw_hobex_settlements]
    payments = all_tables[raw_payments_table]
    bank = all_tables[d.raw_erste_camt053]

    hobex = payments[payments["Payment_Category"] == "Hobex"].copy()
    hobex["day"] = (
        pd.to_datetime(hobex["ClearingDate"], format="%d.%m.%Y")
        .dt.date.astype(str)
    )
    by_day = hobex.groupby("day")["betrag"].sum().round(2)
    merged = settlements.set_index("settlement_date").join(
        by_day.rename("mobimed")
    )
    assert ((merged["gross_amount"] - merged["mobimed"]).abs() < 0.011).all()
    assert (
        (settlements["net_amount"]
         - (settlements["gross_amount"] - settlements["fee_amount"])).abs()
        < 0.011
    ).all()
    hobex_bank = bank[bank["counterparty_name"].str.startswith("HOBEX")]
    assert len(hobex_bank) == len(settlements)


def test_bmd_journal_documents_resolve(all_tables):
    journal = all_tables[d.raw_bmd_journal]
    known = (
        set(all_tables[d.raw_chargebee_invoices]["invoice_number"])
        | set(all_tables[d.raw_zoho_invoices]["invoice_number"])
        | set(all_tables[raw_invoices_table]["Rechnungsnummer"])
    )
    ar = journal[journal["booking_symbol"] == "AR"]
    assert len(ar) > 0
    assert ar["document_number"].isin(known).all()
    assert journal.groupby(["company_id", "fiscal_year"])["sequence_no"] \
        .apply(lambda s: s.is_unique).all()
    assert set(journal["company_id"].unique()) == {1, 2}


def test_every_invoice_row_links_to_its_bill(all_tables):
    for table in (d.raw_chargebee_invoices, d.raw_stripe_invoices,
                  d.raw_zoho_invoices):
        urls = all_tables[table]["pdf_url"]
        assert urls.str.startswith(d.bill_url_base).all()
    assert all_tables[d.raw_domonda_invoices]["document_url"] \
        .str.startswith(d.bill_url_base).all()


def test_single_month_equals_year_slice(tmp_path):
    """--month must produce byte-identical files to that month's slice of
    --year, so the two modes never collide on document numbers."""
    from supertable.demo.medcenter.generate import generate_months

    single_dir = tmp_path / "single"
    year_dir = tmp_path / "year"
    generate_months(["2026-07"], output_dir=str(single_dir),
                    n_invoices=40, n_payments=60)
    generate_months([f"2026-{m:02d}" for m in range(6, 9)],
                    output_dir=str(year_dir), n_invoices=40, n_payments=60)

    for table_dir in sorted(single_dir.iterdir()):
        if not table_dir.is_dir():
            continue
        for file in sorted(table_dir.iterdir()):
            counterpart = year_dir / table_dir.name / file.name
            assert counterpart.exists(), f"missing {counterpart}"
            assert file.read_bytes() == counterpart.read_bytes(), (
                f"{file.name} differs between single-month and year modes"
            )
