"""Generators for the seven non-Mobimed finance sources of the medcenter demo.

Everything is synthesized from specification and — crucially — cross-linked
the way the real systems are, so reconciliation views have something real
to match:

  - Chargebee membership invoices (``MR…``) settle either through Stripe
    (transaction → charge → balance transaction → weekly payout → bank
    credit) or by direct bank transfer (bank credit whose structured
    remittance carries the ``MR…`` number).
  - Stripe additionally carries the weight-program invoices
    (``MEDWEIGHT-…``) through the same account, split by ``source_class``.
  - Zoho B2B invoices (``FK…``) are paid by bank transfer; ~5% of the bank
    references are deliberately mangled so the reconciliation review queue
    is never empty.
  - Hobex settles the Mobimed card-terminal payments daily
    (net = gross − fee) and the net lands on the bank statement.
  - Domonda supplier invoices reappear as bank debits; the BMD journal
    books one AR row per outgoing invoice (document number = the source
    invoice number) and one ER row per supplier invoice.

Every invoice row carries a ``pdf_url`` pointing at the demo bill host, so
a workbench (e.g. Lighthouse) can link from any figure to "the actual
bill". Deterministic under the configured seed.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Dict

import numpy as np
import pandas as pd

from supertable.demo.medcenter import defaults as d
from supertable.demo.medcenter.core import (
    DOCTORS,
    FIRST_NAMES,
    GenerationConfig,
    LAST_NAMES,
)

CHARGEBEE_PLANS = [
    # (plan_id, plan_name, monthly_price, period_months, weight)
    ("plan_basic", "Basic Membership", 29.90, 1, 0.35),
    ("plan_plus", "Plus Membership", 49.90, 1, 0.30),
    ("plan_family", "Family Membership", 79.90, 1, 0.15),
    ("plan_annual_basic", "Basic Membership (Annual)", 299.00, 12, 0.12),
    ("plan_annual_plus", "Plus Membership (Annual)", 499.00, 12, 0.08),
]

WEIGHT_PLANS = [
    ("Weight Program 3 Months", 189.00, 3, 0.45),
    ("Weight Program 6 Months", 349.00, 6, 0.25),
    ("Weight Coaching Month", 89.00, 1, 0.30),
]

ZOHO_SERVICES = [
    # (name, lo, hi, tax_percent)
    ("Arbeitsmedizinische Betreuung", 850.0, 2500.0, 0),
    ("Corporate Membership Paket", 1200.0, 3600.0, 20),
    ("Vorsorgeuntersuchung Paket", 600.0, 1800.0, 0),
    ("Impfaktion Firmenstandort", 400.0, 1500.0, 20),
]

ZOHO_COMPANIES = [
    # (company_name, entity)
    ("Alpenbau Holding GmbH", 1), ("Donau Logistik AG", 1),
    ("Wiener Softwarehaus GmbH", 1), ("Grünfeld Handels GmbH", 1),
    ("Stadtwerke Service GmbH", 1), ("Panorama Hotels AG", 1),
    ("Auringer Transporte GmbH", 1), ("Klimatech Anlagen GmbH", 1),
    ("Labordiagnostik Partner GmbH", 2), ("BioSample Analytics GmbH", 2),
    ("MedResearch Institut GmbH", 2), ("PharmaTrial Services GmbH", 2),
]

SUPPLIERS = [
    ("MedSupply Austria GmbH", "7600"), ("Labortechnik Wien GmbH", "7600"),
    ("CleanService Facility GmbH", "7200"), ("CloudHost IT GmbH", "7790"),
    ("Praxisbedarf24 GmbH", "7600"), ("Energie Wien AG", "7300"),
    ("Immobilien Verwaltung GmbH", "7400"), ("Versicherung Austria AG", "7700"),
]

MISC_BANK_DEBITS = [
    ("Miete Ordination Zentrum", 8400.00),
    ("Gehälter Sammelüberweisung", 42500.00),
    ("Sozialversicherung", 15800.00),
    ("Finanzamt UVA", 9200.00),
]


def _iban_for_entity(entity_id: int) -> str:
    return d.bank_accounts[d.legal_entities[entity_id]]


class FinanceSourcesGenerator:
    """Generates all non-Mobimed source tables for one month."""

    def __init__(
        self,
        config: GenerationConfig,
        mobimed_invoices: pd.DataFrame,
        mobimed_payments: pd.DataFrame,
    ):
        self.config = config
        self.rng = np.random.default_rng(config.seed + 7919)
        self.mobimed_invoices = mobimed_invoices
        self.mobimed_payments = mobimed_payments

        year_str, month_str = config.month.split("-")
        self.year = int(year_str)
        self.month = int(month_str)
        self.days_in_month = calendar.monthrange(self.year, self.month)[1]
        self.month_start = date(self.year, self.month, 1)
        self.month_end = date(self.year, self.month, self.days_in_month)
        self.tag = f"{self.year}{self.month:02d}"

        # Year-scoped sequence offsets per source, continued across months.
        idx = config.month_index
        self.cb_seq = 1 + idx * d.n_chargebee_invoices
        self.cb_cn_seq = 1 + idx * d.n_chargebee_credit_notes
        self.weight_seq = 1 + idx * d.n_stripe_weight_invoices
        self.zoho_seq = 1 + idx * d.n_zoho_invoices
        self.zoho_cn_seq = 1 + idx * d.n_zoho_credit_notes
        self.domonda_seq = 1 + idx * d.n_domonda_invoices
        self.bmd_seq = 1 + idx * 100000  # ample stride per month
        self.bank_seq = 1

        # Bank entries accumulate while the sources generate.
        self._bank_rows: list[dict] = []

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    def _day(self, lo: int = 1, hi: int | None = None) -> date:
        hi = hi or self.days_in_month
        return date(self.year, self.month, int(self.rng.integers(lo, hi + 1)))

    def _cap(self, day: date) -> date:
        return min(day, self.month_end)

    def _person(self) -> str:
        return f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}"

    def _email(self, name: str, i: int) -> str:
        slug = name.lower().replace(" ", ".")
        return f"{slug}{i}@example.at"

    def _pdf(self, number: str) -> str:
        return f"{d.bill_url_base}/{number}.pdf"

    def _bank_entry(
        self,
        booking_date: date,
        amount: float,
        credit_debit: str,
        counterparty_name: str,
        remittance_structured: str = "",
        remittance_unstructured: str = "",
        entity_id: int = 1,
        end_to_end_ref: str = "NOTPROVIDED",
    ) -> None:
        iban = _iban_for_entity(entity_id)
        entry_id = f"EN-{self.tag}-{self.bank_seq:05d}"
        self.bank_seq += 1
        self._bank_rows.append(
            {
                "statement_id": f"ST-{iban[-4:]}-{self.tag}",
                "entry_id": entry_id,
                "account_iban": iban,
                "booking_date": booking_date.isoformat(),
                "value_date": booking_date.isoformat(),
                "amount": round(float(amount), 2),
                "credit_debit": credit_debit,
                "end_to_end_ref": end_to_end_ref,
                "remittance_structured": remittance_structured,
                "remittance_unstructured": remittance_unstructured,
                "counterparty_name": counterparty_name,
                "counterparty_iban": f"AT{self.rng.integers(10, 99)}"
                + "".join(str(self.rng.integers(0, 10)) for _ in range(16)),
                "bank_txn_code": (
                    "PMNT-RCDT-ESCT" if credit_debit == "CRDT"
                    else "PMNT-ICDT-ESCT"
                ),
            }
        )

    # ------------------------------------------------------------------
    # Chargebee (+ the Stripe charges its transactions become)
    # ------------------------------------------------------------------
    def generate_chargebee(self) -> Dict[str, pd.DataFrame]:
        plans = CHARGEBEE_PLANS
        plan_weights = [p[4] for p in plans]

        subscriptions, invoice_lines, transactions = [], [], []
        self._cb_charges: list[dict] = []  # consumed by generate_stripe
        paid_invoices: list[dict] = []

        for i in range(d.n_chargebee_invoices):
            seq = self.cb_seq + i
            invoice_number = f"MR{self.year}-{seq:05d}"
            plan_id, plan_name, price, period_months, _ = plans[
                int(self.rng.choice(len(plans), p=plan_weights))
            ]
            customer_name = self._person()
            customer_id = f"CBC-{self.year}-{seq:05d}"
            issued = self._day()
            period_start = issued
            period_end = issued + timedelta(days=period_months * 30 - 1)

            status = str(self.rng.choice(
                ["paid", "payment_due", "void"], p=[0.88, 0.10, 0.02]
            ))
            paid_at = (
                self._cap(issued + timedelta(int(self.rng.integers(0, 3))))
                if status == "paid" else None
            )

            lines = [(f"{plan_name}", 1, price, 0, 0.0)]
            if self.rng.random() < 0.25:
                lines.append(("Signup & Onboarding Fee", 1, 15.00, 20, 3.00))
            total = round(sum(a + t for _, _, a, _, t in lines), 2)

            subscriptions.append(
                {
                    "subscription_id": f"SUB-{self.year}-{seq:05d}",
                    "customer_id": customer_id,
                    "customer_email": self._email(customer_name, seq),
                    "customer_name": customer_name,
                    "plan_id": plan_id,
                    "plan_name": plan_name,
                    "status": "active" if status != "void" else "cancelled",
                    "billing_period_start": period_start.isoformat(),
                    "billing_period_end": period_end.isoformat(),
                    "next_billing_at": (
                        period_end + timedelta(days=1)
                    ).isoformat(),
                    "mrr": round(price / period_months, 2),
                }
            )
            for line_no, (desc, qty, amount, tax_rate, tax) in enumerate(
                lines, start=1
            ):
                invoice_lines.append(
                    {
                        "invoice_number": invoice_number,
                        "line_no": line_no,
                        "subscription_id": f"SUB-{self.year}-{seq:05d}",
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "date_issued": issued.isoformat(),
                        "date_due": (issued + timedelta(days=14)).isoformat(),
                        "date_paid": paid_at.isoformat() if paid_at else "",
                        "status": status,
                        "currency": "EUR",
                        "line_description": desc,
                        "line_quantity": qty,
                        "line_amount": amount,
                        "line_tax_rate": tax_rate,
                        "line_tax_amount": tax,
                        "service_period_start": period_start.isoformat(),
                        "service_period_end": period_end.isoformat(),
                        "total": total,
                        "pdf_url": self._pdf(invoice_number),
                    }
                )

            if status == "paid":
                paid_invoices.append(
                    {"invoice_number": invoice_number, "total": total,
                     "date": paid_at, "customer_id": customer_id,
                     "customer_name": customer_name, "seq": seq}
                )
                if self.rng.random() < 0.15:
                    # Direct SEPA transfer — MR reference on the bank line.
                    self._bank_entry(
                        booking_date=paid_at,
                        amount=total,
                        credit_debit="CRDT",
                        counterparty_name=customer_name,
                        remittance_structured=invoice_number,
                        remittance_unstructured=(
                            f"Mitgliedschaft {invoice_number}"
                        ),
                    )
                else:
                    charge_id = f"ch_cb{self.tag}{seq:05d}"
                    transactions.append(
                        {
                            "transaction_id": f"TXN-{self.year}-{seq:05d}",
                            "invoice_number": invoice_number,
                            "date": paid_at.isoformat(),
                            "amount": total,
                            "status": "success",
                            "payment_method": "card",
                            "gateway": "stripe",
                            "gateway_txn_id": charge_id,
                        }
                    )
                    self._cb_charges.append(
                        {
                            "charge_id": charge_id,
                            "created": paid_at,
                            "amount": total,
                            "customer_id": customer_id,
                            "customer_name": customer_name,
                            "description": f"Chargebee invoice {invoice_number}",
                            "source_class": "chargebee",
                            "source_invoice_number": invoice_number,
                        }
                    )

        credit_notes = []
        for j in range(min(d.n_chargebee_credit_notes, len(paid_invoices))):
            ref = paid_invoices[
                int(self.rng.integers(0, len(paid_invoices)))
            ]
            cn_seq = self.cb_cn_seq + j
            full_refund = bool(self.rng.random() < 0.4)
            credit_notes.append(
                {
                    "credit_note_number": f"MRGS{self.year}-{cn_seq:05d}",
                    "reference_invoice_number": ref["invoice_number"],
                    "customer_id": ref["customer_id"],
                    "date": self._cap(
                        ref["date"] + timedelta(int(self.rng.integers(1, 10)))
                    ).isoformat(),
                    "amount": (
                        ref["total"] if full_refund
                        else round(ref["total"] / 2, 2)
                    ),
                    "reason": str(self.rng.choice(
                        ["Kündigung", "Doppelbuchung", "Kulanz"]
                    )),
                    "status": "refunded",
                }
            )

        return {
            d.raw_chargebee_subscriptions: pd.DataFrame(subscriptions),
            d.raw_chargebee_invoices: pd.DataFrame(invoice_lines),
            d.raw_chargebee_credit_notes: pd.DataFrame(credit_notes),
            d.raw_chargebee_transactions: pd.DataFrame(transactions),
        }

    # ------------------------------------------------------------------
    # Stripe (Chargebee traffic + weight program, blended in one account)
    # ------------------------------------------------------------------
    def generate_stripe(self) -> Dict[str, pd.DataFrame]:
        weight_invoices, charges = [], []

        for i in range(d.n_stripe_weight_invoices):
            seq = self.weight_seq + i
            invoice_number = f"MEDWEIGHT-{self.year}-{seq:04d}"
            name, price, period_months, _ = WEIGHT_PLANS[
                int(self.rng.choice(
                    len(WEIGHT_PLANS), p=[p[3] for p in WEIGHT_PLANS]
                ))
            ]
            customer_name = self._person()
            customer_id = f"cus_w{self.tag}{seq:04d}"
            created = self._day()
            status = str(self.rng.choice(["paid", "void"], p=[0.92, 0.08]))
            charge_id = f"ch_wt{self.tag}{seq:04d}" if status == "paid" else ""

            weight_invoices.append(
                {
                    "invoice_number": invoice_number,
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "date": created.isoformat(),
                    "status": status,
                    "amount": price,
                    "service_period_start": created.isoformat(),
                    "service_period_end": (
                        created + timedelta(days=period_months * 30 - 1)
                    ).isoformat(),
                    "charge_id": charge_id,
                    "pdf_url": self._pdf(invoice_number),
                }
            )
            if status == "paid":
                charges.append(
                    {
                        "charge_id": charge_id,
                        "created": created,
                        "amount": price,
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "description": name,
                        "source_class": "medcenter_weight",
                        "source_invoice_number": invoice_number,
                    }
                )

        charges.extend(self._cb_charges)

        charge_rows, balance_rows = [], []
        payout_buckets: dict[date, list[dict]] = {}
        for c in charges:
            fee = round(c["amount"] * 0.019 + 0.25, 2)
            net = round(c["amount"] - fee, 2)
            available_on = c["created"] + timedelta(days=2)
            # Weekly payout bucket: the Monday after funds become available.
            bucket = available_on + timedelta(
                days=(7 - available_on.weekday()) % 7
            )
            payout_id = f"po_{self.tag}_{bucket.day:02d}"
            payout_buckets.setdefault(bucket, []).append(
                {"net": net, "available_on": available_on}
            )
            charge_rows.append(
                {
                    "charge_id": c["charge_id"],
                    "created": c["created"].isoformat(),
                    "amount": c["amount"],
                    "currency": "EUR",
                    "customer_id": c["customer_id"],
                    "customer_name": c["customer_name"],
                    "description": c["description"],
                    "source_class": c["source_class"],
                    "source_invoice_number": c["source_invoice_number"],
                    "status": "succeeded",
                }
            )
            balance_rows.append(
                {
                    "balance_txn_id": f"txn_{c['charge_id']}",
                    "charge_id": c["charge_id"],
                    "type": "charge",
                    "amount": c["amount"],
                    "fee": fee,
                    "net": net,
                    "available_on": available_on.isoformat(),
                    "payout_id": payout_id,
                }
            )

        payout_rows = []
        for bucket, entries in sorted(payout_buckets.items()):
            payout_id = f"po_{self.tag}_{bucket.day:02d}"
            amount = round(sum(e["net"] for e in entries), 2)
            arrival = bucket + timedelta(days=1)
            payout_rows.append(
                {
                    "payout_id": payout_id,
                    "amount": amount,
                    "arrival_date": arrival.isoformat(),
                    "status": "paid",
                }
            )
            self._bank_entry(
                booking_date=arrival,
                amount=amount,
                credit_debit="CRDT",
                counterparty_name="STRIPE PAYMENTS EUROPE LTD",
                remittance_unstructured=f"STRIPE PAYOUT {payout_id}",
                end_to_end_ref=payout_id,
            )

        return {
            d.raw_stripe_invoices: pd.DataFrame(weight_invoices),
            d.raw_stripe_charges: pd.DataFrame(charge_rows),
            d.raw_stripe_balance_transactions: pd.DataFrame(balance_rows),
            d.raw_stripe_payouts: pd.DataFrame(payout_rows),
        }

    # ------------------------------------------------------------------
    # Zoho Books (B2B / insurance invoicing)
    # ------------------------------------------------------------------
    def generate_zoho(self) -> Dict[str, pd.DataFrame]:
        contacts = [
            {
                "customer_id": f"ZC-{i + 1:04d}",
                "customer_name": company,
                "company_name": company,
                "email": f"buchhaltung@{company.split()[0].lower()}.at",
                "legal_entity": d.legal_entities[entity_id],
            }
            for i, (company, entity_id) in enumerate(ZOHO_COMPANIES)
        ]

        invoice_lines, payments, credit_notes = [], [], []
        paid_pool = []
        payment_counter = 0
        for i in range(d.n_zoho_invoices):
            seq = self.zoho_seq + i
            invoice_number = f"FK{self.year}-{seq:05d}"
            contact_idx = int(self.rng.integers(0, len(ZOHO_COMPANIES)))
            company, entity_id = ZOHO_COMPANIES[contact_idx]
            invoice_date = self._day(1, 25)
            status = str(self.rng.choice(
                ["draft", "sent", "paid", "overdue", "partially_paid", "void"],
                p=[0.05, 0.20, 0.55, 0.10, 0.07, 0.03],
            ))
            period_months = int(self.rng.choice([1, 3, 6, 12],
                                                p=[0.4, 0.25, 0.2, 0.15]))
            service_start = invoice_date.replace(day=1)
            service_end = (
                service_start + timedelta(days=period_months * 30 - 1)
            )

            n_lines = int(self.rng.integers(1, 4))
            picks = self.rng.choice(
                len(ZOHO_SERVICES), size=n_lines, replace=False
            )
            total = 0.0
            line_records = []
            for line_no, pick in enumerate(picks, start=1):
                name, lo, hi, tax_pct = ZOHO_SERVICES[pick]
                rate = round(float(self.rng.uniform(lo, hi)), 2)
                line_total = round(rate * (1 + tax_pct / 100), 2)
                total = round(total + line_total, 2)
                line_records.append(
                    (line_no, name, 1, rate, tax_pct, line_total)
                )

            if status == "paid":
                balance = 0.0
            elif status == "partially_paid":
                balance = round(total / 2, 2)
            else:
                balance = total

            for line_no, name, qty, rate, tax_pct, line_total in line_records:
                invoice_lines.append(
                    {
                        "invoice_number": invoice_number,
                        "line_no": line_no,
                        "customer_id": f"ZC-{contact_idx + 1:04d}",
                        "customer_name": company,
                        "invoice_date": invoice_date.isoformat(),
                        "due_date": (
                            invoice_date + timedelta(days=30)
                        ).isoformat(),
                        "status": status,
                        "category": str(self.rng.choice(
                            ["Arbeitsmedizin", "Firmenmitgliedschaft",
                             "Versicherung"]
                        )),
                        "line_item_name": name,
                        "line_quantity": qty,
                        "line_rate": rate,
                        "line_tax_percent": tax_pct,
                        "line_total": line_total,
                        "total": total,
                        "balance": balance,
                        "cf_service_start": service_start.isoformat(),
                        "cf_service_end": service_end.isoformat(),
                        "legal_entity": d.legal_entities[entity_id],
                        "pdf_url": self._pdf(invoice_number),
                    }
                )

            paid_amount = (
                total if status == "paid"
                else round(total / 2, 2) if status == "partially_paid"
                else 0.0
            )
            if paid_amount > 0:
                pay_date = self._cap(
                    invoice_date + timedelta(int(self.rng.integers(3, 25)))
                )
                payments.append(
                    {
                        "payment_id": f"ZP-{self.year}-{seq:05d}",
                        "payment_date": pay_date.isoformat(),
                        "amount": paid_amount,
                        "customer_id": f"ZC-{contact_idx + 1:04d}",
                        "customer_name": company,
                        "invoice_numbers_applied": invoice_number,
                        "mode": "banktransfer",
                    }
                )
                payment_counter += 1
                if payment_counter % 8 == 3:
                    # Deliberately mangled reference (deterministic, so the
                    # reconciliation review queue is never empty) →
                    # match_method 'review'.
                    self._bank_entry(
                        booking_date=pay_date,
                        amount=paid_amount,
                        credit_debit="CRDT",
                        counterparty_name=company,
                        remittance_structured="",
                        remittance_unstructured=(
                            f"Zahlung Re {invoice_number.replace('-', '/')}"
                        ),
                        entity_id=entity_id,
                    )
                else:
                    self._bank_entry(
                        booking_date=pay_date,
                        amount=paid_amount,
                        credit_debit="CRDT",
                        counterparty_name=company,
                        remittance_structured=invoice_number,
                        remittance_unstructured=f"Rechnung {invoice_number}",
                        entity_id=entity_id,
                    )
                if status == "paid":
                    paid_pool.append(
                        {"invoice_number": invoice_number, "total": total,
                         "date": pay_date}
                    )

        for j in range(min(d.n_zoho_credit_notes, len(paid_pool))):
            ref = paid_pool[int(self.rng.integers(0, len(paid_pool)))]
            cn_seq = self.zoho_cn_seq + j
            credit_notes.append(
                {
                    "credit_note_number": f"CN{self.year}-{cn_seq:04d}",
                    "reference_invoice_number": ref["invoice_number"],
                    "date": self._cap(
                        ref["date"] + timedelta(int(self.rng.integers(1, 6)))
                    ).isoformat(),
                    "amount": round(
                        ref["total"] * float(self.rng.choice([0.25, 0.5])), 2
                    ),
                    "status": "open",
                }
            )

        return {
            d.raw_zoho_contacts: pd.DataFrame(contacts),
            d.raw_zoho_invoices: pd.DataFrame(invoice_lines),
            d.raw_zoho_payments: pd.DataFrame(payments),
            d.raw_zoho_credit_notes: pd.DataFrame(credit_notes),
        }

    # ------------------------------------------------------------------
    # Hobex card settlements (derived from Mobimed terminal payments)
    # ------------------------------------------------------------------
    def generate_hobex(self) -> Dict[str, pd.DataFrame]:
        hobex_payments = self.mobimed_payments[
            self.mobimed_payments["Payment_Category"] == "Hobex"
        ].copy()
        hobex_payments["clearing"] = pd.to_datetime(
            hobex_payments["ClearingDate"], format="%d.%m.%Y"
        ).dt.date

        rows = []
        for clearing_date, group in sorted(
            hobex_payments.groupby("clearing"), key=lambda kv: kv[0]
        ):
            gross = round(float(group["betrag"].sum()), 2)
            fee = round(gross * d.hobex_fee_rate, 2)
            net = round(gross - fee, 2)
            rows.append(
                {
                    "merchant_id": "M-482771",
                    "terminal_id": "T001",
                    "settlement_date": clearing_date.isoformat(),
                    "transaction_count": int(len(group)),
                    "gross_amount": gross,
                    "fee_amount": fee,
                    "net_amount": net,
                }
            )
            self._bank_entry(
                booking_date=clearing_date + timedelta(days=1),
                amount=net,
                credit_debit="CRDT",
                counterparty_name="HOBEX AG",
                remittance_unstructured=(
                    f"HOBEX ABRECHNUNG {clearing_date.isoformat()} "
                    f"TERMINAL T001"
                ),
            )

        return {d.raw_hobex_settlements: pd.DataFrame(rows)}

    # ------------------------------------------------------------------
    # Domonda (supplier invoices) + misc bank debits
    # ------------------------------------------------------------------
    def generate_domonda(self) -> Dict[str, pd.DataFrame]:
        rows = []
        self._domonda_docs: list[dict] = []
        for i in range(d.n_domonda_invoices):
            seq = self.domonda_seq + i
            supplier, account = SUPPLIERS[
                int(self.rng.integers(0, len(SUPPLIERS)))
            ]
            document_id = f"DOM-{self.year}-{seq:04d}"
            invoice_date = self._day(1, 26)
            tax_pct = int(self.rng.choice([20, 0], p=[0.8, 0.2]))
            net = round(float(self.rng.uniform(120.0, 4800.0)), 2)
            tax = round(net * tax_pct / 100, 2)
            gross = round(net + tax, 2)
            supplier_invoice_no = (
                f"{supplier.split()[0].upper()[:4]}-{self.tag}-{seq:04d}"
            )
            rows.append(
                {
                    "document_id": document_id,
                    "supplier_name": supplier,
                    "invoice_number": supplier_invoice_no,
                    "invoice_date": invoice_date.isoformat(),
                    "net_amount": net,
                    "tax_amount": tax,
                    "gross_amount": gross,
                    "account_assignment": account,
                    "status": "booked",
                    "document_url": self._pdf(document_id),
                }
            )
            self._domonda_docs.append(
                {"document_id": document_id, "gross": gross,
                 "date": invoice_date, "tax": tax, "tax_pct": tax_pct,
                 "account": account, "supplier": supplier,
                 "supplier_invoice_no": supplier_invoice_no}
            )
            if self.rng.random() < 0.8:
                self._bank_entry(
                    booking_date=self._cap(
                        invoice_date + timedelta(int(self.rng.integers(5, 20)))
                    ),
                    amount=gross,
                    credit_debit="DBIT",
                    counterparty_name=supplier,
                    remittance_unstructured=(
                        f"RG {supplier_invoice_no}"
                    ),
                )

        for label, amount in MISC_BANK_DEBITS:
            self._bank_entry(
                booking_date=self._day(25, self.days_in_month),
                amount=amount,
                credit_debit="DBIT",
                counterparty_name=label.split()[0].upper(),
                remittance_unstructured=label,
            )

        return {d.raw_domonda_invoices: pd.DataFrame(rows)}

    # ------------------------------------------------------------------
    # BMD booking journal (the official books, one row per document)
    # ------------------------------------------------------------------
    def generate_bmd(
        self, chargebee: Dict[str, pd.DataFrame],
        zoho: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        rows = []
        seq = self.bmd_seq

        def add(company_id, symbol, account, contra, doc_no, doc_date, text,
                debit, credit, tax_pct, tax_amount):
            nonlocal seq
            rows.append(
                {
                    "company_id": company_id,
                    "fiscal_year": self.year,
                    "sequence_no": seq,
                    "account": account,
                    "contra_account": contra,
                    "booking_symbol": symbol,
                    "document_number": doc_no,
                    "document_date": doc_date,
                    "text": text,
                    "debit": debit,
                    "credit": credit,
                    "tax_code": 2 if tax_pct == 20 else 1 if tax_pct == 10
                    else 0,
                    "tax_percent": tax_pct,
                    "tax_amount": tax_amount,
                }
            )
            seq += 1

        cb = chargebee[d.raw_chargebee_invoices]
        for _, inv in cb.drop_duplicates("invoice_number").iterrows():
            if inv["status"] == "void":
                continue
            add(1, "AR", "20001", "4100", inv["invoice_number"],
                inv["date_issued"], f"Mitglied {inv['customer_name']}",
                inv["total"], 0.0, 0, 0.0)

        zh = zoho[d.raw_zoho_invoices]
        for _, inv in zh.drop_duplicates("invoice_number").iterrows():
            if inv["status"] in ("draft", "void"):
                continue
            company_id = (
                1 if inv["legal_entity"] == d.legal_entities[1] else 2
            )
            add(company_id, "AR", "20002", "4200", inv["invoice_number"],
                inv["invoice_date"], f"Firmenkunde {inv['customer_name']}",
                inv["total"], 0.0, 0, 0.0)

        for _, inv in self.mobimed_invoices.iterrows():
            if inv["Stornogrund"] != "":
                continue
            tax_amount = round(inv["MwSt10"] + inv["MwSt20"], 2)
            doc_date = pd.to_datetime(
                inv["Datum"], format="%d.%m.%Y"
            ).date().isoformat()
            add(1, "AR", "20003", "4300", inv["Rechnungsnummer"],
                doc_date, f"Patient {inv['Patient']}",
                inv["SummeUmsatzinklUSt"], 0.0,
                20 if inv["MwSt20"] > 0 else 10 if inv["MwSt10"] > 0 else 0,
                tax_amount)

        for doc in self._domonda_docs:
            add(1, "ER", doc["account"], "33001",
                doc["supplier_invoice_no"], doc["date"].isoformat(),
                f"Lieferant {doc['supplier']}", 0.0, doc["gross"],
                doc["tax_pct"], doc["tax"])

        return {d.raw_bmd_journal: pd.DataFrame(rows)}

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, pd.DataFrame]:
        tables: Dict[str, pd.DataFrame] = {}
        chargebee = self.generate_chargebee()
        tables.update(chargebee)
        tables.update(self.generate_stripe())
        zoho = self.generate_zoho()
        tables.update(zoho)
        tables.update(self.generate_hobex())
        tables.update(self.generate_domonda())
        tables.update(self.generate_bmd(chargebee, zoho))
        tables[d.raw_erste_camt053] = pd.DataFrame(self._bank_rows)
        return tables
