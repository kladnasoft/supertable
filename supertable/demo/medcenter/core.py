"""Generator engine for the medcenter demo.

Synthesizes one month of Mobimed-style exports from specification only —
no real data is involved anywhere:

  - an invoice CSV (~500 rows) with the exact Mobimed column layout:
    ``Datum;Rechnungsnummer;Patient;Patientenkategorie;doctor_name;Positionen;
    SummeUmsatzinklUSt;Umsatz0;Umsatz10;Umsatz20;MwSt10;MwSt20;Stornogrund;
    Invoice_open``
  - a payment CSV (~700 rows):
    ``belegnr;belegdatum;Payment_Category;betrag;ClearingDate;doctor_name``

Invoice numbers follow ``<3-letter prefix><year>-<5 digits>`` with a
roughly 10/10/10/70 mix of UNI/AME/PRO/other prefixes so the category
rule has something to bite on. The arithmetic is internally consistent:
gross = the three net buckets plus the two VAT amounts, and each VAT
amount is 10% / 20% of its net bucket. Generation is deterministic under
a fixed seed, so re-generation reproduces the identical fixtures.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict

import numpy as np
import pandas as pd

from supertable.demo.medcenter.defaults import (
    category_prefix_rules,
    payment_categories,
    raw_invoices_table,
    raw_payments_table,
)

DATE_FORMAT = "%d.%m.%Y"

INVOICE_COLUMNS = [
    "Datum",
    "Rechnungsnummer",
    "Patient",
    "Patientenkategorie",
    "doctor_name",
    "Positionen",
    "SummeUmsatzinklUSt",
    "Umsatz0",
    "Umsatz10",
    "Umsatz20",
    "MwSt10",
    "MwSt20",
    "Stornogrund",
    "Invoice_open",
]

PAYMENT_COLUMNS = [
    "belegnr",
    "belegdatum",
    "Payment_Category",
    "betrag",
    "ClearingDate",
    "doctor_name",
]


@dataclass
class GenerationConfig:
    output_dir: str = "medcenter_demo_data"
    seed: int = 42
    month: str = "2026-07"  # YYYY-MM
    n_invoices: int = 500
    n_payments: int = 700
    # Invoice / receipt numbering is year-scoped (XXX2026-00001), so a
    # multi-month run must continue the sequences across months instead of
    # restarting at 1 — otherwise months would collide on the upsert keys.
    invoice_sequence_start: int = 1
    payment_sequence_start: int = 1
    # 0-based position of this month in a multi-month run; the finance-source
    # generators derive their own per-source sequence offsets from it.
    month_index: int = 0
    storno_rate: float = 0.03
    open_rate: float = 0.15
    # UNI / AME / PRO / other prefix mix.
    category_mix: tuple[float, float, float, float] = (0.10, 0.10, 0.10, 0.70)
    payment_category_weights: tuple[float, float, float, float] = field(
        default=(0.30, 0.30, 0.20, 0.20)
    )


FIRST_NAMES = [
    "Anna", "Lukas", "Sophie", "Maximilian", "Lena", "Felix", "Marie",
    "Tobias", "Laura", "David", "Julia", "Sebastian", "Katharina", "Paul",
    "Theresa", "Jakob", "Eva", "Florian", "Magdalena", "Simon", "Nora",
    "Matthias", "Clara", "Georg", "Helene", "Stefan", "Ida", "Andreas",
]

LAST_NAMES = [
    "Gruber", "Huber", "Bauer", "Wagner", "Steiner", "Moser", "Mayer",
    "Hofer", "Leitner", "Berger", "Fuchs", "Eder", "Fischer", "Schmid",
    "Winkler", "Weber", "Schwarz", "Maier", "Schneider", "Reiter",
    "Wimmer", "Egger", "Brunner", "Lang", "Baumgartner", "Auer",
]

DOCTORS = [
    "Dr. Weissenbach", "Dr. Kaltenegger", "Dr. Horvath", "Dr. Steindl",
    "Dr. Pichler-Nagy", "Dr. Frühwirth", "Dr. Oberländer", "Dr. Szabo",
    "Dr. Lindmoser", "Dr. Ebner", "Dr. Rautner", "Dr. Gollner",
]

# Prefixes for the "other" bucket. None of these collide with a rule prefix,
# so they all derive to the default category (Normal_Invoices).
OTHER_PREFIXES = ["ORD", "MED", "HNO", "DER", "INT", "GYN", "PHY", "ALL"]

PATIENT_CATEGORIES_BY_GROUP = {
    "UNI": ["UNIQA Versichert", "UNIQA VitalPlan"],
    "AME": ["AMED Firmenkunde", "AMED Vorsorge"],
    "PRO": ["Selbstzahler"],
    "OTHER": ["Selbstzahler", "Wahlarzt", "Privatpatient"],
}

# Position pools per prefix group: (text, vat bucket, price lo, price hi).
# Medical services are VAT-exempt (0%); own products carry 10% (supplements)
# or 20% (cosmetics / devices); attests and reports carry 20%.
SERVICE_POSITIONS = [
    ("Erstuntersuchung", 0, 80.0, 220.0),
    ("Kontrolluntersuchung", 0, 60.0, 140.0),
    ("Blutabnahme inkl. Labor", 0, 45.0, 160.0),
    ("Impfung inkl. Beratung", 0, 40.0, 95.0),
    ("Ultraschall", 0, 70.0, 180.0),
    ("EKG", 0, 55.0, 120.0),
    ("Infusionstherapie", 0, 65.0, 190.0),
    ("Attest / Befundbericht", 20, 25.0, 60.0),
]

PRODUCT_POSITIONS = [
    ("Vitamin-D3 Tropfen", 10, 12.0, 35.0),
    ("Magnesium Komplex", 10, 10.0, 28.0),
    ("Omega-3 Kapseln", 10, 15.0, 42.0),
    ("Dermokosmetik Creme", 20, 18.0, 65.0),
    ("Blutdruckmessgerät", 20, 45.0, 130.0),
    ("Kinesio-Tape Set", 20, 12.0, 38.0),
    ("Ernährungsberatung Paket", 0, 60.0, 150.0),
]

STORNO_REASONS = ["Fehlbuchung", "Patientenwunsch", "Doppelte Rechnung"]


class MedcenterDataGenerator:
    def __init__(self, config: GenerationConfig):
        self.config = config
        self.rng = np.random.default_rng(config.seed)

        try:
            year_str, month_str = config.month.split("-")
            self.year = int(year_str)
            self.month = int(month_str)
            self.days_in_month = calendar.monthrange(self.year, self.month)[1]
        except (ValueError, IndexError):
            raise ValueError("Invalid month; expected YYYY-MM") from None

    # ------------------------------------------------------------------
    # Invoices
    # ------------------------------------------------------------------
    def generate_invoices(self) -> pd.DataFrame:
        cfg = self.config
        groups = list(category_prefix_rules.keys()) + ["OTHER"]
        rows = []
        for i in range(cfg.n_invoices):
            group = self.rng.choice(groups, p=list(cfg.category_mix))
            prefix = (
                group
                if group in category_prefix_rules
                else self.rng.choice(OTHER_PREFIXES)
            )
            invoice_number = (
                f"{prefix}{self.year}-{cfg.invoice_sequence_start + i:05d}"
            )
            invoice_date = date(
                self.year,
                self.month,
                int(self.rng.integers(1, self.days_in_month + 1)),
            )

            patient = (
                f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}"
            )
            patient_category = self.rng.choice(
                PATIENT_CATEGORIES_BY_GROUP[group]
            )
            doctor = self.rng.choice(DOCTORS)

            positions, net0, net10, net20 = self._draw_positions(group)
            vat10 = round(0.10 * net10, 2)
            vat20 = round(0.20 * net20, 2)
            gross = round(net0 + net10 + net20 + vat10 + vat20, 2)

            is_storno = bool(self.rng.random() < cfg.storno_rate)
            storno_reason = (
                str(self.rng.choice(STORNO_REASONS)) if is_storno else ""
            )
            # A cancelled invoice is not an open receivable.
            is_open = (
                False if is_storno else bool(self.rng.random() < cfg.open_rate)
            )

            rows.append(
                {
                    "Datum": invoice_date.strftime(DATE_FORMAT),
                    "Rechnungsnummer": invoice_number,
                    "Patient": patient,
                    "Patientenkategorie": str(patient_category),
                    "doctor_name": str(doctor),
                    "Positionen": positions,
                    "SummeUmsatzinklUSt": gross,
                    "Umsatz0": net0,
                    "Umsatz10": net10,
                    "Umsatz20": net20,
                    "MwSt10": vat10,
                    "MwSt20": vat20,
                    "Stornogrund": storno_reason,
                    "Invoice_open": is_open,
                    # helper fields for payment generation, dropped on write
                    "_date": invoice_date,
                }
            )

        return pd.DataFrame(rows)

    def _draw_positions(self, group: str) -> tuple[str, float, float, float]:
        """Draw 1-3 billing positions and sum their nets per VAT bucket."""
        pool = PRODUCT_POSITIONS if group == "PRO" else SERVICE_POSITIONS
        n_positions = int(self.rng.integers(1, 4))
        picks = self.rng.choice(len(pool), size=n_positions, replace=False)

        texts = []
        nets = {0: 0.0, 10: 0.0, 20: 0.0}
        for idx in picks:
            text, bucket, lo, hi = pool[idx]
            amount = round(float(self.rng.uniform(lo, hi)), 2)
            nets[bucket] = round(nets[bucket] + amount, 2)
            texts.append(text)
        return " | ".join(texts), nets[0], nets[10], nets[20]

    # ------------------------------------------------------------------
    # Payments
    # ------------------------------------------------------------------
    def generate_payments(self, invoices: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config
        rows = []

        # One receipt per settled invoice (not cancelled, not open). Receipt
        # and clearing dates are capped at month end so every month's file is
        # self-contained — downstream Hobex settlements and bank credits are
        # derived from exactly one generated month.
        month_end = date(self.year, self.month, self.days_in_month)
        settled = invoices[
            (invoices["Stornogrund"] == "") & (~invoices["Invoice_open"])
        ]
        for _, inv in settled.iterrows():
            pay_date = min(
                inv["_date"] + timedelta(days=int(self.rng.integers(0, 11))),
                month_end,
            )
            category = self.rng.choice(
                payment_categories, p=list(cfg.payment_category_weights)
            )
            rows.append(self._payment_row(
                pay_date, category, inv["SummeUmsatzinklUSt"], inv["doctor_name"]
            ))
            if len(rows) >= cfg.n_payments:
                break

        # Walk-in receipts (front-desk product / cash sales) top up the file.
        walk_in_weights = [0.10, 0.05, 0.45, 0.40]  # cash & card heavy
        while len(rows) < cfg.n_payments:
            pay_date = date(
                self.year,
                self.month,
                int(self.rng.integers(1, self.days_in_month + 1)),
            )
            category = self.rng.choice(payment_categories, p=walk_in_weights)
            amount = round(float(self.rng.uniform(5.0, 120.0)), 2)
            rows.append(self._payment_row(
                pay_date, category, amount, self.rng.choice(DOCTORS)
            ))

        payments = pd.DataFrame(rows).sort_values("_date", kind="stable")
        payments["belegnr"] = [
            f"BEL{self.year}-{cfg.payment_sequence_start + i:05d}"
            for i in range(len(payments))
        ]
        return payments

    def _payment_row(self, pay_date, category, amount, doctor) -> dict:
        category = str(category)
        # Cash clears immediately; card / transfer settle within a few days
        # (capped at month end — see generate_payments).
        clearing_offset = (
            0 if category == "Barzahlung" else int(self.rng.integers(1, 4))
        )
        clearing_date = min(
            pay_date + timedelta(days=clearing_offset),
            date(self.year, self.month, self.days_in_month),
        )
        return {
            "belegnr": "",  # assigned after date-sorting
            "belegdatum": pay_date.strftime(DATE_FORMAT),
            "Payment_Category": category,
            "betrag": round(float(amount), 2),
            "ClearingDate": clearing_date.strftime(DATE_FORMAT),
            "doctor_name": str(doctor),
            "_date": pay_date,
        }

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, pd.DataFrame]:
        invoices = self.generate_invoices()
        payments = self.generate_payments(invoices)
        return {
            raw_invoices_table: invoices[INVOICE_COLUMNS],
            raw_payments_table: payments[PAYMENT_COLUMNS],
        }
