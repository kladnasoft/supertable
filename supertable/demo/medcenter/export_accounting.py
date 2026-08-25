"""Accounting-import exports for the medcenter demo (the deadline-critical
shape).

Writes monthly semicolon CSVs in the fixed 12-column accounting-import
layout::

    satzart;konto;gkonto;belegnr;belegdatum;buchsymbol;buchcode;prozent;
    steuercode;betrag;steuer;text

Three files per month, one row per document:

  - **chargebee**    — every membership invoice, plus credit notes as
                       negative rows (buchsymbol ``GS``)
  - **stripe_weight**— every weight-program invoice, voids as negatives
  - **eigenprodukte**— the Mobimed own-product rows (from staging)

The account columns carry constant demo values (the real account mapping
is a finance-owned rule supplied after engagement); the tax columns are
stamped per row from the document's dominant VAT rate.
"""

import argparse
import os

import pandas as pd

from supertable.config.homedir import initialize_app_home

from supertable.demo.medcenter.defaults import (
    demo_month,
    export_dir,
    raw_chargebee_credit_notes,
    raw_chargebee_invoices,
    raw_stripe_invoices,
    stg_invoices_table,
)
from supertable.demo.medcenter.helpers import run_query
from supertable.demo.medcenter.validation import require_canonical_month

EXPORT_COLUMNS = [
    "satzart", "konto", "gkonto", "belegnr", "belegdatum", "buchsymbol",
    "buchcode", "prozent", "steuercode", "betrag", "steuer", "text",
]

# Constant demo values for the bookkeeping account columns.
DEMO_SATZART = 0
DEMO_KONTO = "200000"       # debtor collective account
DEMO_BUCHCODE = 1
GKONTO_BY_EXPORT = {
    "chargebee": "4100",
    "stripe_weight": "4200",
    "eigenprodukte": "4000",
}

# Demo tax codes per VAT rate. The layout carries one row per document, so
# a mixed-rate document is stamped with its dominant rate; the real
# per-rate splitting rule is finance-owned and arrives with the engagement.
DEMO_STEUERCODE_BY_RATE = {0: 0, 10: 1, 20: 2}


def _frame(
    export_key: str,
    belegnr: pd.Series,
    belegdatum: pd.Series,
    buchsymbol,
    prozent: pd.Series,
    betrag: pd.Series,
    steuer: pd.Series,
    text: pd.Series,
) -> pd.DataFrame:
    prozent = prozent.astype(int)
    return pd.DataFrame(
        {
            "satzart": DEMO_SATZART,
            "konto": DEMO_KONTO,
            "gkonto": GKONTO_BY_EXPORT[export_key],
            "belegnr": belegnr,
            "belegdatum": pd.to_datetime(belegdatum).dt.strftime("%d.%m.%Y"),
            "buchsymbol": buchsymbol,
            "buchcode": DEMO_BUCHCODE,
            "prozent": prozent,
            "steuercode": prozent.map(DEMO_STEUERCODE_BY_RATE),
            "betrag": betrag.round(2),
            "steuer": steuer.round(2),
            "text": text,
        },
        columns=EXPORT_COLUMNS,
    )


def _empty_series() -> pd.Series:
    return pd.Series(dtype=object)


def _chargebee_frame(month: str) -> pd.DataFrame:
    month = require_canonical_month(month)
    invoices = run_query(
        f"SELECT invoice_number, MIN(date_issued) AS date_issued, "
        f"MIN(customer_name) AS customer_name, MIN(status) AS status, "
        f"MIN(total) AS total, "
        f"ROUND(SUM(line_tax_amount), 2) AS tax_total "
        f"FROM {raw_chargebee_invoices} "
        f"WHERE substr(date_issued, 1, 7) = '{month}' AND status <> 'void' "
        f"GROUP BY invoice_number ORDER BY invoice_number"
    )
    rate = invoices["tax_total"].gt(0).map({True: 20, False: 0}) \
        if len(invoices) else _empty_series()
    frames = [
        _frame(
            "chargebee",
            invoices["invoice_number"],
            invoices["date_issued"],
            "AR",
            rate,
            invoices["total"],
            invoices["tax_total"],
            "Mitgliedschaft " + invoices["customer_name"],
        )
    ]

    credit_notes = run_query(
        f"SELECT credit_note_number, date, amount, reference_invoice_number "
        f"FROM {raw_chargebee_credit_notes} "
        f"WHERE substr(date, 1, 7) = '{month}' ORDER BY credit_note_number"
    )
    if len(credit_notes):
        frames.append(
            _frame(
                "chargebee",
                credit_notes["credit_note_number"],
                credit_notes["date"],
                "GS",
                pd.Series([0] * len(credit_notes)),
                -credit_notes["amount"],
                pd.Series([0.0] * len(credit_notes)),
                "Gutschrift zu " + credit_notes["reference_invoice_number"],
            )
        )
    return pd.concat(frames, ignore_index=True)


def _stripe_weight_frame(month: str) -> pd.DataFrame:
    month = require_canonical_month(month)
    invoices = run_query(
        f"SELECT invoice_number, date, customer_name, status, amount "
        f"FROM {raw_stripe_invoices} "
        f"WHERE substr(date, 1, 7) = '{month}' ORDER BY invoice_number"
    )
    # Voided invoices enter the books as negative rows.
    sign = invoices["status"].map(
        lambda s: -1.0 if s == "void" else 1.0
    ) if len(invoices) else _empty_series()
    text_prefix = invoices["status"].map(
        lambda s: "STORNO " if s == "void" else ""
    ) if len(invoices) else _empty_series()
    return _frame(
        "stripe_weight",
        invoices["invoice_number"],
        invoices["date"],
        "AR",
        pd.Series([0] * len(invoices)),
        invoices["amount"] * sign,
        pd.Series([0.0] * len(invoices)),
        text_prefix + "Weight Programm " + invoices["customer_name"],
    )


def _eigenprodukte_frame(month: str) -> pd.DataFrame:
    month = require_canonical_month(month)
    stg = run_query(
        f"SELECT invoice_number, invoice_date, patient_name, positions, "
        f"gross_total, net_vat0, net_vat10, net_vat20, "
        f"vat10_amount, vat20_amount "
        f"FROM {stg_invoices_table} "
        f"WHERE category = 'Eigenprodukte' AND invoice_month = '{month}' "
        f"ORDER BY invoice_number"
    )

    def dominant_rate(row: pd.Series) -> int:
        nets = {0: row["net_vat0"], 10: row["net_vat10"],
                20: row["net_vat20"]}
        return max(nets, key=lambda rate: nets[rate])

    rates = (
        stg.apply(dominant_rate, axis=1) if len(stg) else _empty_series()
    )
    return _frame(
        "eigenprodukte",
        stg["invoice_number"],
        stg["invoice_date"],
        "AR",
        rates,
        stg["gross_total"],
        stg["vat10_amount"] + stg["vat20_amount"],
        stg["patient_name"] + " - " + stg["positions"],
    )


EXPORT_BUILDERS = {
    "chargebee": _chargebee_frame,
    "stripe_weight": _stripe_weight_frame,
    "eigenprodukte": _eigenprodukte_frame,
}


def export_accounting_import(
    month: str = demo_month, output_dir: str = export_dir
) -> list[str]:
    # Validate before creating the output directory, executing a query, or
    # deriving a filename.  The private builders repeat this check so a
    # direct internal call cannot bypass the SQL-literal boundary.
    month = require_canonical_month(month)
    os.makedirs(output_dir, exist_ok=True)
    written = []
    for export_key, builder in EXPORT_BUILDERS.items():
        export = builder(month)
        file_path = os.path.join(
            output_dir, f"accounting_import_{export_key}_{month}.csv"
        )
        export.to_csv(file_path, sep=";", index=False, float_format="%.2f")
        total = export["betrag"].sum() if len(export) else 0.0
        print(
            f"Accounting import written: {file_path} "
            f"({len(export)} row(s), betrag total {total:,.2f})"
        )
        written.append(file_path)
    return written


def _month_argument(value: str) -> str:
    try:
        return require_canonical_month(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Write the 12-column accounting-import CSVs"
    )
    ap.add_argument(
        "--month", type=_month_argument, default=demo_month, help="YYYY-MM"
    )
    ap.add_argument("--output-dir", default=export_dir)
    return ap.parse_args()


def main() -> None:
    initialize_app_home(change_cwd=True)
    args = parse_args()
    export_accounting_import(month=args.month, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
