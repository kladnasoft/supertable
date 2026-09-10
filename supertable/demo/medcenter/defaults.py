"""
Shared defaults for the medcenter finance demo.

Used by:
  - generate.py          — synthesize the monthly source-system fixtures
  - load.py              — idempotent load of the fixtures into raw_* tables
  - transform.py         — staging + mart builds
  - quality.py           — data-quality test suite
  - export_accounting.py — 12-column accounting-import CSVs
  - run.py               — end-to-end orchestration
"""

# --- SuperTable connection settings ---
organization: str = "kladna-soft"
super_name: str = "medcenter"
role_name: str = "superadmin"

# --- Data generation / loading settings ---
generated_data_dir: str = "medcenter_demo_data"
export_dir: str = "medcenter_demo_exports"

# Default demo month (YYYY-MM). One month of data is generated / processed.
demo_month: str = "2026-07"

# --- Legal entities (every row carries its entity tag) ---
legal_entities: dict[int, str] = {1: "medcenter_gmbh", 2: "medcenter_labs"}
legal_entity: str = legal_entities[1]  # default tag for single-entity sources

# One bank account per legal entity (synthetic IBANs).
bank_accounts: dict[str, str] = {
    legal_entities[1]: "AT611904300234573201",
    legal_entities[2]: "AT051904300298765432",
}

# Demo host that serves the (synthetic) bill PDFs each invoice row links to.
bill_url_base: str = "https://demo.dataisland.ai/bills"

# --- Raw tables (one folder per table under generated_data_dir) ---
raw_mobimed_invoices: str = "raw_mobimed_invoices"
raw_mobimed_payments: str = "raw_mobimed_payments"
raw_chargebee_subscriptions: str = "raw_chargebee_subscriptions"
raw_chargebee_invoices: str = "raw_chargebee_invoices"
raw_chargebee_credit_notes: str = "raw_chargebee_credit_notes"
raw_chargebee_transactions: str = "raw_chargebee_transactions"
raw_stripe_charges: str = "raw_stripe_charges"
raw_stripe_balance_transactions: str = "raw_stripe_balance_transactions"
raw_stripe_payouts: str = "raw_stripe_payouts"
raw_stripe_invoices: str = "raw_stripe_invoices"
raw_zoho_contacts: str = "raw_zoho_contacts"
raw_zoho_invoices: str = "raw_zoho_invoices"
raw_zoho_payments: str = "raw_zoho_payments"
raw_zoho_credit_notes: str = "raw_zoho_credit_notes"
raw_erste_camt053: str = "raw_erste_camt053"
raw_domonda_invoices: str = "raw_domonda_invoices"
raw_bmd_journal: str = "raw_bmd_journal"
raw_hobex_settlements: str = "raw_hobex_settlements"

# Legacy aliases used by the original Mobimed-only modules/tests.
raw_invoices_table: str = raw_mobimed_invoices
raw_payments_table: str = raw_mobimed_payments

# --- Staging / mart tables ---
stg_invoices_table: str = "stg_mobimed_invoices"
mart_monthly_table: str = "mart_mobimed_monthly"
mart_revenue_monthly: str = "mart_revenue_monthly"
mart_bank_reconciliation: str = "mart_bank_reconciliation"
mart_open_invoices: str = "mart_open_invoices"
mart_doctor_monthly: str = "mart_doctor_monthly"
mart_deferred_revenue: str = "mart_deferred_revenue"
mart_settlement_recon: str = "mart_settlement_recon"

# File-export sources land as semicolon CSV; API sources land as parquet.
csv_tables: set[str] = {
    raw_mobimed_invoices,
    raw_mobimed_payments,
    raw_erste_camt053,
    raw_bmd_journal,
    raw_hobex_settlements,
}

# Upsert keys per table: re-loading the same file overwrites instead of
# duplicating — this is what makes every load idempotent.
overwrite_columns_by_table: dict[str, list[str]] = {
    raw_mobimed_invoices: ["Rechnungsnummer"],
    raw_mobimed_payments: ["belegnr"],
    raw_chargebee_subscriptions: ["subscription_id"],
    raw_chargebee_invoices: ["invoice_number", "line_no"],
    raw_chargebee_credit_notes: ["credit_note_number"],
    raw_chargebee_transactions: ["transaction_id"],
    raw_stripe_charges: ["charge_id"],
    raw_stripe_balance_transactions: ["balance_txn_id"],
    raw_stripe_payouts: ["payout_id"],
    raw_stripe_invoices: ["invoice_number"],
    raw_zoho_contacts: ["customer_id"],
    raw_zoho_invoices: ["invoice_number", "line_no"],
    raw_zoho_payments: ["payment_id"],
    raw_zoho_credit_notes: ["credit_note_number"],
    raw_erste_camt053: ["entry_id"],
    raw_domonda_invoices: ["document_id"],
    raw_bmd_journal: ["company_id", "fiscal_year", "sequence_no"],
    raw_hobex_settlements: ["terminal_id", "settlement_date"],
    stg_invoices_table: ["invoice_number"],
    mart_monthly_table: ["invoice_month", "category"],
    mart_revenue_monthly: ["month", "system", "entity"],
    mart_bank_reconciliation: ["entry_id"],
    mart_open_invoices: ["system", "invoice_number"],
    mart_doctor_monthly: ["month", "doctor_name"],
    mart_deferred_revenue: ["month", "system"],
    mart_settlement_recon: ["settlement_date", "terminal_id"],
}

# --- Volumes per month (small but realistic; scale is not the point) ---
n_chargebee_invoices: int = 180
n_chargebee_credit_notes: int = 8
n_stripe_weight_invoices: int = 60
n_zoho_invoices: int = 40
n_zoho_credit_notes: int = 3
n_domonda_invoices: int = 25

# --- Canonical category derivation (demo stand-in for the real mapping) ---
# Invoice-number prefix -> category; anything else falls through to the
# default. The staging model is the single place this rule is applied;
# every downstream view filters off that one definition.
category_prefix_rules: dict[str, str] = {
    "UNI": "UNIQA",
    "AME": "AMED",
    "PRO": "Eigenprodukte",
}
category_default: str = "Normal_Invoices"

# Payment categories as they appear in Mobimed payment exports.
payment_categories: list[str] = [
    "Hobex",
    "Banküberweisung",
    "Barzahlung",
    "Kartenzahlung",
]

# Doctor billing: the fee share the center retains (demo constant).
doctor_take_rate: float = 0.30

# Hobex card-settlement fee rate (demo constant).
hobex_fee_rate: float = 0.0185
