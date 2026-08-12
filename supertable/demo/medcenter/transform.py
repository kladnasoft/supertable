"""Staging + mart builds for the medcenter demo (the dbt-model analogue).

``stg_mobimed_invoices`` types the raw Mobimed export (date parsing,
rounded amounts), applies the canonical category derivation exactly once,
and stamps the constant legal-entity tag. On top of the staging table and
the other raw sources, the marts produce the finance outputs the platform
exists for:

  - ``mart_mobimed_monthly``      month x category x VAT sums
  - ``mart_revenue_monthly``      month x source system x legal entity
  - ``mart_bank_reconciliation``  every bank line matched to its invoice
                                  (reference / counterparty tiers, review
                                  queue for the rest)
  - ``mart_open_invoices``        unpaid invoices across all AR systems
  - ``mart_doctor_monthly``       per-doctor totals + retained take rate
  - ``mart_deferred_revenue``     service-period split: this month vs later
  - ``mart_settlement_recon``     Hobex vs Mobimed card receipts vs bank

Everything is upserted on its natural key, so re-running the transform is
idempotent. Storno (cancelled) rows are kept in the monthly mart and only
counted separately — how they affect revenue is a business rule owned by
finance, not by the platform.
"""

import argparse

from supertable.demo.medcenter.defaults import (
    category_default,
    category_prefix_rules,
    doctor_take_rate,
    legal_entity,
    mart_bank_reconciliation,
    mart_deferred_revenue,
    mart_doctor_monthly,
    mart_monthly_table,
    mart_open_invoices,
    mart_revenue_monthly,
    mart_settlement_recon,
    raw_chargebee_invoices,
    raw_erste_camt053,
    raw_hobex_settlements,
    raw_mobimed_invoices,
    raw_mobimed_payments,
    raw_stripe_invoices,
    raw_zoho_invoices,
    stg_invoices_table,
)
from supertable.demo.medcenter.helpers import run_query, write_df


def category_case_sql(column: str = "Rechnungsnummer") -> str:
    """The one canonical category derivation, built from the rule table."""
    branches = "\n        ".join(
        f"WHEN substr({column}, 1, 3) = '{prefix}' THEN '{category}'"
        for prefix, category in category_prefix_rules.items()
    )
    return f"CASE\n        {branches}\n        ELSE '{category_default}'\n    END"


STAGING_QUERY = f"""
SELECT
    strptime(Datum, '%d.%m.%Y')::DATE                  AS invoice_date,
    strftime(strptime(Datum, '%d.%m.%Y'), '%Y-%m')     AS invoice_month,
    Rechnungsnummer                                    AS invoice_number,
    Patient                                            AS patient_name,
    Patientenkategorie                                 AS patient_category,
    doctor_name                                        AS doctor_name,
    Positionen                                         AS positions,
    ROUND(SummeUmsatzinklUSt, 2)                       AS gross_total,
    ROUND(Umsatz0, 2)                                  AS net_vat0,
    ROUND(Umsatz10, 2)                                 AS net_vat10,
    ROUND(Umsatz20, 2)                                 AS net_vat20,
    ROUND(MwSt10, 2)                                   AS vat10_amount,
    ROUND(MwSt20, 2)                                   AS vat20_amount,
    NULLIF(TRIM(Stornogrund), '')                      AS storno_reason,
    Invoice_open                                       AS is_open,
    {category_case_sql()}                              AS category,
    '{legal_entity}'                                   AS legal_entity
FROM {raw_mobimed_invoices}
"""

MOBIMED_MONTHLY_QUERY = f"""
SELECT
    invoice_month,
    category,
    legal_entity,
    CAST(COUNT(*) AS INTEGER)                          AS invoice_count,
    CAST(SUM(CASE WHEN storno_reason IS NOT NULL THEN 1 ELSE 0 END)
         AS INTEGER)                                   AS storno_count,
    ROUND(SUM(net_vat0), 2)                            AS net_vat0,
    ROUND(SUM(net_vat10), 2)                           AS net_vat10,
    ROUND(SUM(net_vat20), 2)                           AS net_vat20,
    ROUND(SUM(vat10_amount), 2)                        AS vat10_amount,
    ROUND(SUM(vat20_amount), 2)                        AS vat20_amount,
    ROUND(SUM(gross_total), 2)                         AS gross_total
FROM {stg_invoices_table}
GROUP BY invoice_month, category, legal_entity
ORDER BY invoice_month, category
"""

REVENUE_MONTHLY_QUERY = f"""
SELECT
    month,
    system,
    entity,
    CAST(COUNT(*) AS INTEGER)                          AS invoice_count,
    ROUND(SUM(gross), 2)                               AS gross_total
FROM (
    SELECT substr(date_issued, 1, 7) AS month, 'chargebee' AS system,
           '{legal_entity}' AS entity, total AS gross
    FROM (SELECT DISTINCT invoice_number, date_issued, total
          FROM {raw_chargebee_invoices} WHERE status <> 'void')
    UNION ALL
    SELECT substr(date, 1, 7), 'stripe_weight', '{legal_entity}', amount
    FROM {raw_stripe_invoices} WHERE status <> 'void'
    UNION ALL
    SELECT substr(invoice_date, 1, 7), 'zoho', legal_entity, total
    FROM (SELECT DISTINCT invoice_number, invoice_date, legal_entity, total
          FROM {raw_zoho_invoices} WHERE status NOT IN ('draft', 'void'))
    UNION ALL
    SELECT invoice_month, 'mobimed', legal_entity, gross_total
    FROM {stg_invoices_table} WHERE storno_reason IS NULL
)
GROUP BY month, system, entity
ORDER BY month, system, entity
"""

BANK_RECONCILIATION_QUERY = f"""
WITH refs AS (
    SELECT *,
           regexp_extract(remittance_structured,
                          '(MR|FK)[0-9]{{4}}-[0-9]{{5}}') AS ref
    FROM {raw_erste_camt053}
),
cb AS (SELECT DISTINCT invoice_number, total
       FROM {raw_chargebee_invoices}),
zh AS (SELECT DISTINCT invoice_number, total FROM {raw_zoho_invoices})
SELECT
    r.entry_id,
    CAST(r.booking_date AS DATE)                       AS booking_date,
    r.account_iban,
    r.amount,
    r.credit_debit,
    r.counterparty_name,
    r.remittance_structured,
    r.remittance_unstructured,
    CASE
        WHEN r.credit_debit = 'DBIT' THEN 'outgoing'
        WHEN cb.invoice_number IS NOT NULL THEN 'chargebee'
        WHEN zh.invoice_number IS NOT NULL THEN 'zoho'
        WHEN r.counterparty_name LIKE 'HOBEX%' THEN 'hobex'
        WHEN r.counterparty_name LIKE 'STRIPE%' THEN 'stripe_payout'
        ELSE 'unmatched'
    END                                                AS matched_system,
    COALESCE(cb.invoice_number, zh.invoice_number)     AS matched_invoice,
    CASE
        WHEN r.credit_debit = 'DBIT' THEN 'n/a'
        WHEN cb.invoice_number IS NOT NULL
             OR zh.invoice_number IS NOT NULL THEN 'reference'
        WHEN r.counterparty_name LIKE 'HOBEX%'
             OR r.counterparty_name LIKE 'STRIPE%' THEN 'counterparty'
        ELSE 'review'
    END                                                AS match_method
FROM refs r
LEFT JOIN cb ON r.ref = cb.invoice_number
LEFT JOIN zh ON r.ref = zh.invoice_number
ORDER BY r.entry_id
"""

OPEN_INVOICES_QUERY = f"""
SELECT * FROM (
    SELECT 'chargebee' AS system, invoice_number,
           customer_name AS counterparty,
           CAST(date_issued AS DATE) AS invoice_date,
           CAST(date_due AS DATE) AS due_date,
           total AS amount_open
    FROM (SELECT DISTINCT invoice_number, customer_name, date_issued,
                 date_due, total
          FROM {raw_chargebee_invoices} WHERE status = 'payment_due')
    UNION ALL
    SELECT 'zoho', invoice_number, customer_name,
           CAST(invoice_date AS DATE), CAST(due_date AS DATE), balance
    FROM (SELECT DISTINCT invoice_number, customer_name, invoice_date,
                 due_date, balance
          FROM {raw_zoho_invoices}
          WHERE status IN ('sent', 'overdue', 'partially_paid'))
    UNION ALL
    SELECT 'mobimed', invoice_number, patient_name,
           CAST(invoice_date AS DATE),
           CAST(invoice_date + INTERVAL 14 DAY AS DATE), gross_total
    FROM {stg_invoices_table} WHERE is_open
)
ORDER BY system, invoice_number
"""

DOCTOR_MONTHLY_QUERY = f"""
SELECT
    invoice_month                                      AS month,
    doctor_name,
    CAST(COUNT(*) AS INTEGER)                          AS invoice_count,
    ROUND(SUM(gross_total), 2)                         AS gross_total,
    ROUND(SUM(gross_total) * {doctor_take_rate}, 2)    AS medcenter_fee,
    ROUND(SUM(gross_total) * (1 - {doctor_take_rate}), 2) AS doctor_payout
FROM {stg_invoices_table}
WHERE storno_reason IS NULL
GROUP BY invoice_month, doctor_name
ORDER BY invoice_month, doctor_name
"""

DEFERRED_REVENUE_QUERY = f"""
WITH inv AS (
    SELECT 'chargebee' AS system, invoice_number,
           CAST(date_issued AS DATE) AS billed_on,
           CAST(service_period_start AS DATE) AS sps,
           CAST(service_period_end AS DATE) AS spe, total
    FROM (SELECT DISTINCT invoice_number, date_issued, service_period_start,
                 service_period_end, total
          FROM {raw_chargebee_invoices} WHERE status = 'paid')
    UNION ALL
    SELECT 'stripe_weight', invoice_number, CAST(date AS DATE),
           CAST(service_period_start AS DATE),
           CAST(service_period_end AS DATE), amount
    FROM {raw_stripe_invoices} WHERE status = 'paid'
    UNION ALL
    SELECT 'zoho', invoice_number, CAST(invoice_date AS DATE),
           CAST(cf_service_start AS DATE), CAST(cf_service_end AS DATE), total
    FROM (SELECT DISTINCT invoice_number, invoice_date, cf_service_start,
                 cf_service_end, total
          FROM {raw_zoho_invoices}
          WHERE status NOT IN ('draft', 'void'))
),
calc AS (
    -- DATE - DATE yields integer days in DuckDB; date_diff() is avoided
    -- because the read path's sqlglot round-trip reorders its arguments.
    SELECT system, strftime(billed_on, '%Y-%m') AS month, total,
           spe - sps + 1 AS period_days,
           GREATEST(0,
                    LEAST(spe, last_day(billed_on))
                    - GREATEST(sps,
                               CAST(date_trunc('month', billed_on) AS DATE))
                    + 1) AS days_in_month
    FROM inv
)
SELECT
    month,
    system,
    CAST(COUNT(*) AS INTEGER)                          AS invoice_count,
    ROUND(SUM(total), 2)                               AS billed_total,
    ROUND(SUM(total * days_in_month / period_days), 2) AS revenue_current_month,
    ROUND(SUM(total * (period_days - days_in_month) / period_days), 2)
                                                       AS deferred_to_future
FROM calc
GROUP BY month, system
ORDER BY month, system
"""

SETTLEMENT_RECON_QUERY = f"""
WITH hb AS (
    SELECT merchant_id, terminal_id,
           CAST(settlement_date AS DATE) AS settlement_date,
           transaction_count, gross_amount, fee_amount, net_amount
    FROM {raw_hobex_settlements}
),
mm AS (
    SELECT strptime(ClearingDate, '%d.%m.%Y')::DATE AS d,
           ROUND(SUM(betrag), 2) AS mobimed_card_gross,
           CAST(COUNT(*) AS INTEGER) AS mobimed_receipts
    FROM {raw_mobimed_payments}
    WHERE Payment_Category = 'Hobex'
    GROUP BY 1
),
bank AS (
    SELECT CAST(booking_date AS DATE) AS booking_date, amount
    FROM {raw_erste_camt053}
    WHERE counterparty_name LIKE 'HOBEX%'
)
SELECT
    hb.settlement_date,
    hb.terminal_id,
    hb.transaction_count,
    COALESCE(mm.mobimed_receipts, 0)                   AS mobimed_receipts,
    hb.gross_amount,
    COALESCE(mm.mobimed_card_gross, 0)                 AS mobimed_card_gross,
    ROUND(hb.gross_amount - COALESCE(mm.mobimed_card_gross, 0), 2)
                                                       AS delta_gross,
    hb.fee_amount,
    hb.net_amount,
    COALESCE(b.amount, 0)                              AS bank_credit,
    ROUND(hb.net_amount - COALESCE(b.amount, 0), 2)    AS delta_net
FROM hb
LEFT JOIN mm ON mm.d = hb.settlement_date
LEFT JOIN bank b
       ON b.booking_date = CAST(hb.settlement_date + INTERVAL 1 DAY AS DATE)
ORDER BY hb.settlement_date, hb.terminal_id
"""

MART_QUERIES: dict[str, str] = {
    mart_monthly_table: MOBIMED_MONTHLY_QUERY,
    mart_revenue_monthly: REVENUE_MONTHLY_QUERY,
    mart_bank_reconciliation: BANK_RECONCILIATION_QUERY,
    mart_open_invoices: OPEN_INVOICES_QUERY,
    mart_doctor_monthly: DOCTOR_MONTHLY_QUERY,
    mart_deferred_revenue: DEFERRED_REVENUE_QUERY,
    mart_settlement_recon: SETTLEMENT_RECON_QUERY,
}


def build_staging() -> int:
    df = run_query(STAGING_QUERY)
    columns, rows, inserted, deleted = write_df(stg_invoices_table, df)
    print(
        f"[{stg_invoices_table}] columns={columns} rows={rows} "
        f"inserted={inserted} deleted={deleted}"
    )
    return rows


def build_marts() -> None:
    for mart_name, query in MART_QUERIES.items():
        df = run_query(query)
        columns, rows, inserted, deleted = write_df(mart_name, df)
        print(
            f"[{mart_name}] columns={columns} rows={rows} "
            f"inserted={inserted} deleted={deleted}"
        )


def build_mart() -> int:
    """Backwards-compatible single-mart build (mobimed monthly)."""
    df = run_query(MOBIMED_MONTHLY_QUERY)
    columns, rows, inserted, deleted = write_df(mart_monthly_table, df)
    print(
        f"[{mart_monthly_table}] columns={columns} rows={rows} "
        f"inserted={inserted} deleted={deleted}"
    )
    return rows


def transform() -> None:
    build_staging()
    build_marts()


def main() -> None:
    argparse.ArgumentParser(
        description="Build the medcenter staging table and all marts"
    ).parse_args()
    transform()


if __name__ == "__main__":
    main()
