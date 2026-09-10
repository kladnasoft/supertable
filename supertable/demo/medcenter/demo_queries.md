# medcenter demo — question & query pack

Ready-made questions for demoing the medcenter dataset through an AI
workbench (Lighthouse) or any SQL client. Each natural-language question
is paired with the SQL it should resolve to, so the demo driver can both
ask live and fall back to the exact query.

The dataset: one SuperTable (`medcenter`) with 18 `raw_*` source tables
(Chargebee, Stripe, Zoho, Mobimed, Erste Bank camt.053, Domonda, BMD,
Hobex), one staging table and seven marts. Every invoice row carries a
`pdf_url` — surface it in results so any number links to its bill.

---

**1. "How much revenue did we make last month, per system?"**

```sql
SELECT system, entity, invoice_count, gross_total
FROM mart_revenue_monthly
WHERE month = '2026-07'
ORDER BY gross_total DESC;
```

**2. "Which bank payments could we not match to an invoice?"** (the daily
reconciliation question — the queue finance reviews by hand)

```sql
SELECT entry_id, booking_date, amount, counterparty_name,
       remittance_unstructured
FROM mart_bank_reconciliation
WHERE match_method = 'review'
ORDER BY booking_date;
```

**3. "Show me the invoice behind this bank payment."** (reference-tier
match, then jump to the bill)

```sql
SELECT b.entry_id, b.booking_date, b.amount,
       b.matched_invoice, i.customer_name, i.pdf_url
FROM mart_bank_reconciliation b
JOIN (SELECT DISTINCT invoice_number, customer_name, pdf_url
      FROM raw_zoho_invoices) i
  ON b.matched_invoice = i.invoice_number
WHERE b.matched_system = 'zoho'
ORDER BY b.booking_date;
```

**4. "Which invoices are still unpaid, and how much is outstanding?"**

```sql
SELECT system, COUNT(*) AS open_invoices,
       ROUND(SUM(amount_open), 2) AS amount_open
FROM mart_open_invoices
GROUP BY system ORDER BY amount_open DESC;
```

**5. "How much of July's billing is actually earned in July?"**
(deferred revenue — annual memberships and multi-month programs)

```sql
SELECT system, billed_total, revenue_current_month, deferred_to_future
FROM mart_deferred_revenue
WHERE month = '2026-07';
```

**6. "What do we owe each doctor for July?"** (take-rate model)

```sql
SELECT doctor_name, invoice_count, gross_total,
       medcenter_fee, doctor_payout
FROM mart_doctor_monthly
WHERE month = '2026-07'
ORDER BY gross_total DESC;
```

**7. "Did every card settlement reach the bank account?"**
(Hobex vs Mobimed vs bank — deltas should be zero)

```sql
SELECT settlement_date, gross_amount, mobimed_card_gross, delta_gross,
       net_amount, bank_credit, delta_net
FROM mart_settlement_recon
WHERE ABS(delta_gross) > 0.01 OR ABS(delta_net) > 0.01;
```

**8. "Own-product sales by VAT rate."** (the UVA feed)

```sql
SELECT invoice_month, net_vat0, net_vat10, net_vat20,
       vat10_amount, vat20_amount, gross_total
FROM mart_mobimed_monthly
WHERE category = 'Eigenprodukte';
```

**9. "Isolate UNIQA, then exclude it."** (the daily category slicer move)

```sql
SELECT * FROM mart_mobimed_monthly WHERE category = 'UNIQA';

SELECT invoice_month, SUM(invoice_count) AS invoices,
       ROUND(SUM(gross_total), 2) AS gross_total
FROM mart_mobimed_monthly
WHERE category != 'UNIQA'
GROUP BY invoice_month;
```

**10. "Do the official books agree with the source systems?"**
(BMD journal rows link back via document number)

```sql
SELECT j.booking_symbol, COUNT(*) AS journal_rows,
       ROUND(SUM(j.debit), 2) AS debit_total,
       ROUND(SUM(j.credit), 2) AS credit_total
FROM raw_bmd_journal j
GROUP BY j.booking_symbol;
```

**11. "Where did the Stripe payout on the bank statement come from?"**
(payout → balance transactions → charges → invoices, fees explained)

```sql
SELECT p.payout_id, p.arrival_date, p.amount AS payout,
       ROUND(SUM(b.amount), 2) AS charges_gross,
       ROUND(SUM(b.fee), 2) AS stripe_fees,
       COUNT(*) AS charge_count
FROM raw_stripe_payouts p
JOIN raw_stripe_balance_transactions b ON p.payout_id = b.payout_id
GROUP BY p.payout_id, p.arrival_date, p.amount
ORDER BY p.arrival_date;
```

**12. "Split the Stripe account into memberships vs weight program."**
(both streams blend in one account — `source_class` separates them)

```sql
SELECT source_class, COUNT(*) AS charges,
       ROUND(SUM(amount), 2) AS gross
FROM raw_stripe_charges
GROUP BY source_class;
```

**13. "How many active memberships do we have, and what is the MRR?"**

```sql
SELECT status, COUNT(*) AS subscriptions,
       ROUND(SUM(mrr), 2) AS monthly_recurring_revenue
FROM raw_chargebee_subscriptions
GROUP BY status;
```

**14. "Which membership invoices were refunded, and why?"**
(credit notes link back to their original invoice)

```sql
SELECT cn.credit_note_number, cn.reference_invoice_number, cn.date,
       cn.amount, cn.reason, i.customer_name
FROM raw_chargebee_credit_notes cn
JOIN (SELECT DISTINCT invoice_number, customer_name
      FROM raw_chargebee_invoices) i
  ON cn.reference_invoice_number = i.invoice_number
ORDER BY cn.date;
```

**15. "What did we spend with suppliers, per supplier?"** (the payable side)

```sql
SELECT supplier_name, COUNT(*) AS invoices,
       ROUND(SUM(gross_amount), 2) AS gross_spend
FROM raw_domonda_invoices
GROUP BY supplier_name
ORDER BY gross_spend DESC;
```

**16. "Are the official books complete?"** (every valid source invoice
should have exactly one AR row in the BMD journal)

```sql
SELECT j.contra_account,
       COUNT(*) AS journal_rows,
       ROUND(SUM(j.debit), 2) AS booked_gross
FROM raw_bmd_journal j
WHERE j.booking_symbol = 'AR'
GROUP BY j.contra_account;
```

**17. "How much cash came in and went out per month?"**

```sql
SELECT substr(booking_date, 1, 7) AS month,
       ROUND(SUM(CASE WHEN credit_debit = 'CRDT' THEN amount ELSE 0 END), 2) AS cash_in,
       ROUND(SUM(CASE WHEN credit_debit = 'DBIT' THEN amount ELSE 0 END), 2) AS cash_out
FROM raw_erste_camt053
GROUP BY 1 ORDER BY 1;
```

**18. "Which patients still owe us money, per doctor?"** (the front-desk
reminder list)

```sql
SELECT doctor_name, COUNT(*) AS open_invoices,
       ROUND(SUM(gross_total), 2) AS amount_open
FROM stg_mobimed_invoices
WHERE is_open
GROUP BY doctor_name
ORDER BY amount_open DESC;
```

**19. "How were cancelled invoices distributed by reason?"**

```sql
SELECT storno_reason, COUNT(*) AS cancelled,
       ROUND(SUM(gross_total), 2) AS cancelled_gross
FROM stg_mobimed_invoices
WHERE storno_reason IS NOT NULL
GROUP BY storno_reason;
```

**20. "Compare each month's card fees: Hobex terminal vs Stripe."**

```sql
SELECT substr(s.settlement_date, 1, 7) AS month,
       ROUND(SUM(s.fee_amount), 2) AS hobex_fees
FROM raw_hobex_settlements s
GROUP BY 1 ORDER BY 1;
-- and, side by side:
SELECT substr(b.available_on, 1, 7) AS month,
       ROUND(SUM(b.fee), 2) AS stripe_fees
FROM raw_stripe_balance_transactions b
GROUP BY 1 ORDER BY 1;
```
