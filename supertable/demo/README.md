# SuperTable demos

Self-contained demonstrations bundled with `pip install supertable`. All
demos talk to a live Redis + storage backend, so configure your environment
first — see [../../docs/02_configuration.md](../../docs/02_configuration.md).

## quickstart

A numbered sequence of small, focused scripts that walks through the full
SDK surface: create a SuperTable, configure RBAC, write fixtures, exercise
ingestion / staging / pipes, run reads with different engines, inspect
metadata, walk snapshot history, and tear everything down.

Run all steps in order:

```bash
supertable-demo-quickstart
# or
python -m supertable.demo.quickstart
```

Run any individual step directly:

```bash
python -m supertable.demo.quickstart.s01_01_01_create_super_table
python -m supertable.demo.quickstart.s03_08_read_snapshot_history
```

### Quickstart steps

| Module | Concept |
|---|---|
| `s01_01_01_create_super_table` | Bootstrap a SuperTable + Redis catalog |
| `s01_01_02_enable_mirroring_formats` | Turn on Delta / Iceberg mirroring |
| `s01_02_create_roles` / `s01_03_create_users` | RBAC setup |
| `s02_01_write_dummy_data` | Multi-batch upsert with schema evolution |
| `s02_02_write_single_data` | Single write with `lineage` payload |
| `s02_03_01_write_staging` / `s02_03_02_create_pipe` | Staging area + pipe |
| `s02_04_01_write_monitoring_simple` / `s02_04_02_write_monitoring_parallel` | `MonitoringWriter` |
| `s02_05_write_tombstone` | Soft-delete via `delete_only=True` |
| `s03_02_01_read_super_data_ok` / `s03_02_02_read_table_data_ok` | Reads with `engine.AUTO` and `engine.SPARK_SQL` |
| `s03_03_read_meta` | Schema and stats via `MetaReader` |
| `s03_07_01_estimate_read` / `s03_07_02_estimate_files` | Pre-flight estimation |
| `s03_08_read_snapshot_history` | Walk the snapshot linked list |
| `s05_01_delete_table` / `s05_02_delete_super_table` | Destructive teardown (commented out in the controller) |

Shared constants live in `supertable.demo.quickstart.defaults`; fixtures in
`supertable.demo.quickstart.dummy_data`.

## webshop

A larger end-to-end demo: synthesise a realistic webshop dataset
(categories, products, customers, sessions, orders, inventory) and load it
into SuperTable. Three console-script entry points:

| Console script | Module | Purpose |
|---|---|---|
| `supertable-demo-webshop-generate` | `supertable.demo.webshop.generate` | One-shot historical generation (writes parquet to disk) |
| `supertable-demo-webshop-load` | `supertable.demo.webshop.load` | Load the generated parquet files into SuperTable via `DataWriter` |
| `supertable-demo-webshop-topup` | `supertable.demo.webshop.topup` | Continuous incremental top-up against SuperTable |

Typical flow:

```bash
# 1. Generate ~1.2M synthetic rows on disk
supertable-demo-webshop-generate

# 2. Load into SuperTable
supertable-demo-webshop-load

# 3. (Optional) keep the data fresh
supertable-demo-webshop-topup --sleep-minutes 5
```

The `WebshopDataGenerator` engine itself lives in
`supertable.demo.webshop.core`; shared connection / output settings are in
`supertable.demo.webshop.defaults`.

## medcenter

A medical-center group's full finance stack, built entirely from
specification — fully synthetic data, no real records anywhere. Eight
source systems are generated with realistic cross-links and loaded into
one SuperTable:

```
 Chargebee   Stripe    Zoho     Mobimed      Erste Bank    Domonda    BMD       Hobex
 (parquet)  (parquet) (parquet) (CSV)      (camt.053 CSV) (parquet)  (CSV)     (CSV)
     |          |        |         |             |            |         |         |
     +----------+--------+---------+------+------+------------+---------+---------+
                                          v
                     raw_* tables (18) — idempotent upsert loads
                                          v
              stg_mobimed_invoices  +  7 finance marts (documented SQL)
```

The cross-links are the point: bank statement lines carry real `MR…`/`FK…`
invoice numbers in their remittance (a few deliberately mangled → review
queue), Hobex settlements tie out against Mobimed card receipts and the
bank credit, Stripe payouts equal the sum of their balance transactions,
BMD journal rows link back via document numbers, and every invoice row
carries a `pdf_url` to the demo bill host — so a workbench (e.g.
Lighthouse) can drill from any figure to "the actual bill".
`demo_queries.md` in the package is a ready-made question/query pack for
that session.

`annotations.json` is the matching business-semantics layer for
Lighthouse: table descriptions with grain and synonyms, canonical
measures and dimensions, every cross-table join, EUR units, PII
classification (patient names, bank counterparties, BMD booking texts),
data-quality rules, and MUST-FOLLOW policies (line-item grain dedup,
storno/void exclusion, MR/FK reference semantics, the two-entity split).
Upload it on Lighthouse's `/annotations` page via **Import annotations
from JSON** (`POST /api/annotation/import`) after connecting the
`medcenter` super — the catalog crawls the structure itself; this file
supplies what the structure can't say.

Marts: `mart_revenue_monthly`, `mart_bank_reconciliation`,
`mart_open_invoices`, `mart_doctor_monthly`, `mart_deferred_revenue`,
`mart_settlement_recon`, `mart_mobimed_monthly` — plus the three
12-column accounting-import CSVs (chargebee, stripe_weight,
eigenprodukte) and a 20+-test data-quality suite that seals the
arithmetic and every cross-system link.

| Console script | Module | Purpose |
|---|---|---|
| `supertable-demo-medcenter-generate` | `supertable.demo.medcenter.generate` | Synthesize all eight sources (seeded, reproducible; `--year` for 12 months) |
| `supertable-demo-medcenter-load` | `supertable.demo.medcenter.load` | Load fixtures into the 18 `raw_*` tables (idempotent upsert) |
| `supertable-demo-medcenter-transform` | `supertable.demo.medcenter.transform` | Build the staging table + all seven marts |
| `supertable-demo-medcenter-quality` | `supertable.demo.medcenter.quality` | Run the data-quality suite (arithmetic, cross-links, acceptance) |
| `supertable-demo-medcenter-export` | `supertable.demo.medcenter.export_accounting` | Write the three 12-column accounting-import CSVs |
| `supertable-demo-medcenter-run` | `supertable.demo.medcenter.run` | Full end-to-end demo; `--teardown` drops the demo tables |

Typical flow:

```bash
# Everything in one go: generate → load → transform → tests → showcase
# views → exports → idempotency proof
supertable-demo-medcenter-run

# Prove reproducibility: drop the tables, then run again — identical results
supertable-demo-medcenter-run --teardown
supertable-demo-medcenter-run

# Full year: 12 months, continuous year-scoped numbering everywhere
supertable-demo-medcenter-run --year 2026
```

Single-month and full-year modes mix freely: seeds and document-number
sequences derive from the calendar month itself, so `--month 2026-07`
produces byte-identical files to the July slice of `--year 2026`. Running
the default (single-month) demo on top of a year load is simply an
idempotent refresh of that month — the acceptance test then reconciles
the mart against **all** fixture months in the data folder, so keep the
fixture folder and the loaded tables in sync (or `--teardown` and clear
the folder for a full reset).

The Mobimed generator lives in `supertable.demo.medcenter.core`, the other
seven sources in `supertable.demo.medcenter.finance_sources`; connection
settings, table names, upsert keys, and the demo category rule are in
`supertable.demo.medcenter.defaults`.
