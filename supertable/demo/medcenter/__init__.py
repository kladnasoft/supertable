"""medcenter finance demo — a medical-center group's full finance stack.

Implements a multi-source finance data platform on SuperTable, built
entirely from specification with fully synthetic data: eight source
systems (Chargebee memberships, Stripe incl. the weight program, Zoho
B2B invoicing, Mobimed practice-management exports, Erste Bank camt.053
statement entries, Domonda supplier invoices, BMD booking journal, Hobex
card settlements) are generated with realistic cross-links — bank lines
reference real invoice numbers, card settlements tie out against terminal
receipts, Stripe payouts equal the sum of their balance transactions —
loaded idempotently into ``raw_*`` tables, transformed into a staging
model plus seven finance marts (revenue, bank reconciliation with review
queue, open invoices, doctor billing, deferred revenue, settlement
reconciliation, monthly VAT splits), validated with a dbt-style quality
suite, and exported into the fixed 12-column accounting-import layout.

Every invoice row carries a ``pdf_url`` to the demo bill host, so an AI
workbench (e.g. Lighthouse) can link any figure back to "the actual
bill". See ``demo_queries.md`` for a ready-made question/query pack and
``annotations.json`` for the business-semantics layer (descriptions,
measures, joins, PII, policies) importable on Lighthouse's
``/annotations`` page.

Runnable entry points::

    python -m supertable.demo.medcenter.generate            # synthesize all fixtures
    python -m supertable.demo.medcenter.load                # load raw_* tables (idempotent)
    python -m supertable.demo.medcenter.transform           # staging + all marts
    python -m supertable.demo.medcenter.quality             # data-quality suite
    python -m supertable.demo.medcenter.export_accounting   # 12-column import CSVs
    python -m supertable.demo.medcenter.run                 # full end-to-end demo

Console-script aliases shipped with the package::

    supertable-demo-medcenter-generate
    supertable-demo-medcenter-load
    supertable-demo-medcenter-transform
    supertable-demo-medcenter-quality
    supertable-demo-medcenter-export
    supertable-demo-medcenter-run

Configuration lives in ``supertable.demo.medcenter.defaults``.
"""
