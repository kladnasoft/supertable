# STREAD-011: Decimal ingestion fails while extracting Parquet statistics

Status: **Verified fixed in the 2026-09-14 recheck** · Priority: **P2** · Type: Ingestion / dependency compatibility

**Recheck:** All 1 originally assigned failing checks now pass their unchanged expectations. [Verification](../audit/sql_read_matrix_recheck_2026-09-14/original_issue_verification.json).

Observed in the 2026-09-14 SuperTable 3.4.1 LOCAL/DuckDB audit. Assigned failures: **0 query assertions; 1 additional check**.

Writing the events table with Decimal(12,3) fails before native ingestion completes, although the encoded decimal values themselves round-trip intact.

## Expected and actual behavior

**Expected:** Ingest all 24 event rows, preserve decimal values and type, publish a readable snapshot, and handle unsupported statistics safely.

**Actual:** DataWriter raises `ArrowNotImplementedError: Cannot extract statistics for type` while accessing decimal min/max statistics. The matrix records this failure and uses an explicit Arrow fixture fallback only to continue read testing.

## Reproduce

Run from the repository root with the audit dependencies installed and Docker available. Use a fresh output directory each time.

```bash
python -m scripts.sql_read_matrix --groups controls \
  --output /tmp/supertable-stread-011
```

To inspect the five isolated decimal encodings without Redis:

```bash
python -m scripts.sql_read_matrix.ingest_probes /tmp/supertable-decimal-encoding-probes
```

## Investigation starting point

Native Polars 1.42.1 emits an integer-backed decimal representation for which PyArrow 18.1.0 cannot expose converted min/max statistics. `_route_stats` accesses stat.min/stat.max before its decimal exclusion. Raw statistics and all data values remain accessible.

Source locations at the audited revision:

- [supertable/processing.py:1942](../supertable/processing.py#L1942)

## Acceptance criteria

- [ ] Skip or safely handle unsupported decimal statistics before accessing the failing converted min/max properties, or choose a verified compatible encoding.
- [ ] The events table must ingest successfully through DataWriter without the matrix fixture fallback.
- [ ] Preserve exact decimal values/types and keep arithmetic, reading, and other-column statistics correct.

## Assigned failures and evidence

| Case | Assigned failing modes | Count |
| --- | --- | ---: |
| [ingest_events](../audit/sql_read_matrix_2026-09-14/runs/04_supertable-sql-numeric-review/results.json) | setup | 1 |

One additional setup failure; it is separate from the 102 query failures. Five isolated encoding probes are saved in the audit artifacts. Default PyArrow encoding and Polars use_pyarrow=True avoid this specific error; these probes are diagnostic evidence, not implemented fixes.

[Decimal encoding probes](../audit/sql_read_matrix_2026-09-14/ingest_probes.json).

[Back to the issue index](README.md).
