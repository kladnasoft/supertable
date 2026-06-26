# TESTING.md — running & maintaining the characterization suite

This suite seals the **current** SuperTable read behavior as a backward-compat
oracle for the upcoming internal-`__rowid` + deletion-vector + compaction
migration. The golden bytes under `tests/golden/` are the contract; normal runs
**compare** against them and never regenerate them.

All commands use the repo's canonical interpreter and are run from the repo root:

```bash
cd /home/kladnasoft/dev/dataisland/supertable
```

> The suite pins a **hermetic environment** (local storage + fake Redis driven by
> the real Lua scripts via `lupa`) in `tests/conftest.py` *before* the first
> `supertable` import — no real Redis, S3, or network is touched.

---

## Layout

| File | Marker | Purpose |
|---|---|---|
| `tests/characterization/scenarios.py` | — | single source of truth: 60 scenarios |
| `tests/characterization/fixtures_lib.py` | — | deterministic parquet + `catalog.json` authoring |
| `tests/characterization/current_reader.py` | — | adapter over the **real** `query_sql` read path |
| `tests/characterization/table_result.py` | — | canonical result + normalization |
| `tests/characterization/comparison.py` | — | `assert_table_result_matches_golden` (multiset/ordered) |
| `tests/characterization/manifest.py` | — | `SEALED_MANIFEST.json` sha256 compute/validate |
| `tests/characterization/readers.py` | — | `TableReader` Protocol + Current/DeletionVector readers |
| `tests/characterization/test_golden.py` | `golden` | result scenarios vs sealed `result.json` |
| `tests/characterization/test_errors.py` | `golden` | error scenarios vs sealed `error.json` |
| `tests/characterization/test_compatibility.py` | `golden` | every `TableReader` vs the same goldens |
| `tests/characterization/test_cross_engine.py` | `spark` | Spark SQL vs the same goldens (integration) |
| `tests/characterization/test_perf.py` | `perf` | non-blocking benchmarks → `tests/perf_results/` |
| `tests/generate_current_behavior_golden.py` | — | the **only** deliberate reseal entry point |

---

## 1. Generate the original golden outputs (DELIBERATE reseal)

Golden artifacts may only be (re)generated from the **current** implementation,
before the refactor. Two equivalent entry points (they share one code path so
they cannot diverge); both print a prominent warning and overwrite sealed bytes:

```bash
# reseal everything
python -m tests.generate_current_behavior_golden
# reseal a subset
python -m tests.generate_current_behavior_golden basic_insert_single ts_null_timestamp

# equivalently, through pytest:
python -m pytest tests/characterization -m golden --create-golden
```

This rewrites `tests/golden/<id>/{input,expected}/…` and recomputes
`tests/golden/SEALED_MANIFEST.json`. **Review the golden diff before committing** —
a reseal changes the compatibility contract. Normal runs never pass these flags.

## 2. Run the normal tests (compare-only; fails loudly on drift)

```bash
python -m pytest tests/characterization
```

`spark` and `perf` tests self-skip without their flags, so this is the everyday
gate: result goldens + error goldens + reader-compatibility, with a one-shot
`SEALED_MANIFEST` integrity check first. A changed logical result produces a
structured diff (missing / unexpected / duplicate-count / type / column-order /
row-count), never a silent pass.

## 3. Run DuckDB-only tests

DuckDB is the default engine and the oracle. To assert nothing Spark/perf is even
collected:

```bash
python -m pytest tests/characterization -m "golden and not spark and not perf"
```

## 4. Run Spark cross-engine tests (integration CI)

Skipped by default. They need `--run-spark` **and** a reachable Spark Thrift
fleet that can see the local parquet fixtures (shared filesystem):

```bash
export SUPERTABLE_SPARK_THRIFT_HOST=spark-thrift.internal   # required
export SUPERTABLE_SPARK_THRIFT_PORT=10000                   # optional (default 10000)
python -m pytest tests/characterization -m spark --run-spark
```

The suite registers a cluster from those env vars into the catalog, reads each
scenario via `engine="spark"`, and compares against the **same** goldens. If the
fleet is unreachable it **skips** (it does not fail) so CI without Spark stays
green. The goldens are engine-independent logical output; no engine is forced to
imitate the other.

## 5. Validate fixture checksums

Integrity is checked automatically once per session (the `sealed_manifest_ok`
fixture in `tests/conftest.py`). To validate explicitly:

```bash
python -c "from tests.characterization.manifest import validate_manifest; \
from tests.characterization.scenarios import all_scenario_ids; \
p=validate_manifest(all_scenario_ids()); \
print('OK' if not p else '\n'.join(p))"
```

Any edited/corrupt/missing sealed file (input parquet, `catalog.json`, or
`expected/result.json`) fails as an **integrity error** — distinct from a logical
diff — with the reseal instructions. Checksums are keyed by path *relative to*
`tests/golden`, so they contain no environment-dependent absolute paths.

## 6. Run the performance benchmarks (non-blocking)

```bash
python -m pytest tests/characterization -m perf --run-perf
# optional: timing repeats per scenario (default 3)
SUPERTABLE_PERF_REPEATS=5 python -m pytest tests/characterization -m perf --run-perf
```

Writes `tests/perf_results/perf_<UTCstamp>.json` + `perf_latest.json` (gitignored;
timing-dependent, never sealed) with per-scenario file/row/tombstone counts,
output row count, read latency (min/median), peak `tracemalloc` memory, and a real
DuckDB `EXPLAIN ANALYZE` of the parquet reflection scan. It asserts only that the
harness ran — never a wall-clock threshold.

---

## Plugging in the future deletion-vector implementation

The whole suite is the conformance harness for the post-migration reader. When
the internal-`__rowid` + deletion-vector + compaction read path lands:

1. **Implement** `DeletionVectorReader.read(...)` in
   `tests/characterization/readers.py` to read the **existing sealed inputs**
   (they already contain only public columns + the existing `__timestamp__`; do
   **not** require `__rowid` in the fixtures) and return a canonical
   `TableResult` — public projection only (`__rowid` must never appear in
   `SELECT *`, sorting, or comparison).
2. **Flip** `DeletionVectorReader.available()` to `return True`.
3. **Run the compatibility gate** — it must pass against the unchanged goldens:

   ```bash
   python -m pytest tests/characterization/test_compatibility.py -v
   ```

   This parameterizes `(reader × non-error scenario)`; the new reader is now held
   to every sealed byte the current engine produced. **Do not reseal** — if a
   result differs, that is a real backward-incompatibility to fix in the new
   reader, not a golden to update.

4. (Optional) add the new reader to the **error** path and **cross-engine** path
   the same way if it must reproduce failure categories too.

> Acceptance: the same sealed fixtures execute unchanged against the new
> implementation, no test requires `__rowid`, and any externally observable
> difference yields a clear, actionable failure. The current behavior remains the
> sole oracle.
