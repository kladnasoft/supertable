# Task: A/B benchmark SuperTable HEAD (3.3.0) against tag v2.4.0

You are running a performance comparison between two versions of a Python
data-lake library. You have no prior knowledge of this codebase — everything
you need is below. Follow it literally. Where it says "verify", actually run
the check and stop if it fails; do not proceed on assumption.

Budget roughly **90–120 minutes** of wall time.

---

## 0. What you are comparing, and the one thing that will confuse you

| | Arm A ("new") | Arm B ("old") |
|---|---|---|
| git ref | `HEAD` of branch `master` | tag `v2.4.0` |
| commit | `5fe87d0` | `97ef410` |
| **version string it reports** | `3.3.0` | **`2.3.9`** |

**The `v2.4.0` tag reports its version as `2.3.9`.** The release bump was never
applied. Every result file the old arm writes is named `*-2.3.9-*.json`. This
is expected — do not "fix" it, do not conclude you checked out the wrong thing.
Verify the commit hash instead.

---

## 1. Environment

```bash
REPO=/home/kladnasoft/dev/dataisland/supertable
PY=$REPO/.venv/bin/python          # use this interpreter everywhere, not `python3`
```

Verify before starting:

```bash
cd $REPO
git rev-parse --short HEAD                 # expect 5fe87d0
git rev-parse --short v2.4.0               # expect 97ef410
$PY -c "import duckdb, polars, pyarrow; print(duckdb.__version__, polars.__version__)"
nproc                                      # expect 8; if fewer, see §2
```

**Redis must be reachable.** The library needs it for its catalog. Verify:

```bash
cd $REPO && STORAGE_TYPE=LOCAL $PY -c "
from supertable.redis_catalog import RedisCatalog
print('redis ping:', RedisCatalog().ping())"
```

Expect `redis ping: True`. If this fails, stop and report — nothing below works
without it.

**Every command in this document must be run with `STORAGE_TYPE=LOCAL` in the
environment.** This selects local-filesystem storage instead of S3/MinIO.

---

## 2. CPU pinning (required, and it must be identical for both arms)

Both arms must run on the **same four physical cores**, so neither benefits from
a quieter scheduler. Use `taskset`:

```bash
CPUS=0-3          # exactly the same string for both arms, every run
taskset -c $CPUS <command>
```

If `nproc` reports fewer than 8, use `CPUS=0-1` instead and say so in your
report. Never change `CPUS` between arms or between runs.

**Before each run, record the machine load.** A loaded machine invalidates the
comparison more than any code difference:

```bash
uptime        # capture the 1-minute load average
```

Record it. If the 1-minute load average is above **2.0** before a run, wait
until it drops. Report the load average observed for every run.

---

## 3. Build the two working trees

Arm A is the repo itself. Arm B is a detached worktree at the tag.

```bash
cd $REPO
WT=/tmp/bench-v240                 # arm B tree
git worktree remove --force $WT 2>/dev/null || true
git worktree add -f --detach $WT v2.4.0
cd $WT && git rev-parse --short HEAD      # MUST print 97ef410
```

### 3.1 The benchmark suite does not exist at v2.4.0 — copy it in

The suite (`benchmarks/`) was added at v3.0.8. Copy the current one into the old
tree, **but delete its results directory**:

```bash
cp -r $REPO/benchmarks $WT/
rm -rf $WT/benchmarks/results/local $WT/benchmarks/results/minio
```

> **Why deleting results matters.** Each run writes
> `benchmarks/results/local/read-<version>-<scale>.json`. If you copy the new
> arm's results in, the old tree will contain `read-3.3.0-full.json` while
> writing `read-2.3.9-full.json`, and any script that globs for the newest file
> can silently read the wrong arm's numbers. This exact mistake produced a
> clean-looking but entirely fabricated comparison once already.

### 3.2 Two shims the old tree needs (neither changes what is measured)

**Shim 1 — a genuine NameError bug in v2.4.0.**
`supertable/engine/data_estimator.py:204` reads a variable `env_single` that is
never assigned. It fires whenever `STORAGE_ENDPOINT_URL` is set, which the
repo's tracked `.env` does. With `STORAGE_TYPE=LOCAL` that setting is
meaningless, so blank it in the old tree only:

```bash
sed -i 's|^STORAGE_ENDPOINT_URL=.*|STORAGE_ENDPOINT_URL=|' $WT/.env
grep -n '^STORAGE_ENDPOINT_URL=' $WT/.env      # expect: STORAGE_ENDPOINT_URL=
```

Without this, **every** scenario fails with
`RuntimeError: count failed (Status.ERROR): name 'env_single' is not defined`.

**Shim 2 — `DataReader.execute()` has no `fullscan` argument at v2.4.0.**
That parameter arrived with the 3.0.x pruning work. Remove it from the copied
harness in the old tree only:

```bash
cd $WT
$PY - <<'EOF'
import pathlib
p = pathlib.Path("benchmarks/read_suite.py")
s = p.read_text()
assert "        fullscan=FULLSCAN,\n" in s, "anchor missing - inspect manually"
p.write_text(s.replace("        fullscan=FULLSCAN,\n", "", 1))
print("fullscan kwarg removed")
EOF
```

This is behaviour-equivalent: at v2.4.0 predicate pruning is always on, which is
exactly what `fullscan=False` (the suite's default) requests on HEAD. Without
it every read scenario fails with
`TypeError: DataReader.execute() got an unexpected keyword argument 'fullscan'`.

**Do not apply either shim to arm A.**

### 3.3 Verify both arms start clean

```bash
cd $REPO && git status --short          # expect empty
cd $WT   && git status --short          # expect only .env and benchmarks/ modified
```

---

## 4. Build the datasets (once per arm)

Each arm writes its own lake, using its own writer. They are not shared.

```bash
cd $REPO && STORAGE_TYPE=LOCAL taskset -c $CPUS $PY -m benchmarks setup --profile local --scale full
cd $WT   && STORAGE_TYPE=LOCAL taskset -c $CPUS $PY -m benchmarks setup --profile local --scale full
```

Expect ~10 million rows in ~100 files per arm. If `setup` is not a valid
subcommand on either arm, skip it — the first `read` run builds the dataset
automatically. Pass `--rebuild` on the first read run of an arm to force it.

---

## 5. The runs

For **each arm**, run the read suite **3 times** and the write suite **3 times**.
Alternate arms — A, B, A, B, A, B — so that any drift in machine load hits both
equally. Do **not** run all of A then all of B.

```bash
# one read run
cd <ARM_DIR> && STORAGE_TYPE=LOCAL taskset -c $CPUS \
  $PY -m benchmarks read  --profile local --scale full --iterations 12

# one write run
cd <ARM_DIR> && STORAGE_TYPE=LOCAL taskset -c $CPUS \
  $PY -m benchmarks write --profile local --scale full
```

`--iterations 12` matters. The default is 5, and the short scenarios
(80–200 ms) have 30–70% run-to-run spread at that setting — enough to invent a
"regression" that does not exist. 12 roughly halves it.

**After every run, immediately copy the result file out**, because the next run
of the same arm overwrites it (the filename contains only the version, not a
timestamp):

```bash
mkdir -p /tmp/bench-out
# arm A, read, run 1:
cp $REPO/benchmarks/results/local/read-3.3.0-full.json  /tmp/bench-out/A-read-1.json
# arm B, read, run 1:
cp $WT/benchmarks/results/local/read-2.3.9-full.json    /tmp/bench-out/B-read-1.json
# ...and the same pattern for write-*.json and runs 2 and 3.
```

Name them exactly `A-read-{1,2,3}.json`, `B-read-{1,2,3}.json`,
`A-write-{1,2,3}.json`, `B-write-{1,2,3}.json`. Section 8 depends on it.

### 5.1 Known transient failure — retry, do not report it as a result

Redis runs behind Sentinel here and intermittently fails over. You will
occasionally see:

```
MasterNotFoundError: No master found for 'mymaster'
```

This is infrastructure, not the code under test. If a run reports any scenario
failing with `MasterNotFound` or `Timeout reading from socket`, **discard that
run entirely and repeat it.** Record how many retries each arm needed.

Any *other* error is a real result — report it with the scenario name and the
full message.

---

## 6. Supplementary concurrency harness (8 writers / 8 readers)

The built-in suite covers 8 parallel writers on **distinct** tables and 4 on the
**same** table, and does **no** concurrent reads at all. The comparison needs 8
of each, so run this extra harness too.

Save as `/tmp/bench-out/conc.py`. It takes the library path as argument 1 and a
label as argument 2, so the same file measures both arms.

```python
"""8-way concurrency: writers on one table, writers on distinct tables, readers.

Usage:  conc.py <library_root> <label>

Reports, per case: rows/s, latency percentiles, per-worker fairness, and a
correctness check. The correctness check is the important one - a throughput
number means nothing if rows went missing under contention.
"""
import sys, os, time, statistics, threading, uuid, json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

ROOT, LABEL = sys.argv[1], sys.argv[2]
sys.path.insert(0, ROOT)
os.environ.setdefault("STORAGE_TYPE", "LOCAL")
import logging; logging.disable(logging.WARNING)

import polars as pl
from supertable.super_table import SuperTable
from supertable.data_writer import DataWriter
from supertable.data_reader import DataReader, engine
from supertable.rbac.role_manager import RoleManager

ORG, THREADS, ROLE = "bench_conc", 8, "superadmin"
FLAKE = ("MasterNotFound", "No master found", "Timeout reading from socket")

def is_flake(e):
    t = f"{type(e).__name__}: {e}"
    return any(m in t for m in FLAKE)

def scalar(df):
    """First cell of a one-row result, for EITHER dataframe library.

    The two arms do not agree on this: v2.4.0's reader returns a pandas
    DataFrame, HEAD's returns a polars one, and they share almost no API.
    Using a polars-only accessor here made the old arm report a correctness
    FAILURE that did not exist - the worst possible false alarm, since a
    correctness failure is supposed to outrank every timing number.
    ``to_numpy`` is the one accessor both provide.
    """
    return int(df.to_numpy()[0][0])

def batch(w, seq, rows):
    base = w * 10_000_000 + seq * 10_000
    return pl.DataFrame({
        "id": list(range(base, base + rows)),
        "worker": [w] * rows,
        "seq": [seq] * rows,
        "amount": [float(i % 97) for i in range(rows)],
        "country": [["US", "DE", "FR", "JP"][i % 4] for i in range(rows)],
    })

def setup(sup):
    SuperTable(sup, ORG)
    RoleManager(super_name=sup, organization=ORG)

def write_case(label, sup, table_for, batches=12, rows=5000):
    setup(sup)
    lat = {w: [] for w in range(THREADS)}
    written, errors, flakes = Counter(), {w: [] for w in range(THREADS)}, Counter()
    barrier = threading.Barrier(THREADS)

    def work(w):
        writer = DataWriter(super_name=sup, organization=ORG)
        table = table_for(w)
        barrier.wait()                      # maximise overlap
        for seq in range(batches):
            t0 = time.perf_counter()
            try:
                writer.write(role_name=ROLE, simple_name=table,
                             data=batch(w, seq, rows), overwrite_columns=[])
                written[w] += rows
            except Exception as e:
                if is_flake(e):
                    flakes[w] += 1
                else:
                    errors[w].append(f"{type(e).__name__}: {e}"[:160])
            finally:
                lat[w].append((time.perf_counter() - t0) * 1000)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        list(pool.map(work, range(THREADS)))
    elapsed = time.perf_counter() - t0

    # Correctness: every row written must be readable.
    observed, read_err = 0, ""
    try:
        for t in sorted({table_for(w) for w in range(THREADS)}):
            df, status, msg = DataReader(
                super_name=sup, organization=ORG,
                query=f"SELECT COUNT(*) AS n FROM {t}").execute(
                    role_name=ROLE, engine=engine.AUTO)
            if str(status).endswith("OK"):
                observed += scalar(df)
            else:
                read_err = str(msg)[:160]
    except Exception as e:
        read_err = f"{type(e).__name__}: {e}"[:160]

    allv = [v for vs in lat.values() for v in vs]
    p50s = {w: statistics.median(v) for w, v in lat.items() if v}
    total = sum(written.values())
    return {
        "case": label, "rows_per_second": total / elapsed,
        "p50_ms": statistics.median(allv),
        "p95_ms": sorted(allv)[int(len(allv) * .95) - 1],
        "max_ms": max(allv),
        "fairness": max(p50s.values()) / max(min(p50s.values()), 1e-9),
        "rows_written": total, "rows_read_back": observed,
        "correct": (observed == total and not read_err),
        "read_error": read_err,
        "hard_errors": sum(len(v) for v in errors.values()),
        "flakes": sum(flakes.values()),
    }

QUERIES = [
    ("count_star",   "SELECT COUNT(*) AS n FROM shared"),
    ("group_by",     "SELECT country, COUNT(*) AS n FROM shared GROUP BY country"),
    ("filter_agg",   "SELECT SUM(amount) AS s FROM shared WHERE amount > 50"),
    ("point_lookup", "SELECT * FROM shared WHERE id = 10000 LIMIT 1"),
    ("order_limit",  "SELECT id, amount FROM shared ORDER BY id DESC LIMIT 1000"),
    ("distinct",     "SELECT COUNT(DISTINCT worker) AS n FROM shared"),
]

def read_case(sup, rounds=4):
    """8 concurrent readers, each cycling the query mix."""
    lat, errors, flakes = {w: [] for w in range(THREADS)}, [], Counter()
    barrier = threading.Barrier(THREADS)

    def work(w):
        barrier.wait()
        for r in range(rounds):
            name, sql = QUERIES[(w + r) % len(QUERIES)]
            t0 = time.perf_counter()
            try:
                df, status, msg = DataReader(
                    super_name=sup, organization=ORG, query=sql).execute(
                        role_name=ROLE, engine=engine.AUTO)
                if not str(status).endswith("OK"):
                    errors.append(f"{name}: {msg}"[:160])
            except Exception as e:
                if is_flake(e):
                    flakes[w] += 1
                else:
                    errors.append(f"{name}: {type(e).__name__}: {e}"[:160])
            lat[w].append((time.perf_counter() - t0) * 1000)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        list(pool.map(work, range(THREADS)))
    elapsed = time.perf_counter() - t0
    allv = [v for vs in lat.values() for v in vs]
    p50s = {w: statistics.median(v) for w, v in lat.items() if v}
    return {
        "case": "read_8_parallel_mixed",
        "queries_per_second": len(allv) / elapsed,
        "p50_ms": statistics.median(allv),
        "p95_ms": sorted(allv)[int(len(allv) * .95) - 1],
        "max_ms": max(allv),
        "fairness": max(p50s.values()) / max(min(p50s.values()), 1e-9),
        "queries": len(allv), "hard_errors": len(errors),
        "errors": list(dict.fromkeys(errors))[:5], "flakes": sum(flakes.values()),
        "correct": not errors,
    }

def single_case(sup):
    """Singleton baseline: one writer, then one reader, no contention."""
    setup(sup)
    writer = DataWriter(super_name=sup, organization=ORG)
    wl = []
    for seq in range(10):
        t0 = time.perf_counter()
        writer.write(role_name=ROLE, simple_name="solo",
                     data=batch(0, seq, 5000), overwrite_columns=[])
        wl.append((time.perf_counter() - t0) * 1000)
    rl = []
    for i in range(12):
        name, sql = QUERIES[i % len(QUERIES)]
        t0 = time.perf_counter()
        DataReader(super_name=sup, organization=ORG,
                   query=sql.replace("shared", "solo")).execute(
                       role_name=ROLE, engine=engine.AUTO)
        rl.append((time.perf_counter() - t0) * 1000)
    return {"case": "singleton",
            "write_p50_ms": statistics.median(wl),
            "write_rows_per_second": 50000 / (sum(wl) / 1000),
            "read_p50_ms": statistics.median(rl), "correct": True,
            "hard_errors": 0, "flakes": 0}

tag = uuid.uuid4().hex[:6]
out = [
    single_case(f"c_solo_{tag}"),
    write_case("write_8_same_table",      f"c_same_{tag}", lambda w: "shared"),
    write_case("write_8_distinct_tables", f"c_diff_{tag}", lambda w: f"t_{w}"),
]
# The read case needs data: reuse the same-table lake written above.
out.append(read_case(f"c_same_{tag}"))

print(json.dumps({"label": LABEL, "results": out}, indent=2))
```

Run it **3 times per arm**, alternating arms, pinned to the same cores:

```bash
for i in 1 2 3; do
  STORAGE_TYPE=LOCAL taskset -c $CPUS $PY /tmp/bench-out/conc.py $REPO A \
      > /tmp/bench-out/A-conc-$i.json
  STORAGE_TYPE=LOCAL taskset -c $CPUS $PY /tmp/bench-out/conc.py $WT   B \
      > /tmp/bench-out/B-conc-$i.json
done
```

If any case reports `"correct": false` or `hard_errors > 0`, that is a
**correctness finding** and outranks every timing number in your report. Quote
`rows_written` vs `rows_read_back` and the error text.

---

## 7. What the suites already measure (do not re-implement)

**Read suite — 16 scenarios**, each run `--iterations` times after a discarded
warmup, reporting min/p50/p95/max/stdev:

`random_1000_by_key`, `agg_30d_by_country`, `agg_24h_by_country`,
`top_1000_by_date_desc`, `top_10000_by_date_desc`, `count_star_full`,
`point_lookup_single_key`, `distinct_users_30d`, `dimension_filter_full_scan`,
`two_dim_group_by_30d`, `narrow_window_single_file`, `filtered_order_limit`,
`prune_30d_window`, `prune_24h_window`, `prune_key_point`, `prune_no_predicate`

**Write suite — 7 scenarios:**

`parallel_8_distinct_tables` (8 threads, 60 s), `parallel_4_same_table`
(4 threads, 60 s), `serial_append_only` (60 s), `large_batch_write` (500k rows
in one call), `upsert_existing_keys` (100k), `small_write_latency` (100 rows),
`lifecycle_append_delete_update_sealed` (append → delete → update).

**The three 60-second scenarios are time-boxed.** Their `p50` is always ~60000 ms
and is meaningless. Judge them **only** on `metrics.rows_per_second` or
`metrics.total_rows_per_second`. Reporting their p50 as a latency comparison is
a mistake.

**Each scenario also carries a `seal`** — a hash of the logical result, order-
and type-normalised. See §9.

---

## 8. Aggregation — average the 3 runs, then diff the averages

Write and run this. It reads the files from §5 and prints the report.

```python
"""Average 3 runs per arm, then diff the averages."""
import json, glob, statistics, os
OUT = "/tmp/bench-out"

def load(arm, suite):
    runs = []
    for p in sorted(glob.glob(f"{OUT}/{arm}-{suite}-*.json")):
        runs.append(json.load(open(p)))
    assert len(runs) == 3, f"{arm}/{suite}: expected 3 runs, found {len(runs)}"
    return runs

def scenario_metric(sc):
    """The number that means something for this scenario."""
    m = sc.get("metrics") or {}
    for k in ("total_rows_per_second", "rows_per_second"):
        if k in m:
            return k, m[k], "higher_is_better"
    return "p50_ms", (sc.get("timings_ms") or {}).get("p50"), "lower_is_better"

for suite in ("read", "write"):
    A, B = load("A", suite), load("B", suite)
    ids = [s["id"] for s in A[0]["scenarios"]]
    print(f"\n{'='*104}\n{suite.upper()}  —  mean of 3 runs   "
          f"(A = HEAD 3.3.0,  B = v2.4.0 reporting 2.3.9)\n{'='*104}")
    print(f"{'scenario':38s} {'metric':22s} {'A mean':>12s} {'B mean':>12s} "
          f"{'delta':>9s} {'A rsd':>7s} {'B rsd':>7s}")
    print("-"*104)
    for sid in ids:
        def series(runs):
            vals, key, direction = [], None, None
            for r in runs:
                sc = next((s for s in r["scenarios"] if s["id"] == sid), None)
                if not sc:
                    continue
                key, v, direction = scenario_metric(sc)
                if v is not None:
                    vals.append(v)
            return key, vals, direction
        ka, va, dirn = series(A)
        kb, vb, _    = series(B)
        if not va or not vb:
            print(f"{sid:38s} {'(missing in one arm)':22s}")
            continue
        ma, mb = statistics.mean(va), statistics.mean(vb)
        rsd = lambda v: (statistics.stdev(v)/statistics.mean(v)*100) if len(v) > 1 else 0.0
        # Delta is always "A relative to B", signed so + always means A is BETTER.
        raw = (ma - mb) / mb * 100
        delta = raw if dirn == "higher_is_better" else -raw
        print(f"{sid:38s} {ka:22s} {ma:12,.1f} {mb:12,.1f} "
              f"{delta:+8.1f}% {rsd(va):6.1f}% {rsd(vb):6.1f}%")

# Concurrency harness
print(f"\n{'='*104}\nCONCURRENCY (8 threads)  —  mean of 3 runs\n{'='*104}")
def conc(arm):
    per_case = {}
    for p in sorted(glob.glob(f"{OUT}/{arm}-conc-*.json")):
        for r in json.load(open(p))["results"]:
            per_case.setdefault(r["case"], []).append(r)
    return per_case
CA, CB = conc("A"), conc("B")
for case in CA:
    a, b = CA[case], CB.get(case, [])
    if not b:
        continue
    keys = [k for k in ("rows_per_second", "queries_per_second",
                        "write_rows_per_second", "p50_ms", "read_p50_ms",
                        "fairness") if k in a[0]]
    print(f"\n  {case}")
    for k in keys:
        ma = statistics.mean(x[k] for x in a)
        mb = statistics.mean(x[k] for x in b)
        higher = "per_second" in k
        raw = (ma - mb) / mb * 100
        d = raw if higher else -raw
        print(f"    {k:24s} A={ma:12,.1f}  B={mb:12,.1f}  {d:+7.1f}%")
    bad = [x for x in a + b if not x.get("correct", True) or x.get("hard_errors")]
    print(f"    correctness             "
          f"{'ALL OK' if not bad else '*** FAILURES: ' + str(len(bad)) + ' ***'}")
```

**Sign convention: a positive delta always means HEAD (arm A) is better.** The
script inverts latency metrics for you. State this convention in your report.

Also report, per scenario, the **relative standard deviation (rsd)** across the
3 runs of each arm. **A delta smaller than the larger of the two rsd values is
not a finding** — say so explicitly rather than reporting it as a change.

---

## 9. Seals — read this before interpreting anything

Every scenario carries a `seal.digest`: a hash of the logical result, with row
and column order ignored, ints and floats normalised, floats rounded, and
internal columns excluded.

**Compare the seals between arms, per scenario.** Two versions 9 releases apart
may legitimately return different results. If a seal differs:

- Report that scenario as **"behaviour changed"**, not as faster or slower.
- A timing delta on a scenario whose seal moved is meaningless — the two arms
  did different work. Exclude it from your performance conclusions and list it
  separately.

Extract them with:

```python
import json
a = json.load(open("/tmp/bench-out/A-read-1.json"))
b = json.load(open("/tmp/bench-out/B-read-1.json"))
sa = {s["id"]: (s.get("seal") or {}).get("digest") for s in a["scenarios"]}
sb = {s["id"]: (s.get("seal") or {}).get("digest") for s in b["scenarios"]}
for k in sa:
    if sa[k] != sb.get(k):
        print("SEAL DIFFERS:", k, sa[k], "->", sb.get(k))
```

---

## 10. Report

Produce, in this order:

1. **Setup actually used** — both commit hashes, `CPUS`, `nproc`, library
   versions, load average before each run, and how many runs you discarded to
   Sentinel flakes.
2. **Correctness first** — any seal differences, any `"correct": false`, any
   `hard_errors`, any scenario that errored. If a row went missing under
   concurrency, that is the headline and everything else is secondary.
3. **Read table** — per scenario: A mean, B mean, delta %, both rsd values.
4. **Write table** — same, using rows/s for the three time-boxed scenarios and
   p50 for the other four. Say which you used for each.
5. **Concurrency table** — singleton, 8 writers same table, 8 writers distinct
   tables, 8 parallel readers. Include the fairness ratio (max worker p50 ÷ min
   worker p50); a value far above 1.0 means workers are being starved.
6. **Conclusion** — separate three things explicitly:
   - scenarios where A is genuinely faster (delta exceeds both rsd values),
   - scenarios where A is genuinely slower (same test),
   - scenarios within noise, and scenarios excluded for seal drift.

Do not summarise a mixed result as a single average across scenarios. A mean
over unrelated query shapes hides exactly the regressions this exercise exists
to find.

---

## 11. Cleanup

```bash
cd $REPO
git worktree remove --force /tmp/bench-v240
git worktree list                      # /tmp/bench-v240 must be gone
git status --short                     # must be empty
```

The repo must end as clean as it started. `benchmarks/results/local/*.json` in
arm A are **committed baseline files** — if `git status` shows them modified,
restore them:

```bash
git checkout benchmarks/results/local/
```
