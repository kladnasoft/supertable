# Data Island Core — Data Quality

## Overview

The data quality subsystem monitors table health by running automated checks after every data ingestion. It tracks metrics over time, detects anomalies, and surfaces alerts in the WebUI's Data Quality page. The system includes 16 built-in checks covering row counts, freshness, schema drift, null rates, distribution shifts, and more — organized into "quick" checks (run on every ingest) and "deep" checks (run on demand or scheduled).

---

## How it works

### Trigger flow

1. After a successful `DataWriter.write()`, the write path calls `notify_ingest()` on the quality scheduler
2. The scheduler debounces rapid writes (configurable cooldown) to avoid redundant checks
3. When the cooldown expires, the scheduler runs the enabled checks for the affected table
4. Results are stored in Redis and made available via the quality API and UI

### Check levels

Checks are categorized by scope:

- **Table-level** (`T*` checks) — operate on aggregate table metadata: row count, file count, storage size, schema, freshness
- **Column-level** (`C*` and `D*` checks) — operate on per-column statistics: null rate, distinct count, min/max, mean, distribution

And by weight:

- **Quick** (`group: "quick"`) — lightweight, run automatically after ingestion. Enabled by default.
- **Deep** (`group: "deep"`) — heavier computations (histograms, entropy, percentiles). Disabled by default. Run manually or on schedule.

---

## Built-in checks

| ID | Name | Level | Group | Default threshold | Description |
|---|---|---|---|---|---|
| `T1` | Row count delta | table | quick | 30% | Alert when row count changes by more than threshold % |
| `T2` | Freshness | table | quick | 24 hours | Alert when table not updated within threshold hours |
| `T3` | Schema drift | table | quick | — | Alert on any column addition, removal, or type change |
| `T5` | Table size trend | table | quick | 50% | Alert when storage size changes by more than threshold % |
| `C1` | NULL rate | column | quick | 5 pp | Alert when NULL rate spikes by more than threshold percentage points |
| `C2` | Distinct count shift | column | quick | 50% | Alert when distinct value count changes by more than threshold % |
| `C3` | Min/Max breach | column | quick | — | Alert when values exceed previously observed min/max boundaries |
| `C4` | Uniqueness ratio | column | quick | — | Track uniqueness ratio trend (disabled by default) |
| `C5` | Zero/negative rate | column | quick | 5 pp | Alert when zero or negative value rate spikes |
| `C6` | Mean + Stddev drift | column | quick | 2.0σ | Alert when mean drifts beyond threshold standard deviations |
| `D1` | String length stats | column | deep | — | Track avg/min/max/median string length over time |
| `D2` | Shannon entropy | column | deep | 20% | Alert when entropy changes by more than threshold % |
| `D3` | Top N values shift | column | deep | — | Track top 10 most frequent values and their coverage |
| `D4` | Histogram | column | deep | — | Track value distribution in 10 equal-frequency buckets |
| `D5` | Percentiles (IQR) | column | deep | 30% | Alert when IQR changes by more than threshold % |
| `D7` | Placeholder rate | column | deep | 10% | Alert when placeholder values (n/a, unknown, etc.) exceed threshold % |

---

## Anomaly detection

`services/quality/anomaly.py` implements statistical anomaly detection on quality metric time series. The algorithm:

1. Collects the last N check results for a metric (configurable window)
2. Computes mean and standard deviation of the historical values
3. Checks if the latest value exceeds the threshold (in standard deviations or percentage points, depending on the check)
4. Returns an anomaly flag with severity (info, warning, critical)

This enables the quality system to distinguish between expected variation and genuine data issues — a 10% row count change is normal if the table always varies by 10%, but alarming if it has been stable for months.

---

## Quality scores

Each table receives a quality score (0–100) computed from its latest check results:

- Start at 100
- Subtract points for each failed check (weighted by severity)
- The score reflects overall data health at a glance

Scores are displayed on the Quality page and returned by the quality API endpoints.

---

## Scheduler

`services/quality/scheduler.py` coordinates when checks run:

**On-ingest checks** — triggered by `notify_ingest()` after writes. Debounced with a configurable cooldown (default: 60 seconds) to avoid redundant checks during rapid writes.

**Manual checks** — triggered by the `POST /reflection/quality/run` endpoint. Runs checks immediately for a specific table.

**Run-all** — `POST /reflection/quality/run-all` runs checks across all tables. Uses batch processing to avoid overloading the query engine.

The scheduler stores its state in Redis to coordinate across multiple server instances — if two API servers receive writes to the same table, only one quality check runs.

---

## History

`services/quality/history.py` stores check results in Redis sorted sets, keyed by table and check ID. Each result includes the timestamp, metric values, pass/fail status, and anomaly details. History is used by the anomaly detector for baseline computation and by the UI for trend visualization.

---

## API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/reflection/quality/overview` | Dashboard summary (scores, alerts) |
| `GET` | `/reflection/quality/tables` | Tables with quality scores |
| `GET` | `/reflection/quality/latest` | Latest check results for a table |
| `GET` | `/reflection/quality/history` | Historical check results |
| `GET` | `/reflection/quality/global-config` | Global quality configuration |
| `PUT` | `/reflection/quality/global-config` | Update global configuration |
| `GET` | `/reflection/quality/schedule` | Quality check schedule |
| `PUT` | `/reflection/quality/schedule` | Update schedule |
| `PUT` | `/reflection/quality/table-schedule` | Set per-table schedule |
| `DELETE` | `/reflection/quality/table-schedule` | Remove per-table schedule |
| `GET` | `/reflection/quality/table-schedules` | List per-table schedules |
| `GET` | `/reflection/quality/rules` | List quality rules |
| `POST` | `/reflection/quality/rules` | Create a rule |
| `PUT` | `/reflection/quality/rules` | Update a rule |
| `DELETE` | `/reflection/quality/rules` | Delete a rule |
| `POST` | `/reflection/quality/run` | Run checks on a table |
| `POST` | `/reflection/quality/run-all` | Run checks on all tables |

---

## Module structure

```
supertable/services/quality/
  __init__.py        Package marker and public API (13 lines)
  config.py          DQConfig, BUILTIN_CHECKS definitions (396 lines)
  checker.py         Check execution engine (465 lines)
  anomaly.py         Statistical anomaly detection (250 lines)
  history.py         Result storage and retrieval (234 lines)
  scheduler.py       On-ingest scheduling, debouncing, coordination (705 lines)
```

---

## Frequently asked questions

**Can I disable quality checks entirely?**
Yes. Set all checks to `enabled: false` in the global quality configuration via `PUT /reflection/quality/global-config`.

**How do I add a custom quality check?**
Add a new entry to `BUILTIN_CHECKS` in `config.py` with a unique ID, name, level, group, and threshold. Implement the check logic in `checker.py`. The scheduler will pick it up automatically.

**Do quality checks block writes?**
No. Quality checks run asynchronously after the write completes and the lock is released. The `notify_ingest()` call is non-blocking.

**How much Redis memory do quality results use?**
Each check result is a small JSON object (~200 bytes). With 100 tables, 16 checks each, and 30 days of history, the total is approximately 10 MB.
