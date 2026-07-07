"""
stream_demo.py

Live streaming demo for SuperTable.

Every tick generates 50–100 synthetic webshop *session* rows (reusing the same
WebshopDataGenerator internals as topup.py) and writes them to SuperTable as a
single table update.  A self-refreshing web page drives the loop and reports two
numbers separately:

    • Generation time      — building the batch; runs in a SEPARATE process
                             (its own CPU core), ahead of time
    • SuperTable update    — the writer.write() round-trip (this process)

Because generation happens on another core while the previous batch is being
written, the two overlap: the effective cadence drops to ~1 update/second
(bounded by the write, not gen + write).

The browser drives each write, so the stream runs until you pause or close the
page (a full queue makes the generator idle when nobody is pulling).  Only the
`sessions` table is updated (append mode).

Everything is hardcoded for the webshop demo — just run it:

    python -m supertable.demo.webshop.stream_demo
    # then open http://127.0.0.1:8765  (opens automatically)

Optional: --port / --host / --no-browser override the defaults.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue as _queue
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

import supertable.config.homedir  # noqa: F401 — side effect: resolve/chdir app home
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.demo.webshop.core import GenerationConfig, WebshopDataGenerator
from supertable.demo.webshop.defaults import (
    generated_data_dir,
    organization,
    overwrite_columns,
    role_name,
    super_name,
)

# ─────────────────────────────────────────────────────────────────────────────
# Hardcoded demo settings
# ─────────────────────────────────────────────────────────────────────────────

HOST = "127.0.0.1"
PORT = 8765
TABLE = "sessions"          # the single table this demo updates
MIN_RECORDS = 50            # per-tick record count is random in [MIN, MAX]
MAX_RECORDS = 100
SEED = 99                   # distinct from the bulk-gen seed of 42
LIVE_WINDOW_SECONDS = 120   # session_start is spread over the last N seconds


# ─────────────────────────────────────────────────────────────────────────────
# Arrow conversion (guards against all-null columns, same as topup.py)
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_ms(x) -> str:
    return "     –" if x is None else f"{x:6.1f}ms"


def _to_arrow(df: pd.DataFrame) -> pa.Table:
    table = pa.Table.from_pandas(df, preserve_index=False)
    new_cols: list = []
    new_fields: list = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_null(field.type):
            pd_dtype = df[field.name].dtype
            if pd.api.types.is_integer_dtype(pd_dtype):
                target = pa.int64()
            elif pd.api.types.is_float_dtype(pd_dtype):
                target = pa.float64()
            elif pd.api.types.is_bool_dtype(pd_dtype):
                target = pa.bool_()
            else:
                target = pa.string()
            col = col.cast(target)
            field = pa.field(field.name, target, nullable=True)
        new_cols.append(col)
        new_fields.append(field)
    return pa.table(
        {f.name: c for f, c in zip(new_fields, new_cols)},
        schema=pa.schema(new_fields),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dimension loading + the generator process (runs on its own CPU core)
# ─────────────────────────────────────────────────────────────────────────────

def _row_count(path: Path) -> int | None:
    """Fast parquet row count from footer metadata (no full read)."""
    try:
        import pyarrow.parquet as pq
        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    except Exception:
        return None


def _load_dimensions(dim_dir: Path):
    """Load customers/products dimension frames, or cold-start a small set."""
    cust = dim_dir / "customers" / "customers.parquet"
    prod = dim_dir / "products" / "products.parquet"
    if cust.exists() and prod.exists():
        return pd.read_parquet(cust), pd.read_parquet(prod)
    # Cold start: generate a small in-memory dimension set (no disk/network).
    print("  No dimension files found — generating a small set in memory...")
    cfg = GenerationConfig(
        output_dir="", seed=SEED, n_customers=2_000, n_categories=12,
        n_products=200, n_orders=0, n_sessions=0, n_inventory_days=0,
        start_date="2025-01-01", end_date="2025-12-31", n_workers=1,
    )
    gen = WebshopDataGenerator(cfg)
    categories = gen.generate_categories()
    return gen.generate_customers(), gen.generate_products(categories)


def _producer_loop(q, stop, dim_dir: str, seed: int,
                   min_r: int, max_r: int, start_id: int) -> None:
    """Generate session batches in a SEPARATE process (its own CPU core).

    Batches are pushed onto a small bounded queue.  When the queue is full
    (the browser paused or closed), ``put`` blocks and this process idles —
    so closing the page still stops the whole stream.  Each item carries the
    raw ``sessions`` DataFrame plus the pure generation time; ``session_start``
    is re-stamped to "now" by the consumer just before the write.
    """
    try:
        rng = np.random.default_rng(seed)
        customers, products = _load_dimensions(Path(dim_dir))
        today = pd.Timestamp.now().normalize()
        cfg = GenerationConfig(
            output_dir="", seed=seed, n_orders=0, n_sessions=max_r,
            n_inventory_days=0, max_items_per_order=10, n_workers=1,
            start_date=str((today - pd.Timedelta(days=1)).date()),
            end_date=str(today.date()),
        )
        gen = WebshopDataGenerator(cfg)
        next_id = start_id
        while not stop.is_set():
            n = int(rng.integers(min_r, max_r + 1))
            id_from, id_to = next_id, next_id + n
            t0 = time.perf_counter()
            sessions, _pv = gen._generate_sessions_and_pageviews_range(
                customers=customers, products=products,
                start_id=id_from, stop_id=id_to, show_progress=False,
            )
            gen_ms = (time.perf_counter() - t0) * 1000.0
            next_id = id_to
            item = {"id_from": id_from, "id_to": id_to - 1, "n": n,
                    "gen_ms": gen_ms, "df": sessions}
            # Block (with periodic stop checks) until the consumer pulls.
            while not stop.is_set():
                try:
                    q.put(item, timeout=0.5)
                    break
                except _queue.Full:
                    continue
    except Exception as exc:  # surface fatal generator errors to the consumer
        try:
            q.put({"error": f"{type(exc).__name__}: {exc}"}, timeout=1.0)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Streamer — holds all state, produces one batch per tick
# ─────────────────────────────────────────────────────────────────────────────

class Streamer:
    def __init__(self) -> None:
        self._lock = threading.Lock()        # guards snapshot-shared counters
        self._tick_lock = threading.Lock()   # serializes writes (not the printer)
        self.rng = np.random.default_rng(SEED)

        dim_dir = Path(generated_data_dir)
        self.next_session_id = self._bootstrap_session_id(dim_dir)

        # Dimension row counts for the dashboard (footer only, no full read).
        self._n_customers = _row_count(dim_dir / "customers" / "customers.parquet")
        self._n_products = _row_count(dim_dir / "products" / "products.parquet")

        # ── Generator on its own CPU core, started BEFORE the writer ──────────
        # A separate *process* (not thread) sidesteps the GIL, so batch
        # generation runs truly in parallel with this process's SuperTable
        # write.  A tiny bounded queue provides backpressure: when it fills
        # (browser paused/closed) the generator idles.
        ctx = mp.get_context("spawn")
        self._stop = ctx.Event()
        self._queue = ctx.Queue(maxsize=2)
        self._proc = ctx.Process(
            target=_producer_loop,
            args=(self._queue, self._stop, str(dim_dir), SEED,
                  MIN_RECORDS, MAX_RECORDS, self.next_session_id),
            daemon=True,
        )
        self._proc.start()

        self.writer = DataWriter(super_name, organization)

        # Cumulative counters
        self.total_batches = 0
        self.total_rows = 0
        self.total_errors = 0

        # Rolling history + last tick, read by the per-second stats printer
        self._gen_hist: deque = deque(maxlen=200)
        self._write_hist: deque = deque(maxlen=200)
        self.last_tick: dict | None = None

        cust_s = f"{self._n_customers:,}" if self._n_customers is not None else "?"
        prod_s = f"{self._n_products:,}" if self._n_products is not None else "?"
        print(f"  dimensions : {cust_s} customers  {prod_s} products")
        print(f"  generator  : pid {self._proc.pid} (separate process / CPU core)")
        print(f"  first session_id : {self.next_session_id:,}")

    # -- startup helpers ------------------------------------------------------

    def _bootstrap_session_id(self, dim_dir: Path) -> int:
        """Continue session_ids after the *table's* live max, so restarts never
        collide.  Queries SuperTable directly (authoritative); only falls back to
        the local snapshot, then 1, when the table is empty / unreachable."""
        # 1) Live high-water mark from SuperTable (the real source of truth).
        try:
            reader = DataReader(
                organization=organization,
                super_name=super_name,
                query=f'SELECT MAX("session_id") FROM "{TABLE}"',
            )
            df, status, _ = reader.execute(role_name=role_name)
            if status != Status.ERROR and not df.empty:
                val = df.iloc[0, 0]
                if not pd.isna(val):
                    print(f"  bootstrap  : table MAX(session_id) = {int(val):,} "
                          f"(live from SuperTable)")
                    return int(val) + 1
        except Exception as exc:
            print(f"  bootstrap  : table read failed ({type(exc).__name__}: {exc}); "
                  f"falling back to local snapshot")

        # 2) Local snapshot fallback (first cold run / offline).
        path = dim_dir / "sessions" / "sessions.parquet"
        try:
            if path.exists():
                mx = pd.read_parquet(path, columns=["session_id"])["session_id"].max()
                if not pd.isna(mx):
                    print(f"  bootstrap  : snapshot MAX(session_id) = {int(mx):,} "
                          f"(local file fallback)")
                    return int(mx) + 1
        except Exception:
            pass
        print("  bootstrap  : no prior session_id found; starting at 1")
        return 1

    # -- the hot path ---------------------------------------------------------

    def tick(self) -> dict:
        with self._tick_lock:
            # ── Pull a ready-made batch (generated on the OTHER core) ────────
            # This is where the overlap pays off: while we were writing the
            # previous batch, the producer already built this one.
            try:
                item = self._queue.get(timeout=15.0)
            except _queue.Empty:
                return self._record_error("generator produced no batch within 15s")
            if "error" in item:
                return self._record_error(item["error"])

            sessions = item["df"]
            gen_ms = item["gen_ms"]
            start_id = item["id_from"]
            stop_id = item["id_to"] + 1
            n = item["n"]

            # Re-stamp session_start into a live window ending "now" (cheap).
            now = pd.Timestamp.now().floor("s")
            offsets = np.sort(
                self.rng.integers(0, LIVE_WINDOW_SECONDS + 1, size=len(sessions))
            )[::-1]
            sessions = sessions.copy()
            sessions["session_start"] = now - pd.to_timedelta(offsets, unit="s")
            payload = _to_arrow(sessions)

            # ── SuperTable update (network round-trip) — timed ALONE ─────────
            t1 = time.perf_counter()
            error = None
            rows = inserted = 0
            try:
                _, rows, inserted, _deleted = self.writer.write(
                    role_name=role_name,
                    simple_name=TABLE,
                    data=payload,
                    overwrite_columns=overwrite_columns,
                )
            except Exception as exc:  # keep the stream alive, surface the error
                error = f"{type(exc).__name__}: {exc}"
            write_ms = (time.perf_counter() - t1) * 1000.0

            with self._lock:
                self.next_session_id = stop_id
                self.total_batches += 1
                batch_no = self.total_batches
                if error is None:
                    self.total_rows += int(rows)
                    self._gen_hist.append(gen_ms)
                    self._write_hist.append(write_ms)
                else:
                    self.total_errors += 1

                result = {
                    "ok": error is None,
                    "batch": batch_no,
                    "records": n,
                    "rows": int(rows),
                    "inserted": int(inserted),
                    "gen_ms": round(gen_ms, 1),
                    "write_ms": round(write_ms, 1),
                    "id_from": start_id,
                    "id_to": stop_id - 1,
                    "total_batches": self.total_batches,
                    "total_rows": self.total_rows,
                    "total_errors": self.total_errors,
                    "error": error,
                    "clock": time.strftime("%H:%M:%S"),
                }
                self.last_tick = result
            return result

    def _record_error(self, msg: str) -> dict:
        """Build a tick result for an error with no batch (generator-side)."""
        with self._lock:
            self.total_batches += 1
            self.total_errors += 1
            result = {
                "ok": False, "batch": self.total_batches, "records": 0,
                "rows": 0, "inserted": 0, "gen_ms": None, "write_ms": None,
                "id_from": None, "id_to": None,
                "total_batches": self.total_batches,
                "total_rows": self.total_rows,
                "total_errors": self.total_errors,
                "error": msg, "clock": time.strftime("%H:%M:%S"),
            }
            self.last_tick = result
            return result

    def snapshot(self) -> dict:
        """Thread-safe view of current stats for the per-second printer."""
        with self._lock:
            g, w = list(self._gen_hist), list(self._write_hist)
            last = self.last_tick
            ok = bool(last and last.get("ok"))
            return {
                "total_batches": self.total_batches,
                "total_rows": self.total_rows,
                "total_errors": self.total_errors,
                "next_session_id": self.next_session_id,
                "last_gen_ms": last["gen_ms"] if ok else None,
                "last_write_ms": last["write_ms"] if ok else None,
                "avg_gen_ms": (sum(g) / len(g)) if g else None,
                "avg_write_ms": (sum(w) / len(w)) if w else None,
            }

    def info(self) -> dict:
        return {
            "table": TABLE,
            "organization": organization,
            "super_name": super_name,
            "min_records": MIN_RECORDS,
            "max_records": MAX_RECORDS,
            "next_session_id": self.next_session_id,
            "customers": self._n_customers,
            "products": self._n_products,
            "generator_pid": self._proc.pid,
        }

    def shutdown(self) -> None:
        """Signal the generator process to stop and reap it."""
        try:
            self._stop.set()
        except Exception:
            pass
        proc = getattr(self, "_proc", None)
        if proc is not None:
            proc.join(timeout=2.0)
            if proc.is_alive():
                proc.terminate()


# ─────────────────────────────────────────────────────────────────────────────
# HTTP handler
# ─────────────────────────────────────────────────────────────────────────────

STREAMER: Streamer | None = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:  # silence per-request stderr spam
        pass

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj: dict, code: int = 200) -> None:
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path == "/":
            self._send(200, PAGE_HTML.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/api/info":
            self._json(STREAMER.info())
        elif path == "/api/tick":
            try:
                self._json(STREAMER.tick())
            except Exception as exc:
                self._json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, 500)
        elif path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
        else:
            self._send(404, b"not found", "text/plain")


# ─────────────────────────────────────────────────────────────────────────────
# Embedded dashboard
# ─────────────────────────────────────────────────────────────────────────────

PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>SuperTable · Live Session Stream</title>
<style>
  :root{
    --bg:#0b0f17; --panel:#131a26; --panel2:#0f1520; --line:#233044;
    --txt:#e6edf6; --muted:#8296b0; --accent:#4f8cff; --gen:#43d17a;
    --write:#ffb84d; --err:#ff5d6c;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
       font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
  header{padding:18px 24px;border-bottom:1px solid var(--line);
         display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:17px;margin:0;font-weight:650;letter-spacing:.2px}
  .sub{color:var(--muted);font-size:12px}
  .pill{margin-left:auto;display:inline-flex;align-items:center;gap:8px;
        padding:6px 12px;border:1px solid var(--line);border-radius:999px;font-size:12px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--muted)}
  .dot.live{background:var(--gen);box-shadow:0 0 0 0 rgba(67,209,122,.6);
            animation:pulse 1.4s infinite}
  .dot.err{background:var(--err)}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(67,209,122,.55)}
                   70%{box-shadow:0 0 0 8px rgba(67,209,122,0)}
                   100%{box-shadow:0 0 0 0 rgba(67,209,122,0)}}
  main{padding:22px 24px;max-width:1100px;margin:0 auto}
  .controls{display:flex;gap:10px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
  button{background:var(--accent);color:#fff;border:0;border-radius:8px;
         padding:9px 16px;font-size:13px;font-weight:600;cursor:pointer}
  button.ghost{background:transparent;color:var(--txt);border:1px solid var(--line)}
  button:disabled{opacity:.5;cursor:not-allowed}
  label{color:var(--muted);font-size:12px;display:inline-flex;align-items:center;gap:6px}
  input[type=number]{width:78px;background:var(--panel2);border:1px solid var(--line);
         color:var(--txt);border-radius:6px;padding:6px 8px;font-size:13px}
  .cards{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:18px 20px}
  .card h2{margin:0 0 4px;font-size:12px;font-weight:600;text-transform:uppercase;
           letter-spacing:.8px;color:var(--muted);display:flex;align-items:center;gap:8px}
  .swatch{width:10px;height:10px;border-radius:3px;display:inline-block}
  .big{font-size:40px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}
  .unit{font-size:16px;color:var(--muted);font-weight:500;margin-left:4px}
  .meta{color:var(--muted);font-size:12px;margin-top:6px;font-variant-numeric:tabular-nums}
  canvas{width:100%;height:56px;margin-top:12px;display:block}
  .stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:22px}
  .stat{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
  .stat .k{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
  .stat .v{font-size:20px;font-weight:650;font-variant-numeric:tabular-nums;margin-top:3px}
  table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
  th,td{text-align:right;padding:8px 10px;border-bottom:1px solid var(--line);font-size:13px}
  th{color:var(--muted);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  th:first-child,td:first-child{text-align:left}
  tr.row-err td{color:var(--err)}
  .foot{color:var(--muted);font-size:12px;margin-top:18px}
  .tag{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;
       background:var(--panel2);border:1px solid var(--line);padding:2px 7px;border-radius:6px}
</style>
</head>
<body>
<header>
  <div>
    <h1>SuperTable · Live Session Stream</h1>
    <div class="sub" id="cfg">connecting…</div>
  </div>
  <div class="pill"><span class="dot" id="statusDot"></span><span id="statusText">idle</span></div>
</header>

<main>
  <div class="controls">
    <button id="toggle">Pause</button>
    <button class="ghost" id="clear">Clear log</button>
    <label>interval <input type="number" id="interval" min="200" step="100" value="1000"/> ms</label>
    <span class="sub">Close this page to stop the stream.</span>
  </div>

  <div class="cards">
    <div class="card">
      <h2><span class="swatch" style="background:var(--gen)"></span>Generation time</h2>
      <div><span class="big" id="genNow">–</span><span class="unit">ms</span></div>
      <div class="meta" id="genMeta">avg – · min – · max –</div>
      <canvas id="genChart" width="520" height="56"></canvas>
    </div>
    <div class="card">
      <h2><span class="swatch" style="background:var(--write)"></span>SuperTable update time</h2>
      <div><span class="big" id="writeNow">–</span><span class="unit">ms</span></div>
      <div class="meta" id="writeMeta">avg – · min – · max –</div>
      <canvas id="writeChart" width="520" height="56"></canvas>
    </div>
  </div>

  <div class="stats">
    <div class="stat"><div class="k">Batches</div><div class="v" id="sBatches">0</div></div>
    <div class="stat"><div class="k">Rows written</div><div class="v" id="sRows">0</div></div>
    <div class="stat"><div class="k">Records / batch</div><div class="v" id="sRec">–</div></div>
    <div class="stat"><div class="k">Next session_id</div><div class="v" id="sId">–</div></div>
    <div class="stat"><div class="k">Errors</div><div class="v" id="sErr">0</div></div>
  </div>

  <table>
    <thead><tr>
      <th>#</th><th>time</th><th>records</th>
      <th style="color:var(--gen)">gen ms</th>
      <th style="color:var(--write)">write ms</th>
      <th>session_id range</th><th>status</th>
    </tr></thead>
    <tbody id="log"></tbody>
  </table>

  <div class="foot">Table <span class="tag" id="tag">sessions</span> · append-only ·
     one write per tick. Generation runs in a <b>separate process</b> (its own CPU core)
     ahead of time; the update time is the SuperTable write round-trip only.</div>
</main>

<script>
const $ = id => document.getElementById(id);
let running = true, timer = null;
const genHist = [], writeHist = [], MAXH = 60;

function fmt(n){ return n==null ? '–' : Number(n).toLocaleString(); }
function stat(arr){
  if(!arr.length) return {avg:'–',min:'–',max:'–'};
  const s = arr.reduce((a,b)=>a+b,0);
  return {avg:(s/arr.length).toFixed(1), min:Math.min(...arr).toFixed(1), max:Math.max(...arr).toFixed(1)};
}
function drawChart(canvas, data, color){
  const c = canvas.getContext('2d'), W = canvas.width, H = canvas.height;
  c.clearRect(0,0,W,H);
  if(data.length < 2) return;
  const avg = data.reduce((a,b)=>a+b,0)/data.length;
  const max = Math.max(...data, avg, 1), n = data.length;
  const x = i => i*(W/(n-1)), y = v => H-4-(v/max)*(H-8);
  // area
  c.beginPath(); c.moveTo(0,H);
  data.forEach((v,i)=>c.lineTo(x(i),y(v)));
  c.lineTo(W,H); c.closePath();
  c.fillStyle = color+'22'; c.fill();
  // line
  c.beginPath(); data.forEach((v,i)=> i?c.lineTo(x(i),y(v)):c.moveTo(x(i),y(v)));
  c.strokeStyle = color; c.lineWidth = 2; c.stroke();
  // average line (dashed) + its value
  const ay = y(avg);
  c.save();
  c.setLineDash([5,4]); c.strokeStyle = '#e6edf6aa'; c.lineWidth = 1;
  c.beginPath(); c.moveTo(0,ay); c.lineTo(W,ay); c.stroke();
  c.setLineDash([]);
  const label = 'avg ' + avg.toFixed(0) + ' ms';
  c.font = '11px ui-monospace,Menlo,monospace';
  const tw = c.measureText(label).width;
  const tx = W - tw - 4, ty = ay > 14 ? ay - 4 : ay + 13;
  c.fillStyle = '#0b0f17cc'; c.fillRect(tx-3, ty-11, tw+6, 14);
  c.fillStyle = '#e6edf6'; c.fillText(label, tx, ty);
  c.restore();
}
function setStatus(kind){
  const dot = $('statusDot'), txt = $('statusText');
  dot.className = 'dot ' + (kind==='live'?'live':kind==='err'?'err':'');
  txt.textContent = kind==='live'?'streaming':kind==='err'?'error':'paused';
}
function push(arr,v){ arr.push(v); if(arr.length>MAXH) arr.shift(); }

function render(d){
  if(d.error){
    setStatus('err');
  } else {
    setStatus(running?'live':'idle');
    push(genHist, d.gen_ms); push(writeHist, d.write_ms);
  }
  if(!d.error){
    $('genNow').textContent = d.gen_ms.toFixed(1);
    $('writeNow').textContent = d.write_ms.toFixed(1);
    const g = stat(genHist), w = stat(writeHist);
    $('genMeta').textContent = `avg ${g.avg} · min ${g.min} · max ${g.max} ms`;
    $('writeMeta').textContent = `avg ${w.avg} · min ${w.min} · max ${w.max} ms`;
    drawChart($('genChart'), genHist, getComputedStyle(document.body).getPropertyValue('--gen').trim());
    drawChart($('writeChart'), writeHist, getComputedStyle(document.body).getPropertyValue('--write').trim());
  }
  $('sBatches').textContent = fmt(d.total_batches);
  $('sRows').textContent    = fmt(d.total_rows);
  $('sRec').textContent     = fmt(d.records);
  $('sId').textContent      = fmt(d.id_to==null?null:d.id_to+1);
  $('sErr').textContent     = fmt(d.total_errors);

  const tr = document.createElement('tr');
  if(d.error) tr.className = 'row-err';
  const range = d.error ? '—' : `${fmt(d.id_from)} – ${fmt(d.id_to)}`;
  const status = d.error ? (d.error.length>42?d.error.slice(0,42)+'…':d.error) : 'ok';
  tr.innerHTML = `<td>${fmt(d.batch)}</td><td>${d.clock||''}</td><td>${fmt(d.records)}</td>`+
    `<td style="color:var(--gen)">${d.error?'–':d.gen_ms.toFixed(1)}</td>`+
    `<td style="color:var(--write)">${d.error?'–':d.write_ms.toFixed(1)}</td>`+
    `<td>${range}</td><td>${status}</td>`;
  const log = $('log');
  log.insertBefore(tr, log.firstChild);
  while(log.children.length > 25) log.removeChild(log.lastChild);
}

async function tick(){
  if(!running) return;
  const started = performance.now();
  try{
    const r = await fetch('/api/tick');
    render(await r.json());
  }catch(e){
    setStatus('err');
    render({error:'network: '+e.message});
  }finally{
    if(running){
      // Aim for a fixed period measured from tick START, so cadence stays ~1/s
      // even as the write time varies (next fires immediately if we overran).
      const period = Math.max(200, +$('interval').value||1000);
      const elapsed = performance.now() - started;
      timer = setTimeout(tick, Math.max(0, period - elapsed));
    }
  }
}
function start(){ running = true; setStatus('live'); $('toggle').textContent='Pause'; tick(); }
function stop(){ running = false; setStatus('idle'); $('toggle').textContent='Resume';
  if(timer){ clearTimeout(timer); timer=null; } }

$('toggle').onclick = () => running ? stop() : start();
$('clear').onclick  = () => { $('log').innerHTML=''; genHist.length=0; writeHist.length=0; };

fetch('/api/info').then(r=>r.json()).then(info=>{
  const cust = info.customers==null ? '?' : info.customers.toLocaleString();
  const prod = info.products==null ? '?' : info.products.toLocaleString();
  $('cfg').textContent = `${info.organization} · ${info.super_name} · `+
    `${info.min_records}–${info.max_records} records/update · `+
    `${cust} customers · ${prod} products · gen pid ${info.generator_pid}`;
  $('tag').textContent = info.table;
  $('sId').textContent = info.next_session_id.toLocaleString();
}).catch(()=>{});

start();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    global STREAMER

    ap = argparse.ArgumentParser(description="SuperTable live session-stream demo")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--no-browser", action="store_true", help="don't auto-open the page")
    args = ap.parse_args()

    print("=" * 72)
    print("SuperTable · Live Session Stream Demo")
    print(f"  table={TABLE}  records/tick={MIN_RECORDS}-{MAX_RECORDS}  "
          f"org={organization}  super={super_name}")
    print("=" * 72)

    STREAMER = Streamer()

    url = f"http://{args.host}:{args.port}"
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"\n  ▶ Open {url}  (close the page to stop the stream)")
    print("  Ctrl-C to shut down the server.\n")

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    # Per-second stats printer (decoupled from the browser's tick cadence).
    stop_printer = threading.Event()

    def _print_stats() -> None:
        prev_rows, prev_t = 0, time.time()
        while not stop_printer.wait(1.0):
            snap = STREAMER.snapshot()
            now = time.time()
            dt = now - prev_t
            rate = (snap["total_rows"] - prev_rows) / dt if dt > 0 else 0.0
            prev_rows, prev_t = snap["total_rows"], now
            print(
                f"[{time.strftime('%H:%M:%S')}] "
                f"batches={snap['total_batches']:>6,}  "
                f"rows={snap['total_rows']:>10,} ({rate:>5,.0f}/s)  "
                f"err={snap['total_errors']}  |  "
                f"gen  last={_fmt_ms(snap['last_gen_ms'])} avg={_fmt_ms(snap['avg_gen_ms'])}  |  "
                f"write last={_fmt_ms(snap['last_write_ms'])} avg={_fmt_ms(snap['avg_write_ms'])}  |  "
                f"next_id={snap['next_session_id']:,}",
                flush=True,
            )

    threading.Thread(target=_print_stats, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
    finally:
        stop_printer.set()
        if STREAMER is not None:
            STREAMER.shutdown()
        server.shutdown()


if __name__ == "__main__":
    main()
