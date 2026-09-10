"""Analyse the telemetry captured by ``write_telemetry_audit.py``.

Answers, in order:
  1. Where does wall time actually go, per stage?
  2. Which stages grow as the table grows (the ones that will not scale)?
  3. What did compaction cost, and how much write amplification did it add?
  4. Are the tombstone and stats artifacts pulling their weight?

polars only.

    python scripts/write_telemetry_report.py /tmp/write_telemetry.parquet
"""
from __future__ import annotations

import sys

import polars as pl

pl.Config.set_tbl_rows(60)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(60)


def _stage_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith("t.")]


def _count_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith("c.")]


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def report_totals(df: pl.DataFrame) -> pl.DataFrame:
    """Total, mean and share of wall time per stage."""
    stages = _stage_cols(df)
    total_wall = df["wall_ms"].sum()

    long = (
        df.select(stages)
        .unpivot(variable_name="stage", value_name="ms")
        .group_by("stage")
        .agg(
            pl.col("ms").sum().alias("total_ms"),
            pl.col("ms").mean().alias("mean_ms"),
            pl.col("ms").max().alias("max_ms"),
            (pl.col("ms") > 0.05).sum().alias("writes_hit"),
        )
        .with_columns(
            (pl.col("total_ms") / total_wall * 100).alias("pct_of_wall"),
            pl.col("stage").str.strip_prefix("t."),
        )
        .sort("total_ms", descending=True)
    )
    print(f"total wall across all writes: {total_wall / 1000:.1f}s")
    print(long.filter(pl.col("total_ms") > 0).head(30))
    return long


def report_growth(df: pl.DataFrame, head_n: int = 10) -> None:
    """Compare the first N writes with the last N — the scaling question.

    A stage whose cost is flat scales; one that climbs with table size does
    not, and is where the next bottleneck will appear.
    """
    stages = _stage_cols(df)
    n = df.height
    first = df.head(head_n).select(stages).mean()
    last = df.tail(head_n).select(stages).mean()

    rows = []
    for stage in stages:
        f = float(first[stage][0] or 0.0)
        l = float(last[stage][0] or 0.0)
        if max(f, l) < 0.5:      # sub-0.5ms stages are noise
            continue
        rows.append({
            "stage": stage.removeprefix("t."),
            f"first_{head_n}_ms": f,
            f"last_{head_n}_ms": l,
            "delta_ms": l - f,
            "growth_x": (l / f) if f > 0.01 else float("inf"),
        })
    out = pl.DataFrame(rows).sort("delta_ms", descending=True)
    print(f"mean ms per write — first {head_n} vs last {head_n} of {n}")
    print(out)


def report_compaction(df: pl.DataFrame) -> None:
    """Isolate the writes that ran a compaction phase."""
    have = [c for c in ("t.compact_small", "t.compact_tombstones") if c in df.columns]
    if not have:
        print("no compaction stages recorded")
        return

    df = df.with_columns(
        pl.sum_horizontal([pl.col(c) for c in have]).alias("compact_ms")
    )
    hits = df.filter(pl.col("compact_ms") > 1.0)
    clean = df.filter(pl.col("compact_ms") <= 1.0)

    print(f"writes that compacted : {hits.height} / {df.height}")
    if hits.height:
        print(f"  compaction wall     : {hits['compact_ms'].sum() / 1000:.2f}s "
              f"({hits['compact_ms'].sum() / df['wall_ms'].sum() * 100:.1f}% of all write time)")
        print(f"  mean when it fires  : {hits['compact_ms'].mean():.1f}ms")
        print(f"  worst single        : {hits['compact_ms'].max():.1f}ms")
        print(f"  mean write WITH     : {hits['wall_ms'].mean():.1f}ms")
        print(f"  mean write WITHOUT  : {clean['wall_ms'].mean():.1f}ms")
        cols = ["write_idx", "wall_ms", "compact_ms", "new_resources", "sunset_files"]
        cols += [c for c in ("c.files_read", "c.bytes_read",
                             "c.files_written", "c.bytes_written") if c in df.columns]
        print("\n  per compaction event:")
        print(hits.select(cols))


def report_io(df: pl.DataFrame, final_bytes: int | None = None) -> None:
    """Write amplification: bytes pushed vs bytes that survived."""
    counts = _count_cols(df)
    totals = {c.removeprefix("c."): int(df[c].fill_null(0).sum())
              for c in counts if df[c].dtype.is_numeric()}
    interesting = [
        "files_written", "bytes_written", "rows_written",
        "files_read", "bytes_read", "rows_read",
        "compact_small_candidates", "stats_rows_total", "stats_rows_extracted",
        "tombstone_files_total", "tombstone_files_touched",
        "stats_cache_hit", "stats_cache_miss",
        "tombstone_cache_hit", "tombstone_cache_miss",
        "reclaimed_dead_files", "overwrite_resolve_fallback",
    ]
    rows = [{"counter": k, "total": totals[k]} for k in interesting if k in totals]
    print(pl.DataFrame(rows))

    bw = totals.get("bytes_written", 0)
    br = totals.get("bytes_read", 0)
    print(f"\n  bytes written total : {bw / 1048576:.1f} MiB")
    print(f"  bytes read total    : {br / 1048576:.1f} MiB")
    if final_bytes:
        print(f"  bytes live at end   : {final_bytes / 1048576:.1f} MiB")
        print(f"  write amplification : {bw / final_bytes:.2f}x")


def report_tombstone_stats(df: pl.DataFrame) -> None:
    """The two artifacts the audit specifically asked about."""
    for label, col in (("tombstone (build_tombstone)", "t.build_tombstone"),
                       ("stats parquet (build_stats)", "t.build_stats")):
        if col not in df.columns:
            continue
        s = df[col].fill_null(0.0)
        print(f"  {label:32s} total={s.sum() / 1000:7.2f}s  "
              f"mean={s.mean():6.2f}ms  max={s.max():7.2f}ms  "
              f"share={s.sum() / df['wall_ms'].sum() * 100:5.1f}%")


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/write_telemetry.parquet"
    final_bytes = int(sys.argv[2]) if len(sys.argv) > 2 else None
    df = pl.read_parquet(path).fill_null(0.0)

    section("1. WHERE THE TIME GOES")
    report_totals(df)

    section("2. DOES IT SCALE? (first 10 writes vs last 10)")
    report_growth(df)

    section("3. COMPACTION")
    report_compaction(df)

    section("4. TOMBSTONE + STATS ARTIFACTS")
    report_tombstone_stats(df)

    section("5. I/O AND WRITE AMPLIFICATION")
    report_io(df, final_bytes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
