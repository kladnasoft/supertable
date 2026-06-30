"""End-to-end: the precise "compaction during write" log line.

When an inline auto-compaction is triggered DURING a write (the small-file gate
trips, or the deletion-vector grows past its threshold), the write path now
emits a single dedicated log line that states *why* it ran, *what each phase
touched*, and *how much I/O it cost*.  This is the observability the user asked
for — previously a compaction folded into a write was only visible as inflated
global ``files_written`` / ``bytes_written`` counters with no phase attribution.

This drives the real ``DataWriter`` against the per-test hermetic fake Redis
(see ``tests/conftest.py``) so the whole write→gate→compact→log path executes,
then asserts the log content.  A small ``max_overlapping_files`` makes the gate
open deterministically on the second write.
"""
from __future__ import annotations

import logging
import re
import uuid

import polars as pl
import pytest

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo"
ROLE = "superadmin"
LOGGER = "supertable.config.defaults"  # where DataWriter's ``logger`` lives


def _arrow(cols: dict):
    return pl.DataFrame(cols).to_arrow()


def _compaction_lines(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if "compaction during write" in r.getMessage()
    ]


def test_small_file_gate_emits_precise_compaction_log(caplog):
    """Two tiny appends with ``max_overlapping_files=2`` → the 2nd write merges
    them inline and logs exactly what happened."""
    simple = f"compact_log_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)  # bootstrap super + default superadmin role
    dw = DataWriter(super_name=SUPER, organization=ORG)
    # Open the small-file gate after just two accumulated files.
    dw._table_config_cache[simple] = {"max_overlapping_files": 2}

    # Write #1 — one small file; gate closed (1 < 2), no compaction.
    dw.write(
        role_name=ROLE, simple_name=simple,
        data=_arrow({"id": [1, 2, 3], "v": ["a", "b", "c"]}),
        overwrite_columns=[],
    )

    # Write #2 — second small file; gate opens (2 >= 2) → Phase B merges inline.
    with caplog.at_level(logging.INFO, logger=LOGGER):
        dw.write(
            role_name=ROLE, simple_name=simple,
            data=_arrow({"id": [4, 5, 6], "v": ["d", "e", "f"]}),
            overwrite_columns=[],
        )

    lines = _compaction_lines(caplog)
    assert len(lines) == 1, f"expected exactly one compaction line, got: {lines}"
    line = lines[0]

    # Trigger reason is named.
    assert "trigger=small_file_gate" in line
    # Pure append: no deletion-vector, so the tombstone phase did nothing.
    assert "tombstone phase removed 0 row(s) from 0/0 deletion-vector file(s)" in line
    # The two small files were merged into one.
    assert "small-file phase merged 2 small file(s) -> 1 file(s)" in line
    # Targeting + I/O attribution: only the two small files were read.
    assert "live files 2 -> 1" in line
    assert re.search(r"compaction io: read 2 file\(s\)/[\d.]+ MiB, wrote 1 file\(s\)", line), line


def test_normal_write_without_compaction_is_quiet(caplog):
    """A single small write must NOT emit the compaction line (gate closed)."""
    simple = f"quiet_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw._table_config_cache[simple] = {"max_overlapping_files": 100}

    with caplog.at_level(logging.INFO, logger=LOGGER):
        dw.write(
            role_name=ROLE, simple_name=simple,
            data=_arrow({"id": [1, 2, 3], "v": ["a", "b", "c"]}),
            overwrite_columns=[],
        )

    assert _compaction_lines(caplog) == []


def test_logged_compaction_preserves_all_rows(caplog):
    """The compaction the log describes must not lose data: every appended row
    is still readable afterwards."""
    simple = f"compact_safe_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw._table_config_cache[simple] = {"max_overlapping_files": 2}

    dw.write(role_name=ROLE, simple_name=simple,
             data=_arrow({"id": [1, 2, 3], "v": ["a", "b", "c"]}),
             overwrite_columns=[])
    with caplog.at_level(logging.INFO, logger=LOGGER):
        dw.write(role_name=ROLE, simple_name=simple,
                 data=_arrow({"id": [4, 5, 6], "v": ["d", "e", "f"]}),
                 overwrite_columns=[])

    assert len(_compaction_lines(caplog)) == 1  # compaction really did run

    dr = DataReader(
        super_name=SUPER, organization=ORG, query=f"SELECT id, v FROM {simple}"
    )
    df, status, _ = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"read failed: {status}"
    out = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)
    assert sorted(out["id"].to_list()) == [1, 2, 3, 4, 5, 6]
