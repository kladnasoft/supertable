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
from unittest.mock import patch

import polars as pl
import pytest

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
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

    # Write #1 — one small file; gate closed (1 < 2), no compaction.
    dw.write(
        role_name=ROLE, simple_name=simple,
        data=_arrow({"id": [1, 2, 3], "v": ["a", "b", "c"]}),
        overwrite_columns=[],
    )
    # Persist the threshold through the production configuration path.  The
    # process-local cache is observational and writes intentionally refresh
    # from Redis so another process's acknowledged changes cannot be missed.
    dw.configure_table(
        role_name=ROLE,
        simple_name=simple,
        max_overlapping_files=2,
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

    dw.write(role_name=ROLE, simple_name=simple,
             data=_arrow({"id": [1, 2, 3], "v": ["a", "b", "c"]}),
             overwrite_columns=[])
    dw.configure_table(
        role_name=ROLE,
        simple_name=simple,
        max_overlapping_files=2,
    )
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


def test_inline_compaction_keeps_latest_upload_schema_authoritative():
    """A physical union may retain legacy columns without republishing them.

    Auto-compaction is an implementation detail of the second write.  It must
    not replace the documented last-write-wins logical schema with the wider
    physical union of old and current Parquet files.
    """
    simple = f"compact_schema_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [1], "legacy_only": ["old"]}),
        overwrite_columns=[],
    )
    dw.configure_table(
        role_name=ROLE,
        simple_name=simple,
        max_overlapping_files=2,
    )
    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [2], "current_value": [42]}),
        overwrite_columns=[],
    )

    st = SimpleTable(SuperTable(SUPER, ORG), simple)
    snapshot, _ = st.get_simple_table_snapshot()
    schema_raw = snapshot.get("schema") or {}
    schema_names = (
        list(schema_raw)
        if isinstance(schema_raw, dict)
        else [field["name"] for field in schema_raw]
    )
    assert set(schema_names) == {
        "id", "current_value", "__timestamp__", "__rowid__",
    }
    assert "legacy_only" not in schema_names

    # Physical value preservation remains independent of the public schema.
    physical = st.storage.read_parquet(snapshot["resources"][0]["file"])
    physical = physical if isinstance(physical, pl.DataFrame) else pl.from_arrow(physical)
    assert "legacy_only" in physical.columns
    assert "current_value" in physical.columns


def test_explicit_compaction_preserves_latest_schema_byte_for_byte():
    """Maintenance cannot redefine the logical schema from physical union."""
    simple = f"explicit_schema_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [1], "legacy_only": ["old"]}),
        overwrite_columns=[],
    )
    dw.configure_table(
        role_name=ROLE,
        simple_name=simple,
        max_overlapping_files=100,
    )
    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [2], "current_value": [42]}),
        overwrite_columns=[],
    )

    st = SimpleTable(SuperTable(SUPER, ORG), simple)
    before, _ = st.get_simple_table_snapshot()
    schema_before = before.get("schema")
    schema_string_before = before.get("schemaString")

    dw.compact(ROLE, simple)

    after, _ = st.get_simple_table_snapshot()
    assert after.get("schema") == schema_before
    assert after.get("schemaString") == schema_string_before
    assert len(after.get("resources") or []) == 1
    physical = st.storage.read_parquet(after["resources"][0]["file"])
    physical = physical if isinstance(physical, pl.DataFrame) else pl.from_arrow(physical)
    assert {"legacy_only", "current_value"}.issubset(physical.columns)


def test_live_vector_and_small_file_gate_use_one_fused_source_pass(caplog):
    """A live DV plus the small-file gate must not run the old two phases.

    The initial file contains one deleted row and one survivor.  The following
    append opens the two-file gate.  The write must call ``compact_resources``
    once with the vector, never call ``compact_tombstones`` to mint an
    intermediate survivor, and publish the exact live rows/stats.
    """
    simple = f"fused_write_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [1, 2], "v": ["dead", "keep"]}),
        overwrite_columns=["id"],
    )
    dw.configure_table(
        role_name=ROLE,
        simple_name=simple,
        max_overlapping_files=2,
        max_tombstone_rows=100,
    )
    dw.write(
        role_name=ROLE,
        simple_name=simple,
        data=_arrow({"id": [1], "v": ["unused"]}),
        overwrite_columns=["id"],
        delete_only=True,
    )

    import supertable.data_writer as writer_module

    with (
        patch.object(
            writer_module,
            "compact_resources",
            wraps=writer_module.compact_resources,
        ) as fused,
        patch.object(
            writer_module,
            "compact_tombstones",
            wraps=writer_module.compact_tombstones,
        ) as legacy_tombstone,
        caplog.at_level(logging.INFO, logger=LOGGER),
    ):
        dw.write(
            role_name=ROLE,
            simple_name=simple,
            data=_arrow({"id": [3], "v": ["new"]}),
            overwrite_columns=["id"],
        )

    assert legacy_tombstone.call_count == 0
    fused_calls = [
        call for call in fused.call_args_list
        if call.kwargs.get("tombstone_df") is not None
    ]
    assert len(fused_calls) == 1
    assert fused_calls[0].kwargs["return_residual"] is True

    st = SimpleTable(SuperTable(SUPER, ORG), simple)
    snap, _ = st.get_simple_table_snapshot()
    assert snap.get("tombstone") is None
    assert int(snap.get("tombstone_rows") or 0) == 0
    live_files = {r["file"] for r in snap.get("resources") or []}
    stats = st.storage.read_parquet(snap["stats_file"])
    stats = stats if isinstance(stats, pl.DataFrame) else pl.from_arrow(stats)
    assert set(stats.get_column("file_path").to_list()) == live_files

    dr = DataReader(
        super_name=SUPER,
        organization=ORG,
        query=f"SELECT id, v FROM {simple}",
    )
    df, status, _ = dr.execute(
        role_name=ROLE, with_scan=False, engine=engine.AUTO,
    )
    assert str(status).endswith("OK"), status
    out = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)
    assert sorted(out.rows()) == [(2, "keep"), (3, "new")]
