from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import uuid

ORG = "sqlmatrix"
SUPER = "warehouse"


@contextmanager
def isolated_redis():
    name = "st-sql-matrix-" + uuid.uuid4().hex[:12]
    container = None
    if not shutil.which("docker"):
        raise RuntimeError("This matrix needs Docker for an isolated real Redis instance")
    try:
        container = subprocess.check_output([
            "docker", "run", "--detach", "--rm", "--name", name,
            "--label", "supertable.sql-read-matrix=true", "-p", "127.0.0.1::6379",
            "redis:7-alpine", "redis-server", "--save", "", "--appendonly", "no",
        ], text=True).strip()
        port_text = subprocess.check_output(["docker", "port", container, "6379/tcp"], text=True).strip()
        port = int(port_text.rsplit(":", 1)[1])
        import redis
        client = redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=1, socket_timeout=2)
        for _ in range(100):
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Isolated Redis did not become ready")
        image = subprocess.check_output(["docker", "inspect", "--format", "{{.Image}}", container], text=True).strip()
        yield port, {"backend": "real Redis", "version": client.info("server")["redis_version"], "image_id": image}
        client.close()
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, text=True)


def configure_process(workspace: Path, port: int, log_path: Path, session_timezone="UTC"):
    prefixes = ("SUPERTABLE_", "STORAGE_", "AZURE_", "GCS_", "GCP_", "AWS_")
    for key in list(os.environ):
        if key.startswith(prefixes) or key in {"GOOGLE_APPLICATION_CREDENTIALS", "MAX_MEMORY_CHUNK_SIZE",
                "MAX_OVERLAPPING_FILES", "MAX_TOMBSTONE_ROWS", "DEFAULT_TIMEOUT_SEC", "DEFAULT_LOCK_DURATION_SEC"}:
            os.environ.pop(key)
    workspace.mkdir(parents=True, exist_ok=True)
    os.chdir(workspace)
    os.environ.update({
        "SUPERTABLE_HOME": str(workspace), "STORAGE_TYPE": "LOCAL",
        "SUPERTABLE_REDIS_HOST": "127.0.0.1", "SUPERTABLE_REDIS_PORT": str(port),
        "SUPERTABLE_REDIS_DB": "0", "SUPERTABLE_REDIS_SENTINEL": "false",
        "SUPERTABLE_REDIS_SSL": "false", "SUPERTABLE_MONITORING_ENABLED": "false",
        "SUPERTABLE_AUDIT_ENABLED": "false", "SUPERTABLE_LOG_LEVEL": "CRITICAL",
        "SUPERTABLE_DUCKDB_MEMORY_LIMIT": "512MB", "SUPERTABLE_DUCKDB_THREADS": "2",
        "SUPERTABLE_READ_PRUNING_ENABLED": "true", "SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD": "false",
        "SUPERTABLE_STREAM_BATCH_ROWS": "17", "TZ": session_timezone,
    })
    if hasattr(time, "tzset"):
        time.tzset()
    import supertable
    logging.basicConfig(filename=str(log_path), level=logging.WARNING, force=True)
    return supertable.__version__


def prepare_tables(data, schemas):
    import pyarrow as pa
    from supertable import DataWriter, RoleManager, RedisCatalog
    writer = DataWriter(SUPER, ORG)
    def write_table(table_name, rows):
        writer.configure_table("superadmin", table_name, max_memory_chunk_size=64 * 1024 * 1024,
                               max_overlapping_files=10000, max_tombstone_rows=1000000)
        if table_name == "ledger":
            original = [{"lid": i, "value": i * 10, "revision": 1} for i in range(1, 25)]
            for start in range(0, len(original), 8):
                writer.write("superadmin", table_name, pa.Table.from_pylist(original[start:start + 8], schema=schemas[table_name]), [])
            replacements = [r for r in rows if r["lid"] in (3, 9, 18)]
            writer.write("superadmin", table_name, pa.Table.from_pylist(replacements, schema=schemas[table_name]), ["lid"], newer_than="revision")
            writer.write("superadmin", table_name, pa.table({"lid": [5, 11]}), ["lid"], delete_only=True)
            writer.write("superadmin", table_name, pa.Table.from_pylist([r for r in rows if r["lid"] in (25, 26)], schema=schemas[table_name]), [])
        elif table_name == "evolving":
            first = pa.Table.from_pylist(rows[:3], schema=schemas[table_name]).drop(["extra"])
            writer.write("superadmin", table_name, first, [])
            writer.write("superadmin", table_name, pa.Table.from_pylist(rows[3:], schema=schemas[table_name]), [])
        elif not rows:
            writer.write("superadmin", table_name, pa.Table.from_pylist([], schema=schemas[table_name]), [])
        else:
            size = (30 if table_name == "orders" else 60 if table_name == "items" else
                    12 if table_name.startswith(("temporal_", "tz_")) else max(1, len(rows) // 3))
            for start in range(0, len(rows), size):
                writer.write("superadmin", table_name, pa.Table.from_pylist(rows[start:start + size], schema=schemas[table_name]), [])
    setup_checks = []
    for table_name, rows in data.items():
        try:
            write_table(table_name, rows)
            setup_checks.append({"case_id": "ingest_" + table_name, "status": "pass", "method": "DataWriter"})
        except Exception as exc:
            import traceback
            setup_checks.append({"case_id": "ingest_" + table_name, "status": "fail",
                "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc(),
                "fallback": "Explicit PyArrow Parquet fixture + native snapshot/catalog publication"})
            materialize_arrow_fixture(writer, table_name, rows, schemas[table_name])
    roles = RoleManager(SUPER, ORG, actor_role_name="superadmin")
    roles.create_role({"role_name": "eu_reader", "role": "reader", "tables": {"orders": {
        "columns": ["amount", "cid", "oid", "qty", "region", "status"],
        "filters": {"region": {"operation": "=", "type": "value", "value": "eu"}}}}})
    roles.create_role({"role_name": "amount_reader", "role": "reader", "tables": {"orders": {
        "columns": ["oid", "amount"], "filters": {"amount": {"operation": ">=", "type": "value", "value": 200}}}}})
    roles.create_role({"role_name": "denied_reader", "role": "reader", "tables": {"customers": {"columns": ["*"], "filters": ["*"]}}})
    disabled_id = roles.create_role({"role_name": "disabled_reader", "role": "reader", "tables": {"orders": {"columns": ["*"], "filters": ["*"]}}})
    catalog = RedisCatalog()
    catalog.rbac_update_role(ORG, SUPER, disabled_id, {"enabled": False})
    return writer, catalog, setup_checks


def materialize_arrow_fixture(writer, name, rows, schema):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import polars as pl
    from supertable import SimpleTable
    from supertable.processing import extract_stats_rows, build_stats_file
    table = SimpleTable(writer.super_table, name)
    catalog = writer.catalog
    start_id = catalog.reserve_rowids(ORG, SUPER, name, len(rows))
    frame = pa.Table.from_pylist(rows, schema=schema)
    frame = frame.append_column("__rowid__", pa.array(range(start_id, start_id + len(rows)), type=pa.int64()))
    frame = frame.append_column("__timestamp__", pa.array([datetime(2024, 1, 1)] * len(rows), type=pa.timestamp("us")))
    path = Path(table.data_dir) / "independent_arrow_fixture.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(frame, path, row_group_size=8, write_statistics=True)
    resources = [{"file": str(path), "rows": len(rows), "columns": frame.num_columns, "file_size": path.stat().st_size}]
    old, old_path = table.get_simple_table_snapshot()
    stats_rows = extract_stats_rows([str(path)])
    stats_path, combined = build_stats_file(str(Path(table.simple_dir) / "stats"), None, stats_rows, set(), 1)
    old.update(tombstone=None, tombstone_rows=0, stats_file=stats_path,
               stats_rows=0 if combined is None else combined.height, rowid_high_watermark=start_id + len(rows) - 1)
    payload, snapshot_path = table.update(resources, {r["file"] for r in old.get("resources", [])},
        pl.from_arrow(frame), last_snapshot=old, last_snapshot_path=old_path,
        lineage={"source_type": "sql_matrix_fixture_fallback"})
    catalog.set_leaf_payload_cas(ORG, SUPER, name, payload, snapshot_path)
    catalog.bump_root(ORG, SUPER)


def reference_connections(data, schemas, session_timezone="UTC"):
    import duckdb
    import pyarrow as pa
    connections = {}
    for role in ("superadmin", "eu_reader", "amount_reader"):
        con = duckdb.connect(config={"threads": "2"})
        con.execute("SET default_collation='nocase'")
        con.execute("SET TimeZone = ?", [session_timezone])
        con.execute(f'CREATE SCHEMA "{SUPER}"')
        for name, rows in data.items():
            arrow = pa.Table.from_pylist(rows, schema=schemas[name])
            if role != "superadmin":
                if name != "orders":
                    continue
                if role == "eu_reader":
                    allowed = ["amount", "cid", "oid", "qty", "region", "status"]
                    rows = [r for r in rows if r["region"] == "eu"]
                else:
                    allowed = ["oid", "amount"]
                    rows = [r for r in rows if r["amount"] is not None and r["amount"] >= 200]
                arrow = pa.Table.from_pylist(rows, schema=schemas[name]).select(allowed)
            con.register("_input_" + name, arrow)
            con.execute(f'CREATE VIEW "{name}" AS SELECT * FROM "_input_{name}"')
            con.execute(f'CREATE VIEW "{SUPER}"."{name}" AS SELECT * FROM "_input_{name}"')
        connections[role] = con
    return connections
