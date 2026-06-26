"""Adapter that executes the **real** SuperTable read path over a sealed fixture.

``read_current_table`` is the single oracle entry point used by the golden
suite.  It does not reimplement any read logic: it injects the frozen catalog
state into the (patched, fake) Redis using the genuine ``RedisCatalog`` API and
then runs the production ``query_sql`` pipeline
(``DataReader -> DataEstimator -> Executor -> DuckDBLite``), including the real
tombstone + dedup view SQL.  The only thing the adapter "owns" is assembling
inputs and shaping the output into a :class:`TableResult`.

All ``supertable`` imports are lazy so this module can be imported before the
hermetic environment is pinned (the production settings module reads ``.env`` at
import time).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from tests.characterization.harness import FIXED_NOW_MS, install_fake_redis
from tests.characterization.table_result import TableResult


def _engine(engine: str):
    from supertable.engine.engine_enum import Engine

    e = (engine or "duckdb").lower()
    if e in ("duckdb", "duckdb_lite", "lite"):
        return Engine.DUCKDB_LITE
    if e in ("duckdb_pro", "pro"):
        return Engine.DUCKDB_PRO
    if e in ("spark", "spark_sql"):
        return Engine.SPARK_SQL
    raise ValueError(f"unknown engine {engine!r}")


def load_catalog(catalog_path: str | Path) -> dict:
    with open(catalog_path, "rb") as fh:
        return json.load(fh)


def inject_catalog(catalog: dict, catalog_dir: Path) -> None:
    """Bootstrap the supertable + write every table's config + leaf payload into
    the currently-patched (fake) Redis, exactly as the writer would shape it."""
    from supertable.super_table import SuperTable
    from supertable.redis_catalog import RedisCatalog

    org = catalog["organization"]
    sup = catalog["super_name"]

    # Bootstraps meta:root + RBAC (auto superadmin role) — superadmin bypasses
    # RBAC restriction so reads see all rows/columns.
    SuperTable(sup, org)

    cat = RedisCatalog()
    for simple_name, t in catalog["tables"].items():
        cat.set_table_config(
            org, sup, simple_name,
            {"primary_keys": t["primary_keys"], "dedup_on_read": t["dedup_on_read"]},
        )

        resources = []
        for r in t["resources"]:
            abs_file = str((catalog_dir / r["file"]).resolve())
            entry = {"file": abs_file, "rows": r.get("rows"), "columns": r.get("columns")}
            try:
                entry["file_size"] = Path(abs_file).stat().st_size
            except OSError:
                entry["file_size"] = 0
            resources.append(entry)

        payload = {
            "last_updated_ms": FIXED_NOW_MS,
            "previous_snapshot": None,
            "resources": resources,
            "tombstones": {
                "total_tombstones": len(t["tombstones"]["deleted_keys"]),
                "deleted_keys": [list(k) for k in t["tombstones"]["deleted_keys"]],
                "primary_keys": t["tombstones"]["primary_keys"],
            },
            "location": f"{org}/{sup}/tables/{simple_name}",
            "simple_name": simple_name,
            "schema": t.get("schema", {}),
            "snapshot_version": 1,
        }
        cat.set_leaf_payload_cas(
            org, sup, simple_name, payload,
            path=f"{org}/{sup}/tables/{simple_name}/snapshots/sealed.json",
            now_ms=FIXED_NOW_MS,
        )
    cat.bump_root(org, sup)


def build_sql(
    table_name: str,
    projection: Optional[List[str]],
    filters: Optional[List[str]],
    sql: Optional[str],
) -> str:
    if sql is not None:
        return sql
    proj = ", ".join(projection) if projection else "*"
    out = f"SELECT {proj} FROM {table_name}"
    if filters:
        out += " WHERE " + " AND ".join(f"({f})" for f in filters)
    return out


def register_spark_cluster(catalog: dict, cluster: dict) -> None:
    """Register a Spark Thrift cluster into the (already-injected) fake Redis.

    Called *after* ``inject_catalog`` because the read path installs a fresh fake
    Redis per call (which would otherwise wipe a pre-registered cluster).  The
    cross-engine suite supplies the cluster from environment so a real Thrift
    fleet can be targeted in integration CI; absent a fleet the suite skips.
    """
    from supertable.redis_catalog import RedisCatalog

    cfg = dict(cluster)
    cluster_id = cfg.pop("cluster_id", "characterization-spark")
    cfg.setdefault("status", "active")
    cfg.setdefault("min_bytes", 0)
    cfg.setdefault("max_bytes", 0)
    RedisCatalog().register_spark_cluster(catalog["organization"], cluster_id, cfg)


def read_current_table(
    catalog_path: str | Path,
    table_name: str,
    projection: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    engine: str = "duckdb",
    *,
    sql: Optional[str] = None,
    limit: int = 10_000,
    fresh_redis: bool = True,
    spark_cluster: Optional[Dict[str, Any]] = None,
) -> TableResult:
    """Execute the production read path against a sealed catalog fixture.

    Returns the read as a :class:`TableResult`.  Propagates any exception raised
    by the production pipeline (error scenarios assert on it).  When
    ``spark_cluster`` is provided it is registered after catalog injection so the
    Spark engine can resolve a fleet (used only by the cross-engine suite).
    """
    catalog_path = Path(catalog_path)
    catalog_dir = catalog_path.parent
    catalog = load_catalog(catalog_path)

    if fresh_redis:
        # A clean catalog per call so repeated reads never see stale state.
        install_fake_redis()

    inject_catalog(catalog, catalog_dir)

    if spark_cluster is not None:
        register_spark_cluster(catalog, spark_cluster)

    from supertable.data_reader import query_sql

    statement = build_sql(table_name, projection, filters, sql)
    columns, rows, columns_meta = query_sql(
        organization=catalog["organization"],
        super_name=catalog["super_name"],
        sql=statement,
        limit=limit,
        engine=_engine(engine),
        role_name="superadmin",
    )
    return TableResult.from_columns_rows(columns, rows)


def read_scenario(
    scenario,
    golden_root: str | Path,
    engine: str = "duckdb",
    *,
    spark_cluster: Optional[Dict[str, Any]] = None,
) -> TableResult:
    """Convenience: read a :class:`Scenario` from its sealed input directory."""
    from tests.characterization.fixtures_lib import Scenario  # noqa: F401

    catalog_path = Path(golden_root) / scenario.scenario_id / "input" / "catalog.json"
    return read_current_table(
        catalog_path,
        scenario.target_table(),
        projection=scenario.projection,
        filters=scenario.filters,
        engine=engine,
        sql=scenario.sql,
        limit=scenario.limit,
        spark_cluster=spark_cluster,
    )
