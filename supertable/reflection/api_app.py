from __future__ import annotations

import logging
from typing import Optional, Any, Dict, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel

# Guard dependency (authN/authZ) from sibling module
try:
    from .admin_app import admin_guard_api  # type: ignore
except Exception:
    # Fallback no-op guard for isolated usage/tests
    def admin_guard_api():
        return None

# Prefer installed package; fallback to local modules for dev
try:
    from supertable.meta_reader import MetaReader, list_supers, list_tables  # type: ignore
except Exception:
    from meta_reader import MetaReader, list_supers, list_tables  # type: ignore

try:
    from supertable.data_reader import DataReader, engine  # type: ignore
except Exception:
    from data_reader import DataReader, engine  # type: ignore


router = APIRouter(prefix="", tags=["API"])
logger = logging.getLogger(__name__)


class ExecuteRequest(BaseModel):
    query: str
    organization: str
    super_name: str
    user_hash: str
    engine: Optional[str] = "DUCKDB"
    with_scan: bool = False
    preview_rows: int = 10


def _engine_from_str(s: Optional[str]) -> Any:
    """Map string to engine enum, defaults to DUCKDB. Case-insensitive."""
    if not s:
        return getattr(engine, "DUCKDB", s)
    key = str(s).strip().upper()
    if hasattr(engine, key):
        return getattr(engine, key)
    # Known aliases
    aliases = {
        "DUCKDB": "DUCKDB",
        "DUCK": "DUCKDB",
        "DUCK_DB": "DUCKDB",
        "SPARK": "SPARK",
        "POLARS": "POLARS",
    }
    mapped = aliases.get(key, "DUCKDB")
    return getattr(engine, mapped, getattr(engine, "DUCKDB", s))


@router.post("/execute")
def api_execute_sql(
    payload: ExecuteRequest = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Execute SQL against a SuperTable using DataReader, returning a small preview + meta."""
    try:
        dr = DataReader(
            super_name=payload.super_name,
            organization=payload.organization,
            query=payload.query,
        )
        eng = _engine_from_str(payload.engine)

        # DataReader is expected to return (df, meta1, meta2).
        res = dr.execute(
            user_hash=payload.user_hash,
            with_scan=payload.with_scan,
            engine=eng,
        )

        # Defensive unpacking
        df = meta1 = meta2 = None
        if isinstance(res, tuple):
            if len(res) >= 1:
                df = res[0]
            if len(res) >= 2:
                meta1 = res[1]
            if len(res) >= 3:
                meta2 = res[2]
        else:
            # Some implementations may return only a DataFrame
            df = res

        # Build preview rows (list of lists) without assuming pandas presence
        rows_preview = []
        if df is not None:
            try:
                # pandas-like
                it = df.head(payload.preview_rows).itertuples(index=False)  # type: ignore[attr-defined]
                for row in it:
                    rows_preview.append(list(row))
            except Exception:
                try:
                    # duckdb relation or list of dicts
                    if hasattr(df, "fetchmany"):
                        rows_preview = df.fetchmany(payload.preview_rows)  # type: ignore[assignment]
                    elif isinstance(df, list):
                        rows_preview = df[: payload.preview_rows]
                except Exception:
                    rows_preview = []

        shape = getattr(
            df,
            "shape",
            (len(rows_preview), len(rows_preview[0]) if rows_preview else 0),
        )
        columns = (
            list(getattr(df, "columns", []))
            if hasattr(df, "columns")
            else []
        )

        return {
            "ok": True,
            "engine": str(eng),
            "with_scan": payload.with_scan,
            "shape": list(shape),
            "columns": columns,
            "rows_preview_count": len(rows_preview),
            "rows_preview": rows_preview,
            "meta": {
                "result_1": meta1,
                "result_2": meta2,
                "timings": getattr(getattr(dr, "timer", None), "timings", None),
                "plan_stats": getattr(
                    getattr(dr, "plan_stats", None),
                    "stats",
                    None,
                ),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {e}")


# ---------- META ENDPOINTS ----------


@router.get("/meta/supers")
def api_list_supers(
    organization: str = Query(..., description="Organization identifier"),
    _: Any = Depends(admin_guard_api),
):
    try:
        return {
            "ok": True,
            "organization": organization,
            "supers": list_supers(organization=organization),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List supers failed: {e}")


@router.get("/meta/tables")
def api_list_tables(
    organization: str = Query(...),
    super_name: str = Query(...),
    _: Any = Depends(admin_guard_api),
):
    try:
        return {
            "ok": True,
            "organization": organization,
            "super_name": super_name,
            "tables": list_tables(
                organization=organization,
                super_name=super_name,
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List tables failed: {e}")


@router.get("/meta/super")
def api_get_super_meta(
    organization: str = Query(...),
    super_name: str = Query(...),
    user_hash: str = Query(...),
    _: Any = Depends(admin_guard_api),
):
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        return {"ok": True, "meta": mr.get_super_meta(user_hash)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get super meta failed: {e}")


@router.get("/meta/schema")
def api_get_table_schema(
    organization: str = Query(...),
    super_name: str = Query(...),
    table: str = Query(..., description="Table simple name"),
    user_hash: str = Query(...),
    _: Any = Depends(admin_guard_api),
):
    """Correct usage: pass the table (simple) name — NOT the super_name."""
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        schema = mr.get_table_schema(table, user_hash)
        logger.info(f"table.schema.result: {schema}")
        return {"ok": True, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table schema failed: {e}")


@router.get("/meta/stats")
def api_get_table_stats(
    organization: str = Query(...),
    super_name: str = Query(...),
    table: str = Query(..., description="Table simple name"),
    user_hash: str = Query(...),
    _: Any = Depends(admin_guard_api),
):
    """Correct usage: pass the table (simple) name — NOT the super_name."""
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        stats = mr.get_table_stats(table, user_hash)
        logger.info(f"table.stats.result: {stats}")
        return {"ok": True, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table stats failed: {e}")


# ---------- LEAF ENDPOINT WITH MODE HANDLING ----------


@router.get("/leaf/{simple_name}")
def api_leaf(
    simple_name: str,
    organization: str = Query(..., alias="org"),
    super_name: str = Query(..., alias="sup"),
    mode: str = Query("meta"),
    user_hash: Optional[str] = Query(
        None,
        description="User hash; optional, defaults to 'ui' if not provided",
    ),
    _: Any = Depends(admin_guard_api),
):
    """
    Leaf endpoint with mode-based behavior.

      - mode=schema -> MetaReader.get_table_schema(simple_name, user_hash)
                       returns the *schema*, e.g.
                       [{'client': 'String', ...}, ...]

      - mode=stats  -> MetaReader.get_table_stats(simple_name, user_hash)

      - otherwise   -> minimal/meta placeholder
    """
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        effective_user_hash = user_hash

        # --- SCHEMA MODE ---
        if mode.lower() == "schema":
            result = mr.get_table_schema(simple_name, effective_user_hash)
            logger.info(f"simple_name.schema.result: {result}")
            return {
                "ok": True,
                "mode": "schema",
                "org": organization,
                "sup": super_name,
                "simple": simple_name,
                # expected shape: [{'client': 'String', ...}, ...]
                "schema": result,
            }

        # --- STATS MODE ---
        if mode.lower() == "stats":
            result = mr.get_table_stats(simple_name, effective_user_hash)
            logger.info(f"simple_name.stats.result: {result}")
            return {
                "ok": True,
                "mode": "stats",
                "org": organization,
                "sup": super_name,
                "simple": simple_name,
                "stats": result,
            }

        # Fallback for other modes
        return {
            "ok": True,
            "mode": mode,
            "org": organization,
            "sup": super_name,
            "simple": simple_name,
            "message": "Use mode=schema or mode=stats for detailed info.",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Leaf handler failed: {e}")



# Health check
@router.get("/healthz")
def api_health():
    return {"ok": True}
