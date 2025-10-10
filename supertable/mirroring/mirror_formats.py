# supertable/mirroring/mirror_formats.py

import os
from enum import Enum
from typing import Iterable, List, Dict, Any, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog

# Writers are split per-format
from supertable.mirroring.mirror_delta import write_delta_table
from supertable.mirroring.mirror_iceberg import write_iceberg_table
from supertable.mirroring.mirror_parquet import write_parquet_table  # Parquet mirror support


class FormatMirror(str, Enum):
    DELTA = "DELTA"
    ICEBERG = "ICEBERG"
    PARQUET = "PARQUET"

    @staticmethod
    def normalize(values: Iterable[str]) -> List[str]:
        out: List[str] = []
        for v in values or []:
            vu = str(v).upper()
            if vu in ("DELTA", "ICEBERG", "PARQUET"):
                out.append(vu)
        # de-dup, preserve order
        seen = set()
        ordered: List[str] = []
        for v in out:
            if v not in seen:
                seen.add(v)
                ordered.append(v)
        return ordered


class MirrorFormats:
    """
    Redis-backed format mirror configuration and dispatch.

    Storage:
      - Redis key: supertable:{org}:{super}:meta:mirrors
        value: {"formats": ["DELTA", ...], "ts": <epoch_ms>}
    """

    # ---------- config helpers (public) --------------------------------------
    @staticmethod
    def _catalog(super_table) -> RedisCatalog:
        # keep a short-lived instance; redis-py pools underneath
        return RedisCatalog()

    @staticmethod
    def get_enabled(super_table) -> List[str]:
        c = MirrorFormats._catalog(super_table)
        return c.get_mirrors(super_table.organization, super_table.super_name)

    @staticmethod
    def set_with_lock(super_table, formats: Iterable[str]) -> List[str]:
        """
        Historical API kept for compatibility; now it's a single atomic Redis SET.
        """
        enabled = FormatMirror.normalize(formats)
        c = MirrorFormats._catalog(super_table)
        out = c.set_mirrors(super_table.organization, super_table.super_name, enabled)
        logger.info(f"[mirror] set formats = {out}")
        return out

    @staticmethod
    def enable_with_lock(super_table, fmt: str) -> List[str]:
        c = MirrorFormats._catalog(super_table)
        out = c.enable_mirror(super_table.organization, super_table.super_name, fmt)
        logger.info(f"[mirror] enabled {fmt} -> {out}")
        return out

    @staticmethod
    def disable_with_lock(super_table, fmt: str) -> List[str]:
        c = MirrorFormats._catalog(super_table)
        out = c.disable_mirror(super_table.organization, super_table.super_name, fmt)
        logger.info(f"[mirror] disabled {fmt} -> {out}")
        return out

    # ---------- mirroring (internal) ----------------------------------------
    @staticmethod
    def mirror_if_enabled(
        super_table,
        table_name: str,
        simple_snapshot: Dict[str, Any],
        mirrors: Optional[List[str]] = None,
    ) -> None:
        """
        Run immediately after a successful simple snapshot update.
        Caller SHOULD still hold the per-simple lock to avoid concurrent
        writers mirroring the same table.
        """
        enabled = mirrors if mirrors is not None else MirrorFormats.get_enabled(super_table)
        if not enabled:
            return

        base = os.path.join(super_table.organization, super_table.super_name)

        # Ensure top-level dirs exist (lazy)
        if "DELTA" in enabled:
            super_table.storage.makedirs(os.path.join(base, "delta", table_name, "_delta_log"))
        if "ICEBERG" in enabled:
            super_table.storage.makedirs(os.path.join(base, "iceberg", table_name, "metadata"))
            super_table.storage.makedirs(os.path.join(base, "iceberg", table_name, "manifests"))
        if "PARQUET" in enabled:
            super_table.storage.makedirs(os.path.join(base, "parquet", table_name, "files"))

        # Delegate to per-format writers (latest-only mirror)
        if "DELTA" in enabled:
            write_delta_table(super_table, table_name, simple_snapshot)
        if "ICEBERG" in enabled:
            write_iceberg_table(super_table, table_name, simple_snapshot)
        if "PARQUET" in enabled:
            write_parquet_table(super_table, table_name, simple_snapshot)
