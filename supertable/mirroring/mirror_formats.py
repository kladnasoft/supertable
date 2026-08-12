# route: supertable.mirroring.mirror_formats

from enum import Enum
from typing import Iterable, List, Dict, Any, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog

# Writers are split per-format
from supertable.mirroring.mirror_delta import write_delta_table
from supertable.mirroring.mirror_iceberg import write_iceberg_table
from supertable.mirroring.mirror_parquet import write_parquet_table  # Parquet mirror support


class MirrorSyncError(RuntimeError):
    """One enabled mirror format failed during synchronous publication."""

    def __init__(
        self,
        *,
        table_name: str,
        failed_format: str,
        completed_formats: List[str],
        cause: Exception | None = None,
    ):
        self.table_name = table_name
        self.failed_format = failed_format
        self.completed_formats = tuple(completed_formats)
        self.cause = cause
        cause_detail = (
            f", error={type(cause).__name__}: {cause}" if cause is not None else ""
        )
        super().__init__(
            f"Mirror sync failed for table {table_name!r}, format "
            f"{failed_format}; completed={list(self.completed_formats)!r}"
            f"{cause_detail}"
        )


class MirrorPublicationError(RuntimeError):
    """Mirror failed after the authoritative core snapshot was committed.

    ``core_committed`` is deliberately explicit so callers do not blindly
    retry the data mutation and create duplicate rows. Reconciliation should
    rerun mirroring for ``snapshot_path``/``commit_id`` instead.
    """

    def __init__(
        self,
        *,
        organization: str,
        super_name: str,
        table_name: str,
        mirrors: List[str],
        commit_id: str,
        snapshot_path: str,
        core_result: Any,
        cause: Exception,
    ):
        self.organization = organization
        self.super_name = super_name
        self.table_name = table_name
        self.mirrors = tuple(mirrors)
        self.commit_id = commit_id
        self.snapshot_path = snapshot_path
        self.core_result = core_result
        self.core_committed = True
        self.cause = cause
        self.failed_mirror = (
            cause.failed_format if isinstance(cause, MirrorSyncError) else None
        )
        self.completed_mirrors = (
            cause.completed_formats if isinstance(cause, MirrorSyncError) else ()
        )
        super().__init__(
            "Core snapshot committed, but mirror publication failed for "
            f"{organization}/{super_name}/{table_name}; commit_id={commit_id}, "
            f"snapshot={snapshot_path!r}, mirrors={list(self.mirrors)!r}. "
            "The mirror may be stale; reconcile this committed snapshot and "
            "do not blindly retry the mutation."
        )


class FormatMirror(str, Enum):
    DELTA = "DELTA"
    ICEBERG = "ICEBERG"
    PARQUET = "PARQUET"

    @staticmethod
    def normalize(values: Iterable[str]) -> List[str]:
        out: List[str] = []
        for v in values or []:
            vu = (v.value if isinstance(v, FormatMirror) else str(v)).upper()
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
      - Redis key: supertable:{org}:lakes:{super}:meta:mirrors
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
    ) -> List[str]:
        """
        Run immediately after a successful simple snapshot update.
        Caller SHOULD still hold the per-simple lock to avoid concurrent
        writers mirroring the same table.
        """
        configured = (
            mirrors if mirrors is not None else MirrorFormats.get_enabled(super_table)
        )
        invalid = [
            value for value in (configured or [])
            if (
                value.value if isinstance(value, FormatMirror) else str(value)
            ).upper() not in {member.value for member in FormatMirror}
        ]
        if invalid:
            raise ValueError(f"Unsupported mirror format(s): {invalid!r}")
        enabled = FormatMirror.normalize(configured)
        if not enabled:
            return []

        # Mirror writers currently copy the snapshot's physical resources; they
        # do not apply its deletion vector.  Publishing such a mirror would
        # resurrect logically deleted rows.  DataWriter drains an active vector
        # before mirroring, and this guard keeps every other caller fail-closed.
        tombstone = simple_snapshot.get("tombstone")
        tombstone_rows = int(simple_snapshot.get("tombstone_rows") or 0)
        if tombstone or tombstone_rows:
            raise RuntimeError(
                "Cannot mirror a snapshot with an active deletion vector; "
                "compact/drain the tombstone first"
            )

        writers = {
            "DELTA": write_delta_table,
            "ICEBERG": write_iceberg_table,
            "PARQUET": write_parquet_table,
        }
        completed: List[str] = []
        for mirror_format in enabled:
            try:
                writers[mirror_format](super_table, table_name, simple_snapshot)
            except Exception as exc:
                raise MirrorSyncError(
                    table_name=table_name,
                    failed_format=mirror_format,
                    completed_formats=completed,
                    cause=exc,
                ) from exc
            completed.append(mirror_format)
        return completed


__all__ = [
    "FormatMirror",
    "MirrorFormats",
    "MirrorSyncError",
    "MirrorPublicationError",
]
