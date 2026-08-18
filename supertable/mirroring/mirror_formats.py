# route: supertable.mirroring.mirror_formats

import json
from enum import Enum
from typing import Iterable, List, Dict, Any, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog
from supertable.utils.snapshot import (
    complete_snapshot_payload,
    snapshot_cache_payload,
)

# Writers are split per-format
from supertable.mirroring.mirror_delta import (
    verify_delta_table,
    write_delta_table,
)
from supertable.mirroring.mirror_iceberg import (
    verify_iceberg_table,
    write_iceberg_table,
)
from supertable.mirroring.mirror_parquet import (
    verify_parquet_table,
    write_parquet_table,
)


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


class MirrorRecoveryConfirmationRequired(RuntimeError):
    """A previous mirror publisher is not durably known to be quiescent."""

    def __init__(
        self, *, organization: str, super_name: str, table_name: str,
        commit_id: str, publication_owner: str,
    ) -> None:
        self.organization = organization
        self.super_name = super_name
        self.table_name = table_name
        self.commit_id = commit_id
        self.publication_owner = publication_owner
        super().__init__(
            "Mirror recovery requires explicit confirmation that the previous "
            "publisher has stopped for "
            f"{organization}/{super_name}/{table_name}; "
            f"expected_commit_id={commit_id!r}, "
            f"expected_previous_owner={publication_owner!r}"
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


def _canonical_snapshot_bytes(snapshot: Dict[str, Any]) -> bytes:
    """Return the strict canonical representation used for recovery checks."""
    try:
        return json.dumps(
            snapshot,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Committed mirror snapshot is not canonical JSON"
        ) from exc


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
        *,
        commit_id: str = "",
        snapshot_path: str = "",
        verify: bool = True,
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

        mirror_snapshot: Dict[str, Any] = dict(simple_snapshot)
        if commit_id:
            mirror_snapshot["_mirror_commit_id"] = str(commit_id)
        if snapshot_path:
            mirror_snapshot["_mirror_snapshot_path"] = str(snapshot_path)

        writers = {
            "DELTA": write_delta_table,
            "ICEBERG": write_iceberg_table,
            "PARQUET": write_parquet_table,
        }
        verifiers = {
            "DELTA": lambda: verify_delta_table(
                super_table, table_name, mirror_snapshot,
                commit_id=commit_id,
            ),
            "ICEBERG": lambda: verify_iceberg_table(
                super_table, table_name, mirror_snapshot,
                commit_id=commit_id,
            ),
            "PARQUET": lambda: verify_parquet_table(
                super_table, table_name, mirror_snapshot,
            ),
        }
        completed: List[str] = []
        for mirror_format in enabled:
            try:
                writers[mirror_format](super_table, table_name, mirror_snapshot)
                if verify:
                    verifiers[mirror_format]()
            except Exception as exc:
                raise MirrorSyncError(
                    table_name=table_name,
                    failed_format=mirror_format,
                    completed_formats=completed,
                    cause=exc,
                ) from exc
            completed.append(mirror_format)
        return completed

    @staticmethod
    def reconcile_publication(
        super_table,
        table_name: str,
        *,
        lock_timeout_s: int = 30,
        expected_commit_id: Optional[str] = None,
        expected_previous_owner: Optional[str] = None,
        confirm_previous_owner_stopped: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Recover one durable mirror publication without replaying the write.

        The table lock pins both the outbox record and leaf pointer, while the
        durable publication owner prevents lease expiry from authorizing a
        second object-store publisher.  A prior owner that is not durably
        quiescent may be replaced only when an operator supplies its exact
        owner/commit identity and explicitly confirms that it has stopped.
        Recovery then republishes/verifies the authoritative leaf payload and
        performs the owner-and-lock-fenced ``complete`` transition in Redis.
        """
        catalog = MirrorFormats._catalog(super_table)
        org = super_table.organization
        sup = super_table.super_name
        # RedisLocking keeps acquired simple locks alive with its heartbeat
        # thread until release_simple_lock() is called.  Recovery may need to
        # copy a large immutable snapshot, so it must not rely on the initial
        # 30-second lease alone.
        token = catalog.acquire_simple_lock(
            org, sup, table_name, ttl_s=30, timeout_s=lock_timeout_s,
        )
        if not token:
            raise TimeoutError(
                f"Could not acquire mirror recovery lock for {org}/{sup}/{table_name}"
        )
        try:
            state = catalog.get_mirror_publication(org, sup, table_name)
            if state is None:
                return state
            if not isinstance(state, dict):
                raise RuntimeError("Mirror publication state is not an object")
            status = state.get("status")
            if status == "complete":
                return state
            if status not in {"prepared", "core_committed", "failed"}:
                raise RuntimeError("Mirror publication state has invalid status")
            core_committed = state.get("core_committed")
            if not isinstance(core_committed, bool):
                raise RuntimeError(
                    "Mirror publication state has invalid core commit state"
                )

            commit_id = str(state.get("commit_id") or "")
            snapshot_path = str(state.get("snapshot_path") or "")
            if not commit_id or not snapshot_path:
                raise RuntimeError(
                    "Mirror publication state is missing immutable identity"
                )

            leaf = catalog.get_leaf(org, sup, table_name)
            leaf_matches = bool(
                isinstance(leaf, dict)
                and str(leaf.get("commit_id") or "") == commit_id
                and str(leaf.get("path") or "") == snapshot_path
            )

            def claim_for_recovery() -> Dict[str, Any]:
                """Bind this lock as publisher after exact operator consent."""
                publication_owner = str(
                    state.get("publication_owner") or ""
                )
                if publication_owner == token:
                    return state
                publisher_quiesced = (
                    state.get("publisher_quiesced") is True
                )
                exact_confirmation = bool(
                    confirm_previous_owner_stopped is True
                    and expected_commit_id == commit_id
                    and expected_previous_owner is not None
                    and expected_previous_owner == publication_owner
                )
                if not publisher_quiesced and not exact_confirmation:
                    raise MirrorRecoveryConfirmationRequired(
                        organization=org,
                        super_name=sup,
                        table_name=table_name,
                        commit_id=commit_id,
                        publication_owner=publication_owner,
                    )
                claim = getattr(catalog, "claim_mirror_publication", None)
                if not callable(claim):
                    raise RuntimeError(
                        "Catalog does not support durable mirror owner claims"
                    )
                claimed = catalog.claim_mirror_publication(
                    org,
                    sup,
                    table_name,
                    commit_id=commit_id,
                    expected_previous_owner=publication_owner,
                    lock_token=token,
                    confirm_previous_owner_stopped=exact_confirmation,
                )
                if (
                    not isinstance(claimed, dict)
                    or str(claimed.get("commit_id") or "") != commit_id
                    or str(claimed.get("publication_owner") or "") != token
                    or claimed.get("publisher_quiesced") is not False
                ):
                    raise RuntimeError(
                        "Catalog returned an invalid mirror owner claim"
                    )
                return claimed

            if not core_committed:
                if leaf_matches:
                    raise RuntimeError(
                        "Mirror outbox disagrees with the committed catalog leaf"
                    )
                claim_for_recovery()
                error = RuntimeError(
                    "Prepared mirror publication has no committed core snapshot"
                )
                return catalog.fail_mirror_publication(
                    org, sup, table_name, commit_id=commit_id,
                    lock_token=token, failure_stage="recovery:core_not_committed",
                    error=error,
                )
            if not leaf_matches:
                raise RuntimeError(
                    "Current catalog leaf does not match the mirror recovery commit"
                )

            # The atomic core commit stores the complete snapshot in the leaf.
            # Treat that payload as authoritative: the object at ``path`` is
            # writable storage and a stale process or operator could replace it
            # without changing the Redis pointer/commit identity.  Recovery
            # must prove exact canonical equality before any mirror side effect.
            leaf_snapshot = complete_snapshot_payload(
                leaf.get("payload"),
                expected_version=leaf.get("version"),
                require_policy_marker=True,
            )
            if leaf_snapshot is None:
                raise RuntimeError(
                    "Committed catalog leaf has no complete mirror snapshot payload"
                )

            stored_snapshot = super_table.storage.read_json(snapshot_path)
            stored_snapshot = complete_snapshot_payload(
                stored_snapshot,
                expected_version=leaf.get("version"),
            )
            if stored_snapshot is None:
                raise RuntimeError(
                    "Committed mirror snapshot object is incomplete or invalid"
                )
            if (
                _canonical_snapshot_bytes(
                    snapshot_cache_payload(stored_snapshot)
                )
                != _canonical_snapshot_bytes(
                    snapshot_cache_payload(leaf_snapshot)
                )
            ):
                raise RuntimeError(
                    "Committed mirror snapshot object does not match the "
                    "authoritative catalog leaf payload"
                )
            recorded_mirrors = state.get("mirrors")
            if (
                not isinstance(recorded_mirrors, list)
                or not recorded_mirrors
            ):
                raise RuntimeError(
                    "Mirror publication state contains invalid formats"
                )
            mirrors = FormatMirror.normalize(recorded_mirrors)
            if len(mirrors) != len(recorded_mirrors):
                raise RuntimeError("Mirror publication state contains invalid formats")
            state = claim_for_recovery()

            try:
                MirrorFormats.mirror_if_enabled(
                    super_table,
                    table_name,
                    leaf_snapshot,
                    mirrors=mirrors,
                    commit_id=commit_id,
                    snapshot_path=snapshot_path,
                    verify=True,
                )
            except Exception as exc:
                catalog.fail_mirror_publication(
                    org, sup, table_name, commit_id=commit_id,
                    lock_token=token, failure_stage="recovery:mirror",
                    error=exc,
                )
                raise
            return catalog.complete_mirror_publication(
                org, sup, table_name, commit_id=commit_id,
                lock_token=token,
            )
        finally:
            catalog.release_simple_lock(org, sup, table_name, token)


__all__ = [
    "FormatMirror",
    "MirrorFormats",
    "MirrorSyncError",
    "MirrorPublicationError",
    "MirrorRecoveryConfirmationRequired",
]
