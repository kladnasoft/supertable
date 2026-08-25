"""Fail-closed proof helpers for writer-preserved table row identity.

The physical ``__rowid__`` column is an SDK implementation detail.  Most
readers must never see it.  A small number of trusted integrations (currently
the Core OData service) need a stable identity after deletion-vector and RBAC
filtering, however.  This module defines the exact snapshot proof and the one
private projection name used for that integration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ODATA_INTERNAL_ROWID_COLUMN = "__supertable_odata_rowid__"
MAX_TABLE_ROWID = (1 << 63) - 1

_LINKED_MARKERS = frozenset({
    "_linked_share",
    "_linked_generation",
    "_linked_provider_generated_ms",
    "_linked_provider_manifest_digest",
    "_linked_instance_nonce",
    "_share_columns",
})
_ROWID_INTEGRITY_VERSION = 1
_LOWER_HEX = frozenset("0123456789abcdef")


@dataclass(frozen=True)
class ResourceRowIdIntegritySeal:
    """Writer-attested row-ID facts tied to one immutable Parquet footer.

    The digest is the domain-separated physical-order stream of signed BIGINT
    identities.  It is retained for immutable-object/cache fencing and future
    verification; eligibility additionally requires the exact count/domain
    facts below and the matching resource footer seal.
    """

    version: int
    rows: int
    nonnull: int
    unique: int
    minimum: int | None
    maximum: int | None
    digest: str
    footer_sha256: str

    def __post_init__(self) -> None:
        for name, value in (
            ("version", self.version),
            ("rows", self.rows),
            ("nonnull", self.nonnull),
            ("unique", self.unique),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(f"row-ID integrity {name} is invalid")
        if self.version != _ROWID_INTEGRITY_VERSION:
            raise ValueError("row-ID integrity version is unsupported")
        if (self.minimum is None) != (self.maximum is None):
            raise ValueError("row-ID integrity extrema are incomplete")
        if self.minimum is not None and (
            not isinstance(self.minimum, int)
            or isinstance(self.minimum, bool)
            or not isinstance(self.maximum, int)
            or isinstance(self.maximum, bool)
            or self.minimum > self.maximum
            or self.minimum < -(1 << 63)
            or self.maximum > MAX_TABLE_ROWID
        ):
            raise ValueError("row-ID integrity extrema are invalid")
        for name, scalar in (
            ("digest", self.digest),
            ("footer_sha256", self.footer_sha256),
        ):
            if (
                not isinstance(scalar, str)
                or len(scalar) != 64
                or any(character not in _LOWER_HEX for character in scalar)
            ):
                raise ValueError(f"row-ID integrity {name} is invalid")


def resource_rowid_integrity_seal(
    resource: object,
) -> ResourceRowIdIntegritySeal | None:
    """Parse one exact resource seal; malformed/extended documents fail closed."""
    if not isinstance(resource, dict):
        return None
    raw = resource.get("rowid_integrity")
    if not isinstance(raw, dict) or set(raw) != {
        "version",
        "rows",
        "nonnull",
        "unique",
        "minimum",
        "maximum",
        "digest",
        "footer_sha256",
    }:
        return None
    try:
        return ResourceRowIdIntegritySeal(
            version=raw["version"],
            rows=raw["rows"],
            nonnull=raw["nonnull"],
            unique=raw["unique"],
            minimum=raw["minimum"],
            maximum=raw["maximum"],
            digest=raw["digest"],
            footer_sha256=raw["footer_sha256"],
        )
    except (TypeError, ValueError):
        return None


def _resource_has_valid_rowid_integrity(
    resource: dict[str, Any],
    *,
    high_watermark: int,
) -> bool:
    seal = resource_rowid_integrity_seal(resource)
    rows = resource.get("rows")
    footer_sha256 = resource.get("footer_sha256")
    if (
        seal is None
        or not isinstance(rows, int)
        or isinstance(rows, bool)
        or rows < 0
        or seal.rows != rows
        or seal.nonnull != rows
        or seal.unique != rows
        or seal.footer_sha256 != footer_sha256
    ):
        return False
    if rows == 0:
        return seal.minimum is None and seal.maximum is None
    return bool(
        seal.minimum is not None
        and seal.maximum is not None
        and seal.minimum > 0
        and seal.maximum <= high_watermark
    )


def _metadata_wrappers(document: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(document, dict):
        return ()
    pending = [document]
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    while pending:
        candidate = pending.pop()
        marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(candidate)
        for name in ("payload", "data", "snapshot"):
            wrapped = candidate.get(name)
            if isinstance(wrapped, dict):
                pending.append(wrapped)
    return tuple(result)


def snapshot_proves_stable_rowids(
    snapshot: object,
    *authorization_wrappers: object,
) -> bool:
    """Return whether one immutable local snapshot proves modern row IDs.

    ``rowid_high_watermark`` is published only after the writer has allocated
    table-global positive BIGINT identities (including the strict legacy
    migration scan).  The surrounding manifest must also be structurally
    complete.  Linked/provider snapshots are deliberately ineligible: a
    consumer cannot independently prove that the provider preserves the local
    writer contract across publications.

    This predicate is capability detection, not a permissive validator.  Any
    missing, malformed, ambiguous, or future-unrecognised state returns
    ``False`` and keeps the table out of keyed OData metadata.
    """
    if not isinstance(snapshot, dict):
        return False

    documents = (snapshot, *authorization_wrappers)
    for document in documents:
        for candidate in _metadata_wrappers(document):
            if any(marker in candidate for marker in _LINKED_MARKERS):
                return False

    high_watermark = snapshot.get("rowid_high_watermark")
    if (
        not isinstance(high_watermark, int)
        or isinstance(high_watermark, bool)
        or high_watermark < 0
        or high_watermark > MAX_TABLE_ROWID
    ):
        return False

    snapshot_version = snapshot.get("snapshot_version")
    if (
        not isinstance(snapshot_version, int)
        or isinstance(snapshot_version, bool)
        or snapshot_version < 0
    ):
        return False
    if not isinstance(snapshot.get("schema"), (dict, list)):
        return False

    resources = snapshot.get("resources")
    if not isinstance(resources, list):
        return False
    physical_rows = 0
    seen_files: set[str] = set()
    for resource in resources:
        if not isinstance(resource, dict):
            return False
        file_key = resource.get("file")
        rows = resource.get("rows")
        if (
            not isinstance(file_key, str)
            or not file_key
            or "\x00" in file_key
            or "://" in file_key
            or file_key in seen_files
            or not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows < 0
        ):
            return False
        seen_files.add(file_key)
        physical_rows += rows
        if physical_rows > MAX_TABLE_ROWID:
            return False
        # Stable identity requires a writer-attested physical integrity seal.
        # Metadata-only legacy snapshots cannot establish uniqueness.
        if "rowid_integrity" not in resource or not (
            _resource_has_valid_rowid_integrity(
                resource, high_watermark=high_watermark,
            )
        ):
            return False

    # Every extant physical row consumed one table-global allocation.  Deletes
    # and compaction can make this inequality strict, never reverse it.
    return physical_rows <= high_watermark


__all__ = [
    "MAX_TABLE_ROWID",
    "ODATA_INTERNAL_ROWID_COLUMN",
    "ResourceRowIdIntegritySeal",
    "resource_rowid_integrity_seal",
    "snapshot_proves_stable_rowids",
]
