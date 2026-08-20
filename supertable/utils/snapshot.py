"""Conservative validation for Redis-cached snapshot documents.

The Redis leaf payload is only a performance cache of the immutable snapshot
JSON selected by ``leaf.path``.  A partial payload must therefore miss the
cache and load that JSON; treating an omitted deletion-vector field as an
explicit empty state can resurrect physically retained deleted rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

from supertable.storage.storage_interface import ObjectMetadata
from supertable.tombstone_manifest_v2 import (
    MAX_TOMBSTONE_MANIFEST_V2_BYTES,
    TombstoneManifestV2Error,
    load_tombstone_manifest_v2,
    normalize_snapshot_tombstone_state,
    validate_logical_storage_path,
    validate_snapshot_tombstone_state,
)


_MAX_LUA_EXACT_INTEGER = 2**53 - 1


@dataclass(frozen=True)
class SnapshotArtifactReference:
    """One immutable object retained by a snapshot.

    ``declared_digest`` is representation-specific provenance, not always a
    byte hash: data/statistics objects have no snapshot-level digest, v1 and
    v2 segments use the logical deletion-vector digest, and a v2 manifest uses
    the SHA-256 of its canonical JSON.  Consumers that need a raw content seal
    (notably disaster recovery) must hash the referenced object independently.
    """

    path: str
    kind: str
    declared_size: Optional[int] = None
    declared_digest: Optional[str] = None


_ManifestLoader = Callable[[str], Union[str, bytes, bytearray, memoryview]]


def _positive_artifact_size(value: object, *, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
    ):
        raise TombstoneManifestV2Error(
            f"{field} must be a positive exact integer"
        )
    return value


def read_bounded_tombstone_manifest_bytes(
    storage: object,
    path: str,
    *,
    expected_size: Optional[int] = None,
) -> bytes:
    """Conditionally read one v2 manifest without whole-object allocation.

    A separate ``size()`` followed by ``read_bytes()`` is not a bounded read:
    the logical key can be replaced or grow between those operations.  Require
    the storage backend's immutable metadata token and conditional range API,
    cap the requested response before I/O, and verify the exact returned body.
    Backends which cannot provide that contract are rejected at this safety
    boundary rather than falling back to an unbounded whole-object read.

    ``expected_size`` binds a later re-read to an earlier validated body.  It
    is used by disaster recovery's independent second root seal.
    """
    manifest_path = validate_logical_storage_path(
        path,
        field_name="tombstone manifest path",
        required_suffix=".json",
    )
    stat_object = getattr(storage, "stat_object", None)
    read_range = getattr(storage, "read_range", None)
    if not callable(stat_object) or not callable(read_range):
        raise TombstoneManifestV2Error(
            "bounded tombstone manifest reads require stat_object and read_range"
        )
    if expected_size is not None and (
        type(expected_size) is not int
        or not 1 <= expected_size <= MAX_TOMBSTONE_MANIFEST_V2_BYTES
    ):
        raise TombstoneManifestV2Error(
            "expected tombstone manifest size is outside the supported bound"
        )

    try:
        metadata = stat_object(manifest_path)
    except Exception as exc:
        raise TombstoneManifestV2Error(
            f"cannot observe tombstone manifest {manifest_path!r}"
        ) from exc
    if not isinstance(metadata, ObjectMetadata):
        raise TombstoneManifestV2Error(
            "tombstone manifest storage returned invalid object metadata"
        )
    observed_size = metadata.size
    if (
        type(observed_size) is not int
        or not 1 <= observed_size <= MAX_TOMBSTONE_MANIFEST_V2_BYTES
    ):
        raise TombstoneManifestV2Error(
            "tombstone manifest size is outside the supported bound"
        )
    if expected_size is not None and observed_size != expected_size:
        raise TombstoneManifestV2Error(
            "tombstone manifest size changed after validation"
        )

    try:
        sealed_identity = metadata.identity_token()
    except Exception as exc:
        raise TombstoneManifestV2Error(
            "tombstone manifest returned an invalid object identity seal"
        ) from exc
    if not isinstance(sealed_identity, str) or not sealed_identity:
        raise TombstoneManifestV2Error(
            "tombstone manifest storage metadata has no immutable identity seal"
        )

    try:
        payload = read_range(
            manifest_path,
            0,
            observed_size,
            expected=metadata,
        )
    except Exception as exc:
        raise TombstoneManifestV2Error(
            f"cannot conditionally read tombstone manifest {manifest_path!r}"
        ) from exc
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TombstoneManifestV2Error(
            "bounded tombstone manifest read did not return bytes"
        )
    payload = bytes(payload)
    if len(payload) != observed_size:
        raise TombstoneManifestV2Error(
            "tombstone manifest bytes differ from the bounded range"
        )
    try:
        after = stat_object(manifest_path)
    except Exception as exc:
        raise TombstoneManifestV2Error(
            f"cannot reseal tombstone manifest {manifest_path!r}"
        ) from exc
    if not isinstance(after, ObjectMetadata) or after != metadata:
        raise TombstoneManifestV2Error(
            "tombstone manifest object changed during the bounded read"
        )
    return payload


def referenced_snapshot_artifacts(
    snapshot: Mapping[str, Any],
    storage: Optional[object] = None,
    *,
    organization: Optional[str] = None,
    super_name: Optional[str] = None,
    simple_name: Optional[str] = None,
    manifest_loader: Optional[_ManifestLoader] = None,
    require_canonical_manifest: bool = True,
) -> Tuple[SnapshotArtifactReference, ...]:
    """Return every immutable object reachable from one snapshot.

    The traversal is the retention boundary for recovery and external garbage
    collectors.  Legacy v1 snapshots contribute their single Parquet deletion
    vector.  An active v2 snapshot contributes both the JSON root and every
    segment named by that root.  V2 expansion is mandatory: omitting a loader
    is an error rather than permission to return an incomplete live set.

    ``storage`` is an ergonomic adapter for backends exposing stable
    ``ObjectMetadata`` plus conditional ``read_range``; callers with their own
    bounded reader can supply ``manifest_loader`` instead.  Active v2 traversal
    also requires the pinned table identity so the root and all reachable paths
    can be confined and the manifest's identity and lineage can be validated.
    """
    if not isinstance(snapshot, Mapping):
        raise TombstoneManifestV2Error("snapshot must be an object")
    if manifest_loader is not None and storage is not None:
        raise TypeError("provide storage or manifest_loader, not both")
    if manifest_loader is None and storage is not None:
        manifest_loader = lambda path: read_bounded_tombstone_manifest_bytes(
            storage, path,
        )

    identity_values = (organization, super_name, simple_name)
    if any(value is not None for value in identity_values) and not all(
        isinstance(value, str) and value for value in identity_values
    ):
        raise TombstoneManifestV2Error(
            "organization, super_name, and simple_name must be supplied together"
        )
    table_prefix: Optional[str] = None
    tombstone_prefix: Optional[str] = None
    if all(isinstance(value, str) and value for value in identity_values):
        table_prefix = (
            f"{organization}/{super_name}/tables/{simple_name}/"
        )
        tombstone_prefix = table_prefix + "tombstone/"

    references: dict[str, SnapshotArtifactReference] = {}

    def retain(reference: SnapshotArtifactReference) -> None:
        path = validate_logical_storage_path(
            reference.path,
            field_name=f"{reference.kind} artifact path",
        )
        if table_prefix is not None and not path.startswith(table_prefix):
            raise TombstoneManifestV2Error(
                f"{reference.kind} artifact escapes the pinned table"
            )
        normalized = SnapshotArtifactReference(
            path=path,
            kind=reference.kind,
            declared_size=reference.declared_size,
            declared_digest=reference.declared_digest,
        )
        previous = references.setdefault(path, normalized)
        if previous != normalized:
            raise TombstoneManifestV2Error(
                f"snapshot artifact {path!r} has conflicting declarations"
            )

    resources = snapshot.get("resources")
    if not isinstance(resources, list):
        raise TombstoneManifestV2Error("snapshot resources must be a list")
    for index, resource in enumerate(resources):
        if not isinstance(resource, Mapping):
            raise TombstoneManifestV2Error(
                f"snapshot resources[{index}] must be an object"
            )
        path = resource.get("file")
        if not isinstance(path, str):
            raise TombstoneManifestV2Error(
                f"snapshot resources[{index}].file must be a string"
            )
        retain(SnapshotArtifactReference(
            path=path,
            kind="data",
            declared_size=_positive_artifact_size(
                resource.get("file_size"),
                field=f"snapshot resources[{index}].file_size",
            ),
        ))

    tombstone_state = normalize_snapshot_tombstone_state(snapshot)
    pointer = tombstone_state.pointer
    rows = tombstone_state.rows
    digest = tombstone_state.digest
    tombstone_format = tombstone_state.tombstone_format
    if pointer is not None and tombstone_format == 1:
        retain(SnapshotArtifactReference(
            path=pointer,
            kind="tombstone",
            declared_digest=digest,
        ))
    elif pointer is not None:
        if table_prefix is None or tombstone_prefix is None:
            raise TombstoneManifestV2Error(
                "active tombstone_format=2 requires the pinned table identity"
            )
        snapshot_version = snapshot.get("snapshot_version")
        if (
            isinstance(snapshot_version, bool)
            or not isinstance(snapshot_version, int)
            or not 1 <= snapshot_version <= _MAX_LUA_EXACT_INTEGER
        ):
            raise TombstoneManifestV2Error(
                "active tombstone_format=2 requires a valid snapshot_version"
            )
        manifest_path = validate_logical_storage_path(
            pointer,
            field_name="tombstone manifest path",
            required_suffix=".json",
        )
        if tombstone_prefix is not None and not manifest_path.startswith(
            tombstone_prefix
        ):
            raise TombstoneManifestV2Error(
                "tombstone manifest escapes the pinned tombstone namespace"
            )
        if manifest_loader is None:
            raise TombstoneManifestV2Error(
                "active tombstone_format=2 requires a manifest loader"
            )
        try:
            raw_manifest = manifest_loader(manifest_path)
        except TombstoneManifestV2Error:
            raise
        except Exception as exc:
            raise TombstoneManifestV2Error(
                f"cannot read tombstone manifest {manifest_path!r}: {exc}"
            ) from exc
        manifest = load_tombstone_manifest_v2(
            raw_manifest,
            expected_organization=organization,
            expected_super_name=super_name,
            expected_simple_name=simple_name,
            pinned_snapshot_version=snapshot_version,
            expected_total_rows=rows,
            expected_digest=digest,
            expected_segment_prefix=(
                tombstone_prefix.rstrip("/")
                if tombstone_prefix is not None else None
            ),
            require_canonical_json=require_canonical_manifest,
        )
        retain(SnapshotArtifactReference(
            path=manifest_path,
            kind="tombstone_manifest",
            declared_digest=digest,
        ))
        for segment in manifest.segments:
            retain(SnapshotArtifactReference(
                path=segment.file,
                kind="tombstone_segment",
                declared_size=segment.file_size,
                declared_digest=segment.digest,
            ))

    stats_file = snapshot.get("stats_file")
    if stats_file is not None:
        if not isinstance(stats_file, str):
            raise TombstoneManifestV2Error(
                "snapshot stats_file must be a string or null"
            )
        retain(SnapshotArtifactReference(
            path=stats_file,
            kind="stats_file",
        ))

    return tuple(references[path] for path in sorted(references))


def collect_share_row_filters(*documents: object) -> Tuple[str, ...]:
    """Return every distinct linked-share predicate in supported wrappers.

    Policy overlays have historically lived on the leaf itself, beside or
    inside ``payload``/``data``/``snapshot``, and in one nested ``snapshot``
    wrapper.  Traverse exactly those locations rather than recursively walking
    arbitrary user metadata (a schema may legitimately contain a column named
    ``_row_filter``).  The established catalog representation treats an
    absent marker, JSON null, and a blank string as unrestricted.  Any other
    non-string marker is authorization corruption and therefore fails closed.
    """
    candidates = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        candidates.append(document)
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = document.get(wrapper_name)
            if not isinstance(wrapped, dict):
                continue
            candidates.append(wrapped)
            nested = wrapped.get("snapshot")
            if isinstance(nested, dict):
                candidates.append(nested)

    filters = []
    for candidate in candidates:
        if "_row_filter" not in candidate:
            continue
        raw_filter = candidate.get("_row_filter")
        if raw_filter is None:
            continue
        if not isinstance(raw_filter, str):
            raise RuntimeError("Linked-share policy metadata is invalid")
        if not raw_filter.strip():
            continue
        if raw_filter not in filters:
            filters.append(raw_filter)
    return tuple(filters)


def combined_share_row_filter(*documents: object) -> Optional[str]:
    """Return the fail-closed conjunction of supported policy overlays."""
    filters = collect_share_row_filters(*documents)
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return " AND ".join(f"({row_filter})" for row_filter in filters)


def snapshot_cache_payload(payload: object) -> Dict[str, Any]:
    """Return a cache document with an explicit linked-share policy state.

    Current publishers use ``None`` for an unrestricted snapshot.  A shallow
    copy preserves any direct or nested share overlay while ensuring readers
    can distinguish a current complete cache from a legacy cache that may have
    omitted authorization metadata.
    """
    if not isinstance(payload, dict):
        raise TypeError("Snapshot cache payload must be a JSON object")
    normalized = dict(payload)
    normalized.setdefault("_row_filter", None)
    # Validate every supported wrapper marker before publishing it.  The SQL
    # predicate itself is parsed at the protected read-view boundary.
    collect_share_row_filters(normalized)
    return normalized


def complete_snapshot_payload(
        payload: object,
        *,
        expected_version: object = None,
        require_policy_marker: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return a complete cached snapshot, otherwise ``None``.

    Completeness includes an explicit deletion-vector state.  The only valid
    pointerless state is exactly ``(None, 0, None)`` (for legacy v1 or a
    physically drained explicit v2 table); an active pointer needs a positive
    exact integer count and the canonical lowercase SHA-256 seal.  Missing or
    explicit format 1 is the legacy single-Parquet representation.  Explicit
    format 2 requires a canonical standalone JSON-manifest pointer.  Invalid
    or partial payloads deliberately fall back to the heavy immutable JSON
    rather than guessing that no tombstone exists.  Redis callers must set
    ``require_policy_marker=True``: only an explicit ``_row_filter`` key proves
    that the cache publisher represented the linked-share policy state.
    Immutable snapshot objects may omit that key; absence there is the
    established unrestricted representation.
    """
    if not isinstance(payload, dict):
        return None

    if require_policy_marker and "_row_filter" not in payload:
        return None

    candidate = payload
    nested = payload.get("snapshot")
    if not isinstance(candidate.get("resources"), list) and isinstance(
            nested, dict,
    ):
        candidate = nested

    required = {
        "snapshot_version", "schema", "resources", "tombstone",
        "tombstone_rows", "tombstone_digest",
    }
    if not required.issubset(candidate):
        return None
    if not isinstance(candidate.get("resources"), list):
        return None
    if not isinstance(candidate.get("schema"), (dict, list)):
        return None

    version = candidate.get("snapshot_version")
    if not (
        isinstance(version, int)
        and not isinstance(version, bool)
        and 0 <= version <= _MAX_LUA_EXACT_INTEGER
    ):
        return None
    if expected_version is not None:
        if not (
            isinstance(expected_version, int)
            and not isinstance(expected_version, bool)
            and 0 <= expected_version <= _MAX_LUA_EXACT_INTEGER
            and expected_version == version
        ):
            return None

    try:
        validate_snapshot_tombstone_state(
            candidate.get("tombstone"),
            candidate.get("tombstone_rows"),
            candidate.get("tombstone_digest"),
            format_present="tombstone_format" in candidate,
            tombstone_format=candidate.get("tombstone_format"),
        )
    except (TypeError, TombstoneManifestV2Error):
        return None

    return candidate
