"""Strict, storage-independent deletion-vector format primitives.

The v2 deletion-vector root is a small canonical JSON document.  A snapshot
stores the manifest's logical object key in ``tombstone`` and the SHA-256 of
the manifest's *canonical* JSON representation in ``tombstone_digest``.  Each
manifest then seals an ordered set of immutable Parquet segments by path,
cardinality, byte length, and the established logical DV-row SHA-256.

This module deliberately contains no writer or reader policy.  In particular,
the presence of these definitions does not make v2 safe to emit to a mixed
fleet.  Publication remains disabled until every reader, recovery tool,
exporter, mirror, and garbage collector understands ``tombstone_format=2``.

Format 3 keeps the original one-Parquet-per-snapshot layout, but makes the
immutability contract explicit. Its snapshot digest is SHA-256 over the exact
encoded Parquet bytes, computed in one native call; it is intentionally not a
second sorted logical-row scan. Correctness comes from validating the new
delta, writing a fresh immutable object, and fencing the atomic snapshot
publication. Existing v1/v2 semantics remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Dict, Mapping, Optional, Tuple, Union
from urllib.parse import urlsplit

from supertable import redis_keys as RK


TOMBSTONE_FORMAT_V1 = 1
TOMBSTONE_FORMAT_V2 = 2
TOMBSTONE_FORMAT_V3 = 3
TOMBSTONE_MANIFEST_V2_FORMAT = "supertable-tombstone-manifest-v2"

# These are format bounds, not tunables.  Raising them requires a format bump
# and a fresh bounded-work audit across every reader and recovery path.
MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS = 32
MAX_TOMBSTONE_MANIFEST_V2_BYTES = 256 * 1024
MAX_TOMBSTONE_SEGMENT_PATH_BYTES = 4_096
# Redis' bundled Lua CJSON encoder is configured for 14 significant digits.
# Snapshot publication decodes and re-encodes the payload inside Lua, so the
# v2 format must use the stricter all-integers round-trip ceiling rather than
# IEEE-754's wider exact-binary range.
MAX_JSON_EXACT_INTEGER = 10**14 - 1

_SHA256_HEX = frozenset("0123456789abcdef")
_MANIFEST_FIELDS = frozenset({
    "format",
    "organization",
    "super_name",
    "simple_name",
    "base_snapshot_version",
    "snapshot_version",
    "total_rows",
    "segments",
})
_SEGMENT_FIELDS = frozenset({"file", "rows", "file_size", "digest"})
_SNAPSHOT_TOMBSTONE_FIELDS = frozenset({
    "tombstone",
    "tombstone_rows",
    "tombstone_digest",
})
_V2_4_EMPTY_SNAPSHOT_TOMBSTONE_FIELDS = frozenset({
    "tombstone",
    "tombstone_rows",
})
_RawManifest = Union[Mapping[str, Any], str, bytes, bytearray, memoryview]


class TombstoneManifestV2Error(ValueError):
    """A v2 tombstone manifest or one of its sealed artifacts is invalid."""


def _strict_integer(
    value: object,
    *,
    field_name: str,
    minimum: int,
    maximum: int = MAX_JSON_EXACT_INTEGER,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TombstoneManifestV2Error(f"{field_name} must be an integer")
    if value < minimum or value > maximum:
        raise TombstoneManifestV2Error(
            f"{field_name} must be between {minimum} and {maximum}"
        )
    return value


def _strict_digest(value: object, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _SHA256_HEX for character in value)
    ):
        raise TombstoneManifestV2Error(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return value


def _strict_identity(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TombstoneManifestV2Error(f"{field_name} must be a string")
    # Redis is the catalog identity boundary.  Reusing its closed constructor
    # keeps a manifest from introducing an alternate spelling or namespace for
    # a table.  The constructor is pure and performs no Redis access.
    try:
        if field_name == "organization":
            RK.org_prefix(value)
            if RK.is_reserved_org_name(value):
                raise ValueError("reserved organization")
        elif field_name == "super_name":
            RK.meta_table_names("manifest-org", value)
        else:
            RK.meta_leaf("manifest-org", "manifest-super", value)
    except (TypeError, ValueError) as exc:
        raise TombstoneManifestV2Error(
            f"{field_name} is not a canonical table identifier"
        ) from None
    return value


def validate_logical_storage_path(
    value: object,
    *,
    field_name: str = "file",
    required_suffix: Optional[str] = None,
) -> str:
    """Return one canonical logical object key or raise.

    Manifest paths are catalog identities, never local absolute paths,
    provider URLs, or presigned URLs.  Provider-specific base prefixes are
    intentionally outside this module; callers may additionally check an
    expected table prefix after storage normalization.
    """
    if not isinstance(value, str) or not value:
        raise TombstoneManifestV2Error(
            f"{field_name} must be a non-empty logical storage path"
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise TombstoneManifestV2Error(
            f"{field_name} is not valid UTF-8"
        ) from None
    if len(encoded) > MAX_TOMBSTONE_SEGMENT_PATH_BYTES:
        raise TombstoneManifestV2Error(f"{field_name} is too long")
    if value != value.strip() or "\\" in value:
        raise TombstoneManifestV2Error(
            f"{field_name} is not a canonical logical storage path"
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise TombstoneManifestV2Error(f"{field_name} contains control characters")

    try:
        split = urlsplit(value)
    except ValueError as exc:
        # ``urlsplit`` raises for malformed bracketed authorities and a few
        # NFKC-unsafe netloc spellings.  Keep every invalid logical path behind
        # this module's one fail-closed error contract so cache/recovery callers
        # cannot accidentally treat parser-specific exceptions differently.
        raise TombstoneManifestV2Error(
            f"{field_name} is not a valid logical storage path"
        ) from None
    if split.scheme or split.netloc or split.query or split.fragment:
        raise TombstoneManifestV2Error(
            f"{field_name} must not be a URI or carry URL parameters"
        )
    candidate = PurePosixPath(value)
    if (
        candidate.is_absolute()
        or value.startswith("/")
        or value.endswith("/")
        or "//" in value
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or candidate.as_posix() != value
    ):
        raise TombstoneManifestV2Error(
            f"{field_name} escapes or aliases the logical storage namespace"
        )
    if required_suffix is not None and not value.endswith(required_suffix):
        raise TombstoneManifestV2Error(
            f"{field_name} must end with {required_suffix!r}"
        )
    return value


def tombstone_v3_artifact_digest(payload: object) -> str:
    """Return the exact-byte seal for one immutable format-3 Parquet object.

    The encoder already owns the complete byte buffer, so this is one bulk
    OpenSSL call and no Python iteration over logical rows. The explicit format
    discriminator distinguishes this byte fingerprint from legacy logical
    ``st-dv-v1`` seals.
    """
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TombstoneManifestV2Error(
            "format-3 tombstone payload must be bytes"
        )
    return hashlib.sha256(payload).hexdigest()


def validate_snapshot_tombstone_state(
    pointer: object,
    rows: object,
    digest: object,
    *,
    format_present: bool,
    tombstone_format: object = None,
) -> int:
    """Validate the snapshot-level v1/v2/v3 discriminated tombstone state.

    Missing ``tombstone_format`` and explicit integer ``1`` denote the legacy
    single-Parquet representation.  Only explicit integer ``2`` activates the
    standalone JSON manifest.  Explicit integer ``3`` is the immutable
    single-Parquet representation. JSON null, booleans, strings, future
    versions, and pointer/format hybrids are rejected rather than guessed.
    """
    if not isinstance(format_present, bool):
        raise TypeError("format_present must be boolean")
    if format_present:
        if (
            isinstance(tombstone_format, bool)
            or not isinstance(tombstone_format, int)
            or tombstone_format not in (
                TOMBSTONE_FORMAT_V1,
                TOMBSTONE_FORMAT_V2,
                TOMBSTONE_FORMAT_V3,
            )
        ):
            raise TombstoneManifestV2Error(
                "tombstone_format must be integer 1, 2, or 3"
            )
        normalized_format = tombstone_format
    else:
        normalized_format = TOMBSTONE_FORMAT_V1

    exact_rows = isinstance(rows, int) and not isinstance(rows, bool)
    if pointer is None:
        if not (exact_rows and rows == 0 and digest is None):
            raise TombstoneManifestV2Error(
                "pointerless tombstone state must be exactly (None, 0, None)"
            )
        return normalized_format

    if not isinstance(pointer, str) or not pointer:
        raise TombstoneManifestV2Error(
            "an active tombstone pointer must be a non-empty string"
        )
    if not exact_rows or rows <= 0:
        raise TombstoneManifestV2Error(
            "an active tombstone must have a positive exact integer row count"
        )
    _strict_digest(digest, field_name="tombstone_digest")
    if normalized_format == TOMBSTONE_FORMAT_V2:
        _strict_integer(rows, field_name="tombstone_rows", minimum=1)
        validate_logical_storage_path(
            pointer,
            field_name="tombstone manifest pointer",
            required_suffix=".json",
        )
    elif normalized_format == TOMBSTONE_FORMAT_V3:
        _strict_integer(rows, field_name="tombstone_rows", minimum=1)
        validate_logical_storage_path(
            pointer,
            field_name="tombstone format-3 pointer",
            required_suffix=".parquet",
        )
    elif pointer.endswith(".json"):
        # A manifest-looking pointer without the v2 discriminator is unsafe:
        # old readers would try to interpret it as a Parquet deletion vector.
        raise TombstoneManifestV2Error(
            "a JSON tombstone pointer requires tombstone_format=2"
        )
    return normalized_format


@dataclass(frozen=True)
class NormalizedSnapshotTombstoneState:
    """Validated deletion-vector state from one authoritative snapshot."""

    pointer: Optional[str]
    rows: int
    digest: Optional[str]
    tombstone_format: int
    format_present: bool


def normalize_snapshot_tombstone_state(
    snapshot: Mapping[str, Any],
) -> NormalizedSnapshotTombstoneState:
    """Validate an authoritative snapshot, including its historical shape.

    Snapshots written before deletion-vector metadata existed omitted all
    three state fields and the format discriminator.  Version 2.4 snapshots
    used a second unambiguous empty shape: an explicit null ``tombstone`` and
    exact-zero ``tombstone_rows``, with no digest or format discriminator.
    Only those two historical empty shapes are normalized.  Every other
    partial state remains invalid, and the scalar validator stays strict.

    This helper is intentionally for immutable authoritative documents.  A
    cached snapshot payload must still require explicit deletion-vector state
    before it can be treated as complete.
    """
    if not isinstance(snapshot, Mapping):
        raise TypeError("snapshot must be a mapping")

    present_fields = _SNAPSHOT_TOMBSTONE_FIELDS.intersection(snapshot)
    format_present = "tombstone_format" in snapshot
    if not present_fields and not format_present:
        pointer = None
        rows = 0
        digest = None
    elif (
        not format_present
        and present_fields == _V2_4_EMPTY_SNAPSHOT_TOMBSTONE_FIELDS
        and snapshot.get("tombstone") is None
        and type(snapshot.get("tombstone_rows")) is int
        and snapshot["tombstone_rows"] == 0
    ):
        pointer = None
        rows = 0
        digest = None
    else:
        if present_fields != _SNAPSHOT_TOMBSTONE_FIELDS:
            raise TombstoneManifestV2Error(
                "snapshot tombstone state fields must be all present or all absent"
            )
        pointer = snapshot.get("tombstone")
        rows = snapshot.get("tombstone_rows")
        digest = snapshot.get("tombstone_digest")

    normalized_format = validate_snapshot_tombstone_state(
        pointer,
        rows,
        digest,
        format_present=format_present,
        tombstone_format=snapshot.get("tombstone_format"),
    )
    return NormalizedSnapshotTombstoneState(
        pointer=pointer,
        rows=rows,
        digest=digest,
        tombstone_format=normalized_format,
        format_present=format_present,
    )


@dataclass(frozen=True)
class TombstoneSegment:
    """One immutable deletion-vector segment sealed by the v2 root.

    ``digest`` is the existing st-dv-v1 logical canonical row-stream digest,
    not a hash of provider/Parquet bytes.  This permits a valid v1 artifact to
    become a v2 base segment without a rewrite or a second byte-hashing pass.
    Readers prove a segment with its declared byte size, decoded schema and row
    count, and the logical digest after canonical row validation.
    """

    file: str
    rows: int
    file_size: int
    digest: str

    def __post_init__(self) -> None:
        validate_logical_storage_path(
            self.file,
            field_name="segment.file",
            required_suffix=".parquet",
        )
        _strict_integer(self.rows, field_name="segment.rows", minimum=1)
        _strict_integer(
            self.file_size,
            field_name="segment.file_size",
            minimum=1,
        )
        _strict_digest(self.digest, field_name="segment.digest")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file,
            "rows": self.rows,
            "file_size": self.file_size,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class TombstoneManifestV2:
    """Canonical root for one immutable, ordered v2 deletion-vector."""

    organization: str
    super_name: str
    simple_name: str
    base_snapshot_version: int
    snapshot_version: int
    total_rows: int
    segments: Tuple[TombstoneSegment, ...]
    format: str = TOMBSTONE_MANIFEST_V2_FORMAT

    def __post_init__(self) -> None:
        if self.format != TOMBSTONE_MANIFEST_V2_FORMAT:
            raise TombstoneManifestV2Error("manifest format is not v2")
        _strict_identity(self.organization, field_name="organization")
        _strict_identity(self.super_name, field_name="super_name")
        _strict_identity(self.simple_name, field_name="simple_name")
        base = _strict_integer(
            self.base_snapshot_version,
            field_name="base_snapshot_version",
            minimum=0,
            maximum=MAX_JSON_EXACT_INTEGER - 1,
        )
        successor = _strict_integer(
            self.snapshot_version,
            field_name="snapshot_version",
            minimum=1,
        )
        if successor != base + 1:
            raise TombstoneManifestV2Error(
                "snapshot_version must be the immediate successor of "
                "base_snapshot_version"
            )
        total = _strict_integer(
            self.total_rows,
            field_name="total_rows",
            minimum=1,
        )
        if not isinstance(self.segments, tuple):
            raise TombstoneManifestV2Error("segments must be an immutable tuple")
        if not self.segments:
            raise TombstoneManifestV2Error(
                "an active v2 manifest must contain at least one segment"
            )
        if len(self.segments) > MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
            raise TombstoneManifestV2Error("manifest contains too many segments")
        if any(not isinstance(segment, TombstoneSegment) for segment in self.segments):
            raise TombstoneManifestV2Error(
                "segments must contain TombstoneSegment values"
            )
        files = tuple(segment.file for segment in self.segments)
        if files != tuple(sorted(files)):
            raise TombstoneManifestV2Error(
                "segments must be strictly ordered by file"
            )
        if len(files) != len(set(files)):
            raise TombstoneManifestV2Error("segment files must be unique")
        if sum(segment.rows for segment in self.segments) != total:
            raise TombstoneManifestV2Error(
                "total_rows does not equal the sum of segment rows"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format": self.format,
            "organization": self.organization,
            "super_name": self.super_name,
            "simple_name": self.simple_name,
            "base_snapshot_version": self.base_snapshot_version,
            "snapshot_version": self.snapshot_version,
            "total_rows": self.total_rows,
            "segments": [segment.to_dict() for segment in self.segments],
        }

    def canonical_bytes(self) -> bytes:
        return canonical_tombstone_manifest_v2_bytes(self.to_dict())

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def canonical_tombstone_manifest_v2_bytes(value: Mapping[str, Any]) -> bytes:
    """Return the deterministic UTF-8 JSON representation used by root seals."""
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeEncodeError) as exc:
        raise TombstoneManifestV2Error(
            "manifest is not canonical JSON data"
        ) from None
    if len(encoded) > MAX_TOMBSTONE_MANIFEST_V2_BYTES:
        raise TombstoneManifestV2Error("canonical manifest is too large")
    return encoded


def _reject_json_constant(value: str) -> object:
    raise TombstoneManifestV2Error(
        f"manifest contains a non-JSON numeric constant: {value}"
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise TombstoneManifestV2Error(
                f"manifest JSON contains duplicate object key {key!r}"
            )
        result[key] = value
    return result


def _decode_manifest(raw: _RawManifest) -> tuple[Mapping[str, Any], Optional[bytes]]:
    if isinstance(raw, Mapping):
        return raw, None
    if isinstance(raw, str):
        try:
            encoded = raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise TombstoneManifestV2Error("manifest JSON is not valid UTF-8") from None
    elif isinstance(raw, (bytes, bytearray, memoryview)):
        encoded = bytes(raw)
    else:
        raise TypeError("manifest must be a Mapping, UTF-8 string, or bytes")
    if not encoded or len(encoded) > MAX_TOMBSTONE_MANIFEST_V2_BYTES:
        raise TombstoneManifestV2Error("manifest JSON size is outside the supported bound")
    if encoded.startswith(b"\xef\xbb\xbf"):
        raise TombstoneManifestV2Error("manifest JSON must not contain a UTF-8 BOM")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TombstoneManifestV2Error("manifest JSON is not valid UTF-8") from None
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except TombstoneManifestV2Error:
        raise
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError) as exc:
        raise TombstoneManifestV2Error("manifest is not valid JSON") from None
    if not isinstance(decoded, Mapping):
        raise TombstoneManifestV2Error("manifest JSON root must be an object")
    return decoded, encoded


def _require_exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    label: str,
) -> None:
    if any(not isinstance(key, str) for key in value):
        raise TombstoneManifestV2Error(f"{label} keys must be strings")
    actual = frozenset(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise TombstoneManifestV2Error(
            f"{label} has invalid fields (missing={missing!r}, unknown={unknown!r})"
        )


def _segment_from_mapping(value: object, *, index: int) -> TombstoneSegment:
    if not isinstance(value, Mapping):
        raise TombstoneManifestV2Error(f"segments[{index}] must be an object")
    _require_exact_fields(value, _SEGMENT_FIELDS, label=f"segments[{index}]")
    return TombstoneSegment(
        file=value["file"],
        rows=value["rows"],
        file_size=value["file_size"],
        digest=value["digest"],
    )


def _manifest_from_mapping(value: Mapping[str, Any]) -> TombstoneManifestV2:
    _require_exact_fields(value, _MANIFEST_FIELDS, label="manifest")
    raw_segments = value["segments"]
    if not isinstance(raw_segments, list):
        raise TombstoneManifestV2Error("segments must be a JSON array")
    if len(raw_segments) > MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
        raise TombstoneManifestV2Error("manifest contains too many segments")
    segments = tuple(
        _segment_from_mapping(segment, index=index)
        for index, segment in enumerate(raw_segments)
    )
    return TombstoneManifestV2(
        format=value["format"],
        organization=value["organization"],
        super_name=value["super_name"],
        simple_name=value["simple_name"],
        base_snapshot_version=value["base_snapshot_version"],
        snapshot_version=value["snapshot_version"],
        total_rows=value["total_rows"],
        segments=segments,
    )


def _validate_manifest_expectations(
    manifest: TombstoneManifestV2,
    *,
    expected_organization: Optional[str],
    expected_super_name: Optional[str],
    expected_simple_name: Optional[str],
    pinned_snapshot_version: Optional[int],
    expected_total_rows: Optional[int],
    expected_digest: Optional[str],
    expected_segment_prefix: Optional[str],
) -> TombstoneManifestV2:
    for field_name, expected, actual in (
        ("organization", expected_organization, manifest.organization),
        ("super_name", expected_super_name, manifest.super_name),
        ("simple_name", expected_simple_name, manifest.simple_name),
    ):
        if expected is not None:
            _strict_identity(expected, field_name=field_name)
            if actual != expected:
                raise TombstoneManifestV2Error(
                    f"manifest {field_name} does not match the pinned table"
                )
    if pinned_snapshot_version is not None:
        pinned = _strict_integer(
            pinned_snapshot_version,
            field_name="pinned_snapshot_version",
            minimum=1,
        )
        # A pure append may carry an unchanged manifest forward.  The manifest
        # versions describe creation lineage, not the version of every later
        # snapshot that references it.
        if manifest.snapshot_version > pinned:
            raise TombstoneManifestV2Error(
                "manifest was created after the pinned snapshot"
            )
    if expected_total_rows is not None:
        expected_rows = _strict_integer(
            expected_total_rows,
            field_name="expected_total_rows",
            minimum=1,
        )
        if manifest.total_rows != expected_rows:
            raise TombstoneManifestV2Error(
                "manifest total_rows does not match the snapshot"
            )
    if expected_digest is not None:
        root_digest = _strict_digest(
            expected_digest,
            field_name="expected_digest",
        )
        if manifest.digest() != root_digest:
            raise TombstoneManifestV2Error(
                "manifest canonical SHA-256 does not match the snapshot"
            )
    if expected_segment_prefix is not None:
        prefix = validate_logical_storage_path(
            expected_segment_prefix.rstrip("/"),
            field_name="expected_segment_prefix",
        ) + "/"
        if any(not segment.file.startswith(prefix) for segment in manifest.segments):
            raise TombstoneManifestV2Error(
                "manifest segment escapes the expected table prefix"
            )
    return manifest


def load_tombstone_manifest_v2(
    raw: _RawManifest,
    *,
    expected_organization: Optional[str] = None,
    expected_super_name: Optional[str] = None,
    expected_simple_name: Optional[str] = None,
    pinned_snapshot_version: Optional[int] = None,
    expected_total_rows: Optional[int] = None,
    expected_digest: Optional[str] = None,
    expected_segment_prefix: Optional[str] = None,
    require_canonical_json: bool = False,
) -> TombstoneManifestV2:
    """Parse and fully validate one v2 manifest.

    ``pinned_snapshot_version`` is an upper bound, not an equality check: pure
    appends intentionally carry an unchanged manifest into later snapshots.
    The root digest always covers canonical JSON, so insignificant whitespace
    in a storage adapter's JSON encoding is harmless unless
    ``require_canonical_json`` is requested at a stricter recovery boundary.
    """
    decoded, source_bytes = _decode_manifest(raw)
    manifest = _manifest_from_mapping(decoded)
    if require_canonical_json:
        if source_bytes is None:
            raise TombstoneManifestV2Error(
                "canonical JSON validation requires raw JSON bytes or text"
            )
        if source_bytes != manifest.canonical_bytes():
            raise TombstoneManifestV2Error(
                "manifest JSON is not in canonical form"
            )
    return _validate_manifest_expectations(
        manifest,
        expected_organization=expected_organization,
        expected_super_name=expected_super_name,
        expected_simple_name=expected_simple_name,
        pinned_snapshot_version=pinned_snapshot_version,
        expected_total_rows=expected_total_rows,
        expected_digest=expected_digest,
        expected_segment_prefix=expected_segment_prefix,
    )


def validate_tombstone_manifest_v2(
    value: Union[TombstoneManifestV2, _RawManifest],
    **expectations: Any,
) -> TombstoneManifestV2:
    """Validate an existing object or load a raw Mapping/JSON representation."""
    if not isinstance(value, TombstoneManifestV2):
        return load_tombstone_manifest_v2(value, **expectations)
    unexpected = set(expectations) - {
        "expected_organization",
        "expected_super_name",
        "expected_simple_name",
        "pinned_snapshot_version",
        "expected_total_rows",
        "expected_digest",
        "expected_segment_prefix",
    }
    if unexpected:
        raise TypeError(f"unexpected manifest expectations: {sorted(unexpected)!r}")
    return _validate_manifest_expectations(
        value,
        expected_organization=expectations.get("expected_organization"),
        expected_super_name=expectations.get("expected_super_name"),
        expected_simple_name=expectations.get("expected_simple_name"),
        pinned_snapshot_version=expectations.get("pinned_snapshot_version"),
        expected_total_rows=expectations.get("expected_total_rows"),
        expected_digest=expectations.get("expected_digest"),
        expected_segment_prefix=expectations.get("expected_segment_prefix"),
    )


def validate_tombstone_segment_observation(
    segment: TombstoneSegment,
    *,
    file_size: object,
    rows: object,
    digest: object,
) -> TombstoneSegment:
    """Match one decoded segment observation to its manifest descriptor.

    The caller obtains ``file_size`` from the immutable object and computes
    ``rows`` plus ``digest`` only after decoding and validating the deletion-
    vector schema.  This helper intentionally never hashes encoded Parquet
    bytes: ``segment.digest`` seals logical DV rows with the established v1
    canonical digest algorithm.
    """
    if not isinstance(segment, TombstoneSegment):
        raise TypeError("segment must be a TombstoneSegment")
    observed_size = _strict_integer(
        file_size,
        field_name="observed segment file_size",
        minimum=1,
    )
    observed_rows = _strict_integer(
        rows,
        field_name="observed segment rows",
        minimum=1,
    )
    observed_digest = _strict_digest(
        digest,
        field_name="observed segment digest",
    )
    if observed_size != segment.file_size:
        raise TombstoneManifestV2Error(
            f"segment {segment.file!r} file_size does not match the manifest"
        )
    if observed_rows != segment.rows:
        raise TombstoneManifestV2Error(
            f"segment {segment.file!r} row count does not match the manifest"
        )
    if observed_digest != segment.digest:
        raise TombstoneManifestV2Error(
            f"segment {segment.file!r} logical digest does not match the manifest"
        )
    return segment


# Short module-scoped spellings for callers that import this module directly.
load = load_tombstone_manifest_v2
validate = validate_tombstone_manifest_v2


__all__ = [
    "MAX_JSON_EXACT_INTEGER",
    "MAX_TOMBSTONE_MANIFEST_V2_BYTES",
    "MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS",
    "TOMBSTONE_FORMAT_V1",
    "TOMBSTONE_FORMAT_V2",
    "TOMBSTONE_FORMAT_V3",
    "TOMBSTONE_MANIFEST_V2_FORMAT",
    "NormalizedSnapshotTombstoneState",
    "TombstoneManifestV2",
    "TombstoneManifestV2Error",
    "TombstoneSegment",
    "canonical_tombstone_manifest_v2_bytes",
    "load",
    "load_tombstone_manifest_v2",
    "normalize_snapshot_tombstone_state",
    "validate",
    "validate_logical_storage_path",
    "validate_snapshot_tombstone_state",
    "tombstone_v3_artifact_digest",
    "validate_tombstone_manifest_v2",
    "validate_tombstone_segment_observation",
]
