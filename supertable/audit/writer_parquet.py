# route: supertable.audit.writer_parquet
"""
Warm-tier audit writer — Parquet files on the configured storage backend.

This is the system of record for audit events. Files are append-only,
partitioned by date, and named with instance_id + UUID for safe
concurrent writes from multiple server instances.

Partition layout:
    {storage_root}/{org}/__audit__/year=YYYY/month=MM/day=DD/
        audit_{timestamp}_{instance_id}_{publication_sha256}.parquet

Chain proofs:
    {storage_root}/{org}/__audit__/_chain/chain_{date}.json

Compliance: DORA Art. 12 (record keeping, 5+ year retention),
            SOC 2 CC7.3 (forensic integrity).
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import struct
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# PyArrow is a required dependency for SuperTable — safe to import at module level.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

from supertable import redis_keys as RK
from supertable.audit.chain import (
    GENESIS_HASH,
    MerkleProof,
    compute_file_hash,
    verify_merkle_proof,
)
from supertable.audit.events import current_instance_id
from supertable.audit.diagnostics import safe_audit_error_type
from supertable.storage.storage_interface import ObjectMetadata


class AuditReadError(RuntimeError):
    """An audit partition could not be read completely and safely."""


class AuditReadLimitError(AuditReadError, ValueError):
    """An audit partition exceeds a hard reader safety budget."""


class AuditStorageUnavailable(RuntimeError):
    """The configured audit storage backend could not be resolved."""


class AuditArchiveWriteError(RuntimeError):
    """An audit archive object could not be encoded or durably written."""


# Audit files are written in batches of roughly 1,000 rows.  These ceilings
# admit busy production days while bounding every attacker-controlled axis
# before or during decode.  Callers may tighten only the event limit.
_MAX_AUDIT_FILES_PER_DAY = 8_192
_MAX_AUDIT_EVENTS_PER_READ = 250_000
_MAX_AUDIT_FILE_RAW_BYTES = 128 * 1024 * 1024
_MAX_AUDIT_TOTAL_RAW_BYTES = 256 * 1024 * 1024
_MAX_AUDIT_FILE_FOOTER_BYTES = 2 * 1024 * 1024
_MAX_AUDIT_TOTAL_FOOTER_BYTES = 16 * 1024 * 1024
_MAX_AUDIT_SCHEMA_BYTES = 64 * 1024
_MAX_AUDIT_TOTAL_SCHEMA_BYTES = 4 * 1024 * 1024
_MAX_AUDIT_ROW_GROUPS_PER_FILE = 2_048
_MAX_AUDIT_TOTAL_ROW_GROUPS = 8_192
_MAX_AUDIT_TOTAL_COLUMN_CHUNKS = 180_224
_MAX_AUDIT_FILE_DECODED_BYTES = 128 * 1024 * 1024
_MAX_AUDIT_DECODED_BYTES = 256 * 1024 * 1024
_MAX_AUDIT_EVENT_FIELD_BYTES = 64 * 1024
_MAX_AUDIT_EVENT_BYTES = 96 * 1024
_AUDIT_EVENT_RETAINED_OVERHEAD_BYTES = 2 * 1024
_MAX_AUDIT_PROOF_RAW_BYTES = 1024 * 1024
_MAX_AUDIT_CLOSE_MANIFEST_RAW_BYTES = 1024 * 1024
_MAX_AUDIT_PROOF_INSTANCES = 4_096
_MAX_AUDIT_PROOF_COUNTER = (1 << 63) - 1
_AUDIT_DECODE_BATCH_ROWS = 64
_MAX_AUDIT_LOGICAL_PATH_BYTES = 2_048
_MAX_AUDIT_PARTITIONS = 10_000

_YEAR_COMPONENT_RE = re.compile(r"year=(\d{4})")
_MONTH_COMPONENT_RE = re.compile(r"month=(\d{2})")
_DAY_COMPONENT_RE = re.compile(r"day=(\d{2})")
_AUDIT_FILE_RE = re.compile(r"audit_[A-Za-z0-9._-]{1,220}\.parquet")
_PROOF_PUBLICATION_LOCK = threading.Lock()


class _BoundedSpillWriter:
    """Binary sink that refuses writes beyond one sealed object size."""

    def __init__(self, file_obj: Any, byte_limit: int):
        self._file_obj = file_obj
        self._byte_limit = byte_limit
        self.written = 0

    def write(self, data: Any) -> int:
        try:
            view = memoryview(data)
        except TypeError:
            raise AuditReadError(
                "audit storage download returned non-binary data"
            ) from None
        if self.written + len(view) > self._byte_limit:
            raise AuditReadLimitError(
                "audit object exceeded its sealed download size"
            )
        offset = 0
        while offset < len(view):
            result = self._file_obj.write(view[offset:])
            if result is None:
                offset = len(view)
                break
            if type(result) is not int or result <= 0 or result > len(view) - offset:
                raise AuditReadError("audit spill sink made invalid progress")
            offset += result
        self.written += len(view)
        return len(view)

    def flush(self) -> None:
        self._file_obj.flush()

    def seek(self, *args: Any) -> int:
        return self._file_obj.seek(*args)

    def tell(self) -> int:
        return self._file_obj.tell()

    def fileno(self) -> int:
        return self._file_obj.fileno()


def _validated_object_metadata(
    value: Any,
    *,
    minimum_size: int = 12,
    maximum_size: int = _MAX_AUDIT_FILE_RAW_BYTES,
) -> ObjectMetadata:
    """Validate a backend-provided immutable object seal without stringifying it."""
    if not isinstance(value, ObjectMetadata):
        raise AuditReadError("audit storage returned invalid object metadata")
    if type(value.size) is not int or value.size < minimum_size:
        raise AuditReadError("audit object size is invalid")
    if value.size > maximum_size:
        raise AuditReadLimitError("audit object exceeds its raw-byte limit")
    if type(value.last_modified_ns) is not int or value.last_modified_ns < 0:
        raise AuditReadError("audit object timestamp seal is invalid")
    for field_value in (value.version, value.etag, value.checksum_sha256):
        if not isinstance(field_value, str):
            raise AuditReadError("audit object identity seal is invalid")
        try:
            field_size = len(field_value.encode("utf-8"))
        except UnicodeEncodeError:
            raise AuditReadError(
                "audit object identity seal is invalid"
            ) from None
        if field_size > 4_096:
            raise AuditReadLimitError("audit object identity seal is oversized")
    try:
        identity = value.identity_token()
    except Exception:
        raise AuditReadError("audit object identity seal is invalid") from None
    if not isinstance(identity, str) or not identity:
        raise AuditReadError("audit object has no stable identity seal")
    try:
        identity_size = len(identity.encode("utf-8"))
    except UnicodeEncodeError:
        raise AuditReadError("audit object identity seal is invalid") from None
    if identity_size > 16_384:
        raise AuditReadLimitError("audit object identity seal is oversized")
    return value


def _read_sealed_bytes(
    storage: Any,
    path: str,
    *,
    maximum_size: int,
    minimum_size: int = 1,
) -> bytes:
    """Download one identity-sealed object under an exact hard ceiling."""
    stat_object = getattr(storage, "stat_object", None)
    download_to_file = getattr(storage, "download_to_file", None)
    if not callable(stat_object) or not callable(download_to_file):
        raise AuditReadError("audit storage lacks sealed streaming-read support")
    observed = _validated_object_metadata(
        stat_object(path), minimum_size=minimum_size, maximum_size=maximum_size,
    )
    buffer = io.BytesIO()
    sink = _BoundedSpillWriter(buffer, observed.size)
    downloaded = download_to_file(
        path,
        sink,
        expected=observed,
        chunk_size=min(256 * 1024, maximum_size),
    )
    if (
        type(downloaded) is not int
        or downloaded != observed.size
        or sink.written != observed.size
        or buffer.tell() != observed.size
    ):
        raise AuditReadError("audit storage returned an incomplete sealed object")
    resealed = _validated_object_metadata(
        stat_object(path), minimum_size=minimum_size, maximum_size=maximum_size,
    )
    if resealed != observed:
        raise AuditReadError("audit object identity changed during download")
    return buffer.getvalue()


def _decode_chain_proof(
    raw: bytes,
    *,
    org: str,
    expected_date: str,
) -> MerkleProof:
    """Decode the closed proof schema after a bounded, sealed download."""

    def _object_without_duplicates(pairs: List[Any]) -> Dict[str, Any]:
        if len(pairs) > _MAX_AUDIT_PROOF_INSTANCES:
            raise AuditReadLimitError(
                "audit chain proof object exceeds its member limit"
            )
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if not isinstance(key, str) or key in result:
                raise AuditReadError(
                    "audit chain proof contains an invalid object key"
                )
            result[key] = value
        return result

    try:
        data = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object_without_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise AuditReadError("audit chain proof encoding is invalid") from None
    if not isinstance(data, dict) or set(data) != {
        "date",
        "instances",
        "merkle_root",
        "total_events",
        "created_ms",
    }:
        raise AuditReadError("audit chain proof schema is invalid")
    proof_date = data["date"]
    if (
        not isinstance(proof_date, str)
        or _normalized_proof_date(proof_date) != expected_date
    ):
        raise AuditReadError("audit chain proof date is inconsistent")
    instances = data["instances"]
    if not isinstance(instances, dict):
        raise AuditReadError("audit chain proof instances are invalid")
    if len(instances) > _MAX_AUDIT_PROOF_INSTANCES:
        raise AuditReadLimitError(
            "audit chain proof exceeds its instance limit"
        )
    instance_event_total = 0
    for instance_id, entry in instances.items():
        try:
            RK.audit_chain_head(org, instance_id)
        except ValueError:
            raise AuditReadError(
                "audit chain proof instance identity is invalid"
            ) from None
        if not isinstance(entry, dict) or set(entry) != {
            "head", "batches", "events",
        }:
            raise AuditReadError("audit chain proof instance schema is invalid")
        head = entry["head"]
        batches = entry["batches"]
        events = entry["events"]
        if (
            not isinstance(head, str)
            or len(head) != 64
            or any(ch not in "0123456789abcdef" for ch in head)
        ):
            raise AuditReadError("audit chain proof hash is invalid")
        if (
            type(batches) is not int
            or batches < 0
            or batches > _MAX_AUDIT_PROOF_COUNTER
        ):
            raise AuditReadError("audit chain proof batch count is invalid")
        if (
            type(events) is not int
            or events < 0
            or events > _MAX_AUDIT_PROOF_COUNTER
        ):
            raise AuditReadError("audit chain proof event count is invalid")
        if batches == 0 and head != GENESIS_HASH:
            raise AuditReadError("empty audit chain proof has a non-genesis head")
        instance_event_total += events
    merkle_root = data["merkle_root"]
    if (
        not isinstance(merkle_root, str)
        or len(merkle_root) != 64
        or any(ch not in "0123456789abcdef" for ch in merkle_root)
    ):
        raise AuditReadError("audit chain proof root is invalid")
    for counter_name in ("total_events", "created_ms"):
        counter = data[counter_name]
        if (
            type(counter) is not int
            or counter < 0
            or counter > _MAX_AUDIT_PROOF_COUNTER
        ):
            raise AuditReadError("audit chain proof counter is invalid")
    if instance_event_total != data["total_events"]:
        raise AuditReadError(
            "audit chain proof event total does not match its instances"
        )
    return MerkleProof.from_dict(data)


# ---------------------------------------------------------------------------
# Parquet schema — mirrors AuditEvent fields exactly
# ---------------------------------------------------------------------------

_PARQUET_SCHEMA = None


def _get_schema():
    """Lazily build the PyArrow schema (avoids import-time pa dependency check)."""
    global _PARQUET_SCHEMA
    if _PARQUET_SCHEMA is not None:
        return _PARQUET_SCHEMA
    if pa is None:
        raise ImportError("pyarrow is required for audit Parquet writer")
    _PARQUET_SCHEMA = pa.schema([
        ("event_id", pa.string()),
        ("timestamp_ms", pa.int64()),
        ("category", pa.string()),
        ("action", pa.string()),
        ("severity", pa.string()),
        ("actor_type", pa.string()),
        ("actor_id", pa.string()),
        ("actor_username", pa.string()),
        ("actor_ip", pa.string()),
        ("actor_user_agent", pa.string()),
        ("organization", pa.string()),
        ("super_name", pa.string()),
        ("correlation_id", pa.string()),
        ("session_id", pa.string()),
        ("server", pa.string()),
        ("resource_type", pa.string()),
        ("resource_id", pa.string()),
        ("detail", pa.string()),
        ("outcome", pa.string()),
        ("reason", pa.string()),
        ("chain_hash", pa.string()),
        ("instance_id", pa.string()),
    ])
    return _PARQUET_SCHEMA


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _validated_partition_datetime(
    year: int,
    month: int,
    day: int,
) -> datetime:
    if any(type(value) is not int for value in (year, month, day)):
        raise ValueError("audit partition date must contain integers")
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        raise ValueError("audit partition date is invalid") from None


def _normalized_proof_date(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("audit proof date must be a string")
    if re.fullmatch(r"\d{8}", value):
        clean = value
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        clean = value.replace("-", "")
    else:
        raise ValueError("audit proof date must use YYYYMMDD or YYYY-MM-DD")
    try:
        parsed = datetime.strptime(clean, "%Y%m%d")
    except ValueError:
        raise ValueError("audit proof date is invalid") from None
    return parsed.strftime("%Y%m%d")


def _require_listed_child(
    parent: str,
    candidate: Any,
    *,
    component_re: re.Pattern[str],
) -> str:
    if not isinstance(candidate, str):
        raise AuditReadError("audit storage listing returned a non-string path")
    try:
        encoded_size = len(candidate.encode("utf-8"))
    except UnicodeEncodeError:
        raise AuditReadError(
            "audit storage listing returned an invalid path"
        ) from None
    prefix = f"{parent}/"
    if (
        encoded_size > _MAX_AUDIT_LOGICAL_PATH_BYTES
        or not candidate.startswith(prefix)
    ):
        raise AuditReadError("audit storage listing escaped its partition")
    component = candidate[len(prefix):]
    if not component_re.fullmatch(component):
        raise AuditReadError("audit storage listing returned an invalid child")
    return candidate


def _audit_base(org: str) -> str:
    # Reuse the canonical org-segment authority rather than maintaining a
    # weaker filesystem-specific validator.  This rejects absolute/traversal,
    # separators, controls, uppercase aliases, sentinels, and oversize values.
    RK.audit_stream(org)
    return f"{org}/__audit__"


def _partition_dir(org: str, dt: datetime) -> str:
    return (
        f"{_audit_base(org)}/year={dt.year:04d}/"
        f"month={dt.month:02d}/day={dt.day:02d}"
    )


def _batch_filename(dt: datetime) -> str:
    """Generate a unique, concurrency-safe filename."""
    ts = dt.strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"audit_{ts}_{current_instance_id()}_{uid}.parquet"


class AuditArchiveCollisionError(RuntimeError):
    """A stable audit publication path contains different bytes."""


def _chain_proof_path(org: str, date_str: str) -> str:
    """Path for daily chain proof file: _chain/chain_YYYYMMDD.json"""
    clean = _normalized_proof_date(date_str)
    return f"{_audit_base(org)}/_chain/chain_{clean}.json"


def _chain_close_path(org: str, date_str: str) -> str:
    clean = _normalized_proof_date(date_str)
    return f"{_audit_base(org)}/_chain/closed_{clean}.json"


# ---------------------------------------------------------------------------
# ParquetAuditWriter
# ---------------------------------------------------------------------------

class ParquetAuditWriter:
    """Write audit event batches to Parquet files on the storage backend.

    Uses the same StorageInterface as the rest of SuperTable — so audit
    files go to the same S3/MinIO/Azure/GCP/local backend.

    Thread-safety: NOT thread-safe. The AuditLogger's background worker
    serializes all calls.
    """

    def __init__(self, storage=None):
        """Initialize with a storage backend.

        If storage is None, it is lazily resolved from storage_factory
        on first write (avoids import-time circular dependencies).
        """
        self._storage = storage
        self._storage_resolved = storage is not None

    def _get_storage(self):
        if not self._storage_resolved:
            try:
                from supertable.storage.storage_factory import get_storage
                self._storage = get_storage()
                self._storage_resolved = True
            except Exception as exc:
                logger.error(
                    "[audit-parquet] storage resolution failed; error_type=%s",
                    safe_audit_error_type(exc),
                )
                raise AuditStorageUnavailable(
                    "audit storage is unavailable"
                ) from None
        return self._storage

    # ── Write batch ────────────────────────────────────────

    def write_batch(
        self,
        org: str,
        events: List[Dict[str, Any]],
        *,
        publication_id: str = "",
        published_at_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Write a batch of event dicts to a Parquet file.

        Returns:
            {
                "path": "acme/__audit__/year=2025/.../audit_..._xxx.parquet",
                "file_hash": "sha256...",
                "event_count": 42,
                "bytes_written": 12345,
            }
        """
        if not events:
            return {
                "path": "",
                "file_hash": "",
                "event_count": 0,
                "bytes_written": 0,
                "publication_id": "",
            }

        if pa is None or pq is None:
            raise ImportError("pyarrow is required for audit Parquet writer")

        schema = _get_schema()
        storage = self._get_storage()

        try:
            # Build columnar arrays from event dicts.
            columns: Dict[str, List[Any]] = {
                field.name: [] for field in schema
            }
            for event in events:
                for field_name in columns:
                    val = event.get(field_name, "")
                    if field_name == "timestamp_ms":
                        columns[field_name].append(int(val) if val else 0)
                    else:
                        columns[field_name].append(
                            str(val) if val is not None else ""
                        )

            table = pa.table(columns, schema=schema)

            # Write to an in-memory buffer.
            buf = io.BytesIO()
            pq.write_table(
                table,
                buf,
                compression="snappy",
                write_statistics=True,
            )
            parquet_bytes = buf.getvalue()
        except Exception:
            raise AuditArchiveWriteError(
                "audit event batch could not be encoded"
            ) from None

        # Compute file hash for chain integrity
        file_hash = compute_file_hash(parquet_bytes)

        # A caller-supplied publication ID makes retries idempotent.  Legacy
        # callers still receive a unique ID, but the audit worker always passes
        # its content/chain digest and a stable batch timestamp.
        stable_publication = bool(publication_id)
        if stable_publication:
            if (
                len(publication_id) != 64
                or any(ch not in "0123456789abcdef" for ch in publication_id)
            ):
                raise ValueError("audit publication_id must be canonical SHA-256")
        else:
            publication_id = uuid.uuid4().hex + uuid.uuid4().hex
        if published_at_ms is None:
            now = datetime.now(timezone.utc)
        else:
            if (
                isinstance(published_at_ms, bool)
                or not isinstance(published_at_ms, int)
                or published_at_ms < 0
            ):
                raise ValueError("audit publication timestamp is invalid")
            try:
                now = datetime.fromtimestamp(
                    published_at_ms / 1_000, tz=timezone.utc,
                )
            except (OverflowError, OSError, ValueError):
                raise ValueError(
                    "audit publication timestamp is outside the supported range"
                ) from None
        partition = _partition_dir(org, now)
        publication_instance = events[0].get("instance_id") or current_instance_id()
        RK.audit_chain_head(org, publication_instance)
        if any(
            event.get("instance_id") not in {None, "", publication_instance}
            for event in events
        ):
            raise ValueError("audit publication spans multiple instances")
        filename = (
            f"audit_{now.strftime('%Y%m%d_%H%M%S%f')}_"
            f"{publication_instance}_{publication_id}.parquet"
        )
        full_path = f"{partition}/{filename}"

        reconciled = False
        if stable_publication:
            create = getattr(storage, "create_bytes_if_absent", None)
            if not callable(create):
                raise AuditArchiveCollisionError(
                    "audit storage lacks immutable conditional-create support"
                )
            try:
                created = create(full_path, parquet_bytes)
                if type(created) is not bool:
                    raise AuditArchiveCollisionError(
                        "audit conditional-create result is invalid"
                    )
            except Exception:
                try:
                    existing = _read_sealed_bytes(
                        storage,
                        full_path,
                        maximum_size=_MAX_AUDIT_FILE_RAW_BYTES,
                    )
                except Exception:
                    raise AuditArchiveCollisionError(
                        "audit publication could not be reconciled"
                    ) from None
                if existing != parquet_bytes:
                    raise AuditArchiveCollisionError(
                        "audit publication path contains different bytes"
                    ) from None
                created = False
                reconciled = True
            else:
                if not created:
                    try:
                        existing = _read_sealed_bytes(
                            storage,
                            full_path,
                            maximum_size=_MAX_AUDIT_FILE_RAW_BYTES,
                        )
                    except Exception:
                        raise AuditArchiveWriteError(
                            "audit publication readback failed"
                        ) from None
                    if existing != parquet_bytes:
                        raise AuditArchiveCollisionError(
                            "audit publication path contains different bytes"
                        )
                    reconciled = True
            if not created:
                ensure_durable = getattr(storage, "ensure_bytes_durable", None)
                if callable(ensure_durable):
                    try:
                        ensure_durable(full_path)
                    except Exception:
                        raise AuditArchiveWriteError(
                            "audit publication durability confirmation failed"
                        ) from None
            try:
                confirmed = _read_sealed_bytes(
                    storage,
                    full_path,
                    maximum_size=_MAX_AUDIT_FILE_RAW_BYTES,
                )
            except Exception:
                raise AuditArchiveWriteError(
                    "audit publication readback failed"
                ) from None
            if confirmed != parquet_bytes:
                raise AuditArchiveCollisionError(
                    "audit publication readback is not exact"
                )
        else:
            try:
                storage.write_bytes(full_path, parquet_bytes)
            except Exception as exc:
                logger.error(
                    "[audit-parquet] write_bytes failed; error_type=%s",
                    safe_audit_error_type(exc),
                )
                raise AuditArchiveWriteError(
                    "audit archive write failed"
                ) from None

        logger.debug(
            "[audit-parquet] Written %d events (%d bytes)",
            len(events), len(parquet_bytes),
        )

        return {
            "path": full_path,
            "file_hash": file_hash,
            "event_count": len(events),
            "bytes_written": len(parquet_bytes),
            "publication_id": publication_id,
            "reconciled": reconciled,
        }

    # ── Chain proof ────────────────────────────────────────

    def save_chain_proof(self, org: str, proof: MerkleProof) -> bool:
        """Save the daily Merkle proof as a JSON file in storage."""
        if not proof.date:
            return False
        path = _chain_proof_path(org, proof.date)
        try:
            raw = json.dumps(
                proof.to_dict(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if len(raw) > _MAX_AUDIT_PROOF_RAW_BYTES:
                raise AuditReadLimitError(
                    "audit chain proof exceeds its raw-byte limit"
                )
            validated = _decode_chain_proof(
                raw,
                org=org,
                expected_date=_normalized_proof_date(proof.date),
            )
            if not verify_merkle_proof(validated).get("valid", False):
                raise AuditReadError("audit chain proof root is inconsistent")
            storage = self._get_storage()
            with _PROOF_PUBLICATION_LOCK:
                create = getattr(storage, "create_bytes_if_absent", None)
                if not callable(create):
                    raise AuditReadError(
                        "audit storage lacks immutable conditional-create support"
                    )
                try:
                    created = create(path, raw)
                    if type(created) is not bool:
                        raise AuditReadError(
                            "audit conditional-create result is invalid"
                        )
                except Exception:
                    # A provider timeout may follow an exact successful create.
                    # Only exact sealed bytes reconcile that ambiguous result.
                    existing = _read_sealed_bytes(
                        storage,
                        path,
                        maximum_size=_MAX_AUDIT_PROOF_RAW_BYTES,
                        minimum_size=2,
                    )
                    if existing != raw:
                        raise AuditArchiveCollisionError(
                            "audit chain proof path contains different bytes"
                        ) from None
                    created = False
                else:
                    if not created:
                        existing = _read_sealed_bytes(
                            storage,
                            path,
                            maximum_size=_MAX_AUDIT_PROOF_RAW_BYTES,
                            minimum_size=2,
                        )
                        if existing != raw:
                            raise AuditArchiveCollisionError(
                                "audit chain proof path contains different bytes"
                            )
                if not created:
                    ensure_durable = getattr(
                        storage, "ensure_bytes_durable", None,
                    )
                    if callable(ensure_durable):
                        ensure_durable(path)
                published = _read_sealed_bytes(
                    storage,
                    path,
                    maximum_size=_MAX_AUDIT_PROOF_RAW_BYTES,
                    minimum_size=2,
                )
                if published != raw:
                    raise AuditArchiveCollisionError(
                        "audit chain proof publication was not exact"
                    )
            logger.info("[audit-parquet] Saved chain proof")
            return True
        except Exception as exc:
            logger.error(
                "[audit-parquet] save_chain_proof failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            return False

    def load_chain_proof(
        self,
        org: str,
        date_str: str,
        *,
        strict: bool = True,
    ) -> Optional[MerkleProof]:
        """Load a daily proof, distinguishing absence from read corruption.

        ``strict=True`` raises :class:`AuditReadError` for every backend or
        decode failure.  A genuinely missing object remains ``None``.  The
        non-strict mode is retained only for compatibility with callers that
        historically treated all proof failures as absence.
        """
        if type(strict) is not bool:
            raise ValueError("audit proof strict mode must be a boolean")
        expected_date = _normalized_proof_date(date_str)
        path = _chain_proof_path(org, expected_date)
        try:
            storage = self._get_storage()
            stat_object = getattr(storage, "stat_object", None)
            download_to_file = getattr(storage, "download_to_file", None)
            if not callable(stat_object) or not callable(download_to_file):
                raise AuditReadError(
                    "audit storage lacks sealed streaming-read support"
                )
            observed = _validated_object_metadata(
                stat_object(path),
                minimum_size=2,
                maximum_size=_MAX_AUDIT_PROOF_RAW_BYTES,
            )
            buffer = io.BytesIO()
            sink = _BoundedSpillWriter(buffer, observed.size)
            downloaded = download_to_file(
                path,
                sink,
                expected=observed,
                chunk_size=256 * 1024,
            )
            if (
                type(downloaded) is not int
                or downloaded != observed.size
                or sink.written != observed.size
                or buffer.tell() != observed.size
            ):
                raise AuditReadError(
                    "audit storage returned an incomplete sealed proof"
                )
            resealed = _validated_object_metadata(
                stat_object(path),
                minimum_size=2,
                maximum_size=_MAX_AUDIT_PROOF_RAW_BYTES,
            )
            if resealed != observed:
                raise AuditReadError(
                    "audit chain proof identity changed during download"
                )
            return _decode_chain_proof(
                buffer.getvalue(),
                org=org,
                expected_date=expected_date,
            )
        except FileNotFoundError:
            return None
        except Exception as exc:
            logger.warning(
                "[audit-parquet] load_chain_proof failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            if strict:
                if isinstance(exc, AuditReadLimitError):
                    raise AuditReadLimitError(
                        "audit chain proof exceeded a reader safety limit"
                    ) from None
                raise AuditReadError(
                    "audit chain proof could not be read"
                ) from None
            return None

    def load_day_close_manifest(
        self,
        org: str,
        date_str: str,
        *,
        strict: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Load the immutable marker proving journal membership was closed."""
        if type(strict) is not bool:
            raise ValueError("audit close-manifest strict mode must be a boolean")
        expected_date = _normalized_proof_date(date_str)
        path = _chain_close_path(org, expected_date)
        try:
            raw = _read_sealed_bytes(
                self._get_storage(),
                path,
                maximum_size=_MAX_AUDIT_CLOSE_MANIFEST_RAW_BYTES,
                minimum_size=2,
            )
            try:
                value = json.loads(raw)
            except (TypeError, ValueError):
                raise AuditReadError(
                    "audit close manifest is invalid JSON"
                ) from None
            expected_fields = {
                "admitted", "admitted_bytes", "archive_bytes", "batch_ids",
                "close_requested_ms", "cutover_day", "cutover_ms", "date", "day",
                "format_version", "organization", "proof_hash",
                "receipt_count", "receipt_root", "version",
            }
            if not isinstance(value, dict) or set(value) != expected_fields:
                raise AuditReadError("audit close manifest schema is invalid")
            if (
                value.get("version") != 1
                or value.get("format_version") != 1
                or value.get("organization") != org
                or value.get("date") != datetime.strptime(
                    expected_date, "%Y%m%d",
                ).strftime("%Y-%m-%d")
            ):
                raise AuditReadError("audit close manifest identity is invalid")
            expected_day = int(datetime.strptime(
                expected_date, "%Y%m%d",
            ).replace(tzinfo=timezone.utc).timestamp()) // 86_400
            if value.get("day") != expected_day:
                raise AuditReadError("audit close manifest day is invalid")
            if (
                type(value.get("cutover_day")) is not int
                or not 0 <= value["cutover_day"] <= expected_day
            ):
                raise AuditReadError("audit close manifest cutover is invalid")
            for field, maximum in (
                ("admitted", 250_000),
                ("admitted_bytes", 512 * 1024 * 1024),
                ("archive_bytes", 512 * 1024 * 1024),
                ("close_requested_ms", (1 << 63) - 1),
                ("cutover_ms", (1 << 63) - 1),
                ("receipt_count", 8_192),
            ):
                item = value.get(field)
                if type(item) is not int or not 0 <= item <= maximum:
                    raise AuditReadError(
                        "audit close manifest counter is invalid"
                    )
            batch_ids = value.get("batch_ids")
            if (
                not isinstance(batch_ids, list)
                or len(batch_ids) != value["receipt_count"]
                or len(batch_ids) > 8_192
                or batch_ids != sorted(set(batch_ids))
                or any(
                    not isinstance(item, str)
                    or len(item) != 64
                    or any(ch not in "0123456789abcdef" for ch in item)
                    for item in batch_ids
                )
            ):
                raise AuditReadError(
                    "audit close manifest batch membership is invalid"
                )
            if (
                value["cutover_ms"] // 86_400_000 != value["cutover_day"]
                or value["cutover_ms"] > value["close_requested_ms"]
            ):
                raise AuditReadError("audit close manifest cutover time is invalid")
            for field in ("proof_hash", "receipt_root"):
                item = value.get(field)
                if (
                    not isinstance(item, str)
                    or len(item) != 64
                    or any(ch not in "0123456789abcdef" for ch in item)
                ):
                    raise AuditReadError("audit close manifest hash is invalid")
            canonical = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if canonical != raw:
                raise AuditReadError("audit close manifest is not canonical")
            value["manifest_hash"] = hashlib.sha256(raw).hexdigest()
            return value
        except FileNotFoundError:
            return None
        except Exception as exc:
            logger.warning(
                "[audit-parquet] load_day_close_manifest failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            if strict:
                if isinstance(exc, AuditReadLimitError):
                    raise AuditReadLimitError(
                        "audit close manifest exceeded a reader safety limit"
                    ) from None
                raise AuditReadError(
                    "audit close manifest could not be read safely"
                ) from None
            return None
    # ── Listing (for verification and export) ──────────────

    def list_partition_files(self, org: str, year: int, month: int, day: int) -> List[str]:
        """List all Parquet files in a day partition."""
        partition = _partition_dir(
            org, _validated_partition_datetime(year, month, day),
        )
        try:
            storage = self._get_storage()
            files = storage.list_files(partition, "*.parquet")
            if not isinstance(files, list):
                raise AuditReadError("audit storage listing is not a list")
            if len(files) > _MAX_AUDIT_FILES_PER_DAY:
                raise AuditReadLimitError(
                    "audit partition file count exceeds its safety limit"
                )
            validated = [
                _require_listed_child(
                    partition, file_path, component_re=_AUDIT_FILE_RE,
                )
                for file_path in files
            ]
            if len(set(validated)) != len(validated):
                raise AuditReadError("audit storage listing contains duplicates")
            return sorted(validated)
        except Exception as exc:
            logger.warning(
                "[audit-parquet] list_partition_files failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            if isinstance(exc, AuditReadLimitError):
                raise AuditReadLimitError(
                    "audit partition listing exceeded a reader safety limit"
                ) from None
            raise AuditReadError("audit partition could not be listed") from None

    def list_partitions(self, org: str) -> List[str]:
        """List all day-level partition paths for an organization.

        Traverses the year=/month=/day= directory hierarchy using
        list_files at each level (works on all storage backends).
        """
        base = _audit_base(org)
        partitions: List[str] = []
        try:
            storage = self._get_storage()
            year_entries = storage.list_files(base, "year=*")
            if (
                not isinstance(year_entries, list)
                or len(year_entries) > _MAX_AUDIT_PARTITIONS
            ):
                raise AuditReadLimitError(
                    "audit year listing exceeds its safety limit"
                )
            for year_path in year_entries:
                year_path = _require_listed_child(
                    base, year_path, component_re=_YEAR_COMPONENT_RE,
                )
                year = int(year_path.rsplit("/", 1)[-1][5:])
                if year < 1 or year > 9999:
                    raise AuditReadError("audit year partition is invalid")
                month_entries = storage.list_files(year_path, "month=*")
                if not isinstance(month_entries, list) or len(month_entries) > 12:
                    raise AuditReadLimitError(
                        "audit month listing exceeds its safety limit"
                    )
                for month_path in month_entries:
                    month_path = _require_listed_child(
                        year_path,
                        month_path,
                        component_re=_MONTH_COMPONENT_RE,
                    )
                    month = int(month_path.rsplit("/", 1)[-1][6:])
                    if month < 1 or month > 12:
                        raise AuditReadError("audit month partition is invalid")
                    day_entries = storage.list_files(month_path, "day=*")
                    if not isinstance(day_entries, list) or len(day_entries) > 31:
                        raise AuditReadLimitError(
                            "audit day listing exceeds its safety limit"
                        )
                    for day_path in day_entries:
                        day_path = _require_listed_child(
                            month_path,
                            day_path,
                            component_re=_DAY_COMPONENT_RE,
                        )
                        day = int(day_path.rsplit("/", 1)[-1][4:])
                        _validated_partition_datetime(year, month, day)
                        partitions.append(day_path)
                        if len(partitions) > _MAX_AUDIT_PARTITIONS:
                            raise AuditReadLimitError(
                                "audit partition count exceeds its safety limit"
                            )
            if len(set(partitions)) != len(partitions):
                raise AuditReadError("audit partition listing contains duplicates")
            return sorted(partitions)
        except Exception as exc:
            logger.warning(
                "[audit-parquet] list_partitions failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            if isinstance(exc, AuditReadLimitError):
                raise AuditReadLimitError(
                    "audit partition traversal exceeded a reader safety limit"
                ) from None
            raise AuditReadError("audit partitions could not be listed") from None

    def read_batch_events(
        self,
        org: str,
        year: int,
        month: int,
        day: int,
        *,
        limit: int = _MAX_AUDIT_EVENTS_PER_READ,
        strict: bool = True,
        expected_files: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read one complete, identity-sealed audit day under hard budgets.

        Returns a list of dicts, one per Parquet file (batch), containing:
          - file_path: storage path
          - file_hash: SHA-256 of the raw file bytes
          - events: list of event dicts from that file
          - chain_hash: the chain_hash stamped on events in this batch
          - instance_id: the server instance that wrote this batch
          - event_ids: sorted list of event IDs in this batch
          - min_timestamp_ms: earliest event timestamp

        Partial results are never returned. ``strict`` therefore only accepts
        ``True``; the keyword makes verifier intent explicit while preventing
        an ordinary query from accidentally downgrading storage corruption to
        an empty/partial day.
        """
        if strict is not True:
            raise ValueError("partial audit reads are not supported")
        if type(limit) is not int or limit <= 0:
            raise ValueError("audit read limit must be a positive integer")
        if limit > _MAX_AUDIT_EVENTS_PER_READ:
            raise AuditReadLimitError(
                "audit read limit exceeds the hard event ceiling"
            )
        _validated_partition_datetime(year, month, day)
        _audit_base(org)
        if pa is None or pq is None:
            raise ImportError("pyarrow is required for audit Parquet reader")

        if expected_files is None:
            files = self.list_partition_files(org, year, month, day)
        else:
            if (
                isinstance(expected_files, (str, bytes))
                or not isinstance(expected_files, Sequence)
                or len(expected_files) > _MAX_AUDIT_FILES_PER_DAY
            ):
                raise AuditReadLimitError(
                    "audit expected-file membership exceeds its safety limit"
                )
            partition = _partition_dir(
                org, _validated_partition_datetime(year, month, day),
            )
            files = [
                _require_listed_child(
                    partition, file_path, component_re=_AUDIT_FILE_RE,
                )
                for file_path in expected_files
            ]
            if len(set(files)) != len(files):
                raise AuditReadError(
                    "audit expected-file membership contains duplicates"
                )
            files = sorted(files)
        if not files:
            return []

        storage = self._get_storage()
        stat_object = getattr(storage, "stat_object", None)
        download_to_file = getattr(storage, "download_to_file", None)
        if not callable(stat_object) or not callable(download_to_file):
            raise AuditReadError(
                "audit storage lacks sealed streaming-read support"
            )

        expected_schema = _get_schema()
        field_names = tuple(field.name for field in expected_schema)
        string_columns = tuple(
            field.name for field in expected_schema if pa.types.is_string(field.type)
        )
        batches: List[Dict[str, Any]] = []
        seen_event_ids: set[str] = set()
        total_rows = 0
        total_raw_bytes = 0
        total_footer_bytes = 0
        total_schema_bytes = 0
        total_row_groups = 0
        total_column_chunks = 0
        total_declared_decoded_bytes = 0
        total_actual_decoded_bytes = 0
        total_retained_bytes = 0

        for file_path in files:
            try:
                observed = _validated_object_metadata(stat_object(file_path))
                if total_raw_bytes + observed.size > _MAX_AUDIT_TOTAL_RAW_BYTES:
                    raise AuditReadLimitError(
                        "audit day exceeds its aggregate raw-byte limit"
                    )
                total_raw_bytes += observed.size

                with tempfile.TemporaryFile(mode="w+b") as spill:
                    sink = _BoundedSpillWriter(spill, observed.size)
                    downloaded = download_to_file(
                        file_path,
                        sink,
                        expected=observed,
                        chunk_size=1024 * 1024,
                    )
                    if (
                        type(downloaded) is not int
                        or downloaded != observed.size
                        or sink.written != observed.size
                    ):
                        raise AuditReadError(
                            "audit storage returned an incomplete sealed object"
                        )
                    sink.flush()
                    if spill.seek(0, 2) != observed.size:
                        raise AuditReadError(
                            "audit spill size differs from its object seal"
                        )

                    resealed = _validated_object_metadata(stat_object(file_path))
                    if resealed != observed:
                        raise AuditReadError(
                            "audit object identity changed during download"
                        )

                    spill.seek(0)
                    if spill.read(4) != b"PAR1":
                        raise AuditReadError("audit object has an invalid header")
                    spill.seek(-8, 2)
                    footer_tail = spill.read(8)
                    if len(footer_tail) != 8 or footer_tail[4:] != b"PAR1":
                        raise AuditReadError("audit object has an invalid footer")
                    footer_bytes = struct.unpack("<I", footer_tail[:4])[0]
                    if footer_bytes <= 0 or footer_bytes + 12 > observed.size:
                        raise AuditReadError("audit Parquet footer size is invalid")
                    if footer_bytes > _MAX_AUDIT_FILE_FOOTER_BYTES:
                        raise AuditReadLimitError(
                            "audit Parquet footer exceeds its per-file limit"
                        )
                    if (
                        total_footer_bytes + footer_bytes
                        > _MAX_AUDIT_TOTAL_FOOTER_BYTES
                    ):
                        raise AuditReadLimitError(
                            "audit day exceeds its aggregate footer limit"
                        )
                    total_footer_bytes += footer_bytes

                    digest = hashlib.sha256()
                    spill.seek(0)
                    while True:
                        chunk = spill.read(1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)
                    file_hash = digest.hexdigest()

                    spill.seek(0)
                    parquet_file = pq.ParquetFile(
                        spill,
                        read_dictionary=list(string_columns),
                    )
                    actual_schema = parquet_file.schema.to_arrow_schema()
                    schema_bytes = actual_schema.serialize().size
                    if type(schema_bytes) is not int or schema_bytes <= 0:
                        raise AuditReadError("audit Parquet schema size is invalid")
                    if schema_bytes > _MAX_AUDIT_SCHEMA_BYTES:
                        raise AuditReadLimitError(
                            "audit Parquet schema exceeds its per-file limit"
                        )
                    if (
                        total_schema_bytes + schema_bytes
                        > _MAX_AUDIT_TOTAL_SCHEMA_BYTES
                    ):
                        raise AuditReadLimitError(
                            "audit day exceeds its aggregate schema limit"
                        )
                    total_schema_bytes += schema_bytes
                    if not actual_schema.equals(
                        expected_schema,
                        check_metadata=True,
                    ):
                        raise AuditReadError("audit Parquet schema is inconsistent")

                    metadata = parquet_file.metadata
                    if metadata is None:
                        raise AuditReadError("audit Parquet metadata is missing")
                    if (
                        type(metadata.serialized_size) is not int
                        or metadata.serialized_size != footer_bytes
                    ):
                        raise AuditReadError(
                            "audit Parquet footer metadata is inconsistent"
                        )
                    num_rows = metadata.num_rows
                    num_columns = metadata.num_columns
                    num_row_groups = metadata.num_row_groups
                    if type(num_rows) is not int or num_rows <= 0:
                        raise AuditReadError("audit Parquet row count is invalid")
                    if num_columns != len(expected_schema):
                        raise AuditReadError(
                            "audit Parquet column count is inconsistent"
                        )
                    if (
                        type(num_row_groups) is not int
                        or num_row_groups <= 0
                    ):
                        raise AuditReadError(
                            "audit Parquet row-group count is invalid"
                        )
                    if num_row_groups > _MAX_AUDIT_ROW_GROUPS_PER_FILE:
                        raise AuditReadLimitError(
                            "audit Parquet row-group count exceeds its per-file limit"
                        )
                    if (
                        total_row_groups + num_row_groups
                        > _MAX_AUDIT_TOTAL_ROW_GROUPS
                    ):
                        raise AuditReadLimitError(
                            "audit day exceeds its aggregate row-group limit"
                        )
                    total_row_groups += num_row_groups
                    column_chunks = num_row_groups * num_columns
                    if (
                        total_column_chunks + column_chunks
                        > _MAX_AUDIT_TOTAL_COLUMN_CHUNKS
                    ):
                        raise AuditReadLimitError(
                            "audit day exceeds its aggregate column-chunk limit"
                        )
                    total_column_chunks += column_chunks
                    if total_rows + num_rows > limit:
                        raise AuditReadLimitError(
                            "audit day exceeds the requested event limit"
                        )

                    file_declared_decoded_bytes = 0
                    metadata_rows = 0
                    for row_group_index in range(num_row_groups):
                        row_group = metadata.row_group(row_group_index)
                        row_group_rows = row_group.num_rows
                        if type(row_group_rows) is not int or row_group_rows <= 0:
                            raise AuditReadError(
                                "audit Parquet row-group size is invalid"
                            )
                        if row_group.num_columns != num_columns:
                            raise AuditReadError(
                                "audit Parquet row-group schema is inconsistent"
                            )
                        metadata_rows += row_group_rows
                        for column_index in range(num_columns):
                            column = row_group.column(column_index)
                            uncompressed = column.total_uncompressed_size
                            compressed = column.total_compressed_size
                            if (
                                type(uncompressed) is not int
                                or uncompressed < 0
                                or type(compressed) is not int
                                or compressed < 0
                            ):
                                raise AuditReadError(
                                    "audit Parquet column size is invalid"
                                )
                            file_declared_decoded_bytes += uncompressed
                            if (
                                file_declared_decoded_bytes
                                > _MAX_AUDIT_FILE_DECODED_BYTES
                            ):
                                raise AuditReadLimitError(
                                    "audit file exceeds its decoded-byte limit"
                                )
                    if metadata_rows != num_rows:
                        raise AuditReadError(
                            "audit Parquet row metadata is inconsistent"
                        )
                    if (
                        total_declared_decoded_bytes
                        + file_declared_decoded_bytes
                        > _MAX_AUDIT_DECODED_BYTES
                    ):
                        raise AuditReadLimitError(
                            "audit day exceeds its aggregate decoded-byte limit"
                        )
                    total_declared_decoded_bytes += file_declared_decoded_bytes

                    events: List[Dict[str, Any]] = []
                    event_ids: List[str] = []
                    timestamps: List[int] = []
                    chain_hash: Optional[str] = None
                    instance_id: Optional[str] = None
                    decoded_rows = 0
                    file_actual_decoded_bytes = 0
                    file_retained_bytes = 0
                    for decoded_batch in parquet_file.iter_batches(
                        batch_size=_AUDIT_DECODE_BATCH_ROWS,
                        columns=list(field_names),
                        use_threads=False,
                    ):
                        batch_rows = decoded_batch.num_rows
                        batch_bytes = decoded_batch.nbytes
                        if (
                            type(batch_rows) is not int
                            or batch_rows <= 0
                            or batch_rows > _AUDIT_DECODE_BATCH_ROWS
                            or type(batch_bytes) is not int
                            or batch_bytes < 0
                        ):
                            raise AuditReadError(
                                "audit decoder returned an invalid batch"
                            )
                        file_actual_decoded_bytes += batch_bytes
                        total_actual_decoded_bytes += batch_bytes
                        if (
                            file_actual_decoded_bytes
                            > _MAX_AUDIT_FILE_DECODED_BYTES
                        ):
                            raise AuditReadLimitError(
                                "audit file exceeds its decoded-byte limit"
                            )
                        if total_actual_decoded_bytes > _MAX_AUDIT_DECODED_BYTES:
                            raise AuditReadLimitError(
                                "audit day exceeds its aggregate decoded-byte limit"
                            )
                        if (
                            file_retained_bytes + batch_bytes
                            > _MAX_AUDIT_FILE_DECODED_BYTES
                            or total_retained_bytes + batch_bytes
                            > _MAX_AUDIT_DECODED_BYTES
                        ):
                            raise AuditReadLimitError(
                                "audit decoded working set exceeds its safety limit"
                            )

                        for row_index in range(batch_rows):
                            event: Dict[str, Any] = {}
                            event_payload_bytes = 0
                            for column_index, field_name in enumerate(field_names):
                                value = decoded_batch.column(column_index)[
                                    row_index
                                ].as_py()
                                if field_name == "timestamp_ms":
                                    if type(value) is not int or value < 0:
                                        raise AuditReadError(
                                            "audit event timestamp is invalid"
                                        )
                                    event[field_name] = value
                                    event_payload_bytes += 8
                                    continue
                                if not isinstance(value, str):
                                    raise AuditReadError(
                                        "audit event field type is invalid"
                                    )
                                try:
                                    value_bytes = len(value.encode("utf-8"))
                                except UnicodeEncodeError:
                                    raise AuditReadError(
                                        "audit event text is invalid"
                                    ) from None
                                if value_bytes > _MAX_AUDIT_EVENT_FIELD_BYTES:
                                    raise AuditReadLimitError(
                                        "audit event field exceeds its byte limit"
                                    )
                                event_payload_bytes += value_bytes
                                event[field_name] = value
                            if event_payload_bytes > _MAX_AUDIT_EVENT_BYTES:
                                raise AuditReadLimitError(
                                    "audit event exceeds its aggregate byte limit"
                                )
                            if event["organization"] != org:
                                raise AuditReadError(
                                    "audit event organization is inconsistent"
                                )
                            event_id = event["event_id"]
                            if not event_id or event_id in seen_event_ids:
                                raise AuditReadError(
                                    "audit event ID is missing or duplicated"
                                )
                            seen_event_ids.add(event_id)
                            row_chain_hash = event["chain_hash"]
                            row_instance_id = event["instance_id"]
                            if not row_instance_id:
                                raise AuditReadError(
                                    "audit event instance identity is missing"
                                )
                            if chain_hash is None:
                                chain_hash = row_chain_hash
                                instance_id = row_instance_id
                            elif (
                                row_chain_hash != chain_hash
                                or row_instance_id != instance_id
                            ):
                                raise AuditReadError(
                                    "audit batch identity fields are inconsistent"
                                )

                            retained = (
                                _AUDIT_EVENT_RETAINED_OVERHEAD_BYTES
                                + 2 * event_payload_bytes
                            )
                            if (
                                file_retained_bytes + retained
                                > _MAX_AUDIT_FILE_DECODED_BYTES
                                or total_retained_bytes + retained
                                > _MAX_AUDIT_DECODED_BYTES
                            ):
                                raise AuditReadLimitError(
                                    "audit decoded result exceeds its safety limit"
                                )
                            file_retained_bytes += retained
                            total_retained_bytes += retained
                            events.append(event)
                            event_ids.append(event_id)
                            timestamps.append(event["timestamp_ms"])
                            decoded_rows += 1

                    if decoded_rows != num_rows:
                        raise AuditReadError(
                            "audit decoder row count is inconsistent"
                        )
                    total_rows += decoded_rows
                    batches.append({
                        "file_path": file_path,
                        "file_hash": file_hash,
                        "chain_hash": chain_hash or "",
                        "instance_id": instance_id or "",
                        "event_ids": sorted(event_ids),
                        "event_count": decoded_rows,
                        "events": events,
                        "min_timestamp_ms": min(timestamps),
                    })
            except Exception as exc:
                logger.warning(
                    "[audit-parquet] read_batch_events failed; error_type=%s",
                    safe_audit_error_type(exc),
                )
                if isinstance(exc, AuditReadLimitError):
                    raise AuditReadLimitError(
                        "audit batch exceeded a reader safety limit"
                    ) from None
                raise AuditReadError(
                    "audit batch could not be read completely and safely"
                ) from None

        return batches
