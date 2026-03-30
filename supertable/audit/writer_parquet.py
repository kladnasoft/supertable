# route: supertable.audit.writer_parquet
"""
Warm-tier audit writer — Parquet files on the configured storage backend.

This is the system of record for audit events. Files are append-only,
partitioned by date, and named with instance_id + UUID for safe
concurrent writes from multiple server instances.

Partition layout:
    {storage_root}/{org}/__audit__/year=YYYY/month=MM/day=DD/
        audit_{date}_{time}_{instance_id}_{uuid8}.parquet

Chain proofs:
    {storage_root}/{org}/__audit__/_chain/chain_{date}.json

Compliance: DORA Art. 12 (record keeping, 5+ year retention),
            SOC 2 CC7.3 (forensic integrity).
"""
from __future__ import annotations

import io
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# PyArrow is a required dependency for SuperTable — safe to import at module level.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore

from supertable.audit.chain import MerkleProof, compute_file_hash
from supertable.audit.events import INSTANCE_ID


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

def _audit_base(org: str) -> str:
    return f"{org}/__audit__"


def _partition_dir(org: str, dt: datetime) -> str:
    return f"{_audit_base(org)}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


def _batch_filename(dt: datetime) -> str:
    """Generate a unique, concurrency-safe filename."""
    ts = dt.strftime("%Y%m%d_%H%M%S")
    inst = INSTANCE_ID.replace("/", "_").replace("\\", "_")[:40]
    uid = uuid.uuid4().hex[:8]
    return f"audit_{ts}_{inst}_{uid}.parquet"


def _chain_proof_path(org: str, date_str: str) -> str:
    """Path for daily chain proof file: _chain/chain_YYYYMMDD.json"""
    return f"{_audit_base(org)}/_chain/chain_{date_str}.json"


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
            except Exception as e:
                logger.error("[audit-parquet] Failed to resolve storage: %s", e)
                raise
        return self._storage

    # ── Write batch ────────────────────────────────────────

    def write_batch(
        self,
        org: str,
        events: List[Dict[str, Any]],
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
            return {"path": "", "file_hash": "", "event_count": 0, "bytes_written": 0}

        if pa is None or pq is None:
            raise ImportError("pyarrow is required for audit Parquet writer")

        schema = _get_schema()
        storage = self._get_storage()

        # Build columnar arrays from event dicts
        columns = {field.name: [] for field in schema}
        for event in events:
            for field_name in columns:
                val = event.get(field_name, "")
                if field_name == "timestamp_ms":
                    columns[field_name].append(int(val) if val else 0)
                else:
                    columns[field_name].append(str(val) if val is not None else "")

        table = pa.table(columns, schema=schema)

        # Write to an in-memory buffer
        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression="snappy",
            write_statistics=True,
        )
        parquet_bytes = buf.getvalue()

        # Compute file hash for chain integrity
        file_hash = compute_file_hash(parquet_bytes)

        # Determine partition path
        now = datetime.now(timezone.utc)
        partition = _partition_dir(org, now)
        filename = _batch_filename(now)
        full_path = f"{partition}/{filename}"

        # Write to storage
        try:
            storage.write_bytes(full_path, parquet_bytes)
            logger.debug(
                "[audit-parquet] Written %d events to %s (%d bytes)",
                len(events), full_path, len(parquet_bytes),
            )
        except Exception as e:
            logger.error("[audit-parquet] write_bytes failed for %s: %s", full_path, e)

        return {
            "path": full_path,
            "file_hash": file_hash,
            "event_count": len(events),
            "bytes_written": len(parquet_bytes),
        }

    # ── Chain proof ────────────────────────────────────────

    def save_chain_proof(self, org: str, proof: MerkleProof) -> bool:
        """Save the daily Merkle proof as a JSON file in storage."""
        if not proof.date:
            return False
        try:
            storage = self._get_storage()
            path = _chain_proof_path(org, proof.date.replace("-", ""))
            storage.write_json(path, proof.to_dict())
            logger.info("[audit-parquet] Saved chain proof for %s/%s", org, proof.date)
            return True
        except Exception as e:
            logger.error("[audit-parquet] save_chain_proof failed: %s", e)
            return False

    def load_chain_proof(self, org: str, date_str: str) -> Optional[MerkleProof]:
        """Load a daily chain proof. date_str format: YYYYMMDD or YYYY-MM-DD."""
        clean = date_str.replace("-", "")
        try:
            storage = self._get_storage()
            path = _chain_proof_path(org, clean)
            data = storage.read_json(path)
            return MerkleProof.from_dict(data) if data else None
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning("[audit-parquet] load_chain_proof failed for %s: %s", date_str, e)
            return None

    # ── Listing (for verification and export) ──────────────

    def list_partition_files(self, org: str, year: int, month: int, day: int) -> List[str]:
        """List all Parquet files in a day partition."""
        partition = f"{_audit_base(org)}/year={year}/month={month:02d}/day={day:02d}"
        try:
            storage = self._get_storage()
            files = storage.list_files(partition, "*.parquet")
            return [f for f in files if f.endswith(".parquet")]
        except Exception as e:
            logger.warning("[audit-parquet] list_partition_files failed: %s", e)
            return []

    def list_partitions(self, org: str) -> List[str]:
        """List all day-level partition paths for an organization.

        Traverses the year=/month=/day= directory hierarchy using
        list_files at each level (works on all storage backends).
        """
        base = _audit_base(org)
        partitions = []
        try:
            storage = self._get_storage()
            year_entries = storage.list_files(base, "year=*")
            for year_path in year_entries:
                month_entries = storage.list_files(year_path, "month=*")
                for month_path in month_entries:
                    day_entries = storage.list_files(month_path, "day=*")
                    for day_path in day_entries:
                        partitions.append(day_path)
            return sorted(partitions)
        except Exception as e:
            logger.warning("[audit-parquet] list_partitions failed: %s", e)
            return []

    def read_batch_events(
        self,
        org: str,
        year: int,
        month: int,
        day: int,
    ) -> List[Dict[str, Any]]:
        """Read all audit events from Parquet files for a specific day.

        Returns a list of dicts, one per Parquet file (batch), containing:
          - file_path: storage path
          - file_hash: SHA-256 of the raw file bytes
          - events: list of event dicts from that file
          - chain_hash: the chain_hash stamped on events in this batch
          - instance_id: the server instance that wrote this batch
          - event_ids: sorted list of event IDs in this batch
          - min_timestamp_ms: earliest event timestamp
        """
        if pa is None or pq is None:
            return []

        files = self.list_partition_files(org, year, month, day)
        if not files:
            return []

        storage = self._get_storage()
        batches: List[Dict[str, Any]] = []

        for file_path in sorted(files):
            try:
                raw_bytes = storage.read_bytes(file_path)
                file_hash = compute_file_hash(raw_bytes)

                buf = io.BytesIO(raw_bytes)
                table = pq.read_table(buf)
                rows = table.to_pydict()

                num_rows = table.num_rows
                if num_rows == 0:
                    continue

                events = []
                for i in range(num_rows):
                    event = {col: rows[col][i] for col in rows}
                    events.append(event)

                # All events in a batch share the same chain_hash and instance_id
                chain_hash = events[0].get("chain_hash", "")
                instance_id = events[0].get("instance_id", "")
                event_ids = sorted(str(e.get("event_id", "")) for e in events)
                timestamps = [int(e.get("timestamp_ms", 0)) for e in events]

                batches.append({
                    "file_path": file_path,
                    "file_hash": file_hash,
                    "chain_hash": chain_hash,
                    "instance_id": instance_id,
                    "event_ids": event_ids,
                    "event_count": num_rows,
                    "events": events,
                    "min_timestamp_ms": min(timestamps) if timestamps else 0,
                })
            except Exception as e:
                logger.warning("[audit-parquet] read_batch_events failed for %s: %s", file_path, e)

        return batches
