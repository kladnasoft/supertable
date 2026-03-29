# route: supertable.audit.chain
"""
Tamper-evident hash chain for the audit trail.

Each server instance maintains its own SHA-256 chain. A daily Merkle
proof aggregates all instance chains into a single verifiable root.

Design:
  batch_hash = SHA-256(sorted_event_ids + parquet_file_hash)
  chain_hash = SHA-256(previous_chain_hash + batch_hash)

The chain head is persisted in Redis for fast append and in storage
as daily proof files for offline verification.

Compliance: DORA Art. 12 (record keeping), SOC 2 CC7.3 (forensic integrity).
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GENESIS_HASH = "0" * 64  # Chain starts here (no previous batch)


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------

def compute_batch_hash(event_ids: List[str], file_hash: str = "") -> str:
    """Hash a batch of event IDs + the Parquet file hash.

    Event IDs are sorted to ensure deterministic ordering regardless
    of the order events were queued.
    """
    payload = "\n".join(sorted(event_ids)) + "\n" + (file_hash or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_chain_hash(previous_hash: str, batch_hash: str) -> str:
    """Extend the chain: SHA-256(previous || batch)."""
    payload = (previous_hash or GENESIS_HASH) + batch_hash
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_file_hash(data: bytes) -> str:
    """SHA-256 of raw file bytes (Parquet file content)."""
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Per-instance chain state
# ---------------------------------------------------------------------------

@dataclass
class InstanceChain:
    """Mutable chain state for a single server instance.

    Thread-safety: callers must hold a lock when calling advance().
    The AuditLogger holds this lock.
    """
    instance_id: str
    head: str = GENESIS_HASH
    batch_count: int = 0

    def advance(self, event_ids: List[str], file_hash: str = "") -> str:
        """Append a batch to the chain. Returns the new chain head."""
        batch_hash = compute_batch_hash(event_ids, file_hash)
        self.head = compute_chain_hash(self.head, batch_hash)
        self.batch_count += 1
        return self.head

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "head": self.head,
            "batch_count": self.batch_count,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "InstanceChain":
        return cls(
            instance_id=d.get("instance_id", ""),
            head=d.get("head", GENESIS_HASH),
            batch_count=int(d.get("batch_count", 0)),
        )


# ---------------------------------------------------------------------------
# Daily Merkle proof (aggregates all instance chains)
# ---------------------------------------------------------------------------

@dataclass
class MerkleProof:
    """Daily aggregation of all instance chains into a single verifiable root."""
    date: str = ""  # "2025-01-15"
    instances: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    merkle_root: str = ""
    total_events: int = 0
    created_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def add_instance(self, chain: InstanceChain, event_count: int = 0) -> None:
        """Register an instance chain into this proof."""
        self.instances[chain.instance_id] = {
            "head": chain.head,
            "batches": chain.batch_count,
        }
        self.total_events += event_count

    def compute_root(self) -> str:
        """Compute the Merkle root from sorted instance heads.

        Sorting by instance_id ensures deterministic root regardless
        of the order instances are added.
        """
        if not self.instances:
            self.merkle_root = GENESIS_HASH
            return self.merkle_root

        sorted_heads = [
            self.instances[k]["head"]
            for k in sorted(self.instances.keys())
        ]
        combined = "\n".join(sorted_heads)
        self.merkle_root = hashlib.sha256(combined.encode("utf-8")).hexdigest()
        return self.merkle_root

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "instances": self.instances,
            "merkle_root": self.merkle_root,
            "total_events": self.total_events,
            "created_ms": self.created_ms,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MerkleProof":
        return cls(
            date=d.get("date", ""),
            instances=d.get("instances", {}),
            merkle_root=d.get("merkle_root", ""),
            total_events=int(d.get("total_events", 0)),
            created_ms=int(d.get("created_ms", 0)),
        )


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_batch_chain(
    batches: List[Dict[str, Any]],
    expected_head: str,
    starting_hash: str = GENESIS_HASH,
) -> Dict[str, Any]:
    """Verify a sequence of batches against an expected chain head.

    Each batch dict must contain:
      - event_ids: List[str]
      - file_hash: str
      - chain_hash: str (the recorded chain hash after this batch)

    Returns:
      {"valid": bool, "batches_checked": int, "gaps": [...], "computed_head": str}
    """
    current = starting_hash
    gaps = []
    checked = 0

    for i, batch in enumerate(batches):
        event_ids = batch.get("event_ids", [])
        file_hash = batch.get("file_hash", "")
        recorded_hash = batch.get("chain_hash", "")

        batch_hash = compute_batch_hash(event_ids, file_hash)
        expected = compute_chain_hash(current, batch_hash)

        if expected != recorded_hash:
            gaps.append({
                "batch_index": i,
                "expected": expected,
                "recorded": recorded_hash,
                "previous_hash": current,
            })

        current = recorded_hash  # Continue with recorded hash to detect further gaps
        checked += 1

    return {
        "valid": len(gaps) == 0 and current == expected_head,
        "batches_checked": checked,
        "gaps": gaps,
        "computed_head": current,
        "expected_head": expected_head,
    }


def verify_merkle_proof(proof: MerkleProof) -> Dict[str, Any]:
    """Verify a daily Merkle proof by recomputing the root.

    Returns:
      {"valid": bool, "computed_root": str, "recorded_root": str}
    """
    recomputed = MerkleProof(
        date=proof.date,
        instances=proof.instances,
        total_events=proof.total_events,
    )
    computed_root = recomputed.compute_root()

    return {
        "valid": computed_root == proof.merkle_root,
        "computed_root": computed_root,
        "recorded_root": proof.merkle_root,
        "instance_count": len(proof.instances),
        "date": proof.date,
    }
