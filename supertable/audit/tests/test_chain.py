# route: supertable.audit.tests.test_chain
"""Tests for ``supertable.audit.chain``.

Covers:
  - ``compute_batch_hash`` ordering invariance and determinism
  - ``compute_chain_hash`` extension behavior
  - ``compute_file_hash`` SHA-256 contract
  - ``InstanceChain.advance`` head progression and serialization
  - ``MerkleProof.compute_root`` ordering invariance
  - ``verify_batch_chain`` valid / tampered / cross-batch gap scenarios
  - ``verify_merkle_proof`` recompute behavior
"""
from __future__ import annotations

import hashlib

import pytest

from supertable.audit.chain import (
    GENESIS_HASH,
    InstanceChain,
    MerkleProof,
    compute_batch_hash,
    compute_chain_hash,
    compute_file_hash,
    verify_batch_chain,
    verify_merkle_proof,
)


# ---------------------------------------------------------------------------
# Pure hash helpers
# ---------------------------------------------------------------------------


class TestComputeBatchHash:
    def test_order_invariant_in_event_ids(self) -> None:
        a = compute_batch_hash(["a", "b", "c"], "filehash")
        b = compute_batch_hash(["c", "a", "b"], "filehash")
        assert a == b

    def test_changes_with_file_hash(self) -> None:
        a = compute_batch_hash(["a", "b"], "h1")
        b = compute_batch_hash(["a", "b"], "h2")
        assert a != b

    def test_empty_inputs_are_hashable(self) -> None:
        h = compute_batch_hash([], "")
        assert isinstance(h, str)
        assert len(h) == 64  # sha256 hex

    def test_deterministic(self) -> None:
        h1 = compute_batch_hash(["x"], "f")
        h2 = compute_batch_hash(["x"], "f")
        assert h1 == h2


class TestComputeChainHash:
    def test_uses_genesis_when_previous_empty(self) -> None:
        b = "a" * 64
        with_empty = compute_chain_hash("", b)
        with_genesis = compute_chain_hash(GENESIS_HASH, b)
        assert with_empty == with_genesis

    def test_changes_with_previous(self) -> None:
        b = "b" * 64
        a = compute_chain_hash("0" * 64, b)
        c = compute_chain_hash("1" * 64, b)
        assert a != c


class TestComputeFileHash:
    def test_matches_sha256(self) -> None:
        data = b"the quick brown fox"
        assert compute_file_hash(data) == hashlib.sha256(data).hexdigest()

    def test_empty_bytes(self) -> None:
        assert compute_file_hash(b"") == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# InstanceChain
# ---------------------------------------------------------------------------


class TestInstanceChain:
    def test_starts_at_genesis(self) -> None:
        c = InstanceChain(instance_id="srv-1")
        assert c.head == GENESIS_HASH
        assert c.batch_count == 0

    def test_advance_progresses_head_and_count(self) -> None:
        c = InstanceChain(instance_id="srv-1")
        first = c.advance(["a", "b"], "fh-1")
        assert c.head == first
        assert c.batch_count == 1

        second = c.advance(["c"], "fh-2")
        assert second != first
        assert c.head == second
        assert c.batch_count == 2

    def test_to_dict_round_trip(self) -> None:
        c = InstanceChain(instance_id="srv-1")
        c.advance(["a"], "f")
        d = c.to_dict()
        rebuilt = InstanceChain.from_dict(d)
        assert rebuilt.instance_id == "srv-1"
        assert rebuilt.head == c.head
        assert rebuilt.batch_count == 1

    def test_from_dict_supplies_defaults(self) -> None:
        c = InstanceChain.from_dict({})
        assert c.instance_id == ""
        assert c.head == GENESIS_HASH
        assert c.batch_count == 0


# ---------------------------------------------------------------------------
# MerkleProof
# ---------------------------------------------------------------------------


class TestMerkleProof:
    def test_compute_root_with_no_instances_returns_genesis(self) -> None:
        p = MerkleProof(date="2025-01-15")
        assert p.compute_root() == GENESIS_HASH

    def test_compute_root_independent_of_insertion_order(self) -> None:
        c1 = InstanceChain(instance_id="srv-a", head="a" * 64, batch_count=1)
        c2 = InstanceChain(instance_id="srv-b", head="b" * 64, batch_count=1)

        p_ab = MerkleProof(date="2025-01-15")
        p_ab.add_instance(c1, event_count=2)
        p_ab.add_instance(c2, event_count=3)
        root_ab = p_ab.compute_root()

        p_ba = MerkleProof(date="2025-01-15")
        p_ba.add_instance(c2, event_count=3)
        p_ba.add_instance(c1, event_count=2)
        root_ba = p_ba.compute_root()

        assert root_ab == root_ba

    def test_to_dict_round_trip(self) -> None:
        c = InstanceChain(instance_id="srv-1", head="a" * 64, batch_count=1)
        p = MerkleProof(date="2025-01-15")
        p.add_instance(c, event_count=5)
        p.compute_root()

        rebuilt = MerkleProof.from_dict(p.to_dict())
        assert rebuilt.date == p.date
        assert rebuilt.merkle_root == p.merkle_root
        assert rebuilt.total_events == 5
        assert rebuilt.instances == p.instances


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _make_batch(prev: str, event_ids: list[str], file_hash: str) -> dict:
    """Build a recorded batch dict whose chain_hash is consistent with `prev`."""
    bh = compute_batch_hash(event_ids, file_hash)
    new_head = compute_chain_hash(prev, bh)
    return {
        "event_ids": event_ids,
        "file_hash": file_hash,
        "chain_hash": new_head,
    }


class TestVerifyBatchChain:
    def test_valid_chain(self) -> None:
        head = GENESIS_HASH
        batches = []
        for ids, fh in [(["a"], "f1"), (["b", "c"], "f2"), (["d"], "f3")]:
            b = _make_batch(head, ids, fh)
            head = b["chain_hash"]
            batches.append(b)

        result = verify_batch_chain(batches, expected_head=head)
        assert result["valid"] is True
        assert result["batches_checked"] == 3
        assert result["gaps"] == []
        assert result["computed_head"] == head

    def test_tampered_event_id_detected(self) -> None:
        head = GENESIS_HASH
        b1 = _make_batch(head, ["a"], "f1")
        head = b1["chain_hash"]
        b2 = _make_batch(head, ["b"], "f2")
        # Tamper: change the recorded event_ids without updating the chain hash
        b2["event_ids"] = ["b", "EXTRA"]

        result = verify_batch_chain([b1, b2], expected_head=b2["chain_hash"])
        assert result["valid"] is False
        assert any(g["batch_index"] == 1 for g in result["gaps"])

    def test_wrong_expected_head(self) -> None:
        b1 = _make_batch(GENESIS_HASH, ["a"], "f")
        result = verify_batch_chain(
            [b1], expected_head="0" * 64
        )  # not the actual head
        assert result["valid"] is False
        assert result["gaps"] == []  # the chain itself is internally consistent
        assert result["computed_head"] == b1["chain_hash"]


class TestVerifyMerkleProof:
    def test_recomputed_root_matches(self) -> None:
        c1 = InstanceChain(instance_id="srv-a", head="a" * 64, batch_count=1)
        c2 = InstanceChain(instance_id="srv-b", head="b" * 64, batch_count=1)
        p = MerkleProof(date="2025-01-15")
        p.add_instance(c1)
        p.add_instance(c2)
        p.compute_root()

        result = verify_merkle_proof(p)
        assert result["valid"] is True
        assert result["computed_root"] == result["recorded_root"]
        assert result["instance_count"] == 2

    def test_tampered_root_detected(self) -> None:
        c1 = InstanceChain(instance_id="srv-a", head="a" * 64, batch_count=1)
        p = MerkleProof(date="2025-01-15")
        p.add_instance(c1)
        p.compute_root()
        p.merkle_root = "f" * 64  # tamper

        result = verify_merkle_proof(p)
        assert result["valid"] is False
        assert result["computed_root"] != result["recorded_root"]
