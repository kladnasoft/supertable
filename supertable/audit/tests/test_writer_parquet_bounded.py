# route: supertable.audit.tests.test_writer_parquet_bounded
"""Adversarial bounds and integrity tests for the Parquet audit reader."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable.audit.chain import GENESIS_HASH, InstanceChain, MerkleProof
from supertable.audit import writer_parquet as writer_module
from supertable.audit.writer_parquet import (
    AuditReadError,
    AuditReadLimitError,
    ParquetAuditWriter,
    _audit_base,
)
from supertable.storage.storage_interface import ObjectMetadata


_ORG = "customer-acme"
_PARTITION = f"{_ORG}/__audit__/year=2026/month=08/day=24"
_FILE = f"{_PARTITION}/audit_20260824_node_0001.parquet"
_PROOF_PATH = f"{_ORG}/__audit__/_chain/chain_20260824.json"


def _event(event_id: str, *, organization: str = _ORG) -> dict[str, Any]:
    event: dict[str, Any] = {}
    for field in writer_module._get_schema():
        if field.name == "timestamp_ms":
            event[field.name] = 1_777_070_400_000
        else:
            event[field.name] = ""
    event.update({
        "event_id": event_id,
        "organization": organization,
        "action": "query_execute",
        "category": "data_access",
        "chain_hash": "a" * 64,
        "instance_id": "audit-node-1",
    })
    return event


def _parquet_bytes(
    events: list[dict[str, Any]],
    *,
    schema: pa.Schema | None = None,
    row_group_size: int | None = None,
) -> bytes:
    selected_schema = schema or writer_module._get_schema()
    columns = {
        field.name: [event.get(field.name, "") for event in events]
        for field in selected_schema
    }
    table = pa.table(columns, schema=selected_schema)
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="snappy",
        row_group_size=row_group_size,
    )
    return sink.getvalue().to_pybytes()


def _proof_bytes(*, date: str = "2026-08-24") -> bytes:
    return json.dumps({
        "date": date,
        "instances": {
            "audit-node-1": {
                "head": "a" * 64,
                "batches": 2,
                "events": 3,
            },
        },
        "merkle_root": GENESIS_HASH,
        "total_events": 3,
        "created_ms": 1_777_070_400_000,
    }).encode()


class _SealedStorage:
    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})
        self.calls: list[tuple[Any, ...]] = []
        self.listing: list[str] | None = None
        self.unstable = False
        self.download_mode = "normal"
        self._stat_calls: dict[str, int] = {}

    def list_files(self, path: str, pattern: str) -> list[str]:
        self.calls.append(("list_files", path, pattern))
        if self.listing is not None:
            return list(self.listing)
        prefix = f"{path}/"
        return sorted(
            key for key in self.objects
            if key.startswith(prefix) and key.endswith(".parquet")
        )

    def stat_object(self, path: str) -> ObjectMetadata:
        self.calls.append(("stat_object", path))
        try:
            payload = self.objects[path]
        except KeyError as exc:
            raise FileNotFoundError("backend-controlled secret path") from exc
        count = self._stat_calls.get(path, 0) + 1
        self._stat_calls[path] = count
        version = "v2" if self.unstable and count > 1 else "v1"
        return ObjectMetadata(size=len(payload), version=version)

    def download_to_file(
        self,
        path: str,
        file_obj: Any,
        *,
        expected: ObjectMetadata | None = None,
        chunk_size: int = 0,
    ) -> int:
        self.calls.append(("download_to_file", path, expected, chunk_size))
        payload = self.objects[path]
        if self.download_mode == "overflow":
            file_obj.write(payload + b"x")
            return len(payload) + 1
        for offset in range(0, len(payload), 37):
            file_obj.write(payload[offset:offset + 37])
        if self.download_mode == "short_count":
            return len(payload) - 1
        return len(payload)

    def write_bytes(self, path: str, payload: bytes) -> None:
        self.calls.append(("write_bytes", path))
        self.objects[path] = payload

    def create_bytes_if_absent(self, path: str, payload: bytes) -> bool:
        self.calls.append(("create_bytes_if_absent", path))
        if path in self.objects:
            return False
        self.objects[path] = payload
        return True

    def write_json(self, path: str, value: dict[str, Any]) -> None:
        self.calls.append(("write_json", path))
        self.objects[path] = json.dumps(value).encode()


def test_read_batch_events_uses_sealed_spill_and_incremental_decode() -> None:
    payload = _parquet_bytes([_event(f"evt-{index}") for index in range(70)])
    storage = _SealedStorage({_FILE: payload})

    batches = ParquetAuditWriter(storage=storage).read_batch_events(
        _ORG, 2026, 8, 24, strict=True,
    )

    assert len(batches) == 1
    assert batches[0]["event_count"] == 70
    assert batches[0]["file_hash"] == hashlib.sha256(payload).hexdigest()
    assert batches[0]["event_ids"] == sorted(
        f"evt-{index}" for index in range(70)
    )
    assert [call[0] for call in storage.calls].count("stat_object") == 2
    download_call = next(
        call for call in storage.calls if call[0] == "download_to_file"
    )
    assert isinstance(download_call[2], ObjectMetadata)
    assert download_call[3] == 1024 * 1024


def test_exact_receipt_read_does_not_scan_unrelated_partition_objects() -> None:
    payload = _parquet_bytes([_event("evt-1")])
    unrelated = f"{_PARTITION}/audit_unrelated.parquet"
    storage = _SealedStorage({_FILE: payload, unrelated: b"not-parquet"})

    batches = ParquetAuditWriter(storage=storage).read_batch_events(
        _ORG,
        2026,
        8,
        24,
        strict=True,
        expected_files=(_FILE,),
    )

    assert [batch["file_path"] for batch in batches] == [_FILE]
    assert "list_files" not in [call[0] for call in storage.calls]
    assert unrelated not in [call[1] for call in storage.calls]


def test_read_rejects_backend_path_escape_before_object_access() -> None:
    storage = _SealedStorage()
    storage.listing = ["/srv/secret/audit_stolen.parquet"]

    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )

    assert [call[0] for call in storage.calls] == ["list_files"]


def test_read_rejects_file_fanout_before_object_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _SealedStorage()
    storage.listing = [
        f"{_PARTITION}/audit_one.parquet",
        f"{_PARTITION}/audit_two.parquet",
    ]
    monkeypatch.setattr(writer_module, "_MAX_AUDIT_FILES_PER_DAY", 1)

    with pytest.raises(AuditReadLimitError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )

    assert [call[0] for call in storage.calls] == ["list_files"]


@pytest.mark.parametrize("download_mode", ["short_count", "overflow"])
def test_read_rejects_incomplete_or_overlong_download(download_mode: str) -> None:
    storage = _SealedStorage({_FILE: _parquet_bytes([_event("evt-1")])})
    storage.download_mode = download_mode

    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_rejects_identity_change_during_download() -> None:
    storage = _SealedStorage({_FILE: _parquet_bytes([_event("evt-1")])})
    storage.unstable = True

    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_rejects_footer_before_arrow_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _SealedStorage({_FILE: _parquet_bytes([_event("evt-1")])})
    monkeypatch.setattr(writer_module, "_MAX_AUDIT_FILE_FOOTER_BYTES", 8)

    with pytest.raises(AuditReadLimitError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_rejects_schema_mismatch() -> None:
    schema = pa.schema([
        field for field in writer_module._get_schema()
        if field.name != "reason"
    ])
    storage = _SealedStorage({
        _FILE: _parquet_bytes([_event("evt-1")], schema=schema),
    })

    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_rejects_row_group_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _SealedStorage({
        _FILE: _parquet_bytes(
            [_event("evt-1"), _event("evt-2")],
            row_group_size=1,
        ),
    })
    monkeypatch.setattr(writer_module, "_MAX_AUDIT_ROW_GROUPS_PER_FILE", 1)

    with pytest.raises(AuditReadLimitError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_rejects_decoded_byte_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _SealedStorage({_FILE: _parquet_bytes([_event("evt-1")])})
    monkeypatch.setattr(writer_module, "_MAX_AUDIT_FILE_DECODED_BYTES", 1)

    with pytest.raises(AuditReadLimitError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


def test_read_never_returns_partial_result_after_later_corruption() -> None:
    first = f"{_PARTITION}/audit_01.parquet"
    second = f"{_PARTITION}/audit_02.parquet"
    storage = _SealedStorage({
        first: _parquet_bytes([_event("evt-1")]),
        second: b"PAR1" + b"corrupt" + b"\x00\x00\x00\x00PAR1",
    })

    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, 2026, 8, 24,
        )


@pytest.mark.parametrize("limit", [True, 0, -1, 250_001, "10"])
def test_read_limit_is_strict_and_bounded(limit: Any) -> None:
    writer = ParquetAuditWriter(storage=_SealedStorage())

    with pytest.raises((ValueError, AuditReadLimitError)):
        writer.read_batch_events(_ORG, 2026, 8, 24, limit=limit)


def test_read_rejects_partial_mode() -> None:
    with pytest.raises(ValueError, match="partial audit reads"):
        ParquetAuditWriter(storage=_SealedStorage()).read_batch_events(
            _ORG, 2026, 8, 24, strict=False,
        )


@pytest.mark.parametrize(
    "org",
    [
        "",
        "../escape",
        "/absolute",
        "back\\slash",
        "colon:value",
        "line\nbreak",
        "UPPER",
        "a" * 65,
        "_reserved_",
        None,
        7,
    ],
)
def test_audit_base_reuses_canonical_org_authority(org: Any) -> None:
    with pytest.raises(ValueError):
        _audit_base(org)


def _write_operation(writer: ParquetAuditWriter, org: str) -> Any:
    return writer.write_batch(org, [_event("evt-1", organization=org)])


def _save_proof_operation(writer: ParquetAuditWriter, org: str) -> Any:
    return writer.save_chain_proof(org, MerkleProof(date="2026-08-24"))


@pytest.mark.parametrize(
    "operation",
    [
        _write_operation,
        _save_proof_operation,
        lambda writer, org: writer.load_chain_proof(org, "20260824"),
        lambda writer, org: writer.list_partition_files(org, 2026, 8, 24),
        lambda writer, org: writer.list_partitions(org),
        lambda writer, org: writer.read_batch_events(org, 2026, 8, 24),
    ],
)
def test_every_storage_operation_validates_org_before_backend_access(
    operation: Callable[[ParquetAuditWriter, str], Any],
) -> None:
    storage = _SealedStorage()

    with pytest.raises(ValueError):
        operation(ParquetAuditWriter(storage=storage), "../escape")

    assert storage.calls == []


@pytest.mark.parametrize(
    ("year", "month", "day"),
    [(2026, 2, 30), (2026, 0, 1), (2026, 13, 1), (True, 8, 24)],
)
def test_partition_date_rejected_before_backend_access(
    year: Any,
    month: Any,
    day: Any,
) -> None:
    storage = _SealedStorage()
    with pytest.raises(ValueError):
        ParquetAuditWriter(storage=storage).read_batch_events(
            _ORG, year, month, day,
        )
    assert storage.calls == []


def test_proof_read_is_bounded_sealed_and_structurally_validated() -> None:
    storage = _SealedStorage({_PROOF_PATH: _proof_bytes()})

    proof = ParquetAuditWriter(storage=storage).load_chain_proof(
        _ORG, "2026-08-24", strict=True,
    )

    assert proof is not None
    assert proof.date == "2026-08-24"
    assert [call[0] for call in storage.calls].count("stat_object") == 2
    download_call = next(
        call for call in storage.calls if call[0] == "download_to_file"
    )
    assert download_call[3] == 256 * 1024


def test_nonempty_proof_producer_schema_round_trips_through_strict_loader() -> None:
    storage = _SealedStorage()
    proof = MerkleProof(date="2026-08-24")
    proof.add_instance(
        InstanceChain("audit-node-1", head="a" * 64, batch_count=2),
        event_count=3,
    )
    proof.compute_root()
    writer = ParquetAuditWriter(storage=storage)

    assert writer.save_chain_proof(_ORG, proof) is True
    loaded = writer.load_chain_proof(_ORG, "20260824", strict=True)

    assert loaded is not None
    assert loaded.instances == proof.instances
    assert loaded.total_events == 3


def test_daily_proof_publication_is_exactly_idempotent_and_immutable() -> None:
    storage = _SealedStorage()
    writer = ParquetAuditWriter(storage=storage)
    proof = MerkleProof(date="2026-08-24", created_ms=1_777_070_400_000)
    proof.add_instance(
        InstanceChain("audit-node-1", head="a" * 64, batch_count=2),
        event_count=3,
    )
    proof.compute_root()

    assert writer.save_chain_proof(_ORG, proof) is True
    original = storage.objects[_PROOF_PATH]
    assert writer.save_chain_proof(_ORG, proof) is True
    assert [call[0] for call in storage.calls].count(
        "create_bytes_if_absent"
    ) == 2

    conflicting = MerkleProof(
        date="2026-08-24", created_ms=1_777_070_400_001,
    )
    conflicting.add_instance(
        InstanceChain("audit-node-1", head="b" * 64, batch_count=3),
        event_count=4,
    )
    conflicting.compute_root()
    assert writer.save_chain_proof(_ORG, conflicting) is False
    assert storage.objects[_PROOF_PATH] == original
    assert [call[0] for call in storage.calls].count(
        "create_bytes_if_absent"
    ) == 3


def test_daily_proof_reconciles_ambiguous_exact_create() -> None:
    class AmbiguousStorage(_SealedStorage):
        def create_bytes_if_absent(self, path: str, payload: bytes) -> bool:
            super().create_bytes_if_absent(path, payload)
            raise TimeoutError("provider timed out after exact commit")

    storage = AmbiguousStorage()
    proof = MerkleProof(date="2026-08-24", created_ms=1_777_070_400_000)
    proof.add_instance(
        InstanceChain("audit-node-1", head="a" * 64, batch_count=1),
        event_count=1,
    )
    proof.compute_root()

    assert ParquetAuditWriter(storage=storage).save_chain_proof(
        _ORG, proof,
    ) is True
    assert [call[0] for call in storage.calls].count(
        "create_bytes_if_absent"
    ) == 1


def test_proof_missing_is_distinct_from_corruption() -> None:
    writer = ParquetAuditWriter(storage=_SealedStorage())
    assert writer.load_chain_proof(_ORG, "20260824", strict=True) is None

    corrupt = _SealedStorage({_PROOF_PATH: b'{"date":"2026-08-24"}'})
    with pytest.raises(AuditReadError):
        ParquetAuditWriter(storage=corrupt).load_chain_proof(
            _ORG, "20260824", strict=True,
        )
    assert ParquetAuditWriter(storage=corrupt).load_chain_proof(
        _ORG, "20260824", strict=False,
    ) is None


@pytest.mark.parametrize("date", ["../../x", "2026/08/24", "20260230", ""])
def test_proof_date_rejected_before_backend_access(date: str) -> None:
    storage = _SealedStorage()
    with pytest.raises(ValueError):
        ParquetAuditWriter(storage=storage).load_chain_proof(_ORG, date)
    assert storage.calls == []
