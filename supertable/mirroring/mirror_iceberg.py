# supertable/mirroring/mirror_iceberg.py

import os
import uuid
from typing import Dict, Any, List
from datetime import datetime, timezone

from supertable.config.defaults import logger


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _stable_table_uuid(organization: str, super_name: str, table_name: str) -> str:
    # Stable UUID from logical identity (no need to persist a separate uuid file)
    seed = f"st://{organization}/{super_name}/{table_name}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def _schema_fields_from_catalog(schema_any: Any) -> List[Dict[str, Any]]:
    """
    Expecting SuperTable's collect_schema output (usually a list of dicts).
    We map minimally to Iceberg-ish fields. If unknown, we pass through name/type.
    """
    fields = []
    if isinstance(schema_any, list):
        for idx, col in enumerate(schema_any):
            # col can be {"name": "...", "type": "..."} or similar
            name = col.get("name") if isinstance(col, dict) else None
            typ = col.get("type") if isinstance(col, dict) else None
            if not name:
                continue
            fields.append({
                "id": idx + 1,
                "name": name,
                "required": False,
                "type": typ or "string",
            })
    return fields


def write_iceberg_table(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:
    """
    'Iceberg-lite' mirror:
      - metadata/<version>.json with a minimal, current-only spec
      - manifests/<version>.json listing data files (we don't produce true Avro manifests)
    """
    base = os.path.join(super_table.organization, super_table.super_name, "iceberg", table_name)
    metadata_dir = os.path.join(base, "metadata")
    manifests_dir = os.path.join(base, "manifests")
    super_table.storage.makedirs(metadata_dir)
    super_table.storage.makedirs(manifests_dir)

    version = int(simple_snapshot.get("snapshot_version", 0))
    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", [])
    schema_any = simple_snapshot.get("schema", [])

    now_ms = _now_ms()
    table_uuid = _stable_table_uuid(super_table.organization, super_table.super_name, table_name)
    fields = _schema_fields_from_catalog(schema_any)

    # "Manifest list" (JSON, not Avro) – latest only
    manifest_list_path = os.path.join(manifests_dir, f"{version:020d}.json")
    manifest_payload = {
        "version": version,
        "generated_at_ms": now_ms,
        "data_files": [
            {"path": r["file"], "file_size": int(r.get("file_size", 0)), "format": "parquet"}
            for r in resources
        ],
    }
    super_table.storage.write_json(manifest_list_path, manifest_payload)

    # Minimal metadata – latest only
    metadata_path = os.path.join(metadata_dir, f"{version:020d}.json")
    metadata_payload = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": base,
        "last-sequence-number": version,
        "last-updated-ms": now_ms,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": fields,
            }
        ],
        "current-schema-id": 0,
        "partition-spec": [],
        "default-spec-id": 0,
        "properties": {
            "created-by": "supertable",
            "mirror": "iceberg-lite",
        },
        "current-snapshot-id": version,
        "snapshots": [
            {
                "snapshot-id": version,
                "sequence-number": version,
                "timestamp-ms": now_ms,
                "summary": {"operation": "replace"},
                "manifest-list": manifest_list_path,
            }
        ],
    }
    super_table.storage.write_json(metadata_path, metadata_payload)

    # Convenience pointer
    super_table.storage.write_json(
        os.path.join(base, "latest.json"),
        {"version": version, "metadata": metadata_path, "manifest_list": manifest_list_path},
    )

    logger.debug(f"[mirror][iceberg] wrote {metadata_path} with {len(resources)} files")
