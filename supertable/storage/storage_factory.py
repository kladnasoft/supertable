"""
Dynamic storage factory with lazy imports and optional cloud dependencies.

- Default backend: LOCAL (no extra packages required)
- Optional backends (install on demand):
    pip install 'supertable[minio]'  -> MinIO
    pip install 'supertable[s3]'     -> AWS S3
    pip install 'supertable[azure]'  -> Azure Blob
    pip install 'supertable[all]'    -> all of the above
"""

from typing import Any, Optional
import importlib
import os

from supertable.config.defaults import default
from supertable.storage.storage_interface import StorageInterface


def _require(module: str, extra: str) -> None:
    """
    Ensure a module is importable. If not, raise a friendly hint to install the right extra.
    """
    if importlib.util.find_spec(module) is None:
        raise RuntimeError(
            f"Missing dependency '{module}'. Install it with: pip install 'supertable[{extra}]'"
        )


def get_storage(kind: Optional[str] = None, **kwargs: Any) -> StorageInterface:
    """
    Returns a StorageInterface instance for the selected backend.

    Selection order:
      1) explicit `kind` argument if provided
      2) default.STORAGE_TYPE (e.g., 'LOCAL', 'S3', 'MINIO', 'AZURE')
      3) fallback to 'LOCAL'

    For AZURE: if no args are provided, constructs from environment (supports abfss:// SUPERTABLE_HOME and AAD).
    """
    storage_type = (kind or getattr(default, "STORAGE_TYPE", None) or "LOCAL").upper()

    if storage_type == "LOCAL":
        mod = importlib.import_module("supertable.storage.local_storage")
        return getattr(mod, "LocalStorage")(**kwargs)

    if storage_type == "S3":
        _require("boto3", "s3")
        mod = importlib.import_module("supertable.storage.s3_storage")
        return getattr(mod, "S3Storage")(**kwargs)

    if storage_type == "MINIO":
        _require("minio", "minio")
        mod = importlib.import_module("supertable.storage.minio_storage")
        return getattr(mod, "MinioStorage")(**kwargs)

    if storage_type == "AZURE":
        _require("azure.storage.blob", "azure")
        mod = importlib.import_module("supertable.storage.azure_storage")
        AzureBlobStorage = getattr(mod, "AzureBlobStorage")
        if kwargs:
            # Backward compatibility: explicit parameters provided by caller
            return AzureBlobStorage(**kwargs)
        # From environment (supports managed identity & abfss SUPERTABLE_HOME)
        return AzureBlobStorage.from_env()

    if storage_type in ("GCS", "GCP"):
        _require("google.cloud.storage", "gcp")
        mod = importlib.import_module("supertable.storage.gcp_storage")
        return getattr(mod, "GCSStorage")(**kwargs)

    raise ValueError(f"Unknown storage type: {storage_type}")
