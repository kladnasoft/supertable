from supertable.config.defaults import default
from typing import Any
from supertable.config.defaults import default
from supertable.storage.local_storage import LocalStorage
from supertable.storage.s3_storage import S3Storage
from supertable.storage.minio_storage import MinioStorage
from supertable.storage.storage_interface import StorageInterface
# from supertable.storage.azure_storage import AzureStorage
# from supertable.storage.gcp_storage import GCPStorage

def get_storage() -> StorageInterface:
    """
    Returns an instance of a StorageInterface based on default.STORAGE_TYPE.
    Falls back to LOCAL if STORAGE_TYPE is missing or empty.
    """
    storage_type = getattr(default, "STORAGE_TYPE", None)
    if not storage_type:
        storage_type = "LOCAL"

    storage_type = storage_type.upper()
    if storage_type == "LOCAL":
        return LocalStorage()
    elif storage_type == "S3":
        return S3Storage(bucket_name="my-s3-bucket", s3_client=None)
    elif storage_type == "MINIO":
        return MinioStorage(bucket_name="my-minio-bucket", minio_client=None)
    # ... GCP, AZURE, etc.
    else:
        return LocalStorage()