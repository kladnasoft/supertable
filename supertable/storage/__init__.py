# route: supertable.storage
from supertable.storage.storage_interface import StorageInterface
from supertable.storage.storage_factory import get_storage

__all__ = ["StorageInterface", "get_storage"]
