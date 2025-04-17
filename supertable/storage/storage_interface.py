import abc
from typing import Any, Dict, List
import pyarrow as pa

class StorageInterface(abc.ABC):
    """
    Abstract base class for a storage interface that can handle both local and
    cloud/object storage in a unified manner.
    """


    @abc.abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Reads and returns JSON data from the given path.
        Raises FileNotFoundError, ValueError, etc. on error.
        """
        pass

    @abc.abstractmethod
    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Writes JSON data to the given path.
        Overwrites if it already exists.
        """
        pass

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """
        Returns True if the given path exists, False otherwise.
        For cloud storage, 'path' might be a prefix / object key.
        """
        pass

    @abc.abstractmethod
    def size(self, path: str) -> int:
        """
        Returns the size (in bytes) of the object at the given path.
        Raises FileNotFoundError if not found.
        """
        pass

    @abc.abstractmethod
    def makedirs(self, path: str) -> None:
        """
        Creates directories (or the equivalent in cloud storage) if needed.
        No-op if already present.
        """
        pass

    @abc.abstractmethod
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Returns a list of files/objects found in `path` matching the given pattern.
        This may be limited to a single "directory" level, depending on implementation.
        For recursive scans, either extend with a `recursive=True` parameter or
        provide an additional method.
        """
        pass

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        """
        Deletes a file/object at the given path.
        Raises FileNotFoundError if the path does not exist.
        """
        pass

    @abc.abstractmethod
    def get_directory_structure(self, path: str) -> dict:
        """
        Recursively builds and returns a nested dictionary representing
        the folder structure under 'path'. For local storage, uses os.walk.
        For S3/MinIO, lists objects by prefix. The dictionary format might look like:
            {
                "subfolderA": {
                    "file1.parquet": None,
                    "file2.json": None
                },
                "subfolderB": {
                    "nested": {
                        "file3.parquet": None
                    }
                }
            }
        """
        pass

    @abc.abstractmethod
    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to the given path (local or cloud/object storage) in Parquet format.
        For local, use pyarrow.parquet.write_table.
        For cloud, you may rely on s3fs or a custom upload method.
        """
        pass

    @abc.abstractmethod
    def read_parquet(self, path: str) -> pa.Table:
        """
        Reads and returns JSON data from the given path.
        Raises FileNotFoundError, ValueError, etc. on error.
        """
        pass