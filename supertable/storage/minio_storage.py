import io
import json
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Any, Dict, List
from minio import Minio
from minio.error import S3Error

from supertable.storage.storage_interface import StorageInterface


class MinioStorage(StorageInterface):
    """
    A MinIO-based implementation of StorageInterface.
    Leverages the official MinIO Python client.

    Usage Example:
        from minio import Minio
        minio_client = Minio(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        storage = MinioStorage(bucket_name="my-bucket", client=minio_client)
    """

    def __init__(self, bucket_name: str, client: Minio):
        self.bucket_name = bucket_name
        self.client = client

    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Reads and returns JSON data from MinIO at the given object path.
        Raises FileNotFoundError or ValueError on error.
        """
        try:
            response = self.client.get_object(self.bucket_name, path)
            data = response.read()
            response.close()
            response.release_conn()
        except S3Error as e:
            # MinIO raises S3Error if the object is missing
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Writes JSON data to the given object path in MinIO.
        Overwrites if it already exists.
        """
        json_bytes = json.dumps(data).encode("utf-8")
        try:
            # Content length is required by put_object
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=path,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json",
            )
        except S3Error as e:
            raise RuntimeError(f"Failed to write JSON to {path}: {e}") from e

    def exists(self, path: str) -> bool:
        """
        Returns True if the object at 'path' exists in MinIO; False otherwise.
        """
        try:
            self.client.stat_object(self.bucket_name, path)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise

    def size(self, path: str) -> int:
        """
        Returns the size in bytes of the object at 'path'.
        Raises FileNotFoundError if the object doesn't exist.
        """
        try:
            stat = self.client.stat_object(self.bucket_name, path)
            return stat.size
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def makedirs(self, path: str) -> None:
        """
        MinIO (S3-like) does not have real directories. This is effectively a no-op.
        However, if you want to simulate a 'folder', you could create a placeholder object
        with a trailing slash. We'll leave it as no-op.
        """
        # No-op in MinIO
        return

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists objects in MinIO (non-recursive) that start with 'path'.
        Because S3/MinIO doesn't natively support single-level listings, we approximate:
          - We set prefix=path
          - We do NOT set recursive=True, so it only yields immediate children at that 'directory'.
          - We'll apply 'pattern' filtering with fnmatch.
        Note: For deeper control, you might need to parse the returned keys or use custom logic.
        """
        results = []
        # If you want to simulate "non-recursive," try delimiter='/' and prefix=path if path ends with '/'.
        # For a simpler approach, just do a full list and filter keys that don't have extra '/' beyond prefix.
        # We'll do a partial approach here:
        # Ensuring path has a trailing slash if you truly want just that 'directory'
        if not path.endswith("/"):
            path += "/"

        # S3-like listing
        # If you prefer a non-recursive approach with delimiter, pass delimiter='/'.
        objects = self.client.list_objects(self.bucket_name, prefix=path, recursive=False)
        for obj in objects:
            key = obj.object_name
            # If there's an additional slash beyond the prefix, it means a deeper "directory"
            if "/" in key[len(path):].strip("/"):
                # skip deeper levels
                continue
            if fnmatch.fnmatch(key, pattern):
                results.append(key)

        return results

    def delete(self, path: str) -> None:
        """
        Deletes the object at 'path' from MinIO.
        Raises FileNotFoundError if the object does not exist.
        """
        if not self.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        try:
            self.client.remove_object(self.bucket_name, path)
        except S3Error as e:
            raise RuntimeError(f"Failed to delete {path}: {e}") from e

    def get_directory_structure(self, path: str) -> dict:
        """
        Recursively builds and returns a nested dictionary that represents
        the folder structure under 'path'. For example:
        {
          "subfolder1": {
            "fileA.txt": None,
            "fileB.json": None
          },
          "subfolder2": {
            "nested": {
              "fileC.parquet": None
            }
          }
        }

        We'll do a full recursive list of all objects whose key starts with `path`, 
        then parse them to build a nested dictionary.
        """
        directory_structure = {}
        if path and not path.endswith("/"):
            path += "/"

        # List all objects under this prefix
        for obj in self.client.list_objects(self.bucket_name, prefix=path, recursive=True):
            full_key = obj.object_name
            # Remove the leading path from the key
            rel_key = full_key[len(path):] if path else full_key
            # If there's an accidental leading slash, remove it
            if rel_key.startswith("/"):
                rel_key = rel_key[1:]
            if not rel_key:
                continue  # skip if it's exactly the path folder

            # Split into folders/files
            parts = rel_key.split("/")
            parent = directory_structure
            for subfolder in parts[:-1]:
                parent = parent.setdefault(subfolder, {})
            # Mark the file as None for consistency
            parent[parts[-1]] = None

        return directory_structure

    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to MinIO as a Parquet object.
        We'll serialize in-memory, then upload via put_object.
        """
        # Convert the Arrow table to Parquet bytes
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Upload to MinIO
        data_size = len(buffer.getvalue())
        try:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=path,
                data=buffer,
                length=data_size,
                content_type="application/octet-stream"
            )
        except S3Error as e:
            raise RuntimeError(f"Failed to write Parquet to {path}: {e}") from e
