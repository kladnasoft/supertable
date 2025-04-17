import io
import json
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Any, Dict, List
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError

from supertable.storage.storage_interface import StorageInterface


class AzureStorage(StorageInterface):
    """
    An Azure Blob Storage-based implementation of StorageInterface
    using the azure-storage-blob client library.

    Usage Example:
        from azure.storage.blob import BlobServiceClient

        # Using a connection string:
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=...")

        # Or using account URL + credential:
        # blob_service_client = BlobServiceClient(account_url="https://<account>.blob.core.windows.net", credential="...")

        storage = AzureStorage(
            container_name="my-container",
            blob_service_client=blob_service_client
        )
    """

    def __init__(self, container_name: str, blob_service_client: BlobServiceClient):
        self.container_name = container_name
        self.blob_service_client = blob_service_client
        self.container_client: ContainerClient = self.blob_service_client.get_container_client(container_name)

    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Downloads and returns JSON data from Azure Blob Storage at the given blob path.
        Raises FileNotFoundError or ValueError on error.
        """
        blob_client = self.container_client.get_blob_client(blob=path)
        try:
            stream = blob_client.download_blob()
            data = stream.readall()
        except ResourceNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")

        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Uploads JSON data to the given blob path in Azure Blob Storage.
        Overwrites if it already exists.
        """
        blob_client = self.container_client.get_blob_client(blob=path)
        json_bytes = json.dumps(data).encode("utf-8")
        blob_client.upload_blob(json_bytes, overwrite=True, content_type="application/json")

    def exists(self, path: str) -> bool:
        """
        Returns True if the blob at 'path' exists in Azure; False otherwise.
        """
        blob_client = self.container_client.get_blob_client(blob=path)
        try:
            blob_client.get_blob_properties()  # Will raise ResourceNotFoundError if missing
            return True
        except ResourceNotFoundError:
            return False

    def size(self, path: str) -> int:
        """
        Returns the size in bytes of the blob at 'path'.
        Raises FileNotFoundError if the blob doesn't exist.
        """
        blob_client = self.container_client.get_blob_client(blob=path)
        try:
            props = blob_client.get_blob_properties()
            return props.size
        except ResourceNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")

    def makedirs(self, path: str) -> None:
        """
        Azure Blob Storage does not have real directories, so this is effectively a no-op.
        However, you can simulate folder creation by creating zero-length blobs with a trailing slash if needed.
        We'll leave it empty here.
        """
        pass

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists blobs (non-recursive) in Azure that start with 'path' as the "virtual folder."
        We'll emulate single-level listing by specifying a delimiter='/' and verifying
        there's no deeper slash beyond the immediate level. Then apply 'pattern' filtering.
        """
        results = []
        # Ensure path ends with a slash (so we treat it like a folder)
        prefix = path
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        blob_list = self.container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
        for blob_prefix in blob_list:
            # blob_prefix could be a BlobPrefix or BlobProperties object
            # If it's a subfolder (BlobPrefix), skip. We only want direct children.
            if hasattr(blob_prefix, "prefix"):
                # It's a subdirectory. Skip it.
                continue
            # Otherwise, it's a blob
            if hasattr(blob_prefix, "name"):
                full_key = blob_prefix.name
                sub_part = full_key[len(prefix) :] if prefix else full_key
                # If there is a slash in sub_part, it's deeper than single-level
                if "/" not in sub_part:
                    if fnmatch.fnmatch(full_key, pattern):
                        results.append(full_key)
        return results

    def delete(self, path: str) -> None:
        """
        Deletes the blob at 'path' from Azure.
        Raises FileNotFoundError if the blob does not exist.
        """
        if not self.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        blob_client = self.container_client.get_blob_client(blob=path)
        blob_client.delete_blob()

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

        We'll perform a recursive listing of all blobs whose key (name) starts with 'path'
        and parse them into a nested dict. We'll do a simple name_starts_with=prefix,
        then parse the returned blobs for subfolders and file objects.
        """
        directory_structure = {}
        prefix = path
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        # We'll implement a simple approach with container_client.list_blobs(...) and parse.
        continuation_token = None
        while True:
            listing = self.container_client.list_blobs(name_starts_with=prefix, continuation_token=continuation_token)

            # If there's no delimiter, it will be fully recursive. We'll handle subfolders ourselves by splitting names.
            for blob in listing:
                full_key = blob.name
                # Remove the leading prefix
                rel_key = full_key[len(prefix) :] if prefix else full_key
                # If it's empty or slash, skip
                if not rel_key or rel_key.startswith("/"):
                    continue

                parts = rel_key.split("/")
                parent = directory_structure
                for sub in parts[:-1]:
                    parent = parent.setdefault(sub, {})
                parent[parts[-1]] = None

            if not listing.next_marker:
                break
            continuation_token = listing.next_marker

        return directory_structure

    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to Azure as a Parquet blob.
        We'll serialize in-memory, then upload the data via upload_blob.
        """
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob_client = self.container_client.get_blob_client(blob=path)
        blob_client.upload_blob(buffer, overwrite=True, content_type="application/octet-stream")
