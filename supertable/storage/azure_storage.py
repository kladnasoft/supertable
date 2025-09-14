import io
import json
import fnmatch
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from supertable.storage.storage_interface import StorageInterface


class AzureBlobStorage(StorageInterface):
    """
    Azure Blob backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single blob if it exists, otherwise deletes all blobs under prefix
    """

    def __init__(self, container_name: str, blob_service_client: BlobServiceClient):
        self.container_name = container_name
        self.svc = blob_service_client
        self.container = self.svc.get_container_client(container_name)

    # -------------------------
    # Helpers
    # -------------------------
    def _blob_exists(self, name: str) -> bool:
        try:
            self.container.get_blob_client(name).get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def _one_level_children(self, prefix: str) -> List[str]:
        """
        Return immediate child names under prefix using delimiter="/".
        """
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        children = []
        seen = set()

        # Azure supports name_starts_with + delimiter on walk_blobs
        for item in self.container.walk_blobs(name_starts_with=prefix, delimiter="/"):
            if hasattr(item, "name"):
                # item is a BlobPrefix or BlobProperties depending on type
                name = getattr(item, "name", None)
            else:
                name = None

            if not name:
                continue

            if name.endswith("/"):
                # This is a "virtual directory" (prefix)
                part = name[len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)
            else:
                # This is a blob at this level
                part = name[len(prefix):]
                if "/" in part:
                    part = part.split("/", 1)[0]
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

        return children

    # -------------------------
    # JSON
    # -------------------------
    def read_json(self, path: str) -> Dict[str, Any]:
        blob = self.container.get_blob_client(path)
        try:
            data = blob.download_blob().readall()
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e

        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        blob = self.container.get_blob_client(path)
        blob.upload_blob(
            payload,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
    def exists(self, path: str) -> bool:
        return self._blob_exists(path)

    def size(self, path: str) -> int:
        try:
            props = self.container.get_blob_client(path).get_blob_properties()
            return int(props.size)
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e

    def makedirs(self, path: str) -> None:
        # No-op for object storage; optionally create a marker blob if desired.
        pass

    # -------------------------
    # Listing
    # -------------------------
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Local parity: one-level children under prefix `path`, fnmatch on child name.
        """
        if path and not path.endswith("/"):
            path = path + "/"

        children = self._one_level_children(path)
        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        # exact blob?
        if self._blob_exists(path):
            self.container.delete_blob(path)
            return

        # prefix recursive
        prefix = path if path.endswith("/") else f"{path}/"
        to_delete = [b.name for b in self.container.list_blobs(name_starts_with=prefix)]

        if not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")

        # Batch delete best-effort (no single API to batch delete; delete per blob)
        for name in to_delete:
            self.container.delete_blob(name)

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        root = {}
        if path and not path.endswith("/"):
            path = path + "/"

        for blob in self.container.list_blobs(name_starts_with=path):
            key = blob.name
            if key.endswith("/"):
                continue
            suffix = key[len(path):] if path else key
            parts = [p for p in suffix.split("/") if p]
            if not parts:
                continue
            cursor = root
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    cursor[part] = None
                else:
                    cursor = cursor.setdefault(part, {})
        return root

    # -------------------------
    # Parquet
    # -------------------------
    def write_parquet(self, table: pa.Table, path: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        blob = self.container.get_blob_client(path)
        blob.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )

    def read_parquet(self, path: str) -> pa.Table:
        blob = self.container.get_blob_client(path)
        try:
            data = blob.download_blob().readall()
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"Parquet file not found: {path}") from e
        try:
            return pq.read_table(io.BytesIO(data))
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
    def write_bytes(self, path: str, data: bytes) -> None:
        blob = self.container.get_blob_client(path)
        blob.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )

    def read_bytes(self, path: str) -> bytes:
        blob = self.container.get_blob_client(path)
        try:
            return blob.download_blob().readall()
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        src = self.container.get_blob_client(src_path).url
        dst = self.container.get_blob_client(dst_path)
        # Start a server-side copy and wait for completion
        poller = dst.start_copy_from_url(src)
        # Optionally wait; for parity we ensure destination exists by checking properties
        dst.get_blob_properties()
