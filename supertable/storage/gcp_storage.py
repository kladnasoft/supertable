import io
import json
import fnmatch
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import NotFound

from supertable.storage.storage_interface import StorageInterface


class GCSStorage(StorageInterface):
    """
    Google Cloud Storage backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single object if it exists, otherwise deletes all objects under prefix
    """

    def __init__(self, bucket_name: str, client: Optional[storage.Client] = None):
        self.bucket_name = bucket_name
        self.client = client or storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    # -------------------------
    # Helpers
    # -------------------------
    def _blob_exists(self, name: str) -> bool:
        blob = self.bucket.get_blob(name)  # issues a GET; returns None if missing
        return blob is not None

    def _get_blob_raise(self, name: str):
        blob = self.bucket.get_blob(name)
        if blob is None:
            raise FileNotFoundError(f"File not found: {name}")
        return blob

    def _normalize_dir_prefix(self, prefix: str) -> str:
        if prefix and not prefix.endswith("/"):
            return prefix + "/"
        return prefix

    # -------------------------
    # JSON
    # -------------------------
    def read_json(self, path: str) -> Dict[str, Any]:
        try:
            blob = self._get_blob_raise(path)
            data = blob.download_as_bytes()
        except NotFound as e:
            raise FileNotFoundError(f"File not found: {path}") from e

        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        blob = self.bucket.blob(path)
        blob.upload_from_string(payload, content_type="application/json")

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
    def exists(self, path: str) -> bool:
        return self._blob_exists(path)

    def size(self, path: str) -> int:
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        # size is populated on the Blob returned by get_blob
        return int(blob.size)

    def makedirs(self, path: str) -> None:
        # No-op for object storage. Optionally create a marker:
        # marker = self.bucket.blob(self._normalize_dir_prefix(path))
        # marker.upload_from_string(b"")
        pass

    # -------------------------
    # Listing (one-level parity)
    # -------------------------
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Local parity:
        - treat `path` as directory prefix (append '/' if needed)
        - return immediate children (one level) under that prefix
        - apply fnmatch to the child name
        - return full keys (prefix + child)
        """
        prefix = self._normalize_dir_prefix(path)

        # Use delimiter='/' to get immediate children (prefixes + items at this level)
        it = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter="/")

        children = []
        seen = set()

        # Contents at this level (files)
        for blob in it:
            key = blob.name
            if key == prefix:
                continue  # folder marker
            part = key[len(prefix):]
            # If deeper (shouldn't happen with delimiter), guard anyway
            if "/" in part:
                part = part.split("/", 1)[0]
            if part and part not in seen:
                seen.add(part)
                children.append(part)

        # Sub-prefixes (directories) one level down
        # Access prefixes via pages to ensure populated
        for page in it.pages:
            for pfx in getattr(page, "prefixes", []):
                # pfx looks like '<prefix>child/'
                part = pfx[len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [prefix + c for c in filtered]

    # -------------------------
    # Delete (single or recursive on prefix)
    # -------------------------
    def delete(self, path: str) -> None:
        # exact object?
        if self._blob_exists(path):
            self.bucket.delete_blob(path)
            return

        # try as prefix (recursive)
        prefix = self._normalize_dir_prefix(path)
        to_delete = list(self.client.list_blobs(self.bucket_name, prefix=prefix))

        if not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")

        # Best-effort loop (GCS doesn't have a multi-delete API)
        for blob in to_delete:
            self.bucket.delete_blob(blob.name)

    # -------------------------
    # Directory structure (recursive)
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        root: Dict[str, Any] = {}
        prefix = self._normalize_dir_prefix(path) if path else path

        for blob in self.client.list_blobs(self.bucket_name, prefix=prefix):
            key = blob.name
            if key.endswith("/"):
                # ignore "directory markers"
                continue
            suffix = key[len(prefix):] if prefix else key
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
        blob = self.bucket.blob(path)
        blob.upload_from_string(data, content_type="application/octet-stream")

    def read_parquet(self, path: str) -> pa.Table:
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"Parquet file not found: {path}")
        data = blob.download_as_bytes()
        try:
            return pq.read_table(io.BytesIO(data))
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
    def write_bytes(self, path: str, data: bytes) -> None:
        blob = self.bucket.blob(path)
        blob.upload_from_string(data, content_type="application/octet-stream")

    def read_bytes(self, path: str) -> bytes:
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        return blob.download_as_bytes()

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        src_blob = self._get_blob_raise(src_path)
        self.bucket.copy_blob(src_blob, self.bucket, new_name=dst_path)
