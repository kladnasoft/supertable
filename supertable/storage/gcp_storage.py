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
    Google Cloud Storage backend.

    Notes
    -----
    - GCS has "flat" namespace; "directories" are simulated by common prefixes.
    - For one-level listing parity with local, we use delimiter="/" and then collect:
        * blobs (files) directly under the prefix
        * common prefixes (subdirs) reported by the API
    """

    def __init__(
        self,
        bucket: str,
        credentials_path: Optional[str] = None,
        client: Optional[storage.Client] = None,
        **_: Any,
    ) -> None:
        """
        Parameters
        ----------
        bucket : str
            Name of the GCS bucket
        credentials_path : Optional[str]
            If provided, use this service account JSON
        client : Optional[storage.Client]
            Pre-initialized client (for testing/DI)
        """
        if client is not None:
            self.client = client
        else:
            if credentials_path:
                self.client = storage.Client.from_service_account_json(credentials_path)
            else:
                self.client = storage.Client()

        self.bucket_name = bucket
        self.bucket = self.client.bucket(bucket)

    # -------------------------
    # Helpers
    # -------------------------
    @staticmethod
    def _normalize_dir_prefix(path: str) -> str:
        """Ensure directory-like prefix ends with '/' (except empty)."""
        if not path:
            return ""
        return path if path.endswith("/") else (path + "/")

    def _blob_exists(self, path: str) -> bool:
        return self.bucket.blob(path).exists(self.client)

    def _get_blob_raise(self, path: str):
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        return blob

    # -------------------------
    # JSON
    # -------------------------
    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Reads and returns a JSON object.
        Raises FileNotFoundError if missing, ValueError if empty/invalid JSON.
        """
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
        return int(blob.size or 0)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """
        No-op for GCS, but if you want directory markers, uncomment below.
        """
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

        children: List[str] = []
        seen = set()

        # Iterate pages once; gather both blobs and common prefixes.
        for page in it.pages:
            # Files directly under this prefix
            for blob in page:
                key = blob.name
                if key == prefix:
                    continue  # directory marker
                part = key[len(prefix):]
                if "/" in part:
                    part = part.split("/", 1)[0]
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

            # "Directories" (common prefixes) one level down
            for pfx in getattr(page, "prefixes", []):
                part = pfx[len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

        # Deterministic order across providers
        children.sort()

        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [prefix + c for c in filtered]

    # -------------------------
    # Delete (single or recursive on prefix)
    # -------------------------
    def delete(self, path: str) -> None:
        """
        Delete single object or recursively delete a "directory" (prefix).
        """
        if path.endswith("/"):
            # recursive delete
            prefix = self._normalize_dir_prefix(path)
            blobs = list(self.client.list_blobs(self.bucket_name, prefix=prefix))
            for b in blobs:
                b.delete()
            return

        # single object
        blob = self.bucket.get_blob(path)
        if blob is None:
            # mimic local semantics: deleting nonexistent path is not fatal
            return
        blob.delete()

    def delete_prefix(self, path: str) -> None:
        """Explicit recursive delete for a prefix."""
        prefix = self._normalize_dir_prefix(path) if path else path
        for blob in self.client.list_blobs(self.bucket_name, prefix=prefix):
            blob.delete()

    # -------------------------
    # Directory structure (recursive)
    # -------------------------
    def get_directory_structure(self, path: str) -> Dict[str, Any]:
        """
        Build a nested dict mirroring keys. Leaf files -> None, folders -> dict.
        Potentially expensive for large buckets.
        """
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
        """
        Reads and returns a PyArrow Table.
        """
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        data = blob.download_as_bytes()
        if not data:
            raise ValueError(f"File is empty: {path}")
        return pq.read_table(io.BytesIO(data))

    # -------------------------
    # Raw bytes / text
    # -------------------------
    def write_bytes(self, path: str, data: bytes, content_type: Optional[str] = None) -> None:
        blob = self.bucket.blob(path)
        if content_type:
            blob.upload_from_string(data, content_type=content_type)
        else:
            blob.upload_from_string(data)

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
