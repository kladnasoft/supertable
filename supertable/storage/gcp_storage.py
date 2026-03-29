# route: supertable.storage.gcp_storage
import io
import json
import fnmatch
import os

from supertable.config.settings import settings
from typing import Any, Dict, List, Optional
from pathlib import Path

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
        base_prefix: str = "",
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
        base_prefix : str
            Optional prefix prepended to all keys
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
        self.base_prefix = base_prefix.strip("/") if base_prefix else ""

    # -------------------------
    # Factory
    # -------------------------
    @classmethod
    def from_env(cls) -> "GCSStorage":
        """
        Build GCSStorage from environment variables.

        Recognized env:
          - GCS_BUCKET / STORAGE_BUCKET (bucket name, default: 'supertable')
          - SUPERTABLE_PREFIX (optional base prefix)
          - GOOGLE_APPLICATION_CREDENTIALS (path to service account JSON)
          - GCP_SA_JSON (raw JSON string, alternative to file path)
          - GCP_PROJECT (optional project override)
        """
        bucket = settings.effective_gcs_bucket
        base_prefix = settings.SUPERTABLE_PREFIX
        creds_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        sa_json = settings.GCP_SA_JSON
        project = settings.GCP_PROJECT or None

        if creds_path and Path(creds_path).is_file():
            client = storage.Client.from_service_account_json(creds_path, project=project)
        elif sa_json:
            import json as _json
            from google.oauth2 import service_account
            info = _json.loads(sa_json)
            creds = service_account.Credentials.from_service_account_info(info)
            client = storage.Client(project=project or creds.project_id, credentials=creds)
        else:
            # Application Default Credentials (ADC)
            client = storage.Client(project=project)

        return cls(bucket=bucket, client=client, base_prefix=base_prefix)

    # -------------------------
    # Optional: DuckDB path / presign
    # -------------------------
    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        """Return a GCS path usable by DuckDB (gcs:// or https:// form)."""
        key = (key or "").lstrip("/")
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        prefer_httpfs = (
            prefer_httpfs if prefer_httpfs is not None
            else settings.SUPERTABLE_DUCKDB_USE_HTTPFS
        )
        if prefer_httpfs:
            return f"https://storage.googleapis.com/{self.bucket_name}/{full_key}"
        return f"gcs://{self.bucket_name}/{full_key}"

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        """Return a presigned (signed) GET URL for the object."""
        from datetime import timedelta
        full_key = self._with_base(key.lstrip("/"))
        blob = self.bucket.blob(full_key)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=expiry_seconds),
            method="GET",
        )

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
        path = self._with_base(path)
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
        path = self._with_base(path)
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        blob = self.bucket.blob(path)
        blob.upload_from_string(payload, content_type="application/json")

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
    def exists(self, path: str) -> bool:
        return self._blob_exists(self._with_base(path))

    def size(self, path: str) -> int:
        path = self._with_base(path)
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        # size is populated on the Blob returned by get_blob
        return int(blob.size or 0)

    def makedirs(self, path: str) -> None:
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
        path = self._with_base(path)
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
        Delete single object if it exists, otherwise recursively delete a "directory" (prefix).
        Raises FileNotFoundError if nothing exists at the path or under the prefix.
        """
        path = self._with_base(path)
        # Try exact object first
        blob = self.bucket.get_blob(path)
        if blob is not None:
            blob.delete()
            return

        # Fall back to prefix recursive delete
        prefix = self._normalize_dir_prefix(path)
        to_delete = list(self.client.list_blobs(self.bucket_name, prefix=prefix))

        if not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")

        for b in to_delete:
            b.delete()

    def delete_prefix(self, path: str) -> None:
        """Explicit recursive delete for a prefix."""
        path = self._with_base(path)
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
        path = self._with_base(path)
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
        path = self._with_base(path)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        blob = self.bucket.blob(path)
        blob.upload_from_string(data, content_type="application/octet-stream")

    def read_parquet(self, path: str) -> pa.Table:
        """
        Reads and returns a PyArrow Table.
        """
        path = self._with_base(path)
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
    def write_bytes(self, path: str, data: bytes) -> None:
        path = self._with_base(path)
        blob = self.bucket.blob(path)
        blob.upload_from_string(data)

    def read_bytes(self, path: str) -> bytes:
        path = self._with_base(path)
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError(f"File not found: {path}")
        return blob.download_as_bytes()

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        src_path = self._with_base(src_path)
        dst_path = self._with_base(dst_path)
        src_blob = self._get_blob_raise(src_path)
        self.bucket.copy_blob(src_blob, self.bucket, new_name=dst_path)