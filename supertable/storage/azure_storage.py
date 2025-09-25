import io
import json
import fnmatch
import os
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from supertable.storage.storage_interface import StorageInterface


def _parse_abfss(uri: str) -> Tuple[str, str, str, str]:
    """
    abfss://{container}@{account}.dfs.core.windows.net/{prefix}
    -> (account, container, blob_endpoint, prefix)
    """
    if not uri.lower().startswith("abfss://"):
        raise ValueError("ABFSS URI must start with 'abfss://'")
    rest = uri[8:]
    if "/" in rest:
        authority, prefix = rest.split("/", 1)
    else:
        authority, prefix = rest, ""
    if "@" not in authority or ".dfs.core.windows.net" not in authority:
        raise ValueError(f"Malformed ABFSS authority: {authority}")
    container, host = authority.split("@", 1)
    account = host.split(".dfs.core.windows.net")[0]
    blob_endpoint = f"https://{account}.blob.core.windows.net"
    return account, container, blob_endpoint, prefix.strip("/")


class AzureBlobStorage(StorageInterface):
    """
    Azure Blob backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single blob if it exists, otherwise deletes all blobs under prefix

    Supports construction via:
      - explicit (container_name, blob_service_client)
      - from_env(): reads SUPERTABLE_HOME (abfss://...), AZURE_* env vars, and uses
                    DefaultAzureCredential when no key/sas/connection string is provided.
    """

    def __init__(self, container_name: str, blob_service_client: BlobServiceClient, base_prefix: str = ""):
        self.container_name = container_name
        self.svc = blob_service_client
        self.container = self.svc.get_container_client(container_name)
        self.base_prefix = base_prefix.strip("/")

    # -------------------------
    # Factory
    # -------------------------
    @classmethod
    def from_env(cls) -> "AzureBlobStorage":
        """
        Build AzureBlobStorage from environment variables.

        Recognized env:
          - SUPERTABLE_HOME (abfss://container@account.dfs.../<prefix>)
          - AZURE_CONTAINER (fallback container)
          - SUPERTABLE_PREFIX (optional base prefix)
          - AZURE_STORAGE_CONNECTION_STRING
          - AZURE_STORAGE_ACCOUNT
          - AZURE_BLOB_ENDPOINT
          - AZURE_STORAGE_KEY
          - AZURE_SAS_TOKEN
          - AZURE_AUTH_MODE (informational; if AAD and no secrets -> DefaultAzureCredential)
        """
        # Prefer ABFSS home if present
        st_home = os.getenv("SUPERTABLE_HOME", "")
        account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
        container = os.getenv("AZURE_CONTAINER", "")
        endpoint = os.getenv("AZURE_BLOB_ENDPOINT", "")
        base_prefix = os.getenv("SUPERTABLE_PREFIX", "")

        if st_home.lower().startswith("abfss://"):
            try:
                acc2, cont2, endpoint2, prefix2 = _parse_abfss(st_home)
                account = account or acc2
                container = container or cont2
                endpoint = endpoint or endpoint2
                # If user also set SUPERTABLE_PREFIX, keep it, else use abfss prefix
                if not base_prefix and prefix2:
                    base_prefix = prefix2
            except ValueError:
                # keep going with explicit env
                pass

        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
        key = os.getenv("AZURE_STORAGE_KEY", "")
        sas = os.getenv("AZURE_SAS_TOKEN", "")

        if not endpoint:
            if not account:
                raise RuntimeError("Azure configuration requires AZURE_STORAGE_ACCOUNT or ABFSS SUPERTABLE_HOME")
            endpoint = f"https://{account}.blob.core.windows.net"

        # Build BlobServiceClient with the best available credential
        if conn_str:
            svc = BlobServiceClient.from_connection_string(conn_str)
        else:
            if key:
                svc = BlobServiceClient(account_url=endpoint, credential=key)
            elif sas:
                if not sas.startswith("?"):
                    sas = "?" + sas
                svc = BlobServiceClient(account_url=endpoint + sas)
            else:
                # Managed Identity / AAD
                from azure.identity import DefaultAzureCredential
                svc = BlobServiceClient(account_url=endpoint, credential=DefaultAzureCredential())

        if not container:
            raise RuntimeError("Azure configuration requires AZURE_CONTAINER (or abfss container in SUPERTABLE_HOME)")

        return cls(container_name=container, blob_service_client=svc, base_prefix=base_prefix)

    # -------------------------
    # Helpers
    # -------------------------
    def _with_base(self, path: str) -> str:
        path = path.strip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{path}" if path else self.base_prefix
        return path

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

        for item in self.container.walk_blobs(name_starts_with=prefix, delimiter="/"):
            name = getattr(item, "name", None) if hasattr(item, "name") else None
            if not name:
                continue

            if name.endswith("/"):
                part = name[len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)
            else:
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
        path = self._with_base(path)
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
        path = self._with_base(path)
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
        return self._blob_exists(self._with_base(path))

    def size(self, path: str) -> int:
        path = self._with_base(path)
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
        path = self._with_base(path)
        if path and not path.endswith("/"):
            path = path + "/"

        children = self._one_level_children(path)
        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        path = self._with_base(path)
        # exact blob?
        if self._blob_exists(path):
            self.container.delete_blob(path)
            return

        # prefix recursive
        prefix = path if path.endswith("/") else f"{path}/"
        to_delete = [b.name for b in self.container.list_blobs(name_starts_with=prefix)]

        if not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")

        for name in to_delete:
            self.container.delete_blob(name)

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        path = self._with_base(path)
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
        path = self._with_base(path)
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
        path = self._with_base(path)
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
        path = self._with_base(path)
        blob = self.container.get_blob_client(path)
        blob.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )

    def read_bytes(self, path: str) -> bytes:
        path = self._with_base(path)
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
        src_path = self._with_base(src_path)
        dst_path = self._with_base(dst_path)
        src = self.container.get_blob_client(src_path).url
        dst = self.container.get_blob_client(dst_path)
        poller = dst.start_copy_from_url(src)
        dst.get_blob_properties()
