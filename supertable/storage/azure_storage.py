# route: supertable.storage.azure_storage
import io
import json
import fnmatch
import os
import threading
from datetime import datetime, timedelta, timezone

from supertable.config.defaults import logger
from supertable.config.settings import settings
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
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

        Recognized env (common names checked first, Azure-specific as fallback):
          - SUPERTABLE_HOME (abfss://container@account.dfs.../<prefix>)
          - STORAGE_BUCKET / AZURE_CONTAINER (container name)
          - STORAGE_ENDPOINT_URL / AZURE_BLOB_ENDPOINT (blob endpoint)
          - STORAGE_ACCESS_KEY / AZURE_STORAGE_KEY (account key)
          - SUPERTABLE_PREFIX (optional base prefix)
          - AZURE_STORAGE_CONNECTION_STRING (Azure-only)
          - AZURE_STORAGE_ACCOUNT (Azure-only)
          - AZURE_SAS_TOKEN (Azure-only)
          - AZURE_AUTH_MODE (informational; if AAD and no secrets -> DefaultAzureCredential)
        """
        # Prefer ABFSS home if present
        st_home = settings.SUPERTABLE_HOME
        account = settings.AZURE_STORAGE_ACCOUNT
        container = settings.effective_storage_bucket
        endpoint = settings.effective_storage_endpoint
        base_prefix = settings.SUPERTABLE_PREFIX

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

        conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
        key = settings.effective_storage_access_key
        sas = settings.AZURE_SAS_TOKEN

        if not endpoint and account:
            endpoint = f"https://{account}.blob.core.windows.net"
        if not (endpoint or conn_str):
            raise RuntimeError("Azure configuration requires AZURE_STORAGE_ACCOUNT, STORAGE_ENDPOINT_URL, AZURE_STORAGE_CONNECTION_STRING, or ABFSS SUPERTABLE_HOME")

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
            raise RuntimeError("Azure configuration requires STORAGE_BUCKET / AZURE_CONTAINER (or abfss container in SUPERTABLE_HOME)")

        storage = cls(container_name=container, blob_service_client=svc, base_prefix=base_prefix)
        storage._ensure_container_exists()
        return storage

    def _ensure_container_exists(self) -> None:
        """Create the container if it is missing, as MinioStorage does its bucket.

        A new storage account has no containers, and without one every write
        fails with ContainerNotFound. A credential limited to one container
        (a container SAS, a role scoped to it) may be refused the check or the
        create, and the account may be unreachable just now; none of that is
        raised here, so construction behaves as before and the first real
        operation reports the problem.
        """
        try:
            if not self.container.exists():
                self.container.create_container()
        except ResourceExistsError:
            pass  # another process created it in between
        except AzureError as e:
            logger.debug(f"[azure-storage] could not check or create container '{self.container_name}': {e}")

    # -------------------------
    # Optional: DuckDB path / presign
    # -------------------------
    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        """Return an Azure path usable by DuckDB (azure:// or https:// form)."""
        key = (key or "").lstrip("/")
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        prefer_httpfs = (
            prefer_httpfs if prefer_httpfs is not None
            else settings.SUPERTABLE_DUCKDB_USE_HTTPFS
        )
        if prefer_httpfs:
            account_url = self.svc.url.rstrip("/")
            return f"{account_url}/{self.container_name}/{full_key}"
        return f"azure://{self.container_name}/{full_key}"

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        """Return a presigned (SAS) GET URL for the blob."""
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions

        full_key = self._with_base(key)
        blob_client = self.container.get_blob_client(full_key)

        # Attempt to use the account key from the credential for SAS generation.
        # If using DefaultAzureCredential (AAD), user delegation key is needed instead.
        credential = self.svc.credential
        account_name = self.svc.account_name

        if hasattr(credential, "account_key"):
            account_key = credential.account_key
        elif isinstance(credential, str):
            account_key = credential
        else:
            # AAD / managed identity: use user delegation key
            expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expiry_seconds)
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=self.container_name,
                blob_name=full_key,
                user_delegation_key=self._user_delegation_key(expiry_time),
                permission=BlobSasPermissions(read=True),
                expiry=expiry_time,
            )
            return f"{blob_client.url}?{sas_token}"

        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expiry_seconds)
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=self.container_name,
            blob_name=full_key,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry_time,
        )
        return f"{blob_client.url}?{sas_token}"

    # -------------------------
    # Helpers
    # -------------------------
    def _with_base(self, path: str) -> str:
        path = path.strip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{path}" if path else self.base_prefix
        return path

    # A user delegation key costs a round trip to Azure and signs any number
    # of SAS URLs until it expires, so one is fetched and reused rather than
    # one per presigned file (a query presigns every file it reads).
    _DELEGATION_KEY_LIFETIME = timedelta(hours=6)
    _delegation_lock = threading.Lock()

    def _user_delegation_key(self, valid_until: datetime):
        """A user delegation key that is still valid at *valid_until*."""
        with self._delegation_lock:
            cached = getattr(self, "_delegation_key", None)
            if cached is not None and cached[1] >= valid_until:
                return cached[0]
            now = datetime.now(timezone.utc)
            expiry = max(now + self._DELEGATION_KEY_LIFETIME, valid_until)
            # Started a little in the past: tolerates clock skew with Azure.
            key = self.svc.get_user_delegation_key(now - timedelta(minutes=5), expiry)
            self._delegation_key = (key, expiry)
            return key

    # An account with a hierarchical namespace (Data Lake Gen2) has real
    # directories, and the Blob API shows each as an empty blob marked
    # hdi_isfolder: in flat listings, before its contents. It keeps one after
    # its files are deleted and refuses to delete one that is not empty. A
    # flat account has none, so the handling below changes nothing there.
    @staticmethod
    def _is_folder(item: Any) -> bool:
        return (getattr(item, "metadata", None) or {}).get("hdi_isfolder") == "true"

    def _delete_folders(self, path: str) -> int:
        """Delete the directories under *path* (a full key) and *path* itself, deepest first."""
        path = path.rstrip("/")
        folders = [
            b.name
            for b in self.container.list_blobs(name_starts_with=f"{path}/" if path else "", include=["metadata"])
            if self._is_folder(b)
        ]
        if path:
            try:
                if self._is_folder(self.container.get_blob_client(path).get_blob_properties()):
                    folders.append(path)
            except ResourceNotFoundError:
                pass
        for name in sorted(folders, key=lambda n: n.count("/"), reverse=True):
            try:
                self.container.delete_blob(name)
            except ResourceNotFoundError:
                pass
        return len(folders)

    def _blob_exists(self, name: str) -> bool:
        """A file at *name*; a directory is not one, as on a flat account."""
        try:
            return not self._is_folder(self.container.get_blob_client(name).get_blob_properties())
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
        filtered.sort()
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
        to_delete = [
            b.name for b in self.container.list_blobs(name_starts_with=prefix, include=["metadata"])
            if not self._is_folder(b)
        ]

        for name in to_delete:
            self.container.delete_blob(name)

        # The emptied directories go too (Data Lake Gen2).
        if self._delete_folders(path) == 0 and not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")

    def delete_tree(self, path: str) -> int:
        removed = super().delete_tree(path)
        # The files are gone; on Data Lake Gen2 their directories are not.
        self._delete_folders(self._with_base(path))
        return removed

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        path = self._with_base(path)
        root = {}
        if path and not path.endswith("/"):
            path = path + "/"

        for blob in self.container.list_blobs(name_starts_with=path, include=["metadata"]):
            key = blob.name
            if key.endswith("/") or self._is_folder(blob):
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

    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        path = self._with_base(path)
        blob = self.container.get_blob_client(path)
        try:
            data = blob.download_blob().readall()
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"Parquet file not found: {path}") from e
        try:
            buf = io.BytesIO(data)
            proj = None
            if columns:
                proj = self._project_columns(pq.read_schema(buf).names, columns)
                buf.seek(0)
            return pq.read_table(buf, columns=proj) if proj else pq.read_table(buf)
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
        """Copy blob via download→upload (synchronous, works with private containers)."""
        data = self.read_bytes(src_path)
        self.write_bytes(dst_path, data)