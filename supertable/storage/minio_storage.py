# supertable/storage/minio_storage.py

import io
import os
import re
import json
import fnmatch
from typing import Any, Dict, List, Iterable, Optional
from urllib.parse import urlparse
from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from supertable.storage.storage_interface import StorageInterface


class MinioStorage(StorageInterface):
    """
    MinIO backend with LocalStorage parity.
    Exposes endpoint/region/url_style/secure so upstream (DuckDB reader) can auto-configure.
    """

    def __init__(self, bucket_name: str, client: Minio):
        self.bucket_name = bucket_name
        self.client = client
        self.endpoint_url: Optional[str] = None
        self.region: Optional[str] = None
        self.url_style: str = "path"  # DuckDB: 'path' | 'vhost'
        self.secure: bool = False     # whether endpoint_url is https

    @staticmethod
    def _build_client(endpoint: str, access_key: str, secret_key: str, region: Optional[str]) -> Minio:
        parsed = urlparse(endpoint)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"Unsupported scheme in STORAGE_ENDPOINT_URL: {endpoint}")
        secure = parsed.scheme == "https"
        host = parsed.netloc or parsed.path  # tolerate "localhost:9000"
        return Minio(
            endpoint=host,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region or None,
        )

    @staticmethod
    def _extract_expected_region_from_error(e: S3Error) -> Optional[str]:
        msg = getattr(e, "message", "") or str(e)
        m = re.search(r"expecting ['\"]([a-z0-9-]+)['\"]", msg, re.IGNORECASE)
        return m.group(1) if m else None

    def _ensure_bucket_exists(self, bucket: str, region: Optional[str]) -> None:
        try:
            exists = self.client.bucket_exists(bucket)
        except S3Error as e:
            if getattr(e, "code", "") == "AuthorizationHeaderMalformed":
                expected = self._extract_expected_region_from_error(e)
                if expected:
                    self.client = self._rebuild_with_region(expected)
                    exists = self.client.bucket_exists(bucket)
                else:
                    raise
            else:
                raise

        if not exists:
            try:
                if region and region.lower() != "us-east-1":
                    self.client.make_bucket(bucket, location=region)
                else:
                    self.client.make_bucket(bucket)
            except S3Error as e:
                if getattr(e, "code", "") not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    if getattr(e, "code", "") == "AuthorizationHeaderMalformed":
                        expected = self._extract_expected_region_from_error(e)
                        if expected:
                            self.client = self._rebuild_with_region(expected)
                            if expected.lower() != "us-east-1":
                                self.client.make_bucket(bucket, location=expected)
                            else:
                                self.client.make_bucket(bucket)
                            return
                    raise

    def _rebuild_with_region(self, region: str) -> Minio:
        endpoint_url = os.getenv("STORAGE_ENDPOINT_URL")
        access_key = os.getenv("STORAGE_ACCESS_KEY")
        secret_key = os.getenv("STORAGE_SECRET_KEY")
        if not (endpoint_url and access_key and secret_key):
            raise RuntimeError(
                "Cannot rebuild MinIO client with region: missing STORAGE_ENDPOINT_URL / STORAGE_ACCESS_KEY / STORAGE_SECRET_KEY"
            )
        return self._build_client(endpoint_url, access_key, secret_key, region)

    @classmethod
    def from_env(cls) -> "MinioStorage":
        bucket = os.getenv("STORAGE_BUCKET") or "supertable"
        endpoint = os.getenv("STORAGE_ENDPOINT_URL")
        access_key = os.getenv("STORAGE_ACCESS_KEY")
        secret_key = os.getenv("STORAGE_SECRET_KEY")
        region = (
            os.getenv("STORAGE_REGION")
            or None
        )
        if not endpoint or not access_key or not secret_key:
            raise RuntimeError(
                "MinIO environment incomplete. "
                "Expected STORAGE_ENDPOINT_URL, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY."
            )

        client = cls._build_client(endpoint, access_key, secret_key, region)
        storage = cls(bucket_name=bucket, client=client)

        parsed = urlparse(endpoint)
        storage.endpoint_url = endpoint
        storage.region = region
        storage.secure = parsed.scheme == "https"
        force_path_style = (os.getenv("STORAGE_FORCE_PATH_STYLE", "") or "").lower() in ("1", "true", "yes", "on")
        storage.url_style = "path" if force_path_style else "path"

        storage._ensure_bucket_exists(bucket, region)
        return storage

    # ---------- helpers for DuckDB & presign ----------

    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        prefer_httpfs = (
            prefer_httpfs if prefer_httpfs is not None
            else (os.getenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "") or "").lower() in ("1", "true", "yes", "on")
        )
        key = (key or "").lstrip("/")
        if prefer_httpfs:
            parsed = urlparse(self.endpoint_url or "")
            host = parsed.netloc or parsed.path or ""
            scheme = parsed.scheme or ("https" if self.secure else "http")
            return f"{scheme}://{host}/{self.bucket_name}/{key}"
        return f"s3://{self.bucket_name}/{key}"

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        """Return a presigned GET URL: http(s)://host/bucket/key?...X-Amz-..."""
        return self.client.presigned_get_object(
            self.bucket_name,
            key.lstrip("/"),
            expires=timedelta(seconds=expiry_seconds),
        )

    # ---------- low-level helpers ----------

    def _object_exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket_name, key)
            return True
        except S3Error as e:
            if e.code in ("NoSuchKey", "NotFound"):
                return False
            raise

    def _list_objects(self, prefix: str, recursive: bool) -> Iterable:
        return self.client.list_objects(self.bucket_name, prefix=prefix, recursive=recursive)

    def _child_names_one_level(self, prefix: str) -> List[str]:
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        children, seen = [], set()
        for obj in self._list_objects(prefix, recursive=False):
            key = obj.object_name
            if not key.startswith(prefix):
                continue
            suffix = key[len(prefix):]
            part = suffix.split("/", 1)[0]
            if part not in seen:
                seen.add(part)
                children.append(part)
        return children

    # ---------- JSON ----------

    def read_json(self, path: str) -> Dict[str, Any]:
        try:
            resp = self.client.get_object(self.bucket_name, path)
            data = resp.read()
            resp.close()
            resp.release_conn()
        except S3Error as e:
            if e.code in ("NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.client.put_object(
            self.bucket_name,
            path,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )

    # ---------- existence / size / dirs ----------

    def exists(self, path: str) -> bool:
        return self._object_exists(path)

    def size(self, path: str) -> int:
        try:
            stat = self.client.stat_object(self.bucket_name, path)
            return stat.size
        except S3Error as e:
            if e.code in ("NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def makedirs(self, path: str) -> None:
        # Object storage is flat; no-op
        pass

    # ---------- listing ----------

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        if path and not path.endswith("/"):
            path = path + "/"
        children = self._child_names_one_level(path)
        filtered_children = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered_children]

    # ---------- delete ----------

    def delete(self, path: str) -> None:
        if self._object_exists(path):
            self.client.remove_object(self.bucket_name, path)
            return
        prefix = path if path.endswith("/") else f"{path}/"
        to_delete = [obj.object_name for obj in self._list_objects(prefix, recursive=True)]
        if not to_delete:
            raise FileNotFoundError(f"File or folder not found: {path}")
        for key in to_delete:
            self.client.remove_object(self.bucket_name, key)

    # ---------- directory structure ----------

    def get_directory_structure(self, path: str) -> dict:
        root = {}
        if path and not path.endswith("/"):
            path = path + "/"
        for obj in self._list_objects(path, recursive=True):
            key = obj.object_name
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

    # ---------- parquet ----------

    def write_parquet(self, table: pa.Table, path: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        self.client.put_object(
            self.bucket_name,
            path,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )

    def read_parquet(self, path: str) -> pa.Table:
        try:
            resp = self.client.get_object(self.bucket_name, path)
            data = resp.read()
            resp.close()
            resp.release_conn()
        except S3Error as e:
            if e.code in ("NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"Parquet file not found: {path}") from e
            raise
        try:
            return pq.read_table(io.BytesIO(data))
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # ---------- bytes / text / copy ----------

    def write_bytes(self, path: str, data: bytes) -> None:
        self.client.put_object(
            self.bucket_name,
            path,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )

    def read_bytes(self, path: str) -> bytes:
        try:
            resp = self.client.get_object(self.bucket_name, path)
            data = resp.read()
            resp.close()
            resp.release_conn()
            return data
        except S3Error as e:
            if e.code in ("NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        self.client.copy_object(
            bucket_name=self.bucket_name,
            object_name=dst_path,
            source=f"/{self.bucket_name}/{src_path}",
        )
