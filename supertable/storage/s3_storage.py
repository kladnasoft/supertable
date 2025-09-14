import io
import json
import fnmatch
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.storage.storage_interface import StorageInterface


class S3Storage(StorageInterface):
    """
    AWS S3 backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single object if it exists, otherwise deletes all objects under prefix
    """

    def __init__(self, bucket_name: str, client=None):
        self.bucket_name = bucket_name
        self.client = client or boto3.client("s3")

    # -------------------------
    # Helpers
    # -------------------------
    def _object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def _list_common_prefixes_and_objects_one_level(self, prefix: str) -> List[str]:
        """
        Return immediate child names under prefix using Delimiter="/".
        """
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
        )

        children = []
        seen = set()

        for page in page_it:
            for cp in page.get("CommonPrefixes", []):
                part = cp["Prefix"][len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == prefix:
                    continue  # folder marker
                part = key[len(prefix):]
                if "/" in part:
                    # deeper level; Delimiter should prevent this, but guard anyway
                    part = part.split("/", 1)[0]
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

        return children

    # -------------------------
    # JSON
    # -------------------------
    def read_json(self, path: str) -> Dict[str, Any]:
        try:
            resp = self.client.get_object(Bucket=self.bucket_name, Key=path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise
        data = resp["Body"].read()
        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")
        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

        # unreachable

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=payload,
            ContentType="application/json",
        )

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
    def exists(self, path: str) -> bool:
        return self._object_exists(path)

    def size(self, path: str) -> int:
        try:
            resp = self.client.head_object(Bucket=self.bucket_name, Key=path)
            return int(resp["ContentLength"])
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def makedirs(self, path: str) -> None:
        # No-op for object storage; see MinIO note if you want folder markers.
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

        children = self._list_common_prefixes_and_objects_one_level(path)
        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        # exact object?
        if self._object_exists(path):
            self.client.delete_object(Bucket=self.bucket_name, Key=path)
            return

        # prefix recursive
        prefix = path if path.endswith("/") else f"{path}/"

        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        keys = []
        for page in page_it:
            for obj in page.get("Contents", []):
                keys.append({"Key": obj["Key"]})

        if not keys:
            raise FileNotFoundError(f"File or folder not found: {path}")

        # Batch delete in chunks of 1000 (S3 limit)
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i+1000]
            self.client.delete_objects(
                Bucket=self.bucket_name,
                Delete={"Objects": chunk, "Quiet": True},
            )

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        root = {}
        if path and not path.endswith("/"):
            path = path + "/"

        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(Bucket=self.bucket_name, Prefix=path)

        for page in page_it:
            for obj in page.get("Contents", []):
                key = obj["Key"]
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
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=data,
            ContentType="application/octet-stream",
        )

    def read_parquet(self, path: str) -> pa.Table:
        try:
            resp = self.client.get_object(Bucket=self.bucket_name, Key=path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"Parquet file not found: {path}") from e
            raise
        data = resp["Body"].read()
        try:
            return pq.read_table(io.BytesIO(data))
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
    def write_bytes(self, path: str, data: bytes) -> None:
        self.client.put_object(
            Bucket=self.bucket_name, Key=path, Body=data, ContentType="application/octet-stream"
        )

    def read_bytes(self, path: str) -> bytes:
        try:
            resp = self.client.get_object(Bucket=self.bucket_name, Key=path)
            return resp["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        self.client.copy_object(
            Bucket=self.bucket_name,
            Key=dst_path,
            CopySource={"Bucket": self.bucket_name, "Key": src_path},
        )
