import io
import json
import fnmatch
from typing import Any, Dict, List, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from supertable.storage.storage_interface import StorageInterface


class MinioStorage(StorageInterface):
    """
    MinIO backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single object if it exists, otherwise deletes all objects under prefix
    """

    def __init__(self, bucket_name: str, client: Minio):
        self.bucket_name = bucket_name
        self.client = client

    # -------------------------
    # Helpers
    # -------------------------
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
        """
        Return immediate child names under prefix, like LocalStorage listing one directory level.
        MinIO returns full keys; we need to collapse to children at exactly one level.
        """
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        # Use non-recursive listing and extract either object keys at this level or "common prefixes"
        # MinIO's list_objects doesn't directly yield common prefixes, but non-recursive means
        # children one level down (objects and "dir marker" objects) appear distinctly.
        children = []
        seen = set()

        for obj in self._list_objects(prefix, recursive=False):
            key = obj.object_name
            if not key.startswith(prefix):
                continue
            suffix = key[len(prefix):]

            # Consider only the first path component under the prefix (one level)
            # e.g., if suffix = "a/b/c", the child at this level is "a"
            part = suffix.split("/", 1)[0]
            if part not in seen:
                seen.add(part)
                children.append(part)

        return children

    # -------------------------
    # JSON
    # -------------------------
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
        # MinIO put_object requires length
        self.client.put_object(
            self.bucket_name,
            path,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
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
        # No-op for object storage. If you want folder markers, you could:
        # if not path.endswith("/"): path += "/"
        # self.write_bytes(path, b"")
        pass

    # -------------------------
    # Listing
    # -------------------------
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Local parity:
        - treat `path` as a directory prefix (append '/' if needed)
        - return immediate children (one level)
        - apply fnmatch to the child name only
        - return full keys like LocalStorage returns full paths
        """
        if path and not path.endswith("/"):
            path = path + "/"

        children = self._child_names_one_level(path)
        # Filter by pattern against the child basename
        filtered_children = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered_children]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        """
        - If `path` is an exact object: delete it.
        - Else, treat `path` as a prefix (normalize with '/'), delete all children recursively.
        - Raise FileNotFoundError if neither object nor any child exists.
        """
        # exact object?
        if self._object_exists(path):
            self.client.remove_object(self.bucket_name, path)
            return

        # try as prefix
        prefix = path if path.endswith("/") else f"{path}/"
        to_delete = [obj.object_name for obj in self._list_objects(prefix, recursive=True)]

        if not to_delete:
            # Nothing matched as object nor prefix
            raise FileNotFoundError(f"File or folder not found: {path}")

        # Batch delete (best-effort)
        for key in to_delete:
            self.client.remove_object(self.bucket_name, key)

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        """
        Build nested dict of the subtree at `path`.
        """
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

    # -------------------------
    # Parquet
    # -------------------------
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

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
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
        # Server-side copy within the same bucket
        self.client.copy_object(
            bucket_name=self.bucket_name,
            object_name=dst_path,
            source=f"/{self.bucket_name}/{src_path}",
        )
