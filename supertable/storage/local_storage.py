# route: supertable.storage.local_storage
import json
import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import tempfile
import time
import hashlib

from typing import Any, BinaryIO, Dict, List, Optional

from supertable.config.homedir import app_home
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    StorageInterface,
    validate_range_request,
    write_all,
)

class LocalStorage(StorageInterface):
    """
    A local disk-based implementation of StorageInterface.
    """

    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Robust JSON reader:
          - fast path: read once
          - if file is empty or decoding fails, retry briefly (handles concurrent atomic replace)
        """
        # quick existence check
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")

        # micro-retry window for transient writer activity
        attempts = 5
        backoff = 0.02  # 20 ms

        for attempt in range(1, attempts + 1):
            try:
                # if a writer is mid-replace and we catch a brand new file entry that is still size 0,
                # back off and retry once more
                try:
                    if os.path.getsize(path) == 0:
                        if attempt == attempts:
                            raise ValueError(f"File is empty: {path}")
                        time.sleep(backoff)
                        continue
                except FileNotFoundError:
                    # vanished between exists() and getsize(); retry
                    if attempt == attempts:
                        raise FileNotFoundError(f"File not found: {path}")
                    time.sleep(backoff)
                    continue

                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)

            except json.JSONDecodeError as e:
                # reader may have raced with a writer that just replaced the file;
                # give it a tiny moment to settle, then retry
                if attempt == attempts:
                    raise ValueError(f"Invalid JSON in {path}") from e
                time.sleep(backoff)
            except FileNotFoundError:
                # replaced again during open—retry
                if attempt == attempts:
                    raise
                time.sleep(backoff)

        # Should never get here
        raise RuntimeError(f"Unexpected failure reading JSON at {path}")

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Atomic JSON write:
          - write to a temp file in the same directory
          - fsync file
          - os.replace() to atomically swap into place
          - fsync directory entry
        """
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)

        # write to a temp file in the same directory to ensure atomic rename on the same filesystem
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-json-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
                json.dump(data, tmpf, indent=2, ensure_ascii=False)
                tmpf.flush()
                os.fsync(tmpf.fileno())

            # atomic replace
            os.replace(tmp_path, path)

            # fsync the directory to persist the rename on POSIX
            try:
                dir_fd = os.open(directory, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                # best-effort; not all platforms allow this
                pass
        finally:
            # if something failed before replace(), make sure temp is gone
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def size(self, path: str) -> int:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        return os.path.getsize(path)

    @staticmethod
    def _metadata_from_stat(stat_result: os.stat_result) -> ObjectMetadata:
        return ObjectMetadata(
            size=int(stat_result.st_size),
            # ctime fences same-inode, same-size rewrites even on filesystems
            # whose mtime granularity is too coarse for back-to-back writes.
            version=(
                f"{stat_result.st_dev}:{stat_result.st_ino}:"
                f"{stat_result.st_ctime_ns}"
            ),
            last_modified_ns=int(stat_result.st_mtime_ns),
        )

    @classmethod
    def _metadata_from_open_file(cls, source: BinaryIO) -> ObjectMetadata:
        metadata = cls._metadata_from_stat(os.fstat(source.fileno()))
        # Local files can be rewritten in-place with the same size inside one
        # filesystem timestamp tick. A bounded head/tail seal catches common
        # overwrite/replace patterns without reading a multi-GiB local object.
        sample_bytes = 64 * 1024
        if hasattr(os, "pread"):
            head = os.pread(source.fileno(), min(sample_bytes, metadata.size), 0)
            tail = (
                os.pread(
                    source.fileno(),
                    sample_bytes,
                    max(0, metadata.size - sample_bytes),
                )
                if metadata.size > sample_bytes
                else b""
            )
        else:  # pragma: no cover - POSIX production path uses pread
            position = source.tell()
            source.seek(0)
            head = source.read(min(sample_bytes, metadata.size))
            tail = b""
            if metadata.size > sample_bytes:
                source.seek(max(0, metadata.size - sample_bytes))
                tail = source.read(sample_bytes)
            source.seek(position)
        sample = hashlib.sha256(head + tail).hexdigest()
        return ObjectMetadata(
            size=metadata.size,
            version=f"{metadata.version}:{sample}",
            last_modified_ns=metadata.last_modified_ns,
        )

    def stat_object(self, path: str) -> ObjectMetadata:
        try:
            source = open(path, "rb")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e
        with source:
            return self._metadata_from_open_file(source)

    def download_to_file(
        self,
        path: str,
        file_obj: BinaryIO,
        *,
        expected: ObjectMetadata | None = None,
        chunk_size: int = 8 * 1024 * 1024,
    ) -> int:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        try:
            source = open(path, "rb")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e

        with source:
            before = self._metadata_from_open_file(source)
            if expected is not None and before != expected:
                raise OSError(f"Object changed before download: {path}")
            written = 0
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                written += write_all(file_obj, chunk)
            after = self._metadata_from_open_file(source)
            if after != before:
                raise OSError(f"Object changed during download: {path}")
            if written != before.size:
                raise OSError(
                    f"Short download for {path}: expected {before.size} bytes, wrote {written}"
                )
            return written

    def read_range(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> bytes:
        offset, length = validate_range_request(offset, length, expected)
        try:
            source = open(path, "rb")
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"File not found: {path}") from exc
        with source:
            before = self._metadata_from_open_file(source)
            if expected is not None and before.identity_token() != expected.identity_token():
                raise ObjectIdentityMismatch(f"Object changed before range read: {path}")
            if offset > before.size or length > before.size - offset:
                raise ObjectIdentityMismatch(f"Object shrank before range read: {path}")
            if length == 0:
                return b""
            chunks = []
            remaining = length
            position = offset
            while remaining:
                if hasattr(os, "pread"):
                    chunk = os.pread(source.fileno(), remaining, position)
                else:  # pragma: no cover - POSIX production path uses pread
                    source.seek(position)
                    chunk = source.read(remaining)
                if not chunk:
                    raise ObjectIdentityMismatch(f"Short range read: {path}")
                chunks.append(chunk)
                position += len(chunk)
                remaining -= len(chunk)
            after = self._metadata_from_open_file(source)
            if after.identity_token() != before.identity_token():
                raise ObjectIdentityMismatch(f"Object changed during range read: {path}")
            return b"".join(chunks)

    def cache_namespace(self) -> Dict[str, str]:
        return {"provider": "local"}

    def is_local_storage(self) -> bool:
        return True

    def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists files in 'path' matching the given pattern (non-recursive).
        """
        if not os.path.isdir(path):
            return []
        return sorted(glob.glob(os.path.join(path, pattern)))

    def delete(self, path: str) -> None:
        """
        Deletes a file or a folder from local disk.

        For files and symlinks, os.remove() is used.
        For directories, shutil.rmtree() is used to remove the directory and its contents.
        """
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            raise FileNotFoundError(f"File or folder not found: {path}")

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
        """
        directory_structure = {}
        if not os.path.isdir(path):
            return directory_structure

        for root, dirs, files in os.walk(path):
            folder = os.path.relpath(root, path)
            if folder == ".":
                folders = []
            else:
                folders = folder.split(os.sep)

            subdir = dict.fromkeys(files)
            parent = directory_structure
            for sub in folders:
                parent = parent.setdefault(sub, {})

            if subdir:
                parent.update(subdir)

        return directory_structure

    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to a local Parquet file at 'path'.
        """
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        pq.write_table(table, path)

    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Parquet file not found at: {path}")

        try:
            proj = self._project_columns(pq.read_schema(path).names, columns) if columns else None
            # partitioning=None: read only the file's own footer columns; never let
            # pyarrow infer Hive year/month/day from a ``year=YYYY/...`` path.  The
            # object-store backends read from a BytesIO buffer (no path) and so never
            # infer -- this keeps LocalStorage consistent with them and upholds the
            # "partition columns are path-only, never in the body" contract.  Without
            # it a full read injects int32 year/month/day that compaction bakes into
            # the rewritten body, leaking them into query output and breaking later
            # reads with an int32-vs-dictionary merge error.
            return (
                pq.read_table(path, columns=proj, partitioning=None) if proj
                else pq.read_table(path, partitioning=None)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet file at '{path}': {e}")

    def write_bytes(self, path: str, data: bytes) -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(path, "rb") as f:
            return f.read()

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding=encoding) as f:
            f.write(text)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(path, "r", encoding=encoding) as f:
            return f.read()

    def copy(self, src_path: str, dst_path: str) -> None:
        directory = os.path.dirname(dst_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        shutil.copyfile(src_path, dst_path)
