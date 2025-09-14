import json
import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import tempfile
import time

from typing import Any, Dict, List

from supertable.config.homedir import app_home
from supertable.storage.storage_interface import StorageInterface

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

    def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists files in 'path' matching the given pattern (non-recursive).
        """
        if not os.path.isdir(path):
            return []
        return glob.glob(os.path.join(path, pattern))

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

    def read_parquet(self, path: str) -> pa.Table:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Parquet file not found at: {path}")

        try:
            table = pq.read_table(path)
            return table
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
