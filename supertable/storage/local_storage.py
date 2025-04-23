import json
import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq
import shutil

from typing import Any, Dict, List

from supertable.config.homedir import app_home
from supertable.storage.storage_interface import StorageInterface

class LocalStorage(StorageInterface):
    """
    A local disk-based implementation of StorageInterface.
    """

    def read_json(self, path: str) -> Dict[str, Any]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        if os.path.getsize(path) == 0:
            raise ValueError(f"File is empty: {path}")

        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {path}") from e

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

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