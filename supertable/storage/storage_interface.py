# route: supertable.storage.storage_interface
import abc
from typing import Any, Dict, Iterator, List, Optional
import pyarrow as pa

class StorageInterface(abc.ABC):
    """
    Abstract base class for a storage interface that can handle both local and
    cloud/object storage in a unified manner.
    """

    base_prefix: str = ""

    def _with_base(self, path: str) -> str:
        """Prepend base_prefix to path if set. No-op when base_prefix is empty."""
        path = path.strip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{path}" if path else self.base_prefix
        return path


    @abc.abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Reads and returns JSON data from the given path.
        Raises FileNotFoundError, ValueError, etc. on error.
        """
        pass

    @abc.abstractmethod
    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Writes JSON data to the given path.
        Overwrites if it already exists.
        """
        pass

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """
        Returns True if the given path exists, False otherwise.
        For cloud storage, 'path' might be a prefix / object key.
        """
        pass

    @abc.abstractmethod
    def size(self, path: str) -> int:
        """
        Returns the size (in bytes) of the object at the given path.
        Raises FileNotFoundError if not found.
        """
        pass

    @abc.abstractmethod
    def makedirs(self, path: str) -> None:
        """
        Creates directories (or the equivalent in cloud storage) if needed.
        No-op if already present.
        """
        pass

    @abc.abstractmethod
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Returns a list of files/objects found in `path` matching the given pattern.
        This may be limited to a single "directory" level, depending on implementation.
        For recursive scans, either extend with a `recursive=True` parameter or
        provide an additional method.
        """
        pass

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        """
        Deletes a file/object at the given path.
        Raises FileNotFoundError if the path does not exist.
        """
        pass

    # -------------------------
    # Recursive deletion (prefix-listed, not existence-guarded)
    # -------------------------
    def delete_tree(self, path: str) -> int:
        """Delete *path* and every object beneath it.

        Object storage has no directories.  A "folder" is just a set of keys
        sharing a prefix, and ``exists(folder)`` is a HEAD on a key that was
        never created, so it is ``False`` on S3/MinIO/Azure/GCS however much
        data sits underneath.  Guarding a wipe with ``if exists(folder)``
        therefore skips the wipe everywhere except a local filesystem, where
        the guard happens to be ``os.path.isdir`` — the shape of AUDIT_BUGS
        C2.  This lists the prefix instead of stat-ing it, and deletes what
        the listing returns.

        Enumeration goes through :meth:`get_directory_structure` rather than
        :meth:`list_files` for two reasons: it is the only interface method
        that is recursive on every backend (``list_files`` returns a single
        delimited level), and it reports paths *relative* to ``path``, so the
        keys can be rejoined with the caller's logical path without
        re-applying ``base_prefix`` — which ``list_files`` returns baked into
        its result (AUDIT_BUGS M4).  It is also a single listing pass rather
        than one per directory level.

        Each key is removed with an exact-key :meth:`delete`.  The backends'
        prefix-recursive ``delete()`` convenience is deliberately not used:
        the four disagree about an already-empty prefix (S3 returns silently,
        MinIO/Azure/GCS raise ``FileNotFoundError``), and depending on it
        would make this method's contract backend-specific.

        Returns:
            The number of objects removed.  ``0`` means the prefix listed
            nothing, which is the only condition under which an absent folder
            may be read as success.  Anything that fails to delete raises, so
            a caller that reaches the next statement knows the data is gone
            and may drop its catalog pointer to it.
        """
        removed = 0
        for key in self._iter_tree_keys(path):
            if self._delete_if_present(key):
                removed += 1

        # ``path`` itself: an exact object when a key and a prefix collide on
        # a flat namespace, or — on a local filesystem — the directory tree
        # that is now empty.  Either way it must go.
        if self.exists(path) and self._delete_if_present(path) and removed == 0:
            removed = 1

        return removed

    def _iter_tree_keys(self, path: str) -> Iterator[str]:
        """Yield every object key beneath *path*, as logical (un-prefixed) paths."""
        base = path.rstrip("/")
        stack = [(base, self.get_directory_structure(path))]
        while stack:
            parent, node = stack.pop()
            if not isinstance(node, dict):
                continue
            for name, child in node.items():
                full = f"{parent}/{name}" if parent else name
                if isinstance(child, dict):
                    stack.append((full, child))
                else:
                    yield full

    def _delete_if_present(self, path: str) -> bool:
        """``delete(path)``, tolerating only "it was already gone"."""
        try:
            self.delete(path)
            return True
        except FileNotFoundError:
            # Lost a race with another deleter; the post-condition still holds.
            return False

    @abc.abstractmethod
    def get_directory_structure(self, path: str) -> dict:
        """
        Recursively builds and returns a nested dictionary representing
        the folder structure under 'path'. For local storage, uses os.walk.
        For S3/MinIO, lists objects by prefix. The dictionary format might look like:
            {
                "subfolderA": {
                    "file1.parquet": None,
                    "file2.json": None
                },
                "subfolderB": {
                    "nested": {
                        "file3.parquet": None
                    }
                }
            }
        """
        pass

    @abc.abstractmethod
    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to the given path (local or cloud/object storage) in Parquet format.
        For local, use pyarrow.parquet.write_table.
        For cloud, you may rely on s3fs or a custom upload method.
        """
        pass

    @abc.abstractmethod
    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        """
        Reads and returns a PyArrow Table from the given Parquet path.

        When *columns* is given, only those columns are read (projection is
        pushed down to the parquet reader so other column chunks are skipped);
        ``None`` reads every column.  Raises FileNotFoundError, ValueError, etc.
        on error.
        """
        pass

    @staticmethod
    def _project_columns(available, columns: Optional[List[str]]) -> Optional[List[str]]:
        """Intersect requested *columns* with those *available* in the file.

        Parquet files within one table can carry heterogeneous schemas, so a
        requested column a given file lacks is silently dropped rather than
        raising a binder error.  Returns the projection list to hand the parquet
        reader, or ``None`` to read every column (nothing requested, or none of
        the requested columns exist in this file).
        """
        if not columns:
            return None
        present = [c for c in columns if c in set(available)]
        return present or None

    # -------------------------
    # Byte / text / copy operations
    # -------------------------
    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes) -> None:
        """
        Writes raw bytes to the given path.
        Creates parent directories/prefixes as needed.
        """
        pass

    @abc.abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """
        Reads and returns raw bytes from the given path.
        Raises FileNotFoundError if the path does not exist.
        """
        pass

    @abc.abstractmethod
    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        """
        Writes a string to the given path using the specified encoding.
        """
        pass

    @abc.abstractmethod
    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """
        Reads and returns text from the given path using the specified encoding.
        Raises FileNotFoundError if the path does not exist.
        """
        pass

    @abc.abstractmethod
    def copy(self, src_path: str, dst_path: str) -> None:
        """
        Copies an object from src_path to dst_path within the same storage backend.
        """
        pass

    # -------------------------
    # Optional parity helpers (object stores)
    # -------------------------
    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        """
        Return a path usable by DuckDB readers.

        Implementations may return either:
        - s3://bucket/key
        - http(s)://... URLs (when prefer_httpfs=True)
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement to_duckdb_path()")

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        """
        Return a presigned GET URL for the object.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement presign()")