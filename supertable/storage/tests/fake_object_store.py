"""An in-memory :class:`StorageInterface` that behaves like a real bucket.

``MagicMock()`` is the wrong test double for storage.  Every attribute access
on it returns a fresh, **truthy** ``Mock``, so ``storage.exists(folder)`` is
``True`` for any argument and ``storage.delete(...)`` always "succeeds".  A
test written against it cannot tell a bucket full of data from an empty one,
which is why AUDIT_BUGS C2 (drop-table orphans every object) and M4 survived a
~2,700 test suite untouched.

``FakeObjectStore`` models the properties that actually distinguish object
storage from a filesystem:

* **There are no directories, only keys.**  ``a/b/c.parquet`` is one object;
  ``a/b`` is not an object and never becomes one.
* :meth:`exists` is ``HEAD <exact key>``.  ``exists("a/b")`` is therefore
  ``False`` even while ``a/b/c.parquet`` is present — the trap behind C2.
* :meth:`delete` is ``DeleteObject <exact key>``.  Handed a prefix it deletes
  **nothing** and raises ``FileNotFoundError``.
* :meth:`list_files` is a delimited ``LIST``: one level of children under the
  prefix, returned prefix-inclusive, mixing object keys and common prefixes
  exactly the way the real backends do.
* :meth:`get_directory_structure` is an undelimited ``LIST``: every key under
  the prefix, as a nested dict whose leaves (files) are ``None``.
* ``base_prefix`` is applied on the way in and is **not** stripped on the way
  out of :meth:`list_files`, reproducing the asymmetry recorded as M4.

Deliberately stricter than the shipped backends in one respect: the real
``S3Storage``/``MinioStorage``/``AzureStorage``/``GCPStorage`` ``delete()``
implementations fall back to a *recursive prefix* delete when the exact key is
absent, and the four disagree about what that means when the prefix is also
empty (S3 returns silently; the other three raise ``FileNotFoundError``).
Code proven correct against this double therefore cannot be resting on a
convenience whose semantics vary per backend.
"""

from __future__ import annotations

import fnmatch
import io
import json
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from supertable.storage.storage_interface import StorageInterface


class FakeObjectStore(StorageInterface):
    """Flat key/value store with object-store semantics.

    Args:
        base_prefix: bucket-relative prefix prepended to every path, exactly
            as the real backends apply ``SUPERTABLE_PREFIX``.  Left empty by
            default; set it to exercise the M4 asymmetry.
        objects: optional seed mapping of *logical* path -> bytes.
    """

    def __init__(
        self,
        base_prefix: str = "",
        objects: Optional[Dict[str, bytes]] = None,
    ):
        self.base_prefix = base_prefix.strip("/")
        # Keyed by the FULL key (base_prefix included), like the real bucket.
        self._objects: Dict[str, bytes] = {}
        # Test-visible bookkeeping.
        self.deleted_keys: List[str] = []
        self.makedirs_calls: List[str] = []
        if objects:
            for path, payload in objects.items():
                self._objects[self._with_base(path)] = payload

    # ------------------------------------------------------------------ test helpers

    def seed(self, *paths: str, payload: bytes = b"x") -> "FakeObjectStore":
        """Create an object at each *path*.  Returns self for chaining."""
        for path in paths:
            self._objects[self._with_base(path)] = payload
        return self

    def keys(self) -> List[str]:
        """Every key currently in the bucket, full (base_prefix included)."""
        return sorted(self._objects)

    def keys_under(self, path: str) -> List[str]:
        """Every key under *path*, full.  ``[]`` means the prefix is empty."""
        prefix = self._dir_prefix(self._with_base(path))
        return sorted(k for k in self._objects if k.startswith(prefix))

    # ------------------------------------------------------------------ internals

    @staticmethod
    def _dir_prefix(key: str) -> str:
        if not key:
            return ""
        return key if key.endswith("/") else key + "/"

    def _require(self, path: str) -> bytes:
        key = self._with_base(path)
        try:
            return self._objects[key]
        except KeyError:
            raise FileNotFoundError(f"File not found: {key}") from None

    # ------------------------------------------------------------------ json

    def read_json(self, path: str) -> Dict[str, Any]:
        raw = self._require(path)
        if not raw:
            raise ValueError(f"File is empty: {self._with_base(path)}")
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {self._with_base(path)}") from e

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        self._objects[self._with_base(path)] = json.dumps(data).encode("utf-8")

    # ------------------------------------------------------------------ existence

    def exists(self, path: str) -> bool:
        """HEAD on an exact key.  A prefix is never an object -> ``False``."""
        return self._with_base(path) in self._objects

    def size(self, path: str) -> int:
        return len(self._require(path))

    def makedirs(self, path: str) -> None:
        """No-op: object storage has no directories to create."""
        self.makedirs_calls.append(path)

    # ------------------------------------------------------------------ listing

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """One delimited level under *path*, prefix-inclusive.

        Returns object keys AND common prefixes ("directories") at this level,
        indistinguishable from one another — as on S3.  The returned strings
        carry ``base_prefix`` (M4).
        """
        prefix = self._dir_prefix(self._with_base(path))
        children: List[str] = []
        seen = set()
        for key in self._objects:
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix):]
            if not rest:
                continue
            child = rest.split("/", 1)[0]
            if child and child not in seen:
                seen.add(child)
                children.append(child)
        children.sort()
        return [prefix + c for c in children if fnmatch.fnmatch(c, pattern)]

    def get_directory_structure(self, path: str) -> dict:
        """Undelimited LIST under *path* as a nested dict; files map to ``None``."""
        prefix = self._dir_prefix(self._with_base(path))
        root: Dict[str, Any] = {}
        for key in sorted(self._objects):
            if not key.startswith(prefix):
                continue
            parts = [p for p in key[len(prefix):].split("/") if p]
            if not parts:
                continue
            cursor = root
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    cursor[part] = None
                else:
                    cursor = cursor.setdefault(part, {})
        return root

    # ------------------------------------------------------------------ delete

    def delete(self, path: str) -> None:
        """DeleteObject on an exact key.

        A prefix is not an object, so this removes nothing and raises — the
        behaviour that makes an ``exists()``-guarded folder wipe a no-op.
        """
        key = self._with_base(path)
        if key not in self._objects:
            raise FileNotFoundError(f"File or folder not found: {key}")
        del self._objects[key]
        self.deleted_keys.append(key)

    # ------------------------------------------------------------------ bytes / text

    def write_bytes(self, path: str, data: bytes) -> None:
        self._objects[self._with_base(path)] = bytes(data)

    def read_bytes(self, path: str) -> bytes:
        return self._require(path)

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self._objects[self._with_base(path)] = text.encode(encoding)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self._require(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        self._objects[self._with_base(dst_path)] = self._require(src_path)

    # ------------------------------------------------------------------ parquet

    def write_parquet(self, table: pa.Table, path: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        self._objects[self._with_base(path)] = buf.getvalue()

    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        buf = io.BytesIO(self._require(path))
        proj = self._project_columns(pq.read_schema(buf).names, columns) if columns else None
        buf.seek(0)
        return pq.read_table(buf, columns=proj) if proj else pq.read_table(buf)
