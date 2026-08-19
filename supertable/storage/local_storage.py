# route: supertable.storage.local_storage
import json
import os
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import stat
import tempfile
import threading
import time
import hashlib
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from typing import Any, BinaryIO, Dict, List, Optional, Sequence

from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    StorageInterface,
    validate_range_request,
    write_all,
)


@dataclass(frozen=True)
class LocalWriteIdentity:
    """Complete identity of bytes durably published by ``LocalStorage``."""

    canonical_path: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int


class _DirectoryHandle:
    """An open directory identity that prevents inode reuse while cached."""

    __slots__ = ("path", "fd", "device", "inode", "_closed")

    def __init__(self, path: str, fd: int, stat_result: os.stat_result) -> None:
        self.path = os.path.abspath(path)
        self.fd = fd
        self.device = int(stat_result.st_dev)
        self.inode = int(stat_result.st_ino)
        self._closed = False

    @property
    def identity(self) -> tuple[int, int]:
        return self.device, self.inode

    def matches_path(self) -> bool:
        try:
            current = os.stat(self.path)
        except (FileNotFoundError, NotADirectoryError, OSError):
            return False
        return (
            stat.S_ISDIR(current.st_mode)
            and (int(current.st_dev), int(current.st_ino)) == self.identity
        )

    def close(self, _close_fd=os.close) -> None:
        # Bind os.close at definition time: module globals may already be set
        # to None when Python finalizes a process-global LocalStorage instance.
        if getattr(self, "_closed", True):
            return
        self._closed = True
        fd = getattr(self, "fd", None)
        if fd is None:
            return
        try:
            _close_fd(fd)
        except (OSError, TypeError):
            pass

    def __del__(self) -> None:  # pragma: no cover - explicit paths close eagerly
        try:
            self.close()
        except BaseException:
            # Destructors must remain silent during partial interpreter
            # teardown, including if the object was only partly initialized.
            pass


class LocalStorage(StorageInterface):
    """
    A local disk-based implementation of StorageInterface.
    """

    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        # A directly constructed LocalStorage retains its long-standing
        # caller-CWD namespace.  Production construction through
        # storage_factory passes the explicitly initialised application home,
        # so package import never needs to mutate the process CWD.  Absolute
        # caller paths remain supported for compatibility.
        if root is None:
            root = os.getcwd()
        self.root = os.path.realpath(os.path.abspath(os.fspath(root)))
        # Capture a directory that already existed when this storage namespace
        # was opened. If the configured root itself does not exist yet, the
        # first successful logical publication must also fsync every newly
        # created ancestor that links it to this durable anchor.
        self._logical_durability_anchor = self._nearest_existing_directory(
            self.root,
        )
        # Logical writes commonly publish many objects below the same stable
        # table hierarchy.  Once a complete directory chain has been fsynced,
        # only the destination directory needs another fsync for the next
        # atomic replace.  Open handles make the cached identities resistant
        # to inode-number reuse after delete/recreate, while the lock makes a
        # first publication and cache installation one per-instance action.
        self._durability_lock = threading.RLock()
        self._durable_directories: OrderedDict[str, _DirectoryHandle] = OrderedDict()
        self._durable_directory_limit = 256
        self._trusted_durability_anchors = self._open_existing_ancestor_chain(
            self._logical_durability_anchor,
        )

    def _resolve_path(self, path: str | os.PathLike[str]) -> str:
        raw = os.fspath(path)
        if os.path.isabs(raw):
            return os.path.normpath(raw)
        physical = os.path.realpath(os.path.join(self.root, raw))
        try:
            contained = os.path.commonpath((self.root, physical)) == self.root
        except ValueError:
            contained = False
        if not contained:
            raise ValueError(f"Local storage path escapes configured root: {raw!r}")
        return physical

    def canonical_uri(self, path: str) -> str:
        """Return a canonical absolute ``file:`` URI for a logical path."""
        return Path(self._resolve_path(path)).as_uri()

    def to_duckdb_path(
        self,
        key: str,
        prefer_httpfs: Optional[bool] = None,
    ) -> str:
        """Resolve a logical key to the absolute local path DuckDB opens.

        ``Path.as_uri()`` correctly escapes URI characters, including the
        equals signs in Hive-style partition directories. DuckDB's Parquet
        reader treats those escapes literally for local ``file:`` URLs on some
        versions, however, so its backend boundary uses the absolute path.
        Catalogs and storage methods continue to expose/accept logical keys.
        """
        del prefer_httpfs
        return self._resolve_path(key)

    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Robust JSON reader:
          - fast path: read once
          - if file is empty or decoding fails, retry briefly (handles concurrent atomic replace)
        """
        path = self._resolve_path(path)
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
        logical_input = not os.path.isabs(os.fspath(path))
        path = self._resolve_path(path)
        directory = os.path.dirname(path) or "."
        # Remember the closest durable ancestor before creating any missing
        # components. Fsyncing only the deepest new directory persists the file
        # rename inside it, but not the entries that link that hierarchy back to
        # the already-durable storage tree.
        durability_anchor = (
            self._logical_durability_anchor
            if logical_input
            else self._nearest_existing_directory(directory)
        )
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

            # Persist the rename before acknowledging success. This helper uses
            # O_DIRECTORY when available and propagates I/O failures; swallowing
            # them can publish a Redis pointer to a rename lost on host crash.
            self._fsync_published_directory(
                directory,
                logical_input=logical_input,
                stop_directory=durability_anchor,
            )
        finally:
            # if something failed before replace(), make sure temp is gone
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def exists(self, path: str) -> bool:
        return os.path.exists(self._resolve_path(path))

    def size(self, path: str) -> int:
        path = self._resolve_path(path)
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
        path = self._resolve_path(path)
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
        path = self._resolve_path(path)
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
        path = self._resolve_path(path)
        offset, length = validate_range_request(offset, length, expected)
        try:
            source = open(path, "rb")
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"File not found: {path}") from exc
        with source:
            before = self._metadata_from_open_file(source)
            if (
                expected is not None
                and before.identity_token() != expected.identity_token()
            ):
                raise ObjectIdentityMismatch(
                    f"Object changed before range read: {path}"
                )
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
                raise ObjectIdentityMismatch(
                    f"Object changed during range read: {path}"
                )
            return b"".join(chunks)

    def cache_namespace(self) -> Dict[str, str]:
        return {"provider": "local"}

    def is_local_storage(self) -> bool:
        return True

    def makedirs(self, path: str) -> None:
        os.makedirs(self._resolve_path(path), exist_ok=True)

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists files in 'path' matching the given pattern (non-recursive).
        """
        logical_input = not os.path.isabs(path)
        physical_path = self._resolve_path(path)
        if not os.path.isdir(physical_path):
            return []
        # Match only immediate child basenames, like the object-store
        # adapters. Passing ``pattern`` to glob would let ``../*`` or an
        # absolute pattern enumerate paths outside the configured namespace.
        matches = sorted(
            os.path.join(physical_path, name)
            for name in os.listdir(physical_path)
            if fnmatch.fnmatch(name, pattern)
        )
        if logical_input:
            return [os.path.relpath(match, self.root) for match in matches]
        return matches

    def delete(self, path: str) -> None:
        """
        Deletes a file or a folder from local disk.

        For files and symlinks, os.remove() is used.
        For directories, shutil.rmtree() is used to remove the directory and its contents.
        """
        raw_path = os.fspath(path)
        logical_input = not os.path.isabs(raw_path)
        if not logical_input:
            normalized_absolute = os.path.abspath(os.path.normpath(raw_path))
            if os.path.dirname(normalized_absolute) == normalized_absolute:
                raise ValueError("Refusing to delete a filesystem root")
        self._reject_delete_dot_segments(raw_path)
        if logical_input:
            # Resolve and contain the parent, but preserve the final directory
            # entry.  Realpathing the leaf would turn delete("link") into a
            # deletion of its referent (including recursive directory loss).
            normalized = os.path.normpath(raw_path)
            parent, leaf = os.path.split(normalized)
            parent_path = self._resolve_path(parent or ".")
            path = os.path.normpath(os.path.join(parent_path, leaf))
            try:
                contained = os.path.commonpath((self.root, path)) == self.root
            except ValueError:
                contained = False
            if not contained:
                raise ValueError(
                    f"Local storage delete path escapes configured root: {raw_path!r}"
                )
        else:
            path = os.path.normpath(raw_path)
        absolute_path = os.path.abspath(path)
        if os.path.dirname(absolute_path) == absolute_path:
            raise ValueError("Refusing to delete a filesystem root")
        if absolute_path == self.root:
            raise ValueError("Refusing to delete the configured storage root")
        # Close and discard cached descendants before recursive removal. This
        # is required on platforms where an open directory blocks deletion and
        # also prevents a later recreation from inheriting stale cache state.
        self._invalidate_durable_prefix(absolute_path)
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            # A retry after an acknowledged-visible but unsynced deletion can
            # find the target already absent. Re-sync its surviving parent so
            # callers such as delete_prefix can safely finish the tombstone.
            self._fsync_deleted_parent(path, logical_input=logical_input)
            raise FileNotFoundError(f"File or folder not found: {path}")
        self._fsync_deleted_parent(path, logical_input=logical_input)

    @staticmethod
    def _reject_delete_dot_segments(path: str) -> None:
        """Reject destructive aliases before resolving or removing anything.

        The final directory entry must remain unresolved so deleting an in-root
        symlink unlinks the symlink rather than its target.  That makes lexical
        validation important: normalising a final ``..`` first would turn it
        into authority over the configured root's parent.  Treat both slash
        spellings as separators so a path cannot become traversal merely by
        moving the same storage configuration between POSIX and Windows.
        """

        if not isinstance(path, str):
            raise ValueError("Local storage delete path must be a string")
        portable = path.replace("\\", "/")
        components = portable.split("/")
        significant = [component for component in components if component]
        if ".." in significant or (significant and significant[-1] == "."):
            raise ValueError(
                "Refusing to delete a path containing traversal or a final dot segment"
            )

    def _fsync_deleted_parent(
        self,
        deleted_path: str,
        *,
        logical_input: bool,
    ) -> None:
        """Persist a deletion without opening any directory already removed."""

        parent = os.path.dirname(os.path.abspath(deleted_path)) or os.path.sep
        # A retry can arrive after both the target and one or more of its
        # parents disappeared.  Start at the nearest surviving directory so
        # the deletion can still be durably anchored instead of failing while
        # trying to open an already-removed immediate parent.
        surviving_parent = self._nearest_existing_directory(parent)
        stop = self._logical_durability_anchor if logical_input else surviving_parent
        self._fsync_directory_chain(surviving_parent, stop_directory=stop)

    def delete_prefix(self, path: str) -> None:
        """Delete a non-root local prefix, preserving absolute-path support."""
        raw_path = os.fspath(path)
        if os.path.isabs(raw_path):
            normalized_absolute = os.path.abspath(os.path.normpath(raw_path))
            if os.path.dirname(normalized_absolute) == normalized_absolute:
                raise ValueError("Refusing to delete a filesystem root")
        self._reject_delete_dot_segments(raw_path)
        normalized = self._require_nonempty_delete_prefix(path)
        if os.path.isabs(normalized):
            resolved = os.path.normpath(normalized)
        else:
            # Match ``delete``: contain every parent component but do not
            # dereference the final directory entry. A final symlink is the
            # logical prefix to remove, never authority to recurse into its
            # target (which may intentionally live outside this root).
            parent, leaf = os.path.split(normalized)
            resolved_parent = self._resolve_path(parent or ".")
            resolved = os.path.normpath(os.path.join(resolved_parent, leaf))
        absolute_path = os.path.abspath(resolved)
        if os.path.dirname(absolute_path) == absolute_path:
            raise ValueError("Refusing to delete a filesystem root")
        if absolute_path == self.root:
            raise ValueError("Refusing to delete the configured storage root")
        super().delete_prefix(normalized)

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
        path = self._resolve_path(path)
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
        """Durably publish a complete local Parquet file at ``path``."""

        logical_input = not os.path.isabs(os.fspath(path))
        path = self._resolve_path(path)
        directory = os.path.dirname(path) or "."
        durability_anchor = (
            self._logical_durability_anchor
            if logical_input
            else self._nearest_existing_directory(directory)
        )
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-parquet-", dir=directory)
        os.close(fd)
        try:
            # PyArrow owns/closes its path handle. Reopen the completed temp
            # object only for fsync, then atomically install it in the same
            # directory so readers can never observe a partial footer.
            pq.write_table(table, tmp_path)
            with open(tmp_path, "rb") as completed:
                os.fsync(completed.fileno())
            os.replace(tmp_path, path)
            self._fsync_published_directory(
                directory,
                logical_input=logical_input,
                stop_directory=durability_anchor,
            )
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        path = self._resolve_path(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Parquet file not found at: {path}")

        try:
            proj = (
                self._project_columns(pq.read_schema(path).names, columns)
                if columns is not None
                else None
            )
            # partitioning=None: read only the file's own footer columns; never let
            # pyarrow infer Hive year/month/day from a ``year=YYYY/...`` path.  The
            # object-store backends read from a BytesIO buffer (no path) and so never
            # infer -- this keeps LocalStorage consistent with them and upholds the
            # "partition columns are path-only, never in the body" contract.  Without
            # it a full read injects int32 year/month/day that compaction bakes into
            # the rewritten body, leaking them into query output and breaking later
            # reads with an int32-vs-dictionary merge error.
            return pq.read_table(path, columns=proj, partitioning=None)
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet file at '{path}': {e}")

    def write_bytes(self, path: str, data: bytes) -> None:
        # ``processing`` selects this exact-byte path for immutable Parquet
        # resources and publishes their snapshot pointer immediately after the
        # call. Give it the same crash-durable boundary as the explicit audit
        # helper instead of acknowledging a buffered, partially visible file.
        self._write_bytes_atomic_with_identity(path, data)

    def write_bytes_atomic(self, path: str, data: bytes) -> None:
        """Durably replace a byte object without exposing a partial target.

        Audit archives use deterministic paths and must be safely retryable
        after process or host failure.  A same-directory temporary file plus
        ``os.replace`` gives readers either the previous complete object or the
        new complete object, never a prefix written by a crashed process.
        """
        self._write_bytes_atomic_with_identity(path, data)

    def write_bytes_with_identity(
            self, path: str, data: bytes,
    ) -> LocalWriteIdentity:
        """Durably publish bytes and return the identity of that exact inode.

        This is a local-only optimization boundary for immutable resources.
        Ordinary storage-interface callers continue to use ``write_bytes``.
        """
        return self._write_bytes_atomic_with_identity(path, data)

    def _write_bytes_atomic_with_identity(
            self, path: str, data: bytes,
    ) -> LocalWriteIdentity:
        logical_input = not os.path.isabs(os.fspath(path))
        path = self._resolve_path(path)
        directory = os.path.dirname(path) or "."
        canonical_path = os.path.join(
            os.path.realpath(directory), os.path.basename(path),
        )
        durability_anchor = (
            self._logical_durability_anchor
            if logical_input
            else self._nearest_existing_directory(directory)
        )
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-bytes-", dir=directory)
        published_state = None
        try:
            with os.fdopen(fd, "wb") as tmpf:
                write_all(tmpf, data)
                tmpf.flush()
                os.fsync(tmpf.fileno())
                os.replace(tmp_path, path)
                self._fsync_published_directory(
                    directory,
                    logical_input=logical_input,
                    stop_directory=durability_anchor,
                )
                # Capture after rename + directory fsync while the exact
                # published inode is still held open. A later path replacement
                # cannot acquire this cache identity.
                published_state = os.fstat(tmpf.fileno())
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
        if published_state is None:  # pragma: no cover - success sets it
            raise RuntimeError("Local byte publication produced no identity")
        return LocalWriteIdentity(
            canonical_path=canonical_path,
            device=int(published_state.st_dev),
            inode=int(published_state.st_ino),
            size=int(published_state.st_size),
            mtime_ns=int(published_state.st_mtime_ns),
            ctime_ns=int(published_state.st_ctime_ns),
        )

    @staticmethod
    def _open_directory(directory: str) -> _DirectoryHandle:
        """Open one directory and retain its device/inode identity."""

        absolute = os.path.abspath(directory)
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        fd = os.open(absolute, os.O_RDONLY | directory_flag)
        try:
            stat_result = os.fstat(fd)
            if not stat.S_ISDIR(stat_result.st_mode):
                raise NotADirectoryError(absolute)
            return _DirectoryHandle(absolute, fd, stat_result)
        except BaseException:
            os.close(fd)
            raise

    @classmethod
    def _open_existing_ancestor_chain(
        cls,
        directory: str,
    ) -> Dict[str, _DirectoryHandle]:
        """Pin the pre-existing anchor and its ancestors for replacement checks."""

        handles: Dict[str, _DirectoryHandle] = {}
        current = os.path.abspath(directory)
        try:
            while True:
                handles[current] = cls._open_directory(current)
                parent = os.path.dirname(current)
                if parent == current:
                    return handles
                current = parent
        except BaseException:
            for handle in handles.values():
                handle.close()
            raise

    @classmethod
    def _fsync_directory(cls, directory: str) -> _DirectoryHandle:
        """Persist a directory-entry update or raise.

        Storage publication must not acknowledge an object merely because
        ``os.replace`` was visible before a crash. On POSIX the containing
        directory must also be fsynced; suppressing that error would let a
        catalog pointer reference an object whose rename was never durable.
        """

        handle = cls._open_directory(directory)
        try:
            os.fsync(handle.fd)
            return handle
        except BaseException:
            handle.close()
            raise

    @classmethod
    def _nearest_existing_directory(cls, directory: str) -> str:
        """Return the closest existing ancestor before a hierarchy is created."""

        current = os.path.abspath(directory)
        while not os.path.isdir(current):
            parent = os.path.dirname(current)
            if parent == current:
                raise FileNotFoundError(
                    f"no existing ancestor directory for {directory!r}"
                )
            current = parent
        return current

    @staticmethod
    def _directory_chain(
        directory: str,
        *,
        stop_directory: str,
    ) -> tuple[str, ...]:
        """Return ``directory`` through ``stop_directory``, both inclusive."""

        current = os.path.abspath(directory)
        stop = os.path.abspath(stop_directory)
        try:
            common = os.path.commonpath((current, stop))
        except ValueError as exc:
            raise ValueError("directory durability anchor is on another drive") from exc
        if common != stop:
            raise ValueError("directory durability anchor is not an ancestor")
        paths = []
        while True:
            paths.append(current)
            if current == stop:
                return tuple(paths)
            parent = os.path.dirname(current)
            if parent == current:
                raise ValueError("directory durability anchor was not reached")
            current = parent

    @classmethod
    def _fsync_directory_chain(
        cls,
        directory: str,
        *,
        stop_directory: str,
        retain_handles: bool = False,
    ) -> tuple[_DirectoryHandle, ...]:
        """Durably anchor a hierarchy, bounded by a known storage ancestor."""

        handles: list[_DirectoryHandle] = []
        complete_identity_set = True
        try:
            for current in cls._directory_chain(
                directory,
                stop_directory=stop_directory,
            ):
                handle = cls._fsync_directory(current)
                # A few legacy tests replace this helper with a recording stub
                # that returns None. The sync contract is still exercised, but
                # no identity may be cached from incomplete test doubles.
                if isinstance(handle, _DirectoryHandle):
                    handles.append(handle)
                else:
                    complete_identity_set = False
        except BaseException:
            for handle in handles:
                handle.close()
            raise
        if retain_handles and complete_identity_set:
            return tuple(handles)
        for handle in handles:
            handle.close()
        return ()

    @staticmethod
    def _path_is_within(path: str, directory: str) -> bool:
        try:
            return os.path.commonpath((path, directory)) == directory
        except ValueError:
            return False

    def _invalidate_durable_prefix_locked(self, directory: str) -> None:
        prefix = os.path.abspath(directory)
        stale = [
            path
            for path in self._durable_directories
            if self._path_is_within(path, prefix)
        ]
        for path in stale:
            self._durable_directories.pop(path).close()

    def _invalidate_durable_prefix(self, directory: str) -> None:
        with self._durability_lock:
            self._invalidate_durable_prefix_locked(directory)

    def _valid_durable_handle_locked(
        self,
        directory: str,
    ) -> _DirectoryHandle | None:
        path = os.path.abspath(directory)
        cached = self._durable_directories.get(path)
        if cached is not None:
            if cached.matches_path():
                self._durable_directories.move_to_end(path)
                return cached
            # If a cached ancestor changed identity, no descendant below that
            # path may continue to claim a durable link to the old hierarchy.
            self._invalidate_durable_prefix_locked(path)

        trusted = self._trusted_durability_anchors.get(path)
        if trusted is not None and trusted.matches_path():
            return trusted
        return None

    def _deepest_durable_anchor_locked(self, directory: str) -> str | None:
        """Find the closest directory whose complete ancestry is still pinned."""

        paths = []
        current = os.path.abspath(directory)
        while True:
            paths.append(current)
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent

        deepest = None
        complete_suffix = True
        for path in reversed(paths):
            if complete_suffix and self._valid_durable_handle_locked(path) is not None:
                deepest = path
            else:
                complete_suffix = False
        return deepest

    def _cache_durable_handles_locked(
        self,
        handles: Sequence[_DirectoryHandle],
    ) -> None:
        for handle in handles:
            trusted = self._trusted_durability_anchors.get(handle.path)
            if trusted is not None and trusted.identity == handle.identity:
                handle.close()
                continue
            previous = self._durable_directories.pop(handle.path, None)
            if previous is not None:
                previous.close()
            self._durable_directories[handle.path] = handle

        while len(self._durable_directories) > self._durable_directory_limit:
            _path, evicted = self._durable_directories.popitem(last=False)
            evicted.close()

    def _fsync_logical_publication(self, directory: str) -> None:
        """Fsync one rename and anchor only ancestry not already durable."""

        current = os.path.abspath(directory)
        with self._durability_lock:
            anchor = self._deepest_durable_anchor_locked(current)
            if anchor is None:
                raise OSError(
                    f"no inode-validated durability anchor remains for {current!r}"
                )

            if anchor == current:
                expected = self._valid_durable_handle_locked(current)
                synced = self._fsync_directory(current)
                if not isinstance(synced, _DirectoryHandle):
                    return
                try:
                    if (
                        expected is None
                        or synced.identity != expected.identity
                        or not synced.matches_path()
                        or self._deepest_durable_anchor_locked(current) != current
                    ):
                        raise OSError(
                            f"directory hierarchy changed during publication: {current}"
                        )
                finally:
                    synced.close()
                return

            expected_anchor = self._valid_durable_handle_locked(anchor)
            if expected_anchor is None:
                raise OSError(f"durability anchor changed before publication: {anchor}")
            handles = self._fsync_directory_chain(
                current,
                stop_directory=anchor,
                retain_handles=True,
            )
            if not handles:
                return
            try:
                if handles[-1].identity != expected_anchor.identity or any(
                    not handle.matches_path() for handle in handles
                ):
                    raise OSError(
                        f"directory hierarchy changed during publication: {current}"
                    )
                anchor_parent = os.path.dirname(anchor)
                if (
                    anchor_parent != anchor
                    and self._deepest_durable_anchor_locked(anchor_parent)
                    != anchor_parent
                ):
                    raise OSError(
                        f"durability anchor changed during publication: {anchor}"
                    )
                self._cache_durable_handles_locked(handles)
            except BaseException:
                for handle in handles:
                    handle.close()
                raise

    def _fsync_published_directory(
        self,
        directory: str,
        *,
        logical_input: bool,
        stop_directory: str,
    ) -> None:
        if logical_input:
            self._fsync_logical_publication(directory)
            return
        self._fsync_directory_chain(
            directory,
            stop_directory=stop_directory,
        )

    def ensure_bytes_durable(self, path: str) -> None:
        """Re-establish durability for an already-visible byte object.

        This is used when a prior process may have completed ``os.replace``
        but failed before syncing the parent directory.  Retrying the audit
        delivery can then make the existing exact object durable without
        rewriting its immutable contents.
        """

        logical_input = not os.path.isabs(path)
        path = self._resolve_path(path)
        with open(path, "rb") as existing:
            os.fsync(existing.fileno())
        directory = os.path.dirname(path) or "."
        # Archive paths are relative to the configured LocalStorage working
        # directory. Re-sync through that bounded namespace root so a retry
        # also anchors directory components created by a prior interrupted
        # publication. Absolute paths are treated as pre-provisioned and only
        # their immediate parent is synced.
        stop = self._logical_durability_anchor if logical_input else directory
        self._fsync_published_directory(
            directory,
            logical_input=logical_input,
            stop_directory=stop,
        )

    def read_bytes(self, path: str) -> bytes:
        path = self._resolve_path(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(path, "rb") as f:
            return f.read()

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        path = self._resolve_path(path)
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding=encoding) as f:
            f.write(text)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        path = self._resolve_path(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(path, "r", encoding=encoding) as f:
            return f.read()

    def copy(self, src_path: str, dst_path: str) -> None:
        src_path = self._resolve_path(src_path)
        logical_destination = not os.path.isabs(os.fspath(dst_path))
        dst_path = self._resolve_path(dst_path)
        if os.path.exists(dst_path) and os.path.samefile(src_path, dst_path):
            raise shutil.SameFileError(src_path, dst_path, "same file")
        directory = os.path.dirname(dst_path) or "."
        durability_anchor = (
            self._logical_durability_anchor
            if logical_destination
            else self._nearest_existing_directory(directory)
        )
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-copy-", dir=directory)
        os.close(fd)
        try:
            shutil.copyfile(src_path, tmp_path)
            with open(tmp_path, "rb") as completed:
                os.fsync(completed.fileno())
            os.replace(tmp_path, dst_path)
            self._fsync_published_directory(
                directory,
                logical_input=logical_destination,
                stop_directory=durability_anchor,
            )
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
