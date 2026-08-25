# route: supertable.storage.local_storage
import contextvars
import ctypes
import errno
import fnmatch
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import stat
import sys
import tempfile
import threading
import time
import hashlib
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from typing import Any, BinaryIO, Dict, List, Optional, Sequence

from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    StorageInterface,
    storage_error_type,
    validate_range_request,
    write_all,
)


_FILE_SYNC_LOCK = threading.Lock()
_FILE_SYNC_EXECUTOR: ThreadPoolExecutor | None = None
_FILE_SYNC_EXECUTOR_PID: int | None = None


def _reset_file_sync_pool_after_fork() -> None:
    """Discard parent-only worker bookkeeping in a forked child."""

    global _FILE_SYNC_LOCK, _FILE_SYNC_EXECUTOR, _FILE_SYNC_EXECUTOR_PID
    # No executor thread survives fork, and its inherited locks may have been
    # held by a vanished thread. Do not call shutdown in the child; replace the
    # state outright while the parent's independent memory remains untouched.
    _FILE_SYNC_LOCK = threading.Lock()
    _FILE_SYNC_EXECUTOR = None
    _FILE_SYNC_EXECUTOR_PID = None
    inherited_batch = globals().get("_ACTIVE_DURABILITY_BATCH")
    if inherited_batch is not None:
        inherited_batch.set(None)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_file_sync_pool_after_fork)


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


class _PinnedDirectoryChain:
    """Descriptor-pinned path from one trusted ancestor to a destination.

    Immutable publication must never re-resolve a shared directory pathname
    at its commit point. Every child in this chain was opened relative to its
    already-pinned parent with ``O_NOFOLLOW``. Relationship checks make a
    rename, symlink substitution, or delete/recreate race an ambiguous failure
    instead of an acknowledgement in the wrong namespace.
    """

    __slots__ = ("handles", "entry_names")

    def __init__(
        self,
        handles: Sequence[_DirectoryHandle],
        entry_names: Sequence[str],
    ) -> None:
        if not handles or len(entry_names) != len(handles) - 1:
            raise ValueError("invalid pinned directory chain")
        self.handles = tuple(handles)
        self.entry_names = tuple(entry_names)

    @property
    def directory(self) -> _DirectoryHandle:
        return self.handles[-1]

    @staticmethod
    def _same_directory(
        stat_result: os.stat_result,
        handle: _DirectoryHandle,
    ) -> bool:
        return bool(
            stat.S_ISDIR(stat_result.st_mode)
            and (int(stat_result.st_dev), int(stat_result.st_ino))
            == handle.identity
        )

    def validate(self) -> None:
        first = self.handles[0]
        if not first.matches_path():
            raise ObjectIdentityMismatch(
                "local immutable directory hierarchy changed"
            )
        for parent, child, entry_name in zip(
            self.handles,
            self.handles[1:],
            self.entry_names,
        ):
            try:
                entry_state = os.stat(
                    entry_name,
                    dir_fd=parent.fd,
                    follow_symlinks=False,
                )
                opened_state = os.fstat(child.fd)
            except (FileNotFoundError, NotADirectoryError, OSError):
                raise ObjectIdentityMismatch(
                    "local immutable directory hierarchy changed"
                ) from None
            if (
                not self._same_directory(entry_state, child)
                or not self._same_directory(opened_state, child)
            ):
                raise ObjectIdentityMismatch(
                    "local immutable directory hierarchy changed"
                )

    def fsync(self) -> None:
        """Persist the destination entry and every component back to anchor."""

        self.validate()
        for handle in reversed(self.handles):
            os.fsync(handle.fd)
        self.validate()

    def close(self) -> None:
        for handle in reversed(self.handles):
            handle.close()


_AT_FDCWD = -100
_AT_SYMLINK_FOLLOW = 0x400
_AT_EMPTY_PATH = 0x1000


def _linux_link_file_descriptor_no_replace(
    source_fd: int,
    destination_dir_fd: int,
    destination_name: str,
) -> bool:
    """Hard-link one retained inode without resolving a staging pathname.

    Linux's ``linkat(AT_EMPTY_PATH)`` is the direct operation. Unprivileged
    processes normally lack ``CAP_DAC_READ_SEARCH``, so the documented procfs
    descriptor-link form is the safe fallback. Both forms retain the kernel
    file description as source authority and preserve ``EEXIST`` as the only
    definite loser result.
    """

    if not sys.platform.startswith("linux"):
        raise NotImplementedError(
            "Local immutable create requires Linux descriptor linking"
        )
    if not isinstance(destination_name, str) or not destination_name:
        raise ValueError("local immutable destination name is empty")
    if "\x00" in destination_name or os.sep in destination_name:
        raise ValueError("local immutable destination name is invalid")

    source_state = os.fstat(source_fd)
    if not stat.S_ISREG(source_state.st_mode):
        raise ObjectIdentityMismatch("local immutable source is not a regular file")

    try:
        linkat = ctypes.CDLL(None, use_errno=True).linkat
    except (AttributeError, OSError):  # pragma: no cover - non-glibc Linux
        raise NotImplementedError(
            "Local immutable create requires linkat"
        ) from None
    linkat.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
    )
    linkat.restype = ctypes.c_int
    encoded_name = os.fsencode(destination_name)

    ctypes.set_errno(0)
    result = linkat(
        source_fd,
        b"",
        destination_dir_fd,
        encoded_name,
        _AT_EMPTY_PATH,
    )
    if result == 0:
        return True
    direct_errno = ctypes.get_errno()
    if direct_errno == errno.EEXIST:
        return False
    # AT_EMPTY_PATH requires CAP_DAC_READ_SEARCH. Fall back only for errors
    # that mean the descriptor form itself was unavailable; I/O, capacity,
    # permission-on-destination and other ambiguous failures remain errors.
    fallback_errnos = {
        errno.EACCES,
        errno.EINVAL,
        errno.ENOENT,
        errno.ENOSYS,
        errno.EPERM,
    }
    if hasattr(errno, "EOPNOTSUPP"):
        fallback_errnos.add(errno.EOPNOTSUPP)
    if direct_errno not in fallback_errnos:
        raise OSError(direct_errno, os.strerror(direct_errno))

    proc_source = f"/proc/self/fd/{source_fd}"
    try:
        proc_state = os.stat(proc_source)
    except OSError:
        raise NotImplementedError(
            "Local immutable create requires AT_EMPTY_PATH or procfs"
        ) from None
    if (
        not stat.S_ISREG(proc_state.st_mode)
        or int(proc_state.st_dev) != int(source_state.st_dev)
        or int(proc_state.st_ino) != int(source_state.st_ino)
    ):
        raise ObjectIdentityMismatch(
            "local immutable descriptor identity changed"
        )

    ctypes.set_errno(0)
    result = linkat(
        _AT_FDCWD,
        os.fsencode(proc_source),
        destination_dir_fd,
        encoded_name,
        _AT_SYMLINK_FOLLOW,
    )
    if result == 0:
        return True
    proc_errno = ctypes.get_errno()
    if proc_errno == errno.EEXIST:
        return False
    raise OSError(proc_errno, os.strerror(proc_errno))


class _BatchedPublication:
    """One newly-created immutable object owned by a durability batch."""

    __slots__ = (
        "path",
        "directory",
        "device",
        "inode",
        "fd",
        "sync_future",
        "published",
    )

    def __init__(self, path: str, directory: str) -> None:
        self.path = os.path.abspath(path)
        self.directory = os.path.abspath(directory)
        self.device: int | None = None
        self.inode: int | None = None
        self.fd: int | None = None
        self.sync_future: Future[None] | None = None
        self.published = False

    def pin_published_file(
        self,
        published_fd: int | None = None,
        *,
        source_path: str | None = None,
    ) -> None:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = (
            os.dup(published_fd)
            if published_fd is not None
            else os.open(source_path or self.path, flags)
        )
        try:
            observed = os.fstat(fd)
            if not stat.S_ISREG(observed.st_mode):
                raise OSError("published object is not a regular file") from None
            self.device = int(observed.st_dev)
            self.inode = int(observed.st_ino)
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise

    def path_still_names_published_file(self) -> bool:
        if self.fd is None or self.device is None or self.inode is None:
            return False
        try:
            current = os.stat(self.path, follow_symlinks=False)
            pinned = os.fstat(self.fd)
        except (FileNotFoundError, NotADirectoryError, OSError):
            return False
        expected = (self.device, self.inode)
        return (
            stat.S_ISREG(current.st_mode)
            and stat.S_ISREG(pinned.st_mode)
            and (int(current.st_dev), int(current.st_ino)) == expected
            and (int(pinned.st_dev), int(pinned.st_ino)) == expected
        )

    def close(self) -> None:
        fd, self.fd = self.fd, None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass


_ACTIVE_DURABILITY_BATCH: contextvars.ContextVar[Optional["_DurabilityBatch"]] = (
    contextvars.ContextVar("supertable_local_durability_batch", default=None)
)


class _DurabilityBatch:
    """Write-scoped directory durability barrier for immutable objects.

    Each complete temporary file is atomically renamed immediately, keeping
    the final path available to pre-commit compaction and metadata work.  The
    exact renamed inode remains pinned while a bounded storage-wide pool runs
    ``fdatasync``.  The barrier waits for every file, verifies every pathname,
    and only then flushes the deduplicated directory ancestry.  Consequently a
    catalog pointer cannot become visible before both file bytes/size and its
    name are durable, while file latency overlaps the rest of the write.

    A batch is context-local (and explicitly propagated to writer worker
    threads), so independent writers sharing one LocalStorage root cannot
    steal each other's publication scope.
    """

    __slots__ = (
        "storage",
        "root",
        "pid",
        "_lock",
        "_publications",
        "_state",
        "_token",
    )

    def __init__(self, storage: "LocalStorage") -> None:
        self.storage = storage
        self.root = storage.root
        self.pid = os.getpid()
        self._lock = threading.RLock()
        self._publications: list[_BatchedPublication] = []
        self._state = "new"
        self._token = None

    def __enter__(self) -> "_DurabilityBatch":
        with self._lock:
            self._require_owner_locked()
            if self._state != "new":
                raise RuntimeError("durability batch cannot be re-entered")
            if _ACTIVE_DURABILITY_BATCH.get() is not None:
                raise RuntimeError("nested LocalStorage durability batches are not supported")
            self._token = _ACTIVE_DURABILITY_BATCH.set(self)
            self._state = "open"
        return self

    def accepts(self, storage: "LocalStorage") -> bool:
        return (
            self.pid == os.getpid()
            and self.root == storage.root
            and self._state == "open"
        )

    def _require_owner_locked(self) -> None:
        if self.pid != os.getpid():
            raise RuntimeError("durability batch cannot cross a fork boundary")

    def publish_new_immutable(
        self,
        *,
        tmp_path: str,
        path: str,
        directory: str,
        published_fd: int | None = None,
    ) -> bool:
        """Rename, pin and enqueue a new target, or decline replacements."""

        with self._lock:
            self._require_owner_locked()
            if self._state != "open":
                raise RuntimeError("cannot publish after the durability barrier")
            # DataWriter paths are UUID-named immutable objects.  A caller that
            # intentionally replaces an existing object retains the ordinary
            # per-call fsync semantics and is never enrolled in rollback.
            if os.path.lexists(path):
                return False
            publication = _BatchedPublication(path, directory)
            self._publications.append(publication)
            # Pin the exact completed temp inode before exposing its final
            # pathname.  A dup remains valid after the caller closes its
            # stream and cannot be redirected by a later path substitution.
            publication.pin_published_file(
                published_fd,
                source_path=tmp_path,
            )
            os.replace(tmp_path, path)
            publication.published = True
            if not publication.path_still_names_published_file():
                raise OSError(
                    "immutable object changed during publication"
                ) from None
            if publication.fd is None:  # pragma: no cover - pin proves this
                raise RuntimeError("published immutable object has no pinned fd")
            publication.sync_future = self.storage._submit_file_sync(
                publication.fd,
            )
            return True

    def _await_file_syncs_locked(self, *, propagate: bool) -> None:
        """Drain every submitted sync before an owned descriptor can close."""

        first_error: BaseException | None = None
        for publication in self._publications:
            future = publication.sync_future
            if future is None:
                continue
            try:
                future.result()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if propagate and first_error is not None:
            raise first_error

    def _verify_publications_locked(self) -> None:
        for publication in self._publications:
            if (
                publication.published
                and not publication.path_still_names_published_file()
            ):
                raise OSError(
                    "immutable object changed before durability barrier"
                ) from None

    def barrier(self) -> None:
        """Make every enrolled rename durable before catalog publication."""

        with self._lock:
            self._require_owner_locked()
            if self._state != "open":
                raise RuntimeError("durability barrier may run exactly once")
            self._verify_publications_locked()
            # fdatasync persists the bytes and metadata required to retrieve
            # them (notably file size).  Directory entries are deliberately
            # flushed only after every exact inode completed successfully.
            self._await_file_syncs_locked(propagate=True)
            self._verify_publications_locked()
            directories = {
                publication.directory
                for publication in self._publications
                if publication.published
            }
            if directories:
                self.storage._fsync_logical_publications(directories)
            # Detect directory replacement, path substitution, or unlink races
            # both before and after the directory durability operation.
            self._verify_publications_locked()
            self._state = "durable"

    def catalog_commit_started(self) -> None:
        """Mark the point after which a Redis failure can be ambiguous."""

        with self._lock:
            self._require_owner_locked()
            if self._state != "durable":
                raise RuntimeError("catalog commit requires a completed durability barrier")
            self._state = "commit_started"

    def catalog_commit_succeeded(self) -> None:
        with self._lock:
            self._require_owner_locked()
            if self._state != "commit_started":
                raise RuntimeError("catalog commit was not started")
            self._state = "committed"

    def catalog_commit_rejected(self) -> None:
        """Record a typed, definite CAS/lease rejection (not an ambiguity)."""

        with self._lock:
            self._require_owner_locked()
            if self._state != "commit_started":
                raise RuntimeError("catalog commit was not started")
            self._state = "commit_rejected"

    def abort(self) -> None:
        """Remove only still-identical new paths when Redis was never attempted."""

        with self._lock:
            if self.pid != os.getpid():
                return
            if self._state in {"aborted", "committed", "commit_started", "closed"}:
                return
            # An application failure can race an already-running sync. Drain
            # it before closing/reusing the pinned descriptor. Its result does
            # not matter once the uncommitted name is durably removed.
            self._await_file_syncs_locked(propagate=False)
            cleanup_directories: set[str] = set()
            first_error: BaseException | None = None
            for publication in reversed(self._publications):
                if not publication.published:
                    continue
                if not publication.path_still_names_published_file():
                    continue
                try:
                    os.unlink(publication.path)
                    cleanup_directories.add(publication.directory)
                except FileNotFoundError:
                    continue
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
            try:
                if cleanup_directories:
                    self.storage._fsync_logical_publications(cleanup_directories)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            self._state = "aborted"
            if first_error is not None:
                raise first_error

    def close(self) -> None:
        with self._lock:
            if self.pid != os.getpid():
                # Futures refer to parent-only threads and must never be
                # awaited in the forked child. Closing the child's copies of
                # descriptors cannot affect the parent's open-file table.
                for publication in self._publications:
                    publication.close()
                self._token = None
                self._state = "closed"
                return
            self._await_file_syncs_locked(propagate=False)
            for publication in self._publications:
                publication.close()
            if self._token is not None and self.pid == os.getpid():
                _ACTIVE_DURABILITY_BATCH.reset(self._token)
                self._token = None
            if self._state not in {"aborted", "committed", "commit_started"}:
                self._state = "closed"

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        try:
            if exc_type is not None:
                self.abort()
            elif self._state != "committed":
                raise RuntimeError("durability batch exited without a catalog commit")
        finally:
            self.close()
        return False


class LocalStorage(StorageInterface):
    """
    A local disk-based implementation of StorageInterface.
    """

    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        # A directly constructed LocalStorage retains its long-standing
        # caller-CWD namespace.  Production construction through
        # storage_factory passes the explicitly initialised application home,
        # so package import never needs to mutate the process CWD.  Absolute
        # Absolute paths are accepted only when they remain inside this
        # storage namespace; callers that need another namespace must create a
        # separate LocalStorage instance explicitly.
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

    def durability_batch(self) -> _DurabilityBatch:
        """Create a write-scoped immutable-publication durability batch.

        Callers must complete :meth:`_DurabilityBatch.barrier` before starting
        their catalog transaction, then mark that transaction started/succeeded.
        Ordinary LocalStorage callers that do not enter this scope retain the
        existing per-object file+directory fsync boundary.
        """

        return _DurabilityBatch(self)

    @staticmethod
    def _fdatasync_file(fd: int) -> None:
        """Persist file data and the metadata required to read it back."""

        fdatasync = getattr(os, "fdatasync", None)
        if fdatasync is None:  # pragma: no cover - POSIX platforms provide it
            os.fsync(fd)
        else:
            fdatasync(fd)

    def _submit_file_sync(self, fd: int) -> Future[None]:
        """Submit one exact-inode sync to a process-local bounded pool."""

        global _FILE_SYNC_EXECUTOR, _FILE_SYNC_EXECUTOR_PID
        pid = os.getpid()
        with _FILE_SYNC_LOCK:
            # Worker threads do not survive fork. Never submit into the
            # inherited bookkeeping of a parent process's executor.
            if (
                _FILE_SYNC_EXECUTOR is None
                or _FILE_SYNC_EXECUTOR_PID != pid
            ):
                _FILE_SYNC_EXECUTOR = ThreadPoolExecutor(
                    max_workers=4,
                    thread_name_prefix="supertable-fdatasync",
                )
                _FILE_SYNC_EXECUTOR_PID = pid
            return _FILE_SYNC_EXECUTOR.submit(self._fdatasync_file, fd)

    def _active_durability_batch(
        self,
        *,
        logical_input: bool,
    ) -> Optional[_DurabilityBatch]:
        # Absolute paths are a compatibility escape hatch that may live outside
        # this storage namespace; never enroll them in a root-scoped batch.
        if not logical_input:
            return None
        batch = _ACTIVE_DURABILITY_BATCH.get()
        if batch is not None and batch.accepts(self):
            return batch
        return None

    def _publish_completed_temp(
        self,
        *,
        tmp_path: str,
        path: str,
        directory: str,
        logical_input: bool,
        durability_anchor: str,
        published_fd: int | None = None,
    ) -> os.stat_result:
        """Install one file, overlapping durability only for scoped immutables."""

        batch = self._active_durability_batch(logical_input=logical_input)
        if batch is not None and batch.publish_new_immutable(
            tmp_path=tmp_path,
            path=path,
            directory=directory,
            published_fd=published_fd,
        ):
            return (
                os.fstat(published_fd)
                if published_fd is not None
                else os.stat(path, follow_symlinks=False)
            )
        # Replacements and ordinary callers keep their established synchronous
        # file-before-rename ordering. Only UUID-named DataWriter objects are
        # allowed to defer this work to the pre-catalog batch barrier.
        if published_fd is None:
            with open(tmp_path, "rb") as completed:
                os.fsync(completed.fileno())
        else:
            os.fsync(published_fd)
        os.replace(tmp_path, path)
        self._fsync_published_directory(
            directory,
            logical_input=logical_input,
            stop_directory=durability_anchor,
        )
        return (
            os.fstat(published_fd)
            if published_fd is not None
            else os.stat(path, follow_symlinks=False)
        )

    def _resolve_path(self, path: str | os.PathLike[str]) -> str:
        raw = os.fspath(path)
        if os.path.isabs(raw):
            physical = os.path.realpath(os.path.normpath(raw))
            try:
                contained = os.path.commonpath((self.root, physical)) == self.root
            except ValueError:
                contained = False
            if not contained:
                raise ValueError("Local storage path escapes configured root")
            return physical
        physical = os.path.realpath(os.path.join(self.root, raw))
        try:
            contained = os.path.commonpath((self.root, physical)) == self.root
        except ValueError:
            contained = False
        if not contained:
            raise ValueError("Local storage path escapes configured root")
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
            raise FileNotFoundError("File not found")

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
                            raise ValueError("File is empty")
                        time.sleep(backoff)
                        continue
                except FileNotFoundError:
                    # vanished between exists() and getsize(); retry
                    if attempt == attempts:
                        raise FileNotFoundError("File not found") from None
                    time.sleep(backoff)
                    continue

                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)

            except json.JSONDecodeError:
                # reader may have raced with a writer that just replaced the file;
                # give it a tiny moment to settle, then retry
                if attempt == attempts:
                    raise ValueError("Invalid JSON") from None
                time.sleep(backoff)
            except FileNotFoundError:
                # replaced again during open—retry
                if attempt == attempts:
                    raise
                time.sleep(backoff)

        # Should never get here
        raise RuntimeError("Unexpected failure reading JSON")

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
                # Persist immediately for ordinary callers. A DataWriter
                # immutable batch overlaps exact-inode fdatasync and defers the
                # directory flush to its pre-catalog barrier.
                self._publish_completed_temp(
                    tmp_path=tmp_path,
                    path=path,
                    directory=directory,
                    logical_input=logical_input,
                    durability_anchor=durability_anchor,
                    published_fd=tmpf.fileno(),
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
            raise FileNotFoundError("File not found")
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
        except FileNotFoundError:
            raise FileNotFoundError("File not found") from None
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
        except FileNotFoundError:
            raise FileNotFoundError("File not found") from None

        with source:
            before = self._metadata_from_open_file(source)
            if expected is not None and before != expected:
                raise OSError("Object changed before download")
            written = 0
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                written += write_all(file_obj, chunk)
            after = self._metadata_from_open_file(source)
            if after != before:
                raise OSError("Object changed during download")
            if written != before.size:
                raise OSError(
                    f"Short download: expected {before.size} bytes, wrote {written}"
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
        except FileNotFoundError:
            raise FileNotFoundError("File not found") from None
        with source:
            before = self._metadata_from_open_file(source)
            if (
                expected is not None
                and before.identity_token() != expected.identity_token()
            ):
                raise ObjectIdentityMismatch(
                    "Object changed before range read"
                )
            if offset > before.size or length > before.size - offset:
                raise ObjectIdentityMismatch("Object shrank before range read")
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
                    raise ObjectIdentityMismatch("Short range read")
                chunks.append(chunk)
                position += len(chunk)
                remaining -= len(chunk)
            after = self._metadata_from_open_file(source)
            if after.identity_token() != before.identity_token():
                raise ObjectIdentityMismatch("Object changed during range read")
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
                    "Local storage delete path escapes configured root"
                ) from None
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
            raise FileNotFoundError("File or folder not found") from None
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
                self._publish_completed_temp(
                    tmp_path=tmp_path,
                    path=path,
                    directory=directory,
                    logical_input=logical_input,
                    durability_anchor=durability_anchor,
                    published_fd=completed.fileno(),
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
            raise FileNotFoundError("Parquet file not found") from None

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
            raise RuntimeError(
                f"Failed to read Parquet; error_type={storage_error_type(e)}"
            ) from None

    def write_bytes(self, path: str, data: bytes) -> None:
        # ``processing`` selects this exact-byte path for immutable Parquet
        # resources and publishes their snapshot pointer immediately after the
        # call. Give it the same crash-durable boundary as the explicit audit
        # helper instead of acknowledging a buffered, partially visible file.
        self._write_bytes_atomic_with_identity(path, data)

    def create_bytes_if_absent(self, path: str, data: bytes) -> bool:
        """Publish complete bytes from an unnamed inode with no overwrite.

        The commit source is an ``O_TMPFILE`` descriptor: it has no staging
        pathname for another process to swap, follow, truncate, or hard-link
        into the destination. The target is linked relative to a pinned
        directory descriptor and the complete directory ancestry is validated
        and synced before success is acknowledged.
        """

        chain, target_name = self._open_immutable_parent(path)
        source_fd: int | None = None
        try:
            source_fd = self._open_immutable_unnamed_file(chain.directory.fd)
            with os.fdopen(source_fd, "wb") as source:
                source_fd = None
                write_all(source, data)
                source.flush()
                os.fsync(source.fileno())
                source_state = os.fstat(source.fileno())
                if not stat.S_ISREG(source_state.st_mode):
                    raise ObjectIdentityMismatch(
                        "local immutable source is not a regular file"
                    )

                chain.validate()
                if not _linux_link_file_descriptor_no_replace(
                    source.fileno(),
                    chain.directory.fd,
                    target_name,
                ):
                    return False

                self._require_immutable_target_identity(
                    chain.directory.fd,
                    target_name,
                    source_state,
                )
                chain.fsync()
                self._require_immutable_target_identity(
                    chain.directory.fd,
                    target_name,
                    source_state,
                )
                chain.validate()
                return True
        finally:
            if source_fd is not None:
                os.close(source_fd)
            chain.close()

    def _open_immutable_parent(
        self,
        path: str,
    ) -> tuple[_PinnedDirectoryChain, str]:
        """Open/create a no-symlink parent chain using descriptor-relative I/O."""

        raw_path = os.fspath(path)
        if not isinstance(raw_path, str):
            raise TypeError("local immutable path must be a string")
        if "\x00" in raw_path:
            raise ValueError("local immutable path contains a null byte")
        logical_input = not os.path.isabs(raw_path)
        normalized = os.path.normpath(raw_path)
        if normalized in {"", "."}:
            raise ValueError("local immutable path must name an object")

        if logical_input:
            absolute_path = os.path.abspath(os.path.join(self.root, normalized))
            try:
                contained = (
                    os.path.commonpath((self.root, absolute_path)) == self.root
                )
            except ValueError:
                contained = False
            if not contained:
                raise ValueError(
                    "Local storage path escapes configured root"
                ) from None
            anchor_path = self._logical_durability_anchor
            with self._durability_lock:
                trusted = self._trusted_durability_anchors.get(anchor_path)
                if trusted is None or not trusted.matches_path():
                    raise ObjectIdentityMismatch(
                        "local immutable durability anchor changed"
                    )
                anchor_fd = os.dup(trusted.fd)
            anchor_state = os.fstat(anchor_fd)
            anchor = _DirectoryHandle(anchor_path, anchor_fd, anchor_state)
        else:
            absolute_path = os.path.abspath(normalized)
            anchor_path = os.path.abspath(os.path.sep)
            anchor = self._open_directory(anchor_path)

        directory = os.path.dirname(absolute_path)
        target_name = os.path.basename(absolute_path)
        if target_name in {"", ".", ".."} or os.sep in target_name:
            anchor.close()
            raise ValueError("local immutable path must name an object")
        try:
            relative_directory = os.path.relpath(directory, anchor_path)
            if relative_directory == os.pardir or relative_directory.startswith(
                os.pardir + os.sep
            ):
                raise ValueError("local immutable parent is outside its anchor")
            components = (
                ()
                if relative_directory == "."
                else tuple(relative_directory.split(os.sep))
            )
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            directory_flag = getattr(os, "O_DIRECTORY", 0)
            if not nofollow or not directory_flag:
                raise NotImplementedError(
                    "Local immutable create requires descriptor-safe directories"
                )
            open_flags = os.O_RDONLY | directory_flag | nofollow
            open_flags |= getattr(os, "O_CLOEXEC", 0)

            handles = [anchor]
            entry_names: list[str] = []
            for component in components:
                if component in {"", ".", ".."} or "\x00" in component:
                    raise ValueError("local immutable parent path is invalid")
                parent = handles[-1]
                try:
                    child_fd = os.open(
                        component,
                        open_flags,
                        dir_fd=parent.fd,
                    )
                except FileNotFoundError:
                    try:
                        os.mkdir(component, mode=0o777, dir_fd=parent.fd)
                    except FileExistsError:
                        pass
                    child_fd = os.open(
                        component,
                        open_flags,
                        dir_fd=parent.fd,
                    )
                child_path = os.path.join(parent.path, component)
                child_state = os.fstat(child_fd)
                handles.append(_DirectoryHandle(child_path, child_fd, child_state))
                entry_names.append(component)

            chain = _PinnedDirectoryChain(handles, entry_names)
            chain.validate()
            return chain, target_name
        except BaseException:
            for handle in reversed(locals().get("handles", [anchor])):
                handle.close()
            raise

    @staticmethod
    def _open_immutable_unnamed_file(directory_fd: int) -> int:
        tmpfile_flag = getattr(os, "O_TMPFILE", 0)
        if not tmpfile_flag:
            raise NotImplementedError(
                "Local immutable create requires O_TMPFILE"
            )
        flags = tmpfile_flag | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        try:
            return os.open(".", flags, 0o600, dir_fd=directory_fd)
        except OSError as exc:
            unsupported = {
                errno.EINVAL,
                errno.EISDIR,
                errno.ENOSYS,
            }
            if hasattr(errno, "EOPNOTSUPP"):
                unsupported.add(errno.EOPNOTSUPP)
            if exc.errno in unsupported:
                raise NotImplementedError(
                    "Local immutable create requires filesystem O_TMPFILE support"
                ) from None
            raise

    @staticmethod
    def _require_immutable_target_identity(
        directory_fd: int,
        target_name: str,
        source_state: os.stat_result,
    ) -> None:
        try:
            target_state = os.stat(
                target_name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except (FileNotFoundError, NotADirectoryError, OSError):
            raise ObjectIdentityMismatch(
                "local immutable publication identity changed"
            ) from None
        if (
            not stat.S_ISREG(target_state.st_mode)
            or int(target_state.st_dev) != int(source_state.st_dev)
            or int(target_state.st_ino) != int(source_state.st_ino)
            or int(target_state.st_size) != int(source_state.st_size)
        ):
            raise ObjectIdentityMismatch(
                "local immutable publication identity changed"
            )

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
                published_state = self._publish_completed_temp(
                    tmp_path=tmp_path,
                    path=path,
                    directory=directory,
                    logical_input=logical_input,
                    durability_anchor=durability_anchor,
                    published_fd=tmpf.fileno(),
                )
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
                    "no existing ancestor directory"
                ) from None
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
        except ValueError:
            raise ValueError("directory durability anchor is on another drive") from None
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
                    "no inode-validated durability anchor remains"
                ) from None

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
                            "directory hierarchy changed during publication"
                        ) from None
                finally:
                    synced.close()
                return

            expected_anchor = self._valid_durable_handle_locked(anchor)
            if expected_anchor is None:
                raise OSError(
                    "durability anchor changed before publication"
                ) from None
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
                        "directory hierarchy changed during publication"
                    ) from None
                anchor_parent = os.path.dirname(anchor)
                if (
                    anchor_parent != anchor
                    and self._deepest_durable_anchor_locked(anchor_parent)
                    != anchor_parent
                ):
                    raise OSError(
                        "durability anchor changed during publication"
                    ) from None
                self._cache_durable_handles_locked(handles)
            except BaseException:
                for handle in handles:
                    handle.close()
                raise

    def _fsync_logical_publications(self, directories: Sequence[str]) -> None:
        """Fsync a set of publication directories and each ancestor once.

        This is the barrier counterpart of ``_fsync_logical_publication``.  It
        computes every required chain before installing any new cache proof,
        deduplicates shared ancestors, flushes children before parents, and
        validates all pinned inode identities before caching the result.
        """

        currents = {
            os.path.abspath(directory)
            for directory in directories
        }
        if not currents:
            return
        with self._durability_lock:
            expected_anchors: Dict[str, tuple[int, int]] = {}
            required: set[str] = set()
            for current in currents:
                anchor = self._deepest_durable_anchor_locked(current)
                if anchor is None:
                    raise OSError(
                        "no inode-validated durability anchor remains"
                    ) from None
                expected = self._valid_durable_handle_locked(anchor)
                if expected is None:
                    raise OSError(
                        "durability anchor changed before publication"
                    ) from None
                expected_anchors[anchor] = expected.identity
                required.update(
                    self._directory_chain(current, stop_directory=anchor)
                )

            # Directory contents must reach stable storage before the parent
            # entry that links a newly-created directory into its own parent.
            ordered = sorted(
                required,
                key=lambda value: (value.count(os.sep), len(value), value),
                reverse=True,
            )
            handles: list[_DirectoryHandle] = []
            by_path: Dict[str, _DirectoryHandle] = {}
            complete_identity_set = True
            try:
                for current in ordered:
                    handle = self._fsync_directory(current)
                    if isinstance(handle, _DirectoryHandle):
                        handles.append(handle)
                        by_path[current] = handle
                    else:
                        # Preserve unit-test compatibility with recording stubs,
                        # matching the single-publication helper's behaviour.
                        complete_identity_set = False
                if not complete_identity_set:
                    return
                for anchor, expected_identity in expected_anchors.items():
                    synced = by_path.get(anchor)
                    if synced is None or synced.identity != expected_identity:
                        raise OSError(
                            "durability anchor changed during publication"
                        ) from None
                if any(not handle.matches_path() for handle in handles):
                    raise OSError("directory hierarchy changed during publication")
                self._cache_durable_handles_locked(handles)
                handles = []
            finally:
                for handle in handles:
                    handle.close()

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
            raise FileNotFoundError("File not found")
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
            raise FileNotFoundError("File not found")
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
