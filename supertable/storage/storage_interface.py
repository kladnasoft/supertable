# route: supertable.storage.storage_interface
import abc
import base64
import binascii
import hashlib
import os
import posixpath
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.utils.diagnostic_redaction import safe_exception_type


# Decoding one physical row at a time is necessary to make the byte budget
# skew-safe, but retaining one Python ``RecordBatch`` wrapper per very narrow
# row until that budget fills can itself consume unbounded metadata memory.
# Bound both dimensions independently.  The row cap also protects this helper
# if the decoder batch size is increased in the future.
PARQUET_DECODE_MAX_PENDING_BATCHES = 4_096
PARQUET_DECODE_MAX_PENDING_ROWS = 65_536


def storage_error_type(error: BaseException) -> str:
    """Return bounded exception-class metadata without rendering its message."""

    return safe_exception_type(error)


def _iter_bounded_parquet_batch_groups(
    batches,
    *,
    max_decoded_bytes: int,
    max_pending_batches: int = PARQUET_DECODE_MAX_PENDING_BATCHES,
    max_pending_rows: int = PARQUET_DECODE_MAX_PENDING_ROWS,
):
    """Group decoded batches under independent byte, row, and object caps.

    A single input batch is indivisible.  Production calls this with one-row
    decoder batches, so only one unusually wide row may exceed the byte cap.
    Keeping the grouping policy separate also makes its metadata bound directly
    testable without allocating a pathological Parquet object.
    """
    byte_limit = max(1, int(max_decoded_bytes))
    batch_limit = max(1, int(max_pending_batches))
    row_limit = max(1, int(max_pending_rows))
    pending: List[pa.RecordBatch] = []
    pending_bytes = 0
    pending_rows = 0

    for decoded_batch in batches:
        decoded_bytes = max(1, int(decoded_batch.nbytes))
        decoded_rows = max(0, int(decoded_batch.num_rows))
        would_exceed = pending and (
            pending_bytes + decoded_bytes > byte_limit
            or pending_rows + decoded_rows > row_limit
            or len(pending) >= batch_limit
        )
        if would_exceed:
            yield pending
            pending = []
            pending_bytes = 0
            pending_rows = 0

        if decoded_rows > row_limit:
            raise RuntimeError(
                "Parquet decoder emitted a batch larger than the pending-row "
                f"cap ({decoded_rows} > {row_limit})"
            )
        if decoded_bytes > byte_limit:
            # Production decoder batches contain one row. Do not retain any
            # other batch beside an indivisible oversize value.
            yield [decoded_batch]
            continue

        pending.append(decoded_batch)
        pending_bytes += decoded_bytes
        pending_rows += decoded_rows

    if pending:
        yield pending


def write_all(file_obj: BinaryIO, data: bytes | memoryview) -> int:
    """Write an entire chunk, including to sinks which perform partial writes."""
    view = memoryview(data)
    written = 0
    while written < len(view):
        result = file_obj.write(view[written:])
        if result is None:
            return len(view)
        if result <= 0:
            raise OSError("Binary sink made no progress while writing")
        written += int(result)
    return written


def normalize_sha256_checksum(value: Any) -> str:
    """Normalize a hex or base64 SHA-256 digest to lowercase hexadecimal."""
    text = str(value or "").strip()
    if len(text) == 64:
        try:
            bytes.fromhex(text)
            return text.lower()
        except ValueError:
            return ""
    try:
        decoded = base64.b64decode(text, validate=True)
    except (ValueError, TypeError, binascii.Error):
        return ""
    return decoded.hex() if len(decoded) == 32 else ""


@dataclass(frozen=True)
class ObjectMetadata:
    """Stable object attributes used to seal entries in the local file cache."""

    size: int
    version: str = ""
    etag: str = ""
    last_modified_ns: int = 0
    checksum_sha256: str = ""

    def identity_token(self) -> str | None:
        """Return a deterministic token for this exact observed object version.

        Size alone is deliberately not considered an identity.  Providers should
        populate every stable version/checksum attribute they expose; combining
        them makes an in-place local rewrite (same inode) detectable as well.
        """
        seals = []
        if self.version:
            seals.append(f"version={self.version}")
        if self.etag:
            seals.append(f"etag={self.etag}")
        if self.last_modified_ns:
            seals.append(f"mtime_ns={self.last_modified_ns}")
        if self.checksum_sha256:
            seals.append(f"sha256={self.checksum_sha256}")
        if not seals:
            return None
        return f"size={self.size}|" + "|".join(seals)


class ObjectIdentityMismatch(OSError):
    """A conditional object read did not address the sealed object version.

    Callers must treat this differently from cache or network availability:
    retrying against the current unconditioned object could mix two snapshots.
    """


def validate_range_request(
    offset: int,
    length: int,
    expected: ObjectMetadata | None,
) -> tuple[int, int]:
    """Validate and normalize an exact half-open object byte range."""
    try:
        offset = int(offset)
        length = int(length)
    except (TypeError, ValueError):
        raise ValueError("range offset and length must be integers") from None
    if offset < 0 or length < 0:
        raise ValueError("range offset and length must be non-negative")
    if expected is not None:
        if expected.size < 0 or offset > expected.size or length > expected.size - offset:
            raise ValueError("requested range exceeds the sealed object size")
        if length and not expected.identity_token():
            raise ValueError("range reads require a stable object identity seal")
    return offset, length


def read_exact_range_body(body: BinaryIO, length: int) -> bytes:
    """Read one bounded provider response exactly, detecting ignored ranges.

    Some HTTP wrappers may legally return fewer bytes than requested from one
    ``read`` call.  Looping avoids false short-read failures, while the final
    one-byte probe detects a provider/adapter that ignored the Range header
    without ever draining the rest of the object.
    """
    remaining = int(length)
    chunks = []
    while remaining:
        chunk = body.read(remaining)
        if not chunk:
            break
        if len(chunk) > remaining:
            raise ObjectIdentityMismatch("range response exceeded requested length")
        chunks.append(chunk)
        remaining -= len(chunk)
    extra = body.read(1)
    if remaining or extra:
        raise ObjectIdentityMismatch("range response length mismatch")
    return b"".join(chunks)


class StorageInterface(abc.ABC):
    """
    Abstract base class for a storage interface that can handle both local and
    cloud/object storage in a unified manner.
    """

    base_prefix: str = ""

    def _with_base(self, path: str) -> str:
        """Translate one public logical path to a provider object key.

        Public storage methods accept logical paths only.  Provider adapters
        call this helper exactly once at their boundary; values returned by
        public listing methods must be translated back with
        :meth:`_without_base` before they escape the adapter.
        """
        path = path.strip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{path}" if path else self.base_prefix
        return path

    def _without_base(self, path: str) -> str:
        """Translate a provider key back to the public logical namespace."""
        physical = str(path or "").strip("/")
        base = str(self.base_prefix or "").strip("/")
        if not base:
            return physical
        if physical == base:
            return ""
        prefix = f"{base}/"
        if not physical.startswith(prefix):
            raise ValueError("Provider key is outside configured base prefix")
        return physical[len(prefix):]


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
        Return logical paths found in `path` matching the given pattern.

        Both the input and every returned value are in the logical namespace;
        a configured cloud ``base_prefix`` is never exposed to callers.
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

        A successful return acknowledges durable disappearance: callers may
        immediately finalize a catalog tombstone or recreate the same logical
        name without an old local object reappearing after a host crash.
        """
        pass

    def delete_prefix(self, path: str) -> None:
        """Delete a complete logical prefix and verify that it is empty.

        Object-store implementations override this with provider-native,
        retried batch deletion.  The compatibility implementation is suitable
        for local and third-party hierarchical backends whose ``delete`` is
        already recursive.  Missing prefixes make retries idempotent.

        A successful return acknowledges durable disappearance for backends
        whose namespace is hosted on the local filesystem.
        """
        path = self._require_nonempty_delete_prefix(path)
        try:
            self.delete(path)
        except FileNotFoundError:
            pass
        remaining = self.list_files(path, "*")
        if remaining:
            raise OSError("Storage prefix is not empty after deletion")

    @staticmethod
    def _require_nonempty_delete_prefix(path: str) -> str:
        """Normalize dot segments and reject logical-root aliases."""
        if not isinstance(path, str):
            raise ValueError("Storage deletion prefix must be a string")
        candidate = path.strip().replace("\\", "/")
        normalized = posixpath.normpath(candidate)
        if (
            normalized in ("", ".", "/", "..")
            or normalized.startswith("../")
            or not normalized.lstrip("/")
        ):
            raise ValueError("Refusing to delete an empty storage prefix")
        return normalized

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

        A successful return acknowledges a complete, durable object. Snapshot
        and staging publishers may make the path visible in Redis immediately
        afterward, so an implementation must not expose or acknowledge a
        partial local file.
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
        reader, including an empty list when none of the requested columns exist;
        only ``None`` means read every column.
        """
        if columns is None:
            return None
        return [c for c in columns if c in set(available)]

    # -------------------------
    # Byte / text / copy operations
    # -------------------------
    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes) -> None:
        """
        Writes raw bytes to the given path.
        Creates parent directories/prefixes as needed.

        A successful return acknowledges complete, durable bytes. Core Parquet
        publication uses this exact-byte path when a backend supports it and
        may publish the object in the catalog immediately afterward.
        """
        pass

    def create_bytes_if_absent(self, path: str, data: bytes) -> bool:
        """Atomically create one immutable byte object without overwriting.

        Return ``True`` only when this call created *path* and durably
        acknowledged both the exact bytes and their namespace entry.  Return
        ``False`` only when the target already existed.  Authentication,
        transport, timeout, and otherwise ambiguous failures must be raised so
        the caller can reconcile them with a sealed read.

        The method is deliberately non-abstract for compatibility with
        third-party storage adapters, but there is no check-then-write fallback:
        adapters used for immutable publication must implement a provider-side
        create precondition.
        """
        raise NotImplementedError(
            "Storage adapter does not implement create_bytes_if_absent()"
        )

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

    def canonical_uri(self, path: str) -> str:
        """Return the backend-owned canonical URI for one logical path.

        Local storage inherits this implementation.  Cloud backends override
        it so table formats never have to guess a provider from incidental
        attributes such as ``bucket``.
        """
        value = str(path or "")
        if "://" in value:
            return value
        is_local = getattr(self, "is_local_storage", None)
        if callable(is_local) and bool(is_local()):
            return Path(os.path.abspath(value)).as_uri()
        raise NotImplementedError(
            "Storage adapter does not implement canonical_uri()"
        )

    def content_sha256(self, path: str) -> tuple[int, str]:
        """Return an exact size/SHA-256 seal for one logical object.

        Built-in backends download under their strongest available immutable
        version/ETag condition into a bounded disk spill. This is intentionally
        stronger than trusting multipart ETags or provider copy responses: a
        mirror publication may only be acknowledged after its visible bytes
        match the committed source artifact.
        """
        metadata = self.stat_object(path)
        digest = hashlib.sha256()
        try:
            with tempfile.TemporaryFile(prefix="supertable-seal-") as spill:
                written = self.download_to_file(
                    path, spill, expected=metadata,
                )
                if int(written) != int(metadata.size):
                    raise OSError("Short sealed read: object size mismatch")
                spill.seek(0)
                while True:
                    chunk = spill.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
        except NotImplementedError:
            # Compatibility path for third-party adapters that predate the
            # bounded download API. Built-in production adapters never use it.
            payload = self.read_bytes(path)
            if len(payload) != int(metadata.size):
                raise OSError(
                    "Short sealed read: object size mismatch"
                ) from None
            digest.update(payload)
        return int(metadata.size), digest.hexdigest()

    def iter_parquet_batches(
        self,
        path: str,
        *,
        max_decoded_bytes: int,
        columns: Optional[List[str]] = None,
    ):
        """Yield bounded Arrow batches from a logical Parquet object.

        Remote objects are streamed to a temporary spill file under an exact
        version seal, then decoded batch-by-batch.  This avoids materialising a
        compressed object and its decoded table in memory at the same time.
        One unusually wide row remains the indivisible lower bound.
        """
        budget = max(1, int(max_decoded_bytes))
        metadata = self.stat_object(path)
        fd, spill_path = tempfile.mkstemp(prefix="supertable-compact-", suffix=".parquet")
        try:
            with os.fdopen(fd, "wb") as spill:
                self.download_to_file(path, spill, expected=metadata)
                spill.flush()
                os.fsync(spill.fileno())

            parquet = pq.ParquetFile(spill_path)
            # Whole-file averages cannot bound an arbitrarily skewed cluster
            # of variable-width rows. Decode one physical row at a time (the
            # indivisible lower bound), then group already-measured batches
            # into a zero-copy chunked Arrow table no larger than the budget.
            # This avoids ever materialising a speculative multi-row parent
            # that can exceed the limit by N times before it is sliced.
            decoded_batches = parquet.iter_batches(
                batch_size=1,
                columns=columns,
                use_threads=False,
            )
            for pending in _iter_bounded_parquet_batch_groups(
                decoded_batches,
                max_decoded_bytes=budget,
            ):
                if len(pending) == 1 and pending[0].nbytes > budget:
                    yield pending[0]
                else:
                    yield pa.Table.from_batches(pending)
        finally:
            try:
                os.remove(spill_path)
            except FileNotFoundError:
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
        raise NotImplementedError(
            "Storage adapter does not implement to_duckdb_path()"
        )

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        """
        Return a presigned GET URL for the object.
        """
        raise NotImplementedError(
            "Storage adapter does not implement presign()"
        )

    @staticmethod
    def _require_presign_object_key(key: object) -> str:
        """Reject external URLs at every built-in credential-issuer boundary."""
        if (
            not isinstance(key, str)
            or not key
            or "\x00" in key
            or "://" in key
        ):
            raise ValueError("presign requires a non-empty storage object key")
        return key

    # -------------------------
    # Optional shared-file-cache helpers
    # -------------------------
    def stat_object(self, path: str) -> ObjectMetadata:
        """Return cache-relevant metadata without downloading the object.

        This method intentionally remains non-abstract so third-party storage
        implementations continue to instantiate.  Built-in backends override it
        to expose their native immutable version and checksum attributes.
        """
        return ObjectMetadata(size=self.size(path))

    def download_to_file(
        self,
        path: str,
        file_obj: BinaryIO,
        *,
        expected: ObjectMetadata | None = None,
        chunk_size: int = 8 * 1024 * 1024,
    ) -> int:
        """Download *path* into a binary sink and return bytes written.

        This remains non-abstract for third-party subclass compatibility, but
        deliberately has no whole-object ``read_bytes`` fallback.  Backends
        admitted to the shared cache must implement true bounded streaming.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        raise NotImplementedError(
            "Storage adapter does not implement streaming download_to_file()"
        )

    def read_range(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> bytes:
        """Read exactly ``[offset, offset + length)`` without fetching the object.

        Built-in object-store implementations use their provider's bounded
        range API and apply the strongest available version/ETag condition from
        ``expected``.  There is deliberately no ``read_bytes`` fallback: that
        would turn a narrow Parquet footer/column read into a full download.
        """
        validate_range_request(offset, length, expected)
        raise NotImplementedError(
            "Storage adapter does not implement bounded read_range()"
        )

    def cache_namespace(self) -> Dict[str, str]:
        """Return non-secret, non-URL fields that isolate cache key spaces."""
        namespace = {
            "provider": f"{self.__class__.__module__}.{self.__class__.__qualname__}",
        }
        base_prefix = str(getattr(self, "base_prefix", "") or "")
        if base_prefix:
            namespace["base_prefix"] = base_prefix.strip("/")
        return namespace

    def is_local_storage(self) -> bool:
        """Whether paths already refer to files on the local filesystem."""
        return False
