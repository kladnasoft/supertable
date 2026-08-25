"""
Thorough test suite for the storage layer:
  - storage_interface.py   (abstract base + default method coverage)
  - local_storage.py       (every method, every branch)
  - minio_storage.py       (every method, every branch, fully mocked)
  - s3_storage.py          (every method, every branch, fully mocked)
  - storage_factory.py     (every backend path, env fallback, errors)

All external dependencies (pyarrow, minio, boto3) are mocked so the suite
runs with only the Python stdlib + pytest.
"""
import io
import json
import multiprocessing
import os
import shutil
import sys
import tempfile
import threading
import traceback
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch, call

# ---------------------------------------------------------------------------
# Pre-import packages that supertable.* transitively depends on (pandas pulls
# in pyarrow at import time and reads pa.__version__). Doing this BEFORE the
# stub bootstrap guarantees we don't shadow real packages with our test stubs.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - environmental
    import pandas  # noqa: F401
except Exception:
    pass

# ---------------------------------------------------------------------------
# Bootstrap: create stub modules so imports don't fail without real packages
# ---------------------------------------------------------------------------

def _ensure_stub(name, attrs=None):
    """Insert a stub module into sys.modules if it isn't already importable.

    Also gives the stub a non-None ``__spec__`` so that
    :func:`importlib.util.find_spec` (used by ``storage_factory._require``)
    treats it as an installed package rather than raising ``ValueError``.
    """
    import importlib.machinery as _machinery

    if name in sys.modules:
        return sys.modules[name]
    mod = types.ModuleType(name)
    mod.__spec__ = _machinery.ModuleSpec(name, loader=None)
    if attrs:
        for k, v in attrs.items():
            setattr(mod, k, v)
    sys.modules[name] = mod
    # ensure parent packages exist
    parts = name.split(".")
    for i in range(1, len(parts)):
        parent = ".".join(parts[:i])
        if parent not in sys.modules:
            pmod = types.ModuleType(parent)
            pmod.__spec__ = _machinery.ModuleSpec(parent, loader=None)
            sys.modules[parent] = pmod
    return mod


# ---------------------------------------------------------------------------
# Snapshot the pre-bootstrap state of every module name / attribute the minio &
# boto3 stubs below create or overwrite. ``tearDownModule`` uses this to undo the
# process-global mutations so LATER test modules in the same interpreter import
# the REAL minio/boto3 packages. (Without this, the fake ``minio.Minio`` leaks
# into ``supertable.storage.minio_storage`` and breaks every later test that
# builds a real storage backend via ``get_storage()``.)
# ---------------------------------------------------------------------------
_STUBBED_MODULE_NAMES = (
    "minio", "minio.commonconfig", "minio.deleteobjects", "minio.error",
    "boto3", "botocore", "botocore.config", "botocore.exceptions",
)
_ORIGINAL_SYS_MODULES = {n: sys.modules.get(n) for n in _STUBBED_MODULE_NAMES}
# The one *existing* attribute we overwrite is ``minio.Minio`` (set below).
_pre_minio_mod = sys.modules.get("minio")
_ORIGINAL_MINIO_MINIO = (
    (getattr(_pre_minio_mod, "Minio", None), hasattr(_pre_minio_mod, "Minio"))
    if _pre_minio_mod is not None else (None, False)
)
# Production modules that bind the stubbed packages via ``from X import Y`` at
# import time; they must be dropped so they re-import the real packages.
_PRODUCTION_MODULES_TO_REFRESH = (
    "supertable.storage.minio_storage",
    "supertable.storage.s3_storage",
)


# --- pyarrow stubs ---
# Prefer the REAL pyarrow.parquet whenever it's importable; only fall back to a
# no-op stub on a stdlib-only run. This is essential: ~8 OTHER test modules do
# ``import pyarrow.parquet as pq`` at module scope, and pytest imports every
# module during collection — long before this module's ``tearDownModule`` runs.
# A leaked no-op stub would strip ``pq.read_metadata`` / real ``write_table``
# from all of them. The storage tests here never rely on the stub: they patch
# ``...storage.X.pq`` locally where needed.
_pa = _ensure_stub("pyarrow", {
    "Table": type("Table", (), {}),
    "table": lambda data, names=None: MagicMock(spec=[]),
})
try:
    import pyarrow.parquet as _pq  # real package when pyarrow is installed
except Exception:
    _pq = _ensure_stub("pyarrow.parquet", {
        "write_table": MagicMock(),
        "read_table": MagicMock(return_value=MagicMock()),
    })

# --- minio stubs ---
_minio_pkg = _ensure_stub("minio", {"__version__": "7.0.0"})
_Minio_cls = type("Minio", (), {"__init__": lambda self, **kw: None})
_minio_pkg.Minio = _Minio_cls

_minio_commonconfig = _ensure_stub("minio.commonconfig", {
    "CopySource": type("CopySource", (), {"__init__": lambda self, *a, **kw: None}),
})
_minio_deleteobjects = _ensure_stub("minio.deleteobjects", {
    "DeleteObject": type("DeleteObject", (), {"__init__": lambda self, name: setattr(self, "name", name)}),
})


class _FakeS3Error(Exception):
    def __init__(self, code="NoSuchKey", message=""):
        self.code = code
        self.message = message
        super().__init__(message)

_minio_error = _ensure_stub("minio.error", {"S3Error": _FakeS3Error})

# --- boto3 / botocore stubs ---
_botocore = _ensure_stub("botocore")
_botocore_config = _ensure_stub("botocore.config", {
    "Config": type("Config", (), {"__init__": lambda self, **kw: None}),
})


class _BootstrapClientError(Exception):
    """Minimal botocore stand-in used only while importing production code."""

    def __init__(self, error_response=None, operation_name="op"):
        self.response = error_response or {"Error": {}}
        self.operation_name = operation_name
        super().__init__(str(self.response))


_botocore_exceptions = _ensure_stub(
    "botocore.exceptions", {"ClientError": _BootstrapClientError},
)

_boto3 = _ensure_stub("boto3", {"client": MagicMock()})

# --- supertable config stubs ---
# The real supertable package is on disk, so we only ensure the config modules
# have the expected attributes (they already do via our local files).
# We do NOT stub supertable or supertable.config or supertable.storage.

# NOTE: do NOT stub supertable.storage — the real package is on disk

# NOW we can safely import the production code
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    StorageInterface,
)
from supertable.storage.local_storage import LocalStorage
from supertable.storage.minio_storage import MinioStorage
from supertable.storage.s3_storage import S3Storage
from supertable.storage.storage_factory import get_storage, _require
from supertable.config.defaults import default
from supertable.config.settings import settings as _settings
from supertable.storage import minio_storage as _minio_module
from supertable.storage import s3_storage as _s3_module
from supertable.storage import storage_factory as _factory_module
from supertable.tests.fork_semantics_probe import run_fork_probe
from dataclasses import replace as _dc_replace


def _publish_create_bytes_if_absent(root, index, start, outcomes):
    """Spawn-safe worker for the cross-process immutable-create race."""

    storage = LocalStorage(root)
    start.wait()
    try:
        created = storage.create_bytes_if_absent(
            "process-race/proof.json",
            f"proof-{index}".encode("ascii"),
        )
        outcomes.put((index, created, None))
    except BaseException as exc:
        outcomes.put((index, None, type(exc).__name__))


class _FakeClientError(_s3_module.ClientError):
    """ClientError double matching the class production actually imported.

    The full test collection can import ``s3_storage`` before this module.  In
    that order production correctly retains the real botocore ``ClientError``;
    an unrelated ``Exception`` fake would bypass every typed handler.  Derive
    from the exact bound class so both real-SDK and bootstrap-stub orders test
    the same exception boundary.
    """

    def __init__(self, error_response=None, operation_name="op"):
        self.response = error_response or {"Error": {}}
        self.operation_name = operation_name
        Exception.__init__(self, str(self.response))


def _patch_settings(test_case, **overrides):
    """Substitute the per-module ``settings`` binding for the duration of
    the test. ``settings`` is a frozen dataclass so we use dataclasses.replace
    to derive a copy with the overrides applied, then patch every module
    that imported it via ``from supertable.config.settings import settings``.

    Restoration is registered with ``addCleanup`` so each test gets isolation.
    """
    new_settings = _dc_replace(_settings, **overrides)
    for mod in (_minio_module, _s3_module, _factory_module):
        original = getattr(mod, "settings")
        setattr(mod, "settings", new_settings)
        test_case.addCleanup(setattr, mod, "settings", original)
    return new_settings


# ═══════════════════════════════════════════════════════════════════════════
#  STORAGE INTERFACE (abstract base class + default methods)
# ═══════════════════════════════════════════════════════════════════════════

class TestStorageInterface(unittest.TestCase):

    def test_cannot_instantiate_directly(self):
        with self.assertRaises(TypeError):
            StorageInterface()

    def test_to_duckdb_path_raises_not_implemented(self):
        """Default implementation of to_duckdb_path raises NotImplementedError."""
        # Create a minimal concrete subclass just for this test
        class Dummy(StorageInterface):
            def read_json(self, path): ...
            def write_json(self, path, data): ...
            def exists(self, path): ...
            def size(self, path): ...
            def makedirs(self, path): ...
            def list_files(self, path, pattern="*"): ...
            def delete(self, path): ...
            def get_directory_structure(self, path): ...
            def write_parquet(self, table, path): ...
            def read_parquet(self, path): ...
            def write_bytes(self, path, data): ...
            def read_bytes(self, path): ...
            def write_text(self, path, text, encoding="utf-8"): ...
            def read_text(self, path, encoding="utf-8"): ...
            def copy(self, src_path, dst_path): ...

        d = Dummy()
        with self.assertRaises(NotImplementedError):
            d.to_duckdb_path("key")

    def test_presign_raises_not_implemented(self):
        class Dummy(StorageInterface):
            def read_json(self, path): ...
            def write_json(self, path, data): ...
            def exists(self, path): ...
            def size(self, path): ...
            def makedirs(self, path): ...
            def list_files(self, path, pattern="*"): ...
            def delete(self, path): ...
            def get_directory_structure(self, path): ...
            def write_parquet(self, table, path): ...
            def read_parquet(self, path): ...
            def write_bytes(self, path, data): ...
            def read_bytes(self, path): ...
            def write_text(self, path, text, encoding="utf-8"): ...
            def read_text(self, path, encoding="utf-8"): ...
            def copy(self, src_path, dst_path): ...

        d = Dummy()
        with self.assertRaises(NotImplementedError):
            d.presign("key")

    def test_create_bytes_if_absent_has_no_unsafe_default_fallback(self):
        class Dummy(StorageInterface):
            def read_json(self, path): ...
            def write_json(self, path, data): ...
            def exists(self, path): ...
            def size(self, path): ...
            def makedirs(self, path): ...
            def list_files(self, path, pattern="*"): ...
            def delete(self, path): ...
            def get_directory_structure(self, path): ...
            def write_parquet(self, table, path): ...
            def read_parquet(self, path): ...
            def write_bytes(self, path, data): ...
            def read_bytes(self, path): ...
            def write_text(self, path, text, encoding="utf-8"): ...
            def read_text(self, path, encoding="utf-8"): ...
            def copy(self, src_path, dst_path): ...

        with self.assertRaises(NotImplementedError):
            Dummy().create_bytes_if_absent("proof.json", b"proof")


# ═══════════════════════════════════════════════════════════════════════════
#  LOCAL STORAGE
# ═══════════════════════════════════════════════════════════════════════════

class TestLocalStorage(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="test_local_storage_")
        self.storage = LocalStorage(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _path(self, name: str) -> str:
        return os.path.join(self.tmpdir, name)

    # ---- JSON ----

    def test_write_and_read_json(self):
        p = self._path("data.json")
        payload = {"hello": "world", "num": 42}
        self.storage.write_json(p, payload)
        result = self.storage.read_json(p)
        self.assertEqual(result, payload)

    def test_write_json_creates_parent_dirs(self):
        p = self._path("deep/nested/dir/data.json")
        self.storage.write_json(p, {"a": 1})
        self.assertTrue(os.path.isfile(p))

    def test_write_json_overwrites_existing(self):
        p = self._path("overwrite.json")
        self.storage.write_json(p, {"v": 1})
        self.storage.write_json(p, {"v": 2})
        self.assertEqual(self.storage.read_json(p), {"v": 2})

    def test_read_json_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.read_json(self._path("nope.json"))

    def test_read_json_empty_file(self):
        p = self._path("empty.json")
        with open(p, "w") as f:
            pass  # empty
        with self.assertRaises(ValueError) as ctx:
            self.storage.read_json(p)
        self.assertIn("empty", str(ctx.exception).lower())

    def test_read_json_invalid_json(self):
        p = self._path("bad.json")
        with open(p, "w") as f:
            f.write("{invalid json!!")
        with self.assertRaises(ValueError) as ctx:
            self.storage.read_json(p)
        self.assertIn("Invalid JSON", str(ctx.exception))

    def test_json_and_path_validation_errors_do_not_reflect_secret_paths(self):
        secret = "tenant-path-token-DO-NOT-LOG"
        p = self._path(f"{secret}.json")
        Path(p).write_text("{invalid", encoding="utf-8")

        with self.assertRaises(ValueError) as invalid_json:
            self.storage.read_json(p)
        rendered = "".join(
            traceback.format_exception(
                type(invalid_json.exception),
                invalid_json.exception,
                invalid_json.exception.__traceback__,
            )
        )
        self.assertNotIn(secret, rendered)

        rooted = LocalStorage(root=self.tmpdir)
        with self.assertRaises(ValueError) as escaped:
            rooted.read_bytes(f"../{secret}.json")
        self.assertNotIn(secret, str(escaped.exception))

    def test_read_json_unicode(self):
        p = self._path("unicode.json")
        payload = {"emoji": "🎉", "jp": "日本語"}
        self.storage.write_json(p, payload)
        self.assertEqual(self.storage.read_json(p), payload)

    # ---- exists ----

    def test_exists_true_for_file(self):
        p = self._path("exists.txt")
        with open(p, "w") as f:
            f.write("hi")
        self.assertTrue(self.storage.exists(p))

    def test_exists_true_for_directory(self):
        d = self._path("somedir")
        os.makedirs(d)
        self.assertTrue(self.storage.exists(d))

    def test_exists_false(self):
        self.assertFalse(self.storage.exists(self._path("ghost")))

    # ---- size ----

    def test_size_returns_correct_bytes(self):
        p = self._path("sized.bin")
        data = b"A" * 1234
        with open(p, "wb") as f:
            f.write(data)
        self.assertEqual(self.storage.size(p), 1234)

    def test_size_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.size(self._path("nope.bin"))

    def test_size_directory_raises(self):
        d = self._path("adir")
        os.makedirs(d)
        with self.assertRaises(FileNotFoundError):
            self.storage.size(d)

    # ---- makedirs ----

    def test_makedirs_creates_nested(self):
        d = self._path("a/b/c")
        self.storage.makedirs(d)
        self.assertTrue(os.path.isdir(d))

    def test_makedirs_existing_noop(self):
        d = self._path("already")
        os.makedirs(d)
        self.storage.makedirs(d)  # should not raise
        self.assertTrue(os.path.isdir(d))

    # ---- list_files ----

    def test_list_files_all(self):
        for name in ["a.txt", "b.json", "c.parquet"]:
            with open(self._path(name), "w") as f:
                f.write("x")
        result = self.storage.list_files(self.tmpdir)
        basenames = [os.path.basename(p) for p in result]
        self.assertIn("a.txt", basenames)
        self.assertIn("b.json", basenames)
        self.assertIn("c.parquet", basenames)

    def test_list_files_with_pattern(self):
        for name in ["a.txt", "b.json", "c.txt"]:
            with open(self._path(name), "w") as f:
                f.write("x")
        result = self.storage.list_files(self.tmpdir, pattern="*.txt")
        basenames = [os.path.basename(p) for p in result]
        self.assertIn("a.txt", basenames)
        self.assertIn("c.txt", basenames)
        self.assertNotIn("b.json", basenames)

    def test_list_files_pattern_cannot_escape_logical_directory(self):
        storage = LocalStorage(self.tmpdir)
        parent = os.path.dirname(self.tmpdir)
        fd, outside = tempfile.mkstemp(prefix="outside-listing-", dir=parent)
        os.close(fd)
        try:
            self.assertEqual(storage.list_files("", pattern="../*"), [])
            self.assertEqual(storage.list_files("", pattern=f"{parent}/*"), [])
        finally:
            os.remove(outside)

    def test_list_files_nonexistent_dir(self):
        result = self.storage.list_files(self._path("nope"))
        self.assertEqual(result, [])

    def test_list_files_empty_dir(self):
        d = self._path("emptydir")
        os.makedirs(d)
        result = self.storage.list_files(d)
        self.assertEqual(result, [])

    # ---- delete ----

    def test_delete_file(self):
        p = self._path("todelete.txt")
        with open(p, "w") as f:
            f.write("bye")
        self.storage.delete(p)
        self.assertFalse(os.path.exists(p))

    def test_delete_directory(self):
        d = self._path("deldir")
        os.makedirs(os.path.join(d, "sub"))
        with open(os.path.join(d, "sub", "f.txt"), "w") as f:
            f.write("x")
        self.storage.delete(d)
        self.assertFalse(os.path.exists(d))

    def test_delete_symlink(self):
        target = self._path("target.txt")
        with open(target, "w") as f:
            f.write("real")
        link = self._path("link.txt")
        os.symlink(target, link)
        self.storage.delete(link)
        self.assertFalse(os.path.exists(link))
        self.assertTrue(os.path.exists(target))  # target untouched

    def test_delete_relative_symlink_does_not_delete_file_target(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("target-relative.txt")
        with open(target, "w") as stream:
            stream.write("real")
        link = self._path("link-relative.txt")
        os.symlink(target, link)

        storage.delete("link-relative.txt")

        self.assertFalse(os.path.lexists(link))
        self.assertTrue(os.path.isfile(target))

    def test_delete_relative_directory_symlink_does_not_recurse_into_target(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("target-directory")
        os.makedirs(target)
        with open(os.path.join(target, "keep.txt"), "w") as stream:
            stream.write("keep")
        link = self._path("link-directory")
        os.symlink(target, link)

        storage.delete("link-directory")

        self.assertFalse(os.path.lexists(link))
        self.assertTrue(os.path.isfile(os.path.join(target, "keep.txt")))

    def test_delete_rejects_relative_traversal_before_destructive_io(self):
        storage = LocalStorage(self.tmpdir)
        aliases = (
            "..",
            "child/../..",
            "inside/../../sibling",
            "inside/.",
            r"..\sibling",
            r"inside\..\sibling",
        )
        with (
            patch("supertable.storage.local_storage.shutil.rmtree") as rmtree,
            patch("supertable.storage.local_storage.os.remove") as remove,
        ):
            for alias in aliases:
                with self.subTest(alias=alias):
                    with self.assertRaisesRegex(ValueError, "Refusing to delete"):
                        storage.delete(alias)
            rmtree.assert_not_called()
            remove.assert_not_called()

    def test_delete_prefix_rejects_traversal_before_calling_delete(self):
        storage = LocalStorage(self.tmpdir)
        aliases = (
            "..",
            "child/../target",
            "inside/.",
            r"..\target",
            r"inside\..\target",
        )
        with patch.object(storage, "delete") as delete:
            for alias in aliases:
                with self.subTest(alias=alias):
                    with self.assertRaisesRegex(ValueError, "Refusing to delete"):
                        storage.delete_prefix(alias)
            delete.assert_not_called()

    def test_delete_prefix_unlinks_external_directory_symlink_without_recursing(self):
        storage = LocalStorage(self.tmpdir)
        with tempfile.TemporaryDirectory(prefix="outside-storage-") as outside:
            target = os.path.join(outside, "target")
            os.makedirs(target)
            kept = os.path.join(target, "keep.txt")
            with open(kept, "w") as stream:
                stream.write("keep")
            link = self._path("external-prefix")
            os.symlink(target, link)
            file_target = os.path.join(outside, "keep-file.txt")
            with open(file_target, "w") as stream:
                stream.write("keep")
            file_link = self._path("external-file-prefix")
            os.symlink(file_target, file_link)

            storage.delete_prefix("external-prefix")
            storage.delete_prefix("external-file-prefix")

            self.assertFalse(os.path.lexists(link))
            self.assertTrue(os.path.isfile(kept))
            self.assertFalse(os.path.lexists(file_link))
            self.assertTrue(os.path.isfile(file_target))

    def test_delete_rejects_all_absolute_filesystem_root_aliases_without_io(self):
        aliases = [os.sep, os.sep * 2, os.path.join(os.sep, "."), "/tmp/.."]
        with patch("supertable.storage.local_storage.shutil.rmtree") as rmtree, \
             patch("supertable.storage.local_storage.os.remove") as remove:
            for alias in aliases:
                with self.subTest(alias=alias):
                    with self.assertRaisesRegex(ValueError, "filesystem root"):
                        self.storage.delete(alias)
            rmtree.assert_not_called()
            remove.assert_not_called()

    def test_delete_refuses_configured_storage_root(self):
        storage = LocalStorage(self.tmpdir)
        with self.assertRaisesRegex(ValueError, "storage root"):
            storage.delete("")
        self.assertTrue(os.path.isdir(self.tmpdir))

    def test_delete_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.delete(self._path("ghost"))

    def test_delete_file_fsyncs_surviving_parent(self):
        p = self._path("durable-delete.bin")
        with open(p, "wb") as stream:
            stream.write(b"data")
        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda directory: synced.append(os.path.abspath(directory)),
        ):
            self.storage.delete(p)
        self.assertEqual(synced, [self.tmpdir])

    def test_delete_directory_fsyncs_ancestors_but_not_removed_directory(self):
        storage = LocalStorage(self.tmpdir)
        removed = self._path("parent/removed")
        os.makedirs(removed)
        with open(os.path.join(removed, "object"), "wb") as stream:
            stream.write(b"data")
        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda directory: synced.append(os.path.abspath(directory)),
        ):
            storage.delete("parent/removed")
        self.assertEqual(
            synced,
            [self._path("parent"), self.tmpdir],
        )
        self.assertNotIn(removed, synced)

    def test_delete_prefix_retry_resyncs_visible_prior_deletion(self):
        p = self._path("retry-delete")
        os.makedirs(p)
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=OSError("directory fsync failed"),
        ), self.assertRaisesRegex(OSError, "directory fsync failed"):
            self.storage.delete_prefix(p)
        self.assertFalse(os.path.exists(p))

        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda directory: synced.append(os.path.abspath(directory)),
        ):
            self.storage.delete_prefix(p)
        self.assertEqual(synced, [self.tmpdir])

    def test_delete_prefix_retry_with_missing_parent_syncs_surviving_root(self):
        storage = LocalStorage(self.tmpdir)
        os.makedirs(self._path("gone/child"))
        # Model a prior interrupted deletion whose visible namespace change
        # removed both the requested prefix and its immediate parent before
        # the durability acknowledgement was lost.
        shutil.rmtree(self._path("gone"))

        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda directory: synced.append(os.path.abspath(directory)),
        ):
            storage.delete_prefix("gone/child")
        self.assertEqual(synced, [self.tmpdir])

    # ---- get_directory_structure ----

    def test_get_directory_structure_nested(self):
        os.makedirs(self._path("sub1"))
        os.makedirs(self._path("sub2/nested"))
        with open(self._path("sub1/a.txt"), "w") as f:
            f.write("")
        with open(self._path("sub1/b.json"), "w") as f:
            f.write("")
        with open(self._path("sub2/nested/c.parquet"), "w") as f:
            f.write("")
        result = self.storage.get_directory_structure(self.tmpdir)
        self.assertIn("sub1", result)
        self.assertIn("a.txt", result["sub1"])
        self.assertIn("b.json", result["sub1"])
        self.assertIn("sub2", result)
        self.assertIn("nested", result["sub2"])
        self.assertIn("c.parquet", result["sub2"]["nested"])

    def test_get_directory_structure_nonexistent(self):
        result = self.storage.get_directory_structure(self._path("nope"))
        self.assertEqual(result, {})

    def test_get_directory_structure_empty_dir(self):
        d = self._path("empty")
        os.makedirs(d)
        result = self.storage.get_directory_structure(d)
        self.assertEqual(result, {})

    def test_get_directory_structure_flat(self):
        with open(self._path("root.txt"), "w") as f:
            f.write("")
        result = self.storage.get_directory_structure(self.tmpdir)
        self.assertIn("root.txt", result)
        self.assertIsNone(result["root.txt"])

    # ---- parquet ----

    def test_write_and_read_parquet(self):
        p = self._path("data.parquet")
        fake_table = MagicMock()

        with patch("supertable.storage.local_storage.pq") as mock_pq:
            mock_pq.read_table.return_value = fake_table
            self.storage.write_parquet(fake_table, p)
            mock_pq.write_table.assert_called_once()
            self.assertIs(mock_pq.write_table.call_args.args[0], fake_table)
            self.assertNotEqual(mock_pq.write_table.call_args.args[1], p)

        # To read, we need the file to exist
        with open(p, "wb") as f:
            f.write(b"fake parquet data")
        with patch("supertable.storage.local_storage.pq") as mock_pq:
            mock_pq.read_table.return_value = fake_table
            result = self.storage.read_parquet(p)
            self.assertEqual(result, fake_table)

    def test_write_parquet_creates_parent_dirs(self):
        p = self._path("deep/dir/data.parquet")
        with patch("supertable.storage.local_storage.pq"):
            self.storage.write_parquet(MagicMock(), p)
        self.assertTrue(os.path.isdir(os.path.dirname(p)))

    def test_write_parquet_file_fsync_failure_preserves_previous_object(self):
        p = self._path("durable.parquet")

        def encode(value, target):
            with open(target, "wb") as stream:
                stream.write(value)

        with patch(
            "supertable.storage.local_storage.pq.write_table",
            side_effect=encode,
        ):
            self.storage.write_parquet(b"old-complete", p)
            with (
                patch(
                    "supertable.storage.local_storage.os.fsync",
                    side_effect=OSError("file fsync failed"),
                ),
                self.assertRaisesRegex(OSError, "file fsync failed"),
            ):
                self.storage.write_parquet(b"new-complete", p)

        with open(p, "rb") as stream:
            self.assertEqual(stream.read(), b"old-complete")
        self.assertEqual(
            list(Path(self.tmpdir).glob(".tmp-parquet-*")),
            [],
        )

    def test_write_parquet_directory_fsync_failure_is_not_acknowledged(self):
        p = self._path("directory-fsync.parquet")

        def encode(value, target):
            with open(target, "wb") as stream:
                stream.write(value)

        with (
            patch(
                "supertable.storage.local_storage.pq.write_table",
                side_effect=encode,
            ),
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=[None, OSError("directory fsync failed")],
            ),
            self.assertRaisesRegex(OSError, "directory fsync failed"),
        ):
            self.storage.write_parquet(b"complete", p)

        # Rename visibility is not a durability acknowledgement: callers see
        # the raised error and therefore cannot publish a catalog pointer.
        with open(p, "rb") as stream:
            self.assertEqual(stream.read(), b"complete")

    def test_read_parquet_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.read_parquet(self._path("nope.parquet"))

    def test_read_parquet_corrupt_raises_runtime(self):
        p = self._path("corrupt.parquet")
        with open(p, "wb") as f:
            f.write(b"not parquet")
        with patch("supertable.storage.local_storage.pq") as mock_pq:
            mock_pq.read_table.side_effect = Exception("corrupt")
            with self.assertRaises(RuntimeError):
                self.storage.read_parquet(p)

    def test_read_parquet_failure_does_not_expose_path_or_backend_message(self):
        secret = "signed-path-token-DO-NOT-LOG"
        p = self._path(f"{secret}.parquet")
        Path(p).write_bytes(b"not parquet")
        with patch(
            "supertable.storage.local_storage.pq.read_table",
            side_effect=RuntimeError(f"backend-secret-{secret}"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.storage.read_parquet(p)

        rendered = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__,
            )
        )
        self.assertEqual(
            str(ctx.exception),
            "Failed to read Parquet; error_type=RuntimeError",
        )
        self.assertNotIn(secret, rendered)

    # ---- bytes ----

    def test_write_and_read_bytes(self):
        p = self._path("data.bin")
        data = b"\x00\x01\x02\xff"
        self.storage.write_bytes(p, data)
        self.assertEqual(self.storage.read_bytes(p), data)

    def test_write_bytes_creates_parent_dirs(self):
        p = self._path("deep/dir/data.bin")
        self.storage.write_bytes(p, b"x")
        self.assertTrue(os.path.isfile(p))

    def test_write_bytes_partial_temp_failure_preserves_previous_object(self):
        p = self._path("atomic.bin")
        self.storage.write_bytes(p, b"old-complete")

        def partial_then_fail(stream, _data):
            stream.write(b"partial")
            raise OSError("interrupted write")

        with (
            patch(
                "supertable.storage.local_storage.write_all",
                side_effect=partial_then_fail,
            ),
            self.assertRaisesRegex(OSError, "interrupted write"),
        ):
            self.storage.write_bytes(p, b"new-complete")

        self.assertEqual(self.storage.read_bytes(p), b"old-complete")
        self.assertEqual(list(Path(self.tmpdir).glob(".tmp-bytes-*")), [])

    def test_write_bytes_directory_fsync_failure_is_not_acknowledged(self):
        p = self._path("directory-fsync.bin")
        with (
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=[None, OSError("directory fsync failed")],
            ),
            self.assertRaisesRegex(OSError, "directory fsync failed"),
        ):
            self.storage.write_bytes(p, b"complete")
        self.assertEqual(self.storage.read_bytes(p), b"complete")

    def test_create_bytes_if_absent_never_overwrites(self):
        p = self._path("immutable/proof.json")
        self.assertTrue(self.storage.create_bytes_if_absent(p, b"first"))
        self.assertFalse(self.storage.create_bytes_if_absent(p, b"second"))
        self.assertEqual(self.storage.read_bytes(p), b"first")
        self.assertEqual(
            list(Path(p).parent.glob(".tmp-create-bytes-*")),
            [],
        )

    def test_create_bytes_if_absent_has_one_concurrent_winner(self):
        storage = LocalStorage(self.tmpdir)
        barrier = threading.Barrier(8)

        def publish(index):
            barrier.wait()
            created = storage.create_bytes_if_absent(
                "race/proof.json", f"proof-{index}".encode("ascii"),
            )
            return index, created

        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(executor.map(publish, range(8)))

        winners = [index for index, created in outcomes if created]
        self.assertEqual(len(winners), 1)
        self.assertEqual(
            storage.read_bytes("race/proof.json"),
            f"proof-{winners[0]}".encode("ascii"),
        )

    def test_create_bytes_if_absent_has_one_cross_process_winner(self):
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        outcomes = context.Queue()

        processes = [
            context.Process(
                target=_publish_create_bytes_if_absent,
                args=(self.tmpdir, index, start, outcomes),
            )
            for index in range(6)
        ]
        for process in processes:
            process.start()
        try:
            start.set()
            for process in processes:
                process.join(timeout=15)

            self.assertTrue(all(process.exitcode == 0 for process in processes))
            results = [outcomes.get(timeout=5) for _ in processes]
            self.assertTrue(
                all(error is None for _index, _created, error in results),
            )
            winners = [index for index, created, _error in results if created]
            self.assertEqual(len(winners), 1)
            self.assertEqual(
                LocalStorage(self.tmpdir).read_bytes("process-race/proof.json"),
                f"proof-{winners[0]}".encode("ascii"),
            )
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
            for process in processes:
                process.join(timeout=5)
            outcomes.close()
            outcomes.join_thread()

    def test_create_bytes_if_absent_directory_fsync_failure_is_ambiguous(self):
        p = self._path("create-fsync/proof.json")
        with (
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=[None, OSError("directory fsync failed")],
            ),
            self.assertRaisesRegex(OSError, "directory fsync failed"),
        ):
            self.storage.create_bytes_if_absent(p, b"complete")
        # A raised durability acknowledgement is ambiguous by contract: the
        # caller must reconcile the exact visible object rather than retry an
        # overwrite.
        self.assertEqual(self.storage.read_bytes(p), b"complete")

    def test_create_bytes_if_absent_fails_closed_without_unnamed_files(self):
        storage = LocalStorage(self.tmpdir)
        with (
            patch.object(
                storage,
                "_open_immutable_unnamed_file",
                side_effect=NotImplementedError("O_TMPFILE unavailable"),
            ),
            self.assertRaisesRegex(NotImplementedError, "O_TMPFILE unavailable"),
        ):
            storage.create_bytes_if_absent("proofs/day.json", b"proof")

        self.assertFalse(os.path.lexists(self._path("proofs/day.json")))

    def test_create_bytes_if_absent_has_no_staging_path_to_swap(self):
        storage = LocalStorage(self.tmpdir)
        with (
            patch(
                "supertable.storage.local_storage.os.link",
                side_effect=AssertionError("pathname source must not be used"),
            ),
            patch(
                "supertable.storage.local_storage.tempfile.mkstemp",
                side_effect=AssertionError("named staging must not be used"),
            ),
        ):
            self.assertTrue(
                storage.create_bytes_if_absent("proofs/day.json", b"proof")
            )

        self.assertEqual(storage.read_bytes("proofs/day.json"), b"proof")
        self.assertEqual(list(Path(self.tmpdir).rglob(".tmp-create-bytes-*")), [])

    def test_create_bytes_if_absent_destination_race_preserves_winner(self):
        from supertable.storage import local_storage as local_module

        storage = LocalStorage(self.tmpdir)
        outside = self._path("../outside-winner")
        with open(outside, "wb") as stream:
            stream.write(b"existing-winner")
        self.addCleanup(lambda: os.path.lexists(outside) and os.unlink(outside))
        real_link = local_module._linux_link_file_descriptor_no_replace

        def install_winner_before_link(source_fd, directory_fd, target_name):
            os.symlink(outside, target_name, dir_fd=directory_fd)
            return real_link(source_fd, directory_fd, target_name)

        with patch(
            "supertable.storage.local_storage."
            "_linux_link_file_descriptor_no_replace",
            side_effect=install_winner_before_link,
        ):
            self.assertFalse(
                storage.create_bytes_if_absent("race/day.json", b"proof")
            )

        self.assertTrue(os.path.islink(self._path("race/day.json")))
        self.assertEqual(Path(outside).read_bytes(), b"existing-winner")

    def test_create_bytes_if_absent_directory_swap_is_never_acknowledged(self):
        from supertable.storage import local_storage as local_module

        storage = LocalStorage(self.tmpdir)
        outside = tempfile.mkdtemp(
            prefix="outside-proof-directory-",
            dir=os.path.dirname(self.tmpdir),
        )
        displaced = self._path("displaced-proof-directory")
        self.addCleanup(lambda: shutil.rmtree(outside, ignore_errors=True))
        real_link = local_module._linux_link_file_descriptor_no_replace

        def replace_directory_before_link(source_fd, directory_fd, target_name):
            os.rename(self._path("proofs"), displaced)
            os.symlink(outside, self._path("proofs"), target_is_directory=True)
            return real_link(source_fd, directory_fd, target_name)

        with (
            patch(
                "supertable.storage.local_storage."
                "_linux_link_file_descriptor_no_replace",
                side_effect=replace_directory_before_link,
            ),
            self.assertRaisesRegex(
                ObjectIdentityMismatch,
                "directory hierarchy changed",
            ),
        ):
            storage.create_bytes_if_absent("proofs/day.json", b"proof")

        self.assertFalse(os.path.lexists(os.path.join(outside, "day.json")))
        self.assertEqual(Path(displaced, "day.json").read_bytes(), b"proof")
        with self.assertRaisesRegex(ValueError, "escapes configured root"):
            storage.read_bytes("proofs/day.json")

    def test_durability_batch_fsyncs_files_then_each_directory_once(self):
        storage = LocalStorage(self.tmpdir)
        directory = self._path("batch")
        os.makedirs(directory)
        # Establish the directory's ancestry before counting this mutation.
        storage.write_bytes("batch/seed", b"seed")
        calls = []
        real_fsync = os.fsync
        real_fdatasync = os.fdatasync

        def recording_fsync(fd):
            calls.append("directory")
            return real_fsync(fd)

        def recording_fdatasync(fd):
            calls.append("file")
            return real_fdatasync(fd)

        with (
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=recording_fsync,
            ),
            patch(
                "supertable.storage.local_storage.os.fdatasync",
                side_effect=recording_fdatasync,
            ),
        ):
            with storage.durability_batch() as batch:
                storage.write_bytes("batch/a", b"a")
                storage.write_bytes("batch/b", b"b")
                storage.write_bytes("batch/c", b"c")
                batch.barrier()
                batch.catalog_commit_started()
                batch.catalog_commit_succeeded()

        self.assertEqual(calls.count("file"), 3)
        self.assertEqual(calls.count("directory"), 1)
        self.assertLess(
            max(i for i, value in enumerate(calls) if value == "file"),
            calls.index("directory"),
            "every exact inode must be durable before its directory entry",
        )

    def test_durability_batch_file_sync_failure_rolls_back_before_catalog(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("batch-file-fsync")
        with (
            patch(
                "supertable.storage.local_storage.os.fdatasync",
                side_effect=OSError("file fsync crash"),
            ),
            self.assertRaisesRegex(OSError, "file fsync crash"),
        ):
            with storage.durability_batch() as batch:
                storage.write_bytes("batch-file-fsync", b"payload")
                # The final path is intentionally available to metadata and
                # compaction work, but no catalog transaction can start until
                # this exact inode's failed sync is observed at the barrier.
                self.assertEqual(Path(target).read_bytes(), b"payload")
                batch.barrier()
        self.assertFalse(os.path.lexists(target))

    def test_durability_batch_keeps_final_path_readable_while_file_sync_runs(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("batch-readable")
        started = threading.Event()
        release = threading.Event()
        real_fdatasync = os.fdatasync

        def blocked_fdatasync(fd):
            started.set()
            self.assertTrue(release.wait(timeout=5))
            return real_fdatasync(fd)

        with patch(
            "supertable.storage.local_storage.os.fdatasync",
            side_effect=blocked_fdatasync,
        ):
            with storage.durability_batch() as batch:
                storage.write_bytes("batch-readable", b"payload")
                self.assertTrue(started.wait(timeout=5))
                self.assertEqual(Path(target).read_bytes(), b"payload")
                release.set()
                batch.barrier()
                batch.catalog_commit_started()
                batch.catalog_commit_succeeded()

    def test_durability_batch_rename_failure_leaves_no_target(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("batch-rename")
        with (
            patch(
                "supertable.storage.local_storage.os.replace",
                side_effect=OSError("rename crash"),
            ),
            self.assertRaisesRegex(OSError, "rename crash"),
        ):
            with storage.durability_batch():
                storage.write_bytes("batch-rename", b"payload")
        self.assertFalse(os.path.lexists(target))

    def test_durability_batch_barrier_failure_cleans_and_fsyncs_orphan(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("batch-barrier")
        real_barrier = storage._fsync_logical_publications
        calls = 0

        def fail_first(directories):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("barrier crash")
            return real_barrier(directories)

        with (
            patch.object(
                storage,
                "_fsync_logical_publications",
                side_effect=fail_first,
            ),
            self.assertRaisesRegex(OSError, "barrier crash"),
        ):
            with storage.durability_batch() as batch:
                storage.write_bytes("batch-barrier", b"payload")
                batch.barrier()

        self.assertFalse(os.path.lexists(target))
        self.assertEqual(calls, 2, "cleanup must have its own durability barrier")

    def test_durability_batch_ambiguous_catalog_failure_retains_durable_object(self):
        storage = LocalStorage(self.tmpdir)
        target = self._path("batch-redis-boundary")
        with self.assertRaisesRegex(TimeoutError, "ambiguous Redis timeout"):
            with storage.durability_batch() as batch:
                storage.write_bytes("batch-redis-boundary", b"payload")
                batch.barrier()
                batch.catalog_commit_started()
                raise TimeoutError("ambiguous Redis timeout")
        self.assertEqual(Path(target).read_bytes(), b"payload")

    def test_durability_batch_abort_never_removes_replacement(self):
        storage = LocalStorage(self.tmpdir)
        storage.write_bytes("existing", b"old")
        orphan = self._path("new-orphan")
        with self.assertRaisesRegex(RuntimeError, "abort mutation"):
            with storage.durability_batch():
                # Replacement writes retain ordinary immediate durability and
                # are deliberately excluded from batch rollback.
                storage.write_bytes("existing", b"new")
                storage.write_bytes("new-orphan", b"orphan")
                raise RuntimeError("abort mutation")
        self.assertEqual(storage.read_bytes("existing"), b"new")
        self.assertFalse(os.path.lexists(orphan))

    def test_durability_batch_detects_symlink_substitution_without_unlinking_it(self):
        storage = LocalStorage(self.tmpdir)
        victim = self._path("victim")
        Path(victim).write_bytes(b"keep")
        target = self._path("substituted")
        with self.assertRaisesRegex(OSError, "immutable object changed"):
            with storage.durability_batch() as batch:
                storage.write_bytes("substituted", b"published")
                os.unlink(target)
                os.symlink(victim, target)
                batch.barrier()
        self.assertTrue(os.path.islink(target))
        self.assertEqual(Path(victim).read_bytes(), b"keep")

    def test_durability_batch_detects_directory_replacement(self):
        storage = LocalStorage(self.tmpdir)
        os.makedirs(self._path("live"))
        moved = self._path("moved")
        with self.assertRaisesRegex(OSError, "immutable object changed"):
            with storage.durability_batch() as batch:
                storage.write_bytes("live/object", b"published")
                os.rename(self._path("live"), moved)
                os.makedirs(self._path("live"))
                batch.barrier()
        # Rollback never follows the replacement hierarchy or deletes an
        # object through a path whose inode proof no longer matches.
        self.assertEqual(Path(moved, "object").read_bytes(), b"published")
        self.assertEqual(os.listdir(self._path("live")), [])

    def test_durability_batches_are_isolated_between_concurrent_writers(self):
        storage = LocalStorage(self.tmpdir)
        rendezvous = threading.Barrier(2)

        def write_one(name):
            with storage.durability_batch() as batch:
                storage.write_bytes(name, name.encode())
                rendezvous.wait(timeout=5)
                self.assertEqual(len(batch._publications), 1)
                batch.barrier()
                batch.catalog_commit_started()
                batch.catalog_commit_succeeded()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(write_one, name) for name in ("writer-a", "writer-b")]
            for future in futures:
                future.result()
        self.assertEqual(storage.read_bytes("writer-a"), b"writer-a")
        self.assertEqual(storage.read_bytes("writer-b"), b"writer-b")

    def test_durability_batch_can_be_explicitly_propagated_to_worker(self):
        storage = LocalStorage(self.tmpdir)
        with storage.durability_batch() as batch:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    copy_context().run,
                    storage.write_bytes,
                    "worker-object",
                    b"worker",
                )
                future.result()
            self.assertEqual(len(batch._publications), 1)
            batch.barrier()
            batch.catalog_commit_started()
            batch.catalog_commit_succeeded()
        self.assertEqual(storage.read_bytes("worker-object"), b"worker")

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_durability_batch_is_not_inherited_as_active_after_fork(self):
        result = run_fork_probe("durability_batch", root=self.tmpdir)

        self.assertEqual(result["child_exitcode"], 0)
        self.assertEqual(result["parent_publication_count"], 1)
        storage = LocalStorage(self.tmpdir)
        self.assertEqual(storage.read_bytes("child-object"), b"child")
        self.assertEqual(
            storage.read_bytes("parent-before-fork"), b"parent-before",
        )
        self.assertEqual(storage.read_bytes("parent-object"), b"parent")

    def test_logical_write_retry_anchors_previously_created_ancestors(self):
        storage = LocalStorage(self.tmpdir)
        logical_path = "retry-created/child/data.bin"

        def fail_after_directories(_stream, _data):
            raise OSError("first write failed")

        with (
            patch(
                "supertable.storage.local_storage.write_all",
                side_effect=fail_after_directories,
            ),
            self.assertRaisesRegex(OSError, "first write failed"),
        ):
            storage.write_bytes(logical_path, b"first")

        directory = self._path("retry-created/child")
        self.assertTrue(os.path.isdir(directory))
        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda path: synced.append(os.path.abspath(path)),
        ):
            storage.write_bytes(logical_path, b"second")

        self.assertEqual(
            synced,
            [
                directory,
                self._path("retry-created"),
                self.tmpdir,
            ],
        )

    def test_logical_write_anchors_a_root_created_after_storage_init(self):
        missing_root = self._path("new/storage/root")
        storage = LocalStorage(missing_root)
        synced = []
        with patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=lambda path: synced.append(os.path.abspath(path)),
        ):
            storage.write_bytes("data.bin", b"complete")

        self.assertEqual(
            synced,
            [
                missing_root,
                self._path("new/storage"),
                self._path("new"),
                self.tmpdir,
            ],
        )

    def test_all_logical_publication_variants_anchor_through_storage_root(self):
        storage = LocalStorage(self.tmpdir)
        source = self._path("source.bin")
        with open(source, "wb") as stream:
            stream.write(b"source")

        cases = (
            (
                "json/deep/data.json",
                lambda: storage.write_json("json/deep/data.json", {"ok": True}),
            ),
            (
                "parquet/deep/data.parquet",
                lambda: storage.write_parquet(b"parquet", "parquet/deep/data.parquet"),
            ),
            (
                "bytes/deep/data.bin",
                lambda: storage.write_bytes("bytes/deep/data.bin", b"bytes"),
            ),
            (
                "copy/deep/data.bin",
                lambda: storage.copy(source, "copy/deep/data.bin"),
            ),
        )

        def encode_parquet(value, target):
            with open(target, "wb") as stream:
                stream.write(value)

        for logical_path, publish in cases:
            with self.subTest(logical_path=logical_path):
                # Model directories left visible by a prior failed publication.
                os.makedirs(os.path.dirname(self._path(logical_path)), exist_ok=True)
                synced = []
                with (
                    patch(
                        "supertable.storage.local_storage.pq.write_table",
                        side_effect=encode_parquet,
                    ),
                    patch.object(
                        LocalStorage,
                        "_fsync_directory",
                        side_effect=lambda path: synced.append(os.path.abspath(path)),
                    ),
                ):
                    publish()
                self.assertEqual(synced[-1], self.tmpdir)
                self.assertEqual(
                    synced[0],
                    os.path.dirname(self._path(logical_path)),
                )

    def test_read_bytes_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.read_bytes(self._path("ghost.bin"))

    # ---- text ----

    def test_write_and_read_text(self):
        p = self._path("data.txt")
        self.storage.write_text(p, "hello world")
        self.assertEqual(self.storage.read_text(p), "hello world")

    def test_write_and_read_text_latin1(self):
        p = self._path("latin.txt")
        self.storage.write_text(p, "café", encoding="latin-1")
        self.assertEqual(self.storage.read_text(p, encoding="latin-1"), "café")

    def test_write_text_creates_parent_dirs(self):
        p = self._path("deep/dir/data.txt")
        self.storage.write_text(p, "hi")
        self.assertTrue(os.path.isfile(p))

    def test_read_text_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.read_text(self._path("ghost.txt"))

    # ---- copy ----

    def test_copy_file(self):
        src = self._path("src.txt")
        dst = self._path("dst.txt")
        with open(src, "w") as f:
            f.write("content")
        self.storage.copy(src, dst)
        self.assertTrue(os.path.isfile(dst))
        with open(dst) as f:
            self.assertEqual(f.read(), "content")

    def test_copy_creates_parent_dirs(self):
        src = self._path("src2.txt")
        dst = self._path("deep/nested/dst2.txt")
        with open(src, "w") as f:
            f.write("data")
        self.storage.copy(src, dst)
        self.assertTrue(os.path.isfile(dst))

    def test_copy_partial_temp_failure_preserves_source_and_destination(self):
        src = self._path("copy-source.bin")
        dst = self._path("copy-destination.bin")
        with open(src, "wb") as stream:
            stream.write(b"source-complete")
        with open(dst, "wb") as stream:
            stream.write(b"destination-old")

        def partial_then_fail(_source, target):
            with open(target, "wb") as stream:
                stream.write(b"partial")
            raise OSError("copy interrupted")

        with (
            patch(
                "supertable.storage.local_storage.shutil.copyfile",
                side_effect=partial_then_fail,
            ),
            self.assertRaisesRegex(OSError, "copy interrupted"),
        ):
            self.storage.copy(src, dst)
        with open(src, "rb") as stream:
            self.assertEqual(stream.read(), b"source-complete")
        with open(dst, "rb") as stream:
            self.assertEqual(stream.read(), b"destination-old")
        self.assertEqual(list(Path(self.tmpdir).glob(".tmp-copy-*")), [])

    def test_copy_file_fsync_failure_preserves_prior_destination(self):
        src = self._path("copy-fsync-source.bin")
        dst = self._path("copy-fsync-destination.bin")
        with open(src, "wb") as stream:
            stream.write(b"source-complete")
        with open(dst, "wb") as stream:
            stream.write(b"destination-old")
        with (
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=OSError("file fsync failed"),
            ),
            self.assertRaisesRegex(OSError, "file fsync failed"),
        ):
            self.storage.copy(src, dst)
        with open(dst, "rb") as stream:
            self.assertEqual(stream.read(), b"destination-old")

    def test_copy_replace_failure_preserves_prior_destination(self):
        src = self._path("copy-replace-source.bin")
        dst = self._path("copy-replace-destination.bin")
        with open(src, "wb") as stream:
            stream.write(b"source-complete")
        with open(dst, "wb") as stream:
            stream.write(b"destination-old")
        with (
            patch(
                "supertable.storage.local_storage.os.replace",
                side_effect=OSError("replace failed"),
            ),
            self.assertRaisesRegex(OSError, "replace failed"),
        ):
            self.storage.copy(src, dst)
        with open(dst, "rb") as stream:
            self.assertEqual(stream.read(), b"destination-old")

    def test_copy_directory_fsync_failure_is_not_acknowledged(self):
        src = self._path("copy-directory-source.bin")
        dst = self._path("copy-directory-destination.bin")
        with open(src, "wb") as stream:
            stream.write(b"source-complete")
        with (
            patch(
                "supertable.storage.local_storage.os.fsync",
                side_effect=[None, OSError("directory fsync failed")],
            ),
            self.assertRaisesRegex(OSError, "directory fsync failed"),
        ):
            self.storage.copy(src, dst)
        with open(src, "rb") as stream:
            self.assertEqual(stream.read(), b"source-complete")
        with open(dst, "rb") as stream:
            self.assertEqual(stream.read(), b"source-complete")

    # ---- read_json retry / race condition branches ----

    def test_read_json_retries_on_transient_empty_then_succeeds(self):
        """Simulates file appearing empty on first attempt, then valid."""
        p = self._path("retry_empty.json")
        with open(p, "w") as f:
            f.write('{"ok": true}')

        call_count = {"n": 0}
        original_getsize = os.path.getsize

        def flaky_getsize(path_arg):
            call_count["n"] += 1
            if path_arg == p and call_count["n"] <= 1:
                return 0
            return original_getsize(path_arg)

        with patch("supertable.storage.local_storage.os.path.getsize", side_effect=flaky_getsize):
            with patch("supertable.storage.local_storage.time.sleep"):
                result = self.storage.read_json(p)
        self.assertEqual(result, {"ok": True})

    def test_read_json_retries_on_json_decode_error_then_succeeds(self):
        """Simulates corrupt JSON on first read, valid on retry."""
        p = self._path("retry_json.json")
        with open(p, "w") as f:
            f.write('{"ok": true}')

        call_count = {"n": 0}
        original_open = open

        def flaky_open(path_arg, *args, **kwargs):
            if path_arg == p:
                call_count["n"] += 1
                if call_count["n"] <= 1:
                    # Return a file-like that yields bad JSON
                    return io.StringIO("{bad")
            return original_open(path_arg, *args, **kwargs)

        with patch("builtins.open", side_effect=flaky_open):
            with patch("supertable.storage.local_storage.time.sleep"):
                result = self.storage.read_json(p)
        self.assertEqual(result, {"ok": True})

    def test_read_json_file_vanishes_during_getsize_retries(self):
        """Simulates FileNotFoundError from getsize, then exhausts retries."""
        p = self._path("vanish.json")
        with open(p, "w") as f:
            f.write('{"x":1}')

        def vanishing_getsize(path_arg):
            raise FileNotFoundError("gone")

        with patch("supertable.storage.local_storage.os.path.getsize", side_effect=vanishing_getsize):
            with patch("supertable.storage.local_storage.time.sleep"):
                with self.assertRaises(FileNotFoundError):
                    self.storage.read_json(p)


# ═══════════════════════════════════════════════════════════════════════════
#  MINIO STORAGE
# ═══════════════════════════════════════════════════════════════════════════

class TestMinioStorage(unittest.TestCase):
    """Tests for MinioStorage with a fully mocked Minio client."""

    def _make_storage(self, **overrides):
        client = MagicMock()
        s = MinioStorage(bucket_name="test-bucket", client=client)
        s.endpoint_url = overrides.get("endpoint_url", "http://localhost:9000")
        s.region = overrides.get("region", None)
        s.secure = overrides.get("secure", False)
        s.url_style = overrides.get("url_style", "path")
        s._endpoint = overrides.get("_endpoint", "http://localhost:9000")
        s._access_key = overrides.get("_access_key", "minioadmin")
        s._secret_key = overrides.get("_secret_key", "minioadmin")
        return s, client

    # ---- __init__ ----

    def test_init_defaults(self):
        client = MagicMock()
        s = MinioStorage(bucket_name="mybucket", client=client)
        self.assertEqual(s.bucket_name, "mybucket")
        self.assertIsNone(s.endpoint_url)
        self.assertIsNone(s.region)
        self.assertEqual(s.url_style, "path")
        self.assertFalse(s.secure)
        self.assertIsNone(s._endpoint)
        self.assertIsNone(s._access_key)
        self.assertIsNone(s._secret_key)

    # ---- _build_client ----

    def test_build_client_http(self):
        with patch("supertable.storage.minio_storage.Minio") as MockMinio:
            MinioStorage._build_client("http://localhost:9000", "key", "secret", None)
            MockMinio.assert_called_once_with(
                endpoint="localhost:9000", access_key="key", secret_key="secret",
                secure=False, region=None,
            )

    def test_build_client_https(self):
        with patch("supertable.storage.minio_storage.Minio") as MockMinio:
            MinioStorage._build_client("https://s3.example.com", "key", "secret", "us-west-2")
            MockMinio.assert_called_once_with(
                endpoint="s3.example.com", access_key="key", secret_key="secret",
                secure=True, region="us-west-2",
            )

    def test_build_client_bad_scheme(self):
        with self.assertRaises(ValueError):
            MinioStorage._build_client("ftp://localhost:9000", "k", "s", None)

    # ---- _extract_expected_region_from_error ----

    def test_extract_region_from_error_found(self):
        e = _FakeS3Error(message="expecting 'eu-central-1'")
        result = MinioStorage._extract_expected_region_from_error(e)
        self.assertEqual(result, "eu-central-1")

    def test_extract_region_from_error_not_found(self):
        e = _FakeS3Error(message="something else happened")
        result = MinioStorage._extract_expected_region_from_error(e)
        self.assertIsNone(result)

    def test_extract_region_from_error_double_quotes(self):
        e = _FakeS3Error(message='expecting "us-west-2"')
        result = MinioStorage._extract_expected_region_from_error(e)
        self.assertEqual(result, "us-west-2")

    # ---- _rebuild_with_region ----

    def test_rebuild_with_region_success(self):
        s, _ = self._make_storage()
        with patch.object(MinioStorage, "_build_client", return_value=MagicMock()) as mock_build:
            new_client = s._rebuild_with_region("eu-west-1")
            mock_build.assert_called_once_with(
                "http://localhost:9000", "minioadmin", "minioadmin", "eu-west-1"
            )
        self.assertEqual(s.region, "eu-west-1")

    def test_rebuild_with_region_no_credentials_raises(self):
        s, _ = self._make_storage()
        s._endpoint = None
        s._access_key = None
        s._secret_key = None
        with self.assertRaises(RuntimeError):
            s._rebuild_with_region("eu-west-1")

    # ---- _ensure_bucket_exists ----

    def test_ensure_bucket_exists_already_exists(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = True
        s._ensure_bucket_exists("test-bucket", None)
        client.make_bucket.assert_not_called()

    def test_ensure_bucket_exists_creates_bucket_no_region(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        s._ensure_bucket_exists("test-bucket", None)
        client.make_bucket.assert_called_once_with("test-bucket")

    def test_ensure_bucket_exists_creates_bucket_with_region(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        s._ensure_bucket_exists("test-bucket", "eu-west-1")
        client.make_bucket.assert_called_once_with("test-bucket", location="eu-west-1")

    def test_ensure_bucket_exists_creates_bucket_us_east_1(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        s._ensure_bucket_exists("test-bucket", "us-east-1")
        client.make_bucket.assert_called_once_with("test-bucket")

    def test_ensure_bucket_exists_already_owned(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        e = _FakeS3Error(code="BucketAlreadyOwnedByYou")
        client.make_bucket.side_effect = e
        # Should not raise
        s._ensure_bucket_exists("test-bucket", None)

    def test_ensure_bucket_exists_already_exists_code(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        e = _FakeS3Error(code="BucketAlreadyExists")
        client.make_bucket.side_effect = e
        s._ensure_bucket_exists("test-bucket", None)

    def test_ensure_bucket_exists_auth_error_on_check_rebuilds(self):
        s, client = self._make_storage()
        auth_err = _FakeS3Error(code="AuthorizationHeaderMalformed", message="expecting 'eu-central-1'")
        client.bucket_exists.side_effect = [auth_err, True]
        with patch.object(s, "_rebuild_with_region") as mock_rebuild:
            new_client = MagicMock()
            new_client.bucket_exists.return_value = True
            mock_rebuild.return_value = new_client
            s._ensure_bucket_exists("test-bucket", None)
            mock_rebuild.assert_called_once_with("eu-central-1")

    def test_ensure_bucket_exists_auth_error_no_region_raises(self):
        s, client = self._make_storage()
        auth_err = _FakeS3Error(code="AuthorizationHeaderMalformed", message="no region hint")
        client.bucket_exists.side_effect = auth_err
        with self.assertRaises(_FakeS3Error):
            s._ensure_bucket_exists("test-bucket", None)

    def test_ensure_bucket_exists_other_error_raises(self):
        s, client = self._make_storage()
        client.bucket_exists.side_effect = _FakeS3Error(code="AccessDenied", message="denied")
        with self.assertRaises(_FakeS3Error):
            s._ensure_bucket_exists("test-bucket", None)

    def test_ensure_bucket_exists_make_bucket_auth_error_rebuilds(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        auth_err = _FakeS3Error(code="AuthorizationHeaderMalformed", message="expecting 'ap-southeast-1'")
        client.make_bucket.side_effect = auth_err
        with patch.object(s, "_rebuild_with_region") as mock_rebuild:
            new_client = MagicMock()
            mock_rebuild.return_value = new_client
            s._ensure_bucket_exists("test-bucket", None)
            mock_rebuild.assert_called_once_with("ap-southeast-1")
            # Should try make_bucket on the new client
            new_client.make_bucket.assert_called_once_with("test-bucket", location="ap-southeast-1")

    def test_ensure_bucket_exists_make_bucket_auth_error_us_east_1(self):
        s, client = self._make_storage()
        client.bucket_exists.return_value = False
        auth_err = _FakeS3Error(code="AuthorizationHeaderMalformed", message="expecting 'us-east-1'")
        client.make_bucket.side_effect = auth_err
        with patch.object(s, "_rebuild_with_region") as mock_rebuild:
            new_client = MagicMock()
            mock_rebuild.return_value = new_client
            s._ensure_bucket_exists("test-bucket", None)
            new_client.make_bucket.assert_called_once_with("test-bucket")

    # ---- from_env ----

    def test_from_env_success(self):
        # ``from_env`` reads from the frozen ``settings`` singleton, not the
        # live environment. Patch settings instead of os.environ.
        _patch_settings(
            self,
            STORAGE_BUCKET="mybucket",
            STORAGE_ENDPOINT_URL="http://minio:9000",
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
            STORAGE_REGION="us-west-2",
            STORAGE_FORCE_PATH_STYLE=True,
        )
        with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
            with patch.object(MinioStorage, "_ensure_bucket_exists"):
                s = MinioStorage.from_env()
        self.assertEqual(s.bucket_name, "mybucket")
        self.assertEqual(s.endpoint_url, "http://minio:9000")
        self.assertEqual(s.region, "us-west-2")
        self.assertFalse(s.secure)
        self.assertEqual(s.url_style, "path")
        self.assertEqual(s._endpoint, "http://minio:9000")
        self.assertEqual(s._access_key, "ak")
        self.assertEqual(s._secret_key, "sk")

    def test_from_env_https(self):
        _patch_settings(
            self,
            STORAGE_ENDPOINT_URL="https://s3.example.com",
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
        )
        with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
            with patch.object(MinioStorage, "_ensure_bucket_exists"):
                s = MinioStorage.from_env()
        self.assertTrue(s.secure)

    def test_from_env_defaults_bucket(self):
        _patch_settings(
            self,
            STORAGE_BUCKET="supertable",  # default value
            STORAGE_ENDPOINT_URL="http://localhost:9000",
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
        )
        with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
            with patch.object(MinioStorage, "_ensure_bucket_exists"):
                s = MinioStorage.from_env()
        self.assertEqual(s.bucket_name, "supertable")

    def test_from_env_missing_endpoint(self):
        _patch_settings(
            self,
            STORAGE_ENDPOINT_URL="",
            STORAGE_ACCESS_KEY="a",
            STORAGE_SECRET_KEY="s",
        )
        with self.assertRaises(RuntimeError):
            MinioStorage.from_env()

    def test_from_env_missing_access_key(self):
        _patch_settings(
            self,
            STORAGE_ENDPOINT_URL="http://x",
            STORAGE_ACCESS_KEY="",
            STORAGE_SECRET_KEY="s",
        )
        with self.assertRaises(RuntimeError):
            MinioStorage.from_env()

    def test_from_env_missing_secret_key(self):
        _patch_settings(
            self,
            STORAGE_ENDPOINT_URL="http://x",
            STORAGE_ACCESS_KEY="a",
            STORAGE_SECRET_KEY="",
        )
        with self.assertRaises(RuntimeError):
            MinioStorage.from_env()

    def test_from_env_vhost_default(self):
        _patch_settings(
            self,
            STORAGE_ENDPOINT_URL="http://localhost:9000",
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
            STORAGE_FORCE_PATH_STYLE=False,
        )
        with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
            with patch.object(MinioStorage, "_ensure_bucket_exists"):
                s = MinioStorage.from_env()
        self.assertEqual(s.url_style, "vhost")

    # ---- to_duckdb_path ----

    def test_to_duckdb_path_s3(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path("some/key.parquet", prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/some/key.parquet")

    def test_to_duckdb_path_s3_strips_leading_slash(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path("/some/key.parquet", prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/some/key.parquet")

    def test_to_duckdb_path_httpfs(self):
        s, _ = self._make_storage()
        s.endpoint_url = "http://minio:9000"
        result = s.to_duckdb_path("some/key.parquet", prefer_httpfs=True)
        self.assertEqual(result, "http://minio:9000/test-bucket/some/key.parquet")

    def test_to_duckdb_path_httpfs_https(self):
        s, _ = self._make_storage()
        s.endpoint_url = "https://s3.example.com"
        s.secure = True
        result = s.to_duckdb_path("key.parquet", prefer_httpfs=True)
        self.assertEqual(result, "https://s3.example.com/test-bucket/key.parquet")

    def test_to_duckdb_path_env_fallback(self):
        # to_duckdb_path consults settings.SUPERTABLE_DUCKDB_USE_HTTPFS, not
        # the live env var, so we patch the settings binding here too.
        _patch_settings(self, SUPERTABLE_DUCKDB_USE_HTTPFS=True)
        s, _ = self._make_storage()
        s.endpoint_url = "http://minio:9000"
        result = s.to_duckdb_path("key.parquet")
        self.assertIn("http://minio:9000", result)

    def test_to_duckdb_path_empty_key(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path("", prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/")

    def test_to_duckdb_path_none_key(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path(None, prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/")

    # ---- presign ----

    def test_presign(self):
        s, client = self._make_storage()
        client.presigned_get_object.return_value = "http://minio:9000/test-bucket/key?X-Amz=..."
        result = s.presign("key", expiry_seconds=600)
        client.presigned_get_object.assert_called_once()
        self.assertIn("http://", result)

    def test_presign_strips_leading_slash(self):
        s, client = self._make_storage()
        client.presigned_get_object.return_value = "url"
        s.presign("/leading/slash/key")
        args = client.presigned_get_object.call_args
        self.assertEqual(args[0][1], "leading/slash/key")

    def test_presign_rejects_external_provider_url(self):
        s, client = self._make_storage()
        with self.assertRaisesRegex(ValueError, "storage object key"):
            s.presign("https://provider.invalid/object?signature=secret")
        client.presigned_get_object.assert_not_called()

    # ---- _get_object_safe ----

    def test_get_object_safe_success(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b"hello"
        client.get_object.return_value = resp
        result = s._get_object_safe("some/key")
        self.assertEqual(result, b"hello")
        resp.close.assert_called_once()
        resp.release_conn.assert_called_once()

    def test_get_object_safe_read_fails_still_closes(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.side_effect = IOError("network error")
        client.get_object.return_value = resp
        with self.assertRaises(IOError):
            s._get_object_safe("some/key")
        resp.close.assert_called_once()
        resp.release_conn.assert_called_once()

    # ---- _object_exists ----

    def test_object_exists_true(self):
        s, client = self._make_storage()
        client.stat_object.return_value = MagicMock()
        self.assertTrue(s._object_exists("key"))

    def test_object_exists_false_no_such_key(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        self.assertFalse(s._object_exists("key"))

    def test_object_exists_false_not_found(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NotFound")
        self.assertFalse(s._object_exists("key"))

    def test_object_exists_other_error_raises(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="AccessDenied")
        with self.assertRaises(_FakeS3Error):
            s._object_exists("key")

    # ---- _child_names_one_level ----

    def test_child_names_one_level(self):
        s, client = self._make_storage()
        obj1 = MagicMock(object_name="prefix/child1")
        obj2 = MagicMock(object_name="prefix/child2/")
        obj3 = MagicMock(object_name="prefix/child2/subfile")
        client.list_objects.return_value = [obj1, obj2, obj3]
        result = s._child_names_one_level("prefix")
        self.assertIn("child1", result)
        self.assertIn("child2", result)

    def test_child_names_one_level_adds_slash(self):
        s, client = self._make_storage()
        client.list_objects.return_value = []
        s._child_names_one_level("noslash")
        client.list_objects.assert_called_with("test-bucket", prefix="noslash/", recursive=False)

    def test_child_names_skips_non_matching_prefix(self):
        s, client = self._make_storage()
        obj = MagicMock(object_name="other/path")
        client.list_objects.return_value = [obj]
        result = s._child_names_one_level("prefix/")
        self.assertEqual(result, [])

    # ---- read_json / write_json ----

    def test_write_json(self):
        s, client = self._make_storage()
        s.write_json("data.json", {"key": "value"})
        client.put_object.assert_called_once()
        call_kwargs = client.put_object.call_args
        self.assertEqual(call_kwargs[1].get("content_type") or call_kwargs[0][4] if len(call_kwargs[0]) > 4 else call_kwargs[1].get("content_type"), "application/json")

    def test_read_json_success(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b'{"hello": "world"}'
        client.get_object.return_value = resp
        result = s.read_json("data.json")
        self.assertEqual(result, {"hello": "world"})

    def test_read_json_not_found(self):
        s, client = self._make_storage()
        client.get_object.side_effect = _FakeS3Error(code="NoSuchKey")
        with self.assertRaises(FileNotFoundError):
            s.read_json("missing.json")

    def test_read_json_empty(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b""
        client.get_object.return_value = resp
        with self.assertRaises(ValueError):
            s.read_json("empty.json")

    def test_read_json_invalid(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b"{bad json"
        client.get_object.return_value = resp
        with self.assertRaises(ValueError):
            s.read_json("bad.json")

    # ---- exists / size / makedirs ----

    def test_exists_true(self):
        s, client = self._make_storage()
        client.stat_object.return_value = MagicMock()
        self.assertTrue(s.exists("key"))

    def test_exists_false(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        self.assertFalse(s.exists("key"))

    def test_size_success(self):
        s, client = self._make_storage()
        stat = MagicMock()
        stat.size = 42
        client.stat_object.return_value = stat
        self.assertEqual(s.size("key"), 42)

    def test_size_not_found(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        with self.assertRaises(FileNotFoundError):
            s.size("missing")

    def test_size_other_error_raises(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="AccessDenied")
        with self.assertRaises(_FakeS3Error):
            s.size("key")

    def test_makedirs_noop(self):
        s, _ = self._make_storage()
        s.makedirs("any/path")  # Should not raise

    # ---- list_files ----

    def test_list_files_all(self):
        s, _ = self._make_storage()
        with patch.object(s, "_child_names_one_level", return_value=["a.txt", "b.json"]):
            result = s.list_files("prefix")
        self.assertEqual(result, ["prefix/a.txt", "prefix/b.json"])

    def test_list_files_pattern(self):
        s, _ = self._make_storage()
        with patch.object(s, "_child_names_one_level", return_value=["a.txt", "b.json", "c.txt"]):
            result = s.list_files("prefix", pattern="*.txt")
        self.assertEqual(result, ["prefix/a.txt", "prefix/c.txt"])

    def test_list_files_adds_trailing_slash(self):
        s, _ = self._make_storage()
        with patch.object(s, "_child_names_one_level", return_value=[]) as mock_child:
            s.list_files("prefix")
        # The path should have "/" appended
        mock_child.assert_called_once_with("prefix/")

    def test_list_files_already_has_slash(self):
        s, _ = self._make_storage()
        with patch.object(s, "_child_names_one_level", return_value=[]) as mock_child:
            s.list_files("prefix/")
        mock_child.assert_called_once_with("prefix/")

    # ---- delete ----

    def test_delete_single_object(self):
        s, client = self._make_storage()
        client.stat_object.return_value = MagicMock()
        s.delete("key")
        client.remove_object.assert_called_once_with("test-bucket", "key")

    def test_delete_prefix(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        obj1 = MagicMock(object_name="prefix/a.txt")
        obj2 = MagicMock(object_name="prefix/b.txt")
        client.list_objects.return_value = iter([obj1, obj2])
        # MinIO consumes the DeleteObject generator lazily while its returned
        # error iterator is drained; model that SDK contract in the double.
        client.remove_objects.side_effect = (
            lambda _bucket, objects: (list(objects), [])[1]
        )
        s.delete("prefix")
        client.remove_objects.assert_called_once()

    def test_delete_not_found(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        client.list_objects.return_value = iter([])
        with self.assertRaises(FileNotFoundError):
            s.delete("ghost")

    def test_delete_prefix_with_errors(self):
        s, client = self._make_storage()
        client.stat_object.side_effect = _FakeS3Error(code="NoSuchKey")
        obj = MagicMock(object_name="prefix/a.txt")
        client.list_objects.return_value = iter([obj])
        err = MagicMock()
        err.message = "delete failed"
        client.remove_objects.return_value = [err]
        with self.assertRaises(RuntimeError):
            s.delete("prefix")

    def test_verified_delete_prefix_drains_arbitrary_size_in_batches(self):
        s, client = self._make_storage()
        remaining = {f"prefix/file-{index:04d}.parquet" for index in range(3505)}

        def list_current(_prefix, recursive=True):
            self.assertTrue(recursive)
            return [types.SimpleNamespace(object_name=name) for name in sorted(remaining)]

        def remove_current(_bucket, delete_stream):
            names = {
                getattr(item, "name", getattr(item, "_name", ""))
                for item in delete_stream
            }
            remaining.difference_update(names)
            return []

        with patch.object(s, "_object_exists", return_value=False), patch.object(
            s, "_list_objects", side_effect=list_current,
        ):
            client.remove_objects.side_effect = remove_current
            s.delete_prefix("prefix")

        self.assertEqual(remaining, set())
        self.assertEqual(client.remove_objects.call_count, 4)

    def test_verified_delete_prefix_retries_transient_partial_failure(self):
        s, client = self._make_storage()
        remaining = {"prefix/a.parquet", "prefix/b.parquet"}
        attempts = {"count": 0}

        def list_current(_prefix, recursive=True):
            return [types.SimpleNamespace(object_name=name) for name in sorted(remaining)]

        def remove_current(_bucket, delete_stream):
            names = {
                getattr(item, "name", getattr(item, "_name", ""))
                for item in delete_stream
            }
            attempts["count"] += 1
            if attempts["count"] == 1:
                return [types.SimpleNamespace(message="transient delete failure")]
            remaining.difference_update(names)
            return []

        with patch.object(s, "_object_exists", return_value=False), patch.object(
            s, "_list_objects", side_effect=list_current,
        ):
            client.remove_objects.side_effect = remove_current
            s.delete_prefix("prefix")

        self.assertEqual(remaining, set())
        self.assertEqual(attempts["count"], 2)

    def test_verified_delete_prefix_surfaces_error_after_listing_exhaustion(self):
        s, client = self._make_storage()
        secret = "https://minio.invalid/private?signature=DELETE_SECRET"
        listings = iter([
            [types.SimpleNamespace(object_name="prefix/a.parquet")],
            [],
        ])
        client.remove_objects.return_value = [
            types.SimpleNamespace(message=secret)
        ]
        with patch.object(s, "_object_exists", return_value=False), patch.object(
            s, "_list_objects", side_effect=lambda *_a, **_k: next(listings),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "MinIO prefix deletion failed",
            ) as caught:
                s.delete_prefix("prefix")
        self.assertNotIn(
            secret,
            "".join(traceback.format_exception(caught.exception)),
        )

    # ---- get_directory_structure ----

    def test_get_directory_structure(self):
        s, client = self._make_storage()
        obj1 = MagicMock(object_name="prefix/sub/a.txt")
        obj2 = MagicMock(object_name="prefix/b.json")
        client.list_objects.return_value = [obj1, obj2]
        result = s.get_directory_structure("prefix")
        self.assertEqual(result, {"sub": {"a.txt": None}, "b.json": None})

    def test_get_directory_structure_adds_slash(self):
        s, client = self._make_storage()
        client.list_objects.return_value = []
        s.get_directory_structure("prefix")
        client.list_objects.assert_called_with("test-bucket", prefix="prefix/", recursive=True)

    def test_get_directory_structure_empty(self):
        s, client = self._make_storage()
        client.list_objects.return_value = []
        result = s.get_directory_structure("prefix/")
        self.assertEqual(result, {})

    # ---- parquet ----

    def test_write_parquet(self):
        s, client = self._make_storage()
        fake_table = MagicMock()
        with patch("supertable.storage.minio_storage.pq") as mock_pq:
            s.write_parquet(fake_table, "data.parquet")
        client.put_object.assert_called_once()

    def test_read_parquet_success(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b"fake parquet bytes"
        client.get_object.return_value = resp
        with patch("supertable.storage.minio_storage.pq") as mock_pq:
            mock_pq.read_table.return_value = MagicMock()
            result = s.read_parquet("data.parquet")
        self.assertIsNotNone(result)

    def test_read_parquet_not_found(self):
        s, client = self._make_storage()
        client.get_object.side_effect = _FakeS3Error(code="NoSuchKey")
        with self.assertRaises(FileNotFoundError):
            s.read_parquet("missing.parquet")

    def test_read_parquet_corrupt(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b"not parquet"
        client.get_object.return_value = resp
        with patch("supertable.storage.minio_storage.pq") as mock_pq:
            mock_pq.read_table.side_effect = Exception("corrupt")
            with self.assertRaises(RuntimeError):
                s.read_parquet("corrupt.parquet")

    def test_read_parquet_failure_does_not_expose_path_or_backend_message(self):
        s, _ = self._make_storage()
        secret = "signed-path-token-DO-NOT-LOG"
        with (
            patch.object(s, "_get_object_safe", return_value=b"not parquet"),
            patch(
                "supertable.storage.minio_storage.pq.read_table",
                side_effect=RuntimeError(f"backend-secret-{secret}"),
            ),
            self.assertRaises(RuntimeError) as ctx,
        ):
            s.read_parquet(f"tenant/{secret}.parquet")

        rendered = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__,
            )
        )
        self.assertEqual(
            str(ctx.exception),
            "Failed to read Parquet; error_type=RuntimeError",
        )
        self.assertNotIn(secret, rendered)

    def test_read_bytes_not_found_does_not_expose_path_or_backend_message(self):
        s, _ = self._make_storage()
        secret = "tenant-path-token-DO-NOT-LOG"
        with (
            patch.object(
                s,
                "_get_object_safe",
                side_effect=_FakeS3Error("NoSuchKey", f"backend-{secret}"),
            ),
            self.assertRaises(FileNotFoundError) as ctx,
        ):
            s.read_bytes(f"tenant/{secret}.bin")

        rendered = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__,
            )
        )
        self.assertNotIn(secret, rendered)

    # ---- bytes ----

    def test_write_bytes(self):
        s, client = self._make_storage()
        s.write_bytes("key", b"\x00\x01")
        client.put_object.assert_called_once()

    def test_create_bytes_if_absent_uses_conditional_put(self):
        s, client = self._make_storage()
        self.assertTrue(s.create_bytes_if_absent("proof.json", b"proof"))
        client._execute.assert_called_once_with(
            "PUT",
            "test-bucket",
            "proof.json",
            body=b"proof",
            headers={
                "Content-Length": "5",
                "Content-Type": "application/octet-stream",
                "If-None-Match": "*",
            },
            no_body_trace=True,
        )

    def test_create_bytes_if_absent_distinguishes_exists_from_failure(self):
        s, client = self._make_storage()
        client._execute.side_effect = _FakeS3Error(code="PreconditionFailed")
        self.assertFalse(s.create_bytes_if_absent("proof.json", b"proof"))

        status_exists = _FakeS3Error(code="ProviderSpecificPrecondition")
        status_exists.response = types.SimpleNamespace(status=412)
        client._execute.side_effect = status_exists
        self.assertFalse(s.create_bytes_if_absent("proof.json", b"proof"))

        conflict = _FakeS3Error(code="ConditionalRequestConflict")
        conflict.response = types.SimpleNamespace(status=409)
        client._execute.side_effect = conflict
        with self.assertRaises(_FakeS3Error):
            s.create_bytes_if_absent("proof.json", b"proof")

        client._execute.side_effect = _FakeS3Error(code="AccessDenied")
        with self.assertRaises(_FakeS3Error):
            s.create_bytes_if_absent("proof.json", b"proof")

    def test_read_bytes_success(self):
        s, client = self._make_storage()
        resp = MagicMock()
        resp.read.return_value = b"\x00\x01"
        client.get_object.return_value = resp
        self.assertEqual(s.read_bytes("key"), b"\x00\x01")

    def test_read_bytes_not_found(self):
        s, client = self._make_storage()
        client.get_object.side_effect = _FakeS3Error(code="NoSuchKey")
        with self.assertRaises(FileNotFoundError):
            s.read_bytes("missing")

    def test_read_bytes_other_error_raises(self):
        s, client = self._make_storage()
        client.get_object.side_effect = _FakeS3Error(code="AccessDenied")
        with self.assertRaises(_FakeS3Error):
            s.read_bytes("key")

    # ---- text ----

    def test_write_text(self):
        s, _ = self._make_storage()
        with patch.object(s, "write_bytes") as mock_wb:
            s.write_text("key", "hello", encoding="utf-8")
            mock_wb.assert_called_once_with("key", b"hello")

    def test_read_text(self):
        s, _ = self._make_storage()
        with patch.object(s, "read_bytes", return_value=b"hello"):
            result = s.read_text("key", encoding="utf-8")
        self.assertEqual(result, "hello")

    # ---- copy ----

    def test_copy(self):
        s, client = self._make_storage()
        s.copy("src/key", "dst/key")
        client.copy_object.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  S3 STORAGE
# ═══════════════════════════════════════════════════════════════════════════

class TestS3Storage(unittest.TestCase):
    """Tests for S3Storage with a fully mocked boto3 client."""

    def _make_storage(self, **overrides):
        client = MagicMock()
        # Provide a meta object
        client.meta = MagicMock()
        client.meta.endpoint_url = "https://s3.amazonaws.com"
        client.meta.region_name = "us-east-1"
        s = S3Storage(
            bucket_name=overrides.get("bucket_name", "test-bucket"),
            client=client,
            endpoint_url=overrides.get("endpoint_url", "https://s3.amazonaws.com"),
            region=overrides.get("region", "us-east-1"),
            url_style=overrides.get("url_style", "vhost"),
            secure=overrides.get("secure", True),
        )
        s._bucket_region_checked = True  # skip region check in most tests
        return s, client

    # ---- __init__ ----

    def test_init_defaults(self):
        s, _ = self._make_storage()
        self.assertEqual(s.bucket_name, "test-bucket")
        self.assertEqual(s.region, "us-east-1")
        self.assertTrue(s.secure)
        self.assertEqual(s.url_style, "vhost")

    def test_init_no_scheme_adds_https(self):
        client = MagicMock()
        client.meta.endpoint_url = "https://s3.amazonaws.com"
        client.meta.region_name = "us-east-1"
        s = S3Storage(bucket_name="b", client=client, endpoint_url="s3.amazonaws.com")
        self.assertIn("https://", s._endpoint_url_arg)

    def test_init_normalizes_bucket_prefixed_endpoint(self):
        client = MagicMock()
        client.meta.endpoint_url = "https://s3.amazonaws.com"
        client.meta.region_name = "us-east-1"
        s = S3Storage(bucket_name="mybucket", client=client,
                      endpoint_url="https://mybucket.s3.amazonaws.com")
        self.assertEqual(s._endpoint_url_arg, "https://s3.amazonaws.com")

    def test_init_path_style(self):
        client = MagicMock()
        client.meta.endpoint_url = "https://s3.amazonaws.com"
        client.meta.region_name = "us-east-1"
        s = S3Storage(bucket_name="b", client=client, url_style="path")
        self.assertEqual(s.url_style, "path")

    def test_init_secure_explicit_false(self):
        client = MagicMock()
        client.meta.endpoint_url = "https://s3.amazonaws.com"
        client.meta.region_name = "us-east-1"
        s = S3Storage(bucket_name="b", client=client, secure=False)
        self.assertFalse(s.secure)

    def test_init_secure_auto_detect_http(self):
        client = MagicMock()
        client.meta.endpoint_url = "http://localhost:4566"
        client.meta.region_name = "us-east-1"
        s = S3Storage(bucket_name="b", client=client, endpoint_url="http://localhost:4566")
        self.assertFalse(s.secure)

    # ---- from_env ----

    def test_from_env_success(self):
        # ``from_env`` reads from the frozen ``settings`` singleton, not
        # the live environment. Patch settings instead.
        _patch_settings(
            self,
            STORAGE_BUCKET="mybucket",
            STORAGE_ENDPOINT_URL="https://s3.us-west-2.amazonaws.com",
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
            STORAGE_REGION="us-west-2",
            STORAGE_SESSION_TOKEN="token",
            STORAGE_FORCE_PATH_STYLE=True,
        )
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.meta.endpoint_url = "https://s3.us-west-2.amazonaws.com"
            mock_client.meta.region_name = "us-west-2"
            mock_boto3.client.return_value = mock_client
            s = S3Storage.from_env()
        self.assertEqual(s.bucket_name, "mybucket")
        self.assertEqual(s.url_style, "path")

    def test_from_env_defaults(self):
        _patch_settings(
            self,
            STORAGE_BUCKET="supertable",  # default
            STORAGE_ENDPOINT_URL="",
            STORAGE_ACCESS_KEY="",
            STORAGE_SECRET_KEY="",
            STORAGE_REGION="",
            STORAGE_SESSION_TOKEN="",
            STORAGE_FORCE_PATH_STYLE=False,
        )
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.meta.endpoint_url = "https://s3.amazonaws.com"
            mock_client.meta.region_name = "us-east-1"
            mock_boto3.client.return_value = mock_client
            s = S3Storage.from_env()
        self.assertEqual(s.bucket_name, "supertable")
        self.assertEqual(s.url_style, "vhost")

    def test_from_env_no_aws_fallbacks(self):
        """Verify AWS_* env vars are NOT used as fallbacks."""
        # All STORAGE_* are blank in the patched settings; AWS_* env vars
        # exist but should be ignored.
        _patch_settings(
            self,
            STORAGE_BUCKET="",
            STORAGE_ENDPOINT_URL="",
            STORAGE_ACCESS_KEY="",
            STORAGE_SECRET_KEY="",
            STORAGE_REGION="",
            STORAGE_SESSION_TOKEN="",
        )
        env = {
            "AWS_DEFAULT_REGION": "eu-west-1",
            "AWS_ACCESS_KEY_ID": "aws-ak",
            "AWS_SECRET_ACCESS_KEY": "aws-sk",
            "AWS_SESSION_TOKEN": "aws-token",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
                mock_client = MagicMock()
                mock_client.meta.endpoint_url = "https://s3.amazonaws.com"
                mock_client.meta.region_name = None
                mock_boto3.client.return_value = mock_client
                s = S3Storage.from_env()
        # Should NOT pick up AWS_* env values
        call_kwargs = mock_boto3.client.call_args[1]
        self.assertIsNone(call_kwargs.get("region_name"))
        self.assertIsNone(call_kwargs.get("aws_access_key_id"))
        self.assertIsNone(call_kwargs.get("aws_secret_access_key"))
        self.assertIsNone(call_kwargs.get("aws_session_token"))

    # ---- to_duckdb_path ----

    def test_to_duckdb_path_s3(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path("some/key.parquet", prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/some/key.parquet")

    def test_to_duckdb_path_httpfs_vhost(self):
        s, _ = self._make_storage(url_style="vhost")
        s.endpoint_url = "https://s3.amazonaws.com"
        result = s.to_duckdb_path("key.parquet", prefer_httpfs=True)
        self.assertEqual(result, "https://test-bucket.s3.amazonaws.com/key.parquet")

    def test_to_duckdb_path_httpfs_path_style(self):
        s, _ = self._make_storage(url_style="path")
        s.endpoint_url = "https://s3.amazonaws.com"
        result = s.to_duckdb_path("key.parquet", prefer_httpfs=True)
        self.assertEqual(result, "https://s3.amazonaws.com/test-bucket/key.parquet")

    def test_to_duckdb_path_strips_leading_slash(self):
        s, _ = self._make_storage()
        result = s.to_duckdb_path("/key.parquet", prefer_httpfs=False)
        self.assertEqual(result, "s3://test-bucket/key.parquet")

    def test_to_duckdb_path_env_fallback(self):
        # to_duckdb_path consults settings.SUPERTABLE_DUCKDB_USE_HTTPFS.
        _patch_settings(self, SUPERTABLE_DUCKDB_USE_HTTPFS=True)
        s, _ = self._make_storage()
        s.endpoint_url = "http://localhost:4566"
        result = s.to_duckdb_path("key.parquet")
        self.assertIn("http://", result)

    # ---- presign ----

    def test_presign(self):
        s, client = self._make_storage()
        client.generate_presigned_url.return_value = "https://signed-url"
        result = s.presign("key", expiry_seconds=300)
        self.assertEqual(result, "https://signed-url")
        client.generate_presigned_url.assert_called_once()

    def test_presign_strips_leading_slash(self):
        s, client = self._make_storage()
        client.generate_presigned_url.return_value = "url"
        s.presign("/leading/key")
        params = client.generate_presigned_url.call_args[1]["Params"]
        self.assertEqual(params["Key"], "leading/key")

    def test_presign_rejects_external_provider_url(self):
        s, client = self._make_storage()
        with self.assertRaisesRegex(ValueError, "storage object key"):
            s.presign("https://provider.invalid/object?signature=secret")
        client.generate_presigned_url.assert_not_called()

    # ---- _normalize_bucket_region ----

    def test_normalize_bucket_region_none(self):
        s, _ = self._make_storage()
        self.assertIsNone(s._normalize_bucket_region(None))

    def test_normalize_bucket_region_empty(self):
        s, _ = self._make_storage()
        self.assertIsNone(s._normalize_bucket_region(""))

    def test_normalize_bucket_region_eu(self):
        s, _ = self._make_storage()
        self.assertEqual(s._normalize_bucket_region("EU"), "eu-west-1")

    def test_normalize_bucket_region_us(self):
        s, _ = self._make_storage()
        self.assertEqual(s._normalize_bucket_region("US"), "us-east-1")

    def test_normalize_bucket_region_passthrough(self):
        s, _ = self._make_storage()
        self.assertEqual(s._normalize_bucket_region("ap-southeast-1"), "ap-southeast-1")

    # ---- _aws_endpoint_region ----

    def test_aws_endpoint_region_none(self):
        s, _ = self._make_storage()
        self.assertIsNone(s._aws_endpoint_region(None))

    def test_aws_endpoint_region_global(self):
        s, _ = self._make_storage()
        self.assertEqual(s._aws_endpoint_region("https://s3.amazonaws.com"), "us-east-1")

    def test_aws_endpoint_region_external(self):
        s, _ = self._make_storage()
        self.assertEqual(s._aws_endpoint_region("https://s3-external-1.amazonaws.com"), "us-east-1")

    def test_aws_endpoint_region_regional(self):
        s, _ = self._make_storage()
        self.assertEqual(s._aws_endpoint_region("https://s3.eu-central-1.amazonaws.com"), "eu-central-1")

    def test_aws_endpoint_region_non_aws(self):
        s, _ = self._make_storage()
        self.assertIsNone(s._aws_endpoint_region("https://minio.local:9000"))

    def test_aws_endpoint_region_vhost_prefix(self):
        s, _ = self._make_storage()
        result = s._aws_endpoint_region("https://test-bucket.s3.us-west-2.amazonaws.com")
        self.assertEqual(result, "us-west-2")

    def test_aws_endpoint_region_no_match(self):
        s, _ = self._make_storage()
        self.assertIsNone(s._aws_endpoint_region("https://unknown.amazonaws.com"))

    # ---- _is_aws_global_endpoint ----

    def test_is_aws_global_endpoint_true(self):
        s, _ = self._make_storage()
        self.assertTrue(s._is_aws_global_endpoint("https://s3.amazonaws.com"))

    def test_is_aws_global_endpoint_external(self):
        s, _ = self._make_storage()
        self.assertTrue(s._is_aws_global_endpoint("https://s3-external-1.amazonaws.com"))

    def test_is_aws_global_endpoint_false_regional(self):
        s, _ = self._make_storage()
        self.assertFalse(s._is_aws_global_endpoint("https://s3.us-west-2.amazonaws.com"))

    def test_is_aws_global_endpoint_false_none(self):
        s, _ = self._make_storage()
        self.assertFalse(s._is_aws_global_endpoint(None))

    def test_is_aws_global_endpoint_false_minio(self):
        s, _ = self._make_storage()
        self.assertFalse(s._is_aws_global_endpoint("http://minio:9000"))

    def test_is_aws_global_endpoint_with_bucket_prefix(self):
        s, _ = self._make_storage()
        self.assertTrue(s._is_aws_global_endpoint("https://test-bucket.s3.amazonaws.com"))

    # ---- _extract_expected_region_from_error ----

    def test_extract_region_from_error_field(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Region": "eu-west-1"}})
        self.assertEqual(s._extract_expected_region_from_error(e), "eu-west-1")

    def test_extract_region_from_error_header(self):
        s, _ = self._make_storage()
        e = _FakeClientError({
            "Error": {},
            "ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": "ap-northeast-1"}}
        })
        self.assertEqual(s._extract_expected_region_from_error(e), "ap-northeast-1")

    def test_extract_region_from_error_message(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Message": "expecting 'eu-central-1'"}})
        self.assertEqual(s._extract_expected_region_from_error(e), "eu-central-1")

    def test_extract_region_from_error_none(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Message": "something else"}})
        self.assertIsNone(s._extract_expected_region_from_error(e))

    # ---- _extract_expected_endpoint_url_from_error ----

    def test_extract_endpoint_from_error_with_url(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Endpoint": "https://s3.eu-west-1.amazonaws.com"}})
        result = s._extract_expected_endpoint_url_from_error(e)
        self.assertEqual(result, "https://s3.eu-west-1.amazonaws.com")

    def test_extract_endpoint_from_error_bare_host(self):
        s, _ = self._make_storage()
        s.secure = True
        e = _FakeClientError({"Error": {"Endpoint": "s3.eu-west-1.amazonaws.com"}})
        result = s._extract_expected_endpoint_url_from_error(e)
        self.assertEqual(result, "https://s3.eu-west-1.amazonaws.com")

    def test_extract_endpoint_from_error_bucket_prefixed(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Endpoint": "https://test-bucket.s3.us-west-2.amazonaws.com"}})
        result = s._extract_expected_endpoint_url_from_error(e)
        self.assertEqual(result, "https://s3.us-west-2.amazonaws.com")

    def test_extract_endpoint_from_error_none(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {}})
        self.assertIsNone(s._extract_expected_endpoint_url_from_error(e))

    def test_extract_endpoint_from_error_empty_host(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {"Endpoint": "test-bucket."}})
        # After stripping bucket prefix, host is empty
        result = s._extract_expected_endpoint_url_from_error(e)
        self.assertIsNone(result)

    # ---- _extract_expected_endpoint_url_from_location_header ----

    def test_extract_endpoint_from_location(self):
        s, _ = self._make_storage()
        e = _FakeClientError({
            "Error": {},
            "ResponseMetadata": {"HTTPHeaders": {"location": "https://s3.eu-west-1.amazonaws.com/test-bucket"}}
        })
        result = s._extract_expected_endpoint_url_from_location_header(e)
        self.assertEqual(result, "https://s3.eu-west-1.amazonaws.com")

    def test_extract_endpoint_from_location_none(self):
        s, _ = self._make_storage()
        e = _FakeClientError({"Error": {}, "ResponseMetadata": {"HTTPHeaders": {}}})
        self.assertIsNone(s._extract_expected_endpoint_url_from_location_header(e))

    def test_extract_endpoint_from_location_bucket_prefixed(self):
        s, _ = self._make_storage()
        e = _FakeClientError({
            "Error": {},
            "ResponseMetadata": {"HTTPHeaders": {"location": "https://test-bucket.s3.us-west-2.amazonaws.com"}}
        })
        result = s._extract_expected_endpoint_url_from_location_header(e)
        self.assertEqual(result, "https://s3.us-west-2.amazonaws.com")

    # ---- _object_exists ----

    def test_object_exists_true(self):
        s, client = self._make_storage()
        s._object_exists("key")
        # _call should be invoked with head_object

    def test_object_exists_false_404(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call", side_effect=_FakeClientError({"Error": {"Code": "404"}})):
            self.assertFalse(s._object_exists("key"))

    def test_object_exists_false_nosuchkey(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call", side_effect=_FakeClientError({"Error": {"Code": "NoSuchKey"}})):
            self.assertFalse(s._object_exists("key"))

    def test_object_exists_other_error_raises(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call", side_effect=_FakeClientError({"Error": {"Code": "AccessDenied"}})):
            with self.assertRaises(_FakeClientError):
                s._object_exists("key")

    # ---- _get_object_safe ----

    def test_get_object_safe_success(self):
        s, _ = self._make_storage()
        body = MagicMock()
        body.read.return_value = b"data"
        with patch.object(s, "_call", return_value={"Body": body}):
            result = s._get_object_safe("key")
        self.assertEqual(result, b"data")
        body.close.assert_called_once()

    def test_get_object_safe_read_error_closes(self):
        s, _ = self._make_storage()
        body = MagicMock()
        body.read.side_effect = IOError("fail")
        with patch.object(s, "_call", return_value={"Body": body}):
            with self.assertRaises(IOError):
                s._get_object_safe("key")
        body.close.assert_called_once()

    # ---- _call retry logic ----

    def test_call_success(self):
        s, client = self._make_storage()
        client.some_method = MagicMock(return_value="result")
        result = s._call("some_method", Bucket="b")
        self.assertEqual(result, "result")

    def test_call_non_redirect_error_raises(self):
        s, client = self._make_storage()
        client.some_method = MagicMock(
            side_effect=_FakeClientError({"Error": {"Code": "AccessDenied"}})
        )
        with self.assertRaises(_FakeClientError):
            s._call("some_method")

    def test_call_redirect_retries_once(self):
        s, client = self._make_storage()
        redirect_err = _FakeClientError({
            "Error": {"Code": "PermanentRedirect", "Region": "eu-west-1",
                      "Endpoint": "https://s3.eu-west-1.amazonaws.com"}
        })
        client.some_method = MagicMock(side_effect=[redirect_err, "success"])
        with patch.object(s, "_rebuild_client"):
            with patch.object(s, "_probe_bucket_region", return_value="eu-west-1"):
                result = s._call("some_method")
        self.assertEqual(result, "success")

    def test_call_redirect_no_change_raises(self):
        s, client = self._make_storage()
        s.region = "us-east-1"
        redirect_err = _FakeClientError({
            "Error": {"Code": "PermanentRedirect"}
        })
        client.some_method = MagicMock(side_effect=redirect_err)
        with patch.object(s, "_probe_bucket_region", return_value=None):
            with self.assertRaises(_FakeClientError):
                s._call("some_method")

    def test_call_second_attempt_failure_raises(self):
        s, client = self._make_storage()
        err1 = _FakeClientError({"Error": {"Code": "PermanentRedirect", "Region": "eu-west-1"}})
        err2 = _FakeClientError({"Error": {"Code": "SomeOtherError"}})
        client.some_method = MagicMock(side_effect=[err1, err2])
        with patch.object(s, "_rebuild_client"):
            with patch.object(s, "_probe_bucket_region", return_value="eu-west-1"):
                with self.assertRaises(_FakeClientError):
                    s._call("some_method")

    def test_call_auth_malformed_redirect(self):
        s, client = self._make_storage()
        err = _FakeClientError({
            "Error": {"Code": "AuthorizationHeaderMalformed", "Region": "ap-southeast-1"}
        })
        client.some_method = MagicMock(side_effect=[err, "ok"])
        with patch.object(s, "_rebuild_client"):
            with patch.object(s, "_probe_bucket_region", return_value="ap-southeast-1"):
                result = s._call("some_method")
        self.assertEqual(result, "ok")

    def test_call_global_endpoint_dropped_on_redirect(self):
        s, client = self._make_storage()
        s._endpoint_url_arg = "https://s3.amazonaws.com"
        err = _FakeClientError({"Error": {"Code": "PermanentRedirect"}})
        client.some_method = MagicMock(side_effect=[err, "ok"])
        with patch.object(s, "_probe_bucket_region", return_value="eu-west-1"):
            with patch.object(s, "_rebuild_client"):
                result = s._call("some_method")
        self.assertIsNone(s._endpoint_url_arg)

    # ---- _ensure_bucket_region ----

    def test_ensure_bucket_region_already_checked(self):
        s, client = self._make_storage()
        s._bucket_region_checked = True
        s._ensure_bucket_region()
        # Should not call head_bucket since already checked

    def test_ensure_bucket_region_first_call(self):
        s, _ = self._make_storage()
        s._bucket_region_checked = False
        with patch.object(s, "_call") as mock_call:
            s._ensure_bucket_region()
            mock_call.assert_called_once_with("head_bucket", Bucket="test-bucket")
        self.assertTrue(s._bucket_region_checked)

    def test_ensure_bucket_region_error_ignored(self):
        s, _ = self._make_storage()
        s._bucket_region_checked = False
        with patch.object(s, "_call", side_effect=_FakeClientError({"Error": {"Code": "403"}})):
            s._ensure_bucket_region()  # should not raise
        self.assertTrue(s._bucket_region_checked)

    # ---- _rebuild_client ----

    def test_rebuild_client(self):
        s, _ = self._make_storage()
        s._endpoint_url_arg = "https://s3.eu-west-1.amazonaws.com"
        s.region = "eu-west-1"
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.meta.endpoint_url = "https://s3.eu-west-1.amazonaws.com"
            mock_client.meta.region_name = "eu-west-1"
            mock_boto3.client.return_value = mock_client
            s._rebuild_client()
        self.assertEqual(s.endpoint_url, "https://s3.eu-west-1.amazonaws.com")

    # ---- read_json / write_json ----

    def test_read_json_success(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b'{"key": "value"}'):
            result = s.read_json("data.json")
        self.assertEqual(result, {"key": "value"})

    def test_read_json_not_found(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "NoSuchKey"}})):
            with self.assertRaises(FileNotFoundError):
                s.read_json("missing.json")

    def test_read_json_404(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "404"}})):
            with self.assertRaises(FileNotFoundError):
                s.read_json("missing.json")

    def test_read_json_empty(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b""):
            with self.assertRaises(ValueError):
                s.read_json("empty.json")

    def test_read_json_invalid(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b"{bad"):
            with self.assertRaises(ValueError):
                s.read_json("bad.json")

    def test_read_json_other_error_raises(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "AccessDenied"}})):
            with self.assertRaises(_FakeClientError):
                s.read_json("denied.json")

    def test_write_json(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call") as mock_call:
            s.write_json("data.json", {"key": "value"})
            mock_call.assert_called_once()
            self.assertEqual(mock_call.call_args[1]["ContentType"], "application/json")

    # ---- exists / size / makedirs ----

    def test_exists_true(self):
        s, _ = self._make_storage()
        with patch.object(s, "_object_exists", return_value=True):
            self.assertTrue(s.exists("key"))

    def test_exists_false(self):
        s, _ = self._make_storage()
        with patch.object(s, "_object_exists", return_value=False):
            self.assertFalse(s.exists("key"))

    def test_size_success(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call", return_value={"ContentLength": 1234}):
            self.assertEqual(s.size("key"), 1234)

    def test_size_not_found(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call",
                          side_effect=_FakeClientError({"Error": {"Code": "404"}})):
            with self.assertRaises(FileNotFoundError):
                s.size("missing")

    def test_size_other_error(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call",
                          side_effect=_FakeClientError({"Error": {"Code": "AccessDenied"}})):
            with self.assertRaises(_FakeClientError):
                s.size("denied")

    def test_makedirs_noop(self):
        s, _ = self._make_storage()
        s.makedirs("any/path")  # should not raise

    # ---- list_files ----

    def test_list_files_all(self):
        s, _ = self._make_storage()
        with patch.object(s, "_list_common_prefixes_and_objects_one_level",
                          return_value=["a.txt", "b.json"]):
            result = s.list_files("prefix")
        self.assertEqual(result, ["prefix/a.txt", "prefix/b.json"])

    def test_list_files_pattern(self):
        s, _ = self._make_storage()
        with patch.object(s, "_list_common_prefixes_and_objects_one_level",
                          return_value=["a.txt", "b.json", "c.txt"]):
            result = s.list_files("prefix", pattern="*.txt")
        self.assertEqual(result, ["prefix/a.txt", "prefix/c.txt"])

    def test_list_files_adds_trailing_slash(self):
        s, _ = self._make_storage()
        with patch.object(s, "_list_common_prefixes_and_objects_one_level", return_value=[]) as m:
            s.list_files("prefix")
        # path should have "/" appended before calling children

    # ---- _list_common_prefixes_and_objects_one_level ----

    def test_list_common_prefixes_and_objects(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        page = {
            "CommonPrefixes": [{"Prefix": "prefix/subdir/"}],
            "Contents": [{"Key": "prefix/file.txt"}],
        }
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        result = s._list_common_prefixes_and_objects_one_level("prefix/")
        self.assertIn("subdir", result)
        self.assertIn("file.txt", result)

    def test_list_common_prefixes_skips_folder_marker(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        page = {
            "CommonPrefixes": [],
            "Contents": [{"Key": "prefix/"}],  # folder marker
        }
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        result = s._list_common_prefixes_and_objects_one_level("prefix/")
        self.assertEqual(result, [])

    def test_list_common_prefixes_deeper_level_guard(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        page = {
            "CommonPrefixes": [],
            "Contents": [{"Key": "prefix/sub/deep/file.txt"}],
        }
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        result = s._list_common_prefixes_and_objects_one_level("prefix/")
        self.assertIn("sub", result)
        self.assertEqual(len(result), 1)

    # ---- delete ----

    def test_delete_single_object(self):
        s, _ = self._make_storage()
        with patch.object(s, "_object_exists", return_value=True):
            with patch.object(s, "_call") as mock_call:
                s.delete("key")
                mock_call.assert_called_once_with("delete_object", Bucket="test-bucket", Key="key")

    def test_delete_prefix(self):
        s, client = self._make_storage()
        with patch.object(s, "_object_exists", return_value=False):
            paginator = MagicMock()
            page = {"Contents": [{"Key": "prefix/a.txt"}, {"Key": "prefix/b.txt"}]}
            paginator.paginate.return_value = [page]
            client.get_paginator.return_value = paginator
            with patch.object(s, "_call") as mock_call:
                s.delete("prefix")
                mock_call.assert_called_once()

    def test_delete_not_found(self):
        s, client = self._make_storage()
        with patch.object(s, "_object_exists", return_value=False):
            paginator = MagicMock()
            paginator.paginate.return_value = [{"Contents": []}]
            client.get_paginator.return_value = paginator
            with patch.object(s, "_call"):
                with self.assertRaises(FileNotFoundError):
                    s.delete("ghost")

    def test_delete_prefix_large_batch(self):
        """Test batch deletion with >1000 objects triggers multiple delete calls."""
        s, client = self._make_storage()
        with patch.object(s, "_object_exists", return_value=False):
            contents = [{"Key": f"prefix/file{i}.txt"} for i in range(1500)]
            paginator = MagicMock()
            paginator.paginate.return_value = [{"Contents": contents}]
            client.get_paginator.return_value = paginator
            call_count = {"n": 0}

            def tracking_call(method, **kwargs):
                if method == "delete_objects":
                    call_count["n"] += 1

            with patch.object(s, "_call", side_effect=tracking_call):
                s.delete("prefix")
            self.assertEqual(call_count["n"], 2)  # 1000 + 500

    def test_verified_delete_prefix_drains_more_than_three_thousand_objects(self):
        s, client = self._make_storage()
        remaining = {f"prefix/file-{index:04d}.parquet" for index in range(3505)}
        paginator = MagicMock()

        def pages(**_kwargs):
            return [{"Contents": [{"Key": key} for key in sorted(remaining)]}]

        paginator.paginate.side_effect = pages
        client.get_paginator.return_value = paginator

        def delete_call(method, **kwargs):
            if method == "delete_objects":
                remaining.difference_update(
                    item["Key"] for item in kwargs["Delete"]["Objects"]
                )
                return {}
            return {}

        with patch.object(s, "_object_exists", return_value=False), patch.object(
            s, "_call", side_effect=delete_call,
        ) as call_storage:
            s.delete_prefix("prefix")

        self.assertEqual(remaining, set())
        delete_calls = [
            entry for entry in call_storage.call_args_list
            if entry.args and entry.args[0] == "delete_objects"
        ]
        self.assertEqual(len(delete_calls), 4)

    def test_verified_delete_prefix_never_hides_provider_partial_error(self):
        s, client = self._make_storage()
        remaining = {"prefix/a.parquet"}
        paginator = MagicMock()
        paginator.paginate.side_effect = lambda **_kwargs: [{
            "Contents": [{"Key": key} for key in sorted(remaining)]
        }]
        client.get_paginator.return_value = paginator

        def delete_call(method, **kwargs):
            if method == "delete_objects":
                # Model an inconsistent adapter which reports failure but no
                # longer returns the object. The provider error must win.
                remaining.clear()
                return {"Errors": [{
                    "Code": "DELETE_SECRET_CODE",
                    "Message": "https://s3.invalid/private?signature=DELETE_SECRET",
                }]}
            return {}

        with patch.object(s, "_object_exists", return_value=False), patch.object(
            s, "_call", side_effect=delete_call,
        ):
            with self.assertRaisesRegex(
                OSError, "S3 prefix deletion failed",
            ) as caught:
                s.delete_prefix("prefix")
        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertNotIn("DELETE_SECRET", rendered)
        self.assertNotIn("s3.invalid", rendered)

    def test_base_prefix_is_removed_from_listings_and_applied_once(self):
        s, _ = self._make_storage()
        s.base_prefix = "tenant/root"

        with patch.object(
            s, "_list_common_prefixes_and_objects_one_level",
            return_value=["part.parquet"],
        ) as listing:
            logical = s.list_files("orders")

        self.assertEqual(logical, ["orders/part.parquet"])
        listing.assert_called_once_with("tenant/root/orders/")
        self.assertEqual(
            s._with_base(logical[0]),
            "tenant/root/orders/part.parquet",
        )
        self.assertEqual(
            s.canonical_uri(logical[0]),
            "s3://test-bucket/tenant/root/orders/part.parquet",
        )

    # ---- get_directory_structure ----

    def test_get_directory_structure(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        page = {
            "Contents": [
                {"Key": "prefix/sub/a.txt"},
                {"Key": "prefix/b.json"},
            ]
        }
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        result = s.get_directory_structure("prefix")
        self.assertEqual(result, {"sub": {"a.txt": None}, "b.json": None})

    def test_get_directory_structure_skips_folder_markers(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        page = {"Contents": [{"Key": "prefix/subfolder/"}]}
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        result = s.get_directory_structure("prefix")
        self.assertEqual(result, {})

    def test_get_directory_structure_empty(self):
        s, client = self._make_storage()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]
        client.get_paginator.return_value = paginator
        result = s.get_directory_structure("prefix")
        self.assertEqual(result, {})

    # ---- parquet ----

    def test_write_parquet(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.pq"):
            with patch.object(s, "_call") as mock_call:
                s.write_parquet(MagicMock(), "data.parquet")
                mock_call.assert_called_once()

    def test_read_parquet_success(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b"fake"):
            with patch("supertable.storage.s3_storage.pq") as mock_pq:
                mock_pq.read_table.return_value = MagicMock()
                result = s.read_parquet("data.parquet")
        self.assertIsNotNone(result)

    def test_read_parquet_not_found(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "NoSuchKey"}})):
            with self.assertRaises(FileNotFoundError):
                s.read_parquet("missing.parquet")

    def test_read_parquet_corrupt(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b"not parquet"):
            with patch("supertable.storage.s3_storage.pq") as mock_pq:
                mock_pq.read_table.side_effect = Exception("corrupt")
                with self.assertRaises(RuntimeError):
                    s.read_parquet("corrupt.parquet")

    def test_read_parquet_failure_does_not_expose_path_or_backend_message(self):
        s, _ = self._make_storage()
        secret = "signed-path-token-DO-NOT-LOG"
        with (
            patch.object(s, "_get_object_safe", return_value=b"not parquet"),
            patch(
                "supertable.storage.s3_storage.pq.read_table",
                side_effect=RuntimeError(f"backend-secret-{secret}"),
            ),
            self.assertRaises(RuntimeError) as ctx,
        ):
            s.read_parquet(f"tenant/{secret}.parquet")

        rendered = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__,
            )
        )
        self.assertEqual(
            str(ctx.exception),
            "Failed to read Parquet; error_type=RuntimeError",
        )
        self.assertNotIn(secret, rendered)

    def test_read_bytes_not_found_does_not_expose_path_or_backend_message(self):
        s, _ = self._make_storage()
        secret = "tenant-path-token-DO-NOT-LOG"
        failure = _FakeClientError({
            "Error": {"Code": "NoSuchKey", "Message": f"backend-{secret}"},
        })
        with (
            patch.object(s, "_get_object_safe", side_effect=failure),
            self.assertRaises(FileNotFoundError) as ctx,
        ):
            s.read_bytes(f"tenant/{secret}.bin")

        rendered = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__,
            )
        )
        self.assertNotIn(secret, rendered)

    # ---- bytes ----

    def test_write_bytes(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call") as mock_call:
            s.write_bytes("key", b"\x00\x01")
            mock_call.assert_called_once()

    def test_create_bytes_if_absent_uses_if_none_match(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call") as storage_call:
            self.assertTrue(s.create_bytes_if_absent("proof.json", b"proof"))
        storage_call.assert_called_once_with(
            "put_object",
            Bucket="test-bucket",
            Key="proof.json",
            Body=b"proof",
            ContentType="application/octet-stream",
            IfNoneMatch="*",
        )

    def test_create_bytes_if_absent_distinguishes_412_from_409(self):
        s, _ = self._make_storage()
        exists = _FakeClientError({
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        })
        with patch.object(s, "_call", side_effect=exists):
            self.assertFalse(s.create_bytes_if_absent("proof.json", b"proof"))

        race = _FakeClientError({
            "Error": {"Code": "ConditionalRequestConflict"},
            "ResponseMetadata": {"HTTPStatusCode": 409},
        })
        with patch.object(s, "_call", side_effect=race), self.assertRaises(
            _FakeClientError,
        ):
            s.create_bytes_if_absent("proof.json", b"proof")

    def test_read_bytes_success(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe", return_value=b"\x00\x01"):
            self.assertEqual(s.read_bytes("key"), b"\x00\x01")

    def test_read_bytes_not_found(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "NoSuchKey"}})):
            with self.assertRaises(FileNotFoundError):
                s.read_bytes("missing")

    def test_read_bytes_other_error(self):
        s, _ = self._make_storage()
        with patch.object(s, "_get_object_safe",
                          side_effect=_FakeClientError({"Error": {"Code": "AccessDenied"}})):
            with self.assertRaises(_FakeClientError):
                s.read_bytes("denied")

    # ---- text ----

    def test_write_text(self):
        s, _ = self._make_storage()
        with patch.object(s, "write_bytes") as mock_wb:
            s.write_text("key", "hello")
            mock_wb.assert_called_once_with("key", b"hello")

    def test_read_text(self):
        s, _ = self._make_storage()
        with patch.object(s, "read_bytes", return_value=b"hello"):
            self.assertEqual(s.read_text("key"), "hello")

    # ---- copy ----

    def test_copy(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call") as mock_call:
            s.copy("src", "dst")
            mock_call.assert_called_once_with(
                "copy_object",
                Bucket="test-bucket",
                Key="dst",
                CopySource={"Bucket": "test-bucket", "Key": "src"},
            )

    # ---- _probe_bucket_region ----

    def test_probe_bucket_region_via_get_location(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            probe_client = MagicMock()
            probe_client.get_bucket_location.return_value = {"LocationConstraint": "eu-west-1"}
            mock_boto3.client.return_value = probe_client
            result = s._probe_bucket_region()
        self.assertEqual(result, "eu-west-1")

    def test_probe_bucket_region_null_location(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            probe_client = MagicMock()
            probe_client.get_bucket_location.return_value = {"LocationConstraint": None}
            mock_boto3.client.return_value = probe_client
            result = s._probe_bucket_region()
        self.assertEqual(result, "us-east-1")

    def test_probe_bucket_region_via_head_bucket(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            probe_client = MagicMock()
            probe_client.get_bucket_location.side_effect = _FakeClientError({"Error": {}})
            probe_client.head_bucket.return_value = {}
            mock_boto3.client.return_value = probe_client
            result = s._probe_bucket_region()
        self.assertEqual(result, "us-east-1")

    def test_probe_bucket_region_from_head_headers(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            probe_client = MagicMock()
            probe_client.get_bucket_location.side_effect = _FakeClientError({"Error": {}})
            probe_client.head_bucket.side_effect = _FakeClientError({
                "Error": {},
                "ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": "ap-southeast-1"}}
            })
            mock_boto3.client.return_value = probe_client
            result = s._probe_bucket_region()
        self.assertEqual(result, "ap-southeast-1")

    def test_probe_bucket_region_all_fail(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            probe_client = MagicMock()
            probe_client.get_bucket_location.side_effect = _FakeClientError({"Error": {}})
            probe_client.head_bucket.side_effect = Exception("total failure")
            mock_boto3.client.return_value = probe_client
            result = s._probe_bucket_region()
        self.assertIsNone(result)

    def test_probe_bucket_region_client_creation_fails(self):
        s, _ = self._make_storage()
        with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
            mock_boto3.client.side_effect = Exception("cannot create")
            result = s._probe_bucket_region()
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════════════
#  STORAGE FACTORY
# ═══════════════════════════════════════════════════════════════════════════

class TestStorageFactory(unittest.TestCase):

    # ---- _require ----

    def test_require_installed_module(self):
        _require("os", "fake")  # should not raise

    def test_require_missing_module(self):
        with self.assertRaises(RuntimeError) as ctx:
            _require("nonexistent_module_xyz", "myextra")
        self.assertIn("pip install", str(ctx.exception))
        self.assertIn("myextra", str(ctx.exception))

    # ---- get_storage LOCAL ----

    def test_get_storage_local_explicit(self):
        s = get_storage(kind="LOCAL")
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_local_case_insensitive(self):
        s = get_storage(kind="local")
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_local_from_env(self):
        # get_storage reads settings.STORAGE_TYPE (the frozen singleton),
        # not the live os.environ value.
        _patch_settings(self, STORAGE_TYPE="LOCAL")
        s = get_storage()
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_local_default_fallback(self):
        _patch_settings(self, STORAGE_TYPE="")
        original = default.STORAGE_TYPE
        default.STORAGE_TYPE = "LOCAL"
        try:
            s = get_storage()
        finally:
            default.STORAGE_TYPE = original
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_default_when_no_config(self):
        _patch_settings(self, STORAGE_TYPE="")
        original = default.STORAGE_TYPE
        default.STORAGE_TYPE = None
        try:
            s = get_storage()
        finally:
            default.STORAGE_TYPE = original
        self.assertIsInstance(s, LocalStorage)

    # ---- get_storage S3 ----

    def test_get_storage_s3_from_env(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.S3Storage.from_env.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="S3")
        self.assertEqual(s, mock_storage)

    def test_get_storage_s3_with_kwargs(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.S3Storage.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="S3", bucket_name="custom")
        self.assertEqual(s, mock_storage)

    # ---- get_storage MINIO ----

    def test_get_storage_minio_from_env(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.MinioStorage.from_env.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="MINIO")
        self.assertEqual(s, mock_storage)

    def test_get_storage_minio_with_kwargs(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.MinioStorage.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="MINIO", bucket_name="custom")
        self.assertEqual(s, mock_storage)

    # ---- get_storage AZURE ----

    def test_get_storage_azure_from_env(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.AzureBlobStorage.from_env.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="AZURE")
        self.assertEqual(s, mock_storage)

    def test_get_storage_azure_with_kwargs(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.AzureBlobStorage.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="AZURE", container_name="custom")
        self.assertEqual(s, mock_storage)

    # ---- get_storage GCS/GCP ----

    def test_get_storage_gcs(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.GCSStorage.from_env.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="GCS")
        self.assertEqual(s, mock_storage)

    def test_get_storage_gcp_alias(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.GCSStorage.from_env.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="GCP")
        self.assertEqual(s, mock_storage)

    def test_get_storage_gcs_with_kwargs(self):
        with patch("supertable.storage.storage_factory._require"):
            with patch("supertable.storage.storage_factory.importlib") as mock_importlib:
                mock_mod = MagicMock()
                mock_storage = MagicMock()
                mock_mod.GCSStorage.return_value = mock_storage
                mock_importlib.import_module.return_value = mock_mod
                mock_importlib.util.find_spec.return_value = True
                s = get_storage(kind="GCS", bucket_name="custom")
        self.assertEqual(s, mock_storage)

    # ---- unknown ----

    def test_get_storage_unknown_raises(self):
        with self.assertRaises(ValueError) as ctx:
            get_storage(kind="REDIS")
        self.assertIn("Unknown storage type", str(ctx.exception))

    # ---- selection priority ----

    def test_get_storage_kind_overrides_env(self):
        with patch.dict(os.environ, {"STORAGE_TYPE": "S3"}):
            s = get_storage(kind="LOCAL")
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_env_overrides_default(self):
        # When settings.STORAGE_TYPE is set, it takes precedence over
        # default.STORAGE_TYPE (which is the lowest-priority fallback).
        original = default.STORAGE_TYPE
        default.STORAGE_TYPE = "S3"
        _patch_settings(self, STORAGE_TYPE="LOCAL")
        try:
            s = get_storage()
        finally:
            default.STORAGE_TYPE = original
        self.assertIsInstance(s, LocalStorage)


def tearDownModule():
    """Undo the import-time minio/boto3 stubbing performed by the bootstrap above.

    The bootstrap installs method-less fakes into ``sys.modules`` (and overwrites
    ``minio.Minio``) so this suite can run without the real cloud SDKs. Those
    mutations are process-global and were never restored, leaking the fake
    ``minio.Minio`` into ``supertable.storage.minio_storage`` — which breaks every
    later test module that builds a real storage backend via ``get_storage()``
    (e.g. compaction stats writes and read-pruning).

    (pyarrow.parquet is not handled here: it is preferred-real at import time, so
    nothing to restore.)
    """
    # 1) Restore each stubbed module name to its pre-bootstrap state.
    for name, original in _ORIGINAL_SYS_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)       # we created the stub -> drop it
        else:
            sys.modules[name] = original      # we shadowed a real one -> restore

    # 2) Restore the clobbered ``minio.Minio`` on a surviving real module.
    _m = sys.modules.get("minio")
    if _m is not None:
        _orig_minio, _had = _ORIGINAL_MINIO_MINIO
        if _had:
            _m.Minio = _orig_minio
        elif hasattr(_m, "Minio"):
            delattr(_m, "Minio")

    # 3) Drop production storage modules that bound the fakes via ``from X import
    #    Y`` so they re-import against the restored real packages on next use
    #    (``get_storage`` imports them lazily via importlib).
    for name in _PRODUCTION_MODULES_TO_REFRESH:
        sys.modules.pop(name, None)


if __name__ == "__main__":
    unittest.main()
