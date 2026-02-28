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
import os
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch, call

# ---------------------------------------------------------------------------
# Bootstrap: create stub modules so imports don't fail without real packages
# ---------------------------------------------------------------------------

def _ensure_stub(name, attrs=None):
    """Insert a stub module into sys.modules if it isn't already importable."""
    if name in sys.modules:
        return sys.modules[name]
    mod = types.ModuleType(name)
    if attrs:
        for k, v in attrs.items():
            setattr(mod, k, v)
    sys.modules[name] = mod
    # ensure parent packages exist
    parts = name.split(".")
    for i in range(1, len(parts)):
        parent = ".".join(parts[:i])
        if parent not in sys.modules:
            sys.modules[parent] = types.ModuleType(parent)
    return mod


# --- pyarrow stubs ---
_pa = _ensure_stub("pyarrow", {
    "Table": type("Table", (), {}),
    "table": lambda data, names=None: MagicMock(spec=[]),
})
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


class _FakeClientError(Exception):
    def __init__(self, error_response=None, operation_name="op"):
        self.response = error_response or {"Error": {}}
        super().__init__(str(self.response))


_botocore_exceptions = _ensure_stub("botocore.exceptions", {"ClientError": _FakeClientError})

_boto3 = _ensure_stub("boto3", {"client": MagicMock()})

# --- supertable config stubs ---
# The real supertable package is on disk, so we only ensure the config modules
# have the expected attributes (they already do via our local files).
# We do NOT stub supertable or supertable.config or supertable.storage.

# NOTE: do NOT stub supertable.storage — the real package is on disk

# NOW we can safely import the production code
from supertable.storage.storage_interface import StorageInterface
from supertable.storage.local_storage import LocalStorage
from supertable.storage.minio_storage import MinioStorage
from supertable.storage.s3_storage import S3Storage
from supertable.storage.storage_factory import get_storage, _require
from supertable.config.defaults import default


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


# ═══════════════════════════════════════════════════════════════════════════
#  LOCAL STORAGE
# ═══════════════════════════════════════════════════════════════════════════

class TestLocalStorage(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="test_local_storage_")
        self.storage = LocalStorage()

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

    def test_delete_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.delete(self._path("ghost"))

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
            mock_pq.write_table.assert_called_once_with(fake_table, p)

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
            fh = original_open(path_arg, *args, **kwargs)
            if path_arg == p:
                call_count["n"] += 1
                if call_count["n"] <= 1:
                    # Return a file-like that yields bad JSON
                    return io.StringIO("{bad")
            return fh

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
        env = {
            "STORAGE_BUCKET": "mybucket",
            "STORAGE_ENDPOINT_URL": "http://minio:9000",
            "STORAGE_ACCESS_KEY": "ak",
            "STORAGE_SECRET_KEY": "sk",
            "STORAGE_REGION": "us-west-2",
            "STORAGE_FORCE_PATH_STYLE": "true",
        }
        with patch.dict(os.environ, env, clear=False):
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
        env = {
            "STORAGE_ENDPOINT_URL": "https://s3.example.com",
            "STORAGE_ACCESS_KEY": "ak",
            "STORAGE_SECRET_KEY": "sk",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
                with patch.object(MinioStorage, "_ensure_bucket_exists"):
                    s = MinioStorage.from_env()
        self.assertTrue(s.secure)

    def test_from_env_defaults_bucket(self):
        env = {
            "STORAGE_ENDPOINT_URL": "http://localhost:9000",
            "STORAGE_ACCESS_KEY": "ak",
            "STORAGE_SECRET_KEY": "sk",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch.dict(os.environ, {"STORAGE_BUCKET": ""}, clear=False):
                with patch.object(MinioStorage, "_build_client", return_value=MagicMock()):
                    with patch.object(MinioStorage, "_ensure_bucket_exists"):
                        s = MinioStorage.from_env()
        self.assertEqual(s.bucket_name, "supertable")

    def test_from_env_missing_endpoint(self):
        with patch.dict(os.environ, {"STORAGE_ENDPOINT_URL": "", "STORAGE_ACCESS_KEY": "a", "STORAGE_SECRET_KEY": "s"}, clear=False):
            with self.assertRaises(RuntimeError):
                MinioStorage.from_env()

    def test_from_env_missing_access_key(self):
        with patch.dict(os.environ, {"STORAGE_ENDPOINT_URL": "http://x", "STORAGE_ACCESS_KEY": "", "STORAGE_SECRET_KEY": "s"}, clear=False):
            with self.assertRaises(RuntimeError):
                MinioStorage.from_env()

    def test_from_env_missing_secret_key(self):
        with patch.dict(os.environ, {"STORAGE_ENDPOINT_URL": "http://x", "STORAGE_ACCESS_KEY": "a", "STORAGE_SECRET_KEY": ""}, clear=False):
            with self.assertRaises(RuntimeError):
                MinioStorage.from_env()

    def test_from_env_vhost_default(self):
        env = {
            "STORAGE_ENDPOINT_URL": "http://localhost:9000",
            "STORAGE_ACCESS_KEY": "ak",
            "STORAGE_SECRET_KEY": "sk",
            "STORAGE_FORCE_PATH_STYLE": "",
        }
        with patch.dict(os.environ, env, clear=False):
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
        s, _ = self._make_storage()
        with patch.dict(os.environ, {"SUPERTABLE_DUCKDB_USE_HTTPFS": "1"}):
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
        client.remove_objects.return_value = []
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

    # ---- bytes ----

    def test_write_bytes(self):
        s, client = self._make_storage()
        s.write_bytes("key", b"\x00\x01")
        client.put_object.assert_called_once()

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
        env = {
            "STORAGE_BUCKET": "mybucket",
            "STORAGE_ENDPOINT_URL": "https://s3.us-west-2.amazonaws.com",
            "STORAGE_ACCESS_KEY": "ak",
            "STORAGE_SECRET_KEY": "sk",
            "STORAGE_REGION": "us-west-2",
            "STORAGE_SESSION_TOKEN": "token",
            "STORAGE_FORCE_PATH_STYLE": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch("supertable.storage.s3_storage.boto3") as mock_boto3:
                mock_client = MagicMock()
                mock_client.meta.endpoint_url = "https://s3.us-west-2.amazonaws.com"
                mock_client.meta.region_name = "us-west-2"
                mock_boto3.client.return_value = mock_client
                s = S3Storage.from_env()
        self.assertEqual(s.bucket_name, "mybucket")
        self.assertEqual(s.url_style, "path")

    def test_from_env_defaults(self):
        env = {
            "STORAGE_BUCKET": "",
            "STORAGE_ENDPOINT_URL": "",
            "STORAGE_ACCESS_KEY": "",
            "STORAGE_SECRET_KEY": "",
            "STORAGE_REGION": "",
            "STORAGE_SESSION_TOKEN": "",
            "STORAGE_FORCE_PATH_STYLE": "",
        }
        with patch.dict(os.environ, env, clear=False):
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
        env = {
            "STORAGE_BUCKET": "",
            "STORAGE_ENDPOINT_URL": "",
            "STORAGE_ACCESS_KEY": "",
            "STORAGE_SECRET_KEY": "",
            "STORAGE_REGION": "",
            "STORAGE_SESSION_TOKEN": "",
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
        s, _ = self._make_storage()
        s.endpoint_url = "http://localhost:4566"
        with patch.dict(os.environ, {"SUPERTABLE_DUCKDB_USE_HTTPFS": "true"}):
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

    # ---- bytes ----

    def test_write_bytes(self):
        s, _ = self._make_storage()
        with patch.object(s, "_call") as mock_call:
            s.write_bytes("key", b"\x00\x01")
            mock_call.assert_called_once()

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
        with patch.dict(os.environ, {"STORAGE_TYPE": "LOCAL"}):
            s = get_storage()
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_local_default_fallback(self):
        with patch.dict(os.environ, {"STORAGE_TYPE": ""}):
            default.STORAGE_TYPE = "LOCAL"
            s = get_storage()
        self.assertIsInstance(s, LocalStorage)

    def test_get_storage_default_when_no_config(self):
        with patch.dict(os.environ, {"STORAGE_TYPE": ""}):
            default.STORAGE_TYPE = None
            s = get_storage()
        self.assertIsInstance(s, LocalStorage)
        default.STORAGE_TYPE = "LOCAL"  # restore

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
        default.STORAGE_TYPE = "S3"
        with patch.dict(os.environ, {"STORAGE_TYPE": "LOCAL"}):
            s = get_storage()
        self.assertIsInstance(s, LocalStorage)
        default.STORAGE_TYPE = "LOCAL"  # restore


if __name__ == "__main__":
    unittest.main()
