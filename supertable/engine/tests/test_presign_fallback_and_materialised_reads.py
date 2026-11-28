# route: supertable.engine.tests.test_presign_fallback_and_materialised_reads
"""Three reads that used to fail, each for a reason of its own.

1. **A deletion vector behind the presign fallback.** When an object store
   refuses a direct read (301/403), the data files were read again through
   presigned URLs and the deletion vector was not: a table that had one could
   not be read at all. Both reads of a vector (the cached table and the inline
   subquery) now fall back as the data files do.

2. **A ``gcs://`` path behind the presign fallback.** The fallback could turn
   an ``s3://`` or ``http(s)://`` path back into a key and nothing else, so on
   GCS it repeated the path that had just been refused.

3. **The second read of a table with ``SUPERTABLE_DUCKDB_MATERIALIZE=table``.**
   The reflection was created as a table under a name every query shared, and
   teardown dropped views only: the next read of the same table on the
   connection failed with "Table ... already exists".

4. **A storage prefix behind the presign fallback.** A storage with a base
   prefix (``SUPERTABLE_PREFIX``) writes the prefix into the path it hands
   DuckDB and adds it again when it signs. The fallback signed the path's key
   as it stood, ``<prefix>/<prefix>/<key>``, an object that does not exist.

The first, third and fourth run on a real DuckDB. The first also needs DuckDB's httpfs
extension and reaches a store that is a local HTTP server in this process: it
answers 403 to a request without a signature and serves the file to one with.
"""

from __future__ import annotations

import dataclasses
import functools
import http.server
import io
import os
import shutil
import socketserver
import threading
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pytest

import supertable.engine.duckdb as engine_duckdb
import supertable.engine.engine_common as ec
from supertable.config.homedir import get_app_home as real_app_home
from supertable.config.settings import settings as real_settings
from supertable.data_classes import Reflection, SuperSnapshot, TombstoneDef
from supertable.engine.engine_config import EngineRuntimeConfig
from supertable.utils.sql_parser import SQLParser


RUNTIME = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=7, duckdb_external_cache_size="",
)

DATA_KEY = "acme/lake/tables/emp/data/part-0.parquet"
VECTOR_KEYS = ["acme/lake/tables/emp/tombstone/dv-base.parquet",
               "acme/lake/tables/emp/tombstone/dv-delta.parquet"]


def _settings(monkeypatch, **changes):
    values = dict(
        STORAGE_TYPE="LOCAL",
        STORAGE_BUCKET="",
        STORAGE_ENDPOINT_URL="",
        STORAGE_ACCESS_KEY="",
        STORAGE_SECRET_KEY="",
        STORAGE_SESSION_TOKEN="",
        SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK=True,
        SUPERTABLE_DUCKDB_FILE_ACCESS_DIRECTORIES="",
        SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=False,
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE="",
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR="",
        SUPERTABLE_DUCKDB_HTTP_TIMEOUT="3",
        SUPERTABLE_DUCKDB_MATERIALIZE="view",
    )
    values.update(changes)
    monkeypatch.setattr(ec, "settings", dataclasses.replace(real_settings, **values))


# ==========================================================================
# 2. A path is turned back into the key the storage signs
# ==========================================================================

@pytest.mark.parametrize("url, key", [
    ("gcs://bucket/acme/lake/f.parquet", "acme/lake/f.parquet"),
    ("gs://bucket/acme/lake/f.parquet", "acme/lake/f.parquet"),
    ("azure://container/acme/lake/f.parquet", "acme/lake/f.parquet"),
    ("az://container/acme/lake/f.parquet", "acme/lake/f.parquet"),
    ("s3://bucket/acme/lake/f.parquet", "acme/lake/f.parquet"),
])
def test_a_bucket_scheme_gives_the_key_after_the_bucket(url, key):
    assert ec.url_to_key(url, None) == key
    # the configured bucket plays no part: the URL names its own
    assert ec.url_to_key(url, "another-bucket") == key


@pytest.mark.parametrize("url", ["file:///tmp/f.parquet", "/tmp/f.parquet", "acme/f.parquet"])
def test_what_is_no_object_store_url_gives_no_key(url):
    assert ec.url_to_key(url, "bucket") is None


class _Signer:
    """A storage that signs: it records the keys it was asked for."""

    def __init__(self, base_prefix="", **names):
        self.base_prefix = base_prefix
        self.asked = []
        for name, value in names.items():       # bucket_name / container_name
            setattr(self, name, value)

    def presign(self, key):
        self.asked.append(key)
        return f"https://signed.example/{key}?sig=ok"


@pytest.mark.parametrize("scheme", ["gcs", "gs", "azure", "az"])
def test_a_gcs_or_azure_path_is_signed_instead_of_repeated(scheme, monkeypatch):
    _settings(monkeypatch)
    storage = _Signer()
    path = f"{scheme}://bucket/{DATA_KEY}"
    assert ec.make_presigned_list(storage, [path]) == [
        f"https://signed.example/{DATA_KEY}?sig=ok"]
    assert storage.asked == [DATA_KEY]


PREFIX = "warehouse/prod"

#: Every form a storage with the bucket ``lake-bucket`` and the base prefix
#: ``warehouse/prod`` hands DuckDB for ``DATA_KEY``.
PREFIXED_PATHS = {
    "s3://": f"s3://lake-bucket/{PREFIX}/{DATA_KEY}",
    "gcs://": f"gcs://lake-bucket/{PREFIX}/{DATA_KEY}",
    "azure://": f"azure://lake-bucket/{PREFIX}/{DATA_KEY}",
    "http, bucket in the path": f"http://minio:9000/lake-bucket/{PREFIX}/{DATA_KEY}",
    "https, bucket in the host": f"https://lake-bucket.s3.eu-west-1.amazonaws.com/{PREFIX}/{DATA_KEY}",
    "https, GCS": f"https://storage.googleapis.com/lake-bucket/{PREFIX}/{DATA_KEY}",
    "https, Azure": f"https://account.blob.core.windows.net/lake-bucket/{PREFIX}/{DATA_KEY}",
    "already signed": f"http://minio:9000/lake-bucket/{PREFIX}/{DATA_KEY}?X-Amz-Signature=old",
}


@pytest.mark.parametrize("form", sorted(PREFIXED_PATHS))
def test_the_storages_base_prefix_is_not_signed_twice(form, monkeypatch):
    """``to_duckdb_path`` writes the base prefix into the path and ``presign``
    adds it again: the key that is signed is the one the path was made from."""
    _settings(monkeypatch)
    storage = _Signer(base_prefix=PREFIX, bucket_name="lake-bucket")
    assert ec.make_presigned_list(storage, [PREFIXED_PATHS[form]]) == [
        f"https://signed.example/{DATA_KEY}?sig=ok"]
    assert storage.asked == [DATA_KEY]


@pytest.mark.parametrize("form", sorted(PREFIXED_PATHS))
def test_a_storage_without_a_prefix_signs_the_whole_key(form, monkeypatch):
    _settings(monkeypatch)
    storage = _Signer(bucket_name="lake-bucket")
    ec.make_presigned_list(storage, [PREFIXED_PATHS[form]])
    assert storage.asked == [f"{PREFIX}/{DATA_KEY}"]


def test_a_key_outside_the_base_prefix_is_signed_as_it_is(monkeypatch):
    _settings(monkeypatch)
    storage = _Signer(base_prefix="warehouse")
    ec.make_presigned_list(storage, [f"gcs://bucket/elsewhere/{DATA_KEY}"])
    assert storage.asked == [f"elsewhere/{DATA_KEY}"]


def test_a_prefix_is_taken_off_whole_segments_only(monkeypatch):
    """``warehouse`` is no prefix of ``warehouse-archive/...``."""
    _settings(monkeypatch)
    storage = _Signer(base_prefix="warehouse")
    ec.make_presigned_list(storage, [f"s3://bucket/warehouse-archive/{DATA_KEY}"])
    assert storage.asked == [f"warehouse-archive/{DATA_KEY}"]


@pytest.mark.parametrize("names", [{"bucket_name": "lake-bucket"},
                                   {"container_name": "lake-bucket"}])
def test_the_bucket_taken_off_an_http_path_is_the_storages_own(names, monkeypatch):
    """``STORAGE_BUCKET`` is one of the places a storage takes its bucket
    from: GCS reads ``GCS_BUCKET`` first, Azure falls back to
    ``AZURE_CONTAINER``. The path holds the name the storage wrote."""
    _settings(monkeypatch, STORAGE_BUCKET="supertable")
    storage = _Signer(**names)
    ec.make_presigned_list(storage, [f"https://storage.googleapis.com/lake-bucket/{DATA_KEY}"])
    assert storage.asked == [DATA_KEY]


def test_a_storage_that_names_no_bucket_falls_back_to_the_setting(monkeypatch):
    _settings(monkeypatch, STORAGE_BUCKET="lake-bucket")
    storage = _Signer()
    ec.make_presigned_list(storage, [f"http://minio:9000/lake-bucket/{DATA_KEY}"])
    assert storage.asked == [DATA_KEY]


def _real_storage(kind):
    """A real storage class with no client behind it: the paths it writes."""
    if kind == "minio":
        module = pytest.importorskip("supertable.storage.minio_storage")
        storage = object.__new__(module.MinioStorage)
        storage.bucket_name, storage.endpoint_url, storage.secure = (
            "lake-bucket", "http://minio:9000", False)
    elif kind in ("s3 path style", "s3 vhost style"):
        module = pytest.importorskip("supertable.storage.s3_storage")
        storage = object.__new__(module.S3Storage)
        storage.bucket_name, storage.endpoint_url, storage.secure = (
            "lake-bucket", "https://s3.eu-west-1.amazonaws.com", True)
        storage.url_style = "path" if kind == "s3 path style" else "vhost"
    elif kind == "gcs":
        module = pytest.importorskip("supertable.storage.gcp_storage")
        storage = object.__new__(module.GCSStorage)
        storage.bucket_name = "lake-bucket"
    else:
        module = pytest.importorskip("supertable.storage.azure_storage")
        storage = object.__new__(module.AzureBlobStorage)
        storage.container_name = "lake"
        storage.svc = SimpleNamespace(url="https://account.blob.core.windows.net/")
    return storage


@pytest.mark.parametrize("kind", ["minio", "s3 path style", "s3 vhost style", "gcs", "azure"])
@pytest.mark.parametrize("prefer_httpfs", [False, True], ids=["store scheme", "http(s)"])
@pytest.mark.parametrize("base_prefix", ["", PREFIX], ids=["no prefix", "prefix"])
def test_every_real_storage_signs_the_key_its_path_was_made_from(
        kind, prefer_httpfs, base_prefix, monkeypatch):
    """The path each storage class hands DuckDB, taken back to its own
    ``presign``: the key it is asked to sign is the key the path came from.
    ``STORAGE_BUCKET`` is set to something else throughout."""
    _settings(monkeypatch, STORAGE_BUCKET="some-other-bucket")
    storage = _real_storage(kind)
    storage.base_prefix = base_prefix
    asked = []
    storage.presign = lambda key: asked.append(key) or f"https://signed/{key}"
    path = storage.to_duckdb_path(DATA_KEY, prefer_httpfs=prefer_httpfs)
    assert DATA_KEY in path and (not base_prefix or base_prefix in path)
    assert ec.make_presigned_list(storage, [path]) == [f"https://signed/{DATA_KEY}"]
    assert asked == [DATA_KEY]


# ==========================================================================
# 1. A deletion vector's parts fall back to presigned URLs: the rule
# ==========================================================================

REFUSAL = ("HTTP Error: HTTP GET error on 'https://lake-bucket.s3.amazonaws.com/x' "
           "(HTTP 403)")


class _PrivateBucket:
    """Stands in for a connection reading a private bucket.

    A statement that names an unsigned part is refused the way the store
    refuses it; any other statement is recorded and runs.
    """

    def __init__(self, refuse="s3://", error=REFUSAL):
        self.refuse, self.error, self.statements = refuse, error, []

    def execute(self, sql):
        self.statements.append(sql)
        if self.refuse in sql:
            raise RuntimeError(self.error)
        return self


@pytest.fixture
def unit(monkeypatch):
    _settings(monkeypatch)
    monkeypatch.setattr(ec, "configure_httpfs_and_s3", lambda con, paths: None)
    parts = [f"s3://lake-bucket/{key}" for key in VECTOR_KEYS]
    signed = [f"https://signed.example/{key}?sig=ok" for key in VECTOR_KEYS]
    return SimpleNamespace(parts=parts, signed=signed, key=VECTOR_KEYS[0])


def test_the_cached_vector_is_read_again_through_signed_urls(unit):
    con, storage, cache = _PrivateBucket(), _Signer(), ec.TombstoneCache(8, 300)
    table = cache.acquire(con, unit.key, unit.parts, storage=storage)
    assert table == ec.dv_table_name(unit.key)
    assert len(con.statements) == 2
    assert all(part in con.statements[0] for part in unit.parts)
    # every part, and nothing unsigned beside them
    assert all(url in con.statements[1] for url in unit.signed)
    assert "s3://" not in con.statements[1]
    assert storage.asked == VECTOR_KEYS


def test_the_cached_vector_stays_under_its_stable_key(unit):
    """A presigned URL changes with every signature: the table is found again
    by the path, and the parts are not read a second time."""
    con, storage, cache = _PrivateBucket(), _Signer(), ec.TombstoneCache(8, 300)
    first = cache.acquire(con, unit.key, unit.parts, storage=storage)
    again = cache.acquire(con, unit.key, unit.parts, storage=storage)
    assert first == again
    assert len(con.statements) == 2
    assert [e["cache_key"] for e in cache.snapshot()] == [unit.key]
    assert cache.snapshot()[0]["ref_count"] == 2


def test_a_query_whose_data_files_needed_signing_signs_the_vector_at_once(unit):
    con, storage, cache = _PrivateBucket(), _Signer(), ec.TombstoneCache(8, 300)
    cache.acquire(con, unit.key, unit.parts, storage=storage, presigned=True)
    assert len(con.statements) == 1
    assert "s3://" not in con.statements[0]


def test_a_refused_vector_without_a_storage_is_the_querys_error(unit):
    con, cache = _PrivateBucket(), ec.TombstoneCache(8, 300)
    with pytest.raises(RuntimeError, match="403"):
        cache.acquire(con, unit.key, unit.parts)
    assert cache.snapshot() == []


def test_a_storage_that_cannot_sign_does_not_repeat_the_refused_read(unit):
    class CannotSign:
        def presign(self, key):
            raise RuntimeError("no credentials to sign with")

    con, cache = _PrivateBucket(), ec.TombstoneCache(8, 300)
    with pytest.raises(RuntimeError, match="403"):
        cache.acquire(con, unit.key, unit.parts, storage=CannotSign())
    assert len(con.statements) == 1


def test_an_error_that_is_no_refusal_is_not_retried(unit):
    con = _PrivateBucket(error='Binder Error: column "__rowid__" not found')
    storage, cache = _Signer(), ec.TombstoneCache(8, 300)
    with pytest.raises(RuntimeError, match="Binder Error"):
        cache.acquire(con, unit.key, unit.parts, storage=storage)
    assert storage.asked == []


def test_a_vector_refused_signed_as_well_is_the_querys_error(unit):
    """Nothing is passed over: a vector that cannot be read stops the read,
    because a table read without it brings its deleted rows back."""
    con = _PrivateBucket(refuse="read_parquet")
    with pytest.raises(RuntimeError, match="403"):
        ec.TombstoneCache(8, 300).acquire(con, unit.key, unit.parts, storage=_Signer())
    assert len(con.statements) == 2


def test_the_inline_vector_is_read_again_through_signed_urls(unit):
    con, storage = _PrivateBucket(), _Signer()
    definition = TombstoneDef(tombstone_path=unit.parts, cache_key=None)
    used = ec.create_tombstone_view(con, "st_x", "tomb_x", definition, storage=storage)
    assert used is True
    assert len(con.statements) == 2
    assert all(url in con.statements[1] for url in unit.signed)
    assert "s3://" not in con.statements[1]
    assert "ANTI JOIN" in con.statements[1]


def test_the_inline_vector_is_signed_at_once_after_the_data_files_were(unit):
    con, storage = _PrivateBucket(), _Signer()
    definition = TombstoneDef(tombstone_path=unit.parts, cache_key=None)
    assert ec.create_tombstone_view(
        con, "st_x", "tomb_x", definition, storage=storage, presigned=True) is True
    assert len(con.statements) == 1


def test_an_inline_vector_that_is_read_directly_is_not_signed(unit):
    con, storage = _PrivateBucket(refuse="nothing-is-refused"), _Signer()
    definition = TombstoneDef(tombstone_path=unit.parts, cache_key=None)
    assert ec.create_tombstone_view(con, "st_x", "tomb_x", definition, storage=storage) is False
    assert storage.asked == [] and len(con.statements) == 1


def test_an_inline_vector_without_a_storage_is_the_querys_error(unit):
    definition = TombstoneDef(tombstone_path=unit.parts, cache_key=None)
    with pytest.raises(RuntimeError, match="403"):
        ec.create_tombstone_view(_PrivateBucket(), "st_x", "tomb_x", definition)


def test_a_view_over_a_cached_vector_or_no_vector_reads_no_file(unit):
    con, storage = _PrivateBucket(), _Signer()
    assert ec.create_tombstone_view(con, "st_x", "tomb_x", None, storage=storage) is False
    definition = TombstoneDef(tombstone_path=unit.parts, cache_key=unit.key)
    assert ec.create_tombstone_view(
        con, "st_x", "tomb_y", definition, dv_table="dv_abc", storage=storage) is False
    assert storage.asked == []
    assert all("read_parquet" not in sql for sql in con.statements)


# ==========================================================================
# Real DuckDB: a lake, and a store that wants a signature
# ==========================================================================

@pytest.fixture
def lake(tmp_path, monkeypatch):
    """An application home with one table in it: three rows, and a deletion
    vector in two parts that takes rows 2 and 3 away."""
    home = tmp_path / "home"
    table = home / "acme" / "lake" / "tables" / "emp"
    (table / "data").mkdir(parents=True)
    (table / "tombstone").mkdir()
    writer = duckdb.connect()
    writer.execute(
        f"COPY (SELECT * FROM (VALUES (1, 'eu', 1, 0), (2, 'us', 2, 0), (3, 'eu', 3, 0)) "
        f"t(id, region, __rowid__, __timestamp__)) TO '{home / DATA_KEY}' (FORMAT parquet)")
    for key, rowid in zip(VECTOR_KEYS, (2, 3)):
        writer.execute(
            f"COPY (SELECT '{DATA_KEY}' AS __file__, {rowid} AS __rowid__) "
            f"TO '{home / key}' (FORMAT parquet)")
    writer.close()

    monkeypatch.setattr(ec, "get_app_home", lambda: str(home))
    monkeypatch.chdir(home)
    _settings(monkeypatch)
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()
    yield SimpleNamespace(home=home, tmp=tmp_path)
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()


@pytest.fixture
def httpfs(lake):
    """This build's httpfs, where the engine loads it from: the pinned home."""
    found = None
    for root in (Path.home() / ".duckdb" / "extensions",
                 Path(real_app_home()) / ".duckdb" / "extensions"):
        found = next(iter(sorted(
            root.glob(f"v{duckdb.__version__}/*/httpfs.duckdb_extension"))), None)
        if found is not None:
            break
    if found is None:
        pytest.skip("DuckDB's httpfs extension is not installed locally")
    extensions = lake.home / ".duckdb" / "extensions"
    extensions.mkdir(parents=True)
    (extensions / found.parent.parent.name).symlink_to(found.parent.parent)
    return found


class _SignatureWanted(http.server.BaseHTTPRequestHandler):
    """Serves ``/bucket/<key>`` from a directory to a request that carries
    ``sig=ok``, in ranges as an object store does, and answers 403 to one
    that does not."""

    def __init__(self, *args, directory, seen, **kwargs):
        self.directory, self.seen = directory, seen
        super().__init__(*args, **kwargs)

    def log_message(self, *args):
        pass

    def _answer(self, with_body):
        parsed = urlparse(self.path)
        signed = "sig=ok" in (parsed.query or "")
        key = parsed.path.lstrip("/").split("/", 1)[-1]
        self.seen.append((key, signed))
        if not signed:
            body = b"<Error><Code>AccessDenied</Code></Error>"
            self.send_response(403)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if with_body:
                self.wfile.write(body)
            return
        path = os.path.join(self.directory, key)
        if not os.path.isfile(path):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = Path(path).read_bytes()
        size = len(data)
        wanted = self.headers.get("Range")
        if wanted and wanted.startswith("bytes="):
            first, last = wanted[6:].split("-")
            first = int(first) if first else 0
            last = min(int(last) if last else size - 1, size - 1)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {first}-{last}/{size}")
            data = data[first:last + 1]
        else:
            self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if with_body:
            self.wfile.write(io.BytesIO(data).read())

    def do_HEAD(self):
        self._answer(with_body=False)

    def do_GET(self):
        self._answer(with_body=True)


@pytest.fixture
def store(lake, httpfs, monkeypatch):
    """The lake's files behind HTTP, readable with a signature only."""
    seen = []
    server = socketserver.ThreadingTCPServer(
        ("127.0.0.1", 0),
        functools.partial(_SignatureWanted, directory=str(lake.home), seen=seen))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}/bucket"
    _settings(monkeypatch, STORAGE_BUCKET="bucket",
              STORAGE_ENDPOINT_URL=f"http://127.0.0.1:{server.server_address[1]}",
              STORAGE_ACCESS_KEY="test-key", STORAGE_SECRET_KEY="test-secret")

    class Storage:
        asked = []

        def presign(self, key):
            self.asked.append(key)
            return f"{base}/{key}?sig=ok"

    yield SimpleNamespace(
        url=lambda key: f"{base}/{key}", seen=seen, storage=Storage(),
        unsigned=lambda key: (key, False) in seen, signed=lambda key: (key, True) in seen)
    server.shutdown()
    server.server_close()


def _stream(engine, lake, query, files, vector=None, cache_key=None, version=1):
    snapshot = SuperSnapshot(
        super_name="lake", simple_name="emp", simple_version=version,
        files=list(files), columns={"id", "region"},
        column_types={"id": "Int64", "region": "String"},
    )
    reflection = Reflection(
        storage_type="LOCAL", reflection_bytes=0, total_reflections=1,
        supers=[snapshot],
    )
    if vector:
        reflection.tombstone_views["emp"] = TombstoneDef(
            tombstone_path=vector, cache_key=cache_key)
    return engine.stream(
        reflection, SQLParser("lake", query, "duckdb"),
        SimpleNamespace(temp_dir=str(lake.home / "tmp" / "spill")), lambda *_: None,
        engine_config=RUNTIME, batch_rows=1,
    )


def _rows(handle):
    return pa.Table.from_batches(
        list(handle.batches()), schema=handle.schema).to_pylist()


# --------------------------------------------------------------------------
# 1. on a real DuckDB, against a store that answers 403 without a signature
# --------------------------------------------------------------------------

@pytest.mark.parametrize("cached", [True, False], ids=["cached vector", "inline vector"])
def test_a_table_with_a_deletion_vector_is_read_behind_the_fallback(lake, store, cached):
    """The whole read: data file refused, read signed; the vector's two parts
    read signed too; the two deleted rows stay deleted."""
    engine = engine_duckdb.DuckDBEngine(storage=store.storage)
    handle = _stream(
        engine, lake, "SELECT id, region FROM emp ORDER BY id",
        files=[store.url(DATA_KEY)],
        vector=[store.url(key) for key in VECTOR_KEYS],
        cache_key=VECTOR_KEYS[0] if cached else None)
    assert _rows(handle) == [{"id": 1, "region": "eu"}]
    assert store.unsigned(DATA_KEY) and store.signed(DATA_KEY)
    for key in VECTOR_KEYS:
        assert store.signed(key)
        # the data files had just been refused: the vector was not tried unsigned
        assert not store.unsigned(key)
    assert engine_duckdb._shared_state()["file_access"] == ec.FILE_ACCESS_REMOTE


@pytest.mark.parametrize("cached", [True, False], ids=["cached vector", "inline vector"])
def test_a_vector_refused_on_its_own_is_read_signed(lake, store, cached):
    """The data file is one DuckDB reads directly, the vector is not: the
    vector's own read is what is refused, and what falls back."""
    engine = engine_duckdb.DuckDBEngine(storage=store.storage)
    handle = _stream(
        engine, lake, "SELECT id FROM emp ORDER BY id",
        files=[DATA_KEY],
        vector=[store.url(key) for key in VECTOR_KEYS],
        cache_key=VECTOR_KEYS[0] if cached else None)
    assert _rows(handle) == [{"id": 1}]
    assert store.unsigned(VECTOR_KEYS[0])
    assert all(store.signed(key) for key in VECTOR_KEYS)


def test_the_second_read_finds_the_vector_it_cached(lake, store):
    engine = engine_duckdb.DuckDBEngine(storage=store.storage)
    files, vector = [store.url(DATA_KEY)], [store.url(key) for key in VECTOR_KEYS]
    for _ in range(2):
        assert _rows(_stream(engine, lake, "SELECT count(*) AS n FROM emp", files,
                             vector=vector, cache_key=VECTOR_KEYS[0])) == [{"n": 1}]
    assert store.seen.count((VECTOR_KEYS[1], True)) > 0
    requests_after_two = len([s for s in store.seen if s[0] in VECTOR_KEYS])
    assert _rows(_stream(engine, lake, "SELECT count(*) AS n FROM emp", files,
                         vector=vector, cache_key=VECTOR_KEYS[0])) == [{"n": 1}]
    # the cached table answers: no further request for a part
    assert len([s for s in store.seen if s[0] in VECTOR_KEYS]) == requests_after_two


def _open_views():
    con = engine_duckdb._shared_state()["con"]
    return sorted(name for (name,) in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall())


def test_the_read_that_fetches_the_vector_keeps_a_view_per_layer(lake, store):
    """The reflection and the tombstone layer are one view where nothing has
    to be read from the store between them.  Here something has: the vector's
    parts, signed or not according to what the data files met.  So this read
    binds the reflection first, by itself, as every read used to; the next
    one finds the vector's table and makes one view."""
    engine = engine_duckdb.DuckDBEngine(storage=store.storage)
    files, vector = [store.url(DATA_KEY)], [store.url(key) for key in VECTOR_KEYS]

    first = _stream(engine, lake, "SELECT id FROM emp", files, vector=vector,
                    cache_key=VECTOR_KEYS[0])
    views = _open_views()
    assert len(views) == 2
    assert views[0].startswith("st_") and views[1] == f"tomb_{views[0]}_{views[1][-8:]}"
    assert _rows(first) == [{"id": 1}]
    # the data file was refused once and the vector never asked for unsigned
    assert store.seen.count((DATA_KEY, False)) >= 1
    assert not any(store.unsigned(key) for key in VECTOR_KEYS)

    parts_asked = len([s for s in store.seen if s[0] in VECTOR_KEYS])
    refused_before = store.seen.count((DATA_KEY, False))
    second = _stream(engine, lake, "SELECT id FROM emp", files, vector=vector,
                     cache_key=VECTOR_KEYS[0])
    views = _open_views()
    assert len(views) == 1 and views[0].startswith("st_") and len(views[0]) == 3 + 16 + 1 + 8
    assert _rows(second) == [{"id": 1}]
    assert _open_views() == []
    # the one view fell back as the reflection does: refused unsigned, read signed
    assert store.seen.count((DATA_KEY, False)) > refused_before
    assert len([s for s in store.seen if s[0] in VECTOR_KEYS]) == parts_asked


def test_an_inline_vector_from_the_store_keeps_a_view_per_layer_every_time(lake, store):
    """No cache key: the parts are read by every read, so every read binds the
    reflection first."""
    engine = engine_duckdb.DuckDBEngine(storage=store.storage)
    files, vector = [store.url(DATA_KEY)], [store.url(key) for key in VECTOR_KEYS]
    for _ in range(2):
        handle = _stream(engine, lake, "SELECT id FROM emp", files, vector=vector)
        assert len(_open_views()) == 2
        assert _rows(handle) == [{"id": 1}]
    assert not any(store.unsigned(key) for key in VECTOR_KEYS)


def test_without_a_storage_to_sign_with_the_read_fails_and_brings_no_row_back(lake, store):
    engine = engine_duckdb.DuckDBEngine(storage=None)
    with pytest.raises(Exception, match="403|HTTP"):
        _rows(_stream(engine, lake, "SELECT id FROM emp", files=[DATA_KEY],
                      vector=[store.url(key) for key in VECTOR_KEYS],
                      cache_key=VECTOR_KEYS[0]))


# --------------------------------------------------------------------------
# 4. a storage prefix behind the fallback, on a real DuckDB
# --------------------------------------------------------------------------

@pytest.fixture
def prefixed(lake, store):
    """The same table under a storage's base prefix: the store holds
    ``<prefix>/<key>``, the storage writes the prefix into every path, and
    adds it to whatever key it is asked to sign."""
    shutil.copytree(lake.home / "acme", lake.home / PREFIX / "acme")

    class Storage:
        base_prefix = PREFIX
        bucket_name = "bucket"

        def __init__(self):
            self.asked = []

        def to_duckdb_path(self, key):
            return store.url(f"{self.base_prefix}/{key}")

        def presign(self, key):
            self.asked.append(key)
            return store.url(f"{self.base_prefix}/{key}") + "?sig=ok"

    return Storage()


@pytest.mark.parametrize("cached", [True, False], ids=["cached vector", "inline vector"])
def test_a_table_under_a_storage_prefix_is_read_behind_the_fallback(
        lake, store, prefixed, cached):
    engine = engine_duckdb.DuckDBEngine(storage=prefixed)
    handle = _stream(
        engine, lake, "SELECT id, region FROM emp ORDER BY id",
        files=[prefixed.to_duckdb_path(DATA_KEY)],
        vector=[prefixed.to_duckdb_path(key) for key in VECTOR_KEYS],
        cache_key=f"{PREFIX}/{VECTOR_KEYS[0]}" if cached else None)
    assert _rows(handle) == [{"id": 1, "region": "eu"}]
    # signed by the keys the paths were made from, each once
    assert prefixed.asked == [DATA_KEY, *VECTOR_KEYS]
    assert store.signed(f"{PREFIX}/{DATA_KEY}")
    assert all(store.signed(f"{PREFIX}/{key}") for key in VECTOR_KEYS)
    # and nothing was asked of the store under the prefix written twice
    assert not [key for key, _ in store.seen if key.startswith(f"{PREFIX}/{PREFIX}/")]


# --------------------------------------------------------------------------
# 3. SUPERTABLE_DUCKDB_MATERIALIZE=table
# --------------------------------------------------------------------------

def _reflection_tables(con=None):
    con = con or engine_duckdb._shared_state()["con"]
    return [name for (name,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'st\\_%' ESCAPE '\\'"
    ).fetchall()]


def _reflection_views(con=None):
    con = con or engine_duckdb._shared_state()["con"]
    return [name for (name,) in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()]


@pytest.fixture
def materialised(lake, monkeypatch):
    _settings(monkeypatch, SUPERTABLE_DUCKDB_MATERIALIZE="table")
    return lake


def test_a_table_is_read_twice_on_one_connection(materialised):
    engine = engine_duckdb.DuckDBEngine()
    for _ in range(3):
        assert _rows(_stream(engine, materialised, "SELECT id FROM emp ORDER BY id",
                             [DATA_KEY])) == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_a_read_leaves_no_table_and_no_view_behind(materialised):
    engine = engine_duckdb.DuckDBEngine()
    _rows(_stream(engine, materialised, "SELECT id FROM emp", [DATA_KEY]))
    assert _reflection_tables() == []
    assert _reflection_views() == []


def test_the_reflection_is_a_table_while_the_read_is_open(materialised):
    engine = engine_duckdb.DuckDBEngine()
    handle = _stream(engine, materialised, "SELECT id FROM emp", [DATA_KEY])
    tables = _reflection_tables()
    assert len(tables) == 1
    handle.close()
    assert _reflection_tables() == []


def test_two_reads_of_one_table_open_at_once_do_not_share_a_table(materialised):
    """Each read has its own copy: closing the first takes nothing from the
    second, which reads to its end."""
    engine = engine_duckdb.DuckDBEngine()
    first = _stream(engine, materialised, "SELECT id FROM emp ORDER BY id", [DATA_KEY])
    second = _stream(engine, materialised, "SELECT id FROM emp ORDER BY id", [DATA_KEY])
    assert len(set(_reflection_tables())) == 2
    assert _rows(first) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert len(_reflection_tables()) == 1
    assert _rows(second) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert _reflection_tables() == []


def test_a_table_joined_with_itself_is_materialised_once(materialised):
    """Two aliases of one table with the same columns have one reflection."""
    engine = engine_duckdb.DuckDBEngine()
    query = "SELECT a.id FROM emp a JOIN emp b ON a.id = b.id ORDER BY a.id"
    handle = _stream(engine, materialised, query, [DATA_KEY])
    assert len(_reflection_tables()) == 1
    assert _rows(handle) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert _rows(_stream(engine, materialised, query, [DATA_KEY])) == [
        {"id": 1}, {"id": 2}, {"id": 3}]
    assert _reflection_tables() == []


def test_a_materialised_read_applies_the_deletion_vector(materialised):
    engine = engine_duckdb.DuckDBEngine()
    for cache_key in (VECTOR_KEYS[0], None):
        assert _rows(_stream(engine, materialised, "SELECT id FROM emp ORDER BY id",
                             [DATA_KEY], vector=list(VECTOR_KEYS),
                             cache_key=cache_key)) == [{"id": 1}]
    assert _reflection_tables() == []


def test_a_query_that_fails_leaves_no_table_behind(materialised):
    engine = engine_duckdb.DuckDBEngine()
    with pytest.raises(Exception):
        _rows(_stream(engine, materialised, "SELECT id / 'x' AS broken FROM emp", [DATA_KEY]))
    assert _reflection_tables() == []
    # and the table is read again afterwards
    assert _rows(_stream(engine, materialised, "SELECT count(*) AS n FROM emp",
                         [DATA_KEY])) == [{"n": 3}]


def test_an_empty_table_is_read_twice_materialised(materialised):
    engine = engine_duckdb.DuckDBEngine()
    for _ in range(2):
        assert _rows(_stream(engine, materialised, "SELECT count(*) AS n FROM emp",
                             files=[])) == [{"n": 0}]
    assert _reflection_tables() == []


def test_the_default_is_still_a_view_under_the_reflections_own_name(lake):
    engine = engine_duckdb.DuckDBEngine()
    handle = _stream(engine, lake, "SELECT id FROM emp", [DATA_KEY])
    assert _reflection_tables() == []
    views = _reflection_views()
    assert any(name.startswith("st_") for name in views)
    handle.close()
    assert _reflection_views() == []
