# route: supertable.engine.tests.test_duckdb_file_access_lock
"""The read connection opens the lake's files and its store's URLs, nothing else.

``lock_file_access`` sets DuckDB's ``allowed_directories`` and then
``enable_external_access=false`` on the engine's connection. It stands behind
the SQL read gate: these tests put SQL straight onto the connection, which is
what a query that got past the gate would amount to, and look at what DuckDB
itself does with it.

Everything runs on a real DuckDB. The local half needs nothing else. The URL
half needs DuckDB's httpfs extension, linked into a temporary application home
from wherever this machine keeps it, and reaches no store: every endpoint is a
closed local port, so a read that DuckDB lets through fails to connect, and
one it refuses fails with a permission error before any socket is opened.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from types import SimpleNamespace

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


SENTINEL = "OUTSIDE-THE-LAKE-41c9"

RUNTIME = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=7, duckdb_external_cache_size="",
)


def _settings(monkeypatch, **changes):
    values = dict(
        STORAGE_TYPE="LOCAL",
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


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """An application home with one Parquet file in it, and a file outside it.

    The working directory is the home, as the package makes it at import: a
    LOCAL lake's keys are paths relative to it.
    """
    home = tmp_path / "home"
    data = home / "acme" / "lake" / "tables" / "emp" / "data"
    data.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text(SENTINEL + "\n")
    writer = duckdb.connect()
    writer.execute(
        f"COPY (SELECT 1 AS id, 'eu' AS region, 1 AS __rowid__, 0 AS __timestamp__) "
        f"TO '{data}/part-0.parquet' (FORMAT parquet)")
    writer.execute(
        f"COPY (SELECT 7 AS id) TO '{outside}/rows.parquet' (FORMAT parquet)")
    writer.close()

    monkeypatch.setattr(ec, "get_app_home", lambda: str(home))
    monkeypatch.chdir(home)
    _settings(monkeypatch)
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()
    yield SimpleNamespace(
        home=home, outside=outside, tmp=tmp_path,
        key="acme/lake/tables/emp/data/part-0.parquet",
        secret=str(outside / "secret.txt"),
    )
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


def _connection(lake, spill=None):
    con = duckdb.connect()
    ec.init_connection(con, temp_dir=str(spill or lake.home / "tmp" / "spill"))
    return con


def _listed(con):
    """What DuckDB holds as the connection's allowed directories.

    DuckDB adds the spill directory itself when external access is turned
    off, so it is there whether or not the home already covers it."""
    return con.execute("SELECT current_setting('allowed_directories')").fetchone()[0]


def _refused(con, sql):
    with pytest.raises(duckdb.PermissionException):
        con.execute(sql).fetchall()


def _goes_to_the_network(con, sql):
    """DuckDB let the path through: what fails is the closed port."""
    with pytest.raises(duckdb.IOException, match="Could not connect"):
        con.execute(sql).fetchall()


# --------------------------------------------------------------------------
# LOCAL storage: the application home, and no URL
# --------------------------------------------------------------------------

def test_a_local_lake_is_read_and_the_connection_is_locked_to_it(lake):
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    # the lake's keys as LOCAL storage hands them over, and as absolute paths
    assert con.execute(
        f"SELECT id, region FROM parquet_scan(['{lake.key}'])").fetchall() == [(1, "eu")]
    assert con.execute(
        f"SELECT id FROM parquet_scan(['{lake.home}/{lake.key}'])").fetchall() == [(1,)]
    assert con.execute(
        "SELECT current_setting('enable_external_access')").fetchone()[0] is False


@pytest.mark.parametrize("sql", [
    "SELECT content FROM read_text('{secret}')",
    "SELECT size FROM read_blob('{secret}')",
    "SELECT * FROM read_csv('{secret}', header = false)",
    "SELECT count(*) FROM glob('{outside}/*')",
    "SELECT * FROM parquet_scan(['{outside}/rows.parquet'])",
    "SELECT content FROM read_text('{home}/../outside/secret.txt')",
    "SELECT content FROM read_text('../outside/secret.txt')",
    "SELECT content FROM read_text('acme/../../outside/secret.txt')",
    "SELECT content FROM read_text('file://{secret}')",
    "COPY (SELECT 1) TO '{outside}/written.csv'",
    "ATTACH '{outside}/attached.db' AS other",
    "EXPORT DATABASE '{outside}/export'",
    "LOAD httpfs",
    "LOAD '{outside}/planted.duckdb_extension'",
])
def test_the_locked_connection_refuses_what_lies_outside_the_lake(lake, sql):
    """The gate bypass, simulated: the SQL is run on the connection itself."""
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    _refused(con, sql.format(secret=lake.secret, outside=lake.outside, home=lake.home))
    assert sorted(p.name for p in lake.outside.iterdir()) == ["rows.parquet", "secret.txt"]


@pytest.mark.parametrize("path", ["/etc/hostname", "/proc/self/environ"])
def test_the_locked_connection_refuses_the_machines_own_files(lake, path):
    if not os.path.exists(path):
        pytest.skip(f"{path} does not exist here")
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    _refused(con, f"SELECT content FROM read_text('{path}')")
    _refused(con.cursor(), f"SELECT content FROM read_text('{path}')")


def test_the_file_outside_is_there_to_be_read(lake):
    """Why the refusals mean something: an unlocked connection reads it."""
    con = _connection(lake)
    assert con.execute(
        f"SELECT content FROM read_text('{lake.secret}')"
    ).fetchone()[0].strip() == SENTINEL


def test_a_cursor_of_the_connection_is_locked_with_it(lake):
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    cursor = con.cursor()
    _refused(cursor, f"SELECT content FROM read_text('{lake.secret}')")
    assert cursor.execute(f"SELECT id FROM parquet_scan(['{lake.key}'])").fetchall() == [(1,)]


def test_a_view_made_before_the_lock_does_not_read_outside_after_it(lake):
    con = _connection(lake)
    con.execute(
        f"CREATE VIEW planted AS SELECT * FROM parquet_scan(['{lake.outside}/rows.parquet'])")
    ec.lock_file_access(con, [lake.key])
    _refused(con, "SELECT * FROM planted")


def test_a_link_in_the_lake_does_not_lead_out_of_it(lake):
    (lake.home / "acme" / "way_out").symlink_to(lake.outside)
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    _refused(con, "SELECT content FROM read_text('acme/way_out/secret.txt')")


def test_a_home_reached_through_a_link_is_still_the_home(lake, monkeypatch):
    """SUPERTABLE_HOME may be a link to a mounted volume; the working
    directory the operating system reports is then the volume's own path."""
    link = lake.tmp / "home_link"
    link.symlink_to(lake.home)
    monkeypatch.setattr(ec, "get_app_home", lambda: str(link))
    monkeypatch.chdir(link)
    con = _connection(lake, spill=link / "tmp" / "spill")
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    assert con.execute(f"SELECT id FROM parquet_scan(['{lake.key}'])").fetchall() == [(1,)]
    assert con.execute(
        f"SELECT id FROM parquet_scan(['{link}/{lake.key}'])").fetchall() == [(1,)]
    _refused(con, f"SELECT content FROM read_text('{lake.secret}')")


def test_the_lock_cannot_be_undone_or_widened(lake):
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    for sql in ("SET enable_external_access=true",
                "RESET enable_external_access",
                "SET allowed_directories=['/']",
                f"SET allowed_paths=['{lake.secret}']",
                f"SET temp_directory='{lake.outside}'"):
        with pytest.raises(duckdb.Error):
            con.execute(sql)
    _refused(con, f"SELECT content FROM read_text('{lake.secret}')")


def test_what_the_engine_sets_per_query_can_still_be_set(lake):
    """Why ``lock_configuration`` is not used."""
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    con.execute("SET threads=5")
    ec.apply_runtime_pragmas(con, RUNTIME)
    assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 2
    con.execute("PRAGMA memory_limit='300MB'")
    con.execute("SET enable_external_file_cache=false")


def test_the_connection_still_spills_and_builds_its_tables(lake):
    """A spill directory outside the home is on the list beside it, and the
    deletion-vector cache's CREATE TABLE AS reads the lake as a view does."""
    spill = lake.tmp / "spill_elsewhere"
    con = _connection(lake, spill=spill)
    ec.lock_file_access(con, [lake.key])
    con.execute(f"COPY (SELECT 1) TO '{spill}/probe.csv'")
    con.execute(
        f"CREATE TABLE dv AS SELECT DISTINCT __rowid__ FROM read_parquet(['{lake.key}'])")
    assert con.execute("SELECT * FROM dv").fetchall() == [(1,)]
    con.execute("PRAGMA memory_limit='64MB'")
    assert con.execute(
        "SELECT count(*) FROM (SELECT range AS a, md5(range::varchar) AS b "
        "FROM range(1500000) ORDER BY b)").fetchone()[0] == 1500000


def test_locking_twice_changes_nothing(lake):
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    assert con.execute(f"SELECT id FROM parquet_scan(['{lake.key}'])").fetchall() == [(1,)]


def test_the_setting_turns_the_lock_off(lake, monkeypatch):
    _settings(monkeypatch, SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK=False)
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_OPEN
    assert con.execute(
        "SELECT current_setting('enable_external_access')").fetchone()[0] is True
    assert con.execute(
        f"SELECT content FROM read_text('{lake.secret}')"
    ).fetchone()[0].strip() == SENTINEL


def test_a_local_lock_reads_no_url(lake):
    """Without httpfs there is no scheme on the list and none can be loaded."""
    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    assert _listed(con) == [str(lake.home) + "/", str(lake.home) + "/tmp/spill/"]
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_parquet(['http://127.0.0.1:1/b/k.parquet'])")
    assert ec._httpfs_loaded(con) is False


def test_a_failure_to_lock_is_raised_not_passed_over(lake):
    """A connection is locked as designed or the read does not run on it."""
    con = _connection(lake)
    con.close()
    with pytest.raises(RuntimeError, match="SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK=false"):
        ec.lock_file_access(con, [lake.key])


def test_a_lake_kept_outside_the_home_is_named_in_the_setting(lake, monkeypatch):
    """Snapshots that name their files by absolute path, or a process that
    changed its working directory: the directory is listed beside the home,
    and nothing else comes with it."""
    elsewhere = lake.tmp / "lake_elsewhere"
    elsewhere.mkdir()
    duckdb.connect().execute(
        f"COPY (SELECT 5 AS id) TO '{elsewhere}/part-0.parquet' (FORMAT parquet)")
    read = f"SELECT id FROM parquet_scan(['{elsewhere}/part-0.parquet'])"

    con = _connection(lake)
    ec.lock_file_access(con, [lake.key])
    _refused(con, read)

    _settings(monkeypatch, SUPERTABLE_DUCKDB_FILE_ACCESS_DIRECTORIES=os.pathsep.join(
        [str(elsewhere), str(lake.home / "acme")]))
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    assert con.execute(read).fetchall() == [(5,)]
    assert con.execute(f"SELECT id FROM parquet_scan(['{lake.key}'])").fetchall() == [(1,)]
    _refused(con, f"SELECT content FROM read_text('{lake.secret}')")
    # a directory named in the setting that the home already covers adds nothing
    assert _listed(con) == sorted([
        str(lake.home) + "/", str(lake.home) + "/tmp/spill/", str(elsewhere) + "/"])


# --------------------------------------------------------------------------
# Object stores: the home, and the URL schemes the read path resolves keys to
# --------------------------------------------------------------------------

S3_SECRET = ("CREATE OR REPLACE TEMPORARY SECRET s3_closed (TYPE s3, KEY_ID 'k', "
             "SECRET 's', ENDPOINT '127.0.0.1:1', URL_STYLE 'path', USE_SSL FALSE)")
GCS_SECRET = ("CREATE OR REPLACE TEMPORARY SECRET gcs_closed (TYPE gcs, KEY_ID 'k', "
              "SECRET 's', ENDPOINT '127.0.0.1:1', USE_SSL FALSE)")

#: One URL of every form a storage class or the estimator resolves a key to.
STORE_FORMS = {
    "S3/MinIO key": "s3://lake/acme/t/data/part-0.parquet",
    "S3/MinIO key over HTTP, path style": "http://127.0.0.1:1/lake/acme/t/data/part-0.parquet",
    "S3 key over HTTPS, vhost style": "https://lake.localhost:1/acme/t/data/part-0.parquet",
    "S3/MinIO presigned GET": (
        "http://127.0.0.1:1/lake/acme/t/data/part-0.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=k%2F20261117%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host&X-Amz-Signature=0f0f"),
    "Azure blob over HTTPS": "https://127.0.0.1:1/container/acme/t/data/part-0.parquet",
    "Azure SAS URL": (
        "https://127.0.0.1:1/container/acme/t/data/part-0.parquet"
        "?se=2026-11-17T12%3A00%3A00Z&sp=r&sv=2025-11-05&sr=b&sig=0f0f%3D"),
    "GCS key": "gcs://lake/acme/t/data/part-0.parquet",
    "GCS signed URL": (
        "https://127.0.0.1:1/lake/acme/t/data/part-0.parquet?X-Goog-Algorithm=GOOG4-RSA-SHA256"
        "&X-Goog-Credential=sa%40p.iam.gserviceaccount.com%2F20261117%2Fauto%2Fstorage%2Fgoog4_request"
        "&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=0f0f"),
}


def _remote_connection(lake, monkeypatch, paths):
    _settings(monkeypatch, STORAGE_TYPE="MINIO", STORAGE_ENDPOINT_URL="http://127.0.0.1:1",
              STORAGE_ACCESS_KEY="test-key", STORAGE_SECRET_KEY="test-secret")
    con = _connection(lake)
    ec.configure_httpfs_and_s3(con, paths)
    assert ec.lock_file_access(con, paths) == ec.FILE_ACCESS_REMOTE
    return con


def test_an_object_store_lock_lists_the_home_and_the_schemes(lake, httpfs, monkeypatch):
    con = _remote_connection(lake, monkeypatch, [STORE_FORMS["S3/MinIO key"]])
    assert _listed(con) == [str(lake.home) + "/", str(lake.home) + "/tmp/spill/",
                            "gcs://", "http://", "https://", "s3://"]


@pytest.mark.parametrize("form", sorted(STORE_FORMS))
def test_every_store_form_passes_the_lock(lake, httpfs, monkeypatch, form):
    """DuckDB goes to the network for it: the refusal it gets is the closed
    port's, not the lock's."""
    con = _remote_connection(lake, monkeypatch, [STORE_FORMS["S3/MinIO key"]])
    con.execute(S3_SECRET)
    con.execute(GCS_SECRET)
    _goes_to_the_network(con, f"SELECT * FROM parquet_scan(['{STORE_FORMS[form]}'])")


def test_a_scheme_duckdb_does_not_keep_as_a_prefix_stops_the_lock(lake, httpfs, monkeypatch):
    """DuckDB keeps a scheme only while an extension that serves it is loaded
    and takes anything else for a directory under the working directory. A
    list that did not go in as written is not locked on."""
    _settings(monkeypatch, STORAGE_TYPE="MINIO", STORAGE_ENDPOINT_URL="http://127.0.0.1:1",
              STORAGE_ACCESS_KEY="test-key", STORAGE_SECRET_KEY="test-secret")
    monkeypatch.setattr(ec, "FILE_ACCESS_URL_SCHEMES", ("s3://", "azure://"))
    con = _connection(lake)
    with pytest.raises(RuntimeError, match=r"did not keep \['azure://'\]"):
        ec.lock_file_access(con, [STORE_FORMS["S3/MinIO key"]])
    assert con.execute(
        "SELECT current_setting('enable_external_access')").fetchone()[0] is True


def test_an_object_store_lock_still_refuses_local_files_and_other_schemes(
        lake, httpfs, monkeypatch):
    con = _remote_connection(lake, monkeypatch, [STORE_FORMS["S3/MinIO key"]])
    _refused(con, f"SELECT content FROM read_text('{lake.secret}')")
    _refused(con.cursor(), f"SELECT content FROM read_text('{lake.secret}')")
    _refused(con, "SELECT * FROM read_parquet(['hf://datasets/a/b.parquet'])")
    _refused(con, f"COPY (SELECT 1) TO '{lake.outside}/written.csv'")
    # and the home is still read, for the spill directory and a LOCAL table
    assert con.execute(f"SELECT id FROM parquet_scan(['{lake.key}'])").fetchall() == [(1,)]


def test_a_gcs_key_first_loads_httpfs_and_the_store_is_configured_after_the_lock(
        lake, httpfs, monkeypatch):
    """``configure_httpfs_and_s3`` passes over a ``gcs://`` path, so on GCS the
    lock comes first and the store's secret second, when the anonymous read is
    refused and the retry brings signed HTTPS URLs. That order has to work:
    httpfs is loaded by the lock, and DuckDB's secret manager, which would
    otherwise start under the lock by listing a directory outside the list, is
    started by it."""
    _settings(monkeypatch, STORAGE_TYPE="GCS", STORAGE_ACCESS_KEY="test-key",
              STORAGE_SECRET_KEY="test-secret", STORAGE_ENDPOINT_URL="http://127.0.0.1:1")
    con = _connection(lake)
    ec.configure_httpfs_and_s3(con, [STORE_FORMS["GCS key"]])
    assert ec._httpfs_loaded(con) is False
    assert ec.lock_file_access(con, [STORE_FORMS["GCS key"]]) == ec.FILE_ACCESS_REMOTE
    assert ec._httpfs_loaded(con) is True

    ec.configure_httpfs_and_s3(con, [STORE_FORMS["GCS signed URL"]])
    assert con.execute(
        "SELECT name FROM which_secret('s3://lake/x.parquet', 's3')"
    ).fetchall() == [(ec._S3_SECRET_NAME,)]
    _goes_to_the_network(
        con, f"SELECT * FROM parquet_scan(['{STORE_FORMS['GCS signed URL']}'])")


def test_an_object_store_gets_httpfs_before_a_local_first_query_locks(
        lake, httpfs, monkeypatch):
    """The connection's first query may read no URL; its later ones will."""
    _settings(monkeypatch, STORAGE_TYPE="S3")
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_REMOTE
    con.execute(S3_SECRET)
    _goes_to_the_network(
        con, f"SELECT * FROM parquet_scan(['{STORE_FORMS['S3/MinIO key']}'])")


def test_httpfs_is_found_where_duckdb_itself_auto_loads_it_from(lake, httpfs, monkeypatch):
    """DuckDB auto-loads from the operating-system home whatever
    ``home_directory`` says, and a ``gcs://`` read used to come by httpfs that
    way. Under the lock nothing auto-loads, so the lock looks there too."""
    seeded = lake.home / ".duckdb" / "extensions"
    os_home = lake.tmp / "os_home"
    (os_home / ".duckdb").mkdir(parents=True)
    seeded.rename(os_home / ".duckdb" / "extensions")
    monkeypatch.setenv("HOME", str(os_home))
    _settings(monkeypatch, STORAGE_TYPE="GCS")
    con = _connection(lake)
    with pytest.raises(duckdb.Error):
        con.execute("LOAD httpfs")
    assert ec.lock_file_access(con, [STORE_FORMS["GCS key"]]) == ec.FILE_ACCESS_REMOTE
    assert ec._httpfs_loaded(con) is True


def test_a_url_without_httpfs_is_the_querys_error_and_nothing_is_locked(lake, monkeypatch):
    """No httpfs anywhere: the query that needs it says so, and the connection
    is left as it was for the next one."""
    monkeypatch.setenv("HOME", str(lake.tmp / "no_such_home"))
    con = _connection(lake)
    with pytest.raises(RuntimeError, match="httpfs"):
        ec.lock_file_access(con, [STORE_FORMS["GCS key"]])
    assert con.execute(
        "SELECT current_setting('enable_external_access')").fetchone()[0] is True


def test_an_object_store_without_httpfs_locks_for_local_files(lake, monkeypatch):
    monkeypatch.setenv("HOME", str(lake.tmp / "no_such_home"))
    _settings(monkeypatch, STORAGE_TYPE="MINIO")
    con = _connection(lake)
    assert ec.lock_file_access(con, [lake.key]) == ec.FILE_ACCESS_LOCAL
    _refused(con, f"SELECT content FROM read_text('{lake.secret}')")


# --------------------------------------------------------------------------
# Through the engine
# --------------------------------------------------------------------------

def _stream(engine, lake, query, files, tombstone=None):
    snapshot = SuperSnapshot(
        super_name="lake", simple_name="emp", simple_version=1,
        files=list(files), columns={"id", "region"},
    )
    reflection = Reflection(
        storage_type="LOCAL", reflection_bytes=0, total_reflections=1,
        supers=[snapshot],
    )
    if tombstone:
        reflection.tombstone_views["emp"] = TombstoneDef(
            tombstone_path=tombstone, cache_key=None)
    return engine.stream(
        reflection, SQLParser("lake", query, "duckdb"),
        SimpleNamespace(temp_dir=str(lake.home / "tmp" / "spill")), lambda *_: None,
        engine_config=RUNTIME, batch_rows=1,
    )


def _rows(handle):
    return pa.Table.from_batches(
        list(handle.batches()), schema=handle.schema).to_pylist()


def test_the_engines_connection_is_locked_by_its_first_query(lake):
    engine = engine_duckdb.DuckDBEngine()
    assert _rows(_stream(engine, lake, "SELECT id, region FROM emp", [lake.key])) == [
        {"id": 1, "region": "eu"}]
    state = engine_duckdb._shared_state()
    assert state["file_access"] == ec.FILE_ACCESS_LOCAL
    _refused(state["con"], f"SELECT content FROM read_text('{lake.secret}')")
    # the second query finds it locked and runs
    assert _rows(_stream(engine, lake, "SELECT count(*) AS n FROM emp", [lake.key])) == [
        {"n": 1}]
    assert engine_duckdb._shared_state()["con"] is state["con"]


def test_a_connection_made_after_a_reset_is_locked_again(lake):
    engine = engine_duckdb.DuckDBEngine()
    _rows(_stream(engine, lake, "SELECT id FROM emp", [lake.key]))
    first = engine_duckdb._shared_state()["con"]
    engine._reset_connection()
    assert engine_duckdb._shared_state()["file_access"] is None
    _rows(_stream(engine, lake, "SELECT id FROM emp", [lake.key]))
    state = engine_duckdb._shared_state()
    assert state["con"] is not first and state["file_access"] == ec.FILE_ACCESS_LOCAL
    _refused(state["con"], f"SELECT content FROM read_text('{lake.secret}')")


def test_the_engine_reads_unlocked_when_the_setting_is_off(lake, monkeypatch):
    _settings(monkeypatch, SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK=False)
    engine = engine_duckdb.DuckDBEngine()
    assert _rows(_stream(engine, lake, "SELECT id FROM emp", [lake.key])) == [{"id": 1}]
    state = engine_duckdb._shared_state()
    assert state["file_access"] == ec.FILE_ACCESS_OPEN
    assert state["con"].execute(
        f"SELECT content FROM read_text('{lake.secret}')"
    ).fetchone()[0].strip() == SENTINEL


def test_a_connection_locked_for_local_files_is_replaced_when_a_url_comes(
        lake, httpfs, monkeypatch):
    """An extension cannot be loaded into a locked connection, so a query
    that brings the first URL gets a new one. A stream still open on the old
    connection reads to its end."""
    engine = engine_duckdb.DuckDBEngine()
    open_stream = _stream(engine, lake, "SELECT id FROM emp", [lake.key])
    local = engine_duckdb._shared_state()["con"]
    assert engine_duckdb._shared_state()["file_access"] == ec.FILE_ACCESS_LOCAL

    # the URL read itself is a one-row local relation: no store is reached
    monkeypatch.setattr(
        ec, "create_reflection_view",
        lambda con, name, files, columns=None, column_types=None: con.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT 2 AS id, 'us' AS region, "
            f"2 AS __rowid__, 0 AS __timestamp__"))
    _settings(monkeypatch, STORAGE_ENDPOINT_URL="http://127.0.0.1:1",
              STORAGE_ACCESS_KEY="test-key", STORAGE_SECRET_KEY="test-secret")
    assert _rows(_stream(engine, lake, "SELECT id FROM emp",
                         [STORE_FORMS["S3/MinIO key"]])) == [{"id": 2}]
    state = engine_duckdb._shared_state()
    assert state["con"] is not local
    assert state["file_access"] == ec.FILE_ACCESS_REMOTE
    _refused(state["con"], f"SELECT content FROM read_text('{lake.secret}')")

    assert _rows(open_stream) == [{"id": 1}]


def test_a_deletion_vectors_url_counts_as_the_querys_path(lake, httpfs, monkeypatch):
    """The parts of a deletion vector are resolved as the data files are, and
    are read by the same connection: the lock has to see them too."""
    seen = {}
    real = ec.lock_file_access

    def spy(con, for_paths=None):
        seen["paths"] = list(for_paths or [])
        return real(con, for_paths)

    monkeypatch.setattr(engine_duckdb, "lock_file_access", spy)
    monkeypatch.setattr(engine_duckdb, "create_tombstone_view",
                        lambda con, source, view, *a, **k: con.execute(
                            f"CREATE OR REPLACE VIEW {view} AS SELECT id FROM {source}"))
    engine = engine_duckdb.DuckDBEngine()
    vector = "http://127.0.0.1:1/lake/acme/t/tombstone/dv-0.parquet"
    _settings(monkeypatch, STORAGE_ENDPOINT_URL="http://127.0.0.1:1",
              STORAGE_ACCESS_KEY="test-key", STORAGE_SECRET_KEY="test-secret")
    assert _rows(_stream(engine, lake, "SELECT id FROM emp", [lake.key],
                         tombstone=[vector])) == [{"id": 1}]
    assert seen["paths"] == [lake.key, vector]
    assert engine_duckdb._shared_state()["file_access"] == ec.FILE_ACCESS_REMOTE
