"""The read connection's file-access lock, behind a real reader.

``test_read_gate_end_to_end.py`` shows the gate refusing a query that asks for
a file. This file is about what stands behind the gate: the connection the
engine runs a query on. A table is written and read with the real writer,
reader and RBAC on LOCAL storage; then SQL is put straight onto the engine's
connection, which is what a query the gate failed to refuse would amount to,
and DuckDB itself is seen to refuse the file.

``supertable/engine/tests/test_duckdb_file_access_lock.py`` checks the lock
form by form, the object-store URL forms included.
"""

from __future__ import annotations

import os
import threading
import uuid
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest

from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine import engine_common
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``region`` of ``emp``. Not ``salary``.
READER = "two_columns"

#: What the file outside the lake holds.
SENTINEL = "OUTSIDE-THE-LAKE-9d2e"


@pytest.fixture
def secret(tmp_path):
    path = tmp_path / "secret.txt"
    path.write_text(SENTINEL + "\n")
    return str(path)


@pytest.fixture
def lake(hermetic_fakeredis):
    """A table with a deletion vector and a role that reads two columns of it."""
    assert settings.SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK is True
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"lock_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3], "region": ["eu", "us", "eu"],
                           "salary": [40_000, 900_000, 55_000]}))
    # an overwrite by key: the first row for id 2 goes into the deletion vector
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
        data=pl.DataFrame({"id": [2], "region": ["apac"], "salary": [910_000]}))
    RoleManager(super_name=sup, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": READER, "role": "reader", "tables": {
            "emp": {"columns": ["id", "region"], "filters": ["*"]}}})

    def read(sql: str, role: str = READER):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    yield SimpleNamespace(sup=sup, read=read)
    engine_duckdb.reset_shared_duckdb_state()


def _connection():
    state = engine_duckdb._shared_state()
    assert state["con"] is not None
    return state["con"], state["file_access"]


def test_the_lake_is_read_on_a_locked_connection(lake):
    assert lake.read("SELECT id, region FROM emp ORDER BY id") == [
        {"id": 1, "region": "eu"}, {"id": 2, "region": "apac"}, {"id": 3, "region": "eu"}]
    con, file_access = _connection()
    assert file_access == engine_common.FILE_ACCESS_LOCAL
    assert con.execute(
        "SELECT current_setting('enable_external_access')").fetchone()[0] is False

    # and again on the connection now that it is locked: an aggregate, the
    # table's owner, a join of the table with itself
    assert lake.read("SELECT count(*) AS n FROM emp") == [{"n": 3}]
    assert lake.read("SELECT sum(salary) AS total FROM emp", role=ADMIN) == [
        {"total": 1_005_000}]
    assert lake.read(
        "SELECT a.id FROM emp a JOIN emp b ON a.id = b.id WHERE b.region = 'apac'"
    ) == [{"id": 2}]
    assert _connection()[0] is con


@pytest.mark.parametrize("sql", [
    "SELECT content FROM read_text('{secret}')",
    "SELECT size FROM read_blob('{secret}')",
    "SELECT * FROM read_csv('{secret}', header = false)",
    "SELECT count(*) FROM glob('{secret}')",
    "COPY (SELECT 1) TO '{secret}.copy'",
    "ATTACH '{secret}.db' AS other",
])
def test_the_connection_itself_refuses_a_file_outside_the_lake(lake, secret, sql):
    """A gate bypass, simulated: nothing parses or judges this SQL, it is run
    on the engine's connection as the engine would run a rewritten query."""
    lake.read("SELECT id FROM emp")
    con, _ = _connection()
    for target in (con, con.cursor()):
        with pytest.raises(duckdb.PermissionException):
            target.execute(sql.format(secret=secret)).fetchall()
    assert not os.path.exists(secret + ".copy")
    assert not os.path.exists(secret + ".db")


@pytest.mark.parametrize("path", ["/etc/hostname", "/proc/self/environ"])
def test_the_connection_itself_refuses_the_machines_files(lake, path):
    if not os.path.exists(path):
        pytest.skip(f"{path} does not exist here")
    lake.read("SELECT id FROM emp")
    con, _ = _connection()
    for target in (con, con.cursor()):
        with pytest.raises(duckdb.PermissionException):
            target.execute(f"SELECT content FROM read_text('{path}')").fetchall()


def test_the_file_is_there_to_be_read(lake, secret):
    """Why the refusals above mean something: a connection that is not the
    engine's reads the file when asked."""
    assert duckdb.connect().execute(
        "SELECT content FROM read_text(?)", [secret]).fetchone()[0].strip() == SENTINEL


def test_each_threads_connection_is_locked(lake, secret):
    """The engine keeps one connection per thread; each is locked by its own
    first query."""
    lake.read("SELECT id FROM emp")
    main_con, _ = _connection()
    seen = {}

    def other():
        try:
            seen["rows"] = lake.read("SELECT id FROM emp ORDER BY id")
            con, seen["file_access"] = _connection()
            seen["same"] = con is main_con
            try:
                con.execute(f"SELECT content FROM read_text('{secret}')").fetchall()
                seen["outside"] = "read"
            except duckdb.PermissionException:
                seen["outside"] = "refused"
        finally:
            engine_duckdb.reset_shared_duckdb_state()

    thread = threading.Thread(target=other)
    thread.start()
    thread.join()
    assert seen == {"rows": [{"id": 1}, {"id": 2}, {"id": 3}],
                    "file_access": engine_common.FILE_ACCESS_LOCAL,
                    "same": False, "outside": "refused"}


def test_what_the_lock_does_not_do(lake):
    """It is not the gate, and it is not RBAC. On the engine's connection the
    lake's own files are read as they lie: the columns a role may not see, the
    system columns and the rows the deletion vector hides. Keeping a query
    from saying this is the gate's work, and stays the gate's."""
    lake.read("SELECT id FROM emp")
    con, _ = _connection()
    files = [name for (name,) in con.execute(
        f"SELECT file FROM glob('{ORG}/{lake.sup}/tables/emp/data/**/*.parquet')"
    ).fetchall()]
    assert files
    rows = con.execute(
        "SELECT id, salary FROM read_parquet(" + repr(files) + ", union_by_name = true) "
        "ORDER BY id, salary").fetchall()
    assert rows == [(1, 40_000), (2, 900_000), (2, 910_000), (3, 55_000)]
    assert os.path.realpath(files[0]).startswith(os.path.realpath(get_app_home()))
