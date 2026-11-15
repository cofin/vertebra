"""Unit tests for standalone PEP 249 interfaces in vertebra.db.dbapi."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import vertebra.db.dbapi as dbapi_pkg
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.db.dbapi import (
    AsyncDBAPIConnection,
    AsyncDBAPIModule,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
    SyncDBAPIConnection,
    SyncDBAPIModule,
    apilevel,
    connect,
    is_row_returning_sql,
    paramstyle,
    threadsafety,
)
from vertebra.db.registry import DatabaseRegistry

_DBAPI_DIR = Path(__file__).resolve().parents[3] / "vertebra" / "db" / "dbapi"


def test_dbapi_standalone_imports_without_sqlalchemy_or_extensions() -> None:
    """Verify vertebra.db.dbapi stays lazy and imports cleanly without sqlalchemy or vertebra.extensions."""
    script = (
        "import sys\n"
        "import vertebra.db.dbapi as dbapi\n"
        "assert 'vertebra.db.dbapi._sync' not in sys.modules\n"
        "assert 'vertebra.db.dbapi._async' not in sys.modules\n"
        "conn = dbapi.SyncDBAPIConnection('sqlite+vtb:///:memory:', share_pool=False)\n"
        "cur = conn.cursor()\n"
        "cur.execute('SELECT 42 AS answer')\n"
        "assert cur.fetchone() == (42,)\n"
        "conn.close()\n"
        "assert 'sqlalchemy' not in sys.modules\n"
        "ext_mods = [m for m in sys.modules if m == 'vertebra.extensions' or m.startswith('vertebra.extensions.')]\n"
        "assert not ext_mods, ext_mods\n"
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr or proc.stdout


@pytest.mark.parametrize("filename", ["__init__.py", "_sync.py", "_async.py"])
def test_dbapi_source_has_no_forbidden_imports(filename: str) -> None:
    """Verify vertebra.db.dbapi AST contains zero sqlalchemy, extensions, or adapters imports."""
    src = (_DBAPI_DIR / filename).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                assert not name.startswith("sqlalchemy")
                assert not name.startswith("vertebra.extensions")
                assert not name.startswith("vertebra.db.adapters")
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            mod = node.module
            assert not mod.startswith("sqlalchemy")
            assert not mod.startswith("vertebra.extensions")
            assert not mod.startswith("vertebra.db.adapters")


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT 1", True),
        ("  -- leading comment\nSELECT id FROM items", True),
        ("/* block /* inner */ */ WITH cte AS (SELECT 1) SELECT * FROM cte", True),
        ("/* /* inner */ */ SELECT 1", True),
        ("/* outer /* nested SELECT */ inner */ INSERT INTO t VALUES (?)", False),
        ("PRAGMA table_info('users')", True),
        ("SHOW TABLES", True),
        ("DESCRIBE users", True),
        ("DESC users", True),
        ("EXPLAIN SELECT 1", True),
        ("VALUES (1, 'a')", True),
        ("INSERT INTO t (name) VALUES (?) RETURNING id", True),
        ("UPDATE t SET name = :name WHERE id = :id RETURNING id", True),
        ("DELETE FROM t WHERE id = 1 RETURNING id", True),
        ("INSERT INTO t (label) VALUES ('RETURNING later')", False),
        ('INSERT INTO t ("RETURNING") VALUES (?)', False),
        ("UPDATE t SET label = $tag$RETURNING something$tag$ WHERE id = 1", False),
        ("INSERT INTO t (val) VALUES ($$RETURNING$$)", False),
        ("UPDATE t SET returning = :returning -- RETURNING comment", False),
        ("/* RETURNING inside comment */ DELETE FROM t WHERE id = 1", False),
    ],
)
def test_is_row_returning_sql_handles_comments_literals_and_binds(sql: str, expected: bool) -> None:
    """Verify comment, string, dollar-quote, and bind-parameter skipping in is_row_returning_sql."""
    assert is_row_returning_sql(sql) is expected


@pytest.mark.parametrize(
    "url",
    [
        "bigquery://my-project/my_dataset",
        "spanner://my-project/my-instance/my-db",
        "spangres://my-project/my-instance/my-db",
    ],
)
def test_sync_dbapi_rejects_autocommit_false_for_autocommit_only_backends(url: str) -> None:
    """Verify SyncDBAPIConnection defaults autocommit=True and rejects autocommit=False on cloud backends."""
    conn = SyncDBAPIConnection(url, share_pool=False)
    try:
        assert conn.autocommit is True
        with pytest.raises(NotSupportedError):
            conn.autocommit = False
    finally:
        conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url",
    [
        "bigquery://my-project/my_dataset",
        "spanner://my-project/my-instance/my-db",
        "spangres://my-project/my-instance/my-db",
    ],
)
async def test_async_dbapi_rejects_autocommit_false_for_autocommit_only_backends(url: str) -> None:
    """Verify AsyncDBAPIConnection defaults autocommit=True and rejects autocommit=False on cloud backends."""
    conn = AsyncDBAPIConnection(url, share_pool=False)
    try:
        assert conn.autocommit is True
        with pytest.raises(NotSupportedError):
            conn.autocommit = False
    finally:
        await conn.close()


def test_dbapi_module_connect_and_invalid_url_errors() -> None:
    """Verify SyncDBAPIModule, module-level PEP 249 attributes, connect(), and InterfaceError handling."""
    assert apilevel == "2.0"
    assert threadsafety == 2
    assert paramstyle == "qmark"
    assert dbapi_pkg.connect is connect

    direct_conn = connect("sqlite+vtb:///:memory:", share_pool=False)
    try:
        direct_cur = direct_conn.cursor()
        direct_cur.execute("SELECT 7")
        assert direct_cur.fetchone() == (7,)
    finally:
        direct_conn.close()

    sync_mod = SyncDBAPIModule(paramstyle="qmark")
    sync_conn = sync_mod.connect("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO t (id, name) VALUES (?, ?)", [(1, "a"), (2, "b")])
        cur.execute("SELECT id, name FROM t ORDER BY id")
        assert cur.fetchall() == [(1, "a"), (2, "b")]
    finally:
        sync_conn.close()

    with pytest.raises(InterfaceError):
        sync_mod.connect("unknown+vtb://host/db", share_pool=False)


@pytest.mark.anyio
async def test_async_dbapi_cursor_execution_close_and_invalid_url() -> None:
    """Verify AsyncDBAPIModule and AsyncDBAPICursor execute queries, fetch batches, close cleanly, and guard URLs."""
    async_mod = AsyncDBAPIModule(paramstyle="qmark")
    with pytest.raises(InterfaceError):
        async_mod.connect("unknown+vtb_async://host/db", share_pool=False)

    async_conn = async_mod.connect("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        assert isinstance(async_conn, AsyncDBAPIConnection)
        cur = async_conn.cursor()
        try:
            await cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            await cur.executemany(
                "INSERT INTO items (id, name) VALUES (?, ?)",
                [(1, "alpha"), (2, "beta"), (3, "gamma")],
            )
            await cur.execute("SELECT id, name FROM items ORDER BY id")
            assert await cur.fetchone() == (1, "alpha")
            assert await cur.fetchmany(1) == [(2, "beta")]
            assert await cur.fetchall() == [(3, "gamma")]
            assert await cur.fetchone() is None
        finally:
            await cur.close()
    finally:
        await async_conn.close()
    assert async_conn.closed is True


@pytest.mark.anyio
async def test_dbapi_closed_state_errors_and_size_hints() -> None:
    """Verify PEP 249 closed cursor/connection errors and no-op input/output size hints for sync and async."""
    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        sync_cur = sync_conn.cursor()
        sync_cur.setinputsizes([1, 2])
        sync_cur.setoutputsize(64, 0)
        sync_cur.close()
        with pytest.raises(ProgrammingError):
            sync_cur.execute("SELECT 1")
        with pytest.raises(ProgrammingError):
            sync_cur.executemany("SELECT ?", [(1,)])
        with pytest.raises(ProgrammingError):
            sync_cur.fetchone()
        with pytest.raises(ProgrammingError):
            sync_cur.fetchmany(1)
        with pytest.raises(ProgrammingError):
            sync_cur.fetchall()
        with pytest.raises(ProgrammingError):
            sync_cur.setinputsizes([1])
        with pytest.raises(ProgrammingError):
            sync_cur.setoutputsize(8)

        open_sync_cur = sync_conn.cursor()
    finally:
        sync_conn.close()
    with pytest.raises(InterfaceError):
        sync_conn.cursor()
    with pytest.raises(InterfaceError):
        sync_conn.commit()
    with pytest.raises(InterfaceError):
        sync_conn.rollback()
    with pytest.raises(InterfaceError):
        open_sync_cur.execute("SELECT 1")

    async_mod = AsyncDBAPIModule(paramstyle="qmark")
    async_conn = async_mod.connect("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        async_cur = async_conn.cursor()
        async_cur.setinputsizes([1, 2])
        async_cur.setoutputsize(64, 0)
        await async_cur.close()
        with pytest.raises(ProgrammingError):
            await async_cur.execute("SELECT 1")
        with pytest.raises(ProgrammingError):
            await async_cur.fetchone()

        open_async_cur = async_conn.cursor()
    finally:
        await async_conn.close()
    with pytest.raises(InterfaceError):
        await async_conn.cursor()
    with pytest.raises(InterfaceError):
        await async_conn.commit()
    with pytest.raises(InterfaceError):
        await async_conn.rollback()
    with pytest.raises(InterfaceError):
        await open_async_cur.execute("SELECT 1")

    default_mem_sync = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    try:
        assert default_mem_sync._owns_driver is True
    finally:
        default_mem_sync.close()
    assert default_mem_sync.closed is True

    default_mem_async = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:")
    try:
        assert default_mem_async._owns_driver is True
    finally:
        await default_mem_async.close()
    assert default_mem_async.closed is True


def test_dbapi_driver_lastrowid_sequence_binds_and_lease_isolation() -> None:
    """Verify driver INSERT lastrowid, positional tuple bind preservation in select, and lease_managed isolation."""
    driver = SyncSQLiteDriver()
    conn = SyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        cur.execute("INSERT INTO items (name) VALUES (?)", ("first",))
        assert cur.lastrowid == 1
        cur.execute("INSERT INTO items (name) VALUES (?)", ("second",))
        assert cur.lastrowid == 2
        conn.commit()
    finally:
        conn.close()

    seen_select_params: list[tuple[str, Any]] = []

    class _SelectOnlyDriver:
        def select(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
            seen_select_params.append((sql, params))
            return [{"name": params[0]}] if isinstance(params, tuple) else []

    select_conn = SyncDBAPIConnection(driver=_SelectOnlyDriver(), share_pool=False)
    try:
        select_cur = select_conn.cursor()
        select_cur.execute("SELECT name FROM items WHERE name = ?", ("beta",))
        assert select_cur.fetchone() == ("beta",)
        assert seen_select_params == [("SELECT name FROM items WHERE name = ?", ("beta",))]
    finally:
        select_conn.close()

    prev_broker = DatabaseRegistry._DEFAULT_BROKER
    sentinel_broker = object()
    DatabaseRegistry._DEFAULT_BROKER = sentinel_broker
    try:
        lease_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=True, lease_managed=True, broker=None)
        try:
            assert lease_conn.broker is None
        finally:
            lease_conn.close()
    finally:
        DatabaseRegistry._DEFAULT_BROKER = prev_broker


@pytest.mark.parametrize("use_driver", [False, True], ids=["sqlite_conn", "sync_driver"])
def test_sync_dbapi_sqlite_comment_prefixed_insert_captures_and_clears_lastrowid(use_driver: bool) -> None:
    """Verify comment-prefixed INSERT statements capture SQLite lastrowid and non-INSERT DML clears it."""
    driver = SyncSQLiteDriver() if use_driver else None
    conn = (
        SyncDBAPIConnection(driver=driver, share_pool=False)
        if driver is not None
        else SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    )
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        assert cur.lastrowid is None

        cur.execute("/* lead */ INSERT INTO items (name) VALUES (?)", ("alice",))
        assert cur.rowcount == 1
        assert cur.lastrowid == 1

        cur.execute("-- second insert\nINSERT INTO items (name) VALUES (?)", ("bob",))
        assert cur.rowcount == 1
        assert cur.lastrowid == 2

        cur.execute("UPDATE items SET name = ? WHERE id = ?", ("carol", 1))
        assert cur.rowcount == 1
        assert cur.lastrowid is None

        cur.execute("SELECT id, name FROM items ORDER BY id")
        assert cur.fetchall() == [(1, "carol"), (2, "bob")]
        assert cur.lastrowid is None
    finally:
        conn.close()


def test_sync_dbapi_non_sqlite_lastrowid_none_and_autocommit_skips_select() -> None:
    """Verify non-SQLite drivers return None for lastrowid and autocommit skips commit on read-only SELECT."""

    class _FakePostgresDriver:
        dialect = "postgres"

        def __init__(self) -> None:
            self.commit_calls = 0

        def execute_with_description(
            self,
            sql: str,
            parameters: Any = (),
        ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return ([(1,)], ("val",))

        def execute(self, sql: str, parameters: Any = ()) -> int:
            _ = (sql, parameters)
            return 1

        def commit(self) -> None:
            self.commit_calls += 1

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    fake_driver = _FakePostgresDriver()
    conn = SyncDBAPIConnection("postgres+vtb://localhost/testdb", driver=fake_driver, share_pool=False)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
        assert cur.lastrowid is None
        assert fake_driver.commit_calls == 0

        cur.execute("INSERT INTO items VALUES (1)")
        assert cur.rowcount == 1
        assert cur.lastrowid is None
        assert fake_driver.commit_calls == 1
    finally:
        conn.close()


@pytest.mark.anyio
async def test_async_dbapi_non_sqlite_and_async_sqlite_driver_dml_lastrowid() -> None:
    """Verify AsyncSQLiteDriver.execute_dml captures lastrowid and async non-SQLite skips SELECT autocommit."""
    sqlite_driver = AsyncSQLiteDriver()
    try:
        rc0, id0 = await sqlite_driver.execute_dml(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )
        assert rc0 == 0
        assert id0 is None
        rc1, id1 = await sqlite_driver.execute_dml("/* lead */ INSERT INTO items (name) VALUES (?)", ("alice",))
        assert (rc1, id1) == (1, 1)
        assert sqlite_driver.lastrowid == 1
        rc2, id2 = await sqlite_driver.execute_dml("UPDATE items SET name = ?", ("bob",))
        assert (rc2, id2) == (1, None)
        assert sqlite_driver.lastrowid is None
    finally:
        await sqlite_driver.close()

    class _FakeAsyncPostgresDriver:
        dialect = "postgres"

        def __init__(self) -> None:
            self.commit_calls = 0

        async def execute_with_description(
            self,
            sql: str,
            parameters: Any = (),
        ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return ([(1,)], ("val",))

        async def execute(self, sql: str, parameters: Any = ()) -> int:
            _ = (sql, parameters)
            return 1

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    fake_driver = _FakeAsyncPostgresDriver()
    conn = AsyncDBAPIConnection("postgres+vtb://localhost/testdb", driver=fake_driver, share_pool=False)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        assert await cur.fetchall() == [(1,)]
        assert cur.lastrowid is None
        assert fake_driver.commit_calls == 0

        await cur.execute("INSERT INTO items VALUES (1)")
        assert cur.rowcount == 1
        assert cur.lastrowid is None
        assert fake_driver.commit_calls == 1
    finally:
        await conn.close()
