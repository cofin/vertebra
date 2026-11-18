"""Unit tests for standalone PEP 249 interfaces in vertebra.db.dbapi."""

from __future__ import annotations

import ast
import datetime
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import vertebra.db.dbapi as dbapi_pkg
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.dbapi import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    AsyncDBAPIConnection,
    AsyncDBAPIModule,
    Binary,
    Date,
    DateFromTicks,
    Error,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    SyncDBAPIConnection,
    SyncDBAPIModule,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    apilevel,
    connect,
    is_row_returning_sql,
    paramstyle,
    threadsafety,
)
from vertebra.db.driver import (
    OperationalError as DriverOperationalError,
)
from vertebra.db.driver import (
    ProgrammingError as DriverProgrammingError,
)
from vertebra.db.registry import DatabaseRegistry
from vertebra.typing import VertebraError

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
        async_conn.cursor()
    with pytest.raises(InterfaceError):
        await async_conn.commit()
    with pytest.raises(InterfaceError):
        await async_conn.rollback()
    with pytest.raises(InterfaceError):
        await open_async_cur.execute("SELECT 1")

    default_mem_sync_a = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    default_mem_sync_b = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    default_mem_sync_driver = default_mem_sync_a.driver
    try:
        assert default_mem_sync_driver is not None
        assert default_mem_sync_driver is not default_mem_sync_b.driver
        cur_a = default_mem_sync_a.cursor()
        cur_a.execute("CREATE TABLE iso_sync (v INTEGER)")
        cur_a.execute("INSERT INTO iso_sync VALUES (1)")
        cur_b = default_mem_sync_b.cursor()
        with pytest.raises((OperationalError, ProgrammingError, DriverOperationalError, DriverProgrammingError)):
            cur_b.execute("SELECT v FROM iso_sync")
    finally:
        default_mem_sync_b.close()
        default_mem_sync_a.close()
    assert default_mem_sync_a.closed is True
    assert default_mem_sync_driver.closed is True

    default_mem_async_a = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:")
    default_mem_async_b = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:")
    default_mem_async_driver = default_mem_async_a.driver
    try:
        assert default_mem_async_driver is not None
        assert default_mem_async_driver is not default_mem_async_b.driver
        cur_aa = default_mem_async_a.cursor()
        await cur_aa.execute("CREATE TABLE iso_async (v INTEGER)")
        await cur_aa.execute("INSERT INTO iso_async VALUES (1)")
        cur_ab = default_mem_async_b.cursor()
        with pytest.raises((OperationalError, ProgrammingError, DriverOperationalError, DriverProgrammingError)):
            await cur_ab.execute("SELECT v FROM iso_async")
    finally:
        await default_mem_async_b.close()
        await default_mem_async_a.close()
    assert default_mem_async_a.closed is True
    assert default_mem_async_driver.closed is True


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


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "expected_path", "expect_none_pool_key"),
    [
        ("sqlite:///%3Amemory%3A", ":memory:", True),
        ("sqlite+vtb:///%3Amemory%3A", ":memory:", True),
        ("sqlite:///file::memory:?cache=shared", "file::memory:?cache=shared", False),
        ("sqlite:///file:mem_x?mode=memory&cache=shared", "file:mem_x?mode=memory&cache=shared", False),
        ("sqlite:///file:mem_iso?mode=memory", "file:mem_iso?mode=memory", True),
    ],
)
async def test_sqlite_memory_url_variants_never_create_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    expected_path: str,
    expect_none_pool_key: bool,
) -> None:
    """Verify encoded and file: memory SQLite URLs classify cleanly and never leak disk files."""
    monkeypatch.chdir(tmp_path)
    cfg = SQLiteConfig.from_url(url)
    assert cfg.path == expected_path
    assert cfg.is_in_memory is True
    assert (cfg.pool_key() is None) is expect_none_pool_key
    assert SQLiteConfig(path="%3Amemory%3A").path == ":memory:"

    driver = SyncSQLiteDriver(cfg)
    try:
        rows, _cols = driver.execute_with_description("SELECT 1")
        assert rows == [(1,)]
    finally:
        driver.close()

    sync_conn = SyncDBAPIConnection(url, share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("SELECT 2")
        assert cur.fetchall() == [(2,)]
    finally:
        sync_conn.close()

    async_url = (
        url.replace("sqlite://", "sqlite+vtb_async://", 1)
        if url.startswith("sqlite:///")
        else "sqlite+vtb_async:///%3Amemory%3A"
    )
    async_conn = AsyncDBAPIConnection(async_url, share_pool=False)
    try:
        acur = async_conn.cursor()
        await acur.execute("SELECT 3")
        assert await acur.fetchall() == [(3,)]
    finally:
        await async_conn.close()

    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_dbapi_sqlite_insert_returning_and_empty_autocommit_commit() -> None:
    """Verify INSERT ... RETURNING refreshes lastrowid and empty select/description autocommit commits cleanly."""
    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        cur.execute("INSERT INTO items (name) VALUES (?)", ("first",))
        assert cur.lastrowid == 1
        cur.execute("INSERT INTO items (name) VALUES (?) RETURNING id", ("second",))
        assert cur.fetchall() == [(2,)]
        assert cur.lastrowid == 2
        cur.execute("SELECT id FROM items ORDER BY id")
        assert cur.fetchall() == [(1,), (2,)]
        assert cur.lastrowid is None
    finally:
        sync_conn.close()

    async_conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        acur = async_conn.cursor()
        await acur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        await acur.execute("/* lead */ INSERT INTO items (name) VALUES (?)", ("first",))
        assert acur.lastrowid == 1
        await acur.execute("INSERT INTO items (name) VALUES (?) RETURNING id", ("second",))
        assert await acur.fetchall() == [(2,)]
        assert acur.lastrowid == 2
        await acur.execute("SELECT id FROM items ORDER BY id")
        assert await acur.fetchall() == [(1,), (2,)]
        assert acur.lastrowid is None
    finally:
        await async_conn.close()

    class _EmptyResultDriver:
        dialect = "postgres"

        def __init__(self) -> None:
            self.commit_calls = 0

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = (sql, parameters)
            return ([], None)

        def select(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
            _ = (sql, params)
            return []

        def commit(self) -> None:
            self.commit_calls += 1

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    desc_driver = _EmptyResultDriver()
    empty_desc_conn = SyncDBAPIConnection("postgres+vtb://localhost/testdb", driver=desc_driver, share_pool=False)
    try:
        empty_desc_conn.autocommit = True
        dcur = empty_desc_conn.cursor()
        dcur.execute("UPDATE items SET name = 'x' RETURNING id")
        assert dcur.fetchall() == []
        assert desc_driver.commit_calls == 1
    finally:
        empty_desc_conn.close()

    class _SelectOnlyEmptyDriver:
        dialect = "postgres"

        def __init__(self) -> None:
            self.commit_calls = 0

        def select(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
            _ = (sql, params)
            return []

        def commit(self) -> None:
            self.commit_calls += 1

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    sel_driver = _SelectOnlyEmptyDriver()
    empty_sel_conn = SyncDBAPIConnection("postgres+vtb://localhost/testdb", driver=sel_driver, share_pool=False)
    try:
        empty_sel_conn.autocommit = True
        scur = empty_sel_conn.cursor()
        scur.execute("DELETE FROM items RETURNING id")
        assert scur.fetchall() == []
        assert sel_driver.commit_calls == 1
    finally:
        empty_sel_conn.close()


@pytest.mark.parametrize(
    "empty_url",
    ["", "   ", None],
)
def test_dbapi_empty_or_missing_url_raises_interface_error(empty_url: Any) -> None:
    """Verify empty or omitted connection URLs raise InterfaceError for sync and async DBAPI entry points."""
    with pytest.raises(InterfaceError):
        SyncDBAPIConnection(empty_url)
    with pytest.raises(InterfaceError):
        connect(empty_url)
    with pytest.raises(InterfaceError):
        SyncDBAPIModule().connect(empty_url)
    with pytest.raises(InterfaceError):
        AsyncDBAPIConnection(empty_url)
    with pytest.raises(InterfaceError):
        AsyncDBAPIModule().connect(empty_url)


def test_dbapi_native_exception_translation_to_pep249() -> None:
    """Verify native vertebra._vertebra exceptions map to PEP 249 dbapi.*Error subclasses."""
    import vertebra._vertebra as native_vtb

    class _FaultyDriver:
        dialect = "sqlite"

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            if "integrity" in sql:
                raise native_vtb.IntegrityError("UNIQUE constraint failed")
            if "operational" in sql:
                raise native_vtb.OperationalError("database is locked")
            if "programming" in sql:
                raise native_vtb.ProgrammingError("syntax error")
            if "data" in sql:
                raise native_vtb.DataError("invalid number")
            raise native_vtb.DatabaseError("generic failure")

        def begin(self) -> None:
            return None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", driver=_FaultyDriver(), share_pool=False)
    try:
        cur = conn.cursor()
        with pytest.raises(dbapi_pkg.IntegrityError):
            cur.execute("integrity")
        with pytest.raises(dbapi_pkg.OperationalError):
            cur.execute("operational")
        with pytest.raises(dbapi_pkg.ProgrammingError):
            cur.execute("programming")
        with pytest.raises(dbapi_pkg.DataError):
            cur.execute("data")
        with pytest.raises(dbapi_pkg.DatabaseError):
            cur.execute("generic")
    finally:
        conn.close()


def test_sync_dbapi_sqlite_uses_native_driver_and_has_no_sqlite_conn() -> None:
    """Verify SyncDBAPIConnection operates solely over SyncSQLiteDriver with zero sqlite3 connection handles."""
    conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        assert not hasattr(conn, "_sqlite_conn")
        assert isinstance(conn.driver, SyncSQLiteDriver)
        cur = conn.cursor()
        cur.execute("SELECT 100")
        assert cur.fetchone() == (100,)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("INSERT INTO t (name) OUTPUT INSERTED.id VALUES (?)", True),
        ("UPDATE t SET name = ? OUTPUT DELETED.name, INSERTED.name WHERE id = ?", True),
        ("DELETE FROM t OUTPUT DELETED.id WHERE id = ?", True),
        (
            "MERGE INTO tgt USING src ON tgt.id = src.id "
            "WHEN MATCHED THEN UPDATE SET tgt.val = src.val OUTPUT $action, INSERTED.id",
            True,
        ),
        (
            "WITH src AS (SELECT 1 AS id) MERGE INTO tgt USING src ON tgt.id = src.id "
            "WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id) OUTPUT $action",
            True,
        ),
        ("INSERT INTO t (msg) VALUES ('OUTPUT INSERTED.id')", False),
        ('INSERT INTO t ("OUTPUT INSERTED.id") VALUES (?)', False),
        ("INSERT INTO t ([OUTPUT INSERTED.id]) VALUES (?)", False),
        ("INSERT INTO t (name) VALUES (?) -- OUTPUT INSERTED.id", False),
        ("INSERT INTO t (name) VALUES (?) /* OUTPUT DELETED.id */", False),
        ("INSERT INTO t (name) VALUES (?) /* outer /* inner */ OUTPUT $action */", False),
        ("UPDATE t SET output = ? WHERE id = ?", False),
        ("UPDATE t SET output = deleted.name WHERE id = ?", False),
    ],
)
def test_is_row_returning_sql_tsql_output(sql: str, expected: bool) -> None:
    """Verify T-SQL OUTPUT clauses are detected only when unquoted and uncommented inside DML."""
    assert is_row_returning_sql(sql) is expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "overrides", "expected_attrs"),
    [
        (
            "sqlite+vtb:///:memory:",
            {"wal_mode": False, "busy_timeout_ms": 1500},
            {"wal_mode": False, "busy_timeout_ms": 1500},
        ),
        (
            "postgres+vtb://localhost/app",
            {"application_name": "vtb-app", "statement_timeout_ms": 2500},
            {"application_name": "vtb-app", "statement_timeout_ms": 2500},
        ),
        (
            "mysql+vtb://localhost/app",
            {"charset": "latin1", "connect_timeout": 5.0},
            {"charset": "latin1", "connect_timeout": 5.0},
        ),
        (
            "mssql+vtb://localhost/app",
            {"application_name": "vtb-mssql", "connect_timeout_ms": 3000},
            {"application_name": "vtb-mssql", "connect_timeout_ms": 3000},
        ),
        (
            "bigquery://my-project/my_dataset",
            {"maximum_bytes_billed": 1024},
            {"maximum_bytes_billed": 1024},
        ),
        (
            "spanner://my-project/my-instance/my-db",
            {"database_role": "reader"},
            {"database_role": "reader"},
        ),
    ],
)
async def test_dbapi_connect_args_forwarding_and_validation(
    url: str,
    overrides: dict[str, Any],
    expected_attrs: dict[str, Any],
) -> None:
    """Verify per-engine connect_args forward to config and invalid keys or build errors raise PEP 249 Error."""

    class _StubDriver:
        dialect = "stub"

        def close(self) -> None:
            return None

    class _StubAsyncDriver:
        dialect = "stub"

        async def close(self) -> None:
            return None

    use_stub = not url.startswith(("sqlite", "bigquery", "spanner"))
    sync_kwargs: dict[str, Any] = {"share_pool": False, **overrides}
    async_kwargs: dict[str, Any] = {"share_pool": False, **overrides}
    if use_stub:
        sync_kwargs["driver"] = _StubDriver()
        async_kwargs["driver"] = _StubAsyncDriver()

    sync_conn = connect(url, **sync_kwargs)
    try:
        for attr, expected in expected_attrs.items():
            assert getattr(sync_conn.config, attr) == expected
    finally:
        sync_conn.close()

    async_url = url.replace("+vtb://", "+vtb_async://", 1) if "+vtb://" in url else url
    async_conn = AsyncDBAPIConnection(async_url, **async_kwargs)
    try:
        for attr, expected in expected_attrs.items():
            assert getattr(async_conn.config, attr) == expected
    finally:
        await async_conn.close()

    engine_name = url.partition("://")[0].partition("+")[0]
    with pytest.raises(InterfaceError) as sync_bad_exc:
        connect(url, share_pool=False, bad_key=1)
    assert engine_name in str(sync_bad_exc.value).lower()
    assert "bad_key" in str(sync_bad_exc.value)

    with pytest.raises(InterfaceError) as async_bad_exc:
        AsyncDBAPIConnection(async_url, share_pool=False, bad_key=1)
    assert engine_name in str(async_bad_exc.value).lower()
    assert "bad_key" in str(async_bad_exc.value)

    autocommit_conn = connect("sqlite+vtb:///:memory:", share_pool=False, autocommit=True)
    try:
        assert autocommit_conn.autocommit is True
    finally:
        autocommit_conn.close()

    class _FailingConfig(SQLiteConfig, frozen=True):
        def build_sync_driver(self, registry: Any) -> Any:
            _ = registry
            raise VertebraError("sync driver build failed")

        def build_async_driver(self, registry: Any) -> Any:
            _ = registry
            raise TypeError("async driver bad arg type")

    with pytest.raises(Error, match="sync driver build failed"):
        SyncDBAPIConnection(_FailingConfig(), share_pool=False)

    with pytest.raises(Error, match="async driver bad arg type"):
        AsyncDBAPIConnection(_FailingConfig(), share_pool=False)


@pytest.mark.anyio
async def test_dbapi_executemany_returning_and_negative_fetchmany() -> None:
    """Verify executemany accumulates RETURNING rows across batches and fetchmany(-1) preserves cursor state."""
    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        cur.executemany(
            "INSERT INTO items (name) VALUES (?) RETURNING id, name",
            [("alpha",), ("beta",), ("gamma",)],
        )
        assert cur.rowcount == 3
        assert cur.description is not None
        assert [col[0] for col in cur.description] == ["id", "name"]
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)
        assert cur.fetchone() == (1, "alpha")
        assert cur.fetchall() == [(2, "beta"), (3, "gamma")]
    finally:
        sync_conn.close()

    async_conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        acur = async_conn.cursor()
        await acur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        await acur.executemany(
            "INSERT INTO items (name) VALUES (?) RETURNING id, name",
            [("one",), ("two",), ("three",)],
        )
        assert acur.rowcount == 3
        assert acur.description is not None
        assert [col[0] for col in acur.description] == ["id", "name"]
        with pytest.raises(ProgrammingError):
            await acur.fetchmany(-1)
        assert await acur.fetchone() == (1, "one")
        assert await acur.fetchall() == [(2, "two"), (3, "three")]
    finally:
        await async_conn.close()


class _TypeCodeColumnDescriptor:
    def __init__(self, name: str, type_code: Any = None, type_oid: int | None = None) -> None:
        self.name = name
        self.type_code = type_code
        self.type_oid = type_oid


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("columns_meta", "expected_desc"),
    [
        (
            [("id", 23), ("label", 25)],
            (
                ("id", 23, None, None, None, None, None),
                ("label", 25, None, None, None, None, None),
            ),
        ),
        (
            [("id", 23, 10, 4, 10, 0, False)],
            (("id", 23, 10, 4, 10, 0, False),),
        ),
        (
            [
                _TypeCodeColumnDescriptor("id", type_code=NUMBER),
                _TypeCodeColumnDescriptor("payload", type_oid=17),
            ],
            (
                ("id", NUMBER, None, None, None, None, None),
                ("payload", 17, None, None, None, None, None),
            ),
        ),
        (
            ["plain_col"],
            (("plain_col", None, None, None, None, None, None),),
        ),
    ],
)
async def test_dbapi_description_preserves_driver_type_codes(
    columns_meta: list[Any],
    expected_desc: tuple[tuple[Any, ...], ...],
) -> None:
    """Verify coerce_dbapi_rows_and_desc preserves type_code for tuples and descriptors and None for strings."""

    class _SyncDescDriver:
        dialect = "postgres"

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], list[Any]]:
            _ = (sql, parameters)
            return ([tuple(range(len(columns_meta)))], columns_meta)

        def close(self) -> None:
            return None

    class _AsyncDescDriver:
        dialect = "postgres"

        async def execute_with_description(
            self,
            sql: str,
            parameters: Any = (),
        ) -> tuple[list[tuple[Any, ...]], list[Any]]:
            _ = (sql, parameters)
            return ([tuple(range(len(columns_meta)))], columns_meta)

        async def close(self) -> None:
            return None

    sync_conn = SyncDBAPIConnection("postgres+vtb://localhost/testdb", driver=_SyncDescDriver(), share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("SELECT 1")
        assert cur.description == expected_desc
    finally:
        sync_conn.close()

    async_conn = AsyncDBAPIConnection(
        "postgres+vtb_async://localhost/testdb",
        driver=_AsyncDescDriver(),
        share_pool=False,
    )
    try:
        acur = async_conn.cursor()
        await acur.execute("SELECT 1")
        assert acur.description == expected_desc
    finally:
        await async_conn.close()


@pytest.mark.anyio
async def test_dbapi_pep249_constructors_version_scope_var_and_nextset() -> None:
    """Verify PEP 249 constructors, type objects, SQLite-only version attributes, var(), and nextset()."""
    str_type: Any = str
    bytes_type: Any = bytes
    int_type: Any = int
    dt_type: Any = datetime.datetime
    bytearray_type: Any = bytearray
    float_type: Any = float
    date_type: Any = datetime.date

    for target in (dbapi_pkg, SyncDBAPIModule(dialect="sqlite"), AsyncDBAPIModule(dialect="sqlite")):
        assert target.Date(2026, 11, 17) == datetime.date(2026, 11, 17)
        assert target.Time(12, 30, 45) == datetime.time(12, 30, 45)
        assert target.Timestamp(2026, 11, 17, 12, 30, 45) == datetime.datetime(2026, 11, 17, 12, 30, 45)
        assert isinstance(target.DateFromTicks(0), datetime.date)
        assert isinstance(target.TimeFromTicks(0), datetime.time)
        assert isinstance(target.TimestampFromTicks(0), datetime.datetime)
        assert target.Binary(b"abc") == b"abc"
        assert str_type == target.STRING
        assert bytes_type == target.BINARY
        assert int_type == target.NUMBER
        assert dt_type == target.DATETIME
        assert target.ROWID == "ROWID"

    assert Date(2026, 1, 2) == datetime.date(2026, 1, 2)
    assert Time(3, 4, 5) == datetime.time(3, 4, 5)
    assert Timestamp(2026, 1, 2, 3, 4, 5) == datetime.datetime(2026, 1, 2, 3, 4, 5)
    assert isinstance(DateFromTicks(100), datetime.date)
    assert isinstance(TimeFromTicks(100), datetime.time)
    assert isinstance(TimestampFromTicks(100), datetime.datetime)
    assert Binary(bytearray(b"xy")) == b"xy"
    assert STRING == "TEXT"
    assert bytearray_type == BINARY
    assert float_type == NUMBER
    assert date_type == DATETIME
    assert ROWID == 26

    sync_sqlite_mod = SyncDBAPIModule(paramstyle="qmark", dialect="sqlite")
    async_sqlite_mod = AsyncDBAPIModule(paramstyle="qmark", dialect="sqlite")
    assert sync_sqlite_mod.sqlite_version == sqlite3.sqlite_version
    assert sync_sqlite_mod.sqlite_version_info == sqlite3.sqlite_version_info
    assert async_sqlite_mod.sqlite_version == sqlite3.sqlite_version
    assert async_sqlite_mod.sqlite_version_info == sqlite3.sqlite_version_info

    sync_pg_mod = SyncDBAPIModule(paramstyle="pyformat", dialect="postgresql")
    async_pg_mod = AsyncDBAPIModule(paramstyle="pyformat", dialect="postgresql")
    assert not hasattr(sync_pg_mod, "sqlite_version")
    assert not hasattr(sync_pg_mod, "sqlite_version_info")
    assert not hasattr(async_pg_mod, "sqlite_version")
    assert not hasattr(async_pg_mod, "sqlite_version_info")

    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = sync_conn.cursor()
        out_var = cur.var(int, size=8, arraysize=2, outconverter=lambda v: v * 10)
        assert out_var.type_code is int
        assert out_var.size == 8
        assert out_var.arraysize == 2
        assert out_var.getvalue(0) is None
        out_var.setvalue(0, 4)
        assert out_var.getvalue(0) == 40
        assert cur.nextset() is None
    finally:
        sync_conn.close()

    async_conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        acur = async_conn.cursor()
        aout_var = acur.var(str, size=16, arraysize=1)
        assert aout_var.type_code is str
        assert aout_var.size == 16
        aout_var.setvalue(0, "ok")
        assert aout_var.getvalue(0) == "ok"
        assert await acur.nextset() is None
    finally:
        await async_conn.close()

    class _SyncNextSetDriver:
        dialect = "mssql"

        def __init__(self) -> None:
            self._sets = [([(1,)], ("a",)), ([(2,)], ("b",))]

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = (sql, parameters)
            return self._sets.pop(0)

        def nextset(self) -> tuple[list[tuple[Any, ...]], Any] | None:
            if self._sets:
                return self._sets.pop(0)
            return None

        def close(self) -> None:
            return None

    class _AsyncNextSetDriver:
        dialect = "mssql"

        def __init__(self) -> None:
            self._sets = [([(10,)], ("x",)), ([(20,)], ("y",))]

        async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = (sql, parameters)
            return self._sets.pop(0)

        async def nextset(self) -> tuple[list[tuple[Any, ...]], Any] | None:
            if self._sets:
                return self._sets.pop(0)
            return None

        async def close(self) -> None:
            return None

    mssql_sync_conn = SyncDBAPIConnection("mssql+vtb://localhost/db", driver=_SyncNextSetDriver(), share_pool=False)
    try:
        mcur = mssql_sync_conn.cursor()
        mcur.execute("SELECT 1; SELECT 2")
        assert mcur.fetchall() == [(1,)]
        assert mcur.nextset() is True
        assert mcur.fetchall() == [(2,)]
        assert mcur.nextset() is None
    finally:
        mssql_sync_conn.close()

    mssql_async_conn = AsyncDBAPIConnection(
        "mssql+vtb_async://localhost/db",
        driver=_AsyncNextSetDriver(),
        share_pool=False,
    )
    try:
        amcur = mssql_async_conn.cursor()
        await amcur.execute("SELECT 10; SELECT 20")
        assert await amcur.fetchall() == [(10,)]
        assert await amcur.nextset() is True
        assert await amcur.fetchall() == [(20,)]
        assert await amcur.nextset() is None
    finally:
        await mssql_async_conn.close()
