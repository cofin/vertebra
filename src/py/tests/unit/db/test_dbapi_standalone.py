"""Unit tests for standalone PEP 249 interfaces in vertebra.db.dbapi."""

from __future__ import annotations

import ast
import datetime
import functools
import inspect
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import vertebra._vertebra as native_module
import vertebra.db.dbapi as dbapi_pkg
import vertebra.db.dbapi._sync as dbapi_sync
import vertebra.db.driver._common as driver_common
from vertebra._duckdb import AsyncDuckDBCursor, SyncDuckDBCursor
from vertebra._sqlite import AsyncSQLiteCursor, SyncSQLiteCursor
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
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    UnsupportedBackendError,
    VertebraError,
)

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
    """Verify AsyncDBAPIModule and native async cursor execute queries, fetch batches, close cleanly, and guard URLs."""
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
    """Verify PEP 249 closed cursor and connection errors for sync and async native cursors."""
    sync_conn = SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False)
    try:
        sync_cur = sync_conn.cursor()
        sync_cur.close()
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_cur.execute("SELECT 1")
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_cur.executemany("SELECT ?", [(1,)])
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_cur.fetchone()
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_cur.fetchmany(1)
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_cur.fetchall()

        open_sync_cur = sync_conn.cursor()
    finally:
        sync_conn.close()
    with pytest.raises(InterfaceError):
        sync_conn.cursor()
    with pytest.raises(InterfaceError):
        sync_conn.commit()
    with pytest.raises(InterfaceError):
        sync_conn.rollback()
    with pytest.raises((InterfaceError, OperationalError)):
        open_sync_cur.execute("SELECT 1")

    async_mod = AsyncDBAPIModule(paramstyle="qmark")
    async_conn = async_mod.connect(driver=AsyncSQLiteDriver(), share_pool=False)
    try:
        async_cur = async_conn.cursor()
        await async_cur.close()
        with pytest.raises(InterfaceError):
            await async_cur.execute("SELECT 1")
        with pytest.raises(InterfaceError):
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
    with pytest.raises((InterfaceError, OperationalError)):
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
    """Verify driver INSERT lastrowid and lease_managed isolation."""
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


@pytest.mark.anyio
async def test_async_dbapi_non_sqlite_and_async_sqlite_driver_dml_lastrowid() -> None:
    """Verify AsyncSQLiteDriver.execute_dml captures lastrowid on INSERT and clears it on UPDATE."""
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


def test_sync_dbapi_sqlite_uses_native_driver_and_has_no_sqlite_conn() -> None:
    """Verify SyncDBAPIConnection operates solely over SyncSQLiteDriver with zero sqlite3 connection handles."""
    conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
    try:
        assert not hasattr(conn, "_sqlite_conn")
        assert isinstance(conn.driver, SyncSQLiteDriver)
        cur = conn.cursor()
        assert isinstance(cur, SyncSQLiteCursor)
        cur.execute("SELECT 100")
        assert cur.fetchone() == (100,)
    finally:
        conn.close()


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
        in_transaction: bool = False

        def cursor(self, *, lazy_begin: bool = False) -> Any:
            _ = lazy_begin
            return None

        def close(self) -> None:
            return None

    class _StubAsyncDriver:
        dialect = "stub"
        in_transaction: bool = False

        def cursor(self, *, lazy_begin: bool = False) -> Any:
            _ = lazy_begin
            return None

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


@pytest.mark.parametrize(
    ("dbapi_name", "native_name"),
    [
        ("Error", "DatabaseError"),
        ("DatabaseError", "DatabaseError"),
        ("InterfaceError", "InterfaceError"),
        ("DataError", "DataError"),
        ("OperationalError", "OperationalError"),
        ("IntegrityError", "IntegrityError"),
        ("InternalError", "InternalError"),
        ("ProgrammingError", "ProgrammingError"),
        ("NotSupportedError", "NotSupportedError"),
    ],
)
def test_dbapi_exception_names_are_native_classes(dbapi_name: str, native_name: str) -> None:
    """Verify each PEP 249 error name is the vertebra._vertebra class on the package and both module objects."""
    native_cls = getattr(native_module, native_name)
    assert getattr(dbapi_pkg, dbapi_name) is native_cls
    assert getattr(dbapi_sync, dbapi_name) is native_cls
    assert getattr(SyncDBAPIModule(), dbapi_name) is native_cls
    assert getattr(AsyncDBAPIModule(), dbapi_name) is native_cls


def test_dbapi_native_classes_keep_hierarchy_and_remove_old_translators() -> None:
    """Verify the native hierarchy shape, Python construction, and removal of old error translators."""
    assert issubclass(dbapi_pkg.InterfaceError, dbapi_pkg.DatabaseError)
    assert issubclass(dbapi_pkg.Warning, Exception)
    assert not issubclass(dbapi_pkg.Warning, dbapi_pkg.Error)
    assert SyncDBAPIModule().IntegrityError is native_module.IntegrityError
    assert AsyncDBAPIModule().IntegrityError is native_module.IntegrityError

    constructed = dbapi_pkg.ProgrammingError("x")
    assert str(constructed) == "x"
    assert constructed.sqlstate is None
    assert constructed.is_transient is False

    for removed_name in (
        "_translate_native_error",
        "_translate_vertebra_error",
        "_ErrorTranslator",
        "_TRANSLATE_ERRORS",
        "_translate_errors",
        "_DBAPIOutBindVar",
    ):
        assert not hasattr(dbapi_sync, removed_name)


def test_dbapi_oracle_connect_args_allow_enable_reset_relay() -> None:
    """Verify Oracle DB-API connect_args accept enable_reset_relay and forward it to OracleConfig."""
    assert "enable_reset_relay" in dbapi_sync._SUPPORTED_CONNECT_ARGS_BY_DIALECT["oracle"]
    _, cfg = dbapi_sync._resolve_dbapi_config(
        "oracle://system:bench@127.0.0.1:1521/FREEPDB1",
        driver=None,
        driver_broker=None,
        connect_kwargs={"enable_reset_relay": False},
    )
    assert getattr(cfg, "enable_reset_relay", None) is False


@pytest.mark.anyio
async def test_dbapi_executemany_returning_and_negative_fetchmany() -> None:
    """Verify executemany accumulates RETURNING rows across batches and fetchmany(-1) preserves cursor state."""
    sync_conn = SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False)
    try:
        cur = sync_conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        cur.executemany(
            "INSERT INTO items (name) VALUES (?)",
            [("alpha",), ("beta",), ("gamma",)],
        )
        assert cur.rowcount == 3
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)
        cur.execute("SELECT id, name FROM items ORDER BY id")
        assert cur.description is not None
        assert [col[0] for col in cur.description] == ["id", "name"]
        assert cur.fetchone() == (1, "alpha")
        assert cur.fetchall() == [(2, "beta"), (3, "gamma")]
    finally:
        sync_conn.close()

    async_conn = AsyncDBAPIConnection(driver=AsyncSQLiteDriver(), share_pool=False)
    try:
        acur = async_conn.cursor()
        await acur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        await acur.executemany(
            "INSERT INTO items (name) VALUES (?)",
            [("one",), ("two",), ("three",)],
        )
        assert acur.rowcount == 3
        with pytest.raises(ProgrammingError):
            await acur.fetchmany(-1)
        await acur.execute("SELECT id, name FROM items ORDER BY id")
        assert acur.description is not None
        assert [col[0] for col in acur.description] == ["id", "name"]
        assert await acur.fetchone() == (1, "one")
        assert await acur.fetchall() == [(2, "two"), (3, "three")]
    finally:
        await async_conn.close()


def test_dbapi_pep249_constructors_version_scope_and_oracle_var() -> None:
    """Verify PEP 249 constructors, type objects, SQLite version attributes, and Oracle Var exposure."""
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
    assert not hasattr(sync_sqlite_mod, "Var")
    assert not hasattr(async_sqlite_mod, "Var")

    sync_pg_mod = SyncDBAPIModule(paramstyle="pyformat", dialect="postgresql")
    async_pg_mod = AsyncDBAPIModule(paramstyle="pyformat", dialect="postgresql")
    assert not hasattr(sync_pg_mod, "sqlite_version")
    assert not hasattr(sync_pg_mod, "sqlite_version_info")
    assert not hasattr(async_pg_mod, "sqlite_version")
    assert not hasattr(async_pg_mod, "sqlite_version_info")
    assert not hasattr(sync_pg_mod, "Var")
    assert not hasattr(async_pg_mod, "Var")


def test_dbapi_oracle_module_exposes_native_out_bind_var() -> None:
    """Verify sync and async Oracle DB-API modules expose vertebra._oracle.OracleOutBindVar as Var."""
    oracle_ext = pytest.importorskip("vertebra._oracle")
    sync_oracle_mod = SyncDBAPIModule(dialect="oracle")
    async_oracle_mod = AsyncDBAPIModule(dialect="oracle")
    assert vars(sync_oracle_mod)["Var"] is oracle_ext.OracleOutBindVar
    assert vars(async_oracle_mod)["Var"] is oracle_ext.OracleOutBindVar


@pytest.mark.parametrize(
    ("url", "expected_cls"),
    [
        ("sqlite+vtb:///:memory:", SyncSQLiteCursor),
        ("duckdb+vtb:///:memory:", SyncDuckDBCursor),
    ],
    ids=["sqlite", "duckdb"],
)
def test_sync_dbapi_native_engine_cursor_is_native_cursor(url: str, expected_cls: type[Any]) -> None:
    """Verify sync DB-API connections for sqlite and duckdb return distinct native cursors."""
    conn = connect(url, share_pool=False)
    try:
        cur = conn.cursor()
        assert type(cur) is expected_cls
        assert conn.driver is not None
        driver_any: Any = conn.driver
        assert driver_any.in_transaction is False
        c1 = driver_any.cursor()
        c2 = driver_any.cursor()
        assert c1 is not c2
        cur.execute("select 1")
        rows = cur.fetchall()
        assert rows == [(1,)]
        assert all(isinstance(row, tuple) for row in rows)
        assert cur.description is not None
        assert isinstance(cur.description[0][0], str)
        assert cur.description[0][0]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb:///:memory:", "duckdb+vtb:///:memory:"],
    ids=["sqlite", "duckdb"],
)
def test_sync_dbapi_native_cursor_rollback_and_commit_follow_connection_state(url: str) -> None:
    """Verify native cursor writes roll back and commit through SyncDBAPIConnection."""
    conn = connect(url, share_pool=False)
    try:
        setup_cur = conn.cursor()
        setup_cur.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        conn.commit()

        write_cur = conn.cursor()
        write_cur.execute("INSERT INTO items (id, name) VALUES (1, 'rolled_back')")
        conn.rollback()

        check_cur = conn.cursor()
        check_cur.execute("SELECT COUNT(*) FROM items")
        assert check_cur.fetchall() == [(0,)]

        write_cur.execute("INSERT INTO items (id, name) VALUES (2, 'committed')")
        conn.commit()

        final_cur = conn.cursor()
        final_cur.execute("SELECT COUNT(*) FROM items")
        assert final_cur.fetchall() == [(1,)]
    finally:
        conn.close()


def test_sync_dbapi_native_cursor_autocommit_persists_without_commit(tmp_path: Path) -> None:
    """Verify autocommit=True persists writes to file-backed SQLite without explicit commit."""
    db_file = tmp_path / "autocommit.db"
    url = f"sqlite+vtb:///{db_file}"
    conn = connect(url, share_pool=False, autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO items (id, name) VALUES (1, 'visible')")
        driver_any: Any = conn.driver
        assert driver_any.in_transaction is False

        reader = connect(url, share_pool=False)
        try:
            rcur = reader.cursor()
            rcur.execute("SELECT id, name FROM items")
            assert rcur.fetchall() == [(1, "visible")]
        finally:
            reader.close()
    finally:
        conn.close()


def test_sync_dbapi_native_cursor_dml_rowcount_lastrowid_and_description() -> None:
    """Verify native SQLite cursor sets description, rowcount, and lastrowid for DML and RETURNING."""
    conn = connect("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        cur.execute("INSERT INTO items (name) VALUES (?)", ("first",))
        assert cur.description is None
        assert cur.rowcount == 1
        assert cur.lastrowid == 1

        cur.execute("INSERT INTO items (name) VALUES (?) RETURNING id", ("second",))
        assert cur.description is not None
        assert cur.fetchall() == [(2,)]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb:///:memory:", "duckdb+vtb:///:memory:"],
    ids=["sqlite", "duckdb"],
)
def test_sync_dbapi_native_cursor_reused_after_commit_starts_new_transaction(url: str) -> None:
    """Verify reusing a native cursor after commit opens a new transaction that rollback undoes."""
    conn = connect(url, share_pool=False)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        conn.commit()

        cur.execute("INSERT INTO items (id, name) VALUES (1, 'kept')")
        conn.commit()

        cur.execute("INSERT INTO items (id, name) VALUES (2, 'undone')")
        driver_any: Any = conn.driver
        assert driver_any.in_transaction is True

        conn.rollback()
        assert driver_any.in_transaction is False

        check_cur = conn.cursor()
        check_cur.execute("SELECT COUNT(*) FROM items")
        assert check_cur.fetchall() == [(1,)]
    finally:
        conn.close()


def test_sync_dbapi_native_cursor_raises_dbapi_exception_classes() -> None:
    """Verify native SQLite cursor raises native PEP 249 exception classes directly."""
    conn = connect("sqlite+vtb:///:memory:", share_pool=False)
    try:
        cur = conn.cursor()
        assert cur.fetchall() == []
        with pytest.raises(dbapi_pkg.ProgrammingError):
            cur.fetchmany(-1)

        cur.execute("CREATE TABLE uniq_items (id INTEGER PRIMARY KEY)")
        cur.execute("INSERT INTO uniq_items (id) VALUES (1)")
        with pytest.raises(dbapi_pkg.IntegrityError) as exc_info:
            cur.execute("INSERT INTO uniq_items (id) VALUES (1)")
        assert type(exc_info.value) is native_module.IntegrityError

        with pytest.raises(dbapi_pkg.DatabaseError) as missing_exc:
            cur.execute("SELECT * FROM table_that_does_not_exist")
        assert isinstance(missing_exc.value, dbapi_pkg.DatabaseError)
    finally:
        conn.close()


def test_sync_dbapi_native_engine_rejects_driver_without_cursor() -> None:
    """Verify SyncDBAPIConnection raises InterfaceError when a native-cursor engine driver lacks cursor()."""

    class _NoCursorDriver:
        url = "sqlite://"

        def close(self) -> None:
            return None

    with pytest.raises(InterfaceError, match="cursor"):
        SyncDBAPIConnection(driver=_NoCursorDriver(), share_pool=False)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "expected_cls"),
    [
        ("sqlite+vtb_async:///:memory:", AsyncSQLiteCursor),
        ("duckdb+vtb_async:///:memory:", AsyncDuckDBCursor),
    ],
    ids=["sqlite", "duckdb"],
)
async def test_async_dbapi_native_engine_cursor_is_native_cursor(url: str, expected_cls: type[Any]) -> None:
    """Verify async DB-API connections for sqlite and duckdb return native async cursors."""
    conn = AsyncDBAPIConnection(url, share_pool=False)
    try:
        cur = conn.cursor()
        assert type(cur) is expected_cls
        await cur.execute("select 1")
        rows = await cur.fetchall()
        assert rows == [(1,)]
        assert all(isinstance(row, tuple) for row in rows)
        assert not inspect.isawaitable(cur.description)
        assert not inspect.isawaitable(cur.rowcount)
        assert cur.description is not None
        assert isinstance(cur.rowcount, int)
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb_async:///:memory:", "duckdb+vtb_async:///:memory:"],
    ids=["sqlite", "duckdb"],
)
async def test_async_dbapi_native_cursor_explicit_begin_rollback_and_commit(url: str) -> None:
    """Verify explicit begin, rollback, and commit control native async cursor writes."""
    conn = AsyncDBAPIConnection(url, share_pool=False)
    try:
        setup_cur = conn.cursor()
        await setup_cur.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        await conn.commit()

        await conn.begin()
        write_cur = conn.cursor()
        await write_cur.execute("INSERT INTO items (id, name) VALUES (1, 'rolled_back')")
        await conn.rollback()

        check_cur = conn.cursor()
        await check_cur.execute("SELECT COUNT(*) FROM items")
        assert await check_cur.fetchall() == [(0,)]
        await conn.rollback()

        await conn.begin()
        await write_cur.execute("INSERT INTO items (id, name) VALUES (2, 'committed')")
        await conn.commit()

        final_cur = conn.cursor()
        await final_cur.execute("SELECT COUNT(*) FROM items")
        assert await final_cur.fetchall() == [(1,)]
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_native_cursor_dml_rowcount_lastrowid_and_description() -> None:
    """Verify native async SQLite cursor sets description, rowcount, and lastrowid for DML and RETURNING."""
    conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        await cur.execute("INSERT INTO items (name) VALUES (?)", ("first",))
        assert cur.description is None
        assert cur.rowcount == 1
        assert cur.lastrowid == 1

        await cur.execute("INSERT INTO items (name) VALUES (?) RETURNING id", ("second",))
        assert cur.description is not None
        assert await cur.fetchall() == [(2,)]
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb_async:///:memory:", "duckdb+vtb_async:///:memory:"],
    ids=["sqlite", "duckdb"],
)
async def test_async_dbapi_native_cursor_execute_without_begin_is_rolled_back(url: str) -> None:
    """Verify executing without begin() opens a lazy transaction that rollback undoes, including after commit."""
    conn = AsyncDBAPIConnection(url, share_pool=False)
    try:
        setup_cur = conn.cursor()
        await setup_cur.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        await conn.commit()

        cur = conn.cursor()
        await cur.execute("INSERT INTO items (id, name) VALUES (1, 'undone_first')")
        await conn.rollback()

        check_cur = conn.cursor()
        await check_cur.execute("SELECT COUNT(*) FROM items")
        assert await check_cur.fetchall() == [(0,)]

        await cur.execute("INSERT INTO items (id, name) VALUES (2, 'kept')")
        await conn.commit()

        await cur.execute("INSERT INTO items (id, name) VALUES (3, 'undone_second')")
        await conn.rollback()

        final_cur = conn.cursor()
        await final_cur.execute("SELECT COUNT(*) FROM items")
        assert await final_cur.fetchall() == [(1,)]
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_native_cursor_raises_dbapi_exception_classes() -> None:
    """Verify native async SQLite cursor raises native PEP 249 exception classes directly."""
    conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
    try:
        cur = conn.cursor()
        assert await cur.fetchall() == []
        with pytest.raises(dbapi_pkg.ProgrammingError):
            await cur.fetchmany(-1)

        await cur.execute("CREATE TABLE uniq_items (id INTEGER PRIMARY KEY)")
        await cur.execute("INSERT INTO uniq_items (id) VALUES (1)")
        with pytest.raises(dbapi_pkg.IntegrityError) as exc_info:
            await cur.execute("INSERT INTO uniq_items (id) VALUES (1)")
        assert type(exc_info.value) is native_module.IntegrityError
    finally:
        await conn.close()


def test_async_dbapi_native_engine_rejects_driver_without_cursor() -> None:
    """Verify AsyncDBAPIConnection raises InterfaceError when a native-cursor engine driver lacks cursor()."""

    class _NoCursorAsyncDriver:
        url = "sqlite://"

        async def close(self) -> None:
            return None

    with pytest.raises(InterfaceError, match="cursor"):
        AsyncDBAPIConnection(driver=_NoCursorAsyncDriver(), share_pool=False)


def test_removed_async_cursor_class_raises_import_error() -> None:
    """Verify the removed Python async cursor class is absent from vertebra.db.dbapi and _async."""
    removed_name = "AsyncDBAPI" + "Cursor"
    assert removed_name not in dbapi_pkg.__all__
    with pytest.raises(ImportError):
        exec(f"from vertebra.db.dbapi import {removed_name}", {})
    assert not hasattr(dbapi_pkg, removed_name)


@pytest.mark.parametrize(
    ("exc", "expected_cls"),
    [
        (ImproperConfigurationError("x"), InterfaceError),
        (UnsupportedBackendError("x"), InterfaceError),
        (MissingDependencyError("x"), InterfaceError),
        (VertebraError("x"), dbapi_pkg.DatabaseError),
        (ValueError("x"), InterfaceError),
        (TypeError("x"), InterfaceError),
        (OperationalError("x"), None),
        (dbapi_pkg.DatabaseError("x"), None),
        (dbapi_pkg.Warning("x"), None),
        (RuntimeError("x"), None),
    ],
    ids=[
        "improper-config",
        "unsupported-backend",
        "missing-dependency",
        "vertebra-error",
        "value-error",
        "type-error",
        "operational-error",
        "database-error",
        "warning",
        "runtime-error",
    ],
)
def test_connect_error_mapping(exc: Exception, expected_cls: type[Exception] | None) -> None:
    """Verify _connect_error maps configuration and driver-build errors to PEP 249 classes or None."""
    mapped = dbapi_sync._connect_error(exc)
    if expected_cls is None:
        assert mapped is None
    else:
        assert type(mapped) is expected_cls
        assert str(mapped) == str(exc)


@pytest.mark.parametrize(
    ("module_path", "removed_name"),
    [
        ("vertebra.db.dbapi", "SyncDBAPI" + "Cursor"),
        ("vertebra.db.dbapi._sync", "SyncDBAPI" + "Cursor"),
        ("vertebra.db.dbapi", "is_row_returning" + "_sql"),
        ("vertebra.db.dbapi._sync", "is_row_returning" + "_sql"),
    ],
)
def test_removed_sync_dbapi_symbols_raise_import_error(module_path: str, removed_name: str) -> None:
    """Verify removed sync DB-API cursor and classifier symbols cannot be imported."""
    with pytest.raises(ImportError):
        exec(f"from {module_path} import {removed_name}", {})


def test_sync_dbapi_transaction_call_propagates_native_error_unchanged() -> None:
    """Verify a native OperationalError raised during commit reaches the caller unchanged."""
    native_err = OperationalError("commit boom")

    class _CommitFailDriver:
        dialect = "sqlite"
        in_transaction = True

        def cursor(self, *, lazy_begin: bool = False) -> Any:
            _ = lazy_begin
            return None

        def commit(self) -> None:
            raise native_err

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    conn = SyncDBAPIConnection(driver=_CommitFailDriver(), share_pool=False)
    try:
        with pytest.raises(OperationalError) as exc_info:
            conn.commit()
        assert exc_info.value is native_err
        assert exc_info.value.__cause__ is None
    finally:
        conn.close()


def test_sync_dbapi_connect_maps_improper_configuration_to_interface_error() -> None:
    """Verify ImproperConfigurationError during sync driver build maps to InterfaceError."""

    class _BadConfig(driver_common.BaseDatabaseConfig, frozen=True):
        dialect: str = "sqlite"

        def pool_key(self) -> None:
            return None

        def build_sync_driver(self, registry: Any) -> Any:
            _ = registry
            raise ImproperConfigurationError("bad sync config")

    with pytest.raises(InterfaceError, match="bad sync config") as exc_info:
        SyncDBAPIConnection(_BadConfig(), share_pool=False)
    assert isinstance(exc_info.value.__cause__, ImproperConfigurationError)


class _RecordingPostgresSyncDriver:
    """Minimal sync driver double modeling PostgreSQL pinned-session lazy_begin routing."""

    dialect = "postgres"
    in_transaction = False

    def __init__(self, *, pinned: bool = False) -> None:
        self.pinned = pinned
        self.recorded_flags: list[bool] = []

    def cursor(self, *, lazy_begin: bool = False) -> dict[str, bool]:
        self.recorded_flags.append(lazy_begin)
        if lazy_begin and not self.pinned:
            raise InterfaceError("lazy_begin requires a pinned session")
        return {"lazy_begin": lazy_begin}

    def close(self) -> None:
        return None


def test_sync_dbapi_cursor_is_instance_attribute() -> None:
    """Verify SyncDBAPIConnection exposes cursor via a __slots__ member_descriptor without _native_cursor."""
    assert "cursor" in SyncDBAPIConnection.__slots__
    assert "_native_cursor" not in SyncDBAPIConnection.__slots__
    assert type(vars(SyncDBAPIConnection)["cursor"]).__name__ == "member_descriptor"


@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit_off", "autocommit_on"])
def test_sync_dbapi_cursor_callable_follows_autocommit(autocommit: bool) -> None:
    """Verify SyncDBAPIConnection.cursor binds partial(lazy_begin=True) or raw driver.cursor per autocommit."""
    driver = SyncSQLiteDriver()
    conn = SyncDBAPIConnection(driver=driver, share_pool=False, autocommit=autocommit)
    try:
        assert cast("Any", conn.driver) is driver
        if not autocommit:
            assert isinstance(conn.cursor, functools.partial)
            assert conn.cursor.func == driver.cursor
            assert conn.cursor.keywords == {"lazy_begin": True}
        else:
            assert conn.cursor == driver.cursor

        cur = conn.cursor()
        try:
            assert isinstance(cur, SyncSQLiteCursor)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_dbapi_cursor_callable_rebuilt_on_autocommit_change() -> None:
    """Verify toggling autocommit rebuilds the SyncDBAPIConnection.cursor callable."""
    driver = SyncSQLiteDriver()
    conn = SyncDBAPIConnection(driver=driver, share_pool=False, autocommit=False)
    try:
        assert cast("Any", conn.driver) is driver
        first_callable = conn.cursor
        assert isinstance(first_callable, functools.partial)
        assert first_callable.func == driver.cursor
        assert first_callable.keywords == {"lazy_begin": True}

        conn.autocommit = True
        assert conn.cursor == driver.cursor

        conn.autocommit = False
        rebuilt_callable = conn.cursor
        assert isinstance(rebuilt_callable, functools.partial)
        assert rebuilt_callable is not first_callable
        assert rebuilt_callable.func == driver.cursor
        assert rebuilt_callable.keywords == {"lazy_begin": True}
    finally:
        conn.close()


@pytest.mark.parametrize("dialect_name", ["oracle", "mysql", "mssql", "sqlite", "duckdb"])
def test_sync_dbapi_cursor_lazy_begin_by_engine(dialect_name: str) -> None:
    """Verify non-PostgreSQL engines bind lazy_begin=True via partial when autocommit is off."""
    recorded_flags: list[bool] = []

    class _EngineRecordingDriver:
        dialect = dialect_name
        in_transaction = False

        def cursor(self, *, lazy_begin: bool = False) -> dict[str, bool]:
            recorded_flags.append(lazy_begin)
            return {"lazy_begin": lazy_begin}

        def close(self) -> None:
            return None

    conn = SyncDBAPIConnection(driver=_EngineRecordingDriver(), share_pool=False)
    try:
        assert isinstance(conn.cursor, functools.partial)
        result = conn.cursor()
        assert result == {"lazy_begin": True}
        assert recorded_flags == [True]
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("pinned", "expected_flags"),
    [
        (False, [True, False]),
        (True, [True]),
    ],
    ids=["unpinned", "pinned"],
)
def test_sync_dbapi_postgres_cursor_decides_per_call(pinned: bool, expected_flags: list[bool]) -> None:
    """Verify PostgreSQL uses _session_routed_cursor to decide lazy_begin per call when autocommit is off."""
    driver = _RecordingPostgresSyncDriver(pinned=pinned)
    conn = SyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        assert conn.cursor == conn._session_routed_cursor
        conn.cursor()
        assert driver.recorded_flags == expected_flags
    finally:
        conn.close()


def test_sync_dbapi_postgres_cursor_follows_pinning_changes() -> None:
    """Verify PostgreSQL cursor routing adapts dynamically across unpinned and pinned session transitions."""
    driver = _RecordingPostgresSyncDriver(pinned=False)
    conn = SyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        first = conn.cursor()
        driver.pinned = True
        second = conn.cursor()
        driver.pinned = False
        third = conn.cursor()
        assert [first["lazy_begin"], second["lazy_begin"], third["lazy_begin"]] == [False, True, False]
    finally:
        conn.close()


def test_sync_dbapi_postgres_autocommit_binds_driver_cursor() -> None:
    """Verify PostgreSQL with autocommit enabled binds driver.cursor directly and passes lazy_begin=False."""
    driver = _RecordingPostgresSyncDriver(pinned=False)
    conn = SyncDBAPIConnection(driver=driver, share_pool=False, autocommit=True)
    try:
        assert conn.cursor == driver.cursor
        conn.cursor()
        assert driver.recorded_flags == [False]
    finally:
        conn.close()


def test_sync_dbapi_cursor_runs_no_dbapi_module_frame() -> None:
    """Verify conn.cursor() on SQLite executes no Python frame from vertebra/db/dbapi/_sync.py."""
    conn = SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False)
    collected_files: list[str] = []

    def _profiler(frame: Any, event: str, arg: Any) -> None:
        _ = arg
        if event == "call":
            collected_files.append(frame.f_code.co_filename)

    try:
        sys.setprofile(_profiler)
        try:
            cur = conn.cursor()
        finally:
            sys.setprofile(None)
        try:
            assert isinstance(cur, SyncSQLiteCursor)
        finally:
            cur.close()
        assert not any(Path(name).as_posix().endswith("db/dbapi/_sync.py") for name in collected_files)
    finally:
        conn.close()


def test_sync_dbapi_cursor_after_close_raises_interface_error() -> None:
    """Verify cursor() raises InterfaceError after close(), even if autocommit is subsequently modified."""
    conn = SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False)
    conn.close()
    with pytest.raises(InterfaceError, match="Cannot operate on a closed connection."):
        conn.cursor()
    conn.autocommit = True
    with pytest.raises(InterfaceError, match="Cannot operate on a closed connection."):
        conn.cursor()


def test_sync_dbapi_lazy_begin_still_opens_transaction(tmp_path: Path) -> None:
    """Verify a bound partial lazy_begin cursor still opens a transaction undone by rollback()."""
    db_file = tmp_path / "lazy_begin_sync.sqlite3"
    conn = SyncDBAPIConnection(f"sqlite+vtb:///{db_file}", share_pool=False, autocommit=False)
    try:
        setup_cur = conn.cursor()
        setup_cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        setup_cur.close()
        conn.commit()

        write_cur = conn.cursor()
        write_cur.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "rolled_back"))
        write_cur.close()
        conn.rollback()

        verify_cur = conn.cursor()
        verify_cur.execute("SELECT COUNT(*) FROM items")
        assert verify_cur.fetchall() == [(0,)]
        verify_cur.close()
    finally:
        conn.close()


class _RecordingPostgresAsyncDriver:
    """Minimal async driver double modeling PostgreSQL pinned-session lazy_begin routing."""

    dialect = "postgres"
    in_transaction = False

    def __init__(self, *, pinned: bool = False) -> None:
        self.pinned = pinned
        self.recorded_flags: list[bool] = []

    def cursor(self, *, lazy_begin: bool = False) -> dict[str, bool]:
        self.recorded_flags.append(lazy_begin)
        if lazy_begin and not self.pinned:
            raise InterfaceError("lazy_begin requires a pinned session")
        return {"lazy_begin": lazy_begin}

    async def close(self) -> None:
        return None


def test_async_dbapi_cursor_is_instance_attribute() -> None:
    """Verify AsyncDBAPIConnection exposes cursor via a __slots__ member_descriptor without _native_cursor."""
    assert "cursor" in AsyncDBAPIConnection.__slots__
    assert "_native_cursor" not in AsyncDBAPIConnection.__slots__
    assert type(vars(AsyncDBAPIConnection)["cursor"]).__name__ == "member_descriptor"


@pytest.mark.anyio
@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit_off", "autocommit_on"])
async def test_async_dbapi_cursor_callable_follows_autocommit(autocommit: bool) -> None:
    """Verify AsyncDBAPIConnection.cursor binds partial(lazy_begin=True) or raw driver.cursor per autocommit."""
    driver = AsyncSQLiteDriver()
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False, autocommit=autocommit)
    try:
        assert cast("Any", conn.driver) is driver
        if not autocommit:
            assert isinstance(conn.cursor, functools.partial)
            assert conn.cursor.func == driver.cursor
            assert conn.cursor.keywords == {"lazy_begin": True}
        else:
            assert conn.cursor == driver.cursor

        cur = conn.cursor()
        try:
            assert isinstance(cur, AsyncSQLiteCursor)
        finally:
            await cur.close()
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_cursor_callable_rebuilt_on_autocommit_change() -> None:
    """Verify toggling autocommit rebuilds the AsyncDBAPIConnection.cursor callable."""
    driver = AsyncSQLiteDriver()
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False, autocommit=False)
    try:
        assert cast("Any", conn.driver) is driver
        first_callable = conn.cursor
        assert isinstance(first_callable, functools.partial)
        assert first_callable.func == driver.cursor
        assert first_callable.keywords == {"lazy_begin": True}

        conn.autocommit = True
        assert conn.cursor == driver.cursor

        conn.autocommit = False
        rebuilt_callable = conn.cursor
        assert isinstance(rebuilt_callable, functools.partial)
        assert rebuilt_callable is not first_callable
        assert rebuilt_callable.func == driver.cursor
        assert rebuilt_callable.keywords == {"lazy_begin": True}
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect_name", ["oracle", "mysql", "mssql", "sqlite", "duckdb"])
async def test_async_dbapi_cursor_lazy_begin_by_engine(dialect_name: str) -> None:
    """Verify non-PostgreSQL async engines bind lazy_begin=True via partial when autocommit is off."""
    recorded_flags: list[bool] = []

    class _EngineRecordingAsyncDriver:
        dialect = dialect_name
        in_transaction = False

        def cursor(self, *, lazy_begin: bool = False) -> dict[str, bool]:
            recorded_flags.append(lazy_begin)
            return {"lazy_begin": lazy_begin}

        async def close(self) -> None:
            return None

    conn = AsyncDBAPIConnection(driver=_EngineRecordingAsyncDriver(), share_pool=False)
    try:
        assert isinstance(conn.cursor, functools.partial)
        result = conn.cursor()
        assert result == {"lazy_begin": True}
        assert recorded_flags == [True]
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("pinned", "expected_flags"),
    [
        (False, [True, False]),
        (True, [True]),
    ],
    ids=["unpinned", "pinned"],
)
async def test_async_dbapi_postgres_cursor_decides_per_call(pinned: bool, expected_flags: list[bool]) -> None:
    """Verify async PostgreSQL uses _session_routed_cursor to decide lazy_begin per call when autocommit is off."""
    driver = _RecordingPostgresAsyncDriver(pinned=pinned)
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        assert conn.cursor == conn._session_routed_cursor
        conn.cursor()
        assert driver.recorded_flags == expected_flags
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_postgres_cursor_follows_pinning_changes() -> None:
    """Verify async PostgreSQL cursor routing adapts dynamically across unpinned and pinned session transitions."""
    driver = _RecordingPostgresAsyncDriver(pinned=False)
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        first = conn.cursor()
        driver.pinned = True
        second = conn.cursor()
        driver.pinned = False
        third = conn.cursor()
        assert [first["lazy_begin"], second["lazy_begin"], third["lazy_begin"]] == [False, True, False]
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_postgres_autocommit_binds_driver_cursor() -> None:
    """Verify async PostgreSQL with autocommit enabled binds driver.cursor directly and passes lazy_begin=False."""
    driver = _RecordingPostgresAsyncDriver(pinned=False)
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False, autocommit=True)
    try:
        assert conn.cursor == driver.cursor
        conn.cursor()
        assert driver.recorded_flags == [False]
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_cursor_runs_no_dbapi_module_frame() -> None:
    """Verify async conn.cursor() on SQLite executes no Python frame from dbapi/_async.py or dbapi/_sync.py."""
    conn = AsyncDBAPIConnection(driver=AsyncSQLiteDriver(), share_pool=False)
    collected_files: list[str] = []

    def _profiler(frame: Any, event: str, arg: Any) -> None:
        _ = arg
        if event == "call":
            collected_files.append(frame.f_code.co_filename)

    try:
        sys.setprofile(_profiler)
        try:
            cur = conn.cursor()
        finally:
            sys.setprofile(None)
        try:
            assert isinstance(cur, AsyncSQLiteCursor)
        finally:
            await cur.close()
        assert not any(
            Path(name).as_posix().endswith(("db/dbapi/_async.py", "db/dbapi/_sync.py")) for name in collected_files
        )
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_cursor_after_close_raises_interface_error() -> None:
    """Verify async cursor() raises InterfaceError after close(), even if autocommit is subsequently modified."""
    conn = AsyncDBAPIConnection(driver=AsyncSQLiteDriver(), share_pool=False)
    await conn.close()
    with pytest.raises(InterfaceError, match="Cannot operate on a closed connection."):
        conn.cursor()
    conn.autocommit = True
    with pytest.raises(InterfaceError, match="Cannot operate on a closed connection."):
        conn.cursor()


@pytest.mark.anyio
async def test_async_dbapi_lazy_begin_still_opens_transaction(tmp_path: Path) -> None:
    """Verify a bound partial lazy_begin async cursor still opens a transaction undone by rollback()."""
    db_file = tmp_path / "lazy_begin_async.sqlite3"
    conn = AsyncDBAPIConnection(f"sqlite+vtb_async:///{db_file}", share_pool=False, autocommit=False)
    try:
        setup_cur = conn.cursor()
        await setup_cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        await setup_cur.close()
        await conn.commit()

        write_cur = conn.cursor()
        await write_cur.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "rolled_back"))
        await write_cur.close()
        await conn.rollback()

        verify_cur = conn.cursor()
        await verify_cur.execute("SELECT COUNT(*) FROM items")
        assert await verify_cur.fetchall() == [(0,)]
        await verify_cur.close()
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_dbapi_getattr_does_not_shadow_cursor() -> None:
    """Verify AsyncDBAPIConnection.__getattr__ does not shadow the stored cursor callable."""
    driver = AsyncSQLiteDriver()
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False, autocommit=False)
    try:
        assert cast("Any", conn.driver) is driver
        assert isinstance(conn.cursor, functools.partial)
        assert conn.cursor != driver.cursor
        assert conn.cursor.func == driver.cursor
        assert conn.cursor.keywords == {"lazy_begin": True}
    finally:
        await conn.close()
