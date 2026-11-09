"""Unit tests for MySQL sync/async drivers, Cloud SQL dialer integration, and mysql+vtb dialect."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.dialer.cloudsql import CloudSQLConfig, SyncCloudSQLDialer
from vertebra.extensions.sqlalchemy import (
    SyncDBAPIConnection,
    SyncMySQLDialect,
    Vector,
)
from vertebra.typing import MissingDependencyError, reset_dependency_cache


class FakeCursor:
    """Mock database cursor returning canned tabular rows."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.description = (
            ("id", 3, None, None, None, None, None),
            ("name", 253, None, None, None, None, None),
        )
        self.executed_queries: list[tuple[str, Any]] = []
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> None:
        """Record executed query and parameters."""
        self.executed_queries.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return canned rows."""
        return list(self._rows)

    def close(self) -> None:
        """Mark cursor closed."""
        self.closed = True


class FakeSyncConnection:
    """Mock synchronous DB-API connection tracking transactions and socket injection."""

    def __init__(self, *, sock: Any = None) -> None:
        self.sock = sock
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.cursors: list[FakeCursor] = []

    def cursor(self) -> FakeCursor:
        """Create and register a mock cursor."""
        cur = FakeCursor([(1, "alpha"), (2, "beta")])
        self.cursors.append(cur)
        return cur

    def commit(self) -> None:
        """Record commit invocation."""
        self.committed = True

    def rollback(self) -> None:
        """Record rollback invocation."""
        self.rolled_back = True

    def close(self) -> None:
        """Mark connection closed."""
        self.closed = True


class FakeAsyncCursor:
    """Mock asynchronous database cursor."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.description = (
            ("id", 3, None, None, None, None, None),
            ("name", 253, None, None, None, None, None),
        )
        self.closed = False

    async def execute(self, sql: str, params: Any = None) -> None:
        """Simulate async query execution."""
        _ = (sql, params)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Return canned rows."""
        return list(self._rows)

    async def close(self) -> None:
        """Mark cursor closed."""
        self.closed = True


class FakeAsyncConnection:
    """Mock asynchronous database connection context manager."""

    async def __aenter__(self) -> FakeAsyncConnection:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)

    def cursor(self) -> FakeAsyncCursorContext:
        """Return an async cursor context manager."""
        return FakeAsyncCursorContext([(10, "async_one"), (20, "async_two")])


class FakeAsyncCursorContext:
    """Async context manager wrapper around FakeAsyncCursor."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._cursor = FakeAsyncCursor(rows)

    async def __aenter__(self) -> FakeAsyncCursor:
        return self._cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        await self._cursor.close()


class FakeAsyncPool:
    """Mock asyncmy connection pool."""

    def __init__(self) -> None:
        self.closed = False

    def acquire(self) -> FakeAsyncConnection:
        """Return an async connection context manager."""
        return FakeAsyncConnection()

    def close(self) -> None:
        """Mark pool closed."""
        self.closed = True

    async def wait_closed(self) -> None:
        """Wait for pool drainage."""


def test_mysql_config_dsn_parsing() -> None:
    """Verify MySQLConfig.parse_dsn_params extracts connection parameters correctly."""
    cfg = MySQLConfig(
        dsn="mysql+vtb://usr:secret@db.internal:3307/appdb?charset=utf8mb4&autocommit=true",
        min_size=2,
        max_size=5,
    )
    params = cfg.parse_dsn_params()
    assert params["host"] == "db.internal"
    assert params["port"] == 3307
    assert params["user"] == "usr"
    assert params["password"] == "secret"
    assert params["database"] == "appdb"
    assert params["charset"] == "utf8mb4"
    assert params["autocommit"] is True

    plain_cfg = MySQLConfig(dsn="mysql://localhost/testdb")
    plain_params = plain_cfg.parse_dsn_params()
    assert plain_params["host"] == "localhost"
    assert plain_params["port"] == 3306
    assert plain_params["user"] == ""
    assert plain_params["password"] == ""
    assert plain_params["database"] == "testdb"
    assert plain_params["autocommit"] is False


def test_sync_mysql_driver_lifecycle_and_pool() -> None:
    """Verify SyncMySQLDriver pool acquisition, execution, transaction control, and closure."""
    conns_created: list[FakeSyncConnection] = []

    def make_conn(*args: Any, **kwargs: Any) -> FakeSyncConnection:
        _ = (args, kwargs)
        conn = FakeSyncConnection(**kwargs)
        conns_created.append(conn)
        return conn

    cfg = MySQLConfig(dsn="mysql://localhost:3306/app", min_size=1, max_size=2)
    driver = SyncMySQLDriver(cfg, connect_factory=make_conn)

    rows, desc = driver.execute_with_description("SELECT id, name FROM items WHERE id > %s", (0,))
    assert rows == [(1, "alpha"), (2, "beta")]
    assert desc == ("id", "name")

    tuples = driver.execute_tuples("SELECT id, name FROM items")
    assert tuples == [(1, "alpha"), (2, "beta")]

    assert len(conns_created) == 1

    driver.commit()
    assert conns_created[0].committed is True

    driver.rollback()
    assert conns_created[0].rolled_back is True

    driver.close()
    assert conns_created[0].closed is True

    with pytest.raises(RuntimeError, match="SyncMySQLDriver is closed"):
        driver.execute_tuples("SELECT 1")


def test_sync_mysql_driver_cloudsql_socket_injection() -> None:
    """Verify SyncMySQLDriver injects a CloudSQLDialer TLS socket when configured."""
    captured_socks: list[Any] = []

    def make_conn(*args: Any, **kwargs: Any) -> FakeSyncConnection:
        _ = args
        captured_socks.append(kwargs.get("sock"))
        return FakeSyncConnection(**kwargs)

    mock_sock = object()

    class FakeDialer(SyncCloudSQLDialer):
        def connect(self, host: str | None = None, *, ssl_context: Any = None, timeout: float = 10.0) -> Any:
            _ = (host, ssl_context, timeout)
            return mock_sock

    dialer_cfg = CloudSQLConfig(
        instance_connection_name="myproj:us-central1:myinst",
        enable_iam_auth=False,
    )
    dialer = FakeDialer(dialer_cfg)

    mysql_cfg = MySQLConfig(
        dsn="mysql://localhost:3306/app",
        cloud_sql_instance="myproj:us-central1:myinst",
    )
    driver = SyncMySQLDriver(mysql_cfg, dialer=dialer, connect_factory=make_conn)

    driver.execute_tuples("SELECT 1")
    assert captured_socks == [mock_sock]


def test_sync_mysql_missing_dependency_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify unmocked SyncMySQLDriver raises MissingDependencyError when pymysql is missing."""
    monkeypatch.setitem(sys.modules, "pymysql", None)
    reset_dependency_cache("pymysql")
    cfg = MySQLConfig(dsn="mysql://localhost:3306/app")
    driver = SyncMySQLDriver(cfg)
    with pytest.raises(MissingDependencyError) as exc_info:
        driver.execute_tuples("SELECT 1")
    assert exc_info.value.package == "pymysql"
    assert exc_info.value.install_package == "pymysql"


@pytest.mark.anyio
async def test_async_mysql_driver_lifecycle() -> None:
    """Verify AsyncMySQLDriver executes queries and closes pool."""
    pool = FakeAsyncPool()
    cfg = MySQLConfig(dsn="mysql://localhost:3306/app")
    driver = AsyncMySQLDriver(cfg, pool=pool)

    rows, desc = await driver.execute_with_description("SELECT id, name FROM async_items")
    assert rows == [(10, "async_one"), (20, "async_two")]
    assert desc == ("id", "name")

    tuples = await driver.execute_tuples("SELECT id, name FROM async_items")
    assert tuples == [(10, "async_one"), (20, "async_two")]

    await driver.close()
    assert pool.closed is True


@pytest.mark.anyio
async def test_async_mysql_missing_dependency_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify unmocked AsyncMySQLDriver raises MissingDependencyError when asyncmy is missing."""
    monkeypatch.setitem(sys.modules, "asyncmy", None)
    reset_dependency_cache("asyncmy")
    cfg = MySQLConfig(dsn="mysql://localhost:3306/app")
    driver = AsyncMySQLDriver(cfg)
    with pytest.raises(MissingDependencyError) as exc_info:
        await driver.execute_tuples("SELECT 1")
    assert exc_info.value.package == "asyncmy"
    assert exc_info.value.install_package == "asyncmy"


def test_dbapi_connection_routes_mysql_and_dialect_compilation() -> None:
    """Verify DBAPIConnection routes mysql+vtb URLs to SyncMySQLDriver and compiles MySQL vector types."""
    mock_conn = FakeSyncConnection()
    driver = SyncMySQLDriver(MySQLConfig(), connect_factory=lambda **k: mock_conn)

    conn = SyncDBAPIConnection("mysql+vtb://root:pw@localhost:3306/app", driver=driver)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM users WHERE id = %s", (1,))
    rows = cur.fetchall()
    assert rows == [(1, "alpha"), (2, "beta")]
    assert cur.description is not None
    assert cur.description[0][0] == "id"

    conn.commit()
    assert mock_conn.committed is True

    conn.rollback()
    assert mock_conn.rolled_back is True

    conn.close()
    assert mock_conn.closed is True

    assert SyncMySQLDialect.paramstyle == "format"
    assert SyncMySQLDialect.use_insertmanyvalues is True
    assert SyncMySQLDialect.insert_executemany_returning is False
    assert SyncMySQLDialect.supports_native_jsonb is False

    vec = Vector(512)
    assert vec.get_col_spec("mysql+vtb") == "VECTOR(512)"
    dist_expr = vec.cosine_distance([0.1, 0.2], param_name="q")
    assert dist_expr.compile_sql("mysql+vtb") == "DISTANCE_COSINE(embedding, :q)"

    adapted = SyncMySQLDialect.type_descriptor(vec)
    assert adapted is vec


@pytest.mark.parametrize("finish_action", ["commit", "rollback"])
def test_sync_mysql_driver_transaction_pins_connection_without_touching_idle_pool(finish_action: str) -> None:
    """Explicit begin() pins a single connection so commit/rollback never mutates unrelated idle connections."""
    conns_created: list[FakeSyncConnection] = []

    def make_conn(*args: Any, **kwargs: Any) -> FakeSyncConnection:
        _ = (args, kwargs)
        conn = FakeSyncConnection(**kwargs)
        conns_created.append(conn)
        return conn

    cfg = MySQLConfig(dsn="mysql://localhost:3306/app", min_size=1, max_size=4)
    driver = SyncMySQLDriver(cfg, connect_factory=make_conn)
    unrelated_idle = FakeSyncConnection()
    candidate_conn = FakeSyncConnection()
    driver._idle_conns.extend([unrelated_idle, candidate_conn])
    try:
        driver.begin()
        pinned = driver._tx_conn
        assert pinned is candidate_conn
        other_idle = FakeSyncConnection()
        driver._idle_conns.append(other_idle)

        rows = driver.execute_tuples("SELECT id, name FROM items")
        assert rows == [(1, "alpha"), (2, "beta")]
        assert len(pinned.cursors) == 1
        assert len(unrelated_idle.cursors) == 0
        assert len(other_idle.cursors) == 0

        if finish_action == "commit":
            driver.commit()
            assert pinned.committed is True
            assert pinned.rolled_back is False
        else:
            driver.rollback()
            assert pinned.rolled_back is True
            assert pinned.committed is False

        assert unrelated_idle.committed is False
        assert unrelated_idle.rolled_back is False
        assert other_idle.committed is False
        assert other_idle.rolled_back is False
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_mysql_driver_ensure_pool_resolves_iam_password_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """AsyncMySQLDriver._ensure_pool awaits parse_dsn_params_async() and passes the IAM token to asyncmy.create_pool."""
    captured_kwargs: dict[str, Any] = {}
    pool = FakeAsyncPool()

    async def fake_create_pool(**kwargs: Any) -> FakeAsyncPool:
        captured_kwargs.update(kwargs)
        return pool

    fake_asyncmy = types.ModuleType("asyncmy")
    fake_asyncmy.__dict__["create_pool"] = fake_create_pool
    monkeypatch.setitem(sys.modules, "asyncmy", fake_asyncmy)
    reset_dependency_cache("asyncmy")

    cfg = MySQLConfig(
        dsn="mysql://iam-user@10.0.0.8:3307/prod_db",
        enable_iam_auth=True,
        iam_auth_options={"access_token": "mock-oauth-token"},
        min_size=2,
        max_size=6,
    )
    driver = AsyncMySQLDriver(cfg)
    try:
        rows = await driver.execute_tuples("SELECT id, name FROM async_items")
        assert rows == [(10, "async_one"), (20, "async_two")]
        assert captured_kwargs["host"] == "10.0.0.8"
        assert captured_kwargs["port"] == 3307
        assert captured_kwargs["user"] == "iam-user"
        assert captured_kwargs["password"] == "mock-oauth-token"
        assert captured_kwargs["db"] == "prod_db"
        assert captured_kwargs["minsize"] == 2
        assert captured_kwargs["maxsize"] == 6
    finally:
        await driver.close()
        reset_dependency_cache("asyncmy")
