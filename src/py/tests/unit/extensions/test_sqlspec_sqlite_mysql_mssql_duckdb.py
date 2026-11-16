"""Unit tests for Vertebra SQLSpec SQLite, MySQL, MSSQL, and DuckDB adapter packages."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from sqlspec.core import SQL, ParameterStyle, TypedParameter
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class

from vertebra.extensions.sqlspec.adapters.duckdb import (
    DUCKDB_COPY_FILENAME_FOLLOWS_QUERY,
    DuckDBAsyncConfig,
    DuckDBAsyncDataDictionary,
    DuckDBAsyncDriver,
    DuckDBEventQueueStore,
    DuckDBLitestarStore,
    DuckDBSyncConfig,
    DuckDBSyncDataDictionary,
    DuckDBSyncDriver,
    build_duckdb_copy_sql,
    description_has_arrow_divergent_types,
    duckdb_copy_filename_follows_query,
    map_duckdb_exception,
    order_duckdb_copy_parameters,
)
from vertebra.extensions.sqlspec.adapters.duckdb import (
    build_connection_config as build_duckdb_connection_config,
)
from vertebra.extensions.sqlspec.adapters.duckdb.adk import DuckDBAsyncADKStore, DuckDBSyncADKStore
from vertebra.extensions.sqlspec.adapters.mssql import (
    MSSQLAsyncConfig,
    MSSQLAsyncDataDictionary,
    MSSQLAsyncDriver,
    MSSQLEventQueueStore,
    MSSQLLitestarStore,
    MSSQLSyncConfig,
    MSSQLSyncDataDictionary,
    MSSQLSyncDriver,
    map_mssql_exception,
    rewrite_mssql_placeholders,
)
from vertebra.extensions.sqlspec.adapters.mssql import (
    build_connection_config as build_mssql_connection_config,
)
from vertebra.extensions.sqlspec.adapters.mssql.adk import MSSQLAsyncADKStore, MSSQLSyncADKStore
from vertebra.extensions.sqlspec.adapters.mssql.core import build_mssql_scope_where, quote_bracket_identifier
from vertebra.extensions.sqlspec.adapters.mysql import (
    MySQLAsyncConfig,
    MySQLAsyncDataDictionary,
    MySQLAsyncDriver,
    MySQLEventQueueStore,
    MySQLLitestarStore,
    MySQLSyncConfig,
    MySQLSyncDataDictionary,
    MySQLSyncDriver,
    map_mysql_exception,
)
from vertebra.extensions.sqlspec.adapters.mysql import (
    build_statement_config as build_mysql_statement_config,
)
from vertebra.extensions.sqlspec.adapters.mysql.adk import MySQLAsyncADKStore, MySQLSyncADKStore
from vertebra.extensions.sqlspec.adapters.mysql.core import (
    format_mysql_vector_literal,
    mysql_json_dict,
    mysql_table_options,
)
from vertebra.extensions.sqlspec.adapters.sqlite import (
    SQLiteAsyncConfig,
    SQLiteAsyncDataDictionary,
    SQLiteAsyncDriver,
    SQLiteEventQueueStore,
    SQLiteLitestarStore,
    SQLiteSyncConfig,
    SQLiteSyncDataDictionary,
    SQLiteSyncDriver,
    datetime_to_julian,
    julian_to_datetime,
)
from vertebra.extensions.sqlspec.adapters.sqlite.adk import SQLiteAsyncADKStore, SQLiteSyncADKStore
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    cosine_similarity,
    extension_pragma_statements,
    map_sqlite_exception,
    normalize_sqlite_parameters,
    pack_embedding,
    unpack_embedding,
    validate_fts_options,
)
from vertebra.typing import ImproperConfigurationError


class _RecordingSyncConn:
    """In-memory recording native sync driver double for relational adapter tests."""

    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        scalar: Any = None,
    ) -> None:
        self._closed = False
        self._in_tx = False
        self.rows = list(rows or [])
        self.columns = list(columns or [])
        self.scalar = scalar
        self.calls: list[tuple[str, str, Any]] = []
        self.scripts: list[str] = []
        self.tx_events: list[str] = []

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_transaction(self) -> bool:
        return self._in_tx

    def open(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        self.calls.append(("execute", sql, parameters))
        return 1

    def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        return len(batch)

    def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        if self.scalar is not None:
            return [(self.scalar,)], (self.columns or ["val"])
        cols = self.columns or (list(self.rows[0].keys()) if self.rows else [])
        tuple_rows = [tuple(row[col] for col in cols) if isinstance(row, dict) else tuple(row) for row in self.rows]
        return tuple_rows, cols

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return self.rows

    def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return self.rows[0] if self.rows else None

    def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


class _RecordingAsyncConn:
    """In-memory recording native async driver double for relational adapter tests."""

    def __init__(self, *, rows: list[dict[str, Any]] | None = None, columns: list[str] | None = None) -> None:
        self._closed = False
        self._in_tx = False
        self.rows = list(rows or [])
        self.columns = list(columns or [])
        self.calls: list[tuple[str, str, Any]] = []
        self.scripts: list[str] = []
        self.tx_events: list[str] = []

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_transaction(self) -> bool:
        return self._in_tx

    async def open(self) -> None:
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        self.calls.append(("execute", sql, parameters))
        return 1

    async def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        return len(batch)

    async def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        cols = self.columns or (list(self.rows[0].keys()) if self.rows else [])
        tuple_rows = [tuple(row[col] for col in cols) if isinstance(row, dict) else tuple(row) for row in self.rows]
        return tuple_rows, cols

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return self.rows

    async def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return self.rows[0] if self.rows else None

    async def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    async def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    async def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


class _StubCodeError(Exception):
    """Synthetic driver exception carrying numeric error code and SQLSTATE attributes."""

    def __init__(self, message: str, *, errno: int | None = None, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.errno = errno
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    ("module_path", "expected_symbols"),
    [
        (
            "vertebra.extensions.sqlspec.adapters.sqlite",
            (
                "SQLiteSyncConfig",
                "SQLiteAsyncConfig",
                "SQLiteSyncDriver",
                "SQLiteAsyncDriver",
                "SQLiteSyncDataDictionary",
            ),
        ),
        (
            "vertebra.extensions.sqlspec.adapters.mysql",
            ("MySQLSyncConfig", "MySQLAsyncConfig", "MySQLSyncDriver", "MySQLAsyncDriver", "MySQLSyncDataDictionary"),
        ),
        (
            "vertebra.extensions.sqlspec.adapters.mssql",
            ("MSSQLSyncConfig", "MSSQLAsyncConfig", "MSSQLSyncDriver", "MSSQLAsyncDriver", "MSSQLSyncDataDictionary"),
        ),
        (
            "vertebra.extensions.sqlspec.adapters.duckdb",
            (
                "DuckDBSyncConfig",
                "DuckDBAsyncConfig",
                "DuckDBSyncDriver",
                "DuckDBAsyncDriver",
                "DuckDBSyncDataDictionary",
            ),
        ),
    ],
)
def test_relational_adapters_lazy_pep562_exports(module_path: str, expected_symbols: tuple[str, ...]) -> None:
    """Verify PEP 562 `__getattr__` lazy resolution and unknown-attribute rejection."""
    module = import_module(module_path)
    resolved = [getattr(module, sym) for sym in expected_symbols]
    assert all(obj is not None for obj in resolved)
    with pytest.raises(AttributeError):
        _ = module.NonExistentAdapterSymbol


@pytest.mark.parametrize(
    ("config_factory", "expected_adapter", "expected_dialect", "expected_stores"),
    [
        (
            lambda: SQLiteSyncConfig(database=":memory:"),
            "vertebra_sqlite",
            "sqlite",
            {
                "adk": ("adk_store_class", "ADKStore", SQLiteSyncADKStore),
                "events": ("events_store_class", "EventQueueStore", SQLiteEventQueueStore),
                "litestar": ("litestar_store_class", "Store", SQLiteLitestarStore),
            },
        ),
        (
            lambda: SQLiteAsyncConfig(database=":memory:"),
            "vertebra_sqlite",
            "sqlite",
            {
                "adk": ("adk_store_class", "ADKStore", SQLiteAsyncADKStore),
                "events": ("events_store_class", "EventQueueStore", SQLiteEventQueueStore),
                "litestar": ("litestar_store_class", "Store", SQLiteLitestarStore),
            },
        ),
        (
            lambda: MySQLSyncConfig(url="mysql://user:pw@127.0.0.1:3306/appdb"),
            "vertebra_mysql",
            "mysql",
            {
                "adk": ("adk_store_class", "ADKStore", MySQLSyncADKStore),
                "events": ("events_store_class", "EventQueueStore", MySQLEventQueueStore),
                "litestar": ("litestar_store_class", "Store", MySQLLitestarStore),
            },
        ),
        (
            lambda: MySQLAsyncConfig(url="mysql://user:pw@127.0.0.1:3306/appdb"),
            "vertebra_mysql",
            "mysql",
            {
                "adk": ("adk_store_class", "ADKStore", MySQLAsyncADKStore),
                "events": ("events_store_class", "EventQueueStore", MySQLEventQueueStore),
                "litestar": ("litestar_store_class", "Store", MySQLLitestarStore),
            },
        ),
        (
            lambda: MSSQLSyncConfig(url="mssql://sa:Passw0rd@127.0.0.1:1433/appdb"),
            "vertebra_mssql",
            "tsql",
            {
                "adk": ("adk_store_class", "ADKStore", MSSQLSyncADKStore),
                "events": ("events_store_class", "EventQueueStore", MSSQLEventQueueStore),
                "litestar": ("litestar_store_class", "Store", MSSQLLitestarStore),
            },
        ),
        (
            lambda: MSSQLAsyncConfig(url="mssql://sa:Passw0rd@127.0.0.1:1433/appdb"),
            "vertebra_mssql",
            "tsql",
            {
                "adk": ("adk_store_class", "ADKStore", MSSQLAsyncADKStore),
                "events": ("events_store_class", "EventQueueStore", MSSQLEventQueueStore),
                "litestar": ("litestar_store_class", "Store", MSSQLLitestarStore),
            },
        ),
        (
            lambda: DuckDBSyncConfig(database=":memory:unit_sync"),
            "vertebra_duckdb",
            "duckdb",
            {
                "adk": ("adk_store_class", "ADKStore", DuckDBSyncADKStore),
                "events": ("events_store_class", "EventQueueStore", DuckDBEventQueueStore),
                "litestar": ("litestar_store_class", "Store", DuckDBLitestarStore),
            },
        ),
        (
            lambda: DuckDBAsyncConfig(database=":memory:unit_async"),
            "vertebra_duckdb",
            "duckdb",
            {
                "adk": ("adk_store_class", "ADKStore", DuckDBAsyncADKStore),
                "events": ("events_store_class", "EventQueueStore", DuckDBEventQueueStore),
                "litestar": ("litestar_store_class", "Store", DuckDBLitestarStore),
            },
        ),
    ],
)
def test_relational_configs_register_adapter_name_dialect_and_extension_stores(
    config_factory: Any,
    expected_adapter: str,
    expected_dialect: str,
    expected_stores: dict[str, tuple[str, str, type[Any]]],
) -> None:
    """Verify `resolve_adapter_name`, dialect names, and extension store hooks for SQLSpec."""
    cfg = config_factory()
    assert resolve_adapter_name(cfg) == expected_adapter
    assert cfg.dialect == expected_dialect
    adk_attr, adk_suffix, adk_cls = expected_stores["adk"]
    ev_attr, ev_suffix, ev_cls = expected_stores["events"]
    lit_attr, lit_suffix, lit_cls = expected_stores["litestar"]
    assert resolve_extension_store_class(cfg, extension="adk", store_attr=adk_attr, store_suffix=adk_suffix) is adk_cls
    assert resolve_extension_store_class(cfg, extension="events", store_attr=ev_attr, store_suffix=ev_suffix) is ev_cls
    assert (
        resolve_extension_store_class(cfg, extension="litestar", store_attr=lit_attr, store_suffix=lit_suffix)
        is lit_cls
    )


def test_sqlite_core_julian_embeddings_pragmas_and_fts_validation() -> None:
    """Verify SQLite Julian Day timestamps, IEEE-754 vector packing, PRAGMA rendering, and FTS validation."""
    now = datetime(2026, 11, 16, 12, 30, 0, tzinfo=UTC)
    julian = datetime_to_julian(now)
    assert julian is not None
    restored = julian_to_datetime(julian)
    assert restored is not None
    assert abs((restored - now).total_seconds()) < 1e-3
    assert julian_to_datetime("2026-11-16T12:30:00") == datetime(2026, 11, 16, 12, 30, 0, tzinfo=UTC)

    blob = pack_embedding([1.0, 0.0, 0.5])
    assert blob is not None
    unpacked = unpack_embedding(blob)
    assert unpacked is not None
    assert cosine_similarity(unpacked, [1.0, 0.0, 0.5]) == pytest.approx(1.0, abs=1e-6)
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0, abs=1e-6)

    named = normalize_sqlite_parameters(
        "SELECT * FROM items WHERE id = :id AND 'literal :ignored' != ''",
        {"id": TypedParameter(7, int, semantic_name="INTEGER"), "unused": 99},
    )
    assert named == {":id": 7}

    class _FakeCfg:
        extension_config = {
            "litestar": {"pragma_profile": True, "pragma_overrides": {"synchronous": "NORMAL"}},
            "adk": {"fts_tokenize": "porter unicode61", "fts_detail": "column"},
        }

    pragmas = extension_pragma_statements(_FakeCfg(), "litestar")
    assert any(stmt.startswith("PRAGMA foreign_keys = ON") for stmt in pragmas)
    assert "PRAGMA synchronous = NORMAL" in pragmas
    assert validate_fts_options(_FakeCfg()) == {"tokenize": "porter unicode61", "detail": "column"}

    class _BadFtsCfg:
        extension_config = {"adk": {"fts_detail": "bogus"}}

    with pytest.raises(ImproperConfigurationError):
        validate_fts_options(_BadFtsCfg())


def test_sqlite_real_driver_execution_dictionary_litestar_and_adk(tmp_path: Path) -> None:
    """Execute real SQLite queries, transactions, DataDictionary inspection, Litestar sessions, and ADK stores."""
    db_path = tmp_path / "sqlspec_unit.db"
    cfg = SQLiteSyncConfig(
        database=str(db_path),
        extension_config={"litestar": {"pragma_profile": True}},
    )
    with cfg.provide_session() as driver:
        assert isinstance(driver, SQLiteSyncDriver)
        driver.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, meta TEXT);")
        driver.execute_many(
            "INSERT INTO items (id, name, meta) VALUES (?, ?, ?)", [(1, "alpha", "{}"), (2, "beta", "{}")]
        )
        driver.commit()

        row = driver.select_one("SELECT id, name FROM items WHERE id = :item_id", {"item_id": 2})
        assert row == {"id": 2, "name": "beta"}

        version = driver.data_dictionary.get_version(driver)
        assert version is not None
        assert version.major >= 3
        columns = driver.data_dictionary.get_columns(driver, "items")
        assert [col["column_name"] for col in columns] == ["id", "name", "meta"]

    litestar_store = SQLiteLitestarStore(cfg)
    assert isinstance(SQLiteSyncDataDictionary(), SQLiteSyncDataDictionary)

    asyncio.run(litestar_store.create_table())
    asyncio.run(litestar_store.set("sess-1", b"payload", expires_in=timedelta(seconds=60)))
    assert asyncio.run(litestar_store.get("sess-1")) == b"payload"
    asyncio.run(litestar_store.delete("sess-1"))
    assert asyncio.run(litestar_store.get("sess-1")) is None

    events_store = SQLiteEventQueueStore(cfg)
    assert events_store._column_types() == ("TEXT", "TEXT", "TIMESTAMP")
    adk_store = SQLiteSyncADKStore(cfg)
    adk_store.create_tables()
    created = adk_store.create_session("s1", "demo_app", "user_1", {"step": 1})
    assert created["id"] == "s1"
    loaded = adk_store.get_session("demo_app", "user_1", "s1")
    assert loaded is not None
    assert loaded["state"] == {"step": 1}


@pytest.mark.anyio
async def test_sqlite_real_async_driver_execution(tmp_path: Path) -> None:
    """Verify real `SQLiteAsyncConfig` and `SQLiteAsyncDriver` async query and data-dictionary flows."""
    cfg = SQLiteAsyncConfig(database=str(tmp_path / "async_unit.db"))
    async with cfg.provide_session() as driver:
        assert isinstance(driver, SQLiteAsyncDriver)
        await driver.execute_script("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")
        await driver.execute("INSERT INTO users (id, email) VALUES (?, ?)", (10, "a@example.com"))
        await driver.commit()
        found = await driver.select_one("SELECT email FROM users WHERE id = ?", (10,))
        assert found == {"email": "a@example.com"}
        version = await driver.data_dictionary.get_version(driver)
        assert version is not None
        assert isinstance(driver.data_dictionary, SQLiteAsyncDataDictionary)


def test_mysql_core_driver_dictionary_events_and_litestar() -> None:
    """Verify MySQL `QMARK` compilation, vector/JSON helpers, MariaDB feature flags, and stores."""
    stmt_cfg = build_mysql_statement_config()
    assert stmt_cfg.dialect == "mysql"
    assert stmt_cfg.parameter_config.default_execution_parameter_style == ParameterStyle.QMARK

    compiled = SQL("SELECT * FROM docs WHERE id = :id", {"id": 42}, statement_config=stmt_cfg).compile()
    assert "?" in compiled[0]
    assert compiled[1] == (42,)

    assert format_mysql_vector_literal([1.0, 2.5, 3.0]) == "[1.0,2.5,3.0]"
    assert mysql_json_dict(b'{"k": 1}') == {"k": 1}
    assert mysql_table_options({"table_options": "ROW_FORMAT=DYNAMIC"}) == " ROW_FORMAT=DYNAMIC"

    conn = _RecordingSyncConn(rows=[{"id": 1, "name": "row1"}], columns=["id", "name"])
    driver = MySQLSyncDriver(conn)
    result = driver.execute("SELECT id, name FROM docs WHERE id = ?", TypedParameter(1, int, semantic_name="INT"))
    assert result.data == [{"id": 1, "name": "row1"}]
    assert conn.calls[-1][2] == (1,)

    driver.execute_many("INSERT INTO docs (id) VALUES (?)", [(1,), (2,)])
    assert conn.calls[-1][0] == "execute_many"
    assert conn.calls[-1][2] == [(1,), (2,)]

    dict_sync = MySQLSyncDataDictionary()
    maria_conn = _RecordingSyncConn(scalar="11.7.2-MariaDB")
    maria_drv = MySQLSyncDriver(maria_conn)
    assert dict_sync.get_feature_flag(maria_drv, "supports_vector") is True

    mysql8_conn = _RecordingSyncConn(scalar="8.4.3")
    mysql8_drv = MySQLSyncDriver(mysql8_conn)
    assert dict_sync.get_feature_flag(mysql8_drv, "supports_vector") is False
    assert isinstance(MySQLAsyncDataDictionary(), MySQLAsyncDataDictionary)

    cfg = MySQLSyncConfig(url="mysql://user:pw@127.0.0.1:3306/app")
    ev_store = MySQLEventQueueStore(cfg)
    assert ev_store._column_types() == ("JSON", "JSON", "DATETIME(6)")
    assert "ADD INDEX" in (ev_store._index_ddl() or "")
    lit_store = MySQLLitestarStore(cfg)
    ddl = lit_store._table_ddl()
    assert "LONGBLOB NOT NULL" in ddl
    assert "DATETIME(6)" in ddl


@pytest.mark.anyio
async def test_mysql_async_driver_dispatch() -> None:
    """Verify `MySQLAsyncDriver` executes parameterized `?` queries asynchronously."""
    conn = _RecordingAsyncConn(rows=[{"id": 9}], columns=["id"])
    driver = MySQLAsyncDriver(conn)
    selected = await driver.select("SELECT id FROM items WHERE id = ?", (9,))
    assert selected == [{"id": 9}]
    assert conn.calls[-1][2] == (9,)


def test_mssql_core_odbc_rewrites_savepoints_and_stores() -> None:
    """Verify MSSQL ODBC parsing, `@P1` placeholder rewrite, T-SQL savepoints, and store DDL."""
    built = build_mssql_connection_config(
        connection_config={
            "connection_string": "Server=db.internal,14330;Database=sales;Uid=svc;Pwd=secret;ApplicationIntent=ReadOnly;TrustServerCertificate=yes"
        }
    )
    assert built.host == "db.internal"
    assert built.port == 14330
    assert built.database == "sales"
    assert built.read_only_intent is True
    assert built.trust_server_certificate is True

    rewritten, count = rewrite_mssql_placeholders("SELECT '?' AS q, * FROM t WHERE a = ? AND b = $2")
    assert rewritten == "SELECT '?' AS q, * FROM t WHERE a = @P1 AND b = @P2"
    assert count == 2
    assert quote_bracket_identifier("dbo].users") == "[dbo]].users]"
    scope_sql, scope_binds = build_mssql_scope_where("app1", "usr1", "all")
    assert "N'user'" in scope_sql
    assert scope_binds == ("app1", "usr1")

    conn = _RecordingSyncConn(rows=[{"v": 1}], columns=["v"])
    driver = MSSQLSyncDriver(conn)
    driver.create_savepoint("sp_one")
    driver.release_savepoint("sp_one")
    driver.rollback_to_savepoint("sp_one")
    assert ("execute", "SAVE TRANSACTION sp_one", None) in conn.calls
    assert ("execute", "ROLLBACK TRANSACTION sp_one", None) in conn.calls
    assert isinstance(driver.data_dictionary, MSSQLSyncDataDictionary)

    cfg = MSSQLSyncConfig(url="mssql://sa:pw@127.0.0.1:1433/app")
    ev_store = MSSQLEventQueueStore(cfg)
    assert ev_store._column_types() == ("NVARCHAR(MAX)", "NVARCHAR(MAX)", "DATETIMEOFFSET(6)")
    assert "IF OBJECT_ID" in ev_store._wrap_create_statement("CREATE TABLE q (id NVARCHAR(64))", "table")

    lit_store = MSSQLLitestarStore(cfg)
    lit_ddl = lit_store._table_ddl()
    assert "VARBINARY(MAX) NOT NULL" in lit_ddl
    assert "WHERE expires_at IS NOT NULL" in lit_ddl


@pytest.mark.anyio
async def test_mssql_async_driver_savepoints() -> None:
    """Verify `MSSQLAsyncDriver` delegates T-SQL savepoints and exposes `MSSQLAsyncDataDictionary`."""
    conn = _RecordingAsyncConn()
    driver = MSSQLAsyncDriver(conn)
    await driver.create_savepoint("sp_async")
    await driver.rollback_to_savepoint("sp_async")
    assert ("execute", "SAVE TRANSACTION sp_async", None) in conn.calls
    assert ("execute", "ROLLBACK TRANSACTION sp_async", None) in conn.calls
    assert isinstance(driver.data_dictionary, MSSQLAsyncDataDictionary)


def test_duckdb_copy_ordering_arrow_divergence_savepoints_and_stores() -> None:
    """Verify DuckDB `:memory:<id>`, COPY SQL lexical bind ordering, Arrow bypass detection, and stores."""
    cfg = DuckDBSyncConfig(database=":memory:analytics_a")
    built = build_duckdb_connection_config(connection_config={"memory_id": "iso_one", "memory_limit": "512MiB"})
    assert built.database == ":memory:iso_one"
    assert built.memory_limit_mb == 512

    copy_sql = build_duckdb_copy_sql("SELECT id FROM events WHERE kind = ?", "parquet")
    assert copy_sql is not None
    assert "FORMAT PARQUET" in copy_sql.upper()
    assert duckdb_copy_filename_follows_query() is DUCKDB_COPY_FILENAME_FOLLOWS_QUERY
    assert order_duckdb_copy_parameters(["click"], "/tmp/out.parquet") == ("click", "/tmp/out.parquet")
    assert order_duckdb_copy_parameters(["click"], "/tmp/out.parquet", filename_follows_query=False) == (
        "/tmp/out.parquet",
        "click",
    )

    assert description_has_arrow_divergent_types([("big", "HUGEINT")]) is True
    assert description_has_arrow_divergent_types([("fixed", "INT[3]")]) is True
    assert description_has_arrow_divergent_types([("id", "INTEGER")]) is False

    conn = _RecordingSyncConn()
    driver = DuckDBSyncDriver(conn)
    compiled_copy, copy_params = driver.prepare_copy_to_query(
        "SELECT * FROM events WHERE user_id = ?",
        7,
        destination="s3://bucket/out.csv",
        file_format="csv",
    )
    assert "FORMAT CSV" in compiled_copy.upper()
    assert copy_params == (7, "s3://bucket/out.csv")
    assert driver.should_bypass_arrow_materialization([("attrs", "MAP(VARCHAR, INT)")]) is True
    driver.create_savepoint("sp_duck")
    driver.release_savepoint("sp_duck")
    driver.rollback_to_savepoint("sp_duck")
    assert isinstance(driver.data_dictionary, DuckDBSyncDataDictionary)

    ev_store = DuckDBEventQueueStore(cfg)
    assert ev_store._column_types() == ("VARCHAR", "VARCHAR", "TIMESTAMP")
    lit_store = DuckDBLitestarStore(cfg)
    assert "CREATE TABLE IF NOT EXISTS" in lit_store._table_ddl()


@pytest.mark.anyio
async def test_duckdb_async_driver_dictionary() -> None:
    """Verify `DuckDBAsyncDriver` exposes `DuckDBAsyncDataDictionary` and executes savepoints."""
    conn = _RecordingAsyncConn()
    driver = DuckDBAsyncDriver(conn)
    await driver.create_savepoint("sp_async_duck")
    await driver.release_savepoint("sp_async_duck")
    await driver.rollback_to_savepoint("sp_async_duck")
    assert ("execute", "SAVEPOINT sp_async_duck", None) in conn.calls
    assert isinstance(driver.data_dictionary, DuckDBAsyncDataDictionary)


@pytest.mark.parametrize(
    ("mapper", "exc", "expected_cls"),
    [
        (map_sqlite_exception, RuntimeError("UNIQUE constraint failed: users.email"), UniqueViolationError),
        (map_sqlite_exception, RuntimeError("FOREIGN KEY constraint failed"), ForeignKeyViolationError),
        (map_sqlite_exception, RuntimeError("NOT NULL constraint failed: users.name"), NotNullViolationError),
        (map_sqlite_exception, RuntimeError("CHECK constraint failed: pos"), CheckViolationError),
        (map_sqlite_exception, RuntimeError("database is locked (SQLITE_BUSY)"), OperationalError),
        (map_sqlite_exception, RuntimeError("SQLITE_INTERRUPT"), OperationCancelledError),
        (map_sqlite_exception, RuntimeError("no such table: missing"), SQLParsingError),
        (map_mysql_exception, _StubCodeError("Duplicate entry", errno=1062), UniqueViolationError),
        (map_mysql_exception, _StubCodeError("Cannot add child row", errno=1452), ForeignKeyViolationError),
        (map_mysql_exception, _StubCodeError("Column cannot be null", errno=1048), NotNullViolationError),
        (map_mysql_exception, _StubCodeError("Check constraint violated", errno=3819), CheckViolationError),
        (map_mysql_exception, _StubCodeError("Deadlock found", errno=1213), DeadlockError),
        (map_mysql_exception, _StubCodeError("Lock wait timeout exceeded", errno=1205), QueryTimeoutError),
        (map_mysql_exception, _StubCodeError("Lost connection", errno=2013), ConnectionTimeoutError),
        (map_mysql_exception, _StubCodeError("Access denied", errno=1045), PermissionDeniedError),
        (map_mssql_exception, _StubCodeError("Violation of PRIMARY KEY", errno=2627), UniqueViolationError),
        (map_mssql_exception, _StubCodeError("FOREIGN KEY constraint conflict", errno=547), ForeignKeyViolationError),
        (map_mssql_exception, _StubCodeError("CHECK constraint conflict", errno=547), CheckViolationError),
        (map_mssql_exception, _StubCodeError("Cannot insert NULL", errno=515), NotNullViolationError),
        (map_mssql_exception, _StubCodeError("Snapshot isolation conflict", errno=3960), SerializationConflictError),
        (map_mssql_exception, _StubCodeError("Deadlock victim", errno=1205), DeadlockError),
        (map_mssql_exception, _StubCodeError("Login failed", errno=18456), PermissionDeniedError),
        (map_mssql_exception, _StubCodeError("Timeout", sqlstate="HYT00"), QueryTimeoutError),
        (map_mssql_exception, _StubCodeError("Link failure", sqlstate="08S01"), DatabaseConnectionError),
        (
            map_duckdb_exception,
            RuntimeError("Constraint Error: Duplicate key violates primary key constraint"),
            UniqueViolationError,
        ),
        (map_duckdb_exception, RuntimeError("Catalog Error: Table items does not exist!"), NotFoundError),
        (map_duckdb_exception, RuntimeError("Binder Error: Referenced column missing"), SQLParsingError),
        (
            map_duckdb_exception,
            RuntimeError("TransactionContext Error: Conflict on commit"),
            SerializationConflictError,
        ),
        (map_duckdb_exception, RuntimeError("Conversion Error: Could not convert string to INT32"), DataError),
        (map_duckdb_exception, RuntimeError("InterruptException: Interrupted"), OperationCancelledError),
    ],
)
def test_relational_exception_mapping(mapper: Any, exc: BaseException, expected_cls: type[Exception]) -> None:
    """Verify deterministic native driver error translation onto SQLSpec exception hierarchy."""
    mapped = mapper(exc)
    assert isinstance(mapped, expected_cls)
